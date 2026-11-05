//! Multishot accept and recv for TCP.

use std::{
    io,
    net::SocketAddr,
    task::{Context, Poll},
};

use super::{TcpListener, TcpStream};
use crate::{
    driver::{
        op::{
            accept_multi::AcceptMultiOp,
            recv_multi::{self, Bundle, RecvBundleStream, RecvMultiOp},
        },
        raw::{BufRing, SelectedBuf},
    },
    io::stream::Stream,
    net::AcceptFlags,
};

impl TcpListener {
    /// Start a multishot accept (Linux 5.19+): one submission accepts connections until the
    /// returned [`AcceptStream`] is dropped or cancelled, without a new SQE per connection.
    ///
    /// `flags` are set on every accepted socket, as in [`accept_with`](Self::accept_with).
    ///
    /// The kernel does not report the peer address with a multishot accept: [`AcceptStream::next`]
    /// gets it with one `getpeername` syscall per connection. Use
    /// [`next_stream`](AcceptStream::next_stream) to skip it (the address is fetched lazily by
    /// [`TcpStream::peer_addr`] then).
    ///
    /// On kernels without multishot accept the first item is an error (`EINVAL`).
    pub fn accept_multi(&self, flags: AcceptFlags) -> io::Result<AcceptStream> {
        AcceptMultiOp::new(&self.fd, flags.bits()).map(|op| AcceptStream { op })
    }
}

impl TcpListener {
    /// Start a multishot direct accept (Linux 5.19+): like [`accept_multi`](Self::accept_multi),
    /// but every accepted socket goes straight into a slot of the fixed file table that the
    /// kernel allocates (`IORING_FILE_INDEX_ALLOC`), and comes out as a direct-only
    /// [`TcpStream`] (no fd, see [`TcpStream::is_direct`]).
    ///
    /// The kernel allocates from the slots reserved with
    /// [`RuntimeBuilder::with_fixed_files_kernel_alloc`](crate::RuntimeBuilder::with_fixed_files_kernel_alloc);
    /// without them this fails with `Unsupported`. When they are all taken the accept ends with
    /// `ENFILE` (re-arm once streams were closed). The kernel reports no peer address, and a
    /// direct-only stream can't ask for it: [`TcpStream::peer_addr`] fails on these streams.
    ///
    /// `flags` as in [`accept_with`](Self::accept_with), `CLOEXEC` is ignored.
    pub fn accept_multi_direct(&self, flags: AcceptFlags) -> io::Result<DirectAcceptStream> {
        AcceptMultiOp::new_direct(&self.fd, flags.bits()).map(|op| DirectAcceptStream { op })
    }
}

/// Direct-only connections accepted by a multishot direct accept, see
/// [`TcpListener::accept_multi_direct`].
///
/// Behaves like [`AcceptStream`] (queueing, cancel, drop closes the connections not taken, an
/// error ends it), without peer addresses.
#[must_use = "streams do nothing unless polled"]
pub struct DirectAcceptStream {
    op: AcceptMultiOp,
}

impl DirectAcceptStream {
    /// Poll for the next connection.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<TcpStream>>> {
        let next = ready!(self.op.poll_next_fd(cx));
        Poll::Ready(next.map(|fd| fd.map(TcpStream::from_shared_fd)))
    }

    /// The next connection. `None` once the accept ended (cancelled, or after an error that
    /// ended it).
    pub async fn next(&mut self) -> Option<io::Result<TcpStream>> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Stop accepting (AsyncCancel). Connections already accepted are still returned, then
    /// `next` returns `None`.
    pub fn cancel(&self) {
        self.op.cancel()
    }

    /// Whether the accept ended (`next` returns `None` from now on).
    pub fn is_finished(&self) -> bool {
        self.op.is_finished()
    }
}

impl Stream for DirectAcceptStream {
    type Item = io::Result<TcpStream>;

    async fn next(&mut self) -> Option<Self::Item> {
        DirectAcceptStream::next(self).await
    }
}

impl std::fmt::Debug for DirectAcceptStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectAcceptStream")
            .field("finished", &self.is_finished())
            .finish()
    }
}

/// Connections accepted by a multishot accept, see [`TcpListener::accept_multi`].
///
/// Connections the kernel accepted while nobody polls are queued (without bound). Dropping the
/// stream cancels the accept and closes every connection accepted but not taken, including those
/// the kernel posts until the cancel takes effect.
///
/// An error ends the stream if the kernel ends the multishot with it (most errors do); keep
/// calling `next` until it returns `None`, then call `accept_multi` again to re-arm.
#[must_use = "streams do nothing unless polled"]
pub struct AcceptStream {
    op: AcceptMultiOp,
}

impl AcceptStream {
    /// Poll for the next connection, without its peer address.
    pub fn poll_next_stream(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<TcpStream>>> {
        let next = ready!(self.op.poll_next_fd(cx));
        Poll::Ready(next.map(|fd| fd.map(TcpStream::from_shared_fd)))
    }

    /// The next connection, without its peer address (no extra syscall). `None` once the
    /// accept ended (cancelled, or after an error that ended it).
    pub async fn next_stream(&mut self) -> Option<io::Result<TcpStream>> {
        std::future::poll_fn(|cx| self.poll_next_stream(cx)).await
    }

    /// Poll for the next connection and its peer address (one `getpeername` syscall).
    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<(TcpStream, SocketAddr)>>> {
        let next = ready!(self.poll_next_stream(cx));
        Poll::Ready(next.map(|stream| {
            let stream = stream?;
            // Fails if the peer is already gone (ENOTCONN); the stream is closed then.
            let addr = stream.peer_addr()?;
            Ok((stream, addr))
        }))
    }

    /// The next connection and its peer address (one `getpeername` syscall). `None` once the
    /// accept ended.
    pub async fn next(&mut self) -> Option<io::Result<(TcpStream, SocketAddr)>> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Stop accepting (AsyncCancel). Connections already accepted are still returned, then
    /// `next` returns `None`.
    pub fn cancel(&self) {
        self.op.cancel()
    }

    /// Whether the accept ended (`next` returns `None` from now on).
    pub fn is_finished(&self) -> bool {
        self.op.is_finished()
    }
}

impl Stream for AcceptStream {
    type Item = io::Result<(TcpStream, SocketAddr)>;

    async fn next(&mut self) -> Option<Self::Item> {
        AcceptStream::next(self).await
    }
}

impl std::fmt::Debug for AcceptStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptStream")
            .field("finished", &self.is_finished())
            .finish()
    }
}

impl TcpStream {
    /// Start a multishot recv (Linux 6.0+) into buffers of `ring`: one submission receives
    /// until EOF, an error, or until the returned [`RecvStream`] is dropped or cancelled.
    ///
    /// Each item is a [`SelectedBuf`] holding one received chunk; drop it (or
    /// [`detach`](SelectedBuf::detach) it) to give the buffer back to the ring. Do not mix it
    /// with other reads on the same socket: data would be split between them in kernel order.
    pub fn recv_multi(&self, ring: &BufRing) -> io::Result<RecvStream> {
        RecvMultiOp::new(&self.fd, ring, false).map(RecvStream::new)
    }

    /// Like [`recv_multi`](Self::recv_multi), with bundles (`IORING_RECVSEND_BUNDLE`, Linux
    /// 6.10+): each item is a [`Bundle`] of one or more buffers holding one received chunk, so a
    /// large burst costs one completion instead of one per buffer.
    ///
    /// Fails with `Unsupported` if the kernel has no bundles
    /// ([`BufRing::supports_bundles`]). Works with incremental rings too.
    pub fn recv_multi_bundle(&self, ring: &BufRing) -> io::Result<RecvBundleStream> {
        RecvMultiOp::new(&self.fd, ring, true).map(RecvBundleStream::new)
    }

    /// Receive once into one or more buffers of `ring` (single-shot bundle recv, Linux 6.10+).
    /// An empty [`Bundle`] means EOF. `ENOBUFS` if the ring has no free buffer; `Unsupported`
    /// if the kernel has no bundles.
    ///
    /// Await it to completion: dropping it midway can leak the buffers of a receive that
    /// completes anyway.
    pub async fn recv_bundle(&self, ring: &BufRing) -> io::Result<Bundle> {
        recv_multi::recv_bundle(&self.fd, ring).await
    }
}

/// Buffers received by a multishot recv, see [`TcpStream::recv_multi`] (and
/// `UnixStream::recv_multi`).
///
/// - EOF (a 0-length completion) ends the stream: `next` returns `None`.
/// - `ENOBUFS`: the ring had no free buffer. The kernel ends the multishot, so `next` returns
///   `Err(ENOBUFS)` once and then `None`. Release buffers, then call `recv_multi` again to
///   re-arm; no data is lost.
/// - Chunks received while nobody polls are queued (without bound, but each holds a ring
///   buffer, so the ring size bounds them in practice).
/// - If the kernel ends the multishot after a chunk without an error (a bundle recv does that
///   whenever it used the last free buffer), the stream submits it again on the next poll.
///
/// Dropping the stream cancels the recv and gives every buffer not taken back to the ring.
#[must_use = "streams do nothing unless polled"]
pub struct RecvStream {
    op: RecvMultiOp,
}

impl RecvStream {
    pub(crate) fn new(op: RecvMultiOp) -> Self {
        Self { op }
    }

    /// Poll for the next received buffer.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<SelectedBuf>>> {
        self.op.poll_next_buf(cx)
    }

    /// The next received buffer; `None` after EOF, cancel, or an error that ended the recv.
    pub async fn next(&mut self) -> Option<io::Result<SelectedBuf>> {
        std::future::poll_fn(|cx| self.op.poll_next_buf(cx)).await
    }

    /// Stop receiving (AsyncCancel). Buffers already received are still returned, then `next`
    /// returns `None`.
    pub fn cancel(&self) {
        self.op.cancel()
    }

    /// Whether the recv ended (`next` returns `None` from now on).
    pub fn is_finished(&self) -> bool {
        self.op.is_finished()
    }
}

impl Stream for RecvStream {
    type Item = io::Result<SelectedBuf>;

    async fn next(&mut self) -> Option<Self::Item> {
        RecvStream::next(self).await
    }
}

impl std::fmt::Debug for RecvStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvStream")
            .field("finished", &self.is_finished())
            .finish()
    }
}
