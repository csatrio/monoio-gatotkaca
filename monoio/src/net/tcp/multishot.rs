//! Multishot accept and recv for TCP.

use std::{
    io,
    net::SocketAddr,
    task::{Context, Poll},
};

use super::{TcpListener, TcpStream};
use crate::{
    buf::{IoBufMut, IoVecBufMut},
    driver::{
        op::{
            accept_multi::AcceptMultiOp,
            recv_multi::{self, Bundle, OnEnobufs, RecvBundleStream, RecvMultiOp},
        },
        raw::{BufRing, SelectedBuf},
        shared_fd::SharedFd,
    },
    io::{stream::Stream, AsyncReadRent},
    net::AcceptFlags,
    BufResult,
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
/// When the kernel ends the multishot after a connection without an error (it does when it
/// can't post another completion, e.g. on a full CQ during a burst), the stream re-arms itself
/// on the next poll: no connection is lost and the caller sees nothing. An error ends the
/// stream if the kernel ends the multishot with it (most errors do, e.g. `EMFILE`); keep calling
/// `next` until it returns `None`, then call `accept_multi` again to re-arm.
///
/// For a server loop, [`next_batch`](Self::next_batch) takes every connection already queued
/// in one poll (see "Writing an accept loop" on [`TcpListener`]).
#[must_use = "streams do nothing unless polled"]
pub struct AcceptStream {
    op: AcceptMultiOp,
}

impl AcceptStream {
    /// Wait for at least one connection, then append every connection already accepted, up to
    /// `max` (at least 1), to `dst`, with its peer address (one `getpeername` syscall each), in
    /// one poll. Returns how many were appended; `Ok(0)` only once the stream ended.
    ///
    /// A connection whose peer is already gone (`getpeername` fails) is closed and skipped. An
    /// error after some connections is returned by the next call. `dst` is not cleared: reuse
    /// one `Vec` (drain it) to accept without allocating.
    pub async fn next_batch(
        &mut self,
        dst: &mut Vec<(TcpStream, SocketAddr)>,
        max: usize,
    ) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_next_batch(cx, dst, max)).await
    }

    /// Poll form of [`next_batch`](Self::next_batch).
    pub fn poll_next_batch(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut Vec<(TcpStream, SocketAddr)>,
        max: usize,
    ) -> Poll<io::Result<usize>> {
        self.op.poll_batch(cx, max, |fd| push_with_addr(dst, fd))
    }

    /// Like [`next_batch`](Self::next_batch), without peer addresses (no syscall per
    /// connection).
    pub async fn next_stream_batch(
        &mut self,
        dst: &mut Vec<TcpStream>,
        max: usize,
    ) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_next_stream_batch(cx, dst, max)).await
    }

    /// Poll form of [`next_stream_batch`](Self::next_stream_batch).
    pub fn poll_next_stream_batch(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut Vec<TcpStream>,
        max: usize,
    ) -> Poll<io::Result<usize>> {
        self.op.poll_batch(cx, max, |fd| {
            dst.push(TcpStream::from_shared_fd(fd));
            true
        })
    }

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
/// For a framed read loop, [`read_into`](Self::read_into) copies into a `BytesMut` and
/// re-arms after `ENOBUFS` itself; [`MultiRecvReader`] wraps the stream as an
/// [`AsyncReadRent`].
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
        self.op.poll_next_buf(cx, OnEnobufs::Yield)
    }

    /// The next received buffer; `None` after EOF, cancel, or an error that ended the recv.
    pub async fn next(&mut self) -> Option<io::Result<SelectedBuf>> {
        std::future::poll_fn(|cx| self.op.poll_next_buf(cx, OnEnobufs::Yield)).await
    }

    /// Append what was received to `dst` and give the buffers back to the ring at once.
    ///
    /// Waits for the first chunk, then also takes every chunk already queued (no waiting), so
    /// one call may copy several ring buffers. Returns the bytes appended; `Ok(0)` at EOF (and
    /// after a cancel, or once an error that ended the recv was returned). `dst` grows as
    /// needed (`reserve` it to the frame size you expect).
    ///
    /// - **Costs:** one copy per byte (kernel → ring buffer → `dst`); in exchange there is no
    ///   SQE per read and no buffer of yours parked in the kernel per idle connection.
    /// - **Ring exhaustion:** when the kernel ends the recv with `ENOBUFS` (the ring ran dry),
    ///   `read_into` re-arms it and continues: the buffers it copied out are free again, and no
    ///   data is lost. Only if the re-armed recv finds the ring still empty (someone else holds
    ///   its buffers) is `ENOBUFS` returned; the next call re-arms again.
    /// - An error after some data is returned by the next call.
    /// - Don't mix it with other reads on the socket. Mixing with [`next`](Self::next) is
    ///   fine (`next` does not re-arm after `ENOBUFS`).
    #[cfg(feature = "bytes")]
    pub async fn read_into(&mut self, dst: &mut bytes::BytesMut) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.op.poll_read_into(cx, dst)).await
    }

    /// Poll form of [`read_into`](Self::read_into).
    #[cfg(feature = "bytes")]
    pub fn poll_read_into(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut bytes::BytesMut,
    ) -> Poll<io::Result<usize>> {
        self.op.poll_read_into(cx, dst)
    }

    /// Wrap the stream as an [`AsyncReadRent`] reader, see [`MultiRecvReader`].
    pub fn into_reader(self) -> MultiRecvReader {
        MultiRecvReader::new(self)
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

/// An [`AsyncReadRent`] over a multishot recv ([`RecvStream`]), so code written against
/// `read(buf)` can use a provided buffer ring without changes.
///
/// `read`/`readv` copy from the received ring buffers into your buffer: first what is left of
/// the last ring buffer, then every chunk already queued, waiting only if nothing is there.
/// A ring buffer that was copied out completely goes back to the ring at once; the reader
/// holds at most one partly read buffer between calls.
///
/// - **Costs:** one copy per byte, like [`RecvStream::read_into`]. The win is in the kernel:
///   one SQE for the whole connection instead of one per read, and idle connections hold no
///   buffer of yours.
/// - **Ring exhaustion:** re-arms after `ENOBUFS` like [`RecvStream::read_into`]; `ENOBUFS`
///   is returned only if the ring is still empty after a re-arm.
/// - `Ok(0)` at EOF (and after a cancel). An error after some data is returned by the next
///   read.
/// - Don't mix it with other reads on the same socket (data would be split between them in
///   kernel order).
/// - Dropping a `read` future midway loses nothing: data is only taken out of the reader
///   synchronously, when the read completes.
pub struct MultiRecvReader {
    stream: RecvStream,
    // The ring buffer being read, and how much of it was returned.
    leftover: Option<SelectedBuf>,
    offset: usize,
    pending_err: Option<io::Error>,
}

impl MultiRecvReader {
    /// Wrap `stream`.
    pub fn new(stream: RecvStream) -> Self {
        Self {
            stream,
            leftover: None,
            offset: 0,
            pending_err: None,
        }
    }

    /// The received bytes not returned by a read yet (the rest of one ring buffer).
    pub fn buffered(&self) -> &[u8] {
        match &self.leftover {
            Some(buf) => &buf[self.offset..],
            None => &[],
        }
    }

    /// The stream back. Bytes still [`buffered`](Self::buffered) are dropped with their ring
    /// buffer: read them first.
    pub fn into_inner(self) -> RecvStream {
        self.stream
    }

    /// The underlying stream, e.g. to [`cancel`](RecvStream::cancel) it.
    pub fn get_ref(&self) -> &RecvStream {
        &self.stream
    }

    /// Copy up to `len` bytes to `dst`; see the type docs.
    ///
    /// # Safety
    /// `dst` must be valid for `len` bytes of writes.
    unsafe fn poll_copy(
        &mut self,
        cx: &mut Context<'_>,
        dst: *mut u8,
        len: usize,
    ) -> Poll<io::Result<usize>> {
        let mut n = 0;
        while n < len {
            if let Some(buf) = &self.leftover {
                let src = &buf[self.offset..];
                let k = src.len().min(len - n);
                // SAFETY: the caller's contract, `n + k <= len`.
                unsafe { dst.add(n).copy_from_nonoverlapping(src.as_ptr(), k) };
                n += k;
                self.offset += k;
                if self.offset == buf.len() {
                    // Copied out completely: back to the ring now.
                    self.leftover = None;
                    self.offset = 0;
                }
                continue;
            }
            if n == 0 {
                if let Some(e) = self.pending_err.take() {
                    return Poll::Ready(Err(e));
                }
            }
            match self.stream.op.poll_next_buf(cx, OnEnobufs::Rearm) {
                Poll::Pending if n == 0 => return Poll::Pending,
                Poll::Pending | Poll::Ready(None) => break,
                Poll::Ready(Some(Err(e))) => {
                    if n == 0 {
                        return Poll::Ready(Err(e));
                    }
                    self.pending_err = Some(e);
                    break;
                }
                Poll::Ready(Some(Ok(buf))) => {
                    self.leftover = Some(buf);
                    self.offset = 0;
                }
            }
        }
        Poll::Ready(Ok(n))
    }
}

impl AsyncReadRent for MultiRecvReader {
    async fn read<T: IoBufMut>(&mut self, mut buf: T) -> BufResult<usize, T> {
        let res = std::future::poll_fn(|cx| {
            let len = buf.bytes_total();
            if len == 0 {
                return Poll::Ready(Ok(0));
            }
            // SAFETY: `write_ptr` is valid for `bytes_total` bytes.
            unsafe { self.poll_copy(cx, buf.write_ptr(), len) }
        })
        .await;
        if let Ok(n) = res {
            // SAFETY: `n` bytes were written from the start.
            unsafe { buf.set_init(n) };
        }
        (res, buf)
    }

    async fn readv<T: IoVecBufMut>(&mut self, mut buf: T) -> BufResult<usize, T> {
        let res = std::future::poll_fn(|cx| {
            // SAFETY: the iovecs describe writable memory owned by `buf`.
            let iovs =
                unsafe { std::slice::from_raw_parts(buf.write_iovec_ptr(), buf.write_iovec_len()) };
            let mut total = 0;
            for iov in iovs {
                if iov.iov_len == 0 {
                    continue;
                }
                // SAFETY: see above.
                match unsafe { self.poll_copy(cx, iov.iov_base.cast(), iov.iov_len) } {
                    Poll::Ready(Ok(k)) => {
                        total += k;
                        if k < iov.iov_len {
                            break;
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        if total == 0 {
                            return Poll::Ready(Err(e));
                        }
                        self.pending_err = Some(e);
                        break;
                    }
                    Poll::Pending if total == 0 => return Poll::Pending,
                    Poll::Pending => break,
                }
            }
            Poll::Ready(Ok(total))
        })
        .await;
        if let Ok(n) = res {
            // SAFETY: `n` bytes were written into the iovecs in order.
            unsafe { buf.set_init(n) };
        }
        (res, buf)
    }
}

impl std::fmt::Debug for MultiRecvReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiRecvReader")
            .field("stream", &self.stream)
            .field("buffered", &self.buffered().len())
            .finish()
    }
}

/// Push a new connection with its peer address; a connection whose peer is already gone
/// (`getpeername` fails, typically `ENOTCONN`) is closed and skipped.
pub(super) fn push_with_addr(dst: &mut Vec<(TcpStream, SocketAddr)>, fd: SharedFd) -> bool {
    let stream = TcpStream::from_shared_fd(fd);
    match stream.peer_addr() {
        Ok(addr) => {
            dst.push((stream, addr));
            true
        }
        Err(_) => false,
    }
}
