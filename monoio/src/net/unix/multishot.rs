//! Multishot accept and recv for Unix stream sockets.

use std::{
    io,
    task::{Context, Poll},
};

use super::{SocketAddr, UnixListener, UnixStream};
use crate::{
    driver::{
        op::{
            accept_multi::AcceptMultiOp,
            recv_multi::{self, Bundle, RecvBundleStream, RecvMultiOp},
        },
        raw::BufRing,
        shared_fd::SharedFd,
    },
    io::stream::Stream,
    net::{tcp::RecvStream, AcceptFlags},
};

impl UnixListener {
    /// Start a multishot accept (Linux 5.19+), see
    /// [`TcpListener::accept_multi`](crate::net::TcpListener::accept_multi).
    ///
    /// [`UnixAcceptStream::next`] gets the peer address with one `getpeername` syscall per
    /// connection; [`next_stream`](UnixAcceptStream::next_stream) skips it.
    pub fn accept_multi(&self, flags: AcceptFlags) -> io::Result<UnixAcceptStream> {
        AcceptMultiOp::new(&self.fd, flags.bits()).map(|op| UnixAcceptStream { op })
    }

    /// Wait for at least one connection, then return every connection already accepted (up to
    /// `max`) from one poll, with a multishot accept kept armed in the listener. Same rules as
    /// [`TcpListener::accept_batch`](crate::net::TcpListener::accept_batch).
    pub async fn accept_batch(
        &self,
        max: usize,
        flags: AcceptFlags,
    ) -> io::Result<Vec<(UnixStream, SocketAddr)>> {
        let mut dst = Vec::new();
        self.accept_batch_into(&mut dst, max, flags).await?;
        Ok(dst)
    }

    /// [`accept_batch`](Self::accept_batch) appending to `dst`; returns how many were
    /// appended (at least 1).
    pub async fn accept_batch_into(
        &self,
        dst: &mut Vec<(UnixStream, SocketAddr)>,
        max: usize,
        flags: AcceptFlags,
    ) -> io::Result<usize> {
        self.batch
            .accept(&self.fd, flags.bits(), max, |fd| push_with_addr(dst, fd))
            .await
    }
}

/// Push a new connection with its peer address; skip (close) it if `getpeername` fails.
fn push_with_addr(dst: &mut Vec<(UnixStream, SocketAddr)>, fd: SharedFd) -> bool {
    let stream = UnixStream::from_shared_fd(fd);
    match stream.peer_addr() {
        Ok(addr) => {
            dst.push((stream, addr));
            true
        }
        Err(_) => false,
    }
}

/// Connections accepted by [`UnixListener::accept_multi`]. Behaves like
/// [`AcceptStream`](crate::net::tcp::AcceptStream).
#[must_use = "streams do nothing unless polled"]
pub struct UnixAcceptStream {
    op: AcceptMultiOp,
}

impl UnixAcceptStream {
    /// Wait for at least one connection, then append every connection already accepted (up
    /// to `max`) with its peer address to `dst` in one poll, see
    /// [`AcceptStream::next_batch`](crate::net::tcp::AcceptStream::next_batch).
    pub async fn next_batch(
        &mut self,
        dst: &mut Vec<(UnixStream, SocketAddr)>,
        max: usize,
    ) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.op.poll_batch(cx, max, |fd| push_with_addr(dst, fd))).await
    }

    /// Like [`next_batch`](Self::next_batch), without peer addresses.
    pub async fn next_stream_batch(
        &mut self,
        dst: &mut Vec<UnixStream>,
        max: usize,
    ) -> io::Result<usize> {
        std::future::poll_fn(|cx| {
            self.op.poll_batch(cx, max, |fd| {
                dst.push(UnixStream::from_shared_fd(fd));
                true
            })
        })
        .await
    }

    /// Poll for the next connection, without its peer address.
    pub fn poll_next_stream(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<UnixStream>>> {
        let next = ready!(self.op.poll_next_fd(cx));
        Poll::Ready(next.map(|fd| fd.map(UnixStream::from_shared_fd)))
    }

    /// The next connection, without its peer address. `None` once the accept ended.
    pub async fn next_stream(&mut self) -> Option<io::Result<UnixStream>> {
        std::future::poll_fn(|cx| self.poll_next_stream(cx)).await
    }

    /// Poll for the next connection and its peer address (one `getpeername` syscall).
    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<(UnixStream, SocketAddr)>>> {
        let next = ready!(self.poll_next_stream(cx));
        Poll::Ready(next.map(|stream| {
            let stream = stream?;
            let addr = stream.peer_addr()?;
            Ok((stream, addr))
        }))
    }

    /// The next connection and its peer address. `None` once the accept ended.
    pub async fn next(&mut self) -> Option<io::Result<(UnixStream, SocketAddr)>> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Stop accepting (AsyncCancel). Keep calling `next` until it returns `None`.
    pub fn cancel(&self) {
        self.op.cancel()
    }

    /// Whether the accept ended (`next` returns `None` from now on).
    pub fn is_finished(&self) -> bool {
        self.op.is_finished()
    }
}

impl Stream for UnixAcceptStream {
    type Item = io::Result<(UnixStream, SocketAddr)>;

    async fn next(&mut self) -> Option<Self::Item> {
        UnixAcceptStream::next(self).await
    }
}

impl std::fmt::Debug for UnixAcceptStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixAcceptStream")
            .field("finished", &self.is_finished())
            .finish()
    }
}

impl UnixStream {
    /// Start a multishot recv (Linux 6.0+) into buffers of `ring`, see
    /// [`TcpStream::recv_multi`](crate::net::TcpStream::recv_multi).
    pub fn recv_multi(&self, ring: &BufRing) -> io::Result<RecvStream> {
        RecvMultiOp::new(&self.fd, ring, false).map(RecvStream::new)
    }

    /// Start a multishot bundle recv (Linux 6.10+) into buffers of `ring`, see
    /// [`TcpStream::recv_multi_bundle`](crate::net::TcpStream::recv_multi_bundle).
    pub fn recv_multi_bundle(&self, ring: &BufRing) -> io::Result<RecvBundleStream> {
        RecvMultiOp::new(&self.fd, ring, true).map(RecvBundleStream::new)
    }

    /// Receive once into one or more buffers of `ring` (Linux 6.10+), see
    /// [`TcpStream::recv_bundle`](crate::net::TcpStream::recv_bundle).
    pub async fn recv_bundle(&self, ring: &BufRing) -> io::Result<Bundle> {
        recv_multi::recv_bundle(&self.fd, ring).await
    }
}
