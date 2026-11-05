//! Multishot accept and recv for Unix stream sockets.

use std::{
    io,
    task::{Context, Poll},
};

use super::{SocketAddr, UnixListener, UnixStream};
use crate::{
    driver::{
        op::{accept_multi::AcceptMultiOp, recv_multi::RecvMultiOp},
        raw::BufRing,
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
}

/// Connections accepted by [`UnixListener::accept_multi`]. Behaves like
/// [`AcceptStream`](crate::net::tcp::AcceptStream).
#[must_use = "streams do nothing unless polled"]
pub struct UnixAcceptStream {
    op: AcceptMultiOp,
}

impl UnixAcceptStream {
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
        RecvMultiOp::new(&self.fd, ring).map(RecvStream::new)
    }
}
