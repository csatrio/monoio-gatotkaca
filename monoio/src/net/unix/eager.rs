//! Sends and receives that bypass the ring when they can, for Unix streams. Same calls and
//! rules as for TCP: see [`TcpStream::try_send`](crate::net::TcpStream::try_send).

use std::io::{self, IoSlice};

use super::UnixStream;
use crate::{buf::IoBuf, driver::op::send::send_eager, io::futures::SendFuture, net::MsgFlags};

impl UnixStream {
    /// One non-blocking `send(2)` on the calling thread (`MSG_DONTWAIT | MSG_NOSIGNAL`), with
    /// no op slot, no SQE and no CQE.
    ///
    /// - `Ok(n)`: the kernel took `n` bytes, which may be fewer than `buf` holds.
    /// - `Err(WouldBlock)`: the socket buffer is full, **or** a ring send of this stream is
    ///   still in flight (a direct send would overtake it). Send through the ring then
    ///   (`send_op`, `write`), or use [`send_eager`](Self::send_eager), which does both steps.
    /// - Any other error is the socket's. `EINTR` is retried.
    ///
    /// The [rules](crate::net::TcpStream::try_send) of `TcpStream::try_send` apply: SQEs
    /// you submit yourself for this socket are not counted as in flight (do not mix them with
    /// this call), the direct call is meant for small replies, and for `defer_taskrun` rings
    /// or rings without task-run flags.
    #[inline]
    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        self.fd.try_send(buf)
    }

    /// [`try_send`](Self::try_send) for several buffers: one non-blocking `sendmsg(2)`
    /// (`MSG_DONTWAIT | MSG_NOSIGNAL`). `Ok(n)` counts across the buffers and may be short;
    /// one call sends at most 1024 buffers (`IOV_MAX`). Same `WouldBlock` cases.
    #[inline]
    pub fn try_send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.fd.try_send_vectored(bufs)
    }

    /// One non-blocking `recv(2)` on the calling thread (`MSG_DONTWAIT`), with no op slot, no
    /// SQE and no CQE.
    ///
    /// - `Ok(n)`: `n` bytes were copied to the start of `buf`; `Ok(0)` is EOF (or an empty
    ///   `buf`).
    /// - `Err(WouldBlock)`: no data is queued, **or** a ring receive of this stream is in
    ///   flight (the two would race for the same bytes).
    ///
    /// SQEs you submit yourself for this socket are not counted: do not mix them with this
    /// call.
    #[inline]
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.fd.try_recv(buf)
    }

    /// Send `buf` with a direct `send(2)` first ([`try_send`](Self::try_send)), and through the
    /// ring only what that did not take. Returns the same [`SendFuture`] as
    /// [`send_op`](Self::send_op), with the same result: the bytes sent, counted from the
    /// start of `buf`, which may be fewer than it holds.
    ///
    /// - Everything was sent, or the send failed with a real error: the future is resolved
    ///   already; its first poll returns `Ready`.
    /// - The socket buffer is full (`EAGAIN`, or a short count): the rest goes out as a ring
    ///   `SEND` with `IORING_RECVSEND_POLL_FIRST` (5.19+; left out on older kernels).
    /// - A ring send is in flight on this stream: a plain ring `SEND`, exactly
    ///   [`send_op`](Self::send_op).
    ///
    /// If the ring part fails (or is cancelled) after the direct part sent something, the
    /// result is `Ok` with the count of the direct part (a short send). See
    /// [`TcpStream::send_eager`](crate::net::TcpStream::send_eager) and the
    /// [rules](crate::net::TcpStream::try_send).
    #[inline]
    pub fn send_eager<T: IoBuf>(&self, buf: T) -> SendFuture<T> {
        send_eager(&self.fd, buf, 0)
    }

    /// [`send_eager`](Self::send_eager) with extra `MSG_*` flags on both the direct send and
    /// the ring send. `MSG_NOSIGNAL` is always added.
    #[inline]
    pub fn send_eager_with_flags<T: IoBuf>(&self, buf: T, flags: MsgFlags) -> SendFuture<T> {
        send_eager(&self.fd, buf, flags.bits())
    }

    /// Make [`AsyncWriteRent::write`](crate::io::AsyncWriteRent::write) (and with it
    /// `write_all`) go through [`send_eager`](Self::send_eager), and
    /// [`writev`](crate::io::AsyncWriteRent::writev) try one direct `sendmsg(2)` before it
    /// uses the ring (what the direct call took is returned, possibly as a short write). Off
    /// by default.
    ///
    /// The switch belongs to the socket, not to this handle: the halves made by `into_split`
    /// see it, whether it is set before or after the split. See
    /// [`TcpStream::set_eager_send`](crate::net::TcpStream::set_eager_send).
    #[inline]
    pub fn set_eager_send(&self, on: bool) {
        self.fd.set_eager_send(on);
    }

    /// Whether [`set_eager_send`](Self::set_eager_send) is on.
    #[inline]
    pub fn eager_send(&self) -> bool {
        self.fd.eager_send()
    }
}
