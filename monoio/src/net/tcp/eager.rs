//! Sends and receives that bypass the ring when they can: a direct, non-blocking syscall from
//! the calling thread ([`TcpStream::try_send`], [`TcpStream::try_recv`]), and a send that uses
//! the ring only for what the direct call did not take ([`TcpStream::send_eager`]).
//!
//! Why: a ring send is an SQE that reaches the kernel at the next `io_uring_enter`, behind the
//! other connections' work of that round. A small reply sent with `send(2)` from the task poll
//! that handled the request leaves at once, and costs no op slot, no SQE, no CQE and no task
//! wake. The rules are in the docs of [`TcpStream::try_send`].

use std::io::{self, IoSlice};

use super::TcpStream;
use crate::{buf::IoBuf, driver::op::send::send_eager, io::futures::SendFuture, net::MsgFlags};

impl TcpStream {
    /// One non-blocking `send(2)` on the calling thread (`MSG_DONTWAIT | MSG_NOSIGNAL`), with
    /// no op slot, no SQE and no CQE.
    ///
    /// - `Ok(n)`: the kernel took `n` bytes, which may be fewer than `buf` holds.
    /// - `Err(WouldBlock)`: the socket buffer is full, **or** a ring send of this stream is
    ///   still in flight (a direct send would overtake it), **or** the stream is direct-only
    ///   ([`is_direct`](Self::is_direct): no fd). Send through the ring then (`send_op`,
    ///   `write`), or use [`send_eager`](Self::send_eager), which does both steps.
    /// - Any other error is the socket's (`EPIPE`, `ECONNRESET`, ...). `EINTR` is retried.
    ///
    /// A ring send is an SQE that reaches the kernel at the next `io_uring_enter`, behind the
    /// other connections' work of that round. A direct send from the task poll that handled
    /// the request leaves at once.
    ///
    /// # Rules
    ///
    /// These hold for `try_send`, [`try_send_vectored`](Self::try_send_vectored),
    /// [`try_recv`](Self::try_recv), [`send_eager`](Self::send_eager) and
    /// [`set_eager_send`](Self::set_eager_send).
    ///
    /// - **Ordering.** The stream counts its ring sends and receives in flight, those of split
    ///   halves included. While the count is not zero, the `try_*` calls return `WouldBlock`
    ///   and `send_eager` uses the ring. The count can be late, never early: an op whose
    ///   completion its task has not seen yet still counts, a dropped op counts until the
    ///   kernel finished it, and a zero-copy send counts until the kernel released its buffer.
    /// - **Not counted:** SQEs you build yourself for the same socket ([`uring::submit`],
    ///   [`uring::submit_chain`], the CQE hook). Do not mix them with these calls.
    /// - **Size.** The direct call is wasted when the socket buffer is usually full: a bulk
    ///   writer pays a syscall that returns `EAGAIN`, and then the ring send anyway. These
    ///   calls are meant for replies of up to a few KiB.
    /// - **Ring flags.** On a `coop_taskrun` ring every syscall of the thread also runs the
    ///   ring's pending task work on its way out, so each direct send does a piece of the next
    ///   `io_uring_enter`'s work, unbatched; throughput drops (the transport benchmark saw
    ///   −5%). Use these calls on `defer_taskrun` rings, or on rings without task-run flags.
    ///
    /// [`uring::submit`]: crate::uring::submit
    /// [`uring::submit_chain`]: crate::uring::submit_chain
    ///
    /// ```no_run
    /// # async fn reply(stream: &monoio::net::TcpStream, reply: &'static [u8]) -> std::io::Result<()> {
    /// match stream.try_send(reply) {
    ///     Ok(n) if n == reply.len() => {}
    ///     Ok(n) => stream.send_op(&reply[n..]).await.0.map(drop)?,
    ///     Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
    ///         stream.send_op(reply).await.0.map(drop)?
    ///     }
    ///     Err(e) => return Err(e),
    /// }
    /// # Ok(()) }
    /// ```
    #[inline]
    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        self.fd.try_send(buf)
    }

    /// [`try_send`](Self::try_send) for several buffers: one non-blocking `sendmsg(2)`
    /// (`MSG_DONTWAIT | MSG_NOSIGNAL`). `Ok(n)` counts across the buffers and may be short;
    /// one call sends at most 1024 buffers (`IOV_MAX`). Same `WouldBlock` cases and
    /// [rules](Self::try_send).
    #[inline]
    pub fn try_send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.fd.try_send_vectored(bufs)
    }

    /// One non-blocking `recv(2)` on the calling thread (`MSG_DONTWAIT`), with no op slot, no
    /// SQE and no CQE. The receive twin of [`try_send`](Self::try_send), e.g. for a
    /// request/response client that expects the reply to be there already.
    ///
    /// - `Ok(n)`: `n` bytes were copied to the start of `buf`; `Ok(0)` is EOF (or an empty
    ///   `buf`).
    /// - `Err(WouldBlock)`: no data is queued, **or** a ring receive of this stream is in
    ///   flight (`recv_op`, `read`, `readv`, `recv_fixed`: the two would race for the same
    ///   bytes), **or** the stream is direct-only.
    ///
    /// See the [rules](Self::try_send) of `try_send`: SQEs you submit yourself for this
    /// socket are not counted.
    #[inline]
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.fd.try_recv(buf)
    }

    /// Send `buf` with a direct `send(2)` first ([`try_send`](Self::try_send)), and through the
    /// ring only what that did not take. Returns the same [`SendFuture`] as
    /// [`send_op`](Self::send_op), with the same result: the bytes sent, counted from the
    /// start of `buf`, which may be fewer than it holds.
    ///
    /// - **Everything was sent** (the usual case for a small reply), or the send failed with
    ///   a real error: the future is resolved already. Its first poll returns `Ready`, so a
    ///   request whose reply fits finishes without the future ever being `Pending` (and a
    ///   `time::timeout` around it never registers its timer).
    /// - **The socket buffer is full** (`EAGAIN`, or a short count): the rest goes out as a
    ///   ring `SEND` with `IORING_RECVSEND_POLL_FIRST` (5.19+; left out on older kernels),
    ///   since trying again at once would only fail again.
    /// - **A ring send is in flight** on this stream, or the stream is **direct-only**: a
    ///   plain ring `SEND`, exactly [`send_op`](Self::send_op).
    ///
    /// If the ring part fails (or is cancelled) after the direct part sent something, the
    /// result is `Ok` with the count of the direct part: a short send, like `send(2)`, which
    /// reports such an error on the next call. An empty buffer behaves like a plain send of
    /// an empty buffer.
    ///
    /// Both the direct call and the push of the SQE happen when this method is called, not at
    /// the first poll. See the [rules](Self::try_send) of `try_send`: raw SQEs on the
    /// same socket are not counted, large writes that usually fill the socket buffer pay a
    /// wasted syscall, and `coop_taskrun` rings lose throughput with it.
    #[inline]
    pub fn send_eager<T: IoBuf>(&self, buf: T) -> SendFuture<T> {
        send_eager(&self.fd, buf, 0)
    }

    /// [`send_eager`](Self::send_eager) with extra `MSG_*` flags (e.g. [`MsgFlags::MORE`]) on
    /// both the direct send and the ring send. `MSG_NOSIGNAL` is always added.
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
    /// see it, whether it is set before or after the split. The `_op` methods,
    /// `send_with_flags`, `send_vectored`, the zero-copy sends and the `cancelable_*` methods
    /// are not affected.
    ///
    /// See the [rules](Self::try_send) of `try_send` for when to turn it on:
    /// connections that write small replies, on a `defer_taskrun` ring or a ring without
    /// task-run flags.
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
