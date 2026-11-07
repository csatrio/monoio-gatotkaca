//! Reply-then-receive as one operation: [`TcpStream::send_then_recv`] and
//! [`TcpStream::send_then_recv_fixed`]. The mechanism (one op slot, one CQE, the tag bit, every
//! CQE case) is described in `driver/op/send_recv.rs`.

use super::TcpStream;
use crate::{
    buf::{IoBuf, IoBufMut},
    driver::op::send_recv::{SendRecv, SendRecvFixed},
    net::{MsgFlags, SendThenRecv, SendThenRecvFixed},
    uring::FixedBuf,
};

impl TcpStream {
    /// Send `reply`, then receive into `buf`, as **one operation**: a `SEND` linked to a
    /// `RECV` (`IOSQE_IO_LINK | IOSQE_CQE_SKIP_SUCCESS` on the send, `MSG_WAITALL` so that a
    /// short send fails the link instead of being skipped, `IORING_RECVSEND_POLL_FIRST` on the
    /// receive). The pair takes one op slot, posts one CQE and wakes the task once, where
    /// `send_op` + `recv_poll_first_op` cost two of each. Linux 5.19+.
    ///
    /// The shape of a request/response server: the reply to one frame and the receive of the
    /// next frame, pushed together in the task poll that handled the frame
    /// (`examples/reqresp/paths/pair.rs`). Compared with [`send_eager`](Self::send_eager) +
    /// `recv_poll_first_op`: no syscall in the task poll (the reply leaves at the next
    /// `io_uring_enter` with everything else), and it works on direct-only streams and on
    /// `coop_taskrun` rings without the eager path's cost there; the eager path gets the
    /// reply out sooner on a `defer_taskrun` ring with few connections.
    ///
    /// Resolves to `(send, recv, reply, buf)`:
    ///
    /// - `(Ok(reply.len()), Ok(n), ..)`: the whole reply went out, then `n` bytes were
    ///   received (`0` is EOF). `(Ok(reply.len()), Err(e), ..)`: the reply went out, the
    ///   receive failed.
    /// - `(Err(e), Err(ECANCELED), ..)`: the send failed before anything went out
    ///   (`EPIPE`, `ECONNRESET`, ...); the receive did not run. `e` is `ECANCELED` when the
    ///   receive's SQE was rejected by the kernel.
    /// - `(Ok(n), Err(ECANCELED), ..)` with `n < reply.len()`: **the send failed after `n`
    ///   bytes** (the kernel reports the count, not the errno) and the receive did not run.
    ///   Treat it as a broken connection: the next send reports the error.
    ///
    /// A send that fills the socket is retried in the kernel until it is through (or the peer
    /// goes away); the receive starts only then. One rare outcome of that (a reply larger
    /// than about 512 KiB into a full socket moves the send to a kernel worker, and the
    /// linked receive then ends with `EAGAIN` before it was polled) is hidden: the pair
    /// issues a plain receive and keeps waiting. The method is meant for replies of up to a
    /// few KiB; `MSG_WAITALL` is on the send, `recv_flags` go to the receive (`MSG_NOSIGNAL`
    /// is always on the send).
    ///
    /// The future is `Unpin`, `'static`, does not borrow the stream, and is also an op token
    /// ([`SendThenRecv::poll_result`]). Both SQEs are pushed here. Dropping it in flight
    /// keeps both buffers in the op slot until the pair's CQE (with `async-cancel`, both
    /// members are asked to cancel); [`SendThenRecv::cancel`] and keep polling to get them
    /// back. While it is in flight the stream counts one send and one receive, so
    /// [`try_send`](Self::try_send) and [`try_recv`](Self::try_recv) return `WouldBlock`.
    /// With `coop_budget` the pair is one result. Zero-copy sends cannot be the head of a
    /// pair.
    #[inline]
    pub fn send_then_recv<S: IoBuf, R: IoBufMut>(
        &self,
        reply: S,
        buf: R,
        recv_flags: MsgFlags,
    ) -> SendThenRecv<S, R> {
        SendThenRecv::submit(SendRecv::new(
            self.fd.clone(),
            reply,
            buf,
            recv_flags.bits(),
        ))
    }

    /// [`send_then_recv`](Self::send_then_recv) with the receive into a registered buffer
    /// (`IORING_OP_READ_FIXED` on the socket with `POLL_FIRST`, like
    /// [`recv_fixed`](Self::recv_fixed): no `MSG_*` flags). Same results and rules.
    #[inline]
    pub fn send_then_recv_fixed<S: IoBuf>(&self, reply: S, buf: FixedBuf) -> SendThenRecvFixed<S> {
        SendThenRecvFixed::submit(SendRecvFixed::new(self.fd.clone(), reply, buf))
    }
}
