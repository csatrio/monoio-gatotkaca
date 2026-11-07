//! Reply-then-receive as one operation on Unix stream sockets, see `net/tcp/pair.rs`.

use super::UnixStream;
use crate::{
    buf::{IoBuf, IoBufMut},
    driver::op::send_recv::{SendRecv, SendRecvFixed},
    net::{MsgFlags, SendThenRecv, SendThenRecvFixed},
    uring::FixedBuf,
};

impl UnixStream {
    /// Send `reply`, then receive into `buf`, as one linked pair with one op slot and one
    /// CQE. See [`TcpStream::send_then_recv`](crate::net::TcpStream::send_then_recv) for the
    /// results and the rules; they are the same here.
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

    /// [`send_then_recv`](Self::send_then_recv) with the receive into a registered buffer,
    /// see [`TcpStream::send_then_recv_fixed`](crate::net::TcpStream::send_then_recv_fixed).
    #[inline]
    pub fn send_then_recv_fixed<S: IoBuf>(&self, reply: S, buf: FixedBuf) -> SendThenRecvFixed<S> {
        SendThenRecvFixed::submit(SendRecvFixed::new(self.fd.clone(), reply, buf))
    }
}
