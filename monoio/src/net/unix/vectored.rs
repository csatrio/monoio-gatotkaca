//! Vectored sends as named futures: [`UnixStream::send_vectored_op`] and
//! [`UnixStream::send_vectored_with_flags_op`]. See the TCP counterpart
//! (`net/tcp/vectored.rs`) for the rules; they are the same here.

use super::UnixStream;
use crate::{buf::IoVecBuf, driver::op::Op, io::futures::SendVectoredFuture, net::MsgFlags};

impl UnixStream {
    /// Starts a vectored send of `bufs` (with `MSG_NOSIGNAL`) and returns it as a
    /// named [`SendVectoredFuture`]. Same SQE and result as
    /// [`send_vectored`](Self::send_vectored); see
    /// [`TcpStream::send_vectored_op`](crate::net::TcpStream::send_vectored_op).
    #[inline]
    pub fn send_vectored_op<T: IoVecBuf>(&self, bufs: T) -> SendVectoredFuture<T> {
        SendVectoredFuture::new(Op::send_msg_vec(self.fd.clone(), bufs, 0))
    }

    /// [`send_vectored_with_flags`](Self::send_vectored_with_flags) as a named
    /// [`SendVectoredFuture`].
    #[inline]
    pub fn send_vectored_with_flags_op<T: IoVecBuf>(
        &self,
        bufs: T,
        flags: MsgFlags,
    ) -> SendVectoredFuture<T> {
        SendVectoredFuture::new(Op::send_msg_vec(self.fd.clone(), bufs, flags.bits()))
    }
}
