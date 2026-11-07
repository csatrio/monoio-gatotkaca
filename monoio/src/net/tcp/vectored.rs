//! Vectored sends as named futures: [`TcpStream::send_vectored_op`] and
//! [`TcpStream::send_vectored_with_flags_op`].
//!
//! They are to [`TcpStream::send_vectored`] what `send_op` is to `write`: the same SQE
//! (`sendmsg` with `MSG_NOSIGNAL`) and result, but the method takes `&self`, pushes the SQE at
//! once and returns an `Unpin + 'static` [`SendVectoredFuture`] that a struct can store
//! without `Box::pin`. Unlike `writev_op` (`IORING_OP_WRITEV`) a reset peer gives
//! `EPIPE`/`ECONNRESET` and never `SIGPIPE`. Nothing is allocated per send: the `msghdr` comes
//! from a per-thread free list, and an [`IoVecArray`](crate::buf::IoVecArray) owns its iovec
//! array once, so a connection that keeps one allocates nothing in steady state.

use super::TcpStream;
use crate::{buf::IoVecBuf, driver::op::Op, io::futures::SendVectoredFuture, net::MsgFlags};

impl TcpStream {
    /// Starts a vectored send of `bufs` (`sendmsg` with `MSG_NOSIGNAL`) and returns it as a
    /// named [`SendVectoredFuture`].
    ///
    /// Same SQE and result as [`send_vectored`](Self::send_vectored): it may send fewer bytes
    /// than the buffers hold. The future does not borrow `self`. Also usable as an op token:
    /// drive it with [`SendVectoredFuture::poll_result`].
    ///
    /// ```no_run
    /// # async fn reply(stream: &monoio::net::TcpStream, body: Vec<u8>) -> std::io::Result<()> {
    /// use monoio::buf::IoVecArray;
    /// let mut parts: IoVecArray<Vec<u8>, 3> = IoVecArray::new();
    /// parts.push(b"HTTP/1.1 200 OK\r\n\r\n".to_vec()).ok();
    /// parts.push(body).ok();
    /// let (res, parts) = stream.send_vectored_op(parts).await;
    /// assert_eq!(res?, parts.total_len());
    /// # Ok(()) }
    /// ```
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
