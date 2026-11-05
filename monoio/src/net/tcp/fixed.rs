//! Socket IO on registered buffers ([`FixedBuf`], [`FrozenFixedBuf`]).
//!
//! [`FrozenFixedBuf`]: crate::uring::FrozenFixedBuf

use super::TcpStream;
use crate::{
    driver::op::{fixed::NO_OFFSET, Op},
    uring::{FixedBuf, FixedIoBuf, ReadFixedFuture, WriteFixedFuture},
    BufResult,
};

impl TcpStream {
    /// Receive into a registered buffer (`IORING_OP_READ_FIXED` on the socket, i.e. a `read`):
    /// fills it from the start up to its capacity and sets its length. `Ok(0)` means EOF.
    #[inline]
    pub async fn recv_fixed(&self, buf: FixedBuf) -> BufResult<usize, FixedBuf> {
        self.recv_fixed_op(buf).await
    }

    /// [`recv_fixed`](Self::recv_fixed) as a named, `Unpin`, `'static` [`ReadFixedFuture`].
    /// The SQE is pushed when this is called.
    #[inline]
    pub fn recv_fixed_op(&self, buf: FixedBuf) -> ReadFixedFuture {
        ReadFixedFuture::new(Op::read_fixed(self.fd.clone(), buf, NO_OFFSET))
    }

    /// Send the bytes of a registered buffer ([`FixedBuf`] or
    /// [`FrozenFixedBuf`](crate::uring::FrozenFixedBuf)). May send fewer bytes than the buffer
    /// holds, like [`AsyncWriteRent::write`](crate::io::AsyncWriteRent::write).
    ///
    /// This is `IORING_OP_WRITE_FIXED` on the socket: the kernel rejects registered buffers on
    /// a plain `IORING_OP_SEND` (only `SEND_ZC` takes them). So, unlike the other sends of this
    /// crate, there is no `MSG_NOSIGNAL`: writing to a reset connection raises `SIGPIPE`. Rust
    /// binaries ignore `SIGPIPE` by default (then the result is `EPIPE`); if your process
    /// restores the default handler, do not use this on connections that may be reset.
    #[inline]
    pub async fn send_fixed<B: FixedIoBuf>(&self, buf: B) -> BufResult<usize, B> {
        self.send_fixed_op(buf).await
    }

    /// [`send_fixed`](Self::send_fixed) as a named, `Unpin`, `'static` [`WriteFixedFuture`].
    /// The SQE is pushed when this is called.
    #[inline]
    pub fn send_fixed_op<B: FixedIoBuf>(&self, buf: B) -> WriteFixedFuture<B> {
        WriteFixedFuture::new(Op::write_fixed(self.fd.clone(), buf, NO_OFFSET))
    }
}
