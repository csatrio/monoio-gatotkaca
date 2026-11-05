//! Positional reads and writes on registered buffers ([`FixedBuf`], [`FrozenFixedBuf`]).
//!
//! [`FrozenFixedBuf`]: crate::uring::FrozenFixedBuf

use super::File;
use crate::{
    driver::op::Op,
    uring::{FixedBuf, FixedIoBuf, ReadFixedFuture, WriteFixedFuture},
    BufResult,
};

impl File {
    /// Read into a registered buffer at `pos` (`IORING_OP_READ_FIXED`), filling it from the
    /// start up to its capacity. Returns the number of bytes read and the buffer, whose length
    /// is set to that number.
    ///
    /// Same as [`read_at`](Self::read_at), minus the per-op page mapping in the kernel. A buffer
    /// registered on another ring falls back to a plain read.
    #[inline]
    pub async fn read_fixed_at(&self, buf: FixedBuf, pos: u64) -> BufResult<usize, FixedBuf> {
        self.read_fixed_at_op(buf, pos).await
    }

    /// [`read_fixed_at`](Self::read_fixed_at) as a named, `Unpin`, `'static`
    /// [`ReadFixedFuture`]. The SQE is pushed when this is called.
    #[inline]
    pub fn read_fixed_at_op(&self, buf: FixedBuf, pos: u64) -> ReadFixedFuture {
        ReadFixedFuture::new(Op::read_fixed(self.fd.clone(), buf, pos))
    }

    /// Write the bytes of a registered buffer at `pos` (`IORING_OP_WRITE_FIXED`). Takes a
    /// [`FixedBuf`] or a [`FrozenFixedBuf`](crate::uring::FrozenFixedBuf) (or a slice of
    /// one). May write fewer bytes than the buffer holds, like [`write_at`](Self::write_at).
    #[inline]
    pub async fn write_fixed_at<B: FixedIoBuf>(&self, buf: B, pos: u64) -> BufResult<usize, B> {
        self.write_fixed_at_op(buf, pos).await
    }

    /// [`write_fixed_at`](Self::write_fixed_at) as a named, `Unpin`, `'static`
    /// [`WriteFixedFuture`]. The SQE is pushed when this is called.
    #[inline]
    pub fn write_fixed_at_op<B: FixedIoBuf>(&self, buf: B, pos: u64) -> WriteFixedFuture<B> {
        WriteFixedFuture::new(Op::write_fixed(self.fd.clone(), buf, pos))
    }
}
