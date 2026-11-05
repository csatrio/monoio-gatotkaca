//! Zero-copy sends (`IORING_OP_SEND_ZC`, `IORING_OP_SENDMSG_ZC`, Linux 6.0/6.1+) beyond
//! [`TcpStream::write_zc`], and the size threshold that decides when they pay off.
//!
//! On a real NIC the kernel pins the pages and the NIC reads them by DMA. Over loopback (and
//! on devices without scatter-gather) the kernel copies anyway, so zero-copy only adds the
//! notification CQE. Below about 16 KiB the page pinning and the extra CQE usually cost more
//! than the copy they save: see [`zc_worthwhile`].

use super::TcpStream;
use crate::{
    buf::{IoBuf, IoVecBuf},
    driver::op::{
        send_zc::{SendZcBuf, SendZcFuture},
        Op,
    },
    uring::FixedIoBuf,
    BufResult,
};

/// Size from which a zero-copy send usually beats a copying one (16 KiB).
///
/// A rule of thumb from the kernel's `MSG_ZEROCOPY` documentation and io_uring's
/// `SEND_ZC` benchmarks; measure for your NIC. Not related to the `zero-copy` crate feature,
/// which sets `MSG_ZEROCOPY` on plain sends of 10 MiB and more.
pub const ZC_MIN_LEN: usize = 16 * 1024;

/// Whether a send of `len` bytes is worth doing zero-copy (`len >= ZC_MIN_LEN`).
#[inline]
pub const fn zc_worthwhile(len: usize) -> bool {
    len >= ZC_MIN_LEN
}

impl TcpStream {
    /// Zero-copy send that resolves as soon as the send result is known, before the kernel
    /// released the buffer.
    ///
    /// The [`SendZcFuture`] resolves to `(io::Result<usize>, ZcPending<T>)`; await the
    /// [`ZcPending`](crate::net::tcp::ZcPending) to get the buffer back once the kernel is done
    /// with it (the notification CQE). Meanwhile the caller can go on, e.g. send the next
    /// buffer. Dropping the `ZcPending` is fine: the runtime then keeps the buffer until the
    /// notification. Like `write`, it may send fewer bytes than `buf` holds.
    ///
    /// The SQE is pushed when this is called. A kernel without `SEND_ZC` (before 6.0) gives
    /// `EINVAL`; a socket that does not support it `EOPNOTSUPP`.
    #[inline]
    pub fn send_zc_early<T: IoBuf>(&self, buf: T) -> SendZcFuture<T> {
        SendZcFuture::new(Op::send_zc_buf(self.fd.clone(), buf))
    }

    /// Zero-copy send of a registered buffer ([`FixedBuf`](crate::uring::FixedBuf) or
    /// [`FrozenFixedBuf`](crate::uring::FrozenFixedBuf)): `SEND_ZC` with its `buf_index`
    /// (`IORING_RECVSEND_FIXED_BUF`), so the kernel skips pinning the pages. Resolves after
    /// the kernel released the buffer, like [`write_zc`](Self::write_zc).
    ///
    /// A buffer registered on another runtime's ring is sent like a plain buffer. Unlike
    /// [`send_fixed`](Self::send_fixed) this always uses `MSG_NOSIGNAL`.
    pub async fn send_zc_fixed<B: FixedIoBuf>(&self, buf: B) -> BufResult<usize, B> {
        match Op::<SendZcBuf<B>>::send_zc_fixed(self.fd.clone(), buf) {
            Ok(op) => op.result().await,
            Err((e, buf)) => (Err(e), buf),
        }
    }

    /// [`send_zc_fixed`](Self::send_zc_fixed) that resolves with the send result first, like
    /// [`send_zc_early`](Self::send_zc_early).
    #[inline]
    pub fn send_zc_fixed_early<B: FixedIoBuf>(&self, buf: B) -> SendZcFuture<B> {
        SendZcFuture::new(Op::<SendZcBuf<B>>::send_zc_fixed(self.fd.clone(), buf))
    }

    /// Vectored zero-copy send (`IORING_OP_SENDMSG_ZC`, Linux 6.1+) with `MSG_NOSIGNAL`.
    /// Resolves after the kernel released the buffers. It may send fewer bytes than the
    /// buffers hold.
    pub async fn send_vectored_zc<T: IoVecBuf>(&self, buf_vec: T) -> BufResult<usize, T> {
        match Op::send_msg_zc(self.fd.clone(), buf_vec) {
            Ok(op) => op.result().await,
            Err((e, buf_vec)) => (Err(e), buf_vec),
        }
    }

    /// Send `buf` zero-copy ([`write_zc`](Self::write_zc)) if it holds at least
    /// [`ZC_MIN_LEN`] bytes, else with a plain copying send. Either way it resolves when the
    /// buffer can be reused, and it may send fewer bytes than `buf` holds.
    pub async fn send_auto<T: IoBuf>(&self, buf: T) -> BufResult<usize, T> {
        if zc_worthwhile(buf.bytes_init()) {
            self.write_zc(buf).await
        } else {
            match Op::send(self.fd.clone(), buf) {
                Ok(op) => op.result().await,
                Err((e, buf)) => (Err(e), buf),
            }
        }
    }
}
