//! Operations bounded by a link timeout (`IORING_OP_LINK_TIMEOUT`).
//!
//! The op's SQE is pushed with `IO_LINK`, directly followed by a `LINK_TIMEOUT` SQE. When the
//! timeout fires first, the kernel cancels the op, which completes with `-ECANCELED`; otherwise
//! the timeout is cancelled. The timeout's own CQE goes to an internal `user_data` the driver
//! only counts (`DriverStats::link_timeouts`), so it needs no slab slot and no timer wheel.

use std::{io, time::Duration};

use io_uring::{opcode, types, types::Timespec};

use super::{submit::SubmitResult, Completion, Op, OpAble, OpKind};
use crate::{
    buf::{IoBuf, IoBufMut},
    driver::{self, shared_fd::SharedFd, uring::UringInner, util::timespec},
    BufResult,
};

/// An op with a link timeout. `ts` is boxed: the kernel reads it when it prepares the timeout
/// SQE, after the op data may have moved.
pub(crate) struct Timed<T> {
    pub(crate) op: T,
    _ts: Box<Timespec>,
}

impl<T: OpAble> OpAble for Timed<T> {
    const RET_IS_FD: bool = T::RET_IS_FD;
    const SKIP_CANCEL: bool = T::SKIP_CANCEL;
    const KIND: OpKind = T::KIND;

    #[inline]
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        self.op.uring_op()
    }
}

impl<T: OpAble> Op<Timed<T>> {
    /// Submit `op`, cancelled by the kernel if it did not complete within `timeout`. Hands `op`
    /// back if the SQ stayed full.
    pub(crate) fn submit_with_timeout(op: T, timeout: Duration) -> SubmitResult<Self, T> {
        let ts = Box::new(timespec(timeout));
        let ptr: *const Timespec = &*ts;
        let data = Timed { op, _ts: ts };
        driver::CURRENT
            .with(|inner| UringInner::try_submit_with_timeout(&inner.0, data, ptr))
            .map_err(|(e, data)| (e, data.op))
    }
}

/// Unwrap the completion of a timed op: the op's own data and result, with the `-ECANCELED` of
/// a fired timeout turned into [`io::ErrorKind::TimedOut`].
pub(crate) fn untimed<T>(complete: Completion<Timed<T>>) -> Completion<T> {
    let mut meta = complete.meta;
    if let Err(e) = &meta.result {
        if e.raw_os_error() == Some(libc::ECANCELED) {
            meta.result = Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
        }
    }
    Completion {
        data: complete.data.op,
        meta,
    }
}

/// `recv` for [`Timed`]: the whole buffer capacity, no flags.
pub(crate) struct Recv<T> {
    /// Keeps the socket open while the op is in flight.
    fd: SharedFd,
    buf: T,
}

impl<T: IoBufMut> OpAble for Recv<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::Recv::new(
            types::Fd(self.fd.raw_fd()),
            self.buf.write_ptr(),
            self.buf.bytes_total() as _,
        )
        .build()
    }
}

/// `send` with `MSG_NOSIGNAL` for [`Timed`]: the initialized bytes of the buffer.
pub(crate) struct Send<T> {
    /// Keeps the socket open while the op is in flight.
    fd: SharedFd,
    buf: T,
}

impl<T: IoBuf> OpAble for Send<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::Send::new(
            types::Fd(self.fd.raw_fd()),
            self.buf.read_ptr(),
            self.buf.bytes_init() as _,
        )
        .flags(libc::MSG_NOSIGNAL as _)
        .build()
    }
}

/// Receive into `buf`, failing with `TimedOut` after `timeout`.
pub(crate) async fn recv_with_timeout<T: IoBufMut>(
    fd: SharedFd,
    buf: T,
    timeout: Duration,
) -> BufResult<usize, T> {
    let op = match Op::submit_with_timeout(Recv { fd, buf }, timeout) {
        Ok(op) => op,
        Err((e, data)) => return (Err(e), data.buf),
    };
    let Completion { data, meta } = untimed(op.await);
    let mut buf = data.buf;
    let res = meta.result.map(|n| n.into_inner() as usize);
    if let Ok(n) = res {
        // SAFETY: the kernel wrote `n` bytes to the buffer.
        unsafe { buf.set_init(n) };
    }
    (res, buf)
}

/// Send the initialized bytes of `buf`, failing with `TimedOut` after `timeout`.
pub(crate) async fn send_with_timeout<T: IoBuf>(
    fd: SharedFd,
    buf: T,
    timeout: Duration,
) -> BufResult<usize, T> {
    let op = match Op::submit_with_timeout(Send { fd, buf }, timeout) {
        Ok(op) => op,
        Err((e, data)) => return (Err(e), data.buf),
    };
    let Completion { data, meta } = untimed(op.await);
    (meta.result.map(|n| n.into_inner() as usize), data.buf)
}
