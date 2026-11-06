use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use crate::driver;

pub(crate) mod close;
pub(crate) mod read;
pub(crate) mod write;

mod accept;
pub(crate) mod accept_multi;
pub(crate) mod cancel_fd;
mod connect;
mod fadvise;
pub(crate) mod fixed;
mod fsync;
pub(crate) mod link_timeout;
pub(crate) mod multishot;
mod open;
mod poll;
pub(crate) mod recv;
pub(crate) mod recv_multi;
pub(crate) mod send;
pub(crate) mod send_recv;
pub(crate) mod send_zc;
pub(crate) mod shutdown;
pub(crate) mod sockopt;
mod statx;
mod submit;

#[cfg(feature = "mkdirat")]
mod mkdir;

#[cfg(feature = "unlinkat")]
mod unlink;

#[cfg(feature = "renameat")]
mod rename;

#[cfg(feature = "symlinkat")]
mod symlink;

#[cfg(feature = "splice")]
mod splice;

/// In-flight operation
pub(crate) struct Op<T: 'static + OpAble> {
    // Driver running the operation
    pub(super) driver: driver::Inner,

    // Operation index in the slab
    pub(super) index: usize,

    // Per-operation data
    pub(super) data: Option<T>,
}

/// Operation completion. Returns stored state with the result of the operation.
#[derive(Debug)]
pub(crate) struct Completion<T> {
    pub(crate) data: T,
    pub(crate) meta: CompletionMeta,
}

/// Operation completion meta info.
#[derive(Debug)]
pub(crate) struct CompletionMeta {
    pub(crate) result: io::Result<MaybeFd>,
    pub(crate) flags: u32,
}

/// MaybeFd is a wrapper for fd or a normal number. If it is marked as fd, it will close the fd when
/// dropped.
/// Use `into_inner` to take the inner fd or number and skip the drop.
///
/// This wrapper is designed to be used in the syscall return value. It can prevent fd leak when the
/// operation is cancelled.
#[derive(Debug)]
pub(crate) struct MaybeFd {
    is_fd: bool,
    fd: u32,
}

impl MaybeFd {
    #[inline]
    pub(crate) unsafe fn new_result(fdr: io::Result<u32>, is_fd: bool) -> io::Result<Self> {
        fdr.map(|fd| Self { is_fd, fd })
    }

    #[inline]
    pub(crate) const fn into_inner(self) -> u32 {
        let fd = self.fd;
        std::mem::forget(self);
        fd
    }
}

impl Drop for MaybeFd {
    fn drop(&mut self) {
        // The fd close only executed when:
        // 1. the operation is cancelled
        // 2. the cancellation failed
        // 3. the returned result is a fd
        // So this is a relatively cold path. For simplicity, we just do a close syscall here
        // instead of pushing close op.
        if self.is_fd {
            unsafe {
                libc::close(self.fd as libc::c_int);
            }
        }
    }
}

/// How many CQEs an operation posts, which decides how the driver tracks it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpKind {
    /// One CQE. If it unexpectedly carries `IORING_CQE_F_MORE`, the result is kept and the op
    /// completes with the last CQE (the follow-ups are dropped).
    Single,
    /// A zero-copy send: a result CQE with `F_MORE`, then a notification CQE (`F_NOTIF`) once the
    /// kernel released the buffer. Completes with the notification.
    ZeroCopy,
    /// Multishot: any number of CQEs, queued per op, the last one without `F_MORE`. Driven by
    /// [`multishot::MultishotOp`], never by [`Op`].
    Multishot,
}

pub(crate) trait OpAble {
    const RET_IS_FD: bool = false;
    const SKIP_CANCEL: bool = false;
    const KIND: OpKind = OpKind::Single;
    fn uring_op(&mut self) -> io_uring::squeue::Entry;

    /// Multishot ops only: release what a CQE carries (a file descriptor, a provided buffer)
    /// when nobody will read it, i.e. CQEs still queued or arriving after the op was dropped.
    /// Must not call into the driver.
    #[inline]
    fn discard_cqe(&mut self, _cqe: crate::driver::raw::Cqe) {}
}

impl<T: OpAble> Op<T> {
    /// Submit an operation to uring.
    ///
    /// `state` is stored during the operation tracking any state submitted to
    /// the kernel.
    pub(super) fn submit_with(data: T) -> io::Result<Op<T>> {
        driver::CURRENT.with(|this| this.submit_with(data))
    }

    /// Try submitting an operation to uring
    pub(super) fn try_submit_with(data: T) -> io::Result<Op<T>> {
        if driver::CURRENT.is_set() {
            Op::submit_with(data)
        } else {
            Err(io::ErrorKind::Other.into())
        }
    }

    pub(crate) fn op_canceller(&self) -> OpCanceller {
        OpCanceller { index: self.index }
    }

    /// Poll for the result of the operation.
    ///
    /// Once this returned `Ready` the op is finished: its slot is released and `index` is
    /// `usize::MAX`, so dropping the op no longer calls into the driver. The data is still in
    /// the op; [`take_finished`](Self::take_finished) takes it out.
    ///
    /// # Panics
    /// If polled again after `Ready`.
    #[inline]
    pub(crate) fn poll_meta(&mut self, cx: &mut Context<'_>) -> Poll<CompletionMeta> {
        // Coop budget (off: one TLS load). When spent, yield even if the result is ready: it
        // stays in the op slot and is returned on the next poll.
        let budget = ready!(crate::task::coop::poll_proceed(cx));
        if self.index == usize::MAX {
            unexpected_state();
        }
        let meta = ready!(self.driver.poll_op(self.index, cx));
        crate::task::coop::consume(budget);

        self.index = usize::MAX;
        Poll::Ready(meta)
    }

    /// Take a finished op apart, in place: moves the data out and releases the driver handle
    /// (inline), without running `Op::drop` and without moving the op.
    ///
    /// # Safety
    /// [`poll_meta`](Self::poll_meta) must have returned `Ready` for this op, and the caller
    /// must not use or drop the op afterwards: it overwrites the op's place (`ptr::write`)
    /// or forgets it.
    ///
    /// # Panics
    /// If the data was taken before; the op is then untouched and still dropped normally.
    #[inline]
    pub(crate) unsafe fn take_finished(&mut self) -> T {
        debug_assert!(self.index == usize::MAX);
        if self.data.is_none() {
            unexpected_state();
        }
        // SAFETY: by the contract nothing reads or drops the two fields again (`index` needs
        // no drop), so each is moved out exactly once; `data` was checked to be `Some`.
        unsafe {
            std::ptr::drop_in_place(&mut self.driver);
            std::ptr::read(&self.data).unwrap_unchecked()
        }
    }
}

/// An op was polled after it completed, or taken apart before.
#[cold]
#[inline(never)]
fn unexpected_state() -> ! {
    panic!("unexpected operation state")
}

impl<T> Future for Op<T>
where
    T: Unpin + OpAble + 'static,
{
    type Output = Completion<T>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        let meta = ready!(me.poll_meta(cx));
        match me.data.take() {
            Some(data) => Poll::Ready(Completion { data, meta }),
            None => unexpected_state(),
        }
    }
}

impl<T: OpAble> Drop for Op<T> {
    #[inline]
    fn drop(&mut self) {
        // A finished op (the usual case) has nothing left in the driver: no call.
        if self.index != usize::MAX {
            self.driver
                .drop_op(self.index, &mut self.data, T::SKIP_CANCEL);
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub(crate) struct OpCanceller {
    pub(super) index: usize,
}

impl OpCanceller {
    pub(crate) unsafe fn cancel(&self) {
        super::CURRENT.with(|inner| inner.cancel_op(self))
    }
}
