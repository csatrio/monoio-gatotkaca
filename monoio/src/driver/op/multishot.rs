//! Multishot operations: one SQE, many CQEs, queued per op by the driver.

use std::{
    io,
    task::{Context, Poll},
};

use super::{OpAble, OpKind};
use crate::driver::{self, raw::Cqe, uring::UringInner};

/// A multishot operation in flight.
///
/// The driver queues its CQEs in the slab entry; [`poll_next`](Self::poll_next) takes them in
/// order. The op ends with the first CQE without `IORING_CQE_F_MORE`, which frees the slot.
///
/// Dropping it before that discards the queued CQEs ([`OpAble::discard_cqe`]), keeps the data
/// alive in the driver until the final CQE (discarding every CQE until then) and pushes an
/// AsyncCancel.
pub(crate) struct MultishotOp<T: OpAble + 'static> {
    driver: driver::Inner,
    // Slab index, `usize::MAX` once the final CQE was taken.
    index: usize,
    // `Some` until `into_data` or drop.
    data: Option<T>,
}

impl<T: OpAble + 'static> MultishotOp<T> {
    /// Submit `data` to the current driver. Hands `data` back if the SQ stayed full.
    ///
    /// # Panics
    /// Panics outside of a monoio runtime.
    pub(crate) fn submit(data: T) -> Result<Self, (io::Error, T)> {
        driver::CURRENT.with(|inner| Self::submit_in(inner, data))
    }

    /// Submit `data` to the given driver.
    pub(crate) fn submit_in(driver: &driver::Inner, mut data: T) -> Result<Self, (io::Error, T)> {
        debug_assert!(T::KIND == OpKind::Multishot);
        match UringInner::submit_index(&driver.0, &mut data) {
            Ok(index) => Ok(Self {
                driver: driver.clone(),
                index,
                data: Some(data),
            }),
            Err(e) => Err((e, data)),
        }
    }

    /// The next CQE; `Ready(None)` after the final one was returned.
    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Cqe>> {
        if self.index == usize::MAX {
            return Poll::Ready(None);
        }
        let next = ready!(UringInner::poll_multi(&self.driver.0, self.index, cx));
        match next {
            Some(cqe) if cqe.more() => {}
            // The final CQE: the driver freed the slot.
            _ => self.index = usize::MAX,
        }
        Poll::Ready(next)
    }

    /// The final CQE was returned by `poll_next`.
    #[inline]
    pub(crate) fn is_finished(&self) -> bool {
        self.index == usize::MAX
    }

    /// The kernel posted the final CQE (maybe not taken yet).
    pub(crate) fn kernel_done(&self) -> bool {
        self.is_finished() || UringInner::multi_finished(&self.driver.0, self.index)
    }

    /// Ask the kernel to cancel the op (AsyncCancel). Keep polling: the kernel ends it with a
    /// final CQE, usually `-ECANCELED`.
    pub(crate) fn cancel(&self) {
        if !self.kernel_done() {
            // SAFETY: `index` is the slot of this in-flight op.
            unsafe { UringInner::cancel_op(&self.driver.0, self.index) };
        }
    }

    /// The op data.
    #[inline]
    pub(crate) fn data(&self) -> &T {
        // SAFETY: `data` is `Some` until `into_data` or drop, which take `self`.
        unsafe { self.data.as_ref().unwrap_unchecked() }
    }

    /// The data back, once the kernel posted the final CQE. CQEs not taken yet are discarded.
    pub(crate) fn into_data(mut self) -> Result<T, Self> {
        if self.index != usize::MAX {
            // SAFETY: see `data`.
            let data = unsafe { self.data.as_mut().unwrap_unchecked() };
            if !UringInner::finish_multi(&self.driver.0, self.index, data) {
                return Err(self);
            }
            self.index = usize::MAX;
        }
        // SAFETY: see `data`.
        Ok(unsafe { self.data.take().unwrap_unchecked() })
    }
}

impl<T: OpAble + 'static> Drop for MultishotOp<T> {
    fn drop(&mut self) {
        if self.index != usize::MAX {
            UringInner::drop_multi(&self.driver.0, self.index, &mut self.data);
        }
    }
}
