//! Linked SQE chains, see [`submit_chain`](super::raw::submit_chain).
//!
//! Each SQE of a chain gets its own single-shot slab slot. The [`ChainOp`] owns `data` and polls
//! the slots until all of them completed. If it is dropped before that, `data` moves into an `Rc`
//! shared by the slots still in flight, so it lives until the last of them completed (the only
//! allocation besides the slots, and only on that path).

use std::{
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use io_uring::squeue;

use super::{raw::Cqe, uring::UringInner, Inner};

/// `IOSQE_IO_LINK | IOSQE_IO_HARDLINK`.
const LINK_FLAGS: u8 = (squeue::Flags::IO_LINK.bits()) | (squeue::Flags::IO_HARDLINK.bits());

/// The SQE flags byte (`struct io_uring_sqe` starts with `u8 opcode, u8 flags`; `Entry` is
/// `repr(C)` over it). The `io-uring` crate has no getter for it.
#[inline]
fn sqe_flags(sqe: &squeue::Entry) -> u8 {
    // SAFETY: `Entry` is a `repr(C)` wrapper of the 64-byte `io_uring_sqe`, whose second byte is
    // `flags`.
    unsafe { *(sqe as *const squeue::Entry as *const u8).add(1) }
}

/// Check what [`submit_chain`](super::raw::submit_chain) rejects: an empty chain, a last SQE
/// that would link into whatever is pushed next, and `CQE_SKIP_SUCCESS` (the slot of an SQE
/// that posts no CQE would never be freed).
pub(super) fn validate<const N: usize>(sqes: &[squeue::Entry; N]) -> io::Result<()> {
    let invalid = |msg| Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
    let Some(last) = sqes.last() else {
        return invalid("empty chain");
    };
    if sqe_flags(last) & LINK_FLAGS != 0 {
        return invalid("the last SQE of a chain must not set IO_LINK or IO_HARDLINK");
    }
    if sqes
        .iter()
        .any(|sqe| sqe_flags(sqe) & squeue::Flags::SKIP_SUCCESS.bits() != 0)
    {
        return invalid("chain SQEs must not set SKIP_SUCCESS");
    }
    Ok(())
}

/// Set `IO_LINK` on every SQE but the last, so they run in order and an error (or a short
/// read/write) cancels the rest. For [`submit_chain`](super::raw::submit_chain).
pub fn link<const N: usize>(sqes: [squeue::Entry; N]) -> [squeue::Entry; N] {
    let mut i = 0;
    sqes.map(|sqe| {
        i += 1;
        if i < N {
            sqe.flags(squeue::Flags::IO_LINK)
        } else {
            sqe
        }
    })
}

/// An in-flight chain from [`submit_chain`](super::raw::submit_chain). Resolves to the
/// completion of every SQE, in SQE order, and gives `data` back once all of them completed.
///
/// Every SQE of a chain posts exactly one CQE: those cancelled because an earlier link failed
/// complete with `-ECANCELED`. Dropping it before then keeps `data` alive until the last CQE
/// (and, with `async-cancel`, asks the kernel to cancel the SQEs still in flight).
#[must_use = "a chain does nothing useful unless awaited, and keeps its data until it completed"]
pub struct ChainOp<T: Unpin + 'static, const N: usize> {
    driver: Inner,
    /// Slab slot of each SQE; `usize::MAX` once its completion was taken.
    slots: [usize; N],
    cqes: [Cqe; N],
    data: Option<T>,
}

impl<T: Unpin + 'static, const N: usize> ChainOp<T, N> {
    /// Validate and push the chain; hands `data` back on failure.
    pub(crate) fn submit_in(
        driver: &Inner,
        sqes: [squeue::Entry; N],
        data: T,
    ) -> Result<Self, (io::Error, T)> {
        if let Err(e) = validate(&sqes) {
            return Err((e, data));
        }
        let slots = match UringInner::submit_chain(&driver.0, sqes) {
            Ok(slots) => slots,
            Err(e) => return Err((e, data)),
        };
        Ok(ChainOp {
            driver: driver.clone(),
            slots,
            cqes: [Cqe::new(0, 0); N],
            data: Some(data),
        })
    }

    /// The data passed to `submit_chain`. `None` once the chain resolved.
    pub fn data(&self) -> Option<&T> {
        self.data.as_ref()
    }

    /// The completions taken so far, in SQE order (`None` for SQEs still in flight).
    pub fn completed(&self) -> [Option<Cqe>; N] {
        let mut i = 0;
        self.slots.map(|slot| {
            i += 1;
            (slot == usize::MAX).then_some(self.cqes[i - 1])
        })
    }
}

impl<T: Unpin + 'static, const N: usize> Future for ChainOp<T, N> {
    type Output = ([Cqe; N], T);

    /// # Panics
    /// Panics if polled again after it resolved.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        assert!(me.data.is_some(), "ChainOp polled after completion");
        // Coop budget: the whole chain counts as one result.
        let budget = ready!(crate::task::coop::poll_proceed(cx));
        let mut pending = false;
        for (slot, cqe) in me.slots.iter_mut().zip(me.cqes.iter_mut()) {
            if *slot == usize::MAX {
                continue;
            }
            match UringInner::poll_op(&me.driver.0, *slot, cx) {
                Poll::Ready(meta) => {
                    let res = match meta.result {
                        Ok(v) => v.into_inner() as i32,
                        Err(e) => -e.raw_os_error().unwrap_or(libc::EIO),
                    };
                    *cqe = Cqe::new(res, meta.flags);
                    // Polling `Ready` freed the slot.
                    *slot = usize::MAX;
                }
                Poll::Pending => pending = true,
            }
        }
        if pending {
            return Poll::Pending;
        }
        crate::task::coop::consume(budget);
        let data = me.data.take().expect("checked above");
        Poll::Ready((me.cqes, data))
    }
}

impl<T: Unpin + 'static, const N: usize> Drop for ChainOp<T, N> {
    fn drop(&mut self) {
        if self.slots.iter().all(|&slot| slot == usize::MAX) {
            return;
        }
        // Each slot still in flight keeps a reference: the last one to complete drops `data`.
        let shared = self.data.take().map(Rc::new);
        for &slot in self.slots.iter().filter(|&&slot| slot != usize::MAX) {
            let mut keep = shared.clone();
            // A completed (not yet polled) slot is freed right away and leaves `keep` alone.
            self.driver.drop_op(slot, &mut keep, false);
        }
    }
}

impl<T: Unpin + 'static, const N: usize> std::fmt::Debug for ChainOp<T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainOp")
            .field("completed", &self.completed())
            .finish_non_exhaustive()
    }
}
