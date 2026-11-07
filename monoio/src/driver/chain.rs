//! Linked SQE chains, see [`submit_chain`](super::raw::submit_chain).
//!
//! Two shapes:
//!
//! - **Plain** (no `CQE_SKIP_SUCCESS`): each SQE of the chain gets its own single-shot slab
//!   slot and posts exactly one CQE. The [`ChainOp`] owns `data` and polls the slots until all
//!   of them completed. If it is dropped before that, `data` moves into an `Rc` shared by the
//!   slots still in flight, so it lives until the last of them completed (the only allocation
//!   besides the slots, and only on that path).
//! - **Skip** (`IO_LINK | CQE_SKIP_SUCCESS` on every SQE but the last, neither on the last):
//!   the whole chain shares **one** slot and posts exactly **one** CQE (verified on Linux 7.0,
//!   `docs/performance-improvement/round3/r3-probe.md` KV2): the last member's if every
//!   member before it succeeded, else that of the first member that failed, whose link then
//!   cancels the rest without a CQE (`REQ_F_SKIP_LINK_CQES`). Each skipped member's
//!   `user_data` carries a tag and its position (`uring::SKIP_MEMBER_TAG`), which the
//!   dispatcher moves into the stored CQE flags (`uring::cqe_member`), so the [`ChainOp`]
//!   knows which member reported. Dropping it keeps `data` in the slot until that CQE (no
//!   `Rc`). `send_then_recv` is this shape with typed op data (`op/send_recv.rs`).

use std::{
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use io_uring::{opcode, squeue};

use super::{
    raw::Cqe,
    uring::{cqe_member, tagged_cqe_flags, UringInner},
    Inner,
};

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

/// The SQE opcode byte.
#[inline]
fn sqe_opcode(sqe: &squeue::Entry) -> u32 {
    sqe.get_opcode()
}

/// The two shapes of [`submit_chain`](super::raw::submit_chain), see the module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Shape {
    /// One slot and one CQE per SQE.
    Plain,
    /// One slot and one CQE for the whole chain.
    Skip,
}

/// Check what [`submit_chain`](super::raw::submit_chain) rejects and tell the two shapes
/// apart: an empty chain; a last SQE that would link into whatever is pushed next; and
/// `CQE_SKIP_SUCCESS` anywhere but in the skip shape (every SQE but the last has it together
/// with `IO_LINK` and without `IO_HARDLINK`, the last has none of the three), since the
/// kernel posts a simple, known set of CQEs only for that shape (KV2 (f5)–(f7): with the flag
/// on some members only, 2 or 3 CQEs arrive, one of them for a member that has the flag). A
/// `LINK_TIMEOUT` member is rejected in the skip shape: its CQE rules there were not verified.
pub(super) fn validate<const N: usize>(sqes: &[squeue::Entry; N]) -> io::Result<Shape> {
    let invalid = |msg| Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
    let Some(last) = sqes.last() else {
        return invalid("empty chain");
    };
    if sqe_flags(last) & LINK_FLAGS != 0 {
        return invalid("the last SQE of a chain must not set IO_LINK or IO_HARDLINK");
    }
    let skip = squeue::Flags::SKIP_SUCCESS.bits();
    if sqes.iter().all(|sqe| sqe_flags(sqe) & skip == 0) {
        return Ok(Shape::Plain);
    }
    if N < 2 || sqe_flags(last) & skip != 0 {
        return invalid("SKIP_SUCCESS on the last SQE of a chain: its slot would never be freed");
    }
    for sqe in &sqes[..N - 1] {
        let flags = sqe_flags(sqe);
        if flags & skip == 0 {
            return invalid(
                "SKIP_SUCCESS on some SQEs of a chain only; set it on every SQE but the last",
            );
        }
        if flags & squeue::Flags::IO_LINK.bits() == 0
            || flags & squeue::Flags::IO_HARDLINK.bits() != 0
        {
            return invalid("an SQE with SKIP_SUCCESS must set IO_LINK (and not IO_HARDLINK)");
        }
        if sqe_opcode(sqe) == opcode::LinkTimeout::CODE as u32 {
            return invalid("a LINK_TIMEOUT cannot be a SKIP_SUCCESS member of a chain");
        }
    }
    Ok(Shape::Skip)
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

/// Set `IO_LINK | CQE_SKIP_SUCCESS` on every SQE but the last: the skip shape of
/// [`submit_chain`](super::raw::submit_chain), one slot and one CQE for the whole chain.
pub fn link_skip<const N: usize>(sqes: [squeue::Entry; N]) -> [squeue::Entry; N] {
    let mut i = 0;
    sqes.map(|sqe| {
        i += 1;
        if i < N {
            sqe.flags(squeue::Flags::IO_LINK | squeue::Flags::SKIP_SUCCESS)
        } else {
            sqe
        }
    })
}

/// An in-flight chain from [`submit_chain`](super::raw::submit_chain). Resolves to the
/// completion of every SQE, in SQE order, and gives `data` back once the chain completed.
///
/// In the plain shape every SQE posts exactly one CQE: those cancelled because an earlier link
/// failed complete with `-ECANCELED`. In the skip shape (`IO_LINK | CQE_SKIP_SUCCESS` on every
/// SQE but the last) the chain posts one CQE, and the array reports `Cqe::new(0, 0)` for the
/// members before the one that posted ("skipped": they succeeded), its CQE for that member,
/// and `-ECANCELED` for every member after it (they did not run). Dropping the op before it
/// resolved keeps `data` alive until the last CQE (and, with `async-cancel`, asks the kernel
/// to cancel the SQEs still in flight).
#[must_use = "a chain does nothing useful unless awaited, and keeps its data until it completed"]
pub struct ChainOp<T: Unpin + 'static, const N: usize> {
    driver: Inner,
    /// Slab slot of each SQE; `usize::MAX` once its completion was taken. In the skip shape
    /// only `slots[0]` is used: the one slot of the chain.
    slots: [usize; N],
    cqes: [Cqe; N],
    data: Option<T>,
    /// The skip shape: one slot, one CQE.
    shared: bool,
}

impl<T: Unpin + 'static, const N: usize> ChainOp<T, N> {
    /// Validate and push the chain; hands `data` back on failure.
    pub(crate) fn submit_in(
        driver: &Inner,
        sqes: [squeue::Entry; N],
        data: T,
    ) -> Result<Self, (io::Error, T)> {
        let shape = match validate(&sqes) {
            Ok(shape) => shape,
            Err(e) => return Err((e, data)),
        };
        let mut slots = [usize::MAX; N];
        match shape {
            Shape::Plain => match UringInner::submit_chain(&driver.0, sqes) {
                Ok(s) => slots = s,
                Err(e) => return Err((e, data)),
            },
            Shape::Skip => match UringInner::submit_shared_chain(&driver.0, sqes) {
                Ok(index) => slots[0] = index,
                Err(e) => return Err((e, data)),
            },
        }
        Ok(ChainOp {
            driver: driver.clone(),
            slots,
            cqes: [Cqe::new(0, 0); N],
            data: Some(data),
            shared: shape == Shape::Skip,
        })
    }

    /// The data passed to `submit_chain`. `None` once the chain resolved.
    pub fn data(&self) -> Option<&T> {
        self.data.as_ref()
    }

    /// The completions taken so far, in SQE order (`None` for SQEs still in flight). In the
    /// skip shape all of them become known at once.
    pub fn completed(&self) -> [Option<Cqe>; N] {
        if self.shared {
            let done = self.slots[0] == usize::MAX;
            return self.cqes.map(|cqe| done.then_some(cqe));
        }
        let mut i = 0;
        self.slots.map(|slot| {
            i += 1;
            (slot == usize::MAX).then_some(self.cqes[i - 1])
        })
    }

    /// The skip shape: resolve on the chain's one CQE.
    fn poll_shared(&mut self, cx: &mut Context<'_>) -> Poll<([Cqe; N], T)> {
        // Coop budget: the whole chain counts as one result.
        let budget = ready!(crate::task::coop::poll_proceed(cx));
        let meta = ready!(UringInner::poll_op(&self.driver.0, self.slots[0], cx));
        // Polling `Ready` freed the slot.
        self.slots[0] = usize::MAX;
        crate::task::coop::consume(budget);
        let res = match meta.result {
            Ok(v) => v.into_inner() as i32,
            Err(e) => -e.raw_os_error().unwrap_or(libc::EIO),
        };
        // Which member posted: a skipped one that failed, else the last.
        let (posted, flags) = match cqe_member(meta.flags) {
            Some(member) => (member.min(N - 1), tagged_cqe_flags(meta.flags)),
            None => (N - 1, meta.flags),
        };
        for (i, cqe) in self.cqes.iter_mut().enumerate() {
            *cqe = if i < posted {
                // Skipped: it succeeded.
                Cqe::new(0, 0)
            } else if i == posted {
                Cqe::new(res, flags)
            } else {
                // Cancelled by the failed link, without a CQE.
                Cqe::new(-libc::ECANCELED, 0)
            };
        }
        let data = self.data.take().expect("checked by the caller");
        Poll::Ready((self.cqes, data))
    }
}

impl<T: Unpin + 'static, const N: usize> Future for ChainOp<T, N> {
    type Output = ([Cqe; N], T);

    /// # Panics
    /// Panics if polled again after it resolved.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        assert!(me.data.is_some(), "ChainOp polled after completion");
        if me.shared {
            return me.poll_shared(cx);
        }
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
        if self.shared {
            if self.slots[0] != usize::MAX {
                // The one slot keeps `data` until the chain's CQE.
                UringInner::drop_shared(&self.driver.0, self.slots[0], &mut self.data, N);
            }
            return;
        }
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
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nop() -> squeue::Entry {
        opcode::Nop::new().build()
    }

    #[test]
    fn shapes() {
        let link = squeue::Flags::IO_LINK;
        let skip = squeue::Flags::SKIP_SUCCESS;
        assert_eq!(
            validate(&link_skip([nop(), nop(), nop()])).unwrap(),
            Shape::Skip
        );
        assert_eq!(validate(&link_skip([nop(), nop()])).unwrap(), Shape::Skip);
        assert_eq!(
            validate(&super::link([nop(), nop()])).unwrap(),
            Shape::Plain
        );
        assert_eq!(validate(&[nop()]).unwrap(), Shape::Plain);
        // Rejected: the last with SKIP; SKIP on some only; SKIP without IO_LINK; with
        // IO_HARDLINK; a LINK_TIMEOUT member with SKIP; empty.
        assert!(validate(&[nop().flags(skip)]).is_err());
        assert!(validate(&[nop().flags(link | skip), nop().flags(skip)]).is_err());
        assert!(validate(&[nop().flags(link | skip), nop().flags(link), nop()]).is_err());
        assert!(validate(&[nop().flags(skip), nop()]).is_err());
        assert!(validate(&[nop().flags(squeue::Flags::IO_HARDLINK | skip), nop()]).is_err());
        let ts = io_uring::types::Timespec::new();
        let timeout = opcode::LinkTimeout::new(&ts).build().flags(link | skip);
        assert!(validate(&[nop().flags(link | skip), timeout, nop()]).is_err());
        let empty: [squeue::Entry; 0] = [];
        assert!(validate(&empty).is_err());
    }
}
