//! Doorbells: wake a task on a ring from another ring with one `IORING_OP_MSG_RING`.
//!
//! The receiving side registers a bell on its ring. A sender on any ring thread queues a
//! `MSG_RING` whose target `user_data` names the bell; the target's tick wakes the bell's task.
//! On the receiver's own ring the bell is rung directly. The sender's own completion of the
//! `MSG_RING` comes back to its ring: on failure the caller's fallback (an eventfd write) runs.

use std::{
    os::fd::{AsRawFd, OwnedFd},
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use io_uring::{opcode, types};

use super::{msg_ring::RingHandle, Inner, CURRENT};

/// `user_data` prefix of a bell message on the target ring (low 32 bits: the bell slot).
pub(crate) const BELL_PREFIX: u64 = 0b1101 << 60;
/// `user_data` prefix of the sender's own `MSG_RING` completion (low 32 bits: pending slot).
pub(crate) const BELL_SRC_PREFIX: u64 = 0b1110 << 60;
const SLOT_MASK: u64 = u32::MAX as u64;

/// Bells of one ring, and the bell messages it sent that haven't completed yet.
#[derive(Default)]
pub(crate) struct Doorbells {
    bells: Vec<Bell>,
    free_bells: Vec<u32>,
    pending: Vec<Option<PendingBell>>,
    free_pending: Vec<u32>,
}

#[derive(Default)]
struct Bell {
    live: bool,
    rung: bool,
    waker: Option<Waker>,
}

/// Kept until the sender's `MSG_RING` completes: the target ring fd stays open, and on failure
/// the eventfd is written instead.
struct PendingBell {
    _target: RingHandle,
    fallback: Arc<OwnedFd>,
}

impl Doorbells {
    fn register(&mut self) -> u32 {
        let slot = match self.free_bells.pop() {
            Some(slot) => slot,
            None => {
                self.bells.push(Bell::default());
                (self.bells.len() - 1) as u32
            }
        };
        self.bells[slot as usize] = Bell {
            live: true,
            ..Bell::default()
        };
        slot
    }

    fn unregister(&mut self, slot: u32) {
        if let Some(bell) = self.bells.get_mut(slot as usize) {
            *bell = Bell::default();
            self.free_bells.push(slot);
        }
    }

    fn poll(&mut self, slot: u32, cx: &mut Context<'_>) -> Poll<()> {
        let bell = &mut self.bells[slot as usize];
        if std::mem::take(&mut bell.rung) {
            return Poll::Ready(());
        }
        match &mut bell.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            w => *w = Some(cx.waker().clone()),
        }
        Poll::Pending
    }

    /// Ring the bell at `slot`. A stale message for a freed (or reused) slot is a spurious
    /// wakeup at most; bell users re-check their state.
    pub(crate) fn ring(&mut self, slot: u32) {
        if let Some(bell) = self.bells.get_mut(slot as usize) {
            if bell.live {
                bell.rung = true;
                if let Some(w) = bell.waker.take() {
                    w.wake();
                }
            }
        }
    }

    /// Route a CQE with a bell prefix. Called from the tick.
    pub(crate) fn complete(&mut self, user_data: u64, res: i32) {
        let slot = (user_data & SLOT_MASK) as u32;
        if user_data & !SLOT_MASK == BELL_PREFIX {
            self.ring(slot);
        } else if let Some(pending) = self.pending.get_mut(slot as usize).and_then(Option::take) {
            self.free_pending.push(slot);
            if res < 0 {
                write_eventfd(&pending.fallback);
            }
        }
    }

    fn add_pending(&mut self, pending: PendingBell) -> u32 {
        match self.free_pending.pop() {
            Some(slot) => {
                self.pending[slot as usize] = Some(pending);
                slot
            }
            None => {
                self.pending.push(Some(pending));
                (self.pending.len() - 1) as u32
            }
        }
    }

    fn remove_pending(&mut self, slot: u32) {
        self.pending[slot as usize] = None;
        self.free_pending.push(slot);
    }
}

/// Write 1 to an eventfd. It can't fail for a blocking eventfd far from overflow; a failure
/// would only delay a wakeup.
pub(crate) fn write_eventfd(efd: &OwnedFd) {
    let one = 1u64;
    // SAFETY: writes 8 bytes from a live u64.
    unsafe {
        libc::write(
            efd.as_raw_fd(),
            &one as *const u64 as *const libc::c_void,
            8,
        )
    };
}

/// Where to ring a bell from another thread.
#[derive(Clone, Debug)]
pub(crate) struct BellAddr {
    ring: RingHandle,
    /// Identity of the receiver's ring, to ring it directly from its own thread.
    ring_id: usize,
    slot: u32,
}

/// A bell registered on the current ring, owned by the waiting side (`!Send`).
pub(crate) struct Doorbell {
    driver: Inner,
    addr: BellAddr,
}

impl Doorbell {
    /// Register a bell on the current runtime's ring.
    ///
    /// # Panics
    /// Panics outside of a monoio runtime.
    pub(crate) fn register() -> std::io::Result<Self> {
        let ring = super::msg_ring::ring_handle()?;
        CURRENT.with(|inner| {
            // SAFETY: called from task code; the driver state is not borrowed.
            let slot = unsafe { (*inner.0.get()).doorbells.register() };
            Ok(Self {
                driver: inner.clone(),
                addr: BellAddr {
                    ring,
                    ring_id: ring_id(inner),
                    slot,
                },
            })
        })
    }

    pub(crate) fn addr(&self) -> &BellAddr {
        &self.addr
    }

    /// `Ready` once the bell rang since the last `Ready`.
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: called from task code; the driver state is not borrowed.
        unsafe { (*self.driver.0.get()).doorbells.poll(self.addr.slot, cx) }
    }

    /// Whether this bell lives on the current runtime's ring.
    pub(crate) fn is_current(&self) -> bool {
        CURRENT.is_set() && CURRENT.with(|inner| ring_id(inner) == self.addr.ring_id)
    }
}

impl Drop for Doorbell {
    fn drop(&mut self) {
        // SAFETY: dropped from task code (the bell is `!Send` and never stored in the driver).
        unsafe { (*self.driver.0.get()).doorbells.unregister(self.addr.slot) }
    }
}

fn ring_id(inner: &Inner) -> usize {
    Rc::as_ptr(&inner.0) as usize
}

/// Ring `addr` from the current thread through its ring: directly if it is the bell's ring,
/// otherwise with a `MSG_RING` submitted on the next tick (`fallback` is written if that
/// fails). Returns `false` if not on a ring thread or the SQ stays full: the caller then
/// writes `fallback` itself.
pub(crate) fn ring_from_current(addr: &BellAddr, fallback: &Arc<OwnedFd>) -> bool {
    if !CURRENT.is_set() {
        return false;
    }
    CURRENT.with(|inner| {
        // SAFETY: called from task code (a channel send); the driver state is not borrowed.
        let uring = unsafe { &mut *inner.0.get() };
        if ring_id(inner) == addr.ring_id {
            uring.doorbells.ring(addr.slot);
            return true;
        }
        if uring.reserve_sqe().is_err() {
            return false;
        }
        let pending = uring.doorbells.add_pending(PendingBell {
            _target: addr.ring.clone(),
            fallback: fallback.clone(),
        });
        let sqe = opcode::MsgRingData::new(
            types::Fd(addr.ring.as_raw_fd()),
            0,
            BELL_PREFIX | addr.slot as u64,
            None,
        )
        .build()
        .user_data(BELL_SRC_PREFIX | pending as u64);
        // SAFETY: the SQE points to no memory; the target fd stays open in `PendingBell`.
        if unsafe { uring.uring.submission().push(&sqe) }.is_err() {
            uring.doorbells.remove_pending(pending);
            return false;
        }
        true
    })
}
