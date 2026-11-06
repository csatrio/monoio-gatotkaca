//! Timer state structures.
//!
//! This module contains the heart of the intrusive timer implementation.
//!
//! # Ground rules
//!
//! A timer is a [`TimerShared`] that lives inside its owner's [`TimerEntry`] (which is inside a
//! `Sleep`). While it is registered, the driver's wheel links it into one of its lists through a
//! raw pointer, so the entry must stay pinned and must unlink itself before it goes away.
//!
//! The runtime is single-threaded: the owner and the driver never run at the same time, and the
//! whole state is plain `Cell`s. (The code came from tokio, where both sides were separated by a
//! lock and by cache-line padding; neither exists here.)
//!
//! # State field
//!
//! `state` holds either the tick at which the timer expires (its *true* expiration), or one of
//! the flag values below: never registered, on the wheel's pending list, or fired.
//!
//! # Cached vs true expiration
//!
//! Moving a registered timer to a *later* time is one store: the owner writes the new tick into
//! `state` and the entry stays in the wheel slot it was registered for. `cached_when` remembers
//! that slot's tick (the wheel needs it to find the list when the entry is removed). When the
//! driver reaches the slot, it compares against the true expiration and puts the entry into the
//! slot it now belongs to.

use std::{
    cell::{Cell, UnsafeCell},
    fmt,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

use super::Handle;
use crate::{time::Instant, utils::linked_list};

/// Not in the wheel: fired, or removed.
const STATE_DEREGISTERED: u64 = u64::MAX;
/// Not in the wheel yet: an entry that was never registered (a `Sleep` before its first poll).
const STATE_INIT: u64 = u64::MAX - 1;
/// On the wheel's pending list: it fires in the driver's current pass.
const STATE_PENDING_FIRE: u64 = u64::MAX - 2;
/// Every state below this value is an expiration tick.
const STATE_MIN_VALUE: u64 = STATE_PENDING_FIRE;

/// A timer entry.
///
/// This is the handle to a timer that is controlled by the requester of the
/// timer. As this participates in intrusive data structures, it must be pinned
/// before polling.
#[derive(Debug)]
pub(super) struct TimerEntry {
    /// The driver. The reference keeps the wheel alive for as long as this entry may be linked
    /// into it, also when the entry outlives the runtime.
    driver: Handle,
    /// The state the driver sees. It is part of an intrusive linked list, so the driver holds
    /// pointers to it while this entry is registered.
    inner: UnsafeCell<TimerShared>,
    /// Ensure the type is !Unpin
    _m: PhantomPinned,
}

/// An TimerHandle is the (non-enforced) "unique" pointer from the driver to the
/// timer entry. Generally, at most one TimerHandle exists for a timer at a time
/// (enforced by the timer state machine).
///
/// SAFETY: An TimerHandle is essentially a raw pointer, and the usual caveats
/// of pointer safety apply. In particular, TimerHandle does not itself enforce
/// that the timer does still exist; however, normally an TimerHandle is created
/// immediately before registering the timer, and is consumed when firing the
/// timer, to help minimize mistakes. Still, because TimerHandle cannot enforce
/// memory safety, all operations are unsafe.
#[derive(Debug)]
pub(crate) struct TimerHandle {
    inner: NonNull<TimerShared>,
}

pub(super) type EntryList = crate::utils::linked_list::LinkedList<TimerShared, TimerShared>;

/// The state of a timer, shared between its owner (`TimerEntry`) and the driver.
///
/// 48 bytes, alignment 8: everything `poll`, `fire` and the wheel touch is within one cache
/// line's worth of memory.
pub(crate) struct TimerShared {
    /// The true expiration tick, or one of the `STATE_*` flags. Written by the owner (register,
    /// lazy reset) and by the driver (pending, fired).
    state: Cell<u64>,

    /// The tick of the wheel slot this entry is linked into (`u64::MAX`: the pending list). Valid
    /// only while registered; owned by the driver.
    cached_when: Cell<u64>,

    /// A link within the doubly-linked list of timers on a particular level and
    /// slot. Valid only while registered.
    pointers: UnsafeCell<linked_list::Pointers<TimerShared>>,

    /// The waker of the task that last got `Pending` from this timer.
    waker: Cell<Option<Waker>>,

    _p: PhantomPinned,
}

impl fmt::Debug for TimerShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerShared")
            .field("state", &self.state.get())
            .field("cached_when", &self.cached_when.get())
            .finish()
    }
}

impl TimerShared {
    fn new(state: u64) -> Self {
        Self {
            state: Cell::new(state),
            cached_when: Cell::new(0),
            pointers: UnsafeCell::new(linked_list::Pointers::new()),
            waker: Cell::new(None),
            _p: PhantomPinned,
        }
    }

    /// Gets the cached time-of-expiration value
    pub(super) fn cached_when(&self) -> u64 {
        self.cached_when.get()
    }

    /// Gets the true time-of-expiration value, and copies it into the cached
    /// time-of-expiration value.
    ///
    /// SAFETY: Must be called when this entry is not in any timer wheel lists.
    pub(super) unsafe fn sync_when(&self) -> u64 {
        let true_when = self.true_when();

        self.cached_when.set(true_when);

        true_when
    }

    /// Returns the true time-of-expiration value. Only for an entry whose state is a tick.
    pub(super) fn true_when(&self) -> u64 {
        let state = self.state.get();
        debug_assert!(state < STATE_MIN_VALUE, "timer is not scheduled");
        state
    }

    /// Sets the true time-of-expiration value, even if it is less than the
    /// current expiration or the timer is deregistered.
    ///
    /// SAFETY: Must only be called with the entry not in the timer wheel.
    pub(super) unsafe fn set_expiration(&self, t: u64) {
        debug_assert!(t < STATE_MIN_VALUE);
        self.state.set(t);
        self.cached_when.set(t);
    }

    /// Sets the true time-of-expiration only if it is after the current.
    ///
    /// Returns `Err` without changing anything if the timer is not scheduled (never registered,
    /// pending, fired) or the new tick is earlier than the current one. The entry must be
    /// re-registered then.
    pub(super) fn extend_expiration(&self, t: u64) -> Result<(), ()> {
        let state = self.state.get();
        if t < state || state >= STATE_MIN_VALUE {
            return Err(());
        }
        self.state.set(t);
        Ok(())
    }

    /// Returns a TimerHandle for this timer.
    pub(super) fn handle(&self) -> TimerHandle {
        TimerHandle {
            inner: NonNull::from(self),
        }
    }

    /// Whether the entry is linked into the wheel (a slot or the pending list).
    pub(super) fn is_registered(&self) -> bool {
        self.state.get() <= STATE_PENDING_FIRE
    }

    /// `Ready` once the timer fired. Otherwise remembers the waker for `fire`.
    fn poll(&self, waker: &Waker) -> Poll<()> {
        if self.state.get() == STATE_DEREGISTERED {
            return Poll::Ready(());
        }
        self.register_waker(waker);
        Poll::Pending
    }

    /// Stores `waker` unless the stored one already wakes the same task.
    fn register_waker(&self, waker: &Waker) {
        // SAFETY: the reference to the cell's content ends before anything else touches the cell,
        // and `will_wake` only compares pointers (it calls nothing).
        let same = unsafe { (*self.waker.as_ptr()).as_ref() }.is_some_and(|w| w.will_wake(waker));
        if !same {
            // The clone and the drop of the old waker are calls into the waker's vtable; neither
            // happens while the cell is borrowed.
            let old = self.waker.replace(Some(waker.clone()));
            drop(old);
        }
    }

    /// Marks this timer as being moved to the pending list, if its scheduled
    /// time is not after `not_after`.
    ///
    /// If the timer is scheduled for a time after `not_after`, returns an Err
    /// containing the current scheduled time.
    fn mark_pending(&self, not_after: u64) -> Result<(), u64> {
        let cur_state = self.state.get();

        debug_assert!(cur_state < STATE_MIN_VALUE);

        if cur_state > not_after {
            return Err(cur_state);
        }

        self.state.set(STATE_PENDING_FIRE);
        Ok(())
    }

    /// Fires the timer: marks it as deregistered and hands out the waker to wake (if a task
    /// waits). Does nothing if it already fired.
    fn fire(&self) -> Option<Waker> {
        if self.state.get() == STATE_DEREGISTERED {
            return None;
        }

        self.state.set(STATE_DEREGISTERED);

        self.waker.take()
    }
}

unsafe impl linked_list::Link for TimerShared {
    type Handle = TimerHandle;

    type Target = TimerShared;

    fn as_raw(handle: &Self::Handle) -> NonNull<Self::Target> {
        handle.inner
    }

    unsafe fn from_raw(ptr: NonNull<Self::Target>) -> Self::Handle {
        TimerHandle { inner: ptr }
    }

    unsafe fn pointers(
        target: NonNull<Self::Target>,
    ) -> NonNull<linked_list::Pointers<Self::Target>> {
        unsafe { NonNull::new_unchecked(target.as_ref().pointers.get()) }
    }
}

// ===== impl Entry =====

impl TimerEntry {
    /// An entry that is not registered yet. The first `poll_elapsed` (or `reset`) registers it.
    pub(crate) fn new(driver: Handle) -> Self {
        Self {
            driver,
            inner: UnsafeCell::new(TimerShared::new(STATE_INIT)),
            _m: PhantomPinned,
        }
    }

    fn inner(&self) -> &TimerShared {
        unsafe { &*self.inner.get() }
    }

    /// Whether the timer fired (and was not reset since).
    pub(crate) fn is_elapsed(&self) -> bool {
        self.inner().state.get() == STATE_DEREGISTERED
    }

    /// Moves the timer to `new_time`, registering it if it is not in the wheel.
    pub(crate) fn reset(self: Pin<&mut Self>, new_time: Instant) {
        let tick = self.driver.time_source().deadline_to_tick(new_time);

        if self.inner().extend_expiration(tick).is_ok() {
            return;
        }

        unsafe {
            self.driver.reregister(tick, NonNull::from(self.inner()));
        }
    }

    /// `Ready` once the timer fired. `deadline` is used to register the timer on its first poll
    /// (it cannot be registered before it is pinned), unless a `reset` already did.
    pub(crate) fn poll_elapsed(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> Poll<()> {
        if self.inner().state.get() == STATE_INIT {
            self.as_mut().reset(deadline);
        }

        self.inner().poll(cx.waker())
    }
}

impl TimerHandle {
    pub(super) unsafe fn cached_when(&self) -> u64 {
        unsafe { self.inner.as_ref().cached_when() }
    }

    pub(super) unsafe fn sync_when(&self) -> u64 {
        unsafe { self.inner.as_ref().sync_when() }
    }

    /// Forcibly sets the true and cached expiration times to the given tick.
    ///
    /// SAFETY: The caller must ensure that the handle remains valid and that the timer is not in
    /// any wheel linked lists.
    pub(super) unsafe fn set_expiration(&self, tick: u64) {
        self.inner.as_ref().set_expiration(tick);
    }

    /// Attempts to mark this entry as pending. If the expiration time is after
    /// `not_after`, however, returns an Err with the current expiration time.
    ///
    /// If an `Err` is returned, the `cached_when` value will be updated to this
    /// new expiration time.
    ///
    /// SAFETY: The caller must ensure that the handle remains valid and that the timer is not in
    /// any wheel linked lists. After returning Ok, the entry must be added to the pending list.
    pub(super) unsafe fn mark_pending(&self, not_after: u64) -> Result<(), u64> {
        let shared = self.inner.as_ref();
        match shared.mark_pending(not_after) {
            Ok(()) => {
                // mark this as being on the pending queue in cached_when
                shared.cached_when.set(u64::MAX);
                Ok(())
            }
            Err(tick) => {
                shared.cached_when.set(tick);
                Err(tick)
            }
        }
    }

    /// Fires the timer if it did not fire yet.
    ///
    /// Because the entry might be dropped once it fired, this function consumes the handle to
    /// ensure we don't access the entry afterwards.
    ///
    /// Returns the last-registered waker, if any. Wake it only after the driver's state is
    /// released: a wake may run code that uses timers.
    ///
    /// SAFETY: The handle must be valid and the entry must not be in any wheel linked lists.
    pub(super) unsafe fn fire(self) -> Option<Waker> {
        self.inner.as_ref().fire()
    }
}

impl Drop for TimerEntry {
    fn drop(&mut self) {
        // The memory is about to be reused, so the wheel must not keep a pointer to it. A timer
        // that fired or was never registered is in no list, and its waker (if any) goes with the
        // field.
        if self.inner().is_registered() {
            unsafe { self.driver.remove(NonNull::from(self.inner())) };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::{align_of, size_of};

    use super::*;
    use crate::time::{Sleep, Timeout};

    /// A timer carries no cache-line padding (r3-timer, E1): one runtime thread cannot
    /// false-share. Before: `TimerShared` 384, `TimerEntry` 512, `Sleep` 640, `Timeout<()>` 768
    /// bytes, all aligned to 128, an alignment every future holding a `timeout()` inherited.
    #[test]
    fn timer_sizes() {
        assert_eq!(size_of::<TimerShared>(), 48);
        assert_eq!(align_of::<TimerShared>(), 8);
        assert_eq!(size_of::<TimerEntry>(), 56);
        assert_eq!(align_of::<TimerEntry>(), 8);
        assert_eq!(size_of::<Sleep>(), 72);
        assert_eq!(align_of::<Sleep>(), 8);
        assert_eq!(size_of::<Timeout<std::future::Ready<()>>>(), 80);
        assert_eq!(align_of::<Timeout<std::future::Ready<()>>>(), 8);
    }
}
