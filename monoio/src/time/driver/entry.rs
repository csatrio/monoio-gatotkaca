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
//! `cached_when` is the tick of the wheel slot the entry is linked into (the wheel needs it to
//! find the list when the entry is removed). The *true* expiration in `state` may be any tick
//! that is not earlier than it:
//!
//! - Moving a registered timer to a tick `>= cached_when` is one store (`move_expiration`). The
//!   entry stays where it is. When the driver reaches the slot, it compares against the true
//!   expiration and puts the entry into the slot it now belongs to (`mark_pending` fails with
//!   the new tick).
//! - Only a tick before `cached_when` needs the wheel at once: remove and insert.
//!
//! # Disarmed entries (`Deadline`)
//!
//! A [`Deadline`](crate::time::Deadline) that is disarmed while registered gets the true
//! expiration `STATE_NEVER`, again one store. If it is armed again before the wheel reaches its
//! slot, that is a third store. Otherwise the wheel finds `STATE_NEVER` in the slot, puts the
//! entry on the pending list as `STATE_PENDING_SILENT`, and the driver deregisters it without
//! waking anybody (its state becomes `STATE_INIT`, "not registered, not fired").

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

/// Not in the wheel, and fired: a `Sleep` that completed, a `Deadline` that expired while armed.
const STATE_DEREGISTERED: u64 = u64::MAX;
/// Not in the wheel, and not fired: a `Sleep` before its first poll, a disarmed `Deadline`.
const STATE_INIT: u64 = u64::MAX - 1;
/// On the wheel's pending list: it fires in the driver's current pass.
const STATE_PENDING_FIRE: u64 = u64::MAX - 2;
/// On the wheel's pending list, disarmed: the driver's current pass deregisters it silently.
const STATE_PENDING_SILENT: u64 = u64::MAX - 3;
/// Every state below this value is an expiration tick.
const STATE_MIN_VALUE: u64 = STATE_PENDING_SILENT;
/// The expiration tick of a registered, disarmed entry: later than every real tick.
const STATE_NEVER: u64 = u64::MAX - 4;
/// The latest tick a timer can be set to.
pub(super) const MAX_TICK: u64 = STATE_NEVER - 1;

/// A timer entry.
///
/// This is the handle to a timer that is controlled by the requester of the
/// timer. As this participates in intrusive data structures, it must be pinned
/// before polling.
#[derive(Debug)]
pub(crate) struct TimerEntry {
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

    /// Moves the true time-of-expiration to `t` if the entry can stay in its wheel slot: it is
    /// in one, and `t` is not before the slot's tick.
    ///
    /// Returns `false` without changing anything otherwise (not registered, on the pending list,
    /// or `t` is earlier than the slot). The entry must be re-registered then.
    #[inline]
    fn move_expiration(&self, t: u64) -> bool {
        debug_assert!(t <= MAX_TICK);
        if self.state.get() >= STATE_MIN_VALUE || t < self.cached_when.get() {
            return false;
        }
        self.state.set(t);
        true
    }

    /// Disarms the entry: it will not fire, and nobody is woken, until its expiration is set
    /// again.
    #[inline]
    fn disarm(&self) {
        let state = self.state.get();
        if state < STATE_MIN_VALUE {
            // In a wheel slot. It stays there (`STATE_NEVER` is later than the slot's tick).
            self.state.set(STATE_NEVER);
        } else if state == STATE_DEREGISTERED {
            // Expired: forget that.
            self.state.set(STATE_INIT);
        } else if state == STATE_PENDING_FIRE {
            // Only from a waker that runs inside the driver's pass.
            self.state.set(STATE_PENDING_SILENT);
        }
    }

    /// Whether a fire would be reported: the entry is scheduled (or about to fire) and not
    /// disarmed, or it already fired.
    fn is_armed(&self) -> bool {
        let state = self.state.get();
        state < STATE_NEVER || state == STATE_PENDING_FIRE || state == STATE_DEREGISTERED
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

    /// `Ready` once the timer fired. Otherwise remembers the waker for `fire`, unless the entry
    /// is disarmed: nothing will fire then.
    #[inline]
    fn poll(&self, waker: &Waker) -> Poll<()> {
        let state = self.state.get();
        if state == STATE_DEREGISTERED {
            return Poll::Ready(());
        }
        if state < STATE_NEVER || state == STATE_PENDING_FIRE {
            self.register_waker(waker);
        }
        Poll::Pending
    }

    /// Stores `waker` unless the stored one already wakes the same task.
    #[inline]
    fn register_waker(&self, waker: &Waker) {
        // SAFETY: the reference to the cell's content ends before anything else touches the cell,
        // and `same_waker` only compares pointers (it calls nothing).
        match unsafe { (*self.waker.as_ptr()).as_ref() } {
            // A timer made for one request: its first `Pending`.
            None => self.waker.set(Some(waker.clone())),
            Some(old) if same_waker(old, waker) => {}
            Some(_) => self.replace_waker(waker),
        }
    }

    /// Another task polls the timer now.
    #[cold]
    fn replace_waker(&self, waker: &Waker) {
        // The clone and the drop of the old waker are calls into the waker's vtable; neither
        // happens while the cell is borrowed.
        let old = self.waker.replace(Some(waker.clone()));
        drop(old);
    }

    /// Marks this timer as being moved to the pending list, if its scheduled
    /// time is not after `not_after`, or if it is disarmed.
    ///
    /// If the timer is scheduled for a time after `not_after`, returns an Err
    /// containing the current scheduled time.
    fn mark_pending(&self, not_after: u64) -> Result<(), u64> {
        let cur_state = self.state.get();

        debug_assert!(cur_state < STATE_MIN_VALUE);

        if cur_state > not_after {
            if cur_state == STATE_NEVER {
                // Disarmed and not armed again in time: it leaves the wheel.
                self.state.set(STATE_PENDING_SILENT);
                return Ok(());
            }
            return Err(cur_state);
        }

        self.state.set(STATE_PENDING_FIRE);
        Ok(())
    }

    /// Takes the entry out of the driver's hands: it is fired (`STATE_DEREGISTERED`), or, if it
    /// was disarmed, silently deregistered (`STATE_INIT`). Does nothing if it is not registered.
    ///
    /// Returns the waker and whether to wake it. Either way the caller lets go of it only after
    /// it is done with the entry and with the wheel: waking, and also dropping the last waker of
    /// a task, may run code that uses timers or frees this entry.
    fn fire(&self) -> (Option<Waker>, bool) {
        let state = self.state.get();
        if state >= STATE_INIT {
            return (None, false);
        }
        if state == STATE_PENDING_SILENT {
            self.state.set(STATE_INIT);
            return (self.waker.take(), false);
        }

        self.state.set(STATE_DEREGISTERED);

        (self.waker.take(), true)
    }
}

/// Whether `a` and `b` wake the same task.
///
/// Same data pointer and same vtable. This runtime's task wakers take their vtable from one
/// `#[inline(never)]` function per task type (`task/waker.rs`, `vtable`), so a task's waker and
/// its clones share the vtable address and the pointer comparison decides. The content
/// comparison (four function pointers) is the fallback for wakers whose vtable is a promoted
/// constant emitted once per inlining site, as this runtime's used to be: without it a timer
/// polled once per request would clone a waker and drop one on every poll.
#[inline]
fn same_waker(a: &Waker, b: &Waker) -> bool {
    a.data() == b.data() && (std::ptr::eq(a.vtable(), b.vtable()) || a.vtable() == b.vtable())
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

    /// An entry that is not registered yet and knows its expiration already: the tick
    /// `ticks_from_recent` after the coarse clock. The first `poll_elapsed` registers it there.
    pub(crate) fn new_coarse(driver: Handle, ticks_from_recent: u64) -> Self {
        let tick = driver.recent_tick().saturating_add(ticks_from_recent);
        let this = Self::new(driver);
        // Not registered, so `cached_when` is free to carry the tick (never 0: `recent_tick` is
        // at least 1).
        this.inner().cached_when.set(tick.min(MAX_TICK));
        this
    }

    /// Moves the timer to `new_time`, registering it if it is not in the wheel.
    pub(crate) fn reset(self: Pin<&mut Self>, new_time: Instant) {
        let tick = self.driver.time_source().deadline_to_tick(new_time);
        self.set_tick(tick.min(MAX_TICK));
    }

    /// Moves the timer to the tick `ticks_from_recent` after the coarse clock
    /// ([`Instant::recent`]), registering it if it is not in the wheel. Never earlier than
    /// `recent() + ticks`, at most one tick later (both parts are rounded up).
    #[inline]
    pub(crate) fn reset_coarse(self: Pin<&mut Self>, ticks_from_recent: u64) {
        let tick = self.driver.recent_tick().saturating_add(ticks_from_recent);
        self.set_tick(tick.min(MAX_TICK));
    }

    /// Moves the timer to `tick`: one store if the entry can stay in its wheel slot, else it is
    /// (re)registered.
    #[inline]
    fn set_tick(self: Pin<&mut Self>, tick: u64) {
        if self.inner().move_expiration(tick) {
            return;
        }

        unsafe {
            self.driver.reregister(tick, NonNull::from(self.inner()));
        }
    }

    /// See [`TimerShared::disarm`].
    #[inline]
    pub(crate) fn disarm(self: Pin<&mut Self>) {
        self.inner().disarm();
    }

    /// See [`TimerShared::is_armed`].
    pub(crate) fn is_armed(&self) -> bool {
        self.inner().is_armed()
    }

    /// `Ready` once the timer fired. `deadline` is used to register the timer on its first poll
    /// (it cannot be registered before it is pinned), unless a `reset` already did or the entry
    /// was made by `new_coarse`.
    pub(crate) fn poll_elapsed(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> Poll<()> {
        if self.inner().state.get() == STATE_INIT {
            match self.inner().cached_when.get() {
                0 => self.as_mut().reset(deadline),
                tick => self.as_mut().set_tick(tick),
            }
        }

        self.inner().poll(cx.waker())
    }

    /// `Ready` once the timer fired while armed. Never registers the entry: that is `reset`'s
    /// and `reset_coarse`'s job (a `Deadline` is armed explicitly).
    #[inline]
    pub(crate) fn poll_fired(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.inner().poll(cx.waker())
    }

    /// The tick of the wheel slot the entry sits in (valid while registered).
    #[cfg(test)]
    pub(crate) fn slot_tick(&self) -> u64 {
        self.inner().cached_when()
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

    /// Fires the timer if it is registered (or deregisters it silently if it is disarmed), see
    /// [`TimerShared::fire`].
    ///
    /// Because the entry might be dropped once it fired, this function consumes the handle to
    /// ensure we don't access the entry afterwards.
    ///
    /// Returns the last-registered waker, if any, and whether to wake it. Wake or drop it only
    /// after the wheel is released.
    ///
    /// SAFETY: The handle must be valid and the entry must not be in any wheel linked lists.
    pub(super) unsafe fn fire(self) -> (Option<Waker>, bool) {
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
