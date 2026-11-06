// Currently, rust warns when an unsafe fn contains an unsafe {} block. However,
// in the future, this will change to the reverse. For now, suppress this
// warning and generally stick with being explicit about unsafety.
#![allow(unused_unsafe)]

//! Time driver

mod entry;
pub(crate) use self::entry::TimerEntry;
use self::entry::{EntryList, TimerHandle, TimerShared};

mod handle;
pub(crate) use self::handle::Handle;

mod wheel;

pub(super) mod sleep;

use std::{
    cell::{Cell, RefCell},
    fmt, io,
    ptr::NonNull,
    rc::Rc,
};

use crate::{
    driver::Driver,
    time::{Clock, Duration, Instant},
};

/// Time implementation that drives [`Sleep`][sleep], [`Interval`][interval],
/// and [`Timeout`][timeout].
///
/// A `Driver` instance tracks the state necessary for managing time and
/// notifying the [`Sleep`][sleep] instances once their deadlines are reached.
///
/// It is expected that a single instance manages many individual
/// [`Sleep`][sleep] instances. Like the runtime it belongs to, it lives on one
/// thread.
///
/// After creating the `Driver` instance, the caller must repeatedly call `park`
/// or `park_timeout`. The time driver will perform no work unless `park` or
/// `park_timeout` is called repeatedly.
///
/// The driver has a resolution of one millisecond. Any unit of time that falls
/// between milliseconds are rounded up to the next millisecond.
///
/// # Implementation
///
/// The time driver is based on the [paper by Varghese and Lauck][paper].
///
/// A hashed timing wheel is a vector of slots, where each slot handles a time
/// slice. As time progresses, the timer walks over the slot for the current
/// instant, and processes each entry for that slot. When the timer reaches the
/// end of the wheel, it starts again at the beginning.
///
/// The implementation maintains six wheels arranged in a set of levels. As the
/// levels go up, the slots of the associated wheel represent larger intervals
/// of time. At each level, the wheel has 64 slots. Each slot covers a range of
/// time equal to the wheel at the lower level. At level zero, each slot
/// represents one millisecond of time.
///
/// The wheels are:
///
/// * Level 0: 64 x 1 millisecond slots.
/// * Level 1: 64 x 64 millisecond slots.
/// * Level 2: 64 x ~4 second slots.
/// * Level 3: 64 x ~4 minute slots.
/// * Level 4: 64 x ~4 hour slots.
/// * Level 5: 64 x ~12 day slots.
///
/// When the timer processes entries at level zero, it will notify all the
/// `Sleep` instances as their deadlines have been reached. For all higher
/// levels, all entries will be redistributed across the wheel at the next level
/// down. Eventually, as time progresses, entries with [`Sleep`][sleep]
/// instances will either be canceled (dropped) or their associated entries will
/// reach level zero and be notified.
///
/// The wheel only moves when a timer is due: the driver keeps the earliest
/// tick at which anything can happen (`next_wake`) and, per park and per
/// cold-path submit, compares the clock against it. Without a registered
/// timer it does not read the clock at all.
///
/// [paper]: http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
/// [sleep]: crate::time::Sleep
/// [timeout]: crate::time::Timeout
/// [interval]: crate::time::Interval
#[derive(Debug)]
pub struct TimeDriver<D: 'static> {
    /// Shared state
    pub(crate) handle: Handle,

    /// Parker to delegate to
    park: D,
}

/// The longest time a coarse deadline can be set ahead: about 30 years, like
/// `Instant::far_future`.
const MAX_TICKS_AHEAD: u64 = 86400 * 365 * 30 * 1000;

/// The number of ticks (milliseconds) that cover `d`, rounded up, at most `MAX_TICKS_AHEAD`.
/// For `TimerEntry::{new_coarse, reset_coarse}`.
#[inline]
pub(crate) fn duration_to_ticks(d: Duration) -> u64 {
    let ms = d
        .as_secs()
        .saturating_mul(1000)
        .saturating_add(d.subsec_nanos().div_ceil(1_000_000) as u64);
    ms.min(MAX_TICKS_AHEAD)
}

/// `(inserts, removes)` the owners of timers caused in this thread's wheels, see `wheel::OPS`.
#[cfg(test)]
pub(crate) fn wheel_ops() -> (u64, u64) {
    wheel::OPS.with(|ops| ops.get())
}

/// A structure which handles conversion from Instants to u64 timestamps.
#[derive(Debug, Clone)]
struct ClockTime {
    clock: super::clock::Clock,
    start_time: Instant,
}

impl ClockTime {
    pub(self) fn new(clock: Clock) -> Self {
        Self {
            start_time: clock.now(),
            clock,
        }
    }

    /// The tick a timer for the instant `t` expires at: `t` rounded up to the end of its
    /// millisecond.
    #[inline]
    pub(self) fn deadline_to_tick(&self, t: Instant) -> u64 {
        self.instant_to_ticks(t).1
    }

    /// The tick of the instant `t`, rounded down and rounded up, with one subtraction. An
    /// instant before the start is tick 0; one that is more milliseconds away than a `u64`
    /// holds is the last tick.
    #[inline]
    pub(self) fn instant_to_ticks(&self, t: Instant) -> (u64, u64) {
        let dur: Duration = t
            .checked_duration_since(self.start_time)
            .unwrap_or_else(|| Duration::from_secs(0));
        let ms = dur
            .as_secs()
            .saturating_mul(1000)
            .saturating_add(dur.subsec_millis() as u64);
        // A started millisecond counts for the deadline tick.
        let partial = dur.subsec_nanos() > dur.subsec_millis() * 1_000_000;

        (ms, ms.saturating_add(partial as u64))
    }

    pub(self) fn tick_to_duration(&self, t: u64) -> Duration {
        Duration::from_millis(t)
    }

    pub(self) fn now(&self) -> u64 {
        self.instant_to_ticks(self.clock.now()).0
    }
}

/// Timer state shared between the `TimeDriver` and every `Handle` (one per timer entry).
struct Inner {
    /// Timing backend in use. Never changes, so it sits outside the `RefCell`.
    time_source: ClockTime,

    /// A tick that is not later than the expiration of any registered timer, or 0 without one.
    ///
    /// It is a lower bound, not the exact minimum: registering a timer lowers it, removing one
    /// leaves it alone, and it is exact again after each `process_at_time`. The driver reads the
    /// clock and looks at the wheel only when this tick is reached, so a park or a cold-path
    /// submit with nothing due costs one load (no timer) or one clock read (timers pending).
    next_wake: Cell<u64>,

    /// The coarse clock behind [`Instant::recent`]: the latest clock value the driver or
    /// `recent()` read. Valid while `recent_tick` is not 0.
    recent: Cell<Instant>,

    /// `deadline_to_tick(recent)`, at least 1; 0 marks `recent` as stale. The driver stores 0
    /// once per park and once per cold-path submit, so a fresh value was read after the latest
    /// of them: it lags the real clock by less than one round of tasks.
    recent_tick: Cell<u64>,

    /// Timer wheel
    wheel: RefCell<wheel::Wheel>,
}

// ===== impl Driver =====

impl<D> TimeDriver<D>
where
    D: Driver + 'static,
{
    /// The wrapped driver, e.g. the `IoUringDriver` to reach its raw ring.
    pub fn inner(&self) -> &D {
        &self.park
    }

    /// Creates a new `Driver` instance that uses `park` to block the current
    /// thread and `time_source` to get the current time and convert to ticks.
    ///
    /// Specifying the source of time is useful when testing.
    pub(crate) fn new(park: D, clock: Clock) -> TimeDriver<D> {
        let inner = Inner::new(ClockTime::new(clock));

        TimeDriver {
            handle: Handle::new(Rc::new(inner)),
            park,
        }
    }

    fn park_internal(&self, limit: Option<Duration>) -> io::Result<()> {
        let inner = self.handle.get();

        let res = match inner.next_wake.get() {
            // No timer is registered: nothing to wake up for.
            0 => {
                if let Some(duration) = limit {
                    self.park.park_timeout(duration)
                } else {
                    self.park.park()
                }
            }
            // `when` may be earlier than the earliest timer (see `Inner::next_wake`). The wake-up
            // is then early once: `process_at_time` makes the bound exact.
            when => {
                // Not kept for `recent()`: the value is stale as soon as the wait returns.
                let now = inner.time_source.now();
                // Note that we effectively round up to 1ms here - this avoids
                // very short-duration microsecond-resolution sleeps that the OS
                // might treat as zero-length.
                let mut duration = inner.time_source.tick_to_duration(when.saturating_sub(now));

                if duration > Duration::from_millis(0) {
                    if let Some(limit) = limit {
                        duration = std::cmp::min(limit, duration);
                    }

                    self.park.park_timeout(duration)
                } else {
                    self.park.park_timeout(Duration::from_secs(0))
                }
            }
        };

        // Time passed in the wait. Then fire the timers that are due (also after a failed wait).
        inner.mark_recent_stale();
        self.handle.process_if_due();

        res
    }
}

impl Handle {
    /// Fire due timers, cheaply: no clock read without pending timers, no wheel poll before the
    /// earliest deadline (`next_wake` is never later than it).
    pub(self) fn process_if_due(&self) {
        let inner = self.get();
        let next = inner.next_wake.get();
        if next == 0 {
            return;
        }
        let now = inner.read_clock();
        if now >= next {
            self.process_at_time(now);
        }
    }

    /// `deadline_to_tick(recent())`: the coarse clock as a tick, rounded up. At least 1.
    #[inline]
    pub(crate) fn recent_tick(&self) -> u64 {
        let inner = self.get();
        match inner.recent_tick.get() {
            0 => {
                inner.read_clock();
                inner.recent_tick.get()
            }
            tick => tick,
        }
    }

    /// The coarse clock, see [`Instant::recent`].
    #[inline]
    pub(crate) fn recent(&self) -> Instant {
        let inner = self.get();
        if inner.recent_tick.get() == 0 {
            inner.read_clock();
        }
        inner.recent.get()
    }

    /// Advances the wheel to `now`, fires what is due and makes `next_wake` exact.
    #[cold]
    pub(self) fn process_at_time(&self, mut now: u64) {
        let inner = self.get();
        {
            let wheel = inner.wheel.borrow();
            if now < wheel.elapsed() {
                // Time went backwards! This normally shouldn't happen as the Rust language
                // guarantees that an Instant is monotonic, but can happen when running
                // Linux in a VM on a Windows host due to std incorrectly trusting the
                // hardware clock to be monotonic (tokio issue 3619).
                now = wheel.elapsed();
            }
        }
        loop {
            // The wheel is not borrowed while a waker runs: a wake may run code that creates,
            // resets or drops timers (also ones that wait on the wheel's pending list).
            let entry = inner.wheel.borrow_mut().poll(now);
            let Some(entry) = entry else {
                break;
            };
            let (waker, wake) = unsafe { entry.fire() };
            if let Some(waker) = waker {
                if wake {
                    waker.wake();
                }
                // Else a disarmed `Deadline` left the wheel: its waker is dropped here.
            }
        }
        let next = inner.wheel.borrow().poll_at();
        inner.next_wake.set(next.map_or(0, |t| t.max(1)));
    }

    /// Unlinks a registered timer from the wheel. No waker is invoked.
    ///
    /// SAFETY: `entry` must be registered with this driver (`TimerShared::is_registered`).
    pub(self) unsafe fn remove(&self, entry: NonNull<TimerShared>) {
        unsafe { self.get().wheel.borrow_mut().remove(entry) };
    }

    /// Removes and re-adds an entry to the driver.
    ///
    /// SAFETY: The timer must be either unregistered, or registered with this
    /// driver, and the caller must hold the exclusive reference to its `TimerEntry`.
    pub(self) unsafe fn reregister(&self, new_tick: u64, entry: NonNull<TimerShared>) {
        let inner = self.get();
        let waker = unsafe {
            let mut wheel = inner.wheel.borrow_mut();

            if entry.as_ref().is_registered() {
                wheel.remove(entry);
            }

            // Now that the entry is in no list, mint a handle to reinsert it.
            let entry = entry.as_ref().handle();

            entry.set_expiration(new_tick);

            match wheel.insert(entry) {
                Ok(_) => {
                    // Keep `next_wake` no later than any registered timer, so
                    // `process_if_due` never skips a due one.
                    let tick = new_tick.max(1);
                    let wake = inner.next_wake.get();
                    if wake == 0 || tick < wake {
                        inner.next_wake.set(tick);
                    }
                    None
                }
                Err((entry, super::error::InsertError::Elapsed)) => entry.fire().0,
            }
        };

        // The timer was fired synchronously as a result of the reregistration.
        // Wake the waker; this is needed because we might reset _after_ a poll,
        // and otherwise the task won't be awoken to poll again.
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<D> Driver for TimeDriver<D>
where
    D: Driver + 'static,
{
    fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        self.park.with(f)
    }

    fn submit(&self) -> io::Result<()> {
        let res = self.park.submit();
        // A round of tasks ended: the coarse clock is read again by whoever needs it first.
        self.handle.get().mark_recent_stale();
        // The runtime takes this path instead of `park` when ready tasks remain after a round (for
        // example with `task_budget`), and under saturation it may never park. Fire due timers here
        // too, or they would wait until the ready queue empties.
        self.handle.process_if_due();
        res
    }

    fn park(&self) -> io::Result<()> {
        self.park_internal(None)
    }

    #[cfg(feature = "sync")]
    type Unpark = D::Unpark;

    fn park_timeout(&self, duration: Duration) -> io::Result<()> {
        self.park_internal(Some(duration))
    }

    #[cfg(feature = "sync")]
    fn unpark(&self) -> Self::Unpark {
        self.park.unpark()
    }
}

impl<D: std::os::unix::io::AsRawFd> std::os::unix::io::AsRawFd for TimeDriver<D> {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.park.as_raw_fd()
    }
}

// ===== impl Inner =====

impl Inner {
    pub(self) fn new(time_source: ClockTime) -> Self {
        Inner {
            recent: Cell::new(time_source.start_time),
            recent_tick: Cell::new(0),
            time_source,
            next_wake: Cell::new(0),
            wheel: RefCell::new(wheel::Wheel::new()),
        }
    }

    /// Reads the clock and keeps the value as the coarse clock. Returns its tick (rounded
    /// down, as the wheel wants it).
    fn read_clock(&self) -> u64 {
        let now = self.time_source.clock.now();
        let (tick, deadline_tick) = self.time_source.instant_to_ticks(now);
        self.recent.set(now);
        self.recent_tick.set(deadline_tick.max(1));
        tick
    }

    /// One store; the next `recent()` (or the driver, if a timer is pending) reads the clock.
    #[inline]
    fn mark_recent_stale(&self) {
        self.recent_tick.set(0);
    }
}

impl fmt::Debug for Inner {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Inner").finish()
    }
}
