//! Scheduler counters of a runtime (`metrics` feature only).
//!
//! The counters live in the runtime's `Context` and are plain integers: tasks are `!Send`, so
//! everything is updated from the runtime's own thread, without atomics or allocation.

use std::cell::UnsafeCell;

use crate::runtime::CURRENT;

/// Number of buckets in [`RuntimeStats::polls_per_round`].
pub(crate) const POLL_BUCKETS: usize = 11;

/// Scheduler counters of one runtime, see [`runtime_stats`].
///
/// All counters start at 0 when the runtime is built and are cumulative until
/// [`reset_runtime_stats`] (or [`Runtime::reset_stats`](crate::Runtime::reset_stats)).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub struct RuntimeStats {
    /// Iterations of the inner scheduling loop of `Runtime::block_on`: each runs up to the task
    /// budget of ready tasks, then polls the main future.
    pub rounds: u64,
    /// Cold-path `submit()` calls: a round ended with tasks still queued, so the loop submitted
    /// the SQ without waiting and started another round.
    pub submits: u64,
    /// `park()` calls: the ready queue was empty and the runtime waited for completions.
    pub parks: u64,
    /// Total nanoseconds spent inside `park()`: time inside the waiting `io_uring_enter` (and
    /// reaping the completions). It includes inline submission work done by the kernel in that
    /// enter (e.g. the socket sends of the submitted SQEs), so it is not idle time.
    pub park_ns: u64,
    /// Nanoseconds spent outside `park()`, measured from the end of one park to the start of the
    /// next (and from the start of `block_on` to the first park). Time after the last park of a
    /// `block_on` is not counted.
    pub busy_ns: u64,
    /// Spawned tasks polled (`Task::run`). Without `sync` the main future of `block_on` is not a
    /// task and not counted; with `sync` it is.
    pub tasks_polled: u64,
    /// Histogram of tasks polled per round. Buckets: 0, 1, 2–3, 4–7, 8–15, 16–31, 32–63, 64–127,
    /// 128–255, 256–511, 512+.
    pub polls_per_round: [u64; POLL_BUCKETS],
    /// Rounds that ended because the task budget ran out while tasks were still queued.
    pub budget_exhausted_rounds: u64,
    /// Ready-queue length sampled at the start of the latest round (not cleared by a reset).
    pub ready_queue_len: u64,
    /// Highest ready-queue length seen right after a push.
    pub ready_queue_high_water: u64,
    /// Tasks spawned with `monoio::spawn` (and, with `sync`, the main future of `block_on`).
    pub tasks_spawned: u64,
    /// Spawned tasks whose future completed (aborted or dropped tasks are not counted).
    pub tasks_completed: u64,
    /// Wakers handed in from other threads (`sync` only), drained in `park()` and in the
    /// cold-path `submit()`.
    pub remote_wakes: u64,
    /// Polls that returned `Pending` because the task's coop budget was spent
    /// (`RuntimeBuilder::coop_budget`, `task::consume_budget`).
    pub coop_yields: u64,
    /// Tasks polled from the LIFO slot (`RuntimeBuilder::lifo_slot`).
    pub lifo_polls: u64,
}

/// Bucket of [`RuntimeStats::polls_per_round`] for `n` polls in a round.
#[inline]
pub(crate) fn poll_bucket(n: u64) -> usize {
    // 0 -> 0, 1 -> 1, 2..=3 -> 2, 4..=7 -> 3, ..., 512.. -> 10
    let b = (u64::BITS - n.leading_zeros()) as usize;
    if b < POLL_BUCKETS {
        b
    } else {
        POLL_BUCKETS - 1
    }
}

/// The counters of one runtime, kept in its `Context`.
#[derive(Default)]
pub(crate) struct StatsCell(UnsafeCell<RuntimeStats>);

impl StatsCell {
    /// Update the counters. `f` must not reach the stats again (it is a plain counter update).
    #[inline]
    pub(crate) fn with<R>(&self, f: impl FnOnce(&mut RuntimeStats) -> R) -> R {
        // Safety: the `Context` owning this cell is `!Sync` and only used from its runtime's
        // thread, and `f` does not re-enter, so this is the only reference while it runs.
        unsafe { f(&mut *self.0.get()) }
    }
}

impl crate::runtime::Context {
    pub(crate) fn runtime_stats(&self) -> RuntimeStats {
        let mut stats = self.stats.with(|s| *s);
        stats.ready_queue_high_water = self.tasks.high_water() as u64;
        stats
    }

    pub(crate) fn reset_runtime_stats(&self) {
        self.stats.with(|s| {
            *s = RuntimeStats {
                ready_queue_len: s.ready_queue_len,
                ..RuntimeStats::default()
            }
        });
        self.tasks.reset_high_water();
    }
}

/// Count `n` foreign wakers drained by the current runtime. Hook for the driver's park (`sync`);
/// does nothing outside a runtime.
#[cfg(feature = "sync")]
#[inline]
pub(crate) fn note_remote_wakes(n: u64) {
    CURRENT.try_with(|ctx| {
        if let Some(ctx) = ctx {
            ctx.stats.with(|s| s.remote_wakes += n);
        }
    });
}

/// Scheduler counters of the current thread's runtime.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub fn runtime_stats() -> RuntimeStats {
    CURRENT.with(|ctx| ctx.runtime_stats())
}

/// Reset the scheduler counters of the current thread's runtime to 0 (except
/// `ready_queue_len`, which is a sample, not a counter).
///
/// # Panics
/// Panics if called outside of a monoio runtime.
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub fn reset_runtime_stats() {
    CURRENT.with(|ctx| ctx.reset_runtime_stats())
}

#[cfg(test)]
mod tests {
    use super::poll_bucket;

    #[test]
    fn buckets() {
        let cases = [
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 2),
            (4, 3),
            (7, 3),
            (8, 4),
            (255, 8),
            (256, 9),
            (511, 9),
            (512, 10),
            (u64::MAX, 10),
        ];
        for (n, b) in cases {
            assert_eq!(poll_bucket(n), b, "n = {n}");
        }
    }
}
