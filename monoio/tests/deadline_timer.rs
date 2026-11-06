//! r3-timer (T1): the coarse clock `Instant::recent`, the re-armable `Deadline`, `with_deadline`,
//! `timeout_coarse`, and short timers under a busy and an idle loop.
//!
//! Timing rules of this file: lower bounds are exact (a timer must never fire early, a coarse
//! clock must never lag more than the documented bound); upper bounds are generous, so a busy
//! machine does not fail them.

use std::time::Duration;

use monoio::time::Instant;

fn runtime() -> monoio::Runtime<monoio::time::TimeDriver<monoio::IoUringDriver>> {
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .expect("runtime")
}

/// Returns `Pending` once after waking itself: the task stays ready, so the runtime never parks
/// while such a task runs (it takes the cold path, `driver.submit()`, after each round).
async fn yield_once() {
    let mut done = false;
    std::future::poll_fn(|cx| {
        if done {
            return std::task::Poll::Ready(());
        }
        done = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    })
    .await
}

// ===== Instant::recent =====

/// Idle loop: every wake-up comes out of a park. The value `recent()` returns then was read
/// after that park ended, so it cannot be older than the timer that ended the park.
#[test]
fn recent_lags_at_most_one_park_idle() {
    runtime().block_on(async {
        let mut prev = Instant::recent();
        for i in 0..40u64 {
            let wait = Duration::from_millis(1 + i % 4);
            let deadline = Instant::now() + wait;
            monoio::time::sleep_until(deadline).await;
            let recent = Instant::recent();
            let now = Instant::now();
            assert!(
                recent >= deadline,
                "recent() is older than the park that just ended: {:?} before the deadline",
                deadline - recent
            );
            assert!(recent <= now, "recent() is ahead of the clock");
            assert!(recent >= prev, "recent() went backwards");
            // The same round: no second clock read.
            assert_eq!(recent, Instant::recent());
            prev = recent;
        }
    });
}

/// Busy loop: the runtime never parks, it submits after each round. With `task_budget(1)` a
/// round is one poll of the busy task, so a clock value taken before a yield must not be newer
/// than what `recent()` returns after it.
#[test]
fn recent_lags_at_most_one_round_busy() {
    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .task_budget(1)
        .enable_timer()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let task = monoio::spawn(async {
            let start = Instant::now();
            let mut prev = Instant::recent();
            let mut rounds = 0u64;
            while start.elapsed() < Duration::from_millis(60) {
                let before = Instant::now();
                yield_once().await;
                let recent = Instant::recent();
                assert!(
                    recent >= before,
                    "recent() is older than the previous round: {:?}",
                    before - recent
                );
                assert!(recent <= Instant::now(), "recent() is ahead of the clock");
                assert!(recent >= prev, "recent() went backwards");
                assert_eq!(recent, Instant::recent());
                prev = recent;
                rounds += 1;
            }
            rounds
        });
        let rounds = task.await;
        assert!(rounds > 10, "the busy task made only {rounds} rounds");
    });
}

/// The same with the default budget: a round polls the one ready task up to three times
/// (`2 * queue length + 1`), and each round reads the clock once. So the value changes at least
/// every third poll, and never goes backwards.
#[test]
fn recent_is_refreshed_every_round_busy() {
    runtime().block_on(async {
        let task = monoio::spawn(async {
            let start = Instant::now();
            let mut prev = Instant::recent();
            let (mut polls, mut changes) = (0u64, 0u64);
            while start.elapsed() < Duration::from_millis(60) {
                yield_once().await;
                let recent = Instant::recent();
                assert!(recent <= Instant::now(), "recent() is ahead of the clock");
                assert!(recent >= prev, "recent() went backwards");
                polls += 1;
                changes += (recent != prev) as u64;
                prev = recent;
            }
            (polls, changes)
        });
        let (polls, changes) = task.await;
        assert!(polls > 10, "the busy task was polled only {polls} times");
        assert!(
            changes * 4 >= polls,
            "recent() changed {changes} times in {polls} polls"
        );
        assert!(changes < polls, "recent() read the clock on every call");
    });
}

/// With a timer pending the driver reads the clock after each park anyway, and that read is what
/// `recent()` returns. The bound is the same.
#[test]
fn recent_with_a_pending_timer() {
    runtime().block_on(async {
        // Keeps a timer registered for the whole test.
        let _far = monoio::spawn(monoio::time::sleep(Duration::from_secs(600)));
        monoio::time::sleep(Duration::from_millis(1)).await;
        let mut prev = Instant::recent();
        for _ in 0..20 {
            let deadline = Instant::now() + Duration::from_millis(2);
            monoio::time::sleep_until(deadline).await;
            let recent = Instant::recent();
            assert!(recent >= deadline);
            assert!(recent <= Instant::now());
            assert!(recent >= prev);
            prev = recent;
        }
    });
}

/// Outside a runtime and without `enable_timer`, `recent()` is `now()`.
#[test]
fn recent_without_a_time_driver() {
    let a = Instant::now();
    let b = Instant::recent();
    let c = Instant::now();
    assert!(a <= b && b <= c);

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let a = Instant::now();
        let b = Instant::recent();
        let c = Instant::now();
        assert!(a <= b && b <= c);
    });
}
