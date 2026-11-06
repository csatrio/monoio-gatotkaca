//! r3-timer (T1): the coarse clock `Instant::recent`, the re-armable `Deadline`, `with_deadline`,
//! `timeout_coarse`, and short timers under a busy and an idle loop.
//!
//! Timing rules of this file: lower bounds are exact (a timer must never fire early, a coarse
//! clock must never lag more than the documented bound); upper bounds are generous, so a busy
//! machine does not fail them.

use std::{
    cell::Cell,
    future::{pending, poll_fn, Future},
    pin::{pin, Pin},
    rc::Rc,
    task::Poll,
    time::Duration,
};

use monoio::time::{sleep, timeout, timeout_coarse, with_deadline, Deadline, Instant};

const MS: Duration = Duration::from_millis(1);

/// Generous: a busy machine may deschedule the test for a long time.
const LATE: Duration = Duration::from_secs(3);

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

// ===== Deadline, with_deadline =====

/// Polls `poll_expired` once and reports whether it was ready.
async fn expired_now(mut d: Pin<&mut Deadline>) -> bool {
    poll_fn(|cx| Poll::Ready(d.as_mut().poll_expired(cx).is_ready())).await
}

/// A disarmed deadline never fires and wakes nobody, also when the wheel reaches the slot the
/// entry was registered in.
#[test]
fn disarmed_deadline_never_fires() {
    runtime().block_on(async {
        // Never armed.
        let mut d = pin!(Deadline::new());
        assert!(!d.is_armed() && !d.is_expired());
        assert!(!expired_now(d.as_mut()).await);
        assert_eq!(with_deadline(d.as_mut(), async { 7 }).await, Ok(7));

        // Armed, then disarmed before the expiry: the old slot comes up while it is disarmed.
        d.as_mut().arm(10 * MS);
        assert!(d.is_armed());
        d.as_mut().disarm();
        assert!(!d.is_armed());
        sleep(40 * MS).await;
        assert!(!d.is_armed() && !d.is_expired());
        assert!(!expired_now(d.as_mut()).await);
        assert_eq!(with_deadline(d.as_mut(), sleep(5 * MS)).await, Ok(()));

        // The same with a task that waits on the deadline: it must not even be polled again.
        let polls = Rc::new(Cell::new(0u32));
        let polls2 = polls.clone();
        let waiter = monoio::spawn(async move {
            let mut d = pin!(Deadline::new());
            d.as_mut().arm(10 * MS);
            poll_fn(|cx| {
                polls2.set(polls2.get() + 1);
                if polls2.get() == 1 {
                    // Leaves the task's waker in the entry, then disarms.
                    assert!(d.as_mut().poll_expired(cx).is_pending());
                    d.as_mut().disarm();
                }
                Poll::<()>::Pending
            })
            .await
        });
        sleep(50 * MS).await;
        assert_eq!(polls.get(), 1, "a disarmed deadline woke its task");
        drop(waiter);
    });
}

/// `arm` after the entry left the wheel while disarmed registers it again and fires at the new
/// time.
#[test]
fn arm_after_expiry_while_disarmed() {
    runtime().block_on(async {
        let mut d = pin!(Deadline::new());
        d.as_mut().arm(5 * MS);
        d.as_mut().disarm();
        // The slot comes up and the entry is taken out of the wheel.
        sleep(30 * MS).await;

        let armed_at = Instant::recent();
        d.as_mut().arm(30 * MS);
        assert!(d.is_armed() && !d.is_expired());
        let res = with_deadline(d.as_mut(), pending::<()>()).await;
        let took = armed_at.elapsed();
        assert!(res.is_err());
        assert!(took >= 30 * MS, "fired {took:?} after arm(30 ms)");
        assert!(took < LATE, "fired {took:?} after arm(30 ms)");
        assert!(d.is_armed() && d.is_expired());

        // Expired stays expired until it is disarmed or armed again.
        assert!(expired_now(d.as_mut()).await);
        assert!(with_deadline(d.as_mut(), pending::<()>()).await.is_err());
        d.as_mut().disarm();
        assert!(!d.is_armed() && !d.is_expired());
        assert!(!expired_now(d.as_mut()).await);

        // And once more after a real expiry: arm registers again.
        let armed_at = Instant::recent();
        d.as_mut().arm(20 * MS);
        assert!(with_deadline(d.as_mut(), pending::<()>()).await.is_err());
        let took = armed_at.elapsed();
        assert!(
            took >= 20 * MS && took < LATE,
            "fired {took:?} after arm(20 ms)"
        );
    });
}

/// Arming to a later time (one store, the entry stays in its slot) fires at the latest time, not
/// at the slot's.
#[test]
fn rearm_later_fires_at_the_latest_deadline() {
    runtime().block_on(async {
        let mut d = pin!(Deadline::new());
        let start = Instant::now();
        d.as_mut().arm(15 * MS);
        // Keep pushing it out, like a connection that serves request after request.
        let mut armed_at = Instant::recent();
        while start.elapsed() < 80 * MS {
            sleep(3 * MS).await;
            assert!(!d.is_expired(), "fired at an old deadline");
            armed_at = Instant::recent();
            d.as_mut().arm(40 * MS);
        }
        let res = with_deadline(d.as_mut(), pending::<()>()).await;
        let took = armed_at.elapsed();
        assert!(res.is_err());
        assert!(took >= 40 * MS, "fired {took:?} after the last arm(40 ms)");
        assert!(took < LATE, "fired {took:?} after the last arm(40 ms)");
    });
}

/// Arming to an earlier time fires at the earlier time.
#[test]
fn rearm_earlier_fires_at_the_earlier_deadline() {
    runtime().block_on(async {
        let mut d = pin!(Deadline::new());
        d.as_mut().arm(Duration::from_secs(30));
        let armed_at = Instant::recent();
        d.as_mut().arm(20 * MS);
        let res = with_deadline(d.as_mut(), pending::<()>()).await;
        let took = armed_at.elapsed();
        assert!(res.is_err());
        assert!(took >= 20 * MS, "fired {took:?} after arm(20 ms)");
        assert!(
            took < LATE,
            "fired {took:?} after arm(20 ms): the 30 s slot was kept?"
        );

        // Disarmed in between: the same.
        d.as_mut().arm(Duration::from_secs(30));
        d.as_mut().disarm();
        let armed_at = Instant::recent();
        d.as_mut().arm(10 * MS);
        assert!(with_deadline(d.as_mut(), pending::<()>()).await.is_err());
        let took = armed_at.elapsed();
        assert!(
            took >= 10 * MS && took < LATE,
            "fired {took:?} after arm(10 ms)"
        );
    });
}

/// `arm_at` takes an exact instant; an instant in the past and `arm(0)` expire at once.
#[test]
fn arm_at_and_zero() {
    runtime().block_on(async {
        let mut d = pin!(Deadline::new());
        let at = Instant::now() + 25 * MS;
        d.as_mut().arm_at(at);
        assert!(with_deadline(d.as_mut(), pending::<()>()).await.is_err());
        let now = Instant::now();
        assert!(now >= at, "fired {:?} early", at - now);
        assert!(now - at < LATE);

        let start = Instant::now();
        d.as_mut().arm_at(start - 10 * MS);
        assert!(with_deadline(d.as_mut(), pending::<()>()).await.is_err());
        d.as_mut().arm(Duration::ZERO);
        assert!(with_deadline(d.as_mut(), pending::<()>()).await.is_err());
        assert!(start.elapsed() < LATE);

        // The longest duration there is: cut to about 30 years, no overflow.
        d.as_mut().arm(Duration::MAX);
        assert!(d.is_armed() && !d.is_expired());
        assert_eq!(with_deadline(d.as_mut(), sleep(2 * MS)).await, Ok(()));
    });
}

/// `Deadline::timeout`: the inner output, or `Err(Elapsed)` on expiry; disarmed afterwards; the
/// same `Deadline` serves the next request.
#[test]
fn deadline_timeout_reuse() {
    runtime().block_on(async {
        let mut d = pin!(Deadline::new());
        for round in 0..200u32 {
            // A request that finishes in time.
            let res = d.as_mut().timeout(50 * MS, async { round }).await;
            assert_eq!(res, Ok(round));
            assert!(!d.is_armed());

            if round % 50 == 0 {
                // One that does not: a pinned reference works as the future.
                let slow = pin!(sleep(Duration::from_secs(5)));
                let armed_at = Instant::recent();
                let res = d.as_mut().timeout(10 * MS, slow).await;
                let took = armed_at.elapsed();
                assert!(res.is_err());
                assert!(took >= 10 * MS && took < LATE, "timed out after {took:?}");
                assert!(!d.is_armed() && !d.is_expired());
                // Its error converts into `io::ErrorKind::TimedOut`.
                let e: std::io::Error = res.unwrap_err().into();
                assert_eq!(e.kind(), std::io::ErrorKind::TimedOut);
            }

            // One that waits on the runtime, well inside the deadline.
            let res = d.as_mut().timeout(Duration::from_secs(5), sleep(MS)).await;
            assert_eq!(res, Ok(()));
        }

        // Dropped before it completed: disarmed too.
        {
            let fut = d.as_mut().timeout(5 * MS, pending::<()>());
            drop(fut);
        }
        assert!(!d.is_armed());
        sleep(20 * MS).await;
        assert!(!d.is_expired());
    });
}

/// Several steps under one deadline with `with_deadline`: it neither arms nor disarms.
#[test]
fn with_deadline_steps() {
    runtime().block_on(async {
        let mut d = pin!(Deadline::new());
        let armed_at = Instant::recent();
        d.as_mut().arm(40 * MS);
        assert_eq!(with_deadline(d.as_mut(), sleep(5 * MS)).await, Ok(()));
        assert!(d.is_armed());
        assert_eq!(with_deadline(d.as_mut(), async { 1 }).await, Ok(1));
        // The third step runs into what is left of the 40 ms.
        let res = with_deadline(d.as_mut(), sleep(Duration::from_secs(5))).await;
        let took = armed_at.elapsed();
        assert!(res.is_err());
        assert!(took >= 40 * MS && took < LATE, "expired after {took:?}");
        // A ready future still wins against the expired deadline: it is polled first.
        assert_eq!(with_deadline(d.as_mut(), async { 2 }).await, Ok(2));
        d.as_mut().disarm();
        assert_eq!(with_deadline(d.as_mut(), sleep(2 * MS)).await, Ok(()));
    });
}

/// Thousands of deadlines that are armed (so linked into the wheel), disarmed or not, and
/// dropped: nothing stays behind in the wheel. If an entry did, the driver would walk freed
/// memory when the wheel reaches those slots at the end.
#[test]
fn dropped_deadlines_leave_nothing_behind() {
    runtime().block_on(async {
        for i in 0..5000u32 {
            let mut d = Box::pin(Deadline::new());
            d.as_mut().arm((1 + i % 20) * MS);
            if i % 3 == 0 {
                d.as_mut().disarm();
            }
            if i % 500 == 0 {
                // Let the wheel move, so the entries land in different slots.
                sleep(MS).await;
            }
            drop(d);
        }
        // Past every slot used above.
        sleep(40 * MS).await;
    });
}

/// A deadline is woken through the waker of the task that polled it last.
#[test]
fn deadline_follows_its_task() {
    runtime().block_on(async {
        let mut d = Box::pin(Deadline::new());
        d.as_mut().arm(20 * MS);
        // Polled here first (the main task's waker is stored) ...
        assert!(!expired_now(d.as_mut()).await);
        // ... then awaited in another task.
        let task = monoio::spawn(async move {
            poll_fn(|cx| d.as_mut().poll_expired(cx)).await;
        });
        assert!(
            timeout(LATE, task).await.is_ok(),
            "the task that waits on the deadline was not woken"
        );
    });
}

// ===== timeout_coarse, timeout =====

#[test]
fn timeout_coarse_works() {
    runtime().block_on(async {
        // The inner output when it is in time.
        assert_eq!(timeout_coarse(50 * MS, async { 3 }).await, Ok(3));
        assert_eq!(
            timeout_coarse(Duration::from_secs(5), sleep(2 * MS)).await,
            Ok(())
        );

        // Elapsed otherwise: not earlier than `duration` after the coarse clock.
        for wait in [0u32, 1, 5, 30] {
            let from = Instant::recent();
            let res = timeout_coarse(wait * MS, pending::<()>()).await;
            let took = from.elapsed();
            assert!(res.is_err());
            assert!(
                took >= wait * MS,
                "timeout_coarse({wait} ms) fired after {took:?}"
            );
            assert!(
                took < LATE,
                "timeout_coarse({wait} ms) fired after {took:?}"
            );
        }

        // Durations that overflow an `Instant` are cut, as in `timeout`.
        assert_eq!(timeout_coarse(Duration::MAX, sleep(2 * MS)).await, Ok(()));
    });
}

/// `timeout` uses the exact clock: its deadline counts from the call, also when the task has
/// been running for long since the runtime last read the clock.
#[test]
fn timeout_is_exact_after_a_long_poll() {
    runtime().block_on(async {
        sleep(MS).await;
        // The task computes for 40 ms; the coarse clock still shows the start of this poll.
        std::thread::sleep(40 * MS);
        let start = Instant::now();
        let res = timeout(20 * MS, pending::<()>()).await;
        let took = start.elapsed();
        assert!(res.is_err());
        assert!(took >= 20 * MS, "timeout(20 ms) fired after {took:?}");
        assert!(took < LATE);
    });
}

/// A `Sleep` handed to another task wakes that task (the entry keeps the waker of its latest
/// poll, not of its first).
#[test]
fn sleep_follows_its_task() {
    runtime().block_on(async {
        let mut s = Box::pin(sleep(20 * MS));
        let ready = poll_fn(|cx| Poll::Ready(s.as_mut().poll(cx).is_ready())).await;
        assert!(!ready);
        let task = monoio::spawn(s);
        assert!(
            timeout(LATE, task).await.is_ok(),
            "the task that awaits the sleep was not woken"
        );
    });
}

// ===== short and long timers, idle and busy loop =====
//
// What the driver does behind these (r3-timer) and what must not show:
// - The wheel only moves when a timer is due, so after a while without timers it is behind the
//   clock, and a new timer is placed relative to an old time.
// - With the fresh list, a timer that expires 64 ms or more after the wheel's time waits on a
//   list before it gets a slot: every timer of 64 ms and more, and short ones on a wheel that is
//   behind.
// A timer fires at its time either way.

/// Sleeps `ms` milliseconds and checks that it took at least that long, and not absurdly more.
async fn checked_sleep(ms: u64) {
    let start = Instant::now();
    sleep(Duration::from_millis(ms)).await;
    let took = start.elapsed();
    assert!(
        took >= Duration::from_millis(ms),
        "sleep({ms} ms) took {took:?}"
    );
    assert!(took < LATE, "sleep({ms} ms) took {took:?}");
}

/// The timer mix of the two tests below.
async fn timer_mix() {
    // One after the other, on a wheel that is up to date after the first.
    for ms in [0, 1, 5, 63, 64, 65, 70, 100] {
        checked_sleep(ms).await;
    }
    // Short ones on a wheel that is 80 ms behind: no timer is registered meanwhile, so the
    // driver does not move it, and these are 80 ms and more from its time.
    for ms in [0, 1, 5] {
        std::thread::sleep(80 * MS);
        checked_sleep(ms).await;
    }
    // Many at once, short and long mixed, on a stale wheel again.
    std::thread::sleep(80 * MS);
    let tasks: Vec<_> = (0..300u64)
        .map(|i| monoio::spawn(checked_sleep(i * 7 % 150)))
        .collect();
    for task in tasks {
        task.await;
    }
    // A timeout that expires, between timeouts that do not.
    for _ in 0..200 {
        assert_eq!(timeout(Duration::from_secs(10), yield_once()).await, Ok(()));
    }
    let start = Instant::now();
    assert!(timeout(70 * MS, pending::<()>()).await.is_err());
    assert!(start.elapsed() >= 70 * MS && start.elapsed() < LATE);
}

/// Runs `test` on its own thread and fails if it does not finish: timers that stall would hang
/// the runtime (its own timeouts could not end the test).
fn with_watchdog(what: &str, test: impl FnOnce() + Send + 'static) {
    let (tx, rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        test();
        let _ = tx.send(());
    });
    match rx.recv_timeout(Duration::from_secs(60)) {
        Ok(()) => thread.join().expect("test thread"),
        // The thread panicked: show its message.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            std::panic::resume_unwind(thread.join().unwrap_err())
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("{what}: not finished after 60 s (timers stalled)")
        }
    }
}

/// Idle loop: the runtime parks between the timers.
#[test]
fn timers_fire_on_time_idle() {
    with_watchdog("idle", || runtime().block_on(timer_mix()));
}

/// Busy loop: tasks that are always ready, so the runtime never parks and timers are only seen
/// by the cold-path submit.
#[test]
fn timers_fire_on_time_busy() {
    for budget in [None, Some(1), Some(16)] {
        with_watchdog("busy", move || {
            let mut builder = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new();
            if let Some(n) = budget {
                builder = builder.task_budget(n);
            }
            let mut rt = builder.enable_timer().build().expect("runtime");
            rt.block_on(async {
                let stop = Rc::new(Cell::new(false));
                for _ in 0..8 {
                    let stop = stop.clone();
                    monoio::spawn(async move {
                        while !stop.get() {
                            yield_once().await;
                        }
                    });
                }
                // In a task: the main future is not polled in a round of its own.
                monoio::spawn(timer_mix()).await;
                stop.set(true);
            });
        });
    }
}

/// Intervals: a period that goes through the fresh list and one that does not.
#[test]
fn intervals_tick_on_time() {
    runtime().block_on(async {
        for (period, ticks) in [(70u64, 4u32), (3, 30)] {
            let period = Duration::from_millis(period);
            let start = Instant::now();
            let mut interval = monoio::time::interval(period);
            for n in 0..ticks {
                let scheduled = interval.tick().await;
                let now = Instant::now();
                assert!(now >= scheduled, "tick {n} came early");
                assert!(
                    now >= start + period * n,
                    "tick {n} of {period:?} after {:?}",
                    now - start
                );
                assert!(now - start < LATE + period * n);
            }
        }
    });
}

/// The per-request `timeout`: registered on the first poll, dropped when the request is done,
/// tens of thousands of times, with the runtime parking now and then. Nothing is left behind
/// for the flush that the last sleep forces, and timers still fire on time afterwards.
#[test]
fn timeouts_dropped_young_leave_nothing_behind() {
    runtime().block_on(async {
        monoio::spawn(async {
            for i in 0..30_000u32 {
                let res = timeout(Duration::from_secs(10), yield_once()).await;
                assert_eq!(res, Ok(()));
                // Reset while fresh, then dropped.
                let mut s = pin!(sleep(Duration::from_secs(5)));
                assert!(poll_fn(|cx| Poll::Ready(s.as_mut().poll(cx).is_pending())).await);
                s.as_mut()
                    .reset(Instant::now() + Duration::from_secs(i as u64 % 7 + 1));
                if i % 3000 == 0 {
                    checked_sleep(1).await;
                }
            }
        })
        .await;
        checked_sleep(100).await;
        checked_sleep(1).await;
    });
}

// ===== timers used from inside a wake =====

thread_local! {
    /// What the hook waker runs when the driver wakes it.
    static IN_WAKE: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
}

/// A waker that runs `IN_WAKE` (code that uses timers) and then wakes the task. The driver calls
/// it while it fires timers.
struct Hook {
    task: std::task::Waker,
}

impl std::task::Wake for Hook {
    fn wake(self: std::sync::Arc<Self>) {
        let hook = IN_WAKE.with(|f| f.borrow_mut().take());
        if let Some(mut hook) = hook {
            hook();
        }
        self.task.wake_by_ref();
    }
}

/// Polls `fut` with the hook waker until it is ready.
async fn with_hook<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    poll_fn(|cx| {
        let hook = std::task::Waker::from(std::sync::Arc::new(Hook {
            task: cx.waker().clone(),
        }));
        fut.as_mut()
            .poll(&mut std::task::Context::from_waker(&hook))
    })
    .await
}

/// A waker that the driver calls may create, arm, reset and drop timers: the driver does not
/// hold its wheel then. A timer registered there fires on time, also one that is already due.
#[test]
fn timers_registered_inside_a_wake() {
    runtime().block_on(async {
        let later = Rc::new(std::cell::RefCell::new(Box::pin(Deadline::new())));
        let due = Rc::new(std::cell::RefCell::new(Box::pin(Deadline::new())));
        let armed_at = Rc::new(Cell::new(None));
        let (later2, due2, armed_at2) = (later.clone(), due.clone(), armed_at.clone());
        IN_WAKE.with(|f| {
            *f.borrow_mut() = Some(Box::new(move || {
                armed_at2.set(Some(Instant::now()));
                later2
                    .borrow_mut()
                    .as_mut()
                    .arm_at(Instant::now() + 15 * MS);
                // Already due when it is registered.
                due2.borrow_mut().as_mut().arm_at(Instant::now() - MS);
                // Created, registered and dropped right here: near (a slot) and far (fresh).
                for after in [3 * MS, Duration::from_secs(3)] {
                    let mut tmp = Box::pin(Deadline::new());
                    tmp.as_mut().arm(after);
                    drop(tmp);
                }
            }));
        });

        with_hook(sleep(10 * MS)).await;
        let armed_at: Instant = armed_at.get().expect("the hook did not run");

        // The hook ran and is gone, so these are the only references.
        let mut due = Rc::try_unwrap(due).expect("hook is gone").into_inner();
        let mut later = Rc::try_unwrap(later).expect("hook is gone").into_inner();
        assert!(timeout(LATE, with_deadline(due.as_mut(), pending::<()>()))
            .await
            .expect("a timer that was due when registered in a wake did not fire")
            .is_err());
        assert!(with_deadline(later.as_mut(), pending::<()>())
            .await
            .is_err());
        let took = armed_at.elapsed();
        assert!(took >= 15 * MS && took < LATE, "fired after {took:?}");
    });
}

/// A waker that drops and resets other timers which expire in the same pass of the driver
/// (they wait on the wheel's pending list at that moment).
#[test]
fn timers_dropped_and_reset_inside_a_wake() {
    runtime().block_on(async {
        let at = Instant::now() + 10 * MS;
        // Registered first, so it is the first of its slot to fire. Its waker runs the hook.
        let mut first = Box::pin(monoio::time::sleep_until(at));
        assert!(
            with_hook(poll_fn(|cx| Poll::Ready(
                first.as_mut().poll(cx).is_pending()
            )))
            .await
        );
        // Three more timers for the same tick, registered (polled once) by this task.
        let mut victims = Vec::new();
        for _ in 0..3 {
            let mut s = Box::pin(monoio::time::sleep_until(at));
            assert!(poll_fn(|cx| Poll::Ready(s.as_mut().poll(cx).is_pending())).await);
            victims.push(s);
        }
        let kept = Rc::new(std::cell::RefCell::new(victims.pop().unwrap()));
        let kept2 = kept.clone();
        let in_pass = Rc::new(Cell::new(false));
        let in_pass2 = in_pass.clone();
        IN_WAKE.with(|f| {
            *f.borrow_mut() = Some(Box::new(move || {
                // The others are due in this pass and have not fired yet.
                in_pass2.set(victims.iter().all(|s| !s.is_elapsed()));
                // Two are dropped, one is pushed out by 20 ms.
                victims.clear();
                kept2.borrow_mut().as_mut().reset(at + 20 * MS);
            }));
        });

        with_hook(first).await;
        assert!(
            IN_WAKE.with(|f| f.borrow().is_none()),
            "the hook did not run"
        );
        assert!(
            in_pass.get(),
            "the other timers fired before the hook ran: the test did not test the pending list"
        );

        let kept = Rc::try_unwrap(kept).expect("hook is gone").into_inner();
        kept.await;
        let now = Instant::now();
        assert!(
            now >= at + 20 * MS,
            "the reset timer fired {:?} early",
            at + 20 * MS - now
        );
        assert!(now - at < LATE);
    });
}
