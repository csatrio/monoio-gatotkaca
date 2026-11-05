//! Regression: timers must fire while the ready queue never empties (the runtime then only takes
//! the cold path, `driver.submit()`, and never parks). Before the fix `TimeDriver::submit` did not
//! process timers, so with `task_budget` under saturation a `sleep`/`timeout` never completed.
use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

/// Returns Pending once after waking itself, so the task is immediately runnable again.
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

fn busy_runtime(budget: Option<usize>) -> Duration {
    let mut b = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new();
    if let Some(n) = budget {
        b = b.task_budget(n);
    }
    let mut rt = b.enable_timer().build().expect("runtime");
    rt.block_on(async {
        let stop = Rc::new(Cell::new(false));
        // Many tasks that always stay ready: each yields and is immediately runnable again.
        for _ in 0..64 {
            let stop = stop.clone();
            monoio::spawn(async move {
                while !stop.get() {
                    yield_once().await;
                }
            });
        }
        let t0 = Instant::now();
        let fired = monoio::time::timeout(
            Duration::from_secs(5),
            monoio::time::sleep(Duration::from_millis(50)),
        )
        .await;
        let took = t0.elapsed();
        stop.set(true);
        assert!(
            fired.is_ok(),
            "50 ms sleep did not finish within the 5 s timeout"
        );
        took
    })
}

/// Runs `busy_runtime` on its own thread with a wall-clock watchdog: without the fix the runtime's
/// own timers never fire, so a monoio timeout cannot end the test; the watchdog turns the hang
/// into a failure (the stuck thread is left behind; the test process exits).
fn with_watchdog(budget: Option<usize>) -> Duration {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(busy_runtime(budget));
    });
    rx.recv_timeout(Duration::from_secs(10))
        .unwrap_or_else(|_| {
            panic!("budget {budget:?}: a 50 ms sleep did not fire within 10 s (timers stalled)")
        })
}

#[test]
fn timer_fires_with_task_budget_and_busy_queue() {
    for budget in [1, 16, 61] {
        let took = with_watchdog(Some(budget));
        assert!(
            took < Duration::from_secs(1),
            "budget {budget}: 50 ms sleep took {took:?}"
        );
    }
}

#[test]
fn timer_fires_with_default_budget_and_busy_queue() {
    let took = with_watchdog(None);
    assert!(
        took < Duration::from_secs(1),
        "default budget: 50 ms sleep took {took:?}"
    );
}
