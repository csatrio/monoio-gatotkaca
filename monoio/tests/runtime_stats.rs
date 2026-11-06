#![cfg(feature = "metrics")]
//! `monoio::runtime_stats()`: scheduler counters (`metrics` feature).

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use monoio::{IoUringDriver, RuntimeBuilder};

/// Yields once: wakes itself and returns `Pending`, ready on the next poll.
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn yield_now() -> YieldNow {
    YieldNow(false)
}

const N: u64 = 64;

#[test]
fn spawn_yield_complete() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        monoio::reset_runtime_stats();
        let handles: Vec<_> = (0..N)
            .map(|_| {
                monoio::spawn(async {
                    for _ in 0..3 {
                        yield_now().await;
                    }
                })
            })
            .collect();
        for h in handles {
            h.await;
        }
        let s = monoio::runtime_stats();
        assert!(s.tasks_spawned >= N, "{s:?}");
        assert!(s.tasks_completed >= N, "{s:?}");
        assert!(s.rounds > 0, "{s:?}");
        assert!(s.tasks_polled >= N, "{s:?}");
        assert!(s.ready_queue_high_water >= N, "{s:?}");
        assert_eq!(s.polls_per_round.iter().sum::<u64>(), s.rounds, "{s:?}");
        assert_eq!(s.coop_yields, 0);
        assert_eq!(s.lifo_polls, 0);

        monoio::reset_runtime_stats();
        let r = monoio::runtime_stats();
        assert_eq!(r.tasks_spawned, 0, "{r:?}");
        assert_eq!(r.tasks_completed, 0, "{r:?}");
        assert_eq!(r.rounds, 0, "{r:?}");
        assert_eq!(r.tasks_polled, 0, "{r:?}");
        assert_eq!(r.ready_queue_high_water, 0, "{r:?}");
        assert_eq!(r.parks, 0, "{r:?}");
        assert_eq!(r.polls_per_round, [0; 11], "{r:?}");
    });
    // Same counters through the runtime handle.
    rt.block_on(async { monoio::spawn(yield_now()).await });
    let s = rt.stats();
    assert!(
        s.tasks_spawned >= 1 && s.tasks_completed >= 1 && s.rounds > 0,
        "{s:?}"
    );
    rt.reset_stats();
    assert_eq!(rt.stats().tasks_spawned, 0);
    assert_eq!(rt.stats().rounds, 0);
}

#[test]
fn budget_exhausted() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .task_budget(4)
        .build()
        .unwrap();
    rt.block_on(async {
        let handles: Vec<_> = (0..N)
            .map(|_| {
                monoio::spawn(async {
                    for _ in 0..4 {
                        yield_now().await;
                    }
                })
            })
            .collect();
        for h in handles {
            h.await;
        }
        let s = monoio::runtime_stats();
        assert!(s.budget_exhausted_rounds > 0, "{s:?}");
        assert!(s.submits > 0, "{s:?}");
        // With a budget of 4, no round polls more than 4 tasks (buckets 0..=3 only).
        assert_eq!(s.polls_per_round[4..].iter().sum::<u64>(), 0, "{s:?}");
    });
}

#[test]
fn park_time() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    rt.block_on(async {
        monoio::reset_runtime_stats();
        monoio::time::sleep(Duration::from_millis(20)).await;
        let s = monoio::runtime_stats();
        assert!(s.parks >= 1, "{s:?}");
        assert!(s.park_ns >= 10_000_000, "{s:?}");
    });
}

#[test]
#[should_panic]
fn outside_runtime_panics() {
    let _ = monoio::runtime_stats();
}
