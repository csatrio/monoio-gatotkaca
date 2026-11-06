#![cfg(feature = "metrics")]
//! `monoio::runtime_stats()`: scheduler counters (`metrics` feature).

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRentExt},
    io_uring::opcode,
    net::{AcceptFlags, TcpListener, TcpStream, UnixStream},
    IoUringDriver, RuntimeBuilder,
};

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

async fn nop() {
    let (cqe, _) = unsafe { monoio::uring::submit(opcode::Nop::new().build(), ()) }
        .unwrap()
        .await;
    assert_eq!(cqe.res(), 0);
}

/// Two tasks ping-pong one byte over a socket pair: every read and write is polled (`Pending`)
/// before its CQE arrives, so each one is an op completion that wakes its task.
#[test]
fn ops_completed_counts_waiting_ops() {
    const ROUNDS: u64 = 32;
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        monoio::reset_runtime_stats();
        let echo = monoio::spawn(async move {
            for _ in 0..ROUNDS {
                let (res, buf) = b.read_exact(vec![0u8; 1]).await;
                res.unwrap();
                b.write_all(buf).await.0.unwrap();
            }
        });
        let client = monoio::spawn(async move {
            for i in 0..ROUNDS {
                a.write_all(vec![i as u8]).await.0.unwrap();
                let (res, buf) = a.read_exact(vec![0u8; 1]).await;
                res.unwrap();
                assert_eq!(buf, [i as u8]);
            }
        });
        echo.await;
        client.await;
    });
    // Read after `block_on`: the polls of a round are added when the round ends.
    let s = rt.stats();
    // Per round: the client's write and read, the echo's read and write.
    assert!(s.ops_completed >= 4 * ROUNDS, "{s:?}");
    // Each of those completions costs (at least) one poll of the task that waited for it.
    assert!(s.tasks_polled >= s.ops_completed, "{s:?}");

    rt.reset_stats();
    assert_eq!(rt.stats().ops_completed, 0);
}

/// The exact definition: one count per CQE that finds a waker, none for a CQE that arrives
/// before the first poll of its future.
#[test]
fn ops_completed_needs_a_waiting_waker() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        monoio::spawn(async {
            monoio::reset_runtime_stats();
            for _ in 0..N {
                nop().await;
            }
            let s = monoio::runtime_stats();
            assert_eq!(s.ops_completed, N, "{s:?}");

            // Pushed but not polled: its CQE is reaped by the park of the next `nop().await`
            // while nobody waits for it.
            monoio::reset_runtime_stats();
            let early = unsafe { monoio::uring::submit(opcode::Nop::new().build(), ()) }.unwrap();
            nop().await;
            let (cqe, _) = early.await;
            assert_eq!(cqe.res(), 0);
            let s = monoio::runtime_stats();
            assert_eq!(s.ops_completed, 1, "{s:?}");
        })
        .await;
    });
}

/// CQEs of a multishot op are not op completions in this sense.
#[test]
fn ops_completed_skips_multishot() {
    const CONNS: usize = 8;
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut incoming = listener.accept_multi(AcceptFlags::default()).unwrap();
        // Blocking connects: they complete against the listen backlog, without the ring.
        let clients: Vec<_> = (0..CONNS)
            .map(|_| std::net::TcpStream::connect(addr).unwrap())
            .collect();

        monoio::reset_runtime_stats();
        let mut accepted = Vec::new();
        for i in 0..CONNS {
            match incoming.next_stream().await.unwrap() {
                // Multishot accept needs Linux 5.19.
                Err(e) if i == 0 && e.raw_os_error() == Some(libc::EINVAL) => {
                    eprintln!("skipped: multishot accept unsupported: {e}");
                    return;
                }
                r => accepted.push(r.unwrap()),
            }
        }
        let s = monoio::runtime_stats();
        assert_eq!(s.ops_completed, 0, "{s:?}");

        // A single-shot op next to it is counted.
        nop().await;
        assert_eq!(monoio::runtime_stats().ops_completed, 1);
        drop(clients);
    });
}

/// Zero-copy sends post two CQEs (result, notification): counted per wake, not per CQE.
#[test]
fn ops_completed_zero_copy_counts_wakes() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let (_peer, _) = listener.accept().unwrap();

        // `write_zc` resolves at the notification: one wake.
        monoio::reset_runtime_stats();
        let (res, _) = stream.write_zc(vec![1u8; 256]).await;
        match res {
            // SEND_ZC needs Linux 6.0.
            Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
                eprintln!("skipped: zero-copy send unsupported: {e}");
                return;
            }
            r => assert_eq!(r.unwrap(), 256),
        }
        let s = monoio::runtime_stats();
        assert_eq!(s.ops_completed, 1, "{s:?}");

        // `send_zc_early`: one wake for the result, and one more for the notification if
        // `pending` has to wait for it.
        monoio::reset_runtime_stats();
        let (res, pending) = stream.send_zc_early(vec![2u8; 256]).await;
        assert_eq!(res.unwrap(), 256);
        let s = monoio::runtime_stats();
        assert_eq!(s.ops_completed, 1, "{s:?}");
        pending.await;
        let s = monoio::runtime_stats();
        assert!((1..=2).contains(&s.ops_completed), "{s:?}");
    });
}

#[test]
#[should_panic]
fn outside_runtime_panics() {
    let _ = monoio::runtime_stats();
}
