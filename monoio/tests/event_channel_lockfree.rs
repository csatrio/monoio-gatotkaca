//! The lock-free `event_channel`: many producers, capacity limits, the `MSG_RING` wakeup path,
//! and drops of messages nobody received.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use monoio::{
    time::timeout,
    uring::{self, EventChannelBuilder, SendError},
    IoUringDriver, RuntimeBuilder,
};

/// Fails the test instead of hanging if a wakeup is lost.
const LIMIT: Duration = Duration::from_secs(30);

#[monoio::test(timer_enabled = true)]
async fn many_producers_keep_per_sender_order() {
    const SENDERS: u64 = 8;
    const N: u64 = 50_000;
    let (tx, mut rx) = uring::event_channel::<u64>().unwrap();
    let threads: Vec<_> = (0..SENDERS)
        .map(|s| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..N {
                    // Full only if the receiver falls far behind: back off and retry.
                    let mut v = s << 32 | i;
                    while let Err(SendError::Full(back)) = tx.send(v) {
                        v = back;
                        thread::yield_now();
                    }
                    // Pause now and then, so the receiver keeps going back to sleep.
                    if i % 4096 == 0 {
                        thread::sleep(Duration::from_micros(200));
                    }
                }
            })
        })
        .collect();
    drop(tx);

    let mut next = [0u64; SENDERS as usize];
    let received = timeout(LIMIT, async {
        let mut n = 0;
        while let Some(v) = rx.recv().await {
            let (s, i) = ((v >> 32) as usize, v & u32::MAX as u64);
            assert_eq!(i, next[s], "sender {s} out of order");
            next[s] += 1;
            n += 1;
        }
        n
    })
    .await
    .expect("lost wakeup: receiver hung");
    assert_eq!(received, SENDERS * N);
    for t in threads {
        t.join().unwrap();
    }
    assert!(rx.wakeups() < SENDERS * N / 10, "wakeups {}", rx.wakeups());
}

#[monoio::test]
async fn full_then_room_again() {
    let (tx, mut rx) = EventChannelBuilder::new()
        .initial_capacity(4)
        .max_capacity(4)
        .build::<u32>()
        .unwrap();
    for i in 0..4 {
        tx.send(i).unwrap();
    }
    assert_eq!(tx.send(4), Err(SendError::Full(4)));
    assert_eq!(rx.stats().full, 1);
    assert_eq!(rx.recv().await, Some(0));
    // The node comes back once the receiver returns its batch (when it next looks for more).
    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, Some(2));
    assert_eq!(rx.recv().await, Some(3));
    assert_eq!(rx.try_recv(), None);
    for i in 10..14 {
        tx.send(i).unwrap();
    }
    assert_eq!(tx.send(14), Err(SendError::Full(14)));
}

#[monoio::test]
async fn grows_up_to_max_capacity() {
    let (tx, mut rx) = EventChannelBuilder::new()
        .initial_capacity(2)
        .max_capacity(1000)
        .build::<u32>()
        .unwrap();
    assert_eq!(rx.stats().capacity, 2);
    for i in 0..1000 {
        tx.send(i).unwrap();
    }
    assert_eq!(rx.stats().capacity, 1000);
    assert!(matches!(tx.send(1000), Err(SendError::Full(_))));
    for i in 0..1000 {
        assert_eq!(rx.recv().await, Some(i));
    }
}

#[test]
fn builder_rejects_bad_capacity() {
    let bad = |b: EventChannelBuilder| {
        let err = b.build::<u8>().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    };
    bad(EventChannelBuilder::new().initial_capacity(0));
    bad(EventChannelBuilder::new().max_capacity(0));
    bad(EventChannelBuilder::new().max_capacity((1 << 31) + 1));
    bad(EventChannelBuilder::new()
        .initial_capacity(64)
        .max_capacity(16));
}

/// A sender inside another runtime wakes the receiver with `MSG_RING`, not the eventfd.
#[monoio::test(timer_enabled = true)]
async fn ring_thread_sender_uses_msg_ring() {
    const N: u32 = 20_000;
    let (tx, mut rx) = uring::event_channel::<u32>().unwrap();
    let sender = thread::spawn(move || {
        let mut rt = RuntimeBuilder::<IoUringDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async move {
            for i in 0..N {
                tx.send(i).unwrap();
                // Let the ring submit now and then, so the receiver sleeps in between.
                if i % 1000 == 0 {
                    monoio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
    });
    timeout(LIMIT, async {
        for i in 0..N {
            assert_eq!(rx.recv().await, Some(i));
        }
        assert_eq!(rx.recv().await, None);
    })
    .await
    .expect("lost wakeup: receiver hung");
    sender.join().unwrap();
    let stats = rx.stats();
    assert!(stats.ring_wakeups > 0, "{stats:?}");
    // The last sender's drop happens after the runtime exits: an eventfd wakeup at most.
    assert!(stats.eventfd_wakeups <= 1, "{stats:?}");
}

/// A sender on the receiver's own ring rings the bell directly.
#[monoio::test(timer_enabled = true)]
async fn same_ring_sender_rings_directly() {
    let (tx, mut rx) = uring::event_channel::<u32>().unwrap();
    let task = monoio::spawn(async move {
        for i in 0..3 {
            monoio::time::sleep(Duration::from_millis(5)).await;
            tx.send(i).unwrap();
        }
    });
    for i in 0..3 {
        assert_eq!(rx.recv().await, Some(i));
    }
    task.await;
    assert_eq!(rx.recv().await, None);
    let stats = rx.stats();
    assert_eq!(stats.eventfd_wakeups, 0, "{stats:?}");
    assert!(stats.ring_wakeups >= 3, "{stats:?}");
}

/// A target ring built with `defer_taskrun` still gets its bell.
#[test]
fn ring_sender_to_defer_taskrun_receiver() {
    let mut rt = match RuntimeBuilder::<IoUringDriver>::new()
        .defer_taskrun()
        .enable_timer()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("skipped: {e}");
            return;
        }
    };
    rt.block_on(async {
        let (tx, mut rx) = uring::event_channel::<u32>().unwrap();
        let sender = thread::spawn(move || {
            let mut rt = RuntimeBuilder::<IoUringDriver>::new()
                .enable_timer()
                .build()
                .unwrap();
            rt.block_on(async move {
                for i in 0..100 {
                    monoio::time::sleep(Duration::from_micros(500)).await;
                    tx.send(i).unwrap();
                }
            });
        });
        timeout(LIMIT, async {
            for i in 0..100 {
                assert_eq!(rx.recv().await, Some(i));
            }
        })
        .await
        .expect("lost wakeup: receiver hung");
        sender.join().unwrap();
        assert!(rx.stats().ring_wakeups > 0);
    });
}

/// The receiver can move to another runtime on the same thread: the bell moves with it.
#[test]
fn receiver_rebinds_to_a_new_runtime() {
    let (tx, mut rx) = uring::event_channel::<u32>().unwrap();
    let tx2 = tx.clone();
    let build = || {
        RuntimeBuilder::<IoUringDriver>::new()
            .enable_timer()
            .build()
            .unwrap()
    };
    let send_later = |tx: uring::EventSender<u32>, v| {
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(v).unwrap();
        })
    };
    let t = send_later(tx, 1);
    assert_eq!(build().block_on(rx.recv()), Some(1));
    t.join().unwrap();
    let t = send_later(tx2, 2);
    assert_eq!(build().block_on(rx.recv()), Some(2));
    t.join().unwrap();
}

struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[monoio::test]
async fn unreceived_messages_are_dropped() {
    let drops = Arc::new(AtomicUsize::new(0));
    // Receiver gone first: queued messages die with the last sender.
    let (tx, mut rx) = uring::event_channel::<Counted>().unwrap();
    for _ in 0..10 {
        tx.send(Counted(drops.clone())).ok().unwrap();
    }
    // Take one batch partly: the rest of it is dropped with the receiver.
    drop(rx.recv().await.unwrap());
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 10);
    assert!(matches!(
        tx.send(Counted(drops.clone())),
        Err(SendError::Disconnected(_))
    ));
    drop(tx);
    assert_eq!(drops.load(Ordering::Relaxed), 11);

    // Senders gone first: the receiver drops what it never took.
    drops.store(0, Ordering::Relaxed);
    let (tx, rx) = uring::event_channel::<Counted>().unwrap();
    for _ in 0..5 {
        tx.send(Counted(drops.clone())).ok().unwrap();
    }
    drop(tx);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 5);
}

/// Every message lands while the receiver goes to sleep: the arm/re-check race on each one.
#[monoio::test(timer_enabled = true)]
async fn ping_pong_never_loses_a_wakeup() {
    const ROUNDS: u32 = 20_000;
    let (tx, mut rx) = uring::event_channel::<u32>().unwrap();
    let (ack_tx, ack_rx) = std::sync::mpsc::channel::<u32>();
    let t = thread::spawn(move || {
        for i in 0..ROUNDS {
            tx.send(i).unwrap();
            assert_eq!(ack_rx.recv().unwrap(), i);
        }
    });
    timeout(LIMIT, async {
        for i in 0..ROUNDS {
            assert_eq!(rx.recv().await, Some(i));
            ack_tx.send(i).unwrap();
        }
    })
    .await
    .expect("lost wakeup: receiver hung");
    t.join().unwrap();
}
