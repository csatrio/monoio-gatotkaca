//! `uring::event_channel`: any thread to a runtime thread, without the `sync` feature.

use std::{thread, time::Duration};

use monoio::{uring, IoUringDriver, RuntimeBuilder};

#[monoio::test]
async fn std_thread_to_runtime_in_order() {
    const N: u64 = 10_000;
    let (tx, mut rx) = uring::event_channel::<u64>().unwrap();
    let sender = thread::spawn(move || {
        for i in 0..N {
            tx.send(i).unwrap();
            if i % 1000 == 999 {
                // Let the receiver catch up and wait, so some sends have to wake it.
                thread::sleep(Duration::from_millis(2));
            }
        }
    });
    for i in 0..N {
        assert_eq!(rx.recv().await, Some(i));
    }
    // The last sender is gone once the thread ended.
    assert_eq!(rx.recv().await, None);
    sender.join().unwrap();
    // Far fewer wakeups than messages.
    assert!(
        rx.wakeups() >= 1 && rx.wakeups() < N / 10,
        "{}",
        rx.wakeups()
    );
}

#[monoio::test]
async fn closed_when_all_senders_dropped() {
    let (tx, mut rx) = uring::event_channel::<String>().unwrap();
    let tx2 = tx.clone();
    let t = thread::spawn(move || {
        tx2.send("a".into()).unwrap();
        drop(tx2);
        thread::sleep(Duration::from_millis(20));
        tx.send("b".into()).unwrap();
        // The receiver is waiting again: dropping the last sender wakes it.
        thread::sleep(Duration::from_millis(20));
        drop(tx);
    });
    assert_eq!(rx.recv().await.as_deref(), Some("a"));
    assert_eq!(rx.recv().await.as_deref(), Some("b"));
    assert_eq!(rx.recv().await, None);
    assert!(rx.is_closed());
    assert_eq!(rx.recv().await, None);
    t.join().unwrap();
}

#[monoio::test]
async fn send_after_receiver_dropped() {
    let (tx, rx) = uring::event_channel::<u8>().unwrap();
    assert!(!tx.is_closed());
    drop(rx);
    assert!(tx.is_closed());
    assert_eq!(tx.send(1), Err(uring::SendError::Disconnected(1)));
}

/// Sends cost no wakeup unless the receiver waits; a batch sent while it waits costs one.
#[monoio::test]
async fn batch_wakes_once() {
    let (tx, mut rx) = uring::event_channel::<u32>().unwrap();
    // `try_recv` doesn't arm a wakeup, so these sends ring nothing.
    assert_eq!(rx.try_recv(), None);
    let t = thread::spawn(move || {
        for i in 0..100 {
            tx.send(i).unwrap();
        }
        tx
    });
    let tx = t.join().unwrap();
    assert_eq!(rx.wakeups(), 0);
    for i in 0..100 {
        assert_eq!(rx.recv().await, Some(i));
    }
    // Not waiting (queue non-empty or never found empty since): no write.
    tx.send(100).unwrap();
    tx.send(101).unwrap();
    assert_eq!(rx.wakeups(), 0);
    assert_eq!(rx.recv().await, Some(100));
    assert_eq!(rx.recv().await, Some(101));
    // Now the receiver really waits on the ring: a burst of 50 costs one wakeup.
    let t = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        for i in 0..50 {
            tx.send(i).unwrap();
        }
        thread::sleep(Duration::from_millis(20));
    });
    assert_eq!(rx.recv().await, Some(0));
    assert_eq!(rx.wakeups(), 1);
    for i in 1..50 {
        assert_eq!(rx.recv().await, Some(i));
    }
    assert_eq!(rx.recv().await, None);
    assert_eq!(rx.wakeups(), 2);
    // A plain thread wakes through the eventfd.
    assert_eq!(rx.stats().eventfd_wakeups, 2);
    t.join().unwrap();
}

/// Dropping a `recv` future loses neither messages nor the armed read.
#[monoio::test(timer_enabled = true)]
async fn recv_is_cancel_safe() {
    let (tx, mut rx) = uring::event_channel::<u32>().unwrap();
    monoio::select! {
        _ = rx.recv() => panic!("nothing was sent"),
        _ = monoio::time::sleep(Duration::from_millis(10)) => {}
    }
    let t = thread::spawn(move || {
        tx.send(5).unwrap();
        tx
    });
    assert_eq!(rx.recv().await, Some(5));
    drop(t.join().unwrap());
    assert_eq!(rx.recv().await, None);
}

/// The receiver on a `defer_taskrun` ring, the receiver dropped while its read is armed.
#[test]
fn defer_taskrun_and_drop_while_armed() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .defer_taskrun()
        .enable_timer()
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, mut rx) = uring::event_channel::<u32>().unwrap();
        let t = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            tx.send(1).unwrap();
            tx
        });
        assert_eq!(rx.recv().await, Some(1));
        let tx = t.join().unwrap();
        monoio::select! {
            _ = rx.recv() => panic!("nothing was sent"),
            _ = monoio::time::sleep(Duration::from_millis(5)) => {}
        }
        drop(rx);
        assert_eq!(tx.send(2), Err(uring::SendError::Disconnected(2)));
        // Let the completed read be reaped.
        monoio::time::sleep(Duration::from_millis(5)).await;
    });
}

#[test]
fn sender_is_send_sync() {
    fn assert_send_sync<T: Send + Sync + Clone>() {}
    assert_send_sync::<uring::EventSender<Vec<u8>>>();
}
