#![cfg(feature = "sync")]
//! A cross-thread wake is delivered while the ready queue never empties (the runtime never
//! parks): the driver drains foreign wakers on the cold-path `submit()` too.

use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use monoio::{task::yield_now, IoUringDriver, RuntimeBuilder};

#[test]
fn foreign_wake_with_busy_queue() {
    for lifo in [false, true] {
        let mut rt = RuntimeBuilder::<IoUringDriver>::new()
            .task_budget(4)
            .lifo_slot(lifo)
            .build()
            .unwrap();
        let elapsed = rt.block_on(async {
            let start = Instant::now();
            let done = Rc::new(Cell::new(false));
            let fillers: Vec<_> = (0..8)
                .map(|_| {
                    let done = done.clone();
                    monoio::spawn(async move {
                        while !done.get() && start.elapsed() < Duration::from_secs(5) {
                            yield_now().await;
                        }
                    })
                })
                .collect();
            let (tx, rx) = futures::channel::oneshot::channel::<u32>();
            let sender = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                tx.send(7).unwrap();
            });
            let d = done.clone();
            let recv = monoio::spawn(async move {
                let v = rx.await.unwrap();
                d.set(true);
                v
            });
            assert_eq!(recv.await, 7);
            let elapsed = start.elapsed();
            for f in fillers {
                f.await;
            }
            sender.join().unwrap();
            elapsed
        });
        assert!(
            elapsed < Duration::from_secs(4),
            "lifo {lifo}: wake took {elapsed:?}"
        );
    }
}
