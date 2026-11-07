//! Throughput of the lock-free queue behind `uring::event_channel` and the remote wakers.
//!
//! Two measurements, each run for several producer counts and reported as the best round:
//!
//! - `chan`: `N` producer threads send `messages` `u64`s in total into one `event_channel`; the
//!   receiver thread runs a runtime and `recv().await`s them all. Reports messages per second
//!   and, with the `metrics` feature, how many wakeups (eventfd or `MSG_RING`) they cost.
//! - `wake` (feature `sync`): `tasks` tasks on the runtime each wait on a slot; `N` producer
//!   threads wake the slots round-robin until `messages` wakes were delivered. Reports wakes
//!   per second. This is the path a foreign waker (another thread's `wake()`) takes.
//!
//! The receiver runtime is pinned to `--cpu`, the producers to `--producer-cpus` round-robin.
//!
//! ```text
//! cargo run --release -p adaptif-gatotkaca-monoio --features sync --example lfq_bench -- \
//!     --cpu 2 --producer-cpus 3,4,5,6,7,8,9,10 --producers 1,4,8 --rounds 5
//! ```

use std::{
    future::poll_fn,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Poll, Waker},
    thread,
    time::{Duration, Instant},
};

use monoio::{uring::EventChannelBuilder, utils::bind_to_cpu_set};

struct Args {
    cpu: usize,
    producer_cpus: Vec<usize>,
    producers: Vec<usize>,
    messages: u64,
    rounds: usize,
    tasks: usize,
    which: Vec<String>,
}

fn parse() -> Args {
    let mut a = Args {
        cpu: 2,
        producer_cpus: vec![3, 4, 5, 6, 7, 8, 9, 10],
        producers: vec![1, 4, 8],
        messages: 1_000_000,
        rounds: 5,
        tasks: 256,
        which: vec!["chan".into(), "wake".into()],
    };
    let list =
        |s: &str| -> Vec<usize> { s.split(',').map(|x| x.trim().parse().unwrap()).collect() };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let v = it.next().unwrap_or_else(|| panic!("{k} needs a value"));
        match k.as_str() {
            "--cpu" => a.cpu = v.parse().unwrap(),
            "--producer-cpus" => a.producer_cpus = list(&v),
            "--producers" => a.producers = list(&v),
            "--messages" => a.messages = v.parse().unwrap(),
            "--rounds" => a.rounds = v.parse().unwrap(),
            "--tasks" => a.tasks = v.parse().unwrap(),
            "--only" => a.which = v.split(',').map(String::from).collect(),
            _ => panic!("unknown argument {k}"),
        }
    }
    a
}

fn pin(cpu: usize) {
    let _ = bind_to_cpu_set([cpu]);
}

/// One round of `chan`: wall time from the first send to the last receive, plus the wakeup count.
fn chan_round(a: &Args, producers: usize) -> (Duration, u64) {
    let (tx, mut rx) = EventChannelBuilder::new()
        .initial_capacity(1024)
        .max_capacity(131_072)
        .build::<u64>()
        .unwrap();
    let go = Arc::new(AtomicBool::new(false));
    let per = a.messages / producers as u64;
    let total = per * producers as u64;
    let handles: Vec<_> = (0..producers)
        .map(|i| {
            let tx = tx.clone();
            let go = go.clone();
            let cpu = a.producer_cpus[i % a.producer_cpus.len()];
            thread::spawn(move || {
                pin(cpu);
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                let mut v = 0u64;
                while v < per {
                    match tx.send(v) {
                        Ok(()) => v += 1,
                        Err(monoio::uring::SendError::Full(_)) => std::hint::spin_loop(),
                        Err(monoio::uring::SendError::Disconnected(_)) => return,
                    }
                }
            })
        })
        .collect();
    drop(tx);

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    let (elapsed, wakeups) = rt.block_on(async {
        let start = Instant::now();
        go.store(true, Ordering::Release);
        let mut n = 0u64;
        while n < total {
            rx.recv().await.expect("senders alive until done");
            n += 1;
        }
        let elapsed = start.elapsed();
        #[cfg(feature = "metrics")]
        let w = rx.wakeups();
        #[cfg(not(feature = "metrics"))]
        let w = 0;
        (elapsed, w)
    });
    for h in handles {
        h.join().unwrap();
    }
    (elapsed, wakeups)
}

/// A slot a task waits on and a foreign thread wakes.
struct Slot {
    pending: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

/// One round of `wake`: `a.tasks` tasks, `producers` threads waking them round-robin,
/// `a.messages` wakes in total; wall time from the first wake to the last delivery.
#[cfg(feature = "sync")]
fn wake_round(a: &Args, producers: usize) -> Duration {
    let slots: Vec<Arc<Slot>> = (0..a.tasks)
        .map(|_| {
            Arc::new(Slot {
                pending: AtomicBool::new(false),
                waker: Mutex::new(None),
            })
        })
        .collect();
    let delivered = Arc::new(AtomicU64::new(0));
    let go = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let per = a.messages / producers as u64;
    let total = per * producers as u64;

    let handles: Vec<_> = (0..producers)
        .map(|i| {
            let slots = slots.clone();
            let go = go.clone();
            let stop = stop.clone();
            let cpu = a.producer_cpus[i % a.producer_cpus.len()];
            thread::spawn(move || {
                pin(cpu);
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                // Each producer owns every `producers`-th slot, so a slot has one waker at a
                // time and no wake is lost.
                let mine: Vec<_> = slots.iter().skip(i).step_by(producers).cloned().collect();
                let mut sent = 0u64;
                let mut k = 0usize;
                while sent < per && !stop.load(Ordering::Relaxed) {
                    let s = &mine[k % mine.len()];
                    k += 1;
                    // Wait until the task consumed the previous wake of this slot.
                    while s.pending.load(Ordering::Acquire) {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        std::hint::spin_loop();
                    }
                    s.pending.store(true, Ordering::Release);
                    if let Some(w) = s.waker.lock().unwrap().take() {
                        w.wake();
                    }
                    sent += 1;
                }
            })
        })
        .collect();

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    let elapsed = rt.block_on(async {
        let (done_tx, done_rx) = local_sync::oneshot::channel::<Duration>();
        let done_tx = std::rc::Rc::new(std::cell::RefCell::new(Some(done_tx)));
        let start = std::rc::Rc::new(std::cell::Cell::new(None::<Instant>));
        for s in &slots {
            let s = s.clone();
            let delivered = delivered.clone();
            let done_tx = done_tx.clone();
            let start = start.clone();
            monoio::spawn(async move {
                loop {
                    poll_fn(|cx| {
                        if s.pending.load(Ordering::Acquire) {
                            return Poll::Ready(());
                        }
                        *s.waker.lock().unwrap() = Some(cx.waker().clone());
                        // The producer may have set `pending` before the waker was stored.
                        if s.pending.load(Ordering::Acquire) {
                            s.waker.lock().unwrap().take();
                            return Poll::Ready(());
                        }
                        Poll::Pending
                    })
                    .await;
                    s.pending.store(false, Ordering::Release);
                    let n = delivered.fetch_add(1, Ordering::Relaxed) + 1;
                    if n == total {
                        if let Some(tx) = done_tx.borrow_mut().take() {
                            let _ = tx.send(start.get().unwrap().elapsed());
                        }
                    }
                }
            });
        }
        // Let the tasks register their wakers before the producers start.
        monoio::task::yield_now().await;
        monoio::task::yield_now().await;
        start.set(Some(Instant::now()));
        go.store(true, Ordering::Release);
        done_rx.await.unwrap()
    });
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    elapsed
}

fn main() {
    let a = parse();
    pin(a.cpu);
    println!(
        "lfq_bench: {} messages, best of {} rounds, receiver on cpu {}, producers on {:?}, \
         utils {}",
        a.messages,
        a.rounds,
        a.cpu,
        a.producer_cpus,
        option_env!("LFQ_LABEL").unwrap_or("?")
    );

    if a.which.iter().any(|w| w == "chan") {
        for &p in &a.producers {
            let mut best: Option<(Duration, u64)> = None;
            let mut all = Vec::new();
            for _ in 0..a.rounds {
                let r = chan_round(&a, p);
                all.push(r.0);
                if best.is_none_or(|b| r.0 < b.0) {
                    best = Some(r);
                }
            }
            let (d, w) = best.unwrap();
            let worst = all.iter().max().unwrap();
            let rate = a.messages as f64 / d.as_secs_f64() / 1e6;
            let worst_rate = a.messages as f64 / worst.as_secs_f64() / 1e6;
            println!(
                "RESULT chan producers={p} best_mmsg_s={rate:.2} worst_mmsg_s={worst_rate:.2} \
                 wakeups={w}"
            );
        }
    }

    #[cfg(feature = "sync")]
    if a.which.iter().any(|w| w == "wake") {
        for &p in &a.producers {
            let mut all = Vec::new();
            for _ in 0..a.rounds {
                all.push(wake_round(&a, p));
            }
            let best = all.iter().min().unwrap();
            let worst = all.iter().max().unwrap();
            let rate = a.messages as f64 / best.as_secs_f64() / 1e6;
            let worst_rate = a.messages as f64 / worst.as_secs_f64() / 1e6;
            println!(
                "RESULT wake producers={p} tasks={} best_mwake_s={rate:.2} \
                 worst_mwake_s={worst_rate:.2}",
                a.tasks
            );
        }
    }
    #[cfg(not(feature = "sync"))]
    if a.which.iter().any(|w| w == "wake") {
        println!("wake: needs the `sync` feature");
    }
}
