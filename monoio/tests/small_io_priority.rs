//! `RuntimeBuilder::small_io_priority`: a task woken by a small completion runs before the
//! ready queue, unless it recently did bulk I/O.

use std::{cell::Cell, rc::Rc};

use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::UnixStream,
    task::yield_now,
    IoUringDriver, RuntimeBuilder,
};

const FILLERS: usize = 8;
/// Small wakes a bulk mark lasts (the runtime's `BULK_STICKY_WAKES`).
const STICKY: usize = 8;

/// One NOP round trip (result 0: a small completion).
async fn nop() {
    // SAFETY: a NOP points at no memory.
    let op = unsafe { monoio::uring::submit(monoio::uring::opcode::Nop::new().build(), ()) };
    let (cqe, ()) = op.unwrap().await;
    assert_eq!(cqe.result().unwrap(), 0);
}

/// Task `B` first reads 4 bytes from a socket if `bulk_read` (a completion of 4), then does
/// `nops` NOPs, logging after each. `FILLERS` tasks log and yield meanwhile. Returns the number
/// of filler entries between consecutive `B` entries (the first value counts from the start).
///
/// With `task_budget(FILLERS + 1)` a round is one pass over the fillers and `B`. A completion
/// wake at the back of the queue puts `B` last in the next round (gap `FILLERS` in steady
/// state); a priority wake puts it first. A switch from back to front shows as a gap of 0.
fn gaps(builder: RuntimeBuilder<IoUringDriver>, bulk_read: bool, nops: usize) -> Vec<usize> {
    let mut rt = builder.task_budget(FILLERS + 1).build().unwrap();
    rt.block_on(async move {
        let since_b = Rc::new(Cell::new(0usize));
        let gaps = Rc::new(std::cell::RefCell::new(Vec::new()));
        let done = Rc::new(Cell::new(false));
        let (mut tx, mut rx) = UnixStream::pair().unwrap();
        let (r, _) = tx.write_all(vec![7u8; 4]).await;
        r.unwrap();
        let mut handles = vec![];
        {
            let (since_b, gaps, done) = (since_b.clone(), gaps.clone(), done.clone());
            handles.push(monoio::spawn(async move {
                if bulk_read {
                    let (r, _) = rx.read(Vec::with_capacity(4)).await;
                    assert_eq!(r.unwrap(), 4);
                }
                for _ in 0..nops {
                    nop().await;
                    gaps.borrow_mut().push(since_b.replace(0));
                }
                done.set(true);
                drop(tx);
            }));
        }
        for _ in 0..FILLERS {
            let (since_b, done) = (since_b.clone(), done.clone());
            handles.push(monoio::spawn(async move {
                while !done.get() {
                    since_b.set(since_b.get() + 1);
                    yield_now().await;
                }
            }));
        }
        for h in handles {
            h.await;
        }
        let v = gaps.borrow().clone();
        v
    })
}

#[test]
fn default_keeps_completion_wakes_fifo() {
    let g = gaps(RuntimeBuilder::new(), false, 6);
    // After the first wake every completion wake waits behind all fillers: never a gap of 0.
    assert!(g[1..].iter().all(|&n| n == FILLERS), "{g:?}");
}

#[test]
fn small_completion_runs_before_the_queue() {
    let fifo = gaps(RuntimeBuilder::new(), false, 6);
    let prio = gaps(
        RuntimeBuilder::new().small_io_priority(Some(16384)),
        false,
        6,
    );
    // The first wake already jumps the queue.
    assert!(prio[0] < fifo[0], "prio {prio:?} fifo {fifo:?}");
    assert!(prio[1..].iter().all(|&n| n == FILLERS), "{prio:?}");
}

#[test]
fn bulk_completion_marks_the_task() {
    // The 4-byte read is bulk at a threshold of 2: the next 8 small wakes use the queue.
    let n = STICKY;
    let g = gaps(
        RuntimeBuilder::new().small_io_priority(Some(2)),
        true,
        n + 2,
    );
    let fifo = gaps(RuntimeBuilder::new(), true, n + 2);
    assert_eq!(g[0], fifo[0], "bulk {g:?} fifo {fifo:?}");
    // Wakes 1..=n: back of the queue; wake n+1 is a priority wake again (gap 0), then steady.
    assert!(g[1..n].iter().all(|&k| k == FILLERS), "{g:?}");
    assert_eq!(g[n], 0, "{g:?}");
    assert_eq!(g[n + 1], FILLERS, "{g:?}");
    // The same read below the threshold: priority from the start.
    let small = gaps(
        RuntimeBuilder::new().small_io_priority(Some(16384)),
        true,
        3,
    );
    assert!(small[0] < fifo[0], "small {small:?} fifo {fifo:?}");
}

#[cfg(feature = "metrics")]
#[test]
fn prio_polls_counted() {
    let run = |b: RuntimeBuilder<IoUringDriver>| {
        let mut rt = b.build().unwrap();
        rt.block_on(async {
            for _ in 0..4 {
                nop().await;
            }
            let h = monoio::spawn(async {
                for _ in 0..4 {
                    nop().await;
                }
            });
            h.await;
        });
        rt.stats().prio_polls
    };
    assert_eq!(run(RuntimeBuilder::new()), 0);
    assert!(run(RuntimeBuilder::new().small_io_priority(Some(16384))) >= 4);
}
