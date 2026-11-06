//! Opt-in coop budget: `RuntimeBuilder::coop_budget`, `task::consume_budget`,
//! `task::unconstrained`.

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use monoio::{
    task::{consume_budget, unconstrained, yield_now},
    uring::{self, opcode},
    IoUringDriver, RuntimeBuilder,
};

type Log = Rc<RefCell<Vec<char>>>;

/// Longest run of consecutive `'a'`s.
fn longest_a_run(log: &[char]) -> usize {
    log.split(|&c| c != 'a').map(|r| r.len()).max().unwrap_or(0)
}

/// Spawns a task that logs `'b'` and yields until `done`.
fn spawn_other(log: &Log, done: &Rc<Cell<bool>>) -> monoio::task::JoinHandle<()> {
    let (log, done) = (log.clone(), done.clone());
    monoio::spawn(async move {
        while !done.get() {
            log.borrow_mut().push('b');
            yield_now().await;
        }
    })
}

const OPS: usize = 11;

/// Task A: pushes `OPS - 1` NOPs, awaits one more NOP (all complete in the same tick), then
/// awaits the others, whose results are already queued. Returns the log and the results.
fn ready_ops(coop: Option<u32>) -> (Vec<char>, Vec<i32>) {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .coop_budget(coop)
        .build()
        .unwrap();
    rt.block_on(async {
        #[cfg(feature = "metrics")]
        monoio::reset_runtime_stats();
        let log: Log = Default::default();
        let done = Rc::new(Cell::new(false));
        let other = spawn_other(&log, &done);
        let (l, d) = (log.clone(), done.clone());
        let a = monoio::spawn(async move {
            let ops: Vec<_> = (0..OPS - 1)
                .map(|i| unsafe { uring::submit(opcode::Nop::new().build(), i) }.unwrap())
                .collect();
            let last = unsafe { uring::submit(opcode::Nop::new().build(), OPS - 1) }.unwrap();
            let mut results = vec![];
            let (cqe, i) = last.await;
            assert_eq!(i, OPS - 1);
            results.push(cqe.res());
            l.borrow_mut().push('a');
            for (n, op) in ops.into_iter().enumerate() {
                let (cqe, i) = op.await;
                assert_eq!(i, n, "data comes back with its op");
                results.push(cqe.res());
                l.borrow_mut().push('a');
            }
            d.set(true);
            results
        });
        let results = a.await;
        other.await;
        #[cfg(feature = "metrics")]
        {
            let s = monoio::runtime_stats();
            if coop.is_some() {
                assert!(s.coop_yields > 0, "{s:?}");
            } else {
                assert_eq!(s.coop_yields, 0, "{s:?}");
            }
        }
        let v = log.borrow().clone();
        (v, results)
    })
}

#[test]
fn op_results_yield_after_budget() {
    let (log, results) = ready_ops(Some(4));
    assert_eq!(results.len(), OPS, "no result lost");
    assert!(results.iter().all(|&r| r == 0), "{results:?}");
    assert_eq!(log.iter().filter(|&&c| c == 'a').count(), OPS);
    assert!(longest_a_run(&log) <= 4, "{log:?}");
    // Others ran in between.
    let first_a = log.iter().position(|&c| c == 'a').unwrap();
    assert!(log[first_a..].contains(&'b'), "{log:?}");
}

#[test]
fn op_results_without_coop() {
    let (log, results) = ready_ops(None);
    assert_eq!(results.len(), OPS);
    assert_eq!(longest_a_run(&log), OPS, "{log:?}");
}

fn budget_loop(coop: Option<u32>, exempt: bool) -> Vec<char> {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .coop_budget(coop)
        .build()
        .unwrap();
    rt.block_on(async {
        let log: Log = Default::default();
        let done = Rc::new(Cell::new(false));
        let other = spawn_other(&log, &done);
        let (l, d) = (log.clone(), done.clone());
        let a = monoio::spawn(async move {
            let work = async {
                for _ in 0..100 {
                    consume_budget().await;
                    l.borrow_mut().push('a');
                }
            };
            if exempt {
                unconstrained(work).await;
            } else {
                work.await;
            }
            // The outer budget is still in effect after `unconstrained`.
            d.set(true);
        });
        a.await;
        other.await;
        let v = log.borrow().clone();
        v
    })
}

#[test]
fn consume_budget_yields() {
    let log = budget_loop(Some(8), false);
    assert_eq!(log.iter().filter(|&&c| c == 'a').count(), 100);
    assert_eq!(longest_a_run(&log), 8, "{log:?}");
}

#[test]
fn consume_budget_off_by_default() {
    let log = budget_loop(None, false);
    assert_eq!(longest_a_run(&log), 100, "{log:?}");
}

#[test]
fn unconstrained_ignores_budget() {
    let log = budget_loop(Some(8), true);
    assert_eq!(longest_a_run(&log), 100, "{log:?}");
}

#[test]
fn unconstrained_does_not_charge_outer() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .coop_budget(Some(4))
        .build()
        .unwrap();
    rt.block_on(async {
        let log: Log = Default::default();
        let done = Rc::new(Cell::new(false));
        let other = spawn_other(&log, &done);
        let (l, d) = (log.clone(), done.clone());
        let a = monoio::spawn(async move {
            // Wait for a fresh poll, so the budget is full.
            yield_now().await;
            unconstrained(async {
                for _ in 0..50 {
                    consume_budget().await;
                }
            })
            .await;
            // 4 more units are left for this poll.
            for _ in 0..4 {
                consume_budget().await;
                l.borrow_mut().push('a');
            }
            d.set(true);
        });
        a.await;
        other.await;
        let log = log.borrow();
        assert_eq!(longest_a_run(&log), 4, "{log:?}");
        // All four in one poll: they are consecutive.
        let s: String = log.iter().collect();
        assert!(s.contains("aaaa"), "{s}");
    });
}

#[test]
fn chain_counts_once() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .coop_budget(Some(1))
        .build()
        .unwrap();
    rt.block_on(async {
        let a = monoio::spawn(async {
            let chain = unsafe {
                uring::submit_chain(
                    uring::link([opcode::Nop::new().build(), opcode::Nop::new().build()]),
                    7u32,
                )
            }
            .unwrap();
            let (cqes, data) = chain.await;
            assert_eq!(data, 7);
            assert!(cqes.iter().all(|c| c.res() == 0));
            // The budget of 1 is spent: the next ready result needs another poll, but arrives.
            let (cqe, _) = unsafe { uring::submit(opcode::Nop::new().build(), ()) }
                .unwrap()
                .await;
            assert_eq!(cqe.res(), 0);
        });
        a.await;
    });
}
