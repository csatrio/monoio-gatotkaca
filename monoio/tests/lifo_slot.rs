//! `RuntimeBuilder::lifo_slot`: a task woken by the running task runs right after it.

use std::{
    cell::{Cell, RefCell},
    future::poll_fn,
    rc::Rc,
    task::{Poll, Waker},
};

use monoio::{task::yield_now, IoUringDriver, RuntimeBuilder};

type Log = Rc<RefCell<Vec<String>>>;

/// A one-shot local signal: `wait` until `set`.
#[derive(Clone, Default)]
struct Signal(Rc<(Cell<bool>, RefCell<Option<Waker>>)>);

impl Signal {
    fn set(&self) {
        self.0 .0.set(true);
        if let Some(w) = self.0 .1.borrow_mut().take() {
            w.wake();
        }
    }

    async fn wait(&self) {
        poll_fn(|cx| {
            if self.0 .0.get() {
                Poll::Ready(())
            } else {
                *self.0 .1.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

const FILLERS: usize = 8;

/// A chain t0 -> t1 -> ... -> t{len-1}: t0 starts after the fillers ran once, each wakes the next
/// through a [`Signal`]. Fillers log `f` and yield until the chain is done.
fn chain(builder: RuntimeBuilder<IoUringDriver>, len: usize) -> Vec<String> {
    let mut rt = builder.build().unwrap();
    rt.block_on(async move {
        let log: Log = Default::default();
        let done = Rc::new(Cell::new(false));
        let signals: Vec<Signal> = (0..len).map(|_| Signal::default()).collect();
        let mut handles = vec![];
        // t1.. first, so they are waiting when t0 runs.
        for i in 1..len {
            let (log, me, next) = (log.clone(), signals[i].clone(), signals.get(i + 1).cloned());
            let done = done.clone();
            handles.push(monoio::spawn(async move {
                me.wait().await;
                log.borrow_mut().push(format!("t{i}"));
                match next {
                    Some(next) => next.set(),
                    None => done.set(true),
                }
            }));
        }
        for _ in 0..FILLERS {
            let (log, done) = (log.clone(), done.clone());
            handles.push(monoio::spawn(async move {
                while !done.get() {
                    log.borrow_mut().push("f".into());
                    yield_now().await;
                }
            }));
        }
        let (l, next) = (log.clone(), signals.get(1).cloned());
        handles.push(monoio::spawn(async move {
            l.borrow_mut().push("t0".into());
            if let Some(next) = next {
                next.set();
            }
        }));
        // Join only the last link: with `sync` the main future is a task, and a join wake from
        // a link in the middle would take the LIFO slot (the latest wake wins it).
        let last = handles.swap_remove(len - 2);
        drop(handles);
        last.await;
        let v = log.borrow().clone();
        v
    })
}

fn pos(log: &[String], s: &str) -> usize {
    log.iter()
        .position(|x| x == s)
        .unwrap_or_else(|| panic!("{s} in {log:?}"))
}

#[test]
fn default_is_fifo() {
    for b in [None, Some(1), Some(4)] {
        let mut builder = RuntimeBuilder::new();
        if let Some(n) = b {
            builder = builder.task_budget(n);
        }
        let log = chain(builder, 3);
        let t0 = pos(&log, "t0");
        // The woken task waits behind the queued fillers.
        assert_eq!(log[t0 + 1], "f", "budget {b:?}: {log:?}");
        assert!(pos(&log, "t1") > t0 + FILLERS, "budget {b:?}: {log:?}");
    }
}

#[test]
fn hand_over_chain_runs_in_order() {
    for b in [None, Some(1), Some(2), Some(16)] {
        let mut builder = RuntimeBuilder::new().lifo_slot(true);
        if let Some(n) = b {
            builder = builder.task_budget(n);
        }
        let log = chain(builder, 3);
        let t0 = pos(&log, "t0");
        assert_eq!(
            &log[t0..t0 + 3],
            ["t0", "t1", "t2"],
            "budget {b:?}: {log:?}"
        );
    }
}

#[test]
fn slot_streak_is_capped() {
    let log = chain(RuntimeBuilder::new().lifo_slot(true), 6);
    let t0 = pos(&log, "t0");
    // Three slot polls after t0, then t4 goes to the back of the queue.
    assert_eq!(&log[t0..t0 + 4], ["t0", "t1", "t2", "t3"], "{log:?}");
    assert_eq!(log[t0 + 4], "f", "{log:?}");
    // t4 was taken from the queue: its hand-over to t5 uses the slot again.
    let t4 = pos(&log, "t4");
    assert_eq!(log[t4 + 1], "t5", "{log:?}");
}

#[test]
fn displaced_slot_task_goes_to_queue() {
    // A wakes B then C: C takes the slot, B goes to the back of the queue.
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .lifo_slot(true)
        .build()
        .unwrap();
    let log = rt.block_on(async {
        let log: Log = Default::default();
        let (sb, sc) = (Signal::default(), Signal::default());
        let mut handles = vec![];
        for (name, s) in [("b", sb.clone()), ("c", sc.clone())] {
            let l = log.clone();
            handles.push(monoio::spawn(async move {
                s.wait().await;
                l.borrow_mut().push(name.into());
            }));
        }
        let l = log.clone();
        handles.push(monoio::spawn(async move {
            yield_now().await;
            l.borrow_mut().push("f".into());
        }));
        let l = log.clone();
        handles.push(monoio::spawn(async move {
            l.borrow_mut().push("a".into());
            sb.set();
            sc.set();
        }));
        for h in handles {
            h.await;
        }
        let v = log.borrow().clone();
        v
    });
    assert_eq!(log, ["a", "c", "f", "b"]);
}

#[cfg(feature = "metrics")]
#[test]
fn lifo_polls_counted() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .lifo_slot(true)
        .build()
        .unwrap();
    rt.block_on(async {
        monoio::reset_runtime_stats();
        let s = Signal::default();
        let s2 = s.clone();
        let h = monoio::spawn(async move { s2.wait().await });
        let a = monoio::spawn(async move { s.set() });
        a.await;
        h.await;
        assert!(monoio::runtime_stats().lifo_polls >= 1);
    });
}
