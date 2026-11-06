//! `monoio::task::yield_now`: the task goes to the back of the ready queue.

use std::{cell::RefCell, rc::Rc};

use monoio::{IoUringDriver, RuntimeBuilder};

type Log = Rc<RefCell<Vec<&'static str>>>;

fn run(builder: RuntimeBuilder<IoUringDriver>, with_yield: bool) -> Vec<&'static str> {
    let mut rt = builder.build().unwrap();
    rt.block_on(async move {
        let log: Log = Default::default();
        let l = log.clone();
        let a = monoio::spawn(async move {
            l.borrow_mut().push("a1");
            if with_yield {
                monoio::task::yield_now().await;
            }
            l.borrow_mut().push("a2");
        });
        let l = log.clone();
        let b = monoio::spawn(async move {
            l.borrow_mut().push("b");
        });
        a.await;
        b.await;
        let v = log.borrow().clone();
        v
    })
}

#[test]
fn yield_lets_others_run() {
    assert_eq!(
        run(RuntimeBuilder::new(), true),
        ["a1", "b", "a2"],
        "B runs before A resumes"
    );
    assert_eq!(run(RuntimeBuilder::new(), false), ["a1", "a2", "b"]);
}

#[test]
fn yield_ignores_lifo_slot() {
    // A waking itself must not land in the LIFO slot (that would make the yield a no-op).
    let b = RuntimeBuilder::new().lifo_slot(true);
    assert_eq!(run(b, true), ["a1", "b", "a2"]);
    let b = RuntimeBuilder::new().lifo_slot(true).task_budget(1);
    assert_eq!(run(b, true), ["a1", "b", "a2"]);
}

#[test]
fn yield_with_small_budget() {
    let b = RuntimeBuilder::new().task_budget(1);
    assert_eq!(run(b, true), ["a1", "b", "a2"]);
}
