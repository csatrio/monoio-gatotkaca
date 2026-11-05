//! Cross-ring messages: `uring::msg_ring`, `RingHandle`, `Mailbox`.

use std::{
    cell::RefCell,
    os::fd::AsRawFd,
    rc::Rc,
    sync::mpsc,
    task::{Poll, Waker},
    thread,
};

use monoio::{
    uring::{self, libc, opcode, Mailbox, RingHandle},
    IoUringDriver, RuntimeBuilder,
};

type Builder = RuntimeBuilder<IoUringDriver>;

const N: u64 = 1000;

/// `IORING_OP_MSG_RING` needs Linux 5.18; call inside a runtime.
fn supported() -> bool {
    let ok = unsafe {
        uring::with_ring(|ring| {
            let mut probe = uring::Probe::new();
            ring.submitter().register_probe(&mut probe).is_ok()
                && probe.is_supported(opcode::MsgRingData::CODE)
        })
    };
    if !ok {
        eprintln!("skipped: IORING_OP_MSG_RING unsupported");
    }
    ok
}

#[monoio::test]
async fn message_to_own_ring() {
    if !supported() {
        return;
    }
    let me = uring::ring_handle().unwrap();
    let mailbox = uring::msg_receiver();
    uring::msg_ring(&me, 7, 42).await.unwrap();
    uring::msg_ring(&me, uring::MAX_ID, -3).await.unwrap();
    assert_eq!(mailbox.recv().await, (7, 42));
    assert_eq!(mailbox.recv().await, (uring::MAX_ID, -3));
    assert!(mailbox.is_empty());

    uring::msg_ring_nowait(&me, 8, 1).unwrap();
    assert_eq!(mailbox.recv().await, (8, 1));
}

#[monoio::test]
async fn not_a_ring_fails() {
    if !supported() {
        return;
    }
    let file = tempfile::tempfile().unwrap();
    let bogus = RingHandle::new(&file).unwrap();
    assert_ne!(bogus.as_raw_fd(), file.as_raw_fd());
    let err = uring::msg_ring(&bogus, 1, 1).await.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADFD));
    // Fire-and-forget: the error completion is dropped, the runtime keeps going.
    uring::msg_ring_nowait(&bogus, 1, 1).unwrap();
    let me = uring::ring_handle().unwrap();
    let mailbox = uring::msg_receiver();
    uring::msg_ring(&me, 2, 2).await.unwrap();
    assert_eq!(mailbox.recv().await, (2, 2));
    assert!(mailbox.is_empty());
}

#[monoio::test]
async fn mailbox_in_own_hook() {
    if !supported() {
        return;
    }
    let mailbox = Mailbox::new(100..=199);
    let others = Rc::new(RefCell::new(Vec::new()));
    let (mb, rest) = (mailbox.clone(), others.clone());
    uring::set_cqe_hook(move |id, cqe| {
        if !mb.offer(id, cqe) {
            rest.borrow_mut().push((id, cqe.res()));
        }
    });
    let me = uring::ring_handle().unwrap();
    for id in [99, 100, 150, 199, 200] {
        uring::msg_ring(&me, id, id as i32).await.unwrap();
    }
    assert_eq!(mailbox.recv().await, (100, 100));
    assert_eq!(mailbox.try_recv(), Some((150, 150)));
    assert_eq!(mailbox.try_recv(), Some((199, 199)));
    assert_eq!(mailbox.try_recv(), None);
    assert_eq!(*others.borrow(), [(99, 99), (200, 200)]);
}

enum Recv {
    Hook,
    Mailbox,
}

/// The target: runs on its own thread, hands out its ring handle, receives `N` messages with
/// ids `base..base+N` and res `id * 2`, and returns them in arrival order.
fn target(builder: fn() -> Builder, how: Recv) -> (RingHandle, thread::JoinHandle<Vec<u64>>) {
    let (tx, rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let mut rt = builder().build().unwrap();
        rt.block_on(async move {
            if !supported() {
                drop(tx);
                return Vec::new();
            }
            let mut got = Vec::new();
            match how {
                Recv::Hook => {
                    let state = Rc::new(RefCell::new((Vec::new(), None::<Waker>)));
                    let st = state.clone();
                    uring::set_cqe_hook(move |id, cqe| {
                        assert_eq!(cqe.res() as u64, id * 2);
                        assert_eq!(cqe.flags(), 0);
                        let mut st = st.borrow_mut();
                        st.0.push(id);
                        if let Some(w) = st.1.take() {
                            w.wake();
                        }
                    });
                    tx.send(uring::ring_handle().unwrap()).unwrap();
                    std::future::poll_fn(|cx| {
                        let mut st = state.borrow_mut();
                        if st.0.len() as u64 >= N {
                            Poll::Ready(())
                        } else {
                            st.1 = Some(cx.waker().clone());
                            Poll::Pending
                        }
                    })
                    .await;
                    got = std::mem::take(&mut state.borrow_mut().0);
                }
                Recv::Mailbox => {
                    let mailbox = uring::msg_receiver();
                    tx.send(uring::ring_handle().unwrap()).unwrap();
                    for _ in 0..N {
                        let (id, res) = mailbox.recv().await;
                        assert_eq!(res as u64, id * 2);
                        got.push(id);
                    }
                }
            }
            got
        })
    });
    let handle = rx.recv().unwrap_or_else(|_| {
        // Unsupported kernel: the target returned early.
        RingHandle::new(&tempfile::tempfile().unwrap()).unwrap()
    });
    (handle, join)
}

/// The sender: another runtime thread, sending `N` messages, awaited or fire-and-forget.
fn send_all(target: RingHandle, nowait: bool) {
    thread::spawn(move || {
        Builder::new().build().unwrap().block_on(async move {
            for id in 0..N {
                let res = (id * 2) as i32;
                // Fire-and-forget, except the last one: awaiting it flushes the queue (the target
                // CQ holds 2 * entries, more than N).
                if nowait && id + 1 < N {
                    uring::msg_ring_nowait(&target, id, res).unwrap();
                } else {
                    uring::msg_ring(&target, id, res).await.unwrap();
                }
            }
        })
    })
    .join()
    .unwrap();
}

fn run(builder: fn() -> Builder, how: Recv, nowait: bool) {
    let (handle, join) = target(builder, how);
    // Sending to the dummy handle of an unsupported kernel would fail: skip.
    if join.is_finished() {
        let got = join.join().unwrap();
        if got.is_empty() {
            return;
        }
        panic!("target finished early");
    }
    send_all(handle, nowait);
    let got = join.join().unwrap();
    assert_eq!(got, (0..N).collect::<Vec<_>>());
}

#[test]
fn cross_thread_hook() {
    run(Builder::new, Recv::Hook, false);
}

#[test]
fn cross_thread_mailbox() {
    run(Builder::new, Recv::Mailbox, false);
}

#[test]
fn cross_thread_nowait() {
    run(Builder::new, Recv::Hook, true);
}

#[test]
fn single_issuer_target() {
    run(|| Builder::new().single_issuer(), Recv::Mailbox, false);
}

#[test]
fn defer_taskrun_target() {
    run(|| Builder::new().defer_taskrun(), Recv::Hook, false);
}

#[test]
fn defer_taskrun_target_nowait() {
    run(|| Builder::new().defer_taskrun(), Recv::Mailbox, true);
}

/// A handle that outlives its runtime keeps the old ring alive: messages to it never reach the
/// runtime that reused the old fd number.
#[test]
fn handle_outlives_target() {
    for defer in [false, true] {
        let build = move || {
            if defer {
                Builder::new().defer_taskrun().build().unwrap()
            } else {
                Builder::new().build().unwrap()
            }
        };
        // The old runtime, on its own thread, which then exits.
        let old = thread::spawn(move || {
            let mut rt = build();
            let fd = rt.driver().as_raw_fd();
            rt.block_on(async { supported().then(|| uring::ring_handle().unwrap()) })
                .map(|h| (h, fd))
        })
        .join()
        .unwrap();
        let Some((old, old_fd)) = old else {
            return;
        };

        // The new runtime most likely gets the old fd number back.
        let mut rt = Builder::new().build().unwrap();
        let new_fd = rt.driver().as_raw_fd();
        assert_ne!(old.as_raw_fd(), new_fd);
        eprintln!(
            "old ring fd {old_fd}, new ring fd {new_fd}, handle fd {}",
            old.as_raw_fd()
        );
        rt.block_on(async {
            let mailbox = uring::msg_receiver();
            for _ in 0..10 {
                match uring::msg_ring(&old, 1, 1).await {
                    Ok(()) => {}
                    // A defer_taskrun ring whose thread exited.
                    Err(e) if e.raw_os_error() == Some(libc::EOWNERDEAD) => assert!(defer),
                    Err(e) => panic!("{e}"),
                }
            }
            let me = uring::ring_handle().unwrap();
            uring::msg_ring(&me, 2, 2).await.unwrap();
            assert_eq!(mailbox.recv().await, (2, 2));
            assert!(mailbox.is_empty());
        });
        drop(rt);
        // Still fine without any live runtime: the dup keeps the old ring.
        drop(old);
    }
}

#[test]
fn ring_handle_from_driver() {
    let rt = Builder::new().build().unwrap();
    let h = RingHandle::new(rt.driver()).unwrap();
    let h2 = h.clone();
    assert_eq!(h.as_raw_fd(), h2.as_raw_fd());
    assert_ne!(h.as_raw_fd(), rt.driver().as_raw_fd());
    fn assert_send_sync<T: Send + Sync + Clone>(_: &T) {}
    assert_send_sync(&h);
}
