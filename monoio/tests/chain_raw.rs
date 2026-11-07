//! Linked SQE chains (`uring::submit_chain`) and raw link timeouts (`uring::submit_with_timeout`).

use std::{
    cell::Cell,
    io::{ErrorKind, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    rc::Rc,
    time::Duration,
};

use monoio::{
    uring::{self, opcode, squeue, types},
    IoUringDriver, RuntimeBuilder,
};

fn nop() -> squeue::Entry {
    opcode::Nop::new().build()
}

/// A read on a bad fd: fails with `EBADF`.
fn bad_read(buf: &mut [u8]) -> squeue::Entry {
    opcode::Read::new(types::Fd(-1), buf.as_mut_ptr(), buf.len() as _).build()
}

fn timespec(d: Duration) -> Box<types::Timespec> {
    Box::new(
        types::Timespec::new()
            .sec(d.as_secs())
            .nsec(d.subsec_nanos()),
    )
}

#[monoio::test]
async fn nop_chain() {
    #[cfg(feature = "metrics")]
    let before = monoio::driver_stats().chains_submitted;
    let op = unsafe { uring::submit_chain(uring::link([nop(), nop(), nop()]), "data") }.unwrap();
    let (cqes, data) = op.await;
    assert_eq!(data, "data");
    assert!(cqes.iter().all(|c| c.res() == 0), "{cqes:?}");
    #[cfg(feature = "metrics")]
    assert_eq!(monoio::driver_stats().chains_submitted, before + 1);
}

#[monoio::test]
async fn failed_link_cancels_the_rest() {
    let mut buf = vec![0u8; 8];
    let sqes = uring::link([bad_read(&mut buf), nop(), nop()]);
    let (cqes, _buf) = unsafe { uring::submit_chain(sqes, buf) }.unwrap().await;
    let res: Vec<i32> = cqes.iter().map(|c| c.res()).collect();
    assert_eq!(res, [-libc::EBADF, -libc::ECANCELED, -libc::ECANCELED]);
}

#[monoio::test]
async fn unlinked_sqes_run_independently() {
    let mut buf = vec![0u8; 8];
    let (cqes, _) = unsafe { uring::submit_chain([bad_read(&mut buf), nop()], buf) }
        .unwrap()
        .await;
    assert_eq!(cqes[0].res(), -libc::EBADF);
    assert_eq!(cqes[1].res(), 0);
}

#[monoio::test]
async fn invalid_chains_are_rejected() {
    let empty: [squeue::Entry; 0] = [];
    let err = unsafe { uring::submit_chain(empty, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);

    // The last SQE would link into the next, unrelated one.
    let open_end = [nop(), nop()].map(|e| e.flags(squeue::Flags::IO_LINK));
    let err = unsafe { uring::submit_chain(open_end, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let hard = [nop().flags(squeue::Flags::IO_HARDLINK)];
    let err = unsafe { uring::submit_chain(hard, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);

    // No CQE on success: its slot would never be freed.
    let skip = [nop().flags(squeue::Flags::SKIP_SUCCESS)];
    let err = unsafe { uring::submit_chain(skip, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let skip_last = [
        nop().flags(squeue::Flags::IO_LINK | squeue::Flags::SKIP_SUCCESS),
        nop().flags(squeue::Flags::SKIP_SUCCESS),
    ];
    let err = unsafe { uring::submit_chain(skip_last, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    // SKIP_SUCCESS on some members only: the kernel's CQEs follow no simple rule.
    let mixed = [
        nop().flags(squeue::Flags::IO_LINK | squeue::Flags::SKIP_SUCCESS),
        nop().flags(squeue::Flags::IO_LINK),
        nop(),
    ];
    let err = unsafe { uring::submit_chain(mixed, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    // SKIP_SUCCESS without IO_LINK, or with IO_HARDLINK: a failed member would not cancel
    // the rest, so the last member would post a second CQE for the one slot.
    let unlinked = [nop().flags(squeue::Flags::SKIP_SUCCESS), nop()];
    let err = unsafe { uring::submit_chain(unlinked, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let hard = [
        nop().flags(squeue::Flags::IO_HARDLINK | squeue::Flags::SKIP_SUCCESS),
        nop(),
    ];
    let err = unsafe { uring::submit_chain(hard, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);

    // Nothing was queued: the next op works as usual.
    let (cqe, _) = unsafe { uring::submit(nop(), ()) }.unwrap().await;
    assert_eq!(cqe.res(), 0);
}

#[monoio::test(entries = 256)]
async fn chain_larger_than_the_sq() {
    let capacity = unsafe { uring::with_ring(|r| r.params().sq_entries()) } as usize;
    assert_eq!(capacity, 256);
    let sqes: [squeue::Entry; 257] = std::array::from_fn(|_| nop());
    let err = unsafe { uring::submit_chain(sqes, ()) }.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);

    // A chain filling the whole SQ is fine.
    let sqes: [squeue::Entry; 256] = uring::link(std::array::from_fn(|_| nop()));
    let (cqes, ()) = unsafe { uring::submit_chain(sqes, ()) }.unwrap().await;
    assert!(cqes.iter().all(|c| c.res() == 0));
}

/// With one free SQ slot left, the driver flushes the queue before pushing the chain rather than
/// in the middle of it: the link holds, so the failure of the first SQE cancels the second.
#[monoio::test(entries = 256)]
async fn chain_on_a_nearly_full_sq_is_not_split() {
    let capacity = unsafe { uring::with_ring(|r| r.params().sq_entries()) } as usize;
    let fillers: Vec<_> = (0..capacity - 1)
        .map(|_| unsafe { uring::submit(nop(), ()) }.unwrap())
        .collect();
    assert_eq!(
        unsafe { uring::with_ring(|r| r.submission().len()) },
        capacity - 1
    );
    #[cfg(feature = "metrics")]
    let flushes = monoio::driver_stats().sq_full_flushes;

    let mut buf = vec![0u8; 8];
    let sqes = uring::link([bad_read(&mut buf), nop()]);
    let op = unsafe { uring::submit_chain(sqes, buf) }.unwrap();
    #[cfg(feature = "metrics")]
    assert!(monoio::driver_stats().sq_full_flushes > flushes);
    // Both chain SQEs are queued together, after the flush.
    assert_eq!(unsafe { uring::with_ring(|r| r.submission().len()) }, 2);

    let (cqes, _) = op.await;
    assert_eq!(cqes[0].res(), -libc::EBADF);
    assert_eq!(cqes[1].res(), -libc::ECANCELED);
    for f in fillers {
        assert_eq!(f.await.0.res(), 0);
    }
}

/// Tracks when the chain data is dropped.
struct Tracked {
    dropped: Rc<Cell<bool>>,
    _ts: Box<types::Timespec>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

/// Wait about `d` without the timer, through a raw `Timeout` SQE.
async fn raw_sleep(d: Duration) {
    let ts = timespec(d);
    let entry = opcode::Timeout::new(&*ts).build();
    let (cqe, _) = unsafe { uring::submit(entry, ts) }.unwrap().await;
    assert_eq!(cqe.res(), -libc::ETIME);
}

#[monoio::test]
async fn dropped_chain_keeps_data_until_the_last_completion() {
    let dropped = Rc::new(Cell::new(false));
    let ts = timespec(Duration::from_millis(50));
    // A nop, and a timeout reading the timespec owned by the data. Not linked: the nop completes
    // first and the timeout keeps the data alive.
    let sqes = [nop(), opcode::Timeout::new(&*ts).build()];
    let data = Tracked {
        dropped: dropped.clone(),
        _ts: ts,
    };
    let op = unsafe { uring::submit_chain(sqes, data) }.unwrap();
    raw_sleep(Duration::from_millis(1)).await;
    drop(op);
    assert!(!dropped.get());

    // The timeout ends (cancelled with `async-cancel`, or after 50ms), then the data goes.
    for _ in 0..50 {
        if dropped.get() {
            break;
        }
        raw_sleep(Duration::from_millis(5)).await;
    }
    assert!(dropped.get());
}

#[monoio::test]
async fn link_timeout_in_a_chain() {
    let (a, _b) = UnixStream::pair().unwrap();
    let mut buf = vec![0u8; 8];
    let ts = timespec(Duration::from_millis(20));
    let sqes = uring::link([
        opcode::Recv::new(types::Fd(a.as_raw_fd()), buf.as_mut_ptr(), 8).build(),
        opcode::LinkTimeout::new(&*ts).build(),
    ]);
    let (cqes, _) = unsafe { uring::submit_chain(sqes, (buf, ts)) }
        .unwrap()
        .await;
    assert_eq!(cqes[0].res(), -libc::ECANCELED);
    assert_eq!(cqes[1].res(), -libc::ETIME);
}

#[monoio::test]
async fn submit_with_timeout_fires() {
    let (a, _b) = UnixStream::pair().unwrap();
    #[cfg(feature = "metrics")]
    let before = monoio::driver_stats().link_timeouts;
    let mut buf = vec![0u8; 8];
    let entry = opcode::Recv::new(types::Fd(a.as_raw_fd()), buf.as_mut_ptr(), 8).build();
    let (cqe, _) = unsafe { uring::submit_with_timeout(entry, buf, Duration::from_millis(20)) }
        .unwrap()
        .await;
    assert_eq!(cqe.res(), -libc::ECANCELED);
    // The timeout's CQE may come in a later tick.
    #[cfg(feature = "metrics")]
    for _ in 0..20 {
        if monoio::driver_stats().link_timeouts > before {
            break;
        }
        raw_sleep(Duration::from_millis(1)).await;
    }
    #[cfg(feature = "metrics")]
    assert_eq!(monoio::driver_stats().link_timeouts, before + 1);
}

#[monoio::test]
async fn submit_with_timeout_completes_in_time() {
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"hello").unwrap();
    #[cfg(feature = "metrics")]
    let before = monoio::driver_stats().link_timeouts;
    let mut buf = vec![0u8; 8];
    let entry = opcode::Recv::new(types::Fd(a.as_raw_fd()), buf.as_mut_ptr(), 8).build();
    let (cqe, buf) = unsafe { uring::submit_with_timeout(entry, buf, Duration::from_secs(5)) }
        .unwrap()
        .await;
    assert_eq!(cqe.res(), 5);
    assert_eq!(&buf[..5], b"hello");
    raw_sleep(Duration::from_millis(1)).await;
    #[cfg(feature = "metrics")]
    assert_eq!(monoio::driver_stats().link_timeouts, before);
}

#[test]
fn chain_on_the_driver_handle() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    let op = unsafe {
        rt.driver()
            .submit_chain_raw(uring::link([nop(), nop()]), 7u8)
            .unwrap()
    };
    rt.block_on(async {
        let (cqes, data) = op.await;
        assert_eq!(data, 7);
        assert_eq!(cqes.map(|c| c.res()), [0, 0]);
    });
}

// ---- the skip shape: one slot, one CQE ----

/// A `SEND` on a Unix socket, `MSG_WAITALL` so that a short send is a failure, not a skip.
fn send(fd: &UnixStream, data: &[u8]) -> squeue::Entry {
    opcode::Send::new(types::Fd(fd.as_raw_fd()), data.as_ptr(), data.len() as _)
        .flags(libc::MSG_NOSIGNAL | libc::MSG_WAITALL)
        .build()
}

fn recv(fd: &UnixStream, buf: &mut [u8]) -> squeue::Entry {
    opcode::Recv::new(types::Fd(fd.as_raw_fd()), buf.as_mut_ptr(), buf.len() as _).build()
}

/// All members succeed: the last member's CQE, `Cqe::new(0, 0)` for the skipped ones.
#[monoio::test]
async fn skip_chain_all_succeed() {
    let (a, mut b) = UnixStream::pair().unwrap();
    #[cfg(feature = "metrics")]
    let before = monoio::driver_stats();
    let (one, two, mut buf) = (b"one".to_vec(), b"two".to_vec(), vec![0u8; 16]);
    let sqes = uring::link_skip([send(&a, &one), send(&a, &two), recv(&a, &mut buf)]);
    let op = unsafe { uring::submit_chain(sqes, (one, two, buf)) }.unwrap();
    assert!(op.completed().iter().all(Option::is_none));
    // Let the sends run, then answer.
    raw_sleep(Duration::from_millis(5)).await;
    let mut got = [0u8; 6];
    std::io::Read::read_exact(&mut b, &mut got).unwrap();
    assert_eq!(&got, b"onetwo");
    b.write_all(b"back").unwrap();
    let (cqes, (_, _, buf)) = op.await;
    assert_eq!(cqes.map(|c| c.res()), [0, 0, 4], "{cqes:?}");
    assert_eq!(&buf[..4], b"back");
    #[cfg(feature = "metrics")]
    {
        let d = monoio::driver_stats();
        assert_eq!(d.chains_submitted, before.chains_submitted + 1);
        // One CQE for the chain (the sleep's timeout CQE is the other one).
        assert_eq!(d.cqes_reaped, before.cqes_reaped + 2, "{d:?}");
    }
}

/// A member in the middle fails: skipped before it, its CQE, cancelled after it.
#[monoio::test]
async fn skip_chain_member_fails() {
    let mut buf = vec![0u8; 8];
    let sqes = uring::link_skip([nop(), bad_read(&mut buf), nop()]);
    let op = unsafe { uring::submit_chain(sqes, buf) }.unwrap();
    assert_eq!(op.completed(), [None, None, None]);
    let (cqes, _) = op.await;
    assert_eq!(
        cqes.map(|c| c.res()),
        [0, -libc::EBADF, -libc::ECANCELED],
        "{cqes:?}"
    );
}

/// The head fails: its CQE, everything after it cancelled.
#[monoio::test]
async fn skip_chain_head_fails() {
    let mut buf = vec![0u8; 8];
    let sqes = uring::link_skip([bad_read(&mut buf), nop(), nop()]);
    let (cqes, _) = unsafe { uring::submit_chain(sqes, buf) }.unwrap().await;
    assert_eq!(
        cqes.map(|c| c.res()),
        [-libc::EBADF, -libc::ECANCELED, -libc::ECANCELED],
        "{cqes:?}"
    );
}

/// The last member fails: its own CQE, the members before it skipped.
#[monoio::test]
async fn skip_chain_last_fails() {
    let mut buf = vec![0u8; 8];
    let sqes = uring::link_skip([nop(), nop(), bad_read(&mut buf)]);
    let (cqes, _) = unsafe { uring::submit_chain(sqes, buf) }.unwrap().await;
    assert_eq!(cqes.map(|c| c.res()), [0, 0, -libc::EBADF], "{cqes:?}");
}

/// Dropped in flight: `data` lives in the one slot until the chain's CQE; with
/// `async-cancel` every member is asked to cancel (one finds the receive).
#[monoio::test]
async fn dropped_skip_chain_keeps_data_until_the_cqe() {
    let (a, mut b) = UnixStream::pair().unwrap();
    let dropped = Rc::new(Cell::new(false));
    let mut buf = vec![0u8; 8];
    let sqes = uring::link_skip([nop(), recv(&a, &mut buf)]);
    let data = (
        buf,
        Tracked {
            dropped: dropped.clone(),
            _ts: timespec(Duration::ZERO),
        },
    );
    #[cfg(feature = "metrics")]
    let before = monoio::driver_stats().cancels_pushed;
    let op = unsafe { uring::submit_chain(sqes, data) }.unwrap();
    raw_sleep(Duration::from_millis(1)).await;
    drop(op);
    assert!(!dropped.get());
    #[cfg(all(feature = "metrics", feature = "async-cancel"))]
    assert_eq!(monoio::driver_stats().cancels_pushed, before + 2);
    // Cancelled (`async-cancel`), or completed by the peer's data: either way the data goes.
    raw_sleep(Duration::from_millis(5)).await;
    if !dropped.get() {
        b.write_all(b"x").unwrap();
    }
    for _ in 0..50 {
        if dropped.get() {
            break;
        }
        raw_sleep(Duration::from_millis(5)).await;
    }
    assert!(dropped.get());
    // The slot is free and nothing else is in flight: a plain op works as usual.
    let (cqe, _) = unsafe { uring::submit(nop(), ()) }.unwrap().await;
    assert_eq!(cqe.res(), 0);
}

/// The whole SQ as one skip chain: one slot, one CQE.
#[monoio::test(entries = 256)]
async fn skip_chain_filling_the_sq() {
    let sqes: [squeue::Entry; 256] = uring::link_skip(std::array::from_fn(|_| nop()));
    let (cqes, ()) = unsafe { uring::submit_chain(sqes, ()) }.unwrap().await;
    assert!(cqes.iter().all(|c| c.res() == 0));
}
