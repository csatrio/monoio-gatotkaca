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
    let before = monoio::driver_stats().chains_submitted;
    let op = unsafe { uring::submit_chain(uring::link([nop(), nop(), nop()]), "data") }.unwrap();
    let (cqes, data) = op.await;
    assert_eq!(data, "data");
    assert!(cqes.iter().all(|c| c.res() == 0), "{cqes:?}");
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
    let flushes = monoio::driver_stats().sq_full_flushes;

    let mut buf = vec![0u8; 8];
    let sqes = uring::link([bad_read(&mut buf), nop()]);
    let op = unsafe { uring::submit_chain(sqes, buf) }.unwrap();
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
    let before = monoio::driver_stats().link_timeouts;
    let mut buf = vec![0u8; 8];
    let entry = opcode::Recv::new(types::Fd(a.as_raw_fd()), buf.as_mut_ptr(), 8).build();
    let (cqe, _) = unsafe { uring::submit_with_timeout(entry, buf, Duration::from_millis(20)) }
        .unwrap()
        .await;
    assert_eq!(cqe.res(), -libc::ECANCELED);
    // The timeout's CQE may come in a later tick.
    for _ in 0..20 {
        if monoio::driver_stats().link_timeouts > before {
            break;
        }
        raw_sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(monoio::driver_stats().link_timeouts, before + 1);
}

#[monoio::test]
async fn submit_with_timeout_completes_in_time() {
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"hello").unwrap();
    let before = monoio::driver_stats().link_timeouts;
    let mut buf = vec![0u8; 8];
    let entry = opcode::Recv::new(types::Fd(a.as_raw_fd()), buf.as_mut_ptr(), 8).build();
    let (cqe, buf) = unsafe { uring::submit_with_timeout(entry, buf, Duration::from_secs(5)) }
        .unwrap()
        .await;
    assert_eq!(cqe.res(), 5);
    assert_eq!(&buf[..5], b"hello");
    raw_sleep(Duration::from_millis(1)).await;
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
