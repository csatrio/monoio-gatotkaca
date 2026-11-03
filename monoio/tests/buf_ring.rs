use std::{
    cell::RefCell,
    io::Write,
    os::{fd::AsRawFd, unix::net::UnixStream},
    ptr,
    rc::Rc,
    time::Duration,
};

use io_uring::{opcode, squeue, types};
use monoio::{
    uring::{self, BufRing, Cqe},
    IoUringDriver, RuntimeBuilder,
};

/// Provided buffer rings need Linux 5.19; skip on older kernels.
fn ring(bgid: u16, entries: u16, buf_size: usize) -> Option<BufRing> {
    match BufRing::new(bgid, entries, buf_size) {
        Ok(ring) => Some(ring),
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
            eprintln!("skipped: provided buffer rings unsupported: {e}");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

fn recv_entry(fd: &UnixStream, ring: &BufRing) -> squeue::Entry {
    opcode::Recv::new(types::Fd(fd.as_raw_fd()), ptr::null_mut(), 0)
        .buf_group(ring.group())
        .build()
        .flags(squeue::Flags::BUFFER_SELECT)
}

async fn recv_one(fd: &UnixStream, ring: &BufRing) -> Cqe {
    unsafe { uring::submit(recv_entry(fd, ring), ()) }
        .unwrap()
        .await
        .0
}

#[monoio::test(timer_enabled = true)]
async fn recv_selects_a_buffer() {
    let Some(ring) = ring(1, 4, 64) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"hello").unwrap();

    let buf = ring.take(recv_one(&a, &ring).await).unwrap();
    assert_eq!(&*buf, b"hello");
    assert!(buf.bid() < 4);
}

#[monoio::test(timer_enabled = true)]
async fn pool_runs_dry_and_refills_on_drop() {
    let Some(ring) = ring(2, 2, 16) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();

    b.write_all(b"a").unwrap();
    let first = ring.take(recv_one(&a, &ring).await).unwrap();
    b.write_all(b"b").unwrap();
    let second = ring.take(recv_one(&a, &ring).await).unwrap();
    assert_eq!((&*first, &*second), (&b"a"[..], &b"b"[..]));

    // Both buffers are held: the kernel has none to select.
    b.write_all(b"c").unwrap();
    let dry = recv_one(&a, &ring).await;
    assert_eq!(dry.res(), -libc::ENOBUFS);
    assert!(ring.take(dry).is_none());

    // Dropping a buffer hands it back.
    let freed = first.bid();
    drop(first);
    let third = ring.take(recv_one(&a, &ring).await).unwrap();
    assert_eq!(&*third, b"c");
    assert_eq!(third.bid(), freed);
}

#[monoio::test(timer_enabled = true)]
async fn a_buffer_can_only_be_taken_once() {
    let Some(ring) = ring(3, 4, 16) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"x").unwrap();
    let cqe = recv_one(&a, &ring).await;

    let held = ring.take(cqe).unwrap();
    assert!(ring.take(cqe).is_none());
    drop(held);
}

#[monoio::test(timer_enabled = true)]
async fn multishot_recv_reuses_a_small_pool() {
    let Some(ring) = ring(4, 4, 32) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();

    // (id, completion, bytes of the selected buffer)
    type Got = Rc<RefCell<Vec<(u64, Cqe, Option<Vec<u8>>)>>>;
    let got = Got::default();
    let (sink, pool) = (got.clone(), ring.clone());
    uring::set_cqe_hook(move |id, cqe| {
        // Dropping the buffer right away gives it back to the ring.
        let data = pool.take(cqe).map(|buf| buf.to_vec());
        sink.borrow_mut().push((id, cqe, data));
    });

    let recv = opcode::RecvMulti::new(types::Fd(a.as_raw_fd()), ring.group())
        .build()
        .flags(squeue::Flags::BUFFER_SELECT)
        .user_data(uring::user_data(5));
    unsafe { uring::with_ring(|r| r.submission().push(&recv).unwrap()) };

    // Twice as many messages as buffers: only works if buffers are recycled.
    for i in 0..8 {
        b.write_all(format!("msg{i}").as_bytes()).unwrap();
        for _ in 0..2000 {
            if got.borrow().len() > i {
                break;
            }
            monoio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(got.borrow().len() > i, "no completion for message {i}");
    }

    let cancel = opcode::AsyncCancel::new(uring::user_data(5))
        .build()
        .user_data(uring::user_data(6));
    unsafe { uring::with_ring(|r| r.submission().push(&cancel).unwrap()) };
    for _ in 0..2000 {
        if got.borrow().len() >= 10 {
            break;
        }
        monoio::time::sleep(Duration::from_millis(1)).await;
    }

    let got = got.borrow();
    let data: Vec<_> = got.iter().filter_map(|(_, _, d)| d.clone()).collect();
    let expected: Vec<_> = (0..8).map(|i| format!("msg{i}").into_bytes()).collect();
    assert_eq!(data, expected);
    assert!(got[..8].iter().all(|(id, cqe, _)| *id == 5 && cqe.more()));
    let end = got
        .iter()
        .find(|(id, cqe, _)| *id == 5 && !cqe.more())
        .unwrap();
    assert_eq!(end.1.res(), -libc::ECANCELED);
}

#[monoio::test(timer_enabled = true)]
async fn rejects_bad_parameters_and_duplicate_groups() {
    for (entries, buf_size) in [(0, 8), (3, 8), (100, 8), (4, 0)] {
        let e = BufRing::new(9, entries, buf_size).err().unwrap();
        assert_eq!(
            e.kind(),
            std::io::ErrorKind::InvalidInput,
            "{entries}/{buf_size}"
        );
    }

    let Some(first) = ring(10, 2, 8) else { return };
    assert!(
        BufRing::new(10, 2, 8).is_err(),
        "group id is already registered"
    );
    drop(first);
    // Dropping unregistered the group, so it can be used again.
    assert!(BufRing::new(10, 2, 8).is_ok());
}

#[test]
fn works_on_a_timer_runtime_through_the_driver_handle() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    let ring = match BufRing::new_in(rt.driver().inner(), 11, 4, 64) {
        Ok(ring) => ring,
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => return,
        Err(e) => panic!("{e}"),
    };
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"timer").unwrap();

    rt.block_on(async {
        monoio::time::sleep(Duration::from_millis(5)).await;
        let buf = ring.take(recv_one(&a, &ring).await).unwrap();
        assert_eq!(&*buf, b"timer");
    });
}

#[test]
fn attach_wq_shares_the_worker_pool_with_timers() {
    let primary = RuntimeBuilder::<IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    // The timer driver exposes the ring fd of the driver it wraps.
    let fd = primary.driver().as_raw_fd();

    let worker = std::thread::spawn(move || {
        let mut rt = RuntimeBuilder::<IoUringDriver>::new()
            .attach_wq(fd)
            .enable_timer()
            .build()
            .expect("attach_wq rejected");
        rt.block_on(async {
            monoio::time::sleep(Duration::from_millis(5)).await;
            let listener = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let connect = monoio::spawn(monoio::net::TcpStream::connect(
                listener.local_addr().unwrap(),
            ));
            listener.accept().await.unwrap();
            connect.await.unwrap();
        });
    });
    worker.join().unwrap();
    drop(primary);

    // The flag is really passed on: a bogus fd makes the kernel refuse the ring.
    let bogus = RuntimeBuilder::<IoUringDriver>::new()
        .attach_wq(-1)
        .enable_timer()
        .build();
    assert!(bogus.is_err());
}
