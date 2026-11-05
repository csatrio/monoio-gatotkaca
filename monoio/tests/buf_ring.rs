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
    buf::IoBuf,
    io::{AsyncReadRentExt, AsyncWriteRentExt},
    uring::{self, BufRing, Cqe, SelectedBuf},
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

fn recv_entry(fd: &impl AsRawFd, ring: &BufRing) -> squeue::Entry {
    opcode::Recv::new(types::Fd(fd.as_raw_fd()), ptr::null_mut(), 0)
        .buf_group(ring.group())
        .build()
        .flags(squeue::Flags::BUFFER_SELECT)
}

async fn recv_one(fd: &impl AsRawFd, ring: &BufRing) -> Cqe {
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

/// Like [`ring`], with options (`BufRing::options()...`); skips when the kernel lacks one.
macro_rules! ring_with {
    ($bgid:expr, $entries:expr, $buf_size:expr, $options:expr) => {
        match BufRing::with_options($bgid, $entries, $buf_size, $options) {
            Ok(ring) => ring,
            Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
                eprintln!("skipped: unsupported by the kernel: {e}");
                return;
            }
            Err(e) => panic!("{e}"),
        }
    };
}

/// Wait (sleeping 1 ms at a time, up to 2 s) until `cond` holds.
async fn wait_for(mut cond: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if cond() {
            return;
        }
        monoio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("timed out");
}

#[monoio::test(timer_enabled = true)]
async fn batch_publish_needs_a_publish() {
    // Batch mode: drops only fill ring slots; the tail is published by `publish()` (or, once the
    // driver calls it, by the per-tick flush before entering the kernel).
    let ring = ring_with!(20, 2, 16, BufRing::options().batch_publish(true));
    let (a, mut b) = UnixStream::pair().unwrap();

    b.write_all(b"a").unwrap();
    let first = ring.take(recv_one(&a, &ring).await).unwrap();
    b.write_all(b"b").unwrap();
    let second = ring.take(recv_one(&a, &ring).await).unwrap();
    let freed = [first.bid(), second.bid()];
    drop((first, second));
    assert_eq!(ring.stats().recycled, 2);
    ring.publish();

    for msg in [&b"c"[..], b"d", b"e"] {
        b.write_all(msg).unwrap();
        let buf = ring.take(recv_one(&a, &ring).await).unwrap();
        assert_eq!(&*buf, msg);
        assert!(freed.contains(&buf.bid()));
        drop(buf);
        ring.publish();
    }
}

#[monoio::test(timer_enabled = true)]
async fn batch_publish_is_flushed_by_the_driver() {
    // No manual `publish()`: the driver publishes recycled buffers before entering the kernel.
    // With 2 buffers and 6 receives, the pool only refills through that flush.
    let ring = ring_with!(21, 2, 16, BufRing::options().batch_publish(true));
    let (a, mut b) = UnixStream::pair().unwrap();

    for msg in [&b"a"[..], b"b", b"c", b"d", b"e", b"f"] {
        b.write_all(msg).unwrap();
        let buf = ring.take(recv_one(&a, &ring).await).unwrap();
        assert_eq!(&*buf, msg);
    }
    assert_eq!(ring.stats().enobufs, 0);
}

#[monoio::test(timer_enabled = true)]
async fn pool_is_page_aligned() {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    let Some(ring) = ring(21, 4, 100) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"x").unwrap();
    let buf = ring.take(recv_one(&a, &ring).await).unwrap();
    let base = buf.as_ptr() as usize - buf.bid() as usize * 100;
    assert_eq!(base % page, 0);
}

#[monoio::test(timer_enabled = true)]
async fn kernel_allocated_ring() {
    let ring = ring_with!(22, 2, 32, BufRing::options().kernel_ring(true));
    let (a, mut b) = UnixStream::pair().unwrap();
    // More messages than buffers: recycling writes into the kernel's ring.
    for i in 0..6 {
        let msg = format!("kmsg{i}");
        b.write_all(msg.as_bytes()).unwrap();
        let buf = ring.take(recv_one(&a, &ring).await).unwrap();
        assert_eq!(&*buf, msg.as_bytes());
    }
    assert_eq!(ring.stats().recycled, 6);
}

#[monoio::test(timer_enabled = true)]
async fn kernel_ring_with_batch_and_populate() {
    let options = BufRing::options()
        .kernel_ring(true)
        .batch_publish(true)
        .populate(true);
    let ring = ring_with!(23, 2, 32, options);
    let (a, mut b) = UnixStream::pair().unwrap();
    for i in 0..4 {
        let msg = format!("m{i}");
        b.write_all(msg.as_bytes()).unwrap();
        let buf = ring.take(recv_one(&a, &ring).await).unwrap();
        assert_eq!(&*buf, msg.as_bytes());
        drop(buf);
        ring.publish();
    }
}

#[monoio::test(timer_enabled = true)]
async fn hugetlb_pool_if_available() {
    match BufRing::with_options(24, 4, 4096, BufRing::options().hugetlb(true)) {
        Ok(ring) => {
            let (a, mut b) = UnixStream::pair().unwrap();
            b.write_all(b"huge").unwrap();
            let buf = ring.take(recv_one(&a, &ring).await).unwrap();
            assert_eq!(&*buf, b"huge");
        }
        // No huge pages reserved: a clear error, no fallback.
        Err(e) => assert!(e.to_string().contains("MAP_HUGETLB"), "{e}"),
    }
}

#[monoio::test(timer_enabled = true)]
async fn incremental_buffer_holds_several_messages() {
    let ring = ring_with!(25, 2, 64, BufRing::options().incremental(true));
    let (a, mut b) = UnixStream::pair().unwrap();

    type Got = Rc<RefCell<Vec<(Cqe, Option<SelectedBuf>)>>>;
    let got = Got::default();
    let (sink, pool) = (got.clone(), ring.clone());
    uring::set_cqe_hook(move |_, cqe| {
        // Keep every view alive.
        let buf = pool.take(cqe);
        sink.borrow_mut().push((cqe, buf));
    });
    let recv = opcode::RecvMulti::new(types::Fd(a.as_raw_fd()), ring.group())
        .build()
        .flags(squeue::Flags::BUFFER_SELECT)
        .user_data(uring::user_data(1));
    unsafe { uring::with_ring(|r| r.submission().push(&recv).unwrap()) };

    // 10 + 20 bytes fit in the first buffer, which stays with the kernel (BUF_MORE).
    b.write_all(&[b'a'; 10]).unwrap();
    wait_for(|| !got.borrow().is_empty()).await;
    b.write_all(&[b'b'; 20]).unwrap();
    wait_for(|| got.borrow().len() >= 2).await;
    {
        let got = got.borrow();
        let (c0, b0) = (&got[0].0, got[0].1.as_ref().unwrap());
        let (c1, b1) = (&got[1].0, got[1].1.as_ref().unwrap());
        assert!(uring::cqueue::buffer_more(c0.flags()) && uring::cqueue::buffer_more(c1.flags()));
        assert_eq!(b0.bid(), b1.bid());
        assert_eq!((b0.offset(), &**b0), (0, &[b'a'; 10][..]));
        assert_eq!((b1.offset(), &**b1), (10, &[b'b'; 20][..]));
    }
    assert_eq!(ring.stats().in_use, 1);
    assert_eq!(ring.stats().taken, 2);

    // 40 more bytes: 34 finish the first buffer, the rest goes to the second one.
    b.write_all(&[b'c'; 40]).unwrap();
    wait_for(|| {
        let n: usize = got
            .borrow()
            .iter()
            .filter_map(|(_, b)| b.as_ref())
            .map(|b| b.len())
            .sum();
        n >= 70
    })
    .await;

    let mut views: Vec<_> = got
        .borrow_mut()
        .drain(..)
        .filter_map(|(c, b)| Some((c, b?)))
        .collect();
    let first = views[0].1.bid();
    let data: Vec<u8> = views.iter().flat_map(|(_, b)| b.iter().copied()).collect();
    let mut expected = vec![b'a'; 10];
    expected.extend([b'b'; 20]);
    expected.extend([b'c'; 40]);
    assert_eq!(data, expected);
    // The first buffer is complete: its last completion clears BUF_MORE.
    let last_of_first = views.iter().rfind(|(_, b)| b.bid() == first).unwrap();
    assert!(!uring::cqueue::buffer_more(last_of_first.0.flags()));
    assert_eq!(last_of_first.1.offset() + last_of_first.1.len(), 64);
    // The second buffer started at offset 0 and is still with the kernel.
    let second = views.iter().find(|(_, b)| b.bid() != first).unwrap();
    assert_eq!(second.1.offset(), 0);
    assert!(uring::cqueue::buffer_more(second.0.flags()));
    assert_eq!(ring.stats().in_use, 2);
    assert_eq!(ring.stats().recycled, 0);

    // Dropping all views of the first buffer recycles it; the second stays with the kernel.
    views.retain(|(_, b)| b.bid() != first);
    assert_eq!(ring.stats().recycled, 1);
    drop(views);
    assert_eq!(ring.stats().recycled, 1);
    assert_eq!(ring.stats().in_use, 0);

    let cancel = opcode::AsyncCancel::new(uring::user_data(1))
        .build()
        .user_data(uring::user_data(2));
    unsafe { uring::with_ring(|r| r.submission().push(&cancel).unwrap()) };
    wait_for(|| got.borrow().len() >= 2).await;
    uring::clear_cqe_hook();
}

#[monoio::test(timer_enabled = true)]
async fn selected_buf_is_written_without_a_copy() {
    let Some(ring) = ring(26, 4, 64) else { return };
    let (a, mut b) = monoio::net::UnixStream::pair().unwrap();

    let (res, _) = b.write_all(b"ping").await;
    res.unwrap();
    let buf = ring.take(recv_one(&a, &ring).await).unwrap();
    let ptr = buf.as_ptr();
    let mut a = a;

    // Echo it back as is.
    let (res, buf) = a.write_all(buf).await;
    res.unwrap();
    assert_eq!(buf.read_ptr(), ptr);
    let (res, echo) = b.read_exact(vec![0; 4]).await;
    res.unwrap();
    assert_eq!(echo, b"ping");

    // And a slice of it.
    let (res, slice) = a.write_all(buf.slice(1..3)).await;
    res.unwrap();
    assert_eq!(slice.read_ptr(), ptr.wrapping_add(1));
    let (res, echo) = b.read_exact(vec![0; 2]).await;
    res.unwrap();
    assert_eq!(echo, b"in");

    assert_eq!(ring.stats().in_use, 1);
    drop(slice);
    assert_eq!(ring.stats().in_use, 0);
}

#[monoio::test(timer_enabled = true)]
async fn detach_copies_and_recycles_at_once() {
    let Some(ring) = ring(27, 2, 16) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"hello").unwrap();
    let buf = ring.take(recv_one(&a, &ring).await).unwrap();
    assert_eq!(ring.stats().in_use, 1);
    assert_eq!(buf.detach(), b"hello");
    assert_eq!(ring.stats().in_use, 0);
    assert_eq!(ring.stats().recycled, 1);

    b.write_all(b"bytes").unwrap();
    let buf = ring.take(recv_one(&a, &ring).await).unwrap();
    assert_eq!(buf.into_bytes(), &b"bytes"[..]);
    assert_eq!(ring.stats().recycled, 2);
}

#[monoio::test(timer_enabled = true)]
async fn stats_count_use_and_enobufs() {
    let Some(ring) = ring(28, 2, 16) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    assert_eq!(ring.stats(), Default::default());

    b.write_all(b"1").unwrap();
    let first = ring.take(recv_one(&a, &ring).await).unwrap();
    b.write_all(b"2").unwrap();
    let second = ring.take(recv_one(&a, &ring).await).unwrap();
    b.write_all(b"3").unwrap();
    assert!(ring.take(recv_one(&a, &ring).await).is_none());

    let stats = ring.stats();
    assert_eq!((stats.in_use, stats.high_water), (2, 2));
    assert_eq!((stats.taken, stats.enobufs, stats.recycled), (2, 1, 0));

    drop(first);
    let stats = ring.stats();
    assert_eq!((stats.in_use, stats.high_water, stats.recycled), (1, 2, 1));
    drop(second);
}

#[monoio::test(timer_enabled = true)]
async fn selected_buf_keeps_the_pool_alive() {
    let Some(ring) = ring(29, 2, 16) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    b.write_all(b"late").unwrap();
    let buf = ring.take(recv_one(&a, &ring).await).unwrap();
    drop(ring);
    assert_eq!(&*buf, b"late");
    drop(buf);
    // The group was unregistered with the last view.
    assert!(BufRing::new(29, 2, 16).is_ok());
}

/// Single receives (`take`) and single-shot bundle receives (`take_bundle`) on one ring, with
/// the buffers given back in a scrambled order: the bundle walk must follow the slots the kernel
/// consumed, which are no longer consecutive bids.
#[monoio::test(timer_enabled = true)]
async fn mixed_single_and_bundle_receives_with_scrambled_recycling() {
    let Some(ring) = ring(60, 8, 32) else { return };
    if !ring.supports_bundles() {
        eprintln!("skipped: recv bundles unsupported");
        return;
    }
    let (a, mut b) = UnixStream::pair().unwrap();
    let a_rt = monoio::net::UnixStream::from_std(a.try_clone().unwrap()).unwrap();

    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut rand = move |n: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % n as u64) as usize
    };
    let mut sent = Vec::new();
    let mut got = Vec::new();
    let mut held: Vec<SelectedBuf> = Vec::new();
    let mut bundles_seen = 0;
    for round in 0..400usize {
        let len = 1 + rand(5 * 32);
        let msg: Vec<u8> = (0..len).map(|i| (i * 7 + round) as u8).collect();
        b.write_all(&msg).unwrap();
        sent.extend_from_slice(&msg);
        while got.len() < sent.len() {
            if rand(3) == 0 {
                let buf = ring.take(recv_one(&a, &ring).await).unwrap();
                got.extend_from_slice(&buf);
                held.push(buf);
            } else {
                let bundle = a_rt.recv_bundle(&ring).await.unwrap();
                if bundle.len() > 1 {
                    bundles_seen += 1;
                }
                for buf in bundle {
                    got.extend_from_slice(&buf);
                    held.push(buf);
                }
            }
            // Keep at most 3 buffers, dropping random ones.
            while held.len() > 3 {
                let i = rand(held.len());
                held.swap_remove(i);
            }
        }
    }
    drop(held);
    assert!(got == sent, "data corrupted");
    assert!(bundles_seen > 0);
    assert_eq!(ring.stats().in_use, 0);
    assert_eq!(ring.stats().taken, ring.stats().recycled);
}
