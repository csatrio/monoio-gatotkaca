//! Raw multishot operations: `uring::submit_multishot` and `MultiOp`.

use std::{io::Write, os::fd::AsRawFd, os::unix::net::UnixStream, rc::Rc, time::Duration};

use monoio::uring::{self, opcode, types, BufRing, MultiOp};

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

fn recv_multi<T: Unpin + 'static>(fd: &impl AsRawFd, ring: &BufRing, data: T) -> MultiOp<T> {
    let sqe = opcode::RecvMulti::new(types::Fd(fd.as_raw_fd()), ring.group()).build();
    // SAFETY: the SQE points to no memory; the fd outlives the op in every test.
    unsafe { uring::submit_multishot(sqe, data) }.unwrap()
}

async fn sleep() {
    monoio::time::sleep(Duration::from_millis(20)).await;
}

#[monoio::test(timer_enabled = true)]
async fn recv_several_messages_then_cancel() {
    let Some(ring) = ring(11, 8, 64) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    let mut op = recv_multi(&a, &ring, ());

    let mut got = Vec::new();
    for msg in [&b"one"[..], b"two", b"three"] {
        b.write_all(msg).unwrap();
        let cqe = op.next().await.unwrap();
        if cqe.res() == -libc::EINVAL {
            eprintln!("skipped: multishot recv unsupported");
            return;
        }
        assert!(cqe.more(), "{cqe:?}");
        let buf = ring.take(cqe).unwrap();
        got.extend_from_slice(&buf);
    }
    assert_eq!(got, b"onetwothree");
    assert!(!op.is_finished());
    assert!(monoio::driver_stats().multishot_cqes >= 3);

    // Not finished: the data stays with the op.
    let mut op = op.into_data().unwrap_err();

    op.cancel();
    let last = op.next().await.unwrap();
    assert!(!last.more());
    assert_eq!(last.res(), -libc::ECANCELED);
    assert!(op.is_finished());
    assert!(op.next().await.is_none());
    op.into_data().unwrap();
    // The op is gone; the ring got every buffer back.
    assert_eq!(ring.stats().in_use, 0);
}

#[monoio::test(timer_enabled = true)]
async fn cqes_queue_while_not_polled() {
    let Some(ring) = ring(12, 8, 64) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    let mut op = recv_multi(&a, &ring, ());
    for msg in [&b"a"[..], b"b", b"c"] {
        b.write_all(msg).unwrap();
        // Let each message complete on its own.
        sleep().await;
    }
    let mut got = Vec::new();
    while got.len() < 3 {
        let cqe = op.next().await.unwrap();
        if cqe.res() == -libc::EINVAL {
            eprintln!("skipped: multishot recv unsupported");
            return;
        }
        got.extend_from_slice(&ring.take(cqe).unwrap());
    }
    assert_eq!(got, b"abc");
    assert!(monoio::driver_stats().multishot_queue_high_water >= 2);
}

#[monoio::test(timer_enabled = true)]
async fn drop_with_pending_cqes_keeps_data_until_the_end() {
    let Some(ring) = ring(13, 8, 64) else { return };
    let (a, mut b) = UnixStream::pair().unwrap();
    let alive = Rc::new(());
    let op = recv_multi(&a, &ring, alive.clone());
    b.write_all(b"queued").unwrap();
    sleep().await;
    // A CQE is queued; dropping cancels and keeps `data` until the final CQE.
    drop(op);
    sleep().await;
    assert_eq!(
        Rc::strong_count(&alive),
        1,
        "data dropped after the final CQE"
    );

    // The slot is free and the ring still works for a new op (the dropped CQE's buffer is not
    // returned: one buffer fewer).
    let mut op = recv_multi(&a, &ring, ());
    b.write_all(b"again").unwrap();
    let cqe = op.next().await.unwrap();
    if cqe.res() == -libc::EINVAL {
        return;
    }
    assert_eq!(&*ring.take(cqe).unwrap(), b"again");
}

#[monoio::test(timer_enabled = true)]
async fn drop_before_any_cqe() {
    let Some(ring) = ring(14, 4, 64) else { return };
    let (a, _b) = UnixStream::pair().unwrap();
    let alive = Rc::new(());
    let op = recv_multi(&a, &ring, alive.clone());
    // Let the op reach the kernel, then drop it.
    sleep().await;
    drop(op);
    sleep().await;
    assert_eq!(Rc::strong_count(&alive), 1);
}

#[monoio::test(timer_enabled = true)]
async fn single_cqe_ends_at_once() {
    let alive = Rc::new(());
    // SAFETY: a nop points to nothing.
    let mut op =
        unsafe { uring::submit_multishot(opcode::Nop::new().build(), alive.clone()) }.unwrap();
    assert!(op.data().as_ref() == alive.as_ref());
    let cqe = op.next().await.unwrap();
    assert_eq!(cqe.res(), 0);
    assert!(!cqe.more());
    assert!(op.is_finished());
    assert!(op.next().await.is_none());
    // Cancel after the end is a no-op.
    op.cancel();
    drop(op.into_data().unwrap());
    assert_eq!(Rc::strong_count(&alive), 1);
}

#[monoio::test(timer_enabled = true)]
async fn into_data_after_kernel_end_without_polling() {
    // SAFETY: a nop points to nothing.
    let op = unsafe { uring::submit_multishot(opcode::Nop::new().build(), 7u32) }.unwrap();
    // Not completed yet: the SQE was not even submitted.
    let op = op.into_data().unwrap_err();
    sleep().await;
    // The final CQE arrived but was not taken: `into_data` drops it.
    assert_eq!(op.into_data().unwrap(), 7);
}

#[test]
fn submit_multishot_raw_outside_the_runtime() {
    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    // SAFETY: a nop points to nothing.
    let mut op = unsafe {
        rt.driver()
            .submit_multishot_raw(opcode::Nop::new().build(), ())
    }
    .unwrap();
    let cqe = rt.block_on(async move {
        let cqe = op.next().await.unwrap();
        assert!(op.next().await.is_none());
        cqe
    });
    assert_eq!(cqe.res(), 0);
}
