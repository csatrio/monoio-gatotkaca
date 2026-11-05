//! `uring::msg_ring_nowait` keeps its target handle alive only until the kernel consumed the SQE.
//! A test binary of its own: it checks that an fd number gets closed, which a test running in
//! parallel could reuse.

use std::os::fd::AsRawFd;

use monoio::uring::{self, libc, opcode};

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

fn is_open(fd: i32) -> bool {
    unsafe { libc::fcntl(fd, libc::F_GETFD) != -1 }
}

/// `msg_ring_nowait` keeps the handle until the kernel consumed the SQE, then lets it go.
#[monoio::test]
async fn nowait_releases_handle() {
    if !supported() {
        return;
    }
    let mailbox = uring::msg_receiver();
    let me = uring::ring_handle().unwrap();
    let other = uring::ring_handle().unwrap();
    let fd = other.as_raw_fd();
    uring::msg_ring_nowait(&other, 1, 1).unwrap();
    drop(other);
    // Not submitted yet: the queued SQE still holds the fd.
    assert!(is_open(fd));
    // Awaiting a message submits the queue.
    uring::msg_ring(&me, 2, 2).await.unwrap();
    assert_eq!(mailbox.recv().await, (1, 1));
    assert_eq!(mailbox.recv().await, (2, 2));
    // The next nowait send prunes the consumed one.
    uring::msg_ring_nowait(&me, 3, 3).unwrap();
    assert!(!is_open(fd));
    assert_eq!(mailbox.recv().await, (3, 3));
}
