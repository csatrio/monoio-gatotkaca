use std::{io::Write, os::fd::AsRawFd};

use io_uring::{opcode, types};
use monoio::{IoUringDriver, RuntimeBuilder};

#[monoio::test]
async fn nop_returns_data() {
    let entry = opcode::Nop::new().build();
    let (cqe, data) = unsafe { monoio::uring::submit(entry, 42u32) }
        .unwrap()
        .await;
    assert_eq!(cqe.result().unwrap(), 0);
    assert_eq!(data, 42);
}

#[monoio::test]
async fn read_with_owned_buffer() {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(b"hello raw ring").unwrap();

    let mut buf = vec![0u8; 5];
    let entry = opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), 5)
        .offset(6)
        .build();
    let (cqe, buf) = unsafe { monoio::uring::submit(entry, buf) }.unwrap().await;
    assert_eq!(cqe.res(), 5);
    assert_eq!(&buf, b"raw r");
}

#[monoio::test]
async fn error_is_negated_errno() {
    let mut buf = vec![0u8; 1];
    let entry = opcode::Read::new(types::Fd(-1), buf.as_mut_ptr(), 1).build();
    let (cqe, _buf) = unsafe { monoio::uring::submit(entry, buf) }.unwrap().await;
    assert_eq!(cqe.res(), -libc::EBADF);
    assert_eq!(cqe.result().unwrap_err().raw_os_error(), Some(libc::EBADF));
}

#[monoio::test]
async fn access_ring_in_runtime() {
    let entries = unsafe { monoio::uring::with_ring(|ring| ring.params().sq_entries()) };
    assert!(entries >= 256);

    let supported = unsafe {
        monoio::uring::with_ring(|ring| {
            let mut probe = io_uring::Probe::new();
            ring.submitter().register_probe(&mut probe).unwrap();
            probe.is_supported(opcode::Nop::CODE)
        })
    };
    assert!(supported);
}

#[test]
fn driver_handle() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();

    // Submitted through the driver handle before the runtime runs.
    let op = unsafe {
        rt.driver()
            .submit_raw(opcode::Nop::new().build(), "kept")
            .unwrap()
    };
    let entries = unsafe { rt.driver().with_ring(|ring| ring.params().sq_entries()) };
    assert!(entries >= 256);

    rt.block_on(async {
        let (cqe, data) = op.await;
        assert_eq!(cqe.res(), 0);
        assert_eq!(data, "kept");
    });
}

#[test]
fn io_uring_is_reexported() {
    // The same items, whether through the crate root or the `uring` module.
    let _: fn() -> monoio::io_uring::squeue::Entry =
        || monoio::io_uring::opcode::Nop::new().build();
    let _: fn() -> monoio::uring::squeue::Entry = || monoio::uring::opcode::Nop::new().build();
    let _ = monoio::uring::types::Fd(0);
    let _: Option<monoio::uring::Probe> = None;
    let _: Option<monoio::uring::IoUring> = None;
    let _: Option<monoio::uring::cqueue::Entry> = None;
    // Same type as the one `with_ring` hands out.
    let _: Option<monoio::IoUring> = None::<monoio::io_uring::IoUring>;
}
