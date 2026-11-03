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

mod hook {
    use std::{cell::RefCell, rc::Rc, time::Duration};

    use io_uring::{opcode, types};
    use monoio::{
        uring::{self, Cqe},
        IoUringDriver, RuntimeBuilder,
    };

    type Got = Rc<RefCell<Vec<(u64, Cqe)>>>;

    fn collect() -> Got {
        let got = Got::default();
        let sink = got.clone();
        uring::set_cqe_hook(move |id, cqe| sink.borrow_mut().push((id, cqe)));
        got
    }

    async fn wait_for(got: &Got, n: usize) {
        for _ in 0..2000 {
            if got.borrow().len() >= n {
                return;
            }
            monoio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("expected {n} completions, got {:?}", got.borrow());
    }

    fn push(entry: io_uring::squeue::Entry) {
        unsafe { uring::with_ring(|ring| ring.submission().push(&entry).unwrap()) };
    }

    fn nop(id: u64) -> io_uring::squeue::Entry {
        opcode::Nop::new().build().user_data(uring::user_data(id))
    }

    #[monoio::test(timer_enabled = true)]
    async fn multishot_poll_on_the_raw_ring() {
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        let (r, w) = (fds[0], fds[1]);
        let got = collect();

        let poll = opcode::PollAdd::new(types::Fd(r), libc::POLLIN as u32)
            .multi(true)
            .build()
            .user_data(uring::user_data(7));
        push(poll);

        for round in 1..=2 {
            assert_eq!(unsafe { libc::write(w, b"x".as_ptr().cast(), 1) }, 1);
            wait_for(&got, round).await;
            let (id, cqe) = got.borrow()[round - 1];
            assert_eq!(id, 7);
            assert!(cqe.more(), "multishot must stay armed");
            assert!(cqe.res() & libc::POLLIN as i32 != 0);
            let mut byte = 0u8;
            assert_eq!(
                unsafe { libc::read(r, (&mut byte as *mut u8).cast(), 1) },
                1
            );
        }

        // Cancel it: the poll ends with ECANCELED and no `more`, the cancel itself reports 0.
        push(
            opcode::AsyncCancel::new(uring::user_data(7))
                .build()
                .user_data(uring::user_data(8)),
        );
        wait_for(&got, 4).await;
        let tail: Vec<_> = got.borrow()[2..].to_vec();
        let poll_end = tail.iter().find(|(id, _)| *id == 7).unwrap().1;
        assert_eq!(poll_end.res(), -libc::ECANCELED);
        assert!(!poll_end.more());
        assert_eq!(tail.iter().find(|(id, _)| *id == 8).unwrap().1.res(), 0);

        unsafe {
            libc::close(r);
            libc::close(w);
        }
    }

    #[monoio::test(timer_enabled = true)]
    async fn hook_may_call_back_into_the_driver() {
        let got = Got::default();
        let sink = got.clone();
        uring::set_cqe_hook(move |id, cqe| {
            sink.borrow_mut().push((id, cqe));
            if id == 1 {
                push(nop(2)); // submit from inside the hook
            }
        });
        push(nop(1));
        wait_for(&got, 2).await;
        let ids: Vec<_> = got.borrow().iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [1, 2]);
    }

    #[monoio::test(timer_enabled = true)]
    async fn completions_without_a_hook_are_dropped() {
        push(nop(1));
        monoio::time::sleep(Duration::from_millis(20)).await;
        let got = collect();
        push(nop(2));
        wait_for(&got, 1).await;
        monoio::time::sleep(Duration::from_millis(20)).await;
        let ids: Vec<_> = got.borrow().iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [2]);

        uring::clear_cqe_hook();
        push(nop(3));
        monoio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(got.borrow().len(), 1);
    }

    #[monoio::test(timer_enabled = true)]
    async fn untagged_user_data_is_ignored() {
        let got = collect();
        // An unassigned prefix, and driver reserved values.
        for user_data in [1u64 << 62, u64::MAX - 5, u64::MAX] {
            push(opcode::Nop::new().build().user_data(user_data));
        }
        push(nop(3));
        wait_for(&got, 1).await;
        monoio::time::sleep(Duration::from_millis(20)).await;
        let ids: Vec<_> = got.borrow().iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [3]);
    }

    #[test]
    fn user_data_ids() {
        assert_eq!(uring::user_data(0) >> 62, 0b10);
        assert_eq!(uring::user_data(uring::MAX_ID) >> 62, 0b10);
        assert!(std::panic::catch_unwind(|| uring::user_data(uring::MAX_ID + 1)).is_err());
    }

    #[test]
    fn hook_on_the_driver_handle() {
        let mut rt = RuntimeBuilder::<IoUringDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        let got = Got::default();
        let sink = got.clone();
        rt.driver()
            .inner()
            .set_cqe_hook(move |id, cqe| sink.borrow_mut().push((id, cqe)));
        unsafe {
            rt.driver()
                .inner()
                .with_ring(|ring| ring.submission().push(&nop(9)).unwrap())
        };
        rt.block_on(async {
            for _ in 0..1000 {
                if !got.borrow().is_empty() {
                    break;
                }
                monoio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        assert_eq!(got.borrow().len(), 1);
        assert_eq!(got.borrow()[0].0, 9);
    }
}
