//! Driver statistics (`IoUringDriver::stats`, `monoio::driver_stats`).
#![cfg(feature = "metrics")]

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRentExt},
    io_uring::opcode,
    net::{TcpListener, TcpStream},
    IoUringDriver, RuntimeBuilder,
};

async fn nop() {
    let (cqe, _) = unsafe { monoio::uring::submit(opcode::Nop::new().build(), ()) }
        .unwrap()
        .await;
    assert_eq!(cqe.res(), 0);
}

#[test]
fn nop_is_counted_and_reset_works() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        monoio::reset_driver_stats();
        nop().await;
        nop().await;
        let s = monoio::driver_stats();
        assert_eq!(s.ops_by_opcode[opcode::Nop::CODE as usize], 2);
        assert!(s.cqes_reaped >= 2, "{s:?}");
        assert!(s.sqes_submitted >= 2, "{s:?}");
        assert!(s.enter_submit + s.enter_wait > 0, "{s:?}");
        assert!(s.cqes_per_tick.iter().sum::<u64>() > 0, "{s:?}");
        assert_eq!(s.cancels_lost, 0);
    });

    let s = rt.driver().stats();
    assert_eq!(s.ops_by_opcode[opcode::Nop::CODE as usize], 2);
    rt.driver().reset_stats();
    let s = rt.driver().stats();
    assert_eq!(s.ops_by_opcode.iter().sum::<u64>(), 0);
    assert_eq!(s.cqes_reaped, 0);
    assert_eq!(s.enter_submit + s.enter_wait, 0);
}

#[test]
fn stats_through_time_driver() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    rt.block_on(async {
        nop().await;
        monoio::time::sleep(std::time::Duration::from_millis(1)).await;
    });
    let s = rt.driver().inner().stats();
    assert_eq!(s.ops_by_opcode[opcode::Nop::CODE as usize], 1);
    assert!(s.enter_wait > 0, "{s:?}");
}

#[monoio::test]
async fn tcp_echo_moves_counters() {
    monoio::reset_driver_stats();
    let srv = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = srv.local_addr().unwrap();
    let server = monoio::spawn(async move {
        let (mut conn, _) = srv.accept().await.unwrap();
        let (res, buf) = conn.read_exact(vec![0u8; 5]).await;
        res.unwrap();
        conn.write_all(buf).await.0.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(b"hello".to_vec()).await.0.unwrap();
    let (res, buf) = client.read_exact(vec![0u8; 5]).await;
    res.unwrap();
    assert_eq!(&buf, b"hello");
    server.await;

    let s = monoio::driver_stats();
    assert!(s.cqes_reaped >= 4, "{s:?}");
    assert!(s.ops_by_opcode.iter().sum::<u64>() >= 4, "{s:?}");
    assert_eq!(s.cq_overflow, 0);
    // A ring without the TASKRUN flag (the default) never counts this.
    assert_eq!(s.enter_left_taskrun, 0);
}

// More SQEs than the SQ holds: the driver flushes instead of failing (F2).
#[monoio::test(entries = 256)]
async fn full_sq_is_flushed() {
    monoio::reset_driver_stats();
    let ops: Vec<_> = (0..1000)
        .map(|_| unsafe { monoio::uring::submit(opcode::Nop::new().build(), ()) }.unwrap())
        .collect();
    for op in ops {
        assert_eq!(op.await.0.res(), 0);
    }
    let s = monoio::driver_stats();
    assert!(s.sq_full_flushes >= 3, "{s:?}");
    assert_eq!(s.ops_by_opcode[opcode::Nop::CODE as usize], 1000);
}

// Dropping an in-flight read pushes an AsyncCancel (F4).
#[cfg(feature = "async-cancel")]
#[monoio::test]
async fn dropped_read_counts_cancel() {
    use std::{future::Future, pin::pin, task::Poll};

    let srv = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = srv.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let (mut conn, _) = srv.accept().await.unwrap();

    monoio::reset_driver_stats();
    {
        let mut read = pin!(conn.read_exact(vec![0u8; 8]));
        std::future::poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    let s = monoio::driver_stats();
    assert_eq!(s.cancels_pushed, 1, "{s:?}");
    assert_eq!(s.cancels_lost, 0);
    drop(client);
}

/// `BURST` receives are armed in the kernel (no data yet), then all made ready by plain
/// `write(2)` calls while the loop is busy, and only then the runtime parks. Returns the
/// counters of the part after the receives were armed.
///
/// Each write makes one receive ready; the kernel completes it from task work.
fn ready_burst(rt: &mut monoio::Runtime<IoUringDriver>) -> monoio::DriverStats {
    use std::os::fd::AsRawFd;

    use monoio::{io::AsyncReadRent, net::UnixStream};

    const BURST: usize = 64;
    rt.block_on(async {
        let mut writers = Vec::new();
        let mut readers = Vec::new();
        for _ in 0..BURST {
            let (mut rd, wr) = UnixStream::pair().unwrap();
            writers.push(wr);
            readers.push(monoio::spawn(async move {
                let (res, buf) = rd.read(vec![0u8; 8]).await;
                assert_eq!(res.unwrap(), 1);
                assert_eq!(buf, [b'x']);
            }));
        }
        // Round trips through the ring until the kernel has the receives of all tasks. The tasks
        // push them when they first run, and an enter submits the whole SQ: once they are
        // pushed, one more round trip is enough. None has data, so the kernel arms a poll for
        // each.
        monoio::reset_driver_stats();
        let mut armed = false;
        for _ in 0..16 {
            nop().await;
            if monoio::driver_stats().ops_by_opcode.iter().sum::<u64>() > BURST as u64 {
                nop().await;
                let s = monoio::driver_stats();
                armed = s.sqes_submitted >= s.ops_by_opcode.iter().sum::<u64>();
                break;
            }
        }
        assert!(
            armed,
            "receives not submitted: {:?}",
            monoio::driver_stats()
        );

        monoio::reset_driver_stats();
        for wr in &writers {
            let n = unsafe { libc::write(wr.as_raw_fd(), b"x".as_ptr().cast(), 1) };
            assert_eq!(n, 1);
        }
        // The first await parks with nothing to submit and `BURST` completions pending.
        for reader in readers {
            reader.await;
        }
        monoio::driver_stats()
    })
}

/// First kernel whose DEFER_TASKRUN rings limit the task work run per enter.
const LOCAL_TW_LIMIT_KERNEL: (u32, u32) = (6, 13);

/// `(major, minor)` of the running kernel.
fn kernel_version() -> (u32, u32) {
    let mut uname: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut uname) } != 0 {
        return (0, 0);
    }
    let release = unsafe { std::ffi::CStr::from_ptr(uname.release.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    let mut it = release.split(|c: char| !c.is_ascii_digit());
    let major: u32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: u32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (major, minor)
}

// Without `IORING_SETUP_TASKRUN_FLAG` the kernel never raises `IORING_SQ_TASKRUN`.
#[test]
fn enter_left_taskrun_is_zero_without_taskrun_flag() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    let s = ready_burst(&mut rt);
    assert!(s.enter_wait > 0, "{s:?}");
    assert_eq!(s.enter_left_taskrun, 0, "plain ring: {s:?}");

    // COOP_TASKRUN alone (5.19+) has no flag either.
    match RuntimeBuilder::<IoUringDriver>::new()
        .coop_taskrun()
        .build()
    {
        Ok(mut rt) => {
            let s = ready_burst(&mut rt);
            assert!(s.enter_wait > 0, "{s:?}");
            assert_eq!(s.enter_left_taskrun, 0, "coop_taskrun ring: {s:?}");
        }
        Err(e) => eprintln!("skipped: coop_taskrun unsupported: {e}"),
    }
}

// A DEFER_TASKRUN ring runs its task work only inside `io_uring_enter`, and a wait runs at most
// 20 items of it (`IO_LOCAL_TW_DEFAULT_MAX`) once it has the CQEs it waits for (one, here): the
// rest stays queued and the kernel raises `IORING_SQ_TASKRUN` again. With 64 receives ready
// before the park, the first waiting enter must come back with the flag set.
#[test]
fn enter_left_taskrun_counts_leftover_task_work() {
    let mut rt = match RuntimeBuilder::<IoUringDriver>::new()
        .defer_taskrun()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            // DEFER_TASKRUN needs Linux 6.1.
            eprintln!("skipped: defer_taskrun unsupported: {e}");
            return;
        }
    };
    let s = ready_burst(&mut rt);
    eprintln!(
        "defer_taskrun: enter_wait {}, enter_left_taskrun {}, cqes_per_tick {:?}",
        s.enter_wait, s.enter_left_taskrun, s.cqes_per_tick
    );
    assert!(s.enter_wait > 0, "{s:?}");
    assert!(s.enter_left_taskrun <= s.enter_wait, "{s:?}");
    // Older kernels run the whole list in one enter and leave nothing behind.
    if kernel_version() >= LOCAL_TW_LIMIT_KERNEL {
        assert!(s.enter_left_taskrun >= 1, "{s:?}");
    } else {
        eprintln!("not asserted: this kernel has no limit on task work per enter");
    }
}

// The counters live in the driver state; `metrics_off.rs` checks the other build.
#[test]
fn driver_state_holds_the_counters() {
    use monoio::__private::{DRIVER_STATS_SIZE, URING_INNER_HAS_STATS, URING_INNER_SIZE};
    const { assert!(URING_INNER_HAS_STATS) };
    assert_eq!(
        DRIVER_STATS_SIZE,
        std::mem::size_of::<monoio::DriverStats>()
    );
    const { assert!(URING_INNER_SIZE > DRIVER_STATS_SIZE) };
    eprintln!("UringInner: {URING_INNER_SIZE} bytes (DriverStats: {DRIVER_STATS_SIZE} bytes)");
}
