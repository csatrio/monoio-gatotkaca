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
