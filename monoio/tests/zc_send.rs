//! Zero-copy send refinements: `send_zc_early` (result first, buffer released later),
//! `send_zc_fixed` (`SEND_ZC` on a registered buffer), `send_vectored_zc` (`SENDMSG_ZC`) and
//! `send_auto`. Over loopback the kernel copies anyway, so these check correctness and the
//! buffer lifecycle, not speed.

use std::{cell::Cell, future::Future, pin::Pin, rc::Rc, task::Context};

#[cfg(feature = "metrics")]
use monoio::uring::opcode;
use monoio::{
    buf::{IoBuf, IoBufMut, VecBuf},
    io::AsyncReadRentExt,
    net::{zc_worthwhile, TcpListener, TcpStream, ZC_MIN_LEN},
    uring::FixedBufPool,
    IoUringDriver, RuntimeBuilder,
};

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}

async fn read_n(conn: &mut TcpStream, n: usize) -> Vec<u8> {
    let (res, buf) = conn.read_exact(Vec::with_capacity(n).slice_mut(0..n)).await;
    res.unwrap();
    buf.into_inner()
}

fn payload(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

/// A buffer that counts its drops.
struct Tracked {
    data: Vec<u8>,
    drops: Rc<Cell<usize>>,
}

unsafe impl IoBuf for Tracked {
    fn read_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.data.len()
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

/// Run the driver a few times so that pending CQEs are processed.
async fn settle() {
    for _ in 0..8 {
        monoio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

#[monoio::test(timer_enabled = true)]
async fn early_result_then_release() {
    let (client, mut server) = tcp_pair().await;
    let data = payload(64 * 1024);
    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();

    let fut = client.send_zc_early(data.clone());
    assert!(!fut.is_terminated());
    let (res, pending) = fut.await;
    let n = res.unwrap();
    assert!(n > 0 && n <= data.len());
    // The buffer comes back once the kernel released it.
    let buf = pending.await;
    assert_eq!(buf, data);
    assert_eq!(read_n(&mut server, n).await, data[..n]);

    #[cfg(feature = "metrics")]
    let s = monoio::driver_stats();
    #[cfg(feature = "metrics")]
    assert_eq!(s.zc_sends, 1, "{s:?}");
    #[cfg(feature = "metrics")]
    assert_eq!(s.zc_notifs, 1, "{s:?}");
    #[cfg(feature = "metrics")]
    assert_eq!(s.ops_by_opcode[opcode::SendZc::CODE as usize], 1);
}

/// Several sends started back to back, each resolving with its result before its buffer is
/// awaited; the buffers come back afterwards, in any order.
#[monoio::test(timer_enabled = true)]
async fn early_results_pipeline() {
    let (client, mut server) = tcp_pair().await;
    let chunks: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i; 20 * 1024]).collect();

    let mut pendings = Vec::new();
    let mut sent = 0;
    for chunk in &chunks {
        let (res, pending) = client.send_zc_early(chunk.clone()).await;
        let n = res.unwrap();
        assert_eq!(n, chunk.len(), "loopback takes 20 KiB at once");
        sent += n;
        pendings.push(pending);
    }
    let received = read_n(&mut server, sent).await;
    assert_eq!(received, chunks.concat());
    for (pending, chunk) in pendings.into_iter().zip(&chunks) {
        assert_eq!(&pending.await, chunk);
    }
}

/// Dropping the `ZcPending` does not drop the buffer before the kernel released it, and drops
/// it exactly once afterwards.
#[monoio::test(timer_enabled = true)]
async fn drop_pending_keeps_buffer_alive() {
    let (client, mut server) = tcp_pair().await;
    let drops = Rc::new(Cell::new(0));
    let buf = Tracked {
        data: payload(32 * 1024),
        drops: drops.clone(),
    };
    let (res, pending) = client.send_zc_early(buf).await;
    let n = res.unwrap();
    let released = pending.is_released();
    drop(pending);
    if released {
        // The notification came with the result: the buffer was handed straight back.
        assert_eq!(drops.get(), 1);
    } else {
        // Still held by the kernel: the runtime keeps it.
        assert_eq!(drops.get(), 0);
    }
    assert_eq!(read_n(&mut server, n).await.len(), n);
    settle().await;
    assert_eq!(drops.get(), 1);
}

/// Dropping a `SendZcFuture` before it resolved keeps the buffer until the kernel finished
/// (cancelled or sent), then drops it once.
#[monoio::test(timer_enabled = true)]
async fn drop_send_future_keeps_buffer_alive() {
    let (client, _server) = tcp_pair().await;
    let drops = Rc::new(Cell::new(0));
    let fut = client.send_zc_early(Tracked {
        data: payload(1024),
        drops: drops.clone(),
    });
    // Not submitted yet (that happens when the runtime parks), so it is surely in flight.
    drop(fut);
    assert_eq!(drops.get(), 0);
    settle().await;
    assert_eq!(drops.get(), 1);
}

/// A failed send: the error comes with the first CQE, the buffer still comes back (with or
/// without a notification, depending on the kernel).
#[monoio::test]
async fn early_error_has_buffer_ready() {
    let (client, server) = tcp_pair().await;
    drop(server);
    // Send until the reset peer makes it fail.
    let mut buf = payload(4096);
    for _ in 0..64 {
        let (res, pending) = client.send_zc_early(buf).await;
        if res.is_err() {
            let back = pending.await;
            assert_eq!(back.len(), 4096);
            return;
        }
        buf = pending.await;
    }
    panic!("sends to a closed peer kept succeeding");
}

#[monoio::test]
async fn send_zc_fixed_loopback() {
    let pool = match FixedBufPool::new(2, 64 * 1024) {
        Ok(pool) => pool,
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => return,
        Err(e) => panic!("{e}"),
    };
    let (client, mut server) = tcp_pair().await;
    let data = payload(48 * 1024);
    let mut buf = pool.get().unwrap();
    buf.extend_from_slice(&data);

    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();
    let (res, buf) = client.send_zc_fixed(buf).await;
    let n = res.unwrap();
    assert_eq!(read_n(&mut server, n).await, data[..n]);
    assert_eq!(&buf[..], &data[..]);
    #[cfg(feature = "metrics")]
    let s = monoio::driver_stats();
    #[cfg(feature = "metrics")]
    assert_eq!(s.ops_by_opcode[opcode::SendZc::CODE as usize], 1);
    #[cfg(feature = "metrics")]
    assert_eq!(s.zc_notifs, 1, "{s:?}");

    // Frozen (shared) views are accepted too, and the early variant.
    let frozen = buf.freeze();
    let (res, pending) = client.send_zc_fixed_early(frozen.clone()).await;
    let n = res.unwrap();
    assert_eq!(read_n(&mut server, n).await, data[..n]);
    drop(pending.await);
    drop(frozen);
    assert_eq!(pool.stats().in_use, 0);
}

/// A buffer registered on another ring is sent as a plain buffer.
#[test]
fn send_zc_fixed_other_ring() {
    let rt_a = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    let pool = match FixedBufPool::new_in(rt_a.driver(), 1, 4096) {
        Ok(pool) => pool,
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => return,
        Err(e) => panic!("{e}"),
    };
    let mut rt_b = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt_b.block_on(async {
        let (client, mut server) = tcp_pair().await;
        let mut buf = pool.get().unwrap();
        buf.extend_from_slice(b"other ring");
        let (res, _buf) = client.send_zc_fixed(buf).await;
        assert_eq!(res.unwrap(), 10);
        assert_eq!(read_n(&mut server, 10).await, b"other ring");
    });
    drop(pool);
    drop(rt_a);
}

#[monoio::test]
async fn send_vectored_zc_echo() {
    let (client, mut server) = tcp_pair().await;
    let parts = vec![b"head|".to_vec(), payload(30 * 1024), b"|tail".to_vec()];
    let total: usize = parts.iter().map(Vec::len).sum();
    let expected = parts.concat();

    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();
    let (res, bufs) = client.send_vectored_zc(VecBuf::from(parts.clone())).await;
    assert_eq!(res.unwrap(), total);
    assert_eq!(Vec::<Vec<u8>>::from(bufs), parts);
    #[cfg(feature = "metrics")]
    let s = monoio::driver_stats();
    #[cfg(feature = "metrics")]
    assert_eq!(s.ops_by_opcode[opcode::SendMsgZc::CODE as usize], 1);
    #[cfg(feature = "metrics")]
    assert_eq!(s.zc_sends, 1);

    // Echo it back the same way.
    let got = read_n(&mut server, total).await;
    assert_eq!(got, expected);
    let (res, _) = server.send_vectored_zc(VecBuf::from(vec![got])).await;
    assert_eq!(res.unwrap(), total);
    let mut client = client;
    assert_eq!(read_n(&mut client, total).await, expected);
}

#[monoio::test]
async fn send_auto_picks_by_size() {
    assert_eq!(ZC_MIN_LEN, 16 * 1024);
    assert!(!zc_worthwhile(ZC_MIN_LEN - 1));
    assert!(zc_worthwhile(ZC_MIN_LEN));

    let (client, mut server) = tcp_pair().await;
    #[cfg(feature = "metrics")]
    let send_code = opcode::Send::CODE as usize;
    #[cfg(feature = "metrics")]
    let zc_code = opcode::SendZc::CODE as usize;

    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();
    let small = payload(100);
    let (res, _) = client.send_auto(small.clone()).await;
    assert_eq!(res.unwrap(), 100);
    assert_eq!(read_n(&mut server, 100).await, small);
    #[cfg(feature = "metrics")]
    let s = monoio::driver_stats();
    #[cfg(feature = "metrics")]
    assert_eq!(
        (s.ops_by_opcode[send_code], s.ops_by_opcode[zc_code]),
        (1, 0)
    );

    let large = payload(ZC_MIN_LEN);
    let (res, buf) = client.send_auto(large.clone()).await;
    let n = res.unwrap();
    assert_eq!(buf, large);
    assert_eq!(read_n(&mut server, n).await, large[..n]);
    #[cfg(feature = "metrics")]
    let s = monoio::driver_stats();
    #[cfg(feature = "metrics")]
    assert_eq!(
        (s.ops_by_opcode[send_code], s.ops_by_opcode[zc_code]),
        (1, 1)
    );
}

/// The futures are `Unpin + 'static` and can be driven by hand.
#[monoio::test]
async fn futures_by_hand() {
    fn assert_unpin_static<T: Unpin + 'static>() {}
    assert_unpin_static::<monoio::net::tcp::SendZcFuture<Vec<u8>>>();
    assert_unpin_static::<monoio::net::tcp::ZcPending<Vec<u8>>>();

    let (client, mut server) = tcp_pair().await;
    let mut fut = client.send_zc_early(b"by hand".to_vec());
    let (res, mut pending) =
        std::future::poll_fn(|cx: &mut Context<'_>| Pin::new(&mut fut).poll(cx)).await;
    assert!(fut.is_terminated());
    assert_eq!(res.unwrap(), 7);
    let buf = std::future::poll_fn(|cx: &mut Context<'_>| Pin::new(&mut pending).poll(cx)).await;
    assert!(pending.is_terminated());
    assert_eq!(buf, b"by hand");
    assert_eq!(read_n(&mut server, 7).await, b"by hand");
}
