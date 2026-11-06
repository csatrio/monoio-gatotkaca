//! Multishot recv: `TcpStream::recv_multi` / `UnixStream::recv_multi`.

use std::{io::Write, time::Duration};

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    net::{TcpListener, TcpStream, UnixStream},
    uring::BufRing,
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

/// Multishot recv needs Linux 6.0.
fn unsupported(e: &std::io::Error) -> bool {
    if e.raw_os_error() == Some(libc::EINVAL) {
        eprintln!("skipped: multishot recv unsupported: {e}");
        return true;
    }
    false
}

async fn sleep(ms: u64) {
    monoio::time::sleep(Duration::from_millis(ms)).await;
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = monoio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (server, _) = listener.accept().await.unwrap();
    (server, client.await)
}

#[monoio::test(timer_enabled = true)]
async fn echo_until_eof() {
    let Some(ring) = ring(21, 8, 256) else { return };
    let (server, mut client) = tcp_pair().await;

    let echo = monoio::spawn(async move {
        let mut server = server;
        let mut chunks = server.recv_multi(&ring).unwrap();
        let mut total = 0;
        while let Some(buf) = chunks.next().await {
            let buf = match buf {
                Err(e) if total == 0 && unsupported(&e) => return None,
                b => b.unwrap(),
            };
            total += buf.len();
            // A `SelectedBuf` is written back as is.
            let (res, _) = server.write_all(buf).await;
            res.unwrap();
        }
        // EOF ended the stream.
        assert!(chunks.is_finished());
        assert!(chunks.next().await.is_none());
        drop(chunks);
        assert_eq!(ring.stats().in_use, 0);
        Some(total)
    });

    let mut sent = 0;
    for i in 0..20u8 {
        let msg = vec![i; 100 + i as usize];
        sent += msg.len();
        let (res, msg) = client.write_all(msg).await;
        res.unwrap();
        let (res, back) = client.read_exact(vec![0u8; msg.len()]).await;
        if res.is_err() {
            // The server skipped (no multishot recv).
            assert!(echo.await.is_none());
            return;
        }
        assert_eq!(back, msg);
    }
    client.shutdown().await.unwrap();
    assert_eq!(echo.await, Some(sent));
}

#[monoio::test(timer_enabled = true)]
async fn enobufs_then_rearm() {
    let Some(ring) = ring(22, 2, 16) else { return };
    let (a, mut b) = std::os::unix::net::UnixStream::pair().unwrap();
    let a = UnixStream::from_std(a).unwrap();

    let mut chunks = a.recv_multi(&ring).unwrap();
    let mut held = Vec::new();
    for msg in [&b"first"[..], b"second"] {
        b.write_all(msg).unwrap();
        let buf = match chunks.next().await.unwrap() {
            Err(e) if unsupported(&e) => return,
            r => r.unwrap(),
        };
        assert_eq!(&*buf, msg);
        held.push(buf);
    }

    // Both buffers are held: the kernel ends the recv with ENOBUFS.
    b.write_all(b"third").unwrap();
    let err = chunks.next().await.unwrap().unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOBUFS));
    assert!(chunks.next().await.is_none());
    assert!(chunks.is_finished());
    #[cfg(feature = "metrics")]
    assert!(ring.stats().enobufs >= 1);

    // Free the buffers and re-arm: the data is still in the socket.
    drop(held);
    let mut chunks = a.recv_multi(&ring).unwrap();
    assert_eq!(&*chunks.next().await.unwrap().unwrap(), b"third");
}

#[monoio::test(timer_enabled = true)]
async fn drop_returns_queued_buffers() {
    let Some(ring) = ring(23, 4, 64) else { return };
    let (a, mut b) = std::os::unix::net::UnixStream::pair().unwrap();
    let a = UnixStream::from_std(a).unwrap();

    let mut chunks = a.recv_multi(&ring).unwrap();
    b.write_all(b"x").unwrap();
    match chunks.next().await.unwrap() {
        Err(e) if unsupported(&e) => return,
        r => drop(r.unwrap()),
    }
    // Queue a few chunks without taking them.
    for msg in [&b"1"[..], b"2", b"3"] {
        b.write_all(msg).unwrap();
        sleep(10).await;
    }
    drop(chunks);
    sleep(20).await;
    // Every buffer is back: a new recv can fill all four.
    assert_eq!(ring.stats().in_use, 0);
    let mut chunks = a.recv_multi(&ring).unwrap();
    let mut held = Vec::new();
    for i in 0..4u8 {
        b.write_all(&[b'a' + i]).unwrap();
        held.push(chunks.next().await.unwrap().unwrap());
    }
    assert_eq!(ring.stats().in_use, 4);

    // Cancel ends the stream.
    chunks.cancel();
    assert!(chunks.next().await.is_none());
}

// ---- bundles (`recv_multi_bundle`, `recv_bundle`; Linux 6.10+) ----

/// A ring for bundle tests, or `None` if the kernel has no bundles.
fn bundle_ring(bgid: u16, entries: u16, buf_size: usize) -> Option<BufRing> {
    let ring = ring(bgid, entries, buf_size)?;
    if !ring.supports_bundles() {
        eprintln!("skipped: recv bundles unsupported");
        return None;
    }
    Some(ring)
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

// A small deterministic PRNG (xorshift).
struct Rng(u64);

impl Rng {
    fn next(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

#[monoio::test(timer_enabled = true)]
async fn bundle_reassembles_a_large_message() {
    let Some(ring) = bundle_ring(24, 16, 4096) else {
        return;
    };
    let (server, mut client) = tcp_pair().await;
    let msg = pattern(40 * 1024, 7);
    let (res, msg) = client.write_all(msg).await;
    res.unwrap();
    sleep(20).await;

    let mut bundles = server.recv_multi_bundle(&ring).unwrap();
    let mut got = Vec::new();
    let mut most = 0;
    while got.len() < msg.len() {
        let bundle = bundles.next().await.unwrap().unwrap();
        assert!(!bundle.is_empty());
        most = most.max(bundle.len());
        let bytes = bundle.bytes();
        let before = got.len();
        for buf in bundle {
            assert!(buf.len() <= 4096);
            got.extend_from_slice(&buf);
        }
        assert_eq!(got.len() - before, bytes);
    }
    assert_eq!(got, msg);
    // The data was all queued: some completion covered several buffers.
    assert!(most > 1, "no bundle had more than one buffer");
    assert_eq!(ring.stats().in_use, 0);

    client.shutdown().await.unwrap();
    assert!(bundles.next().await.is_none());
    assert!(bundles.is_finished());
}

#[monoio::test(timer_enabled = true)]
async fn bundles_with_out_of_order_recycling() {
    let Some(ring) = bundle_ring(25, 16, 256) else {
        return;
    };
    let (server, mut client) = tcp_pair().await;
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let total: usize = 200 * 1024;
    let sent = pattern(total, 3);
    let writer = monoio::spawn({
        let sent = sent.clone();
        async move {
            let mut off = 0;
            let mut rng = Rng(12345);
            while off < sent.len() {
                let n = (1 + rng.next(3000)).min(sent.len() - off);
                let (res, _) = client.write_all(sent[off..off + n].to_vec()).await;
                res.unwrap();
                off += n;
                if rng.next(4) == 0 {
                    sleep(1).await;
                }
            }
            client
        }
    });

    let mut got = Vec::with_capacity(total);
    let mut held = Vec::new();
    let mut rearms = 0;
    'outer: while got.len() < total {
        let mut bundles = server.recv_multi_bundle(&ring).unwrap();
        while let Some(item) = bundles.next().await {
            let bundle = match item {
                Ok(bundle) => bundle,
                Err(e) if e.raw_os_error() == Some(libc::ENOBUFS) => {
                    // Release everything, then re-arm.
                    held.clear();
                    assert!(bundles.next().await.is_none());
                    rearms += 1;
                    continue 'outer;
                }
                Err(e) => panic!("{e}"),
            };
            for buf in bundle {
                got.extend_from_slice(&buf);
                held.push(buf);
            }
            // Drop some held buffers, in a scrambled order.
            while held.len() > 6 {
                let i = rng.next(held.len());
                held.swap_remove(i);
            }
            if got.len() >= total {
                break 'outer;
            }
        }
        panic!("recv ended early");
    }
    drop(held);
    assert_eq!(got.len(), total);
    assert!(got == sent, "data corrupted");
    assert_eq!(ring.stats().in_use, 0);
    drop(writer.await);
    eprintln!("re-armed {rearms} times");
}

#[monoio::test(timer_enabled = true)]
async fn shared_ring_bundles_taken_out_of_order() {
    let Some(ring) = bundle_ring(26, 32, 512) else {
        return;
    };
    let (sa, mut ca) = tcp_pair().await;
    let (sb, mut cb) = tcp_pair().await;
    let mut a = sa.recv_multi_bundle(&ring).unwrap();
    let mut b = sb.recv_multi_bundle(&ring).unwrap();
    let mut got_a = Vec::new();
    let mut got_b = Vec::new();
    let mut sent_a = Vec::new();
    let mut sent_b = Vec::new();
    for round in 0..30u8 {
        // Interleave sends so the two streams consume interleaved slots, then take the
        // completions of `b` first.
        for i in 0..3u8 {
            let ma = pattern(300 + 97 * i as usize, round.wrapping_add(i));
            let mb = pattern(
                700 + 211 * i as usize,
                round.wrapping_mul(3).wrapping_add(i),
            );
            sent_a.extend_from_slice(&ma);
            sent_b.extend_from_slice(&mb);
            ca.write_all(ma).await.0.unwrap();
            sleep(1).await;
            cb.write_all(mb).await.0.unwrap();
            sleep(1).await;
        }
        while got_b.len() < sent_b.len() {
            got_b.extend(b.next().await.unwrap().unwrap().detach());
        }
        while got_a.len() < sent_a.len() {
            got_a.extend(a.next().await.unwrap().unwrap().detach());
        }
    }
    assert!(got_a == sent_a, "stream a corrupted");
    assert!(got_b == sent_b, "stream b corrupted");
    assert_eq!(ring.stats().in_use, 0);
}

#[monoio::test(timer_enabled = true)]
async fn bundle_enobufs_then_rearm() {
    let Some(ring) = bundle_ring(27, 2, 16) else {
        return;
    };
    let (a, mut b) = std::os::unix::net::UnixStream::pair().unwrap();
    let a = UnixStream::from_std(a).unwrap();

    let mut bundles = a.recv_multi_bundle(&ring).unwrap();
    // 32 bytes fill both buffers.
    b.write_all(&pattern(32, 1)).unwrap();
    let mut held = Vec::new();
    let mut got = Vec::new();
    while got.len() < 32 {
        let bundle = bundles.next().await.unwrap().unwrap();
        for buf in bundle {
            got.extend_from_slice(&buf);
            held.push(buf);
        }
    }
    assert_eq!(got, pattern(32, 1));

    b.write_all(b"more").unwrap();
    let err = bundles.next().await.unwrap().unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOBUFS));
    assert!(bundles.next().await.is_none());
    #[cfg(feature = "metrics")]
    assert!(ring.stats().enobufs >= 1);

    drop(held);
    let mut bundles = a.recv_multi_bundle(&ring).unwrap();
    assert_eq!(bundles.next().await.unwrap().unwrap().detach(), b"more");
}

#[monoio::test(timer_enabled = true)]
async fn drop_returns_queued_bundles() {
    let Some(ring) = bundle_ring(28, 16, 64) else {
        return;
    };
    let (server, mut client) = tcp_pair().await;
    let mut bundles = server.recv_multi_bundle(&ring).unwrap();
    client.write_all(b"x".to_vec()).await.0.unwrap();
    drop(bundles.next().await.unwrap().unwrap());
    // Queue several multi-buffer bundles without taking them.
    for i in 0..3u8 {
        client.write_all(pattern(200, i)).await.0.unwrap();
        sleep(10).await;
    }
    drop(bundles);
    sleep(20).await;
    assert_eq!(ring.stats().in_use, 0);

    // The data of the dropped bundles is gone, but all 16 buffers are free again.
    let mut bundles = server.recv_multi_bundle(&ring).unwrap();
    client.write_all(pattern(16 * 64, 9)).await.0.unwrap();
    sleep(20).await;
    let mut held = Vec::new();
    let mut got = Vec::new();
    while got.len() < 16 * 64 {
        for buf in bundles.next().await.unwrap().unwrap() {
            got.extend_from_slice(&buf);
            held.push(buf);
        }
    }
    assert_eq!(got, pattern(16 * 64, 9));
    assert_eq!(ring.stats().in_use, 16);
    drop(held);
    bundles.cancel();
    assert!(bundles.next().await.is_none());
}

#[monoio::test(timer_enabled = true)]
async fn single_shot_bundle_and_incremental_ring() {
    let opts = BufRing::options().incremental(true);
    let ring = match BufRing::with_options(29, 8, 1024, opts) {
        Ok(ring) if ring.supports_bundles() => ring,
        _ => {
            eprintln!("skipped: incremental rings or bundles unsupported");
            return;
        }
    };
    let (server, mut client) = tcp_pair().await;
    let msg = pattern(3000, 5);
    client.write_all(msg.clone()).await.0.unwrap();
    sleep(20).await;

    let mut got = Vec::new();
    let mut held = Vec::new();
    while got.len() < msg.len() {
        let bundle = server.recv_bundle(&ring).await.unwrap();
        assert!(!bundle.is_empty());
        for buf in bundle {
            got.extend_from_slice(&buf);
            held.push(buf);
        }
    }
    assert_eq!(got, msg);
    // A small message continues in the partially used last buffer.
    client.write_all(b"tail".to_vec()).await.0.unwrap();
    let mut tail = server.recv_bundle(&ring).await.unwrap();
    assert_eq!(tail.len(), 1);
    let buf = tail.next().unwrap();
    assert_eq!(&*buf, b"tail");
    assert_eq!(buf.offset(), 3000 % 1024);
    drop((held, buf));
    assert_eq!(ring.stats().in_use, 0);

    // EOF is an empty bundle.
    client.shutdown().await.unwrap();
    assert!(server.recv_bundle(&ring).await.unwrap().is_empty());
}
