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
