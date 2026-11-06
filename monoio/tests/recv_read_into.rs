//! Framed reads over a multishot recv: `RecvStream::read_into`, `RecvBundleStream::read_into`
//! and `MultiRecvReader` (round 2, F3).
#![cfg(feature = "bytes")]

use std::{io::Write, time::Duration};

use bytes::{Buf, BytesMut};
use monoio::{
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    net::{TcpListener, TcpStream, UnixStream},
    uring::{BufRing, BufRingOptions},
};

/// Provided buffer rings need Linux 5.19 (incremental ones 6.12); skip on older kernels.
fn ring(bgid: u16, entries: u16, buf_size: usize, options: BufRingOptions) -> Option<BufRing> {
    match BufRing::with_options(bgid, entries, buf_size, options) {
        Ok(ring) => Some(ring),
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
            eprintln!("skipped: buffer ring unsupported: {e}");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

/// Multishot recv needs Linux 6.0.
fn unsupported(e: &std::io::Error) -> bool {
    if e.raw_os_error() == Some(libc::EINVAL) || e.kind() == std::io::ErrorKind::Unsupported {
        eprintln!("skipped: multishot recv unsupported: {e}");
        return true;
    }
    false
}

async fn sleep(ms: u64) {
    monoio::time::sleep(Duration::from_millis(ms)).await;
}

fn pair() -> (UnixStream, std::os::unix::net::UnixStream) {
    let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
    (UnixStream::from_std(a).unwrap(), b)
}

/// Frames: a 4-byte big-endian length, then the body.
fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = (body.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(body);
    out
}

/// Take one complete frame off the front of `buf`, if there is one.
fn parse(buf: &mut BytesMut) -> Option<Vec<u8>> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
    if buf.len() < 4 + len {
        return None;
    }
    buf.advance(4);
    Some(buf.split_to(len).to_vec())
}

fn bodies() -> Vec<Vec<u8>> {
    vec![
        b"hello".to_vec(),
        (0..300u32).map(|i| i as u8).collect(),
        vec![b'z'; 129],
    ]
}

/// Write the frames in chunks that cut through headers and bodies, with pauses so the server
/// sees separate receives.
async fn send_split(peer: &mut std::os::unix::net::UnixStream, wire: &[u8]) {
    let mut rest = wire;
    for cut in [3usize, 1, 7, 64, 65, 200] {
        let (head, tail) = rest.split_at(cut.min(rest.len()));
        peer.write_all(head).unwrap();
        rest = tail;
        sleep(2).await;
    }
    peer.write_all(rest).unwrap();
}

#[monoio::test(timer_enabled = true)]
async fn framed_reads_across_buffer_boundaries() {
    let Some(ring) = ring(41, 8, 64, BufRingOptions::default()) else {
        return;
    };
    let (sock, mut peer) = pair();
    let mut chunks = sock.recv_multi(&ring).unwrap();

    let wire: Vec<u8> = bodies().iter().flat_map(|b| frame(b)).collect();
    let writer = async {
        send_split(&mut peer, &wire).await;
        peer.shutdown(std::net::Shutdown::Write).unwrap();
    };
    let reader = async {
        let mut buf = BytesMut::with_capacity(64);
        let mut frames = Vec::new();
        loop {
            match chunks.read_into(&mut buf).await {
                Err(e) if frames.is_empty() && unsupported(&e) => return None,
                Ok(0) => break,
                r => r.unwrap(),
            };
            while let Some(f) = parse(&mut buf) {
                frames.push(f);
            }
        }
        assert!(buf.is_empty());
        Some(frames)
    };
    let ((), frames) = monoio::join!(writer, reader);
    let Some(frames) = frames else { return };
    assert_eq!(frames, bodies());
    // EOF is sticky, and every buffer went back to the ring.
    assert_eq!(chunks.read_into(&mut BytesMut::new()).await.unwrap(), 0);
    assert_eq!(ring.stats().in_use, 0);
}

/// 2 buffers of 64 bytes and a 64 KiB message: the kernel ends the recv with `ENOBUFS` again
/// and again, `read_into` re-arms each time, and the caller sees all the data.
#[monoio::test(timer_enabled = true)]
async fn tiny_ring_rearms_after_enobufs() {
    let Some(ring) = ring(42, 2, 64, BufRingOptions::default()) else {
        return;
    };
    let (sock, mut peer) = pair();
    let mut chunks = sock.recv_multi(&ring).unwrap();
    let msg: Vec<u8> = (0..64 * 1024u32).map(|i| (i % 251) as u8).collect();
    peer.write_all(&msg).unwrap();

    let mut buf = BytesMut::new();
    while buf.len() < msg.len() {
        match chunks.read_into(&mut buf).await {
            Err(e) if buf.is_empty() && unsupported(&e) => return,
            Ok(0) => panic!("EOF after {} bytes", buf.len()),
            r => r.unwrap(),
        };
    }
    assert_eq!(&buf[..], &msg[..]);
    #[cfg(feature = "metrics")]
    assert!(ring.stats().enobufs >= 1, "the ring never ran dry");

    drop(peer);
    assert_eq!(chunks.read_into(&mut buf).await.unwrap(), 0);
}

/// If the buffers are held elsewhere, the re-armed recv finds the ring still empty and
/// `read_into` reports `ENOBUFS`; once they are freed it continues.
#[monoio::test(timer_enabled = true)]
async fn enobufs_reported_while_buffers_are_held() {
    let Some(ring) = ring(43, 2, 16, BufRingOptions::default()) else {
        return;
    };
    let (sock, mut peer) = pair();
    let mut chunks = sock.recv_multi(&ring).unwrap();
    let mut held = Vec::new();
    for msg in [&b"first"[..], b"second"] {
        peer.write_all(msg).unwrap();
        match chunks.next().await.unwrap() {
            Err(e) if unsupported(&e) => return,
            r => held.push(r.unwrap()),
        }
    }
    peer.write_all(b"third").unwrap();
    let mut buf = BytesMut::new();
    let err = chunks.read_into(&mut buf).await.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOBUFS));
    assert!(!chunks.is_finished());

    drop(held);
    assert_eq!(chunks.read_into(&mut buf).await.unwrap(), 5);
    assert_eq!(&buf[..], b"third");
}

#[monoio::test(timer_enabled = true)]
async fn multi_recv_reader_is_an_async_read_rent() {
    let Some(ring) = ring(44, 4, 32, BufRingOptions::default()) else {
        return;
    };
    let (sock, mut peer) = pair();
    let mut reader = sock.recv_multi(&ring).unwrap().into_reader();

    let wire: Vec<u8> = bodies().iter().flat_map(|b| frame(b)).collect();
    let writer = async {
        send_split(&mut peer, &wire).await;
        drop(peer);
    };
    let read = async {
        let mut frames = Vec::new();
        for _ in 0..3 {
            let (res, len) = reader.read_exact(vec![0u8; 4]).await;
            if let Err(e) = &res {
                if frames.is_empty() && unsupported(e) {
                    return None;
                }
            }
            res.unwrap();
            let len = u32::from_be_bytes(len.try_into().unwrap()) as usize;
            let (res, body) = reader.read_exact(vec![0u8; len]).await;
            res.unwrap();
            frames.push(body);
        }
        // EOF.
        let (res, _) = reader.read(Vec::with_capacity(8)).await;
        assert_eq!(res.unwrap(), 0);
        Some(frames)
    };
    let ((), frames) = monoio::join!(writer, read);
    if let Some(frames) = frames {
        assert_eq!(frames, bodies());
        assert!(reader.buffered().is_empty());
        drop(reader);
        assert_eq!(ring.stats().in_use, 0);
    }
}

#[monoio::test(timer_enabled = true)]
async fn multi_recv_reader_readv_and_leftover() {
    let Some(ring) = ring(45, 4, 64, BufRingOptions::default()) else {
        return;
    };
    let (sock, mut peer) = pair();
    let mut reader = sock.recv_multi(&ring).unwrap().into_reader();
    peer.write_all(b"0123456789").unwrap();

    // A 4-byte read leaves 6 bytes of the ring buffer in the reader.
    let (res, buf) = reader.read(Vec::with_capacity(4)).await;
    match res {
        Err(e) if unsupported(&e) => return,
        r => assert_eq!(r.unwrap(), 4),
    }
    assert_eq!(buf, b"0123");
    assert_eq!(reader.buffered(), b"456789");

    let bufs = monoio::buf::VecBuf::from(vec![vec![0u8; 2], vec![0u8; 8]]);
    let (res, bufs) = reader.readv(bufs).await;
    assert_eq!(res.unwrap(), 6);
    let bufs: Vec<Vec<u8>> = bufs.into();
    assert_eq!(bufs[0], b"45");
    assert_eq!(bufs[1], b"6789");
}

#[monoio::test(timer_enabled = true)]
async fn tcp_read_into_and_bundles() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = monoio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (server, _) = listener.accept().await.unwrap();
    let mut client = client.await;

    let Some(ring) = ring(46, 4, 128, BufRingOptions::default()) else {
        return;
    };
    let mut chunks = if ring.supports_bundles() {
        server.recv_multi_bundle(&ring).map(Err).unwrap()
    } else {
        server.recv_multi(&ring).map(Ok).unwrap()
    };
    let msg: Vec<u8> = (0..10_000u32).map(|i| (i % 253) as u8).collect();
    let (res, _) = client.write_all(msg.clone()).await;
    res.unwrap();
    client.shutdown().await.unwrap();

    let mut buf = BytesMut::new();
    loop {
        let res = match &mut chunks {
            Ok(single) => single.read_into(&mut buf).await,
            Err(bundles) => bundles.read_into(&mut buf).await,
        };
        match res {
            Err(e) if buf.is_empty() && unsupported(&e) => return,
            Ok(0) => break,
            r => r.unwrap(),
        };
    }
    assert_eq!(&buf[..], &msg[..]);
}

/// Incremental rings (6.12+): several receives share one buffer.
#[monoio::test(timer_enabled = true)]
async fn incremental_ring_read_into() {
    let Some(ring) = ring(47, 2, 4096, BufRing::options().incremental(true)) else {
        return;
    };
    let (sock, mut peer) = pair();
    let mut chunks = sock.recv_multi(&ring).unwrap();
    let wire: Vec<u8> = bodies().iter().flat_map(|b| frame(b)).collect();
    let writer = async {
        send_split(&mut peer, &wire).await;
        // More than both buffers together: exhaustion and re-arm with an incremental ring.
        let big = vec![7u8; 3 * 4096];
        peer.write_all(&frame(&big)).unwrap();
        drop(peer);
    };
    let reader = async {
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        loop {
            match chunks.read_into(&mut buf).await {
                Err(e) if frames.is_empty() && buf.is_empty() && unsupported(&e) => return None,
                Ok(0) => break,
                r => r.unwrap(),
            };
            while let Some(f) = parse(&mut buf) {
                frames.push(f);
            }
        }
        Some(frames)
    };
    let ((), frames) = monoio::join!(writer, reader);
    let Some(frames) = frames else { return };
    let mut want = bodies();
    want.push(vec![7u8; 3 * 4096]);
    assert_eq!(frames, want);
    assert_eq!(ring.stats().in_use, 0);
}
