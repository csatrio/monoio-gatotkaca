//! `TcpStream::read_with_deadline` / `write_with_deadline` (kernel link timeouts).

use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream as StdTcpStream,
    time::{Duration, Instant},
};

use monoio::{
    io::AsyncReadRent,
    net::{TcpListener, TcpStream},
    IoUringDriver, RuntimeBuilder,
};

/// A connected pair: the monoio side and a blocking std peer.
async fn pair() -> (TcpStream, StdTcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = StdTcpStream::connect(addr).unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    (stream, peer)
}

#[monoio::test]
async fn read_times_out_and_returns_the_buffer() {
    let (stream, _peer) = pair().await;
    let before = monoio::driver_stats().link_timeouts;
    let start = Instant::now();
    let (res, buf) = stream
        .read_with_deadline(Vec::with_capacity(64), Duration::from_millis(30))
        .await;
    assert_eq!(res.unwrap_err().kind(), ErrorKind::TimedOut);
    assert!(start.elapsed() >= Duration::from_millis(25));
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.capacity(), 64);

    // The timeout's own CQE is counted once it was reaped.
    for _ in 0..20 {
        if monoio::driver_stats().link_timeouts > before {
            break;
        }
        let _ = stream
            .read_with_deadline(Vec::with_capacity(1), Duration::from_millis(1))
            .await;
    }
    assert!(monoio::driver_stats().link_timeouts > before);
}

#[monoio::test]
async fn read_succeeds_when_data_arrives_in_time() {
    let (stream, mut peer) = pair().await;
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        peer.write_all(b"hello").unwrap();
        peer
    });
    let (res, buf) = stream
        .read_with_deadline(Vec::with_capacity(64), Duration::from_secs(5))
        .await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(&buf, b"hello");
    drop(writer.join().unwrap());
}

#[monoio::test]
async fn stream_stays_usable_after_a_timeout() {
    let (mut stream, mut peer) = pair().await;
    let (res, _) = stream
        .read_with_deadline(Vec::with_capacity(8), Duration::from_millis(10))
        .await;
    assert_eq!(res.unwrap_err().kind(), ErrorKind::TimedOut);

    peer.write_all(b"after").unwrap();
    let (res, buf) = stream
        .read_with_deadline(Vec::with_capacity(8), Duration::from_secs(5))
        .await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(&buf, b"after");

    // Plain IO too, in both directions.
    peer.write_all(b"plain").unwrap();
    let (res, buf) = stream.read(Vec::with_capacity(8)).await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(&buf, b"plain");
    let (res, _) = stream
        .write_with_deadline(b"back".to_vec(), Duration::from_secs(5))
        .await;
    assert_eq!(res.unwrap(), 4);
    let mut got = [0u8; 4];
    peer.read_exact(&mut got).unwrap();
    assert_eq!(&got, b"back");
}

#[monoio::test]
async fn read_error_is_kept() {
    let (stream, peer) = pair().await;
    drop(peer);
    // EOF, not a timeout.
    let (res, _) = stream
        .read_with_deadline(Vec::with_capacity(8), Duration::from_secs(5))
        .await;
    assert_eq!(res.unwrap(), 0);
}

#[monoio::test]
async fn write_times_out_when_the_peer_does_not_read() {
    let (stream, _peer) = pair().await;
    let chunk = vec![0u8; 256 * 1024];
    let mut buf = chunk;
    for _ in 0..1000 {
        let (res, b) = stream
            .write_with_deadline(buf, Duration::from_millis(20))
            .await;
        buf = b;
        match res {
            Ok(n) => assert!(n > 0),
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::TimedOut);
                assert_eq!(buf.len(), 256 * 1024);
                return;
            }
        }
    }
    panic!("the socket buffers never filled up");
}

#[test]
fn works_without_the_timer() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        let (stream, _peer) = pair().await;
        let (res, _) = stream
            .read_with_deadline(Vec::with_capacity(8), Duration::from_millis(10))
            .await;
        assert_eq!(res.unwrap_err().kind(), ErrorKind::TimedOut);
    });
}
