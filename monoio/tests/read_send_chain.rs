//! File range → TCP peer through one linked read→send chain.

use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream as StdTcpStream,
    os::fd::AsRawFd,
};

use monoio::{
    fs::File,
    net::{TcpListener, TcpStream},
    uring::{self, opcode, types},
};

async fn pair() -> (TcpStream, StdTcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = StdTcpStream::connect(addr).unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    (stream, peer)
}

fn temp_file(content: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(content).unwrap();
    f.flush().unwrap();
    f
}

/// Half-close the monoio side right away (dropping it closes through the ring, on a later tick).
fn shutdown_write(stream: &TcpStream) {
    assert_eq!(
        unsafe { libc::shutdown(stream.as_raw_fd(), libc::SHUT_WR) },
        0
    );
}

fn read_n(peer: &mut StdTcpStream, n: usize) -> Vec<u8> {
    let mut got = vec![0u8; n];
    peer.read_exact(&mut got).unwrap();
    got
}

/// The raw form: a `Read` linked to a `Send` of the same buffer, its length matching the file
/// range.
#[monoio::test]
async fn raw_chain_sends_a_file_range() {
    let file = temp_file(b"0123456789abcdef");
    let (stream, mut peer) = pair().await;
    let mut buf = vec![0u8; 6];
    let sqes = uring::link([
        opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), 6)
            .offset(4)
            .build(),
        opcode::Send::new(types::Fd(stream.as_raw_fd()), buf.as_ptr(), 6).build(),
    ]);
    let (cqes, _buf) = unsafe { uring::submit_chain(sqes, buf) }.unwrap().await;
    assert_eq!(cqes.map(|c| c.res()), [6, 6]);
    assert_eq!(read_n(&mut peer, 6), b"456789");
}

/// A short read breaks the link: the kernel cancels the send instead of sending stale bytes.
#[monoio::test]
async fn raw_chain_short_read_cancels_the_send() {
    let file = temp_file(b"0123456789");
    let (stream, mut peer) = pair().await;
    let mut buf = vec![b'x'; 8];
    let sqes = uring::link([
        opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), 8)
            .offset(5)
            .build(),
        opcode::Send::new(types::Fd(stream.as_raw_fd()), buf.as_ptr(), 8).build(),
    ]);
    let (cqes, _buf) = unsafe { uring::submit_chain(sqes, buf) }.unwrap().await;
    assert_eq!(cqes.map(|c| c.res()), [5, -libc::ECANCELED]);
    shutdown_write(&stream);
    let mut rest = Vec::new();
    peer.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty());
}

#[monoio::test]
async fn read_at_then_send() {
    let content: Vec<u8> = (0..100_000u32).map(|i| i as u8).collect();
    let tmp = temp_file(&content);
    let file = File::open(tmp.path()).await.unwrap();
    let (stream, mut peer) = pair().await;
    let reader = std::thread::spawn(move || read_n(&mut peer, 64 * 1024));

    let (res, buf) = file
        .read_at_then_send(Vec::with_capacity(64 * 1024), 1000, &stream)
        .await;
    assert_eq!(res.unwrap(), (64 * 1024, 64 * 1024));
    assert_eq!(buf.len(), 64 * 1024);
    assert_eq!(&buf[..], &content[1000..1000 + 64 * 1024]);
    assert_eq!(reader.join().unwrap(), &content[1000..1000 + 64 * 1024]);
}

#[monoio::test]
async fn read_at_then_send_short_read() {
    let tmp = temp_file(b"0123456789");
    let file = File::open(tmp.path()).await.unwrap();
    let (stream, mut peer) = pair().await;

    let (res, buf) = file
        .read_at_then_send(Vec::with_capacity(8), 5, &stream)
        .await;
    assert_eq!(res.unwrap_err().kind(), ErrorKind::UnexpectedEof);
    assert_eq!(&buf, b"56789");

    // Nothing was sent; the stream still works.
    let (res, _) = file
        .read_at_then_send(Vec::with_capacity(4), 0, &stream)
        .await;
    assert_eq!(res.unwrap(), (4, 4));
    shutdown_write(&stream);
    let mut rest = Vec::new();
    peer.read_to_end(&mut rest).unwrap();
    assert_eq!(rest, b"0123");
}
