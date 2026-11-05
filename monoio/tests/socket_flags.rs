//! Socket op flags: `send_with_flags`, `recv_with_flags`, `recv_poll_first`, `send_vectored`,
//! and the `IoVecArray` buffers.

use std::time::Duration;

use monoio::{
    buf::{IoVecArray, IoVecArrayMut},
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRentExt, Splitable},
    net::{MsgFlags, TcpListener, TcpStream, UnixStream},
    uring::libc,
};

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cli = monoio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (srv, _) = listener.accept().await.unwrap();
    (cli.await, srv)
}

#[monoio::test]
async fn send_with_flags_more_then_flush() {
    let (cli, mut srv) = tcp_pair().await;
    let (res, _) = cli.send_with_flags(b"head:".to_vec(), MsgFlags::MORE).await;
    assert_eq!(res.unwrap(), 5);
    // A send without MORE pushes out what was held back.
    let (res, _) = cli.send_with_flags(b"body".to_vec(), MsgFlags::EMPTY).await;
    assert_eq!(res.unwrap(), 4);
    let (res, buf) = srv.read_exact(vec![0u8; 9]).await;
    res.unwrap();
    assert_eq!(buf, b"head:body");
}

#[monoio::test(timer_enabled = true)]
async fn recv_with_flags_waitall_fills_buffer() {
    let (mut cli, srv) = tcp_pair().await;
    let writer = monoio::spawn(async move {
        for chunk in [&b"abcd"[..], b"efgh", b"ijkl"] {
            let (res, _) = cli.write_all(chunk.to_vec()).await;
            res.unwrap();
            monoio::time::sleep(Duration::from_millis(20)).await;
        }
        cli
    });
    let (res, buf) = srv
        .recv_with_flags(Vec::with_capacity(12), MsgFlags::WAITALL)
        .await;
    assert_eq!(res.unwrap(), 12);
    assert_eq!(buf, b"abcdefghijkl");
    let _cli = writer.await;
}

#[monoio::test]
async fn recv_with_flags_peek_keeps_data() {
    let (mut cli, mut srv) = tcp_pair().await;
    let (res, _) = cli.write_all(b"peek".to_vec()).await;
    res.unwrap();
    let (res, buf) = srv
        .recv_with_flags(Vec::with_capacity(4), MsgFlags::PEEK | MsgFlags::WAITALL)
        .await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(buf, b"peek");
    let (res, buf) = srv.read_exact(vec![0u8; 4]).await;
    res.unwrap();
    assert_eq!(buf, b"peek");
}

#[monoio::test(timer_enabled = true)]
async fn recv_poll_first_waits_for_data() {
    let (mut cli, srv) = tcp_pair().await;
    let writer = monoio::spawn(async move {
        monoio::time::sleep(Duration::from_millis(20)).await;
        let (res, _) = cli.write_all(b"late".to_vec()).await;
        res.unwrap();
        cli
    });
    let (res, buf) = srv
        .recv_poll_first(Vec::with_capacity(16), MsgFlags::EMPTY)
        .await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(buf, b"late");
    let _cli = writer.await;
}

#[monoio::test]
async fn send_vectored_echo() {
    let (cli, mut srv) = tcp_pair().await;
    let mut v: IoVecArray<Vec<u8>, 4> = IoVecArray::new();
    // Reuse the same array for two writes.
    for round in 0..2u8 {
        v.push(vec![b'h', round]).unwrap();
        v.push(b"-body".to_vec()).unwrap();
        let (res, back) = cli.send_vectored(v).await;
        assert_eq!(res.unwrap(), 7);
        v = back;
        let bufs: Vec<Vec<u8>> = v.drain().collect();
        assert_eq!(bufs.len(), 2);
        assert!(v.is_empty());

        let (res, got) = srv.read_exact(vec![0u8; 7]).await;
        res.unwrap();
        assert_eq!(got, [b'h', round, b'-', b'b', b'o', b'd', b'y']);
    }
}

#[monoio::test]
async fn send_vectored_with_flags_and_split_halves() {
    let (cli, srv) = tcp_pair().await;
    let (_cr, mut cw) = cli.into_split();
    let (mut sr, _sw) = srv.into_split();
    let mut v: IoVecArray<&'static [u8], 2> = IoVecArray::new();
    v.push(b"ab").unwrap();
    v.push(b"cd").unwrap();
    assert_eq!(v.push(b"x"), Err(&b"x"[..]));
    let (res, _) = cw.send_vectored_with_flags(v, MsgFlags::DONTWAIT).await;
    assert_eq!(res.unwrap(), 4);
    let (res, _) = cw.send_with_flags(b"ef".to_vec(), MsgFlags::EMPTY).await;
    assert_eq!(res.unwrap(), 2);
    let (res, buf) = sr
        .recv_with_flags(Vec::with_capacity(6), MsgFlags::WAITALL)
        .await;
    assert_eq!(res.unwrap(), 6);
    assert_eq!(buf, b"abcdef");
}

#[monoio::test]
async fn readv_into_io_vec_array_mut() {
    let (mut cli, mut srv) = tcp_pair().await;
    let (res, _) = cli.write_all(b"0123456789".to_vec()).await;
    res.unwrap();
    let mut v: IoVecArrayMut<Vec<u8>, 2> = IoVecArrayMut::new();
    v.push(Vec::with_capacity(4)).unwrap();
    v.push(Vec::with_capacity(6)).unwrap();
    let (res, mut v) = srv.read_vectored_exact(v).await;
    res.unwrap();
    let bufs: Vec<Vec<u8>> = v.drain().collect();
    assert_eq!(bufs[0], b"0123");
    assert_eq!(bufs[1], b"456789");
}

#[test]
fn io_vec_array_advance() {
    use monoio::buf::IoVecBuf;

    let mut v: IoVecArray<Vec<u8>, 3> = IoVecArray::new();
    v.push(b"abc".to_vec()).unwrap();
    v.push(b"de".to_vec()).unwrap();
    v.push(b"fgh".to_vec()).unwrap();
    assert!(v.is_full());
    assert_eq!(v.total_len(), 8);

    // Inside the first buffer.
    v.advance(1);
    assert_eq!(v.len(), 3);
    assert_eq!(v.total_len(), 7);
    // Finishes "abc" and one byte of "de".
    v.advance(3);
    assert_eq!(v.len(), 2);
    assert_eq!(v.total_len(), 4);
    let first = unsafe { *v.read_iovec_ptr() };
    assert_eq!(first.iov_len, 1);
    assert_eq!(unsafe { *(first.iov_base as *const u8) }, b'e');
    assert_eq!(v.get(0).unwrap(), b"de");
    // Room again after the shift.
    v.push(b"i".to_vec()).unwrap();
    v.advance(100);
    assert!(v.is_empty());
    assert_eq!(v.total_len(), 0);
}

#[monoio::test]
async fn send_vectored_partial_then_advance() {
    let (cli, mut srv) = tcp_pair().await;
    let mut v: IoVecArray<Vec<u8>, 2> = IoVecArray::new();
    v.push(vec![1u8; 1000]).unwrap();
    v.push(vec![2u8; 1000]).unwrap();
    // Loop like write_all would.
    while !v.is_empty() {
        let (res, back) = cli.send_vectored(v).await;
        v = back;
        v.advance(res.unwrap());
    }
    let (res, buf) = srv.read_exact(vec![0u8; 2000]).await;
    res.unwrap();
    assert!(buf[..1000].iter().all(|&b| b == 1));
    assert!(buf[1000..].iter().all(|&b| b == 2));
}

#[monoio::test]
async fn send_vectored_to_reset_peer_gives_error_not_sigpipe() {
    // Restore the default SIGPIPE action: a SIGPIPE would now kill the test process.
    let old = unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };

    let (cli, srv) = tcp_pair().await;
    // Close the server side with a RST.
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    let rc = unsafe {
        libc::setsockopt(
            std::os::unix::io::AsRawFd::as_raw_fd(&srv),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            &linger as *const _ as *const _,
            std::mem::size_of::<libc::linger>() as _,
        )
    };
    assert_eq!(rc, 0);
    drop(srv);

    let mut err = None;
    for _ in 0..100 {
        let mut v: IoVecArray<Vec<u8>, 2> = IoVecArray::new();
        v.push(vec![0u8; 1024]).unwrap();
        v.push(vec![0u8; 1024]).unwrap();
        let (res, _) = cli.send_vectored(v).await;
        if let Err(e) = res {
            err = Some(e);
            break;
        }
    }
    unsafe { libc::signal(libc::SIGPIPE, old) };
    let err = err.expect("send to a reset peer must fail");
    let code = err.raw_os_error();
    assert!(
        code == Some(libc::EPIPE) || code == Some(libc::ECONNRESET),
        "unexpected error: {err:?}"
    );
}

#[monoio::test]
async fn unix_stream_flags() {
    let (a, mut b) = UnixStream::pair().unwrap();
    let (res, _) = a.send_with_flags(b"x".to_vec(), MsgFlags::EMPTY).await;
    assert_eq!(res.unwrap(), 1);
    let mut v: IoVecArray<Vec<u8>, 2> = IoVecArray::new();
    v.push(b"yz".to_vec()).unwrap();
    let (res, _) = a.send_vectored(v).await;
    assert_eq!(res.unwrap(), 2);
    let (res, buf) = b
        .recv_with_flags(Vec::with_capacity(3), MsgFlags::WAITALL)
        .await;
    assert_eq!(res.unwrap(), 3);
    assert_eq!(buf, b"xyz");
    // EOF after the peer closes.
    drop(a);
    let (res, _) = b.read(Vec::with_capacity(1)).await;
    assert_eq!(res.unwrap(), 0);
}
