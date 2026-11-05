//! `cancel_all`: one `IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_ALL` SQE cancels every
//! in-flight op on a socket and leaves other sockets alone.

use monoio::{
    io::AsyncWriteRentExt,
    net::{TcpListener, TcpStream, UnixStream},
};

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}

/// Cancel-by-fd needs Linux 5.19; the cancel then fails silently and the ops run on.
fn cancel_fd_supported() -> bool {
    let mut uname: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut uname) } != 0 {
        return false;
    }
    let release = unsafe { std::ffi::CStr::from_ptr(uname.release.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    let mut it = release.split(|c: char| !c.is_ascii_digit());
    let major: u32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: u32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (major, minor) >= (5, 19)
}

#[monoio::test]
async fn cancel_all_tcp() {
    if !cancel_fd_supported() {
        eprintln!("skipped: cancel by fd needs Linux 5.19");
        return;
    }
    let (mut client, server) = tcp_pair().await;
    let (mut other_client, other_server) = tcp_pair().await;

    let recvs: Vec<_> = (0..4u8).map(|i| server.recv_op(vec![i])).collect();
    let other = other_server.recv_op(Vec::with_capacity(16));

    monoio::reset_driver_stats();
    server.cancel_all().unwrap();
    for (i, recv) in recvs.into_iter().enumerate() {
        let (res, buf) = recv.await;
        assert_eq!(
            res.unwrap_err().raw_os_error(),
            Some(libc::ECANCELED),
            "recv {i}"
        );
        // The buffer comes back untouched.
        assert_eq!(buf, [i as u8]);
    }
    let s = monoio::driver_stats();
    assert_eq!(s.cancel_fd_pushed, 1, "{s:?}");
    assert_eq!(s.cancels_pushed, 0, "{s:?}");

    // The other connection is not affected.
    let (res, _) = other_client.write_all(b"still here".to_vec()).await;
    res.unwrap();
    let (res, buf) = other.await;
    assert_eq!(res.unwrap(), 10);
    assert_eq!(buf, b"still here");

    // Nothing in flight: a cancel is harmless, and the stream works on.
    server.cancel_all().unwrap();
    let (res, _) = client.write_all(b"after".to_vec()).await;
    res.unwrap();
    let (res, buf) = server.recv_op(Vec::with_capacity(16)).await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(buf, b"after");
}

#[monoio::test]
async fn cancel_all_unix() {
    if !cancel_fd_supported() {
        return;
    }
    let (a, mut b) = UnixStream::pair().unwrap();
    let r1 = a.recv_op(Vec::with_capacity(8));
    let r2 = a.recv_op(Vec::with_capacity(8));
    a.cancel_all().unwrap();
    for recv in [r1, r2] {
        let (res, buf) = recv.await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
        assert_eq!(buf.capacity(), 8);
    }
    let (res, _) = b.write_all(b"ok".to_vec()).await;
    res.unwrap();
    let (res, buf) = a.recv_op(Vec::with_capacity(8)).await;
    assert_eq!(res.unwrap(), 2);
    assert_eq!(buf, b"ok");
}

#[test]
fn cancel_all_outside_runtime_errors() {
    let stream = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = stream.local_addr().unwrap();
    let std_stream = std::net::TcpStream::connect(addr).unwrap();
    // Building a monoio stream needs no runtime; cancelling does.
    let stream = TcpStream::from_std(std_stream).unwrap();
    assert!(stream.cancel_all().is_err());
    // Dropping a TcpStream outside a runtime just closes the fd.
    drop(stream);
}
