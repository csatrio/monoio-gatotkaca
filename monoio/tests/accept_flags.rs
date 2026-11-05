//! `accept_with` sets `SOCK_NONBLOCK` / `SOCK_CLOEXEC` on the accepted socket.

use std::os::unix::io::AsRawFd;

use monoio::{
    net::{AcceptFlags, TcpListener, TcpStream, UnixListener, UnixStream},
    uring::libc,
};

fn nonblock(fd: i32) -> bool {
    let fl = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(fl >= 0);
    fl & libc::O_NONBLOCK != 0
}

fn cloexec(fd: i32) -> bool {
    let fl = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(fl >= 0);
    fl & libc::FD_CLOEXEC != 0
}

#[monoio::test]
async fn tcp_accept_plain_sets_no_flags() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cli = monoio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (srv, _) = listener.accept().await.unwrap();
    let _cli = cli.await;
    assert!(!nonblock(srv.as_raw_fd()));
    assert!(!cloexec(srv.as_raw_fd()));
}

#[monoio::test]
async fn tcp_accept_with_flags() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cli = monoio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (srv, peer) = listener
        .accept_with(AcceptFlags::NONBLOCK | AcceptFlags::CLOEXEC)
        .await
        .unwrap();
    let cli = cli.await;
    assert_eq!(peer, cli.local_addr().unwrap());
    assert!(nonblock(srv.as_raw_fd()));
    assert!(cloexec(srv.as_raw_fd()));
}

#[monoio::test]
async fn tcp_accept_with_one_flag() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cli = monoio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (srv, _) = listener.accept_with(AcceptFlags::CLOEXEC).await.unwrap();
    let _cli = cli.await;
    assert!(!nonblock(srv.as_raw_fd()));
    assert!(cloexec(srv.as_raw_fd()));
}

#[monoio::test]
async fn tcp_nonblocking_accepted_socket_still_works() {
    use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cli = monoio::spawn(async move {
        let mut c = TcpStream::connect(addr).await.unwrap();
        let (res, _) = c.write_all(b"ping".to_vec()).await;
        res.unwrap();
        c
    });
    let (mut srv, _) = listener.accept_with(AcceptFlags::NONBLOCK).await.unwrap();
    // io_uring polls internally, so a read before data arrives still waits.
    let (res, buf) = srv.read_exact(vec![0u8; 4]).await;
    res.unwrap();
    assert_eq!(buf, b"ping");
    let _cli = cli.await;
}

#[monoio::test]
async fn tcp_cancelable_accept_with() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let canceller = monoio::io::Canceller::new();
    let cli = monoio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (srv, _) = listener
        .cancelable_accept_with(AcceptFlags::NONBLOCK, canceller.handle())
        .await
        .unwrap();
    let _cli = cli.await;
    assert!(nonblock(srv.as_raw_fd()));

    // A canceled handle fails right away.
    let canceller = monoio::io::Canceller::new();
    let handle = canceller.handle();
    canceller.cancel();
    let err = listener
        .cancelable_accept_with(AcceptFlags::NONBLOCK, handle)
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ECANCELED));
}

#[monoio::test]
async fn unix_accept_with_flags() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("accept_flags.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let p = path.clone();
    let cli = monoio::spawn(async move { UnixStream::connect(p).await.unwrap() });
    let (srv, _) = listener
        .accept_with(AcceptFlags::NONBLOCK | AcceptFlags::CLOEXEC)
        .await
        .unwrap();
    let _cli = cli.await;
    assert!(nonblock(srv.as_raw_fd()));
    assert!(cloexec(srv.as_raw_fd()));

    let p = path.clone();
    let cli = monoio::spawn(async move { UnixStream::connect(p).await.unwrap() });
    let (srv, _) = listener.accept().await.unwrap();
    let _cli = cli.await;
    assert!(!nonblock(srv.as_raw_fd()));
}
