//! Sockets a server can use without an fd (round 3, K1): `shutdown_with` / `shutdown_both`,
//! socket options through the ring (`set_sockopt`, `get_sockopt`, `set_nodelay_async`,
//! `set_tcp_keepalive_async`) and `accept_batch_direct_into`. Each call is tested on a stream
//! with an fd (plain syscalls) and on a direct-only stream (ring ops).

use std::{
    future::Future,
    io::{self, Read, Write},
    net::Shutdown,
    os::unix::io::{AsRawFd, FromRawFd, OwnedFd},
    rc::Rc,
    time::Duration,
};

use libc::{c_int, IPPROTO_TCP, SOL_SOCKET};
use monoio::{
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    net::{AcceptFlags, ListenerOpts, TcpListener, TcpStream, UnixStream},
    uring::{self, fixed_file_stats, opcode, types},
    IoUringDriver, RuntimeBuilder,
};

/// Run `f` on a fresh runtime with a fixed file table of `user` + `kernel` slots. `None` (the
/// test is skipped) if the kernel can't register it.
fn run<F: Future>(user: u32, kernel: u32, f: impl FnOnce() -> F) -> Option<F::Output> {
    let mut builder = RuntimeBuilder::<IoUringDriver>::new();
    if user > 0 {
        builder = builder.with_fixed_files(user);
    }
    if kernel > 0 {
        builder = builder.with_fixed_files_kernel_alloc(kernel);
    }
    match builder.build() {
        Ok(mut rt) => Some(rt.block_on(f())),
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
            eprintln!("skipped: fixed file table unsupported: {e}");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

/// Let the driver reap the closes pushed by drops (each round is a submit and a reap).
async fn settle() {
    for _ in 0..4 {
        // SAFETY: a NOP points to nothing.
        let op = unsafe { uring::submit(opcode::Nop::new().build(), ()) }.unwrap();
        op.await;
    }
}

/// A connected pair; the server side has an fd.
async fn fd_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}

/// A connected pair; the server side is direct-only (needs a runtime slot).
async fn direct_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept_direct());
    let server = accepted.unwrap().0;
    assert!(server.is_direct() && server.as_raw_fd() == -1);
    (client.unwrap(), server)
}

/// Whether `e` says that the kernel has no socket options through the ring (before 6.7).
fn no_ring_sockopt(e: &io::Error) -> bool {
    if e.kind() == io::ErrorKind::Unsupported {
        eprintln!("skipped: {e}");
        return true;
    }
    false
}

/// An fd for the socket of a direct-only stream (`IORING_OP_FIXED_FD_INSTALL`, Linux 6.8+),
/// to look at the socket with plain syscalls. `None` on older kernels.
async fn install_fd(stream: &TcpStream) -> Option<OwnedFd> {
    let slot = stream.fixed_slot().expect("direct-only");
    let sqe = opcode::FixedFdInstall::new(types::Fixed(slot), 0).build();
    // SAFETY: the SQE points to nothing.
    let (cqe, ()) = unsafe { uring::submit(sqe, ()) }.unwrap().await;
    if cqe.res() < 0 {
        eprintln!(
            "no FIXED_FD_INSTALL: {}",
            io::Error::from_raw_os_error(-cqe.res())
        );
        return None;
    }
    // SAFETY: the kernel handed us this new fd.
    Some(unsafe { OwnedFd::from_raw_fd(cqe.res()) })
}

/// `getsockopt(2)` of an int option.
fn sys_getsockopt(fd: &impl AsRawFd, level: c_int, name: c_int) -> c_int {
    let mut value: c_int = 0;
    let mut len = std::mem::size_of::<c_int>() as libc::socklen_t;
    // SAFETY: `value` and `len` are valid for the call.
    let ret = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            level,
            name,
            (&mut value as *mut c_int).cast(),
            &mut len,
        )
    };
    assert_eq!(ret, 0, "{}", io::Error::last_os_error());
    value
}

/// `shutdown_with` in every direction, on `server` (fd or direct-only) against `client`.
async fn shutdown_directions(pair: impl Fn() -> PairFuture) {
    // Write: what was sent before is delivered, then the peer reads EOF; the other direction
    // stays open.
    let (mut client, mut server) = pair().await;
    let (res, _) = server.write_all(b"last words".to_vec()).await;
    res.unwrap();
    server.shutdown_with(Shutdown::Write).await.unwrap();
    let (res, buf) = client.read_exact(vec![0u8; 10]).await;
    res.unwrap();
    assert_eq!(&buf, b"last words");
    let (res, _) = client.read(vec![0u8; 8]).await;
    assert_eq!(res.unwrap(), 0);
    let (res, _) = client.write_all(b"still open".to_vec()).await;
    res.unwrap();
    let (res, buf) = server.read_exact(vec![0u8; 10]).await;
    res.unwrap();
    assert_eq!(&buf, b"still open");
    // A send after the write shutdown fails.
    let (res, _) = server.write(b"x".to_vec()).await;
    assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EPIPE));

    // Read: our receives see EOF, our sends still arrive.
    let (mut client, mut server) = pair().await;
    server.shutdown_with(Shutdown::Read).await.unwrap();
    let (res, _) = server.read(vec![0u8; 8]).await;
    assert_eq!(res.unwrap(), 0);
    let (res, _) = server.write_all(b"one way".to_vec()).await;
    res.unwrap();
    let (res, buf) = client.read_exact(vec![0u8; 7]).await;
    res.unwrap();
    assert_eq!(&buf, b"one way");

    // Both.
    let (mut client, mut server) = pair().await;
    let (res, _) = server.write_all(b"bye".to_vec()).await;
    res.unwrap();
    server.shutdown_both().await.unwrap();
    let (res, _) = server.read(vec![0u8; 8]).await;
    assert_eq!(res.unwrap(), 0);
    let (res, _) = server.write(b"x".to_vec()).await;
    assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EPIPE));
    let (res, buf) = client.read_exact(vec![0u8; 3]).await;
    res.unwrap();
    assert_eq!(&buf, b"bye");
    let (res, _) = client.read(vec![0u8; 8]).await;
    assert_eq!(res.unwrap(), 0);
}

type PairFuture = std::pin::Pin<Box<dyn Future<Output = (TcpStream, TcpStream)>>>;

/// Compiles only if `f` borrows nothing (here: not the stream it was made from).
fn owned<F: Future + 'static>(f: F) -> F {
    f
}

/// A supervisor task shuts the connection down while its own task has a receive (and then a
/// send) in flight: the receive completes with EOF or an error.
async fn shutdown_from_supervisor(client: TcpStream, server: TcpStream) {
    let server = Rc::new(server);
    let conn = {
        let server = server.clone();
        monoio::spawn(async move {
            // Nothing arrives: this stays in flight until the shutdown.
            let (res, _) = server.recv_op(vec![0u8; 16]).await;
            let received = res.unwrap_or(0);
            // The connection's task goes on and finds the socket shut down.
            let (res, _) = server.send_op(b"too late".to_vec()).await;
            (received, res.map_err(|e| e.raw_os_error()))
        })
    };
    // Let the task push its receive and park.
    settle().await;
    assert!(!conn.is_finished());
    // `&self`, and the future borrows nothing: `server` is free again at once.
    let shutdown = owned(server.shutdown_both());
    shutdown.await.unwrap();
    let (received, sent) = conn.await;
    assert_eq!(received, 0);
    assert_eq!(sent, Err(Some(libc::EPIPE)));
    // The other calls return such futures too. Their results do not matter on a socket that
    // is shut down; they are awaited because an op in flight keeps the socket open.
    let _ = owned(server.shutdown_with(Shutdown::Read)).await;
    let _ = owned(server.set_sockopt(SOL_SOCKET, libc::SO_KEEPALIVE, &(1 as c_int))).await;
    let _ = owned(unsafe { server.get_sockopt::<c_int>(SOL_SOCKET, libc::SO_KEEPALIVE) }).await;
    let _ = owned(server.set_nodelay_async(true)).await;
    let _ = owned(server.set_tcp_keepalive_async(None, None, None)).await;
    // The peer sees the end of the stream.
    let mut client = client;
    let (res, _) = client.read(vec![0u8; 8]).await;
    assert_eq!(res.unwrap(), 0);
}

#[monoio::test]
async fn shutdown_with_fd_stream() {
    shutdown_directions(|| Box::pin(fd_pair())).await;
    assert!(!fd_pair().await.1.is_direct());
}

#[test]
fn shutdown_with_direct_stream() {
    run(4, 0, || async {
        shutdown_directions(|| Box::pin(direct_pair())).await;
    });
}

#[monoio::test]
async fn shutdown_both_ends_a_pending_receive_fd() {
    let (client, server) = fd_pair().await;
    shutdown_from_supervisor(client, server).await;
}

#[test]
fn shutdown_both_ends_a_pending_receive_direct() {
    run(2, 0, || async {
        let (client, server) = direct_pair().await;
        shutdown_from_supervisor(client, server).await;
        settle().await;
        assert_eq!(fixed_file_stats().unwrap().in_use, 0);
    });
}

/// The shutdown is requested by the call: dropping the future does not undo it.
#[test]
fn dropped_shutdown_future_still_shuts_down() {
    run(2, 0, || async {
        for direct in [false, true] {
            let (mut client, server) = if direct {
                direct_pair().await
            } else {
                fd_pair().await
            };
            drop(server.shutdown_with(Shutdown::Write));
            let (res, _) = client.read(vec![0u8; 8]).await;
            assert_eq!(res.unwrap(), 0, "direct: {direct}");
        }
    });
}

#[monoio::test]
async fn unix_shutdown_and_sockopt() {
    let (mut a, mut b) = UnixStream::pair().unwrap();
    let (res, _) = a.write_all(b"data".to_vec()).await;
    res.unwrap();
    a.shutdown_with(Shutdown::Write).await.unwrap();
    let (res, buf) = b.read_exact(vec![0u8; 4]).await;
    res.unwrap();
    assert_eq!(&buf, b"data");
    let (res, _) = b.read(vec![0u8; 4]).await;
    assert_eq!(res.unwrap(), 0);
    // The other direction still works.
    let (res, _) = b.write_all(b"back".to_vec()).await;
    res.unwrap();
    let (res, buf) = a.read_exact(vec![0u8; 4]).await;
    res.unwrap();
    assert_eq!(&buf, b"back");

    // From another task, with a receive in flight.
    let a = Rc::new(a);
    let conn = {
        let a = a.clone();
        monoio::spawn(async move { a.recv_op(vec![0u8; 8]).await.0 })
    };
    settle().await;
    assert!(!conn.is_finished());
    a.shutdown_both().await.unwrap();
    assert_eq!(conn.await.unwrap(), 0);
    let (res, _) = b.read(vec![0u8; 4]).await;
    assert_eq!(res.unwrap(), 0);

    // Options: plain syscalls.
    let (a, _b) = UnixStream::pair().unwrap();
    a.set_sockopt(SOL_SOCKET, libc::SO_SNDBUF, &(64 * 1024 as c_int))
        .await
        .unwrap();
    // The kernel doubles the value.
    let got: c_int = unsafe { a.get_sockopt(SOL_SOCKET, libc::SO_SNDBUF) }
        .await
        .unwrap();
    assert_eq!(got, 2 * 64 * 1024);
    let err = a
        .set_sockopt(SOL_SOCKET, 0x7fff, &(1 as c_int))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOPROTOOPT));
}

#[monoio::test]
async fn sockopt_fd_stream() {
    let (_client, server) = fd_pair().await;
    assert!(!server.nodelay().unwrap());
    server.set_nodelay_async(true).await.unwrap();
    assert!(server.nodelay().unwrap());
    // The syscall path reads any level.
    let got: c_int = unsafe { server.get_sockopt(IPPROTO_TCP, libc::TCP_NODELAY) }
        .await
        .unwrap();
    assert_eq!(got, 1);
    server.set_nodelay_async(false).await.unwrap();
    assert!(!server.nodelay().unwrap());

    // The call is made when the method is called: the future need not be polled.
    drop(server.set_nodelay_async(true));
    assert!(server.nodelay().unwrap());

    server
        .set_sockopt(SOL_SOCKET, libc::SO_RCVBUF, &(32 * 1024 as c_int))
        .await
        .unwrap();
    assert_eq!(
        sys_getsockopt(&server, SOL_SOCKET, libc::SO_RCVBUF),
        2 * 32 * 1024
    );

    server
        .set_tcp_keepalive_async(
            Some(Duration::from_secs(120)),
            Some(Duration::from_secs(15)),
            Some(4),
        )
        .await
        .unwrap();
    assert_eq!(sys_getsockopt(&server, SOL_SOCKET, libc::SO_KEEPALIVE), 1);
    assert_eq!(
        sys_getsockopt(&server, IPPROTO_TCP, libc::TCP_KEEPIDLE),
        120
    );
    assert_eq!(
        sys_getsockopt(&server, IPPROTO_TCP, libc::TCP_KEEPINTVL),
        15
    );
    assert_eq!(sys_getsockopt(&server, IPPROTO_TCP, libc::TCP_KEEPCNT), 4);

    // A struct-valued option.
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 7,
    };
    server
        .set_sockopt(SOL_SOCKET, libc::SO_LINGER, &linger)
        .await
        .unwrap();
    let got: libc::linger = unsafe { server.get_sockopt(SOL_SOCKET, libc::SO_LINGER) }
        .await
        .unwrap();
    assert_eq!((got.l_onoff, got.l_linger), (1, 7));

    // Errors are the kernel's.
    let err = server
        .set_sockopt(SOL_SOCKET, 0x7fff, &(1 as c_int))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOPROTOOPT));
    let err = unsafe { server.get_sockopt::<c_int>(SOL_SOCKET, 0x7fff) }
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOPROTOOPT));
}

#[test]
fn sockopt_direct_stream() {
    run(4, 0, || async {
        let (mut client, mut server) = direct_pair().await;

        // The synchronous calls have no fd to use; the error names the async way.
        for err in [
            server.set_nodelay(true).unwrap_err(),
            server.nodelay().unwrap_err(),
            server.set_tcp_keepalive(None, None, None).unwrap_err(),
        ] {
            assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err}");
            assert!(err.to_string().contains("set_nodelay_async"), "{err}");
        }

        // SOL_SOCKET options: set and read back, both through the ring.
        match server
            .set_sockopt(SOL_SOCKET, libc::SO_KEEPALIVE, &(1 as c_int))
            .await
        {
            Err(e) if no_ring_sockopt(&e) => return,
            r => r.unwrap(),
        }
        let got: c_int = unsafe { server.get_sockopt(SOL_SOCKET, libc::SO_KEEPALIVE) }
            .await
            .unwrap();
        assert_eq!(got, 1);
        server
            .set_sockopt(SOL_SOCKET, libc::SO_RCVBUF, &(32 * 1024 as c_int))
            .await
            .unwrap();
        let got: c_int = unsafe { server.get_sockopt(SOL_SOCKET, libc::SO_RCVBUF) }
            .await
            .unwrap();
        assert_eq!(got, 2 * 32 * 1024);
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 7,
        };
        server
            .set_sockopt(SOL_SOCKET, libc::SO_LINGER, &linger)
            .await
            .unwrap();
        let got: libc::linger = unsafe { server.get_sockopt(SOL_SOCKET, libc::SO_LINGER) }
            .await
            .unwrap();
        assert_eq!((got.l_onoff, got.l_linger), (1, 7));
        // An option shorter than `T`: the rest is zero.
        let got: [c_int; 2] = unsafe { server.get_sockopt(SOL_SOCKET, libc::SO_KEEPALIVE) }
            .await
            .unwrap();
        assert_eq!(got, [1, 0]);

        // TCP options: set through the ring; the kernel reads only SOL_SOCKET through it.
        server.set_nodelay_async(true).await.unwrap();
        server
            .set_tcp_keepalive_async(
                Some(Duration::from_secs(120)),
                Some(Duration::from_secs(15)),
                Some(4),
            )
            .await
            .unwrap();
        match unsafe { server.get_sockopt::<c_int>(IPPROTO_TCP, libc::TCP_NODELAY) }.await {
            // A kernel that learned other levels.
            Ok(v) => assert_eq!(v, 1),
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::Unsupported, "{e}"),
        }
        // So look at the socket through an fd installed for the test.
        if let Some(fd) = install_fd(&server).await {
            assert_eq!(sys_getsockopt(&fd, IPPROTO_TCP, libc::TCP_NODELAY), 1);
            assert_eq!(sys_getsockopt(&fd, IPPROTO_TCP, libc::TCP_KEEPIDLE), 120);
            assert_eq!(sys_getsockopt(&fd, IPPROTO_TCP, libc::TCP_KEEPINTVL), 15);
            assert_eq!(sys_getsockopt(&fd, IPPROTO_TCP, libc::TCP_KEEPCNT), 4);
            assert_eq!(sys_getsockopt(&fd, SOL_SOCKET, libc::SO_KEEPALIVE), 1);
            server.set_nodelay_async(false).await.unwrap();
            assert_eq!(sys_getsockopt(&fd, IPPROTO_TCP, libc::TCP_NODELAY), 0);
            // Not polled, not even kept: the SQE was pushed by the call and is not cancelled.
            drop(server.set_nodelay_async(true));
            settle().await;
            assert_eq!(sys_getsockopt(&fd, IPPROTO_TCP, libc::TCP_NODELAY), 1);
        }

        // Errors are the kernel's.
        let err = server
            .set_sockopt(SOL_SOCKET, 0x7fff, &(1 as c_int))
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOPROTOOPT), "{err}");
        let err = server
            .set_sockopt(IPPROTO_TCP, libc::TCP_NODELAY, &1u8)
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL), "{err}");
        let err = unsafe { server.get_sockopt::<c_int>(SOL_SOCKET, 0x7fff) }
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOPROTOOPT), "{err}");

        // The stream is unharmed.
        let (res, _) = client.write_all(b"ping".to_vec()).await;
        res.unwrap();
        let (res, buf) = server.read_exact(vec![0u8; 4]).await;
        res.unwrap();
        assert_eq!(&buf, b"ping");
    });
}

fn listener() -> TcpListener {
    let opts = ListenerOpts::new().backlog(4096);
    TcpListener::bind_with_config("127.0.0.1:0", &opts).unwrap()
}

/// Multishot accept needs Linux 5.19.
fn no_multishot(e: &io::Error) -> bool {
    if e.raw_os_error() == Some(libc::EINVAL) {
        eprintln!("skipped: multishot accept unsupported: {e}");
        return true;
    }
    false
}

const N: usize = 300;

#[test]
fn accept_batch_direct_takes_the_queue() {
    run(2, 512, || async {
        let listener = listener();
        let addr = listener.local_addr().unwrap();
        // The kernel completes the handshakes: all of them wait in the accept queue.
        let mut clients: Vec<_> = (0..N)
            .map(|_| std::net::TcpStream::connect(addr).unwrap())
            .collect();

        let mut conns = Vec::with_capacity(N);
        let mut calls = 0;
        while conns.len() < N {
            match listener
                .accept_batch_direct_into(&mut conns, usize::MAX, AcceptFlags::EMPTY)
                .await
            {
                Err(e) if conns.is_empty() && no_multishot(&e) => return,
                r => assert!(r.unwrap() >= 1),
            }
            calls += 1;
        }
        eprintln!("{N} direct connections in {calls} calls");
        assert_eq!(conns.len(), N);
        assert!(calls <= 3, "{calls} calls");
        let stats = fixed_file_stats().unwrap();
        assert_eq!((stats.kernel_in_use, stats.in_use), (N as u32, 0));
        for conn in &conns {
            assert!(conn.is_direct() && conn.as_raw_fd() == -1);
            // Kernel slots follow the 2 runtime slots.
            assert!(conn.fixed_slot().unwrap() >= 2);
            assert_eq!(
                conn.peer_addr().unwrap_err().kind(),
                io::ErrorKind::Unsupported
            );
        }

        // Echo on every direct-only stream. Accept order is connect order.
        for (i, client) in clients.iter_mut().enumerate() {
            client
                .write_all(format!("hello {i:04}").as_bytes())
                .unwrap();
        }
        for (i, conn) in conns.iter_mut().enumerate() {
            let (res, buf) = conn.read_exact(vec![0u8; 10]).await;
            res.unwrap();
            assert_eq!(buf, format!("hello {i:04}").into_bytes());
            let (res, _) = conn.write_all(buf).await;
            res.unwrap();
        }
        for (i, client) in clients.iter_mut().enumerate() {
            let mut buf = [0u8; 10];
            client.read_exact(&mut buf).unwrap();
            assert_eq!(&buf[..], format!("hello {i:04}").as_bytes());
        }

        // The accept stays armed: a later connection needs no new submission, and `max` is
        // respected with the rest kept queued.
        let _late: Vec<_> = (0..5)
            .map(|_| std::net::TcpStream::connect(addr).unwrap())
            .collect();
        let mut late = Vec::new();
        while late.len() < 5 {
            let n = listener
                .accept_batch_direct_into(&mut late, 2, AcceptFlags::CLOEXEC)
                .await
                .unwrap();
            assert!((1..=2).contains(&n));
        }
        // Other flags while it is armed (`CLOEXEC` above is ignored for direct accepts).
        let err = listener
            .accept_batch_direct(2, AcceptFlags::NONBLOCK)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // Dropping the streams gives the slots back.
        drop(conns);
        drop(late);
        settle().await;
        assert_eq!(fixed_file_stats().unwrap().kernel_in_use, 0);
        // The clients see the close.
        let mut buf = [0u8; 1];
        assert_eq!(clients[0].read(&mut buf).unwrap(), 0);
    });
}

#[test]
fn accept_batch_direct_one_task_at_a_time() {
    run(0, 16, || async {
        let listener = Rc::new(listener());
        let addr = listener.local_addr().unwrap();
        let first = {
            let listener = listener.clone();
            monoio::spawn(async move { listener.accept_batch_direct(16, AcceptFlags::EMPTY).await })
        };
        // Let `first` arm and park.
        monoio::spawn(async {}).await;
        let err = listener
            .accept_batch_direct(16, AcceptFlags::EMPTY)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        // The fd flavour has its own accept and is not blocked by the direct one.
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        let got = match first.await {
            Err(e) if no_multishot(&e) => return,
            r => r.unwrap(),
        };
        assert_eq!(got.len(), 1);
        client.write_all(b"hi").unwrap();
        let (res, buf) = got[0].recv_op(vec![0u8; 2]).await;
        assert_eq!((res.unwrap(), &buf[..]), (2, &b"hi"[..]));

        // A dropped call keeps the accept armed and its connections queued.
        monoio::select! {
            r = listener.accept_batch_direct(16, AcceptFlags::EMPTY) => panic!("{r:?}"),
            _ = monoio::spawn(async {}) => {}
        }
        let _client2 = std::net::TcpStream::connect(addr).unwrap();
        let got2 = listener
            .accept_batch_direct(16, AcceptFlags::EMPTY)
            .await
            .unwrap();
        assert_eq!(got2.len(), 1);
        assert_eq!(fixed_file_stats().unwrap().kernel_in_use, 2);
    });
}

/// Without kernel-allocated slots: the error of `accept_multi_direct`.
#[test]
fn accept_batch_direct_needs_kernel_slots() {
    let check = || async {
        let listener = listener();
        let want = listener
            .accept_multi_direct(AcceptFlags::EMPTY)
            .unwrap_err();
        let err = listener
            .accept_batch_direct(8, AcceptFlags::EMPTY)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert_eq!(err.to_string(), want.to_string());
        // The fd flavour is not affected.
        let _client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        match listener.accept_batch(8, AcceptFlags::EMPTY).await {
            Err(e) if no_multishot(&e) => {}
            r => assert_eq!(r.unwrap().len(), 1),
        }
    };
    // No table at all, and a table with runtime slots only.
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(check());
    run(4, 0, check);
}

/// A full kernel range ends the accept with `ENFILE`; the next call arms a new one, which
/// works once slots are free again.
#[test]
fn accept_batch_direct_full_table() {
    run(0, 4, || async {
        let listener = listener();
        let addr = listener.local_addr().unwrap();
        let _clients: Vec<_> = (0..6)
            .map(|_| std::net::TcpStream::connect(addr).unwrap())
            .collect();
        let mut held = Vec::new();
        let err = loop {
            match listener
                .accept_batch_direct_into(&mut held, 16, AcceptFlags::EMPTY)
                .await
            {
                Err(e) if held.is_empty() && no_multishot(&e) => return,
                Err(e) => break e,
                Ok(_) => assert!(held.len() <= 4),
            }
        };
        assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
        assert_eq!(held.len(), 4);

        drop(held);
        settle().await;
        assert_eq!(fixed_file_stats().unwrap().kernel_in_use, 0);
        let _client = std::net::TcpStream::connect(addr).unwrap();
        let mut again = Vec::new();
        // Connections the kernel could not place were closed by it; whatever is still queued
        // plus the new one arrive now.
        let n = listener
            .accept_batch_direct_into(&mut again, 16, AcceptFlags::EMPTY)
            .await
            .unwrap();
        // 2 on Linux 7.0: the 6th client and the new one. The 5th got `ENFILE`.
        eprintln!("accepted after ENFILE: {n}");
        assert!(n >= 1 && again.iter().all(|s| s.is_direct()));
    });
}

/// The whole server path on direct-only streams: batch accept, nodelay through the ring, a
/// supervisor that ends connections with `shutdown_both`.
#[test]
fn direct_server_with_supervisor() {
    run(0, 64, || async {
        let listener = listener();
        let addr = listener.local_addr().unwrap();
        // Clients of this runtime: the echo tasks must run while they wait.
        let mut clients = Vec::new();
        for _ in 0..8 {
            clients.push(TcpStream::connect(addr).await.unwrap());
        }
        let mut conns = Vec::new();
        while conns.len() < 8 {
            match listener
                .accept_batch_direct_into(&mut conns, 64, AcceptFlags::EMPTY)
                .await
            {
                Err(e) if conns.is_empty() && no_multishot(&e) => return,
                r => r.unwrap(),
            };
        }
        // One SQE each, pushed by the calls; the results arrive together.
        let nodelay: Vec<_> = conns.iter().map(|c| c.set_nodelay_async(true)).collect();
        for res in nodelay {
            match res.await {
                Err(e) if no_ring_sockopt(&e) => break,
                r => r.unwrap(),
            }
        }
        // Each connection echoes until EOF or an error.
        let conns: Vec<_> = conns.into_iter().map(Rc::new).collect();
        let tasks: Vec<_> = conns
            .iter()
            .map(|conn| {
                let conn = conn.clone();
                monoio::spawn(async move {
                    let mut echoed = 0;
                    loop {
                        let (res, buf) = conn.recv_op(vec![0u8; 64]).await;
                        match res {
                            Ok(0) | Err(_) => return echoed,
                            Ok(n) => {
                                echoed += n;
                                let (res, _) = conn.send_op(buf).await;
                                if res.is_err() {
                                    return echoed;
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        for client in &mut clients {
            let (res, _) = client.write_all(b"echo me".to_vec()).await;
            res.unwrap();
        }
        for client in &mut clients {
            let (res, buf) = client.read_exact(vec![0u8; 7]).await;
            res.unwrap();
            assert_eq!(&buf, b"echo me");
        }
        // The supervisor ends every connection; their tasks are parked in a receive.
        for conn in &conns {
            conn.shutdown_both().await.unwrap();
        }
        for task in tasks {
            assert_eq!(task.await, 7);
        }
        for client in &mut clients {
            let (res, _) = client.read(vec![0u8; 1]).await;
            assert_eq!(res.unwrap(), 0);
        }
        drop(conns);
        settle().await;
        assert_eq!(fixed_file_stats().unwrap().kernel_in_use, 0);
    });
}
