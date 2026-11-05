//! Fixed file table: `RuntimeBuilder::with_fixed_files`, `register_fixed`, `accept_direct`,
//! `accept_multi_direct`.

use std::{
    future::Future,
    io::{self, Write},
    net::SocketAddr,
    os::unix::io::{AsRawFd, IntoRawFd},
};

use monoio::{
    buf::IoVecArray,
    fs::OpenOptions,
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    net::{AcceptFlags, TcpListener, TcpStream, UnixStream},
    uring::{self, fixed_file_stats, opcode},
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

fn in_use() -> u32 {
    fixed_file_stats().expect("no fixed file table").in_use
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}

/// Write `rounds` messages on `a` and read each one back from `b`, then the other way.
async fn ping_pong<A, B>(a: &mut A, b: &mut B, rounds: usize)
where
    A: AsyncReadRent + AsyncWriteRent,
    B: AsyncReadRent + AsyncWriteRent,
{
    for i in 0..rounds {
        let msg = format!("message {i:05}").into_bytes();
        let (res, _) = a.write_all(msg.clone()).await;
        res.unwrap();
        let (res, got) = b.read_exact(vec![0u8; msg.len()]).await;
        res.unwrap();
        assert_eq!(got, msg);
        // Echo it back.
        let (res, _) = b.write_all(got).await;
        res.unwrap();
        let (res, back) = a.read_exact(vec![0u8; msg.len()]).await;
        res.unwrap();
        assert_eq!(back, msg);
    }
}

#[test]
fn builder_registers_table() {
    run(8, 0, || async {
        let stats = fixed_file_stats().expect("table");
        assert_eq!(
            (stats.capacity, stats.in_use, stats.kernel_capacity),
            (8, 0, 0)
        );
    });
    // Off by default.
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        assert!(fixed_file_stats().is_none());
        let (a, _b) = tcp_pair().await;
        let err = a.register_fixed().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(!a.is_fixed());
    });
    // Also with the timer driver.
    let rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_fixed_files(4)
        .enable_timer()
        .build();
    if let Ok(mut rt) = rt {
        rt.block_on(async { assert_eq!(fixed_file_stats().unwrap().capacity, 4) });
    }
}

#[test]
fn register_fixed_tcp_echo() {
    run(4, 0, || async {
        let (mut a, mut b) = tcp_pair().await;
        a.register_fixed().unwrap();
        b.register_fixed().unwrap();
        // Idempotent.
        a.register_fixed().unwrap();
        assert!(a.is_fixed() && b.is_fixed());
        assert!(!a.is_direct());
        assert_ne!(a.fixed_slot(), b.fixed_slot());
        assert_eq!(in_use(), 2);

        // Raw-fd APIs keep working on a registered stream.
        assert!(a.as_raw_fd() >= 0);
        a.set_nodelay(true).unwrap();
        assert!(a.nodelay().unwrap());
        assert_eq!(a.peer_addr().unwrap(), b.local_addr().unwrap());

        ping_pong(&mut a, &mut b, 200).await;

        // Vectored and flagged paths too.
        let mut v: IoVecArray<Vec<u8>, 2> = IoVecArray::new();
        v.push(b"ab".to_vec()).unwrap();
        v.push(b"cd".to_vec()).unwrap();
        let (res, _) = a.send_vectored(v).await;
        assert_eq!(res.unwrap(), 4);
        let (res, buf) = b.read_exact(vec![0u8; 4]).await;
        res.unwrap();
        assert_eq!(&buf, b"abcd");

        // Shutdown on a registered stream: the peer reads EOF.
        a.shutdown().await.unwrap();
        let (res, _) = b.read(vec![0u8; 8]).await;
        assert_eq!(res.unwrap(), 0);

        drop(a);
        drop(b);
        settle().await;
        assert_eq!(in_use(), 0);
    });
}

/// After `register_fixed` the ops name the slot, not the fd: they keep reaching the socket
/// even when the fd number is pointed at another file.
#[test]
fn registered_ops_use_the_slot() {
    run(2, 0, || async {
        let (mut a, mut b) = tcp_pair().await;
        a.register_fixed().unwrap();
        let devnull = std::fs::File::open("/dev/null").unwrap();
        // SAFETY: replaces the fd `a` owns; the table still references the socket.
        assert!(unsafe { libc::dup2(devnull.as_raw_fd(), a.as_raw_fd()) } >= 0);
        ping_pong(&mut a, &mut b, 5).await;
        // Closing `a` closes the slot (the socket) and the fd (now /dev/null).
        drop(a);
        settle().await;
        let (res, _) = b.read(vec![0u8; 8]).await;
        assert_eq!(res.unwrap(), 0);
        assert_eq!(in_use(), 0);
    });
}

#[test]
fn slots_are_reused() {
    run(2, 0, || async {
        // Many more streams than slots, one pair at a time.
        for _ in 0..20 {
            let (mut a, mut b) = tcp_pair().await;
            a.register_fixed().unwrap();
            b.register_fixed().unwrap();
            ping_pong(&mut a, &mut b, 2).await;
            drop((a, b));
            settle().await;
        }
        let stats = fixed_file_stats().unwrap();
        assert_eq!((stats.in_use, stats.high_water, stats.exhausted), (0, 2, 0));
    });
}

#[test]
fn table_exhaustion() {
    run(1, 0, || async {
        let (a, b) = tcp_pair().await;
        a.register_fixed().unwrap();
        let err = b.register_fixed().unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
        assert!(!b.is_fixed());
        assert_eq!(fixed_file_stats().unwrap().exhausted, 1);

        // accept_direct fails before accepting: the connection stays in the backlog.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let err = listener.accept_direct().await.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENFILE));

        // Freeing the slot makes room.
        drop(a);
        settle().await;
        let (stream, _) = listener.accept_direct().await.unwrap();
        assert!(stream.is_direct());
    });
}

#[test]
fn into_raw_fd_leaves_the_table() {
    run(2, 0, || async {
        let (a, mut b) = tcp_pair().await;
        a.register_fixed().unwrap();
        assert_eq!(in_use(), 1);
        let fd = a.into_raw_fd();
        assert_eq!(in_use(), 0);
        // The fd is still open and usable.
        let mut std =
            unsafe { <std::net::TcpStream as std::os::unix::io::FromRawFd>::from_raw_fd(fd) };
        std.write_all(b"hi").unwrap();
        let (res, buf) = b.read_exact(vec![0u8; 2]).await;
        res.unwrap();
        assert_eq!(&buf, b"hi");
    });
}

#[test]
fn registered_listener_accepts() {
    run(4, 0, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.register_fixed().unwrap();
        assert!(listener.is_fixed());
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
        let (mut client, (mut server, peer)) = (client.unwrap(), accepted.unwrap());
        assert_eq!(peer, client.local_addr().unwrap());
        ping_pong(&mut client, &mut server, 3).await;
        // The multishot accept works on a registered listener too.
        let mut accepts = listener.accept_multi(AcceptFlags::EMPTY).unwrap();
        let (c, s) = monoio::join!(TcpStream::connect(addr), accepts.next());
        let (mut c, (mut s, _)) = (c.unwrap(), s.unwrap().unwrap());
        ping_pong(&mut c, &mut s, 3).await;
    });
}

async fn direct_pair(listener: &TcpListener) -> (TcpStream, TcpStream, SocketAddr) {
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept_direct());
    let (server, peer) = accepted.unwrap();
    (client.unwrap(), server, peer)
}

#[test]
fn accept_direct_echo() {
    run(4, 0, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let (mut client, mut server, peer) = direct_pair(&listener).await;
        assert!(server.is_direct() && server.is_fixed());
        assert_eq!(server.as_raw_fd(), -1);
        assert_eq!(in_use(), 1);
        // The peer address comes from the accept.
        assert_eq!(peer, client.local_addr().unwrap());
        assert_eq!(server.peer_addr().unwrap(), peer);

        // Raw-fd APIs fail cleanly.
        for err in [
            server.local_addr().unwrap_err(),
            server.set_nodelay(true).unwrap_err(),
            server.nodelay().unwrap_err(),
            server.set_tcp_keepalive(None, None, None).unwrap_err(),
        ] {
            assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err}");
        }

        ping_pong(&mut client, &mut server, 200).await;
        ping_pong(&mut server, &mut client, 10).await;

        // Shutdown goes through IORING_OP_SHUTDOWN: the peer reads EOF.
        server.shutdown().await.unwrap();
        let (res, _) = client.read(vec![0u8; 8]).await;
        assert_eq!(res.unwrap(), 0);

        drop(server);
        settle().await;
        assert_eq!(in_use(), 0);

        // With flags (CLOEXEC is dropped, the kernel would reject it).
        let addr = listener.local_addr().unwrap();
        let (c, s) = monoio::join!(
            TcpStream::connect(addr),
            listener.accept_direct_with(AcceptFlags::NONBLOCK | AcceptFlags::CLOEXEC)
        );
        let (mut c, (mut s, _)) = (c.unwrap(), s.unwrap());
        ping_pong(&mut c, &mut s, 3).await;
    });
}

/// `into_raw_fd` on a direct-only stream has no fd to give: it panics, documented.
#[test]
#[should_panic(expected = "direct-only")]
fn direct_into_raw_fd_panics() {
    let res = run(2, 0, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let (_client, server, _) = direct_pair(&listener).await;
        let _ = server.into_raw_fd();
    });
    if res.is_none() {
        panic!("direct-only (skipped)");
    }
}

#[test]
fn accept_direct_without_table() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let err = listener.accept_direct().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        let err = listener
            .accept_multi_direct(AcceptFlags::EMPTY)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    });
    // A table without kernel slots can't serve a multishot direct accept.
    run(4, 0, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let err = listener
            .accept_multi_direct(AcceptFlags::EMPTY)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    });
}

#[test]
fn accept_multi_direct_echo() {
    run(2, 4, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut accepts = listener.accept_multi_direct(AcceptFlags::EMPTY).unwrap();
        let mut pairs = Vec::new();
        for _ in 0..3 {
            let (client, server) = monoio::join!(TcpStream::connect(addr), accepts.next());
            let server = server.unwrap().unwrap();
            assert!(server.is_direct());
            // Kernel slots follow the runtime's.
            assert!(server.fixed_slot().unwrap() >= 2);
            // A multishot accept reports no address.
            assert_eq!(
                server.peer_addr().unwrap_err().kind(),
                io::ErrorKind::Unsupported
            );
            pairs.push((client.unwrap(), server));
        }
        let stats = fixed_file_stats().unwrap();
        assert_eq!((stats.kernel_in_use, stats.in_use), (3, 0));
        for (client, server) in &mut pairs {
            ping_pong(client, server, 20).await;
        }
        // Runtime slots still work next to kernel ones.
        let (a, _b) = tcp_pair().await;
        a.register_fixed().unwrap();
        assert!(a.fixed_slot().unwrap() < 2);

        drop(pairs);
        drop(accepts);
        settle().await;
        assert_eq!(fixed_file_stats().unwrap().kernel_in_use, 0);

        // The kernel range fills up: the accept ends with ENFILE.
        let mut accepts = listener.accept_multi_direct(AcceptFlags::EMPTY).unwrap();
        let mut held = Vec::new();
        let mut clients = Vec::new();
        let err = loop {
            clients.push(TcpStream::connect(addr).await.unwrap());
            match accepts.next().await {
                Some(Ok(s)) => held.push(s),
                Some(Err(e)) => break e,
                None => panic!("ended without an error"),
            }
        };
        assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
        assert_eq!(held.len(), 4);
    });
}

/// Connections the kernel accepted into kernel slots but nobody took are closed when the
/// stream is dropped.
#[test]
fn accept_multi_direct_drop_closes_untaken() {
    run(0, 4, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accepts = listener.accept_multi_direct(AcceptFlags::EMPTY).unwrap();
        let mut clients = Vec::new();
        for _ in 0..3 {
            clients.push(TcpStream::connect(addr).await.unwrap());
        }
        settle().await;
        drop(accepts);
        settle().await;
        // The slots were cleared: the clients see EOF.
        for mut c in clients {
            let (res, _) = c.read(vec![0u8; 8]).await;
            assert_eq!(res.unwrap(), 0);
        }
        // And all four kernel slots can be used again.
        let mut accepts = listener.accept_multi_direct(AcceptFlags::EMPTY).unwrap();
        let mut held = Vec::new();
        for _ in 0..4 {
            let (c, s) = monoio::join!(TcpStream::connect(addr), accepts.next());
            held.push((c.unwrap(), s.unwrap().unwrap()));
        }
    });
}

/// The slot outlives the stream while an op still uses it, and is freed once it completed.
#[test]
fn drop_with_op_in_flight() {
    run(2, 0, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let (mut client, server, _) = direct_pair(&listener).await;
        let read = server.recv_op(vec![0u8; 16]);
        drop(server);
        // The pending recv holds the file: the slot is still taken.
        assert_eq!(in_use(), 1);
        drop(read);
        // Make the recv complete (cancelled with async-cancel, else by the data).
        let (res, _) = client.write_all(b"late".to_vec()).await;
        res.unwrap();
        settle().await;
        assert_eq!(in_use(), 0);

        // Registered mode: the same.
        let (a, mut b) = tcp_pair().await;
        a.register_fixed().unwrap();
        let read = a.recv_op(vec![0u8; 16]);
        drop(a);
        assert_eq!(in_use(), 1);
        drop(read);
        let (res, _) = b.write_all(b"late".to_vec()).await;
        res.unwrap();
        settle().await;
        assert_eq!(in_use(), 0);
    });
}

/// Dropping an `accept_direct` before it completes never leaks the slot or the connection.
#[test]
fn drop_accept_direct_in_flight() {
    run(1, 0, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        {
            let mut fut = std::pin::pin!(listener.accept_direct());
            assert!(futures::poll!(fut.as_mut()).is_pending());
            assert_eq!(in_use(), 1);
        }
        // Without async-cancel the accept completes with this connection, which is then closed;
        // with it the accept is cancelled and the connection waits in the backlog.
        let mut first = TcpStream::connect(addr).await.unwrap();
        settle().await;
        assert_eq!(in_use(), 0);
        // The table's only slot is usable again.
        let (mut c, (mut s, _)) = {
            let (c, s) = monoio::join!(TcpStream::connect(addr), async {
                // Under async-cancel the first connection is still queued: take it first.
                #[cfg(feature = "async-cancel")]
                {
                    let (s, _) = listener.accept_direct().await.unwrap();
                    drop(s);
                    settle().await;
                }
                listener.accept_direct().await
            });
            (c.unwrap(), s.unwrap())
        };
        ping_pong(&mut c, &mut s, 2).await;
        // The first connection was closed either way.
        let (res, _) = first.read(vec![0u8; 8]).await;
        assert_eq!(res.unwrap(), 0);
    });
}

#[test]
fn register_fixed_unix_stream() {
    run(2, 0, || async {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        a.register_fixed().unwrap();
        b.register_fixed().unwrap();
        assert!(a.is_fixed() && b.is_fixed());
        assert!(a.as_raw_fd() >= 0);
        ping_pong(&mut a, &mut b, 50).await;
        drop((a, b));
        settle().await;
        assert_eq!(in_use(), 0);
    });
}

#[test]
fn register_fixed_file() {
    run(2, 0, || async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .await
            .unwrap();
        file.register_fixed().unwrap();
        assert!(file.is_fixed());
        let (res, _) = file.write_all_at(b"hello fixed".to_vec(), 0).await;
        res.unwrap();
        file.sync_all().await.unwrap();
        let (res, buf) = file.read_exact_at(vec![0u8; 11], 0).await;
        res.unwrap();
        assert_eq!(&buf, b"hello fixed");
        // The fd is still there.
        assert_eq!(file.metadata().await.unwrap().len(), 11);
        assert!(file.as_raw_fd() >= 0);
        // An explicit close frees the slot once it completed.
        file.close().await.unwrap();
        settle().await;
        assert_eq!(in_use(), 0);
        assert_eq!(std::fs::read(tmp.path()).unwrap(), b"hello fixed");
    });
}

#[test]
fn zero_copy_and_cancel_all_use_the_slot() {
    // A direct-only stream has no raw fd: these ops would fail with EBADF if they used it.
    run(4, 0, || async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let (mut client, server, _) = direct_pair(&listener).await;

        let (res, _) = server.write_zc(b"zc".to_vec()).await;
        assert_eq!(res.unwrap(), 2);
        let (res, pending) = server.send_zc_early(b"early".to_vec()).await;
        assert_eq!(res.unwrap(), 5);
        pending.await;
        let mut bufs = IoVecArray::<Vec<u8>, 2>::new();
        bufs.push(b"ve".to_vec()).unwrap();
        bufs.push(b"c".to_vec()).unwrap();
        let (res, _) = server.send_vectored_zc(bufs).await;
        assert_eq!(res.unwrap(), 3);
        let (res, buf) = client.read_exact(vec![0u8; 10]).await;
        res.unwrap();
        assert_eq!(&buf, b"zcearlyvec");

        // Nothing to read: the recv stays in flight until cancel_all ends it.
        let recv = server.recv_op(vec![0u8; 8]);
        server.cancel_all().unwrap();
        let (res, _) = recv.await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ECANCELED));

        // The stream still works.
        let (res, _) = client.write_all(b"ok".to_vec()).await;
        res.unwrap();
        let (res, buf) = server.recv_op(vec![0u8; 2]).await;
        assert_eq!((res.unwrap(), &buf[..]), (2, &b"ok"[..]));
    });
}
