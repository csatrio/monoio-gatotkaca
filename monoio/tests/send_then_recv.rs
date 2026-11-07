//! `send_then_recv` / `send_then_recv_fixed` (round 3, C1): a reply and the next receive as
//! one linked pair with one op slot and one CQE.
//!
//! The peer of every test is a plain blocking socket on another thread, so that the runtime
//! under test runs only the pair (and the counters count only it).

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
    io::{self, Read, Write},
    os::fd::{AsRawFd, RawFd},
    pin::Pin,
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};

use monoio::{
    net::{MsgFlags, SendThenRecv, TcpListener, TcpStream, UnixStream},
    time::sleep,
    uring::{libc, FixedBufPool},
    IoUringDriver, RuntimeBuilder,
};

// ---- a counting allocator, for the "nothing leaks" checks ----

struct Counting;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocs() -> u64 {
    ALLOCS.with(|c| c.get())
}

// ---- helpers ----

/// What the tests use of a stream, so that each runs for TCP and for Unix sockets. The peer
/// is a blocking std socket.
trait Stream: AsRawFd + Sized + 'static {
    type Peer: Read + Write + AsRawFd + Send + 'static;
    /// A connected pair: `(the stream under test, its peer)`. Called inside the runtime.
    async fn pair() -> (Self, Self::Peer);
    fn send_then_recv(
        &self,
        reply: Vec<u8>,
        buf: Vec<u8>,
        flags: MsgFlags,
    ) -> SendThenRecv<Vec<u8>, Vec<u8>>;
    fn try_send(&self, buf: &[u8]) -> io::Result<usize>;
    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize>;
    /// Shut down the write side of the stream under test, so that a send fails with `EPIPE`.
    fn shutdown_write(&self);
}

impl Stream for TcpStream {
    type Peer = std::net::TcpStream;

    async fn pair() -> (Self, Self::Peer) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = thread::spawn(move || listener.accept().unwrap().0);
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let peer = accept.join().unwrap();
        peer.set_nodelay(true).unwrap();
        (stream, peer)
    }

    fn send_then_recv(
        &self,
        reply: Vec<u8>,
        buf: Vec<u8>,
        flags: MsgFlags,
    ) -> SendThenRecv<Vec<u8>, Vec<u8>> {
        TcpStream::send_then_recv(self, reply, buf, flags)
    }

    fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        TcpStream::try_send(self, buf)
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        TcpStream::try_recv(self, buf)
    }

    fn shutdown_write(&self) {
        // SAFETY: a valid fd.
        assert_eq!(
            unsafe { libc::shutdown(self.as_raw_fd(), libc::SHUT_WR) },
            0
        );
    }
}

impl Stream for UnixStream {
    type Peer = std::os::unix::net::UnixStream;

    async fn pair() -> (Self, Self::Peer) {
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        (UnixStream::from_std(a).unwrap(), b)
    }

    fn send_then_recv(
        &self,
        reply: Vec<u8>,
        buf: Vec<u8>,
        flags: MsgFlags,
    ) -> SendThenRecv<Vec<u8>, Vec<u8>> {
        UnixStream::send_then_recv(self, reply, buf, flags)
    }

    fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        UnixStream::try_send(self, buf)
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        UnixStream::try_recv(self, buf)
    }

    fn shutdown_write(&self) {
        // SAFETY: a valid fd.
        assert_eq!(
            unsafe { libc::shutdown(self.as_raw_fd(), libc::SHUT_WR) },
            0
        );
    }
}

fn run<S: Stream, F: Future>(f: impl FnOnce(S, S::Peer) -> F) -> F::Output {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    rt.block_on(async {
        let (s, peer) = S::pair().await;
        f(s, peer).await
    })
}

/// Poll a future once with a waker that does nothing.
fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    Pin::new(fut).poll(&mut Context::from_waker(Waker::noop()))
}

fn os_error<T>(res: &io::Result<T>) -> Option<i32> {
    res.as_ref().err().and_then(io::Error::raw_os_error)
}

fn would_block<T>(res: io::Result<T>) -> bool {
    matches!(res, Err(e) if e.kind() == io::ErrorKind::WouldBlock)
}

fn set_buf(fd: RawFd, opt: libc::c_int, size: libc::c_int) {
    // SAFETY: `size` is a valid `int` option value.
    let r = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            opt,
            (&size as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    assert_eq!(r, 0, "{}", io::Error::last_os_error());
}

/// Small socket buffers on both ends, so that a few dozen KiB fill the connection.
fn small_buffers(tx: &impl AsRawFd, rx: &impl AsRawFd, size: libc::c_int) {
    set_buf(tx.as_raw_fd(), libc::SO_SNDBUF, size);
    set_buf(rx.as_raw_fd(), libc::SO_RCVBUF, size);
}

/// Close `peer` with a reset (`SO_LINGER` 0): on TCP a RST, on a Unix socket the queued data
/// is dropped and the other side gets `ECONNRESET`.
fn reset_close(peer: impl AsRawFd) {
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    // SAFETY: a valid linger struct.
    let r = unsafe {
        libc::setsockopt(
            peer.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            (&linger as *const libc::linger).cast(),
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        )
    };
    assert_eq!(r, 0);
    drop(peer);
}

/// Run `cond` every millisecond (the runtime runs in between) until it holds.
async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(std::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(1)).await;
    }
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// The peer: reads exactly `want` bytes, then writes `answer`, then returns what it read.
fn peer_echo<P: Read + Write + Send + 'static>(
    mut peer: P,
    want: usize,
    answer: Vec<u8>,
) -> thread::JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut got = vec![0u8; want];
        peer.read_exact(&mut got).unwrap();
        peer.write_all(&answer).unwrap();
        got
    })
}

// ---- the normal round trip ----

async fn round_trip<S: Stream>(s: S, peer: S::Peer) {
    let reply = payload(70);
    let peer = peer_echo(peer, 70, b"next".to_vec());
    let (send, recv, reply_back, buf) = s
        .send_then_recv(reply.clone(), Vec::with_capacity(64), MsgFlags::EMPTY)
        .await;
    assert_eq!(send.unwrap(), 70, "the send is reported full");
    assert_eq!(recv.unwrap(), 4);
    assert_eq!(&buf[..], b"next");
    assert_eq!(reply_back, reply, "the reply buffer comes back");
    assert_eq!(peer.join().unwrap(), reply);
    // Both guards are gone: the direct calls are not refused any more (the peer is closed by
    // now, so what they return is the socket's business).
    assert!(!would_block(s.try_recv(&mut [0u8; 4])));
    assert!(!would_block(s.try_send(b"x")));
}

#[test]
fn round_trip_tcp() {
    run(round_trip::<TcpStream>);
}

#[test]
fn round_trip_unix() {
    run(round_trip::<UnixStream>);
}

/// The reply already waits in the socket when the pair is pushed (the kernel receives
/// inline): same result.
#[test]
fn reply_already_there() {
    run(|s: UnixStream, mut peer| async move {
        peer.write_all(b"early").unwrap();
        let (send, recv, _, buf) = s
            .send_then_recv(payload(8), Vec::with_capacity(64), MsgFlags::EMPTY)
            .await;
        assert_eq!(send.unwrap(), 8);
        assert_eq!(recv.unwrap(), 5);
        assert_eq!(&buf[..], b"early");
    });
}

/// EOF on the receive: `(Ok(len), Ok(0))`.
#[test]
fn peer_closes_after_the_reply() {
    run(|s: TcpStream, mut peer| async move {
        let peer = thread::spawn(move || {
            let mut got = [0u8; 3];
            peer.read_exact(&mut got).unwrap();
            drop(peer);
        });
        let (send, recv, _, buf) = s
            .send_then_recv(b"abc".to_vec(), Vec::with_capacity(16), MsgFlags::EMPTY)
            .await;
        assert_eq!(send.unwrap(), 3);
        assert_eq!(recv.unwrap(), 0);
        assert!(buf.is_empty());
        peer.join().unwrap();
    });
}

/// `recv_flags` reach the receive: `MSG_WAITALL` fills the buffer from two writes.
#[test]
fn recv_flags_are_applied() {
    run(|s: UnixStream, mut peer| async move {
        let peer = thread::spawn(move || {
            let mut got = [0u8; 2];
            peer.read_exact(&mut got).unwrap();
            peer.write_all(b"abcd").unwrap();
            thread::sleep(Duration::from_millis(20));
            peer.write_all(b"efgh").unwrap();
        });
        let (send, recv, _, buf) = s
            .send_then_recv(b"hi".to_vec(), Vec::with_capacity(8), MsgFlags::WAITALL)
            .await;
        assert_eq!(send.unwrap(), 2);
        assert_eq!(recv.unwrap(), 8);
        assert_eq!(&buf[..], b"abcdefgh");
        peer.join().unwrap();
    });
}

// ---- failed sends: the head's CQE ----

/// The peer reset the connection before the send: the send error and `ECANCELED` for the
/// receive, which did not run.
async fn peer_reset_before_send<S: Stream>(s: S, peer: S::Peer) {
    reset_close(peer);
    // Let the reset arrive.
    sleep(Duration::from_millis(20)).await;
    let (send, recv, reply, buf) = s
        .send_then_recv(payload(70), Vec::with_capacity(64), MsgFlags::EMPTY)
        .await;
    let e = os_error(&send);
    assert!(
        matches!(e, Some(libc::ECONNRESET | libc::EPIPE)),
        "send: {send:?}"
    );
    assert_eq!(os_error(&recv), Some(libc::ECANCELED), "recv: {recv:?}");
    assert_eq!(reply.len(), 70);
    assert_eq!(buf.capacity(), 64);
    assert!(buf.is_empty(), "nothing was received");
    assert!(!would_block(s.try_send(b"x")), "the guards are gone");
}

#[test]
fn peer_reset_before_send_tcp() {
    run(peer_reset_before_send::<TcpStream>);
}

#[test]
fn peer_reset_before_send_unix() {
    run(peer_reset_before_send::<UnixStream>);
}

/// A thousand failed pairs on one socket (the local write side is shut down, so every send
/// fails with `EPIPE` at issue and the head posts the only CQE): the slot is freed each time
/// and nothing leaks. Checked with the counting allocator: after a warm-up, a failed pair
/// allocates nothing (a leaked slot would make the slab grow by pages; a leaked `data` would
/// keep the buffers).
#[test]
fn failed_pairs_free_their_slot() {
    run(|s: UnixStream, _peer| async move {
        s.shutdown_write();
        let mut reply = payload(70);
        let mut buf = Vec::with_capacity(64);
        let one = |reply, buf| s.send_then_recv(reply, buf, MsgFlags::EMPTY);
        for _ in 0..100 {
            let (send, recv, r, b) = one(reply, buf).await;
            assert_eq!(os_error(&send), Some(libc::EPIPE), "{send:?}");
            assert_eq!(os_error(&recv), Some(libc::ECANCELED));
            (reply, buf) = (r, b);
        }
        let before = allocs();
        for _ in 0..1000 {
            let (send, recv, r, b) = one(reply, buf).await;
            assert_eq!(os_error(&send), Some(libc::EPIPE));
            assert_eq!(os_error(&recv), Some(libc::ECANCELED));
            (reply, buf) = (r, b);
        }
        let n = allocs() - before;
        assert_eq!(n, 0, "{n} allocations for 1000 failed pairs");
        #[cfg(feature = "metrics")]
        {
            let d = monoio::driver_stats();
            assert_eq!(d.ignored_inline + d.ignored_boxed, 0, "{d:?}");
        }
    });
}

/// A reply that fills the socket is retried in the kernel (`MSG_WAITALL`); the peer reads it
/// later and answers: full delivery, then the receive.
async fn short_send_peer_reads_later<S: Stream>(s: S, peer: S::Peer) {
    small_buffers(&s, &peer, 16 * 1024);
    let reply = payload(256 * 1024);
    let want = reply.clone();
    let peer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        let mut peer = peer;
        let mut got = vec![0u8; want.len()];
        peer.read_exact(&mut got).unwrap();
        assert_eq!(got, want, "the reply arrived intact");
        peer.write_all(b"ok").unwrap();
    });
    let (send, recv, _, buf) = s
        .send_then_recv(reply, Vec::with_capacity(16), MsgFlags::EMPTY)
        .await;
    assert_eq!(send.unwrap(), 256 * 1024);
    assert_eq!(recv.unwrap(), 2);
    assert_eq!(&buf[..], b"ok");
    peer.join().unwrap();
}

#[test]
fn short_send_peer_reads_later_tcp() {
    run(short_send_peer_reads_later::<TcpStream>);
}

#[test]
fn short_send_peer_reads_later_unix() {
    run(short_send_peer_reads_later::<UnixStream>);
}

/// The peer reads part of a large reply and then resets: the send failed after `n` bytes,
/// which the kernel reports as the count (`Ok(n)`, `n < len`), and the receive did not run
/// (`ECANCELED`).
async fn failed_after_partial_send<S: Stream>(s: S, peer: S::Peer) {
    small_buffers(&s, &peer, 16 * 1024);
    let len = 1024 * 1024;
    let read = 64 * 1024;
    let peer = thread::spawn(move || {
        let mut peer = peer;
        let mut got = vec![0u8; read];
        peer.read_exact(&mut got).unwrap();
        reset_close(peer);
    });
    let (send, recv, reply, buf) = s
        .send_then_recv(payload(len), Vec::with_capacity(16), MsgFlags::EMPTY)
        .await;
    peer.join().unwrap();
    match send {
        Ok(n) => {
            assert!(n >= read && n < len, "failed after {n} of {len} bytes");
            assert_eq!(os_error(&recv), Some(libc::ECANCELED), "{recv:?}");
        }
        // The reset got in before the first send attempt: a failure at issue.
        Err(e) => {
            assert!(
                matches!(e.raw_os_error(), Some(libc::ECONNRESET | libc::EPIPE)),
                "{e}"
            );
            assert_eq!(os_error(&recv), Some(libc::ECANCELED));
        }
    }
    assert_eq!(reply.len(), len);
    assert!(buf.is_empty());
}

#[test]
fn failed_after_partial_send_tcp() {
    run(failed_after_partial_send::<TcpStream>);
}

#[test]
fn failed_after_partial_send_unix() {
    run(failed_after_partial_send::<UnixStream>);
}

// ---- cancel and drop ----

/// `cancel()` while the receive waits: `(Ok(len), Err(ECANCELED))` and both buffers back.
#[test]
fn cancel_returns_both_buffers() {
    run(|s: UnixStream, mut peer| async move {
        let mut fut = s.send_then_recv(payload(70), Vec::with_capacity(64), MsgFlags::EMPTY);
        assert!(poll_once(&mut fut).is_pending());
        // Let the runtime submit the pair (the SQEs reach the kernel at an enter).
        sleep(Duration::from_millis(10)).await;
        let mut got = [0u8; 70];
        peer.read_exact(&mut got).unwrap();
        fut.cancel();
        let (send, recv, reply, buf) = fut.await;
        assert_eq!(send.unwrap(), 70, "the send was through");
        assert_eq!(os_error(&recv), Some(libc::ECANCELED), "{recv:?}");
        assert_eq!(reply.len(), 70);
        assert_eq!(buf.capacity(), 64);
        assert!(buf.is_empty());
        // The stream is usable: what the peer sends now arrives.
        peer.write_all(b"after").unwrap();
        let (res, b) = s.recv_op(Vec::with_capacity(16)).await;
        assert_eq!(res.unwrap(), 5);
        assert_eq!(&b[..], b"after");
    });
}

/// `cancel()` while the send waits for socket space: the send reports what went out
/// (`Ok(n)`, `n < len`, as the head's CQE) and the receive did not run.
#[test]
fn cancel_while_the_send_blocks() {
    run(|s: UnixStream, mut peer| async move {
        small_buffers(&s, &peer, 16 * 1024);
        let len = 1024 * 1024;
        let mut fut = s.send_then_recv(payload(len), Vec::with_capacity(64), MsgFlags::EMPTY);
        assert!(poll_once(&mut fut).is_pending());
        sleep(Duration::from_millis(30)).await;
        fut.cancel();
        let (send, recv, _, buf) = fut.await;
        let n = send.unwrap();
        assert!(n > 0 && n < len, "sent {n} of {len}");
        assert_eq!(os_error(&recv), Some(libc::ECANCELED), "{recv:?}");
        assert!(buf.is_empty());
        // Exactly those bytes are at the peer.
        peer.set_nonblocking(true).unwrap();
        let mut got = Vec::new();
        let mut chunk = [0u8; 65536];
        loop {
            match peer.read(&mut chunk) {
                Ok(0) => break,
                Ok(k) => got.extend_from_slice(&chunk[..k]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(got.len(), n);
        assert_eq!(got, payload(len)[..n]);
    });
}

/// Dropping the future in flight keeps both buffers (and the fd guards) in the op slot until
/// the pair's CQE; then they are dropped and nothing leaks. The stream counts a send and a
/// receive in flight until then.
#[test]
fn drop_in_flight_keeps_buffers_until_the_cqe() {
    run(|s: UnixStream, mut peer| async move {
        #[cfg(feature = "metrics")]
        monoio::reset_driver_stats();
        let fut = s.send_then_recv(payload(70), Vec::with_capacity(64), MsgFlags::EMPTY);
        assert!(would_block(s.try_send(b"x")));
        assert!(would_block(s.try_recv(&mut [0u8; 4])));
        // Let the runtime submit the pair; the send runs, the receive waits.
        sleep(Duration::from_millis(10)).await;
        let before = allocs();
        drop(fut);
        // The op data (56 bytes) is above the slot's 48 bytes of inline storage: boxed.
        assert_eq!(allocs() - before, 1, "the dropped data is boxed, once");
        // The guards are still held by the slot: the pair is in flight.
        assert!(would_block(s.try_send(b"x")));
        let mut got = [0u8; 70];
        peer.read_exact(&mut got).unwrap();
        // The receive is cancelled (`async-cancel`) or completes with the peer's data; either
        // way the one CQE frees the slot and drops the data.
        peer.write_all(b"data").unwrap();
        eventually("slot freed after drop", || !would_block(s.try_send(b"x"))).await;
        #[cfg(feature = "metrics")]
        {
            let d = monoio::driver_stats();
            assert_eq!(d.ignored_boxed, 1, "{d:?}");
            assert_eq!(d.ignored_inline, 0, "{d:?}");
            #[cfg(feature = "async-cancel")]
            assert_eq!(d.cancels_pushed, 2, "one cancel per member: {d:?}");
        }
    });
}

// ---- the `-EAGAIN` path ----

/// A reply far larger than the socket buffer moves the send to an io-wq worker after 127 poll
/// re-arms; the linked `RECV(POLL_FIRST)` then posts `-EAGAIN` ("send complete, receive not
/// started"). The future hides it with a plain receive: the caller sees a normal result.
#[test]
fn eagain_after_iowq_send_ends_in_a_normal_receive() {
    run(|s: UnixStream, peer| async move {
        small_buffers(&s, &peer, 4096);
        #[cfg(feature = "metrics")]
        monoio::reset_driver_stats();
        let len = 1024 * 1024;
        let want = payload(len);
        let peer = thread::spawn(move || {
            let mut peer = peer;
            let mut got = vec![0u8; len];
            peer.read_exact(&mut got).unwrap();
            assert_eq!(got, want);
            // Only after the whole reply: the receive must wait for this.
            thread::sleep(Duration::from_millis(50));
            peer.write_all(b"done").unwrap();
        });
        let (send, recv, _, buf) = s
            .send_then_recv(payload(len), Vec::with_capacity(16), MsgFlags::EMPTY)
            .await;
        assert_eq!(send.unwrap(), len);
        assert_eq!(recv.unwrap(), 4);
        assert_eq!(&buf[..], b"done");
        peer.join().unwrap();
        #[cfg(feature = "metrics")]
        {
            use monoio::uring::opcode;
            let d = monoio::driver_stats();
            let recvs = d.ops_by_opcode[opcode::Recv::CODE as usize];
            eprintln!("RECV SQEs for the pair: {recvs} (2 = the -EAGAIN path was taken)");
            assert!(recvs >= 1);
        }
    });
}

// ---- registered buffers ----

#[test]
fn fixed_receive_buffer() {
    run(|s: TcpStream, peer| async move {
        let pool = match FixedBufPool::new(2, 4096) {
            Ok(pool) => pool,
            Err(e) => {
                eprintln!("skipped: registered buffers unsupported: {e}");
                return;
            }
        };
        let peer = peer_echo(peer, 70, b"fixed".to_vec());
        let buf = pool.get().unwrap();
        let (send, recv, _, buf) = s.send_then_recv_fixed(payload(70), buf).await;
        assert_eq!(send.unwrap(), 70);
        assert_eq!(recv.unwrap(), 5);
        assert_eq!(&buf[..], b"fixed");
        peer.join().unwrap();
        #[cfg(feature = "metrics")]
        {
            use monoio::uring::opcode;
            let d = monoio::driver_stats();
            assert!(
                d.ops_by_opcode[opcode::ReadFixed::CODE as usize] >= 1,
                "{d:?}"
            );
        }
        drop(buf);
        assert_eq!(pool.stats().in_use, 0);
    });
}

// ---- direct-only streams ----

/// A direct-only stream (no fd, a fixed-file slot) works: the SQEs name the slot.
#[test]
fn direct_only_stream() {
    let mut rt = match RuntimeBuilder::<IoUringDriver>::new()
        .with_fixed_files(8)
        .enable_timer()
        .build()
    {
        Ok(rt) => rt,
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
            eprintln!("skipped: fixed file table unsupported: {e}");
            return;
        }
        Err(e) => panic!("{e}"),
    };
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept_direct());
        let client = client.unwrap();
        let (server, _) = match accepted {
            Ok(accepted) => accepted,
            Err(e) if e.kind() == io::ErrorKind::Unsupported => {
                eprintln!("skipped: accept_direct unsupported: {e}");
                return;
            }
            Err(e) => panic!("{e}"),
        };
        assert!(server.is_direct());
        let pair = server.send_then_recv(payload(70), Vec::with_capacity(16), MsgFlags::EMPTY);
        let (res, got) = client.recv_op(Vec::with_capacity(128)).await;
        assert_eq!(res.unwrap(), 70);
        assert_eq!(got, payload(70));
        assert_eq!(client.try_send(b"back").unwrap(), 4);
        let (send, recv, _, buf) = pair.await;
        assert_eq!(send.unwrap(), 70);
        assert_eq!(recv.unwrap(), 4);
        assert_eq!(&buf[..], b"back");
    });
}

// ---- coop budget ----

/// With `coop_budget(Some(1))` a pair is one result: a task draining several ready pairs
/// yields after each one, and every pair still resolves.
#[test]
fn coop_budget_counts_the_pair_once() {
    use std::{cell::RefCell, rc::Rc};

    use monoio::task::yield_now;

    const PAIRS: usize = 5;
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .coop_budget(Some(1))
        .build()
        .unwrap();
    rt.block_on(async {
        #[cfg(feature = "metrics")]
        monoio::reset_runtime_stats();
        let log: Rc<RefCell<Vec<char>>> = Default::default();
        let done = Rc::new(Cell::new(false));
        let other = {
            let (log, done) = (log.clone(), done.clone());
            monoio::spawn(async move {
                while !done.get() {
                    log.borrow_mut().push('b');
                    yield_now().await;
                }
            })
        };
        let (l, d) = (log.clone(), done.clone());
        let a = monoio::spawn(async move {
            // The replies wait in the sockets already: every pair completes in one tick.
            let pairs: Vec<_> = (0..PAIRS)
                .map(|i| {
                    let (a, mut b) = std::os::unix::net::UnixStream::pair().unwrap();
                    b.write_all(&[i as u8; 4]).unwrap();
                    let s = UnixStream::from_std(a).unwrap();
                    let fut =
                        s.send_then_recv(b"hi".to_vec(), Vec::with_capacity(8), MsgFlags::EMPTY);
                    (s, b, fut)
                })
                .collect();
            for (i, (_s, _b, fut)) in pairs.into_iter().enumerate() {
                let (send, recv, _, buf) = fut.await;
                assert_eq!(send.unwrap(), 2);
                assert_eq!(recv.unwrap(), 4);
                assert_eq!(&buf[..], &[i as u8; 4]);
                l.borrow_mut().push('a');
            }
            d.set(true);
        });
        a.await;
        other.await;
        let v = log.borrow().clone();
        assert_eq!(v.iter().filter(|&&c| c == 'a').count(), PAIRS);
        let longest_a = v.split(|&c| c != 'a').map(|r| r.len()).max().unwrap_or(0);
        assert!(longest_a <= 1, "a pair is one unit of budget: {v:?}");
        #[cfg(feature = "metrics")]
        assert!(monoio::runtime_stats().coop_yields > 0);
    });
}

// ---- guards ----

/// While a pair is in flight the stream counts a send and a receive, so the direct calls are
/// refused; once the pair resolved they work again.
#[test]
fn try_send_and_try_recv_refused_while_a_pair_is_in_flight() {
    run(|s: TcpStream, mut peer| async move {
        let mut fut = s.send_then_recv(payload(70), Vec::with_capacity(64), MsgFlags::EMPTY);
        assert!(would_block(s.try_send(b"x")));
        assert!(would_block(s.try_recv(&mut [0u8; 4])));
        assert!(poll_once(&mut fut).is_pending());
        sleep(Duration::from_millis(10)).await;
        let mut got = [0u8; 70];
        peer.read_exact(&mut got).unwrap();
        // The socket has room and data is queued, but the pair is still in flight.
        peer.write_all(b"data").unwrap();
        sleep(Duration::from_millis(20)).await;
        assert!(would_block(s.try_send(b"x")));
        assert!(would_block(s.try_recv(&mut [0u8; 4])));
        let (send, recv, _, buf) = fut.await;
        assert_eq!(send.unwrap(), 70);
        assert_eq!(recv.unwrap(), 4);
        assert_eq!(&buf[..], b"data");
        assert_eq!(s.try_send(b"x").unwrap(), 1);
        assert!(would_block(s.try_recv(&mut [0u8; 4])));
    });
}

// ---- the cost per frame (`metrics`) ----

/// A request/response loop of `N` frames on `send_then_recv` costs one task poll, one CQE
/// and two SQEs per frame.
#[cfg(feature = "metrics")]
#[test]
fn one_poll_and_one_cqe_per_frame() {
    const N: usize = 2000;
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        let (a, mut peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let s = UnixStream::from_std(a).unwrap();
        // The client: waits for each reply, sends the next request.
        peer.write_all(b"req").unwrap();
        let client = thread::spawn(move || {
            for _ in 0..N {
                let mut got = [0u8; 70];
                peer.read_exact(&mut got).unwrap();
                peer.write_all(b"req").unwrap();
            }
        });
        // One request is in the socket already: read it, then N pairs.
        let (res, buf) = s.recv_op(Vec::with_capacity(16)).await;
        assert_eq!(res.unwrap(), 3);
        let server = monoio::spawn(async move {
            monoio::reset_runtime_stats();
            monoio::reset_driver_stats();
            let (mut reply, mut buf) = (payload(70), buf);
            for _ in 0..N {
                buf.clear();
                let (send, recv, r, b) = s.send_then_recv(reply, buf, MsgFlags::EMPTY).await;
                assert_eq!(send.unwrap(), 70);
                assert_eq!(recv.unwrap(), 3);
                (reply, buf) = (r, b);
            }
            (monoio::runtime_stats(), monoio::driver_stats())
        });
        let (r, d) = server.await;
        client.join().unwrap();
        let polls = r.tasks_polled as f64 / N as f64;
        let cqes = d.cqes_reaped as f64 / N as f64;
        let sqes = d.sqes_submitted as f64 / N as f64;
        eprintln!("per frame: task polls {polls:.3}, CQEs {cqes:.3}, SQEs {sqes:.3}");
        assert!(polls <= 1.1, "{polls} task polls per frame: {r:?}");
        assert!(cqes <= 1.1, "{cqes} CQEs per frame: {d:?}");
        assert!((1.9..=2.1).contains(&sqes), "{sqes} SQEs per frame: {d:?}");
        assert_eq!(
            d.ops_by_opcode[monoio::uring::opcode::Send::CODE as usize],
            N as u64
        );
        assert_eq!(
            d.ops_by_opcode[monoio::uring::opcode::Recv::CODE as usize],
            N as u64
        );
    });
}
