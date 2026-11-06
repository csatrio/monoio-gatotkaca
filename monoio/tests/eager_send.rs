//! Eager sends (S1): `try_send`, `try_send_vectored`, `try_recv`, `send_eager` and the
//! per-stream `set_eager_send` switch, on `TcpStream`, `UnixStream` and their owned halves.
//!
//! The point of most tests is ordering: a direct syscall must never overtake a ring op that is
//! still in flight on the same socket, and that also holds for an op whose future was dropped.

use std::{
    future::Future,
    io::{self, IoSlice},
    os::fd::{AsRawFd, RawFd},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use monoio::{
    buf::VecBuf,
    io::{
        futures::{RecvFuture, SendFuture},
        AsyncWriteRent, AsyncWriteRentExt, Splitable,
    },
    net::{MsgFlags, TcpListener, TcpStream, UnixStream},
    time::sleep,
    uring::libc,
    IoUringDriver, RuntimeBuilder,
};

// ---- helpers ----

/// What the tests use of a stream, so that each runs for TCP and for Unix sockets.
trait Stream: AsRawFd + AsyncWriteRent + Sized + 'static {
    async fn pair() -> (Self, Self);
    fn try_send(&self, buf: &[u8]) -> io::Result<usize>;
    fn try_send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize>;
    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize>;
    fn send_op(&self, buf: Vec<u8>) -> SendFuture<Vec<u8>>;
    fn send_eager(&self, buf: Vec<u8>) -> SendFuture<Vec<u8>>;
    fn send_eager_with_flags(&self, buf: Vec<u8>, flags: MsgFlags) -> SendFuture<Vec<u8>>;
    fn recv_op(&self, buf: Vec<u8>) -> RecvFuture<Vec<u8>>;
    fn set_eager_send(&self, on: bool);
    fn eager_send(&self) -> bool;
}

macro_rules! impl_stream {
    ($ty:ty, $pair:expr) => {
        impl Stream for $ty {
            async fn pair() -> (Self, Self) {
                $pair.await
            }
            fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
                <$ty>::try_send(self, buf)
            }
            fn try_send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
                <$ty>::try_send_vectored(self, bufs)
            }
            fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
                <$ty>::try_recv(self, buf)
            }
            fn send_op(&self, buf: Vec<u8>) -> SendFuture<Vec<u8>> {
                <$ty>::send_op(self, buf)
            }
            fn send_eager(&self, buf: Vec<u8>) -> SendFuture<Vec<u8>> {
                <$ty>::send_eager(self, buf)
            }
            fn send_eager_with_flags(&self, buf: Vec<u8>, flags: MsgFlags) -> SendFuture<Vec<u8>> {
                <$ty>::send_eager_with_flags(self, buf, flags)
            }
            fn recv_op(&self, buf: Vec<u8>) -> RecvFuture<Vec<u8>> {
                <$ty>::recv_op(self, buf)
            }
            fn set_eager_send(&self, on: bool) {
                <$ty>::set_eager_send(self, on)
            }
            fn eager_send(&self) -> bool {
                <$ty>::eager_send(self)
            }
        }
    };
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}

impl_stream!(TcpStream, tcp_pair());
impl_stream!(UnixStream, async { UnixStream::pair().unwrap() });

/// Poll a future once with a waker that does nothing.
fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    Pin::new(fut).poll(&mut Context::from_waker(Waker::noop()))
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

/// Small socket buffers on both ends (16 KiB each; with less, TCP over loopback stalls in its
/// window probes), so that a few dozen KiB fill the connection.
fn small_buffers(tx: &impl AsRawFd, rx: &impl AsRawFd) {
    set_buf(tx.as_raw_fd(), libc::SO_SNDBUF, 16384);
    set_buf(rx.as_raw_fd(), libc::SO_RCVBUF, 16384);
}

/// Take everything that is queued on `fd` right now, with plain syscalls (no ring).
fn drain(fd: RawFd, into: &mut Vec<u8>) -> usize {
    let mut buf = [0u8; 16 * 1024];
    let mut total = 0;
    loop {
        // SAFETY: `buf` is valid for writes of its length.
        let n = unsafe { libc::recv(fd, buf.as_mut_ptr().cast(), buf.len(), libc::MSG_DONTWAIT) };
        if n <= 0 {
            return total;
        }
        into.extend_from_slice(&buf[..n as usize]);
        total += n as usize;
    }
}

/// Whether `poll(2)` reports room in the socket's send buffer.
fn writable(fd: RawFd) -> bool {
    let mut p = libc::pollfd {
        fd,
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: one valid pollfd, no timeout.
    unsafe { libc::poll(&mut p, 1, 0) == 1 && p.revents & libc::POLLOUT != 0 }
}

/// Wait, in a plain `poll(2)` (the runtime does not run meanwhile), until data is queued on
/// `fd`.
fn wait_readable(fd: RawFd) {
    let mut p = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one valid pollfd.
    let n = unsafe { libc::poll(&mut p, 1, 10_000) };
    assert!(n == 1 && p.revents & libc::POLLIN != 0, "no data arrived");
}

/// `try_send` until the kernel reports a full socket buffer. Returns the bytes it took.
fn fill<S: Stream>(s: &S) -> usize {
    let chunk = [7u8; 1024];
    let mut filled = 0;
    loop {
        match s.try_send(&chunk) {
            Ok(n) => filled += n,
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::WouldBlock, "{e}");
                assert!(filled > 0);
                return filled;
            }
        }
    }
}

/// Run `cond` every millisecond (the runtime runs in between) until it holds.
async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(1)).await;
    }
}

/// Receive on `s` through the ring until `n` bytes arrived.
async fn recv_n<S: Stream>(s: &S, n: usize) -> Vec<u8> {
    let mut got = Vec::with_capacity(n);
    while got.len() < n {
        let (res, buf) = s.recv_op(Vec::with_capacity(n - got.len())).await;
        assert!(res.unwrap() > 0, "EOF after {} of {n} bytes", got.len());
        got.extend_from_slice(&buf);
    }
    got
}

// ---- ordering against a ring send in flight ----

/// A ring send waits on a full socket buffer. Until it completed (or, when its future was
/// dropped, until the kernel finished it), `try_send` is refused although the socket has room
/// again: the bytes would overtake the ring send's.
async fn ring_send_in_flight<S: Stream>(drop_it: bool) {
    let (a, b) = S::pair().await;
    small_buffers(&a, &b);
    let filled = fill(&a);

    let mut fut = a.send_op(vec![9u8; 512]);
    // The SQE is only queued, the kernel has not seen it: it counts already.
    assert!(would_block(a.try_send(b"x")));
    sleep(Duration::from_millis(20)).await;
    assert!(
        poll_once(&mut fut).is_pending(),
        "a send on a full socket must wait"
    );
    assert!(would_block(a.try_send(b"x")));
    let mut fut = Some(fut);
    if drop_it {
        fut = None;
        assert!(would_block(a.try_send(b"x")));
    }

    // The peer takes everything, with plain syscalls: the runtime does not run meanwhile, so
    // it cannot have seen the ring send complete.
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < filled || !writable(a.as_raw_fd()) {
        drain(b.as_raw_fd(), &mut got);
        assert!(Instant::now() < deadline, "the socket never drained");
        std::thread::yield_now();
    }
    // There is room now, and still no direct send: the ring send is not known to be done.
    assert!(writable(a.as_raw_fd()));
    assert!(would_block(a.try_send(b"x")));

    match fut {
        Some(fut) => {
            let (res, _) = fut.await;
            assert_eq!(res.unwrap(), 512);
            assert_eq!(a.try_send(b"x").unwrap(), 1);
        }
        // Dropped: it works again once the driver reaped the op's CQE (the send's result, or
        // `ECANCELED` with `async-cancel`), and not before.
        None => {
            eventually("try_send after a dropped ring send", || {
                a.try_send(b"x").is_ok()
            })
            .await
        }
    }
}

#[monoio::test(timer_enabled = true)]
async fn try_send_waits_for_ring_send_tcp() {
    ring_send_in_flight::<TcpStream>(false).await;
}

#[monoio::test(timer_enabled = true)]
async fn try_send_waits_for_ring_send_unix() {
    ring_send_in_flight::<UnixStream>(false).await;
}

#[monoio::test(timer_enabled = true)]
async fn try_send_waits_for_dropped_ring_send_tcp() {
    ring_send_in_flight::<TcpStream>(true).await;
}

#[monoio::test(timer_enabled = true)]
async fn try_send_waits_for_dropped_ring_send_unix() {
    ring_send_in_flight::<UnixStream>(true).await;
}

/// `send_eager` and the vectored `try_send` are refused in the same way: with a ring send in
/// flight `send_eager` queues behind it instead of sending directly.
async fn eager_queues_behind_ring_send<S: Stream>() {
    let (a, b) = S::pair().await;
    let first = a.send_op(b"first ".to_vec());
    assert!(would_block(
        a.try_send_vectored(&[IoSlice::new(b"no"), IoSlice::new(b"pe")])
    ));
    let mut second = a.send_eager(b"second".to_vec());
    assert!(
        poll_once(&mut second).is_pending(),
        "send_eager must use the ring while a ring send is in flight"
    );
    assert_eq!(first.await.0.unwrap(), 6);
    assert_eq!(second.await.0.unwrap(), 6);
    assert_eq!(recv_n(&b, 12).await, b"first second");
}

#[monoio::test(timer_enabled = true)]
async fn send_eager_queues_behind_ring_send() {
    eager_queues_behind_ring_send::<TcpStream>().await;
    eager_queues_behind_ring_send::<UnixStream>().await;
}

// ---- the fast path ----

/// A `send_eager` that fits is done before its future is polled: the first poll is `Ready`,
/// and the bytes are at the peer without the runtime having run.
async fn eager_ready_at_first_poll<S: Stream>() {
    let (a, b) = S::pair().await;
    let mut fut = a.send_eager(b"hello".to_vec());
    // Already there, before any poll.
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < 5 {
        drain(b.as_raw_fd(), &mut got);
        assert!(Instant::now() < deadline);
    }
    assert_eq!(got, b"hello");
    match poll_once(&mut fut) {
        Poll::Ready((res, buf)) => {
            assert_eq!(res.unwrap(), 5);
            assert_eq!(buf, b"hello");
        }
        Poll::Pending => panic!("a send_eager that fits must be ready at its first poll"),
    }
    assert!(fut.is_terminated());

    // An empty buffer: like a plain send of an empty buffer.
    let mut fut = a.send_eager(Vec::new());
    assert!(matches!(poll_once(&mut fut), Poll::Ready((Ok(0), _))));
    assert_eq!(a.send_op(Vec::new()).await.0.unwrap(), 0);

    // Flags are passed on.
    let mut fut = a.send_eager_with_flags(b"more".to_vec(), MsgFlags::MORE);
    assert!(matches!(poll_once(&mut fut), Poll::Ready((Ok(4), _))));
    assert_eq!(a.try_send(b"!").unwrap(), 1);
    assert_eq!(recv_n(&b, 5).await, b"more!");
}

#[monoio::test(timer_enabled = true)]
async fn send_eager_that_fits_is_ready() {
    eager_ready_at_first_poll::<TcpStream>().await;
    eager_ready_at_first_poll::<UnixStream>().await;
}

/// `try_send`, `try_send_vectored` and `try_recv` move bytes without the runtime.
async fn direct_calls<S: Stream>() {
    let (a, b) = S::pair().await;
    let mut buf = [0u8; 32];
    assert!(would_block(b.try_recv(&mut buf)), "nothing was sent yet");

    assert_eq!(a.try_send(b"ab").unwrap(), 2);
    assert_eq!(
        a.try_send_vectored(&[IoSlice::new(b"cd"), IoSlice::new(b""), IoSlice::new(b"ef")])
            .unwrap(),
        4
    );
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < 6 {
        match b.try_recv(&mut buf) {
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::WouldBlock),
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(got, b"abcdef");

    // EOF is `Ok(0)`.
    drop(a);
    eventually("EOF", || matches!(b.try_recv(&mut buf), Ok(0))).await;
}

#[monoio::test(timer_enabled = true)]
async fn try_calls_bypass_the_ring() {
    direct_calls::<TcpStream>().await;
    direct_calls::<UnixStream>().await;
}

/// A real error is returned by the first poll, as an error (no `SIGPIPE`).
#[monoio::test(timer_enabled = true)]
async fn send_eager_reports_errors() {
    let (a, b) = tcp_pair().await;
    drop(b);
    let mut last = None;
    for _ in 0..200 {
        let mut fut = a.send_eager(b"into the void".to_vec());
        match poll_once(&mut fut) {
            Poll::Ready((Err(e), buf)) => {
                assert_eq!(buf, b"into the void");
                last = Some(e.kind());
                break;
            }
            // The first sends after the peer closed still succeed.
            Poll::Ready((Ok(_), _)) => sleep(Duration::from_millis(1)).await,
            Poll::Pending => panic!("nothing can make this send wait"),
        }
    }
    assert!(
        matches!(
            last,
            Some(io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset)
        ),
        "{last:?}"
    );
    assert!(a.try_send(b"x").is_err());
}

// ---- partial eager send, the ring for the rest ----

/// `buf` is far larger than the socket buffer: the direct send takes the start, the ring send
/// the rest, and the result counts from the start of the buffer. `MSG_WAITALL` makes the ring
/// part send everything, so the result is the whole length.
async fn partial_then_ring<S: Stream>() {
    const LEN: usize = 256 * 1024;
    let (a, b) = S::pair().await;
    small_buffers(&a, &b);
    let data: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();

    let mut fut = a.send_eager_with_flags(data.clone(), MsgFlags::WAITALL);
    assert!(
        poll_once(&mut fut).is_pending(),
        "256 KiB cannot fit a 16 KiB socket buffer"
    );
    // The direct part is at the peer already; the runtime did not run yet.
    let mut head = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while head.is_empty() {
        drain(b.as_raw_fd(), &mut head);
        assert!(Instant::now() < deadline, "the direct send took nothing");
    }
    assert!(would_block(a.try_send(b"x")), "the rest is in flight");

    let reader = monoio::spawn(async move {
        let rest = recv_n(&b, LEN - head.len()).await;
        head.extend_from_slice(&rest);
        head
    });
    let (res, buf) = fut.await;
    assert_eq!(
        res.unwrap(),
        LEN,
        "the count is relative to the whole buffer"
    );
    assert_eq!(buf.len(), LEN);
    assert!(reader.await == data, "bytes lost, repeated or out of order");
    assert_eq!(a.try_send(b"x").unwrap(), 1);
}

#[monoio::test(timer_enabled = true)]
async fn partial_eager_send_returns_whole_length() {
    partial_then_ring::<TcpStream>().await;
    partial_then_ring::<UnixStream>().await;
}

/// The ring part is cancelled after the direct part sent something: the result is the short
/// count of the direct part, not an error, and exactly those bytes are at the peer.
#[monoio::test(timer_enabled = true)]
async fn cancelled_rest_is_a_short_send() {
    const LEN: usize = 256 * 1024;
    let (mut a, b) = UnixStream::pair().unwrap();
    small_buffers(&a, &b);
    let data: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();

    let mut fut = a.send_eager(data.clone());
    assert!(poll_once(&mut fut).is_pending());
    sleep(Duration::from_millis(10)).await;
    fut.cancel();
    let (res, _) = fut.await;
    let sent = res.expect("what the direct send took is reported, not ECANCELED");
    assert!(sent > 0 && sent < LEN, "{sent}");

    a.shutdown().await.unwrap();
    let mut got = Vec::new();
    loop {
        let (res, buf) = b.recv_op(Vec::with_capacity(64 * 1024)).await;
        if res.unwrap() == 0 {
            break;
        }
        got.extend_from_slice(&buf);
    }
    assert!(
        got == data[..sent],
        "{} bytes at the peer, {sent} reported",
        got.len()
    );
}

// ---- byte order with all send paths mixed ----

/// Chunk `i`: its number, its length, and a payload that depends on both.
fn chunk(i: u32, len: usize) -> Vec<u8> {
    assert!(len >= 8);
    let mut c = Vec::with_capacity(len);
    c.extend_from_slice(&i.to_le_bytes());
    c.extend_from_slice(&(len as u32).to_le_bytes());
    c.extend((8..len).map(|j| (i as usize + j) as u8));
    c
}

/// 1 MiB of numbered chunks through a small socket buffer to a slow reader, each chunk by a
/// different path: `send_eager`, a ring send, `try_send` with a ring fallback, and `write_all`
/// with the per-stream switch. The reader must see every chunk whole and in order.
async fn mixed_sends_keep_order<S: Stream>() {
    const TOTAL: usize = 1 << 20;
    const SIZES: [usize; 8] = [70, 4096, 300, 16 * 1024, 1000, 64 * 1024, 8, 2500];
    let (mut a, b) = S::pair().await;
    small_buffers(&a, &b);

    let reader = monoio::spawn(async move {
        let mut got = Vec::with_capacity(TOTAL + 64 * 1024);
        let mut reads = 0u32;
        loop {
            let (res, buf) = b.recv_op(Vec::with_capacity(3000)).await;
            if res.unwrap() == 0 {
                return got;
            }
            got.extend_from_slice(&buf);
            reads += 1;
            // A slow reader: a pause after every eighth read.
            if reads & 7 == 0 {
                sleep(Duration::from_micros(300)).await;
            }
        }
    });

    let (mut ready, mut pending, mut refused, mut direct) = (0u32, 0u32, 0u32, 0u32);
    let mut sent = 0;
    let mut i = 0u32;
    while sent < TOTAL {
        let len = SIZES[i as usize % SIZES.len()];
        let data = chunk(i, len);
        let mut off = 0;
        match i % 5 {
            // `send_eager`; a short count goes around again.
            0 | 1 => {
                while off < len {
                    let mut fut = a.send_eager(data[off..].to_vec());
                    let (res, _) = match poll_once(&mut fut) {
                        Poll::Ready(out) => {
                            ready += 1;
                            out
                        }
                        Poll::Pending => {
                            pending += 1;
                            fut.await
                        }
                    };
                    off += res.unwrap();
                }
            }
            // A ring send. While it is in flight a direct send would overtake it.
            2 => {
                while off < len {
                    let fut = a.send_op(data[off..].to_vec());
                    assert!(would_block(a.try_send(b"overtake")));
                    refused += 1;
                    off += fut.await.0.unwrap();
                }
            }
            // `try_send`, and the ring when the socket is full.
            3 => {
                while off < len {
                    match a.try_send(&data[off..]) {
                        Ok(n) => {
                            direct += 1;
                            off += n;
                        }
                        Err(e) => {
                            assert_eq!(e.kind(), io::ErrorKind::WouldBlock, "{e}");
                            off += a.send_op(data[off..].to_vec()).await.0.unwrap();
                        }
                    }
                }
            }
            // `write_all` with the switch on.
            _ => {
                a.set_eager_send(true);
                let (res, _) = a.write_all(data).await;
                assert_eq!(res.unwrap(), len);
                a.set_eager_send(false);
            }
        }
        sent += len;
        i += 1;
    }
    a.shutdown().await.unwrap();
    let got = reader.await;

    assert_eq!(got.len(), sent);
    let mut pos = 0;
    for n in 0..i {
        let len = SIZES[n as usize % SIZES.len()];
        assert!(
            got[pos..pos + len] == chunk(n, len)[..],
            "chunk {n} (at byte {pos}) is damaged or out of order"
        );
        pos += len;
    }
    assert_eq!(pos, got.len());
    // Every path was taken.
    assert!(ready > 0, "no send_eager was ready at once");
    assert!(pending > 0, "no send_eager needed the ring");
    assert!(refused > 0 && direct > 0);
}

#[monoio::test(timer_enabled = true)]
async fn mixed_sends_keep_order_tcp() {
    mixed_sends_keep_order::<TcpStream>().await;
}

#[monoio::test(timer_enabled = true)]
async fn mixed_sends_keep_order_unix() {
    mixed_sends_keep_order::<UnixStream>().await;
}

// ---- the per-stream switch ----

/// The first poll of a future tells the two paths apart: a ring op is never ready at its
/// first poll, an eager send that fits always is.
fn first_poll_ready<F: Future>(fut: F) -> bool {
    let mut fut = std::pin::pin!(fut);
    fut.as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_ready()
}

async fn switch<S: Stream + monoio::io::Split>() {
    let (mut a, b) = S::pair().await;
    assert!(!a.eager_send(), "off by default");
    assert!(!first_poll_ready(a.write_all(b"ring".to_vec())));
    assert!(!first_poll_ready(a.write(b"ring".to_vec())));

    a.set_eager_send(true);
    assert!(a.eager_send());
    // The two dropped ring writes above are still in flight: the eager path must wait for them.
    eventually("the dropped writes finished", || a.try_send(b"").is_ok()).await;
    assert!(first_poll_ready(a.write_all(b"eager1".to_vec())));
    assert!(first_poll_ready(a.write(b"eager2".to_vec())));
    let iov: VecBuf = vec![b"eag".to_vec(), b"er3".to_vec()].into();
    assert!(first_poll_ready(a.writev(iov)));
    let iov: VecBuf = vec![b"eag".to_vec(), b"er4".to_vec()].into();
    assert!(first_poll_ready(a.write_vectored_all(iov)));

    // The halves share the switch.
    let (r, mut w) = a.into_split();
    assert!(first_poll_ready(w.write_all(b"eager5".to_vec())));
    assert!(first_poll_ready(w.write(b"eager6".to_vec())));
    let a = r
        .reunite(w)
        .unwrap_or_else(|_| panic!("halves of one stream"));
    assert!(a.eager_send());
    a.set_eager_send(false);
    let (r, mut w) = a.into_split();
    assert!(!first_poll_ready(w.write_all(b"ring".to_vec())));
    let (res, _) = w.write_all(b"ring".to_vec()).await;
    assert_eq!(res.unwrap(), 4);
    drop((r, w));

    // Everything written by a future that was polled arrived, in order. (The dropped ring
    // writes may or may not have been sent before their cancel.)
    let mut got = Vec::new();
    loop {
        let (res, buf) = b.recv_op(Vec::with_capacity(256)).await;
        if res.unwrap() == 0 {
            break;
        }
        got.extend_from_slice(&buf);
    }
    let text = String::from_utf8(got).unwrap().replace("ring", "");
    assert_eq!(text, "eager1eager2eager3eager4eager5eager6");
}

#[monoio::test(timer_enabled = true)]
async fn set_eager_send_switches_write_all() {
    switch::<TcpStream>().await;
    switch::<UnixStream>().await;
}

/// The owned halves have the calls themselves.
#[monoio::test(timer_enabled = true)]
async fn owned_halves() {
    let (a, b) = tcp_pair().await;
    let b_fd = b.as_raw_fd();
    let (ar, aw) = a.into_split();
    let (br, bw) = b.into_split();

    assert!(!aw.eager_send());
    aw.set_eager_send(true);
    assert!(aw.eager_send());
    assert_eq!(aw.try_send(b"ab").unwrap(), 2);
    assert_eq!(aw.try_send_vectored(&[IoSlice::new(b"cd")]).unwrap(), 2);
    let mut fut = aw.send_eager(b"ef".to_vec());
    assert!(matches!(poll_once(&mut fut), Poll::Ready((Ok(2), _))));
    let mut fut = aw.send_eager_with_flags(b"gh".to_vec(), MsgFlags::EMPTY);
    assert!(matches!(poll_once(&mut fut), Poll::Ready((Ok(2), _))));

    let mut buf = [0u8; 16];
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < 8 {
        if let Ok(n) = br.try_recv(&mut buf) {
            got.extend_from_slice(&buf[..n]);
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(got, b"abcdefgh");

    // A ring receive of the read half blocks the direct receive (the data is queued already,
    // the receive is not even submitted), and not the direct send of the write half.
    assert_eq!(aw.try_send(b"data").unwrap(), 4);
    wait_readable(b_fd);
    let recv = br.recv_op(Vec::with_capacity(8));
    assert!(would_block(br.try_recv(&mut buf)));
    assert_eq!(bw.try_send(b"ok").unwrap(), 2);
    let (res, data) = recv.await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(data, b"data");
    drop(ar);
}

// ---- receives ----

/// While a ring receive is in flight, `try_recv` is refused even though data is queued: the
/// ring receive gets it.
async fn try_recv_waits_for_ring_recv<S: Stream>() {
    let (a, b) = S::pair().await;
    let mut buf = [0u8; 16];

    // The data is queued before the ring receive is even submitted to the kernel: only the
    // count of ops in flight keeps `try_recv` from taking it (the ring receive would then
    // wait for ever).
    assert_eq!(a.try_send(b"abc").unwrap(), 3);
    wait_readable(b.as_raw_fd());
    let recv = b.recv_op(Vec::with_capacity(16));
    assert!(
        would_block(b.try_recv(&mut buf)),
        "the ring receive owns it"
    );
    let (res, data) = recv.await;
    assert_eq!(res.unwrap(), 3);
    assert_eq!(data, b"abc");

    // The same once the receive waits in the kernel.
    let mut recv = b.recv_op(Vec::with_capacity(16));
    sleep(Duration::from_millis(10)).await;
    assert!(poll_once(&mut recv).is_pending());
    assert!(would_block(b.try_recv(&mut buf)));
    assert_eq!(a.try_send(b"def").unwrap(), 3);
    let (res, data) = recv.await;
    assert_eq!(res.unwrap(), 3);
    assert_eq!(data, b"def");

    // Free again.
    assert_eq!(a.try_send(b"ghi").unwrap(), 3);
    wait_readable(b.as_raw_fd());
    assert_eq!(b.try_recv(&mut buf).unwrap(), 3);
    assert_eq!(&buf[..3], b"ghi");

    // A dropped receive counts until the kernel finished it: the queued byte is its, not
    // `try_recv`'s.
    assert_eq!(a.try_send(b"x").unwrap(), 1);
    wait_readable(b.as_raw_fd());
    drop(b.recv_op(Vec::with_capacity(16)));
    assert!(would_block(b.try_recv(&mut buf)));
    // A round of the runtime: the dropped receive completes (it takes the "x" with it, or its
    // cancel wins and the "x" stays), and the socket is free again.
    sleep(Duration::from_millis(20)).await;
    assert_eq!(a.try_send(b"jkl").unwrap(), 3);
    let mut got = Vec::new();
    eventually("try_recv after a dropped ring receive", || {
        if let Ok(n) = b.try_recv(&mut buf) {
            got.extend_from_slice(&buf[..n]);
        }
        got.ends_with(b"jkl")
    })
    .await;
    assert!(got == b"jkl" || got == b"xjkl", "{got:?}");
}

#[monoio::test(timer_enabled = true)]
async fn try_recv_waits_for_ring_recv_tcp() {
    try_recv_waits_for_ring_recv::<TcpStream>().await;
}

#[monoio::test(timer_enabled = true)]
async fn try_recv_waits_for_ring_recv_unix() {
    try_recv_waits_for_ring_recv::<UnixStream>().await;
}

/// The other ring ops of a stream count too: vectored, zero-copy and fixed-buffer sends, and
/// vectored receives.
#[monoio::test(timer_enabled = true)]
async fn other_ring_ops_count() {
    let (a, b) = tcp_pair().await;
    let mut buf = [0u8; 16];

    let iov: VecBuf = vec![b"wri".to_vec(), b"tev".to_vec()].into();
    let op = a.writev_op(iov);
    assert!(would_block(a.try_send(b"x")));
    assert_eq!(op.await.0.unwrap(), 6);
    assert_eq!(a.try_send(b"1").unwrap(), 1);

    {
        let iov: VecBuf = vec![b"send".to_vec(), b"msg".to_vec()].into();
        let op = std::pin::pin!(a.send_vectored(iov));
        // Not submitted before its first poll.
        let mut op = op;
        assert!(op
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert!(would_block(a.try_send(b"x")));
        assert_eq!(op.await.0.unwrap(), 7);
    }
    assert_eq!(a.try_send(b"2").unwrap(), 1);

    let op = a.send_zc_early(b"zerocopy".to_vec());
    assert!(would_block(a.try_send(b"x")));
    let (res, pending) = op.await;
    match res {
        Ok(n) => {
            assert_eq!(n, 8);
            // Counted until the kernel released the buffer.
            pending.await;
        }
        // No `SEND_ZC` on this kernel or socket.
        Err(e) => eprintln!("send_zc_early: {e}"),
    }
    eventually("try_send after a zero-copy send", || {
        a.try_send(b"3").is_ok()
    })
    .await;

    // Registered buffers: `WRITE_FIXED` / `READ_FIXED` on the socket.
    match monoio::uring::FixedBufPool::new(2, 64) {
        Ok(pool) => {
            let mut out = pool.get().unwrap();
            out.extend_from_slice(b"fixed");
            let op = a.send_fixed_op(out);
            assert!(would_block(a.try_send(b"x")));
            assert_eq!(op.await.0.unwrap(), 5);
            assert_eq!(a.try_send(b"4").unwrap(), 1);

            wait_readable(b.as_raw_fd());
            let op = b.recv_fixed_op(pool.get().unwrap());
            assert!(would_block(b.try_recv(&mut buf)));
            assert!(op.await.0.unwrap() > 0);
        }
        Err(e) => eprintln!("skipped the fixed-buffer part: {e}"),
    }

    // Data is queued; the vectored receive is pushed but not submitted yet.
    assert_eq!(a.try_send(b"tail").unwrap(), 4);
    wait_readable(b.as_raw_fd());
    let iov: VecBuf = vec![vec![0u8; 4], vec![0u8; 4]].into();
    let op = b.readv_op(iov);
    assert!(would_block(b.try_recv(&mut buf)));
    assert!(op.await.0.unwrap() > 0);
    drop(a);
    eventually("EOF", || {
        let mut sink = [0u8; 64];
        matches!(b.try_recv(&mut sink), Ok(0))
    })
    .await;
}

// ---- direct-only streams ----

/// A direct-only stream (`accept_direct`) has no fd: the direct calls are refused and
/// `send_eager` uses the ring.
#[test]
fn direct_only_stream_uses_the_ring() {
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
        let (mut server, _) = match accepted {
            Ok(accepted) => accepted,
            Err(e) if e.kind() == io::ErrorKind::Unsupported => {
                eprintln!("skipped: accept_direct unsupported: {e}");
                return;
            }
            Err(e) => panic!("{e}"),
        };
        assert!(server.is_direct());
        assert_eq!(server.as_raw_fd(), -1);

        assert!(would_block(server.try_send(b"x")));
        assert!(would_block(server.try_send_vectored(&[IoSlice::new(b"x")])));
        // Data is queued, but there is no fd to receive from.
        assert_eq!(client.try_send(b"ping").unwrap(), 4);
        sleep(Duration::from_millis(10)).await;
        let mut buf = [0u8; 16];
        assert!(would_block(server.try_recv(&mut buf)));
        let (res, data) = server.recv_op(Vec::with_capacity(16)).await;
        assert_eq!(res.unwrap(), 4);
        assert_eq!(data, b"ping");

        let mut fut = server.send_eager(b"pong".to_vec());
        assert!(
            poll_once(&mut fut).is_pending(),
            "a direct-only stream can only send through the ring"
        );
        assert_eq!(fut.await.0.unwrap(), 4);

        server.set_eager_send(true);
        let (res, _) = server.write_all(b" again".to_vec()).await;
        assert_eq!(res.unwrap(), 6);
        let iov: VecBuf = vec![b" and".to_vec(), b" again".to_vec()].into();
        let (res, _) = server.write_vectored_all(iov).await;
        assert_eq!(res.unwrap(), 10);
        assert_eq!(recv_n(&client, 20).await, b"pong again and again");
    });
}

// ---- no SQE on the fast path ----

#[cfg(feature = "metrics")]
#[monoio::test(timer_enabled = true)]
async fn eager_sends_that_fit_push_no_sqe() {
    use monoio::uring::opcode;

    const REPLY: &[u8; 70] =
        b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nx-ack: 00000000000000000000000\r\n\r\n";
    const N: usize = 100;
    let send = opcode::Send::CODE as usize;

    let (mut a, b) = tcp_pair().await;
    // Let the connect and accept ops (and their closes) settle.
    sleep(Duration::from_millis(10)).await;
    let before = monoio::driver_stats();

    let mut got = 0;
    let mut sink = [0u8; 4096];
    for _ in 0..N {
        let (res, _) = a.send_eager(&REPLY[..]).await;
        assert_eq!(res.unwrap(), REPLY.len());
        got += b.try_recv(&mut sink).unwrap_or(0);
    }
    assert_eq!(a.try_send(REPLY).unwrap(), REPLY.len());
    let iov = [IoSlice::new(&REPLY[..35]), IoSlice::new(&REPLY[35..])];
    assert_eq!(a.try_send_vectored(&iov).unwrap(), REPLY.len());
    a.set_eager_send(true);
    let (res, _) = a.write_all(&REPLY[..]).await;
    assert_eq!(res.unwrap(), REPLY.len());
    let iov: VecBuf = vec![REPLY[..35].to_vec(), REPLY[35..].to_vec()].into();
    let (res, _) = a.write_vectored_all(iov).await;
    assert_eq!(res.unwrap(), REPLY.len());
    a.set_eager_send(false);
    let deadline = Instant::now() + Duration::from_secs(10);
    while got < (N + 4) * REPLY.len() {
        got += b.try_recv(&mut sink).unwrap_or(0);
        assert!(Instant::now() < deadline);
    }

    let after = monoio::driver_stats();
    assert_eq!(
        after.ops_by_opcode, before.ops_by_opcode,
        "sends and receives that fit must not touch the ring"
    );
    assert_eq!(after.sqes_submitted, before.sqes_submitted);

    // The same reply through the ring is one SEND SQE.
    let (res, _) = a.send_op(&REPLY[..]).await;
    assert_eq!(res.unwrap(), REPLY.len());
    let ring = monoio::driver_stats();
    assert_eq!(ring.ops_by_opcode[send], before.ops_by_opcode[send] + 1);
}
