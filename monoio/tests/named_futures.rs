//! Named op futures (`monoio::io::futures`): echo through every `_op` method, storing them in
//! struct fields, driving them by hand with `poll_result`, cancel and drop, and a counting
//! allocator that shows the steady-state op path does not allocate.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
    io::Write,
    pin::Pin,
    task::{Context, Poll},
};

use monoio::{
    buf::VecBuf,
    io::futures::{ReadAtFuture, ReadvFuture, RecvFuture, SendFuture, WriteAtFuture, WritevFuture},
    net::{MsgFlags, TcpListener, TcpStream, UnixStream},
    BufResult,
};

// ---- counting allocator (per thread, so parallel tests don't disturb each other) ----

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

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect_addr(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server)
}

/// The named futures are `Unpin + 'static`, so they can live in plain struct fields.
#[allow(dead_code)]
struct Stored {
    recv: Option<RecvFuture<Vec<u8>>>,
    send: Option<SendFuture<Vec<u8>>>,
    readv: Option<ReadvFuture<VecBuf>>,
    writev: Option<WritevFuture<VecBuf>>,
    read_at: Option<ReadAtFuture<Vec<u8>>>,
    write_at: Option<WriteAtFuture<&'static [u8]>>,
}

fn assert_unpin_static<T: Unpin + 'static>() {}

#[test]
fn named_futures_are_unpin_and_static() {
    assert_unpin_static::<RecvFuture<Vec<u8>>>();
    assert_unpin_static::<SendFuture<Vec<u8>>>();
    assert_unpin_static::<ReadvFuture<VecBuf>>();
    assert_unpin_static::<WritevFuture<VecBuf>>();
    assert_unpin_static::<ReadAtFuture<Vec<u8>>>();
    assert_unpin_static::<WriteAtFuture<Vec<u8>>>();
    assert_unpin_static::<Stored>();
}

// ---- echo through every op ----

async fn echo_ops<S: OpStream>(a: &S, b: &S) {
    // send_op / recv_op
    let (res, _) = a.send(b"hello".to_vec()).await;
    assert_eq!(res.unwrap(), 5);
    let (res, buf) = b.recv(Vec::with_capacity(16)).await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(&buf, b"hello");

    // writev_op / readv_op
    let iov: VecBuf = vec![b"iovec_".to_vec(), b"is_good".to_vec()].into();
    let (res, _) = a.writev(iov).await;
    assert_eq!(res.unwrap(), 13);
    let mut got = Vec::new();
    while got.len() < 13 {
        let iov: VecBuf = vec![vec![0; 4], vec![0; 9]].into();
        let (res, iov) = b.readv(iov).await;
        let n = res.unwrap();
        assert!(n > 0);
        let parts: Vec<Vec<u8>> = iov.into();
        got.extend(parts.concat().into_iter().take(n));
    }
    assert_eq!(&got, b"iovec_is_good");

    // flag variants
    let (res, _) = a.send_flags(b"more".to_vec(), MsgFlags::EMPTY).await;
    assert_eq!(res.unwrap(), 4);
    let (res, buf) = b.recv_flags(Vec::with_capacity(4), MsgFlags::WAITALL).await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(&buf, b"more");
}

/// Small shim so the echo runs over both stream types.
trait OpStream {
    fn send(&self, buf: Vec<u8>) -> SendFuture<Vec<u8>>;
    fn send_flags(&self, buf: Vec<u8>, flags: MsgFlags) -> SendFuture<Vec<u8>>;
    fn recv(&self, buf: Vec<u8>) -> RecvFuture<Vec<u8>>;
    fn recv_flags(&self, buf: Vec<u8>, flags: MsgFlags) -> RecvFuture<Vec<u8>>;
    fn writev(&self, buf: VecBuf) -> WritevFuture<VecBuf>;
    fn readv(&self, buf: VecBuf) -> ReadvFuture<VecBuf>;
}

macro_rules! op_stream {
    ($t:ty) => {
        impl OpStream for $t {
            fn send(&self, buf: Vec<u8>) -> SendFuture<Vec<u8>> {
                self.send_op(buf)
            }
            fn send_flags(&self, buf: Vec<u8>, flags: MsgFlags) -> SendFuture<Vec<u8>> {
                self.send_with_flags_op(buf, flags)
            }
            fn recv(&self, buf: Vec<u8>) -> RecvFuture<Vec<u8>> {
                self.recv_op(buf)
            }
            fn recv_flags(&self, buf: Vec<u8>, flags: MsgFlags) -> RecvFuture<Vec<u8>> {
                self.recv_with_flags_op(buf, flags)
            }
            fn writev(&self, buf: VecBuf) -> WritevFuture<VecBuf> {
                self.writev_op(buf)
            }
            fn readv(&self, buf: VecBuf) -> ReadvFuture<VecBuf> {
                self.readv_op(buf)
            }
        }
    };
}
op_stream!(TcpStream);
op_stream!(UnixStream);

#[monoio::test]
async fn tcp_echo_ops() {
    let (a, b) = tcp_pair().await;
    echo_ops(&a, &b).await;
    echo_ops(&b, &a).await;
}

#[monoio::test]
async fn unix_echo_ops() {
    let (a, b) = UnixStream::pair().unwrap();
    echo_ops(&a, &b).await;
    echo_ops(&b, &a).await;
}

#[monoio::test]
async fn split_halves_ops() {
    use monoio::io::Splitable;
    let (a, b) = tcp_pair().await;
    let (ar, aw) = a.into_split();
    let (br, bw) = b.into_split();
    let (res, _) = aw.send_op(b"ping".to_vec()).await;
    assert_eq!(res.unwrap(), 4);
    let (res, buf) = br.recv_op(Vec::with_capacity(8)).await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(&buf, b"ping");
    let (res, _) = bw
        .writev_op(VecBuf::from(vec![b"po".to_vec(), b"ng".to_vec()]))
        .await;
    assert_eq!(res.unwrap(), 4);
    let (res, buf) = ar
        .recv_with_flags_op(Vec::with_capacity(4), MsgFlags::WAITALL)
        .await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(&buf, b"pong");
}

#[monoio::test]
async fn file_at_ops() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(b"0123456789").unwrap();
    let file = monoio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(tmp.path())
        .await
        .unwrap();

    let (res, buf) = file.read_at_op(Vec::with_capacity(4), 3).await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(&buf, b"3456");

    let (res, _) = file.write_at_op(&b"ab"[..], 8).await;
    assert_eq!(res.unwrap(), 2);
    let (res, buf) = file.read_at_op(Vec::with_capacity(16), 0).await;
    assert_eq!(res.unwrap(), 10);
    assert_eq!(&buf, b"01234567ab");

    // Two ops in flight at once, stored side by side.
    let r1 = file.read_at_op(Vec::with_capacity(2), 0);
    let r2 = file.read_at_op(Vec::with_capacity(2), 8);
    let ((res1, b1), (res2, b2)) = monoio::join!(r1, r2);
    assert_eq!((res1.unwrap(), res2.unwrap()), (2, 2));
    assert_eq!((&b1[..], &b2[..]), (&b"01"[..], &b"ab"[..]));
    file.close().await.unwrap();
}

// ---- token API: a hand-written state machine ----

enum EchoState {
    Reading(RecvFuture<Vec<u8>>),
    Writing(SendFuture<Vec<u8>>),
}

/// Echoes `rounds` messages back to the peer, polling the stored tokens by hand.
struct EchoMachine {
    stream: TcpStream,
    state: EchoState,
    rounds: usize,
}

impl Future for EchoMachine {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
        let this = &mut *self;
        loop {
            match &mut this.state {
                EchoState::Reading(tok) => {
                    let (res, buf) = std::task::ready!(tok.poll_result(cx));
                    assert!(tok.is_terminated());
                    let n = res.unwrap();
                    assert!(n > 0);
                    this.state = EchoState::Writing(this.stream.send_op(buf));
                }
                EchoState::Writing(tok) => {
                    let (res, mut buf) = std::task::ready!(tok.poll_result(cx));
                    assert_eq!(res.unwrap(), buf.len());
                    this.rounds -= 1;
                    if this.rounds == 0 {
                        return Poll::Ready(buf.capacity());
                    }
                    buf.clear();
                    this.state = EchoState::Reading(this.stream.recv_op(buf));
                }
            }
        }
    }
}

#[monoio::test]
async fn poll_result_state_machine() {
    let (client, server) = tcp_pair().await;
    let machine = EchoMachine {
        state: EchoState::Reading(server.recv_op(Vec::with_capacity(64))),
        stream: server,
        rounds: 3,
    };
    let handle = monoio::spawn(machine);
    for msg in [&b"one"[..], b"two", b"three"] {
        let (res, _) = client.send_op(msg.to_vec()).await;
        assert_eq!(res.unwrap(), msg.len());
        let (res, buf) = client
            .recv_with_flags_op(Vec::with_capacity(msg.len()), MsgFlags::WAITALL)
            .await;
        assert_eq!(res.unwrap(), msg.len());
        assert_eq!(&buf, msg);
    }
    assert_eq!(handle.await, 64);
}

// ---- cancel and drop ----

#[monoio::test(timer_enabled = true)]
async fn cancel_returns_buffer() {
    let (_a, b) = tcp_pair().await;
    let mut tok = b.recv_op(Vec::<u8>::with_capacity(8));
    // Nothing to read: the op stays pending.
    let pending = std::future::poll_fn(|cx| Poll::Ready(tok.poll_result(cx).is_pending())).await;
    assert!(pending);
    tok.cancel();
    let (res, buf) = tok.await;
    assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
    assert!(buf.is_empty());
    assert_eq!(buf.capacity(), 8);
}

// Without `async-cancel` a dropped read is never cancelled, so the next read would wait forever.
#[cfg(feature = "async-cancel")]
#[monoio::test(timer_enabled = true)]
async fn drop_in_flight_then_reuse() {
    let (a, b) = tcp_pair().await;
    {
        let mut tok = b.recv_op(Vec::with_capacity(16));
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(tok.poll_result(cx).is_pending())).await;
        assert!(pending);
        // Dropped in flight: async-cancel cancels it; the runtime keeps the buffer alive.
    }
    // Let the ring process the cancel before data arrives.
    monoio::time::sleep(std::time::Duration::from_millis(20)).await;

    let (res, _) = a.send_op(b"after".to_vec()).await;
    assert_eq!(res.unwrap(), 5);
    let (res, buf) = b.recv_op(Vec::with_capacity(16)).await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(&buf, b"after");

    // A future dropped before it was ever polled is also fine.
    drop(b.recv_op(Vec::with_capacity(16)));
    monoio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (res, _) = a.send_op(b"again".to_vec()).await;
    assert_eq!(res.unwrap(), 5);
    let (res, buf) = b.recv_op(Vec::with_capacity(16)).await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(&buf, b"again");
}

/// A future whose op completed in the kernel but whose result was never taken: dropping it
/// frees the op slot and the buffer (the path next to the inline "already `Done`" drop).
#[monoio::test(timer_enabled = true)]
async fn drop_completed_before_result_was_taken() {
    let (a, b) = tcp_pair().await;

    // Never polled: the CQE finds the op in its submitted state.
    let (res, _) = a.send_op(b"lost1".to_vec()).await;
    assert_eq!(res.unwrap(), 5);
    let tok = b.recv_op(Vec::with_capacity(16));
    monoio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!tok.is_terminated());
    drop(tok);

    // Polled once (pending), completed afterwards, then dropped.
    let mut tok = b.recv_op(Vec::with_capacity(16));
    let pending = std::future::poll_fn(|cx| Poll::Ready(tok.poll_result(cx).is_pending())).await;
    assert!(pending);
    let (res, _) = a.send_op(b"lost2".to_vec()).await;
    assert_eq!(res.unwrap(), 5);
    monoio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!tok.is_terminated());
    drop(tok);

    // Both receives took their data; the stream goes on with what comes next.
    let (res, _) = a.send_op(b"kept".to_vec()).await;
    assert_eq!(res.unwrap(), 4);
    let mut tok = b.recv_op(Vec::with_capacity(16));
    let (res, buf) = std::future::poll_fn(|cx| tok.poll_result(cx)).await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(&buf, b"kept");
    // Returned its output: nothing is left in it, dropping it is a no-op.
    assert!(tok.is_terminated());
    drop(tok);

    let (res, _) = a.send_op(b"more".to_vec()).await;
    assert_eq!(res.unwrap(), 4);
    let (res, buf) = b.recv_op(Vec::with_capacity(16)).await;
    assert_eq!(res.unwrap(), 4);
    assert_eq!(&buf, b"more");
}

#[test]
#[should_panic(expected = "op future polled after completion")]
fn poll_after_completion_panics() {
    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    rt.block_on(async {
        let (a, _b) = tcp_pair().await;
        let mut tok = a.send_op(b"x".to_vec());
        let (res, _) = std::future::poll_fn(|cx| tok.poll_result(cx)).await;
        assert_eq!(res.unwrap(), 1);
        assert!(tok.is_terminated());
        let _ = std::future::poll_fn(|cx| tok.poll_result(cx)).await;
    });
}

// ---- allocation count ----

/// One request/response round trip with reused buffers.
async fn round_trip(
    a: &TcpStream,
    b: &TcpStream,
    req: Vec<u8>,
    resp: Vec<u8>,
) -> (Vec<u8>, Vec<u8>) {
    let (res, mut req) = a.send_op(req).await;
    assert_eq!(res.unwrap(), 64);
    let (res, rbuf) = b.recv_op(resp).await;
    assert_eq!(res.unwrap(), 64);
    let (res, mut rbuf) = b.send_op(rbuf).await;
    assert_eq!(res.unwrap(), 64);
    req.clear();
    let (res, req) = a.recv_op(req).await;
    assert_eq!(res.unwrap(), 64);
    rbuf.clear();
    (req, rbuf)
}

/// With the trait methods, for comparison (they return the same named futures).
async fn round_trip_trait(
    a: &mut TcpStream,
    b: &mut TcpStream,
    req: Vec<u8>,
    resp: Vec<u8>,
) -> (Vec<u8>, Vec<u8>) {
    use monoio::io::{AsyncReadRent, AsyncWriteRent};
    let (res, mut req) = a.write(req).await;
    assert_eq!(res.unwrap(), 64);
    let (res, rbuf) = b.read(resp).await;
    assert_eq!(res.unwrap(), 64);
    let (res, mut rbuf) = b.write(rbuf).await;
    assert_eq!(res.unwrap(), 64);
    req.clear();
    let (res, req) = a.read(req).await;
    assert_eq!(res.unwrap(), 64);
    rbuf.clear();
    (req, rbuf)
}

#[test]
fn steady_state_ops_do_not_allocate() {
    const WARMUP: usize = 64;
    const N: usize = 10_000;

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    let counts = rt.block_on(async {
        let (mut a, mut b) = tcp_pair().await;
        let mut req = vec![1u8; 64];
        let mut resp = Vec::with_capacity(64);

        for _ in 0..WARMUP {
            (req, resp) = round_trip(&a, &b, req, resp).await;
            req.resize(64, 1);
        }
        let before = allocs();
        for _ in 0..N {
            (req, resp) = round_trip(&a, &b, req, resp).await;
            // `resize` within capacity does not allocate.
            req.resize(64, 1);
        }
        let named = allocs() - before;

        for _ in 0..WARMUP {
            (req, resp) = round_trip_trait(&mut a, &mut b, req, resp).await;
            req.resize(64, 1);
        }
        let before = allocs();
        for _ in 0..N {
            (req, resp) = round_trip_trait(&mut a, &mut b, req, resp).await;
            req.resize(64, 1);
        }
        let traits = allocs() - before;

        // For scale: the facade pattern this replaces (one `Box::pin` per op).
        let before = allocs();
        for _ in 0..N {
            let fut: Pin<Box<dyn Future<Output = BufResult<usize, Vec<u8>>>>> =
                Box::pin(a.send_op(req));
            let (res, r) = fut.await;
            req = r;
            assert_eq!(res.unwrap(), 64);
            let (res, r) = b.recv_op(resp).await;
            resp = r;
            assert_eq!(res.unwrap(), 64);
            resp.clear();
        }
        let boxed = allocs() - before;
        (named, traits, boxed)
    });
    let (named, traits, boxed) = counts;
    println!(
        "allocations over {N} round trips (4 ops each): named futures {named}, trait methods \
         {traits}; {N} boxed sends: {boxed}"
    );
    assert_eq!(named, 0, "named-future round trips allocated");
    assert_eq!(traits, 0, "trait-method round trips allocated");
    assert!(boxed >= N as u64);
}
