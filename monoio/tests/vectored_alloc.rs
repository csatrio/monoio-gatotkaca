//! `send_vectored_op` (round 3, A1): no allocation per vectored send in steady state (the
//! `msghdr` comes from a per-thread free list, the iovec array is owned by the `IoVecArray`),
//! a short send is reported like `send_vectored`, a future dropped in flight keeps its buffers
//! until the kernel is done, and `cancel()` gives them back. A counting allocator (per
//! thread) checks the allocations.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    os::unix::io::AsRawFd,
    rc::Rc,
    task::Poll,
    time::Duration,
};

use monoio::{
    buf::{IoVecArray, IoVecBuf},
    net::{MsgFlags, TcpListener, TcpStream},
};

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

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}

fn set_sndbuf(stream: &TcpStream, bytes: libc::c_int) {
    // SAFETY: a valid socket and a valid int.
    let rc = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &bytes as *const _ as *const _,
            std::mem::size_of_val(&bytes) as _,
        )
    };
    assert_eq!(rc, 0);
}

/// `try_send` until the socket is full. Returns the bytes it took.
fn fill(stream: &TcpStream) -> usize {
    let chunk = vec![0xAAu8; 64 * 1024];
    let mut total = 0;
    loop {
        match stream.try_send(&chunk) {
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return total,
            Err(e) => panic!("{e}"),
        }
    }
}

/// Fills the socket, and again after the peer's ACKs freed what was in flight, so that a
/// send afterwards cannot make progress until the peer reads. Needs the timer.
async fn fill_fully(stream: &TcpStream) -> usize {
    let mut total = 0;
    for _ in 0..3 {
        total += fill(stream);
        monoio::time::sleep(Duration::from_millis(60)).await;
    }
    total + fill(stream)
}

/// Reads `n` bytes from `stream` (any content) and returns them.
async fn read_n(stream: &TcpStream, n: usize) -> Vec<u8> {
    let mut got = Vec::with_capacity(n);
    while got.len() < n {
        let (res, buf) = stream
            .recv_op(Vec::with_capacity((n - got.len()).min(256 * 1024)))
            .await;
        let k = res.unwrap();
        assert!(k > 0, "EOF before {n} bytes");
        got.extend_from_slice(&buf[..k]);
    }
    got
}

async fn settle() {
    for _ in 0..4 {
        monoio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ---- allocation count ----

/// One frame: a 3-iovec reply (header, body, trailer) with `send_vectored_op`, read back
/// with one `WAITALL` receive. The three buffers and the array are reused.
async fn frame(
    a: &TcpStream,
    b: &TcpStream,
    mut parts: IoVecArray<Vec<u8>, 3>,
    mut reply: Vec<u8>,
    bufs: [Vec<u8>; 3],
) -> (IoVecArray<Vec<u8>, 3>, Vec<u8>, [Vec<u8>; 3]) {
    let [h, body, t] = bufs;
    parts.push(h).unwrap();
    parts.push(body).unwrap();
    parts.push(t).unwrap();
    let total = parts.total_len();
    let (res, mut parts) = a.send_vectored_op(parts).await;
    assert_eq!(res.unwrap(), total);
    reply.clear();
    let (res, reply) = b.recv_with_flags_op(reply, MsgFlags::WAITALL).await;
    assert_eq!(res.unwrap(), total);
    assert_eq!(&reply[..8], b"HDR-0001");
    let mut it = parts.drain();
    let bufs = [it.next().unwrap(), it.next().unwrap(), it.next().unwrap()];
    drop(it);
    (parts, reply, bufs)
}

#[test]
fn steady_state_vectored_sends_do_not_allocate() {
    const WARMUP: usize = 64;
    const N: usize = 1000;

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    let (made, idle) = rt.block_on(async {
        let (a, b) = tcp_pair().await;
        let mut parts: IoVecArray<Vec<u8>, 3> = IoVecArray::new();
        let bufs = [b"HDR-0001".to_vec(), vec![0x42u8; 40], b"\r\n\r\n".to_vec()];
        // `WAITALL` fills the whole capacity: exactly one reply.
        let mut reply = Vec::with_capacity(bufs.iter().map(Vec::len).sum());
        let mut bufs = bufs;
        for _ in 0..WARMUP {
            (parts, reply, bufs) = frame(&a, &b, parts, reply, bufs).await;
        }
        let before = allocs();
        for _ in 0..N {
            (parts, reply, bufs) = frame(&a, &b, parts, reply, bufs).await;
        }
        (allocs() - before, monoio::__vectored::idle_msghdrs())
    });
    println!("allocations over {N} vectored sends: {made}; idle msghdrs on the thread: {idle}");
    assert_eq!(made, 0, "vectored sends allocated in steady state");
    assert!(
        idle <= 1,
        "one send at a time keeps at most one idle msghdr"
    );
}

/// The rent method `send_vectored` uses the same op, so it allocates nothing either.
#[test]
fn rent_send_vectored_does_not_allocate_either() {
    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    let made = rt.block_on(async {
        let (a, b) = tcp_pair().await;
        let mut parts: IoVecArray<Vec<u8>, 2> = IoVecArray::new();
        // `WAITALL` fills the whole capacity: exactly one reply.
        let mut reply = Vec::with_capacity(4);
        let mut h = b"ab".to_vec();
        let mut t = b"cd".to_vec();
        let mut made = 0;
        for i in 0..100 {
            parts.push(h).unwrap();
            parts.push(t).unwrap();
            let before = allocs();
            let (res, mut p) = a.send_vectored(parts).await;
            if i >= 10 {
                made += allocs() - before;
            }
            assert_eq!(res.unwrap(), 4);
            reply.clear();
            let (res, r) = b.recv_with_flags_op(reply, MsgFlags::WAITALL).await;
            assert_eq!(res.unwrap(), 4);
            assert_eq!(&r, b"abcd");
            reply = r;
            let mut it = p.drain();
            h = it.next().unwrap();
            t = it.next().unwrap();
            drop(it);
            parts = p;
        }
        made
    });
    assert_eq!(made, 0);
}

// ---- short send ----

fn big_parts() -> IoVecArray<Vec<u8>, 3> {
    let mut p: IoVecArray<Vec<u8>, 3> = IoVecArray::new();
    p.push(vec![1u8; 300_000]).unwrap();
    p.push(vec![2u8; 300_000]).unwrap();
    p.push(vec![3u8; 300_000]).unwrap();
    p
}

/// A send into a small socket buffer takes what fits: `send_vectored_op` reports the short
/// count exactly like `send_vectored`, and the bytes on the wire are the prefix of the
/// concatenated buffers. `advance` then sends the rest.
#[monoio::test]
async fn short_send_is_reported_like_send_vectored() {
    let (a, b) = tcp_pair().await;
    set_sndbuf(&a, 4096);
    let expected: Vec<u8> = big_parts().drain().flatten().collect();
    let total = expected.len();

    // Named future.
    let (res, p) = a.send_vectored_op(big_parts()).await;
    let n = res.unwrap();
    assert!(
        n > 0 && n < total,
        "short send expected, got {n} of {total}"
    );
    assert_eq!(
        p.len(),
        3,
        "the buffers come back whole; `advance` is the caller's"
    );
    let got = read_n(&b, n).await;
    assert_eq!(got, expected[..n]);

    // Rent method, same socket state.
    let (res, _) = a.send_vectored(big_parts()).await;
    let m = res.unwrap();
    assert!(
        m > 0 && m < total,
        "short send expected, got {m} of {total}"
    );
    let got = read_n(&b, m).await;
    assert_eq!(got, expected[..m]);

    // `advance` after each short send; the peer reads concurrently.
    let reader = monoio::spawn(async move { read_n(&b, total).await });
    let mut p = big_parts();
    let mut sent = 0;
    let mut sends = 0;
    while sent < total {
        let (res, q) = a.send_vectored_op(p).await;
        let n = res.unwrap();
        assert!(n > 0);
        sent += n;
        sends += 1;
        p = q;
        p.advance(n);
    }
    assert!(p.is_empty());
    assert!(
        sends > 1,
        "the socket buffer is 4 KiB, 900 KB take several sends"
    );
    assert_eq!(reader.await, expected);
}

// ---- drop in flight ----

/// A gather list that counts its drops.
struct Tracked {
    inner: IoVecArray<Vec<u8>, 3>,
    drops: Rc<Cell<usize>>,
}

impl Tracked {
    fn new(drops: &Rc<Cell<usize>>) -> Self {
        let mut inner = IoVecArray::new();
        inner.push(vec![7u8; 1024]).unwrap();
        inner.push(vec![8u8; 1024]).unwrap();
        inner.push(vec![9u8; 1024]).unwrap();
        Tracked {
            inner,
            drops: drops.clone(),
        }
    }
}

unsafe impl IoVecBuf for Tracked {
    fn read_iovec_ptr(&self) -> *const libc::iovec {
        self.inner.read_iovec_ptr()
    }

    fn read_iovec_len(&self) -> usize {
        self.inner.read_iovec_len()
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

const N: usize = 64;

/// Start `N` vectored sends on a full socket, drop them all, and return the allocations the
/// drops made.
fn drop_in_flight(stream: &TcpStream, drops: &Rc<Cell<usize>>) -> u64 {
    let mut futs: Vec<_> = (0..N)
        .map(|_| Some(stream.send_vectored_op(Tracked::new(drops))))
        .collect();
    let before = allocs();
    for fut in futs.iter_mut() {
        drop(fut.take());
    }
    let made = allocs() - before;
    drop(futs);
    made
}

/// Dropped in flight: the buffers (and the `msghdr`) live until the kernel finished the send.
/// With `async-cancel` the drop pushes a cancel, so the ops end with `ECANCELED` at the next
/// ticks; the buffers are dropped then, exactly once. The op data (an `IoVecArray` of three
/// `Vec`s plus the counter) is over the 48-byte inline limit, so each drop boxes it once.
#[cfg(feature = "async-cancel")]
#[monoio::test(timer_enabled = true)]
async fn dropped_in_flight_keeps_buffers_until_the_cqe() {
    let (a, _b) = tcp_pair().await;
    set_sndbuf(&a, 4096);
    fill_fully(&a).await;
    let drops = Rc::new(Cell::new(0));

    // Warm up: grows the op slab, the driver's deferred-drop list and the msghdr pool once.
    drop_in_flight(&a, &drops);
    settle().await;
    assert_eq!(drops.get(), N);
    drops.set(0);

    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();
    let made = drop_in_flight(&a, &drops);
    assert_eq!(
        made, N as u64,
        "one box per dropped op (data over 48 bytes)"
    );
    // Nothing completed yet (no driver tick ran): the buffers are alive.
    assert_eq!(drops.get(), 0, "buffers of in-flight sends must stay alive");
    #[cfg(feature = "metrics")]
    {
        let s = monoio::driver_stats();
        assert_eq!(s.ignored_boxed, N as u64, "{s:?}");
        assert_eq!(s.ignored_inline, 0);
        assert_eq!(s.cancels_pushed, N as u64);
    }

    settle().await;
    assert_eq!(
        drops.get(),
        N,
        "each gather list dropped once, after its op completed"
    );
    settle().await;
    assert_eq!(drops.get(), N);
    // The msghdrs of both bursts went back to the free list.
    assert!(monoio::__vectored::idle_msghdrs() >= N);
}

/// A bare `Vec<libc::iovec>` makes 48 bytes of op data: dropped in flight it is stored inline
/// in the op slot, no allocation.
#[cfg(feature = "async-cancel")]
#[monoio::test(timer_enabled = true)]
async fn iovec_vec_is_inline_when_dropped() {
    static PAYLOAD: [u8; 512] = [5u8; 512];
    let iov = || -> Vec<libc::iovec> {
        vec![
            libc::iovec {
                iov_base: PAYLOAD.as_ptr() as *mut _,
                iov_len: 256,
            },
            libc::iovec {
                iov_base: PAYLOAD[256..].as_ptr() as *mut _,
                iov_len: 256,
            },
        ]
    };
    let (a, _b) = tcp_pair().await;
    set_sndbuf(&a, 4096);
    fill_fully(&a).await;

    // Warm up.
    for _ in 0..N {
        drop(a.send_vectored_op(iov()));
    }
    settle().await;

    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();
    let mut futs: Vec<_> = (0..N).map(|_| Some(a.send_vectored_op(iov()))).collect();
    let before = allocs();
    for fut in futs.iter_mut() {
        drop(fut.take());
    }
    let made = allocs() - before;
    assert_eq!(
        made, 0,
        "dropping an in-flight `Vec<iovec>` send must not allocate"
    );
    #[cfg(feature = "metrics")]
    {
        let s = monoio::driver_stats();
        assert_eq!(s.ignored_inline, N as u64, "{s:?}");
        assert_eq!(s.ignored_boxed, 0);
    }
    settle().await;
}

// ---- cancel ----

/// `cancel()` on a send that waits for room: the result is `ECANCELED` (or a short count if
/// the kernel took some bytes first), and the gather list comes back whole.
#[monoio::test(timer_enabled = true)]
async fn cancel_returns_the_buffers() {
    let (a, _b) = tcp_pair().await;
    set_sndbuf(&a, 4096);
    fill_fully(&a).await;
    let drops = Rc::new(Cell::new(0));
    let mut tok = a.send_vectored_op(Tracked::new(&drops));
    let pending = std::future::poll_fn(|cx| Poll::Ready(tok.poll_result(cx).is_pending())).await;
    assert!(pending);
    tok.cancel();
    let (res, bufs) = tok.await;
    match res {
        Err(e) => assert_eq!(e.raw_os_error(), Some(libc::ECANCELED)),
        Ok(n) => assert!(n < 3072, "a full socket cannot take the whole send"),
    }
    assert_eq!(bufs.inner.len(), 3);
    assert_eq!(drops.get(), 0);
    drop(bufs);
    assert_eq!(drops.get(), 1);
}
