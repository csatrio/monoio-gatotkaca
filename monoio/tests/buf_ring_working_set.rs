//! The working set of a `BufRing` (`published`, `grow_published`, `shrink_published`,
//! `BufRingOptions::working_set`), `BufRing::wait_free`, and `next_rearm` on a ring that is
//! smaller than the number of busy connections.

use std::{
    cell::Cell,
    collections::BTreeSet,
    future::Future,
    io::Write,
    os::{fd::AsRawFd, unix::net::UnixStream as StdUnixStream},
    pin::Pin,
    ptr,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use io_uring::{opcode, squeue, types};
use monoio::{
    io::AsyncWriteRentExt,
    net::{
        tcp::{RecvBundleStream, RecvStream},
        UnixStream,
    },
    uring::{self, BufRing, BufRingOptions, Cqe, SelectedBuf},
};

/// Provided buffer rings need Linux 5.19 (incremental ones 6.12); skip on older kernels.
fn ring(bgid: u16, entries: u16, buf_size: usize, options: BufRingOptions) -> Option<BufRing> {
    match BufRing::with_options(bgid, entries, buf_size, options) {
        Ok(ring) => Some(ring),
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
            eprintln!("skipped: buffer ring unsupported: {e}");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

/// Multishot recv needs Linux 6.0.
fn unsupported(e: &std::io::Error) -> bool {
    if e.raw_os_error() == Some(libc::EINVAL) {
        eprintln!("skipped: multishot recv unsupported: {e}");
        return true;
    }
    false
}

async fn recv_one(fd: &impl AsRawFd, ring: &BufRing) -> Cqe {
    let sqe = opcode::Recv::new(types::Fd(fd.as_raw_fd()), ptr::null_mut(), 0)
        .buf_group(ring.group())
        .build()
        .flags(squeue::Flags::BUFFER_SELECT);
    unsafe { uring::submit(sqe, ()) }.unwrap().await.0
}

/// Send one byte and receive it into a buffer of `ring`.
async fn recv_buf(a: &StdUnixStream, b: &mut StdUnixStream, ring: &BufRing) -> SelectedBuf {
    b.write_all(b"x").unwrap();
    ring.take(recv_one(a, ring).await).expect("no buffer")
}

#[monoio::test(timer_enabled = true)]
async fn published_follows_shrink_and_grow() {
    let Some(ring) = ring(60, 8, 16, BufRing::options()) else {
        return;
    };
    let (a, mut b) = StdUnixStream::pair().unwrap();
    assert_eq!((ring.published(), ring.stats().published), (8, 8));

    // The target drops at once, the count as buffers are recycled.
    assert_eq!(ring.shrink_published(6), 2);
    assert_eq!(ring.published(), 8);
    let mut retired = Vec::new();
    for left in (2..8).rev() {
        let buf = recv_buf(&a, &mut b, &ring).await;
        retired.push(buf.bid());
        drop(buf);
        assert_eq!((ring.published(), ring.stats().in_use), (left, 0));
    }
    // Two buffers are left for the kernel: the third receive finds none.
    let first = recv_buf(&a, &mut b, &ring).await;
    let second = recv_buf(&a, &mut b, &ring).await;
    assert!(!retired.contains(&first.bid()) && !retired.contains(&second.bid()));
    b.write_all(b"x").unwrap();
    let dry = recv_one(&a, &ring).await;
    assert_eq!(dry.res(), -libc::ENOBUFS);
    assert!(ring.take(dry).is_none());
    // Without a working set the ring does not grow by itself.
    assert_eq!(ring.published(), 2);

    // Growing hands out the buffer that was retired last.
    assert_eq!(ring.grow_published(1), 3);
    assert_eq!(ring.published(), 3);
    let third = ring.take(recv_one(&a, &ring).await).unwrap();
    assert_eq!(Some(third.bid()), retired.pop());
    assert_eq!(ring.stats().in_use, 3);
    // Held buffers go back into the ring: the count is at its target.
    drop((first, second, third));
    assert_eq!((ring.published(), ring.stats().in_use), (3, 0));
    for _ in 0..3 {
        drop(recv_buf(&a, &mut b, &ring).await);
    }
    assert_eq!(ring.published(), 3);

    // The rest comes back in reverse order of retirement.
    assert_eq!(ring.grow_published(u16::MAX), 8);
    assert_eq!(ring.published(), 8);
    let mut seen = BTreeSet::new();
    let mut held = Vec::new();
    for _ in 0..8 {
        let buf = recv_buf(&a, &mut b, &ring).await;
        seen.insert(buf.bid());
        held.push(buf);
    }
    assert_eq!(seen.len(), 8);
    #[cfg(feature = "metrics")]
    {
        let stats = ring.stats();
        assert_eq!((stats.retired, stats.grown, stats.enobufs), (6, 6, 1));
    }
}

struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// A waker that counts its wakes.
fn counter() -> (Arc<Wakes>, Waker) {
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    (wakes.clone(), Waker::from(wakes))
}

fn poll<F: Future + Unpin>(fut: &mut F, waker: &Waker) -> Poll<F::Output> {
    Pin::new(fut).poll(&mut Context::from_waker(waker))
}

#[monoio::test(timer_enabled = true)]
async fn wait_free_wakes_one_waiter_per_buffer() {
    let Some(ring) = ring(61, 4, 16, BufRing::options()) else {
        return;
    };
    let (a, mut b) = StdUnixStream::pair().unwrap();
    let wakes = |w: &Arc<Wakes>| w.0.load(Ordering::Relaxed);
    let (na, wa) = counter();
    let (nb, wb) = counter();

    // A buffer is free: ready at once.
    assert!(poll(&mut ring.wait_free(), &wa).is_ready());
    ring.wait_free().await;

    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(recv_buf(&a, &mut b, &ring).await);
    }
    let mut first = ring.wait_free();
    let mut second = ring.wait_free();
    assert!(poll(&mut first, &wa).is_pending());
    assert!(poll(&mut second, &wb).is_pending());
    // Polled again without a wake: still waiting, in the same place.
    assert!(poll(&mut first, &wa).is_pending());
    assert_eq!((wakes(&na), wakes(&nb)), (0, 0));

    // One buffer back: the waiter that waits longest is woken, and only it.
    held.pop();
    assert_eq!((wakes(&na), wakes(&nb)), (1, 0));
    held.pop();
    assert_eq!((wakes(&na), wakes(&nb)), (1, 1));
    assert!(poll(&mut first, &wa).is_ready());
    assert!(poll(&mut second, &wb).is_ready());
    drop((first, second));

    // A woken waiter that is dropped passes the wake on.
    for _ in 0..2 {
        held.push(recv_buf(&a, &mut b, &ring).await);
    }
    let mut first = ring.wait_free();
    let mut second = ring.wait_free();
    let mut third = ring.wait_free();
    assert!(poll(&mut first, &wa).is_pending());
    assert!(poll(&mut second, &wb).is_pending());
    assert!(poll(&mut third, &wb).is_pending());
    // A waiter that gives up before a wake just leaves.
    drop(second);
    held.pop();
    assert_eq!((wakes(&na), wakes(&nb)), (2, 1));
    drop(first);
    assert_eq!((wakes(&na), wakes(&nb)), (2, 2));
    assert!(poll(&mut third, &wb).is_ready());

    // Publishing from the reserve wakes too.
    held.clear();
    assert_eq!(ring.shrink_published(3), 1);
    for _ in 0..3 {
        drop(recv_buf(&a, &mut b, &ring).await);
    }
    assert_eq!(ring.published(), 1);
    let last = recv_buf(&a, &mut b, &ring).await;
    let mut wait = ring.wait_free();
    assert!(poll(&mut wait, &wa).is_pending());
    assert_eq!(ring.grow_published(1), 2);
    assert_eq!(wakes(&na), 3);
    assert!(poll(&mut wait, &wa).is_ready());
    drop(last);

    // And as a plain future in a task.
    let all: Vec<_> = {
        let mut all = Vec::new();
        for _ in 0..2 {
            all.push(recv_buf(&a, &mut b, &ring).await);
        }
        all
    };
    let done = Rc::new(Cell::new(false));
    let task = monoio::spawn({
        let (ring, done) = (ring.clone(), done.clone());
        async move {
            ring.wait_free().await;
            done.set(true);
        }
    });
    monoio::time::sleep(Duration::from_millis(20)).await;
    assert!(!done.get());
    drop(all);
    task.await;
    assert!(done.get());
}

/// A ring with a working set that can grow never makes `wait_free` wait.
#[monoio::test(timer_enabled = true)]
async fn wait_free_grows_a_working_set() {
    let Some(ring) = ring(62, 8, 16, BufRing::options().working_set(1, 2)) else {
        return;
    };
    let (a, mut b) = StdUnixStream::pair().unwrap();
    let (_, waker) = counter();
    assert_eq!(ring.published(), 1);
    // The take that leaves nothing free grows the ring by a step.
    let mut held = vec![recv_buf(&a, &mut b, &ring).await];
    assert_eq!(ring.published(), 3);
    while ring.published() < 8 {
        assert!(poll(&mut ring.wait_free(), &waker).is_ready());
        held.push(recv_buf(&a, &mut b, &ring).await);
    }
    while held.len() < 8 {
        held.push(recv_buf(&a, &mut b, &ring).await);
    }
    // All eight are held: now it waits.
    let mut wait = ring.wait_free();
    assert!(poll(&mut wait, &waker).is_pending());
    held.pop();
    assert!(poll(&mut wait, &waker).is_ready());
}

fn pair() -> (UnixStream, StdUnixStream) {
    let (a, b) = StdUnixStream::pair().unwrap();
    (UnixStream::from_std(a).unwrap(), b)
}

/// One round of demand: `n` connections get a message at the same time, and every buffer is
/// held until the last message arrived. Returns the bids used, or `None` on a kernel without
/// multishot recv.
async fn round(
    streams: &mut [RecvStream],
    peers: &mut [StdUnixStream],
    n: usize,
    tag: u8,
) -> Option<Vec<u16>> {
    for (i, peer) in peers[..n].iter_mut().enumerate() {
        peer.write_all(&[tag, i as u8]).unwrap();
    }
    let mut held = Vec::new();
    for (i, stream) in streams[..n].iter_mut().enumerate() {
        let buf = match stream.next_rearm().await.expect("stream ended") {
            Err(e) if unsupported(&e) => return None,
            r => r.expect("next_rearm failed"),
        };
        assert_eq!(&*buf, &[tag, i as u8]);
        held.push(buf);
    }
    Some(held.iter().map(|b| b.bid()).collect())
}

#[monoio::test(timer_enabled = true)]
async fn working_set_grows_once_per_demand_level() {
    const CONNS: usize = 8;
    let Some(ring) = ring(63, 64, 64, BufRing::options().working_set(2, 2)) else {
        return;
    };
    assert_eq!(ring.published(), 2);
    let mut socks = Vec::new();
    let mut streams = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..CONNS {
        let (a, b) = pair();
        streams.push(a.recv_multi(&ring).unwrap());
        socks.push(a);
        peers.push(b);
    }

    // The demand goes from 0 to 8 buffers: the ring grows, through `ENOBUFS` at first.
    let mut used = BTreeSet::new();
    let Some(bids) = round(&mut streams, &mut peers, CONNS, 0).await else {
        return;
    };
    used.extend(bids);
    let grown = ring.published();
    assert!((CONNS..=CONNS + 4).contains(&grown), "published {grown}");
    #[cfg(feature = "metrics")]
    let enobufs = ring.stats().enobufs;
    #[cfg(feature = "metrics")]
    assert!(enobufs >= 1, "the ring never ran dry");

    // The demand stays: no growth, no shrink, and no `ENOBUFS` any more.
    for tag in 1..60 {
        used.extend(round(&mut streams, &mut peers, CONNS, tag).await.unwrap());
        assert_eq!(ring.published(), grown);
    }
    assert!(used.len() <= grown);
    #[cfg(feature = "metrics")]
    assert_eq!(ring.stats().enobufs, enobufs);
    assert_eq!(ring.stats().in_use, 0);

    // The demand drops to one buffer: after a window of takes the ring goes back to its
    // initial size, by retiring the buffers that come back.
    for tag in 0..100 {
        round(&mut streams, &mut peers, 1, tag).await.unwrap();
    }
    assert_eq!(ring.published(), 2);
    #[cfg(feature = "metrics")]
    {
        let stats = ring.stats();
        assert_eq!(stats.retired as usize, grown - 2);
        assert_eq!(stats.enobufs, enobufs);
    }

    // The demand is back: the buffers published again are the ones used before (the reserve
    // hands out what was retired last), not untouched ones.
    let again = round(&mut streams, &mut peers, CONNS, 7).await.unwrap();
    assert!(
        again.iter().all(|bid| used.contains(bid)),
        "cold buffers published: {again:?} not in {used:?}"
    );
    let regrown = ring.published();
    assert!(
        (CONNS..=CONNS + 4).contains(&regrown),
        "published {regrown}"
    );
    #[cfg(feature = "metrics")]
    let enobufs = ring.stats().enobufs;
    for tag in 0..60 {
        round(&mut streams, &mut peers, CONNS, tag).await.unwrap();
    }
    assert_eq!(ring.published(), regrown);
    #[cfg(feature = "metrics")]
    assert_eq!(ring.stats().enobufs, enobufs);
    assert_eq!(ring.stats().in_use, 0);
}

// ---- B2: a ring smaller than the number of busy connections ----

const CONNS: usize = 12;
const CHUNK: usize = 64;
const TOTAL: usize = 150 * CHUNK;

fn byte(conn: usize, at: usize) -> u8 {
    ((at * 31 + conn * 7) % 251) as u8
}

/// Counts the polls of a stream's `poll_next_rearm`.
struct Polls<'a, S>(&'a mut S, &'a Cell<u64>);

impl Future for Polls<'_, RecvStream> {
    type Output = Option<std::io::Result<SelectedBuf>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.1.set(self.1.get() + 1);
        self.0.poll_next_rearm(cx)
    }
}

impl Future for Polls<'_, RecvBundleStream> {
    type Output = Option<std::io::Result<uring::Bundle>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.1.set(self.1.get() + 1);
        self.0.poll_next_rearm(cx)
    }
}

/// `CONNS` connections each receive `TOTAL` bytes through `next_rearm` on `ring`, which has
/// fewer buffers than there are connections; every buffer is held over a yield. All data
/// must arrive in order, no `ENOBUFS` may reach the caller, and the number of polls per
/// received chunk must stay small.
async fn starved(ring: BufRing, bundle: bool) {
    assert!(ring.entries() < CONNS);
    let polls = Rc::new(Cell::new(0u64));
    let chunks = Rc::new(Cell::new(0u64));
    let mut tasks = Vec::new();
    for conn in 0..CONNS {
        let (a, b) = StdUnixStream::pair().unwrap();
        let (a, mut b) = (
            UnixStream::from_std(a).unwrap(),
            UnixStream::from_std(b).unwrap(),
        );
        monoio::spawn(async move {
            let data: Vec<u8> = (0..TOTAL).map(|at| byte(conn, at)).collect();
            b.write_all(data).await.0.unwrap();
            // Keep the socket open until the reader is done.
            monoio::time::sleep(Duration::from_secs(30)).await;
        });
        let (ring, polls, chunks) = (ring.clone(), polls.clone(), chunks.clone());
        tasks.push(monoio::spawn(async move {
            let mut got = 0;
            let check = |got: &mut usize, buf: &[u8]| {
                for (i, b) in buf.iter().enumerate() {
                    assert_eq!(*b, byte(conn, *got + i), "conn {conn} at {}", *got + i);
                }
                *got += buf.len();
            };
            if bundle {
                let mut stream = a.recv_multi_bundle(&ring).unwrap();
                while got < TOTAL {
                    let bundle = match Polls(&mut stream, &polls).await.expect("stream ended") {
                        Err(e) if unsupported(&e) => return false,
                        r => r.unwrap_or_else(|e| panic!("conn {conn}: {e}")),
                    };
                    let held: Vec<_> = bundle.collect();
                    held.iter().for_each(|buf| check(&mut got, buf));
                    // Counted in buffers: a bundle can take every free buffer at once.
                    chunks.set(chunks.get() + held.len() as u64);
                    monoio::task::yield_now().await;
                }
            } else {
                let mut stream = a.recv_multi(&ring).unwrap();
                assert_eq!(stream.ring().group(), ring.group());
                while got < TOTAL {
                    let buf = match Polls(&mut stream, &polls).await.expect("stream ended") {
                        Err(e) if unsupported(&e) => return false,
                        r => r.unwrap_or_else(|e| panic!("conn {conn}: {e}")),
                    };
                    check(&mut got, &buf);
                    chunks.set(chunks.get() + 1);
                    // The ring stays short of buffers.
                    monoio::task::yield_now().await;
                    drop(buf);
                }
            }
            true
        }));
    }
    let all = async {
        let mut ok = true;
        for task in tasks {
            ok &= task.await;
        }
        ok
    };
    let ok = monoio::time::timeout(Duration::from_secs(60), all)
        .await
        .expect("a connection stalled: lost wake");
    if !ok {
        return;
    }
    assert_eq!(ring.stats().in_use, 0);
    let (polls, chunks) = (polls.get(), chunks.get());
    eprintln!(
        "{} chunks, {} polls ({:.2} per chunk), published {}, {:?}",
        chunks,
        polls,
        polls as f64 / chunks as f64,
        ring.published(),
        ring.stats()
    );
    // Without starvation a chunk costs up to two polls (pending, then ready). A starved recv
    // adds the poll that sees `ENOBUFS` and registers, and the one that re-arms.
    assert!(
        polls <= 4 * chunks,
        "{polls} polls for {chunks} chunks: next_rearm spins"
    );
    #[cfg(feature = "metrics")]
    assert!(ring.stats().enobufs > 0, "the ring never ran dry");
}

#[monoio::test(timer_enabled = true)]
async fn next_rearm_on_a_ring_smaller_than_the_connections() {
    if let Some(ring) = ring(64, 4, CHUNK, BufRing::options()) {
        starved(ring, false).await;
    }
}

/// The same with exact accounting: a working set that cannot grow (`step` 0).
#[monoio::test(timer_enabled = true)]
async fn next_rearm_on_a_fixed_working_set() {
    if let Some(ring) = ring(65, 4, CHUNK, BufRing::options().working_set(4, 0)) {
        starved(ring, false).await;
    }
}

/// A working set that grows to the whole ring and is still too small.
#[monoio::test(timer_enabled = true)]
async fn next_rearm_on_a_working_set_at_its_limit() {
    if let Some(ring) = ring(66, 4, CHUNK, BufRing::options().working_set(1, 1)) {
        let handle = ring.clone();
        starved(ring, false).await;
        assert_eq!(handle.published(), 4);
    }
}

#[monoio::test(timer_enabled = true)]
async fn next_rearm_on_a_batch_published_ring() {
    if let Some(ring) = ring(67, 4, CHUNK, BufRing::options().batch_publish(true)) {
        starved(ring, false).await;
    }
}

#[monoio::test(timer_enabled = true)]
async fn next_rearm_on_an_incremental_ring() {
    let options = BufRing::options().incremental(true);
    if let Some(ring) = ring(68, 4, CHUNK, options) {
        starved(ring, false).await;
    }
    let options = BufRing::options().incremental(true).working_set(1, 1);
    if let Some(ring) = ring(69, 4, CHUNK, options) {
        starved(ring, false).await;
    }
}

#[monoio::test(timer_enabled = true)]
async fn bundle_next_rearm_on_a_ring_smaller_than_the_connections() {
    for (bgid, options) in [
        (70, BufRing::options()),
        (71, BufRing::options().working_set(2, 1)),
    ] {
        let Some(ring) = ring(bgid, 4, CHUNK, options) else {
            return;
        };
        if !ring.supports_bundles() {
            eprintln!("skipped: recv bundles unsupported");
            return;
        }
        starved(ring, true).await;
    }
}
