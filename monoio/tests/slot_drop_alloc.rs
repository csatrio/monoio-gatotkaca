//! What it costs to drop an in-flight receive, by buffer type (r3-slot, E5).
//!
//! The data of a dropped op stays in its op slot until the kernel is done with it: inline if
//! it fits the slot's storage, else in a box. A receive into a `Vec<u8>` is 40 bytes of op
//! data, one into a `BytesMut` is 48: both must stay inline, so that a timeout firing on a
//! receive (or a `select!` dropping one) does not allocate. A counting allocator (per thread)
//! checks it; each test prints its numbers (`--nocapture`).
#![cfg(all(feature = "async-cancel", feature = "bytes"))]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    time::Duration,
};

use bytes::BytesMut;
use monoio::{
    buf::IoBufMut,
    io::AsyncReadRent,
    net::{TcpListener, TcpStream},
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

/// Let the runtime submit the cancels and reap their completions.
async fn settle() {
    for _ in 0..4 {
        monoio::time::sleep(Duration::from_millis(5)).await;
    }
}

const N: usize = 256;

/// Start `N` receives (none gets data) into buffers from `make`, drop them all in flight, and
/// return the allocations the drops made.
async fn drop_in_flight<B: IoBufMut + 'static>(stream: &TcpStream, make: impl Fn() -> B) -> u64 {
    let mut futs: Vec<_> = (0..N).map(|_| Some(stream.recv_op(make()))).collect();
    let before = allocs();
    for fut in futs.iter_mut() {
        drop(fut.take());
    }
    let made = allocs() - before;
    drop(futs);
    made
}

/// The drops of `N` in-flight receives into buffers from `make`: no allocation, all kept
/// inline.
async fn dropped_inline<B: IoBufMut + 'static>(name: &str, make: impl Fn() -> B) {
    let (_client, server) = tcp_pair().await;
    // Warm up: grows the op slab and the driver's deferred-drop list once.
    drop_in_flight(&server, &make).await;
    settle().await;

    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();
    let made = drop_in_flight(&server, &make).await;
    eprintln!(
        "slot_drop_alloc: {name}: {made} allocations for {N} dropped receives ({:.2} per op)",
        made as f64 / N as f64
    );
    #[cfg(feature = "metrics")]
    {
        let s = monoio::driver_stats();
        eprintln!(
            "slot_drop_alloc: {name}: ignored_inline {} ignored_boxed {}",
            s.ignored_inline, s.ignored_boxed
        );
        assert_eq!(s.ignored_inline, N as u64, "{s:?}");
        assert_eq!(s.ignored_boxed, 0, "{s:?}");
    }
    assert_eq!(made, 0, "dropping an in-flight receive must not allocate");
    settle().await;
}

#[monoio::test(timer_enabled = true)]
async fn recv_into_vec_dropped_in_flight() {
    dropped_inline("Vec<u8>", || Vec::<u8>::with_capacity(1024)).await;
}

#[monoio::test(timer_enabled = true)]
async fn recv_into_bytes_mut_dropped_in_flight() {
    dropped_inline("BytesMut", || BytesMut::with_capacity(1024)).await;
}

/// The same through `time::timeout`: every round a timeout fires on a receive that never
/// completes, which drops the receive in flight. A round allocates its buffer and nothing
/// else.
#[monoio::test(timer_enabled = true)]
async fn timeout_firing_on_a_receive() {
    const ROUNDS: usize = 64;
    let (_client, mut server) = tcp_pair().await;

    async fn rounds(server: &mut TcpStream, n: usize) -> u64 {
        let before = allocs();
        for _ in 0..n {
            let buf = BytesMut::with_capacity(1024);
            let res = monoio::time::timeout(Duration::from_millis(1), server.read(buf)).await;
            assert!(res.is_err(), "nothing was sent, so the timeout fires");
        }
        allocs() - before
    }

    // Warm up: the timer, the op slab and the deferred-drop list grow once.
    rounds(&mut server, ROUNDS).await;
    settle().await;

    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();
    let made = rounds(&mut server, ROUNDS).await;
    eprintln!(
        "slot_drop_alloc: timeout on a BytesMut receive: {made} allocations in {ROUNDS} rounds \
         ({:.2} per round, 1 of them the buffer)",
        made as f64 / ROUNDS as f64
    );
    #[cfg(feature = "metrics")]
    {
        let s = monoio::driver_stats();
        eprintln!(
            "slot_drop_alloc: timeout on a BytesMut receive: ignored_inline {} ignored_boxed {}",
            s.ignored_inline, s.ignored_boxed
        );
        assert_eq!(s.ignored_inline, ROUNDS as u64, "{s:?}");
        assert_eq!(s.ignored_boxed, 0, "{s:?}");
    }
    assert_eq!(made, ROUNDS as u64, "one allocation per round: the buffer");
    settle().await;
}
