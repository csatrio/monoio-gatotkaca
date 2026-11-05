//! Dropping an in-flight op keeps its data in the op slot until the kernel is done. Small data
//! (most ops: a fd handle, a buffer handle, a few integers) is stored inline, so the drop does
//! not allocate; large data is boxed. Either way the data is dropped exactly once, after the
//! op completed. A counting allocator (per thread) checks the allocations.
#![cfg(feature = "async-cancel")]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    rc::Rc,
    time::Duration,
};

use monoio::{
    buf::{IoBuf, IoBufMut},
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

/// A receive buffer that counts its drops, with `PAD` extra bytes to make the op data large.
struct Tracked<const PAD: usize> {
    data: Vec<u8>,
    drops: Rc<Cell<usize>>,
    _pad: [u8; PAD],
}

impl<const PAD: usize> Tracked<PAD> {
    fn new(drops: &Rc<Cell<usize>>) -> Self {
        Tracked {
            data: Vec::with_capacity(16),
            drops: drops.clone(),
            _pad: [0; PAD],
        }
    }
}

unsafe impl<const PAD: usize> IoBuf for Tracked<PAD> {
    fn read_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.data.len()
    }
}

unsafe impl<const PAD: usize> IoBufMut for Tracked<PAD> {
    fn write_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    fn bytes_total(&mut self) -> usize {
        self.data.capacity()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        self.data.set_len(init_len)
    }
}

impl<const PAD: usize> Drop for Tracked<PAD> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

/// Let the runtime submit the cancels and reap their completions.
async fn settle() {
    for _ in 0..4 {
        monoio::time::sleep(Duration::from_millis(5)).await;
    }
}

const N: usize = 64;

/// Start `N` receives (none gets data), drop them all, and return the allocations the drops
/// made.
async fn drop_in_flight<const PAD: usize>(stream: &TcpStream, drops: &Rc<Cell<usize>>) -> u64 {
    let mut futs: Vec<_> = (0..N)
        .map(|_| Some(stream.recv_op(Tracked::<PAD>::new(drops))))
        .collect();
    let before = allocs();
    for fut in futs.iter_mut() {
        drop(fut.take());
    }
    let made = allocs() - before;
    drop(futs);
    made
}

#[monoio::test(timer_enabled = true)]
async fn small_data_is_inline() {
    let (_client, server) = tcp_pair().await;
    let drops = Rc::new(Cell::new(0));
    // Warm up: grows the op slab and the driver's deferred-drop list once.
    drop_in_flight::<0>(&server, &drops).await;
    settle().await;
    assert_eq!(drops.get(), N);
    drops.set(0);

    monoio::reset_driver_stats();
    let made = drop_in_flight::<0>(&server, &drops).await;
    assert_eq!(
        made, 0,
        "dropping in-flight ops with small data must not allocate"
    );
    // Kept alive while the kernel may still write to them.
    assert_eq!(drops.get(), 0);
    let s = monoio::driver_stats();
    assert_eq!(s.ignored_inline, N as u64, "{s:?}");
    assert_eq!(s.ignored_boxed, 0);
    assert_eq!(s.cancels_pushed, N as u64);

    settle().await;
    assert_eq!(
        drops.get(),
        N,
        "each buffer dropped once, after its op completed"
    );
    settle().await;
    assert_eq!(drops.get(), N);
}

#[monoio::test(timer_enabled = true)]
async fn large_data_is_boxed() {
    let (_client, server) = tcp_pair().await;
    let drops = Rc::new(Cell::new(0));
    drop_in_flight::<256>(&server, &drops).await;
    settle().await;
    drops.set(0);

    monoio::reset_driver_stats();
    let made = drop_in_flight::<256>(&server, &drops).await;
    assert_eq!(made, N as u64, "one box per op");
    assert_eq!(drops.get(), 0);
    let s = monoio::driver_stats();
    assert_eq!(s.ignored_boxed, N as u64, "{s:?}");
    assert_eq!(s.ignored_inline, 0);

    settle().await;
    assert_eq!(drops.get(), N);
    settle().await;
    assert_eq!(drops.get(), N);
}

/// A dropped op whose completion carries data still sees it land in the kept buffer, and the
/// stream works on afterwards.
#[monoio::test(timer_enabled = true)]
async fn dropped_then_stream_still_works() {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};

    let (mut client, mut server) = tcp_pair().await;
    let drops = Rc::new(Cell::new(0));
    drop(server.recv_op(Tracked::<0>::new(&drops)));
    settle().await;
    assert_eq!(drops.get(), 1);

    let (res, _) = client.write_all(b"after".to_vec()).await;
    res.unwrap();
    let (res, buf) = server.read(Vec::with_capacity(16)).await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(buf, b"after");
}
