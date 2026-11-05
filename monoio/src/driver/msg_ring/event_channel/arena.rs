//! Message nodes for `event_channel`: mmap'd segments, a tagged-index free list and a bump
//! index.
//!
//! Segment `k` holds `base << k` nodes (the last one capped at `max`), so node `i` lives in
//! segment `log2(i / base + 1)`. Segments are mapped on demand and stay mapped until the arena
//! is dropped, so any index below `mapped` always points to valid memory.
//!
//! Free list: the head packs `(tag << 32) | index`, linked through `next_free`. Senders pop with
//! one CAS that also bumps the tag, so a head that was popped and pushed back in between (ABA)
//! makes the CAS fail. A stale `next_free` read is harmless for the same reason, and the memory
//! is never unmapped while the arena lives. The tag is 32 bits: an ABA needs 2^32 updates of
//! the head while one sender sits between its load and its CAS.
//!
//! Nodes never handed out yet come from `bump`, so their pages are only faulted in when used.

use std::{
    cell::UnsafeCell,
    io,
    mem::MaybeUninit,
    ptr,
    sync::{
        atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering},
        Mutex,
    },
};

use adaptif_gatotkaca_utils::TreiberNode;
use crossbeam_utils::{Backoff, CachePadded};

/// No node: an empty free list.
const NIL: u32 = u32::MAX;
const MAX_SEGMENTS: usize = 32;

/// One message slot.
#[repr(C)]
pub(super) struct Node<T> {
    /// Link in the inbox (the Treiber stack).
    next: AtomicPtr<Node<T>>,
    /// Link in the free list.
    next_free: AtomicU32,
    index: u32,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> TreiberNode for Node<T> {
    type Value = UnsafeCell<MaybeUninit<T>>;

    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }

    /// Called for nodes still in a chain that is dropped (receiver or channel gone): drop the
    /// message. The memory belongs to the arena, which unmaps it later.
    unsafe fn drop_node(ptr: *mut Self) {
        if !ptr.is_null() {
            // SAFETY: a node in a chain carries an initialized value nobody else owns.
            unsafe { (*(*ptr).value.get()).assume_init_drop() };
        }
    }
}

impl<T> Node<T> {
    pub(super) fn index(&self) -> u32 {
        self.index
    }

    /// # Safety
    /// The node must be owned by the caller and hold no value.
    pub(super) unsafe fn write(&self, value: T) {
        unsafe { (*self.value.get()).write(value) };
    }

    /// # Safety
    /// The node must be owned by the caller and hold a value, which is moved out.
    pub(super) unsafe fn take(&self) -> T {
        unsafe { (*self.value.get()).assume_init_read() }
    }
}

pub(super) struct Arena<T> {
    base: u32,
    max: u32,
    segments: [AtomicPtr<Node<T>>; MAX_SEGMENTS],
    /// Indices below this point into mapped memory.
    mapped: AtomicU32,
    /// Next index never handed out.
    bump: CachePadded<AtomicU32>,
    free: CachePadded<AtomicU64>,
    /// Held by a sender that maps a new segment (cold path; the receiver never takes it).
    grow: Mutex<()>,
}

// SAFETY: nodes are handed between threads with their values (`T: Send`); all shared state is
// atomic.
unsafe impl<T: Send> Send for Arena<T> {}
unsafe impl<T: Send> Sync for Arena<T> {}

impl<T> Arena<T> {
    /// `base` must be a power of two, `base <= max < u32::MAX`.
    pub(super) fn new(base: u32, max: u32) -> io::Result<Self> {
        let page = page_size();
        if std::mem::align_of::<Node<T>>() > page {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "event_channel: message alignment above the page size",
            ));
        }
        let arena = Self {
            base,
            max,
            segments: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            mapped: AtomicU32::new(0),
            bump: CachePadded::new(AtomicU32::new(0)),
            free: CachePadded::new(AtomicU64::new(NIL as u64)),
            grow: Mutex::new(()),
        };
        arena.map_next()?;
        Ok(arena)
    }

    /// Nodes backed by memory.
    pub(super) fn capacity(&self) -> u32 {
        self.mapped.load(Ordering::Relaxed)
    }

    /// Segment number and its first index for `index`.
    fn locate(&self, index: u32) -> (usize, u32) {
        let block = index / self.base + 1;
        let k = (u32::BITS - 1 - block.leading_zeros()) as usize;
        let start = self.base * ((1 << k) - 1);
        (k, start)
    }

    /// The node at `index`, which must be below `mapped`.
    pub(super) fn node(&self, index: u32) -> &Node<T> {
        let (k, start) = self.locate(index);
        let seg = self.segments[k].load(Ordering::Acquire);
        debug_assert!(!seg.is_null());
        // SAFETY: `index < mapped`, so segment `k` is mapped and holds this offset; segments
        // stay mapped while the arena lives.
        unsafe { &*seg.add((index - start) as usize) }
    }

    /// Take a free node, mapping a new segment if needed. `None` when `max` nodes are in use.
    pub(super) fn alloc(&self) -> Option<&Node<T>> {
        let backoff = Backoff::new();
        loop {
            let head = self.free.load(Ordering::Acquire);
            let index = head as u32;
            if index != NIL {
                // A stale read here only makes the CAS below fail.
                let next = self.node(index).next_free.load(Ordering::Relaxed);
                let new = pack(tag(head).wrapping_add(1), next);
                if self
                    .free
                    .compare_exchange_weak(head, new, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(self.node(index));
                }
                backoff.spin();
                continue;
            }

            let next = self.bump.load(Ordering::Relaxed);
            if next < self.mapped.load(Ordering::Acquire) {
                if self
                    .bump
                    .compare_exchange_weak(next, next + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(self.init(next));
                }
                backoff.spin();
                continue;
            }

            if !self.grow() {
                return None;
            }
        }
    }

    fn init(&self, index: u32) -> &Node<T> {
        let (k, start) = self.locate(index);
        let seg = self.segments[k].load(Ordering::Acquire);
        // SAFETY: the bump index hands each slot out once; the memory is mapped and unused.
        unsafe {
            let node = seg.add((index - start) as usize);
            node.write(Node {
                next: AtomicPtr::new(ptr::null_mut()),
                next_free: AtomicU32::new(NIL),
                index,
                value: UnsafeCell::new(MaybeUninit::uninit()),
            });
            &*node
        }
    }

    /// Make room: `true` if nodes may be available now (retry), `false` when full.
    fn grow(&self) -> bool {
        let _guard = self.grow.lock().unwrap_or_else(|e| e.into_inner());
        // Another sender may have grown it, or the receiver returned nodes, meanwhile.
        if self.bump.load(Ordering::Relaxed) < self.mapped.load(Ordering::Acquire)
            || self.free.load(Ordering::Acquire) as u32 != NIL
        {
            return true;
        }
        self.map_next().is_ok()
    }

    /// Map the next segment. Called at creation and under `grow`.
    fn map_next(&self) -> io::Result<()> {
        let mapped = self.mapped.load(Ordering::Relaxed);
        if mapped >= self.max {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }
        let (k, start) = self.locate(mapped);
        debug_assert_eq!(start, mapped);
        let len = self.seg_len(k, start);
        let bytes = seg_bytes::<T>(len);
        // SAFETY: an anonymous private mapping, faulted in lazily.
        let seg = unsafe {
            libc::mmap(
                ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if seg == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        self.segments[k].store(seg.cast(), Ordering::Release);
        self.mapped.store(start + len, Ordering::Release);
        Ok(())
    }

    fn seg_len(&self, k: usize, start: u32) -> u32 {
        (self.base << k).min(self.max - start)
    }

    /// Return a chain of nodes linked through `next_free` (`first` to `last`) with one CAS.
    /// Their values must have been taken.
    pub(super) fn free_chain(&self, first: u32, last: u32) {
        let last = self.node(last);
        let backoff = Backoff::new();
        let mut head = self.free.load(Ordering::Relaxed);
        loop {
            last.next_free.store(head as u32, Ordering::Relaxed);
            let new = pack(tag(head).wrapping_add(1), first);
            match self
                .free
                .compare_exchange_weak(head, new, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(h) => {
                    head = h;
                    backoff.spin();
                }
            }
        }
    }

    /// Link `index` in front of a local chain headed by `first` (see [`free_chain`]).
    pub(super) fn link_free(&self, index: u32, first: u32) {
        self.node(index).next_free.store(first, Ordering::Relaxed);
    }
}

impl<T> Drop for Arena<T> {
    fn drop(&mut self) {
        let mapped = *self.mapped.get_mut();
        let mut start = 0;
        for k in 0..MAX_SEGMENTS {
            if start >= mapped {
                break;
            }
            let len = self.seg_len(k, start);
            let seg = *self.segments[k].get_mut();
            // SAFETY: mapped by `map_next` with this size; no node is used any more (values
            // still queued were dropped by the channel before).
            unsafe { libc::munmap(seg.cast(), seg_bytes::<T>(len)) };
            start += len;
        }
    }
}

fn pack(tag: u32, index: u32) -> u64 {
    (tag as u64) << 32 | index as u64
}

fn tag(head: u64) -> u32 {
    (head >> 32) as u32
}

fn seg_bytes<T>(len: u32) -> usize {
    let page = page_size();
    (len as usize * std::mem::size_of::<Node<T>>()).div_ceil(page) * page
}

fn page_size() -> usize {
    // SAFETY: sysconf has no preconditions.
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}
