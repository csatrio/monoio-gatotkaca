//! Provided buffer rings: `IORING_REGISTER_PBUF_RING` with `IOSQE_BUFFER_SELECT` (Linux 5.19+).
//!
//! The kernel picks a buffer from a registered pool when data arrives, instead of the caller
//! handing one over per request. That lets many idle connections (or a multishot receive) share
//! one pool.
//!
//! # Memory
//! The pool is an anonymous `mmap` (`MAP_NORESERVE`): page aligned, and a page only costs memory
//! once the kernel writes to it. The ring of buffer descriptors is a separate page-aligned
//! mapping, or allocated by the kernel ([`BufRingOptions::kernel_ring`]).
//!
//! # Ownership of the shared state
//! [`BufRing`] handles and [`SelectedBuf`]s point to one heap-allocated `Shared` through a plain
//! pointer. `Shared` counts the live handles and the live `SelectedBuf`s (the latter also drives
//! the statistics), and is freed (group unregistered, memory unmapped) by whichever drops last.
//! So it outlives every pointer to it, and a `SelectedBuf` costs no reference count of its own.
//! Everything is `!Send`: the counters are plain `Cell`s.
//!
//! # Bundles
//! A bundle completion (`IORING_RECVSEND_BUNDLE`) names only its first buffer; the others are
//! the buffers of the ring slots that followed it when the kernel consumed them, which after
//! out-of-order recycling are not consecutive bids. The ring slots cannot be read back for that:
//! once the kernel consumed a slot, recycling may overwrite it before the completion is taken
//! (when several operations share the ring and their completions are taken in another order).
//! So every fill links the previously filled buffer to the new one (`BufState::next`): the
//! buffers of a bundle are the first one and its successors in fill order. A link only changes
//! when its buffer is filled again, which needs the buffer to be taken and dropped first.
//! The kernel can also shorten the first buffer of a selection (it writes the slot `len`); that
//! length is read from the slot, or from the copy a fill saves before it overwrites a slot whose
//! buffer was consumed but not taken (`BufState::len_snap`).

use std::{
    cell::{Cell, RefCell},
    io,
    ops::Deref,
    os::fd::{AsRawFd, RawFd},
    ptr::{self, NonNull},
    sync::atomic::{AtomicU16, Ordering},
};

use io_uring::{
    cqueue,
    types::{IOU_PBUF_RING_INC, IOU_PBUF_RING_MMAP},
};

use super::{raw::Cqe, uring::IoUringDriver, Inner, CURRENT};
use crate::buf::IoBuf;

// mmap offset of a kernel-allocated buffer ring: `IORING_OFF_PBUF_RING | (bgid <<
// IORING_OFF_PBUF_SHIFT)`, from include/uapi/linux/io_uring.h. The io-uring crate does not export
// them.
const IORING_OFF_PBUF_RING: u64 = 0x8000_0000;
const IORING_OFF_PBUF_SHIFT: u32 = 16;

/// `struct io_uring_buf` from the uapi header (16 bytes). The `resv` field of entry 0 is the
/// ring tail. The fields are written through raw pointers only: the kernel reads (and with
/// `IOU_PBUF_RING_INC` writes) them concurrently, so no Rust reference to an entry is ever made.
#[repr(C)]
struct RawEntry {
    addr: u64,
    len: u32,
    bid: u16,
    resv: u16,
}

/// Options for [`BufRing::with_options`]. All off by default, which is what [`BufRing::new`]
/// uses.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufRingOptions {
    batch_publish: bool,
    hugetlb: bool,
    populate: bool,
    kernel_ring: bool,
    incremental: bool,
}

impl BufRingOptions {
    /// The defaults: every option off.
    pub fn new() -> Self {
        Self::default()
    }

    /// Batch recycling. When off (the default), dropping a [`SelectedBuf`] gives the buffer back
    /// to the kernel at once (one atomic store to the shared ring tail per buffer). When on, the
    /// drop only fills the ring slot, and the tail is published once per driver tick, right
    /// before the runtime enters the kernel, or when you call [`BufRing::publish`].
    ///
    /// Until it is published a recycled buffer looks unavailable to the kernel; this is always
    /// safe, it can only cause `ENOBUFS` sooner. Note: the per-tick publish needs driver
    /// support; until the driver calls it, use [`BufRing::publish`].
    pub fn batch_publish(mut self, on: bool) -> Self {
        self.batch_publish = on;
        self
    }

    /// Back the pool with huge pages (`MAP_HUGETLB`, default huge page size). The pool size is
    /// rounded up to a whole number of huge pages. Creating the ring fails (it does not fall
    /// back) if not enough huge pages are reserved (`vm.nr_hugepages`).
    pub fn hugetlb(mut self, on: bool) -> Self {
        self.hugetlb = on;
        self
    }

    /// Fault in the whole pool up front (`MAP_POPULATE`) instead of on first use.
    pub fn populate(mut self, on: bool) -> Self {
        self.populate = on;
        self
    }

    /// Let the kernel allocate the ring of buffer descriptors (`IOU_PBUF_RING_MMAP`, Linux 6.4+);
    /// it is then mapped from the io_uring fd. The pool is still ours.
    pub fn kernel_ring(mut self, on: bool) -> Self {
        self.kernel_ring = on;
        self
    }

    /// Incremental consumption (`IOU_PBUF_RING_INC`, Linux 6.12+): a receive uses only as much
    /// of a buffer as it needs, and the next receive continues in the same buffer. A
    /// [`SelectedBuf`] is then a view of part of a buffer (see [`SelectedBuf::offset`]), and
    /// several views of one buffer can be alive at once. The buffer goes back to the ring when
    /// the kernel is done with it (a completion without `IORING_CQE_F_BUF_MORE`) and all its
    /// views are dropped.
    ///
    /// [`BufRing::take`] must then see every completion of the group exactly once, in completion
    /// order: the offset of the data is tracked from the lengths.
    pub fn incremental(mut self, on: bool) -> Self {
        self.incremental = on;
        self
    }
}

/// Counters of a [`BufRing`], see [`BufRing::stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufRingStats {
    /// Completions passed to [`BufRing::take`] that failed with `ENOBUFS` (the pool was empty).
    pub enobufs: u64,
    /// Buffers currently held by the application (with at least one live [`SelectedBuf`]).
    pub in_use: usize,
    /// The highest `in_use` seen.
    pub high_water: usize,
    /// Buffers handed out by [`BufRing::take`] and [`BufRing::take_bundle`].
    pub taken: u64,
    /// Buffers given back to the ring.
    pub recycled: u64,
}

/// A registered pool of `entries` buffers of `buf_size` bytes, identified by a group id.
///
/// To receive into the pool, set `IOSQE_BUFFER_SELECT` and the group id on a receive SQE (for
/// example `opcode::Recv::new(fd, null_mut(), 0).buf_group(ring.group())` or
/// `opcode::RecvMulti::new(fd, ring.group())`), submit it with
/// [`submit`](super::raw::submit) or push it yourself (see [`with_ring`](super::raw::with_ring)),
/// and pass the resulting [`Cqe`] to [`take`](Self::take).
///
/// The handle is cheap to clone; the group is unregistered when the last clone and the last
/// [`SelectedBuf`] are gone. Finish or cancel the operations using the group before that.
pub struct BufRing(NonNull<Shared>);

struct Shared {
    driver: Inner,
    bgid: u16,
    mask: u16,
    buf_size: usize,
    incremental: bool,
    batch: bool,
    // Entry 0 of the ring (`ring_map`).
    ring: NonNull<RawEntry>,
    // Start of the pool (`pool_map`). Never moves: the kernel holds its addresses.
    pool: NonNull<u8>,
    // `BufRing` handles alive.
    handles: Cell<usize>,
    // `SelectedBuf`s alive.
    views: Cell<usize>,
    // Next ring slot to fill, and the tail the kernel has seen. They differ only in batch mode.
    tail: Cell<u16>,
    published: Cell<u16>,
    // In `PENDING`.
    queued: Cell<bool>,
    // The buffer filled last, whose `next` the next fill sets.
    last_filled: Cell<u16>,
    bufs: Box<[BufState]>,
    in_use: Cell<usize>,
    high_water: Cell<usize>,
    enobufs: Cell<u64>,
    taken: Cell<u64>,
    recycled: Cell<u64>,
    // Dropped after `Drop for Shared` has unregistered the group.
    _ring_map: Mapping,
    _pool_map: Mapping,
}

struct BufState {
    // The kernel owns the buffer: it is in the ring, or (incremental mode) partially consumed.
    in_kernel: Cell<bool>,
    // Live `SelectedBuf`s of this buffer.
    views: Cell<u32>,
    // Incremental mode: bytes of the buffer already consumed by completions.
    offset: Cell<u32>,
    // The tail value it was last filled at (its slot is `pos & mask`).
    pos: Cell<u16>,
    // The buffer filled right after it (see the module docs).
    next: Cell<u16>,
    // The slot `len` saved when its slot was overwritten while it was still in the kernel.
    len_snap: Cell<u32>,
    // Where the data of a taken bundle buffer starts and how long it is, until `Bundle`
    // hands it out.
    view_start: Cell<u32>,
    view_len: Cell<u32>,
}

thread_local! {
    // Rings in batch mode with recycled buffers the kernel has not seen yet.
    static PENDING: RefCell<Vec<NonNull<Shared>>> = const { RefCell::new(Vec::new()) };
}

/// Publish the ring tail of every buffer ring of this thread that has unpublished recycled
/// buffers ([`BufRingOptions::batch_publish`]).
///
/// The driver must call it right before each `io_uring_enter` (submit and park), so the kernel
/// sees recycled buffers before it needs them. Cheap when nothing is pending.
pub(crate) fn flush_pending() {
    let _ = PENDING.try_with(|pending| {
        // Publishing calls no user code, so nothing else borrows `PENDING` meanwhile.
        let mut pending = pending.borrow_mut();
        for shared in pending.drain(..) {
            // SAFETY: a ring removes itself from `PENDING` before it is freed, so `shared` is
            // live.
            let shared = unsafe { shared.as_ref() };
            shared.queued.set(false);
            shared.publish();
        }
    });
}

impl BufRing {
    /// Register a new group `bgid` with `entries` buffers of `buf_size` bytes on the current
    /// runtime's ring. `entries` must be a power of two (the kernel allows at most 32768).
    ///
    /// Fails with `InvalidInput` for bad sizes or an already used `bgid`, and on kernels older
    /// than 5.19.
    ///
    /// # Panics
    /// Panics if called outside of a monoio runtime.
    pub fn new(bgid: u16, entries: u16, buf_size: usize) -> io::Result<Self> {
        Self::with_options(bgid, entries, buf_size, BufRingOptions::default())
    }

    /// The default [`BufRingOptions`], to chain setters on for
    /// [`with_options`](Self::with_options): `BufRing::options().incremental(true)`.
    pub fn options() -> BufRingOptions {
        BufRingOptions::default()
    }

    /// Like [`new`](Self::new), with [`BufRingOptions`].
    ///
    /// # Panics
    /// Panics if called outside of a monoio runtime.
    pub fn with_options(
        bgid: u16,
        entries: u16,
        buf_size: usize,
        options: BufRingOptions,
    ) -> io::Result<Self> {
        CURRENT.with(|inner| Self::create(inner.clone(), bgid, entries, buf_size, options))
    }

    /// Like [`new`](Self::new), on the ring of `driver`. Usable outside of the runtime.
    pub fn new_in(
        driver: &IoUringDriver,
        bgid: u16,
        entries: u16,
        buf_size: usize,
    ) -> io::Result<Self> {
        Self::new_in_with_options(driver, bgid, entries, buf_size, BufRingOptions::default())
    }

    /// Like [`with_options`](Self::with_options), on the ring of `driver`. Usable outside of the
    /// runtime.
    pub fn new_in_with_options(
        driver: &IoUringDriver,
        bgid: u16,
        entries: u16,
        buf_size: usize,
        options: BufRingOptions,
    ) -> io::Result<Self> {
        Self::create(driver.handle(), bgid, entries, buf_size, options)
    }

    fn create(
        driver: Inner,
        bgid: u16,
        entries: u16,
        buf_size: usize,
        options: BufRingOptions,
    ) -> io::Result<Self> {
        let invalid = |msg| io::Error::new(io::ErrorKind::InvalidInput, msg);
        if !entries.is_power_of_two() {
            return Err(invalid("entries must be a power of two"));
        }
        if buf_size == 0 || buf_size > u32::MAX as usize {
            return Err(invalid("buf_size must be between 1 and u32::MAX"));
        }
        let pool_len = (entries as usize)
            .checked_mul(buf_size)
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or_else(|| invalid("pool too large"))?;
        let ring_len = entries as usize * std::mem::size_of::<RawEntry>();

        let pool_map = Mapping::pool(pool_len, options.hugetlb, options.populate)?;

        let mut flags = 0;
        if options.incremental {
            flags |= IOU_PBUF_RING_INC as u16;
        }
        let ring_map = if options.kernel_ring {
            // SAFETY: no ring address is passed, the kernel allocates the ring.
            driver.with_ring(|ring| unsafe {
                ring.submitter().register_buf_ring_with_flags(
                    0,
                    entries,
                    bgid,
                    flags | IOU_PBUF_RING_MMAP as u16,
                )
            })?;
            let fd = driver.with_ring(|ring| ring.as_raw_fd());
            let offset = IORING_OFF_PBUF_RING | (u64::from(bgid) << IORING_OFF_PBUF_SHIFT);
            match Mapping::ring_fd(fd, offset, ring_len) {
                Ok(map) => map,
                Err(e) => {
                    let _ = driver.with_ring(|ring| ring.submitter().unregister_buf_ring(bgid));
                    return Err(e);
                }
            }
        } else {
            // Zeroed and page aligned, as the kernel requires.
            let map = Mapping::anon(ring_len, libc::MAP_NORESERVE)?;
            // SAFETY: the mapping holds `entries` entries and stays mapped until `Shared` is
            // dropped, which unregisters the group first. On failure it is unmapped here.
            driver.with_ring(|ring| unsafe {
                ring.submitter().register_buf_ring_with_flags(
                    map.ptr.as_ptr() as u64,
                    entries,
                    bgid,
                    flags,
                )
            })?;
            map
        };

        let bufs = (0..entries)
            .map(|_| BufState {
                in_kernel: Cell::new(true),
                views: Cell::new(0),
                offset: Cell::new(0),
                pos: Cell::new(0),
                next: Cell::new(0),
                len_snap: Cell::new(0),
                view_start: Cell::new(0),
                view_len: Cell::new(0),
            })
            .collect();
        let shared = Shared {
            driver,
            bgid,
            mask: entries - 1,
            buf_size,
            incremental: options.incremental,
            batch: options.batch_publish,
            ring: ring_map.ptr.cast(),
            pool: pool_map.ptr,
            handles: Cell::new(1),
            views: Cell::new(0),
            tail: Cell::new(0),
            published: Cell::new(0),
            queued: Cell::new(false),
            last_filled: Cell::new(0),
            bufs,
            in_use: Cell::new(0),
            high_water: Cell::new(0),
            enobufs: Cell::new(0),
            taken: Cell::new(0),
            recycled: Cell::new(0),
            _ring_map: ring_map,
            _pool_map: pool_map,
        };

        // Put every buffer in the ring. The kernel reads none of it until the tail is published.
        for bid in 0..entries {
            shared.fill_slot(bid);
        }
        shared.publish();

        Ok(Self(NonNull::from(Box::leak(Box::new(shared)))))
    }

    fn shared(&self) -> &Shared {
        // SAFETY: `Shared` is alive while a handle exists (see the module docs).
        unsafe { self.0.as_ref() }
    }

    /// The group id, to set as `buf_group` on the SQE.
    pub fn group(&self) -> u16 {
        self.shared().bgid
    }

    /// The size of each buffer.
    pub fn buf_size(&self) -> usize {
        self.shared().buf_size
    }

    /// The number of buffers.
    pub fn entries(&self) -> usize {
        self.shared().bufs.len()
    }

    /// Make the buffers recycled so far visible to the kernel now. Only needed with
    /// [`BufRingOptions::batch_publish`]; without it every recycle is published at once.
    pub fn publish(&self) {
        self.shared().publish();
    }

    /// The counters of this ring.
    pub fn stats(&self) -> BufRingStats {
        let s = self.shared();
        BufRingStats {
            enobufs: s.enobufs.get(),
            in_use: s.in_use.get(),
            high_water: s.high_water.get(),
            taken: s.taken.get(),
            recycled: s.recycled.get(),
        }
    }

    /// Take the buffer the kernel selected for `cqe`.
    ///
    /// Returns `None` if the completion carries no buffer (error, or nothing was read), or if
    /// that buffer is already taken. Call it for every completion of this group, even if you
    /// ignore the data: the buffer only goes back to the ring when the returned [`SelectedBuf`]
    /// is dropped, and the pool runs dry otherwise (receives then fail with `ENOBUFS`, which is
    /// counted in [`stats`](Self::stats)).
    ///
    /// Passing a completion that is not of this group, or a stale one (whose buffer has been
    /// recycled since), is a logic error: the returned bytes may be overwritten by the kernel.
    /// Completions of bundle receives (`IORING_RECVSEND_BUNDLE`) go to
    /// [`take_bundle`](Self::take_bundle) instead.
    pub fn take(&self, cqe: Cqe) -> Option<SelectedBuf> {
        self.take_parts(cqe.res(), cqe.flags())
    }

    fn take_parts(&self, res: i32, flags: u32) -> Option<SelectedBuf> {
        let s = self.shared();
        if res == -libc::ENOBUFS {
            s.enobufs.set(s.enobufs.get() + 1);
            return None;
        }
        let bid = cqueue::buffer_select(flags)?;
        let res = usize::try_from(res).ok()?;
        let state = s.bufs.get(bid as usize)?;
        if !state.in_kernel.get() {
            return None;
        }

        let (offset, len) = if s.incremental {
            // Nothing consumed: the kernel keeps the buffer where it is.
            if res == 0 {
                return None;
            }
            let offset = state.offset.get() as usize;
            let len = res.min(s.buf_size - offset);
            state.offset.set((offset + len) as u32);
            if !cqueue::buffer_more(flags) {
                state.in_kernel.set(false);
            }
            (offset, len)
        } else {
            state.in_kernel.set(false);
            (0, res.min(s.buf_size))
        };

        s.add_view(state);

        Some(SelectedBuf {
            shared: self.0,
            // SAFETY: `offset + len <= buf_size`, so this is inside buffer `bid` of the pool.
            ptr: unsafe { s.buf_ptr(bid).add(offset) },
            len,
            bid,
        })
    }
}

impl BufRing {
    /// Whether the kernel supports bundle receives (`IORING_FEAT_RECVSEND_BUNDLE`, Linux 6.10+),
    /// for [`take_bundle`](Self::take_bundle) and `recv_multi_bundle`.
    pub fn supports_bundles(&self) -> bool {
        self.shared()
            .driver
            .with_ring(|ring| ring.params().is_feature_recvsend_bundle())
    }

    /// Take the buffers of a bundle receive completion (`IORING_RECVSEND_BUNDLE`, Linux 6.10+):
    /// one completion whose `res` bytes fill several buffers, in order.
    ///
    /// The returned [`Bundle`] yields one [`SelectedBuf`] per buffer; concatenated, they are the
    /// received bytes. It allocates nothing. Buffers it still holds go back to the ring when it
    /// is dropped.
    ///
    /// The bundle is empty if the completion carries no data (EOF, an error, `ENOBUFS`, which
    /// is counted in [`stats`](Self::stats)): the kernel then consumed no buffer. Call it for
    /// every bundle completion of the group, and [`take`](Self::take) for every other one; the
    /// two mix freely on one ring, in any order, also across operations. Works in incremental
    /// mode too ([`BufRingOptions::incremental`]): the first buffer continues at its offset and
    /// the last one may stay with the kernel.
    ///
    /// Passing a completion that is not a bundle of this group, or a stale one, is a logic
    /// error, as for `take`.
    pub fn take_bundle(&self, cqe: Cqe) -> Bundle {
        self.take_bundle_parts(cqe.res(), cqe.flags())
    }

    fn take_bundle_parts(&self, res: i32, flags: u32) -> Bundle {
        let s = self.shared();
        let mut bundle = Bundle {
            shared: self.0,
            next: 0,
            count: 0,
            bytes: 0,
        };
        if res == -libc::ENOBUFS {
            s.enobufs.set(s.enobufs.get() + 1);
            return bundle;
        }
        // No data: the kernel consumed nothing (a 0-byte bundle commits no buffer), even if the
        // completion names one.
        let Ok(res) = usize::try_from(res) else {
            return bundle;
        };
        let Some(first) = cqueue::buffer_select(flags) else {
            return bundle;
        };
        if res == 0 || s.bufs.get(first as usize).map(|b| b.in_kernel.get()) != Some(true) {
            return bundle;
        }

        // The kernel fills the buffers in the order it consumed their slots, each up to the
        // length it mapped: the slot `len` (non-incremental), or what is left of the buffer
        // (incremental, the same walk as the kernel's `io_kbuf_inc_commit`).
        let entries = s.bufs.len();
        let mut rem = res;
        let mut bid = first;
        loop {
            let state = &s.bufs[bid as usize];
            if !state.in_kernel.get() {
                // Only a stale or foreign completion gets here.
                debug_assert!(
                    false,
                    "bundle walk reached a buffer that is not in the kernel"
                );
                break;
            }
            let (start, avail) = if s.incremental {
                let offset = state.offset.get() as usize;
                (offset, s.buf_size - offset)
            } else {
                (0, s.slot_len(bid))
            };
            let len = rem.min(avail);
            rem -= len;
            state.view_start.set(start as u32);
            state.view_len.set(len as u32);
            if s.incremental {
                state.offset.set((start + len) as u32);
                // Only the last buffer can be left partially consumed; it then stays in the
                // kernel, as with `take`.
                if rem > 0 || !cqueue::buffer_more(flags) {
                    state.in_kernel.set(false);
                }
            } else {
                state.in_kernel.set(false);
            }
            s.add_view(state);
            bundle.count += 1;
            bundle.bytes += len;
            if rem == 0 || bundle.count as usize == entries {
                break;
            }
            bid = state.next.get();
        }
        debug_assert_eq!(rem, 0, "bundle longer than the ring");
        bundle.next = first;
        bundle
    }
}

impl Clone for BufRing {
    fn clone(&self) -> Self {
        let s = self.shared();
        s.handles.set(s.handles.get() + 1);
        Self(self.0)
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        let s = self.shared();
        s.handles.set(s.handles.get() - 1);
        if s.handles.get() == 0 && s.views.get() == 0 {
            // SAFETY: this was the last pointer to `Shared`.
            unsafe { Shared::free(self.0) };
        }
    }
}

impl std::fmt::Debug for BufRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.shared();
        f.debug_struct("BufRing")
            .field("group", &s.bgid)
            .field("entries", &s.bufs.len())
            .field("buf_size", &s.buf_size)
            .field("stats", &self.stats())
            .finish()
    }
}

impl Shared {
    fn buf_ptr(&self, bid: u16) -> *mut u8 {
        // SAFETY: `bid < entries`, so the offset is inside the pool mapping.
        unsafe { self.pool.as_ptr().add(bid as usize * self.buf_size) }
    }

    // A view of `state` is handed out.
    fn add_view(&self, state: &BufState) {
        let views = state.views.get();
        state.views.set(views + 1);
        if views == 0 {
            let in_use = self.in_use.get() + 1;
            self.in_use.set(in_use);
            self.high_water.set(self.high_water.get().max(in_use));
        }
        self.views.set(self.views.get() + 1);
        self.taken.set(self.taken.get() + 1);
    }

    fn slot(&self, pos: u16) -> *mut RawEntry {
        // SAFETY: the index is masked, so inside the ring.
        unsafe { self.ring.as_ptr().add((pos & self.mask) as usize) }
    }

    // The length the kernel mapped for buffer `bid`, which it consumed: its slot `len` (the
    // kernel shortens the first buffer of a selection to what it wants to read), or the copy
    // saved when the slot was overwritten. Non-incremental mode only.
    fn slot_len(&self, bid: u16) -> usize {
        let state = &self.bufs[bid as usize];
        let slot = self.slot(state.pos.get());
        // SAFETY: inside the ring; read by field, like the kernel's READ_ONCE. The slot holds
        // `bid` until a fill overwrites it (the kernel never writes `bid`), and `bid` itself is
        // not filled again while it is in the kernel, so a matching `bid` means the slot is ours.
        let len = unsafe {
            if ptr::addr_of!((*slot).bid).read_volatile() == bid {
                ptr::addr_of!((*slot).len).read_volatile()
            } else {
                state.len_snap.get()
            }
        } as usize;
        if len == 0 || len > self.buf_size {
            self.buf_size
        } else {
            len
        }
    }

    // Write buffer `bid`, whole, into the next free slot. The slot is free: each buffer is in at
    // most one slot, and there are as many slots as buffers.
    fn fill_slot(&self, bid: u16) {
        let tail = self.tail.get();
        let slot = self.slot(tail);
        // SAFETY: the slot is inside the ring. The kernel does not read slots past the
        // published tail, and only fields are accessed, never a reference to the entry (whose
        // `resv` may be the tail the kernel reads).
        unsafe {
            // The previous occupant, filled one lap ago: if the kernel consumed it but it was not
            // taken yet, save the length the kernel used for a later `take_bundle`.
            let old = ptr::addr_of!((*slot).bid).read();
            if let Some(state) = self.bufs.get(old as usize) {
                let lap_ago = tail.wrapping_sub(self.mask).wrapping_sub(1);
                if state.in_kernel.get() && state.pos.get() == lap_ago {
                    state
                        .len_snap
                        .set(ptr::addr_of!((*slot).len).read_volatile());
                }
            }
            ptr::addr_of_mut!((*slot).addr).write(self.buf_ptr(bid) as u64);
            ptr::addr_of_mut!((*slot).len).write(self.buf_size as u32);
            ptr::addr_of_mut!((*slot).bid).write(bid);
        }
        let state = &self.bufs[bid as usize];
        state.pos.set(tail);
        self.bufs[self.last_filled.get() as usize].next.set(bid);
        self.last_filled.set(bid);
        self.tail.set(tail.wrapping_add(1));
    }

    fn publish(&self) {
        let tail = self.tail.get();
        if self.published.get() != tail {
            self.published.set(tail);
            // SAFETY: the tail is the `resv` field of entry 0, a `u16` the kernel reads
            // atomically. `Release` makes the slot writes visible before it.
            unsafe { AtomicU16::from_ptr(ptr::addr_of_mut!((*self.ring.as_ptr()).resv)) }
                .store(tail, Ordering::Release);
        }
    }

    // Give buffer `bid` back to the kernel.
    fn recycle(&self, this: NonNull<Shared>, bid: u16) {
        let state = &self.bufs[bid as usize];
        state.in_kernel.set(true);
        state.offset.set(0);
        self.fill_slot(bid);
        self.recycled.set(self.recycled.get() + 1);
        if !self.batch {
            self.publish();
        } else if !self.queued.get() {
            let queued = PENDING.try_with(|pending| pending.borrow_mut().push(this));
            if queued.is_ok() {
                self.queued.set(true);
            } else {
                // Thread-local storage is being torn down: publish now.
                self.publish();
            }
        }
    }

    /// A `SelectedBuf` of `bid` was dropped.
    ///
    /// # Safety
    /// `this` must be live; it may be freed by this call.
    unsafe fn release(this: NonNull<Shared>, bid: u16) {
        // SAFETY: live per the contract; the reference ends before `free`.
        let s = unsafe { this.as_ref() };
        let state = &s.bufs[bid as usize];
        let views = state.views.get() - 1;
        state.views.set(views);
        s.views.set(s.views.get() - 1);
        if views == 0 {
            s.in_use.set(s.in_use.get() - 1);
            // Without handles the group is about to be unregistered; no need to recycle.
            if !state.in_kernel.get() && s.handles.get() > 0 {
                s.recycle(this, bid);
            }
        }
        if s.handles.get() == 0 && s.views.get() == 0 {
            // SAFETY: no handle and no view is left: this was the last pointer.
            unsafe { Shared::free(this) };
        }
    }

    /// # Safety
    /// `this` must come from `BufRing::create` and no pointer to it may be used afterwards.
    unsafe fn free(this: NonNull<Shared>) {
        // SAFETY: per the contract.
        let shared = unsafe { Box::from_raw(this.as_ptr()) };
        if shared.queued.get() {
            let _ = PENDING.try_with(|pending| pending.borrow_mut().retain(|p| *p != this));
        }
        drop(shared);
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // The kernel stops using the ring and the pool once this returns; the mappings are
        // unmapped after it.
        let _ = self
            .driver
            .with_ring(|ring| ring.submitter().unregister_buf_ring(self.bgid));
    }
}

/// An owned memory mapping, unmapped on drop.
struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
}

impl Mapping {
    fn mmap(len: usize, prot: i32, flags: i32, fd: RawFd, offset: u64) -> io::Result<Self> {
        // SAFETY: a fresh mapping at an address of the kernel's choice; nothing is overwritten.
        let ptr = unsafe { libc::mmap(ptr::null_mut(), len, prot, flags, fd, offset as _) };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: NonNull::new(ptr.cast()).expect("mmap returned null"),
            len,
        })
    }

    // Private anonymous memory: zeroed, page aligned, faulted in on first use.
    fn anon(len: usize, extra_flags: i32) -> io::Result<Self> {
        Self::mmap(
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | extra_flags,
            -1,
            0,
        )
    }

    fn pool(len: usize, hugetlb: bool, populate: bool) -> io::Result<Self> {
        let populate = if populate { libc::MAP_POPULATE } else { 0 };
        if !hugetlb {
            return Self::anon(len, libc::MAP_NORESERVE | populate);
        }
        // Without MAP_NORESERVE: the huge pages are reserved now, so a shortage fails here
        // instead of as SIGBUS (or EFAULT in the kernel) on first use.
        // munmap needs a whole number of huge pages.
        let huge = huge_page_size();
        let len = len
            .checked_next_multiple_of(huge)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "pool too large"))?;
        Self::anon(len, libc::MAP_HUGETLB | populate).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "buffer pool with MAP_HUGETLB failed ({e}): reserve huge pages \
                     (vm.nr_hugepages) or turn off BufRingOptions::hugetlb"
                ),
            )
        })
    }

    // The kernel-allocated ring of a group, mapped from the io_uring fd.
    fn ring_fd(fd: RawFd, offset: u64, len: usize) -> io::Result<Self> {
        Self::mmap(
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            fd,
            offset,
        )
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the mapping is ours and nothing points into it any more.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

// The default huge page size (`Hugepagesize` in /proc/meminfo), 2 MiB if unknown.
fn huge_page_size() -> usize {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|info| {
            let line = info.lines().find(|l| l.starts_with("Hugepagesize:"))?;
            let kib: usize = line.split_whitespace().nth(1)?.parse().ok()?;
            Some(kib * 1024)
        })
        .unwrap_or(2 << 20)
}

/// A buffer selected by the kernel, holding the received bytes (`Deref<Target = [u8]>`).
/// Dropping it returns the buffer to the ring (in incremental mode: once the kernel is done with
/// the buffer and its last view is dropped).
///
/// It implements [`IoBuf`], so it can be written or sent as is, without a copy (also through
/// [`IoBuf::slice`]).
pub struct SelectedBuf {
    shared: NonNull<Shared>,
    ptr: *mut u8,
    len: usize,
    bid: u16,
}

impl SelectedBuf {
    /// The buffer id.
    pub fn bid(&self) -> u16 {
        self.bid
    }

    /// Where the data starts inside the buffer: always 0, except in incremental mode
    /// ([`BufRingOptions::incremental`]).
    pub fn offset(&self) -> usize {
        // SAFETY: `Shared` is alive while a view exists.
        let s = unsafe { self.shared.as_ref() };
        self.ptr as usize - s.buf_ptr(self.bid) as usize
    }

    /// Copy the data out and give the buffer back to the ring at once.
    pub fn detach(self) -> Vec<u8> {
        self.to_vec()
    }

    /// Like [`detach`](Self::detach), as [`bytes::Bytes`].
    #[cfg(feature = "bytes")]
    pub fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::from(self.detach())
    }
}

impl Deref for SelectedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: `len` bytes at `ptr` are in the pool, which stays mapped while a view exists,
        // and the kernel no longer writes them (it moved past them, or handed the buffer over).
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl AsRef<[u8]> for SelectedBuf {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

// SAFETY: the pointer is stable (the pool never moves) and `len` bytes are initialized, for as
// long as the `SelectedBuf` lives.
unsafe impl IoBuf for SelectedBuf {
    #[inline]
    fn read_ptr(&self) -> *const u8 {
        self.ptr
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        self.len
    }
}

impl std::fmt::Debug for SelectedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // SAFETY: `Shared` is alive while a view exists.
        let s = unsafe { self.shared.as_ref() };
        f.debug_struct("SelectedBuf")
            .field("group", &s.bgid)
            .field("bid", &self.bid)
            .field("offset", &self.offset())
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for SelectedBuf {
    fn drop(&mut self) {
        // SAFETY: `Shared` is alive while a view exists; `self.shared` is not used afterwards.
        unsafe { Shared::release(self.shared, self.bid) };
    }
}

/// The buffers of one bundle receive completion, see [`BufRing::take_bundle`].
///
/// An iterator of [`SelectedBuf`]s, in data order; concatenated they are the received bytes.
/// [`len`](ExactSizeIterator::len) is the number of buffers left, [`bytes`](Self::bytes) their
/// total length. Buffers not yet yielded are held by the bundle and go back to the ring when it
/// is dropped. Allocation free: the buffers are linked through the ring's own bookkeeping.
#[must_use = "dropping a bundle gives its buffers back unread"]
pub struct Bundle {
    shared: NonNull<Shared>,
    // The next buffer to yield; valid while `count > 0`.
    next: u16,
    count: u32,
    bytes: usize,
}

impl Bundle {
    /// Bytes in the buffers not yielded yet.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// No buffer left (also: the completion carried no data).
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Copy the data out and give the buffers back to the ring at once.
    pub fn detach(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bytes);
        for buf in self {
            out.extend_from_slice(&buf);
        }
        out
    }

    /// Like [`detach`](Self::detach), as [`bytes::Bytes`].
    #[cfg(feature = "bytes")]
    pub fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::from(self.detach())
    }
}

impl Iterator for Bundle {
    type Item = SelectedBuf;

    fn next(&mut self) -> Option<SelectedBuf> {
        if self.count == 0 {
            return None;
        }
        // SAFETY: the bundle holds a view of each buffer not yielded, so `Shared` is alive.
        let s = unsafe { self.shared.as_ref() };
        let bid = self.next;
        let state = &s.bufs[bid as usize];
        let (start, len) = (
            state.view_start.get() as usize,
            state.view_len.get() as usize,
        );
        self.count -= 1;
        self.bytes -= len;
        // Read the link before the buffer can be dropped and filled again.
        self.next = state.next.get();
        // The bundle's view moves into the `SelectedBuf`.
        Some(SelectedBuf {
            shared: self.shared,
            // SAFETY: `start + len <= buf_size`, inside buffer `bid` of the pool.
            ptr: unsafe { s.buf_ptr(bid).add(start) },
            len,
            bid,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.count as usize, Some(self.count as usize))
    }
}

impl ExactSizeIterator for Bundle {}

impl std::iter::FusedIterator for Bundle {}

impl Drop for Bundle {
    fn drop(&mut self) {
        self.by_ref().for_each(drop);
    }
}

impl std::fmt::Debug for Bundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bundle")
            .field("buffers", &self.count)
            .field("bytes", &self.bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeBuilder;

    const F_BUFFER: u32 = 1;

    fn flags(bid: u16, more: bool) -> u32 {
        F_BUFFER | (u32::from(bid) << 16) | if more { 1 << 4 } else { 0 }
    }

    fn kernel_tail(ring: &BufRing) -> u16 {
        let s = ring.shared();
        // SAFETY: entry 0 is live; read like the kernel does.
        unsafe { AtomicU16::from_ptr(ptr::addr_of_mut!((*s.ring.as_ptr()).resv)) }
            .load(Ordering::Acquire)
    }

    #[test]
    fn batch_mode_publishes_on_flush() {
        let rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
        let opts = BufRingOptions::new().batch_publish(true);
        let Ok(ring) = BufRing::new_in_with_options(rt.driver(), 40, 4, 64, opts) else {
            return;
        };
        assert_eq!(kernel_tail(&ring), 4);

        let a = ring.take_parts(5, flags(0, false)).unwrap();
        let b = ring.take_parts(5, flags(1, false)).unwrap();
        drop((a, b));
        // Slots written, tail not published.
        assert_eq!(kernel_tail(&ring), 4);
        assert_eq!(ring.stats().recycled, 2);
        flush_pending();
        assert_eq!(kernel_tail(&ring), 6);

        let c = ring.take_parts(5, flags(2, false)).unwrap();
        drop(c);
        ring.publish();
        assert_eq!(kernel_tail(&ring), 7);
        // Already published: the stale entry in `PENDING` is harmless.
        flush_pending();
        assert_eq!(kernel_tail(&ring), 7);

        // A ring freed while queued removes itself.
        let d = ring.take_parts(5, flags(3, false)).unwrap();
        drop(d);
        drop(ring);
        flush_pending();
        assert!(PENDING.with(|p| p.borrow().is_empty()));
    }

    #[test]
    fn views_keep_the_ring_alive_and_incremental_offsets() {
        let rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
        let opts = BufRingOptions::new().incremental(true);
        let ring = match BufRing::new_in_with_options(rt.driver(), 41, 2, 16, opts) {
            Ok(ring) => ring,
            Err(_) => return,
        };
        let a = ring.take_parts(4, flags(0, true)).unwrap();
        let b = ring.take_parts(5, flags(0, true)).unwrap();
        assert_eq!((a.offset(), a.len()), (0, 4));
        assert_eq!((b.offset(), b.len()), (4, 5));
        let c = ring.take_parts(7, flags(0, false)).unwrap();
        assert_eq!((c.offset(), c.len()), (9, 7));
        // Done with the buffer: a stale completion is refused.
        assert!(ring.take_parts(1, flags(0, false)).is_none());
        assert_eq!(ring.stats().in_use, 1);

        drop(ring);
        drop((a, b));
        assert_eq!(c.len(), 7);
        drop(c); // frees the ring
    }

    // What the kernel does when it shortens the first buffer of a selection.
    fn set_slot_len(ring: &BufRing, pos: u16, len: u32) {
        let s = ring.shared();
        unsafe { ptr::addr_of_mut!((*s.slot(pos)).len).write(len) };
    }

    fn parts(bundle: Bundle) -> Vec<(u16, usize, usize)> {
        bundle.map(|b| (b.bid(), b.offset(), b.len())).collect()
    }

    #[test]
    fn bundle_follows_fill_order_after_out_of_order_recycling() {
        let rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
        let Ok(ring) = BufRing::new_in(rt.driver(), 42, 4, 64) else {
            return;
        };
        // Slots 0, 1 (bids 0, 1) go to single receives; recycled in reverse order they fill
        // slots 4 and 5 (ring slots 0 and 1) as bids 1, 0.
        let a = ring.take_parts(5, flags(0, false)).unwrap();
        let b = ring.take_parts(5, flags(1, false)).unwrap();
        drop(b);
        drop(a);
        // The kernel shortened the slot of bid 3 when it once picked it first.
        set_slot_len(&ring, 3, 20);
        // One bundle over slots 2..6: bids 2, 3, 1, 0.
        let bundle = ring.take_bundle_parts(64 + 20 + 64 + 10, flags(2, false));
        assert_eq!((bundle.len(), bundle.bytes()), (4, 158));
        assert_eq!(ring.stats().in_use, 4);
        assert_eq!(
            parts(bundle),
            [(2, 0, 64), (3, 0, 20), (1, 0, 64), (0, 0, 10)]
        );
        assert_eq!(ring.stats().in_use, 0);
        assert_eq!(ring.stats().taken, 6);

        // Nothing received: nothing consumed, even if the completion names a buffer.
        assert!(ring.take_bundle_parts(0, flags(2, false)).is_empty());
        assert!(ring
            .take_bundle_parts(-libc::ECONNRESET, flags(2, false))
            .is_empty());
        assert!(ring.take_bundle_parts(-libc::ENOBUFS, 0).is_empty());
        assert_eq!(ring.stats().enobufs, 1);
        assert_eq!(ring.stats().in_use, 0);
    }

    #[test]
    fn bundle_lengths_survive_slot_reuse() {
        let rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
        let Ok(ring) = BufRing::new_in(rt.driver(), 43, 4, 64) else {
            return;
        };
        // A bundle consumed slots 0, 1 (bids 0, 1; the first shortened to 30), but its
        // completion is not taken yet. Single receives on slots 2, 3 are taken and recycled,
        // which overwrites ring slots 0 and 1.
        set_slot_len(&ring, 0, 30);
        let c = ring.take_parts(5, flags(2, false)).unwrap();
        let d = ring.take_parts(5, flags(3, false)).unwrap();
        drop((c, d));
        let s = ring.shared();
        assert_eq!(s.bufs[0].len_snap.get(), 30);
        let bundle = ring.take_bundle_parts(30 + 40, flags(0, false));
        assert_eq!(parts(bundle), [(0, 0, 30), (1, 0, 40)]);
        assert_eq!(ring.stats().in_use, 0);
    }

    #[test]
    fn incremental_bundle_continues_and_keeps_the_last_buffer() {
        let rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
        let opts = BufRingOptions::new().incremental(true);
        let Ok(ring) = BufRing::new_in_with_options(rt.driver(), 44, 4, 16, opts) else {
            return;
        };
        let a = ring.take_parts(4, flags(0, true)).unwrap();
        // Rest of bid 0, all of bid 1, the start of bid 2 (which the kernel keeps).
        let mut bundle = ring.take_bundle_parts(12 + 16 + 5, flags(0, true));
        assert_eq!(bundle.len(), 3);
        let first = bundle.next().unwrap();
        assert_eq!((first.bid(), first.offset(), first.len()), (0, 4, 12));
        assert_eq!(parts(bundle), [(1, 0, 16), (2, 0, 5)]);
        // Bid 2 continues.
        let c = ring.take_parts(3, flags(2, false)).unwrap();
        assert_eq!((c.bid(), c.offset(), c.len()), (2, 5, 3));
        drop((a, first, c));
        assert_eq!(ring.stats().in_use, 0);
        assert_eq!(ring.stats().recycled, 3);
    }
}
