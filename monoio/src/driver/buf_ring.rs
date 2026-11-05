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
    /// [`SelectedBuf`]s handed out by [`BufRing::take`].
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

        let views = state.views.get();
        state.views.set(views + 1);
        if views == 0 {
            let in_use = s.in_use.get() + 1;
            s.in_use.set(in_use);
            s.high_water.set(s.high_water.get().max(in_use));
        }
        s.views.set(s.views.get() + 1);
        s.taken.set(s.taken.get() + 1);

        Some(SelectedBuf {
            shared: self.0,
            // SAFETY: `offset + len <= buf_size`, so this is inside buffer `bid` of the pool.
            ptr: unsafe { s.buf_ptr(bid).add(offset) },
            len,
            bid,
        })
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

    // Write buffer `bid`, whole, into the next free slot. The slot is free: each buffer is in at
    // most one slot, and there are as many slots as buffers.
    fn fill_slot(&self, bid: u16) {
        let tail = self.tail.get();
        // SAFETY: the index is masked, so inside the ring. The kernel does not read slots past
        // the published tail, and only fields are written, never a reference to the entry (whose
        // `resv` may be the tail the kernel reads).
        unsafe {
            let slot = self.ring.as_ptr().add((tail & self.mask) as usize);
            ptr::addr_of_mut!((*slot).addr).write(self.buf_ptr(bid) as u64);
            ptr::addr_of_mut!((*slot).len).write(self.buf_size as u32);
            ptr::addr_of_mut!((*slot).bid).write(bid);
        }
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
}
