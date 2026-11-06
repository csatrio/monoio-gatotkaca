//! Registered (fixed) buffers: `IORING_REGISTER_BUFFERS2` (sparse) and
//! `IORING_REGISTER_BUFFERS_UPDATE` (Linux 5.13+), used by `IORING_OP_READ_FIXED` and
//! `IORING_OP_WRITE_FIXED`.
//!
//! The kernel pins a registered buffer once, at registration, instead of mapping the user pages
//! on every operation. A [`FixedBufPool`] is a set of equal-sized buffers registered on one
//! ring; [`FixedBufPool::get`] hands out an owned [`FixedBuf`], which goes back to the pool when
//! dropped. The fixed ops ([`File::read_fixed_at`], [`File::write_fixed_at`],
//! [`TcpStream::recv_fixed`], [`TcpStream::send_fixed`]) take it by value like every rent op.
//! A `FixedBuf` also implements [`IoBuf`] and [`IoBufMut`], so it works with the ordinary ops
//! too (then without the fixed-buffer benefit).
//!
//! [`FixedBuf::freeze`] turns a buffer into a [`FrozenFixedBuf`]: read-only, reference counted
//! and cheap to clone and [`slice`](FrozenFixedBuf::slice), still usable with the fixed write
//! ops. The buffer goes back to the pool when the last clone is dropped.
//!
//! The user-facing rules (buffer table, memory) are documented on [`FixedBufPool`].
//!
//! # Soundness
//! [`FixedBufPool`] handles and [`FixedBuf`]s point to one heap-allocated `Shared` through a
//! plain pointer, which counts both and is freed by whichever drops last. Freeing it clears the
//! pool's table slots, gives the slot range back to the table and only then unmaps the memory.
//! An op holds its `FixedBuf` by value, and the driver keeps the data of a dropped op until its
//! completion, so neither the memory nor the registration can go away while the kernel uses
//! them. `Shared` and the table hold a handle to the driver, so the ring outlives them too.
//! Everything is `!Send`.
//!
//! [`File::read_fixed_at`]: crate::fs::File::read_fixed_at
//! [`File::write_fixed_at`]: crate::fs::File::write_fixed_at
//! [`TcpStream::recv_fixed`]: crate::net::TcpStream::recv_fixed
//! [`TcpStream::send_fixed`]: crate::net::TcpStream::send_fixed

use std::{
    cell::{Cell, RefCell},
    fmt, io,
    ops::{Bound, Deref, DerefMut, Range, RangeBounds},
    ptr::{self, NonNull},
    rc::{Rc, Weak},
};

pub use super::op::fixed::{ReadFixedFuture, WriteFixedFuture};
use super::{uring::IoUringDriver, Inner, CURRENT};
use crate::buf::{IoBuf, IoBufMut};

/// The kernel's limit on the size of the buffer table (`IORING_MAX_REG_BUFFERS`).
const MAX_TABLE_SLOTS: u32 = 1 << 14;
/// The kernel's limit on the size of one registered buffer.
const MAX_BUF_SIZE: usize = 1 << 30;

/// State and counters of a [`FixedBufPool`], see [`FixedBufPool::stats`]. The counters
/// (`high_water`, `exhausted`) need the `metrics` feature.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FixedBufStats {
    /// Buffers currently handed out ([`FixedBuf`]s and frozen buffers alive, including those
    /// held by in-flight ops).
    pub in_use: usize,
    /// The highest `in_use` seen.
    #[cfg(feature = "metrics")]
    #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
    pub high_water: usize,
    /// The number of buffers in the pool.
    pub capacity: usize,
    /// Calls to [`FixedBufPool::get`] that found the pool empty.
    #[cfg(feature = "metrics")]
    #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
    pub exhausted: u64,
}

// ---------------------------------------------------------------------------------------------
// The per-ring buffer table.

/// The registered-buffer table of one ring, shared by all pools on it.
struct Table {
    driver: Inner,
    slots: u32,
    // Free slot ranges: sorted, disjoint, never adjacent.
    free: RefCell<Vec<Range<u32>>>,
}

thread_local! {
    // The tables of the rings of this thread. A live table holds its driver, so the driver
    // pointer used as the key cannot be reused while the entry is live.
    static TABLES: RefCell<Vec<Weak<Table>>> = const { RefCell::new(Vec::new()) };
}

impl Table {
    /// The table of `driver`'s ring, registered with `slots` slots if there is none yet.
    fn get_or_register(driver: &Inner, slots: u32) -> io::Result<Rc<Table>> {
        TABLES.with(|tables| {
            let mut tables = tables.borrow_mut();
            tables.retain(|t| t.strong_count() > 0);
            if let Some(table) = tables
                .iter()
                .filter_map(Weak::upgrade)
                .find(|t| Rc::ptr_eq(&t.driver.0, &driver.0))
            {
                return Ok(table);
            }
            driver
                .with_ring(|ring| ring.submitter().register_buffers_sparse(slots))
                .map_err(|e| {
                    if e.raw_os_error() == Some(libc::EBUSY) {
                        io::Error::new(
                            e.kind(),
                            format!(
                                "registering the fixed buffer table failed ({e}): the ring \
                                 already has registered buffers (raw register_buffers?)"
                            ),
                        )
                    } else {
                        e
                    }
                })?;
            let table = Rc::new(Table {
                driver: driver.clone(),
                slots,
                free: RefCell::new(std::iter::once(0..slots).collect()),
            });
            tables.push(Rc::downgrade(&table));
            Ok(table)
        })
    }

    /// Take `n` consecutive slots (first fit).
    fn alloc(&self, n: u32) -> Option<u32> {
        if n == 0 {
            return Some(0);
        }
        let mut free = self.free.borrow_mut();
        let pos = free.iter().position(|r| r.len() >= n as usize)?;
        let start = free[pos].start;
        free[pos].start += n;
        if free[pos].is_empty() {
            free.remove(pos);
        }
        Some(start)
    }

    /// Give back the slots `start..start + n`.
    fn release(&self, start: u32, n: u32) {
        if n == 0 {
            return;
        }
        let range = start..start + n;
        let mut free = self.free.borrow_mut();
        let pos = free.partition_point(|r| r.start < range.start);
        free.insert(pos, range);
        // Merge with the next, then with the previous range.
        if pos + 1 < free.len() && free[pos].end == free[pos + 1].start {
            free[pos].end = free[pos + 1].end;
            free.remove(pos + 1);
        }
        if pos > 0 && free[pos - 1].end == free[pos].start {
            free[pos - 1].end = free[pos].end;
            free.remove(pos);
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        // No pool (hence no buffer, hence no op using one) is left on this ring.
        let _ = self
            .driver
            .with_ring(|ring| ring.submitter().unregister_buffers());
    }
}

// ---------------------------------------------------------------------------------------------
// The pool.

/// A pool of `count` registered buffers of `size` bytes each, on one ring.
///
/// Get a buffer with [`get`](Self::get); it returns to the pool when dropped. The handle is
/// cheap to clone. The memory stays mapped and registered until the last handle and the last
/// buffer are gone (an in-flight op holds its buffer), so dropping the pool early is safe.
///
/// The fixed ops take the buffer by value like every rent op and use `IORING_OP_READ_FIXED` /
/// `IORING_OP_WRITE_FIXED`, which skip the per-op mapping of the user pages:
/// [`File::read_fixed_at`](crate::fs::File::read_fixed_at),
/// [`File::write_fixed_at`](crate::fs::File::write_fixed_at),
/// [`TcpStream::recv_fixed`](crate::net::TcpStream::recv_fixed),
/// [`TcpStream::send_fixed`](crate::net::TcpStream::send_fixed) (and their `_op` variants,
/// returning [`ReadFixedFuture`] / [`WriteFixedFuture`]). A [`FixedBuf`] is also an [`IoBuf`] and
/// [`IoBufMut`], so the ordinary ops accept it too. [`FixedBuf::freeze`] makes a cheap-to-clone,
/// sliceable [`FrozenFixedBuf`] that the fixed writes still accept.
///
/// # The buffer table
/// A ring has one table of registered buffers. The first pool created on a ring registers it
/// (sparse, [`FixedBufPool::DEFAULT_TABLE_SLOTS`] slots unless
/// [`with_table_size`](FixedBufPool::with_table_size) asked for more, and never fewer than the
/// pool needs), and every pool on that ring takes a range of slots from it, so pools never share
/// an index. [`grow`](FixedBufPool::grow) takes another range. When the last pool of a ring is
/// gone (with all its buffers), the table is unregistered.
///
/// While a pool exists the table belongs to the pools: do not call `register_buffers*` or
/// `unregister_buffers` through [`with_ring`](crate::uring::with_ring) then. Conversely,
/// creating a pool fails with `EBUSY` if buffers were registered through the raw API.
///
/// # Memory
/// Each pool (and each `grow`) is one page-aligned anonymous mapping, buffer `i` at
/// `i * buf_size`. Registration pins the pages, which faults them in and counts them against
/// `RLIMIT_MEMLOCK` (unless the process has `CAP_IPC_LOCK`): a too small limit fails with
/// `ENOMEM`. For `O_DIRECT` use a `buf_size` that is a multiple of 4096.
///
/// ```no_run
/// use monoio::{fs::File, uring::FixedBufPool};
///
/// # async fn demo() -> std::io::Result<()> {
/// let pool = FixedBufPool::new(16, 64 * 1024)?;
/// let file = File::open("data.bin").await?;
/// let buf = pool.get().expect("pool empty");
/// let (res, buf) = file.read_fixed_at(buf, 0).await;
/// println!("read {} bytes: {:?}", res?, &buf[..]);
/// # Ok(())
/// # }
/// ```
pub struct FixedBufPool(NonNull<Shared>);

struct Shared {
    driver: Inner,
    table: Rc<Table>,
    buf_size: usize,
    // Per buffer (pool-local id): its address and its table index. Addresses never move.
    bufs: RefCell<Vec<(NonNull<u8>, u16)>>,
    // Pool-local ids of the buffers in the pool.
    free: RefCell<Vec<u32>>,
    // Slot range and memory of `new` and of each `grow`.
    segments: RefCell<Vec<Segment>>,
    // `FixedBufPool` handles alive.
    handles: Cell<usize>,
    // `FixedBuf`s alive (= buffers in use).
    live: Cell<usize>,
    #[cfg(feature = "metrics")]
    high_water: Cell<usize>,
    #[cfg(feature = "metrics")]
    exhausted: Cell<u64>,
}

struct Segment {
    first_slot: u32,
    count: u32,
    // Unmapped after `Drop for Shared` cleared the slots.
    _map: Option<Mapping>,
}

impl FixedBufPool {
    /// The size of the buffer table registered on a ring by its first pool, unless
    /// [`with_table_size`](Self::with_table_size) asks for more.
    pub const DEFAULT_TABLE_SLOTS: u32 = 1024;

    /// Register `count` buffers of `size` bytes on the current runtime's ring.
    ///
    /// Fails with `InvalidInput` for a zero `size` or one above 1 GiB, with `ENOSPC` if the
    /// ring's buffer table has no `count` free consecutive slots, with `ENOMEM` if the memlock
    /// limit is too small, and with `EBUSY` if buffers were registered through the raw API.
    ///
    /// # Panics
    /// Panics if called outside of a monoio runtime.
    pub fn new(count: u16, size: usize) -> io::Result<Self> {
        Self::with_table_size(count, size, Self::DEFAULT_TABLE_SLOTS)
    }

    /// Like [`new`](Self::new); if this is the first pool on the ring, its buffer table gets
    /// `table_slots` slots (at least `count`, at most the kernel limit of 16384). Ignored if the
    /// table exists already.
    ///
    /// # Panics
    /// Panics if called outside of a monoio runtime.
    pub fn with_table_size(count: u16, size: usize, table_slots: u32) -> io::Result<Self> {
        CURRENT.with(|inner| Self::create(inner.clone(), count, size, table_slots))
    }

    /// Like [`new`](Self::new), on the ring of `driver`. Usable outside of the runtime.
    pub fn new_in(driver: &IoUringDriver, count: u16, size: usize) -> io::Result<Self> {
        Self::new_in_with_table_size(driver, count, size, Self::DEFAULT_TABLE_SLOTS)
    }

    /// Like [`with_table_size`](Self::with_table_size), on the ring of `driver`. Usable outside
    /// of the runtime.
    pub fn new_in_with_table_size(
        driver: &IoUringDriver,
        count: u16,
        size: usize,
        table_slots: u32,
    ) -> io::Result<Self> {
        Self::create(driver.handle(), count, size, table_slots)
    }

    fn create(driver: Inner, count: u16, size: usize, table_slots: u32) -> io::Result<Self> {
        if size == 0 || size > MAX_BUF_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer size must be between 1 and 1 GiB",
            ));
        }
        let slots = table_slots.max(u32::from(count)).min(MAX_TABLE_SLOTS);
        let table = Table::get_or_register(&driver, slots)?;
        let shared = Shared {
            driver,
            table,
            buf_size: size,
            bufs: RefCell::new(Vec::new()),
            free: RefCell::new(Vec::new()),
            segments: RefCell::new(Vec::new()),
            handles: Cell::new(1),
            live: Cell::new(0),
            #[cfg(feature = "metrics")]
            high_water: Cell::new(0),
            #[cfg(feature = "metrics")]
            exhausted: Cell::new(0),
        };
        // On failure `shared` is dropped here, which gives back what `add` took.
        shared.add(count)?;
        Ok(Self(NonNull::from(Box::leak(Box::new(shared)))))
    }

    fn shared(&self) -> &Shared {
        // SAFETY: `Shared` is alive while a handle exists (see the module docs).
        unsafe { self.0.as_ref() }
    }

    /// Take a buffer out of the pool, empty (`len() == 0`). `None` if all are in use.
    pub fn get(&self) -> Option<FixedBuf> {
        let s = self.shared();
        let id = s.free.borrow_mut().pop();
        metric!(if id.is_none() {
            s.exhausted.set(s.exhausted.get() + 1);
        });
        let id = id?;
        let (ptr, index) = s.bufs.borrow()[id as usize];
        let live = s.live.get() + 1;
        s.live.set(live);
        metric!(s.high_water.set(s.high_water.get().max(live)));
        Some(FixedBuf {
            shared: self.0,
            ptr,
            cap: s.buf_size,
            len: 0,
            index,
            id,
        })
    }

    /// Register `extra` more buffers. Buffers already registered (and in use) are untouched.
    ///
    /// Fails like [`new`](Self::new); the pool is unchanged then.
    pub fn grow(&self, extra: u16) -> io::Result<()> {
        self.shared().add(extra)
    }

    /// The size of each buffer.
    pub fn buf_size(&self) -> usize {
        self.shared().buf_size
    }

    /// The number of buffers in the pool (in use or not).
    pub fn capacity(&self) -> usize {
        self.shared().bufs.borrow().len()
    }

    /// The state (and, with the `metrics` feature, the counters) of this pool.
    pub fn stats(&self) -> FixedBufStats {
        let s = self.shared();
        FixedBufStats {
            in_use: s.live.get(),
            #[cfg(feature = "metrics")]
            high_water: s.high_water.get(),
            capacity: s.bufs.borrow().len(),
            #[cfg(feature = "metrics")]
            exhausted: s.exhausted.get(),
        }
    }

    /// The number of slots of the ring's buffer table, shared by all pools on the ring.
    pub fn table_slots(&self) -> u32 {
        self.shared().table.slots
    }

    /// The table indexes of this pool's buffers, as ranges (one per `new`/`grow`).
    pub fn index_ranges(&self) -> Vec<Range<u16>> {
        self.shared()
            .segments
            .borrow()
            .iter()
            .filter(|s| s.count > 0)
            .map(|s| s.first_slot as u16..(s.first_slot + s.count) as u16)
            .collect()
    }
}

impl Clone for FixedBufPool {
    fn clone(&self) -> Self {
        let s = self.shared();
        s.handles.set(s.handles.get() + 1);
        Self(self.0)
    }
}

impl Drop for FixedBufPool {
    fn drop(&mut self) {
        let s = self.shared();
        s.handles.set(s.handles.get() - 1);
        if s.handles.get() == 0 && s.live.get() == 0 {
            // SAFETY: this was the last pointer to `Shared`.
            unsafe { Shared::free(self.0) };
        }
    }
}

impl fmt::Debug for FixedBufPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBufPool")
            .field("buf_size", &self.buf_size())
            .field("indexes", &self.index_ranges())
            .field("stats", &self.stats())
            .finish()
    }
}

impl Shared {
    /// Map and register `n` more buffers.
    fn add(&self, n: u16) -> io::Result<()> {
        let n32 = u32::from(n);
        if n == 0 {
            return Ok(());
        }
        let len = (n as usize)
            .checked_mul(self.buf_size)
            .filter(|len| *len <= isize::MAX as usize)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "pool too large"))?;
        // No room in the table: see `FixedBufPool::with_table_size`.
        let first = self
            .table
            .alloc(n32)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOSPC))?;
        let map = match Mapping::anon(len) {
            Ok(map) => map,
            Err(e) => {
                self.table.release(first, n32);
                return Err(e);
            }
        };
        let iovecs: Vec<libc::iovec> = (0..n as usize)
            .map(|i| libc::iovec {
                // SAFETY: inside the mapping.
                iov_base: unsafe { map.ptr.as_ptr().add(i * self.buf_size) }.cast(),
                iov_len: self.buf_size,
            })
            .collect();
        // SAFETY: the memory stays mapped until the slots are cleared again (`Drop for Shared`),
        // which happens only once no buffer of the pool is alive.
        let res = self.driver.with_ring(|ring| unsafe {
            ring.submitter()
                .register_buffers_update(first, &iovecs, None)
        });
        if let Err(e) = res {
            // The update registers all or nothing.
            self.table.release(first, n32);
            return Err(e);
        }

        let mut bufs = self.bufs.borrow_mut();
        let mut free = self.free.borrow_mut();
        let base = bufs.len() as u32;
        for (i, iov) in iovecs.iter().enumerate() {
            let ptr = NonNull::new(iov.iov_base.cast()).expect("mapping is non-null");
            bufs.push((ptr, (first + i as u32) as u16));
        }
        // Hand out low indexes first.
        free.extend((base..base + n32).rev());
        self.segments.borrow_mut().push(Segment {
            first_slot: first,
            count: n32,
            _map: Some(map),
        });
        Ok(())
    }

    /// A `FixedBuf` with pool-local id `id` was dropped.
    ///
    /// # Safety
    /// `this` must be live; it may be freed by this call.
    unsafe fn release(this: NonNull<Shared>, id: u32) {
        // SAFETY: live per the contract; the reference ends before `free`.
        let s = unsafe { this.as_ref() };
        s.free.borrow_mut().push(id);
        s.live.set(s.live.get() - 1);
        if s.handles.get() == 0 && s.live.get() == 0 {
            // SAFETY: no handle and no buffer is left: this was the last pointer.
            unsafe { Shared::free(this) };
        }
    }

    /// # Safety
    /// `this` must come from `FixedBufPool::create` and no pointer to it may be used afterwards.
    unsafe fn free(this: NonNull<Shared>) {
        // SAFETY: per the contract.
        drop(unsafe { Box::from_raw(this.as_ptr()) });
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // No buffer is alive, so no op uses one. Clear the slots (an iovec with a null base
        // empties a slot), give them back to the table, then the mappings are unmapped. If
        // this is the last pool of the ring, dropping `table` afterwards unregisters the table.
        for seg in self.segments.get_mut().iter() {
            let empty = vec![
                libc::iovec {
                    iov_base: ptr::null_mut(),
                    iov_len: 0,
                };
                seg.count as usize
            ];
            // SAFETY: empty slots point to no memory.
            let _ = self.driver.with_ring(|ring| unsafe {
                ring.submitter()
                    .register_buffers_update(seg.first_slot, &empty, None)
            });
            self.table.release(seg.first_slot, seg.count);
        }
    }
}

/// An owned anonymous mapping, unmapped on drop.
struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
}

impl Mapping {
    // Private anonymous memory: zeroed and page aligned.
    fn anon(len: usize) -> io::Result<Self> {
        // SAFETY: a fresh mapping at an address of the kernel's choice; nothing is overwritten.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: NonNull::new(ptr.cast()).expect("mmap returned null"),
            len,
        })
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the mapping is ours and nothing points into it any more.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

// ---------------------------------------------------------------------------------------------
// Buffers.

/// A registered buffer taken from a [`FixedBufPool`]; returned to the pool on drop.
///
/// It has a capacity (the pool's buffer size) and a length: `Deref` gives the first `len()`
/// bytes. Reads (fixed or not) fill it from the start and set the length; writes send the first
/// `len()` bytes. The memory is always initialized (zeroed at first), so
/// [`set_len`](Self::set_len) is safe.
///
/// Use it with the fixed ops ([`File::read_fixed_at`](crate::fs::File::read_fixed_at), ...) to
/// skip the per-op page mapping, or with any ordinary op through [`IoBuf`]/[`IoBufMut`].
pub struct FixedBuf {
    shared: NonNull<Shared>,
    ptr: NonNull<u8>,
    cap: usize,
    len: usize,
    index: u16,
    id: u32,
}

impl FixedBuf {
    /// The index of this buffer in the ring's registered-buffer table (the SQE's `buf_index`).
    #[inline]
    pub fn buf_index(&self) -> u16 {
        self.index
    }

    /// The buffer size.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// The number of valid bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether `len() == 0`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Set the length. The bytes are whatever the buffer held before.
    ///
    /// # Panics
    /// If `len > capacity()`.
    #[inline]
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.cap, "length {len} > capacity {}", self.cap);
        self.len = len;
    }

    /// Set the length to 0.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Append `data`.
    ///
    /// # Panics
    /// If it does not fit in the remaining capacity.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let end = self.len + data.len();
        assert!(end <= self.cap, "{end} bytes > capacity {}", self.cap);
        // SAFETY: `len..end` is inside the buffer, which no op uses (we own it).
        unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr().add(self.len), data.len())
        };
        self.len = end;
    }

    /// The whole buffer, `capacity()` bytes, regardless of the length.
    pub fn as_full_mut(&mut self) -> &mut [u8] {
        // SAFETY: the buffer is initialized memory of `cap` bytes that only we use.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.cap) }
    }

    /// Make the buffer read-only and shareable: the first `len()` bytes become a
    /// [`FrozenFixedBuf`].
    pub fn freeze(self) -> FrozenFixedBuf {
        let len = self.len;
        FrozenFixedBuf {
            buf: Rc::new(self),
            start: 0,
            len,
        }
    }

    /// Whether the buffer is registered on the ring of `driver`.
    pub(crate) fn registered_on(&self, driver: &Inner) -> bool {
        // SAFETY: `Shared` is alive while a buffer exists.
        let s = unsafe { self.shared.as_ref() };
        Rc::ptr_eq(&s.driver.0, &driver.0)
    }
}

impl Deref for FixedBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: `len <= cap` initialized bytes that no op writes while we hold the buffer.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for FixedBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`, and we own the buffer.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl AsRef<[u8]> for FixedBuf {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for FixedBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

// SAFETY: the pointer is stable (the mapping never moves and stays mapped while the buffer
// lives) and `len` bytes are initialized.
unsafe impl IoBuf for FixedBuf {
    #[inline]
    fn read_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        self.len
    }
}

// SAFETY: as for `IoBuf`; `cap` bytes are writable.
unsafe impl IoBufMut for FixedBuf {
    #[inline]
    fn write_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    fn bytes_total(&mut self) -> usize {
        self.cap
    }

    #[inline]
    unsafe fn set_init(&mut self, pos: usize) {
        self.len = pos.min(self.cap);
    }
}

impl fmt::Debug for FixedBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBuf")
            .field("buf_index", &self.index)
            .field("len", &self.len)
            .field("capacity", &self.cap)
            .finish()
    }
}

impl Drop for FixedBuf {
    fn drop(&mut self) {
        // SAFETY: `Shared` is alive while a buffer exists; `self.shared` is not used afterwards.
        unsafe { Shared::release(self.shared, self.id) };
    }
}

/// A frozen [`FixedBuf`]: read-only, reference counted (single thread), a view of a range of
/// the buffer. Cloning and [`slice`](Self::slice) are cheap and copy nothing; the buffer goes
/// back to the pool when the last view is dropped.
///
/// Implements [`IoBuf`] and works with the fixed write ops
/// ([`File::write_fixed_at`](crate::fs::File::write_fixed_at),
/// [`TcpStream::send_fixed`](crate::net::TcpStream::send_fixed)): a range of a registered
/// buffer is still registered.
#[derive(Clone)]
pub struct FrozenFixedBuf {
    buf: Rc<FixedBuf>,
    start: usize,
    len: usize,
}

impl FrozenFixedBuf {
    /// The index of the underlying buffer in the ring's registered-buffer table.
    #[inline]
    pub fn buf_index(&self) -> u16 {
        self.buf.index
    }

    /// The length of this view.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the view is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Narrow the view to `range` (relative to this view), without copying.
    ///
    /// This shadows [`IoBuf::slice`]: the result is again a `FrozenFixedBuf`, so it still uses
    /// the fixed ops.
    ///
    /// # Panics
    /// If the range is out of bounds.
    pub fn slice(mut self, range: impl RangeBounds<usize>) -> FrozenFixedBuf {
        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len,
        };
        assert!(
            begin <= end && end <= self.len,
            "range {begin}..{end} out of bounds of a view of {} bytes",
            self.len
        );
        self.start += begin;
        self.len = end - begin;
        self
    }

    /// Like [`slice`](Self::slice), on a clone.
    pub fn slice_ref(&self, range: impl RangeBounds<usize>) -> FrozenFixedBuf {
        self.clone().slice(range)
    }

    /// The number of views of the buffer alive (this one included).
    pub fn ref_count(&self) -> usize {
        Rc::strong_count(&self.buf)
    }

    /// Get the buffer back if this is its only view. Its length is the length at
    /// [`FixedBuf::freeze`] (the view's range is forgotten).
    pub fn try_into_mut(self) -> Result<FixedBuf, FrozenFixedBuf> {
        let (start, len) = (self.start, self.len);
        Rc::try_unwrap(self.buf).map_err(|buf| FrozenFixedBuf { buf, start, len })
    }

    pub(crate) fn fixed(&self) -> &FixedBuf {
        &self.buf
    }
}

impl Deref for FrozenFixedBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.buf[self.start..self.start + self.len]
    }
}

impl AsRef<[u8]> for FrozenFixedBuf {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

// SAFETY: the bytes are inside the underlying buffer, which lives (and never moves) while a view
// exists, and nothing writes to a frozen buffer.
unsafe impl IoBuf for FrozenFixedBuf {
    #[inline]
    fn read_ptr(&self) -> *const u8 {
        // SAFETY: `start + len <= buf.len`.
        unsafe { self.buf.ptr.as_ptr().add(self.start) }
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        self.len
    }
}

impl fmt::Debug for FrozenFixedBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrozenFixedBuf")
            .field("buf_index", &self.buf.index)
            .field("start", &self.start)
            .field("len", &self.len)
            .field("refs", &self.ref_count())
            .finish()
    }
}

mod sealed {
    pub trait Sealed {
        fn fixed(&self) -> &super::FixedBuf;
    }
}

/// A buffer the fixed write ops accept: [`FixedBuf`] or [`FrozenFixedBuf`]. Sealed.
pub trait FixedIoBuf: IoBuf + sealed::Sealed {
    /// The index of the buffer in the ring's registered-buffer table.
    fn buf_index(&self) -> u16 {
        self.fixed().index
    }
}

impl sealed::Sealed for FixedBuf {
    fn fixed(&self) -> &FixedBuf {
        self
    }
}

impl FixedIoBuf for FixedBuf {}

impl sealed::Sealed for FrozenFixedBuf {
    fn fixed(&self) -> &FixedBuf {
        FrozenFixedBuf::fixed(self)
    }
}

impl FixedIoBuf for FrozenFixedBuf {}

/// Whether `buf` is registered on the current ring: the fixed opcodes are used only then.
pub(crate) fn on_current_ring<B: FixedIoBuf>(buf: &B) -> bool {
    CURRENT.is_set() && CURRENT.with(|inner| buf.fixed().registered_on(inner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeBuilder;

    fn table(slots: u32) -> Table {
        let rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
        Table {
            driver: rt.driver().handle(),
            slots,
            free: RefCell::new(std::iter::once(0..slots).collect()),
        }
    }

    #[test]
    fn range_allocator_merges() {
        let t = table(10);
        let a = t.alloc(3).unwrap();
        let b = t.alloc(3).unwrap();
        let c = t.alloc(4).unwrap();
        assert_eq!((a, b, c), (0, 3, 6));
        assert_eq!(t.alloc(1), None);
        t.release(b, 3);
        assert_eq!(t.alloc(4), None);
        t.release(a, 3);
        assert_eq!(*t.free.borrow(), vec![0..6]);
        t.release(c, 4);
        assert_eq!(*t.free.borrow(), vec![0..10]);
        assert_eq!(t.alloc(10), Some(0));
    }
}
