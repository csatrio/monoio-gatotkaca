//! Provided buffer rings: `IORING_REGISTER_PBUF_RING` with `IOSQE_BUFFER_SELECT` (Linux 5.19+).
//!
//! The kernel picks a buffer from a registered pool when data arrives, instead of the caller
//! handing one over per request. That lets many idle connections (or a multishot receive) share
//! one pool.

use std::{
    alloc::{self, Layout},
    cell::RefCell,
    io,
    ops::Deref,
    ptr::NonNull,
    rc::Rc,
    sync::atomic::{AtomicU16, Ordering},
};

use io_uring::types::BufRingEntry;

use super::{raw::Cqe, uring::IoUringDriver, Inner, CURRENT};

/// The kernel requires the ring to be page aligned.
const PAGE_SIZE: usize = 4096;

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
#[derive(Clone)]
pub struct BufRing(Rc<Shared>);

struct Shared {
    driver: Inner,
    bgid: u16,
    mask: u16,
    buf_size: usize,
    ring: NonNull<BufRingEntry>,
    layout: Layout,
    // The pool. Never reallocated, the kernel holds its addresses.
    data: Box<[u8]>,
    state: RefCell<State>,
}

struct State {
    // Next free ring slot, as published to the kernel (wraps).
    tail: u16,
    // `held[bid]`: a `SelectedBuf` has this buffer. Otherwise it is in the ring or in the kernel.
    held: Vec<bool>,
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
        CURRENT.with(|inner| Self::create(inner.clone(), bgid, entries, buf_size))
    }

    /// Like [`new`](Self::new), on the ring of `driver`. Usable outside of the runtime.
    pub fn new_in(
        driver: &IoUringDriver,
        bgid: u16,
        entries: u16,
        buf_size: usize,
    ) -> io::Result<Self> {
        Self::create(driver.handle(), bgid, entries, buf_size)
    }

    fn create(driver: Inner, bgid: u16, entries: u16, buf_size: usize) -> io::Result<Self> {
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

        let layout = Layout::from_size_align(
            entries as usize * std::mem::size_of::<BufRingEntry>(),
            PAGE_SIZE,
        )
        .map_err(|_| invalid("ring too large"))?;
        // Zeroed: the tail lives in the first entry.
        let ring = NonNull::new(unsafe { alloc::alloc_zeroed(layout) }.cast::<BufRingEntry>())
            .ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let data = vec![0u8; pool_len].into_boxed_slice();

        // Put every buffer in the ring.
        let base = data.as_ptr() as u64;
        for bid in 0..entries {
            let entry = unsafe { &mut *ring.as_ptr().add(bid as usize) };
            entry.set_addr(base + bid as u64 * buf_size as u64);
            entry.set_len(buf_size as u32);
            entry.set_bid(bid);
        }
        unsafe { tail_ptr(ring).store(entries, Ordering::Release) };

        // SAFETY: `ring` and `data` stay valid until `Shared` is dropped, which unregisters first.
        let registered = driver.with_ring(|ring_fd| unsafe {
            ring_fd
                .submitter()
                .register_buf_ring_with_flags(ring.as_ptr() as u64, entries, bgid, 0)
        });
        if let Err(e) = registered {
            unsafe { alloc::dealloc(ring.as_ptr().cast(), layout) };
            return Err(e);
        }

        Ok(Self(Rc::new(Shared {
            driver,
            bgid,
            mask: entries - 1,
            buf_size,
            ring,
            layout,
            data,
            state: RefCell::new(State {
                tail: entries,
                held: vec![false; entries as usize],
            }),
        })))
    }

    /// The group id, to set as `buf_group` on the SQE.
    pub fn group(&self) -> u16 {
        self.0.bgid
    }

    /// The size of each buffer.
    pub fn buf_size(&self) -> usize {
        self.0.buf_size
    }

    /// Take the buffer the kernel selected for `cqe`.
    ///
    /// Returns `None` if the completion carries no buffer (error, or nothing was read), or if
    /// that buffer is already taken. Call it for every completion of this group that carries a
    /// buffer, even if you ignore the data: the buffer only goes back to the ring when the
    /// returned [`SelectedBuf`] is dropped, and the pool runs dry otherwise (receives then fail
    /// with `ENOBUFS`).
    pub fn take(&self, cqe: Cqe) -> Option<SelectedBuf> {
        let bid = cqe.buffer_id()?;
        let len = usize::try_from(cqe.res()).ok()?;
        let mut state = self.0.state.borrow_mut();
        let held = state.held.get_mut(bid as usize)?;
        if std::mem::replace(held, true) {
            return None;
        }
        Some(SelectedBuf {
            ring: self.clone(),
            bid,
            len: len.min(self.0.buf_size),
        })
    }
}

impl Shared {
    fn buf(&self, bid: u16) -> &[u8] {
        let start = bid as usize * self.buf_size;
        &self.data[start..start + self.buf_size]
    }

    // Give the buffer back to the kernel.
    fn recycle(&self, bid: u16) {
        let mut state = self.state.borrow_mut();
        state.held[bid as usize] = false;

        // The slot is free: each buffer is in at most one place, and there are as many slots as
        // buffers.
        let slot = unsafe { &mut *self.ring.as_ptr().add((state.tail & self.mask) as usize) };
        slot.set_addr(self.buf(bid).as_ptr() as u64);
        slot.set_len(self.buf_size as u32);
        slot.set_bid(bid);

        state.tail = state.tail.wrapping_add(1);
        unsafe { tail_ptr(self.ring).store(state.tail, Ordering::Release) };
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // The kernel stops using the ring and the pool once this returns.
        let _ = self
            .driver
            .with_ring(|ring| ring.submitter().unregister_buf_ring(self.bgid));
        unsafe { alloc::dealloc(self.ring.as_ptr().cast(), self.layout) };
    }
}

/// The ring tail, shared with the kernel: it is the `resv` field of the first entry.
///
/// # Safety
/// `ring` must point to a live, initialized ring.
unsafe fn tail_ptr<'a>(ring: NonNull<BufRingEntry>) -> &'a AtomicU16 {
    AtomicU16::from_ptr(BufRingEntry::tail(ring.as_ptr()).cast_mut())
}

/// A buffer selected by the kernel, holding the received bytes (`Deref<Target = [u8]>`).
/// Dropping it returns the buffer to the ring.
pub struct SelectedBuf {
    ring: BufRing,
    bid: u16,
    len: usize,
}

impl SelectedBuf {
    /// The buffer id.
    pub fn bid(&self) -> u16 {
        self.bid
    }
}

impl Deref for SelectedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.ring.0.buf(self.bid)[..self.len]
    }
}

impl AsRef<[u8]> for SelectedBuf {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for SelectedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectedBuf")
            .field("group", &self.ring.0.bgid)
            .field("bid", &self.bid)
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for SelectedBuf {
    fn drop(&mut self) {
        self.ring.0.recycle(self.bid);
    }
}
