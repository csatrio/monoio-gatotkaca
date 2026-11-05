//! The fixed (registered) file table: `IORING_REGISTER_FILES2` (sparse) plus
//! `IORING_REGISTER_FILES_UPDATE`, and direct descriptors (Linux 5.19+).
//!
//! An SQE normally names its file by fd, so the kernel looks the fd up and takes a reference
//! (`fget`/`fput`, an atomic refcount) on every operation. With `IOSQE_FIXED_FILE` the SQE names
//! a slot of the ring's file table instead, which skips that work.
//!
//! [`RuntimeBuilder::with_fixed_files`](crate::RuntimeBuilder::with_fixed_files) registers an
//! empty table when the runtime is built. Its slots are handed out by the runtime:
//! `register_fixed` (on [`TcpStream`](crate::net::TcpStream), `TcpListener`, `UnixStream`,
//! `File`) puts an open fd into a free slot (the fd stays open: "registered" mode), and
//! [`TcpListener::accept_direct`](crate::net::TcpListener::accept_direct) lets the kernel install
//! the new socket straight into a free slot (no fd at all: "direct-only" mode).
//! [`RuntimeBuilder::with_fixed_files_kernel_alloc`](crate::RuntimeBuilder::with_fixed_files_kernel_alloc)
//! appends slots that the kernel allocates itself (`IORING_FILE_INDEX_ALLOC`, restricted to them
//! with `IORING_REGISTER_FILE_ALLOC_RANGE`), which multishot direct accept needs. The two ranges
//! never overlap, so the runtime's allocator and the kernel's never pick the same slot.
//!
//! # Slot lifetime
//! A slot belongs to one `SharedFd`. Every op holds a clone of it, and the driver keeps the data
//! of a dropped op until its completion, so the slot is only closed once no op uses it. Closing
//! pushes `IORING_OP_CLOSE` on the slot; the slot goes back to the free list when that close
//! completes (the op data's drop), never earlier, so a new file can't be put into a slot that a
//! queued close would then clear. If the close can't be pushed, the slot is cleared
//! synchronously (`IORING_REGISTER_FILES_UPDATE` with `-1`) before it is freed.
//!
//! Paths that run inside the driver (dropping the data of an op nobody waits for, discarding a
//! multishot CQE) only call `io_uring_register(2)` on the ring fd and touch the table's own
//! cells, never the driver.

use std::{
    cell::{Cell, RefCell, UnsafeCell},
    fmt, io,
    os::unix::io::RawFd,
    rc::{Rc, Weak},
};

use super::{
    uring::{IoUringDriver, UringInner},
    Inner, CURRENT,
};

/// The kernel's limit on the size of the file table (`IORING_MAX_FIXED_FILES`).
const MAX_TABLE_SLOTS: u32 = 1 << 20;

const REGISTER_FILES_UPDATE: libc::c_uint = 6;
const REGISTER_FILE_ALLOC_RANGE: libc::c_uint = 25;

/// `struct io_uring_files_update`.
#[repr(C)]
struct FilesUpdate {
    offset: u32,
    resv: u32,
    fds: u64,
}

/// `struct io_uring_file_index_range`.
#[repr(C)]
struct FileIndexRange {
    off: u32,
    len: u32,
    resv: u64,
}

/// Counters of the current ring's fixed file table, see [`fixed_file_stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FixedFileStats {
    /// Slots the runtime hands out (`register_fixed`, `accept_direct`):
    /// [`with_fixed_files`](crate::RuntimeBuilder::with_fixed_files).
    pub capacity: u32,
    /// Runtime slots in use, including those whose close is still in flight and those reserved
    /// by an `accept_direct` that has not completed.
    pub in_use: u32,
    /// The highest `in_use` seen.
    pub high_water: u32,
    /// Slots the kernel allocates (multishot direct accept):
    /// [`with_fixed_files_kernel_alloc`](crate::RuntimeBuilder::with_fixed_files_kernel_alloc).
    pub kernel_capacity: u32,
    /// Kernel-allocated slots owned by a stream (or whose close is in flight).
    pub kernel_in_use: u32,
    /// Requests for a runtime slot that found the table full.
    pub exhausted: u64,
}

/// The fixed file table of one ring.
pub(crate) struct FileTable {
    // The ring: identity, and whether it still exists. Weak, so the table never keeps the
    // driver alive (the registry and the streams hold the table).
    driver: Weak<UnsafeCell<UringInner>>,
    ring_fd: RawFd,
    // Runtime slots are `0..user`, kernel-allocated ones `user..user + kernel`.
    user: u32,
    kernel: u32,
    // Free runtime slots, used as a stack (the lowest slot is handed out first at the start).
    free: RefCell<Vec<u32>>,
    stats: Cell<FixedFileStats>,
}

thread_local! {
    // The file tables of the rings of this thread. Entries whose ring is gone are pruned.
    static TABLES: RefCell<Vec<Rc<FileTable>>> = const { RefCell::new(Vec::new()) };
}

impl FileTable {
    /// Register a sparse table of `user + kernel` slots on `driver`'s ring. Called by the
    /// builder right after the ring is built, before anything else uses it.
    pub(crate) fn register(driver: &Inner, user: u32, kernel: u32) -> io::Result<()> {
        let total = user
            .checked_add(kernel)
            .filter(|t| *t > 0 && *t <= MAX_TABLE_SLOTS)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("the fixed file table needs 1 to {MAX_TABLE_SLOTS} slots"),
                )
            })?;
        let ring_fd = driver.with_ring(|ring| {
            use std::os::unix::io::AsRawFd;
            ring.as_raw_fd()
        });
        driver
            .with_ring(|ring| ring.submitter().register_files_sparse(total))
            .map_err(|e| {
                let hint = match e.raw_os_error() {
                    Some(libc::EBUSY) => "the ring already has registered files",
                    Some(libc::EMFILE) => "more slots than RLIMIT_NOFILE allows",
                    Some(libc::EINVAL) => "sparse file tables need Linux 5.19+",
                    _ => "",
                };
                io::Error::new(
                    e.kind(),
                    format!("registering the fixed file table failed ({e}) {hint}"),
                )
            })?;
        // Keep the kernel's own allocations (IORING_FILE_INDEX_ALLOC) out of the runtime's
        // slots. With `kernel == 0` the range is empty and the kernel allocates nothing.
        let range = FileIndexRange {
            off: user,
            len: kernel,
            resv: 0,
        };
        // SAFETY: a valid ring fd and a valid `io_uring_file_index_range`.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                ring_fd,
                REGISTER_FILE_ALLOC_RANGE,
                &range as *const FileIndexRange,
                0,
            )
        };
        if ret < 0 && kernel > 0 {
            // Linux 6.0+. Without it kernel allocations could land in runtime slots.
            let e = io::Error::last_os_error();
            let _ = driver.with_ring(|ring| ring.submitter().unregister_files());
            return Err(io::Error::new(
                e.kind(),
                format!("restricting kernel file allocation failed ({e}), needs Linux 6.0+"),
            ));
        }
        let table = Rc::new(FileTable {
            driver: Rc::downgrade(&driver.0),
            ring_fd,
            user,
            kernel,
            free: RefCell::new((0..user).rev().collect()),
            stats: Cell::new(FixedFileStats {
                capacity: user,
                kernel_capacity: kernel,
                ..Default::default()
            }),
        });
        TABLES.with(|tables| {
            let mut tables = tables.borrow_mut();
            tables.retain(|t| t.alive());
            tables.push(table);
        });
        Ok(())
    }

    /// The table of the current thread's running ring, if it has one.
    pub(crate) fn current() -> Option<Rc<FileTable>> {
        if !CURRENT.is_set() {
            return None;
        }
        CURRENT.with(|inner| {
            TABLES.with(|tables| {
                let tables = tables.borrow();
                tables
                    .iter()
                    .find(|t| std::ptr::eq(t.driver.as_ptr(), Rc::as_ptr(&inner.0)))
                    .cloned()
            })
        })
    }

    /// The table of the current ring, or the error to return when there is none.
    pub(crate) fn current_or_err() -> io::Result<Rc<FileTable>> {
        Self::current().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "no fixed file table on this ring (see RuntimeBuilder::with_fixed_files)",
            )
        })
    }

    /// Whether the ring still exists.
    fn alive(&self) -> bool {
        self.driver.strong_count() > 0
    }

    /// The ring's driver, if it still exists.
    pub(crate) fn driver(&self) -> Option<Inner> {
        self.driver.upgrade().map(Inner)
    }

    /// Whether the kernel may allocate slots (multishot direct accept).
    pub(crate) fn has_kernel_range(&self) -> bool {
        self.kernel > 0
    }

    fn update_stats(&self, f: impl FnOnce(&mut FixedFileStats)) {
        let mut s = self.stats.get();
        f(&mut s);
        self.stats.set(s);
    }

    pub(crate) fn stats(&self) -> FixedFileStats {
        self.stats.get()
    }

    /// Take a free runtime slot; `ENFILE` ("Too many open files in system") when the table is
    /// full.
    pub(crate) fn alloc(&self) -> io::Result<u32> {
        match self.free.borrow_mut().pop() {
            Some(slot) => {
                self.update_stats(|s| {
                    s.in_use += 1;
                    s.high_water = s.high_water.max(s.in_use);
                });
                Ok(slot)
            }
            None => {
                self.update_stats(|s| s.exhausted += 1);
                Err(io::Error::from_raw_os_error(libc::ENFILE))
            }
        }
    }

    /// Account for a slot the kernel allocated (and that a stream now owns).
    pub(crate) fn adopt_kernel_slot(&self) {
        self.update_stats(|s| s.kernel_in_use += 1);
    }

    /// Give `slot` back. The slot must be empty or about to be overwritten only by the runtime.
    pub(crate) fn release(&self, slot: u32) {
        if slot < self.user {
            self.free.borrow_mut().push(slot);
            self.update_stats(|s| s.in_use -= 1);
        } else {
            self.update_stats(|s| s.kernel_in_use = s.kernel_in_use.saturating_sub(1));
        }
    }

    /// Put `fd` into `slot` (or clear it with `-1`, which also drops the kernel's reference to
    /// the file there). Synchronous: one `io_uring_register(2)`. Does nothing if the ring is
    /// gone (its table went with it).
    pub(crate) fn update_sync(&self, slot: u32, fd: RawFd) -> io::Result<()> {
        if !self.alive() {
            return Ok(());
        }
        let fds = [fd];
        let up = FilesUpdate {
            offset: slot,
            resv: 0,
            fds: fds.as_ptr() as u64,
        };
        // SAFETY: the ring is alive, so `ring_fd` is its fd; `up` and `fds` outlive the call.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd,
                REGISTER_FILES_UPDATE,
                &up as *const FilesUpdate,
                1,
            )
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Clear `slot` synchronously and give it back.
    pub(crate) fn clear_and_release(&self, slot: u32) {
        let _ = self.update_sync(slot, -1);
        self.release(slot);
    }
}

impl fmt::Debug for FileTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileTable")
            .field("slots", &self.user)
            .field("kernel_slots", &self.kernel)
            .field("stats", &self.stats.get())
            .finish()
    }
}

/// A fixed slot owned by a `SharedFd`.
#[derive(Debug)]
pub(crate) struct FixedSlot {
    pub(crate) table: Rc<FileTable>,
    pub(crate) slot: u32,
}

/// Returns a slot to its table when dropped: held by the data of the op that closes or fills
/// the slot, so it runs once the kernel is done with that op.
#[derive(Debug)]
pub(crate) struct SlotRelease {
    table: Rc<FileTable>,
    slot: u32,
    // Clear the slot first (the op may have filled it and nobody took the result).
    clear: bool,
}

impl SlotRelease {
    /// Release after the slot was closed by the op holding this.
    pub(crate) fn after_close(fixed: FixedSlot) -> Self {
        Self {
            table: fixed.table,
            slot: fixed.slot,
            clear: false,
        }
    }

    /// A freshly allocated slot that an op may fill: cleared and released on drop unless
    /// [`claim`](Self::claim)ed.
    pub(crate) fn reserved(table: Rc<FileTable>, slot: u32) -> Self {
        Self {
            table,
            slot,
            clear: true,
        }
    }

    pub(crate) fn slot(&self) -> u32 {
        self.slot
    }

    /// The op filled the slot and the caller takes it over.
    pub(crate) fn claim(self) -> FixedSlot {
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: `this` is never dropped, so the table is moved out exactly once.
        let table = unsafe { std::ptr::read(&this.table) };
        FixedSlot {
            table,
            slot: this.slot,
        }
    }

    /// Clear the slot synchronously on drop (the op that should have closed it never ran).
    pub(crate) fn clear_on_drop(&mut self) {
        self.clear = true;
    }
}

impl Drop for SlotRelease {
    fn drop(&mut self) {
        if self.clear {
            self.table.clear_and_release(self.slot);
        } else {
            self.table.release(self.slot);
        }
    }
}

/// Register the fixed file table of a freshly built driver (`RuntimeBuilder`).
pub(crate) fn register_fixed_files(
    driver: &IoUringDriver,
    user: u32,
    kernel: u32,
) -> io::Result<()> {
    FileTable::register(&driver.handle(), user, kernel)
}

/// The counters of the current ring's fixed file table, or `None` if the ring has none (or
/// this is called outside of a runtime).
pub fn fixed_file_stats() -> Option<FixedFileStats> {
    FileTable::current().map(|t| t.stats())
}
