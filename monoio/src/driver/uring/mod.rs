//! Monoio Uring Driver.

use std::{
    cell::UnsafeCell,
    io,
    mem::ManuallyDrop,
    os::unix::prelude::{AsRawFd, RawFd},
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};

use io_uring::{opcode, squeue, types, types::Timespec, EnterFlags, IoUring};
use lifecycle::MaybeFdLifecycle;
pub(crate) use lifecycle::ZcFirst;

use super::{
    op::{CompletionMeta, Op, OpAble, OpKind},
    raw::Cqe,
    util::timespec,
    Driver, Inner, CURRENT,
};
use crate::utils::slab::Slab;

mod ignored;
mod lifecycle;
#[cfg(feature = "sync")]
mod waker;
#[cfg(feature = "sync")]
pub(crate) use waker::UnparkHandle;

pub(crate) const TIMEOUT_USERDATA: u64 = u64::MAX - 1;
#[cfg(feature = "sync")]
pub(crate) const EVENTFD_USERDATA: u64 = u64::MAX - 2;
/// `user_data` of the driver's own `LINK_TIMEOUT` SQEs: their CQEs are only counted.
const LINK_TIMEOUT_USERDATA: u64 = u64::MAX - 16;

// `IORING_SETUP_*` flags (the `io-uring` crate has no accessor for them).
const SETUP_IOPOLL: u32 = 1 << 0;
const SETUP_SQPOLL: u32 = 1 << 1;
const SETUP_COOP_TASKRUN: u32 = 1 << 8;
const SETUP_TASKRUN_FLAG: u32 = 1 << 9;
const SETUP_DEFER_TASKRUN: u32 = 1 << 13;

// `io_uring_register` opcodes for ring fd registration.
const REGISTER_RING_FDS: libc::c_uint = 20;
const UNREGISTER_RING_FDS: libc::c_uint = 21;

/// `IORING_CQE_F_NOTIF`: the notification CQE of a zero-copy send.
const CQE_F_NOTIF: u32 = 1 << 3;

/// How often a full SQ is flushed before pushing an SQE gives up.
const PUSH_RETRIES: usize = 8;

/// The file a cancel-by-fd targets.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub(crate) enum CancelTarget {
    /// A plain file descriptor.
    Fd(RawFd),
    /// A slot of the ring's registered file table.
    Fixed(u32),
}

/// Extra driver settings from the runtime builder.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DriverOptions {
    /// Register the ring fd (`IORING_REGISTER_RING_FDS`).
    pub(crate) register_ring_fd: bool,
    /// Batching wait: wait up to this long for `min_wait_batch` CQEs.
    pub(crate) min_wait: Option<Duration>,
    /// CQEs a batching wait waits for.
    pub(crate) min_wait_batch: u32,
    /// Skip `io_uring_enter` in [`Driver::submit`] when there is nothing to submit.
    pub(crate) skip_empty_enter: bool,
}

/// Counters of an [`IoUringDriver`], see [`IoUringDriver::stats`].
///
/// They count from the creation of the driver or the last
/// [`reset_stats`](IoUringDriver::reset_stats). Kernel async worker (io-wq) punts are not visible
/// here: use the `io_uring:io_uring_queue_async_work` tracepoint for those.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DriverStats {
    /// `io_uring_enter` calls that only submit.
    pub enter_submit: u64,
    /// `io_uring_enter` calls that wait for completions.
    pub enter_wait: u64,
    /// `io_uring_enter` calls skipped because there was nothing to do (see `skip_empty_enter`).
    pub enter_skipped: u64,
    /// SQEs the kernel consumed (sum of the `io_uring_enter` results).
    pub sqes_submitted: u64,
    /// CQEs reaped.
    pub cqes_reaped: u64,
    /// CQEs reaped per tick, in buckets: 0, 1, 2-3, 4-7, 8-15, 16-31, 32+.
    pub cqes_per_tick: [u64; 7],
    /// Times the SQ was full and flushed to the kernel before a push.
    pub sq_full_flushes: u64,
    /// Submits retried after `EBUSY`/`EAGAIN`.
    pub busy_retries: u64,
    /// AsyncCancel SQEs pushed.
    pub cancels_pushed: u64,
    /// Cancels dropped because the SQ stayed full.
    pub cancels_lost: u64,
    /// CQEs the kernel dropped because the CQ was full. This is the kernel's own counter, read at
    /// every tick; [`reset_stats`](IoUringDriver::reset_stats) does not clear it.
    pub cq_overflow: u64,
    /// Operations submitted by the driver (including `uring::submit`), indexed by the SQE opcode
    /// (`opcode::Nop::CODE`, ...). Opcodes from 63 up share the last slot. SQEs pushed through
    /// `uring::with_ring` and the driver's own cancel, timeout and eventfd SQEs are not counted.
    pub ops_by_opcode: [u64; 64],
    /// CQEs of multishot operations (`uring::submit_multishot`, `accept_multi`, `recv_multi`),
    /// including those discarded after their handle was dropped.
    pub multishot_cqes: u64,
    /// Longest per-op CQE queue of a multishot operation seen: how far a consumer fell behind.
    pub multishot_queue_high_water: u64,
    /// Linked chains pushed by `uring::submit_chain` (each SQE is also counted in
    /// `ops_by_opcode`).
    pub chains_submitted: u64,
    /// Link timeouts (`read_with_deadline`, `uring::submit_with_timeout`, ...) that fired and
    /// cancelled their operation.
    pub link_timeouts: u64,
    /// Zero-copy sends submitted (`IORING_OP_SEND_ZC`, `IORING_OP_SENDMSG_ZC`) by the
    /// `write_zc`/`send_zc*`/`send_vectored_zc` methods.
    pub zc_sends: u64,
    /// Zero-copy notification CQEs (`IORING_CQE_F_NOTIF`: the kernel released a buffer) of
    /// the driver's own ops.
    pub zc_notifs: u64,
    /// Cancel-by-fd SQEs (`IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_ALL`) pushed by
    /// `cancel_all`. Not included in `cancels_pushed`.
    pub cancel_fd_pushed: u64,
    /// In-flight ops dropped whose data was kept inline in the op slot (no allocation).
    pub ignored_inline: u64,
    /// In-flight ops dropped whose data was too large for the slot and was boxed.
    pub ignored_boxed: u64,
}

impl Default for DriverStats {
    fn default() -> Self {
        DriverStats {
            enter_submit: 0,
            enter_wait: 0,
            enter_skipped: 0,
            sqes_submitted: 0,
            cqes_reaped: 0,
            cqes_per_tick: [0; 7],
            sq_full_flushes: 0,
            busy_retries: 0,
            cancels_pushed: 0,
            cancels_lost: 0,
            cq_overflow: 0,
            ops_by_opcode: [0; 64],
            multishot_cqes: 0,
            multishot_queue_high_water: 0,
            chains_submitted: 0,
            link_timeouts: 0,
            zc_sends: 0,
            zc_notifs: 0,
            cancel_fd_pushed: 0,
            ignored_inline: 0,
            ignored_boxed: 0,
        }
    }
}

/// `struct io_uring_getevents_arg` (kernel ABI).
#[repr(C)]
struct GetEventsArg {
    sigmask: u64,
    sigmask_sz: u32,
    min_wait_usec: u32,
    ts: u64,
}

/// `struct io_uring_rsrc_update` (kernel ABI).
#[repr(C)]
struct RsrcUpdate {
    offset: u32,
    resv: u32,
    data: u64,
}

/// How the driver enters the ring, fixed when the ring is built.
struct EnterConfig {
    /// `IORING_SETUP_*` flags of the ring.
    setup_flags: u32,
    /// Index of the registered ring fd, or -1.
    ring_index: i32,
    /// Batching wait in microseconds (0 = off) and the CQEs it waits for.
    min_wait_usec: u32,
    min_wait_batch: u32,
    /// `Driver::submit` may skip `io_uring_enter` when the SQ is empty.
    skip_empty_enter: bool,
}

impl EnterConfig {
    fn new(uring: &IoUring, opts: DriverOptions) -> Self {
        let params = uring.params();
        // `Parameters` is `repr(transparent)` over `struct io_uring_params`, whose third `u32`
        // field is `flags`.
        let setup_flags = unsafe { *(params as *const io_uring::Parameters as *const u32).add(2) };

        // Without the TASKRUN flag, a COOP/DEFER_TASKRUN ring only runs its task work when we
        // enter, and we can't tell when that is needed. IOPOLL needs an enter to reap.
        let skip_empty_enter = opts.skip_empty_enter
            && setup_flags & SETUP_IOPOLL == 0
            && (setup_flags & SETUP_TASKRUN_FLAG != 0
                || setup_flags & (SETUP_COOP_TASKRUN | SETUP_DEFER_TASKRUN) == 0);

        // The batching wait needs IORING_FEAT_MIN_TIMEOUT (6.12+); ignored otherwise.
        let (min_wait_usec, min_wait_batch) = match opts.min_wait {
            Some(d) if params.is_feature_min_timeout() && opts.min_wait_batch > 1 => (
                d.as_micros().clamp(1, u32::MAX as u128) as u32,
                opts.min_wait_batch,
            ),
            _ => (0, 0),
        };

        // Registration fails on kernels before 5.18; we then keep using the plain fd.
        let ring_index = if opts.register_ring_fd {
            register_ring_fd(uring.as_raw_fd()).unwrap_or(-1)
        } else {
            -1
        };

        EnterConfig {
            setup_flags,
            ring_index,
            min_wait_usec,
            min_wait_batch,
            skip_empty_enter,
        }
    }

    /// A DEFER_TASKRUN ring without the TASKRUN flag: every enter must get events, since we
    /// can't see whether task work is pending.
    fn always_get_events(&self) -> bool {
        self.setup_flags & (SETUP_DEFER_TASKRUN | SETUP_TASKRUN_FLAG) == SETUP_DEFER_TASKRUN
    }
}

/// Register `fd` in the current thread's ring fd table and return its index.
fn register_ring_fd(fd: RawFd) -> io::Result<i32> {
    let mut up = RsrcUpdate {
        offset: u32::MAX,
        resv: 0,
        data: fd as u64,
    };
    let ret = unsafe {
        libc::syscall(
            libc::SYS_io_uring_register,
            fd,
            REGISTER_RING_FDS,
            &mut up as *mut RsrcUpdate,
            1,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(up.offset as i32)
}

/// Driver with uring.
pub struct IoUringDriver {
    inner: Rc<UnsafeCell<UringInner>>,

    // Used for drop
    #[cfg(feature = "sync")]
    thread_id: usize,
}

pub(crate) struct UringInner {
    /// In-flight operations
    ops: Ops,

    // Used as timeout buffer
    timespec: Timespec,

    // Used as read eventfd buffer
    #[cfg(feature = "sync")]
    eventfd_read_dst: [u8; 8],

    /// IoUring bindings
    pub(super) uring: ManuallyDrop<IoUring>,

    /// Shared waker
    #[cfg(feature = "sync")]
    shared_waker: std::sync::Arc<waker::EventWaker>,

    // Mark if eventfd is in the ring
    #[cfg(feature = "sync")]
    eventfd_installed: bool,

    // Waker receiver
    #[cfg(feature = "sync")]
    waker_receiver: flume::Receiver<std::task::Waker>,

    // Uring support ext_arg
    ext_arg: bool,

    /// CQEs of operations the user pushed through `uring::with_ring`.
    pub(super) external: super::raw::ExternalCqes,

    /// Enter settings.
    cfg: EnterConfig,

    /// Counters, see [`IoUringDriver::stats`].
    stats: DriverStats,

    /// Finished slots of dropped ops, dropped by [`drop_deferred`] once the driver holds no
    /// state: dropping op data may call back into the driver.
    deferred: Vec<MaybeFdLifecycle>,
}

// When dropping the driver, all in-flight operations must have completed. This
// type wraps the slab and ensures that, on drop, the slab is empty.
struct Ops {
    slab: Slab<MaybeFdLifecycle>,
}

impl IoUringDriver {
    const DEFAULT_ENTRIES: u32 = 1024;

    #[cfg(not(feature = "sync"))]
    pub(crate) fn new_with_entries(
        urb: &io_uring::Builder,
        entries: Option<u32>,
        opts: DriverOptions,
    ) -> io::Result<IoUringDriver> {
        let uring = ManuallyDrop::new(urb.build(entries.unwrap_or(Self::DEFAULT_ENTRIES))?);

        let inner = Rc::new(UnsafeCell::new(UringInner {
            ops: Ops::new(),
            timespec: Timespec::new(),
            ext_arg: uring.params().is_feature_ext_arg(),
            cfg: EnterConfig::new(&uring, opts),
            uring,
            external: Default::default(),
            stats: DriverStats::default(),
            deferred: Vec::new(),
        }));

        Ok(IoUringDriver { inner })
    }

    #[cfg(feature = "sync")]
    pub(crate) fn new_with_entries(
        urb: &io_uring::Builder,
        entries: Option<u32>,
        opts: DriverOptions,
    ) -> io::Result<IoUringDriver> {
        let uring = ManuallyDrop::new(urb.build(entries.unwrap_or(Self::DEFAULT_ENTRIES))?);

        // Create eventfd and register it to the ring.
        let waker = {
            let fd = crate::syscall!(eventfd@RAW(0, libc::EFD_CLOEXEC))?;
            unsafe {
                use std::os::unix::io::FromRawFd;
                std::fs::File::from_raw_fd(fd)
            }
        };

        let (waker_sender, waker_receiver) = flume::unbounded::<std::task::Waker>();

        let inner = Rc::new(UnsafeCell::new(UringInner {
            ops: Ops::new(),
            timespec: Timespec::new(),
            ext_arg: uring.params().is_feature_ext_arg(),
            cfg: EnterConfig::new(&uring, opts),
            uring,
            shared_waker: std::sync::Arc::new(waker::EventWaker::new(waker)),
            eventfd_installed: false,
            eventfd_read_dst: [0_u8; 8],
            waker_receiver,
            external: Default::default(),
            stats: DriverStats::default(),
            deferred: Vec::new(),
        }));

        let thread_id = crate::builder::BUILD_THREAD_ID.with(|id| *id);
        let driver = IoUringDriver { inner, thread_id };

        // Register unpark handle
        super::thread::register_unpark_handle(thread_id, driver.unpark());
        super::thread::register_waker_sender(thread_id, waker_sender);
        Ok(driver)
    }

    pub(super) fn handle(&self) -> Inner {
        Inner(self.inner.clone())
    }

    /// The driver's counters (cheap: a copy of a few plain integers).
    ///
    /// With the timer enabled, reach it with `runtime.driver().inner().stats()`; inside the
    /// runtime, [`crate::driver_stats`] works too.
    pub fn stats(&self) -> DriverStats {
        unsafe { (*self.inner.get()).stats() }
    }

    /// Reset the counters to zero (except [`DriverStats::cq_overflow`]).
    pub fn reset_stats(&self) {
        unsafe { (*self.inner.get()).reset_stats() }
    }

    /// Whether the ring fd is registered (asked with `RuntimeBuilder::register_ring_fd` and
    /// supported by the kernel, 5.18+).
    pub fn is_ring_fd_registered(&self) -> bool {
        unsafe { (*self.inner.get()).cfg.ring_index >= 0 }
    }

    // Flush to make enough space
    fn flush_space(inner: &mut UringInner, need: usize) -> io::Result<()> {
        let sq = inner.uring.submission();
        debug_assert!(sq.capacity() >= need);
        if sq.len() + need > sq.capacity() {
            drop(sq);
            inner.stats.sq_full_flushes += 1;
            inner.submit()?;
        }
        Ok(())
    }

    #[cfg(feature = "sync")]
    fn install_eventfd(&self, inner: &mut UringInner, fd: RawFd) {
        let entry = opcode::Read::new(
            io_uring::types::Fd(fd),
            inner.eventfd_read_dst.as_mut_ptr(),
            8,
        )
        .build()
        .user_data(EVENTFD_USERDATA);

        let mut sq = inner.uring.submission();
        let _ = unsafe { sq.push(&entry) };
        inner.eventfd_installed = true;
    }

    fn install_timeout(&self, inner: &mut UringInner, duration: Duration) {
        inner.timespec = timespec(duration);
        let entry = opcode::Timeout::new(&inner.timespec as *const Timespec)
            .build()
            .user_data(TIMEOUT_USERDATA);

        let mut sq = inner.uring.submission();
        let _ = unsafe { sq.push(&entry) };
    }

    fn inner_park(&self, timeout: Option<Duration>) -> io::Result<()> {
        let inner = unsafe { &mut *self.inner.get() };

        #[allow(unused_mut)]
        let mut need_wait = true;

        #[cfg(feature = "sync")]
        {
            // Process foreign wakers
            while let Ok(w) = inner.waker_receiver.try_recv() {
                w.wake();
                need_wait = false;
            }

            // Set status as not awake if we are going to sleep
            if need_wait {
                inner
                    .shared_waker
                    .awake
                    .store(false, std::sync::atomic::Ordering::Release);
            }

            // Process foreign wakers left
            while let Ok(w) = inner.waker_receiver.try_recv() {
                w.wake();
                need_wait = false;
            }
        }

        if need_wait {
            // Install timeout and eventfd for unpark if sync is enabled

            // 1. alloc spaces
            let mut space = 0;
            #[cfg(feature = "sync")]
            if !inner.eventfd_installed {
                space += 1;
            }
            if timeout.is_some() {
                space += 1;
            }
            if space != 0 {
                Self::flush_space(inner, space)?;
            }

            // 2.1 install eventfd
            #[cfg(feature = "sync")]
            if !inner.eventfd_installed {
                self.install_eventfd(inner, inner.shared_waker.as_raw_fd());
            }

            // 2.2 install timeout and submit_and_wait with timeout
            if let Some(duration) = timeout {
                match inner.ext_arg {
                    // Submit and Wait with timeout in an TimeoutOp way.
                    // Better compatibility(5.4+).
                    false => {
                        self.install_timeout(inner, duration);
                        inner.enter(1, None)?;
                    }
                    // Submit and Wait with enter args.
                    // Better performance(5.11+).
                    true => {
                        let timespec = timespec(duration);
                        if let Err(e) = inner.enter(1, Some(&timespec)) {
                            if e.raw_os_error() != Some(libc::ETIME) {
                                return Err(e);
                            }
                        }
                    }
                }
            } else {
                // Submit and Wait without timeout
                inner.enter(1, None)?;
            }
        } else {
            // Submit only
            inner.enter(0, None)?;
        }

        // Set status as awake
        #[cfg(feature = "sync")]
        inner
            .shared_waker
            .awake
            .store(true, std::sync::atomic::Ordering::Release);

        // Process CQ
        inner.tick()?;
        drop_deferred(&self.inner);
        super::raw::deliver(&self.inner);

        Ok(())
    }
}

impl Driver for IoUringDriver {
    /// Enter the driver context. This enables using uring types.
    fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        // TODO(ihciah): remove clone
        let inner = Inner(self.inner.clone());
        CURRENT.set(&inner, f)
    }

    fn submit(&self) -> io::Result<()> {
        let inner = unsafe { &mut *self.inner.get() };
        if inner.nothing_to_enter() {
            // No enter, but the kernel may still pick buffers from task work.
            super::buf_ring::flush_pending();
            inner.stats.enter_skipped += 1;
        } else {
            inner.submit()?;
        }
        inner.tick()?;
        drop_deferred(&self.inner);
        super::raw::deliver(&self.inner);
        Ok(())
    }

    fn park(&self) -> io::Result<()> {
        self.inner_park(None)
    }

    fn park_timeout(&self, duration: Duration) -> io::Result<()> {
        self.inner_park(Some(duration))
    }

    #[cfg(feature = "sync")]
    type Unpark = waker::UnparkHandle;

    #[cfg(feature = "sync")]
    fn unpark(&self) -> Self::Unpark {
        UringInner::unpark(&self.inner)
    }
}

impl UringInner {
    pub(super) fn stats(&self) -> DriverStats {
        self.stats
    }

    pub(super) fn reset_stats(&mut self) {
        self.stats = DriverStats {
            cq_overflow: self.stats.cq_overflow,
            ..DriverStats::default()
        };
    }

    fn tick(&mut self) -> io::Result<()> {
        let cq = self.uring.completion();
        self.stats.cq_overflow = cq.overflow() as u64;

        let mut n = 0_u64;
        for cqe in cq {
            n += 1;
            let index = cqe.user_data();
            match index {
                #[cfg(feature = "sync")]
                EVENTFD_USERDATA => self.eventfd_installed = false,
                // Operations of this driver: `user_data` is a slab index.
                _ if index >> 62 == 0 => {
                    if cqe.flags() & CQE_F_NOTIF != 0 {
                        self.stats.zc_notifs += 1;
                    }
                    // SAFETY: the CQE comes from the kernel for the op at `index`.
                    let done = unsafe { self.ops.complete(index as _, cqe.result(), cqe.flags()) };
                    if let Some(queued) = done.queued {
                        self.stats.multishot_cqes += 1;
                        let hw = &mut self.stats.multishot_queue_high_water;
                        *hw = (*hw).max(queued as u64);
                    }
                    if let Some(slot) = done.deferred {
                        self.deferred.push(slot);
                    }
                }
                // Operations pushed by the user, see `uring::user_data`.
                _ if index >> 62 == 0b10 => self.external.push(index, &cqe),
                // A link timeout of the driver: `-ETIME` means it fired (and cancelled its op),
                // `-ECANCELED` that the op completed first.
                LINK_TIMEOUT_USERDATA if cqe.result() == -libc::ETIME => {
                    self.stats.link_timeouts += 1
                }
                // Driver internal (cancel, timeout) or unknown: nothing waits for it.
                _ => (),
            }
        }
        self.stats.cqes_reaped += n;
        // Buckets 0, 1, 2-3, 4-7, ...: the bit length of `n`.
        let bucket = (u64::BITS - n.leading_zeros()).min(6) as usize;
        self.stats.cqes_per_tick[bucket] += 1;
        Ok(())
    }

    /// Whether `Driver::submit` can skip `io_uring_enter`: opted in, nothing queued, and the
    /// kernel flags no pending task work or CQ overflow.
    fn nothing_to_enter(&mut self) -> bool {
        if !self.cfg.skip_empty_enter {
            return false;
        }
        let sq = self.uring.submission();
        sq.is_empty() && !sq.taskrun() && !sq.cq_overflow()
    }

    /// One `io_uring_enter`: submit the queued SQEs and wait for `want` CQEs, at most until `ts`.
    ///
    /// Same flags as `Submitter::submit_and_wait`/`submit_with_args` of `io-uring`, plus the
    /// registered ring fd, GETEVENTS for a DEFER_TASKRUN ring without the TASKRUN flag, and the
    /// batching wait. Returns the number of SQEs the kernel consumed.
    fn enter(&mut self, want: u32, ts: Option<&Timespec>) -> io::Result<usize> {
        // Let the kernel see buffers recycled into batch-mode buffer rings.
        super::buf_ring::flush_pending();

        let sq = self.uring.submission();
        let len = sq.len() as u32;
        let cq_overflow = sq.cq_overflow();
        let taskrun = sq.taskrun();
        let need_wakeup = self.cfg.setup_flags & SETUP_SQPOLL != 0 && sq.need_wakeup();
        drop(sq);

        let params = self.uring.params();
        let mut flags = EnterFlags::empty();
        if want > 0
            || params.is_setup_iopoll()
            || cq_overflow
            || taskrun
            || self.cfg.always_get_events()
        {
            flags.insert(EnterFlags::GETEVENTS);
        }
        if params.is_setup_sqpoll() {
            let need_syscall = (cq_overflow && params.is_feature_nodrop()) || taskrun;
            if need_wakeup {
                flags.insert(EnterFlags::SQ_WAKEUP);
            } else if want == 0 && !need_syscall {
                // The SQ thread is awake and picks the SQEs up.
                return Ok(len as usize);
            }
        }

        let mut want = want;
        let min_wait = if want > 0 { self.cfg.min_wait_usec } else { 0 };
        if min_wait != 0 {
            want = self.cfg.min_wait_batch;
        }
        let arg = GetEventsArg {
            sigmask: 0,
            sigmask_sz: 0,
            min_wait_usec: min_wait,
            ts: ts.map_or(0, |ts| ts as *const Timespec as u64),
        };
        let (arg_ptr, arg_size) = if ts.is_some() || min_wait != 0 {
            flags.insert(EnterFlags::EXT_ARG);
            (
                &arg as *const GetEventsArg as *const libc::c_void,
                std::mem::size_of::<GetEventsArg>(),
            )
        } else {
            (std::ptr::null(), std::mem::size_of::<libc::sigset_t>())
        };
        let fd = if self.cfg.ring_index >= 0 {
            flags.insert(EnterFlags::REGISTERED_RING);
            self.cfg.ring_index
        } else {
            self.uring.as_raw_fd()
        };

        if want > 0 {
            self.stats.enter_wait += 1;
        } else {
            self.stats.enter_submit += 1;
        }
        // `arg` (and the timespec it points to) outlives the call.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                fd,
                len,
                want,
                flags.bits(),
                arg_ptr,
                arg_size,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        self.stats.sqes_submitted += ret as u64;
        Ok(ret as usize)
    }

    /// Make room for one SQE: flush the SQ (and reap CQEs) while it is full, a few times.
    #[inline]
    fn reserve_sqe(&mut self) -> io::Result<()> {
        self.reserve_sqes(1)
    }

    /// Make room for `n` SQEs pushed back to back: flush the SQ (and reap CQEs) while it has
    /// less free space, a few times. `InvalidInput` if `n` is larger than the SQ.
    fn reserve_sqes(&mut self, n: usize) -> io::Result<()> {
        let free = |inner: &mut Self| {
            let sq = inner.uring.submission();
            sq.capacity() - sq.len()
        };
        if n > self.uring.submission().capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "more SQEs than the submission queue holds",
            ));
        }
        for _ in 0..PUSH_RETRIES {
            if free(self) >= n {
                return Ok(());
            }
            self.stats.sq_full_flushes += 1;
            if self.cfg.setup_flags & SETUP_SQPOLL != 0 {
                // The SQ thread consumes SQEs on its own; wait until it did.
                self.uring.submitter().squeue_wait()?;
            } else {
                self.submit()?;
            }
            self.tick()?;
        }
        if free(self) < n {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        Ok(())
    }

    /// Push an AsyncCancel for the operation at `index`, if it is still in flight. Counts a lost
    /// cancel instead of failing.
    fn push_cancel(&mut self, index: usize) {
        if self.reserve_sqe().is_err() {
            self.stats.cancels_lost += 1;
            return;
        }
        // Reaping CQEs to make room may have completed (and freed) the operation.
        if self.ops.slab.get(index).is_none() {
            return;
        }
        let cancel = opcode::AsyncCancel::new(index as u64)
            .build()
            .user_data(u64::MAX);
        // Can't fail: `reserve_sqe` made room.
        if unsafe { self.uring.submission().push(&cancel) }.is_ok() {
            self.stats.cancels_pushed += 1;
        } else {
            self.stats.cancels_lost += 1;
        }
    }

    fn submit(&mut self) -> io::Result<()> {
        loop {
            match self.enter(0, None) {
                #[cfg(feature = "unstable")]
                Err(ref e)
                    if matches!(e.kind(), io::ErrorKind::Other | io::ErrorKind::ResourceBusy) =>
                {
                    self.stats.busy_retries += 1;
                    self.tick()?;
                }
                #[cfg(not(feature = "unstable"))]
                Err(ref e)
                    if matches!(e.raw_os_error(), Some(libc::EAGAIN) | Some(libc::EBUSY)) =>
                {
                    // This error is constructed with io::Error::last_os_error():
                    // https://github.com/tokio-rs/io-uring/blob/01c83bbce965d4aaf93ebfaa08c3aa8b7b0f5335/src/sys/mod.rs#L32
                    // So we can use https://doc.rust-lang.org/nightly/std/io/struct.Error.html#method.raw_os_error
                    // to get the raw error code.
                    self.stats.busy_retries += 1;
                    self.tick()?;
                }
                e => return e.map(|_| ()),
            }
        }
    }

    /// Reserve SQ space, allocate a slab slot for `data` and push its SQE. Returns the slot
    /// index. On failure nothing is left behind and `data` is untouched.
    fn push_op<T: OpAble>(&mut self, data: &mut T) -> io::Result<usize> {
        // If the submission queue is full, flush it to the kernel
        self.reserve_sqe()?;

        let index = self.ops.insert(T::RET_IS_FD, T::KIND);
        let sqe: squeue::Entry = data.uring_op().user_data(index as _);

        // Can't fail after `reserve_sqe`; if it does, free the slot again.
        if unsafe { self.uring.submission().push(&sqe) }.is_err() {
            if let Some(slot) = self.ops.slab.get(index) {
                slot.remove();
            }
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        let opcode = (sqe.get_opcode() as usize).min(63);
        self.stats.ops_by_opcode[opcode] += 1;
        if T::KIND == OpKind::ZeroCopy {
            self.stats.zc_sends += 1;
        }

        // The SQE is visible to the kernel once the tail is published. We don't enter now: the
        // runtime submits on `park` (or `submit`) when it waits for IO.
        Ok(index)
    }

    /// Push `sqes` back to back (nothing is submitted in between, so a linked chain stays
    /// whole), each with a new single-shot slab slot. Returns the slot indices. On failure
    /// nothing is left behind.
    fn push_chain<const N: usize>(&mut self, sqes: [squeue::Entry; N]) -> io::Result<[usize; N]> {
        self.reserve_sqes(N)?;
        let mut slots = [usize::MAX; N];
        let mut i = 0;
        let sqes = sqes.map(|sqe| {
            let index = self.ops.insert(false, OpKind::Single);
            slots[i] = index;
            i += 1;
            sqe.user_data(index as _)
        });
        // Can't fail after `reserve_sqes`; if it does, free the slots again.
        if unsafe { self.uring.submission().push_multiple(&sqes) }.is_err() {
            for index in slots {
                if let Some(slot) = self.ops.slab.get(index) {
                    slot.remove();
                }
            }
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        for sqe in &sqes {
            self.stats.ops_by_opcode[(sqe.get_opcode() as usize).min(63)] += 1;
        }
        self.stats.chains_submitted += 1;
        Ok(slots)
    }

    /// Push the SQE of `data` with `IO_LINK`, directly followed by a `LINK_TIMEOUT` SQE for
    /// `ts` (one reservation, so the pair is never split). Returns the slot index of `data`.
    ///
    /// `ts` must stay valid until the kernel consumed the SQEs (it copies the timespec when it
    /// prepares the timeout); the callers keep it with the op data until the op completed.
    fn push_op_with_timeout<T: OpAble>(
        &mut self,
        data: &mut T,
        ts: *const Timespec,
    ) -> io::Result<usize> {
        debug_assert!(T::KIND != OpKind::Multishot);
        self.reserve_sqes(2)?;

        let index = self.ops.insert(T::RET_IS_FD, T::KIND);
        let sqe = data
            .uring_op()
            .user_data(index as _)
            .flags(squeue::Flags::IO_LINK);
        let timeout = opcode::LinkTimeout::new(ts)
            .build()
            .user_data(LINK_TIMEOUT_USERDATA);

        let opcode = (sqe.get_opcode() as usize).min(63);

        // Can't fail after `reserve_sqes`; if it does, free the slot again.
        if unsafe { self.uring.submission().push_multiple(&[sqe, timeout]) }.is_err() {
            if let Some(slot) = self.ops.slab.get(index) {
                slot.remove();
            }
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        self.stats.ops_by_opcode[opcode] += 1;
        Ok(index)
    }

    /// Submit a chain of raw SQEs, see `uring::submit_chain`.
    pub(crate) fn submit_chain<const N: usize>(
        this: &Rc<UnsafeCell<UringInner>>,
        sqes: [squeue::Entry; N],
    ) -> io::Result<[usize; N]> {
        let inner = unsafe { &mut *this.get() };
        inner.push_chain(sqes)
    }

    /// Submit `data` as a new operation, cancelled by a link timeout `ts` (see
    /// [`push_op_with_timeout`](Self::push_op_with_timeout) for how long `ts` must live). Hands
    /// `data` back on failure.
    pub(crate) fn try_submit_with_timeout<T: OpAble>(
        this: &Rc<UnsafeCell<UringInner>>,
        mut data: T,
        ts: *const Timespec,
    ) -> Result<Op<T>, (io::Error, T)> {
        let inner = unsafe { &mut *this.get() };
        match inner.push_op_with_timeout(&mut data, ts) {
            Ok(index) => Ok(Op {
                driver: Inner(this.clone()),
                index,
                data: Some(data),
            }),
            Err(e) => Err((e, data)),
        }
    }

    /// Submit `data` as a new operation.
    pub(crate) fn submit_with_data<T>(
        this: &Rc<UnsafeCell<UringInner>>,
        data: T,
    ) -> io::Result<Op<T>>
    where
        T: OpAble,
    {
        Self::try_submit_with_data(this, data).map_err(|(e, _)| e)
    }

    /// Like [`submit_with_data`](Self::submit_with_data), but hands `data` back on failure (the
    /// SQ stayed full).
    pub(crate) fn try_submit_with_data<T>(
        this: &Rc<UnsafeCell<UringInner>>,
        mut data: T,
    ) -> Result<Op<T>, (io::Error, T)>
    where
        T: OpAble,
    {
        // Multishot ops are driven by `MultishotOp`, which uses `submit_index`.
        debug_assert!(T::KIND != OpKind::Multishot);
        match Self::submit_index(this, &mut data) {
            Ok(index) => Ok(Op {
                driver: Inner(this.clone()),
                index,
                data: Some(data),
            }),
            Err(e) => Err((e, data)),
        }
    }

    /// Push the SQE of `data` and return its slot index; the caller keeps `data`.
    pub(crate) fn submit_index<T: OpAble>(
        this: &Rc<UnsafeCell<UringInner>>,
        data: &mut T,
    ) -> io::Result<usize> {
        let inner = unsafe { &mut *this.get() };
        inner.push_op(data)
    }

    pub(crate) fn poll_op(
        this: &Rc<UnsafeCell<UringInner>>,
        index: usize,
        cx: &mut Context<'_>,
    ) -> Poll<CompletionMeta> {
        let inner = unsafe { &mut *this.get() };
        let lifecycle = unsafe { inner.ops.slab.get(index).unwrap_unchecked() };
        lifecycle.poll_op(cx)
    }

    pub(crate) fn drop_op<T: 'static>(
        this: &Rc<UnsafeCell<UringInner>>,
        index: usize,
        data: &mut Option<T>,
        _skip_cancel: bool,
    ) {
        let inner = unsafe { &mut *this.get() };
        if index == usize::MAX {
            // already finished
            return;
        }
        if let Some(lifecycle) = inner.ops.slab.get(index) {
            let dropped = lifecycle.drop_op(data);
            match dropped.kept_inline {
                Some(true) => inner.stats.ignored_inline += 1,
                Some(false) => inner.stats.ignored_boxed += 1,
                None => {}
            }
            #[cfg(feature = "async-cancel")]
            if dropped.cancel && !_skip_cancel {
                inner.push_cancel(index);
            }
        }
    }

    /// Two-step zero-copy send: the result of the first CQE, see `Ref::poll_zc_first`.
    pub(crate) fn poll_zc_first(
        this: &Rc<UnsafeCell<UringInner>>,
        index: usize,
        cx: &mut Context<'_>,
    ) -> Poll<ZcFirst> {
        let inner = unsafe { &mut *this.get() };
        match inner.ops.slab.get(index) {
            Some(slot) => slot.poll_zc_first(cx),
            // Not reachable: the slot lives until the notification was seen.
            None => Poll::Ready(ZcFirst {
                meta: CompletionMeta {
                    result: Err(io::Error::other("zero-copy op slot gone")),
                    flags: 0,
                },
                done: true,
            }),
        }
    }

    /// Two-step zero-copy send: ready once the notification CQE arrived, see
    /// `Ref::poll_zc_notif`.
    pub(crate) fn poll_zc_notif(
        this: &Rc<UnsafeCell<UringInner>>,
        index: usize,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        let inner = unsafe { &mut *this.get() };
        match inner.ops.slab.get(index) {
            Some(slot) => slot.poll_zc_notif(cx),
            None => Poll::Ready(()),
        }
    }

    /// Two-step zero-copy send: whether the notification arrived, see `Ref::zc_released`.
    pub(crate) fn zc_released(this: &Rc<UnsafeCell<UringInner>>, index: usize) -> bool {
        let inner = unsafe { &mut *this.get() };
        match inner.ops.slab.get(index) {
            Some(slot) => slot.zc_released(),
            None => true,
        }
    }

    /// Push one `AsyncCancel` matching every in-flight request on `target`
    /// (`IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_ALL`, kernel 5.19+; older kernels fail
    /// the cancel with `EINVAL`, which nobody sees). Errors if the SQ stayed full.
    pub(crate) fn cancel_fd(
        this: &Rc<UnsafeCell<UringInner>>,
        target: CancelTarget,
    ) -> io::Result<()> {
        let inner = unsafe { &mut *this.get() };
        inner.reserve_sqe()?;
        let builder = match target {
            CancelTarget::Fd(fd) => types::CancelBuilder::fd(types::Fd(fd)),
            CancelTarget::Fixed(slot) => types::CancelBuilder::fd(types::Fixed(slot)),
        };
        let cancel = opcode::AsyncCancel2::new(builder.all())
            .build()
            .user_data(u64::MAX);
        // Can't fail: `reserve_sqe` made room.
        unsafe { inner.uring.submission().push(&cancel) }
            .map_err(|_| io::Error::from_raw_os_error(libc::EBUSY))?;
        inner.stats.cancel_fd_pushed += 1;
        Ok(())
    }

    pub(crate) unsafe fn cancel_op(this: &Rc<UnsafeCell<UringInner>>, index: usize) {
        let inner = &mut *this.get();
        inner.push_cancel(index);
    }

    /// Next CQE of the multishot op at `index`, see `Ref::poll_multi`.
    pub(crate) fn poll_multi(
        this: &Rc<UnsafeCell<UringInner>>,
        index: usize,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Cqe>> {
        let inner = unsafe { &mut *this.get() };
        match inner.ops.slab.get(index) {
            Some(slot) => slot.poll_multi(cx),
            None => Poll::Ready(None),
        }
    }

    /// The handle of the multishot op at `index` is dropped. Discards its queued CQEs; if it is
    /// still running, keeps `data` until the final CQE and asks the kernel to cancel it.
    ///
    /// Unlike single ops the cancel is pushed even without `async-cancel`: a multishot op
    /// (accept, recv) may never end on its own.
    pub(crate) fn drop_multi<T: OpAble + 'static>(
        this: &Rc<UnsafeCell<UringInner>>,
        index: usize,
        data: &mut Option<T>,
    ) {
        let inner = unsafe { &mut *this.get() };
        if let Some(slot) = inner.ops.slab.get(index) {
            if !slot.drop_multi(data) {
                inner.push_cancel(index);
            }
        }
    }

    /// If the kernel is done with the multishot op at `index`, free its slot (discarding the
    /// CQEs not taken) and return true.
    pub(crate) fn finish_multi<T: OpAble>(
        this: &Rc<UnsafeCell<UringInner>>,
        index: usize,
        data: &mut T,
    ) -> bool {
        let inner = unsafe { &mut *this.get() };
        match inner.ops.slab.get(index) {
            Some(slot) => slot.finish_multi(data),
            None => true,
        }
    }

    /// Whether the kernel posted the final CQE of the multishot op at `index`.
    pub(crate) fn multi_finished(this: &Rc<UnsafeCell<UringInner>>, index: usize) -> bool {
        let inner = unsafe { &mut *this.get() };
        match inner.ops.slab.get(index) {
            Some(slot) => slot.multi_finished(),
            None => true,
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn unpark(this: &Rc<UnsafeCell<UringInner>>) -> waker::UnparkHandle {
        let inner = unsafe { &*this.get() };
        let weak = std::sync::Arc::downgrade(&inner.shared_waker);
        waker::UnparkHandle(weak)
    }
}

impl AsRawFd for IoUringDriver {
    fn as_raw_fd(&self) -> RawFd {
        unsafe { (*self.inner.get()).uring.as_raw_fd() }
    }
}

impl Drop for IoUringDriver {
    fn drop(&mut self) {
        trace!("MONOIO DEBUG[IoUringDriver]: drop");

        // Deregister thread id
        #[cfg(feature = "sync")]
        {
            use crate::driver::thread::{unregister_unpark_handle, unregister_waker_sender};
            unregister_unpark_handle(self.thread_id);
            unregister_waker_sender(self.thread_id);
        }
    }
}

impl Drop for UringInner {
    fn drop(&mut self) {
        // no need to wait for completion, as the kernel will clean up the ring asynchronically.
        let _ = self.uring.submitter().submit();
        if self.cfg.ring_index >= 0 {
            let up = RsrcUpdate {
                offset: self.cfg.ring_index as u32,
                resv: 0,
                data: 0,
            };
            unsafe {
                libc::syscall(
                    libc::SYS_io_uring_register,
                    self.uring.as_raw_fd(),
                    UNREGISTER_RING_FDS,
                    &up as *const RsrcUpdate,
                    1,
                )
            };
        }
        unsafe {
            ManuallyDrop::drop(&mut self.uring);
        }
    }
}

impl Ops {
    const fn new() -> Self {
        Ops { slab: Slab::new() }
    }

    // Insert a new operation
    #[inline]
    pub(crate) fn insert(&mut self, is_fd: bool, kind: OpKind) -> usize {
        self.slab.insert(MaybeFdLifecycle::new(is_fd, kind))
    }

    // Complete an operation. A CQE for a free slot (which the kernel never posts) is ignored.
    // # Safety
    // Caller must make sure the result is valid.
    #[inline]
    unsafe fn complete(&mut self, index: usize, res: i32, flags: u32) -> lifecycle::Completed {
        match self.slab.get(index) {
            Some(lifecycle) => lifecycle.complete(res, flags),
            None => {
                trace!("MONOIO DEBUG[IoUringDriver]: CQE for free slot {}", index);
                lifecycle::Completed::default()
            }
        }
    }
}

/// Drop the finished slots of dropped ops. Called by the driver once it holds no state, since
/// the drops may call back into it (e.g. a `SharedFd` closing through the ring).
fn drop_deferred(cell: &UnsafeCell<UringInner>) {
    // No other reference to the driver state is alive here.
    let mut slots = {
        let inner = unsafe { &mut *cell.get() };
        if inner.deferred.is_empty() {
            return;
        }
        std::mem::take(&mut inner.deferred)
    };
    slots.clear();
    // Keep the allocation, unless the drops deferred new slots meanwhile.
    let inner = unsafe { &mut *cell.get() };
    if inner.deferred.is_empty() {
        inner.deferred = slots;
    }
}
