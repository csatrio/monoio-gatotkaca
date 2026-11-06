use std::{io, marker::PhantomData, time::Duration};

use crate::{
    driver::{Driver, DriverOptions, IoUringDriver},
    time::{driver::TimeDriver, Clock},
    utils::thread_id::gen_id,
    Runtime,
};

mod sqpoll_check;

// ===== basic builder structure definition =====

/// Runtime builder
pub struct RuntimeBuilder<D> {
    // iouring entries
    entries: Option<u32>,

    urb: io_uring::Builder,

    // driver settings beyond the ring flags
    opts: DriverOptions,

    // tasks run per round, see `task_budget`
    task_budget: Option<usize>,

    // per-poll coop budget, see `coop_budget`
    coop_budget: Option<u32>,

    // LIFO slot for hand-over wakes, see `lifo_slot`
    lifo_slot: bool,

    // priority for tasks woken by small completions, see `small_io_priority` (0: off)
    small_io_max: u32,

    // [bounded, unbounded] io-wq worker caps, see `iowq_max_workers`
    iowq_max_workers: Option<[u32; 2]>,

    // [runtime, kernel] fixed file slots, see `with_fixed_files`
    fixed_files: [u32; 2],

    // SQPOLL settings, applied at `build()`, see `sqpoll`
    sqpoll: sqpoll_check::SqpollSettings,

    // blocking handle
    #[cfg(feature = "sync")]
    blocking_handle: crate::blocking::BlockingHandle,
    // driver mark
    _mark: PhantomData<D>,
}

scoped_thread_local!(pub(crate) static BUILD_THREAD_ID: usize);

impl<T> Default for RuntimeBuilder<T> {
    /// Create a default runtime builder.
    ///
    /// # Note
    ///
    /// When the sync feature is enabled, the default behavior of
    /// [monoio::blocking::BlockingStrategy] is to execute tasks on the local thread. In other
    /// words, there is no thread pool involved—all blocking I/O operations and heavy computations
    /// will block the current thread.
    fn default() -> Self {
        RuntimeBuilder::<T>::new()
    }
}

impl<T> RuntimeBuilder<T> {
    /// Create a default runtime builder.
    ///
    /// # Note
    ///
    /// When the sync feature is enabled, the default behavior of
    /// [monoio::blocking::BlockingStrategy] is to execute tasks on the local thread. In other
    /// words, there is no thread pool involved—all blocking I/O operations and heavy computations
    /// will block the current thread.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: None,

            urb: io_uring::IoUring::builder(),
            opts: DriverOptions::default(),
            task_budget: None,
            coop_budget: None,
            lifo_slot: false,
            small_io_max: 0,
            iowq_max_workers: None,
            fixed_files: [0, 0],
            sqpoll: sqpoll_check::SqpollSettings::default(),

            #[cfg(feature = "sync")]
            blocking_handle: crate::blocking::BlockingStrategy::ExecuteLocal.into(),
            _mark: PhantomData,
        }
    }
}

// ===== buildable trait and forward methods =====

/// Buildable trait.
pub trait Buildable: Sized {
    /// Build the runtime.
    fn build(this: RuntimeBuilder<Self>) -> io::Result<Runtime<Self>>;
}

macro_rules! direct_build {
    ($ty: ty) => {
        impl RuntimeBuilder<$ty> {
            /// Build the runtime.
            pub fn build(self) -> io::Result<Runtime<$ty>> {
                Buildable::build(self)
            }
        }
    };
}

direct_build!(IoUringDriver);
direct_build!(TimeDriver<IoUringDriver>);

// ===== builder impl =====

impl Buildable for IoUringDriver {
    fn build(this: RuntimeBuilder<Self>) -> io::Result<Runtime<IoUringDriver>> {
        let thread_id = gen_id();
        #[cfg(feature = "sync")]
        let blocking_handle = this.blocking_handle;

        let mut urb = this.urb;
        this.sqpoll.apply(&mut urb)?;

        BUILD_THREAD_ID.set(&thread_id, || {
            let driver = IoUringDriver::new_with_entries(&urb, this.entries, this.opts)?;
            if let Some(mut max) = this.iowq_max_workers {
                // Safety: the runtime does not exist yet, nothing else touches the ring.
                unsafe {
                    driver.with_ring(|ring| ring.submitter().register_iowq_max_workers(&mut max))
                }?;
            }
            let [user, kernel] = this.fixed_files;
            if user > 0 || kernel > 0 {
                crate::driver::register_fixed_files(&driver, user, kernel)?;
            }
            #[cfg(feature = "sync")]
            let mut context = crate::runtime::Context::new(blocking_handle);
            #[cfg(not(feature = "sync"))]
            let mut context = crate::runtime::Context::new();
            context.task_budget = this.task_budget;
            context.coop_budget = this.coop_budget;
            context.lifo_slot = this.lifo_slot;
            context.small_io_max = this.small_io_max;
            Ok(Runtime::new(context, driver))
        })
    }
}

impl<D> RuntimeBuilder<D> {
    const MIN_ENTRIES: u32 = 256;

    /// Set io_uring entries, min size is 256 and the default size is 1024.
    #[must_use]
    pub fn with_entries(mut self, entries: u32) -> Self {
        // If entries is less than 256, it will be 256.
        if entries < Self::MIN_ENTRIES {
            self.entries = Some(Self::MIN_ENTRIES);
            return self;
        }
        self.entries = Some(entries);
        self
    }

    /// Share the kernel async worker pool of another ring (`IORING_SETUP_ATTACH_WQ`).
    ///
    /// `fd` is the ring fd of an existing runtime, e.g. `runtime.driver().as_raw_fd()`; that
    /// runtime must outlive this one. Call it after [`uring_builder`](Self::uring_builder), which
    /// replaces all ring settings. Works with and without the timer.
    #[must_use]
    pub fn attach_wq(mut self, fd: std::os::unix::io::RawFd) -> Self {
        self.urb.setup_attach_wq(fd);
        self
    }

    /// Replaces the default [`io_uring::Builder`], which controls the settings for the
    /// inner `io_uring` API.
    ///
    /// Refer to the [`io_uring::Builder`] documentation for all the supported methods.
    ///
    /// This replaces all ring flags set before (`attach_wq`, `coop_taskrun`, `taskrun_flag`,
    /// `single_issuer`, `defer_taskrun`, `cq_size`), so call it first.
    #[must_use]
    pub fn uring_builder(mut self, urb: io_uring::Builder) -> Self {
        self.urb = urb;
        self
    }

    /// Run completion task work only when the thread enters the kernel instead of interrupting
    /// it (`IORING_SETUP_COOP_TASKRUN`, 5.19+). Call it after
    /// [`uring_builder`](Self::uring_builder).
    #[must_use]
    pub fn coop_taskrun(mut self) -> Self {
        self.urb.setup_coop_taskrun();
        self
    }

    /// Let the kernel flag pending completion task work in the SQ ring
    /// (`IORING_SETUP_TASKRUN_FLAG`, 5.19+), so the driver enters the kernel to run it only when
    /// needed. Use it with `coop_taskrun` or `defer_taskrun`. Call it after
    /// [`uring_builder`](Self::uring_builder).
    #[must_use]
    pub fn taskrun_flag(mut self) -> Self {
        self.urb.setup_taskrun_flag();
        self
    }

    /// Promise that only this thread submits to the ring (`IORING_SETUP_SINGLE_ISSUER`, 6.0+).
    /// Call it after [`uring_builder`](Self::uring_builder).
    #[must_use]
    pub fn single_issuer(mut self) -> Self {
        self.urb.setup_single_issuer();
        self
    }

    /// Run completion task work only when the driver asks for events
    /// (`IORING_SETUP_DEFER_TASKRUN`, 6.1+). Also sets `single_issuer` (required by the kernel)
    /// and `taskrun_flag` (so the driver knows when to collect completions while tasks keep
    /// running). Call it after [`uring_builder`](Self::uring_builder).
    #[must_use]
    pub fn defer_taskrun(mut self) -> Self {
        self.urb
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_taskrun_flag();
        self
    }

    /// Set the CQ size (`IORING_SETUP_CQSIZE`); the default is twice the SQ entries. A bigger CQ
    /// helps when multishot operations post many CQEs. Call it after
    /// [`uring_builder`](Self::uring_builder).
    #[must_use]
    pub fn cq_size(mut self, entries: u32) -> Self {
        self.urb.setup_cqsize(entries);
        self
    }

    /// Register the ring fd with the kernel (`IORING_REGISTER_RING_FDS`, 5.18+), which saves a
    /// file lookup per `io_uring_enter`. If the kernel refuses, the driver silently keeps using
    /// the plain fd; check with [`IoUringDriver::is_ring_fd_registered`].
    #[must_use]
    pub fn register_ring_fd(mut self) -> Self {
        self.opts.register_ring_fd = true;
        self
    }

    /// Batch wakeups: when the driver waits for completions, it waits for `batch` CQEs, or for
    /// `wait` to pass with at least one CQE ready (`min_wait_usec`, needs
    /// `IORING_FEAT_MIN_TIMEOUT`, 6.12+; ignored on older kernels or when `batch < 2`).
    ///
    /// This adds up to `wait` of latency when there is little traffic, including for wakeups from
    /// other threads.
    #[must_use]
    pub fn min_wait(mut self, wait: Duration, batch: u32) -> Self {
        self.opts.min_wait = Some(wait);
        self.opts.min_wait_batch = batch;
        self
    }

    /// Skip `io_uring_enter` between task rounds when no SQE is queued and the kernel flags no
    /// pending task work or CQ overflow. Completions are still reaped.
    ///
    /// Off by default. It has no effect on a `coop_taskrun`/`defer_taskrun` ring without
    /// `taskrun_flag` (the driver can't see pending task work there) or with `IOPOLL`.
    #[must_use]
    pub fn skip_empty_enter(mut self) -> Self {
        self.opts.skip_empty_enter = true;
        self
    }

    /// Cap the kernel's io-wq worker threads (`iou-wrk-*`) for this ring
    /// (`IORING_REGISTER_IOWQ_MAX_WORKERS`, 5.15+), registered right after the ring is built.
    ///
    /// io_uring punts work it can't finish inline to these threads: `bounded` caps the workers
    /// for regular file and block device IO, `unbounded` those for everything else that can
    /// wait forever (sockets, pipes; a splice into a pipe counts here). `0` keeps the kernel's
    /// current limit for that class (by default `bounded` is at most the SQ size and about four
    /// per CPU, `unbounded` follows `RLIMIT_NPROC`). Fewer workers mean fewer threads and less
    /// context switching, but punted ops wait in a queue for a free worker.
    ///
    /// The workers belong to the io-wq of the thread that submits. Rings that share one io-wq
    /// via [`attach_wq`](Self::attach_wq) share the workers, so the last cap registered on any
    /// of them applies to all; set it on the primary. Older kernels (before 6.3) apply the
    /// limits per NUMA node.
    ///
    /// Off by default. [`build`](RuntimeBuilder::build) returns the kernel's error if the
    /// registration fails (`EINVAL` on kernels without it).
    #[must_use]
    pub fn iowq_max_workers(mut self, bounded: u32, unbounded: u32) -> Self {
        self.iowq_max_workers = Some([bounded, unbounded]);
        self
    }

    /// Register a fixed file table with `n` slots handed out by the runtime when the ring is
    /// built (`IORING_REGISTER_FILES2`, sparse, Linux 5.19+).
    ///
    /// A file in the table is named by its slot in every SQE (`IOSQE_FIXED_FILE`) instead of
    /// its fd, which saves the kernel a file lookup and an atomic refcount pair per op. Put
    /// sockets and files in with `register_fixed` ([`TcpStream`](crate::net::TcpStream),
    /// [`TcpListener`](crate::net::TcpListener), [`UnixStream`](crate::net::UnixStream),
    /// [`File`](crate::fs::File); the fd stays open), or accept connections straight into it
    /// with [`TcpListener::accept_direct`](crate::net::TcpListener::accept_direct) (no fd at
    /// all). A slot is freed when its file is closed. A full table fails those calls with
    /// `ENFILE`; [`fixed_file_stats`](crate::uring::fixed_file_stats) reports the use.
    ///
    /// Off by default: without it nothing changes, every SQE names its fd. Each slot costs the
    /// kernel a few bytes; the table size counts against `RLIMIT_NOFILE` (`EMFILE`). The table
    /// belongs to the runtime: do not register or update files through
    /// [`with_ring`](crate::uring::with_ring) on such a ring (`register_files` then fails with
    /// `EBUSY` anyway). [`build`](RuntimeBuilder::build) returns the kernel's error if the
    /// registration fails.
    #[must_use]
    pub fn with_fixed_files(mut self, n: u32) -> Self {
        self.fixed_files[0] = n;
        self
    }

    /// Add `n` slots to the fixed file table that the kernel allocates itself
    /// (`IORING_FILE_INDEX_ALLOC`), for
    /// [`TcpListener::accept_multi_direct`](crate::net::TcpListener::accept_multi_direct).
    ///
    /// They follow the [`with_fixed_files`](Self::with_fixed_files) slots, and the kernel is
    /// restricted to them (`IORING_REGISTER_FILE_ALLOC_RANGE`, Linux 6.0+; `build` fails on
    /// older kernels), so its allocations never collide with the runtime's. Can be used with
    /// or without `with_fixed_files`. Off by default.
    #[must_use]
    pub fn with_fixed_files_kernel_alloc(mut self, n: u32) -> Self {
        self.fixed_files[1] = n;
        self
    }

    /// Run at most `n` ready tasks (at least 1) before checking the driver for completions. The
    /// default is about twice the number of tasks queued at the start of the round.
    #[must_use]
    pub fn task_budget(mut self, n: usize) -> Self {
        self.task_budget = Some(n);
        self
    }

    /// Cooperative budget per task poll (off by default). With `Some(n)`, a task may take `n`
    /// ready results from I/O futures (op futures, `uring::MultiOp::next`, `uring::submit_chain`)
    /// and [`task::consume_budget`](crate::task::consume_budget) calls per poll; the next one
    /// wakes the task and returns `Pending`, so it goes to the back of the ready queue (the
    /// result stays queued, nothing is lost). `Some(0)` is treated as `Some(1)`.
    ///
    /// An op future is always `Pending` on its first poll, so this only limits tasks that keep
    /// finding results already queued: multishot `MultiOp`s, chains, CPU loops with
    /// `consume_budget`. The typed multishot streams (`AcceptStream`, `RecvStream`) are not
    /// budgeted. Exempt a future with [`task::unconstrained`](crate::task::unconstrained).
    /// Without `sync`, the main future of `block_on` is polled again right away when it yields,
    /// so the budget does not help there.
    #[must_use]
    pub fn coop_budget(mut self, n: Option<u32>) -> Self {
        self.coop_budget = n.map(|n| n.clamp(1, crate::task::coop::UNCONSTRAINED - 1));
        self
    }

    /// LIFO slot (off by default, tokio's design). A task woken by the task that is running
    /// (a waker hand-over: a channel send, a pool lease, a join) is polled right after it,
    /// instead of at the back of the ready queue. If another task is woken the same way before
    /// that, the earlier one goes to the back of the queue. After 3 consecutive slot polls the
    /// next slot task goes to the back of the queue. Slot polls count against
    /// [`task_budget`](Self::task_budget).
    ///
    /// Completions (CQEs), wakes from other threads or from outside a task, and a task waking
    /// itself ([`task::yield_now`](crate::task::yield_now), coop yields) always use the queue.
    ///
    /// With a small `task_budget(n)` this keeps hand-over chains from waiting a full queue
    /// traversal per hop.
    #[must_use]
    pub fn lifo_slot(mut self, on: bool) -> Self {
        self.lifo_slot = on;
        self
    }

    /// Let a kernel thread poll the submission queue (`IORING_SETUP_SQPOLL`), so submitting
    /// needs no `io_uring_enter` while the thread is awake. It sleeps after `idle` without work
    /// (rounded down to milliseconds) and the driver wakes it when needed. `cpu` pins it
    /// (`IORING_SETUP_SQ_AFF`); with `None` it inherits the affinity of the thread that calls
    /// `build()` (Linux 7.0). `cpu` may lie outside that thread's affinity mask, but not
    /// outside its cpuset (`EINVAL`). Unprivileged since 5.11 (older kernels need root for
    /// SQPOLL and `CAP_SYS_NICE` for `cpu`).
    ///
    /// The poller busy-spins on its CPU. Give it a CPU of its own: on the event loop's own CPU
    /// the two compete for one core (measured: about −85% throughput and client timeouts).
    /// `build()` reads the building thread's affinity and reports a conflict when the thread
    /// may run on one CPU only and `cpu` is `None` or that CPU: by default with one warning on
    /// stderr per process, with [`deny_sqpoll_conflict`](Self::deny_sqpoll_conflict) by
    /// failing with `InvalidInput`.
    ///
    /// Applied at `build()`, so it survives a later [`uring_builder`](Self::uring_builder)
    /// call. SQPOLL set directly on an `io_uring::Builder` passed to `uring_builder` keeps
    /// working, but is not checked.
    #[must_use]
    pub fn sqpoll(mut self, idle: Duration, cpu: Option<u32>) -> Self {
        self.sqpoll.cfg = Some((idle, cpu));
        self
    }

    /// Make `build()` fail with [`io::ErrorKind::InvalidInput`] instead of warning when the
    /// [`sqpoll`](Self::sqpoll) thread would share the event loop's only CPU. No effect without
    /// `sqpoll`.
    #[must_use]
    pub fn deny_sqpoll_conflict(mut self) -> Self {
        self.sqpoll.deny = true;
        self
    }

    /// Priority for tasks woken by small completions (off by default). With `Some(n)`, a task
    /// woken by a completion (CQE) whose result is below `n` (a byte count under `n`, an fd, 0,
    /// or an error) goes to a priority queue that runs before the ready queue, instead of to the
    /// back of the ready queue. Completions of `n` bytes or more (bulk reads and writes) keep
    /// the FIFO order and mark their task as a bulk task: its next 8 small completion wakes
    /// also use the queue, so the small steps of a bulk transfer (reading the next request,
    /// opening or closing a file) between its large chunks don't jump the queue either.
    /// `Some(0)` is the same as `None`.
    ///
    /// Use it with a small [`task_budget`](Self::task_budget) (and
    /// [`lifo_slot`](Self::lifo_slot)) when small requests share a core with bulk transfers: a
    /// request with k I/O steps then waits about k rounds of `task_budget` tasks instead of k
    /// traversals of the whole ready queue. Without a task budget a round already runs the
    /// whole queue, so this changes little. At most half of a round's budget goes to the
    /// priority queue while other tasks are ready, so bulk tasks keep running. Waking a task
    /// from another task, a timer, another thread, or by itself is not affected. A good `n` is
    /// above the request/response sizes and below the bulk chunk size, e.g. 16 KiB.
    #[must_use]
    pub fn small_io_priority(mut self, n: Option<u32>) -> Self {
        self.small_io_max = n.unwrap_or(0);
        self
    }
}

// ===== enable_timer related =====
mod time_wrap {
    pub trait TimeWrapable {}
}

impl time_wrap::TimeWrapable for IoUringDriver {}

impl<D: Driver> Buildable for TimeDriver<D>
where
    D: Buildable,
{
    /// Build the runtime
    fn build(this: RuntimeBuilder<Self>) -> io::Result<Runtime<TimeDriver<D>>> {
        let Runtime {
            driver,
            mut context,
        } = Buildable::build(RuntimeBuilder::<D> {
            entries: this.entries,
            urb: this.urb,
            opts: this.opts,
            task_budget: this.task_budget,
            coop_budget: this.coop_budget,
            lifo_slot: this.lifo_slot,
            small_io_max: this.small_io_max,
            iowq_max_workers: this.iowq_max_workers,
            fixed_files: this.fixed_files,
            sqpoll: this.sqpoll,
            #[cfg(feature = "sync")]
            blocking_handle: this.blocking_handle,
            _mark: PhantomData,
        })?;

        let timer_driver = TimeDriver::new(driver, Clock::new());
        context.time_handle = Some(timer_driver.handle.clone());
        Ok(Runtime {
            driver: timer_driver,
            context,
        })
    }
}

impl<D: time_wrap::TimeWrapable> RuntimeBuilder<D> {
    /// Enable all(currently only timer)
    #[must_use]
    pub fn enable_all(self) -> RuntimeBuilder<TimeDriver<D>> {
        self.enable_timer()
    }

    /// Enable timer
    #[must_use]
    pub fn enable_timer(self) -> RuntimeBuilder<TimeDriver<D>> {
        let Self {
            entries,
            urb,
            opts,
            task_budget,
            coop_budget,
            lifo_slot,
            small_io_max,
            iowq_max_workers,
            fixed_files,
            sqpoll,
            #[cfg(feature = "sync")]
            blocking_handle,
            ..
        } = self;
        RuntimeBuilder {
            entries,
            urb,
            opts,
            task_budget,
            coop_budget,
            lifo_slot,
            small_io_max,
            iowq_max_workers,
            fixed_files,
            sqpoll,
            #[cfg(feature = "sync")]
            blocking_handle,
            _mark: PhantomData,
        }
    }
}

impl<D> RuntimeBuilder<D> {
    /// Attach thread pool, this will overwrite blocking strategy.
    /// All `spawn_blocking` will be executed on given thread pool.
    #[cfg(feature = "sync")]
    #[must_use]
    pub fn attach_thread_pool(
        mut self,
        tp: Box<dyn crate::blocking::ThreadPool + Send + 'static>,
    ) -> Self {
        self.blocking_handle = crate::blocking::BlockingHandle::Attached(tp);
        self
    }

    /// Set blocking strategy, this will overwrite thread pool setting.
    /// If `BlockingStrategy::Panic` is used, it will panic if `spawn_blocking` on this thread.
    /// If `BlockingStrategy::ExecuteLocal` is used, it will execute with current thread, and may
    /// cause tasks high latency.
    /// Attaching a thread pool is recommended if `spawn_blocking` will be used.
    #[cfg(feature = "sync")]
    #[must_use]
    pub fn with_blocking_strategy(mut self, strategy: crate::blocking::BlockingStrategy) -> Self {
        self.blocking_handle = crate::blocking::BlockingHandle::Empty(strategy);
        self
    }
}
