//! Thread-per-core helper: N pinned runtimes, one per CPU, started and joined together.
//!
//! See [`ThreadPerCore`].

use std::{
    fmt,
    future::Future,
    io,
    os::fd::{AsRawFd, RawFd},
    sync::{mpsc, Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

use crate::{
    driver::{raw::RingHandle, Driver, IoUringDriver},
    time::TimeDriver,
    utils::bind_to_cpu_set,
    Buildable, RuntimeBuilder,
};

/// What a core knows about itself and its peers, passed to the closures of [`ThreadPerCore`].
#[derive(Clone)]
pub struct CoreInfo {
    /// Position of this core in the CPU list given to [`ThreadPerCore::new`] (0-based).
    pub index: usize,
    /// The CPU this core's thread is pinned to.
    pub cpu: usize,
    /// Number of cores (runtimes) started by this [`ThreadPerCore`].
    pub cores: usize,
    /// The ring of every core, in core order (`rings[index]` is this core's own ring), for
    /// [`uring::msg_ring`](crate::uring::msg_ring). Filled once *all* runtimes are built, so a
    /// core may message any peer from its first poll on. Empty in the
    /// [`builder`](ThreadPerCore::builder) closure, which runs before that.
    pub rings: Arc<[RingHandle]>,
}

impl fmt::Debug for CoreInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoreInfo")
            .field("index", &self.index)
            .field("cpu", &self.cpu)
            .field("cores", &self.cores)
            .field("rings", &self.rings.len())
            .finish()
    }
}

type BuilderFn<D> = dyn Fn(&CoreInfo) -> RuntimeBuilder<D> + Send + Sync;

/// Starts one pinned runtime per CPU and runs the same closure on each.
///
/// ```no_run
/// use monoio::{net::{ListenerOpts, TcpListener}, utils::ThreadPerCore};
///
/// let handle = ThreadPerCore::new([0, 1, 2, 3])
///     .run(|core| async move {
///         // Every core binds its own listener on the same port; the kernel spreads
///         // connections over the SO_REUSEPORT group by a hash of the 4-tuple.
///         let opts = ListenerOpts::new().reuse_port(true);
///         let listener = TcpListener::bind_with_config("0.0.0.0:8080", &opts).unwrap();
///         loop {
///             let (_conn, _) = listener.accept().await.unwrap();
///             // serve on this core (monoio::spawn)
///         }
///         #[allow(unreachable_code)]
///         core.index
///     })
///     .unwrap();
/// let _results = handle.join();
/// ```
///
/// - **Driver.** By default each core runs `RuntimeBuilder::new().enable_timer()` (driver
///   `TimeDriver<IoUringDriver>`). [`builder`](Self::builder) replaces it with any
///   `RuntimeBuilder<D>`, with or without the timer (`D` is then `IoUringDriver`).
/// - **Startup.** Each thread pins itself ([`bind_to_cpu_set`]), calls the builder closure and
///   builds its runtime. [`run`](Self::run) waits until every core has done so. If any core
///   fails (pinning, build, a panic in the builder closure, thread spawn), no closure runs: the
///   started cores drop their runtimes, their threads are joined and `run` returns the first
///   error (naming the core and CPU). Otherwise every core gets the full [`CoreInfo::rings`]
///   and runs `block_on(f(info))`.
/// - **Shutdown.** Each core's thread ends when its future returns (tasks still spawned on it
///   are dropped with the runtime). [`TpcHandle::join`] waits for all of them.
/// - **SO_REUSEPORT.** Bind a listener per core with
///   `TcpListener::bind_with_config(addr, &ListenerOpts::new().reuse_port(true))`. The kernel
///   picks the socket by a hash of the connection's addresses, so the split is only even on
///   average. Steering by CPU (`SO_INCOMING_CPU`, a `SO_ATTACH_REUSEPORT_CBPF` program) is not
///   done here.
pub struct ThreadPerCore<D = TimeDriver<IoUringDriver>> {
    cpus: Vec<usize>,
    builder: Arc<BuilderFn<D>>,
    attach_wq: bool,
    thread_name: String,
}

impl<D> fmt::Debug for ThreadPerCore<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreadPerCore")
            .field("cpus", &self.cpus)
            .field("attach_wq", &self.attach_wq)
            .field("thread_name", &self.thread_name)
            .finish_non_exhaustive()
    }
}

impl ThreadPerCore {
    /// One runtime per CPU in `cpus`, in that order (core `i` is pinned to the `i`-th CPU).
    /// Each core uses `RuntimeBuilder::new().enable_timer()` unless [`builder`](Self::builder)
    /// says otherwise.
    pub fn new(cpus: impl IntoIterator<Item = usize>) -> Self {
        Self {
            cpus: cpus.into_iter().collect(),
            builder: Arc::new(|_: &CoreInfo| RuntimeBuilder::new().enable_timer()),
            attach_wq: false,
            thread_name: "monoio-core".to_string(),
        }
    }
}

impl<D> ThreadPerCore<D> {
    /// The runtime builder of each core, called on that core's thread after pinning (so
    /// [`CoreInfo::rings`] is still empty). Return `RuntimeBuilder::<IoUringDriver>::new()`
    /// for a runtime without the timer, or add `.enable_timer()`.
    ///
    /// Don't set `attach_wq` yourself; use [`attach_wq`](Self::attach_wq), which is applied
    /// after this closure (so it survives `uring_builder`).
    #[must_use]
    pub fn builder<D2, B>(self, f: B) -> ThreadPerCore<D2>
    where
        B: Fn(&CoreInfo) -> RuntimeBuilder<D2> + Send + Sync + 'static,
    {
        ThreadPerCore {
            cpus: self.cpus,
            builder: Arc::new(f),
            attach_wq: self.attach_wq,
            thread_name: self.thread_name,
        }
    }

    /// Share one io-wq (`IORING_SETUP_ATTACH_WQ`) between all cores. Core 0 is built first and
    /// is the primary; the others attach to its ring. Core 0's runtime is kept alive until the
    /// threads of all other cores have ended (its thread waits for them after its own future
    /// returned, or while unwinding from a panic), so do not make core 0's future wait for a
    /// thread that waits for core 0's *thread* to end.
    ///
    /// Default off: each ring has its own io-wq.
    #[must_use]
    pub fn attach_wq(mut self, attach: bool) -> Self {
        self.attach_wq = attach;
        self
    }

    /// Thread name prefix; core `i` is named `{prefix}-{i}`. Default `monoio-core`.
    #[must_use]
    pub fn thread_name(mut self, prefix: impl Into<String>) -> Self {
        self.thread_name = prefix.into();
        self
    }

    /// Start every core and run `f(info)` on each with `block_on`.
    ///
    /// Returns once all runtimes are built (see the type docs for the error behaviour), with a
    /// handle to join the cores. `cpus` must not be empty (`InvalidInput`).
    pub fn run<F, Fut, T>(self, f: F) -> io::Result<TpcHandle<T>>
    where
        D: Buildable + Driver + AsRawFd + 'static,
        F: Fn(CoreInfo) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let cores = self.cpus.len();
        if cores == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ThreadPerCore: empty CPU list",
            ));
        }
        let f = Arc::new(f);
        // With attach_wq, core 0 waits until the others are gone.
        let latch = Arc::new(Latch::new(if self.attach_wq { cores - 1 } else { 0 }));

        let mut started: Vec<Started<T>> = Vec::with_capacity(cores);
        let mut rings: Vec<RingHandle> = Vec::with_capacity(cores);
        let mut error: Option<io::Error> = None;

        let mut index = 0;
        while index < cores {
            let wq_fd = match (self.attach_wq, rings.first()) {
                (true, Some(primary)) => Some(primary.as_raw_fd()),
                _ => None,
            };
            match self.spawn_core(index, wq_fd, &f, &latch) {
                Ok(s) => started.push(s),
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
            index += 1;
            // With attach_wq, core 0 must exist before the others can attach to it.
            if self.attach_wq && index == 1 {
                match collect(&self.cpus, &mut started[..1], &mut rings) {
                    Ok(()) => {}
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
        }
        if self.attach_wq {
            // Cores that were never started will not count down themselves.
            for _ in index.max(1)..cores {
                latch.count_down();
            }
        }
        if error.is_none() {
            let from = rings.len();
            if let Err(e) = collect(&self.cpus, &mut started[from..], &mut rings) {
                error = Some(e);
            }
        }

        if let Some(e) = error {
            for s in &started {
                let _ = s.go.send(None);
            }
            for s in started {
                let _ = s.thread.join();
            }
            return Err(e);
        }

        let rings: Arc<[RingHandle]> = rings.into();
        let mut threads = Vec::with_capacity(cores);
        for s in started {
            // The thread waits on this receiver, so the send cannot fail unless it panicked,
            // which `join` reports.
            let _ = s.go.send(Some(rings.clone()));
            threads.push(s.thread);
        }
        Ok(TpcHandle { threads })
    }

    fn spawn_core<F, Fut, T>(
        &self,
        index: usize,
        wq_fd: Option<RawFd>,
        f: &Arc<F>,
        latch: &Arc<Latch>,
    ) -> io::Result<Started<T>>
    where
        D: Buildable + Driver + AsRawFd + 'static,
        F: Fn(CoreInfo) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let (ready_tx, ready) = mpsc::channel::<io::Result<RingHandle>>();
        let (go, go_rx) = mpsc::channel::<Option<Arc<[RingHandle]>>>();
        let cpu = self.cpus[index];
        let cores = self.cpus.len();
        let attach = self.attach_wq;
        let builder = self.builder.clone();
        let f = f.clone();
        let latch = latch.clone();

        let thread = thread::Builder::new()
            .name(format!("{}-{index}", self.thread_name))
            .spawn(move || -> Option<T> {
                // Declared first, dropped last: a secondary counts down after its runtime is gone.
                let _secondary = (attach && index > 0).then(|| CountDown(latch.clone()));
                let mut info = CoreInfo {
                    index,
                    cpu,
                    cores,
                    rings: Arc::from(Vec::new()),
                };
                let built = bind_to_cpu_set(Some(cpu))
                    .map_err(|e| io::Error::from_raw_os_error(e as i32))
                    .and_then(|()| {
                        let mut b = builder(&info);
                        if let Some(fd) = wq_fd {
                            b = b.attach_wq(fd);
                        }
                        <D as Buildable>::build(b)
                    })
                    .and_then(|rt| RingHandle::new(rt.driver()).map(|h| (rt, h)));
                let mut rt = match built {
                    Ok((rt, handle)) => {
                        let _ = ready_tx.send(Ok(handle));
                        rt
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return None;
                    }
                };
                drop(ready_tx);
                // Declared after the runtime, dropped before it: the primary's runtime lives
                // until every secondary has ended.
                let _primary = (attach && index == 0).then(|| WaitZero(latch.clone()));
                info.rings = go_rx.recv().ok().flatten()?;
                Some(rt.block_on(f(info)))
            })?;
        Ok(Started { thread, ready, go })
    }
}

/// Wait for the startup reports of `started`, appending their rings.
fn collect<T>(
    cpus: &[usize],
    started: &mut [Started<T>],
    rings: &mut Vec<RingHandle>,
) -> io::Result<()> {
    for s in started {
        let index = rings.len();
        let cpu = cpus[index];
        match s.ready.recv() {
            Ok(Ok(handle)) => rings.push(handle),
            Ok(Err(e)) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("ThreadPerCore: core {index} (cpu {cpu}) failed to start: {e}"),
                ))
            }
            Err(_) => {
                return Err(io::Error::other(format!(
                    "ThreadPerCore: core {index} (cpu {cpu}) panicked during startup"
                )))
            }
        }
    }
    Ok(())
}

struct Started<T> {
    thread: JoinHandle<Option<T>>,
    ready: mpsc::Receiver<io::Result<RingHandle>>,
    go: mpsc::Sender<Option<Arc<[RingHandle]>>>,
}

/// The running cores of a [`ThreadPerCore`].
#[derive(Debug)]
pub struct TpcHandle<T> {
    threads: Vec<JoinHandle<Option<T>>>,
}

impl<T> TpcHandle<T> {
    /// Number of cores.
    pub fn len(&self) -> usize {
        self.threads.len()
    }

    /// Always false: `run` never starts zero cores.
    pub fn is_empty(&self) -> bool {
        self.threads.is_empty()
    }

    /// Wait for every core's thread and return each result in core order; `Err` holds the
    /// panic payload of a core whose future (or runtime) panicked.
    pub fn join(self) -> Vec<thread::Result<T>> {
        self.threads
            .into_iter()
            .map(|t| match t.join() {
                Ok(Some(v)) => Ok(v),
                // Not reachable: every thread got its rings before the handle was returned.
                Ok(None) => Err(Box::new("ThreadPerCore: core did not start") as _),
                Err(p) => Err(p),
            })
            .collect()
    }
}

struct Latch {
    count: Mutex<usize>,
    zero: Condvar,
}

impl Latch {
    fn new(count: usize) -> Self {
        Self {
            count: Mutex::new(count),
            zero: Condvar::new(),
        }
    }

    fn count_down(&self) {
        let mut c = self.count.lock().unwrap_or_else(|e| e.into_inner());
        *c = c.saturating_sub(1);
        if *c == 0 {
            self.zero.notify_all();
        }
    }

    fn wait_zero(&self) {
        let mut c = self.count.lock().unwrap_or_else(|e| e.into_inner());
        while *c > 0 {
            c = self.zero.wait(c).unwrap_or_else(|e| e.into_inner());
        }
    }
}

struct CountDown(Arc<Latch>);

impl Drop for CountDown {
    fn drop(&mut self) {
        self.0.count_down();
    }
}

struct WaitZero(Arc<Latch>);

impl Drop for WaitZero {
    fn drop(&mut self) {
        self.0.wait_zero();
    }
}
