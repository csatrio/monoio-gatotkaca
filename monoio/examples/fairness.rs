//! `fairness`: a one-core load reproducer for round 2 (M1, M2, M3).
//!
//! One server runtime pinned to `--cpu` serves three kinds of traffic on loopback, while client
//! runtimes on the other CPUs generate the load and measure it:
//! - **bulk**: closed-loop connections that ask for `--bulk-size` bytes, written by the server in
//!   `--chunk` units (one `write_all` each), like the benchmark's `/stream/1024`;
//! - **rr**: open-loop request/response connections at `--rate` requests/s in total, latency
//!   measured from the *scheduled* send time (coordinated-omission corrected); with `--hop` the
//!   connection task hands each request to a worker task through a lease queue of wakers (models
//!   the proxy's upstream pool), without it the connection task answers directly (models json);
//! - **accept ramp**: `--accept-ramp K` extra rr connections opened during the first
//!   `--ramp-secs` of the measured window, accepted with `--accept single|multi`.
//!
//! Run: `cargo run --release -p adaptif-gatotkaca-monoio --example fairness -- --help`.
//! Notes and flag reference: `docs/performance-improvement/round2/W0-repro.md`.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::poll_fn,
    io,
    net::SocketAddr,
    rc::Rc,
    sync::{mpsc, Arc, OnceLock},
    task::{Poll, Waker},
    time::{Duration, Instant},
};

use bytes::Bytes;
use monoio::{
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRentExt},
    net::{AcceptFlags, ListenerOpts, TcpListener, TcpStream},
    IoUringDriver, RuntimeBuilder,
};

const USAGE: &str = "\
fairness: one-core load reproducer (bulk streams vs small request/response, hop, accept ramp)

USAGE: fairness [FLAGS]

server
  --cpu C              server CPU (default 2)
  --client-cpus a,b,.. client CPUs, one client thread each (default: all but --cpu)
  --entries N          io_uring entries of the server ring (default 4096)
  --budget N           RuntimeBuilder::task_budget(N)
  --coop               RuntimeBuilder::coop_taskrun()
  --coop-budget N      RuntimeBuilder::coop_budget(Some(N)) (per-task coop budget)
  --lifo               RuntimeBuilder::lifo_slot(true)
  --small-io SZ        RuntimeBuilder::small_io_priority(Some(SZ)) (tasks woken by completions
                       below SZ bytes run first)
  --skip-empty-enter   RuntimeBuilder::skip_empty_enter() (reap without entering when the SQ is empty)
  --defer              RuntimeBuilder::defer_taskrun()
  --accept single|multi  accept loop: one accept() at a time + spawn (default), or accept_multi
workload
  --bulk N             bulk connections (default 0)
  --bulk-size SZ       bytes per bulk response (default 1m)
  --chunk SZ           server write unit of a bulk response (default 128k)
  --rr M               request/response connections (default 0)
  --rate R             rr requests/s in total over all rr connections incl. ramp ones (default 1000)
  --rr-size SZ         rr response size (default 8192; request is 64 bytes)
  --timeout S          client timeout per rr request (default 10)
  --hop                answer rr requests through a worker task leased from a pool (proxy model)
  --pool P             workers in the --hop pool (default 64)
  --hop-nops N         io_uring NOP round trips a worker does per request (default 0)
  --upstream           proxy model with a real upstream: a share of the rr connections (--proxy-frac)
                       lease one of --pool upstream connections (strict FIFO waiters, like the SUT's
                       pool), write --rr-size bytes to an echo server on --upstream-cpu, read them
                       back, release, answer; the others answer directly (json model). Reports
                       both classes and the server-side time of each proxy step
  --proxy-frac F       share of rr connections that are proxy connections (default 0.5)
  --upstream-cpu C     CPU of the upstream echo thread (default: the first client CPU, removed from
                       the client set)
  --accept-ramp K      extra rr connections opened during the first --ramp-secs of the window
  --ramp-secs S        ramp length (default 5)
run
  --warmup S           seconds before the measured window (default 2)
  --duration S         measured window in seconds (default 10)
  --json               also print one JSON object
sizes take k/m suffixes (KiB/MiB); seconds may be fractional";

// ===== arguments =====

#[derive(Clone, Debug)]
struct Args {
    cpu: usize,
    client_cpus: Vec<usize>,
    entries: u32,
    budget: Option<usize>,
    coop: bool,
    defer: bool,
    coop_budget: Option<u32>,
    lifo: bool,
    small_io: Option<u32>,
    skip_empty_enter: bool,
    accept_multi: bool,
    bulk: usize,
    bulk_size: usize,
    chunk: usize,
    rr: usize,
    rate: f64,
    rr_size: usize,
    timeout: Duration,
    hop: bool,
    pool: usize,
    hop_nops: usize,
    upstream: bool,
    proxy_frac: f64,
    upstream_cpu: Option<usize>,
    ramp: usize,
    ramp_secs: f64,
    warmup: f64,
    duration: f64,
    json: bool,
}

fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.trim().to_ascii_lowercase();
    let (num, mul) = if let Some(n) = s.strip_suffix(['k']) {
        (n, 1024)
    } else if let Some(n) = s.strip_suffix(['m']) {
        (n, 1024 * 1024)
    } else {
        (s.as_str(), 1)
    };
    num.parse::<usize>()
        .map(|n| n * mul)
        .map_err(|_| format!("bad size `{s}`"))
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        cpu: 2,
        client_cpus: Vec::new(),
        entries: 4096,
        budget: None,
        coop: false,
        coop_budget: None,
        lifo: false,
        small_io: None,
        skip_empty_enter: false,
        defer: false,
        accept_multi: false,
        bulk: 0,
        bulk_size: 1024 * 1024,
        chunk: 128 * 1024,
        rr: 0,
        rate: 1000.0,
        rr_size: 8192,
        timeout: Duration::from_secs(10),
        hop: false,
        pool: 64,
        hop_nops: 0,
        upstream: false,
        proxy_frac: 0.5,
        upstream_cpu: None,
        ramp: 0,
        ramp_secs: 5.0,
        warmup: 2.0,
        duration: 10.0,
        json: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        fn num<T: std::str::FromStr>(flag: &str, v: String) -> Result<T, String> {
            v.parse().map_err(|_| format!("bad value `{v}` for {flag}"))
        }
        match flag.as_str() {
            "--help" | "-h" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "--cpu" => a.cpu = num(&flag, val()?)?,
            "--client-cpus" => {
                a.client_cpus = val()?
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| num(&flag, s.to_string()))
                    .collect::<Result<_, _>>()?
            }
            "--entries" => a.entries = num(&flag, val()?)?,
            "--budget" => a.budget = Some(num(&flag, val()?)?),
            "--coop" => a.coop = true,
            "--coop-budget" => a.coop_budget = Some(num(&flag, val()?)?),
            "--lifo" => a.lifo = true,
            "--small-io" => a.small_io = Some(parse_size(&val()?)? as u32),
            "--skip-empty-enter" => a.skip_empty_enter = true,
            "--defer" => a.defer = true,
            "--accept" => match val()?.as_str() {
                "single" => a.accept_multi = false,
                "multi" => a.accept_multi = true,
                v => return Err(format!("--accept takes single|multi, not `{v}`")),
            },
            "--bulk" => a.bulk = num(&flag, val()?)?,
            "--bulk-size" => a.bulk_size = parse_size(&val()?)?,
            "--chunk" => a.chunk = parse_size(&val()?)?,
            "--rr" => a.rr = num(&flag, val()?)?,
            "--rate" => a.rate = num(&flag, val()?)?,
            "--rr-size" => a.rr_size = parse_size(&val()?)?,
            "--timeout" => a.timeout = Duration::from_secs_f64(num(&flag, val()?)?),
            "--hop" => a.hop = true,
            "--pool" => a.pool = num(&flag, val()?)?,
            "--hop-nops" => a.hop_nops = num(&flag, val()?)?,
            "--upstream" => a.upstream = true,
            "--proxy-frac" => a.proxy_frac = num(&flag, val()?)?,
            "--upstream-cpu" => a.upstream_cpu = Some(num(&flag, val()?)?),
            "--accept-ramp" => a.ramp = num(&flag, val()?)?,
            "--ramp-secs" => a.ramp_secs = num(&flag, val()?)?,
            "--warmup" => a.warmup = num(&flag, val()?)?,
            "--duration" => a.duration = num(&flag, val()?)?,
            "--json" => a.json = true,
            _ => return Err(format!("unknown flag `{flag}` (see --help)")),
        }
    }
    if a.chunk == 0 || a.bulk_size == 0 || a.rr_size == 0 || a.pool == 0 {
        return Err("--chunk, --bulk-size, --rr-size and --pool must be > 0".into());
    }
    if a.duration <= 0.0 || a.warmup < 0.0 || a.rate < 0.0 || a.ramp_secs < 0.0 {
        return Err("--duration must be > 0; --warmup, --rate, --ramp-secs >= 0".into());
    }
    if a.client_cpus.is_empty() {
        let n = std::thread::available_parallelism().map_or(1, |n| n.get());
        a.client_cpus = (0..n).filter(|&c| c != a.cpu).collect();
        if a.client_cpus.is_empty() {
            a.client_cpus.push(a.cpu); // single-CPU machine: share it
        }
    }
    if a.upstream {
        if a.hop {
            return Err("--upstream and --hop are exclusive".into());
        }
        if !(0.0..=1.0).contains(&a.proxy_frac) {
            return Err("--proxy-frac must be in 0..=1".into());
        }
        let up = match a.upstream_cpu {
            Some(c) => c,
            None => a.client_cpus[0],
        };
        a.upstream_cpu = Some(up);
        if a.client_cpus.len() > 1 {
            a.client_cpus.retain(|&c| c != up);
        }
    }
    Ok(a)
}

/// Builder tuning of the measured (server) runtime.
fn apply_tune(mut b: RuntimeBuilder<IoUringDriver>, a: &Args) -> RuntimeBuilder<IoUringDriver> {
    b = b.with_entries(a.entries);
    if let Some(n) = a.budget {
        b = b.task_budget(n);
    }
    if a.coop {
        b = b.coop_taskrun();
    }
    if a.defer {
        b = b.defer_taskrun();
    }
    if a.coop_budget.is_some() {
        b = b.coop_budget(a.coop_budget);
    }
    if a.lifo {
        b = b.lifo_slot(true);
    }
    if a.skip_empty_enter {
        b = b.skip_empty_enter();
    }
    if a.small_io.is_some() {
        b = b.small_io_priority(a.small_io);
    }
    b
}

fn tune_label(a: &Args) -> String {
    let mut v = Vec::new();
    if let Some(n) = a.budget {
        v.push(format!("budget{n}"));
    }
    if a.coop {
        v.push("coop".into());
    }
    if a.defer {
        v.push("defer".into());
    }
    if let Some(n) = a.coop_budget {
        v.push(format!("coopbudget{n}"));
    }
    if a.lifo {
        v.push("lifo".into());
    }
    if a.skip_empty_enter {
        v.push("skipenter".into());
    }
    if let Some(n) = a.small_io {
        v.push(format!("smallio{n}"));
    }
    if v.is_empty() {
        "default".into()
    } else {
        v.join(",")
    }
}

// ===== histogram (log-bucketed, microseconds, 1 us .. ~100 s, <= ~1.6% error) =====

const SUB: u64 = 32; // buckets per power of two above 64 us
const NBUCKETS: usize = 64 + 40 * SUB as usize;

#[derive(Clone)]
struct Hist {
    counts: Vec<u64>,
    total: u64,
    max: u64,
}

impl Hist {
    fn new() -> Self {
        Hist {
            counts: vec![0; NBUCKETS],
            total: 0,
            max: 0,
        }
    }

    fn index(v: u64) -> usize {
        if v < 64 {
            return v as usize;
        }
        let e = 63 - v.leading_zeros() as u64; // >= 6
        let shift = e - 5;
        let m = v >> shift; // in [32, 64)
        (64 + (shift - 1) * SUB + (m - SUB)).min(NBUCKETS as u64 - 1) as usize
    }

    fn value(idx: usize) -> u64 {
        if idx < 64 {
            return idx as u64;
        }
        let i = idx as u64 - 64;
        let shift = i / SUB + 1;
        let m = i % SUB + SUB;
        (m << shift) + (1 << shift) / 2
    }

    fn record(&mut self, d: Duration) {
        let us = d.as_micros().min(u64::MAX as u128) as u64;
        self.counts[Self::index(us)] += 1;
        self.total += 1;
        self.max = self.max.max(us);
    }

    fn merge(&mut self, o: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(&o.counts) {
            *a += b;
        }
        self.total += o.total;
        self.max = self.max.max(o.max);
    }

    /// Quantile in milliseconds (0 when empty).
    fn q_ms(&self, q: f64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let rank = ((q * self.total as f64).ceil() as u64).clamp(1, self.total);
        let mut seen = 0;
        for (i, c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Self::value(i).min(self.max) as f64 / 1000.0;
            }
        }
        self.max as f64 / 1000.0
    }

    fn max_ms(&self) -> f64 {
        self.max as f64 / 1000.0
    }
}

// ===== shared timeline =====

#[derive(Clone, Copy)]
struct Window {
    /// Load starts (warmup begins).
    load: Instant,
    start: Instant,
    end: Instant,
}

impl Window {
    fn contains(&self, t: Instant) -> bool {
        t >= self.start && t < self.end
    }
}

type SharedWindow = Arc<OnceLock<Window>>;

async fn wait_window(w: &SharedWindow) -> Window {
    loop {
        if let Some(w) = w.get() {
            return *w;
        }
        monoio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn sleep_until(t: Instant) {
    let now = Instant::now();
    if t > now {
        monoio::time::sleep(t - now).await;
    }
}

fn raise_nofile() {
    use monoio::uring::libc;
    unsafe {
        let mut r: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) == 0 && r.rlim_cur < r.rlim_max {
            r.rlim_cur = r.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &r);
        }
    }
}

fn nofile() -> u64 {
    use monoio::uring::libc;
    unsafe {
        let mut r: libc::rlimit = std::mem::zeroed();
        libc::getrlimit(libc::RLIMIT_NOFILE, &mut r);
        r.rlim_cur as u64
    }
}

// ===== server =====

struct ServerResult {
    accepts_window: u64,
    accepts_peak_1s: u64,
    accept_to_first_read: Hist,
    control_late_ms: f64,
    steps: Vec<Hist>,
    #[cfg(feature = "metrics")]
    metrics: (String, String),
}

struct ServerState {
    window: Cell<Option<Window>>,
    accept_buckets: RefCell<Vec<u64>>,
    accepts_window: Cell<u64>,
    accept_to_first_read: RefCell<Hist>,
    bulk_chunk: Bytes,
    rr_resp: Bytes,
    bulk_size: usize,
    pool: Option<Pool>,
    hop_nops: usize,
    /// `--upstream`: the upstream connection pool and its address.
    up: Option<UpPool>,
    /// `--upstream`: server-side time of each proxy step in the window, see [`Step`].
    steps: RefCell<Vec<Hist>>,
}

/// Server-side steps of a proxy request (`--upstream`), each from the end of the previous one.
#[derive(Clone, Copy)]
enum Step {
    /// Request read → lease granted (idle connection or permission to open one).
    Lease,
    /// → request written to the upstream (`write_all` returned).
    UpWrite,
    /// → echo read back (`read_exact` returned); lease released right after.
    UpRead,
    /// → response written to the client.
    Reply,
    /// json requests: request read → response written.
    JsonReply,
}
const STEPS: [&str; 5] = ["lease", "up_write", "up_read", "reply", "json_reply"];

impl ServerState {
    fn step(&self, s: Step, from: &mut Instant) {
        let now = Instant::now();
        if self.window.get().is_some_and(|w| w.contains(now)) {
            self.steps.borrow_mut()[s as usize].record(now - *from);
        }
        *from = now;
    }
}

impl ServerState {
    fn on_accept(&self) -> Instant {
        let now = Instant::now();
        if let Some(w) = self.window.get() {
            if w.contains(now) {
                self.accepts_window.set(self.accepts_window.get() + 1);
                let sec = (now - w.start).as_secs() as usize;
                let mut b = self.accept_buckets.borrow_mut();
                if b.len() <= sec {
                    b.resize(sec + 1, 0);
                }
                b[sec] += 1;
            }
        }
        now
    }
}

/// The `--hop` pool: `free` leases, and a FIFO of waiters that a released lease is handed to
/// directly (`Waker::wake`), like the benchmark's upstream pool.
struct Pool {
    free: RefCell<Vec<usize>>,
    waiters: RefCell<VecDeque<Rc<LeaseWaiter>>>,
    workers: Vec<Rc<WorkerSlot>>,
}

#[derive(Default)]
struct LeaseWaiter {
    lease: Cell<Option<usize>>,
    waker: RefCell<Option<Waker>>,
}

#[derive(Default)]
struct WorkerSlot {
    job: Cell<bool>,
    done: Cell<bool>,
    worker: RefCell<Option<Waker>>,
    requester: RefCell<Option<Waker>>,
}

impl Pool {
    fn new(n: usize) -> Self {
        Pool {
            free: RefCell::new((0..n).rev().collect()),
            waiters: RefCell::new(VecDeque::new()),
            workers: (0..n).map(|_| Rc::new(WorkerSlot::default())).collect(),
        }
    }

    async fn lease(&self) -> usize {
        if self.waiters.borrow().is_empty() {
            if let Some(i) = self.free.borrow_mut().pop() {
                return i;
            }
        }
        let w = Rc::new(LeaseWaiter::default());
        self.waiters.borrow_mut().push_back(w.clone());
        poll_fn(|cx| match w.lease.take() {
            Some(i) => Poll::Ready(i),
            None => {
                *w.waker.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    fn release(&self, i: usize) {
        let next = self.waiters.borrow_mut().pop_front();
        match next {
            Some(w) => {
                w.lease.set(Some(i));
                if let Some(k) = w.waker.borrow_mut().take() {
                    k.wake();
                }
            }
            None => self.free.borrow_mut().push(i),
        }
    }

    /// One request through worker `i`: wake the worker, wait for its wake back.
    async fn run_job(&self, i: usize) {
        let slot = &self.workers[i];
        slot.job.set(true);
        if let Some(k) = slot.worker.borrow_mut().take() {
            k.wake();
        }
        poll_fn(|cx| {
            if slot.done.replace(false) {
                Poll::Ready(())
            } else {
                *slot.requester.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

/// The `--upstream` pool, the same bookkeeping as the benchmark's `common::pool`: at most `cap`
/// connections; idle ones are reused; when none is idle and `cap` exist, the caller waits in a
/// strict FIFO; returning a connection wakes the front waiter, which takes it when it runs.
struct UpPool {
    addr: SocketAddr,
    cap: usize,
    live: Cell<usize>,
    idle: RefCell<Vec<TcpStream>>,
    next_id: Cell<u64>,
    waiters: RefCell<VecDeque<(u64, Waker)>>,
}

impl UpPool {
    fn new(addr: SocketAddr, cap: usize) -> Self {
        UpPool {
            addr,
            cap,
            live: Cell::new(0),
            idle: RefCell::new(Vec::new()),
            next_id: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
        }
    }

    fn available(&self) -> bool {
        !self.idle.borrow().is_empty() || self.live.get() < self.cap
    }

    fn wake_front(&self) {
        if let Some((_, w)) = self.waiters.borrow().front() {
            w.wake_by_ref();
        }
    }

    /// A connection: `Some` idle one, `None` = open a new one (the slot is counted).
    async fn acquire(&self) -> Option<TcpStream> {
        let mut id: Option<u64> = None;
        poll_fn(|cx| {
            let my_turn = match (id, self.waiters.borrow().front()) {
                (None, None) => true,
                (Some(me), Some((front, _))) => me == *front,
                _ => false,
            };
            if my_turn && self.available() {
                let c = self.idle.borrow_mut().pop();
                if c.is_none() {
                    self.live.set(self.live.get() + 1);
                }
                if id.is_some() {
                    self.waiters.borrow_mut().pop_front();
                }
                if self.available() {
                    self.wake_front();
                }
                return Poll::Ready(c);
            }
            match id {
                None => {
                    let n = self.next_id.get();
                    self.next_id.set(n + 1);
                    self.waiters.borrow_mut().push_back((n, cx.waker().clone()));
                    id = Some(n);
                }
                Some(me) => {
                    if let Some(e) = self.waiters.borrow_mut().iter_mut().find(|(w, _)| *w == me) {
                        e.1 = cx.waker().clone();
                    }
                }
            }
            Poll::Pending
        })
        .await
    }

    fn put(&self, c: TcpStream) {
        self.idle.borrow_mut().push(c);
        self.wake_front();
    }

    fn discard(&self) {
        self.live.set(self.live.get() - 1);
        self.wake_front();
    }
}

/// One proxy request through the upstream pool; `false` on an upstream error.
async fn proxy_request(st: &ServerState, up: &UpPool, t: &mut Instant) -> bool {
    let conn = up.acquire().await;
    st.step(Step::Lease, t);
    let mut conn = match conn {
        Some(c) => c,
        None => match TcpStream::connect(up.addr).await {
            Ok(c) => {
                let _ = c.set_nodelay(true);
                c
            }
            Err(_) => {
                up.discard();
                return false;
            }
        },
    };
    let (r, _) = conn.write_all(st.rr_resp.clone()).await;
    st.step(Step::UpWrite, t);
    if r.is_err() {
        up.discard();
        return false;
    }
    let (r, _) = conn.read_exact(Vec::with_capacity(st.rr_resp.len())).await;
    st.step(Step::UpRead, t);
    if r.is_err() {
        up.discard();
        return false;
    }
    up.put(conn);
    true
}

/// `--upstream`: the echo server, one task per connection, on its own thread and CPU.
fn upstream_thread(cpu: usize, size: usize, addr_tx: mpsc::Sender<io::Result<SocketAddr>>) {
    if let Err(e) = monoio::utils::bind_to_cpu_set([cpu]) {
        eprintln!("upstream: bind to cpu {cpu} failed: {e:?}");
    }
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(1024)
        .enable_timer()
        .build()
        .expect("upstream runtime");
    rt.block_on(async move {
        let opts = ListenerOpts::new().backlog(4096);
        let l = match TcpListener::bind_with_config("127.0.0.1:0", &opts) {
            Ok(l) => l,
            Err(e) => {
                let _ = addr_tx.send(Err(e));
                return;
            }
        };
        let _ = addr_tx.send(Ok(l.local_addr().unwrap()));
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                continue;
            };
            let _ = s.set_nodelay(true);
            monoio::spawn(async move {
                let mut buf = Vec::with_capacity(size);
                loop {
                    buf.clear();
                    let (r, b) = s.read_exact(buf).await;
                    if r.is_err() {
                        return;
                    }
                    let (r, b) = s.write_all(b).await;
                    buf = b;
                    if r.is_err() {
                        return;
                    }
                }
            });
        }
    });
}

async fn worker_task(slot: Rc<WorkerSlot>, nops: usize) {
    loop {
        poll_fn(|cx| {
            if slot.job.replace(false) {
                Poll::Ready(())
            } else {
                *slot.worker.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
        for _ in 0..nops {
            // SAFETY: a NOP points at no memory.
            let op =
                unsafe { monoio::uring::submit(monoio::uring::opcode::Nop::new().build(), ()) };
            if let Ok(op) = op {
                let _ = op.await;
            }
        }
        // "Produce" the response (the shared response buffer) and hand it back.
        slot.done.set(true);
        if let Some(k) = slot.requester.borrow_mut().take() {
            k.wake();
        }
    }
}

async fn serve_bulk(st: Rc<ServerState>, mut s: TcpStream) {
    let mut buf = Vec::with_capacity(16);
    loop {
        let (r, b) = s.read(buf).await;
        buf = b;
        match r {
            Ok(0) | Err(_) => return,
            Ok(_) => buf.clear(),
        }
        let mut left = st.bulk_size;
        while left > 0 {
            let n = left.min(st.bulk_chunk.len());
            let (r, _) = s.write_all(st.bulk_chunk.slice(..n)).await;
            if r.is_err() {
                return;
            }
            left -= n;
        }
    }
}

async fn serve_rr(st: Rc<ServerState>, mut s: TcpStream, accepted: Instant) {
    let mut buf = Vec::with_capacity(64);
    let mut first = true;
    loop {
        let (r, b) = s.read_exact(buf).await;
        buf = b;
        if r.is_err() {
            return;
        }
        if first {
            first = false;
            if st.window.get().is_some_and(|w| w.contains(accepted)) {
                st.accept_to_first_read
                    .borrow_mut()
                    .record(accepted.elapsed());
            }
        }
        let mut t = Instant::now();
        let proxy = match &st.up {
            Some(up) if buf[0] == b'p' => {
                if !proxy_request(&st, up, &mut t).await {
                    return;
                }
                true
            }
            _ => false,
        };
        if let Some(pool) = &st.pool {
            let i = pool.lease().await;
            pool.run_job(i).await;
            pool.release(i);
        }
        let (r, _) = s.write_all(st.rr_resp.clone()).await;
        if r.is_err() {
            return;
        }
        if st.up.is_some() {
            st.step(if proxy { Step::Reply } else { Step::JsonReply }, &mut t);
        }
    }
}

async fn accept_loop(st: Rc<ServerState>, l: TcpListener, bulk: bool, multi: bool) {
    let spawn_conn = |s: TcpStream| {
        let at = st.on_accept();
        let _ = s.set_nodelay(true);
        if bulk {
            monoio::spawn(serve_bulk(st.clone(), s));
        } else {
            monoio::spawn(serve_rr(st.clone(), s, at));
        }
    };
    if multi {
        loop {
            let mut acc = match l.accept_multi(AcceptFlags::NONBLOCK | AcceptFlags::CLOEXEC) {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("accept_multi: {e}");
                    monoio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            };
            // Re-arm when the kernel ends the multishot (error or `None`).
            while let Some(r) = acc.next_stream().await {
                match r {
                    Ok(s) => spawn_conn(s),
                    Err(e) => eprintln!("accept_multi item: {e}"),
                }
            }
        }
    } else {
        loop {
            match l.accept().await {
                Ok((s, _)) => spawn_conn(s),
                Err(e) => {
                    eprintln!("accept: {e}");
                    monoio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
}

#[cfg(feature = "metrics")]
fn metrics_delta(
    (a, ra): (&monoio::DriverStats, &monoio::RuntimeStats),
    (b, rb): (&monoio::DriverStats, &monoio::RuntimeStats),
    secs: f64,
) -> (String, String) {
    let accept = monoio::uring::opcode::Accept::CODE as usize;
    let d = |x: u64, y: u64| y.saturating_sub(x);
    let cpt: Vec<String> = a
        .cqes_per_tick
        .iter()
        .zip(&b.cqes_per_tick)
        .map(|(x, y)| d(*x, *y).to_string())
        .collect();
    let f = [
        ("enter_submit", d(a.enter_submit, b.enter_submit)),
        ("enter_wait", d(a.enter_wait, b.enter_wait)),
        ("enter_skipped", d(a.enter_skipped, b.enter_skipped)),
        ("sqes_submitted", d(a.sqes_submitted, b.sqes_submitted)),
        ("cqes_reaped", d(a.cqes_reaped, b.cqes_reaped)),
        (
            "ops_accept",
            d(a.ops_by_opcode[accept], b.ops_by_opcode[accept]),
        ),
    ];
    let mut line = String::from("driver delta:");
    let mut json = String::from("{");
    for (k, v) in f {
        line += &format!(" {k}={v} ({:.0}/s)", v as f64 / secs);
        json += &format!("\"{k}\":{v},");
    }
    line += &format!(" cqes_per_tick=[{}]", cpt.join(","));
    json += &format!("\"cqes_per_tick\":[{}]", cpt.join(","));
    let r = [
        ("rounds", d(ra.rounds, rb.rounds)),
        ("submits", d(ra.submits, rb.submits)),
        ("parks", d(ra.parks, rb.parks)),
        ("park_ms", d(ra.park_ns, rb.park_ns) / 1_000_000),
        ("busy_ms", d(ra.busy_ns, rb.busy_ns) / 1_000_000),
        ("tasks_polled", d(ra.tasks_polled, rb.tasks_polled)),
        (
            "budget_exhausted_rounds",
            d(ra.budget_exhausted_rounds, rb.budget_exhausted_rounds),
        ),
        ("coop_yields", d(ra.coop_yields, rb.coop_yields)),
        ("lifo_polls", d(ra.lifo_polls, rb.lifo_polls)),
        ("prio_polls", d(ra.prio_polls, rb.prio_polls)),
    ];
    line += "\nruntime delta:";
    json += ",\"runtime\":{";
    for (k, v) in r {
        line += &format!(" {k}={v} ({:.0}/s)", v as f64 / secs);
        json += &format!("\"{k}\":{v},");
    }
    let ppr: Vec<String> = ra
        .polls_per_round
        .iter()
        .zip(&rb.polls_per_round)
        .map(|(x, y)| d(*x, *y).to_string())
        .collect();
    line += &format!(
        " polls_per_round=[{}] queue_len={} queue_high_water={}",
        ppr.join(","),
        rb.ready_queue_len,
        rb.ready_queue_high_water
    );
    json += &format!(
        "\"polls_per_round\":[{}],\"queue_len\":{},\"queue_high_water\":{}}}",
        ppr.join(","),
        rb.ready_queue_len,
        rb.ready_queue_high_water
    );
    json += "}";
    (line, json)
}

fn server_thread(
    a: Args,
    up_addr: Option<SocketAddr>,
    addr_tx: mpsc::Sender<io::Result<(SocketAddr, SocketAddr)>>,
    window: SharedWindow,
    res_tx: mpsc::Sender<ServerResult>,
) {
    if let Err(e) = monoio::utils::bind_to_cpu_set([a.cpu]) {
        eprintln!("server: bind to cpu {} failed: {e:?}", a.cpu);
    }
    let rt = apply_tune(RuntimeBuilder::<IoUringDriver>::new(), &a)
        .enable_timer()
        .build();
    let mut rt = match rt {
        Ok(rt) => rt,
        Err(e) => {
            let _ = addr_tx.send(Err(e));
            return;
        }
    };
    rt.block_on(async move {
        let opts = ListenerOpts::new().backlog(65535);
        let bind = || TcpListener::bind_with_config("127.0.0.1:0", &opts);
        let (lb, lr) = match (bind(), bind()) {
            (Ok(b), Ok(r)) => (b, r),
            (Err(e), _) | (_, Err(e)) => {
                let _ = addr_tx.send(Err(e));
                return;
            }
        };
        let addrs = (lb.local_addr().unwrap(), lr.local_addr().unwrap());
        let st = Rc::new(ServerState {
            window: Cell::new(None),
            accept_buckets: RefCell::new(Vec::new()),
            accepts_window: Cell::new(0),
            accept_to_first_read: RefCell::new(Hist::new()),
            bulk_chunk: Bytes::from(vec![b'b'; a.chunk.min(a.bulk_size)]),
            rr_resp: Bytes::from(vec![b'r'; a.rr_size]),
            bulk_size: a.bulk_size,
            pool: a.hop.then(|| Pool::new(a.pool)),
            hop_nops: a.hop_nops,
            up: up_addr.map(|addr| UpPool::new(addr, a.pool)),
            steps: RefCell::new(vec![Hist::new(); STEPS.len()]),
        });
        if let Some(pool) = &st.pool {
            for w in &pool.workers {
                monoio::spawn(worker_task(w.clone(), st.hop_nops));
            }
        }
        monoio::spawn(accept_loop(st.clone(), lb, true, a.accept_multi));
        monoio::spawn(accept_loop(st.clone(), lr, false, a.accept_multi));
        let _ = addr_tx.send(Ok(addrs));

        // Control: snapshots must be taken on this thread (the stats are thread-local).
        let w = wait_window(&window).await;
        st.window.set(Some(w));
        sleep_until(w.start).await;
        #[cfg(feature = "metrics")]
        let s0 = (monoio::driver_stats(), monoio::runtime_stats());
        sleep_until(w.end).await;
        let control_late_ms = Instant::now()
            .saturating_duration_since(w.end)
            .as_secs_f64()
            * 1e3;
        #[cfg(feature = "metrics")]
        let metrics = metrics_delta(
            (&s0.0, &s0.1),
            (&monoio::driver_stats(), &monoio::runtime_stats()),
            a.duration,
        );
        let _ = res_tx.send(ServerResult {
            accepts_window: st.accepts_window.get(),
            accepts_peak_1s: st
                .accept_buckets
                .borrow()
                .iter()
                .copied()
                .max()
                .unwrap_or(0),
            accept_to_first_read: st.accept_to_first_read.borrow().clone(),
            control_late_ms,
            steps: st.steps.borrow().clone(),
            #[cfg(feature = "metrics")]
            metrics,
        });
        // Keep serving until the process exits.
        std::future::pending::<()>().await;
    });
}

// ===== clients =====

#[derive(Default)]
struct ClientStats {
    rr_hist: RefCell<Option<Hist>>,
    /// `--upstream`: latency of the proxy connections (`rr_hist` then holds the json ones).
    px_hist: RefCell<Option<Hist>>,
    rr_sent: Cell<u64>,
    rr_ok: Cell<u64>,
    rr_timeouts: Cell<u64>,
    rr_errors: Cell<u64>,
    bulk_bytes: Cell<u64>,
    bulk_resps: Cell<u64>,
    bulk_errors: Cell<u64>,
    ramp_hist: RefCell<Option<Hist>>,
    ramp_ok: Cell<u64>,
    ramp_timeouts: Cell<u64>,
    ramp_errors: Cell<u64>,
    /// Per rr connection: the next scheduled slot not issued yet (for the backlog count).
    cursors: RefCell<Vec<Rc<Cell<Instant>>>>,
}

fn inc(c: &Cell<u64>, n: u64) {
    c.set(c.get() + n);
}

struct ClientResult {
    rr_hist: Hist,
    px_hist: Hist,
    rr_sent: u64,
    rr_ok: u64,
    rr_timeouts: u64,
    rr_errors: u64,
    bulk_bytes: u64,
    bulk_resps: u64,
    bulk_errors: u64,
    ramp_hist: Hist,
    ramp_ok: u64,
    ramp_timeouts: u64,
    ramp_errors: u64,
    /// Requests scheduled inside the window but not issued by collection time (late senders).
    rr_backlog: u64,
}

impl ClientStats {
    fn take(&self, w: &Window, interval: Duration) -> ClientResult {
        let slots = |from: Instant, to: Instant| -> u64 {
            // Slots `cursor + k * interval` before `to`, counted from `from`.
            if to <= from {
                0
            } else {
                ((to - from).as_secs_f64() / interval.as_secs_f64()).ceil() as u64
            }
        };
        let rr_backlog = self
            .cursors
            .borrow()
            .iter()
            .map(|c| {
                let c = c.get();
                let skipped = if c < w.start { slots(c, w.start) } else { 0 };
                slots(c, w.end).saturating_sub(skipped)
            })
            .sum();
        ClientResult {
            rr_backlog,
            rr_hist: self.rr_hist.borrow_mut().take().unwrap(),
            px_hist: self.px_hist.borrow_mut().take().unwrap(),
            rr_sent: self.rr_sent.get(),
            rr_ok: self.rr_ok.get(),
            rr_timeouts: self.rr_timeouts.get(),
            rr_errors: self.rr_errors.get(),
            bulk_bytes: self.bulk_bytes.get(),
            bulk_resps: self.bulk_resps.get(),
            bulk_errors: self.bulk_errors.get(),
            ramp_hist: self.ramp_hist.borrow_mut().take().unwrap(),
            ramp_ok: self.ramp_ok.get(),
            ramp_timeouts: self.ramp_timeouts.get(),
            ramp_errors: self.ramp_errors.get(),
        }
    }
}

struct Ctx {
    a: Args,
    rr_addr: SocketAddr,
    bulk_addr: SocketAddr,
    w: Window,
    interval: Duration,
    st: ClientStats,
}

async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    let s = TcpStream::connect(addr).await?;
    let _ = s.set_nodelay(true);
    Ok(s)
}

async fn exchange(
    s: &mut TcpStream,
    req: Vec<u8>,
    resp: Vec<u8>,
) -> (io::Result<()>, Vec<u8>, Vec<u8>) {
    let (r, req) = s.write_all(req).await;
    if let Err(e) = r {
        return (Err(e), req, resp);
    }
    let (r, resp) = s.read_exact(resp).await;
    (r.map(|_| ()), req, resp)
}

/// One request at `sched`; `Ok(true)` = response in time, `Ok(false)` = timed out.
async fn one_request(
    cx: &Ctx,
    stream: &mut Option<TcpStream>,
    bufs: &mut (Vec<u8>, Vec<u8>),
    sched: Instant,
) -> io::Result<bool> {
    let deadline = sched + cx.a.timeout;
    let left = deadline.saturating_duration_since(Instant::now());
    let fut = async {
        if stream.is_none() {
            *stream = Some(connect(cx.rr_addr).await?);
        }
        let s = stream.as_mut().unwrap();
        let (req, resp) = std::mem::take(bufs);
        let (r, req, resp) = exchange(s, req, resp).await;
        *bufs = (req, resp);
        r
    };
    match monoio::time::timeout(left, fut).await {
        Ok(Ok(())) => Ok(true),
        Ok(Err(e)) => {
            *stream = None;
            Err(e)
        }
        Err(_) => {
            // The exchange was dropped midway: the connection state is unknown.
            *stream = None;
            let proxy = bufs.0.first() == Some(&b'p');
            *bufs = rr_bufs(&cx.a, proxy);
            Ok(false)
        }
    }
}

fn rr_bufs(a: &Args, proxy: bool) -> (Vec<u8>, Vec<u8>) {
    let mut req = vec![b'q'; 64];
    if proxy {
        req[0] = b'p';
    }
    (req, Vec::with_capacity(a.rr_size))
}

/// Open-loop rr connection: requests at `sched`, `sched + interval`, ...; latency from the
/// scheduled time. A scheduled request already older than the timeout counts as a timeout.
async fn rr_loop(
    cx: Rc<Ctx>,
    mut stream: Option<TcpStream>,
    mut sched: Instant,
    cursor: Rc<Cell<Instant>>,
    proxy: bool,
) {
    let mut bufs = rr_bufs(&cx.a, proxy);
    let st = &cx.st;
    loop {
        cursor.set(sched);
        sleep_until(sched).await;
        let now = Instant::now();
        while now.saturating_duration_since(sched) >= cx.a.timeout {
            if cx.w.contains(sched) {
                inc(&st.rr_sent, 1);
                inc(&st.rr_timeouts, 1);
            }
            sched += cx.interval;
        }
        if sched > now {
            continue;
        }
        let in_win = cx.w.contains(sched);
        if in_win {
            inc(&st.rr_sent, 1);
        }
        cursor.set(sched + cx.interval);
        match one_request(&cx, &mut stream, &mut bufs, sched).await {
            Ok(true) if in_win => {
                inc(&st.rr_ok, 1);
                let hist = if proxy { &st.px_hist } else { &st.rr_hist };
                if let Some(h) = hist.borrow_mut().as_mut() {
                    h.record(sched.elapsed());
                }
            }
            Ok(true) => {}
            Ok(false) if in_win => inc(&st.rr_timeouts, 1),
            Ok(false) => {}
            Err(_) => {
                if in_win {
                    inc(&st.rr_errors, 1);
                }
                monoio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        sched += cx.interval;
    }
}

/// A ramp connection: connect at `at`, first request right away, measure connect -> first
/// response from `at`, then continue as an ordinary rr connection.
async fn ramp_conn(cx: Rc<Ctx>, at: Instant) {
    let rr = cx.interval < Duration::MAX / 4;
    let cursor = Rc::new(Cell::new(if rr { at + cx.interval } else { cx.w.end }));
    cx.st.cursors.borrow_mut().push(cursor.clone());
    sleep_until(at).await;
    let mut stream = None;
    let mut bufs = rr_bufs(&cx.a, false);
    match one_request(&cx, &mut stream, &mut bufs, at).await {
        Ok(true) => {
            inc(&cx.st.ramp_ok, 1);
            if let Some(h) = cx.st.ramp_hist.borrow_mut().as_mut() {
                h.record(at.elapsed());
            }
        }
        Ok(false) => inc(&cx.st.ramp_timeouts, 1),
        Err(_) => inc(&cx.st.ramp_errors, 1),
    }
    if rr {
        rr_loop(cx.clone(), stream, at + cx.interval, cursor, false).await;
    }
}

/// Closed-loop bulk connection: 1-byte request, read `bulk_size` bytes, repeat.
async fn bulk_loop(cx: Rc<Ctx>, mut stream: Option<TcpStream>) {
    let st = &cx.st;
    let mut buf = Vec::with_capacity(256 * 1024);
    'conn: loop {
        let mut s = match stream.take() {
            Some(s) => s,
            None => match connect(cx.bulk_addr).await {
                Ok(s) => s,
                Err(_) => {
                    inc(&st.bulk_errors, 1);
                    monoio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            },
        };
        loop {
            let (r, _) = s.write_all(vec![1u8]).await;
            if r.is_err() {
                inc(&st.bulk_errors, 1);
                continue 'conn;
            }
            let mut left = cx.a.bulk_size;
            while left > 0 {
                buf.clear();
                let (r, b) = s.read(buf).await;
                buf = b;
                match r {
                    Ok(n) if n > 0 => {
                        left = left.saturating_sub(n);
                        if cx.w.contains(Instant::now()) {
                            inc(&st.bulk_bytes, n as u64);
                        }
                    }
                    _ => {
                        inc(&st.bulk_errors, 1);
                        continue 'conn;
                    }
                }
            }
            if cx.w.contains(Instant::now()) {
                inc(&st.bulk_resps, 1);
            }
        }
    }
}

struct Plan {
    cpu: usize,
    bulk: usize,
    /// Global indices of the base rr connections (spread the schedule phases).
    rr: Vec<usize>,
    /// Ramp connection indices (0..K): connect at `start + i * ramp_secs / K`.
    ramp: Vec<usize>,
}

#[allow(clippy::too_many_arguments)]
fn client_thread(
    a: Args,
    plan: Plan,
    addrs: (SocketAddr, SocketAddr),
    window: SharedWindow,
    ready_tx: mpsc::Sender<io::Result<()>>,
    res_tx: mpsc::Sender<ClientResult>,
) {
    let _ = monoio::utils::bind_to_cpu_set([plan.cpu]);
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(4096)
        .enable_timer()
        .build()
        .expect("client runtime");
    rt.block_on(async move {
        // Connect the base connections before the load starts.
        let mut bulk = Vec::with_capacity(plan.bulk);
        let mut rr = Vec::with_capacity(plan.rr.len());
        let res: io::Result<()> = async {
            for _ in 0..plan.bulk {
                bulk.push(connect(addrs.0).await?);
            }
            for _ in &plan.rr {
                rr.push(connect(addrs.1).await?);
            }
            Ok(())
        }
        .await;
        let failed = res.is_err();
        let _ = ready_tx.send(res);
        if failed {
            return;
        }
        let w = wait_window(&window).await;
        let total_rr = a.rr + a.ramp;
        let interval = if a.rate > 0.0 && total_rr > 0 {
            Duration::from_secs_f64(total_rr as f64 / a.rate)
        } else {
            Duration::MAX / 2
        };
        let cx = Rc::new(Ctx {
            a: a.clone(),
            bulk_addr: addrs.0,
            rr_addr: addrs.1,
            w,
            interval,
            st: ClientStats {
                rr_hist: RefCell::new(Some(Hist::new())),
                px_hist: RefCell::new(Some(Hist::new())),
                ramp_hist: RefCell::new(Some(Hist::new())),
                ..Default::default()
            },
        });
        for s in bulk {
            monoio::spawn(bulk_loop(cx.clone(), Some(s)));
        }
        if a.rate > 0.0 {
            for (s, &idx) in rr.into_iter().zip(&plan.rr) {
                let phase = interval.mul_f64(idx as f64 / a.rr.max(1) as f64);
                let at = w.load + phase;
                let cursor = Rc::new(Cell::new(at));
                cx.st.cursors.borrow_mut().push(cursor.clone());
                // `--upstream`: proxy connections spread evenly over the indices.
                let proxy = a.upstream && is_proxy(idx, a.proxy_frac);
                monoio::spawn(rr_loop(cx.clone(), Some(s), at, cursor, proxy));
            }
        }
        for &i in &plan.ramp {
            let at = w.start + Duration::from_secs_f64(a.ramp_secs * i as f64 / a.ramp as f64);
            monoio::spawn(ramp_conn(cx.clone(), at));
        }
        // Collect after a short grace so requests scheduled at the end can finish.
        sleep_until(w.end + Duration::from_millis(500)).await;
        let _ = res_tx.send(cx.st.take(&cx.w, cx.interval));
        std::future::pending::<()>().await;
    });
}

/// Whether rr connection `idx` is a proxy connection, for a share `frac` spread evenly.
fn is_proxy(idx: usize, frac: f64) -> bool {
    ((idx + 1) as f64 * frac).floor() > (idx as f64 * frac).floor()
}

// ===== main =====

fn main() {
    let a = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    raise_nofile();
    let need = 2 * (a.bulk + a.rr + a.ramp) as u64 + 64;
    if nofile() < need {
        eprintln!(
            "warning: RLIMIT_NOFILE {} < {need} needed (2 fds per connection)",
            nofile()
        );
    }
    if !monoio::utils::detect_uring() {
        eprintln!("io_uring is not available");
        std::process::exit(1);
    }

    let up_addr = a.upstream_cpu.filter(|_| a.upstream).map(|cpu| {
        let (tx, rx) = mpsc::channel();
        let size = a.rr_size;
        std::thread::Builder::new()
            .name("fairness-upstream".into())
            .spawn(move || upstream_thread(cpu, size, tx))
            .unwrap();
        match rx.recv() {
            Ok(Ok(addr)) => addr,
            _ => {
                eprintln!("upstream failed to start");
                std::process::exit(1);
            }
        }
    });
    let window: SharedWindow = Arc::new(OnceLock::new());
    let (addr_tx, addr_rx) = mpsc::channel();
    let (srv_tx, srv_rx) = mpsc::channel();
    {
        let (a, window) = (a.clone(), window.clone());
        std::thread::Builder::new()
            .name("fairness-server".into())
            .spawn(move || server_thread(a, up_addr, addr_tx, window, srv_tx))
            .unwrap();
    }
    let addrs = match addr_rx.recv() {
        Ok(Ok(addrs)) => addrs,
        Ok(Err(e)) => {
            eprintln!("server: {e}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("server thread died");
            std::process::exit(1);
        }
    };

    // Distribute connections round-robin over the client threads.
    let n = a.client_cpus.len();
    let mut plans: Vec<Plan> = a
        .client_cpus
        .iter()
        .map(|&cpu| Plan {
            cpu,
            bulk: 0,
            rr: Vec::new(),
            ramp: Vec::new(),
        })
        .collect();
    for i in 0..a.bulk {
        plans[i % n].bulk += 1;
    }
    for i in 0..a.rr {
        plans[i % n].rr.push(i);
    }
    for i in 0..a.ramp {
        plans[i % n].ramp.push(i);
    }
    let (ready_tx, ready_rx) = mpsc::channel();
    let (res_tx, res_rx) = mpsc::channel();
    for (i, plan) in plans.into_iter().enumerate() {
        let (a, window, ready_tx, res_tx) =
            (a.clone(), window.clone(), ready_tx.clone(), res_tx.clone());
        std::thread::Builder::new()
            .name(format!("fairness-client-{i}"))
            .spawn(move || client_thread(a, plan, addrs, window, ready_tx, res_tx))
            .unwrap();
    }
    drop((ready_tx, res_tx));
    for _ in 0..n {
        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("client connect failed: {e}");
                std::process::exit(1);
            }
            Err(_) => {
                eprintln!("client thread died");
                std::process::exit(1);
            }
        }
    }
    let load = Instant::now() + Duration::from_millis(50);
    let start = load + Duration::from_secs_f64(a.warmup);
    let end = start + Duration::from_secs_f64(a.duration);
    let _ = window.set(Window { load, start, end });

    let grace = Duration::from_secs(30) + a.timeout;
    let deadline = end + grace;
    let mut clients = Vec::new();
    for _ in 0..n {
        match res_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(r) => clients.push(r),
            Err(_) => break,
        }
    }
    let server = srv_rx
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .ok();
    report(&a, &clients, n, server.as_ref());
    std::process::exit(0);
}

fn report(a: &Args, clients: &[ClientResult], nthreads: usize, server: Option<&ServerResult>) {
    let mut rr = Hist::new();
    let mut px = Hist::new();
    let mut ramp = Hist::new();
    let (mut sent, mut ok, mut to, mut err) = (0, 0, 0, 0);
    let (mut bytes, mut resps, mut berr) = (0, 0, 0);
    let (mut ramp_ok, mut ramp_to, mut ramp_err, mut backlog) = (0, 0, 0, 0);
    for c in clients {
        backlog += c.rr_backlog;
        rr.merge(&c.rr_hist);
        px.merge(&c.px_hist);
        ramp.merge(&c.ramp_hist);
        sent += c.rr_sent;
        ok += c.rr_ok;
        to += c.rr_timeouts;
        err += c.rr_errors;
        bytes += c.bulk_bytes;
        resps += c.bulk_resps;
        berr += c.bulk_errors;
        ramp_ok += c.ramp_ok;
        ramp_to += c.ramp_timeouts;
        ramp_err += c.ramp_errors;
    }
    let secs = a.duration;
    let scheduled = sent + backlog;
    // Ramp connections still waiting for their first response at collection time.
    let ramp_pending = (a.ramp as u64).saturating_sub(ramp_ok + ramp_to + ramp_err);
    let unfinished = sent.saturating_sub(ok + to + err);
    let pct = if scheduled > 0 {
        100.0 * ok as f64 / scheduled as f64
    } else {
        0.0
    };
    let mbs = bytes as f64 / secs / 1e6;
    let rps = resps as f64 / secs;
    let (acc, acc_peak, a2r, late) = match server {
        Some(s) => (
            s.accepts_window,
            s.accepts_peak_1s,
            s.accept_to_first_read.clone(),
            s.control_late_ms,
        ),
        None => (0, 0, Hist::new(), -1.0),
    };
    let missing = nthreads - clients.len();
    println!(
        "fairness [{tune} accept={acc_mode}{hop}] bulk={bulk} rr={rrn} rate={rate} ramp={rampn} \
         | rr p50={p50:.2} p90={p90:.2} p99={p99:.2} p99.9={p999:.2} max={max:.2} ms \
         scheduled={scheduled} sent={sent} ok={ok} timeouts={to} errors={err} \
         unfinished={unfinished} achieved={pct:.1}% \
         | bulk {mbs:.0} MB/s {rps:.0} resp/s errors={berr} \
         | ramp first-resp p50={rp50:.2} p99={rp99:.2} max={rmax:.2} ms ok={ramp_ok} timeouts={ramp_to} \
         errors={ramp_err} pending={ramp_pending} \
         | accepts {aps:.1}/s peak {acc_peak}/s accept->first-read p99={a2r99:.2} ms \
         | control late {late:.1} ms{miss}",
        tune = tune_label(a),
        acc_mode = if a.accept_multi { "multi" } else { "single" },
        hop = if a.hop {
            format!(" hop pool={} nops={}", a.pool, a.hop_nops)
        } else {
            String::new()
        },
        bulk = a.bulk,
        rrn = a.rr,
        rate = a.rate,
        rampn = a.ramp,
        p50 = rr.q_ms(0.5),
        p90 = rr.q_ms(0.9),
        p99 = rr.q_ms(0.99),
        p999 = rr.q_ms(0.999),
        max = rr.max_ms(),
        rp50 = ramp.q_ms(0.5),
        rp99 = ramp.q_ms(0.99),
        rmax = ramp.max_ms(),
        aps = acc as f64 / secs,
        a2r99 = a2r.q_ms(0.99),
        miss = if missing > 0 {
            format!(" | {missing} client threads did not report")
        } else {
            String::new()
        },
    );
    let mut steps_json = String::new();
    if a.upstream {
        let mut line = format!(
            "upstream: json n={} p50={:.2} p99={:.2} | proxy n={} p50={:.2} p99={:.2} p99.9={:.2} max={:.2} ms | \
             server steps p50/p99 ms:",
            rr.total,
            rr.q_ms(0.5),
            rr.q_ms(0.99),
            px.total,
            px.q_ms(0.5),
            px.q_ms(0.99),
            px.q_ms(0.999),
            px.max_ms()
        );
        if let Some(s) = server {
            for (name, h) in STEPS.iter().zip(&s.steps) {
                line += &format!(" {name}={:.2}/{:.2}", h.q_ms(0.5), h.q_ms(0.99));
                steps_json += &format!(
                    ",\"step_{name}_p50_ms\":{:.3},\"step_{name}_p99_ms\":{:.3}",
                    h.q_ms(0.5),
                    h.q_ms(0.99)
                );
            }
        }
        println!("{line}");
    }
    #[cfg(feature = "metrics")]
    if let Some(s) = server {
        println!("{}", s.metrics.0);
    }
    if a.json {
        #[allow(unused_mut)]
        let mut j = format!(
            "{{\"tune\":\"{tune}\",\"accept\":\"{acc_mode}\",\"hop\":{hop},\"pool\":{pool},\
             \"hop_nops\":{nops},\"bulk_conns\":{bulk},\"rr_conns\":{rrn},\"rate\":{rate},\
             \"ramp_conns\":{rampn},\"duration_s\":{secs},\
             \"rr_p50_ms\":{p50:.3},\"rr_p90_ms\":{p90:.3},\"rr_p99_ms\":{p99:.3},\
             \"rr_p999_ms\":{p999:.3},\"rr_max_ms\":{max:.3},\"rr_scheduled\":{scheduled},\"rr_sent\":{sent},\"rr_ok\":{ok},\
             \"rr_unfinished\":{unfinished},\
             \"rr_timeouts\":{to},\"rr_errors\":{err},\"rr_achieved_pct\":{pct:.2},\
             \"bulk_mb_s\":{mbs:.1},\"bulk_resp_s\":{rps:.1},\"bulk_errors\":{berr},\
             \"ramp_p50_ms\":{rp50:.3},\"ramp_p99_ms\":{rp99:.3},\"ramp_max_ms\":{rmax:.3},\
             \"ramp_ok\":{ramp_ok},\"ramp_timeouts\":{ramp_to},\
             \"ramp_errors\":{ramp_err},\"ramp_pending\":{ramp_pending},\"accepts_per_s\":{aps:.2},\
             \"accepts_peak_1s\":{acc_peak},\"accept_to_first_read_p50_ms\":{a2r50:.3},\
             \"accept_to_first_read_p99_ms\":{a2r99:.3},\"accept_to_first_read_max_ms\":{a2rmax:.3},\
             \"control_late_ms\":{late:.2},\"clients_missing\":{missing},\
             \"upstream\":{upstream},\"proxy_frac\":{pfrac},\"json_p50_ms\":{p50:.3},\"json_p99_ms\":{p99:.3},\
             \"proxy_n\":{pxn},\"proxy_p50_ms\":{px50:.3},\"proxy_p99_ms\":{px99:.3},\"proxy_p999_ms\":{px999:.3}{steps_json}",
            upstream = a.upstream,
            pfrac = a.proxy_frac,
            pxn = px.total,
            px50 = px.q_ms(0.5),
            px99 = px.q_ms(0.99),
            px999 = px.q_ms(0.999),
            tune = tune_label(a),
            acc_mode = if a.accept_multi { "multi" } else { "single" },
            hop = a.hop,
            pool = a.pool,
            nops = a.hop_nops,
            bulk = a.bulk,
            rrn = a.rr,
            rate = a.rate,
            rampn = a.ramp,
            p50 = rr.q_ms(0.5),
            p90 = rr.q_ms(0.9),
            p99 = rr.q_ms(0.99),
            p999 = rr.q_ms(0.999),
            max = rr.max_ms(),
            rp50 = ramp.q_ms(0.5),
            rp99 = ramp.q_ms(0.99),
            rmax = ramp.max_ms(),
            aps = acc as f64 / secs,
            a2r50 = a2r.q_ms(0.5),
            a2r99 = a2r.q_ms(0.99),
            a2rmax = a2r.max_ms(),
        );
        #[cfg(feature = "metrics")]
        if let Some(s) = server {
            j += &format!(",\"driver\":{}", s.metrics.1);
        }
        j += "}";
        println!("{j}");
    }
}
