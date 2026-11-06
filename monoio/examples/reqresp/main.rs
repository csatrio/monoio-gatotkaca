//! `reqresp`: a closed loop of request → tiny reply on few or many connections, optionally
//! next to bulk writers, on one server core (round 3, X1).
//!
//! One process, loopback. The server runtime is pinned to `--cpu`; one client runtime per CPU
//! of `--client-cpus`. Each of the `--conns` request/response connections sends one frame
//! (4-byte little-endian total length + payload, sizes cycled through `--sizes`) and waits
//! for the 70-byte reply before it sends the next. On each of the `--bulk` connections the
//! server writes `--chunk` buffers without pause and the client discards them.
//!
//! How the server handles a connection is `--path` (one file each in `paths/`), the per-frame
//! deadline is `--deadline` (`deadline.rs`), the ring set-up is `--ring` (here and
//! `ring_ext.rs`), the accept loop is `accept.rs`.
//!
//! The window starts once all connections are accepted and `--warmup` seconds passed; every
//! counter is sampled at its start and its end. Output: a readable line, a `RESULT` line of
//! `key=value` pairs and, with the `metrics` feature, a `STATS` line (`stats.rs`).
//! Notes: `docs/performance-improvement/round3/r3-repro.md`.
//!
//! Run: `cargo run --release -p adaptif-gatotkaca-monoio --example reqresp -- --conns 8`.

mod accept;
mod bulk;
mod client;
mod deadline;
mod measure;
mod paths;
mod ring_ext;
#[cfg(feature = "metrics")]
mod stats;

use std::{
    fmt::{Display, Write},
    net::SocketAddr,
    rc::Rc,
    sync::{atomic::Ordering, mpsc, Arc},
    time::{Duration, Instant},
};

use monoio::{net::TcpListener, IoUringDriver, RuntimeBuilder};

use crate::{
    client::Shared,
    measure::{Hist, Perf},
    paths::{Ctx, Path},
};

const USAGE: &str = "\
reqresp: closed-loop request/response load (frame -> 70-byte reply), optional bulk writers,
one server core

  --cpu C              server CPU (default 2)
  --client-cpus a,b    client CPUs, one client thread each (default 3,4)
  --conns N            request/response connections (default 8)
  --sizes a,b,..       frame sizes, cycled per connection (default 4k,16k,64k; k, m suffixes)
  --bulk M             bulk connections the server writes to without pause (default 0)
  --chunk SZ           bulk write size (default 32k)
  --bulk-work SZ       bytes of user-space work on each bulk chunk before it is sent (default 0;
                       a byte-serial pass, about 2 us per KiB: models a server that encrypts
                       its stream chunks; may exceed --chunk)
  --think US           client pause between a reply and its next frame, microseconds (default 0)
  --path P             plain | pipe | mshot | eager | pair (default pipe)
  --deadline D         none | timeout | coarse | timeoutpin | deadline | nonepin (default none)
  --ring a,b,..        coop, defer, skipenter, ringfd, fixedfd, minwait<us>, drain<N>
                       (default: none)
  --entries N          server ring entries (default 4096)
  --cq N               server completion queue size (default: 2 x entries)
  --bufring N,SZ       path mshot: buffers in the ring and their size (default 4096,16k)
  --wset I,S           path mshot: working set (initial, step)
  --direct             direct accept
  --warmup S           seconds between the last accept and the window (default 2)
  --duration S         measured window in seconds (default 6)";

struct Args {
    cpu: usize,
    client_cpus: Vec<usize>,
    conns: usize,
    sizes: Vec<usize>,
    bulk: usize,
    chunk: usize,
    bulk_work: usize,
    think: u64,
    path: Path,
    deadline: deadline::Mode,
    ring: Vec<String>,
    entries: u32,
    cq: Option<u32>,
    bufring: (u16, usize),
    wset: Option<(u16, u16)>,
    direct: bool,
    warmup: f64,
    duration: f64,
}

/// A command line error.
fn usage(msg: &str) -> ! {
    eprintln!("reqresp: {msg}\n\n{USAGE}");
    std::process::exit(2)
}

/// A run time error.
fn die(msg: impl Display) -> ! {
    eprintln!("reqresp: {msg}");
    std::process::exit(1)
}

fn num<T: std::str::FromStr>(flag: &str, s: &str) -> T {
    s.parse()
        .unwrap_or_else(|_| usage(&format!("{flag}: bad value '{s}'")))
}

fn size(flag: &str, s: &str) -> usize {
    let s = s.to_ascii_lowercase();
    if let Some(k) = s.strip_suffix('k') {
        num::<usize>(flag, k) * 1024
    } else if let Some(m) = s.strip_suffix('m') {
        num::<usize>(flag, m) * 1024 * 1024
    } else {
        num(flag, &s)
    }
}

/// `A,B` with a parser per side.
fn pair<A, B>(flag: &str, s: &str, a: impl Fn(&str) -> A, b: impl Fn(&str) -> B) -> (A, B) {
    match s.split_once(',') {
        Some((x, y)) => (a(x), b(y)),
        None => usage(&format!("{flag}: expected two values 'A,B', got '{s}'")),
    }
}

fn args() -> Args {
    let mut a = Args {
        cpu: 2,
        client_cpus: vec![3, 4],
        conns: 8,
        sizes: vec![4 << 10, 16 << 10, 64 << 10],
        bulk: 0,
        chunk: 32 << 10,
        bulk_work: 0,
        think: 0,
        path: Path::Pipe,
        deadline: deadline::Mode::None,
        ring: Vec::new(),
        entries: 4096,
        cq: None,
        bufring: (4096, 16 << 10),
        wset: None,
        direct: false,
        warmup: 2.0,
        duration: 6.0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(f) = it.next() {
        let f = f.as_str();
        let mut v = || {
            it.next()
                .unwrap_or_else(|| usage(&format!("{f}: missing value")))
        };
        match f {
            "--cpu" => a.cpu = num(f, &v()),
            "--client-cpus" => a.client_cpus = v().split(',').map(|c| num(f, c)).collect(),
            "--conns" => a.conns = num(f, &v()),
            "--sizes" => a.sizes = v().split(',').map(|s| size(f, s)).collect(),
            "--bulk" => a.bulk = num(f, &v()),
            "--chunk" => a.chunk = size(f, &v()),
            "--bulk-work" => a.bulk_work = size(f, &v()),
            "--think" => a.think = num(f, &v()),
            "--path" => {
                let v = v();
                a.path = Path::parse(&v).unwrap_or_else(|| usage(&format!("--path: '{v}'")));
            }
            "--deadline" => {
                let v = v();
                a.deadline = deadline::Mode::parse(&v)
                    .unwrap_or_else(|| usage(&format!("--deadline: '{v}'")));
            }
            "--ring" => {
                a.ring = v()
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect()
            }
            "--entries" => a.entries = num(f, &v()),
            "--cq" => a.cq = Some(num(f, &v())),
            "--bufring" => a.bufring = pair(f, &v(), |n| num(f, n), |s| size(f, s)),
            "--wset" => a.wset = Some(pair(f, &v(), |n| num(f, n), |n| num(f, n))),
            "--direct" => a.direct = true,
            "--warmup" => a.warmup = num(f, &v()),
            "--duration" => a.duration = num(f, &v()),
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            _ => usage(&format!("unknown argument '{f}'")),
        }
    }
    if a.client_cpus.is_empty() {
        usage("--client-cpus: empty");
    }
    if a.sizes.is_empty()
        || a.sizes
            .iter()
            .any(|&s| s < paths::HEADER || s > u32::MAX as usize)
    {
        usage("--sizes: each frame needs at least the 4-byte header");
    }
    if a.conns + a.bulk == 0 {
        usage("no connections: --conns and --bulk are both 0");
    }
    let seconds = |s: f64| s.is_finite() && s >= 0.0;
    if a.chunk == 0 || !seconds(a.duration) || a.duration == 0.0 || !seconds(a.warmup) {
        usage("--chunk and --duration must be positive, --warmup not negative");
    }
    a
}

/// The server's builder with `--entries`, `--cq` and the `--ring` items.
fn server_builder(a: &Args, accept_opts: &accept::Opts) -> RuntimeBuilder<IoUringDriver> {
    let mut rb = RuntimeBuilder::<IoUringDriver>::new().with_entries(a.entries);
    if let Some(cq) = a.cq {
        rb = rb.cq_size(cq);
    }
    for item in &a.ring {
        rb = match item.as_str() {
            "coop" => rb.coop_taskrun().taskrun_flag(),
            "defer" => rb.defer_taskrun(),
            "skipenter" => rb.skip_empty_enter(),
            "ringfd" => rb.register_ring_fd(),
            // The table and the per-stream registration are `accept.rs`.
            "fixedfd" => rb,
            other => match other.strip_prefix("minwait").map(str::parse::<u64>) {
                Some(Ok(us)) => rb.min_wait(Duration::from_micros(us), 8),
                _ => ring_ext::apply(rb, other)
                    .unwrap_or_else(|_| usage(&format!("--ring: unknown item '{other}'"))),
            },
        };
    }
    accept::configure(rb, accept_opts)
}

/// The server's counters at one instant.
struct Sample {
    at: Instant,
    cpu: Duration,
    frames: u64,
    frame_bytes: u64,
    bulk_bytes: u64,
    enobufs: u64,
    perf: [Option<u64>; 3],
}

impl Sample {
    fn take(ctx: &Ctx, perf: &Perf) -> Self {
        Self {
            at: Instant::now(),
            cpu: measure::thread_cpu(),
            frames: ctx.count.frames.get(),
            frame_bytes: ctx.count.frame_bytes.get(),
            bulk_bytes: ctx.count.bulk_bytes.get(),
            enobufs: ctx.count.enobufs.get(),
            perf: perf.read(),
        }
    }
}

/// What the server thread measured over the window.
struct ServerReport {
    wall: f64,
    cpu: f64,
    frames: u64,
    frame_bytes: u64,
    bulk_bytes: u64,
    /// Multishot receives ended by `ENOBUFS` inside the window (path `mshot`).
    enobufs: u64,
    /// User cycles, user instructions, all cycles.
    perf: [Option<u64>; 3],
    /// Seconds until every connection was accepted.
    ramp: f64,
    /// Request/response connections that ended with an error, since the start.
    errors: u64,
    /// The pairs of the `STATS` line (`metrics`).
    stats: Option<String>,
}

fn server(
    a: Args,
    shared: Arc<Shared>,
    addr_tx: mpsc::Sender<(SocketAddr, SocketAddr)>,
    report: mpsc::Sender<ServerReport>,
) {
    if monoio::utils::bind_to_cpu_set([a.cpu]).is_err() {
        eprintln!("reqresp: cannot pin the server thread to CPU {}", a.cpu);
    }
    let accept_opts = accept::Opts {
        fixedfd: a.ring.iter().any(|r| r == "fixedfd"),
        direct: a.direct,
    };
    let mut rt = server_builder(&a, &accept_opts)
        .enable_timer()
        .build()
        .unwrap_or_else(|e| die(format!("server runtime: {e}")));
    let perf = Perf::open();
    rt.block_on(async move {
        let ctx = Rc::new(Ctx::new(paths::Config {
            path: a.path,
            deadline: a.deadline,
            max_frame: a.sizes.iter().copied().max().expect("not empty"),
            bufring: a.bufring,
            wset: a.wset,
        }));
        paths::setup(&ctx).unwrap_or_else(|e| die(format!("path {}: {e}", a.path.name())));

        let bind = || TcpListener::bind("127.0.0.1:0").unwrap_or_else(|e| die(e));
        let (listener, bulk_listener) = (bind(), bind());
        let addr = |l: &TcpListener| l.local_addr().unwrap_or_else(|e| die(e));
        let _ = addr_tx.send((addr(&listener), addr(&bulk_listener)));

        let accepted = |ctx: &Ctx| ctx.count.accepted.set(ctx.count.accepted.get() + 1);
        let c = ctx.clone();
        monoio::spawn(accept::run(listener, accept_opts, move |stream| {
            accepted(&c);
            monoio::spawn(paths::conn(stream, c.clone()));
        }));
        let (c, chunk, work) = (ctx.clone(), a.chunk, a.bulk_work);
        monoio::spawn(accept::run(bulk_listener, accept_opts, move |stream| {
            accepted(&c);
            monoio::spawn(bulk::serve(stream, chunk, work, c.clone()));
        }));

        // Ramp: wait until every client connection is accepted, then warm up.
        let begin = Instant::now();
        while ctx.count.accepted.get() < (a.conns + a.bulk) as u64 {
            if begin.elapsed() > Duration::from_secs(60) {
                die(format!(
                    "only {} of {} connections arrived in 60 s",
                    ctx.count.accepted.get(),
                    a.conns + a.bulk
                ));
            }
            monoio::time::sleep(Duration::from_millis(1)).await;
        }
        let ramp = begin.elapsed().as_secs_f64();
        monoio::time::sleep(Duration::from_secs_f64(a.warmup)).await;

        // The window. Nothing of this task runs inside it.
        #[cfg(feature = "metrics")]
        let window = stats::start();
        let s0 = Sample::take(&ctx, &perf);
        shared.window_start.store(shared.now(), Ordering::Relaxed);
        shared.phase.store(client::WINDOW, Ordering::Relaxed);
        monoio::time::sleep(Duration::from_secs_f64(a.duration)).await;
        let s1 = Sample::take(&ctx, &perf);
        shared.window_end.store(shared.now(), Ordering::Relaxed);
        shared.phase.store(client::DONE, Ordering::Relaxed);

        let (frames, wall) = (s1.frames - s0.frames, (s1.at - s0.at).as_secs_f64());
        #[cfg(feature = "metrics")]
        let stats = Some(stats::finish(window, frames, wall));
        #[cfg(not(feature = "metrics"))]
        let stats = None;
        let mut perf = [None; 3];
        for (d, (p0, p1)) in perf.iter_mut().zip(s0.perf.iter().zip(s1.perf)) {
            *d = p0.zip(p1).map(|(p0, p1)| p1 - p0);
        }
        let _ = report.send(ServerReport {
            wall,
            cpu: (s1.cpu - s0.cpu).as_secs_f64(),
            frames,
            frame_bytes: s1.frame_bytes - s0.frame_bytes,
            bulk_bytes: s1.bulk_bytes - s0.bulk_bytes,
            enobufs: s1.enobufs - s0.enobufs,
            perf,
            ramp,
            errors: ctx.count.errors.get(),
            stats,
        });
        // Tearing down thousands of connections is not part of the measurement: the main
        // thread exits the process.
        loop {
            std::thread::park();
        }
    })
}

/// A frame of `len` bytes for every size, alive for the whole process.
fn frames(sizes: &[usize]) -> &'static [&'static [u8]] {
    let frames: Vec<&'static [u8]> = sizes
        .iter()
        .map(|&len| {
            let mut frame = vec![b'x'; len];
            frame[..paths::HEADER].copy_from_slice(&(len as u32).to_le_bytes());
            &*frame.leak()
        })
        .collect();
    frames.leak()
}

/// `a / b`, or `na`.
fn per(a: Option<u64>, b: u64) -> String {
    match a {
        Some(a) if b > 0 => format!("{:.0}", a as f64 / b as f64),
        _ => "na".into(),
    }
}

/// Nanoseconds as microseconds, or `na`.
fn us(ns: Option<u64>) -> String {
    ns.map_or("na".into(), |ns| format!("{:.1}", ns as f64 / 1e3))
}

fn main() {
    let a = args();
    let fixedfd = a.ring.iter().any(|r| r == "fixedfd");
    // Both ends of every connection are in this process; a fixed file table may not be
    // larger than the limit either.
    let tables = usize::from(fixedfd) + usize::from(a.direct);
    let need = (2 * (a.conns + a.bulk) + 64).max(tables * accept::FIXED_FILES as usize + 64);
    match measure::raise_nofile(need as u64) {
        Ok(None) => {}
        Ok(Some((old, new))) => eprintln!("reqresp: raised RLIMIT_NOFILE from {old} to {new}"),
        Err(e) => die(format!("RLIMIT_NOFILE: {e}")),
    }

    let label = [
        ("path", a.path.name().to_string()),
        (
            "ring",
            if a.ring.is_empty() {
                "none".into()
            } else {
                a.ring.join(",")
            },
        ),
        ("deadline", a.deadline.name().to_string()),
        ("accept", if a.direct { "direct" } else { "fd" }.to_string()),
        ("conns", a.conns.to_string()),
        (
            "sizes",
            a.sizes
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        ("bulk", a.bulk.to_string()),
        ("think_us", a.think.to_string()),
    ];
    let (conns, bulk, sizes) = (a.conns, a.bulk, a.sizes.clone());
    let think = Duration::from_micros(a.think);
    let client_cpus = a.client_cpus.clone();

    let shared = Arc::new(Shared::new());
    let (addr_tx, addr_rx) = mpsc::channel();
    let (srv_tx, srv_rx) = mpsc::channel();
    let s = shared.clone();
    std::thread::Builder::new()
        .name("rr-server".into())
        .spawn(move || server(a, s, addr_tx, srv_tx))
        .expect("spawn");
    let Ok((addr, bulk_addr)) = addr_rx.recv() else {
        die("the server thread ended before it listened")
    };

    let frames = frames(&sizes);
    let n = client_cpus.len();
    let (cli_tx, cli_rx) = mpsc::channel();
    for (t, &cpu) in client_cpus.iter().enumerate() {
        let plan = client::Plan {
            cpu,
            // Connection `i` starts at size `i % len` and lives on thread `i % n`.
            conns: (0..conns)
                .filter(|i| i % n == t)
                .map(|i| i % sizes.len())
                .collect(),
            bulk: (0..bulk).filter(|j| j % n == t).count(),
            frames,
            think,
            addr,
            bulk_addr,
        };
        let (s, tx) = (shared.clone(), cli_tx.clone());
        std::thread::Builder::new()
            .name(format!("rr-client{t}"))
            .spawn(move || client::run(plan, s, tx))
            .expect("spawn");
    }

    let Ok(srv) = srv_rx.recv() else {
        die("the server thread ended without a result")
    };
    let mut hist = Hist::new();
    let (mut max_gap, mut failed) = (0, 0);
    let (mut conn_min, mut conn_max) = (u64::MAX, 0);
    for _ in 0..n {
        let Ok(r) = cli_rx.recv_timeout(Duration::from_secs(10)) else {
            die("a client thread did not report")
        };
        hist.merge(&r.hist);
        max_gap = max_gap.max(r.max_gap_ns);
        conn_min = conn_min.min(r.conn_frames_min);
        conn_max = conn_max.max(r.conn_frames_max);
        failed += r.failed;
    }
    if conns == 0 {
        conn_min = 0;
    }

    let frames_per_s = srv.frames as f64 / srv.wall;
    let frames_per_cpu_s = if srv.cpu > 0.0 {
        format!("{:.0}", srv.frames as f64 / srv.cpu)
    } else {
        "na".into()
    };
    let (p50, p99) = (us(hist.quantile(0.5)), us(hist.quantile(0.99)));
    let max = us((hist.n > 0).then_some(hist.max));
    let max_gap_ms = format!("{:.2}", max_gap as f64 / 1e6);
    let bulk_mb_s = format!("{:.1}", srv.bulk_bytes as f64 / 1e6 / srv.wall);
    let [user_cycles, user_instr, cycles] = srv.perf.map(|p| per(p, srv.frames));

    println!(
        "reqresp {}: {frames_per_s:.0} frames/s, {frames_per_cpu_s} frames/cpu-s (server cpu \
         {:.2}), reply p50 {p50} us p99 {p99} us max {max} us, longest gap {max_gap_ms} ms, \
         bulk {bulk_mb_s} MB/s; per frame: user instructions {user_instr}, user cycles \
         {user_cycles}, cycles {cycles}",
        label
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" "),
        srv.cpu / srv.wall,
    );

    let mut line = String::from("RESULT");
    let mut kv = |k: &str, v: &dyn Display| {
        let _ = write!(line, " {k}={v}");
    };
    for (k, v) in &label {
        kv(k, v);
    }
    kv("frames_per_s", &format_args!("{frames_per_s:.0}"));
    kv("frames_per_cpu_s", &frames_per_cpu_s);
    kv("p50_us", &p50);
    kv("p99_us", &p99);
    kv("max_us", &max);
    kv("max_gap_ms", &max_gap_ms);
    kv("bulk_mb_s", &bulk_mb_s);
    kv("user_instr_per_frame", &user_instr);
    kv("user_cycles_per_frame", &user_cycles);
    kv("cycles_per_frame", &cycles);
    kv("server_cpu", &format_args!("{:.3}", srv.cpu / srv.wall));
    kv("window_s", &format_args!("{:.3}", srv.wall));
    kv("frames", &srv.frames);
    kv(
        "frame_mb_s",
        &format_args!("{:.1}", srv.frame_bytes as f64 / 1e6 / srv.wall),
    );
    kv("client_frames", &hist.n);
    kv("p90_us", &us(hist.quantile(0.9)));
    kv("p999_us", &us(hist.quantile(0.999)));
    kv(
        "mean_us",
        &us((hist.n > 0).then(|| hist.sum / hist.n.max(1))),
    );
    kv("conn_frames_min", &conn_min);
    kv("conn_frames_max", &conn_max);
    kv("ramp_s", &format_args!("{:.3}", srv.ramp));
    kv("server_errors", &srv.errors);
    kv("client_failed", &failed);
    kv("enobufs", &srv.enobufs);
    println!("{line}");
    if let Some(stats) = srv.stats {
        println!("STATS{stats}");
    }
    // Server and clients are parked with their connections open.
    std::process::exit(0);
}
