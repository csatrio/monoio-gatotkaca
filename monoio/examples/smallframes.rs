//! `smallframes`: many connections, small frames, one server core (round 2, M5).
//!
//! The server runtime is pinned to `--cpu`. Client runtimes on `--client-cpus` keep `--conns`
//! loopback connections busy in a closed loop: each keeps `--depth` frames of `--size` bytes in
//! flight and sends the next one per 16 byte ack. The server reads the whole frame (one recv per
//! read, appended to a `BytesMut`), then sends the ack, like the benchmark's `POST /repl`. Each
//! frame is served under a
//! `monoio::time::timeout` like the benchmark server (`--no-timeout` drops it).
//!
//! Reported: frames/s and frames per CPU-second of the server thread (`getrusage(RUSAGE_THREAD)`
//! over the measured window), MB/CPU-s of request bytes, and per frame the user cycles, user
//! instructions and all cycles of the server thread (`perf_event_open`, if allowed).
//! Notes: `docs/performance-improvement/round2/W2-m5.md`.
//!
//! Run: `cargo run --release -p adaptif-gatotkaca-monoio --example smallframes -- --conns 2000`.

use std::{
    cell::Cell,
    io,
    net::SocketAddr,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use monoio::{
    buf::IoBufMut,
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{AcceptFlags, TcpListener, TcpStream},
    IoUringDriver, RuntimeBuilder,
};

const USAGE: &str = "\
smallframes: many connections, small request frames, small ack, one server core

  --cpu C              server CPU (default 2)
  --client-cpus a,b    client CPUs, one client thread each (default 4,6)
  --conns N            connections (default 2000)
  --size SZ            request frame size in bytes (default 4096; k suffix = KiB)
  --entries N          server ring entries (default 4096)
  --no-timeout         no per-frame timeout on the server
  --fixed              register each accepted stream as a fixed file
  --ring f1,f2        ring setup: nosqarray, coop, defer, ringfd, skipenter
  --depth D            frames in flight per connection (default 2: the next frame is usually
                       queued when the server issues its recv, like an overloaded open loop)
  --warmup S           seconds before the window (default 2)
  --duration S         measured window (default 8)";

const ACK: &[u8] = b"HTTP/1.1 200 OK\r";

struct Args {
    cpu: usize,
    client_cpus: Vec<usize>,
    conns: usize,
    size: usize,
    entries: u32,
    timeout: bool,
    fixed: bool,
    depth: usize,
    ring: Vec<String>,
    warmup: f64,
    duration: f64,
}

fn parse_size(s: &str) -> usize {
    let s = s.to_ascii_lowercase();
    if let Some(k) = s.strip_suffix('k') {
        k.parse::<usize>().expect("size") * 1024
    } else {
        s.parse().expect("size")
    }
}

fn args() -> Args {
    let mut a = Args {
        cpu: 2,
        client_cpus: vec![4, 6],
        conns: 2000,
        size: 4096,
        entries: 4096,
        timeout: true,
        fixed: false,
        depth: 2,
        ring: Vec::new(),
        warmup: 2.0,
        duration: 8.0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(f) = it.next() {
        let mut v = || it.next().expect("missing value");
        match f.as_str() {
            "--cpu" => a.cpu = v().parse().unwrap(),
            "--client-cpus" => a.client_cpus = v().split(',').map(|c| c.parse().unwrap()).collect(),
            "--conns" => a.conns = v().parse().unwrap(),
            "--size" => a.size = parse_size(&v()),
            "--entries" => a.entries = v().parse().unwrap(),
            "--no-timeout" => a.timeout = false,
            "--fixed" => a.fixed = true,
            "--depth" => a.depth = v().parse().unwrap(),
            "--ring" => a.ring = v().split(',').map(String::from).collect(),
            "--warmup" => a.warmup = v().parse().unwrap(),
            "--duration" => a.duration = v().parse().unwrap(),
            _ => {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
        }
    }
    a
}

/// CPU time of the calling thread.
fn thread_cpu() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `ru` is a valid out pointer.
    unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut ru) };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// A hardware counter of the calling thread (`perf_event_open`), `None` if not allowed.
struct Counter(Option<i32>);

impl Counter {
    /// `config`: 0 cycles, 1 instructions. `user_only` excludes kernel and hypervisor.
    fn new(config: u64, user_only: bool) -> Self {
        // struct perf_event_attr, PERF_ATTR_SIZE_VER5 (112 bytes).
        let mut attr = [0_u64; 14];
        attr[0] = 112 << 32; // type PERF_TYPE_HARDWARE (0), size
        attr[1] = config;
        // flags bitfield: exclude_kernel (bit 5), exclude_hv (bit 6)
        if user_only {
            attr[5] = (1 << 5) | (1 << 6);
        }
        // SAFETY: valid attr buffer; pid 0 = this thread, cpu -1 = any.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                attr.as_ptr(),
                0,
                -1,
                -1,
                8_u64, // PERF_FLAG_FD_CLOEXEC
            )
        };
        Counter((fd >= 0).then_some(fd as i32))
    }

    fn read(&self) -> u64 {
        let Some(fd) = self.0 else { return 0 };
        let mut v = 0_u64;
        // SAFETY: reads 8 bytes into `v`.
        unsafe { libc::read(fd, (&mut v as *mut u64).cast(), 8) };
        v
    }
}

async fn serve_frame(s: &mut TcpStream, buf: &mut BytesMut, size: usize) -> io::Result<bool> {
    while buf.len() < size {
        if buf.capacity() - buf.len() < size {
            buf.reserve(size);
        }
        let len = buf.len();
        let (r, b) = s.read(std::mem::take(buf).slice_mut(len..)).await;
        *buf = b.into_inner();
        if r? == 0 {
            return Ok(false);
        }
    }
    let _ = buf.split_to(size);
    let (r, _) = s.write_all(Bytes::from_static(ACK)).await;
    r?;
    Ok(true)
}

async fn conn(mut s: TcpStream, size: usize, timeout: bool, frames: Rc<Cell<u64>>) {
    let mut buf = BytesMut::with_capacity(size * 2);
    loop {
        let r = if timeout {
            match monoio::time::timeout(
                Duration::from_secs(10),
                serve_frame(&mut s, &mut buf, size),
            )
            .await
            {
                Ok(r) => r,
                Err(_) => return,
            }
        } else {
            serve_frame(&mut s, &mut buf, size).await
        };
        match r {
            Ok(true) => frames.set(frames.get() + 1),
            _ => return,
        }
    }
}

fn server(a: &Args, addr_tx: mpsc::Sender<SocketAddr>, stop: Arc<AtomicBool>) {
    let _ = monoio::utils::bind_to_cpu_set([a.cpu]);
    let has = |f: &str| a.ring.iter().any(|r| r == f);
    let mut b = RuntimeBuilder::<IoUringDriver>::new();
    if has("nosqarray") {
        let mut u = monoio::io_uring::IoUring::builder();
        u.setup_no_sqarray();
        b = b.uring_builder(u);
    }
    let mut b = b.with_entries(a.entries);
    if has("coop") {
        b = b.coop_taskrun().taskrun_flag();
    }
    if has("defer") {
        b = b.defer_taskrun();
    }
    if has("ringfd") {
        b = b.register_ring_fd();
    }
    if a.fixed {
        b = b.with_fixed_files(8192);
    }
    if has("skipenter") {
        b = b.skip_empty_enter();
    }
    let mut rt = b.enable_timer().build().expect("server runtime");
    let (size, timeout, fixed) = (a.size, a.timeout, a.fixed);
    let (warmup, duration) = (a.warmup, a.duration);
    rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        addr_tx.send(listener.local_addr().unwrap()).unwrap();
        let frames = Rc::new(Cell::new(0_u64));
        let f2 = frames.clone();
        monoio::spawn(async move {
            let mut v = Vec::new();
            loop {
                if listener
                    .accept_batch_into(&mut v, 256, AcceptFlags::EMPTY)
                    .await
                    .is_err()
                {
                    continue;
                }
                for (s, _) in v.drain(..) {
                    s.set_nodelay(true).unwrap();
                    if fixed {
                        s.register_fixed().unwrap();
                    }
                    monoio::spawn(conn(s, size, timeout, f2.clone()));
                }
            }
        });
        let counters = [
            Counter::new(0, true),
            Counter::new(1, true),
            Counter::new(0, false),
        ];
        monoio::time::sleep(Duration::from_secs_f64(warmup)).await;
        let (f0, c0, t0) = (frames.get(), thread_cpu(), Instant::now());
        let p0 = counters.each_ref().map(|c| c.read());
        #[cfg(feature = "metrics")]
        let (d0, r0) = (monoio::driver_stats(), monoio::runtime_stats());
        monoio::time::sleep(Duration::from_secs_f64(duration)).await;
        let (f1, c1, t1) = (frames.get(), thread_cpu(), Instant::now());
        let p1 = counters.each_ref().map(|c| c.read());
        let fr = (f1 - f0) as f64;
        let cpu = (c1 - c0).as_secs_f64();
        let wall = (t1 - t0).as_secs_f64();
        println!(
            "frames/s {:.0}  cpu {:.2}  frames/cpu-s {:.0}  MB/cpu-s {:.1}",
            fr / wall,
            cpu / wall,
            fr / cpu,
            fr * size as f64 / cpu / 1e6
        );
        let per = |i: usize| (p1[i] - p0[i]) as f64 / fr;
        println!(
            "per frame: user cycles {:.0}  user instructions {:.0}  all cycles {:.0}",
            per(0),
            per(1),
            per(2)
        );
        #[cfg(feature = "metrics")]
        {
            let (d1, r1) = (monoio::driver_stats(), monoio::runtime_stats());
            println!(
                "enter_wait {} enter_submit {} sqes {} rounds {} parks {}",
                d1.enter_wait - d0.enter_wait,
                d1.enter_submit - d0.enter_submit,
                d1.sqes_submitted - d0.sqes_submitted,
                r1.rounds - r0.rounds,
                r1.parks - r0.parks,
            );
        }
        stop.store(true, Ordering::Relaxed);
    });
}

fn client(
    cpu: usize,
    conns: usize,
    (size, depth): (usize, usize),
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
) {
    let _ = monoio::utils::bind_to_cpu_set([cpu]);
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(4096)
        .build()
        .expect("client runtime");
    rt.block_on(async move {
        let frame = Bytes::from(vec![b'x'; size]);
        let mut tasks = Vec::new();
        for _ in 0..conns {
            let s = loop {
                match TcpStream::connect(addr).await {
                    Ok(s) => break s,
                    Err(_) => monoio::task::yield_now().await,
                }
            };
            s.set_nodelay(true).unwrap();
            let frame = frame.clone();
            let stop = stop.clone();
            tasks.push(monoio::spawn(async move {
                let mut s = s;
                for _ in 0..depth {
                    if s.write_all(frame.clone()).await.0.is_err() {
                        return;
                    }
                }
                let mut acks = vec![0_u8; 4096];
                let mut pending = 0;
                while !stop.load(Ordering::Relaxed) {
                    let (r, b) = s.read(acks).await;
                    acks = b;
                    match r {
                        Ok(n) if n > 0 => pending += n,
                        _ => return,
                    }
                    while pending >= ACK.len() {
                        pending -= ACK.len();
                        if s.write_all(frame.clone()).await.0.is_err() {
                            return;
                        }
                    }
                }
            }));
        }
        for t in tasks {
            t.await;
        }
    });
}

fn main() {
    let a = args();
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let s2 = stop.clone();
    let srv = std::thread::Builder::new()
        .name("sf-server".into())
        .spawn(move || server(&a, tx, s2))
        .unwrap();
    let addr = rx.recv().unwrap();
    let a = args();
    let n = a.client_cpus.len();
    for (i, &cpu) in a.client_cpus.iter().enumerate() {
        let conns = a.conns / n + usize::from(i < a.conns % n);
        let (size, depth, stop) = (a.size, a.depth, stop.clone());
        std::thread::Builder::new()
            .name(format!("sf-client{i}"))
            .spawn(move || client(cpu, conns, (size, depth), addr, stop))
            .unwrap();
    }
    srv.join().unwrap();
    // Clients may be blocked in reads; just exit.
    std::process::exit(0);
}
