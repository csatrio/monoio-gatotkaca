//! Kernel behaviour probes for round 3 (KV1-KV5 of `docs/performance-improvement/ROUND3-PLAN.md`).
//! Measurements, not tests: every probe prints what the running kernel does and asserts only
//! its own set-up. The answers for Linux 7.0 are in `docs/performance-improvement/round3/r3-probe.md`.
//!
//! ```sh
//! cargo test -p adaptif-gatotkaca-monoio --test kernel_probe_r3 -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Every probe drives a raw ring of its own (`monoio::io_uring::IoUring`) with plain sockets, so
//! monoio's driver is not part of what is observed. A probe whose feature is missing prints
//! `skipped: ...`.
//!
//! - KV1: how many deferred task-work items one `io_uring_enter` runs on a `DEFER_TASKRUN` ring,
//!   and when `IORING_SQ_TASKRUN` is set.
//! - KV2: which CQEs a link chain with `IOSQE_CQE_SKIP_SUCCESS` posts.
//! - KV3: a stream `SEND` larger than the socket buffer, with and without `MSG_WAITALL`.
//! - KV4: when the kernel reads the `msghdr` and the iovec array of a `SENDMSG`.
//! - KV5: `IORING_SEND_VECTORIZED`.

use std::{
    fs::File,
    io::{self, ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    os::{
        fd::{AsRawFd, OwnedFd, RawFd},
        unix::net::UnixStream,
    },
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use monoio::io_uring::{cqueue, opcode, squeue, types, IoUring};

/// `IORING_ENTER_GETEVENTS`.
const GETEVENTS: u32 = 1;
/// `IORING_ENTER_EXT_ARG`.
const EXT_ARG: u32 = 8;
/// `IORING_RECVSEND_POLL_FIRST` (`sqe->ioprio`).
const POLL_FIRST: u16 = 1 << 0;
/// `IORING_SEND_VECTORIZED` (`sqe->ioprio`), from the uapi header of Linux 7.0.
const SEND_VECTORIZED: u16 = 1 << 5;
/// A bit no send/recv opcode accepts in `sqe->ioprio`: the SQE is rejected when it is prepared.
const BAD_IOPRIO: u16 = 1 << 15;

const MS: Duration = Duration::from_millis(1);

// ---------------------------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------------------------

fn kernel() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|_| "?".into())
}

/// Aborts the process when a probe blocks in an enter it expected to return.
struct Watchdog(Arc<AtomicBool>);

fn watchdog(name: &'static str, secs: u64) -> Watchdog {
    let done = Arc::new(AtomicBool::new(false));
    let seen = done.clone();
    std::thread::spawn(move || {
        let end = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < end {
            if seen.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(50 * MS);
        }
        eprintln!("{name}: still running after {secs} s (an enter blocked?), aborting");
        std::process::abort();
    });
    Watchdog(done)
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn res_str(res: i32) -> String {
    if res >= 0 {
        return res.to_string();
    }
    let name = match -res {
        libc::EPIPE => "EPIPE",
        libc::ECONNRESET => "ECONNRESET",
        libc::ECANCELED => "ECANCELED",
        libc::EINVAL => "EINVAL",
        libc::EBADF => "EBADF",
        libc::EFAULT => "EFAULT",
        libc::ENOENT => "ENOENT",
        libc::EALREADY => "EALREADY",
        libc::EAGAIN => "EAGAIN",
        libc::ENOTCONN => "ENOTCONN",
        libc::ETIME => "ETIME",
        libc::EINTR => "EINTR",
        libc::EOPNOTSUPP => "EOPNOTSUPP",
        libc::ENOTSOCK => "ENOTSOCK",
        libc::EMSGSIZE => "EMSGSIZE",
        libc::ENOMEM => "ENOMEM",
        _ => return format!("{res} ({})", io::Error::from_raw_os_error(-res)),
    };
    format!("-{name}")
}

fn ret_str(ret: &io::Result<usize>) -> String {
    match ret {
        Ok(n) => n.to_string(),
        Err(e) => res_str(-e.raw_os_error().unwrap_or(0)),
    }
}

/// `io_uring_enter` with exactly these arguments.
fn enter(ring: &IoUring, to_submit: u32, min_complete: u32, flags: u32) -> io::Result<usize> {
    unsafe {
        ring.submitter()
            .enter::<libc::sigset_t>(to_submit, min_complete, flags, None)
    }
}

/// `io_uring_enter(GETEVENTS | EXT_ARG)` with a timeout.
fn enter_ts(
    ring: &IoUring,
    to_submit: u32,
    min_complete: u32,
    timeout: Duration,
) -> io::Result<usize> {
    let ts = types::Timespec::new()
        .sec(timeout.as_secs())
        .nsec(timeout.subsec_nanos());
    let args = types::SubmitArgs::new().timespec(&ts);
    unsafe {
        ring.submitter()
            .enter(to_submit, min_complete, GETEVENTS | EXT_ARG, Some(&args))
    }
}

fn push(ring: &mut IoUring, sqe: squeue::Entry) {
    unsafe { ring.submission().push(&sqe).expect("SQ full") };
}

/// Submit what is queued, without `GETEVENTS`.
fn submit_only(ring: &mut IoUring) {
    let n = ring.submission().len() as u32;
    let done = enter(ring, n, 0, 0).expect("submit");
    assert_eq!(done, n as usize, "the kernel did not take every SQE");
}

#[derive(Clone, Copy, Debug)]
struct Cqe {
    ud: u64,
    res: i32,
    flags: u32,
}

fn reap(ring: &mut IoUring) -> Vec<Cqe> {
    ring.completion()
        .map(|c| Cqe {
            ud: c.user_data(),
            res: c.result(),
            flags: c.flags(),
        })
        .collect()
}

/// Submit what is queued and collect CQEs, in CQ order, until none arrived for `quiet`.
fn settle(ring: &mut IoUring, quiet: Duration) -> Vec<Cqe> {
    let mut all = Vec::new();
    loop {
        let n = ring.submission().len() as u32;
        match enter_ts(ring, n, 1, quiet) {
            Ok(_) => {}
            Err(e) if matches!(e.raw_os_error(), Some(libc::ETIME | libc::EINTR)) => {}
            Err(e) => panic!("enter: {e}"),
        }
        let got = reap(ring);
        if got.is_empty() {
            return all;
        }
        all.extend(got);
    }
}

/// `[send: -EPIPE, recv: 4]`, with the member names of `names` for the `user_data` values.
fn show(cqes: &[Cqe], names: &[(u64, &str)]) -> String {
    if cqes.is_empty() {
        return "0 CQEs".into();
    }
    let parts: Vec<String> = cqes
        .iter()
        .map(|c| {
            let name = names
                .iter()
                .find(|(ud, _)| *ud == c.ud)
                .map(|(_, n)| (*n).to_owned())
                .unwrap_or_else(|| format!("user_data {}", c.ud));
            let mut s = format!("{name}: {}", res_str(c.res));
            if cqueue::more(c.flags) {
                s.push_str(" F_MORE");
            }
            if cqueue::notif(c.flags) {
                s.push_str(" F_NOTIF");
            }
            s
        })
        .collect();
    let n = cqes.len();
    let plural = if n == 1 { "" } else { "s" };
    format!("{n} CQE{plural} [{}]", parts.join(", "))
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Tcp,
    Unix,
}

fn setsockopt_int(fd: RawFd, name: libc::c_int, val: libc::c_int) {
    let r = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            name,
            (&val as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    assert_eq!(r, 0, "setsockopt: {}", io::Error::last_os_error());
}

fn set_nonblocking(f: &File, on: bool) {
    let fd = f.as_raw_fd();
    unsafe {
        let fl = libc::fcntl(fd, libc::F_GETFL);
        assert!(fl >= 0);
        let fl = if on {
            fl | libc::O_NONBLOCK
        } else {
            fl & !libc::O_NONBLOCK
        };
        assert_eq!(libc::fcntl(fd, libc::F_SETFL, fl), 0);
    }
}

fn shutdown_write(f: &File) {
    let r = unsafe { libc::shutdown(f.as_raw_fd(), libc::SHUT_WR) };
    assert_eq!(r, 0, "shutdown: {}", io::Error::last_os_error());
}

/// Close so that the other side sees a reset (TCP: `SO_LINGER` 0 sends an RST).
fn reset_close(f: File, kind: Kind) {
    if kind == Kind::Tcp {
        let l = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        let r = unsafe {
            libc::setsockopt(
                f.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&l as *const libc::linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(r, 0, "SO_LINGER: {}", io::Error::last_os_error());
    }
    drop(f);
    // Let the reset reach the other socket (loopback delivers it at once; be generous).
    std::thread::sleep(30 * MS);
}

/// A connected stream pair: (the side the ring uses, its peer). Both as `File`, so that plain
/// `read`/`write` work on either kind.
fn pair(kind: Kind) -> (File, File) {
    pair_with(kind, None)
}

fn pair_with(kind: Kind, peer_rcvbuf: Option<i32>) -> (File, File) {
    match kind {
        Kind::Tcp => {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            if let Some(n) = peer_rcvbuf {
                // Before the connection exists, so that the accepted socket inherits it.
                setsockopt_int(l.as_raw_fd(), libc::SO_RCVBUF, n);
            }
            let a = TcpStream::connect(l.local_addr().unwrap()).unwrap();
            let (b, _) = l.accept().unwrap();
            a.set_nodelay(true).unwrap();
            b.set_nodelay(true).unwrap();
            (File::from(OwnedFd::from(a)), File::from(OwnedFd::from(b)))
        }
        Kind::Unix => {
            let (a, b) = UnixStream::pair().unwrap();
            (File::from(OwnedFd::from(a)), File::from(OwnedFd::from(b)))
        }
    }
}

/// A pair with the smallest socket buffers the kernel allows and a non-blocking peer: a large
/// send cannot complete until the peer reads.
fn small_pair(kind: Kind) -> (File, File) {
    let (a, b) = pair_with(kind, Some(4096));
    setsockopt_int(a.as_raw_fd(), libc::SO_SNDBUF, 4096);
    set_nonblocking(&b, true);
    (a, b)
}

/// Write to `a` until its socket buffer is full. Returns the number of bytes queued.
fn fill_socket(a: &File) -> usize {
    set_nonblocking(a, true);
    let junk = [0x55u8; 4096];
    let mut total = 0;
    let mut w = a;
    loop {
        match w.write(&junk) {
            Ok(n) => total += n,
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) => panic!("prefill: {e}"),
        }
    }
    set_nonblocking(a, false);
    assert!(total > 0);
    total
}

/// Read from the non-blocking `peer` into `out` until `out` holds `want` bytes or nothing arrived
/// for `quiet`. While the peer has nothing, the ring is entered with `GETEVENTS` (2 ms timeout),
/// so pending task work runs; CQEs are left in the CQ.
fn pump(ring: &mut IoUring, peer: &mut File, out: &mut Vec<u8>, want: usize, quiet: Duration) {
    let mut chunk = vec![0u8; 256 * 1024];
    let mut last = Instant::now();
    while out.len() < want {
        match peer.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                out.extend_from_slice(&chunk[..n]);
                last = Instant::now();
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                if last.elapsed() > quiet {
                    break;
                }
                let have = ring.completion().len() as u32;
                let _ = enter_ts(ring, 0, have + 1, 2 * MS);
            }
            Err(e) => {
                println!("      (peer read error: {e})");
                break;
            }
        }
    }
}

/// What the peer has received so far, without blocking.
fn peer_available(peer: &mut File) -> Vec<u8> {
    set_nonblocking(peer, true);
    let mut out = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match peer.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => out.extend_from_slice(&chunk[..n]),
        }
    }
    set_nonblocking(peer, false);
    out
}

fn peer_text(peer: &mut File) -> String {
    let got = peer_available(peer);
    if got.is_empty() {
        "nothing".into()
    } else {
        format!("{:?}", String::from_utf8_lossy(&got))
    }
}

fn payload(k: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 7 + k * 31) % 251) as u8).collect()
}

fn compare(got: &[u8], want: &[u8]) -> String {
    if got == want {
        return "intact".into();
    }
    let at = got
        .iter()
        .zip(want)
        .position(|(g, w)| g != w)
        .unwrap_or(got.len().min(want.len()));
    format!(
        "DIFFERENT (got {} bytes, expected {}, first difference at byte {at})",
        got.len(),
        want.len()
    )
}

/// Number of io-wq workers (`iou-wrk-<tid>` threads) of the calling thread.
fn iowq_workers() -> usize {
    let name = format!("iou-wrk-{}", unsafe { libc::gettid() });
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .flatten()
        .filter(|e| {
            std::fs::read_to_string(e.path().join("comm")).is_ok_and(|comm| comm.trim() == name)
        })
        .count()
}

fn link_skip() -> squeue::Flags {
    squeue::Flags::IO_LINK | squeue::Flags::SKIP_SUCCESS
}

fn send(fd: RawFd, buf: &[u8], ioprio: u16, msg_flags: i32) -> squeue::Entry {
    opcode::Send::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
        .ioprio(ioprio)
        .flags(msg_flags | libc::MSG_NOSIGNAL)
        .build()
}

fn recv(fd: RawFd, buf: &mut [u8], ioprio: u16) -> squeue::Entry {
    opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
        .ioprio(ioprio)
        .build()
}

/// The pair C1 wants: SEND (`user_data` 1) linked to RECV (`user_data` 2).
#[derive(Clone, Copy)]
struct PairSpec {
    send_fd: RawFd,
    head: squeue::Flags,
    send_prio: u16,
    send_flags: i32,
    recv_fd: RawFd,
    recv_prio: u16,
}

impl PairSpec {
    /// SEND(`IO_LINK | CQE_SKIP_SUCCESS`, `MSG_WAITALL`) -> RECV(`POLL_FIRST`) on `fd`.
    fn new(fd: RawFd) -> Self {
        PairSpec {
            send_fd: fd,
            head: link_skip(),
            send_prio: 0,
            send_flags: libc::MSG_WAITALL,
            recv_fd: fd,
            recv_prio: POLL_FIRST,
        }
    }

    fn push(self, ring: &mut IoUring, msg: &[u8], rbuf: &mut [u8]) {
        push(
            ring,
            send(self.send_fd, msg, self.send_prio, self.send_flags)
                .flags(self.head)
                .user_data(1),
        );
        push(ring, recv(self.recv_fd, rbuf, self.recv_prio).user_data(2));
    }
}

const PAIR: [(u64, &str); 3] = [(1, "send"), (2, "recv"), (9, "cancel")];
const TRIO: [(u64, &str); 3] = [(1, "m1"), (2, "m2"), (3, "m3")];

// ---------------------------------------------------------------------------------------------
// KV1
// ---------------------------------------------------------------------------------------------

/// Receives armed per scenario.
const N: usize = 200;
/// `user_data` of the NOPs a scenario submits.
const NOP: u64 = 1000;

/// A `SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG` ring with `N` socket pairs. `ready()` arms one
/// `RECV(POLL_FIRST)` per pair and writes one byte to every peer: `N` deferred task-work items
/// are then pending, each of which posts one CQE when it is run.
struct Kv1 {
    ring: IoUring,
    pairs: Vec<(UnixStream, UnixStream)>,
    /// One receive buffer per pair; never resized, so the addresses given to the kernel stay.
    bufs: Vec<[u8; 8]>,
    /// Receive CQEs reaped since the last `arm()`.
    done: usize,
}

impl Kv1 {
    fn new() -> Option<Self> {
        let ring = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_taskrun_flag()
            .setup_cqsize(4096)
            .build(256);
        let ring = match ring {
            Ok(r) => r,
            Err(e) => {
                println!("skipped: no SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG ring ({e})");
                return None;
            }
        };
        let pairs = (0..N).map(|_| UnixStream::pair().unwrap()).collect();
        let bufs = vec![[0u8; 8]; N];
        Some(Kv1 {
            ring,
            pairs,
            bufs,
            done: 0,
        })
    }

    fn taskrun(&mut self) -> bool {
        self.ring.submission().taskrun()
    }

    fn cq_len(&mut self) -> usize {
        self.ring.completion().len()
    }

    fn arm(&mut self) {
        assert_eq!(self.done, 0);
        for (i, (s, _)) in self.pairs.iter().enumerate() {
            let sqe = opcode::Recv::new(types::Fd(s.as_raw_fd()), self.bufs[i].as_mut_ptr(), 8)
                .ioprio(POLL_FIRST)
                .build()
                .user_data(i as u64);
            unsafe { self.ring.submission().push(&sqe).unwrap() };
        }
        let n = enter(&self.ring, N as u32, 0, 0).unwrap();
        assert_eq!(n, N);
        assert_eq!(
            self.cq_len(),
            0,
            "a receive completed before its peer wrote"
        );
        assert!(!self.taskrun(), "SQ_TASKRUN set with nothing pending");
    }

    /// Arm, then make every receive ready.
    fn ready(&mut self) {
        self.arm();
        for (_, p) in &self.pairs {
            (&*p).write_all(b"x").unwrap();
        }
        assert_eq!(self.cq_len(), 0, "a CQE was posted outside an enter");
    }

    fn reap(&mut self) -> usize {
        let mut n = 0;
        for c in self.ring.completion() {
            if c.user_data() < N as u64 {
                assert_eq!(c.result(), 1, "receive result");
                self.done += 1;
            } else {
                assert_eq!(c.user_data(), NOP);
            }
            n += 1;
        }
        n
    }

    fn nop(&mut self) {
        push(&mut self.ring, opcode::Nop::new().build().user_data(NOP));
    }

    /// One enter. Prints what it did and returns the number of CQEs it added. Nothing is reaped.
    fn step(
        &mut self,
        label: &str,
        to_submit: u32,
        min_complete: u32,
        getevents: bool,
        ts: Option<Duration>,
    ) -> usize {
        let before = self.cq_len();
        let t = Instant::now();
        let (ret, flags) = match ts {
            Some(d) => (
                enter_ts(&self.ring, to_submit, min_complete, d),
                format!("GETEVENTS|EXT_ARG ts={d:?}"),
            ),
            None if getevents => (
                enter(&self.ring, to_submit, min_complete, GETEVENTS),
                "GETEVENTS".to_owned(),
            ),
            None => (
                enter(&self.ring, to_submit, min_complete, 0),
                "0".to_owned(),
            ),
        };
        let us = t.elapsed().as_micros();
        let after = self.cq_len();
        let flag = self.taskrun();
        println!(
            "    {label}: enter(to_submit={to_submit}, min_complete={min_complete}, {flags}) -> {}: CQ \
             {before} -> {after} (+{}), SQ_TASKRUN after: {flag}, {us} us",
            ret_str(&ret),
            after - before
        );
        after - before
    }

    /// Reap, then enter (`to_submit` 0, `min_complete` 0, `GETEVENTS`) until all `N` receives
    /// completed. Returns the CQEs each of those enters added and the flag after each.
    fn finish(&mut self) -> (Vec<usize>, Vec<bool>) {
        let mut per_call = Vec::new();
        let mut flags = Vec::new();
        self.reap();
        while self.done < N {
            assert!(per_call.len() < 10 * N, "receives never completed");
            enter(&self.ring, 0, 0, GETEVENTS).unwrap();
            per_call.push(self.reap());
            flags.push(self.taskrun());
        }
        assert!(!self.taskrun(), "SQ_TASKRUN set after everything completed");
        self.done = 0;
        (per_call, flags)
    }

    fn finish_and_print(&mut self) {
        let (per_call, _) = self.finish();
        println!(
            "    rest: {} more enter(0, 0, GETEVENTS), CQEs per enter {per_call:?}",
            per_call.len()
        );
    }

    /// Block in `enter(0, min_complete, GETEVENTS)` on an idle ring; another thread makes all
    /// receives ready 50 ms later.
    fn sleeping_wait(&mut self, label: &str, min_complete: u32) {
        self.arm();
        let pairs = &self.pairs;
        let ring = &mut self.ring;
        std::thread::scope(|s| {
            let writer = s.spawn(move || {
                std::thread::sleep(50 * MS);
                for (_, p) in pairs {
                    (&*p).write_all(b"x").unwrap();
                }
            });
            let t = Instant::now();
            let ret = enter_ts(ring, 0, min_complete, Duration::from_secs(5));
            let ms = t.elapsed().as_millis();
            let cq = ring.completion().len();
            let flag = ring.submission().taskrun();
            println!(
                "    {label}: enter(to_submit=0, min_complete={min_complete}, GETEVENTS|EXT_ARG ts=5s) slept \
                 {ms} ms -> {}: CQ 0 -> {cq}, SQ_TASKRUN after: {flag}",
                ret_str(&ret)
            );
            writer.join().unwrap();
        });
        self.finish_and_print();
    }
}

#[test]
#[ignore = "probe: prints kernel behaviour, run by hand"]
fn kv1_defer_taskrun_local_work() {
    let _wd = watchdog("kv1", 120);
    println!(
        "KV1 (kernel {}): DEFER_TASKRUN ring, {N} RECV(POLL_FIRST) made ready before each scenario \
         (one task-work item and one CQE each)",
        kernel()
    );
    let Some(mut k) = Kv1::new() else { return };

    println!("  [flag] IORING_SQ_TASKRUN around one pending item");
    k.arm();
    println!("    idle, receives armed: SQ_TASKRUN {}", k.taskrun());
    (&k.pairs[0].1).write_all(b"x").unwrap();
    println!(
        "    after one peer wrote: SQ_TASKRUN {}, CQ {}",
        k.taskrun(),
        k.cq_len()
    );
    k.step("no GETEVENTS", 0, 0, false, None);
    k.step("GETEVENTS", 0, 0, true, None);
    for (_, p) in &k.pairs[1..] {
        (&*p).write_all(b"x").unwrap();
    }
    k.finish();

    println!("  [A] repeated enter(to_submit=0, min_complete=0, GETEVENTS), CQ reaped after each");
    k.ready();
    println!("    before: CQ {}, SQ_TASKRUN {}", k.cq_len(), k.taskrun());
    let t = Instant::now();
    let (per_call, flags) = k.finish();
    println!(
        "    {} enters in {} us; CQEs per enter {per_call:?}",
        per_call.len(),
        t.elapsed().as_micros()
    );
    println!("    SQ_TASKRUN after each {flags:?}");

    println!("  [B] to_submit=0, GETEVENTS, min_complete > 0, CQ empty before");
    for min in [1, 19, 21, 50, 200] {
        k.ready();
        k.step(&format!("min_complete={min}"), 0, min, true, None);
        k.finish_and_print();
    }

    println!("  [C] one NOP submitted in the same enter");
    k.ready();
    k.nop();
    k.step("NOP, no GETEVENTS", 1, 0, false, None);
    k.finish_and_print();
    for min in [0, 1, 50] {
        k.ready();
        k.nop();
        k.step(
            &format!("NOP, GETEVENTS, min_complete={min}"),
            1,
            min,
            true,
            None,
        );
        k.finish_and_print();
    }

    println!("  [D] min_complete = number of unreaped CQEs in the CQ (nothing reaped in between)");
    k.ready();
    k.step("first", 0, 0, true, None);
    loop {
        let have = k.cq_len() as u32;
        if k.step("next", 0, have, true, None) == 0 {
            break;
        }
    }
    k.finish_and_print();

    println!(
        "  [E] all {N} CQEs delivered but not reaped, no task work pending: does the enter sleep?"
    );
    k.ready();
    while k.taskrun() {
        enter(&k.ring, 0, 0, GETEVENTS).unwrap();
    }
    let have = k.cq_len() as u32;
    assert_eq!(have as usize, N);
    k.step("min_complete = CQ length", 0, have, true, Some(2000 * MS));
    k.step(
        "min_complete = CQ length + 1",
        0,
        have + 1,
        true,
        Some(300 * MS),
    );
    k.finish();

    println!(
        "  [F] the enter sleeps first (idle ring), a second thread writes to all peers after 50 ms"
    );
    k.sleeping_wait("min_complete=1", 1);
    k.sleeping_wait("min_complete=8", 8);
    k.sleeping_wait("min_complete=64", 64);

    println!("  [G] zero timeout (EXT_ARG, ts=0) with a large min_complete: never sleeps");
    for min in [200, 1000] {
        k.ready();
        k.step(
            &format!("min_complete={min}"),
            0,
            min,
            true,
            Some(Duration::ZERO),
        );
        k.finish_and_print();
    }

    println!("  [H] cost of an enter that finds nothing (screening: the machine is not quiet)");
    k.arm();
    const ROUNDS: u32 = 20_000;
    let t = Instant::now();
    for _ in 0..ROUNDS {
        enter(&k.ring, 0, 0, 0).unwrap();
    }
    let plain = t.elapsed().as_nanos() / ROUNDS as u128;
    let t = Instant::now();
    for _ in 0..ROUNDS {
        enter(&k.ring, 0, 0, GETEVENTS).unwrap();
    }
    let getevents = t.elapsed().as_nanos() / ROUNDS as u128;
    let t = Instant::now();
    let mut etime = 0;
    for _ in 0..ROUNDS {
        if let Err(e) = enter_ts(&k.ring, 0, 4096, Duration::ZERO) {
            etime += (e.raw_os_error() == Some(libc::ETIME)) as u32;
        }
    }
    let zero_ts = t.elapsed().as_nanos() / ROUNDS as u128;
    println!(
        "    enter(0, 0, 0): {plain} ns; enter(0, 0, GETEVENTS): {getevents} ns; enter(0, 4096, \
         GETEVENTS|EXT_ARG ts=0): {zero_ts} ns ({etime} of {ROUNDS} returned -ETIME)"
    );
    for (_, p) in &k.pairs {
        (&*p).write_all(b"x").unwrap();
    }
    k.finish();
}

// ---------------------------------------------------------------------------------------------
// KV2
// ---------------------------------------------------------------------------------------------

#[test]
#[ignore = "probe: prints kernel behaviour, run by hand"]
fn kv2_skip_success_in_link_chains() {
    let _wd = watchdog("kv2", 120);
    println!(
        "KV2 (kernel {}): pair = SEND(IO_LINK|CQE_SKIP_SUCCESS, MSG_WAITALL) -> RECV(POLL_FIRST) on a \
         loopback TCP socket; CQEs in CQ order",
        kernel()
    );
    let quiet = 200 * MS;
    let hello = *b"hello";

    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (a1) send ok, reply already in the socket: {}; peer got {}",
            show(&cq, &PAIR),
            peer_text(&mut b)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &hello, &mut rbuf);
        let early = settle(&mut ring, quiet);
        let got = peer_text(&mut b);
        b.write_all(b"pong").unwrap();
        let cq = settle(&mut ring, quiet);
        println!(
            "  (a2) send ok, reply arrives later: before the reply {}, peer got {got}; after the reply {}",
            show(&early, &PAIR),
            show(&cq, &PAIR)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        shutdown_write(&a);
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        b.write_all(b"pong").unwrap();
        let late = settle(&mut ring, quiet);
        println!(
            "  (b1) send fails at issue (local SHUT_WR): {}; after the peer wrote {}",
            show(&cq, &PAIR),
            show(&late, &PAIR)
        );
    }
    {
        let (a, b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        reset_close(b, Kind::Tcp);
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (b2) send fails at issue (peer reset): {}",
            show(&cq, &PAIR)
        );
    }
    println!("  (c)  short send / full socket buffer: see KV3");
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.send_prio = BAD_IOPRIO;
        spec.push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (d1) SEND rejected at prep (invalid ioprio flag): {}; peer got {}",
            show(&cq, &PAIR),
            peer_text(&mut b)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.send_fd = -1;
        spec.push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!("  (d2) SEND on fd -1: {}", show(&cq, &PAIR));
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.recv_prio = POLL_FIRST | BAD_IOPRIO;
        spec.push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (d3) valid SEND, RECV rejected at prep (invalid ioprio flag): {}; peer got {}",
            show(&cq, &PAIR),
            peer_text(&mut b)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.recv_fd = -1;
        spec.push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (d4) valid SEND, RECV on fd -1: {}; peer got {}",
            show(&cq, &PAIR),
            peer_text(&mut b)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        let head = opcode::SendZc::new(types::Fd(a.as_raw_fd()), hello.as_ptr(), 5)
            .flags(libc::MSG_WAITALL | libc::MSG_NOSIGNAL)
            .build()
            .flags(link_skip())
            .user_data(1);
        push(&mut ring, head);
        push(
            &mut ring,
            recv(a.as_raw_fd(), &mut rbuf, POLL_FIRST).user_data(2),
        );
        let cq = settle(&mut ring, quiet);
        println!(
            "  (d5) head is SEND_ZC(IO_LINK|CQE_SKIP_SUCCESS): {}; peer got {}",
            show(&cq, &PAIR),
            peer_text(&mut b)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        shutdown_write(&b);
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (e1) send ok, peer had shut down its write side (EOF): {}; peer got {}",
            show(&cq, &PAIR),
            peer_text(&mut b)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        let spec = PairSpec::new(a.as_raw_fd());
        push(
            &mut ring,
            send(spec.send_fd, &hello, 0, spec.send_flags)
                .flags(spec.head)
                .user_data(1),
        );
        // An unmapped receive buffer (first page): it passes the range check at prep and the
        // RECV fails with EFAULT when it copies.
        let bad = opcode::Recv::new(types::Fd(a.as_raw_fd()), 4096 as *mut u8, 64)
            .ioprio(POLL_FIRST)
            .build()
            .user_data(2);
        push(&mut ring, bad);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (e2) send ok, RECV fails at issue (unmapped buffer): {}; peer got {}",
            show(&cq, &PAIR),
            peer_text(&mut b)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &hello, &mut rbuf);
        let early = settle(&mut ring, quiet);
        let got = peer_text(&mut b);
        reset_close(b, Kind::Tcp);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (e3) send ok, then the peer resets while the RECV waits: before {}, peer got {got}; after {}",
            show(&early, &PAIR),
            show(&cq, &PAIR)
        );
    }

    println!("  chains of three: m1 = SEND, m2 = SEND, m3 = RECV(POLL_FIRST); 'x' is a socket whose write side is shut down");
    // (label, m1 on x, m1 SKIP, m2 on x, m2 SKIP, m2 ioprio)
    let trios: [(&str, bool, bool, bool, bool, u16); 7] = [
        (
            "(f1) SKIP on m1 and m2, all succeed",
            false,
            true,
            false,
            true,
            0,
        ),
        (
            "(f2) SKIP on m1 and m2, m2 fails",
            false,
            true,
            true,
            true,
            0,
        ),
        (
            "(f3) SKIP on m1 and m2, m1 fails",
            true,
            true,
            false,
            true,
            0,
        ),
        (
            "(f4) SKIP on m1 and m2, m2 rejected at prep",
            false,
            true,
            false,
            true,
            BAD_IOPRIO,
        ),
        (
            "(f5) SKIP on m2 only, m2 fails",
            false,
            false,
            true,
            true,
            0,
        ),
        (
            "(f6) SKIP on m2 only, m1 fails",
            true,
            false,
            false,
            true,
            0,
        ),
        (
            "(f7) SKIP on m1 only, m2 fails",
            false,
            true,
            true,
            false,
            0,
        ),
    ];
    for (label, m1_x, m1_skip, m2_x, m2_skip, m2_prio) in trios {
        let (a, mut b) = pair(Kind::Tcp);
        let (x, _y) = pair(Kind::Tcp);
        shutdown_write(&x);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        let fd = |on_x: bool| if on_x { x.as_raw_fd() } else { a.as_raw_fd() };
        let flags = |skip: bool| {
            if skip {
                link_skip()
            } else {
                squeue::Flags::IO_LINK
            }
        };
        push(
            &mut ring,
            send(fd(m1_x), b"one", 0, libc::MSG_WAITALL)
                .flags(flags(m1_skip))
                .user_data(1),
        );
        push(
            &mut ring,
            send(fd(m2_x), b"two", m2_prio, libc::MSG_WAITALL)
                .flags(flags(m2_skip))
                .user_data(2),
        );
        push(
            &mut ring,
            recv(a.as_raw_fd(), &mut rbuf, POLL_FIRST).user_data(3),
        );
        let cq = settle(&mut ring, quiet);
        println!(
            "  {label}: {}; peer got {}",
            show(&cq, &TRIO),
            peer_text(&mut b)
        );
    }

    println!("  control: the same pair with IO_LINK only (no CQE_SKIP_SUCCESS)");
    {
        let (a, _b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        shutdown_write(&a);
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.head = squeue::Flags::IO_LINK;
        spec.push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (g1) send fails at issue (local SHUT_WR): {}",
            show(&cq, &PAIR)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.head = squeue::Flags::IO_LINK;
        spec.push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (g2) send ok, reply already in the socket: {}",
            show(&cq, &PAIR)
        );
    }
    {
        let (a, _b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.head = squeue::Flags::IO_LINK;
        spec.send_prio = BAD_IOPRIO;
        spec.push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!("  (g3) SEND rejected at prep: {}", show(&cq, &PAIR));
    }
    {
        let (a, _b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.head = squeue::Flags::IO_LINK;
        spec.recv_prio = POLL_FIRST | BAD_IOPRIO;
        spec.push(&mut ring, &hello, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (g4) valid SEND, RECV rejected at prep: {}",
            show(&cq, &PAIR)
        );
    }

    println!("  cancel (IORING_OP_ASYNC_CANCEL by user_data, its own CQE is 'cancel') while the RECV of the pair waits");
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &hello, &mut rbuf);
        let early = settle(&mut ring, quiet);
        push(&mut ring, opcode::AsyncCancel::new(1).build().user_data(9));
        let c1 = settle(&mut ring, quiet);
        push(&mut ring, opcode::AsyncCancel::new(2).build().user_data(9));
        let c2 = settle(&mut ring, quiet);
        b.write_all(b"pong").unwrap();
        let late = settle(&mut ring, quiet);
        println!(
            "  (h1) before {}; cancel(user_data of the send): {}; cancel(user_data of the recv): {}; after \
             the peer wrote {}",
            show(&early, &PAIR),
            show(&c1, &PAIR),
            show(&c2, &PAIR),
            show(&late, &PAIR)
        );
    }
}

/// On a `DEFER_TASKRUN` ring: `PAIRS` connections whose reply is already in the socket. `linked`:
/// the pair as one chain; otherwise SEND and RECV(POLL_FIRST) as two independent SQEs. Prints the
/// CQEs of the submitting enter (no `GETEVENTS`) and of each following `enter(0, 0, GETEVENTS)`.
fn kv2_defer(linked: bool) {
    const PAIRS: usize = 50;
    let ring = IoUring::builder()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .setup_taskrun_flag()
        .build(256);
    let mut ring = match ring {
        Ok(r) => r,
        Err(e) => {
            println!("    skipped: no DEFER_TASKRUN ring ({e})");
            return;
        }
    };
    let socks: Vec<(File, File)> = (0..PAIRS).map(|_| pair(Kind::Unix)).collect();
    let mut rbufs = vec![[0u8; 64]; PAIRS];
    for (i, (a, b)) in socks.iter().enumerate() {
        let mut b = b;
        b.write_all(b"pong").unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        if !linked {
            spec.head = squeue::Flags::SKIP_SUCCESS;
        }
        spec.push(&mut ring, b"hello", &mut rbufs[i]);
    }
    let n = ring.submission().len() as u32;
    assert_eq!(enter(&ring, n, 0, 0).unwrap(), n as usize);
    let first = ring.completion().len();
    let flag = ring.submission().taskrun();
    let mut per_enter = Vec::new();
    let mut total = first;
    while total < PAIRS {
        assert!(per_enter.len() < 100, "receives never completed");
        enter(&ring, 0, 0, GETEVENTS).unwrap();
        let now = ring.completion().len();
        per_enter.push(now - total);
        total = now;
    }
    let all_recv = ring
        .completion()
        .all(|c| c.user_data() == 2 && c.result() == 4);
    println!(
        "    {}: submitting enter (no GETEVENTS) {first} CQEs, SQ_TASKRUN {flag}; then enter(0, 0, GETEVENTS) x {}: CQEs per enter {per_enter:?}; all {PAIRS} are [recv: 4]: {all_recv}",
        if linked {
            "linked pair, SEND(IO_LINK|CQE_SKIP_SUCCESS)"
        } else {
            "unlinked, SEND(CQE_SKIP_SUCCESS)"
        },
        per_enter.len()
    );
}

#[test]
#[ignore = "probe: prints kernel behaviour, run by hand"]
fn kv2_pair_on_defer_taskrun() {
    let _wd = watchdog("kv2-defer", 60);
    println!(
        "KV2 on DEFER_TASKRUN (kernel {}): 50 Unix stream connections, reply already in the socket; how many deferred task-work runs until the receives complete",
        kernel()
    );
    kv2_defer(true);
    kv2_defer(false);
}

// ---------------------------------------------------------------------------------------------
// KV3
// ---------------------------------------------------------------------------------------------

const BIG: usize = 1 << 20;

fn kv3_kind(kind: Kind) {
    let quiet = 300 * MS;
    let msg = payload(0, BIG);
    println!(
        "  {kind:?} stream, SO_SNDBUF/SO_RCVBUF 4096, SEND of {BIG} bytes, the peer does not read at first"
    );
    {
        let (a, mut b) = small_pair(kind);
        let mut ring = IoUring::new(8).unwrap();
        push(&mut ring, send(a.as_raw_fd(), &msg, 0, 0).user_data(1));
        let cq = settle(&mut ring, quiet);
        let mut got = Vec::new();
        pump(&mut ring, &mut b, &mut got, BIG, quiet);
        let late = settle(&mut ring, quiet);
        println!(
            "    (1) SEND, no MSG_WAITALL: while the peer is idle {}; then the peer read {} bytes; later {}",
            show(&cq, &PAIR),
            got.len(),
            show(&late, &PAIR)
        );
    }
    {
        let (a, mut b) = small_pair(kind);
        let mut ring = IoUring::new(8).unwrap();
        push(
            &mut ring,
            send(a.as_raw_fd(), &msg, 0, libc::MSG_WAITALL).user_data(1),
        );
        let cq = settle(&mut ring, quiet);
        let mut got = Vec::new();
        pump(&mut ring, &mut b, &mut got, BIG, quiet);
        let late = settle(&mut ring, quiet);
        println!(
            "    (2) SEND, MSG_WAITALL: while the peer is idle {}; then the peer read {} bytes ({}); after that {}",
            show(&cq, &PAIR),
            got.len(),
            compare(&got, &msg),
            show(&late, &PAIR)
        );
    }
    {
        let (a, mut b) = small_pair(kind);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.send_flags = 0;
        spec.push(&mut ring, &msg, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        let mut got = Vec::new();
        pump(&mut ring, &mut b, &mut got, BIG, quiet);
        let mid = settle(&mut ring, quiet);
        b.write_all(b"pong").unwrap();
        let late = settle(&mut ring, quiet);
        println!(
            "    (3) pair, SEND(LINK|SKIP) without MSG_WAITALL: while the peer is idle {}; the peer could read \
             {} of {BIG} bytes, then {}; after the peer replied {}",
            show(&cq, &PAIR),
            got.len(),
            show(&mid, &PAIR),
            show(&late, &PAIR)
        );
    }
    {
        let (a, mut b) = small_pair(kind);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &msg, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        let mut got = Vec::new();
        pump(&mut ring, &mut b, &mut got, BIG, quiet);
        let mid = settle(&mut ring, quiet);
        b.write_all(b"pong").unwrap();
        let late = settle(&mut ring, quiet);
        println!(
            "    (4) pair, SEND(LINK|SKIP) with MSG_WAITALL: while the peer is idle {}; the peer read {} bytes \
             ({}), then {}; after the peer replied {}",
            show(&cq, &PAIR),
            got.len(),
            compare(&got, &msg),
            show(&mid, &PAIR),
            show(&late, &PAIR)
        );
    }
    {
        let (a, mut b) = small_pair(kind);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &msg, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        let mut got = Vec::new();
        pump(&mut ring, &mut b, &mut got, 100 * 1024, quiet);
        reset_close(b, kind);
        let late = settle(&mut ring, quiet);
        println!(
            "    (5) pair with MSG_WAITALL, the peer reads {} bytes and then closes (TCP: reset): before {}; \
             after {}",
            got.len(),
            show(&cq, &PAIR),
            show(&late, &PAIR)
        );
    }
    {
        let (a, mut b) = small_pair(kind);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        PairSpec::new(a.as_raw_fd()).push(&mut ring, &msg, &mut rbuf);
        let cq = settle(&mut ring, quiet);
        push(&mut ring, opcode::AsyncCancel::new(2).build().user_data(9));
        let c2 = settle(&mut ring, quiet);
        push(&mut ring, opcode::AsyncCancel::new(1).build().user_data(9));
        let c1 = settle(&mut ring, quiet);
        let mut got = Vec::new();
        pump(&mut ring, &mut b, &mut got, BIG, quiet);
        b.write_all(b"pong").unwrap();
        let late = settle(&mut ring, quiet);
        println!(
            "    (6) pair with MSG_WAITALL, cancelled while the send waits for socket space: before {}; \
             cancel(user_data of the recv): {}; cancel(user_data of the send): {}; the peer could read {} \
             bytes; after the peer replied {}",
            show(&cq, &PAIR),
            show(&c2, &PAIR),
            show(&c1, &PAIR),
            got.len(),
            show(&late, &PAIR)
        );
    }
}

/// The pair with a send of `len` bytes that needs many retries: which context runs the RECV
/// once the send is complete? Run on a fresh thread, so that the thread has no io-wq workers
/// before.
fn kv3_retries(len: usize, recv_prio: u16) {
    std::thread::spawn(move || {
        let quiet = 300 * MS;
        let msg = payload(0, len);
        let (a, mut b) = small_pair(Kind::Tcp);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        let mut spec = PairSpec::new(a.as_raw_fd());
        spec.recv_prio = recv_prio;
        spec.push(&mut ring, &msg, &mut rbuf);
        let early = settle(&mut ring, 100 * MS);
        let mut got = Vec::new();
        pump(&mut ring, &mut b, &mut got, len, quiet);
        let mid = settle(&mut ring, quiet);
        let workers = iowq_workers();
        b.write_all(b"pong").unwrap();
        let late = settle(&mut ring, quiet);
        println!(
            "    send of {len:>7} bytes, RECV ioprio {recv_prio}: before {}; the peer read {} bytes ({}), then {}, io-wq workers {workers}; after the peer replied {}",
            show(&early, &PAIR),
            got.len(),
            compare(&got, &msg),
            show(&mid, &PAIR),
            show(&late, &PAIR)
        );
    })
    .join()
    .unwrap();
}

#[test]
#[ignore = "probe: prints kernel behaviour, run by hand"]
fn kv3_stream_send_waitall() {
    let _wd = watchdog("kv3", 180);
    println!(
        "KV3 (kernel {}): stream SEND larger than the socket buffer; pair = SEND(IO_LINK|CQE_SKIP_SUCCESS) -> \
         RECV(POLL_FIRST)",
        kernel()
    );
    kv3_kind(Kind::Tcp);
    kv3_kind(Kind::Unix);
    println!(
        "  (7) Tcp pair with MSG_WAITALL by send size (every retry of the send re-arms its poll; RECV ioprio \
         1 = POLL_FIRST)"
    );
    for len in [16 << 10, 128 << 10, 256 << 10, 512 << 10, 768 << 10, BIG] {
        kv3_retries(len, POLL_FIRST);
    }
    kv3_retries(BIG, 0);
}

// ---------------------------------------------------------------------------------------------
// KV4
// ---------------------------------------------------------------------------------------------

const PART: usize = 300 * 1024;

/// Three payload buffers, a heap iovec array over them and a heap `msghdr` over that.
struct Gather {
    bufs: Vec<Vec<u8>>,
    /// Valid memory with other content: what the peer would receive if the kernel read the
    /// iovec array again after it was overwritten.
    wrong: Vec<u8>,
    iov: Box<[libc::iovec; 3]>,
    hdr: Box<libc::msghdr>,
}

impl Gather {
    fn new(part: usize) -> Self {
        let bufs: Vec<Vec<u8>> = (0..3).map(|k| payload(k, part)).collect();
        let mut iov: Box<[libc::iovec; 3]> = Box::new(std::array::from_fn(|k| libc::iovec {
            iov_base: bufs[k].as_ptr() as *mut libc::c_void,
            iov_len: part,
        }));
        let mut hdr: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
        hdr.msg_iov = iov.as_mut_ptr();
        hdr.msg_iovlen = 3;
        Gather {
            wrong: vec![0xEE; part],
            bufs,
            iov,
            hdr,
        }
    }

    fn expected(&self) -> Vec<u8> {
        self.bufs.concat()
    }

    /// Point every iovec at the wrong buffer.
    fn trash_iov(&mut self) {
        for v in self.iov.iter_mut() {
            *v = libc::iovec {
                iov_base: self.wrong.as_ptr() as *mut libc::c_void,
                iov_len: self.wrong.len(),
            };
        }
        std::hint::black_box(&mut self.iov);
    }

    /// Fill the `msghdr` with values that would make a `sendmsg` fail or send something else.
    fn trash_hdr(&mut self) {
        self.hdr.msg_iov = usize::MAX as *mut libc::iovec;
        self.hdr.msg_iovlen = 1000;
        self.hdr.msg_name = usize::MAX as *mut libc::c_void;
        self.hdr.msg_namelen = 77;
        self.hdr.msg_control = usize::MAX as *mut libc::c_void;
        self.hdr.msg_controllen = 4096;
        std::hint::black_box(&mut self.hdr);
    }
}

fn kv4_case(label: &str, kind: Kind, prefill: bool, poll_first: bool) {
    let quiet = 300 * MS;
    let (a, mut b) = small_pair(kind);
    let mut g = Gather::new(PART);
    let want = g.expected();
    let mut ring = IoUring::new(8).unwrap();
    let pre = if prefill { fill_socket(&a) } else { 0 };
    let sqe = opcode::SendMsg::new(types::Fd(a.as_raw_fd()), &*g.hdr as *const libc::msghdr)
        .ioprio(if poll_first { POLL_FIRST } else { 0 })
        .flags((libc::MSG_WAITALL | libc::MSG_NOSIGNAL) as u32)
        .build()
        .user_data(1);
    push(&mut ring, sqe);
    submit_only(&mut ring);
    // io_uring_enter has returned and the send is not complete. Overwrite both structures and
    // free them; only the payload buffers stay.
    g.trash_iov();
    g.trash_hdr();
    let Gather {
        bufs,
        wrong,
        iov,
        hdr,
        ..
    } = g;
    drop(iov);
    drop(hdr);
    let early = settle(&mut ring, 100 * MS);
    let mut got = Vec::new();
    pump(&mut ring, &mut b, &mut got, pre + want.len(), quiet);
    let cq = settle(&mut ring, quiet);
    let junk_ok = got.len() >= pre && got[..pre].iter().all(|b| *b == 0x55);
    println!(
        "    {label} ({kind:?}, {pre} bytes already queued): before the peer read {}; the peer read {} bytes, \
         payload {}{}; then {}",
        show(&early, &[(1, "sendmsg")]),
        got.len(),
        compare(&got[pre.min(got.len())..], &want),
        if junk_ok { "" } else { " (prefill damaged)" },
        show(&cq, &[(1, "sendmsg")])
    );
    drop(ring);
    drop((bufs, wrong));
}

#[test]
#[ignore = "probe: prints kernel behaviour, run by hand"]
fn kv4_sendmsg_copies_msghdr_at_enter() {
    let _wd = watchdog("kv4", 120);
    println!(
        "KV4 (kernel {}): SENDMSG(MSG_WAITALL) of 3 x {PART} bytes that cannot complete inside the enter; \
         after io_uring_enter returned, the msghdr and the iovec array are overwritten (iovecs point at a \
         buffer of 0xEE, msghdr fields at invalid addresses) and freed; then the peer reads",
        kernel()
    );
    for kind in [Kind::Tcp, Kind::Unix] {
        kv4_case(
            "(A) first attempt inside the enter is partial",
            kind,
            false,
            false,
        );
        kv4_case(
            "(B) socket buffer full, first attempt gets EAGAIN",
            kind,
            true,
            false,
        );
        kv4_case(
            "(C) socket buffer full, POLL_FIRST: no attempt inside the enter",
            kind,
            true,
            true,
        );
    }

    println!("  when is the msghdr read: between the push of the SQE and the enter");
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut g = Gather::new(4);
        let mut ring = IoUring::new(8).unwrap();
        let sqe = opcode::SendMsg::new(types::Fd(a.as_raw_fd()), &*g.hdr as *const libc::msghdr)
            .flags(libc::MSG_NOSIGNAL as u32)
            .build()
            .user_data(1);
        push(&mut ring, sqe);
        g.trash_hdr();
        let cq = settle(&mut ring, 100 * MS);
        println!(
            "    (D) valid when the SQE is pushed, overwritten before the enter: {}; peer got {} bytes",
            show(&cq, &[(1, "sendmsg")]),
            peer_available(&mut b).len()
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let mut g = Gather::new(4);
        let want = g.expected();
        let iov_ptr = g.iov.as_mut_ptr();
        g.trash_hdr();
        let mut ring = IoUring::new(8).unwrap();
        let sqe = opcode::SendMsg::new(types::Fd(a.as_raw_fd()), &*g.hdr as *const libc::msghdr)
            .flags(libc::MSG_NOSIGNAL as u32)
            .build()
            .user_data(1);
        push(&mut ring, sqe);
        *g.hdr = unsafe { std::mem::zeroed() };
        g.hdr.msg_iov = iov_ptr;
        g.hdr.msg_iovlen = 3;
        let cq = settle(&mut ring, 100 * MS);
        println!(
            "    (E) invalid when the SQE is pushed, filled in before the enter: {}; peer payload {}",
            show(&cq, &[(1, "sendmsg")]),
            compare(&peer_available(&mut b), &want)
        );
    }

    println!("  SQPOLL ring: the SQ thread prepares the request, not io_uring_enter");
    {
        const ROUNDS: usize = 500;
        let ring = IoUring::builder().setup_sqpoll(2000).build(8);
        let mut ring = match ring {
            Ok(r) => r,
            Err(e) => {
                println!("    (F) skipped: no SQPOLL ring ({e})");
                return;
            }
        };
        let (a, mut b) = pair(Kind::Tcp);
        let mut g = Gather::new(4);
        let iov_ptr = g.iov.as_mut_ptr();
        let (mut sent, mut efault, mut other) = (0, 0, 0);
        for _ in 0..ROUNDS {
            g.hdr.msg_iov = iov_ptr;
            let sqe =
                opcode::SendMsg::new(types::Fd(a.as_raw_fd()), &*g.hdr as *const libc::msghdr)
                    .flags(libc::MSG_NOSIGNAL as u32)
                    .build()
                    .user_data(1);
            push(&mut ring, sqe);
            // Returns once the SQ thread is awake; with an awake thread it makes no syscall.
            ring.submit().unwrap();
            g.hdr.msg_iov = usize::MAX as *mut libc::iovec;
            std::hint::black_box(&mut g.hdr);
            ring.submit_and_wait(1).unwrap();
            match reap(&mut ring).first().map(|c| c.res) {
                Some(12) => sent += 1,
                Some(res) if res == -libc::EFAULT => efault += 1,
                _ => other += 1,
            }
        }
        println!(
            "    (F) {ROUNDS} x: push, submit(), then msg_iov overwritten at once: {sent} sent, {efault} failed \
             with -EFAULT, {other} other; peer got {} bytes",
            peer_available(&mut b).len()
        );
    }
}

// ---------------------------------------------------------------------------------------------
// KV5
// ---------------------------------------------------------------------------------------------

fn header_define(needle: &str) -> Vec<String> {
    let rel = kernel();
    [
        format!("/usr/src/linux-headers-{rel}/include/uapi/linux/io_uring.h"),
        "/usr/include/linux/io_uring.h".to_owned(),
    ]
    .iter()
    .map(|path| match std::fs::read_to_string(path) {
        Ok(text) => match text
            .lines()
            .find(|l| l.contains("#define") && l.contains(needle))
        {
            Some(l) => format!(
                "{path}: {}",
                l.split_whitespace().collect::<Vec<_>>().join(" ")
            ),
            None => format!("{path}: not defined"),
        },
        Err(e) => format!("{path}: {e}"),
    })
    .collect()
}

/// `SEND` with `IORING_SEND_VECTORIZED`: `addr` is the iovec array, `len` the number of iovecs.
fn send_vectored(fd: RawFd, iov: &[libc::iovec], msg_flags: i32) -> squeue::Entry {
    opcode::Send::new(types::Fd(fd), iov.as_ptr().cast::<u8>(), iov.len() as u32)
        .ioprio(SEND_VECTORIZED)
        .flags(msg_flags | libc::MSG_NOSIGNAL)
        .build()
}

#[test]
#[ignore = "probe: prints kernel behaviour, run by hand"]
fn kv5_send_vectorized() {
    let _wd = watchdog("kv5", 120);
    println!(
        "KV5 (kernel {}): IORING_OP_SEND with IORING_SEND_VECTORIZED (ioprio bit {SEND_VECTORIZED:#x})",
        kernel()
    );
    for line in header_define("IORING_SEND_VECTORIZED") {
        println!("  header {line}");
    }
    let quiet = 300 * MS;
    let names = [(1, "send"), (2, "recv")];

    for kind in [Kind::Tcp, Kind::Unix] {
        let (a, mut b) = pair(kind);
        let g = Gather::new(5);
        let mut ring = IoUring::new(8).unwrap();
        push(
            &mut ring,
            send_vectored(a.as_raw_fd(), &g.iov[..], 0).user_data(1),
        );
        let cq = settle(&mut ring, quiet);
        println!(
            "  (A) {kind:?}, 3 iovecs of 5 bytes: {}; peer payload {}",
            show(&cq, &names),
            compare(&peer_available(&mut b), &g.expected())
        );
        if cq.first().map(|c| c.res) == Some(-libc::EINVAL) {
            println!("skipped: this kernel rejects IORING_SEND_VECTORIZED");
            return;
        }
    }
    {
        let (a, mut b) = small_pair(Kind::Tcp);
        let mut g = Gather::new(PART);
        let want = g.expected();
        let mut ring = IoUring::new(8).unwrap();
        let pre = fill_socket(&a);
        push(
            &mut ring,
            send_vectored(a.as_raw_fd(), &g.iov[..], libc::MSG_WAITALL).user_data(1),
        );
        submit_only(&mut ring);
        g.trash_iov();
        let early = settle(&mut ring, 100 * MS);
        let mut got = Vec::new();
        pump(&mut ring, &mut b, &mut got, pre + want.len(), quiet);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (B) Tcp, 3 x {PART} bytes, MSG_WAITALL, socket buffer full, iovec array overwritten after the \
             enter: before the peer read {}; the peer read {} bytes, payload {}; then {}",
            show(&early, &names),
            got.len(),
            compare(&got[pre.min(got.len())..], &want),
            show(&cq, &names)
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let g = Gather::new(5);
        let mut rbuf = [0u8; 64];
        let mut ring = IoUring::new(8).unwrap();
        b.write_all(b"pong").unwrap();
        push(
            &mut ring,
            send_vectored(a.as_raw_fd(), &g.iov[..], libc::MSG_WAITALL)
                .flags(link_skip())
                .user_data(1),
        );
        push(
            &mut ring,
            recv(a.as_raw_fd(), &mut rbuf, POLL_FIRST).user_data(2),
        );
        let cq = settle(&mut ring, quiet);
        println!(
            "  (C) Tcp, vectored SEND(LINK|SKIP, MSG_WAITALL) of 3 iovecs of 5 bytes -> RECV(POLL_FIRST), \
             reply already in the socket: {}; peer payload {}",
            show(&cq, &names),
            compare(&peer_available(&mut b), &g.expected())
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let g = Gather::new(5);
        let mut ring = IoUring::new(8).unwrap();
        let sqe = opcode::SendZc::new(types::Fd(a.as_raw_fd()), g.iov.as_ptr().cast::<u8>(), 3)
            .zc_flags(SEND_VECTORIZED)
            .flags(libc::MSG_NOSIGNAL)
            .build()
            .user_data(1);
        push(&mut ring, sqe);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (D) Tcp, SEND_ZC with the flag, 3 iovecs of 5 bytes: {}; peer payload {}",
            show(&cq, &[(1, "send_zc")]),
            compare(&peer_available(&mut b), &g.expected())
        );
    }
    {
        let (a, mut b) = pair(Kind::Tcp);
        let g = Gather::new(5);
        let mut ring = IoUring::new(8).unwrap();
        let sqe = opcode::SendMsg::new(types::Fd(a.as_raw_fd()), &*g.hdr as *const libc::msghdr)
            .ioprio(SEND_VECTORIZED)
            .flags(libc::MSG_NOSIGNAL as u32)
            .build()
            .user_data(1);
        push(&mut ring, sqe);
        let cq = settle(&mut ring, quiet);
        println!(
            "  (E) Tcp, SENDMSG with the flag set (it has no meaning there): {}; peer payload {}",
            show(&cq, &[(1, "sendmsg")]),
            compare(&peer_available(&mut b), &g.expected())
        );
    }
}
