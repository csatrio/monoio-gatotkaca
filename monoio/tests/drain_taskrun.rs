//! `RuntimeBuilder::drain_taskrun` and `uring::set_min_wait`.
//!
//! Kernel behaviour the tests rely on (Linux 6.13+, `docs/performance-improvement/round3/`
//! `r3-probe.md`, KV1): a DEFER_TASKRUN ring runs completions only inside an `io_uring_enter`
//! with GETEVENTS, at most max(20, `min_complete`) task-work items per run, and raises
//! `IORING_SQ_TASKRUN` again when items are left. Older kernels run everything in one enter:
//! there the drain has nothing to do and the tests only check that nothing breaks.

use std::{
    cell::Cell,
    os::fd::AsRawFd,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use monoio::{
    io::AsyncReadRent,
    io_uring::{opcode, squeue, types},
    net::UnixStream,
    time::TimeDriver,
    Driver, IoUringDriver, Runtime, RuntimeBuilder,
};

type Builder = RuntimeBuilder<IoUringDriver>;

/// Receives made ready at once. More than a drain of 4 can fetch 20 at a time (also when the
/// first enter submits something and gets two runs, as with the `sync` feature).
const BURST: usize = 200;

/// First kernel whose DEFER_TASKRUN rings limit the task work run per enter.
const LOCAL_TW_LIMIT_KERNEL: (u32, u32) = (6, 13);

/// `(major, minor)` of the running kernel.
fn kernel_version() -> (u32, u32) {
    let mut uname: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut uname) } != 0 {
        return (0, 0);
    }
    let release = unsafe { std::ffi::CStr::from_ptr(uname.release.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    let mut it = release.split(|c: char| !c.is_ascii_digit());
    let major: u32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: u32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (major, minor)
}

fn limited_kernel() -> bool {
    kernel_version() >= LOCAL_TW_LIMIT_KERNEL
}

/// Builds the runtime, or `None` (with a note) if the kernel refuses the ring flags.
fn build(what: &str, rb: Builder) -> Option<Runtime<IoUringDriver>> {
    match rb.build() {
        Ok(rt) => Some(rt),
        Err(e) => {
            eprintln!("skipped: {what} unsupported: {e}");
            None
        }
    }
}

/// Aborts the test process if `f` does not return in `secs` seconds: a drain enter that
/// blocked would otherwise hang the test run without a message.
fn with_watchdog<R>(what: &'static str, secs: u64, f: impl FnOnce() -> R) -> R {
    let done = Arc::new(AtomicBool::new(false));
    let flag = done.clone();
    let dog = std::thread::spawn(move || {
        let end = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < end {
            if flag.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        eprintln!("{what}: still running after {secs} s, a park must have blocked");
        std::process::abort();
    });
    let r = f();
    done.store(true, Ordering::Release);
    dog.join().unwrap();
    r
}

async fn nop() {
    let (cqe, _) = unsafe { monoio::uring::submit(opcode::Nop::new().build(), ()) }
        .unwrap()
        .await;
    assert_eq!(cqe.res(), 0);
}

/// What one burst showed.
struct Outcome {
    /// Receives that had completed when the main future ran again after its first wait: what
    /// one park delivered to the tasks.
    first_park: usize,
    /// How long that took, from the last write.
    first_park_took: Duration,
    /// Counters from the writes to the last completion.
    #[cfg(feature = "metrics")]
    stats: monoio::DriverStats,
}

/// `BURST` receives are armed in the kernel (no data yet), then all made ready by plain
/// `write(2)` calls from the runtime thread, and only then the runtime parks (the technique of
/// `enter_left_taskrun_counts_leftover_task_work` in `driver_stats.rs`). `before_park` runs
/// after the writes, `after_first_park` when the main future runs again.
async fn ready_burst(before_park: impl FnOnce(), after_first_park: impl FnOnce()) -> Outcome {
    let started = Rc::new(Cell::new(0_usize));
    let done = Rc::new(Cell::new(0_usize));
    let mut writers = Vec::new();
    let mut readers = Vec::new();
    for _ in 0..BURST {
        let (mut rd, wr) = UnixStream::pair().unwrap();
        writers.push(wr);
        let (started, done) = (started.clone(), done.clone());
        readers.push(monoio::spawn(async move {
            started.set(started.get() + 1);
            // Pushes the receive in this poll.
            let (res, buf) = rd.read(vec![0u8; 8]).await;
            assert_eq!(res.unwrap(), 1);
            assert_eq!(buf, [b'x']);
            done.set(done.get() + 1);
        }));
    }
    // Round trips through the ring until every task pushed its receive, then two more: an
    // enter submits the whole SQ, so after them the kernel has all receives. None has data,
    // so the kernel arms a poll for each.
    for _ in 0..64 {
        if started.get() == BURST {
            break;
        }
        nop().await;
    }
    assert_eq!(started.get(), BURST, "the reader tasks did not run");
    nop().await;
    nop().await;
    assert_eq!(done.get(), 0);

    #[cfg(feature = "metrics")]
    monoio::reset_driver_stats();
    for wr in &writers {
        let n = unsafe { libc::write(wr.as_raw_fd(), b"x".as_ptr().cast(), 1) };
        assert_eq!(n, 1);
    }
    before_park();
    let since = Instant::now();
    // The first write made the first reader's receive ready: its completion is in the first
    // batch the kernel runs. This await parks once; the main future is polled again after the
    // tasks woken by that park ran.
    let mut readers = readers.into_iter();
    readers.next().unwrap().await;
    let first_park = done.get();
    let first_park_took = since.elapsed();
    after_first_park();
    for reader in readers {
        reader.await;
    }
    assert_eq!(done.get(), BURST);
    Outcome {
        first_park,
        first_park_took,
        #[cfg(feature = "metrics")]
        stats: monoio::driver_stats(),
    }
}

fn burst_on(what: &'static str, rb: Builder) -> Option<Outcome> {
    let mut rt = build(what, rb)?;
    let out = with_watchdog(what, 60, || rt.block_on(ready_burst(|| (), || ())));
    #[cfg(feature = "metrics")]
    eprintln!(
        "{what}: first park delivered {} of {BURST}; enter_wait {}, enter_left_taskrun {}, \
         drain_enters {}, cqes_per_tick {:?}",
        out.first_park,
        out.stats.enter_wait,
        out.stats.enter_left_taskrun,
        out.stats.drain_enters,
        out.stats.cqes_per_tick
    );
    #[cfg(not(feature = "metrics"))]
    eprintln!("{what}: first park delivered {} of {BURST}", out.first_park);
    Some(out)
}

// (a) With the drain, one park delivers more of the ready receives to the tasks.
#[test]
fn drain_delivers_more_per_park() {
    let Some(plain) = burst_on("defer", Builder::new().defer_taskrun()) else {
        return; // DEFER_TASKRUN needs Linux 6.1.
    };
    let a1 = burst_on(
        "defer, drain 1",
        Builder::new().defer_taskrun().drain_taskrun(1),
    )
    .unwrap();
    let a4 = burst_on(
        "defer, drain 4",
        Builder::new().defer_taskrun().drain_taskrun(4),
    )
    .unwrap();

    // Never less than without it, on any kernel.
    for (name, out) in [("a1", &a1), ("a4", &a4)] {
        assert!(out.first_park >= plain.first_park, "{name}");
    }
    #[cfg(feature = "metrics")]
    assert_eq!(plain.stats.drain_enters, 0);

    if !limited_kernel() {
        eprintln!("not asserted: this kernel runs all task work in one enter");
        return;
    }
    // 20 per run: the plain park delivers one or two runs, each drain enter one more.
    assert!(
        plain.first_park < a1.first_park,
        "one drain enter adds a run"
    );
    assert!(a1.first_park < a4.first_park, "four add more");
    assert!(a4.first_park < BURST, "but the drain is bounded");

    #[cfg(feature = "metrics")]
    {
        // Fewer parks (rounds) for the same completions.
        assert!(a4.stats.enter_wait < plain.stats.enter_wait);
        assert!(a1.stats.drain_enters >= 1);
        // The first park alone makes 4; later parks drain what is left.
        assert!(a4.stats.drain_enters >= 4, "{:?}", a4.stats.drain_enters);
        for out in [&a1, &a4] {
            let s = &out.stats;
            // Drain enters are not waits, and there is at most `max` of them per wait.
            assert!(s.drain_enters <= s.enter_submit, "{s:?}");
            assert!(s.drain_enters <= 4 * s.enter_wait, "{s:?}");
            assert!(s.enter_left_taskrun >= 1, "{s:?}");
            assert!(s.enter_left_taskrun <= s.enter_wait, "{s:?}");
            assert!(s.drain_enters >= s.enter_left_taskrun, "{s:?}");
        }
    }
}

// (b) Off by default, off with `max = 0`, and no effect on rings that are not DEFER_TASKRUN
// with the TASKRUN flag.
#[test]
fn drain_is_off_where_it_does_not_apply() {
    let check = |what: &'static str, with: Builder, without: Builder| {
        let (Some(with), Some(without)) = (burst_on(what, with), burst_on(what, without)) else {
            return;
        };
        assert_eq!(with.first_park, without.first_park, "{what}");
        #[cfg(feature = "metrics")]
        {
            assert_eq!(with.stats.drain_enters, 0, "{what}");
            assert_eq!(with.stats.enter_wait, without.stats.enter_wait, "{what}");
            assert_eq!(
                with.stats.enter_submit, without.stats.enter_submit,
                "{what}"
            );
            assert_eq!(
                with.stats.cqes_per_tick, without.stats.cqes_per_tick,
                "{what}"
            );
        }
    };
    check(
        "defer, drain 0",
        Builder::new().defer_taskrun().drain_taskrun(0),
        Builder::new().defer_taskrun(),
    );
    // The later call wins, also when it turns the drain off.
    check(
        "defer, drain 4 then 0",
        Builder::new()
            .defer_taskrun()
            .drain_taskrun(4)
            .drain_taskrun(0),
        Builder::new().defer_taskrun(),
    );
    check(
        "default ring, drain 4",
        Builder::new().drain_taskrun(4),
        Builder::new(),
    );
    check(
        "coop_taskrun, drain 4",
        Builder::new().coop_taskrun().drain_taskrun(4),
        Builder::new().coop_taskrun(),
    );
    check(
        "coop_taskrun + taskrun_flag, drain 4",
        Builder::new()
            .coop_taskrun()
            .taskrun_flag()
            .drain_taskrun(4),
        Builder::new().coop_taskrun().taskrun_flag(),
    );
}

/// How many of `fds` still have a byte to read.
fn unread(fds: &[std::os::unix::net::UnixStream]) -> usize {
    fds.iter()
        .filter(|s| {
            let mut b = 0_u8;
            let n = unsafe {
                libc::recv(
                    s.as_raw_fd(),
                    (&mut b as *mut u8).cast(),
                    1,
                    libc::MSG_PEEK | libc::MSG_DONTWAIT,
                )
            };
            n == 1
        })
        .count()
}

/// Arms `n` receives with `CQE_SKIP_SUCCESS` on socket pairs of their own (pushed, not yet
/// submitted). Each becomes one task-work item that posts no CQE when its peer is written to.
/// Returns the receiving and the writing ends. Call it inside the runtime.
fn arm_silent(
    n: usize,
) -> (
    Vec<std::os::unix::net::UnixStream>,
    Vec<std::os::unix::net::UnixStream>,
) {
    let (mut rds, mut wrs) = (Vec::new(), Vec::new());
    // The kernel writes here until the last receive is done: never freed.
    let bufs: &'static mut [[u8; 8]] = Box::leak(vec![[0_u8; 8]; n].into());
    for (i, buf) in bufs.iter_mut().enumerate() {
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        let sqe = opcode::Recv::new(types::Fd(a.as_raw_fd()), buf.as_mut_ptr(), 8)
            .build()
            .flags(squeue::Flags::SKIP_SUCCESS)
            .user_data(monoio::uring::user_data(i as u64));
        unsafe {
            monoio::uring::with_ring(|ring| ring.submission().push(&sqe)).unwrap();
        }
        rds.push(a);
        wrs.push(b);
    }
    (rds, wrs)
}

fn write_byte(fd: &impl AsRawFd) {
    let n = unsafe { libc::write(fd.as_raw_fd(), b"x".as_ptr().cast(), 1) };
    assert_eq!(n, 1);
}

/// Round trips until the kernel ran all the silent receives (before their sockets close).
async fn finish_silent(rds: &[std::os::unix::net::UnixStream]) {
    for _ in 0..64 {
        if unread(rds) == 0 {
            break;
        }
        nop().await;
    }
    assert_eq!(unread(rds), 0);
}

/// One park on a DEFER_TASKRUN ring with 64 pending task-work items that post **no** CQE
/// (receives with `CQE_SKIP_SUCCESS`) behind one that does. Returns how many of the 64 the
/// park ran, and how long the park took.
///
/// The waiting enter returns with the one CQE and leaves the flag set. Every drain enter after
/// it then runs task work without getting a CQE for it: the case in which a drain enter that
/// waited for completions would sleep.
fn silent_burst(what: &'static str, rb: Builder) -> Option<(usize, Duration)> {
    const SILENT: usize = 64;
    let mut rt = build(what, rb)?;
    let out = with_watchdog(what, 30, || {
        rt.block_on(async {
            let (mut rd, wr) = UnixStream::pair().unwrap();
            let reader = monoio::spawn(async move {
                let (res, _) = rd.read(vec![0u8; 8]).await;
                assert_eq!(res.unwrap(), 1);
            });

            let (silent_rd, silent_wr) = arm_silent(SILENT);
            // The reader task runs and an enter submits everything.
            for _ in 0..4 {
                nop().await;
            }

            // The reader's completion first, so that the waiting enter gets its CQE in the
            // first run and returns.
            write_byte(&wr);
            silent_wr.iter().for_each(write_byte);
            let since = Instant::now();
            reader.await;
            let took = since.elapsed();
            let ran = SILENT - unread(&silent_rd);

            finish_silent(&silent_rd).await;
            (ran, took)
        })
    });
    eprintln!(
        "{what}: the park ran {} of {SILENT} silent items in {:?}",
        out.0, out.1
    );
    Some(out)
}

// (c) A drain enter never waits, also when the task work it runs posts no completion, and an
// enter that brought no completion ends the drain (the flag may be stale then, and there is
// nothing new for the tasks).
#[test]
fn drain_enter_does_not_wait_for_completions() {
    let Some((plain, _)) = silent_burst("defer", Builder::new().defer_taskrun()) else {
        return;
    };
    let defer = || Builder::new().defer_taskrun();
    let (a1, _) = silent_burst("defer, drain 1", defer().drain_taskrun(1)).unwrap();
    let (a8, a_took) = silent_burst("defer, drain 8", defer().drain_taskrun(8)).unwrap();
    // Nothing else can end such a park: a blocked enter is stopped by the watchdog. A park
    // that slept would also show here.
    assert!(a_took < Duration::from_secs(2), "{a_took:?}");
    assert!(a1 >= plain);
    // One enter without a CQE and the drain is over, whatever the bound.
    assert_eq!(a1, a8);
    if limited_kernel() {
        // The drain enter did run: more of the silent items are done after one park.
        assert!(a1 > plain, "drain: {a1}, plain: {plain}");
    }
}

/// `op_ticks_by_opcode` of the one receive in this scenario: 50 silent task-work items are
/// pending, then the park's enter submits a NOP (its CQE ends the wait) and a
/// `RECV(POLL_FIRST)` on a socket that has data. The receive becomes task-work item 51; the
/// enter runs 40.
#[cfg(feature = "metrics")]
fn late_receive_ticks(what: &'static str, rb: Builder) -> Option<[u64; 4]> {
    use monoio::net::MsgFlags;

    let mut rt = build(what, rb)?;
    let ticks = with_watchdog(what, 30, || {
        rt.block_on(async {
            let (silent_rd, silent_wr) = arm_silent(50);
            let (rd, wr) = UnixStream::pair().unwrap();
            for _ in 0..4 {
                nop().await;
            }
            write_byte(&wr);
            silent_wr.iter().for_each(write_byte);

            monoio::reset_driver_stats();
            // Both are pushed here and submitted by the park's enter.
            let recv = rd.recv_poll_first_op(vec![0u8; 8], MsgFlags::EMPTY);
            let nop = unsafe { monoio::uring::submit(opcode::Nop::new().build(), ()) }.unwrap();
            let (res, _) = recv.await;
            assert_eq!(res.unwrap(), 1);
            let s = monoio::driver_stats();
            nop.await;
            finish_silent(&silent_rd).await;
            s.op_ticks_by_opcode[opcode::Recv::CODE as usize]
        })
    });
    eprintln!("{what}: op ticks of the late receive {ticks:?}");
    Some(ticks)
}

// The D1 measure: a receive submitted by the park's enter, whose completion that enter left
// behind, reaches its task in the same round ("same tick") with the drain, one round later
// without.
#[cfg(feature = "metrics")]
#[test]
fn drained_completion_counts_for_the_tick_of_its_park() {
    let Some(plain) = late_receive_ticks("defer", Builder::new().defer_taskrun()) else {
        return;
    };
    let a = late_receive_ticks(
        "defer, drain 1",
        Builder::new().defer_taskrun().drain_taskrun(1),
    )
    .unwrap();
    assert_eq!(a, [1, 0, 0, 0]);
    if limited_kernel() {
        assert_eq!(plain, [0, 1, 0, 0]);
    } else {
        assert_eq!(plain, [1, 0, 0, 0]);
    }
}

// (c) Parks with a timeout return on time, with and without traffic from another thread.
#[test]
fn parks_with_a_timeout_return_on_time() {
    type TimedBuilder = RuntimeBuilder<TimeDriver<IoUringDriver>>;
    let variants: [(&'static str, TimedBuilder); 1] = [(
        "defer, drain 4, timer",
        Builder::new()
            .defer_taskrun()
            .drain_taskrun(4)
            .enable_timer(),
    )];
    for (what, builder) in variants {
        let mut rt = match builder.build() {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("skipped: {what} unsupported: {e}");
                return;
            }
        };
        with_watchdog(what, 60, || {
            // Nothing pending at all: the park times out with an empty CQ.
            rt.block_on(async {
                for _ in 0..5 {
                    let since = Instant::now();
                    monoio::time::sleep(Duration::from_millis(20)).await;
                    let took = since.elapsed();
                    assert!(took >= Duration::from_millis(20), "{what}: {took:?}");
                    assert!(took < Duration::from_millis(1500), "{what}: {took:?}");
                }
            });

            // Bursts from another thread while the loop waits with short timeouts: waits
            // that end by timeout, by a completion, and by both at nearly the same time.
            const CONNS: usize = 16;
            const ROUNDS: usize = 200;
            rt.block_on(async {
                let mut peers = Vec::new();
                let mut tasks = Vec::new();
                let got = Rc::new(Cell::new(0_usize));
                for _ in 0..CONNS {
                    let (rd, wr) = std::os::unix::net::UnixStream::pair().unwrap();
                    let mut rd = UnixStream::from_std(rd).unwrap();
                    peers.push(wr);
                    let got = got.clone();
                    tasks.push(monoio::spawn(async move {
                        let mut buf = vec![0u8; 64];
                        loop {
                            let (res, b) = rd.read(buf).await;
                            buf = b;
                            match res.unwrap() {
                                0 => break,
                                n => got.set(got.get() + n),
                            }
                        }
                    }));
                }
                let writer = std::thread::spawn(move || {
                    use std::io::Write;
                    for round in 0..ROUNDS {
                        for mut peer in &peers {
                            peer.write_all(b"x").unwrap();
                        }
                        // 0 to 1.2 ms: around the loop's 1 ms timeout.
                        std::thread::sleep(Duration::from_micros((round % 7) as u64 * 200));
                    }
                    // Dropping the peers ends the readers.
                });
                let since = Instant::now();
                let mut ticks = 0_u32;
                while got.get() < CONNS * ROUNDS {
                    monoio::time::sleep(Duration::from_millis(1)).await;
                    ticks += 1;
                    assert!(
                        since.elapsed() < Duration::from_secs(30),
                        "{what}: {} of {} bytes",
                        got.get(),
                        CONNS * ROUNDS
                    );
                }
                writer.join().unwrap();
                for task in tasks {
                    task.await;
                }
                let took = since.elapsed();
                eprintln!(
                    "{what}: {} bytes in {took:?}, {ticks} timer ticks",
                    got.get()
                );
                // No sleep took much longer than its millisecond.
                assert!(took < Duration::from_millis(u64::from(ticks) * 50 + 2000));
            });
        });
    }
}

// The drain enters take neither the batching wait nor its batch size: with `min_wait` set, a
// drain enter that waited for `batch` CQEs would sleep once fewer than that are left.
#[test]
fn drain_enter_ignores_min_wait() {
    const WAIT: Duration = Duration::from_millis(400);
    for (what, rb) in [(
        "defer, drain 4",
        Builder::new().defer_taskrun().drain_taskrun(4),
    )] {
        let Some(mut rt) = build(what, rb) else {
            return;
        };
        let out = with_watchdog(what, 60, || {
            rt.block_on(ready_burst(
                // 96 of the 200 ready receives satisfy the waiting enter at once. With 20 per
                // drain enter, the later ones find fewer than 96 left.
                || match monoio::uring::set_min_wait(Some(WAIT), 96) {
                    Ok(()) => (),
                    Err(e) => eprintln!("min_wait not available: {e}"),
                },
                // The parks after the first would rightly sleep: off again.
                || monoio::uring::set_min_wait(None, 0).unwrap(),
            ))
        });
        eprintln!(
            "{what}, min_wait 96: first park delivered {} of {BURST} in {:?}",
            out.first_park, out.first_park_took
        );
        assert!(out.first_park_took < WAIT / 2, "{:?}", out.first_park_took);
        if limited_kernel() {
            assert!(out.first_park > 96, "{}", out.first_park);
        }
    }
}

/// The fastest of a few NOP round trips: each parks once.
async fn nop_round_trip() -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..5 {
        let since = Instant::now();
        nop().await;
        best = best.min(since.elapsed());
    }
    best
}

// (d) `set_min_wait` on and off, from inside the runtime and through the driver.
#[test]
fn set_min_wait_round_trip() {
    const WAIT: Duration = Duration::from_millis(100);
    const SLOW: Duration = Duration::from_millis(80);
    const FAST: Duration = Duration::from_millis(40);

    let mut rt = Builder::new().build().unwrap();
    let supported = rt.block_on(async {
        assert!(nop_round_trip().await < FAST);
        match monoio::uring::set_min_wait(Some(WAIT), 8) {
            Ok(()) => true,
            Err(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::Unsupported, "{e}");
                // Unchanged, and turning it off is fine anyway.
                assert!(nop_round_trip().await < FAST);
                monoio::uring::set_min_wait(None, 0).unwrap();
                monoio::uring::set_min_wait(Some(WAIT), 1).unwrap();
                false
            }
        }
    });
    if !supported {
        eprintln!("skipped: the kernel has no IORING_FEAT_MIN_TIMEOUT (Linux 6.12)");
        return;
    }
    rt.block_on(async {
        // On: one CQE is not the batch, so each park waits the whole time.
        let took = nop_round_trip().await;
        assert!(took >= SLOW, "on: {took:?}");
        // Off.
        monoio::uring::set_min_wait(None, 8).unwrap();
        let took = nop_round_trip().await;
        assert!(took < FAST, "off: {took:?}");
        // On again with another value, then off by a batch that needs no waiting.
        monoio::uring::set_min_wait(Some(WAIT * 2), 2).unwrap();
        let took = nop_round_trip().await;
        assert!(took >= SLOW * 2, "on again: {took:?}");
        monoio::uring::set_min_wait(Some(WAIT), 1).unwrap();
        let took = nop_round_trip().await;
        assert!(took < FAST, "batch 1: {took:?}");
        monoio::uring::set_min_wait(Some(WAIT), 0).unwrap();
        assert!(nop_round_trip().await < FAST);
    });

    // Through the driver, outside of `block_on`; it stays until changed.
    rt.driver().set_min_wait(Some(WAIT), 8).unwrap();
    let took = rt.block_on(nop_round_trip());
    assert!(took >= SLOW, "driver, on: {took:?}");
    rt.driver().set_min_wait(None, 0).unwrap();
    let took = rt.block_on(nop_round_trip());
    assert!(took < FAST, "driver, off: {took:?}");

    // It overrides the builder's value, and works under the timer driver.
    let mut rt = Builder::new()
        .min_wait(WAIT, 8)
        .enable_timer()
        .build()
        .unwrap();
    rt.block_on(async {
        let took = nop_round_trip().await;
        assert!(took >= SLOW, "builder: {took:?}");
        monoio::uring::set_min_wait(None, 0).unwrap();
        let took = nop_round_trip().await;
        assert!(took < FAST, "builder, then off: {took:?}");
    });
    rt.driver().inner().set_min_wait(Some(WAIT), 8).unwrap();
    let took = rt.block_on(nop_round_trip());
    assert!(took >= SLOW, "timer driver, on: {took:?}");
}

// A waiting park through the `Driver` trait: the drain must leave `park_timeout` usable on its
// own (no runtime loop around it).
#[test]
fn bare_park_timeout() {
    for (what, rb) in [(
        "defer, drain 4",
        Builder::new().defer_taskrun().drain_taskrun(4),
    )] {
        let Some(rt) = build(what, rb) else {
            return;
        };
        with_watchdog(what, 30, || {
            let since = Instant::now();
            for _ in 0..3 {
                rt.driver().park_timeout(Duration::from_millis(10)).unwrap();
            }
            let took = since.elapsed();
            assert!(took >= Duration::from_millis(30), "{what}: {took:?}");
            assert!(took < Duration::from_millis(1500), "{what}: {took:?}");
        });
    }
}
