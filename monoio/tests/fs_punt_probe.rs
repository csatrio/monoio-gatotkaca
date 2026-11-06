//! Which file ops does the kernel complete inline, and which go to io-wq? A measurement, not
//! a test: run it by hand (a few seconds), with the directory to probe in `PROBE_DIR` (default:
//! the system temp dir), e.g. on two filesystems:
//!
//! ```sh
//! cargo test -p adaptif-gatotkaca-monoio --test fs_punt_probe -- --ignored --nocapture
//! PROBE_DIR=$HOME cargo test -p adaptif-gatotkaca-monoio --test fs_punt_probe -- --ignored --nocapture
//! ```
//!
//! Method (no tracepoints needed, they are root-only on most systems): each op runs on a raw
//! ring of a fresh thread, one SQE at a time. `inline` counts ops whose CQE is already in the
//! CQ when the submitting `io_uring_enter` (submit only, no wait) returns: the kernel ran them
//! in the submitting thread. The others completed later, either from an io-wq worker (a punt)
//! or from task work (async buffered reads, polled sockets). `wrk` is the number of io-wq
//! workers (`iou-wrk-<tid>` threads of the probing thread) alive after the loop, and `wrk_cs`
//! their voluntary context switches, about one per work item: 0 workers means nothing was
//! punted. io-wq is per thread, so each row starts with none.

use std::{
    ffi::CString,
    os::fd::{AsRawFd, RawFd},
    path::{Path, PathBuf},
};

use monoio::io_uring::{opcode, squeue, types, IoUring};

const CHUNK: usize = 128 * 1024;

fn tid() -> i32 {
    unsafe { libc::gettid() }
}

/// (workers, their voluntary context switches) of the io-wq of thread `owner`.
fn iowq_workers(owner: i32) -> (usize, u64) {
    let name = format!("iou-wrk-{owner}");
    let mut n = 0;
    let mut cs = 0;
    for entry in std::fs::read_dir("/proc/self/task").unwrap().flatten() {
        let p = entry.path();
        let Ok(comm) = std::fs::read_to_string(p.join("comm")) else {
            continue;
        };
        if comm.trim() != name {
            continue;
        }
        n += 1;
        if let Ok(status) = std::fs::read_to_string(p.join("status")) {
            for line in status.lines() {
                if let Some(v) = line.strip_prefix("voluntary_ctxt_switches:") {
                    cs += v.trim().parse::<u64>().unwrap_or(0);
                }
            }
        }
    }
    (n, cs)
}

struct Probe {
    ring: IoUring,
    inline: u64,
    later: u64,
    errors: u64,
}

impl Probe {
    fn new() -> Self {
        Probe {
            ring: IoUring::new(64).unwrap(),
            inline: 0,
            later: 0,
            errors: 0,
        }
    }

    /// Submit one SQE, note whether its CQE was there when the submit returned, wait for it.
    /// Returns the result.
    fn one(&mut self, sqe: squeue::Entry) -> i32 {
        unsafe { self.ring.submission().push(&sqe.user_data(7)).unwrap() };
        self.ring.submit().unwrap();
        let ready = !self.ring.completion().is_empty();
        if ready {
            self.inline += 1;
        } else {
            self.later += 1;
            self.ring.submit_and_wait(1).unwrap();
        }
        let cqe = self.ring.completion().next().unwrap();
        if cqe.result() < 0 {
            self.errors += 1;
        }
        cqe.result()
    }
}

fn evict(fd: RawFd) {
    unsafe {
        libc::fdatasync(fd);
        libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED);
    }
}

/// Fraction of the first `len` bytes of `fd` that are in the page cache.
fn resident(fd: RawFd, len: usize) -> f64 {
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            fd,
            0,
        );
        assert_ne!(p, libc::MAP_FAILED);
        let pages = len.div_ceil(4096);
        let mut vec = vec![0u8; pages];
        libc::mincore(p, len, vec.as_mut_ptr());
        libc::munmap(p, len);
        vec.iter().filter(|b| *b & 1 != 0).count() as f64 / pages as f64
    }
}

/// Run `f` with a fresh probe on a fresh thread and print one row.
fn row(name: &str, iters: u64, f: impl FnOnce(&mut Probe, u64) + Send + 'static) {
    let name = name.to_string();
    std::thread::spawn(move || {
        let mut p = Probe::new();
        let start = std::time::Instant::now();
        f(&mut p, iters);
        let elapsed = start.elapsed();
        let (wrk, cs) = iowq_workers(tid());
        println!(
            "| {name:<34} | {iters:>6} | {:>6.1}% | {:>6.1}% | {wrk:>3} | {cs:>6} | {:>7.2} | {} |",
            100.0 * p.inline as f64 / iters as f64,
            100.0 * p.later as f64 / iters as f64,
            elapsed.as_secs_f64() * 1e6 / iters as f64,
            p.errors,
        );
    })
    .join()
    .unwrap();
}

fn cpath(p: &Path) -> CString {
    CString::new(p.as_os_str().as_encoded_bytes()).unwrap()
}

#[test]
#[ignore = "a measurement; run by hand with --ignored --nocapture"]
fn file_op_punts() {
    let dir: PathBuf = std::env::var_os("PROBE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let file = tempfile::Builder::new()
        .prefix("monoio-punt-probe")
        .tempfile_in(&dir)
        .unwrap();
    std::fs::write(file.path(), vec![0x5a_u8; 4 * CHUNK]).unwrap();
    let fstype = std::process::Command::new("stat")
        .args(["-f", "-c", "%T"])
        .arg(&dir)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    println!(
        "\nkernel {}, {} ({fstype})",
        std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_default()
            .trim(),
        dir.display()
    );
    println!(
        "| op                                 |  iters | inline |  later | wrk | wrk_cs | us/op   | errors |"
    );
    println!(
        "|------------------------------------|--------|--------|--------|-----|--------|---------|--------|"
    );
    let path = file.path().to_path_buf();
    let n = 10_000;

    let p = cpath(&path);
    row("openat (cached path, O_RDONLY)", n, move |pr, n| {
        for _ in 0..n {
            let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), p.as_ptr())
                .flags(libc::O_RDONLY | libc::O_CLOEXEC)
                .build();
            let fd = pr.one(sqe);
            if fd >= 0 {
                unsafe { libc::close(fd) };
            }
        }
    });

    let p = cpath(&path);
    row("close (fd)", n, move |pr, n| {
        for _ in 0..n {
            let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
            pr.one(opcode::Close::new(types::Fd(fd)).build());
        }
    });

    let p = cpath(&path);
    row("openat direct (file_index)", n, move |pr, n| {
        pr.ring.submitter().register_files_sparse(4).unwrap();
        let slot = types::DestinationSlot::try_from_slot_target(0).unwrap();
        for _ in 0..n {
            let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), p.as_ptr())
                .flags(libc::O_RDONLY)
                .file_index(Some(slot))
                .build();
            pr.one(sqe);
            // Clear the slot again synchronously (not measured).
            pr.ring.submitter().register_files_update(0, &[-1]).unwrap();
        }
    });

    let p = cpath(&path);
    row("close direct (fixed slot)", n, move |pr, n| {
        pr.ring.submitter().register_files_sparse(4).unwrap();
        for _ in 0..n {
            let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDONLY) };
            pr.ring.submitter().register_files_update(0, &[fd]).unwrap();
            unsafe { libc::close(fd) };
            pr.one(opcode::Close::new(types::Fixed(0)).build());
        }
    });

    let p = cpath(&path);
    row("read 128 KiB, warm (fd)", n, move |pr, n| {
        let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDONLY) };
        let mut buf = vec![0u8; CHUNK];
        for i in 0..n {
            let off = (i % 4) * CHUNK as u64;
            pr.one(
                opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), CHUNK as u32)
                    .offset(off)
                    .build(),
            );
        }
        unsafe { libc::close(fd) };
    });

    let p = cpath(&path);
    row("read 128 KiB, warm (fixed slot)", n, move |pr, n| {
        let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDONLY) };
        pr.ring.submitter().register_files_sparse(4).unwrap();
        pr.ring.submitter().register_files_update(0, &[fd]).unwrap();
        let mut buf = vec![0u8; CHUNK];
        for i in 0..n {
            let off = (i % 4) * CHUNK as u64;
            pr.one(
                opcode::Read::new(types::Fixed(0), buf.as_mut_ptr(), CHUNK as u32)
                    .offset(off)
                    .build(),
            );
        }
        unsafe { libc::close(fd) };
    });

    // Cold: evict the file (POSIX_FADV_DONTNEED works on own clean pages without root).
    {
        let f = std::fs::File::open(&path).unwrap();
        let _ = std::fs::read(&path);
        let before = resident(f.as_raw_fd(), 4 * CHUNK);
        evict(f.as_raw_fd());
        let after = resident(f.as_raw_fd(), 4 * CHUNK);
        println!(
            "(eviction check: {:.0}% resident before DONTNEED, {:.0}% after)",
            before * 100.0,
            after * 100.0
        );
    }
    let p = cpath(&path);
    row("read 128 KiB, cold (fd, DONTNEED)", 500, move |pr, n| {
        let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDONLY) };
        let mut buf = vec![0u8; CHUNK];
        for _ in 0..n {
            evict(fd);
            pr.one(
                opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), CHUNK as u32)
                    .offset(0)
                    .build(),
            );
        }
        unsafe { libc::close(fd) };
    });

    for (name, advice) in [
        ("fadvise SEQUENTIAL", libc::POSIX_FADV_SEQUENTIAL),
        ("fadvise WILLNEED", libc::POSIX_FADV_WILLNEED),
    ] {
        let p = cpath(&path);
        row(name, n, move |pr, n| {
            let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDONLY) };
            for _ in 0..n {
                pr.one(
                    opcode::Fadvise::new(types::Fd(fd), CHUNK as _, advice)
                        .offset(0)
                        .build(),
                );
            }
            unsafe { libc::close(fd) };
        });
    }

    let p = cpath(&path);
    row("splice file->pipe 64 KiB, warm", 2_000, move |pr, n| {
        let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDONLY) };
        let mut pipe = [0; 2];
        unsafe { libc::pipe(pipe.as_mut_ptr()) };
        let mut sink = vec![0u8; 64 * 1024];
        for i in 0..n {
            let off = ((i % 4) * CHUNK as u64) as i64;
            let moved = pr.one(
                opcode::Splice::new(types::Fd(fd), off, types::Fd(pipe[1]), -1, 64 * 1024).build(),
            );
            let mut left = moved.max(0) as usize;
            while left > 0 {
                let r = unsafe { libc::read(pipe[0], sink.as_mut_ptr() as *mut _, left) };
                assert!(r > 0);
                left -= r as usize;
            }
        }
        unsafe {
            libc::close(fd);
            libc::close(pipe[0]);
            libc::close(pipe[1]);
        }
    });

    let p = cpath(&path);
    row("splice ... SPLICE_F_NONBLOCK", 2_000, move |pr, n| {
        let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDONLY) };
        let mut pipe = [0; 2];
        unsafe { libc::pipe(pipe.as_mut_ptr()) };
        let mut sink = vec![0u8; 64 * 1024];
        for i in 0..n {
            let off = ((i % 4) * CHUNK as u64) as i64;
            let moved = pr.one(
                opcode::Splice::new(types::Fd(fd), off, types::Fd(pipe[1]), -1, 64 * 1024)
                    .flags(libc::SPLICE_F_NONBLOCK)
                    .build(),
            );
            let mut left = moved.max(0) as usize;
            while left > 0 {
                let r = unsafe { libc::read(pipe[0], sink.as_mut_ptr() as *mut _, left) };
                assert!(r > 0);
                left -= r as usize;
            }
        }
        unsafe {
            libc::close(fd);
            libc::close(pipe[0]);
            libc::close(pipe[1]);
        }
    });

    let p = cpath(&path);
    row("statx (path)", n, move |pr, n| {
        let mut stx: libc::statx = unsafe { std::mem::zeroed() };
        for _ in 0..n {
            pr.one(
                opcode::Statx::new(
                    types::Fd(libc::AT_FDCWD),
                    p.as_ptr(),
                    &mut stx as *mut libc::statx as *mut _,
                )
                .mask(libc::STATX_SIZE)
                .build(),
            );
        }
    });
}
