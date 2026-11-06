//! `RuntimeBuilder::sqpoll` and its placement check. Each case runs on its own thread whose
//! affinity it sets; the probe finds the SQ thread by its name, `iou-sqp-<creator tid>`.
//! Cases run one at a time so the SQ pollers don't spin against each other.

use std::{io, sync::Mutex, thread, time::Duration};

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
    uring::libc,
    IoUringDriver, RuntimeBuilder,
};

static SERIAL: Mutex<()> = Mutex::new(());

type Builder = RuntimeBuilder<IoUringDriver>;

fn allowed_cpus() -> Vec<u32> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let rc =
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    assert_eq!(rc, 0);
    (0..8 * std::mem::size_of::<libc::cpu_set_t>())
        .filter(|&i| unsafe { libc::CPU_ISSET(i, &set) })
        .map(|i| i as u32)
        .collect()
}

fn pin(cpus: &[u32]) {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for &c in cpus {
        unsafe { libc::CPU_SET(c as usize, &mut set) };
    }
    let rc = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    assert_eq!(rc, 0, "sched_setaffinity: {}", io::Error::last_os_error());
}

/// Run `f` on a fresh thread pinned to `cpus`, one case at a time.
fn on_thread<T: Send + 'static>(cpus: Vec<u32>, f: impl FnOnce() -> T + Send + 'static) -> T {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    thread::spawn(move || {
        pin(&cpus);
        f()
    })
    .join()
    .unwrap()
}

/// `true` if the error means SQPOLL is not available here (privileges, sandbox).
fn unavailable(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EPERM) | Some(libc::ENOSYS))
}

/// Cpus_allowed_list of the SQ thread this thread's ring created (`iou-sqp-<our tid>`). It
/// may show up in `/proc` a little after `io_uring_setup` returns, so poll briefly.
fn own_sqp_cpus() -> Option<String> {
    let name = format!("iou-sqp-{}", unsafe { libc::gettid() });
    for _ in 0..200 {
        for e in std::fs::read_dir("/proc/self/task").unwrap().flatten() {
            let Ok(status) = std::fs::read_to_string(e.path().join("status")) else {
                continue;
            };
            let field = |k: &str| {
                status
                    .lines()
                    .find_map(|l| l.strip_prefix(k))
                    .map(str::trim)
            };
            if field("Name:") == Some(name.as_str()) {
                return field("Cpus_allowed_list:").map(str::to_string);
            }
        }
        thread::sleep(Duration::from_millis(5));
    }
    None
}

/// Build with `sqpoll`, run a loopback echo, and return the Cpus_allowed_list of the new SQ
/// thread. `None` if SQPOLL is unavailable.
fn build_and_probe(b: Builder) -> Option<String> {
    let mut rt = match b.build() {
        Ok(rt) => rt,
        Err(e) if unavailable(&e) => {
            eprintln!("skip: SQPOLL unavailable: {e}");
            return None;
        }
        Err(e) => panic!("build: {e}"),
    };
    rt.block_on(echo());
    let cpus = own_sqp_cpus().expect("SQ thread not found");
    eprintln!("SQ thread Cpus_allowed_list: {cpus}");
    Some(cpus)
}

async fn echo() {
    let srv = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = srv.local_addr().unwrap();
    let server = monoio::spawn(async move {
        let (mut c, _) = srv.accept().await.unwrap();
        let (res, buf) = c.read_exact(vec![0u8; 64]).await;
        res.unwrap();
        c.write_all(buf).await.0.unwrap();
    });
    let mut c = TcpStream::connect(addr).await.unwrap();
    c.write_all(vec![7u8; 64]).await.0.unwrap();
    let (res, buf) = c.read_exact(vec![0u8; 64]).await;
    res.unwrap();
    assert_eq!(buf, vec![7u8; 64]);
    server.await;
}

const IDLE: Duration = Duration::from_millis(10);

#[test]
fn conflict_unpinned_sq_on_one_cpu_denied() {
    let cpu = allowed_cpus()[0];
    let err = on_thread(vec![cpu], || {
        Builder::new()
            .sqpoll(IDLE, None)
            .deny_sqpoll_conflict()
            .build()
            .map(|_| ())
    })
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");
    assert!(err.to_string().contains("SQPOLL conflict"), "{err}");
}

#[test]
fn conflict_sq_on_loop_cpu_denied() {
    let cpu = allowed_cpus()[0];
    let err = on_thread(vec![cpu], move || {
        Builder::new()
            .sqpoll(IDLE, Some(cpu))
            .deny_sqpoll_conflict()
            .enable_timer()
            .build()
            .map(|_| ())
    })
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");
}

/// Without the deny flag a conflict only warns. Also the affinity finding: an SQ thread
/// without `cpu` inherits the creating thread's one-CPU mask, so it shares the loop's core.
#[test]
fn conflict_warns_and_unpinned_sq_inherits_affinity() {
    let cpu = allowed_cpus()[0];
    let got = on_thread(vec![cpu], || {
        build_and_probe(Builder::new().sqpoll(IDLE, None))
    });
    if let Some(list) = got {
        assert_eq!(list, cpu.to_string(), "SQ thread affinity");
    }
}

/// The flag survives a later `uring_builder` call (applied at `build()`).
#[test]
fn sqpoll_survives_uring_builder() {
    let cpu = allowed_cpus()[0];
    let err = on_thread(vec![cpu], || {
        Builder::new()
            .sqpoll(IDLE, None)
            .deny_sqpoll_conflict()
            .uring_builder(monoio::uring::IoUring::builder())
            .build()
            .map(|_| ())
    })
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

/// Loop may run on two CPUs, SQ thread pinned to the other one: no conflict, builds, works,
/// and the SQ thread sits on its CPU.
#[test]
fn no_conflict_sq_on_other_cpu() {
    let cpus = allowed_cpus();
    if cpus.len() < 2 {
        eprintln!("skip: needs 2 CPUs");
        return;
    }
    let (a, b) = (cpus[0], cpus[1]);
    let got = on_thread(vec![a, b], move || {
        build_and_probe(Builder::new().sqpoll(IDLE, Some(b)).deny_sqpoll_conflict())
    });
    if let Some(list) = got {
        assert_eq!(list, b.to_string());
    }
}

/// Wider mask, unpinned SQ thread: no conflict; it inherits the whole mask.
#[test]
fn no_conflict_unpinned_sq_on_wide_mask() {
    let cpus = allowed_cpus();
    if cpus.len() < 2 {
        eprintln!("skip: needs 2 CPUs");
        return;
    }
    let pair = vec![cpus[0], cpus[1]];
    let expect = format!("{}-{}", cpus[0], cpus[1]);
    let got = on_thread(pair, || {
        build_and_probe(Builder::new().sqpoll(IDLE, None).deny_sqpoll_conflict())
    });
    if let Some(list) = got {
        // "0-1" for adjacent CPUs, "a,b" otherwise
        assert!(list == expect || list.contains(','), "{list}");
    }
}

/// Loop pinned to one CPU, SQ thread pinned to another one outside the loop's mask: no
/// conflict. Linux 7.0 accepts an SQ CPU outside the creating thread's affinity mask (it
/// checks the cpuset only), so this is the usual thread-per-core setup.
#[test]
fn no_conflict_sq_outside_one_cpu_mask() {
    let cpus = allowed_cpus();
    if cpus.len() < 2 {
        eprintln!("skip: needs 2 CPUs");
        return;
    }
    let (a, b) = (cpus[0], cpus[1]);
    let got = on_thread(vec![a], move || {
        build_and_probe(Builder::new().sqpoll(IDLE, Some(b)).deny_sqpoll_conflict())
    });
    if let Some(list) = got {
        assert_eq!(list, b.to_string());
    }
}
