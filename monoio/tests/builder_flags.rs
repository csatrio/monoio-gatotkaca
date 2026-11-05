//! Runtime builder ring flags and driver options: each one builds a runtime that runs a TCP
//! echo, with and without the timer.

use std::{
    cell::Cell,
    future::Future,
    io::Write,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
    IoUringDriver, RuntimeBuilder,
};

type Builder = RuntimeBuilder<IoUringDriver>;

/// Echo a few messages over loopback TCP.
async fn echo() {
    let srv = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = srv.local_addr().unwrap();
    let server = monoio::spawn(async move {
        let (mut conn, _) = srv.accept().await.unwrap();
        loop {
            let (res, buf) = conn.read_exact(vec![0u8; 16]).await;
            if res.is_err() {
                return;
            }
            conn.write_all(buf).await.0.unwrap();
        }
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    for i in 0..100u8 {
        client.write_all(vec![i; 16]).await.0.unwrap();
        let (res, buf) = client.read_exact(vec![0u8; 16]).await;
        res.unwrap();
        assert_eq!(buf, vec![i; 16]);
    }
    drop(client);
    server.await;
}

/// Whether a build error means the kernel lacks the flag.
fn unsupported(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(monoio::uring::libc::EINVAL) | Some(monoio::uring::libc::EOPNOTSUPP)
    )
}

/// Run the echo on `make()`'s runtime, plain and with the timer. Skips if the kernel refuses.
fn run(name: &str, make: impl Fn() -> Builder) {
    match make().build() {
        Ok(mut rt) => rt.block_on(echo()),
        Err(e) if unsupported(&e) => {
            eprintln!("skipping {name}: {e}");
            return;
        }
        Err(e) => panic!("{name}: {e}"),
    }
    let mut rt = make().enable_timer().build().unwrap();
    rt.block_on(async {
        echo().await;
        monoio::time::sleep(Duration::from_millis(1)).await;
    });
}

#[test]
fn coop_taskrun() {
    run("coop_taskrun", || Builder::new().coop_taskrun());
}

#[test]
fn taskrun_flag() {
    run("taskrun_flag", || {
        Builder::new().coop_taskrun().taskrun_flag()
    });
}

#[test]
fn single_issuer() {
    run("single_issuer", || Builder::new().single_issuer());
}

#[test]
fn defer_taskrun() {
    run("defer_taskrun", || Builder::new().defer_taskrun());
}

#[test]
fn cq_size() {
    run("cq_size", || Builder::new().cq_size(8192));
}

#[test]
fn register_ring_fd() {
    run("register_ring_fd", || Builder::new().register_ring_fd());
    let rt = Builder::new().register_ring_fd().build().unwrap();
    eprintln!(
        "ring fd registered: {}",
        rt.driver().is_ring_fd_registered()
    );
}

#[test]
fn min_wait() {
    run("min_wait", || {
        Builder::new().min_wait(Duration::from_micros(50), 8)
    });
}

#[test]
fn task_budget() {
    run("task_budget", || Builder::new().task_budget(1));
    run("task_budget", || Builder::new().task_budget(64));
}

#[test]
fn skip_empty_enter() {
    run("skip_empty_enter", || Builder::new().skip_empty_enter());
}

#[test]
fn combined() {
    run("combined", || {
        Builder::new()
            .with_entries(512)
            .cq_size(4096)
            .coop_taskrun()
            .defer_taskrun()
            .register_ring_fd()
            .min_wait(Duration::from_micros(20), 4)
            .task_budget(32)
            .skip_empty_enter()
    });
}

#[test]
fn uring_builder_then_flags() {
    run("uring_builder_then_flags", || {
        Builder::new()
            .uring_builder(monoio::IoUring::builder())
            .coop_taskrun()
            .taskrun_flag()
    });
}

/// Yields once, waking itself.
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// With DEFER_TASKRUN, completions must be processed while a task stays ready (the runtime
/// never parks), so a read finishes without the yielding task stopping first.
fn completions_while_busy(name: &str, make: impl Fn() -> Builder) {
    let mut rt = match make().build() {
        Ok(rt) => rt,
        Err(e) if unsupported(&e) => {
            eprintln!("skipping {name}: {e}");
            return;
        }
        Err(e) => panic!("{name}: {e}"),
    };

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = std::thread::spawn(move || {
        let (mut conn, _) = listener.accept().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        conn.write_all(b"hello").unwrap();
    });

    let timed_out = rt.block_on(async move {
        let done = Rc::new(Cell::new(false));
        let spin_done = done.clone();
        let spinner = monoio::spawn(async move {
            let start = Instant::now();
            while !spin_done.get() {
                if start.elapsed() > Duration::from_secs(5) {
                    return true;
                }
                YieldNow(false).await;
            }
            false
        });

        let mut conn = TcpStream::connect(addr).await.unwrap();
        let (res, buf) = conn.read_exact(vec![0u8; 5]).await;
        res.unwrap();
        assert_eq!(&buf, b"hello");
        done.set(true);
        spinner.await
    });
    assert!(
        !timed_out,
        "{name}: completions waited for the task queue to drain"
    );
    peer.join().unwrap();
}

#[test]
fn plain_processes_completions_while_busy() {
    completions_while_busy("plain", Builder::new);
}

#[test]
fn defer_taskrun_processes_completions_while_busy() {
    completions_while_busy("defer_taskrun", || Builder::new().defer_taskrun());
}

#[test]
fn defer_taskrun_without_taskrun_flag_processes_completions_while_busy() {
    completions_while_busy("raw defer_taskrun", || {
        let mut urb = monoio::IoUring::builder();
        urb.setup_single_issuer().setup_defer_taskrun();
        Builder::new().uring_builder(urb)
    });
}

#[test]
fn defer_taskrun_skip_empty_enter_processes_completions_while_busy() {
    completions_while_busy("defer_taskrun + skip_empty_enter", || {
        Builder::new()
            .defer_taskrun()
            .skip_empty_enter()
            .register_ring_fd()
    });
}

#[test]
fn skip_empty_enter_skips() {
    let mut rt = Builder::new().skip_empty_enter().build().unwrap();
    rt.block_on(async {
        let spinner = monoio::spawn(async {
            for _ in 0..100 {
                YieldNow(false).await;
            }
        });
        spinner.await;
    });
    let s = rt.driver().stats();
    assert!(s.enter_skipped > 0, "{s:?}");
}
