//! Multishot accept: `TcpListener::accept_multi` / `UnixListener::accept_multi`.

// Each test runs its own single-threaded runtime; the lock only serializes the tests.
#![allow(clippy::await_holding_lock)]

use std::{
    io::Read,
    os::unix::io::AsRawFd,
    sync::{mpsc, Mutex},
    time::Duration,
};

use monoio::{
    io::stream::Stream,
    net::{AcceptFlags, TcpListener, UnixListener},
};

// The fd counting test needs the fd table to itself.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn nonblock(fd: i32) -> bool {
    let fl = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(fl >= 0);
    fl & libc::O_NONBLOCK != 0
}

fn cloexec(fd: i32) -> bool {
    let fl = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(fl >= 0);
    fl & libc::FD_CLOEXEC != 0
}

fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

async fn sleep(ms: u64) {
    monoio::time::sleep(Duration::from_millis(ms)).await;
}

/// Multishot accept needs Linux 5.19: skip if the first result says it is unsupported.
fn unsupported(e: &std::io::Error) -> bool {
    if e.raw_os_error() == Some(libc::EINVAL) {
        eprintln!("skipped: multishot accept unsupported: {e}");
        return true;
    }
    false
}

#[monoio::test(timer_enabled = true)]
async fn accept_50_connections_with_flags() {
    let _serial = serial();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mut incoming = listener
        .accept_multi(AcceptFlags::NONBLOCK | AcceptFlags::CLOEXEC)
        .unwrap();

    let (done_tx, done_rx) = mpsc::channel::<()>();
    let client = std::thread::spawn(move || {
        let conns: Vec<_> = (0..50)
            .map(|_| std::net::TcpStream::connect(addr).unwrap())
            .collect();
        let locals: Vec<_> = conns.iter().map(|c| c.local_addr().unwrap()).collect();
        done_rx.recv().unwrap();
        locals
    });

    let mut peers = Vec::new();
    for i in 0..50 {
        let res = incoming.next().await.unwrap();
        let (stream, peer) = match res {
            Err(e) if i == 0 && unsupported(&e) => {
                drop(done_tx);
                let _ = client.join();
                return;
            }
            r => r.unwrap(),
        };
        assert!(nonblock(stream.as_raw_fd()));
        assert!(cloexec(stream.as_raw_fd()));
        peers.push(peer);
    }
    assert!(!incoming.is_finished());
    done_tx.send(()).unwrap();
    let mut locals = client.join().unwrap();
    locals.sort();
    peers.sort();
    assert_eq!(locals, peers);

    // Cancel: the stream ends with `None`.
    incoming.cancel();
    assert!(incoming.next().await.is_none());
    assert!(incoming.is_finished());
}

#[monoio::test(timer_enabled = true)]
async fn next_stream_and_no_flags() {
    let _serial = serial();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mut incoming = listener.accept_multi(AcceptFlags::default()).unwrap();
    let client = std::thread::spawn(move || std::net::TcpStream::connect(addr).unwrap());
    let stream = match incoming.next_stream().await.unwrap() {
        Err(e) if unsupported(&e) => return,
        r => r.unwrap(),
    };
    let client = client.join().unwrap();
    assert!(!nonblock(stream.as_raw_fd()));
    assert!(!cloexec(stream.as_raw_fd()));
    assert_eq!(stream.peer_addr().unwrap(), client.local_addr().unwrap());

    // The `Stream` impl yields the same items.
    let client = std::thread::spawn(move || std::net::TcpStream::connect(addr).unwrap());
    let (_stream, peer) = Stream::next(&mut incoming).await.unwrap().unwrap();
    assert_eq!(peer, client.join().unwrap().local_addr().unwrap());
}

#[monoio::test(timer_enabled = true)]
async fn drop_closes_queued_connections() {
    let _serial = serial();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Probe support first, so the drop below really has accepted fds queued.
    {
        let mut probe = listener.accept_multi(AcceptFlags::default()).unwrap();
        let client = std::thread::spawn(move || std::net::TcpStream::connect(addr).unwrap());
        if let Err(e) = probe.next_stream().await.unwrap() {
            assert!(unsupported(&e), "{e}");
            return;
        }
        drop(client.join());
    }
    sleep(20).await;

    let before = open_fds();
    let incoming = listener.accept_multi(AcceptFlags::default()).unwrap();
    // Submit the accept before the clients connect.
    sleep(10).await;

    const N: usize = 10;
    let (tx, rx) = mpsc::channel();
    let client = std::thread::spawn(move || {
        let conns: Vec<_> = (0..N)
            .map(|_| std::net::TcpStream::connect(addr).unwrap())
            .collect();
        tx.send(()).unwrap();
        conns
    });
    rx.recv().unwrap();
    // Let the kernel post the accepts: N client fds and N accepted fds.
    sleep(50).await;
    assert_eq!(open_fds(), before + 2 * N);

    drop(incoming);
    sleep(50).await;
    // Only the client sockets are left.
    assert_eq!(open_fds(), before + N);

    for mut conn in client.join().unwrap() {
        conn.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut buf = [0u8; 1];
        let ret = conn.read(&mut buf);
        assert!(
            matches!(ret, Ok(0))
                || matches!(&ret, Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset),
            "{ret:?}"
        );
    }
}

#[monoio::test(timer_enabled = true)]
async fn unix_accept_multi() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let mut incoming = listener.accept_multi(AcceptFlags::CLOEXEC).unwrap();

    let p = path.clone();
    let client = std::thread::spawn(move || {
        (0..3)
            .map(|_| std::os::unix::net::UnixStream::connect(&p).unwrap())
            .collect::<Vec<_>>()
    });
    for i in 0..3 {
        let stream = match incoming.next_stream().await.unwrap() {
            Err(e) if i == 0 && unsupported(&e) => return,
            r => r.unwrap(),
        };
        assert!(cloexec(stream.as_raw_fd()));
    }
    let _clients = client.join().unwrap();

    let p = path.clone();
    let client = std::thread::spawn(move || std::os::unix::net::UnixStream::connect(p).unwrap());
    let (_stream, _addr) = incoming.next().await.unwrap().unwrap();
    let _client = client.join().unwrap();
    incoming.cancel();
    assert!(incoming.next().await.is_none());
}
