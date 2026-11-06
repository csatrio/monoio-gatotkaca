//! The in-flight guards of the ring ops that live outside of `driver/op/{send,recv,..}.rs`:
//! multishot and bundle receives, the read→send chain, the link-timeout ops and splice. Each
//! must make `try_send` / `try_recv` return `WouldBlock` while it is in flight (see
//! `tests/eager_send.rs` for the rest).

use std::{
    future::Future,
    io::{self, Write},
    os::fd::AsRawFd,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use monoio::{
    fs::File,
    net::{TcpListener, TcpStream},
    time::sleep,
    uring::BufRing,
};

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}

fn would_block<T>(res: io::Result<T>) -> bool {
    matches!(res, Err(e) if e.kind() == io::ErrorKind::WouldBlock)
}

/// Poll once with a waker that does nothing. An async fn pushes its SQE at its first poll.
fn poll_once<F: Future>(fut: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    fut.poll(&mut Context::from_waker(Waker::noop()))
}

/// Wait, in a plain `poll(2)` (the runtime does not run meanwhile), until data is queued on
/// `fd`.
fn wait_readable(fd: std::os::fd::RawFd) {
    let mut p = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one valid pollfd.
    let n = unsafe { libc::poll(&mut p, 1, 10_000) };
    assert!(n == 1 && p.revents & libc::POLLIN != 0, "no data arrived");
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(1)).await;
    }
}

/// An armed multishot receive is a receive in flight, for as long as its stream lives.
#[monoio::test(timer_enabled = true)]
async fn multishot_recv_counts() {
    let ring = match BufRing::new(41, 8, 256) {
        Ok(ring) => ring,
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
            eprintln!("skipped: provided buffer rings unsupported: {e}");
            return;
        }
        Err(e) => panic!("{e}"),
    };
    let (a, b) = tcp_pair().await;
    let mut buf = [0u8; 16];

    // The data is queued before the multishot receive is submitted to the kernel: only the
    // count of ops in flight keeps `try_recv` from taking it.
    assert_eq!(a.try_send(b"abc").unwrap(), 3);
    wait_readable(b.as_raw_fd());
    let mut chunks = b.recv_multi(&ring).unwrap();
    assert!(would_block(b.try_recv(&mut buf)));
    let got = match chunks.next().await.unwrap() {
        Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("skipped: multishot recv unsupported: {e}");
            return;
        }
        r => r.unwrap(),
    };
    assert_eq!(&*got, b"abc");
    drop(got);
    // Still armed: the next bytes belong to the multishot receive.
    assert!(would_block(b.try_recv(&mut buf)));
    assert_eq!(a.try_send(b"def").unwrap(), 3);
    assert_eq!(&*chunks.next().await.unwrap().unwrap(), b"def");

    // Dropped: cancelled, and free again once the kernel ended it.
    drop(chunks);
    assert!(would_block(b.try_recv(&mut buf)));
    sleep(Duration::from_millis(20)).await;
    assert_eq!(a.try_send(b"ghi").unwrap(), 3);
    let mut n = 0;
    eventually("try_recv after the multishot receive", || {
        n = b.try_recv(&mut buf).unwrap_or(0);
        n > 0
    })
    .await;
    assert_eq!(&buf[..n], b"ghi");

    // The single-shot bundle receive counts, too.
    if ring.supports_bundles() {
        assert_eq!(a.try_send(b"jkl").unwrap(), 3);
        wait_readable(b.as_raw_fd());
        let mut bundle = std::pin::pin!(b.recv_bundle(&ring));
        assert!(poll_once(bundle.as_mut()).is_pending());
        assert!(would_block(b.try_recv(&mut buf)));
        let bundle = bundle.await.unwrap();
        assert_eq!(&bundle.detach()[..], b"jkl");
    }
}

/// The send of a `read_at_then_send` chain is a send in flight on the stream.
#[monoio::test(timer_enabled = true)]
async fn read_then_send_chain_counts() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(&[5u8; 4096]).unwrap();
    tmp.flush().unwrap();
    let file = File::open(tmp.path()).await.unwrap();
    let (a, b) = tcp_pair().await;

    {
        let mut chain = std::pin::pin!(file.read_at_then_send(Vec::with_capacity(4096), 0, &a));
        assert!(poll_once(chain.as_mut()).is_pending());
        assert!(would_block(a.try_send(b"overtake")));
        let (res, _) = chain.await;
        assert_eq!(res.unwrap(), (4096, 4096));
    }
    assert_eq!(a.try_send(b"after").unwrap(), 5);

    let mut got = Vec::new();
    while got.len() < 4101 {
        let (res, buf) = b.recv_op(Vec::with_capacity(8192)).await;
        assert!(res.unwrap() > 0);
        got.extend_from_slice(&buf);
    }
    assert!(got[..4096].iter().all(|&b| b == 5));
    assert_eq!(&got[4096..], b"after");
}

/// `read_with_deadline` / `write_with_deadline` (link-timeout ops) count like plain ones.
#[monoio::test(timer_enabled = true)]
async fn deadline_ops_count() {
    let (a, b) = tcp_pair().await;
    let mut buf = [0u8; 16];

    {
        // Queued before the receive is submitted: only the count keeps `try_recv` off it.
        assert_eq!(a.try_send(b"abc").unwrap(), 3);
        wait_readable(b.as_raw_fd());
        let mut read =
            std::pin::pin!(b.read_with_deadline(Vec::with_capacity(16), Duration::from_secs(5)));
        assert!(poll_once(read.as_mut()).is_pending());
        assert!(would_block(b.try_recv(&mut buf)));
        let (res, data) = read.await;
        assert_eq!(res.unwrap(), 3);
        assert_eq!(data, b"abc");
    }
    assert!(would_block(b.try_recv(&mut buf)), "nothing is queued");

    {
        let mut write =
            std::pin::pin!(a.write_with_deadline(b"def".to_vec(), Duration::from_secs(5)));
        assert!(poll_once(write.as_mut()).is_pending());
        assert!(would_block(a.try_send(b"overtake")));
        assert_eq!(write.await.0.unwrap(), 3);
    }
    assert_eq!(a.try_send(b"ghi").unwrap(), 3);
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < 6 {
        if let Ok(n) = b.try_recv(&mut buf) {
            got.extend_from_slice(&buf[..n]);
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(got, b"defghi");

    // A receive that timed out is finished: nothing stays counted.
    let (res, _) = b
        .read_with_deadline(Vec::with_capacity(16), Duration::from_millis(5))
        .await;
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert_eq!(a.try_send(b"x").unwrap(), 1);
    eventually("try_recv after a timed-out receive", || {
        matches!(b.try_recv(&mut buf), Ok(1))
    })
    .await;
}

/// A splice into a socket is a send in flight on it, a splice out of it a receive in flight.
#[cfg(feature = "splice")]
#[monoio::test(timer_enabled = true)]
async fn splice_counts() {
    use monoio::{io::Splitable, net::unix::new_pipe};

    let (a, b) = tcp_pair().await;
    let (ar, mut aw) = a.into_split();
    let (mut br, _bw) = b.into_split();
    // A second handle of each stream, to call `try_*` while the splice borrows the half.
    let a_stream = aw.0.clone();
    let b_stream = br.0.clone();
    let mut buf = [0u8; 16];

    // pipe → socket
    let (rx, tx) = new_pipe().unwrap();
    {
        use std::os::fd::AsRawFd;
        // SAFETY: a plain write of 7 bytes into the pipe.
        let n = unsafe { libc::write(tx.as_raw_fd(), b"spliced".as_ptr().cast(), 7) };
        assert_eq!(n, 7);
    }
    {
        let mut splice = std::pin::pin!(rx.splice_to(&mut aw, 7));
        assert!(poll_once(splice.as_mut()).is_pending());
        // SAFETY: one thread; the call only reads the fd handle.
        assert!(would_block(unsafe { &*a_stream.get() }.try_send(b"x")));
        assert_eq!(splice.await.unwrap(), 7);
    }
    assert_eq!(unsafe { &*a_stream.get() }.try_send(b"!").unwrap(), 1);

    // socket → pipe
    let (rx2, tx2) = new_pipe().unwrap();
    {
        use std::os::fd::AsRawFd;
        // The data is queued; the splice is pushed, not submitted.
        wait_readable(unsafe { &*b_stream.get() }.as_raw_fd());
        let mut splice = std::pin::pin!(tx2.splice_from(&mut br, 8));
        assert!(poll_once(splice.as_mut()).is_pending());
        // SAFETY: as above.
        assert!(would_block(unsafe { &*b_stream.get() }.try_recv(&mut buf)));
        // "spliced" and "!" may arrive as one or two segments.
        let n = splice.await.unwrap();
        assert!(n == 7 || n == 8, "{n}");
    }
    drop((ar, rx2));
}
