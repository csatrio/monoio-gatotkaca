//! Accept batching: `TcpListener::accept_batch`, `AcceptStream::next_batch` and the automatic
//! re-arm of a multishot accept (round 2, M1).

use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use monoio::{
    net::{AcceptFlags, ListenerOpts, TcpListener, UnixListener},
    IoUringDriver, RuntimeBuilder,
};

/// Multishot accept needs Linux 5.19.
fn unsupported(e: &std::io::Error) -> bool {
    if e.raw_os_error() == Some(libc::EINVAL) {
        eprintln!("skipped: multishot accept unsupported: {e}");
        return true;
    }
    false
}

/// Counts how often the wrapped future is polled.
struct CountPolls<F> {
    inner: Pin<Box<F>>,
    polls: Rc<Cell<u32>>,
}

impl<F: Future> Future for CountPolls<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.polls.set(self.polls.get() + 1);
        self.inner.as_mut().poll(cx)
    }
}

fn listener() -> TcpListener {
    let opts = ListenerOpts::new().backlog(4096);
    TcpListener::bind_with_config("127.0.0.1:0", &opts).unwrap()
}

/// Connect `n` blocking clients; the kernel completes the handshakes, so they all sit in the
/// listener's accept queue.
fn connect_all(addr: std::net::SocketAddr, n: usize) -> Vec<std::net::TcpStream> {
    (0..n)
        .map(|_| std::net::TcpStream::connect(addr).unwrap())
        .collect()
}

const N: usize = 1000;

#[monoio::test]
async fn queued_connections_accepted_in_two_polls() {
    let listener = Rc::new(listener());
    let clients = connect_all(listener.local_addr().unwrap(), N);

    let polls = Rc::new(Cell::new(0));
    let accept_loop = {
        let listener = listener.clone();
        async move {
            let mut conns = Vec::with_capacity(N);
            while conns.len() < N {
                if let Err(e) = listener
                    .accept_batch_into(&mut conns, usize::MAX, AcceptFlags::EMPTY)
                    .await
                {
                    if conns.is_empty() && unsupported(&e) {
                        return None;
                    }
                    panic!("{e}");
                }
            }
            Some(conns)
        }
    };
    let task = monoio::spawn(CountPolls {
        inner: Box::pin(accept_loop),
        polls: polls.clone(),
    });
    let Some(conns) = task.await else { return };
    assert_eq!(conns.len(), N);
    // Poll 1 arms the multishot accept; the kernel accepts the whole queue in that one
    // submission, and poll 2 takes all of it.
    eprintln!("accept loop polled {} times", polls.get());
    assert!(polls.get() <= 2, "accept loop polled {} times", polls.get());

    // Peer addresses are the clients' local addresses.
    let mut want: Vec<_> = clients.iter().map(|c| c.local_addr().unwrap()).collect();
    let mut got: Vec<_> = conns.iter().map(|(_, a)| *a).collect();
    want.sort();
    got.sort();
    assert_eq!(want, got);
}

#[monoio::test]
async fn accept_batch_respects_max_and_keeps_the_rest_queued() {
    let listener = listener();
    let _clients = connect_all(listener.local_addr().unwrap(), 10);
    let mut total = 0;
    let mut batches = 0;
    while total < 10 {
        let got = match listener.accept_batch(3, AcceptFlags::CLOEXEC).await {
            Err(e) if total == 0 && unsupported(&e) => return,
            r => r.unwrap(),
        };
        assert!(!got.is_empty() && got.len() <= 3, "batch of {}", got.len());
        total += got.len();
        batches += 1;
    }
    assert_eq!(total, 10);
    assert!(batches >= 4);

    // Other flags while the accept is armed.
    let err = listener
        .accept_batch(3, AcceptFlags::NONBLOCK)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[monoio::test]
async fn concurrent_accept_batch_is_refused() {
    let listener = Rc::new(listener());
    let addr = listener.local_addr().unwrap();
    let first = {
        let listener = listener.clone();
        monoio::spawn(async move { listener.accept_batch(16, AcceptFlags::EMPTY).await })
    };
    // Let `first` arm and park.
    monoio::spawn(async {}).await;
    let err = listener
        .accept_batch(16, AcceptFlags::EMPTY)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy);

    let _client = std::net::TcpStream::connect(addr).unwrap();
    match first.await {
        Err(e) if unsupported(&e) => {}
        r => assert_eq!(r.unwrap().len(), 1),
    }
}

#[monoio::test]
async fn dropped_accept_batch_keeps_queued_connections() {
    let listener = listener();
    let addr = listener.local_addr().unwrap();
    // Arm, then drop the future while it waits.
    monoio::select! {
        r = listener.accept_batch(16, AcceptFlags::EMPTY) => {
            let e = r.unwrap_err();
            if unsupported(&e) { return; }
            panic!("{e}");
        }
        _ = monoio::spawn(async {}) => {}
    }
    let client = std::net::TcpStream::connect(addr).unwrap();
    let got = listener.accept_batch(16, AcceptFlags::EMPTY).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, client.local_addr().unwrap());
}

/// A CQ of 256 entries can't hold 1000 accept completions: the kernel ends the multishot
/// after a connection (no error, no `F_MORE`), and the stream re-arms on its own.
#[test]
fn accept_stream_rearms_after_full_cq() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(256)
        .cq_size(256)
        .build()
        .unwrap();
    rt.block_on(async {
        let listener = listener();
        let _clients = connect_all(listener.local_addr().unwrap(), N);
        let mut incoming = listener.accept_multi(AcceptFlags::EMPTY).unwrap();
        let mut conns = Vec::new();
        while conns.len() < N {
            match incoming.next_batch(&mut conns, usize::MAX).await {
                Err(e) if conns.is_empty() && unsupported(&e) => return,
                Ok(0) => panic!("accept stream ended after {} connections", conns.len()),
                r => r.unwrap(),
            };
        }
        assert_eq!(conns.len(), N);
        assert!(!incoming.is_finished());
        #[cfg(feature = "metrics")]
        {
            let code = monoio::uring::opcode::AcceptMulti::CODE as usize;
            let armed = monoio::driver_stats().ops_by_opcode[code];
            assert!(
                armed >= 2,
                "multishot accept armed {armed} times, expected a re-arm"
            );
        }

        // Still accepting after the re-arm.
        let _late = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let mut more = Vec::new();
        assert_eq!(incoming.next_stream_batch(&mut more, 8).await.unwrap(), 1);
    });
}

/// The listener's own acceptor survives the same re-arm.
#[test]
fn accept_batch_rearms_after_full_cq() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(256)
        .cq_size(256)
        .build()
        .unwrap();
    rt.block_on(async {
        let listener = listener();
        let _clients = connect_all(listener.local_addr().unwrap(), N);
        let mut conns = Vec::new();
        while conns.len() < N {
            match listener
                .accept_batch_into(&mut conns, usize::MAX, AcceptFlags::EMPTY)
                .await
            {
                Err(e) if conns.is_empty() && unsupported(&e) => return,
                r => r.unwrap(),
            };
        }
        assert_eq!(conns.len(), N);
    });
}

#[monoio::test]
async fn unix_accept_batch() {
    let dir = std::env::temp_dir().join(format!("monoio-accept-batch-{}", std::process::id()));
    let _ = std::fs::remove_file(&dir);
    let listener = UnixListener::bind(&dir).unwrap();
    let _clients: Vec<_> = (0..20)
        .map(|_| std::os::unix::net::UnixStream::connect(&dir).unwrap())
        .collect();
    let mut conns = Vec::new();
    while conns.len() < 20 {
        match listener
            .accept_batch_into(&mut conns, 64, AcceptFlags::EMPTY)
            .await
        {
            Err(e) if conns.is_empty() && unsupported(&e) => break,
            r => r.unwrap(),
        };
    }
    let _ = std::fs::remove_file(&dir);
}
