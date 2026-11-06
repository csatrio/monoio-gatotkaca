//! Many connections sharing one incremental buffer ring (round 2, W2-recvinc, F3).
//!
//! Each connection runs `recv_multi` + `read_into`: the server reads a 4 KiB request, checks
//! its bytes, and answers with a 2-byte ack; a client runtime does ping-pong on every
//! connection. With an incremental ring the kernel hands out pieces of one buffer to many
//! connections, so the completions of different connections must be taken in the order the
//! kernel posted them. Before the fix they were taken in task order: offsets went wrong, data
//! was lost or corrupted and connections stalled.
#![cfg(feature = "bytes")]

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::BytesMut;
use monoio::{
    io::AsyncWriteRentExt,
    net::{TcpListener, TcpStream},
    uring::{BufRing, BufRingOptions},
    IoUringDriver, RuntimeBuilder,
};

const CONNS: usize = 200;
const REQ: usize = 4096;
const HEAD: usize = 200;

#[derive(Default)]
struct ServerStats {
    frames: AtomicU64,
    corrupt: AtomicU64,
    errors: AtomicU64,
    enobufs: AtomicU64,
}

fn fill(conn: u32, seq: u32) -> Vec<u8> {
    let mut req = vec![0u8; REQ];
    req[..4].copy_from_slice(&conn.to_be_bytes());
    req[4..8].copy_from_slice(&seq.to_be_bytes());
    for (i, b) in req[8..].iter_mut().enumerate() {
        *b = (conn as usize + seq as usize * 7 + i) as u8;
    }
    req
}

fn check(req: &[u8]) -> bool {
    let conn = u32::from_be_bytes(req[..4].try_into().unwrap());
    let seq = u32::from_be_bytes(req[4..8].try_into().unwrap());
    req == fill(conn, seq).as_slice()
}

/// Run the server on its own thread. Returns `None` if the kernel lacks incremental rings or
/// multishot recv.
fn server(
    options: BufRingOptions,
    entries: u16,
    buf_size: usize,
    stop: Arc<AtomicBool>,
    stats: Arc<ServerStats>,
) -> Option<(SocketAddr, std::thread::JoinHandle<()>)> {
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let mut rt = RuntimeBuilder::<IoUringDriver>::new()
            .with_entries(1024)
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async move {
            let ring = match BufRing::with_options(7, entries, buf_size, options) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("skipped: buffer ring unsupported: {e}");
                    tx.send(None).unwrap();
                    return;
                }
            };
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            tx.send(Some(listener.local_addr().unwrap())).unwrap();
            let mut tasks = Vec::new();
            for _ in 0..CONNS {
                let (mut stream, _) = listener.accept().await.unwrap();
                let ring = ring.clone();
                let stats = stats.clone();
                let stop = stop.clone();
                tasks.push(monoio::spawn(async move {
                    let mut chunks = match stream.recv_multi(&ring) {
                        Ok(c) => c,
                        Err(e) => panic!("recv_multi: {e}"),
                    };
                    let mut buf = BytesMut::with_capacity(2 * REQ);
                    let mut ack = vec![0u8; 2];
                    loop {
                        while buf.len() < REQ {
                            match chunks.read_into(&mut buf).await {
                                Ok(0) => return,
                                Ok(_) => {}
                                Err(e) if e.raw_os_error() == Some(libc::ENOBUFS) => {
                                    // Every buffer is held elsewhere: try again.
                                    stats.enobufs.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    if !stop.load(Ordering::Relaxed) {
                                        eprintln!("read_into: {e}");
                                        stats.errors.fetch_add(1, Ordering::Relaxed);
                                    }
                                    return;
                                }
                            }
                        }
                        let req = buf.split_to(REQ);
                        if !check(&req) {
                            stats.corrupt.fetch_add(1, Ordering::Relaxed);
                        }
                        stats.frames.fetch_add(1, Ordering::Relaxed);
                        let (res, back) = stream.write_all(ack).await;
                        ack = back;
                        if res.is_err() {
                            return;
                        }
                    }
                }));
            }
            for t in tasks {
                t.await;
            }
            assert_eq!(ring.stats().in_use, 0, "buffers still held at the end");
        });
    });
    let addr = rx.recv().unwrap()?;
    Some((addr, handle))
}

/// Ping-pong on `CONNS` connections until `stop`; `counts[i]` is the acks of connection `i`.
fn client(addr: SocketAddr, stop: Arc<AtomicBool>, counts: Arc<Vec<AtomicU64>>) {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(1024)
        .enable_timer()
        .build()
        .unwrap();
    rt.block_on(async move {
        let mut streams = Vec::new();
        for _ in 0..CONNS {
            streams.push(TcpStream::connect(addr).await.unwrap());
        }
        let mut tasks = Vec::new();
        for (i, mut stream) in streams.into_iter().enumerate() {
            let stop = stop.clone();
            let counts = counts.clone();
            tasks.push(monoio::spawn(async move {
                let mut seq = 0u32;
                let mut ack = Vec::with_capacity(2);
                while !stop.load(Ordering::Relaxed) {
                    // A head and a body, like an HTTP request: the server often gets two
                    // completions per request, interleaved with other connections'.
                    let mut body = fill(i as u32, seq);
                    let head: Vec<u8> = body.drain(..HEAD).collect();
                    let (res, _) = stream.write_all(head).await;
                    if res.is_err() {
                        return;
                    }
                    let (res, _) = stream.write_all(body).await;
                    if res.is_err() {
                        return;
                    }
                    // Short deadlines so a stalled connection still sees `stop`; a timed-out
                    // recv loses no data.
                    let mut got = 0;
                    while got < 2 {
                        let (res, back) = stream
                            .read_with_deadline(ack, Duration::from_millis(100))
                            .await;
                        ack = back;
                        match res {
                            Ok(0) => return,
                            Ok(n) => {
                                got += n;
                                ack.clear();
                                ack.reserve(2);
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                                if stop.load(Ordering::Relaxed) {
                                    return;
                                }
                            }
                            Err(_) => return,
                        }
                    }
                    counts[i].fetch_add(1, Ordering::Relaxed);
                    seq = seq.wrapping_add(1);
                }
            }));
        }
        for t in tasks {
            t.await;
        }
    });
}

struct Run {
    total: u64,
    stalled: usize,
    stats: Arc<ServerStats>,
}

fn run(options: BufRingOptions, entries: u16, buf_size: usize) -> Option<Run> {
    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(ServerStats::default());
    let (addr, server) = server(options, entries, buf_size, stop.clone(), stats.clone())?;
    let counts: Arc<Vec<AtomicU64>> = Arc::new((0..CONNS).map(|_| AtomicU64::new(0)).collect());
    let client = {
        let (stop, counts) = (stop.clone(), counts.clone());
        std::thread::spawn(move || client(addr, stop, counts))
    };
    std::thread::sleep(Duration::from_millis(1500));
    let before: Vec<u64> = counts.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    std::thread::sleep(Duration::from_millis(1000));
    let after: Vec<u64> = counts.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    stop.store(true, Ordering::Relaxed);
    client.join().unwrap();
    server.join().unwrap();
    let stalled = before.iter().zip(&after).filter(|(b, a)| a == b).count();
    let total = after.iter().sum::<u64>() - before.iter().sum::<u64>();
    Some(Run {
        total,
        stalled,
        stats,
    })
}

fn report(name: &str, r: &Run) {
    eprintln!(
        "{name}: {} frames in the last second, {} stalled connections, server frames {}, \
         corrupt {}, errors {}, enobufs {}",
        r.total,
        r.stalled,
        r.stats.frames.load(Ordering::Relaxed),
        r.stats.corrupt.load(Ordering::Relaxed),
        r.stats.errors.load(Ordering::Relaxed),
        r.stats.enobufs.load(Ordering::Relaxed),
    );
}

fn assert_healthy(name: &str, r: &Run) {
    assert_eq!(
        r.stats.corrupt.load(Ordering::Relaxed),
        0,
        "{name}: corrupt"
    );
    assert_eq!(r.stats.errors.load(Ordering::Relaxed), 0, "{name}: errors");
    assert_eq!(r.stalled, 0, "{name}: stalled connections");
}

/// A shared incremental ring with few buffers (exhausted often) next to a flat ring with one
/// buffer per connection: every connection keeps making progress, the data is intact, and the
/// incremental run is within a reasonable factor of the flat one.
#[test]
fn shared_incremental_ring_many_connections() {
    let Some(flat) = run(BufRingOptions::new(), 256, 16 * 1024) else {
        return;
    };
    report("flat", &flat);
    let Some(inc) = run(BufRingOptions::new().incremental(true), 4, 64 * 1024) else {
        return;
    };
    report("incremental", &inc);
    assert_healthy("flat", &flat);
    assert_healthy("incremental", &inc);
    assert!(
        inc.total * 4 >= flat.total,
        "incremental {} vs flat {}",
        inc.total,
        flat.total
    );
}
