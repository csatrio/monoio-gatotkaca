//! `thread_per_core`: an echo server on N pinned cores, each with its own `SO_REUSEPORT` listener
//! on the same port (`monoio::utils::ThreadPerCore`).
//!
//! Run: `cargo run --release -p adaptif-gatotkaca-monoio --example thread_per_core -- --cores 0,1
//! --port 9000 --secs 30`, then e.g. `nc 127.0.0.1 9000`. Prints the connections each core
//! accepted after `--secs` (or on Ctrl-C when built with `--features signal`).
//!
//! The kernel picks the listener of a new connection by a hash of its addresses, so the split
//! is even only on average. Steering by CPU (`SO_INCOMING_CPU`, a reuse-port BPF program) is
//! out of scope here.

use std::{cell::Cell, rc::Rc, sync::mpsc, time::Duration};

use monoio::{
    buf::IoBuf,
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{ListenerOpts, TcpListener},
    uring,
    utils::ThreadPerCore,
    IoUringDriver, RuntimeBuilder,
};

const USAGE: &str = "\
thread_per_core: echo server, one pinned runtime and one SO_REUSEPORT listener per core

USAGE: thread_per_core [--cores a,b,..] [--port P] [--secs S] [--attach-wq]
  --cores a,b,..  CPUs to run on (default: 0..min(4, available CPUs))
  --port P        TCP port on 0.0.0.0 (default 9000)
  --secs S        stop after S seconds (default: 10, or until Ctrl-C with --features signal)
  --attach-wq     share one io-wq between the cores (core 0 is the primary)";

fn main() {
    let mut cores: Option<Vec<usize>> = None;
    let mut port = 9000u16;
    let mut secs: Option<u64> = None;
    let mut attach_wq = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || {
            args.next()
                .unwrap_or_else(|| panic!("{a} needs a value\n{USAGE}"))
        };
        match a.as_str() {
            "--cores" => {
                cores = Some(
                    val()
                        .split(',')
                        .map(|c| c.trim().parse().expect("bad CPU number"))
                        .collect(),
                )
            }
            "--port" => port = val().parse().expect("bad port"),
            "--secs" => secs = Some(val().parse().expect("bad --secs")),
            "--attach-wq" => attach_wq = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                return;
            }
            other => panic!("unknown flag {other}\n{USAGE}"),
        }
    }
    let cores = cores.unwrap_or_else(|| {
        let n = std::thread::available_parallelism().map_or(1, |n| n.get());
        (0..n.min(4)).collect()
    });

    // Each core hands back a sender to stop it.
    let (stop_tx, stop_rx) = mpsc::channel::<uring::EventSender<()>>();
    let n = cores.len();
    let handle = ThreadPerCore::new(cores.iter().copied())
        .builder(|_| RuntimeBuilder::<IoUringDriver>::new().with_entries(1024))
        .attach_wq(attach_wq)
        .thread_name("echo-core")
        .run(move |core| {
            let stop_tx = stop_tx.clone();
            async move {
                let opts = ListenerOpts::new().reuse_port(true);
                let listener =
                    TcpListener::bind_with_config(("0.0.0.0", port), &opts).expect("bind failed");
                let (tx, mut stop) = uring::event_channel::<()>().expect("event_channel");
                let _ = stop_tx.send(tx);

                let accepted = Rc::new(Cell::new(0u64));
                let count = accepted.clone();
                monoio::spawn(async move {
                    loop {
                        let Ok((conn, _)) = listener.accept().await else {
                            continue;
                        };
                        count.set(count.get() + 1);
                        monoio::spawn(echo(conn));
                    }
                });
                stop.recv().await;
                (core.cpu, accepted.get())
            }
        })
        .expect("failed to start the cores");
    let stops: Vec<_> = (0..n).map(|_| stop_rx.recv().expect("core died")).collect();
    println!("echo on 0.0.0.0:{port}, cores {cores:?}");

    wait(secs);
    for s in &stops {
        let _ = s.send(());
    }
    let mut total = 0;
    for (index, r) in handle.join().into_iter().enumerate() {
        match r {
            Ok((cpu, count)) => {
                total += count;
                println!("core {index} (cpu {cpu}): {count} connections");
            }
            Err(_) => println!("core {index}: panicked"),
        }
    }
    println!("total: {total} connections");
}

async fn echo(mut conn: monoio::net::TcpStream) {
    let mut buf = vec![0u8; 4096];
    loop {
        let (res, b) = conn.read(buf).await;
        let n = match res {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let (res, b) = conn.write_all(b.slice(..n)).await;
        if res.is_err() {
            return;
        }
        buf = b.into_inner();
    }
}

/// Block until `secs` passed, or (with `signal` and no `--secs`) until Ctrl-C.
fn wait(secs: Option<u64>) {
    #[cfg(feature = "signal")]
    if secs.is_none() {
        println!("Ctrl-C to stop");
        let ctrlc = monoio::utils::CtrlC::new().expect("Ctrl-C handler");
        RuntimeBuilder::<IoUringDriver>::new()
            .build()
            .expect("runtime")
            .block_on(ctrlc);
        return;
    }
    let secs = secs.unwrap_or(10);
    println!("stopping in {secs} s");
    std::thread::sleep(Duration::from_secs(secs));
}
