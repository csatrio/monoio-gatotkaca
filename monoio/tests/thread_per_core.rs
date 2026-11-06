//! `utils::ThreadPerCore`: per-core runtimes, SO_REUSEPORT, attach_wq lifetime, MSG_RING, startup
//! errors.
#![cfg(feature = "utils")]

use std::{
    io::{Read, Write},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Barrier, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{ListenerOpts, TcpListener},
    uring,
    utils::ThreadPerCore,
    IoUringDriver, RuntimeBuilder,
};

/// Two CPUs this thread may run on, or `None` (test skipped).
fn two_cpus() -> Option<[usize; 2]> {
    if std::thread::available_parallelism().map_or(1, |n| n.get()) < 2 {
        eprintln!("skip: fewer than 2 CPUs");
        return None;
    }
    // SAFETY: plain syscall into a zeroed cpu_set_t we own.
    let mut set: uring::libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let ok =
        unsafe { uring::libc::sched_getaffinity(0, std::mem::size_of_val(&set), &mut set) == 0 };
    if !ok {
        return Some([0, 1]);
    }
    let cpus: Vec<usize> = (0..uring::libc::CPU_SETSIZE as usize)
        .filter(|&c| unsafe { uring::libc::CPU_ISSET(c, &set) })
        .take(2)
        .collect();
    if cpus.len() < 2 {
        eprintln!("skip: fewer than 2 CPUs in the affinity mask");
        return None;
    }
    Some([cpus[0], cpus[1]])
}

#[test]
fn reuse_port_two_cores() {
    const CONNS: usize = 64;
    let Some(cpus) = two_cpus() else { return };

    // Core 0 binds port 0 and publishes the address; core 1 joins the same SO_REUSEPORT group.
    let addr = Arc::new((Mutex::new(None::<SocketAddr>), Condvar::new()));
    let (stop_tx, stop_rx) = mpsc::channel::<uring::EventSender<()>>();
    let tpc = ThreadPerCore::new(cpus)
        .thread_name("tpc-test")
        .builder(|_| RuntimeBuilder::<IoUringDriver>::new());
    let handle = {
        let addr = addr.clone();
        tpc.run(move |core| {
            let addr = addr.clone();
            let stop_tx = stop_tx.clone();
            async move {
                let opts = ListenerOpts::new().reuse_port(true);
                let listener = if core.index == 0 {
                    let l = TcpListener::bind_with_config("127.0.0.1:0", &opts).unwrap();
                    *addr.0.lock().unwrap() = Some(l.local_addr().unwrap());
                    addr.1.notify_all();
                    l
                } else {
                    let mut a = addr.0.lock().unwrap();
                    while a.is_none() {
                        a = addr.1.wait(a).unwrap();
                    }
                    TcpListener::bind_with_config(a.unwrap(), &opts).unwrap()
                };
                let (tx, mut rx) = uring::event_channel::<()>().unwrap();
                stop_tx.send(tx).unwrap();

                let count = std::rc::Rc::new(std::cell::Cell::new(0usize));
                let c = count.clone();
                monoio::spawn(async move {
                    loop {
                        let (mut conn, _) = listener.accept().await.unwrap();
                        c.set(c.get() + 1);
                        monoio::spawn(async move {
                            let (res, buf) = conn.read(vec![0u8; 16]).await;
                            let n = res.unwrap();
                            let (res, _) = conn.write_all(buf[..n].to_vec()).await;
                            res.unwrap();
                        });
                    }
                });
                rx.recv().await;
                (core.index, count.get())
            }
        })
    };
    let handle = match handle {
        Ok(h) => h,
        Err(e) => {
            eprintln!("skip: could not start 2 cores: {e}");
            return;
        }
    };
    assert_eq!(handle.len(), 2);

    let stops: Vec<_> = (0..2).map(|_| stop_rx.recv().unwrap()).collect();
    let server = addr.0.lock().unwrap().unwrap();
    for i in 0..CONNS {
        let mut s = std::net::TcpStream::connect(server).unwrap();
        s.write_all(&[i as u8]).unwrap();
        let mut b = [0u8; 1];
        s.read_exact(&mut b).unwrap();
        assert_eq!(b[0], i as u8);
    }
    for s in &stops {
        s.send(()).unwrap();
    }
    let results: Vec<_> = handle.join().into_iter().map(Result::unwrap).collect();
    println!("accepted per core: {results:?}");
    assert_eq!(results[0].0, 0);
    assert_eq!(results[1].0, 1);
    assert_eq!(results.iter().map(|r| r.1).sum::<usize>(), CONNS);
}

#[test]
fn attach_wq_primary_outlives_secondaries() {
    let Some(cpus) = two_cpus() else { return };
    let done = Arc::new(AtomicBool::new(false));
    let dir = Arc::new(tempfile::tempdir().unwrap());
    let handle = {
        let done = done.clone();
        ThreadPerCore::new(cpus).attach_wq(true).run(move |core| {
            let done = done.clone();
            let dir = dir.clone();
            async move {
                if core.index == 1 {
                    monoio::time::sleep(Duration::from_millis(200)).await;
                    // O_CREAT opens are punted to the (shared) io-wq.
                    let f = monoio::fs::File::create(dir.path().join("f"))
                        .await
                        .unwrap();
                    let (res, _) = f.write_all_at(b"hello".to_vec(), 0).await;
                    res.unwrap();
                    f.close().await.unwrap();
                    done.store(true, Ordering::SeqCst);
                }
                core.index
            }
        })
    };
    let handle = match handle {
        Ok(h) => h,
        Err(e) => {
            eprintln!("skip: could not start 2 cores with attach_wq: {e}");
            return;
        }
    };
    let mut results = handle.join().into_iter();
    // Core 0's future returns at once, but its thread ends only after core 1's.
    assert_eq!(results.next().unwrap().unwrap(), 0);
    assert!(done.load(Ordering::SeqCst), "core 0 ended before core 1");
    assert_eq!(results.next().unwrap().unwrap(), 1);
}

#[test]
fn msg_ring_between_cores() {
    let Some(cpus) = two_cpus() else { return };
    // Core 1 installs its mailbox before core 0 sends (completions without a hook are dropped).
    let barrier = Arc::new(Barrier::new(2));
    let handle = ThreadPerCore::new(cpus)
        .builder(|_| RuntimeBuilder::<IoUringDriver>::new())
        .run(move |core| {
            let barrier = barrier.clone();
            async move {
                assert_eq!(core.cores, 2);
                assert_eq!(core.rings.len(), 2);
                if core.index == 0 {
                    barrier.wait();
                    uring::msg_ring(&core.rings[1], 7, 42).await.unwrap();
                    None
                } else {
                    let mailbox = uring::msg_receiver();
                    barrier.wait();
                    Some(mailbox.recv().await)
                }
            }
        });
    let handle = match handle {
        Ok(h) => h,
        Err(e) => {
            eprintln!("skip: could not start 2 cores: {e}");
            return;
        }
    };
    let results: Vec<_> = handle.join().into_iter().map(Result::unwrap).collect();
    assert_eq!(results, vec![None, Some((7, 42))]);
}

#[test]
fn invalid_cpu_fails_startup() {
    for attach in [false, true] {
        let ran = Arc::new(AtomicBool::new(false));
        let r = ran.clone();
        let start = Instant::now();
        let res = ThreadPerCore::new([0, 100_000])
            .attach_wq(attach)
            .run(move |_| {
                r.store(true, Ordering::SeqCst);
                async {}
            });
        let err = res.expect_err("cpu 100000 must fail");
        println!("attach_wq={attach}: {err}");
        assert!(err.to_string().contains("core 1"), "{err}");
        assert!(
            !ran.load(Ordering::SeqCst),
            "no closure runs on a failed start"
        );
        assert!(start.elapsed() < Duration::from_secs(10));
    }
}

#[test]
fn empty_cpu_list() {
    let err = ThreadPerCore::new([]).run(|_| async {}).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
