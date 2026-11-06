//! Direct-open files: `OpenOptions::open_direct` (a fixed file table slot, no fd).

use std::{
    future::Future,
    io::{ErrorKind, Read, Write},
    net::TcpStream as StdTcpStream,
    os::fd::AsRawFd,
    pin::pin,
    task::Poll,
};

use monoio::{
    fs::{Advice, File, OpenOptions},
    net::{TcpListener, TcpStream},
    uring::{self, fixed_file_stats, opcode, FixedBufPool},
    IoUringDriver, RuntimeBuilder,
};

/// Run `f` on a fresh runtime with a fixed file table of `user` + `kernel` slots. `None` (the
/// test is skipped) if the kernel can't register it.
fn run<F: Future>(user: u32, kernel: u32, f: impl FnOnce() -> F) -> Option<F::Output> {
    let mut builder = RuntimeBuilder::<IoUringDriver>::new();
    if user > 0 {
        builder = builder.with_fixed_files(user);
    }
    if kernel > 0 {
        builder = builder.with_fixed_files_kernel_alloc(kernel);
    }
    match builder.build() {
        Ok(mut rt) => Some(rt.block_on(f())),
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
            eprintln!("skipped: fixed file table unsupported: {e}");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

/// Let the driver reap the closes pushed by drops.
async fn settle() {
    for _ in 0..4 {
        // SAFETY: a NOP points to nothing.
        let op = unsafe { uring::submit(opcode::Nop::new().build(), ()) }.unwrap();
        op.await;
    }
}

fn in_use() -> u32 {
    fixed_file_stats().expect("no fixed file table").in_use
}

fn kernel_in_use() -> u32 {
    fixed_file_stats()
        .expect("no fixed file table")
        .kernel_in_use
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 % 251) as u8).collect()
}

fn temp_file(content: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(content).unwrap();
    f.flush().unwrap();
    f
}

async fn open_ro(path: &std::path::Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).open_direct(path).await
}

#[test]
fn open_direct_without_table() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt.block_on(async {
        let tmp = temp_file(b"x");
        let err = open_ro(tmp.path()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
    });
}

#[test]
fn open_direct_read_write_close() {
    run(4, 0, || async {
        let data = pattern(300 * 1024);
        let tmp = temp_file(&data);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open_direct(tmp.path())
            .await
            .unwrap();
        assert!(file.is_direct());
        assert!(file.is_fixed());
        assert_eq!(file.fixed_slot(), Some(0));
        assert_eq!(file.as_raw_fd(), -1);
        assert_eq!(in_use(), 1);

        // Positional reads, 128 KiB chunks.
        let (res, buf) = file.read_at(vec![0u8; 128 * 1024], 0).await;
        assert_eq!(res.unwrap(), 128 * 1024);
        assert_eq!(&buf[..], &data[..128 * 1024]);
        let (res, buf) = file.read_exact_at(vec![0u8; 1000], 200_000).await;
        res.unwrap();
        assert_eq!(&buf[..], &data[200_000..201_000]);
        let (res, _) = file.read_at(vec![0u8; 16], data.len() as u64).await;
        assert_eq!(res.unwrap(), 0);

        // Hints and sync work on the slot.
        file.fadvise(0, 0, Advice::Sequential).await.unwrap();
        file.fadvise(0, 128 * 1024, Advice::WillNeed).await.unwrap();

        let (res, _) = file.write_all_at(b"direct!".to_vec(), 10).await;
        res.unwrap();
        file.sync_data().await.unwrap();

        // No statx on a fixed file.
        let err = file.metadata().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        // The close is a slot update, done when it completes.
        file.close().await.unwrap();
        assert_eq!(in_use(), 0);
        let on_disk = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&on_disk[10..17], b"direct!");
        assert_eq!(on_disk.len(), data.len());
    });
}

#[test]
fn open_direct_drop_frees_the_slot() {
    run(2, 0, || async {
        let tmp = temp_file(b"hello");
        for _ in 0..10 {
            let file = open_ro(tmp.path()).await.unwrap();
            let (res, buf) = file.read_at(vec![0u8; 5], 0).await;
            assert_eq!(res.unwrap(), 5);
            assert_eq!(&buf, b"hello");
            drop(file);
            settle().await;
            assert_eq!(in_use(), 0);
        }
    });
}

#[test]
fn open_direct_errors_release_the_slot() {
    run(2, 0, || async {
        let err = open_ro("/nonexistent/monoio-fs-direct".as_ref())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(in_use(), 0);
        // Invalid options fail before a slot is taken.
        let err = OpenOptions::new().open_direct("/tmp").await.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
        assert_eq!(in_use(), 0);
    });
}

#[test]
fn open_direct_table_full() {
    run(2, 0, || async {
        let tmp = temp_file(b"abc");
        let a = open_ro(tmp.path()).await.unwrap();
        let b = open_ro(tmp.path()).await.unwrap();
        let err = open_ro(tmp.path()).await.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
        a.close().await.unwrap();
        let c = open_ro(tmp.path()).await.unwrap();
        let (res, buf) = c.read_at(vec![0u8; 3], 0).await;
        assert_eq!(res.unwrap(), 3);
        assert_eq!(&buf, b"abc");
        drop((b, c));
        settle().await;
        assert_eq!(in_use(), 0);
    });
}

#[test]
fn open_direct_kernel_alloc_fallback() {
    // 1 runtime slot, then kernel-allocated ones (needs Linux 6.0+ for the alloc range).
    let mut builder = RuntimeBuilder::<IoUringDriver>::new()
        .with_fixed_files(1)
        .with_fixed_files_kernel_alloc(2);
    builder = builder.with_entries(256);
    let mut rt = match builder.build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("skipped: kernel-allocated fixed files unsupported: {e}");
            return;
        }
    };
    rt.block_on(async {
        let tmp = temp_file(b"kernel slots");
        let a = open_ro(tmp.path()).await.unwrap();
        assert_eq!(a.fixed_slot(), Some(0));
        let b = open_ro(tmp.path()).await.unwrap();
        let c = open_ro(tmp.path()).await.unwrap();
        // Kernel slots follow the runtime ones.
        let (sb, sc) = (b.fixed_slot().unwrap(), c.fixed_slot().unwrap());
        assert!((1..3).contains(&sb) && (1..3).contains(&sc) && sb != sc);
        assert_eq!((in_use(), kernel_in_use()), (1, 2));
        for f in [&a, &b, &c] {
            let (res, buf) = f.read_exact_at(vec![0u8; 12], 0).await;
            res.unwrap();
            assert_eq!(&buf, b"kernel slots");
        }
        // The kernel range is full too.
        let err = open_ro(tmp.path()).await.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
        b.close().await.unwrap();
        assert_eq!(kernel_in_use(), 1);
        drop((a, c));
        settle().await;
        assert_eq!((in_use(), kernel_in_use()), (0, 0));
    });
}

/// Polls `fut` once (pushing its SQE), then drops it.
async fn poll_once_and_drop<F: Future>(fut: F) {
    let mut fut = pin!(fut);
    std::future::poll_fn(|cx| {
        let _ = fut.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;
}

#[test]
fn open_direct_dropped_in_flight() {
    run(2, 2, || async {
        let tmp = temp_file(b"dropped");
        for _ in 0..20 {
            poll_once_and_drop(open_ro(tmp.path())).await;
            settle().await;
            assert_eq!((in_use(), kernel_in_use()), (0, 0));
        }
        // With the runtime slots taken, the kernel-allocated path is dropped in flight.
        let a = open_ro(tmp.path()).await.unwrap();
        let b = open_ro(tmp.path()).await.unwrap();
        for _ in 0..20 {
            poll_once_and_drop(open_ro(tmp.path())).await;
            settle().await;
            assert_eq!(kernel_in_use(), 0);
        }
        // The kernel slots really are free: both can be filled.
        let c = open_ro(tmp.path()).await.unwrap();
        let d = open_ro(tmp.path()).await.unwrap();
        drop((a, b, c, d));
        settle().await;
        assert_eq!((in_use(), kernel_in_use()), (0, 0));
    });
}

#[test]
fn read_fixed_at_on_direct_file() {
    run(2, 0, || async {
        let data = pattern(64 * 1024);
        let tmp = temp_file(&data);
        let pool = match FixedBufPool::new(2, 64 * 1024) {
            Ok(pool) => pool,
            Err(e) => {
                eprintln!("skipped: fixed buffers unavailable: {e}");
                return;
            }
        };
        let file = open_ro(tmp.path()).await.unwrap();
        let buf = pool.get().unwrap();
        let (res, buf) = file.read_fixed_at(buf, 0).await;
        assert_eq!(res.unwrap(), data.len());
        assert_eq!(&buf[..], &data[..]);
        file.close().await.unwrap();
    });
}

#[test]
fn read_at_then_send_from_direct_file() {
    run(2, 0, || async {
        let data = pattern(100 * 1024);
        let tmp = temp_file(&data);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut peer = StdTcpStream::connect(addr).unwrap();
        let (stream, _): (TcpStream, _) = listener.accept().await.unwrap();
        let file = open_ro(tmp.path()).await.unwrap();
        let reader = std::thread::spawn(move || {
            let mut got = vec![0u8; 100 * 1024];
            peer.read_exact(&mut got).unwrap();
            got
        });
        let (res, _) = file
            .read_at_then_send(vec![0u8; 100 * 1024], 0, &stream)
            .await;
        assert_eq!(res.unwrap(), (100 * 1024, 100 * 1024));
        assert_eq!(reader.join().unwrap(), data);
        file.close().await.unwrap();
    });
}

#[cfg(feature = "splice")]
#[test]
fn splice_from_direct_file() {
    run(2, 0, || async {
        let data = pattern(200 * 1024);
        let tmp = temp_file(&data);
        let file = open_ro(tmp.path()).await.unwrap();
        let (rx, tx) = monoio::net::unix::new_pipe().unwrap();
        let mut got = Vec::new();
        let mut off = 0u64;
        loop {
            let n = file.splice_to_pipe(&tx, off, 64 * 1024).await.unwrap();
            if n == 0 {
                break;
            }
            off += n as u64;
            let mut chunk = vec![0u8; n as usize];
            let mut rd = 0;
            while rd < chunk.len() {
                let r = unsafe {
                    libc::read(
                        rx.as_raw_fd(),
                        chunk[rd..].as_mut_ptr() as *mut _,
                        chunk.len() - rd,
                    )
                };
                assert!(r > 0);
                rd += r as usize;
            }
            got.extend_from_slice(&chunk);
        }
        assert_eq!(got, data);
        file.close().await.unwrap();
    });
}
