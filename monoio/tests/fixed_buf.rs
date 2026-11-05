use std::{collections::HashSet, io::Write};

use monoio::{
    buf::IoBufMut,
    fs::File,
    io::{AsyncReadRentExt, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
    uring::{FixedBuf, FixedBufPool},
    IoUringDriver, RuntimeBuilder,
};
use tempfile::NamedTempFile;

/// Sparse buffer tables need Linux 5.13; skip on older kernels.
fn pool(count: u16, size: usize) -> Option<FixedBufPool> {
    pool_with_table(count, size, FixedBufPool::DEFAULT_TABLE_SLOTS)
}

fn pool_with_table(count: u16, size: usize, slots: u32) -> Option<FixedBufPool> {
    match FixedBufPool::with_table_size(count, size, slots) {
        Ok(pool) => Some(pool),
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => {
            eprintln!("skipped: sparse registered buffers unsupported: {e}");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

fn filled(pool: &FixedBufPool, data: &[u8]) -> FixedBuf {
    let mut buf = pool.get().expect("pool empty");
    buf.extend_from_slice(data);
    buf
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = monoio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), accepted.unwrap().0)
}

#[monoio::test]
async fn get_drop_reuse() {
    let Some(pool) = pool(2, 4096) else { return };
    assert_eq!(pool.capacity(), 2);
    assert_eq!(pool.buf_size(), 4096);

    let a = pool.get().unwrap();
    let b = pool.get().unwrap();
    assert_ne!(a.buf_index(), b.buf_index());
    assert_eq!((a.len(), a.capacity()), (0, 4096));
    assert!(pool.get().is_none());
    let stats = pool.stats();
    assert_eq!((stats.in_use, stats.high_water, stats.capacity), (2, 2, 2));
    assert_eq!(stats.exhausted, 1);

    let index = a.buf_index();
    drop(a);
    assert_eq!(pool.stats().in_use, 1);
    let c = pool.get().unwrap();
    assert_eq!(c.buf_index(), index);
    drop((b, c));
    let stats = pool.stats();
    assert_eq!((stats.in_use, stats.high_water), (0, 2));
}

#[monoio::test]
async fn buffer_api() {
    let Some(pool) = pool(1, 8) else { return };
    let mut buf = pool.get().unwrap();
    buf.extend_from_slice(b"abc");
    buf.extend_from_slice(b"de");
    assert_eq!(&buf[..], b"abcde");
    buf[0] = b'A';
    buf.set_len(2);
    assert_eq!(&buf[..], b"Ab");
    assert_eq!(buf.as_full_mut().len(), 8);
    buf.clear();
    assert!(buf.is_empty());
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| buf.set_len(9)));
    assert!(r.is_err());
}

#[monoio::test]
async fn file_round_trip() {
    let Some(pool) = pool(2, 4096) else { return };
    let tmp = NamedTempFile::new().unwrap();
    let file = File::create(tmp.path()).await.unwrap();

    let data: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let (res, buf) = file.write_fixed_at(filled(&pool, &data), 100).await;
    assert_eq!(res.unwrap(), 4096);
    drop(buf);
    file.sync_all().await.unwrap();

    let file = File::open(tmp.path()).await.unwrap();
    let (res, buf) = file.read_fixed_at(pool.get().unwrap(), 100).await;
    assert_eq!(res.unwrap(), 4096);
    assert_eq!(&buf[..], &data[..]);

    // Short read at the end of the file sets the length.
    let (res, buf) = file.read_fixed_at(buf, 4000).await;
    assert_eq!(res.unwrap(), 196);
    assert_eq!(&buf[..], &data[3900..]);

    // The named futures.
    let fut = file.read_fixed_at_op(buf, 0);
    let (res, buf) = fut.await;
    assert_eq!(res.unwrap(), 4096);
    assert_eq!(&buf[100..], &data[..3996]);
    file.close().await.unwrap();
}

#[monoio::test]
async fn tcp_send_recv_fixed() {
    let Some(pool) = pool(4, 1024) else { return };
    let (client, server) = tcp_pair().await;

    let (res, sent) = client.send_fixed(filled(&pool, b"hello fixed")).await;
    assert_eq!(res.unwrap(), 11);
    assert_eq!(&sent[..], b"hello fixed");

    let (res, got) = server.recv_fixed(pool.get().unwrap()).await;
    assert_eq!(res.unwrap(), 11);
    assert_eq!(&got[..], b"hello fixed");

    // Frozen slices go out as fixed writes too.
    let frozen = filled(&pool, b"0123456789").freeze();
    let (res, _) = server.send_fixed_op(frozen.slice_ref(2..5)).await;
    assert_eq!(res.unwrap(), 3);
    let (res, _) = server.send_fixed(frozen.slice(7..)).await;
    assert_eq!(res.unwrap(), 3);
    let (res, got) = client.recv_fixed_op(got).await;
    let n = res.unwrap();
    let mut all = got[..n].to_vec();
    if all.len() < 6 {
        let (res, got) = client.recv_fixed(got).await;
        all.extend_from_slice(&got[..res.unwrap()]);
    }
    assert_eq!(all, b"234789");

    // EOF.
    drop(server);
    let (res, _) = client.recv_fixed(pool.get().unwrap()).await;
    assert_eq!(res.unwrap(), 0);
}

#[monoio::test]
async fn freeze_clone_slice() {
    let Some(pool) = pool(1, 64) else { return };
    let frozen = filled(&pool, b"hello world").freeze();
    assert_eq!(pool.stats().in_use, 1);
    assert_eq!(&frozen[..], b"hello world");

    let a = frozen.clone();
    let b = frozen.slice_ref(6..);
    let c = b.clone().slice(1..=2);
    assert_eq!(&b[..], b"world");
    assert_eq!(&c[..], b"or");
    assert_eq!(c.buf_index(), frozen.buf_index());
    assert_eq!(frozen.ref_count(), 4);
    assert!(pool.get().is_none());

    let frozen = frozen.try_into_mut().unwrap_err();
    drop((frozen, a, b));
    assert_eq!(pool.stats().in_use, 1);
    let buf = c.try_into_mut().unwrap();
    assert_eq!(&buf[..], b"hello world");
    drop(buf);
    assert_eq!(pool.stats().in_use, 0);
    assert!(pool.get().is_some());
}

#[monoio::test]
async fn grow_keeps_buffers() {
    let Some(pool) = pool(2, 512) else { return };
    let a = filled(&pool, b"first");
    let b = pool.get().unwrap();
    assert!(pool.get().is_none());

    pool.grow(3).unwrap();
    assert_eq!(pool.capacity(), 5);
    assert_eq!(pool.index_ranges().len(), 2);
    let more: Vec<_> = (0..3).map(|_| pool.get().unwrap()).collect();
    assert!(pool.get().is_none());
    let indexes: HashSet<u16> = [&a, &b]
        .into_iter()
        .chain(&more)
        .map(|b| b.buf_index())
        .collect();
    assert_eq!(indexes.len(), 5);
    assert_eq!(&a[..], b"first");

    // The grown buffers work with the fixed ops.
    let tmp = NamedTempFile::new().unwrap();
    let file = File::create(tmp.path()).await.unwrap();
    let mut grown = more.into_iter().next().unwrap();
    grown.extend_from_slice(b"grown");
    let (res, _) = file.write_fixed_at(grown, 0).await;
    assert_eq!(res.unwrap(), 5);
    let (res, _) = file.write_fixed_at(a, 5).await;
    assert_eq!(res.unwrap(), 5);
    assert_eq!(std::fs::read(tmp.path()).unwrap(), b"grownfirst");
    assert_eq!(pool.stats().capacity, 5);
}

#[monoio::test]
async fn two_pools_do_not_overlap() {
    let Some(p1) = pool(3, 256) else { return };
    let p2 = FixedBufPool::new(4, 128).unwrap();
    assert_eq!(p1.table_slots(), p2.table_slots());
    let i1: HashSet<u16> = p1.index_ranges().into_iter().flatten().collect();
    let i2: HashSet<u16> = p2.index_ranges().into_iter().flatten().collect();
    assert_eq!((i1.len(), i2.len()), (3, 4));
    assert!(i1.is_disjoint(&i2));

    // Each index maps to its own pool's memory in the kernel.
    let tmp = NamedTempFile::new().unwrap();
    let file = File::create(tmp.path()).await.unwrap();
    let (r1, _) = file.write_fixed_at(filled(&p1, b"AAAA"), 0).await;
    let (r2, _) = file.write_fixed_at(filled(&p2, b"BBBB"), 4).await;
    assert_eq!((r1.unwrap(), r2.unwrap()), (4, 4));
    assert_eq!(std::fs::read(tmp.path()).unwrap(), b"AAAABBBB");

    // A dropped pool's slots are reused.
    drop(p1);
    let p3 = FixedBufPool::new(3, 64).unwrap();
    let i3: HashSet<u16> = p3.index_ranges().into_iter().flatten().collect();
    assert_eq!(i3, i1);
}

#[monoio::test]
async fn table_full_and_released() {
    let Some(pool) = pool_with_table(3, 64, 4) else {
        return;
    };
    assert_eq!(pool.table_slots(), 4);
    let err = pool.grow(2).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOSPC));
    assert_eq!(pool.capacity(), 3);
    pool.grow(1).unwrap();

    // The buffer keeps the pool's slots after the pool handle is gone.
    let buf = filled(&pool, b"kept");
    drop(pool);
    assert_eq!(&buf[..], b"kept");
    let err = FixedBufPool::new(1, 64).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOSPC));

    // Last buffer gone: slots cleared and the table unregistered; a new one can be registered.
    drop(buf);
    let pool = FixedBufPool::with_table_size(8, 64, 8).unwrap();
    assert_eq!(pool.table_slots(), 8);
}

// Without `async-cancel` a dropped op is never cancelled, so its buffer is never released.
#[cfg(feature = "async-cancel")]
#[monoio::test(timer_enabled = true)]
async fn drop_pool_with_inflight_op() {
    let Some(pool) = pool_with_table(1, 4096, 1) else {
        return;
    };
    let (client, server) = tcp_pair().await;
    let buf = pool.get().unwrap();
    drop(pool);

    // Nothing to read: the op stays in flight, holding the only buffer.
    let fut = server.recv_fixed_op(buf);
    monoio::time::sleep(std::time::Duration::from_millis(10)).await;
    drop(fut);

    // The driver keeps the buffer until the (cancelled) op completes; then the pool is freed and
    // its table slot (the only one) can be used again.
    let mut again = None;
    for _ in 0..100 {
        match FixedBufPool::with_table_size(1, 4096, 1) {
            Ok(p) => {
                again = Some(p);
                break;
            }
            Err(e) => assert_eq!(e.raw_os_error(), Some(libc::ENOSPC)),
        }
        monoio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let pool = again.expect("the in-flight op never released its buffer");

    let (res, _) = client.send_fixed(filled(&pool, b"x")).await;
    assert_eq!(res.unwrap(), 1);
    let (res, got) = server.recv_fixed(pool.get().unwrap()).await;
    assert_eq!((res.unwrap(), &got[..]), (1, &b"x"[..]));
}

#[monoio::test]
async fn ordinary_ops_accept_fixed_bufs() {
    let Some(pool) = pool(4, 1024) else { return };
    let (mut client, mut server) = tcp_pair().await;

    // `FixedBuf` and `FrozenFixedBuf` are `IoBuf`s.
    let (res, _) = client.write_all(filled(&pool, b"plain ")).await;
    res.unwrap();
    let (res, _) = client
        .write_all(filled(&pool, b"frozen!").freeze().slice(..6))
        .await;
    res.unwrap();

    // And `FixedBuf` is an `IoBufMut`.
    let mut buf = pool.get().unwrap();
    buf.set_len(0);
    let (res, buf) = server.read_exact(buf.slice_mut(0..12)).await;
    res.unwrap();
    assert_eq!(&buf.into_inner()[..], b"plain frozen");

    let tmp = NamedTempFile::new().unwrap();
    let file = File::create(tmp.path()).await.unwrap();
    let (res, _) = file.write_all_at(filled(&pool, b"file"), 0).await;
    res.unwrap();
    assert_eq!(std::fs::read(tmp.path()).unwrap(), b"file");
}

#[test]
fn new_in_and_other_ring_fallback() {
    let mut rt_a = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    let pool = match FixedBufPool::new_in(rt_a.driver(), 2, 256) {
        Ok(pool) => pool,
        Err(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) => return,
        Err(e) => panic!("{e}"),
    };
    let mut tmp = NamedTempFile::new().unwrap();
    tmp.write_all(b"from file").unwrap();

    rt_a.block_on(async {
        let file = File::open(tmp.path()).await.unwrap();
        let (res, buf) = file.read_fixed_at(pool.get().unwrap(), 0).await;
        assert_eq!(res.unwrap(), 9);
        assert_eq!(&buf[..], b"from file");
    });

    // On another ring the buffer index means nothing: the ops fall back to plain read/write.
    let mut rt_b = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
    rt_b.block_on(async {
        let file = File::open(tmp.path()).await.unwrap();
        let (res, buf) = file.read_fixed_at(pool.get().unwrap(), 5).await;
        assert_eq!(res.unwrap(), 4);
        assert_eq!(&buf[..], b"file");
    });
    drop(pool);
    drop(rt_a);
}
