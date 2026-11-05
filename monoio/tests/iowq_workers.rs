//! `RuntimeBuilder::iowq_max_workers`: the runtime builds and does file IO, with and without the
//! timer.

use std::io::Write;

use monoio::{fs::File, IoUringDriver, RuntimeBuilder};
use tempfile::NamedTempFile;

async fn file_io() {
    let mut tmp = NamedTempFile::new().unwrap();
    tmp.write_all(b"hello iowq").unwrap();
    let file = File::open(tmp.path()).await.unwrap();
    let (res, buf) = file.read_at(vec![0u8; 32], 0).await;
    let n = res.unwrap();
    assert_eq!(&buf[..n], b"hello iowq");

    let out = File::create(tmp.path().with_extension("out"))
        .await
        .unwrap();
    let (res, _) = out.write_at(b"written".to_vec(), 0).await;
    assert_eq!(res.unwrap(), 7);
    out.sync_all().await.unwrap();
    out.close().await.unwrap();
    std::fs::remove_file(tmp.path().with_extension("out")).unwrap();
}

#[test]
fn iowq_max_workers_plain() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .iowq_max_workers(2, 2)
        .build()
        .unwrap();
    rt.block_on(file_io());
    // Registering zeros changes nothing and reads back the limits.
    let mut max = [0u32; 2];
    unsafe {
        rt.driver()
            .with_ring(|r| r.submitter().register_iowq_max_workers(&mut max))
            .unwrap()
    };
    assert_eq!(max, [2, 2]);
}

#[test]
fn iowq_max_workers_timer() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .iowq_max_workers(2, 2)
        .enable_timer()
        .build()
        .unwrap();
    rt.block_on(async {
        file_io().await;
        monoio::time::sleep(std::time::Duration::from_millis(1)).await;
    });
}

#[test]
fn iowq_max_workers_zero_keeps_limits() {
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .enable_timer()
        .iowq_max_workers(0, 1)
        .build()
        .unwrap();
    rt.block_on(file_io());
}
