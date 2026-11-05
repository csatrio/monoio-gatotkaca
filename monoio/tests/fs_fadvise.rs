//! `File::fadvise` (`IORING_OP_FADVISE`).

use std::{io::Write, os::fd::FromRawFd};

use monoio::{
    fs::{Advice, File},
    uring::libc,
};
use tempfile::NamedTempFile;

fn data_file() -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&vec![7u8; 256 * 1024]).unwrap();
    f.as_file_mut().sync_data().unwrap();
    f
}

#[monoio::test_all]
async fn every_advice_succeeds() {
    let tmp = data_file();
    let file = File::open(tmp.path()).await.unwrap();
    for advice in [
        Advice::Normal,
        Advice::Sequential,
        Advice::Random,
        Advice::NoReuse,
        Advice::WillNeed,
        Advice::DontNeed,
    ] {
        file.fadvise(0, 0, advice).await.unwrap();
        file.fadvise(4096, 64 * 1024, advice).await.unwrap();
    }
    file.fadvise_raw(0, 0, libc::POSIX_FADV_SEQUENTIAL)
        .await
        .unwrap();

    // Hints never change what reads return.
    let (res, buf) = file.read_at(vec![0u8; 4096], 8192).await;
    assert_eq!(res.unwrap(), 4096);
    assert!(buf.iter().all(|&b| b == 7));
    file.close().await.unwrap();
}

#[monoio::test_all]
async fn bad_input_errors() {
    let tmp = data_file();
    let file = File::open(tmp.path()).await.unwrap();

    let err = file.fadvise(0, u32::MAX as u64 + 1, Advice::Random).await;
    assert_eq!(err.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

    let err = file.fadvise_raw(0, 0, 1000).await.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
}

#[monoio::test_all]
async fn pipe_is_espipe() {
    let mut fds = [0; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    let rx = unsafe { std::fs::File::from_raw_fd(fds[0]) };
    let tx = unsafe { std::fs::File::from_raw_fd(fds[1]) };
    let file = File::from_std(rx).unwrap();
    let err = file.fadvise(0, 0, Advice::Sequential).await.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ESPIPE));
    drop(tx);
}
