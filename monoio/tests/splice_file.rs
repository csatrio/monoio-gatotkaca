//! File <-> pipe splice with explicit offsets (feature `splice`).
#![cfg(feature = "splice")]

use std::io::Write;

use monoio::{
    fs::{File, OpenOptions},
    io::{splice::SpliceDestination, AsyncReadRentExt},
    net::{unix::new_pipe, TcpListener, TcpStream},
};
use tempfile::NamedTempFile;

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 % 251) as u8).collect()
}

fn source(data: &[u8]) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(data).unwrap();
    f.as_file_mut().sync_data().unwrap();
    f
}

#[monoio::test_all]
async fn file_to_pipe_to_file() {
    let data = pattern(300 * 1024);
    let src_tmp = source(&data);
    let dst_tmp = NamedTempFile::new().unwrap();
    let src = File::open(src_tmp.path()).await.unwrap();
    let dst = OpenOptions::new()
        .write(true)
        .open(dst_tmp.path())
        .await
        .unwrap();
    let (rx, tx) = new_pipe().unwrap();

    // Copy src[1000..1000 + 200 KiB] to dst at offset 5.
    let (off, len, dst_off) = (1000u64, 200 * 1024u64, 5u64);
    let mut written = 0u64;
    let mut done = 0u64;
    while done < len {
        let chunk = (len - done).min(64 * 1024) as u32;
        let n = src.splice_to_pipe(&tx, off + done, chunk).await.unwrap();
        assert!(n > 0);
        let mut left = n;
        while left > 0 {
            let m = dst
                .splice_from_pipe(&rx, dst_off + written, left)
                .await
                .unwrap();
            written += m as u64;
            left -= m;
        }
        done += n as u64;
    }
    assert_eq!(written, len);
    dst.sync_all().await.unwrap();

    let got = std::fs::read(dst_tmp.path()).unwrap();
    assert_eq!(got.len() as u64, dst_off + len);
    assert!(got[..dst_off as usize].iter().all(|&b| b == 0));
    assert_eq!(
        &got[dst_off as usize..],
        &data[off as usize..(off + len) as usize]
    );

    // Past the end: 0 bytes.
    let n = src
        .splice_to_pipe(&tx, data.len() as u64 + 10, 4096)
        .await
        .unwrap();
    assert_eq!(n, 0);
    // Offset beyond i64::MAX is refused before submission.
    let err = src.splice_to_pipe(&tx, u64::MAX, 1).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[monoio::test_all]
async fn file_to_pipe_to_tcp() {
    let data = pattern(256 * 1024 + 123);
    let src_tmp = source(&data);
    let src = File::open(src_tmp.path()).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (off, len) = (777u64, 200 * 1024u64 + 9);
    let reader = monoio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        let (res, buf) = conn.read_exact(vec![0u8; len as usize]).await;
        res.unwrap();
        buf
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    let (mut rx, tx) = new_pipe().unwrap();
    // Alternate the two drain APIs: `SpliceDestination` on the socket and `Pipe::splice_to`.
    let mut done = 0u64;
    let mut flip = false;
    while done < len {
        let chunk = (len - done).min(64 * 1024) as u32;
        let n = src.splice_to_pipe(&tx, off + done, chunk).await.unwrap();
        assert!(n > 0);
        let mut left = n;
        while left > 0 {
            flip = !flip;
            left -= if flip {
                sock.splice_from_pipe(&mut rx, left).await.unwrap()
            } else {
                rx.splice_to(&mut sock, left).await.unwrap()
            };
        }
        done += n as u64;
    }
    let got = reader.await;
    assert_eq!(&got[..], &data[off as usize..(off + len) as usize]);
}

#[monoio::test_all]
async fn pipe_splice_from_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let writer = monoio::spawn(async move {
        use monoio::io::AsyncWriteRentExt;
        let (mut conn, _) = listener.accept().await.unwrap();
        conn.write_all(b"spliced via pipe".to_vec())
            .await
            .0
            .unwrap();
    });
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let (mut rx, tx) = new_pipe().unwrap();
    writer.await;
    let mut got = 0;
    while got < 16 {
        let n = tx.splice_from(&mut sock, 16 - got).await.unwrap();
        assert!(n > 0);
        got += n;
    }
    let (res, buf) = rx.read_exact(vec![0u8; 16]).await;
    res.unwrap();
    assert_eq!(&buf, b"spliced via pipe");
}
