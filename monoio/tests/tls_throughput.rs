#![cfg(all(feature = "tls-rustls", feature = "tls-native"))]
//! Bulk transfer, vectored writes, write coalescing and buffer options of both TLS backends,
//! plus the number of socket writes the TLS layer makes.

mod tls_common;

use std::{cell::Cell, rc::Rc};

use monoio::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut, VecBuf},
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt, Split, Splitable},
    net::TcpStream,
    tls::{native, rustls as monoio_rustls},
    BufResult,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName};
use tls_common::*;

#[derive(Default)]
struct Stats {
    writes: Cell<usize>,
    reads: Cell<usize>,
}

/// Forwards to a `TcpStream` and counts the underlying read/write calls.
struct Counting {
    inner: TcpStream,
    stats: Rc<Stats>,
}

// SAFETY: forwards to `TcpStream`, whose read and write sides are independent.
unsafe impl Split for Counting {}

impl AsyncReadRent for Counting {
    async fn read<T: IoBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        self.stats.reads.set(self.stats.reads.get() + 1);
        self.inner.read(buf).await
    }
    async fn readv<T: IoVecBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        self.stats.reads.set(self.stats.reads.get() + 1);
        self.inner.readv(buf).await
    }
}

impl AsyncWriteRent for Counting {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        self.stats.writes.set(self.stats.writes.get() + 1);
        self.inner.write(buf).await
    }
    async fn writev<T: IoVecBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        self.stats.writes.set(self.stats.writes.get() + 1);
        self.inner.writev(buf).await
    }
    async fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush().await
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.inner.shutdown().await
    }
}

fn rustls_server_config() -> rustls::ServerConfig {
    let chain = CertificateDer::pem_slice_iter(SERVER_CERT)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_slice(SERVER_KEY_PKCS1).unwrap();
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap()
}

fn rustls_client_config() -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ROOT_CA).unwrap())
        .unwrap();
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

fn native_acceptor() -> native::TlsAcceptor {
    let mut chain = SERVER_CERT.to_vec();
    chain.extend_from_slice(ROOT_CA);
    let identity = native_tls::Identity::from_pkcs8(&chain, SERVER_KEY_PKCS8).unwrap();
    native::TlsAcceptor::from(native_tls::TlsAcceptor::new(identity).unwrap())
}

fn native_connector() -> native::TlsConnector {
    native::TlsConnector::from(
        native_tls::TlsConnector::builder()
            .add_root_certificate(native_tls::Certificate::from_pem(ROOT_CA).unwrap())
            .build()
            .unwrap(),
    )
}

/// Echo until EOF; with `flush` (for write-coalescing streams) flush after every write.
async fn echo_flush<S: AsyncReadRent + AsyncWriteRent>(mut stream: S) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(32 * 1024);
    loop {
        let (res, b) = stream.read(buf).await;
        buf = b;
        if res? == 0 {
            return Ok(());
        }
        let (res, b) = stream.write_all(buf).await;
        buf = b;
        res?;
        stream.flush().await?;
        buf.clear();
    }
}

fn rustls_echo_server(acceptor: monoio_rustls::TlsAcceptor) -> std::net::SocketAddr {
    spawn_server(move |stream| {
        let acceptor = acceptor.clone();
        async move {
            if let Ok(tls) = acceptor.accept(stream).await {
                let _ = echo_flush(tls).await;
            }
        }
    })
}

fn native_echo_server(acceptor: native::TlsAcceptor) -> std::net::SocketAddr {
    spawn_server(move |stream| {
        let acceptor = acceptor.clone();
        async move {
            if let Ok(tls) = acceptor.accept(stream).await {
                let _ = echo_flush(tls).await;
            }
        }
    })
}

fn rustls_acceptor() -> monoio_rustls::TlsAcceptor {
    monoio_rustls::TlsAcceptor::from(rustls_server_config())
}

fn rustls_connector() -> monoio_rustls::TlsConnector {
    monoio_rustls::TlsConnector::from(rustls_client_config())
}

async fn counting_tcp(addr: std::net::SocketAddr) -> (Counting, Rc<Stats>) {
    let stats = Rc::new(Stats::default());
    let inner = TcpStream::connect(addr).await.unwrap();
    (
        Counting {
            inner,
            stats: stats.clone(),
        },
        stats,
    )
}

async fn rustls_client(
    connector: monoio_rustls::TlsConnector,
    addr: std::net::SocketAddr,
) -> (monoio_rustls::ClientTlsStream<Counting>, Rc<Stats>) {
    let (tcp, stats) = counting_tcp(addr).await;
    let stream = connector
        .connect(ServerName::try_from(DOMAIN).unwrap(), tcp)
        .await
        .unwrap();
    (stream, stats)
}

async fn native_client(
    connector: native::TlsConnector,
    addr: std::net::SocketAddr,
) -> (native::TlsStream<Counting>, Rc<Stats>) {
    let (tcp, stats) = counting_tcp(addr).await;
    let stream = connector.connect(DOMAIN, tcp).await.unwrap();
    (stream, stats)
}

/// Write `len` bytes with one `write_all`, return the number of underlying writes, then check
/// the echo. `len` must fit the socket buffers (no concurrent reader).
async fn measure<S: AsyncReadRent + AsyncWriteRent>(
    stream: &mut S,
    stats: &Stats,
    len: usize,
) -> usize {
    let data = payload(len);
    stats.writes.set(0);
    let (res, data) = stream.write_all(data).await;
    res.unwrap();
    let writes = stats.writes.get();
    let (res, got) = stream.read_exact(vec![0; len]).await;
    res.unwrap();
    assert!(got == data, "echoed payload differs");
    writes
}

/// Send `len` bytes and read the echo concurrently (split halves); return the writes made.
async fn transfer<S>(stream: S, stats: &Stats, len: usize) -> usize
where
    S: AsyncReadRent + AsyncWriteRent + Split + 'static,
{
    let (mut reader, mut writer) = stream.into_split();
    let data = payload(len);
    let expected = data.clone();
    stats.writes.set(0);
    let write_task = monoio::spawn(async move {
        let (res, _) = writer.write_all(data).await;
        res.unwrap();
        writer.flush().await.unwrap();
    });
    let (res, got) = reader.read_exact(vec![0; len]).await;
    res.unwrap();
    write_task.await;
    assert!(got == expected, "echoed payload differs");
    stats.writes.get()
}

const MIB: usize = 1 << 20;

// 64 KiB used to take 8 writes with rustls (16,384 + ~22 bytes per record) and 5 with
// native-tls; 1 MiB took 128 and 65.

#[monoio::test]
async fn rustls_write_counts() {
    let addr = rustls_echo_server(rustls_acceptor());
    let (mut stream, stats) = rustls_client(rustls_connector(), addr).await;
    let w64k = measure(&mut stream, &stats, 64 * 1024).await;
    let w1m = transfer(stream, &stats, MIB).await;
    println!("rustls: 64 KiB write_all -> {w64k} writes, 1 MiB -> {w1m} writes");
    assert!(w64k <= 2, "64 KiB took {w64k} writes");
    assert!(w1m <= 20, "1 MiB took {w1m} writes");
}

#[monoio::test]
async fn native_write_counts() {
    let addr = native_echo_server(native_acceptor());
    let (mut stream, stats) = native_client(native_connector(), addr).await;
    let w64k = measure(&mut stream, &stats, 64 * 1024).await;
    let w1m = transfer(stream, &stats, MIB).await;
    println!("native: 64 KiB write_all -> {w64k} writes, 1 MiB -> {w1m} writes");
    assert!(w64k <= 2, "64 KiB took {w64k} writes");
    assert!(w1m <= 20, "1 MiB took {w1m} writes");
}

#[monoio::test]
async fn rustls_1mib_both_directions() {
    // Server side with small buffers and coalescing, client side with large buffers.
    let acceptor = rustls_acceptor()
        .read_buffer(Some(20 * 1024))
        .write_buffer(Some(20 * 1024))
        .write_coalescing(true);
    let addr = rustls_echo_server(acceptor);
    let connector = rustls_connector()
        .read_buffer(Some(256 * 1024))
        .write_buffer(Some(256 * 1024));
    let (stream, stats) = rustls_client(connector, addr).await;
    let writes = transfer(stream, &stats, MIB).await;
    println!("rustls 256 KiB buffer: 1 MiB -> {writes} writes");
    assert!(writes <= 8, "1 MiB took {writes} writes");
}

#[monoio::test]
async fn native_1mib_both_directions() {
    let acceptor = native_acceptor()
        .read_buffer(Some(20 * 1024))
        .write_buffer(Some(20 * 1024))
        .write_coalescing(true);
    let addr = native_echo_server(acceptor);
    let connector = native_connector()
        .read_buffer(Some(256 * 1024))
        .write_buffer(Some(256 * 1024));
    let (stream, stats) = native_client(connector, addr).await;
    let writes = transfer(stream, &stats, MIB).await;
    println!("native 256 KiB buffer: 1 MiB -> {writes} writes");
    assert!(writes <= 8, "1 MiB took {writes} writes");
}

#[monoio::test]
async fn interop_1mib_both_directions() {
    let addr = native_echo_server(native_acceptor());
    let (stream, stats) = rustls_client(rustls_connector(), addr).await;
    transfer(stream, &stats, MIB).await;

    let addr = rustls_echo_server(rustls_acceptor());
    let (stream, stats) = native_client(native_connector(), addr).await;
    transfer(stream, &stats, MIB).await;
}

fn slices() -> (Vec<Vec<u8>>, Vec<u8>) {
    let parts: Vec<Vec<u8>> = [7usize, 1000, 0, 20_000, 3, 50_000]
        .iter()
        .enumerate()
        .map(|(i, len)| (0..*len).map(|j| (i * 31 + j) as u8).collect())
        .collect();
    let joined = parts.concat();
    (parts, joined)
}

async fn check_writev<S: AsyncReadRent + AsyncWriteRent>(stream: &mut S, stats: &Stats) -> usize {
    let (parts, joined) = slices();
    stats.writes.set(0);
    let (res, _) = stream.write_vectored_all(VecBuf::from(parts)).await;
    assert_eq!(res.unwrap(), joined.len());
    let writes = stats.writes.get();
    let (res, got) = stream.read_exact(vec![0; joined.len()]).await;
    res.unwrap();
    assert!(got == joined, "writev payload differs");

    // small slices: one record, one write
    stats.writes.set(0);
    let small = vec![b"GET / HTTP/1.1\r\n".to_vec(), b"Host: x\r\n\r\n".to_vec()];
    let (res, _) = stream.write_vectored_all(VecBuf::from(small)).await;
    assert_eq!(res.unwrap(), 27);
    assert_eq!(stats.writes.get(), 1);
    let (res, got) = stream.read_exact(vec![0; 27]).await;
    res.unwrap();
    assert_eq!(&got, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    writes
}

#[monoio::test]
async fn rustls_writev_all_slices() {
    let addr = rustls_echo_server(rustls_acceptor());
    let (mut stream, stats) = rustls_client(rustls_connector(), addr).await;
    let writes = check_writev(&mut stream, &stats).await;
    println!("rustls writev 71,010 bytes in 6 slices -> {writes} writes");
    assert!(writes <= 2, "{writes} writes");
}

#[monoio::test]
async fn native_writev_all_slices() {
    let addr = native_echo_server(native_acceptor());
    let (mut stream, stats) = native_client(native_connector(), addr).await;
    let writes = check_writev(&mut stream, &stats).await;
    println!("native writev 71,010 bytes in 6 slices -> {writes} writes");
    assert!(writes <= 3, "{writes} writes");
}

/// 1000 small writes are buffered; nothing is sent before `flush`.
async fn check_coalescing<S: AsyncReadRent + AsyncWriteRent>(
    stream: &mut S,
    stats: &Stats,
    max_writes: usize,
) {
    stats.writes.set(0);
    let mut expected = Vec::new();
    for i in 0..1000u32 {
        let msg = format!("msg {i};").into_bytes();
        expected.extend_from_slice(&msg);
        let (res, _) = stream.write_all(msg).await;
        res.unwrap();
    }
    let before_flush = stats.writes.get();
    stream.flush().await.unwrap();
    let total = stats.writes.get();
    println!(
        "coalescing: {} bytes in 1000 writes -> {before_flush} socket writes before flush, \
         {total} after",
        expected.len()
    );
    assert_eq!(before_flush, 0);
    assert!(total <= max_writes, "{total} writes");
    let (res, got) = stream.read_exact(vec![0; expected.len()]).await;
    res.unwrap();
    assert!(got == expected, "coalesced payload differs");

    // A large write in coalescing mode, then flush.
    let big = payload(200 * 1024);
    let (res, _) = stream.write_all(big.clone()).await;
    res.unwrap();
    stream.flush().await.unwrap();
    let (res, got) = stream.read_exact(vec![0; big.len()]).await;
    res.unwrap();
    assert!(got == big, "large coalesced payload differs");
}

#[monoio::test]
async fn rustls_write_coalescing() {
    let addr = rustls_echo_server(rustls_acceptor());
    let (mut stream, stats) = rustls_client(rustls_connector().write_coalescing(true), addr).await;
    assert!(stream.write_coalescing());
    check_coalescing(&mut stream, &stats, 1).await;

    // can be switched off again: writes are sent immediately
    stream.set_write_coalescing(false);
    stats.writes.set(0);
    let (res, _) = stream.write_all(b"now".to_vec()).await;
    res.unwrap();
    assert_eq!(stats.writes.get(), 1);
    let (res, got) = stream.read_exact(vec![0; 3]).await;
    res.unwrap();
    assert_eq!(&got, b"now");
}

#[monoio::test]
async fn native_write_coalescing() {
    let addr = native_echo_server(native_acceptor());
    let (mut stream, stats) = native_client(native_connector().write_coalescing(true), addr).await;
    assert!(stream.write_coalescing());
    // native-tls makes one record per write (~30 bytes on the wire each here), which still fits
    // the 64 KiB write buffer.
    check_coalescing(&mut stream, &stats, 2).await;
}

#[monoio::test]
async fn rustls_coalescing_shutdown_sends_pending() {
    let acceptor = rustls_acceptor();
    let (tx, rx) = local_sync::oneshot::channel::<Vec<u8>>();
    let tx = std::cell::RefCell::new(Some(tx));
    let addr = spawn_server(move |stream| {
        let acceptor = acceptor.clone();
        let tx = tx.borrow_mut().take();
        async move {
            let mut tls = acceptor.accept(stream).await.unwrap();
            let mut all = Vec::new();
            loop {
                let (res, buf) = tls.read(vec![0; 4096]).await;
                match res {
                    Ok(0) | Err(_) => break,
                    Ok(n) => all.extend_from_slice(&buf[..n]),
                }
            }
            let _ = tx.unwrap().send(all);
        }
    });
    let (mut stream, _) = rustls_client(rustls_connector().write_coalescing(true), addr).await;
    let (res, _) = stream.write_all(b"pending until shutdown".to_vec()).await;
    res.unwrap();
    stream.shutdown().await.unwrap();
    assert_eq!(rx.await.unwrap(), b"pending until shutdown");
}

#[monoio::test]
async fn tiny_buffers_clamp_up() {
    // rustls: a 1-byte write buffer is raised to one full record, so 64 KiB is 4 writes.
    let acceptor = rustls_acceptor().read_buffer(Some(1)).write_buffer(Some(1));
    let addr = rustls_echo_server(acceptor);
    let connector = rustls_connector()
        .read_buffer(Some(1))
        .write_buffer(Some(0));
    let (mut stream, stats) = rustls_client(connector, addr).await;
    let writes = measure(&mut stream, &stats, 64 * 1024).await;
    println!("rustls 18 KiB buffer: 64 KiB -> {writes} writes");
    assert_eq!(writes, 4);
    transfer(stream, &stats, MIB).await;

    let acceptor = native_acceptor().read_buffer(Some(1)).write_buffer(Some(1));
    let addr = native_echo_server(acceptor);
    let connector = native_connector()
        .read_buffer(Some(1))
        .write_buffer(Some(0));
    let (mut stream, stats) = native_client(connector, addr).await;
    let writes = measure(&mut stream, &stats, 64 * 1024).await;
    println!("native 18 KiB buffer: 64 KiB -> {writes} writes");
    assert!((4..=5).contains(&writes), "{writes} writes");
    transfer(stream, &stats, MIB).await;
}
