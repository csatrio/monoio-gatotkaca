#![allow(dead_code)]

use std::{io, net::SocketAddr};

use monoio::{
    io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
};

pub const DOMAIN: &str = "monoio.rs";
pub const ROOT_CA: &[u8] = include_bytes!("../certs/rootCA.crt");
pub const SERVER_CERT: &[u8] = include_bytes!("../certs/server.crt");
pub const SERVER_KEY_PKCS1: &[u8] = include_bytes!("../certs/server.key");
pub const SERVER_KEY_PKCS8: &[u8] = include_bytes!("../certs/server.pkcs8");

/// Deterministic payload that is not periodic in the TLS record size.
pub fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Echo everything until EOF.
pub async fn echo<S: AsyncReadRent + AsyncWriteRent>(mut stream: S) -> io::Result<()> {
    let mut buf = Vec::with_capacity(16 * 1024);
    loop {
        let (res, b) = stream.read(buf).await;
        buf = b;
        if res? == 0 {
            return Ok(());
        }
        let (res, b) = stream.write_all(buf).await;
        buf = b;
        res?;
        buf.clear();
    }
}

/// Bind on a free local port and run `handle` for every accepted connection on a new task.
pub fn spawn_server<F, Fut>(handle: F) -> SocketAddr
where
    F: Fn(TcpStream) -> Fut + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    monoio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            monoio::spawn(handle(stream));
        }
    });
    addr
}
