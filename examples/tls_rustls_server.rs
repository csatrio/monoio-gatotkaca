//! An echo server with TLS (rustls).
//!
//! Run it, then in another shell run the `tls_rustls_client` or `tls_native_client` example, or:
//! `curl --resolve monoio.rs:50443:127.0.0.1 --cacert monoio/tests/certs/rootCA.crt https://monoio.rs:50443`

use std::io;

use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
    tls::rustls::TlsAcceptor,
};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    ServerConfig,
};

#[monoio::main]
async fn main() {
    let chain = CertificateDer::pem_slice_iter(include_bytes!("../monoio/tests/certs/server.crt"))
        .collect::<Result<Vec<_>, _>>()
        .expect("invalid certificate");
    let key = PrivateKeyDer::from_pem_slice(include_bytes!("../monoio/tests/certs/server.key"))
        .expect("invalid private key");
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .expect("invalid cert chain or key");
    let tls_acceptor = TlsAcceptor::from(config);

    let listener = TcpListener::bind("127.0.0.1:50443").expect("unable to listen 127.0.0.1:50443");
    while let Ok((stream, addr)) = listener.accept().await {
        println!("Accepted from {addr}, will accept tls handshake");
        let tls_acceptor = tls_acceptor.clone();
        monoio::spawn(async move {
            let e = process_raw_stream(stream, tls_acceptor).await;
            println!("Relay finished {e:?}");
        });
    }
}

async fn process_raw_stream(stream: TcpStream, tls_acceptor: TlsAcceptor) -> io::Result<()> {
    let mut tls_stream = tls_acceptor.accept(stream).await?;
    println!("Handshake finished, will relay data");

    let mut n = 0;
    let mut buf = Vec::with_capacity(8 * 1024);
    loop {
        let (res, b) = tls_stream.read(buf).await;
        buf = b;
        if res? == 0 {
            break;
        }
        let (res, b) = tls_stream.write_all(buf).await;
        buf = b;
        n += res?;
    }

    println!("Relay finished normally, {n} bytes processed");
    Ok(())
}
