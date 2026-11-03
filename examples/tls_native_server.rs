//! An echo server with TLS (native-tls).

use std::io;

use monoio::tls::native::native_tls::{self, Identity};
use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
    tls::native::TlsAcceptor,
};

#[monoio::main]
async fn main() {
    let mut chain = include_bytes!("../monoio/tests/certs/server.crt").to_vec();
    chain.extend_from_slice(include_bytes!("../monoio/tests/certs/rootCA.crt"));
    let identity =
        Identity::from_pkcs8(&chain, include_bytes!("../monoio/tests/certs/server.pkcs8"))
            .expect("invalid identity");
    let acceptor = TlsAcceptor::from(native_tls::TlsAcceptor::new(identity).unwrap());

    let listener = TcpListener::bind("127.0.0.1:50443").expect("unable to listen 127.0.0.1:50443");
    while let Ok((stream, addr)) = listener.accept().await {
        println!("Accepted from {addr}, will accept tls handshake");
        let acceptor = acceptor.clone();
        monoio::spawn(async move {
            let e = process_raw_stream(stream, acceptor).await;
            println!("Relay finished {e:?}");
        });
    }
}

async fn process_raw_stream(stream: TcpStream, acceptor: TlsAcceptor) -> io::Result<()> {
    let mut tls_stream = acceptor.accept(stream).await?;
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
