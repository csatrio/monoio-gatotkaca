//! A TLS (native-tls) client for the `tls_rustls_server` / `tls_native_server` examples.

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    net::TcpStream,
    tls::native::TlsConnector,
};
use native_tls::Certificate;

#[monoio::main]
async fn main() {
    let ca = Certificate::from_pem(include_bytes!("../monoio/tests/certs/rootCA.crt"))
        .expect("invalid CA certificate");
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(ca)
        .build()
        .unwrap();
    let connector = TlsConnector::from(connector);

    let stream = TcpStream::connect("127.0.0.1:50443").await.unwrap();
    println!("127.0.0.1:50443 connected");

    let mut stream = connector.connect("monoio.rs", stream).await.unwrap();
    println!("handshake success");

    let data = "hello world";
    stream.write_all(data).await.0.expect("unable to send data");
    println!("send data: {data}");
    let (res, buf) = stream.read_exact(vec![0; data.len()]).await;
    res.expect("unable to recv data");
    println!(
        "recv data: {}",
        String::from_utf8(buf).expect("invalid data")
    );
    let _ = stream.shutdown().await;
}
