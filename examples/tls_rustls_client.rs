//! A TLS (rustls) client for the `tls_rustls_server` / `tls_native_server` examples.

use monoio::tls::rustls::rustls::{
    pki_types::{pem::PemObject, CertificateDer, ServerName},
    ClientConfig, RootCertStore,
};
use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    net::TcpStream,
    tls::rustls::TlsConnector,
};

#[monoio::main]
async fn main() {
    let mut root_store = RootCertStore::empty();
    root_store
        .add(
            CertificateDer::from_pem_slice(include_bytes!("../monoio/tests/certs/rootCA.crt"))
                .expect("invalid CA certificate"),
        )
        .expect("unable to trust self-signed CA");
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let connector = TlsConnector::from(config);
    let stream = TcpStream::connect("127.0.0.1:50443").await.unwrap();
    println!("127.0.0.1:50443 connected");

    let domain = ServerName::try_from("monoio.rs").unwrap();
    let mut stream = connector.connect(domain, stream).await.unwrap();
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
