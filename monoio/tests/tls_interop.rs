#![cfg(all(feature = "tls-rustls", feature = "tls-native"))]

mod tls_common;

use std::sync::Arc;

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRentExt},
    net::TcpStream,
    tls::{native, rustls as monoio_rustls},
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName};
use tls_common::*;

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

fn native_identity() -> native_tls::Identity {
    let mut chain = SERVER_CERT.to_vec();
    chain.extend_from_slice(ROOT_CA);
    native_tls::Identity::from_pkcs8(&chain, SERVER_KEY_PKCS8).unwrap()
}

#[monoio::test]
async fn rustls_client_native_server() {
    let acceptor =
        native::TlsAcceptor::from(native_tls::TlsAcceptor::new(native_identity()).unwrap());
    let addr = spawn_server(move |stream| {
        let acceptor = acceptor.clone();
        async move {
            if let Ok(tls) = acceptor.accept(stream).await {
                let _ = echo(tls).await;
            }
        }
    });

    let tcp = TcpStream::connect(addr).await.unwrap();
    let mut stream = monoio_rustls::TlsConnector::from(rustls_client_config())
        .connect(ServerName::try_from(DOMAIN).unwrap(), tcp)
        .await
        .unwrap();
    let (res, _) = stream.write_all("rustls to native").await;
    res.unwrap();
    let (res, buf) = stream.read_exact(vec![0; 16]).await;
    res.unwrap();
    assert_eq!(buf, b"rustls to native");
}

#[monoio::test]
async fn native_client_rustls_server() {
    let acceptor = monoio_rustls::TlsAcceptor::from(Arc::new(rustls_server_config()));
    let addr = spawn_server(move |stream| {
        let acceptor = acceptor.clone();
        async move {
            if let Ok(tls) = acceptor.accept(stream).await {
                let _ = echo(tls).await;
            }
        }
    });

    let mut builder = native_tls::TlsConnector::builder();
    builder.add_root_certificate(native_tls::Certificate::from_pem(ROOT_CA).unwrap());
    let connector = native::TlsConnector::from(builder.build().unwrap());
    let tcp = TcpStream::connect(addr).await.unwrap();
    let mut stream = connector.connect(DOMAIN, tcp).await.unwrap();
    let (res, _) = stream.write_all("native to rustls").await;
    res.unwrap();
    let (res, buf) = stream.read_exact(vec![0; 16]).await;
    res.unwrap();
    assert_eq!(buf, b"native to rustls");
}
