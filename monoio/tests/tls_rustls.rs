#![cfg(feature = "tls-rustls")]

mod tls_common;

use std::sync::Arc;

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRentExt, Splitable},
    net::TcpStream,
    tls::rustls::{ClientTlsStream, TlsAcceptor, TlsConnector},
};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName},
    ClientConfig, RootCertStore, ServerConfig,
};
use tls_common::*;

fn server_config(alpn: &[&[u8]]) -> Arc<ServerConfig> {
    let chain = CertificateDer::pem_slice_iter(SERVER_CERT)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_slice(SERVER_KEY_PKCS1).unwrap();
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

fn client_config(trust_ca: bool, alpn: &[&[u8]]) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    if trust_ca {
        roots
            .add(CertificateDer::from_pem_slice(ROOT_CA).unwrap())
            .unwrap();
    }
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

fn start_echo_server(alpn: &[&[u8]]) -> std::net::SocketAddr {
    let acceptor = TlsAcceptor::from(server_config(alpn));
    spawn_server(move |stream| {
        let acceptor = acceptor.clone();
        async move {
            if let Ok(tls) = acceptor.accept(stream).await {
                let _ = echo(tls).await;
            }
        }
    })
}

async fn connect(
    addr: std::net::SocketAddr,
    config: Arc<ClientConfig>,
    domain: &'static str,
) -> Result<ClientTlsStream<TcpStream>, monoio::tls::rustls::TlsError> {
    let tcp = TcpStream::connect(addr).await.unwrap();
    TlsConnector::from(config)
        .connect(ServerName::try_from(domain).unwrap(), tcp)
        .await
}

#[monoio::test]
async fn echo_roundtrip() {
    let addr = start_echo_server(&[]);
    let mut stream = connect(addr, client_config(true, &[]), DOMAIN)
        .await
        .unwrap();

    let (res, _) = stream.write_all("hello tls").await;
    res.unwrap();
    let (res, buf) = stream.read_exact(vec![0; 9]).await;
    res.unwrap();
    assert_eq!(buf, b"hello tls");
}

#[monoio::test]
async fn large_payload_with_split() {
    let addr = start_echo_server(&[]);
    let stream = connect(addr, client_config(true, &[]), DOMAIN)
        .await
        .unwrap();
    let (mut reader, mut writer) = stream.into_split();

    let data = payload(1 << 20);
    let expected = data.clone();
    let write_task = monoio::spawn(async move {
        let (res, _) = writer.write_all(data).await;
        res.unwrap();
    });
    let (res, got) = reader.read_exact(vec![0; expected.len()]).await;
    res.unwrap();
    write_task.await;
    assert!(got == expected, "echoed payload differs");
}

#[monoio::test]
async fn alpn_is_negotiated() {
    let addr = start_echo_server(&[b"h2", b"http/1.1"]);
    let stream = connect(addr, client_config(true, &[b"http/1.1"]), DOMAIN)
        .await
        .unwrap();
    assert_eq!(stream.alpn_protocol().as_deref(), Some(&b"http/1.1"[..]));
}

#[monoio::test]
async fn untrusted_ca_is_rejected() {
    let addr = start_echo_server(&[]);
    let err = connect(addr, client_config(false, &[]), DOMAIN)
        .await
        .err()
        .unwrap();
    assert!(format!("{err:?}").contains("UnknownIssuer"), "{err:?}");
}

#[monoio::test]
async fn wrong_server_name_is_rejected() {
    let addr = start_echo_server(&[]);
    let err = connect(addr, client_config(true, &[]), "not-monoio.example")
        .await
        .err()
        .unwrap();
    assert!(format!("{err:?}").contains("NotValidForName"), "{err:?}");
}
