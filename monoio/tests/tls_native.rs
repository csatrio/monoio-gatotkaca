#![cfg(feature = "tls-native")]

mod tls_common;

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRentExt, Splitable},
    net::TcpStream,
    tls::native::{TlsAcceptor, TlsConnector, TlsError, TlsStream},
};
use native_tls::{Certificate, Identity};
use tls_common::*;

fn start_echo_server() -> std::net::SocketAddr {
    let mut chain = SERVER_CERT.to_vec();
    chain.extend_from_slice(ROOT_CA);
    let identity = Identity::from_pkcs8(&chain, SERVER_KEY_PKCS8).unwrap();
    let acceptor = TlsAcceptor::from(native_tls::TlsAcceptor::new(identity).unwrap());
    spawn_server(move |stream| {
        let acceptor = acceptor.clone();
        async move {
            if let Ok(tls) = acceptor.accept(stream).await {
                let _ = echo(tls).await;
            }
        }
    })
}

fn connector(trust_ca: bool) -> TlsConnector {
    let mut builder = native_tls::TlsConnector::builder();
    if trust_ca {
        builder.add_root_certificate(Certificate::from_pem(ROOT_CA).unwrap());
    }
    TlsConnector::from(builder.build().unwrap())
}

async fn connect(
    addr: std::net::SocketAddr,
    connector: &TlsConnector,
    domain: &str,
) -> Result<TlsStream<TcpStream>, TlsError> {
    let tcp = TcpStream::connect(addr).await.unwrap();
    connector.connect(domain, tcp).await
}

#[monoio::test]
async fn echo_roundtrip() {
    let addr = start_echo_server();
    let mut stream = connect(addr, &connector(true), DOMAIN).await.unwrap();

    let (res, _) = stream.write_all("hello tls").await;
    res.unwrap();
    let (res, buf) = stream.read_exact(vec![0; 9]).await;
    res.unwrap();
    assert_eq!(buf, b"hello tls");
}

#[monoio::test]
async fn large_payload_with_split() {
    let addr = start_echo_server();
    let stream = connect(addr, &connector(true), DOMAIN).await.unwrap();
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
async fn custom_buffer_sizes() {
    let addr = start_echo_server();
    let connector = connector(true)
        .read_buffer(Some(4096))
        .write_buffer(Some(4096));
    let mut stream = connect(addr, &connector, DOMAIN).await.unwrap();

    let data = payload(64 * 1024);
    let (res, _) = stream.write_all(data.clone()).await;
    res.unwrap();
    let (res, got) = stream.read_exact(vec![0; data.len()]).await;
    res.unwrap();
    assert!(got == data, "echoed payload differs");
}

#[monoio::test]
async fn untrusted_ca_is_rejected() {
    let addr = start_echo_server();
    let err = connect(addr, &connector(false), DOMAIN)
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("native-tls"), "{err:?}");
    assert!(
        format!("{err:?}").contains("self-signed certificate"),
        "{err:?}"
    );
}

#[monoio::test]
async fn wrong_server_name_is_rejected() {
    let addr = start_echo_server();
    let err = connect(addr, &connector(true), "not-monoio.example")
        .await
        .err()
        .unwrap();
    assert!(format!("{err:?}").contains("hostname mismatch"), "{err:?}");
}
