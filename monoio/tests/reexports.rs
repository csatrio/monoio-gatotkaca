//! Everything needed to use the raw ring, buffer rings and TLS is reachable through this crate,
//! so dependents need no `io_uring`, `libc`, `rustls` or `native-tls` dependency of their own.

#[test]
fn io_uring_and_libc() {
    use monoio::{
        io_uring::{opcode, squeue, types, Builder, IoUring},
        uring::{libc, BufRing, Cqe, Probe, SelectedBuf},
    };

    // Names used by the raw ring, hook and buffer-ring APIs.
    let _: Option<(IoUring, Probe, Cqe, BufRing, SelectedBuf)> = None;
    let _: Option<Builder> = None;
    let entry: squeue::Entry = opcode::Recv::new(types::Fd(0), std::ptr::null_mut(), 0)
        .buf_group(1)
        .build()
        .flags(squeue::Flags::BUFFER_SELECT);
    let _ = entry;
    assert_eq!(libc::ENOBUFS, 105);
    let _: monoio::IoUring = monoio::io_uring::IoUring::new(8).unwrap();
}

#[test]
fn driver_types() {
    // Naming the runtime types of a runtime with the timer enabled.
    fn _name_them(_: Option<monoio::Runtime<monoio::time::TimeDriver<monoio::IoUringDriver>>>) {}
    let _ = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new().attach_wq(-1);
}

#[cfg(feature = "tls-rustls")]
#[test]
fn rustls() {
    use monoio::tls::rustls::{
        rustls::{pki_types::ServerName, ClientConfig, RootCertStore, ServerConfig},
        TlsAcceptor, TlsConnector,
    };

    let _: Option<(
        ClientConfig,
        ServerConfig,
        RootCertStore,
        ServerName<'static>,
    )> = None;
    let _: Option<(TlsAcceptor, TlsConnector)> = None;
}

#[cfg(feature = "tls-rustls-ring")]
#[test]
fn rustls_ring_provider_comes_with_the_feature() {
    use monoio::tls::rustls::rustls::{ClientConfig, RootCertStore};

    // Panics without a crypto provider.
    let config = ClientConfig::builder()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    let _ = monoio::tls::rustls::TlsConnector::from(config);
}

#[cfg(feature = "tls-native")]
#[test]
fn native_tls() {
    use monoio::tls::native::{
        native_tls::{Certificate, Identity, TlsAcceptor as NativeAcceptor},
        TlsAcceptor, TlsConnector,
    };

    let _: Option<(Certificate, Identity, NativeAcceptor)> = None;
    let _: Option<(TlsAcceptor, TlsConnector)> = None;
}
