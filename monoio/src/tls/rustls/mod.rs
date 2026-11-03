//! TLS streams based on [rustls](https://docs.rs/rustls).

// The rustls crate itself (`ServerConfig`, `ClientConfig`, `pki_types`, ...), so users do not need
// a dependency of their own. Pick a crypto provider with `tls-rustls-ring` or
// `tls-rustls-aws-lc-rs`.
pub use ::rustls;

mod client;
mod error;
mod server;
mod stream;

pub use client::{
    TlsConnector, TlsStream as ClientTlsStream, TlsStreamReadHalf as ClientTlsStreamReadHalf,
    TlsStreamWriteHalf as ClientTlsStreamWriteHalf,
};
pub use error::TlsError;
pub use server::{
    TlsAcceptor, TlsStream as ServerTlsStream, TlsStreamReadHalf as ServerTlsStreamReadHalf,
    TlsStreamWriteHalf as ServerTlsStreamWriteHalf,
};
