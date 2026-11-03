//! TLS streams based on [rustls](https://docs.rs/rustls).

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
