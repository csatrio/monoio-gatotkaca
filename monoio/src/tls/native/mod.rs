//! TLS streams based on the platform TLS library via
//! [native-tls](https://docs.rs/native-tls) (OpenSSL on Linux).

// The native-tls crate itself (`Identity`, `Certificate`, builders, ...), so users do not need a
// dependency of their own.
pub use ::native_tls;

mod client;
mod error;
mod server;
mod stream;
mod utils;

pub use client::TlsConnector;
pub use error::TlsError;
pub use server::TlsAcceptor;
pub use stream::TlsStream;
