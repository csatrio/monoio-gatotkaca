use std::io;

use thiserror::Error;

/// Error of a TLS handshake or operation.
#[derive(Error, Debug)]
pub enum TlsError {
    /// The underlying IO failed.
    #[error("io error")]
    Io(#[from] std::io::Error),
    /// rustls reported an error.
    #[error("rustls error")]
    Rustls(#[from] rustls::Error),
}

impl From<TlsError> for io::Error {
    fn from(e: TlsError) -> Self {
        match e {
            TlsError::Io(e) => e,
            TlsError::Rustls(e) => io::Error::other(e),
        }
    }
}
