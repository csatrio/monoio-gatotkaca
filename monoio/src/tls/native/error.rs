use std::io;

use thiserror::Error;

/// Error of a TLS handshake or operation.
#[derive(Error, Debug)]
pub enum TlsError {
    /// The underlying IO failed.
    #[error("io error")]
    Io(#[from] std::io::Error),
    /// native-tls reported an error.
    #[error("native-tls error")]
    NativeTls(#[from] native_tls::Error),
}

impl From<TlsError> for io::Error {
    fn from(e: TlsError) -> Self {
        match e {
            TlsError::Io(e) => e,
            TlsError::NativeTls(e) => io::Error::other(e),
        }
    }
}
