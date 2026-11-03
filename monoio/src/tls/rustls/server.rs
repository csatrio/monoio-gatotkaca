use std::sync::Arc;

use crate::io::{AsyncReadRent, AsyncWriteRent, OwnedReadHalf, OwnedWriteHalf};
use rustls::{ServerConfig, ServerConnection};

use super::{stream::Stream, TlsError};

/// A wrapper around an underlying raw stream which implements the TLS protocol.
pub type TlsStream<IO> = Stream<IO, ServerConnection>;
/// TlsStream for read only.
pub type TlsStreamReadHalf<IO> = OwnedReadHalf<TlsStream<IO>>;
/// TlsStream for write only.
pub type TlsStreamWriteHalf<IO> = OwnedWriteHalf<TlsStream<IO>>;

/// A wrapper around a `rustls::ServerConfig`, providing an async `accept` method.
#[derive(Clone)]
pub struct TlsAcceptor {
    inner: Arc<ServerConfig>,
}

impl From<Arc<ServerConfig>> for TlsAcceptor {
    fn from(inner: Arc<ServerConfig>) -> TlsAcceptor {
        TlsAcceptor { inner }
    }
}

impl From<ServerConfig> for TlsAcceptor {
    fn from(inner: ServerConfig) -> TlsAcceptor {
        TlsAcceptor {
            inner: Arc::new(inner),
        }
    }
}

impl TlsAcceptor {
    /// Perform the server side TLS handshake over `stream`.
    pub async fn accept<IO>(&self, stream: IO) -> Result<TlsStream<IO>, TlsError>
    where
        IO: AsyncReadRent + AsyncWriteRent,
    {
        let session = ServerConnection::new(self.inner.clone())?;
        let mut stream = Stream::new(stream, session);
        stream.handshake().await?;
        Ok(stream)
    }
}
