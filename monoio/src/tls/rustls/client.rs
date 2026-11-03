use std::sync::Arc;

use crate::io::{AsyncReadRent, AsyncWriteRent, OwnedReadHalf, OwnedWriteHalf};
use rustls::{pki_types::ServerName, ClientConfig, ClientConnection};

use super::{stream::Stream, TlsError};

/// A wrapper around an underlying raw stream which implements the TLS protocol.
pub type TlsStream<IO> = Stream<IO, ClientConnection>;
/// TlsStream for read only.
pub type TlsStreamReadHalf<IO> = OwnedReadHalf<TlsStream<IO>>;
/// TlsStream for write only.
pub type TlsStreamWriteHalf<IO> = OwnedWriteHalf<TlsStream<IO>>;

/// A wrapper around a `rustls::ClientConfig`, providing an async `connect` method.
#[derive(Clone)]
pub struct TlsConnector {
    inner: Arc<ClientConfig>,
}

impl From<Arc<ClientConfig>> for TlsConnector {
    fn from(inner: Arc<ClientConfig>) -> TlsConnector {
        TlsConnector { inner }
    }
}

impl From<ClientConfig> for TlsConnector {
    fn from(inner: ClientConfig) -> TlsConnector {
        TlsConnector {
            inner: Arc::new(inner),
        }
    }
}

impl TlsConnector {
    /// Perform the TLS handshake over `stream`, validating the peer against `domain`.
    pub async fn connect<IO>(
        &self,
        domain: ServerName<'static>,
        stream: IO,
    ) -> Result<TlsStream<IO>, TlsError>
    where
        IO: AsyncReadRent + AsyncWriteRent,
    {
        let session = ClientConnection::new(self.inner.clone(), domain)?;
        let mut stream = Stream::new(stream, session);
        stream.handshake().await?;
        Ok(stream)
    }
}
