use std::sync::Arc;

use crate::io::{AsyncReadRent, AsyncWriteRent, OwnedReadHalf, OwnedWriteHalf};
use rustls::{pki_types::ServerName, ClientConfig, ClientConnection};

use super::{
    stream::{Stream, StreamOptions},
    TlsError,
};

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
    opts: StreamOptions,
}

impl From<Arc<ClientConfig>> for TlsConnector {
    fn from(inner: Arc<ClientConfig>) -> TlsConnector {
        TlsConnector {
            inner,
            opts: StreamOptions::default(),
        }
    }
}

impl From<ClientConfig> for TlsConnector {
    fn from(inner: ClientConfig) -> TlsConnector {
        TlsConnector {
            inner: Arc::new(inner),
            opts: StreamOptions::default(),
        }
    }
}

impl TlsConnector {
    /// Size in bytes of the read staging buffer that socket reads land in.
    ///
    /// `None` (the default) is 64 KiB plus room for record overhead (65,792 bytes). Values below
    /// one full TLS record (18 KiB) are raised to 18 KiB.
    pub fn read_buffer(mut self, size: Option<usize>) -> Self {
        self.opts.read_buffer = size;
        self
    }

    /// Size in bytes of the write staging buffer that TLS records are gathered in before one
    /// socket write.
    ///
    /// `None` (the default) is 64 KiB plus room for record overhead (65,792 bytes), so a 64 KiB
    /// write is one socket write. Values below one full TLS record (18 KiB) are raised to 18 KiB.
    pub fn write_buffer(mut self, size: Option<usize>) -> Self {
        self.opts.write_buffer = size;
        self
    }

    /// Enable write coalescing on the streams this creates (off by default); see
    /// `set_write_coalescing` on the stream.
    ///
    /// With coalescing on, `write` does not send: data goes out when the write buffer is full or
    /// on `flush()`/`shutdown()`, so **you must call `flush()`** before waiting for the peer.
    pub fn write_coalescing(mut self, enabled: bool) -> Self {
        self.opts.write_coalescing = enabled;
        self
    }

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
        let mut stream = Stream::with_options(stream, session, self.opts);
        stream.handshake().await?;
        Ok(stream)
    }
}
