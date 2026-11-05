use std::fmt;

use crate::io::{AsyncReadRent, AsyncWriteRent};

use super::{
    utils::{handshake, IOWrapper},
    TlsError, TlsStream,
};

/// A wrapper around a `native_tls::TlsConnector`, providing an async `connect`
/// method.
#[derive(Clone)]
pub struct TlsConnector {
    inner: native_tls::TlsConnector,
    read_buffer: Option<usize>,
    write_buffer: Option<usize>,
    write_coalescing: bool,
}

impl TlsConnector {
    /// Connects the provided stream with this connector, assuming the provided
    /// domain.
    ///
    /// This function will internally call `TlsConnector::connect` to connect
    /// the stream and returns a future representing the resolution of the
    /// connection operation. The returned future will resolve to either
    /// `TlsStream<S>` or `Error` depending if it's successful or not.
    ///
    /// This is typically used for clients who have already established, for
    /// example, a TCP connection to a remote server. That stream is then
    /// provided here to perform the client half of a connection to a
    /// TLS-powered server.
    pub async fn connect<S>(&self, domain: &str, stream: S) -> Result<TlsStream<S>, TlsError>
    where
        S: AsyncReadRent + AsyncWriteRent,
    {
        let io = IOWrapper::new_with_buffer_size(stream, self.read_buffer, self.write_buffer);
        let mut stream = handshake(move |s_wrap| self.inner.connect(domain, s_wrap), io).await?;
        stream.set_write_coalescing(self.write_coalescing);
        Ok(stream)
    }

    /// Set the size of the internal read buffer in bytes.
    ///
    /// `None` (the default) is 64 KiB plus room for record overhead (65,792 bytes). Values below
    /// one full TLS record (18 KiB) are raised to 18 KiB.
    pub fn read_buffer(mut self, size: Option<usize>) -> Self {
        self.read_buffer = size;
        self
    }

    /// Set the size of the internal write buffer, where TLS records are gathered before one
    /// socket write.
    ///
    /// `None` (the default) is 64 KiB plus room for record overhead (65,792 bytes). Values below
    /// one full TLS record (18 KiB) are raised to 18 KiB.
    pub fn write_buffer(mut self, size: Option<usize>) -> Self {
        self.write_buffer = size;
        self
    }

    /// Enable write coalescing on the streams this creates (off by default); see
    /// [`TlsStream::set_write_coalescing`].
    ///
    /// With coalescing on, `write` does not send: data goes out when the write buffer is full or
    /// on `flush()`/`shutdown()`, so **you must call `flush()`** before waiting for the peer.
    pub fn write_coalescing(mut self, enabled: bool) -> Self {
        self.write_coalescing = enabled;
        self
    }
}

impl fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnector").finish()
    }
}

impl From<native_tls::TlsConnector> for TlsConnector {
    fn from(inner: native_tls::TlsConnector) -> TlsConnector {
        TlsConnector {
            inner,
            read_buffer: None,
            write_buffer: None,
            write_coalescing: false,
        }
    }
}
