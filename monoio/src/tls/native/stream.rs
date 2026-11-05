use std::io::{self, IoSlice, Read, Write};

use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut, RawBuf},
    io::{AsyncReadRent, AsyncWriteRent, Split},
    tls::io_wrapper::{iovec_slices, write_all_slices, MAX_FRAGMENT_LEN, RECORD_OVERHEAD},
    BufResult,
};

use super::utils::{Buffers, IOWrapper};

/// A wrapper around an underlying raw stream which implements the TLS or SSL
/// protocol.
///
/// A `TlsStream<S>` represents a handshake that has been completed successfully
/// and both the server and the client are ready for receiving and sending
/// data. Bytes read from a `TlsStream` are decrypted from `S` and bytes written
/// to a `TlsStream` are encrypted when passing through to `S`.
///
/// Encrypted records are gathered in a write staging buffer (64 KiB plus record overhead by
/// default) and sent with one socket write per buffer.
#[derive(Debug)]
pub struct TlsStream<S> {
    tls: native_tls::TlsStream<Buffers>,
    io: IOWrapper<S>,
    coalesce: bool,
}

impl<S> TlsStream<S> {
    pub(crate) fn new(tls_stream: native_tls::TlsStream<Buffers>, io: IOWrapper<S>) -> Self {
        Self {
            tls: tls_stream,
            io,
            coalesce: false,
        }
    }

    /// Enable or disable write coalescing (off by default).
    ///
    /// When enabled, `write`/`writev` only encrypt into the write staging buffer; bytes reach
    /// the socket when that buffer is full, or on [`flush`](AsyncWriteRent::flush) /
    /// [`shutdown`](AsyncWriteRent::shutdown). **You must call `flush()`** before waiting for
    /// the peer, otherwise data may never be sent. Each `write` still makes its own TLS record.
    /// When disabled, data is on the socket when `write` returns.
    pub fn set_write_coalescing(&mut self, enabled: bool) {
        self.coalesce = enabled;
    }

    /// Whether write coalescing is enabled.
    pub fn write_coalescing(&self) -> bool {
        self.coalesce
    }

    /// Consume the stream and return the underlying IO. Buffered data is discarded.
    pub fn into_inner(self) -> S {
        self.io.into_parts().0
    }

    #[cfg(feature = "tls-native-alpn")]
    /// The ALPN protocol negotiated during the handshake, if any.
    pub fn alpn_protocol(&self) -> Option<Vec<u8>> {
        self.tls.negotiated_alpn().ok().flatten()
    }
}

unsafe impl<S: Split> Split for TlsStream<S> {}

impl<S: AsyncReadRent> AsyncReadRent for TlsStream<S> {
    #[allow(clippy::await_holding_refcell_ref)]
    async fn read<T: IoBufMut>(&mut self, mut buf: T) -> BufResult<usize, T> {
        let slice = unsafe { std::slice::from_raw_parts_mut(buf.write_ptr(), buf.bytes_total()) };

        loop {
            // read from native-tls to buffer
            match self.tls.read(slice) {
                Ok(mut n) => {
                    // Keep decrypting records that are already buffered (no IO); errors and
                    // EOF show up again on the next read.
                    while n > 0 && n < slice.len() {
                        match self.tls.read(&mut slice[n..]) {
                            Ok(0) | Err(_) => break,
                            Ok(m) => n += m,
                        }
                    }
                    unsafe { buf.set_init(n) };
                    return (Ok(n), buf);
                }
                // we need more data, read something.
                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => (),
                Err(e) => {
                    return (Err(e), buf);
                }
            }

            // now we need data, read something into native-tls
            match unsafe { self.io.do_read_io() }.await {
                Ok(0) => {
                    return (Ok(0), buf);
                }
                Ok(_) => (),
                Err(e) => {
                    return (Err(e), buf);
                }
            };
        }
    }

    async fn readv<T: IoVecBufMut>(&mut self, mut buf: T) -> BufResult<usize, T> {
        let n = match unsafe { RawBuf::new_from_iovec_mut(&mut buf) } {
            Some(raw_buf) => self.read(raw_buf).await.0,
            None => Ok(0),
        };
        if let Ok(n) = n {
            unsafe { buf.set_init(n) };
        }
        (n, buf)
    }
}

impl<S: AsyncWriteRent> TlsStream<S> {
    /// Encrypt `bufs` into the write staging buffer, writing it to the socket only when a record
    /// would not fit. Stops (returning the bytes accepted) when the buffer is full. Does not
    /// flush at the end.
    async fn write_slices(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let mut total = 0;
        for b in bufs {
            let mut off = 0;
            while off < b.len() {
                let needed = (b.len() - off).min(MAX_FRAGMENT_LEN) + RECORD_OVERHEAD;
                if self.io.write_available() < needed {
                    if total > 0 {
                        return Ok(total);
                    }
                    unsafe { self.io.do_write_io() }.await?;
                }
                match self.tls.write(&b[off..]) {
                    Ok(0) => return Ok(total),
                    Ok(n) => {
                        off += n;
                        total += n;
                    }
                    // The staging buffer is full in the middle of a record: send it, then OpenSSL
                    // needs the same bytes again.
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        unsafe { self.io.do_write_io() }.await?;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(total)
    }

    async fn write_and_send(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let n = self.write_slices(bufs).await?;
        if !self.coalesce {
            unsafe { self.io.do_write_io() }.await?;
        }
        Ok(n)
    }
}

impl<S: AsyncWriteRent> AsyncWriteRent for TlsStream<S> {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        let slice = unsafe { std::slice::from_raw_parts(buf.read_ptr(), buf.bytes_init()) };
        let res = self.write_and_send(&[IoSlice::new(slice)]).await;
        (res, buf)
    }

    async fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        // SAFETY: the memory is owned by `buf_vec`, which outlives this use.
        let slices = unsafe { iovec_slices(&buf_vec) };
        let res = write_all_slices!(self, write_and_send, &slices);
        (res, buf_vec)
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn flush(&mut self) -> io::Result<()> {
        loop {
            match self.tls.flush() {
                Ok(_) => {
                    unsafe { self.io.do_write_io() }.await?;
                    return Ok(());
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    unsafe { self.io.do_write_io() }.await?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        // Make room for close_notify (coalesced writes may have filled the buffer).
        unsafe { self.io.do_write_io() }.await?;
        self.tls.shutdown()?;
        unsafe { self.io.do_write_io() }.await?;
        Ok(())
    }
}
