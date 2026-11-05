use std::{
    io::{self, IoSlice, Read, Write},
    mem,
    ops::{Deref, DerefMut},
};

use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut, RawBuf},
    io::{AsyncReadRent, AsyncWriteRent, Split},
    tls::io_wrapper::{
        iovec_slices, sub_slices, write_all_slices, ReadBuffer, WriteBuffer, MAX_FRAGMENT_LEN,
        RECORD_OVERHEAD,
    },
    BufResult,
};
use rustls::{ClientConnection, ConnectionCommon, ServerConnection, SideData};

/// Per-stream options shared by [`TlsAcceptor`](super::TlsAcceptor) and
/// [`TlsConnector`](super::TlsConnector).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StreamOptions {
    pub(crate) read_buffer: Option<usize>,
    pub(crate) write_buffer: Option<usize>,
    pub(crate) write_coalescing: bool,
}

/// A TLS stream over `IO` (see [`ClientTlsStream`](super::ClientTlsStream) and
/// [`ServerTlsStream`](super::ServerTlsStream)).
///
/// Data moves through a read and a write staging buffer (64 KiB plus record overhead by default,
/// see `read_buffer`/`write_buffer` on the acceptor and connector).
#[derive(Debug)]
pub struct Stream<IO, C> {
    pub(crate) io: IO,
    pub(crate) session: C,
    r_buffer: ReadBuffer,
    w_buffer: WriteBuffer,
    /// Write coalescing: `write` only buffers, `flush`/`shutdown` (or a full buffer) send.
    coalesce: bool,
    /// Plaintext gathered by coalesced small writes, encrypted as one record (at most 16 KiB).
    pending: Vec<u8>,
    /// Error found while decrypting already buffered data after some plaintext was returned;
    /// reported by the next read.
    read_err: Option<io::Error>,
}

impl<IO> Stream<IO, ServerConnection> {
    #[inline]
    /// The ALPN protocol negotiated during the handshake, if any.
    pub fn alpn_protocol(&self) -> Option<Vec<u8>> {
        self.session.alpn_protocol().map(|s| s.to_vec())
    }
}

impl<IO> Stream<IO, ClientConnection> {
    #[inline]
    /// The ALPN protocol negotiated during the handshake, if any.
    pub fn alpn_protocol(&self) -> Option<Vec<u8>> {
        self.session.alpn_protocol().map(|s| s.to_vec())
    }
}

unsafe impl<IO: Split, C> Split for Stream<IO, C> {}

impl<IO, C> Stream<IO, C> {
    /// Wrap `io` with an already created (not yet handshaken) rustls session, using the default
    /// buffer sizes and no write coalescing.
    pub fn new(io: IO, session: C) -> Self {
        Self::with_options(io, session, StreamOptions::default())
    }

    pub(crate) fn with_options(io: IO, session: C, opts: StreamOptions) -> Self {
        Self {
            io,
            session,
            r_buffer: ReadBuffer::new(opts.read_buffer),
            w_buffer: WriteBuffer::new(opts.write_buffer),
            coalesce: opts.write_coalescing,
            pending: Vec::new(),
            read_err: None,
        }
    }

    /// Split into the underlying IO and the rustls session. Buffered data is discarded.
    pub fn into_parts(self) -> (IO, C) {
        (self.io, self.session)
    }

    /// Enable or disable write coalescing (off by default).
    ///
    /// When enabled, `write`/`writev` only encrypt into memory: small writes are gathered into
    /// records of up to 16 KiB, and bytes reach the socket only when the write staging buffer is
    /// full, or on [`flush`](AsyncWriteRent::flush) / [`shutdown`](AsyncWriteRent::shutdown).
    /// **You must call `flush()`** before waiting for the peer, otherwise data may never be
    /// sent. When disabled, data is on the socket when `write` returns.
    pub fn set_write_coalescing(&mut self, enabled: bool) {
        self.coalesce = enabled;
    }

    /// Whether write coalescing is enabled.
    pub fn write_coalescing(&self) -> bool {
        self.coalesce
    }
}

impl<IO: AsyncReadRent + AsyncWriteRent, C, SD: SideData> Stream<IO, C>
where
    C: DerefMut + Deref<Target = ConnectionCommon<SD>>,
{
    pub(crate) async fn read_io(&mut self, splitted: bool) -> io::Result<usize> {
        let n = loop {
            match self.session.read_tls(&mut self.r_buffer) {
                Ok(n) => {
                    break n;
                }
                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                    self.r_buffer.do_io(&mut self.io).await?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        };

        let state = match self.session.process_new_packets() {
            Ok(state) => state,
            Err(err) => {
                // When split, the write half may be in use, so do not write from the read
                // path. The user should manually shut down the stream on error.
                if !splitted {
                    let _ = self.write_io().await;
                }
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        };

        if state.peer_has_closed() && self.session.is_handshaking() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "tls handshake alert",
            ));
        }

        Ok(n)
    }

    /// Feed bytes already in the read staging buffer to rustls (no IO) until some plaintext (or
    /// the peer's close) is available. Returns false if nothing more can be had without IO.
    fn read_buffered(&mut self) -> bool {
        if self.session.is_handshaking() {
            return false;
        }
        while self.r_buffer.has_buffered() {
            match self.session.read_tls(&mut self.r_buffer) {
                Ok(n) if n > 0 => (),
                _ => return false,
            }
            match self.session.process_new_packets() {
                Ok(state) if state.plaintext_bytes_to_read() > 0 || state.peer_has_closed() => {
                    return true
                }
                Ok(_) => (),
                Err(err) => {
                    self.read_err = Some(io::Error::new(io::ErrorKind::InvalidData, err));
                    return false;
                }
            }
        }
        false
    }

    /// Move queued TLS records from rustls into the write staging buffer, as many as fit.
    fn fill_w_buffer(&mut self) -> io::Result<usize> {
        let mut n = 0;
        while self.session.wants_write() && self.w_buffer.available() > 0 {
            match self.session.write_tls(&mut self.w_buffer) {
                Ok(0) => break,
                Ok(k) => n += k,
                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err),
            }
        }
        Ok(n)
    }

    /// Fill the write staging buffer from rustls, then write it out with one `write_all`.
    /// Returns the number of TLS bytes taken from rustls.
    pub(crate) async fn write_io(&mut self) -> io::Result<usize> {
        let n = self.fill_w_buffer()?;
        self.w_buffer.do_io(&mut self.io).await?;
        Ok(n)
    }

    /// Send everything rustls and the staging buffer hold.
    async fn flush_tls(&mut self) -> io::Result<()> {
        while self.session.wants_write() || !self.w_buffer.is_empty() {
            self.write_io().await?;
        }
        Ok(())
    }

    /// How much plaintext can be encrypted so that its records still fit the staging buffer.
    fn plaintext_budget(&self) -> usize {
        let avail = self.w_buffer.available();
        let records = avail.div_ceil(MAX_FRAGMENT_LEN + RECORD_OVERHEAD);
        avail.saturating_sub(records * RECORD_OVERHEAD)
    }

    /// Encrypt plaintext from `bufs` into the staging buffer, writing the staging buffer to the
    /// socket only when it has no room. Does not flush at the end. Returns the bytes accepted
    /// (at most about one staging buffer).
    async fn push_plaintext(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Ok(0);
        }
        // Make room for at least one record (or all of `bufs` if smaller) without splitting it.
        loop {
            self.fill_w_buffer()?;
            if self.plaintext_budget() >= total.min(MAX_FRAGMENT_LEN) || self.w_buffer.is_empty() {
                break;
            }
            self.w_buffer.do_io(&mut self.io).await?;
        }

        let mut done = 0;
        while done < total {
            let budget = self.plaintext_budget();
            // Do not make a short record just to use the last bytes of the buffer.
            if budget == 0 || (done > 0 && budget < (total - done).min(MAX_FRAGMENT_LEN)) {
                break;
            }
            // Only full records unless this finishes the input.
            let mut take = (total - done).min(budget);
            if take < total - done && take >= MAX_FRAGMENT_LEN {
                take -= take % MAX_FRAGMENT_LEN;
            }
            let k = if bufs.len() == 1 {
                self.session.writer().write(&bufs[0][done..done + take])?
            } else {
                let limited = sub_slices(bufs, done, take);
                self.session.writer().write_vectored(&limited)?
            };
            if k == 0 {
                break;
            }
            done += k;
            self.fill_w_buffer()?;
        }
        Ok(done)
    }

    /// Encrypt the coalesced plaintext, if any.
    async fn push_pending(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut pending = mem::take(&mut self.pending);
        let mut off = 0;
        let res = loop {
            if off == pending.len() {
                break Ok(());
            }
            match self.push_plaintext(&[IoSlice::new(&pending[off..])]).await {
                Ok(0) => break Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => off += n,
                Err(e) => break Err(e),
            }
        };
        pending.clear();
        self.pending = pending;
        res
    }

    async fn write_slices(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if self.coalesce {
            if self.pending.len() + total > MAX_FRAGMENT_LEN {
                self.push_pending().await?;
            }
            if total >= MAX_FRAGMENT_LEN {
                // Large write: encrypt directly; it is sent once the staging buffer fills up.
                return self.push_plaintext(bufs).await;
            }
            if self.pending.capacity() == 0 {
                self.pending.reserve_exact(MAX_FRAGMENT_LEN);
            }
            for b in bufs {
                self.pending.extend_from_slice(b);
            }
            return Ok(total);
        }

        self.push_pending().await?;
        let n = self.push_plaintext(bufs).await?;
        self.flush_tls().await?;
        Ok(n)
    }

    pub(crate) async fn handshake(&mut self) -> io::Result<(usize, usize)> {
        let mut wrlen = 0;
        let mut rdlen = 0;
        let mut eof = false;

        loop {
            while self.session.wants_write() && self.session.is_handshaking() {
                wrlen += self.write_io().await?;
            }
            while !eof && self.session.wants_read() && self.session.is_handshaking() {
                let n = self.read_io(false).await?;
                rdlen += n;
                if n == 0 {
                    eof = true;
                }
            }

            match (eof, self.session.is_handshaking()) {
                (true, true) => {
                    let err = io::Error::new(io::ErrorKind::UnexpectedEof, "tls handshake eof");
                    return Err(err);
                }
                (false, true) => (),
                (_, false) => {
                    break;
                }
            };
        }

        // flush buffer
        while self.session.wants_write() {
            wrlen += self.write_io().await?;
        }

        Ok((rdlen, wrlen))
    }

    pub(crate) async fn read_inner<T: crate::buf::IoBufMut>(
        &mut self,
        mut buf: T,
        splitted: bool,
    ) -> BufResult<usize, T> {
        let slice = unsafe { std::slice::from_raw_parts_mut(buf.write_ptr(), buf.bytes_total()) };
        let mut filled = 0;
        loop {
            if filled == 0 {
                if let Some(err) = self.read_err.take() {
                    if !splitted {
                        let _ = self.write_io().await;
                    }
                    return (Err(err), buf);
                }
            }
            // read from rustls to buffer
            match self.session.reader().read(&mut slice[filled..]) {
                Ok(n) => {
                    filled += n;
                    // Keep filling from records already received, as long as that needs no IO.
                    if n == 0 || filled == slice.len() || !self.read_buffered() {
                        break;
                    }
                    continue;
                }
                // we need more data, read something.
                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                    if filled > 0 {
                        break;
                    }
                }
                // The error is sticky in rustls: report it on the next read.
                Err(_) if filled > 0 => break,
                Err(e) => {
                    return (Err(e), buf);
                }
            }

            // now we need data, read something into rustls
            if let Err(e) = self.read_io(splitted).await {
                return (Err(e), buf);
            }
        }
        unsafe { buf.set_init(filled) };
        (Ok(filled), buf)
    }
}

impl<IO: AsyncReadRent + AsyncWriteRent, C, SD: SideData + 'static> AsyncReadRent for Stream<IO, C>
where
    C: DerefMut + Deref<Target = ConnectionCommon<SD>>,
{
    async fn read<T: IoBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        self.read_inner(buf, false).await
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

impl<IO: AsyncReadRent + AsyncWriteRent, C, SD: SideData + 'static> AsyncWriteRent for Stream<IO, C>
where
    C: DerefMut + Deref<Target = ConnectionCommon<SD>>,
{
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        let slice = unsafe { std::slice::from_raw_parts(buf.read_ptr(), buf.bytes_init()) };
        let res = self.write_slices(&[IoSlice::new(slice)]).await;
        (res, buf)
    }

    async fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        // SAFETY: the memory is owned by `buf_vec`, which outlives this use.
        let slices = unsafe { iovec_slices(&buf_vec) };
        let res = write_all_slices!(self, write_slices, &slices);
        (res, buf_vec)
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.push_pending().await?;
        self.session.writer().flush()?;
        self.flush_tls().await?;
        self.io.flush().await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.push_pending().await?;
        self.session.send_close_notify();
        self.flush_tls().await?;
        self.io.shutdown().await
    }
}
