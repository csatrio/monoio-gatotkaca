use std::{
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
};

use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut, RawBuf},
    io::{AsyncReadRent, AsyncWriteRent, Split},
    tls::io_wrapper::{ReadBuffer, WriteBuffer},
    BufResult,
};
use rustls::{ClientConnection, ConnectionCommon, ServerConnection, SideData};

#[derive(Debug)]
pub struct Stream<IO, C> {
    pub(crate) io: IO,
    pub(crate) session: C,
    r_buffer: ReadBuffer,
    w_buffer: WriteBuffer,
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
    /// Wrap `io` with an already created (not yet handshaken) rustls session.
    pub fn new(io: IO, session: C) -> Self {
        Self {
            io,
            session,
            r_buffer: Default::default(),
            w_buffer: Default::default(),
        }
    }

    /// Split into the underlying IO and the rustls session. Buffered data is discarded.
    pub fn into_parts(self) -> (IO, C) {
        (self.io, self.session)
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

    pub(crate) async fn write_io(&mut self) -> io::Result<usize> {
        let n = loop {
            match self.session.write_tls(&mut self.w_buffer) {
                Ok(n) => {
                    self.w_buffer.do_io(&mut self.io).await?;
                    break n;
                }
                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                    self.w_buffer.do_io(&mut self.io).await?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        };

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
        loop {
            // read from rustls to buffer
            match self.session.reader().read(slice) {
                Ok(n) => {
                    unsafe { buf.set_init(n) };
                    return (Ok(n), buf);
                }
                // we need more data, read something.
                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => (),
                Err(e) => {
                    return (Err(e), buf);
                }
            }

            // now we need data, read something into rustls
            if let Err(e) = self.read_io(splitted).await {
                return (Err(e), buf);
            }
        }
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
        // construct slice
        let slice = unsafe { std::slice::from_raw_parts(buf.read_ptr(), buf.bytes_init()) };

        // flush rustls inner write buffer to make sure there is space for new data
        if self.session.wants_write() {
            if let Err(e) = self.write_io().await {
                return (Err(e), buf);
            }
        }

        // write slice to rustls
        let n = match self.session.writer().write(slice) {
            Ok(n) => n,
            Err(e) => return (Err(e), buf),
        };

        // write from rustls to connection
        while self.session.wants_write() {
            match self.write_io().await {
                Ok(0) => {
                    break;
                }
                Ok(_) => (),
                Err(e) => return (Err(e), buf),
            }
        }
        (Ok(n), buf)
    }

    // TODO: use real writev
    async fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        let n = match unsafe { RawBuf::new_from_iovec(&buf_vec) } {
            Some(raw_buf) => self.write(raw_buf).await.0,
            None => Ok(0),
        };
        (n, buf_vec)
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.session.writer().flush()?;
        while self.session.wants_write() {
            self.write_io().await?;
        }
        self.io.flush().await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.session.send_close_notify();

        while self.session.wants_write() {
            self.write_io().await?;
        }
        self.io.shutdown().await
    }
}
