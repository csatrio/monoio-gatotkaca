//! Bridges sync `std::io` TLS engines and the owned-buffer async IO of this crate.

use std::{fmt::Debug, io, mem};

use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf},
    io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt},
};

/// Largest TLS plaintext fragment (2^14 bytes).
pub(crate) const MAX_FRAGMENT_LEN: usize = 16 * 1024;
/// Upper bound of the per-record wire overhead we plan for (header, nonce, tag, padding).
/// TLS 1.3 AES-GCM adds 22 bytes, TLS 1.2 AES-GCM 29.
pub(crate) const RECORD_OVERHEAD: usize = 64;
/// Default staging buffer size: 64 KiB of plaintext plus the overhead of the 4 records it makes,
/// so one 64 KiB write goes out in one socket write.
pub(crate) const DEFAULT_BUFFER_SIZE: usize = 64 * 1024 + 4 * RECORD_OVERHEAD;
/// Smallest staging buffer: one full record (16 KiB + up to 2 KiB of expansion allowed by TLS 1.2).
/// Smaller requested sizes are raised to this.
pub(crate) const MIN_BUFFER_SIZE: usize = 16 * 1024 + 2 * 1024;

/// The non-empty slices of an `IoVecBuf`, for feeding a TLS engine.
///
/// # Safety
/// The iovecs of `buf` must describe valid initialized memory owned by `buf`.
pub(crate) unsafe fn iovec_slices<T: IoVecBuf>(buf: &T) -> Vec<io::IoSlice<'_>> {
    let len = buf.read_iovec_len();
    if len == 0 {
        return Vec::new();
    }
    std::slice::from_raw_parts(buf.read_iovec_ptr(), len)
        .iter()
        .filter(|v| v.iov_len > 0 && !v.iov_base.is_null())
        .map(|v| {
            io::IoSlice::new(std::slice::from_raw_parts(
                v.iov_base as *const u8,
                v.iov_len,
            ))
        })
        .collect()
}

/// The bytes of `bufs` after skipping `skip`, limited to `limit` bytes.
pub(crate) fn sub_slices<'a>(
    bufs: &[io::IoSlice<'a>],
    mut skip: usize,
    mut limit: usize,
) -> Vec<io::IoSlice<'a>> {
    let mut out = Vec::with_capacity(bufs.len());
    for b in bufs {
        // SAFETY: io::IoSlice derefs to a slice with the lifetime 'a of its data.
        let b: &'a [u8] = unsafe { std::slice::from_raw_parts(b.as_ptr(), b.len()) };
        if skip >= b.len() {
            skip -= b.len();
            continue;
        }
        let b = &b[skip..];
        skip = 0;
        let take = b.len().min(limit);
        out.push(io::IoSlice::new(&b[..take]));
        limit -= take;
        if limit == 0 {
            break;
        }
    }
    out
}

/// `writev` helper: call `$stream.$write(slices)` until every byte of `$slices` is accepted.
///
/// A TLS `writev` takes all slices at once: callers like `write_vectored_all` then never retry
/// with a partially consumed iovec list.
macro_rules! write_all_slices {
    ($stream:ident, $write:ident, $slices:expr) => {{
        let slices: &[std::io::IoSlice<'_>] = $slices;
        let total: usize = slices.iter().map(|b| b.len()).sum();
        let mut done = 0;
        loop {
            if done == total {
                break Ok(done);
            }
            let res = if done == 0 {
                $stream.$write(slices).await
            } else {
                let rest = $crate::tls::io_wrapper::sub_slices(slices, done, usize::MAX);
                $stream.$write(&rest).await
            };
            match res {
                Ok(0) => break Ok(done),
                Ok(n) => done += n,
                Err(e) => break Err(e),
            }
        }
    }};
}
pub(crate) use write_all_slices;

/// Clamp a user-requested buffer size; `None` means the default.
pub(crate) fn buffer_size(size: Option<usize>) -> usize {
    size.map_or(DEFAULT_BUFFER_SIZE, |s| s.max(MIN_BUFFER_SIZE))
}

struct Buffer {
    read: usize,
    write: usize,
    buf: Box<[u8]>,
}

impl Buffer {
    fn new(size: usize) -> Self {
        Self {
            read: 0,
            write: 0,
            // `vec![0; n]` is a calloc: fresh zeroed pages, no memset for large sizes.
            buf: vec![0; size].into_boxed_slice(),
        }
    }

    fn len(&self) -> usize {
        self.write - self.read
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn available(&self) -> usize {
        self.buf.len() - self.write
    }

    fn is_full(&self) -> bool {
        self.available() == 0
    }

    fn advance(&mut self, n: usize) {
        assert!(self.read + n <= self.write);
        self.read += n;
        if self.read == self.write {
            self.read = 0;
            self.write = 0;
        }
    }
}

unsafe impl crate::buf::IoBuf for Buffer {
    fn read_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.read) }
    }

    fn bytes_init(&self) -> usize {
        self.write - self.read
    }
}

unsafe impl crate::buf::IoBufMut for Buffer {
    fn write_ptr(&mut self) -> *mut u8 {
        unsafe { self.buf.as_mut_ptr().add(self.write) }
    }

    fn bytes_total(&mut self) -> usize {
        self.buf.len() - self.write
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.write += pos;
    }
}

pub(crate) struct ReadBuffer {
    // the option is only meant for temporary take, it always should be some
    buffer: Option<Buffer>,
    status: ReadStatus,
}

impl Debug for ReadBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadBuffer")
            .field("status", &self.status)
            .finish()
    }
}

#[derive(Debug)]
enum ReadStatus {
    Eof,
    Err(io::Error),
    Ok,
}

impl Default for ReadBuffer {
    fn default() -> Self {
        Self::new(None)
    }
}

impl ReadBuffer {
    /// Create a new ReadBuffer; `None` is the default size, smaller sizes are clamped up.
    pub(crate) fn new(size: Option<usize>) -> Self {
        Self {
            buffer: Some(Buffer::new(buffer_size(size))),
            status: ReadStatus::Ok,
        }
    }

    /// Whether received bytes are waiting in the buffer (so reading them needs no IO).
    #[cfg(feature = "tls-rustls")]
    pub(crate) fn has_buffered(&self) -> bool {
        self.buffer.as_ref().is_some_and(|b| !b.is_empty())
    }

    /// `do_io` do async read from io to inner buffer.
    /// # Handle return value
    /// _: the read result.
    pub(crate) async fn do_io<IO: AsyncReadRent>(&mut self, mut io: IO) -> io::Result<usize> {
        // if there are some data inside the buffer, just return.
        let buffer = self.buffer.as_ref().expect("buffer ref expected");
        if !buffer.is_empty() {
            return Ok(buffer.len());
        }

        // read from raw io
        // # Safety
        // We have already checked it is not None.
        let buffer = unsafe { self.buffer.take().unwrap_unchecked() };
        let (result, buf) = io.read(buffer).await;
        self.buffer = Some(buf);
        match result {
            Ok(0) => {
                self.status = ReadStatus::Eof;
                result
            }
            Ok(_) => {
                self.status = ReadStatus::Ok;
                result
            }
            Err(e) => {
                let rerr = e.kind().into();
                self.status = ReadStatus::Err(e);
                Err(rerr)
            }
        }
    }
}

impl io::Read for ReadBuffer {
    /// `read` from buffer.
    /// # Handle return value
    /// 1. Err(WouldBlock): the buffer is empty, call do_io to fetch more.
    /// 2. _: handle it.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // if buffer is empty, return WoundBlock.
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        if buffer.is_empty() {
            return match mem::replace(&mut self.status, ReadStatus::Ok) {
                ReadStatus::Eof => Ok(0),
                ReadStatus::Err(e) => Err(e),
                ReadStatus::Ok => Err(io::ErrorKind::WouldBlock.into()),
            };
        }

        // now buffer is not empty. copy it.
        let to_copy = buffer.len().min(buf.len());
        unsafe { std::ptr::copy_nonoverlapping(buffer.read_ptr(), buf.as_mut_ptr(), to_copy) };
        buffer.advance(to_copy);

        Ok(to_copy)
    }
}

pub(crate) struct WriteBuffer {
    // the option is only meant for temporary take, it always should be some
    buffer: Option<Buffer>,
    status: WriteStatus,
}

impl Debug for WriteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBuffer")
            .field("status", &self.status)
            .finish()
    }
}

#[derive(Debug)]
enum WriteStatus {
    Err(io::Error),
    Ok,
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new(None)
    }
}

impl WriteBuffer {
    /// Create a new WriteBuffer; `None` is the default size, smaller sizes are clamped up.
    pub(crate) fn new(size: Option<usize>) -> Self {
        Self {
            buffer: Some(Buffer::new(buffer_size(size))),
            status: WriteStatus::Ok,
        }
    }

    fn buffer(&self) -> &Buffer {
        self.buffer.as_ref().expect("buffer ref expected")
    }

    /// No bytes are waiting to be written.
    #[cfg(feature = "tls-rustls")]
    pub(crate) fn is_empty(&self) -> bool {
        self.buffer().is_empty()
    }

    /// Free space at the end of the buffer.
    pub(crate) fn available(&self) -> usize {
        self.buffer().available()
    }

    /// `do_io` do async write from inner buffer to io.
    /// # Handle return value
    /// _: the write_all result(note: the data may have been written even when error).
    pub(crate) async fn do_io<IO: AsyncWriteRent>(&mut self, mut io: IO) -> io::Result<usize> {
        // if the buffer is empty, just return.
        let buffer = self.buffer.as_ref().expect("buffer ref expected");
        if buffer.is_empty() {
            return Ok(0);
        }

        // buffer is not empty now. write it.
        // # Safety
        // We have already checked it is not None.
        let buffer = unsafe { self.buffer.take().unwrap_unchecked() };
        let (result, buffer) = io.write_all(buffer).await;
        self.buffer = Some(buffer);
        match result {
            Ok(written_len) => {
                unsafe { self.buffer.as_mut().unwrap_unchecked().advance(written_len) };
                Ok(written_len)
            }
            Err(e) => {
                let rerr = e.kind().into();
                self.status = WriteStatus::Err(e);
                Err(rerr)
            }
        }
    }
}

impl io::Write for WriteBuffer {
    /// `write` to buffer.
    /// # Handle return value
    /// 1. Err(WouldBlock): the buffer is full, call do_io to flush it.
    /// 2. _: handle it.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // if there is too much data inside the buffer, return WoundBlock
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        match mem::replace(&mut self.status, WriteStatus::Ok) {
            WriteStatus::Err(e) => return Err(e),
            WriteStatus::Ok if buffer.is_full() => return Err(io::ErrorKind::WouldBlock.into()),
            _ => (),
        }

        // there is space inside the buffer, copy to it.
        let to_copy = buf.len().min(buffer.available());
        unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), buffer.write_ptr(), to_copy) };
        unsafe { buffer.set_init(to_copy) };
        Ok(to_copy)
    }

    /// `write_vectored` to buffer: copies as many slices as fit (the last one maybe partially).
    /// rustls' `write_tls` hands over all queued records this way.
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        match mem::replace(&mut self.status, WriteStatus::Ok) {
            WriteStatus::Err(e) => return Err(e),
            WriteStatus::Ok if buffer.is_full() => return Err(io::ErrorKind::WouldBlock.into()),
            _ => (),
        }

        let mut copied = 0;
        for buf in bufs {
            let to_copy = buf.len().min(buffer.available());
            if to_copy == 0 && !buf.is_empty() {
                break;
            }
            unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), buffer.write_ptr(), to_copy) };
            unsafe { buffer.set_init(to_copy) };
            copied += to_copy;
        }
        Ok(copied)
    }

    /// `flush` to buffer.
    /// # Handle return value
    /// 1. Err(WouldBlock): the buffer is full, call do_io to flush it.
    /// 2. _: handle it.
    fn flush(&mut self) -> io::Result<()> {
        let buffer = self.buffer.as_mut().expect("buffer mut expected");
        match mem::replace(&mut self.status, WriteStatus::Ok) {
            WriteStatus::Err(e) => Err(e),
            WriteStatus::Ok if !buffer.is_empty() => Err(io::ErrorKind::WouldBlock.into()),
            _ => Ok(()),
        }
    }
}
