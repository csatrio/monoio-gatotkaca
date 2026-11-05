use std::{
    future::Future,
    io,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd},
        unix::prelude::RawFd,
    },
    process::Stdio,
};

use crate::{
    buf::{IoBufMut, IoVecBufMut},
    driver::{op::Op, shared_fd::SharedFd},
    io::{
        as_fd::{AsReadFd, AsWriteFd, SharedFdWrapper},
        AsyncReadRent,
    },
    BufResult,
};

/// One end of a Unix pipe, created with [`new_pipe`]. It reads with [`AsyncReadRent`].
#[cfg_attr(
    feature = "splice",
    doc = "
With the `splice` feature it is also the middle of a zero-copy transfer: fill the write end with
[`File::splice_to_pipe`](crate::fs::File::splice_to_pipe), [`Pipe::splice_from`] or
[`SpliceSource`](crate::io::splice::SpliceSource), and empty the read end with
[`Pipe::splice_to`], [`File::splice_from_pipe`](crate::fs::File::splice_from_pipe) or
[`SpliceDestination`](crate::io::splice::SpliceDestination)."
)]
pub struct Pipe {
    pub(crate) fd: SharedFd,
}

impl Pipe {
    pub(crate) fn from_shared_fd(fd: SharedFd) -> Self {
        Self { fd }
    }

    fn from_raw_fd(fd: RawFd) -> Self {
        Self::from_shared_fd(SharedFd::new_without_register(fd))
    }
}

/// Create a pipe (`pipe2(2)`, blocking mode) and return `(read_end, write_end)`.
///
/// The pipe holds 64 KiB by default (`/proc/sys/fs/pipe-max-size` limits how far
/// `fcntl(F_SETPIPE_SZ)` on [`Pipe::as_raw_fd`](AsRawFd::as_raw_fd) can raise it). io_uring
/// runs a splice that would block on a blocking pipe in an io-wq worker.
///
/// # Examples
///
/// ```no_run
/// let (read_end, write_end) = monoio::net::unix::new_pipe()?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn new_pipe() -> io::Result<(Pipe, Pipe)> {
    let mut pipes = [0 as libc::c_int; 2];
    crate::syscall!(pipe2@RAW(pipes.as_mut_ptr() as _, 0))?;
    Ok((Pipe::from_raw_fd(pipes[0]), Pipe::from_raw_fd(pipes[1])))
}

#[cfg(feature = "splice")]
impl Pipe {
    /// Move up to `len` bytes from this pipe (the read end) to `dst`, e.g. a `TcpStream`
    /// (`IORING_OP_SPLICE`, feature `splice`). Returns the number of bytes moved; `0` means the
    /// write end is closed and the pipe is empty.
    ///
    /// Same as [`SpliceDestination::splice_from_pipe`](crate::io::splice::SpliceDestination),
    /// but called on the pipe and without `&mut` on it. For a regular file destination, use
    /// [`File::splice_from_pipe`](crate::fs::File::splice_from_pipe), which takes an offset.
    pub async fn splice_to<T: AsWriteFd>(&self, dst: &mut T, len: u32) -> io::Result<u32> {
        Op::splice_from_pipe(&self.fd, dst.as_writer_fd().as_ref(), len)?
            .splice()
            .await
    }

    /// Move up to `len` bytes from `src`, e.g. a `TcpStream`, into this pipe (the write end)
    /// (`IORING_OP_SPLICE`, feature `splice`). Returns the number of bytes moved; `0` means `src`
    /// reached end of file.
    ///
    /// Same as [`SpliceSource::splice_to_pipe`](crate::io::splice::SpliceSource), but called on
    /// the pipe. For a regular file source, use
    /// [`File::splice_to_pipe`](crate::fs::File::splice_to_pipe), which takes an offset.
    pub async fn splice_from<T: AsReadFd>(&self, src: &mut T, len: u32) -> io::Result<u32> {
        Op::splice_to_pipe(src.as_reader_fd().as_ref(), &self.fd, len)?
            .splice()
            .await
    }
}

impl AsReadFd for Pipe {
    #[inline]
    fn as_reader_fd(&mut self) -> &SharedFdWrapper {
        SharedFdWrapper::new(&self.fd)
    }
}

impl AsWriteFd for Pipe {
    #[inline]
    fn as_writer_fd(&mut self) -> &SharedFdWrapper {
        SharedFdWrapper::new(&self.fd)
    }
}

impl IntoRawFd for Pipe {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self.fd
            .try_unwrap()
            .expect("unexpected multiple reference to rawfd")
    }
}

impl AsRawFd for Pipe {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd.raw_fd()
    }
}

impl From<Pipe> for Stdio {
    #[inline]
    fn from(pipe: Pipe) -> Self {
        let rawfd = pipe.fd.try_unwrap().unwrap();
        unsafe { Stdio::from_raw_fd(rawfd) }
    }
}

impl AsyncReadRent for Pipe {
    #[inline]
    fn read<T: IoBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        // Submit the read operation
        let op = Op::read(self.fd.clone(), buf);
        async move {
            match op {
                Ok(op) => op.result().await,
                Err((e, buf)) => (Err(e), buf),
            }
        }
    }

    #[inline]
    fn readv<T: IoVecBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        // Submit the read operation
        let op = Op::readv(self.fd.clone(), buf);
        async move {
            match op {
                Ok(op) => op.result().await,
                Err((e, buf)) => (Err(e), buf),
            }
        }
    }
}
