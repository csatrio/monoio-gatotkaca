//! File <-> pipe splice with explicit file offsets.

use std::io;

use super::File;
use crate::{driver::op::Op, net::unix::Pipe};

fn file_offset(offset: u64) -> io::Result<i64> {
    i64::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset larger than i64::MAX"))
}

impl File {
    /// Move up to `len` bytes from the file, starting at `offset`, into the write end of `pipe`
    /// (`IORING_OP_SPLICE`, Linux 5.7+, feature `splice`). Returns the number of bytes moved;
    /// `0` means `offset` is at or past the end of the file.
    ///
    /// The file has no cursor in monoio, so the offset is explicit and the file's own position
    /// is not used or changed. The data goes from the page cache to the pipe without a copy
    /// through user space; empty the pipe with a [`SpliceDestination`] (for example a
    /// `TcpStream`) or [`Pipe::splice_to`]. A pipe holds 64 KiB by default, so one call moves at
    /// most that much.
    ///
    /// io_uring runs a splice from a regular file in an io-wq worker (`iou-wrk-*` thread); cap
    /// them with [`RuntimeBuilder::iowq_max_workers`](crate::RuntimeBuilder::iowq_max_workers).
    ///
    /// # Errors
    ///
    /// `InvalidInput` if `offset` is larger than `i64::MAX`; otherwise the kernel's error (e.g.
    /// `EINVAL` if `pipe` is not a pipe, `EBADF` if it is the read end).
    ///
    /// [`SpliceDestination`]: crate::io::splice::SpliceDestination
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use monoio::{fs::File, io::splice::SpliceDestination, net::TcpStream};
    ///
    /// #[monoio::main]
    /// async fn main() -> std::io::Result<()> {
    ///     let file = File::open("blob.bin").await?;
    ///     let mut sock = TcpStream::connect("127.0.0.1:8080").await?;
    ///     let (mut rx, tx) = monoio::net::unix::new_pipe()?;
    ///     let mut offset = 0u64;
    ///     loop {
    ///         let n = file.splice_to_pipe(&tx, offset, 64 * 1024).await?;
    ///         if n == 0 {
    ///             break;
    ///         }
    ///         offset += n as u64;
    ///         let mut left = n;
    ///         while left > 0 {
    ///             left -= sock.splice_from_pipe(&mut rx, left).await?;
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn splice_to_pipe(&self, pipe: &Pipe, offset: u64, len: u32) -> io::Result<u32> {
        let off = file_offset(offset)?;
        Op::splice_at(&self.fd, off, &pipe.fd, -1, len)?
            .splice()
            .await
    }

    /// Move up to `len` bytes from the read end of `pipe` into the file at `offset`
    /// (`IORING_OP_SPLICE`, Linux 5.7+, feature `splice`). Returns the number of bytes moved;
    /// `0` means the write end is closed and the pipe is empty. The file's own position is not
    /// used or changed. Waits (in an io-wq worker) while the pipe is empty.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if `offset` is larger than `i64::MAX`; otherwise the kernel's error (e.g.
    /// `EBADF` if the file is not open for writing or `pipe` is the write end).
    pub async fn splice_from_pipe(&self, pipe: &Pipe, offset: u64, len: u32) -> io::Result<u32> {
        let off = file_offset(offset)?;
        Op::splice_at(&pipe.fd, -1, &self.fd, off, len)?
            .splice()
            .await
    }
}
