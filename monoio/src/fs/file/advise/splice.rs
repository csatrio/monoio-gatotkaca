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
    /// # Always an io-wq punt
    ///
    /// io_uring never runs `IORING_OP_SPLICE` inline: its prep sets `REQ_F_FORCE_ASYNC` (the
    /// splice code can block on the page cache and on pipe locks, and has no non-blocking issue
    /// path), so every call is handed to an io-wq worker (`iou-wrk-*` thread), even when the
    /// data is cached. `SPLICE_F_NONBLOCK` does not change that (it only affects the pipe side).
    /// Measured on Linux 7.0 (f2fs and ext4, warm cache): 100% of splices punted, about one
    /// worker context switch per call; see `tests/fs_punt_probe.rs`. The hand-off adds
    /// latency, the workers' CPU is charged to the process, and under load io-wq starts many
    /// workers (cap them with
    /// [`RuntimeBuilder::iowq_max_workers`](crate::RuntimeBuilder::iowq_max_workers)).
    ///
    /// So splice is for large, cold files, where the read blocks anyway and the copy saved is
    /// worth a worker. For files likely in the page cache, read into memory inline instead:
    /// [`read_fixed_at`](Self::read_fixed_at) then `TcpStream::send_fixed`, or
    /// [`read_at_then_send`](Self::read_at_then_send) (one linked chain). A warm buffered read
    /// completes in the submitting `io_uring_enter`; a cold one completes from task work, still
    /// without an io-wq worker.
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
    /// used or changed. Waits (in an io-wq worker) while the pipe is empty. Like
    /// [`splice_to_pipe`](Self::splice_to_pipe), always runs in an io-wq worker.
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
