use std::{
    fs::File as StdFile,
    io,
    os::fd::{AsRawFd, IntoRawFd, RawFd},
};

pub(crate) use iouring::*;

use super::File;
use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut},
    driver::{op::Op, shared_fd::SharedFd},
    fs::{metadata::FileAttr, Metadata},
};

impl File {
    /// Converts a [`std::fs::File`] to a [`monoio::fs::File`](File).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// // This line could block. It is not recommended to do this on the monoio
    /// // runtime.
    /// let std_file = std::fs::File::open("foo.txt").unwrap();
    /// let file = monoio::fs::File::from_std(std_file);
    /// ```
    pub fn from_std(std: StdFile) -> std::io::Result<File> {
        Ok(File {
            fd: SharedFd::new_without_register(std.into_raw_fd()),
        })
    }

    /// Queries metadata about the underlying file.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use monoio::fs::File;
    ///
    /// #[monoio::main]
    /// async fn main() -> std::io::Result<()> {
    ///     let mut f = File::open("foo.txt").await?;
    ///     let metadata = f.metadata().await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// io_uring always runs `STATX` in an io-wq worker (measured on Linux 7.0), so avoid it on
    /// a per-request path.
    ///
    /// # Errors
    ///
    /// `Unsupported` for a direct-only file ([`is_direct`](Self::is_direct)): io_uring's
    /// `STATX` resolves a path relative to a directory fd and does not take a fixed file, so
    /// there is nothing to stat it with. Use [`fs::metadata`](crate::fs::metadata) on the path.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        if self.fd.is_direct() {
            return Err(crate::driver::shared_fd::direct_only_error());
        }
        metadata(self.fd.clone()).await
    }

    /// Put the file into the runtime's fixed file table
    /// ([`RuntimeBuilder::with_fixed_files`](crate::RuntimeBuilder::with_fixed_files)): every
    /// op submitted from now on names the table slot (`IOSQE_FIXED_FILE`) instead of the fd,
    /// which saves the kernel a file lookup and an atomic refcount pair per op.
    ///
    /// The fd stays open, so [`as_raw_fd`](AsRawFd::as_raw_fd) and
    /// [`metadata`](Self::metadata) keep working. Closing or dropping the file closes the slot
    /// and the fd. A no-op if the file already has a slot. Fails with `Unsupported` if the ring
    /// has no fixed file table and with `ENFILE` if it is full.
    pub fn register_fixed(&self) -> io::Result<()> {
        self.fd.register_fixed()
    }

    /// Whether the file is in the fixed file table (registered, or direct-only).
    pub fn is_fixed(&self) -> bool {
        self.fd.fixed_slot().is_some()
    }

    /// Whether the file is direct-only: opened with
    /// [`OpenOptions::open_direct`](crate::fs::OpenOptions::open_direct), it lives only in the
    /// fixed file table and has no fd. [`as_raw_fd`](AsRawFd::as_raw_fd) then returns `-1` and
    /// [`metadata`](Self::metadata) returns `Unsupported`; the positional ops, `fadvise`,
    /// `sync_*`, splice and `read_at_then_send` work.
    pub fn is_direct(&self) -> bool {
        self.fd.is_direct()
    }

    /// The file's slot in the fixed file table, for hand-built SQEs with `types::Fixed`.
    pub fn fixed_slot(&self) -> Option<u32> {
        self.fd.fixed_slot()
    }
}

impl AsRawFd for File {
    /// The file's fd; `-1` for a direct-only file ([`File::is_direct`]).
    fn as_raw_fd(&self) -> RawFd {
        self.fd.raw_fd()
    }
}

pub(crate) async fn metadata(fd: SharedFd) -> std::io::Result<Metadata> {
    let flags = libc::AT_STATX_SYNC_AS_STAT | libc::AT_EMPTY_PATH;
    let op = Op::statx_using_fd(fd, flags)?;

    op.result().await.map(FileAttr::from).map(Metadata)
}

mod iouring {
    use super::*;
    use crate::uring_op;

    uring_op!(read<IoBufMut>(read, buf));
    uring_op!(read_vectored<IoVecBufMut>(readv, buf_vec));

    uring_op!(write<IoBuf>(write, buf));
    uring_op!(write_vectored<IoVecBuf>(writev, buf_vec));
}
