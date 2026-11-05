//! File access hints (`fadvise`) and, with the `splice` feature, file <-> pipe splicing.

use std::io;

use super::File;
use crate::driver::op::Op;

#[cfg(feature = "splice")]
mod splice;

/// Access pattern hint for [`File::fadvise`] (`posix_fadvise(2)` advice values).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Advice {
    /// No special treatment (`POSIX_FADV_NORMAL`): the default readahead.
    Normal,
    /// The range will be read in order (`POSIX_FADV_SEQUENTIAL`): Linux doubles the readahead
    /// window.
    Sequential,
    /// The range will be read in random order (`POSIX_FADV_RANDOM`): Linux turns readahead off.
    Random,
    /// The range will be read only once (`POSIX_FADV_NOREUSE`). A no-op before Linux 6.3.
    NoReuse,
    /// The range will be needed soon (`POSIX_FADV_WILLNEED`): Linux starts reading it into the
    /// page cache in the background.
    WillNeed,
    /// The range will not be needed soon (`POSIX_FADV_DONTNEED`): Linux drops its clean pages
    /// from the page cache.
    DontNeed,
}

impl From<Advice> for i32 {
    #[inline]
    fn from(advice: Advice) -> i32 {
        match advice {
            Advice::Normal => libc::POSIX_FADV_NORMAL,
            Advice::Sequential => libc::POSIX_FADV_SEQUENTIAL,
            Advice::Random => libc::POSIX_FADV_RANDOM,
            Advice::NoReuse => libc::POSIX_FADV_NOREUSE,
            Advice::WillNeed => libc::POSIX_FADV_WILLNEED,
            Advice::DontNeed => libc::POSIX_FADV_DONTNEED,
        }
    }
}

impl File {
    /// Announce how the byte range `offset..offset + len` of the file will be accessed
    /// (`IORING_OP_FADVISE`, the io_uring form of `posix_fadvise(2)`, Linux 5.6+).
    ///
    /// `len == 0` means "up to the end of the file". It is only a hint: the kernel may ignore it,
    /// and it never changes what reads return.
    ///
    /// # Errors
    ///
    /// - `InvalidInput` if `len` is larger than `u32::MAX` (the SQE length field is 32 bits; use
    ///   `0` for "to the end" or split the range).
    /// - The kernel's error otherwise, e.g. `ESPIPE` for a pipe or `EINVAL` for an unknown advice.
    ///   A full submission queue that cannot be flushed is reported too.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use monoio::fs::{Advice, File};
    ///
    /// #[monoio::main]
    /// async fn main() -> std::io::Result<()> {
    ///     let file = File::open("big.dat").await?;
    ///     // Random reads over the whole file: turn readahead off.
    ///     file.fadvise(0, 0, Advice::Random).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn fadvise(&self, offset: u64, len: u64, advice: Advice) -> io::Result<()> {
        self.fadvise_raw(offset, len, advice.into()).await
    }

    /// Like [`fadvise`](Self::fadvise), but takes a raw advice value (`libc::POSIX_FADV_*`, see
    /// `monoio::uring::libc`), for values [`Advice`] does not list.
    pub async fn fadvise_raw(&self, offset: u64, len: u64, advice: i32) -> io::Result<()> {
        let len = u32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "fadvise length larger than u32::MAX",
            )
        })?;
        let op = Op::fadvise(&self.fd, offset, len, advice)?;
        op.await.meta.result?;
        Ok(())
    }
}
