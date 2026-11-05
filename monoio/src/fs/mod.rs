//! Filesystem manipulation operations.

mod file;
use std::{io, path::Path};

pub use file::{advise::Advice, File};

#[cfg(feature = "mkdirat")]
mod dir_builder;
#[cfg(feature = "mkdirat")]
pub use dir_builder::DirBuilder;

#[cfg(feature = "mkdirat")]
mod create_dir;
#[cfg(feature = "mkdirat")]
pub use create_dir::*;

#[cfg(feature = "symlinkat")]
mod symlink;
#[cfg(feature = "symlinkat")]
pub use symlink::symlink;

mod open_options;
pub use open_options::OpenOptions;

mod metadata;
pub use metadata::{metadata, symlink_metadata, Metadata};

mod file_type;
pub use file_type::FileType;

mod permissions;

pub use permissions::Permissions;

use crate::buf::IoBuf;

/// A macro that generates the some Op-call functions.
#[macro_export]
macro_rules! uring_op {
    ($fn_name:ident<$trait_name:ident>($op_name: ident, $buf_name:ident $(, $pos:ident: $pos_type:ty)?)) => {
        pub(crate) async fn $fn_name<T: $trait_name>(fd: SharedFd, $buf_name: T, $($pos: $pos_type)?) -> $crate::BufResult<usize, T> {
            match $crate::driver::op::Op::$op_name(fd, $buf_name, $($pos)?) {
                Ok(op) => op.result().await,
                Err((e, buf)) => (Err(e), buf),
            }
        }
    };
}

/// Read the entire contents of a file into a bytes vector.
pub async fn read<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    use crate::buf::IoBufMut;

    let file = File::open(path).await?;

    let size = file.metadata().await?.len() as usize;

    let (res, buf) = file
        .read_exact_at(Vec::with_capacity(size).slice_mut(0..size), 0)
        .await;
    res?;
    Ok(buf.into_inner())
}

/// Write a buffer as the entire contents of a file.
pub async fn write<P: AsRef<Path>, C: IoBuf>(path: P, contents: C) -> (io::Result<()>, C) {
    match File::create(path).await {
        Ok(f) => f.write_all_at(contents, 0).await,
        Err(e) => (Err(e), contents),
    }
}

/// Removes a file from the filesystem.
///
/// Note that there is no
/// guarantee that the file is immediately deleted (e.g., depending on
/// platform, other open file descriptors may prevent immediate removal).
///
/// # Platform-specific behavior
///
/// This function is currently only implemented for Unix.
///
/// # Errors
///
/// This function will return an error in the following situations, but is not
/// limited to just these cases:
///
/// * `path` points to a directory.
/// * The file doesn't exist.
/// * The user lacks permissions to remove the file.
///
/// # Examples
///
/// ```no_run
/// #[monoio::main]
/// async fn main() -> std::io::Result<()> {
///     monoio::fs::remove_file("a.txt").await?;
///     Ok(())
/// }
/// ```
#[cfg(feature = "unlinkat")]
pub async fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    crate::driver::op::Op::unlink(path)?.await.meta.result?;
    Ok(())
}

/// Removes an empty directory.
///
/// # Platform-specific behavior
///
/// This function is currently only implemented for Unix.
///
/// # Errors
///
/// This function will return an error in the following situations, but is not
/// limited to just these cases:
///
/// * `path` doesn't exist.
/// * `path` isn't a directory.
/// * The user lacks permissions to remove the directory at the provided `path`.
/// * The directory isn't empty.
///
/// # Examples
///
/// ```no_run
/// #[monoio::main]
/// async fn main() -> std::io::Result<()> {
///     monoio::fs::remove_dir("/some/dir").await?;
///     Ok(())
/// }
/// ```
#[cfg(feature = "unlinkat")]
pub async fn remove_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    crate::driver::op::Op::rmdir(path)?.await.meta.result?;
    Ok(())
}

/// Rename a file or directory to a new name, replacing the original file if
/// `to` already exists.
///
/// This will not work if the new name is on a different mount point.
///
/// This is async version of [std::fs::rename].
///
/// # Errors
///
/// This function will return an error in the following situations, but is not
/// limited to just these cases:
///
/// * `from` does not exist.
/// * The user lacks permissions to view contents.
/// * `from` and `to` are on separate filesystems.
///
/// # Examples
///
/// ```no_run
/// #[monoio::main]
/// async fn main() -> std::io::Result<()> {
///     monoio::fs::rename("a.txt", "b.txt").await?; // Rename a.txt to b.txt
///     Ok(())
/// }
/// ```
#[cfg(feature = "renameat")]
pub async fn rename<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<()> {
    crate::driver::op::Op::rename(from.as_ref(), to.as_ref())?
        .await
        .meta
        .result?;
    Ok(())
}
