//! `shutdown` of any direction and socket options for [`UnixStream`], see the TCP twin
//! (`net/tcp/sockctl.rs`).
//!
//! A `UnixStream` always has an fd today (there is no direct accept for Unix listeners), so
//! these are plain syscalls. The code is the shared one, which uses the ring for a
//! direct-only socket.

use std::{future::Future, io, net::Shutdown};

use super::UnixStream;
use crate::driver::op::{shutdown, sockopt};

impl UnixStream {
    /// Shut down the read side, the write side or both, like `shutdown(2)`: one syscall, done
    /// when this is called; the future is ready at once.
    ///
    /// Takes `&self` and returns a future that borrows nothing, so a supervisor task can call
    /// it while the connection's own task has ops in flight; those ops then complete
    /// (receives with `Ok(0)` or an error, sends with `EPIPE`). See
    /// [`TcpStream::shutdown_with`](crate::net::TcpStream::shutdown_with).
    pub fn shutdown_with(&self, how: Shutdown) -> impl Future<Output = io::Result<()>> {
        shutdown::shutdown(&self.fd, shutdown::how_to_raw(how))
    }

    /// [`shutdown_with`](Self::shutdown_with)`(Shutdown::Both)`.
    pub fn shutdown_both(&self) -> impl Future<Output = io::Result<()>> {
        self.shutdown_with(Shutdown::Both)
    }

    /// Set the socket option `name` of `level` to `value`, like `setsockopt(2)`: one syscall,
    /// done when this is called. `value` is passed byte for byte: use the C type the option
    /// wants, usually `libc::c_int`. See
    /// [`TcpStream::set_sockopt`](crate::net::TcpStream::set_sockopt).
    pub fn set_sockopt<T: Copy>(
        &self,
        level: i32,
        name: i32,
        value: &T,
    ) -> impl Future<Output = io::Result<()>> {
        sockopt::set(&self.fd, level, name, value)
    }

    /// Read the socket option `name` of `level` as a `T`, like `getsockopt(2)`: one syscall,
    /// done when this is called. Bytes of `T` the kernel does not write are zero.
    ///
    /// # Safety
    /// Every bit pattern the kernel may write for this option, and all-zero, must be a valid
    /// `T`: integers and the `libc` option structs are fine, `bool`, enums and references are
    /// not.
    pub unsafe fn get_sockopt<T: Copy>(
        &self,
        level: i32,
        name: i32,
    ) -> impl Future<Output = io::Result<T>> {
        // SAFETY: the caller's contract.
        unsafe { sockopt::get(&self.fd, level, name) }
    }
}
