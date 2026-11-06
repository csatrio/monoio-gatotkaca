//! Socket control that also works without an fd: `shutdown` of any direction and socket
//! options. A stream with an fd uses the plain syscalls; a direct-only stream
//! ([`TcpStream::is_direct`]) goes through the ring.

use std::{future::Future, io, net::Shutdown, time::Duration};

use super::TcpStream;
use crate::driver::op::{shutdown, sockopt};

/// The error of the synchronous socket-option calls on a direct-only stream.
pub(super) fn direct_only_option_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "direct-only socket (no file descriptor): socket options go through the ring, which a \
         synchronous call cannot wait for. Use the async TcpStream::set_sockopt / \
         set_nodelay_async / set_tcp_keepalive_async (Linux 6.7+) and get_sockopt, or set the \
         option on the listener",
    )
}

impl TcpStream {
    /// Shut down the read side, the write side or both, like `shutdown(2)`.
    ///
    /// Takes `&self` and returns a future that borrows nothing, so a supervisor task (a
    /// request deadline, an admin "kick") can call it while the connection's own task has ops
    /// in flight. Those ops then complete: receives with `Ok(0)` (EOF) or an error, sends with
    /// `EPIPE`. The peer reads what was sent before, then EOF.
    ///
    /// The shutdown is requested when this is called, not when the future is polled:
    /// - a stream with an fd makes one `shutdown(2)` syscall; the future is ready at once;
    /// - a direct-only stream ([`is_direct`](Self::is_direct)) pushes `IORING_OP_SHUTDOWN`
    ///   (Linux 5.11+), which the kernel always runs in an io-wq worker; the future resolves
    ///   with its completion. Dropping the future does not cancel it.
    ///
    /// [`AsyncWriteRent::shutdown`](crate::io::AsyncWriteRent::shutdown) is
    /// `shutdown_with(Shutdown::Write)` with `&mut self`.
    pub fn shutdown_with(&self, how: Shutdown) -> impl Future<Output = io::Result<()>> {
        shutdown::shutdown(&self.fd, shutdown::how_to_raw(how))
    }

    /// [`shutdown_with`](Self::shutdown_with)`(Shutdown::Both)`: ends the connection for every
    /// op in flight on it, without closing it.
    pub fn shutdown_both(&self) -> impl Future<Output = io::Result<()>> {
        self.shutdown_with(Shutdown::Both)
    }

    /// Set the socket option `name` of `level` (`libc::SOL_SOCKET`, `libc::IPPROTO_TCP`, ...)
    /// to `value`, like `setsockopt(2)`. Works on a direct-only stream too.
    ///
    /// The call is made when this is called, not when the future is polled:
    /// - a stream with an fd makes one `setsockopt(2)` syscall; the future is ready at once;
    /// - a direct-only stream ([`is_direct`](Self::is_direct)) copies `value` and pushes
    ///   `IORING_OP_URING_CMD` / `SOCKET_URING_OP_SETSOCKOPT` (Linux 6.7+; `Unsupported`
    ///   before): no syscall of its own, the result comes with the next completions. Dropping
    ///   the future does not cancel it.
    ///
    /// `value` is passed to the kernel byte for byte (`size_of::<T>()` bytes): use the C type
    /// the option wants, usually `libc::c_int`. If the value holds pointers (e.g.
    /// `SO_ATTACH_FILTER`), what they point to must stay valid until the future resolved.
    pub fn set_sockopt<T: Copy>(
        &self,
        level: i32,
        name: i32,
        value: &T,
    ) -> impl Future<Output = io::Result<()>> {
        sockopt::set(&self.fd, level, name, value)
    }

    /// Read the socket option `name` of `level` as a `T`, like `getsockopt(2)`. Bytes of `T`
    /// the kernel does not write (an option shorter than `T`) are zero.
    ///
    /// A stream with an fd makes one `getsockopt(2)` syscall when this is called. A
    /// direct-only stream pushes `IORING_OP_URING_CMD` / `SOCKET_URING_OP_GETSOCKOPT` (Linux
    /// 6.7+), which the kernel supports **only for `SOL_SOCKET` options**: other levels (so
    /// `TCP_NODELAY`, `TCP_INFO`, ...) and older kernels give `Unsupported`.
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

    /// [`set_nodelay`](Self::set_nodelay) that also works on a direct-only stream (through
    /// [`set_sockopt`](Self::set_sockopt): Linux 6.7+ there, a plain syscall with an fd).
    pub fn set_nodelay_async(&self, nodelay: bool) -> impl Future<Output = io::Result<()>> {
        self.set_sockopt(
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &libc::c_int::from(nodelay),
        )
    }

    /// [`set_tcp_keepalive`](Self::set_tcp_keepalive) that also works on a direct-only stream:
    /// turns `SO_KEEPALIVE` on and sets the given `TCP_KEEPIDLE` (`time`), `TCP_KEEPINTVL`
    /// (`interval`) and `TCP_KEEPCNT` (`retries`), each through
    /// [`set_sockopt`](Self::set_sockopt). All of them are requested when this is called; the
    /// future returns the first error.
    pub fn set_tcp_keepalive_async(
        &self,
        time: Option<Duration>,
        interval: Option<Duration>,
        retries: Option<u32>,
    ) -> impl Future<Output = io::Result<()>> {
        // Whole seconds, as the kernel (and socket2) take them.
        let secs = |d: Duration| d.as_secs().min(libc::c_int::MAX as u64) as libc::c_int;
        let tcp = |name, value: Option<libc::c_int>| {
            value.map(|v| self.set_sockopt(libc::IPPROTO_TCP, name, &v))
        };
        let on = self.set_sockopt(libc::SOL_SOCKET, libc::SO_KEEPALIVE, &(1 as libc::c_int));
        let opts = [
            tcp(libc::TCP_KEEPIDLE, time.map(secs)),
            tcp(libc::TCP_KEEPINTVL, interval.map(secs)),
            tcp(
                libc::TCP_KEEPCNT,
                retries.map(|n| n.min(libc::c_int::MAX as u32) as libc::c_int),
            ),
        ];
        async move {
            let mut res = on.await;
            for opt in opts.into_iter().flatten() {
                // Await every one: they are in flight, and the first error is the answer.
                let r = opt.await;
                if res.is_ok() {
                    res = r;
                }
            }
            res
        }
    }
}
