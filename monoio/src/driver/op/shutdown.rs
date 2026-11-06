//! `shutdown(2)` on a socket: a direct syscall when it has an fd, `IORING_OP_SHUTDOWN` (Linux
//! 5.11+) when it is direct-only (no fd).

use std::{future::Future, io};

use io_uring::opcode;

use super::{super::shared_fd::SharedFd, Op, OpAble};
use crate::driver::shared_fd::sqe_fd;

pub(crate) struct Shutdown {
    /// Keeps the socket open while the op is in flight.
    fd: SharedFd,
    how: libc::c_int,
}

impl Op<Shutdown> {
    pub(crate) fn shutdown(fd: &SharedFd, how: libc::c_int) -> io::Result<Op<Shutdown>> {
        Op::submit_with(Shutdown {
            fd: fd.clone(),
            how,
        })
    }

    pub(crate) async fn result(self) -> io::Result<()> {
        self.await.meta.result.map(|_| ())
    }
}

impl OpAble for Shutdown {
    // The shutdown is requested when the SQE is pushed. The kernel always runs it in an io-wq
    // worker (`REQ_F_FORCE_ASYNC`), where a cancel pushed by a dropped future could still catch
    // it: a caller that does not wait for the result must not undo the request.
    const SKIP_CANCEL: bool = true;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        sqe_fd!(self.fd, |fd| opcode::Shutdown::new(fd, self.how).build())
    }
}

/// The `SHUT_*` value of a [`std::net::Shutdown`].
pub(crate) fn how_to_raw(how: std::net::Shutdown) -> libc::c_int {
    match how {
        std::net::Shutdown::Read => libc::SHUT_RD,
        std::net::Shutdown::Write => libc::SHUT_WR,
        std::net::Shutdown::Both => libc::SHUT_RDWR,
    }
}

/// Shut down one or both directions of the socket `fd` (`how` is a `SHUT_*` value).
///
/// The work is done when this is called, not when the future is polled:
/// - with an fd: one `shutdown(2)` syscall; the future is ready at its first poll;
/// - direct-only: an `IORING_OP_SHUTDOWN` SQE is pushed; the future resolves with its CQE.
///   Dropping the future does not cancel the shutdown.
///
/// The future borrows nothing, so another task can call this on a socket whose owner has ops
/// in flight.
pub(crate) fn shutdown(fd: &SharedFd, how: libc::c_int) -> impl Future<Output = io::Result<()>> {
    let state = if fd.is_direct() {
        Err(Op::shutdown(fd, how))
    } else {
        // SAFETY: a plain syscall on an fd we keep open.
        Ok(match unsafe { libc::shutdown(fd.raw_fd(), how) } {
            -1 => Err(io::Error::last_os_error()),
            _ => Ok(()),
        })
    };
    async move {
        match state {
            Ok(res) => res,
            Err(op) => op?.result().await,
        }
    }
}
