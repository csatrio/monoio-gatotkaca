//! Multishot accept (`IORING_ACCEPT_MULTISHOT`, Linux 5.19+).

use std::{
    io,
    task::{Context, Poll},
};

use io_uring::{opcode, types};

use super::{multishot::MultishotOp, OpAble, OpKind};
use crate::driver::{raw::Cqe, shared_fd::SharedFd};

/// Multishot accept on a listening socket: one CQE per accepted connection.
pub(crate) struct AcceptMulti {
    fd: SharedFd,
    /// `SOCK_*` flags for every new socket (`accept4`).
    flags: libc::c_int,
}

impl OpAble for AcceptMulti {
    const KIND: OpKind = OpKind::Multishot;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::AcceptMulti::new(types::Fd(self.fd.raw_fd()))
            .flags(self.flags)
            .build()
    }

    fn discard_cqe(&mut self, cqe: Cqe) {
        // An accepted connection nobody takes: close it.
        if cqe.res() >= 0 {
            // SAFETY: the kernel handed us this new fd, nothing else owns it.
            unsafe { libc::close(cqe.res()) };
        }
    }
}

/// The accept loop shared by the TCP and Unix listeners: yields the accepted fds.
pub(crate) struct AcceptMultiOp(MultishotOp<AcceptMulti>);

impl AcceptMultiOp {
    /// Start a multishot accept on `fd`, with `SOCK_*` `flags` for the new sockets.
    pub(crate) fn new(fd: &SharedFd, flags: libc::c_int) -> io::Result<Self> {
        let data = AcceptMulti {
            fd: fd.clone(),
            flags,
        };
        MultishotOp::submit(data).map(Self).map_err(|(e, _)| e)
    }

    /// Next accepted fd, owned by the caller. An error ends the accept loop when the kernel
    /// ends the multishot (it does for most errors); `-ECANCELED` after a cancel ends it
    /// silently with `None`.
    pub(crate) fn poll_next_fd(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<SharedFd>>> {
        let Some(cqe) = ready!(self.0.poll_next(cx)) else {
            return Poll::Ready(None);
        };
        Poll::Ready(match cqe.result() {
            // `SharedFd::new` takes ownership (it never fails today).
            Ok(fd) => Some(SharedFd::new(fd as _)),
            Err(e) if !cqe.more() && e.raw_os_error() == Some(libc::ECANCELED) => None,
            Err(e) => Some(Err(e)),
        })
    }

    pub(crate) fn cancel(&self) {
        self.0.cancel()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}
