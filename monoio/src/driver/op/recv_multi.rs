//! Multishot recv into a provided buffer ring (`IORING_RECV_MULTISHOT`, Linux 6.0+).

use std::{
    io,
    task::{Context, Poll},
};

use io_uring::{opcode, types};

use super::{multishot::MultishotOp, OpAble, OpKind};
use crate::driver::{
    raw::{BufRing, Cqe, SelectedBuf},
    shared_fd::SharedFd,
};

/// Multishot recv: one CQE per received chunk, each in a buffer of `ring`.
pub(crate) struct RecvMulti {
    fd: SharedFd,
    ring: BufRing,
}

impl OpAble for RecvMulti {
    const KIND: OpKind = OpKind::Multishot;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::RecvMulti::new(types::Fd(self.fd.raw_fd()), self.ring.group()).build()
    }

    fn discard_cqe(&mut self, cqe: Cqe) {
        // Give the buffer it carries back to the ring. `self.ring` keeps the group alive, so
        // this never calls into the driver.
        drop(self.ring.take(cqe));
    }
}

/// The receive loop shared by TCP and Unix streams: yields the filled buffers.
pub(crate) struct RecvMultiOp {
    op: MultishotOp<RecvMulti>,
    // The peer closed (a 0-length CQE): yield `None` from now on.
    eof: bool,
}

impl RecvMultiOp {
    /// Start a multishot recv on `fd` into buffers of `ring`.
    pub(crate) fn new(fd: &SharedFd, ring: &BufRing) -> io::Result<Self> {
        let data = RecvMulti {
            fd: fd.clone(),
            ring: ring.clone(),
        };
        let op = MultishotOp::submit(data).map_err(|(e, _)| e)?;
        Ok(Self { op, eof: false })
    }

    /// Next filled buffer. `None` after EOF, after the final CQE, or after a cancel
    /// (`-ECANCELED`). `ENOBUFS` (the ring ran dry) is yielded as an error; the kernel ends the
    /// multishot with it, so `None` follows.
    pub(crate) fn poll_next_buf(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<SelectedBuf>>> {
        if self.eof {
            return Poll::Ready(None);
        }
        let Some(cqe) = ready!(self.op.poll_next(cx)) else {
            return Poll::Ready(None);
        };
        let ring = &self.op.data().ring;
        Poll::Ready(match cqe.res() {
            0 => {
                // EOF: a buffer it may carry goes back to the ring.
                drop(ring.take(cqe));
                self.eof = true;
                if cqe.more() {
                    self.op.cancel();
                }
                None
            }
            n if n > 0 => match ring.take(cqe) {
                Some(buf) => Some(Ok(buf)),
                None => Some(Err(io::Error::other(
                    "recv completion without a provided buffer",
                ))),
            },
            n if n == -libc::ECANCELED && !cqe.more() => None,
            _ => {
                // Counts ENOBUFS in the ring stats.
                drop(ring.take(cqe));
                Some(Err(io::Error::from_raw_os_error(-cqe.res())))
            }
        })
    }

    pub(crate) fn cancel(&self) {
        self.op.cancel()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.eof || self.op.is_finished()
    }
}
