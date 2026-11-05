//! This module works only on linux.

use std::io;

use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, Op, OpAble};

/// A splice between two fds. An offset of `-1` means "use (and advance) the fd's own position";
/// it must be `-1` for the pipe end.
pub(crate) struct Splice {
    fd_in: SharedFd,
    off_in: i64,
    fd_out: SharedFd,
    off_out: i64,
    len: u32,
}

impl Op<Splice> {
    pub(crate) fn splice_to_pipe(
        fd_in: &SharedFd,
        fd_out: &SharedFd,
        len: u32,
    ) -> io::Result<Op<Splice>> {
        Self::splice_at(fd_in, -1, fd_out, -1, len)
    }

    pub(crate) fn splice_from_pipe(
        fd_in: &SharedFd,
        fd_out: &SharedFd,
        len: u32,
    ) -> io::Result<Op<Splice>> {
        Self::splice_at(fd_in, -1, fd_out, -1, len)
    }

    /// Splice with explicit offsets (`-1` for none). Offsets above `i64::MAX` are rejected.
    pub(crate) fn splice_at(
        fd_in: &SharedFd,
        off_in: i64,
        fd_out: &SharedFd,
        off_out: i64,
        len: u32,
    ) -> io::Result<Op<Splice>> {
        Op::submit_with(Splice {
            fd_in: fd_in.clone(),
            off_in,
            fd_out: fd_out.clone(),
            off_out,
            len,
        })
    }

    pub(crate) async fn splice(self) -> io::Result<u32> {
        let complete = self.await;
        complete.meta.result.map(|v| v.into_inner())
    }
}

impl OpAble for Splice {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        const FLAG: u32 = libc::SPLICE_F_MOVE;
        opcode::Splice::new(
            types::Fd(self.fd_in.raw_fd()),
            self.off_in,
            types::Fd(self.fd_out.raw_fd()),
            self.off_out,
            self.len,
        )
        .flags(FLAG)
        .build()
    }
}
