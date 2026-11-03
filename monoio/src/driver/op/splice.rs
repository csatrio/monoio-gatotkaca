//! This module works only on linux.

use std::io;

use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, Op, OpAble};

// Currently our Splice does not support setting offset.
pub(crate) struct Splice {
    fd_in: SharedFd,
    fd_out: SharedFd,
    len: u32,
}

impl Op<Splice> {
    pub(crate) fn splice_to_pipe(
        fd_in: &SharedFd,
        fd_out: &SharedFd,
        len: u32,
    ) -> io::Result<Op<Splice>> {
        Op::submit_with(Splice {
            fd_in: fd_in.clone(),
            fd_out: fd_out.clone(),
            len,
        })
    }

    pub(crate) fn splice_from_pipe(
        fd_in: &SharedFd,
        fd_out: &SharedFd,
        len: u32,
    ) -> io::Result<Op<Splice>> {
        Op::submit_with(Splice {
            fd_in: fd_in.clone(),
            fd_out: fd_out.clone(),
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
            -1,
            types::Fd(self.fd_out.raw_fd()),
            -1,
            self.len,
        )
        .flags(FLAG)
        .build()
    }
}
