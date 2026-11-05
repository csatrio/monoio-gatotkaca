use std::io;

use io_uring::opcode;

use super::{super::shared_fd::SharedFd, Op, OpAble};
use crate::driver::shared_fd::sqe_fd;

/// `IORING_OP_FADVISE` (5.6+).
pub(crate) struct Fadvise {
    fd: SharedFd,
    offset: u64,
    len: u32,
    advice: i32,
}

impl Op<Fadvise> {
    pub(crate) fn fadvise(
        fd: &SharedFd,
        offset: u64,
        len: u32,
        advice: i32,
    ) -> io::Result<Op<Fadvise>> {
        Op::submit_with(Fadvise {
            fd: fd.clone(),
            offset,
            len,
            advice,
        })
    }
}

impl OpAble for Fadvise {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        sqe_fd!(self.fd, |fd| opcode::Fadvise::new(
            fd,
            self.len as _,
            self.advice
        )
        .offset(self.offset)
        .build())
    }
}
