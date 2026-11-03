use std::io;
use std::os::unix::io::RawFd;

use io_uring::{opcode, types};

use super::{Op, OpAble};

pub(crate) struct Close {
    fd: RawFd,
}

impl Op<Close> {
    pub(crate) fn close(fd: RawFd) -> io::Result<Op<Close>> {
        Op::try_submit_with(Close { fd })
    }
}

impl OpAble for Close {
    const SKIP_CANCEL: bool = true;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::Close::new(types::Fd(self.fd)).build()
    }
}
