use std::io;

use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, Op, OpAble};
use crate::driver::shared_fd::sqe_fd;

pub(crate) struct Fsync {
    fd: SharedFd,
    data_sync: bool,
}

impl Op<Fsync> {
    pub(crate) fn fsync(fd: &SharedFd) -> io::Result<Op<Fsync>> {
        Op::submit_with(Fsync {
            fd: fd.clone(),
            data_sync: false,
        })
    }

    pub(crate) fn datasync(fd: &SharedFd) -> io::Result<Op<Fsync>> {
        Op::submit_with(Fsync {
            fd: fd.clone(),
            data_sync: true,
        })
    }
}

impl OpAble for Fsync {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let flags = if self.data_sync {
            types::FsyncFlags::DATASYNC
        } else {
            types::FsyncFlags::empty()
        };
        sqe_fd!(self.fd, |fd| opcode::Fsync::new(fd).flags(flags).build())
    }
}
