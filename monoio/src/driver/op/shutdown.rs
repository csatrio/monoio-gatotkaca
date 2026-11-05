//! `IORING_OP_SHUTDOWN` (Linux 5.11+), for sockets without an fd (direct descriptors).

use std::io;

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
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        sqe_fd!(self.fd, |fd| opcode::Shutdown::new(fd, self.how).build())
    }
}
