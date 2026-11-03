use std::io;

use super::{super::shared_fd::SharedFd, Op, OpAble};

pub(crate) struct PollAdd {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(unused)]
    fd: SharedFd,
    // true: read; false: write
    is_read: bool,
}

impl Op<PollAdd> {
    pub(crate) fn poll_read(fd: &SharedFd) -> io::Result<Op<PollAdd>> {
        Op::submit_with(PollAdd {
            fd: fd.clone(),
            is_read: true,
        })
    }

    pub(crate) fn poll_write(fd: &SharedFd) -> io::Result<Op<PollAdd>> {
        Op::submit_with(PollAdd {
            fd: fd.clone(),
            is_read: false,
        })
    }

    pub(crate) async fn wait(self) -> io::Result<()> {
        let complete = self.await;
        complete.meta.result.map(|_| ())
    }
}

impl OpAble for PollAdd {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        use io_uring::{opcode, types};

        opcode::PollAdd::new(
            types::Fd(self.fd.raw_fd()),
            if self.is_read {
                libc::POLLIN as _
            } else {
                libc::POLLOUT as _
            },
        )
        .build()
    }
}
