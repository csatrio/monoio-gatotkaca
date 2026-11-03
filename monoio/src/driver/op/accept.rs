use std::{
    io,
    mem::{size_of, MaybeUninit},
};

use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, Op, OpAble};

/// Accept
pub(crate) struct Accept {
    pub(crate) fd: SharedFd,
    pub(crate) addr: Box<(MaybeUninit<libc::sockaddr_storage>, libc::socklen_t)>,
}

impl Op<Accept> {
    /// Accept a connection
    pub(crate) fn accept(fd: &SharedFd) -> io::Result<Self> {
        let addr = Box::new((
            MaybeUninit::uninit(),
            size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        ));

        Op::submit_with(Accept {
            fd: fd.clone(),
            addr,
        })
    }
}

impl OpAble for Accept {
    const RET_IS_FD: bool = true;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::Accept::new(
            types::Fd(self.fd.raw_fd()),
            self.addr.0.as_mut_ptr() as *mut _,
            &mut self.addr.1,
        )
        .build()
    }
}
