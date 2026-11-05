use std::{
    io,
    mem::{size_of, MaybeUninit},
};

use io_uring::{opcode, types::DestinationSlot};

use super::{super::shared_fd::SharedFd, Op, OpAble};
use crate::driver::{
    fixed_files::{FileTable, SlotRelease},
    shared_fd::sqe_fd,
};

/// Accept
pub(crate) struct Accept {
    pub(crate) fd: SharedFd,
    pub(crate) addr: Box<(MaybeUninit<libc::sockaddr_storage>, libc::socklen_t)>,
    /// `SOCK_*` flags for the new socket (`accept4`).
    flags: libc::c_int,
}

impl Op<Accept> {
    /// Accept a connection
    pub(crate) fn accept(fd: &SharedFd) -> io::Result<Self> {
        Self::accept_with(fd, 0)
    }

    /// Accept a connection, with `SOCK_*` flags for the new socket.
    pub(crate) fn accept_with(fd: &SharedFd, flags: libc::c_int) -> io::Result<Self> {
        let addr = Box::new((
            MaybeUninit::uninit(),
            size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        ));

        Op::submit_with(Accept {
            fd: fd.clone(),
            addr,
            flags,
        })
    }
}

impl OpAble for Accept {
    const RET_IS_FD: bool = true;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        sqe_fd!(self.fd, |fd| opcode::Accept::new(
            fd,
            self.addr.0.as_mut_ptr() as *mut _,
            &mut self.addr.1,
        )
        .flags(self.flags)
        .build())
    }
}

/// Accept straight into a fixed file table slot (a direct descriptor, Linux 5.19+). The slot
/// is chosen by the runtime's allocator before the SQE is pushed.
pub(crate) struct AcceptDirect {
    fd: SharedFd,
    pub(crate) addr: Box<(MaybeUninit<libc::sockaddr_storage>, libc::socklen_t)>,
    flags: libc::c_int,
    /// The reserved slot. If nobody takes it (the op failed, or was dropped and completed
    /// anyway), dropping it clears the slot and frees it, so an accepted socket never leaks.
    pub(crate) slot: Option<SlotRelease>,
}

impl Op<AcceptDirect> {
    /// Accept a connection into a free slot of the current ring's fixed file table.
    ///
    /// `SOCK_CLOEXEC` is dropped from `flags`: the kernel rejects it for direct descriptors
    /// (there is no fd to close on exec).
    pub(crate) fn accept_direct(fd: &SharedFd, flags: libc::c_int) -> io::Result<Self> {
        let table = FileTable::current_or_err()?;
        let slot = table.alloc()?;
        Op::submit_with(AcceptDirect {
            fd: fd.clone(),
            addr: Box::new((
                MaybeUninit::uninit(),
                size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            )),
            flags: flags & !libc::SOCK_CLOEXEC,
            slot: Some(SlotRelease::reserved(table, slot)),
        })
    }
}

impl OpAble for AcceptDirect {
    // The result is 0 (the socket is in our slot), not an fd: never close it.
    const RET_IS_FD: bool = false;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        // Slots are below the kernel's table limit (2^20), so this always converts.
        let dest = self
            .slot
            .as_ref()
            .and_then(|s| DestinationSlot::try_from_slot_target(s.slot()).ok());
        let (addr, len) = (
            self.addr.0.as_mut_ptr() as *mut _,
            &mut self.addr.1 as *mut _,
        );
        sqe_fd!(self.fd, |fd| opcode::Accept::new(fd, addr, len)
            .flags(self.flags)
            .file_index(dest)
            .build())
    }
}
