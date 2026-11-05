//! Multishot accept (`IORING_ACCEPT_MULTISHOT`, Linux 5.19+).

use std::{
    io,
    rc::Rc,
    task::{Context, Poll},
};

use io_uring::opcode;

use super::{multishot::MultishotOp, OpAble, OpKind};
use crate::driver::{
    fixed_files::{FileTable, FixedSlot},
    raw::Cqe,
    shared_fd::sqe_fd,
    shared_fd::SharedFd,
};

/// Multishot accept on a listening socket: one CQE per accepted connection.
pub(crate) struct AcceptMulti {
    fd: SharedFd,
    /// `SOCK_*` flags for every new socket (`accept4`).
    flags: libc::c_int,
    /// Direct accept: every socket goes into a slot the kernel allocates in this table
    /// (`IORING_FILE_INDEX_ALLOC`, within the kernel range of the table).
    direct: Option<Rc<FileTable>>,
}

impl OpAble for AcceptMulti {
    const KIND: OpKind = OpKind::Multishot;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let direct = self.direct.is_some();
        sqe_fd!(self.fd, |fd| opcode::AcceptMulti::new(fd)
            .flags(self.flags)
            .allocate_file_index(direct)
            .build())
    }

    fn discard_cqe(&mut self, cqe: Cqe) {
        // An accepted connection nobody takes: close it.
        if cqe.res() >= 0 {
            match &self.direct {
                // A slot the kernel allocated: clear it (a register syscall, not the driver).
                Some(table) => {
                    let _ = table.update_sync(cqe.res() as u32, -1);
                }
                // SAFETY: the kernel handed us this new fd, nothing else owns it.
                None => unsafe {
                    libc::close(cqe.res());
                },
            }
        }
    }
}

/// The accept loop shared by the TCP and Unix listeners: yields the accepted fds.
pub(crate) struct AcceptMultiOp(MultishotOp<AcceptMulti>);

impl AcceptMultiOp {
    /// Start a multishot accept on `fd`, with `SOCK_*` `flags` for the new sockets.
    pub(crate) fn new(fd: &SharedFd, flags: libc::c_int) -> io::Result<Self> {
        let data = AcceptMulti {
            fd: fd.clone(),
            flags,
            direct: None,
        };
        MultishotOp::submit(data).map(Self).map_err(|(e, _)| e)
    }

    /// Start a multishot direct accept on `fd`: the sockets go into kernel-allocated slots of
    /// the current ring's fixed file table. Needs the table's kernel range
    /// (`RuntimeBuilder::with_fixed_files_kernel_alloc`), else `Unsupported`. `SOCK_CLOEXEC` is
    /// dropped from `flags` (the kernel rejects it for direct descriptors).
    pub(crate) fn new_direct(fd: &SharedFd, flags: libc::c_int) -> io::Result<Self> {
        let table = FileTable::current_or_err()?;
        if !table.has_kernel_range() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "multishot direct accept needs kernel-allocated fixed file slots (see \
                 RuntimeBuilder::with_fixed_files_kernel_alloc)",
            ));
        }
        let data = AcceptMulti {
            fd: fd.clone(),
            flags: flags & !libc::SOCK_CLOEXEC,
            direct: Some(table),
        };
        MultishotOp::submit(data).map(Self).map_err(|(e, _)| e)
    }

    /// Next accepted fd, owned by the caller. An error ends the accept loop when the kernel
    /// ends the multishot (it does for most errors); `-ECANCELED` after a cancel ends it
    /// silently with `None`.
    pub(crate) fn poll_next_fd(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<SharedFd>>> {
        let Some(cqe) = ready!(self.0.poll_next(cx)) else {
            return Poll::Ready(None);
        };
        Poll::Ready(match cqe.result() {
            Ok(res) => match &self.0.data().direct {
                // A direct accept: `res` is the slot the kernel picked, now ours.
                Some(table) => {
                    table.adopt_kernel_slot();
                    Some(Ok(SharedFd::new_direct(FixedSlot {
                        table: table.clone(),
                        slot: res,
                    })))
                }
                // `SharedFd::new` takes ownership (it never fails today).
                None => Some(SharedFd::new(res as _)),
            },
            Err(e) if !cqe.more() && e.raw_os_error() == Some(libc::ECANCELED) => None,
            Err(e) => Some(Err(e)),
        })
    }

    pub(crate) fn cancel(&self) {
        self.0.cancel()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}
