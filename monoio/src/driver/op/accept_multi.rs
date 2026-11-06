//! Multishot accept (`IORING_ACCEPT_MULTISHOT`, Linux 5.19+).

use std::{
    cell::{Cell, RefCell},
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
///
/// Re-arms itself when the kernel ends the multishot after a connection without an error (it
/// does so when it can't post another CQE, e.g. on a full CQ): the next poll submits a new
/// multishot accept, so the caller sees one uninterrupted stream. An error ends it (yielded
/// once, then `None`); so does a cancel.
pub(crate) struct AcceptMultiOp {
    // `None` once a re-arm failed to submit.
    op: Option<MultishotOp<AcceptMulti>>,
    // The kernel ended the multishot after a connection: submit it again on the next poll.
    rearm: Cell<bool>,
    // `cancel` was called: never re-arm.
    cancelled: Cell<bool>,
    // An error `poll_batch` met after it had taken connections: returned by the next poll.
    pending_err: Option<io::Error>,
}

impl AcceptMultiOp {
    fn from_op(op: MultishotOp<AcceptMulti>) -> Self {
        Self {
            op: Some(op),
            rearm: Cell::new(false),
            cancelled: Cell::new(false),
            pending_err: None,
        }
    }

    /// Start a multishot accept on `fd`, with `SOCK_*` `flags` for the new sockets.
    pub(crate) fn new(fd: &SharedFd, flags: libc::c_int) -> io::Result<Self> {
        let data = AcceptMulti {
            fd: fd.clone(),
            flags,
            direct: None,
        };
        MultishotOp::submit(data)
            .map(Self::from_op)
            .map_err(|(e, _)| e)
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
        MultishotOp::submit(data)
            .map(Self::from_op)
            .map_err(|(e, _)| e)
    }

    /// The `SOCK_*` flags set on every accepted socket.
    pub(crate) fn flags(&self) -> Option<libc::c_int> {
        self.op.as_ref().map(|op| op.data().flags)
    }

    /// Next accepted fd, owned by the caller. An error ends the accept loop when the kernel
    /// ends the multishot (it does for most errors); `-ECANCELED` after a cancel ends it
    /// silently with `None`. A connection without `F_MORE` makes the next poll re-arm.
    pub(crate) fn poll_next_fd(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<SharedFd>>> {
        if let Some(e) = self.pending_err.take() {
            return Poll::Ready(Some(Err(e)));
        }
        if self.rearm.replace(false) {
            let Some(op) = self.op.take() else {
                return Poll::Ready(None);
            };
            // The final CQE was taken, so the data is free.
            let Ok(data) = op.into_data() else {
                unreachable!("re-arm before the final CQE")
            };
            match MultishotOp::submit(data) {
                Ok(op) => self.op = Some(op),
                Err((e, _)) => return Poll::Ready(Some(Err(e))),
            }
        }
        let Some(op) = self.op.as_mut() else {
            return Poll::Ready(None);
        };
        let Some(cqe) = ready!(op.poll_next(cx)) else {
            return Poll::Ready(None);
        };
        Poll::Ready(match cqe.result() {
            Ok(res) => {
                if !cqe.more() && !self.cancelled.get() {
                    self.rearm.set(true);
                }
                match &op.data().direct {
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
                }
            }
            Err(e) if !cqe.more() && e.raw_os_error() == Some(libc::ECANCELED) => None,
            Err(e) => Some(Err(e)),
        })
    }

    /// Wait for at least one connection, then take every connection already queued, up to
    /// `max` (at least 1), in this one poll. `push` gets each fd and returns whether it was kept
    /// (a connection it drops does not count). `Ok(0)` only once the accept ended. An error
    /// after some connections is kept and returned by the next poll.
    pub(crate) fn poll_batch(
        &mut self,
        cx: &mut Context<'_>,
        max: usize,
        mut push: impl FnMut(SharedFd) -> bool,
    ) -> Poll<io::Result<usize>> {
        let max = max.max(1);
        let mut n = 0;
        while n < max {
            match self.poll_next_fd(cx) {
                Poll::Pending if n == 0 => return Poll::Pending,
                Poll::Pending | Poll::Ready(None) => break,
                Poll::Ready(Some(Err(e))) => {
                    if n == 0 {
                        return Poll::Ready(Err(e));
                    }
                    self.pending_err = Some(e);
                    break;
                }
                Poll::Ready(Some(Ok(fd))) => {
                    if push(fd) {
                        n += 1;
                    }
                }
            }
        }
        Poll::Ready(Ok(n))
    }

    /// Stop accepting (AsyncCancel); no re-arm after it.
    pub(crate) fn cancel(&self) {
        self.cancelled.set(true);
        self.rearm.set(false);
        if let Some(op) = &self.op {
            op.cancel()
        }
    }

    /// Whether the accept ended (`poll_next_fd` returns `None` from now on).
    pub(crate) fn is_finished(&self) -> bool {
        if self.rearm.get() || self.pending_err.is_some() {
            return false;
        }
        match &self.op {
            Some(op) => op.is_finished(),
            None => true,
        }
    }
}

/// The multishot accept a listener keeps for `accept_batch`: armed by the first call, kept
/// between calls (connections the kernel accepts in between are queued, not lost), and
/// re-created by the next call after it ended (after an error was returned).
#[derive(Default)]
pub(crate) struct BatchAcceptor {
    op: RefCell<Option<AcceptMultiOp>>,
    busy: Cell<bool>,
}

/// Clears `BatchAcceptor::busy` when an `accept` future completes or is dropped.
struct BusyGuard<'a>(&'a Cell<bool>);

impl Drop for BusyGuard<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

impl BatchAcceptor {
    /// Accept at least one and up to `max` connections on `fd` (see
    /// `AcceptMultiOp::poll_batch`). Never returns `Ok(0)`.
    pub(crate) async fn accept(
        &self,
        fd: &SharedFd,
        flags: libc::c_int,
        max: usize,
        mut push: impl FnMut(SharedFd) -> bool,
    ) -> io::Result<usize> {
        if self.busy.replace(true) {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "another task is in accept_batch on this listener",
            ));
        }
        let _guard = BusyGuard(&self.busy);
        let mut fresh = false;
        loop {
            {
                let mut op = self.op.borrow_mut();
                match &*op {
                    Some(armed) if !armed.is_finished() => {
                        if armed.flags() != Some(flags) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "accept_batch called with other flags than the armed accept",
                            ));
                        }
                    }
                    _ => {
                        *op = Some(AcceptMultiOp::new(fd, flags)?);
                        fresh = true;
                    }
                }
            }
            // The borrow is only held within a poll: `busy` keeps other tasks out, and dropping
            // this future leaves the accept armed with its queued connections.
            let res = std::future::poll_fn(|cx| match self.op.borrow_mut().as_mut() {
                Some(op) => op.poll_batch(cx, max, &mut push),
                None => Poll::Ready(Ok(0)),
            })
            .await;
            match res {
                // A new accept that ends without a connection or an error: don't spin.
                Ok(0) if fresh => {
                    *self.op.borrow_mut() = None;
                    return Err(io::Error::other("multishot accept ended at once"));
                }
                Ok(0) => *self.op.borrow_mut() = None,
                Ok(n) => return Ok(n),
                Err(e) => {
                    let mut op = self.op.borrow_mut();
                    if op.as_ref().is_some_and(|op| op.is_finished()) {
                        *op = None;
                    }
                    return Err(e);
                }
            }
        }
    }
}
