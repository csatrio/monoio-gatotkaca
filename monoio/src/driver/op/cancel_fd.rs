//! Cancel every in-flight op on a file with one SQE (`IORING_ASYNC_CANCEL_FD |
//! IORING_ASYNC_CANCEL_ALL`).

use std::io;

use crate::driver::{
    self,
    shared_fd::{FdRef, SharedFd},
    uring::{CancelTarget, UringInner},
};

/// Push one SQE that cancels every in-flight op on `fd` (see `UringInner::cancel_fd`). Uses the
/// fixed slot when the file has one.
pub(crate) fn cancel_fd(fd: &SharedFd) -> io::Result<()> {
    let target = match fd.fd_ref() {
        FdRef::Raw(raw) => CancelTarget::Fd(raw),
        FdRef::Fixed(slot) => CancelTarget::Fixed(slot),
    };
    if !driver::CURRENT.is_set() {
        return Err(io::Error::other("not inside a monoio runtime"));
    }
    driver::CURRENT.with(|inner| UringInner::cancel_fd(&inner.0, target))
}
