//! Cancel every in-flight op of a connection with one SQE.

use std::io;

use super::TcpStream;
use crate::driver::op::cancel_fd::cancel_fd;

impl TcpStream {
    /// Ask the kernel to cancel every in-flight operation on this socket, with one
    /// `IORING_OP_ASYNC_CANCEL` (`IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_ALL`, Linux
    /// 5.19+). Covers ops started through any handle of the socket (split halves too).
    ///
    /// The SQE is queued now and submitted with the next submit. Keep awaiting the cancelled
    /// futures: each resolves with `ECANCELED` (os error 125) and its buffer, or with its
    /// result if it completed first. On older kernels the cancel fails silently and the ops
    /// run on. Counted in [`DriverStats::cancel_fd_pushed`](crate::DriverStats).
    ///
    /// Errors if called outside of a monoio runtime or the submission queue stayed full.
    pub fn cancel_all(&self) -> io::Result<()> {
        cancel_fd(&self.fd)
    }
}
