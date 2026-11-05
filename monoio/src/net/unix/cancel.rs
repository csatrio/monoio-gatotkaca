//! Cancel every in-flight op of a connection with one SQE.

use std::io;

use super::UnixStream;
use crate::driver::op::cancel_fd::cancel_fd;

impl UnixStream {
    /// Ask the kernel to cancel every in-flight operation on this socket with one SQE, see
    /// [`TcpStream::cancel_all`](crate::net::TcpStream::cancel_all) (Linux 5.19+).
    pub fn cancel_all(&self) -> io::Result<()> {
        cancel_fd(&self.fd)
    }
}
