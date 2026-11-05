//! Reads and writes bounded by a kernel link timeout.

use std::time::Duration;

use super::TcpStream;
use crate::{
    buf::{IoBuf, IoBufMut},
    driver::{op::link_timeout, shared_fd::SharedFd},
    BufResult,
};

impl TcpStream {
    /// Receive into `buf`, like [`read`](crate::io::AsyncReadRent::read), but give up after
    /// `timeout`: the result is then an [`ErrorKind::TimedOut`](std::io::ErrorKind::TimedOut)
    /// error, and the buffer comes back untouched.
    ///
    /// The deadline is a kernel link timeout (`IORING_OP_LINK_TIMEOUT`) submitted with the
    /// receive: one extra SQE, no timer-wheel entry, so it works without `enable_timer`. The
    /// stream stays usable after a timeout. Other errors are returned as they are.
    pub async fn read_with_deadline<T: IoBufMut>(
        &self,
        buf: T,
        timeout: Duration,
    ) -> BufResult<usize, T> {
        link_timeout::recv_with_timeout(self.fd.clone(), buf, timeout).await
    }

    /// Send the initialized bytes of `buf` (one `send` with `MSG_NOSIGNAL`, which may be
    /// partial, like [`write`](crate::io::AsyncWriteRent::write)), but give up after `timeout`
    /// with an [`ErrorKind::TimedOut`](std::io::ErrorKind::TimedOut) error.
    ///
    /// A send that timed out may have queued part of the data on the socket already; the
    /// kernel does not report how much. See [`read_with_deadline`](Self::read_with_deadline)
    /// for how the deadline works.
    pub async fn write_with_deadline<T: IoBuf>(
        &self,
        buf: T,
        timeout: Duration,
    ) -> BufResult<usize, T> {
        link_timeout::send_with_timeout(self.fd.clone(), buf, timeout).await
    }

    /// The socket, for ops built outside of this module (linked chains).
    pub(crate) fn shared_fd(&self) -> &SharedFd {
        &self.fd
    }
}
