//! File → socket through one linked chain (read with `IO_LINK`, then send).

use io_uring::opcode;

use super::File;
use crate::{
    buf::IoBufMut,
    driver::{raw::ChainOp, shared_fd::sqe_fd, CURRENT},
    net::TcpStream,
    BufResult,
};

impl File {
    /// Read `buf.bytes_total()` bytes at `pos` into `buf` and send them on `stream`, as one
    /// linked chain: a `Read` with `IO_LINK` and a `Send` (`MSG_NOSIGNAL | MSG_WAITALL`) of the
    /// same memory, submitted together. It saves a round trip through the runtime between the
    /// two, compared to `read_at` then `write_all`.
    ///
    /// Returns `(read, sent)`. `read` is always the full capacity on success; `sent` can be
    /// less on kernels that ignore `MSG_WAITALL` for `send` (before 5.18): send the rest from
    /// `buf[sent..]` yourself. The buffer comes back with its initialized length set to what
    /// was read.
    ///
    /// The send always uses the length fixed when the chain is built, since io_uring cannot
    /// pass the read count to it. So the file range must be fully present: if the read is
    /// short (end of file) or fails, the kernel breaks the link and cancels the send, and this
    /// returns the read error, or [`ErrorKind::UnexpectedEof`](std::io::ErrorKind::UnexpectedEof)
    /// for a short read. Nothing was sent then. If the send fails, its error is returned.
    ///
    /// Do not drop the future midway if you need to know what was sent: the chain runs to the
    /// end anyway (the buffer is kept alive until then), but its results are lost.
    pub async fn read_at_then_send<T: IoBufMut>(
        &self,
        mut buf: T,
        pos: u64,
        stream: &TcpStream,
    ) -> BufResult<(usize, usize), T> {
        let len = buf.bytes_total();
        let Ok(len32) = u32::try_from(len) else {
            return (
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "buffer larger than u32::MAX",
                )),
                buf,
            );
        };
        let ptr = buf.write_ptr();
        let read = sqe_fd!(self.fd, |fd| opcode::Read::new(fd, ptr, len32)
            .offset(pos)
            .build());
        let send = sqe_fd!(stream.shared_fd(), |fd| opcode::Send::new(fd, ptr, len32)
            .flags(libc::MSG_NOSIGNAL | libc::MSG_WAITALL)
            .build());
        // The chain owns both fds (kept open until it finished) and the buffer, whose memory
        // does not move with it (`IoBufMut` contract).
        let data = (self.fd.clone(), stream.shared_fd().clone(), buf);
        let sqes = crate::uring::link([read, send]);
        let op = match CURRENT.with(|inner| ChainOp::submit_in(inner, sqes, data)) {
            Ok(op) => op,
            Err((e, (_, _, buf))) => return (Err(e), buf),
        };
        let (cqes, (_, _, mut buf)) = op.await;
        let (read, sent) = (cqes[0].result(), cqes[1].result());
        let res = match read {
            Err(e) => Err(e),
            Ok(n) => {
                // SAFETY: the kernel wrote `n` bytes to the buffer.
                unsafe { buf.set_init(n as usize) };
                if (n as usize) < len {
                    Err(std::io::ErrorKind::UnexpectedEof.into())
                } else {
                    sent.map(|s| (len, s as usize))
                }
            }
        };
        (res, buf)
    }
}
