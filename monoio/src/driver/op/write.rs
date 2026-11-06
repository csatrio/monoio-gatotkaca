use io_uring::opcode;

use super::{super::shared_fd::SharedFd, submit::SubmitResult, Completion, Op, OpAble};
use crate::{
    buf::{IoBuf, IoVecBuf},
    driver::shared_fd::{sqe_fd, TxFd},
    io::futures::{BufOp, WritevFuture},
    BufResult,
};

// Which of these count as a send in flight (`TxFd`, see `InFlight` in `shared_fd.rs`): `Write`
// and `WriteVec`, the writes without an offset, which is what the socket streams use.
// `WriteAt` is the files' op and stays as it was: files have no `try_send`.

macro_rules! write_result {
    ($($name:ident<$T:ident : $Trait:ident> { $buf:ident }), * $(,)?) => {
        $(
            impl<$T: $Trait> super::Op<$name<$T>> {
                // Unused for the `*At` ops, which only go through the named futures.
                #[allow(dead_code)]
                pub(crate) async fn result(self) -> BufResult<usize, $T> {
                    <$name<$T> as BufOp>::complete(self.await)
                }
            }

            impl<$T: $Trait> BufOp for $name<$T> {
                type Buf = $T;

                fn complete(complete: Completion<Self>) -> BufResult<usize, $T> {
                    (complete.meta.result.map(|v| v.into_inner() as _), complete.data.$buf)
                }
            }
        )*
    };
}

write_result! {
    Write<T: IoBuf> { buf },
    WriteAt<T: IoBuf> { buf },
    WriteVec<T: IoVecBuf> { buf_vec },
}

pub(crate) struct Write<T> {
    /// Counts as a send in flight.
    fd: TxFd,
    pub(crate) buf: T,
}

impl<T: IoBuf> Op<Write<T>> {
    pub(crate) fn write(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        Op::submit_or_return(Write {
            fd: TxFd::new(fd),
            buf,
        })
        .map_err(|(e, d)| (e, d.buf))
    }
}

impl<T: IoBuf> OpAble for Write<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        // Refers to https://docs.rs/io-uring/latest/io_uring/opcode/struct.Write.html.
        //
        // If `offset` is set to `-1`, the offset will use (and advance) the file position, like
        // the write(2) system calls
        sqe_fd!(self.fd, |fd| opcode::Write::new(
            fd,
            self.buf.read_ptr(),
            self.buf.bytes_init() as _,
        )
        .offset(-1i64 as _)
        .build())
    }
}

pub(crate) struct WriteAt<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,
    /// Refers to https://docs.rs/io-uring/latest/io_uring/opcode/struct.Write.html.
    ///
    /// If `offset` is set to `-1`, the offset will use (and advance) the file position, like
    /// the write(2) system calls.
    offset: u64,
    pub(crate) buf: T,
}

impl<T: IoBuf> Op<WriteAt<T>> {
    pub(crate) fn write_at(fd: SharedFd, buf: T, offset: u64) -> SubmitResult<Op<WriteAt<T>>, T> {
        Op::submit_or_return(WriteAt { fd, offset, buf }).map_err(|(e, d)| (e, d.buf))
    }
}

impl<T: IoBuf> OpAble for WriteAt<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        sqe_fd!(self.fd, |fd| opcode::Write::new(
            fd,
            self.buf.read_ptr(),
            self.buf.bytes_init() as _,
        )
        .offset(self.offset)
        .build())
    }
}

pub(crate) struct WriteVec<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight. Counts as a send in flight.
    fd: TxFd,
    pub(crate) buf_vec: T,
}

impl<T: IoVecBuf> Op<WriteVec<T>> {
    pub(crate) fn writev(fd: SharedFd, buf_vec: T) -> SubmitResult<Self, T> {
        Op::submit_or_return(WriteVec {
            fd: TxFd::new(fd),
            buf_vec,
        })
        .map_err(|(e, d)| (e, d.buf_vec))
    }
}

/// `writev` of a stream socket with `set_eager_send(true)`: one direct, non-blocking
/// `sendmsg(2)` (`MSG_NOSIGNAL`) from the calling thread first.
///
/// - It took something (all, or a part): a [`WritevFuture`] that is resolved already, with
///   that count. A part is a short write, which every `writev` caller has to handle anyway, so
///   no offset into the iovecs is needed.
/// - The socket buffer is full, a ring send is in flight on the socket (ordering) or the
///   socket has no fd: the ring `WRITEV`, as without the switch.
/// - A real error: a resolved future with it.
///
/// Never inlined, like `send_eager`: `writev` callers that never turn the switch on must not
/// grow.
#[inline(never)]
pub(crate) fn writev_eager<T: IoVecBuf>(fd: &SharedFd, buf_vec: T) -> WritevFuture<T> {
    if fd.can_send_now() {
        // SAFETY: `IoVecBuf`: the pointer is valid for that many iovecs, and their memory is
        // readable, while `buf_vec` lives.
        let res =
            unsafe { fd.send_vectored_now(buf_vec.read_iovec_ptr(), buf_vec.read_iovec_len()) };
        match res {
            Ok(n) => return WritevFuture::ready(Ok(n), buf_vec),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return WritevFuture::ready(Err(e), buf_vec),
        }
    }
    WritevFuture::new(Op::writev(fd.clone(), buf_vec))
}

impl<T: IoVecBuf> OpAble for WriteVec<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let ptr = self.buf_vec.read_iovec_ptr() as *const _;
        let len = self.buf_vec.read_iovec_len() as _;
        // Refers to https://docs.rs/io-uring/latest/io_uring/opcode/struct.Write.html.
        //
        // If `offset` is set to `-1`, the offset will use (and advance) the file position, like
        // the writev(2) system calls
        sqe_fd!(self.fd, |fd| opcode::Writev::new(fd, ptr, len)
            .offset(-1i64 as u64)
            .build())
    }
}
