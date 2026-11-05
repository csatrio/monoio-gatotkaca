use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, submit::SubmitResult, Completion, Op, OpAble};
use crate::{
    buf::{IoBufMut, IoVecBufMut},
    io::futures::BufOp,
    BufResult,
};

macro_rules! read_result {
    ($($name:ident<$T:ident : $Trait:ident> { $buf:ident }),* $(,)?) => {
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
                    // Convert the operation result to `usize`
                    let res = complete.meta.result.map(|v| v.into_inner() as usize);
                    // Recover the buffer
                    let mut buf = complete.data.$buf;

                    if let Ok(read_len) = res {
                        // Safety: the kernel wrote `n` bytes to the buffer
                        unsafe { buf.set_init(read_len) };
                    }

                    (res, buf)
                }
            }
        )*
    }
}

read_result! {
    Read<T: IoBufMut> { buf },
    ReadAt<T: IoBufMut> { buf },
    ReadVec<T: IoVecBufMut> { buf_vec },
}

pub(crate) struct Read<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,
    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
}

impl<T: IoBufMut> Op<Read<T>> {
    pub(crate) fn read(fd: SharedFd, buf: T) -> SubmitResult<Op<Read<T>>, T> {
        Op::submit_or_return(Read { fd, buf }).map_err(|(e, d)| (e, d.buf))
    }
}

impl<T: IoBufMut> OpAble for Read<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        // Refers to https://docs.rs/io-uring/latest/io_uring/opcode/struct.Read.html.
        // If `offset` is set to `-1`, the offset will use (and advance) the file position, like
        // the read(2) syscall.
        opcode::Read::new(
            types::Fd(self.fd.raw_fd()),
            self.buf.write_ptr(),
            self.buf.bytes_total() as _,
        )
        .offset(-1i64 as u64)
        .build()
    }
}

pub(crate) struct ReadAt<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,
    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
    offset: u64,
}

impl<T: IoBufMut> Op<ReadAt<T>> {
    pub(crate) fn read_at(fd: SharedFd, buf: T, offset: u64) -> SubmitResult<Op<ReadAt<T>>, T> {
        Op::submit_or_return(ReadAt { fd, offset, buf }).map_err(|(e, d)| (e, d.buf))
    }
}

impl<T: IoBufMut> OpAble for ReadAt<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::Read::new(
            types::Fd(self.fd.raw_fd()),
            self.buf.write_ptr(),
            self.buf.bytes_total() as _,
        )
        .offset(self.offset)
        .build()
    }
}

pub(crate) struct ReadVec<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,
    /// Reference to the in-flight buffer.
    pub(crate) buf_vec: T,
}

impl<T: IoVecBufMut> Op<ReadVec<T>> {
    pub(crate) fn readv(fd: SharedFd, buf_vec: T) -> SubmitResult<Self, T> {
        Op::submit_or_return(ReadVec { fd, buf_vec }).map_err(|(e, d)| (e, d.buf_vec))
    }
}

impl<T: IoVecBufMut> OpAble for ReadVec<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let ptr = self.buf_vec.write_iovec_ptr() as _;
        let len = self.buf_vec.write_iovec_len() as _;

        // Refersto https://docs.rs/io-uring/latest/io_uring/opcode/struct.Readv.html.
        // If `offset` is set to `-1`, the offset will use (and advance) the file position, like
        // the readv(2) syscall.
        opcode::Readv::new(types::Fd(self.fd.raw_fd()), ptr, len)
            .offset(-1i64 as u64)
            .build()
    }
}
