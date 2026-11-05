use std::os::unix::prelude::AsRawFd;

use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, submit::SubmitResult, Completion, Op, OpAble};
use crate::{
    buf::{IoBuf, IoVecBuf},
    io::futures::BufOp,
    BufResult,
};

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
    fd: SharedFd,
    pub(crate) buf: T,
}

impl<T: IoBuf> Op<Write<T>> {
    pub(crate) fn write(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        Op::submit_or_return(Write { fd, buf }).map_err(|(e, d)| (e, d.buf))
    }
}

impl<T: IoBuf> OpAble for Write<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        // Refers to https://docs.rs/io-uring/latest/io_uring/opcode/struct.Write.html.
        //
        // If `offset` is set to `-1`, the offset will use (and advance) the file position, like
        // the write(2) system calls
        opcode::Write::new(
            types::Fd(self.fd.as_raw_fd()),
            self.buf.read_ptr(),
            self.buf.bytes_init() as _,
        )
        .offset(-1i64 as _)
        .build()
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
        opcode::Write::new(
            types::Fd(self.fd.raw_fd()),
            self.buf.read_ptr(),
            self.buf.bytes_init() as _,
        )
        .offset(self.offset)
        .build()
    }
}

pub(crate) struct WriteVec<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,
    pub(crate) buf_vec: T,
}

impl<T: IoVecBuf> Op<WriteVec<T>> {
    pub(crate) fn writev(fd: SharedFd, buf_vec: T) -> SubmitResult<Self, T> {
        Op::submit_or_return(WriteVec { fd, buf_vec }).map_err(|(e, d)| (e, d.buf_vec))
    }
}

impl<T: IoVecBuf> OpAble for WriteVec<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let ptr = self.buf_vec.read_iovec_ptr() as *const _;
        let len = self.buf_vec.read_iovec_len() as _;
        // Refers to https://docs.rs/io-uring/latest/io_uring/opcode/struct.Write.html.
        //
        // If `offset` is set to `-1`, the offset will use (and advance) the file position, like
        // the writev(2) system calls
        opcode::Writev::new(types::Fd(self.fd.raw_fd()), ptr, len)
            .offset(-1i64 as u64)
            .build()
    }
}
