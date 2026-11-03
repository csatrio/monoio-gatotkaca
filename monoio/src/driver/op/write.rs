use std::io;
use std::os::unix::prelude::AsRawFd;

use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, Op, OpAble};
use crate::{
    buf::{IoBuf, IoVecBuf},
    BufResult,
};

macro_rules! write_result {
    ($($name:ident<$T:ident : $Trait:ident> { $buf:ident }), * $(,)?) => {
        $(
            impl<$T: $Trait> super::Op<$name<$T>> {
                pub(crate) async fn result(self) -> BufResult<usize, $T> {
                    let complete = self.await;
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

write_result! {
    WriteVecAt<T: IoVecBuf> { buf_vec },
}

pub(crate) struct Write<T> {
    fd: SharedFd,
    pub(crate) buf: T,
}

impl<T: IoBuf> Op<Write<T>> {
    pub(crate) fn write(fd: SharedFd, buf: T) -> io::Result<Self> {
        Op::submit_with(Write { fd, buf })
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
    #[allow(unused)]
    fd: SharedFd,
    /// Refers to https://docs.rs/io-uring/latest/io_uring/opcode/struct.Write.html.
    ///
    /// If `offset` is set to `-1`, the offset will use (and advance) the file position, like
    /// the write(2) system calls.
    offset: u64,
    pub(crate) buf: T,
}

impl<T: IoBuf> Op<WriteAt<T>> {
    pub(crate) fn write_at(fd: SharedFd, buf: T, offset: u64) -> io::Result<Op<WriteAt<T>>> {
        Op::submit_with(WriteAt { fd, offset, buf })
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
    pub(crate) fn writev(fd: SharedFd, buf_vec: T) -> io::Result<Self> {
        Op::submit_with(WriteVec { fd, buf_vec })
    }

    pub(crate) fn writev_raw(fd: &SharedFd, buf_vec: T) -> WriteVec<T> {
        WriteVec {
            fd: fd.clone(),
            buf_vec,
        }
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

pub(crate) struct WriteVecAt<T> {
    fd: SharedFd,
    /// Refers to https://docs.rs/io-uring/latest/io_uring/opcode/struct.Write.html.
    ///
    /// If `offset` is set to `-1`, the offset will use (and advance) the file position, like
    /// the writev(2) system calls.
    offset: u64,
    buf_vec: T,
}

impl<T: IoVecBuf> OpAble for WriteVecAt<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::Writev::new(
            types::Fd(libc::AT_FDCWD),
            self.buf_vec.read_iovec_ptr(),
            self.buf_vec.read_iovec_len() as _,
        )
        .offset(self.offset)
        .build()
    }
}
