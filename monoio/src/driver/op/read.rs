use std::io;

use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, Op, OpAble};
use crate::{
    buf::{IoBufMut, IoVecBufMut},
    BufResult,
};

macro_rules! read_result {
    ($($name:ident<$T:ident : $Trait:ident> { $buf:ident }),* $(,)?) => {
        $(
            impl<$T: $Trait> super::Op<$name<$T>> {
                pub(crate) async fn result(self) -> BufResult<usize, $T> {
                    let complete = self.await;

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

read_result! {
    ReadVecAt<T: IoVecBufMut> { buf_vec },
}

pub(crate) struct Read<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,
    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
}

impl<T: IoBufMut> Op<Read<T>> {
    pub(crate) fn read(fd: SharedFd, buf: T) -> io::Result<Op<Read<T>>> {
        Op::submit_with(Read { fd, buf })
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
    pub(crate) fn read_at(fd: SharedFd, buf: T, offset: u64) -> io::Result<Op<ReadAt<T>>> {
        Op::submit_with(ReadAt { fd, offset, buf })
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
    pub(crate) fn readv(fd: SharedFd, buf_vec: T) -> io::Result<Self> {
        Op::submit_with(ReadVec { fd, buf_vec })
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

pub(crate) struct ReadVecAt<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,
    /// Reference to the in-flight buffer.
    pub(crate) buf_vec: T,
    offset: u64,
}

impl<T: IoVecBufMut> Op<ReadVecAt<T>> {
    pub(crate) fn read_vectored_at(fd: SharedFd, buf_vec: T, offset: u64) -> io::Result<Self> {
        Op::submit_with(ReadVecAt {
            fd,
            buf_vec,
            offset,
        })
    }
}

impl<T: IoVecBufMut> OpAble for ReadVecAt<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let ptr = self.buf_vec.write_iovec_ptr() as _;
        let len = self.buf_vec.write_iovec_len() as _;
        opcode::Readv::new(types::Fd(self.fd.raw_fd()), ptr, len)
            .offset(self.offset)
            .build()
    }
}
