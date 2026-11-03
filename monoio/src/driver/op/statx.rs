use std::{ffi::CString, mem::MaybeUninit, path::Path};

use io_uring::{opcode, types};
use libc::statx;

use super::{Op, OpAble};
use crate::driver::{shared_fd::SharedFd, util::cstr};

#[derive(Debug)]
pub(crate) struct Statx<T> {
    inner: T,
    flags: i32,
    statx_buf: Box<MaybeUninit<statx>>,
}

type FdStatx = Statx<SharedFd>;

impl Op<FdStatx> {
    /// submit a statx operation
    pub(crate) fn statx_using_fd(fd: SharedFd, flags: i32) -> std::io::Result<Self> {
        Op::submit_with(Statx {
            inner: fd,
            flags,
            statx_buf: Box::new(MaybeUninit::uninit()),
        })
    }

    pub(crate) async fn result(self) -> std::io::Result<statx> {
        let complete = self.await;
        complete.meta.result?;

        Ok(unsafe { MaybeUninit::assume_init(*complete.data.statx_buf) })
    }
}

impl OpAble for FdStatx {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        use std::os::fd::AsRawFd;

        let statxbuf = self.statx_buf.as_mut_ptr() as *mut _;

        opcode::Statx::new(types::Fd(self.inner.as_raw_fd()), c"".as_ptr(), statxbuf)
            .flags(libc::AT_EMPTY_PATH | libc::AT_STATX_SYNC_AS_STAT)
            .mask(libc::STATX_ALL)
            .build()
    }
}

type PathStatx = Statx<CString>;

impl Op<PathStatx> {
    /// submit a statx operation
    pub(crate) fn statx_using_path<P: AsRef<Path>>(path: P, flags: i32) -> std::io::Result<Self> {
        let path = cstr(path.as_ref())?;
        Op::submit_with(Statx {
            inner: path,
            flags,
            statx_buf: Box::new(MaybeUninit::uninit()),
        })
    }

    pub(crate) async fn result(self) -> std::io::Result<statx> {
        let complete = self.await;
        complete.meta.result?;

        Ok(unsafe { MaybeUninit::assume_init(*complete.data.statx_buf) })
    }
}

impl OpAble for PathStatx {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let statxbuf = self.statx_buf.as_mut_ptr() as *mut _;

        opcode::Statx::new(types::Fd(libc::AT_FDCWD), self.inner.as_ptr(), statxbuf)
            .flags(self.flags)
            .mask(libc::STATX_ALL)
            .build()
    }
}
