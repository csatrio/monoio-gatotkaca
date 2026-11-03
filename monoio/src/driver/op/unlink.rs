use std::{ffi::CString, io, path::Path};

use io_uring::{opcode, squeue::Entry, types::Fd};
use libc::{AT_FDCWD, AT_REMOVEDIR};

use super::{Op, OpAble};
use crate::driver::util::cstr;

pub(crate) struct Unlink {
    path: CString,
    remove_dir: bool,
}

impl Op<Unlink> {
    pub(crate) fn unlink<P: AsRef<Path>>(path: P) -> io::Result<Op<Unlink>> {
        let path = cstr(path.as_ref())?;
        Op::submit_with(Unlink {
            path,
            remove_dir: false,
        })
    }

    pub(crate) fn rmdir<P: AsRef<Path>>(path: P) -> io::Result<Op<Unlink>> {
        let path = cstr(path.as_ref())?;
        Op::submit_with(Unlink {
            path,
            remove_dir: true,
        })
    }
}

impl OpAble for Unlink {
    fn uring_op(&mut self) -> Entry {
        opcode::UnlinkAt::new(Fd(AT_FDCWD), self.path.as_c_str().as_ptr())
            .flags(if self.remove_dir { AT_REMOVEDIR } else { 0 })
            .build()
    }
}
