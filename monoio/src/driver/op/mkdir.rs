use std::{ffi::CString, path::Path};

use libc::mode_t;

use super::{Op, OpAble};
use crate::driver::util::cstr;

pub(crate) struct MkDir {
    path: CString,
    mode: mode_t,
}

impl Op<MkDir> {
    pub(crate) fn mkdir<P: AsRef<Path>>(path: P, mode: mode_t) -> std::io::Result<Op<MkDir>> {
        let path = cstr(path.as_ref())?;
        Op::submit_with(MkDir { path, mode })
    }
}

impl OpAble for MkDir {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        use io_uring::{opcode, types};

        opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), self.path.as_ptr())
            .mode(self.mode)
            .build()
    }
}
