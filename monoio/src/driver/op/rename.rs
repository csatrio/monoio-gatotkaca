use std::{ffi::CString, path::Path};

use super::{Op, OpAble};
use crate::driver::util::cstr;

pub(crate) struct Rename {
    from: CString,
    to: CString,
}

impl Op<Rename> {
    pub(crate) fn rename(from: &Path, to: &Path) -> std::io::Result<Self> {
        let from = cstr(from)?;
        let to = cstr(to)?;

        Op::submit_with(Rename { from, to })
    }
}

impl OpAble for Rename {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        use io_uring::{opcode::RenameAt, types};
        use libc::AT_FDCWD;

        RenameAt::new(
            types::Fd(AT_FDCWD),
            self.from.as_ptr(),
            types::Fd(AT_FDCWD),
            self.to.as_ptr(),
        )
        .build()
    }
}
