use std::{ffi::CString, io, path::Path};

use io_uring::{opcode, types};

use super::{Op, OpAble};
use crate::{driver::util::cstr, fs::OpenOptions};

/// Open a file
pub(crate) struct Open {
    pub(crate) path: CString,
    flags: i32,
    mode: libc::mode_t,
}

impl Op<Open> {
    /// Submit a request to open a file.
    pub(crate) fn open<P: AsRef<Path>>(path: P, options: &OpenOptions) -> io::Result<Op<Open>> {
        // Here the path will be copied, so its safe.
        let path = cstr(path.as_ref())?;
        let flags = libc::O_CLOEXEC
            | options.access_mode()?
            | options.creation_mode()?
            | (options.custom_flags & !libc::O_ACCMODE);
        let mode = options.mode;

        Op::submit_with(Open { path, flags, mode })
    }
}

impl OpAble for Open {
    const RET_IS_FD: bool = true;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), self.path.as_c_str().as_ptr())
            .flags(self.flags)
            .mode(self.mode)
            .build()
    }
}
