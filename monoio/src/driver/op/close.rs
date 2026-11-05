use std::{io, os::unix::io::RawFd};

use io_uring::{opcode, types};

use super::{Op, OpAble};
use crate::driver::{
    fixed_files::{FixedSlot, SlotRelease},
    shared_fd::FdRef,
    uring::UringInner,
};

pub(crate) struct Close {
    fd: FdRef,
    /// For a fixed slot: gives the slot back to its table once the close completed (dropped
    /// with the op data, which the driver keeps until the CQE).
    _release: Option<SlotRelease>,
}

impl Op<Close> {
    pub(crate) fn close(fd: RawFd) -> io::Result<Op<Close>> {
        Op::try_submit_with(Close {
            fd: FdRef::Raw(fd),
            _release: None,
        })
    }
}

impl Close {
    /// Close a fixed slot (`IORING_OP_CLOSE` with `file_index`) on the ring it belongs to,
    /// which need not be the current one. If the ring is gone, so is its table: nothing to do.
    /// If the close can't be pushed, the slot is cleared synchronously instead.
    pub(crate) fn fixed(fixed: FixedSlot) -> Option<Op<Close>> {
        let driver = fixed.table.driver()?;
        let data = Close {
            fd: FdRef::Fixed(fixed.slot),
            _release: Some(SlotRelease::after_close(fixed)),
        };
        match UringInner::try_submit_with_data(&driver.0, data) {
            Ok(op) => Some(op),
            Err((_, mut data)) => {
                if let Some(release) = data._release.as_mut() {
                    release.clear_on_drop();
                }
                None
            }
        }
    }
}

impl OpAble for Close {
    const SKIP_CANCEL: bool = true;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        match self.fd {
            FdRef::Raw(fd) => opcode::Close::new(types::Fd(fd)).build(),
            FdRef::Fixed(slot) => opcode::Close::new(types::Fixed(slot)).build(),
        }
    }
}
