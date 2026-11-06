use std::{ffi::CString, io, path::Path, rc::Rc};

use io_uring::{
    opcode,
    types::{self, DestinationSlot},
};

use super::{multishot::MultishotOp, Op, OpAble, OpKind};
use crate::{
    driver::{
        fixed_files::{FileTable, FixedSlot, SlotRelease},
        raw::Cqe,
        shared_fd::SharedFd,
        util::cstr,
    },
    fs::OpenOptions,
};

/// Open a file
pub(crate) struct Open {
    pub(crate) path: CString,
    flags: i32,
    mode: libc::mode_t,
}

/// The `open(2)` flags for `options` (with `O_CLOEXEC`).
fn open_flags(options: &OpenOptions) -> io::Result<i32> {
    Ok(libc::O_CLOEXEC
        | options.access_mode()?
        | options.creation_mode()?
        | (options.custom_flags & !libc::O_ACCMODE))
}

impl Op<Open> {
    /// Submit a request to open a file.
    pub(crate) fn open<P: AsRef<Path>>(path: P, options: &OpenOptions) -> io::Result<Op<Open>> {
        // Here the path will be copied, so its safe.
        let path = cstr(path.as_ref())?;
        let flags = open_flags(options)?;
        let mode = options.mode;

        Op::submit_with(Open { path, flags, mode })
    }

    /// Start a direct open (see [`open_direct`]).
    pub(crate) fn open_direct(path: &Path, options: &OpenOptions) -> io::Result<OpenDirectOp> {
        open_direct(path, options)
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

/// Open straight into a runtime-allocated slot of the fixed file table (`IORING_OP_OPENAT`
/// with `file_index`, Linux 5.15+; the sparse table needs 5.19+). No fd is created.
pub(crate) struct OpenDirect {
    path: CString,
    flags: i32,
    mode: libc::mode_t,
    /// The reserved slot. If nobody takes it (the open failed, or the op was dropped and
    /// completed anyway), dropping it clears the slot and frees it, so the file never leaks.
    slot: Option<SlotRelease>,
}

/// Open into a slot the kernel picks (`IORING_FILE_INDEX_ALLOC`) from the table's kernel
/// range. Driven as a one-CQE multishot op, so that a dropped open whose CQE carries a slot can
/// still close it ([`OpAble::discard_cqe`]).
pub(crate) struct OpenDirectAlloc {
    path: CString,
    flags: i32,
    mode: libc::mode_t,
    table: Rc<FileTable>,
}

/// An in-flight direct open, see [`open_direct`].
pub(crate) enum OpenDirectOp {
    Runtime(Op<OpenDirect>),
    Kernel(MultishotOp<OpenDirectAlloc>),
}

/// Start a direct open on the current ring. Prefers a runtime slot; falls back to a
/// kernel-allocated one when the runtime has none free and the table has a kernel range.
fn open_direct(path: &Path, options: &OpenOptions) -> io::Result<OpenDirectOp> {
    let path = cstr(path)?;
    // The kernel rejects O_CLOEXEC for direct descriptors (there is no fd to close on exec).
    let flags = open_flags(options)? & !libc::O_CLOEXEC;
    let mode = options.mode;
    let table = FileTable::current_or_err()?;
    if !table.has_free_slot() && table.has_kernel_range() {
        let data = OpenDirectAlloc {
            path,
            flags,
            mode,
            table,
        };
        return MultishotOp::submit(data)
            .map(OpenDirectOp::Kernel)
            .map_err(|(e, _)| e);
    }
    let slot = table.alloc()?;
    Op::submit_with(OpenDirect {
        path,
        flags,
        mode,
        slot: Some(SlotRelease::reserved(table, slot)),
    })
    .map(OpenDirectOp::Runtime)
}

impl OpenDirectOp {
    /// Wait for the open; the new direct-only file.
    pub(crate) async fn finish(self) -> io::Result<SharedFd> {
        match self {
            OpenDirectOp::Runtime(op) => {
                let completion = op.await;
                let mut data = completion.data;
                // On error the slot is released (and cleared) with `data`.
                completion.meta.result?;
                let slot = data
                    .slot
                    .take()
                    .ok_or_else(|| io::Error::other("direct open without a slot"))?;
                Ok(SharedFd::new_direct(slot.claim()))
            }
            OpenDirectOp::Kernel(mut op) => {
                let cqe = std::future::poll_fn(|cx| op.poll_next(cx))
                    .await
                    .ok_or_else(|| io::Error::other("direct open without a completion"))?;
                // With IORING_FILE_INDEX_ALLOC the result of a successful open is the slot.
                let slot = cqe.result()?;
                let table = op.data().table.clone();
                table.adopt_kernel_slot();
                Ok(SharedFd::new_direct(FixedSlot { table, slot }))
            }
        }
    }
}

fn openat_direct(
    path: &CString,
    flags: i32,
    mode: libc::mode_t,
    dest: Option<DestinationSlot>,
) -> io_uring::squeue::Entry {
    opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_c_str().as_ptr())
        .file_index(dest)
        .flags(flags)
        .mode(mode)
        .build()
}

impl OpAble for OpenDirect {
    // The result is 0 (the file is in our slot), not an fd: never close it.
    const RET_IS_FD: bool = false;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        // Slots are below the kernel's table limit (2^20), so this always converts.
        let dest = self
            .slot
            .as_ref()
            .and_then(|s| DestinationSlot::try_from_slot_target(s.slot()).ok());
        openat_direct(&self.path, self.flags, self.mode, dest)
    }
}

impl OpAble for OpenDirectAlloc {
    const KIND: OpKind = OpKind::Multishot;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        openat_direct(
            &self.path,
            self.flags,
            self.mode,
            Some(DestinationSlot::auto_target()),
        )
    }

    fn discard_cqe(&mut self, cqe: Cqe) {
        // Nobody took the file: clear the slot the kernel put it in (it was never adopted).
        if let Ok(slot) = cqe.result() {
            let _ = self.table.update_sync(slot, -1);
        }
    }
}
