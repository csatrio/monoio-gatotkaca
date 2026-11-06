//! Socket options: a direct `setsockopt(2)` / `getsockopt(2)` when the socket has an fd, and
//! `IORING_OP_URING_CMD` with `SOCKET_URING_OP_SETSOCKOPT` / `SOCKET_URING_OP_GETSOCKOPT`
//! (Linux 6.7+) when it is direct-only (it lives only in the fixed file table).
//!
//! The kernel reads (or writes) the option value when it issues the SQE, at `io_uring_enter`,
//! not when the SQE is pushed. The futures built here move after the push, and may be dropped
//! before the enter, so the SQE points at a heap copy owned by the op data, which the driver
//! keeps until the CQE.

use std::{
    future::Future,
    io,
    mem::{size_of, MaybeUninit},
};

use io_uring::{opcode, squeue::Entry, types};

use super::{super::shared_fd::SharedFd, Op, OpAble};
use crate::driver::shared_fd::sqe_fd;

/// `enum io_uring_socket_op`.
const SOCKET_URING_OP_GETSOCKOPT: u32 = 2;
const SOCKET_URING_OP_SETSOCKOPT: u32 = 3;

/// Offset of `cmd_op` in `struct io_uring_sqe` (the first word of the union that also holds
/// `off` and `addr2`).
const SQE_CMD_OP: usize = 8;

pub(crate) struct SockOpt {
    /// Keeps the socket open while the op is in flight.
    fd: SharedFd,
    get: bool,
    level: u32,
    name: u32,
    /// The option value the SQE points to. Bytes, so that the op is not generic over the
    /// option's type (the kernel copies with `copy_{from,to}_user`: no alignment needed).
    val: Box<[MaybeUninit<u8>]>,
}

/// A copy of `value`'s bytes on the heap (padding included, which is why they are
/// `MaybeUninit`).
fn boxed_bytes<T: Copy>(value: &T) -> Box<[MaybeUninit<u8>]> {
    let mut val = Box::<[u8]>::new_uninit_slice(size_of::<T>());
    // SAFETY: `value` is valid for `size_of::<T>()` bytes of reads, `val` for as many writes.
    unsafe {
        std::ptr::copy_nonoverlapping(
            (value as *const T).cast::<MaybeUninit<u8>>(),
            val.as_mut_ptr(),
            size_of::<T>(),
        );
    }
    val
}

/// The `cmd_op` word of an SQE.
fn cmd_op(entry: &mut Entry) -> *mut u32 {
    const _: () = assert!(size_of::<Entry>() == 64);
    // SAFETY: `Entry` is `repr(C)` around the 64-byte `io_uring_sqe`: offset 8 is in bounds.
    unsafe { (entry as *mut Entry).cast::<u8>().add(SQE_CMD_OP).cast() }
}

/// The `io_uring` crate has no typed `GETSOCKOPT` (and `UringCmd16` can't set `optlen`). The
/// SQE is the one of `SetSockOpt` with another `cmd_op`, so build that and change the word.
/// This checks once per call that the word is where the kernel ABI puts it, so that a layout
/// change in the crate can never turn a "get" into a "set".
fn getsockopt_layout_ok() -> bool {
    let mut probe = opcode::SetSockOpt::new(types::Fd(-1), 0, 0, std::ptr::null(), 0).build();
    // SAFETY: in bounds (see `cmd_op`), any bit pattern is a `u32`.
    unsafe { cmd_op(&mut probe).read_unaligned() == SOCKET_URING_OP_SETSOCKOPT }
}

impl OpAble for SockOpt {
    // Runs inline at submit; a caller that does not wait for the result still wants the option
    // set.
    const SKIP_CANCEL: bool = true;

    fn uring_op(&mut self) -> Entry {
        let (val, len) = (self.val.as_ptr().cast(), self.val.len() as u32);
        let mut entry = sqe_fd!(self.fd, |fd| opcode::SetSockOpt::new(
            fd, self.level, self.name, val, len
        )
        .build());
        if self.get {
            // SAFETY: in bounds (see `cmd_op`); checked by `getsockopt_layout_ok` before.
            unsafe { cmd_op(&mut entry).write_unaligned(SOCKET_URING_OP_GETSOCKOPT) };
        }
        entry
    }
}

/// `EOPNOTSUPP` from the socket command: the kernel has no `SOCKET_URING_OP_*SOCKOPT` (before
/// 6.7), or (get) the level is not `SOL_SOCKET` (still so on Linux 7.0). Other errors are the
/// option's own (`ENOPROTOOPT`, `EINVAL`, ...). `EINVAL` never means "no such opcode" here:
/// `IORING_OP_URING_CMD` is as old as the sparse file table a direct-only socket needs (5.19).
fn map_err(e: io::Error, get: bool) -> io::Error {
    if e.raw_os_error() != Some(libc::EOPNOTSUPP) {
        return e;
    }
    io::Error::new(
        io::ErrorKind::Unsupported,
        if get {
            "getsockopt through io_uring (direct-only socket) needs Linux 6.7+ and a \
             SOL_SOCKET option"
        } else {
            "setsockopt through io_uring (direct-only socket) needs Linux 6.7+"
        },
    )
}

/// Set the socket option `name` of `level` to `value`.
///
/// The work is done when this is called, not when the future is polled:
/// - with an fd: one `setsockopt(2)` syscall; the future is ready at its first poll;
/// - direct-only: `value` is copied to the heap and the SQE is pushed; the future resolves
///   with its CQE (`Unsupported` before Linux 6.7). Dropping it does not cancel the op.
pub(crate) fn set<T: Copy>(
    fd: &SharedFd,
    level: libc::c_int,
    name: libc::c_int,
    value: &T,
) -> impl Future<Output = io::Result<()>> {
    let state = if fd.is_direct() {
        Err(Op::submit_with(SockOpt {
            fd: fd.clone(),
            get: false,
            level: level as u32,
            name: name as u32,
            val: boxed_bytes(value),
        }))
    } else {
        // SAFETY: `value` is valid for `size_of::<T>()` bytes for the duration of the call.
        let ret = unsafe {
            libc::setsockopt(
                fd.raw_fd(),
                level,
                name,
                (value as *const T).cast(),
                size_of::<T>() as libc::socklen_t,
            )
        };
        Ok(match ret {
            -1 => Err(io::Error::last_os_error()),
            _ => Ok(()),
        })
    };
    async move {
        match state {
            Ok(res) => res,
            Err(op) => match op?.await.meta.result {
                Ok(_) => Ok(()),
                Err(e) => Err(map_err(e, false)),
            },
        }
    }
}

/// Read the socket option `name` of `level` as a `T`. Bytes of `T` the kernel does not write
/// (an option shorter than `T`) are zero.
///
/// With an fd this is one `getsockopt(2)` syscall, done when this is called. Direct-only: an
/// SQE pushed when this is called; the kernel only supports `SOL_SOCKET` options this way
/// (`Unsupported` for other levels and before Linux 6.7).
///
/// # Safety
/// Every bit pattern the kernel may write for this option, and all-zero, must be a valid `T`
/// (integers and the `libc` option structs are; `bool`, enums and references are not).
pub(crate) unsafe fn get<T: Copy>(
    fd: &SharedFd,
    level: libc::c_int,
    name: libc::c_int,
) -> impl Future<Output = io::Result<T>> {
    let state = if fd.is_direct() {
        Err(if getsockopt_layout_ok() {
            Op::submit_with(SockOpt {
                fd: fd.clone(),
                get: true,
                level: level as u32,
                name: name as u32,
                val: vec![MaybeUninit::new(0u8); size_of::<T>()].into_boxed_slice(),
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "getsockopt through io_uring: unexpected SQE layout",
            ))
        })
    } else {
        let mut value = MaybeUninit::<T>::zeroed();
        let mut len = size_of::<T>() as libc::socklen_t;
        // SAFETY: `value` is valid for `len` bytes of writes for the duration of the call.
        let ret = unsafe {
            libc::getsockopt(
                fd.raw_fd(),
                level,
                name,
                value.as_mut_ptr().cast(),
                &mut len,
            )
        };
        Ok(match ret {
            -1 => Err(io::Error::last_os_error()),
            // SAFETY: zeroed, then partly overwritten by the kernel: valid by the contract.
            _ => Ok(unsafe { value.assume_init() }),
        })
    };
    async move {
        match state {
            Ok(res) => res,
            Err(op) => {
                let completion = op?.await;
                match completion.meta.result {
                    // SAFETY: `val` holds `size_of::<T>()` bytes, zeroed and then partly
                    // overwritten by the kernel: valid by the contract. Unaligned read: the
                    // box is a byte slice.
                    Ok(_) => {
                        Ok(unsafe { completion.data.val.as_ptr().cast::<T>().read_unaligned() })
                    }
                    Err(e) => Err(map_err(e, true)),
                }
            }
        }
    }
}
