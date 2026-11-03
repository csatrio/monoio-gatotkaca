//! Raw io_uring access.
//!
//! The typed APIs of this crate cover the common operations. This module is the escape hatch for
//! everything else: submit any [`squeue::Entry`] and get the raw completion back, or reach the
//! underlying [`IoUring`] to register files and buffers, probe opcodes, and so on.
//!
//! Both are available on the driver handle ([`IoUringDriver`], see [`Runtime::driver`]) and, from
//! inside a running runtime, as the free functions [`submit`] and [`with_ring`].
//!
//! [`Runtime::driver`]: crate::Runtime::driver

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{squeue, IoUring};

use super::{
    op::{Completion, Op, OpAble},
    uring::IoUringDriver,
    Inner, CURRENT,
};

/// A raw completion queue result.
///
/// `user_data` is not exposed: the driver owns it to route the completion to its [`RawOp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cqe {
    res: i32,
    flags: u32,
}

impl Cqe {
    /// The raw `res` field: a non-negative result, or a negated errno.
    #[inline]
    pub fn res(&self) -> i32 {
        self.res
    }

    /// The raw CQE flags.
    #[inline]
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// `res` as an [`io::Result`], mapping a negative value to the corresponding OS error.
    #[inline]
    pub fn result(&self) -> io::Result<u32> {
        if self.res >= 0 {
            Ok(self.res as u32)
        } else {
            Err(io::Error::from_raw_os_error(-self.res))
        }
    }
}

struct RawSqe<T> {
    entry: squeue::Entry,
    data: T,
}

impl<T: 'static> OpAble for RawSqe<T> {
    fn uring_op(&mut self) -> squeue::Entry {
        self.entry.clone()
    }
}

/// An in-flight raw operation. Resolves to the completion and gives `data` back.
///
/// Dropping it before completion keeps `data` alive until the kernel is done with the operation,
/// and (with `async-cancel`) asks the kernel to cancel it.
pub struct RawOp<T: Unpin + 'static>(Op<RawSqe<T>>);

impl<T: Unpin + 'static> Future for RawOp<T> {
    type Output = (Cqe, T);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Completion { data, meta } = ready!(Pin::new(&mut self.0).poll(cx));
        let res = match meta.result {
            Ok(v) => v.into_inner() as i32,
            Err(e) => -e.raw_os_error().unwrap_or(libc::EIO),
        };
        Poll::Ready((
            Cqe {
                res,
                flags: meta.flags,
            },
            data.data,
        ))
    }
}

/// Submit a raw SQE to the current runtime's ring.
///
/// The SQE is queued immediately and submitted to the kernel on the next driver tick. Its
/// `user_data` is overwritten. `data` is returned with the completion and is kept alive for as
/// long as the kernel may use it.
///
/// # Safety
/// The caller must ensure that:
/// - the SQE is valid for its opcode, and every pointer it holds points into heap memory owned by
///   `data` (e.g. a `Box` or `Vec`), never into `data`'s inline fields, which move;
/// - the operation is single-shot. Multishot operations (which post several CQEs) are not
///   supported;
/// - if the operation returns a file descriptor, the caller takes ownership of it and closes it.
///   If the [`RawOp`] is dropped first, the descriptor leaks;
/// - if the SQE sets `IO_LINK` or `IO_HARDLINK`, the whole chain is queued without anything
///   submitted in between (the driver flushes a full queue and may split the chain).
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub unsafe fn submit<T: Unpin + 'static>(entry: squeue::Entry, data: T) -> io::Result<RawOp<T>> {
    CURRENT.with(|inner| inner.submit_raw(entry, data))
}

/// Run `f` with the current runtime's [`IoUring`].
///
/// Use it for ring-level setup and queries, e.g. `submitter().register_files(..)`,
/// `submitter().register_buffers(..)`, `params()` or probing opcodes.
///
/// # Safety
/// `f` must not re-enter the runtime (spawn, submit or poll operations, drop an in-flight
/// operation, ...), and must not push SQEs or reap CQEs on the ring itself: the driver owns
/// `user_data`, and every CQE is consumed by the driver and routed by it. Use [`submit`] for
/// operations. Registrations must stay valid for as long as operations use them.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub unsafe fn with_ring<R>(f: impl FnOnce(&mut IoUring) -> R) -> R {
    CURRENT.with(|inner| inner.with_ring(f))
}

impl IoUringDriver {
    /// Submit a raw SQE to this driver's ring. See [`submit`](fn@submit) for the contract.
    ///
    /// Unlike the free function, this works outside of the runtime, e.g. before `block_on`. The
    /// operation makes progress while the runtime runs.
    ///
    /// # Safety
    /// Same as [`submit`](fn@submit).
    pub unsafe fn submit_raw<T: Unpin + 'static>(
        &self,
        entry: squeue::Entry,
        data: T,
    ) -> io::Result<RawOp<T>> {
        self.handle().submit_raw(entry, data)
    }

    /// Run `f` with this driver's [`IoUring`]. See [`with_ring`](fn@with_ring) for the contract.
    ///
    /// # Safety
    /// Same as [`with_ring`](fn@with_ring).
    pub unsafe fn with_ring<R>(&self, f: impl FnOnce(&mut IoUring) -> R) -> R {
        self.handle().with_ring(f)
    }
}

impl Inner {
    fn submit_raw<T: Unpin + 'static>(
        &self,
        entry: squeue::Entry,
        data: T,
    ) -> io::Result<RawOp<T>> {
        self.submit_with(RawSqe { entry, data }).map(RawOp)
    }

    fn with_ring<R>(&self, f: impl FnOnce(&mut IoUring) -> R) -> R {
        // Caller guarantees exclusive, non re-entrant access, see `with_ring`.
        f(unsafe { &mut (*self.0.get()).uring })
    }
}
