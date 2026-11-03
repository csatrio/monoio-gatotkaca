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

// The io_uring items needed to build SQEs and use the ring, so callers need no direct dependency
// (and cannot end up with a mismatching `io_uring` version).
pub use io_uring::{cqueue, opcode, squeue, types, IoUring, Probe};

pub use super::buf_ring::{BufRing, SelectedBuf};
// errno and poll constants for SQEs and completions (`libc::ENOBUFS`, `libc::POLLIN`, ...).
pub use libc;

use super::{
    op::{Completion, Op, OpAble},
    uring::{IoUringDriver, UringInner},
    Inner, CURRENT,
};

/// A raw completion queue result.
///
/// `user_data` is not exposed: it routes the completion, to a [`RawOp`] or to the hook (which gets
/// the id you chose instead).
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

    /// More completions will follow for the same SQE (multishot).
    #[inline]
    pub fn more(&self) -> bool {
        cqueue::more(self.flags)
    }

    /// The id of the selected provided buffer, if the completion carries one.
    #[inline]
    pub fn buffer_id(&self) -> Option<u16> {
        cqueue::buffer_select(self.flags)
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
/// Use it for ring-level setup and queries (`submitter().register_files(..)`, `params()`, probing
/// opcodes, ...), and to push your own SQEs, including multishot ones: tag them with
/// [`user_data`] and receive their completions in a hook ([`set_cqe_hook`]). The SQEs are submitted
/// on the next driver tick.
///
/// # Safety
/// `f` must not re-enter the runtime (spawn, submit or poll operations, drop an in-flight
/// operation, ...) and must not reap CQEs (`completion()`): the driver consumes them. Every SQE you
/// push must carry a [`user_data`] value. Any other value below 2^62 is taken for one of the
/// driver's own operations when it completes, which is undefined behaviour; values above that are
/// ignored and never reported. Everything an SQE points to must stay valid until its final CQE. Registrations must stay valid
/// for as long as operations use them.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub unsafe fn with_ring<R>(f: impl FnOnce(&mut IoUring) -> R) -> R {
    CURRENT.with(|inner| inner.with_ring(f))
}

const EXTERNAL_PREFIX: u64 = 0b10 << 62;

/// The largest id accepted by [`user_data`].
pub const MAX_ID: u64 = (1 << 62) - 1;

/// The `user_data` to put on an SQE pushed with [`with_ring`]. The driver reports the completions
/// of such SQEs to the hook ([`set_cqe_hook`]) together with `id`, and never interprets them.
///
/// # Panics
/// Panics if `id` is greater than [`MAX_ID`].
#[inline]
pub fn user_data(id: u64) -> u64 {
    assert!(id <= MAX_ID, "id must not exceed MAX_ID");
    EXTERNAL_PREFIX | id
}

/// Install the hook receiving every completion of SQEs tagged with [`user_data`], replacing any
/// previous hook, for the current runtime.
///
/// The hook is called as `hook(id, cqe)` from the driver, in completion order, after the driver
/// has finished processing a batch of completions and holds no state: it may use [`with_ring`],
/// [`submit`], wake tasks or spawn. Check [`Cqe::more`] to see if a multishot SQE is still active.
/// A new or cleared hook applies from the next batch. Completions arriving while no hook is
/// installed are dropped, so install it before pushing SQEs.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub fn set_cqe_hook(hook: impl FnMut(u64, Cqe) + 'static) {
    CURRENT.with(|inner| inner.set_cqe_hook(hook))
}

/// Remove the hook, and drop the completions not delivered yet.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub fn clear_cqe_hook() {
    CURRENT.with(|inner| inner.clear_cqe_hook())
}

/// Completions of user SQEs, waiting for delivery to the hook.
#[derive(Default)]
pub(super) struct ExternalCqes {
    hook: Option<Box<dyn FnMut(u64, Cqe)>>,
    // `hook` is taken out while it runs; this tells that one is installed anyway.
    installed: bool,
    // Bumped by `set`/`clear`, to tell if the hook changed while it was running.
    epoch: u64,
    pending: Vec<(u64, Cqe)>,
    // Buffer swapped with `pending` during delivery, to keep the allocation.
    spare: Vec<(u64, Cqe)>,
}

impl ExternalCqes {
    pub(super) fn push(&mut self, user_data: u64, cqe: &cqueue::Entry) {
        if self.installed {
            let cqe = Cqe {
                res: cqe.result(),
                flags: cqe.flags(),
            };
            self.pending.push((user_data & MAX_ID, cqe));
        }
    }

    fn set(&mut self, hook: Option<Box<dyn FnMut(u64, Cqe)>>) {
        self.installed = hook.is_some();
        self.hook = hook;
        self.epoch += 1;
        self.pending.clear();
    }
}

/// Hand the pending completions to the hook. Called by the driver once it is done with its own
/// state, so the hook is free to call back into the driver.
pub(super) fn deliver(cell: &std::cell::UnsafeCell<UringInner>) {
    let (mut hook, epoch, mut batch) = {
        // No other reference to the driver state is alive here.
        let ext = unsafe { &mut (*cell.get()).external };
        if ext.pending.is_empty() {
            return;
        }
        let Some(hook) = ext.hook.take() else {
            return;
        };
        let batch = std::mem::replace(&mut ext.pending, std::mem::take(&mut ext.spare));
        (hook, ext.epoch, batch)
    };

    for (id, cqe) in batch.drain(..) {
        hook(id, cqe);
    }

    let ext = unsafe { &mut (*cell.get()).external };
    ext.spare = batch;
    if ext.epoch == epoch {
        ext.hook = Some(hook);
    }
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

    /// Install the hook for user SQE completions. See [`set_cqe_hook`] for the contract.
    pub fn set_cqe_hook(&self, hook: impl FnMut(u64, Cqe) + 'static) {
        self.handle().set_cqe_hook(hook)
    }

    /// Remove the hook. See [`clear_cqe_hook`].
    pub fn clear_cqe_hook(&self) {
        self.handle().clear_cqe_hook()
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

    fn set_cqe_hook(&self, hook: impl FnMut(u64, Cqe) + 'static) {
        // Not called from within the hook's own delivery: that only holds `&mut` to the hook.
        unsafe { (*self.0.get()).external.set(Some(Box::new(hook))) }
    }

    fn clear_cqe_hook(&self) {
        unsafe { (*self.0.get()).external.set(None) }
    }

    pub(super) fn with_ring<R>(&self, f: impl FnOnce(&mut IoUring) -> R) -> R {
        // Caller guarantees exclusive, non re-entrant access, see `with_ring`.
        f(unsafe { &mut (*self.0.get()).uring })
    }
}
