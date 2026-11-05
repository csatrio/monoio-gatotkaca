//! Raw io_uring access.
//!
//! The typed APIs of this crate cover the common operations. This module is the escape hatch for
//! everything else: submit any [`squeue::Entry`] and get the raw completion back, or reach the
//! underlying [`IoUring`] to register files and buffers, probe opcodes, and so on.
//!
//! Multishot SQEs (several CQEs per SQE) go through [`submit_multishot`], which queues their
//! completions per operation and hands them out through a [`MultiOp`]. Linked SQEs go through
//! [`submit_chain`], which queues them as one unit; [`submit_with_timeout`] bounds one SQE with a
//! link timeout.
//!
//! All of these are available on the driver handle ([`IoUringDriver`], see [`Runtime::driver`])
//! and, from inside a running runtime, as the free functions [`submit`], [`submit_multishot`]
//! and [`with_ring`].
//!
//! [`Runtime::driver`]: crate::Runtime::driver

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

// The io_uring items needed to build SQEs and use the ring, so callers need no direct dependency
// (and cannot end up with a mismatching `io_uring` version).
pub use io_uring::{cqueue, opcode, squeue, types, IoUring, Probe};

pub use super::buf_ring::{BufRing, BufRingOptions, BufRingStats, Bundle, SelectedBuf};
pub use super::chain::{link, ChainOp};
pub use super::fixed_buf::{
    FixedBuf, FixedBufPool, FixedBufStats, FixedIoBuf, FrozenFixedBuf, ReadFixedFuture,
    WriteFixedFuture,
};
pub use super::fixed_files::{fixed_file_stats, FixedFileStats};
pub use super::msg_ring::{
    event_channel, msg_receiver, msg_ring, msg_ring_nowait, ring_handle, EventReceiver,
    EventSender, Mailbox, RingHandle,
};
// errno and poll constants for SQEs and completions (`libc::ENOBUFS`, `libc::POLLIN`, ...).
pub use libc;

use super::{
    op::{multishot::MultishotOp, Completion, Op, OpAble, OpKind},
    uring::{IoUringDriver, UringInner},
    util::timespec,
    Inner, CURRENT,
};
use crate::io::stream::Stream;

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
    #[inline]
    pub(crate) const fn new(res: i32, flags: u32) -> Self {
        Self { res, flags }
    }

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
    /// The timespec of the link timeout, for [`submit_with_timeout`]. Boxed: the kernel reads
    /// it when it prepares the timeout SQE.
    _timeout: Option<Box<types::Timespec>>,
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
/// - the operation is single-shot. Use [`submit_multishot`] for multishot operations (which post
///   several CQEs); here, only the first CQE's result would be returned and the others dropped;
/// - if the operation returns a file descriptor, the caller takes ownership of it and closes it.
///   If the [`RawOp`] is dropped first, the descriptor leaks;
/// - the SQE does not set `IO_LINK` or `IO_HARDLINK`: it would link to whatever SQE is queued
///   next, and the driver may flush a full queue in between, splitting the chain. Use
///   [`submit_chain`] for linked SQEs, and [`submit_with_timeout`] for a link timeout.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub unsafe fn submit<T: Unpin + 'static>(entry: squeue::Entry, data: T) -> io::Result<RawOp<T>> {
    CURRENT.with(|inner| inner.submit_raw(entry, data))
}

/// Submit a raw SQE bounded by a link timeout: the SQE is pushed with `IO_LINK`, directly
/// followed by an `IORING_OP_LINK_TIMEOUT` SQE (both in one reservation, so they are never
/// split).
///
/// If the operation did not complete within `timeout`, the kernel cancels it and its CQE carries
/// `-ECANCELED` (some operations that cannot be interrupted complete normally instead). The
/// timeout's own CQE is handled by the driver (counted in
/// [`DriverStats::link_timeouts`](crate::DriverStats::link_timeouts) when it fired). This needs
/// no timer: it works without `enable_timer`.
///
/// # Safety
/// Same as [`submit`](fn@submit). Also, the SQE must not set `IO_LINK`, `IO_HARDLINK` or
/// `CQE_SKIP_SUCCESS` itself.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub unsafe fn submit_with_timeout<T: Unpin + 'static>(
    entry: squeue::Entry,
    data: T,
    timeout: Duration,
) -> io::Result<RawOp<T>> {
    CURRENT.with(|inner| inner.submit_raw_with_timeout(entry, data, timeout))
}

/// Submit a chain of linked SQEs as one unit, and get every completion through the returned
/// [`ChainOp`].
///
/// The driver first makes room for all `N` SQEs (flushing the queue if needed), then pushes them
/// back to back with nothing submitted in between, so the kernel sees the whole chain in one
/// submission and a link is never split. Their `user_data` is overwritten.
///
/// **Link flags are the caller's**: set `IO_LINK` (or `IO_HARDLINK`) on every SQE but the last,
/// e.g. with [`link`]. SQEs without a link flag are still queued together but run independently.
/// With `IO_LINK`, an SQE that fails (or a short read/write) cancels the rest of the chain, which
/// then completes with `-ECANCELED`. To bound an SQE with a timeout, put an `opcode::LinkTimeout`
/// right after it (its timespec in heap memory owned by `data`); its CQE is part of the result.
///
/// The [`ChainOp`] resolves to `([Cqe; N], data)` once all `N` SQEs completed (each posts exactly
/// one CQE). Dropping it first keeps `data` alive until then.
///
/// # Errors
/// `InvalidInput` if `N` is 0 or larger than the submission queue, if the last SQE sets
/// `IO_LINK`/`IO_HARDLINK` (it would link into the next, unrelated SQE) or if an SQE sets
/// `CQE_SKIP_SUCCESS`. `EBUSY` if the queue stayed full. Nothing is queued on error, and `data`
/// is dropped.
///
/// # Safety
/// The caller must ensure that every SQE is valid for its opcode and single-shot, that every
/// pointer they hold points into heap memory owned by `data` (never into its inline fields, which
/// move), and that file descriptors returned in completions are closed by the caller (those of a
/// dropped chain leak). Different SQEs may point to the same memory (e.g. a read into a buffer
/// linked to a send from it).
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub unsafe fn submit_chain<T: Unpin + 'static, const N: usize>(
    sqes: [squeue::Entry; N],
    data: T,
) -> io::Result<ChainOp<T, N>> {
    CURRENT
        .with(|inner| ChainOp::submit_in(inner, sqes, data))
        .map_err(|(e, _)| e)
}

struct RawMultiSqe<T> {
    entry: squeue::Entry,
    data: T,
}

impl<T: 'static> OpAble for RawMultiSqe<T> {
    const KIND: OpKind = OpKind::Multishot;

    fn uring_op(&mut self) -> squeue::Entry {
        self.entry.clone()
    }
}

/// An in-flight multishot operation, from [`submit_multishot`]: a stream of its completions.
///
/// The driver queues each CQE of the operation; [`next`](Self::next) (or
/// [`poll_next`](Self::poll_next), or the [`Stream`] impl) returns them in order. The operation
/// ends with the first CQE without [`Cqe::more`]: it is returned like the others, then `next`
/// returns `None`.
///
/// The queue is unbounded: while nobody polls, every CQE the kernel posts is kept (see
/// [`DriverStats::multishot_queue_high_water`](crate::DriverStats::multishot_queue_high_water)).
/// The kernel itself stops a multishot op when it runs out of resources (`ENOBUFS` for an empty
/// buffer ring, CQ overflow).
///
/// Dropping it before the final CQE drops the queued CQEs, keeps `data` alive until the kernel
/// posted the final CQE (dropping every CQE until then) and asks the kernel to cancel the
/// operation (always, even without the `async-cancel` feature, since a multishot op may never
/// end on its own). Dropped CQEs are not inspected: a file descriptor or provided buffer they
/// carry is not closed or returned. To keep those, [`cancel`](Self::cancel) and drain with
/// `next` instead.
#[must_use = "multishot operations are cancelled when dropped"]
pub struct MultiOp<T: Unpin + 'static>(MultishotOp<RawMultiSqe<T>>);

impl<T: Unpin + 'static> MultiOp<T> {
    /// Poll for the next completion. `Ready(None)` once the final one was returned.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Cqe>> {
        self.0.poll_next(cx)
    }

    /// The next completion, or `None` once the final one was returned.
    pub async fn next(&mut self) -> Option<Cqe> {
        std::future::poll_fn(|cx| self.0.poll_next(cx)).await
    }

    /// Ask the kernel to cancel the operation (AsyncCancel). Keep polling: the completions
    /// already posted are still returned, then the final one, typically `-ECANCELED`. Does
    /// nothing once the kernel posted the final completion.
    pub fn cancel(&self) {
        self.0.cancel()
    }

    /// Whether the final completion was returned (`next` returns `None` from now on).
    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }

    /// The data passed to [`submit_multishot`].
    pub fn data(&self) -> &T {
        &self.0.data().data
    }

    /// Get `data` back. Succeeds once the kernel posted the final completion, even if it was not
    /// returned by `next` yet (completions not returned are dropped); otherwise gives `self`
    /// back.
    pub fn into_data(self) -> Result<T, Self> {
        self.0.into_data().map(|raw| raw.data).map_err(Self)
    }
}

impl<T: Unpin + 'static> Stream for MultiOp<T> {
    type Item = Cqe;

    async fn next(&mut self) -> Option<Cqe> {
        MultiOp::next(self).await
    }
}

impl<T: Unpin + 'static> std::fmt::Debug for MultiOp<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiOp")
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

/// Submit a multishot SQE (`RecvMulti`, `AcceptMulti`, `PollAdd` multi, ...) to the current
/// runtime's ring, and get its completions through the returned [`MultiOp`].
///
/// The SQE is queued immediately and submitted on the next driver tick. Its `user_data` is
/// overwritten. A single-shot SQE works too: its only CQE is the final one.
///
/// # Safety
/// The caller must ensure that:
/// - the SQE is valid for its opcode, and every pointer it holds points into heap memory owned by
///   `data`, never into `data`'s inline fields, which move. `data` is kept alive until the final
///   CQE;
/// - file descriptors returned in completions are closed by the caller (see [`MultiOp`] for the
///   ones dropped with it);
/// - the SQE does not set `IO_LINK`/`IO_HARDLINK`.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub unsafe fn submit_multishot<T: Unpin + 'static>(
    entry: squeue::Entry,
    data: T,
) -> io::Result<MultiOp<T>> {
    CURRENT.with(|inner| inner.submit_multishot_raw(entry, data))
}

/// Run `f` with the current runtime's [`IoUring`].
///
/// Use it for ring-level setup and queries (`submitter().register_files(..)`, `params()`, probing
/// opcodes, ...), and to push your own SQEs: tag them with [`user_data`] and receive their
/// completions in a hook ([`set_cqe_hook`]). The SQEs are submitted on the next driver tick. For
/// multishot SQEs, [`submit_multishot`] is usually simpler: it queues the completions per
/// operation and cancels the operation when dropped.
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

    /// Submit a linked chain of SQEs to this driver's ring. See [`submit_chain`] for the
    /// contract.
    ///
    /// Like [`submit_raw`](Self::submit_raw), this works outside of the runtime.
    ///
    /// # Safety
    /// Same as [`submit_chain`].
    pub unsafe fn submit_chain_raw<T: Unpin + 'static, const N: usize>(
        &self,
        sqes: [squeue::Entry; N],
        data: T,
    ) -> io::Result<ChainOp<T, N>> {
        ChainOp::submit_in(&self.handle(), sqes, data).map_err(|(e, _)| e)
    }

    /// Submit a raw SQE bounded by a link timeout to this driver's ring. See
    /// [`submit_with_timeout`] for the contract.
    ///
    /// Like [`submit_raw`](Self::submit_raw), this works outside of the runtime.
    ///
    /// # Safety
    /// Same as [`submit_with_timeout`].
    pub unsafe fn submit_with_timeout_raw<T: Unpin + 'static>(
        &self,
        entry: squeue::Entry,
        data: T,
        timeout: Duration,
    ) -> io::Result<RawOp<T>> {
        self.handle().submit_raw_with_timeout(entry, data, timeout)
    }

    /// Submit a multishot SQE to this driver's ring. See [`submit_multishot`] for the contract.
    ///
    /// Like [`submit_raw`](Self::submit_raw), this works outside of the runtime.
    ///
    /// # Safety
    /// Same as [`submit_multishot`].
    pub unsafe fn submit_multishot_raw<T: Unpin + 'static>(
        &self,
        entry: squeue::Entry,
        data: T,
    ) -> io::Result<MultiOp<T>> {
        self.handle().submit_multishot_raw(entry, data)
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
        self.submit_with(RawSqe {
            entry,
            data,
            _timeout: None,
        })
        .map(RawOp)
    }

    fn submit_raw_with_timeout<T: Unpin + 'static>(
        &self,
        entry: squeue::Entry,
        data: T,
        timeout: Duration,
    ) -> io::Result<RawOp<T>> {
        let ts = Box::new(timespec(timeout));
        let ptr: *const types::Timespec = &*ts;
        let sqe = RawSqe {
            entry,
            data,
            _timeout: Some(ts),
        };
        UringInner::try_submit_with_timeout(&self.0, sqe, ptr)
            .map(RawOp)
            .map_err(|(e, _)| e)
    }

    fn submit_multishot_raw<T: Unpin + 'static>(
        &self,
        entry: squeue::Entry,
        data: T,
    ) -> io::Result<MultiOp<T>> {
        MultishotOp::submit_in(self, RawMultiSqe { entry, data })
            .map(MultiOp)
            .map_err(|(e, _)| e)
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
