//! Uring state lifecycle.
//! Partly borrow from tokio-uring.
//!
//! Every slab entry tracks one operation. Single and zero-copy ops resolve to one result
//! ([`Lifecycle::Completed`]); multishot ops queue their CQEs ([`MultiState`]). No CQE the kernel
//! posts can make these state machines panic: unexpected ones are dropped (closing an fd they
//! carry) and logged under the `debug` feature.
//!
//! Zero-copy sends can also be driven in two steps (`SendZcFuture`, then `ZcPending`): the send
//! result is handed out with the first CQE ([`Ref::poll_zc_first`]) while the slot (and the op
//! data with the buffer) stays until the notification CQE ([`Ref::poll_zc_notif`]).
//!
//! The data of a dropped op is kept in the slot ([`IgnoredData`]): inline when it fits, so
//! dropping an in-flight op usually does not allocate.
//!
//! A slot is one cache line: 64 bytes, and the slab aligns its entries to 64. The largest
//! state is the dropped one (48 bytes of inline data and its drop function), which leaves
//! one word for everything else. So what a slot keeps for its whole life ([`Meta`]) is not a
//! field next to the state but the first field of every state: there it shares the word of the
//! state's tag.

use std::{
    collections::VecDeque,
    io,
    task::{Context, Poll, Waker},
};

use super::ignored::IgnoredData;
use crate::{
    driver::{
        buf_ring::{BufRing, Bundle, SelectedBuf},
        op::{CompletionMeta, MaybeFd, OpAble, OpKind},
        raw::Cqe,
    },
    utils::slab::{Ref, Slab},
};

/// Flag indicating more CQEs will follow for this SQE (SEND_ZC, multishot).
const IORING_CQE_F_MORE: u32 = 2;

/// What a slot keeps from submit to its last CQE, whatever its state.
///
/// Alignment 1 and at most 7 bytes: as the first field of every [`Lifecycle`] state it lies
/// in the same word as the state's tag, so it costs the slot nothing.
#[derive(Clone, Copy)]
struct Meta {
    /// The result of the op is a fd, to be closed if nobody takes it.
    is_fd: bool,
    kind: OpKind,
    /// The SQE opcode, recorded at submit for `op_ticks_by_opcode`; [`NO_TICKS`] once the first
    /// CQE was measured (or never recorded).
    #[cfg(feature = "metrics")]
    opcode: u8,
    /// The driver tick counter (a `u32`) when the SQE was pushed, as bytes: `Meta` must keep
    /// alignment 1.
    #[cfg(feature = "metrics")]
    submit_tick: [u8; 4],
}

/// The state of a slot. Every state starts with the slot's [`Meta`], which never changes: a
/// transition copies it over.
enum Lifecycle {
    /// The operation has been submitted to uring and is currently in-flight
    Submitted(Meta),

    /// The submitter is waiting for the completion of the operation
    Waiting(Meta, Waker),

    /// The submitter no longer has interest in the operation result. The state
    /// must be passed to the driver and held until the operation completes.
    // Holds the op data alive; never read, only dropped.
    #[allow(dead_code)]
    Ignored(Meta, IgnoredData),

    /// The operation has completed.
    Completed(Meta, io::Result<MaybeFd>, u32),

    /// First CQE with IORING_CQE_F_MORE received, waiting for the final CQE (the SEND_ZC
    /// notification). The future has not been polled yet (was in Submitted state).
    CompletedMore(Meta, io::Result<MaybeFd>, u32),

    /// First CQE with IORING_CQE_F_MORE received, waiting for the final CQE.
    /// The future is actively polling (was in Waiting state).
    WaitingMore(Meta, Waker, io::Result<MaybeFd>, u32),

    /// The future was dropped but the final CQE is still pending.
    // Holds the buffer alive; never read, only dropped.
    #[allow(dead_code)]
    IgnoredMore(Meta, IgnoredData),

    /// Zero-copy, two-step: the submitter waits for the first CQE (the send result), not for
    /// the notification.
    WaitingFirst(Meta, Waker),

    /// Zero-copy, two-step: the send result was handed out; waiting for the notification CQE
    /// (the kernel releasing the buffer), with the waker of the `ZcPending` that polled.
    AwaitNotif(Meta, Option<Waker>),

    /// Zero-copy, two-step: the notification arrived after the result was handed out; the
    /// `ZcPending` has not seen it yet.
    Notified(Meta),

    /// A multishot operation.
    Multi(Meta, Box<MultiState>),
}

impl Lifecycle {
    /// The slot's [`Meta`]. It is at the same place in every state, so this is one load.
    #[inline]
    fn meta(&self) -> Meta {
        match self {
            Lifecycle::Submitted(meta)
            | Lifecycle::Waiting(meta, _)
            | Lifecycle::Ignored(meta, _)
            | Lifecycle::Completed(meta, _, _)
            | Lifecycle::CompletedMore(meta, _, _)
            | Lifecycle::WaitingMore(meta, _, _, _)
            | Lifecycle::IgnoredMore(meta, _)
            | Lifecycle::WaitingFirst(meta, _)
            | Lifecycle::AwaitNotif(meta, _)
            | Lifecycle::Notified(meta)
            | Lifecycle::Multi(meta, _) => *meta,
        }
    }

    #[cfg(feature = "metrics")]
    #[inline]
    fn meta_mut(&mut self) -> &mut Meta {
        match self {
            Lifecycle::Submitted(meta)
            | Lifecycle::Waiting(meta, _)
            | Lifecycle::Ignored(meta, _)
            | Lifecycle::Completed(meta, _, _)
            | Lifecycle::CompletedMore(meta, _, _)
            | Lifecycle::WaitingMore(meta, _, _, _)
            | Lifecycle::IgnoredMore(meta, _)
            | Lifecycle::WaitingFirst(meta, _)
            | Lifecycle::AwaitNotif(meta, _)
            | Lifecycle::Notified(meta)
            | Lifecycle::Multi(meta, _) => meta,
        }
    }
}

/// Releases what a CQE carries, for a multishot op whose handle is gone.
trait Discard {
    fn discard(&mut self, cqe: Cqe);
}

/// Owns the data of a dropped multishot op until its final CQE.
struct Discarder<T>(T);

impl<T: OpAble> Discard for Discarder<T> {
    #[inline]
    fn discard(&mut self, cqe: Cqe) {
        self.0.discard_cqe(cqe)
    }
}

/// Take the provided buffers of each CQE of a multishot op when the driver reaps it, instead of
/// when the consumer polls (see [`MultiState::pretake`]).
pub(crate) struct PreTake {
    ring: BufRing,
    bundle: bool,
}

impl PreTake {
    pub(crate) fn new(ring: BufRing, bundle: bool) -> Self {
        Self { ring, bundle }
    }

    #[inline]
    fn take(&self, cqe: Cqe) -> Taken {
        if self.bundle {
            Taken::Bundle(self.ring.take_bundle(cqe))
        } else {
            Taken::Buf(self.ring.take(cqe))
        }
    }
}

/// What [`PreTake`] took for one CQE.
pub(crate) enum Taken {
    Buf(Option<SelectedBuf>),
    Bundle(Bundle),
}

/// State of a multishot operation.
#[derive(Default)]
struct MultiState {
    waker: Option<Waker>,
    /// CQEs not yet taken by the handle. Unbounded: the kernel keeps posting while the consumer
    /// does not poll. Its allocation is reused for the whole life of the op.
    queue: VecDeque<Cqe>,
    /// With `pretake`: the buffers of the CQEs in `queue`, one entry per CQE. Declared before
    /// `pretake` so the views drop before its ring handle.
    taken: VecDeque<Taken>,
    /// Take the buffers at reap time. Needed for incremental rings shared by several ops: the
    /// offset of a piece of a buffer comes from the order of the CQEs on the ring, and a
    /// partly used buffer may only go back to the ring once every CQE that points into it was
    /// taken. Consumers poll in their own order, so the driver takes them, in CQ order.
    pretake: Option<PreTake>,
    /// The final CQE (without `F_MORE`) is in `queue` or was taken.
    finished: bool,
    /// Set once the handle is dropped: owns the op data and discards every further CQE.
    ignored: Option<Box<dyn Discard>>,
}

/// An op slot: 64 bytes, with and without `metrics` (see the module docs).
pub(crate) struct MaybeFdLifecycle {
    lifecycle: Lifecycle,
}

/// `Meta::opcode` when there is nothing (more) to measure.
#[cfg(feature = "metrics")]
const NO_TICKS: u8 = u8::MAX;

/// Buckets of [`op_ticks_bucket`]: completed in the first tick after the submitting enter, one
/// tick later, 2-3 ticks later, 4 or more.
#[cfg(feature = "metrics")]
pub(crate) const OP_TICK_BUCKETS: usize = 4;

/// The bucket for an op pushed while the tick counter was `submit` and reaped in tick `now`
/// (the counter is bumped at the start of each tick, so the first tick that can see the CQE of
/// an inline op is `submit + 1`). Wrapping-safe.
#[cfg(feature = "metrics")]
#[inline]
pub(crate) fn op_ticks_bucket(submit: u32, now: u32) -> usize {
    match now.wrapping_sub(submit).saturating_sub(1) {
        0 => 0,
        1 => 1,
        2 | 3 => 2,
        _ => 3,
    }
}

/// The first CQE of a zero-copy send, see [`Ref::poll_zc_first`].
pub(crate) struct ZcFirst {
    pub(crate) meta: CompletionMeta,
    /// The slot is free: no notification follows (the send failed, or both CQEs were in).
    pub(crate) done: bool,
}

/// What [`Ref::drop_op`] did with a dropped op.
pub(crate) struct Dropped {
    /// The kernel may still be running the op: an `AsyncCancel` may end it sooner.
    #[cfg_attr(not(feature = "async-cancel"), allow(dead_code))]
    pub(crate) cancel: bool,
    /// The op data was kept in the slot: `Some(true)` inline, `Some(false)` boxed. Only for
    /// the `ignored_*` counters (and tests).
    #[cfg(any(test, feature = "metrics"))]
    pub(crate) kept_inline: Option<bool>,
}

/// What [`Ref::complete`] leaves to the driver.
#[derive(Default)]
pub(crate) struct Completed {
    /// A finished slot holding the data of a dropped op. Drop it once the driver no longer holds
    /// its own state: the drop may call back into the driver (a `SharedFd` closing, ...).
    pub(crate) deferred: Option<MaybeFdLifecycle>,
    /// For a multishot op: the length of its CQE queue after this CQE. Only for the multishot
    /// counters (and tests).
    #[cfg(any(test, feature = "metrics"))]
    pub(crate) queued: Option<usize>,
}

impl MaybeFdLifecycle {
    #[inline]
    pub(crate) fn new(is_fd: bool, kind: OpKind) -> Self {
        let meta = Meta {
            is_fd,
            kind,
            #[cfg(feature = "metrics")]
            opcode: NO_TICKS,
            #[cfg(feature = "metrics")]
            submit_tick: [0; 4],
        };
        let lifecycle = match kind {
            OpKind::Multishot => Lifecycle::Multi(meta, Box::default()),
            OpKind::Single | OpKind::ZeroCopy => Lifecycle::Submitted(meta),
        };
        Self { lifecycle }
    }

    /// The waker of a single-shot op whose future waits for it, if any.
    #[inline]
    pub(crate) fn waiting_waker(&self) -> Option<&Waker> {
        match &self.lifecycle {
            Lifecycle::Waiting(_, waker) => Some(waker),
            _ => None,
        }
    }

    /// Record the opcode and the driver's tick counter at submit (`metrics` only).
    #[cfg(feature = "metrics")]
    #[inline]
    pub(crate) fn set_submit_tick(&mut self, opcode: u32, tick: u32) {
        let meta = self.lifecycle.meta_mut();
        // Opcodes from 63 up share the last slot, like `ops_by_opcode`.
        meta.opcode = opcode.min(63) as u8;
        meta.submit_tick = tick.to_ne_bytes();
    }

    /// For the first CQE of the op: `(opcode, bucket)` of its completion latency in ticks, see
    /// [`op_ticks_bucket`]. `None` for later CQEs (zero-copy notification, further multishot
    /// CQEs) and for slots without a recorded submit. Call before `Ref::complete`.
    #[cfg(feature = "metrics")]
    #[inline]
    pub(crate) fn take_op_ticks(&mut self, now: u32) -> Option<(usize, usize)> {
        let meta = self.lifecycle.meta_mut();
        if meta.opcode == NO_TICKS {
            return None;
        }
        let opcode = std::mem::replace(&mut meta.opcode, NO_TICKS) as usize;
        let submit_tick = u32::from_ne_bytes(meta.submit_tick);
        Some((opcode, op_ticks_bucket(submit_tick, now)))
    }
}

impl Ref<'_, MaybeFdLifecycle> {
    /// Handle one CQE of this operation.
    ///
    /// # Safety
    /// `res` must come from the kernel for this operation, since it may be a fd.
    #[inline]
    pub(crate) unsafe fn complete(mut self, res: i32, flags: u32) -> Completed {
        let more = flags & IORING_CQE_F_MORE != 0;

        // Fast path: the final (only) CQE of a single-shot op, whose future is waiting or was
        // not polled yet. Writes the result in place: no move of the whole slot, no drop glue.
        if !more {
            let slot: *mut Lifecycle = &mut self.lifecycle;
            // SAFETY: `slot` points to the live state of this occupied slot. The shared borrow
            // ends before the state is overwritten; a moved-out waker is not dropped again, and
            // `Waiting`/`Submitted` own nothing else.
            //
            // The meta is copied last, right before the write, with nothing in between that
            // could change memory (building an error result may call out): the compiler then
            // sees that the copy stores what is already there and drops it.
            unsafe {
                match &*slot {
                    Lifecycle::Waiting(meta, waker) => {
                        let result = Self::result(res, meta.is_fd);
                        let waker = std::ptr::read(waker);
                        let meta = *meta;
                        std::ptr::write(slot, Lifecycle::Completed(meta, result, flags));
                        metric!(crate::runtime_stats::note_op_completed());
                        waker.wake();
                        return Completed::default();
                    }
                    Lifecycle::Submitted(meta) => {
                        let result = Self::result(res, meta.is_fd);
                        let meta = *meta;
                        std::ptr::write(slot, Lifecycle::Completed(meta, result, flags));
                        return Completed::default();
                    }
                    _ => {}
                }
            }
        }
        let (slab, key) = self.into_parts();
        // SAFETY: as for this function; `key` is the occupied slot of the `Ref` just taken
        // apart.
        unsafe { Self::complete_slow(slab, key, res, flags) }
    }

    #[inline]
    fn result(res: i32, is_fd: bool) -> io::Result<MaybeFd> {
        if res >= 0 {
            // SAFETY: `res` is the result of the op, a fd if `is_fd`.
            unsafe { MaybeFd::new_result(Ok(res as u32), is_fd) }
        } else {
            Err(io::Error::from_raw_os_error(-res))
        }
    }

    /// [`complete`](Self::complete) for everything but the fast path: multishot ops, CQEs with
    /// `F_MORE`, zero-copy and dropped ops.
    ///
    /// Takes the slot as slab and key, which travel in registers. A `Ref` is passed in memory,
    /// and those stores would sit before the branch to the fast path, on every CQE.
    ///
    /// # Safety
    /// As for [`complete`](Self::complete), and the slot of `key` must be occupied.
    #[inline(never)]
    unsafe fn complete_slow(
        slab: &mut Slab<MaybeFdLifecycle>,
        key: usize,
        res: i32,
        flags: u32,
    ) -> Completed {
        // SAFETY: the slot is occupied (caller).
        let mut this = unsafe { slab.get_unchecked(key) };
        let more = flags & IORING_CQE_F_MORE != 0;
        let meta = this.lifecycle.meta();
        if let Lifecycle::Multi(_, state) = &mut this.lifecycle {
            let cqe = Cqe::new(res, flags);
            if let Some(ignored) = state.ignored.as_mut() {
                ignored.discard(cqe);
                if more {
                    return Completed {
                        deferred: None,
                        #[cfg(any(test, feature = "metrics"))]
                        queued: Some(0),
                    };
                }
                return Completed {
                    deferred: Some(this.remove()),
                    #[cfg(any(test, feature = "metrics"))]
                    queued: Some(0),
                };
            }
            if let Some(pre) = &state.pretake {
                state.taken.push_back(pre.take(cqe));
            }
            state.queue.push_back(cqe);
            state.finished |= !more;
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
            return Completed {
                deferred: None,
                #[cfg(any(test, feature = "metrics"))]
                queued: Some(state.queue.len()),
            };
        }

        let result = Self::result(res, meta.is_fd);
        if more && meta.kind == OpKind::Single {
            trace!(
                "MONOIO DEBUG[lifecycle]: F_MORE on a single-shot op, flags {}",
                flags
            );
        }

        // A first CQE with F_MORE is kept until the final CQE (without F_MORE: the SEND_ZC
        // notification, or the last CQE of an unexpected multishot); results of the CQEs in
        // between are dropped, which closes a fd they carry.
        let lifecycle = &mut this.lifecycle;
        let mut wake = None;
        let next = match std::mem::replace(lifecycle, Lifecycle::Submitted(meta)) {
            Lifecycle::WaitingFirst(_, waker) => {
                // Two-step zero-copy: the result is what the submitter waits for.
                wake = Some(waker);
                Some(if more {
                    Lifecycle::CompletedMore(meta, result, flags)
                } else {
                    Lifecycle::Completed(meta, result, flags)
                })
            }
            Lifecycle::AwaitNotif(_, waker) if !more => {
                wake = waker;
                Some(Lifecycle::Notified(meta))
            }
            Lifecycle::Submitted(_) if more => Some(Lifecycle::CompletedMore(meta, result, flags)),
            Lifecycle::Submitted(_) => Some(Lifecycle::Completed(meta, result, flags)),
            Lifecycle::Waiting(_, waker) if more => {
                // Don't wake yet: the final CQE is still to come.
                Some(Lifecycle::WaitingMore(meta, waker, result, flags))
            }
            Lifecycle::Waiting(_, waker) => {
                wake = Some(waker);
                Some(Lifecycle::Completed(meta, result, flags))
            }
            Lifecycle::Ignored(_, data) if more => Some(Lifecycle::IgnoredMore(meta, data)),
            Lifecycle::CompletedMore(_, stored, stored_flags) if !more => Some(
                Lifecycle::Completed(meta, stored, stored_flags & !IORING_CQE_F_MORE),
            ),
            Lifecycle::WaitingMore(_, waker, stored, stored_flags) if !more => {
                wake = Some(waker);
                Some(Lifecycle::Completed(
                    meta,
                    stored,
                    stored_flags & !IORING_CQE_F_MORE,
                ))
            }
            Lifecycle::Ignored(_, data) | Lifecycle::IgnoredMore(_, data) if !more => {
                // Put it back so that `remove` hands it out to be dropped later.
                *lifecycle = Lifecycle::Ignored(meta, data);
                None
            }
            // A CQE in between, or one after the op completed: keep the state.
            other => {
                if let Lifecycle::Completed(..) = other {
                    trace!(
                        "MONOIO DEBUG[lifecycle]: CQE for a completed op, flags {}",
                        flags
                    );
                }
                Some(other)
            }
        };
        match next {
            Some(next) => {
                *lifecycle = next;
                if let Some(waker) = wake {
                    metric!(crate::runtime_stats::note_op_completed());
                    waker.wake();
                }
                Completed::default()
            }
            None => Completed {
                deferred: Some(this.remove()),
                #[cfg(any(test, feature = "metrics"))]
                queued: None,
            },
        }
    }

    /// Poll the op of this slot: its result once the final CQE is in (which frees the slot).
    ///
    /// The three states an op is normally polled in are handled here, with compares instead
    /// of a jump table and nothing that needs more than a scratch register or two; the other
    /// states are out of line ([`poll_op_slow`](Self::poll_op_slow)), so that the common polls
    /// do not pay for their register saves.
    #[allow(clippy::needless_pass_by_ref_mut)]
    #[inline]
    pub(crate) fn poll_op(mut self, cx: &mut Context<'_>) -> Poll<CompletionMeta> {
        let slot: *mut Lifecycle = &mut self.lifecycle;
        // SAFETY: `slot` points to the live state of this occupied slot; the borrow of the
        // match ends before the `Submitted` arm overwrites the state.
        match unsafe { &mut *slot } {
            Lifecycle::Completed(_, result, flags) => {
                let meta = CompletionMeta {
                    // SAFETY: the result is moved out here and the slot is freed right below
                    // without dropping it; nothing else in the slot needs a drop.
                    result: unsafe { std::ptr::read(result) },
                    flags: *flags,
                };
                // SAFETY: see above, only plain data is left in the slot.
                unsafe { self.remove_forget() };
                Poll::Ready(meta)
            }
            Lifecycle::Submitted(_) => {
                let waker = cx.waker().clone();
                // SAFETY: `Submitted` owns nothing, so the old state needs no drop. A plain
                // assignment calls the drop glue of `Lifecycle`: after the clone (a call
                // through the waker's vtable) the compiler no longer knows the state. For the
                // same reason the meta is read after the clone: no value to keep across it.
                unsafe {
                    let meta = (*slot).meta();
                    std::ptr::write(slot, Lifecycle::Waiting(meta, waker));
                }
                Poll::Pending
            }
            // Polled again before the CQE, by the task that already waits.
            Lifecycle::Waiting(_, waker) if waker.will_wake(cx.waker()) => Poll::Pending,
            _ => {
                let (slab, key) = self.into_parts();
                // SAFETY: `key` is the occupied slot of the `Ref` just taken apart.
                unsafe { Self::poll_op_slow(slab, key, cx) }
            }
        }
    }

    /// [`poll_op`](Self::poll_op) for the states it leaves out: a new waker, the states of a
    /// zero-copy send, and the states an `Op` is never polled in.
    ///
    /// # Safety
    /// The slot of `key` must be occupied.
    #[inline(never)]
    unsafe fn poll_op_slow(
        slab: &mut Slab<MaybeFdLifecycle>,
        key: usize,
        cx: &mut Context<'_>,
    ) -> Poll<CompletionMeta> {
        // SAFETY: the slot is occupied (caller).
        let mut this = unsafe { slab.get_unchecked(key) };
        let meta = this.lifecycle.meta();
        let ref_mut = &mut this.lifecycle;
        match ref_mut {
            Lifecycle::Submitted(_) => {
                *ref_mut = Lifecycle::Waiting(meta, cx.waker().clone());
                Poll::Pending
            }
            Lifecycle::Waiting(_, waker) | Lifecycle::WaitingMore(_, waker, _, _) => {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
                Poll::Pending
            }
            // Multi-CQE: first CQE arrived but still waiting for notification
            Lifecycle::CompletedMore(..) => {
                if let Lifecycle::CompletedMore(_, result, flags) =
                    std::mem::replace(ref_mut, Lifecycle::Submitted(meta))
                {
                    *ref_mut = Lifecycle::WaitingMore(meta, cx.waker().clone(), result, flags);
                }
                Poll::Pending
            }
            Lifecycle::Completed(_, result, flags) => {
                let meta = CompletionMeta {
                    // SAFETY: the result is moved out here and the slot is freed right below
                    // without dropping it; nothing else in the slot needs a drop.
                    result: unsafe { std::ptr::read(result) },
                    flags: *flags,
                };
                // SAFETY: see above, only plain data is left in the slot.
                unsafe { this.remove_forget() };
                Poll::Ready(meta)
            }
            // Not reachable through `Op` (dropped ops are not polled, multishot ops are not
            // `Op`s, two-step zero-copy sends use `poll_zc_*`).
            Lifecycle::Ignored(..)
            | Lifecycle::IgnoredMore(..)
            | Lifecycle::Multi(..)
            | Lifecycle::WaitingFirst(..)
            | Lifecycle::AwaitNotif(..)
            | Lifecycle::Notified(_) => Poll::Pending,
        }
    }

    /// The `Op` of this slot is dropped. Frees the slot if the op is finished, else keeps
    /// `data` in it until the final CQE.
    pub(crate) fn drop_op<T: 'static>(mut self, data: &mut Option<T>) -> Dropped {
        #[cfg(any(test, feature = "metrics"))]
        let mut kept_inline = None;
        #[cfg_attr(not(any(test, feature = "metrics")), allow(unused_mut))]
        let mut keep = |data: &mut Option<T>| -> IgnoredData {
            match data.take() {
                Some(data) => {
                    #[cfg(any(test, feature = "metrics"))]
                    {
                        kept_inline = Some(IgnoredData::fits::<T>());
                    }
                    IgnoredData::new(data)
                }
                None => {
                    #[cfg(any(test, feature = "metrics"))]
                    {
                        kept_inline = Some(true);
                    }
                    IgnoredData::empty()
                }
            }
        };
        let meta = self.lifecycle.meta();
        let ref_mut = &mut self.lifecycle;
        let cancel = match ref_mut {
            Lifecycle::Submitted(_) | Lifecycle::Waiting(..) | Lifecycle::WaitingFirst(..) => {
                *ref_mut = Lifecycle::Ignored(meta, keep(data));
                true
            }
            // Zero-copy: the send is done, only the notification (the kernel releasing the
            // buffer) is still to come, so keep the slot and the buffer alive; a cancel could not
            // speed that up. Dropping the stored result closes a fd it carries.
            Lifecycle::CompletedMore(..)
            | Lifecycle::WaitingMore(..)
            | Lifecycle::AwaitNotif(..) => {
                *ref_mut = Lifecycle::IgnoredMore(meta, keep(data));
                false
            }
            Lifecycle::Completed(..) | Lifecycle::Notified(_) => {
                self.remove();
                false
            }
            // Already dropped, or not an `Op`: nothing to do.
            Lifecycle::Ignored(..) | Lifecycle::IgnoredMore(..) | Lifecycle::Multi(..) => false,
        };
        Dropped {
            cancel,
            #[cfg(any(test, feature = "metrics"))]
            kept_inline,
        }
    }

    /// Two-step zero-copy send: the result of the first CQE. When the notification is still to
    /// come, the slot stays (owning nothing: the caller keeps the data) until
    /// [`poll_zc_notif`](Self::poll_zc_notif) saw it; when it is not (`done`), the slot is freed.
    pub(crate) fn poll_zc_first(mut self, cx: &mut Context<'_>) -> Poll<ZcFirst> {
        let meta = self.lifecycle.meta();
        let ref_mut = &mut self.lifecycle;
        match ref_mut {
            Lifecycle::WaitingFirst(_, waker) => {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
                return Poll::Pending;
            }
            Lifecycle::Submitted(_) | Lifecycle::Waiting(..) => {
                *ref_mut = Lifecycle::WaitingFirst(meta, cx.waker().clone());
                return Poll::Pending;
            }
            Lifecycle::CompletedMore(..) | Lifecycle::WaitingMore(..) => {
                let (result, flags) =
                    match std::mem::replace(ref_mut, Lifecycle::AwaitNotif(meta, None)) {
                        Lifecycle::CompletedMore(_, result, flags)
                        | Lifecycle::WaitingMore(_, _, result, flags) => (result, flags),
                        _ => unreachable!(),
                    };
                return Poll::Ready(ZcFirst {
                    meta: CompletionMeta { result, flags },
                    done: false,
                });
            }
            Lifecycle::Completed(..) => {}
            // The result was taken already, or not a zero-copy `Op`: the futures never ask.
            _ => {
                return Poll::Ready(ZcFirst {
                    meta: CompletionMeta {
                        result: Err(io::Error::other("zero-copy result already taken")),
                        flags: 0,
                    },
                    done: false,
                })
            }
        }
        match self.remove().lifecycle {
            Lifecycle::Completed(_, result, flags) => Poll::Ready(ZcFirst {
                meta: CompletionMeta { result, flags },
                done: true,
            }),
            _ => unreachable!(),
        }
    }

    /// Two-step zero-copy send, after [`poll_zc_first`](Self::poll_zc_first): ready (and the
    /// slot freed) once the notification CQE arrived.
    pub(crate) fn poll_zc_notif(mut self, cx: &mut Context<'_>) -> Poll<()> {
        match &mut self.lifecycle {
            Lifecycle::AwaitNotif(_, waker) => {
                match waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => *waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
            Lifecycle::Notified(_) | Lifecycle::Completed(..) => {
                self.remove();
                Poll::Ready(())
            }
            // Not reachable through the futures.
            _ => Poll::Pending,
        }
    }

    /// Two-step zero-copy send: whether the notification CQE arrived (without consuming it).
    pub(crate) fn zc_released(&self) -> bool {
        matches!(
            self.lifecycle,
            Lifecycle::Notified(_) | Lifecycle::Completed(..)
        )
    }

    /// Next CQE of a multishot op. `Ready(None)` once the final CQE was taken. Taking the final
    /// CQE frees the slot.
    pub(crate) fn poll_multi(self, cx: &mut Context<'_>) -> Poll<Option<Cqe>> {
        self.poll_multi_taken(cx)
            .map(|next| next.map(|(cqe, _)| cqe))
    }

    /// [`poll_multi`](Self::poll_multi), with the buffers taken at reap time if the op has a
    /// [`PreTake`].
    pub(crate) fn poll_multi_taken(
        mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(Cqe, Option<Taken>)>> {
        let Lifecycle::Multi(_, state) = &mut self.lifecycle else {
            return Poll::Ready(None);
        };
        if let Some(cqe) = state.queue.pop_front() {
            let taken = state.taken.pop_front();
            if !cqe.more() {
                self.remove();
            }
            return Poll::Ready(Some((cqe, taken)));
        }
        if state.finished {
            self.remove();
            return Poll::Ready(None);
        }
        match &state.waker {
            Some(waker) if waker.will_wake(cx.waker()) => {}
            _ => state.waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }

    /// The handle of a multishot op is dropped: discard the queued CQEs, then free the slot if
    /// the op is finished (returns true) or keep `data` in it until the final CQE (false).
    pub(crate) fn drop_multi<T: OpAble + 'static>(mut self, data: &mut Option<T>) -> bool {
        let Lifecycle::Multi(_, state) = &mut self.lifecycle else {
            return true;
        };
        state.waker = None;
        if state.pretake.is_some() {
            // Already taken: dropping them releases the buffers.
            state.taken.clear();
            state.queue.clear();
        }
        match data.as_mut() {
            Some(data) => state.queue.drain(..).for_each(|cqe| data.discard_cqe(cqe)),
            None => state.queue.clear(),
        }
        if state.finished {
            self.remove();
            return true;
        }
        state.ignored = Some(match data.take() {
            Some(data) => Box::new(Discarder(data)),
            None => Box::new(Discarder(NoData)),
        });
        false
    }

    /// If the kernel is done with a multishot op: discard the CQEs not taken yet, free the slot
    /// and return true.
    pub(crate) fn finish_multi<T: OpAble>(mut self, data: &mut T) -> bool {
        let Lifecycle::Multi(_, state) = &mut self.lifecycle else {
            return true;
        };
        if !state.finished {
            return false;
        }
        if state.pretake.is_some() {
            state.taken.clear();
            state.queue.clear();
        }
        state.queue.drain(..).for_each(|cqe| data.discard_cqe(cqe));
        self.remove();
        true
    }

    /// Take the provided buffers of every later CQE of this multishot op at reap time. Set it
    /// right after the submit, before the driver can reap a CQE of the op.
    pub(crate) fn set_pretake(mut self, pre: PreTake) {
        if let Lifecycle::Multi(_, state) = &mut self.lifecycle {
            debug_assert!(state.queue.is_empty());
            state.pretake = Some(pre);
        }
    }

    /// Whether the kernel posted the final CQE of a multishot op.
    pub(crate) fn multi_finished(&self) -> bool {
        match &self.lifecycle {
            Lifecycle::Multi(_, state) => state.finished,
            _ => true,
        }
    }
}

/// Stand-in data for a dropped multishot op that had none left.
struct NoData;

impl OpAble for NoData {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Nop::new().build()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    };

    use super::*;

    const IORING_CQE_F_NOTIF: u32 = 8;

    fn vtable_ref() -> &'static RawWakerVTable {
        &RawWakerVTable::new(
            |ptr| {
                let arc = unsafe { Arc::from_raw(ptr as *const AtomicBool) };
                let cloned = arc.clone();
                std::mem::forget(arc);
                RawWaker::new(Arc::into_raw(cloned) as *const (), vtable_ref())
            },
            |ptr| {
                let arc = unsafe { Arc::from_raw(ptr as *const AtomicBool) };
                arc.store(true, Ordering::SeqCst);
            },
            |ptr| {
                let arc = unsafe { Arc::from_raw(ptr as *const AtomicBool) };
                arc.store(true, Ordering::SeqCst);
                std::mem::forget(arc);
            },
            |ptr| {
                unsafe { Arc::from_raw(ptr as *const AtomicBool) };
            },
        )
    }

    /// Create a waker that sets a flag when woken.
    fn flag_waker() -> (Waker, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let raw = RawWaker::new(Arc::into_raw(flag_clone) as *const (), vtable_ref());
        let waker = unsafe { Waker::from_raw(raw) };
        (waker, flag)
    }

    fn insert_lifecycle(slab: &mut Slab<MaybeFdLifecycle>) -> usize {
        slab.insert(MaybeFdLifecycle::new(false, OpKind::Single))
    }

    /// Submitted → MORE CQE → CompletedMore → NOTIF CQE → Completed → poll → Ready
    #[test]
    fn multi_cqe_no_poll_between() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);

        // First CQE with MORE flag
        let r = slab.get(key).unwrap();
        unsafe { r.complete(42, IORING_CQE_F_MORE) };

        // Notification CQE
        let r = slab.get(key).unwrap();
        unsafe { r.complete(0, IORING_CQE_F_NOTIF) };

        // Poll should return Ready with the result from the first CQE
        let (waker, _flag) = flag_waker();
        let mut cx = Context::from_waker(&waker);
        let r = slab.get(key).unwrap();
        match r.poll_op(&mut cx) {
            Poll::Ready(meta) => {
                assert_eq!(meta.result.unwrap().into_inner(), 42);
                // MORE flag should be cleared
                assert_eq!(meta.flags & IORING_CQE_F_MORE, 0);
            }
            Poll::Pending => panic!("expected Ready"),
        }
    }

    /// Submitted → poll → Waiting → MORE CQE → WaitingMore → poll (Pending)
    /// → NOTIF CQE (wakes) → poll → Ready
    #[test]
    fn multi_cqe_poll_between() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);

        let (waker, woken) = flag_waker();
        let mut cx = Context::from_waker(&waker);

        // First poll: Submitted → Waiting
        let r = slab.get(key).unwrap();
        assert!(r.poll_op(&mut cx).is_pending());

        // First CQE with MORE: Waiting → WaitingMore (should NOT wake)
        woken.store(false, Ordering::SeqCst);
        let r = slab.get(key).unwrap();
        unsafe { r.complete(100, IORING_CQE_F_MORE) };
        assert!(!woken.load(Ordering::SeqCst), "should not wake on MORE CQE");

        // Poll again: WaitingMore → still Pending
        let r = slab.get(key).unwrap();
        assert!(r.poll_op(&mut cx).is_pending());

        // Notification CQE: WaitingMore → Completed, wakes
        woken.store(false, Ordering::SeqCst);
        let r = slab.get(key).unwrap();
        unsafe { r.complete(0, IORING_CQE_F_NOTIF) };
        assert!(woken.load(Ordering::SeqCst), "should wake on NOTIF CQE");

        // Final poll: Completed → Ready
        let r = slab.get(key).unwrap();
        match r.poll_op(&mut cx) {
            Poll::Ready(meta) => {
                assert_eq!(meta.result.unwrap().into_inner(), 100);
                assert_eq!(meta.flags & IORING_CQE_F_MORE, 0);
            }
            Poll::Pending => panic!("expected Ready"),
        }
    }

    /// Submitted → MORE CQE → CompletedMore → drop_op → IgnoredMore
    /// → NOTIF CQE → slot removed
    #[test]
    fn multi_cqe_drop_before_notification_from_completed_more() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);

        // First CQE with MORE
        let r = slab.get(key).unwrap();
        unsafe { r.complete(10, IORING_CQE_F_MORE) };

        // Drop the op (future dropped before notification)
        let r = slab.get(key).unwrap();
        let mut data: Option<()> = Some(());
        let dropped = r.drop_op(&mut data);
        assert!(dropped.kept_inline.is_some(), "notification pending");
        // The send is done: nothing left to cancel.
        assert!(!dropped.cancel);

        // Notification CQE arrives — should clean up the slot
        let r = slab.get(key).unwrap();
        unsafe { r.complete(0, IORING_CQE_F_NOTIF) };

        // Slot should have been removed
        assert!(slab.get(key).is_none());
    }

    /// Submitted → poll → Waiting → MORE CQE → WaitingMore → drop_op → IgnoredMore
    /// → NOTIF CQE → slot removed
    #[test]
    fn multi_cqe_drop_before_notification_from_waiting_more() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);

        let (waker, _flag) = flag_waker();
        let mut cx = Context::from_waker(&waker);

        // Poll: Submitted → Waiting
        let r = slab.get(key).unwrap();
        assert!(r.poll_op(&mut cx).is_pending());

        // First CQE with MORE: Waiting → WaitingMore
        let r = slab.get(key).unwrap();
        unsafe { r.complete(20, IORING_CQE_F_MORE) };

        // Drop the op
        let r = slab.get(key).unwrap();
        let mut data: Option<()> = Some(());
        let dropped = r.drop_op(&mut data);
        assert!(dropped.kept_inline.is_some(), "notification pending");
        // The send is done: nothing left to cancel.
        assert!(!dropped.cancel);

        // Notification CQE arrives — should clean up the slot
        let r = slab.get(key).unwrap();
        unsafe { r.complete(0, IORING_CQE_F_NOTIF) };

        // Slot should have been removed
        assert!(slab.get(key).is_none());
    }

    /// Normal single-CQE completion path (regression guard).
    #[test]
    fn single_cqe_submitted_to_completed() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);

        // Complete without MORE flag
        let r = slab.get(key).unwrap();
        unsafe { r.complete(7, 0) };

        // Poll should return Ready
        let (waker, _flag) = flag_waker();
        let mut cx = Context::from_waker(&waker);
        let r = slab.get(key).unwrap();
        match r.poll_op(&mut cx) {
            Poll::Ready(meta) => {
                assert_eq!(meta.result.unwrap().into_inner(), 7);
                assert_eq!(meta.flags, 0);
            }
            Poll::Pending => panic!("expected Ready"),
        }
    }

    /// Single-CQE with waker: Submitted → poll → Waiting → complete → wakes → poll → Ready
    #[test]
    fn single_cqe_with_waker() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);

        let (waker, woken) = flag_waker();
        let mut cx = Context::from_waker(&waker);

        // Poll: Submitted → Waiting
        let r = slab.get(key).unwrap();
        assert!(r.poll_op(&mut cx).is_pending());

        // Complete: Waiting → Completed, wakes
        woken.store(false, Ordering::SeqCst);
        let r = slab.get(key).unwrap();
        unsafe { r.complete(99, 0) };
        assert!(woken.load(Ordering::SeqCst));

        // Poll: Completed → Ready
        let r = slab.get(key).unwrap();
        match r.poll_op(&mut cx) {
            Poll::Ready(meta) => {
                assert_eq!(meta.result.unwrap().into_inner(), 99);
            }
            Poll::Pending => panic!("expected Ready"),
        }
    }

    /// Unexpected CQEs never panic: a follow-up on a single op, a CQE after completion.
    #[test]
    fn unexpected_cqes_are_ignored() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);
        // An unexpected multishot on a single op: the first result is kept, the op ends with
        // the CQE without F_MORE.
        unsafe { slab.get(key).unwrap().complete(1, IORING_CQE_F_MORE) };
        unsafe { slab.get(key).unwrap().complete(2, IORING_CQE_F_MORE) };
        unsafe { slab.get(key).unwrap().complete(3, 0) };
        // Stray CQE for the completed op.
        unsafe { slab.get(key).unwrap().complete(4, 0) };
        unsafe { slab.get(key).unwrap().complete(5, IORING_CQE_F_NOTIF) };
        let (waker, _flag) = flag_waker();
        let mut cx = Context::from_waker(&waker);
        match slab.get(key).unwrap().poll_op(&mut cx) {
            Poll::Ready(meta) => assert_eq!(meta.result.unwrap().into_inner(), 1),
            Poll::Pending => panic!("expected Ready"),
        }
        assert!(slab.get(key).is_none());

        // NOTIF while nothing is pending, then the real completion.
        let key = insert_lifecycle(&mut slab);
        unsafe {
            slab.get(key)
                .unwrap()
                .complete(-libc::EINVAL, IORING_CQE_F_NOTIF)
        };
        match slab.get(key).unwrap().poll_op(&mut cx) {
            Poll::Ready(meta) => assert!(meta.result.is_err()),
            Poll::Pending => panic!("expected Ready"),
        }
    }

    /// Dropped single op: the data is handed out to be dropped later.
    #[test]
    fn ignored_single_defers_data() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);
        let mut data = Some(vec![1u8]);
        let dropped = slab.get(key).unwrap().drop_op(&mut data);
        assert!(dropped.cancel);
        assert_eq!(dropped.kept_inline, Some(true));
        assert!(data.is_none());
        let done = unsafe { slab.get(key).unwrap().complete(0, 0) };
        assert!(done.deferred.is_some());
        assert!(slab.get(key).is_none());
    }

    fn insert_zc(slab: &mut Slab<MaybeFdLifecycle>) -> usize {
        slab.insert(MaybeFdLifecycle::new(false, OpKind::ZeroCopy))
    }

    /// Two-step zero-copy: the first CQE wakes and is handed out, then the notification wakes
    /// the pending side and frees the slot.
    #[test]
    fn zc_two_step_result_then_release() {
        let mut slab = Slab::new();
        let key = insert_zc(&mut slab);
        let (waker, woken) = flag_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(slab.get(key).unwrap().poll_zc_first(&mut cx).is_pending());
        unsafe { slab.get(key).unwrap().complete(42, IORING_CQE_F_MORE) };
        assert!(woken.swap(false, Ordering::SeqCst), "the result wakes");
        match slab.get(key).unwrap().poll_zc_first(&mut cx) {
            Poll::Ready(first) => {
                assert_eq!(first.meta.result.unwrap().into_inner(), 42);
                assert!(!first.done);
            }
            Poll::Pending => panic!("expected the result"),
        }
        assert!(!slab.get(key).unwrap().zc_released());
        assert!(slab.get(key).unwrap().poll_zc_notif(&mut cx).is_pending());
        // A stray F_MORE CQE changes nothing.
        unsafe { slab.get(key).unwrap().complete(0, IORING_CQE_F_MORE) };
        assert!(!woken.load(Ordering::SeqCst));
        unsafe { slab.get(key).unwrap().complete(0, IORING_CQE_F_NOTIF) };
        assert!(woken.load(Ordering::SeqCst), "the notification wakes");
        assert!(slab.get(key).unwrap().zc_released());
        assert!(slab.get(key).unwrap().poll_zc_notif(&mut cx).is_ready());
        assert!(slab.get(key).is_none());
    }

    /// Both CQEs before the first poll, and a failed send without notification: the result
    /// comes with `done`, and the slot is freed at once.
    #[test]
    fn zc_two_step_done_at_once() {
        let mut slab = Slab::new();
        let (waker, _woken) = flag_waker();
        let mut cx = Context::from_waker(&waker);

        let key = insert_zc(&mut slab);
        unsafe { slab.get(key).unwrap().complete(7, IORING_CQE_F_MORE) };
        unsafe { slab.get(key).unwrap().complete(0, IORING_CQE_F_NOTIF) };
        match slab.get(key).unwrap().poll_zc_first(&mut cx) {
            Poll::Ready(first) => {
                assert_eq!(first.meta.result.unwrap().into_inner(), 7);
                assert!(first.done);
            }
            Poll::Pending => panic!("expected the result"),
        }
        assert!(slab.get(key).is_none());

        let key = insert_zc(&mut slab);
        assert!(slab.get(key).unwrap().poll_zc_first(&mut cx).is_pending());
        unsafe { slab.get(key).unwrap().complete(-libc::EPIPE, 0) };
        match slab.get(key).unwrap().poll_zc_first(&mut cx) {
            Poll::Ready(first) => {
                assert_eq!(
                    first.meta.result.unwrap_err().raw_os_error(),
                    Some(libc::EPIPE)
                );
                assert!(first.done);
            }
            Poll::Pending => panic!("expected the result"),
        }
        assert!(slab.get(key).is_none());
    }

    /// Dropping the pending side keeps the data until the notification, without a cancel;
    /// dropping it after the notification frees the slot at once; dropping before the result
    /// asks for a cancel.
    #[test]
    fn zc_two_step_drops() {
        let mut slab = Slab::new();
        let (waker, _woken) = flag_waker();
        let mut cx = Context::from_waker(&waker);
        let count = std::rc::Rc::new(());

        // Dropped while waiting for the notification.
        let key = insert_zc(&mut slab);
        unsafe { slab.get(key).unwrap().complete(1, IORING_CQE_F_MORE) };
        assert!(slab.get(key).unwrap().poll_zc_first(&mut cx).is_ready());
        let mut data = Some(count.clone());
        let dropped = slab.get(key).unwrap().drop_op(&mut data);
        assert!(!dropped.cancel);
        assert_eq!(dropped.kept_inline, Some(true));
        assert_eq!(std::rc::Rc::strong_count(&count), 2);
        let done = unsafe { slab.get(key).unwrap().complete(0, IORING_CQE_F_NOTIF) };
        assert!(slab.get(key).is_none());
        drop(done);
        assert_eq!(std::rc::Rc::strong_count(&count), 1);

        // Dropped after the notification.
        let key = insert_zc(&mut slab);
        unsafe { slab.get(key).unwrap().complete(1, IORING_CQE_F_MORE) };
        assert!(slab.get(key).unwrap().poll_zc_first(&mut cx).is_ready());
        unsafe { slab.get(key).unwrap().complete(0, IORING_CQE_F_NOTIF) };
        let mut data = Some(count.clone());
        let dropped = slab.get(key).unwrap().drop_op(&mut data);
        assert!(!dropped.cancel);
        assert_eq!(dropped.kept_inline, None);
        assert!(slab.get(key).is_none());

        // Dropped before the result.
        let key = insert_zc(&mut slab);
        assert!(slab.get(key).unwrap().poll_zc_first(&mut cx).is_pending());
        let dropped = slab.get(key).unwrap().drop_op(&mut data);
        assert!(dropped.cancel);
        unsafe { slab.get(key).unwrap().complete(-libc::ECANCELED, 0) };
        assert!(slab.get(key).is_none());
    }

    /// Large op data is boxed, small data stays inline.
    #[test]
    fn ignored_inline_or_boxed() {
        let mut slab = Slab::new();
        let key = insert_lifecycle(&mut slab);
        let mut data = Some([0u64; 32]);
        let dropped = slab.get(key).unwrap().drop_op(&mut data);
        assert_eq!(dropped.kept_inline, Some(false));
        unsafe { slab.get(key).unwrap().complete(0, 0) };

        let key = insert_lifecycle(&mut slab);
        let mut none: Option<Vec<u8>> = None;
        let dropped = slab.get(key).unwrap().drop_op(&mut none);
        assert_eq!(dropped.kept_inline, Some(true));
        unsafe { slab.get(key).unwrap().complete(0, 0) };
        assert!(slab.get(key).is_none());
    }

    /// Tick-latency buckets: same tick, 1, 2-3, 4+; only the first CQE counts.
    #[cfg(feature = "metrics")]
    #[test]
    fn op_ticks_buckets() {
        use super::op_ticks_bucket;
        assert_eq!(op_ticks_bucket(5, 5), 0);
        assert_eq!(op_ticks_bucket(5, 6), 0);
        assert_eq!(op_ticks_bucket(5, 7), 1);
        assert_eq!(op_ticks_bucket(5, 8), 2);
        assert_eq!(op_ticks_bucket(5, 9), 2);
        assert_eq!(op_ticks_bucket(5, 10), 3);
        assert_eq!(op_ticks_bucket(u32::MAX, 0), 0);
        assert_eq!(op_ticks_bucket(u32::MAX - 1, 2), 2);

        let mut slab = Slab::new();
        let key = insert_zc(&mut slab);
        assert_eq!(slab.get(key).unwrap().take_op_ticks(1), None);
        slab.get(key).unwrap().set_submit_tick(26, 1);
        assert_eq!(slab.get(key).unwrap().take_op_ticks(4), Some((26, 2)));
        assert_eq!(slab.get(key).unwrap().take_op_ticks(9), None);
        assert_eq!(super::OP_TICK_BUCKETS, 4);
    }

    /// An op slot is one cache line, with and without `metrics`, and still keeps 48 bytes of
    /// a dropped op inline. The layout of the states is the compiler's choice (the meta goes
    /// into the word of the tag, the slab's vacant marker into a spare tag value), so it is
    /// pinned here rather than by a compile-time assertion that could break a user's build.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn slot_is_one_cache_line() {
        use std::mem::{align_of, size_of};

        use crate::driver::uring::ignored::INLINE_SIZE;

        assert!(size_of::<Meta>() <= 7);
        assert_eq!(align_of::<Meta>(), 1);
        assert_eq!(INLINE_SIZE, 48);
        assert_eq!(size_of::<IgnoredData>(), 56);
        assert_eq!(size_of::<Lifecycle>(), 64);
        assert_eq!(size_of::<MaybeFdLifecycle>(), 64);
        assert_eq!(size_of::<Option<MaybeFdLifecycle>>(), 64);
        assert_eq!(Slab::<MaybeFdLifecycle>::ENTRY_SIZE, 64);
        assert_eq!(Slab::<MaybeFdLifecycle>::ENTRY_ALIGN, 64);
    }

    /// The meta survives every transition of a slot.
    #[test]
    fn meta_is_kept_across_states() {
        let (waker, _flag) = flag_waker();
        let mut cx = Context::from_waker(&waker);
        let check = |slab: &mut Slab<MaybeFdLifecycle>, key: usize, kind: OpKind| {
            let meta = slab.get(key).unwrap().lifecycle.meta();
            assert!(meta.is_fd);
            assert_eq!(meta.kind, kind);
        };

        // Submitted -> Waiting -> Completed.
        let mut slab = Slab::new();
        let key = slab.insert(MaybeFdLifecycle::new(true, OpKind::Single));
        check(&mut slab, key, OpKind::Single);
        assert!(slab.get(key).unwrap().poll_op(&mut cx).is_pending());
        check(&mut slab, key, OpKind::Single);
        unsafe { slab.get(key).unwrap().complete(-libc::EINVAL, 0) };
        check(&mut slab, key, OpKind::Single);
        assert!(slab.get(key).unwrap().poll_op(&mut cx).is_ready());

        // Submitted -> Ignored -> removed with the final CQE.
        let key = slab.insert(MaybeFdLifecycle::new(true, OpKind::Single));
        let mut data = Some([0u64; 6]);
        assert_eq!(
            slab.get(key).unwrap().drop_op(&mut data).kept_inline,
            Some(true)
        );
        check(&mut slab, key, OpKind::Single);
        let done = unsafe { slab.get(key).unwrap().complete(-libc::ECANCELED, 0) };
        assert!(done.deferred.unwrap().lifecycle.meta().is_fd);

        // Zero-copy: WaitingFirst -> CompletedMore -> AwaitNotif -> Notified.
        let key = slab.insert(MaybeFdLifecycle::new(true, OpKind::ZeroCopy));
        assert!(slab.get(key).unwrap().poll_zc_first(&mut cx).is_pending());
        check(&mut slab, key, OpKind::ZeroCopy);
        unsafe {
            slab.get(key)
                .unwrap()
                .complete(-libc::EINVAL, IORING_CQE_F_MORE)
        };
        check(&mut slab, key, OpKind::ZeroCopy);
        assert!(slab.get(key).unwrap().poll_zc_first(&mut cx).is_ready());
        check(&mut slab, key, OpKind::ZeroCopy);
        unsafe { slab.get(key).unwrap().complete(0, IORING_CQE_F_NOTIF) };
        check(&mut slab, key, OpKind::ZeroCopy);
        assert!(slab.get(key).unwrap().poll_zc_notif(&mut cx).is_ready());

        // Multishot.
        let key = slab.insert(MaybeFdLifecycle::new(true, OpKind::Multishot));
        unsafe { slab.get(key).unwrap().complete(1, IORING_CQE_F_MORE) };
        check(&mut slab, key, OpKind::Multishot);
    }

    struct Counting(std::rc::Rc<std::cell::Cell<usize>>);

    impl OpAble for Counting {
        const KIND: OpKind = OpKind::Multishot;
        fn uring_op(&mut self) -> io_uring::squeue::Entry {
            io_uring::opcode::Nop::new().build()
        }
        fn discard_cqe(&mut self, _cqe: Cqe) {
            self.0.set(self.0.get() + 1);
        }
    }

    /// Multishot: CQEs queue up, wake the poller, and the op ends with the CQE without F_MORE.
    #[test]
    fn multishot_queue_and_end() {
        let mut slab = Slab::new();
        let key = slab.insert(MaybeFdLifecycle::new(false, OpKind::Multishot));
        let (waker, woken) = flag_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(slab.get(key).unwrap().poll_multi(&mut cx).is_pending());

        let done = unsafe { slab.get(key).unwrap().complete(1, IORING_CQE_F_MORE) };
        assert_eq!(done.queued, Some(1));
        assert!(woken.load(Ordering::SeqCst));
        let done = unsafe { slab.get(key).unwrap().complete(2, IORING_CQE_F_MORE) };
        assert_eq!(done.queued, Some(2));
        unsafe { slab.get(key).unwrap().complete(-libc::ECANCELED, 0) };
        assert!(slab.get(key).unwrap().multi_finished());

        let next = |slab: &mut Slab<MaybeFdLifecycle>, cx: &mut Context<'_>| match slab
            .get(key)
            .unwrap()
            .poll_multi(cx)
        {
            Poll::Ready(Some(cqe)) => cqe.res(),
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(next(&mut slab, &mut cx), 1);
        assert_eq!(next(&mut slab, &mut cx), 2);
        assert_eq!(next(&mut slab, &mut cx), -libc::ECANCELED);
        // The final CQE freed the slot.
        assert!(slab.get(key).is_none());
    }

    /// Dropping a multishot handle discards queued CQEs and every later one, and frees the slot
    /// with the final CQE.
    #[test]
    fn multishot_drop_discards() {
        let mut slab = Slab::new();
        let key = slab.insert(MaybeFdLifecycle::new(false, OpKind::Multishot));
        let count = std::rc::Rc::new(std::cell::Cell::new(0));
        unsafe { slab.get(key).unwrap().complete(1, IORING_CQE_F_MORE) };
        unsafe { slab.get(key).unwrap().complete(2, IORING_CQE_F_MORE) };

        let mut data = Some(Counting(count.clone()));
        assert!(!slab.get(key).unwrap().drop_multi(&mut data));
        assert!(data.is_none());
        assert_eq!(count.get(), 2);

        let done = unsafe { slab.get(key).unwrap().complete(3, IORING_CQE_F_MORE) };
        assert!(done.deferred.is_none());
        assert_eq!(count.get(), 3);
        let done = unsafe { slab.get(key).unwrap().complete(-libc::ECANCELED, 0) };
        assert_eq!(count.get(), 4);
        assert!(slab.get(key).is_none());
        // The data lives until the deferred slot is dropped.
        assert_eq!(std::rc::Rc::strong_count(&count), 2);
        drop(done);
        assert_eq!(std::rc::Rc::strong_count(&count), 1);
    }

    /// A finished multishot op: `finish_multi` discards what was not taken and frees the slot.
    #[test]
    fn multishot_finish() {
        let mut slab = Slab::new();
        let key = slab.insert(MaybeFdLifecycle::new(false, OpKind::Multishot));
        let count = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut data = Counting(count.clone());
        unsafe { slab.get(key).unwrap().complete(1, IORING_CQE_F_MORE) };
        assert!(!slab.get(key).unwrap().finish_multi(&mut data));
        unsafe { slab.get(key).unwrap().complete(0, 0) };
        assert!(slab.get(key).unwrap().finish_multi(&mut data));
        assert_eq!(count.get(), 2);
        assert!(slab.get(key).is_none());
    }
}
