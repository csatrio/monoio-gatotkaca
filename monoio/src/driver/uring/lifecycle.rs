//! Uring state lifecycle.
//! Partly borrow from tokio-uring.
//!
//! Every slab entry tracks one operation. Single and zero-copy ops resolve to one result
//! ([`Lifecycle::Completed`]); multishot ops queue their CQEs ([`MultiState`]). No CQE the kernel
//! posts can make these state machines panic: unexpected ones are dropped (closing an fd they
//! carry) and logged under the `debug` feature.

use std::{
    collections::VecDeque,
    io,
    task::{Context, Poll, Waker},
};

use crate::{
    driver::{
        op::{CompletionMeta, MaybeFd, OpAble, OpKind},
        raw::Cqe,
    },
    utils::slab::Ref,
};

/// Flag indicating more CQEs will follow for this SQE (SEND_ZC, multishot).
const IORING_CQE_F_MORE: u32 = 2;

enum Lifecycle {
    /// The operation has been submitted to uring and is currently in-flight
    Submitted,

    /// The submitter is waiting for the completion of the operation
    Waiting(Waker),

    /// The submitter no longer has interest in the operation result. The state
    /// must be passed to the driver and held until the operation completes.
    Ignored(Box<dyn std::any::Any>),

    /// The operation has completed.
    Completed(io::Result<MaybeFd>, u32),

    /// First CQE with IORING_CQE_F_MORE received, waiting for the final CQE (the SEND_ZC
    /// notification). The future has not been polled yet (was in Submitted state).
    CompletedMore(io::Result<MaybeFd>, u32),

    /// First CQE with IORING_CQE_F_MORE received, waiting for the final CQE.
    /// The future is actively polling (was in Waiting state).
    WaitingMore(Waker, io::Result<MaybeFd>, u32),

    /// The future was dropped but the final CQE is still pending.
    // Holds the buffer alive; never read, only dropped.
    #[allow(dead_code)]
    IgnoredMore(Box<dyn std::any::Any>),

    /// A multishot operation.
    Multi(Box<MultiState>),
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

/// State of a multishot operation.
#[derive(Default)]
struct MultiState {
    waker: Option<Waker>,
    /// CQEs not yet taken by the handle. Unbounded: the kernel keeps posting while the consumer
    /// does not poll. Its allocation is reused for the whole life of the op.
    queue: VecDeque<Cqe>,
    /// The final CQE (without `F_MORE`) is in `queue` or was taken.
    finished: bool,
    /// Set once the handle is dropped: owns the op data and discards every further CQE.
    ignored: Option<Box<dyn Discard>>,
}

pub(crate) struct MaybeFdLifecycle {
    is_fd: bool,
    kind: OpKind,
    lifecycle: Lifecycle,
}

/// What [`Ref::complete`] leaves to the driver.
#[derive(Default)]
pub(crate) struct Completed {
    /// A finished slot holding the data of a dropped op. Drop it once the driver no longer holds
    /// its own state: the drop may call back into the driver (a `SharedFd` closing, ...).
    pub(crate) deferred: Option<MaybeFdLifecycle>,
    /// For a multishot op: the length of its CQE queue after this CQE.
    pub(crate) queued: Option<usize>,
}

impl MaybeFdLifecycle {
    #[inline]
    pub(crate) fn new(is_fd: bool, kind: OpKind) -> Self {
        let lifecycle = match kind {
            OpKind::Multishot => Lifecycle::Multi(Box::default()),
            OpKind::Single | OpKind::ZeroCopy => Lifecycle::Submitted,
        };
        Self {
            is_fd,
            kind,
            lifecycle,
        }
    }
}

impl Ref<'_, MaybeFdLifecycle> {
    /// Handle one CQE of this operation.
    ///
    /// # Safety
    /// `res` must come from the kernel for this operation, since it may be a fd.
    pub(crate) unsafe fn complete(mut self, res: i32, flags: u32) -> Completed {
        let more = flags & IORING_CQE_F_MORE != 0;

        if let Lifecycle::Multi(state) = &mut self.lifecycle {
            let cqe = Cqe::new(res, flags);
            if let Some(ignored) = state.ignored.as_mut() {
                ignored.discard(cqe);
                if more {
                    return Completed {
                        deferred: None,
                        queued: Some(0),
                    };
                }
                return Completed {
                    deferred: Some(self.remove()),
                    queued: Some(0),
                };
            }
            state.queue.push_back(cqe);
            state.finished |= !more;
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
            return Completed {
                deferred: None,
                queued: Some(state.queue.len()),
            };
        }

        let result = if res >= 0 {
            MaybeFd::new_result(Ok(res as u32), self.is_fd)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        };
        if more && self.kind == OpKind::Single {
            trace!(
                "MONOIO DEBUG[lifecycle]: F_MORE on a single-shot op, flags {}",
                flags
            );
        }

        // A first CQE with F_MORE is kept until the final CQE (without F_MORE: the SEND_ZC
        // notification, or the last CQE of an unexpected multishot); results of the CQEs in
        // between are dropped, which closes a fd they carry.
        let lifecycle = &mut self.lifecycle;
        let mut wake = None;
        let next = match std::mem::replace(lifecycle, Lifecycle::Submitted) {
            Lifecycle::Submitted if more => Some(Lifecycle::CompletedMore(result, flags)),
            Lifecycle::Submitted => Some(Lifecycle::Completed(result, flags)),
            Lifecycle::Waiting(waker) if more => {
                // Don't wake yet: the final CQE is still to come.
                Some(Lifecycle::WaitingMore(waker, result, flags))
            }
            Lifecycle::Waiting(waker) => {
                wake = Some(waker);
                Some(Lifecycle::Completed(result, flags))
            }
            Lifecycle::Ignored(data) if more => Some(Lifecycle::IgnoredMore(data)),
            Lifecycle::CompletedMore(stored, stored_flags) if !more => Some(Lifecycle::Completed(
                stored,
                stored_flags & !IORING_CQE_F_MORE,
            )),
            Lifecycle::WaitingMore(waker, stored, stored_flags) if !more => {
                wake = Some(waker);
                Some(Lifecycle::Completed(
                    stored,
                    stored_flags & !IORING_CQE_F_MORE,
                ))
            }
            Lifecycle::Ignored(data) | Lifecycle::IgnoredMore(data) if !more => {
                // Put it back so that `remove` hands it out to be dropped later.
                *lifecycle = Lifecycle::Ignored(data);
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
                    waker.wake();
                }
                Completed::default()
            }
            None => Completed {
                deferred: Some(self.remove()),
                queued: None,
            },
        }
    }

    #[allow(clippy::needless_pass_by_ref_mut)]
    pub(crate) fn poll_op(mut self, cx: &mut Context<'_>) -> Poll<CompletionMeta> {
        let ref_mut = &mut self.lifecycle;
        match ref_mut {
            Lifecycle::Submitted => {
                *ref_mut = Lifecycle::Waiting(cx.waker().clone());
                return Poll::Pending;
            }
            Lifecycle::Waiting(waker) | Lifecycle::WaitingMore(waker, _, _) => {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
                return Poll::Pending;
            }
            // Multi-CQE: first CQE arrived but still waiting for notification
            Lifecycle::CompletedMore(_, _) => {
                if let Lifecycle::CompletedMore(result, flags) =
                    std::mem::replace(ref_mut, Lifecycle::Submitted)
                {
                    *ref_mut = Lifecycle::WaitingMore(cx.waker().clone(), result, flags);
                }
                return Poll::Pending;
            }
            Lifecycle::Completed(..) => {}
            // Not reachable through `Op` (dropped ops are not polled, multishot ops are not
            // `Op`s).
            Lifecycle::Ignored(_) | Lifecycle::IgnoredMore(_) | Lifecycle::Multi(_) => {
                return Poll::Pending
            }
        }

        match self.remove().lifecycle {
            Lifecycle::Completed(result, flags) => Poll::Ready(CompletionMeta { result, flags }),
            _ => Poll::Pending,
        }
    }

    // return if the op must has been finished
    pub(crate) fn drop_op<T: 'static>(mut self, data: &mut Option<T>) -> bool {
        let boxed = |data: &mut Option<T>| -> Box<dyn std::any::Any> {
            match data.take() {
                Some(data) => Box::new(data),
                // () is a ZST, so it does not allocate
                None => Box::new(()),
            }
        };
        let ref_mut = &mut self.lifecycle;
        match ref_mut {
            Lifecycle::Submitted | Lifecycle::Waiting(_) => {
                *ref_mut = Lifecycle::Ignored(boxed(data));
                false
            }
            // Multi-CQE: still waiting for the final CQE, must keep the slot alive. Dropping the
            // stored result closes a fd it carries.
            Lifecycle::CompletedMore(_, _) | Lifecycle::WaitingMore(_, _, _) => {
                *ref_mut = Lifecycle::IgnoredMore(boxed(data));
                false
            }
            Lifecycle::Completed(..) => {
                self.remove();
                true
            }
            // Already dropped, or not an `Op`: nothing to do.
            Lifecycle::Ignored(..) | Lifecycle::IgnoredMore(..) | Lifecycle::Multi(_) => true,
        }
    }

    /// Next CQE of a multishot op. `Ready(None)` once the final CQE was taken. Taking the final
    /// CQE frees the slot.
    pub(crate) fn poll_multi(mut self, cx: &mut Context<'_>) -> Poll<Option<Cqe>> {
        let Lifecycle::Multi(state) = &mut self.lifecycle else {
            return Poll::Ready(None);
        };
        if let Some(cqe) = state.queue.pop_front() {
            if !cqe.more() {
                self.remove();
            }
            return Poll::Ready(Some(cqe));
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
        let Lifecycle::Multi(state) = &mut self.lifecycle else {
            return true;
        };
        state.waker = None;
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
        let Lifecycle::Multi(state) = &mut self.lifecycle else {
            return true;
        };
        if !state.finished {
            return false;
        }
        state.queue.drain(..).for_each(|cqe| data.discard_cqe(cqe));
        self.remove();
        true
    }

    /// Whether the kernel posted the final CQE of a multishot op.
    pub(crate) fn multi_finished(&self) -> bool {
        match &self.lifecycle {
            Lifecycle::Multi(state) => state.finished,
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
    use crate::utils::slab::Slab;

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
        let finished = r.drop_op(&mut data);
        assert!(!finished, "op not finished yet, notification pending");

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
        let finished = r.drop_op(&mut data);
        assert!(!finished, "op not finished yet, notification pending");

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
        assert!(!slab.get(key).unwrap().drop_op(&mut data));
        assert!(data.is_none());
        let done = unsafe { slab.get(key).unwrap().complete(0, 0) };
        assert!(done.deferred.is_some());
        assert!(slab.get(key).is_none());
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
