//! Cross-ring and cross-thread messages without the `sync` feature.
//!
//! - [`RingHandle`] + [`msg_ring`] / [`msg_ring_nowait`]: post a completion (`IORING_OP_MSG_RING`
//!   with `IORING_MSG_DATA`, Linux 5.18+) straight into another runtime's ring. The target sees
//!   it as a CQE tagged with [`user_data(id)`](super::raw::user_data): its CQE hook gets `(id,
//!   Cqe { res, flags: 0 })`. No fd, no syscall on the target side, no flume.
//! - [`Mailbox`] / [`msg_receiver`]: the receiving side, so the target does not have to write the
//!   hook itself.
//! - [`event_channel`]: a lock-free channel from any thread (ring or not) to a runtime thread,
//!   woken with `MSG_RING` from ring threads and through an eventfd otherwise.

mod event_channel;

use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    io,
    ops::RangeInclusive,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
};

pub use self::event_channel::{
    event_channel, EventChannelBuilder, EventChannelStats, EventReceiver, EventSender, SendError,
};
use super::raw::{opcode, squeue, types};
use super::{
    raw::{self, Cqe},
    CURRENT,
};

/// A handle to a runtime's ring that other threads can post messages to with [`msg_ring`].
///
/// It holds a duplicate (`F_DUPFD_CLOEXEC`) of the ring fd, shared by all clones and closed when
/// the last clone drops. The duplicate keeps the kernel ring alive, so a handle that outlives its
/// runtime never points to a closed or reused fd number: a message to it still succeeds (or,
/// for a `single_issuer`/`defer_taskrun` target whose thread exited, fails with `EOWNERDEAD`) and
/// lands in a ring nobody reaps. Such messages pile up in the kernel's CQ overflow list (kernel
/// memory, charged to the cgroup) until the last handle drops, so drop the handles of a runtime
/// that has gone away.
///
/// Get one with [`ring_handle`] from inside the target runtime, or with
/// [`RingHandle::new(&driver)`](RingHandle::new) from its driver (with the timer enabled:
/// `runtime.driver().inner()`).
#[derive(Clone, Debug)]
pub struct RingHandle(Arc<OwnedFd>);

impl RingHandle {
    /// Create a handle for the ring behind `ring` (an [`IoUringDriver`](crate::IoUringDriver),
    /// or anything giving an io_uring fd). The fd is duplicated; `ring` may close its own fd
    /// later.
    ///
    /// `ring` must give an io_uring fd: messages to any other file fail with `EBADFD`.
    pub fn new(ring: &impl AsRawFd) -> io::Result<Self> {
        Self::dup(ring.as_raw_fd())
    }

    fn dup(fd: RawFd) -> io::Result<Self> {
        // fcntl on any number is memory safe; it fails with EBADF if `fd` is not open.
        let dup = crate::syscall!(fcntl@RAW(fd, libc::F_DUPFD_CLOEXEC, 0))?;
        // SAFETY: `dup` is a fresh fd we own.
        Ok(Self(Arc::new(unsafe { OwnedFd::from_raw_fd(dup) })))
    }
}

impl AsRawFd for RingHandle {
    /// The duplicated ring fd, valid while this handle (or a clone) lives.
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

/// A [`RingHandle`] for the current runtime's ring, to hand to other threads.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub fn ring_handle() -> io::Result<RingHandle> {
    let fd = CURRENT.with(|inner| inner.with_ring(|ring| ring.as_raw_fd()));
    RingHandle::dup(fd)
}

fn msg_sqe(target: &RingHandle, id: u64, res: i32) -> squeue::Entry {
    opcode::MsgRingData::new(types::Fd(target.as_raw_fd()), res, raw::user_data(id), None).build()
}

/// Post a completion `(id, res)` to the ring of `target` (`IORING_OP_MSG_RING`, Linux 5.18+).
///
/// The target runtime receives it like a completion of an SQE tagged with
/// [`user_data(id)`](raw::user_data): its CQE hook ([`set_cqe_hook`](raw::set_cqe_hook),
/// [`Mailbox`], [`msg_receiver`]) is called with `id` and a [`Cqe`] whose `res()` is `res` and
/// whose `flags()` are 0. The target wakes up if it is parked. Messages from one sender arrive in
/// order. Sending to the current runtime's own ring works too.
///
/// The SQE is queued on the current runtime's ring, submitted on its next tick, and the future
/// resolves with the source-side completion: `Ok(())` once the message was posted. Errors:
/// `EBADFD` (not an io_uring fd), `EOVERFLOW` (the target CQ is full and the kernel could not
/// queue the message), `EOWNERDEAD` (a `single_issuer`/`defer_taskrun` target whose thread
/// exited), `EINVAL`/`EOPNOTSUPP` (kernel without `MSG_RING`). See [`msg_ring_nowait`] to skip
/// the source-side completion.
///
/// # Panics
/// Panics if `id` is greater than [`MAX_ID`](raw::MAX_ID), or if called outside of a monoio
/// runtime.
pub fn msg_ring(
    target: &RingHandle,
    id: u64,
    res: i32,
) -> impl Future<Output = io::Result<()>> + 'static {
    let sqe = msg_sqe(target, id, res);
    // SAFETY: the SQE points to no memory; the fd it names stays open while the op holds the
    // handle clone. MSG_RING is single-shot and returns no fd.
    let op = unsafe { raw::submit(sqe, target.clone()) };
    async move {
        let (cqe, _target) = op?.await;
        cqe.result().map(drop)
    }
}

// `user_data` of `msg_ring_nowait` SQEs: top bits 0b11, so the driver ignores their (error-only)
// CQEs. Distinct from the driver's own `u64::MAX`, `u64::MAX - 1` and `u64::MAX - 2`.
const NOWAIT_USER_DATA: u64 = (0b11 << 62) | 0x4d53_4752; // "MSGR"

thread_local! {
    // Handles of queued `msg_ring_nowait` SQEs, each with the value of `sqes_submitted` after
    // which the kernel has consumed the SQE (so the fd number in it is no longer needed).
    static NOWAIT_KEEPALIVE: RefCell<VecDeque<(u64, RingHandle)>> =
        const { RefCell::new(VecDeque::new()) };
}

/// Like [`msg_ring`], but fire-and-forget: queue the SQE on the current ring (submitted on its
/// next tick) and return.
///
/// The SQE carries `IOSQE_CQE_SKIP_SUCCESS` (5.17+), so a successful message costs no
/// completion on the sending ring. The trade-off: a failed message (see [`msg_ring`] for the
/// errors) is not reported; its error completion is dropped by the driver. Use [`msg_ring`]
/// where a lost message matters.
///
/// The handle is kept alive (a clone) until the kernel has consumed the SQE, so the target's fd
/// number cannot be reused under it. Returns `EBUSY` if the submission queue stays full.
///
/// # Panics
/// Panics if `id` is greater than [`MAX_ID`](raw::MAX_ID), or if called outside of a monoio
/// runtime.
pub fn msg_ring_nowait(target: &RingHandle, id: u64, res: i32) -> io::Result<()> {
    let sqe = msg_sqe(target, id, res)
        .flags(squeue::Flags::SKIP_SUCCESS)
        .user_data(NOWAIT_USER_DATA);
    CURRENT.with(|inner| {
        let submitted = unsafe { (*inner.0.get()).stats() }.sqes_submitted;
        // The driver is not borrowed here: no runtime code runs inside this closure.
        let queued = inner.with_ring(|ring| {
            // SAFETY: the SQE points to no memory; its fd stays open (see the keepalive below).
            if unsafe { ring.submission().push(&sqe) }.is_err() {
                // Full: flush to the kernel ourselves, then retry once.
                ring.submit()?;
                if unsafe { ring.submission().push(&sqe) }.is_err() {
                    return Err(io::Error::from_raw_os_error(libc::EBUSY));
                }
            }
            // Unconsumed SQEs, ours last: the kernel consumes in order.
            Ok(ring.submission().len() as u64)
        })?;
        NOWAIT_KEEPALIVE.with(|keep| {
            let mut keep = keep.borrow_mut();
            // `sqes_submitted` only counts the driver's own enters (and restarts at 0 on
            // `reset_stats` or with a new runtime): it can lag behind, never run ahead, so
            // handles are at worst kept a bit longer.
            while keep.front().is_some_and(|(at, _)| *at <= submitted) {
                keep.pop_front();
            }
            keep.push_back((submitted + queued, target.clone()));
        });
        Ok(())
    })
}

/// A queue of messages for the current runtime, fed from the CQE hook.
///
/// It accepts the ids in its range ([`offer`](Self::offer)) and hands them out with
/// [`recv`](Self::recv) as `(id, res)`. The hook is a single function per runtime, so either let
/// the mailbox own it ([`install`](Self::install), [`msg_receiver`]) or call `offer` from your own
/// hook:
///
/// ```no_run
/// use monoio::uring::{self, Mailbox};
///
/// # async fn f() {
/// let mailbox = Mailbox::new(1000..=1999);
/// let mb = mailbox.clone();
/// uring::set_cqe_hook(move |id, cqe| {
///     if !mb.offer(id, cqe) {
///         // one of my own SQEs
///     }
/// });
/// let (id, res) = mailbox.recv().await;
/// # }
/// ```
///
/// Clones share the queue. It is `!Send`: it lives on the receiving runtime's thread. One task
/// at a time should wait in `recv` (a second waiter replaces the first's waker).
#[derive(Clone)]
pub struct Mailbox(Rc<RefCell<MailboxInner>>);

struct MailboxInner {
    ids: RangeInclusive<u64>,
    queue: VecDeque<(u64, i32)>,
    waker: Option<Waker>,
}

impl Mailbox {
    /// A mailbox accepting the ids in `ids` (within `0..=MAX_ID`).
    pub fn new(ids: RangeInclusive<u64>) -> Self {
        Self(Rc::new(RefCell::new(MailboxInner {
            ids,
            queue: VecDeque::new(),
            waker: None,
        })))
    }

    /// Queue the completion if `id` is in the mailbox's range and wake the waiting task. Returns
    /// whether it was taken.
    pub fn offer(&self, id: u64, cqe: Cqe) -> bool {
        let mut inner = self.0.borrow_mut();
        if !inner.ids.contains(&id) {
            return false;
        }
        inner.queue.push_back((id, cqe.res()));
        if let Some(waker) = inner.waker.take() {
            drop(inner);
            waker.wake();
        }
        true
    }

    /// Make this mailbox the runtime's CQE hook, replacing any previous hook. Completions with
    /// ids outside its range are dropped.
    ///
    /// # Panics
    /// Panics if called outside of a monoio runtime.
    pub fn install(&self) {
        let mb = self.clone();
        raw::set_cqe_hook(move |id, cqe| {
            mb.offer(id, cqe);
        });
    }

    /// The next message, `(id, res)`, waiting until one arrives.
    pub async fn recv(&self) -> (u64, i32) {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Poll for the next message.
    pub fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<(u64, i32)> {
        let mut inner = self.0.borrow_mut();
        match inner.queue.pop_front() {
            Some(msg) => Poll::Ready(msg),
            None => {
                inner.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// The next message if one is queued.
    pub fn try_recv(&self) -> Option<(u64, i32)> {
        self.0.borrow_mut().queue.pop_front()
    }

    /// Number of queued messages.
    pub fn len(&self) -> usize {
        self.0.borrow().queue.len()
    }

    /// Whether no message is queued.
    pub fn is_empty(&self) -> bool {
        self.0.borrow().queue.is_empty()
    }
}

impl std::fmt::Debug for Mailbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.borrow();
        f.debug_struct("Mailbox")
            .field("ids", &inner.ids)
            .field("queued", &inner.queue.len())
            .finish()
    }
}

/// A [`Mailbox`] for every id, installed as the current runtime's CQE hook (replacing any
/// previous hook). Use [`Mailbox::new`] + [`Mailbox::offer`] to share the hook with your own
/// SQEs.
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub fn msg_receiver() -> Mailbox {
    let mailbox = Mailbox::new(0..=raw::MAX_ID);
    mailbox.install();
    mailbox
}
