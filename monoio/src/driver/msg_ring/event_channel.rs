//! A channel from any thread into a runtime thread, without locks on the message path.
//!
//! - Messages go through an `LfQueue` (`driver::lf_queue`): one CAS to take a node, one CAS to
//!   push it; the receiver takes a whole batch with one swap and returns its nodes with one CAS.
//! - Wakeups: the receiver sets `armed` before it sleeps. A sender that sees it clears it and
//!   rings once: through `MSG_RING` from a ring thread (see `driver::doorbell`), with an
//!   eventfd write otherwise. A burst of messages costs one wakeup.

use std::{
    fmt,
    future::{poll_fn, Future},
    io,
    marker::PhantomData,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::{
        atomic::{fence, AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};

use crossbeam_utils::CachePadded;

use super::super::{
    doorbell::{self, BellAddr, Doorbell},
    lf_queue::{Consumer, LfQueue},
    raw::{self, opcode, types, RawOp},
};

/// Create a channel from any thread to a runtime thread, with the default capacity (1024
/// slots, growing to 131,072). See [`EventChannelBuilder`].
pub fn event_channel<T: Send + 'static>() -> io::Result<(EventSender<T>, EventReceiver<T>)> {
    EventChannelBuilder::new().build()
}

/// Settings of an [`event_channel`].
///
/// Message slots live in mmap'd segments that double in size (`initial_capacity`,
/// `2 × initial_capacity`, ...) until `max_capacity` slots exist; then [`EventSender::send`]
/// fails with [`SendError::Full`] until the receiver catches up. Pages are faulted in only
/// when slots are first used, and stay until the channel is gone.
#[derive(Debug, Clone, Copy)]
pub struct EventChannelBuilder {
    initial: u32,
    max: u32,
}

impl Default for EventChannelBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EventChannelBuilder {
    /// Default number of message slots mapped up front.
    pub const DEFAULT_INITIAL_CAPACITY: u32 = 1024;
    /// Default limit on queued messages: room for one in-flight message per connection at
    /// 100k connections, with headroom.
    pub const DEFAULT_MAX_CAPACITY: u32 = 131_072;

    /// The defaults: [`DEFAULT_INITIAL_CAPACITY`](Self::DEFAULT_INITIAL_CAPACITY) and
    /// [`DEFAULT_MAX_CAPACITY`](Self::DEFAULT_MAX_CAPACITY).
    pub fn new() -> Self {
        Self {
            initial: Self::DEFAULT_INITIAL_CAPACITY,
            max: Self::DEFAULT_MAX_CAPACITY,
        }
    }

    /// Slots in the first segment. Rounded up to a power of two.
    pub fn initial_capacity(mut self, slots: u32) -> Self {
        self.initial = slots;
        self
    }

    /// The most messages that can be queued at once (at most 2^31).
    pub fn max_capacity(mut self, slots: u32) -> Self {
        self.max = slots;
        self
    }

    /// Create the channel. Fails with `InvalidInput` for a zero, too large or inverted
    /// capacity, and with the OS error if the first segment or the eventfd can't be created.
    pub fn build<T: Send + 'static>(self) -> io::Result<(EventSender<T>, EventReceiver<T>)> {
        let queue = LfQueue::new(self.initial, self.max)
            .map_err(|e| io::Error::new(e.kind(), format!("event_channel: {e}")))?;
        let fd = crate::syscall!(eventfd@RAW(0, libc::EFD_CLOEXEC))?;
        let shared = Arc::new(Shared {
            queue,
            armed: CachePadded::new(AtomicBool::new(false)),
            senders: AtomicUsize::new(1),
            receiver_alive: AtomicBool::new(true),
            // SAFETY: `fd` is a fresh fd we own.
            efd: Arc::new(unsafe { OwnedFd::from_raw_fd(fd) }),
            bell: Mutex::new(None),
            counters: Counters::default(),
        });
        let receiver = EventReceiver {
            shared: shared.clone(),
            consumer: Consumer::fifo(),
            read: None,
            buf: Some(Box::new([0; 8])),
            bell: None,
            _not_send: PhantomData,
        };
        Ok((EventSender { shared }, receiver))
    }
}

#[derive(Default)]
struct Counters {
    eventfd_wakeups: AtomicU64,
    ring_wakeups: AtomicU64,
    full: AtomicU64,
}

struct Shared<T> {
    queue: LfQueue<T>,
    /// The receiver found the inbox empty and waits for a wakeup.
    armed: CachePadded<AtomicBool>,
    senders: AtomicUsize,
    receiver_alive: AtomicBool,
    /// Blocking (no EFD_NONBLOCK): io_uring would answer EAGAIN on a non-blocking eventfd
    /// instead of waiting. A write blocks only when the counter would overflow, which 1 per
    /// wait never reaches.
    efd: Arc<OwnedFd>,
    /// The receiver's bell, set once it first waited on a ring. Locked only to wake and to
    /// (re)bind, never per message.
    bell: Mutex<Option<BellAddr>>,
    counters: Counters,
}

// SAFETY: messages (`T: Send`) move between threads through the atomics; nothing else in
// `Shared` is thread-bound.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> Shared<T> {
    /// Wake the receiver if it is waiting. Call after the change it must see (a message
    /// pushed, the last sender gone), which the fence orders before the `armed` check.
    fn wake_if_armed(&self) {
        // Pairs with the fence in `EventReceiver::take`: either we see `armed`, or the
        // receiver sees our change.
        fence(Ordering::SeqCst);
        if !self.armed.load(Ordering::Relaxed) || !self.armed.swap(false, Ordering::Acquire) {
            return;
        }
        let bell = self.bell.lock().unwrap_or_else(|e| e.into_inner()).clone();
        match bell {
            Some(addr) if doorbell::ring_from_current(&addr, &self.efd) => {
                self.counters.ring_wakeups.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                doorbell::write_eventfd(&self.efd);
                self.counters
                    .eventfd_wakeups
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Why [`EventSender::send`] failed. Holds the message.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum SendError<T> {
    /// `max_capacity` messages are queued; try again once the receiver caught up.
    Full(T),
    /// The receiver was dropped.
    Disconnected(T),
}

impl<T> SendError<T> {
    /// The message that wasn't sent.
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(v) | Self::Disconnected(v) => v,
        }
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("Full(..)"),
            Self::Disconnected(_) => f.write_str("Disconnected(..)"),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("event channel is full"),
            Self::Disconnected(_) => f.write_str("event channel receiver was dropped"),
        }
    }
}

impl<T> std::error::Error for SendError<T> {}

/// The sending half of an [`event_channel`]: `Send + Sync + Clone`, usable from any thread.
pub struct EventSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T: Send> EventSender<T> {
    /// Queue `value` and wake the receiver if it waits. Never blocks and takes no lock (a
    /// sender that must map a new segment takes the arena's growth lock).
    ///
    /// From a ring thread the wakeup is a `MSG_RING` submitted with that ring's next tick;
    /// from any other thread it is an eventfd write.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let shared = &*self.shared;
        if !shared.receiver_alive.load(Ordering::Acquire) {
            return Err(SendError::Disconnected(value));
        }
        if let Err(value) = shared.queue.push(value) {
            shared.counters.full.fetch_add(1, Ordering::Relaxed);
            return Err(SendError::Full(value));
        }
        shared.wake_if_armed();
        Ok(())
    }

    /// Whether the receiver was dropped.
    pub fn is_closed(&self) -> bool {
        !self.shared.receiver_alive.load(Ordering::Acquire)
    }
}

impl<T> Clone for EventSender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for EventSender<T> {
    fn drop(&mut self) {
        // The last sender wakes a waiting receiver, so it can return `None`.
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.wake_if_armed();
        }
    }
}

impl<T> fmt::Debug for EventSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSender").finish_non_exhaustive()
    }
}

type EventRead = RawOp<(Box<[u8; 8]>, Arc<OwnedFd>)>;

/// Counters of an [`event_channel`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventChannelStats {
    /// Wakeups by eventfd write (senders off a ring thread, or a failed `MSG_RING`).
    pub eventfd_wakeups: u64,
    /// Wakeups by `MSG_RING` from a ring thread, or rung directly on the receiver's ring.
    pub ring_wakeups: u64,
    /// Sends that failed with [`SendError::Full`].
    pub full: u64,
    /// Message slots currently mapped.
    pub capacity: u32,
}

/// The receiving half of an [`event_channel`]. `!Send`: it lives on a runtime thread.
pub struct EventReceiver<T: Send + 'static> {
    shared: Arc<Shared<T>>,
    /// The batch being handed out, in send order.
    consumer: Consumer<T>,
    /// The armed eventfd read. Kept across dropped `recv` futures, so a wakeup is never lost.
    read: Option<EventRead>,
    buf: Option<Box<[u8; 8]>>,
    bell: Option<Doorbell>,
    _not_send: PhantomData<*mut ()>,
}

impl<T: Send + 'static> EventReceiver<T> {
    /// The next message, waiting for one if the queue is empty. `None` once every sender was
    /// dropped and the queue is empty.
    ///
    /// Cancel safe: dropping the future loses no message, and the wakeup stays armed for the
    /// next call.
    ///
    /// # Panics
    /// Panics if called outside of a monoio runtime, or if the eventfd read fails (it cannot,
    /// short of fd exhaustion of the ring).
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            match self.take(true) {
                Ok(next) => return next,
                Err(()) => self.wait().await,
            }
        }
    }

    /// The next message if one is queued, without waiting. `None` also when the queue is empty
    /// but senders are left.
    pub fn try_recv(&mut self) -> Option<T> {
        self.take(false).ok().flatten()
    }

    /// `Ok(Some)` a message, `Ok(None)` closed, `Err` empty (armed for a wakeup if `arm`).
    fn take(&mut self, arm: bool) -> Result<Option<T>, ()> {
        let shared = &*self.shared;
        loop {
            if let Some(value) = self.consumer.pop(&shared.queue) {
                return Ok(Some(value));
            }
            if !arm {
                if shared.senders.load(Ordering::Acquire) != 0 {
                    return Err(());
                }
                // The last sender may have pushed just before it left: look again.
                if shared.queue.is_empty() {
                    return Ok(None);
                }
                continue;
            }

            shared.armed.store(true, Ordering::Relaxed);
            // Pairs with the fence in `wake_if_armed`: either the sender sees `armed`, or we
            // see its message (or its drop) below.
            fence(Ordering::SeqCst);
            let closed = shared.senders.load(Ordering::Relaxed) == 0;
            if !shared.queue.is_empty() || closed {
                // A sender may still claim `armed` and wake us: a spurious wakeup at most.
                shared.armed.store(false, Ordering::Relaxed);
                if closed && shared.queue.is_empty() {
                    return Ok(None);
                }
                continue;
            }
            return Err(());
        }
    }

    /// Bind the bell to the current ring (again, if the receiver moved to another runtime).
    fn bind_bell(&mut self) {
        if self.bell.as_ref().is_some_and(Doorbell::is_current) {
            return;
        }
        self.bell = None;
        match Doorbell::register() {
            Ok(bell) => {
                *self.shared.bell.lock().unwrap_or_else(|e| e.into_inner()) =
                    Some(bell.addr().clone());
                self.bell = Some(bell);
            }
            // Without a ring handle, senders fall back to the eventfd.
            Err(_) => *self.shared.bell.lock().unwrap_or_else(|e| e.into_inner()) = None,
        }
    }

    async fn wait(&mut self) {
        self.bind_bell();
        if self.read.is_none() {
            let buf = self.buf.take().unwrap_or_else(|| Box::new([0; 8]));
            let efd = self.shared.efd.clone();
            let mut data = (buf, efd);
            let sqe =
                opcode::Read::new(types::Fd(data.1.as_raw_fd()), data.0.as_mut_ptr(), 8).build();
            // SAFETY: the SQE points into the boxed buffer owned by `data`, which also keeps the
            // eventfd open; a read is single-shot and returns no fd.
            let op = unsafe { raw::submit(sqe, data) }.expect("event_channel: submit read");
            self.read = Some(op);
        }
        poll_fn(|cx| {
            if let Some(bell) = &self.bell {
                if bell.poll(cx).is_ready() {
                    // The eventfd read stays armed for the next wait.
                    return Poll::Ready(());
                }
            }
            let op = self.read.as_mut().expect("armed read");
            let (cqe, (buf, _)) = std::task::ready!(std::pin::Pin::new(op).poll(cx));
            self.read = None;
            self.buf = Some(buf);
            match cqe.res() {
                r if r >= 0 => {}
                r if r == -libc::EINTR || r == -libc::EAGAIN || r == -libc::ECANCELED => {}
                r => panic!(
                    "event_channel: eventfd read failed: {}",
                    io::Error::from_raw_os_error(-r)
                ),
            }
            Poll::Ready(())
        })
        .await
    }

    /// Wakeups so far (eventfd plus ring): one per wait that a message (or the last sender's
    /// drop) ended. A measure of how well messages are batched.
    pub fn wakeups(&self) -> u64 {
        let s = self.stats();
        s.eventfd_wakeups + s.ring_wakeups
    }

    /// Counters of this channel.
    pub fn stats(&self) -> EventChannelStats {
        let c = &self.shared.counters;
        EventChannelStats {
            eventfd_wakeups: c.eventfd_wakeups.load(Ordering::Relaxed),
            ring_wakeups: c.ring_wakeups.load(Ordering::Relaxed),
            full: c.full.load(Ordering::Relaxed),
            capacity: self.shared.queue.capacity(),
        }
    }

    /// Whether every sender was dropped (messages may still be queued).
    pub fn is_closed(&self) -> bool {
        self.shared.senders.load(Ordering::Acquire) == 0
    }
}

impl<T: Send + 'static> Drop for EventReceiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_alive.store(false, Ordering::Release);
        self.shared.armed.store(false, Ordering::Relaxed);
        *self.shared.bell.lock().unwrap_or_else(|e| e.into_inner()) = None;
        // Messages of the current batch are dropped with it; the queue frees the rest once the
        // senders are gone too.
        self.consumer.clear(&self.shared.queue);
        // Complete the armed read now, rather than leaving it in the ring (or cancelling it).
        if self.read.is_some() {
            doorbell::write_eventfd(&self.shared.efd);
        }
    }
}

impl<T: Send + 'static> fmt::Debug for EventReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventReceiver")
            .field("armed", &self.read.is_some())
            .finish_non_exhaustive()
    }
}
