//! A channel from any thread into a runtime thread, woken through an eventfd read by the ring.

use std::{
    collections::VecDeque,
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::{Arc, Mutex, MutexGuard},
};

use super::super::raw::{self, opcode, types, RawOp};

struct State<T> {
    queue: VecDeque<T>,
    senders: usize,
    receiver_alive: bool,
    // The receiver found the queue empty and waits for an eventfd write.
    armed: bool,
    // eventfd writes made so far.
    wakeups: u64,
}

struct Shared<T> {
    state: Mutex<State<T>>,
    // Blocking (no EFD_NONBLOCK): io_uring would answer EAGAIN on a non-blocking eventfd
    // instead of waiting. A write blocks only when the counter would overflow, which 1 per wait
    // never reaches.
    efd: OwnedFd,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        // A panicking sender cannot leave the queue inconsistent: keep going.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn notify(&self) {
        let one = 1u64;
        // SAFETY: writes 8 bytes from a live u64 to our eventfd. It cannot fail in practice
        // (see `efd`), and a failure would only delay a wakeup.
        unsafe {
            libc::write(
                self.efd.as_raw_fd(),
                &one as *const u64 as *const libc::c_void,
                8,
            )
        };
    }
}

/// Create a channel from any thread to a runtime thread.
///
/// [`EventSender`] is `Send + Clone` and works from any thread, runtime or not, without the
/// `sync` feature. [`EventReceiver`] stays on a runtime thread and waits through the ring: an
/// `opcode::Read` of an eventfd. The senders write the eventfd only when the receiver is waiting
/// on an empty queue, so a burst of messages costs one `write(2)` and one completion, not one per
/// message. Messages from one sender arrive in order.
///
/// The queue is unbounded and protected by a `std::sync::Mutex`.
pub fn event_channel<T: Send + 'static>() -> io::Result<(EventSender<T>, EventReceiver<T>)> {
    let fd = crate::syscall!(eventfd@RAW(0, libc::EFD_CLOEXEC))?;
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            senders: 1,
            receiver_alive: true,
            armed: false,
            wakeups: 0,
        }),
        // SAFETY: `fd` is a fresh fd we own.
        efd: unsafe { OwnedFd::from_raw_fd(fd) },
    });
    let receiver = EventReceiver {
        shared: shared.clone(),
        local: VecDeque::new(),
        read: None,
        buf: Some(Box::new([0; 8])),
    };
    Ok((EventSender { shared }, receiver))
}

/// The sending half of an [`event_channel`]: `Send + Sync + Clone`, usable from any thread.
pub struct EventSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> EventSender<T> {
    /// Queue `value` and wake the receiver if it waits. Gives `value` back if the receiver was
    /// dropped.
    pub fn send(&self, value: T) -> Result<(), T> {
        let mut state = self.shared.lock();
        if !state.receiver_alive {
            return Err(value);
        }
        state.queue.push_back(value);
        let wake = std::mem::take(&mut state.armed);
        if wake {
            state.wakeups += 1;
        }
        drop(state);
        if wake {
            self.shared.notify();
        }
        Ok(())
    }

    /// Whether the receiver was dropped.
    pub fn is_closed(&self) -> bool {
        !self.shared.lock().receiver_alive
    }
}

impl<T> Clone for EventSender<T> {
    fn clone(&self) -> Self {
        self.shared.lock().senders += 1;
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for EventSender<T> {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.senders -= 1;
        // The last sender wakes a waiting receiver, so it can return `None`.
        let wake = state.senders == 0 && std::mem::take(&mut state.armed);
        if wake {
            state.wakeups += 1;
        }
        drop(state);
        if wake {
            self.shared.notify();
        }
    }
}

impl<T> std::fmt::Debug for EventSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventSender").finish_non_exhaustive()
    }
}

type EventRead<T> = RawOp<(Box<[u8; 8]>, Arc<Shared<T>>)>;

/// The receiving half of an [`event_channel`]. `!Send`: it lives on a runtime thread.
pub struct EventReceiver<T: 'static> {
    shared: Arc<Shared<T>>,
    // Messages taken from the shared queue in one swap, handed out one by one.
    local: VecDeque<T>,
    // The armed eventfd read. Kept across dropped `recv` futures, so a wakeup is never lost.
    read: Option<EventRead<T>>,
    buf: Option<Box<[u8; 8]>>,
}

impl<T: Send + 'static> EventReceiver<T> {
    /// The next message, waiting for one if the queue is empty. `None` once every sender was
    /// dropped and the queue is empty.
    ///
    /// Cancel safe: dropping the future loses no message, and the eventfd read stays armed for
    /// the next call.
    ///
    /// # Panics
    /// Panics if called outside of a monoio runtime, or if the eventfd read fails (it cannot,
    /// short of fd exhaustion of the ring).
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            match self.take() {
                Ok(next) => return next,
                Err(()) => self.wait().await,
            }
        }
    }

    /// The next message if one is queued, without waiting. `None` also when the queue is empty
    /// but senders are left.
    pub fn try_recv(&mut self) -> Option<T> {
        self.take().ok().flatten()
    }

    /// `Ok(Some)` a message, `Ok(None)` closed, `Err` empty (and armed for a wakeup).
    fn take(&mut self) -> Result<Option<T>, ()> {
        if let Some(value) = self.local.pop_front() {
            return Ok(Some(value));
        }
        let mut state = self.shared.lock();
        if !state.queue.is_empty() {
            // `local` is empty: swap the buffers, keeping both allocations.
            std::mem::swap(&mut state.queue, &mut self.local);
            state.armed = false;
            drop(state);
            return Ok(self.local.pop_front());
        }
        if state.senders == 0 {
            return Ok(None);
        }
        state.armed = true;
        Err(())
    }

    async fn wait(&mut self) {
        if self.read.is_none() {
            let buf = self.buf.take().unwrap_or_else(|| Box::new([0; 8]));
            let mut data = (buf, self.shared.clone());
            let sqe = opcode::Read::new(
                types::Fd(self.shared.efd.as_raw_fd()),
                data.0.as_mut_ptr(),
                8,
            )
            .build();
            // SAFETY: the SQE points into the boxed buffer owned by `data`, which also keeps the
            // eventfd open; a read is single-shot and returns no fd.
            let op = unsafe { raw::submit(sqe, data) }.expect("event_channel: submit read");
            self.read = Some(op);
        }
        let op = self.read.as_mut().expect("armed read");
        let (cqe, (buf, _)) = op.await;
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
    }

    /// Number of eventfd writes the senders made so far: one per wait that a message (or the
    /// last sender's drop) ended. A measure of how well messages are batched.
    pub fn wakeups(&self) -> u64 {
        self.shared.lock().wakeups
    }

    /// Whether every sender was dropped (messages may still be queued).
    pub fn is_closed(&self) -> bool {
        self.shared.lock().senders == 0
    }
}

impl<T: 'static> Drop for EventReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.receiver_alive = false;
        state.armed = false;
        drop(state);
        // Complete the armed read now, rather than leaving it in the ring (or cancelling it).
        if self.read.is_some() {
            self.shared.notify();
        }
    }
}

impl<T: 'static> std::fmt::Debug for EventReceiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventReceiver")
            .field("buffered", &self.local.len())
            .field("armed", &self.read.is_some())
            .finish()
    }
}
