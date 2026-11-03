use std::{
    cell::UnsafeCell,
    io,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    rc::Rc,
};

// Tracks in-flight operations on a file descriptor. Ensures all in-flight
// operations complete before submitting the close.
#[derive(Clone, Debug)]
pub(crate) struct SharedFd {
    inner: Rc<Inner>,
}

struct Inner {
    // Open file descriptor
    fd: RawFd,

    // Waker to notify when the close operation completes.
    state: UnsafeCell<State>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner").field("fd", &self.fd).finish()
    }
}

enum State {
    /// Initial state
    Init,

    /// Waiting for all in-flight operation to complete.
    Waiting(Option<std::task::Waker>),

    /// The FD is closing
    Closing(super::op::Op<super::op::close::Close>),

    /// The FD is fully closed
    Closed,
}

impl AsRawFd for SharedFd {
    fn as_raw_fd(&self) -> RawFd {
        self.raw_fd()
    }
}

impl SharedFd {
    pub(crate) fn new(fd: RawFd) -> io::Result<SharedFd> {
        Ok(Self::new_without_register(fd))
    }

    pub(crate) fn new_without_register(fd: RawFd) -> SharedFd {
        SharedFd {
            inner: Rc::new(Inner {
                fd,
                state: UnsafeCell::new(State::Init),
            }),
        }
    }

    /// Returns the RawFd
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.inner.fd
    }

    /// Try unwrap Rc and return rawfd.
    /// Note: this action will consume self and return rawfd without closing it.
    pub(crate) fn try_unwrap(self) -> Result<RawFd, Self> {
        use std::mem::ManuallyDrop;

        let fd = self.inner.fd;
        match Rc::try_unwrap(self.inner) {
            Ok(inner) => {
                // Only drop Inner's state, skip its drop impl.
                let inner = ManuallyDrop::new(inner);
                drop(unsafe { std::ptr::read(&inner.state) });
                Ok(fd)
            }
            Err(inner) => Err(Self { inner }),
        }
    }

    /// An FD cannot be closed until all in-flight operation have completed.
    /// This prevents bugs where in-flight reads could operate on the incorrect
    /// file descriptor.
    pub(crate) async fn close(self) {
        let fd = self.inner.fd;
        let mut this = self;
        let state = unsafe { &mut *this.inner.state.get() };
        if Rc::get_mut(&mut this.inner).is_some() {
            *state = match super::op::Op::close(fd) {
                Ok(op) => State::Closing(op),
                Err(_) => {
                    let _ = unsafe { std::fs::File::from_raw_fd(fd) };
                    return;
                }
            };
        }
        this.inner.closed().await;
    }
}

impl Inner {
    /// Completes when the FD has been closed.
    async fn closed(&self) {
        use std::task::Poll;

        crate::macros::support::poll_fn(|cx| {
            let state = unsafe { &mut *self.state.get() };

            use std::{future::Future, pin::Pin};

            match state {
                State::Init => {
                    *state = State::Waiting(Some(cx.waker().clone()));
                    Poll::Pending
                }
                State::Waiting(Some(waker)) => {
                    if !waker.will_wake(cx.waker()) {
                        waker.clone_from(cx.waker());
                    }

                    Poll::Pending
                }
                State::Waiting(None) => {
                    *state = State::Waiting(Some(cx.waker().clone()));
                    Poll::Pending
                }
                State::Closing(op) => {
                    // Nothing to do if the close operation failed.
                    let _ = ready!(Pin::new(op).poll(cx));
                    *state = State::Closed;
                    Poll::Ready(())
                }
                State::Closed => Poll::Ready(()),
            }
        })
        .await;
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let fd = self.fd;
        let state = unsafe { &mut *self.state.get() };
        if let State::Init | State::Waiting(..) = state {
            if super::op::Op::close(fd).is_err() {
                let _ = unsafe { std::fs::File::from_raw_fd(fd) };
            }
        }
    }
}
