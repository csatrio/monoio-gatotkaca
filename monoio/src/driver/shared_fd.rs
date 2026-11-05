use std::{
    cell::{OnceCell, UnsafeCell},
    io,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    rc::Rc,
};

use super::fixed_files::{FileTable, FixedSlot};

/// How an SQE names a file: by fd, or by slot of the ring's fixed file table
/// (`IOSQE_FIXED_FILE`, see `driver/fixed_files.rs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FdRef {
    Raw(RawFd),
    Fixed(u32),
}

/// Build an SQE for the file of a [`SharedFd`]: binds `$t` to `types::Fd` or `types::Fixed`
/// (the `io_uring` crate then sets `IOSQE_FIXED_FILE`) and evaluates `$body` with it. Without a
/// fixed slot this is exactly the old `types::Fd(fd.raw_fd())`.
///
/// ```ignore
/// sqe_fd!(self.fd, |fd| opcode::Read::new(fd, ptr, len).build())
/// ```
macro_rules! sqe_fd {
    ($shared:expr, |$t:ident| $body:expr) => {
        match $crate::driver::shared_fd::SharedFd::fd_ref(&$shared) {
            $crate::driver::shared_fd::FdRef::Raw(raw) => {
                let $t = ::io_uring::types::Fd(raw);
                $body
            }
            $crate::driver::shared_fd::FdRef::Fixed(slot) => {
                let $t = ::io_uring::types::Fixed(slot);
                $body
            }
        }
    };
}
pub(crate) use sqe_fd;

// Tracks in-flight operations on a file descriptor. Ensures all in-flight
// operations complete before submitting the close.
#[derive(Clone, Debug)]
pub(crate) struct SharedFd {
    inner: Rc<Inner>,
}

struct Inner {
    // Open file descriptor, `-1` for a direct-only file (it lives only in the fixed table).
    fd: RawFd,

    // The fixed table slot, if registered. Set once; ops built after that use it.
    fixed: OnceCell<FixedSlot>,

    // Waker to notify when the close operation completes.
    state: UnsafeCell<State>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("fd", &self.fd)
            .field("fixed", &self.fixed.get().map(|f| f.slot))
            .finish()
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

/// The error of raw-fd APIs on a direct-only file.
pub(crate) fn direct_only_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "direct-only file (io_uring fixed file table): it has no file descriptor",
    )
}

impl SharedFd {
    pub(crate) fn new(fd: RawFd) -> io::Result<SharedFd> {
        Ok(Self::new_without_register(fd))
    }

    pub(crate) fn new_without_register(fd: RawFd) -> SharedFd {
        Self::with_parts(fd, OnceCell::new())
    }

    /// A direct-only file: it exists only in the fixed table slot, there is no fd.
    pub(crate) fn new_direct(fixed: FixedSlot) -> SharedFd {
        Self::with_parts(-1, OnceCell::from(fixed))
    }

    fn with_parts(fd: RawFd, fixed: OnceCell<FixedSlot>) -> SharedFd {
        SharedFd {
            inner: Rc::new(Inner {
                fd,
                fixed,
                state: UnsafeCell::new(State::Init),
            }),
        }
    }

    /// Returns the RawFd (`-1` for a direct-only file).
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.inner.fd
    }

    /// How SQEs name this file. See [`sqe_fd!`].
    #[inline]
    pub(crate) fn fd_ref(&self) -> FdRef {
        match self.inner.fixed.get() {
            Some(f) => FdRef::Fixed(f.slot),
            None => FdRef::Raw(self.inner.fd),
        }
    }

    /// The fixed table slot, if any.
    pub(crate) fn fixed_slot(&self) -> Option<u32> {
        self.inner.fixed.get().map(|f| f.slot)
    }

    /// Whether the file has no fd (it lives only in the fixed table).
    pub(crate) fn is_direct(&self) -> bool {
        self.inner.fd < 0 && self.inner.fixed.get().is_some()
    }

    /// Put the fd into a free slot of the current ring's fixed file table; ops built from now
    /// on use the slot. The fd stays open. A no-op if the file already has a slot.
    pub(crate) fn register_fixed(&self) -> io::Result<()> {
        if self.inner.fixed.get().is_some() {
            return Ok(());
        }
        let table = FileTable::current_or_err()?;
        let slot = table.alloc()?;
        if let Err(e) = table.update_sync(slot, self.inner.fd) {
            table.release(slot);
            return Err(e);
        }
        let _ = self.inner.fixed.set(FixedSlot { table, slot });
        Ok(())
    }

    /// Try unwrap Rc and return rawfd.
    /// Note: this action will consume self and return rawfd without closing it.
    ///
    /// A registered file leaves the fixed table first. A direct-only file has no fd: `Err`.
    pub(crate) fn try_unwrap(self) -> Result<RawFd, Self> {
        use std::mem::ManuallyDrop;

        if self.is_direct() {
            return Err(self);
        }
        let fd = self.inner.fd;
        match Rc::try_unwrap(self.inner) {
            Ok(mut inner) => {
                if let Some(fixed) = inner.fixed.take() {
                    // No op uses the slot any more (we hold the only reference).
                    fixed.table.clear_and_release(fixed.slot);
                }
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
        let mut this = self;
        if let Some(inner) = Rc::get_mut(&mut this.inner) {
            let op = inner.start_close();
            *inner.state.get_mut() = match op {
                Some(op) => State::Closing(op),
                None => State::Closed,
            };
        }
        this.inner.closed().await;
    }
}

impl Inner {
    /// Push the close ops; returns the one to wait for (`None`: already closed synchronously).
    ///
    /// Only called once no op uses the file any more (sole owner, or drop).
    fn start_close(&mut self) -> Option<super::op::Op<super::op::close::Close>> {
        use super::op::{close::Close, Op};

        // A fixed slot: `Close` on the slot, pushed to the ring the slot belongs to. The slot is
        // freed when that close completes (the op data's drop).
        let fixed_op = self.fixed.take().and_then(Close::fixed);
        if self.fd < 0 {
            return fixed_op;
        }
        // The fd itself (registered mode keeps it open next to the slot). Dropping the slot's
        // op is fine: closes are never cancelled and the driver keeps the data until the CQE.
        drop(fixed_op);
        match Op::close(self.fd) {
            Ok(op) => Some(op),
            Err(_) => {
                let _ = unsafe { std::fs::File::from_raw_fd(self.fd) };
                None
            }
        }
    }

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
        if let State::Init | State::Waiting(..) = self.state.get_mut() {
            drop(self.start_close());
        }
    }
}
