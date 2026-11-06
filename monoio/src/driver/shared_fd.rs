use std::{
    cell::{Cell, OnceCell, UnsafeCell},
    io,
    ops::Deref,
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

// `repr(C)`: the fd, the two counters and the flag sit together at the start, right behind the
// `Rc` counts that every op clone and drop already writes. Counting an op touches no other
// cache line.
#[repr(C)]
struct Inner {
    // Open file descriptor, `-1` for a direct-only file (it lives only in the fixed table).
    fd: RawFd,

    // Ring ops in flight that send on / receive from this socket: the number of live
    // [`InFlight`] guards. Not zero: a direct `send(2)` / `recv(2)` could overtake them.
    tx_in_flight: Cell<u32>,
    rx_in_flight: Cell<u32>,

    // The per-stream switch of `set_eager_send`: `AsyncWriteRent::write` tries a direct send
    // first. Here (and not in the stream) so that split halves see it.
    eager_send: Cell<bool>,

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
                tx_in_flight: Cell::new(0),
                rx_in_flight: Cell::new(0),
                eager_send: Cell::new(false),
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

    /// Whether a direct `send(2)` may go out now: the socket has an fd (not direct-only) and no
    /// ring send is in flight on it (see [`InFlight`]).
    #[inline]
    pub(crate) fn can_send_now(&self) -> bool {
        self.inner.tx_in_flight.get() == 0 && self.inner.fd >= 0
    }

    /// The receive twin of [`can_send_now`](Self::can_send_now).
    #[inline]
    pub(crate) fn can_recv_now(&self) -> bool {
        self.inner.rx_in_flight.get() == 0 && self.inner.fd >= 0
    }

    /// One `send(2)` with `MSG_DONTWAIT | MSG_NOSIGNAL | flags`, without the checks of
    /// [`try_send`](Self::try_send): the caller made sure of [`can_send_now`]. `EINTR` is
    /// retried; a full socket buffer gives `WouldBlock`.
    ///
    /// [`can_send_now`]: Self::can_send_now
    #[inline]
    pub(crate) fn send_now(&self, buf: &[u8], flags: libc::c_int) -> io::Result<usize> {
        #[allow(deprecated)]
        let flags = flags | libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
        loop {
            // SAFETY: `buf` is valid for `buf.len()` bytes; the kernel only reads it.
            let n = unsafe { libc::send(self.inner.fd, buf.as_ptr().cast(), buf.len(), flags) };
            if n >= 0 {
                return Ok(n as usize);
            }
            let e = io::Error::last_os_error();
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e);
            }
        }
    }

    /// `TcpStream::try_send` / `UnixStream::try_send`: one non-blocking `send(2)` on the calling
    /// thread. `WouldBlock` when the socket buffer is full, when a ring send is in flight
    /// (ordering) or when the socket has no fd.
    #[inline]
    pub(crate) fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        if !self.can_send_now() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.send_now(buf, 0)
    }

    /// One non-blocking `sendmsg(2)` of the `iov_len` iovecs at `iov` (`MSG_DONTWAIT |
    /// MSG_NOSIGNAL`), without the checks of [`try_send_vectored`](Self::try_send_vectored).
    ///
    /// # Safety
    /// `iov` must point to `iov_len` valid iovecs whose memory is readable.
    pub(crate) unsafe fn send_vectored_now(
        &self,
        iov: *const libc::iovec,
        iov_len: usize,
    ) -> io::Result<usize> {
        // The kernel rejects more (`EMSGSIZE`); sending fewer is a short send.
        const IOV_MAX: usize = 1024;
        #[allow(deprecated)]
        const FLAGS: libc::c_int = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
        // SAFETY: an all-zero msghdr is valid (no name, no control).
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = iov as *mut libc::iovec;
        msg.msg_iovlen = iov_len.min(IOV_MAX) as _;
        loop {
            // SAFETY: `msg` points to the caller's iovecs; the kernel only reads them.
            let n = unsafe { libc::sendmsg(self.inner.fd, &msg, FLAGS) };
            if n >= 0 {
                return Ok(n as usize);
            }
            let e = io::Error::last_os_error();
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e);
            }
        }
    }

    /// `try_send` for several buffers (`sendmsg`). Same `WouldBlock` rules.
    #[inline]
    pub(crate) fn try_send_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        if !self.can_send_now() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        // SAFETY: `IoSlice` is ABI-compatible with `iovec` on Unix, and borrows its memory.
        unsafe { self.send_vectored_now(bufs.as_ptr().cast(), bufs.len()) }
    }

    /// `TcpStream::try_recv` / `UnixStream::try_recv`: one non-blocking `recv(2)` on the calling
    /// thread. `WouldBlock` when no data is queued, when a ring receive is in flight (the two
    /// would race for the same bytes) or when the socket has no fd.
    #[inline]
    pub(crate) fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.can_recv_now() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        loop {
            // SAFETY: `buf` is valid for writes of `buf.len()` bytes.
            let n = unsafe {
                libc::recv(
                    self.inner.fd,
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            if n >= 0 {
                return Ok(n as usize);
            }
            let e = io::Error::last_os_error();
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e);
            }
        }
    }

    /// The `set_eager_send` switch of the stream.
    #[inline]
    pub(crate) fn eager_send(&self) -> bool {
        self.inner.eager_send.get()
    }

    /// See [`eager_send`](Self::eager_send).
    #[inline]
    pub(crate) fn set_eager_send(&self, on: bool) {
        self.inner.eager_send.set(on);
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

/// The [`SharedFd`] in the data of a ring op that sends on (`TX = true`, [`TxFd`]) or receives
/// from (`TX = false`, [`RxFd`]) a socket. While it lives, the op counts as "in flight" in the
/// socket's `tx_in_flight` / `rx_in_flight`. `try_send`, `try_recv` and `send_eager` check that
/// counter before they bypass the ring: a direct syscall must not overtake bytes that a ring
/// op still has to move.
///
/// It is as large as a `SharedFd` (the counter is found through it), so no op data grows.
///
/// # The counter is never early
///
/// "Early" would be: the counter says zero while the kernel may still move bytes for a ring
/// op. That cannot happen, because the guard is part of the op data, and the op data outlives
/// the op in the kernel:
///
/// - **Raised** in [`InFlight::new`], when the op data is built, before its SQE is pushed. An
///   op that sits in the SQ, not yet seen by the kernel, counts already.
/// - **Lowered** in `Drop`, when the op data is dropped. Op data is dropped only after the
///   op's final CQE:
///   - the future completes: `Op::poll` takes the data out once `poll_op` returned the CQE's
///     result, and the `BufOp::complete` / `result()` that hands the buffer back drops the
///     rest of the data (this guard);
///   - the future is dropped in flight: `Ref::drop_op` moves the data into the op slot
///     (`Lifecycle::Ignored`, or `IgnoredMore` for a zero-copy send), where it is dropped
///     when the final CQE arrives (for a zero-copy send: the notification);
///   - a linked chain (`ChainOp`) keeps its data until the last CQE of the chain, a multishot
///     op (`MultishotOp`) until the CQE without `F_MORE`;
///   - the submit fails (the SQ stayed full): the kernel never saw the op, and the
///     constructor's `map_err` drops the data at once.
/// - The runtime is single-threaded, and the counters live in the `Rc` that the guard itself
///   keeps alive.
///
/// The counter can be **late**: between the CQE being posted and the task that owns the future
/// being polled (for a dropped op: the driver's next tick) it still says "in flight". Then
/// `try_send` returns `WouldBlock` and `send_eager` uses the ring: slower, never out of order.
///
/// Not counted: SQEs the user builds (`uring::submit`, chains, the CQE hook). They carry no op
/// data of this crate.
pub(crate) struct InFlight<const TX: bool>(SharedFd);

/// Guard of an op that sends on a socket. See [`InFlight`].
pub(crate) type TxFd = InFlight<true>;
/// Guard of an op that receives from a socket. See [`InFlight`].
pub(crate) type RxFd = InFlight<false>;

impl<const TX: bool> InFlight<TX> {
    /// Count one more op in flight on `fd`, until the returned guard is dropped.
    #[inline]
    pub(crate) fn new(fd: SharedFd) -> Self {
        fd.begin_op::<TX>();
        Self(fd)
    }
}

impl<const TX: bool> Drop for InFlight<TX> {
    #[inline]
    fn drop(&mut self) {
        self.0.end_op::<TX>();
    }
}

impl SharedFd {
    #[inline]
    fn in_flight<const TX: bool>(&self) -> &Cell<u32> {
        if TX {
            &self.inner.tx_in_flight
        } else {
            &self.inner.rx_in_flight
        }
    }

    /// Count one more ring send (`TX`) or receive in flight. Use the [`InFlight`] guard; this
    /// is for op data that cannot hold one (it must call [`end_op`](Self::end_op) exactly
    /// once, from its own `Drop`).
    #[inline]
    pub(crate) fn begin_op<const TX: bool>(&self) {
        let counter = self.in_flight::<TX>();
        counter.set(counter.get() + 1);
    }

    /// See [`begin_op`](Self::begin_op).
    #[inline]
    pub(crate) fn end_op<const TX: bool>(&self) {
        let counter = self.in_flight::<TX>();
        counter.set(counter.get() - 1);
    }
}

impl<const TX: bool> Deref for InFlight<TX> {
    type Target = SharedFd;

    #[inline]
    fn deref(&self) -> &SharedFd {
        &self.0
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

#[cfg(test)]
mod tests {
    use std::os::fd::IntoRawFd;

    use super::*;

    fn devnull() -> SharedFd {
        // `SharedFd` closes its fd when dropped.
        let file = std::fs::File::open("/dev/null").unwrap();
        SharedFd::new_without_register(file.into_raw_fd())
    }

    #[test]
    fn guards_count_per_direction() {
        let fd = devnull();
        assert!(fd.can_send_now() && fd.can_recv_now());

        let tx1 = TxFd::new(fd.clone());
        let tx2 = TxFd::new(fd.clone());
        assert!(!fd.can_send_now() && fd.can_recv_now());
        drop(tx1);
        assert!(!fd.can_send_now(), "one send is still in flight");
        let rx = RxFd::new(fd.clone());
        assert!(!fd.can_send_now() && !fd.can_recv_now());
        drop(tx2);
        assert!(fd.can_send_now() && !fd.can_recv_now());
        // The guard is the op's handle of the file.
        assert_eq!(rx.raw_fd(), fd.raw_fd());
        drop(rx);
        assert!(fd.can_send_now() && fd.can_recv_now());
    }

    #[test]
    fn guard_is_a_shared_fd() {
        assert_eq!(std::mem::size_of::<TxFd>(), std::mem::size_of::<SharedFd>());
        // The counters and the switch are in the first 16 bytes of the shared state, next to
        // the `Rc` counts.
        let fd = devnull();
        let base = &*fd.inner as *const Inner as usize;
        for field in [
            &fd.inner.tx_in_flight as *const _ as usize,
            &fd.inner.rx_in_flight as *const _ as usize,
            &fd.inner.eager_send as *const _ as usize,
        ] {
            assert!(field - base < 16);
        }
    }

    #[test]
    fn clones_share_the_eager_switch() {
        let fd = devnull();
        let other = fd.clone();
        assert!(!other.eager_send());
        fd.set_eager_send(true);
        assert!(other.eager_send());
    }

    #[test]
    fn nothing_direct_without_an_fd() {
        // A direct-only file has fd -1; the checks only look at the fd.
        let fd = SharedFd::new_without_register(-1);
        assert!(!fd.can_send_now() && !fd.can_recv_now());
        assert_eq!(
            fd.try_send(b"x").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut buf = [0u8; 1];
        assert_eq!(
            fd.try_recv(&mut buf).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        // Nothing to close.
        std::mem::forget(fd);
    }
}
