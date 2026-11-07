//! Reply-then-receive as one operation (`send_then_recv`, round 3 item C1): a `SEND` linked to
//! a `RECV` (or `READ_FIXED`), with **one op slot and one CQE** for the pair.
//!
//! # Kernel facts the design rests on
//!
//! Verified on Linux 7.0 (`docs/performance-improvement/round3/r3-probe.md`, KV2 and KV3):
//!
//! - The head is `SEND` with `IOSQE_IO_LINK | IOSQE_CQE_SKIP_SUCCESS` and `MSG_WAITALL`, the
//!   tail a `RECV` with `IORING_RECVSEND_POLL_FIRST`. **Exactly one CQE is posted in every
//!   case**: the tail's if the send succeeded (its own CQE is skipped), else the head's (a
//!   failed member with `SKIP_SUCCESS` posts its CQE and the members after it post nothing:
//!   `REQ_F_SKIP_LINK_CQES`). So one slot waits for one CQE, and the slot is freed by it.
//! - `MSG_WAITALL` is what makes "skipped" mean "fully sent": without it a short send is a
//!   success and would be skipped. With it the kernel retries a short send inside the request;
//!   a send that fails after partial progress reports the **byte count** (`res >= 0`), not an
//!   errno.
//! - The value of the one CQE cannot tell the head from the tail (both report `-ECONNRESET`,
//!   `-ECANCELED`, or a count). So the head's `user_data` carries a tag
//!   ([`SKIP_MEMBER_TAG`](crate::driver::uring::SKIP_MEMBER_TAG), bit 61, with the member's
//!   position); the dispatcher strips it and completes the slot with
//!   [`MEMBER_CQE_FLAG`](crate::driver::uring::MEMBER_CQE_FLAG) set in the stored CQE flags,
//!   where the future reads it ([`cqe_member`]). The flags of a `SEND` CQE are otherwise
//!   unused. The same mechanism serves `uring::submit_chain` with the skip shape
//!   (`driver/chain.rs`).
//! - After 127 poll re-arms (a send of more than ~512 KiB into a full socket) the send moves to
//!   an io-wq worker, and the linked `RECV(POLL_FIRST)` is then issued inline there and posts
//!   `-EAGAIN`: "send complete, receive not started". The future hides it: it pushes a plain
//!   receive (same flags, `POLL_FIRST`) on a new slot and keeps waiting.
//! - `SEND_ZC` cannot be such a head (`-EINVAL`), so there is no zero-copy pair.
//!
//! # What the caller sees
//!
//! | CQE from | `res` | result `(send, recv)` |
//! |---|---|---|
//! | tail | `n >= 0` | `(Ok(reply_len), Ok(n))`, `n == 0` is EOF |
//! | tail | `-EAGAIN` | hidden: a plain receive is issued, the pair keeps waiting |
//! | tail | other error | `(Ok(reply_len), Err(e))` |
//! | head | `< 0` | `(Err(e), Err(ECANCELED))`: nothing went out, the receive did not run |
//! | head | `0 <= n < reply_len` | `(Ok(n), Err(ECANCELED))`: `n` bytes went out, then the send failed (the errno is lost); the receive did not run |
//!
//! Cancellation (`cancel()` or dropping the future) targets both `user_data` values: a cancel
//! by `user_data` finds only the member in flight (`-ENOENT` for the other).

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, squeue};

use super::{fixed::NO_OFFSET, CompletionMeta, OpAble};
use crate::{
    buf::{IoBuf, IoBufMut},
    driver::{
        fixed_buf::{on_current_ring, FixedBuf},
        shared_fd::{sqe_fd, SharedFd},
        uring::{cqe_member, UringInner},
        Inner, CURRENT,
    },
};

/// The SQEs of a pair, for the cancels on drop.
const MEMBERS: usize = 2;

/// `IORING_RECVSEND_POLL_FIRST`: poll before trying the receive (5.19+). The client sends the
/// next request only after it got the reply, so the data is never there when the SQE is
/// pushed.
const RECVSEND_POLL_FIRST: u16 = 1 << 0;

/// `MSG_NOSIGNAL | MSG_WAITALL`: no `SIGPIPE`, and a short send is retried in the kernel (or
/// fails the link) instead of being skipped as a success.
const SEND_FLAGS: libc::c_int = libc::MSG_NOSIGNAL | libc::MSG_WAITALL;

/// Keeps the socket open while the pair is in flight, and counts as **one send and one
/// receive** in flight on it (`try_send` / `try_recv` are refused meanwhile, see `InFlight`
/// in `shared_fd.rs`). One `SharedFd` for both counters instead of a `TxFd` and an `RxFd`:
/// one `Rc` clone and drop per pair, not two.
struct PairFd(SharedFd);

impl PairFd {
    #[inline]
    fn new(fd: SharedFd) -> Self {
        fd.begin_op::<true>();
        fd.begin_op::<false>();
        Self(fd)
    }
}

impl Drop for PairFd {
    #[inline]
    fn drop(&mut self) {
        self.0.end_op::<true>();
        self.0.end_op::<false>();
    }
}

/// Op data of a pair. [`OpAble::uring_op`] builds the **tail** (the receive): it is also what
/// a plain re-issue after `-EAGAIN` pushes. [`head_sqe`](Self::head_sqe) builds the send,
/// with the link and skip flags; the driver sets both `user_data`s.
pub(crate) trait PairAble: OpAble {
    fn head_sqe(&mut self) -> squeue::Entry;
}

/// What the future needs from the op data to hand the result back.
pub(crate) trait PairOp: PairAble + Unpin + 'static {
    type Reply;
    type Buf;

    /// The bytes of the reply, i.e. the send result when the send was skipped as a success.
    fn reply_len(&self) -> usize;

    /// The two buffers; `recv` is the receive result, so that `Ok(n)` can set the length of
    /// the receive buffer.
    fn into_parts(self, recv: &io::Result<usize>) -> (Self::Reply, Self::Buf);
}

/// `SEND` → `RECV`.
pub(crate) struct SendRecv<S, R> {
    fd: PairFd,
    reply: S,
    buf: R,
    /// `MSG_*` flags of the receive.
    recv_flags: libc::c_int,
}

impl<S: IoBuf, R: IoBufMut> SendRecv<S, R> {
    #[inline]
    pub(crate) fn new(fd: SharedFd, reply: S, buf: R, recv_flags: libc::c_int) -> Self {
        Self {
            fd: PairFd::new(fd),
            reply,
            buf,
            recv_flags,
        }
    }
}

/// The send SQE of a pair: `MSG_NOSIGNAL | MSG_WAITALL`, linked, its success CQE skipped.
#[inline]
fn head<S: IoBuf>(fd: &SharedFd, reply: &S) -> squeue::Entry {
    sqe_fd!(fd, |fd| opcode::Send::new(
        fd,
        reply.read_ptr(),
        reply.bytes_init() as _
    )
    .flags(SEND_FLAGS)
    .build()
    .flags(squeue::Flags::IO_LINK | squeue::Flags::SKIP_SUCCESS))
}

impl<S: IoBuf, R: IoBufMut> OpAble for SendRecv<S, R> {
    #[inline]
    fn uring_op(&mut self) -> squeue::Entry {
        sqe_fd!(self.fd.0, |fd| opcode::Recv::new(
            fd,
            self.buf.write_ptr(),
            self.buf.bytes_total() as _,
        )
        .flags(self.recv_flags)
        .ioprio(RECVSEND_POLL_FIRST)
        .build())
    }
}

impl<S: IoBuf, R: IoBufMut> PairAble for SendRecv<S, R> {
    #[inline]
    fn head_sqe(&mut self) -> squeue::Entry {
        head(&self.fd.0, &self.reply)
    }
}

impl<S: IoBuf, R: IoBufMut> PairOp for SendRecv<S, R> {
    type Reply = S;
    type Buf = R;

    #[inline]
    fn reply_len(&self) -> usize {
        self.reply.bytes_init()
    }

    #[inline]
    fn into_parts(self, recv: &io::Result<usize>) -> (S, R) {
        let Self {
            fd, reply, mut buf, ..
        } = self;
        drop(fd);
        if let Ok(n) = recv {
            // SAFETY: the kernel wrote `n` bytes to the buffer.
            unsafe { buf.set_init(*n) };
        }
        (reply, buf)
    }
}

/// `SEND` → `READ_FIXED` into a registered buffer (a plain `READ` if the buffer is registered
/// on another ring, like `recv_fixed`).
pub(crate) struct SendRecvFixed<S> {
    fd: PairFd,
    reply: S,
    buf: FixedBuf,
    fixed: bool,
}

impl<S: IoBuf> SendRecvFixed<S> {
    #[inline]
    pub(crate) fn new(fd: SharedFd, reply: S, buf: FixedBuf) -> Self {
        let fixed = on_current_ring(&buf);
        Self {
            fd: PairFd::new(fd),
            reply,
            buf,
            fixed,
        }
    }
}

impl<S: IoBuf> OpAble for SendRecvFixed<S> {
    #[inline]
    fn uring_op(&mut self) -> squeue::Entry {
        // No `POLL_FIRST` here: on `READ`/`READ_FIXED` the `ioprio` field is a real I/O
        // priority (class `NONE` with data 1 is `-EINVAL` at prep, and a rejected tail makes
        // the head report `-ECANCELED`, KV2 d3). The read is tried once and then arms poll,
        // like `recv_fixed`.
        let ptr = self.buf.write_ptr();
        let len = self.buf.bytes_total() as u32;
        sqe_fd!(self.fd.0, |fd| if self.fixed {
            opcode::ReadFixed::new(fd, ptr, len, self.buf.buf_index())
                .offset(NO_OFFSET)
                .build()
        } else {
            opcode::Read::new(fd, ptr, len).offset(NO_OFFSET).build()
        })
    }
}

impl<S: IoBuf> PairAble for SendRecvFixed<S> {
    #[inline]
    fn head_sqe(&mut self) -> squeue::Entry {
        head(&self.fd.0, &self.reply)
    }
}

impl<S: IoBuf> PairOp for SendRecvFixed<S> {
    type Reply = S;
    type Buf = FixedBuf;

    #[inline]
    fn reply_len(&self) -> usize {
        self.reply.bytes_init()
    }

    #[inline]
    fn into_parts(self, recv: &io::Result<usize>) -> (S, FixedBuf) {
        let Self {
            fd, reply, mut buf, ..
        } = self;
        drop(fd);
        if let Ok(n) = recv {
            // SAFETY: the kernel wrote `n` bytes to the buffer.
            unsafe { buf.set_init(*n) };
        }
        (reply, buf)
    }
}

/// The output of a pair: `(send result, receive result, reply, buf)`.
pub type PairResult<S, R> = (io::Result<usize>, io::Result<usize>, S, R);

enum State<D: PairOp> {
    /// Both SQEs are pushed; `index` is the pair's slot (`usize::MAX` only transiently, while
    /// a plain receive is re-issued after `-EAGAIN`).
    Running {
        driver: Inner,
        index: usize,
        data: D,
    },
    /// Resolved without an op in flight (the submit failed); returned on the first poll.
    Ready(io::Result<usize>, io::Result<usize>, D::Reply, D::Buf),
    /// The output was returned.
    Done,
}

/// The shared implementation behind [`SendThenRecv`] and [`SendThenRecvFixed`].
pub(crate) struct PairFuture<D: PairOp>(State<D>);

impl<D: PairOp> PairFuture<D> {
    /// Push the pair on the current runtime.
    ///
    /// # Panics
    /// Panics if called outside of a monoio runtime.
    pub(crate) fn submit(mut data: D) -> Self {
        let state = CURRENT.with(
            |driver| match UringInner::submit_pair(&driver.0, &mut data) {
                Ok(index) => State::Running {
                    driver: driver.clone(),
                    index,
                    data,
                },
                Err(e) => {
                    let (reply, buf) = data.into_parts(&Err(io::ErrorKind::Other.into()));
                    State::Ready(Err(e), Err(canceled()), reply, buf)
                }
            },
        );
        Self(state)
    }

    #[inline]
    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<PairResult<D::Reply, D::Buf>> {
        match &mut self.0 {
            State::Running {
                driver,
                index,
                data,
            } => {
                // Coop budget: the pair is one result.
                let budget = ready!(crate::task::coop::poll_proceed(cx));
                let (send, recv) = loop {
                    let meta = ready!(UringInner::poll_op(&driver.0, *index, cx));
                    // `Ready` freed the slot.
                    *index = usize::MAX;
                    let (send, recv) = interpret(meta, data.reply_len());
                    if is_eagain(&recv) {
                        // The tail was issued inline by an io-wq worker after the send moved
                        // there (KV3): "send complete, receive not started". A plain receive
                        // on a new slot; the caller never sees the `-EAGAIN`.
                        match UringInner::submit_index(&driver.0, data) {
                            Ok(new) => {
                                *index = new;
                                continue;
                            }
                            Err(e) => break (send, Err(e)),
                        }
                    }
                    break (send, recv);
                };
                crate::task::coop::consume(budget);
                let State::Running { data, .. } = std::mem::replace(&mut self.0, State::Done)
                else {
                    // SAFETY: matched as `Running` just above.
                    unsafe { std::hint::unreachable_unchecked() }
                };
                let (reply, buf) = data.into_parts(&recv);
                Poll::Ready((send, recv, reply, buf))
            }
            State::Ready(..) => match std::mem::replace(&mut self.0, State::Done) {
                State::Ready(send, recv, reply, buf) => Poll::Ready((send, recv, reply, buf)),
                // SAFETY: matched as `Ready` just above.
                _ => unsafe { std::hint::unreachable_unchecked() },
            },
            State::Done => polled_after_completion(),
        }
    }

    #[inline]
    fn is_terminated(&self) -> bool {
        matches!(self.0, State::Done)
    }

    fn cancel(&self) {
        if let State::Running { driver, index, .. } = &self.0 {
            if *index != usize::MAX {
                UringInner::cancel_shared(&driver.0, *index, MEMBERS);
            }
        }
    }

    fn state_name(&self) -> &'static str {
        match self.0 {
            State::Running { .. } => "Running",
            State::Ready(..) => "Ready",
            State::Done => "Done",
        }
    }
}

impl<D: PairOp> Drop for PairFuture<D> {
    fn drop(&mut self) {
        if let State::Running { .. } = self.0 {
            let State::Running {
                driver,
                index,
                data,
            } = std::mem::replace(&mut self.0, State::Done)
            else {
                return;
            };
            if index != usize::MAX {
                // The slot keeps `data` (both buffers, the fd guard) until the one CQE.
                UringInner::drop_shared(&driver.0, index, &mut Some(data), MEMBERS);
            }
        }
    }
}

/// The two results from the one CQE, see the module docs.
#[inline]
fn interpret(meta: CompletionMeta, reply_len: usize) -> (io::Result<usize>, io::Result<usize>) {
    let res = meta.result.map(|v| v.into_inner() as usize);
    if cqe_member(meta.flags).is_some() {
        // The head's CQE: the send failed, before anything went out (`Err`) or after `n`
        // bytes (`Ok(n)`, `n < reply_len`); the receive was cancelled without a CQE.
        (res, Err(canceled()))
    } else {
        // The tail's CQE: the send was skipped, i.e. fully sent.
        (Ok(reply_len), res)
    }
}

#[inline]
fn is_eagain(res: &io::Result<usize>) -> bool {
    matches!(res, Err(e) if e.raw_os_error() == Some(libc::EAGAIN))
}

#[cold]
fn canceled() -> io::Error {
    io::Error::from_raw_os_error(libc::ECANCELED)
}

#[cold]
#[inline(never)]
fn polled_after_completion() -> ! {
    panic!("op future polled after completion")
}

macro_rules! pair_future {
    ($(#[$meta:meta])* $name:ident<$($T:ident: $Bound:ident),+> => $data:ty, $reply:ty, $buf:ty) => {
        $(#[$meta])*
        ///
        /// `Unpin` and `'static`; resolves to `(send result, receive result, reply, buf)`. Both
        /// SQEs are pushed when the creating method is called. Same drop and cancel rules as
        /// the futures in [`monoio::io::futures`](crate::io::futures): dropping it in flight
        /// keeps both buffers in the op slot until the pair's CQE (and, with `async-cancel`,
        /// asks the kernel to cancel both members); [`cancel`](Self::cancel) and keep polling
        /// to get them back.
        #[must_use = "the pair is already submitted; dropping this cancels it and loses both buffers"]
        pub struct $name<$($T: $Bound),+>(PairFuture<$data>);

        impl<$($T: $Bound),+> $name<$($T),+> {
            #[inline]
            pub(crate) fn submit(data: $data) -> Self {
                Self(PairFuture::submit(data))
            }

            /// Polls the pair without pinning (token-style API). Returns `Ready` once.
            ///
            /// # Panics
            ///
            /// If called again after it returned `Ready`.
            #[inline]
            pub fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<PairResult<$reply, $buf>> {
                self.0.poll_result(cx)
            }

            /// True once the output was returned; polling again would panic.
            #[inline]
            pub fn is_terminated(&self) -> bool {
                self.0.is_terminated()
            }

            /// Asks the kernel to cancel the pair (one `AsyncCancel` per member: only the one in
            /// flight is found). Keep polling: the result is then `ECANCELED` (os error 125) for
            /// the member that was cancelled, or the pair's normal result if it finished first,
            /// and you get both buffers back either way. Does nothing if the submit failed or
            /// the output was returned.
            #[inline]
            pub fn cancel(&self) {
                self.0.cancel()
            }
        }

        impl<$($T: $Bound),+> Future for $name<$($T),+> {
            type Output = PairResult<$reply, $buf>;

            #[inline]
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.0.poll_result(cx)
            }
        }

        impl<$($T: $Bound),+> fmt::Debug for $name<$($T),+> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("state", &self.0.state_name())
                    .finish()
            }
        }
    };
}

pair_future! {
    /// An in-flight reply-then-receive pair (`SEND` linked to `RECV`), returned by
    /// [`TcpStream::send_then_recv`](crate::net::TcpStream::send_then_recv) and
    /// [`UnixStream::send_then_recv`](crate::net::UnixStream::send_then_recv). One op slot,
    /// one CQE and one task wake for both; see those methods for how to read the result.
    SendThenRecv<S: IoBuf, R: IoBufMut> => SendRecv<S, R>, S, R
}

pair_future! {
    /// An in-flight reply-then-receive pair whose receive is `IORING_OP_READ_FIXED` into a
    /// registered buffer, returned by
    /// [`TcpStream::send_then_recv_fixed`](crate::net::TcpStream::send_then_recv_fixed) and
    /// [`UnixStream::send_then_recv_fixed`](crate::net::UnixStream::send_then_recv_fixed).
    SendThenRecvFixed<S: IoBuf> => SendRecvFixed<S>, S, FixedBuf
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The head's tag flag is outside what a plain `SEND`/`RECV` CQE carries, and the
    /// interpretation of the one CQE follows the table of the module docs.
    #[test]
    fn interpret_one_cqe() {
        use crate::driver::{op::MaybeFd, uring::MEMBER_CQE_FLAG};
        let ok = |n: i32, flags: u32| CompletionMeta {
            result: unsafe { MaybeFd::new_result(Ok(n as u32), false) },
            flags,
        };
        let err = |e: i32, flags: u32| CompletionMeta {
            result: Err(io::Error::from_raw_os_error(e)),
            flags,
        };
        // Tail, data.
        let (s, r) = interpret(ok(4, 0), 70);
        assert_eq!((s.unwrap(), r.unwrap()), (70, 4));
        // Tail, EOF.
        let (s, r) = interpret(ok(0, 0), 70);
        assert_eq!((s.unwrap(), r.unwrap()), (70, 0));
        // Tail, error.
        let (s, r) = interpret(err(libc::ECONNRESET, 0), 70);
        assert_eq!(s.unwrap(), 70);
        assert_eq!(r.unwrap_err().raw_os_error(), Some(libc::ECONNRESET));
        // Tail, -EAGAIN is what the future re-issues on.
        let (_, r) = interpret(err(libc::EAGAIN, 0), 70);
        assert!(is_eagain(&r));
        // Head, failed at once.
        let (s, r) = interpret(err(libc::EPIPE, MEMBER_CQE_FLAG), 70);
        assert_eq!(s.unwrap_err().raw_os_error(), Some(libc::EPIPE));
        assert_eq!(r.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
        // Head, failed after 12 bytes.
        let (s, r) = interpret(ok(12, MEMBER_CQE_FLAG), 70);
        assert_eq!(s.unwrap(), 12);
        assert_eq!(r.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
        // The flag does not collide with the CQE flags the kernel defines (low bits).
        assert_eq!(MEMBER_CQE_FLAG & 0xffff, 0);
        assert_eq!(cqe_member(0), None);
        assert_eq!(cqe_member(MEMBER_CQE_FLAG), Some(0));
        assert_eq!(cqe_member(MEMBER_CQE_FLAG | 3 << 16), Some(3));
    }

    /// The op data of a typical pair (a static reply, a `Vec` receive buffer) and what a
    /// dropped-in-flight one costs: it is above the slot's 48 bytes of inline storage, so it
    /// is boxed then (one allocation per pair dropped in flight, none otherwise).
    #[test]
    fn op_data_sizes() {
        use std::mem::size_of;
        assert_eq!(size_of::<PairFd>(), size_of::<SharedFd>());
        assert_eq!(size_of::<SendRecv<&'static [u8], Vec<u8>>>(), 56);
        // A `FixedBuf` is 40 bytes on its own.
        assert_eq!(size_of::<SendRecvFixed<&'static [u8]>>(), 72);
    }
}
