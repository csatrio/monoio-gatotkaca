//! Named, `Unpin` futures for in-flight IO ops.
//!
//! The rent traits ([`AsyncReadRent`](crate::io::AsyncReadRent),
//! [`AsyncWriteRent`](crate::io::AsyncWriteRent)) return anonymous `impl Future`s, which a struct
//! can only store behind `Box::pin`. The types here name those futures, so a connection state
//! machine can keep an op in a field with no allocation:
//!
//! ```no_run
//! use monoio::{io::futures::RecvFuture, net::TcpStream};
//!
//! struct Conn {
//!     stream: TcpStream,
//!     // No `Pin<Box<dyn Future>>`: the op lives inline.
//!     pending_read: Option<RecvFuture<Vec<u8>>>,
//! }
//!
//! impl Conn {
//!     fn start_read(&mut self, buf: Vec<u8>) {
//!         self.pending_read = Some(self.stream.recv_op(buf));
//!     }
//! }
//! ```
//!
//! The methods that create them end in `_op`: [`TcpStream::recv_op`](crate::net::TcpStream::recv_op),
//! [`send_op`](crate::net::TcpStream::send_op), [`readv_op`](crate::net::TcpStream::readv_op),
//! [`writev_op`](crate::net::TcpStream::writev_op), the `*_with_flags_op` variants, the same on
//! [`UnixStream`](crate::net::UnixStream) and on the owned split halves, and
//! [`File::read_at_op`](crate::fs::File::read_at_op) /
//! [`File::write_at_op`](crate::fs::File::write_at_op). They take `&self` and the returned future
//! does not borrow the stream (it holds its own reference to the fd), so it is `'static`.
//!
//! # Rules
//!
//! - **The SQE is pushed when the method is called**, not on first poll. The kernel may run the
//!   op before you poll it.
//! - **Each future resolves to the same output as the matching rent method**, e.g. `(res, buf)` of
//!   [`BufResult`]. A submit failure (the submission queue could not be flushed) is kept in the
//!   future and returned, with the buffer, on the first poll.
//! - **Dropping an unfinished future** behaves exactly like dropping the rent method's future: the
//!   runtime keeps the buffer alive until the kernel is done with it and, with `async-cancel`,
//!   pushes an `AsyncCancel`. The buffer is lost to you. Prefer [`RecvFuture::cancel`] and keep
//!   polling when you want the buffer back.
//! - Polling again after `Ready` panics; check [`is_terminated`](RecvFuture::is_terminated).
//!
//! # Token / poll API
//!
//! For hand-written state machines each future doubles as an op token: create it with the `_op`
//! method (this starts the op), store it, and drive it with `poll_result(&mut self, cx)` from your
//! own `poll`. Because the futures are `Unpin`, no `Pin` is needed:
//!
//! ```no_run
//! use std::{
//!     future::Future,
//!     pin::Pin,
//!     task::{Context, Poll},
//! };
//!
//! use monoio::{io::futures::RecvFuture, net::TcpStream, BufResult};
//!
//! struct ReadOnce {
//!     token: RecvFuture<Vec<u8>>,
//! }
//!
//! impl Future for ReadOnce {
//!     type Output = BufResult<usize, Vec<u8>>;
//!
//!     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//!         self.token.poll_result(cx)
//!     }
//! }
//!
//! async fn read_once(stream: &TcpStream) -> BufResult<usize, Vec<u8>> {
//!     ReadOnce {
//!         token: stream.recv_op(vec![0; 4096]),
//!     }
//!     .await
//! }
//! ```

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut},
    driver::op::{
        read::{ReadAt, ReadVec},
        recv::Recv,
        send::Send as SendData,
        write::{WriteAt, WriteVec},
        Completion, Op, OpAble,
    },
    BufResult,
};

/// An op whose completion maps to `BufResult<usize, Buf>`.
///
/// Implemented next to each op (in `driver/op/*.rs`); the op's `async fn result` uses the same
/// mapping, so the named futures and the rent methods cannot diverge.
pub(crate) trait BufOp: OpAble + Unpin + Sized + 'static {
    /// The buffer handed back to the caller.
    type Buf;

    /// Turns a completion into the caller-facing result.
    fn complete(completion: Completion<Self>) -> BufResult<usize, Self::Buf>;
}

enum State<D: BufOp> {
    /// Submitted; the op owns the buffer.
    Running(Op<D>),
    /// Submission failed; returned on the first poll.
    Failed(io::Error, D::Buf),
    /// The output was returned.
    Done,
}

/// The shared implementation behind every named future.
pub(crate) struct OpFuture<D: BufOp> {
    state: State<D>,
}

impl<D: BufOp> OpFuture<D> {
    #[inline]
    fn new(submitted: Result<Op<D>, (io::Error, D::Buf)>) -> Self {
        let state = match submitted {
            Ok(op) => State::Running(op),
            Err((e, buf)) => State::Failed(e, buf),
        };
        Self { state }
    }

    #[inline]
    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<BufResult<usize, D::Buf>> {
        match &mut self.state {
            State::Running(op) => {
                let completion = ready!(Pin::new(op).poll(cx));
                // The op is finished (its slot is released), so dropping it does nothing.
                self.state = State::Done;
                Poll::Ready(D::complete(completion))
            }
            State::Failed(..) => match std::mem::replace(&mut self.state, State::Done) {
                State::Failed(e, buf) => Poll::Ready((Err(e), buf)),
                _ => unreachable!(),
            },
            State::Done => panic!("op future polled after completion"),
        }
    }

    #[inline]
    fn is_terminated(&self) -> bool {
        matches!(self.state, State::Done)
    }

    #[inline]
    fn cancel(&self) {
        if let State::Running(op) = &self.state {
            // Safety: `op` is alive and unfinished, so its slab slot (the index the cancel
            // targets) still belongs to it.
            unsafe { op.op_canceller().cancel() }
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            State::Running(_) => "Running",
            State::Failed(..) => "Failed",
            State::Done => "Done",
        }
    }
}

macro_rules! named_future {
    ($(#[$meta:meta])* $name:ident<$T:ident: $Bound:ident> => $data:ty) => {
        $(#[$meta])*
        ///
        /// `Unpin` and `'static`; resolves to `(io::Result<usize>, buffer)`. See the
        /// [module docs](self) for the drop and cancel rules.
        #[must_use = "the op is already submitted; dropping this cancels it and loses the buffer"]
        pub struct $name<$T: $Bound>(OpFuture<$data>);

        impl<$T: $Bound> $name<$T> {
            #[inline]
            pub(crate) fn new(submitted: Result<Op<$data>, (io::Error, $T)>) -> Self {
                Self(OpFuture::new(submitted))
            }

            /// Polls the op without pinning: the token-style API for hand-written state
            /// machines. Returns `Ready((res, buf))` once, like [`Future::poll`].
            ///
            /// # Panics
            ///
            /// If called again after it returned `Ready`.
            #[inline]
            pub fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<BufResult<usize, $T>> {
                self.0.poll_result(cx)
            }

            /// True once the output was returned; polling again would panic.
            #[inline]
            pub fn is_terminated(&self) -> bool {
                self.0.is_terminated()
            }

            /// Asks the kernel to cancel the op (pushes an `AsyncCancel` SQE). Keep polling: the
            /// result is then `Err` with `ECANCELED` (os error 125), or the op's normal result if
            /// it finished first, and you get the buffer back either way. Does nothing if the op
            /// failed to submit or already returned.
            #[inline]
            pub fn cancel(&self) {
                self.0.cancel()
            }
        }

        impl<$T: $Bound> Future for $name<$T> {
            type Output = BufResult<usize, $T>;

            #[inline]
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.0.poll_result(cx)
            }
        }

        impl<$T: $Bound> fmt::Debug for $name<$T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("state", &self.0.state_name())
                    .finish()
            }
        }
    };
}

named_future! {
    /// An in-flight socket receive (`IORING_OP_RECV`), returned by `recv_op` and
    /// `recv_with_flags_op` on [`TcpStream`](crate::net::TcpStream) and
    /// [`UnixStream`](crate::net::UnixStream). Same op and result as
    /// [`AsyncReadRent::read`](crate::io::AsyncReadRent::read) on those streams.
    RecvFuture<T: IoBufMut> => Recv<T>
}

named_future! {
    /// An in-flight socket send (`IORING_OP_SEND` with `MSG_NOSIGNAL`), returned by `send_op` and
    /// `send_with_flags_op` on [`TcpStream`](crate::net::TcpStream) and
    /// [`UnixStream`](crate::net::UnixStream). Same op and result as
    /// [`AsyncWriteRent::write`](crate::io::AsyncWriteRent::write) on those streams.
    SendFuture<T: IoBuf> => SendData<T>
}

named_future! {
    /// An in-flight vectored read (`IORING_OP_READV`), returned by `readv_op`. Same op and result
    /// as [`AsyncReadRent::readv`](crate::io::AsyncReadRent::readv) on the socket streams.
    ReadvFuture<T: IoVecBufMut> => ReadVec<T>
}

named_future! {
    /// An in-flight vectored write (`IORING_OP_WRITEV`), returned by `writev_op`. Same op and
    /// result as [`AsyncWriteRent::writev`](crate::io::AsyncWriteRent::writev) on the socket
    /// streams. Note: like `writev`, a reset peer can raise `SIGPIPE`; use
    /// `send_vectored` when that matters.
    WritevFuture<T: IoVecBuf> => WriteVec<T>
}

named_future! {
    /// An in-flight positional read (`IORING_OP_READ` at an offset), returned by
    /// [`File::read_at_op`](crate::fs::File::read_at_op). Same op and result as
    /// [`File::read_at`](crate::fs::File::read_at).
    ReadAtFuture<T: IoBufMut> => ReadAt<T>
}

named_future! {
    /// An in-flight positional write (`IORING_OP_WRITE` at an offset), returned by
    /// [`File::write_at_op`](crate::fs::File::write_at_op). Same op and result as
    /// [`File::write_at`](crate::fs::File::write_at).
    WriteAtFuture<T: IoBuf> => WriteAt<T>
}
