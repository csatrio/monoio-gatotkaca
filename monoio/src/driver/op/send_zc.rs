//! Zero-copy sends beyond the plain `write_zc` ([`super::send::SendZc`]): a send whose result
//! is handed out before the kernel released the buffer ([`SendZcFuture`], [`ZcPending`]),
//! `SEND_ZC` on a registered buffer, and `SENDMSG_ZC` for vectored sends.
//!
//! A zero-copy send posts two CQEs: the send result (with `IORING_CQE_F_MORE`), then a
//! notification (`IORING_CQE_F_NOTIF`) once the kernel no longer reads the buffer. A failed send
//! posts only the first one, without `F_MORE`.

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::opcode;

use super::{
    super::shared_fd::{sqe_fd, SharedFd},
    submit::SubmitResult,
    Op, OpAble, OpKind,
};
use crate::{
    buf::{IoBuf, IoVecBuf},
    driver::{
        fixed_buf::{on_current_ring, FixedIoBuf},
        uring::UringInner,
    },
    BufResult,
};

#[allow(deprecated)]
const NOSIGNAL: i32 = libc::MSG_NOSIGNAL;

/// `IORING_OP_SEND_ZC` of one buffer, optionally a registered one (`buf_index`).
pub(crate) struct SendZcBuf<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed while the operation
    /// is in-flight.
    fd: SharedFd,
    pub(crate) buf: T,
    /// The registered-buffer index (sets `IORING_RECVSEND_FIXED_BUF`), if the buffer is
    /// registered on this ring.
    buf_index: Option<u16>,
}

impl<T: IoBuf> Op<SendZcBuf<T>> {
    /// Zero-copy send of a plain buffer.
    pub(crate) fn send_zc_buf(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        Op::submit_or_return(SendZcBuf {
            fd,
            buf,
            buf_index: None,
        })
        .map_err(|(e, d)| (e, d.buf))
    }
}

impl<T: FixedIoBuf> Op<SendZcBuf<T>> {
    /// Zero-copy send of a registered buffer. A buffer registered on another ring is sent like
    /// a plain one (its index means nothing to this ring).
    pub(crate) fn send_zc_fixed(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        let buf_index = on_current_ring(&buf).then(|| buf.buf_index());
        Op::submit_or_return(SendZcBuf { fd, buf, buf_index }).map_err(|(e, d)| (e, d.buf))
    }
}

impl<T: IoBuf> Op<SendZcBuf<T>> {
    /// Wait for both CQEs.
    pub(crate) async fn result(self) -> BufResult<usize, T> {
        let complete = self.await;
        (
            complete.meta.result.map(|v| v.into_inner() as _),
            complete.data.buf,
        )
    }
}

impl<T: IoBuf> OpAble for SendZcBuf<T> {
    const KIND: OpKind = OpKind::ZeroCopy;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let (ptr, len) = (self.buf.read_ptr(), self.buf.bytes_init() as _);
        sqe_fd!(self.fd, |fd| opcode::SendZc::new(fd, ptr, len)
            .buf_index(self.buf_index)
            .flags(NOSIGNAL)
            .build())
    }
}

/// `IORING_OP_SENDMSG_ZC` on a connected socket (no address), with `MSG_NOSIGNAL`.
pub(crate) struct SendMsgZc<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed while the operation
    /// is in-flight.
    fd: SharedFd,
    /// The in-flight buffers; `msg` points at their iovecs.
    pub(crate) buf_vec: T,
    /// Boxed so the kernel sees a stable address after the op moves.
    msg: Box<libc::msghdr>,
}

impl<T: IoVecBuf> Op<SendMsgZc<T>> {
    pub(crate) fn send_msg_zc(fd: SharedFd, buf_vec: T) -> SubmitResult<Self, T> {
        // SAFETY: an all-zero msghdr is valid (no name, no control). `uring_op` sets the iovecs.
        let msg: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
        Op::submit_or_return(SendMsgZc { fd, buf_vec, msg }).map_err(|(e, d)| (e, d.buf_vec))
    }

    /// Wait for both CQEs.
    pub(crate) async fn result(self) -> BufResult<usize, T> {
        let complete = self.await;
        (
            complete.meta.result.map(|v| v.into_inner() as _),
            complete.data.buf_vec,
        )
    }
}

impl<T: IoVecBuf> OpAble for SendMsgZc<T> {
    const KIND: OpKind = OpKind::ZeroCopy;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        self.msg.msg_iov = self.buf_vec.read_iovec_ptr() as *mut _;
        self.msg.msg_iovlen = self.buf_vec.read_iovec_len() as _;
        let msg = &*self.msg as *const libc::msghdr;
        sqe_fd!(self.fd, |fd| opcode::SendMsgZc::new(fd, msg)
            .flags(NOSIGNAL as u32)
            .build())
    }
}

enum FirstState<T: IoBuf> {
    /// Submitted; the op owns the buffer.
    Running(Op<SendZcBuf<T>>),
    /// Submission failed; returned on the first poll.
    Failed(io::Error, T),
    /// The output was returned.
    Done,
}

/// A zero-copy send that resolves with the send result as soon as the kernel posts it, made
/// by [`TcpStream::send_zc_early`](crate::net::TcpStream::send_zc_early) and
/// [`TcpStream::send_zc_fixed_early`](crate::net::TcpStream::send_zc_fixed_early).
///
/// Resolves to `(io::Result<usize>, ZcPending<T>)`: the bytes sent (or the error), and a
/// [`ZcPending`] that gives the buffer back once the kernel released it. `Unpin` and `'static`;
/// the SQE is pushed when the creating method is called. Dropping it before it resolved cancels
/// the send (with `async-cancel`) and keeps the buffer alive until the kernel is done.
#[must_use = "the op is already submitted; dropping this cancels it and loses the buffer"]
pub struct SendZcFuture<T: IoBuf>(FirstState<T>);

impl<T: IoBuf> SendZcFuture<T> {
    #[inline]
    pub(crate) fn new(submitted: SubmitResult<Op<SendZcBuf<T>>, T>) -> Self {
        Self(match submitted {
            Ok(op) => FirstState::Running(op),
            Err((e, buf)) => FirstState::Failed(e, buf),
        })
    }

    /// Whether the result was returned.
    #[inline]
    pub fn is_terminated(&self) -> bool {
        matches!(self.0, FirstState::Done)
    }

    /// Ask the kernel to cancel the send (best effort). Keep polling: the result is then
    /// `ECANCELED`, or the send if it was already done.
    pub fn cancel(&self) {
        if let FirstState::Running(op) = &self.0 {
            // SAFETY: `op` is alive and unfinished, so its slab slot still belongs to it.
            unsafe { op.op_canceller().cancel() }
        }
    }
}

impl<T: IoBuf> Future for SendZcFuture<T> {
    type Output = (io::Result<usize>, ZcPending<T>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.0 {
            FirstState::Running(op) => {
                let first = ready!(UringInner::poll_zc_first(&op.driver.0, op.index, cx));
                let FirstState::Running(mut op) = std::mem::replace(&mut this.0, FirstState::Done)
                else {
                    unreachable!()
                };
                let res = first.meta.result.map(|v| v.into_inner() as usize);
                let pending = if first.done {
                    // The slot is free: the kernel holds nothing any more.
                    op.index = usize::MAX;
                    let buf = op
                        .data
                        .take()
                        .expect("op data is present until completion")
                        .buf;
                    PendingState::Ready(buf)
                } else {
                    PendingState::Waiting(op)
                };
                Poll::Ready((res, ZcPending(pending)))
            }
            FirstState::Failed(..) => match std::mem::replace(&mut this.0, FirstState::Done) {
                FirstState::Failed(e, buf) => {
                    Poll::Ready((Err(e), ZcPending(PendingState::Ready(buf))))
                }
                _ => unreachable!(),
            },
            FirstState::Done => panic!("SendZcFuture polled after completion"),
        }
    }
}

impl<T: IoBuf> fmt::Debug for SendZcFuture<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.0 {
            FirstState::Running(_) => "Running",
            FirstState::Failed(..) => "Failed",
            FirstState::Done => "Done",
        };
        f.debug_struct("SendZcFuture")
            .field("state", &state)
            .finish()
    }
}

enum PendingState<T: IoBuf> {
    /// The kernel may still read the buffer; the op owns it.
    Waiting(Op<SendZcBuf<T>>),
    /// Released; returned on the next poll.
    Ready(T),
    /// The buffer was returned.
    Done,
}

/// The buffer of a [`SendZcFuture`] whose send is done, but which the kernel may still read
/// (zero-copy: the NIC may still be sending from it).
///
/// A future resolving to the buffer once the kernel released it (the notification CQE).
/// `Unpin` and `'static`. Dropping it is fine: the runtime then keeps the buffer until the
/// notification and drops it after, without pushing a cancel.
#[must_use = "dropping this frees the buffer only after the kernel released it"]
pub struct ZcPending<T: IoBuf>(PendingState<T>);

impl<T: IoBuf> ZcPending<T> {
    /// Whether the kernel released the buffer already (polling then resolves at once). After
    /// a `false`, poll (or await) to learn when it is.
    pub fn is_released(&self) -> bool {
        match &self.0 {
            PendingState::Waiting(op) => UringInner::zc_released(&op.driver.0, op.index),
            PendingState::Ready(_) => true,
            PendingState::Done => false,
        }
    }

    /// Whether the buffer was returned.
    #[inline]
    pub fn is_terminated(&self) -> bool {
        matches!(self.0, PendingState::Done)
    }
}

impl<T: IoBuf> Future for ZcPending<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        match &mut this.0 {
            PendingState::Waiting(op) => {
                ready!(UringInner::poll_zc_notif(&op.driver.0, op.index, cx));
                let PendingState::Waiting(mut op) =
                    std::mem::replace(&mut this.0, PendingState::Done)
                else {
                    unreachable!()
                };
                op.index = usize::MAX;
                let buf = op
                    .data
                    .take()
                    .expect("op data is present until completion")
                    .buf;
                Poll::Ready(buf)
            }
            PendingState::Ready(_) => match std::mem::replace(&mut this.0, PendingState::Done) {
                PendingState::Ready(buf) => Poll::Ready(buf),
                _ => unreachable!(),
            },
            PendingState::Done => panic!("ZcPending polled after completion"),
        }
    }
}

impl<T: IoBuf> fmt::Debug for ZcPending<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.0 {
            PendingState::Waiting(_) => "Waiting",
            PendingState::Ready(_) => "Ready",
            PendingState::Done => "Done",
        };
        f.debug_struct("ZcPending").field("state", &state).finish()
    }
}
