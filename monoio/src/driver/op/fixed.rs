//! `IORING_OP_READ_FIXED` / `IORING_OP_WRITE_FIXED` on registered buffers
//! ([`FixedBuf`], [`FrozenFixedBuf`](crate::uring::FrozenFixedBuf)), and their named futures.
//!
//! A buffer registered on another ring than the current one (a pool made with `new_in` for a
//! different runtime) is still accepted: the op then falls back to the plain `READ`/`WRITE`
//! opcode, since its `buf_index` means nothing to this ring.

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::opcode;

use super::{super::shared_fd::SharedFd, submit::SubmitResult, Completion, Op, OpAble};
use crate::{
    buf::IoBufMut,
    driver::fixed_buf::{on_current_ring, FixedBuf, FixedIoBuf},
    driver::shared_fd::sqe_fd,
    io::futures::BufOp,
    BufResult,
};

/// The offset for "no offset / current position" (sockets, pipes).
pub(crate) const NO_OFFSET: u64 = -1i64 as u64;

/// The file and offset of a fixed-buffer op. These ops serve both files (an offset) and
/// sockets ([`NO_OFFSET`]): without an offset the op counts as a send (`TX`) or receive in
/// flight on the socket, like [`InFlight`](crate::driver::shared_fd::InFlight) (see there for
/// why the count is never early); with one (a file, which has no `try_send`) nothing is
/// counted. The offset tells the two apart, so the op data needs no extra field.
struct Target<const TX: bool> {
    /// Holds a strong ref to the FD, preventing the file from being closed while the operation
    /// is in-flight.
    fd: SharedFd,
    offset: u64,
}

impl<const TX: bool> Target<TX> {
    #[inline]
    fn new(fd: SharedFd, offset: u64) -> Self {
        if offset == NO_OFFSET {
            fd.begin_op::<TX>();
        }
        Self { fd, offset }
    }
}

impl<const TX: bool> Drop for Target<TX> {
    #[inline]
    fn drop(&mut self) {
        if self.offset == NO_OFFSET {
            self.fd.end_op::<TX>();
        }
    }
}

pub(crate) struct ReadFixed {
    at: Target<false>,
    pub(crate) buf: FixedBuf,
    fixed: bool,
}

impl Op<ReadFixed> {
    pub(crate) fn read_fixed(
        fd: SharedFd,
        buf: FixedBuf,
        offset: u64,
    ) -> SubmitResult<Self, FixedBuf> {
        let fixed = on_current_ring(&buf);
        Op::submit_or_return(ReadFixed {
            at: Target::new(fd, offset),
            buf,
            fixed,
        })
        .map_err(|(e, d)| (e, d.buf))
    }
}

impl OpAble for ReadFixed {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let ptr = self.buf.write_ptr();
        let len = self.buf.bytes_total() as u32;
        let offset = self.at.offset;
        sqe_fd!(self.at.fd, |fd| if self.fixed {
            opcode::ReadFixed::new(fd, ptr, len, self.buf.buf_index())
                .offset(offset)
                .build()
        } else {
            opcode::Read::new(fd, ptr, len).offset(offset).build()
        })
    }
}

impl BufOp for ReadFixed {
    type Buf = FixedBuf;

    fn complete(complete: Completion<Self>) -> BufResult<usize, FixedBuf> {
        let res = complete.meta.result.map(|v| v.into_inner() as usize);
        let mut buf = complete.data.buf;
        if let Ok(n) = res {
            // SAFETY: the kernel wrote `n` bytes to the buffer.
            unsafe { buf.set_init(n) };
        }
        (res, buf)
    }
}

pub(crate) struct WriteFixed<B> {
    at: Target<true>,
    pub(crate) buf: B,
    fixed: bool,
}

impl<B: FixedIoBuf> Op<WriteFixed<B>> {
    pub(crate) fn write_fixed(fd: SharedFd, buf: B, offset: u64) -> SubmitResult<Self, B> {
        let fixed = on_current_ring(&buf);
        Op::submit_or_return(WriteFixed {
            at: Target::new(fd, offset),
            buf,
            fixed,
        })
        .map_err(|(e, d)| (e, d.buf))
    }
}

impl<B: FixedIoBuf> OpAble for WriteFixed<B> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let ptr = self.buf.read_ptr();
        let len = self.buf.bytes_init() as u32;
        let offset = self.at.offset;
        sqe_fd!(self.at.fd, |fd| if self.fixed {
            opcode::WriteFixed::new(fd, ptr, len, self.buf.buf_index())
                .offset(offset)
                .build()
        } else {
            opcode::Write::new(fd, ptr, len).offset(offset).build()
        })
    }
}

impl<B: FixedIoBuf> BufOp for WriteFixed<B> {
    type Buf = B;

    fn complete(complete: Completion<Self>) -> BufResult<usize, B> {
        (
            complete.meta.result.map(|v| v.into_inner() as usize),
            complete.data.buf,
        )
    }
}

enum State<D: BufOp> {
    /// Submitted; the op owns the buffer.
    Running(Op<D>),
    /// Submission failed; returned on the first poll.
    Failed(io::Error, D::Buf),
    /// The output was returned.
    Done,
}

/// Same state machine as the futures of [`crate::io::futures`].
struct FixedOpFuture<D: BufOp>(State<D>);

impl<D: BufOp> FixedOpFuture<D> {
    fn new(submitted: SubmitResult<Op<D>, D::Buf>) -> Self {
        Self(match submitted {
            Ok(op) => State::Running(op),
            Err((e, buf)) => State::Failed(e, buf),
        })
    }

    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<BufResult<usize, D::Buf>> {
        match &mut self.0 {
            State::Running(op) => {
                let completion = ready!(Pin::new(op).poll(cx));
                self.0 = State::Done;
                Poll::Ready(D::complete(completion))
            }
            State::Failed(..) => match std::mem::replace(&mut self.0, State::Done) {
                State::Failed(e, buf) => Poll::Ready((Err(e), buf)),
                _ => unreachable!(),
            },
            State::Done => panic!("op future polled after completion"),
        }
    }

    fn is_terminated(&self) -> bool {
        matches!(self.0, State::Done)
    }

    fn cancel(&self) {
        if let State::Running(op) = &self.0 {
            // Safety: `op` is alive and unfinished, so its slab slot still belongs to it.
            unsafe { op.op_canceller().cancel() }
        }
    }

    fn state_name(&self) -> &'static str {
        match self.0 {
            State::Running(_) => "Running",
            State::Failed(..) => "Failed",
            State::Done => "Done",
        }
    }
}

macro_rules! fixed_future {
    ($(#[$meta:meta])* $name:ident $(<$T:ident: $Bound:ident>)? => $data:ty, $buf:ty) => {
        $(#[$meta])*
        ///
        /// `Unpin` and `'static`; resolves to `(io::Result<usize>, buffer)`. The SQE is pushed
        /// when the creating method is called. Same drop and cancel rules as the futures in
        /// [`monoio::io::futures`](crate::io::futures).
        #[must_use = "the op is already submitted; dropping this cancels it and loses the buffer"]
        pub struct $name$(<$T: $Bound>)?(FixedOpFuture<$data>);

        impl$(<$T: $Bound>)? $name$(<$T>)? {
            #[inline]
            pub(crate) fn new(submitted: SubmitResult<Op<$data>, $buf>) -> Self {
                Self(FixedOpFuture::new(submitted))
            }

            /// Polls the op without pinning (token-style API). Returns `Ready((res, buf))` once.
            ///
            /// # Panics
            ///
            /// If called again after it returned `Ready`.
            #[inline]
            pub fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<BufResult<usize, $buf>> {
                self.0.poll_result(cx)
            }

            /// True once the output was returned; polling again would panic.
            #[inline]
            pub fn is_terminated(&self) -> bool {
                self.0.is_terminated()
            }

            /// Asks the kernel to cancel the op. Keep polling: the result is then `ECANCELED`
            /// (os error 125) or the op's normal result, and you get the buffer back either way.
            #[inline]
            pub fn cancel(&self) {
                self.0.cancel()
            }
        }

        impl$(<$T: $Bound>)? Future for $name$(<$T>)? {
            type Output = BufResult<usize, $buf>;

            #[inline]
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.0.poll_result(cx)
            }
        }

        impl$(<$T: $Bound>)? fmt::Debug for $name$(<$T>)? {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("state", &self.0.state_name())
                    .finish()
            }
        }
    };
}

fixed_future! {
    /// An in-flight `IORING_OP_READ_FIXED` into a [`FixedBuf`], returned by
    /// [`File::read_fixed_at_op`](crate::fs::File::read_fixed_at_op) and
    /// [`TcpStream::recv_fixed_op`](crate::net::TcpStream::recv_fixed_op).
    ReadFixedFuture => ReadFixed, FixedBuf
}

fixed_future! {
    /// An in-flight `IORING_OP_WRITE_FIXED` from a [`FixedBuf`] or
    /// [`FrozenFixedBuf`](crate::uring::FrozenFixedBuf), returned by
    /// [`File::write_fixed_at_op`](crate::fs::File::write_fixed_at_op) and
    /// [`TcpStream::send_fixed_op`](crate::net::TcpStream::send_fixed_op).
    WriteFixedFuture<B: FixedIoBuf> => WriteFixed<B>, B
}

#[cfg(test)]
mod tests {
    use std::os::fd::IntoRawFd;

    use super::*;
    use crate::{driver::IoUringDriver, uring::FixedBufPool, RuntimeBuilder};

    fn opcode_of(mut data: impl OpAble) -> u8 {
        let entry = data.uring_op();
        // SAFETY: `Entry` is `repr(C)` around the SQE, whose first byte is the opcode.
        unsafe { *(&entry as *const io_uring::squeue::Entry).cast::<u8>() }
    }

    #[test]
    fn fixed_opcode_only_on_the_owning_ring() {
        // `SharedFd` closes its fd when dropped.
        let fd = || {
            let devnull = std::fs::File::open("/dev/null").unwrap();
            SharedFd::new_without_register(devnull.into_raw_fd())
        };
        let mut rt_a = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
        let Ok(pool) = FixedBufPool::new_in(rt_a.driver(), 1, 64) else {
            return;
        };
        let check = |want_fixed: bool| {
            let buf = pool.get().unwrap();
            let fixed = on_current_ring(&buf);
            assert_eq!(fixed, want_fixed);
            let write = WriteFixed {
                at: Target::new(fd(), 0),
                buf: buf.freeze(),
                fixed,
            };
            let code = opcode_of(write);
            let want = if want_fixed {
                opcode::WriteFixed::CODE
            } else {
                opcode::Write::CODE
            };
            assert_eq!(code, want);
            let read = ReadFixed {
                at: Target::new(fd(), 0),
                buf: pool.get().unwrap(),
                fixed,
            };
            let want = if want_fixed {
                opcode::ReadFixed::CODE
            } else {
                opcode::Read::CODE
            };
            assert_eq!(opcode_of(read), want);
        };
        rt_a.block_on(async { check(true) });
        let mut rt_b = RuntimeBuilder::<IoUringDriver>::new().build().unwrap();
        rt_b.block_on(async { check(false) });
        check(false);
    }

    /// Only the ops without an offset (sockets) count as in flight; a file op counts nothing.
    #[test]
    fn only_ops_without_offset_count() {
        let devnull = std::fs::File::open("/dev/null").unwrap();
        let fd = SharedFd::new_without_register(devnull.into_raw_fd());
        assert!(fd.can_send_now() && fd.can_recv_now());

        let file_write = Target::<true>::new(fd.clone(), 0);
        let file_read = Target::<false>::new(fd.clone(), 4096);
        assert!(fd.can_send_now() && fd.can_recv_now());
        drop((file_write, file_read));
        assert!(fd.can_send_now() && fd.can_recv_now());

        let sock_write = Target::<true>::new(fd.clone(), NO_OFFSET);
        assert!(!fd.can_send_now() && fd.can_recv_now());
        let sock_read = Target::<false>::new(fd.clone(), NO_OFFSET);
        assert!(!fd.can_send_now() && !fd.can_recv_now());
        drop(sock_write);
        assert!(fd.can_send_now() && !fd.can_recv_now());
        drop(sock_read);
        assert!(fd.can_send_now() && fd.can_recv_now());
    }
}
