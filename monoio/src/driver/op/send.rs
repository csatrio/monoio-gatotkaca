use std::net::SocketAddr;

use io_uring::{opcode, types};
use socket2::SockAddr;

use super::{super::shared_fd::SharedFd, submit::SubmitResult, Op, OpAble};
use crate::net::unix::SocketAddr as UnixSocketAddr;
use crate::{
    buf::{IoBuf, IoVecBuf, IoVecBufMut, IoVecMeta, MsgMeta},
    BufResult,
};

pub(crate) struct Send<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,

    pub(crate) buf: T,

    /// Extra `MSG_*` flags, ORed with `MSG_NOSIGNAL`.
    flags: libc::c_int,
}

impl<T: IoBuf> Op<Send<T>> {
    pub(crate) fn send(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        Self::send_with_flags(fd, buf, 0)
    }

    pub(crate) fn send_with_flags(
        fd: SharedFd,
        buf: T,
        flags: libc::c_int,
    ) -> SubmitResult<Self, T> {
        Op::submit_or_return(Send { fd, buf, flags }).map_err(|(e, d)| (e, d.buf))
    }

    pub(crate) async fn result(self) -> BufResult<usize, T> {
        let complete = self.await;
        (
            complete.meta.result.map(|v| v.into_inner() as _),
            complete.data.buf,
        )
    }
}

impl<T: IoBuf> OpAble for Send<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        #[allow(deprecated)]
        #[cfg(feature = "zero-copy")]
        fn zero_copy_flag_guard<T: IoBuf>(buf: &T) -> libc::c_int {
            // TODO: use libc const after supported.
            const MSG_ZEROCOPY: libc::c_int = 0x4000000;
            // According to Linux's documentation, zero copy introduces extra overhead and
            // is only considered effective for at writes over around 10 KB.
            // see also: https://www.kernel.org/doc/html/v4.16/networking/msg_zerocopy.html
            const MSG_ZEROCOPY_THRESHOLD: usize = 10 * 1024 * 1024;
            if buf.bytes_init() >= MSG_ZEROCOPY_THRESHOLD {
                libc::MSG_NOSIGNAL as libc::c_int | MSG_ZEROCOPY
            } else {
                libc::MSG_NOSIGNAL as libc::c_int
            }
        }

        #[cfg(feature = "zero-copy")]
        let flags = zero_copy_flag_guard(&self.buf);
        #[cfg(not(feature = "zero-copy"))]
        #[allow(deprecated)]
        let flags = libc::MSG_NOSIGNAL as libc::c_int;

        opcode::Send::new(
            types::Fd(self.fd.raw_fd()),
            self.buf.read_ptr(),
            self.buf.bytes_init() as _,
        )
        .flags(flags | self.flags)
        .build()
    }
}

pub(crate) struct SendMsg<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
    /// For multiple message send in the future
    pub(crate) info: Box<(Option<SockAddr>, IoVecMeta, MsgMeta)>,
}

impl<T: IoBuf> Op<SendMsg<T>> {
    pub(crate) fn send_msg(
        fd: SharedFd,
        buf: T,
        socket_addr: Option<SocketAddr>,
    ) -> SubmitResult<Self, T> {
        let mut info: Box<(Option<SockAddr>, IoVecMeta, MsgMeta)> = Box::new((
            socket_addr.map(Into::into),
            IoVecMeta::from(&buf),
            unsafe { std::mem::zeroed() },
        ));

        {
            info.2.msg_iov = info.1.write_iovec_ptr();
            info.2.msg_iovlen = info.1.write_iovec_len() as _;
            match info.0.as_ref() {
                Some(socket_addr) => {
                    info.2.msg_name = socket_addr.as_ptr() as *mut libc::c_void;
                    info.2.msg_namelen = socket_addr.len();
                }
                None => {
                    info.2.msg_name = std::ptr::null_mut();
                    info.2.msg_namelen = 0;
                }
            }
        }

        Op::submit_or_return(SendMsg { fd, buf, info }).map_err(|(e, d)| (e, d.buf))
    }

    pub(crate) async fn wait(self) -> BufResult<usize, T> {
        let complete = self.await;
        let res = complete.meta.result.map(|v| v.into_inner() as _);
        let buf = complete.data.buf;
        (res, buf)
    }
}

impl<T: IoBuf> OpAble for SendMsg<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        #[allow(deprecated)]
        const FLAGS: u32 = libc::MSG_NOSIGNAL as u32;
        opcode::SendMsg::new(types::Fd(self.fd.raw_fd()), &*self.info.2)
            .flags(FLAGS)
            .build()
    }
}

pub(crate) struct SendMsgUnix<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
    /// For multiple message send in the future
    pub(crate) info: Box<(Option<UnixSocketAddr>, IoVecMeta, libc::msghdr)>,
}

impl<T: IoBuf> Op<SendMsgUnix<T>> {
    pub(crate) fn send_msg_unix(
        fd: SharedFd,
        buf: T,
        socket_addr: Option<UnixSocketAddr>,
    ) -> SubmitResult<Self, T> {
        let mut info: Box<(Option<UnixSocketAddr>, IoVecMeta, libc::msghdr)> =
            Box::new((socket_addr, IoVecMeta::from(&buf), unsafe {
                std::mem::zeroed()
            }));

        info.2.msg_iov = info.1.write_iovec_ptr();
        info.2.msg_iovlen = info.1.write_iovec_len() as _;

        match info.0.as_ref() {
            Some(socket_addr) => {
                info.2.msg_name = socket_addr.as_ptr() as *mut libc::c_void;
                info.2.msg_namelen = socket_addr.len();
            }
            None => {
                info.2.msg_name = std::ptr::null_mut();
                info.2.msg_namelen = 0;
            }
        }

        Op::submit_or_return(SendMsgUnix { fd, buf, info }).map_err(|(e, d)| (e, d.buf))
    }

    pub(crate) async fn wait(self) -> BufResult<usize, T> {
        let complete = self.await;
        let res = complete.meta.result.map(|v| v.into_inner() as _);
        let buf = complete.data.buf;
        (res, buf)
    }
}

impl<T: IoBuf> OpAble for SendMsgUnix<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        #[allow(deprecated)]
        const FLAGS: u32 = libc::MSG_NOSIGNAL as u32;
        opcode::SendMsg::new(types::Fd(self.fd.raw_fd()), &mut self.info.2 as *mut _)
            .flags(FLAGS)
            .build()
    }
}

/// Zero-copy send using IORING_OP_SEND_ZC (Linux 6.0+).
///
/// This operation produces two CQEs: the first indicates send completion,
/// and the second (notification) signals that the kernel has released the buffer.
/// The future only resolves after both CQEs arrive, ensuring buffer safety.
pub(crate) struct SendZc<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,

    pub(crate) buf: T,
}

impl<T: IoBuf> Op<SendZc<T>> {
    pub(crate) fn send_zc(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        Op::submit_or_return(SendZc { fd, buf }).map_err(|(e, d)| (e, d.buf))
    }

    pub(crate) async fn result(self) -> BufResult<usize, T> {
        let complete = self.await;
        (
            complete.meta.result.map(|v| v.into_inner() as _),
            complete.data.buf,
        )
    }
}

impl<T: IoBuf> OpAble for SendZc<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        #[allow(deprecated)]
        const FLAGS: i32 = libc::MSG_NOSIGNAL;

        opcode::SendZc::new(
            types::Fd(self.fd.raw_fd()),
            self.buf.read_ptr(),
            self.buf.bytes_init() as _,
        )
        .flags(FLAGS)
        .build()
    }
}

/// Vectored send on a connected socket: `sendmsg` with no address and `MSG_NOSIGNAL`.
///
/// Unlike `writev`, a reset peer gives `EPIPE` and no `SIGPIPE`.
pub(crate) struct SendMsgVec<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,

    /// The in-flight buffers; `msg` points at their iovecs.
    pub(crate) buf_vec: T,

    /// Boxed so the kernel sees a stable address after the op moves.
    msg: Box<libc::msghdr>,

    /// Extra `MSG_*` flags, ORed with `MSG_NOSIGNAL`.
    flags: u32,
}

impl<T: IoVecBuf> Op<SendMsgVec<T>> {
    pub(crate) fn send_msg_vec(
        fd: SharedFd,
        buf_vec: T,
        flags: libc::c_int,
    ) -> SubmitResult<Self, T> {
        // Safety: an all-zero msghdr is valid (no name, no control). `uring_op` sets the iovecs.
        let msg: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
        Op::submit_or_return(SendMsgVec {
            fd,
            buf_vec,
            msg,
            flags: flags as u32,
        })
        .map_err(|(e, d)| (e, d.buf_vec))
    }

    pub(crate) async fn result(self) -> BufResult<usize, T> {
        let complete = self.await;
        (
            complete.meta.result.map(|v| v.into_inner() as _),
            complete.data.buf_vec,
        )
    }
}

impl<T: IoVecBuf> OpAble for SendMsgVec<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        #[allow(deprecated)]
        const NOSIGNAL: u32 = libc::MSG_NOSIGNAL as u32;
        self.msg.msg_iov = self.buf_vec.read_iovec_ptr() as *mut _;
        self.msg.msg_iovlen = self.buf_vec.read_iovec_len() as _;
        opcode::SendMsg::new(types::Fd(self.fd.raw_fd()), &*self.msg)
            .flags(NOSIGNAL | self.flags)
            .build()
    }
}
