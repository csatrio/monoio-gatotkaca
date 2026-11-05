use std::{
    io,
    mem::{transmute, MaybeUninit},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
};

use io_uring::{opcode, types};
use {
    crate::net::unix::SocketAddr as UnixSocketAddr,
    libc::{sockaddr_in, sockaddr_in6, sockaddr_storage, socklen_t, AF_INET, AF_INET6},
};

use super::{super::shared_fd::SharedFd, submit::SubmitResult, Op, OpAble};
use crate::{
    buf::{IoBufMut, IoVecBufMut, IoVecMeta, MsgMeta},
    BufResult,
};

pub(crate) struct Recv<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: T,

    /// `MSG_*` flags.
    flags: libc::c_int,

    /// SQE ioprio, e.g. `IORING_RECVSEND_POLL_FIRST`.
    ioprio: u16,
}

/// `IORING_RECVSEND_POLL_FIRST`: poll before trying the receive.
const RECVSEND_POLL_FIRST: u16 = 1 << 0;

impl<T: IoBufMut> Op<Recv<T>> {
    pub(crate) fn recv(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        Self::recv_with(fd, buf, 0, false)
    }

    /// Receive with `MSG_*` flags; `poll_first` sets `IORING_RECVSEND_POLL_FIRST`.
    pub(crate) fn recv_with(
        fd: SharedFd,
        buf: T,
        flags: libc::c_int,
        poll_first: bool,
    ) -> SubmitResult<Self, T> {
        let ioprio = if poll_first { RECVSEND_POLL_FIRST } else { 0 };
        Op::submit_or_return(Recv {
            fd,
            buf,
            flags,
            ioprio,
        })
        .map_err(|(e, d)| (e, d.buf))
    }

    pub(crate) async fn result(self) -> BufResult<usize, T> {
        let complete = self.await;
        let res = complete.meta.result.map(|v| v.into_inner() as _);
        let mut buf = complete.data.buf;

        if let Ok(n) = res {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                buf.set_init(n);
            }
        }
        (res, buf)
    }
}

impl<T: IoBufMut> OpAble for Recv<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::Recv::new(
            types::Fd(self.fd.raw_fd()),
            self.buf.write_ptr(),
            self.buf.bytes_total() as _,
        )
        .flags(self.flags)
        .ioprio(self.ioprio)
        .build()
    }
}

pub(crate) struct RecvMsg<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
    /// For multiple message recv in the future
    pub(crate) info: Box<(MaybeUninit<sockaddr_storage>, IoVecMeta, MsgMeta)>,
}

impl<T: IoBufMut> Op<RecvMsg<T>> {
    pub(crate) fn recv_msg(fd: SharedFd, mut buf: T) -> SubmitResult<Self, T> {
        let mut info: Box<(MaybeUninit<sockaddr_storage>, IoVecMeta, MsgMeta)> =
            Box::new((MaybeUninit::uninit(), IoVecMeta::from(&mut buf), unsafe {
                std::mem::zeroed()
            }));

        {
            info.2.msg_iov = info.1.write_iovec_ptr();
            info.2.msg_iovlen = info.1.write_iovec_len() as _;
            info.2.msg_name = &mut info.0 as *mut _ as *mut libc::c_void;
            info.2.msg_namelen = std::mem::size_of::<sockaddr_storage>() as socklen_t;
        }

        Op::submit_or_return(RecvMsg { fd, buf, info }).map_err(|(e, d)| (e, d.buf))
    }

    pub(crate) async fn wait(self) -> BufResult<(usize, SocketAddr), T> {
        let complete = self.await;
        let res = complete.meta.result.map(|v| v.into_inner() as _);
        let mut buf = complete.data.buf;

        let res = res.and_then(|n| {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe { buf.set_init(n) };

            // No address was written (e.g. not an inet socket): report it, don't read garbage.
            if complete.data.info.2.msg_namelen == 0 {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            let storage = unsafe { complete.data.info.0.assume_init() };

            let addr = unsafe {
                match storage.ss_family as _ {
                    AF_INET => {
                        // Safety: if the ss_family field is AF_INET then storage must be a
                        // sockaddr_in.
                        let addr: &sockaddr_in = transmute(&storage);
                        let ip = Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes());
                        let port = u16::from_be(addr.sin_port);
                        SocketAddr::V4(SocketAddrV4::new(ip, port))
                    }
                    AF_INET6 => {
                        // Safety: if the ss_family field is AF_INET6 then storage must be a
                        // sockaddr_in6.
                        let addr: &sockaddr_in6 = transmute(&storage);
                        let ip = Ipv6Addr::from(addr.sin6_addr.s6_addr);
                        let port = u16::from_be(addr.sin6_port);
                        let scope_id = addr.sin6_scope_id;
                        SocketAddr::V6(SocketAddrV6::new(ip, port, addr.sin6_flowinfo, scope_id))
                    }
                    _ => return Err(io::ErrorKind::InvalidInput.into()),
                }
            };

            Ok((n, addr))
        });
        (res, buf)
    }
}

impl<T: IoBufMut> OpAble for RecvMsg<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::RecvMsg::new(types::Fd(self.fd.raw_fd()), &mut *self.info.2).build()
    }
}

pub(crate) struct RecvMsgUnix<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
    /// For multiple message recv in the future
    pub(crate) info: Box<(MaybeUninit<sockaddr_storage>, IoVecMeta, libc::msghdr)>,
}

impl<T: IoBufMut> Op<RecvMsgUnix<T>> {
    pub(crate) fn recv_msg_unix(fd: SharedFd, mut buf: T) -> SubmitResult<Self, T> {
        let mut info: Box<(MaybeUninit<sockaddr_storage>, IoVecMeta, libc::msghdr)> =
            Box::new((MaybeUninit::uninit(), IoVecMeta::from(&mut buf), unsafe {
                std::mem::zeroed()
            }));

        info.2.msg_iov = info.1.write_iovec_ptr();
        info.2.msg_iovlen = info.1.write_iovec_len() as _;
        info.2.msg_name = &mut info.0 as *mut _ as *mut libc::c_void;
        info.2.msg_namelen = std::mem::size_of::<sockaddr_storage>() as socklen_t;

        Op::submit_or_return(RecvMsgUnix { fd, buf, info }).map_err(|(e, d)| (e, d.buf))
    }

    pub(crate) async fn wait(self) -> BufResult<(usize, UnixSocketAddr), T> {
        let complete = self.await;
        let res = complete.meta.result.map(|v| v.into_inner() as _);
        let mut buf = complete.data.buf;

        let res = res.map(|n| {
            let storage = unsafe { complete.data.info.0.assume_init() };
            let name_len = complete.data.info.2.msg_namelen;

            let addr = unsafe {
                let addr: &libc::sockaddr_un = transmute(&storage);
                UnixSocketAddr::from_parts(*addr, name_len)
            };

            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                buf.set_init(n);
            }

            (n, addr)
        });
        (res, buf)
    }
}

impl<T: IoBufMut> OpAble for RecvMsgUnix<T> {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::RecvMsg::new(types::Fd(self.fd.raw_fd()), &mut self.info.2 as *mut _).build()
    }
}
