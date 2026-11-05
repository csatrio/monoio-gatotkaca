//! Flag sets for socket ops.

use std::ops::{BitOr, BitOrAssign};

use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf},
    driver::{op::Op, shared_fd::SharedFd},
    BufResult,
};

macro_rules! flag_set {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
        pub struct $name(libc::c_int);

        impl $name {
            /// No flags.
            pub const EMPTY: Self = Self(0);

            /// Builds a set from raw libc bits. The kernel rejects or ignores unknown bits.
            #[inline]
            pub const fn from_bits(bits: libc::c_int) -> Self {
                Self(bits)
            }

            /// The raw libc bits.
            #[inline]
            pub const fn bits(self) -> libc::c_int {
                self.0
            }

            /// True if every flag of `other` is set in `self`.
            #[inline]
            pub const fn contains(self, other: Self) -> bool {
                self.0 & other.0 == other.0
            }
        }

        impl BitOr for $name {
            type Output = Self;

            #[inline]
            fn bitor(self, rhs: Self) -> Self {
                Self(self.0 | rhs.0)
            }
        }

        impl BitOrAssign for $name {
            #[inline]
            fn bitor_assign(&mut self, rhs: Self) {
                self.0 |= rhs.0;
            }
        }
    };
}

flag_set! {
    /// Flags for the socket created by `accept_with` (the `accept4(2)` flags).
    ///
    /// ```
    /// use monoio::net::AcceptFlags;
    /// let flags = AcceptFlags::NONBLOCK | AcceptFlags::CLOEXEC;
    /// assert!(flags.contains(AcceptFlags::CLOEXEC));
    /// ```
    AcceptFlags
}

impl AcceptFlags {
    /// `SOCK_NONBLOCK`: the new socket is non-blocking.
    pub const NONBLOCK: Self = Self(libc::SOCK_NONBLOCK);
    /// `SOCK_CLOEXEC`: the new socket is closed on `exec`.
    pub const CLOEXEC: Self = Self(libc::SOCK_CLOEXEC);
}

flag_set! {
    /// `MSG_*` flags for `send_with_flags`, `recv_with_flags` and `send_vectored_with_flags`.
    ///
    /// Sends always add `MSG_NOSIGNAL`, so a reset peer gives `EPIPE` and no `SIGPIPE`.
    ///
    /// ```
    /// use monoio::net::MsgFlags;
    /// let flags = MsgFlags::MORE | MsgFlags::DONTWAIT;
    /// assert!(flags.contains(MsgFlags::MORE));
    /// ```
    MsgFlags
}

impl MsgFlags {
    /// `MSG_MORE`: more data follows; the kernel may hold this send to fill a segment.
    pub const MORE: Self = Self(libc::MSG_MORE);
    /// `MSG_WAITALL`: a receive waits until the whole buffer is filled (or EOF / error).
    pub const WAITALL: Self = Self(libc::MSG_WAITALL);
    /// `MSG_DONTWAIT`: fail with `EAGAIN` instead of waiting.
    pub const DONTWAIT: Self = Self(libc::MSG_DONTWAIT);
    /// `MSG_NOSIGNAL`: no `SIGPIPE` on a broken connection (sends set it anyway).
    #[allow(deprecated)]
    pub const NOSIGNAL: Self = Self(libc::MSG_NOSIGNAL);
    /// `MSG_PEEK`: receive without removing the data from the queue.
    pub const PEEK: Self = Self(libc::MSG_PEEK);
    /// `MSG_OOB`: send or receive out-of-band data.
    pub const OOB: Self = Self(libc::MSG_OOB);
    /// `MSG_TRUNC`: on receive, return the real packet length even if it was longer.
    pub const TRUNC: Self = Self(libc::MSG_TRUNC);
}

// Shared op helpers for stream sockets (TCP and Unix).

/// `send` with extra flags (`MSG_NOSIGNAL` is always added).
pub(crate) async fn send_with_flags<T: IoBuf>(
    fd: &SharedFd,
    buf: T,
    flags: MsgFlags,
) -> BufResult<usize, T> {
    match Op::send_with_flags(fd.clone(), buf, flags.bits()) {
        Ok(op) => op.result().await,
        Err((e, buf)) => (Err(e), buf),
    }
}

/// `recv` with flags, optionally with `IORING_RECVSEND_POLL_FIRST`.
pub(crate) async fn recv_with_flags<T: IoBufMut>(
    fd: &SharedFd,
    buf: T,
    flags: MsgFlags,
    poll_first: bool,
) -> BufResult<usize, T> {
    match Op::recv_with(fd.clone(), buf, flags.bits(), poll_first) {
        Ok(op) => op.result().await,
        Err((e, buf)) => (Err(e), buf),
    }
}

/// Vectored `sendmsg` with extra flags (`MSG_NOSIGNAL` is always added).
pub(crate) async fn send_vectored_with_flags<T: IoVecBuf>(
    fd: &SharedFd,
    buf_vec: T,
    flags: MsgFlags,
) -> BufResult<usize, T> {
    match Op::send_msg_vec(fd.clone(), buf_vec, flags.bits()) {
        Ok(op) => op.result().await,
        Err((e, buf_vec)) => (Err(e), buf_vec),
    }
}
