use std::net::SocketAddr;

use io_uring::opcode;
use socket2::SockAddr;

use super::{super::shared_fd::SharedFd, submit::SubmitResult, Completion, Op, OpAble};
use crate::driver::{
    msghdr_pool::{vectorized_send, PooledMsgHdr, IORING_SEND_VECTORIZED},
    shared_fd::{sqe_fd, TxFd},
};
use crate::net::unix::SocketAddr as UnixSocketAddr;
use crate::{
    buf::{IoBuf, IoVecBuf, IoVecBufMut, IoVecMeta, MsgMeta},
    io::futures::{BufOp, SendFuture},
    BufResult,
};

pub(crate) struct Send<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight. Counts as a send in flight on the socket.
    fd: TxFd,

    pub(crate) buf: T,

    /// Extra `MSG_*` flags, ORed with `MSG_NOSIGNAL`.
    flags: libc::c_int,

    /// The eager part of [`send_eager`]. Low 31 bits ([`EAGER_SENT`]): the bytes of `buf` that
    /// a direct `send(2)` already took; the SQE covers the rest and `complete` adds them back.
    /// Top bit ([`EAGER_POLL_FIRST`]): the socket reported it is full, so the SQE carries
    /// `IORING_RECVSEND_POLL_FIRST`. Zero for a plain send.
    ///
    /// One `u32` in what was padding: `Send<Vec<u8>>` stays at 40 bytes, `Send<Bytes>` at 48.
    eager: u32,
}

/// See [`Send::eager`].
const EAGER_POLL_FIRST: u32 = 1 << 31;
/// See [`Send::eager`].
const EAGER_SENT: u32 = EAGER_POLL_FIRST - 1;

impl<T: IoBuf> Op<Send<T>> {
    // Inlined into the caller's future: whether these land inline decided 1-3% of the user
    // instructions per request in round 3 (r3-op, r3-slot, r3-eager notes).
    #[inline(always)]
    pub(crate) fn send(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        Self::send_with_flags(fd, buf, 0)
    }

    #[inline(always)]
    pub(crate) fn send_with_flags(
        fd: SharedFd,
        buf: T,
        flags: libc::c_int,
    ) -> SubmitResult<Self, T> {
        Op::submit_or_return(Send {
            fd: TxFd::new(fd),
            buf,
            flags,
            eager: 0,
        })
        .map_err(|(e, d)| (e, d.buf))
    }

    pub(crate) async fn result(self) -> BufResult<usize, T> {
        <Send<T> as BufOp>::complete(self.await)
    }
}

impl<T: IoBuf> BufOp for Send<T> {
    type Buf = T;

    #[inline]
    fn complete(complete: Completion<Self>) -> BufResult<usize, T> {
        let res = complete.meta.result.map(|v| v.into_inner() as usize);
        // A plain send: one test more than before `eager` existed.
        let sent = complete.data.eager & EAGER_SENT;
        if sent != 0 {
            return (after_eager(res, sent as usize), complete.data.buf);
        }
        (res, complete.data.buf)
    }
}

/// The result of a send whose first `sent` bytes went out with a direct `send(2)` and the rest
/// through the ring (`ring` is that part's result): the count is relative to the whole buffer.
/// If the ring part failed, the bytes of the direct part are reported as a short send, like
/// `send(2)` does when an error hits after it copied something; the error shows on the next
/// send.
#[cold]
#[inline(never)]
fn after_eager(ring: std::io::Result<usize>, sent: usize) -> std::io::Result<usize> {
    Ok(ring.map_or(sent, |n| n + sent))
}

/// `send_eager` of the stream sockets: a direct, non-blocking `send(2)` from the calling
/// thread, and the ring only for what that did not take.
///
/// - The socket has no fd (direct-only) or a ring send is in flight on it: a plain ring send
///   (a direct send could overtake the one in flight).
/// - Everything was sent, or the send failed with a real error: a [`SendFuture`] that is
///   resolved already. No op slot, no SQE, no CQE, and no `Pending`.
/// - The socket buffer is full (`EAGAIN`, or a short count): a ring send of the rest, with
///   `IORING_RECVSEND_POLL_FIRST` since an immediate retry would only get `EAGAIN` again. The
///   result counts from the start of `buf`.
///
/// Never inlined: it ends in a syscall, where a call does not matter, and inlined into
/// `AsyncWriteRent::write` it would grow every caller of `write`, also those that never turn
/// the eager switch on.
#[inline(never)]
pub(crate) fn send_eager<T: IoBuf>(fd: &SharedFd, buf: T, flags: libc::c_int) -> SendFuture<T> {
    if !fd.can_send_now() {
        return SendFuture::new(Op::send_with_flags(fd.clone(), buf, flags));
    }
    let len = buf.bytes_init();
    // One `send(2)` takes at most `MAX_RW_COUNT` (2^31 − 4096) bytes; never ask for more, so
    // the count fits the 31 bits of `Send::eager`.
    let now = len.min(EAGER_SENT as usize);
    // SAFETY: `IoBuf`: `read_ptr()` is valid for `bytes_init()` bytes while `buf` lives.
    let bytes = unsafe { std::slice::from_raw_parts(buf.read_ptr(), now) };
    match fd.send_now(bytes, flags) {
        Ok(n) if n >= len => SendFuture::ready(Ok(n), buf),
        Ok(n) => send_rest(fd, buf, flags, n),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => send_rest(fd, buf, flags, 0),
        Err(e) => SendFuture::ready(Err(e), buf),
    }
}

/// The ring part of [`send_eager`]: the socket took only the first `sent` bytes of `buf`.
#[cold]
fn send_rest<T: IoBuf>(fd: &SharedFd, buf: T, flags: libc::c_int, sent: usize) -> SendFuture<T> {
    let poll_first = if poll_first_supported() {
        EAGER_POLL_FIRST
    } else {
        0
    };
    let data = Send {
        fd: TxFd::new(fd.clone()),
        buf,
        flags,
        eager: sent as u32 | poll_first,
    };
    match Op::submit_or_return(data) {
        Ok(op) => SendFuture::new(Ok(op)),
        // The SQ stayed full. What the eager part sent is sent: a short send.
        Err((_, data)) if sent != 0 => SendFuture::ready(Ok(sent), data.buf),
        Err((e, data)) => SendFuture::ready(Err(e), data.buf),
    }
}

/// Whether the kernel knows `IORING_RECVSEND_POLL_FIRST` (5.19; older kernels reject such an
/// SQE with `EINVAL`). There is no feature bit for it, so this asks for `IORING_OP_SOCKET`,
/// which came with the same release. Probed once per process.
fn poll_first_supported() -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};

    #[cold]
    fn probe(known: &AtomicU8) -> bool {
        let mut probe = io_uring::Probe::new();
        // SAFETY: the closure only registers a probe; it neither re-enters the runtime nor
        // touches the queues.
        let registered = unsafe {
            crate::driver::raw::with_ring(|ring| ring.submitter().register_probe(&mut probe))
        };
        let yes = registered.is_ok() && probe.is_supported(opcode::Socket::CODE);
        known.store(if yes { 1 } else { 2 }, Ordering::Relaxed);
        yes
    }

    static KNOWN: AtomicU8 = AtomicU8::new(0);
    match KNOWN.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => probe(&KNOWN),
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
            // The threshold is deliberately far above that (10 MiB): `MSG_ZEROCOPY` on a plain
            // send has no completion notification here (nobody reads the error queue), so it
            // stays rare. `SEND_ZC` (`write_zc`, `send_zc_*`) has its own notification and uses
            // `net::ZC_MIN_LEN` (16 KiB) in `send_auto`.
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

        /// The rest of an eager send: skip what the direct send took, and poll first if the
        /// socket was full. Out of line, so that a plain send pays one test for it.
        #[cold]
        #[inline(never)]
        fn rest(ptr: *const u8, len: usize, eager: u32) -> (*const u8, usize, u16) {
            let sent = (eager & EAGER_SENT) as usize;
            // `EAGER_POLL_FIRST` is the top bit: 1 = `IORING_RECVSEND_POLL_FIRST`.
            (ptr.wrapping_add(sent), len - sent, (eager >> 31) as u16)
        }

        let (ptr, len) = (self.buf.read_ptr(), self.buf.bytes_init());
        let (ptr, len, ioprio) = match self.eager {
            0 => (ptr, len, 0),
            eager => rest(ptr, len, eager),
        };
        sqe_fd!(self.fd, |fd| opcode::Send::new(fd, ptr, len as _)
            .flags(flags | self.flags)
            .ioprio(ioprio)
            .build())
    }
}

/// `sendmsg` with an optional address, for UDP sockets.
///
/// Not counted as a send in flight (`TxFd`): datagram sockets have no `try_send`, and each
/// datagram is its own message.
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
        sqe_fd!(self.fd, |fd| opcode::SendMsg::new(fd, &*self.info.2)
            .flags(FLAGS)
            .build())
    }
}

/// `sendmsg` with an optional address, for Unix datagram and seqpacket sockets. Not counted as
/// a send in flight, like [`SendMsg`].
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
        sqe_fd!(self.fd, |fd| opcode::SendMsg::new(
            fd,
            &mut self.info.2 as *mut _
        )
        .flags(FLAGS)
        .build())
    }
}

/// Zero-copy send using IORING_OP_SEND_ZC (Linux 6.0+).
///
/// This operation produces two CQEs: the first indicates send completion,
/// and the second (notification) signals that the kernel has released the buffer.
/// The future only resolves after both CQEs arrive, ensuring buffer safety.
pub(crate) struct SendZc<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight. Counts as a send in flight on the socket, until the
    /// notification CQE.
    fd: TxFd,

    pub(crate) buf: T,
}

impl<T: IoBuf> Op<SendZc<T>> {
    pub(crate) fn send_zc(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
        Op::submit_or_return(SendZc {
            fd: TxFd::new(fd),
            buf,
        })
        .map_err(|(e, d)| (e, d.buf))
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
    const KIND: super::OpKind = super::OpKind::ZeroCopy;
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        #[allow(deprecated)]
        const FLAGS: i32 = libc::MSG_NOSIGNAL;

        let (ptr, len) = (self.buf.read_ptr(), self.buf.bytes_init() as _);
        sqe_fd!(self.fd, |fd| opcode::SendZc::new(fd, ptr, len)
            .flags(FLAGS)
            .build())
    }
}

/// Vectored send on a connected socket, with `MSG_NOSIGNAL` and no address: a plain `SEND`
/// with `IORING_SEND_VECTORIZED` (6.17+) or, on older kernels, `sendmsg` with a pooled
/// `msghdr` (`driver::msghdr_pool` decides, once per process).
///
/// Unlike `writev`, a reset peer gives `EPIPE` and no `SIGPIPE`.
pub(crate) struct SendMsgVec<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight. Counts as a send in flight on the socket.
    fd: TxFd,

    /// The in-flight buffers; the SQE (or `msg`) points at their iovecs.
    pub(crate) buf_vec: T,

    /// `None`: a vectorized `SEND`, the SQE itself points at the iovecs. `Some`: `sendmsg`;
    /// the `msghdr` is on the heap, from the thread's free list, since the kernel reads it at
    /// the enter, after the op data moved into its future. Back on the list when the op data
    /// is dropped, which is after the CQE on every path (see `driver::msghdr_pool`).
    msg: Option<PooledMsgHdr>,

    /// Extra `MSG_*` flags, ORed with `MSG_NOSIGNAL`.
    flags: u32,
}

impl<T: IoVecBuf> Op<SendMsgVec<T>> {
    #[inline]
    pub(crate) fn send_msg_vec(
        fd: SharedFd,
        buf_vec: T,
        flags: libc::c_int,
    ) -> SubmitResult<Self, T> {
        let msg = if vectorized_send() {
            None
        } else {
            Some(PooledMsgHdr::take())
        };
        Op::submit_or_return(SendMsgVec {
            fd: TxFd::new(fd),
            buf_vec,
            msg,
            flags: flags as u32,
        })
        .map_err(|(e, d)| (e, d.buf_vec))
    }

    pub(crate) async fn result(self) -> BufResult<usize, T> {
        <SendMsgVec<T> as BufOp>::complete(self.await)
    }
}

impl<T: IoVecBuf> BufOp for SendMsgVec<T> {
    type Buf = T;

    #[inline]
    fn complete(complete: Completion<Self>) -> BufResult<usize, T> {
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
        let (iov, len) = (self.buf_vec.read_iovec_ptr(), self.buf_vec.read_iovec_len());
        match &mut self.msg {
            None => sqe_fd!(self.fd, |fd| opcode::Send::new(fd, iov.cast(), len as _)
                .flags((NOSIGNAL | self.flags) as _)
                .ioprio(IORING_SEND_VECTORIZED)
                .build()),
            Some(msg) => {
                let hdr = msg.get_mut();
                hdr.msg_iov = iov as *mut _;
                hdr.msg_iovlen = len as _;
                sqe_fd!(self.fd, |fd| opcode::SendMsg::new(fd, msg.as_ptr())
                    .flags(NOSIGNAL | self.flags)
                    .build())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    /// The in-flight guard and the eager field are free: the guard is a `SharedFd`, and `eager`
    /// sits in what was padding. So a dropped in-flight send still fits the inline storage of
    /// the op slot (48 bytes; `Send<Vec<u8>>` also fits 40).
    #[test]
    fn send_data_did_not_grow() {
        assert_eq!(size_of::<TxFd>(), size_of::<SharedFd>());
        assert_eq!(size_of::<Send<&'static [u8]>>(), 32);
        assert_eq!(size_of::<Send<Vec<u8>>>(), 40);
        #[cfg(feature = "bytes")]
        {
            assert_eq!(size_of::<Send<bytes::Bytes>>(), 48);
            assert_eq!(size_of::<Send<bytes::BytesMut>>(), 48);
        }
        assert_eq!(size_of::<SendZc<Vec<u8>>>(), 32);
    }

    /// The optional pooled `msghdr` is one pointer (niche), like the box before it. What
    /// decides whether a dropped in-flight vectored send fits the 48 bytes of inline storage
    /// is the buffer type: a bare iovec `Vec` fits, `VecBuf` (two `Vec`s) and every
    /// `IoVecArray` (its buffers are inline) do not.
    #[test]
    fn send_msg_vec_data_sizes() {
        use crate::buf::{IoVecArray, VecBuf};
        assert_eq!(size_of::<Option<PooledMsgHdr>>(), size_of::<usize>());
        assert_eq!(size_of::<SendMsgVec<Vec<libc::iovec>>>(), 48);
        assert_eq!(size_of::<SendMsgVec<VecBuf>>(), 72);
        assert_eq!(size_of::<SendMsgVec<IoVecArray<Vec<u8>, 1>>>(), 64);
        assert_eq!(size_of::<SendMsgVec<IoVecArray<Vec<u8>, 4>>>(), 136);
        #[cfg(feature = "bytes")]
        assert_eq!(size_of::<SendMsgVec<IoVecArray<bytes::Bytes, 4>>>(), 168);
    }

    #[test]
    fn eager_part_is_added_or_kept() {
        assert_eq!(after_eager(Ok(5), 3).unwrap(), 8);
        let reset = std::io::Error::from_raw_os_error(libc::ECONNRESET);
        assert_eq!(after_eager(Err(reset), 3).unwrap(), 3);
    }
}
