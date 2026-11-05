use std::{
    future::Future,
    io::{self},
    os::unix::prelude::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    path::Path,
};

use super::{
    socket_addr::{local_addr, pair, peer_addr, socket_addr, SocketAddr},
    ucred::UCred,
};
use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut},
    driver::{op::Op, shared_fd::SharedFd},
    io::{
        as_fd::{AsReadFd, AsWriteFd, SharedFdWrapper},
        futures::{ReadvFuture, RecvFuture, SendFuture, WritevFuture},
        operation_canceled, AsyncReadRent, AsyncWriteRent, CancelHandle, CancelableAsyncReadRent,
        CancelableAsyncWriteRent, Split,
    },
    net::{flags, new_socket, MsgFlags},
    BufResult,
};

/// UnixStream
pub struct UnixStream {
    pub(super) fd: SharedFd,
}

/// UnixStream is safe to split to two parts
unsafe impl Split for UnixStream {}

impl UnixStream {
    pub(crate) fn from_shared_fd(fd: SharedFd) -> Self {
        Self { fd }
    }

    /// Connect UnixStream to a path.
    pub async fn connect<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let (addr, addr_len) = socket_addr(path.as_ref())?;
        Self::inner_connect(addr, addr_len).await
    }

    /// Connects the socket to an address.
    pub async fn connect_addr(addr: SocketAddr) -> io::Result<Self> {
        let (addr, addr_len) = addr.into_parts();
        Self::inner_connect(addr, addr_len).await
    }

    #[inline(always)]
    async fn inner_connect(
        sockaddr: libc::sockaddr_un,
        socklen: libc::socklen_t,
    ) -> io::Result<Self> {
        let socket = new_socket(libc::AF_UNIX, libc::SOCK_STREAM)?;
        let op = Op::connect_unix(SharedFd::new(socket)?, sockaddr, socklen)?;
        let completion = op.await;
        completion.meta.result?;

        let stream = Self::from_shared_fd(completion.data.fd);
        // getsockopt
        let sys_socket = unsafe { std::os::unix::net::UnixStream::from_raw_fd(stream.fd.raw_fd()) };
        let err = sys_socket.take_error();
        let _ = sys_socket.into_raw_fd();
        if let Some(e) = err? {
            return Err(e);
        }
        Ok(stream)
    }

    /// Put the socket into the runtime's fixed file table, so every op submitted from now on
    /// names the table slot instead of the fd. The fd stays open. See
    /// [`TcpStream::register_fixed`](crate::net::TcpStream::register_fixed).
    pub fn register_fixed(&self) -> io::Result<()> {
        self.fd.register_fixed()
    }

    /// Whether the socket is in the fixed file table.
    pub fn is_fixed(&self) -> bool {
        self.fd.fixed_slot().is_some()
    }

    /// Creates an unnamed pair of connected sockets.
    ///
    /// Returns two `UnixStream`s which are connected to each other.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (a, b) = pair(libc::SOCK_STREAM)?;
        Ok((Self::from_std(a)?, Self::from_std(b)?))
    }

    /// Sends `buf` with extra `MSG_*` flags, e.g. [`MsgFlags::MORE`] on a response head that a
    /// body follows. `MSG_NOSIGNAL` is always added.
    ///
    /// Like `write`, it may send fewer bytes than `buf` holds.
    pub async fn send_with_flags<T: IoBuf>(&self, buf: T, flags: MsgFlags) -> BufResult<usize, T> {
        flags::send_with_flags(&self.fd, buf, flags).await
    }

    /// Receives into `buf` with `MSG_*` flags, e.g. [`MsgFlags::WAITALL`] to fill the whole
    /// buffer (it still returns less on EOF, an error or a signal).
    pub async fn recv_with_flags<T: IoBufMut>(
        &self,
        buf: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        flags::recv_with_flags(&self.fd, buf, flags, false).await
    }

    /// Like [`recv_with_flags`](Self::recv_with_flags), with `IORING_RECVSEND_POLL_FIRST` set:
    /// the kernel waits for data before it tries the receive. Use it on connections that are
    /// probably idle (keep-alive waits), where the first try would just get `EAGAIN`.
    /// Kernel 5.19+; older kernels reject it with `EINVAL`.
    pub async fn recv_poll_first<T: IoBufMut>(
        &self,
        buf: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        flags::recv_with_flags(&self.fd, buf, flags, true).await
    }

    /// Vectored send through `sendmsg` with `MSG_NOSIGNAL`.
    ///
    /// Unlike `writev` (`IORING_OP_WRITEV`), a reset peer gives `EPIPE`/`ECONNRESET` and never
    /// raises `SIGPIPE`. Pair it with [`IoVecArray`](crate::buf::IoVecArray) to avoid an iovec
    /// allocation per write. It may send fewer bytes than the buffers hold.
    pub async fn send_vectored<T: IoVecBuf>(&self, buf_vec: T) -> BufResult<usize, T> {
        flags::send_vectored_with_flags(&self.fd, buf_vec, MsgFlags::EMPTY).await
    }

    /// [`send_vectored`](Self::send_vectored) with extra `MSG_*` flags.
    pub async fn send_vectored_with_flags<T: IoVecBuf>(
        &self,
        buf_vec: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        flags::send_vectored_with_flags(&self.fd, buf_vec, flags).await
    }

    // Named op futures (see [`crate::io::futures`]). Each pushes its SQE now and returns an
    // `Unpin`, `'static` future that a struct can store without boxing.

    /// Starts a receive into `buf` and returns it as a named [`RecvFuture`].
    ///
    /// Same SQE and result as [`AsyncReadRent::read`]; the future does not borrow `self`.
    /// Also usable as an op token: drive it with [`RecvFuture::poll_result`].
    #[inline]
    pub fn recv_op<T: IoBufMut>(&self, buf: T) -> RecvFuture<T> {
        RecvFuture::new(Op::recv(self.fd.clone(), buf))
    }

    /// [`recv_with_flags`](Self::recv_with_flags) as a named [`RecvFuture`].
    #[inline]
    pub fn recv_with_flags_op<T: IoBufMut>(&self, buf: T, flags: MsgFlags) -> RecvFuture<T> {
        RecvFuture::new(Op::recv_with(self.fd.clone(), buf, flags.bits(), false))
    }

    /// [`recv_poll_first`](Self::recv_poll_first) as a named [`RecvFuture`].
    #[inline]
    pub fn recv_poll_first_op<T: IoBufMut>(&self, buf: T, flags: MsgFlags) -> RecvFuture<T> {
        RecvFuture::new(Op::recv_with(self.fd.clone(), buf, flags.bits(), true))
    }

    /// Starts a send of `buf` (`MSG_NOSIGNAL`) and returns it as a named [`SendFuture`].
    ///
    /// Same SQE and result as [`AsyncWriteRent::write`]; it may send fewer bytes than `buf`
    /// holds.
    #[inline]
    pub fn send_op<T: IoBuf>(&self, buf: T) -> SendFuture<T> {
        SendFuture::new(Op::send(self.fd.clone(), buf))
    }

    /// [`send_with_flags`](Self::send_with_flags) as a named [`SendFuture`].
    #[inline]
    pub fn send_with_flags_op<T: IoBuf>(&self, buf: T, flags: MsgFlags) -> SendFuture<T> {
        SendFuture::new(Op::send_with_flags(self.fd.clone(), buf, flags.bits()))
    }

    /// Starts a vectored read (`IORING_OP_READV`) and returns it as a named [`ReadvFuture`].
    ///
    /// Same SQE and result as [`AsyncReadRent::readv`].
    #[inline]
    pub fn readv_op<T: IoVecBufMut>(&self, buf_vec: T) -> ReadvFuture<T> {
        ReadvFuture::new(Op::readv(self.fd.clone(), buf_vec))
    }

    /// Starts a vectored write (`IORING_OP_WRITEV`) and returns it as a named
    /// [`WritevFuture`].
    ///
    /// Same SQE and result as [`AsyncWriteRent::writev`].
    #[inline]
    pub fn writev_op<T: IoVecBuf>(&self, buf_vec: T) -> WritevFuture<T> {
        WritevFuture::new(Op::writev(self.fd.clone(), buf_vec))
    }

    /// Returns effective credentials of the process which called `connect` or
    /// `pair`.
    pub fn peer_cred(&self) -> io::Result<UCred> {
        super::ucred::get_peer_cred(self)
    }

    /// Creates new `UnixStream` from a `std::os::unix::net::UnixStream`.
    pub fn from_std(stream: std::os::unix::net::UnixStream) -> io::Result<Self> {
        match SharedFd::new(stream.as_raw_fd()) {
            Ok(shared) => {
                let _ = stream.into_raw_fd();
                Ok(Self::from_shared_fd(shared))
            }
            Err(e) => Err(e),
        }
    }

    /// Returns the socket address of the local half of this connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        local_addr(self.as_raw_fd())
    }

    /// Returns the socket address of the remote half of this connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        peer_addr(self.as_raw_fd())
    }

    /// Wait for read readiness.
    /// Note: Do not use it before every io. It is different from other runtimes!
    ///
    /// Everytime call to this method may pay a syscall cost.
    /// It pushes a PollAdd op to the ring.
    pub async fn readable(&self) -> io::Result<()> {
        let op = Op::poll_read(&self.fd)?;
        op.wait().await
    }

    /// Wait for write readiness.
    /// Note: Do not use it before every io. It is different from other runtimes!
    ///
    /// Everytime call to this method may pay a syscall cost.
    /// It pushes a PollAdd op to the ring.
    pub async fn writable(&self) -> io::Result<()> {
        let op = Op::poll_write(&self.fd)?;
        op.wait().await
    }
}

impl AsReadFd for UnixStream {
    #[inline]
    fn as_reader_fd(&mut self) -> &SharedFdWrapper {
        SharedFdWrapper::new(&self.fd)
    }
}

impl AsWriteFd for UnixStream {
    #[inline]
    fn as_writer_fd(&mut self) -> &SharedFdWrapper {
        SharedFdWrapper::new(&self.fd)
    }
}

impl IntoRawFd for UnixStream {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self.fd
            .try_unwrap()
            .expect("unexpected multiple reference to rawfd")
    }
}

impl AsRawFd for UnixStream {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd.raw_fd()
    }
}

impl std::fmt::Debug for UnixStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixStream").field("fd", &self.fd).finish()
    }
}

impl AsyncWriteRent for UnixStream {
    #[inline]
    fn write<T: IoBuf>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        // Submit the write operation
        self.send_op(buf)
    }

    #[inline]
    fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> impl Future<Output = BufResult<usize, T>> {
        self.writev_op(buf_vec)
    }

    #[inline]
    async fn flush(&mut self) -> std::io::Result<()> {
        // Unix stream does not need flush.
        Ok(())
    }

    fn shutdown(&mut self) -> impl Future<Output = std::io::Result<()>> {
        // We could use shutdown op here, which requires kernel 5.11+.
        // However, for simplicity, we just close the socket using direct syscall.
        let fd = self.as_raw_fd();
        async move {
            match unsafe { libc::shutdown(fd, libc::SHUT_WR) } {
                -1 => Err(io::Error::last_os_error()),
                _ => Ok(()),
            }
        }
    }
}

impl CancelableAsyncWriteRent for UnixStream {
    #[inline]
    async fn cancelable_write<T: IoBuf>(
        &mut self,
        buf: T,
        c: CancelHandle,
    ) -> crate::BufResult<usize, T> {
        let fd = self.fd.clone();

        if c.canceled() {
            return (Err(operation_canceled()), buf);
        }

        let op = match Op::send(fd, buf) {
            Ok(op) => op,
            Err((e, buf)) => return (Err(e), buf),
        };
        let _guard = c.associate_op(op.op_canceller());
        op.result().await
    }

    #[inline]
    async fn cancelable_writev<T: IoVecBuf>(
        &mut self,
        buf_vec: T,
        c: CancelHandle,
    ) -> crate::BufResult<usize, T> {
        let fd = self.fd.clone();

        if c.canceled() {
            return (Err(operation_canceled()), buf_vec);
        }

        let op = match Op::writev(fd.clone(), buf_vec) {
            Ok(op) => op,
            Err((e, buf)) => return (Err(e), buf),
        };
        let _guard = c.associate_op(op.op_canceller());
        op.result().await
    }

    #[inline]
    async fn cancelable_flush(&mut self, _c: CancelHandle) -> io::Result<()> {
        // Unix stream does not need flush.
        Ok(())
    }

    async fn cancelable_shutdown(&mut self, _c: CancelHandle) -> io::Result<()> {
        // We could use shutdown op here, which requires kernel 5.11+.
        // However, for simplicity, we just close the socket using direct syscall.
        let fd = self.as_raw_fd();
        match unsafe { libc::shutdown(fd, libc::SHUT_WR) } {
            -1 => Err(io::Error::last_os_error()),
            _ => Ok(()),
        }
    }
}

impl AsyncReadRent for UnixStream {
    #[inline]
    fn read<T: IoBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        // Submit the read operation
        self.recv_op(buf)
    }

    #[inline]
    fn readv<T: IoVecBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        // Submit the read operation
        self.readv_op(buf)
    }
}

impl CancelableAsyncReadRent for UnixStream {
    #[inline]
    async fn cancelable_read<T: IoBufMut>(
        &mut self,
        buf: T,
        c: CancelHandle,
    ) -> crate::BufResult<usize, T> {
        let fd = self.fd.clone();

        if c.canceled() {
            return (Err(operation_canceled()), buf);
        }

        let op = match Op::recv(fd, buf) {
            Ok(op) => op,
            Err((e, buf)) => return (Err(e), buf),
        };
        let _guard = c.associate_op(op.op_canceller());
        op.result().await
    }

    #[inline]
    async fn cancelable_readv<T: IoVecBufMut>(
        &mut self,
        buf: T,
        c: CancelHandle,
    ) -> crate::BufResult<usize, T> {
        let fd = self.fd.clone();

        if c.canceled() {
            return (Err(operation_canceled()), buf);
        }

        let op = match Op::readv(fd, buf) {
            Ok(op) => op,
            Err((e, buf)) => return (Err(e), buf),
        };
        let _guard = c.associate_op(op.op_canceller());
        op.result().await
    }
}
