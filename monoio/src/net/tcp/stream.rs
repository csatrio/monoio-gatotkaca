use std::{
    cell::UnsafeCell,
    future::Future,
    io,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use {
    libc::{shutdown, AF_INET, AF_INET6, SHUT_WR, SOCK_STREAM},
    std::os::unix::prelude::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
};

use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut},
    driver::{op::Op, shared_fd::SharedFd},
    io::{
        as_fd::{AsReadFd, AsWriteFd, SharedFdWrapper},
        operation_canceled, AsyncReadRent, AsyncWriteRent, CancelHandle, CancelableAsyncReadRent,
        CancelableAsyncWriteRent, Split,
    },
    net::{flags, MsgFlags},
    BufResult,
};

/// Custom tcp connect options
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct TcpConnectOpts {
    /// TCP fast open.
    pub tcp_fast_open: bool,
}

impl Default for TcpConnectOpts {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl TcpConnectOpts {
    /// Create a default TcpConnectOpts.
    #[inline]
    pub const fn new() -> Self {
        Self {
            tcp_fast_open: false,
        }
    }

    /// Specify FastOpen
    /// Note: This option only works for linux 4.1+
    /// and macos/ios 9.0+.
    /// If it is enabled, the connection will be
    /// established on the first call to write.
    #[must_use]
    #[inline]
    pub fn tcp_fast_open(mut self, fast_open: bool) -> Self {
        self.tcp_fast_open = fast_open;
        self
    }
}
/// TcpStream
pub struct TcpStream {
    pub(super) fd: SharedFd,
    meta: StreamMeta,
}

/// TcpStream is safe to split to two parts
unsafe impl Split for TcpStream {}

impl TcpStream {
    pub(crate) fn from_shared_fd(fd: SharedFd) -> Self {
        let meta = StreamMeta::new(fd.raw_fd());
        #[cfg(feature = "zero-copy")]
        // enable SOCK_ZEROCOPY
        meta.set_zero_copy();

        Self { fd, meta }
    }

    /// Open a TCP connection to a remote host.
    /// Note: This function may block the current thread while resolution is
    /// performed.
    // TODO(chihai): Fix it, maybe spawn_blocking like tokio.
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        // TODO(chihai): loop for all addrs
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::other("empty address"))?;

        Self::connect_addr(addr).await
    }

    /// Establish a connection to the specified `addr`.
    pub async fn connect_addr(addr: SocketAddr) -> io::Result<Self> {
        const DEFAULT_OPTS: TcpConnectOpts = TcpConnectOpts {
            tcp_fast_open: false,
        };
        Self::connect_addr_with_config(addr, &DEFAULT_OPTS).await
    }

    /// Establish a connection to the specified `addr` with given config.
    pub async fn connect_addr_with_config(
        addr: SocketAddr,
        opts: &TcpConnectOpts,
    ) -> io::Result<Self> {
        let domain = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };
        let socket = crate::net::new_socket(domain, SOCK_STREAM)?;
        #[allow(unused_mut)]
        let mut tfo = opts.tcp_fast_open;

        if tfo {
            super::tfo::try_set_tcp_fastopen_connect(&socket);
        }
        let completion = Op::connect(SharedFd::new(socket)?, addr, tfo)?.await;
        completion.meta.result?;

        let stream = TcpStream::from_shared_fd(completion.data.fd);
        Ok(stream)
    }

    /// Return the local address that this stream is bound to.
    #[inline]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.meta.local_addr()
    }

    /// Return the remote address that this stream is connected to.
    #[inline]
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.meta.peer_addr()
    }

    /// Get the value of the `TCP_NODELAY` option on this socket.
    #[inline]
    pub fn nodelay(&self) -> io::Result<bool> {
        self.meta.no_delay()
    }

    /// Set the value of the `TCP_NODELAY` option on this socket.
    #[inline]
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.meta.set_no_delay(nodelay)
    }

    /// Set the value of the `SO_KEEPALIVE` option on this socket.
    #[inline]
    pub fn set_tcp_keepalive(
        &self,
        time: Option<Duration>,
        interval: Option<Duration>,
        retries: Option<u32>,
    ) -> io::Result<()> {
        self.meta.set_tcp_keepalive(time, interval, retries)
    }

    /// Creates new `TcpStream` from a `std::net::TcpStream`.
    pub fn from_std(stream: std::net::TcpStream) -> io::Result<Self> {
        let fd = stream.as_raw_fd();
        match SharedFd::new(fd) {
            Ok(shared) => {
                let _ = stream.into_raw_fd();
                Ok(Self::from_shared_fd(shared))
            }
            Err(e) => Err(e),
        }
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

    /// Attempts a zero-copy write using `IORING_OP_SEND_ZC` (Linux 6.0+).
    ///
    /// When supported by the kernel, NIC and socket configuration, the kernel
    /// may pin the userspace buffer and DMA directly from it, avoiding a copy
    /// into the socket buffer. The buffer is then held until the kernel
    /// signals it is safe to release via a notification CQE.
    ///
    /// In practice, `SEND_ZC` is best-effort: it may fall back to a regular
    /// send (with copying) or fail with errors such as `EOPNOTSUPP` or
    /// `EINVAL`. Callers should treat this like a normal write and must not
    /// assume that a successful call always performed true zero-copy.
    ///
    /// This is most effective for large writes (e.g. 10KB+).
    pub async fn write_zc<T: IoBuf>(&self, buf: T) -> BufResult<usize, T> {
        let op = match Op::send_zc(self.fd.clone(), buf) {
            Ok(op) => op,
            Err((e, buf)) => return (Err(e), buf),
        };
        op.result().await
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
}

impl AsReadFd for TcpStream {
    #[inline]
    fn as_reader_fd(&mut self) -> &SharedFdWrapper {
        SharedFdWrapper::new(&self.fd)
    }
}

impl AsWriteFd for TcpStream {
    #[inline]
    fn as_writer_fd(&mut self) -> &SharedFdWrapper {
        SharedFdWrapper::new(&self.fd)
    }
}

impl IntoRawFd for TcpStream {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self.fd
            .try_unwrap()
            .expect("unexpected multiple reference to rawfd")
    }
}
impl AsRawFd for TcpStream {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd.raw_fd()
    }
}

impl std::fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpStream").field("fd", &self.fd).finish()
    }
}

impl AsyncWriteRent for TcpStream {
    #[inline]
    fn write<T: IoBuf>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        // Submit the write operation
        let op = Op::send(self.fd.clone(), buf);
        async move {
            match op {
                Ok(op) => op.result().await,
                Err((e, buf)) => (Err(e), buf),
            }
        }
    }

    #[inline]
    fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> impl Future<Output = BufResult<usize, T>> {
        let op = Op::writev(self.fd.clone(), buf_vec);
        async move {
            match op {
                Ok(op) => op.result().await,
                Err((e, buf)) => (Err(e), buf),
            }
        }
    }

    #[inline]
    fn flush(&mut self) -> impl Future<Output = std::io::Result<()>> {
        // Tcp stream does not need flush.
        std::future::ready(Ok(()))
    }

    fn shutdown(&mut self) -> impl Future<Output = std::io::Result<()>> {
        // We could use shutdown op here, which requires kernel 5.11+.
        // However, for simplicity, we just close the socket using direct syscall.
        let fd = self.as_raw_fd();
        let res = match unsafe { shutdown(fd, SHUT_WR) } {
            -1 => Err(io::Error::last_os_error()),
            _ => Ok(()),
        };
        std::future::ready(res)
    }
}

impl CancelableAsyncWriteRent for TcpStream {
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
        // Tcp stream does not need flush.
        Ok(())
    }

    fn cancelable_shutdown(&mut self, _c: CancelHandle) -> impl Future<Output = io::Result<()>> {
        // We could use shutdown op here, which requires kernel 5.11+.
        // However, for simplicity, we just close the socket using direct syscall.
        let fd = self.as_raw_fd();
        let res = match unsafe { shutdown(fd, SHUT_WR) } {
            -1 => Err(io::Error::last_os_error()),
            _ => Ok(()),
        };
        std::future::ready(res)
    }
}

impl AsyncReadRent for TcpStream {
    #[inline]
    fn read<T: IoBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        // Submit the read operation
        let op = Op::recv(self.fd.clone(), buf);
        async move {
            match op {
                Ok(op) => op.result().await,
                Err((e, buf)) => (Err(e), buf),
            }
        }
    }

    #[inline]
    fn readv<T: IoVecBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        // Submit the read operation
        let op = Op::readv(self.fd.clone(), buf);
        async move {
            match op {
                Ok(op) => op.result().await,
                Err((e, buf)) => (Err(e), buf),
            }
        }
    }
}

impl CancelableAsyncReadRent for TcpStream {
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

struct StreamMeta {
    socket: Option<socket2::Socket>,
    meta: UnsafeCell<Meta>,
}

#[derive(Debug, Default, Clone)]
struct Meta {
    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl StreamMeta {
    fn new(fd: RawFd) -> Self {
        Self {
            socket: unsafe { Some(socket2::Socket::from_raw_fd(fd)) },
            meta: Default::default(),
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        let meta = unsafe { &mut *self.meta.get() };
        if let Some(addr) = meta.local_addr {
            return Ok(addr);
        }

        let ret = self
            .socket
            .as_ref()
            .unwrap()
            .local_addr()
            .map(|addr| addr.as_socket().expect("tcp socket is expected"));
        if let Ok(addr) = ret {
            meta.local_addr = Some(addr);
        }
        ret
    }

    fn peer_addr(&self) -> io::Result<SocketAddr> {
        let meta = unsafe { &mut *self.meta.get() };
        if let Some(addr) = meta.peer_addr {
            return Ok(addr);
        }

        let ret = self
            .socket
            .as_ref()
            .unwrap()
            .peer_addr()
            .map(|addr| addr.as_socket().expect("tcp socket is expected"));
        if let Ok(addr) = ret {
            meta.peer_addr = Some(addr);
        }
        ret
    }

    fn no_delay(&self) -> io::Result<bool> {
        self.socket.as_ref().unwrap().tcp_nodelay()
    }

    fn set_no_delay(&self, no_delay: bool) -> io::Result<()> {
        self.socket.as_ref().unwrap().set_tcp_nodelay(no_delay)
    }

    #[allow(unused_variables)]
    fn set_tcp_keepalive(
        &self,
        time: Option<Duration>,
        interval: Option<Duration>,
        retries: Option<u32>,
    ) -> io::Result<()> {
        let mut t = socket2::TcpKeepalive::new();
        if let Some(time) = time {
            t = t.with_time(time)
        }
        if let Some(interval) = interval {
            t = t.with_interval(interval)
        }
        if let Some(retries) = retries {
            t = t.with_retries(retries)
        }
        self.socket.as_ref().unwrap().set_tcp_keepalive(&t)
    }

    #[cfg(feature = "zero-copy")]
    fn set_zero_copy(&self) {
        unsafe {
            let fd = self.socket.as_ref().unwrap().as_raw_fd();
            let v: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ZEROCOPY,
                &v as *const _ as *const _,
                std::mem::size_of::<libc::c_int>() as _,
            );
        }
    }
}

impl Drop for StreamMeta {
    fn drop(&mut self) {
        let socket = self.socket.take().unwrap();
        let _ = socket.into_raw_fd();
    }
}
