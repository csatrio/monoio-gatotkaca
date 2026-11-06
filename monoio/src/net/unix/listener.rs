use std::{
    io,
    mem::ManuallyDrop,
    os::unix::prelude::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    path::Path,
};

use super::{socket_addr::SocketAddr, UnixStream};
use crate::{
    driver::{
        op::{accept_multi::BatchAcceptor, Op},
        shared_fd::SharedFd,
    },
    io::{stream::Stream, CancelHandle},
    net::{AcceptFlags, ListenerOpts},
};

/// UnixListener
pub struct UnixListener {
    pub(super) fd: SharedFd,
    sys_listener: Option<std::os::unix::net::UnixListener>,
    // The multishot accept of `accept_batch`.
    pub(super) batch: BatchAcceptor,
}

impl UnixListener {
    pub(crate) fn from_shared_fd(fd: SharedFd) -> Self {
        let sys_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(fd.raw_fd()) };
        Self {
            fd,
            sys_listener: Some(sys_listener),
            batch: BatchAcceptor::default(),
        }
    }

    /// Creates a new `UnixListener` bound to the specified socket with custom
    /// config.
    pub fn bind_with_config<P: AsRef<Path>>(
        path: P,
        config: &ListenerOpts,
    ) -> io::Result<UnixListener> {
        let sys_listener =
            socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
        let addr = socket2::SockAddr::unix(path)?;

        if config.reuse_port {
            // TODO: properly handle this. Warn?
            // this seems to cause an error on current (>6.x) kernels:
            // sys_listener.set_reuse_port(true)?;
        }
        if config.reuse_addr {
            sys_listener.set_reuse_address(true)?;
        }
        if let Some(send_buf_size) = config.send_buf_size {
            sys_listener.set_send_buffer_size(send_buf_size)?;
        }
        if let Some(recv_buf_size) = config.recv_buf_size {
            sys_listener.set_recv_buffer_size(recv_buf_size)?;
        }

        sys_listener.bind(&addr)?;
        sys_listener.listen(config.backlog)?;

        let fd = SharedFd::new(sys_listener.into_raw_fd())?;

        Ok(Self::from_shared_fd(fd))
    }

    /// Creates a new `UnixListener` bound to the specified socket with default
    /// config.
    pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<UnixListener> {
        Self::bind_with_config(path, &ListenerOpts::default())
    }

    /// Accept
    pub async fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
        self.accept_inner(0, None).await
    }

    /// Accept a connection and set `flags` on the new socket in the same step (`accept4`).
    /// See [`TcpListener::accept_with`](crate::net::TcpListener::accept_with).
    pub async fn accept_with(&self, flags: AcceptFlags) -> io::Result<(UnixStream, SocketAddr)> {
        self.accept_inner(flags.bits(), None).await
    }

    /// Cancelable accept
    pub async fn cancelable_accept(&self, c: CancelHandle) -> io::Result<(UnixStream, SocketAddr)> {
        self.accept_inner(0, Some(c)).await
    }

    /// Cancelable [`accept_with`](Self::accept_with).
    pub async fn cancelable_accept_with(
        &self,
        flags: AcceptFlags,
        c: CancelHandle,
    ) -> io::Result<(UnixStream, SocketAddr)> {
        self.accept_inner(flags.bits(), Some(c)).await
    }

    async fn accept_inner(
        &self,
        flags: libc::c_int,
        c: Option<CancelHandle>,
    ) -> io::Result<(UnixStream, SocketAddr)> {
        if let Some(c) = &c {
            if c.canceled() {
                return Err(crate::io::operation_canceled());
            }
        }
        let op = Op::accept_with(&self.fd, flags)?;
        let _guard = c.map(|c| c.associate_op(op.op_canceller()));

        // Await the completion of the event
        let completion = op.await;

        // Convert fd
        let fd = completion.meta.result?;

        // Construct stream
        let stream = UnixStream::from_shared_fd(SharedFd::new(fd.into_inner() as _)?);

        // Construct SocketAddr
        let mut storage = unsafe { std::mem::MaybeUninit::assume_init(completion.data.addr.0) };
        let storage: *mut libc::sockaddr_storage = &mut storage as *mut _;
        let raw_addr_un: libc::sockaddr_un = unsafe { *storage.cast() };
        let raw_addr_len = completion.data.addr.1;

        let addr = SocketAddr::from_parts(raw_addr_un, raw_addr_len);

        Ok((stream, addr))
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

    /// Creates new `UnixListener` from a `std::os::unix::net::UnixListener`.
    pub fn from_std(sys_listener: std::os::unix::net::UnixListener) -> io::Result<Self> {
        match SharedFd::new(sys_listener.as_raw_fd()) {
            Ok(shared) => Ok(Self {
                fd: shared,
                sys_listener: Some(sys_listener),
                batch: BatchAcceptor::default(),
            }),
            Err(e) => Err(e),
        }
    }
}

impl Stream for UnixListener {
    type Item = io::Result<(UnixStream, SocketAddr)>;

    #[inline]
    async fn next(&mut self) -> Option<Self::Item> {
        Some(self.accept().await)
    }
}

impl std::fmt::Debug for UnixListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixListener")
            .field("fd", &self.fd)
            .finish()
    }
}

impl IntoRawFd for UnixListener {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        let mut this = ManuallyDrop::new(self);
        let _ = this.sys_listener.take().unwrap().into_raw_fd();
        // SAFETY: `this` is never used or dropped again; each field is dropped or moved out
        // exactly once (`sys_listener` is `None` now and owns nothing).
        let fd = unsafe {
            std::ptr::drop_in_place(&mut this.batch);
            std::ptr::read(&this.fd)
        };

        // Fails while a multishot accept (`accept_multi`, `accept_batch`) still holds the fd.
        fd.try_unwrap()
            .expect("unexpected multiple reference to rawfd")
    }
}

impl AsRawFd for UnixListener {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd.raw_fd()
    }
}

impl Drop for UnixListener {
    #[inline]
    fn drop(&mut self) {
        let _ = self.sys_listener.take().unwrap().into_raw_fd();
    }
}
