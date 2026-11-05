use std::{
    cell::UnsafeCell,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs},
};

use {
    libc::{sockaddr_in, sockaddr_in6, AF_INET, AF_INET6},
    std::os::unix::prelude::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
};

use super::stream::TcpStream;
use crate::{
    driver::{op::Op, shared_fd::SharedFd},
    io::{stream::Stream, CancelHandle},
    net::{AcceptFlags, ListenerOpts},
};

/// TcpListener
pub struct TcpListener {
    pub(super) fd: SharedFd,
    sys_listener: Option<std::net::TcpListener>,
    meta: UnsafeCell<ListenerMeta>,
}

impl TcpListener {
    #[allow(unreachable_code, clippy::diverging_sub_expression, unused_variables)]
    pub(crate) fn from_shared_fd(fd: SharedFd) -> Self {
        let sys_listener = unsafe { std::net::TcpListener::from_raw_fd(fd.raw_fd()) };
        Self {
            fd,
            sys_listener: Some(sys_listener),
            meta: UnsafeCell::new(ListenerMeta::default()),
        }
    }

    /// Bind to address with config
    pub fn bind_with_config<A: ToSocketAddrs>(addr: A, opts: &ListenerOpts) -> io::Result<Self> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::other("empty address"))?;

        let domain = if addr.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };
        let sys_listener =
            socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;

        let addr = socket2::SockAddr::from(addr);
        if opts.reuse_port {
            sys_listener.set_reuse_port(true)?;
        }
        if opts.reuse_addr {
            sys_listener.set_reuse_address(true)?;
        }
        if let Some(send_buf_size) = opts.send_buf_size {
            sys_listener.set_send_buffer_size(send_buf_size)?;
        }
        if let Some(recv_buf_size) = opts.recv_buf_size {
            sys_listener.set_recv_buffer_size(recv_buf_size)?;
        }
        if opts.tcp_fast_open {
            super::tfo::set_tcp_fastopen(&sys_listener, opts.backlog)?;
        }
        sys_listener.bind(&addr)?;
        sys_listener.listen(opts.backlog)?;

        let fd = sys_listener.into_raw_fd();

        Ok(Self::from_shared_fd(SharedFd::new(fd)?))
    }

    /// Bind to address
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        const DEFAULT_CFG: ListenerOpts = ListenerOpts::new();
        Self::bind_with_config(addr, &DEFAULT_CFG)
    }

    /// Accept
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept_inner(0, None).await
    }

    /// Accept a connection and set `flags` on the new socket in the same step (`accept4`).
    ///
    /// For example `AcceptFlags::NONBLOCK | AcceptFlags::CLOEXEC` saves the `fcntl` calls that
    /// code handing the socket to another event loop would make. Monoio itself does not need
    /// `O_NONBLOCK`; plain [`accept`](Self::accept) sets no flags. The peer address is returned
    /// as usual.
    pub async fn accept_with(&self, flags: AcceptFlags) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept_inner(flags.bits(), None).await
    }

    /// Cancelable accept
    pub async fn cancelable_accept(&self, c: CancelHandle) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept_inner(0, Some(c)).await
    }

    /// Cancelable [`accept_with`](Self::accept_with).
    pub async fn cancelable_accept_with(
        &self,
        flags: AcceptFlags,
        c: CancelHandle,
    ) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept_inner(flags.bits(), Some(c)).await
    }

    async fn accept_inner(
        &self,
        flags: libc::c_int,
        c: Option<CancelHandle>,
    ) -> io::Result<(TcpStream, SocketAddr)> {
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
        let stream = TcpStream::from_shared_fd(SharedFd::new(fd.into_inner() as _)?);

        // Construct SocketAddr
        let storage = completion.data.addr.0.as_ptr();
        let addr = unsafe {
            match (*storage).ss_family as _ {
                AF_INET => {
                    // Safety: if the ss_family field is AF_INET then storage must be a sockaddr_in.
                    let addr: &sockaddr_in = &*(storage as *const sockaddr_in);
                    let ip = Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes());
                    let port = u16::from_be(addr.sin_port);
                    SocketAddr::V4(SocketAddrV4::new(ip, port))
                }
                AF_INET6 => {
                    // Safety: if the ss_family field is AF_INET6 then storage must be a
                    // sockaddr_in6.
                    let addr: &sockaddr_in6 = &*(storage as *const sockaddr_in6);
                    let ip = Ipv6Addr::from(addr.sin6_addr.s6_addr);
                    let port = u16::from_be(addr.sin6_port);
                    let scope_id = addr.sin6_scope_id;
                    SocketAddr::V6(SocketAddrV6::new(ip, port, addr.sin6_flowinfo, scope_id))
                }
                _ => {
                    return Err(io::ErrorKind::InvalidInput.into());
                }
            }
        };

        Ok((stream, addr))
    }

    /// Returns the local address that this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let meta = self.meta.get();
        if let Some(addr) = unsafe { &*meta }.local_addr {
            return Ok(addr);
        }
        self.sys_listener
            .as_ref()
            .unwrap()
            .local_addr()
            .inspect(|&addr| {
                unsafe { &mut *meta }.local_addr = Some(addr);
            })
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

    /// Creates new `TcpListener` from a `std::net::TcpListener`.
    pub fn from_std(stdl: std::net::TcpListener) -> io::Result<Self> {
        let fd = stdl.as_raw_fd();
        match SharedFd::new(fd) {
            Ok(shared) => {
                let _ = stdl.into_raw_fd();
                Ok(Self::from_shared_fd(shared))
            }
            Err(e) => Err(e),
        }
    }
}

impl Stream for TcpListener {
    type Item = io::Result<(TcpStream, SocketAddr)>;

    #[inline]
    async fn next(&mut self) -> Option<Self::Item> {
        Some(self.accept().await)
    }
}

impl std::fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpListener").field("fd", &self.fd).finish()
    }
}

impl AsRawFd for TcpListener {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd.raw_fd()
    }
}

impl Drop for TcpListener {
    #[inline]
    fn drop(&mut self) {
        let listener = self.sys_listener.take().unwrap();
        let _ = listener.into_raw_fd();
    }
}

#[derive(Debug, Default, Clone)]
struct ListenerMeta {
    local_addr: Option<SocketAddr>,
}
