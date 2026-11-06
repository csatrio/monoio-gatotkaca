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
    driver::{
        op::{accept_multi::BatchAcceptor, Op},
        shared_fd::SharedFd,
    },
    io::{stream::Stream, CancelHandle},
    net::{AcceptFlags, ListenerOpts},
};

/// A TCP listening socket.
///
/// # Writing an accept loop
///
/// A loop around [`accept`](Self::accept) has one accept SQE in flight, so it accepts **one
/// connection per scheduler round**: the accept completes, the task runs once in the next
/// round, and only then is the next accept submitted. With many busy connections a round is
/// long, and the kernel's accept queue grows while connections wait (with their client
/// timeouts running). Measured (round 2, gate G0) with 4096 connections and 1 MiB responses:
/// rounds of ~10 ms gave ~100 accepts/s, the accept queue reached 3,338 and ~6,200 requests
/// per run timed out. Accepting with a multishot accept removed every timeout at no
/// throughput cost.
///
/// Under load, take everything queued per wakeup:
///
/// ```no_run
/// # async fn serve(listener: monoio::net::TcpListener) -> std::io::Result<()> {
/// use monoio::net::AcceptFlags;
///
/// let mut conns = Vec::with_capacity(256);
/// loop {
///     // One multishot accept stays armed between calls (Linux 5.19+).
///     listener
///         .accept_batch_into(&mut conns, 256, AcceptFlags::EMPTY)
///         .await?;
///     for (stream, _peer) in conns.drain(..) {
///         monoio::spawn(async move { /* serve stream */ drop(stream) });
///     }
/// }
/// # }
/// ```
///
/// [`accept_batch`](Self::accept_batch) / [`accept_batch_into`](Self::accept_batch_into)
/// keep the multishot accept inside the listener. To own it, use
/// [`accept_multi`](Self::accept_multi) and [`AcceptStream::next_batch`](super::AcceptStream::next_batch)
/// (or `next()` per connection: still no SQE per connection, and queued connections are taken
/// one per poll without waiting for a new round trip). Both re-arm themselves when the kernel
/// ends the multishot without an error.
pub struct TcpListener {
    pub(super) fd: SharedFd,
    sys_listener: Option<std::net::TcpListener>,
    meta: UnsafeCell<ListenerMeta>,
    batch: BatchAcceptor,
}

impl TcpListener {
    #[allow(unreachable_code, clippy::diverging_sub_expression, unused_variables)]
    pub(crate) fn from_shared_fd(fd: SharedFd) -> Self {
        let sys_listener = unsafe { std::net::TcpListener::from_raw_fd(fd.raw_fd()) };
        Self {
            fd,
            sys_listener: Some(sys_listener),
            meta: UnsafeCell::new(ListenerMeta::default()),
            batch: BatchAcceptor::default(),
        }
    }

    /// Wait for at least one connection, then return every connection already accepted, up to
    /// `max`, with their peer addresses, from one poll. See "Writing an accept loop" above.
    ///
    /// The first call arms a multishot accept (Linux 5.19+) that stays armed in the listener:
    /// connections arriving between calls are accepted by the kernel and queued, and the next
    /// call returns them without a round trip. `flags` are set on every accepted socket; later
    /// calls must pass the same `flags` while it is armed (`InvalidInput` otherwise).
    ///
    /// - Allocates the returned `Vec`; [`accept_batch_into`](Self::accept_batch_into) reuses
    ///   yours.
    /// - Never returns an empty `Vec`. An error ends the multishot (e.g. `EMFILE`); it is
    ///   returned (after the connections taken before it), and the next call arms a new one.
    ///   On kernels without multishot accept the first call fails with `EINVAL`.
    /// - One task at a time: a second concurrent call fails with `ResourceBusy`. Dropping the
    ///   future (e.g. in `select!`) is fine: the accept and its queued connections stay.
    /// - Peers already gone (`getpeername` fails) are closed and skipped.
    /// - [`accept`](Self::accept) on the same listener still works; the two compete for
    ///   connections.
    /// - Dropping the listener cancels the multishot and closes the connections it queued.
    pub async fn accept_batch(
        &self,
        max: usize,
        flags: AcceptFlags,
    ) -> io::Result<Vec<(TcpStream, SocketAddr)>> {
        let mut dst = Vec::new();
        self.accept_batch_into(&mut dst, max, flags).await?;
        Ok(dst)
    }

    /// [`accept_batch`](Self::accept_batch) appending to `dst` (not cleared), so a loop that
    /// drains one `Vec` allocates nothing. Returns how many were appended (at least 1).
    pub async fn accept_batch_into(
        &self,
        dst: &mut Vec<(TcpStream, SocketAddr)>,
        max: usize,
        flags: AcceptFlags,
    ) -> io::Result<usize> {
        self.batch
            .accept(&self.fd, flags.bits(), max, |fd| {
                super::multishot::push_with_addr(dst, fd)
            })
            .await
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
        let addr = sockaddr_to_std(&completion.data.addr.0)?;

        Ok((stream, addr))
    }

    /// Accept a connection straight into the runtime's fixed file table
    /// ([`RuntimeBuilder::with_fixed_files`](crate::RuntimeBuilder::with_fixed_files)), as a
    /// direct descriptor (Linux 5.19+): the kernel never creates an fd for it, and every op on
    /// the stream names its table slot, which saves a file lookup and an atomic refcount pair
    /// per op. The slot is picked by the runtime before the accept is submitted.
    ///
    /// The stream is direct-only, see [`TcpStream::is_direct`] for what that excludes (socket
    /// options: set them on the listener, accepted sockets inherit them). The peer address
    /// comes from the accept itself and is cached, so [`TcpStream::peer_addr`] works.
    ///
    /// Fails with `Unsupported` without a fixed file table and with
    /// `ENFILE` if it is full (the connection then stays in the backlog). Dropping the future
    /// before it completes is safe: a connection accepted anyway is closed.
    pub async fn accept_direct(&self) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept_direct_with(AcceptFlags::EMPTY).await
    }

    /// [`accept_direct`](Self::accept_direct) with `SOCK_*` flags for the new socket, as in
    /// [`accept_with`](Self::accept_with) (`CLOEXEC` is ignored).
    pub async fn accept_direct_with(
        &self,
        flags: AcceptFlags,
    ) -> io::Result<(TcpStream, SocketAddr)> {
        let completion = Op::accept_direct(&self.fd, flags.bits())?.await;
        let mut data = completion.data;
        // On error the slot is released (and cleared) with `data`.
        completion.meta.result?;
        let addr = sockaddr_to_std(&data.addr.0);
        let Some(slot) = data.slot.take() else {
            return Err(io::Error::other("direct accept without a slot"));
        };
        let fd = SharedFd::new_direct(slot.claim());
        let stream = TcpStream::from_direct(fd, addr.as_ref().ok().copied());
        Ok((stream, addr?))
    }

    /// Put the listening socket into the runtime's fixed file table, so accepts name the slot
    /// instead of the fd. See [`TcpStream::register_fixed`]; the fd stays open.
    pub fn register_fixed(&self) -> io::Result<()> {
        self.fd.register_fixed()
    }

    /// Whether the listening socket is in the fixed file table.
    pub fn is_fixed(&self) -> bool {
        self.fd.fixed_slot().is_some()
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

/// The address an accept wrote (`AF_INET` or `AF_INET6`), else `InvalidInput`.
fn sockaddr_to_std(
    storage: &std::mem::MaybeUninit<libc::sockaddr_storage>,
) -> io::Result<SocketAddr> {
    let storage = storage.as_ptr();
    // Safety: the kernel wrote a socket address of the family it reports; the storage is large
    // and aligned enough for any of them.
    unsafe {
        match (*storage).ss_family as _ {
            AF_INET => {
                let addr: &sockaddr_in = &*(storage as *const sockaddr_in);
                let ip = Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes());
                let port = u16::from_be(addr.sin_port);
                Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
            }
            AF_INET6 => {
                let addr: &sockaddr_in6 = &*(storage as *const sockaddr_in6);
                let ip = Ipv6Addr::from(addr.sin6_addr.s6_addr);
                let port = u16::from_be(addr.sin6_port);
                let scope_id = addr.sin6_scope_id;
                Ok(SocketAddr::V6(SocketAddrV6::new(
                    ip,
                    port,
                    addr.sin6_flowinfo,
                    scope_id,
                )))
            }
            _ => Err(io::ErrorKind::InvalidInput.into()),
        }
    }
}
