//! Network related
//! Currently, TCP/UnixStream/UnixDatagram are implemented.

mod listener_config;
pub mod tcp;
pub mod udp;
pub mod unix;

pub use listener_config::ListenerOpts;
#[deprecated(since = "0.2.0", note = "use ListenerOpts")]
pub use listener_config::ListenerOpts as ListenerConfig;
pub use tcp::{TcpConnectOpts, TcpListener, TcpStream};
pub use unix::{Pipe, UnixDatagram, UnixListener, UnixStream};

pub(crate) fn new_socket(
    domain: libc::c_int,
    socket_type: libc::c_int,
) -> std::io::Result<libc::c_int> {
    crate::syscall!(socket@RAW(domain, socket_type | libc::SOCK_CLOEXEC, 0))
}
