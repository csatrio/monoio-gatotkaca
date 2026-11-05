#![allow(unreachable_pub)]
//! Unix related.

mod datagram;
mod listener;
mod multishot;
mod pipe;
mod socket_addr;
mod split;
mod stream;
mod ucred;

mod seq_packet;
pub use datagram::UnixDatagram;
pub use listener::UnixListener;
pub use multishot::UnixAcceptStream;
pub use pipe::{new_pipe, Pipe};
pub use seq_packet::{UnixSeqpacket, UnixSeqpacketListener};
pub use socket_addr::SocketAddr;
pub use split::{UnixOwnedReadHalf, UnixOwnedWriteHalf};
pub use stream::UnixStream;

/// Buffers received by [`UnixStream::recv_multi`] (the same type as for TCP).
pub use crate::net::tcp::RecvStream;

pub(crate) fn path_offset(sockaddr: &libc::sockaddr_un) -> usize {
    let base = sockaddr as *const _ as usize;
    let path = &sockaddr.sun_path as *const _ as usize;
    path - base
}
