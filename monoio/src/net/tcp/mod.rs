#![allow(unreachable_pub)]
//! TCP related.

mod cancel;
mod deadline;
mod fixed;
mod listener;
mod multishot;
mod split;
mod stream;
mod tfo;
mod zero_copy;

pub use crate::driver::op::{
    recv_multi::RecvBundleStream,
    send_zc::{SendZcFuture, ZcPending},
};
pub use listener::TcpListener;
pub use multishot::{AcceptStream, DirectAcceptStream, MultiRecvReader, RecvStream};
pub use split::{TcpOwnedReadHalf, TcpOwnedWriteHalf};
pub use stream::{TcpConnectOpts, TcpStream};
pub use zero_copy::{zc_worthwhile, ZC_MIN_LEN};
