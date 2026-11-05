#![allow(unreachable_pub)]
//! TCP related.

mod deadline;
mod fixed;
mod listener;
mod multishot;
mod split;
mod stream;
mod tfo;

pub use crate::driver::op::recv_multi::RecvBundleStream;
pub use listener::TcpListener;
pub use multishot::{AcceptStream, RecvStream};
pub use split::{TcpOwnedReadHalf, TcpOwnedWriteHalf};
pub use stream::{TcpConnectOpts, TcpStream};
