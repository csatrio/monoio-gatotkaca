//! `--path pair`: the C1 path (`send_then_recv`: the reply and the receive of the next frame
//! as one operation), filled by package r3-pair.
//!
//! Stub: same interface as the other paths (see `paths/mod.rs`), not available yet.

use std::io;

use monoio::net::TcpStream;

use super::{not_available, Ctx};

pub fn setup(_ctx: &Ctx) -> io::Result<()> {
    not_available("pair")
}

pub async fn serve(_stream: TcpStream, _ctx: &Ctx) -> io::Result<()> {
    not_available("pair")
}
