//! `--path eager`: the S1 path (`send_eager` for the reply), filled by package r3-eager.
//!
//! Stub: same interface as the other paths (see `paths/mod.rs`), not available yet.

use std::io;

use monoio::net::TcpStream;

use super::{not_available, Ctx};

pub fn setup(_ctx: &Ctx) -> io::Result<()> {
    not_available("eager")
}

pub async fn serve(_stream: TcpStream, _ctx: &Ctx) -> io::Result<()> {
    not_available("eager")
}
