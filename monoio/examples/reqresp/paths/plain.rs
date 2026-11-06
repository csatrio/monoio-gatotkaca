//! `--path plain`: `read` until a whole frame is there, then `write_all` of the reply.
//!
//! A frame costs two loop rounds: the reply is submitted in one enter, and the receive of the
//! next frame only after the reply's completion was seen.

use std::io;

use monoio::{
    buf::IoBufMut,
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::TcpStream,
};

use super::{consume, eof, frame_len, new_buf, Ctx, REPLY};
use crate::deadline::Guard;

pub fn setup(_ctx: &Ctx) -> io::Result<()> {
    Ok(())
}

pub async fn serve(mut stream: TcpStream, ctx: &Ctx) -> io::Result<()> {
    let mut buf = new_buf(ctx);
    let mut guard = Guard::new(ctx.cfg.deadline);
    while guard.frame(frame(&mut stream, &mut buf, ctx)).await? {}
    Ok(())
}

/// Serves one frame. `Ok(false)`: the peer closed between two frames.
async fn frame(stream: &mut TcpStream, buf: &mut Vec<u8>, ctx: &Ctx) -> io::Result<bool> {
    let total = loop {
        if let Some(total) = frame_len(buf, ctx.cfg.max_frame)? {
            break total;
        }
        let len = buf.len();
        let (res, b) = stream.read(std::mem::take(buf).slice_mut(len..)).await;
        *buf = b.into_inner();
        if res? == 0 {
            return eof(!buf.is_empty());
        }
    };
    consume(buf, total);
    let (res, _) = stream.write_all(&REPLY[..]).await;
    res?;
    ctx.frame_done(total);
    Ok(true)
}
