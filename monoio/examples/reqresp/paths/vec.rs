//! `--path vec`: `pipe` with the reply sent as three iovecs (header, body, trailer) through
//! `send_vectored_op`, with one `IoVecArray` kept per connection. The measurement path of
//! round 3's A1 (r3-vec).

use std::io;

use monoio::{
    buf::{IoBufMut, IoVecArray, SliceMut},
    io::futures::RecvFuture,
    net::{MsgFlags, TcpStream},
};

use super::{consume, eof, frame_len, new_buf, Ctx, REPLY, REPLY_LEN};
use crate::deadline::Guard;

pub fn setup(_ctx: &Ctx) -> io::Result<()> {
    // Measurement switch: `REQRESP_VEC=sendmsg` forces `sendmsg` + pooled msghdr,
    // `REQRESP_VEC=send` the vectorized SEND; unset: the kernel probe decides.
    match std::env::var("REQRESP_VEC").as_deref() {
        Ok("sendmsg") => monoio::__vectored::set_vectored_send(Some(false)),
        Ok("send") => monoio::__vectored::set_vectored_send(Some(true)),
        _ => {}
    }
    eprintln!(
        "reqresp vec: vectorized SEND = {}",
        monoio::__vectored::vectored_send_is_vectorized()
    );
    Ok(())
}

/// The receive side of a connection, kept across frames.
struct Rx {
    buf: Vec<u8>,
    recv: Option<RecvFuture<SliceMut<Vec<u8>>>>,
    /// The reply's gather list, reused: header, body, trailer. `None` while a send owns it.
    parts: Option<IoVecArray<&'static [u8], 3>>,
}

pub async fn serve(stream: TcpStream, ctx: &Ctx) -> io::Result<()> {
    let mut rx = Rx {
        buf: new_buf(ctx),
        recv: None,
        parts: Some(IoVecArray::new()),
    };
    let mut guard = Guard::new(ctx.cfg.deadline);
    while guard.frame(frame(&stream, &mut rx, ctx)).await? {}
    Ok(())
}

/// The three pieces of the reply: status line, headers, the blank line.
const HEAD: usize = 17;
const BODY: usize = REPLY_LEN - HEAD - 2;

async fn frame(stream: &TcpStream, rx: &mut Rx, ctx: &Ctx) -> io::Result<bool> {
    let max = ctx.cfg.max_frame;
    let total = loop {
        if rx.recv.is_none() {
            if let Some(total) = frame_len(&rx.buf, max)? {
                break total;
            }
            let len = rx.buf.len();
            rx.recv = Some(stream.recv_op(std::mem::take(&mut rx.buf).slice_mut(len..)));
        }
        let recv = rx.recv.as_mut().expect("set above");
        let (res, b) = recv.await;
        rx.recv = None;
        rx.buf = b.into_inner();
        if res? == 0 {
            return eof(!rx.buf.is_empty());
        }
    };
    consume(&mut rx.buf, total);

    let mut parts = rx.parts.take().expect("returned after each frame");
    parts.push(&REPLY[..HEAD]).ok();
    parts.push(&REPLY[HEAD..HEAD + BODY]).ok();
    parts.push(&REPLY[HEAD + BODY..]).ok();
    let send = stream.send_vectored_op(parts);
    if frame_len(&rx.buf, max)?.is_none() {
        let len = rx.buf.len();
        rx.recv = Some(stream.recv_poll_first_op(
            std::mem::take(&mut rx.buf).slice_mut(len..),
            MsgFlags::EMPTY,
        ));
    }
    let (res, mut parts) = send.await;
    let mut n = res?;
    let mut sent = n;
    while sent < REPLY_LEN {
        // A short send of 70 bytes: only with a full socket buffer.
        parts.advance(n);
        let (res, p) = stream.send_vectored_op(parts).await;
        parts = p;
        n = res?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        sent += n;
    }
    parts.clear();
    rx.parts = Some(parts);
    ctx.frame_done(total);
    Ok(true)
}
