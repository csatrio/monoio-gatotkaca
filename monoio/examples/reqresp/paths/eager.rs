//! `--path eager`: the S1 path, the bench server's "eagerack".
//!
//! Like `pipe`, but the reply goes through `send_eager`: a direct, non-blocking `send(2)` from
//! the task poll that parsed the frame. The reply leaves at once instead of at the next
//! `io_uring_enter`, and when the socket takes all of it (70 bytes: always, unless the client
//! stopped reading) the future is ready at once, so nothing is awaited: no SEND SQE, no CQE,
//! no wake. Only when the socket is full does the rest go through the ring.
//!
//! The receive of the next frame is queued in the same poll (`recv_poll_first_op`), so a frame
//! costs one SQE and one loop round.

use std::io;

use monoio::{
    buf::{IoBufMut, SliceMut},
    io::futures::RecvFuture,
    net::{MsgFlags, TcpStream},
};

use super::{consume, eof, frame_len, new_buf, Ctx, REPLY, REPLY_LEN};
use crate::deadline::Guard;

pub fn setup(_ctx: &Ctx) -> io::Result<()> {
    Ok(())
}

/// The receive side of a connection, kept across frames.
struct Rx {
    /// The receive buffer. Empty (no capacity) while `recv` owns the real one.
    buf: Vec<u8>,
    /// A receive in flight.
    recv: Option<RecvFuture<SliceMut<Vec<u8>>>>,
}

pub async fn serve(stream: TcpStream, ctx: &Ctx) -> io::Result<()> {
    let mut rx = Rx {
        buf: new_buf(ctx),
        recv: None,
    };
    let mut guard = Guard::new(ctx.cfg.deadline);
    while guard.frame(frame(&stream, &mut rx, ctx)).await? {}
    Ok(())
}

/// Serves one frame. `Ok(false)`: the peer closed between two frames.
async fn frame(stream: &TcpStream, rx: &mut Rx, ctx: &Ctx) -> io::Result<bool> {
    let max = ctx.cfg.max_frame;
    // Receive until the frame is whole. Its first receive is usually in flight already: it
    // was queued right after the previous reply.
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

    // The reply: sent here and now if the socket has room (no send of this connection is in
    // flight: the previous one was awaited). Then the receive of the next frame. It polls
    // first: the client only sends after it got the reply, so the data is usually not there
    // yet when the SQE is pushed.
    let send = stream.send_eager(&REPLY[..]);
    if frame_len(&rx.buf, max)?.is_none() {
        let len = rx.buf.len();
        rx.recv = Some(stream.recv_poll_first_op(
            std::mem::take(&mut rx.buf).slice_mut(len..),
            MsgFlags::EMPTY,
        ));
    }
    // Ready at its first poll when the direct send took everything: this await does not
    // suspend then.
    let (res, _) = send.await;
    let mut sent = res?;
    while sent < REPLY_LEN {
        // A short send of 70 bytes: only with a full socket buffer.
        let (res, _) = stream.send_eager(&REPLY[sent..]).await;
        match res? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            n => sent += n,
        }
    }
    ctx.frame_done(total);
    Ok(true)
}
