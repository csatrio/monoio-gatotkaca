//! `--path pair`: the C1 path, the bench server's "pairack".
//!
//! Like `pipe`, but the reply and the receive of the next frame are **one operation**
//! (`send_then_recv`: `SEND` linked to `RECV`, one op slot, one CQE, one task wake). The first
//! receive of a connection, and any receive that continues a partial frame, is a plain
//! `recv_op`.

use std::io;

use monoio::{
    buf::{IoBufMut, SliceMut},
    io::futures::RecvFuture,
    net::{MsgFlags, SendThenRecv, TcpStream},
};

use super::{consume, eof, frame_len, new_buf, Ctx, REPLY, REPLY_LEN};
use crate::deadline::Guard;

pub fn setup(_ctx: &Ctx) -> io::Result<()> {
    Ok(())
}

/// A receive in flight: plain, or as the tail of a reply pair.
enum Pending {
    Recv(RecvFuture<SliceMut<Vec<u8>>>),
    Pair(SendThenRecv<&'static [u8], SliceMut<Vec<u8>>>),
}

/// The receive side of a connection, kept across frames.
struct Rx {
    /// The receive buffer. Empty (no capacity) while `pending` owns the real one.
    buf: Vec<u8>,
    pending: Option<Pending>,
}

pub async fn serve(stream: TcpStream, ctx: &Ctx) -> io::Result<()> {
    let mut rx = Rx {
        buf: new_buf(ctx),
        pending: None,
    };
    let mut guard = Guard::new(ctx.cfg.deadline);
    while guard.frame(frame(&stream, &mut rx, ctx)).await? {}
    Ok(())
}

/// Serves one frame. `Ok(false)`: the peer closed between two frames.
async fn frame(stream: &TcpStream, rx: &mut Rx, ctx: &Ctx) -> io::Result<bool> {
    let max = ctx.cfg.max_frame;
    // Receive until the frame is whole. Its first receive is usually in flight already, as
    // the tail of the previous frame's reply pair.
    let total = loop {
        if rx.pending.is_none() {
            if let Some(total) = frame_len(&rx.buf, max)? {
                break total;
            }
            let len = rx.buf.len();
            rx.pending = Some(Pending::Recv(
                stream.recv_op(std::mem::take(&mut rx.buf).slice_mut(len..)),
            ));
        }
        let res = match rx.pending.take().expect("set above") {
            Pending::Recv(mut recv) => {
                let (res, b) = (&mut recv).await;
                rx.buf = b.into_inner();
                res
            }
            Pending::Pair(mut pair) => {
                let (send, recv, _, b) = (&mut pair).await;
                rx.buf = b.into_inner();
                // The send result is known only now: the previous frame's reply.
                match send? {
                    n if n == REPLY_LEN => recv,
                    // The send failed after `n` bytes; the receive did not run.
                    _ => return Err(io::ErrorKind::WriteZero.into()),
                }
            }
        };
        if res? == 0 {
            return eof(!rx.buf.is_empty());
        }
    };
    consume(&mut rx.buf, total);

    // The reply and the receive of the next frame as one pair, when the buffer holds no
    // whole frame already (else the next round needs no receive and the reply goes alone).
    if frame_len(&rx.buf, max)?.is_none() {
        let len = rx.buf.len();
        rx.pending = Some(Pending::Pair(stream.send_then_recv(
            &REPLY[..],
            std::mem::take(&mut rx.buf).slice_mut(len..),
            MsgFlags::EMPTY,
        )));
        // The send result arrives with the pair's CQE, i.e. with the next frame: counted
        // here, checked there.
        ctx.frame_done(total);
        return Ok(true);
    }
    let (res, _) = stream.send_op(&REPLY[..]).await;
    let mut sent = res?;
    while sent < REPLY_LEN {
        let (res, _) = stream.send_op(&REPLY[sent..]).await;
        match res? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            n => sent += n,
        }
    }
    ctx.frame_done(total);
    Ok(true)
}
