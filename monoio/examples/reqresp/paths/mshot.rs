//! `--path mshot`: one multishot receive per connection on a shared `BufRing`, reply by
//! `send_op`.
//!
//! The frame bytes are counted in place in the `SelectedBuf`s (no copy; only a length prefix
//! is copied out, 4 bytes). The receive stays armed, so a frame costs one SQE (the reply)
//! and one loop round. When the ring runs dry the kernel ends the receive with `ENOBUFS`:
//! `next_rearm` waits for a free buffer and arms it again.
//!
//! `--wset INITIAL,STEP` gives the ring a working set (`BufRingOptions::working_set`): only
//! `INITIAL` buffers are published at first, and the ring grows and shrinks with the demand.
//!
//! `enobufs` in the `RESULT` line is the ring's own counter, which only exists with the
//! `metrics` feature (0 without it: `next_rearm` never shows an `ENOBUFS`). With `metrics`
//! the state of the ring is also printed to stderr once a second (`BUFRING ...`).

use std::io;

use monoio::{
    net::{tcp::RecvStream, TcpStream},
    uring::{BufRing, SelectedBuf},
};

use super::{check_len, eof, Ctx, HEADER, REPLY, REPLY_LEN};
use crate::deadline::Guard;

/// Buffer group id of the ring.
const BGID: u16 = 7;

pub fn setup(ctx: &Ctx) -> io::Result<()> {
    let (entries, size) = ctx.cfg.bufring;
    let mut options = BufRing::options();
    if let Some((initial, step)) = ctx.cfg.wset {
        options = options.working_set(initial, step);
    }
    let ring = BufRing::with_options(BGID, entries, size, options)?;
    #[cfg(feature = "metrics")]
    monoio::spawn(report(ring.clone()));
    ctx.put_state(ring);
    Ok(())
}

/// Prints the ring's state once a second (the counters are sums since the start).
#[cfg(feature = "metrics")]
async fn report(ring: BufRing) {
    loop {
        monoio::time::sleep(std::time::Duration::from_secs(1)).await;
        let s = ring.stats();
        eprintln!(
            "BUFRING published={} in_use={} high_water={} enobufs={} taken={} recycled={} \
             grown={} retired={}",
            s.published, s.in_use, s.high_water, s.enobufs, s.taken, s.recycled, s.grown, s.retired
        );
    }
}

/// Finds the frame boundaries in the received chunks.
#[derive(Default)]
struct Parser {
    header: [u8; HEADER],
    /// Bytes of `header` seen. `HEADER` while inside the payload.
    have: usize,
    /// Payload bytes of the current frame not seen yet.
    left: usize,
    total: usize,
}

impl Parser {
    /// Takes bytes from the front of `data` up to the end of the current frame. Returns how
    /// many it took, and the frame's total length if the frame ended there.
    #[inline]
    fn feed(&mut self, data: &[u8], max: usize) -> io::Result<(usize, Option<usize>)> {
        let mut used = 0;
        if self.have < HEADER {
            used = (HEADER - self.have).min(data.len());
            self.header[self.have..self.have + used].copy_from_slice(&data[..used]);
            self.have += used;
            if self.have < HEADER {
                return Ok((used, None));
            }
            self.total = check_len(u32::from_le_bytes(self.header), max)?;
            self.left = self.total - HEADER;
        }
        let n = self.left.min(data.len() - used);
        self.left -= n;
        used += n;
        if self.left > 0 {
            return Ok((used, None));
        }
        self.have = 0;
        Ok((used, Some(self.total)))
    }

    fn mid_frame(&self) -> bool {
        self.have > 0
    }
}

/// The receive side of a connection, kept across frames.
struct Rx {
    stream: RecvStream,
    /// The chunk being counted and how much of it is counted.
    chunk: Option<(SelectedBuf, usize)>,
    parser: Parser,
}

pub async fn serve(stream: TcpStream, ctx: &Ctx) -> io::Result<()> {
    let mut rx = Rx {
        stream: stream.recv_multi(ctx.state::<BufRing>())?,
        chunk: None,
        parser: Parser::default(),
    };
    let mut guard = Guard::new(ctx.cfg.deadline);
    while guard.frame(frame(&stream, &mut rx, ctx)).await? {}
    Ok(())
}

/// Serves one frame. `Ok(false)`: the peer closed between two frames.
async fn frame(stream: &TcpStream, rx: &mut Rx, ctx: &Ctx) -> io::Result<bool> {
    let total = loop {
        if let Some((chunk, at)) = &mut rx.chunk {
            let (used, done) = rx.parser.feed(&chunk[*at..], ctx.cfg.max_frame)?;
            *at += used;
            if *at == chunk.len() {
                // Counted: the buffer goes back to the ring.
                rx.chunk = None;
            }
            if let Some(total) = done {
                break total;
            }
            continue;
        }
        // When the ring is empty the kernel ends the receive (`ENOBUFS`): `next_rearm` waits
        // for a free buffer and arms it again. No data is lost.
        match rx.stream.next_rearm().await {
            Some(Ok(chunk)) => rx.chunk = Some((chunk, 0)),
            Some(Err(e)) => return Err(e),
            None => return eof(rx.parser.mid_frame()),
        }
    };
    #[cfg(feature = "metrics")]
    ctx.count.enobufs.set(rx.stream.ring().stats().enobufs);

    let (res, _) = stream.send_op(&REPLY[..]).await;
    let mut sent = res?;
    while sent < REPLY_LEN {
        // A short send of 70 bytes: only with a full socket buffer.
        let (res, _) = stream.send_op(&REPLY[sent..]).await;
        match res? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            n => sent += n,
        }
    }
    ctx.frame_done(total);
    Ok(true)
}
