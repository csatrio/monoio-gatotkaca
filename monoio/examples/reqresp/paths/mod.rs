//! The server side of one request/response connection, in one file per `--path`.
//!
//! Every path module has the same two functions:
//!
//! ```ignore
//! /// Once, on the server runtime, before the listeners accept. Path-wide state (a buffer
//! /// ring, ...) goes into `ctx.put_state(..)`.
//! pub fn setup(ctx: &Ctx) -> io::Result<()>;
//! /// One connection, until the peer closes (`Ok`) or something fails (`Err`).
//! pub async fn serve(stream: TcpStream, ctx: &Ctx) -> io::Result<()>;
//! ```
//!
//! `serve` wraps the work for one frame in `deadline::Guard::frame` and calls
//! [`Ctx::frame_done`] once per frame, after the reply was handed to the kernel and accepted.
//!
//! Wire format: a frame is a 4-byte little-endian total length (header included) plus payload;
//! the reply is [`REPLY`] (70 bytes).

pub mod eager;
pub mod mshot;
pub mod pair;
pub mod pipe;
pub mod plain;

use std::{
    any::Any,
    cell::{Cell, OnceCell},
    io,
    rc::Rc,
};

use monoio::net::TcpStream;

use crate::deadline;

/// Bytes of the length prefix of a frame.
pub const HEADER: usize = 4;
/// Bytes of the reply.
pub const REPLY_LEN: usize = 70;
/// The reply to every frame.
pub static REPLY: [u8; REPLY_LEN] =
    *b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nx-ack: 00000000000000000000000\r\n\r\n";

/// Room in a connection's receive buffer beyond the largest frame.
const BUF_SLACK: usize = 4096;

/// `--path`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Plain,
    Pipe,
    Mshot,
    Eager,
    Pair,
}

impl Path {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "plain" => Path::Plain,
            "pipe" => Path::Pipe,
            "mshot" => Path::Mshot,
            "eager" => Path::Eager,
            "pair" => Path::Pair,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Path::Plain => "plain",
            Path::Pipe => "pipe",
            Path::Mshot => "mshot",
            Path::Eager => "eager",
            Path::Pair => "pair",
        }
    }
}

/// What the paths need to know from the command line.
pub struct Config {
    pub path: Path,
    pub deadline: deadline::Mode,
    /// The largest frame a client sends (header included).
    pub max_frame: usize,
    /// `--bufring ENTRIES,SIZE` (path `mshot`).
    pub bufring: (u16, usize),
    /// `--wset INITIAL,STEP` (path `mshot`, filled by a later package).
    pub wset: Option<(u16, u16)>,
}

/// Counters of the server runtime, sampled by `main.rs` at both ends of the window.
#[derive(Default)]
pub struct Counters {
    /// Frames replied to.
    pub frames: Cell<u64>,
    /// Bytes of those frames.
    pub frame_bytes: Cell<u64>,
    /// Bytes written on bulk connections.
    pub bulk_bytes: Cell<u64>,
    /// Connections accepted (request/response and bulk).
    pub accepted: Cell<u64>,
    /// Request/response connections that ended with an error.
    pub errors: Cell<u64>,
    /// Multishot receives that ended with `ENOBUFS` (path `mshot`).
    pub enobufs: Cell<u64>,
}

/// Shared by all connections of the server runtime.
pub struct Ctx {
    pub cfg: Config,
    pub count: Counters,
    state: OnceCell<Box<dyn Any>>,
}

impl Ctx {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            count: Counters::default(),
            state: OnceCell::new(),
        }
    }

    /// One frame of `bytes` bytes was replied to.
    #[inline]
    pub fn frame_done(&self, bytes: usize) {
        let c = &self.count;
        c.frames.set(c.frames.get() + 1);
        c.frame_bytes.set(c.frame_bytes.get() + bytes as u64);
    }

    /// Store the path's runtime-wide state (from `setup`, once).
    pub fn put_state<T: 'static>(&self, state: T) {
        assert!(
            self.state.set(Box::new(state)).is_ok(),
            "path state set twice"
        );
    }

    /// The state stored by [`put_state`](Self::put_state).
    pub fn state<T: 'static>(&self) -> &T {
        self.state
            .get()
            .and_then(|s| s.downcast_ref())
            .expect("path state missing: setup() stores it")
    }
}

/// Runs the path's `setup`.
pub fn setup(ctx: &Ctx) -> io::Result<()> {
    match ctx.cfg.path {
        Path::Plain => plain::setup(ctx),
        Path::Pipe => pipe::setup(ctx),
        Path::Mshot => mshot::setup(ctx),
        Path::Eager => eager::setup(ctx),
        Path::Pair => pair::setup(ctx),
    }
}

/// The task of one request/response connection.
pub async fn conn(stream: TcpStream, ctx: Rc<Ctx>) {
    let res = match ctx.cfg.path {
        Path::Plain => plain::serve(stream, &ctx).await,
        Path::Pipe => pipe::serve(stream, &ctx).await,
        Path::Mshot => mshot::serve(stream, &ctx).await,
        Path::Eager => eager::serve(stream, &ctx).await,
        Path::Pair => pair::serve(stream, &ctx).await,
    };
    if let Err(e) = res {
        let n = ctx.count.errors.get();
        ctx.count.errors.set(n + 1);
        if n < 3 {
            eprintln!("reqresp: server connection ended: {e}");
        }
    }
}

/// For the paths a later package fills.
pub fn not_available(path: &str) -> ! {
    eprintln!("path {path}: not available in this build");
    std::process::exit(2)
}

// Helpers for the paths that receive into their own buffer (plain, pipe).

/// A receive buffer for one connection. It holds the largest frame, so it never grows.
pub fn new_buf(ctx: &Ctx) -> Vec<u8> {
    Vec::with_capacity(ctx.cfg.max_frame + BUF_SLACK)
}

/// The total length of the frame at the start of `buf`, once the whole frame is there.
#[inline]
pub fn frame_len(buf: &[u8], max: usize) -> io::Result<Option<usize>> {
    let Some(header) = buf.first_chunk::<HEADER>() else {
        return Ok(None);
    };
    let total = check_len(u32::from_le_bytes(*header), max)?;
    Ok((buf.len() >= total).then_some(total))
}

/// Validates the length field of a frame.
#[inline]
pub fn check_len(total: u32, max: usize) -> io::Result<usize> {
    let total = total as usize;
    if total < HEADER || total > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {total} outside {HEADER}..={max}"),
        ));
    }
    Ok(total)
}

/// Removes the first `n` bytes (one frame) from `buf` and keeps its capacity: `clear()` when
/// that is everything (the closed-loop case), else the rest moves to the front.
#[inline]
pub fn consume(buf: &mut Vec<u8>, n: usize) {
    if buf.len() == n {
        buf.clear();
    } else {
        buf.copy_within(n.., 0);
        buf.truncate(buf.len() - n);
    }
}

/// What a connection returns when the peer closed: fine between frames, an error inside one.
pub fn eof(mid_frame: bool) -> io::Result<bool> {
    if mid_frame {
        Err(io::ErrorKind::UnexpectedEof.into())
    } else {
        Ok(false)
    }
}
