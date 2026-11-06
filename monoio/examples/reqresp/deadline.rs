//! `--deadline`: what bounds the time a connection may take for one frame.
//!
//! A path creates one [`Guard`] per connection and runs the work for each frame (receive it,
//! reply) through [`Guard::frame`].

use std::{future::Future, io, time::Duration};

/// The request deadline of the bench server.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// `--deadline`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// No deadline.
    None,
    /// `monoio::time::timeout(10 s, ..)` around every frame, like the bench server: one timer
    /// registered and removed per frame.
    Timeout,
    // === r3-timer: `Deadline` (a per-connection `time::Deadline`, armed per frame) ===
}

impl Mode {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "none" => Mode::None,
            "timeout" => Mode::Timeout,
            // === r3-timer: "deadline" => Mode::Deadline ===
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Mode::None => "none",
            Mode::Timeout => "timeout",
        }
    }
}

/// The deadline state of one connection.
pub struct Guard {
    mode: Mode,
    // === r3-timer: the connection's `Deadline` goes here (e.g. `Pin<Box<Deadline>>`) ===
}

impl Guard {
    /// Once per connection.
    pub fn new(mode: Mode) -> Self {
        Self { mode }
    }

    /// Runs `frame` (the work for one frame) under the deadline. On expiry `frame` is dropped
    /// and the result is `TimedOut`; the connection must end then.
    #[inline]
    pub async fn frame<T>(&mut self, frame: impl Future<Output = io::Result<T>>) -> io::Result<T> {
        match self.mode {
            Mode::None => frame.await,
            Mode::Timeout => match monoio::time::timeout(REQUEST_TIMEOUT, frame).await {
                Ok(res) => res,
                Err(_) => Err(io::ErrorKind::TimedOut.into()),
            },
            // === r3-timer: Mode::Deadline => arm, `with_deadline(..)`, disarm ===
        }
    }
}
