//! `--deadline`: what bounds the time a connection may take for one frame.
//!
//! A path creates one [`Guard`] per connection and runs the work for each frame (receive it,
//! reply) through [`Guard::frame`].

use std::{
    future::Future,
    io,
    pin::{pin, Pin},
    time::Duration,
};

use monoio::time::Deadline;

/// The request deadline of the bench server.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// `--deadline`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// No deadline.
    None,
    /// `monoio::time::timeout(10 s, ..)` around every frame, like the bench server: one clock
    /// read, one timer registered and removed per frame, and the frame future moved into the
    /// `Timeout`.
    Timeout,
    /// `monoio::time::timeout_coarse(10 s, ..)`: `timeout` without the clock read
    /// (`Instant::recent`). Wrapped by value like `timeout`, so the difference between the two
    /// is the clock.
    Coarse,
    /// `timeout(10 s, pin!(frame))`: the timer work of `timeout`, but the frame future stays
    /// where it is (a `Pin<&mut F>` is wrapped).
    TimeoutPin,
    /// One `monoio::time::Deadline` per connection, armed for 10 s around each frame
    /// (`Deadline::timeout` around `pin!(frame)`): two stores per frame, no clock read, no
    /// wheel insert per frame.
    Deadline,
    /// `pin!(frame).await`: no deadline, in the shape the two modes above poll the frame in
    /// (through a `Pin<&mut F>`, which the compiler may not inline into the task like the
    /// plain `frame.await` of `none`). The cost of a deadline in those modes is the difference
    /// to this one.
    NonePin,
}

impl Mode {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "none" => Mode::None,
            "timeout" => Mode::Timeout,
            "coarse" => Mode::Coarse,
            "timeoutpin" => Mode::TimeoutPin,
            "deadline" => Mode::Deadline,
            "nonepin" => Mode::NonePin,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Mode::None => "none",
            Mode::Timeout => "timeout",
            Mode::Coarse => "coarse",
            Mode::TimeoutPin => "timeoutpin",
            Mode::Deadline => "deadline",
            Mode::NonePin => "nonepin",
        }
    }
}

/// The deadline state of one connection.
pub struct Guard {
    mode: Mode,
    /// Mode `deadline`: the connection's deadline. Boxed because a `Guard` is moved after
    /// `new`; a server that owns its connection future would pin it there (`pin!`) instead.
    deadline: Option<Pin<Box<Deadline>>>,
}

impl Guard {
    /// Once per connection.
    pub fn new(mode: Mode) -> Self {
        Self {
            mode,
            deadline: (mode == Mode::Deadline).then(|| Box::pin(Deadline::new())),
        }
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
            Mode::Coarse => match monoio::time::timeout_coarse(REQUEST_TIMEOUT, frame).await {
                Ok(res) => res,
                Err(_) => Err(io::ErrorKind::TimedOut.into()),
            },
            Mode::TimeoutPin | Mode::Deadline | Mode::NonePin => self.pinned(pin!(frame)).await,
        }
    }

    /// The modes that poll the frame future in place.
    async fn pinned<T>(
        &mut self,
        frame: Pin<&mut impl Future<Output = io::Result<T>>>,
    ) -> io::Result<T> {
        let res = match self.mode {
            Mode::TimeoutPin => monoio::time::timeout(REQUEST_TIMEOUT, frame).await,
            Mode::Deadline => {
                // Armed here, disarmed when the frame is done.
                let deadline = self.deadline.as_mut().expect("set in new()").as_mut();
                deadline.timeout(REQUEST_TIMEOUT, frame).await
            }
            _ => return frame.await,
        };
        match res {
            Ok(res) => res,
            Err(_) => Err(io::ErrorKind::TimedOut.into()),
        }
    }
}
