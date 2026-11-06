//! A re-armable deadline for request timeouts.
//!
//! See [`Deadline`].

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

use crate::time::{
    driver::{duration_to_ticks, Handle, TimerEntry},
    error::Elapsed,
    Duration, Instant,
};

pin_project! {
    /// A deadline that is armed and disarmed many times: one per connection, armed around each
    /// request.
    ///
    /// [`timeout`](crate::time::timeout()) creates a timer for every call: a clock read, an
    /// insert into the timer wheel on the first poll and a removal when it is dropped. For a
    /// request that takes 100 µs under a deadline of 10 s that is all wasted. A `Deadline` is
    /// one timer that stays where it is:
    ///
    /// - [`arm`](Self::arm) sets the expiration from the coarse clock
    ///   ([`Instant::recent`]): no clock read. If the timer is already in the wheel and the new
    ///   expiration is not earlier than the slot it sits in, that is one store. It is inserted
    ///   only when it is not in the wheel (the first `arm`, and the first after it left the
    ///   wheel), and re-inserted only when the new expiration is earlier than its slot.
    /// - [`disarm`](Self::disarm) is one store. The timer stays in the wheel. When the wheel
    ///   reaches its slot and it is still disarmed, it is taken out silently: nobody is woken.
    /// - While it is armed and the expiration keeps moving ahead, the driver moves the timer to
    ///   its new slot when the old one comes up: about once per deadline period per connection.
    ///
    /// So a request costs two stores, and a connection one wheel insert per deadline period at
    /// most.
    ///
    /// # Precision
    ///
    /// - Never early with respect to the coarse clock: an armed deadline expires at
    ///   `Instant::recent() + after` or up to 2 ms later (the wheel's resolution is 1 ms, and
    ///   both the clock value and `after` are rounded up to it). The coarse clock lags the real
    ///   one by less than one round of tasks, so **with respect to the real clock the deadline
    ///   can come early by that lag**. Use it for deadlines that are long compared to a round
    ///   (seconds), not to measure time. [`arm_at`](Self::arm_at) takes an exact instant.
    /// - Like every timer of this runtime it is noticed when the runtime next reaches its
    ///   driver (a park or a cold-path submit).
    ///
    /// # Use
    ///
    /// A `Deadline` holds a timer entry the wheel points to, so it must be pinned
    /// (`std::pin::pin!`, or `Box::pin` to keep it in a struct). It belongs to the runtime it
    /// was created in.
    ///
    /// ```
    /// use std::{pin::pin, time::Duration};
    ///
    /// use monoio::time::Deadline;
    ///
    /// # async fn serve_one_request() -> std::io::Result<bool> { Ok(false) }
    /// # #[monoio::main(timer_enabled = true)]
    /// # async fn main() -> std::io::Result<()> {
    /// let mut deadline = std::pin::pin!(Deadline::new());
    /// loop {
    ///     // Armed for 10 s, disarmed when the request is done (or dropped).
    ///     let request = pin!(serve_one_request());
    ///     match deadline.as_mut().timeout(Duration::from_secs(10), request).await {
    ///         Ok(Ok(true)) => continue,
    ///         Ok(Ok(false)) => break,
    ///         Ok(Err(e)) => return Err(e),
    ///         Err(elapsed) => return Err(elapsed.into()),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// With several steps under one deadline, arm once and wrap each step with
    /// [`with_deadline`]:
    ///
    /// ```
    /// use std::{pin::pin, time::Duration};
    ///
    /// use monoio::time::{with_deadline, Deadline};
    ///
    /// # async fn read_request() {}
    /// # async fn write_reply() {}
    /// # #[monoio::main(timer_enabled = true)]
    /// # async fn main() -> Result<(), monoio::time::error::Elapsed> {
    /// let mut deadline = std::pin::pin!(Deadline::new());
    /// deadline.as_mut().arm(Duration::from_secs(10));
    /// with_deadline(deadline.as_mut(), read_request()).await?;
    /// with_deadline(deadline.as_mut(), write_reply()).await?;
    /// deadline.as_mut().disarm();
    /// # Ok(())
    /// # }
    /// ```
    #[derive(Debug)]
    pub struct Deadline {
        // One timer entry. Armed or not, expired or not is all in its state.
        #[pin]
        entry: TimerEntry,
    }
}

impl Deadline {
    /// A disarmed deadline. It is not in the timer wheel.
    ///
    /// # Panics
    ///
    /// Outside a runtime, and in a runtime built without `enable_timer`, like
    /// [`sleep`](crate::time::sleep()).
    pub fn new() -> Self {
        Self {
            entry: TimerEntry::new(Handle::current()),
        }
    }

    /// Arms the deadline: it expires `after` from now, by the coarse clock
    /// ([`Instant::recent`]). An earlier expiration, reported or not, is forgotten.
    ///
    /// One store if the timer is in the wheel and the new expiration is not earlier than the
    /// slot it sits in; else one wheel insert (after a removal if it was in the wheel).
    ///
    /// Durations beyond about 30 years are cut to that.
    #[inline]
    pub fn arm(self: Pin<&mut Self>, after: Duration) {
        self.project().entry.reset_coarse(duration_to_ticks(after));
    }

    /// Arms the deadline for the instant `at`, exactly (no coarse clock involved). The same
    /// cost rules as [`arm`](Self::arm), plus the conversion of the instant.
    pub fn arm_at(self: Pin<&mut Self>, at: Instant) {
        self.project().entry.reset(at);
    }

    /// Disarms the deadline: [`poll_expired`](Self::poll_expired) stays `Pending` and nobody is
    /// woken until it is armed again. An expiration that already happened is forgotten.
    ///
    /// One store. The timer stays in the wheel; if it is still disarmed when the wheel reaches
    /// it, it is taken out without a wake-up.
    #[inline]
    pub fn disarm(self: Pin<&mut Self>) {
        self.project().entry.disarm();
    }

    /// Whether the deadline is armed. It stays armed after it expired, until
    /// [`disarm`](Self::disarm) or the next [`arm`](Self::arm).
    pub fn is_armed(&self) -> bool {
        self.entry.is_armed()
    }

    /// Whether the deadline is armed and has expired.
    pub fn is_expired(&self) -> bool {
        self.entry.is_elapsed()
    }

    /// `Ready` when the deadline is armed and has expired, and from then on until it is
    /// disarmed or armed again.
    ///
    /// While it is armed and not expired, the task of `cx` is woken when it expires. A disarmed
    /// deadline returns `Pending` and keeps no waker: poll again after arming it.
    #[inline]
    pub fn poll_expired(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.project().entry.poll_fired(cx)
    }

    /// Runs `future` under this deadline, armed for `after` from now: the one call for "arm,
    /// serve one request, disarm".
    ///
    /// The deadline is armed when this function is called (see [`arm`](Self::arm)) and disarmed
    /// when the returned future is dropped, which is also when it completed. The output is
    /// `Ok(output)` of `future`, or `Err(Elapsed)` if the deadline expired first (`future` is
    /// not polled again and is dropped with the returned future).
    ///
    /// `future` is moved into the returned future. For a large one pass a pinned reference
    /// instead (`pin!(future)`): a `Pin<&mut F>` is a future too, and nothing is copied.
    #[inline]
    pub fn timeout<F: Future>(
        mut self: Pin<&mut Self>,
        after: Duration,
        future: F,
    ) -> WithDeadline<'_, F> {
        self.as_mut().arm(after);
        WithDeadline {
            future,
            deadline: self,
            disarm: true,
        }
    }
}

impl Default for Deadline {
    fn default() -> Self {
        Self::new()
    }
}

/// Runs `future` until it completes or `deadline` expires, whichever is first.
///
/// This neither arms nor disarms the deadline: arm it before (once for several steps, see the
/// example at [`Deadline`]), disarm it when the exchange is over. With a disarmed deadline the
/// result is just `Ok(future.await)`. To arm, run and disarm in one call use
/// [`Deadline::timeout`].
///
/// The output is `Ok(output)` of `future`, or `Err(Elapsed)` if the deadline is armed and
/// expired (`future` is not polled again and is dropped with the returned future). `future` is
/// polled first, so one that is ready wins against a deadline that expired at the same time.
/// After an `Err` the deadline stays expired: the next `with_deadline` returns `Err` at once
/// (after one poll of its future) until the deadline is armed again or disarmed.
///
/// `future` is moved into the returned future. For a large one pass a pinned reference instead
/// (`pin!(future)`): a `Pin<&mut F>` is a future too, and nothing is copied.
#[inline]
pub fn with_deadline<F: Future>(deadline: Pin<&mut Deadline>, future: F) -> WithDeadline<'_, F> {
    WithDeadline {
        future,
        deadline,
        disarm: false,
    }
}

pin_project! {
    /// Future returned by [`with_deadline`] and [`Deadline::timeout`].
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    pub struct WithDeadline<'a, F> {
        #[pin]
        future: F,
        deadline: Pin<&'a mut Deadline>,
        // Made by `Deadline::timeout`: disarm when dropped.
        disarm: bool,
    }

    impl<F> PinnedDrop for WithDeadline<'_, F> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if *this.disarm {
                this.deadline.as_mut().disarm();
            }
        }
    }
}

impl<F> WithDeadline<'_, F> {
    /// Gets a reference to the wrapped future.
    pub fn get_ref(&self) -> &F {
        &self.future
    }

    /// Gets a mutable reference to the wrapped future.
    pub fn get_mut(&mut self) -> &mut F {
        &mut self.future
    }
}

impl<F: Future> Future for WithDeadline<'_, F> {
    type Output = Result<F::Output, Elapsed>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // First, try polling the future
        if let Poll::Ready(v) = this.future.poll(cx) {
            return Poll::Ready(Ok(v));
        }

        // Now check the deadline
        match this.deadline.as_mut().poll_expired(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed::new())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::driver::wheel_ops;

    fn runtime() -> crate::Runtime<crate::time::TimeDriver<crate::IoUringDriver>> {
        crate::RuntimeBuilder::<crate::IoUringDriver>::new()
            .enable_timer()
            .build()
            .expect("runtime")
    }

    #[test]
    fn deadline_size() {
        assert_eq!(std::mem::size_of::<Deadline>(), 56);
        assert_eq!(std::mem::align_of::<Deadline>(), 8);
    }

    /// The cost model, counted: the wheel is touched once for the first `arm`, and not by any
    /// number of later arm/disarm pairs that do not move the expiration before the slot.
    #[test]
    fn rearm_and_disarm_do_not_touch_the_wheel() {
        runtime().block_on(async {
            let mut d = std::pin::pin!(Deadline::new());
            let before = wheel_ops();
            d.as_mut().arm(Duration::from_secs(10));
            assert_eq!(
                wheel_ops(),
                (before.0 + 1, before.1),
                "the first arm inserts"
            );
            let slot = d.entry.slot_tick();

            for i in 0..10_000u64 {
                d.as_mut().disarm();
                assert!(!d.is_armed());
                // The same duration, later ones, and (after the first round) a shorter one that
                // still is not before the slot the entry sits in.
                let after = match i % 3 {
                    0 => Duration::from_secs(10),
                    1 => Duration::from_secs(10 + i),
                    _ => Duration::from_millis(10_000 + i),
                };
                d.as_mut().arm(after);
                assert!(d.is_armed());
                if i % 1000 == 0 {
                    // Let rounds pass, so the coarse clock moves.
                    crate::time::sleep(Duration::from_millis(1)).await;
                }
            }
            // One insert for each of the 10 short sleeps above (they fired, so the wheel took
            // them out itself), nothing else.
            assert_eq!(wheel_ops(), (before.0 + 1 + 10, before.1));
            assert_eq!(d.entry.slot_tick(), slot, "the entry moved to another slot");

            // Earlier than the slot: this one must go through the wheel.
            let ops = wheel_ops();
            d.as_mut().arm(Duration::from_secs(1));
            assert_eq!(wheel_ops(), (ops.0 + 1, ops.1 + 1));
            assert!(d.entry.slot_tick() < slot);
        });
    }

    /// Dropping a registered deadline unlinks it; an idle one does not touch the wheel.
    #[test]
    fn drop_unlinks() {
        runtime().block_on(async {
            let before = wheel_ops();
            {
                let _idle = std::pin::pin!(Deadline::new());
            }
            assert_eq!(wheel_ops(), before);
            {
                let mut armed = std::pin::pin!(Deadline::new());
                armed.as_mut().arm(Duration::from_secs(5));
                let mut disarmed = std::pin::pin!(Deadline::new());
                disarmed.as_mut().arm(Duration::from_secs(5));
                disarmed.as_mut().disarm();
            }
            assert_eq!(wheel_ops(), (before.0 + 2, before.1 + 2));
        });
    }
}
