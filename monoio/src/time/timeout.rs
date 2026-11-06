//! Allows a future to execute for a maximum amount of time.
//!
//! See [`Timeout`] documentation for more details.
//!
//! [`Timeout`]: struct@Timeout

use std::{
    future::Future,
    pin::Pin,
    task::{self, Poll},
};

use pin_project_lite::pin_project;

use crate::time::{error::Elapsed, sleep_until, Duration, Instant, Sleep};

/// Require a `Future` to complete before the specified duration has elapsed.
///
/// If the future completes before the duration has elapsed, then the completed
/// value is returned. Otherwise, an error is returned and the future is
/// canceled.
///
/// The duration counts from the exact clock ([`Instant::now`], one clock read
/// per call), so the deadline never comes early, also when the task has been
/// running for long before the call. Where a deadline that is early by up to one
/// round of tasks is fine (request timeouts in seconds), these are cheaper:
///
/// - [`timeout_coarse`]: the same with the coarse clock, no clock read;
/// - [`Deadline`](crate::time::Deadline): one timer per connection that is armed
///   and disarmed around each request, two stores per request and no insert
///   into the timer wheel.
///
/// `future` is moved into the returned `Timeout`. For a large one pass a pinned
/// reference instead (`std::pin::pin!(future)`): a `Pin<&mut F>` is a future
/// too, and nothing is copied.
///
/// # Cancelation
///
/// Cancelling a timeout is done by dropping the future. No additional cleanup
/// or other work is required.
///
/// The original future may be obtained by calling [`Timeout::into_inner`]. This
/// consumes the `Timeout`.
#[inline(always)]
pub fn timeout<T>(duration: Duration, future: T) -> Timeout<T>
where
    T: Future,
{
    let deadline = Instant::now().checked_add(duration);
    let delay = match deadline {
        Some(deadline) => Sleep::new_timeout(deadline),
        None => Sleep::far_future(),
    };
    Timeout::new_with_delay(future, delay)
}

/// [`timeout`] with the coarse clock: require a `Future` to complete before
/// `duration` has elapsed since [`Instant::recent`].
///
/// No clock read, and the timer's place in the wheel is computed without one:
/// cheaper than [`timeout`] by what `Instant::now()` costs and a little more.
/// The timer is still inserted into the wheel on the first poll and removed
/// when the `Timeout` is dropped; a [`Deadline`](crate::time::Deadline) avoids
/// that too.
///
/// The price: the coarse clock lags the real one by less than one round of
/// tasks, so the deadline can come **early** by that lag (and up to 2 ms late:
/// the clock value and the duration are both rounded up to the wheel's
/// millisecond). Use it for deadlines that are long compared to a round.
/// [`timeout`] keeps the exact clock.
///
/// [`Sleep::deadline`] of the timer inside is `Instant::recent() + duration`.
///
/// # Panics
///
/// Outside a runtime, and in a runtime built without `enable_timer`, like
/// [`timeout`].
///
/// # Examples
///
/// ```
/// use monoio::time::{timeout_coarse, Duration};
///
/// # async fn serve_one_request() {}
/// # #[monoio::main(timer_enabled = true)]
/// # async fn main() {
/// if timeout_coarse(Duration::from_secs(10), serve_one_request())
///     .await
///     .is_err()
/// {
///     println!("request timed out");
/// }
/// # }
/// ```
#[inline(always)]
pub fn timeout_coarse<T>(duration: Duration, future: T) -> Timeout<T>
where
    T: Future,
{
    Timeout::new_with_delay(future, Sleep::new_coarse(duration))
}

/// Require a `Future` to complete before the specified instant in time.
///
/// If the future completes before the instant is reached, then the completed
/// value is returned. Otherwise, an error is returned.
///
/// # Cancelation
///
/// Cancelling a timeout is done by dropping the future. No additional cleanup
/// or other work is required.
///
/// The original future may be obtained by calling [`Timeout::into_inner`]. This
/// consumes the `Timeout`.
#[inline(always)]
pub fn timeout_at<T>(deadline: Instant, future: T) -> Timeout<T>
where
    T: Future,
{
    let delay = sleep_until(deadline);

    Timeout {
        value: future,
        delay,
    }
}

pin_project! {
    /// Future returned by [`timeout`](timeout) and [`timeout_at`](timeout_at).
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    pub struct Timeout<T> {
        #[pin]
        value: T,
        #[pin]
        delay: Sleep,
    }
}

impl<T> Timeout<T> {
    #[inline(always)]
    pub(crate) fn new_with_delay(value: T, delay: Sleep) -> Timeout<T> {
        Timeout { value, delay }
    }

    /// Gets a reference to the underlying value in this timeout.
    pub fn get_ref(&self) -> &T {
        &self.value
    }

    /// Gets a mutable reference to the underlying value in this timeout.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// Consumes this timeout, returning the underlying value.
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Future for Timeout<T>
where
    T: Future,
{
    type Output = Result<T::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let me = self.project();

        // First, try polling the future
        if let Poll::Ready(v) = me.value.poll(cx) {
            return Poll::Ready(Ok(v));
        }

        // Now check the timer
        match me.delay.poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed::new())),
            Poll::Pending => Poll::Pending,
        }
    }
}
