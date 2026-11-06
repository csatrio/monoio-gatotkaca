//! Cooperative scheduling: [`yield_now`], the opt-in per-task budget
//! ([`RuntimeBuilder::coop_budget`](crate::RuntimeBuilder::coop_budget)), [`consume_budget`] and
//! [`unconstrained`].
//!
//! The budget is a thread-local counter. With `coop_budget(Some(n))` the runtime sets it to `n`
//! before it polls a task (and before each poll of the main future). Every I/O future that
//! returns a result (`Op`, raw and named op futures, `MultiOp::next`, `ChainOp`) and every
//! [`consume_budget`] spends 1. Once it is 0, the next such poll wakes the task and returns
//! `Pending`, whether or not the result is ready (a ready completion stays in its op slot and is
//! returned on the next poll), so the task goes to the back of the ready queue.
//!
//! Without `coop_budget` the counter stays at "unconstrained" and the check is one thread-local
//! load and a branch per poll.

use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Budget value meaning "no budget" (coop off, or inside [`unconstrained`]).
pub(crate) const UNCONSTRAINED: u32 = u32::MAX;

#[cfg(feature = "unstable")]
#[thread_local]
static BUDGET: Cell<u32> = Cell::new(UNCONSTRAINED);

#[cfg(not(feature = "unstable"))]
thread_local! {
    static BUDGET: Cell<u32> = const { Cell::new(UNCONSTRAINED) };
}

/// Set the budget of the task about to be polled (`UNCONSTRAINED` turns it off).
#[inline]
pub(crate) fn set(n: u32) {
    BUDGET.set(n);
}

/// Check the budget before polling a result-producing future. `Pending` (after waking the task)
/// if it is spent; otherwise `Ready` with the budget seen, to pass to [`consume`] if the poll
/// produced a result.
#[inline]
pub(crate) fn poll_proceed(cx: &mut Context<'_>) -> Poll<u32> {
    let b = BUDGET.get();
    if b == 0 {
        exhausted(cx);
        return Poll::Pending;
    }
    Poll::Ready(b)
}

/// A poll that was allowed by [`poll_proceed`] (which returned `b`) produced a result.
#[inline]
pub(crate) fn consume(b: u32) {
    if b != UNCONSTRAINED {
        BUDGET.set(b - 1);
    }
}

#[cold]
#[inline(never)]
fn exhausted(cx: &mut Context<'_>) {
    metric!(crate::runtime::CURRENT.try_with(|ctx| {
        if let Some(ctx) = ctx {
            ctx.stats.with(|s| s.coop_yields += 1);
        }
    }));
    cx.waker().wake_by_ref();
}

/// Yield to the scheduler once: the task is put at the back of the ready queue, so every task
/// already queued runs before it continues.
///
/// The future returns `Pending` on its first poll after waking its own task, and `Ready` on the
/// next. It is not affected by [`RuntimeBuilder::lifo_slot`](crate::RuntimeBuilder::lifo_slot)
/// or the coop budget.
///
/// Note: without the `sync` feature the main future of `block_on` is not a task; it is polled
/// again right away when it wakes itself, so a yield there returns without running other tasks.
///
/// ```no_run
/// #[monoio::main]
/// async fn main() {
///     monoio::spawn(async {
///         for chunk in 0..1000 {
///             // ... CPU work on `chunk` ...
///             let _ = chunk;
///             monoio::task::yield_now().await;
///         }
///     })
///     .await;
/// }
/// ```
pub async fn yield_now() {
    YieldNow { yielded: false }.await
}

struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Spend one unit of the task's coop budget, for CPU-heavy loops (parsing, sealing) that would
/// otherwise never return to the scheduler.
///
/// With [`RuntimeBuilder::coop_budget`](crate::RuntimeBuilder::coop_budget) unset (the default),
/// or inside [`unconstrained`], this is always ready. When the budget is spent it yields like
/// [`yield_now`]; the task gets a fresh budget on its next poll.
pub async fn consume_budget() {
    std::future::poll_fn(|cx| {
        let b = ready!(poll_proceed(cx));
        consume(b);
        Poll::Ready(())
    })
    .await
}

/// Run `fut` without a coop budget: I/O futures inside it never yield because of the budget,
/// and what they consume is not charged to the surrounding task. For an accept loop or a control
/// task that must not be throttled.
pub fn unconstrained<F: Future>(fut: F) -> Unconstrained<F> {
    Unconstrained { inner: fut }
}

pin_project_lite::pin_project! {
    /// Future returned by [`unconstrained`].
    #[must_use = "futures do nothing unless polled"]
    #[derive(Debug)]
    pub struct Unconstrained<F> {
        #[pin]
        inner: F,
    }
}

impl<F: Future> Future for Unconstrained<F> {
    type Output = F::Output;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let prev = BUDGET.replace(UNCONSTRAINED);
        // Restore even if `inner` panics, so the task's budget stays meaningful.
        struct Restore(u32);
        impl Drop for Restore {
            fn drop(&mut self) {
                BUDGET.set(self.0);
            }
        }
        let _restore = Restore(prev);
        self.project().inner.poll(cx)
    }
}
