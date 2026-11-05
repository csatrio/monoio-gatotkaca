/// Monoio Driver.
pub(crate) mod op;
pub(crate) mod shared_fd;
#[cfg(feature = "sync")]
pub(crate) mod thread;

mod buf_ring;
mod chain;
mod fixed_buf;
pub(crate) mod fixed_files;
mod msg_ring;
pub mod raw;
mod uring;
mod util;

use std::{
    io,
    task::{Context, Poll},
    time::Duration,
};

pub(crate) use self::fixed_files::register_fixed_files;
use self::op::{CompletionMeta, Op, OpAble};
pub(crate) use self::uring::DriverOptions;
use self::uring::UringInner;
pub use self::uring::{DriverStats, IoUringDriver};

/// Unpark a runtime of another thread.
pub(crate) mod unpark {
    #[allow(unreachable_pub, dead_code)]
    pub trait Unpark: Sync + Send + 'static {
        /// Unblocks a thread that is blocked by the associated `Park` handle.
        ///
        /// Calling `unpark` atomically makes available the unpark token, if it
        /// is not already available.
        ///
        /// # Panics
        ///
        /// This function **should** not panic, but ultimately, panics are left
        /// as an implementation detail. Refer to the documentation for
        /// the specific `Unpark` implementation
        fn unpark(&self) -> std::io::Result<()>;
    }
}

impl unpark::Unpark for Box<dyn unpark::Unpark> {
    fn unpark(&self) -> io::Result<()> {
        (**self).unpark()
    }
}

impl unpark::Unpark for std::sync::Arc<dyn unpark::Unpark> {
    fn unpark(&self) -> io::Result<()> {
        (**self).unpark()
    }
}

/// Core driver trait.
pub trait Driver {
    /// Run with driver TLS.
    fn with<R>(&self, f: impl FnOnce() -> R) -> R;
    /// Submit ops to kernel and process returned events.
    fn submit(&self) -> io::Result<()>;
    /// Wait infinitely and process returned events.
    fn park(&self) -> io::Result<()>;
    /// Wait with timeout and process returned events.
    fn park_timeout(&self, duration: Duration) -> io::Result<()>;

    /// The struct to wake thread from another.
    #[cfg(feature = "sync")]
    type Unpark: unpark::Unpark;

    /// Get Unpark.
    #[cfg(feature = "sync")]
    fn unpark(&self) -> Self::Unpark;
}

scoped_thread_local!(pub(crate) static CURRENT: Inner);

/// The counters of the current thread's driver, see [`IoUringDriver::stats`].
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub fn driver_stats() -> DriverStats {
    CURRENT.with(|inner| unsafe { (*inner.0.get()).stats() })
}

/// Reset the counters of the current thread's driver, see [`IoUringDriver::reset_stats`].
///
/// # Panics
/// Panics if called outside of a monoio runtime.
pub fn reset_driver_stats() {
    CURRENT.with(|inner| unsafe { (*inner.0.get()).reset_stats() })
}

#[derive(Clone)]
pub(crate) struct Inner(std::rc::Rc<std::cell::UnsafeCell<UringInner>>);

impl Inner {
    fn submit_with<T: OpAble>(&self, data: T) -> io::Result<Op<T>> {
        UringInner::submit_with_data(&self.0, data)
    }

    fn poll_op<T: OpAble>(
        &self,
        _data: &mut T,
        index: usize,
        cx: &mut Context<'_>,
    ) -> Poll<CompletionMeta> {
        UringInner::poll_op(&self.0, index, cx)
    }

    #[inline]
    fn drop_op<T: 'static>(&self, index: usize, data: &mut Option<T>, skip_cancel: bool) {
        UringInner::drop_op(&self.0, index, data, skip_cancel)
    }

    pub(super) unsafe fn cancel_op(&self, op_canceller: &op::OpCanceller) {
        UringInner::cancel_op(&self.0, op_canceller.index)
    }
}

/// The UnparkHandle.
#[cfg(feature = "sync")]
pub(crate) use self::uring::UnparkHandle;

#[cfg(feature = "signal")]
impl UnparkHandle {
    pub(crate) fn current() -> Self {
        CURRENT.with(|inner| UringInner::unpark(&inner.0))
    }
}
