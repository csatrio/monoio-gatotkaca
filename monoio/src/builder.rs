use std::{io, marker::PhantomData};

use crate::driver::IoUringDriver;
use crate::utils::thread_id::gen_id;
use crate::{
    driver::Driver,
    time::{driver::TimeDriver, Clock},
    Runtime,
};

// ===== basic builder structure definition =====

/// Runtime builder
pub struct RuntimeBuilder<D> {
    // iouring entries
    entries: Option<u32>,

    urb: io_uring::Builder,

    // blocking handle
    #[cfg(feature = "sync")]
    blocking_handle: crate::blocking::BlockingHandle,
    // driver mark
    _mark: PhantomData<D>,
}

scoped_thread_local!(pub(crate) static BUILD_THREAD_ID: usize);

impl<T> Default for RuntimeBuilder<T> {
    /// Create a default runtime builder.
    ///
    /// # Note
    ///
    /// When the sync feature is enabled, the default behavior of
    /// [monoio::blocking::BlockingStrategy] is to execute tasks on the local thread. In other
    /// words, there is no thread pool involved—all blocking I/O operations and heavy computations
    /// will block the current thread.
    fn default() -> Self {
        RuntimeBuilder::<T>::new()
    }
}

impl<T> RuntimeBuilder<T> {
    /// Create a default runtime builder.
    ///
    /// # Note
    ///
    /// When the sync feature is enabled, the default behavior of
    /// [monoio::blocking::BlockingStrategy] is to execute tasks on the local thread. In other
    /// words, there is no thread pool involved—all blocking I/O operations and heavy computations
    /// will block the current thread.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: None,

            urb: io_uring::IoUring::builder(),

            #[cfg(feature = "sync")]
            blocking_handle: crate::blocking::BlockingStrategy::ExecuteLocal.into(),
            _mark: PhantomData,
        }
    }
}

// ===== buildable trait and forward methods =====

/// Buildable trait.
pub trait Buildable: Sized {
    /// Build the runtime.
    fn build(this: RuntimeBuilder<Self>) -> io::Result<Runtime<Self>>;
}

macro_rules! direct_build {
    ($ty: ty) => {
        impl RuntimeBuilder<$ty> {
            /// Build the runtime.
            pub fn build(self) -> io::Result<Runtime<$ty>> {
                Buildable::build(self)
            }
        }
    };
}

direct_build!(IoUringDriver);
direct_build!(TimeDriver<IoUringDriver>);

// ===== builder impl =====

impl Buildable for IoUringDriver {
    fn build(this: RuntimeBuilder<Self>) -> io::Result<Runtime<IoUringDriver>> {
        let thread_id = gen_id();
        #[cfg(feature = "sync")]
        let blocking_handle = this.blocking_handle;

        BUILD_THREAD_ID.set(&thread_id, || {
            let driver = match this.entries {
                Some(entries) => IoUringDriver::new_with_entries(&this.urb, entries)?,
                None => IoUringDriver::new(&this.urb)?,
            };
            #[cfg(feature = "sync")]
            let context = crate::runtime::Context::new(blocking_handle);
            #[cfg(not(feature = "sync"))]
            let context = crate::runtime::Context::new();
            Ok(Runtime::new(context, driver))
        })
    }
}

impl<D> RuntimeBuilder<D> {
    const MIN_ENTRIES: u32 = 256;

    /// Set io_uring entries, min size is 256 and the default size is 1024.
    #[must_use]
    pub fn with_entries(mut self, entries: u32) -> Self {
        // If entries is less than 256, it will be 256.
        if entries < Self::MIN_ENTRIES {
            self.entries = Some(Self::MIN_ENTRIES);
            return self;
        }
        self.entries = Some(entries);
        self
    }

    /// Replaces the default [`io_uring::Builder`], which controls the settings for the
    /// inner `io_uring` API.
    ///
    /// Refer to the [`io_uring::Builder`] documentation for all the supported methods.
    #[must_use]
    pub fn uring_builder(mut self, urb: io_uring::Builder) -> Self {
        self.urb = urb;
        self
    }
}

// ===== enable_timer related =====
mod time_wrap {
    pub trait TimeWrapable {}
}

impl time_wrap::TimeWrapable for IoUringDriver {}

impl<D: Driver> Buildable for TimeDriver<D>
where
    D: Buildable,
{
    /// Build the runtime
    fn build(this: RuntimeBuilder<Self>) -> io::Result<Runtime<TimeDriver<D>>> {
        let Runtime {
            driver,
            mut context,
        } = Buildable::build(RuntimeBuilder::<D> {
            entries: this.entries,
            urb: this.urb,
            #[cfg(feature = "sync")]
            blocking_handle: this.blocking_handle,
            _mark: PhantomData,
        })?;

        let timer_driver = TimeDriver::new(driver, Clock::new());
        context.time_handle = Some(timer_driver.handle.clone());
        Ok(Runtime {
            driver: timer_driver,
            context,
        })
    }
}

impl<D: time_wrap::TimeWrapable> RuntimeBuilder<D> {
    /// Enable all(currently only timer)
    #[must_use]
    pub fn enable_all(self) -> RuntimeBuilder<TimeDriver<D>> {
        self.enable_timer()
    }

    /// Enable timer
    #[must_use]
    pub fn enable_timer(self) -> RuntimeBuilder<TimeDriver<D>> {
        let Self {
            entries,
            urb,
            #[cfg(feature = "sync")]
            blocking_handle,
            ..
        } = self;
        RuntimeBuilder {
            entries,
            urb,
            #[cfg(feature = "sync")]
            blocking_handle,
            _mark: PhantomData,
        }
    }
}

impl<D> RuntimeBuilder<D> {
    /// Attach thread pool, this will overwrite blocking strategy.
    /// All `spawn_blocking` will be executed on given thread pool.
    #[cfg(feature = "sync")]
    #[must_use]
    pub fn attach_thread_pool(
        mut self,
        tp: Box<dyn crate::blocking::ThreadPool + Send + 'static>,
    ) -> Self {
        self.blocking_handle = crate::blocking::BlockingHandle::Attached(tp);
        self
    }

    /// Set blocking strategy, this will overwrite thread pool setting.
    /// If `BlockingStrategy::Panic` is used, it will panic if `spawn_blocking` on this thread.
    /// If `BlockingStrategy::ExecuteLocal` is used, it will execute with current thread, and may
    /// cause tasks high latency.
    /// Attaching a thread pool is recommended if `spawn_blocking` will be used.
    #[cfg(feature = "sync")]
    #[must_use]
    pub fn with_blocking_strategy(mut self, strategy: crate::blocking::BlockingStrategy) -> Self {
        self.blocking_handle = crate::blocking::BlockingHandle::Empty(strategy);
        self
    }
}
