//! Instrumentation switch. Counters, high-water marks and clock reads that exist only to be
//! reported are compiled in only with the `metrics` feature, so the hot paths carry none of them
//! by default.

/// Runs the statements only with the `metrics` feature; expands to nothing otherwise.
///
/// `metric!(inner.stats.enter_wait += 1);`
macro_rules! metric {
    ($($body:tt)*) => {
        #[cfg(feature = "metrics")]
        {
            $($body)*
        }
    };
}
