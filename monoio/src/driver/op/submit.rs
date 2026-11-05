//! Submit helpers that give the op data back when submission fails.

use std::io;

use super::{Op, OpAble};
use crate::driver::{self, uring::UringInner};

/// Result of a submit that hands the buffer back on failure.
pub(crate) type SubmitResult<O, B> = Result<O, (io::Error, B)>;

impl<T: OpAble> Op<T> {
    /// Like `submit_with`, but returns `data` with the error (the SQ stayed full after the
    /// driver's flush retries).
    pub(crate) fn submit_or_return(data: T) -> SubmitResult<Op<T>, T> {
        driver::CURRENT.with(|inner| UringInner::try_submit_with_data(&inner.0, data))
    }
}
