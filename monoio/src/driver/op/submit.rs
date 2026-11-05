//! Submit helpers that give the op data back when submission fails.

use std::io;

use io_uring::opcode;

use super::{Op, OpAble};
use crate::driver;

/// Result of a submit that hands the buffer back on failure.
pub(crate) type SubmitResult<O, B> = Result<O, (io::Error, B)>;

/// A no-op used to flush a full submission queue.
struct Flush;

impl OpAble for Flush {
    // A nop needs no cancel when dropped.
    const SKIP_CANCEL: bool = true;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        opcode::Nop::new().build()
    }
}

impl<T: OpAble> Op<T> {
    /// Like `submit_with`, but returns `data` with the error.
    ///
    /// `submit_with` only fails while flushing a full SQ, before it takes `data`. So when the SQ
    /// is full we first flush it with a cheap nop op (same driver path: tick and retry on
    /// `EBUSY`). If that fails, `data` is still ours to return. After it, the SQ has room and
    /// `submit_with` cannot fail.
    pub(crate) fn submit_or_return(data: T) -> SubmitResult<Op<T>, T> {
        let full = driver::CURRENT.with(|inner| inner.with_ring(|r| r.submission().is_full()));
        if full {
            match Op::submit_with(Flush) {
                // The nop completes on its own; dropping the handle is fine.
                Ok(op) => drop(op),
                Err(e) => return Err((e, data)),
            }
        }
        match Op::submit_with(data) {
            Ok(op) => Ok(op),
            // Unreachable today: the SQ has room, so `submit_with` does not flush.
            Err(e) => panic!("submit failed after the SQ was flushed: {e}"),
        }
    }
}
