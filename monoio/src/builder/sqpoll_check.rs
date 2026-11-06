//! SQPOLL placement check for [`RuntimeBuilder::sqpoll`](super::RuntimeBuilder::sqpoll).
//!
//! The SQ poll thread busy-polls the submission queue. If it can only run on the CPU the event
//! loop is pinned to, the two compete for one core (measured: about −85% throughput and client
//! timeouts). On Linux 7.0 an SQ thread created without `IORING_SETUP_SQ_AFF` inherits the
//! affinity of the thread that creates the ring (checked by `tests/sqpoll.rs`), so a loop pinned
//! to one CPU with an unpinned SQ thread shares that CPU too.

use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

/// Settings recorded by `sqpoll` / `deny_sqpoll_conflict`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SqpollSettings {
    /// `(idle, cpu)` when `sqpoll` was called.
    pub(crate) cfg: Option<(Duration, Option<u32>)>,
    /// Fail `build()` on a conflict instead of warning.
    pub(crate) deny: bool,
}

static WARNED: AtomicBool = AtomicBool::new(false);

impl SqpollSettings {
    /// Check the placement and apply the settings to `urb`. Called at `build()` on the thread
    /// that creates the ring.
    pub(crate) fn apply(&self, urb: &mut io_uring::Builder) -> io::Result<()> {
        let Some((idle, cpu)) = self.cfg else {
            return Ok(());
        };
        if let Some(msg) = conflict(thread_affinity().as_deref(), cpu) {
            if self.deny {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
            }
            if !WARNED.swap(true, Ordering::Relaxed) {
                eprintln!("monoio warning: {msg} (shown once per process)");
            }
        }
        let idle_ms = u32::try_from(idle.as_millis()).unwrap_or(u32::MAX);
        urb.setup_sqpoll(idle_ms);
        if let Some(cpu) = cpu {
            urb.setup_sqpoll_cpu(cpu);
        }
        Ok(())
    }
}

/// The CPUs the calling thread may run on, or `None` if they can't be read.
pub(crate) fn thread_affinity() -> Option<Vec<u32>> {
    // Safety: `cpu_set_t` is plain data; zeroed is an empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // Safety: pid 0 is the calling thread; the size matches `set`.
    let rc =
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if rc != 0 {
        // EINVAL with more than 1024 CPUs: skip the check rather than guess.
        return None;
    }
    let max = 8 * std::mem::size_of::<libc::cpu_set_t>();
    // Safety: `i` is below the set's capacity.
    Some(
        (0..max)
            .filter(|&i| unsafe { libc::CPU_ISSET(i, &set) })
            .map(|i| i as u32)
            .collect(),
    )
}

/// The conflict message, if the SQ poll thread would share the loop's only CPU.
pub(crate) fn conflict(mask: Option<&[u32]>, cpu: Option<u32>) -> Option<String> {
    let &[only] = mask? else {
        return None;
    };
    match cpu {
        None => Some(format!(
            "SQPOLL conflict: the runtime thread is pinned to CPU {only} and the SQ poll thread \
             has no CPU of its own, so it inherits that one-CPU mask and competes with the event \
             loop for one core; pin it to another CPU with `sqpoll(idle, Some(cpu))`, or drop \
             SQPOLL"
        )),
        Some(c) if c == only => Some(format!(
            "SQPOLL conflict: the SQ poll thread is pinned to CPU {c}, the only CPU the runtime \
             thread may run on, so it competes with the event loop for one core; pin it to \
             another CPU or drop SQPOLL"
        )),
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::conflict;

    #[test]
    fn rules() {
        assert!(conflict(Some(&[3]), None).is_some());
        assert!(conflict(Some(&[3]), Some(3)).is_some());
        assert!(conflict(Some(&[3]), Some(4)).is_none());
        assert!(conflict(Some(&[2, 3]), None).is_none());
        assert!(conflict(Some(&[2, 3]), Some(3)).is_none());
        assert!(conflict(None, None).is_none());
    }
}
