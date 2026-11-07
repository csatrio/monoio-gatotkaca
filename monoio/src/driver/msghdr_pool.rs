//! A per-thread free list of `msghdr`s for the vectored stream sends (`send_vectored`,
//! `send_vectored_op`).
//!
//! `IORING_OP_SENDMSG` points at a `msghdr` whose `msg_iov` is the iovec array. The kernel
//! copies the `msghdr` when it prepares the request (inside `io_uring_enter`, or later still
//! on an SQPOLL ring), not when the SQE is pushed, so it needs an address that is stable from
//! the push until the op's CQE. The op data moves with its future (the named futures are
//! returned by value), so the `msghdr` lives on the heap: a [`PooledMsgHdr`] from this free
//! list instead of a `Box` per send. It is returned when the op data is dropped, which the
//! driver does only after the CQE (or at once if the SQE was never pushed).

use std::{cell::Cell, ptr::NonNull};

/// Idle `msghdr`s a thread keeps. 256 × 64 bytes; a burst of sends beyond that frees the
/// surplus on completion instead of pinning it.
const MAX_IDLE: usize = 256;

/// A `msghdr` with a `next` link, so an idle one is its own free-list node.
#[repr(C)]
struct Node {
    hdr: libc::msghdr,
    next: *mut Node,
}

/// The thread's free list.
struct Pool {
    head: Cell<*mut Node>,
    idle: Cell<usize>,
}

impl Drop for Pool {
    fn drop(&mut self) {
        let mut p = self.head.get();
        while !p.is_null() {
            // SAFETY: every node on the list came from `Box::leak` in `alloc` and is on
            // the list exactly once.
            let node = unsafe { Box::from_raw(p) };
            p = node.next;
        }
    }
}

thread_local! {
    static POOL: Pool = const {
        Pool {
            head: Cell::new(std::ptr::null_mut()),
            idle: Cell::new(0),
        }
    };
}

/// A `msghdr` on the heap, taken from the thread's free list and returned to it on drop. Its
/// address is stable for its lifetime. Only `msg_iov` and `msg_iovlen` are ever set; the rest
/// stays zero (no name, no control data).
pub(crate) struct PooledMsgHdr(NonNull<Node>);

impl PooledMsgHdr {
    /// A zeroed `msghdr` (but for `msg_iov`/`msg_iovlen`, which the op sets): from the free
    /// list, or a new box when it is empty.
    #[inline]
    pub(crate) fn take() -> Self {
        let p = POOL.with(|s| {
            let p = s.head.get();
            if !p.is_null() {
                // SAFETY: `p` is a live node on the list (see `Drop for Pool`).
                s.head.set(unsafe { (*p).next });
                s.idle.set(s.idle.get() - 1);
            }
            p
        });
        match NonNull::new(p) {
            Some(node) => Self(node),
            None => Self(alloc()),
        }
    }

    /// The `msghdr` for an SQE.
    #[inline]
    pub(crate) fn as_ptr(&self) -> *const libc::msghdr {
        self.0.as_ptr().cast()
    }

    #[inline]
    pub(crate) fn get_mut(&mut self) -> &mut libc::msghdr {
        // SAFETY: the node is uniquely owned by `self` while it is not on the free list.
        unsafe { &mut (*self.0.as_ptr()).hdr }
    }
}

#[cold]
#[inline(never)]
fn alloc() -> NonNull<Node> {
    // SAFETY: an all-zero `msghdr` is valid (no name, no control data), and a null `next`.
    let node = Box::new(Node {
        hdr: unsafe { std::mem::zeroed() },
        next: std::ptr::null_mut(),
    });
    NonNull::from(Box::leak(node))
}

impl Drop for PooledMsgHdr {
    #[inline]
    fn drop(&mut self) {
        let p = self.0.as_ptr();
        // `try_with`: the thread may be tearing down its locals (a future dropped by a
        // thread-local destructor); the box is freed then.
        let kept = POOL
            .try_with(|s| {
                if s.idle.get() >= MAX_IDLE {
                    return false;
                }
                // SAFETY: `p` is owned by `self` and not on the list.
                unsafe { (*p).next = s.head.get() };
                s.head.set(p);
                s.idle.set(s.idle.get() + 1);
                true
            })
            .unwrap_or(false);
        if !kept {
            // SAFETY: `p` came from `Box::leak` and is owned by `self`.
            drop(unsafe { Box::from_raw(p) });
        }
    }
}

/// Hooks for the tests and measurements of the vectored sends (`monoio::__vectored`). Not
/// API.
pub mod hooks {
    /// Idle `msghdr`s on the calling thread's free list.
    pub fn idle_msghdrs() -> usize {
        super::POOL.with(|s| s.idle.get())
    }
}

#[cfg(test)]
mod tests {
    use super::{hooks::idle_msghdrs, *};

    #[test]
    fn free_list_reuses_and_caps() {
        std::thread::spawn(|| {
            let a = PooledMsgHdr::take();
            let a_ptr = a.as_ptr();
            assert_eq!(idle_msghdrs(), 0);
            drop(a);
            assert_eq!(idle_msghdrs(), 1);
            let b = PooledMsgHdr::take();
            assert_eq!(b.as_ptr(), a_ptr, "the idle msghdr is reused");
            assert_eq!(idle_msghdrs(), 0);
            drop(b);

            let many: Vec<_> = (0..MAX_IDLE + 10).map(|_| PooledMsgHdr::take()).collect();
            assert_eq!(idle_msghdrs(), 0);
            drop(many);
            assert_eq!(idle_msghdrs(), MAX_IDLE, "surplus is freed, not kept");
        })
        .join()
        .unwrap();
    }

    #[test]
    fn taken_msghdr_is_zeroed_but_for_the_iovecs() {
        std::thread::spawn(|| {
            let mut a = PooledMsgHdr::take();
            a.get_mut().msg_iov = 8 as *mut _;
            a.get_mut().msg_iovlen = 3;
            drop(a);
            let b = PooledMsgHdr::take();
            // SAFETY: a valid, owned msghdr.
            let hdr = unsafe { &*b.as_ptr() };
            assert!(hdr.msg_name.is_null());
            assert_eq!(hdr.msg_namelen, 0);
            assert!(hdr.msg_control.is_null());
            assert_eq!(hdr.msg_controllen, 0);
            assert_eq!(hdr.msg_flags, 0);
        })
        .join()
        .unwrap();
    }
}
