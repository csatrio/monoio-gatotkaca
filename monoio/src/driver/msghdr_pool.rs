//! Per-thread state of the vectored stream sends (`send_vectored`, `send_vectored_op`): which
//! SQE they use, and a free list of `msghdr`s for the `sendmsg` form.
//!
//! A vectored send points the kernel at an iovec array. Two SQEs can carry one:
//!
//! - `IORING_OP_SEND` with `IORING_SEND_VECTORIZED` in `ioprio` (Linux 6.17+): `addr` is the
//!   array, `len` its count. No `msghdr` at all. The kernel copies the array when it prepares
//!   the request, like the one of `sendmsg`.
//! - `IORING_OP_SENDMSG`: `addr` is a `msghdr` whose `msg_iov` is the array. The kernel copies
//!   the `msghdr` when it prepares the request (inside `io_uring_enter`, or later still on an
//!   SQPOLL ring), not when the SQE is pushed, so it needs an address that is stable from the
//!   push until the op's CQE. The op data moves with its future (the named futures are
//!   returned by value), so the `msghdr` lives on the heap: a [`PooledMsgHdr`] from this free
//!   list instead of a `Box` per send. It is returned when the op data is dropped, which the
//!   driver does only after the CQE (or at once if the SQE was never pushed).
//!
//! Which one a thread uses is decided once per process by a probe ([`vectorized_send`]: a
//! throwaway ring, since there is no feature bit) and can be overridden per thread
//! ([`hooks::set_vectored_send`]). Both forms give the same result and flags.

use std::{cell::Cell, ptr::NonNull, sync::atomic::AtomicU8};

/// `IORING_SEND_VECTORIZED` in `sqe->ioprio`: `addr`/`len` of a `SEND` are an iovec array and
/// its count (Linux 6.17+; older kernels reject the SQE with `EINVAL` when it is prepared).
/// `io-uring` 0.7.15 has no constant for it.
pub(crate) const IORING_SEND_VECTORIZED: u16 = 1 << 5;

/// Idle `msghdr`s a thread keeps. 256 × 64 bytes; a burst of sends beyond that frees the
/// surplus on completion instead of pinning it.
const MAX_IDLE: usize = 256;

/// A `msghdr` with a `next` link, so an idle one is its own free-list node.
#[repr(C)]
struct Node {
    hdr: libc::msghdr,
    next: *mut Node,
}

const MODE_UNKNOWN: u8 = 0;
const MODE_SEND: u8 = 1;
const MODE_SENDMSG: u8 = 2;

/// The thread's state: the SQE form in use, and the free list. One thread-local, so an op
/// pays one lookup.
struct Pool {
    mode: Cell<u8>,
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
            mode: Cell::new(MODE_UNKNOWN),
            head: Cell::new(std::ptr::null_mut()),
            idle: Cell::new(0),
        }
    };
}

/// Whether this thread's vectored sends use a plain `SEND` with `IORING_SEND_VECTORIZED`
/// (true) or `SENDMSG` with a pooled `msghdr` (false). Probes the kernel on the first call of
/// the process.
#[inline]
pub(crate) fn vectorized_send() -> bool {
    POOL.with(|s| match s.mode.get() {
        MODE_SEND => true,
        MODE_SENDMSG => false,
        _ => resolve(s),
    })
}

#[cold]
#[inline(never)]
fn resolve(s: &Pool) -> bool {
    let yes = kernel_supports_vectorized_send();
    s.mode.set(if yes { MODE_SEND } else { MODE_SENDMSG });
    yes
}

/// Once per process: does the kernel accept `IORING_SEND_VECTORIZED`? There is no feature bit,
/// so a throwaway ring submits a `SEND` with the flag, no iovecs, on fd `-1`: a kernel without
/// the flag rejects the SQE when it prepares it (`-EINVAL`), one with it fails later, at
/// issue, on the fd (`-EBADF`). Any failure to run the probe counts as "no" (`sendmsg` always
/// works). A few syscalls, once.
fn kernel_supports_vectorized_send() -> bool {
    use std::sync::atomic::Ordering;

    static KNOWN: AtomicU8 = AtomicU8::new(0);
    match KNOWN.load(Ordering::Relaxed) {
        1 => return true,
        2 => return false,
        _ => {}
    }
    let yes = probe().unwrap_or(false);
    KNOWN.store(if yes { 1 } else { 2 }, Ordering::Relaxed);
    yes
}

fn probe() -> std::io::Result<bool> {
    use io_uring::{opcode, types};

    let mut ring = io_uring::IoUring::new(2)?;
    let sqe = opcode::Send::new(types::Fd(-1), std::ptr::null(), 0)
        .ioprio(IORING_SEND_VECTORIZED)
        .build()
        .user_data(1);
    // SAFETY: the SQE points at nothing (zero iovecs), so there is nothing to keep alive.
    unsafe { ring.submission().push(&sqe) }
        .map_err(|_| std::io::Error::other("probe: submission queue full"))?;
    ring.submit_and_wait(1)?;
    let cqe = ring
        .completion()
        .next()
        .ok_or_else(|| std::io::Error::other("probe: no completion"))?;
    Ok(cqe.result() != -libc::EINVAL)
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
    use super::{MODE_SEND, MODE_SENDMSG, MODE_UNKNOWN, POOL};

    /// Idle `msghdr`s on the calling thread's free list.
    pub fn idle_msghdrs() -> usize {
        POOL.with(|s| s.idle.get())
    }

    /// Choose the SQE form of the calling thread's vectored sends: `Some(true)` the
    /// vectorized `SEND` (every send fails with `EINVAL` on kernels before 6.17),
    /// `Some(false)` `SENDMSG` with a pooled `msghdr`, `None` what the kernel probe says (the
    /// default). Per thread, so parallel tests do not disturb each other.
    pub fn set_vectored_send(mode: Option<bool>) {
        POOL.with(|s| {
            s.mode.set(match mode {
                Some(true) => MODE_SEND,
                Some(false) => MODE_SENDMSG,
                None => MODE_UNKNOWN,
            })
        });
    }

    /// Whether the calling thread's vectored sends use the vectorized `SEND` (probing the
    /// kernel if not decided yet).
    pub fn vectored_send_is_vectorized() -> bool {
        super::vectorized_send()
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
            a.get_mut().msg_iov = std::ptr::without_provenance_mut(8);
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

    #[test]
    fn override_is_per_thread_and_probe_is_stable() {
        use super::hooks::{set_vectored_send, vectored_send_is_vectorized};
        std::thread::spawn(|| {
            set_vectored_send(Some(false));
            assert!(!vectored_send_is_vectorized());
            set_vectored_send(Some(true));
            assert!(vectored_send_is_vectorized());
            set_vectored_send(None);
            let probed = vectored_send_is_vectorized();
            assert_eq!(probed, vectored_send_is_vectorized());
            // Another thread sees the probe's answer, not this thread's overrides.
            let other = std::thread::spawn(vectored_send_is_vectorized)
                .join()
                .unwrap();
            assert_eq!(other, probed);
            println!("IORING_SEND_VECTORIZED supported: {probed}");
        })
        .join()
        .unwrap();
    }
}
