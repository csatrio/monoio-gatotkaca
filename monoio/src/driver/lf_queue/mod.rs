//! A lock-free, many-producer queue drained in batches, shared by `uring::event_channel` and the
//! driver's remote-waker queue.
//!
//! - Messages live in nodes of an mmap'd [`arena`]; a producer takes a free node with one CAS.
//! - The inbox is a Treiber stack (`adaptif-gatotkaca-utils`): a producer pushes with one CAS,
//!   a [`Consumer`] takes the whole batch with one swap and reverses it into push order.
//! - The consumer returns the nodes of a batch to the free list with one CAS.
//!
//! Wakeups are not part of the queue: each user adds its own.

mod arena;

use std::{io, ptr, sync::atomic::AtomicPtr};

use adaptif_gatotkaca_utils::crossbeam_utils::CachePadded;
use adaptif_gatotkaca_utils::{GkTreiberPtr, GkTreiberStackIterator, TreiberStackIterator};
use arena::{Arena, Node};

/// Largest `max` capacity.
pub(crate) const LIMIT: u32 = 1 << 31;
/// Nodes a consumer collects before returning them to the free list.
const FREE_BATCH: u32 = 64;

pub(crate) struct LfQueue<T> {
    /// Treiber stack of pushed messages, newest first.
    inbox: CachePadded<AtomicPtr<Node<T>>>,
    arena: Arena<T>,
}

// SAFETY: values (`T: Send`) move between threads through the atomics.
unsafe impl<T: Send> Send for LfQueue<T> {}
unsafe impl<T: Send> Sync for LfQueue<T> {}

impl<T> LfQueue<T> {
    /// `initial` slots mapped now (rounded up to a power of two), growing by doubling up to
    /// `max` (at most [`LIMIT`]).
    pub(crate) fn new(initial: u32, max: u32) -> io::Result<Self> {
        let invalid = |msg| io::Error::new(io::ErrorKind::InvalidInput, msg);
        if initial == 0 || max == 0 {
            return Err(invalid("capacity must not be 0"));
        }
        if max > LIMIT {
            return Err(invalid("max_capacity above 2^31"));
        }
        let initial = initial.checked_next_power_of_two().unwrap_or(LIMIT);
        if initial > max.next_power_of_two() {
            return Err(invalid("initial_capacity above max_capacity"));
        }
        Ok(Self {
            inbox: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            arena: Arena::new(initial.min(LIMIT / 2), max)?,
        })
    }

    /// Queue `value`, or give it back when `max` values are queued. Never blocks (a producer
    /// that must map a new segment takes the arena's growth lock).
    pub(crate) fn push(&self, value: T) -> Result<(), T> {
        let Some(node) = self.arena.alloc() else {
            return Err(value);
        };
        // SAFETY: `alloc` gave us the node; it holds no value.
        unsafe { node.write(value) };
        self.inbox
            .push_to_stack(node as *const Node<T> as *mut Node<T>);
        Ok(())
    }

    /// No value is waiting in the inbox (a consumer may still hold a batch).
    pub(crate) fn is_empty(&self) -> bool {
        self.inbox.is_empty()
    }

    /// Slots currently mapped.
    pub(crate) fn capacity(&self) -> u32 {
        self.arena.capacity()
    }
}

impl<T> Drop for LfQueue<T> {
    fn drop(&mut self) {
        // Drop the values nobody took; the arena unmaps the memory afterwards.
        drop(self.inbox.reap_all().reversed_iterator());
    }
}

/// The receiving side of an [`LfQueue`]: the batch being handed out and the nodes to return.
///
/// Use it with one queue only. Several consumers on one queue are safe (each takes whole
/// batches), but values are then only ordered within a consumer.
pub(crate) struct Consumer<T> {
    batch: Option<TreiberStackIterator<Node<T>>>,
    /// Nodes taken but not yet returned: `(first, last, count)` of a `next_free` chain.
    freed: Option<(u32, u32, u32)>,
    /// Hand values out in push order; otherwise newest first, which skips a pass over the
    /// batch.
    fifo: bool,
}

impl<T> Consumer<T> {
    /// Values come out in push order (per producer).
    pub(crate) fn fifo() -> Self {
        Self {
            batch: None,
            freed: None,
            fifo: true,
        }
    }

    /// Values come out in any order (newest first within a batch). Cheaper: the batch is not
    /// reversed.
    // Used by the remote-waker queue (feature `sync`).
    #[cfg_attr(not(feature = "sync"), allow(dead_code))]
    pub(crate) fn unordered() -> Self {
        Self {
            fifo: false,
            ..Self::fifo()
        }
    }

    /// The next value in push order (per producer), or `None` if `queue` is empty. Returns the
    /// nodes of a finished batch to the free list before looking for more.
    pub(crate) fn pop(&mut self, queue: &LfQueue<T>) -> Option<T> {
        loop {
            if let Some(node) = self.batch.as_mut().and_then(Iterator::next) {
                // SAFETY: the node came from the inbox: it holds a value and is ours now.
                let node = unsafe { &*node };
                let value = unsafe { node.take() };
                self.free_later(queue, node.index());
                return Some(value);
            }
            self.batch = None;
            self.flush(queue);
            // Load first: swapping an empty head would still take the cache line from the
            // producers.
            if queue.inbox.is_empty() {
                return None;
            }
            let chain = queue.inbox.reap_all();
            self.batch = Some(if self.fifo {
                chain.iterator()
            } else {
                chain.reversed_iterator()
            });
        }
    }

    fn free_later(&mut self, queue: &LfQueue<T>, index: u32) {
        self.freed = Some(match self.freed {
            None => (index, index, 1),
            Some((first, last, count)) => {
                queue.arena.link_free(index, first);
                (index, last, count + 1)
            }
        });
        if matches!(self.freed, Some((_, _, n)) if n >= FREE_BATCH) {
            self.flush(queue);
        }
    }

    /// Return the taken nodes to `queue` with one CAS.
    pub(crate) fn flush(&mut self, queue: &LfQueue<T>) {
        if let Some((first, last, _)) = self.freed.take() {
            queue.arena.free_chain(first, last);
        }
    }

    /// Drop the rest of the current batch (values dropped, nodes returned at the queue's end).
    pub(crate) fn clear(&mut self, queue: &LfQueue<T>) {
        self.batch = None;
        self.flush(queue);
    }
}
