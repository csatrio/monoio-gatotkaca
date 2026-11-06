use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    marker::PhantomData,
};

use crate::task::{Schedule, Task};

// `small_io_priority`: the threshold of the running runtime (0 = off), and the class of the CQE
// the driver is completing (`HINT_*`). Thread-locals because the driver has no access to the
// runtime's queue; both are only written when the option is on.
#[cfg(feature = "unstable")]
#[thread_local]
static SMALL_IO_MAX: Cell<u32> = Cell::new(0);
#[cfg(feature = "unstable")]
#[thread_local]
static CQE_HINT: Cell<u8> = Cell::new(HINT_NONE);

#[cfg(not(feature = "unstable"))]
thread_local! {
    static SMALL_IO_MAX: Cell<u32> = const { Cell::new(0) };
    static CQE_HINT: Cell<u8> = const { Cell::new(HINT_NONE) };
}

/// Not inside a CQE completion: a wake by a task, a timer, another thread.
const HINT_NONE: u8 = 0;
/// Completion with a result below the threshold (or an error).
const HINT_SMALL: u8 = 1;
/// Completion of at least the threshold: bulk I/O.
const HINT_BULK: u8 = 2;

/// A bulk completion marks its task as a bulk task until it has been woken by this many small
/// completions in a row; until then its completion wakes use the normal queue. Covers the small
/// steps of a bulk transfer cycle (read the next request, open/close a file) between its large
/// reads/writes, so tasks that move bulk data don't get the priority between chunks.
pub(crate) const BULK_STICKY_WAKES: u8 = 8;

/// Threshold of `RuntimeBuilder::small_io_priority` for this thread's runtime (0: off).
#[inline]
pub(crate) fn small_io_max() -> u32 {
    SMALL_IO_MAX.get()
}

/// Set by `block_on` for the runtime it drives.
#[inline]
pub(crate) fn set_small_io_max(n: u32) {
    SMALL_IO_MAX.set(n);
    CQE_HINT.set(HINT_NONE);
}

/// The driver is about to complete an op with result `res` (only called with the option on):
/// classify the wakes it causes.
#[inline]
pub(crate) fn set_cqe_hint(res: i32, max: u32) {
    CQE_HINT.set(if res < 0 || (res as u32) < max {
        HINT_SMALL
    } else {
        HINT_BULK
    });
}

/// The driver finished completing that op: later wakes are not CQE wakes.
#[inline]
pub(crate) fn clear_cqe_hint() {
    CQE_HINT.set(HINT_NONE);
}

pub(crate) struct LocalScheduler;

impl Schedule for LocalScheduler {
    /// A task was woken (by a waker, not by itself).
    fn schedule(&self, task: Task<Self>) {
        crate::runtime::CURRENT.with(|cx| cx.tasks.push_woken(task));
    }

    /// The task woke itself while it ran (`yield_now`, a coop yield): always the back of the
    /// queue, never the LIFO slot.
    fn yield_now(&self, task: Task<Self>) {
        crate::runtime::CURRENT.with(|cx| cx.tasks.push(task));
    }
}

pub(crate) struct TaskQueue {
    // Local queue.
    queue: UnsafeCell<VecDeque<Task<LocalScheduler>>>,
    // LIFO slot: the task most recently woken by the running task (`lifo_slot` only).
    slot: UnsafeCell<Option<Task<LocalScheduler>>>,
    // Tasks woken by a small completion (`small_io_priority` only), run before `queue`.
    prio: UnsafeCell<VecDeque<Task<LocalScheduler>>>,
    // `small_io_priority` is on.
    prio_on: Cell<bool>,
    // `lifo_slot` is on and a task is being polled: wakes go to `slot`.
    use_slot: Cell<bool>,
    // Highest length seen after a push.
    #[cfg(feature = "metrics")]
    high_water: std::cell::Cell<usize>,
    // Make sure the type is `!Send` and `!Sync`.
    _marker: PhantomData<*const ()>,
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TaskQueue {
    fn drop(&mut self) {
        unsafe {
            let queue = &mut *self.queue.get();
            while let Some(_task) = queue.pop_front() {}
            drop((*self.slot.get()).take());
            let prio = &mut *self.prio.get();
            while let Some(_task) = prio.pop_front() {}
        }
    }
}

impl TaskQueue {
    pub(crate) fn new() -> Self {
        const DEFAULT_TASK_QUEUE_SIZE: usize = 4096;
        Self::new_with_capacity(DEFAULT_TASK_QUEUE_SIZE)
    }
    pub(crate) fn new_with_capacity(capacity: usize) -> Self {
        Self {
            queue: UnsafeCell::new(VecDeque::with_capacity(capacity)),
            slot: UnsafeCell::new(None),
            prio: UnsafeCell::new(VecDeque::new()),
            prio_on: Cell::new(false),
            use_slot: Cell::new(false),
            #[cfg(feature = "metrics")]
            high_water: std::cell::Cell::new(0),
            _marker: PhantomData,
        }
    }

    /// Queued tasks, the LIFO slot and the priority queue included.
    pub(crate) fn len(&self) -> usize {
        unsafe {
            (*self.queue.get()).len()
                + (*self.slot.get()).is_some() as usize
                + (*self.prio.get()).len()
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn push(&self, runnable: Task<LocalScheduler>) {
        unsafe {
            (*self.queue.get()).push_back(runnable);
        }
        metric!(
            let len = self.len();
            if len > self.high_water.get() {
                self.high_water.set(len);
            }
        );
    }

    pub(crate) fn pop(&self) -> Option<Task<LocalScheduler>> {
        unsafe { (*self.queue.get()).pop_front() }
    }

    /// Next task outside the LIFO slot: the priority queue first while `prio_left > 0` (each
    /// such pop spends one), then the queue, then the priority queue regardless of
    /// `prio_left` (nothing else is ready). Without `small_io_priority` the priority queue is
    /// always empty and this is [`pop`](Self::pop).
    #[inline]
    pub(crate) fn pop_next(&self, prio_left: &mut usize) -> Option<Task<LocalScheduler>> {
        if *prio_left > 0 {
            if let Some(t) = unsafe { (*self.prio.get()).pop_front() } {
                *prio_left -= 1;
                return Some(t);
            }
        }
        if let Some(t) = self.pop() {
            return Some(t);
        }
        unsafe { (*self.prio.get()).pop_front() }
    }

    /// Enable the priority queue for tasks woken by small completions (`small_io_priority`).
    pub(crate) fn set_prio(&self, on: bool) {
        self.prio_on.set(on);
    }

    /// A task woken by a waker: into the LIFO slot while a task runs with `lifo_slot` on (the
    /// previous occupant goes to the back of the queue), else to the back of the queue.
    #[inline]
    pub(crate) fn push_woken(&self, task: Task<LocalScheduler>) {
        if self.use_slot.get() {
            // Safety: `!Sync`, and nothing here re-enters the queue (dropping is not possible:
            // the displaced task is pushed).
            if let Some(prev) = unsafe { (*self.slot.get()).replace(task) } {
                self.push(prev);
            }
            return;
        }
        if self.prio_on.get() && self.cqe_priority(&task) {
            // Safety: as above.
            unsafe { (*self.prio.get()).push_back(task) };
            metric!(
                let len = self.len();
                if len > self.high_water.get() {
                    self.high_water.set(len);
                }
            );
            return;
        }
        self.push(task);
    }

    /// `small_io_priority`: whether `task`, being woken now, goes to the priority queue: a
    /// small completion woke it and it is not a bulk task. Updates its bulk mark.
    #[inline]
    fn cqe_priority(&self, task: &Task<LocalScheduler>) -> bool {
        let bulk = task.bulk();
        match CQE_HINT.get() {
            HINT_SMALL => match bulk.get() {
                0 => true,
                n => {
                    bulk.set(n - 1);
                    false
                }
            },
            HINT_BULK => {
                bulk.set(BULK_STICKY_WAKES);
                false
            }
            _ => false,
        }
    }

    /// While `on`, [`push_woken`](Self::push_woken) uses the LIFO slot. The runtime sets it
    /// around each task poll when `lifo_slot` is enabled.
    #[inline]
    pub(crate) fn set_use_slot(&self, on: bool) {
        self.use_slot.set(on);
    }

    /// Take the LIFO slot task.
    #[inline]
    pub(crate) fn take_slot(&self) -> Option<Task<LocalScheduler>> {
        unsafe { (*self.slot.get()).take() }
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn high_water(&self) -> usize {
        self.high_water.get()
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn reset_high_water(&self) {
        self.high_water.set(0);
    }
}
