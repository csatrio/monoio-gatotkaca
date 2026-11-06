use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    marker::PhantomData,
};

use crate::task::{Schedule, Task};

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
            use_slot: Cell::new(false),
            #[cfg(feature = "metrics")]
            high_water: std::cell::Cell::new(0),
            _marker: PhantomData,
        }
    }

    /// Queued tasks, the LIFO slot included.
    pub(crate) fn len(&self) -> usize {
        unsafe { (*self.queue.get()).len() + (*self.slot.get()).is_some() as usize }
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
        self.push(task);
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
