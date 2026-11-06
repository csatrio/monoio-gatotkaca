use std::{cell::UnsafeCell, collections::VecDeque, marker::PhantomData};

use crate::task::{Schedule, Task};

pub(crate) struct LocalScheduler;

impl Schedule for LocalScheduler {
    fn schedule(&self, task: Task<Self>) {
        crate::runtime::CURRENT.with(|cx| cx.tasks.push(task));
    }

    fn yield_now(&self, task: Task<Self>) {
        self.schedule(task);
    }
}

pub(crate) struct TaskQueue {
    // Local queue.
    queue: UnsafeCell<VecDeque<Task<LocalScheduler>>>,
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
            #[cfg(feature = "metrics")]
            high_water: std::cell::Cell::new(0),
            _marker: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        unsafe { (*self.queue.get()).len() }
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

    #[cfg(feature = "metrics")]
    pub(crate) fn high_water(&self) -> usize {
        self.high_water.get()
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn reset_high_water(&self) {
        self.high_water.set(0);
    }
}
