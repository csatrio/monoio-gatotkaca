use std::future::Future;

use crate::{
    driver::Driver,
    scheduler::{LocalScheduler, TaskQueue},
    task::{
        new_task,
        waker_fn::{dummy_waker, set_poll, should_poll},
        JoinHandle,
    },
    time::driver::Handle as TimeHandle,
};

#[cfg(feature = "sync")]
thread_local! {
    pub(crate) static DEFAULT_CTX: Context = Context {
        thread_id: crate::utils::thread_id::DEFAULT_THREAD_ID,
        unpark_cache: std::cell::RefCell::new(rustc_hash::FxHashMap::default()),
        waker_sender_cache: std::cell::RefCell::new(rustc_hash::FxHashMap::default()),
        tasks: Default::default(),
        time_handle: None,
        blocking_handle: crate::blocking::BlockingHandle::Empty(crate::blocking::BlockingStrategy::Panic),
        task_budget: None,
    };
}

scoped_thread_local!(pub(crate) static CURRENT: Context);

pub(crate) struct Context {
    /// Owned task set and local run queue
    pub(crate) tasks: TaskQueue,

    /// Thread id(not the kernel thread id but a generated unique number)
    pub(crate) thread_id: usize,

    /// Thread unpark handles
    #[cfg(feature = "sync")]
    pub(crate) unpark_cache:
        std::cell::RefCell<rustc_hash::FxHashMap<usize, crate::driver::UnparkHandle>>,

    /// Waker sender cache
    #[cfg(feature = "sync")]
    pub(crate) waker_sender_cache:
        std::cell::RefCell<rustc_hash::FxHashMap<usize, flume::Sender<std::task::Waker>>>,

    /// Time Handle
    pub(crate) time_handle: Option<TimeHandle>,

    /// Blocking Handle
    #[cfg(feature = "sync")]
    pub(crate) blocking_handle: crate::blocking::BlockingHandle,

    /// Tasks run before the driver is checked again; `None`: about twice the queue length.
    pub(crate) task_budget: Option<usize>,
}

impl Context {
    #[cfg(feature = "sync")]
    pub(crate) fn new(blocking_handle: crate::blocking::BlockingHandle) -> Self {
        let thread_id = crate::builder::BUILD_THREAD_ID.with(|id| *id);

        Self {
            thread_id,
            unpark_cache: std::cell::RefCell::new(rustc_hash::FxHashMap::default()),
            waker_sender_cache: std::cell::RefCell::new(rustc_hash::FxHashMap::default()),
            tasks: TaskQueue::default(),
            time_handle: None,
            blocking_handle,
            task_budget: None,
        }
    }

    #[cfg(not(feature = "sync"))]
    pub(crate) fn new() -> Self {
        let thread_id = crate::builder::BUILD_THREAD_ID.with(|id| *id);

        Self {
            thread_id,
            tasks: TaskQueue::default(),
            time_handle: None,
            task_budget: None,
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn unpark_thread(&self, id: usize) {
        use crate::driver::{thread::get_unpark_handle, unpark::Unpark};
        if let Some(handle) = self.unpark_cache.borrow().get(&id) {
            let _ = handle.unpark();
            return;
        }

        if let Some(v) = get_unpark_handle(id) {
            // Write back to local cache
            let w = v.clone();
            self.unpark_cache.borrow_mut().insert(id, w);
            let _ = v.unpark();
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn send_waker(&self, id: usize, w: std::task::Waker) {
        use crate::driver::thread::get_waker_sender;
        if let Some(sender) = self.waker_sender_cache.borrow().get(&id) {
            let _ = sender.send(w);
            return;
        }

        if let Some(s) = get_waker_sender(id) {
            // Write back to local cache
            let _ = s.send(w);
            self.waker_sender_cache.borrow_mut().insert(id, s);
        }
    }
}

/// Monoio runtime
pub struct Runtime<D> {
    pub(crate) context: Context,
    pub(crate) driver: D,
}

impl<D> Runtime<D> {
    pub(crate) fn new(context: Context, driver: D) -> Self {
        Self { context, driver }
    }

    /// The driver of this runtime.
    pub fn driver(&self) -> &D {
        &self.driver
    }

    /// Block on
    pub fn block_on<F>(&mut self, future: F) -> F::Output
    where
        F: Future,
        D: Driver,
    {
        assert!(
            !CURRENT.is_set(),
            "Can not start a runtime inside a runtime"
        );

        let waker = dummy_waker();
        let cx = &mut std::task::Context::from_waker(&waker);

        self.driver.with(|| {
            CURRENT.set(&self.context, || {
                #[cfg(feature = "sync")]
                let join = unsafe { spawn_without_static(future) };
                #[cfg(not(feature = "sync"))]
                let join = future;

                let mut join = std::pin::pin!(join);
                set_poll();
                loop {
                    loop {
                        // Consume all tasks(with max round to prevent io starvation)
                        // (with an explicit budget of `n`, exactly `n` tasks run)
                        let mut max_round = match self.context.task_budget {
                            Some(n) => n.saturating_sub(1),
                            None => self.context.tasks.len() * 2,
                        };
                        while let Some(t) = self.context.tasks.pop() {
                            t.run();
                            if max_round == 0 {
                                // maybe there's a looping task
                                break;
                            } else {
                                max_round -= 1;
                            }
                        }

                        // Check main future
                        while should_poll() {
                            // check if ready
                            if let std::task::Poll::Ready(t) = join.as_mut().poll(cx) {
                                return t;
                            }
                        }

                        if self.context.tasks.is_empty() {
                            // No task to execute, we should wait for io blockingly
                            // Hot path
                            break;
                        }

                        // Cold path
                        let _ = self.driver.submit();
                    }

                    // Wait and Process CQ(the error is ignored for not debug mode)
                    #[cfg(not(all(debug_assertions, feature = "debug")))]
                    let _ = self.driver.park();

                    #[cfg(all(debug_assertions, feature = "debug"))]
                    if let Err(e) = self.driver.park() {
                        trace!("park error: {:?}", e);
                    }
                }
            })
        })
    }
}

/// Spawns a new asynchronous task, returning a [`JoinHandle`] for it.
///
/// Spawning a task enables the task to execute concurrently to other tasks.
/// There is no guarantee that a spawned task will execute to completion. When a
/// runtime is shutdown, all outstanding tasks are dropped, regardless of the
/// lifecycle of that task.
///
///
/// [`JoinHandle`]: super::task::JoinHandle
///
/// # Examples
///
/// In this example, a server is started and `spawn` is used to start a new task
/// that processes each received connection.
///
/// ```no_run
/// #[monoio::main]
/// async fn main() {
///     let handle = monoio::spawn(async {
///         println!("hello from a background task");
///     });
///
///     // Let the task complete
///     handle.await;
/// }
/// ```
pub fn spawn<T>(future: T) -> JoinHandle<T::Output>
where
    T: Future + 'static,
    T::Output: 'static,
{
    let (task, join) = new_task(
        crate::utils::thread_id::get_current_thread_id(),
        future,
        LocalScheduler,
    );

    CURRENT.with(|ctx| {
        ctx.tasks.push(task);
    });
    join
}

#[cfg(feature = "sync")]
unsafe fn spawn_without_static<T>(future: T) -> JoinHandle<T::Output>
where
    T: Future,
{
    use crate::task::new_task_holding;
    let (task, join) = new_task_holding(
        crate::utils::thread_id::get_current_thread_id(),
        future,
        LocalScheduler,
    );

    CURRENT.with(|ctx| {
        ctx.tasks.push(task);
    });
    join
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "sync")]
    #[test]
    fn across_thread() {
        use futures::channel::oneshot;

        use crate::driver::IoUringDriver;

        let (tx1, rx1) = oneshot::channel::<u8>();
        let (tx2, rx2) = oneshot::channel::<u8>();

        std::thread::spawn(move || {
            let mut rt = crate::RuntimeBuilder::<IoUringDriver>::new()
                .build()
                .unwrap();
            rt.block_on(async move {
                let n = rx1.await.expect("unable to receive rx1");
                assert!(tx2.send(n).is_ok());
            });
        });

        let mut rt = crate::RuntimeBuilder::<IoUringDriver>::new()
            .build()
            .unwrap();
        rt.block_on(async move {
            assert!(tx1.send(24).is_ok());
            assert_eq!(rx2.await.expect("unable to receive rx2"), 24);
        });
    }

    #[test]
    fn timer() {
        use crate::driver::IoUringDriver;
        let mut rt = crate::RuntimeBuilder::<IoUringDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        let instant = std::time::Instant::now();
        rt.block_on(async {
            crate::time::sleep(std::time::Duration::from_millis(200)).await;
        });
        let eps = instant.elapsed().subsec_millis();
        assert!((eps as i32 - 200).abs() < 50);
    }
}
