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
        remote_targets: Default::default(),
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

    /// Remote-waker queues and unpark handles of other runtimes this thread woke tasks of.
    #[cfg(feature = "sync")]
    pub(crate) remote_targets: std::cell::RefCell<RemoteTargets>,

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
            remote_targets: Default::default(),
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

    /// Hand `waker` (of a task owned by runtime `id`) to its runtime and wake it if it sleeps.
    /// The waker is dropped if that runtime is gone.
    #[cfg(feature = "sync")]
    pub(crate) fn remote_wake(&self, id: usize, mut waker: std::task::Waker) {
        use crate::driver::unpark::Unpark;
        let mut targets = self.remote_targets.borrow_mut();
        let Some(target) = targets.get(id) else {
            return;
        };
        // The queue only fills at 2^31 wakers; back off rather than lose a wakeup.
        while let Err(w) = target.queue.push(waker) {
            waker = w;
            std::thread::yield_now();
        }
        // Pairs with the fence in the driver's park: either we see it is going to sleep, or it
        // sees our waker when it drains again.
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        let _ = target.unpark.unpark();
    }
}

/// Where to send wakers of another runtime's tasks.
#[cfg(feature = "sync")]
#[derive(Clone)]
pub(crate) struct RemoteTarget {
    queue: crate::driver::thread::WakerQueue,
    unpark: crate::driver::UnparkHandle,
}

/// Per-thread cache of [`RemoteTarget`]s, with the last one used checked first.
#[cfg(feature = "sync")]
#[derive(Default)]
pub(crate) struct RemoteTargets {
    last: Option<(usize, RemoteTarget)>,
    map: adaptif_gatotkaca_utils::FastU64Map<RemoteTarget>,
}

#[cfg(feature = "sync")]
impl RemoteTargets {
    fn get(&mut self, id: usize) -> Option<&RemoteTarget> {
        if self.last.as_ref().is_some_and(|(last, _)| *last == id) {
            return self.last.as_ref().map(|(_, t)| t);
        }
        let target = match self.map.get(&(id as u64)) {
            Some(t) => t.clone(),
            None => {
                use crate::driver::thread::{get_unpark_handle, get_waker_sender};
                let target = RemoteTarget {
                    queue: get_waker_sender(id)?,
                    unpark: get_unpark_handle(id)?,
                };
                self.map.insert(id as u64, target.clone());
                target
            }
        };
        self.last = Some((id, target));
        self.last.as_ref().map(|(_, t)| t)
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
