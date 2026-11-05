#[cfg(feature = "unstable")]
use std::sync::LazyLock;
use std::{
    sync::{Arc, Mutex},
    task::Waker,
};

use adaptif_gatotkaca_utils::FastU64Map;
#[cfg(not(feature = "unstable"))]
use once_cell::sync::Lazy as LazyLock;

use crate::driver::{lf_queue::LfQueue, UnparkHandle};

/// The remote-waker queue of a runtime: other threads push wakers of its tasks.
pub(crate) type WakerQueue = Arc<LfQueue<Waker>>;

// Unpark handle of each runtime, by thread id (ids are small and unique: no hashing needed).
static UNPARK: LazyLock<Mutex<FastU64Map<UnparkHandle>>> =
    LazyLock::new(|| Mutex::new(FastU64Map::new()));

// Remote-waker queue of each runtime, by thread id.
static WAKER_SENDER: LazyLock<Mutex<FastU64Map<WakerQueue>>> =
    LazyLock::new(|| Mutex::new(FastU64Map::new()));

macro_rules! lock {
    ($x: ident) => {
        $x.lock()
            .expect("Unable to lock global map, which is unexpected")
    };
}

pub(crate) fn register_unpark_handle(id: usize, unpark: UnparkHandle) {
    lock!(UNPARK).insert(id as u64, unpark);
}

pub(crate) fn unregister_unpark_handle(id: usize) {
    lock!(UNPARK).remove(&(id as u64));
}

pub(crate) fn get_unpark_handle(id: usize) -> Option<UnparkHandle> {
    lock!(UNPARK).get(&(id as u64)).cloned()
}

pub(crate) fn register_waker_sender(id: usize, sender: WakerQueue) {
    lock!(WAKER_SENDER).insert(id as u64, sender);
}

pub(crate) fn unregister_waker_sender(id: usize) {
    lock!(WAKER_SENDER).remove(&(id as u64));
}

pub(crate) fn get_waker_sender(id: usize) -> Option<WakerQueue> {
    lock!(WAKER_SENDER).get(&(id as u64)).cloned()
}
