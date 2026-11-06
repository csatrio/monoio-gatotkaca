//! Without the `metrics` feature the driver carries no counters: `DriverStats` is not compiled
//! and the driver state has no stats field. The metrics side is `driver_stats.rs`
//! (`driver_state_holds_the_counters`); run both with `--nocapture` to compare the sizes.
#![cfg(not(feature = "metrics"))]

use monoio::__private::{DRIVER_STATS_SIZE, URING_INNER_HAS_STATS, URING_INNER_SIZE};

#[test]
fn driver_state_holds_no_counters() {
    const { assert!(!URING_INNER_HAS_STATS) };
    // A metrics build is larger by at least this much.
    eprintln!("UringInner: {URING_INNER_SIZE} bytes (DriverStats: {DRIVER_STATS_SIZE} bytes)");
}
