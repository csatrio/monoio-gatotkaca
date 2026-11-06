# W0-metrics: counters only with the `metrics` feature

Branch `r2/W0-metrics`, from `r2-base`.

## What changed

Without `metrics` (the default) no counter exists: no `DriverStats`, no stats field in `UringInner`,
no counter stores, no high-water `max()`, no per-tick `cq.overflow()` read. A call to a gated API is
a compile error, never silent zeros. With `metrics` every number is as in 0.7.1.

`metric!` (seed commit) wraps every update, including locals that only feed a counter (CQE count
`n` and bucket index in `tick`, the opcode index in `push_op`/`push_chain`/`push_op_with_timeout`,
the `kept_inline` match in `drop_op`). `#[allow(unused_macros)]` is removed from `metrics.rs`: the
macro is invoked in code compiled in every configuration.

## Gated items

`driver/uring/mod.rs`
- `DriverStats` (struct + `Default`), `UringInner::stats` field, `IoUringDriver::{stats, reset_stats}`,
  `UringInner::{stats, reset_stats}`: `#[cfg(feature = "metrics")]` (+ `doc(cfg)` on the public ones).
- Updates in `metric!`: `enter_submit`/`enter_wait`, `enter_skipped`, `sqes_submitted`, `cqes_reaped`,
  `cqes_per_tick` (bucket computation inside), `sq_full_flushes` (both sites), `busy_retries` (both
  cfg arms), `cancels_pushed`, `cancels_lost` (both), `ops_by_opcode` (push_op, push_chain,
  push_op_with_timeout; index computed only with metrics), `zc_sends`, `zc_notifs` (the flag test is
  inside), `multishot_cqes`, `multishot_queue_high_water`, `chains_submitted`, `cancel_fd_pushed`,
  `ignored_inline`/`ignored_boxed`.
- `link_timeouts`: the `LINK_TIMEOUT_USERDATA if res == -ETIME` match arm is `#[cfg(feature = "metrics")]`
  (without it the CQE falls into the `_ => ()` arm, as it did apart from the count).
- `CQE_F_NOTIF` const: metrics only (only the counter used it).
- `cq_overflow`: no longer read in `tick()`. `UringInner::stats()` (now `&mut self`) reads
  `completion().overflow()` from the kernel each call. `reset_stats()` just resets to default: the
  value is re-read on the next `stats()`, so it is still never cleared (doc kept/adjusted).

`driver/uring/lifecycle.rs` (instrumentation lines only)
- `Dropped::kept_inline` and `Completed::queued` existed only for the counters. They are
  `#[cfg(any(test, feature = "metrics"))]`, with their stores: the in-crate lifecycle tests assert on
  them, so they keep that coverage in a default `cargo test`; a non-test default build has neither.

`driver/mod.rs`, `lib.rs`
- `driver_stats()`, `reset_driver_stats()`, and the re-exports `monoio::{DriverStats, driver_stats,
  reset_driver_stats}`: metrics only.

Pool stats (structs stay, now `#[non_exhaustive]`; counter fields + their `Cell`s/atomics + updates gated)
- `BufRingStats::{enobufs, high_water, taken, recycled}` (`buf_ring.rs`). `in_use` stays.
- `FixedBufStats::{high_water, exhausted}` (`fixed_buf.rs`). `in_use` (= `live`, used to free the
  pool) and `capacity` stay. `get()` was rewritten from `let..else` to `pop()` + `metric!` + `?` (clippy
  `question_mark` fires otherwise); same behaviour.
- `FixedFileStats::{high_water, exhausted}` (`fixed_files.rs`; `exhausted` is also a pure counter).
  `capacity`, `in_use`, `kernel_capacity`, `kernel_in_use` stay.
- `EventChannelStats::{eventfd_wakeups, ring_wakeups, full}` and the whole private `Counters` struct
  (three `AtomicU64` per channel, one `fetch_add` per wakeup/full send) in `event_channel.rs`.
  `capacity` stays. **`EventReceiver::wakeups()`** is derived from the counters, so it is gated too.
- No other pure counters found (`msg_ring/mod.rs`, `recv_multi.rs`, `doorbell.rs`, `lf_queue`,
  `raw.rs` have none; `recv_multi.rs` only has comments mentioning ENOBUFS counting).

## Decisions

- **`BufRing` `in_use`**: kept and maintained in both modes. It is not read by the pool's own logic
  (recycling uses per-buffer `views` and `in_kernel`), so strictly it is reporting. But the plan's
  table lists it as state present without `metrics`, it is part of the always-available
  `BufRing::stats()`, tests (`buf_ring`, `multishot_raw`, `multishot_recv`) use `in_use` to check that
  buffers came back, and the cost is one `Cell` add/sub on a buffer's first view / last drop (not per
  CQE). If W1 wants it gone, it could be derived on demand by counting `bufs` with `views > 0`
  (O(entries) in `stats()`), no stored counter needed.
- **`sqes_consumed` (new, ungated, 8 bytes)**: `msg_ring_nowait` used `stats().sqes_submitted` to know
  when the kernel consumed its SQE (keepalive of the target `RingHandle`). That is correctness, not
  reporting, so `UringInner` gets a never-reset `sqes_consumed: u64` (one add per `io_uring_enter`)
  and `UringInner::sqes_consumed()`. `msg_ring/mod.rs` uses it. With metrics `sqes_submitted` is
  still counted separately (reset semantics unchanged). Side effect: the keepalive no longer restarts
  on `reset_stats`, so handles are released no later than before (the old comment already said it may
  lag, never run ahead). The io-uring crate does not expose the kernel SQ head, otherwise that would
  replace the counter.
- **Size test**: `monoio::__private::{URING_INNER_SIZE, URING_INNER_HAS_STATS, DRIVER_STATS_SIZE}`
  (`#[doc(hidden)]`, from `driver::uring::layout`). A test can't compare two builds, so:
  - `tests/metrics_off.rs` (`#![cfg(not(feature = "metrics"))]`): `HAS_STATS` is false.
  - `tests/driver_stats.rs` (metrics only): `HAS_STATS` is true, `DRIVER_STATS_SIZE ==
    size_of::<DriverStats>()`, `URING_INNER_SIZE > DRIVER_STATS_SIZE`.
  - `DRIVER_STATS_SIZE` is `90 * 8` (a const), checked against the real struct by a `const _` assert
    in metrics builds, so it can't silently drift.
  - Both print the size with `--nocapture`. Measured (x86_64): default 1784 B vs metrics 2504 B
    (−720 B = exactly `size_of::<DriverStats>()`); with `sync` 1848 vs 2568. 0.7.1 (default) is
    2496 B by the same arithmetic (not measured): the new `sqes_consumed` is the +8.
  - The plan said a `pub(crate)` const; integration tests can't see those, hence `doc(hidden) pub`.

## Tests

- `driver_stats.rs`: `#![cfg(feature = "metrics")]` (it only tests stats), plus the size test.
- Gated lines only (rest of each test still runs in both modes): in-crate `buf_ring.rs` tests;
  `tests/{buf_ring, builder_flags, cancel_fd, chain_raw, deadline, event_channel,
  event_channel_lockfree, fixed_buf, fixed_files, ignored_inline, multishot_raw, multishot_recv,
  zc_send}.rs`. Tuple asserts mixing state and counters were split (state asserted always).
  The "wait until the counter moved" loops in `chain_raw`/`deadline` are gated with their asserts.
  `zc_send.rs` gates the `opcode` import. `reexports.rs` pins no stats path: unchanged.
- New `tests/metrics_off.rs`.

## Results

- `cargo test -p adaptif-gatotkaca-monoio`: 429 passed, 0 failed, 1 ignored.
- `cargo test -p adaptif-gatotkaca-monoio --features metrics`: 434 passed, 0 failed, 1 ignored.
- `cargo test -p adaptif-gatotkaca-monoio --features metrics,sync`: 440 passed, 0 failed, 1 ignored.
- `cargo check -p adaptif-gatotkaca-monoio --features sync --tests`: ok.
- `cargo clippy --workspace --all-targets` (and with `--features adaptif-gatotkaca-monoio/metrics`,
  and with `sync`): no new warnings (remaining ones are pre-existing in `net/udp.rs`,
  `utils/slab.rs`, `tests/async_write_rent_*.rs`).
- `cargo check -p monoio-examples --examples` (with and without `--features metrics`): ok; no example
  calls a gated API.
- `cargo doc`: the two intra-doc links to `crate::DriverStats::*` in `raw.rs` became plain code
  spans ("with the `metrics` feature"), so the default doc build has no new broken links.
- `cargo fmt`: applied.

## Remaining `grep -rn 'stats\.' monoio/src | grep -v metric!`

- `driver/uring/mod.rs`: every hit is either a continuation line inside a multi-line `metric!(...)`
  (`zc_notifs`, `multishot_*`, `cqes_reaped`/`cqes_per_tick`, `enter_wait`/`enter_submit`,
  `ops_by_opcode`/`zc_sends`, `chains_submitted`, `ignored_*`), inside the `#[cfg(metrics)]` match arm
  (`link_timeouts`), or inside the gated `UringInner::stats()` (`cq_overflow`).
- `driver/fixed_files.rs` (`self.stats.get()/set()`, Debug field): the `Cell<FixedFileStats>` that
  holds the table's state (`capacity`, `in_use`, `kernel_*`); its counter fields exist only with
  metrics, and their updates are in `metric!`.
- `driver/op/recv_multi.rs:154`: a comment.

## For other owners / the lead

- Files outside the W0-metrics list that I edited (instrumentation lines only):
  `driver/uring/lifecycle.rs` (`kept_inline`, `queued`), `driver/msg_ring/mod.rs` (`sqes_consumed`
  instead of `stats().sqes_submitted`), `driver/raw.rs` (two doc links), tests `chain_raw.rs`,
  `builder_flags.rs`, `multishot_recv.rs`.
- Breaking: `driver_stats()`, `DriverStats`, `IoUringDriver::stats`, `EventReceiver::wakeups()` and
  the pool counter fields need `features = ["metrics"]`; the pool stats structs are now
  `#[non_exhaustive]` (no struct literals outside the crate). RELEASES.md for 0.8.0 should say so.
- W0-rtstats / W0-repro: `driver_stats()` deltas in `examples/fairness.rs` must be under
  `#[cfg(feature = "metrics")]` (the examples crate forwards the feature).
