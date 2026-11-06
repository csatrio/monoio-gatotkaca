# W0-rtstats (F2): scheduler metrics

## What was added

Everything below exists only with `--features metrics`; without it `runtime.rs`, `scheduler.rs` and
`task/harness.rs` compile to what 0.7.1 had (no new fields in `Context`/`TaskQueue`, no clock reads, no stores).

- `monoio/src/runtime_stats.rs` (new, module compiled only with `metrics`):
  - `RuntimeStats` (`Clone, Copy, Debug, Default, PartialEq, Eq`, `#[non_exhaustive]`).
  - `StatsCell` (an `UnsafeCell<RuntimeStats>` with a `with(|s| ..)` updater), stored as `Context::stats`.
  - `monoio::runtime_stats()` / `monoio::reset_runtime_stats()` (panic outside a runtime, like `driver_stats()`),
    plus `Runtime::stats()` / `Runtime::reset_stats()`.
  - `note_remote_wakes(n)` (`sync` only, `pub(crate)`, currently unused): hook for the driver, see below.
- `lib.rs`: `mod runtime_stats;` and `pub use runtime_stats::{reset_runtime_stats, runtime_stats, RuntimeStats};`,
  both under `cfg(feature = "metrics")`, right after `pub use runtime::{spawn, Runtime};`.
- `AGENTS.md`: a "Scheduler stats" bullet after the Stats bullet.
- Test `monoio/tests/runtime_stats.rs`; unit test of the bucket function in `runtime_stats.rs`.

## Where each counter is updated

| field | where |
|---|---|
| `rounds`, `ready_queue_len` | `Runtime::block_on`, start of each inner-loop iteration (`len` sampled before any task runs) |
| `tasks_polled`, `polls_per_round` | `block_on`: a local `polled` counter per `t.run()`, added once at the end of the round |
| `budget_exhausted_rounds` | `block_on`, at the `max_round == 0` break, only if the queue is not empty |
| `submits` | `block_on`, before the cold-path `driver.submit()` |
| `parks`, `park_ns`, `busy_ns` | `block_on`: `Instant::now()` before and after `driver.park()`; busy = previous park end (or `block_on` start) to this park start |
| `ready_queue_high_water` | `TaskQueue::push` (field `TaskQueue::high_water: Cell<usize>`), merged into the snapshot |
| `tasks_spawned` | `spawn` and `spawn_without_static` (`sync`: the main future counts) |
| `tasks_completed` | `task/harness.rs` `Harness::poll`, `PollFuture::Complete` arm, via `CURRENT.try_with` |
| `remote_wakes` | not counted yet (0), see "Hooks needed" |
| `coop_yields`, `lifo_polls` | reserved, always 0 (filled in by later packages, W1-sched) |

`reset` zeroes everything except `ready_queue_len` (a sample, not a counter); high water goes to 0.

Notes:
- Without `sync` the main future of `block_on` is not a task, so it is not in `tasks_polled`/`tasks_spawned`.
- `busy_ns` misses the time after the last park of a `block_on` (until it returns).
- ROUND2-PLAN calls the submit counter `ticks_submit`; the field is `submits` as in the package prompt.

## Cost (with `metrics`)

- Per task poll: one register increment (`polled += 1`).
- Per round: one `len()` read, a few adds to the thread-local struct, a `leading_zeros` for the bucket, and
  one `is_empty()` when the budget ends the round.
- Per push: one `len()` and a compare/store on a `Cell` in `TaskQueue`.
- Per spawn: one add. Per completion: one scoped-TLS read + add.
- Per park: two `Instant::now()` (vDSO `clock_gettime`). Nothing per task.
- No atomics, no allocation. `RuntimeStats` is ~170 bytes in `Context`.

## Hooks needed in files this package does not own

- `remote_wakes` (`sync`): in `monoio/src/driver/uring/mod.rs`, inner park, the two
  `while let Some(w) = inner.waker_consumer.pop(&inner.waker_queue) { w.wake(); .. }` loops: count the drained
  wakers and call `metric!(crate::runtime_stats::note_remote_wakes(n));` (or `+= 1` per waker). The runtime side
  cannot tell these apart: once drained they wake like local wakers. Then drop `#[allow(dead_code)]` on
  `note_remote_wakes`.
- W1-sched: fill `coop_yields` / `lifo_polls` via `self.context.stats.with(|s| ..)` inside `metric!`.

## Tried and rejected

- Counting `tasks_completed` from the runtime side: `Task::run()` returns nothing, and changing it to report
  completion would touch the non-metrics path. The harness arm is the only place that knows, and it runs once per
  task.
