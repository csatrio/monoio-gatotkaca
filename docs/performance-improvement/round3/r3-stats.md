# r3-stats (X1, counters)

Branch `r3/r3-stats`, base `r3-base`. Two counters, both compiled only with `--features metrics`.

## What changed

| file | change |
|---|---|
| `monoio/src/runtime_stats.rs` | field `RuntimeStats::ops_completed: u64`; `pub(crate) fn note_op_completed()` |
| `monoio/src/driver/uring/lifecycle.rs` | two `metric!(crate::runtime_stats::note_op_completed());` lines in `Ref::complete` / `complete_slow` |
| `monoio/src/driver/uring/mod.rs` | field `DriverStats::enter_left_taskrun: u64` (+ `Default`), `DRIVER_STATS_U64S` 19 → 20 scalars, one `metric!` block in `inner_park` |
| `monoio/tests/runtime_stats.rs` | 4 new tests |
| `monoio/tests/driver_stats.rs` | 2 new tests, one assertion added to `tcp_echo_moves_counters` |

`monoio/tests/metrics_off.rs` is unchanged and passes.

Without `metrics`, the only thing that differs from `r3-base` in the compiled crate is the value of the
constant `DRIVER_STATS_U64S` (it feeds the `#[doc(hidden)]` `layout::DRIVER_STATS_SIZE`, which
`metrics_off.rs` prints). Every other added line is inside `metric!` or belongs to a type that exists only
with `metrics`. No behaviour change in either build.

## Definitions as implemented

### `RuntimeStats::ops_completed`

Incremented by `runtime_stats::note_op_completed()` (scoped-TLS `CURRENT.try_with`, same shape as
`note_remote_wakes`; does nothing when no runtime is current), called right before `waker.wake()` at two
places in `lifecycle.rs`:

1. `Ref::complete`, fast path, arm `Lifecycle::Waiting(waker)`: the only CQE of a single-shot op whose
   future was polled and is `Pending`.
2. `Ref::complete_slow`, the single `if let Some(waker) = wake { .. }` at the end. `wake` is set by:
   - `WaitingFirst(waker)`: the result CQE of a two-step zero-copy send (`send_zc_early`, ...);
   - `AwaitNotif(Some(waker))` with a CQE without `F_MORE`: the notification of a two-step zero-copy send,
     when its `pending` future is waiting;
   - `Waiting(waker)` without `F_MORE` (not reachable through `complete`, the fast path takes it; counted
     for completeness);
   - `WaitingMore(waker, ..)` without `F_MORE`: the notification of a one-step zero-copy send (`write_zc`).

So it is "CQEs of non-multishot ops that woke a waker". Consequences:

- Not counted: a CQE that arrives while the slot is `Submitted` (future not polled yet, e.g. an `_op`
  future created and polled later), CQEs of dropped ops (`Ignored*`), a first zero-copy CQE with `F_MORE`
  on a one-step send (it wakes nobody), and every CQE of the multishot slot type (`Lifecycle::Multi`:
  `uring::submit_multishot`, `accept_multi`, `recv_multi*`, and `open_direct` into a kernel-allocated
  slot, which uses `MultishotOp` internally).
- `uring::submit` is a single-shot op and is counted. Each SQE of `uring::submit_chain` has its own
  single-shot slot and `ChainOp::poll` leaves a waker in every pending slot, so a chain of N counts up to N.
- Zero-copy: `write_zc` counts 1; `send_zc_early` counts 1 for the result and 1 more if `pending` is
  waiting when the notification is reaped (0 more if the notification came in the same tick as the result
  or `pending` is dropped / not polled).
- `tasks_polled / ops_completed` can be below 1: a task that waits for several ops (pipelined
  `send_op` + `recv_op`, `join!`, a chain) is woken by each CQE but polled once if they are reaped in the
  same tick. It only relates to ops awaited in spawned tasks (without `sync` the main future of `block_on`
  is not in `tasks_polled`, but its ops are in `ops_completed`).
- Reset by `reset_runtime_stats()` / `Runtime::reset_stats()` like the other counters (the reset builds a
  `RuntimeStats::default()`).

Cost with `metrics`: one scoped-TLS read and one add per counted CQE.

### `DriverStats::enter_left_taskrun`

Incremented in `IoUringDriver::inner_park`, directly after `inner.tick()?` and before `drop_deferred` /
`deliver`:

```rust
metric!(if need_wait && inner.uring.submission().taskrun() {
    inner.stats.enter_left_taskrun += 1;
});
```

- `need_wait` is the local of `inner_park`: true when the park took the waiting branch (`enter(1, ..)`,
  with or without a timeout; a wait that ended with `ETIME` counts as a wait). The submit-only branch
  (`enter(0, None)`, taken with `sync` when foreign wakers were drained) is not counted. `Driver::submit`
  (cold path) and the flushes of a full SQ are not counted either.
- `taskrun()` is the `IORING_SQ_TASKRUN` bit of the SQ ring flags, read once after the tick that reaped
  the CQEs of that enter. The kernel sets the bit only on rings created with `IORING_SETUP_TASKRUN_FLAG`
  (`taskrun_flag()`, implied by `defer_taskrun()`), so the counter stays 0 on every other ring.
- If `enter` or `tick` returns an error, `inner_park` returns before the count (unchanged control flow).
- `enter_left_taskrun <= enter_wait`. `enter_left_taskrun / enter_wait` is the share of waits that
  returned although the kernel had (or got, between the return and the end of the tick) more task work.
- Cleared by `reset_stats()` (it is part of `DriverStats::default()`).

Cost with `metrics`: one SQ view per waiting park (loads head, tail and flags; its drop stores the
unchanged tail, as the views in `enter` and `nothing_to_enter` do).

## Tests

`monoio/tests/runtime_stats.rs`:
- `ops_completed_counts_waiting_ops`: two spawned tasks ping-pong one byte 32 times over
  `UnixStream::pair()`; `ops_completed >= 4 * 32` and `tasks_polled >= ops_completed`; reset gives 0.
- `ops_completed_needs_a_waiting_waker`: 64 awaited NOPs give exactly 64; a NOP pushed with
  `uring::submit` and awaited only after another NOP completed is not counted (exactly 1 for the two).
- `ops_completed_skips_multishot`: 8 connections taken from `accept_multi` give 0, a NOP after them 1
  (skips on `EINVAL`, Linux < 5.19).
- `ops_completed_zero_copy_counts_wakes`: `write_zc` gives exactly 1; `send_zc_early` 1 at the result and
  1 or 2 after `pending` (skips on `EINVAL`/`ENOSYS`).

`monoio/tests/driver_stats.rs`:
- `enter_left_taskrun_is_zero_without_taskrun_flag`: the burst workload below on a default ring and on a
  `coop_taskrun()` ring (no `taskrun_flag`): `enter_wait > 0`, `enter_left_taskrun == 0`.
- `enter_left_taskrun_counts_leftover_task_work`: deterministic `> 0` case on a `defer_taskrun()` ring.
  64 tasks each wait in a `read` on their own socket pair. The test first makes sure the kernel has all
  64 receives (NOP round trips until every pushed op was consumed), then makes all of them ready with
  plain `write(2)` calls from the runtime thread, then parks. The park's enter has nothing to submit and
  64 task-work items pending; with `min_complete = 1` the kernel runs 20 of them
  (`IO_LOCAL_TW_DEFAULT_MAX`), sees that work is left and sets `IORING_SQ_TASKRUN` again
  (`io_run_local_work_continue`). Asserted `>= 1` on Linux 6.13 or later; on older kernels the value is
  printed, not asserted (see below). Skips if `defer_taskrun` cannot be built (Linux < 6.1).
  Observed here (Linux 7.0.0-42, 65 runs, identical every time):
  `enter_wait 3, enter_left_taskrun 2, cqes_per_tick [0, 0, 0, 0, 0, 1, 2]` (one tick with 16–31 CQEs,
  two with 32 or more).
- `tcp_echo_moves_counters`: `enter_left_taskrun == 0` on the default ring.

Kernel version gate: I read `io_uring/io_uring.c` at tags v6.12 and v6.13 (GitHub raw, through a
summarising fetch, so not line-by-line). v6.12's `__io_run_local_work(ctx, ts, min_events)` has no
maximum and runs the whole list; v6.13 has `IO_LOCAL_TW_DEFAULT_MAX` (20) and `retry_llist`. I did not
run the test on any kernel other than 7.0. This is also a data point for KV1 (r3-probe): on 7.0 one
waiting enter with `min_complete = 1` reaped 20 of 64 ready receives and left the flag set.

## Test results (this worktree, final tree)

Run as `taskset -cp 0,1,5,6,7,11 $$; CARGO_BUILD_JOBS=4 cargo ...` (the shell is pinned first; the
harness of this agent refuses `taskset ... cargo test` as one command).

| command | result |
|---|---|
| `cargo test -p adaptif-gatotkaca-monoio` | exit 0; 74 test binaries, 487 passed, 0 failed, 2 ignored |
| `cargo test -p adaptif-gatotkaca-monoio --features metrics` | exit 0; 74 test binaries, 506 passed, 0 failed, 2 ignored |
| `cargo clippy --workspace` | the 3 old lib warnings, nothing new |
| `cargo clippy --workspace --features adaptif-gatotkaca-monoio/metrics` | the 3 old lib warnings, nothing new |
| `cargo fmt --check` | clean |

Also run, not required: `--features metrics,sync --test driver_stats --test runtime_stats` (16 passed), and
the two files 25 times in a row with `metrics` (all passed). No test needed a rerun without the CPU mask.

## Tried and rejected

- **First version of the `defer_taskrun` test** did one `nop().await` before the writes. It failed
  (`enter_left_taskrun 0`, one tick with all 64 CQEs): the main future is polled before the spawned tasks
  run, so the cold-path `submit()` took the NOP alone and the receives were still in user space when the
  writes happened. They were then submitted by the park's enter with data already there and completed
  inline. Fixed by waiting until the kernel has the receives.
- **Count per tick instead of per CQE** (a `woke` flag in `lifecycle::Completed`, summed in `tick()` and
  passed to one `note_*` call per tick). Considered, not built: it needs a metrics-only field in
  `Completed` plus changes in `handle_cqe`/`tick`, which other round-3 packages edit; the direct call is
  two lines. Not measured (no benchmarking in this package). If the per-CQE TLS read shows up in
  `metrics` runs of `reqresp`, this is the cheaper form.
- **Reading the flag inside `tick()`** (ROUND3.md's wording). `tick()` is also called from
  `Driver::submit` and from `reserve_sqes`, and does not know whether the enter before it waited. Counting
  in `inner_park` after the tick needs no extra state.
- **A `coop_taskrun().taskrun_flag()` case.** Not added and not tried: there the task work runs on the way
  out of any syscall of the thread (also the test's own `write(2)`), so I see no deterministic way to
  leave the flag set, and no value is guaranteed either way.
- **Split of `op_ticks_by_opcode` bucket 0**: skipped, as the plan says.

## Requested changes (files this package does not own)

1. `AGENTS.md`, "Stats" bullet, after "enter calls (submit/wait/skipped)": add
   "waiting enters that returned with `IORING_SQ_TASKRUN` still set (`enter_left_taskrun`; only counts on
   rings with `taskrun_flag`, e.g. `defer_taskrun`)".
2. `AGENTS.md`, "Scheduler stats" bullet, after "tasks polled and a polls-per-round histogram": add
   "`ops_completed` (CQEs of single-shot ops that woke a waiting waker; `tasks_polled / ops_completed` is
   the task polls per op completion)".
3. r3-drain (`inner_park`): the `metric!` block sits right after the first `inner.tick()?`. If the drain
   loop is inserted after it, the counter keeps meaning "waits that left task work" (how often the drain
   has something to do). If the loop is inserted before it, the counter means "task work still left after
   the drain". The first reading matches ROUND3.md D1; please keep the block directly after the first
   tick, or say in `r3-drain.md` which one it became.
4. r3-repro (`reqresp` output with `metrics`): print `tasks_polled / ops_completed` and
   `enter_left_taskrun / enter_wait` from the two deltas.

No `pub use` line was needed: both fields are on types that are already exported.

## Open questions

- None for this package. The kernel statement behind the `> 0` test (20 items per run, flag set again)
  is KV1 and belongs to r3-probe; the test here shows it on 7.0 only.
