# W1-sched (F1, M2, M3 scheduler side, finding 4)

Branch `r2/W1-sched` from `r2-g0`. All new behaviour is opt-in. With a default builder the scheduling
order is 0.7.1's: FIFO queue, the same round length (`2 × len + 1` tasks, or `max(n, 1)` with
`task_budget(n)`; the loop was rewritten as a countdown with the same bounds), the same main-future
handling. `tests/lifo_slot.rs::default_is_fifo` pins it.

## API

| item | where |
|---|---|
| `monoio::task::yield_now()` | `task/coop.rs`. `Pending` once, wakes itself; the harness re-schedules a self-woken task through `Schedule::yield_now`, which always pushes to the back of the queue. |
| `RuntimeBuilder::coop_budget(Option<u32>)` | default `None`. `Some(0)` is treated as 1. |
| `monoio::task::consume_budget().await` | spends one unit; yields when the budget is spent. Always ready without coop. |
| `monoio::task::unconstrained(fut)` → `Unconstrained<F>` | polls `fut` with the budget set to "unconstrained" and restores the outer value afterwards (also on panic), so the inner work is not charged to the task. |
| `RuntimeBuilder::lifo_slot(bool)` | default `false`. |
| `RuntimeStats::{coop_yields, lifo_polls}` | filled (metrics only). `park_ns` and `remote_wakes` docs updated. |
| `fairness` flags | `--coop-budget N`, `--lifo`, `--skip-empty-enter`. |

No `lib.rs` change (everything is under the existing `pub mod task`).

## Coop budget design

- Thread-local `Cell<u32>` (`#[thread_local]` with `unstable`, `thread_local!` const otherwise);
  `u32::MAX` means unconstrained. `block_on` sets it to unconstrained at start (a runtime without coop
  never inherits a budget); with coop on the loop sets it to `n` before each `t.run()` and before
  each poll of the main future.
- The check is **before** the inner poll (tokio's `poll_proceed`): budget 0 → `wake_by_ref()` +
  `Pending`, whether or not the result is ready. A peek ("only when ready") would need a non-consuming
  lifecycle query in `driver/uring/{mod,lifecycle}.rs`, which this package doesn't own; the
  difference is one extra poll of a task whose budget is already spent. A completion is never
  consumed on that path (the lifecycle stays `Completed` in its slot) and is returned on the next poll;
  `tests/coop.rs::op_results_yield_after_budget` checks that no result is lost and each `data` comes
  back with its own op.
- A ready result spends one unit (the value loaded before the poll is reused: off = one TLS load and
  a branch per poll, no store).
- Checked in: `Op::poll` (typed ops, `RawOp`, all named futures in `io/futures.rs` and
  `driver/op/fixed.rs`, which go through `Op::poll`), `MultiOp::poll_next`/`next`, `ChainOp::poll`
  (the whole chain counts as one result), `consume_budget`.
- Not checked: the zero-copy two-step futures (`send_zc.rs`, `poll_zc_first/notif`, W1-bufzc's file;
  low value: a zc send never has a result queued before its first poll), and the **typed multishot
  streams**, see next.
- `coop_yields` is counted in the cold yield path only.
- Main future without `sync`: it is not a task and is re-polled at once when it wakes itself, so a
  coop yield (or `yield_now`) there is a no-op (the budget is reset before each re-poll, so it can't
  spin). Documented on `yield_now` and `coop_budget`.

### Multishot + coop: typed multishot streams are exempt

`MultishotOp::poll_next` (`driver/op/multishot.rs`) is **not** budgeted, so `AcceptStream::next`,
W1-net's `accept_batch`/`next_batch` (which drain all queued CQEs in one poll) and `RecvStream` /
`RecvBundleStream` never yield because of coop. Only the raw `uring::MultiOp` (the escape hatch) is
budgeted, per CQE. Reasons: G0 says the accept drain must never be throttled (M1); a per-CQE charge
in `MultishotOp` would cut a batch drain after `n` connections. Users who want a budgeted recv loop
can call `consume_budget()` per item. If G2 wants `RecvStream` budgeted, the right place is one
`poll_proceed`/`consume` pair around `poll_next_cqe` in `recv_multi.rs` charged once per poll (not
done here; W1-net edits that file).

Screening with `--accept multi --accept-ramp 2000 --coop-budget 128`: 2000/2000 ramp connections,
0 timeouts, same accepts/s as without coop (table below), `coop_yields = 0`.

Note: in the reproducer and the SUT stream tasks do one read + one send per poll, and an op future is
`Pending` on its first poll, so `coop_budget` alone changes nothing there (`coop_yields = 0`). It
matters for CPU loops (`consume_budget`), raw multishot drains and chains.

## LIFO slot design

- `TaskQueue` gets `slot: Option<Task>` and `use_slot: Cell<bool>`. The loop sets `use_slot` only
  around `t.run()` and only with `lifo_slot(true)`, so wakes from the driver tick (CQEs), the cold
  submit, the foreign-waker drain, the main future and outside a task use the queue.
- `Schedule::schedule` (a waker woke another task) → `push_woken`: into the slot if `use_slot`, the
  previous occupant goes to the back of the queue. `Schedule::yield_now` (the task woke itself
  while running: `yield_now`, coop yield) → always the queue. So `yield_now` is not affected
  (`tests/yield_now.rs::yield_ignores_lifo_slot`).
- After each task, the loop takes the slot task next; after 3 consecutive slot polls the slot task
  goes to the back of the queue and the next queue task runs (`LIFO_MAX_STREAK`). Slot polls count
  against `task_budget`. When the budget ends a round with the slot occupied, it stays in the slot
  and runs first in the next round (after the cold submit). `len()`/`is_empty()` include the slot,
  so the runtime never parks with it occupied.
- With `sync` the main future is a task: a task completing wakes its `JoinHandle` waiter, which is a
  hand-over and takes the slot (latest wake wins, as in tokio). The chain tests join only the last
  link for that reason.
- Cost when off: one predictable branch on a local `bool` per task, plus one extra load in `len()`.

## Item 4 (uncharged hand-over wakes): tried, dropped

Implemented as an experiment: wakes by the running task go to a FIFO hand-over queue run before the
ready queue and not charged to `task_budget` (cap `2 × len + 1` uncharged polls per round). Screening
(`--hop --budget 16`): it fixed M2 just like LIFO (100% achieved, p50 ≈ 15 ms); p99 swung 22–324 ms vs
LIFO's 46–389 ms across interleaved runs, i.e. noise on this machine. The counts show why they are
equivalent here: both did 4,000 hand-over polls/s (2 per request), so the LIFO slot never displaced
anything in this model. No measurable advantage → removed; **LIFO is the recommended M2 fix**
(tokio-compatible, O(1) state, budget semantics unchanged). If a workload shows displacement
(a task waking two hand-over targets per poll), the experiment is easy to revive.

## Item 5 (reap without enter): not implemented, already exists

`skip_empty_enter()` already skips `io_uring_enter` in the cold `submit()` when the SQ is empty (and
no taskrun/overflow flag), then runs `tick()`. Added `--skip-empty-enter` to `fairness`; with
`--hop --budget 16 --lifo` it did not change the picture (G0: enters are not the cause).

## Finding 4 (`sync`)

`Driver::submit` for `IoUringDriver` drains the foreign-waker queue after the tick (counted in
`remote_wakes`). `tests/budget_wakes.rs` (`sync`): `task_budget(4)`, 8 tasks yielding forever, a
`futures::oneshot` sent from another thread after 50 ms: received in ~0.1 s, with and without
`lifo_slot`. Without the drain the test fails (the wake waits until the fillers' 5 s cutoff) —
checked by disabling the drain.

## Screening (machine NOT quiet: load average ~10 on 12 cores, other agents building/testing)

`target/release/examples/fairness --bulk 400 --rr 200 --rate 2000 --duration 8 --warmup 2` + flags,
metrics build. `polls/s` (tasks polled per second) is a rough indicator of how much CPU the server
actually got; runs below ~30k polls/s were starved by other load.

| flags | rr p50 / p99 ms | achieved | bulk MB/s | polls/s | lifo_polls/s |
|---|---|---|---|---|---|
| default (1st run) | 21.8 / 85.0 | 100% | 3,223 | 31.7k | 0 |
| default (2nd run, starved) | 62.0 / 696 | 100% | 1,411 | 16.1k | 0 |
| `--hop --budget 16` | 1,131 / 1,622 | 86.0%, 200 unfinished | 3,723 | 40.5k | 0 |
| `--hop --budget 16` (2nd) | 2,392 / 3,572 | 61.0% | 3,048 | 33.2k | 0 |
| `--hop --budget 16 --lifo` | 14.7 / 58.9 | 100% | 3,223 | 35.7k | 4,000 |
| `--hop --budget 16 --lifo` (2nd, 3rd, 4th) | 19.7 / 51.7; 16.6 / 45.6; 35.3 / 389 (starved) | 100% | 2,594; 2,869; 1,993 | 30.3k; –; – | 3,998 |
| `--budget 16 --lifo` (starved) | 40.5 / 356 | 100% | 1,289 | 15.1k | 0 |
| `--budget 16` | 17.2 / 72.7 | 100% | 2,706 | 27.2k | 0 |
| `--hop --budget 61` | 1,851 / 3,179 | 66.2% | 2,620 | 28.8k | 0 |
| `--hop --budget 61 --lifo` | **15.0 / 21.8** | 100% | 3,716 | 39.9k | 3,999 |
| `--coop-budget 128` | 19.7 / 29.4 | 100% | 3,656 | 35.4k | 0 (coop_yields 0) |
| `--hop` | 22.3 / 93.2 | 100% | 3,137 | 34.9k | 0 |
| `--hop --lifo` | 22.8 / 76.8 | 100% | 3,032 | 34.0k | 3,996 |
| `--hop --budget 16 --lifo --skip-empty-enter` | 25.9 / 66.6 | 100% | 2,259 | 27.4k | 3,994 |
| item 4 `--hop --budget 16` uncharged (dropped) | 14.7 / 21.8; 41.5 / 324; 15.0 / 60.9 | 100% | 3,635; 1,255; 3,119 | 39.2k; –; – | 4,000 |
| `--accept multi --accept-ramp 2000` | 24.3 / 70.7; ramp first-resp p99 115.7 | 100%, ramp 2000 ok | 2,959 | 28.8k | 0 |
| `--accept multi --accept-ramp 2000 --coop-budget 128` | 22.8 / 70.7; ramp p99 65.0 | 100%, ramp 2000 ok | 3,266 | 31.4k | 0 |
| `--accept multi --accept-ramp 2000 --coop-budget 128 --budget 16 --lifo` | 15.5 / 19.7; ramp p99 48.6 | 100%, ramp 2000 ok | 3,542 | 33.8k | 0 |

Reading (screening only): the M2 collapse (`--hop` with a small budget: 61–86% achieved, p50 in
seconds) is gone with `--lifo` at budgets 16 and 61; `--hop --budget 61 --lifo` and
`--budget 16 --lifo` + multishot accept were the best rows (p99 ≈ 20 ms vs ≈ 70–90 ms default).
The accept ramp keeps up with coop on (accepts 250/s average, peak ~403/s, identical to without).
G2 should measure `budget16/61 + lifo` on the SUT (`--cells 64:4096`, `1024:4096` with
`acceptmulti`).

## Tests added

- `tests/yield_now.rs`: A yields, B runs before A resumes (default, `task_budget(1)`, with
  `lifo_slot` and budget 1).
- `tests/coop.rs`: queued NOP results yield after n with others running in between and nothing lost;
  without coop all run in one poll; `consume_budget` run length = n; off by default; `unconstrained`
  ignores the budget and does not charge the outer task; chain counts once. With `metrics`,
  `coop_yields > 0` only with coop.
- `tests/lifo_slot.rs`: default FIFO (budget none/1/4); hand-over chain t0→t1→t2 consecutive with
  budget none/1/2/16; 3-poll cap (t0..t3 consecutive, then a filler, then t4→t5 consecutive);
  displaced slot task goes to the queue; `lifo_polls` counted (metrics).
- `tests/budget_wakes.rs` (`sync`): foreign wake under a never-empty queue.

Checks: `cargo test -p adaptif-gatotkaca-monoio` with no features, `metrics`, `sync`, `metrics,sync`
all pass. `cargo clippy --workspace --all-targets` (+ `metrics`): only the pre-existing warnings
(udp.rs, slab.rs, async_write_rent_*). `cargo +nightly check --features unstable,sync,metrics` builds
(`#[thread_local]` path). `cargo fmt` clean.

## Requests for files this package does not own

- None required. Optional:
  - `driver/op/send_zc.rs` (W1-bufzc): `poll_proceed`/`consume` around `poll_zc_first` if zc sends
    should be budgeted (low value).
  - `driver/op/recv_multi.rs` (W1-net): if G2 wants `RecvStream` budgeted, charge once per poll around
    `poll_next_cqe` (not per CQE), see "Multishot + coop".
  - `monoio-macros`: no attribute added for `coop_budget`/`lifo_slot` (builder only).
