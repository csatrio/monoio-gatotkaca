# r3-bufring: working set, `next_rearm`, `wait_free` (B1, B2, E10)

Branch `r3/r3-bufring`, on `r3-g0`.

Files changed:

| file | change |
|---|---|
| `monoio/src/driver/buf_ring.rs` | reserve stack, `published` / `grow_published` / `shrink_published`, `BufRingOptions::working_set`, the automatic policy, waiter list, `wait_free` / `WaitFree`, claims, stats, unit tests |
| `monoio/src/driver/op/recv_multi.rs` | `OnEnobufs::Wait`, `rearm_on`, `ring`, claims; `RecvBundleStream::{next_rearm, poll_next_rearm, rearm_on, ring}`; take at reap time on working-set rings |
| `monoio/src/net/tcp/multishot.rs` | `RecvStream::{next_rearm, poll_next_rearm, rearm_on, ring}`, docs |
| `monoio/src/driver/raw.rs` | registry line: `WaitFree` added to the `pub use` of `buf_ring` items |
| `monoio/tests/buf_ring_working_set.rs` | new, 10 tests |
| `monoio/tests/multishot_recv.rs` | 7 tests added |
| `monoio/tests/buf_ring.rs` | one assertion of `stats_count_use_and_enobufs` (see "Behaviour changes") |
| `monoio/examples/reqresp/paths/mshot.rs` | `--wset I,S`, `next_rearm`, ring stats on `metrics` builds |

`monoio/src/net/unix/multishot.rs` is unchanged: Unix streams return the same `RecvStream` /
`RecvBundleStream` types. The `RecvMulti` struct has the same three fields as before (r3-eager's
guard can go in as planned); the only new thing done to it is that a re-arm may replace its `ring`
field before the data is submitted again.

All performance numbers below are **screening, machine not quiet** (six other agents were building
and measuring). Frames/s and cycles moved by ±8% between identical runs; user instructions per
frame by about ±1%.

## What is new (API)

```rust
impl BufRingOptions {
    pub fn working_set(self, initial: u16, step: u16) -> Self;
}
impl BufRing {
    pub fn published(&self) -> usize;                 // buffers not in the reserve
    pub fn grow_published(&self, n: u16) -> usize;    // new target, <= entries
    pub fn shrink_published(&self, n: u16) -> usize;  // new target, >= 1; effective as buffers are recycled
    pub fn wait_free(&self) -> WaitFree;              // Future<Output = ()>, Unpin, not tied to &self
}
pub struct BufRingStats { .., pub published: usize, /* metrics: */ pub grown: u64, pub retired: u64 }

impl RecvStream /* and RecvBundleStream; TCP and Unix */ {
    pub async fn next_rearm(&mut self) -> Option<io::Result<SelectedBuf>>;   // Bundle for bundles
    pub fn poll_next_rearm(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<SelectedBuf>>>;
    pub fn rearm_on(&mut self, ring: &BufRing) -> io::Result<()>;
    pub fn ring(&self) -> &BufRing;
}
```

## Design

### Reserve stack (B1 manual part, E10)

- A buffer is **published** (in the ring, with the kernel, or held by the application) or in the
  **reserve**. `Shared::count` is the number of published buffers, `Shared::target` what it should
  be. Invariant: `count >= target`; `count > target` only while a shrink is under way.
- The reserve is an intrusive LIFO stack: `Shared::reserve_top` (a bid, or `NO_BUF = u16::MAX`) and
  `BufState::below`. No allocation. `BufState` stays 28 bytes (the new `u16` fills padding).
- A reserve buffer has `in_kernel == false` and no views, so a stale completion that names it is
  refused by `take` like any buffer that is not with the kernel.
- `recycle` tests one flag (`Shared::slow`: a shrink is under way, or someone waits for a buffer).
  If it is clear, the path is the old one (`give`: `fill_slot` + publish). If `count > target` the
  buffer is pushed on the stack instead (`recycle_slow`, `#[cold]`). A shrink cannot take buffers
  out of the ring (the kernel owns them), so it takes effect as buffers come back.
- `publish_reserve` pops from the top (the buffer retired last), fills slots, publishes once
  (through `PENDING` in batch mode), and wakes waiters.
- `fill_slot` is unchanged and still fills slots strictly in sequence, so its invariants hold as
  before: each buffer is in at most one slot; `pos` / `len_snap` (a slot is overwritten exactly one
  lap after it was filled; slots never filled are zero and cannot match `lap_ago` during the first
  lap); `next` links follow fill order, which is the kernel's consumption order; views keep
  `Shared` alive.
- With `working_set(initial, step)` only bids `0..initial` are filled at creation. The others start
  on the stack, lowest bid on top.
- `Shared::published` (the tail the kernel has seen) was renamed `kernel_tail`; `published` now
  means the count everywhere.

### Automatic policy (B1 automatic part; only with `working_set`)

`Shared::auto: Option<Auto>`; a ring without it pays one `Option` test per `take` and per
`ENOBUFS` (and in `recycle` the `slow` test and a test for incremental mode), nothing else.

- **Demand** = `busy`: buffers taken and not recycled (`in_use`; on an incremental ring `out`, the
  buffers the kernel is done with whose views are still alive).
- **Typed streams take at reap time on a working-set ring** (`BufRing::takes_at_reap`: incremental
  or working set; the `PreTake` mechanism that incremental rings already used). Without that the
  demand is invisible: at 8 connections the kernel uses 8 to 32 buffers per tick, but the tasks take
  and drop them one after the other, so `in_use` measured at poll time never exceeds 1 (the
  `high_water=1` of the rows without `--wset` below). The ring would shrink to `initial` and live on
  `ENOBUFS`.
- **Grow** (never above `entries`; `step == 0` turns growth off):
  - a `take` that leaves `busy >= count`: `step` buffers;
  - `ENOBUFS`: as many as it takes to have `step` free again, at least one. Deviation from the plan
    ("grow by `step` per `ENOBUFS`"): K receives usually fail in the same tick, and `step` each
    would publish `K × step` buffers for a demand of K. At least one, because the count of free
    buffers can be too high (raw users that take late) while the `ENOBUFS` is a fact;
  - `wait_free` / `next_rearm` finding no free buffer: `step` (a stream never waits while the
    reserve has buffers).
- **Shrink**: a window is `WINDOW_TAKES_PER_BUF (4) × target` takes. At its end, if the highest
  `busy` seen in it, times `SHRINK_DIVISOR (2)`, is below `count`, the target becomes
  `max(initial, 2 × high)`. Per take: one max, one compare for growth, one decrement and compare
  for the window.
- **Backoff**: an `ENOBUFS` after a shrink (flag `Auto::shrunk`) doubles the window, up to
  `2^MAX_BACKOFF (10)` times. An `ENOBUFS` also records `count` as the window's high (the ring was
  empty, so every published buffer was needed). This bounds the damage when the demand is
  under-reported (raw users taking late) or bursty.
- **Incremental rings**: supported, not rejected. The same rules run when the kernel is done with a
  whole buffer (the completion without `BUF_MORE`), and `busy` is `out`. Covered by a unit test and
  by the starved-ring test; not measured for speed.

### Waiting for a buffer (B2)

- `Shared::waiters: RefCell<VecDeque<(u64, Waker)>>`, oldest first. Keys ascend, so a waiter is
  found by binary search (re-poll, cancel). The deque keeps its capacity: no allocation per wait in
  steady state.
- `spare = count − busy − claims`. It can be too high (buffers the kernel used for completions
  that were not taken yet are not in `busy`), never too low.
- `kick(n)`: for each of up to `n` buffers that became free, wake the oldest waiter while
  `spare > woken`. Called from `recycle` (slow path), `publish_reserve`, `unclaim`, and when a woken
  waiter gives up. `Shared::woken` counts waiters that were woken and did not come back yet, so two
  events about the same buffer wake one waiter.
- **Claims** (`Shared::claims`): a stream that re-arms because a buffer is free claims it until the
  first completion of that receive was handled (after the buffer was taken, so the ring counts it as
  in use before the claim ends). Without claims every stream that just saw `ENOBUFS` would re-arm
  for the same free buffer.
- `RecvMultiOp` in `OnEnobufs::Wait` mode (`next_rearm`): after `ENOBUFS`, and when the kernel
  ended the receive after a chunk (a bundle that used the last buffer), the re-arm is gated on
  `poll_free`. Ready: claim, re-arm. Not ready: the waker is listed, `Pending`.
- **No spin when the count is wrong** (`empty_at`): the stream remembers the ring's fill count
  (`tail`) when it armed. If that receive ends with `ENOBUFS` and the fill count is unchanged, the
  kernel saw exactly these slots and found none: the ring is empty whatever `spare` says, and the
  stream waits for the next fill. So every retry is preceded by at least one buffer coming back.
- `WaitFree` (public): the same `poll_free` without a claim. One waiter is woken per free buffer; a
  woken future that is dropped passes the wake on. A waiter that resolves and then starts no receive
  leaves the others waiting for the next buffer (documented).
- `Shared::release` now lowers the ring-wide view count after `recycle`, so a waker that drops the
  last handle cannot free `Shared` under `recycle`.

### `rearm_on` (B2): what happens to the receive on the old ring

- **It has ended** (the stream yielded `ENOBUFS` or another error, or a re-arm is due): the next
  poll submits on the new ring. With the ring it already uses this is a re-arm in place.
- **It is in flight**: it is cancelled (AsyncCancel). The stream keeps yielding what the old ring
  delivers, in order, until that receive's final completion; only then is the receive on the new
  ring submitted. Two receives are never armed at once, so nothing is lost or reordered.
  - `ECANCELED`, a final chunk, or `ENOBUFS` of the old ring all lead to the re-arm on the new ring.
  - Any other error is yielded and ends the stream, as without the move.
  - The cancel is best effort like `cancel()`; calling `rearm_on` with the same ring again repeats it.
- After EOF or `cancel()`: no effect. A ring of another runtime: `InvalidInput`.
- The submit itself happens in the next poll, so its error shows up in `next`.

## Behaviour changes for existing users

- `BufRingStats` has the new field `published`, so `ring.stats() == BufRingStats::default()` is no
  longer true for a new ring. `tests/buf_ring.rs::stats_count_use_and_enobufs` asserted that; the
  assertion now expects `published: 2`. Nothing else in the existing tests was changed.
- Nothing else changes for a ring without `working_set`: same take time, same recycle path.

## Tests

- Unit (`driver::buf_ring::tests`, with a model of the kernel's head): the reserve is a stack of the
  buffers retired last; `published` through take / recycle / grow / shrink; grow at a take that
  leaves nothing free and on `ENOBUFS`; shrink after a low window and re-growth from the top of the
  stack; incremental ring counts whole buffers.
- `tests/buf_ring_working_set.rs`:
  - `published_follows_shrink_and_grow`: real receives; a grow hands out the buffer retired last.
  - `wait_free_wakes_one_waiter_per_buffer`, `wait_free_grows_a_working_set`.
  - `working_set_grows_once_per_demand_level`: 8 connections, `working_set(2, 2)`: `enobufs` stops
    after the first round, `published` constant for 60 rounds; shrinks to `initial` when the demand
    drops; the buffers published when it returns are the ones used before.
  - Six "starved" tests (B2 acceptance): 12 connections, 4 buffers of 64 bytes, 9600 bytes each,
    every buffer held over a yield; plain ring, `working_set(4, 0)`, `working_set(1, 1)`, batch
    publish, incremental (with and without working set), bundles. All data in order, no `ENOBUFS`
    reaches the caller, `polls <= 4 × chunks`, a 60 s timeout against lost wakes.
- `tests/multishot_recv.rs`: `next_rearm` waits and resumes (TCP type on Unix sockets, and bundles);
  cancel and drop while waiting; a working set grows instead of waiting; `rearm_on` after `ENOBUFS`,
  in place, after EOF, with a foreign ring; `rearm_on` in flight keeps order, then 64 KiB through
  random moves between two rings; bundles.

Polls per received chunk in the starved tests (`metrics` build):

| ring | chunks | polls | per chunk | `enobufs` |
|---|---|---|---|---|
| plain | 1800 | 5047 | 2.80 | 1744 |
| `working_set(4, 0)` | 1800 | 5130 | 2.85 | 1783 |
| incremental | 1800 | 5130 | 2.85 | 1783 |
| bundles (counted per buffer) | 1800 | 3464 | 1.92 | 1128 |

- About one `ENOBUFS` per chunk is inherent: a multishot receive with more data pending ends again
  as soon as it used the buffer it was woken for. A starved chunk costs three polls (woken and
  re-arm; data; `ENOBUFS` and wait).

Results:

- `cargo test -p adaptif-gatotkaca-monoio`: all pass.
- `cargo test -p adaptif-gatotkaca-monoio --features metrics`: all pass.
- `cargo clippy --workspace`, and with `--features adaptif-gatotkaca-monoio/metrics`: the 3 old
  warnings in the library, none new (also none new with `--all-targets`).
- `cargo build --workspace --examples`, and with `metrics`: build. `cargo fmt` applied.

## Screening numbers

`reqresp --ring defer,skipenter,ringfd --cpu 2 --client-cpus 3,4 --warmup 1 --duration 4`, default
build, one run per cell entry. "base" is the `r3-g0` tree built into its own target directory.

### 8 connections, `--sizes 4k,16k,64k`, ring 4096 × 16 KiB (a 64 KiB frame is 4 buffers)

| variant | frames/s (each run) | frames per CPU-s (median) | user instr/frame | all cycles/frame |
|---|---|---|---|---|
| base `--path mshot` | 71.0k, 71.3k, 71.7k, 75.0k, 75.6k | 71.8k | 2672–2736 | 56.7k–60.4k |
| `--path mshot` | 70.2k, 74.7k, 74.7k, 75.0k, 76.0k | 74.7k | 2702–2722 | 56.4k–61.0k |
| `--path mshot --wset 16,16` | 79.8k, 85.3k, 91.0k, 91.6k, 91.9k, 92.6k, 93.3k | 91.6k | 2911–2951 | 46.0k–53.5k |
| `--path pipe` | 81.9k, 82.8k, 83.8k, 90.5k, 92.5k, 94.1k, 95.0k, 95.2k | 91.5k | 2096–2134 | 45.0k–52.2k |

- The working set takes `mshot` from about 74k to about 92k frames/s (+23%) and from 57k to 46k
  cycles per frame. (The `--wset` and `pipe` rows hold runs of two builds that differ only in the
  backoff cap and in a `rearm_on` detail. Final binary: `--wset` 79.8k, 91.0k, 91.6k; `pipe`
  83.8k, 92.5k, 94.1k.)
- Against `pipe`, in the three pairs measured back to back on the final binary: 91.6k / 94.1k
  (−2.6%), 91.0k / 92.5k (−1.7%), 79.8k / 83.8k (−4.8%, a noisy minute). So `mshot` with a working
  set is 2 to 3% behind per-connection buffers here, not within 2% on every pair.
- What is left is not cold buffers: `mshot` handles two completions per frame on average with
  16 KiB buffers and runs 2920 user instructions per frame against 2105.
- Without `--wset` the new code is at the base's level (2702–2722 against 2672–2736 instructions
  per frame).

### 8 connections, buffers that hold a whole frame (`--bufring N,72k`)

| variant | frames/s | user instr/frame | all cycles/frame |
|---|---|---|---|
| `mshot --bufring 1024,72k` | 74.6k, 75.6k | 2035, 2036 | 57.4k, 56.7k |
| `mshot --bufring 1024,72k --wset 16,16` | 96.6k, 97.3k | 2174, 2179 | 44.3k, 44.1k |
| `mshot --bufring 16,72k` (hand-made hot ring) | 96.5k, 97.1k | 2096, 2096 | 44.4k, 44.1k |
| `pipe` (same minutes) | 90.5k, 95.0k | 2097, 2096 | 47.3k, 45.1k |

- With one buffer per frame the working set equals the hand-made 16-buffer ring and is ahead of
  `pipe` in both pairs (+2% and +7%).

### 2000 connections, `--sizes 4k --bufring 4096,16k`

| variant | frames/s (each run) | user instr/frame | all cycles/frame |
|---|---|---|---|
| base `mshot` | 119.4k, 120.3k | 2352 | 35.9k, 35.6k |
| `mshot` | 118.8k, 119.2k, 120.5k | 2342 | 36.1k, 36.0k, 35.6k |
| `mshot --wset 16,16` | 121.9k, 122.0k, 122.1k, 122.4k; **94.1k, 95.5k** | 2421 | 35.0k–35.2k; 45.4k, 44.8k |
| `pipe` | 112.5k, 112.8k, 113.7k; **87.3k, 93.3k** | 2348 | 37.7k–38.1k; 49.0k, 45.9k |

- With the working set: +2% frames/s against the full ring, +8% against `pipe`, at +79 user
  instructions per frame (taking at reap time, plus the policy).
- **The bold runs are outliers I cannot explain**: the same instructions per frame, but 1.6 to 1.9
  times the user cycles per frame (3.6k–4.6k instead of 2.2k–2.7k). It hit `--wset` twice in six
  runs and `pipe` twice in five, never the two `mshot` rows without `--wset` (five runs). In a
  `metrics` run that was slow in the same way (97.0k frames/s) the ring was in its normal state
  (`published=80`, no `ENOBUFS` in the window), so it is not a state of the ring. Hypothesis, not
  tested: memory placement of the run (huge pages) or another agent's load on a sibling CPU.

### Ring state (`metrics` build, `BUFRING` lines of the example, counters since the start)

| load | `published` | `high_water` | `enobufs` total / in the window | `grown` / `retired` |
|---|---|---|---|---|
| 8 conns, no `--wset` | 4096 | 1 | 0 / 0 | 0 / 0 |
| 8 conns, `--wset 16,16`, 4 s | 32–36 | 32 | 5 / 4 | 69 / 51 |
| 8 conns, `--wset 16,16`, 20 s | 30–44 | 32 | 6 / 5 | 182 / 160 |
| 2000 conns, no `--wset` | 4096 | 1 | 0 / 0 | 0 / 0 |
| 2000 conns, `--wset 16,16`, `defer` | 80 | 413–453 (ramp) | 421–559 / 0 | 565–687 / 501–623 |
| 2000 conns, `--wset 16,16`, `coop` | 2016 | 2000 | 1721 / 0 | 2000 / 0 |

- 2000 connections: every `ENOBUFS` is in the ramp, none in the window, and `published` is constant
  afterwards (80 with `defer`, where a round carries 40 frames; 2016 with `coop`, where all 2000
  arrive in one tick).
- 8 connections: the ring does not come to rest. The usual window sees 16 to 18 buffers in use, so
  the target is 32 to 36; the rare tick in which all eight connections hold a 64 KiB frame needs 32
  and makes it grow by a step (or, right after a shrink to 30, end a few receives with
  `ENOBUFS`), and the next low window shrinks it again. About 16 buffers move in and out per
  second at first, less as the backoff lengthens the window (nothing moved between seconds 11 and
  18 of the 20 s run). Five `ENOBUFS` in the 20 s window, four in the 4 s one. So at 8 connections
  "`enobufs` stops growing" holds only roughly: a handful per run, not zero. No effect on the
  frames/s above that I could see.

### A starved ring in the reproducer (`--conns 200 --sizes 4k --bufring 16,4k --ring coop,skipenter,ringfd`, `metrics` builds, one run each)

| build | frames/s | task polls per frame | user instr/frame | `enobufs` in the window |
|---|---|---|---|---|
| base (yield, then `recv_multi` again) | 75.6k | 24.0 | 29,683 | 3,475,918 |
| `next_rearm` | 123.3k | 3.0 | 4,940 | 493,280 |

- `--conns 200 --bufring 64,4k` with `defer` (not starved): 141.0k (base, default build) and
  141.1k frames/s (`metrics` build; 23 `ENOBUFS`, all in the ramp).

## What was replaced, tried and rejected

- **Demand measured at poll time** (the plan's "`published − in_use` at `take`"): rejected, it
  reads 1 while the kernel holds dozens of buffers (see "Automatic policy"). Replaced by taking at
  reap time on working-set rings.
- **Taking at reap time on every ring**: not done. It would make `in_use` exact everywhere (and
  `next_rearm` herd-free on plain rings), but it changes when existing users see buffers counted and
  costs about 75 user instructions per chunk. Offered as an open question.
- **`step` buffers per `ENOBUFS`**: replaced (overshoot by a factor of `step` in a burst).
- **Wake every waiter per recycled buffer**, and **wake one without claims**: rejected by
  reasoning, not built (N waiters, or every stream that just saw `ENOBUFS`, re-arm for one buffer).
  Built and measured: ending the claim before the buffer was taken, and waking without the `woken`
  count, gave 3.58 polls per chunk in the plain-ring starved test and 3.00 on the exact rings; the
  final version gives 2.80 and 2.85.
- **"Greedy" claims for bundle streams** (claim every free buffer, hold back other wakes until the
  stream polled): rejected, a stream that is woken and not polled would block all others.
  Bundles keep one wake per free buffer and pay some extra re-arms.
- **`MAX_BACKOFF = 6`**: tried. One 4 s run at 8 connections with 6: 3 `ENOBUFS` (1 in the
  window); with 10: 5 (4 in the window). The runs differ more than the settings, so this is not
  decided by a measurement. 10 is kept on reasoning: only a long window can bridge bursts that
  are a second apart at 180k takes/s, and a ramp cannot drive the backoff far, because each step
  needs a whole window first.
- **First version cost 3% on rings without a working set** (2778–2824 instructions per frame against
  2672–2736): `BufRing::take` and the recycle path were no longer inlined. Fixed with `#[inline]`
  on `take` / `take_parts` / `recycle` / `fill_slot`, `#[inline(always)]` on `give`, and
  `#[inline(never)]` on `auto_take`; now 2702–2722.
- A vector for the reserve: replaced by the intrusive stack (no allocation, no `RefCell`).

## Open questions

1. **B1 gate at R8.** With 16 KiB buffers `mshot` + working set is 2 to 3% behind `pipe` here, with
   72 KiB buffers 2 to 7% ahead. The gate text says 4096 × 16 KiB. The lead should decide the
   geometry, or accept that the rest of the gap is the per-chunk cost of `mshot`.
2. **The ring does not rest at 8 connections** (above). A hysteresis (keep one `step` above
   `2 × high`) would stop it; I kept the rule of the plan.
3. **Take at reap time everywhere?** See "tried and rejected".
4. **Cost of taking at reap time**: about 75 user instructions per chunk, most of it the second
   queue in `lifecycle.rs` (not my file). A leaner `Taken` could halve it.
5. **`wait_free` is notify-one.** A caller that resolves and starts no receive strands the other
   waiters until the next buffer. `next_rearm` does not have the problem (it claims and re-arms in
   the same poll, and gives the claim back on any outcome).
6. **Raw users on a working-set ring** (raw `MultiOp`, single-shot receives taken at poll time):
   the demand is under-reported, the ring shrinks too far, and only the backoff limits the
   `ENOBUFS`. Documented; not tested with a load.
7. **The outliers at 2000 connections** are unexplained and affect `pipe` too. Gate runs should
   look at user cycles per frame to recognise them.
8. `enobufs` in the `RESULT` line of `reqresp --path mshot` is 0 on builds without `metrics`
   (`next_rearm` hides the error and the ring's counter needs `metrics`).

## Requested changes

1. **Registry line (applied by me, allowed by the rules):** `monoio/src/driver/raw.rs`

   ```rust
   pub use super::buf_ring::{
       BufRing, BufRingOptions, BufRingStats, Bundle, SelectedBuf, WaitFree,
   };
   ```

2. **Bench (`sut-monoio`, tune item `wset`):** one
   `BufRing::with_options(bgid, 4096, 16 << 10, BufRing::options().working_set(16, 16))` instead of
   hot + spill, and `stream.next_rearm()` instead of yield-and-retry.

3. **Release notes (0.9.0):** `BufRingStats` gained `published` (always) and `grown` / `retired`
   (`metrics`); a new ring's stats are no longer equal to `BufRingStats::default()`.

4. **AGENTS.md (r3-docs), proposed wording.**

   In "Provided buffers", a new option bullet:

   > - `working_set(initial, step)`: a ring is a FIFO, so with more buffers than are needed at once
   >   every receive lands in the buffer that was idle longest (8 connections on 4096 × 16 KiB:
   >   74k frames/s; with `working_set(16, 16)`: 92k; per-connection buffers: 92k). This option
   >   publishes only `initial` buffers and keeps the rest in a reserve, a stack, so the published
   >   buffers are always the ones used last. It grows by `step` when a `take` leaves none free and
   >   when `wait_free`/`next_rearm` find none, and on `ENOBUFS` (at least one buffer per failed
   >   receive); it shrinks, not below `initial`, when over `4 × published` takes less than half of
   >   the published buffers were in use at once. Size `entries` for the worst case and `initial`
   >   for the usual one. `recv_multi`/`recv_multi_bundle` take at reap time on such a ring, which
   >   is what makes the demand visible; with raw SQEs take in the hook. By hand, on any ring:
   >   `ring.published()`, `grow_published(n)`, `shrink_published(n)` (takes effect as buffers are
   >   recycled; growing hands back the buffers retired last). Stats: `published` (always), `grown`,
   >   `retired` (`metrics`).

   After the options list:

   > `ring.wait_free().await` resolves when a buffer is free by the ring's own count (published
   > minus taken; a working set grows first). Each buffer that comes back wakes one waiter, the
   > oldest; start a receive when it resolves.

   In "Multishot ops", after the `recv_multi` bullet:

   > - `stream.next_rearm().await` (`RecvStream`, `RecvBundleStream`; also `poll_next_rearm`): like
   >   `next`, but never yields `ENOBUFS`. It waits for a free buffer without polling, re-arms and
   >   continues, with no copy. On a ring that stays too small every chunk costs a re-arm (200
   >   connections on 16 buffers: 123k frames/s at 3 task polls per frame, against 76k at 24 with
   >   yield-and-retry), so size the ring or give it a working set.
   > - `stream.rearm_on(&other)`: continue on another ring. If the recv has ended (`ENOBUFS`), the
   >   next poll arms it there (with the same ring: a re-arm in place). If it is in flight, it is
   >   cancelled, the stream yields what the old ring still delivers, then arms on the new one:
   >   nothing is lost or reordered. `stream.ring()` is the ring in use.
