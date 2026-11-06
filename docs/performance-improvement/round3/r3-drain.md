# r3-drain: bounded task-work drain for `DEFER_TASKRUN` rings (D1), `set_min_wait`

Branch `r3/r3-drain`, base `r3-g0`. Three commits:

1. `feat(driver): drain_taskrun for DEFER_TASKRUN rings, set_min_wait (r3-drain: D1)`: both variants, the
   counter, `set_min_wait`, the tests, the `reqresp` ring items.
2. `docs(round3): r3-drain notes`: this file.
3. `perf(driver): suspend drain_taskrun on a standing backlog (r3-drain, optional)`: its own commit, for the lead
   to keep or drop after a quiet measurement (see "The optional commit").

All measurements are **screening, machine not quiet**: other agents were building the whole time. Frames/s of
the same command varied by 10–20% between runs. Where that hides the effect I give numbers that do not depend
on the machine's speed (counters, shares, user instructions).

## What changed

| file | change |
|---|---|
| `monoio/src/builder.rs` | `RuntimeBuilder::drain_taskrun(max)`; `#[doc(hidden)] drain_taskrun_pre_tick(max)` |
| `monoio/src/driver/raw.rs` | `uring::set_min_wait(wait, batch)`, `IoUringDriver::set_min_wait`, private `Inner::set_min_wait` |
| `monoio/src/driver/uring/mod.rs` | `DriverOptions::drain` (private enum `TaskrunDrain`: `Off`, `AfterTick(max)`, `BeforeTick(max)`) with two setters; `EnterConfig::{drain, min_wait_values, set_min_wait}`; `DriverStats::drain_enters` (`DRIVER_STATS_U64S` 20 → 21 scalars); `inner_park` (the drain); `tick` split into `tick` + `reap`; `enter` → `enter` + `enter_with(want, ts, drain)`; new private `drain_after_tick`, `drain_before_tick`, `UringInner::set_min_wait` next to `enter` |
| `monoio/tests/drain_taskrun.rs` | new, 8 tests (7 without `metrics`); 9 with the optional commit |
| `monoio/examples/reqresp/ring_ext.rs` | `drain<N>` → `drain_taskrun(N)`, `draincq<N>` → `drain_taskrun_pre_tick(N)` |

- No `pub use` line was needed: `set_min_wait` is a `pub fn` of `driver/raw.rs`, which is `monoio::uring`.
- `Driver::submit` and `nothing_to_enter` are unchanged. The functions r3-op owns are untouched (they still call
  `tick()`, which keeps its signature).
- With a default builder nothing changes: `TaskrunDrain::Off` costs one `match` per park. Without `metrics` the
  only other difference is the value of `DRIVER_STATS_U64S`.

## Semantics

### Common to both variants

- Stored in `DriverOptions`, resolved once in `EnterConfig::new`: the drain is `Off` unless the ring's setup
  flags contain **both** `DEFER_TASKRUN` and `TASKRUN_FLAG`. `max = 0` is `Off`. The later of the two builder
  calls wins.
- It runs only in `inner_park`, only on the waiting branch (`need_wait`). **Not after the submit-only branch**
  (`need_wait == false`, which exists only with `sync` when a foreign waker fired): that branch is the cold path
  under another name, a task is ready already, and `Driver::submit` does not drain either (ROUND3.md D1 says
  the cold path needs nothing). It also keeps `drain_enters <= max × enter_wait`. I have no measurement for
  this choice: the reproducer runs without `sync`. If the lead wants the drain there, it is the one
  `if need_wait` in `inner_park`.
- A drain enter goes through `enter_with(min_complete, None, drain = true)`: `GETEVENTS` is forced, `min_wait`
  is skipped (`if want > 0 && !drain`), no timespec, so `EXT_ARG` is never set. It cannot take the batching
  path, whatever `min_complete` is.
- Best effort: a drain enter that fails ends the drain and the error is dropped. Nothing is lost (queued SQEs
  and pending task work stay) and the next enter reports the error if it lasts.
- **A drain enter that brought no CQE ends the drain.** This was not in the plan; see "Kernel finding".
- `metrics`: each drain enter counts in `drain_enters` and in `enter_submit` (so the example's
  `enters_per_frame` includes it), never in `enter_wait`.

### Variant A, `drain_taskrun(max)` (the documented one)

```text
enter(1, ..)                         // the wait, unchanged
tick()
[metrics] enter_left_taskrun += 1 if the flag is set
n = 0
while n < max && sq.taskrun():
    enter_with(0, None, drain)       // GETEVENTS, min_complete 0: at most 20 items, never sleeps
    if the CQ is empty: break
    reap()                           // the body of tick(), without advancing the tick counter
    n += 1
drop_deferred; deliver               // once, as before
```

- The `enter_left_taskrun` block is directly after the first `tick()`, unchanged in meaning.
- `reap()` instead of `tick()`: the tick counter behind `op_ticks_by_opcode` does not advance for a drain
  pass. A completion fetched by the drain reaches its task in the same round as the park's own completions, so
  it belongs in bucket 0 ("same tick"). With `tick()` the `recv_same_tick` share could not show what the drain
  does. Each drain pass still adds its own entry to `cqes_per_tick`.

### Variant B, `drain_taskrun_pre_tick(max)` (`#[doc(hidden)]`, for comparison)

```text
enter(1, ..)
n = 0
while n < max && sq.taskrun():
    len = cq.len()                               // unreaped CQEs
    if len + max(20, len) > cq.capacity(): break // the next run could overflow the CQ
    enter_with(if len > 20 { len } else { 0 }, None, drain)
    n += 1
    if cq.len() == len: break                    // brought nothing
tick()                                           // one tick for everything
[metrics] enter_left_taskrun += 1 if n > 0 or the flag is set
```

- Why it never blocks (comment in `drain_before_tick`; KV1 probe lines [A], [D], [E]): the kernel runs task
  work when the wait begins, then returns at once if the CQ holds at least `min_complete` CQEs
  (`io_cqring_wait`: `if (__io_cqring_events_user(ctx) >= min_events) return 0;`). It holds at least `len`: only
  this thread reaps, it does not between the count and the enter, and the kernel only adds. `min_complete = 0`
  never waits. No timeout and no `min_wait` are passed.
- `min_complete` is the CQ length only above 20: up to 20 the kernel's cap is 20 either way, and a non-zero
  `min_complete` has a cost (see "Kernel finding").
- `enter_left_taskrun` keeps its meaning: the pre-tick drain runs only because the wait left work, so `n > 0`
  counts as "left".

### `uring::set_min_wait(wait: Option<Duration>, batch: u32) -> io::Result<()>`

- Two stores into `EnterConfig::{min_wait_usec, min_wait_batch}` of the current thread's driver; applies from
  the next waiting enter. Same rule as the builder (`EnterConfig::min_wait_values`, now shared by both):
  `None` or `batch <= 1` is off, the duration is clamped to 1 µs .. `u32::MAX` µs.
- `Unsupported` when a wait is asked for and the kernel lacks `IORING_FEAT_MIN_TIMEOUT`; the setting is then
  unchanged. Turning it off always succeeds. (The builder's `min_wait` still silently ignores the setting on such
  kernels: unchanged behaviour.)
- `IoUringDriver::set_min_wait` is the same through the driver, usable outside `block_on`; with the timer it is
  `rt.driver().inner().set_min_wait(..)`. The free function works under the timer driver too.

### `DriverStats::drain_enters`

Extra enters made by either variant. With variant A, `drain_enters / enter_left_taskrun` is the average number
of drain enters per wait that left work.

## Kernel finding: `IORING_SQ_TASKRUN` can be stale after an enter with `min_complete > 1`

Not in r3-probe.md. It changed both variants.

- `__io_run_local_work` clears the flag once, **before** its `again:` label (the source quote in r3-probe.md,
  KV1). A run that has handled fewer items than `min_events` loops while work is pending. An item queued to
  the empty list during that loop sets the flag (`io_req_local_work_add`: `if (!head) atomic_or(..)`) and is
  then handled by the same run. Nothing clears the flag again: it stays set with nothing pending until the next
  run that finds work.
- With `min_complete <= 1` (the plain wait, variant A's enters) the run never loops after handling an item, so
  the flag is exact apart from the narrow race r3-probe.md names (an item added while a run starts). That is
  what the KV1 probe saw.
- It happens with `min_wait` (the wait asks for `batch` CQEs) and with variant B's `min_complete = cq.len()`.

Evidence (temporary counters in a scratch build, since removed; `reqresp --conns 9 --bulk 2 --bulk-work 8k`,
4 s):

| ring items | drain enters | of those, brought no CQE | parks whose drain used all `max` enters |
|---|---|---|---|
| `draincq32`, first version (`min_complete = cq.len()` always, no stop rule) | 71,379 | 58,931 | 1,852 |
| `draincq4`, first version | 21,327 | 9,332 | 3,106 |
| `drain32` (variant A, before the stop rule) | 15,164 | 0 | 0 |
| `drain4` (variant A, before the stop rule) | 13,746 | 0 | 54 |
| `draincq32`, as committed (0 up to 20 CQEs, stop on an enter without CQE) | 16,155 | 0 | 0 |
| `minwait80,drain4`, as committed | 18,119 | 16,983 | 4 |

- First version of B: a stale flag made the loop spin to its bound with enters that did nothing.
- `minwait80,drain4`: after a batching wait the flag is stale in about 94% of the flagged parks. The stop rule
  holds the cost at one empty enter per park (about 0.26 µs, r3-probe [H]). Without it variant A would make
  `max`.
- Consequences outside this package (for r3-docs / the lead):
  - With `min_wait`, `enter_left_taskrun / enter_wait` is mostly stale flags (67–72% in the mixed load, 93–96%
    at 8 connections), not work left behind.
  - With `min_wait` and `skip_empty_enter`, a stale flag makes `nothing_to_enter` false: one enter that could
    have been skipped.
- Not established: I did not write a deterministic probe for it (it needs an arrival during a run). The
  mechanism is from the source text, the counters fit it exactly (empties only where `min_complete > 1`).

## Validation

Builds: `target/release` (default) for frames/s, `target/metrics` (`--features metrics`) for the counters. All
runs `--cpu 2 --client-cpus 3,4 --warmup 1 --duration 4 --path pipe`, under `flock /tmp/r3-measure.lock`, the
variants interleaved. Commit 1 unless stated.

### Mixed load: `--conns 9 --bulk 2 --bulk-work 8k --ring R`

Counters (`metrics` build; 4 runs each, `draincq4` 2 runs; min–max):

| R | frames per round | `recv_same_tick` | `enter_left_taskrun / enter_wait` | `drain_enters` per park | enters per frame |
|---|---|---|---|---|---|
| `coop,skipenter,ringfd` | 8.55–8.91 | 95.9–99.4% | 0.6–2.0% | 0 | 0.112–0.117 |
| `defer,skipenter,ringfd` | 7.49–8.06 | 82.1–89.0% | 43.1–49.6% | 0 | 0.124–0.133 |
| `defer,skipenter,ringfd,drain4` | 8.50–8.74 | 95.1–97.3% | 49.5–59.6% | 0.54–0.70 | 0.177–0.200 |
| `defer,skipenter,ringfd,draincq4` | 8.57–8.61 | 95.7–96.2% | 47.7–55.8% | 0.52–0.64 | 0.177–0.191 |
| `defer,skipenter,ringfd,minwait80` | 8.28–8.62 | 93.3–96.3% | 66.7–71.7% (stale flags) | 0 | 0.116–0.121 |

`drain_enters` was 13,249–18,692 per 4 s window for `drain4` and 11,215–17,299 for `draincq4`. Two of the four
runs of each row are from a first series made before the stop rule existed; it never fires in these four
configurations without `min_wait` (0 empty enters, table above), and for `minwait80` no drain runs. The
`draincq4` row has only the two runs made with the committed code.

Other bounds, one `metrics` run each: `drain1` 8.59 frames per round, 96.0% same tick, 0.58 drain enters per
park; `drain2` 8.69, 97.0%, 0.64; `drain8` 8.54, 95.5%, 0.65; `drain32` 8.46, 94.7%, 0.64 (a disturbed run).
The bound hardly matters here: one drain enter is usually enough.

Default build, 6 runs each (`drain1`: 3):

| R | frames/s median [min..max] | best run: frames/s, bulk MB/s, p99 | p99 median | frames per bulk MB, median [min..max] | user instr per frame, median |
|---|---|---|---|---|---|
| `coop…` | 53,418 [48,505..58,941] | 58,941, 433, 207 µs | 315 µs | 133.1 [130.8..136.2] | 13,069 |
| `defer…` | 48,575 [46,685..57,332] | 57,332, 459, 299 µs | 365 µs | 122.0 [117.9..125.6] | 14,118 |
| `defer…,drain4` | 51,632 [47,843..58,647] | 58,647, 441, 283 µs | 324 µs | 130.4 [125.8..132.9] | 13,321 |
| `defer…,draincq4` | 57,150 [48,564..58,585] | 58,585, 447, 291 µs | 299 µs | 130.5 [125.5..132.8] | 13,310 |
| `defer…,minwait80` | 51,582 [49,413..58,487] | 58,487, 454, 299 µs | 332 µs | 129.4 [127.6..131.5] | 13,401 |
| `defer…,drain1` | 57,858 [53,135..58,619] | 58,619, 445, 291 µs | 307 µs | 130.2 [124.7..131.7] | 13,342 |

- "Frames per bulk MB" is frames/s ÷ bulk MB/s of the same run: how the server's time is split between the two
  loads. D1 is a shift of that split towards bulk. It moves far less with the machine's noise than frames/s.
- "User instructions per frame" includes the bulk writers' user-space work, so it falls when the server serves
  more frames per bulk chunk. It is exact.
- The medians of frames/s are not usable: the ranges (±10%) are larger than the effect (3%). The best runs are
  the ones closest to a quiet machine (cycles per frame 72.6k–74.7k).

### 8 connections: `--conns 8 --ring R`

| R | frames/s median [min..max] (default, 3 runs) | p50 / p99 | frames per round | `recv_same_tick` | `drain_enters` per park | enters per frame |
|---|---|---|---|---|---|---|
| `defer,skipenter,ringfd` | 94,992 [94,830..95,000] | 76.8 / 170.0 µs | 6.62–6.77 | 80.5–82.9% | 0 | 0.148–0.151 |
| `…,drain4` | 94,005 [88,855..94,603] | 78.8 / 161.8–165.9 µs | 7.06–7.63 | 90.5–95.8% | 0.64–0.70 | 0.215–0.240 |
| `…,draincq4` | 94,223 [94,165..94,736] | 76.8–78.8 / 161.8–165.9 µs | 7.43–7.55 | 93.5–94.8% | 0.62–0.64 | 0.215–0.221 |
| `…,minwait80` | 94,724 [93,006..95,030] | 76.8–78.8 / 161.8–170.0 µs | 7.26–7.44 | 91.4–93.5% | 0 | 0.134–0.138 |

(Counters: `metrics` build, 2 runs each.)

### 2000 connections: `--conns 2000 --sizes 4k --ring R`

| R | frames/s median [min..max], runs (default) | the undisturbed runs (top cluster, cycles per frame ≤ 39.3k) | p99 of those | frames per round | `drain_enters` per park | enters per frame |
|---|---|---|---|---|---|---|
| `defer,skipenter,ringfd` | 106,994 [89,407..113,074], 7 | 112,587, 113,074, 112,058 | 18.1–19.7 ms | 40.0 | 0 | 0.025 |
| `…,drain1` | 111,154 [102,796..111,354], 4 | 111,354, 111,062, 111,245 | 19.1–19.7 ms | no `metrics` run (60 by construction) | no `metrics` run | no `metrics` run |
| `…,drain4` | 109,064 [81,733..109,905], 7 | 109,346, 109,841, 109,064, 109,905 | 19.1–21.2 ms | 120.0 | 4.00 | 0.042 |
| `…,draincq4` | 83,878 [81,451..89,313], 3 | none (`metrics` build: 90,720, 95,772) | 24.4–29.6 ms | 2000.0 | 1.00 | 0.001 |
| `…,drain4` with commit 3 | 96,751 [84,554..111,800], 4 | 111,800 | 19.1 ms | 42.4 | 0.12 | 0.026 |

- Means of the undisturbed runs: `defer` 112,573; `drain1` 111,220 (−1.2%); `drain4` 109,539 (−2.7%). I picked
  those runs after the fact, by their cycles per frame: each variant's runs fall into one tight cluster at the
  top (within 1%) and scattered slower ones. The lead's quiet slot should replace this.
- The row with commit 3 was measured in a noisy window: `defer` gave 91,138 [85,246..98,985] (4 runs) in the
  same window. Its one quiet run (111,800 at 38.3k cycles per frame) is at `defer`'s level.

### 200 connections next to bulk: `--conns 200 --sizes 4k --bulk 2 --bulk-work 8k` (one `metrics` run each)

| R | frames/s | bulk MB/s | frames per round | frames per bulk MB |
|---|---|---|---|---|
| `coop,skipenter,ringfd` | 117,852 | 39 | 200.0 | 3,053 |
| `defer,skipenter,ringfd` | 84,743 (disturbed run) | 138 | 40.1 | 612 |
| `…,drain4` | 120,746 | 66 | 120.4 | 1,838 |
| `…,draincq4` | 119,045 | 39 | 199.9 | 3,052 |
| `…,drain4` with commit 3 | 108,553 | 167 | 42.6 | 650 |

At 40 connections next to bulk (`--conns 40 --bulk 2 --bulk-work 8k`, one run each) there is nothing to fix:
39.4–39.9 frames per round and 99.2–99.8% same tick with every ring.

### What the numbers show

1. **Mixed load, the D1 case.** The drain does what it was built for, by the counters: the same-tick share goes
   from 82–89% to 95–97% and frames per round from 7.5–8.1 to 8.5–8.7. That is the level of `minwait80`
   (93–96%, 8.3–8.6), reached without sleeping. It is **not quite `coop`** (96–99%, 8.6–8.9): 1 to 3 points
   remain, and a larger bound does not close them (`drain8`, `drain32` are no better than `drain1`).
2. **Frames/s in the mixed load** cannot be resolved on this machine. By the best runs: `coop` 58,941, `defer`
   57,332 (−2.7%), `drain4` 58,647 (−0.5%), `draincq4` 58,585, `minwait80` 58,487, `drain1` 58,619. By the
   split: `coop` 133.1 frames per bulk MB, `defer` 122.0 (−8%), drain 130.2–130.5 (−2%), `minwait80` 129.4. The
   drain closes roughly three quarters of the gap. The goal "reach the `coop` values" is met for frames per
   round within the ranges, and missed by 1–3 points for the same-tick share.
3. **The price is syscalls**: 0.18–0.20 enters per frame against 0.125–0.133 (+50%). Each is made only when the
   flag is set, and 0.5–0.7 are made per park.
4. **8 connections**: the same-tick share rises (81–83% → 91–96%), frames/s does not: −0.4% to −1% for both
   variants, p99 4–8 µs lower, p50 one histogram step (2 µs) higher. `minwait80` is 0.3% below `defer` here.
   The server is idle 97% of the time at this load, so a frame the drain collects delays the reply to the frame
   that woke the loop by about as much as it gains itself. The acceptance line "R8: not below
   `$MANY:minwait80`" is at the edge: 94,005 against 94,724 (medians of 3, −0.8%).
5. **2000 connections, variant A**: every drain enter comes back full, so the round grows from 40 to
   `40 + 20 × max` frames and nothing else changes. That costs throughput: −1.2% with `drain1`, −2.7% with
   `drain4` (quiet runs). Bigger rounds are slower at this load, as G0 found for `coop` (2000 per round, −10%).
   `drain4` is outside the D1 acceptance bound of 2% here; `drain1` is inside.
6. **2000 connections, variant B**: one enter with `min_complete` of about 2000 makes the kernel loop until
   nothing is pending, so every round carries all 2000 frames: the `coop` regime. Frames/s falls by 12–15%
   against plain `defer` (90,720–95,772 against 107,649–112,035, `metrics` build), with p99 at 24–25 ms against
   18.6 ms in `defer`'s undisturbed run. `max` does not bound this: the bound is in enters, and one enter takes
   everything.
7. **200 connections next to bulk**: the drain moves the split towards the connections in proportion to the
   round it builds: 40 frames per round (`defer`) → 120 (`drain4`) → 200 (`draincq4`, as `coop`). Whether that
   is wanted is a policy question (it is D1's direction: the frames are the priority); `max` is the knob.

## The optional commit: suspend the drain on a standing backlog

Commit 3 (`perf(driver): suspend drain_taskrun on a standing backlog`). Variant A only.

- Rule: when a drain used all `max` enters, each brought a full run (≥ 20 CQEs), and the flag is still set, the
  ring has a standing backlog (many connections ready at once), not a few late completions. The drain is then
  skipped for the next 32 waiting parks (`DRAIN_PAUSE`; state in `EnterConfig::drain_pause`).
- It does not show in the mixed load (a drain pass brings 1–3 CQEs there, rarely more): 2 `metrics` runs with
  it gave 8.54–8.69 frames per round, 95.5–97.0% same tick, 0.62–0.71 drain enters per park, the same as
  without. At 8 connections it cannot trigger (a pass cannot bring 20 completions); not measured there.
- For it: 2000 connections with `drain4` go from 120 to 42.4 frames per round and from 0.042 to 0.026 enters per
  frame; the one quiet run is at `defer`'s level (111,800) instead of −2.7%.
- Against it: with hundreds of connections next to other work the drain stops shifting the split (200
  connections + bulk: 650 frames per bulk MB against 1,838 without the commit, 612 for plain `defer`). The
  behaviour becomes bimodal, and the bound `max` no longer means "this many more batches per round".
- Test: `standing_backlog_suspends_the_drain` (three bursts of 200 ready receives with `drain_taskrun(1)`: the
  first park delivers 40, the next burst's 20, after 64 parks 40 again). The commit also changes two assertions
  of `drain_delivers_more_per_park` (`drain_enters` is exactly 1 and 4 there) and adds a line to the builder
  doc.
- My recommendation: **drop it** and run the many-connection configuration with `drain1` (or `drain2`), which
  is inside the 2% bound without a second mechanism and loses nothing in the mixed load. Keep it only if the
  gate needs `drain4` and measures more than 2% at R2000.

## Recommendation between A and B

**Keep A, remove B.**

- In the D1 case (CQ ≤ 20 at the drain) B makes the same enters as A. It saves the extra reap passes, which
  are cheap. No difference in any counter beyond the run-to-run spread.
- With many connections B is unbounded and turns `defer` into the `coop` regime (−12–15% at 2000 connections).
  A is bounded by `max` as documented.
- B depends on a second kernel detail (`min_complete` raising the cap) and creates stale flags by itself; A's
  enters cannot.
- Removing B: delete `TaskrunDrain::BeforeTick`, `drain_before_tick`, `DriverOptions::drain_taskrun_pre_tick`,
  the builder method, the `pre_drained` lines in `inner_park` (the `metric!` condition goes back to
  `need_wait && taskrun()`), `draincq<N>` in `ring_ext.rs`, and the pre-tick cases in the tests. `enter_with`
  can then lose its `want` use for drains, but keeping it costs nothing.

## Tests (`monoio/tests/drain_taskrun.rs`)

Without `metrics` the tests observe behaviour (how many receives had completed when the main future ran again
after one park; elapsed time); the counter assertions are `#[cfg(feature = "metrics")]`.

- `drain_delivers_more_per_park` (a): 200 receives armed in the kernel, made ready by `write(2)`, then one park.
  Completed after that park, on 7.0: plain 20, `drain 1` 40, `drain 4` 100, pre-tick `1` 40, pre-tick `4` 200.
  Asserted as inequalities on Linux ≥ 6.13 (plain < A1 < A4 < 200 = B4, B1 ≥ A1), "never less" on older kernels.
  `metrics`: fewer waits with the drain, `drain_enters` values, `drain_enters <= enter_submit`,
  `<= 4 × enter_wait`.
- `drain_is_off_where_it_does_not_apply` (b): `max = 0`, "4 then 0", a default ring, `coop_taskrun`,
  `coop_taskrun + taskrun_flag`, each compared with the same ring without the call: same completions per park;
  `metrics`: `drain_enters == 0` and identical `enter_wait`, `enter_submit`, `cqes_per_tick`.
- `drain_enter_does_not_wait_for_completions` (c): 64 pending task-work items that post no CQE (receives with
  `CQE_SKIP_SUCCESS`) behind one that does. Every drain enter runs work and gets nothing for it: the case in
  which an enter that waited for completions would sleep. Both variants return at once (a watchdog thread aborts
  the test process after 30 s), run more items than the plain park, and stop after the first enter without a
  CQE (`drain 1` and `drain 8` run the same number).
- `parks_with_a_timeout_return_on_time` (c): timer sleeps on an idle ring (the park times out with an empty CQ),
  then 200 bursts from another thread at 0–1.2 ms intervals against 1 ms timer parks; both variants.
- `bare_park_timeout` (c): `Driver::park_timeout` called directly, three 10 ms parks.
- `drain_enter_ignores_min_wait`: `set_min_wait(400 ms, 96)` with 200 ready receives. The waiting enter gets its
  96 at once; drain enters that took the batching path would sleep once fewer than 96 are left. The park ends
  in under 1 ms (asserted < 200 ms).
- `set_min_wait_round_trip` (d): on, off, on with another value, off by `batch = 1` and `0`, through the driver
  outside `block_on`, overriding the builder's value, under the timer driver. "On" is seen as a NOP round trip
  of ≥ 80 ms with a 100 ms wait, "off" as < 40 ms (best of 5). Skips the timing part with `Unsupported`.
- `drained_completion_counts_for_the_tick_of_its_park` (`metrics`): a `RECV(POLL_FIRST)` submitted by the park's
  own enter whose completion that enter leaves behind (51st task-work item). `op_ticks_by_opcode[RECV]` is
  `[0, 1, 0, 0]` without the drain and `[1, 0, 0, 0]` with either variant.

What is not covered: a flagged ring with an **empty** CQ before a pre-tick drain. It cannot be built on purpose:
a waiting enter that returns without a CQE (timeout) has run every pending item on its way (its wake-up loop
runs one item per iteration until a CQE exists), so the flag can only be set by an item queued in the instant
between the kernel's last look and our read. The enter is then `min_complete = 0`, which the KV1 probe shows
never waits ([A]); the burst test above hits that window by chance at best.

## Test results (this worktree)

Run as `taskset -cp 0,1,5,6,7,11 $$; CARGO_BUILD_JOBS=4 cargo ...`.

| command | commit 1 | with commit 3 |
|---|---|---|
| `cargo test -p adaptif-gatotkaca-monoio` | exit 0; 494 passed, 0 failed, 8 ignored | exit 0; 495 passed, 0 failed, 8 ignored |
| `cargo test -p adaptif-gatotkaca-monoio --features metrics` | exit 0; 514 passed, 0 failed, 8 ignored | exit 0; 515 passed, 0 failed, 8 ignored |
| `cargo clippy --workspace` | the 3 old lib warnings | the same |
| `cargo clippy --workspace --features adaptif-gatotkaca-monoio/metrics` | the 3 old lib warnings | the same |
| `cargo build --workspace --examples`, and with `--features adaptif-gatotkaca-monoio/metrics` | ok | ok |
| `cargo fmt` | applied | applied |

- No test failed in any full run, so nothing had to be rerun alone.
- `cargo clippy --workspace --all-targets` also shows warnings in `tests/async_write_rent_boxed.rs` and
  `tests/async_write_rent_cursor.rs`. They are not from this package; `tests/drain_taskrun.rs` has none.
- Also run, not required: `--features metrics,sync --test drain_taskrun` with commit 3 (9 passed; the first
  park still delivers 20 with `sync`), and the test file 8 times in a row with `metrics` and 4 times without
  (all passed).

## Tried and rejected

- **B with `min_complete = cq.len()` always, loop until the flag clears.** 58,931 of 71,379 drain enters did
  nothing with `draincq32` (table above). Replaced by "0 up to 20 CQEs" and the stop rule.
- **A without the stop rule.** Correct in the default configuration (0 empty enters in 15,164), but next to
  `min_wait` it would make `max` empty enters per park.
- **`tick()` in the drain loop**, as the plan sketches. It moves drained completions to the "next tick" bucket,
  which hides the effect from the one counter that measures it.
- **Draining on the cold path (`Driver::submit`) or the submit-only park.** Not built, see "Semantics".
- **One enter that drains everything** (`GETEVENTS | EXT_ARG`, zero timeout, large `min_complete`; r3-probe KV1
  point 5). Not built: variant B at 2000 connections is that behaviour, and it is 12–15% slower than not
  draining.
- **A pause after the first full drain pass** instead of after `max` full passes. Not built: with `max > 1` it
  would give up while the drain can still empty the queue.
- **`#[cold]` on the two drain helpers.** Removed before measuring: they are the hot path when the option is on.
- **A getter for the current `min_wait`.** Not asked for; the tests observe the behaviour.

## Open questions

1. The remaining 1–3 points of same-tick share against `coop` in the mixed load. A larger bound does not help,
   so these are frames that arrive after the last drain enter found nothing. Only waiting (`min_wait`) or the
   bench's real timing can tell whether it matters.
2. The real benchmark differs from the reproducer in what G0 lists (−16% there, −3% here). On the bench the
   waiting enter may sleep and wake with one item (KV1 [F]); the drain then has more to fetch per park than
   the 0.5–0.7 enters seen here. Not measurable locally.
3. R8 is −0.4% to −1% with the drain (screening). If the gate confirms it, a rule "no drain when the wait slept
   and brought a single CQE" is not the answer: that is exactly the bench's D1 case.
4. Whether `drain_taskrun` should also run after the submit-only park (one condition).
5. The stale flag after `min_wait`: `enter_left_taskrun` and `skip_empty_enter` are affected (see "Kernel
   finding"). No fix here; a user-space fix would need to know that nothing is pending, which the ring does not
   expose.

## Requested changes

Nothing outside my files was changed locally.

1. `monoio/examples/reqresp/main.rs`, usage text, line `--ring a,b,..`: add `drain<N>, draincq<N>` to the list
   (the items work without it; only the help is incomplete).
2. `AGENTS.md`, "Runtime configuration", after the `min_wait` bullet:

   ```markdown
   - `drain_taskrun(max)` (off by default; only on a ring with `DEFER_TASKRUN` and `TASKRUN_FLAG`, which
     `defer_taskrun()` sets): after a waiting enter returned, enter again without waiting while the kernel flags
     task work (`IORING_SQ_TASKRUN`), at most `max` times, reaping after each, before the tasks run. One drain
     enter delivers at most 20 completions (Linux 6.13+; everything on older kernels), is made only when the
     flag is set, and never sleeps; one that brings nothing ends the drain. For a few busy connections next to
     bulk work, where the bulk completions end every wait early and replies that arrive a moment later lose a
     round (`reqresp --conns 9 --bulk 2`: receives reaped in the tick of their enter 82–89% → 95–97%, `coop`
     96–99%). It replaces `min_wait` there; do not combine them (after a batching wait the flag is often set
     with nothing pending, which costs one empty enter per park). `max` 1 or 2 is enough. With thousands of
     ready connections every drain enter comes back full and only lengthens the round (2000 connections: about
     −1% frames/s with 1, −3% with 4). Stat: `drain_enters`.
   - `uring::set_min_wait(wait, batch)` (or `IoUringDriver::set_min_wait`): changes `min_wait` at run time, from
     the next park on. `None` or `batch < 2` turns it off; `Unsupported` if the kernel lacks
     `IORING_FEAT_MIN_TIMEOUT` (6.12).
   ```

3. `AGENTS.md`, "Stats" bullet, after "enter calls (submit/wait/skipped)": add "`drain_enters` (extra enters of
   `drain_taskrun`, included in the submit count)".
4. `AGENTS.md`, wherever r3-docs puts kernel notes: "On a `defer_taskrun` ring, after an enter that asked for
   more than one completion (`min_wait`), `IORING_SQ_TASKRUN` can stay set with nothing pending:
   `enter_left_taskrun` then over-counts, and `skip_empty_enter` makes one enter it could have skipped."
5. r3-probe.md, KV1 "Consequence for D1" point 3 says the flag is "reliable for 'work is left'". That holds for
   `min_complete <= 1` only; see "Kernel finding" above.
