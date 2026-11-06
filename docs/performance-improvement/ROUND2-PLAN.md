# Round 2 work plan

Implements [ROUND2.md](ROUND2.md). Item ids (M1–M8, F1–F5) refer to that document. The benchmark lives in
`../gatotkaca-transport` (bench-runner, `sut-monoio`, `sut-tokio`); it depends on `=0.7.1` with features
`async-cancel, bytes, utils` (no `sync`).

Two changes run through the whole round:
1. **The fixes and features of ROUND2.md**, split into packages that agents can do in parallel.
2. **A `metrics` cargo feature, off by default.** Without it, the hot paths contain no counter updates, no
   high-water checks and no clock reads. All instrumentation (today's `DriverStats`, the pool counters, and the new
   scheduler stats of F2) compiles in only with `--features metrics`.

---

## Rules for every package

- New behaviour is opt-in (builder method, cargo feature or new API). With default features and a default builder,
  scheduling behaves as in 0.7.1.
- **Instrumentation only behind `metrics`.** Every new counter, histogram, high-water mark or `Instant::now()` that
  exists only to be reported goes through the `metric!` macro (see "Metrics design"). Review rejects a bare
  `stats.x += 1` on a hot path.
- No panic on a submit path. Errors come back as `io::Result` / `BufResult` (buffer returned).
- Kernel-dependent features check support at runtime and fail with `Unsupported`/`InvalidInput`; tests skip on older
  kernels.
- Each package adds tests in `monoio/tests/` and passes, in its worktree:
  - `cargo test -p adaptif-gatotkaca-monoio`
  - `cargo test -p adaptif-gatotkaca-monoio --features metrics`
  - `cargo clippy --workspace` and `cargo clippy --workspace --features adaptif-gatotkaca-monoio/metrics`
  - `cargo fmt`
- Packages in one wave own disjoint files. A package that needs a change in a file it does not own writes it up in
  its notes; the lead applies it at merge.
- **No benchmarking inside packages.** Packages build and run functional tests in parallel, and that load would make
  any measurement worthless. Packages may run the fork's `fairness` example (W0-repro) for a quick sanity check, and
  they label any such number "screening, machine not quiet". All verdicts come from the measurement gates (G0–G3),
  which the lead runs with no agent active.
- Each package writes `docs/performance-improvement/round2/<package>.md`: what changed, what was tried and rejected,
  open questions, changes requested in files it does not own.

## How agents run

- One agent per package, each in its own git worktree (`isolation: "worktree"`), on branch `r2/<package>` from the
  wave's base commit.
- The lead (orchestrator) does the seed commit, merges each wave in the order listed, runs the full test matrix,
  commits the wave, and runs the measurement gate before starting the next wave.
- Prompt for each agent: this file's "Rules" section, its package section, `AGENTS.md`, and ROUND2.md's section for
  its items. Diagnosis packages also get the gate results they depend on.

---

## What the code says (shapes the plan)

Read from 0.7.1 (`runtime.rs`, `scheduler.rs`, `driver/uring/mod.rs`, `task/harness.rs`) and `sut-monoio`:

1. **A round is**: pop up to `task_budget` tasks from a FIFO `VecDeque` and run each once, poll the main future if
   woken, then either `driver.submit()` (enter + `tick()`, tasks still queued) or `park()` (queue empty). Every CQE
   reaped in `tick()` pushes its task to the *back* of the queue. A task's latency is about one queue traversal per
   I/O hop.
2. **An op future is always `Pending` on its first poll** (the SQE was just pushed). So a single task cannot loop
   on I/O within one poll the way it can on epoll. It can only do that on futures whose result is already queued:
   multishot `next()`, `BufRing` receives, `event_channel` batches, local channels, TLS with buffered plaintext.
   Tokio-style coop (F1) therefore limits those cases. It does not limit a stream task that does one read and one
   send per poll. The fairness problem in M1/M2/M3 is per round, not per task.
3. **The SUT's accept loop has one `accept_with` SQE in flight.** The next accept is pushed only when the loop task
   runs again, which is once per round. At 1 MiB × 4096, a round contains ~400 stream tasks, so the accept rate is
   bounded by the round rate. Tokio accepts until `EWOULDBLOCK` per wakeup. The listen backlog is large
   (`somaxconn` 65535), so connections complete the handshake, sit in the queue, and the client's 10 s timer runs.
   This is M1 hypothesis 1, made concrete. `acceptmulti` should remove it, because queued multishot CQEs make
   `next()` ready without a round trip. It was never run on the 1 MiB cell.
4. **Foreign wakers (`sync`) are drained only in `park()`**, never in `submit()`. With a fixed `task_budget` and a
   queue that never empties, they would starve. The SUT does not enable `sync`, so this does **not** explain M2. It
   is still a bug, fixed in W1-sched.
5. **M2 candidate**: the proxy path is 4–5 hops (client read → pool lease hand-over via `Waker::wake` → upstream
   write → upstream read → client write). Each hop goes to the back of the queue. With `budget16` every 16 tasks
   cost an `io_uring_enter`, so a traversal of a ~400-task queue costs ~25 enters, and the hop latency multiplies.
   json is one hop, so it gains from the extra reaping. A LIFO slot for a task woken by the running task (tokio's
   design) targets exactly the hand-over hop.
6. Today the counters run unconditionally: about 20 `inner.stats.* += 1` sites in `driver/uring/mod.rs` (per enter,
   per tick, per op submitted and dropped), `cq.overflow()` read on every tick, and `Cell` counters and high-water
   `max()` calls in `buf_ring.rs`, `fixed_buf.rs` and `event_channel.rs`.

---

## Metrics design (W0-metrics implements it)

**Feature:** `metrics = []` in `monoio/Cargo.toml`, not in `default`. The examples crate forwards it.

**Macro:** `monoio/src/metrics.rs` (seed commit):

```rust
/// Runs the statement only with the `metrics` feature; expands to nothing otherwise.
macro_rules! metric {
    ($($body:tt)*) => {
        #[cfg(feature = "metrics")]
        { $($body)* }
    };
}
```

Use: `metric!(inner.stats.enter_wait += 1);`. Fields that only metrics read get `#[cfg(feature = "metrics")]`, so
without the feature they do not exist (no memory, no stores).

**What is gated and what is not:**

| thing | without `metrics` | with `metrics` |
|---|---|---|
| `DriverStats`, `IoUringDriver::{stats, reset_stats}`, `monoio::{driver_stats, reset_driver_stats}` | not compiled (a call is a compile error, never silent zeros) | as in 0.7.1 |
| `UringInner::stats` field and all its updates | absent | present |
| `cq_overflow` | n/a | read from the kernel in `stats()`, no longer on every tick |
| new `RuntimeStats`, `monoio::{runtime_stats, reset_runtime_stats}` (F2) | not compiled | present |
| pool counters: `BufRingStats::{enobufs, high_water, taken, recycled}`, `FixedBufStats::{high_water, exhausted}`, `EventChannelStats::{eventfd_wakeups, ring_wakeups, full}`, `FixedFileStats::high_water` | fields absent | present |
| pool state: `in_use`, `capacity`, `kernel_capacity` | present: it is state the pool keeps anyway, not instrumentation | present |
| `uring::fixed_file_stats()`, `BufRing::stats()`, `FixedBufPool::stats()`, `EventReceiver::stats()` | present, state fields only | all fields |

- The pool stats structs become `#[non_exhaustive]`, so a gated field can't break construction.
- Docs for every gated item carry `#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]`.
- The `debug` (tracing) feature is unchanged and independent.
- This is a breaking change (`driver_stats()` needs the feature), so the release is **0.8.0**. RELEASES.md tells
  users of `driver_stats()` to add `features = ["metrics"]`.

**Benchmark consequence (our side, in `../gatotkaca-transport`):** `sut-monoio` gets a `metrics` feature forwarding
`monoio/metrics`, and the runner builds it only for diagnosis runs. **Performance verdicts are always made on builds
without `metrics`.** The tokio side gets the same split (`RuntimeMetrics` behind `tokio_unstable`/a feature), so both
SUTs pay the same.

**Acceptance (gate G1):**
- `cargo build -p adaptif-gatotkaca-monoio` (default features) has no reference to the stats fields:
  `cargo expand`/review, plus `size_of::<UringInner>()` drops by at least `size_of::<DriverStats>()` (a test
  under `#[cfg(not(feature = "metrics"))]` asserts it via a `pub(crate)` const).
- 0.8.0 without `metrics` vs 0.7.1, on `--cells 64:4096` and `--cells 1024:2 --workloads stream,repl`, 5 rounds:
  payload per CPU-second no worse by the 8% rule (expected: equal or slightly better).
- With `metrics`, every existing `driver_stats.rs`, `buf_ring.rs`, `fixed_buf.rs`, `event_channel*.rs` assertion
  still passes (those tests get `#![cfg(feature = "metrics")]` or gated assertions).

---

## Seed commit (lead, before wave 0)

- `monoio/Cargo.toml`: `metrics = []`; `examples/Cargo.toml`: forward it.
- `monoio/src/metrics.rs` with `metric!`; `#[macro_use] mod metrics;` in `lib.rs` before `mod driver`.
- `docs/performance-improvement/round2/` with an empty `README.md`.
- Tag `r2-base`.

## Wave 0 (parallel): instrumentation and reproducer

| Package | Items | Owns |
|---|---|---|
| W0-metrics | gate today's counters | `driver/uring/mod.rs`, `driver/mod.rs` (stats fns), `driver/buf_ring.rs`, `driver/fixed_buf.rs`, `driver/fixed_files.rs`, `driver/msg_ring/event_channel.rs`, `lib.rs` exports, tests `driver_stats.rs`, `buf_ring.rs`, `fixed_buf.rs`, `fixed_files.rs`, `event_channel*.rs`, `ignored_inline.rs`, `zc_send.rs`, `send_zc.rs`, `cancel_fd.rs`, `deadline.rs`, `multishot_*.rs`, `reexports.rs` (whichever read stats) |
| W0-rtstats | F2 | `runtime.rs`, `scheduler.rs`, `task/` (spawn/complete counters), new `runtime_stats.rs`, new test `runtime_stats.rs` |
| W0-repro | M2's reproducer (and M1/M3 shape) | new `examples/fairness.rs`, `examples/Cargo.toml` entry |

**W0-metrics**
- Wrap every update in `metric!`. Move the `cq_overflow` read into `stats()`. Gate the API as in the table.
- The ops-by-opcode index computation goes inside the macro too.
- Done when: both feature sets build and test, and there is no `stats` reference outside `metric!`/`cfg` in the
  owned files (`grep -n 'stats\.' | grep -v metric!` is empty apart from the gated `stats()` bodies).

**W0-rtstats (F2)**
- `RuntimeStats` (`Copy`, `#[non_exhaustive]`), thread-local in `Context` under `cfg(metrics)`:
  - `rounds` (inner-loop iterations), `ticks_submit` (cold-path `submit()`), `parks`, `park_ns` and `busy_ns` (one
    `Instant::now()` before and after `park()`; metrics only);
  - `tasks_polled`, `polls_per_round` buckets (0, 1, 2–3, 4–7, … 512+), `budget_exhausted_rounds` (round ended by
    the budget with tasks left);
  - `ready_queue_len` (sampled at round start) and `ready_queue_high_water` (updated in `TaskQueue::push`);
  - `tasks_spawned`, `tasks_completed`, `remote_wakes` (foreign wakers drained, `sync` only);
  - reserved for wave 1: `coop_yields`, `lifo_polls` (fields exist, stay 0 until W1-sched fills them).
- `monoio::runtime_stats()` / `reset_runtime_stats()` (panic outside a runtime, like `driver_stats()`).
- Done when: a test spawns N tasks, yields, and checks `tasks_spawned`, `tasks_completed`, `rounds > 0`,
  high water ≥ N.

**W0-repro (`examples/fairness.rs`)**
- One runtime pinned to a CPU (`--cpu`), loopback, all in one process. A second thread with its own runtime plays
  the clients, so the measured runtime only serves.
  - `--bulk N --chunk 128k`: N connections doing an unbounded copy (the server reads/sends `--chunk` units, like
    `/stream/1024`).
  - `--rr M --rate R`: M request/response connections at R req/s in total, open loop, 10 s client timeout.
  - `--hop`: each rr request is answered via a second server task woken through a local FIFO of wakers (models the
    proxy pool hand-over). Without `--hop` it answers directly (models json).
  - `--accept-ramp K`: open K new rr connections during the run (models the ramp), with `--accept single|multi`.
  - Builder flags: `--budget n`, and later `--coop n`, `--lifo` (added by W1-sched as the options appear).
- Output: one line of rr p50/p99/max, timeouts, bulk MB/s and accepted connections per second. With
  `--features metrics`, the `driver_stats()` and `runtime_stats()` deltas too.
- Done when: it runs with `--bulk 400 --rr 200 --rate 2000` and prints the line. Numbers are screening only.

### Gate G0 (lead, quiet machine): diagnose M1/M2 before writing fixes

Needs W0 merged, and `sut-monoio` built against the fork with `metrics` for diagnosis.
1. **M1 accept hypothesis** on `--cells 1024:4096`, 1 round, diagnosis build:
   - sample once a second: `ss -lnt sport = :<port>` (listener Recv-Q = queued handshakes),
     `nstat -az TcpExtListenOverflows TcpExtListenDrops`, `driver_stats().ops_by_opcode[Accept]`, `runtime_stats()`
     queue length and rounds/s;
   - compare the time of the client timeouts with the Recv-Q curve;
   - repeat with `monoio:acceptmulti` (perf build, 3 rounds).
   - Verdict: if Recv-Q climbs while the timeouts happen and `acceptmulti` gives 0 timeouts → hypothesis 1 holds;
     W1-net's runtime part is then "make the fast accept the obvious one", and W1-sched supplies priority/exemption.
     If not → look at the time from accept to the first read completion (hypothesis 2), using the repro with
     `--accept-ramp`.
2. **M2** with the repro: `--bulk 400 --rr 200 --hop`, with and without `--budget 16`, plus the SUT on
   `--cells 64:4096` with `budget16`, diagnosis build. Record queue length, rounds/s, enters per round. Check
   finding 5 (hop × traversal) against the measured hop latency.
3. **Baseline**: 0.7.1 vs the merged tree without `metrics` (G1's first comparison), so later gates have a
   same-version baseline.

The lead writes `round2/G0.md`, which goes into the prompts of wave 1.

---

## Wave 1 (parallel): scheduler, accept, buffers, files, thread-per-core

| Package | Items | Owns |
|---|---|---|
| W1-sched | F1, M2 fix, M3 (scheduler side), finding 4 | `runtime.rs`, `scheduler.rs`, `task/` (incl. new `task/coop.rs`), `builder.rs`, `driver/op.rs`, `driver/chain.rs`, `io/futures.rs`, `driver/raw.rs` (`MultiOp` poll only), `Driver::submit` in `driver/uring/mod.rs`, `examples/fairness.rs` (flags only) |
| W1-net | M1 (runtime side), F3 | `net/` (listener, `multishot.rs`, unix), `driver/op/accept.rs`, `driver/op/accept_multi.rs`, `driver/op/recv_multi.rs` |
| W1-bufzc | M4 | `driver/fixed_buf.rs`, `driver/op/fixed.rs`, `driver/op/send_zc.rs`, `driver/uring/lifecycle.rs` (zc notif path only) |
| W1-file | F5, M6 | `fs/`, `driver/op/open.rs`, `driver/op/close.rs`, `driver/op/splice.rs`, `driver/op/fadvise.rs` |
| W1-tpc | F4 | new `utils/thread_per_core.rs`, new `examples/thread_per_core.rs`; one export line in `lib.rs`/`utils/mod.rs` (lead merges) |

`driver/uring/mod.rs` is touched by W1-sched (`submit`) and W1-bufzc (notif accounting, if any). They edit different
functions. The lead merges W1-sched first.

**W1-sched**
1. `monoio::task::yield_now()`: a future that is `Pending` once and wakes itself, public and documented. Same
   semantics as the SUT's `yield_once`, so the SUT can switch.
2. **Coop budget (opt-in)**: `RuntimeBuilder::coop_budget(Option<u32>)`, default `None`.
   - `task/coop.rs`: thread-local `Cell<u32>` budget plus an "enabled" flag. The runtime loop sets the budget to `n`
     before each `t.run()` and before polling the main future.
   - The check is in `Op::poll` (covers `RawOp`, typed ops), `MultiOp::next`, `ChainOp`, and the named futures: when
     the result is ready and the budget is 0, the future calls `wake_by_ref()` and returns `Pending`. The
     completion stays in the op slot, so nothing is lost. A ready result consumes 1.
   - `task::consume_budget().await` for CPU loops; `task::unconstrained(fut)` runs `fut` with no budget.
   - Cost when off: one thread-local load and a branch per ready poll. G1 checks that this costs nothing measurable.
     If it does, the lead moves the check behind a `coop` cargo feature.
   - Count `coop_yields` (metrics only).
3. **Fairness fixes for M2/M3.** Pick by G0, and keep each one opt-in until G2 decides:
   - `RuntimeBuilder::lifo_slot(true)`: a task woken while another task runs goes to a one-task slot polled right
     after the current one; at most 3 consecutive slot polls, then the slot task goes to the back of the queue
     (tokio's rule, avoids ping-pong starvation). Count `lifo_polls`.
   - Make `task_budget(n)` reap without entering: after `n` tasks, if the CQ has entries (a memory read, no
     syscall) run `tick()`. Only enter when there are SQEs to submit or when `n` × k tasks have passed. This removes
     the "every 16 tasks an enter" cost, which is the suspected M2 mechanism.
   - Finding 4: drain the foreign-waker queue in `submit()` too (`sync` only), so a never-empty queue can't starve
     cross-thread wakes.
4. `fairness` example: `--coop n`, `--lifo`, `--reap-without-enter` flags.
- Done when: tests cover yield order; budget exhaustion returns `Pending` and the op result is delivered on the next
  poll; `unconstrained` ignores the budget; LIFO cap; and a `sync` test where a foreign wake is delivered while the
  queue never empties.

**W1-net**
1. M1: whatever G0 shows. Expected: document `accept_multi` as the accept loop for servers, and add
   `TcpListener::accept_batch(max, flags)` (multishot under the hood, or `accept4` until `EAGAIN` on a
   non-blocking listener), so the loop accepts everything queued per poll. Test: 1000 connects queued before the
   accept loop starts are all accepted within ≤ 2 polls of the loop task.
2. F3: `RecvStream::read_into(&mut BytesMut) -> io::Result<usize>` (copy, return the buffer to the ring at once).
   Also an `AsyncReadRent` adapter `MultiRecvReader` that keeps a leftover `SelectedBuf` between reads. Documented
   costs: one copy; on ring exhaustion the stream re-arms after `ENOBUFS` once buffers are freed, and `read_into`
   does this itself. Same for `UnixStream`. Tests: framed reads across buffer boundaries, exhaustion and re-arm,
   EOF.

**W1-bufzc (M4)**
- First diagnose with the repro (`--fixedbuf`, `--zc` flags added in this package's own copy, given to the lead as a
  requested change) or the SUT's `--tune fixedbuf|zc` with a metrics build. Look at: `FixedBufStats::exhausted`
  (does the server wait or fall back when the 512-buffer pool is empty at 4096 connections?), `zc_notifs` vs
  `zc_sends`, CQEs per tick, round length during the ramp.
- Then fix whatever stalls the loop. If the cause is pool exhaustion in the SUT, the fix is SUT-side (pool sizing
  or fallback) and goes into the notes. Runtime fixes might be: a `get_or_wait` that doesn't spin, or batching
  notification handling.
- Done when: the root cause is written up with stats, and a test reproduces the stall if it is in the runtime.

**W1-file (F5, M6)**
1. `OpenOptions::open_direct(path)` → a direct-only `File` in a fixed slot (`IORING_OP_OPENAT` with `file_index`,
   needs `with_fixed_files`). `close()` is then a slot update (`IORING_OP_CLOSE` with `file_index`), and no fd is
   created. `read_at`/`read_fixed_at` work on it.
2. Verify on this kernel (7.0, f2fs) with the `io_uring:io_uring_queue_async_work` tracepoint
   (`perf stat -e io_uring:io_uring_queue_async_work`) which of open/read/close punt, for normal and direct files,
   warm and cold cache. Write the table into AGENTS.md.
3. Punt visibility (metrics only): per-opcode completion latency in ticks, i.e. the tick number stored in the op
   slot at submit, compared at completion, in buckets (same tick, 1, 2–3, 4+). No clock reads. A punted op shows up
   as ≥ 1 tick. Field `op_ticks_by_opcode` in `DriverStats`. (This touches `driver/uring/lifecycle.rs`/`mod.rs`:
   implement in `lifecycle.rs`, and request the submit-side hook from the lead.)
4. M6: confirm in the kernel source that file→pipe `SPLICE` always goes async in io_uring (no nowait path). If so,
   document that `splice` is for large cold files, and that cached files should use `read_fixed_at` + `send_fixed`
   or `read_at_then_send`. If there is an inline path (e.g. `SPLICE_F_NONBLOCK` on a cached page), add it as an
   option.
5. `fadvise`: document when `Sequential`/`WillNeed` helps 128 KiB reads (measured in G2, cold cache once the SUT
   has a drop-caches step).

**W1-tpc (F4)**
- `monoio::utils::ThreadPerCore`: `ThreadPerCore::new(cpus: &[usize]).builder(|| RuntimeBuilder<IoUringDriver>)`
  `.run(|core: CoreInfo| async move { .. }) -> TpcHandle` with `join() -> Vec<thread::Result<T>>`.
  - Each thread pins itself (`bind_to_cpu_set`), builds its runtime, and runs the closure.
  - Optional `.attach_wq(true)` shares one io-wq (the first runtime is the primary and lives until all others end).
  - `CoreInfo` has the index, the CPU and a `RingHandle` list of all runtimes, for MSG_RING hand-off.
- `examples/thread_per_core.rs`: per-core `SO_REUSEPORT` listeners (`ListenerOpts::reuse_port`), echo.
- Done when: a test starts 2 runtimes on 2 CPUs (skips with fewer), each accepts on a shared `SO_REUSEPORT` port,
  and join returns both results.

### Gate G2 (lead, quiet machine): fixes vs ROUND2.md acceptance

Bump the SUT's `[patch.crates-io]` to the wave-1 tree (perf build, no `metrics`), wire the new options into
`--tune` (`coop<N>`, `lifo`, `reapnoenter`, `acceptbatch`, `recvmulti`, `opendirect`), then:

| item | cell / command | rounds | passes when |
|---|---|---|---|
| M1 | `--cells 1024:4096 --variants tokio,monoio,monoio:<fix>` | 5 | 0 client timeouts; json/proxy ≥ 95%; stream req/s within 8% of 2,163–2,284 or > tokio |
| M2 | `--cells 64:4096 --variants monoio,monoio:budget16,monoio:budget61` (+ fix) | 3 | budget61 proxy ≥ 95%, p99 not worse than default by the 8% rule; json keeps its gain |
| F1 | M1 cell with `coop<N>`, no `--yield-every`; encrypted mix `--cells 1024:2 --workloads stream,repl --repl-conns 8 --monoio-stream-mode encrypted` vs `xyield-every=1` | 5 | M1 passes; repl p99 within 8% |
| M3 | `--cells 64:4096`, tokio vs monoio + the M1/M2 fix | 5 | proxy p99 ≤ 20 ms; json p99 within 8% of tokio; payload/CPU-s loss ≤ 8% |
| M4 | `--cells 64:4096 --variants monoio:fixedbuf,monoio:zc` | 3 | 0 client timeouts |
| F3 | M5 cell with `recvmulti` | 5 | measured (no target; feeds M5) |

The lead writes `round2/G2.md` and a run doc per command in the bench repo (`docs/run/runN.md`, version and
`[patch.crates-io]` noted). A fix that fails its gate goes back to its package for one more iteration in wave 2.

---

## Wave 2 (parallel): driver efficiency, stalls, SQPOLL check, leftovers

| Package | Items | Owns |
|---|---|---|
| W2-enter | M5 | `driver/uring/mod.rs` (enter/submit/tick), `driver/doorbell.rs` |
| W2-stall | M7 | measurement and notes first; code only in files the lead assigns from findings (likely `driver/buf_ring.rs`, `driver/uring/lifecycle.rs`) |
| W2-sqpoll | M8 | `builder.rs` (new methods only), new `builder/sqpoll_check.rs` or a private fn in `builder.rs` |
| W2-retry | wave-1 fixes that failed G2 | the files of the original package |

**W2-enter (M5)**
- Measure on the M5 cell (2000 connections, 4 KiB and 16 KiB `POST /repl`), with a metrics build for counts and a
  perf build plus `perf stat -e syscalls:sys_enter_io_uring_enter` for the clean number: enters per frame, SQEs per
  enter, CQEs per tick, vs tokio's `epoll_wait` + `read`/`write` per frame (`strace -c -f` on the tokio SUT).
- If monoio enters more per frame: merge the cold-path `submit()` enter with the next park when the queue is short
  (enter at most once per round), and make `skip_empty_enter` cover more cases (e.g. only CQ work pending).
  `min_wait` defaults are not changed; any new behaviour is opt-in.
- Done when: the enters-per-frame table is in the notes, and any change has a test that counts enters (metrics
  build).

**W2-stall (M7)**
- Repeat the A/A cell (`aa-w1`: encrypted 1 MiB × 2 + closed-loop repl, 5 rounds × 2 arms) with a metrics build and
  per-round `driver_stats()`/`runtime_stats()` (the runner records them per round, "our side" item 1).
- Correlate the slow rounds with `cq_overflow`, `cancels_lost`, `BufRing` `enobufs`, `polls_per_round` tail, and
  `park_ns`. If none moves, check `perf sched`/`runqlat` on the SUT core (harness or OS noise).
- Fix in the runtime only if a runtime counter explains the stall.

**W2-sqpoll (M8)**
- `RuntimeBuilder::sqpoll(idle: Duration, cpu: Option<u32>)` records the setting (the opaque
  `uring_builder(..).setup_sqpoll(..)` keeps working, but can't be checked).
- At `build()`: read the thread's affinity (`sched_getaffinity`). Conflict if `cpu` is in a one-CPU mask, or if the
  mask has one CPU and no `cpu` is given (check on this kernel where an unpinned SQ thread runs, and document it).
- Default: one `eprintln!` warning per process. `.deny_sqpoll_conflict()`: `build()` fails with `InvalidInput`.
- Done when: tests cover the conflict and the no-conflict case (skip without SQPOLL privileges).

### Gate G3 (lead, quiet machine)

- M5: frames per CPU-second within 8% of tokio on both frame sizes (5 rounds each).
- M7: A/A spread of monoio payload ≤ 8% (5 rounds × 2 arms), or a documented harness cause.
- M8: `monoio:sqpoll` on the server's core fails to build with the deny flag and warns without it.
- F4: a 2-core smoke run of the SUT on `ThreadPerCore` (no verdict; the multi-core stage is a separate study).
- Re-run G1 (metrics off vs 0.7.1) on the final tree: the hot path must still be at least as fast as 0.7.1.

---

## Dependencies at a glance

```
seed ──► W0-metrics ─┐
     ├─► W0-rtstats ─┼─► merge ──► G0 (diagnose M1/M2, baseline) ──┐
     └─► W0-repro ───┘                                             │
                                                                    ▼
        W1-sched · W1-net · W1-bufzc · W1-file · W1-tpc  (parallel, worktrees)
                                                                    │
                                              merge ──► G1 + G2 ────┤
                                                                    ▼
                    W2-enter · W2-stall · W2-sqpoll · W2-retry  (parallel)
                                                                    │
                                              merge ──► G3 ──► release 0.8.0
```

Five agents at most run at once (wave 1). Gates are serial, with no agent active. Wave 1 packages do not depend on
each other. W1-file's punt-latency hook and W1-bufzc's notif accounting are the only edits in `driver/uring/mod.rs`
besides W1-sched's, and the lead applies them at merge.

## Commits and release

- One commit per package, merged onto `main` in the order of the wave tables. One wave commit is fine if the lead
  squashes. Messages list the item ids.
- After G3:
  - `AGENTS.md`: the `metrics` feature (table and the "verdicts without metrics" rule), `yield_now`/coop/
    `unconstrained`, `lifo_slot`, the new `task_budget` reaping, `runtime_stats()`, `accept_batch`, `read_into`,
    `open_direct`, the punt table, `ThreadPerCore`, `sqpoll`.
  - `docs/RELEASES.md`: 0.8.0 with the breaking change (stats behind `metrics`, `non_exhaustive` stats structs).
  - Bump `adaptif-gatotkaca-monoio` (and `-macros` if touched) to 0.8.0, in its own commit.
  - Update ROUND2.md item status, and hand the run docs back as ROUND2.md "Handing results back" asks.

## Status

| Wave / gate | State |
|---|---|
| seed | done (`a4de6cf`, tag `r2-base`) |
| 0 | done (tag `r2-wave0`): W0-metrics, W0-rtstats, W0-repro; fixups: `remote_wakes` hook, runtime stats in `fairness` |
| G0 | done ([round2/G0.md](round2/G0.md)): M1 = one accept per round (multishot accept fixes it); M2 = intra-round wakes wait a full traversal under a fixed budget; M4 = same cause as M1, W1-bufzc dropped; baseline: no regression vs 0.7.1 |
| 1 | done (tag `r2-wave1`): W1-sched, W1-net, W1-file, W1-tpc; W1-bufzc dropped (G0); fixup: W1-file lifecycle hook, applied by hand |
| G1 + G2 | done (G1 first half in [G0.md](round2/G0.md), [round2/G2.md](round2/G2.md)): M1, M2 (with `lifo_slot`), F1, M4 pass; M3 fails on proxy p99 (~40 ms vs ≤ 20) → W2-retry; F3 moved to wave 2 with M5 |
| 2 | not started |
| G3 | not started |
