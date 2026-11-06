# monoio fork: round-2 asks (after the tuning program)

Written 2026-11-06 for the maintainer of `adaptif-gatotkaca-monoio` (fork at `../monoio-gatotkaca`). Round 1 is
`monoio-fork.md` (F1–F7, A1–A14); 0.6.0/0.7.x delivered most of it, and 0.7.1 fixed the timer cold path. This round
is based only on what the benchmark measured with 0.7.1. Read 0.7.1's source to check what already exists.

**Scope and caveats:**
- One machine, one pinned core for the server, loopback, warm page cache, `performance` governor.
- Verdicts use the 8% rule (AGENTS.md):
  - medians ≥ 8% apart (larger median as base);
  - ≥ 4/5 rounds in the same direction;
  - disjoint p99 ranges.
- Anything marked **hypothesis** was not tested.

## Summary

Round-2 results: [ROUND2-PLAN.md](ROUND2-PLAN.md), per-gate notes in [round2/](round2/) (final gate:
[round2/G3.md](round2/G3.md)), shipped in 0.8.0.

| id | kind | priority | one line | round 2 status |
|---|---|---|---|---|
| M1 | fix | **P0** | ~6,200 small requests per run time out (10 s) behind 1 MiB streams at 4096 connections; tokio has none | done: `accept_batch_into` (G2, G3) |
| M2 | fix | **P0** | `task_budget(n)` collapses the proxy path to 28–44% of target with ~11 s median latency | done: pair `task_budget` with `lifo_slot(true)` (G2) |
| F1 | feature | **P0** | per-task cooperative budget (tokio-style coop) plus a public `yield_now()` | done: `coop_budget`, `yield_now`, `consume_budget`, `unconstrained` (G2) |
| M3 | fix | P1 | small-request p99 under bulk load is 1.5–2× tokio's (json 13.4 vs 8.7 ms, proxy 42.4 vs 21.5 ms) | done: `small_io_priority` (G3: proxy p99 13.5 ms) |
| F2 | feature | P1 | runtime (scheduler) metrics next to `driver_stats()`: ready-queue depth, polls, ticks, busy time | done: `runtime_stats()` (W0-rtstats) |
| M4 | fix | P1 | `fixedbuf` and `zc` cause client timeouts while 4096 connections open | done: same cause as M1, no change (G0, G2) |
| M5 | investigate | P1 | many connections, small frames: 8–11% fewer frames per CPU-second than tokio | done: within 8% (G3: −7.9% / −7.4%); rest is kernel per-request cost |
| M6 | investigate | P2 | `splice` stream: 12% less payload per CPU-second than tokio `sendfile`, p99 429 vs 11 ms, 16 threads | answered: file splice always punts (W1-file) |
| M7 | investigate | P2 | 1–2 of 5 rounds with repl p99 about 4 ms (median 0.6 ms), frequency normal | closed: not reproduced (W2-lead, G3) |
| M8 | fix | P2 | SQPOLL on the loop's own CPU: −85% and timeouts; warn or refuse | done: `sqpoll(idle, cpu)` warns, `deny_sqpoll_conflict()` (G3) |
| F3 | feature | P2 | a `recv_multi` adapter that fills a `BytesMut`, so a framed read loop can use multishot receive | done: `read_into`, `into_reader`; measured, no gain on M5 (G3) |
| F4 | feature | P2 | thread-per-core helper (N pinned runtimes, per-core `SO_REUSEPORT` listeners) | done: `utils::ThreadPerCore` (G3 smoke) |
| F5 | feature | P2 | file path without io-wq punts (CLOSE, splice) for cold-cache and per-request opens | done: `open_direct`, punt table (W1-file) |

**If only one thing gets done:** M1 together with F1. Together they decide whether monoio can serve large files and
many small requests on one core. M3 is probably the same root cause.

---

## How to reproduce (all items)

From this repo's root:

```sh
cargo build --release
./target/release/bench-runner run --discard-first --require-governor-performance \
  --warmup 5 --cooldown 5 --rounds <R> --duration 15 <workload flags> --variants <list> --out results/<name>
```

- Each run writes `docs/run/run{N}.md` and `results/<name>/runs.csv`.
- To test a local fork build, point the workspace `monoio` dependency at the fork with `[patch.crates-io]` and record
  it in the run doc.
- Variant syntax:
  - `monoio:<tune item>`, e.g. `monoio:budget16`, `monoio:fixedbuf`, `monoio:zc`, `monoio:sqpoll`;
  - `monoio:xyield-every=N`;
  - `monoio:chunk32k`.
- Full list: `docs/tuning/KNOBS.md`. Analysis: `scripts/tuning_analyze.py --base <variant> results/<name>/runs.csv`.

---

## M1 (P0): small requests time out behind 1 MiB streams

**Evidence:**
- Data: `results/tuning/s3-w2h`, run doc `docs/run/run87.md`; 5 rounds, all monoio rounds affected.
- Workload: the default web mix on cell 1 MiB × 4096 connections. 10% of the connections run unbounded `GET /stream/1024`.
  45% send `GET /json` and 45% `POST /echo` (proxied to the upstream), at 1000 req/s in total each.

| per round (median of 5) | tokio | monoio |
|---|---|---|
| json: achieved % of target | 95.0% | 68–72% |
| json: client timeouts (10 s) | 0 | 5,966–6,435 |
| json p50 / p99 | 4.8 ms / 12–15 ms | 24–28 ms / 21.4–21.7 s |
| proxy: achieved % of target | 95.0% | 67–71% |
| proxy: client timeouts | 0 | 5,975–6,451 |
| proxy p50 / p99 | 8.1–8.5 ms / 24–37 ms | 132–145 ms / 21.4–21.6 s |
| stream requests/s | 1,533–1,587 | 2,163–2,284 |
| stream p99 | 664–706 ms | 203–230 ms |
| server RSS / threads | 196–199 MiB / 2 | 127–129 MiB / 2 |

**It is not new and not the timer bug:**
- Same counts on 0.7.0 without the timer fix (`results/tuning/diag-w2h-070`, run 89: 6,272 / 6,289 per run).
- Already present on 0.5.0: run 67 had 8,283–8,491 per run with a 30 s window.
- The 64 KiB cell (`results/tuning/s3-w2`) has **no** timeouts.

**What the numbers suggest:**
- Monoio favours the stream: +43% stream requests/s and a 3× lower stream p99 than tokio.
- The *median* small request is only moderately slow (json p50 26 ms), but a near-constant ~6,200 per run wait more than
  10 s. That looks like a **group of connections that get no service for over 10 s**, not a uniform slowdown.

**Hypotheses (untested), in the order I would test them:**
1. **Accept starvation during the connection ramp.** The accept loop is one task among ~400 stream tasks. Each stream
   task reads and sends 1 MiB in 128 KiB units and is ready again as soon as its completion arrives. New connections
   wait in the kernel's accept queue; the client counts the wait against its 10 s timeout.
   - The 64 KiB cell would be spared because each stream request is 16× shorter.
   - Test: count accepted connections per second in the server (or read `ops_by_opcode[Accept]` from `driver_stats()`
     once a second), and compare with the time of the client timeouts.
   - Also run `monoio:acceptmulti` on this cell; it was only screened on 64 KiB.
2. **First-request starvation after accept.** The connection is accepted but its first read completes late, because
   the ready queue is always full of stream tasks.
3. **Waiter-queue starvation on the proxy pool.** This does not explain json, so it is secondary here (see M2).

**What I want:**
- A diagnosis: which of the three, or something else.
- A fix in the runtime (see F1), or a documented pattern that the server can apply. Today's server already yields
  after every sealed chunk on the encrypted path, but not on the buffered path.

**Acceptance:** on `--cells 1024:4096`, 5 rounds, default variants:
- monoio has **0 client timeouts**, and json/proxy reach ≥ 95% of target;
- stream throughput stays within 8% of today's monoio (2,163–2,284 req/s) or better than tokio's.

```sh
./target/release/bench-runner run --discard-first --require-governor-performance --warmup 5 --cooldown 5 \
  --rounds 5 --duration 15 --cells 1024:4096 --variants tokio,monoio --out results/m1-repro
```

---

## M2 (P0): `task_budget(n)` collapses the proxy path

**Evidence:**
- Data: `results/tuning/s1-w2`, run 80; 1 round, 10 s, screening level.
- Cell: 64 KiB × 4096, 0.7.1 with the timer fix.

| variant | json % / p50 / p99 | proxy % / p50 / p99 | client timeouts |
|---|---|---|---|---|
| monoio (default) | 93.4% / 6.5 ms / 11.7 ms | 93.3% / 12.3 ms / 34.8 ms | 0 |
| `budget16` | 93.4% / 4.6 ms / 8.3 ms | **28.0% / 11.7 s / 14.2 s** | 0 |
| `budget61` | 93.4% / 4.6 ms / 9.2 ms | **34.5% / 10.4 s / 13.0 s** | 0 |
| `budget256` | 93.4% / 5.0 ms / 10.9 ms | **43.9% / 8.8 s / 12.6 s** | 0 |

**Observations:**
- The budget **helps** the plain small request (json p99 down 7–30%) and **destroys** the proxied one.
- The proxy path differs from json in three ways:
  - each request waits for a second socket (an upstream connection from a 64-connection pool);
  - pool leases are handed over through a local FIFO of waiters, woken by `Waker::wake` (`common::pool`);
  - the upstream is a separate process on its own core.
- Run 72 (0.7.0, before the timer fix) showed the same shape, so this is **not** the cold-path timer bug.

**What I checked in 0.7.1 (`runtime.rs`, `driver/uring/mod.rs`, `scheduler.rs`):**
- `task_budget(n)` runs exactly `n` tasks, then calls `driver.submit()`, which enters the kernel if needed and runs
  `tick()`, so completions *are* reaped on the cold path.
- The ready queue is FIFO.
- So "completions never reaped" and "LIFO starvation" are ruled out by reading.

**Hypotheses (untested):**
1. Woken proxy tasks are pushed behind a queue that refills faster than `n` per round, because every stream completion
   re-queues a stream task. An upstream completion then waits many rounds before its task runs. The default budget
   (≈ 2 × queue length) drains the queue more often.
2. The `should_poll()` main-future check, or waker delivery of tasks woken from inside other tasks (lease hand-over),
   interacts badly with a fixed `n`.

**What I want:**
- A small reproducer in the fork: one runtime, an unbounded bulk-copy task set, and request/response pairs that need a
  wake-up from another task. Measure their latency with and without `task_budget`.
- A fix, or a documented limitation.
- A budget that works is the natural counterpart of tokio's `event_interval(61)`, which the tokio server uses. Without
  it monoio has no tuned equivalent (tuning parity).

**Acceptance:** `monoio:budget61` on `--cells 64:4096`, 3 rounds:
- proxy ≥ 95% of target and p99 no worse than the default by the 8% rule;
- json keeps its gain.

```sh
./target/release/bench-runner run --discard-first --require-governor-performance --warmup 5 --cooldown 5 \
  --rounds 3 --duration 15 --cells 64:4096 --variants monoio,monoio:budget16,monoio:budget61 --out results/m2-repro
```

---

## F1 (P0): cooperative per-task budget and a public `yield_now()`

**Today:**
- 0.7.1 has no public `yield_now` and no per-task budget.
- `task_budget(n)` controls how many tasks run between driver checks. It does not stop *one* task that finds its I/O
  always ready from running long; with io_uring every completion makes a task ready again.
- We wrote our own yield (`common::yield_once`: `wake_by_ref` plus one `Pending`) and call it after every stream chunk
  (`--yield-every`).
  - On tokio a yield per chunk fixed the replication tail (runs 37/41).
  - On monoio the encrypted-mix gain (repl p99 0.574 → 0.254 ms, Stage 2) came mostly from the 32 KiB chunk; yield
    alone was not distinguishable.
  - So today the work-unit size is the only lever, and it costs about half the stream throughput.
  - The yield is manual and easy to forget, and the buffered stream path has none (M1).

**What I want:**
1. `monoio::task::yield_now()` (public, documented, cheap).
2. **Opt-in cooperative budgeting**, like tokio's coop:
   - each task gets a budget of N I/O operations per poll; once spent, I/O futures return `Pending` and wake
     themselves;
   - set with `RuntimeBuilder::coop_budget(Some(n))`, off by default so 0.7.x behaviour is unchanged;
   - plus `monoio::task::consume_budget().await` for CPU-heavy loops (sealing, parsing).
3. A way to exempt a task (`unconstrained(fut)`), for the accept loop or a control task.

**Acceptance:**
- M1's cell passes with `coop_budget` on and no `--yield-every` in the server.
- On the encrypted mix (`--cells 1024:2 --workloads stream,repl --repl-conns 8 --monoio-stream-mode encrypted`), repl
  p99 matches today's `xyield-every=1` within 8%.

---

## M3 (P1): small-request latency under bulk load

**Evidence:**
- Data: `results/tuning/s3-w2`, run 84; 5 rounds, cell 64 KiB × 4096, all runs valid.

| | tokio | monoio | verdict |
|---|---|---|---|---|
| json p99 | 8.65 ms | 13.43 ms | tokio −36%, counts |
| proxy p99 | 21.54 ms | 42.37 ms | tokio −49%, counts |
| payload per CPU-second | 1,129 MB | 1,619 MB | monoio +43%, counts |

**Tried, none fixed it:**
- coop, defer, skipenter, ringfd, ring size 1k/16k, minwait, bufring, acceptmulti, fixedfd, socket buffers, req-buf 64,
  yield-every 1/4, and budget (M2).
- Proxy p99 never got under 20 ms.
- `yield-every 1` lowered json/proxy p99 by 15–18% in 3 rounds; a similar configuration did not reproduce over 5.

**What I want:** treat it as the steady-state form of M1 and check it with the same fix. No separate ask unless M1's
fix leaves it.

**Acceptance:** proxy p99 ≤ 20 ms and json p99 within 8% of tokio's on this cell, without losing more than 8% payload
per CPU-second.

---

## F2 (P1): scheduler metrics next to `driver_stats()`

`DriverStats` (enters, SQEs, CQEs per tick, cancels, ops by opcode, multishot queue high water) is good. I will add it
to the runner first (see "On our side"). It does not show the scheduler.

**What I want:**
- `monoio::runtime_stats()` (thread-local, cheap counters, resettable). Fields:
  - ticks (rounds of the loop), tasks polled, polls per round in buckets;
  - **ready-queue length** (current and high water);
  - times the budget ended a round early, parks, park time (busy vs idle);
  - tasks spawned/completed, and remote wakes received.
- With these, M1 and M2 can be diagnosed from a run's CSV, and the runner can show the same columns for tokio
  (`RuntimeMetrics`).

---

## M4 (P1): registered buffers and zero-copy send time out during the connection ramp

**Evidence** (`results/tuning/s1-w2`, run 80, 1 round, 64 KiB × 4096):

| variant | json client timeouts | proxy client timeouts | json/proxy p99 |
|---|---|---|---|---|
| `fixedbuf` (512 × (head + chunk) registered, READ_FIXED for the stream) | 122 | 124 | 10.6 s |
| `zc` (SEND_ZC for the stream) | 170 | 170 | 10.9 s |
| default | 0 | 0 | 11.7 / 34.8 ms |

The stream itself was fine; the timeouts fall on small requests.

**Hypotheses (untested):**
- a registration or notification path that stalls the loop while thousands of connections open;
- for `zc`, the extra notification CQE per send doubling CQE traffic on the stream connections.

**What I want:** a diagnosis. With F2 and `driver_stats()` the runner can show where the loop time goes. Over loopback
`zc` copies anyway, so I don't expect a throughput gain there; this is only about the stall.

**Acceptance:** both variants with 0 client timeouts on `--cells 64:4096`.

---

## M5 (P1): many connections, small frames

**Evidence:**
- Data: `results/tuning/s3-w3a` / `s3-w3b`, runs 85/86; 5 rounds.
- Load: 2000 connections, open-loop `POST /repl` of 4 KiB / 16 KiB, offered above capacity.

| | tokio | monoio | verdict |
|---|---|---|---|---|
| frames/s (4 KiB) | 107.4k | 98.9k | −7.9%, tie (just under 8%) |
| payload per CPU-second (4 KiB) | 459 MB | 422 MB | tokio +8.2%, counts |
| frames/s (16 KiB) | 79.2k | 71.8k | tokio +9.3%, counts |

- Latency here is queueing delay and not comparable.
- Monoio `coop+skipenter` makes no difference.

**What I want:**
- Per-frame syscall/SQE counts from `driver_stats()` (enters per frame, CQEs per tick) on this cell, compared with
  tokio's `epoll_wait` + `read`/`write` per frame.
- If monoio does more enters per frame, a way to batch: `min_wait` exists, and `skip_empty_enter` is on.

**Acceptance:** frames per CPU-second within 8% of tokio on both frame sizes.

---

## M6 (P2): zero-copy file stream via `splice`

**Evidence** (`results/tuning/s3-w4`, run 88, 5 rounds, 1 MiB files × 64 connections, plaintext, stream only):

| | tokio `sendfile` | monoio `splice` |
|---|---|---|
| payload per CPU-second | 6,997 MB | 6,249 MB (−10.7%, counts) |
| stream p99 | 11 ms | 429 ms |
| peak threads | 2 | 16 (io-wq workers) |
| user / sys CPU (round 1) | 0.54 / 14.49 s | 0.15 / 14.87 s |

**Reading:**
- The file → pipe leg runs on io-wq threads, since file splice is not done inline. Its CPU is charged to the process, and
  the hand-off adds latency.
- `iowq_max_workers(1)` bounds the thread count, but was neutral on throughput.

**What I want:**
- An investigation of the punt: is there a way to keep the file leg inline when the data is in the page cache?
- If the kernel does not allow it, a note in the docs that `splice` is for large cold files, not cached ones.

---

## M7 (P2): occasional slow rounds

**Evidence:**
- Data: `results/tuning/aa-w1`, run 75; A/A calibration, encrypted 1 MiB × 2 streams + closed-loop repl, 5 rounds per
  arm.
- In each monoio arm, 1–2 rounds had repl p99 about 4 ms (ranges 0.511–3.860 and 0.527–4.546 ms) against a median of
  0.59–0.61 ms. Frames/s in those rounds were 21–24k against 26–28k.
- SUT core frequency was normal (4.28–4.29 GHz), at 50–51 °C. Tokio's arms had no such rounds.

**Why it matters:** monoio's payload spread on this mix (9%) is above the 8% threshold, which weakens every monoio
verdict there.

**What I want:** a look at whether anything in the runtime can stall for milliseconds now and then, for example buffer
ring refill, CQ overflow, or a cancel storm. `driver_stats()` per round would show CQ overflow or `cancels_lost`. It may
be our harness: the runner will record `driver_stats()` per round first.

---

## M8 (P2): SQPOLL on the loop's own CPU

**Evidence** (Stage 1, runs 79/80): `monoio:sqpoll` with the SQ thread pinned to the server's core gives −85% payload
on the encrypted mix, and 778/782 json/proxy client timeouts on the web mix.

**Expected.** The poller thread competes with the loop for the one core.

**What I want:** the builder warns (or `build()` fails, behind a flag) when `sqpoll_cpu` equals the CPU the runtime is
bound to, or when the process's CPU mask has one CPU.

---

## F3 (P2): `recv_multi` that a framed read loop can use

**Today:** `TcpStream::recv_multi(&ring)` yields `SelectedBuf`s, and the docs say not to mix them with normal reads.
Our connection loop parses requests out of a per-connection `BytesMut` (httparse, `Content-Length` framing). Using
multishot receive meant a rewrite, so it was not screened (KNOBS.md).

**What I want:**
- An adapter, e.g. `RecvStream::read_into(&mut BytesMut) -> io::Result<usize>`, that copies the selected buffer
  into the caller's buffer and returns it to the ring.
- Or an `AsyncReadRent` implementation over `RecvStream`.
- Documented costs: one copy, and how ring exhaustion behaves.

**Acceptance:** our server can switch with a flag (`--tune recvmulti`). We then measure it on the many-connection cell
(M5), where it should matter most.

---

## F4 (P2): thread-per-core helper

**Today:** `bind_to_cpu_set`, `event_channel` (MSG_RING) and the lock-free remote-waker queue exist. Our monoio server
rejects `--threads > 1`, so there are no multi-core results.

**What I want:** a small helper that starts N runtimes, one per CPU in a set, each pinned, each with its own
`SO_REUSEPORT` listener built by a user closure, and a join handle for all of them. An example in the fork is enough.
The benchmark's multi-core stage would use it (tokio side: `multi_thread` with N workers).

---

## F5 (P2): file path without io-wq punts

**Context:**
- PIP U3: on this machine's f2fs, `CLOSE` of a regular file punts to io-wq.
- The server opens and closes the file on **every** stream request (an invariant), so each request costs a punt.
- Cold-cache reads were never measured (no drop-caches step in `setup-system.sh`).

**What I want:**
- Guidance, or an API, for keeping open/read/close inline: direct (fixed) file descriptors for regular files, so
  close is a table update; and whether `fadvise` (exists) helps readahead for the 128 KiB reads.
- `driver_stats()` cannot see punts (its docs say so). If a cheap counter is possible, for example a CQE flag or
  timing, add it.

---

## Done, and thanks
- **0.7.1 timer cold-path fix** (`TimeDriver::submit` processes due timers). Our 10 s request-timeout check passes on
  0.7.1, idle and under load, also with `budget16`.
- 0.6/0.7: `accept_with` flags, coop/defer through the builder, `skip_empty_enter`, `register_ring_fd`, fixed files,
  multishot accept, `min_wait`, `iowq_max_workers`, `driver_stats()`. All are wired as `--tune` items in our server
  (KNOBS.md).

## On our side (we will do these; no fork work needed)
1. Record `driver_stats()` per run (and per second) in `runs.csv`, with a tokio `RuntimeMetrics` counterpart. This
   comes first, because M1, M2, M4, M5 and M7 all need it.
2. Add per-second accept counts in the server to test M1 hypothesis 1.
3. Confirm or reject `--yield-every 1` on the web mix (5 rounds).
4. Add a drop-caches step to `setup-system.sh` so cold-cache runs become possible (F5).
5. Build monoio thread-per-core in the server once F4 exists.

## Handing results back
For each item, a fork commit plus a run doc in this repo:
- same cell and command as above;
- 5 rounds for P0/P1, compared with today's numbers by the 8% rule;
- `[patch.crates-io]` noted in the run doc.

Runs before and after a monoio version change are not comparable as a baseline; the run doc says which version was
used.
