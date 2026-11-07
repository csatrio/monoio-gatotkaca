# Releases

Releases of `adaptif-gatotkaca-monoio` (library name `monoio`), newest first. The fork starts from upstream [Monoio](https://github.com/bytedance/monoio) 0.2.4 (`8675e94`). Open work is in [TODO.md](TODO.md).

## Performance since the fork (rounds 1–3)

Where the runtime stands against upstream Monoio 0.2.4 after the three performance rounds (0.6.0, 0.8.0, 0.9.0). All numbers come from the gatotkaca-transport harness (one pinned core, loopback, Ryzen 5 4600G, Linux 7.0, tokio 1.54.1 as the reference) and the `reqresp`/`smallframes` reproducers in this crate; the gate files under [performance-improvement/](performance-improvement/) hold the full tables. Two things to keep in mind when reading them: upstream 0.2.4 was never run on the harness itself, but 0.5.0's default path submits the same SQEs as upstream (0.3.0–0.5.0 removed drivers and added opt-in APIs), so the first harness run on 0.5.0 is the upstream figure; and each round was measured against the previous release in the same session, so the per-round deltas describe that release's default path and must not be multiplied across rounds. The "against tokio" column is the one that is comparable across the whole period, since tokio was re-run every time.

### Where it started (0.5.0, default path = upstream)

Mixed web load, 64 KiB streams × 4096 connections next to 8 KiB json and proxy requests (FINDINGS.md §1, smoke-level): upstream monoio moved twice tokio's bulk payload (1,670 vs 850 MB/s, stream p99 23 vs 176 ms) but served only 35% of the small-request rate (tokio 72%) with a p99 of about 3.4 s (tokio 42–57 ms). Everything the small requests needed waited behind the stream tasks: a plain `accept()` loop (thousands of client timeouts per run at 4096 connections), a ready queue traversed in full for every completion, `defer_taskrun` rings that ran completions only in `park()`, timers that stalled while the queue never emptied, and a TLS write path that made 5–8 socket writes per 64 KiB. On replication (request/response frames) no configuration served few and many connections at once, and the per-frame user-space cost was only measured from 0.8.0 on (2,342 instructions at 2000 connections, 3,341 with a `timeout` per frame).

### What each round changed on the default path

| round, release | measured against | throughput | latency | per-op cost |
|---|---|---|---|---|
| **1** (0.6.0): io_uring surface, bug fixes, TLS batching | tests and single-op measurements only (the harness compared variants, not releases) | registered files +5–7% on single file reads; a 64 KiB TLS write is 1 socket write instead of 8 (rustls) / 5 (native-tls) | `defer_taskrun` completions no longer wait for `park()` (F7); submit paths return `EBUSY` instead of panicking | dropped in-flight op data inline up to 48 bytes (no box per cancelled op) |
| **0.7.0 / 0.7.1**: lock-free `event_channel`, remote wakes, timer fix | 0.6.0, same session | 4 producers: 13–15 → 23–44 M msg/s, wakeups per 1M messages ~7,000 → under 20; remote wakes 4.2–4.9 M/s with 4 producers (was 4.0–4.8 through `flume`) | timers fire under saturation (they never did before 0.7.1) | the owner runtime spends 8.6% of its time draining remote wakers, was ~14% |
| **2** (0.8.0): scheduler (`task_budget`, `lifo_slot`, `small_io_priority`, `coop_budget`), batch accept, cheaper reaping | 0.7.1, 5 rounds, [round2/G3.md](performance-improvement/round2/G3.md) | payload per CPU-second +1.8%, encrypted mix +1.8%; many small connections within 8% of tokio (−7.9% at 4 KiB, −7.4% at 16 KiB, was −8.3% / −8.9%) | json p99 −5.7%, proxy p99 −4.2% on the default path; with the recommended scheduler settings proxy p99 41.3 → 13.5 ms and json p99 13.0 → 6.2 ms on 64 KiB × 4096; the ~6,200 client timeouts per run of the `accept()` loop at 4096 connections → 0 | user instructions per frame −5% (CQEs reaped in batches of 32, slots removed without a refcount round trip) |
| **3** (0.9.0): per-op cost, `drain_taskrun`, eager sends, linked send+recv, working-set buffer rings, fd-less sockets, `Deadline` | 0.8.0, 3 rounds, [round3/G3.md](performance-improvement/round3/G3.md) | +1.1% at 8 connections, +1.8% at 2000, +0.7% next to bulk writers, +4.4% on `smallframes` (never below) | ack p99 at 8 connections 135 → 109 µs with eager acks (inside tokio's range); `coop_taskrun` at 2000 connections 1.3 s → 22 ms (batch accept) | user instructions per frame −21% to −27% (2,342 → 1,795 at 2000 connections; 3,341 → 2,433 with a `timeout` per frame; `Sleep` 640 → 72 bytes; op slot one cache line) |

### Where it stands against tokio (0.9.0)

| load | upstream (0.5.0 run, or 0.8.0 `coop:skipenter` where 0.5.0 has no figure) | 0.9.0, one configuration¹ | tokio |
|---|---|---|---|
| mixed web load, 64 KiB × 4096: bulk payload | 2× tokio | +42% payload per CPU-second (round 2 G3; unchanged in round 3) | — |
| mixed web load: json / proxy p99 | ~3.4 s (35% of the target rate) | 6.2 ms / 13.5 ms at the full rate (`task_budget(61)`, `lifo_slot`, `small_io_priority`) | 8.4 ms / 20.6 ms |
| replication, 8 connections × 4/16/64 KiB | 91,940 frames/s (−1.4%), ack p99 138 µs (+27%) | 98,275 frames/s (+6.2%), ack p99 113 µs (inside tokio's 103–120) | 92,557, 103–120 µs |
| replication, 2000 connections × 4 KiB | 104,563 (−7.4%), ack p99 22.6 ms | 113,212 (+4.5%), 18.6 ms | 108,373, 18.8 ms |
| replication next to 2 encrypted 1 MiB streams | 36,948 (−31.9%), ack p99 369 µs | 55,173 (+0.8%), 204 µs | 54,758, 230 µs |
| many small connections, frames per CPU-second (round 2 M5) | −8.3% / −8.9% (4 / 16 KiB) | −7.9% / −7.4% (0.8.0; not re-measured in round 3) | — |

¹ `defer_taskrun`, `skip_empty_enter`, `register_ring_fd`, `drain_taskrun(2)`, batch accept, eager acks, multishot receive on a 72 KiB working-set ring (AGENTS.md, "Recommended configuration for request/response servers"). The replication figures in the second column are 0.8.0 with the ring flags of the time (`coop:skipenter`, FINDINGS.md §17), the closest measured stand-in for the upstream path on that load.

In short: upstream monoio was already the faster runtime for bulk transfers and remains so (+42% payload per CPU-second); the rounds removed the cases where it lost, so that small requests next to bulk work, few and many replication connections, and their tail latencies are now at or ahead of tokio from one configuration, and a request/response frame costs about a quarter fewer user-space instructions than in 0.8.0. By the harness's 8% rule most of the replication wins are ties; the losses (−7% to −32%, and the 80× small-request tail) are gone. All of it on loopback and one machine; a real NIC, cold caches and connection churn are still unmeasured ([TODO.md](TODO.md)).

## 0.9.0 (2026-11-07)
Round 3 performance work on the request/response (replication) path, specified in [performance-improvement/ROUND3.md](performance-improvement/ROUND3.md), planned in [ROUND3-PLAN.md](performance-improvement/ROUND3-PLAN.md), measured in [round3/](performance-improvement/round3/) (gates [G0](performance-improvement/round3/G0.md), [G1](performance-improvement/round3/G1.md), [G2](performance-improvement/round3/G2.md), [G3](performance-improvement/round3/G3.md)). Everything new is opt-in; a default builder behaves as 0.8.0 did, only cheaper. Commits `957f8c5`..HEAD.

**Breaking** (only with `metrics`)
- `DriverStats` gained `enter_left_taskrun` and `drain_enters`, `RuntimeStats` gained `ops_completed`, `BufRingStats` gained `published` (always) and `grown`/`retired` (`metrics`). A new ring's stats no longer equal `BufRingStats::default()`.
- `IORING_OP_SHUTDOWN` (direct-only streams) is no longer cancelled when its future is dropped.

**Per-op cost** (O1, no API change). User instructions per request/response frame −21% to −27% (2000 connections × 4 KiB: 2,342 → 1,798; with a `time::timeout` per frame 3,341 → 2,433). One SQ view per push; a finished op is consumed without drop glue; `write_all` sends the plain buffer first; every op slot is one 64-byte cache line (inline storage for dropped ops stays 48 bytes); timers lost their multi-thread padding (`Sleep` 640 → 72 bytes); task wakers have one vtable address, so `Waker::will_wake` holds for the same task.

**Driver**
- `RuntimeBuilder::drain_taskrun(max)` (D1): on a `defer_taskrun` ring, up to `max` extra enters after a waiting enter while the kernel flags deferred task work (each delivers at most 20 completions on Linux 6.13+). Use 2 for request/response servers next to bulk work; it replaces `min_wait` there without adding latency. `uring::set_min_wait(wait, batch)` changes the batching wait at run time.
- The kernel facts behind it, verified on 7.0 and written up in [round3/r3-probe.md](performance-improvement/round3/r3-probe.md): a run of deferred task work handles at most max(20, `min_complete`) items; an enter that slept delivers exactly `min_complete`; after a `min_wait` enter the task-run flag can stay set with nothing pending.

**Net**
- Eager sends (S1): `try_send`, `try_send_vectored`, `try_recv` (one non-blocking syscall, `WouldBlock` while a ring op of that direction is in flight or on a direct-only stream), `send_eager(buf)` (a `SendFuture` that is ready at once when the direct send took everything, else a ring send of the rest), `set_eager_send(true)` per socket for `write`/`write_all`. At 8 connections the ack p99 falls from 135 to 109 µs, inside tokio's range, at −1.1% frames/s.
- `send_then_recv(reply, buf, flags)` / `send_then_recv_fixed` (C1): a `SEND` linked to a `RECV` on one op slot, one CQE, one task wake; `SendThenRecv`/`SendThenRecvFixed` named futures. `uring::submit_chain` accepts the skip shape (`CQE_SKIP_SUCCESS` on all but the last member, `uring::link_skip`). Use it on `coop_taskrun` rings or with few connections; on `defer_taskrun` with many connections `send_eager` + `recv_poll_first_op` is faster (a linked receive costs a deferred task-work item).
- `send_vectored_op` / `send_vectored_with_flags_op` (A1): `send_vectored` as a named future. No allocation per vectored send: a pooled `msghdr`, or on 6.17+ a plain `SEND` with `IORING_SEND_VECTORIZED` and no `msghdr` at all.
- Provided-buffer working set (B1): `BufRingOptions::working_set(initial, step)` publishes only what demand needs (grows on `ENOBUFS`, shrinks when idle; retired buffers come back hottest first), `BufRing::{published, grow_published, shrink_published, wait_free}`; `RecvStream`/`RecvBundleStream::{next_rearm, rearm_on, ring}` (B2). With whole-frame buffers a single ring ties per-connection buffers at 8 connections and gains 2–3.5% at 2000.
- fd-less sockets in a server (K1): `shutdown_with`/`shutdown_both` (`&self`), async `set_sockopt`/`get_sockopt`, `set_nodelay_async`, `set_tcp_keepalive_async`, `TcpListener::accept_batch_direct_into`. Zero syscalls per connection from accept to close (3 with `register_fixed`), +0.4% frames/s at 2000 connections.

**Time**
- `time::Deadline` (`arm`, `disarm`, `poll_expired`, `Deadline::timeout(after, fut)`), `with_deadline`, `timeout_coarse`, `Instant::recent()` (T1): a request deadline for two stores per request instead of a wheel insert and remove. `time::timeout` itself costs about 40% less than in 0.8.0.

**Stats** (`metrics`): `RuntimeStats::ops_completed`, `DriverStats::{enter_left_taskrun, drain_enters}`, `BufRingStats::{published, grown, retired}`.

**Examples**: `reqresp` (closed-loop request/response reproducer with bulk neighbours, every server path of this round, per-frame hardware counters); `lfq_bench` (`event_channel` throughput and remote wakes, the benchmark TODO.md asked for).

**Dependencies**: `adaptif-gatotkaca-utils` 0.11.3 → 0.11.4 (tighter orderings in the Treiber stack: `reap_all` swaps with `Acquire`, the empty check and counters are `Relaxed`). On x86-64 these compile to the same instructions, but the `Relaxed` empty check lets LLVM lay out the receiver's drain (`lf_queue::Consumer::pop`) differently, and that shows under contention: `lfq_bench`, 1M `u64` messages, best of 5, five alternating runs of each build: 8 producers 17.7 → 20.2 M msg/s (+14%, 5/5 runs, worst rounds 15.4–16.8 → 17.1–18.5); 1 and 4 producers (20–22 M/s) and remote wakes (9.0 M/s from one thread, 6.0/6.3 M/s from 4/8) unchanged within the spread. The request/response path does not touch the stack.

**Measured** (gatotkaca-transport harness, 3 rounds, builds without `metrics`, [G1](performance-improvement/round3/G1.md)): one configuration (`defer_taskrun`, `skip_empty_enter`, `register_ring_fd`, batch accept, `drain_taskrun(2)`, eager acks, multishot receive on a 72 KiB working-set ring) now serves all three replication loads: next to 2 encrypted 1 MiB streams 55,173 frames/s with ack p99 204 µs (tokio 54,758 / 230 µs); 2000 connections × 4 KiB 113,212 frames/s (tokio 108,373); 8 connections 98,275 frames/s with ack p99 113 µs (tokio 92,557 / 103–120 µs). Against 0.8.0 on the same paths ([G3](performance-improvement/round3/G3.md)): +0.7% to +4.4% frames/s, never below. The 1.3 s ack p99 of `coop_taskrun` at 2000 connections (ROUND3.md D2) was a single-accept loop under `coop`'s one-round-per-sweep regime, not the driver: batch accept brings it to 22 ms.

## 0.8.0 (2026-11-07)
Round 2 performance work, specified in [performance-improvement/ROUND2.md](performance-improvement/ROUND2.md), planned in [ROUND2-PLAN.md](performance-improvement/ROUND2-PLAN.md), measured in [round2/](performance-improvement/round2/) (final gate: [G3.md](performance-improvement/round2/G3.md)). Everything new is opt-in: a default builder schedules and submits as 0.7.1 did. Commits `a4de6cf`..HEAD.

**Breaking**
- Counters compile only with the new `metrics` feature. Without it `driver_stats()`, `reset_driver_stats()`, `DriverStats`, `IoUringDriver::{stats, reset_stats}`, `EventReceiver::wakeups()` and the counter fields of `BufRingStats` (`enobufs`, `high_water`, `taken`, `recycled`), `FixedBufStats` (`high_water`, `exhausted`), `EventChannelStats` (`eventfd_wakeups`, `ring_wakeups`, `full`) and `FixedFileStats::high_water` do not exist, and the hot paths carry no counter updates. Users of these add `features = ["metrics"]`. Pool state (`in_use`, `capacity`, ...) stays available.
- `BufRingStats`, `FixedBufStats`, `FixedFileStats`, `EventChannelStats` and the new `RuntimeStats` are `#[non_exhaustive]`: no struct literals outside the crate.

**Scheduler**
- `monoio::task::yield_now()`, `task::consume_budget()`, `task::unconstrained(fut)`; `RuntimeBuilder::coop_budget(Some(n))`: a per-poll budget of ready op results (F1).
- `RuntimeBuilder::lifo_slot(true)`: a task woken by the running task runs right after it. Pair it with `task_budget(n)`; a small budget alone starves waker hand-overs (M2: proxy at 28–36% of target without it, 95% with it).
- `RuntimeBuilder::small_io_priority(Some(n))`: tasks woken by small completions (< `n` bytes) run before the ready queue, capped at half the round budget; a bulk completion keeps its task in the queue for its next 8 small wakes (M3). Recommended for small requests next to bulk streams: `task_budget(61)`, `lifo_slot(true)`, `small_io_priority(Some(16 * 1024))`. Measured (`--cells 64:4096`): proxy p99 41.3 → 13.5 ms (tokio 20.6), json p99 13.0 → 6.2 ms, payload −0.5%.
- `RuntimeStats` via `monoio::runtime_stats()` / `reset_runtime_stats()` or `Runtime::stats()` (feature `metrics`): rounds, parks with busy/park time, polls per round, budget cuts, ready-queue length and high water, spawns, remote wakes, `coop_yields`, `lifo_polls`, `prio_polls` (F2).
- Cheaper completion path: CQEs reaped in batches of 32 with prefetch, op slots removed without a refcount round trip (M5; user-space instructions per frame −5%).

**Net**
- `TcpListener`/`UnixListener::accept_batch_into(&mut vec, max, flags)` / `accept_batch`: keeps one multishot accept armed in the listener and returns everything queued in one poll. Removes the ~6,200 timeouts per run of a plain `accept()` loop at 4096 connections behind bulk streams (M1, M4). `AcceptStream::next_batch` / `next_stream_batch`.
- `RecvStream::read_into(&mut BytesMut)` (also on `RecvBundleStream`) and `into_reader()` (`MultiRecvReader`, `AsyncReadRent`): multishot receive for framed read loops, one copy per byte, re-arms after `ENOBUFS` (F3).
- Fix: `recv_multi` on an incremental (`IOU_PBUF_RING_INC`) ring shared by many connections stalled, because buffers were taken in poll order instead of CQ order. They are now taken at reap time.

**Files**
- `OpenOptions::open_direct(path)`: opens a regular file straight into a fixed-file slot; close is a slot removal, never an io-wq punt. `File::is_direct()`, `fixed_slot()` (F5).
- Per-opcode completion latency in driver ticks (`DriverStats::op_ticks_by_opcode`) shows punted ops; AGENTS.md lists which file ops run inline and which punt on Linux 7.0. File splice always runs in io-wq (M6).

**Runtime setup**
- `utils::ThreadPerCore::new(cpus).run(|core| ..)`: one pinned runtime per CPU, `CoreInfo` with every core's `RingHandle`, optional shared io-wq, `TpcHandle::join()` (F4). Example `thread_per_core`.
- `RuntimeBuilder::sqpoll(idle, cpu)`: SQPOLL applied at `build()`. Warns once per process when the poller would share the runtime's only CPU; `deny_sqpoll_conflict()` makes `build()` fail with `InvalidInput` instead (M8).

**Examples**: `fairness` (load reproducer with `--upstream` proxy model and runtime stats), `smallframes` (M5 reproducer), `thread_per_core`.

**Measured** against 0.7.1 (gatotkaca-transport harness, 5 rounds, builds without `metrics`): 64 KiB mix payload per CPU-second +1.8%, json p99 −5.7%, proxy p99 −4.2%; encrypted mix +1.8%. Many connections with small frames are now within 8% of tokio (−7.9% at 4 KiB, −7.4% at 16 KiB, was −8.3% / −8.9%).

## 0.7.1 (2026-11-06)
No public API changes. Commits `ec850da`..HEAD.

- Fix: timers stalled while the ready queue never empties. `Runtime::block_on` calls `driver.submit()` instead of `park()` when tasks remain after a round (always the case under saturation with `task_budget`, and possible with the default budget), and `TimeDriver::submit` did not process timers, so no `sleep`/`timeout` fired. `TimeDriver::submit` now fires due timers too, with an early-out: no clock read while no timer is pending, no wheel poll before the earliest deadline (registering a timer now keeps `next_wake` up to date). Busy-queue rounds with the timer enabled: no measurable change at the default budget or `task_budget(16)`, with or without 1000 pending timers; about 25 ns more per round (~10%) only with `task_budget(1)` and pending timers, where timers previously never fired. Regression test `tests/timer_cold_path.rs` (fails without the fix, also at the default budget). Found by the gatotkaca-transport harness (run 72).
- Remote wakes (feature `sync`): a waker woken on another thread now goes through the lock-free queue behind `event_channel` (moved to `driver::lf_queue`) instead of a `flume` channel. `flume`, `rustc-hash` and `crossbeam-utils` are no longer direct dependencies (crossbeam-utils comes re-exported from `adaptif-gatotkaca-utils` 0.11.3).
- The remote-waker queue is drained without reversing each batch (wake order doesn't matter); `event_channel` keeps send order.
- The per-thread cache of other runtimes' queues checks the last one used first; registries use `FastU64Map` from `adaptif-gatotkaca-utils` (0.11.2+, whose iterators no longer use an atomic swap per node).
- Measured (256 tasks woken from 1 or 4 threads, release, 12 cores): 4.5–6.9 M wakes/s with 1 producer and 4.2–4.9 with 4, against 4.6–6.5 and 4.0–4.8 with `flume`; ping-pong latency unchanged (~9.2 µs). The owner runtime spends 8.6% of its time draining wakers, against ~14% with `flume` and its lock.

## 0.7.0 (2026-11-06)
Lock-free `uring::event_channel`. Breaking: the send error type and the minimum Rust version changed. Commit `3f8e7f0`.

**Breaking**
- `EventSender::send` returns `Result<(), SendError<T>>`: `SendError::Full(v)` when the channel is at capacity, `SendError::Disconnected(v)` when the receiver is gone (was `Result<(), T>`).
- Minimum Rust version is 1.85 (was 1.75): the new `adaptif-gatotkaca-utils` dependency uses edition 2024.
- `EventReceiver::try_recv` no longer arms a wakeup, so senders pay no syscall for a receiver that only polls.

**event_channel**
- Messages live in nodes of mmap'd segments that double in size: `EventChannelBuilder` (default 1024 slots, up to 131,072; max 2^31). Pages are faulted in on first use.
- Free nodes come from a tagged-index free list (ABA-safe CAS pop); the receiver returns them in batches with one CAS.
- The inbox is the Treiber stack of `adaptif-gatotkaca-utils` 0.11.1: one CAS per send, one swap per received batch, with cache padding and CAS backoff from `crossbeam-utils`.
- Wakeups only when the receiver waits: `MSG_RING` from ring threads through a new driver doorbell (rung directly on the receiver's own ring, eventfd fallback if the `MSG_RING` fails), an eventfd write from other threads.
- The receiver's runtime thread never blocks on a lock held by a sender.
- `EventReceiver::stats()`: eventfd and ring wakeups, `Full` errors, mapped capacity.

**Measured** (1M `u64` messages into one receiver, release build, 12 cores, three runs of best-of-5; a throwaway test, see [TODO.md](TODO.md)):

| Producers | 0.6.0 mutex | 0.7.0 lock-free |
|---|---|---|
| 1 | 42–44 M msg/s, ~11k wakeups | 17–44 M msg/s, 1–22 wakeups |
| 4 | 13–15 M msg/s, ~7k wakeups | 23–44 M msg/s, 2–18 wakeups |
| 8 | ~10 M msg/s, ~6k wakeups | 15–22 M msg/s, 6–153 wakeups |

## 0.6.0 (2026-11-06)
Round 1 performance work, specified in [performance-improvement/ROUND1.md](performance-improvement/ROUND1.md) and planned in [ROUND1-PLAN.md](performance-improvement/ROUND1-PLAN.md). Everything new is opt-in except the bug fixes and the TLS batching: with the default builder and features the runtime submits the same SQEs as 0.5.0. Commits `0dce84e`..`6cb8d82`.

**Fixes**
- No panic on submit paths. A full SQ is flushed and retried, then the op fails with `EBUSY`; rent calls return `(Err, buf)` with the buffer.
- Cancel SQEs are retried and counted when lost.
- `defer_taskrun()` also sets `single_issuer` and `taskrun_flag`, and DEFER rings without the flag pass GETEVENTS on every enter, so completions no longer wait for `park()`.
- The op lifecycle no longer panics on unexpected CQEs; multishot completions and fds in dropped CQEs are handled.
- Partial vectored IO: wrong iovec length and an unstored pointer advance in `IoVecMeta`, and `read_vectored_exact` not setting the init length.

**Driver and builder**
- `IoUringDriver::stats()` / `monoio::driver_stats()`: enters, SQEs, CQEs per tick, retries, cancels, CQ overflow, ops by opcode, multishot, zero-copy, chains.
- Builder: `coop_taskrun`, `taskrun_flag`, `single_issuer`, `defer_taskrun`, `cq_size`, `register_ring_fd`, `min_wait`, `skip_empty_enter`, `task_budget`, `iowq_max_workers`, `with_fixed_files`.
- Multishot ops: `uring::submit_multishot` / `MultiOp`, `accept_multi`, `recv_multi`, recv bundles (`recv_multi_bundle`, `recv_bundle`).
- Linked chains (`uring::submit_chain`, never split) and kernel link timeouts (`submit_with_timeout`, `TcpStream::read_with_deadline` / `write_with_deadline`, no timer needed).
- Cross-ring messages: `msg_ring`, `RingHandle`, `Mailbox`, and `event_channel` for wakeups from any thread without the `sync` feature.
- Dropped in-flight op data up to 48 bytes is stored inline instead of boxed.

**Buffers**
- Provided buffer rings: batched tail publish, lazy page-aligned `mmap` pool, kernel-allocated ring, incremental consumption, `IoBuf` for `SelectedBuf`, stats.
- Registered buffers: `FixedBufPool`, `FixedBuf`, refcounted `FrozenFixedBuf`, fixed read/write/send.
- `IoVecArray` / `IoVecArrayMut`: reusable vectored buffers with no allocation per write.

**Sockets and files**
- `accept_with(AcceptFlags)`, `accept_direct`, `accept_multi_direct`; `register_fixed` for sockets, listeners and files.
- `send_with_flags`, `recv_with_flags`, `recv_poll_first`, `send_vectored` (`sendmsg` + `MSG_NOSIGNAL`).
- Zero-copy: `send_zc_early`, `send_zc_fixed`, `send_vectored_zc`, `send_auto`.
- `cancel_all`: one SQE cancels every op on a connection.
- Named op futures (`monoio::io::futures`, `_op` methods) that structs can hold without boxing.
- `File::fadvise`, file/pipe splice with explicit offsets, `File::read_at_then_send`.

**TLS**
- 64 KiB staging buffers, records batched into one socket write, real `writev`, buffer-size options for rustls, opt-in write coalescing. A 64 KiB write takes 1 socket write instead of 8 (rustls) or 5 (native-tls).

## 0.5.0 (2026-11-03)
Commit `8791c83`.
- Provided buffer rings: `uring::BufRing` / `SelectedBuf` (`IOSQE_BUFFER_SELECT`, Linux 5.19+), recycled on drop.
- `RuntimeBuilder::attach_wq` (`IORING_SETUP_ATTACH_WQ`), and `AsRawFd` for `TimeDriver` so both work with the timer.
- Re-exports so dependents need no direct dependency: `uring::libc`, `tls::rustls::rustls`, `tls::native::native_tls`, plus the `tls-rustls-ring` and `tls-rustls-aws-lc-rs` features.
- All dependency requirements raised to the latest stable versions.

## 0.4.0 (2026-11-03)
Commits `a369e1b`, `57a69dc`.
- CQE hook for SQEs pushed by the user on the raw ring: `uring::user_data`, `MAX_ID`, `set_cqe_hook`, `clear_cqe_hook` (also on `IoUringDriver`), `Cqe::more`, `Cqe::buffer_id`, `TimeDriver::inner`. Hook completions are delivered after the tick, so the hook can call back into the driver.
- The driver ignores completions with unassigned `user_data` prefixes instead of treating them as slab indices.

## 0.3.1 (2026-11-03)
Commits `8beeaa8`, `342df0e`, `93fc6b8`.
- Re-export the `io_uring` crate as `monoio::io_uring`, and `opcode`, `squeue`, `cqueue`, `types`, `IoUring`, `Probe` from `monoio::uring`, so dependents use the same version.
- Fix packaging: the crate README is a symlink inside the crate, so the published package verifies.

## 0.3.0 (2026-11-03)
First release of the fork. Commits `dda6037`..`0a2ab02`.
- io_uring is the only driver: the legacy (epoll/kqueue), IOCP and poll-io drivers, `FusionDriver`/`FusionRuntime` and monoio-compat are removed, with the `legacy`, `iouring`, `poll-io` and `tokio-compat` features and the mio, tokio, windows-sys and lazy_static dependencies. Linux only, kernel 5.6+. `monoio-macros` goes to 0.2.0.
- Raw io_uring access: `uring::submit` (any SQE, resolves to `(Cqe, data)`) and `uring::with_ring`, also on `IoUringDriver`.
- TLS: `monoio::tls::{rustls, native}`, ported from monoio-tls, behind the `tls-rustls`, `tls-native`, `tls-native-alpn` and `tls-native-vendored` features, with examples and tests.
- Dependencies bumped (io-uring 0.7, socket2 0.6, nix 0.31, flume 0.12, syn 3); dead code removed.
- Renamed to `adaptif-gatotkaca-monoio` / `adaptif-gatotkaca-monoio-macros`. Licenses and a NOTICE listing the changes are kept; `AGENTS.md` replaces the old docs tree.
