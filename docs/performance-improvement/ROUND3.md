# monoio fork: round-3 asks (replication path, after 0.8.0)

Written 2026-11-07 for the maintainer of `adaptif-gatotkaca-monoio` (fork at `../monoio-gatotkaca`, HEAD `3992ff2`,
version 0.8.0). Round 1 is `monoio-fork.md`, round 2 is `ROUND2.md`; 0.8.0 delivered round 2. This round comes from
one working session on the **replication workload** (`POST /repl`, 4/16/64 KiB frames, a tiny ack per frame), where
the monoio server lost to or tied with the tokio server. I read the 0.8.0 source for every item; file and line
references are to `monoio/src/` at HEAD.

**Scope and caveats:**
- One machine (Ryzen 5 4600G, Linux 7.0.0-42), one pinned core for the server, loopback, `performance` governor.
- Verdicts use the 8% rule (AGENTS.md): medians ≥ 8% apart, same direction in every round of a 3-round item, disjoint
  p99 ranges. 3-round items are indicative; 1–2-round items are smoke-level and say so.
- Anything marked **hypothesis** was not tested. Statements about what the kernel does are my reading of its
  behaviour from profiles and counters; I did not read kernel source in this session.
- The server-side changes this document refers to (`--tune pipeack`, `sweep`, `mshot`, `syncack` in
  `crates/sut-monoio/src/main.rs`) are in the working tree of this repo, not committed.

## Where the transport stands with 0.8.0

Three loads, tokio against the old recommended monoio configuration and two new ones. 3 rounds each.

| load (run doc) | tokio | monoio `coop:skipenter` (old) | monoio `coop:skipenter:pipeack:sweep:ringfd:fixedfd` ("few") | monoio `defer:skipenter:pipeack:mshot:ringfd:fixedfd:minwait80` ("many") |
|---|---|---|---|---|
| repl only, 8 conns, 4/16/64 KiB (run 117): frames/s | 93,264 | 91,940 (−1.4%) | 97,745 (+4.8%, tie) | 98,525 (+5.6%, tie) |
| same: ack p99 | 111 µs [102–118] | 138 [135–139] | 141 [139–145] (**+27%, counts**) | 140 [140–147] (**+26%, counts**) |
| repl only, 2000 conns, 4 KiB (run 118): frames/s | 112,941 | 104,563 (−7.4%) | 97,750 (**−13.5%, counts**) | 114,816 (+1.7%, tie) |
| same: ack p99 | 19.3 ms [18.6–21.3] | 22.6 ms [22.5–147.6] | **1.32 s [1.01–1.84]** | 19.6 ms [18.5–20.4] |
| mixed: 2 encrypted 1 MiB streams + 9 repl conns (run 119): repl frames/s | 54,265 | 36,948 (**−31.9%**) | 53,862 (−0.7%) | 51,621 (−4.9%) |
| same: repl ack p99 | 249 µs [241–250] | 369 [358–382] | 251 [238–274] | 307 [305–307] (**+23%, counts**) |
| same: stream MB/s | 388.0 | 536.3 (+38.2%) | 386.9 | 402.5 |

What the server does now, without any crate change:
- `pipeack`: after a `/repl` frame it queues the ack (`send_op`) and the receive of the next frame
  (`recv_poll_first_op`) in the same loop round, and awaits both. Before, a frame cost two rounds.
- `sweep`: the 10 s request deadline is one sweeper task that calls `shutdown(2)` on stalled sockets, instead of
  `time::timeout` around every request.
- `mshot`: `recv_multi` on two `BufRing`s (a small "hot" one, a large "spill" one). With `pipeack` only connections
  beyond the first 64 use it.
- The request buffer keeps its capacity (`clear()` instead of `advance()` when fully consumed).

**What limits it.** perf on the server core (6 s, 4999 Hz):
- At 8 connections the kernel is 93.2% of the core for tokio and 92.6% for monoio. The ack send alone is 46% (on
  loopback it runs the client's receive path), receive is 42% (23% copy, 12% a pure TCP ACK from `recvmsg`).
- At 2000 connections × 4 KiB the ack send is 65%. User space is 4.9% for tokio and 8.4% for monoio
  (`coop:skipenter`, before the changes above).
- So the runtime moves this workload by single-digit percents. That is why no item below promises more, and why
  the latency rows above matter as much as the throughput rows.

**Two problems are left that the server cannot solve by itself**, and they define the priorities:
1. No single ring configuration works for both few and many connections. `coop` is right for few (mixed load: tie
   with tokio), and wrong for many (p99 1.3 s). `defer` is right for many, and under-serves replication next to
   bulk streams unless `min_wait` is added, which costs 23% ack p99.
2. With either configuration the ack p99 at 8 connections is 26–27% above tokio's, although the median is lower
   (66–72 µs against 80 µs in the screens). The loop does all receives of a round, then all sends.

## Summary

| id | kind | priority | one line |
|---|---|---|---|
| D1 | feature | **P0** | `defer_taskrun`: a waiting enter returns before the receives that are about to complete; add a bounded drain so one configuration serves few and many connections |
| S1 | feature | **P0** | `try_send` / eager send: a non-blocking `send(2)` on the loop thread with a ring fallback, so a reply leaves while its request is handled |
| D2 | investigate | P1 | `coop_taskrun` with 2000 connections: ack p99 1.0–1.8 s (19 ms with `defer`) |
| C1 | feature | P1 | reply-then-receive as one operation: one completion and one task wake per frame (`CQE_SKIP_SUCCESS` inside a chain) |
| B1 | feature | P1 | `BufRing` working set: the ring is FIFO, so every receive lands in the coldest buffer; let the published set follow demand |
| B2 | feature | P2 | `RecvStream` after `ENOBUFS`: re-arm in place (also on another ring) and an awaitable "a buffer is free" |
| T1 | feature | P2 | timers for request deadlines: a per-connection `Deadline`, a coarse clock, and no wheel insert for timers that live under a millisecond |
| O1 | perf | P2 | per-operation user-space cost: 8.4% of the core against tokio's 4.9% at 2000 connections; symbols listed |
| K1 | feature | P2 | make fd-less (direct) sockets usable by a server: `shutdown(Both)`, socket options through the ring, `accept_batch` direct |
| A1 | feature | P2 | `&self` named futures for vectored send without a `Box<msghdr>` per op (lets the facade drop `into_split`) |
| X1 | feature | P3 | counters that would have shortened this session |
| W1 | docs | P3 | README and AGENTS.md corrections and three patterns worth documenting |

**If only one thing gets done:** D1. It is the reason two configurations are needed, and its fix is contained in
`driver/uring/mod.rs`.

---

## How to reproduce (all items)

From this repo's root (`gatotkaca-transport`), with the fork patched in through `[patch.crates-io]` when testing a
local build (record it in the run doc):

```sh
cargo build --release
T=tokio
FEW=monoio:coop:skipenter:pipeack:sweep:ringfd:fixedfd
MANY=monoio:defer:skipenter:pipeack:mshot:ringfd:fixedfd
COMMON="--require-governor-performance --rounds 3 --duration 10 --warmup 3 --cooldown 2 --cells 1024:2"

# R8: repl only, 8 connections, 4/16/64 KiB frames (run 117)
./target/release/bench-runner run $COMMON --workloads repl --repl-conns 8 \
  --variants $T,$FEW,$MANY,$MANY:minwait80 --out results/r3-repl8
# R2000: repl only, 2000 connections, 4 KiB frames (run 118)
./target/release/bench-runner run $COMMON --workloads repl --repl-conns 2000 --repl-sizes-kib 4 \
  --variants $T,$FEW,$MANY,$MANY:minwait80 --out results/r3-repl2000
# MIX: 2 encrypted 1 MiB streams + repl (run 119)
./target/release/bench-runner run --require-governor-performance --rounds 3 --duration 15 --warmup 5 --cooldown 3 \
  --cells 1024:2 --workloads stream,repl --repl-conns 8 --tokio-stream-mode encrypted --monoio-stream-mode encrypted \
  --variants tokio:mmap:yield:chunk32k,${FEW/monoio:/monoio:mmap:chunk32k:},${MANY/monoio:/monoio:mmap:chunk32k:} \
  --out results/r3-mix
```

- Runtime and driver counters: build the server with `--features metrics` and start it with `SUT_STATS_MS=1000`
  (variant flag `envSUT_STATS_MS=1000`); it prints `monoio::runtime_stats()` and `driver_stats()` once a second.
  The figures quoted below are differences between the snapshots at about 4 s and 8 s of a 10 s load.
- In the mixed load the runner rounds the replication load to 9 connections (3 wrk threads); the label says 8.
- `fairness` and `smallframes` in the fork's `examples/` do not cover this shape (a closed loop of
  request → tiny reply on few connections, next to bulk writers). A reproducer in the fork would help: see X1.

---

## D1 (P0): `defer_taskrun` returns from a waiting enter too early

**Evidence** (mixed load, `metrics` build, one 10 s run per variant, smoke-level; same server binary, only the ring
flag differs):

| variant | repl frames/s | stream MB/s | rounds/s | frames per round (9 conns) | RECV completions reaped in the tick of the enter that submitted them / one tick later |
|---|---|---|---|---|---|
| `coop:skipenter:pipeack:sweep:ringfd:fixedfd` | 52,904 | 368 | 6,118 | 8.6 | 96% / 4% |
| `defer:skipenter:pipeack:sweep:ringfd:fixedfd` | 44,303 | 453 | 7,441 | 5.95 | **52% / 47%** |
| `coop:skipenter:chainack` (0.8.0 `submit_chain`, registered buffers) | 53,422 | 381 | 6,304 | 8.5 | 96% / 4% (`READ_FIXED`) |

- The receive is submitted in the same enter as the ack of the previous frame, so "reaped in the same tick" means
  the client answered and the receive completed **inside that enter**. With `coop` that happens for 96% of the
  frames, with `defer` for half.
- Every round carries the two stream chunks (their send completions are ready at once). A frame that misses the
  enter waits a whole round, about 135–165 µs here. That is the whole difference in replication share.
- The 3-round runs agree: the `defer` configuration needs `minwait80` to get near the `coop` one in the mixed load
  (repl −4.9% against tokio; without it −17% in the scratch run above), and pays for it with ack p99 307 µs against 251 µs (run 119).

**What I read in 0.8.0:**
- `driver/uring/mod.rs:531` `inner_park`: one `inner.enter(1, ..)`, then `tick()`, `drop_deferred`, `deliver`. There
  is no second look at the ring before tasks run again.
- `enter` (`:815`) sets `GETEVENTS` when `want > 0` or the SQ ring's `taskrun` flag is set, so the task work is run
  by that enter. `nothing_to_enter` (`:802`) reads the same flag for `skip_empty_enter`.

**Hypothesis for the mechanism:**
- With `DEFER_TASKRUN` the kernel runs the ring's deferred task work once, when the wait begins, and returns as
  soon as one CQE exists. The stream sends completed at submit, so it returns at once. Receives whose data arrives
  a few microseconds later stay queued as task work until the next enter.
- With `COOP_TASKRUN` the task work is ordinary task work of the thread: it is run again on the way out of the
  syscall, and each receive it runs copies up to 64 KiB (about 10 µs), during which more frames arrive. The syscall
  exit keeps draining. This acts as a short, self-limiting collection window, and it is why `coop` gets 96%.

**What I want:** an opt-in bounded drain after a waiting enter on a `DEFER_TASKRUN` ring.

```rust
impl<D> RuntimeBuilder<D> {
    /// After a waiting `io_uring_enter` returned, enter again (GETEVENTS, no wait) while the kernel flags
    /// pending task work (`IORING_SQ_TASKRUN`), at most `max` times, before the ready tasks run.
    /// Needs `taskrun_flag` (set by `defer_taskrun()`); no effect otherwise. Off by default.
    pub fn drain_taskrun(self, max: u32) -> Self;
}
```

- In `inner_park`, after `tick()`: `while n < max && sq.taskrun() { enter(0, None); tick(); n += 1 }`. `enter(0, ..)`
  already adds `GETEVENTS` when the flag is set.
- The cold path (`Driver::submit`) needs nothing: it already enters whenever the flag is set.
- Each extra enter costs one syscall, only when the kernel says there is work. The bound keeps a flood of arrivals
  from starving the tasks.
- If a more general hook is preferred: a callback after park (`RuntimeBuilder::after_park(fn(&mut ParkCtx) -> bool)`)
  would let applications try policies without a crate release. I would still want `drain_taskrun` as the ready-made one.
- Related, smaller: `min_wait(wait, batch)` is the other lever, but `batch` is fixed for the life of the runtime.
  A good batch here is "the number of connections with a reply in flight", which the application knows per round.
  A way to set it at run time (`monoio::uring::set_min_wait(wait, batch)`) would let me test that. Low priority
  if `drain_taskrun` works.

**Acceptance** (commands above; `$MANY:drain4` stands for the new option at `max = 4`):
- MIX, 3 rounds: `defer…:drainN` without `minwait` has repl frames/s within 3% of `$FEW` and ack p99 within the
  min–max range of `$FEW`; RECV "same tick" share ≥ 90% in a `metrics` run.
- R2000, 3 rounds: frames/s not below `$MANY` by more than 2%, p99 range overlapping.
- R8, 3 rounds: frames/s not below `$MANY:minwait80`.

---

## S1 (P0): eager send (`try_send`), so a reply leaves while its request is handled

**Evidence** (run 120, repl only, 8 connections, 2 rounds, smoke-level):

| variant | frames/s | ack p99 |
|---|---|---|
| tokio | 88,556 | 128 µs [126–130] |
| `coop:skipenter:pipeack:sweep:ringfd:fixedfd` (ack as SEND SQE) | 95,821 | 147 [143–151] |
| `defer:skipenter:pipeack:sweep:ringfd:fixedfd:syncack` (ack as a direct `send(2)`) | 95,861 | **122 [121–124]** |
| `coop:…:syncack` | 91,300 | 154 [152–156] |

- `syncack` sends the ack with `libc::send(fd, .., MSG_DONTWAIT | MSG_NOSIGNAL)` inside the task poll that parsed
  the frame, and only uses the ring when the socket would block. The receive of the next frame is still a ring op.
- Under `defer` it keeps the throughput and removes the p99 penalty of the batched loop (run 117: 140–141 µs against
  tokio's 111 µs; here 122 against 128). The ack no longer waits behind the other connections' work in the next enter.
- At 2000 connections × 4 KiB it is neutral (run 121, 2 rounds: 110,271 against 110,395 frames/s). So one syscall
  per reply costs about the same as one SEND SQE with its CQE, slab slot and task wake, even with this machine's
  return-thunk mitigations.
- Under `coop` it is worse (91,300). Not investigated; see D2.

**Why this belongs in the crate and not in my server:**
- **Ordering.** A direct send must not overtake a ring send that is still in flight on the same socket. My server
  can guarantee that only because it controls every write of the connection. The stream knows its in-flight sends.
- **Direct-only sockets** (`accept_direct`, `accept_multi_direct`) have no fd, so the syscall is not available.
  The eager path has to fall back to the ring there. That decision belongs to the stream type.
- **Timeouts.** Today every op future is `Pending` on its first poll, so `time::timeout(d, request)` always
  registers its timer (T1). With an eager send, a request whose bytes are already in the buffer finishes inside
  one poll, as on tokio, and the timer is never registered.
- **The facade** (`gatotkaca-transport-monoio`) can then offer the same "write completes now" fast path as the
  tokio backend through one method.

**What I want:**

```rust
impl TcpStream /* and UnixStream */ {
    /// One non-blocking `send(2)` on the calling thread (`MSG_DONTWAIT | MSG_NOSIGNAL`).
    /// `Ok(n)`: n bytes taken (may be short). `Err(WouldBlock)`: socket buffer full, a ring send of this
    /// stream is still in flight (ordering), or the stream is direct-only. No op slot, no SQE, no CQE.
    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize>;
    pub fn try_send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize>;

    /// `try_send` first; whatever is left goes through the ring (`IORING_OP_SEND`, with
    /// `IORING_RECVSEND_POLL_FIRST`, since the socket just reported it is full). Resolves at once,
    /// without a `Pending`, when the eager send took everything.
    pub fn send_eager<T: IoBuf>(&self, buf: T) -> SendEagerFuture<T>;

    /// The receive counterpart, for symmetry and for request/response clients:
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize>;
}
```

- "A ring send is in flight" needs a counter per stream (in `SharedFd` or the stream): incremented when a send op
  is pushed, decremented when it completes or its future is dropped after completion.
- `write_all` on such a stream could take a builder-level or per-stream switch (`set_eager_send(true)`), so that
  existing code gets the behaviour without edits. Off by default.
- Document when it loses: large writes that usually fill the socket buffer (bulk streams) pay a wasted syscall.
  My server would use it for replies up to a few KiB only.

**Acceptance:**
- R8, 3 rounds, `defer:skipenter:pipeack:sweep:ringfd:fixedfd` with the ack through `send_eager`: frames/s within
  2% of the SQE ack, ack p99 range not above tokio's.
- R2000, 3 rounds: within 2% of `$MANY`.
- A test that a `try_send` issued while a ring send is in flight returns `WouldBlock`, and one for direct-only streams.

---

## D2 (P1, investigate): `coop_taskrun` with 2000 connections

**Evidence** (run 118, 3 rounds, 2000 connections × 4 KiB, closed loop):

| variant | frames/s | ack p50 (screen) | ack p99 |
|---|---|---|---|
| tokio | 112,941 | 17.6 ms | 19.3 ms [18.6–21.3] |
| `coop:skipenter` (0.8.0 defaults otherwise, plain `read`/`write_all`) | 104,563 | 19.4 ms | 22.6 ms [22.5–**147.6**] |
| `coop:skipenter:pipeack:sweep:ringfd:fixedfd` | 97,750 | 20.7 ms | **1.32 s [1.01–1.84]** |
| `defer:skipenter:pipeack:sweep:ringfd:fixedfd` (run 111, 1 round) | 106,431 | 18.6 ms | 19.4 ms |
| `coop:skipenter:mshot:ringfd:fixedfd` (run 112, 1 round) | 104,096 | 19.0 ms | 23.2 ms |
| `defer:skipenter:mshot:ringfd:fixedfd` (run 112, 1 round) | 111,841 | 17.7 ms | 18.9 ms |

- Same server binary in rows 3 and 4; only `coop` against `defer`. A p99 of 60 times the median means some
  connections are not served for many rounds. No errors, no client timeouts.
- The pipelined receive uses `recv_poll_first_op`, so every receive completes through task work, never inline.
- Multishot receive also prefers `defer` (−7% frames/s with `coop`).

**What I want:**
- Find out whether the starvation is in the kernel's handling of that many task-work items per syscall exit, or in
  the driver (for example `tick()` handling only the CQEs present on entry while more are posted during the exit).
  `RuntimeStats`/`DriverStats` of a `metrics` build at this load would show whether rounds, CQEs per tick or the
  ready-queue high water differ.
- If it is inherent to `COOP_TASKRUN`: say so in AGENTS.md ("above some hundreds of connections use
  `defer_taskrun`"), and consider a `build()` warning like the SQPOLL one is not needed; the doc line is enough.

**Acceptance:** an explanation with numbers, and either a fix (p99 at R2000 within 2× of the `defer` value) or the
documented rule.

---

## C1 (P1): reply-then-receive as one operation

**Evidence:**
- A frame costs two operations: SEND (the ack) and RECV (the next frame). Each has its own slab slot, CQE and waker.
  The task is polled twice per frame: once for the send completion (it finds the receive pending), once for the
  receive. `metrics`, mixed load: 81.6k task polls/s for 44.3k frames/s plus about 14k stream chunks/s.
- `uring::submit_chain` exists and I used it (`chainack`: SEND → READ_FIXED, one future). It still reaps two CQEs
  and holds two slots, and `driver/chain.rs:35` `validate` rejects `CQE_SKIP_SUCCESS` because "the slot of an SQE
  that posts no CQE would never be freed".
- At 2000 connections the per-request kernel cost outside TCP is about 10% of the core for the two ops
  (`io_file_get_normal` 2.66%, `__io_submit_flush_completions` 2.72%, `io_free_batch_list` 1.41%,
  `io_sendmsg_prep` 1.37%, `io_recvmsg_prep` 1.24%, `io_init_req` 1.16%); user space adds the slot, the future and
  the wake per op (O1). One CQE less per frame removes part of the flush/free share and one of the two wakes.
  **Hypothesis:** worth 1–2% of the core at small frames; not measured.

**What I want:**

```rust
impl TcpStream /* and UnixStream */ {
    /// Send `reply`, then receive into `buf`, as one linked pair (SEND with IOSQE_IO_LINK | IOSQE_CQE_SKIP_SUCCESS,
    /// then RECV with IORING_RECVSEND_POLL_FIRST). One completion and one task wake in the normal case.
    /// Resolves to (send result, receive result, reply, buf):
    /// - send complete: `(Ok(reply_len), recv_result, ..)`;
    /// - send failed (possibly after a partial send): `(Err(e), Err(ECANCELED), ..)`; nothing was received.
    pub fn send_then_recv<S: IoBuf, R: IoBufMut>(&self, reply: S, buf: R, recv_flags: MsgFlags)
        -> SendThenRecv<S, R>;
    /// The same with a registered receive buffer (`READ_FIXED`), which is what `chainack` builds by hand today.
    pub fn send_then_recv_fixed<S: IoBuf>(&self, reply: S, buf: FixedBuf) -> SendThenRecvFixed<S>;
}
```

- Bookkeeping that makes `SKIP_SUCCESS` safe: both SQEs carry the **same slot** (`user_data`). The slot expects
  either one CQE (the receive: the send succeeded and was skipped) or two (the failed send, then the receive with
  `-ECANCELED`). A failed link always posts a CQE for every remaining member, so the receive's CQE always arrives
  and always arrives last. The slot is freed on the receive's CQE; a CQE seen before it is the send's result.
  Distinguishing them needs one bit (for example a spare high bit of `user_data` below the 2^62 limit, or the
  order rule alone).
- A short send (`0 < n < len`) must not be skipped as a success, or the caller could never see it. Set
  `MSG_WAITALL` on the send: as far as I know the kernel then finishes the send itself or fails it, so "skipped"
  means "fully sent". Please verify this on the target kernel with a test that fills the socket buffer.
- Raw form, so applications can build other shapes (SEND → SEND → RECV): let `submit_chain` accept `SKIP_SUCCESS`
  on every member but the last, with the rule above (the chain resolves on the last member's CQE; members without
  a CQE report `Cqe::new(0, 0)`/"skipped"). Reject it on the last member as today.
- With `coop_budget`, the pair counts as one result (as `ChainOp` does).

**Acceptance:**
- R2000 and R8, 3 rounds, the pipelined path rebuilt on `send_then_recv`: frames/s not below the `send_op` +
  `recv_poll_first_op` pair, task polls per frame ≤ 1.1 in a `metrics` run (2.0 today).
- Tests: failed send (peer reset) yields the send error and `ECANCELED`, the slot is freed, nothing leaks; drop of
  the future in flight keeps both buffers alive until the receive's CQE.

---

## B1 (P1): `BufRing` working set follows demand

**Evidence:**
- A provided-buffer ring is FIFO: the kernel takes buffers at the head, `Shared::recycle` (`driver/buf_ring.rs:753`)
  appends at the tail. Every receive therefore lands in the buffer that has been idle longest, and a ring with more
  buffers than are in use at once is cache-cold by construction.
- Runs 26–28 (0.5.0, 2 rounds): `bufring` = 128 × 128 KiB (16 MiB) cost about 6% frames/s and 12–15% per CPU-second
  at 8 connections; `bufring16` (2 MiB) was within noise of the per-connection buffer. Tokio and the non-ring monoio
  path reuse one buffer per connection (8 × 68 KiB), which stays in L2/L3.
- The right size depends on the number of connections that have data in the same round: 8 here, about 2000 in
  R2000 (ack p50 ≈ one sweep over all connections). `BufRing::new` publishes all `entries` at creation
  (`create`, `:312`) and there is no way to hold some back.
- My workaround in the server: two rings. "hot" = 16 × 72 KiB; "spill" = 4096 × 16 KiB; a connection moves to
  spill at its first `ENOBUFS`. It works (R2000 ties tokio) but it is application code guessing at something the
  pool can measure (`in_use`, `ENOBUFS`).
- **Hypothesis** (not isolated in this session): the cold destination is what made `bufring` slower. The copy is
  23% of the core at 8 connections, so a few percent are at stake, not more.

**What I want:**

```rust
impl BufRingOptions {
    /// Publish only `initial` buffers at creation; keep the rest in reserve. When a receive ends with
    /// `ENOBUFS`, or `in_use` reaches the published count, publish `step` more (up to `entries`).
    /// When the high-water mark of `in_use` over the last `window` ticks is below half the published count,
    /// retire recycled buffers into the reserve instead of republishing them (down to `initial`).
    pub fn working_set(self, initial: u16, step: u16) -> Self;
}
impl BufRing {
    pub fn published(&self) -> usize;          // buffers currently visible to the kernel or in use
    pub fn grow_published(&self, n: u16) -> usize;   // manual control, returns the new count
    pub fn shrink_published(&self, n: u16) -> usize; // takes effect as buffers are recycled
}
```

- Growth on `ENOBUFS` makes exhaustion a one-time event per demand level instead of a steady state. Combined with
  B2 (re-arm in place) a connection would not notice.
- Retiring is cheap: `recycle` simply does not write the ring slot for a buffer that is being retired.
- With `incremental` rings the same idea applies per buffer.
- If this is judged too much policy for the crate, the two manual methods are enough for me to build it.

**Acceptance:**
- R8 with all connections on multishot receive (`defer:skipenter:mshot:ringfd:fixedfd`, one ring of 4096 × 16 KiB
  with `working_set(16, 16)`), 3 rounds: frames/s within 2% of the `pipeack` path (per-connection buffers).
- R2000 with the same ring: within 2% of `$MANY`; `BufRingStats::enobufs` stops growing after the ramp.

---

## B2 (P2): `RecvStream` after `ENOBUFS`

**Evidence** (`net/tcp/multishot.rs:302–307`, `driver/op/recv_multi.rs` `OnEnobufs`):
- `RecvStream::next()` / `poll_next` use `OnEnobufs::Yield`: `Err(ENOBUFS)` once, then `None`. To continue I must
  call `recv_multi` again, which builds a new stream (a new op slot), and I must guess when a buffer is free. My
  server yields once and retries (`common::yield_once`), the older `bufring` path sleeps 200 µs.
- `read_into` and `MultiRecvReader` re-arm by themselves (`OnEnobufs::Rearm`), but they copy every byte into a
  `BytesMut`. The replication path counts frames in place in the ring buffer and must not copy, so it needs
  `next()`.

**What I want:**

```rust
impl RecvStream /* and RecvBundleStream */ {
    /// Like `next`, but after `ENOBUFS` it re-arms the multishot (same op slot where possible) once a
    /// buffer of the ring is free, and continues. Never yields `ENOBUFS`.
    pub async fn next_rearm(&mut self) -> Option<io::Result<SelectedBuf>>;
    /// Re-arm on another ring (my hot → spill move), keeping the stream and anything already queued.
    pub fn rearm_on(&mut self, ring: &BufRing) -> io::Result<()>;
}
impl BufRing {
    /// Resolves when at least one buffer is available to the kernel again (immediately if one is).
    pub fn wait_free(&self) -> impl Future<Output = ()>;
}
```

- `wait_free` needs a waiter list in `Shared`, woken from `recycle`. It replaces yield/sleep polling.
- Also useful: `RecvStream::ring(&self) -> &BufRing`, so helpers need not pass the ring alongside the stream.

**Acceptance:** a test with a ring smaller than the number of active connections: all data arrives in order, no
busy loop (bounded task polls per received chunk), and no `ENOBUFS` reaches the caller of `next_rearm`.

---

## T1 (P2): timers for request deadlines

**Evidence:**
- The server wraps every request in `time::timeout(10 s, serve_one(..))`, as the tokio server does. On tokio the
  request future usually finishes inside its first poll (read and write are syscalls), so the `Sleep` is never
  polled and never registered. On monoio every op future is `Pending` on its first poll, so `Timeout::poll` polls
  the `Sleep`, which registers it (`time/driver/entry.rs:501` `poll_elapsed` → `reset` → `reregister`), and the drop
  removes it again. That is one wheel insert and one remove per request, plus `Instant::now()` in `timeout()`
  (`time/timeout.rs:34`).
- perf at 2000 connections (`coop:skipenter`): `[vdso]` clock reads 1.02% of the core against 0.22% on tokio;
  `Sleep::poll` 0.15%, `Wheel::remove` 0.06%.
- Replacing the timeout with a sweeper task (`sweep`): 101,052 → 103,710 frames/s (+2.6%) on the plain path and
  104,628 → 105,819 (+1.1%) on the pipelined one (run 111, 1 round each, smoke-level).
- The sweeper is not a general answer: it closes the socket with `shutdown(2)`, which needs an fd and does not
  interrupt a wait on another socket (the proxy path keeps `timeout` around its upstream round trip).
- `Sleep::reset` to a **later** deadline is already O(1) (`entry.rs:486`, `extend_expiration`): the wheel is only
  touched when the old slot fires. A per-connection `Sleep` that is reset at each request would be cheap, but an
  idle keep-alive connection must not time out, and `Sleep` has no "disarmed" state.

**What I want:**
1. **A re-armable deadline** built on the lazy reset:
   ```rust
   pub struct Deadline { /* one timer entry, registered at most once per expiry */ }
   impl Deadline {
       pub fn new() -> Self;                      // disarmed, not in the wheel
       pub fn arm(self: Pin<&mut Self>, after: Duration);   // O(1) when already registered with an earlier deadline
       pub fn disarm(self: Pin<&mut Self>);       // O(1): a flag; the entry may stay in the wheel
       /// Ready only when armed and expired. A wheel expiry while disarmed is swallowed.
       pub fn poll_expired(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()>;
   }
   pub fn with_deadline<F: Future>(d: Pin<&mut Deadline>, f: F) -> impl Future<Output = Result<F::Output, Elapsed>>;
   ```
   Cost per request: two stores. Cost per connection: one wheel insert per 10 s at most.
2. **A coarse clock.** `time::Instant::recent()`: the time of the last driver tick (one clock read per park or
   cold-path submit, which `TimeDriver` already does while timers are pending). `Deadline::arm` and a
   `timeout_coarse` would use it. A deadline measured in seconds does not need a clock read per request.
3. **No wheel insert for timers that die young** (helps existing `timeout()` callers without edits): an entry
   created at driver tick `t` goes to a "new" list and is moved into the wheel only when the driver next processes
   time at a tick later than `t` (`process_at_time`). If it is dropped before that, the cost is a list unlink. The
   wheel's resolution is 1 ms and these requests live 10–200 µs.

**Acceptance:**
- R2000, 3 rounds: the plain pipelined path with `with_deadline` is within 1% of `sweep`.
- Tests: a disarmed deadline never fires; `arm` after an expiry while disarmed fires at the new time; `recent()`
  never lags the real clock by more than one park.

---

## O1 (P2): per-operation user-space cost

**Evidence** (perf, 2000 connections × 4 KiB, `coop:skipenter`, plain `read`/`write_all` path; percent of the core):

| | tokio | monoio |
|---|---|---|
| user space total (server binary + libc + vdso) | 4.9% | 8.4% |

Monoio symbols, self time:

| symbol | % |
|---|---|
| `common::proto::parse_request` (application, same code on tokio) | 1.49 |
| `[vdso]` clock reads (T1) | 1.02 |
| `sut_monoio::write_all::<Bytes>::{closure}` (the `AsyncWriteRentExt::write_all` state machine) | 0.99 |
| `UringInner::reserve_sqes` | 0.81 |
| `UringInner::tick` | 0.62 |
| `task::raw::poll` | 0.57 |
| `drop_glue::<io::futures::State<op::send::Send<Slice<Bytes>>>>` | 0.47 |
| `drop_glue::<io::futures::State<op::recv::Recv<SliceMut<BytesMut>>>>` | 0.41 |
| `Slab::<MaybeFdLifecycle>::insert` | 0.33 |
| `UringInner::poll_op` | 0.31 |
| `Sleep::poll` | 0.15 |

- Round 2 (M5) already cut the completion path by 5%. This list is the submit side and the future plumbing, which
  M5 did not touch. Two ops per frame at 100k frames/s is 200k passes through each row.

**What I want** (each is a suggestion to check with the `smallframes` example, not a diagnosis):
- `reserve_sqes` (`driver/uring/mod.rs:906`): for `n == 1` it takes `self.uring.submission()` up to three times
  (capacity check, the `free` closure, then `push_op` takes it again) and runs the retry loop set-up. A fast path
  `if sq.len() < sq.capacity() { return Ok(()) }` first, with the capacity cached in `UringInner`.
- The drop glue of `io::futures::State<D>`: after `poll_result` the state is `Done`, yet the glue is a call that
  shows up at 0.4–0.5% per op kind. Check that `Done` drops inline, without a call (for example by taking the op
  out by value in `poll_result` so the enum holds nothing with a destructor afterwards).
- `write_all` for a buffer that the first send takes completely (the normal case for a reply): return without
  building a `Slice` and re-entering the loop. Or document `send_op` + a length check as the fast path for small
  replies, which is what the pipelined path uses now.
- `Slab::insert` + `MaybeFdLifecycle::new` per op: C1 halves the number of slots per frame; multishot receive
  removes the receive slot. No ask beyond those.

**Acceptance:** `smallframes` (or the R2000 load) shows user-space instructions per frame down by ≥ 5% with
unchanged results; no API change.

---

## K1 (P2): fd-less sockets that a server can actually use

**Evidence:**
- Every op on a plain fd pays `io_file_get_normal` (2.66% of the core at 2000 connections). `register_fixed`
  removes it and I use it (`fixedfd`), at the price of one `io_uring_register` syscall per accepted connection.
  `accept_direct` / `accept_multi_direct` avoid both, but the stream then has no fd, and the server needs one for:
  - the request deadline: the sweeper uses `shutdown(fd, SHUT_RDWR)`. `TcpStream` only offers `shutdown()` of
    the write side through `AsyncWriteRent` (`&mut self`; `net/tcp/stream.rs:554` `shutdown_write`), although
    `Op::shutdown(fd, how)` (`driver/op/shutdown.rs:17`) takes any `how`;
  - socket options after accept (`set_nodelay` fallback, `TCP_QUICKACK`, buffer sizes per connection);
  - `try_send` (S1), which cannot work without an fd.
- `accept_batch_into` (round 2, M1/M4) has no direct variant, so choosing direct accept means going back to a
  stream of single accepts.

**What I want:**
- `TcpStream::shutdown_both(&self)` / `shutdown_with(&self, std::net::Shutdown)`: a direct syscall when there is
  an fd, `IORING_OP_SHUTDOWN` for direct-only. `&self`, so a supervisor task can call it while the connection task
  has ops in flight.
- Socket options through the ring for direct-only sockets: `IORING_OP_URING_CMD` with
  `SOCKET_URING_OP_SETSOCKOPT` / `GETSOCKOPT` (Linux 6.7+), behind the existing setters (`set_nodelay`, ...) and a
  generic `set_sockopt(level, name, &value)`. `Unsupported` on older kernels, as today.
- `accept_batch_direct_into(&mut vec, max, flags)` on `TcpListener`/`UnixListener`.
- In AGENTS.md: what a direct-only stream costs and saves per connection and per op, so users can decide.
  `fixedfd` was "neutral" in the round-1 screens and worth +0.6% here (run 111, 1 round): small.

**Acceptance:** tests for each new call on a direct-only stream; R2000 with direct accept within 1% of `fixedfd`
or better, with one syscall less per connection (strace count during the ramp).

---

## A1 (P2): `&self` named futures for vectored sends, without a box per op

**Evidence** (facade backend `crates/gatotkaca-transport-monoio/src/stream.rs`):
- The facade's `StreamIo` takes `&self` and may have one read and one write in flight. The rent traits take
  `&mut self`, so the backend splits every stream (`into_split`) and moves each half in and out of a
  `Cell<Option<Half>>` around every operation.
- `recv_op(&self)`, `send_op(&self)`, `writev_op(&self)` exist on `TcpStream` and `UnixStream` and would remove the
  split. Two gaps keep the facade on `writev`:
  - there is no named future for `send_vectored` (already in the fork's TODO.md), and `writev_op` is
    `IORING_OP_WRITEV`: no `MSG_NOSIGNAL`, and the generic write path instead of `sendmsg`;
  - `Op::send_msg_vec` allocates a `Box<libc::msghdr>` per call (`driver/op/send.rs:264, :277`). The facade made
    its gather list allocation-free on purpose; this puts one allocation back per write.

**What I want:**
- `send_vectored_op(&self, bufs) -> SendVectoredFuture<T>` and `send_vectored_with_flags_op` on `TcpStream` and
  `UnixStream`.
- The `msghdr` stored inline in the op (it is 56 bytes; the op data is pinned in its slot or future while the
  kernel uses it, which is the same guarantee the boxed one relies on). If the address must be stable across a
  move of the future, build it in `uring_op()` from the stored iovec pointer, as `SendMsgVec::uring_op` already
  re-reads the iovec pointer there.
- TODO.md also lists "dropped in-flight op data is stored inline up to 48 bytes; `SendMsgVec` (72) is still
  boxed": the same change brings it under the limit if the `msghdr` moves out of the dropped-data part.

**Acceptance:** the facade backend rebuilt on `_op(&self)` methods with no `into_split` and no allocation per
write (counted with a test allocator); `gk-monoio` on R8 not below the current build.

---

## X1 (P3): counters and a reproducer

What I had to derive by hand or could not see:
- **Task polls that found nothing.** `RuntimeStats::tasks_polled` counts all polls. A count of polls that ended
  `Pending` without any op of the task having completed since its last poll is not possible in general, but
  "polls per op completion" is: add `ops_completed` (non-multishot CQEs delivered to a waiting waker) next to
  `tasks_polled`. In the pipelined path that ratio is the two-wakes-per-frame of C1.
- **Enters that left work behind.** `DriverStats::enter_left_taskrun`: waiting enters after which the SQ ring's
  `TASKRUN` flag was still (or again) set when the driver looked at it in `tick()`. This is D1 in one number.
- **Inline against deferred completions.** `op_ticks_by_opcode` bucket 0 mixes "completed inline at submit" and
  "completed by task work in the same enter". Splitting bucket 0 needs the CQ tail before and after the submit
  phase, which one enter does not expose; skip unless there is a cheap way.
- **A request/response reproducer in the fork's `examples/`**: N closed-loop clients sending 4/16/64 KiB frames
  and waiting for a 70-byte reply, optionally next to M bulk writers; prints frames/s, reply p50/p99 and the two
  stats structs. `smallframes` is open-loop and has no bulk neighbour; `fairness` has no closed-loop small
  replies. All four mechanisms in this document (D1, S1, C1, B1) show up only in that shape.

---

## W1 (P3): documentation

- `README.md:23` still says `adaptif-gatotkaca-monoio = "0.7"`.
- `README.md:71` says registered buffers are "not supported natively"; `FixedBufPool`, `recv_fixed`, `send_fixed`
  exist since 0.6/0.7.
- `BufRingOptions::batch_publish` says "the per-tick publish needs driver support; until the driver calls it, use
  `BufRing::publish`", but `driver/uring/mod.rs` calls `buf_ring::flush_pending()` in `enter` and in the
  skip-enter path. One of the two is out of date.
- Patterns that cost me measurements and are worth a paragraph each in AGENTS.md:
  - **Request/response loops:** queue the reply and the next receive in the same round (`send_op` +
    `recv_poll_first_op`, or C1). Awaiting the send first costs a whole loop round per request; next to bulk tasks
    that was −32% replication throughput (run 119).
  - **Receive buffers must not shrink.** `BytesMut::advance` after each request leaves less capacity for the next
    one; a request that does not fit costs a second receive, which is a loop round here and only a syscall on an
    epoll runtime. `clear()` when the buffer is fully consumed. (About +4% frames/s at 8 connections relative to
    tokio, runs 113 → 115, 1 round each.)
  - **`coop_taskrun` or `defer_taskrun`:** few connections next to bulk work → `coop`; many connections or
    multishot receive → `defer` (until D1 and D2 change this).

---

## Not asked, and why

- **Zero-copy receive (`zcrx`) and NAPI busy poll.** They need a real NIC; loopback cannot show them. Worth a
  round of their own once the transport is measured on hardware.
- **SQPOLL, `send_zc` for replies, registered buffers for receives.** Measured in earlier rounds or here:
  SQPOLL on the server's own core is −85%; replies are 70 bytes; `fixedrecv` most likely helped because its buffer
  never shrank (the pipelined path with a plain `BytesMut` that keeps its capacity is as fast).
- **The pure TCP ACK from `recvmsg` (12% of the core at 8 connections) and the client-side work done by the
  server's send on loopback.** Kernel and benchmark-setup matters, the same for both runtimes.
- **A `MSG_TRUNC`-style "discard the body in the kernel" receive.** It would remove the copy, and no real
  replication server could use it.
