# Round 3 work plan

Implements [ROUND3.md](ROUND3.md). Item ids (D1, S1, D2, C1, B1, B2, T1, O1, K1, A1, X1, W1) and load names
(R8, R2000, MIX, `$FEW`, `$MANY`) are the ones defined there. Base: `3992ff2` (0.8.0). Target release: **0.9.0**.
The benchmark lives in `../gatotkaca-transport` (bench-runner, `sut-monoio`, `sut-tokio`).

Three things run through the round:
1. **The asks of ROUND3.md**, split into 13 packages that agents do in parallel, in three waves (3, 7 and 3
   agents). A lead merges each wave and runs the gates.
2. **Code review while working.** Every package reads the code it touches for a cheaper way to do the same work.
   A clearly cheaper alternative replaces the existing code. A doubtful one gets a quick measurement first
   ("Quick-measure protocol"). The alternatives already found while reading 0.8.0 are in "Efficiency alternatives".
3. **Kernel behaviour is checked before code depends on it.** Three asks rest on statements about the kernel that
   nobody verified (ROUND3.md says so). Wave 0 verifies them with probe tests and the kernel source at v7.0.

Status of the statements below: everything about monoio comes from reading the 0.8.0 source. Everything about the
kernel is from memory and marked **to verify**. Nothing was built or measured for this plan.

---

## Rules for every package

- New behaviour is opt-in (builder method, per-stream switch or new API). With default features and a default
  builder, behaviour is as in 0.8.0. Internal rewrites that keep behaviour (O1, the alternatives) need no switch.
- Instrumentation only behind `metrics`, through `metric!`. Verdicts on builds without `metrics`.
- No panic on a submit path. Errors come back as `io::Result` / `BufResult` with the buffer.
- Kernel-dependent features check support at runtime and return `Unsupported`; tests skip on older kernels.
- Each package adds tests in `monoio/tests/` and passes, in its worktree:
  - `cargo test -p adaptif-gatotkaca-monoio`
  - `cargo test -p adaptif-gatotkaca-monoio --features metrics`
  - `cargo clippy --workspace` and `cargo clippy --workspace --features adaptif-gatotkaca-monoio/metrics`
  - `cargo fmt`
- **Review rule.** Before changing a function, read its callers and its hot path. A clearly cheaper way replaces
  the code; a doubtful one is measured first. The package notes say what was replaced and why, and what was
  measured and rejected (with the numbers).
- **File ownership.** A package edits only the files (or the named functions of a shared file) its wave table
  gives it. Two exceptions:
  - a `pub use` line for its own new items in a registry file (`lib.rs`, `net/tcp/mod.rs`, `net/unix/mod.rs`,
    `time/mod.rs`, `driver/raw.rs`), listed in its notes;
  - any other change it needs elsewhere is written up in its notes under "requested changes" and applied by the
    lead at merge. The agent may make it locally to get its tests to pass, and says so.
- Each package writes `docs/performance-improvement/round3/<package>.md`: what changed, what was replaced, what
  was tried and rejected, open questions, requested changes.
- Verdicts come only from the gates (lead, no agent active, bench repo, `performance` governor, the 8% rule or
  the tighter bound ROUND3.md gives for the item).

## How agents run

- One agent per package, each in its own git worktree (`isolation: "worktree"`), on branch `r3/<package>` from the
  wave's base tag (`r3-base`, `r3-wave0`, `r3-wave1`).
- The lead does the seed commit, starts all agents of a wave at once, merges them in the order given, runs the
  full test matrix on the merged tree, applies the requested changes, tags the wave (`r3-wave0`, ...) and runs the
  gate before the next wave starts.
- Prompt for each agent: this file's "Rules", "Quick-measure protocol" and "What the code says" sections, its
  package section and the rows of the two tables (KV, E) it is named in, `AGENTS.md`, and ROUND3.md's section for
  its items. Wave 1 and 2 agents also get `round3/G0.md` (and `G1.md` for wave 2).
- **CPU split, so that work and measurement do not collide** (this machine: 6 cores, CPU `n` and `n+6` are
  siblings):
  - builds and tests: `taskset -c 0,1,5,6,7,11` and `CARGO_BUILD_JOBS=4`. A test that pins a CPU outside that set
    is rerun alone without the mask;
  - quick measurements: cores 2, 3, 4 (`--cpu 2 --client-cpus 3,4`), one at a time, under
    `flock /tmp/r3-measure.lock`.
- A package that fails its gate goes back to its agent for one more iteration, alongside the next wave.

## Quick-measure protocol (for doubtful alternatives)

- Tools: `monoio/examples/smallframes.rs` (exists) and the `reqresp` example (r3-repro). Both pin the server and
  the clients and print frames per CPU-second and, per frame, user instructions, user cycles and all cycles
  (`perf_event_open` on the server thread).
- A/B: build both variants into separate target directories, run them alternately under the measure lock, 5 runs
  of 6 s each, compare medians.
- **Inside a package** (other agents are building): the deciding metric is **user instructions per frame**. It is
  exact and does not depend on what else the machine does. Adopt when it drops by ≥ 1%.
- Cycles, cache misses and latency measured inside a package are "screening, machine not quiet". An alternative
  whose case rests on them (E5, the D1 drain variants) is delivered as its **own commit** on the package branch,
  with the screening numbers. The lead re-measures it in a quiet slot at the wave merge and keeps or drops that
  commit.

---

## What the code says (shapes the plan)

1. **`inner_park` enters once** (`driver/uring/mod.rs:531`): `enter(1, ..)`, `tick()`, `drop_deferred`, `deliver`.
   `enter` (`:815`) adds `GETEVENTS` when `want > 0` or the SQ ring's `TASKRUN` flag is set. `nothing_to_enter`
   (`:802`) reads the same flag. So the D1 drain loop fits in `inner_park` without touching `enter`.
2. **`tick()` handles only the CQEs present when it starts** (`:700`). On a `COOP_TASKRUN` ring more CQEs can be
   posted at any later kernel entry. They wait for the next tick. This is one D2 candidate.
3. **A pipelined connection pushes two SQEs per frame.** `sut-monoio` uses 4096 ring entries. At R2000 a full round
   is 4000 SQEs of 4096. A full SQ makes `reserve_sqes` (`:906`) call `submit()` and `tick()` inside a task poll.
   This is another D2 candidate (`DriverStats::sq_full_flushes` shows it).
4. **Every op holds an `Rc` of the driver** (`Op::driver`, `driver/op.rs`): one `Rc` clone and one drop per op, plus
   the scoped-TLS lookup at submit. `OpFuture::poll_result` (`io/futures.rs`) assigns `State::Done` over
   `State::Running(op)`, which runs `Op::drop` (a driver call that returns at once) and the `Rc` drop. That is the
   drop glue ROUND3.md lists under O1.
5. **`push_op` builds the SQ view three times per op** (`:986`): the capacity check and the `free` closure in
   `reserve_sqes`, then the push. Each view loads the kernel's head and stores the tail on drop.
6. **An op slot is 72 bytes** (`Lifecycle` 64, of which the inline storage for dropped ops is 56). Most slots span
   two cache lines; `Slab::prefetch` fetches both (round 2, W2-m5).
7. **Timers carry multi-thread padding in a single-thread runtime.** `TimerShared` (`time/driver/entry.rs`) holds
   two `CachePadded` fields aligned to 128 bytes on x86_64, so a `Sleep` is at least 256 bytes with 128-byte
   alignment, and every future containing a `timeout()` inherits that alignment. The state is already `Cell`s.
   `Sleep::new_timeout` also clones the time handle (TLS lookup, `Rc` clone, a copied `Instant`) per timer.
8. **The named futures move after the SQE is pushed.** They are `Unpin` and returned by value. An SQE may therefore
   point only at heap memory the op data owns, never at the op data itself. This matters for A1 (inline `msghdr`).
9. **`SendFuture` can already resolve without an op**: `State::Failed(e, buf)` returns on the first poll. A
   `State::Ready(result, buf)` gives S1 its "no `Pending`" path without a new future type.
10. **The buffer ring knows its demand**: `in_use` is always kept, and each `take` consumes one kernel-visible
    buffer, so "free buffers visible to the kernel" is `published − in_use` without asking the kernel.
    `recycle` (`driver/buf_ring.rs:753`) is the only place a buffer goes back.
11. **`RecvMultiOp` already re-arms** (`OnEnobufs::Rearm`, `driver/op/recv_multi.rs`); `RecvStream::next` just does
    not use it. B2 is a small addition on top.
12. **io-uring 0.7.15 has what K1 needs**: `opcode::SetSockOpt`, `opcode::Shutdown`, `squeue::Flags::SKIP_SUCCESS`.
13. **Stream methods already live in one file per topic** (`net/tcp/{fixed,deadline,zero_copy,cancel}.rs` are
    `impl TcpStream` blocks). New methods follow that pattern, which keeps the packages' files apart.

## Kernel statements to verify in wave 0

| id | statement (from memory) | matters for | how |
|---|---|---|---|
| KV1 | A `DEFER_TASKRUN` ring runs at most max(20, `min_complete`) deferred task-work items per run (`IO_LOCAL_TW_DEFAULT_MAX`, 6.12+) and sets `IORING_SQ_TASKRUN` again when items are left | D1: one drain enter handles at most 20 completions; `max` must be documented in those units | read `io_uring/io_uring.c` at v7.0; probe: 200 ready receives, count CQEs per enter |
| KV2 | If a link member with `CQE_SKIP_SUCCESS` fails, the kernel posts its CQE and **skips** the CQEs of the cancelled members after it (`REQ_F_SKIP_LINK_CQES`). ROUND3.md assumes the opposite ("the receive's CQE always arrives") | C1 slot bookkeeping. If true, a pair posts exactly one CQE in every case | probe: SEND(skip, link) + RECV; cases: send ok, peer reset, short send on a full socket, invalid SQE at prep |
| KV3 | `MSG_WAITALL` on a stream SEND retries a short send in the kernel and fails the link if it stays short | C1: "skipped" must mean "fully sent" | probe with a full socket buffer |
| KV4 | The kernel copies the `msghdr` and imports the iovec when it prepares the SQE (at `io_uring_enter`), not when the SQE is pushed | A1: lifetime of the `msghdr` | read `io_uring/net.c` at v7.0 |
| KV5 | `IORING_SEND_VECTORIZED` (vectored plain SEND, no `msghdr`) exists from 6.17 | A1 alternative | read the uapi header; probe |

A design below that depends on a statement is fixed only after its probe.

## Efficiency alternatives found while reading

"Adopt" means the alternative is clearly cheaper and replaces the code. "Measure" means quick-measure first.

| id | where | today | alternative | decision, package |
|---|---|---|---|---|
| E1 | `time/driver/entry.rs` | `CachePadded` (128-byte alignment) around the waker and the driver state of every timer | remove the padding; one runtime thread cannot false-share | adopt, r3-timer; assert `size_of::<Sleep>()` in a test |
| E2 | `UringInner::push_op` | three SQ views per op | one view: check `is_full`, else the cold `reserve_sqes`; then push | adopt, r3-op |
| E3 | `OpFuture::poll_result`, `Op` | `State::Done` assignment drops the `Op` shell through a call | move the op out by value and consume it, so `Done` drops nothing | adopt, r3-op |
| E4 | `Op::driver` | `Rc` clone and drop per op | resolve the driver through `CURRENT`, or keep a pointer that is valid while the slot exists | measure, r3-op; adopt only if ≥ 1% of user instructions and an op dropped after the runtime ended stays safe |
| E5 | op slot (`lifecycle.rs`, `ignored.rs`, `slab.rs`) | 72 bytes, two cache lines | 64 bytes at 64-byte alignment. Costs inline storage for dropped ops (48 → 40 bytes: `Bytes`/`BytesMut` ops would be boxed when dropped in flight) | measure, r3-slot: cycles and cache misses per frame at 2000 connections against a drop-heavy case (timeouts firing). Round 1 added the inline storage on purpose, so the lead confirms in a quiet slot |
| E6 | `AsyncWriteRentExt::write_all` | wraps the buffer in a `Slice` and loops even when the first send takes everything | first send on the plain buffer, loop only after a short write | measure, r3-op (a second await point grows the future) |
| E7 | S1 | ROUND3.md proposes a new `SendEagerFuture` | reuse `SendFuture` with `State::Ready`; carry a partial eager send as an offset inside `Send<T>` (fits in existing padding) | adopt, seed commit + r3-eager |
| E8 | A1 | ROUND3.md proposes the `msghdr` inline in the op | unsound by finding 8 if KV4 holds. Use a per-thread free list of boxed `msghdr`s: no allocation in steady state, stable address | adopt, r3-vec; vectored SEND (KV5) as a follow-up to measure |
| E9 | C1 | ROUND3.md: slot waits for one or two CQEs, told apart by order | if KV2 holds: always one CQE. Tag the head's `user_data` (bit 61) so the dispatcher knows which member posted it; the hot path keeps one test (`>> 61 == 0`) | decided by the probe, r3-pair |
| E10 | B1 | ROUND3.md: window-based grow/shrink policy | keep retired buffers on a LIFO stack and publish from its top, so the published set is always the most recently used buffers | adopt, r3-bufring |

---

## Seed commit (lead, before wave 0)

Creates every shared declaration once, so that no agent edits a registry file beyond its own `pub use` line.
- `docs/performance-improvement/round3/README.md`.
- Empty module files with their `mod` lines:
  - `net/tcp/{eager,pair,sockctl,vectored}.rs` and the same four in `net/unix/`;
  - `driver/op/{send_recv,sockopt}.rs`, `driver/msghdr_pool.rs`, `time/deadline.rs`.
- `io/futures.rs` (E7 groundwork): `State::Failed(e, buf)` becomes `State::Ready(io::Result<usize>, buf)`, with a
  `pub(crate) fn ready(res, buf)` constructor on `OpFuture` and on each named future. No behaviour change.
- `monoio/Cargo.toml`: `[[example]] name = "reqresp"`, `path = "examples/reqresp/main.rs"`, with a placeholder
  `main.rs`.
- Tag `r3-base`.

## Wave 0 (3 agents in parallel): reproducer, counters, kernel probes

| package | items | owns |
|---|---|---|
| r3-repro | X1 (reproducer) | `monoio/examples/reqresp/**` |
| r3-stats | X1 (counters) | `runtime_stats.rs`; metrics-only lines in `driver/uring/mod.rs` and `driver/uring/lifecycle.rs`; tests `runtime_stats.rs`, `driver_stats.rs`, `metrics_off.rs` |
| r3-probe | KV1–KV5 | new `monoio/tests/kernel_probe_r3.rs`; `round3/r3-probe.md` holds the answers |

Merge order: r3-stats, r3-repro, r3-probe.

**r3-repro (`reqresp` example)**
- Layout, one file per part, so later packages each fill exactly one file:

  | file | content | filled by |
  |---|---|---|
  | `main.rs` | arguments, client threads, measurement, output | r3-repro |
  | `paths/plain.rs` | `read`, then `write_all` (two loop rounds per frame) | r3-repro |
  | `paths/pipe.rs` | `send_op` + `recv_poll_first_op` queued together (today's `pipeack`) | r3-repro |
  | `paths/mshot.rs` | `recv_multi` on a `BufRing`, reply by `send_op` | r3-repro, then r3-bufring (`--wset`) |
  | `paths/eager.rs` | stub ("not available"), then the S1 path | r3-eager |
  | `paths/pair.rs` | stub, then the C1 path | r3-pair |
  | `ring_ext.rs` | stub for extra `--ring` items, then `drain<N>` | r3-drain |
  | `deadline.rs` | `none`, `timeout`; then `deadline` (T1) | r3-repro, then r3-timer |
  | `accept.rs` | `accept_batch_into`; then `--direct` (K1) | r3-repro, then r3-direct |

- One server runtime pinned to `--cpu`; client runtimes on `--client-cpus`. Loopback, one process.
- Load: `--conns N` closed-loop clients, each sends a frame of `--sizes 4k,16k,64k` (cycled) and waits for a
  70-byte reply. `--bulk M --chunk 32k`: M connections on which the server writes without pause (the MIX
  neighbour).
- Options: `--path`, `--deadline`, `--ring coop,defer,skipenter,ringfd,fixedfd,minwait<us>`, `--entries`, `--cq`.
- Output: frames/s, frames per CPU-second, reply p50/p99/max, the longest gap any single connection saw (for D2),
  bulk MB/s, the per-frame hardware counters (code taken from `smallframes`), and with `metrics` the deltas of
  both stats structs plus "task polls per frame" and "RECV same-tick share".
- Done when: `--conns 8`, `--conns 2000 --sizes 4k` and `--conns 9 --bulk 2` run and print the line.

**r3-stats**
- `RuntimeStats::ops_completed`: single-shot CQEs that woke a waiting waker (counted in `Ref::complete`'s
  `Waiting` arm through a `runtime_stats::note_*` call, like `note_remote_wakes`).
- `DriverStats::enter_left_taskrun`: waiting enters after which `sq.taskrun()` was set when `tick()` finished.
  Update `DRIVER_STATS_U64S`.
- The split of `op_ticks_by_opcode` bucket 0 is skipped, as ROUND3.md suggests.

**r3-probe**
- The probes of the KV table as `#[ignore]` tests that print what they see (like `fs_punt_probe.rs`).
- Reads the kernel source at the v7.0 tag (`io_uring/io_uring.c`, `io_uring/net.c`, the uapi header) and quotes
  the lines that decide each statement.

### Gate G0 (lead, no agent active; in the fork, no bench run needed)

1. **Kernel statements**: KV1–KV5 answered in `G0.md`, with the consequence for D1, C1 and A1.
2. **D1 reproduced locally**: `reqresp --conns 9 --bulk 2 --path pipe`, `coop` against `defer`: same-tick share and
   frames/s should show the 96% / 52% split of ROUND3.md. If not, the reproducer is fixed before wave 1.
3. **D2 diagnosed**: `reqresp --conns 2000 --sizes 4k --path pipe`, `coop` against `defer`, `metrics` build.
   - Record: rounds/s, CQEs-per-tick histogram, polls per round, ready-queue high water, `sq_full_flushes`,
     `cq_overflow`, `enter_skipped`, `enter_left_taskrun`, and the longest per-connection gap.
   - Test the candidates, cheapest first:
     a. SQ pressure (finding 3): rerun with `--entries 16384`. If the gap disappears, the cause is the mid-round
        flush; look at what `reserve_sqes` does to ordering.
     b. Late CQEs (finding 2): a scratch build whose `tick()` repeats while the CQ is not empty (bounded).
     c. Neither: trace one starved connection in the kernel (`io_uring:io_uring_complete`,
        `io_uring:io_uring_task_work_run` with `bpftrace`), to see whether its receive completes late or is
        delivered late.
   - Result: the mechanism with numbers, and either a fix for r3-drain's prompt or the rule for AGENTS.md.
4. **Baselines** for every later A/B: user instructions and cycles per frame for `plain` and `pipe` at 8 and 2000
   connections, with and without `--deadline timeout`.

---

## Wave 1 (7 agents in parallel)

| package | items | owns |
|---|---|---|
| r3-slot | E5 | `driver/uring/lifecycle.rs`, `driver/uring/ignored.rs`, `utils/slab.rs`; test `ignored_inline.rs` |
| r3-op | O1, E2, E3, E4, E6 | `driver/op.rs`, `driver/op/submit.rs`, `driver/mod.rs`, `io/futures.rs`, `io/async_write_rent_ext.rs`; in `driver/uring/mod.rs`: `reserve_sqe(s)`, `push_op`, `push_chain`, `push_op_with_timeout`, the `submit_*` functions, `poll_op`, `drop_op` |
| r3-drain | D1, D2 fix if it is in the driver | `builder.rs`, `driver/raw.rs` (`set_min_wait`); in `driver/uring/mod.rs`: `DriverOptions`, `EnterConfig`, `inner_park`, `Driver::submit`, `tick`, `nothing_to_enter`, `enter`; test `drain_taskrun.rs`; `reqresp/ring_ext.rs` |
| r3-bufring | B1, B2, E10 | `driver/buf_ring.rs`, `driver/op/recv_multi.rs`, `net/tcp/multishot.rs`, `net/unix/multishot.rs`; tests `buf_ring_working_set.rs`, `multishot_recv.rs`; `reqresp/paths/mshot.rs` |
| r3-eager | S1, E7 | `driver/shared_fd.rs`, `driver/op/{send,recv,send_zc,read,write,fixed}.rs`, `net/tcp/eager.rs`, `net/unix/eager.rs`, `net/tcp/split.rs`, `net/unix/split.rs`; the `write`/`writev` bodies in `net/tcp/stream.rs` and `net/unix/stream.rs`; test `eager_send.rs`; `reqresp/paths/eager.rs` |
| r3-direct | K1 | `net/tcp/sockctl.rs`, `net/unix/sockctl.rs`, `net/tcp/stream.rs` and `net/unix/stream.rs` except `write`/`writev`, `net/tcp/listener.rs`, `net/unix/listener.rs`, `driver/op/{shutdown,accept_multi,sockopt}.rs`; test `direct_socket.rs`; `reqresp/accept.rs` |
| r3-timer | T1, E1 | `time/**`; test `deadline_timer.rs`; `reqresp/deadline.rs` |

- `driver/uring/mod.rs` is shared by r3-op and r3-drain by function, as listed. `net/*/stream.rs` is shared by
  r3-eager (two function bodies) and r3-direct (the rest).
- Merge order: r3-slot, r3-op, r3-drain, r3-bufring, r3-eager, r3-direct, r3-timer.
- Known requested changes the lead applies at merge: the receive guard field in `RecvMulti` (r3-eager needs it,
  r3-bufring owns the file) and the send guard in `File::read_at_then_send`.

**r3-slot (E5)**
- Build the 64-byte, 64-byte-aligned slot as one commit: inline storage for dropped ops 48 → 40 bytes, entry
  stride 64, `Slab::prefetch` fetches one line.
- Screening numbers: `smallframes` and `reqresp` at 2000 connections (instructions, cycles, cache misses per
  frame), and a drop-heavy case (a timeout firing on an in-flight receive with a `BytesMut`, allocations counted).
- The lead confirms in a quiet slot at the merge and keeps or drops the commit. If dropped, the package still
  delivers anything else it found in the three files.

**r3-op (O1 and the alternatives)**
- E2 and E3 first (adopt), then E6 and E4 by quick measurement, each as its own commit.
- Check with `perf annotate` on `smallframes` that `drop_glue::<State<..>>` is gone from the profile after E3.
- Done when: `smallframes` at 2000 × 4 KiB shows user instructions per frame down against `r3-wave0` with the same
  frame counts, and no API changed. The ≥ 5% target of O1 is judged on the whole wave at G1 (r3-slot and r3-timer
  contribute).

**r3-drain (D1)**
- `RuntimeBuilder::drain_taskrun(max: u32)`, stored in `DriverOptions`. Active only on a ring with
  `DEFER_TASKRUN` and `TASKRUN_FLAG`; otherwise ignored (documented).
- In `inner_park`, after the first `tick()`:
  `while n < max && sq.taskrun() { enter(0, None)?; tick()?; n += 1 }`, then `drop_deferred` and `deliver` once.
- The doc comment states what one drain enter can do (KV1: at most 20 completions on 6.12+).
- Counter (`metrics`): `drain_enters`.
- `monoio::uring::set_min_wait(wait, batch)`: changes `EnterConfig::{min_wait_usec, min_wait_batch}` at run time
  (two stores; needs `IORING_FEAT_MIN_TIMEOUT`, else `Unsupported`).
- Not done: the generic `after_park` hook. `drain_taskrun` covers the need; a callback on the park path would cost
  every user an indirect call.
- If G0 shows that a larger backlog is left behind at many connections: a second commit with a single
  non-blocking drain, `want = cq.len()` (returns at once because that many CQEs already exist, and raises the
  kernel's cap to that number), for the lead to compare against the loop. Depends on KV1.
- The D2 fix, if G0 found one in the driver.
- `reqresp --ring drain<N>`.
- Tests: on a `defer` ring, receives that complete after the wait returned are delivered before the tasks run
  again (`metrics`: `drain_enters > 0`); `max = 0` and non-DEFER rings behave as today.

**r3-bufring (B1, B2)**
- B1, manual part: `BufRing::published()`, `grow_published(n)`, `shrink_published(n)`.
  - Retired buffers sit on a LIFO stack in `Shared`. `recycle` pushes a buffer there instead of writing a ring
    slot while the published count is above its target. `grow_published` pops from the top (the most recently
    used buffers) and publishes them.
  - `BufRingOptions::working_set(initial, step)`: publish `initial` at creation, the rest starts on the stack.
- B1, automatic part (only with `working_set`): grow by `step` when a receive ends with `ENOBUFS` or when a `take`
  leaves no free buffer visible to the kernel; shrink toward `initial` when the high-water mark of `in_use` over
  the last `published × 4` takes stayed below half the published count. One compare per `take`; nothing on a ring
  without `working_set`.
- Incremental rings: the same rule applied when a whole buffer is recycled.
- B2: `RecvStream::next_rearm()` and `RecvBundleStream::next_rearm()` (`OnEnobufs::Rearm`, then wait on the ring
  when it is still empty), `rearm_on(&BufRing)`, `ring()`, and `BufRing::wait_free()` (a waiter list in `Shared`,
  woken from `recycle`; ready at once when a buffer is visible).
- `reqresp --path mshot --wset 16,16`.
- Tests: growth on `ENOBUFS` happens once per demand level (`enobufs` stops growing); shrink retires and reuses
  the hottest buffers; the B2 test of ROUND3.md (ring smaller than the number of connections: all data in order,
  bounded polls per chunk, no `ENOBUFS` from `next_rearm`).

**r3-eager (S1)**
- `SharedFd`: `tx_in_flight` and `rx_in_flight` (`Cell<u32>`), and the eager flag. A guard field in the op data
  of every socket send (`Send`, `SendMsgVec`, `SendZc*`, `WriteVec`/`Write` made by a stream, `WriteFixed`) and
  receive (`Recv`, `ReadVec`) raises the counter at submit and lowers it when the op data is dropped. That is
  after the CQE in every case (a dropped in-flight op keeps its data in the slot until then), so the counter can
  only be late, never early.
- `try_send(&self, &[u8])`, `try_send_vectored(&self, &[IoSlice])`, `try_recv(&self, &mut [u8])` on `TcpStream`
  and `UnixStream`: `WouldBlock` when the matching counter is not zero or the stream is direct-only; else one
  `send`/`sendmsg`/`recv` with `MSG_DONTWAIT` (sends add `MSG_NOSIGNAL`).
- `send_eager(&self, buf) -> SendFuture<T>` (E7): everything sent, or a hard error: `SendFuture::ready`. Partly
  sent or `WouldBlock`: ring SEND for the rest (with `POLL_FIRST` after a real `EAGAIN`, without it when the eager
  path was skipped), the eager byte count added to the result through an offset in `Send<T>`.
- `set_eager_send(bool)` per stream (the flag lives in `SharedFd`, so split halves see it): `write` and
  `write_all` go through `send_eager`. Off by default.
- Docs: raw SQEs on the same socket are not counted (do not mix them with `try_send`); large writes that usually
  fill the socket buffer pay a wasted syscall; on `coop_taskrun` rings each direct syscall also runs pending task
  work (ROUND3.md saw −5% there), so it is meant for `defer_taskrun`.
- `reqresp --path eager`.
- Tests: `try_send` during an in-flight ring send returns `WouldBlock`; the same for a direct-only stream; order
  of bytes with eager and ring sends mixed (1 MiB of numbered chunks through a small socket buffer); the future
  is ready on its first poll when the send fits.

**r3-direct (K1)**
- `TcpStream::shutdown_with(&self, std::net::Shutdown)` and `shutdown_both(&self)` (also `UnixStream`): a direct
  syscall with an fd, `IORING_OP_SHUTDOWN` for direct-only.
- `set_sockopt(level, name, &value)` / `get_sockopt` through `opcode::SetSockOpt` for direct-only streams, used by
  the existing setters (`set_nodelay`, `set_tcp_keepalive`, ...). `Unsupported` before 6.7.
- `TcpListener::accept_batch_direct_into(&mut vec, max, flags)` and the `UnixListener` twin, on the multishot
  direct accept (needs `with_fixed_files_kernel_alloc`).
- `reqresp --direct` (direct accept, `shutdown_with` from a sweeper task).
- Tests: each new call on a direct-only stream; `shutdown_with` from another task while a receive is in flight.

**r3-timer (T1)**
- E1: remove the padding. Test: `size_of::<Sleep>()` and its alignment.
- `time::Instant::recent()`: the driver stores the clock value whenever it reads the clock anyway (park,
  `process_if_due`) and clears a "fresh" flag once per park and per cold-path submit. `recent()` returns the stored
  value while fresh, else reads the clock once and stores it. Cost when unused: one store per round.
- `time::Deadline` (`new`, `arm`, `disarm`, `poll_expired`) and `time::with_deadline(d, fut)`, as ROUND3.md
  specifies, on the existing lazy reset (`extend_expiration`):
  - `arm` uses `recent()`; one wheel insert when the entry is not registered or the new time is earlier;
  - `disarm` clears a flag and takes the waker, so an expiry while disarmed wakes nobody;
  - `poll_expired` is ready only when armed and fired.
- `time::timeout_coarse(d, fut)`: `timeout` with `recent()` instead of a clock read. `timeout()` keeps the exact
  clock: its deadline must not come early after a long task poll.
- Timers that die young: an entry registered at wheel tick `t` goes to an intrusive "fresh" list. It moves into
  the wheel when the driver parks (the park needs the next expiry) or processes time at a tick later than `t`.
  Dropped before that, it costs a list unlink. A deadline at or before the wheel's elapsed tick fires at once, as
  today. Its own commit; quick-measure with `smallframes` (one `timeout` per frame); keep only if instructions
  per frame drop.
- `reqresp --deadline deadline`.
- Tests: the three of ROUND3.md (disarmed never fires; `arm` after an expiry while disarmed; `recent()` lags by at
  most one park), plus short timers on the fresh list fire on time with a busy and with an idle loop.

### Gate G1 (lead, no agent active; bench repo, 3 rounds, commands of ROUND3.md "How to reproduce")

First the quiet-slot confirmations (E5, the drain variant, the fresh list if its numbers were close), then:

| item | load | compare | passes when |
|---|---|---|---|
| D1 | MIX | `defer…:drain4` (no `minwait`) against `$FEW` | repl frames/s within 3%; ack p99 inside `$FEW`'s min–max; `metrics` run: RECV same-tick ≥ 90% |
| D1 | R2000 | `$MANY:drain4` against `$MANY` | frames/s not more than 2% below; p99 ranges overlap |
| D1 | R8 | `$MANY:drain4` against `$MANY:minwait80` | frames/s not below |
| S1 | R8 | `defer:skipenter:pipeack:sweep:ringfd:fixedfd:eagerack` against the same without | frames/s within 2%; ack p99 range not above tokio's |
| S1 | R2000 | `$MANY:eagerack` against `$MANY` | within 2% |
| D2 | R2000 | `coop…:pipeack:sweep…` after the fix | ack p99 within 2× the `defer` value, or the rule is in AGENTS.md with the numbers |
| T1 | R2000 | `…:pipeack:deadline` against `…:pipeack:sweep` | within 1% |
| O1 | R2000 (or `smallframes`) | wave-1 tree against 0.8.0 | user instructions per frame −5%, results unchanged |
| B1 | R8 | all connections on `mshot`, one ring 4096 × 16 KiB with `working_set(16, 16)`, against the `pipeack` path | frames/s within 2% |
| B1 | R2000 | the same ring against `$MANY` | within 2%; `enobufs` stops growing after the ramp |
| K1 | R2000 | direct accept against `fixedfd` | within 1% or better; one syscall less per connection (`strace -c` during the ramp) |

---

## Wave 2 (3 agents in parallel, plus retries of wave 1)

| package | items | owns |
|---|---|---|
| r3-pair | C1, E9 | `driver/uring/lifecycle.rs`, `driver/chain.rs`, `driver/op/send_recv.rs`, `net/tcp/pair.rs`, `net/unix/pair.rs`; in `driver/uring/mod.rs`: `handle_cqe` and a new `push_pair`; test `send_then_recv.rs`; `reqresp/paths/pair.rs` |
| r3-vec | A1, E8 | `driver/op/send.rs` (`SendMsgVec`), `driver/msghdr_pool.rs`, `net/tcp/vectored.rs`, `net/unix/vectored.rs`, `io/futures.rs` (one `named_future!`); tests `named_futures.rs`, `vectored_alloc.rs` |
| r3-docs | W1, AGENTS.md for wave 1 | `README.md`, `AGENTS.md`, `docs/TODO.md`, doc comments only in `driver/buf_ring.rs` |

Merge order: r3-pair, r3-vec, r3-docs. These wait for wave 1 because r3-pair builds on the slot layout r3-slot
settled and compares against the eager path, and r3-vec edits the struct r3-eager gave a guard.

**r3-pair (C1)**. The design is fixed by KV2 and KV3 (in `G0.md`).
- If KV2 holds (expected): `send_then_recv` uses **one slot and posts one CQE**.
  - SEND: `IO_LINK | CQE_SKIP_SUCCESS`, `MSG_WAITALL`, `user_data = index | 1 << 61`.
    RECV: `POLL_FIRST`, `user_data = index`.
  - CQE without the tag: the receive's result; the send succeeded. CQE with the tag: the send failed (the
    receive was cancelled without a CQE). Either one completes and frees the slot.
  - The dispatcher in `handle_cqe` tests `>> 61 == 0` for plain ops (same cost as today's `>> 62`) and sends the
    tagged case to a cold function.
  - Result: `(Ok(reply_len), recv_result, reply, buf)` or `(Err(e), Err(ECANCELED), reply, buf)`.
- If KV2 does not hold: the two-CQE rule of ROUND3.md with the same tag bit (the slot is freed on the untagged CQE).
- `send_then_recv_fixed` (`READ_FIXED` as the second member).
- Dropping the future keeps both buffers until the pair's last CQE; with `async-cancel` both members are cancelled.
- `coop_budget` counts the pair as one result.
- Raw chains (`submit_chain` accepting `SKIP_SUCCESS` on all but the last member): only if the probe showed one
  simple rule for which members post a CQE after a failure. Otherwise `validate` keeps rejecting it and the doc
  comment says why.
- `reqresp --path pair`.
- Tests: those of ROUND3.md (peer reset gives the send error and `ECANCELED`, slot freed, nothing leaks; drop in
  flight keeps the buffers), plus a full socket buffer (KV3), plus `metrics`: task polls per frame ≤ 1.1.
- Note for the gate: S1 already reaches one completion and one wake per frame at the price of one syscall per
  reply. G2 therefore compares `pair` against both `pipe` and `eager`; the docs then say which to use when.

**r3-vec (A1)**
- `send_vectored_op(&self, bufs)` and `send_vectored_with_flags_op` on `TcpStream`, `UnixStream` and the owned
  halves (`named_future!` plus a `BufOp` impl for `SendMsgVec`).
- E8: `SendMsgVec::msg` comes from a per-thread free list of `Box<libc::msghdr>` (`driver/msghdr_pool.rs`) and
  returns to it when the op data is dropped. Check that `SendMsgVec<IoVecArray<..>>` then fits the inline storage
  for dropped ops (48 bytes, or 40 if E5 was kept).
- If KV5 holds: quick-measure a vectored plain SEND against `sendmsg`; its own commit, adopted on 6.17+ with a
  fallback only with numbers.
- Tests: `named_futures.rs` cases; an allocation-counting test (`#[global_allocator]` in its own test binary) that
  1000 vectored sends after warm-up allocate nothing.

**r3-docs**
- W1: `README.md:23` (version), `README.md:71` (registered buffers exist), the stale half of the `batch_publish`
  doc comment (the driver does call `flush_pending`).
- AGENTS.md for everything wave 1 merged: `drain_taskrun`, `set_min_wait`, `try_send` / `send_eager` /
  `set_eager_send`, `working_set`, `next_rearm` / `wait_free`, `Deadline` / `with_deadline` / `timeout_coarse` /
  `Instant::recent`, `shutdown_with`, ring socket options, `accept_batch_direct_into`, the two new counters, the
  `reqresp` example, the per-connection and per-op cost of direct-only streams (K1), and the three patterns of
  ROUND3.md W1, updated with what G0 and G1 found about `coop` and `defer`.
- `docs/TODO.md`: remove what the round closed.

### Gate G2 (lead, no agent active; bench repo, 3 rounds)

| item | load | compare | passes when |
|---|---|---|---|
| C1 | R8, R2000 | pipelined path on `send_then_recv` against `send_op` + `recv_poll_first_op`, and against `eagerack` | frames/s not below the pair of ops; `metrics`: task polls per frame ≤ 1.1 |
| A1 | R8 | `gk-monoio` facade on `_op(&self)` methods, no `into_split` | not below the current build; no allocation per write |

## Release and gate G3 (lead)

- AGENTS.md additions for wave 2 (`send_then_recv`, `send_vectored_op`), `docs/RELEASES.md` for 0.9.0 (breaking
  with `metrics` only: new `DriverStats` fields), version bump in its own commit, an item-status table in
  ROUND3.md, the run docs handed back to the bench repo.
- **G3, final tree without `metrics`:**
  - R8, R2000, MIX against 0.8.0 with the configurations of ROUND3.md's first table: no loss by the 8% rule.
  - **One configuration for all three loads** (problems 1 and 2 of ROUND3.md): `defer:skipenter:pipeack:…:drain4`
    with the eager ack: MIX within 3% of `$FEW`, R2000 ties `$MANY`, R8 ack p99 not above tokio's range. If it
    passes, AGENTS.md recommends it. If not, `G3.md` says which part still needs two configurations.

## Bench-side changes (`../gatotkaca-transport`, lead)

The gates need new `--tune` items in `crates/sut-monoio/src/main.rs` and the `[patch.crates-io]` entry pointing at
this tree:

| tune item | uses | gate |
|---|---|---|
| `drain<N>` | `RuntimeBuilder::drain_taskrun(N)` | G1 |
| `eagerack` | ack through `send_eager` (replaces the hand-written `syncack`) | G1 |
| `deadline` | `with_deadline` instead of `time::timeout` / the sweeper | G1 |
| `wset` | one `BufRing` with `working_set(16, 16)` for `mshot` (replaces hot + spill) | G1 |
| `direct` | `accept_batch_direct_into`, `shutdown_with` in the sweeper | G1 |
| `pairack` | `send_then_recv` | G2 |
| facade | `gatotkaca-transport-monoio` on the `_op(&self)` methods | G2 |

That repo has uncommitted work (ROUND3.md says so). See "Open decisions".

## Dependencies at a glance

```
seed (r3-base)
  ├─► r3-repro ─┐
  ├─► r3-stats ─┼─► merge (r3-wave0) ─► G0: KV1–KV5, D1 reproduced, D2 diagnosed, baselines
  └─► r3-probe ─┘                          │
                                           ▼
  r3-slot · r3-op · r3-drain · r3-bufring · r3-eager · r3-direct · r3-timer     (7 worktrees)
                                           │
                    merge (r3-wave1) ─► quiet-slot confirmations ─► G1
                                           │
                                           ▼
                      r3-pair · r3-vec · r3-docs  (+ retries of wave 1)
                                           │
                    merge (r3-wave2) ─► G2 ─► release 0.9.0 ─► G3
```

- At most 7 agents run at once. Gates are serial, with no agent active.
- r3-drain needs the D2 result and KV1; r3-pair needs KV2, KV3 and the merged slot layout; r3-vec needs KV4 and
  r3-eager's guard. Everything else in wave 1 needs only the wave-0 tree.

## Open decisions (my default in brackets)

1. **Bench repo edits.** May the lead add the `--tune` items above to `sut-monoio` and point its
   `[patch.crates-io]` at this tree, without committing there? [yes; otherwise a patch per gate for you to apply]
2. **Gate runs.** They need a quiet machine and the `performance` governor. [the lead runs them when you say the
   machine is free; the next wave starts after the gate]
3. **Scope if time is short.** [G1 is the cut line: everything in waves 0 and 1 is a releasable 0.9.0; wave 2 can
   follow as 0.9.x]

## Status

| wave / gate | state |
|---|---|
| seed | not started |
| wave 0 | not started |
| G0 | not started |
| wave 1 | not started |
| G1 | not started |
| wave 2 | not started |
| G2 | not started |
| release, G3 | not started |
