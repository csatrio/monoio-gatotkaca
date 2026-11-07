# r3-pair: reply-then-receive as one operation (C1, E9)

Branch `r3/r3-pair` on `r3-g1` (`7f1864d`). Two commits:

| commit | what |
|---|---|
| `20a5834` `feat(net): send_then_recv, one slot and one CQE per reply+receive pair (r3-pair: C1)` | the pair: driver tag and dispatch, op data and futures, stream methods, `reqresp --path pair`, `tests/send_then_recv.rs` |
| `bb86c49` `feat(uring): submit_chain accepts the skip shape, one slot and one CQE per chain (r3-pair)` | item 6: `CQE_SKIP_SUCCESS` on every SQE but the last, `uring::link_skip`, `ChainOp` on one slot, `tests/chain_raw.rs`; the tag gains the member position |
| (third) `docs(round3): r3-pair notes` | this file |

Files changed, all in the package's list unless noted: `driver/uring/mod.rs` (`handle_cqe` and its new
cold arm `complete_skipped_member`, the tag constants and helpers next to `LINK_TIMEOUT_USERDATA`,
`push_pair`/`submit_pair`, `push_shared_chain`/`submit_shared_chain`, `drop_shared`, `cancel_shared`
next to `push_op_with_timeout`, `Ops::prefetch*`), `driver/chain.rs`, `driver/raw.rs` (the
`submit_chain` docs and one `pub use` of `link_skip`), `driver/op/send_recv.rs`, `net/tcp/pair.rs`,
`net/unix/pair.rs`, `examples/reqresp/paths/pair.rs`, `tests/send_then_recv.rs` (new),
`tests/chain_raw.rs`. `pub use` lines: `net/tcp/mod.rs` and `net/mod.rs` (`SendThenRecv`,
`SendThenRecvFixed`, `PairResult`). Outside the list, applied locally: `examples/reqresp/paths/mod.rs`
loses `not_available` (dead once the last stub is filled; see "Requested changes").
**`lifecycle.rs` is unchanged**: the design needs no new slot state (below).

All numbers: this machine, other agents building ("screening, machine not quiet"); user instructions
per frame are exact.

## Short answer

- `TcpStream`/`UnixStream::send_then_recv(reply, buf, recv_flags)` and `send_then_recv_fixed(reply,
  FixedBuf)`: a `SEND(IO_LINK | CQE_SKIP_SUCCESS, MSG_NOSIGNAL | MSG_WAITALL)` linked to a
  `RECV(POLL_FIRST)` (or `READ_FIXED`), pushed back to back on **one op slot**, resolved by **one CQE**
  (KV2), one task wake. The head's `user_data` is tagged (bit 61); the dispatcher's hot path tests
  `>> 61 == 0` instead of `>> 62 == 0` (same cost) and sends the tagged case to a cold function.
- In the reproducer the pair costs **1,229** user instructions per frame against 1,200 for `eager`
  and 1,809 for `pipe` (2000 connections, 4 KiB, `defer`; exact), with 1.000 task polls and 1.000
  CQEs per frame (`metrics`). Frames/s: a tie with `eager` at 8 connections on both rings, +1.5% on
  `coop` at 2000 connections, **−4% on `defer` at 2000 connections** (−3.6% with `drain2`): on a
  `DEFER_TASKRUN` ring the kernel starts the linked receive from a deferred task-work item, so a pair
  costs two items per frame and a park's run of 20 items carries 20 frames instead of 40. Beats
  `pipe` everywhere but there (p99 at 8 connections 170 → 119 µs).
- Raw chains: `submit_chain` accepts the skip shape (`IO_LINK | CQE_SKIP_SUCCESS` on all but the last,
  `uring::link_skip`) on one slot with one CQE; mixed shapes stay rejected.

## Design

### The slot and the tag

Both SQEs of a pair are pushed with one reservation (`push_pair`, like `push_op_with_timeout`) and
**one slab slot** of `OpKind::Single`: the tail (the receive) with `user_data = index`, the head (the
send) with `user_data = SKIP_MEMBER_TAG | member << 32 | index` (`tagged_user_data`, bit 61, member 0
for a pair). The slot goes through the normal `Submitted → Waiting → Completed` path; the one CQE that
arrives completes it and the future's `Ready` poll frees it.

Why bit 61 is free: slab indices are small; `uring::user_data` ids are `0b10` in the top two bits
(`>> 61` = `0b100`/`0b101`); doorbells are `0b1101`/`0b1110` in the top four (`>> 61` = `0b110`/`0b111`);
the driver's own cancel/timeout/eventfd/link-timeout values have every top bit set. `Slab::get` on a
tagged key returns `None` (r3-slot), so the dispatcher strips the tag before the lookup.

### Dispatch (`handle_cqe`)

```text
_ if index >> 61 == 0  => plain op (the tail of a pair lands here)      // hot path: one test, as before
_ if index >> 61 == 1  => complete_skipped_member(index, cqe)           // #[cold] #[inline(never)]
_ if index >> 62 == 0b10 => external (uring::user_data)
...
```

`complete_skipped_member` strips the tag, takes the member position, and completes the slot through
the same `Ops::complete` with the CQE flags replaced by `tagged_cqe_flags(flags) | MEMBER_CQE_FLAG
(1 << 31) | member << 16`. `Ops::prefetch`/`prefetch_task` test `>> 61 == 0` too (a tagged CQE is rare
and not prefetched).

**"Which member posted" lives in the stored CQE flags, not in the slot's `Meta`.** The dispatcher
already hands `flags` to `complete`, and the future already gets them back in `CompletionMeta`, so
the mark costs the hot path of ordinary ops nothing: no byte in `Meta` (which has no spare byte with
`metrics` anyway, r3-slot), no new `Lifecycle` state, no new `OpKind`. Bits 16..31 of a CQE's flags
are the kernel's buffer-id field, which a `SEND`, a `RECV` without `BUFFER_SELECT`, and any failed
member never carry. `lifecycle.rs` is untouched.

### Every CQE case and what the caller sees

| CQE from | `res` | `(send, recv)` |
|---|---|---|
| tail (untagged) | `n >= 0` | `(Ok(reply_len), Ok(n))`; `n == 0` is EOF; `set_init(n)` on `buf` |
| tail | `-EAGAIN` | hidden: the future pushes a plain receive (`uring_op()` of the same op data: same `recv_flags`, `POLL_FIRST`) on a new slot and keeps waiting (`continue` in its poll loop). KV3: after 127 poll re-arms (a reply above ~512 KiB into a full socket) the send moved to io-wq and the linked `RECV(POLL_FIRST)` was issued there without a poll |
| tail | other error | `(Ok(reply_len), Err(e))` |
| head (tagged) | `< 0` | `(Err(e), Err(ECANCELED))`: nothing went out, the receive did not run. `-ECANCELED` on the head also stands for "the receive's SQE was rejected at prep" (KV2 d3) |
| head | `0 <= n < reply_len` | `(Ok(n), Err(ECANCELED))`: `n` bytes went out, then the send failed (the kernel reports the count, the errno is lost, KV3). Documented on the method as "treat as a broken connection" |
| submit failed (SQ stayed full) | — | `(Err(e), Err(ECANCELED), reply, buf)` on the first poll, nothing pushed |

`-EAGAIN` and the plain re-issue: on a `SendThenRecvFixed` the re-issued `READ_FIXED` has no
`POLL_FIRST` (that field is a real I/O priority on `READ*`: `EINVAL` at prep, and a rejected tail
makes the head report `-ECANCELED`, which the first test run caught). The fixed pair therefore
never sees `-EAGAIN` from the kernel-worker path; the worker blocks in the read until data arrives
(the probe's note on a `RECV` without `POLL_FIRST`).

### Lifecycle, cancel, drop

- Dropping the future in flight: `UringInner::drop_shared(index, data, members = 2)` =
  `drop_op(skip_cancel = true)` (the slot keeps `data` with both buffers and the fd guard until the
  CQE, as `Ignored`) then, with `async-cancel`, `cancel_shared`: one `AsyncCancel` per `user_data`
  value (the head's tagged one and the tail's), since a cancel by `user_data` finds only the member in
  flight (`-ENOENT` for the other, KV2 h1 / KV3 6). Both go out with one reservation; the slot is
  re-checked after it (a flush may have completed the pair).
- `cancel()` on the future: `cancel_shared`, then keep polling.
- `coop_budget`: `poll_proceed` once per poll of the future, `consume` once when it resolves: the pair
  (with a re-issued receive, if any) is one result.
- The op data holds one `PairFd(SharedFd)` that raises **both** in-flight counters (`begin_op::<true>`
  and `<false>`) and lowers both on drop: `try_send` and `try_recv` are refused while the pair is in
  flight, including after a drop until the CQE. One `Rc` clone per pair instead of two (`TxFd` +
  `RxFd`).
- Op data sizes: `SendRecv<&'static [u8], Vec<u8>>` is 56 bytes (fd 8, reply 16, buf 24, flags 4),
  `SendRecvFixed<&'static [u8]>` 72 (`FixedBuf` is 40). Both are above the slot's 48 bytes of inline
  storage, so a pair dropped in flight is **boxed** (one allocation; `ignored_boxed`). A pair that
  resolves allocates nothing. See "Open questions".

### The futures

`SendThenRecv<S: IoBuf, R: IoBufMut>` and `SendThenRecvFixed<S: IoBuf>` (`net::` and `net::tcp::`),
`Unpin + 'static`, output `PairResult<S, R> = (io::Result<usize>, io::Result<usize>, S, R)`, with
`poll_result(&mut self, cx)`, `is_terminated()`, `cancel()`, `Debug`, built by a local
`pair_future!` macro in `driver/op/send_recv.rs` over one `PairFuture<D: PairOp>` (states `Running {
driver, index, data }`, `Ready(..)` for a failed submit, `Done`). The op data types implement
`OpAble` (the tail SQE, also used for the re-issue) plus `PairAble::head_sqe` (the send) and `PairOp`
(`reply_len`, `into_parts` with `set_init`).

Not done: the owned halves. A pair is a send *and* a receive, so neither `OwnedReadHalf` nor
`OwnedWriteHalf` can own it cleanly; a user with split halves can keep the stream's `send_then_recv`
on the unsplit stream or reunite. Listed as an open question.

### Raw chains: the skip shape (second commit)

`validate` returns a `Shape`: `Plain` (no `SKIP_SUCCESS` anywhere: as today, one slot and one CQE per
SQE) or `Skip` (every SQE but the last has `IO_LINK | SKIP_SUCCESS` and not `IO_HARDLINK`, the last
has none of the three, and no `SKIP_SUCCESS` member is a `LINK_TIMEOUT`). Anything else is
`InvalidInput`, with a message that says which rule. `uring::link_skip([..])` sets the shape's flags.

A skip chain is pushed by `push_shared_chain` on **one slot**: member `i < N-1` gets
`tagged_user_data(index, i)`, the last `index`; the same dispatcher path as the pair reports the
failing member's position in the stored flags. `ChainOp` gets a `shared: bool`; its `poll` tests it
first and goes to `poll_shared`, which polls the one slot and fills the array: `Cqe::new(0, 0)` for
members before the one that posted (skipped = succeeded), that member's CQE (buffer-id field of its
flags stripped, documented), `-ECANCELED` for the members after it. `completed()` reports all-`None`
until then. `Drop` of a shared chain: `drop_shared(.., N)` (data in the slot, `N` cancels); no `Rc`.
The plain shape's poll and drop are the code as before plus one `if me.shared` test.

Why `IO_HARDLINK` is rejected in the skip shape: a hard link does not cancel the rest on failure, so
the last member would post a second CQE for the one slot, after it was freed (and maybe reused).
Why a `LINK_TIMEOUT` member is rejected: its CQE rules in a skip chain were not probed (a cancelled
timeout completes with `-ECANCELED`, a failure by `req_set_fail`'s definition, so it would likely post
a tagged CQE after the chain is done). Both are cheap to allow later with a probe.

### `reqresp --path pair`

`paths/pair.rs`: `pipe` with the reply and the next receive as one `send_then_recv` (a plain
`recv_op` for the first frame of a connection and when a partial frame needs more data). The send
result of a frame is known with the next frame's CQE and checked there (`Ok(70)` or the connection
ends with `WriteZero`). `frame_done` is counted when the pair is pushed.

## Tests

`tests/send_then_recv.rs` (20 tests, 21 with `metrics`; the peer of every test is a blocking std
socket on another thread, so the runtime runs only the pair): round trip on TCP and Unix (data
received, send reported full, both buffers back, the guards gone); reply already queued; EOF; `recv_flags`
reach the receive (`MSG_WAITALL` across two writes); peer reset before the send (TCP and Unix:
`ECONNRESET`/`EPIPE` and `ECANCELED`, nothing received); **1,000 failed pairs on one socket allocate
nothing after warm-up** (counting allocator: a leaked slot would grow the slab by pages, a leaked
`data` would keep the buffers; `ignored_*` stay 0); short send into 16 KiB buffers with a peer that
reads after 50 ms (256 KiB delivered intact, then the receive); **failed after partial** (the peer
reads 64 KiB of 1 MiB and resets: `Ok(n)` with `64 KiB <= n < 1 MiB` and `ECANCELED`, TCP and Unix);
`cancel()` while the receive waits (`Ok(70)`, `ECANCELED`, both buffers back, the stream usable);
`cancel()` while the send blocks (`Ok(n < len)`, exactly those bytes at the peer); drop in flight
(one allocation, the boxed data; `try_send` refused until the CQE; then `ignored_boxed == 1`,
`cancels_pushed == 2`); the **`-EAGAIN` path** (Unix, 4 KiB buffers, 1 MiB reply: a normal result,
and with `metrics` 2 `RECV` SQEs for the pair, i.e. the re-issue happened); `send_then_recv_fixed`
(`READ_FIXED` counted, the buffer back to its pool); a direct-only stream (`accept_direct`);
`coop_budget(Some(1))` counts the pair once (a task draining 5 ready pairs yields after each);
`try_send`/`try_recv` refused while a pair is in flight, even with room and queued data; and with
`metrics` a 2,000-frame loop: **1.000 task polls, 1.000 CQEs and 2.000 SQEs per frame**.

`tests/chain_raw.rs` (+6): skip chain all succeed (`[0, 0, 4]`, one CQE for the chain in
`cqes_reaped`), a middle member fails (`[0, -EBADF, -ECANCELED]`), the head fails, the last fails,
dropped in flight (data kept until the CQE, `N` cancels), a 256-SQE skip chain filling the SQ; and the
rejected shapes (SKIP on the last, on some only, without `IO_LINK`, with `IO_HARDLINK`).

Unit tests: `send_recv::tests` (the CQE table, op data sizes), `chain::tests::shapes`.

## Screening numbers

Tree `bb86c49`, `target/release/examples/reqresp` (default features) and `target/metrics/...`
(`metrics`), `--cpu 2 --client-cpus 3,4 --warmup 1 --duration 4`, runs listed in the order made,
interleaved by path. Frames/s, p50, p99 and cycles are screening (machine not quiet); user
instructions per frame are exact. `*` marks a run in a busy period.

### `--ring defer,skipenter,ringfd`

| conns | path | frames/s | p50 µs | p99 µs | user instr/frame | user cycles/frame | all cycles/frame |
|---|---|---|---|---|---|---|---|
| 8 | pair | 94,667 / 94,662 | 80.9 | 117.8 / 119.8 | 1,310 | 998 / 997 | 45,268 / 45,280 |
| 8 | pipe | 92,854 / 93,454 | 80.9 | 170.0 / 170.0 | 1,547 / 1,545 | 1,108 / 1,113 | 45,999 / 45,876 |
| 8 | eager | 95,047 / 95,327 | 82.9 | 123.9 / 119.8 | 1,282 / 1,281 | 1,291 / 1,296 | 45,096 / 44,979 |
| 2000 (4k) | pair | 106,549 / 106,919 / 106,537 | 18,612 | 19,661 / 19,137 / 20,185 | 1,229 | 1,084 / 1,079 / 1,079 | 40,223 / 40,087 / 40,235 |
| 2000 | pipe | 109,968 / 112,934 | 18,088 / 17,564 | 20,709 / 18,612 | 1,809 | 1,736 / 1,756 | 38,966 / 37,945 |
| 2000 | eager | 111,633 / 111,602 | 18,088 | 18,612 | 1,200 | 1,993 / 1,912 | 38,387 / 38,396 |

### `--ring defer,skipenter,ringfd,drain2` (the configuration G1 recommends), 2000 × 4k

| path | frames/s | p50 µs | p99 µs | user instr/frame | user cycles/frame | all cycles/frame |
|---|---|---|---|---|---|---|
| pair | 108,253 / 108,678 | 18,612 | 19,661 / 18,612 | 1,220 | 1,013 / 996 | 39,586 / 39,434 |
| pipe | 114,593 | 17,564 | 18,088 | 1,805 | 1,678 | 37,394 |
| eager | 112,723 | 17,564 | 18,088 | 1,195 | 1,944 | 38,014 |

### `--ring coop,skipenter,ringfd`

| conns | path | frames/s | p50 µs | p99 µs | user instr/frame | user cycles/frame | all cycles/frame |
|---|---|---|---|---|---|---|---|
| 8 | pair | 92,450 / 81,621 * / 96,158 | 85.0 / 97.3 / 85.0 | 133.1 / 165.9 / 121.9 | 1,287 / 1,301 / 1,288 | 936 / 1,077 / 914 | 46,352 / 52,446 / 44,568 |
| 8 | pipe | 94,908 / 88,511 * | 78.8 / 82.9 | 157.7 / 178.2 | 1,511 / 1,524 | 1,062 / 1,118 | 45,172 / 48,366 |
| 8 | eager | 95,200 / 95,062 | 78.8 | 137.2 / 141.3 | 1,282 / 1,284 | 1,296 / 1,329 | 45,029 / 45,086 |
| 2000 (4k) | pair | 97,813 / 99,069 | 20,185 | 21,234 | 1,195 | 1,348 / 1,273 | 43,813 / 43,257 |
| 2000 | pipe | 98,432 / 98,297 | 20,185 | 21,234 | 1,407 | 1,516 / 1,522 | 43,537 / 43,600 |
| 2000 | eager | 96,765 / 97,449 | 20,709 | 21,234 | 1,183 | 2,440 / 2,345 | 44,297 / 43,980 |

### `metrics` build (one run each)

`send_ops` is from `op_ticks_by_opcode[SEND]`, which counts first CQEs: a pair's send posts none, so
it is 0 like `eager`'s (`ops_by_opcode[SEND]` does count the SQE). `left` = `enter_left_taskrun /
enter_wait`.

| ring | conns | path | frames/s | task polls/frame | SQEs/frame | CQEs/frame | enters/frame | frames/round | left |
|---|---|---|---|---|---|---|---|---|---|
| defer | 8 | pair | 95,578 | **1.001** | 2.001 | **1.001** | 0.167 | 6.0 | 70% |
| defer | 8 | pipe | 78,542 * | 1.247 | 2.007 | 2.007 | 0.170 | 5.9 | 47% |
| defer | 8 | eager | 95,285 | 1.004 | 1.004 | 1.004 | 0.143 | 7.0 | 42% |
| defer | 2000 | pair | 97,356 | **1.000** | 2.000 | **1.000** | 0.050 | **20.0** | 100% |
| defer | 2000 | pipe | 88,124 * | 2.000 | 2.000 | 2.000 | 0.025 | 40.0 | 100% |
| defer | 2000 | eager | 89,865 * | 1.000 | 1.000 | 1.000 | 0.025 | 40.0 | 100% |
| defer+drain2 | 2000 | pair | 108,082 | 1.000 | 2.000 | 1.000 | 0.075 (21,413 drain enters) | 39.6 | 100% |
| coop | 8 | pair | 92,527 | 1.018 | 2.018 | 1.018 | 0.137 | 7.3 | 2% |
| coop | 8 | pipe | 92,593 | 1.055 | 2.005 | 2.005 | 0.132 | 7.6 | 3% |
| coop | 8 | eager | 84,788 * | 1.022 | 1.022 | 1.022 | 0.137 | 7.3 | 1% |
| coop | 2000 | pair | 97,612 | 1.000 | 2.000 | 1.000 | 0.001 | 2000 | 0 |
| coop | 2000 | pipe | 99,625 | 1.000 | 2.000 | 2.000 | 0.001 | 2000 | 0 |
| coop | 2000 | eager | 97,556 | 1.000 | 1.000 | 1.000 | 0.001 | 2000 | 0 |

The acceptance numbers of ROUND3.md hold: task polls per frame 1.00 (bound 1.1), one CQE per frame.

## Verdict against `eager` (and `pipe`)

- **User instructions per frame (exact):** pair 1,229 at 2000 / 1,310 at 8 (defer); eager 1,200 /
  1,282; pipe 1,809 / 1,546. The pair ties `eager` within 2–3% (one SQE more to build, one syscall
  less) and is 20–32% below `pipe`. User cycles per frame are the lowest of the three (about 1,000
  against 1,300–2,000 for `eager`, whose syscall's entry/return mitigations are charged to user mode).
- **`defer`, 8 connections: a tie with `eager`.** Frames/s −0.5% (94.7k against 95.0–95.3k, inside
  the spread), p99 the same (118–120 µs against 120–124), p50 2 µs better (80.9 against 82.9: no
  syscall in the task poll), all cycles per frame equal. Against `pipe`: +1.7% frames/s and p99 170 →
  119 µs (the reply no longer waits for a second completion and wake).
- **`defer`, 2000 connections: the pair loses.** −4.5% frames/s against `eager` (106.5–106.9k against
  111.6k) and −3 to −5% against `pipe`, with +4–5% all cycles per frame. The cause is in the `metrics`
  table: **20 frames per round instead of 40.** On a `DEFER_TASKRUN` ring the linked receive is
  started by a deferred task-work item (KV2, probe `kv2_pair_on_defer_taskrun`), so a frame costs two
  items (start the receive, complete it) where `pipe`/`eager` cost one, and a park's run of 20 items
  (KV1) carries half the frames: twice the enters, twice the rounds, each with the fixed per-round
  cost. `drain2` (G1's configuration) restores 39.6 frames per round at 3 enters per round (one wait
  plus two drains of 10 frames each): 108.3–108.7k, still −3.6% against `eager` (112.7k) and −5%
  against `pipe` (114.6k). This is the kernel's behaviour for linked SQEs on `DEFER_TASKRUN`; nothing
  in the driver can change it short of not linking.
- **`coop`, 8 connections: a tie** (noisy period: pair 92.5k / 96.2k, eager 95.2k / 95.1k, pipe 94.9k;
  p99 122–133 against 137–141 and 158).
- **`coop`, 2000 connections: the pair is best, by a little.** 97.8k / 99.1k against `eager` 96.8k /
  97.4k (+1.5%) and `pipe` 98.4k / 98.3k (tie), with the lowest all cycles per frame (43.3–43.8k
  against 44.0–44.3k and 43.5–43.6k). Here `eager` pays the task-work-on-every-syscall penalty of
  `coop` and `pipe` pays two CQEs and two polls (hidden by the one-round-per-sweep regime); the pair
  pays neither.

**Plainly:** the pair beats `pipe` everywhere except `defer` at 2000 connections, ties `eager` at 8
connections on both rings and beats it on `coop` at 2000 connections, and loses to both on `defer`
at 2000 connections (−4%; −3.6% with `drain2`). G1 chose `defer:…:drain2:eagerack` as the one
configuration; on that ring the pair is not the better reply path at many connections. It is the
right choice where the eager path is excluded: `coop_taskrun` rings, direct-only streams (no fd for
`send(2)`), and callers that must not spend a syscall inside the task poll.

## Tried and rejected

- **A flag in the slot's `Meta` for "which member posted"**: `Meta` has no spare byte with
  `metrics` (7 of 7 used, r3-slot), and every state transition would copy one more byte. The CQE
  flags word carries it for free (above).
- **A new `OpKind::Pair` / lifecycle state**: nothing needs it; the slot is an ordinary single-shot
  op. `lifecycle.rs` is unchanged.
- **`TxFd` + `RxFd` in the op data** (as r3-eager's note asked): two `Rc` clones and drops per pair.
  One `PairFd` guard raising both counters does the same with one.
- **`POLL_FIRST` on the `READ_FIXED` tail**: `ioprio` is a real I/O priority there (`EINVAL` at
  prep → the head reports `-ECANCELED`); found by the first test run, removed.
- **Reusing the pair's slot for the `-EAGAIN` re-issue**: the slot is freed by the `Ready` poll
  before the future sees the result; a fresh `push_op` of the same op data's `uring_op()` is simpler
  and the slab hands the same index back anyway.
- **A separate `SkipChainOp` type**: `submit_chain` returns `ChainOp`; a `shared: bool` with one test
  at the top of `poll` keeps the plain shape's code as it was.
- **Pairs on the owned halves**: a pair spans both directions; not added (open question 1).

## Open questions

1. **Owned halves.** `OwnedReadHalf`/`OwnedWriteHalf` have no `send_then_recv`. The future takes
   `&self` of the stream and does not borrow it, so a server that splits could call it on the stream
   before splitting and keep the future; whether a method on the write half (which would receive too)
   is wanted is a design question for the lead.
2. **The dropped-pair box.** `SendRecv<&[u8], Vec<u8>>` is 56 bytes, 8 above the slot's inline
   storage, so a pair dropped in flight (a `timeout` firing) allocates once. Dropping `recv_flags`
   from the data (always `EMPTY` in the bench) would make it 48 and inline; keeping the flags in the
   SQE only is not possible for the `-EAGAIN` re-issue. A `u16` flags field would save 2 bytes, not
   8. Left as is; the bench's timeout path drops a receive, not a pair, most of the time.
3. **`defer` at many connections** (the verdict): one more deferred task-work item per frame is the
   price of the link. If G2 confirms the −4%, AGENTS.md should steer `defer_taskrun` servers with many
   connections to `send_eager` + `recv_poll_first_op` and `coop_taskrun` / direct-only servers to
   `send_then_recv`.
4. **`send_ops` in the reproducer's STATS line** counts first CQEs by opcode, so a pair's (skipped)
   send shows as 0 there, like `eager`'s direct send. `ops_by_opcode[SEND]` has the SQE count. The
   line could print both; r3-repro's file.
5. **A `LINK_TIMEOUT` member in a skip chain** is rejected without a probe. If someone needs
   "send, receive with a deadline" as one slot, the probe is cheap: push `[SEND(skip, link),
   RECV(link), LINK_TIMEOUT]` and count CQEs with the timeout firing and not firing.
6. **Kernels before 5.19**: `POLL_FIRST` on the tail is rejected at prep there, so the head reports
   `-ECANCELED` and `send_then_recv` is unusable (`(Err(ECANCELED), Err(ECANCELED))` every time). No
   runtime probe was added; `send_eager` probes `IORING_OP_SOCKET` for the same flag and could be
   shared if an old kernel matters.

## Requested changes

1. **`examples/reqresp/paths/mod.rs` (r3-repro's file, applied locally in `20a5834`)**: `pair` was
   the last stub, so `not_available` has no caller and warns as dead code. Removed:
   ```diff
   -/// For the paths a later package fills.
   -pub fn not_available(path: &str) -> ! {
   -    eprintln!("path {path}: not available in this build");
   -    std::process::exit(2)
   -}
   -
   ```
2. **AGENTS.md (r3-docs).** Under "Core concepts", after the eager-sends bullet r3-eager requested:

   > - **Reply then receive as one operation.** `TcpStream`/`UnixStream::send_then_recv(reply, buf,
   >   recv_flags)` (and `send_then_recv_fixed(reply, FixedBuf)`, Linux 5.19+) push a `SEND`
   >   (`MSG_NOSIGNAL | MSG_WAITALL`, `IO_LINK | CQE_SKIP_SUCCESS`) linked to a `RECV`
   >   (`POLL_FIRST`) on **one op slot with one CQE and one task wake**: the request/response
   >   pattern of `reqresp --path pair`. They return `SendThenRecv<S, R>` / `SendThenRecvFixed<S>`
   >   (`monoio::net`; `Unpin`, `'static`, token API like the other named futures), resolving to
   >   `(send, recv, reply, buf)`: `(Ok(len), Ok(n))` is the normal case (`n == 0` EOF);
   >   `(Err(e), Err(ECANCELED))` the send failed before anything went out and the receive did not
   >   run; `(Ok(n < len), Err(ECANCELED))` the send failed after `n` bytes (the kernel reports the
   >   count, not the errno): treat as a broken connection. A send that fills the socket is retried
   >   in the kernel; the receive starts when it is through. Dropping the future keeps both buffers
   >   until the CQE and (with `async-cancel`) cancels both members; `cancel()` and keep polling to
   >   get them back. The pair counts as one send and one receive in flight (`try_send`/`try_recv`
   >   refuse) and as one `coop_budget` result. `SEND_ZC` cannot be the head.
   >   **When to use which:** on a `coop_taskrun` ring, on direct-only streams, or when the task poll
   >   must not make a syscall, use `send_then_recv` (at 2000 connections on `coop` it is +1.5% on
   >   `send_eager` + `recv_poll_first_op` and equal to `send_op` + `recv_poll_first_op` with half the
   >   CQEs and polls). On a `defer_taskrun` ring with many connections use `send_eager` +
   >   `recv_poll_first_op`: the kernel starts a linked receive from a deferred task-work item, so a
   >   pair costs two items per frame against one, which halves the frames a park delivers (20 of
   >   the cap of 20 items per run against 40; −4% frames/s at 2000 connections, −3.6% with
   >   `drain_taskrun(2)`). With few connections the two tie on either ring (8 connections: same
   >   frames/s and p99, the pair's p50 2 µs lower).
   > - **Raw chains, skip shape.** `uring::submit_chain` also accepts `IO_LINK | CQE_SKIP_SUCCESS`
   >   on every SQE but the last (`uring::link_skip([..])`; neither flag on the last; no
   >   `IO_HARDLINK`, no `LINK_TIMEOUT` member): the chain takes one op slot and posts one CQE, and
   >   `ChainOp` resolves with `Cqe::new(0, 0)` for the members before the one that posted (they
   >   succeeded), that member's CQE, and `-ECANCELED` for the members after it. Put `MSG_WAITALL`
   >   on a `SEND`/`RECV` member whose short result must not count as a success. Any other
   >   placement of `CQE_SKIP_SUCCESS` is `InvalidInput`.

   And in the "Linked chains" bullet, replace "a linking last SQE and `SKIP_SUCCESS` are rejected" by
   "a linking last SQE is rejected; `SKIP_SUCCESS` only in the skip shape (below)". In "Named op
   futures / token API", add `SendThenRecv`, `SendThenRecvFixed` (`monoio::net`) to the list.
3. **ROUND3-PLAN.md, row E9** (lead): as decided by the probe; the tag carries the member's position
   (bits 32..60) besides bit 61, so one mechanism serves the pair and the skip-shape chains.
4. **Bench (lead, G2)**: a `pairack` tune item on `send_then_recv` as planned; measure it on `coop` as
   well as on `defer`, since that is where it wins.

## Checks

Pinned to CPUs 0,1,5,6,7,11, `CARGO_BUILD_JOBS=4`, at `bb86c49`:

| command | result |
|---|---|
| `cargo test -p adaptif-gatotkaca-monoio` | 639 passed, 0 failed, 8 ignored |
| `cargo test -p adaptif-gatotkaca-monoio --features metrics` | 661 passed, 0 failed, 8 ignored |
| `cargo clippy --workspace` / `--features adaptif-gatotkaca-monoio/metrics` | the 2 old warnings (`net/udp.rs`), nothing new |
| `cargo clippy -p adaptif-gatotkaca-monoio --tests --examples` | nothing from the new or changed files |
| `cargo build --workspace --examples`, with and without `metrics` | ok, no warnings |
| `cargo fmt` | applied |
| `tests/send_then_recv.rs` alone | 3 runs without `metrics` and 1 with: all pass; no rerun was needed in the full runs |

Not run: the bench repo (`sut-monoio`), gate runs on a quiet machine.
