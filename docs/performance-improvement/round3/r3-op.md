# r3-op: per-operation user-space cost (O1; E2, E3, E6, E4)

Branch `r3/r3-op`, on `r3-g0`. No API change, no behaviour change, nothing to opt in.

Files: `driver/op.rs`, `driver/mod.rs`, `io/futures.rs`, `io/async_write_rent_ext.rs`, and in
`driver/uring/mod.rs` the functions `reserve_sqe`, `reserve_sqes`, `push_op`, `push_chain`,
`push_op_with_timeout` (plus the private helper `push_failed` next to `push_op`). Tests: `tests/named_futures.rs`
(extended), `tests/write_all.rs` (new).

## Short answer

- User instructions per frame at 2000 connections × 4 KiB, `r3-g0` → this branch: `reqresp` pipe 2,342 → 2,073
  (−11.5%), plain 2,398 → 2,063 (−14.0%), `smallframes` 2,671 → 2,540 (−4.9%), `smallframes --no-timeout`
  1,883 → 1,629 (−13.5%).
- Adopted: E2, E3, E6, and two things found on the way (always inline the named futures' poll; take a finished op
  apart in place).
- Not adopted: E4. Its upper bound is 0.7–0.8% of a frame, below the 1% bar, before any cost for safety.
- One requested change in r3-eager's files is worth another 1–3% (`#[inline(always)]` on four submit wrappers).
- `smallframes` with the per-frame `timeout` gains much less than the other loads. The cause is inlining, not the
  timer: see "What limits the numbers".

## Numbers

User instructions per frame, exact (two runs of a binary give the same number, ±1). One 4 s run per cell.
`reqresp --conns 2000 --sizes 4k --ring defer,skipenter,ringfd`; `smallframes --conns 2000 --size 4096
--client-cpus 3,4`. Every commit was built into its own target directory.

| commit | item | reqresp pipe | reqresp plain | smallframes `--no-timeout` | smallframes (timeout) |
|---|---|---|---|---|---|
| `r3-g0` | base | 2,342 | 2,398 | 1,883 | 2,671 |
| `27f21c3` | E2 | 2,270 | 2,325 | 1,828 | 2,611 |
| `441af6d` | E3 | 2,151 | 2,131 | 1,679 | 2,549 |
| `d345709` | always inline poll | 2,101 | 2,131 | 1,676 | 2,549 |
| `7a10f91` | E6 | 2,095 | 2,092 | 1,647 | 2,601 |
| `eeaabc0` | finished op in place | 2,073 | 2,065 | 1,625 | 2,536 |
| `51aa1c9` | E6, future size | 2,073 | 2,063 | 1,629 | 2,540 |
| | total | −269 (−11.5%) | −335 (−14.0%) | −254 (−13.5%) | −131 (−4.9%) |

Other loads, base → final:

| load | base | final | |
|---|---|---|---|
| reqresp pipe, `--deadline timeout`, 2000 connections | 3,341 | 3,082 | −7.8% |
| reqresp plain, `--deadline timeout`, 2000 connections | 3,310 | 3,004 | −9.2% |
| reqresp pipe, 8 connections (4/16/64 KiB) | 2,098 | 1,866 | −11.1% |
| reqresp plain, 8 connections | 2,623 | 2,286 | −12.8% |

- The 8-connection numbers depend on the frames per round, so they move by about 1% between runs (G0 has 2,077
  for the pipe base).
- Frame counts: the runs served between 380,000 and 452,000 frames in the window with `server_errors=0` and
  `client_failed=0`.
- Cycles and frames/s were measured but are **screening, machine not quiet** (other agents were building):
  frames/s moved between about 90,000 and 120,000 across runs of the same load. No verdict rests on them.

## What changed

### E2: one SQ view per push (`27f21c3`)

- **Was:** `push_op` called `reserve_sqe` (a view for the capacity check, a view in the `free` closure) and then
  made a third view to push. Each view loads the kernel's head and stores the tail when it is dropped.
- **Now:** `push_op`, `push_chain` and `push_op_with_timeout` make one view, test for room on it and push through
  it. A full SQ drops the view and calls `reserve_sqes`, which is unchanged (retries, flush, `tick`, `EBUSY`,
  `InvalidInput` for a chain larger than the SQ) and now `#[cold]`.
- The SQE is built (`uring_op`) before the view is made. `uring_op` calls buffer methods, which are user code, so
  no view is alive across it. Consequence: when the reservation fails, `uring_op` has already run. A search over
  the 48 `uring_op` implementations found none that changes the op data, except two that set `msghdr` fields to
  the same values on every call.
- `reserve_sqe` (used by `push_cancel`, `cancel_fd` and the doorbell) tests `is_full` inline before the cold call.
- The "can't happen" cleanup after a failed push moved to the cold helper `push_failed`.

### E3: a finished op is consumed without drop glue (`441af6d`)

- **Was:** `poll_result` assigned `State::Done` over `State::Running(op)`. That called
  `drop_glue::<State<..>>`: `Op::drop` (a call into the driver that returns because the index is `usize::MAX`),
  the `Rc` drop and the data drop. The same glue was called a second time when the future was dropped after the
  `await`. `Op::poll` was an out-of-line function per op type.
- **Now:**
  - `Op::poll_meta` polls and marks the op finished. `Op::poll` is built on it.
  - `OpFuture` keeps its state in `ManuallyDrop` and has a `Drop` that tests for `Done` inline. Only a future
    dropped before it returned its output calls `drop_unfinished`.
  - `Op::drop` tests `index != usize::MAX` inline before it calls the driver.
  - `Inner::poll_op` lost its unused `_data` parameter. The two `expect`s in `Op::poll` became one test with a
    cold panic (same message).
- **Check:** `nm` finds 14 `drop_glue::<monoio::io::futures::State<..>>` symbols in the base binaries and none
  now. A `perf report` of `smallframes` and `reqresp` (server CPU) shows neither that glue nor `drop_unfinished`.

### Always inline the named futures' poll (`d345709`)

- LLVM inlined `OpFuture::poll_result` only where a future type had one use in the binary. A type awaited in
  several places (the pipe path's `send_op` and `recv_op`) got an out-of-line `poll` with the `BufResult` passed
  through memory.
- `#[inline(always)]` on `OpFuture::poll_result`, the public `poll_result` and `Future::poll`. The body is small
  after E3.
- −50 on the pipe path, nothing on the others. E6 needs it: without it E6 was +14 on `reqresp` plain.

### E6: `write_all` sends the plain buffer first (`7a10f91`, `51aa1c9`)

- **Was:** every `write_all` wrapped the buffer in a `Slice`, sent it, unwrapped it and went round the loop again
  to find it was done.
- **Now:** the first write goes out on the buffer as it is and returns when it took everything. The `Slice` loop
  runs only for the rest after a short write or `Interrupted`. An empty buffer still writes nothing.
- **Future size:** the first version grew the future by 16 bytes. The result of the first write was a
  function-scope local, partly moved by the `match`, so its storage was kept across the loop's awaits. It now
  lives in its own block. `write_all::<Bytes>` on a `TcpStream` is 192 bytes, as before E6.
- **Measured against the same tree without E6** (the deciding numbers):

  | load | without E6 | with E6 | |
  |---|---|---|---|
  | reqresp plain | 2,100 | 2,064 | −1.7% |
  | reqresp plain, `--deadline timeout` | 3,032 | 3,004 | −0.9% |
  | smallframes `--no-timeout` | 1,652 | 1,629 | −1.4% |
  | smallframes (timeout) | 2,524 | 2,540 | **+0.6%** |

- **The lead should decide about the last row.** In that one binary shape the larger `write_all` body makes LLVM
  stop inlining `Op::recv_with` and `Op::send_with_flags` into the frame's `poll`, which costs more than E6
  saves. With the requested change below the row is 2,512; "without E6, with the requested change" was not
  measured. If the bench shows the same shape, dropping `7a10f91` and `51aa1c9` costs 0.9–1.7% on the other three
  loads and gains 0.6% on this one.
- `tests/write_all.rs` pins which byte ranges each `write` gets against a scripted writer. It passes on the old
  implementation too.

### A finished op is taken apart in place (`eeaabc0`)

- **Was (after E3):** `poll_result` moved the whole state out with `mem::replace`, the op into `Op::finish`, and
  the data into the `Completion`. The op data was copied twice on the stack.
- **Now:** `Op::take_finished` (unsafe, replaces `finish`) reads the data out of the op where it is and drops the
  driver handle in place. `poll_result` then overwrites the state with `Done`.
- −22 to −27 per frame on every load.

## Rejected, with numbers

### E4: the driver handle in `Op`

Measured on top of E6 (pipe 2,095, plain 2,092, smallframes `--no-timeout` 1,647):

| variant | pipe | plain | smallframes `--no-timeout` |
|---|---|---|---|
| handle not counted (no `Rc` clone at submit, no drop at completion, nothing else) | 2,079 (−0.8%) | 2,078 (−0.7%) | 1,636 (−0.7%) |
| the same, and the driver looked up through `CURRENT` at each poll | 2,150 (+2.6%) | not run | not run |

- The first row is the **upper bound** of the raw-pointer variant: it is unsound as measured (nothing keeps the
  driver alive). A sound version has to pay for liveness on top, so E4 cannot reach 1%.
- The `CURRENT` variant is slower than today: an op is polled twice (pending, then ready), and each lookup is a
  call through the `LocalKey` accessor.
- `tests/named_futures.rs::op_future_outlives_block_on_and_runtime` (`11d2484`) pins the property a later attempt
  has to keep: op futures are polled and dropped after `block_on` returned and after the runtime was dropped.

### Smaller things

| tried | result | |
|---|---|---|
| `const` initialiser for the scoped thread-local (`macros/scoped_tls.rs`, not my file) | pipe 2,095 → 2,101 | no gain |
| `#[inline]` on `try_submit_with_data` and `submit_or_return` | pipe 2,095 → 2,091 | noise, LLVM decides the same |
| `submit_or_return` without capturing the data in the `CURRENT.with` closure (raw pointer to the handle) | pipe 2,073 → 2,089 | worse |
| E6 with the short-write tail as its own `async fn` | smallframes (timeout) 2,540 → 2,615 | worse |
| E6 without the always-inline commit | plain 2,131 → 2,145, smallframes `--no-timeout` 1,679 → 1,642 | mixed, hence the order of the commits |

## What limits the numbers

- **Inlining decides as much as the code.** The whole frame is one `poll` function per connection type. When it
  grows, LLVM stops inlining something else. That is why `smallframes` with the timeout gained 131 where the same
  code without the timeout gained 254, and why E6 is +0.6% on it. The numbers of a commit hold for the binary
  they were measured on; the bench server is a third shape.
- **`perf` profiles by instruction are not reliable on this machine.** There is no precise event
  (`instructions:upp` is refused), and with skid the samples land after cache misses, so the shares per function
  are distorted. I used them only to see whether a symbol is present, took the totals per frame for every
  decision, and read the disassembly.
- What is left, counted in the disassembly of the receive op: about 85 instructions per submit in
  `try_submit_with_data` (the SQE is written straight into the ring) plus `Slab::insert`, and about 60 on the
  ready path of a poll, of which about 12 copy the `CompletionMeta` that `Ref::poll_op` returns through memory
  (`lifecycle.rs`, r3-slot).

## Requested changes

1. **`driver/op/recv.rs`, `driver/op/send.rs` (r3-eager): `#[inline(always)]` on the four submit wrappers.** Not
   applied on this branch, not needed for the tests. Measured locally on top of `51aa1c9`:

   | load | branch | with the change | |
   |---|---|---|---|
   | reqresp pipe | 2,073 | 2,013 | −2.9% |
   | reqresp plain | 2,064 | 1,997 | −3.2% |
   | reqresp plain, `--deadline timeout` | 3,004 | 2,934 | −2.3% |
   | smallframes `--no-timeout` | 1,629 | 1,587 | −2.6% |
   | smallframes (timeout) | 2,540 | 2,512 | −1.1% |

   The wrappers only build the op data and call `submit_or_return`. Out of line they copy the data in and the
   result out (about 20 instructions per op). `try_submit_with_data` stays out of line.

   ```diff
   --- a/monoio/src/driver/op/recv.rs
   +++ b/monoio/src/driver/op/recv.rs
   @@ -37,11 +37,13 @@ const RECVSEND_POLL_FIRST: u16 = 1 << 0;
    impl<T: IoBufMut> Op<Recv<T>> {
   +    #[inline(always)]
        pub(crate) fn recv(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
            Self::recv_with(fd, buf, 0, false)
        }

        /// Receive with `MSG_*` flags; `poll_first` sets `IORING_RECVSEND_POLL_FIRST`.
   +    #[inline(always)]
        pub(crate) fn recv_with(
   --- a/monoio/src/driver/op/send.rs
   +++ b/monoio/src/driver/op/send.rs
   @@ -24,10 +24,12 @@ pub(crate) struct Send<T> {
    impl<T: IoBuf> Op<Send<T>> {
   +    #[inline(always)]
        pub(crate) fn send(fd: SharedFd, buf: T) -> SubmitResult<Self, T> {
            Self::send_with_flags(fd, buf, 0)
        }

   +    #[inline(always)]
        pub(crate) fn send_with_flags(
   ```

   If r3-eager reshaped these functions, the rule is the same: the thin function between the stream method and
   `submit_or_return` is always inlined. Re-measure after the merge.

2. **Merge note for `driver/uring/mod.rs`.** My first hunk starts at `reserve_sqe`, directly below `enter`
   (r3-drain). A change in the last three lines of `enter` shares its context. The two are independent.

No other file outside my list was changed. `driver/op/send_zc.rs` reads `op.driver.0`, `op.index` and `op.data`;
those fields and their meaning are unchanged.

## Open questions

- `RecvFuture::cancel` (and the other `cancel`s) go through `driver::CURRENT`, so calling one outside a runtime
  panics ("cannot access a scoped thread local variable"). Polling and dropping outside a runtime are fine. The
  op has its own driver handle, so `cancel` could use that instead. That would be a behaviour change (a panic
  becomes a cancel), so I left it.
- `Op::poll` (the `.await` on an `Op` in `accept`, `connect`, `close`, file ops) still ends with the `Rc` drop
  and the data test in the op's drop glue. Those ops are not on the frame path, so I did not measure them.

## Tests

At `51aa1c9`, pinned to CPUs 0,1,5,6,7,11:

| command | result |
|---|---|
| `cargo test -p adaptif-gatotkaca-monoio` | 76 test binaries, 498 passed, 0 failed, 8 ignored |
| `cargo test -p adaptif-gatotkaca-monoio --features metrics` | 76 test binaries, 517 passed, 0 failed, 8 ignored |
| `cargo clippy --workspace` | the 3 old warnings in the lib (`net/udp.rs` twice, `utils/slab.rs`) |
| `cargo clippy --workspace --features adaptif-gatotkaca-monoio/metrics` | the same 3 |
| `cargo fmt --check` | clean |

No test failed in any run, so nothing was rerun alone.

New tests:
- `named_futures.rs`: `drop_completed_before_result_was_taken`, `poll_after_completion_panics`,
  `op_future_outlives_block_on_and_runtime`.
- `write_all.rs`: whole buffer in one write, short writes, `Interrupted` on the first and on a later write, zero
  write, error, empty buffer, a `Slice` argument, 8 MiB over TCP with a slow reader.
