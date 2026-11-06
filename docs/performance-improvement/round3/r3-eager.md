# r3-eager: eager sends (S1, E7)

Branch `r3/r3-eager` on `r3-g0`, one commit: `feat(net): try_send, try_recv, send_eager and per-stream eager
sends (r3-eager: S1)`. Side branch `r3/r3-eager-requested` (one commit on top) holds the requested changes in
files of other packages, with their test; its diff is at the end of this file.

Files changed (all in the package's list): `driver/shared_fd.rs`, `driver/op/{send,recv,send_zc,read,write,
fixed}.rs`, `net/tcp/{eager,split}.rs`, `net/unix/{eager,split}.rs`, the `write`/`writev` bodies of
`net/tcp/stream.rs` and `net/unix/stream.rs`, `tests/eager_send.rs` (new), `examples/reqresp/paths/eager.rs`.
No registry file (`mod.rs`, `lib.rs`) was touched: the new methods are inherent methods of `TcpStream`,
`UnixStream` and the owned halves, and the futures are the existing `SendFuture` / `WritevFuture`.

All numbers below: this machine, other agents building ("screening, machine not quiet"); user instructions per
frame are exact.

## What was built

### Counters (`driver/shared_fd.rs`)

`SharedFd`'s `Inner` gets `tx_in_flight: Cell<u32>`, `rx_in_flight: Cell<u32>` and `eager_send: Cell<bool>`.
`Inner` is now `#[repr(C)]` with `fd`, the two counters and the flag first (16 bytes), right behind the `Rc`
counts that every op clone and drop already writes. A unit test checks the offsets.

The guard is `InFlight<const TX: bool>(SharedFd)`, with `TxFd = InFlight<true>` and `RxFd = InFlight<false>`.
It is exactly a `SharedFd` (same size, `Deref<Target = SharedFd>`, so `sqe_fd!` works on it unchanged):
`new` raises the counter, `Drop` lowers it. The op data holds the guard where it held the `SharedFd` before,
so no op data grows: `Send<Vec<u8>>` 40 bytes, `Send<Bytes>` 48, `Recv<Vec<u8>>` 40, `Recv<BytesMut>` 48
(unit tests in `send.rs` and `recv.rs`; `tests/ignored_inline.rs` still passes).

**Raised:** in `InFlight::new`, when the op data is built, before `submit_or_return` pushes the SQE. So an op
that sits in the SQ, not yet seen by the kernel, counts already.

**Lowered:** in `Drop`, when the op data is dropped. The driver drops op data only after the op's final CQE:

- the future completes: `Op::poll` takes the data out once `poll_op` returned the CQE's result, and the
  `BufOp::complete` / `result()` that hands the buffer back drops the rest (the guard) before the caller sees
  the result;
- the future is dropped in flight: `Ref::drop_op` moves the data into the op slot (`Lifecycle::Ignored`,
  `IgnoredMore` for a zero-copy send) and it is dropped when the final CQE arrives (for a zero-copy send that
  is the notification, after the result);
- a linked chain (`ChainOp`) keeps its data until the last CQE of the chain; a multishot op (`MultishotOp`)
  until the CQE without `F_MORE`, and `RecvMultiOp` keeps the finished op (with its data) until the stream is
  dropped or re-armed;
- the submit fails (SQ stayed full): the kernel never saw the op, and the constructor's `map_err` drops the
  data at once (`(e, d.buf)` moves the buffer out, the guard drops).
- The runtime is single-threaded; the counters are `Cell`s in the `Rc` the guard itself keeps alive.

So the counter can only be **late** (the CQE is in the CQ, or reaped, but the owning task was not polled yet;
a dropped op until the driver's next tick; a zero-copy send until the kernel released the buffer), never
early. The tests check exactly the late cases: `try_send` is still refused after the peer drained the socket
and `poll(2)` says writable, until the ring send's future was awaited (or, dropped, until the driver saw its
CQE).

### Ops that carry a guard

| guard | ops (file) |
|---|---|
| `TxFd` | `Send`, `SendZc`, `SendMsgVec` (`send.rs`); `SendZcBuf`, `SendMsgZc` (`send_zc.rs`); `Write`, `WriteVec` (`write.rs`); `WriteFixed` without an offset (`fixed.rs`) |
| `RxFd` | `Recv` (`recv.rs`); `Read`, `ReadVec` (`read.rs`); `ReadFixed` without an offset (`fixed.rs`) |
| requested (other files) | `RecvMulti`, `RecvBundle` (`recv_multi.rs`); the socket of `File::read_at_then_send` (`fs/file/chain.rs`); `link_timeout::{Recv, Send}` (`read_with_deadline` / `write_with_deadline`); `Splice` both ends (`splice.rs`) |
| none | `ReadAt`, `WriteAt` (files); `ReadFixed` / `WriteFixed` with an offset (files); `SendMsg`, `SendMsgUnix`, `RecvMsg`, `RecvMsgUnix` (UDP, Unix datagram and seqpacket: no `try_send` there); accept, connect, poll, close, shutdown, cancel |

`ReadFixed` / `WriteFixed` serve files and sockets through one constructor whose signature is called from
`fs/` and `net/` (not my files), so they tell the two apart by the offset: `Target<const TX: bool> { fd,
offset }` counts only when `offset == NO_OFFSET` (sockets, pipes). A file op pays one compare; its counters
stay zero (unit test `only_ops_without_offset_count`). `Read` is only used by `Pipe`, `ReadVec` by sockets
and pipes: both count (a pipe's counters are never read). `Write` has no caller at all; it counts, so that a
future socket use is covered.

### API (`net/tcp/eager.rs`, `net/unix/eager.rs`, the split halves)

On `TcpStream`, `UnixStream`, `TcpOwnedWriteHalf`, `UnixOwnedWriteHalf` (`try_recv` on the read halves):

- `try_send(&self, &[u8])`, `try_send_vectored(&self, &[IoSlice])`, `try_recv(&self, &mut [u8])`: one
  `send`/`sendmsg`/`recv` with `MSG_DONTWAIT` (sends add `MSG_NOSIGNAL`) on the calling thread, `EINTR`
  retried, at most 1024 iovecs. `WouldBlock` when the socket is full/empty, when the matching counter is not
  zero, or when `fd < 0` (direct-only). No op slot, SQE or CQE.
- `send_eager(&self, buf) -> SendFuture<T>`, `send_eager_with_flags(buf, MsgFlags)` (E7, no new future
  type): `try_send` first. All sent or a hard error: `SendFuture::ready(..)` (first poll `Ready`). `EAGAIN` or
  a short count: a ring `SEND` of the rest with `IORING_RECVSEND_POLL_FIRST`; counter not zero or direct-only:
  a plain ring `SEND` (no `POLL_FIRST`). The bytes the direct send took are carried in `Send<T>` as
  `eager: u32` (low 31 bits the offset, top bit `POLL_FIRST`; a `send(2)` returns at most `MAX_RW_COUNT`
  < 2^31, and the direct call never asks for more). The SQE points at `read_ptr() + offset` with
  `bytes_init() − offset`; `complete` adds the offset to an `Ok(n)`. If the ring part fails or is cancelled
  after the direct part sent something, the result is `Ok(offset)` (a short send, like `send(2)` after a
  partial copy; the error shows on the next send). A submit failure after a partial direct send is also a
  short send. An empty buffer: `send(fd, ptr, 0)`, same result as the ring (test).
- `POLL_FIRST` needs 5.19. There is no feature bit; `poll_first_supported()` probes `IORING_OP_SOCKET`
  (same release) once per process through `register_probe` and leaves the flag out on older kernels, so
  `send_eager` never produces an `EINVAL` that `send_op` would not.
- `set_eager_send(&self, bool)` / `eager_send(&self)`: the flag lives in `SharedFd`, so halves and clones
  see it (test through `into_split` and `reunite`). When on, `AsyncWriteRent::write` of both streams (and so
  `write_all`, and the owned write halves, which delegate to the stream's `write`) goes through `send_eager`;
  `writev` goes through `writev_eager`: one direct `sendmsg(2)`, whose count is returned as is (a short
  write if partial, which every `writev` caller handles anyway, so no offset into the iovecs is needed),
  the ring `WRITEV` on `EAGAIN`/in-flight/direct-only. Off by default; `_op` methods, `send_with_flags`,
  `send_vectored`, zero-copy and `cancelable_*` are unchanged.
- `send_eager` and `writev_eager` are `#[inline(never)]` (they end in a syscall; inlined into `write` they
  grew every caller of `write`, see "cost" below). The ring fallback of `send_eager` is a `#[cold]` function.

Docs on each method: raw SQEs (`uring::submit`, chains, the hook) are not counted, don't mix them with the
`try_*` calls; large writes that usually fill the socket pay a wasted syscall (meant for replies up to a few
KiB); on `coop_taskrun` rings every direct syscall runs the ring's pending task work on its way out, so the
eager path is for `defer_taskrun` rings or rings without task-run flags; a request whose reply fits finishes
without the future ever being `Pending`.

### Reproducer and tests

`reqresp --path eager`: `paths/pipe.rs` with the reply through `send_eager` (also for the rest after a short
send) and the receive of the next frame queued as `recv_poll_first_op` in the same poll. With `metrics`,
`send_ops=0` and `sqes_per_frame=1.00`.

`tests/eager_send.rs` (19 tests, 18 without `metrics`, each for TCP and Unix where it applies):

- `try_send` while a ring send waits on a full socket (16 KiB buffers; 4 KiB stalled TCP in its window
  probes, 7.5 s): refused while the SQE is only queued, while it waits in the kernel, and **after the peer
  drained everything and `poll(2)` says writable**, until the future is awaited; the same with the future
  dropped in flight, then it works again once the driver reaped the op (`eventually`).
- `try_recv` while a ring receive is in flight: the data is queued *before* the receive's SQE is submitted,
  so only the counter keeps `try_recv` from stealing it; the same once the receive waits in the kernel, for a
  dropped receive, for `readv_op`, `recv_fixed_op`, and for a ring receive of one split half against the
  other handle.
- direct-only stream (`accept_direct`, `with_fixed_files(8)`): `try_send`, `try_send_vectored`, `try_recv`
  refused (with data queued); `send_eager` is `Pending` at its first poll and goes through the ring;
  `write_all` / `write_vectored_all` with the switch on arrive.
- byte order: 1 MiB of numbered chunks (70 B to 64 KiB) through 16 KiB socket buffers to a slow reader,
  chunk by chunk through `send_eager` (ready or pending), `send_op` with a `try_send` attempt while it is in
  flight (must be refused), `try_send` with a ring fallback, and `write_all` with the switch; the reader sees
  every chunk whole and in order, and every path was taken at least once.
- `send_eager` that fits is `Ready` at its first poll (noop waker), and the bytes are at the peer before any
  poll; empty buffer; `MSG_MORE` passed through; a reset peer gives `BrokenPipe`/`ConnectionReset` at the
  first poll, no `SIGPIPE`.
- partial direct send + ring rest (256 KiB, `MSG_WAITALL`) returns the whole length and the bytes arrive
  in order; the cancelled rest returns the short count and exactly those bytes are at the peer.
- `set_eager_send(true)` makes `write`, `write_all`, `writev`, `write_vectored_all` ready at the first poll
  (a ring op never is), through `into_split` halves too, and off again after `set_eager_send(false)`.
- ring ops of other kinds count: `writev_op`, `send_vectored`, `send_zc_early` (until the notification),
  `send_fixed_op` / `recv_fixed_op`.
- `metrics`: 100 `send_eager` + `try_send` + `try_send_vectored` + `write_all` + `write_vectored_all` with the
  switch, all read by `try_recv`: `ops_by_opcode` and `sqes_submitted` unchanged; one `send_op` adds one
  `SEND`.

Mutation checks (run by hand, then reverted): with `can_send_now` ignoring the counter 9 of 18 tests fail,
with `can_recv_now` ignoring it 4 fail; on the side branch, the 4 tests of `eager_send_guards.rs` all fail
with the unguarded versions of the four files. The test binary ran 20 times in a row without a failure, with
and without `metrics`, while other agents were building.

## Screening numbers

`reqresp --cpu 2 --client-cpus 3,4 --warmup 1 --duration 4`, release build of this branch (the same binary
for `pipe` and `eager`), one run per line, in the order run. "user cyc" / "cyc" are user cycles and all cycles
per frame. The tables were run before a last tweak (the `#[cold]` helper in `Send::uring_op`), which takes 2–4
user instructions per frame off the ring-send path (`pipe` 2,430 → 2,428 at 2000) and changes nothing else;
the "Cost" section below is from the final build.

### `--ring defer,skipenter,ringfd`

| conns | path | frames/s | p50 µs | p99 µs | user instr/frame | user cyc | cyc |
|---|---|---|---|---|---|---|---|
| 8 | pipe | 94,663 | 76.8 | 170.0 | 2,162 | 1,432 | 45,274 |
| 8 | pipe | 94,415 | 76.8 | 170.0 | 2,170 | 1,437 | 45,399 |
| 8 | pipe | 94,860 | 76.8 | 170.0 | 2,170 | 1,451 | 45,184 |
| 8 | eager | 95,089 | 80.9 | 141.3 | 1,653 | 1,513 | 45,076 |
| 8 | eager | 93,818 | 80.9 | 145.4 | 1,650 | 1,528 | 45,679 |
| 8 | eager | 93,836 | 80.9 | 145.4 | 1,652 | 1,544 | 45,670 |
| 2000 (4k) | pipe | 112,392 | 17,564 | 18,612 | 2,430 | 2,200 | 38,145 |
| 2000 | pipe | 113,003 | 17,564 | 19,137 | 2,430 | 2,230 | 37,939 |
| 2000 | pipe | 92,029 * | 21,758 | 27,525 | 2,430 | 3,796 | 46,532 |
| 2000 | pipe | 102,330 * | 18,088 | 25,428 | 2,430 | 2,827 | 41,863 |
| 2000 | pipe | 110,679 | 18,088 | 19,137 | 2,430 | 2,244 | 38,705 |
| 2000 | eager | 111,783 | 17,564 | 20,185 | 1,540 | 2,530 | 38,337 |
| 2000 | eager | 107,474 | 18,612 | 19,137 | 1,540 | 2,680 | 39,871 |
| 2000 | eager | 102,061 * | 19,137 | 24,904 | 1,540 | 2,824 | 41,902 |
| 2000 | eager | 112,335 | 17,564 | 18,088 | 1,540 | 2,605 | 38,158 |
| 2000 | eager | 110,300 | 18,088 | 21,758 | 1,540 | 2,599 | 38,856 |

`*`: a neighbour was building (cycles per frame 10–20% above the other runs of the same thing).

- 8 connections: frames/s equal (medians 94,663 against 93,836, −0.9%, inside the spread), **p99 170 → 145 µs
  (−15%)**, p50 76.8 → 80.9 µs (+5%; the direct send runs inside the task poll of the frame), user
  instructions −24% (no SEND SQE/CQE, no second task wake), all cycles per frame equal (45.3k against 45.4k):
  the syscall costs what the SQE+CQE cost.
- 2000 connections: neutral. Medians of the undisturbed runs: 112,392 against 111,783 frames/s, 38,145
  against 38,337 cycles/frame; p50 equal, p99 18.6–19.1 against 18.1–21.8 ms (noisy). User cycles per frame
  are higher with `eager` (2,530 against 2,200) although user instructions are 37% fewer: the return-thunk /
  entry mitigations of the extra syscall are attributed to user mode.

### `--ring coop,skipenter,ringfd`

| conns | path | frames/s | p50 µs | p99 µs | user instr/frame | user cyc | cyc |
|---|---|---|---|---|---|---|---|
| 8 | pipe | 81,442 * | 91.1 | 198.7 | 2,122 | 1,665 | 52,531 |
| 8 | pipe | 92,964 | 80.9 | 170.0 | 2,126 | 1,457 | 46,039 |
| 8 | pipe | 89,289 | 82.9 | 174.1 | 2,102 | 1,471 | 47,976 |
| 8 | eager | 81,794 * | 91.1 | 186.4 | 1,674 | 1,599 | 52,305 |
| 8 | eager | 94,651 | 80.9 | 145.4 | 1,647 | 1,480 | 45,247 |
| 8 | eager | 90,159 | 82.9 | 161.8 | 1,652 | 1,555 | 47,510 |
| 2000 (4k) | pipe | 97,787 | 20,185 | 21,234 | 1,961 | 2,187 | 43,824 |
| 2000 | pipe | 97,701 | 20,185 | 21,234 | 1,961 | 2,143 | 43,866 |
| 2000 | pipe | 98,794 | 20,185 | 20,709 | 1,961 | 2,151 | 43,379 |
| 2000 | eager | 96,064 | 20,709 | 21,234 | 1,519 | 2,812 | 44,612 |
| 2000 | eager | 94,987 | 20,709 | 22,807 | 1,519 | 2,789 | 45,111 |
| 2000 | eager | 96,808 | 20,709 | 21,234 | 1,519 | 2,767 | 44,271 |

- `coop`, 8 connections: not worse here (pairs run back to back: 81.4/81.8k, 93.0/94.7k, 89.3/90.2k), p99
  lower. The −5% of the bench (run 120) did not show in this reproducer; its server has more task work per
  syscall exit (bulk connections, timeouts), this one has 8 idle-ish connections.
- `coop`, 2000 connections: **−2% frames/s** (medians 97,787 against 96,064), cycles per frame +1.8%, p50
  +0.5 ms. This is the "worse under coop" case: 2000 direct syscalls per round, each running task work.

### `metrics` build (one run each)

| ring | conns | path | frames/s | task polls/frame | SQEs/frame | CQEs/frame | enters/frame | frames/round | `send_ops` |
|---|---|---|---|---|---|---|---|---|---|
| defer | 8 | pipe | 93,922 | 1.175 | 2.006 | 2.006 | 0.149 | 6.7 | 375,787 |
| defer | 8 | eager | 96,014 | 1.008 | 1.008 | 1.008 | 0.141 | 7.1 | 0 |
| defer | 2000 | pipe | 112,143 | 2.000 | 2.000 | 2.000 | 0.025 | 40.0 | 448,710 |
| defer | 2000 | eager | 113,018 | 1.000 | 1.000 | 1.000 | 0.025 | 40.0 | 0 |
| coop | 8 | pipe | 93,818 | 1.037 | 2.004 | 2.004 | 0.130 | 7.7 | 375,370 |
| coop | 8 | eager | 94,655 | 1.006 | 1.006 | 1.006 | 0.128 | 7.8 | 0 |
| coop | 2000 | pipe | 98,373 | 1.000 | 2.000 | 2.000 | 0.001 | 2000 | 394,000 |
| coop | 2000 | eager | 96,079 | 1.000 | 1.000 | 1.000 | 0.001 | 2000 | 0 |

One SQE, one CQE and one task poll per frame on the eager path, no SEND SQE at all. Under `defer` at 2000 the
pipe path takes two task polls per frame (the send's CQE lands in a later tick than the receive's: 40 frames
per round) and the eager path one.

### Other runs (one each, functional)

- `--conns 9 --bulk 2` (MIX): eager 79,030 frames/s, p99 161.8 µs, bulk 577.7 MB/s; pipe 76,307, p99 231.4
  µs, bulk 612.1 MB/s.
- `--conns 8 --deadline timeout`, eager: 93,747 frames/s, p99 137.2 µs, 2,589 user instructions per frame
  (pipe + timeout in G0: 3,033). The timer is still registered per frame: the frame awaits the next receive.
- `--ring defer,skipenter,ringfd,fixedfd`, eager: 94,949 frames/s, p99 133.1 µs: the direct send works on a
  registered (fixed) stream, which keeps its fd.

## Cost for code that never uses the eager path

Compared against the base tree (`r3-g0`, built in a scratch copy with the same `Cargo.lock`), user
instructions per frame, 2000 connections unless said:

| binary / path | base | this branch | Δ |
|---|---|---|---|
| reqresp `pipe`, defer | 2,342 | 2,428 | +86 (+3.7%) |
| reqresp `pipe`, defer, 8 conns | 2,096 | 2,162–2,190 (varies with frames per round) | +66..94 |
| reqresp `pipe`, coop | 1,895 | 1,959 | +64 |
| reqresp `plain`, defer | 2,399 | 2,419 | +20 (+0.8%) |
| smallframes `--no-timeout` | 1,883 | 1,915 | +32 (+1.7%) |

Cycles per frame are inside the run-to-run noise (pipe at 8: 45.2–45.4k against 45.2k; at 2000: 37.9–38.7k
against 38.9k).

What the +86 on `pipe` is, from the disassembly of both binaries and `perf record -e instructions:u` on the
server thread:

- the guard itself is **one instruction** at op creation (`incl 0x14(%rsi)` on the `Rc` cache line, in
  `send_with_flags` / `recv_with`) and one at drop (`decl`), plus one store of `eager: 0` and one test of it in
  `complete`, and in `Send::uring_op` one test of `eager` (the rest is a `#[cold]` function). About 10
  instructions per frame for the two ops;
- the rest is **two inlining flips in the example binary**, not op cost: LLVM stopped inlining `Op::poll`
  into `<&mut RecvFuture>::poll` (base inlined it at that one site only; the call, prologue and the copy of
  the 80-byte `Completion` through memory are ~45 instructions), and the `conn` closure is no longer inlined
  into `CoreStage::poll` (~20). Evidence: with the base `write`/`writev` bodies and no-op guards the number is
  exactly 2,342; with the guards counting and the eager field in `Send` it is 2,416; with `#[inline]` on
  `Op::poll` (one line in `driver/op.rs`, r3-op's file) it is 2,330 on this branch and 2,311 on base.

With that one line in both trees (apples to apples, the inlining fixed):

| binary / path | base + hint | this branch + hint | Δ |
|---|---|---|---|
| reqresp `pipe`, defer | 2,311 | 2,330 | +19 (+0.8%) |
| reqresp `pipe`, defer, 8 conns | 2,064 | 2,083 | +19 |
| reqresp `plain`, defer | 2,286 | 2,312 | +26 |
| smallframes `--no-timeout` | 1,805 | 1,839 | +34 |

So the real cost of the guards and the eager field is 10–17 instructions per op pair; the measured +86 on
`pipe` is mostly the inliner's mood, which r3-op's rewrite of `Op::poll` / `OpFuture::poll_result` (E3) will
reshuffle anyway. **Finding for r3-op / the lead:** `#[inline]` on `impl Future for Op<T>::poll` is worth
−1.3% (pipe), −4.7% (plain) and −4.1% (smallframes) of user instructions per frame on the base tree alone,
most of O1's −5% target. The line is in "Requested changes".

The `write` body (`if self.fd.eager_send() { send_eager } else { send_op }`) costs 4 instructions when off.
Its first version inlined `send_eager` (a syscall loop, the probe, the ring fallback) into every `write`
caller and moved the `plain` path by +52 instructions; `#[inline(never)]` on `send_eager` and `writev_eager`
brought that back to +20.

## What was tried and rejected

- **A runtime flag in the guard** (`counted: bool`) for the ops that serve both files and sockets: would grow
  `ReadFixed`/`WriteFixed`; the offset tells them apart for free.
- **Counting on the file side** (`ReadAt`/`WriteAt`): files have no `try_*`, so the plan's "do not make file
  writes pay" wins; they are untouched.
- **A `ioprio`/`offset` pair of fields in `Send<T>`**: `Send<Bytes>` would go from 48 to 56 bytes and stop
  fitting the inline storage of dropped ops. One `u32` with the top bit for `POLL_FIRST` fits in the padding
  (a `send(2)` count is below 2^31).
- **`send_eager` returning an error for the ring part after a partial direct send** (e.g. `Err(ECANCELED)`
  after `cancel()`): the caller would not learn how many bytes were sent, and `write_all` would resend them.
  The short count is what `send(2)` does.
- **Smaller socket buffers (4 KiB) in the ordering test**: TCP over loopback with a 4 KiB window stalls in
  its persist timer (7.5 s per test); 16 KiB keeps the test at 1.3 s and still forces partial sends with
  64 KiB chunks.
- **A kernel-version check for `POLL_FIRST`** (`uname`): the probe of `IORING_OP_SOCKET` is one syscall per
  process and exact.
- **Inlining `send_eager` into `write`** (see above, +52 instructions on the plain path for everyone).
- **Adding `#[inline]` to `Recv::complete`**: no effect on the flip; reverted.

## Open questions

1. **The `+86` on `pipe` is an inlining artefact of this example binary**; the lead should measure the
   never-eager cost again after r3-op is merged (E3 rewrites exactly the two functions whose inlining flipped),
   and consider the `#[inline]` on `Op::poll` as an r3-op change (numbers above).
2. **Zero-copy sends count until the notification**, i.e. until the kernel released the buffer. On a real NIC
   that is after the data was acked, so a connection mixing `send_zc_*` with `try_send` / `send_eager` falls
   back to the ring for that long. Correct, but if someone wants the eager path next to zero-copy sends, the
   guard could be released at the first CQE in `SendZcFuture::poll` (the send is complete then); that needs a
   guard that can be disarmed early (one bool in `SendZcBuf`'s padding). Not done: no user of that mix.
3. **`RecvStream` keeps counting after it finished** (EOF, `ENOBUFS`, `cancel`) until it is dropped or
   re-armed, because `RecvMultiOp` keeps the op's data. "Late", and consistent with "don't mix `recv_multi`
   with other reads on the socket".
4. **Pipelined sends**: two ring sends in flight on one stream socket are not ordered by the kernel once the
   socket is full (both arm poll; LIFO wait queues). `send_eager` with a send in flight is "exactly `send_op`",
   so it inherits that, and with `POLL_FIRST` on the remainder a following plain `send_op` may even overtake
   it when the socket has room again. Not new; the docs say "await a send before the next" implicitly through
   the ordering rule. Worth a sentence in AGENTS.md.
5. **`coop` at 2000 connections loses 2%**, as expected; the docs say so. At 8 connections it did not lose
   here. The bench will tell at G1.
6. `try_recv` takes `&mut [u8]`; a `&mut [MaybeUninit<u8>]` variant would avoid zeroing the buffer first. Not
   requested.
7. The owned halves take `&self` for the `try_*` calls and `send_eager`, like the `_op` methods; the halves'
   `write` (`AsyncWriteRent`) still takes `&mut self`.

## Requested changes

### 1. Guards in files owned by other packages (branch `r3/r3-eager-requested`, commit `87e2563`)

Without these, `try_send` / `try_recv` / `send_eager` can overtake a `read_at_then_send` chain, a
`write_with_deadline` / `read_with_deadline`, a `recv_multi` / `recv_bundle` or a splice on the same socket
(the four tests of `tests/eager_send_guards.rs` fail on `r3/r3-eager` alone and pass with the branch). Tests
pass with and without `metrics`, and with `splice`. The diff (`git diff r3/r3-eager -- monoio/src` on the side
branch; the test file is the fifth file of that commit):

```diff
diff --git a/monoio/src/driver/op/link_timeout.rs b/monoio/src/driver/op/link_timeout.rs
--- a/monoio/src/driver/op/link_timeout.rs
+++ b/monoio/src/driver/op/link_timeout.rs
@@ -12,7 +12,7 @@ use io_uring::{opcode, types::Timespec};
 use super::{submit::SubmitResult, Completion, Op, OpAble, OpKind};
 use crate::{
     buf::{IoBuf, IoBufMut},
-    driver::shared_fd::sqe_fd,
+    driver::shared_fd::{sqe_fd, RxFd, TxFd},
     driver::{self, shared_fd::SharedFd, uring::UringInner, util::timespec},
     BufResult,
 };
@@ -65,8 +65,8 @@ pub(crate) fn untimed<T>(complete: Completion<Timed<T>>) -> Completion<T> {
 
 /// `recv` for [`Timed`]: the whole buffer capacity, no flags.
 pub(crate) struct Recv<T> {
-    /// Keeps the socket open while the op is in flight.
-    fd: SharedFd,
+    /// Keeps the socket open while the op is in flight, and counts as a receive in flight.
+    fd: RxFd,
     buf: T,
 }
 
@@ -83,8 +83,8 @@ impl<T: IoBufMut> OpAble for Recv<T> {
 
 /// `send` with `MSG_NOSIGNAL` for [`Timed`]: the initialized bytes of the buffer.
 pub(crate) struct Send<T> {
-    /// Keeps the socket open while the op is in flight.
-    fd: SharedFd,
+    /// Keeps the socket open while the op is in flight, and counts as a send in flight.
+    fd: TxFd,
     buf: T,
 }
 
@@ -106,6 +106,7 @@ pub(crate) async fn recv_with_timeout<T: IoBufMut>(
     buf: T,
     timeout: Duration,
 ) -> BufResult<usize, T> {
+    let fd = RxFd::new(fd);
     let op = match Op::submit_with_timeout(Recv { fd, buf }, timeout) {
         Ok(op) => op,
         Err((e, data)) => return (Err(e), data.buf),
@@ -126,6 +127,7 @@ pub(crate) async fn send_with_timeout<T: IoBuf>(
     buf: T,
     timeout: Duration,
 ) -> BufResult<usize, T> {
+    let fd = TxFd::new(fd);
     let op = match Op::submit_with_timeout(Send { fd, buf }, timeout) {
         Ok(op) => op,
         Err((e, data)) => return (Err(e), data.buf),
diff --git a/monoio/src/driver/op/recv_multi.rs b/monoio/src/driver/op/recv_multi.rs
--- a/monoio/src/driver/op/recv_multi.rs
+++ b/monoio/src/driver/op/recv_multi.rs
@@ -15,7 +15,7 @@ pub(crate) use crate::driver::buf_ring::Bundle;
 use crate::{
     driver::{
         raw::{BufRing, Cqe, SelectedBuf},
-        shared_fd::{sqe_fd, SharedFd},
+        shared_fd::{sqe_fd, RxFd, SharedFd},
         uring::{PreTake, Taken},
     },
     io::stream::Stream,
@@ -24,7 +24,9 @@ use crate::{
 /// Multishot recv: one CQE per received chunk, in one buffer of `ring` (or, with `bundle`, in
 /// several).
 pub(crate) struct RecvMulti {
-    fd: SharedFd,
+    /// Counts as a receive in flight on the socket (`try_recv` is refused) for as long as the
+    /// data lives: while the multishot is armed, and until the stream that owns it is dropped.
+    fd: RxFd,
     ring: BufRing,
     bundle: bool,
 }
@@ -120,7 +122,7 @@ impl RecvMultiOp {
             check_bundles(ring)?;
         }
         let data = RecvMulti {
-            fd: fd.clone(),
+            fd: RxFd::new(fd.clone()),
             ring: ring.clone(),
             bundle,
         };
@@ -362,7 +364,8 @@ impl RecvMultiOp {
 
 /// Single-shot bundle recv: one CQE, its data in one or more buffers of `ring`.
 pub(crate) struct RecvBundle {
-    fd: SharedFd,
+    /// Counts as a receive in flight on the socket.
+    fd: RxFd,
     ring: BufRing,
 }
 
@@ -377,7 +380,7 @@ impl OpAble for RecvBundle {
 pub(crate) async fn recv_bundle(fd: &SharedFd, ring: &BufRing) -> io::Result<Bundle> {
     check_bundles(ring)?;
     let op = Op::submit_with(RecvBundle {
-        fd: fd.clone(),
+        fd: RxFd::new(fd.clone()),
         ring: ring.clone(),
     })?;
     let done = op.await;
diff --git a/monoio/src/driver/op/splice.rs b/monoio/src/driver/op/splice.rs
--- a/monoio/src/driver/op/splice.rs
+++ b/monoio/src/driver/op/splice.rs
@@ -5,14 +5,18 @@ use std::io;
 use io_uring::opcode;
 
 use super::{super::shared_fd::SharedFd, Op, OpAble};
-use crate::driver::shared_fd::sqe_fd;
+use crate::driver::shared_fd::{sqe_fd, RxFd, TxFd};
 
 /// A splice between two fds. An offset of `-1` means "use (and advance) the fd's own position";
 /// it must be `-1` for the pipe end.
+///
+/// Either end may be a stream socket: the splice counts as a receive in flight on `fd_in` and
+/// as a send in flight on `fd_out` (see `InFlight` in `shared_fd.rs`), so `try_recv` /
+/// `try_send` on that socket are refused meanwhile. On a file or a pipe the count is unused.
 pub(crate) struct Splice {
-    fd_in: SharedFd,
+    fd_in: RxFd,
     off_in: i64,
-    fd_out: SharedFd,
+    fd_out: TxFd,
     off_out: i64,
     len: u32,
 }
@@ -43,9 +47,9 @@ impl Op<Splice> {
         len: u32,
     ) -> io::Result<Op<Splice>> {
         Op::submit_with(Splice {
-            fd_in: fd_in.clone(),
+            fd_in: RxFd::new(fd_in.clone()),
             off_in,
-            fd_out: fd_out.clone(),
+            fd_out: TxFd::new(fd_out.clone()),
             off_out,
             len,
         })
diff --git a/monoio/src/fs/file/chain.rs b/monoio/src/fs/file/chain.rs
--- a/monoio/src/fs/file/chain.rs
+++ b/monoio/src/fs/file/chain.rs
@@ -5,7 +5,11 @@ use io_uring::opcode;
 use super::File;
 use crate::{
     buf::IoBufMut,
-    driver::{raw::ChainOp, shared_fd::sqe_fd, CURRENT},
+    driver::{
+        raw::ChainOp,
+        shared_fd::{sqe_fd, TxFd},
+        CURRENT,
+    },
     net::TcpStream,
     BufResult,
 };
@@ -53,8 +57,10 @@ impl File {
             .flags(libc::MSG_NOSIGNAL | libc::MSG_WAITALL)
             .build());
         // The chain owns both fds (kept open until it finished) and the buffer, whose memory
-        // does not move with it (`IoBufMut` contract).
-        let data = (self.fd.clone(), stream.shared_fd().clone(), buf);
+        // does not move with it (`IoBufMut` contract). The socket's handle is a `TxFd`: the
+        // chain counts as a send in flight on the stream until its last CQE, so `try_send`
+        // and `send_eager` do not overtake it.
+        let data = (self.fd.clone(), TxFd::new(stream.shared_fd().clone()), buf);
         let sqes = crate::uring::link([read, send]);
         let op = match CURRENT.with(|inner| ChainOp::submit_in(inner, sqes, data)) {
             Ok(op) => op,
```

Wave 2: r3-pair's `send_then_recv` op data should hold a `TxFd` and an `RxFd` (one each; the pair is a send
and a receive in flight); r3-vec keeps the `TxFd` in `SendMsgVec`.

### 2. For r3-op (or the lead), `driver/op.rs`

Measured, see "Cost": `#[inline]` on `Op::poll` gives −1.3% (`pipe`), −4.7% (`plain`), −4.1% (`smallframes`)
user instructions per frame on the base tree, and takes this branch's `pipe` path below base.

```diff
--- a/monoio/src/driver/op.rs
+++ b/monoio/src/driver/op.rs
@@ -171,6 +171,7 @@ where
 {
     type Output = Completion<T>;
 
+    #[inline]
     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
```

### 3. AGENTS.md wording (for r3-docs)

Under "Core concepts", after "Socket flags and vectored sends":

> - **Eager sends (direct syscalls).** `TcpStream`/`UnixStream` (and the owned halves) have
>   `try_send(&[u8])`, `try_send_vectored(&[IoSlice])` and `try_recv(&mut [u8])`: one non-blocking
>   `send`/`sendmsg`/`recv` on the calling thread, no SQE, no CQE, no op slot. They return `WouldBlock` when
>   the socket is full/empty, when a ring op of that direction is still in flight on the socket (a direct call
>   must not overtake it; the stream counts its ring sends and receives, split halves included, and the count
>   can be late, never early), or when the stream is direct-only (no fd). `send_eager(buf)` /
>   `send_eager_with_flags` return the usual `SendFuture`: ready at its first poll when the direct send took
>   everything (or failed), else a ring `SEND` of the rest with `IORING_RECVSEND_POLL_FIRST`; the result counts
>   from the start of the buffer, and a ring failure after a partial direct send is reported as that short
>   count. `set_eager_send(true)` (per socket, seen by both halves; off by default) makes `write`/`write_all`
>   go through `send_eager` and `writev` try one direct `sendmsg` first. Rules: SQEs you build yourself
>   (`uring::submit`, chains, the hook) are not counted, don't mix them with the `try_*` calls; the direct call
>   is wasted on writes that usually fill the socket (bulk streams): use it for replies up to a few KiB; on
>   `coop_taskrun` rings every syscall runs the ring's pending task work on its way out (−2% at 2000
>   connections here), so use it on `defer_taskrun` rings or rings without task-run flags; a zero-copy send
>   counts until the kernel released its buffer, a `RecvStream` until it is dropped. Two ring sends in flight
>   at once on a full stream socket are not ordered by the kernel; await a send before starting the next.
>   `reqresp --path eager` is the pattern: `send_eager(&REPLY[..])`, then `recv_poll_first_op` for the next
>   frame, in one task poll: one SQE, one CQE, one task poll per frame.

## Checks

- `cargo test -p adaptif-gatotkaca-monoio`: all pass (76 test binaries incl. doc tests; `eager_send.rs` 18).
- `cargo test -p adaptif-gatotkaca-monoio --features metrics`: all pass (`eager_send.rs` 19).
- `cargo clippy --workspace` and `--features adaptif-gatotkaca-monoio/metrics`: the 3 old warnings
  (`net/udp.rs` ×2, `utils/slab.rs`) and nothing else; `clippy --tests --examples` clean for the new files.
- `cargo build --workspace --examples`, with and without `metrics`: ok. `cargo doc`: no new warnings.
- `cargo fmt` applied.
- Side branch: the same matrix plus `--features splice`, all pass.
- Not run: the bench repo (`sut-monoio` `eagerack`), gate runs on a quiet machine.
