# r3-vec: `&self` vectored sends without an allocation per op (A1, E8, KV5)

Branch `r3/r3-vec` on `r3-g1`, two commits:

1. `feat(net): send_vectored_op, msghdr pool instead of a box per vectored send (r3-vec: A1)`
2. `perf(net): vectored sends as a plain SEND with IORING_SEND_VECTORIZED on 6.17+ (r3-vec: KV5)`

Files (all in the package's list): `driver/msghdr_pool.rs` (was empty), `driver/op/send.rs` (`SendMsgVec`),
`io/futures.rs` (one `named_future!` and its import), `net/tcp/vectored.rs`, `net/unix/vectored.rs` (were
empty), `net/tcp/split.rs` and `net/unix/split.rs` (the two methods on the write halves), `tests/named_futures.rs`
(extended), `tests/vectored_alloc.rs` (new). One registry line in `lib.rs`: `pub use driver::msghdr_pool::hooks
as __vectored` (`#[doc(hidden)]`, test hooks). Nothing in another package's files is committed; the `reqresp`
`vec` path used for the measurement is a local change, its diff is under "Requested changes".

All numbers: this machine, other agents building ("screening, machine not quiet"). User instructions per frame are
exact (two runs of a binary give the same number, ±1).

## Short answer

- `send_vectored_op(&self, bufs)` / `send_vectored_with_flags_op(&self, bufs, MsgFlags)` on `TcpStream`,
  `UnixStream`, `TcpOwnedWriteHalf`, `UnixOwnedWriteHalf` return `SendVectoredFuture<T>` (`Unpin + 'static`,
  same token API as the other named futures: `poll_result`, `is_terminated`, `cancel`). Same op and result as
  `send_vectored`; `MSG_NOSIGNAL` always. The facade can drop `into_split` for its vectored writes.
- **E8 adopted.** No `Box<libc::msghdr>` per vectored send. On this kernel (7.0) a vectored send is a plain
  `SEND` with `IORING_SEND_VECTORIZED` and needs no `msghdr` at all; on kernels before 6.17 it is `SENDMSG` with
  a `msghdr` from a per-thread free list. Both allocate nothing in steady state (test: 1000 sends, 0
  allocations, in each form).
- `reqresp` with a 3-iovec reply (header, body, trailer) at 2000 × 4 KiB, user instructions per frame:
  boxed `msghdr` (what `r3-g1` would do with the same API) 2,443 → pool 2,113 (−13.5%) → vectorized `SEND`
  2,100 (−0.6% more). At 8 connections 2,210 → 1,902 → 1,829.
- **KV5 adopted** (commit 2): the vectorized `SEND` is not slower than `sendmsg` + pool on either metric (−19
  user instructions per frame at both loads; all cycles per frame equal or lower, screening). Fallback to
  `sendmsg` + pool is automatic (a probe, once per process) and tested by forcing it.
- Inline storage: a dropped in-flight `SendMsgVec<Vec<libc::iovec>>` is 48 bytes and inline; every
  `IoVecArray` shape and `VecBuf` are boxed when dropped in flight (the buffer type is the size, not the op).
  `docs/TODO.md:35` needs rewording (below).
- The thread-local lookup does not show: 6 user instructions per frame on the fallback path, none worth moving
  into the driver.

## What was built

### `SendVectoredFuture` and the `_op` methods

`named_future! { SendVectoredFuture<T: IoVecBuf> => SendMsgVec<T> }` in `io/futures.rs`, with a `BufOp` impl
for `SendMsgVec` in `send.rs` (the op's `async fn result` now uses the same `complete`, as `Send` does, so the
rent method and the named future cannot diverge). The methods live in `net/tcp/vectored.rs` and
`net/unix/vectored.rs` (`impl TcpStream` / `impl UnixStream` blocks, the pattern of `eager.rs`,
`deadline.rs`), and the write halves forward to them in `split.rs`. `flags::send_vectored_with_flags` (the rent
path) is untouched and uses the same `Op::send_msg_vec`, so `send_vectored` gets the pool and the vectorized
`SEND` too.

### E8: the `msghdr` free list (`driver/msghdr_pool.rs`)

- `PooledMsgHdr(NonNull<Node>)`, `Node { hdr: libc::msghdr, next: *mut Node }` (`repr(C)`, 64 bytes): an idle
  `msghdr` is its own free-list node. `take()` pops the thread's list or boxes a new zeroed node (cold path);
  `Drop` pushes it back, or frees it when 256 are already idle (`MAX_IDLE`), so a burst of in-flight sends
  does not pin memory forever. The list is a `thread_local!` with a destructor that frees the idle nodes when
  the thread ends; `Drop` uses `try_with` and frees the box if the thread is already tearing down its locals.
- Only `msg_iov` / `msg_iovlen` are ever written; name, control and flags stay zero, so a reused `msghdr` needs
  no re-zeroing (unit test `taken_msghdr_is_zeroed_but_for_the_iovecs`).
- **When it goes back:** when the op data (`SendMsgVec`) is dropped. By r3-eager's analysis of the `TxFd`
  guard (which `SendMsgVec` keeps), that is after the CQE on every path: future completed (`take_finished`
  moves the data out, `complete` drops what the caller does not get back), future dropped in flight
  (`Lifecycle::Ignored` holds the data until the final CQE), SQ stayed full (never pushed, dropped at once).
  That satisfies KV4's rule "keep the `msghdr` and the iovec array until the op's CQE", on SQPOLL rings too.
- **Why not inline in the op data (ROUND3.md's proposal):** finding 8. The named futures are returned by value
  and `Unpin`; `OpFuture::new` moves the `Op`, so the op data moves after `uring_op()` built the SQE. KV4 showed
  the kernel reads the `msghdr` at the enter, i.e. after that move. An inline `msghdr` would be read at a stale
  address. (Building the `msghdr` inside `uring_op` does not help: the pointer in the SQE is what moves.)

### KV5: vectorized `SEND` (commit 2)

- `IORING_SEND_VECTORIZED = 1 << 5` in `sqe->ioprio` (defined locally with a comment; `io-uring` 0.7.15 has no
  constant). The SQE is `opcode::Send::new(fd, iov_ptr, iov_count).flags(MSG_NOSIGNAL | flags).ioprio(1 << 5)`.
  `SendMsgVec::msg` is `Option<PooledMsgHdr>` (`None` = vectorized `SEND`; one pointer either way, niche). The
  iovec array is the one `IoVecBuf::read_iovec_ptr` gives, owned by the buffer type (an `IoVecArray` boxes it
  once per connection), which lives as long as the op data: the same lifetime as the `msghdr` had.
- **Detection, once per process:** there is no feature bit, and the first real send cannot be the probe (a
  `SendVectoredFuture` cannot re-submit itself as `sendmsg` when its CQE says `-EINVAL`). So the first vectored
  send of the process opens a throwaway ring (`IoUring::new(2)`), submits a vectorized `SEND` with zero iovecs
  on fd `-1`, and reads the CQE: `-EINVAL` means the kernel rejected the flag at prep (before 6.17);
  anything else (`-EBADF`, from the fd lookup at issue time, on 6.17+) means it is supported. A kernel without
  the flag rejects at prep because `sr->flags & ~SENDMSG_FLAGS` is tested in `io_sendmsg_prep` before anything
  touches the file; a kernel with it imports zero iovecs successfully (`iovec_from_user` returns the fast iovec
  for `nr_segs == 0`) and fails only at `io_assign_file`. If the probe cannot run (ring setup fails) the answer
  is "no", and `sendmsg` always works. Cost: three syscalls and an mmap, once per process. A `with_ring`-based
  probe on the live ring would need a CQE delivered to a hook (not this package's files), and a kernel version
  test would miss backports; the throwaway ring is exact.
- The answer is cached in a process-wide atomic and copied into the per-thread state (one `Cell<u8>` next to the
  free list, so an op does one thread-local lookup). Per-thread override for tests and measurements:
  `monoio::__vectored::set_vectored_send(Some(bool) | None)` and `vectored_send_is_vectorized()`
  (`#[doc(hidden)]`, not API).
- Not done: flipping the mode when a vectorized send completes with `EINVAL`. The probe is the real SQE on the
  real kernel, so a false positive needs a kernel that accepts the flag at prep and rejects it at issue, which
  does not exist; and the flip would misfire on a user's bad `MsgFlags`. One compare on the error path saved.

### Inline or boxed when dropped in flight (`size_of::<SendMsgVec<T>>`, unit test `send_msg_vec_data_sizes`)

The op data is `TxFd` (8) + `T` + `Option<PooledMsgHdr>` (8) + `flags: u32` (4, padded). Inline limit 48, alignment 8.

| `T` | op data bytes | dropped in flight |
|---|---|---|
| `Vec<libc::iovec>` (24) | 48 | inline (`ignored_inline`; test `iovec_vec_is_inline_when_dropped`) |
| `VecBuf` (48: two `Vec`s) | 72 | boxed (unchanged from TODO.md's "SendMsgVec (72)") |
| `IoVecArray<Vec<u8>, 1>` (40) | 64 | boxed |
| `IoVecArray<Vec<u8>, 4>` (112) | 136 | boxed |
| `IoVecArray<Bytes, 4>` (144) | 168 | boxed (test `dropped_in_flight_keeps_buffers_until_the_cqe`: one allocation per dropped op, `ignored_boxed`) |

The `msghdr` move brought the fixed part from 24 to 24 bytes (a box and a pooled pointer are both 8); what makes
`IoVecArray` ops boxed is that the array stores its `N` buffers inline (`[Option<B>; N]`, 24–32 bytes each) plus
the iovec box and a length. Even `N = 1` with `Vec<u8>` is 64. Making `IoVecArray` ops inline would need the
buffers behind a pointer (one allocation per array, which it already has for the iovecs: the buffers could share
that box), a change to `buf/io_vec_array.rs` that is out of this package's scope and only matters for sends
dropped in flight (timeouts firing on a bulk writer). Open question below.

## Numbers

`reqresp --cpu 2 --client-cpus 3,4 --sizes 4k --path vec --ring defer,skipenter,ringfd --warmup 1 --duration 4`,
release builds, one target directory per variant, one run per line (the second run of a variant where there is
one). "boxed" is commit 1 with `msg: Box<libc::msghdr>` restored (what `r3-g1` does per vectored send), "pool"
is commit 1, the last two are commit 2's binary with `REQRESP_VEC=sendmsg` / `send` forcing the form.

### 2000 connections × 4 KiB (exact instructions; the rest screening)

| variant | frames/s | p99 µs | user instr/frame | user cyc | all cyc |
|---|---|---|---|---|---|
| boxed `msghdr` | 107,398 | 20,709 | **2,443** | 2,551 | 39,904 |
| pool (commit 1) | 107,294 | 23,331 | **2,113** | 2,560 | 39,903 |
| commit 2, `sendmsg` + pool forced | 107,697 / 108,545 | 24,904 / 19,137 | **2,119** | 2,460 / 2,411 | 39,786 / 39,483 |
| commit 2, vectorized `SEND` | 109,891 / 108,990 | 19,137 / 19,137 | **2,100** | 2,420 / 2,400 | 39,001 / 39,319 |
| `--path pipe` (one 70-byte `send_op`), commit 1 | 113,163 | 18,612 | 1,813 | 1,673 | 37,876 |

### 8 connections × 4 KiB

| variant | frames/s | p99 µs | user instr/frame | user cyc | all cyc |
|---|---|---|---|---|---|
| boxed `msghdr` | 131,148 | 119.8 | **2,210** | 1,358 | 32,635 |
| pool (commit 1) | 129,111 | 133.1 | **1,902** | 1,271 | 33,148 |
| commit 2, `sendmsg` + pool forced | 136,981 | 109.6 | **1,848** | 1,141 | 31,283 |
| commit 2, vectorized `SEND` | 137,362 | 109.6 | **1,829** | 1,150 | 31,200 |

Reading:
- Box → pool: −330 (2000) / −308 (8) user instructions per frame: a `malloc` + `free` of 56 bytes.
- Pool → vectorized `SEND`: −19 at both loads (no `msghdr` write, no pool pop/push, one thread-local access
  instead of two). All cycles per frame 39,0–39,3k against 39,5–39,8k (screening; the two sendmsg runs are
  the noisier ones). Kernel side the two forms share `io_sendmsg_prep` → `io_net_import_vec`; the vectorized one
  skips `io_msg_copy_hdr` (one `copy_from_user` of 56 bytes). Adopted by the rule "not slower on either metric".
- Commit 1's pool (2,113) against commit 2's forced `sendmsg` (2,119): the mode test (thread-local load, compare,
  branch) costs 6 instructions on the fallback path and is paid back three times over on the vectorized one.
  The thread-local is one `mov %fs:` with a lazy-destructor state check; moving the state into `UringInner`
  (r3-pair's file this wave) would save at most those 6 instructions on pre-6.17 kernels. Not worth a request.
- The 8-connection "pool" row is 50–70 instructions above commit 2's forced sendmsg; at 8 connections the
  frames-per-round mix varies between runs (r3-eager saw the same spread, 2,162–2,190). The 2000-row
  comparison (6) is the clean one.
- `vec` against `pipe`: +287 user instructions for the 3-iovec reply over a single 70-byte `send_op`. That is
  the reproducer's `IoVecArray` use (three `push`es writing an iovec and an `Option<&[u8]>` each, `clear()`
  through the drain iterator, `Option::take` of the array) plus the vectored SQE, not an allocation: the alloc
  tests pin that at zero. A facade that sends a header and a body as two iovecs instead of a copy into one
  buffer pays about this and saves the copy.

## Tests

- `tests/named_futures.rs` (extended): `SendVectoredFuture` is `Unpin + 'static` and storable in a field; the
  echo over every `_op` method now includes a 3-iovec `send_vectored_op` with an `IoVecArray` and a
  `send_vectored_with_flags_op(.., WAITALL)` with a `VecBuf`, over TCP and Unix in both directions; the split
  halves of both stream types send through the new methods.
- `tests/vectored_alloc.rs` (new, own counting `#[global_allocator]`):
  - 1000 vectored sends after 64 of warm-up allocate nothing, in the probe's form, with `sendmsg` + pool forced
    (one idle `msghdr` left on the thread) and with the vectorized `SEND` forced (no `msghdr` at all); the rent
    `send_vectored` too;
  - a send into a 4 KiB `SO_SNDBUF` is short with the same count and the same bytes on the wire as
    `send_vectored`, and `advance` + resend delivers 900 KB intact;
  - `both_forms_send_three_iovecs`: three iovecs (4 + 5000 + 6 bytes) arrive in order and complete, with and
    without `MSG_WAITALL`, on TCP and Unix, both directions, in each form the kernel supports;
  - `forced_vectorized_form_matches_the_probe`: forcing the vectorized form gives `EINVAL` exactly on the
    kernels where the probe said no (on this kernel: it works);
  - dropped in flight (`async-cancel`): the gather list is alive until the CQE, dropped exactly once after it,
    one box per op (`ignored_boxed = 64`, `cancels_pushed = 64` with `metrics`), and the `msghdr`s are back
    on the list; a `Vec<libc::iovec>` send dropped in flight is inline (`ignored_inline = 64`, 0 allocations);
  - `cancel()` on a send waiting for room returns `ECANCELED` (or a short count if the kernel took some bytes
    first, as KV3 documents) and the buffers come back whole.
- Unit tests: the free list reuses and caps at 256; a reused `msghdr` has no name/control/flags; the override is
  per thread and the probe is stable across threads; `SendMsgVec` sizes for the five buffer shapes above.

Test setup note: the "socket full" tests `try_send` until `EAGAIN`, wait 60 ms for the delayed ACKs to free what
was in flight, and fill again (three times); otherwise a send started right after the first `EAGAIN` can make a
few KiB of progress and the cancel test sees a count instead of `ECANCELED` (the test accepts both anyway).

## What was tried and rejected

- **`msghdr` inline in the op data** (ROUND3.md's wish): unsound by finding 8 + KV4, see above. Not tried.
- **A `Vec<Box<msghdr>>` free list:** the intrusive list needs no second allocation and no capacity; same cost
  otherwise. Kept the intrusive one.
- **Mode in a process-wide atomic only (no per-thread cell):** one less thread-local access on the fallback
  path, but then tests cannot force a form without affecting the other test threads of the binary, and the
  measurement switch would be global. The per-thread cell shares the thread-local of the pool, so the
  vectorized path still does exactly one lookup.
- **Flip to `sendmsg` on a vectorized send's `EINVAL`:** rejected, see KV5 above.
- **Kernel version test instead of the probe:** simpler, but blind to backports and to a build without the
  flag; the probe is exact and costs a few syscalls once.
- **Probe on the live ring via `with_ring`:** needs a CQE route (hook or a driver `user_data`), which is
  r3-pair's file; and the first send would still have to wait for it.

## Open questions

1. `IoVecArray` ops are always boxed when dropped in flight (64 bytes at `N = 1`). If a load drops vectored
   sends in flight at a rate that matters (a `timeout` around a vectored bulk write), `IoVecArray` could keep
   its `[Option<B>; N]` in the same box as the iovecs (one allocation per array, as today) and shrink to a
   pointer plus a length: 16 bytes, 40 with the op's fixed part, inline. It changes `buf/io_vec_array.rs`
   (not this package's file) and the array's `Deref`-style accessors. Measure before doing it.
2. `send_vectored_zc` (`SENDMSG_ZC`, `driver/op/send_zc.rs::SendMsgZc`) still boxes its `msghdr`. On 6.17+
   `SEND_ZC` takes `IORING_SEND_VECTORIZED` too (KV5 probe case D), and the pool serves the fallback. Same
   change, r3-pair's or a later package's file.
3. `SendMsg` / `SendMsgUnix` (UDP, Unix datagram) box `(addr, IoVecMeta, msghdr)` per datagram. The pool could
   take a larger node; the `IORING_SEND_VECTORIZED` form cannot carry an address. Out of scope here.
4. Whether `TcpStream::try_send_vectored` + `send_vectored_op` should get an eager variant like `send_eager`
   (`send_vectored_eager`): the facade's writes are the request/response replies S1 targets. Nothing here
   blocks it; `SendVectoredFuture::ready` exists (generated by the macro, `#[allow(dead_code)]` until used).

## Requested changes

### AGENTS.md (r3-docs)

1. "Socket flags and vectored sends", after "`writev` still uses `IORING_OP_WRITEV`)": add

   > On Linux 6.17+ a vectored send is a plain `SEND` with `IORING_SEND_VECTORIZED` (no `msghdr`); older kernels
   > get `sendmsg` with a `msghdr` from a per-thread free list (probed once per process). Either way nothing is
   > allocated per send.

2. "Named op futures / token API": add `SendVectoredFuture` to the list of future types, and
   `send_vectored_op`, `send_vectored_with_flags_op` to the `TcpStream`/`UnixStream` (and owned halves) method
   list. Suggested sentence after the list: "`send_vectored_op(bufs)` is `send_vectored` as a named future
   (`sendmsg`/vectorized `SEND` with `MSG_NOSIGNAL`); with an `IoVecArray` kept per connection a vectored
   write allocates nothing."

3. "Dropped in-flight ops": after "larger data is boxed" add "(`IoVecArray` and `VecBuf` sends: the buffer type
   is over the limit; a bare `Vec<libc::iovec>` send is inline)".

### docs/TODO.md (r3-docs)

- Line 19: remove `send_vectored` from "Named futures (`_op` methods) are missing for `send_vectored`,
  `write_zc`, and `File` `read`/`readv`" → "... missing for `write_zc` and `File` `read`/`readv`".
- Line 35: "Dropped in-flight op data is stored inline up to 48 bytes; `ReadFixed` (64) and `SendMsgVec` (72)
  are still boxed." → "Dropped in-flight op data is stored inline up to 48 bytes; `ReadFixed` (64) is still
  boxed, and so is any vectored send whose buffer type is over 24 bytes (`VecBuf`, every `IoVecArray`; a bare
  `Vec<libc::iovec>` is inline). Making `IoVecArray` ops inline needs the array's buffers behind its existing
  iovec box (r3-vec.md, open question 1)."
- Optionally add: "`send_vectored_zc` and the datagram `SendMsg`/`SendMsgUnix` still box a `msghdr` per op;
  the pool in `driver/msghdr_pool.rs` and `IORING_SEND_VECTORIZED` (for `SEND_ZC`) cover both (r3-vec.md,
  open questions 2 and 3)."

### `reqresp` (r3-repro's files): a `vec` path, used for the numbers above, not committed

`monoio/examples/reqresp/paths/vec.rs` is `pipe.rs` with the reply as three iovecs (`&REPLY[..17]`,
`[17..68]`, `[68..]`) through `send_vectored_op`, one `IoVecArray<&'static [u8], 3>` per connection
(`Option`, taken for the send and put back after `clear()`; **not** `std::mem::take`, whose `Default` boxes a
new iovec array per frame and cost 370 instructions in the first measurement), `advance(n)` + resend after a
short send, and in `setup` a measurement switch `REQRESP_VEC=sendmsg|send` that calls
`monoio::__vectored::set_vectored_send` on the server thread. Wiring: `pub mod vec;`, `Path::Vec` ("vec") in
`parse`/`name`/`setup`/`conn` of `paths/mod.rs`, and `| vec` in the `--path` usage line of `main.rs`. Apply if
G2 wants the A1 row reproduced in the fork; the file is in the worktree
(`.claude/worktrees/agent-a13997acc4006a5b8/monoio/examples/reqresp/paths/vec.rs`), untracked.

### For the bench facade (`gatotkaca-transport-monoio`, lead)

`StreamIo` can hold the `TcpStream` directly and use `recv_op` / `send_op` / `send_vectored_op` (`&self`) with
no `into_split` and no `Cell<Option<Half>>` shuffle. For the gather write keep one
`IoVecArray<B, N>` per connection (`push` the pieces, `send_vectored_op`, then `drain()` the buffers back or
`clear()`), and after a short send `advance(n)` and send again. `send_vectored_op` always adds `MSG_NOSIGNAL`;
`writev_op` does not. Nothing allocates per write on either kernel generation; on 6.17+ the SQE carries the
iovec pointer itself.

## Checks

- `cargo test -p adaptif-gatotkaca-monoio`: all 84 test binaries pass (commit 1 and commit 2).
- `cargo test -p adaptif-gatotkaca-monoio --features metrics`: passes (commit 2; the vectored binaries also
  on commit 1).
- `cargo clippy --workspace` and `--features adaptif-gatotkaca-monoio/metrics`: the 2 old warnings
  (`net/udp.rs`), nothing new. One new warning seen on the way (`NonNull::new_unchecked(Box::into_raw(..))`)
  was replaced by `NonNull::from(Box::leak(..))`, which is safe and within the 1.85 MSRV.
- `cargo fmt` applied.
