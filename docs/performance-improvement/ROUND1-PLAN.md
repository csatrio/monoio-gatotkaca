# Round 1 work plan

Implements [ROUND1.md](ROUND1.md). Item ids (F1–F7, A1–A15) refer to that document.

## Rules for every work package
- New behaviour is opt-in (builder method, cargo feature or new API). With default features and a default builder, the runtime behaves as before.
- No panic on a submit path. Errors come back as `io::Result` / `BufResult` (buffer returned).
- Kernel-dependent features check support at runtime and fail with `Unsupported`/`InvalidInput`. Tests skip when the kernel lacks support.
- Each package adds tests in `monoio/tests/` and passes `cargo test -p adaptif-gatotkaca-monoio` (plus its features), `cargo clippy --workspace` and `cargo fmt`.
- Packages in one wave own disjoint files, so they run in parallel. Waves run in order, because later packages build on earlier driver changes.

## Wave 1 (parallel)

| Package | Items | Owns |
|---|---|---|
| W1-core | F2 (no `unimplemented!` on push), F4 (cancel retry), F7 (`defer_taskrun` adds `taskrun_flag`), A9 (`IoUringDriver::stats()`), A10 (skip empty `enter`, builder setup methods incl. `cq_size`, `register_ring_fd`, `min_wait`, task budget) | `driver/uring/mod.rs`, `driver/uring/waker.rs`, `driver/mod.rs`, `builder.rs`, `runtime.rs`, `time/`, `lib.rs` |
| W1-net | F1 (no `unwrap` on submit paths), A2 (accept flags), F6 + A7 (socket `writev` via `SendMsg` + `MSG_NOSIGNAL`, send/recv flags, `POLL_FIRST`, `IoVecArray`) | `net/`, `fs/`, `io/`, `buf/`, `driver/op/*.rs` (not `driver/op.rs`) |
| W1-tls | A15 rows 1–4 (buffer size, `write_vectored`, real `writev`, opt-in write coalescing) | `tls/` |
| W1-bufring | A6 (batched tail publish, `mmap` page-aligned lazy pool, `IoBuf` for `SelectedBuf`, `detach`, counters, `IOU_PBUF_RING_INC`, kernel-allocated ring option) | `driver/buf_ring.rs` |

Merge: apply the four diffs, wire the buffer-ring flush into the driver tick, run the full test suite.

## Wave 2 (parallel)

| Package | Items | Owns |
|---|---|---|
| W2-multishot | F3 + A1 (`OpAble` op kind, per-op CQE queue, `uring::submit_multishot` / `MultiOp`, typed `accept_multi` and `recv_multi` with optional bundles) | `driver/op.rs`, `driver/uring/lifecycle.rs`, `driver/uring/mod.rs`, `driver/raw.rs`, new `net/tcp/multishot.rs` |
| W2-futures | A8 (named op futures and a poll-style token API) | `net/tcp/stream.rs`, `net/unix/stream.rs`, `fs/file/`, new `*/futures.rs` |
| W2-fileops | A12 (`fadvise`, `splice_to_pipe`, `register_iowq_max_workers`) | new `driver/op/fadvise.rs`, `fs/file/` additions in a new file, `builder.rs` |

## Wave 3 (parallel)

| Package | Items | Owns |
|---|---|---|
| W3-chains | A4 (`submit_chain`, link timeouts, `read_with_deadline`) | `driver/raw.rs`, `driver/uring/mod.rs`, new `driver/chain.rs` |
| W3-fixedbuf | A5 (`FixedBufPool`, `FixedBuf`, fixed read/write/send_zc) | new `driver/fixed_buf.rs`, new fixed ops |
| W3-msgring | A11 (`msg_ring`, `RingHandle`, eventfd channel without `sync`) | new `driver/msg_ring.rs` |
| W3-bundles | A6 recv bundles: `BufRing::take_bundle` and `recv_multi_bundle` (`IORING_RECVSEND_BUNDLE`, 6.10+) | `driver/buf_ring.rs`, `net/*/multishot.rs`, `driver/op/recv_multi.rs` |

## Wave 4

| Package | Items | Owns |
|---|---|---|
| W4-fixedfiles | A3 (fixed file table, `FdRef` in `SharedFd`) | `driver/shared_fd.rs`, every op's SQE builder |
| W4-zc-cancel | A13 (early send result, fixed-buffer `SEND_ZC`, `SENDMSG_ZC`), A14 (`cancel_fd`, inline ignored data) | `driver/op/send.rs`, `driver/uring/lifecycle.rs` |

A3 touches every op, so it runs alone in its wave apart from W4-zc-cancel, which only touches send and the lifecycle.

## Commits
One commit per wave on `main`, made after the wave's diffs are merged and the full suite passes. Each commit message lists the items it covers.

## After all waves
- Update `AGENTS.md` with the new APIs.
- Bump `adaptif-gatotkaca-monoio` to 0.6.0 (new API, baseline unchanged), in its own commit.
- Update ROUND1.md item status.

## Status

| Wave | State |
|---|---|
| 1 | done: F1, F2, F4, F7, A2, A6, A7, A9, A10, A15 rows 1–4. Also fixed: `IoVecMeta` iovec length and `consume` offset bugs, `read_vectored_exact` not setting init. Not done: A6 recv bundles (moved to W2-multishot). |
| 2 | done: F3, A1, A8, A12; `submit_with_data` returns `data` on failure (no-op flush trick removed). Not done: recv bundles (need `BufRing::take_bundle`, moved to wave 3). |
| 3 | done: A4, F5, A5, A11, A6 recv bundles. Notes: `send_fixed` uses `WRITE_FIXED` (kernel rejects fixed buffers on `SEND`); a short read breaks an `IO_LINK`, so read→send chains never send stale bytes; bundles track per-buffer links, not a consumer head. |
| 4 | done (released as 0.6.0): A3 (+ A2 `accept_direct`), A13, A14. Not done: coalescing dropped ops into one cancel-by-fd (it would also cancel ops that are still wanted); direct-only mode for Unix sockets and files. |
