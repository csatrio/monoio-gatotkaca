# Releases

Releases of `adaptif-gatotkaca-monoio` (library name `monoio`), newest first. The fork starts from upstream [Monoio](https://github.com/bytedance/monoio) 0.2.4 (`8675e94`). Open work is in [TODO.md](TODO.md).

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
