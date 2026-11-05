# Releases

Releases of `adaptif-gatotkaca-monoio` (library name `monoio`), newest first. The fork starts from upstream [Monoio](https://github.com/bytedance/monoio) 0.2.4 (`8675e94`). Open work is in [TODO.md](TODO.md).

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
