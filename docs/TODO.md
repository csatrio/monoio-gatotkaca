# TODO

What is still open after 0.6.0. Item ids (A1, F7, ...) refer to [performance-improvement/ROUND1.md](performance-improvement/ROUND1.md).

## Measure first
Everything in round 1 was checked by tests only. Nothing has been benchmarked.
- Re-run the transport's `--tune defer` variant. F7 (completions waiting until `park()` on DEFER_TASKRUN rings) may explain why it showed no difference in runs 26–28. Compare `DriverStats` with and without the fix.
- Benchmark each new opt-in behind a `--tune` flag (ROUND1.md section 5): `skip_empty_enter`, `register_ring_fd`, `min_wait`, `task_budget`, multishot accept/recv, recv bundles, fixed files, fixed buffers, `send_zc_early`.
- Confirm the TLS write counts (A15) with `strace -c` on a real 64 KiB TLS echo. Today they come from a counting wrapper in `tests/tls_throughput.rs`.
- Fixed files (A3): only a quick, uncommitted benchmark was run (about 5–7% on single file reads, no difference on loopback TCP ping-pong). Measure on a real NIC.

## Not done from ROUND1.md
- **A14, coalesced cancel-by-fd on drop.** Several dropped ops of one fd still push one `AsyncCancel` each. `IORING_ASYNC_CANCEL_FD | ALL` would also cancel ops on that fd that are still wanted (the other split half, other futures). It needs per-fd counting of live vs dropped ops in `SharedFd`.
- **A15 row 5, rustls unbuffered API.** TLS still copies twice in each direction (socket → staging buffer → rustls → user).
- **A3, direct-only Unix sockets and files.** They only support registered mode (`register_fixed`), not accept-into-slot.

## Gaps in the new APIs
- Named futures (`_op` methods) are missing for `send_vectored`, `write_zc`, and `File` `read`/`readv`. Each is one `named_future!` line plus a `BufOp` impl.
- `read_with_deadline` / `write_with_deadline` exist only on `TcpStream`; add them for UDP and Unix sockets.
- `send_zc_early` covers single buffers only; vectored zero-copy (`send_vectored_zc`) waits for the notification.
- Fixed buffers: no `FixedBufPool::get_or_wait()`, no `recv_fixed`/`send_fixed` on `UnixStream`, no `write_all_fixed` helper.
- `send_fixed` uses `WRITE_FIXED`, so it has no `MSG_NOSIGNAL`. The kernel rejects fixed buffers on plain `SEND`; `send_zc_fixed` is the alternative.
- `accept_with(AcceptFlags)` is missing on `UnixSeqpacketListener`.
- `register_fixed` and `accept_direct` take no cancel handle.
- `fadvise` lengths above `u32::MAX` are rejected (io-uring 0.7.15 can't set the 64-bit length).
- No helper to set the pipe size (`F_SETPIPE_SZ`) for splice.
- `EventSender` has no fast path through `msg_ring` when the sender is itself a ring thread.
- `DriverStats` doesn't count fixed vs raw SQEs (`fixed_file_stats()` covers table use only).

## Known limits (documented, may be worth revisiting)
- Multishot CQE queues are unbounded per op (`DriverStats::multishot_queue_high_water` shows the peak). There is no backpressure.
- Dropping a raw `MultiOp` does not inspect its remaining CQEs, so fds or provided buffers in them leak. The typed streams (`AcceptStream`, `RecvStream`, ...) handle this.
- `msg_ring_nowait` keeps target handles alive until the driver's submitted-SQE count passes them. It can only hold them too long, never release them early.
- `BufRing` hugetlb pools fail when no huge pages are reserved instead of falling back.
- Dropped in-flight op data is stored inline up to 48 bytes; `ReadFixed` (64) and `SendMsgVec` (72) are still boxed.
- The submit-failure path (SQ can't be flushed) has no test that forces it.

## Cleanup
- `BufRing::options()` was a workaround before `BufRingOptions` was exported; `BufRingOptions::new()` does the same now.
- Old clippy warnings: `net/udp.rs` (let_and_return), `utils/slab.rs`, `tests/async_write_rent_boxed.rs`, `tests/async_write_rent_cursor.rs`.
- 18 old rustdoc warnings (unresolved intra-doc links in `builder.rs`, `fs/file/mod.rs`, `net/udp.rs`, `time/interval.rs`, the README and others).
- `rustfmt.toml` sets nightly-only options (import grouping, comment wrapping); with stable rustfmt they are ignored. Run nightly `cargo fmt` once to normalize.
