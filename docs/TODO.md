# TODO

What is still open after round 3 (0.9.0). Item ids (A1, F7, ...) refer to [performance-improvement/ROUND1.md](performance-improvement/ROUND1.md); the gate files of each round are under `performance-improvement/round{1,2,3}/`.

## Measure first
Measured so far (do not re-open): `defer` against `coop` (round 3 G0/G1: many connections → `defer`), `skip_empty_enter` and `register_ring_fd` (in every round-3 variant, not isolated), `min_wait` (round 3 G1, `minwait80`), `task_budget`/`lifo_slot`/`small_io_priority` (round 2 G2/G3), multishot accept (round 2 G0, round 3 G0/G1), multishot recv and the working set (round 2 G3, round 3 G1), fixed files and direct accept (round 2 G3, round 3 G1), fixed buffers and `zc` (round 2 G0/G2), `drain_taskrun`, `send_eager`, `Deadline` (round 3 G1). All on loopback.
- Recv bundles (`recv_multi_bundle`, `recv_bundle`) and `send_zc_early` have no gate run of their own.
- Confirm the TLS write counts (A15) with `strace -c` on a real 64 KiB TLS echo. Today they come from a counting wrapper in `tests/tls_throughput.rs`.
- `event_channel`: the 1/4/8-producer comparison against the 0.6.0 mutex queue was a throwaway test (1M `u64` messages, best of 5). Add it as a proper benchmark and run it on the target hardware, also with larger `T`.
- Fixed files (A3) and direct accept: measured on loopback only (round 3 G1: direct accept +0.4% against `register_fixed`, 3 syscalls fewer per connection). Measure on a real NIC, and on a connection-churn load (accept and close per request), where the per-connection savings would show.
- Zero-copy receive (`zcrx`) and NAPI busy poll need a real NIC (ROUND3.md, "Not asked").

## Not done from ROUND1.md
- **A14, coalesced cancel-by-fd on drop.** Several dropped ops of one fd still push one `AsyncCancel` each. `IORING_ASYNC_CANCEL_FD | ALL` would also cancel ops on that fd that are still wanted (the other split half, other futures). It needs per-fd counting of live vs dropped ops in `SharedFd`.
- **A15 row 5, rustls unbuffered API.** TLS still copies twice in each direction (socket → staging buffer → rustls → user).
- **A3, direct-only Unix sockets.** `UnixListener` has no `accept_direct`/`accept_multi_direct`/`accept_batch_direct_into`; a `UnixStream` always has an fd (`register_fixed` works). `BatchAcceptor::accept_direct` works on any listening fd, so the Unix twin is small.

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
- `DriverStats` doesn't count fixed vs raw SQEs (`fixed_file_stats()` covers table use only).
- The socket-control calls (`shutdown_with`, `shutdown_both`, `set_sockopt`, `get_sockopt`, `set_nodelay_async`, `set_tcp_keepalive_async`) are not on the owned halves; a supervisor needs the `TcpStream` (e.g. in an `Rc`). Their futures are `impl Future` (not `Unpin`, not nameable); `get_sockopt` is `unsafe` and has no safe `get_sockopt_int` form.
- `accept_batch_direct_into` loses the connection the kernel accepted when the slot range is full (`ENFILE`); `accept_direct` checks for a free runtime slot first and does not. A loss-free batch variant would have to submit single direct accepts into runtime slots.
- `try_recv` takes `&mut [u8]`; no `&mut [MaybeUninit<u8>]` variant.
- A zero-copy send counts as "in flight" for `try_send`/`send_eager` until the kernel released its buffer (after the ACK on a real NIC), so a connection that mixes `send_zc_*` with the eager path falls back to the ring for that long. Releasing the guard at the first CQE needs a guard that can be disarmed early.
- `wait_free` is notify-one: a waiter that resolves and starts no receive strands the others until the next buffer (`next_rearm` claims and re-arms in one poll, so it has no such problem).
- `Deadline`: no `metrics` counters for timers (wheel inserts/removals, fired, swallowed); they exist only in unit tests.
- `IORING_ENTER_ABS_TIMER` (6.12+) would save the clock read before a park while timers are pending; needs a `park_until` in the `Driver` trait.

## Known limits (documented, may be worth revisiting)
- Multishot CQE queues are unbounded per op (`DriverStats::multishot_queue_high_water` shows the peak). There is no backpressure.
- Dropping a raw `MultiOp` does not inspect its remaining CQEs, so fds or provided buffers in them leak. The typed streams (`AcceptStream`, `RecvStream`, ...) handle this.
- `msg_ring_nowait` keeps target handles alive until the driver's submitted-SQE count passes them. It can only hold them too long, never release them early.
- `BufRing` hugetlb pools fail when no huge pages are reserved instead of falling back.
- Dropped in-flight op data is stored inline up to 48 bytes; `ReadFixed` (64) and `SendMsgVec` (72) are still boxed.
- The submit-failure path (SQ can't be flushed) has no test that forces it.
- `IORING_SQ_TASKRUN` can stay set with nothing pending after an enter that asked for more than one completion (`min_wait`): `enter_left_taskrun` over-counts and `skip_empty_enter` makes one enter it could have skipped. No user-space fix: the ring does not expose "nothing pending".
- `drain_taskrun` runs only after a park that waited, not after the submit-only park (`sync`, foreign wakers) and not on the cold-path submit.
- A working-set `BufRing` under-reports demand for raw users that take at poll time (raw `MultiOp`s, single-shot receives), so it shrinks too far and relies on `ENOBUFS` to grow; at 8 connections the ring does not come to rest (a handful of `ENOBUFS` per run). Taking at reap time on every ring would make `in_use` exact but costs about 75 user instructions per chunk.
- A `shutdown` of a direct-only socket always costs an io-wq round trip (`REQ_F_FORCE_ASYNC` in the kernel); a sweeper that ends many connections in one tick wakes io-wq workers.
- A main future that wakes itself is polled again without the runtime reaching the driver, so timers do not fire and `Instant::recent()` does not move in such a loop (`runtime.rs`, `while should_poll()`).
- `RecvFuture::cancel` (and the other named futures' `cancel`) go through the current driver, so calling one outside a runtime panics; polling and dropping outside a runtime are fine.

## Cleanup
- `BufRing::options()` was a workaround before `BufRingOptions` was exported; `BufRingOptions::new()` does the same now.
- Old clippy warnings: `net/udp.rs` (let_and_return), `tests/async_write_rent_boxed.rs`, `tests/async_write_rent_cursor.rs`.
- 16 old rustdoc warnings (unresolved intra-doc links in `builder.rs`, `fs/file/mod.rs`, `net/udp.rs`, `time/interval.rs`, `net/tcp/cancel.rs`, `io/async_write_rent.rs`, `macros/join.rs`, `net/unix/socket_addr.rs`).
- `rustfmt.toml` sets nightly-only options (import grouping, comment wrapping); with stable rustfmt they are ignored. Run nightly `cargo fmt` once to normalize.
- `time/driver/entry.rs`, `same_waker`: its comment says the task wakers' vtable is a promoted constant with one address per inlining site; `task/waker.rs` now has one `#[inline(never)]` vtable per task type, so `Waker::will_wake` holds and the content comparison is only a fallback. Update the comment, or drop the fallback.
- `reqresp`: `--path pair` is a stub until r3-pair lands; the `RESULT` line has no `accept=direct|fd` label.
