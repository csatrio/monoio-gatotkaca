# r3-direct (K1): fd-less sockets a server can use

Branch `r3/r3-direct`, base `r3-g0`. Everything is new API; with a default builder and the existing calls
nothing changes (one exception, on purpose: see "Behaviour changes").

All timings below are **screening, machine not quiet** (six other agents were building and measuring).
Syscall and SQE counts and instructions per frame are exact.

## What changed

| file | change |
|---|---|
| `monoio/src/driver/op/shutdown.rs` | `pub(crate) fn shutdown(&SharedFd, how)` (syscall with an fd, `IORING_OP_SHUTDOWN` without), `how_to_raw`; `Shutdown: SKIP_CANCEL = true` |
| `monoio/src/driver/op/sockopt.rs` | new op `SockOpt` (`IORING_OP_URING_CMD` + `SOCKET_URING_OP_{SET,GET}SOCKOPT`), `pub(crate) fn set`, `pub(crate) unsafe fn get` |
| `monoio/src/driver/op/accept_multi.rs` | `BatchAcceptor::accept_direct`; `accept` and `accept_direct` share the old loop (`run`, with a `direct` flag). `accept`'s signature is unchanged (`net/unix/multishot.rs` calls it) |
| `monoio/src/net/tcp/sockctl.rs` | `impl TcpStream`: `shutdown_with`, `shutdown_both`, `set_sockopt`, `get_sockopt`, `set_nodelay_async`, `set_tcp_keepalive_async` |
| `monoio/src/net/unix/sockctl.rs` | `impl UnixStream`: `shutdown_with`, `shutdown_both`, `set_sockopt`, `get_sockopt` |
| `monoio/src/net/tcp/listener.rs` | `accept_batch_direct`, `accept_batch_direct_into`; field `batch_direct: BatchAcceptor` |
| `monoio/src/net/tcp/stream.rs` | `shutdown_write` calls the shared `shutdown`; the sync option calls on a direct-only stream return an error that names the async calls; docs. `write`/`writev` untouched |
| `monoio/tests/direct_socket.rs` | 13 tests (new file) |
| `monoio/examples/reqresp/accept.rs` | `--direct` |

Not touched: `net/unix/stream.rs`, `net/unix/listener.rs`, `net/tcp/multishot.rs`, `net/unix/multishot.rs`,
`driver/shared_fd.rs`, `builder.rs`, `reqresp/main.rs`. No registry (`pub use`) line was needed: every new item
is a method.

## API (exact signatures)

```rust
// monoio::net::TcpStream  (net/tcp/sockctl.rs)
pub fn shutdown_with(&self, how: std::net::Shutdown) -> impl Future<Output = io::Result<()>>;
pub fn shutdown_both(&self) -> impl Future<Output = io::Result<()>>;
pub fn set_sockopt<T: Copy>(&self, level: i32, name: i32, value: &T) -> impl Future<Output = io::Result<()>>;
pub unsafe fn get_sockopt<T: Copy>(&self, level: i32, name: i32) -> impl Future<Output = io::Result<T>>;
pub fn set_nodelay_async(&self, nodelay: bool) -> impl Future<Output = io::Result<()>>;
pub fn set_tcp_keepalive_async(&self, time: Option<Duration>, interval: Option<Duration>, retries: Option<u32>)
    -> impl Future<Output = io::Result<()>>;

// monoio::net::UnixStream  (net/unix/sockctl.rs)
pub fn shutdown_with(&self, how: std::net::Shutdown) -> impl Future<Output = io::Result<()>>;
pub fn shutdown_both(&self) -> impl Future<Output = io::Result<()>>;
pub fn set_sockopt<T: Copy>(&self, level: i32, name: i32, value: &T) -> impl Future<Output = io::Result<()>>;
pub unsafe fn get_sockopt<T: Copy>(&self, level: i32, name: i32) -> impl Future<Output = io::Result<T>>;

// monoio::net::TcpListener  (net/tcp/listener.rs)
pub async fn accept_batch_direct(&self, max: usize, flags: AcceptFlags) -> io::Result<Vec<TcpStream>>;
pub async fn accept_batch_direct_into(&self, dst: &mut Vec<TcpStream>, max: usize, flags: AcceptFlags)
    -> io::Result<usize>;
```

Rules that hold for all the stream calls:

- **The work is done by the call, the future only carries the result.**
  - With an fd: one `shutdown(2)` / `setsockopt(2)` / `getsockopt(2)` inside the call. The future is ready at
    its first poll.
  - Direct-only: the SQE is pushed inside the call; the future resolves with its CQE.
  - Dropping the future does not undo the request (`SKIP_CANCEL` on both ops; tested).
- **The futures borrow nothing** (the crate is edition 2021: `impl Future` does not capture `&self`). A
  supervisor can call `stream.shutdown_both()` on an `Rc<TcpStream>` whose task has a receive in flight.
- They are async blocks, so not `Unpin` and not nameable. A `Vec` of the futures of one call site works
  (`reqresp --direct` collects the `set_nodelay_async` results of a batch that way).
- `shutdown_with`: in-flight receives complete with `Ok(0)`, sends with `EPIPE`; the peer reads what was sent
  before, then EOF.
- `set_sockopt`: `value` is passed byte for byte (`size_of::<T>()`), so `T` is the option's C type
  (`libc::c_int`, `libc::linger`, ...).
  - Direct-only: the bytes are copied into a `Box<[MaybeUninit<u8>]>` in the op data and the SQE points there
    (plan finding 8: the future moves after the push, and the kernel reads the value at `io_uring_enter`).
    One small allocation per call; no syscall of its own.
  - `EOPNOTSUPP` from the kernel (no `SOCKET_URING_OP_SETSOCKOPT`: before 6.7) becomes `ErrorKind::Unsupported`
    with a message. Every other error is the option's own (`ENOPROTOOPT`, `EINVAL` for a wrong size, ...).
  - `-EINVAL` is **not** mapped to `Unsupported`: it cannot mean "no such opcode" here, because
    `IORING_OP_URING_CMD` (5.19) is as old as the sparse file table a direct-only socket needs, and it is what
    `setsockopt` returns for a bad value.
- `get_sockopt` is `unsafe` because the kernel's bytes become a `T` (contract: any bit pattern and all-zero
  must be valid; bytes the kernel does not write are zero).
  - Direct-only: **the kernel reads only `SOL_SOCKET` options through the ring** (`io_uring_cmd_getsockopt`
    returns `-EOPNOTSUPP` for any other level; still so on Linux 7.0.0, probed). So `TCP_NODELAY`, `TCP_INFO`
    etc. cannot be read on a direct-only stream: `Unsupported`.
  - The `io_uring` crate 0.7.15 has no typed `GETSOCKOPT`. See "Tried and rejected" for how the SQE is built.
- `set_tcp_keepalive_async` requests `SO_KEEPALIVE` and the given `TCP_KEEPIDLE` / `TCP_KEEPINTVL` /
  `TCP_KEEPCNT` at once (up to four SQEs on a direct-only stream), awaits all, returns the first error.

`accept_batch_direct_into`:

- Same contract as `accept_batch_into`: a multishot accept armed by the first call and kept in the listener,
  ≥ 1 connection per call, everything queued in one poll, `ResourceBusy` for a second concurrent caller,
  `InvalidInput` for other `flags` while armed (`CLOEXEC` is ignored, as in `accept_multi_direct`), re-created
  by the next call after an error.
- It has its own `BatchAcceptor` (`batch_direct`), so `accept_batch` and `accept_batch_direct` can both be
  armed on one listener; they then compete for connections.
- Without `with_fixed_files_kernel_alloc`: the `Unsupported` error of `accept_multi_direct`, same text (tested).
- The streams are direct-only and have no peer address (`peer_addr()` is `Unsupported`): the kernel reports
  none with a multishot accept.
- **A full kernel range loses a connection.** The accept ends with `ENFILE`, and the kernel has already
  accepted that connection and closes it (it has no slot for it). Tested: 6 clients, 4 slots → 4 streams, then
  `ENFILE`; after the slots were freed and one more client connected, the next call returned 2 connections
  (the 6th and the new one), so the 5th was lost. Size the range for the peak, and back off after `ENFILE`.
- It does not use `DirectAcceptStream` (`net/tcp/multishot.rs`, r3-bufring's file): `BatchAcceptor` drives
  `AcceptMultiOp::new_direct` itself. So no change is requested there.

**Unix:** `UnixListener` has no direct accept at all (`accept_direct`, `accept_multi_direct` do not exist
there), so, as instructed, no `accept_batch_direct_into` was built for it. A `UnixStream` therefore always has
an fd and its four new calls are plain syscalls; the shared code would take the ring path for a direct-only
one. `BatchAcceptor::accept_direct` works on any listening fd, so the Unix twin is about 20 lines once someone
wants it.

### Why the sync setters are not routed through the ring

`set_nodelay`, `set_tcp_keepalive` and `nodelay` are synchronous functions. A ring op has its result only
after an `io_uring_enter` and after the driver reaped the CQE. Inside a sync function that leaves three
choices, all bad:

1. Enter and reap inside the call. That is a driver tick in the middle of a task poll: it wakes other tasks'
   ops out of order with the round, and on a `DEFER_TASKRUN` ring it runs everyone's pending task work. It
   also adds a syscall per option, which is what direct-only is meant to save.
2. Push the SQE and return `Ok(())` before the result is known. A failure (kernel before 6.7) would be
   silent.
3. Block the thread until the CQE arrives.

So they keep returning `Unsupported` on a direct-only stream. The error text now names `set_sockopt`,
`set_nodelay_async` and `set_tcp_keepalive_async` (tested).

## Behaviour changes

- `IORING_OP_SHUTDOWN` now has `SKIP_CANCEL`. Before, dropping the future of `AsyncWriteRent::shutdown` on a
  **direct-only** stream before it completed pushed an `AsyncCancel` (with `async-cancel`), and that cancel
  undid the shutdown: the kernel queues every ring shutdown to io-wq (see below), so it is still pending when
  the cancel is issued. Checked: with `SKIP_CANCEL = false` the test `dropped_shutdown_future_still_shuts_down`
  hangs on the direct-only stream (the peer never reads EOF); with `true` it passes. Now the shutdown happens
  once requested, as it always did on a stream with an fd (there the syscall runs inside the call). Streams
  with an fd are not affected.
- Nothing else: `AsyncWriteRent::shutdown` / `cancelable_shutdown` of `TcpStream` go through the same code as
  before, moved to `driver/op/shutdown.rs`; `UnixStream`'s are untouched.

## What I checked in the kernel (Linux 7.0.0-42, probes run once, not committed)

| statement | result |
|---|---|
| `IORING_OP_SHUTDOWN` runs in an io-wq worker (`REQ_F_FORCE_ASYNC` in `io_shutdown_prep`, from memory of the source) | observed: an `iou-wrk-*` thread exists after the first ring shutdown, none before; and an `AsyncCancel` pushed right after it cancels it |
| `SOCKET_URING_OP_SETSOCKOPT` / `GETSOCKOPT` run inline | yes: no `iou-wrk-*` thread after them |
| ring `GETSOCKOPT` with `IPPROTO_TCP` (`TCP_NODELAY`) | `-EOPNOTSUPP`; `SOL_SOCKET` options work |
| ring `SETSOCKOPT` on a Unix stream socket (by fd, raw SQE) | works (`res = 0`) |
| `IORING_OP_FIXED_FD_INSTALL` on a direct socket | works; the tests use it to read `TCP_*` options of a direct-only stream with `getsockopt(2)` |

## Cost table

### Per connection, measured

A test binary (not committed) accepted 200 queued connections in each mode, set `TCP_NODELAY`, shut every
connection down from outside, and dropped them. Syscalls from `strace -f` between marker syscalls; SQEs from
`DriverStats::ops_by_opcode` (`metrics`). Numbers are per connection.

| step | fd stream (`accept_batch_into`) | registered (`+ register_fixed`) | direct-only (`accept_batch_direct_into`) |
|---|---|---|---|
| accept | 0 SQE, **1 syscall** (`getpeername`, for the peer address) | 0 SQE, **2 syscalls** (`getpeername`, `io_uring_register`) | 0 SQE, **0 syscalls** |
| `TCP_NODELAY` | **1 syscall** (`setsockopt`) | **1 syscall** | 1 SQE (`URING_CMD`), 0 syscalls |
| `shutdown_both` | **1 syscall** (`shutdown`) | **1 syscall** | 1 SQE (`SHUTDOWN`), 0 syscalls, **1 io-wq round trip** |
| close (drop) | 1 SQE (`CLOSE`) | 2 SQEs (`CLOSE` of the slot, `CLOSE` of the fd) | 1 SQE (`CLOSE` of the slot) |
| **syscalls, accept to close without shutdown** | **2** | **3** | **0** |

(The one multishot accept SQE per listener is not counted. All SQEs of a batch go out with the loop's next
`io_uring_enter`.)

The same on the reproducer, whole process (server and clients), `strace -c -f -e
trace=io_uring_register,accept4,close,shutdown,setsockopt,getpeername,socket`, `reqresp --conns 200 --sizes 4k
--path pipe --ring defer,skipenter,ringfd` (2 s window):

| syscall | plain | `--ring ..,fixedfd` | `--direct` | note |
|---|---|---|---|---|
| `getpeername` | 200 | 200 | 0 | server, `accept_batch_into` |
| `setsockopt` | 404 | 404 | 204 | 200 are the clients' `set_nodelay`, 4 the listeners |
| `io_uring_register` | 1 | 203 | 3 | 1 = ring fd; 2 = table + alloc range; 200 = `register_fixed` |
| `accept4`, `shutdown` | 0 | 0 | 0 | |
| `close` | 4 | 4 | 4 | the process exits without closing connections |
| `socket` | 202 | 202 | 202 | clients and listeners |

So the server makes 2 / 3 / 0 syscalls per accepted connection. ROUND3.md's acceptance asks for "one syscall
less per connection" against `fixedfd`: it is three less.

### Per op

A direct-only (or registered) stream names its table slot in every SQE (`IOSQE_FIXED_FILE`), so the kernel
skips the fd lookup and the reference pair (`io_file_get_normal` / `fput`; 2.66% of the core at 2000
connections in ROUND3.md). User space does the same work in all three modes: **2,342 user instructions per
frame in every run of every mode** below.

### What does not work without an fd

- `as_raw_fd()` is `-1`, `into_raw_fd()` panics; nothing that takes an fd works (`try_send` of r3-eager,
  `poll`/`epoll`, passing the socket to another process or runtime: the slot belongs to its ring).
- `local_addr()`; `peer_addr()` after a multishot direct accept (`accept_direct` caches the address).
- The sync option calls (`set_nodelay`, `nodelay`, `set_tcp_keepalive`): use the `_async` ones.
- Reading options of other levels than `SOL_SOCKET` (kernel limit).
- Socket options need Linux 6.7; the accept itself 5.19 (`accept_direct`) / 6.0 (kernel-allocated slots).
- `shutdown` costs an io-wq round trip instead of an inline syscall.
- A full table refuses (and, with the multishot accept, drops) connections; with fds the limit is
  `RLIMIT_NOFILE`, and the connection stays in the backlog.

### Proposed AGENTS.md text

In the "Fixed files" bullet, replace "socket options and `local_addr` return `Unsupported` (set options on the
listener)" by "`local_addr` and the sync option calls return `Unsupported`", and append:

> **Direct-only sockets in a server.** `listener.accept_batch_direct_into(&mut vec, max, flags)` is
> `accept_batch_into` on a multishot direct accept (needs `with_fixed_files_kernel_alloc(n)`; streams without
> peer address). When all `n` slots are taken the accept ends with `ENFILE` and the kernel drops the
> connection it had just accepted, so size `n` for the peak. What needs an fd elsewhere goes through the ring
> or a syscall, whichever the stream has: `stream.shutdown_with(Shutdown::Both)` / `shutdown_both()` (`&self`,
> the future borrows nothing: call it from a supervisor task while the connection's task has ops in flight;
> they end with `Ok(0)` / `EPIPE`), `set_sockopt(level, name, &value)`, `unsafe get_sockopt::<T>(level, name)`,
> `set_nodelay_async(bool)`, `set_tcp_keepalive_async(..)` (also on `UnixStream`, without the two TCP
> helpers). The request is made by the call; dropping the future does not cancel it. On a direct-only stream
> socket options need Linux 6.7 (`Unsupported` before) and `get_sockopt` only reads `SOL_SOCKET` options.
>
> | per connection | fd (`accept_batch_into`) | `+ register_fixed` | direct-only |
> |---|---|---|---|
> | accept | 1 syscall (`getpeername`) | 2 syscalls (+ `io_uring_register`) | none |
> | `TCP_NODELAY` | 1 syscall | 1 syscall | 1 SQE |
> | full shutdown | 1 syscall | 1 syscall | 1 SQE, runs in io-wq |
> | close | 1 SQE | 2 SQEs | 1 SQE |
> | each op | fd lookup + refcount in the kernel | none | none |
>
> Without an fd there is no `as_raw_fd` (`-1`), `local_addr`, `peer_addr` (multishot accept), `try_send`, and
> no sync option call. Throughput at 2000 connections is the same as with `register_fixed` (screening).

## Screening numbers

`reqresp --conns 2000 --sizes 4k --path pipe --ring defer,skipenter,ringfd[,fixedfd] [--direct] --cpu 2
--client-cpus 3,4 --warmup 1 --duration 4`, default features, one binary, the three modes alternated
(plain, fixedfd, direct) per round. **Screening, machine not quiet.**

Per cell: frames/s · frames per CPU-second · user cycles per frame · all cycles per frame. User instructions
per frame are **2,342 in all 18 runs**, so they are left out of the cells.

| round | plain | `fixedfd` | `--direct` |
|---|---|---|---|
| 1 | 112,824 · 112,868 · 2,215 · 37,974 | 114,540 · 114,557 · 2,186 · 37,422 | 113,539 · 113,554 · 2,227 · 37,747 |
| 2 | 112,859 · 112,883 · 2,228 · 37,975 | 113,630 · 113,638 · 2,201 · 37,713 | 114,013 · 114,033 · 2,220 · 37,591 |
| 3 | 103,255 · 103,273 · 2,795 · 41,470 | 94,816 · 94,828 · 3,521 · 45,131 | 98,299 · 98,329 · 3,247 · 43,573 |
| 4 | 96,944 · 96,977 · 3,249 · 44,123 | 92,405 · 92,419 · 3,693 · 46,095 | 99,288 · 99,304 · 3,192 · 43,008 |
| 5 | 91,026 · 91,039 · 3,730 · 46,734 | 101,338 · 101,328 · 2,849 · 42,172 | 113,900 · 113,911 · 2,191 · 37,635 |
| 6 | 106,019 · 106,030 · 2,506 · 40,370 | 94,316 · 94,333 · 3,590 · 45,253 | 108,780 · 108,805 · 2,528 · 39,321 |

- `server_errors=0` and `client_failed=0` in every run; ramp 0.10–0.17 s in all modes.
- **The machine was disturbed in most runs.** The same 2,342 user instructions took between 2,186 and 3,730
  user cycles, depending on the run and not on the mode (round 4 ran while my own test suite was running on
  the build cores; the rest is other agents' load). Medians over all six rounds would compare the noise.
- The runs that were not disturbed (user cycles per frame ≤ 2,230: rounds 1 and 2 for all modes, round 5 for
  `--direct`):

  | | plain (2 runs) | `fixedfd` (2 runs) | `--direct` (3 runs) |
  |---|---|---|---|
  | frames per CPU-second, mean | 112,876 | 114,098 (+1.1%) | 113,833 (+0.8%) |
  | all cycles per frame, mean | 37,975 | 37,568 (−1.1%) | 37,658 (−0.8%) |

  `--direct` against `fixedfd`: −0.2% frames per CPU-second, +0.2% cycles per frame, with a spread between
  equal runs of up to 0.8%. So: **both save about 1% of the cycles per frame against plain fds, and I cannot
  tell them apart here.** That fits ROUND3.md (`fixedfd` +0.6%, lookup 2.66% of the core) and its acceptance
  ("within 1% of `fixedfd`"), but the verdict needs the gate's quiet runs.
- Per-connection syscalls: 2 (plain), 3 (`fixedfd`), 0 (`--direct`), see the cost table. With 2000
  connections that is the ramp only; it does not show in a steady-state window.
- Not measured: a connection-churn load (accept and close per request), where the per-connection savings
  would show; `reqresp` keeps its connections.

## Tests

`monoio/tests/direct_socket.rs`, 13 tests, all pass with and without `metrics`:

| test | what it shows |
|---|---|
| `shutdown_with_fd_stream`, `shutdown_with_direct_stream` | `Write`: data sent before is delivered, then EOF, the other direction stays open, our next send is `EPIPE`. `Read`: our receive sees EOF, our sends arrive. `Both` |
| `shutdown_both_ends_a_pending_receive_fd`, `.._direct` | a receive in flight in another task completes with 0 after `shutdown_both()` from outside; the task's next send is `EPIPE`; the slot is released. Also: the futures of all six calls are `'static` (compile-time) |
| `dropped_shutdown_future_still_shuts_down` | fd and direct-only |
| `unix_shutdown_and_sockopt` | the `UnixStream` calls, including shutdown from another task |
| `sockopt_fd_stream` | `set_nodelay_async`, `set_sockopt` (int and `linger`), `get_sockopt` (any level), `set_tcp_keepalive_async`, `ENOPROTOOPT`; the call acts without a poll |
| `sockopt_direct_stream` | error text of the sync calls; `SO_KEEPALIVE`, `SO_RCVBUF`, `SO_LINGER` set and read back through the ring; short option zero-filled; `TCP_NODELAY` and the keepalive options verified with `getsockopt(2)` on an fd made by `FIXED_FD_INSTALL`; a dropped `set_nodelay_async` future still sets the option; `ENOPROTOOPT`, `EINVAL` (1-byte value), `Unsupported` for a `TCP` level read |
| `accept_batch_direct_takes_the_queue` | 300 queued connections in 1 call (asserted ≤ 3), all direct-only in kernel slots, echo on every one, `max` respected, `InvalidInput` for other flags, `kernel_in_use` back to 0 after the drop |
| `accept_batch_direct_one_task_at_a_time` | `ResourceBusy`; a dropped call keeps the accept armed |
| `accept_batch_direct_needs_kernel_slots` | the error of `accept_multi_direct`; `accept_batch` unaffected |
| `accept_batch_direct_full_table` | `ENFILE` after 4 of 4 slots, works again after slots are freed |
| `direct_server_with_supervisor` | batch accept, `set_nodelay_async` for the batch, echo tasks, `shutdown_both` from outside ends each task |

Skips: no fixed file table (`EINVAL`/`ENOSYS` at build), no multishot accept (`EINVAL`), no ring socket
options (`Unsupported`), no `FIXED_FD_INSTALL` (that check only). None of them triggered on this kernel.

## Checks

- `cargo test -p adaptif-gatotkaca-monoio`: every test binary and the doc tests pass (76 `test result: ok`
  lines, no failure).
- `cargo test -p adaptif-gatotkaca-monoio --features metrics`: the same.
- Both full runs were made on the final library source. After them only `tests/direct_socket.rs` changed
  (the `'static` checks); `direct_socket`, `fixed_files` and `accept_batch` were then run again with both
  feature sets (and with `--test-threads 1`), and the `direct_socket` binary 30 times in a row: all pass.
- One failure on the way, in my own test and fixed there: I had dropped control futures on a direct-only
  stream right before asserting that its slot was free. An op in flight keeps the socket open until its CQE,
  and a ring shutdown takes an io-wq round trip, so the slot was sometimes still in use after four loop
  rounds (1 failure in 2 runs). The test now awaits them. Worth knowing for users: a dropped ring shutdown
  still delays the close until it completed.
- `cargo clippy --workspace` and `cargo clippy --workspace --features adaptif-gatotkaca-monoio/metrics`: the
  3 old warnings in the library (`net/udp.rs` twice, `utils/slab.rs`), nothing new.
  `cargo clippy -p adaptif-gatotkaca-monoio --tests --examples`: nothing in the files of this package.
- `cargo build --workspace --examples`, and the crate's examples with `--features metrics`: build.
- `cargo doc -p adaptif-gatotkaca-monoio --no-deps`: no warning in the changed files (18 older ones elsewhere).
- `cargo fmt` applied.
- `reqresp --direct` ran with 200 and 2000 connections (above).

## Tried and rejected

- **`UringCmd16` for `GETSOCKOPT`** (the brief's suggestion): it cannot set `optlen` (SQE offset 44) nor
  `level`/`optname` as fields, only `cmd_op`, `addr` and the 16 command bytes. Instead the op builds the
  `opcode::SetSockOpt` entry and overwrites its `cmd_op` word (offset 8) with `SOCKET_URING_OP_GETSOCKOPT`.
  Before each get, a probe entry is built and checked to carry `SOCKET_URING_OP_SETSOCKOPT` at that offset;
  if a later `io-uring` version changed the layout, the call returns `Unsupported` instead of turning a "get"
  into a "set". A `const` assertion pins `size_of::<Entry>() == 64`.
- **A batch poll on `DirectAcceptStream`**: not needed, see above.
- **Option value inline in the op data**: unsound (finding 8). The heap copy costs one small allocation per
  ring `setsockopt`; that is once per connection, not per frame.
- **A named, `Unpin` future type for the control calls**: async blocks are enough for the uses I found
  (`await`, a `Vec` per call site). If a struct must hold one, it needs a `Box::pin` today.
- **Getting an fd for sync setters with `FIXED_FD_INSTALL`**: that is a ring op too, plus a `close`.

## Open questions

1. **`shutdown` of a direct-only socket always costs an io-wq round trip** (kernel: `REQ_F_FORCE_ASYNC`). A
   sweeper that ends many connections in one tick wakes io-wq workers (bounded by `iowq_max_workers`). For
   "end this connection now", `cancel_all()` plus dropping the stream is all inline (one `ASYNC_CANCEL`, one
   `CLOSE`), but the peer then sees the close only when the connection's task has dropped its ops. Not
   measured.
2. **`reqresp` has no sweeper.** The plan line says "`shutdown_with` from a sweeper task"; my brief asked for
   the accept only, and the deadline code is `deadline.rs` (r3-timer). The tests cover the supervisor case.
3. **`ENFILE` drops a connection** with the multishot direct accept (above). The single-shot `accept_direct`
   does not (the runtime checks for a free slot before it submits). A batch flavour without that loss would
   have to submit single direct accepts into runtime slots: one SQE per connection.
4. **Owned halves** (`TcpOwnedReadHalf` / `WriteHalf`, `net/tcp/split.rs`, r3-eager's file) do not have the
   new calls. A supervisor needs the `TcpStream` (e.g. in an `Rc`) today.
5. **`get_sockopt` is `unsafe`.** A safe `get_sockopt_int(level, name) -> i32` would cover most uses; not
   added, to keep the surface small.
6. `set_tcp_keepalive_async` rounds to whole seconds like `socket2`.

## Requested changes

None in `monoio/src/`. Two in `monoio/examples/reqresp/main.rs` (r3-repro's file). **Neither is applied
locally**; `--direct` works without them here (the soft `RLIMIT_NOFILE` is 1,048,576, and the label is
cosmetic).

1. The fixed file table of `--direct` also counts against `RLIMIT_NOFILE`:

   ```diff
    fn main() {
        let a = args();
        let fixedfd = a.ring.iter().any(|r| r == "fixedfd");
   +    // `--direct` registers a table of the same size (kernel-allocated slots); both may be set.
   +    let tables = usize::from(fixedfd) + usize::from(a.direct);
        // Both ends of every connection are in this process; a fixed file table may not be
        // larger than the limit either.
   -    let need = (2 * (a.conns + a.bulk) + 64).max(if fixedfd {
   -        accept::FIXED_FILES as usize + 64
   -    } else {
   -        0
   -    });
   +    let need = (2 * (a.conns + a.bulk) + 64).max(tables * accept::FIXED_FILES as usize + 64);
   ```

2. The `RESULT` line does not say that `--direct` was set (a run with it prints the same labels as one
   without). Add a label next to `deadline=`, e.g. `accept=direct|fd`, where the line is built.
