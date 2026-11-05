# AGENTS.md

Monoio (published as `adaptif-gatotkaca-monoio`, library name `monoio`): a stripped-down, thread-per-core async runtime built only on io_uring. Linux only, kernel 5.6+. There is no epoll/legacy/IOCP driver. A fork of bytedance/monoio; keep `LICENSE-*` and `NOTICE` when changing licensing or packaging.

## Layout
- `monoio/`: the runtime crate (package `adaptif-gatotkaca-monoio`; refer to it as `-p adaptif-gatotkaca-monoio`).
  - `src/`: `builder`, `runtime`, `scheduler`, `task`, `driver/uring` (+ `op`), `io` (rent traits), `buf`, `net/{tcp,udp,unix}`, `fs`, `time`, `blocking`, `macros` (`join!`, `select!`, `try_join!`), `utils`, `tls/{rustls,native}` (feature-gated; `tls/io_wrapper.rs` bridges the sync TLS engines to rent IO).
  - `tests/`: integration tests; most need a working io_uring.
- `monoio-macros/`: separate proc-macro crate (package `adaptif-gatotkaca-monoio-macros`; proc macros cannot live in the runtime crate, so it is published first): proc macros `#[monoio::main]`, `#[monoio::test]`, `#[monoio::test_all]`, and `select!` internals.
- `examples/`: unpublished crate, built with default features off. Run one with `cargo run -p monoio-examples --example echo`. Examples: accept, builder, channel, echo, echo-tfo, join, macro, proxy, tcp_uring, timer, timer-select, uds, tls-{rustls,native}-{server,client} (certs in `monoio/tests/certs`).

## Commands
- Check: `cargo check --workspace`
- Test: `cargo test -p adaptif-gatotkaca-monoio` (add `--features splice,mkdirat,...` for the optional ops; they need a new enough kernel)
- Format and lint: `cargo fmt`, `cargo clippy --workspace` (style is in `rustfmt.toml`; some options are nightly-only)
- Stable Rust 1.75+ works. Only the `unstable` feature needs nightly.

## Core concepts
- **Thread-per-core.** Each thread owns one `Runtime`. Tasks are `!Send`; thread-local state is fine. `monoio::spawn` runs on the current thread only. Use it for IO-bound servers, not for unbalanced CPU work.
- **Ownership-based IO ("async rent").** Calls take the buffer by value and return it: `let (res, buf) = stream.read(buf).await;`. The kernel may touch the buffer after the future is dropped, so borrowed buffers are unsound. Buffers implement `IoBuf`/`IoBufMut` (`IoVecBuf`/`IoVecBufMut` for vectored IO). Do not expect Tokio's `AsyncRead`/`AsyncWrite`; there is no compat layer.
- **Traits.** `AsyncReadRent`, `AsyncWriteRent` and their `*Ext` traits (`read_exact`, `write_all`, ...). They use async-fn-in-trait / `impl Future` returns, not GAT or `poll_*`.
- **Cancellation.** Dropping a future cannot reliably stop an in-flight op (and with `async-cancel` only tries to). For timeouts use the `Cancelable*` traits:
  ```rust
  let canceler = monoio::io::Canceller::new();
  let fut = conn.cancelable_read(buf, canceler.handle());
  // on timeout: canceler.cancel(); then keep awaiting `fut`
  ```
  Always await the future after `cancel()`: you get the buffer back, and the result is either `Err(os error 125)` (ECANCELED) or a completed result you must handle. Wrappers just pass the `CancelHandle` through.
- **Communication.** Same thread: `local-sync` (mpsc, oneshot, semaphore, no atomics). Between runtime threads, no `sync` needed: `uring::msg_ring` (see below). From any thread (runtime or not) into a runtime thread: `uring::event_channel()` gives an `EventSender` (`Send + Clone`; `send(v) -> Result<(), T>`) and an `EventReceiver` (`recv().await -> Option<T>`, `None` once all senders dropped; cancel safe): a mutex queue plus an eventfd read through the ring, one `write(2)` per wait, not per message. With `sync`, any channel (`flume`, `async-channel`, Tokio's `sync`) or atomics work too. Prefer per-thread aggregation on hot paths.
- **Raw io_uring access.** The `io_uring` crate is re-exported as `monoio::io_uring` (and `opcode`, `squeue`, `cqueue`, `types`, `IoUring`, `Probe` as `monoio::uring::*`): use these instead of depending on `io_uring` yourself, so versions always match. `monoio::uring` is the escape hatch for ops the crate doesn't wrap. `unsafe uring::submit(sqe, data)` queues any `squeue::Entry` and resolves to `(Cqe, data)`; `data` must own the heap memory the SQE points to and is kept alive until the kernel finishes. `unsafe uring::with_ring(|ring| ..)` exposes the `IoUring` for registration, probing and `params()`. Both also exist on `IoUringDriver` (`submit_raw`, `with_ring`; reach it with `Runtime::driver()`). `submit` is for single-shot SQEs; an fd it returns is yours to close. For multishot SQEs use `unsafe uring::submit_multishot(sqe, data)` (or `IoUringDriver::submit_multishot_raw`), see below.
  - **Your own SQEs.** For ops `submit_multishot` can't express (or to route many ops through one table), push them yourself inside `with_ring` with `user_data(uring::user_data(id))` (`id <= uring::MAX_ID`) and receive every completion in a hook: `uring::set_cqe_hook(|id, cqe| ..)` (`Cqe::more()` says a multishot is still armed, `buffer_id()` gives the provided buffer). The driver delivers the hook's batch after it finished its own tick, so the hook may call `with_ring`/`submit`, wake or spawn. Completions are dropped while no hook is installed, so set it first. You own everything those SQEs point to until their last CQE. Never reap the CQ, and never use a `user_data` below 2^62 (it collides with the driver's ops). With the timer enabled, reach the driver with `runtime.driver().inner()`.
- **Registered (fixed) buffers (5.13+).** `uring::FixedBufPool::new(count, size)` (or `new_in(&driver, ..)`) registers `count` buffers of `size` bytes; `pool.get()` returns an owned `FixedBuf` (`IoBuf + IoBufMut`, `Deref<[u8]>`, `buf_index()`), or `None` when empty; it returns to the pool on drop. Use it with `File::read_fixed_at` / `write_fixed_at` and `TcpStream::recv_fixed` / `send_fixed` (each with an `_op` named-future variant), or with any ordinary op. `send_fixed` is `WRITE_FIXED` (the kernel rejects fixed buffers on plain `SEND`), so it has no `MSG_NOSIGNAL`. `buf.freeze()` gives a refcounted, cloneable `FrozenFixedBuf` (`slice(range)` copies nothing); the buffer returns when the last clone drops. `pool.grow(n)` adds buffers without touching those in use; `pool.stats()` gives `in_use`, `high_water`, `capacity`, `exhausted`. All pools on a ring share one sparse table (`DEFAULT_TABLE_SLOTS` = 1024, larger via `with_table_size`, max 16384) with disjoint index ranges; it is unregistered once the last pool and buffer are gone. While a pool exists, don't call `register_buffers`/`unregister_buffers` raw (a pool after raw registration fails with `EBUSY`). Registration pins the pages, which counts against `ulimit -l` (`ENOMEM`). Registered files (`register_files`) are still raw-only.
- **Provided buffers (`IOSQE_BUFFER_SELECT`, Linux 5.19+).** `uring::BufRing::new(bgid, entries, buf_size)` (or `BufRing::new_in(&driver, ..)`; `entries` a power of two) registers a pool the kernel picks from. Set `squeue::Flags::BUFFER_SELECT` and `.buf_group(ring.group())` on a `Recv` (null ptr, len 0) or use `RecvMulti`, submit via `uring::submit` or the hook, then `ring.take(cqe)` gives a `SelectedBuf`. It is `Deref<[u8]>` and `IoBuf` (write or send it, or a `.slice(..)`, with no copy) and returns to the pool on drop; `detach()`/`into_bytes()` copy it out and return it at once. Call `take` for every completion of the group or the pool runs dry (`ENOBUFS`, counted in `ring.stats()` with `in_use`/`high_water`/`taken`/`recycled`). The pool is lazily faulted, page-aligned `mmap` memory. Options via `BufRing::with_options(.., uring::BufRingOptions::new()...)`:
  - `batch_publish`: recycled buffers are published once per driver tick (before each `io_uring_enter`), or by `ring.publish()`.
  - `kernel_ring`: the kernel allocates the ring (`IOU_PBUF_RING_MMAP`, 6.4+).
  - `incremental`: `IOU_PBUF_RING_INC` (6.12+). Several receives share one buffer; each `SelectedBuf` is a view at `offset()`. The buffer is recycled when the kernel clears `BUF_MORE` and all its views are dropped. `take` must see every completion once, in order.
  - `hugetlb` (needs reserved huge pages; errors instead of falling back), `populate`.

  Finish or cancel ops using the group before dropping the last `BufRing` and `SelectedBuf`.
- **Socket flags and vectored sends.** `TcpListener`/`UnixListener::accept_with(AcceptFlags::NONBLOCK | AcceptFlags::CLOEXEC)` (and `cancelable_accept_with`) set `accept4` flags; plain `accept()` sets none (monoio never needs `O_NONBLOCK`). `TcpStream`/`UnixStream` (and their owned halves) have `send_with_flags(buf, MsgFlags)`, `recv_with_flags(buf, MsgFlags)` (`monoio::net::MsgFlags::{MORE, WAITALL, DONTWAIT, PEEK, ..}`; sends always add `MSG_NOSIGNAL`), `recv_poll_first(buf, flags)` (`IORING_RECVSEND_POLL_FIRST`, 5.19+, for probably idle connections), and `send_vectored(bufs)` / `send_vectored_with_flags` (`sendmsg` with `MSG_NOSIGNAL`: a reset peer gives EPIPE/ECONNRESET, never SIGPIPE; `writev` still uses `IORING_OP_WRITEV`). `monoio::buf::IoVecArray<B, N>` (write) / `IoVecArrayMut<B, N>` (read) hold up to N buffers and one reused boxed iovec array: keep one per connection, `push`, send, then `drain()`/`clear()`; after a short write call `advance(n)` and send again.
- **Multishot ops.** `unsafe uring::submit_multishot(sqe, data) -> MultiOp<T>`: the driver queues each CQE of the op (unbounded; see `DriverStats::multishot_queue_high_water`). `op.next().await` returns them in order; the CQE without `F_MORE` is the last, then `None`. `op.cancel()` pushes AsyncCancel; keep polling to drain (the end is usually `-ECANCELED`). `into_data()` works once the kernel posted the final CQE. Dropping a `MultiOp` cancels the op (even without `async-cancel`) and keeps `data` alive until the final CQE; dropped raw CQEs are not inspected (fds and provided buffers in them leak), so cancel and drain if that matters. Typed:
  - `TcpListener::accept_multi(AcceptFlags)` / `UnixListener::accept_multi` (5.19+) give an `AcceptStream`: `next()` yields `(stream, addr)` with one `getpeername` syscall each, `next_stream()` skips it; dropping it closes queued connections.
  - `TcpStream::recv_multi(&ring)` / `UnixStream::recv_multi` (6.0+) give a `RecvStream` of `SelectedBuf`s: EOF gives `None`; `Err(ENOBUFS)` means the kernel ended the recv, so free buffers and call `recv_multi` again; dropping it returns queued buffers to the ring. Don't mix `recv_multi` with other reads on the same socket.
  - Recv bundles (`IORING_RECVSEND_BUNDLE`, 6.10+): `TcpStream`/`UnixStream::recv_multi_bundle(&ring)` yields `Bundle`s, `recv_bundle(&ring)` receives once. A `Bundle` is an allocation-free iterator of `SelectedBuf`s in data order (`len()`, `bytes()`, `detach()`). With raw SQEs use `ring.take_bundle(cqe)` for bundle completions and `take` for the others; both mix on one ring in any order. Check `ring.supports_bundles()`; the wrappers return `Unsupported` without it. The kernel ends a multishot recv when the ring runs empty; `RecvStream`/`RecvBundleStream` re-arm themselves then (not after `ENOBUFS`, `cancel()` or EOF).
  - Size the CQ (`cq_size`) for multishot bursts.
- **Linked chains.** `unsafe uring::submit_chain([sqe; N], data)` queues N SQEs as one unit: room for all is reserved first and nothing is submitted in between, so a link is never split. The caller sets the link flags (`IO_LINK` on all but the last, or `uring::link([..])`); a linking last SQE and `SKIP_SUCCESS` are rejected. It resolves to `([Cqe; N], data)` once all completed; links cancelled after a failure (or a short read, which breaks an `IO_LINK`) complete with `-ECANCELED`. Dropping it keeps `data` alive until the last CQE. Don't put `IO_LINK` on SQEs passed to plain `uring::submit`. Typed: `File::read_at_then_send(buf, pos, &tcp)` (a short read returns `UnexpectedEof` and nothing is sent).
- **Deadlines without the timer.** `TcpStream::read_with_deadline(buf, dur)` / `write_with_deadline` attach a kernel link timeout (`IORING_OP_LINK_TIMEOUT`): one extra SQE, no timer-wheel entry, works without `enable_timer`. On expiry they return `ErrorKind::TimedOut` with the buffer, and the stream stays usable. Raw: `unsafe uring::submit_with_timeout(sqe, data, dur)` (CQE `-ECANCELED` if it fired). `DriverStats::{chains_submitted, link_timeouts}` count them.
- **Cross-ring messages (`IORING_OP_MSG_RING`, 5.18+).** Hand out `uring::ring_handle()` (or `RingHandle::new(rt.driver())`; `Send + Sync + Clone`). It holds a dup of the ring fd, so it stays valid after the target runtime exits (messages then go nowhere or fail with `EOWNERDEAD`; drop handles of dead runtimes, their messages pile up in kernel memory). `uring::msg_ring(&h, id, res).await` posts a CQE `(id, res)` to the target's hook; it works with `single_issuer`/`defer_taskrun` targets and keeps one sender's messages in order. `msg_ring_nowait` just queues the SQE (`CQE_SKIP_SUCCESS`): no source completion, failures silently dropped. On the target, `uring::msg_receiver()` installs a hook feeding a `Mailbox` (`recv().await -> (id, res)`); if you need the hook yourself, create `Mailbox::new(lo..=hi)` and call `mailbox.offer(id, cqe)` from it.
- **Named op futures / token API.** `monoio::io::futures` has `Unpin + 'static` `RecvFuture`, `SendFuture`, `ReadvFuture`, `WritevFuture`, `ReadAtFuture`, `WriteAtFuture`, made by the `_op` methods: `TcpStream`/`UnixStream` (and owned halves) `recv_op`, `recv_with_flags_op`, `recv_poll_first_op`, `send_op`, `send_with_flags_op`, `readv_op`, `writev_op`; `File::read_at_op`, `write_at_op`. They take `&self`, push the SQE immediately and don't borrow the stream, so a struct can hold an in-flight op without `Box::pin` (steady state allocates nothing). In a hand-written state machine drive one with `fut.poll_result(cx)` (no `Pin`); `is_terminated()` says it already returned. Dropping one cancels it (with `async-cancel`) and the runtime keeps the buffer alive, but you lose the buffer: to get it back call `fut.cancel()` and keep polling (`ECANCELED` or the completed result). The trait `read`/`write`/`readv`/`writev` on socket streams return these futures.
- **File ops.** `file.fadvise(offset, len, monoio::fs::Advice::Random).await?` (`IORING_OP_FADVISE`, 5.6+; `len` 0 = to EOF, at most `u32::MAX`; `fadvise_raw` takes `libc::POSIX_FADV_*`). File/pipe splice (feature `splice`, 5.7+): `let (rx, tx) = monoio::net::unix::new_pipe()?;` then `file.splice_to_pipe(&tx, offset, len)` (explicit offset, 0 at EOF) and `rx.splice_to(&mut tcp, n)`; `file.splice_from_pipe(&rx, offset, len)` and `tx.splice_from(&mut tcp, n)` go the other way. File splices run in io-wq workers. `OpenOptions::custom_flags(libc::O_NOATIME)` skips atime updates (own files only, `EPERM` otherwise); `openat` runs inline only on a cached path lookup without `O_CREAT`/`O_TRUNC`, otherwise in an io-wq worker.
- **Submit errors.** IO methods don't panic when the SQ can't be flushed (e.g. EBUSY on CQ overflow): a full SQ is flushed and retried up to 8 times, then the op fails with EBUSY. Rent-style calls return `(Err(e), buf)` with the buffer.
- **Sharing the io-wq (`IORING_SETUP_ATTACH_WQ`).** `RuntimeBuilder::attach_wq(fd)` with `fd = primary.driver().as_raw_fd()` (works with `enable_timer()` too; call it after `uring_builder`). The primary runtime must outlive the others.
- **Blocking work.** `monoio::spawn_blocking` needs `sync`. Attach a pool with `RuntimeBuilder::attach_thread_pool` or choose `with_blocking_strategy` (`Panic` or `ExecuteLocal`).

## Runtime configuration
Via builder or macro: `RuntimeBuilder::<IoUringDriver>::new().with_entries(n).enable_timer().build()`, or `#[monoio::main(entries = n, timer_enabled = true, worker_threads = n)]`.

- `entries`: io_uring ring size. Default 1024, minimum 256 (smaller values are raised to 256), 0 is a macro error. Larger rings mean fewer submits and more memory. The completion queue gets the kernel default of `2 * entries`; with multishot ops (many CQEs per SQE), raise it through `uring_builder(..)` with `setup_cqsize(n)` to avoid CQ overflow. The in-flight op table is not pre-sized: it grows in pages as ops are submitted.
- `enable_timer` / `timer_enabled`: off by default. `monoio::time::{sleep, interval, ...}` panics unless it is on. `std::time` is unaffected. `enable_all()` is also available.
- `worker_threads` (aliases `workers`, `threads`): default 1. With `n`, the macro starts `n-1` extra threads, each with its own runtime, plus the main thread. For CPU pinning or other per-thread setup, create threads and runtimes yourself (see `utils::bind_to_cpu_set`).
- `uring_builder(io_uring::Builder)`: tune the raw io_uring settings. Call it before the ring-flag methods below: it replaces the stored builder.
- Ring flags: `coop_taskrun()`, `taskrun_flag()` (needs `coop_taskrun` or `defer_taskrun`, EINVAL otherwise), `single_issuer()`, `defer_taskrun()` (also sets `single_issuer` and `taskrun_flag`, so completions are processed while tasks stay ready), `cq_size(n)`.
- `register_ring_fd()`: registers the ring fd (5.18+), saving an fd lookup per `io_uring_enter`. Falls back silently; check `driver().is_ring_fd_registered()`.
- `min_wait(wait, batch)`: a waiting enter waits for `batch` CQEs, or `wait` with at least one ready (6.12+, ignored otherwise). Adds up to `wait` of latency at low load.
- `skip_empty_enter()` (off by default): skip `io_uring_enter` between task rounds when nothing is queued. No effect on COOP/DEFER rings without `taskrun_flag`.
- `iowq_max_workers(bounded, unbounded)` (5.15+): caps `iou-wrk-*` threads (`0` keeps the current limit); `build()` fails if the kernel refuses. Rings sharing a wq via `attach_wq` share the cap, so set it on the primary.
- `task_budget(n)`: run `n` ready tasks per round before checking the driver (default about 2× the queue length).
- Stats: `IoUringDriver::stats()` / `reset_stats()` (with the timer: `rt.driver().inner().stats()`), or `monoio::driver_stats()` inside the runtime. A `DriverStats` copy: enter calls (submit/wait/skipped), SQEs submitted, CQEs reaped, CQEs-per-tick histogram, SQ-full flushes, EBUSY retries, cancels pushed/lost, the kernel CQ overflow counter, ops by opcode. io-wq punts are not visible; use the `io_uring:io_uring_queue_async_work` tracepoint.

## Feature flags (`monoio/Cargo.toml`)
Default: `async-cancel`, `bytes`, `macros`, `utils`.

| Flag | Meaning |
|---|---|
| `async-cancel` (default) | On future drop, push an AsyncCancel entry to the submission queue. Best effort only; recommended. |
| `bytes` (default) | `IoBuf`/`IoBufMut` impls for the `bytes` crate types. |
| `macros` (default) | `#[monoio::main]`, `#[monoio::test]`, `#[monoio::test_all]`. |
| `utils` (default) | `utils::bind_to_cpu_set` (thread CPU affinity; pulls in `nix`). |
| `sync` | Allow wakers to be used across threads (cross-thread `await`), and enable `spawn_blocking` and `RuntimeBuilder::attach_thread_pool`/`with_blocking_strategy`. Pulls in `flume`, `threadpool`, `once_cell`. Avoid on the hot path. |
| `signal` | Enable `utils::CtrlC` (Ctrl-C handler). Implies `sync`. |
| `signal-termination` | `signal` plus SIGTERM/SIGHUP handling (`ctrlc/termination`). |
| `zero-copy` | Set `SOCK_ZEROCOPY` on sockets and `MSG_ZEROCOPY` on sends. Fewer copies, but can degrade performance and was unstable in testing; stress test first. |
| `splice` | `splice` op (kernel 5.7+). |
| `mkdirat` | `mkdirat` op (kernel 5.15+). |
| `unlinkat` | `unlinkat` op (kernel 5.11+). |
| `renameat` | `renameat` op (kernel 5.11+). |
| `symlinkat` | `symlinkat` op (kernel 5.15+). |
| `tls-rustls` | `monoio::tls::rustls` (rustls 0.23, TLS 1.2/1.3). Needs a crypto provider: add `tls-rustls-ring` or `tls-rustls-aws-lc-rs`. |
| `tls-rustls-ring` | `tls-rustls` plus rustls' `ring` crypto provider. |
| `tls-rustls-aws-lc-rs` | `tls-rustls` plus rustls' `aws_lc_rs` provider (needs a C toolchain). |
| `tls-native` | `monoio::tls::native` (native-tls; OpenSSL on Linux, needs the OpenSSL dev package). |
| `tls-native-alpn` | `tls-native` plus `TlsStream::alpn_protocol()` (OpenSSL ALPN). |
| `tls-native-vendored` | `tls-native` with OpenSSL built from source. |
| `debug` | Emit `tracing` events describing what the runtime does. For runtime development only. Examples need `tracing-subscriber` to show them. |
| `unstable` | Enable nightly-only code paths (`io_error_more`, `lazy_cell`, `stmt_expr_attributes`, `thread_local`). Nightly toolchain required. |

## TLS
Enable `tls-rustls` and/or `tls-native`. Both wrap any `AsyncReadRent + AsyncWriteRent` (usually `TcpStream`) and yield a stream implementing the rent traits, `Split` (so `into_split()` works) and `shutdown()` (sends close_notify). Handshakes return `Result<_, TlsError>`, which converts into `io::Error`. Tests: `monoio/tests/tls_*.rs`; runnable demos: the `tls-*` examples.

**rustls** (`monoio::tls::rustls`). Enable `tls-rustls-ring` (or `tls-rustls-aws-lc-rs`) for a crypto provider; the rustls crate is re-exported as `monoio::tls::rustls::rustls` (`ServerConfig`, `ClientConfig`, `RootCertStore`, `pki_types`, PEM parsing), so you need no `rustls` dependency. Pass rustls configs straight in:
```rust
use monoio::tls::rustls::{rustls::pki_types::ServerName, TlsAcceptor, TlsConnector};

// server: ServerConfig -> TlsAcceptor (From<ServerConfig> / From<Arc<ServerConfig>>)
let acceptor = TlsAcceptor::from(server_config);
let mut tls = acceptor.accept(tcp_stream).await?;            // ServerTlsStream<TcpStream>

// client: ClientConfig -> TlsConnector
let connector = TlsConnector::from(client_config);
let mut tls = connector.connect(ServerName::try_from("example.com")?, tcp_stream).await?;

let (res, buf) = tls.read(buf).await;                          // normal rent IO
let alpn = tls.alpn_protocol();                                // set via config.alpn_protocols
```
Types: `ClientTlsStream`/`ServerTlsStream` (+ `*ReadHalf`/`*WriteHalf`). There is no single unified stream type.

**native-tls** (`monoio::tls::native`). The crate is re-exported as `monoio::tls::native::native_tls` (`Identity`, `Certificate`, builders). Wrap the `native_tls` builders' output:
```rust
use monoio::tls::native::{TlsAcceptor, TlsConnector};

let acceptor = TlsAcceptor::from(native_tls::TlsAcceptor::new(identity)?);   // Identity::from_pkcs8(chain, key)
let mut tls = acceptor.accept(tcp_stream).await?;                            // TlsStream<TcpStream>

let connector = TlsConnector::from(native_tls::TlsConnector::builder()
    .add_root_certificate(ca).build()?)
    .read_buffer(Some(32 * 1024));                                           // optional; .write_buffer() too, default 64 KiB
let mut tls = connector.connect("example.com", tcp_stream).await?;
```
The client verifies the hostname against the domain you pass. `alpn_protocol()` needs `tls-native-alpn` (client side).

Notes: everything above is reachable through this crate alone (`monoio::io_uring`, `monoio::uring::libc`, `monoio::tls::{rustls::rustls, native::native_tls}`); `monoio/tests/reexports.rs` pins those paths. Both backends copy through a read and a write staging buffer (the sync engines need `std::io`), 64 KiB plus record overhead each by default; set them with `.read_buffer(Some(n))` / `.write_buffer(Some(n))` on either backend's `TlsAcceptor`/`TlsConnector` (values below 18 KiB, one full record, are raised to 18 KiB). Records are gathered and sent with one socket write per buffer, and `writev` feeds all slices to the engine before one flush. Opt-in write coalescing: `.write_coalescing(true)` on the acceptor/connector, or `stream.set_write_coalescing(true)` (before `into_split`): `write` only buffers, data goes out when the write buffer is full or on `flush()`/`shutdown()`, so you **must call `flush()`** before waiting for the peer (rustls also merges small writes into records of up to 16 KiB). Default off: data is on the socket when `write` returns. Do not drop a handshake or IO future midway: it takes the staging buffer, so the stream breaks (a later use may panic). Await it to completion. Interop between the two backends works (covered by `tests/tls_interop.rs`).

## Environment gotchas
- io_uring needs kernel 5.6+. Check at runtime with `monoio::utils::detect_uring()`. Containers and sandboxes often block io_uring syscalls.
- The memlock limit must be large enough (`ulimit -l`). 512 KiB is usually enough; use `unlimited` for high throughput or large `entries`. Set it in `/etc/security/limits.conf` (`* soft memlock unlimited`, `* hard memlock unlimited`; log in again) or systemd `LimitMEMLOCK`. Symptom of too little: IO fails with os error 105 (`No buffer space available`).
- On macOS or Windows, develop in a Linux VM, container, or WSL2.
- Tests and examples that use the timer need `enable_timer`; `#[monoio::test]` accepts the same attributes as `main`.
