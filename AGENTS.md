# AGENTS.md

Monoio: a thread-per-core async runtime built only on io_uring. Linux only, kernel 5.6+. There is no epoll/legacy/IOCP driver.

## Layout
- `monoio/`: the runtime crate.
  - `src/`: `builder`, `runtime`, `scheduler`, `task`, `driver/uring` (+ `op`), `io` (rent traits), `buf`, `net/{tcp,udp,unix}`, `fs`, `time`, `blocking`, `macros` (`join!`, `select!`, `try_join!`), `utils`.
  - `tests/`: integration tests; most need a working io_uring.
- `monoio-macros/`: proc macros `#[monoio::main]`, `#[monoio::test]`, `#[monoio::test_all]`, and `select!` internals.
- `examples/`: unpublished crate, built with default features off. Run one with `cargo run -p monoio-examples --example echo`. Examples: accept, builder, channel, echo, echo-tfo, join, macro, proxy, tcp_uring, timer, timer-select, uds.

## Commands
- Check: `cargo check --workspace`
- Test: `cargo test -p monoio` (add `--features splice,mkdirat,...` for the optional ops; they need a new enough kernel)
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
- **Communication.** Same thread: `local-sync` (mpsc, oneshot, semaphore, no atomics). Across threads: enable `sync` and use any channel (`flume`, `async-channel`, Tokio's `sync`) or atomics. Prefer per-thread aggregation on hot paths.
- **Raw io_uring access.** `monoio::uring` is the escape hatch for ops the crate doesn't wrap. `unsafe uring::submit(sqe, data)` queues any `squeue::Entry` and resolves to `(Cqe, data)`; `data` must own the heap memory the SQE points to and is kept alive until the kernel finishes. `unsafe uring::with_ring(|ring| ..)` exposes the `IoUring` for registration, probing and `params()`. Both also exist on `IoUringDriver` (`submit_raw`, `with_ring`; reach it with `Runtime::driver()`). Do not push SQEs or reap CQEs on the ring yourself: the driver owns `user_data` and consumes every CQE. Single-shot ops only; an fd returned by a raw op is yours to close.
- **Registered buffers.** Not supported natively (`driver/pool.rs` is dead code). Use the raw API: register once with `unsafe uring::with_ring(|r| r.submitter().register_buffers(&iovecs))`, then submit `opcode::ReadFixed`/`WriteFixed` (buffer index) via `uring::submit`. The registered memory must stay valid and unmoved until unregistered; keep it alive in `data`. Same applies to registered files (`register_files`) and buffer rings.
- **Blocking work.** `monoio::spawn_blocking` needs `sync`. Attach a pool with `RuntimeBuilder::attach_thread_pool` or choose `with_blocking_strategy` (`Panic` or `ExecuteLocal`).

## Runtime configuration
Via builder or macro: `RuntimeBuilder::<IoUringDriver>::new().with_entries(n).enable_timer().build()`, or `#[monoio::main(entries = n, timer_enabled = true, worker_threads = n)]`.

- `entries`: io_uring ring size. Default 1024, minimum 256 (smaller values are raised to 256), 0 is a macro error. Larger rings mean fewer submits and more memory. It also sets the initial inflight-op cache (`10 * entries`).
- `enable_timer` / `timer_enabled`: off by default. `monoio::time::{sleep, interval, ...}` panics unless it is on. `std::time` is unaffected. `enable_all()` is also available.
- `worker_threads` (aliases `workers`, `threads`): default 1. With `n`, the macro starts `n-1` extra threads, each with its own runtime, plus the main thread. For CPU pinning or other per-thread setup, create threads and runtimes yourself (see `utils::bind_to_cpu_set`).
- `uring_builder(io_uring::Builder)`: tune the raw io_uring settings.

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
| `debug` | Emit `tracing` events describing what the runtime does. For runtime development only. Examples need `tracing-subscriber` to show them. |
| `unstable` | Enable nightly-only code paths (`io_error_more`, `lazy_cell`, `stmt_expr_attributes`, `thread_local`). Nightly toolchain required. |

## Environment gotchas
- io_uring needs kernel 5.6+. Check at runtime with `monoio::utils::detect_uring()`. Containers and sandboxes often block io_uring syscalls.
- The memlock limit must be large enough (`ulimit -l`). 512 KiB is usually enough; use `unlimited` for high throughput or large `entries`. Set it in `/etc/security/limits.conf` (`* soft memlock unlimited`, `* hard memlock unlimited`; log in again) or systemd `LimitMEMLOCK`. Symptom of too little: IO fails with os error 105 (`No buffer space available`).
- On macOS or Windows, develop in a Linux VM, container, or WSL2.
- Tests and examples that use the timer need `enable_timer`; `#[monoio::test]` accepts the same attributes as `main`.
