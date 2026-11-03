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
- **Communication.** Same thread: `local-sync` (mpsc, oneshot, semaphore, no atomics). Across threads: enable `sync` and use any channel (`flume`, `async-channel`, Tokio's `sync`) or atomics. Prefer per-thread aggregation on hot paths.
- **Raw io_uring access.** The `io_uring` crate is re-exported as `monoio::io_uring` (and `opcode`, `squeue`, `cqueue`, `types`, `IoUring`, `Probe` as `monoio::uring::*`): use these instead of depending on `io_uring` yourself, so versions always match. `monoio::uring` is the escape hatch for ops the crate doesn't wrap. `unsafe uring::submit(sqe, data)` queues any `squeue::Entry` and resolves to `(Cqe, data)`; `data` must own the heap memory the SQE points to and is kept alive until the kernel finishes. `unsafe uring::with_ring(|ring| ..)` exposes the `IoUring` for registration, probing and `params()`. Both also exist on `IoUringDriver` (`submit_raw`, `with_ring`; reach it with `Runtime::driver()`). Do not push SQEs or reap CQEs on the ring yourself: the driver owns `user_data` and consumes every CQE. Single-shot ops only; an fd returned by a raw op is yours to close.
- **Registered buffers.** Not supported natively. Use the raw API: register once with `unsafe uring::with_ring(|r| r.submitter().register_buffers(&iovecs))`, then submit `opcode::ReadFixed`/`WriteFixed` (buffer index) via `uring::submit`. The registered memory must stay valid and unmoved until unregistered; keep it alive in `data`. Same applies to registered files (`register_files`) and buffer rings.
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
| `tls-rustls` | `monoio::tls::rustls` (rustls 0.23, TLS 1.2/1.3). Does not pick a crypto provider; see TLS below. |
| `tls-native` | `monoio::tls::native` (native-tls; OpenSSL on Linux, needs the OpenSSL dev package). |
| `tls-native-alpn` | `tls-native` plus `TlsStream::alpn_protocol()` (OpenSSL ALPN). |
| `tls-native-vendored` | `tls-native` with OpenSSL built from source. |
| `debug` | Emit `tracing` events describing what the runtime does. For runtime development only. Examples need `tracing-subscriber` to show them. |
| `unstable` | Enable nightly-only code paths (`io_error_more`, `lazy_cell`, `stmt_expr_attributes`, `thread_local`). Nightly toolchain required. |

## TLS
Enable `tls-rustls` and/or `tls-native`. Both wrap any `AsyncReadRent + AsyncWriteRent` (usually `TcpStream`) and yield a stream implementing the rent traits, `Split` (so `into_split()` works) and `shutdown()` (sends close_notify). Handshakes return `Result<_, TlsError>`, which converts into `io::Error`. Tests: `monoio/tests/tls_*.rs`; runnable demos: the `tls-*` examples.

**rustls** (`monoio::tls::rustls`). Add `rustls` to your own `Cargo.toml` and enable a crypto provider (`ring` or `aws_lc_rs`); monoio does not. Pass rustls configs straight in:
```rust
use monoio::tls::rustls::{TlsAcceptor, TlsConnector};
use rustls::pki_types::ServerName;

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

**native-tls** (`monoio::tls::native`). Wrap the `native_tls` builders' output:
```rust
use monoio::tls::native::{TlsAcceptor, TlsConnector};

let acceptor = TlsAcceptor::from(native_tls::TlsAcceptor::new(identity)?);   // Identity::from_pkcs8(chain, key)
let mut tls = acceptor.accept(tcp_stream).await?;                            // TlsStream<TcpStream>

let connector = TlsConnector::from(native_tls::TlsConnector::builder()
    .add_root_certificate(ca).build()?)
    .read_buffer(Some(32 * 1024));                                           // optional; .write_buffer() too, default 16 KiB
let mut tls = connector.connect("example.com", tcp_stream).await?;
```
The client verifies the hostname against the domain you pass. `alpn_protocol()` needs `tls-native-alpn` (client side).

Notes: both backends copy through a 16 KiB staging buffer (the sync engines need `std::io`); do not drop a handshake or IO future midway: it takes the staging buffer, so the stream breaks (a later use may panic). Await it to completion. Interop between the two backends works (covered by `tests/tls_interop.rs`).

## Environment gotchas
- io_uring needs kernel 5.6+. Check at runtime with `monoio::utils::detect_uring()`. Containers and sandboxes often block io_uring syscalls.
- The memlock limit must be large enough (`ulimit -l`). 512 KiB is usually enough; use `unlimited` for high throughput or large `entries`. Set it in `/etc/security/limits.conf` (`* soft memlock unlimited`, `* hard memlock unlimited`; log in again) or systemd `LimitMEMLOCK`. Symptom of too little: IO fails with os error 105 (`No buffer space available`).
- On macOS or Windows, develop in a Linux VM, container, or WSL2.
- Tests and examples that use the timer need `enable_timer`; `#[monoio::test]` accepts the same attributes as `main`.
