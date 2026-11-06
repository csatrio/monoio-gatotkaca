# Monoio

A thread-per-core Rust async runtime built purely on io_uring.

This is a stripped-down fork of [Monoio](https://github.com/bytedance/monoio) by ByteDance and the Monoio contributors. It runs only on Linux and only on io_uring. It is the runtime behind the transport system of adaptif-gatotkaca, from [Adaptif](https://www.adaptif.co.id).

- Linux only, kernel 5.6+.
- TLS support: rustls and native-tls backends behind the `tls-rustls` and `tls-native` features (see `monoio::tls`).
- Tasks are `!Send`, so thread-local state is safe and cheap.
- IO takes ownership of buffers, which io_uring requires. Calls return `(result, buffer)`.
- Intended for IO-bound servers such as proxies and load balancers. It is not a general-purpose replacement for Tokio: with very unbalanced workloads some cores sit idle and Tokio's work stealing can do better.

For the design, feature flags and configuration, see [AGENTS.md](AGENTS.md).

## Requirements
- Rust 1.85 or newer (nightly only for the `unstable` feature).
- Linux kernel 5.6+ with io_uring enabled. Containers and sandboxes often block it. Check with `monoio::utils::detect_uring()`.
- A sufficient memlock limit: `ulimit -l` should be at least 512 (KiB), or `unlimited`. If IO fails with os error 105 (`No buffer space available`), raise it in `/etc/security/limits.conf` or via systemd `LimitMEMLOCK`.

## Quick start
```toml
[dependencies]
adaptif-gatotkaca-monoio = "0.9"
```
The library is still named `monoio`, so code says `use monoio::...` and `#[monoio::main]`.

```rust,no_run
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};

#[monoio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:50002").unwrap();
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("accepted a connection from {addr}");
                monoio::spawn(echo(stream));
            }
            Err(e) => {
                println!("accept failed: {e}");
                return;
            }
        }
    }
}

async fn echo(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut res;
    loop {
        (res, buf) = stream.read(buf).await;
        if res? == 0 {
            return Ok(());
        }
        (res, buf) = stream.write_all(buf).await;
        res?;
        buf.clear();
    }
}
```
Try it with `nc 127.0.0.1 50002`. More runnable programs live in [examples/](examples/) and, for load reproducers (`fairness`, `smallframes`, `reqresp`, `thread_per_core`), in `monoio/examples/`.

## Using it
- **Runtime options.** Build one with `RuntimeBuilder` or configure `#[monoio::main(...)]` with `entries`, `timer_enabled` and `worker_threads`. The timer is off by default, and `monoio::time` panics unless you enable it.
- **Multiple cores.** Run one runtime per thread. `worker_threads = n` in the macro does this for you. For one pinned runtime per CPU use `monoio::utils::ThreadPerCore`, or create the threads yourself and call `monoio::utils::bind_to_cpu_set`.
- **Talking between tasks.** Within a thread use [local-sync](https://crates.io/crates/local-sync). Between runtime threads use `monoio::uring::msg_ring` (`IORING_OP_MSG_RING`); from any thread into a runtime thread, `monoio::uring::event_channel()`. Neither needs the `sync` feature; with it, any async channel or atomics work too.
- **Timeouts and cancellation.** Dropping a future does not reliably cancel the kernel operation. Use the `Cancelable*` IO traits with `Canceller`/`CancelHandle`, then await the future to get your buffer back. A cancelled op may still have completed, so check for `ECANCELED` (os error 125). `TcpStream::read_with_deadline`/`write_with_deadline` attach a kernel link timeout without the timer; with the timer, `monoio::time::Deadline` is one re-armable timer per connection for request deadlines.
- **Servers.** Accept with `listener.accept_batch_into(..)` (a multishot accept kept in the listener), not one `accept()` per loop round. For request/response loops see "Recommended configuration for request/response servers" in [AGENTS.md](AGENTS.md): `defer_taskrun`, `drain_taskrun(2)`, replies through `send_eager`.
- **Raw io_uring.** The `io_uring` and `libc` crates are re-exported (`monoio::io_uring`, `monoio::uring::libc`, plus `monoio::uring::{opcode, squeue, cqueue, types, IoUring, Probe}`), so you don't need your own dependency on them. The same goes for TLS: `monoio::tls::rustls::rustls` and `monoio::tls::native::native_tls`. For operations the crate doesn't wrap, `monoio::uring::submit(sqe, data)` (unsafe) queues any `io_uring::squeue::Entry` and resolves to `(Cqe, data)`. `monoio::uring::with_ring(|ring| ..)` (unsafe) gives you the underlying `IoUring` for registering files or probing opcodes. Both are also methods on `IoUringDriver`, reachable with `Runtime::driver()`. The SQE must point into heap memory owned by `data`; to use multishot or other ops yourself, push SQEs inside `with_ring` tagged with `monoio::uring::user_data(id)` and receive their completions with `monoio::uring::set_cqe_hook(|id, cqe| ..)`. Never reap the completion queue yourself.
- **Provided buffers and shared worker pool.** `monoio::uring::BufRing` registers a kernel-selected buffer pool (`IOSQE_BUFFER_SELECT`, Linux 5.19+): `stream.recv_multi(&ring)` receives into it, or submit a `Recv`/`RecvMulti` with the ring's group yourself and call `ring.take(cqe)` for the data. `RuntimeBuilder::attach_wq(fd)` shares the io-wq of another runtime (`IORING_SETUP_ATTACH_WQ`). Both work with the timer enabled.
- **Registered (fixed) buffers.** `monoio::uring::FixedBufPool::new(count, size)` registers a pool (Linux 5.13+); `pool.get()` gives a `FixedBuf` for `File::read_fixed_at`/`write_fixed_at` and `TcpStream::recv_fixed`/`send_fixed`. Fixed files: `RuntimeBuilder::with_fixed_files(n)` and `stream.register_fixed()`, or `listener.accept_direct()`/`accept_batch_direct_into(..)` for sockets with no fd at all.
- **Tokio types.** `AsyncRead`/`AsyncWrite` from Tokio are not supported. Use the `*Rent` traits.

## Cargo features
Defaults: `async-cancel`, `bytes`, `macros`, `utils`. Optional: `sync`, `signal`, `signal-termination`, `zero-copy`, `splice`, `mkdirat`, `unlinkat`, `renameat`, `symlinkat`, `metrics` (driver and scheduler counters; off for production builds), `debug`, `unstable`, and TLS: `tls-rustls` (with `tls-rustls-ring` or `tls-rustls-aws-lc-rs` for the crypto provider), `tls-native`, `tls-native-alpn`, `tls-native-vendored` (`monoio::tls`, see the `tls-*` examples). See [AGENTS.md](AGENTS.md) for what each one does and its kernel requirement.

## Development
```sh
cargo check --workspace
cargo test -p adaptif-gatotkaca-monoio
cargo run -p monoio-examples --example echo
```

## Documentation
- [AGENTS.md](AGENTS.md): design, APIs, feature flags and configuration.
- [docs/RELEASES.md](docs/RELEASES.md): what each release changed.
- [docs/TODO.md](docs/TODO.md): open work and known limits.
- [docs/performance-improvement/](docs/performance-improvement/): the performance rounds, each with a spec (`ROUNDn.md`), a work plan (`ROUNDn-PLAN.md`) and the gate results (`roundn/G*.md`).

## Related projects
- [local-sync](https://github.com/monoio-rs/local-sync): thread-local channels.
- [monoio-tls](https://github.com/monoio-rs/monoio-tls): TLS wrapper.
- [monoio-codec](https://github.com/monoio-rs/monoio-codec): codec utilities.

## Credits and license
This crate is a modified fork of [Monoio](https://github.com/bytedance/monoio), Copyright (c) 2024 Monoio Contributors, and its TLS support is derived from [monoio-tls](https://github.com/monoio-rs/monoio-tls). Both are licensed under MIT or Apache-2.0, as is this crate: see [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE). Code derived from Mio, Tokio, Tokio-uring, Futures-rs and Slab is covered by [LICENSE-THIRD-PARTY](LICENSE-THIRD-PARTY). [NOTICE](./NOTICE) lists the changes made to the original.

Maintained by [csatrio](https://github.com/csatrio) for [Adaptif](https://www.adaptif.co.id).
