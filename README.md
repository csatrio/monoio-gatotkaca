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
- Rust 1.75 or newer (nightly only for the `unstable` feature).
- Linux kernel 5.6+ with io_uring enabled. Containers and sandboxes often block it. Check with `monoio::utils::detect_uring()`.
- A sufficient memlock limit: `ulimit -l` should be at least 512 (KiB), or `unlimited`. If IO fails with os error 105 (`No buffer space available`), raise it in `/etc/security/limits.conf` or via systemd `LimitMEMLOCK`.

## Quick start
```toml
[dependencies]
adaptif-gatotkaca-monoio = "0.3"
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
Try it with `nc 127.0.0.1 50002`. More runnable programs live in [examples/](examples).

## Using it
- **Runtime options.** Build one with `RuntimeBuilder` or configure `#[monoio::main(...)]` with `entries`, `timer_enabled` and `worker_threads`. The timer is off by default, and `monoio::time` panics unless you enable it.
- **Multiple cores.** Run one runtime per thread. `worker_threads = n` in the macro does this for you. If you need CPU pinning, create the threads yourself and call `monoio::utils::bind_to_cpu_set`.
- **Talking between tasks.** Within a thread use [local-sync](https://crates.io/crates/local-sync). Across threads enable the `sync` feature and use any async channel, or atomics.
- **Timeouts and cancellation.** Dropping a future does not reliably cancel the kernel operation. Use the `Cancelable*` IO traits with `Canceller`/`CancelHandle`, then await the future to get your buffer back. A cancelled op may still have completed, so check for `ECANCELED` (os error 125).
- **Raw io_uring.** The `io_uring` crate is re-exported (`monoio::io_uring`, plus `monoio::uring::{opcode, squeue, cqueue, types, IoUring, Probe}`), so you don't need your own dependency on it. For operations the crate doesn't wrap, `monoio::uring::submit(sqe, data)` (unsafe) queues any `io_uring::squeue::Entry` and resolves to `(Cqe, data)`. `monoio::uring::with_ring(|ring| ..)` (unsafe) gives you the underlying `IoUring` for registering files/buffers or probing opcodes. Both are also methods on `IoUringDriver`, reachable with `Runtime::driver()`. The SQE must point into heap memory owned by `data`; to use multishot or other ops yourself, push SQEs inside `with_ring` tagged with `monoio::uring::user_data(id)` and receive their completions with `monoio::uring::set_cqe_hook(|id, cqe| ..)`. Never reap the completion queue yourself.
- **Registered buffers.** Not supported natively. Register them through the raw API: `unsafe monoio::uring::with_ring(|r| r.submitter().register_buffers(&iovecs))`, then submit `opcode::ReadFixed`/`WriteFixed` with `monoio::uring::submit`. The memory must stay valid and unmoved while registered.
- **Tokio types.** `AsyncRead`/`AsyncWrite` from Tokio are not supported. Use the `*Rent` traits.

## Cargo features
Defaults: `async-cancel`, `bytes`, `macros`, `utils`. Optional: `sync`, `signal`, `signal-termination`, `zero-copy`, `splice`, `mkdirat`, `unlinkat`, `renameat`, `symlinkat`, `debug`, `unstable`, and TLS: `tls-rustls`, `tls-native`, `tls-native-alpn`, `tls-native-vendored` (`monoio::tls`, see the `tls-*` examples). See [AGENTS.md](AGENTS.md) for what each one does and its kernel requirement.

## Development
```sh
cargo check --workspace
cargo test -p adaptif-gatotkaca-monoio
cargo run -p monoio-examples --example echo
```

## Related projects
- [local-sync](https://github.com/monoio-rs/local-sync): thread-local channels.
- [monoio-tls](https://github.com/monoio-rs/monoio-tls): TLS wrapper.
- [monoio-codec](https://github.com/monoio-rs/monoio-codec): codec utilities.

## Credits and license
This crate is a modified fork of [Monoio](https://github.com/bytedance/monoio), Copyright (c) 2024 Monoio Contributors, and its TLS support is derived from [monoio-tls](https://github.com/monoio-rs/monoio-tls). Both are licensed under MIT or Apache-2.0, as is this crate: see [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE). Code derived from Mio, Tokio, Tokio-uring, Futures-rs and Slab is covered by [LICENSE-THIRD-PARTY](LICENSE-THIRD-PARTY). [NOTICE](NOTICE) lists the changes made to the original.

Maintained by [csatrio](https://github.com/csatrio) for [Adaptif](https://www.adaptif.co.id).
