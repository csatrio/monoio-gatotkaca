# Monoio

A thread-per-core Rust async runtime built purely on io_uring.

- Linux only, kernel 5.6+.
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
monoio = "0.3"
```

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
- **Raw io_uring.** For operations the crate doesn't wrap, `monoio::uring::submit(sqe, data)` (unsafe) queues any `io_uring::squeue::Entry` and resolves to `(Cqe, data)`. `monoio::uring::with_ring(|ring| ..)` (unsafe) gives you the underlying `IoUring` for registering files/buffers or probing opcodes. Both are also methods on `IoUringDriver`, reachable with `Runtime::driver()`. The SQE must point into heap memory owned by `data`; do not push SQEs or reap CQEs on the ring yourself, since the driver owns `user_data`.
- **Tokio types.** `AsyncRead`/`AsyncWrite` from Tokio are not supported. Use the `*Rent` traits.

## Cargo features
Defaults: `async-cancel`, `bytes`, `macros`, `utils`. Optional: `sync`, `signal`, `signal-termination`, `zero-copy`, `splice`, `mkdirat`, `unlinkat`, `renameat`, `symlinkat`, `debug`, `unstable`. See [AGENTS.md](AGENTS.md) for what each one does and its kernel requirement.

## Development
```sh
cargo check --workspace
cargo test -p monoio
cargo run -p monoio-examples --example echo
```

## Related projects
- [local-sync](https://github.com/monoio-rs/local-sync): thread-local channels.
- [monoio-tls](https://github.com/monoio-rs/monoio-tls): TLS wrapper.
- [monoio-codec](https://github.com/monoio-rs/monoio-codec): codec utilities.
