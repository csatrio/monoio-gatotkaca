# W1-tpc (F4): `ThreadPerCore` helper

New: `monoio/src/utils/thread_per_core.rs` (feature `utils`, exported from `monoio::utils`),
`monoio/examples/thread_per_core.rs` (`[[example]]` with `required-features = ["utils"]`),
`monoio/tests/thread_per_core.rs`, one bullet in `AGENTS.md`. No change to `lib.rs`, the builder, the runtime or `net/`.

## API

```rust
use monoio::utils::{CoreInfo, ThreadPerCore, TpcHandle};

let handle: TpcHandle<T> = ThreadPerCore::new([0, 1, 2, 3])          // core i pinned to the i-th CPU
    .builder(|core: &CoreInfo| RuntimeBuilder::<IoUringDriver>::new()) // optional, see below
    .attach_wq(true)                                                   // optional, default false
    .thread_name("srv")                                                // threads srv-0, srv-1, ..
    .run(|core: CoreInfo| async move { /* per-core server */ })?;     // io::Result<TpcHandle<T>>
let results: Vec<std::thread::Result<T>> = handle.join();             // core order
```

- `ThreadPerCore<D = TimeDriver<IoUringDriver>>`. `new` uses `RuntimeBuilder::new().enable_timer()`.
  `.builder(f)` changes `D` to whatever the closure returns (`IoUringDriver` without the timer). `run` needs
  `D: Buildable + Driver + AsRawFd`, which both drivers satisfy. One generic type, no second constructor.
- `CoreInfo { index, cpu, cores, rings: Arc<[RingHandle]> }`. `rings[i]` is core `i`'s ring, for
  `uring::msg_ring`. The builder closure runs before every runtime exists, so it sees an empty `rings`. The
  `run` closure always gets the full list.
- `run`'s closure is `Fn(CoreInfo) -> Fut + Send + Sync + 'static`, and `Fut` need not be `Send`: it is created on
  the core's thread. Clone shared state (`Arc`, channels) into it.
- Listeners: on each core, `TcpListener::bind_with_config(addr, &ListenerOpts::new().reuse_port(true))`
  (`reuse_port` is already `true` in `ListenerOpts::new()`). The kernel picks the socket by a hash of the 4-tuple,
  so the split is only even on average (the test saw 32/32 of 64; the example 11/9 of 20). `SO_INCOMING_CPU` and
  `SO_ATTACH_REUSEPORT_CBPF` steering are out of scope.

## Startup and shutdown

1. `run` spawns one thread per CPU. Each thread pins itself (`bind_to_cpu_set`), calls the builder closure, adds
   `attach_wq(primary_fd)` if needed (after the closure, so it survives `uring_builder`), builds the runtime,
   makes a `RingHandle` and reports `Ok(handle)` or `Err(e)` on its own channel.
2. `run` waits for every report. A missing report (the thread panicked in the builder closure) counts as a
   failure, so a panic cannot hang `run`.
3. If anything failed (pinning, build, thread spawn, panic), `run` tells every started core to stop. No user
   closure runs. The started runtimes are dropped, their threads joined, and `run` returns the first error, e.g.
   `ThreadPerCore: core 1 (cpu 100000) failed to start: Invalid argument (os error 22)`. An empty CPU list gives
   `InvalidInput`.
4. Otherwise every core gets `Arc<[RingHandle]>` and runs `block_on(f(info))`, and `run` returns the handle.
   Because of the barrier, any core may `msg_ring` any peer from its first poll. The target must have its CQE hook
   or `msg_receiver()` installed before the message arrives (completions without a hook are dropped). The test
   uses a `Barrier` for that.
5. A core's thread ends when its future returns. Tasks still spawned on it are dropped with the runtime.
   `TpcHandle::join` joins the threads in core order. A panicking future gives `Err(payload)` for that core only.
   There is no built-in stop signal: the user's future decides when to return. The example and test use one
   `uring::event_channel` per core, whose `EventSender` is sent back to the main thread.

## attach_wq lifetimes

With `.attach_wq(true)`, core 0 is built first. Its ring fd (the dup held by its `RingHandle`, which is the same
kernel file) is passed to the other cores' `attach_wq`, and they are spawned only after core 0 has reported. A
latch counts the secondaries. Each secondary counts down with a guard that drops *after* its runtime. Core 0
holds a guard that is declared after its runtime, so it drops *before* the runtime and waits for zero. So core 0's
runtime outlives every other runtime, also when core 0's future returns early or panics (the guard runs during
unwinding). A secondary that fails or is never spawned also counts down. Caveat (documented): core 0's *thread*
only ends after all the others, so don't make another core wait for core 0's thread to finish.

## Tests (`tests/thread_per_core.rs`)

- `reuse_port_two_cores`: 2 cores (first two CPUs in the affinity mask; skipped with fewer than 2 or if `run`
  fails), one shared `SO_REUSEPORT` port, 64 echo connections from a std client. Both results come back and the
  sum is 64. The per-core split is printed.
- `attach_wq_primary_outlives_secondaries`: core 0 returns at once. Core 1 sleeps 200 ms, then does an `O_CREAT`
  open and a write (io-wq) and sets a flag. After core 0's join, the flag is already set.
- `msg_ring_between_cores`: core 0 sends `(7, 42)` to `rings[1]`, and core 1 receives it from `msg_receiver()`.
- `invalid_cpu_fails_startup` (with and without attach_wq): `[0, 100000]` returns `Err` naming core 1 and runs no
  closure. `empty_cpu_list` gives `InvalidInput`.

## Benchmark SUT with `--threads N` (sketch)

```rust
let cpus: Vec<usize> = args.cpus.clone().unwrap_or_else(|| (0..args.threads).collect());
let handle = ThreadPerCore::new(cpus)
    .builder(move |_| apply_tune(RuntimeBuilder::<IoUringDriver>::new().with_entries(entries)).enable_timer())
    .attach_wq(tune.attach_wq)               // one io-wq for file punts instead of N
    .run(move |core| {
        let cfg = cfg.clone();                     // Arc<Config>
        async move {
            let opts = ListenerOpts::new().reuse_port(true).backlog(65535);
            let listener = TcpListener::bind_with_config(cfg.addr, &opts)?;
            serve(listener, cfg, core.index).await    // the existing single-core accept loop
        }
    })?;
for (i, r) in handle.join().into_iter().enumerate() { /* report per-core stats / panic */ }
```

The single-core server stays as it is. `--threads 1` is `ThreadPerCore::new([cpu])`. Per-core counters come back
as `T`. The tokio side uses `multi_thread` with N workers. Cores that need to hand work to each other (for example
a shared upstream pool) use `core.rings` with `msg_ring`, or `event_channel` for payloads.

## Open questions / not done

- No CPU-aware connection steering (BPF/`SO_INCOMING_CPU`). Hash imbalance at low connection counts is expected.
- No option to skip pinning (for example in containers where `sched_setaffinity` is refused). Pinning failure
  is a startup error. A `.pin(false)` would be a one-line addition if the SUT needs it.
- No `lib.rs` re-export (the type is reached as `monoio::utils::ThreadPerCore`).
