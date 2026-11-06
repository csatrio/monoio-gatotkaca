# W2-sqpoll (M8): SQPOLL placement check

Changed: `monoio/src/builder.rs` (field `sqpoll`, methods `sqpoll` and `deny_sqpoll_conflict`, the check and
apply at `build()`), new private `monoio/src/builder/sqpoll_check.rs`, new `monoio/tests/sqpoll.rs`, one bullet in
`AGENTS.md` ("Runtime configuration"). The driver already handled SQPOLL rings (`need_wakeup`); it is unchanged.
The `#[monoio::main]` macro has no `sqpoll` attribute, so nothing was added there.

## API

```rust
RuntimeBuilder::<IoUringDriver>::new()
    .sqpoll(Duration::from_millis(10), Some(3)) // idle (rounded down to ms, capped at u32::MAX), SQ thread CPU
    .deny_sqpoll_conflict()                      // optional: conflict => build() fails with InvalidInput
    .build()?;
```

- `sqpoll` only records the setting. `build()` (both drivers; the timer driver goes through the same path) checks
  the placement, then applies `setup_sqpoll(idle_ms)` and, with a `cpu`, `setup_sqpoll_cpu(cpu)` to a copy of the
  stored `io_uring::Builder`. So `sqpoll` survives a later `uring_builder(..)` call (test
  `sqpoll_survives_uring_builder`).
- `uring_builder(IoUring::builder().setup_sqpoll(..))` still works but is not checked (the builder's flags are
  opaque); the method docs say so.

## The check

At `build()`, on the thread that creates the ring: `sched_getaffinity(0)` (the calling thread's mask; if it can't be
read, e.g. more than 1024 CPUs, the check is skipped).

| thread mask | `cpu` | result |
|---|---|---|
| one CPU `c` | `None` | conflict (the SQ thread inherits `{c}`, see below) |
| one CPU `c` | `Some(c)` | conflict |
| one CPU `c` | `Some(d)`, `d != c` | fine (SQ thread runs on `d`) |
| two or more CPUs | any | fine |

On a conflict: without `deny_sqpoll_conflict`, one `eprintln!` per process (`AtomicBool` swap) and the build goes
on; with it, `build()` returns `io::ErrorKind::InvalidInput` with a message that starts with `SQPOLL conflict:` and
says what to do. The error has no OS code, so it can be told apart from a kernel `EINVAL`.

`ThreadPerCore` pins each thread before building, so `sqpoll(idle, None)` in its builder closure hits the first
row (warns); pass each core a different CPU for the poller.

## Affinity finding (Linux 7.0.0, measured by `tests/sqpoll.rs`)

The tests read `Cpus_allowed_list` from `/proc/self/task/*/status` of the thread named `iou-sqp-<creator tid>`.
(It can appear in `/proc` a moment after `io_uring_setup` returns; the probe polls for up to 1 s.)

| creating thread mask | `cpu` | SQ thread `Cpus_allowed_list` |
|---|---|---|
| `{0}` | `None` | `0`: inherits the creator's mask, so it shares the loop's core |
| `{0,1}` | `None` | `0-1` |
| `{0,1}` | `Some(1)` | `1` |
| `{0}` | `Some(1)` | `1`: accepted, although CPU 1 is outside the creator's mask |

So on this kernel an unpinned SQ thread of a 1-CPU-pinned loop shares the loop's core (the M8 failure mode even
without `sqpoll_cpu`), and a 1-CPU-pinned loop can pin its poller to another CPU (the kernel checks the cpuset,
not the task's affinity; a CPU outside the cpuset gives `EINVAL`).

## Tests (`monoio/tests/sqpoll.rs`)

Each case runs on a fresh thread with its own affinity, one at a time (mutex). SQPOLL failing with `EPERM`/`ENOSYS`
skips the probing cases; cases needing two CPUs skip with fewer.

- `conflict_unpinned_sq_on_one_cpu_denied`: mask `{c}`, `sqpoll(.., None)` + deny → `InvalidInput`.
- `conflict_sq_on_loop_cpu_denied`: mask `{c}`, `sqpoll(.., Some(c))` + deny, with the timer → `InvalidInput`.
- `conflict_warns_and_unpinned_sq_inherits_affinity`: same without deny → builds, warns, echo runs; SQ thread on `c`.
- `sqpoll_survives_uring_builder`: `sqpoll` before `uring_builder` still checked.
- `no_conflict_sq_on_other_cpu`: mask `{a,b}`, `Some(b)` + deny → builds, echo runs, SQ thread on `b`.
- `no_conflict_unpinned_sq_on_wide_mask`: mask `{a,b}`, `None` + deny → builds, SQ thread on `a-b`.
- `no_conflict_sq_outside_one_cpu_mask`: mask `{a}`, `Some(b)` + deny → builds, SQ thread on `b`.
- Unit test `builder::sqpoll_check::tests::rules` for the table.

`cargo test -p adaptif-gatotkaca-monoio` (plain, `--features metrics`, `--features sync`) passes; `cargo clippy
--workspace` shows only the three warnings that were there before (`net/udp.rs`, `utils/slab.rs`).

G3 check for M8: `monoio:sqpoll` on the server's core with `deny_sqpoll_conflict()` fails to build, and warns
without it (covered by the first three tests).
