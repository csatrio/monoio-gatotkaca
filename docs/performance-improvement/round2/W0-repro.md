# W0-repro: `fairness` load reproducer

A one-core reproducer for M1, M2 and M3 (ROUND2.md): unbounded bulk streams against small request/response
traffic, the proxy-style hand-over hop, and the accept ramp. It is an example of the runtime crate itself
(`monoio/examples/fairness.rs`, `[[example]]` entry in `monoio/Cargo.toml` with `required-features = ["bytes",
"utils"]`), so it builds with the crate's own features. Like the benchmark server, it runs without `sync`. It has
no extra dependencies: `bytes` and `nix` (`utils`) are already runtime dependencies, and arguments, histogram and
JSON are hand-written.

```sh
cargo run --release -p adaptif-gatotkaca-monoio --example fairness -- --bulk 400 --rr 200 --rate 2000 --hop
cargo run --release -p adaptif-gatotkaca-monoio --example fairness --features metrics -- ... --json
```

## How it works

- **Server**: one thread pinned to `--cpu` with one runtime (`with_entries(4096)`, `enable_timer()`, plus
  `apply_tune`). It has two listeners on 127.0.0.1 (bulk, rr), backlog 65535 like `somaxconn` on the benchmark
  box, and one accept loop each in the `--accept` mode. Sockets get `TCP_NODELAY`.
- **Clients**: one OS thread per client CPU, each with its own runtime, pinned. Connections are spread
  round-robin over the threads. Latency is measured on the client with `std::time::Instant`.
- **Timeline**: clients connect the base connections first. Then load starts, followed by `--warmup` s and the
  measured window of `--duration` s. Only events inside the window count. Clients collect 0.5 s after the window
  ends, then the process prints and calls `exit(0)`.
- **bulk** (closed loop): the client sends 1 byte. The server writes `--bulk-size` bytes as `--chunk` slices of
  one preallocated `Bytes`, one `write_all` per chunk (like `/stream/1024`). The client reads it all and asks
  again at once. Reported: MB/s (10^6 bytes, received in the window) and responses/s.
- **rr** (open loop, coordinated-omission corrected): every rr connection, base and ramp alike, gets a fixed
  schedule with interval `(rr + accept-ramp) / rate`. Base connections get phases spread over one interval.
  - Requests are 64 bytes and responses `--rr-size`. One request is outstanding per connection (like HTTP/1.1
    without pipelining).
  - Latency runs from the *scheduled* send time to the end of the response.
  - A request that misses its `sched + --timeout` deadline is a timeout and is not recorded in the histogram. The
    connection is closed and reconnected, because its state is unknown.
  - A slot that is already older than the timeout when its turn comes counts as a timeout without being sent.
- **rr accounting**:
  - `scheduled` = slots in the window, including those not yet issued at collection time (backlog of late
    connections);
  - `sent` = slots issued, or expired as timeouts;
  - `unfinished` = issued but still in flight at collection;
  - `achieved` = ok / scheduled.
- **--hop** (proxy model): the connection task leases one of `--pool` worker slots.
  - If all slots are taken, it queues a waiter in a FIFO, and `release` hands the lease straight to the first
    waiter with `Waker::wake` (like the benchmark's upstream pool).
  - It then wakes the worker task, which does `--hop-nops` io_uring NOP round trips (default 0), "produces" the
    response and wakes the connection task back. The connection task releases the lease and writes the response.
  - Per request: read CQE → (lease wake) → worker wake → requester wake → write. Each hop puts a task at the back
    of the ready queue. `--hop-nops 2` adds two CQE-driven hops as a stand-in for the upstream write and read.
  - Without `--hop`, the connection task answers directly (json model).
- **accept ramp**: `--accept-ramp K` connections are opened at `start + i * ramp-secs / K`.
  - Each one sends its first request right away. Connect → first response, measured from the scheduled connect
    time, goes into the ramp histogram. The connection then continues as an ordinary rr connection.
  - `pending` = ramp connections still without a first response at collection time.
  - The server counts accepts in the window (`accepts/s`, plus the peak one-second bucket), and the time from the
    accept returning to the first full request read (`accept->first-read`). The ramp latency minus that time is
    roughly the time spent in the kernel accept queue (M1 hypothesis 1 vs 2).
- **control late**: how late the server's control task woke after the window end. This is a crude measure of
  round length / timer starvation on the server.
- **`--features metrics`**: the server control task snapshots `monoio::driver_stats()` at the window start and
  end, on the server thread (the stats are thread-local). It prints the delta: `enter_submit`, `enter_wait`,
  `enter_skipped`, `sqes_submitted`, `cqes_reaped`, `ops_accept` (`ops_by_opcode[Accept]`), and `cqes_per_tick`
  (buckets 0, 1, 2–3, 4–7, 8–15, 16–31, 32+). In JSON this is `"driver":{..}`. `// TODO(merge): runtime_stats()`
  in `metrics_delta` marks where the scheduler stats delta goes.

## Flags

| flag | default | meaning |
|---|---|---|
| `--cpu C` | 2 | server CPU |
| `--client-cpus a,b,..` | all but `--cpu` | client CPUs, one client thread (runtime) each |
| `--entries N` | 4096 | server ring size |
| `--budget N` | off | `task_budget(N)` |
| `--coop` | off | `coop_taskrun()` |
| `--defer` | off | `defer_taskrun()` |
| `--accept single\|multi` | single | `single`: one `accept()` at a time, then `monoio::spawn` (today's SUT). `multi`: `accept_multi(NONBLOCK\|CLOEXEC)`, re-armed when it ends |
| `--bulk N` | 0 | bulk connections |
| `--bulk-size SZ` | 1m | bytes per bulk response |
| `--chunk SZ` | 128k | server write unit |
| `--rr M` | 0 | base rr connections |
| `--rate R` | 1000 | rr requests/s in total over all rr connections (base + ramp) |
| `--rr-size SZ` | 8192 | rr response size (request is 64 B) |
| `--timeout S` | 10 | client timeout per rr request |
| `--hop` | off | answer via a leased worker task (proxy model) |
| `--pool P` | 64 | worker slots in the `--hop` pool |
| `--hop-nops N` | 0 | NOP round trips per worker job (extra CQE hops) |
| `--upstream` | off | proxy model with a real upstream echo thread (W2-m3): `--proxy-frac` of the rr connections lease one of `--pool` upstream connections (strict FIFO waiters, like `common::pool`), write/read `--rr-size` bytes upstream, release, reply. Reports json and proxy latency separately plus server-side step times (`lease`, `up_write`, `up_read`, `reply`, `json_reply`) |
| `--proxy-frac F` | 0.5 | share of rr connections that are proxy connections (`--upstream`) |
| `--upstream-cpu C` | first client CPU | upstream echo CPU (removed from the client set) |
| `--small-io SZ` | off | `small_io_priority(Some(SZ))` (W2-m3) |
| `--accept-ramp K` | 0 | extra rr connections opened during the ramp |
| `--ramp-secs S` | 5 | ramp length, from the window start |
| `--warmup S` | 2 | seconds before the window |
| `--duration S` | 10 | measured window |
| `--json` | off | also print one JSON object |

Sizes take `k`/`m` (KiB/MiB). Seconds may be fractional. `RLIMIT_NOFILE` is raised to the hard limit in-process,
with a warning if it is below 2 fds per connection (both ends live in this process).

**Spots for later packages:**
- `fn apply_tune(builder, args)` holds the server's builder options. `TODO(W1-sched)` marks `--coop-budget N` and
  `--lifo`, `TODO(W2)` marks `--reap-without-enter`.
- The `Args` struct carries the same TODOs.

## Mapping to the benchmark

| benchmark | reproducer |
|---|---|
| `/stream/1024` at 1 MiB × 4096 (10% of 4096 ≈ 400 unbounded streams, 128 KiB units) | `--bulk 400` (defaults 1m / 128k) |
| `GET /json` (one hop) | rr without `--hop` |
| `POST /echo` via the 64-connection upstream pool (lease hand-over, 4–5 hops) | `--hop --pool 64` (`--hop-nops 2` for the upstream I/O hops) |
| connection ramp of 4096 connections | `--accept-ramp K --accept single` (SUT) / `multi` (`monoio:acceptmulti`) |
| `monoio:budget16`, `budget61` | `--budget 16`, `--budget 61` |
| `monoio:coop`, `monoio:defer` | `--coop`, `--defer` |
| 64 KiB cell | `--bulk-size 64k --chunk 64k` (or smaller chunk) |

- **M1** (1 MiB × 4096): `--bulk 400 --rr 200 --rate 2000 --accept-ramp 2000 --accept single`, compared with
  `--accept multi`. Watch `accepts/s` against the round rate, `ramp pending`, and `accept->first-read` vs ramp
  first response.
- **M2**: `--bulk 400 --rr 200 --rate 2000 --hop` with and without `--budget 16`. With `--features metrics`, look
  at `enter_submit` per second.
- **M3**: steady-state rr p99 under `--bulk 400` without ramp, against a light run (`--bulk 0` or `--bulk 40`).

## Smoke runs (screening, machine not quiet)

Release build, 12 CPUs, kernel 7.0, server on CPU 2, clients on the other 11. Other agents were compiling at the
same time, so these numbers are only good for checking that the tool works and shows the shapes.

```
$ fairness --bulk 40 --rr 50 --rate 500 --duration 3 --warmup 1
fairness [default accept=single] bulk=40 rr=50 rate=500 ramp=0 | rr p50=2.53 p90=3.30 p99=3.94 p99.9=5.18 max=5.78 ms scheduled=1500 sent=1500 ok=1500 timeouts=0 errors=0 unfinished=0 achieved=100.0% | bulk 3861 MB/s 3683 resp/s errors=0 | ramp first-resp p50=0.00 p99=0.00 max=0.00 ms ok=0 timeouts=0 errors=0 pending=0 | accepts 0.0/s peak 0/s accept->first-read p99=0.00 ms | control late 1.8 ms

$ fairness --bulk 400 --rr 200 --rate 2000 --duration 5
fairness [default accept=single] bulk=400 rr=200 rate=2000 ramp=0 | rr p50=20.22 p90=26.88 p99=32.51 p99.9=36.35 max=38.39 ms scheduled=10000 sent=10000 ok=10000 timeouts=0 errors=0 unfinished=0 achieved=100.0% | bulk 3550 MB/s 3424 resp/s errors=0 | ramp first-resp p50=0.00 p99=0.00 max=0.00 ms ok=0 timeouts=0 errors=0 pending=0 | accepts 0.0/s peak 0/s accept->first-read p99=0.00 ms | control late 13.5 ms

$ fairness --bulk 400 --rr 200 --rate 2000 --duration 5 --hop
fairness [default accept=single hop pool=64 nops=0] bulk=400 rr=200 rate=2000 ramp=0 | rr p50=20.74 p90=27.39 p99=35.33 p99.9=40.45 max=42.63 ms scheduled=10000 sent=10000 ok=10000 timeouts=0 errors=0 unfinished=0 achieved=100.0% | bulk 3523 MB/s 3360 resp/s errors=0 | ramp first-resp p50=0.00 p99=0.00 max=0.00 ms ok=0 timeouts=0 errors=0 pending=0 | accepts 0.0/s peak 0/s accept->first-read p99=0.00 ms | control late 0.6 ms

$ fairness --bulk 400 --rr 200 --rate 2000 --duration 5 --budget 16
fairness [budget16 accept=single] bulk=400 rr=200 rate=2000 ramp=0 | rr p50=14.21 p90=15.23 p99=19.20 p99.9=21.25 max=22.84 ms scheduled=10000 sent=10000 ok=10000 timeouts=0 errors=0 unfinished=0 achieved=100.0% | bulk 3740 MB/s 3535 resp/s errors=0 | ramp first-resp p50=0.00 p99=0.00 max=0.00 ms ok=0 timeouts=0 errors=0 pending=0 | accepts 0.0/s peak 0/s accept->first-read p99=0.00 ms | control late 0.9 ms

$ fairness --bulk 400 --rr 200 --rate 2000 --duration 5 --hop --budget 16
fairness [budget16 accept=single hop pool=64 nops=0] bulk=400 rr=200 rate=2000 ramp=0 | rr p50=1359.87 p90=1785.86 p99=1884.16 p99.9=1903.00 max=1903.00 ms scheduled=10000 sent=7406 ok=7206 timeouts=0 errors=0 unfinished=200 achieved=72.1% | bulk 3077 MB/s 2901 resp/s errors=0 | ramp first-resp p50=0.00 p99=0.00 max=0.00 ms ok=0 timeouts=0 errors=0 pending=0 | accepts 0.0/s peak 0/s accept->first-read p99=0.00 ms | control late 0.8 ms

$ fairness --bulk 400 --rr 200 --rate 2000 --duration 5 --accept-ramp 2000 --accept single
fairness [default accept=single] bulk=400 rr=200 rate=2000 ramp=2000 | rr p50=29.44 p90=1654.78 p99=3244.03 p99.9=3571.71 max=3595.60 ms scheduled=4509 sent=2092 ok=2086 timeouts=0 errors=0 unfinished=6 achieved=46.3% | bulk 2719 MB/s 2590 resp/s errors=0 | ramp first-resp p50=2588.67 p99=4653.06 max=4691.20 ms ok=316 timeouts=0 errors=0 pending=1684 | accepts 58.4/s peak 66/s accept->first-read p99=40.45 ms | control late 10.8 ms

$ fairness --bulk 400 --rr 200 --rate 2000 --duration 5 --accept-ramp 2000 --accept multi      # --features metrics build
fairness [default accept=multi] bulk=400 rr=200 rate=2000 ramp=2000 | rr p50=20.22 p90=26.88 p99=28.93 p99.9=33.28 max=35.18 ms scheduled=4509 sent=4509 ok=4509 timeouts=0 errors=0 unfinished=0 achieved=100.0% | bulk 3664 MB/s 3462 resp/s errors=0 | ramp first-resp p50=32.51 p99=43.52 max=50.12 ms ok=2000 timeouts=0 errors=0 pending=0 | accepts 399.4/s peak 402/s accept->first-read p99=17.15 ms | control late 4.1 ms
driver delta: enter_submit=0 (0/s) enter_wait=392 (78/s) enter_skipped=0 (0/s) sqes_submitted=171742 (34348/s) cqes_reaped=171797 (34359/s) ops_accept=0 (0/s) cqes_per_tick=[0,0,0,0,0,0,392]

$ fairness --bulk 400 --rr 200 --rate 2000 --duration 5 --hop --budget 16 --json             # --features metrics build
fairness [budget16 accept=single hop pool=64 nops=0] bulk=400 rr=200 rate=2000 ramp=0 | rr p50=495.62 p90=827.39 p99=909.31 p99.9=925.70 max=934.23 ms scheduled=10000 sent=9336 ok=9136 timeouts=0 errors=0 unfinished=200 achieved=91.4% | bulk 3758 MB/s 3593 resp/s errors=0 | ramp first-resp p50=0.00 p99=0.00 max=0.00 ms ok=0 timeouts=0 errors=0 pending=0 | accepts 0.0/s peak 0/s accept->first-read p99=0.00 ms | control late 0.7 ms
driver delta: enter_submit=12761 (2552/s) enter_wait=0 (0/s) enter_skipped=0 (0/s) sqes_submitted=178400 (35680/s) cqes_reaped=178400 (35680/s) ops_accept=0 (0/s) cqes_per_tick=[0,0,254,1642,1234,9631,0]
{"tune":"budget16","accept":"single","hop":true,"pool":64,"hop_nops":0,"bulk_conns":400,"rr_conns":200,"rate":2000,"ramp_conns":0,"duration_s":5,"rr_p50_ms":495.616,"rr_p90_ms":827.392,"rr_p99_ms":909.312,"rr_p999_ms":925.696,"rr_max_ms":934.230,"rr_scheduled":10000,"rr_sent":9336,"rr_ok":9136,"rr_unfinished":200,"rr_timeouts":0,"rr_errors":0,"rr_achieved_pct":91.36,"bulk_mb_s":3758.4,"bulk_resp_s":3592.6,"bulk_errors":0,"ramp_p50_ms":0.000,"ramp_p99_ms":0.000,"ramp_max_ms":0.000,"ramp_ok":0,"ramp_timeouts":0,"ramp_errors":0,"ramp_pending":0,"accepts_per_s":0.00,"accepts_peak_1s":0,"accept_to_first_read_p50_ms":0.000,"accept_to_first_read_p99_ms":0.000,"accept_to_first_read_max_ms":0.000,"control_late_ms":0.69,"clients_missing":0,"driver":{"enter_submit":12761,"enter_wait":0,"enter_skipped":0,"sqes_submitted":178400,"cqes_reaped":178400,"ops_accept":0,"cqes_per_tick":[0,0,254,1642,1234,9631,0]}}
```

What the shapes suggest (for G0 to confirm on a quiet machine, not verdicts):
- **M2 reproduces.**
  - `--budget 16` alone improves rr (p99 32.5 → 19.2 ms), like json in the benchmark.
  - `--hop --budget 16` collapses: p50 0.5–1.4 s, 72–91% achieved, and latency grows through the window (queueing).
  - Default `--hop` is fine (p99 35 ms).
- **M1 accept hypothesis looks right.**
  - With `--accept single`, accepts run at ~58–81/s. That is about the round rate: the metrics run shows
    `enter_wait` 78/s with the default budget, i.e. ~13 ms rounds with 400 bulk tasks.
  - Ramp connections wait seconds for their first response (p50 2.6 s, 1684 of 2000 still pending at the end).
  - The server-side accept→first-read stays small (p99 40 ms), so the time goes into the kernel accept queue
    (hypothesis 1), not into the first read (hypothesis 2).
  - `--accept multi` takes all 2000 in the ramp (400/s, ramp p99 44–67 ms, 100% achieved).
- **M3 shape**: rr p99 goes from ~4 ms (40 bulk) to ~32 ms (400 bulk) at default settings.

## Limitations

- All in one process on loopback: client threads share the machine (other CPUs) and the kernel's loopback path.
  Bulk throughput is bounded partly by client-side copies. No HTTP parsing, no TLS.
- The `--hop` worker has no real upstream socket or separate process. `--hop-nops` adds CQE-driven hops but no
  network I/O or upstream latency.
- `--rate` is shared by base and ramp connections, so a ramp lowers the per-connection rate of the base
  connections. Ramp connections' later requests (from their own schedule) go into the rr histogram and include
  the accept wait.
- Collection is 0.5 s after the window ends. Requests scheduled near the end that time out later are reported as
  `unfinished`, not `timeouts`. With `--timeout 10` and a 10 s window, only requests stuck since the first part of
  the window can be counted as timeouts. Use `scheduled`/`achieved` for the full picture.
- One outstanding request per rr connection. A stalled connection shows up as backlog and expired slots (counted
  as timeouts), not as parallel late requests.
- `ops_accept` counts accept SQEs. A multishot accept is armed once before the window, so it shows 0 with
  `--accept multi`. Use the server's `accepts/s` instead.
- The histogram is log-bucketed (1 µs to ~100 s, ≤ ~1.6% error, max exact). Percentiles are bucket midpoints.
- `--accept multi` needs Linux 5.19+. Client runtimes use the default builder with the timer.
- No scheduler stats yet: `TODO(merge): runtime_stats()` in `metrics_delta`.
