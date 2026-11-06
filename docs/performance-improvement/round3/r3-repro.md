# r3-repro: the `reqresp` reproducer (X1)

Branch `r3/r3-repro`, on `r3-base`. Owns `monoio/examples/reqresp/**` and this file. No file in `monoio/src/` and
no `Cargo.toml` was changed.

All numbers below are **screening, machine not quiet** (other agents were building; 3 s windows, one run each
unless stated).

## What it does

One process, loopback.

- **Server:** one monoio runtime (`enable_timer`) on its own thread, pinned to `--cpu`. Two listeners on
  ephemeral ports: request/response and bulk.
- **Clients:** one monoio runtime per CPU of `--client-cpus`. Connection `i` lives on thread `i % threads`.
- **Request/response load:** `--conns N` closed-loop connections. Each sends one frame and waits for the whole
  70-byte reply before it sends the next. Frame = 4-byte little-endian total length (header included) + payload.
  Sizes come from `--sizes`; connection `i` starts at index `i % len` and cycles.
- **Bulk load:** on each of `--bulk M` connections the server writes `--chunk` buffers in an endless `write_all`
  loop; the client reads into a 64 KiB buffer and discards.
- **Window:** the server waits until all `N + M` connections are accepted (fails after 60 s), sleeps `--warmup`,
  samples its counters, sleeps `--duration`, samples again. The clients record only inside the window (a shared
  atomic phase). After the report every thread parks and the main thread exits the process: no teardown of
  thousands of connections.
- **Receive buffer (plain, pipe):** a `Vec<u8>` of the largest frame + 4 KiB per connection, allocated once.
  A consumed frame is removed with `clear()` (or by moving the rest to the front if more than one frame is
  buffered), so the capacity never changes.

Build and run:

```sh
taskset -c 0,1,5,6,7,11 env CARGO_BUILD_JOBS=4 cargo build --release -p adaptif-gatotkaca-monoio --example reqresp
# stats build, in its own target directory so both binaries exist
taskset -c 0,1,5,6,7,11 env CARGO_BUILD_JOBS=4 cargo build --release -p adaptif-gatotkaca-monoio --example reqresp \
  --features metrics --target-dir target/metrics

flock /tmp/r3-measure.lock ./target/release/examples/reqresp --cpu 2 --client-cpus 3,4 --warmup 1 --duration 3 \
  --conns 8 --path pipe --ring defer,skipenter,ringfd
```

| option | default | meaning |
|---|---|---|
| `--cpu C` | 2 | server CPU |
| `--client-cpus a,b` | 3,4 | one client thread each |
| `--conns N` | 8 | request/response connections |
| `--sizes a,b,..` | 4k,16k,64k | frame sizes (`k`, `m` suffixes) |
| `--bulk M` | 0 | bulk connections |
| `--chunk SZ` | 32k | bulk write size |
| `--think US` | 0 | client pause between a reply and its next frame (an `IORING_OP_TIMEOUT`, so it does not block the client thread). Added to vary the client's turnaround, see "D1" below |
| `--path P` | pipe | `plain`, `pipe`, `mshot`; `eager` and `pair` are stubs (exit code 2) |
| `--deadline D` | none | `none`, `timeout` (`monoio::time::timeout(10 s, ..)` around every frame) |
| `--ring a,b,..` | (none) | `coop` (`coop_taskrun().taskrun_flag()`), `defer`, `skipenter`, `ringfd`, `fixedfd` (`with_fixed_files(32768)` + `register_fixed()` per accepted stream, both listeners), `minwait<us>` (`min_wait(us, 8)`); anything else goes to `ring_ext::apply`, unknown is an error (exit code 2) |
| `--entries N` | 4096 | server ring entries |
| `--cq N` | 2 × entries | `cq_size` |
| `--bufring N,SZ` | 4096,16k | path `mshot`: the ring |
| `--wset I,S` | | parsed and passed to `paths/mshot.rs`, which rejects it for now (exit code 2) |
| `--direct` | | parsed and passed to `accept.rs`, which rejects it for now (exit code 2) |
| `--warmup S` | 2 | seconds between the last accept and the window |
| `--duration S` | 6 | window |

### Output

Line 1 is readable. Line 2 is `RESULT` + `key=value` pairs:

- Labels: `path`, `ring` (`none` if empty), `deadline`, `conns`, `sizes`, `bulk`, `think_us`.
- `frames_per_s`: frames the server replied to in the window / window length.
- `frames_per_cpu_s`: the same / CPU time of the server thread (`getrusage(RUSAGE_THREAD)`).
- `p50_us`, `p90_us`, `p99_us`, `p999_us`, `mean_us`, `max_us`: reply latency measured by the clients, send start
  to reply complete. Per-thread log-linear histogram of nanoseconds (32 buckets per power of two, so a value is
  off by at most 1.6%), merged at the end; `max_us` is exact.
- `max_gap_ms`: for each connection the longest interval between two consecutive replies inside the window,
  maximum over all connections. The window's start and end count as replies, so a connection that got no reply
  shows the whole window.
- `conn_frames_min`, `conn_frames_max`: fewest and most replies of a single connection.
- `bulk_mb_s`: bytes the server wrote on bulk connections (10^6 bytes per second). `frame_mb_s`: request bytes.
- `user_instr_per_frame`, `user_cycles_per_frame`, `cycles_per_frame`: `perf_event_open` on the server thread
  (`na` if not allowed). With `--bulk` the bulk work is in these numbers too.
- `server_cpu`, `window_s`, `frames`, `client_frames`, `ramp_s` (time until all connections were accepted),
  `server_errors` (request/response connections that ended with an error, since the start; the first three are
  printed to stderr), `client_failed`, `enobufs` (path `mshot`: multishot receives ended by `ENOBUFS` in the
  window).

Line 3, only with `--features metrics`: `STATS` + `key=value` pairs, read on the server runtime thread.

- Both stats structs are reset at the start of the window, so every counter is a difference over the window and
  the high-water marks (`ready_queue_high_water`, `multishot_queue_high_water`) belong to it. `cq_overflow` is
  the kernel's counter, so it is printed as a difference.
- Every scalar field of `DriverStats` and `RuntimeStats` is printed under its own name. The names come from the
  structs' `Debug` output, so **fields added by r3-stats (`ops_completed`, `enter_left_taskrun`) appear without
  a change in the example**. A name both structs used would get the prefix `rt_` for the runtime's.
- Histograms as `a/b/c/..`: `cqes_per_tick` (0, 1, 2–3, 4–7, 8–15, 16–31, 32+), `polls_per_round`.
- Derived: `rounds_per_s`, `frames_per_round`, `task_polls_per_frame` (= `tasks_polled` / frames; includes the
  polls of bulk tasks when `--bulk` is set), `sqes_per_frame`, `cqes_per_frame`, `enters_per_frame`, `park_share`.
- `recv_ops`, `recv_same_tick`, `recv_next_tick`, `recv_later`: from `op_ticks_by_opcode[opcode::Recv::CODE]`,
  in percent: bucket 0, bucket 1, buckets 2 + 3. The same for `send_*`, and for `read_fixed_*` if such ops ran.

## Files and the interfaces later packages fill

| file | content |
|---|---|
| `main.rs` | arguments, server set-up, window, output |
| `client.rs` | client threads: closed-loop connections, latency histogram, gap tracking |
| `bulk.rs` | bulk writer (server) and reader (client) |
| `measure.rs` | thread CPU time, `perf_event_open` counters (code of `smallframes`), histogram, `RLIMIT_NOFILE` |
| `stats.rs` | the `STATS` line (`metrics` only) |
| `paths/mod.rs` | `Path`, `Config`, `Counters`, `Ctx`, dispatch, frame helpers |
| `paths/{plain,pipe,mshot}.rs` | the three paths |
| `paths/{eager,pair}.rs` | stubs |
| `ring_ext.rs` | stub for extra `--ring` items |
| `deadline.rs` | `Mode`, `Guard` |
| `accept.rs` | builder set-up for the accept mode, accept loop |

Spots for later packages are marked `=== r3-<package>: ... ===` in the code.

### A path (`paths/eager.rs` for r3-eager, `paths/pair.rs` for r3-pair)

Replace the two functions of the stub. Nothing else needs a change: `--path eager|pair` is already parsed and
dispatched in `paths/mod.rs`.

```rust
/// Once, on the server runtime, before the listeners accept.
pub fn setup(ctx: &Ctx) -> io::Result<()>;
/// One connection, until the peer closes (`Ok`) or something fails (`Err`).
pub async fn serve(stream: TcpStream, ctx: &Ctx) -> io::Result<()>;
```

What `paths/mod.rs` offers:

```rust
pub const HEADER: usize = 4;
pub const REPLY_LEN: usize = 70;
pub static REPLY: [u8; REPLY_LEN];            // send `&REPLY[..]` (`&'static [u8]` is `IoBuf`)

pub struct Config { pub path: Path, pub deadline: deadline::Mode, pub max_frame: usize,
                    pub bufring: (u16, usize), pub wset: Option<(u16, u16)> }
pub struct Ctx { pub cfg: Config, pub count: Counters, /* path state */ }
impl Ctx {
    pub fn frame_done(&self, bytes: usize);   // once per frame, after the reply was sent
    pub fn put_state<T: 'static>(&self, state: T);   // from setup(): runtime-wide state of the path
    pub fn state<T: 'static>(&self) -> &T;
}
pub fn new_buf(ctx: &Ctx) -> Vec<u8>;                                   // receive buffer, largest frame + slack
pub fn frame_len(buf: &[u8], max: usize) -> io::Result<Option<usize>>;  // Some(total) when the frame is whole
pub fn check_len(total: u32, max: usize) -> io::Result<usize>;
pub fn consume(buf: &mut Vec<u8>, n: usize);                            // drop one frame, keep the capacity
pub fn eof(mid_frame: bool) -> io::Result<bool>;                        // Ok(false) between frames, else an error
```

The pattern of a path (see `pipe.rs`, 86 lines): keep the connection's receive state in a struct outside the
per-frame future, write `async fn frame(..) -> io::Result<bool>` (`false` = peer closed between frames), and
drive it with

```rust
let mut guard = Guard::new(ctx.cfg.deadline);
while guard.frame(frame(&stream, &mut rx, ctx)).await? {}
```

For r3-pair, `pipe.rs` is the thing to copy: the `send_op` + `recv_poll_first_op` pair is the one place to
replace with `send_then_recv`.

### `--wset` (`paths/mshot.rs`, r3-bufring)

`--wset INITIAL,STEP` is parsed in `main.rs` into `ctx.cfg.wset: Option<(u16, u16)>`. `mshot::setup` rejects it
at the marked spot; build the ring with the working-set option there instead. `mshot::serve` re-arms after
`ENOBUFS` in one place (`yield_now`, then `recv_multi` again), which is where `next_rearm` goes.

### A ring item (`ring_ext.rs`, r3-drain)

```rust
pub fn apply(rb: RuntimeBuilder<IoUringDriver>, item: &str)
    -> Result<RuntimeBuilder<IoUringDriver>, RuntimeBuilder<IoUringDriver>>;   // Err = unknown item
```

`main.rs` (`server_builder`) calls it for every `--ring` item that is not `coop`, `defer`, `skipenter`, `ringfd`,
`fixedfd` or `minwait<us>`, in command-line order, before `accept::configure` and `enable_timer()`. A commented
`drain<N>` sketch is at the marked spot.

### A deadline mode (`deadline.rs`, r3-timer)

```rust
pub enum Mode { None, Timeout }                 // add the variant
impl Mode { pub fn parse(s: &str) -> Option<Self>; pub fn name(self) -> &'static str; }
pub struct Guard { mode: Mode }                 // add the connection's `Deadline` here
impl Guard {
    pub fn new(mode: Mode) -> Self;             // once per connection
    pub async fn frame<T>(&mut self, frame: impl Future<Output = io::Result<T>>) -> io::Result<T>;
}
```

Every path calls `Guard::frame` around the work for one frame, so a new mode needs no change in the paths. On
expiry `frame` is dropped and the connection ends with `TimedOut`. `--deadline` is parsed by `Mode::parse`.

### An accept mode (`accept.rs`, r3-direct)

```rust
pub struct Opts { pub fixedfd: bool, pub direct: bool }      // Copy
pub fn configure(rb: RuntimeBuilder<IoUringDriver>, opts: &Opts) -> RuntimeBuilder<IoUringDriver>;
pub async fn run(listener: TcpListener, opts: Opts, on_conn: impl FnMut(TcpStream));
```

`--direct` is parsed in `main.rs` into `Opts::direct`; `configure` rejects it at the marked spot. `configure`
is where the builder gets its fixed-file table (today `with_fixed_files(32768)` for `fixedfd`); `run` is the
accept loop of both listeners (`accept_batch_into`, `set_nodelay`, `register_fixed`, then `on_conn`, which
spawns the connection's task). `main.rs` raises `RLIMIT_NOFILE` to cover `accept::FIXED_FILES` when `fixedfd` is
set (the kernel refuses a table larger than the limit).

## Sample output

Default build, `--cpu 2 --client-cpus 3,4 --warmup 1 --duration 3`, `--ring defer,skipenter,ringfd`:

```
# --conns 8 --path pipe
RESULT path=pipe ring=defer,skipenter,ringfd deadline=none conns=8 sizes=4096,16384,65536 bulk=0 think_us=0 frames_per_s=98127 frames_per_cpu_s=98148 p50_us=74.8 p99_us=161.8 max_us=1928.9 max_gap_ms=1.93 bulk_mb_s=0.0 user_instr_per_frame=2083 user_cycles_per_frame=1474 cycles_per_frame=43676 server_cpu=1.000 window_s=3.001 frames=294484 frame_mb_s=2813.5 client_frames=294484 p90_us=126.0 p999_us=178.2 mean_us=81.5 conn_frames_min=35599 conn_frames_max=38312 ramp_s=0.002 server_errors=0 client_failed=0 enobufs=0
# --conns 2000 --sizes 4k --path pipe
RESULT path=pipe ring=defer,skipenter,ringfd deadline=none conns=2000 sizes=4096 bulk=0 think_us=0 frames_per_s=112615 frames_per_cpu_s=112637 p50_us=17563.6 p99_us=18087.9 max_us=18605.2 max_gap_ms=18.61 bulk_mb_s=0.0 user_instr_per_frame=2342 user_cycles_per_frame=2205 cycles_per_frame=38057 server_cpu=1.000 window_s=3.001 frames=337966 frame_mb_s=461.3 client_frames=337966 p90_us=18087.9 p999_us=18605.2 mean_us=17759.5 conn_frames_min=168 conn_frames_max=169 ramp_s=0.135 server_errors=0 client_failed=0 enobufs=0
# --conns 9 --bulk 2 --path pipe
RESULT path=pipe ring=defer,skipenter,ringfd deadline=none conns=9 sizes=4096,16384,65536 bulk=2 think_us=0 frames_per_s=79228 frames_per_cpu_s=79241 p50_us=105.5 p99_us=223.2 max_us=471.2 max_gap_ms=0.47 bulk_mb_s=636.9 user_instr_per_frame=2311 user_cycles_per_frame=1660 cycles_per_frame=54125 server_cpu=1.000 window_s=3.001 frames=237767 frame_mb_s=2271.6 client_frames=237767 p90_us=165.9 p999_us=239.6 mean_us=113.5 conn_frames_min=22196 conn_frames_max=28722 ramp_s=0.005 server_errors=0 client_failed=0 enobufs=0
# --conns 8 --path plain
RESULT path=plain ring=defer,skipenter,ringfd deadline=none conns=8 sizes=4096,16384,65536 bulk=0 think_us=0 frames_per_s=100491 frames_per_cpu_s=100503 p50_us=76.8 p99_us=123.9 max_us=244.6 max_gap_ms=0.24 bulk_mb_s=0.0 user_instr_per_frame=2616 user_cycles_per_frame=1768 cycles_per_frame=42656 server_cpu=1.000 window_s=3.001 frames=301573 frame_mb_s=2881.3 client_frames=301573 p90_us=103.4 p999_us=145.4 mean_us=79.6 conn_frames_min=37677 conn_frames_max=37714 ramp_s=0.002 server_errors=0 client_failed=0 enobufs=0
# --conns 8 --path mshot
RESULT path=mshot ring=defer,skipenter,ringfd deadline=none conns=8 sizes=4096,16384,65536 bulk=0 think_us=0 frames_per_s=76301 frames_per_cpu_s=76323 p50_us=99.3 p99_us=210.9 max_us=2555.5 max_gap_ms=2.56 bulk_mb_s=0.0 user_instr_per_frame=2687 user_cycles_per_frame=1950 cycles_per_frame=56164 server_cpu=1.000 window_s=3.001 frames=228972 frame_mb_s=2187.7 client_frames=228972 p90_us=141.3 p999_us=243.7 mean_us=104.8 conn_frames_min=28274 conn_frames_max=29022 ramp_s=0.002 server_errors=0 client_failed=0 enobufs=0
```

`metrics` build, `--conns 8 --path pipe --ring defer,skipenter,ringfd` (the `RESULT` line is as above):

```
STATS enter_submit=0 enter_wait=42542 enter_skipped=0 sqes_submitted=585594 cqes_reaped=585595 cqes_per_tick=0/17/17/32/40209/2267/0 sq_full_flushes=0 busy_retries=0 cancels_pushed=0 cancels_lost=0 cq_overflow=0 multishot_cqes=0 multishot_queue_high_water=0 chains_submitted=0 link_timeouts=0 zc_sends=0 zc_notifs=0 cancel_fd_pushed=0 ignored_inline=0 ignored_boxed=0 rounds=42542 submits=0 parks=42542 park_ns=2926629811 busy_ns=74328624 tasks_polled=339986 polls_per_round=0/17/17/95/42413/0/0/0/0/0/0 budget_exhausted_rounds=0 ready_queue_len=8 ready_queue_high_water=8 tasks_spawned=0 tasks_completed=0 remote_wakes=0 coop_yields=0 lifo_polls=0 prio_polls=0 rounds_per_s=14176 frames_per_round=6.861 task_polls_per_frame=1.165 sqes_per_frame=2.006 cqes_per_frame=2.006 enters_per_frame=0.146 park_share=0.975 recv_ops=293699 recv_same_tick=84.2 recv_next_tick=15.7 recv_later=0.1 send_ops=291896 send_same_tick=100.0 send_next_tick=0.0 send_later=0.0
```

Other runs made once as functional checks (not baselines): `--deadline timeout` with `pipe` and `plain`;
`--ring ..,fixedfd`; `--ring ..,minwait80`; `--path mshot --conns 200 --sizes 4k --bufring 64,4k` (a ring
smaller than the number of connections: 143,557 frames/s, `enobufs=13`, no error, so the re-arm after `ENOBUFS`
works). `--path eager`, `--path pair`, `--direct`, `--wset 16,16` print "not available in this build" and exit
with code 2; `--ring defer,drain4` and `--deadline deadline` are usage errors (code 2).

`RLIMIT_NOFILE`: raising it was **not necessary** here (soft limit 1,048,576). The code is in place
(`measure::raise_nofile`): it raises the soft limit to `2 × (conns + bulk) + 64` (and above the fixed-file table
with `fixedfd`), prints one line to stderr when it does, and fails with a message if the hard limit is lower.

## D1 (`--conns 9 --bulk 2 --path pipe`, `coop` against `defer`): what I see

`metrics` build, `--ring <coop|defer>,skipenter,ringfd`, two runs each:

| ring | frames/s | bulk MB/s | rounds/s | frames per round | `recv_same_tick` / `recv_next_tick` | polls per frame |
|---|---|---|---|---|---|---|
| `coop` | 79,213 / 79,234 | 598 / 591 | 9,124 / 9,023 | 8.68 / 8.78 | 96.8 / 3.2 and 97.8 / 2.2 | 1.27 / 1.25 |
| `defer` | 78,614 / 78,763 | 634 / 627 | 9,667 / 9,570 | 8.13 / 8.23 | 89.9 / 10.0 and 91.0 / 9.0 | 1.35 / 1.34 |

- **The 96% / 52% split of ROUND3.md is not reproduced.** The direction is: `coop` 97%, `defer` 90%; `defer`
  runs more rounds with fewer frames each and moves share to the bulk writers (+6% MB/s). But frames/s differ by
  0.7%, not by 16%.
- Varying the client's turnaround with `--think` (one run each) lowers both shares and does not open the gap:

  | `--think` | `coop`: frames/s, same / next tick | `defer`: frames/s, same / next tick |
  |---|---|---|
  | 0 | 79,213, 96.8 / 3.2 | 78,614, 89.9 / 10.0 |
  | 10 µs | 71,456, 50.4 / 49.4 | 70,581, 33.5 / 66.3 |
  | 30 µs | 65,203, 3.7 / 72.9 | 64,369, 2.8 / 63.7 |

- One client thread (`--client-cpus 3`) makes the client the bottleneck (56–57k frames/s, 1.6 GB/s bulk, about
  1.3–1.6 frames per round with either ring): not the bench's regime.
- What differs from the bench's MIX load, as far as I can tell from ROUND3.md (I did not read the bench repo):
  - The bench server encrypts each 32 KiB chunk in user space. Its rounds take 135–165 µs for 6–9 frames; here a
    round takes about 105–110 µs and nearly all of it is inside the enter (`park_share` 0.98). **Hypothesis,
    not tested:** user-space work per bulk chunk (a `--bulk-work` knob in `bulk.rs`) is the missing part, since
    it lengthens the time between two enters without lengthening the enter.
  - The bench's clients are other programs (HTTP requests, three `wrk`-style threads, stream clients that
    decrypt). Their turnaround and whether a request arrives in one piece are unknown to me.

## Other observations (screening)

- **R8 and R2000 land near the bench's `defer` rows of ROUND3.md:**
  - 8 connections: 98,127 frames/s, p50 74.8 µs, p99 161.8 µs. Bench (`$MANY`, run 117): 98,525 frames/s,
    median 66–72 µs, p99 140 µs.
  - 2000 × 4 KiB: 112,615 frames/s, p50 17.6 ms, p99 18.1 ms. Bench: 106,431, 18.6 ms, 19.4 ms with
    `defer…pipeack:sweep` (run 111, 1 round); 111,841–114,816 with `mshot` added (runs 112, 118).
- **D2 (`--conns 2000 --sizes 4k --path pipe`, `metrics` build, one run each):**

  | ring | frames/s | p50 / p99 | `max_gap_ms` | rounds/s | frames per round | `cqes_per_tick` | `recv_same_tick` | polls per frame | `sq_full_flushes` |
  |---|---|---|---|---|---|---|---|---|---|
  | `coop,skipenter,ringfd` | 100,180 | 20.2 / 20.7 ms | 20.7 | 50 | 2000.0 | all in `32+` (4000 per tick) | 100.0 | 1.000 | 0 |
  | `defer,skipenter,ringfd` | 113,468 | 17.6 / 18.1 ms | 19.0 | 2,836 | 40.0 | all in `32+` | 0.0 (all in `recv_later`) | 2.000 | 0 |

  - With `coop` the loop locks into **one enter per 20 ms that carries all 2000 frames** (151 enters in 3 s):
    −11.7% frames/s against `defer` (bench: 97,750 against 106,431, −8.2%). The 1.0–1.8 s p99 of the bench did
    not show in this 3 s run (`max_gap_ms` 20.7, every connection got exactly 151 replies).
  - That round pushes 4000 SQEs into a ring of 4096 entries, so it is close to the SQ limit (finding 3 of the
    plan) without reaching it here.
  - With `defer` a round carries exactly 40.0 frames, which fits KV1 (a bounded number of deferred task-work
    items per run); every receive is reaped two or more ticks after its submit.
- **`mshot` at 8 connections is the slowest path** (76,301 frames/s here, 63,016 in an earlier run, against
  98,127 for `pipe`) with the default 4096 × 16 KiB ring, and the fastest thing I ran with a small hot ring
  (`--conns 200 --sizes 4k --bufring 64,4k`: 143,557 frames/s). That is B1's subject.
- Run-to-run spread on this busy machine is large: `plain` at 8 connections gave 89,866 and 100,491 frames/s in
  two runs of the same command.

## Checks

- `cargo clippy -p adaptif-gatotkaca-monoio --examples` and the same with `--features metrics`: no warning from
  `examples/reqresp` (the three warnings shown are in the library and were there before).
- `cargo fmt` applied.
- Not run: `cargo test` (the package adds no test and changes no library code).

## Open questions

1. D1 does not reproduce at the bench's size (above). G0 item 2 needs a decision: add user-space work per bulk
   chunk, or accept the smaller split.
2. `task_polls_per_frame` counts all task polls, bulk tasks included. With `--bulk` it is not "polls of
   request/response tasks per frame". `ops_completed` from r3-stats will give the cleaner ratio.
3. The latency of a frame is taken when its reply completes inside the window, even if it was sent before the
   window began (at most one frame per connection).

## Requested changes

None in `monoio/src/` or `monoio/Cargo.toml`.

Notes for the lead:

- The task text names `MsgFlags::empty()`; the API is `MsgFlags::EMPTY` (used in `paths/pipe.rs`).
- "RecvMulti separately" in the stats is not possible: `opcode::RecvMulti::CODE == opcode::Recv::CODE`
  (`IORING_OP_RECV`), so multishot receives are counted in the `recv_*` values, and only their first completion.
  On path `mshot` those values describe the receives armed in the window, not frames.
- `target/metrics/` is a second target directory inside the worktree (ignored by git like `target/`).
