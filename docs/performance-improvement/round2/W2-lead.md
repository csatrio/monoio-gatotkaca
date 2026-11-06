# Wave 2: lead measurements (M5, M7, F3)

Date 2026-11-06. Fork at `r2-g2`. Bench worktree as in G0/G2. Machine quiet (no agent running) for every number
here. Binaries: `r2-bins/sut-monoio.perf1` / `perf2` (no `metrics`; `perf2` adds `--tune recvmulti|pollfirst`),
`sut-monoio.metrics2` / `metrics3` (diagnosis, with a per-second `[stats]` line that now also reports `cancels`,
`cancels_lost`, `sq_full`, `busy_retries` and `late_max_us`, the worst lateness of a 1 ms sleep, i.e. a loop stall
probe).

## M5: many connections, small frames

Cell: `--cells 1024:2000 --workloads repl --repl-conns 2000 --repl-sizes-kib 4 --repl-rate 150000` (and 16 KiB at
120,000), 3 rounds (`results/r2/w2-m5-4k`, `w2-m5-16k`).

| variant | 4 KiB MB/CPU-s | 4 KiB fr/s | 16 KiB MB/CPU-s | 16 KiB fr/s |
|---|---|---|---|---|
| tokio | 454.6 | 106.4k | 1,318 | 77.3k |
| monoio | 417.0 (−8.3%*) | 97.8k | 1,200 (−8.9%*) | 70.5k |
| `acceptbatch` | 418.1 (−8.0%*) | 98.2k | 1,196 (−9.3%*) | 70.3k |
| `acceptbatch:budget61:lifo` | 430.3 (−5.3%) | 100.9k | 1,209 (−8.3%*) | 71.1k |

Default monoio also has client timeouts here (the accept ramp, M1); `acceptbatch` removes them on 4 KiB. Repl p99 is
queueing delay (offered above capacity) and not comparable.

**Enters are not the cause** (`w2-m5-*-diag`, metrics build). With `acceptbatch`, each round is one waiting
`io_uring_enter` carrying ~2,000 SQEs (one recv + one send per frame), ~100 rounds/s, 0 CQ overflow, 0 SQ-full.
That is ~0.0005 enters per frame. ~92% of the loop's wall time is inside that enter: the kernel runs the inline
sends and recvs there. Tokio under `strace -c` (`w2-m5-4k-strace`, ratios only): `recvfrom` and `sendto` once per
frame (919 EAGAIN in 630k recvs), `epoll_wait` once per ~60 frames.

**Profile** (`perf record -g`, 8 s in the measured window, `r2-bins/perf/m5-{tokio,monoio}.data`):
- User space: monoio 9.8% of cycles in its binary vs tokio 4.7% + 1.8% libc.
- Monoio runtime symbols: `slab::Ref<MaybeFdLifecycle>::remove` 1.43%, `LocalScheduler::schedule` 0.83%,
  `task::raw::poll` 0.75%, `Op<Recv>::recv_with` 0.56%, `tick` 0.37%, `poll_op` 0.36%, `Slab::insert` 0.32%.
- Kernel, monoio only: `io_free_batch_list` → `fput` 1.13% (per-op file refcount; fixed files avoid it),
  `__io_submit_flush_completions` 1.40%. No poll arming at all: data is there when the recv is issued.

**Screens** (1 round, metrics build, `w2-m5-screen-diag`, `w2-m5-flat-diag`):
- `pollfirst`: 89.3k fr/s, worse (it forces a poll arm that the plain recv never needs here).
- `defer`: 96.9k, neutral.
- `recvmulti` on a shared **incremental** ring (1024 × 64 KiB): stalls. It climbs to ~80k fr/s, then connections
  stop receiving and the run decays to ~11k fr/s at 31% CPU. That is a runtime bug, assigned to W2-recvinc.
- `recvmulti` on a flat ring (4096 × 16 KiB, `R2_MRING_FLAT=1`): stable, 94.2k fr/s, 401 MB/CPU-s, i.e. no gain:
  the recv arm is not where the cost is.

**Next:** W2-m5 works on the per-op user-space cost (slot table, schedule, poll). The lead measures `fixedfd` on this
cell for the `fput` share.

## M7: occasional slow rounds: not reproduced

A/A cell of run 75 (`--cells 1024:2 --workloads stream,repl --repl-conns 8`, encrypted both), `monoio:tagA,tagB`,
5 rounds × 2 arms, twice: perf build (`w2-m7-perf`) and metrics build (`w2-m7-diag`).

- 20 rounds, repl p99 0.490–0.574 ms. No round near the 4 ms of run 75.
- Payload per round 1,490–1,570 MB/CPU-s (5.4% spread, under 8%).
- Metrics build: worst 1 ms-sleep lateness per run 1.45–2.57 ms (the timer wheel has 1 ms granularity, so this is
  one tick), `cq_overflow` 0, `cancels` 0, `busy_retries` 0 in every run.

Nothing in the runtime stalls on this cell on the current tree. Run 75's slow rounds were most likely harness or OS
noise (other load on the machine at the time). M7 is closed without a code change; the runner should keep
recording per-round driver stats so a recurrence can be checked.
