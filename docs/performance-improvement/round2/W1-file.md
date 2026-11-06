# W1-file: direct-open files, punt visibility, splice guidance (F5, M6)

Branch `r2/W1-file` from `r2-g0`. Kernel 7.0.0-42, `/` ext4, `/home` f2fs (the bench and this repo live on f2fs).
The measurements below are short functional probes on a shared machine (other wave-1 agents were building), not
benchmarks: the inline/punt split is a property of the kernel path and doesn't depend on load; the µs/op column does
(screening, machine not quiet).

## What changed

- `OpenOptions::open_direct(path) -> io::Result<File>` (`fs/open_options.rs`, `driver/op/open.rs`):
  `IORING_OP_OPENAT` with `file_index`, returning a direct-only `File` (no fd), same design as
  `TcpListener::accept_direct`:
  - The slot comes from the runtime allocator (`SlotRelease::reserved`: cleared and freed if the open fails, or if
    its future is dropped and the open completes anyway).
  - If no runtime slot is free and the table has a kernel range (`with_fixed_files_kernel_alloc`), it uses
    `IORING_FILE_INDEX_ALLOC`. That path is submitted as a one-CQE `MultishotOp` (`OpenDirectAlloc`,
    `KIND = Multishot`), so a dropped open whose CQE carries a kernel slot is cleaned up by `discard_cqe`
    (`update_sync(slot, -1)`). Single-shot ops have no hook that sees the result of a dropped op.
  - `O_CLOEXEC` is removed, because the kernel rejects it with `file_index`.
  - No table: `Unsupported`. Full table: `ENFILE`. A slot is freed when the close completes.
- `File::is_direct()`, `File::fixed_slot()`. `as_raw_fd()` returns -1 on a direct file. `metadata()` returns
  `Unsupported` on a direct file: io_uring's statx prep rejects `REQ_F_FIXED_FILE` (`EBADF`), and statx resolves a
  path relative to a dirfd. There is no fixed-file form.
- Existing ops already build SQEs with `sqe_fd!` (`types::Fixed` for slots). `read_at`, `read_exact_at`,
  `read_fixed_at`, `write_at`, `fadvise`, `sync_*`, splice and `read_at_then_send` were verified on direct files
  (`tests/fs_direct.rs`). Close and drop already went through `Close::fixed` (`IORING_OP_CLOSE` with
  `file_index`).
- `FileTable::has_free_slot()` (`driver/fixed_files.rs`): a check that doesn't bump `exhausted`.
- Lifecycle side of the punt metric (`driver/uring/lifecycle.rs`, everything `#[cfg(feature = "metrics")]`):
  - `MaybeFdLifecycle` gets `opcode: u8` and `submit_tick: u32`. The `lifecycle_size` test (≤ 72 B) passes with
    `metrics`.
  - New: `set_submit_tick`, `take_op_ticks`, `op_ticks_bucket`, `OP_TICK_BUCKETS`, plus a unit test.
  - They carry `#[allow(dead_code)]` until the driver hook below lands. Remove the allows then.
- Docs:
  - `splice_to_pipe`/`splice_from_pipe`: always punted, use for large cold files.
  - `fadvise`: which advice is inline, and readahead.
  - `File::close`: fd close vs slot close.
  - `File::metadata`: statx is always punted.
  - AGENTS.md: "Fixed files" and "File ops".
- Tests:
  - `tests/fs_direct.rs` (10 tests): no table, read/write/fadvise/sync/metadata/close, drop frees the slot, errors
    release the slot, a full table, kernel-alloc fallback, drop in flight (both slot kinds), `read_fixed_at`,
    `read_at_then_send`, splice (feature `splice`).
  - `tests/fs_punt_probe.rs`: the measurement, `#[ignore]`.

## Punt table (item 2)

**Method.** `perf stat -e io_uring:io_uring_queue_async_work` is denied for this user (`perf_event_paranoid=1`, and
`/sys/kernel/tracing/events/io_uring/*/id` is root-only), so the probe measures directly instead
(`tests/fs_punt_probe.rs`, about 1 s per filesystem):
- Each op runs on a raw `IoUring` of a fresh thread, one SQE at a time, submitted with a submit-only
  `io_uring_enter`.
- **inline**: the CQE is in the CQ when that enter returns, so the kernel ran the op in the submitting thread.
- **wrk**: `iou-wrk-<tid>` threads of the probing thread after the loop (io-wq is per task, so each row starts at 0).
  **cs**: their voluntary context switches, about one per work item.
- An op that isn't inline but has wrk = 0 completed from task work (an async buffered read), not from a punt.

```sh
cargo test -p adaptif-gatotkaca-monoio --release --test fs_punt_probe -- --ignored --nocapture   # $TMPDIR (ext4 here)
PROBE_DIR=<dir on f2fs> cargo test -p adaptif-gatotkaca-monoio --release --test fs_punt_probe -- --ignored --nocapture
```

| op | iters | f2fs inline | f2fs wrk / cs | ext4 inline | ext4 wrk / cs | µs/op f2fs |
|---|---|---|---|---|---|---|
| `openat` cached path, `O_RDONLY` | 10k | 100% | 0 / 0 | 100% | 0 / 0 | 3.1 |
| **`close` (fd)** | 10k | **0%** | **1 / 9,997** | 100% | 0 / 0 | **10.9** |
| `openat` direct (`file_index`) | 10k | 100% | 0 / 0 | 100% | 0 / 0 | 3.3 |
| **`close` direct (slot)** | 10k | **100%** | 0 / 0 | 100% | 0 / 0 | **3.8** |
| `read` 128 KiB warm (fd) | 10k | 100% | 0 / 0 | 100% | 0 / 0 | 11.9 |
| `read` 128 KiB warm (slot) | 10k | 100% | 0 / 0 | 100% | 0 / 0 | 11.9 |
| `read` 128 KiB cold (`DONTNEED` before each) | 500 | 0% | 0 / 0 | 0% | 0 / 0 | 203 |
| `fadvise` SEQUENTIAL | 10k | 100% | 0 / 0 | 100% | 0 / 0 | 0.4 |
| `fadvise` WILLNEED | 10k | 0% | 1 / 9,999 | 0% | 1 / 10,000 | 11.2 |
| `splice` file→pipe 64 KiB warm | 2k | 0% | 1 / 2,001 | 0% | 1 / 2,002 | 14.8 |
| `splice` … `SPLICE_F_NONBLOCK` | 2k | 0% | 1 / 2,000 | 0% | 1 / 2,000 | 17.3 |
| `statx` (path) | 10k | 0% | 1 / 9,998 | 0% | 1 / 10,000 | 10.0 |

Cold cache works without root. `posix_fadvise(DONTNEED)` after `fdatasync` evicts your own clean file: `mincore`
showed 100% resident before and 0% after. So the cold row is a real device read.

**Reading:**
- **U3 confirmed and explained.** On f2fs a close by fd always punts.
  - `io_close` returns `-EAGAIN` under `IO_URING_F_NONBLOCK` when `file->f_op->flush` is set, and f2fs has
    `f2fs_file_flush`. ext4 has no `->flush`, so it closes inline.
  - A direct close is `io_close_fixed`, a table removal: inline on both filesystems. The final `fput` doesn't call
    `->flush` (only `filp_close` does).
  - So `open_direct` + `read_at` + `close` is fully inline for a warm file on f2fs, against one punt per request
    today.
- Open (cached path) and warm reads are inline either way.
- Cold reads aren't punted either. They complete via task work (an async buffered read, `FMODE_BUF_RASYNC`) with no
  io-wq worker. A cold cache costs device latency, but no thread.
- `statx` is always punted. So `File::metadata()` and `monoio::fs::read` (which calls `metadata()` for the size)
  punt on every call. Worth checking whether the SUT's stream path uses either.
- Not measured: `O_CREAT`/`O_TRUNC` opens and paths not in the dentry cache. Both punt by design: `io_openat2`
  returns `-EAGAIN` under nonblock for `O_CREAT|O_TRUNC|__O_TMPFILE`, and on a `LOOKUP_CACHED` miss.

### Through the runtime (tick metric, with the driver hook applied locally)

32 tasks × 200 × (open, `read_at` 128 KiB, `close`), one runtime with `with_fixed_files(256)`, release build with
`metrics`. `op_ticks_by_opcode` buckets `[0, 1, 2–3, 4+]`:

| f2fs | OPENAT | READ | CLOSE |
|---|---|---|---|
| `open` (fd) | [6400, 0, 0, 0] | [6400, 0, 0, 0] | **[19, 6364, 17, 0]** |
| `open_direct` | [6400, 0, 0, 0] | [6400, 0, 0, 0] | [6400, 0, 0, 0] |

On ext4 every bucket was `[6400, 0, 0, 0]`, fd closes included. The metric separates the punted close cleanly.
Screening, machine not quiet.

## M6: does `IORING_OP_SPLICE` ever run inline? (item 4)

**No.** There is no kernel source on this machine (`/usr/src/linux-hwe-7.0-headers-*` holds only headers and
Kconfig, no `.c`), so this is from my knowledge of `io_uring/splice.c`:
- In current kernels `__io_splice_prep` sets `req->flags |= REQ_F_FORCE_ASYNC` for both `SPLICE` and `TEE`, and
  `io_splice` has `WARN_ON_ONCE(issue_flags & IO_URING_F_NONBLOCK)`: the op is never issued non-blocking.
- The early versions (5.7) returned `-EAGAIN` under `force_nonblock` instead, with the same effect.
- `SPLICE_F_NONBLOCK` only makes pipe waits non-blocking. It doesn't change the issue path.

Confidence: high on the mechanism, medium on the exact line. The measurement agrees: 100% punted with a warm cache,
with and without `SPLICE_F_NONBLOCK`, on ext4 and f2fs, about one worker switch per splice. There is no inline
option to add.

Documented in `splice_to_pipe` and AGENTS.md: splice is for large cold files. For cached files use
`read_fixed_at` + `send_fixed`, or `read_at_then_send`, both inline for warm data.

Cost model for M6:
- One io-wq hand-off per pipe-full (64 KiB by default, so 16 punts per 1 MiB file).
- With 64 connections, io-wq grows to many workers, which matches the 16 threads and the p99 tail in ROUND2.
- A larger pipe (`F_SETPIPE_SZ`) cuts the punts per file, but not the per-punt latency.

## fadvise (item 5)

- `NORMAL`/`SEQUENTIAL`/`RANDOM` are inline (0.4 µs). They only change the open file's readahead state, so they
  must be repeated after every open.
  - `SEQUENTIAL` doubles `ra_pages`. Here `read_ahead_kb` is 128 (nvme0n1), so the window grows to 256 KiB.
  - On a cold, sequential read in 128 KiB chunks, the first `read` then reads ahead past the requested chunk, and
    later chunks hit pages already in flight.
  - On a cached file it does nothing.
  - Worth trying in G2 cold-cache runs: it costs next to nothing.
- `WILLNEED`/`DONTNEED`/`NOREUSE` are always punted (the kernel forces these advices async; measured 100%, one
  worker switch each).
  - `WILLNEED` starts readahead of the range in the worker.
  - It pays off only when issued well before the read. Before each read of a cached file it is pure overhead (an
    io-wq hand-off, about 11 µs here).
- Measuring the readahead effect on cold 128 KiB reads is for G2 (the SUT needs a cold-cache step). Not done here.

## Changes requested in files I don't own

### `driver/uring/mod.rs`: the `op_ticks_by_opcode` hook (tested)

Apply this diff, then remove the six `#[allow(dead_code)]` lines on the metrics items in `lifecycle.rs`.

I applied it locally and the full test suite passed with `--features metrics,splice` (`driver_stats.rs` and the
size asserts included); then I reverted it. What it does:
- `tick()`: bumps a `u32` counter at the start, and looks up each op CQE's slot before `ops.complete`.
- `push_op`, `push_chain`, `push_op_with_timeout`: record the tick inside the existing `metric!` blocks, next to
  `ops_by_opcode`.
- `DriverStats` gains `op_ticks_by_opcode: [[u64; 4]; 64]`, so `DRIVER_STATS_U64S` grows by 256 and `DriverStats`
  from 720 to 2,768 bytes. It is still `Copy`, and copied rarely.

Without `metrics` nothing changes. With it, each push and each op CQE costs one extra slab lookup, a load and a
store into memory the driver touches anyway.

```diff
diff --git a/monoio/src/driver/uring/mod.rs b/monoio/src/driver/uring/mod.rs
--- a/monoio/src/driver/uring/mod.rs
+++ b/monoio/src/driver/uring/mod.rs
@@ -137,11 +137,25 @@ pub struct DriverStats {
     pub ignored_inline: u64,
     /// In-flight ops dropped whose data was too large for the slot and was boxed.
     pub ignored_boxed: u64,
+    /// Completion latency in driver ticks, per SQE opcode (indexed like `ops_by_opcode`), in
+    /// buckets: `[0]` reaped by the first tick after the enter that submitted the SQE, `[1]`
+    /// one tick later, `[2]` 2-3 ticks later, `[3]` 4 or more. Counts the first CQE of each op
+    /// pushed by the driver (`ops_by_opcode`'s ops; for zero-copy sends the send result, for
+    /// multishot ops the first CQE). No clock is read.
+    ///
+    /// An op the kernel completes inline (during `io_uring_enter`) lands in `[0]`. An op punted
+    /// to an io-wq worker, or waiting for a device or the network, usually lands in `[1]` or
+    /// later *when the loop keeps ticking meanwhile* (other tasks ready). A loop that parks
+    /// right after the submit waits in that same enter, so a slow op also lands in `[0]` then:
+    /// compare opcodes under load, not in an idle loop.
+    pub op_ticks_by_opcode: [[u64; lifecycle::OP_TICK_BUCKETS]; 64],
 }
 
 /// `DriverStats` is all `u64`s: this many. Checked against the struct in metrics builds, so the
 /// `metrics_off` test can tell without the type that the driver holds no counters.
-const DRIVER_STATS_U64S: usize = 19 + 7 + 64;
+const DRIVER_STATS_U64S: usize = 19 + 7 + 64 + 4 * 64;
+#[cfg(feature = "metrics")]
+const _: () = assert!(lifecycle::OP_TICK_BUCKETS == 4);
 #[cfg(feature = "metrics")]
 const _: () = assert!(std::mem::size_of::<DriverStats>() == DRIVER_STATS_U64S * 8);
 
@@ -181,6 +195,7 @@ impl Default for DriverStats {
             cancel_fd_pushed: 0,
             ignored_inline: 0,
             ignored_boxed: 0,
+            op_ticks_by_opcode: [[0; lifecycle::OP_TICK_BUCKETS]; 64],
         }
     }
 }
@@ -339,6 +354,10 @@ pub(crate) struct UringInner {
     #[cfg(feature = "metrics")]
     stats: DriverStats,
 
+    /// Ticks so far (wrapping), for `DriverStats::op_ticks_by_opcode`.
+    #[cfg(feature = "metrics")]
+    tick: u32,
+
     /// Finished slots of dropped ops, dropped by [`drop_deferred`] once the driver holds no
     /// state: dropping op data may call back into the driver.
     deferred: Vec<MaybeFdLifecycle>,
@@ -374,6 +393,8 @@ impl IoUringDriver {
             sqes_consumed: 0,
             #[cfg(feature = "metrics")]
             stats: DriverStats::default(),
+            #[cfg(feature = "metrics")]
+            tick: 0,
             deferred: Vec::new(),
             doorbells: Default::default(),
         }));
@@ -418,6 +439,8 @@ impl IoUringDriver {
             sqes_consumed: 0,
             #[cfg(feature = "metrics")]
             stats: DriverStats::default(),
+            #[cfg(feature = "metrics")]
+            tick: 0,
             deferred: Vec::new(),
             doorbells: Default::default(),
         }));
@@ -658,6 +681,7 @@ impl UringInner {
     }
 
     fn tick(&mut self) -> io::Result<()> {
+        metric!(self.tick = self.tick.wrapping_add(1));
         let cq = self.uring.completion();
 
         #[cfg(feature = "metrics")]
@@ -673,6 +697,11 @@ impl UringInner {
                     metric!(if cqe.flags() & CQE_F_NOTIF != 0 {
                         self.stats.zc_notifs += 1;
                     });
+                    metric!(if let Some(mut slot) = self.ops.slab.get(index as _) {
+                        if let Some((op, bucket)) = slot.take_op_ticks(self.tick) {
+                            self.stats.op_ticks_by_opcode[op][bucket] += 1;
+                        }
+                    });
                     // SAFETY: the CQE comes from the kernel for the op at `index`.
                     let done = unsafe { self.ops.complete(index as _, cqe.result(), cqe.flags()) };
                     metric!(if let Some(queued) = done.queued {
@@ -915,6 +944,9 @@ impl UringInner {
             if T::KIND == OpKind::ZeroCopy {
                 self.stats.zc_sends += 1;
             }
+            if let Some(mut slot) = self.ops.slab.get(index) {
+                slot.set_submit_tick(sqe.get_opcode(), self.tick);
+            }
         );
 
         // The SQE is visible to the kernel once the tail is published. We don't enter now: the
@@ -945,8 +977,11 @@ impl UringInner {
             return Err(io::Error::from_raw_os_error(libc::EBUSY));
         }
         metric!(
-            for sqe in &sqes {
+            for (sqe, index) in sqes.iter().zip(slots) {
                 self.stats.ops_by_opcode[(sqe.get_opcode() as usize).min(63)] += 1;
+                if let Some(mut slot) = self.ops.slab.get(index) {
+                    slot.set_submit_tick(sqe.get_opcode(), self.tick);
+                }
             }
             self.stats.chains_submitted += 1;
         );
@@ -985,7 +1020,12 @@ impl UringInner {
             }
             return Err(io::Error::from_raw_os_error(libc::EBUSY));
         }
-        metric!(self.stats.ops_by_opcode[opcode] += 1);
+        metric!(
+            self.stats.ops_by_opcode[opcode] += 1;
+            if let Some(mut slot) = self.ops.slab.get(index) {
+                slot.set_submit_tick(opcode as u32, self.tick);
+            }
+        );
         Ok(index)
     }
```

Line numbers are against `r2-g0`. W1-sched also edits `submit`/`push` paths, so expect small offsets.

**How to read it.**
- `op_ticks_by_opcode[opcode]` is `[first tick after the submitting enter, +1, +2–3, +4 or more]`, counted on the
  first CQE of each op.
- An inline op (open with a cached path, warm read, slot close) is always in `[0]`.
- A punted op (fd close on f2fs, splice, statx, fadvise WILLNEED) is in `[1]` or later *if the loop ticks while the
  worker runs*, that is, under load, with other tasks ready.
- If the loop parks right after the submit, one enter both submits and waits, so a punted op comes back in that
  same tick and lands in `[0]`. In an idle loop the metric cannot tell the two apart. Compare opcodes under load (as
  in the table above), or use the probe.
- A cold read (task-work completion) can also land in `[1]` or later: the metric means "not done during the
  submit", not "punted".
- Multishot ops count their first CQE only. Zero-copy sends count the send result.
- Afterwards, AGENTS.md's "Stats" line ("io-wq punts are not visible …") should mention `op_ticks_by_opcode`. I left
  it alone because the field doesn't exist on this branch.

## Open issues

- `fs::read`/`File::metadata` punt (statx). A per-request path that needs the size could `read_at` until a short
  read, or cache sizes.
- `open_direct` with `O_CREAT`/`O_TRUNC` works, but the open itself punts. Only the close is saved.
- Direct files can't be stat'ed. `fs::metadata(path)` (also punted) is the fallback.
- For G2, the SUT has to switch to `open_direct` and set `with_fixed_files`. Expected on f2fs: one io-wq punt fewer
  per stream request (about 7 µs less per close in the probe, screening), and no worker threads started for closes.
