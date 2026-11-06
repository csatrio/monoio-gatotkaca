# r3-docs (W1, AGENTS.md for wave 1)

Branch `r3/r3-docs`, base `r3-g1`. Files: `README.md`, `AGENTS.md`, `docs/TODO.md`, one doc comment in
`monoio/src/driver/buf_ring.rs` (`BufRingOptions::batch_publish`), this file. No code change. Every API name
written into the docs was checked against `monoio/src` (`grep -rn 'pub fn <name>'`); numbers come from `G0.md`
and `G1.md` only.

Checks: `cargo doc -p adaptif-gatotkaca-monoio --no-deps` goes from 18 warnings to 16 (two README links fixed,
see below; none added); `cargo fmt --check` clean; `cargo clippy -p adaptif-gatotkaca-monoio` the 3 old warnings.

## What changed

### `README.md`
- `adaptif-gatotkaca-monoio = "0.7"` → `"0.9"`.
- "Registered buffers. Not supported natively" replaced by `FixedBufPool`, `read_fixed_at`/`write_fixed_at`,
  `recv_fixed`/`send_fixed`, fixed files and direct accept.
- "Multiple cores": `utils::ThreadPerCore` named. "Talking between tasks": `uring::msg_ring` and
  `uring::event_channel()` work without `sync` (the old text said `sync` was needed across threads).
- "Timeouts and cancellation": `read_with_deadline`/`write_with_deadline` and `time::Deadline` named.
- New "Servers" line: batch accept and a pointer to the recommended configuration in AGENTS.md.
- "Provided buffers": `recv_multi` named. "Raw io_uring": `with_ring` no longer suggested for registering
  buffers (the pool does that).
- Cargo features: `metrics` was missing from the list.
- Documentation: the per-round files are described generically (the old text named ROUND1 only).
- `[examples/](examples)` → `[examples/](examples/)` and `[NOTICE](NOTICE)` → `[NOTICE](./NOTICE)`: the README
  is the crate doc (`#![doc = include_str!("../README.md")]`), and rustdoc read both as intra-doc links (2 of
  the 18 old warnings). `monoio/examples/` named next to `examples/`.

### `monoio/src/driver/buf_ring.rs`
`batch_publish`: "the per-tick publish needs driver support; until the driver calls it, use `BufRing::publish`"
replaced by what the driver does (`driver/uring/mod.rs`: `flush_pending()` in `enter_with`, before every
`io_uring_enter`, and in `Driver::submit` where `skip_empty_enter` skips one). Nothing else in the file.

### `AGENTS.md`, sections touched
- **Layout**: `monoio/examples/` with `fairness`, `smallframes`, `thread_per_core`, `reqresp` (its main flags).
- **Core concepts**
  - Provided buffers: buffer sizing rule (whole frame; G1 numbers), `published`/`grown`/`retired` in the stats,
    `batch_publish` wording, new `working_set(initial, step)` bullet with the grow/shrink rule,
    `published()`/`grow_published`/`shrink_published`, take-at-reap-time rule, G1 verdict; `wait_free()`
    paragraph after the options.
  - New **Eager sends (direct syscalls)** bullet after "Socket flags and vectored sends".
  - New **Request/response loops** bullet (the three patterns of ROUND3.md W1 with the G0/G1 findings).
  - Multishot ops: accept-loop bullet gets the D2 rule (one round per sweep under `coop`, numbers from round 3
    G0) and names `accept_batch_direct_into`; the `recv_multi` bullet gets `next_rearm`/`poll_next_rearm`,
    `rearm_on`, `ring()`; the bundles sentence about re-arming now mentions `next_rearm`/`rearm_on`.
  - Fixed files: `is_direct()`, the sync setters named, new **Direct-only sockets in a server** sub-bullet
    (`accept_batch_direct_into`, `ENFILE` loss, `shutdown_with`/`shutdown_both`, `set_sockopt`/`get_sockopt`,
    `set_nodelay_async`, `set_tcp_keepalive_async`, the `SKIP_CANCEL` change on `IORING_OP_SHUTDOWN`, kernel
    limits, the syscalls-per-connection line 2 / 3 / 0, G1 verdict).
  - **Op slots** (64-byte slot, 48 bytes inline unchanged) replaces the head of "Dropped in-flight ops".
  - New **Request deadlines with the timer** bullet after "Deadlines without the timer" (`Deadline` API,
    `with_deadline`, `timeout_coarse`, `Instant::recent`, G1 instruction counts, behaviour changes).
- **Runtime configuration**: `min_wait` bullet gets `uring::set_min_wait` and the stale-flag kernel note; new
  `drain_taskrun(max)` bullet; `DriverStats` gets `enter_left_taskrun` and `drain_enters`; `RuntimeStats` gets
  `ops_completed`; new paragraph **Recommended configuration for request/response servers** with the G1
  figures against tokio on the three loads.
- **Feature flags**: the `metrics` row lists `RuntimeStats`, `Runtime::{stats, reset_stats}`,
  `monoio::{runtime_stats, reset_runtime_stats}` and the new counters.

### `docs/TODO.md`
- Header now "after round 3 (0.9.0)".
- "Measure first": the opt-ins that rounds 2 and 3 measured are listed once with their gate files and
  removed as items; left open: recv bundles and `send_zc_early` (no gate run of their own), the TLS write
  count, the `event_channel` benchmark, fixed files on a real NIC and a churn load, `zcrx`/NAPI.
- A3: `open_direct` closed the file half; only direct-only Unix sockets remain.
- Gaps and known limits from the package notes added (socket-control calls not on halves and not nameable,
  `ENFILE` loss of the direct batch accept, `try_recv` without `MaybeUninit`, zero-copy sends and the eager
  path, `wait_free` notify-one, timer counters, `IORING_ENTER_ABS_TIMER`, the stale `TASKRUN` flag with
  `min_wait`, `drain_taskrun` only after a waiting park, working-set under-reporting for raw users, io-wq
  shutdown of direct sockets, the self-waking main future, `cancel` of a named future outside a runtime).
- Cleanup: `utils/slab.rs` clippy warning gone (r3-slot); rustdoc warnings 18 → 16 with the files named; the
  stale `same_waker` comment (below); `reqresp` leftovers (`--path pair` stub, no `accept=` label). The
  `RLIMIT_NOFILE` fixup r3-direct asked for was applied by the lead (`main.rs:416`), so it is not listed.
- Left as they were (the lead's instruction, or unchanged): the named future for `send_vectored` and
  `SendMsgVec` boxed (r3-vec), A14, A15 row 5.

## Sentences of the package notes not adopted, and why

- **r3-drain's proposed `drain_taskrun` bullet** quotes reproducer shares ("82–89% → 95–97%, `coop` 96–99%",
  "−1% with 1, −3% with 4"). Screening numbers; replaced by G0's left-behind shares (47% / 1–3%) and G1's
  verdicts (`drain2` −2% at R2000, `drain4` −2.6% on the reproducer in the quiet slot). Its "`max` 1 or 2 is
  enough" became "use 2" (G1: `drain1` −3.4% looked like a bad draw, `drain2` is the recommended value).
- **r3-drain's `set_min_wait` text** says `Unsupported` "if the kernel lacks `IORING_FEAT_MIN_TIMEOUT` (6.12)";
  the code returns `Unsupported` only when a wait is asked for, and turning it off always succeeds (`raw.rs`
  doc). Written that way.
- **r3-eager's bullet** quotes "−2% at 2000 connections here" for `coop`; screening. Replaced by the G1
  verdict (R8 ack p99 135 → 109 µs, −1.1%; R2000 −1.2%) and the rule without a number. Its "`try_send`
  counts ... split halves" was widened to the ops the lead's guard commit (`87e2563`) added: `recv_multi`,
  `read_at_then_send`, `*_with_deadline`, splice.
- **r3-bufring's `working_set` bullet** quotes "8 connections on 4096 × 16 KiB: 74k frames/s; with
  `working_set(16, 16)`: 92k" and "200 connections on 16 buffers: 123k at 3 task polls against 76k at 24";
  screening. Replaced by G1 (72 KiB buffers tie at R8, +2.1% at R2000; 16 KiB −5.8% at R8). The "about three
  task polls per re-arm" cost of `next_rearm` is kept as a qualitative statement from the method's own doc.
- **r3-direct's cost table** condensed to one line (2 / 3 / 0 syscalls per connection), as asked. Its
  "Throughput at 2000 connections is the same as with `register_fixed` (screening)" replaced by G1's +0.4%.
  Its proposed replacement "`local_addr` and the sync option calls return `Unsupported`" adopted, with the
  listener inheritance kept (the `is_direct` doc says so).
- **r3-timer's bullet** quotes "about 550–630 user instructions per request" and "about 75–100 instructions
  per request" for a `Deadline`; those are the package's own counts. Replaced by G1's per-frame counts on the
  pipe path (1,798 / 1,983 / 2,433, G0 3,341). Its "a `Sleep` is 72 bytes, a `Deadline` 56" kept (pinned by
  unit tests). The fresh-list paragraph was dropped: the commit was not merged.
- **r3-slot's "Op slots" wording** adopted nearly verbatim (it is a layout fact, not a measurement).
- **r3-stats's two sentences** adopted, with the `min_wait` caveat r3-drain found added to
  `enter_left_taskrun`.
- **ROUND3.md W1's third pattern** ("few connections next to bulk work → `coop`") is no longer the rule after
  G1: written as "`defer` with `drain_taskrun(2)` matches it".
- **r3-repro's option table** is not reproduced; the Layout line names the flags that matter.

## Contradictions between notes and code (code wins)

1. **r3-slot.md** says the inline op data covers "the fixed-buffer ops". `driver/op/fixed.rs`: `ReadFixed` is
   `Target` (16 bytes) + `FixedBuf` (40) + `bool`, i.e. 64 bytes, above the 48-byte limit, as `TODO.md` has
   said since round 1. Not verified by a compiled size test (that would be a code change); the TODO line stays.
2. **r3-timer.md requested change 3** (one vtable address for task wakers) was applied by the lead
   (`task/waker.rs::vtable`, `#[inline(never)]`), but the comment on `same_waker` in `time/driver/entry.rs`
   still describes the old promoted-constant vtable. Listed in `TODO.md` "Cleanup".
3. **r3-direct.md requested changes** for `reqresp/main.rs`: the `RLIMIT_NOFILE` one is applied, the
   `accept=direct|fd` label is not. Listed in `TODO.md`.
4. **r3-probe.md KV1 consequence 3** ("the flag is reliable for 'work is left'") holds only for
   `min_complete <= 1`; r3-drain's kernel finding (stale flag after a batching wait) is the one written into
   AGENTS.md. The lead's note in r3-probe.md already says so.
5. **r3-drain.md** describes variant B (`drain_taskrun_pre_tick`) and the optional backlog-suspend commit; both
   are gone from the tree (`c33cad3`, and commit 3 was never merged). Only variant A is documented.
6. **r3-repro.md's option table** lists `--path eager|pair` as stubs and `--wset`/`--direct` as rejected;
   `eager`, `--wset` and `--direct` are filled now, `pair` is still a stub. The AGENTS.md Layout line lists
   `plain|pipe|mshot|eager`.
7. **r3-eager.md open question 4** (two ring sends in flight on a full socket are not ordered by the kernel)
   is written into the eager-sends bullet as a rule, as the note asked; it is the note's statement, not
   probed in a gate.

## Open questions

- `TODO.md` keeps "Dropped in-flight op data ... `ReadFixed` (64) ... still boxed" against r3-slot's claim
  (contradiction 1). A one-line size assertion in `tests/ignored_inline.rs` would settle it.
- The "Recommended configuration" paragraph says the first few dozen connections stay on per-connection
  buffers next to bulk work. That is the bench's `pipeack` + `mshot` split (first 64 connections, ROUND3.md);
  the crate has no helper for it, and `reqresp --path mshot` puts every connection on the ring. G3 may want
  to decide whether the split belongs in the recommendation or whether all-multishot (−3% frames, +30 µs p99
  on MIX) is close enough.
- The README's "Servers" line and the AGENTS.md paragraph recommend `drain_taskrun(2)` as a fixed value. G1
  measured 1, 2 and 4; nothing between 2 and 4, and nothing on a real NIC.
- `BufRing::options()` is still listed under Cleanup; it is used in the `with_options` doc example
  (`buf_ring.rs:398`), so removing it touches that example too.
