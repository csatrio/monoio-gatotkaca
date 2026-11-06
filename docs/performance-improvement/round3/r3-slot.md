# r3-slot (E5: one cache line per op slot)

Branch `r3/r3-slot`, base `r3-g0` (`c3a56dc`). Files: `monoio/src/utils/slab.rs`,
`monoio/src/driver/uring/lifecycle.rs`, `monoio/src/driver/uring/ignored.rs`, tests
`monoio/tests/ignored_inline.rs`, `monoio/tests/slot_drop_alloc.rs` (new), this file. Nothing outside the
package's files was changed. No API or behaviour change; nothing to opt in.

Commits, in order:

| commit | what |
|---|---|
| `60cb355` `perf(slot): cheaper op-slot lookup, insert and poll (r3-slot)` | the clear wins of the review (task 3); no layout change |
| `c94955e` `test(slot): allocations of receives dropped in flight (r3-slot)` | the drop-heavy test (task 2) |
| `e3e1229` `perf(slot): 64-byte op slot, one cache line (r3-slot: E5)` | **E5**: 64-byte, 64-byte-aligned slot that keeps the 48 bytes of inline storage |
| (this file) | notes |

The lead can keep `60cb355` and `c94955e` without `e3e1229`. Below, "A" is the tree at `60cb355`, "E5" the tree
at `e3e1229`, "r3-g0" the base.

## Short answer

- **Keep E5.** The slot is one cache line (64 bytes, aligned, with and without `metrics`) *and* still keeps 48
  bytes of a dropped op inline: a receive into a `BytesMut` dropped in flight (a `timeout` firing) allocates
  nothing, exactly as before. The 48 → 40 cut the plan expected is not needed: `is_fd`/`kind` move into the
  word of the state's tag instead of sitting after the state.
- User instructions per frame (exact): `reqresp` pipe 2342 → 2098 (A) → 2077 (E5), plain 2397 → 2153 → 2132.
  A is −10.4%, E5 another −1.0%.
- Cache: user-space L1D load misses of the whole process fall by about 10% with E5 against A (perf counters,
  7 and 5 runs, in quiet and in busy periods alike). User cycles per frame on the server thread, with every
  function aligned to 64 bytes so that code placement does not differ between builds: plain 2680 (r3-g0) → 2195
  (A) → 2055 (E5); pipe 2270 → 2075 → 2095 (equal within noise). All cycles per frame: plain −1.1 to −1.9% for
  E5 against A, pipe equal. Screening numbers, machine not quiet; the lead's quiet slot decides.
- Drop-heavy case: 0 allocations per dropped `Vec<u8>` or `BytesMut` receive, before and after; a `timeout`
  firing on a `BytesMut` receive costs one allocation per round, the buffer itself; `ignored_inline` 256 /
  `ignored_boxed` 0 on both trees. The straightforward 40-byte variant would cost 1 allocation per dropped
  `BytesMut`/`Bytes` op (measured: 1.00 per op, 2.00 per timeout round).

## Layout before and after

Sizes in bytes on x86-64, from `lifecycle::tests::slot_is_one_cache_line` and `slab::tests::entries_are_cache_line_aligned`.

| | r3-g0 and A | E5 (no `metrics`) | E5 (`metrics`) |
|---|---|---|---|
| `IgnoredData` (48 inline + drop fn) | 56 | 56 | 56 |
| `Lifecycle` | 64 (tag widened to 4 bytes, payload at 8) | 64 (tag 1, `Meta` 2 at offset 1, payload at 8) | 64 (`Meta` 7 at offset 1) |
| `MaybeFdLifecycle` | 72 (`is_fd`, `kind`[, `opcode`, `submit_tick`] after the state) | 64 (= `Lifecycle`) | 64 |
| slab entry | 72, align 8 (`Entry<T>`) | 64, align 64 (`Slot<T>(Entry<T>)`) | 64, align 64 |
| lines per slot | 2 for most slots (stride 72) | 1 | 1 |
| `Slab::prefetch` | 2 prefetches | 1 | 1 |
| inline storage of a dropped op | 48 | 48 | 48 |

How E5 fits: the largest state is `Ignored(IgnoredData)` = 56 bytes, leaving one word for the tag and
whatever the slot keeps for its whole life. A field next to the state (as before) cannot share that word, so
`is_fd`, `kind` and the two `metrics` fields became a `Meta` (alignment 1, 2 or 7 bytes) that is the **first
field of every state**: rustc lays the fields of an enum variant out by ascending alignment, so it lands at
offset 1 in every state, in the tag's word. The slab's vacant marker uses a spare tag value (niche), so
`Entry<MaybeFdLifecycle>` stays 64. The 64-byte alignment is a wrapper struct in the slab (`Slot<T>`): a
`repr(align(64))` on the entry enum itself pads the `Vacant` variant to 64 bytes, which loses the niche
(128-byte entries).

Hot-path cost of carrying `Meta` in every state: none. A transition copies it, but the fast paths copy it
right before the write with nothing in between, and the compiler drops the store (it stores what is already
there); `poll_op`'s first poll writes one byte of tag plus the waker, `complete` writes tag, flags and result.
The first E5 build copied the meta before building the result (which may call `io::Error::from_raw_os_error`)
and paid 2 loads + 2 stores + a spill per completion: +11 instructions per frame. Ordering fixed it.

Which op data is inline (unchanged by E5, 48 bytes, alignment 8): `Recv`/`Send`/`Read`/`Write` with a
`Vec<u8>` (40 bytes of op data), `Box<[u8]>`, `Bytes` or `BytesMut` (48), `Send<Slice<&[u8]>>` (48), the
fixed-buffer ops. Boxed: anything larger, e.g. a sliced owned buffer (`read_exact`/`write_all` wrap the buffer
in a `Slice`, +16 bytes: `Recv<SliceMut<Vec<u8>>>` 56, `Recv<SliceMut<BytesMut>>` 64), vectored ops. With
the 40-byte variant the `Bytes`/`BytesMut` ops and `Send<Slice<&[u8]>>` would have moved to the boxed side.

The layout relies on choices rustc makes (field order in enum variants, the niche for the vacant marker).
They are pinned by **tests**, not by compile-time assertions: a `const` assertion would break a user's build on
a compiler that lays the enum out differently, where a test only tells us that the slot grew. The slab's
`prefetch` is written for any entry size (one line up to 64 bytes, first and last line beyond).

## Review of the three files (task 3): what `60cb355` changes

All measured by user instructions per frame on `reqresp` (pipe, 2000 connections, 4 KiB): 2342 → 2098.

**`slab.rs`**
- *Lookup.* Before: page id by `saturating_add`, shift, `leading_zeros` with its zero case, a clamp (`min`),
  then the page's `Option` test, its `prev_len` and its `initialized` count, then the slot: about 20
  instructions and three loads from the page. Now a per-page table (`PageIndex`: the slot pointer moved back
  by the page's first key, and the first key without an initialized slot) in front of the pages: one `bsr`
  for the page (`page_of`), one compare against `end`, one load of `base`, `base + key * stride`. The table
  has an entry for every value the bit scan can produce (58 on 64-bit), so no range check; keys beyond the
  last page or wrapping ones hit empty entries. A lookup with checks is 12 instructions (`prefetch`), an
  unchecked one (`poll_op`) 9. The pages' slots are a raw allocation (`Box::into_raw` of an uninit slice) so
  that lookups through the table and the pages' own accesses are the same pointer under Stacked Borrows;
  the slab and lifecycle unit tests pass under Miri with `-Zmiri-strict-provenance`, both feature sets.
- *Insert.* Before: `Slab::insert` was a call (6 register saves) that walked the pages and copied the
  72-byte value from the caller's stack into the slot with five 16-byte moves. Now the fast path is inlined
  (`#[inline]`), the page search is a cold out-of-line function, and the value never crosses a call (a
  `ManuallyDrop` keeps the unwind path from taking its address): the caller's `MaybeFdLifecycle::new` writes
  the two fields it sets straight into the slot (`movl` + `movw`). `Page::alloc` tests `next < initialized`
  first (the common case) instead of `is_full` first.
- *`Ref`.* `Ref::into_parts` / `Slab::get_unchecked` hand a slot to an out-of-line function as `(slab, key)` in
  two registers; a `Ref` (three words) is passed in memory, and those stores sat before the fast-path branch.
- Unchanged: page sizes, free lists, the compaction policy (every 2048 removals), the `Vec`-like growth.
  `Slab<T>` now holds raw pointers and is `!Send`; its only user (`UringInner`) is `!Send` anyway. The slab
  struct grows by the table (928 bytes).

**`lifecycle.rs`**
- *`poll_op`.* The three states a `Op` is normally polled in (`Completed`, `Submitted`, `Waiting` with the same
  waker) are handled inline with compares (no jump table); everything else is `poll_op_slow`, out of line.
  `UringInner::poll_op` (which inlines it) went from 5 register saves and a 0x50 stack frame to 2 saves. The
  first poll writes `Waiting` with `ptr::write`: the assignment called the drop glue of `Lifecycle` because
  after the waker clone (an indirect call) the compiler no longer knew the state.
- *`complete`.* The slow path takes `(slab, key)`: the `Ref` stores on the fast path are gone.
- Unchanged: the state machine, every slow path's behaviour, the `metrics` counters.

**`ignored.rs`**: unchanged but for a comment (the 48 is now what fills the line).

Nothing else in the three files is clearly cheaper. `mark_remove`'s generation counter (4 instructions per
removal) stays; see "tried and rejected" for the alternative.

## Measurements

Machine: Ryzen 5 4600G (6 cores, L2 512 KiB per core, two 4 MiB L3 slices; CPUs 0–2 share one slice with the
measurement core 2), other agents building and testing throughout. Binaries: r3-g0 from `target/before`, A from
`target/a`, E5 from `target/e5`, all default release profile; a second set built with
`RUSTFLAGS="-C llvm-args=-align-all-functions=6"` (every function 64-byte aligned) in `target/al-*`. Each run:
`flock /tmp/r3-measure.lock <bin> --conns 2000 --sizes 4k --path <p> --ring defer,skipenter,ringfd --cpu 2
--client-cpus 3,4 --warmup 1 --duration 4`, `smallframes --conns 2000 --size 4096 --client-cpus 3,4 --duration 4`.

### User instructions per frame (exact, server thread)

| workload | r3-g0 | A `60cb355` | E5 `e3e1229` |
|---|---|---|---|
| `reqresp` pipe | 2342 | 2098 (−10.4%) | 2077 (−11.3%) |
| `reqresp` plain | 2397 | 2153 (−10.2%) | 2132 (−11.1%) |
| `smallframes` (depth 2, per-frame timeout) | 2671 | 2615–2628 | 2524–2525 |
| `smallframes --no-timeout` | 1883 | 1698 | 1604 |
| `smallframes --depth 1` | 3207 | 3134 | not run |

`reqresp` repeats to the instruction across runs. `smallframes` with the timeout moves with two things that
are not in this package: how many frames one receive brings in (depth 2), and whether `Op::<T>::poll`,
`Op::recv_with` and `Op::send_with_flags` are inlined into the user's future, which depends on codegen-unit
partitioning (they carry no `#[inline]`): in r3-g0's build they were inlined into
`Timeout<serve_frame>::poll`, in A's and E5's they are calls, worth about 130 instructions per frame there.
See "requested changes".

### Cycles and throughput, default builds (screening, machine not quiet)

The machine alternated between quiet periods (server at 110–113k frames per CPU-second) and busy ones (80–100k,
all cycles per frame up to +40%, from the neighbours' L3 use). Medians of three interleaved runs are therefore
reported with the number of busy runs among them, and the quietest run of all runs of that cell is given too.

| workload, build | runs (busy) | median user cycles | median all cycles | median frames/CPU-s | quietest run (user / all / frames/CPU-s) |
|---|---|---|---|---|---|
| pipe, r3-g0 | 3 (1) | 2189 | 38,058 | 112,667 | 2177 / 37,960 / 112,923 (of 11) |
| pipe, A | 3 (1) | 2400 | 39,647 | 108,132 | 2116 / 37,818 / 113,365 (of 15) |
| pipe, E5 | 3 (3) | 3345 | 44,863 | 95,394 | 2110 / 38,339 / 111,804 (of 14) |
| pipe, A vs E5 alternating in one quiet stretch, 4 each | 4+4 (0) | 2136 vs 2152 | 38,318 vs 38,612 | 111,875 vs 111,017 | |
| plain, r3-g0 | 3 (0) | 2491 | 39,855 | 107,533 | 2424 / 39,277 / 109,129 (of 7) |
| plain, A | 3 (0) | 2727 | 40,374 | 106,166 | 2618 / 39,221 / 109,301 (of 8) |
| plain, E5 | 3 (0) | 2009 | 39,140 | 109,493 | 1879 / 38,617 / 110,991 (of 7) |
| smallframes, r3-g0 | 3 (1) | 2431 | 36,986 | 115,871 | 2423 / 36,735 / 116,666 |
| smallframes, A | 3 (1) | 2791 | 38,791 | 110,110 | 2641 / 36,639 / 116,986 |
| smallframes, E5 | 3 (0) | 2318 | 36,886 | 116,178 | 2264 / 35,694 / 120,061 |

Two things in this table are not the slot:
- The three pipe E5 runs of the series all fell into busy periods. In the alternating quiet stretch (row 4)
  and in the paired aligned runs below, A and E5 are equal on pipe.
- A's plain user cycles (2727, consistently, 8 runs) are *above* r3-g0's although it executes 10% fewer
  instructions, and a build of A with one function moved out of line (`alloc_slot`, +17 instructions) gave
  2071–2173. The hot function `CoreStage<conn>::poll` (14,413 bytes, byte-identical in all builds, half of the
  server's user cycles) starts at a different offset modulo 64 in each build (r3-g0 0x30, A 0x20, E5 0x30).
  Hence the aligned builds:

### Cycles and throughput, every function 64-byte aligned (screening)

Same source, `RUSTFLAGS="-C llvm-args=-align-all-functions=6"` for all crates. Instructions per frame are
unchanged (2342 / 2098 / 2077 pipe, 2398 / 2153 / 2131 plain).

| workload, build | runs | user cycles (quiet runs) | all cycles (quiet runs) | frames/CPU-s (quiet runs) |
|---|---|---|---|---|
| plain, r3-g0 | 3 (1 busy) | 2642, 2725 | 39,805, 40,930 | 107,663, 104,662 |
| plain, A | 3 (1 busy) | 2203, 2186 | 39,505, 40,264 | 108,492, 106,437 |
| plain, E5 | 3 (0 busy) | 2054, 2076, 2040 | 39,053, 40,596, 39,058 | 109,769, 105,523, 109,724 |
| pipe, r3-g0 | 4 (2 busy) | 2317, 2223 | 39,310, 38,605 | 108,999, 111,038 |
| pipe, A | 5 + 5 paired | 2130, 2235, 2088, 2062, 2084 | 38,846, 39,315, 38,354, 37,793, 37,914 | 110,325, 108,994, 111,766, 113,433, 113,061 |
| pipe, E5 | 6 + 5 paired | 2222, 2128, 2083, 2110 | 39,409, 38,854, 37,628, 38,232 | 108,674, 110,308, 113,933, 112,117 |

Paired A/E5 pipe runs (A then E5, five pairs, frames/CPU-s): 111,766 vs 99,717 (E5 in a busy period), 98,312 vs
100,443 (both busy), 106,965 vs 110,308, 113,433 vs 113,933, 113,061 vs 112,117. Reading: on plain, A's
instruction savings show as cycles once code placement is equal (user −18%, all −1%), and E5 takes another −6%
of user cycles and −1 to −2% of all cycles; on pipe, A and E5 are equal and both about −2% all cycles against
r3-g0.

### Cache misses (screening)

`perf stat -e instructions:u,L1-dcache-load-misses:u` on the whole process (server and both client threads,
user space; `--per-thread` needs an existing pid; three or more events would have multiplexed the example's own
counters). Normalised by the frames of the measurement window; the run is about 1.27 windows long for every
build, so the ratios compare. `reqresp` pipe:

| build | runs | L1D load misses per window frame | user instructions per window frame |
|---|---|---|---|
| r3-g0 | 4 | 217.1, 209.7, 206.8, 204.1 | 7,198 |
| A | 5 | 207.3, 221.4, 229.2, 227.9 (quiet runs 221–229) | 6,363 |
| E5 | 7 | 185.6, 192.1, 185.8, 182.2, 206.5, 196.1 (quiet runs 196–207) | 6,354 |

E5 against A: about −10% L1D load misses per frame, in quiet and busy periods alike (the counts rise with
throughput for every build). That is roughly 4 fewer misses per op across the four slot touches of an op
(insert, first poll, completion, final poll), which is what one line instead of two should give.

### Drop-heavy case (`monoio/tests/slot_drop_alloc.rs`, `--features metrics --nocapture`)

256 receives started and dropped in flight; a round of `time::timeout(1 ms, stream.read(BytesMut))` 64 times.

| case | r3-g0 | A | E5 | 40-byte inline (rejected) |
|---|---|---|---|---|
| `Vec<u8>` receive dropped: allocations per op | 0.00 | 0.00 | 0.00 | 0.00 |
| `BytesMut` receive dropped: allocations per op | 0.00 | 0.00 | 0.00 | 1.00 |
| `ignored_inline` / `ignored_boxed` (`BytesMut`) | 256 / 0 | 256 / 0 | 256 / 0 | 0 / 256 |
| `timeout` firing on a `BytesMut` receive: allocations per round | 1.00 (the buffer) | 1.00 | 1.00 | 2.00 |

The r3-g0 column was run on a detached `r3-g0` checkout with the test file copied in; the 40-byte column on the
same checkout with `INLINE_SIZE = 40` (the assertions fail there, the numbers print first). `ignored_inline.rs`
gained a case just over the limit (`Tracked<8>`: 56 bytes, boxed) so the limit is pinned from outside too.

## Tried and rejected

- **48 → 40 bytes of inline storage** (the plan's E5): costs one allocation per dropped `Bytes`/`BytesMut`
  op and `Send<Slice<&[u8]>>`, measured above; the meta-in-the-tag-word layout reaches 64 bytes without it.
- **`#[repr(align(64))]` on the slab's entry enum**: rustc lays each variant out with the enum's alignment, the
  `Vacant(usize)` variant becomes 64 bytes and no longer fits beside the niche → 128-byte entries (the
  `slot_is_one_cache_line` test caught it). A wrapper struct around the enum has no such problem.
- **Smaller drop function pointer** in `IgnoredData` (48-bit pointer, an index into a registry): would free
  bytes for the meta without touching the states, but a 48-bit pointer is an architecture assumption and a
  registry costs a lookup on every drop. Not needed.
- **A hand-rolled tagged union** for `Lifecycle` (defined layout, in-place field writes): not needed once the
  meta copy is free; more `unsafe` for nothing.
- **`alloc_slot` out of line** (insert's fast path as a call that returns the slot to write): +17 instructions
  per frame (2115 vs 2098 pipe), kept for a while because it keeps every op type's submit path ~180 bytes
  smaller; rejected on the instruction count. (Its plain-path cycles looked better than A's, which turned out
  to be the code-placement effect above.)
- **Deferring compaction to the next insert** (so `remove` is leaf code with no call that can clobber
  registers): saves ~6 instructions per op but changes when the last page is returned (not before another
  op is submitted). Not done: behaviour change for a small gain.
- **Uniform chunks or one flat table** instead of doubling pages: a flat table moves slots when it grows
  (a copy of megabytes at 100k ops in flight) and uniform chunks change the memory profile; the page table
  gives most of the lookup gain without either.
- **`ManuallyDrop` around the result in `poll_op`'s `Completed` arm** to keep the result in registers across
  the (cold) compaction call: same instruction count, dropped.
- **Compile-time size assertions**: see "Layout": tests instead.
- The first E5 build copied the meta before building the result: +11 instructions per frame; fixed by copying
  it last (now 0 extra stores).

## Open questions and observations

- The large run-to-run swings in all-cycles per frame (up to +40%) come from the neighbours' use of the shared
  L3 slice; the L1D-miss counter and the instruction count are the robust numbers from this package. The
  cycle effect of E5 should be confirmed in the lead's quiet slot, on plain and pipe, with the function
  alignment flag or with enough runs to see past placement effects (A's plain numbers show a ±20% user-cycle
  swing from placement alone).
- `Slab` is now `!Send` (raw pointers). If a future change wants to move a driver between threads, the pages
  are plain heap memory; an `unsafe impl Send` would be sound.
- With `metrics` the `Meta` is 7 bytes (the submit tick as bytes), so the metrics build has the same 64-byte
  slot; a counter that needs more than that (none planned) would push the slot to 128 bytes.
- Spare room for r3-pair, which "builds on the slot layout": the tag word has 5 free bytes without `metrics`,
  none with it (`Meta` is 7 bytes there); a new `OpKind` variant costs nothing; every state but
  `Ignored`/`IgnoredMore` has 16 or more spare bytes. `Slab::get` on a tagged key (bit 61 set) returns `None`
  (such keys land on empty index entries), so the dispatcher must strip the tag before the lookup, as planned.

## Requested changes

1. **AGENTS.md (r3-docs)**, the "Dropped in-flight ops" bullet. Current text: "Dropped in-flight ops keep their
   data in the op slot until the kernel is done: inline (no allocation) up to 48 bytes and alignment 8, which
   covers recv/send/read/write with `Vec`/`Bytes`/`BytesMut`; larger data is boxed. Stats (`metrics` feature):
   `ignored_inline`, `ignored_boxed`." Proposed: "**Op slots.** Every in-flight op has a 64-byte slot (one
   cache line, 64-byte aligned, the same with `metrics`) in a paged table that grows without moving slots.
   **Dropped in-flight ops** keep their data in the slot until the kernel is done: inline (no allocation) up
   to 48 bytes and alignment 8, which covers recv/send/read/write with `Vec`/`Bytes`/`BytesMut`; larger data
   (e.g. a `Slice` of an owned buffer, as `read_exact`/`write_all` use) is boxed. Stats (`metrics` feature):
   `ignored_inline`, `ignored_boxed`."
2. **ROUND3-PLAN.md, "What the code says" 6** (lead): now "An op slot is 64 bytes at alignment 64, one cache
   line, with 48 bytes of inline storage for dropped ops (r3-slot)".
3. **r3-op** (`driver/op.rs`, `driver/op/recv.rs`, `driver/op/send.rs`): `#[inline]` on `Op::<T>::poll`,
   `Op::<Recv<T>>::recv_with` and `Op::<Send<T>>::send_with_flags` (and the other constructors of that shape).
   They are generic, so a user crate instantiates them, but without `#[inline]` their inlining into the
   user's future depends on which codegen unit the instance lands in; `smallframes` with the per-frame
   timeout pays about 130 instructions per frame when they are calls. Not applied here (not my files).
4. **`handle_cqe` in `driver/uring/mod.rs`** (nobody's in wave 1; optional): `Completed` comes back from
   `Ref::complete` through memory (it holds an `Option<MaybeFdLifecycle>`), so the fast path stores the
   `None`, and the common code reloads and tests it: 3 instructions per CQE. Passing `&mut self.deferred` into
   `Ops::complete` / `Ref::complete` (`self.ops` and `self.deferred` are disjoint fields) would let the fast
   path return nothing. Worth ~6 instructions per frame; left for the owner of that function.

## Status

- `cargo test -p adaptif-gatotkaca-monoio`: 498 passed, 0 failed, 8 ignored (76 binaries).
- `cargo test -p adaptif-gatotkaca-monoio --features metrics`: 517 passed, 0 failed, 8 ignored.
- Miri (`cargo +nightly miri test --lib`, `-Zmiri-strict-provenance`): the `slab`, `lifecycle` and `ignored`
  unit tests pass, both feature sets.
- `cargo clippy --workspace` and with `--features adaptif-gatotkaca-monoio/metrics`: the 2 old warnings in
  `net/udp.rs`; the third old one (`question_mark` on the page `match` in `Slab::get`, `slab.rs:42`) is gone
  with the rewrite. No new warnings.
- `cargo fmt` applied.
