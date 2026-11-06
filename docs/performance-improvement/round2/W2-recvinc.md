# W2-recvinc: recv_multi stalls on a shared incremental ring (F3)

Branch `r2/W2-recvinc` from `r2-g2`. Files: `driver/uring/{lifecycle,mod}.rs`, `driver/op/{multishot,recv_multi}.rs`,
`driver/buf_ring.rs` (docs, `is_incremental`), test `monoio/tests/recv_incremental_shared.rs`, `AGENTS.md`.

## Symptom

2000 connections, `recv_multi` + `read_into` on one incremental ring (2 × 64 KiB). Throughput fell from
~80k to ~11k frames/s, the server went idle, connections hit the 10 s timeout. About one SQE per frame, so
no recv was re-armed. A flat ring was stable.

## Root cause

Hypothesis (c). With an incremental ring the CQE gives only the buffer id and the length. The offset of the
piece comes from the order of the CQEs on the ring. `BufRing::take` tracks it, so it must see the CQEs in
CQ order. But the driver queued the CQEs per op, and each connection called `take` when its task ran.
Tasks run in their own order, and `read_into` drains all queued CQEs of one connection at once. So
connection A could take its second piece before connection B took the piece in between.

Two effects:
- Wrong offsets. A got B's bytes, and later pieces were cut short (`len = min(res, buf_size - offset)`).
  Lost bytes mean a request never completes. The client waits for the ack, sends nothing more, the
  multishot recv stays armed but idle. That is the stall, with one SQE per frame.
- Early recycle. If the piece that ended the buffer (no `BUF_MORE`) was taken and dropped first, the buffer
  went back to the ring while an earlier piece was still queued. The kernel then refilled it.

Hypotheses (a) and (b) were not the cause. ENOBUFS re-arm works.

## Fix

The driver now takes the buffers when it reaps the CQE, in CQ order, for `recv_multi` and
`recv_multi_bundle` on an incremental ring. `MultiState` has an optional `PreTake` (ring handle, bundle
flag) and a queue of `Taken` (the `SelectedBuf` or `Bundle`), one per queued CQE. The consumer gets the
CQE and its buffers together (`MultishotOp::poll_next_taken`). The views exist from reap time, so a buffer
cannot recycle while a queued CQE still points into it. Dropping the stream drops the queued views.

Flat rings are unchanged (no pre-take, take at poll as before: each buffer belongs to one CQE). Other
multishot ops (accept, raw `MultiOp`) are unchanged. Cost on the incremental path: one `VecDeque` push and
pop per CQE.

Not fixed: raw `MultiOp`s and single-shot receives (`recv_bundle`) on an incremental ring shared with other
ops still take in poll order. Documented: use the CQE hook for raw SQEs, or don't share the ring.

## Sizing

A buffer goes back only after all its pieces are consumed. One slow connection pins a whole buffer. For a
shared ring use about `2 × connections × bytes in flight per connection`, more buffers rather than bigger
ones. For the benchmark (2000 × 4 KiB): ~16 MiB, e.g. 256 × 64 KiB. The 2 × 64 KiB ring is far too small;
the server then sees `ENOBUFS` from `read_into` often.

## Test

`recv_incremental_shared::shared_incremental_ring_many_connections`: 200 loopback connections, client
runtime on another thread does ping-pong (4 KiB request in two writes, 2-byte ack). Server checks every
byte. Runs a flat ring (256 × 16 KiB) and an incremental ring (4 × 64 KiB, often exhausted). Asserts no
corruption, no errors, every connection made progress in the last second, incremental total ≥ 1/4 of flat,
`in_use == 0` at the end.

Before the fix (release): 82 of 200 connections stalled, 1172 corrupt requests. Debug: 29 corrupt. After: 0
stalled, 0 corrupt, incremental ≈ flat (~4.7k frames/s each in this setup).

## To measure on the bench

- Incremental run with the old 2 × 64 KiB ring: should no longer decay, but expect `ENOBUFS` from
  `read_into` (check how the bench handles it: retry, not close).
- Incremental with 256 × 64 KiB vs flat 4096 × 16 KiB: throughput and RSS.
- `BufRingStats` (`metrics`): `enobufs`, `high_water`.
