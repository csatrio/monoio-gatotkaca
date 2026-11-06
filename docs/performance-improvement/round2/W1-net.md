# W1-net: accept batching and the recv_multi read adapter (M1, F3)

Branch `r2/W1-net` from `r2-g0`. Files: `net/tcp/{listener,multishot,mod}.rs`,
`net/unix/{listener,multishot,mod}.rs`, `driver/op/accept_multi.rs`, `driver/op/recv_multi.rs`, tests
`monoio/tests/accept_batch.rs`, `monoio/tests/recv_read_into.rs`, `AGENTS.md`. No changes in files owned by
others, none in `lib.rs` (new types are exported from `net::tcp` / `net::unix`).

## M1: accept everything queued per poll

G0 showed one accept SQE in flight ⇒ one accept per scheduler round (~100/s at 1 MiB × 4096, accept queue
3,338, ~6,200 timeouts), fixed by multishot accept at no throughput cost. The runtime side makes that the
obvious API.

### APIs

| API | What |
|---|---|
| `TcpListener::accept_batch(max, flags) -> io::Result<Vec<(TcpStream, SocketAddr)>>` | ≥ 1 connection, all queued up to `max`, from one poll |
| `TcpListener::accept_batch_into(&mut Vec<..>, max, flags) -> io::Result<usize>` | same, appends to your `Vec` (no allocation in steady state) |
| `UnixListener::accept_batch` / `accept_batch_into` | same for Unix sockets |
| `AcceptStream::next_batch(&mut Vec<(TcpStream, SocketAddr)>, max)` / `next_stream_batch(&mut Vec<TcpStream>, max)` (+ `poll_*`) | batch from a stream you own; `Ok(0)` only when it ended |
| `UnixAcceptStream::next_batch` / `next_stream_batch` | same |

`TcpListener` docs have a "Writing an accept loop" section with the G0 numbers and an example.

### Design: persistent multishot accept (option a)

`accept_batch` keeps a `BatchAcceptor` (`driver/op/accept_multi.rs`) in the listener: a
`RefCell<Option<AcceptMultiOp>>` plus a busy flag. The first call arms a multishot accept; it stays armed
between calls, so connections that arrive between calls are already accepted and queued, and the next call
takes them without a round trip. Each poll drains the op's CQE queue up to `max`.

Why not (b) "one io_uring accept, then `accept4` until `EAGAIN`": monoio listeners are blocking and
`accept4` has no `MSG_DONTWAIT`; making the listener `O_NONBLOCK` changes io_uring's behaviour for the
listener's other accepts (a non-blocking file can make io_uring return `EAGAIN` instead of arming poll),
`poll()`+`accept4` is racy with other acceptors on the same fd, and `IORING_ACCEPT_DONTWAIT` would still be
one SQE round trip per connection. (a) needs no syscalls besides `getpeername` and works on 5.19+.

Rules:
- Same `flags` on every call while armed (`InvalidInput` otherwise).
- One task at a time: a second concurrent call fails with `ResourceBusy` (the op slot has one waker; a
  silent second waiter could hang). The `RefCell` borrow is only held within a poll.
- Dropping an `accept_batch` future (e.g. in `select!`) keeps the accept and its queued connections
  (tested).
- An error ends the multishot (e.g. `EMFILE`): it is returned, after the connections taken before it in the
  same poll (stashed and returned by the next call), and the next call arms a new one. A fresh multishot that
  ends at once without connection or error returns an error instead of looping.
- Peers already gone (`getpeername` fails) are closed and skipped in batch calls.
- Dropping the listener cancels the multishot and closes queued connections. `UnixListener::into_raw_fd`
  drops the acceptor first (and its old `MaybeUninit::uninit().assume_init()` swap was replaced by
  `ptr::read`/`drop_in_place`); it still panics while a multishot accept holds the fd, as before with
  `accept_multi`.
- `accept()` on the same listener still works; both compete for connections.

### Automatic re-arm

`AcceptMultiOp` (used by `AcceptStream`, `UnixAcceptStream`, `DirectAcceptStream`, `accept_batch`) now
re-arms like `RecvMultiOp`: a successful CQE without `F_MORE` (the kernel can't post more, e.g. full CQ:
the final CQE then carries a connection and goes to the overflow list) sets `rearm`, and the next poll
submits a new multishot with the same data. Not after a cancel; an error still ends the stream (yielded
once, then `None`). The benchmark server's "re-create on `None`" is no longer needed for that case.

## F3: `recv_multi` for framed read loops

| API | What |
|---|---|
| `RecvStream::read_into(&mut BytesMut) -> io::Result<usize>` (+ `poll_read_into`) | feature `bytes` |
| `RecvBundleStream::read_into` (+ `poll_read_into`) | copies every buffer of each bundle |
| `RecvStream::into_reader()` / `MultiRecvReader::new(stream)` | `AsyncReadRent` adapter, `buffered()`, `get_ref()`, `into_inner()` |

`read_into` waits for the first chunk, then copies every chunk already queued without waiting, giving each
buffer back to the ring right after its copy. `Ok(0)` at EOF (also after cancel, or once an error that ended
the recv was returned). An error after some data is returned by the next call.

`MultiRecvReader::read`/`readv` copy from the leftover ring buffer, then from queued chunks, waiting only
when nothing was copied. It holds at most one partly read `SelectedBuf` between reads; a fully copied buffer
is returned to the ring at once. Data leaves the reader only synchronously inside a completing poll, so
dropping a read future loses nothing.

### ENOBUFS re-arm rules (`OnEnobufs` in `recv_multi.rs`)

- `next()` / `poll_next` (`OnEnobufs::Yield`): unchanged: `Err(ENOBUFS)` once, then `None`.
- `read_into` and `MultiRecvReader` (`OnEnobufs::Rearm`): an `ENOBUFS` that ended the multishot is
  discarded and the recv re-armed in the same poll (the buffers were copied out, so they are free; no data is
  lost because the kernel didn't consume any). If the re-armed recv hits `ENOBUFS` again before any data
  (`enobufs_retry`), the ring is held by someone else: `ENOBUFS` is returned, and the next call re-arms
  again. So the caller never sees `ENOBUFS` unless it holds the ring's buffers elsewhere.
- Re-arm submission failure (SQ full after retries) is returned as the error; the stream then ends.

### Costs (documented on the types)

One copy per byte (kernel → ring buffer → `dst`); in exchange no SQE per read and no user buffer parked in
the kernel per idle connection. With a tiny ring the recv ends with `ENOBUFS` every `entries` buffers, which
costs one re-arm SQE each time. Don't mix with other reads on the socket.

### Fix on the way

`RecvMultiOp::poll_next_buf`/`poll_next_bundle` called `self.data()` (which panics when the op is gone)
before checking for an error; after a failed re-arm submit that would panic. They now match on the result
first.

## Tests

`tests/accept_batch.rs`:
- `queued_connections_accepted_in_two_polls`: 1000 connections queued before the loop starts; the loop
  task (wrapped in a poll-counting future) gets all 1000 with correct peer addresses in ≤ 2 polls (measured: 2).
- `accept_batch_respects_max_and_keeps_the_rest_queued`; flags mismatch → `InvalidInput`.
- `concurrent_accept_batch_is_refused` (`ResourceBusy`), `dropped_accept_batch_keeps_queued_connections`.
- `accept_stream_rearms_after_full_cq` / `accept_batch_rearms_after_full_cq`: `entries 256`, `cq_size 256`,
  1000 queued connections: all accepted, stream not finished, still accepting afterwards; with `metrics`
  asserts `ops_by_opcode[AcceptMulti] >= 2` (a real re-arm happened).
- `unix_accept_batch`.

`tests/recv_read_into.rs` (`bytes`):
- `framed_reads_across_buffer_boundaries`: 3 length-prefixed frames written in cuts through headers and
  bodies, 8 × 64 B ring, parsed from a `BytesMut`; EOF sticky; `in_use == 0`.
- `tiny_ring_rearms_after_enobufs`: 2 × 64 B ring, 64 KiB message, all data in order (`enobufs >= 1` with
  `metrics`); EOF.
- `enobufs_reported_while_buffers_are_held`: then continues once they are dropped.
- `multi_recv_reader_is_an_async_read_rent` (`read_exact` framing, EOF, buffers returned),
  `multi_recv_reader_readv_and_leftover`.
- `tcp_read_into_and_bundles`: bundle stream when supported (it is on 7.0), else plain.
- `incremental_ring_read_into`: incremental ring (6.12+, skipped on `EINVAL`), incl. exhaustion.

All skip on `EINVAL`/`Unsupported`. Matrix: `cargo test -p adaptif-gatotkaca-monoio` (default, `--features
metrics`, `--features sync`) pass; `cargo clippy --workspace --all-targets` (+ metrics) shows only the
pre-existing warnings (udp.rs, slab.rs, async_write_rent_* tests); `cargo fmt` clean.

Screening (machine not quiet): `fairness --accept multi --rr 200 --accept-ramp 2000 --ramp-secs 2`: 2000 ramp
connections, 0 timeouts, accepts peak 1000/s, accept→first-read p99 0.07 ms.

## For other packages / the lead

- **W1-sched (coop):** the batch drain polls `MultishotOp::poll_next` in a loop within one task poll. If the
  coop check lands in `MultishotOp::poll_next` (or `UringInner::poll_multi`) and charges each CQE, a batch
  would stop after the budget and wake itself, which brings back part of M1. Suggest: charge a batch as one
  unit (e.g. a `poll_next` variant without the budget check that `AcceptMultiOp::poll_batch` and
  `RecvMultiOp::poll_read_into` use after the first item), or exempt multishot drains as G0 suggests.
  `driver/op/multishot.rs` is not in anyone's list; I did not change it.
- **SUT:** switch the accept loop to `accept_batch_into` (or `AcceptStream::next_batch`); the manual re-create
  on `None` can stay for the error case but is no longer needed for CQ overflow.
- Optional `fairness --accept batch` mode (file owned by W1-sched): `listener.accept_batch_into(&mut v, 256,
  AcceptFlags::EMPTY)` then spawn each.

## Open

- `accept_multi_direct` (`DirectAcceptStream`) re-arms too but has no batch method (no peer address to
  fetch; `poll_next` per connection costs no syscall). Easy to add if wanted.
- `MultiRecvReader` covers `RecvStream` only, not bundles.
