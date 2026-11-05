# monoio fork: what the transport needs

Fork: `/home/csatrio/rust-dev/monoio-gatotkaca` (branch `main`, head `8791c83`, published as `adaptif-gatotkaca-monoio` 0.5.0).
Companion to [PIP.md](PIP.md). Item ids such as U4 or C1 refer to that document.
Status: review of the source only. Nothing has been built or measured. Paths below are relative to the fork's `monoio/src/`.

## Summary

The fork already has a usable escape hatch:
- raw single-shot SQEs (`uring::submit`);
- ring access (`with_ring`);
- a CQE hook for SQEs pushed by the user (`set_cqe_hook` + `user_data`);
- provided buffer rings (`BufRing`);
- `SEND_ZC`;
- `ATTACH_WQ`.

That is enough to *prototype* every io_uring item in the PIP, but not to build an efficient transport on top of it:
1. **Multishot only works through one global hook per runtime.** The driver's own op lifecycle treats any `F_MORE` completion as `SEND_ZC` and hits `unreachable!` for anything else, so multishot can't be a normal op.
2. **The op futures have no names** (`impl Future` from async fns). The transport therefore has to box every read and write to store it (PIP C1).
3. **No first-class fixed files, registered buffers, accept flags, linked chains or cross-ring messages.** Each one is unsafe hand-written code in the transport today.
4. **Several panics on the submit path.** With our `panic = "abort"` profile, a submission failure kills the SUT.
5. **No driver counters.** We can't see SQEs, enters or CQEs per tick, which PIP phase 0 needs.
6. **The TLS layer wastes syscalls.** Its 16 KiB staging buffer is smaller than one full TLS record, and it writes one record per `write`, so bulk TLS sends alternate 16 KiB and ~22-byte writes (A15). This only matters for runs that use TLS.

Everything below should be **off by default** (a cargo feature or runtime opt-in), so the benchmark baseline (`async-cancel`, `bytes`, `utils`) does not change. **SQPOLL stays out.**

## 1. Current state (inventory)

| Area | What exists | File |
|---|---|---|
| Raw single-shot SQE | `unsafe uring::submit(entry, data) -> RawOp<T>` resolving to `(Cqe, T)`; `IoUringDriver::submit_raw` also works before `block_on` | `driver/raw.rs:134`, `:265` |
| Ring access | `unsafe uring::with_ring(|r| ..)` for registration, probing, pushing user SQEs | `driver/raw.rs:155` |
| User SQE completions | `user_data(id)` (prefix `0b10 << 62`), a single `set_cqe_hook(FnMut(u64, Cqe))` per runtime, delivered after each tick | `driver/raw.rs:159-255`, `driver/uring/mod.rs:330` |
| Provided buffers | `BufRing::new(bgid, entries, buf_size)`, `take(cqe) -> SelectedBuf` (`Deref<[u8]>`), returned to the ring on drop | `driver/buf_ring.rs` |
| Zero-copy send | `TcpStream::write_zc` (`SEND_ZC`), resolves after the notification CQE | `driver/op/send.rs:185-226`, `driver/uring/lifecycle.rs:14-135` |
| io-wq sharing | `RuntimeBuilder::attach_wq(fd)` | `builder.rs:132` |
| Setup flags | Only through `uring_builder(io_uring::Builder)` | `builder.rs` |
| Socket ops | `Recv`, `Send` (`MSG_NOSIGNAL`), `Writev` (vfs path, no `MSG_NOSIGNAL`), `SendMsg`, `Accept` (no flags), `PollAdd`, `Close`, `Splice` (feature) | `driver/op/*.rs` |
| File ops | `OpenAt`, `Read`/`Write` with offset, `Readv`/`Writev`, `Fsync`, `Statx`, `Close` | `driver/op/*.rs`, `fs/` |
| Event loop | Run ready tasks (up to 2× the queue), an `io_uring_enter` submit while tasks remain, otherwise `submit_and_wait(1)` / ext-arg timeout | `runtime.rs:128-192`, `driver/uring/mod.rs:185-336` |
| Tests | `raw_uring.rs`, `buf_ring.rs`, `send_zc.rs`, `zero_copy.rs`, TCP/UDS/UDP/TLS suites | `tests/` |

## 2. Correctness problems to fix first

| ID | Problem | Where | Effect on the transport | Fix |
|---|---|---|---|---|
| F1 | `Op::send(..).unwrap()`, `Op::writev(..).unwrap()`, `Op::poll_read(..).unwrap()` and similar: 19 `unwrap()` in `net/tcp/stream.rs` alone | `net/tcp/stream.rs:178,188,206,250,256,291,308…` | `submit_with` returns `Err` when flushing a full SQ fails (for example `EBUSY` under CQ overflow). With `panic = "abort"` the SUT dies. | Return the error through `BufResult` (buffer handed back) everywhere in `net/` and `fs/` |
| F2 | `unimplemented!("when is this hit?")` when `sq.push` fails right after a flush | `driver/uring/mod.rs:394-396` | It is hit if `submit()` drained less than one slot | Loop: tick, submit, retry; return `Err` after N tries |
| F3 | The lifecycle treats every `IORING_CQE_F_MORE` completion as "SEND_ZC, a notification will follow"; any other multishot op (`RecvMulti`, `AcceptMulti`, `PollAdd` multi) reaches `unreachable!` | `driver/uring/lifecycle.rs:106-135` | Multishot is impossible through the typed op path; it only works through the hook | Track op kind (`OpAble::KIND = Single \| ZeroCopy \| Multishot`); multishot ops queue CQEs (see A1) |
| F4 | Cancel SQEs are pushed with `let _ = ..push()`; if the second push fails the cancel is silently lost | `driver/uring/mod.rs:441-445`, `:456-459` | A timed-out op keeps running and holds the buffer; with the 10 s request timeout under load this leaks work | Same retry loop as F2; count lost cancels in the stats (A9) |
| F5 | The raw `submit` contract says a linked chain **may be split** when the SQ fills mid-chain | `driver/raw.rs:129-130` | A split chain loses its link, so a read→send chain could send before the read finished | `submit_chain` that reserves room for the whole chain first (A4) |
| F6 | Socket `writev` uses `IORING_OP_WRITEV` (the generic file write path) and therefore has no `MSG_NOSIGNAL` | `driver/op/write.rs:109` | A peer reset raises SIGPIPE (Rust binaries ignore it by default, but library users may not); it also takes a slightly longer kernel path than `sendmsg` | Use `SendMsg` with `MSG_NOSIGNAL` for sockets (A7) |
| F7 | With `setup_defer_taskrun`, the cold-path `Driver::submit()` calls `submit_and_wait(0)`. `io-uring` 0.7.15 only adds `IORING_ENTER_GETEVENTS` when the kernel sets `IORING_SQ_TASKRUN`, and the kernel only sets that if the ring was also built with `IORING_SETUP_TASKRUN_FLAG` (`io-uring-0.7.15/src/submit.rs:171-198`). The SUT's `--tune defer` sets `single_issuer + defer_taskrun` **without** `taskrun_flag` (`sut-monoio/src/main.rs:145-147`), so deferred completions only run in `park()`. **Code-read; not measured.** | `driver/uring/mod.rs:289-295`, `runtime.rs:179` | While there are always ready tasks (the facade's policy pump yields), completions wait until the queue empties. May explain why `--tune defer` showed "no difference" (runs 26–28) | Make `defer_taskrun()` in the builder always add `taskrun_flag`, or have the driver force GETEVENTS when `DEFER_TASKRUN` is set. Re-run the `defer` variant afterwards |

## 3. Features and API changes needed

Priority: **P0** blocks PIP phase 3 or the facade's allocation work; **P1** gives a measurable gain on today's workloads; **P2** is nice to have, or only pays off on a real NIC.

### A1. First-class multishot ops [P0] (PIP U4, U6)
- **Today:** one global `FnMut(u64, Cqe)` hook. The transport must keep its own id → connection table, route every CQE through it, and wake tasks itself. Ops pushed with `with_ring` also skip the driver's cancel-on-drop.
- **Ask:** a multishot op type driven by the driver. CQEs are queued per op in the slab entry (a small ring, no allocation per CQE), and the op is re-armed or ended when `F_MORE` is clear.
  ```rust
  // raw
  pub unsafe fn submit_multishot<T: Unpin + 'static>(sqe: squeue::Entry, data: T)
      -> io::Result<MultiOp<T>>;
  impl<T> MultiOp<T> {
      pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Cqe>>; // None = ended (no F_MORE)
      pub async fn next(&mut self) -> Option<Cqe>;
      pub fn cancel(&self);                                  // AsyncCancel, keep polling to drain
      pub fn into_data(self) -> Result<T, Self>;             // only after the final CQE
  }
  // typed
  impl TcpListener { pub fn accept_multi(&self, flags: AcceptFlags) -> AcceptStream; }
  impl TcpStream   { pub fn recv_multi(&self, ring: &BufRing) -> RecvStream; } // yields io::Result<SelectedBuf>
  ```
- Keep the hook for exotic ops. The transport would use the typed streams.
- **CQ sizing:** multishot posts many CQEs per SQE, and the CQ is only `2 * entries` by default. Add a builder `cq_size(n)` (see A10) and count CQ overflows in the stats (A9). Overflow is also what makes `submit` return `EBUSY` (F1).

### A2. Accept with flags, and direct descriptors [P0] (PIP U6)
- **Today:** `Accept` is built with no flags (`driver/op/accept.rs:35`), so every accepted socket needs two `fcntl` calls to set `O_NONBLOCK`.
- **Ask:**
  - `accept_with(flags)`, supporting at least `SOCK_NONBLOCK | SOCK_CLOEXEC`, also on `accept_multi`.
  - Optional `accept_direct(file_index_alloc)` that puts the new socket in the fixed file table (needs A3).
  - Return the peer address on every path (already done in `TcpListener::accept`).
- **Parity note:** mio/tokio already call `accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)`.

### A3. Fixed file table [P1] (PIP U8)
- **Today:** only the raw path (`register_files` through `with_ring`, then `types::Fixed` in hand-built SQEs). None of the typed ops can use a fixed fd.
- **Ask:**
  - `RuntimeBuilder::with_fixed_files(n)`: register a sparse table at startup, with a slot allocator in the driver.
  - Typed sockets and files carry `FdRef { Raw(RawFd) | Fixed(u32) }` inside `SharedFd`, and every op builds its SQE from it.
  - Close a fixed slot with `Close` + `file_index`.
  - Expose `TcpStream::register_fixed()` / `is_fixed()`.
- **Gain:** saves `fget`/`fput` (an atomic refcount) on every SQE. Expected a few %. Measure it.

### A4. Linked chains and per-op link timeouts [P0 for chains, P1 for timeouts] (PIP U1, request timeout)
- **Ask:**
  ```rust
  pub unsafe fn submit_chain<const N: usize, T: Unpin + 'static>(sqes: [squeue::Entry; N], data: T)
      -> io::Result<[RawOp<()>; N]>;   // or one future yielding all N CQEs; reserves N slots first
  impl TcpStream { pub async fn read_with_deadline<T: IoBufMut>(&mut self, buf: T, d: Duration) -> BufResult<usize, T>; }
  ```
- Chains: reserve space for all N first (`flush_space(N)`), then push without a submit in between (fixes F5). Typed helper for the stream path: `file.read_at(buf, off)` linked to `stream.send(buf)` (PIP U1). The fast way to build it is fixed buffers (A5), because the buffer has to be the same memory for both SQEs.
- Link timeouts: `IORING_OP_LINK_TIMEOUT` as an alternative to a timer-wheel entry plus `AsyncCancel` when a timed op is dropped. It is one extra SQE and gives a clean `-ECANCELED`.

### A5. Registered (fixed) buffers [P1] (PIP U8, U1)
- **Today:** documented as "not supported natively", raw `ReadFixed`/`WriteFixed` only.
- **Ask:**
  - `FixedBufPool::new(count, size)`, registered through `register_buffers_sparse` + `register_buffers_update`, so it can grow.
  - `FixedBuf` (owned, `IoBuf + IoBufMut`, carries `buf_index`), returned to the pool on drop.
  - Typed ops that pick the fixed opcode when they get a `FixedBuf`: `read_at_fixed`, `write_fixed`, `send_zc` with `buf_index`.
  - `FixedBuf` must be freezable into a refcounted, clonable view, so the facade's `Buf` can wrap it without copying (PIP L2).

### A6. Provided buffer ring improvements [P1] (PIP U4)
Today's `BufRing` is fixed-size, zero-filled up front, recycles one buffer at a time, and `SelectedBuf` is only `Deref<[u8]>`.

| Ask | Why |
|---|---|
| Incremental consumption (`IOU_PBUF_RING_INC`, flags argument of `register_buf_ring_with_flags`, kernel 6.12+) | Lets large buffers be shared by many small receives, so one 64 KiB buffer isn't burned per tiny request |
| `impl IoBuf for SelectedBuf` (+ `slice(range)`) | Forward or echo received bytes with no copy (proxy, repl ack paths); today they must be copied into `BytesMut` |
| `SelectedBuf::into_owned_if(cond)` / `detach()` | Keep a buffer past the callback without blocking the ring (copy-on-retain) |
| Batch recycle: write ring slots on drop, publish the tail **once per tick** | Today every drop takes a `RefCell` borrow and does a `Release` store to the shared tail (`driver/buf_ring.rs:178`). Each `SelectedBuf` also holds a cloned `BufRing` (an `Rc` increment and decrement per buffer); a plain pointer to the shared state is enough |
| Lazy memory: allocate the pool with `mmap(MAP_NORESERVE)` (optionally `MAP_HUGETLB`), not `vec![0u8; n]` | `vec![0; n]` faults in the whole pool at startup (RSS +10–20 MiB was seen in runs 26–28) |
| Page-aligned pool (comes with the `mmap` above) | `vec![0u8; n]` (`driver/buf_ring.rs:100`) only guarantees 1-byte alignment, so buffers straddle page and cache-line boundaries |
| Kernel-allocated ring (`IOU_PBUF_RING_MMAP`) as an option | No page-aligned user allocation for the ring itself |
| Counters: `enobufs`, `in_use`, `high_water` | Size the ring from data instead of guessing |
| Recv bundles (`IORING_RECVSEND_BUNDLE`, 6.10+) as an option on `recv_multi` | One CQE can cover several buffers when a large frame arrives |

### A7. Socket op flags and variants [P1] (PIP U1, U5, F6)
- `send_with_flags(buf, MsgFlags)`, used for `MSG_MORE` on the response head before a body (avoids a small segment) and for `MSG_WAITALL` on receive.
- `recv_with_flags`, plus the `IORING_RECVSEND_POLL_FIRST` ioprio flag for receives on connections known to be idle (it skips the attempt that would just return `EAGAIN` on keep-alive waits).
- `writev` on sockets through `SendMsg` with `MSG_NOSIGNAL` (F6).
- An inline vectored buffer type, so the caller needn't allocate a `Vec<iovec>` per write (PIP C1):
  ```rust
  pub struct IoVecArray<B: IoBuf, const N: usize> { bufs: [Option<B>; N], iov: Box<[libc::iovec; N]> } // reused per connection
  ```

### A8. Op futures with names (or a poll API) [P0] (PIP C1)
- **Today:** `AsyncReadRent::read` / `AsyncWriteRent::write` return anonymous `impl Future`. A connection struct can't hold one without `Box::pin`, so the facade allocates on every op.
- **Ask:**
  - Public concrete types (`RecvFuture<T>`, `SendFuture<T>`, `WritevFuture<T>`, `ReadAtFuture<T>`), each `Unpin` and holding the `Op<T>`.
  - Or a lower-level completion API, which suits a state machine like the facade's `run_conn` better:
  ```rust
  let tok = stream.start_recv(buf)?;          // pushes the SQE, returns an op token
  match stream.poll_recv(&mut tok, cx) { Poll::Ready((res, buf)) => .., Poll::Pending => .. }
  ```
- Either way: no allocation per op, and a dropped token still cancels and keeps the buffer alive (the same rules as today).

### A9. Driver statistics [P0] (PIP phase 0, U3)
`IoUringDriver::stats()`, using plain `Cell<u64>` counters (single thread):
- `io_uring_enter` calls, split into submit-only and wait;
- SQEs submitted;
- CQEs reaped;
- CQEs per tick (histogram buckets);
- SQ-full flushes;
- `EBUSY`/`EAGAIN` retries;
- cancels pushed / cancels lost (F4);
- ops by opcode.

io-wq punts can't be seen from user space. Document using the `io_uring:io_uring_queue_async_work` tracepoint for those.

### A10. Event loop and submission control [P1]
| Ask | Why |
|---|---|
| Skip `io_uring_enter` in the cold-path `submit()` when the SQ is empty and no completion work is pending. Confirmed: without SQPOLL, `io-uring` 0.7.15 always calls `enter`, even with 0 SQEs and no flags (`submit.rs:171-215`) | One saved syscall per loop iteration while tasks are always ready (the policy pump) |
| First-class builder methods: `coop_taskrun()`, `taskrun_flag()`, `single_issuer()`, `defer_taskrun()` (with the F7 fix), `register_ring_fd()` (the crate has `Submitter::register_ring_fd`, `submit.rs:658`), `cq_size(n)` | Today only through the raw `io_uring::Builder`; ring-fd registration saves an `fdget` per enter; `cq_size` is needed once multishot is used (A1) |
| Optional batching wait: `SubmitArgs::min_wait_usec` (ext-arg, 6.12+; the crate has it at `types.rs:310`) or "wait for N or T µs" | Fewer wakeups per CQE when the load is high |
| Tunable task budget (today `max_round = tasks.len() * 2`) | The facade's fairness policy and the runtime's own budget interact; it should be configurable or exposed |

### A11. Cross-thread / cross-ring wake without the `sync` feature [P1] (PIP U7)
- **Today:** without `sync` there is no cross-thread wake. The transport uses a socketpair, a ring poll, and a separate `read(2)`. With `sync`, the driver brings in flume + eventfd and stores an atomic `awake` flag twice per park.
- **Ask:**
  - `uring::msg_ring(target: &RingHandle, id: u64, res: i32)` (`IORING_OP_MSG_RING`, 5.18+). The completion shows up on the target ring's hook / multishot stream, so worker-to-worker wakes and posts don't need an fd.
  - A `RingHandle` (`Send`, wraps the ring fd) that a worker can hand out.
  - For non-ring threads: a small built-in eventfd channel read straight through the ring (one `Read` SQE kept armed), with no flume and no `sync` feature.

### A12. File-path ops for the stream workload [P1] (PIP U2, U3)
- `File::fadvise(off, len, advice)` (`IORING_OP_FADVISE`), so PIP U3 can be tested (`POSIX_FADV_RANDOM` vs `SEQUENTIAL`).
- A public file→pipe splice (`File::splice_to_pipe(&Pipe, off, len)`). Today the transport hand-writes a raw `Splice` SQE (`sut-monoio/src/main.rs:492-501`).
- `OpenOptions::custom_flags` already exists. Document `O_NOATIME`, and that `openat` runs inline only on the cached-lookup path.
- `register_iowq_max_workers(bounded, unbounded)` wrapper, to cap `iou-wrk` when splice is used. A cap is a recorded run option, never hidden.

### A13. Zero-copy send refinements [P2] (real NIC only; useless on loopback)
- Return the send result as soon as the first CQE arrives, plus a separate "buffer released" signal, so the caller can go on while the kernel still holds the buffer.
- `SEND_ZC` with a fixed buffer (`buf_index`, needs A5).
- `SENDMSG_ZC` for vectored sends.
- A size threshold helper (zero-copy below about 16 KiB is usually a loss).

### A14. Cheaper cancellation [P2]
- Dropping an in-flight op boxes its data (`Lifecycle::Ignored(Box<dyn Any>)`, `lifecycle.rs:28`) and pushes one `AsyncCancel`. Ask for:
  - `cancel_fd(&SharedFd)` (`IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_ALL`): one SQE cancels every op on a connection when it closes.
  - Storing the ignored data inline in the slab entry when it fits.

### A15. TLS staging path [P1 for TLS runs; not used by plaintext runs]
Both backends (`tls::rustls`, `tls::native`) copy through `tls/io_wrapper.rs`: a `ReadBuffer` and a `WriteBuffer` of 16 KiB each (`BUFFER_SIZE`, `tls/io_wrapper.rs:10`).

| Problem | Where | Effect | Fix |
|---|---|---|---|
| The 16 KiB staging buffer is smaller than one full TLS record (16 KiB of plaintext is ~16,406 bytes on the wire for TLS 1.3 AES-GCM) | `tls/io_wrapper.rs:10` | A full record never fits: it goes out as one 16,384-byte write and one ~22-byte write. Reads need two `recv`s per full record | Default to 64 KiB (at least 16 KiB + record overhead). Give rustls a buffer-size option like native-tls has (`read_buffer`/`write_buffer`) |
| `WriteBuffer` doesn't implement `write_vectored`. rustls' `write_tls` passes up to 64 queued records as one vectored write, so the default implementation copies only the first one | `tls/io_wrapper.rs:251`; rustls 0.23.46 `vecbuf.rs:194-207` | One io_uring write per record. Combined with the row above, 64 KiB of plaintext takes about 8 writes alternating 16,384 and ~22 bytes. Native-tls (OpenSSL BIO) shows the same pattern | Implement `write_vectored` to fill the buffer from several records. In `rustls/stream.rs` `write_io` (`:97`), call `write_tls` until the buffer is full or rustls is empty, then do one `write_all` |
| `writev` sends only the first iovec (`RawBuf::new_from_iovec` takes slice 0) | `tls/rustls/stream.rs:235`, `tls/native/stream.rs:116` | `write_vectored_all` makes one TLS record and one syscall per slice (head + body = 2 records, 2 writes) | Feed all slices to `session.writer().write_vectored(..)`, then flush once. Same idea as A7's socket `writev` |
| Every `write` flushes to the socket before returning | `tls/rustls/stream.rs:204` | Many small writes give many small records and many syscalls | Opt-in: let rustls buffer plaintext (its default limit is 64 KiB) and send only when the limit is hit or on `flush()`. This changes write semantics, so it must stay opt-in |
| Two copies in each direction: socket → staging → rustls buffer → user | `tls/rustls/stream.rs:61` (`read_io`) | One extra `memcpy` per byte on bulk transfers. rustls' deframer also reads at most 4 KiB per `read_tls` call, so CPU time grows with message size | Larger effort: move to rustls 0.23's unbuffered API, which decrypts and encrypts in place in our own buffer |

Not counted: `alpn_protocol()` allocating a `Vec` per call (cold path).

## 4. Proposed order of work in the fork

| Step | Items | Unblocks (PIP) |
|---|---|---|
| 1 | F1–F7 (correctness), A9 (stats) | Safe experiments; phase 0 measurements |
| 2 | A8 (named futures / poll API), A7 (flags, `IoVecArray`, socket writev via SendMsg), A15 rows 1–3 (TLS buffer size, `write_vectored`, real `writev`; small and local to `tls/`) | C1 allocation-free driver; U1/U5; TLS runs |
| 3 | A2 (accept flags), A1 (multishot accept/recv), A6 (bufring INC, `IoBuf` for `SelectedBuf`, batch recycle, lazy memory) | U4, U6 |
| 4 | A4 (chains, link timeout), A5 (fixed buffers), A3 (fixed files) | U1 linked read→send, U8 |
| 5 | A11 (`MSG_RING`, eventfd channel), A10 (loop control), A12 (fadvise, splice, iowq cap) | U7, U3, U2 |
| 6 | A13, A14, A15 rows 4–5 (opt-in write coalescing, rustls unbuffered API) | Real-NIC runs; cheaper close; TLS bulk throughput |

Each step: tests in the fork's `tests/` (a kernel-version check that skips when unsupported), a version bump of `adaptif-gatotkaca-monoio`, then the transport bumps the `=0.5.x` pin in the workspace `Cargo.toml`.

## 5. Rules for using fork features in the benchmark

- A fork version bump with no behaviour change on the baseline path (F1–F6 bug fixes, stats) can be adopted for every run. Say so in the next run doc.
- Every new io_uring feature is used only behind a `--tune` flag (PIP "variant") with a `docs/TUNING.md` entry. The baseline keeps `async-cancel`, `bytes`, `utils`.
- A15 rows 1–3 don't change any API, but they change how many syscalls a TLS run makes. Adopt them for every TLS run at once (not behind a `--tune` flag), and note the fork version in the run doc so TLS results from before and after aren't compared. Row 4 (write coalescing) is a `--tune` variant.
- **Parity exception:** A2 accept flags. tokio already gets these flags from `accept4`.
- Threads created by any feature (`iou-wrk-*`) are reported, never hidden. No SQPOLL.

## 6. Open questions

- ~~Which `io-uring` version does the fork pin?~~ Answered: `0.7.15`. It already exposes `register_ring_fd`, `register_files_sparse`, `register_buffers_sparse`/`_update`, `register_buf_ring_with_flags` (`IOU_PBUF_RING_INC`), `RECVSEND_BUNDLE` feature detection, `min_wait_usec`, `register_iowq_max_workers` and `register_sync_cancel`. None of A3/A5/A6/A10/A12 needs a crate bump; the work is wrapping them in the fork.
- F7: confirm with A9 stats (count completions handled in `submit()` vs `park()` with `--tune defer`, with and without `taskrun_flag`).
- A15: does any PIP workload run over TLS? If none does, A15 drops to P2. Its syscall counts are code-read only; confirm them with `strace -c` or the A9 stats on a 64 KiB TLS echo before and after.
- A8: named future types or a poll-style token API? The token API fits the facade driver better; named futures fit ordinary async code better. Possibly both.
