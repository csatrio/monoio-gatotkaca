# r3-probe: kernel behaviour for round 3 (KV1–KV5)

Package r3-probe of wave 0. It answers the five kernel statements of
[ROUND3-PLAN.md](../ROUND3-PLAN.md) ("Kernel statements to verify in wave 0") with the kernel source and
with probes on the running kernel.

- **Probes:** `monoio/tests/kernel_probe_r3.rs`, six `#[ignore]` tests that print what they see. Each drives
  a raw ring of its own (`monoio::io_uring::IoUring`) with plain sockets, so monoio's driver is not involved.

  ```sh
  cargo test -p adaptif-gatotkaca-monoio --test kernel_probe_r3 -- --ignored --nocapture --test-threads=1
  ```

- **Running kernel:** `7.0.0-42-generic` (Ubuntu). All probe output below is from this kernel, one run
  (29 s), with the test binary of this commit.
- **Source read:** mainline **Linux v7.0** (tag `v7.0` of torvalds/linux, `Makefile`: 7.0.0), fetched from
  `raw.githubusercontent.com`. Files: `io_uring/{io_uring.c, io_uring.h, tw.c, tw.h, wait.c, net.c, timeout.c,
  poll.c}`, `include/uapi/linux/io_uring.h`, `include/linux/io_uring_types.h`. In v7.0 the deferred task-work
  code is in `io_uring/tw.c` and the wait path in `io_uring/wait.c`, not in `io_uring.c`.
  For two "since when" questions I also read `io_uring/io_uring.c` of v6.11, v6.12, v6.13 and the uapi header
  of v6.16 and v6.17.
- **Not read:** the Ubuntu source of 7.0.0-42 (no local tree; `/usr/src` has headers only). Every source
  statement the verdicts rest on is confirmed by a probe on the running kernel. The few that are from the
  source alone are marked "source only".

## Summary

| id | verdict | what is really true | consequence |
|---|---|---|---|
| KV1 | partly | The cap is max(20, `min_complete`) task-work items per *run*, since **6.13** (not 6.12), and the flag is set again when items are left. But one enter can make two runs, and after a sleep the run is capped at the number of CQEs still missing: **one** for `min_complete = 1` | D1: one drain enter delivers ≤ 20; the flag is reliable; `want = cq.len()` works only before the CQ is reaped |
| KV2 | true | A failed member with `CQE_SKIP_SUCCESS` posts its CQE, the members after it post nothing. The pair posts **exactly one CQE in every case** | C1: one slot, freed at the first CQE; a tag bit is needed |
| KV3 | true, with two additions | `MSG_WAITALL` retries until all is sent; a send that ends short is failed and reports the **short count**, not the errno. After 127 retries the send moves to io-wq, and a linked `RECV(POLL_FIRST)` then fails with **`-EAGAIN`** | C1: `MSG_WAITALL` is required; handle `-EAGAIN` on the receive |
| KV4 | true without SQPOLL, false with SQPOLL | `msghdr` and iovec array are copied when the request is prepared: inside `io_uring_enter`, or by the SQ thread on an SQPOLL ring | A1: keep both until the op's CQE |
| KV5 | true | `IORING_SEND_VECTORIZED = 1U << 5` in `sqe->ioprio`, since 6.17; works on 7.0 for `SEND` and `SEND_ZC`, TCP and Unix | A1: a vectored send needs no `msghdr` at all on 6.17+ |

Findings nobody asked for, which change a design (details in the sections):

1. **D1 mechanism.** A waiting enter that sleeps with `min_complete = 1` runs exactly one task-work item
   when it wakes. This is the cause of "52% same tick", more exactly than the hypothesis in ROUND3.md.
2. **C1 on `DEFER_TASKRUN`.** The receive of a linked pair is started by a deferred task-work item. It costs
   one more item per frame than the unlinked pair, and counts against the cap of 20.
3. **C1 and io-wq.** `RECV(POLL_FIRST)` that runs in an io-wq worker fails with `-EAGAIN`.
4. **C1 cancel.** A cancel by `user_data` finds only the member that is in flight.
5. **A1.** The kernel adds `MSG_NOSIGNAL` to every `SEND`/`SENDMSG` itself (source only).

---

## KV1: deferred task work per enter on a `DEFER_TASKRUN` ring

**Verdict: partly true.** One *run* of deferred task work handles at most max(20, `min_complete`) items and
sets `IORING_SQ_TASKRUN` again when items are left; this cap exists since 6.13. But the number of runs and
their caps depend on the enter:

| enter | runs | items at most |
|---|---|---|
| `to_submit = 0`, `GETEVENTS`, `min_complete = m` | one, when the wait begins | max(20, m) |
| `to_submit > 0`, `GETEVENTS`, `min_complete = m` | two: after the submit and when the wait begins | 2 × max(20, m) |
| any enter without `GETEVENTS` | none | 0 (the flag stays set) |
| the enter slept (fewer than m CQEs after those runs) | one more per wake-up | m − (CQEs in the CQ): **1 for m = 1** |
| `GETEVENTS \| EXT_ARG` with a zero timeout, large m | as above, never sleeps | max(20, m) |

- `enter(0, 0, GETEVENTS)` does not block: it runs up to 20 items and returns.
- `min_complete = N` with ≥ N unreaped CQEs in the CQ returns at once and runs up to max(20, N) items first.

### Source (v7.0)

`io_uring/tw.h`:

```c
#define IO_LOCAL_TW_DEFAULT_MAX		20
```

`io_uring/tw.c`, the run. The flag is cleared at the start; `max_events` caps the items; the run repeats only
while it handled fewer than `min_events` and more is pending; if it stops with work left, it sets the flag:

```c
static bool io_run_local_work_continue(struct io_ring_ctx *ctx, int events, int min_events)
{
	if (!io_local_work_pending(ctx))
		return false;
	if (events < min_events)
		return true;
	if (ctx->flags & IORING_SETUP_TASKRUN_FLAG)
		atomic_or(IORING_SQ_TASKRUN, &ctx->rings->sq_flags);
	return false;
}

static int __io_run_local_work(struct io_ring_ctx *ctx, io_tw_token_t tw, int min_events, int max_events)
{
	...
	if (ctx->flags & IORING_SETUP_TASKRUN_FLAG)
		atomic_andnot(IORING_SQ_TASKRUN, &ctx->rings->sq_flags);
again:
	min_events -= ret;
	ret = __io_run_local_work_loop(&ctx->retry_llist.first, tw, max_events);
	if (ctx->retry_llist.first)
		goto retry_done;
	node = llist_reverse_order(llist_del_all(&ctx->work_llist));
	ret += __io_run_local_work_loop(&node, tw, max_events - ret);
	ctx->retry_llist.first = node;          /* what was not run stays here */
	...
```

The flag is also set when an item is queued to an empty list (`io_req_local_work_add`: `if (!head)
io_ctx_mark_taskrun(ctx);`).

`io_uring/io_uring.c`, `SYSCALL_DEFINE6(io_uring_enter, ...)`, first run, only with `to_submit > 0`:

```c
		ret = io_submit_sqes(ctx, to_submit);
		...
		if (flags & IORING_ENTER_GETEVENTS) {
			...
			if (ctx->flags & IORING_SETUP_DEFER_TASKRUN)
				(void)io_run_local_work_locked(ctx, min_complete);
```

with `io_run_local_work_locked(ctx, min_events)` = `__io_run_local_work(ctx, ts, min_events,
max(IO_LOCAL_TW_DEFAULT_MAX, min_events))`.

`io_uring/wait.c`, `io_cqring_wait()`, second run, then the test that decides about sleeping, then the run
after each wake-up, capped at `nr_wait`:

```c
	if (io_local_work_pending(ctx))
		io_run_local_work(ctx, min_events, max(IO_LOCAL_TW_DEFAULT_MAX, min_events));
	...
	if (__io_cqring_events_user(ctx) >= min_events) {
		rcu_read_unlock();
		return 0;
	}
	...
	iowq.cq_tail = READ_ONCE(rings->cq.head) + min_events;
	nr_wait = (int) iowq.cq_tail - READ_ONCE(rings->cq.tail);
	...
	do {
		...
		ret = io_cqring_wait_schedule(ctx, &iowq, ext_arg, start_time);
		...
		if (io_local_work_pending(ctx))
			io_run_local_work(ctx, nr_wait, nr_wait);
```

It returns `-ETIME` only when the CQ is empty (`return cq.head == cq.tail ? ret : 0;`).

**Since when:** `IO_LOCAL_TW_DEFAULT_MAX` is in `io_uring/io_uring.c` of v6.13 and not of v6.12 or v6.11. In
v6.12 `__io_run_local_work(ctx, ts, min_events)` has no cap: one run takes everything that is pending.

### Probe (`kv1_defer_taskrun_local_work`)

Ring: `SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG`. Before each scenario 200 `RECV(POLL_FIRST)` are armed
and one byte is written to each peer: 200 pending items, each posts one CQE when it is run.

```text
KV1 (kernel 7.0.0-42-generic): DEFER_TASKRUN ring, 200 RECV(POLL_FIRST) made ready before each scenario (one task-work item and one CQE each)
  [flag] IORING_SQ_TASKRUN around one pending item
    idle, receives armed: SQ_TASKRUN false
    after one peer wrote: SQ_TASKRUN true, CQ 0
    no GETEVENTS: enter(to_submit=0, min_complete=0, 0) -> 0: CQ 0 -> 0 (+0), SQ_TASKRUN after: true, 1 us
    GETEVENTS: enter(to_submit=0, min_complete=0, GETEVENTS) -> 0: CQ 0 -> 1 (+1), SQ_TASKRUN after: false, 6 us
  [A] repeated enter(to_submit=0, min_complete=0, GETEVENTS), CQ reaped after each
    before: CQ 0, SQ_TASKRUN true
    10 enters in 179 us; CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20, 20, 20]
    SQ_TASKRUN after each [true, true, true, true, true, true, true, true, true, false]
  [B] to_submit=0, GETEVENTS, min_complete > 0, CQ empty before
    min_complete=1: enter(to_submit=0, min_complete=1, GETEVENTS) -> 0: CQ 0 -> 20 (+20), SQ_TASKRUN after: true, 17 us
    rest: 9 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20, 20]
    min_complete=19: enter(to_submit=0, min_complete=19, GETEVENTS) -> 0: CQ 0 -> 20 (+20), SQ_TASKRUN after: true, 17 us
    rest: 9 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20, 20]
    min_complete=21: enter(to_submit=0, min_complete=21, GETEVENTS) -> 0: CQ 0 -> 21 (+21), SQ_TASKRUN after: true, 18 us
    rest: 9 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20, 19]
    min_complete=50: enter(to_submit=0, min_complete=50, GETEVENTS) -> 0: CQ 0 -> 50 (+50), SQ_TASKRUN after: true, 41 us
    rest: 8 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 10]
    min_complete=200: enter(to_submit=0, min_complete=200, GETEVENTS) -> 0: CQ 0 -> 200 (+200), SQ_TASKRUN after: false, 161 us
    rest: 0 more enter(0, 0, GETEVENTS), CQEs per enter []
  [C] one NOP submitted in the same enter
    NOP, no GETEVENTS: enter(to_submit=1, min_complete=0, 0) -> 1: CQ 0 -> 1 (+1), SQ_TASKRUN after: true, 5 us
    rest: 10 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20, 20, 20]
    NOP, GETEVENTS, min_complete=0: enter(to_submit=1, min_complete=0, GETEVENTS) -> 1: CQ 0 -> 41 (+41), SQ_TASKRUN after: true, 33 us
    rest: 8 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20]
    NOP, GETEVENTS, min_complete=1: enter(to_submit=1, min_complete=1, GETEVENTS) -> 1: CQ 0 -> 41 (+41), SQ_TASKRUN after: true, 33 us
    rest: 8 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20]
    NOP, GETEVENTS, min_complete=50: enter(to_submit=1, min_complete=50, GETEVENTS) -> 1: CQ 0 -> 101 (+101), SQ_TASKRUN after: true, 81 us
    rest: 5 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20]
  [D] min_complete = number of unreaped CQEs in the CQ (nothing reaped in between)
    first: enter(to_submit=0, min_complete=0, GETEVENTS) -> 0: CQ 0 -> 20 (+20), SQ_TASKRUN after: true, 17 us
    next: enter(to_submit=0, min_complete=20, GETEVENTS) -> 0: CQ 20 -> 40 (+20), SQ_TASKRUN after: true, 16 us
    next: enter(to_submit=0, min_complete=40, GETEVENTS) -> 0: CQ 40 -> 80 (+40), SQ_TASKRUN after: true, 32 us
    next: enter(to_submit=0, min_complete=80, GETEVENTS) -> 0: CQ 80 -> 160 (+80), SQ_TASKRUN after: true, 64 us
    next: enter(to_submit=0, min_complete=160, GETEVENTS) -> 0: CQ 160 -> 200 (+40), SQ_TASKRUN after: false, 33 us
    next: enter(to_submit=0, min_complete=200, GETEVENTS) -> 0: CQ 200 -> 200 (+0), SQ_TASKRUN after: false, 0 us
    rest: 0 more enter(0, 0, GETEVENTS), CQEs per enter []
  [E] all 200 CQEs delivered but not reaped, no task work pending: does the enter sleep?
    min_complete = CQ length: enter(to_submit=0, min_complete=200, GETEVENTS|EXT_ARG ts=2s) -> 0: CQ 200 -> 200 (+0), SQ_TASKRUN after: false, 1 us
    min_complete = CQ length + 1: enter(to_submit=0, min_complete=201, GETEVENTS|EXT_ARG ts=300ms) -> 0: CQ 200 -> 200 (+0), SQ_TASKRUN after: false, 300285 us
  [F] the enter sleeps first (idle ring), a second thread writes to all peers after 50 ms
    min_complete=1: enter(to_submit=0, min_complete=1, GETEVENTS|EXT_ARG ts=5s) slept 50 ms -> 0: CQ 0 -> 1, SQ_TASKRUN after: true
    rest: 10 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20, 20, 19]
    min_complete=8: enter(to_submit=0, min_complete=8, GETEVENTS|EXT_ARG ts=5s) slept 50 ms -> 0: CQ 0 -> 8, SQ_TASKRUN after: true
    rest: 10 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 20, 20, 20, 12]
    min_complete=64: enter(to_submit=0, min_complete=64, GETEVENTS|EXT_ARG ts=5s) slept 50 ms -> 0: CQ 0 -> 64, SQ_TASKRUN after: true
    rest: 7 more enter(0, 0, GETEVENTS), CQEs per enter [20, 20, 20, 20, 20, 20, 16]
  [G] zero timeout (EXT_ARG, ts=0) with a large min_complete: never sleeps
    min_complete=200: enter(to_submit=0, min_complete=200, GETEVENTS|EXT_ARG ts=0ns) -> 0: CQ 0 -> 200 (+200), SQ_TASKRUN after: false, 174 us
    rest: 0 more enter(0, 0, GETEVENTS), CQEs per enter []
    min_complete=1000: enter(to_submit=0, min_complete=1000, GETEVENTS|EXT_ARG ts=0ns) -> 0: CQ 0 -> 200 (+200), SQ_TASKRUN after: false, 171 us
    rest: 0 more enter(0, 0, GETEVENTS), CQEs per enter []
  [H] cost of an enter that finds nothing (screening: the machine is not quiet)
    enter(0, 0, 0): 203 ns; enter(0, 0, GETEVENTS): 257 ns; enter(0, 4096, GETEVENTS|EXT_ARG ts=0): 7048 ns (20000 of 20000 returned -ETIME)
```

How the lines map to the table: [A] 20 per run and the flag; [B] max(20, m); [C] two runs with a submit
(1 NOP + 20 + 20 = 41; 1 + 50 + 50 = 101) and none without `GETEVENTS`; [D]/[E] `min_complete` ≤ CQ length
never sleeps; [F] after a sleep exactly `min_complete` items (1, 8, 64) although 200 become ready.

### Consequence for D1 (`drain_taskrun(max)`)

1. **Why `defer` returns too early.** `inner_park` enters with `want = 1`. If the enter has to sleep, the
   wake-up run is capped at one item: one completion is delivered and the enter returns, whatever else became
   ready meanwhile ([F]: 1 of 200, flag set). If it does not sleep, it delivers up to 20 (40 with a submit).
   This replaces the hypothesis in ROUND3.md ("runs the task work once, when the wait begins").
2. **What one drain enter delivers.** `enter(0, 0, GETEVENTS)` runs at most 20 task-work items. An item is
   one deferred step of one request, so at most 20 completions; steps that post no CQE count too (the start
   of a linked request, see KV2). `max` is therefore in units of "20 items", and the doc comment should say
   "kernel 6.13+; older kernels run everything in one enter".
3. **The flag is reliable for "work is left".** It was set after every run that left items and clear after
   the last one ([A]). By the source it can be set for a moment with no work left (an item added while a run
   starts), which costs one empty enter (about 0.26 µs, [H]), never the reverse for work a run left behind.
   So `while n < max && sq.taskrun()` is the right loop condition.
   **Lead's note after r3-drain:** this holds for `min_complete <= 1`. A run that was asked for more events
   than it handled (`min_wait`, or a drain with `min_complete = cq.len()`) loops inside the kernel, and an
   item queued during that loop sets the flag and is handled by the same run, so the flag can stay set with
   nothing pending (r3-drain.md: most drain enters after `minwait80` came back empty).
4. **`want = cq.len()` is valid, with two conditions.**
   - It never sleeps as long as `want` ≤ the CQEs that are in the CQ and not reaped ([D], [E]), and it runs up
     to max(20, `want`) items.
   - It only helps **before the CQ is reaped**. The plan puts the drain "after the first `tick()`": there
     `cq.len()` is 0 and the call is the plain drain. The order has to be: waiting enter, drain enter with
     `want = cq.len()`, then `tick()`.
   - The gain is a doubling, not "everything": 20, +20, +40, +80 ([D]). After a sleeping wake-up `cq.len()` is
     1, so the first drain still delivers 20.
   - `UringInner::enter` treats `want > 0` as a waiting enter: it replaces `want` by `min_wait_batch` and adds
     `min_wait_usec` when `min_wait` is configured, and counts `enter_wait`. The drain must not go through
     that path.
5. **One syscall that drains everything:** `GETEVENTS | EXT_ARG` with a zero timespec and a large
   `min_complete` ([G]: 200 of 200, also with `min_complete = 1000`). It never sleeps. When fewer than
   `min_complete` CQEs exist afterwards it goes through the timer path: about 7 µs here against 0.26 µs for an
   empty `GETEVENTS` enter ([H], screening). Worth a measurement at R2000 (a backlog of 2000 is 100 plain
   drain enters), not at 9 connections.

---

## KV2: `CQE_SKIP_SUCCESS` in a link chain

**Verdict: true.** If a member with `IOSQE_CQE_SKIP_SUCCESS` fails, the kernel posts that member's CQE and
none for the members after it. ROUND3.md C1 ("a failed link always posts a CQE for every remaining member")
is true only when the failed member has no `CQE_SKIP_SUCCESS`. The pair
`SEND(IO_LINK | CQE_SKIP_SUCCESS)` → `RECV` posts exactly one CQE in every case.

### Source (v7.0)

`io_uring/io_uring.h`:

```c
static inline void req_set_fail(struct io_kiocb *req)
{
	req->flags |= REQ_F_FAIL;
	if (req->flags & REQ_F_CQE_SKIP) {
		req->flags &= ~REQ_F_CQE_SKIP;
		req->flags |= REQ_F_SKIP_LINK_CQES;
	}
}
```

`io_uring/timeout.c`, `io_fail_links()`: the failed member decides for all members after it, whatever their
own flag was:

```c
	bool ignore_cqes = req->flags & REQ_F_SKIP_LINK_CQES;
	...
	while (link) {
		if (ignore_cqes)
			link->flags |= REQ_F_CQE_SKIP;
		else
			link->flags &= ~REQ_F_CQE_SKIP;
```

A member rejected when it is prepared: `io_uring/io_uring.c`, `io_submit_fail_init()` marks that member and
the **head** as failed, the head with `-ECANCELED`:

```c
	req_fail_link_node(req, ret);
	if (head && !(head->flags & REQ_F_FAIL))
		req_fail_link_node(head, -ECANCELED);
```

`IORING_OP_SEND_ZC` cannot be such a head, `io_send_zc_prep()`:

```c
	/* we don't support IOSQE_CQE_SKIP_SUCCESS just yet */
	if (req->flags & REQ_F_CQE_SKIP)
		return -EINVAL;
```

### Probe (`kv2_skip_success_in_link_chains`)

Loopback TCP. `user_data` 1 = send (or m1), 2 = recv (or m2), 3 = m3, 9 = the cancel SQE. After the CQEs
shown, each case waited 200 ms more for further CQEs.

```text
KV2 (kernel 7.0.0-42-generic): pair = SEND(IO_LINK|CQE_SKIP_SUCCESS, MSG_WAITALL) -> RECV(POLL_FIRST) on a loopback TCP socket; CQEs in CQ order
  (a1) send ok, reply already in the socket: 1 CQE [recv: 4]; peer got "hello"
  (a2) send ok, reply arrives later: before the reply 0 CQEs, peer got "hello"; after the reply 1 CQE [recv: 4]
  (b1) send fails at issue (local SHUT_WR): 1 CQE [send: -EPIPE]; after the peer wrote 0 CQEs
  (b2) send fails at issue (peer reset): 1 CQE [send: -ECONNRESET]
  (c)  short send / full socket buffer: see KV3
  (d1) SEND rejected at prep (invalid ioprio flag): 1 CQE [send: -EINVAL]; peer got nothing
  (d2) SEND on fd -1: 1 CQE [send: -EBADF]
  (d3) valid SEND, RECV rejected at prep (invalid ioprio flag): 1 CQE [send: -ECANCELED]; peer got nothing
  (d4) valid SEND, RECV on fd -1: 1 CQE [recv: -EBADF]; peer got "hello"
  (d5) head is SEND_ZC(IO_LINK|CQE_SKIP_SUCCESS): 1 CQE [send: -EINVAL]; peer got nothing
  (e1) send ok, peer had shut down its write side (EOF): 1 CQE [recv: 0]; peer got "hello"
  (e2) send ok, RECV fails at issue (unmapped buffer): 1 CQE [recv: -EFAULT]; peer got "hello"
  (e3) send ok, then the peer resets while the RECV waits: before 0 CQEs, peer got "hello"; after 1 CQE [recv: -ECONNRESET]
  chains of three: m1 = SEND, m2 = SEND, m3 = RECV(POLL_FIRST); 'x' is a socket whose write side is shut down
  (f1) SKIP on m1 and m2, all succeed: 1 CQE [m3: 4]; peer got "onetwo"
  (f2) SKIP on m1 and m2, m2 fails: 1 CQE [m2: -EPIPE]; peer got "one"
  (f3) SKIP on m1 and m2, m1 fails: 1 CQE [m1: -EPIPE]; peer got nothing
  (f4) SKIP on m1 and m2, m2 rejected at prep: 1 CQE [m1: -ECANCELED]; peer got nothing
  (f5) SKIP on m2 only, m2 fails: 2 CQEs [m1: 3, m2: -EPIPE]; peer got "one"
  (f6) SKIP on m2 only, m1 fails: 3 CQEs [m1: -EPIPE, m2: -ECANCELED, m3: -ECANCELED]; peer got nothing
  (f7) SKIP on m1 only, m2 fails: 2 CQEs [m2: -EPIPE, m3: -ECANCELED]; peer got "one"
  control: the same pair with IO_LINK only (no CQE_SKIP_SUCCESS)
  (g1) send fails at issue (local SHUT_WR): 2 CQEs [send: -EPIPE, recv: -ECANCELED]
  (g2) send ok, reply already in the socket: 2 CQEs [send: 5, recv: 4]
  (g3) SEND rejected at prep: 2 CQEs [send: -EINVAL, recv: -ECANCELED]
  (g4) valid SEND, RECV rejected at prep: 2 CQEs [send: -ECANCELED, recv: -EINVAL]
  cancel (IORING_OP_ASYNC_CANCEL by user_data, its own CQE is 'cancel') while the RECV of the pair waits
  (h1) before 0 CQEs; cancel(user_data of the send): 1 CQE [cancel: -ENOENT]; cancel(user_data of the recv): 2 CQEs [cancel: 0, recv: -ECANCELED]; after the peer wrote 0 CQEs
```

Notes on single lines:

- (d2): a bad fd is found when the request is issued, not when it is prepared (`io_assign_file` in
  `io_issue_sqe`). The pattern is the same as for any failed send.
- (d3)/(f4): when a *later* member is rejected at prep, the only CQE is the head's with `-ECANCELED`. The real
  error (`-EINVAL`) is not reported and the send did not run.

### Probe (`kv2_pair_on_defer_taskrun`): the pair on a `DEFER_TASKRUN` ring

```text
KV2 on DEFER_TASKRUN (kernel 7.0.0-42-generic): 50 Unix stream connections, reply already in the socket; how many deferred task-work runs until the receives complete
    linked pair, SEND(IO_LINK|CQE_SKIP_SUCCESS): submitting enter (no GETEVENTS) 0 CQEs, SQ_TASKRUN true; then enter(0, 0, GETEVENTS) x 5: CQEs per enter [0, 0, 10, 20, 20]; all 50 are [recv: 4]: true
    unlinked, SEND(CQE_SKIP_SUCCESS): submitting enter (no GETEVENTS) 0 CQEs, SQ_TASKRUN true; then enter(0, 0, GETEVENTS) x 3: CQEs per enter [20, 20, 10]; all 50 are [recv: 4]: true
```

The next member of a link is not issued inline. `io_free_batch_list()` → `io_queue_next()` →
`io_req_task_queue(nxt)` queues it as task work. On a `DEFER_TASKRUN` ring that is a deferred item: it needs
an enter with `GETEVENTS` and counts against the cap of KV1. So a linked pair costs one item to start its
receive and one for the receive's completion; the unlinked pair costs one.

### Consequence for C1 (`send_then_recv`), together with KV3

The pair is SEND `IO_LINK | CQE_SKIP_SUCCESS` with `MSG_WAITALL`, then RECV with `POLL_FIRST`.

- **(i) When is the pair finished?** At its first CQE. Exactly one CQE is posted in every case: success,
  failed send, short send, send or receive rejected at prep, EOF, failed receive, cancel. The slot is freed
  there.
- **(ii) Can a stray CQE arrive later?** No. In every case nothing more arrived, and by the source the
  members after a failed `SKIP` member get `REQ_F_CQE_SKIP`. The cancel SQE's own CQE is separate, as today.
- **(iii) Is a tag bit enough to tell the two?** Yes, and it is needed. The result value cannot tell them
  apart: both members report `-ECONNRESET`, `-EBADF`, `-ECANCELED`, and a failed send can report a positive
  count (KV3). Order cannot help either, there is only one CQE.
- **(iv) Does "no CQE for the send" mean "fully sent"?** Only with `MSG_WAITALL` (KV3, case 3).

How to read the one CQE:

| CQE from | `res` | meaning |
|---|---|---|
| RECV | ≥ 0, or an error other than `-EAGAIN` | the send is complete; `res` is the receive result |
| RECV | `-EAGAIN` | the send is complete; the receive did not run (KV3, case 7). Issue a plain receive |
| SEND | < 0 | nothing of this send went out; the receive did not run. `-ECANCELED` also stands for "the RECV SQE was rejected at prep" (d3) |
| SEND | 0 ≤ `res` < len | `res` bytes went out, then the send failed or was cancelled. The errno is lost (KV3, cases 5 and 6) |

Three more things the implementation needs:

- **Cancel.** `ASYNC_CANCEL` by `user_data` finds only the member that is in flight: (h1) and KV3 case 6 give
  `-ENOENT` for the other one. With a tag bit in `user_data` the drop path has to cancel both values (or use
  the fd form).
- **`DEFER_TASKRUN`.** One more task-work item per frame than `send_op` + `recv_poll_first_op` (probe above),
  for one CQE less. Gate G2 should measure the pair on `coop` and on `defer`.
- **Zero-copy.** `SEND_ZC` cannot be the head (d5).

**General chains.** With `CQE_SKIP_SUCCESS` on **all members but the last** there is one simple rule: exactly
one CQE. It is the last member's if all others succeeded, otherwise that of the first member that failed;
if a member was rejected at prep, it is the head's (`-ECANCELED`, or the head's own error). Members before
the reporting one succeeded, members after it did not run. `submit_chain` can accept this form if every
member gets its own tag.
With `CQE_SKIP_SUCCESS` on only some members there is no simple rule: (f5)–(f7) give 2, 3 and 2 CQEs, and in
(f6) a member *with* the flag posts `-ECANCELED` because an earlier member without it failed. `validate`
should keep rejecting that form.

---

## KV3: stream `SEND` with `MSG_WAITALL`

**Verdict: true, with two additions.** With `MSG_WAITALL` the kernel retries a short send inside the request
(poll, then send the rest) until all is sent, and a send that ends short is marked failed, which breaks the
link. Additions: (1) the failed send reports the bytes sent so far, not the errno; (2) after 127 retries the
request moves to an io-wq worker, and a linked `RECV(POLL_FIRST)` then fails with `-EAGAIN`.
Without `MSG_WAITALL` a short send is a success: with `CQE_SKIP_SUCCESS` no CQE is posted for it.

### Source (v7.0)

`io_uring/net.c`, `io_send()` (`io_sendmsg()` is the same):

```c
	if (flags & MSG_WAITALL || sr->flags & IORING_RECVSEND_BUNDLE)
		min_ret = iov_iter_count(&kmsg->msg.msg_iter);
	...
	ret = sock_sendmsg(sock, &kmsg->msg);
	if (ret < min_ret) {
		if (ret == -EAGAIN && (issue_flags & IO_URING_F_NONBLOCK))
			return -EAGAIN;
		if (ret > 0 && io_net_retry(sock, flags)) {     /* MSG_WAITALL on a stream socket */
			sr->len -= ret;
			sr->buf += ret;
			sr->done_io += ret;
			return io_net_kbuf_recyle(req, sel.buf_list, kmsg, ret);   /* IOU_RETRY */
		}
		...
		req_set_fail(req);
	}
	if (ret >= 0)
		ret += sr->done_io;
	else if (sr->done_io)
		ret = sr->done_io;
```

Without `MSG_WAITALL`, `min_ret` is 0, so a short positive result never reaches `req_set_fail`.

`io_uring/poll.c`: each retry arms the poll again, at most 127 times; then `io_arm_poll_handler` gives up and
`io_queue_async()` sends the request to io-wq:

```c
#define APOLL_MAX_RETRY		128
...
	if (unlikely(!--apoll->poll.retries))
		return NULL;
```

`io_uring/io_uring.c`: a request that completes in io-wq hands its link to the same worker
(`io_wq_free_work()` returns `io_req_find_next(req)`), and there `-EAGAIN` is final for a request that is
not `REQ_F_FORCE_ASYNC`. `io_recv()` returns `-EAGAIN` for `POLL_FIRST` before it was polled:

```c
	if (!(req->flags & REQ_F_POLLED) && (sr->flags & IORING_RECVSEND_POLL_FIRST))
		return -EAGAIN;
```

```c
	/* io_wq_submit_work() */
		ret = io_issue_sqe(req, issue_flags);
		if (ret != -EAGAIN)
			break;
		...
		if (!needs_poll) {
			if (!(req->ctx->flags & IORING_SETUP_IOPOLL))
				break;
	...
	if (ret)
		io_req_task_queue_fail(req, ret);
```

### Probe (`kv3_stream_send_waitall`)

`SO_SNDBUF` and `SO_RCVBUF` 4096, a send of 1 MiB, the peer reads only when the line says so.

```text
KV3 (kernel 7.0.0-42-generic): stream SEND larger than the socket buffer; pair = SEND(IO_LINK|CQE_SKIP_SUCCESS) -> RECV(POLL_FIRST)
  Tcp stream, SO_SNDBUF/SO_RCVBUF 4096, SEND of 1048576 bytes, the peer does not read at first
    (1) SEND, no MSG_WAITALL: while the peer is idle 1 CQE [send: 6144]; then the peer read 6144 bytes; later 0 CQEs
    (2) SEND, MSG_WAITALL: while the peer is idle 0 CQEs; then the peer read 1048576 bytes (intact); after that 1 CQE [send: 1048576]
    (3) pair, SEND(LINK|SKIP) without MSG_WAITALL: while the peer is idle 0 CQEs; the peer could read 6144 of 1048576 bytes, then 0 CQEs; after the peer replied 1 CQE [recv: 4]
    (4) pair, SEND(LINK|SKIP) with MSG_WAITALL: while the peer is idle 0 CQEs; the peer read 1048576 bytes (intact), then 1 CQE [recv: -EAGAIN]; after the peer replied 0 CQEs
    (5) pair with MSG_WAITALL, the peer reads 102400 bytes and then closes (TCP: reset): before 0 CQEs; after 1 CQE [send: 110592]
    (6) pair with MSG_WAITALL, cancelled while the send waits for socket space: before 0 CQEs; cancel(user_data of the recv): 1 CQE [cancel: -ENOENT]; cancel(user_data of the send): 2 CQEs [cancel: 0, send: 12288]; the peer could read 12288 bytes; after the peer replied 0 CQEs
  Unix stream, SO_SNDBUF/SO_RCVBUF 4096, SEND of 1048576 bytes, the peer does not read at first
    (1) SEND, no MSG_WAITALL: while the peer is idle 1 CQE [send: 8064]; then the peer read 8064 bytes; later 0 CQEs
    (2) SEND, MSG_WAITALL: while the peer is idle 0 CQEs; then the peer read 1048576 bytes (intact); after that 1 CQE [send: 1048576]
    (3) pair, SEND(LINK|SKIP) without MSG_WAITALL: while the peer is idle 0 CQEs; the peer could read 8064 of 1048576 bytes, then 0 CQEs; after the peer replied 1 CQE [recv: 4]
    (4) pair, SEND(LINK|SKIP) with MSG_WAITALL: while the peer is idle 0 CQEs; the peer read 1048576 bytes (intact), then 1 CQE [recv: -EAGAIN]; after the peer replied 0 CQEs
    (5) pair with MSG_WAITALL, the peer reads 104832 bytes and then closes (TCP: reset): before 0 CQEs; after 1 CQE [send: 112896]
    (6) pair with MSG_WAITALL, cancelled while the send waits for socket space: before 0 CQEs; cancel(user_data of the recv): 1 CQE [cancel: -ENOENT]; cancel(user_data of the send): 2 CQEs [cancel: 0, send: 8064]; the peer could read 8064 bytes; after the peer replied 0 CQEs
  (7) Tcp pair with MSG_WAITALL by send size (every retry of the send re-arms its poll; RECV ioprio 1 = POLL_FIRST)
    send of   16384 bytes, RECV ioprio 1: before 0 CQEs; the peer read 16384 bytes (intact), then 0 CQEs, io-wq workers 0; after the peer replied 1 CQE [recv: 4]
    send of  131072 bytes, RECV ioprio 1: before 0 CQEs; the peer read 131072 bytes (intact), then 0 CQEs, io-wq workers 0; after the peer replied 1 CQE [recv: 4]
    send of  262144 bytes, RECV ioprio 1: before 0 CQEs; the peer read 262144 bytes (intact), then 0 CQEs, io-wq workers 0; after the peer replied 1 CQE [recv: 4]
    send of  524288 bytes, RECV ioprio 1: before 0 CQEs; the peer read 524288 bytes (intact), then 0 CQEs, io-wq workers 0; after the peer replied 1 CQE [recv: 4]
    send of  786432 bytes, RECV ioprio 1: before 0 CQEs; the peer read 786432 bytes (intact), then 1 CQE [recv: -EAGAIN], io-wq workers 1; after the peer replied 0 CQEs
    send of 1048576 bytes, RECV ioprio 1: before 0 CQEs; the peer read 1048576 bytes (intact), then 1 CQE [recv: -EAGAIN], io-wq workers 1; after the peer replied 0 CQEs
    send of 1048576 bytes, RECV ioprio 0: before 0 CQEs; the peer read 1048576 bytes (intact), then 0 CQEs, io-wq workers 1; after the peer replied 1 CQE [recv: 4]
```

- (1) and (3): without `MSG_WAITALL` the send ends short at once (6144 of 1 MiB). In the pair that short send
  is skipped as a success: the peer never gets the rest and no CQE says so.
- (2) and (4): with `MSG_WAITALL` no CQE while the peer is idle; the whole message arrives when it reads.
- (5): the send fails after 110592 bytes; its CQE carries that count. No CQE for the receive.
- (7): up to 512 KiB (fewer than 127 retries with these buffers) the receive is armed normally. From 768 KiB
  the send finished in an io-wq worker ("io-wq workers 1") and the `RECV(POLL_FIRST)` posts `-EAGAIN`. The
  same pair with a RECV without `POLL_FIRST` works, but the worker then blocks in `recv` until data arrives.

### Consequence for C1

- `MSG_WAITALL` on the send is required. With it, "no CQE for the send" means "fully sent".
- A CQE for the send with `res ≥ 0` is a failure after `res` bytes. ROUND3.md's result
  `(Err(e), Err(ECANCELED), ..)` has no errno to put into `e` in that case; the driver has to make one up or
  report the count.
- `-EAGAIN` on the receive's CQE means "send complete, receive not started". For a 70-byte reply this needs a
  send that was retried 127 times, so it is rare, but it is a legal outcome and must not reach the caller as
  a receive error. Re-issuing a plain receive is enough.

---

## KV4: when the kernel reads the `msghdr` and the iovec array of `SENDMSG`

**Verdict: true on a ring without SQPOLL, false with SQPOLL.** The kernel copies the user `msghdr` and the
iovec array when it prepares the request. Without SQPOLL that happens inside `io_uring_enter`; afterwards
both may be overwritten or freed although the send has not completed. With SQPOLL the SQ thread prepares the
request at some later time, so the return of an enter (or of `submit()`, which may make no syscall) says
nothing.

### Source (v7.0)

`io_uring/net.c`, called from `io_init_req()` → `def->prep` while the SQE is consumed:

```c
int io_sendmsg_prep(struct io_kiocb *req, const struct io_uring_sqe *sqe)
{
	...
	sr->msg_flags = READ_ONCE(sqe->msg_flags) | MSG_NOSIGNAL;
	...
	if (unlikely(!io_msg_alloc_async(req)))
		return -ENOMEM;
	if (req->opcode != IORING_OP_SENDMSG)
		return io_send_setup(req, sqe);
	...
	return io_sendmsg_setup(req, sqe);
}

static int io_sendmsg_setup(struct io_kiocb *req, const struct io_uring_sqe *sqe)
{
	...
	sr->umsg = u64_to_user_ptr(READ_ONCE(sqe->addr));
	ret = io_msg_copy_hdr(req, kmsg, &msg, ITER_SOURCE, NULL);    /* copies the user msghdr */
	...
	/* save msg_control as sys_sendmsg() overwrites it */
	sr->msg_control = kmsg->msg.msg_control_user;
	...
	return io_net_import_vec(req, kmsg, msg.msg_iov, msg.msg_iovlen, ITER_SOURCE);   /* copies the iovecs */
}
```

`io_net_import_vec()` calls `__import_iovec()`, which copies the array into `kmsg->fast_iov` or a kernel
allocation; the retries use `kmsg->msg.msg_iter`.

- **Source only:** `msg_control` is kept as a user pointer and read when the send runs
  (`kmsg->msg.msg_control_user = sr->msg_control;` in `io_sendmsg()`). Control data must stay valid until the
  CQE. `send_vectored` passes none.
- SQPOLL, `io_uring_enter`: `if (ctx->flags & IORING_SETUP_SQPOLL) { ... ret = to_submit; }`. It only wakes
  the thread.

### Probe (`kv4_sendmsg_copies_msghdr_at_enter`)

```text
KV4 (kernel 7.0.0-42-generic): SENDMSG(MSG_WAITALL) of 3 x 307200 bytes that cannot complete inside the enter; after io_uring_enter returned, the msghdr and the iovec array are overwritten (iovecs point at a buffer of 0xEE, msghdr fields at invalid addresses) and freed; then the peer reads
    (A) first attempt inside the enter is partial (Tcp, 0 bytes already queued): before the peer read 0 CQEs; the peer read 921600 bytes, payload intact; then 1 CQE [sendmsg: 921600]
    (B) socket buffer full, first attempt gets EAGAIN (Tcp, 10240 bytes already queued): before the peer read 0 CQEs; the peer read 931840 bytes, payload intact; then 1 CQE [sendmsg: 921600]
    (C) socket buffer full, POLL_FIRST: no attempt inside the enter (Tcp, 10240 bytes already queued): before the peer read 0 CQEs; the peer read 931840 bytes, payload intact; then 1 CQE [sendmsg: 921600]
    (A) first attempt inside the enter is partial (Unix, 0 bytes already queued): before the peer read 0 CQEs; the peer read 921600 bytes, payload intact; then 1 CQE [sendmsg: 921600]
    (B) socket buffer full, first attempt gets EAGAIN (Unix, 8128 bytes already queued): before the peer read 0 CQEs; the peer read 929728 bytes, payload intact; then 1 CQE [sendmsg: 921600]
    (C) socket buffer full, POLL_FIRST: no attempt inside the enter (Unix, 8128 bytes already queued): before the peer read 0 CQEs; the peer read 929728 bytes, payload intact; then 1 CQE [sendmsg: 921600]
  when is the msghdr read: between the push of the SQE and the enter
    (D) valid when the SQE is pushed, overwritten before the enter: 1 CQE [sendmsg: -EFAULT]; peer got 0 bytes
    (E) invalid when the SQE is pushed, filled in before the enter: 1 CQE [sendmsg: 12]; peer payload intact
  SQPOLL ring: the SQ thread prepares the request, not io_uring_enter
    (F) 500 x: push, submit(), then msg_iov overwritten at once: 0 sent, 500 failed with -EFAULT, 0 other; peer got 0 bytes
```

- (A)–(C): the right data arrives although both structures were overwritten and freed after the enter, even
  when the kernel had not attempted the send at all inside the enter (C).
- (D)/(E): the structures are read at the enter, not when the SQE is written into the ring.
- (F): on an SQPOLL ring, overwriting right after `submit()` returned made all 500 sends fail.

### Consequence for A1

- **May the `msghdr` and the iovec array be released right after `io_uring_enter` returned?** On a ring
  without SQPOLL yes, for the enter that consumed the SQE. On an SQPOLL ring no (`RuntimeBuilder::sqpoll`
  exists).
- **Is "keep them until the op's CQE" safe?** Yes, on every ring, and it is the rule to use: an op in monoio
  is not told when its SQE was consumed, only when its CQE arrived. The planned free list of boxed
  `msghdr`s (E8), returned when the op data is dropped, satisfies it.
- Finding 8 of the plan stands: the `msghdr` is read at the enter, which is after the push, so it needs an
  address that is stable from the push on. Inline in a future that may move is unsound.

---

## KV5: `IORING_SEND_VECTORIZED`

**Verdict: true.** The flag exists since Linux 6.17, its value is `1U << 5` in `sqe->ioprio`, and it works on
this 7.0 kernel for `IORING_OP_SEND` and `IORING_OP_SEND_ZC`, on TCP and Unix stream sockets.

### Source

`include/uapi/linux/io_uring.h` (v7.0; identical in `/usr/src/linux-headers-7.0.0-42-generic`):

```c
 * IORING_SEND_VECTORIZED	If set, SEND[_ZC] will take a pointer to a io_vec
 *				to allow vectorized send operations.
 ...
#define IORING_SEND_VECTORIZED		(1U << 5)
```

The define is in the uapi header of v6.17 and not of v6.16.

`io_uring/net.c`, `io_send_setup()`: `sqe->addr` is the iovec array, `sqe->len` the number of iovecs, and the
array is imported (copied) at prep, like the one of `SENDMSG`:

```c
	if (sr->flags & IORING_RECVSEND_FIXED_BUF) {
		if (sr->flags & IORING_SEND_VECTORIZED)
			return -EINVAL;
		...
	if (sr->flags & IORING_SEND_VECTORIZED)
		return io_net_import_vec(req, kmsg, sr->buf, sr->len, ITER_SOURCE);
```

- A kernel without the flag rejects the SQE at prep with `-EINVAL` (`if (sr->flags & ~SENDMSG_FLAGS) return
  -EINVAL;`).
- Not allowed together with `IORING_RECVSEND_FIXED_BUF` (source only).
- `io_sendmsg_prep()` serves `SEND` and `SENDMSG` and ORs `MSG_NOSIGNAL` into the flags of both (source only).

### Probe (`kv5_send_vectorized`)

`io-uring` 0.7.15 has no constant; the SQE is `opcode::Send::new(fd, iov.as_ptr().cast(), iov.len() as
u32).ioprio(1 << 5)`.

```text
KV5 (kernel 7.0.0-42-generic): IORING_OP_SEND with IORING_SEND_VECTORIZED (ioprio bit 0x20)
  header /usr/src/linux-headers-7.0.0-42-generic/include/uapi/linux/io_uring.h: #define IORING_SEND_VECTORIZED (1U << 5)
  header /usr/include/linux/io_uring.h: not defined
  (A) Tcp, 3 iovecs of 5 bytes: 1 CQE [send: 15]; peer payload intact
  (A) Unix, 3 iovecs of 5 bytes: 1 CQE [send: 15]; peer payload intact
  (B) Tcp, 3 x 307200 bytes, MSG_WAITALL, socket buffer full, iovec array overwritten after the enter: before the peer read 0 CQEs; the peer read 931840 bytes, payload intact; then 1 CQE [send: 921600]
  (C) Tcp, vectored SEND(LINK|SKIP, MSG_WAITALL) of 3 iovecs of 5 bytes -> RECV(POLL_FIRST), reply already in the socket: 1 CQE [recv: 4]; peer payload intact
  (D) Tcp, SEND_ZC with the flag, 3 iovecs of 5 bytes: 2 CQEs [send_zc: 15 F_MORE, send_zc: 0 F_NOTIF]; peer payload intact
  (E) Tcp, SENDMSG with the flag set (it has no meaning there): 1 CQE [sendmsg: 15]; peer payload intact
```

(`/usr/include/linux/io_uring.h` is an older user-space copy of the header; it does not describe the running
kernel.)

### Consequence for A1

- Usable on 7.0, flag value `1 << 5` (`0x20`) in `ioprio`.
- A vectored plain `SEND` needs **no `msghdr`**: the SQE points at the iovec array, which `IoVecArray` already
  owns on the heap. On 6.17+ that removes the allocation and the pool of E8 altogether; `SENDMSG` with the
  pool stays as the fallback.
- The lifetime rule is the one of KV4 (array copied at prep; keep it until the CQE).
- It works with `MSG_WAITALL` across retries (B) and as the head of the C1 pair (C).
- There is no feature bit for it. Support shows as `-EINVAL` from the first send (or a kernel version test).

---

## Requested changes

All in documents or in later packages; this package touched no source file.

1. **ROUND3-PLAN.md, KV1 row and r3-drain:** "6.12+" should read "6.13+". "One drain enter handles at most
   20 completions" is right for `enter(0, 0, GETEVENTS)`; add the wake-up cap (one item for `want = 1`).
2. **r3-drain, the `want = cq.len()` variant:** it has to run before `tick()` reaps the CQ, and it must not
   go through the `min_wait` branch of `UringInner::enter` (KV1, consequence 4).
3. **ROUND3.md C1:** "A failed link always posts a CQE for every remaining member" is false for a failed
   member with `CQE_SKIP_SUCCESS`. The pair posts one CQE, never two.
4. **r3-pair:** the reading table of KV2 (`-EAGAIN` on the receive, positive count on a failed send,
   `-ECANCELED` on the send for a rejected RECV); cancel both `user_data` values; `SEND_ZC` cannot be the
   head; measure on `defer` as well as `coop`.
5. **r3-vec:** keep `msghdr` and iovec array until the CQE; consider the vectored `SEND` as the main path on
   6.17+.

## Not established

- The Ubuntu kernel source was not read; where the notes say "source", it is mainline v7.0.
- The timings in KV1 [H] were taken while other agents were building. They are screening values.
- Source only, no probe: `msg_control` is read when the send runs; `IORING_SEND_VECTORIZED` with
  `IORING_RECVSEND_FIXED_BUF` gives `-EINVAL`; the kernel adds `MSG_NOSIGNAL`; the short window in which
  `IORING_SQ_TASKRUN` can be set with no work left.
- `COOP_TASKRUN` rings were not probed (the statements are about `DEFER_TASKRUN`).
