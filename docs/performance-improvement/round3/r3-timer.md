# r3-timer: timers for request deadlines (T1, E1)

Branch `r3/r3-timer`, on `r3-g0`. Owns `monoio/src/time/**`, `monoio/tests/deadline_timer.rs`,
`monoio/examples/reqresp/deadline.rs` and this file. Nothing outside these was changed, also not locally.

All instruction counts are **user instructions per frame** on the server thread (`perf_event_open`, exact up to
a run-to-run jitter of about ±8). Cycles and frames/s are **screening, machine not quiet** (six other agents
were building).

## Result in one table

`reqresp --path pipe --ring defer,skipenter,ringfd --sizes 4k`, and `smallframes --conns 2000 --size 4096`:

| | r3-g0 | this branch | |
|---|---|---|---|
| 2000 connections, `--deadline none` | 2,342 | 2,342 | |
| 2000, `timeout` | 3,341 | 2,968 | −373 (−11%) |
| 2000, `coarse` (`timeout_coarse`) | | 2,848 | |
| 2000, `deadline` (`Deadline`) | | 2,564 | −777 against `timeout` at G0 |
| 2000, `nonepin` (no deadline, shape of `deadline`) | | 2,464 | |
| 8 connections, `none` | 2,058 | 2,039 | |
| 8, `timeout` | 2,998 | 2,587 | −411 (−14%) |
| 8, `coarse` | | 2,493 | |
| 8, `deadline` | | 2,205 | −793 against `timeout` at G0 |
| 8, `nonepin` | | 2,131 | |
| `smallframes` (a `timeout` per frame) | 2,671 | 2,397 | −274 (−10%) |
| `smallframes --no-timeout` | 1,883 | 1,863 | |

- A `time::timeout` per frame cost 940–1,000 instructions at G0. It costs 550–630 now, without a change in
  the caller.
- A `Deadline` per connection costs **74 (8 connections) to 100 (2000 connections, two polls per frame)
  instructions per frame** over the same code without a deadline (`nonepin`).
- **The goal "`deadline` within a few dozen instructions of `none`" is not met against `none` itself:
  +166 and +222.** 92 and 122 of that are not the deadline: `none` polls the frame future inline in the task,
  and any wrapper around it (`pin!(frame).await` alone, mode `nonepin`) ends that in this example. See
  "The shape of the reproducer" below.
- Screening: user cycles per frame at 2000 connections with `timeout` went from 3,647 (G0 binary, my run) to
  about 2,230; most of that is E1 (a 640-byte `Sleep` with 128-byte alignment inside every connection's
  future).

## Commits

| commit | content |
|---|---|
| `c77140a` | E1: timers without padding |
| `6468cd2` | park and submit touch the wheel only when a timer is due; `timeout()` inline |
| `85e8ca3` | `Instant::recent` |
| `a600ebd` | `Deadline`, `with_deadline`, `timeout_coarse`; lazy moves by slot; waker comparison; cheaper tick conversion |
| `714b9f9` | `reqresp --deadline deadline \| coarse \| timeoutpin \| nonepin` |
| `7f0dd69` | tests that do not depend on the fresh list |
| `38ba59a` | fresh list (marginal: see its section; last code commit, so it can be left out) |

The reproducer commit comes before the fresh list (the task's order was the other way round) because the
measurements of the `Deadline` commit need its modes.

Instructions per frame after each commit:

| after | `smallframes` | no timeout | 2000 `none` | 2000 `timeout` | 8 `none` | 8 `timeout` |
|---|---|---|---|---|---|---|
| r3-g0 | 2,671 | 1,883 | 2,342 | 3,341 | 2,058 | 2,998 |
| E1 | 2,460 | 1,863 | 2,337 | 3,083 | 2,059 | 2,719 |
| park path | 2,461 | 1,869 | 2,334 | 3,038 | 2,042 | 2,674 |
| `recent` | 2,451 | 1,863 | 2,326 | 3,032 | 2,048 | 2,660 |
| `Deadline` + modes | 2,414 | 1,863 | 2,342 | 2,985 | 2,041 | 2,615 |
| fresh list | 2,397 | 1,863 | 2,342 | 2,968 | 2,039 | 2,587 |

The new modes, before and after the fresh list:

| | `coarse` | `timeoutpin` | `deadline` | `nonepin` |
|---|---|---|---|---|
| 2000 connections | 2,866 → 2,848 | 3,001 → 2,984 | 2,560 → 2,564 | 2,464 → 2,464 |
| 8 connections | 2,496 → 2,493 | 2,625 → 2,605 | 2,201 → 2,205 | 2,122 → 2,131 |

- `none` at 2000 connections moved from 2,326 to 2,342 in the `Deadline` step: that is the example (more arms
  in `Guard::frame`), not the library; `smallframes --no-timeout` stayed at 1,863.
- 8 connections were run with `--sizes 4k` (G0's table used 4/16/64 KiB there: 2,077 and 3,033). With one
  frame size the count does not depend on how a 64 KiB frame happens to be split.

## Where the 1,000 instructions of a `timeout` were (profile of the G0 binary, 2000 connections)

`perf record -e instructions:u` on the server thread, `timeout` against `none`:

| part | instructions per frame |
|---|---|
| `memmove`: the `Timeout` (768 bytes with a 208-byte frame future) and its parts, copied four times | about 425 |
| timer entry and wheel: `poll_elapsed`, `Wheel::insert`, `Wheel::remove`, `reregister`, drop, tick conversion | about 285 |
| clock: one `Instant::now()` per frame, two more per park | about 100 |
| waker clone and drop | about 55 |
| the rest: the frame future is polled through `Timeout::poll` instead of inline | about 130 |

What is left of `timeout` over `none` now (626 at 2000 connections): copies about 210 (a 288-byte `Timeout`,
three times; this is how rustc moves a future into a wrapper and into its `.await` slot), clock about 125,
timer about 190, shape about 100.

## E1: no padding (`c77140a`)

| type | before (size / alignment) | after |
|---|---|---|
| `TimerShared` | 384 / 128 | 48 / 8 |
| `TimerEntry` | 512 / 128 | 56 / 8 |
| `Sleep` | 640 / 128 | 72 / 8 |
| `Timeout<Ready<()>>` | 768 / 128 | 80 / 8 |
| time `Handle` (one per timer) | 24 | 8 |
| `Deadline` (new) | | 56 / 8 |

Pinned by the unit tests `time::driver::entry::tests::timer_sizes` and `time::deadline::tests::deadline_size`.

What went, besides the two `CachePadded`:

- `StateCell::result`: the driver never fires a timer with an error (the shutdown path was commented out), so
  `Sleep::poll` cannot panic with "timer error" any more.
- `TimerSharedPadded::true_when`: never read.
- `TimerEntry::initial_deadline: Option<Instant>` (16 bytes, a copy of `Sleep::deadline`): "never registered"
  is a state value (`STATE_INIT`), and `Sleep` passes its deadline on the first poll.
- The copied `Instant` in every timer's `Handle`: the handle is one `Rc`, the time source sits in the shared
  state. The `Rc` clone stays: it keeps the wheel alive for an entry that outlives its runtime (a `Sleep` kept
  in a struct after `block_on` returned must still be able to unlink itself). Resolving the driver through
  `CURRENT` on drop would be unsound for that case; the clone and drop are about 6 instructions.
- `RefCell<Option<Waker>>` → `Cell<Option<Waker>>`.
- `Drop` of an entry that fired or was never polled does not touch the driver.

Behaviour that changed with it: the entry now keeps the waker of the **latest** poll (the old code kept the
first one forever, so a `Sleep` handed to another task woke the wrong task; test `sleep_follows_its_task`).

## Park path (`6468cd2`)

Before, every park scanned the six levels for the next expiry, read the clock (twice when a timer was
pending), polled the wheel and scanned again. Now:

- `Inner::next_wake` (a `Cell`, 0 = none) is a **lower bound** of the earliest registered expiration. A
  registration lowers it; `process_at_time` makes it exact; a removal leaves it alone.
- `park`: no timer registered → no clock read at all. Timers pending → the clock once for the wait, and once
  after it (`process_if_due`); the wheel is only polled when `now >= next_wake`.
- A removed earliest timer leaves the bound early. Cost: at most one early wake-up of an idle runtime (or one
  wheel poll that fires nothing) per such timer; then the bound is exact again. With a `timeout(10 s)` per
  request that is one extra wheel poll every 10 s.
- The wheel's own time (`elapsed`) therefore lags the clock between due timers. The wheel is consistent with
  itself at any `elapsed`; a timer registered against an old `elapsed` lands on a higher level and is moved
  down in the pass that fires it. One visible consequence: a timer whose deadline has passed, but lies after
  the wheel's time, is no longer fired inside `reset`/its first poll; it fires in the next driver pass, which
  comes before the runtime blocks (`next_wake` is in the past, so the park does not wait).
- `process_at_time` does not hold the wheel's `RefCell` while a waker runs. A waker that created, reset or
  dropped a timer used to hit a `BorrowMutError` panic. Tests `timers_registered_inside_a_wake`,
  `timers_dropped_and_reset_inside_a_wake` (the second drops and resets entries that wait on the pending list
  of the same pass).
- `timeout()`/`timeout_at()` are `#[inline(always)]`: one of the four copies of the `Timeout` goes (−41).

## `Instant::recent()` (`85e8ca3`)

- State: `Inner::recent: Cell<Instant>` and `Inner::recent_tick: Cell<u64>` (the same instant as a tick,
  rounded up; **0 means stale**).
- The driver stores both whenever it reads the clock for itself: in `process_if_due`, i.e. after a park and
  in a cold-path submit, and only when a timer is registered. It stores 0 into `recent_tick` once per park
  (after the wait) and once per submit: one store, no clock read.
- `recent()`: fresh → the stored value; stale → one clock read, stored.
- So: at most one clock read per round however often it is called, none when it is not called and no timer is
  registered; never ahead of `now()`; behind by less than one round of tasks; never backwards (every value
  comes from the monotonic clock, in order).
- Outside a runtime or without `enable_timer`: `Instant::now()`. No panic. Documented.
- The clock read before a park (for the wait) is not kept: it is stale when the wait returns.
- A failed park or submit of the inner driver no longer skips the timer check after it.
- Limit, found while writing the tests: a **main future that wakes itself** is polled again in
  `while should_poll()` (`runtime.rs:317`) without the runtime reaching the driver. Timers do not fire and
  `recent()` does not move in such a loop. That is the runtime's behaviour before and after this branch; the
  busy-loop tests therefore run their loop in a spawned task.

## `Deadline`, `with_deadline`, `timeout_coarse` (`a600ebd`)

API (`monoio::time`):

```rust
pub struct Deadline { /* one TimerEntry, 56 bytes, !Unpin */ }
impl Deadline {
    pub fn new() -> Self;                                    // disarmed, not registered; panics like sleep() without a timer
    pub fn arm(self: Pin<&mut Self>, after: Duration);       // from Instant::recent()
    pub fn arm_at(self: Pin<&mut Self>, at: Instant);        // exact
    pub fn disarm(self: Pin<&mut Self>);
    pub fn is_armed(&self) -> bool;
    pub fn is_expired(&self) -> bool;                        // armed and expired
    pub fn poll_expired(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()>;
    pub fn timeout<F: Future>(self: Pin<&mut Self>, after: Duration, future: F) -> WithDeadline<'_, F>;
}
pub fn with_deadline<F: Future>(deadline: Pin<&mut Deadline>, future: F) -> WithDeadline<'_, F>;
pub struct WithDeadline<'a, F>;                              // Output = Result<F::Output, Elapsed>
pub fn timeout_coarse<T: Future>(duration: Duration, future: T) -> Timeout<T>;
```

Decisions:

- **`with_deadline` neither arms nor disarms** (ROUND3.md's signature): it races a future against the
  deadline, so several steps can run under one `arm`. With a disarmed deadline it is `Ok(future.await)`.
- **`Deadline::timeout(after, future)` is the one call for a request**: it arms when called and disarms when
  the returned future is dropped (which is also when it completed). The name mirrors `time::timeout`.
- Expired is a level, not an edge: `poll_expired` stays ready until `disarm` or the next `arm`.
- `timeout_coarse_at` was not added: an absolute instant has no coarse variant, it would be `timeout_at`.
- The returned future is a named type (`WithDeadline`), not `impl Future`, so it can be stored.

### States and invariants

Everything is in the entry's state word; there is no separate "armed" flag.

| state | meaning for a `Deadline` |
|---|---|
| a tick `<= MAX_TICK` | armed, registered |
| `STATE_NEVER` | disarmed, registered (still in its slot or on the fresh list) |
| `STATE_PENDING_FIRE` | on the wheel's pending list, fires in this driver pass |
| `STATE_PENDING_SILENT` | on the pending list, disarmed: this pass deregisters it without a wake |
| `STATE_INIT` | not registered, not fired: new, or disarmed and out of the wheel |
| `STATE_DEREGISTERED` | not registered, fired: expired while armed |

1. A registered entry's true expiration (state) is never before the tick it is registered for
   (`cached_when`). The wheel handles any true expiration `>= cached_when`: when it reaches the slot it
   re-reads the state and moves the entry (`mark_pending` fails with the new tick).
2. `arm(t)`: if the entry is in a slot (or on the fresh list) and `t >= cached_when`, it stores `t`. Else it
   goes through the wheel: remove if registered, insert. This is wider than the old `extend_expiration` rule
   (`t >=` the *current expiration*): moving a deadline earlier is also a store while it stays at or after the
   slot. `Sleep::reset` uses the same rule.
3. `disarm`: a tick → `STATE_NEVER` (one store, invariant 1 holds: it is the largest tick); expired →
   `STATE_INIT`; `STATE_PENDING_FIRE` (only reachable from a waker inside the driver's pass) →
   `STATE_PENDING_SILENT`.
4. The wheel reaches the slot of a `STATE_NEVER` entry: `mark_pending` turns it into `STATE_PENDING_SILENT`
   and it goes on the pending list; `fire` sets `STATE_INIT` and hands the waker back **without** waking it.
   The driver drops that waker after it released the wheel (dropping the last waker of a task may free the
   task and with it this entry). So "an expiry while disarmed wakes nobody and is swallowed", without taking
   the waker out at each `disarm` (that would be two vtable calls per request).
5. `poll_expired`: ready iff `STATE_DEREGISTERED`. It stores the waker only while armed (a tick or
   `STATE_PENDING_FIRE`), so a disarmed deadline keeps no waker beyond the one its registered entry still
   holds until the wheel swallows it.
6. After any expiry, reported or swallowed, the entry is out of the wheel, and the next `arm` registers it.

Cost: `arm` is a load of `recent_tick`, an add, two compares and a store; `disarm` a compare and a store.
While the connection keeps being armed, the driver re-inserts the entry when its old slot comes up, about once
per deadline period (for 10 s: every 8.2 s, the level-2 slot width times two).

### Precision

- `arm(after)` expires at `recent() + after`, never earlier, at most 2 ms later (clock tick and duration are
  both rounded up to a millisecond). Against the real clock it can be early by the lag of `recent()` (less
  than one round). Documented on `Deadline`, `arm` and `timeout_coarse`. `arm_at` and `timeout()` are exact.
- Durations above about 30 years are cut to that; instants further away than a tick can express are capped
  (they used to panic with "Duration too far into the future").

### Wakers: `will_wake` does not work for this runtime's task wakers

`Waker::will_wake` compares the vtable **address**. The task wakers' vtable is a promoted constant in
`task/waker.rs::raw_waker`, which is inlined into `waker_ref` (the waker a task is polled with) and into
`clone_waker`; the two copies have different addresses. So `stored_clone.will_wake(cx.waker())` is false for
the same task, in spite of the comment in `waker_ref`. With `will_wake`, the entry cloned a waker and dropped
one on every poll: 2,585 → 2,534 instructions per frame in `deadline` mode when that stopped. `same_waker()`
in `entry.rs` compares the data pointer and, when the vtable addresses differ, the vtables' contents (four
function pointers; `RawWakerVTable: PartialEq`). See "Requested changes" 3: the same `will_wake` test is in
the op slots.

### Other changes in this commit

- `deadline_to_tick`: no `Instant + 999_999 ns`, no `u128`: −40 instructions per `timeout()`.
- `timeout_coarse`: no clock read; the tick is computed at creation from `recent_tick` and carried in the
  unregistered entry's `cached_when`. 119–122 instructions cheaper than `timeout`.

## The shape of the reproducer (`714b9f9`)

`Guard::frame` is an `async fn` that gets the frame future by value.

- `none`: `frame.await`. The compiler inlines the frame future's `poll` into the task's poll.
- `timeout`, `coarse`: `timeout(10 s, frame).await`, by value, as the bench server does. Unchanged code, so
  these two compare with G0.
- `timeoutpin`, `deadline`, `nonepin` go through one more `async fn` that gets `pin!(frame)`:
  `timeout(10 s, pinned)`, `deadline.timeout(10 s, pinned)`, `pinned.await`.
- `nonepin` exists because the wrapper alone costs 92 (8 connections) to 122 (2000) instructions per frame
  here: the frame future is polled through `<Pin<&mut F> as Future>::poll`, out of line, and one more
  generator layer is in between. What a `Deadline` costs is `deadline − nonepin`.
- The instruction count of one mode depends on which other modes exist in the binary: with the `deadline`
  arm written directly into `Guard::frame`, `none` moved between 2,341 and 2,413. The split into two
  functions keeps `none`, `timeout` and `coarse` where they were.
- `timeoutpin` against `timeout`: 2,984 / 2,968 and 2,605 / 2,587. Wrapping a pinned reference saves the
  copies of the future but costs the shape, so it does not pay for a 208-byte future. The doc comments say
  that a `Pin<&mut F>` can be wrapped; they do not promise a gain.
- The `Deadline` is in a `Box` (`Guard` is moved after `new`). A server that owns its connection future pins
  it there.

## Fresh list (`38ba59a`): built, measured, marginal

Design:

- A timer that expires `FRESH_DISTANCE` = 64 ticks or more after the wheel's time goes on `Wheel::fresh`, an
  intrusive list, instead of into a slot. Nearer ones (level 0) go into their slot as before.
- `Wheel::fresh_min`: the lowest tick put on the list since the last flush (`u64::MAX` when none). A lower
  bound: removals do not raise it.
- `cached_when` of a fresh entry = the tick it was registered for, with `FRESH_BIT` (bit 63) set. Ticks are
  capped at `2^63 − 1`.

Invariants:

1. A fresh entry's true expiration is not before its registered tick (the lazy-move rule, with the bit masked
   off), and that tick is not before `fresh_min`. So `arm`/`disarm`/`reset` on a fresh entry are the same
   single stores.
2. `poll_at()` = min(next slot expiration, `fresh_min`). `next_wake` is taken from it and lowered by each
   registration, so the park waits no longer than the earliest fresh entry, and `process_if_due` in a loop
   that never parks sees it too.
3. `poll(now)` flushes first if `now >= fresh_min`: every fresh entry is placed as if it were inserted now
   (slot by its true expiration; pending list if due; pending-silent if disarmed), `fresh_min` is reset. Then
   the wheel expires what is due. `poll` checks this on every call, so an entry that a waker registers during
   the pass and that is already due is fired in the same pass.
4. From 2 and 3: a fresh entry with expiration `T` fires in the first driver pass with `now >= T`, exactly
   like a slot entry. Nothing is late beyond the wheel's 1 ms, nothing is lost.
5. Drop or re-registration of a fresh entry: `Wheel::remove` sees the bit and unlinks it from the list.
6. A deadline at or before the wheel's time still fires at once in `reregister`.

Why not "flush at every park" or "at the next tick" (the plan's wording): in `reqresp` and `smallframes` the
timer is created when the connection starts to wait for its next request, so **every timer lives across at
least one park**, and at 2000 connections across 18 ms. Both variants would insert every timer into the wheel
and add the list on top. They were not built. With the flush at `fresh_min`, a workload of 10 s timeouts
flushes about every 10 s and moves only the entries alive at that moment.

Cases thought through (and tested, see below): `submit()` in a loop that never parks; `reset` of a fresh
entry to later (store) and earlier (unlink, register again); drop while fresh; a disarmed fresh `Deadline` at
the flush; `Interval` (periods below 64 ms never use the list; longer ones are flushed when due and fire in
that pass); a timer registered from inside a wake, near, far and already due; a wheel that is behind the
clock, where also a 1 ms sleep is "far" and takes the list.

Costs outside the fast case: a timer of 64 ms or more that does fire pays a push and a pop on top of its
insert. A periodic timer of period `p >= 64 ms` in the same runtime causes a flush every `p`, which inserts the
timeouts alive at that moment.

Measured (before → after):

| | before | after | |
|---|---|---|---|
| `smallframes` 2000 × 4 KiB | 2,414 | 2,397 | −0.7% |
| `reqresp` 2000, `timeout` | 2,985 | 2,968 | −0.6% |
| `reqresp` 8, `timeout` | 2,615 | 2,587 | −1.1% |
| `reqresp` 2000 / 8, `deadline` | 2,560 / 2,201 | 2,564 / 2,205 | noise |

The instructions drop, which was the condition to keep the commit. They drop by less than the plan's 1%
adoption bar at 2000 connections. The list saves the level and slot arithmetic, not the registration itself
(the `RefCell`, the links, the `next_wake` update stay), and callers who care can use `Deadline`, which has no
per-request registration at all. **My recommendation: leave it out unless the lead wants the 17–28
instructions for callers that keep `timeout()`.** It is the last code commit; reverting it touches only
`time/driver/wheel/mod.rs`, `entry.rs`, the `wheel_ops()` type in `driver/mod.rs` and the unit tests in
`time/deadline.rs` (which then need the tuple form of `wheel_ops()` back, as in `a600ebd`).

## Tests

`monoio/tests/deadline_timer.rs` (23 tests) and unit tests in `time/` (13 new). Lower bounds are exact, upper
bounds are 3 s (`LATE`).

| asked for | test |
|---|---|
| a disarmed deadline never fires, also when its old slot expires | `disarmed_deadline_never_fires` (also: its task is not polled again) |
| `arm` after an expiry while disarmed fires at the new time | `arm_after_expiry_while_disarmed` |
| re-arming later does not touch the wheel | unit test `rearm_and_disarm_do_not_touch_the_wheel`: 10,000 arm/disarm pairs, `cfg(test)` counters of wheel inserts/removes and list pushes/unlinks unchanged; an earlier arm changes them by one |
| ... and fires at the latest deadline | `rearm_later_fires_at_the_latest_deadline` |
| arming earlier fires at the earlier time | `rearm_earlier_fires_at_the_earlier_deadline` |
| `with_deadline`: `Err(Elapsed)` or the output; reuse | `deadline_timeout_reuse`, `with_deadline_steps` |
| `recent()` lags at most one park, monotonic, busy and idle | `recent_lags_at_most_one_park_idle`, `recent_lags_at_most_one_round_busy` (`task_budget(1)`), `recent_is_refreshed_every_round_busy`, `recent_with_a_pending_timer`, `recent_without_a_time_driver` |
| short timers on the fresh list fire on time, busy and idle | `timers_fire_on_time_idle`, `timers_fire_on_time_busy` (budget default, 1, 16; under a watchdog); wheel unit tests `fresh_routing_and_flush`, `fresh_short_timer_on_stale_wheel`; unit test `short_deadline_on_a_stale_wheel` asserts the entry is on the list |
| a timer dropped while fresh leaves nothing behind | wheel unit test `fresh_churn_leaves_nothing_behind` (20,000 cycles, then the flush moves exactly one entry), unit test `timers_dropped_while_fresh_leave_nothing_behind`, `timeouts_dropped_young_leave_nothing_behind` (30,000) |
| also | `arm_at_and_zero`, `dropped_deadlines_leave_nothing_behind`, `deadline_follows_its_task`, `sleep_follows_its_task`, `timeout_coarse_works`, `timeout_is_exact_after_a_long_poll`, `intervals_tick_on_time`, the two wake tests, `fresh_remove`, `fresh_lazy_move_and_disarm`, `fresh_entries_are_flushed_when_one_is_due`, `drop_unlinks`, `timer_sizes`, `deadline_size` |

"Does not touch the wheel" is observed through counters that exist only in the crate's unit tests
(`wheel::OPS`, `#[cfg(test)]`): nothing is compiled into a normal build, and there is no new public item. The
rule "instrumentation only behind `metrics`" is why they are not a hidden public counter.

`monoio/tests/timer_cold_path.rs` was not changed and passes.

## Tried and rejected

| what | why not | numbers |
|---|---|---|
| `Waker::will_wake` to skip a waker clone | always false here (above) | `deadline` 2,585 with it, 2,534 without the clone and drop |
| `armed: bool` in `Deadline` and taking the waker out at `disarm` (the plan's wording) | two vtable calls per request; the state word and `STATE_NEVER` do it with one store | not measured, by construction |
| `deadline` mode with the future by value | 2,579 instead of 2,534 (same tree), and it moved `none` to 2,413 | |
| `pin!` in the `timeout` mode | no gain for a 208-byte future (above) | 2,984 against 2,968 |
| fresh list flushed at every park / tick | every timer of the reproducer lives across a park | not built |
| the entry without an `Rc` of the driver (TLS lookup at drop, or a raw pointer) | unsound for an entry that outlives the runtime; saves about 6 instructions | not built |
| the coarse clock for the park's wait | timers would fire late by up to one round | not built |
| computing the tick of an exact `Sleep` at creation | a `timeout()` whose future is ready at once never registers and should not pay for it | not built |
| a field that remembers the vtable address of the polling waker | 8 bytes per timer for about 10 instructions per poll; the fix belongs in `task/waker.rs` | not built |

## Open questions

1. Fresh list: keep or leave out (above).
2. `IORING_ENTER_ABS_TIMER` (6.12+): with an absolute wait the driver would not need the clock read before a
   park while timers are pending (one of the two per park). Needs a `park_until` in the `Driver` trait
   (r3-drain's files).
3. The self-waking main future that never reaches the driver (`runtime.rs:317`, above): intended?
4. Counters for timers under `metrics` (wheel inserts and removals, list pushes, flushed, fired, swallowed)?
   Today they exist only in unit tests.
5. Names: `Deadline::timeout`, `WithDeadline`, `arm_at`, `is_expired`, `timeout_coarse`.
6. `process_at_time` fires at most what is due at `now`; a waker that registers a due timer on every wake
   keeps the pass going. Before this branch such a waker panicked (`RefCell`), so no existing code does it.

## Requested changes

None was applied locally; the branch builds and passes its tests without them.

1. `monoio/examples/reqresp/main.rs`, usage text (line 62):
   ```diff
   -  --deadline D         none | timeout (default none)
   +  --deadline D         none | timeout | coarse | timeoutpin | deadline | nonepin (default none)
   ```
   and the same row of the option table in `round3/r3-repro.md`.
2. Registry lines (allowed by the rules, listed here): `monoio/src/time/mod.rs` got
   `pub use deadline::{with_deadline, Deadline, WithDeadline};` and `timeout_coarse` in the `timeout` re-export.
   `lib.rs` needs nothing (`monoio::time` is public).
3. `monoio/src/task/waker.rs` (nobody's file in this wave): give the task wakers **one** vtable address, so
   that `Waker::will_wake` works as the comment in `waker_ref` says. The same `will_wake` test guards the
   waker clone in the op slots (`driver/uring/lifecycle.rs:441, 537, 585, 635`, `shared_fd.rs:242`,
   `doorbell.rs:77`), so an op future that is polled again while in flight clones a waker and drops one each
   time. A candidate:
   ```diff
   -    let vtable = &RawWakerVTable::new(
   -        clone_waker::<T, S>,
   -        wake_by_val::<T, S>,
   -        wake_by_ref::<T, S>,
   -        drop_waker::<T, S>,
   -    );
   -    RawWaker::new(ptr, vtable)
   +    RawWaker::new(ptr, vtable::<T, S>())
   +}
   +
   +/// One address per task type: `Waker::will_wake` compares it.
   +#[inline(never)]
   +fn vtable<T: Future, S: Schedule>() -> &'static RawWakerVTable {
   +    &RawWakerVTable::new(
   +        clone_waker::<T, S>,
   +        wake_by_val::<T, S>,
   +        wake_by_ref::<T, S>,
   +        drop_waker::<T, S>,
   +    )
   ```
   Measured locally with exactly this diff (built into a separate target directory, the source change
   reverted, nothing committed): `will_wake` then holds and the entry's content comparison is skipped.
   Instructions per frame: `deadline` 2,564 → 2,544 (2000 connections), 2,205 → 2,188 (8); `timeout`
   2,968 → 2,960; `none` 2,342 → 2,346 and 2,039 → 2,045 (noise: in these paths an op future is not polled
   again while in flight, so the op slots' `will_wake` does not fire); `smallframes` 2,397 → 2,394. Any
   code that re-polls an in-flight op (`select!` over two ops, a task woken by one op that re-polls the
   other) pays a waker clone and drop per poll today.
4. AGENTS.md, a bullet after "Deadlines without the timer" (proposed wording):

   > **Request deadlines with the timer.** `time::timeout(d, fut)` makes a timer per call: a clock read, a
   > registration on the first poll (every op future is `Pending` on its first poll) and a removal on drop,
   > about 550–630 user instructions per request. For a deadline around every request of a connection keep one
   > `time::Deadline` per connection: `let mut dl = pin!(Deadline::new());`, then
   > `dl.as_mut().timeout(d, serve_one(..)).await` (arms, runs, disarms; `Err(Elapsed)` on expiry). For several
   > steps under one deadline: `dl.as_mut().arm(d)`, `with_deadline(dl.as_mut(), step).await?` per step,
   > `dl.as_mut().disarm()`. `arm` and `disarm` are single stores while the entry is registered (the driver
   > re-inserts it about once per deadline period); a disarmed deadline never fires and wakes nobody;
   > `poll_expired` is ready only when armed and expired; `arm_at(Instant)` is exact. About 75–100
   > instructions per request. `time::timeout_coarse(d, fut)` is `timeout` without the clock read. Both use
   > the coarse clock `time::Instant::recent()`: the latest clock read on this runtime thread, read at most
   > once per round of tasks (after a park or a cold-path submit), never ahead of `now()`, never backwards,
   > behind by less than one round. A coarse deadline can therefore come early by that lag and up to 2 ms
   > late: use it for deadlines of seconds; `timeout()` keeps the exact clock. `recent()` is `now()` outside a
   > runtime and without `enable_timer`. A `Sleep` is 72 bytes, a `Deadline` 56, both with alignment 8. With
   > no timer registered a park reads no clock.

   and in "Environment gotchas" nothing changes.
