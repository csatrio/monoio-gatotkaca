//! The `STATS` line (`--features metrics`): `DriverStats` and `RuntimeStats` of the server
//! runtime over the measured window. Both functions must run on the server runtime thread.
//!
//! The window starts with a reset of both structs, so every counter is a difference over the
//! window and the high-water marks belong to it. Every scalar field of the two structs is
//! printed under its own name (taken from their `Debug` output, so fields added later show up
//! without a change here), then the histograms and the derived values.

use std::fmt::Write;

use monoio::uring::opcode;

/// State kept from the start of the window.
pub struct Window {
    /// The kernel's CQ overflow counter is not cleared by a reset.
    cq_overflow: u64,
}

/// Starts the window: resets the counters.
pub fn start() -> Window {
    monoio::reset_driver_stats();
    monoio::reset_runtime_stats();
    Window {
        cq_overflow: monoio::driver_stats().cq_overflow,
    }
}

/// The `key: value` pairs of a derived `Debug` output (`Name { a: 1, b: [1, 2] }`).
fn fields(debug: &str) -> Vec<(&str, &str)> {
    let body = match (debug.find('{'), debug.rfind('}')) {
        (Some(a), Some(b)) if a < b => &debug[a + 1..b],
        _ => return Vec::new(),
    };
    // Split at the commas outside of nested values.
    let mut items = Vec::new();
    let (mut depth, mut from) = (0_i32, 0);
    for (i, c) in body.char_indices() {
        match c {
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => depth -= 1,
            ',' if depth == 0 => {
                items.push(&body[from..i]);
                from = i + 1;
            }
            _ => {}
        }
    }
    items.push(&body[from..]);
    items
        .into_iter()
        .filter_map(|item| item.split_once(':'))
        .map(|(k, v)| (k.trim(), v.trim()))
        .collect()
}

/// `[1, 2, 3]` as `1/2/3`.
fn histogram(value: &str) -> String {
    value
        .trim_matches(['[', ']'])
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("/")
}

/// Ends the window and returns the `key=value` pairs of the `STATS` line.
pub fn finish(window: Window, frames: u64, wall: f64) -> String {
    let d = monoio::driver_stats();
    let r = monoio::runtime_stats();
    let mut out = String::new();
    let mut seen = Vec::new();

    let (d_dbg, r_dbg) = (format!("{d:?}"), format!("{r:?}"));
    for (key, value) in fields(&d_dbg).into_iter().chain(fields(&r_dbg)) {
        // A name both structs use: the second one is the runtime's.
        let prefix = if seen.contains(&key) { "rt_" } else { "" };
        seen.push(key);
        if key == "cq_overflow" {
            let _ = write!(out, " cq_overflow={}", d.cq_overflow - window.cq_overflow);
        } else if value.parse::<u64>().is_ok() {
            let _ = write!(out, " {prefix}{key}={value}");
        } else if key == "cqes_per_tick" || key == "polls_per_round" {
            let _ = write!(out, " {prefix}{key}={}", histogram(value));
        }
    }

    let ratio = |a: u64, b: u64| {
        if b == 0 {
            "na".to_string()
        } else {
            format!("{:.3}", a as f64 / b as f64)
        }
    };
    let _ = write!(
        out,
        " rounds_per_s={:.0} frames_per_round={} task_polls_per_frame={} sqes_per_frame={} \
         cqes_per_frame={} enters_per_frame={} park_share={}",
        r.rounds as f64 / wall,
        ratio(frames, r.rounds),
        ratio(r.tasks_polled, frames),
        ratio(d.sqes_submitted, frames),
        ratio(d.cqes_reaped, frames),
        ratio(d.enter_wait + d.enter_submit, frames),
        ratio(r.park_ns, r.park_ns + r.busy_ns),
    );

    // Completion latency in driver ticks: the share of ops whose completion was reaped by
    // the tick of the enter that submitted them (bucket 0), one tick later (bucket 1), later.
    let ticks = |name: &str, code: u8, always: bool, out: &mut String| {
        let b = d.op_ticks_by_opcode[(code as usize).min(63)];
        let n: u64 = b.iter().sum();
        if n == 0 && !always {
            return;
        }
        let pct = |x: u64| {
            if n == 0 {
                "na".to_string()
            } else {
                format!("{:.1}", x as f64 * 100.0 / n as f64)
            }
        };
        let _ = write!(
            out,
            " {name}_ops={n} {name}_same_tick={} {name}_next_tick={} {name}_later={}",
            pct(b[0]),
            pct(b[1]),
            pct(b[2] + b[3]),
        );
    };
    // A multishot receive is the same opcode (`IORING_OP_RECV`) and only its first completion
    // is counted: on path `mshot` the `recv_*` values are about the receives armed inside the
    // window, not about frames.
    ticks("recv", opcode::Recv::CODE, true, &mut out);
    ticks("send", opcode::Send::CODE, true, &mut out);
    ticks("read_fixed", opcode::ReadFixed::CODE, false, &mut out);
    out
}
