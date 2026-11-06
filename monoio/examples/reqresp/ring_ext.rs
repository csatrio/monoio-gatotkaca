//! Extra `--ring` items. `main.rs` passes every item it does not know itself to [`apply`].

use monoio::{IoUringDriver, RuntimeBuilder};

/// Applies one `--ring` item to the server's builder. `Err` gives the builder back: the item
/// is unknown (`main.rs` then exits with an error).
#[allow(clippy::result_large_err)]
pub fn apply(
    rb: RuntimeBuilder<IoUringDriver>,
    item: &str,
) -> Result<RuntimeBuilder<IoUringDriver>, RuntimeBuilder<IoUringDriver>> {
    // `draincq<N>`: the pre-tick variant (`drain_taskrun_pre_tick`), `drain<N>`: `drain_taskrun`.
    // Both need `defer` in the same `--ring` list; on any other ring they do nothing.
    if let Some(n) = item.strip_prefix("draincq").and_then(|n| n.parse().ok()) {
        return Ok(rb.drain_taskrun_pre_tick(n));
    }
    if let Some(n) = item.strip_prefix("drain").and_then(|n| n.parse().ok()) {
        return Ok(rb.drain_taskrun(n));
    }
    Err(rb)
}
