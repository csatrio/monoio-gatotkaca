//! Extra `--ring` items. `main.rs` passes every item it does not know itself to [`apply`].

use monoio::{IoUringDriver, RuntimeBuilder};

/// Applies one `--ring` item to the server's builder. `Err` gives the builder back: the item
/// is unknown (`main.rs` then exits with an error).
#[allow(clippy::result_large_err)]
pub fn apply(
    rb: RuntimeBuilder<IoUringDriver>,
    item: &str,
) -> Result<RuntimeBuilder<IoUringDriver>, RuntimeBuilder<IoUringDriver>> {
    // === r3-drain: `drain<N>` ===
    // if let Some(n) = item.strip_prefix("drain").and_then(|n| n.parse().ok()) {
    //     return Ok(rb.drain_taskrun(n));
    // }
    let _ = item;
    Err(rb)
}
