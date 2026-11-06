//! The accept loop of a listener, and what it needs from the runtime builder.

use monoio::{
    net::{AcceptFlags, TcpListener, TcpStream},
    IoUringDriver, RuntimeBuilder,
};

/// Slots of the fixed file table (`--ring fixedfd`).
pub const FIXED_FILES: u32 = 32768;

/// How connections are accepted.
#[derive(Clone, Copy, Debug, Default)]
pub struct Opts {
    /// `--ring fixedfd`: `register_fixed()` on each accepted stream.
    pub fixedfd: bool,
    /// `--direct` (filled by a later package).
    pub direct: bool,
}

/// Applies what the accept mode needs to the server's builder.
pub fn configure(rb: RuntimeBuilder<IoUringDriver>, opts: &Opts) -> RuntimeBuilder<IoUringDriver> {
    // === r3-direct: `--direct` ===
    // Needs kernel-allocated slots here (`with_fixed_files_kernel_alloc`), and its accept
    // call in `run`.
    if opts.direct {
        eprintln!("--direct: not available in this build");
        std::process::exit(2);
    }
    if opts.fixedfd {
        rb.with_fixed_files(FIXED_FILES)
    } else {
        rb
    }
}

/// Accepts on `listener` forever and hands every connection to `on_conn` (which spawns its
/// task). Runs as a task of the server runtime.
pub async fn run(listener: TcpListener, opts: Opts, mut on_conn: impl FnMut(TcpStream)) {
    let mut batch = Vec::new();
    let mut failures = 0;
    loop {
        // === r3-direct: `--direct` accepts with `accept_batch_direct_into` instead ===
        match listener
            .accept_batch_into(&mut batch, 256, AcceptFlags::EMPTY)
            .await
        {
            Ok(_) => failures = 0,
            Err(e) => {
                failures += 1;
                if failures == 100 {
                    eprintln!("reqresp: accept keeps failing: {e}");
                    std::process::exit(1);
                }
                monoio::task::yield_now().await;
                continue;
            }
        }
        for (stream, _) in batch.drain(..) {
            let _ = stream.set_nodelay(true);
            if opts.fixedfd {
                if let Err(e) = stream.register_fixed() {
                    eprintln!("reqresp: register_fixed: {e}");
                    std::process::exit(1);
                }
            }
            on_conn(stream);
        }
    }
}
