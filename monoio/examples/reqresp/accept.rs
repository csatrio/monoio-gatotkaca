//! The accept loop of a listener, and what it needs from the runtime builder.

use monoio::{
    net::{AcceptFlags, TcpListener, TcpStream},
    IoUringDriver, RuntimeBuilder,
};

/// Slots of the fixed file table (`--ring fixedfd`), and of its kernel-allocated part
/// (`--direct`).
pub const FIXED_FILES: u32 = 32768;

/// How connections are accepted.
#[derive(Clone, Copy, Debug, Default)]
pub struct Opts {
    /// `--ring fixedfd`: `register_fixed()` on each accepted stream.
    pub fixedfd: bool,
    /// `--direct`: multishot direct accept (`accept_batch_direct_into`). The streams have no
    /// fd, so nothing is registered per connection and `TCP_NODELAY` is set through the ring.
    pub direct: bool,
}

/// Applies what the accept mode needs to the server's builder.
pub fn configure(rb: RuntimeBuilder<IoUringDriver>, opts: &Opts) -> RuntimeBuilder<IoUringDriver> {
    let rb = if opts.fixedfd {
        rb.with_fixed_files(FIXED_FILES)
    } else {
        rb
    };
    if opts.direct {
        // The slots a multishot direct accept puts its sockets into.
        rb.with_fixed_files_kernel_alloc(FIXED_FILES)
    } else {
        rb
    }
}

/// Counts consecutive failed accepts; gives up after 100.
struct Failures(u32);

impl Failures {
    async fn check<T>(&mut self, res: std::io::Result<T>) -> bool {
        match res {
            Ok(_) => {
                self.0 = 0;
                true
            }
            Err(e) => {
                self.0 += 1;
                if self.0 == 100 {
                    eprintln!("reqresp: accept keeps failing: {e}");
                    std::process::exit(1);
                }
                monoio::task::yield_now().await;
                false
            }
        }
    }
}

/// Accepts on `listener` forever and hands every connection to `on_conn` (which spawns its
/// task). Runs as a task of the server runtime.
pub async fn run(listener: TcpListener, opts: Opts, on_conn: impl FnMut(TcpStream)) {
    if opts.direct {
        run_direct(listener, on_conn).await
    } else {
        run_fd(listener, opts, on_conn).await
    }
}

async fn run_fd(listener: TcpListener, opts: Opts, mut on_conn: impl FnMut(TcpStream)) {
    let mut batch = Vec::new();
    let mut failures = Failures(0);
    loop {
        let res = listener
            .accept_batch_into(&mut batch, 256, AcceptFlags::EMPTY)
            .await;
        if !failures.check(res).await {
            continue;
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

/// `--direct`: per connection no syscall at all. The accept is one CQE of the multishot
/// direct accept, and `TCP_NODELAY` is one SQE (`set_nodelay_async` pushes it at once; the
/// results of a whole batch are collected after the connections' tasks were spawned).
async fn run_direct(listener: TcpListener, mut on_conn: impl FnMut(TcpStream)) {
    let mut batch = Vec::new();
    let mut nodelay = Vec::new();
    let mut failures = Failures(0);
    loop {
        let res = listener
            .accept_batch_direct_into(&mut batch, 256, AcceptFlags::EMPTY)
            .await;
        if !failures.check(res).await {
            continue;
        }
        for stream in batch.drain(..) {
            nodelay.push(stream.set_nodelay_async(true));
            on_conn(stream);
        }
        for res in nodelay.drain(..) {
            if let Err(e) = res.await {
                // Linux before 6.7 has no socket options through the ring.
                eprintln!("reqresp: --direct: set_nodelay_async: {e}");
                std::process::exit(1);
            }
        }
    }
}
