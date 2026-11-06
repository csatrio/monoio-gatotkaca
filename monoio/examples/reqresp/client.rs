//! The client threads: closed-loop request/response connections and bulk readers.
//!
//! One monoio runtime per client CPU. A request/response connection sends one frame, waits
//! for the whole reply and only then sends the next one. Inside the measured window it
//! records the reply latency (send start to reply complete) into the thread's histogram and
//! tracks the longest interval between two of its replies.

use std::{
    cell::{Cell, RefCell},
    net::SocketAddr,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};

use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::TcpStream,
    uring::{opcode, types},
    IoUringDriver, RuntimeBuilder,
};

use crate::{bulk, measure::Hist, paths::REPLY_LEN};

/// `Shared::phase`: connections ramp up and warm up.
pub const WARMUP: u8 = 0;
/// `Shared::phase`: inside the measured window.
pub const WINDOW: u8 = 1;
/// `Shared::phase`: the window is over.
pub const DONE: u8 = 2;

/// What the server thread tells the client threads. Times are nanoseconds since `epoch`.
pub struct Shared {
    pub epoch: Instant,
    pub phase: AtomicU8,
    pub window_start: AtomicU64,
    pub window_end: AtomicU64,
}

impl Shared {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            phase: AtomicU8::new(WARMUP),
            window_start: AtomicU64::new(0),
            window_end: AtomicU64::new(0),
        }
    }

    pub fn now(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }
}

/// The work of one client thread.
pub struct Plan {
    pub cpu: usize,
    /// For each request/response connection: the index into `frames` of its first frame.
    pub conns: Vec<usize>,
    /// Bulk connections of this thread.
    pub bulk: usize,
    /// One frame per `--sizes` entry, header filled in.
    pub frames: &'static [&'static [u8]],
    /// `--think`: the pause between a reply and the next frame.
    pub think: Duration,
    pub addr: SocketAddr,
    pub bulk_addr: SocketAddr,
}

/// What one client thread measured inside the window.
pub struct Report {
    /// Reply latencies in nanoseconds.
    pub hist: Hist,
    /// The longest interval between two consecutive replies of one connection. The window's
    /// start and end count as replies, so a connection without any reply shows the whole
    /// window.
    pub max_gap_ns: u64,
    /// Fewest and most replies a single connection got.
    pub conn_frames_min: u64,
    pub conn_frames_max: u64,
    /// Connections that ended before the window did.
    pub failed: u64,
}

/// Per-connection state the thread reads at the end.
#[derive(Default)]
struct Slot {
    /// Time of the last reply inside the window (0: none yet).
    last: Cell<u64>,
    max_gap: Cell<u64>,
    replies: Cell<u64>,
    failed: Cell<bool>,
}

struct State {
    shared: Arc<Shared>,
    hist: RefCell<Hist>,
    slots: Vec<Slot>,
}

async fn connect(addr: SocketAddr) -> TcpStream {
    loop {
        match TcpStream::connect(addr).await {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                break s;
            }
            Err(_) => monoio::task::yield_now().await,
        }
    }
}

/// A pause with microsecond resolution (the timer wheel has 1 ms): an `IORING_OP_TIMEOUT`.
async fn pause(d: Duration) {
    let ts = Box::new(
        types::Timespec::new()
            .sec(d.as_secs())
            .nsec(d.subsec_nanos()),
    );
    let sqe = opcode::Timeout::new(&*ts as *const types::Timespec).build();
    // SAFETY: a single-shot SQE whose only pointer is into `ts`, which the op owns until its
    // completion.
    if let Ok(op) = unsafe { monoio::uring::submit(sqe, ts) } {
        op.await;
    }
}

/// One closed-loop connection.
async fn conn(
    mut stream: TcpStream,
    mut next: usize,
    (frames, think): (&'static [&'static [u8]], Duration),
    st: Rc<State>,
    slot: usize,
) {
    let mut reply = Vec::with_capacity(2 * REPLY_LEN);
    let shared = &*st.shared;
    let slot = &st.slots[slot];
    loop {
        let frame = frames[next];
        next = (next + 1) % frames.len();
        let start = Instant::now();
        if stream.write_all(frame).await.0.is_err() {
            break;
        }
        let mut got = 0;
        while got < REPLY_LEN {
            let (res, b) = stream.read(reply).await;
            reply = b;
            match res {
                Ok(n) if n > 0 => got += n,
                _ => {
                    slot.failed.set(true);
                    return;
                }
            }
        }
        if got != REPLY_LEN {
            eprintln!("reqresp: client got {got} reply bytes, expected {REPLY_LEN}");
            break;
        }
        let end = Instant::now();
        if shared.phase.load(Ordering::Relaxed) == WINDOW {
            st.hist.borrow_mut().record((end - start).as_nanos() as u64);
            let now = (end - shared.epoch).as_nanos() as u64;
            let prev = match slot.last.get() {
                0 => shared.window_start.load(Ordering::Relaxed),
                last => last,
            };
            slot.max_gap
                .set(slot.max_gap.get().max(now.saturating_sub(prev)));
            slot.last.set(now);
            slot.replies.set(slot.replies.get() + 1);
        }
        if !think.is_zero() {
            pause(think).await;
        }
    }
    slot.failed.set(true);
}

/// Runs one client thread: connects, drives the load until the window is over, sends its
/// report and then keeps the connections open (the process exits from the main thread).
pub fn run(plan: Plan, shared: Arc<Shared>, report: mpsc::Sender<Report>) {
    if monoio::utils::bind_to_cpu_set([plan.cpu]).is_err() {
        eprintln!("reqresp: cannot pin a client thread to CPU {}", plan.cpu);
    }
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(4096)
        .enable_timer()
        .build()
        .expect("client runtime");
    rt.block_on(async move {
        let st = Rc::new(State {
            shared: shared.clone(),
            hist: RefCell::new(Hist::new()),
            slots: plan.conns.iter().map(|_| Slot::default()).collect(),
        });
        for (slot, &first) in plan.conns.iter().enumerate() {
            let stream = connect(plan.addr).await;
            let load = (plan.frames, plan.think);
            monoio::spawn(conn(stream, first, load, st.clone(), slot));
        }
        for _ in 0..plan.bulk {
            monoio::spawn(bulk::drain(connect(plan.bulk_addr).await));
        }
        while shared.phase.load(Ordering::Relaxed) != DONE {
            monoio::time::sleep(Duration::from_millis(2)).await;
        }

        let (start, end) = (
            shared.window_start.load(Ordering::Relaxed),
            shared.window_end.load(Ordering::Relaxed),
        );
        let mut out = Report {
            hist: std::mem::replace(&mut *st.hist.borrow_mut(), Hist::new()),
            max_gap_ns: 0,
            conn_frames_min: u64::MAX,
            conn_frames_max: 0,
            failed: 0,
        };
        for slot in &st.slots {
            let prev = match slot.last.get() {
                0 => start,
                last => last,
            };
            let gap = slot.max_gap.get().max(end.saturating_sub(prev));
            out.max_gap_ns = out.max_gap_ns.max(gap);
            out.conn_frames_min = out.conn_frames_min.min(slot.replies.get());
            out.conn_frames_max = out.conn_frames_max.max(slot.replies.get());
            out.failed += u64::from(slot.failed.get());
        }
        let _ = report.send(out);
        // Tearing down thousands of connections is not part of the measurement.
        loop {
            std::thread::park();
        }
    })
}
