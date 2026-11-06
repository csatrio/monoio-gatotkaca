//! Measurement helpers: thread CPU time, hardware counters, a latency histogram, the fd
//! limit. The counter code is the one of `smallframes`.

use std::{io, time::Duration};

/// CPU time (user + system) of the calling thread.
pub fn thread_cpu() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `ru` is a valid out pointer.
    unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut ru) };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// A hardware counter of the calling thread (`perf_event_open`), `None` if not allowed.
struct Counter(Option<i32>);

impl Counter {
    /// `config`: 0 cycles, 1 instructions. `user_only` excludes kernel and hypervisor.
    fn new(config: u64, user_only: bool) -> Self {
        // struct perf_event_attr, PERF_ATTR_SIZE_VER5 (112 bytes).
        let mut attr = [0_u64; 14];
        attr[0] = 112 << 32; // type PERF_TYPE_HARDWARE (0), size
        attr[1] = config;
        // flags bitfield: exclude_kernel (bit 5), exclude_hv (bit 6)
        if user_only {
            attr[5] = (1 << 5) | (1 << 6);
        }
        // SAFETY: valid attr buffer; pid 0 = this thread, cpu -1 = any.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                attr.as_ptr(),
                0,
                -1,
                -1,
                8_u64, // PERF_FLAG_FD_CLOEXEC
            )
        };
        Counter((fd >= 0).then_some(fd as i32))
    }

    fn read(&self) -> Option<u64> {
        let fd = self.0?;
        let mut v = 0_u64;
        // SAFETY: reads 8 bytes into `v`.
        let n = unsafe { libc::read(fd, (&mut v as *mut u64).cast(), 8) };
        (n == 8).then_some(v)
    }
}

/// The three counters of the server thread: user cycles, user instructions, all cycles.
pub struct Perf([Counter; 3]);

impl Perf {
    /// Opens the counters for the calling thread.
    pub fn open() -> Self {
        Perf([
            Counter::new(0, true),
            Counter::new(1, true),
            Counter::new(0, false),
        ])
    }

    pub fn read(&self) -> [Option<u64>; 3] {
        self.0.each_ref().map(Counter::read)
    }
}

/// Log-linear histogram of nanoseconds: exact below 64 ns, then 32 buckets per power of two
/// (bucket width at most 3.2% of the value). One add per sample.
pub struct Hist {
    counts: Box<[u64]>,
    pub n: u64,
    pub max: u64,
    pub sum: u64,
}

const SUB_BITS: u32 = 5;
const SUB: u64 = 1 << SUB_BITS;
/// Buckets for every `u64`.
const BUCKETS: usize = ((64 - SUB_BITS + 1) as usize) << SUB_BITS;

impl Hist {
    pub fn new() -> Self {
        Self {
            counts: vec![0; BUCKETS].into_boxed_slice(),
            n: 0,
            max: 0,
            sum: 0,
        }
    }

    #[inline]
    fn index(v: u64) -> usize {
        if v < 2 * SUB {
            return v as usize;
        }
        let shift = 63 - v.leading_zeros() - SUB_BITS;
        // `v >> shift` is in `SUB..2 * SUB`.
        ((shift as usize) << SUB_BITS) + (v >> shift) as usize
    }

    /// The middle of bucket `i`.
    fn value(i: usize) -> u64 {
        if (i as u64) < 2 * SUB {
            return i as u64;
        }
        let shift = (i >> SUB_BITS) as u32 - 1;
        let low = (i as u64 - ((shift as u64) << SUB_BITS)) << shift;
        low + (1 << shift) / 2
    }

    #[inline]
    pub fn record(&mut self, ns: u64) {
        self.counts[Self::index(ns)] += 1;
        self.n += 1;
        self.sum += ns;
        self.max = self.max.max(ns);
    }

    pub fn merge(&mut self, other: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a += b;
        }
        self.n += other.n;
        self.sum += other.sum;
        self.max = self.max.max(other.max);
    }

    /// The value at quantile `q` (0..=1) in nanoseconds, `None` without samples.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        if self.n == 0 {
            return None;
        }
        let rank = ((q * self.n as f64).ceil() as u64).clamp(1, self.n);
        let mut seen = 0;
        for (i, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(Self::value(i).min(self.max));
            }
        }
        Some(self.max)
    }
}

/// Raises the soft `RLIMIT_NOFILE` to `need` if it is lower, as far as the hard limit allows.
/// Returns `Some((old, new))` when it changed the limit.
pub fn raise_nofile(need: u64) -> io::Result<Option<(u64, u64)>> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `lim` is a valid out pointer.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let old = lim.rlim_cur;
    if old >= need {
        return Ok(None);
    }
    if lim.rlim_max < need {
        return Err(io::Error::other(format!(
            "needs {need} file descriptors, the hard limit is {} (ulimit -Hn)",
            lim.rlim_max
        )));
    }
    lim.rlim_cur = need;
    // SAFETY: `lim` is a valid rlimit.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(Some((old, need)))
}
