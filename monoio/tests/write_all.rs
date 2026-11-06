//! `AsyncWriteRentExt::write_all` against a scripted writer: which ranges of the buffer each
//! `write` gets, and what comes back, for a whole write, short writes, `Interrupted`, a zero
//! write, an error and an empty buffer. Plus one run over a real socket with short sends.

use std::{collections::VecDeque, io};

use monoio::{
    buf::{IoBuf, IoVecBuf},
    io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt},
    net::{TcpListener, TcpStream},
    BufResult,
};

/// What the next `write` call does.
enum Step {
    /// Accept at most this many bytes.
    Take(usize),
    Fail(io::ErrorKind),
}

/// Records every `write` as `(offset into the caller's buffer, length offered)`.
struct Scripted {
    base: usize,
    script: VecDeque<Step>,
    calls: Vec<(usize, usize)>,
    out: Vec<u8>,
}

impl Scripted {
    fn new(base: *const u8, script: impl IntoIterator<Item = Step>) -> Self {
        Self {
            base: base as usize,
            script: script.into_iter().collect(),
            calls: Vec::new(),
            out: Vec::new(),
        }
    }
}

impl AsyncWriteRent for Scripted {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        let (ptr, len) = (buf.read_ptr(), buf.bytes_init());
        self.calls.push((ptr as usize - self.base, len));
        match self.script.pop_front().expect("more writes than scripted") {
            Step::Take(max) => {
                let n = max.min(len);
                self.out
                    .extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, n) });
                (Ok(n), buf)
            }
            Step::Fail(kind) => (Err(kind.into()), buf),
        }
    }

    async fn writev<T: IoVecBuf>(&mut self, _buf_vec: T) -> BufResult<usize, T> {
        unreachable!("write_all does not use writev")
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn data(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

#[monoio::test]
async fn whole_buffer_in_one_write() {
    let buf = data(100);
    let mut w = Scripted::new(buf.as_ptr(), [Step::Take(usize::MAX)]);
    let (res, buf) = w.write_all(buf).await;
    assert_eq!(res.unwrap(), 100);
    assert_eq!(w.calls, [(0, 100)]);
    assert_eq!(w.out, buf);
}

#[monoio::test]
async fn short_writes_send_the_rest() {
    let buf = data(100);
    let mut w = Scripted::new(
        buf.as_ptr(),
        [
            Step::Take(30),
            Step::Take(1),
            Step::Take(50),
            Step::Take(usize::MAX),
        ],
    );
    let (res, buf) = w.write_all(buf).await;
    assert_eq!(res.unwrap(), 100);
    assert_eq!(w.calls, [(0, 100), (30, 70), (31, 69), (81, 19)]);
    assert_eq!(w.out, buf);
}

#[monoio::test]
async fn interrupted_is_retried() {
    let buf = data(64);
    let mut w = Scripted::new(
        buf.as_ptr(),
        [
            // On the first write and on a later one.
            Step::Fail(io::ErrorKind::Interrupted),
            Step::Take(10),
            Step::Fail(io::ErrorKind::Interrupted),
            Step::Take(usize::MAX),
        ],
    );
    let (res, buf) = w.write_all(buf).await;
    assert_eq!(res.unwrap(), 64);
    assert_eq!(w.calls, [(0, 64), (0, 64), (10, 54), (10, 54)]);
    assert_eq!(w.out, buf);
}

#[monoio::test]
async fn zero_write_is_an_error() {
    // On the first write.
    let buf = data(8);
    let mut w = Scripted::new(buf.as_ptr(), [Step::Take(0)]);
    let (res, buf) = w.write_all(buf).await;
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::WriteZero);
    assert_eq!(buf.len(), 8);
    assert_eq!(w.calls, [(0, 8)]);

    // After a short write.
    let mut w = Scripted::new(buf.as_ptr(), [Step::Take(3), Step::Take(0)]);
    let (res, buf) = w.write_all(buf).await;
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::WriteZero);
    assert_eq!(buf.len(), 8);
    assert_eq!(w.calls, [(0, 8), (3, 5)]);
}

#[monoio::test]
async fn error_returns_the_buffer() {
    let buf = data(8);
    let mut w = Scripted::new(buf.as_ptr(), [Step::Fail(io::ErrorKind::BrokenPipe)]);
    let (res, buf) = w.write_all(buf).await;
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buf, data(8));
    assert_eq!(w.calls, [(0, 8)]);

    let mut w = Scripted::new(
        buf.as_ptr(),
        [Step::Take(5), Step::Fail(io::ErrorKind::ConnectionReset)],
    );
    let (res, buf) = w.write_all(buf).await;
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(buf, data(8));
    assert_eq!(w.calls, [(0, 8), (5, 3)]);
}

#[monoio::test]
async fn empty_buffer_writes_nothing() {
    let buf = Vec::<u8>::new();
    let mut w = Scripted::new(buf.as_ptr(), []);
    let (res, _) = w.write_all(buf).await;
    assert_eq!(res.unwrap(), 0);
    assert!(w.calls.is_empty());

    // A slice with an empty range, and a static empty slice.
    let buf = data(8);
    let mut w = Scripted::new(buf.as_ptr(), []);
    let (res, _) = w.write_all(buf.slice(4..4)).await;
    assert_eq!(res.unwrap(), 0);
    let (res, _) = w.write_all(&b""[..]).await;
    assert_eq!(res.unwrap(), 0);
    assert!(w.calls.is_empty());
}

#[monoio::test]
async fn slice_of_a_buffer_keeps_its_range() {
    let buf = data(100);
    let base = buf.as_ptr();
    let mut w = Scripted::new(base, [Step::Take(10), Step::Take(usize::MAX)]);
    let (res, slice) = w.write_all(buf.slice(20..60)).await;
    assert_eq!(res.unwrap(), 40);
    assert_eq!(w.calls, [(20, 40), (30, 30)]);
    assert_eq!((slice.begin(), slice.end()), (20, 60));
    let buf = slice.into_inner();
    assert_eq!(w.out, buf[20..60]);
}

/// Over a real socket: a buffer much larger than the socket buffers, read slowly, so the sends
/// come back short and the loop after the first write runs.
#[monoio::test(timer_enabled = true)]
async fn large_buffer_over_tcp() {
    const LEN: usize = 8 << 20;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let reader = monoio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut got = Vec::with_capacity(LEN);
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let (res, c) = s.read(chunk).await;
            chunk = c;
            let n = res.unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&chunk[..n]);
            if got.len() < LEN / 2 {
                monoio::time::sleep(std::time::Duration::from_micros(200)).await;
            }
        }
        got
    });
    let mut s = TcpStream::connect_addr(addr).await.unwrap();
    let (res, buf) = s.write_all(data(LEN)).await;
    assert_eq!(res.unwrap(), LEN);
    s.shutdown().await.unwrap();
    drop(s);
    let got = reader.await;
    assert_eq!(got.len(), LEN);
    assert!(got == buf);
}
