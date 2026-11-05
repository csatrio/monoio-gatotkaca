//! Regression: `write_vectored_all` / `read_vectored_exact` after a short result must continue
//! at the right iovec and byte (the iovec count and start were wrong after a partial result).

use std::future::{ready, Future};

use monoio::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut, VecBuf},
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    BufResult,
};

/// Takes at most `step` bytes per call.
struct Trickle {
    step: usize,
    out: Vec<u8>,
    src: Vec<u8>,
    calls: usize,
}

impl Trickle {
    fn new(step: usize, src: Vec<u8>) -> Self {
        Self {
            step,
            out: Vec::new(),
            src,
            calls: 0,
        }
    }
}

impl AsyncWriteRent for Trickle {
    fn write<T: IoBuf>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        let n = buf.bytes_init().min(self.step);
        self.out.extend_from_slice(&buf.as_slice()[..n]);
        ready((Ok(n), buf))
    }

    fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> impl Future<Output = BufResult<usize, T>> {
        self.calls += 1;
        let iovs = unsafe {
            std::slice::from_raw_parts(buf_vec.read_iovec_ptr(), buf_vec.read_iovec_len())
        };
        let mut left = self.step;
        for v in iovs {
            let n = v.iov_len.min(left);
            let bytes = unsafe { std::slice::from_raw_parts(v.iov_base as *const u8, n) };
            self.out.extend_from_slice(bytes);
            left -= n;
            if left == 0 {
                break;
            }
        }
        ready((Ok(self.step - left), buf_vec))
    }

    fn flush(&mut self) -> impl Future<Output = std::io::Result<()>> {
        ready(Ok(()))
    }

    fn shutdown(&mut self) -> impl Future<Output = std::io::Result<()>> {
        ready(Ok(()))
    }
}

impl AsyncReadRent for Trickle {
    fn read<T: IoBufMut>(&mut self, mut buf: T) -> impl Future<Output = BufResult<usize, T>> {
        let n = buf.bytes_total().min(self.step).min(self.src.len());
        unsafe {
            std::ptr::copy_nonoverlapping(self.src.as_ptr(), buf.write_ptr(), n);
            buf.set_init(n);
        }
        self.src.drain(..n);
        ready((Ok(n), buf))
    }

    fn readv<T: IoVecBufMut>(&mut self, mut buf: T) -> impl Future<Output = BufResult<usize, T>> {
        self.calls += 1;
        let iovs =
            unsafe { std::slice::from_raw_parts(buf.write_iovec_ptr(), buf.write_iovec_len()) };
        let mut done = 0;
        for v in iovs {
            let n = v.iov_len.min(self.step - done).min(self.src.len() - done);
            unsafe {
                std::ptr::copy_nonoverlapping(self.src.as_ptr().add(done), v.iov_base as *mut u8, n)
            };
            done += n;
            if done == self.step || done == self.src.len() {
                break;
            }
        }
        self.src.drain(..done);
        // Like the real readv op: report the filled bytes to the buffer.
        unsafe { buf.set_init(done) };
        ready((Ok(done), buf))
    }
}

fn payload(slices: &[usize]) -> Vec<Vec<u8>> {
    let mut b = 0u8;
    slices
        .iter()
        .map(|&len| {
            (0..len)
                .map(|_| {
                    b = b.wrapping_add(1);
                    b
                })
                .collect()
        })
        .collect()
}

#[monoio::test]
async fn write_vectored_all_after_partial_writes() {
    for step in [1, 2, 3, 5, 7, 64] {
        let parts = payload(&[4, 1, 9, 2, 6]);
        let expected: Vec<u8> = parts.concat();
        let mut w = Trickle::new(step, Vec::new());
        let (res, buf) = w.write_vectored_all(VecBuf::from(parts.clone())).await;
        assert_eq!(res.unwrap(), expected.len());
        assert_eq!(w.out, expected, "step {step}");
        assert_eq!(w.calls, expected.len().div_ceil(step));
        let back: Vec<Vec<u8>> = buf.into();
        assert_eq!(back, parts);
    }
}

#[monoio::test]
async fn read_vectored_exact_after_partial_reads() {
    for step in [1, 2, 3, 5, 7, 64] {
        let src: Vec<u8> = payload(&[22]).concat();
        let mut r = Trickle::new(step, src.clone());
        let bufs = VecBuf::from(vec![vec![0u8; 4], vec![0u8; 1], vec![0u8; 9], vec![0u8; 8]]);
        let (res, buf) = r.read_vectored_exact(bufs).await;
        assert_eq!(res.unwrap(), 22);
        let got: Vec<Vec<u8>> = buf.into();
        assert_eq!(got.concat(), src, "step {step}");
        assert_eq!(r.calls, 22usize.div_ceil(step));
    }
}

#[monoio::test]
async fn write_vectored_all_over_socket_with_small_sndbuf() {
    use monoio::net::{TcpListener, TcpStream};
    use std::os::unix::io::AsRawFd;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let parts = payload(&[100_000, 3, 250_000, 1, 77_777]);
    let expected: Vec<u8> = parts.concat();
    let total = expected.len();
    let writer = monoio::spawn(async move {
        let mut c = TcpStream::connect(addr).await.unwrap();
        let size: monoio::uring::libc::c_int = 4096;
        unsafe {
            monoio::uring::libc::setsockopt(
                c.as_raw_fd(),
                monoio::uring::libc::SOL_SOCKET,
                monoio::uring::libc::SO_SNDBUF,
                &size as *const _ as *const _,
                std::mem::size_of_val(&size) as _,
            );
        }
        let (res, _) = c.write_vectored_all(VecBuf::from(parts)).await;
        assert_eq!(res.unwrap(), total);
    });
    let (mut s, _) = listener.accept().await.unwrap();
    let (res, buf) = s.read_exact(vec![0u8; total]).await;
    res.unwrap();
    assert!(buf == expected, "bytes out of order");
    writer.await;
}
