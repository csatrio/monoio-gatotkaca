//! Fixed-capacity vectored buffers that reuse one iovec array.

use super::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};

const EMPTY_IOVEC: libc::iovec = libc::iovec {
    iov_base: std::ptr::null_mut(),
    iov_len: 0,
};

/// Up to `N` buffers for one vectored write, with a reusable boxed iovec array.
///
/// Keep one per connection and refill it: `push` buffers, write, then `clear` or `drain`. No
/// allocation happens per write. The iovec array is boxed, so its address stays stable when the
/// value moves into an op.
///
/// ```
/// use monoio::buf::{IoVecArray, IoVecBuf};
///
/// let mut v: IoVecArray<Vec<u8>, 4> = IoVecArray::new();
/// v.push(b"head ".to_vec()).unwrap();
/// v.push(b"body".to_vec()).unwrap();
/// assert_eq!(v.len(), 2);
/// assert_eq!(v.total_len(), 9);
/// let bufs: Vec<_> = v.drain().collect();
/// assert_eq!(bufs.len(), 2);
/// assert!(v.is_empty());
/// ```
pub struct IoVecArray<B: IoBuf, const N: usize> {
    bufs: [Option<B>; N],
    iov: Box<[libc::iovec; N]>,
    len: usize,
}

impl<B: IoBuf, const N: usize> IoVecArray<B, N> {
    /// Creates an empty array (allocates the iovec box once).
    pub fn new() -> Self {
        Self {
            bufs: std::array::from_fn(|_| None),
            iov: Box::new([EMPTY_IOVEC; N]),
            len: 0,
        }
    }

    /// Appends `buf`. Gives it back if the array is full.
    pub fn push(&mut self, buf: B) -> Result<(), B> {
        if self.len == N {
            return Err(buf);
        }
        self.iov[self.len] = libc::iovec {
            iov_base: buf.read_ptr() as *mut _,
            iov_len: buf.bytes_init(),
        };
        self.bufs[self.len] = Some(buf);
        self.len += 1;
        Ok(())
    }

    /// Number of buffers.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// True if there are no buffers.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// True if no more buffers fit.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len == N
    }

    /// Maximum number of buffers (`N`).
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Bytes still to write (sum of the iovec lengths).
    pub fn total_len(&self) -> usize {
        self.iov[..self.len].iter().map(|v| v.iov_len).sum()
    }

    /// The buffer at `index`.
    pub fn get(&self, index: usize) -> Option<&B> {
        self.bufs.get(index).and_then(|b| b.as_ref())
    }

    /// Drops all buffers; keeps the iovec box.
    pub fn clear(&mut self) {
        self.drain().for_each(drop);
    }

    /// Takes all buffers out in order, leaving the array empty and reusable.
    pub fn drain(&mut self) -> impl Iterator<Item = B> + '_ {
        let len = std::mem::take(&mut self.len);
        self.bufs[..len].iter_mut().filter_map(Option::take)
    }

    /// Marks `n` bytes as written after a short write: drops buffers that were fully written
    /// and moves the start of the next one. Call it before writing the rest.
    pub fn advance(&mut self, mut n: usize) {
        let mut done = 0;
        while done < self.len && self.iov[done].iov_len <= n {
            n -= self.iov[done].iov_len;
            self.bufs[done] = None;
            done += 1;
        }
        if done > 0 {
            self.bufs[..self.len].rotate_left(done);
            self.iov[..self.len].rotate_left(done);
            self.len -= done;
        }
        if self.len > 0 && n > 0 {
            let v = &mut self.iov[0];
            let n = n.min(v.iov_len);
            // Safety: `n` is within the buffer.
            v.iov_base = unsafe { (v.iov_base as *mut u8).add(n) } as *mut _;
            v.iov_len -= n;
        }
    }
}

impl<B: IoBuf, const N: usize> Default for IoVecArray<B, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: IoBuf + std::fmt::Debug, const N: usize> std::fmt::Debug for IoVecArray<B, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.bufs[..self.len].iter().flatten())
            .finish()
    }
}

// Safety: the iovecs are boxed (stable address) and point into the owned buffers, whose
// pointers are stable by the `IoBuf` contract.
unsafe impl<B: IoBuf, const N: usize> IoVecBuf for IoVecArray<B, N> {
    #[inline]
    fn read_iovec_ptr(&self) -> *const libc::iovec {
        self.iov.as_ptr()
    }

    #[inline]
    fn read_iovec_len(&self) -> usize {
        self.len
    }
}

/// Up to `N` buffers for one vectored read, with a reusable boxed iovec array.
///
/// Each buffer is filled from its start up to its capacity, in order. After the read,
/// `drain` gives the buffers back with their initialized length set.
///
/// ```
/// use monoio::buf::IoVecArrayMut;
///
/// let mut v: IoVecArrayMut<Vec<u8>, 2> = IoVecArrayMut::new();
/// v.push(Vec::with_capacity(4)).unwrap();
/// v.push(Vec::with_capacity(8)).unwrap();
/// assert_eq!(v.total_capacity(), 12);
/// ```
pub struct IoVecArrayMut<B: IoBufMut, const N: usize> {
    bufs: [Option<B>; N],
    iov: Box<[libc::iovec; N]>,
    len: usize,
}

impl<B: IoBufMut, const N: usize> IoVecArrayMut<B, N> {
    /// Creates an empty array (allocates the iovec box once).
    pub fn new() -> Self {
        Self {
            bufs: std::array::from_fn(|_| None),
            iov: Box::new([EMPTY_IOVEC; N]),
            len: 0,
        }
    }

    /// Appends `buf` as a read target. Gives it back if the array is full.
    pub fn push(&mut self, mut buf: B) -> Result<(), B> {
        if self.len == N {
            return Err(buf);
        }
        self.iov[self.len] = libc::iovec {
            iov_base: buf.write_ptr() as *mut _,
            iov_len: buf.bytes_total(),
        };
        self.bufs[self.len] = Some(buf);
        self.len += 1;
        Ok(())
    }

    /// Number of buffers.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// True if there are no buffers.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Maximum number of buffers (`N`).
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Bytes a read can fill (sum of the iovec lengths).
    pub fn total_capacity(&self) -> usize {
        self.iov[..self.len].iter().map(|v| v.iov_len).sum()
    }

    /// The buffer at `index`.
    pub fn get(&self, index: usize) -> Option<&B> {
        self.bufs.get(index).and_then(|b| b.as_ref())
    }

    /// Drops all buffers; keeps the iovec box.
    pub fn clear(&mut self) {
        self.drain().for_each(drop);
    }

    /// Takes all buffers out in order, leaving the array empty and reusable.
    pub fn drain(&mut self) -> impl Iterator<Item = B> + '_ {
        let len = std::mem::take(&mut self.len);
        self.bufs[..len].iter_mut().filter_map(Option::take)
    }
}

impl<B: IoBufMut, const N: usize> Default for IoVecArrayMut<B, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: IoBufMut + std::fmt::Debug, const N: usize> std::fmt::Debug for IoVecArrayMut<B, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.bufs[..self.len].iter().flatten())
            .finish()
    }
}

// Safety: as for `IoVecArray`.
unsafe impl<B: IoBufMut, const N: usize> IoVecBufMut for IoVecArrayMut<B, N> {
    #[inline]
    fn write_iovec_ptr(&mut self) -> *mut libc::iovec {
        self.iov.as_mut_ptr()
    }

    #[inline]
    fn write_iovec_len(&mut self) -> usize {
        self.len
    }

    unsafe fn set_init(&mut self, mut pos: usize) {
        for i in 0..self.len {
            let n = pos.min(self.iov[i].iov_len);
            if let Some(buf) = self.bufs[i].as_mut() {
                buf.set_init(n);
            }
            pos -= n;
            if pos == 0 {
                break;
            }
        }
    }
}
