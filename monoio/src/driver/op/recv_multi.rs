//! Multishot recv into a provided buffer ring (`IORING_RECV_MULTISHOT`, Linux 6.0+), optionally
//! as bundles (`IORING_RECVSEND_BUNDLE`, Linux 6.10+), and the single-shot bundle recv.

use std::{
    cell::Cell,
    io,
    task::{Context, Poll},
};

use io_uring::opcode;

use super::{multishot::MultishotOp, Op, OpAble, OpKind};
// For the net modules, which cannot reach `driver::buf_ring`.
pub(crate) use crate::driver::buf_ring::Bundle;
use crate::{
    driver::{
        raw::{BufRing, Cqe, SelectedBuf},
        shared_fd::{sqe_fd, SharedFd},
        uring::{PreTake, Taken},
    },
    io::stream::Stream,
};

/// Multishot recv: one CQE per received chunk, in one buffer of `ring` (or, with `bundle`, in
/// several).
pub(crate) struct RecvMulti {
    fd: SharedFd,
    ring: BufRing,
    bundle: bool,
}

impl OpAble for RecvMulti {
    const KIND: OpKind = OpKind::Multishot;

    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        let group = self.ring.group();
        sqe_fd!(self.fd, |fd| if self.bundle {
            opcode::RecvMultiBundle::new(fd, group).build()
        } else {
            opcode::RecvMulti::new(fd, group).build()
        })
    }

    fn discard_cqe(&mut self, cqe: Cqe) {
        // Give the buffers it carries back to the ring. `self.ring` keeps the group alive, so
        // this never calls into the driver.
        if self.bundle {
            drop(self.ring.take_bundle(cqe));
        } else {
            drop(self.ring.take(cqe));
        }
    }
}

/// Bundles need `IORING_FEAT_RECVSEND_BUNDLE` (Linux 6.10).
fn check_bundles(ring: &BufRing) -> io::Result<()> {
    if ring.supports_bundles() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "recv bundles need Linux 6.10 (IORING_FEAT_RECVSEND_BUNDLE)",
        ))
    }
}

/// Submit a multishot recv. On an incremental ring the driver takes the buffers when it reaps
/// each CQE: the pieces of one buffer go to many connections, and only the CQ order gives their
/// offsets (and says when the buffer is free again).
fn submit(data: RecvMulti) -> Result<MultishotOp<RecvMulti>, (io::Error, RecvMulti)> {
    let pre = data
        .ring
        .is_incremental()
        .then(|| PreTake::new(data.ring.clone(), data.bundle));
    let op = MultishotOp::submit(data)?;
    if let Some(pre) = pre {
        op.set_pretake(pre);
    }
    Ok(op)
}

/// A CQE with data, and its buffers if the driver took them already.
type Got = (Cqe, Option<Taken>);

/// The receive loop shared by TCP and Unix streams: yields the filled buffers.
pub(crate) struct RecvMultiOp {
    // `None` once a re-arm failed.
    op: Option<MultishotOp<RecvMulti>>,
    // The peer closed (a 0-length CQE): yield `None` from now on.
    eof: bool,
    // The kernel ended the multishot after a chunk: submit it again on the next poll.
    rearm: Cell<bool>,
    // `cancel` was called: never re-arm.
    cancelled: Cell<bool>,
    // The last re-arm was after `ENOBUFS` and no data came since: another `ENOBUFS` means the
    // ring is still empty, so it is reported instead of re-arming in a loop.
    enobufs_retry: Cell<bool>,
    // An error met by `poll_read_into` after it had copied data: returned by the next call.
    #[cfg(feature = "bytes")]
    pending_err: Option<io::Error>,
}

/// What `poll_next_cqe` does with an `ENOBUFS` that ended the multishot.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnEnobufs {
    /// Yield it as an error; the stream ends (`next`).
    Yield,
    /// Re-arm and continue (the buffers the caller copied out are back in the ring). Only if
    /// the re-armed recv hits `ENOBUFS` again before any data is it yielded, and the next poll
    /// re-arms again.
    Rearm,
}

impl RecvMultiOp {
    /// Start a multishot recv on `fd` into buffers of `ring`. With `bundle`, each CQE may cover
    /// several buffers (use [`poll_next_bundle`](Self::poll_next_bundle)); fails with
    /// `Unsupported` if the kernel has no bundles.
    pub(crate) fn new(fd: &SharedFd, ring: &BufRing, bundle: bool) -> io::Result<Self> {
        if bundle {
            check_bundles(ring)?;
        }
        let data = RecvMulti {
            fd: fd.clone(),
            ring: ring.clone(),
            bundle,
        };
        let op = submit(data).map_err(|(e, _)| e)?;
        Ok(Self {
            op: Some(op),
            eof: false,
            rearm: Cell::new(false),
            cancelled: Cell::new(false),
            enobufs_retry: Cell::new(false),
            #[cfg(feature = "bytes")]
            pending_err: None,
        })
    }

    fn data(&self) -> &RecvMulti {
        // Only called while the op is there (after `poll_next_cqe` yielded a CQE).
        self.op.as_ref().expect("recv op gone").data()
    }

    /// Next CQE with data (`res > 0`). `None` after EOF, after a cancel, or after an error that
    /// ended the multishot. `ENOBUFS` (the ring ran dry) ends the multishot: see [`OnEnobufs`].
    /// CQEs not yielded have their buffers released.
    ///
    /// The kernel may also end the multishot after a chunk (a bundle recv does whenever it used
    /// the last free buffer, any recv on CQ overflow): it is then submitted again.
    fn poll_next_cqe(
        &mut self,
        cx: &mut Context<'_>,
        on_enobufs: OnEnobufs,
    ) -> Poll<Option<io::Result<Got>>> {
        loop {
            if self.eof {
                return Poll::Ready(None);
            }
            if self.rearm.replace(false) {
                let Some(op) = self.op.take() else {
                    return Poll::Ready(None);
                };
                // The final CQE was taken, so the data is free.
                let Ok(data) = op.into_data() else {
                    unreachable!("re-arm before the final CQE")
                };
                match submit(data) {
                    Ok(op) => self.op = Some(op),
                    Err((e, _)) => return Poll::Ready(Some(Err(e))),
                }
            }
            let Some(op) = self.op.as_mut() else {
                return Poll::Ready(None);
            };
            let Some((cqe, taken)) = ready!(op.poll_next_taken(cx)) else {
                return Poll::Ready(None);
            };
            let res = cqe.res();
            let more = cqe.more();
            return Poll::Ready(match res {
                n if n > 0 => {
                    if !more && !self.cancelled.get() {
                        self.rearm.set(true);
                    }
                    self.enobufs_retry.set(false);
                    Some(Ok((cqe, taken)))
                }
                0 => {
                    // EOF: a buffer it may carry goes back to the ring (bundles consume none).
                    self.discard(cqe, taken);
                    self.eof = true;
                    if more {
                        self.cancel();
                    }
                    None
                }
                n if n == -libc::ECANCELED && !more => None,
                n if n == -libc::ENOBUFS
                    && !more
                    && on_enobufs == OnEnobufs::Rearm
                    && !self.cancelled.get() =>
                {
                    // Counts ENOBUFS in the ring stats.
                    self.discard(cqe, taken);
                    self.rearm.set(true);
                    if !self.enobufs_retry.replace(true) {
                        // The caller copied its buffers out: they are free again.
                        continue;
                    }
                    // Still no free buffer after a re-arm: someone else holds them. Report
                    // it; the next poll re-arms again.
                    Some(Err(io::Error::from_raw_os_error(libc::ENOBUFS)))
                }
                _ => {
                    // Counts ENOBUFS in the ring stats.
                    self.discard(cqe, taken);
                    Some(Err(io::Error::from_raw_os_error(-res)))
                }
            });
        }
    }

    // Release the buffers of `cqe` (counting `ENOBUFS` in the ring stats).
    fn discard(&self, cqe: Cqe, taken: Option<Taken>) {
        if taken.is_none() {
            let data = self.data();
            if data.bundle {
                drop(data.ring.take_bundle(cqe));
            } else {
                drop(data.ring.take(cqe));
            }
        }
    }

    // The buffer of a non-bundle CQE.
    fn buf(&self, (cqe, taken): Got) -> Option<SelectedBuf> {
        match taken {
            Some(Taken::Buf(buf)) => buf,
            Some(Taken::Bundle(_)) => unreachable!("bundle taken for a plain recv"),
            None => self.data().ring.take(cqe),
        }
    }

    // The buffers of a bundle CQE.
    fn bundle(&self, (cqe, taken): Got) -> Bundle {
        match taken {
            Some(Taken::Bundle(bundle)) => bundle,
            Some(Taken::Buf(_)) => unreachable!("buffer taken for a bundle recv"),
            None => self.data().ring.take_bundle(cqe),
        }
    }

    /// Next filled buffer (not a bundle op), see `poll_next_cqe`.
    pub(crate) fn poll_next_buf(
        &mut self,
        cx: &mut Context<'_>,
        on_enobufs: OnEnobufs,
    ) -> Poll<Option<io::Result<SelectedBuf>>> {
        let next = match ready!(self.poll_next_cqe(cx, on_enobufs)) {
            Some(Ok(cqe)) => cqe,
            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            None => return Poll::Ready(None),
        };
        debug_assert!(!self.data().bundle);
        Poll::Ready(Some(self.buf(next).ok_or_else(|| {
            io::Error::other("recv completion without a provided buffer")
        })))
    }

    /// Next filled bundle (a bundle op), see `poll_next_cqe`. Never empty.
    pub(crate) fn poll_next_bundle(
        &mut self,
        cx: &mut Context<'_>,
        on_enobufs: OnEnobufs,
    ) -> Poll<Option<io::Result<Bundle>>> {
        let next = match ready!(self.poll_next_cqe(cx, on_enobufs)) {
            Some(Ok(cqe)) => cqe,
            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            None => return Poll::Ready(None),
        };
        debug_assert!(self.data().bundle);
        let bundle = self.bundle(next);
        Poll::Ready(Some(if bundle.is_empty() {
            Err(io::Error::other(
                "recv completion without a provided buffer",
            ))
        } else {
            Ok(bundle)
        }))
    }

    /// Copy what was received into `dst` (bundle or not): waits for the first chunk, then takes
    /// every chunk already queued without waiting, and gives each buffer back to the ring right
    /// after its copy. `Ok(0)` once the recv ended (EOF, cancel, or after an error was returned).
    /// `ENOBUFS` re-arms (see [`OnEnobufs::Rearm`]). An error after some data is returned by
    /// the next call.
    #[cfg(feature = "bytes")]
    pub(crate) fn poll_read_into(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut bytes::BytesMut,
    ) -> Poll<io::Result<usize>> {
        if let Some(e) = self.pending_err.take() {
            return Poll::Ready(Err(e));
        }
        let mut n = 0;
        loop {
            let got = match self.poll_next_cqe(cx, OnEnobufs::Rearm) {
                Poll::Pending if n == 0 => return Poll::Pending,
                Poll::Pending | Poll::Ready(None) => return Poll::Ready(Ok(n)),
                Poll::Ready(Some(Err(e))) => {
                    if n == 0 {
                        return Poll::Ready(Err(e));
                    }
                    self.pending_err = Some(e);
                    return Poll::Ready(Ok(n));
                }
                Poll::Ready(Some(Ok(got))) => got,
            };
            if self.data().bundle {
                let bundle = self.bundle(got);
                dst.reserve(bundle.bytes());
                for buf in bundle {
                    dst.extend_from_slice(&buf);
                    n += buf.len();
                }
            } else {
                match self.buf(got) {
                    Some(buf) => {
                        dst.extend_from_slice(&buf);
                        n += buf.len();
                    }
                    None => {
                        let e = io::Error::other("recv completion without a provided buffer");
                        if n == 0 {
                            return Poll::Ready(Err(e));
                        }
                        self.pending_err = Some(e);
                        return Poll::Ready(Ok(n));
                    }
                }
            }
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.set(true);
        self.rearm.set(false);
        if let Some(op) = &self.op {
            op.cancel()
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        let op_done = match &self.op {
            Some(op) => op.is_finished(),
            None => true,
        };
        self.eof || !self.rearm.get() && op_done
    }
}

/// Single-shot bundle recv: one CQE, its data in one or more buffers of `ring`.
pub(crate) struct RecvBundle {
    fd: SharedFd,
    ring: BufRing,
}

impl OpAble for RecvBundle {
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        sqe_fd!(self.fd, |fd| opcode::RecvBundle::new(fd, self.ring.group())
            .build())
    }
}

/// Receive once into one or more buffers of `ring`. An empty bundle means EOF.
pub(crate) async fn recv_bundle(fd: &SharedFd, ring: &BufRing) -> io::Result<Bundle> {
    check_bundles(ring)?;
    let op = Op::submit_with(RecvBundle {
        fd: fd.clone(),
        ring: ring.clone(),
    })?;
    let done = op.await;
    let ring = &done.data.ring;
    match done.meta.result {
        Ok(n) => {
            let res = n.into_inner() as i32;
            let bundle = ring.take_bundle(Cqe::new(res, done.meta.flags));
            if res > 0 && bundle.is_empty() {
                return Err(io::Error::other(
                    "recv completion without a provided buffer",
                ));
            }
            Ok(bundle)
        }
        Err(e) => {
            // Counts ENOBUFS in the ring stats; nothing else to release.
            let errno = e.raw_os_error().unwrap_or(0);
            drop(ring.take_bundle(Cqe::new(-errno, done.meta.flags)));
            Err(e)
        }
    }
}

/// Bundles received by a multishot bundle recv, see [`TcpStream::recv_multi_bundle`](crate::net::TcpStream::recv_multi_bundle) (and
/// `UnixStream::recv_multi_bundle`). Behaves like [`RecvStream`](crate::net::tcp::RecvStream) (EOF, `ENOBUFS` and re-arm,
/// cancel, drop), with a non-empty [`Bundle`] per item instead of one buffer.
///
/// Dropping the stream cancels the recv and gives every buffer of the bundles not taken back to
/// the ring.
#[must_use = "streams do nothing unless polled"]
pub struct RecvBundleStream {
    op: RecvMultiOp,
}

impl RecvBundleStream {
    pub(crate) fn new(op: RecvMultiOp) -> Self {
        Self { op }
    }

    /// Poll for the next received bundle.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bundle>>> {
        self.op.poll_next_bundle(cx, OnEnobufs::Yield)
    }

    /// The next received bundle; `None` after EOF, cancel, or an error that ended the recv.
    pub async fn next(&mut self) -> Option<io::Result<Bundle>> {
        std::future::poll_fn(|cx| self.op.poll_next_bundle(cx, OnEnobufs::Yield)).await
    }

    /// Copy the next received bundle(s) into `dst`, giving the buffers back to the ring at
    /// once; `Ok(0)` at EOF. Same rules as
    /// [`RecvStream::read_into`](crate::net::tcp::RecvStream::read_into), including the
    /// automatic re-arm after `ENOBUFS`.
    #[cfg(feature = "bytes")]
    pub async fn read_into(&mut self, dst: &mut bytes::BytesMut) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.op.poll_read_into(cx, dst)).await
    }

    /// Poll form of [`read_into`](Self::read_into).
    #[cfg(feature = "bytes")]
    pub fn poll_read_into(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut bytes::BytesMut,
    ) -> Poll<io::Result<usize>> {
        self.op.poll_read_into(cx, dst)
    }

    /// Stop receiving (AsyncCancel). Bundles already received are still returned, then `next`
    /// returns `None`.
    pub fn cancel(&self) {
        self.op.cancel()
    }

    /// Whether the recv ended (`next` returns `None` from now on).
    pub fn is_finished(&self) -> bool {
        self.op.is_finished()
    }
}

impl Stream for RecvBundleStream {
    type Item = io::Result<Bundle>;

    async fn next(&mut self) -> Option<Self::Item> {
        RecvBundleStream::next(self).await
    }
}

impl std::fmt::Debug for RecvBundleStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvBundleStream")
            .field("finished", &self.is_finished())
            .finish()
    }
}
