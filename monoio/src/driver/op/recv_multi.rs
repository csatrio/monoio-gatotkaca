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
        let op = MultishotOp::submit(data).map_err(|(e, _)| e)?;
        Ok(Self {
            op: Some(op),
            eof: false,
            rearm: Cell::new(false),
            cancelled: Cell::new(false),
        })
    }

    fn data(&self) -> &RecvMulti {
        // Only called while the op is there (after `poll_next_cqe` yielded a CQE).
        self.op.as_ref().expect("recv op gone").data()
    }

    /// Next CQE with data (`res > 0`). `None` after EOF, after a cancel, or after an error that
    /// ended the multishot. `ENOBUFS` (the ring ran dry) is yielded as an error; the kernel ends
    /// the multishot with it, so `None` follows. CQEs not yielded have their buffers released.
    ///
    /// The kernel may also end the multishot after a chunk (a bundle recv does whenever it used
    /// the last free buffer, any recv on CQ overflow): it is then submitted again.
    fn poll_next_cqe(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Cqe>>> {
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
            match MultishotOp::submit(data) {
                Ok(op) => self.op = Some(op),
                Err((e, _)) => return Poll::Ready(Some(Err(e))),
            }
        }
        let Some(op) = self.op.as_mut() else {
            return Poll::Ready(None);
        };
        let Some(cqe) = ready!(op.poll_next(cx)) else {
            return Poll::Ready(None);
        };
        let res = cqe.res();
        let more = cqe.more();
        Poll::Ready(match res {
            n if n > 0 => {
                if !more && !self.cancelled.get() {
                    self.rearm.set(true);
                }
                Some(Ok(cqe))
            }
            0 => {
                // EOF: a buffer it may carry goes back to the ring (bundles consume none).
                self.discard(cqe);
                self.eof = true;
                if more {
                    self.cancel();
                }
                None
            }
            n if n == -libc::ECANCELED && !more => None,
            _ => {
                // Counts ENOBUFS in the ring stats.
                self.discard(cqe);
                Some(Err(io::Error::from_raw_os_error(-res)))
            }
        })
    }

    fn discard(&self, cqe: Cqe) {
        let data = self.data();
        if data.bundle {
            drop(data.ring.take_bundle(cqe));
        } else {
            drop(data.ring.take(cqe));
        }
    }

    /// Next filled buffer (not a bundle op), see `poll_next_cqe`.
    pub(crate) fn poll_next_buf(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<SelectedBuf>>> {
        let next = ready!(self.poll_next_cqe(cx));
        Poll::Ready(next.map(|cqe| {
            let data = self.data();
            debug_assert!(!data.bundle);
            data.ring
                .take(cqe?)
                .ok_or_else(|| io::Error::other("recv completion without a provided buffer"))
        }))
    }

    /// Next filled bundle (a bundle op), see `poll_next_cqe`. Never empty.
    pub(crate) fn poll_next_bundle(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Bundle>>> {
        let next = ready!(self.poll_next_cqe(cx));
        Poll::Ready(next.map(|cqe| {
            let data = self.data();
            debug_assert!(data.bundle);
            let bundle = data.ring.take_bundle(cqe?);
            if bundle.is_empty() {
                Err(io::Error::other(
                    "recv completion without a provided buffer",
                ))
            } else {
                Ok(bundle)
            }
        }))
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
        self.op.poll_next_bundle(cx)
    }

    /// The next received bundle; `None` after EOF, cancel, or an error that ended the recv.
    pub async fn next(&mut self) -> Option<io::Result<Bundle>> {
        std::future::poll_fn(|cx| self.op.poll_next_bundle(cx)).await
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
