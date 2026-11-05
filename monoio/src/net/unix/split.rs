use std::io;

use super::{SocketAddr, UnixStream};
use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf},
    io::{
        as_fd::{AsReadFd, AsWriteFd, SharedFdWrapper},
        OwnedReadHalf, OwnedWriteHalf,
    },
    net::MsgFlags,
    BufResult,
};

/// OwnedReadHalf.
pub type UnixOwnedReadHalf = OwnedReadHalf<UnixStream>;

/// OwnedWriteHalf.
pub type UnixOwnedWriteHalf = OwnedWriteHalf<UnixStream>;

impl UnixOwnedReadHalf {
    /// Returns the remote address that this stream is connected to.
    #[inline]
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        let raw_stream = unsafe { &mut *self.0.get() };
        raw_stream.peer_addr()
    }

    /// Returns the local address that this stream is bound to.
    #[inline]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let raw_stream = unsafe { &mut *self.0.get() };
        raw_stream.local_addr()
    }

    /// See [`UnixStream::recv_with_flags`].
    pub async fn recv_with_flags<T: IoBufMut>(
        &mut self,
        buf: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        // Safety: halves of one stream share a thread; the op only clones the fd.
        let stream = unsafe { &*self.0.get() };
        stream.recv_with_flags(buf, flags).await
    }

    /// See [`UnixStream::recv_poll_first`].
    pub async fn recv_poll_first<T: IoBufMut>(
        &mut self,
        buf: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        let stream = unsafe { &*self.0.get() };
        stream.recv_poll_first(buf, flags).await
    }
}

impl UnixOwnedWriteHalf {
    /// See [`UnixStream::send_with_flags`].
    pub async fn send_with_flags<T: IoBuf>(
        &mut self,
        buf: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        // Safety: halves of one stream share a thread; the op only clones the fd.
        let stream = unsafe { &*self.0.get() };
        stream.send_with_flags(buf, flags).await
    }

    /// See [`UnixStream::send_vectored`].
    pub async fn send_vectored<T: IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        let stream = unsafe { &*self.0.get() };
        stream.send_vectored(buf_vec).await
    }

    /// See [`UnixStream::send_vectored_with_flags`].
    pub async fn send_vectored_with_flags<T: IoVecBuf>(
        &mut self,
        buf_vec: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        let stream = unsafe { &*self.0.get() };
        stream.send_vectored_with_flags(buf_vec, flags).await
    }
}

impl AsReadFd for UnixOwnedReadHalf {
    #[inline]
    fn as_reader_fd(&mut self) -> &SharedFdWrapper {
        let raw_stream = unsafe { &mut *self.0.get() };
        raw_stream.as_reader_fd()
    }
}

impl AsWriteFd for UnixOwnedWriteHalf {
    #[inline]
    fn as_writer_fd(&mut self) -> &SharedFdWrapper {
        let raw_stream = unsafe { &mut *self.0.get() };
        raw_stream.as_writer_fd()
    }
}
