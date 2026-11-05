use std::{io, net::SocketAddr};

use super::TcpStream;
use crate::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut},
    io::{
        as_fd::{AsReadFd, AsWriteFd, SharedFdWrapper},
        futures::{ReadvFuture, RecvFuture, SendFuture, WritevFuture},
        OwnedReadHalf, OwnedWriteHalf,
    },
    net::MsgFlags,
    BufResult,
};

/// OwnedReadHalf.
pub type TcpOwnedReadHalf = OwnedReadHalf<TcpStream>;
/// OwnedWriteHalf
pub type TcpOwnedWriteHalf = OwnedWriteHalf<TcpStream>;

impl TcpOwnedReadHalf {
    /// Returns the remote address that this stream is connected to.
    #[inline]
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        unsafe { &*self.0.get() }.peer_addr()
    }

    /// Returns the local address that this stream is bound to.
    #[inline]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        unsafe { &*self.0.get() }.local_addr()
    }

    /// See [`TcpStream::recv_with_flags`].
    pub async fn recv_with_flags<T: IoBufMut>(
        &mut self,
        buf: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        // Safety: halves of one stream share a thread; the op only clones the fd.
        let stream = unsafe { &*self.0.get() };
        stream.recv_with_flags(buf, flags).await
    }

    /// See [`TcpStream::recv_poll_first`].
    pub async fn recv_poll_first<T: IoBufMut>(
        &mut self,
        buf: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        let stream = unsafe { &*self.0.get() };
        stream.recv_poll_first(buf, flags).await
    }

    /// See [`TcpStream::recv_op`]. The future does not borrow the half.
    #[inline]
    pub fn recv_op<T: IoBufMut>(&self, buf: T) -> RecvFuture<T> {
        // Safety: halves of one stream share a thread; the op only clones the fd.
        unsafe { &*self.0.get() }.recv_op(buf)
    }

    /// See [`TcpStream::recv_with_flags_op`].
    #[inline]
    pub fn recv_with_flags_op<T: IoBufMut>(&self, buf: T, flags: MsgFlags) -> RecvFuture<T> {
        unsafe { &*self.0.get() }.recv_with_flags_op(buf, flags)
    }

    /// See [`TcpStream::recv_poll_first_op`].
    #[inline]
    pub fn recv_poll_first_op<T: IoBufMut>(&self, buf: T, flags: MsgFlags) -> RecvFuture<T> {
        unsafe { &*self.0.get() }.recv_poll_first_op(buf, flags)
    }

    /// See [`TcpStream::readv_op`].
    #[inline]
    pub fn readv_op<T: IoVecBufMut>(&self, buf_vec: T) -> ReadvFuture<T> {
        unsafe { &*self.0.get() }.readv_op(buf_vec)
    }
}

impl AsReadFd for TcpOwnedReadHalf {
    #[inline]
    fn as_reader_fd(&mut self) -> &SharedFdWrapper {
        let raw_stream = unsafe { &mut *self.0.get() };
        raw_stream.as_reader_fd()
    }
}

impl TcpOwnedWriteHalf {
    /// Returns the remote address that this stream is connected to.
    #[inline]
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        unsafe { &*self.0.get() }.peer_addr()
    }

    /// Returns the local address that this stream is bound to.
    #[inline]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        unsafe { &*self.0.get() }.local_addr()
    }

    /// See [`TcpStream::send_with_flags`].
    pub async fn send_with_flags<T: IoBuf>(
        &mut self,
        buf: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        // Safety: halves of one stream share a thread; the op only clones the fd.
        let stream = unsafe { &*self.0.get() };
        stream.send_with_flags(buf, flags).await
    }

    /// See [`TcpStream::send_vectored`].
    pub async fn send_vectored<T: IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        let stream = unsafe { &*self.0.get() };
        stream.send_vectored(buf_vec).await
    }

    /// See [`TcpStream::send_vectored_with_flags`].
    pub async fn send_vectored_with_flags<T: IoVecBuf>(
        &mut self,
        buf_vec: T,
        flags: MsgFlags,
    ) -> BufResult<usize, T> {
        let stream = unsafe { &*self.0.get() };
        stream.send_vectored_with_flags(buf_vec, flags).await
    }

    /// See [`TcpStream::send_op`]. The future does not borrow the half.
    #[inline]
    pub fn send_op<T: IoBuf>(&self, buf: T) -> SendFuture<T> {
        // Safety: halves of one stream share a thread; the op only clones the fd.
        unsafe { &*self.0.get() }.send_op(buf)
    }

    /// See [`TcpStream::send_with_flags_op`].
    #[inline]
    pub fn send_with_flags_op<T: IoBuf>(&self, buf: T, flags: MsgFlags) -> SendFuture<T> {
        unsafe { &*self.0.get() }.send_with_flags_op(buf, flags)
    }

    /// See [`TcpStream::writev_op`].
    #[inline]
    pub fn writev_op<T: IoVecBuf>(&self, buf_vec: T) -> WritevFuture<T> {
        unsafe { &*self.0.get() }.writev_op(buf_vec)
    }
}

impl AsWriteFd for TcpOwnedWriteHalf {
    #[inline]
    fn as_writer_fd(&mut self) -> &SharedFdWrapper {
        let raw_stream = unsafe { &mut *self.0.get() };
        raw_stream.as_writer_fd()
    }
}
