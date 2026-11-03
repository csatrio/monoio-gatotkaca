use std::ops::{Deref, DerefMut};

use libc::msghdr;

/// An `io_uring` compatible msg buffer.
///
/// # Safety
/// See the safety note of the methods.
#[allow(clippy::unnecessary_safety_doc)]
pub unsafe trait MsgBuf: Unpin + 'static {
    /// Returns a raw pointer to msghdr struct.
    ///
    /// # Safety
    /// The implementation must ensure that, while the runtime owns the value,
    /// the pointer returned by `stable_mut_ptr` **does not** change.
    /// Also, the value pointed must be a valid msghdr struct.
    fn read_msghdr_ptr(&self) -> *const msghdr;
}

/// An `io_uring` compatible msg buffer.
///
/// # Safety
/// See the safety note of the methods.
#[allow(clippy::unnecessary_safety_doc)]
pub unsafe trait MsgBufMut: Unpin + 'static {
    /// Returns a raw pointer to msghdr struct.
    ///
    /// # Safety
    /// The implementation must ensure that, while the runtime owns the value,
    /// the pointer returned by `stable_mut_ptr` **does not** change.
    /// Also, the value pointed must be a valid msghdr struct.
    fn write_msghdr_ptr(&mut self) -> *mut msghdr;
}

#[allow(missing_docs)]
pub struct MsgMeta {
    pub(crate) data: msghdr,
}

unsafe impl MsgBuf for MsgMeta {
    fn read_msghdr_ptr(&self) -> *const msghdr {
        &self.data
    }
}

unsafe impl MsgBufMut for MsgMeta {
    fn write_msghdr_ptr(&mut self) -> *mut msghdr {
        &mut self.data
    }
}

impl From<msghdr> for MsgMeta {
    fn from(data: msghdr) -> Self {
        Self { data }
    }
}

impl Deref for MsgMeta {
    type Target = msghdr;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for MsgMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
