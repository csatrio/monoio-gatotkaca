//! TCP Fast Open

mod linux;
pub(crate) use linux::{set_tcp_fastopen, try_set_tcp_fastopen_connect};
