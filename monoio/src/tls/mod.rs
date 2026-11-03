//! TLS support.
//!
//! - `tls-rustls`: [`rustls`] backend.
//! - `tls-native`: [`native`] backend (platform TLS library).

mod io_wrapper;

#[cfg(feature = "tls-native")]
pub mod native;
#[cfg(feature = "tls-rustls")]
pub mod rustls;
