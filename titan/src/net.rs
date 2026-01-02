//! Network transport primitives.
//!
//! Provides UDP socket abstractions for the driver's TX/RX threads.
//! Currently mio-based; endpoint types are designed to be reusable
//! across future kernel-bypass backends (DPDK, `AF_XDP`).

pub mod endpoint;
pub mod socket;

pub use endpoint::Endpoint;
pub use socket::UdpSocket;
