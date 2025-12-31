//! Network endpoint types.
//!
//! These types are intentionally backend-agnostic and will be reused
//! when kernel-bypass transports (DPDK, AF_XDP) are added.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

/// A network endpoint (IP address + port).
///
/// Wrapper around [`SocketAddr`] that provides a stable API across
/// different transport backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Endpoint(SocketAddr);

impl Endpoint {
    /// Creates a new endpoint from an IP address and port.
    #[must_use]
    pub const fn new(addr: IpAddr, port: u16) -> Self {
        Self(SocketAddr::new(addr, port))
    }

    /// Creates a new IPv4 endpoint.
    #[must_use]
    pub const fn new_v4(a: u8, b: u8, c: u8, d: u8, port: u16) -> Self {
        Self(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(a, b, c, d),
            port,
        )))
    }

    /// Creates an endpoint bound to all interfaces (0.0.0.0) on the given port.
    #[must_use]
    pub const fn any(port: u16) -> Self {
        Self::new_v4(0, 0, 0, 0, port)
    }

    /// Creates a localhost endpoint on the given port.
    #[must_use]
    pub const fn localhost(port: u16) -> Self {
        Self::new_v4(127, 0, 0, 1, port)
    }

    /// Returns the IP address.
    #[must_use]
    pub const fn ip(&self) -> IpAddr {
        self.0.ip()
    }

    /// Returns the port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.0.port()
    }

    /// Returns the underlying [`SocketAddr`].
    #[must_use]
    pub const fn as_socket_addr(&self) -> SocketAddr {
        self.0
    }
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

impl From<Endpoint> for SocketAddr {
    fn from(ep: Endpoint) -> Self {
        ep.0
    }
}

impl From<SocketAddrV4> for Endpoint {
    fn from(addr: SocketAddrV4) -> Self {
        Self(SocketAddr::V4(addr))
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<crate::control::types::RemoteEndpoint> for Endpoint {
    fn from(remote: crate::control::types::RemoteEndpoint) -> Self {
        Self::new_v4(
            remote.addr[0],
            remote.addr[1],
            remote.addr[2],
            remote.addr[3],
            remote.port,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_new_v4() {
        let ep = Endpoint::new_v4(192, 168, 1, 100, 8080);
        assert_eq!(ep.ip(), IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));
        assert_eq!(ep.port(), 8080);
    }

    #[test]
    fn endpoint_any() {
        let ep = Endpoint::any(9000);
        assert_eq!(ep.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(ep.port(), 9000);
    }

    #[test]
    fn endpoint_localhost() {
        let ep = Endpoint::localhost(3000);
        assert_eq!(ep.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(ep.port(), 3000);
    }

    #[test]
    fn endpoint_from_socket_addr() {
        let addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let ep = Endpoint::from(addr);
        assert_eq!(ep.as_socket_addr(), addr);
    }

    #[test]
    fn endpoint_display() {
        let ep = Endpoint::new_v4(127, 0, 0, 1, 8080);
        assert_eq!(format!("{ep}"), "127.0.0.1:8080");
    }
}
