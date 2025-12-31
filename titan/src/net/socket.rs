//! UDP socket wrapper for mio-based I/O.
//!
//! Provides a thin wrapper around [`mio::net::UdpSocket`] with ergonomic
//! send/recv APIs and integration with mio's polling infrastructure.

use std::io::{self, ErrorKind};
use std::os::fd::{AsFd, BorrowedFd};

use mio::event::Source;
use mio::net::UdpSocket as MioUdpSocket;
use mio::{Interest, Registry, Token};

use super::Endpoint;

/// A non-blocking UDP socket.
///
/// Wraps a mio UDP socket and provides methods for sending and receiving
/// datagrams. The socket is non-blocking; use with mio's [`Poll`] for
/// readiness notification.
///
/// [`Poll`]: mio::Poll
pub struct UdpSocket {
    inner: MioUdpSocket,
}

impl UdpSocket {
    /// Creates a new UDP socket bound to the given endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be bound (e.g., address in use).
    pub fn bind(endpoint: Endpoint) -> io::Result<Self> {
        let inner = MioUdpSocket::bind(endpoint.into())?;
        Ok(Self { inner })
    }

    /// Returns the local address this socket is bound to.
    ///
    /// # Errors
    ///
    /// Returns an error if the local address cannot be retrieved.
    pub fn local_addr(&self) -> io::Result<Endpoint> {
        self.inner.local_addr().map(Endpoint::from)
    }

    /// Sends a datagram to the specified endpoint.
    ///
    /// Returns the number of bytes sent, or `WouldBlock` if the socket
    /// is not ready for writing.
    ///
    /// # Errors
    ///
    /// Returns an error on I/O failure or if the socket would block.
    pub fn send_to(&self, buf: &[u8], dest: Endpoint) -> io::Result<usize> {
        self.inner.send_to(buf, dest.into())
    }

    /// Receives a datagram from the socket.
    ///
    /// Returns the number of bytes received and the source endpoint,
    /// or `WouldBlock` if no data is available.
    ///
    /// # Errors
    ///
    /// Returns an error on I/O failure or if the socket would block.
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, Endpoint)> {
        self.inner
            .recv_from(buf)
            .map(|(n, addr)| (n, Endpoint::from(addr)))
    }

    /// Attempts to send, returning `Ok(None)` instead of `WouldBlock`.
    ///
    /// Useful in polling loops where `WouldBlock` is expected.
    pub fn try_send_to(&self, buf: &[u8], dest: Endpoint) -> io::Result<Option<usize>> {
        match self.send_to(buf, dest) {
            Ok(n) => Ok(Some(n)),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Attempts to receive, returning `Ok(None)` instead of `WouldBlock`.
    ///
    /// Useful in polling loops where `WouldBlock` is expected.
    pub fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(usize, Endpoint)>> {
        match self.recv_from(buf) {
            Ok((n, ep)) => Ok(Some((n, ep))),
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Sets the socket's send buffer size.
    ///
    /// # Errors
    ///
    /// Returns an error if the option cannot be set.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        // Use rustix for socket options since mio doesn't expose them directly
        let fd = self.inner.as_fd();
        rustix::net::sockopt::set_socket_send_buffer_size(fd, size)?;
        Ok(())
    }

    /// Sets the socket's receive buffer size.
    ///
    /// # Errors
    ///
    /// Returns an error if the option cannot be set.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        let fd = self.inner.as_fd();
        rustix::net::sockopt::set_socket_recv_buffer_size(fd, size)?;
        Ok(())
    }

    /// Gets the socket's send buffer size.
    ///
    /// # Errors
    ///
    /// Returns an error if the option cannot be retrieved.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        let fd = self.inner.as_fd();
        Ok(rustix::net::sockopt::socket_send_buffer_size(fd)?)
    }

    /// Gets the socket's receive buffer size.
    ///
    /// # Errors
    ///
    /// Returns an error if the option cannot be retrieved.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        let fd = self.inner.as_fd();
        Ok(rustix::net::sockopt::socket_recv_buffer_size(fd)?)
    }
}

impl AsFd for UdpSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl Source for UdpSocket {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.inner.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.inner.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.inner.deregister(registry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_bind_and_local_addr() {
        let socket = UdpSocket::bind(Endpoint::localhost(0)).unwrap();
        let addr = socket.local_addr().unwrap();
        assert_eq!(
            addr.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
        assert_ne!(addr.port(), 0); // OS assigned a port
    }

    #[test]
    fn socket_send_recv_loopback() {
        let sender = UdpSocket::bind(Endpoint::localhost(0)).unwrap();
        let receiver = UdpSocket::bind(Endpoint::localhost(0)).unwrap();

        let receiver_addr = receiver.local_addr().unwrap();

        let msg = b"hello";
        let sent = sender.send_to(msg, receiver_addr).unwrap();
        assert_eq!(sent, msg.len());

        let mut buf = [0u8; 64];
        let (received, from) = receiver.recv_from(&mut buf).unwrap();
        assert_eq!(received, msg.len());
        assert_eq!(&buf[..received], msg);
        assert_eq!(from, sender.local_addr().unwrap());
    }

    #[test]
    fn socket_try_recv_empty() {
        let socket = UdpSocket::bind(Endpoint::localhost(0)).unwrap();
        let mut buf = [0u8; 64];
        let result = socket.try_recv_from(&mut buf).unwrap();
        assert!(result.is_none()); // No data, returns None instead of WouldBlock
    }

    #[test]
    fn socket_buffer_sizes() {
        let socket = UdpSocket::bind(Endpoint::localhost(0)).unwrap();

        // Get default sizes (should be non-zero)
        let send_size = socket.send_buffer_size().unwrap();
        let recv_size = socket.recv_buffer_size().unwrap();
        assert!(send_size > 0);
        assert!(recv_size > 0);

        // Try to set larger sizes (kernel may adjust)
        socket.set_send_buffer_size(1024 * 1024).unwrap();
        socket.set_recv_buffer_size(1024 * 1024).unwrap();

        // Verify they changed (kernel doubles the value on Linux)
        let new_send = socket.send_buffer_size().unwrap();
        let new_recv = socket.recv_buffer_size().unwrap();
        assert!(new_send >= send_size);
        assert!(new_recv >= recv_size);
    }
}
