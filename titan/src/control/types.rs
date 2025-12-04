//! Protocol types for client-driver control communication.
//!
//! Defines the message types and connection primitives used by [`Client`](super::client::Client)
//! and [`Driver`](super::driver::Driver) to communicate over SPSC queues.

use crate::SharedMemorySafe;
use crate::ipc::shmem::{ShmError, ShmPath};
use std::fmt;

/// Capacity of per-client message queues (tx and rx).
pub const CLIENT_QUEUE_CAPACITY: usize = 1024;

/// Capacity of the driver's inbox for incoming connection requests.
pub const DRIVER_INBOX_CAPACITY: usize = 256;

/// Returns the well-known path for the driver's connection inbox.
///
/// # Panics
///
/// Never panics—the static path is compile-time validated.
#[must_use]
pub fn driver_inbox_path() -> ShmPath {
    ShmPath::new("/titan-driver-inbox").expect("static path is valid")
}

/// Generates unique shared memory paths for a client's tx/rx channels.
///
/// Returns `(tx_path, rx_path)` where tx is client→driver and rx is driver→client.
///
/// # Panics
///
/// Never panics—generated paths are always valid (start with `/`, no extra `/`,
/// well under 255 bytes).
#[must_use]
pub fn channel_paths(id: &ClientId) -> (ShmPath, ShmPath) {
    let tx = format!("/titan-{}-{}-tx", id.pid, id.nonce);
    let rx = format!("/titan-{}-{}-rx", id.pid, id.nonce);
    (
        ShmPath::new(tx).expect("generated path is valid"),
        ShmPath::new(rx).expect("generated path is valid"),
    )
}

/// Unique identifier for a client connection.
///
/// Combines the process ID with a random nonce to ensure uniqueness even if
/// a process reconnects or PIDs are reused.
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct ClientId {
    pid: u32,
    nonce: u32,
}

impl ClientId {
    /// Generates a new unique client ID for the current process.
    #[must_use]
    pub fn generate() -> Self {
        Self {
            pid: std::process::id(),
            nonce: rand::random(),
        }
    }
}

/// Initial handshake message sent by client on connection.
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ClientHello {
    /// The client's unique identifier.
    pub id: ClientId,
}

/// Commands sent from client to driver during an active session.
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ClientCommand {
    /// Keep-alive signal to prevent timeout.
    Heartbeat,
    /// Graceful disconnect request.
    Disconnect,
}

/// Messages sent from client to driver.
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ClientMessage {
    /// Initial handshake (first message on new connection).
    Hello(ClientHello),
    /// Session command (after handshake).
    Command(ClientCommand),
}

impl TryFrom<ClientMessage> for ClientHello {
    type Error = ();

    fn try_from(msg: ClientMessage) -> Result<Self, Self::Error> {
        match msg {
            ClientMessage::Hello(hello) => Ok(hello),
            ClientMessage::Command(_) => Err(()),
        }
    }
}

impl TryFrom<ClientMessage> for ClientCommand {
    type Error = ();

    fn try_from(msg: ClientMessage) -> Result<Self, Self::Error> {
        match msg {
            ClientMessage::Command(cmd) => Ok(cmd),
            ClientMessage::Hello(_) => Err(()),
        }
    }
}

/// Messages sent from driver to client.
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum DriverMessage {
    /// Handshake acknowledgment (connection established).
    Welcome,
    /// Keep-alive response.
    Heartbeat,
    /// Driver is shutting down, client should disconnect.
    Shutdown,
}

/// Errors that can occur during connection establishment or communication.
#[derive(Debug)]
pub enum ConnectionError {
    /// Shared memory operation failed.
    Shm(ShmError),
    /// Timed out waiting for response.
    Timeout,
    /// Received unexpected message type.
    ProtocolViolation,
    /// Message queue is full.
    QueueFull,
    /// Client ID in handshake doesn't match expected.
    IdMismatch,
}

impl From<ShmError> for ConnectionError {
    fn from(e: ShmError) -> Self {
        Self::Shm(e)
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shm(e) => write!(f, "shared memory error: {e}"),
            Self::Timeout => write!(f, "timed out waiting for response"),
            Self::ProtocolViolation => write!(f, "protocol violation"),
            Self::QueueFull => write!(f, "queue full"),
            Self::IdMismatch => write!(f, "client ID mismatch"),
        }
    }
}

/// A bidirectional connection between client and driver.
pub struct Connection<Tx, Rx> {
    /// The client's unique identifier.
    pub id: ClientId,
    /// Transmit channel.
    pub tx: Tx,
    /// Receive channel.
    pub rx: Rx,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_paths() {
        let id = ClientId {
            pid: 12345,
            nonce: 43981, // 0xABCD
        };
        let (tx, rx) = channel_paths(&id);
        assert_eq!(tx.as_ref(), "/titan-12345-43981-tx");
        assert_eq!(rx.as_ref(), "/titan-12345-43981-rx");
    }

    #[test]
    fn test_client_id_uniqueness() {
        let t1 = ClientId::generate();
        let t2 = ClientId::generate();
        assert_ne!(t1, t2);
    }
}
