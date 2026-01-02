//! Protocol types for client-driver control communication.

use crate::SharedMemorySafe;
use crate::ipc::shmem::{Creator, Opener, ShmError, ShmMode, ShmPath};
use crate::ipc::spsc::{Consumer, Producer};
use std::fmt;
use std::time::Duration;
use thiserror::Error;
use type_hash::TypeHash;

/// Capacity of per-client control message queues (tx and rx).
pub const CONTROL_QUEUE_CAPACITY: usize = 1024;

/// Capacity of the driver's inbox for incoming connection requests.
pub const DRIVER_INBOX_CAPACITY: usize = 256;

/// Capacity of per-client data queues (tx or rx).
pub const DATA_QUEUE_CAPACITY: usize = 1024;

/// Timeout for receiving Hello/Welcome during handshake.
pub const HELLO_TIMEOUT: Duration = Duration::from_millis(500);

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
pub fn control_channel_paths(id: &ClientId) -> (ShmPath, ShmPath) {
    let tx = format!("/titan-{}-{}-tx", id.pid, id.nonce);
    let rx = format!("/titan-{}-{}-rx", id.pid, id.nonce);
    (
        ShmPath::new(tx).expect("generated path is valid"),
        ShmPath::new(rx).expect("generated path is valid"),
    )
}

/// Generates shared memory path for a data channel.
///
/// # Panics
///
/// Never panics—generated paths are always valid.
#[must_use]
pub fn data_tx_path(id: &ClientId, channel: ChannelId) -> ShmPath {
    let path = format!("/titan-data-{}-{}-{}-tx", id.pid, id.nonce, channel.0);
    ShmPath::new(path).expect("generated path is valid")
}

/// Generates shared memory path for a client's RX data channel.
///
/// # Panics
///
/// Never panics—generated paths are always valid.
#[must_use]
pub fn data_rx_path(id: &ClientId, channel: ChannelId) -> ShmPath {
    let path = format!("/titan-data-{}-{}-{}-rx", id.pid, id.nonce, channel.0);
    ShmPath::new(path).expect("generated path is valid")
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

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{:04x}", self.pid, self.nonce)
    }
}

/// Initial handshake message sent by client on connection.
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ClientHello {
    /// The client's unique identifier.
    pub id: ClientId,
}

/// Application-defined channel identifier.
// TODO we need to figure this out but this works for now
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ChannelId(u32);

impl ChannelId {
    /// Creates a new channel identifier.
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }
}

impl From<u32> for ChannelId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<ChannelId> for u32 {
    fn from(id: ChannelId) -> Self {
        id.0
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a data message type
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct TypeId(u64);

impl TypeId {
    /// Returns the structural type hash for `T`.
    #[must_use]
    pub fn of<T: TypeHash>() -> Self {
        Self(T::type_hash())
    }
}

impl From<TypeId> for u64 {
    fn from(id: TypeId) -> Self {
        id.0
    }
}

impl From<u64> for TypeId {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl fmt::Display for TypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Commands sent from client to driver during an active session.
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ClientCommand {
    /// Keep-alive signal to prevent timeout.
    Heartbeat,
    /// Graceful disconnect request.
    Disconnect,
    /// Request a new transmit channel (client -> driver).
    OpenTx { channel: ChannelId, type_id: TypeId },
    /// Request a new receive channel (driver -> client).
    OpenRx { channel: ChannelId, type_id: TypeId },
    /// Subscribe to a remote publisher's channel.
    ///
    /// The driver will send a SETUP frame to the remote endpoint and
    /// await SETUP_ACK/NAK. On success, DATA frames from the remote
    /// publisher will be routed to the client's RX queue.
    SubscribeRemote {
        /// Local channel ID for this subscription.
        channel: ChannelId,
        /// Expected message type (must match publisher).
        type_id: TypeId,
        /// Remote driver's endpoint.
        remote: RemoteEndpoint,
        /// Remote channel ID on the publisher.
        remote_channel: ChannelId,
    },
    /// Close an existing channel.
    CloseChannel(ChannelId),
}

/// Remote endpoint representation for IPC (must be SharedMemorySafe).
///
/// We store the raw bytes of a socket address since SocketAddr isn't repr(C).
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct RemoteEndpoint {
    /// IPv4 address bytes (a.b.c.d).
    pub addr: [u8; 4],
    /// Port number.
    pub port: u16,
}

impl RemoteEndpoint {
    /// Creates a new remote endpoint from IPv4 octets and port.
    #[must_use]
    pub const fn new(a: u8, b: u8, c: u8, d: u8, port: u16) -> Self {
        Self {
            addr: [a, b, c, d],
            port,
        }
    }

    /// Creates a localhost endpoint on the given port.
    #[must_use]
    pub const fn localhost(port: u16) -> Self {
        Self::new(127, 0, 0, 1, port)
    }
}

impl From<RemoteEndpoint> for crate::net::Endpoint {
    fn from(remote: RemoteEndpoint) -> Self {
        Self::new_v4(
            remote.addr[0],
            remote.addr[1],
            remote.addr[2],
            remote.addr[3],
            remote.port,
        )
    }
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

/// Error when parsing a `ClientMessage` into a specific variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MessageParseError {
    /// Expected a `Hello` message, got `Command`.
    #[error("expected Hello, got Command")]
    ExpectedHello,
    /// Expected a `Command` message, got `Hello`.
    #[error("expected Command, got Hello")]
    ExpectedCommand,
}

impl TryFrom<ClientMessage> for ClientHello {
    type Error = MessageParseError;

    fn try_from(msg: ClientMessage) -> Result<Self, Self::Error> {
        match msg {
            ClientMessage::Hello(hello) => Ok(hello),
            ClientMessage::Command(_) => Err(MessageParseError::ExpectedHello),
        }
    }
}

impl TryFrom<ClientMessage> for ClientCommand {
    type Error = MessageParseError;

    fn try_from(msg: ClientMessage) -> Result<Self, Self::Error> {
        match msg {
            ClientMessage::Command(cmd) => Ok(cmd),
            ClientMessage::Hello(_) => Err(MessageParseError::ExpectedCommand),
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
    /// Data channel is ready to use.
    ChannelReady(ChannelId),
    /// Data channel failed to open.
    ChannelError(ChannelId),
    /// Data channel closed/acknowledged.
    ChannelClosed(ChannelId),
    /// Remote subscription established successfully.
    SubscriptionReady(ChannelId),
    /// Remote subscription failed.
    SubscriptionFailed {
        /// The local channel that failed.
        channel: ChannelId,
        /// Reason for failure.
        reason: SubscriptionFailure,
    },
}

/// Reasons a remote subscription can fail.
#[derive(SharedMemorySafe, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubscriptionFailure {
    /// Remote publisher's type doesn't match.
    TypeMismatch = 1,
    /// Remote channel doesn't exist.
    ChannelNotFound = 2,
    /// Remote driver is at capacity.
    CapacityExceeded = 3,
    /// Network or protocol error.
    NetworkError = 4,
    /// Handshake timed out.
    Timeout = 5,
}

/// Errors that can occur during connection establishment or communication.
#[derive(Debug, Error)]
pub enum ConnectionError {
    /// Shared memory operation failed.
    #[error("shared memory error: {0}")]
    Shm(ShmError),
    /// Timed out waiting for response.
    #[error("timed out waiting for response")]
    Timeout,
    /// Received unexpected message type.
    #[error("protocol violation")]
    ProtocolViolation,
    /// Message queue is full.
    #[error("queue full")]
    QueueFull,
    /// Client ID in handshake doesn't match expected.
    #[error("client ID mismatch")]
    IdMismatch,
}

impl From<ShmError> for ConnectionError {
    fn from(e: ShmError) -> Self {
        Self::Shm(e)
    }
}

/// A bidirectional control connection between client and driver.
pub struct ControlConnection<Role: ConnectionRole> {
    /// The client's unique identifier.
    pub id: ClientId,
    /// Transmit channel.
    pub tx: Producer<Role::TxMessage, CONTROL_QUEUE_CAPACITY, Role::Mode>,
    /// Receive channel.
    pub rx: Consumer<Role::RxMessage, CONTROL_QUEUE_CAPACITY, Role::Mode>,
}

/// Marker for a client-side control connection.
pub struct ClientRole;
/// Marker for a driver-side control connection.
pub struct DriverRole;

/// Strategy trait mapping a role to its message types and shared memory mode.
pub trait ConnectionRole {
    type TxMessage: SharedMemorySafe;
    type RxMessage: SharedMemorySafe;
    type Mode: ShmMode;
}

impl ConnectionRole for ClientRole {
    type TxMessage = ClientMessage;
    type RxMessage = DriverMessage;
    type Mode = Creator;
}

impl ConnectionRole for DriverRole {
    type TxMessage = DriverMessage;
    type RxMessage = ClientMessage;
    type Mode = Opener;
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
        let (tx, rx) = control_channel_paths(&id);
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
