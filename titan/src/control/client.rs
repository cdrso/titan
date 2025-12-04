//! Client-side control connection to a driver.

use crate::control::types::{
    CLIENT_QUEUE_CAPACITY, ClientCommand, ClientHello, ClientId, ClientMessage, Connection,
    ConnectionError, DRIVER_INBOX_CAPACITY, DriverMessage, channel_paths,
};
use crate::ipc::shmem::{Creator, Opener, ShmPath};
use crate::ipc::spsc::{Consumer, Producer, Timeout};

type ClientConnection = Connection<
    Producer<ClientMessage, CLIENT_QUEUE_CAPACITY, Creator>,
    Consumer<DriverMessage, CLIENT_QUEUE_CAPACITY, Creator>,
>;

/// A connected client that can send commands to and receive messages from a driver.
///
/// Created via [`Client::connect()`]. The client owns its tx/rx channels and
/// unlinks them on drop ([`Creator`] mode).
pub struct Client {
    conn: ClientConnection,
}

impl Client {
    /// Connects to a driver and completes the handshake.
    ///
    /// Creates tx/rx channels, sends a Hello to the driver inbox, and waits
    /// for a Welcome response.
    ///
    /// # Errors
    ///
    /// - [`ConnectionError::Shm`] - Failed to create channels or open driver inbox
    /// - [`ConnectionError::QueueFull`] - Driver inbox or tx queue is full
    /// - [`ConnectionError::Timeout`] - No Welcome received within timeout
    /// - [`ConnectionError::ProtocolViolation`] - Received unexpected message
    pub fn connect(driver_inbox_path: ShmPath, timeout: Timeout) -> Result<Self, ConnectionError> {
        let id = ClientId::generate();
        let (tx_path, rx_path) = channel_paths(&id);

        let tx = Producer::create(tx_path)?;
        let rx = Consumer::create(rx_path)?;

        tx.push(ClientMessage::Hello(ClientHello { id }))
            .map_err(|_| ConnectionError::QueueFull)?;

        let driver_inbox =
            Producer::<ClientId, DRIVER_INBOX_CAPACITY, Opener>::open(driver_inbox_path)?;

        driver_inbox
            .push(id)
            .map_err(|_| ConnectionError::QueueFull)?;

        match rx.pop_blocking(timeout) {
            Some(DriverMessage::Welcome) => Ok(Self {
                conn: Connection { id, tx, rx },
            }),
            Some(_) => Err(ConnectionError::ProtocolViolation),
            None => Err(ConnectionError::Timeout),
        }
    }

    /// Sends a command to the driver (non-blocking).
    ///
    /// # Errors
    ///
    /// Returns `Err(msg)` if the queue is full.
    pub fn send(&self, cmd: ClientCommand) -> Result<(), ClientMessage> {
        self.conn.tx.push(ClientMessage::Command(cmd))
    }

    /// Sends a command to the driver, blocking until space is available.
    ///
    /// # Errors
    ///
    /// Returns `Err(msg)` on timeout.
    pub fn send_blocking(&self, cmd: ClientCommand, timeout: Timeout) -> Result<(), ClientMessage> {
        self.conn
            .tx
            .push_blocking(ClientMessage::Command(cmd), timeout)
    }

    /// Receives a message from the driver (non-blocking).
    #[must_use]
    pub fn recv(&self) -> Option<DriverMessage> {
        self.conn.rx.pop()
    }

    /// Receives a message from the driver, blocking until available.
    #[must_use]
    pub fn recv_blocking(&self, timeout: Timeout) -> Option<DriverMessage> {
        self.conn.rx.pop_blocking(timeout)
    }

    /// Sends a disconnect command and drops the connection.
    ///
    /// The disconnect command is best-effort (ignored if queue is full).
    pub fn disconnect(self) {
        let _ = self
            .conn
            .tx
            .push(ClientMessage::Command(ClientCommand::Disconnect));
    }
}
