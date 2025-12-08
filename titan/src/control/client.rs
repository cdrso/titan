//! Client-side control connection to a driver.

use crate::control::handshake::client_handshake;
use crate::control::types::{
    CONTROL_QUEUE_CAPACITY, ClientCommand, ClientMessage, ConnectionError, ControlConnection,
    DATA_QUEUE_CAPACITY, DataChannelRequest, DataDirection, DriverMessage, data_channel_path,
};
use crate::data::Frame;
use crate::ipc::shmem::{Creator, ShmPath};
use crate::ipc::spsc::{Consumer, Producer, Timeout};
use minstant::Instant;

type ClientControlConnection = ControlConnection<
    Producer<ClientMessage, CONTROL_QUEUE_CAPACITY, Creator>,
    Consumer<DriverMessage, CONTROL_QUEUE_CAPACITY, Creator>,
>;

/// A connected client that can send commands to and receive messages from a driver.
///
/// Created via [`Client::connect()`]. The client owns its tx/rx channels and
/// unlinks them on drop ([`Creator`] mode).
pub struct Client {
    control_conn: ClientControlConnection,
}

impl Client {
    /// Connects to a driver and completes the handshake.
    pub fn connect(driver_inbox_path: ShmPath, timeout: Timeout) -> Result<Self, ConnectionError> {
        let control_conn = client_handshake(driver_inbox_path, timeout)?;
        Ok(Self {
            control_conn,
        })
    }

    /// Sends a command to the driver (non-blocking).
    ///
    /// # Errors
    ///
    /// Returns `Err(msg)` if the queue is full.
    pub fn send(&self, cmd: ClientCommand) -> Result<(), ClientMessage> {
        self.control_conn.tx.push(ClientMessage::Command(cmd))
    }

    /// Sends a command to the driver, blocking until space is available.
    ///
    /// # Errors
    ///
    /// Returns `Err(msg)` on timeout.
    pub fn send_blocking(&self, cmd: ClientCommand, timeout: Timeout) -> Result<(), ClientMessage> {
        self.control_conn
            .tx
            .push_blocking(ClientMessage::Command(cmd), timeout)
    }

    /// Receives a message from the driver (non-blocking).
    #[must_use]
    pub fn recv(&self) -> Option<DriverMessage> {
        self.control_conn.rx.pop()
    }

    /// Receives a message from the driver, blocking until available.
    #[must_use]
    pub fn recv_blocking(&self, timeout: Timeout) -> Option<DriverMessage> {
        self.control_conn.rx.pop_blocking(timeout)
    }

    /// Sends a disconnect command and drops the connection.
    ///
    /// The disconnect command is best-effort (ignored if queue is full).
    pub fn disconnect(self) {
        let _ = self
            .control_conn
            .tx
            .push(ClientMessage::Command(ClientCommand::Disconnect));
    }

    /// Opens a data TX channel (client → driver) and returns the producer end.
    ///
    /// Creates the queue, notifies the driver over the control channel, and
    /// waits for a `DataChannelReady` acknowledgment.
    pub fn open_data_tx(
        &self,
        channel: u32,
        timeout: Timeout,
    ) -> Result<Producer<Frame, DATA_QUEUE_CAPACITY, Creator>, ConnectionError> {
        let open = DataChannelRequest {
            channel,
            dir: DataDirection::Tx,
        };
        let path = data_channel_path(&self.control_conn.id, channel, DataDirection::Tx);
        let queue = Producer::<Frame, DATA_QUEUE_CAPACITY, Creator>::create(path)?;

        self.control_conn
            .tx
            .push(ClientMessage::Command(ClientCommand::OpenDataChannel(open)))
            .map_err(|_| ConnectionError::QueueFull)?;

        wait_for_ready(&self.control_conn, open, timeout)?;
        Ok(queue)
    }

    /// Opens a data RX channel (driver → client) and returns the consumer end.
    ///
    /// Requests the channel over control, waits for `DataChannelReady`, then opens the
    /// queue created by the driver.
    pub fn open_data_rx(
        &self,
        channel: u32,
        timeout: Timeout,
    ) -> Result<Consumer<Frame, DATA_QUEUE_CAPACITY, Creator>, ConnectionError> {
        let open = DataChannelRequest {
            channel,
            dir: DataDirection::Rx,
        };

        let queue_path = data_channel_path(&self.control_conn.id, channel, DataDirection::Rx);
        let queue = Consumer::<Frame, DATA_QUEUE_CAPACITY, Creator>::create(queue_path)?;

        self.control_conn
            .tx
            .push(ClientMessage::Command(ClientCommand::OpenDataChannel(open)))
            .map_err(|_| ConnectionError::QueueFull)?;

        wait_for_ready(&self.control_conn, open, timeout)?;

        Ok(queue)
    }

    /// Requests closure of a data channel (either direction).
    pub fn close_data(&self, channel: u32, dir: DataDirection) -> Result<(), ConnectionError> {
        let req = DataChannelRequest { channel, dir };
        self.control_conn
            .tx
            .push(ClientMessage::Command(ClientCommand::CloseDataChannel(
                req,
            )))
            .map_err(|_| ConnectionError::QueueFull)
    }
}

fn wait_for_ready(
    conn: &ControlConnection<
        Producer<ClientMessage, CONTROL_QUEUE_CAPACITY, Creator>,
        Consumer<DriverMessage, CONTROL_QUEUE_CAPACITY, Creator>,
    >,
    target: DataChannelRequest,
    timeout: Timeout,
) -> Result<(), ConnectionError> {
    let deadline = match timeout {
        Timeout::Infinite => None,
        Timeout::Duration(d) => Some(Instant::now() + d),
    };

    loop {
        let remaining = match deadline {
            Some(dl) => {
                if let Some(remaining) = dl.checked_duration_since(Instant::now()) {
                    Timeout::Duration(remaining)
                } else {
                    return Err(ConnectionError::Timeout);
                }
            }
            None => Timeout::Infinite,
        };

        match conn.rx.pop_blocking(remaining) {
            Some(DriverMessage::DataChannelReady(open)) if open == target => return Ok(()),
            Some(DriverMessage::DataChannelError(open)) if open == target => {
                return Err(ConnectionError::ProtocolViolation);
            }
            Some(DriverMessage::DataChannelClosed(open)) if open == target => {
                return Err(ConnectionError::ProtocolViolation);
            }
            Some(_) => continue,
            None => return Err(ConnectionError::Timeout),
        }
    }
}
