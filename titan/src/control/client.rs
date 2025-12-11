//! Client-side control connection to a driver.

use crate::control::handshake::client_connect;
use crate::control::types::{
    ChannelId, ClientCommand, ClientMessage, ClientRole, ConnectionError, ControlConnection,
    DATA_QUEUE_CAPACITY, DriverMessage, TypeId, data_rx_path, data_tx_path,
};
use crate::data::{DEFAULT_FRAME_CAP, Frame, TypedConsumer, TypedProducer, Wire};
use crate::ipc::shmem::{Creator, ShmPath};
use crate::ipc::spsc::{Consumer, Producer, Timeout};
use minstant::Instant;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Duration;

type ClientControlConnection = ControlConnection<ClientRole>;

/// A connected client that can send commands to and receive messages from a driver.
///
/// Created via [`Client::connect()`]. The client owns its tx/rx channels and
/// unlinks them on drop ([`Creator`] mode).
pub struct Client {
    control_conn: ClientControlConnection,
    pending: RefCell<VecDeque<DriverMessage>>,
}

impl Client {
    /// Connects to a driver and completes the handshake.
    ///
    /// # Errors
    /// - [`ConnectionError::Timeout`] if the driver does not respond in time
    /// - [`ConnectionError::ProtocolViolation`] on unexpected handshake message
    /// - [`ConnectionError::QueueFull`] if the control queue is full
    /// - [`ConnectionError::Shm`] for shared memory creation/open failures
    pub fn connect(driver_inbox_path: ShmPath, timeout: Timeout) -> Result<Self, ConnectionError> {
        let control_conn = client_connect(driver_inbox_path, timeout)?;
        Ok(Self {
            control_conn,
            pending: RefCell::new(VecDeque::new()),
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
        if let Some(msg) = self.pending.borrow_mut().pop_front() {
            return Some(msg);
        }
        self.control_conn.rx.pop()
    }

    /// Receives a message from the driver, blocking until available.
    #[must_use]
    pub fn recv_blocking(&self, timeout: Timeout) -> Option<DriverMessage> {
        if let Some(msg) = self.pending.borrow_mut().pop_front() {
            return Some(msg);
        }
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

    /// Opens a data TX channel and returns the producer end.
    ///
    /// Creates the queue, notifies the driver over the control channel, and
    /// waits for a `DataChannelReady` acknowledgment.
    ///
    /// # Errors
    ///
    /// - [`ConnectionError::Shm`] if the data queue cannot be created
    /// - [`ConnectionError::QueueFull`] if the control queue is full
    /// - [`ConnectionError::Timeout`] if acknowledgment is not received in time
    /// - [`ConnectionError::ProtocolViolation`] if the driver responds with an error
    pub fn open_data_tx<T: Wire>(
        &self,
        channel: ChannelId,
        timeout: Duration,
    ) -> Result<TypedProducer<T, DEFAULT_FRAME_CAP, DATA_QUEUE_CAPACITY, Creator>, ConnectionError>
    {
        let path = data_tx_path(&self.control_conn.id, channel);
        let queue_producer_raw =
            Producer::<Frame<DEFAULT_FRAME_CAP>, DATA_QUEUE_CAPACITY, Creator>::create(path)?;

        self.control_conn
            .tx
            .push(ClientMessage::Command(ClientCommand::OpenTx {
                channel,
                type_id: TypeId::of::<T>(),
            }))
            .map_err(|_| ConnectionError::QueueFull)?;

        wait_for_ready(self, channel, timeout)?;
        Ok(TypedProducer::new(queue_producer_raw))
    }

    /// Opens a data RX channel and returns the consumer end.
    ///
    /// Creates the queue, notifies the driver over the control channel, and
    /// waits for a `DataChannelReady` acknowledgment.
    ///
    /// # Errors
    ///
    /// - [`ConnectionError::Shm`] if the data queue cannot be created
    /// - [`ConnectionError::QueueFull`] if the control queue is full
    /// - [`ConnectionError::Timeout`] if acknowledgment is not received in time
    /// - [`ConnectionError::ProtocolViolation`] if the driver responds with an error
    pub fn open_data_rx<T: Wire>(
        &self,
        channel: ChannelId,
        timeout: Duration,
    ) -> Result<TypedConsumer<T, DEFAULT_FRAME_CAP, DATA_QUEUE_CAPACITY, Creator>, ConnectionError>
    {
        let queue_path = data_rx_path(&self.control_conn.id, channel);
        let queue_consumer_raw =
            Consumer::<Frame<DEFAULT_FRAME_CAP>, DATA_QUEUE_CAPACITY, Creator>::create(queue_path)?;

        self.control_conn
            .tx
            .push(ClientMessage::Command(ClientCommand::OpenRx {
                channel,
                type_id: TypeId::of::<T>(),
            }))
            .map_err(|_| ConnectionError::QueueFull)?;

        wait_for_ready(self, channel, timeout)?;
        Ok(TypedConsumer::new(queue_consumer_raw))
    }

    /// Requests closure of a data channel (either direction).
    ///
    /// # Errors
    /// - [`ConnectionError::QueueFull`] if control queue is full
    pub fn close_data(&self, channel: ChannelId) -> Result<(), ConnectionError> {
        self.control_conn
            .tx
            .push(ClientMessage::Command(ClientCommand::CloseChannel(channel)))
            .map_err(|_| ConnectionError::QueueFull)
    }
}

fn wait_for_ready(
    client: &Client,
    target: ChannelId,
    timeout: Duration,
) -> Result<(), ConnectionError> {
    let deadline = Instant::now() + timeout;

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(ConnectionError::Timeout)?;

        match client
            .control_conn
            .rx
            .pop_blocking(Timeout::Duration(remaining))
        {
            Some(DriverMessage::ChannelReady(ch)) if ch == target => return Ok(()),
            Some(DriverMessage::ChannelError(ch)) if ch == target => {
                return Err(ConnectionError::ProtocolViolation);
            }
            Some(other) => {
                client.pending.borrow_mut().push_back(other);
            }
            None => return Err(ConnectionError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::Driver;
    use crate::control::types::driver_inbox_path;
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use std::thread;
    use std::time::Duration;
    use type_hash::TypeHash;

    fn with_driver<F, R>(f: F) -> R
    where
        F: FnOnce(ShmPath) -> R,
    {
        let path = driver_inbox_path();
        let _ = rustix::shm::unlink(path.as_ref());
        f(path)
    }

    #[test]
    #[serial]
    fn client_connect_and_disconnect() {
        with_driver(|inbox_path| {
            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                let accepted = driver.accept_pending();
                assert_eq!(accepted, 1);
                driver.connection_count()
            });

            thread::sleep(Duration::from_millis(10)); // Let driver start
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            client.disconnect();

            let count = driver_thread.join().unwrap();
            assert_eq!(count, 1);
        });
    }

    #[test]
    #[serial]
    fn client_send_recv_commands() {
        with_driver(|inbox_path| {
            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                driver.accept_pending();

                // Send heartbeat to client
                let _ = driver.broadcast(DriverMessage::Heartbeat);

                // Process client messages
                thread::sleep(Duration::from_millis(50));
                driver.tick(Duration::from_secs(10));

                driver
            });

            thread::sleep(Duration::from_millis(10));
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            // Send heartbeat to driver
            client.send(ClientCommand::Heartbeat).unwrap();

            // Receive heartbeat from driver
            let msg = client.recv_blocking(Timeout::Duration(Duration::from_millis(500)));
            assert!(matches!(msg, Some(DriverMessage::Heartbeat)));

            client.disconnect();
            let _ = driver_thread.join();
        });
    }

    #[test]
    fn client_connect_timeout_no_driver() {
        // Use a path that doesn't exist
        let nonce: u32 = rand::random();
        let path = ShmPath::new(format!("/test-no-driver-{nonce}")).unwrap();

        let result = Client::connect(path, Timeout::Duration(Duration::from_millis(50)));
        assert!(matches!(result, Err(ConnectionError::Shm(_))));
    }

    #[test]
    #[serial]
    fn multiple_clients_connect() {
        with_driver(|inbox_path| {
            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();
                let mut total = 0;

                // Keep accepting until we have 3 clients
                for _ in 0..20 {
                    total += driver.accept_pending();
                    if total >= 3 {
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
                total
            });

            thread::sleep(Duration::from_millis(10));

            let inbox1 = inbox_path.clone();
            let inbox2 = inbox_path.clone();
            let inbox3 = inbox_path;

            let c1 = thread::spawn(move || {
                Client::connect(inbox1, Timeout::Duration(Duration::from_secs(1)))
            });
            let c2 = thread::spawn(move || {
                Client::connect(inbox2, Timeout::Duration(Duration::from_secs(1)))
            });
            let c3 = thread::spawn(move || {
                Client::connect(inbox3, Timeout::Duration(Duration::from_secs(1)))
            });

            let client1 = c1.join().unwrap().unwrap();
            let client2 = c2.join().unwrap().unwrap();
            let client3 = c3.join().unwrap().unwrap();

            let accepted = driver_thread.join().unwrap();
            assert_eq!(accepted, 3);

            client1.disconnect();
            client2.disconnect();
            client3.disconnect();
        });
    }

    #[test]
    #[serial]
    fn driver_session_timeout() {
        with_driver(|inbox_path| {
            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                driver.accept_pending();

                // Tick with very short timeout
                thread::sleep(Duration::from_millis(100));
                let disconnected = driver.tick(Duration::from_millis(10));

                (driver.connection_count(), disconnected.len())
            });

            thread::sleep(Duration::from_millis(10));
            let _client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            // Don't send any messages - let it timeout
            thread::sleep(Duration::from_millis(200));

            let (count, disconnected) = driver_thread.join().unwrap();
            assert_eq!(count, 0);
            assert_eq!(disconnected, 1);
        });
    }

    #[test]
    #[serial]
    fn data_channel_open_tx() {
        with_driver(|inbox_path| {
            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                driver.accept_pending();

                // Process data channel open request
                for _ in 0..10 {
                    driver.tick(Duration::from_secs(10));
                    thread::sleep(Duration::from_millis(20));
                }
                driver
            });

            thread::sleep(Duration::from_millis(10));
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            #[derive(Serialize, Deserialize, TypeHash, Debug, PartialEq)]
            struct TestData(u32);

            let producer = client
                .open_data_tx::<TestData>(ChannelId::new(1), Duration::from_secs(1))
                .unwrap();

            // Send some data
            producer.send(&TestData(42)).unwrap();

            client.disconnect();
            let _ = driver_thread.join();
        });
    }

    #[test]
    #[serial]
    fn data_channel_open_rx() {
        with_driver(|inbox_path| {
            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                driver.accept_pending();

                for _ in 0..10 {
                    driver.tick(Duration::from_secs(10));
                    thread::sleep(Duration::from_millis(20));
                }
                driver
            });

            thread::sleep(Duration::from_millis(10));
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            #[derive(Serialize, Deserialize, TypeHash, Debug, PartialEq)]
            struct TestData(u32);

            let _consumer = client
                .open_data_rx::<TestData>(ChannelId::new(2), Duration::from_secs(1))
                .unwrap();

            client.disconnect();
            let _ = driver_thread.join();
        });
    }

    #[test]
    #[serial]
    fn data_channel_bidirectional() {
        with_driver(|inbox_path| {
            #[derive(Serialize, Deserialize, TypeHash, Debug, PartialEq, Clone)]
            struct Msg {
                id: u32,
            }

            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                driver.accept_pending();

                // Keep processing until channels are ready
                for _ in 0..20 {
                    driver.tick(Duration::from_secs(10));
                    thread::sleep(Duration::from_millis(20));
                }

                driver
            });

            thread::sleep(Duration::from_millis(10));
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            // Open TX channel (client -> driver)
            let tx = client
                .open_data_tx::<Msg>(ChannelId::new(1), Duration::from_secs(1))
                .unwrap();

            // Open RX channel (driver -> client)
            let _rx = client
                .open_data_rx::<Msg>(ChannelId::new(2), Duration::from_secs(1))
                .unwrap();

            // Send data
            tx.send(&Msg { id: 123 }).unwrap();

            client.disconnect();
            let _ = driver_thread.join();
        });
    }

    #[test]
    #[serial]
    fn close_data_channel() {
        with_driver(|inbox_path| {
            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                driver.accept_pending();

                for _ in 0..15 {
                    driver.tick(Duration::from_secs(10));
                    thread::sleep(Duration::from_millis(20));
                }
                driver
            });

            thread::sleep(Duration::from_millis(10));
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            #[derive(Serialize, Deserialize, TypeHash)]
            struct Data(u8);

            let _tx = client
                .open_data_tx::<Data>(ChannelId::new(5), Duration::from_secs(1))
                .unwrap();

            // Close the channel
            client.close_data(ChannelId::new(5)).unwrap();

            // Should receive close acknowledgment (buffered in pending)
            thread::sleep(Duration::from_millis(100));

            client.disconnect();
            let _ = driver_thread.join();
        });
    }
}
