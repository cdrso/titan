//! Driver-side session management for client control connections.

use crate::control::handshake::driver_accept;
use crate::control::types::{
    ChannelId, ClientCommand, ClientId, ConnectionError, ControlConnection, DATA_QUEUE_CAPACITY,
    DRIVER_INBOX_CAPACITY, DriverMessage, DriverRole, data_rx_path, data_tx_path,
    driver_inbox_path,
};
use crate::data::Frame;
use crate::ipc::mpsc::Consumer as MpscConsumer;
use crate::ipc::shmem::{Creator, Opener};
use crate::ipc::spsc::{Consumer, Producer};
use minstant::Instant;
use std::collections::HashMap;
use std::time::Duration;

type DriverConnection = ControlConnection<DriverRole>;

/// A data channel endpoint on the driver side.
pub enum DataEndpoint {
    /// Data flowing from client to driver (client TX → driver RX).
    Inbound(Consumer<Frame, DATA_QUEUE_CAPACITY, Opener>),
    /// Data flowing from driver to client (driver TX → client RX).
    Outbound(Producer<Frame, DATA_QUEUE_CAPACITY, Opener>),
}

struct DataChannels {
    channels: HashMap<ChannelId, DataEndpoint>,
}

impl DataChannels {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.channels.clear();
    }

    fn close(&mut self, channel: ChannelId) -> Option<DataEndpoint> {
        self.channels.remove(&channel)
    }

    // TODO we need to be more explicit about the error when an existing id is passed
    fn handle_open_tx(
        &mut self,
        conn: &DriverConnection,
        channel: ChannelId,
    ) -> Result<(), ConnectionError> {
        if self.channels.contains_key(&channel) {
            conn.tx
                .push(DriverMessage::ChannelError(channel))
                .map_err(|_| ConnectionError::QueueFull)?;
            return Err(ConnectionError::ProtocolViolation);
        }

        let path = data_tx_path(&conn.id, channel);
        match Consumer::<Frame, DATA_QUEUE_CAPACITY, Opener>::open(path) {
            Ok(rx) => {
                self.channels.insert(channel, DataEndpoint::Inbound(rx));
                conn.tx
                    .push(DriverMessage::ChannelReady(channel))
                    .map_err(|_| ConnectionError::QueueFull)?;
                Ok(())
            }
            Err(err) => {
                conn.tx
                    .push(DriverMessage::ChannelError(channel))
                    .map_err(|_| ConnectionError::QueueFull)?;
                Err(ConnectionError::from(err))
            }
        }
    }

    // TODO we need to be more explicit about the error when an existing id is passed
    fn handle_open_rx(
        &mut self,
        conn: &DriverConnection,
        channel: ChannelId,
    ) -> Result<(), ConnectionError> {
        if self.channels.contains_key(&channel) {
            conn.tx
                .push(DriverMessage::ChannelError(channel))
                .map_err(|_| ConnectionError::QueueFull)?;
            return Err(ConnectionError::ProtocolViolation);
        }

        let path = data_rx_path(&conn.id, channel);
        match Producer::<Frame, DATA_QUEUE_CAPACITY, Opener>::open(path) {
            Ok(tx) => {
                self.channels.insert(channel, DataEndpoint::Outbound(tx));
                conn.tx
                    .push(DriverMessage::ChannelReady(channel))
                    .map_err(|_| ConnectionError::QueueFull)?;
                Ok(())
            }
            Err(err) => {
                conn.tx
                    .push(DriverMessage::ChannelError(channel))
                    .map_err(|_| ConnectionError::QueueFull)?;
                Err(ConnectionError::from(err))
            }
        }
    }

    fn get(&self, channel: ChannelId) -> Option<&DataEndpoint> {
        self.channels.get(&channel)
    }
}

struct Session {
    conn: DriverConnection,
    last_activity: Instant,
    data_channels: DataChannels,
}

impl Session {
    fn new(conn: DriverConnection) -> Self {
        Self {
            conn,
            last_activity: Instant::now(),
            data_channels: DataChannels::new(),
        }
    }

    //TODO do we need to manually drop??
    fn close_data_channel(&mut self, channel: ChannelId) {
        if let Some(endpoint) = self.data_channels.close(channel) {
            match endpoint {
                DataEndpoint::Inbound(rx) => drop(rx),
                DataEndpoint::Outbound(tx) => drop(tx),
            }
        }
    }
}

/// Manages client sessions and processes incoming connections.
///
/// The driver owns an inbox queue where clients announce themselves, then
/// opens their tx/rx channels to complete the handshake.
pub struct Driver {
    inbox: MpscConsumer<ClientId, DRIVER_INBOX_CAPACITY, Creator>,
    sessions: HashMap<ClientId, Session>,
}

impl Driver {
    /// Creates a new driver with an inbox for incoming connections.
    ///
    /// # Errors
    ///
    /// - [`ConnectionError::Shm`] with `EEXIST` if a driver is already running
    /// - [`ConnectionError::Shm`] for other shared memory errors
    pub fn new() -> Result<Self, ConnectionError> {
        let path = driver_inbox_path();
        let inbox = MpscConsumer::create(path)?;

        Ok(Self {
            inbox,
            sessions: HashMap::new(),
        })
    }

    /// Accepts all pending connection requests from the inbox.
    ///
    /// Returns the number of successfully accepted clients.
    pub fn accept_pending(&mut self) -> usize {
        let mut count = 0;

        while let Some(client_id) = self.inbox.pop() {
            if let Ok(conn) = driver_accept(client_id) {
                self.sessions.insert(conn.id, Session::new(conn));
                count += 1;
            }
        }

        count
    }

    /// Processes messages from all sessions and removes disconnected clients.
    ///
    /// Clients are disconnected if they send a [`ClientCommand::Disconnect`]
    /// or if no activity is received within `timeout`.
    ///
    /// Returns the IDs of disconnected clients.
    pub fn tick(&mut self, timeout: Duration) -> Vec<ClientId> {
        // TODO we are creating a vector on every tick... bad but lets get it working first
        // also TODO, we are moving this logic into the runtime module... is this deprecated?
        let mut disconnected = Vec::new();
        let now = Instant::now();

        for session in self.sessions.values_mut() {
            while let Some(msg) = session.conn.rx.pop() {
                if let Ok(cmd) = ClientCommand::try_from(msg) {
                    session.last_activity = now;
                    match cmd {
                        ClientCommand::Disconnect => {
                            // Client-initiated graceful disconnect.
                            session.data_channels.clear();
                            disconnected.push(session.conn.id);
                            break;
                        }
                        ClientCommand::Heartbeat => {}
                        ClientCommand::OpenTx { channel, .. } => {
                            if let Err(err) =
                                session.data_channels.handle_open_tx(&session.conn, channel)
                            {
                                let _ = err;
                            }
                        }
                        ClientCommand::OpenRx { channel, .. } => {
                            if let Err(err) =
                                session.data_channels.handle_open_rx(&session.conn, channel)
                            {
                                let _ = err;
                            }
                        }
                        ClientCommand::CloseChannel(channel) => {
                            session.close_data_channel(channel);
                            if let Err(err) = session
                                .conn
                                .tx
                                .push(DriverMessage::ChannelClosed(channel))
                                .map_err(|_| ConnectionError::QueueFull)
                            {
                                let _ = err;
                            }
                        }
                        ClientCommand::SubscribeRemote { .. } => {
                            // Remote subscriptions are only supported by the runtime driver.
                            // This legacy driver doesn't support them.
                        }
                    }
                }
            }

            if now.duration_since(session.last_activity) > timeout {
                // Control heartbeat missed: treat as session death. Notify and drop.
                let _ = session.conn.tx.push(DriverMessage::Shutdown);
                session.data_channels.clear();
                disconnected.push(session.conn.id);
            }
        }

        for id in &disconnected {
            self.sessions.remove(id);
        }

        disconnected
    }

    /// Returns the number of active sessions.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.sessions.len()
    }

    /// Sends a message to all connected clients.
    ///
    /// Returns the number of clients that successfully received the message.
    #[must_use]
    pub fn broadcast(&self, msg: DriverMessage) -> usize {
        self.sessions
            .values()
            .filter(|s| s.conn.tx.push(msg).is_ok())
            .count()
    }

    /// Broadcasts a shutdown message and drops all sessions.
    pub fn shutdown(self) {
        let _ = self.broadcast(DriverMessage::Shutdown);
    }

    /// Returns a reference to a client's data channel.
    ///
    /// Returns `None` if the client or channel doesn't exist.
    /// Returns `Some(DataEndpoint::Inbound)` or `Some(DataEndpoint::Outbound)`
    /// depending on the channel direction.
    #[must_use]
    pub fn channel(&self, client: ClientId, channel: ChannelId) -> Option<&DataEndpoint> {
        self.sessions
            .get(&client)
            .and_then(|s| s.data_channels.get(channel))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::Client;
    use crate::ipc::spsc::Timeout;
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use std::thread;
    use std::time::Duration;
    use type_hash::TypeHash;

    fn with_clean_inbox<F, R>(f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let path = driver_inbox_path();
        let _ = rustix::shm::unlink(path.as_ref());
        f()
    }

    #[test]
    #[serial]
    fn driver_new_creates_inbox() {
        with_clean_inbox(|| {
            let driver = Driver::new().unwrap();
            assert_eq!(driver.connection_count(), 0);
            drop(driver);
        });
    }

    #[test]
    #[serial]
    fn driver_new_fails_if_already_running() {
        with_clean_inbox(|| {
            let driver1 = Driver::new().unwrap();
            let result = Driver::new();
            assert!(matches!(result, Err(ConnectionError::Shm(_))));
            drop(driver1);
        });
    }

    #[test]
    #[serial]
    fn driver_accept_pending_empty() {
        with_clean_inbox(|| {
            let mut driver = Driver::new().unwrap();
            let accepted = driver.accept_pending();
            assert_eq!(accepted, 0);
        });
    }

    #[test]
    #[serial]
    fn driver_broadcast_no_clients() {
        with_clean_inbox(|| {
            let driver = Driver::new().unwrap();
            let sent = driver.broadcast(DriverMessage::Shutdown);
            assert_eq!(sent, 0);
        });
    }

    #[test]
    #[serial]
    fn driver_tick_empty_sessions() {
        with_clean_inbox(|| {
            let mut driver = Driver::new().unwrap();
            let disconnected = driver.tick(Duration::from_secs(10));
            assert!(disconnected.is_empty());
        });
    }

    #[test]
    #[serial]
    fn driver_channel_no_session() {
        with_clean_inbox(|| {
            let driver = Driver::new().unwrap();
            let fake_id = ClientId::generate();

            assert!(driver.channel(fake_id, ChannelId::new(1)).is_none());
        });
    }

    #[test]
    #[serial]
    fn driver_data_channel_access() {
        with_clean_inbox(|| {
            #[derive(Serialize, Deserialize, TypeHash, Debug, PartialEq)]
            struct TestMsg(u32);

            use std::sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            };

            let data_sent = Arc::new(AtomicBool::new(false));
            let data_sent_clone = Arc::clone(&data_sent);

            let driver_thread = thread::spawn(move || {
                let mut driver = Driver::new().unwrap();

                // Wait for client to connect
                for _ in 0..20 {
                    if driver.accept_pending() > 0 {
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }

                let client_id = driver.sessions.keys().next().copied();

                // Process data channel open and wait for data to be sent
                for _ in 0..50 {
                    driver.tick(Duration::from_secs(10));
                    if data_sent_clone.load(Ordering::Acquire) {
                        // Give a little more time after data is sent
                        thread::sleep(Duration::from_millis(20));
                        driver.tick(Duration::from_secs(10));
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }

                // Check channel exists and read data
                if let Some(id) = client_id {
                    let endpoint = driver.channel(id, ChannelId::new(1));
                    assert!(endpoint.is_some(), "channel should exist");

                    if let Some(DataEndpoint::Inbound(consumer)) = endpoint {
                        // Try a few times to read
                        for _ in 0..10 {
                            if let Some(frame) = consumer.pop() {
                                let msg: TestMsg = frame.decode().unwrap();
                                assert_eq!(msg, TestMsg(999));
                                return driver;
                            }
                            thread::sleep(Duration::from_millis(10));
                        }
                        panic!("Failed to read message from inbound channel");
                    } else {
                        panic!("Expected inbound channel");
                    }
                }

                driver
            });

            thread::sleep(Duration::from_millis(10));
            let inbox_path = driver_inbox_path();
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            // Open TX channel
            let tx = client
                .open_data_tx::<TestMsg>(ChannelId::new(1), Duration::from_secs(1))
                .unwrap();

            // Send data
            tx.send(&TestMsg(999)).unwrap();
            data_sent.store(true, Ordering::Release);

            // Wait for driver to finish
            let _ = driver_thread.join().unwrap();

            client.disconnect();
        });
    }

    #[test]
    #[serial]
    fn driver_shutdown() {
        with_clean_inbox(|| {
            let driver_thread = thread::spawn(|| {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                driver.accept_pending();
                driver.shutdown();
            });

            thread::sleep(Duration::from_millis(10));
            let inbox_path = driver_inbox_path();
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            // Wait for shutdown message
            thread::sleep(Duration::from_millis(100));
            let msg = client.recv();

            // Client should receive shutdown
            assert!(matches!(msg, Some(DriverMessage::Shutdown)));

            driver_thread.join().unwrap();
        });
    }

    #[test]
    #[serial]
    fn driver_handles_client_disconnect() {
        with_clean_inbox(|| {
            let driver_thread = thread::spawn(|| {
                let mut driver = Driver::new().unwrap();
                thread::sleep(Duration::from_millis(50));
                let accepted = driver.accept_pending();
                assert_eq!(accepted, 1);

                // Wait for disconnect command
                thread::sleep(Duration::from_millis(100));
                let disconnected = driver.tick(Duration::from_secs(10));

                (driver.connection_count(), disconnected.len())
            });

            thread::sleep(Duration::from_millis(10));
            let inbox_path = driver_inbox_path();
            let client =
                Client::connect(inbox_path, Timeout::Duration(Duration::from_secs(1))).unwrap();

            // Disconnect
            client.disconnect();

            let (count, disconnected) = driver_thread.join().unwrap();
            assert_eq!(count, 0);
            assert_eq!(disconnected, 1);
        });
    }
}
