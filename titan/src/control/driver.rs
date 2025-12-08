//! Driver-side session management for client control connections.

use crate::control::handshake::driver_accept;
use crate::control::types::{
    CONTROL_QUEUE_CAPACITY, ClientCommand, ClientId, ClientMessage, ConnectionError,
    ControlConnection, DATA_QUEUE_CAPACITY, DRIVER_INBOX_CAPACITY, DataChannelRequest,
    DataDirection, DriverMessage, data_channel_path, driver_inbox_path,
};
use crate::data::Frame;
use crate::ipc::shmem::{Creator, Opener};
use crate::ipc::spsc::{Consumer, Producer};
use minstant::Instant;
use std::collections::HashMap;
use std::time::Duration;

type DriverConnection = ControlConnection<
    Producer<DriverMessage, CONTROL_QUEUE_CAPACITY, Opener>,
    Consumer<ClientMessage, CONTROL_QUEUE_CAPACITY, Opener>,
>;

enum DataEndpoint {
    Inbound(Consumer<Frame, DATA_QUEUE_CAPACITY, Opener>),
    Outbound(Producer<Frame, DATA_QUEUE_CAPACITY, Opener>),
}

struct Session {
    conn: DriverConnection,
    last_activity: Instant,
    data_channels: HashMap<(u32, DataDirection), DataEndpoint>,
}

impl Session {
    fn new(conn: DriverConnection) -> Self {
        Self {
            conn,
            last_activity: Instant::now(),
            data_channels: HashMap::new(),
        }
    }

    fn close_data_channel(&mut self, req: DataChannelRequest) {
        self.data_channels.remove(&(req.channel, req.dir));
    }
}

fn handle_open_data(session: &mut Session, req: DataChannelRequest) -> Result<(), ConnectionError> {
    let path = data_channel_path(&session.conn.id, req.channel, req.dir);

    match req.dir {
        DataDirection::Tx => {
            // Client created producer; driver opens consumer.
            match Consumer::<Frame, DATA_QUEUE_CAPACITY, Opener>::open(path) {
                Ok(rx) => {
                    session
                        .data_channels
                        .insert((req.channel, req.dir), DataEndpoint::Inbound(rx));
                    let _ = session.conn.tx.push(DriverMessage::DataChannelReady(req));
                }
                Err(err) => {
                    eprintln!("Failed to open inbound data channel {:?}: {err}", req);
                    let _ = session.conn.tx.push(DriverMessage::DataChannelError(req));
                }
            }
        }
        DataDirection::Rx => {
            // Client created consumer; driver opens producer.
            match Producer::<Frame, DATA_QUEUE_CAPACITY, Opener>::open(path) {
                Ok(tx) => {
                    session
                        .data_channels
                        .insert((req.channel, req.dir), DataEndpoint::Outbound(tx));
                    let _ = session.conn.tx.push(DriverMessage::DataChannelReady(req));
                }
                Err(err) => {
                    eprintln!("Failed to open outbound data channel {:?}: {err}", req);
                    let _ = session.conn.tx.push(DriverMessage::DataChannelError(req));
                }
            }
        }
    }

    Ok(())
}

/// Manages client sessions and processes incoming connections.
///
/// The driver owns an inbox queue where clients announce themselves, then
/// opens their tx/rx channels to complete the handshake.
pub struct Driver {
    inbox: Consumer<ClientId, DRIVER_INBOX_CAPACITY, Creator>,
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
        let inbox = Consumer::create(path)?;

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
            match driver_accept(client_id) {
                Ok(conn) => {
                    self.sessions.insert(conn.id, Session::new(conn));
                    count += 1;
                }
                Err(e) => {
                    eprintln!("Failed to accept client {client_id:?}: {e}");
                }
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
                        ClientCommand::OpenDataChannel(req) => {
                            if let Err(err) = handle_open_data(session, req) {
                                eprintln!("Failed to open data channel {:?}: {err}", req);
                            }
                        }
                        ClientCommand::CloseDataChannel(req) => {
                            session.close_data_channel(req);
                            let _ = session.conn.tx.push(DriverMessage::DataChannelClosed(req));
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
}
