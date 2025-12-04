//! Driver-side session management for client control connections.

use std::collections::HashMap;
use std::time::Duration;

use crate::control::types::{
    CLIENT_QUEUE_CAPACITY, ClientCommand, ClientHello, ClientId, ClientMessage, Connection,
    ConnectionError, DRIVER_INBOX_CAPACITY, DriverMessage, channel_paths, driver_inbox_path,
};
use crate::ipc::shmem::{Creator, Opener};
use crate::ipc::spsc::{Consumer, Producer, Timeout};
use minstant::Instant;

/// Timeout for receiving Hello message during handshake.
const HELLO_TIMEOUT: Duration = Duration::from_millis(500);

type DriverConnection = Connection<
    Producer<DriverMessage, CLIENT_QUEUE_CAPACITY, Opener>,
    Consumer<ClientMessage, CLIENT_QUEUE_CAPACITY, Opener>,
>;

struct Session {
    conn: DriverConnection,
    last_activity: Instant,
}

impl Session {
    fn new(conn: DriverConnection) -> Self {
        Self {
            conn,
            last_activity: Instant::now(),
        }
    }
}

fn accept(client_id: ClientId) -> Result<DriverConnection, ConnectionError> {
    let (client_tx_path, client_rx_path) = channel_paths(&client_id);

    let tx = Producer::<DriverMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(client_rx_path)?;
    let rx = Consumer::<ClientMessage, CLIENT_QUEUE_CAPACITY, Opener>::open(client_tx_path)?;

    // Parse: Expect Hello message
    match rx
        .pop_blocking(Timeout::Duration(HELLO_TIMEOUT))
        .map(ClientHello::try_from)
    {
        Some(Ok(hello)) if hello.id == client_id => {}
        Some(_) => return Err(ConnectionError::ProtocolViolation),
        None => return Err(ConnectionError::Timeout),
    }

    tx.push(DriverMessage::Welcome)
        .map_err(|_| ConnectionError::QueueFull)?;

    Ok(Connection {
        id: client_id,
        tx,
        rx,
    })
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
            match accept(client_id) {
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
                            disconnected.push(session.conn.id);
                            break;
                        }
                        ClientCommand::Heartbeat => {}
                    }
                }
            }

            if now.duration_since(session.last_activity) > timeout {
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
