//! Driver control thread runtime.
//!
//! Responsibilities:
//! - Own canonical state: clients, channels, endpoint lists, publications.
//! - Apply client control commands (open/close channel, etc.).
//! - Emit deltas to TX/RX threads via SPSC command rings.
//! - Handle client session lifecycle (accept, heartbeat, disconnect).
//! - Process driver-driver protocol events (SETUP, TEARDOWN).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use minstant::Instant;

use crate::trace::{debug, info, trace, warn};

use crate::control::handshake::driver_accept;
use crate::control::types::{
    ChannelId, ClientCommand, ClientId, ConnectionError, ControlConnection, DRIVER_INBOX_CAPACITY,
    DriverMessage, DriverRole, RemoteEndpoint, SubscriptionFailure, TypeId, data_rx_path,
    data_tx_path, driver_inbox_path,
};
use crate::ipc::mpsc::Consumer as MpscConsumer;
use crate::ipc::shmem::Creator;
use crate::ipc::spsc::{Consumer as IpcConsumer, Producer as IpcProducer};
use crate::net::Endpoint;
use crate::sync::spsc::{Consumer, Producer};

use super::commands::{
    COMMAND_QUEUE_CAPACITY, ClientRxQueue, ClientTxQueue, RemoteStreamKey, RxCommand,
    RxToControlEvent, TxCommand,
};
use super::protocol::{
    NakReason, ProtocolFrame, SessionId, SetupAckFrame, SetupFrame, SetupNakFrame, encode_frame,
};

type DriverConnection = ControlConnection<DriverRole>;

/// Per-client session state.
struct Session {
    /// Control connection to the client.
    conn: DriverConnection,
    /// Last activity timestamp for timeout detection.
    last_activity: Instant,
    /// Channels owned by this client.
    channels: HashMap<ChannelId, ChannelDirection>,
}

/// Direction of a data channel.
#[derive(Clone, Copy)]
enum ChannelDirection {
    /// Client → Driver (client TX, driver RX).
    Inbound,
    /// Driver → Client (driver TX, client RX).
    Outbound,
}

impl Session {
    fn new(conn: DriverConnection) -> Self {
        Self {
            conn,
            last_activity: Instant::now(),
            channels: HashMap::new(),
        }
    }
}

/// State for a published channel (this driver publishes data to remote subscribers).
struct Publication {
    /// The type ID of messages on this channel.
    type_id: TypeId,
    /// The client that owns this publication.
    #[allow(dead_code)]
    client: ClientId,
}

/// State for a pending outgoing subscription (waiting for SETUP_ACK/NAK).
struct PendingSubscription {
    /// Local channel ID for this subscription.
    local_channel: ChannelId,
    /// Remote channel we're subscribing to.
    remote_channel: ChannelId,
    /// Client that requested this subscription.
    client: ClientId,
    /// Remote endpoint we sent SETUP to.
    remote: Endpoint,
}

/// State for an active remote subscription (receiving data from remote publisher).
struct RemoteSubscription {
    /// Local channel ID for routing to client.
    local_channel: ChannelId,
    /// Remote publisher's session ID (for Status Messages).
    #[allow(dead_code)]
    publisher_session: SessionId,
    /// Client that owns this subscription.
    client: ClientId,
}

/// Control thread state and event loop.
pub struct ControlThread {
    /// Inbox for incoming client connection requests (MPSC - multiple clients).
    inbox: MpscConsumer<ClientId, DRIVER_INBOX_CAPACITY, Creator>,
    /// Active client sessions.
    sessions: HashMap<ClientId, Session>,
    /// Command queue to TX thread.
    tx_commands: Producer<TxCommand, COMMAND_QUEUE_CAPACITY>,
    /// Command queue to RX thread.
    rx_commands: Producer<RxCommand, COMMAND_QUEUE_CAPACITY>,
    /// Event queue from RX thread (protocol frames).
    rx_events: Consumer<RxToControlEvent, COMMAND_QUEUE_CAPACITY>,
    /// Published channels (ChannelId → Publication).
    /// These are channels where local clients send data that we publish to remote subscribers.
    publications: HashMap<ChannelId, Publication>,
    /// Pending outgoing subscriptions (SessionId → PendingSubscription).
    /// Keyed by our session ID from the SETUP we sent.
    pending_subscriptions: HashMap<SessionId, PendingSubscription>,
    /// Active remote subscriptions (keyed by (remote_endpoint, remote_channel)).
    /// Used to route incoming DATA frames to the correct local client.
    remote_subscriptions: HashMap<(Endpoint, ChannelId), RemoteSubscription>,
    /// Network endpoints for channel fan-out.
    /// TODO: This should come from configuration or discovery.
    default_endpoints: Vec<Endpoint>,
    /// Session timeout duration.
    session_timeout: Duration,
    /// Shutdown flag - shared with Driver handle.
    shutdown_flag: Arc<AtomicBool>,
    /// Reusable buffer for encoding protocol frames.
    encode_buf: Vec<u8>,
}

impl ControlThread {
    /// Creates a new control thread state.
    ///
    /// # Arguments
    ///
    /// * `inbox_path` - Path for the driver inbox. If `None`, uses the default path.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver inbox cannot be created (e.g., already exists).
    pub fn new(
        tx_commands: Producer<TxCommand, COMMAND_QUEUE_CAPACITY>,
        rx_commands: Producer<RxCommand, COMMAND_QUEUE_CAPACITY>,
        rx_events: Consumer<RxToControlEvent, COMMAND_QUEUE_CAPACITY>,
        default_endpoints: Vec<Endpoint>,
        session_timeout: Duration,
        shutdown_flag: Arc<AtomicBool>,
        inbox_path: Option<crate::ipc::shmem::ShmPath>,
    ) -> Result<Self, ConnectionError> {
        let path = inbox_path.unwrap_or_else(driver_inbox_path);
        let inbox = MpscConsumer::create(path)?;

        Ok(Self {
            inbox,
            sessions: HashMap::new(),
            tx_commands,
            rx_commands,
            rx_events,
            publications: HashMap::new(),
            pending_subscriptions: HashMap::new(),
            remote_subscriptions: HashMap::new(),
            default_endpoints,
            session_timeout,
            shutdown_flag,
            encode_buf: Vec::with_capacity(64),
        })
    }

    /// Runs the control thread event loop.
    ///
    /// Returns when shutdown is requested via the shutdown flag.
    pub fn run(&mut self) {
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            // Accept pending connections
            self.accept_pending();

            // Process protocol events from RX thread
            self.process_protocol_events();

            // Process client messages and check timeouts
            let disconnected = self.tick();

            // Clean up disconnected sessions
            for client_id in disconnected {
                self.cleanup_session(client_id);
            }

            // TODO: Use timing wheel for session timeouts instead of polling

            // Brief sleep to avoid busy-spinning (1ms)
            std::thread::sleep(Duration::from_millis(1));
        }

        // Graceful shutdown
        self.do_shutdown();
    }

    /// Processes incoming protocol events from RX thread.
    fn process_protocol_events(&mut self) {
        while let Some(event) = self.rx_events.pop() {
            match event {
                RxToControlEvent::SetupRequest {
                    from,
                    session,
                    channel,
                    type_id,
                    receiver_window,
                    mtu,
                } => {
                    self.handle_setup_request(
                        from,
                        session,
                        channel,
                        type_id,
                        receiver_window,
                        mtu,
                    );
                }
                RxToControlEvent::SetupAck {
                    from,
                    session,
                    publisher_session,
                    mtu: _,
                } => {
                    self.handle_setup_ack(from, session, publisher_session);
                }
                RxToControlEvent::SetupNak {
                    from: _,
                    session,
                    reason,
                } => {
                    self.handle_setup_nak(session, reason);
                }
                RxToControlEvent::Teardown { session, .. } => {
                    self.handle_teardown(session);
                }
            }
        }
    }

    /// Handles an incoming SETUP request from a remote subscriber.
    fn handle_setup_request(
        &mut self,
        from: Endpoint,
        session: SessionId,
        channel: ChannelId,
        type_id: TypeId,
        receiver_window: u32,
        mtu: u16,
    ) {
        info!(
            from = %from,
            session = %session,
            channel = %channel,
            type_id = %type_id,
            receiver_window = receiver_window,
            mtu = mtu,
            "received SETUP request"
        );

        // Check if we have a publication for this channel
        let publication = match self.publications.get(&channel) {
            Some(pub_) => pub_,
            None => {
                warn!(channel = %channel, from = %from, "SETUP rejected: channel not found");
                self.send_setup_nak(from, session, NakReason::ChannelNotFound);
                return;
            }
        };

        // Validate type hash
        if publication.type_id != type_id {
            warn!(
                channel = %channel,
                from = %from,
                expected_type = %publication.type_id,
                received_type = %type_id,
                "SETUP rejected: type mismatch"
            );
            self.send_setup_nak(from, session, NakReason::TypeMismatch);
            return;
        }

        // Generate publisher session ID for this subscription
        let publisher_session = SessionId::generate();

        // Negotiate MTU (use minimum of ours and theirs)
        // TODO: Make our MTU configurable
        let our_mtu: u16 = 1500;
        let negotiated_mtu = our_mtu.min(mtu);

        // Tell TX thread to add this subscriber
        let cmd = TxCommand::AddSubscriber {
            channel,
            session: publisher_session,
            endpoint: from,
            receiver_window,
        };
        if self.tx_commands.push(cmd).is_err() {
            warn!(channel = %channel, from = %from, "SETUP rejected: capacity exceeded");
            self.send_setup_nak(from, session, NakReason::CapacityExceeded);
            return;
        }

        info!(
            channel = %channel,
            subscriber = %from,
            publisher_session = %publisher_session,
            negotiated_mtu = negotiated_mtu,
            "SETUP accepted, sending ACK"
        );
        self.send_setup_ack(from, session, publisher_session, negotiated_mtu);
    }

    /// Sends a SETUP_ACK frame to a subscriber.
    fn send_setup_ack(
        &mut self,
        to: Endpoint,
        subscriber_session: SessionId,
        publisher_session: SessionId,
        mtu: u16,
    ) {
        let frame = ProtocolFrame::SetupAck(SetupAckFrame {
            session: subscriber_session,
            publisher_session,
            mtu,
        });

        if encode_frame(&frame, &mut self.encode_buf).is_ok() {
            let cmd = TxCommand::SendProtocolFrame {
                endpoint: to,
                frame_bytes: self.encode_buf.clone(),
            };
            let _ = self.tx_commands.push(cmd);
        }
    }

    /// Sends a SETUP_NAK frame to a subscriber.
    fn send_setup_nak(&mut self, to: Endpoint, session: SessionId, reason: NakReason) {
        let frame = ProtocolFrame::SetupNak(SetupNakFrame { session, reason });

        if encode_frame(&frame, &mut self.encode_buf).is_ok() {
            let cmd = TxCommand::SendProtocolFrame {
                endpoint: to,
                frame_bytes: self.encode_buf.clone(),
            };
            let _ = self.tx_commands.push(cmd);
        }
    }

    /// Handles a TEARDOWN from a remote peer.
    fn handle_teardown(&mut self, session: SessionId) {
        // Find and remove the subscriber from all channels
        for (channel, _) in self.publications.iter() {
            let cmd = TxCommand::RemoveSubscriber {
                channel: *channel,
                session,
            };
            let _ = self.tx_commands.push(cmd);
        }
    }

    /// Handles SETUP_ACK from a remote publisher (subscription accepted).
    fn handle_setup_ack(
        &mut self,
        from: Endpoint,
        session: SessionId,
        publisher_session: SessionId,
    ) {
        info!(
            from = %from,
            session = %session,
            publisher_session = %publisher_session,
            "received SETUP_ACK"
        );

        // Find the pending subscription by our session ID
        let pending = match self.pending_subscriptions.remove(&session) {
            Some(p) => p,
            None => {
                warn!(session = %session, "SETUP_ACK for unknown session, ignoring");
                return;
            }
        };

        // Verify the ACK came from the expected endpoint
        if pending.remote != from {
            warn!(
                session = %session,
                expected = %pending.remote,
                actual = %from,
                "SETUP_ACK from unexpected endpoint, ignoring"
            );
            self.pending_subscriptions.insert(session, pending);
            return;
        }

        info!(
            local_channel = %pending.local_channel,
            remote_channel = %pending.remote_channel,
            remote = %from,
            "subscription established"
        );

        // Register the active subscription for DATA routing
        let key = (from, pending.remote_channel);
        self.remote_subscriptions.insert(
            key,
            RemoteSubscription {
                local_channel: pending.local_channel,
                publisher_session,
                client: pending.client,
            },
        );

        // Tell RX thread to route DATA for this remote channel to the local client
        let remote_key = RemoteStreamKey {
            endpoint: from,
            channel: pending.remote_channel,
        };
        let _ = self.rx_commands.push(RxCommand::AddRemoteMapping {
            remote: remote_key,
            local_channel: pending.local_channel,
        });

        // Notify the client that subscription is ready
        if let Some(session) = self.sessions.get(&pending.client) {
            let _ = session
                .conn
                .tx
                .push(DriverMessage::SubscriptionReady(pending.local_channel));
        }
    }

    /// Handles SETUP_NAK from a remote publisher (subscription rejected).
    fn handle_setup_nak(&mut self, session: SessionId, reason: NakReason) {
        warn!(session = %session, reason = ?reason, "received SETUP_NAK");

        // Find the pending subscription by our session ID
        let pending = match self.pending_subscriptions.remove(&session) {
            Some(p) => p,
            None => {
                warn!(session = %session, "SETUP_NAK for unknown session, ignoring");
                return;
            }
        };

        warn!(
            local_channel = %pending.local_channel,
            remote_channel = %pending.remote_channel,
            remote = %pending.remote,
            reason = ?reason,
            "subscription rejected by remote"
        );

        // Convert NAK reason to subscription failure
        let failure = match reason {
            NakReason::TypeMismatch => SubscriptionFailure::TypeMismatch,
            NakReason::ChannelNotFound => SubscriptionFailure::ChannelNotFound,
            NakReason::CapacityExceeded => SubscriptionFailure::CapacityExceeded,
            NakReason::Unknown => SubscriptionFailure::NetworkError,
        };

        // Notify the client that subscription failed
        if let Some(session) = self.sessions.get(&pending.client) {
            let _ = session.conn.tx.push(DriverMessage::SubscriptionFailed {
                channel: pending.local_channel,
                reason: failure,
            });
        }
    }

    /// Initiates a subscription to a remote publisher.
    fn initiate_remote_subscription(
        &mut self,
        client: ClientId,
        local_channel: ChannelId,
        type_id: TypeId,
        remote: RemoteEndpoint,
        remote_channel: ChannelId,
    ) {
        let remote_endpoint = Endpoint::from(remote);

        // Generate a session ID for this subscription handshake
        let session = SessionId::generate();

        info!(
            client = %client,
            local_channel = %local_channel,
            remote = %remote_endpoint,
            remote_channel = %remote_channel,
            session = %session,
            type_id = %type_id,
            "initiating remote subscription, sending SETUP"
        );

        // Track the pending subscription
        self.pending_subscriptions.insert(
            session,
            PendingSubscription {
                local_channel,
                remote_channel,
                client,
                remote: remote_endpoint,
            },
        );

        // Build and send SETUP frame
        let frame = ProtocolFrame::Setup(SetupFrame {
            session,
            channel: remote_channel,
            type_id,
            receiver_window: 128 * 1024, // 128KB default
            mtu: 1500,
        });

        if encode_frame(&frame, &mut self.encode_buf).is_ok() {
            let cmd = TxCommand::SendProtocolFrame {
                endpoint: remote_endpoint,
                frame_bytes: self.encode_buf.clone(),
            };
            let _ = self.tx_commands.push(cmd);
        }
    }

    /// Performs graceful shutdown: notifies clients and TX/RX threads.
    fn do_shutdown(&mut self) {
        // Notify all clients
        for session in self.sessions.values() {
            let _ = session.conn.tx.push(DriverMessage::Shutdown);
        }

        // Send shutdown to TX/RX threads
        let _ = self.tx_commands.push(TxCommand::Shutdown);
        let _ = self.rx_commands.push(RxCommand::Shutdown);
    }

    /// Accepts all pending connection requests.
    fn accept_pending(&mut self) {
        while let Some(client_id) = self.inbox.pop() {
            debug!(client_id = %client_id, "accepting client from inbox");
            match driver_accept(client_id) {
                Ok(conn) => {
                    info!(client_id = %client_id, "client session established");
                    self.sessions.insert(conn.id, Session::new(conn));
                }
                Err(e) => {
                    warn!(client_id = %client_id, error = ?e, "failed to accept client");
                }
            }
        }
    }

    /// Processes messages from all sessions and detects timeouts.
    ///
    /// Returns IDs of clients that should be disconnected.
    fn tick(&mut self) -> Vec<ClientId> {
        let mut disconnected = Vec::new();
        let mut new_publications = Vec::new();
        let mut new_remote_subscriptions: Vec<(
            ClientId,
            ChannelId,
            TypeId,
            RemoteEndpoint,
            ChannelId,
        )> = Vec::new();
        let now = Instant::now();
        let session_timeout = self.session_timeout;

        for session in self.sessions.values_mut() {
            // Process pending messages
            while let Some(msg) = session.conn.rx.pop() {
                if let Ok(cmd) = ClientCommand::try_from(msg) {
                    session.last_activity = now;

                    match cmd {
                        ClientCommand::Disconnect => {
                            disconnected.push(session.conn.id);
                            break;
                        }
                        ClientCommand::Heartbeat => {
                            // Just update last_activity (already done above)
                        }
                        ClientCommand::OpenTx { channel, type_id } => {
                            if let Some(pub_info) = Self::handle_open_tx(
                                session,
                                channel,
                                type_id,
                                &self.tx_commands,
                                &self.default_endpoints,
                            ) {
                                new_publications.push(pub_info);
                            }
                        }
                        ClientCommand::OpenRx { channel, .. } => {
                            Self::handle_open_rx(session, channel, &self.rx_commands);
                        }
                        ClientCommand::SubscribeRemote {
                            channel,
                            type_id,
                            remote,
                            remote_channel,
                        } => {
                            // First open the local RX channel (client already created the queue)
                            Self::handle_open_rx(session, channel, &self.rx_commands);
                            // Queue the subscription request for processing after the loop
                            new_remote_subscriptions.push((
                                session.conn.id,
                                channel,
                                type_id,
                                remote,
                                remote_channel,
                            ));
                        }
                        ClientCommand::CloseChannel(channel) => {
                            Self::handle_close_channel(
                                session,
                                channel,
                                &self.tx_commands,
                                &self.rx_commands,
                            );
                        }
                    }
                }
            }

            // Check for timeout
            if now.duration_since(session.last_activity) > session_timeout {
                let _ = session.conn.tx.push(DriverMessage::Shutdown);
                disconnected.push(session.conn.id);
            }
        }

        // Register new publications after the session loop
        for (channel, publication) in new_publications {
            self.publications.insert(channel, publication);
        }

        // Initiate remote subscriptions after the session loop
        for (client, local_channel, type_id, remote, remote_channel) in new_remote_subscriptions {
            self.initiate_remote_subscription(
                client,
                local_channel,
                type_id,
                remote,
                remote_channel,
            );
        }

        disconnected
    }

    /// Handles OpenTx command: client wants to send data to driver.
    ///
    /// Returns the publication info to register if successful.
    fn handle_open_tx(
        session: &mut Session,
        channel: ChannelId,
        type_id: TypeId,
        tx_commands: &Producer<TxCommand, COMMAND_QUEUE_CAPACITY>,
        default_endpoints: &[Endpoint],
    ) -> Option<(ChannelId, Publication)> {
        debug!(
            client = %session.conn.id,
            channel = %channel,
            type_id = %type_id,
            "handling OpenTx command"
        );

        if session.channels.contains_key(&channel) {
            warn!(client = %session.conn.id, channel = %channel, "OpenTx failed: channel already exists");
            let _ = session.conn.tx.push(DriverMessage::ChannelError(channel));
            return None;
        }

        let path = data_tx_path(&session.conn.id, channel);
        let queue: ClientTxQueue = match IpcConsumer::open(path) {
            Ok(q) => q,
            Err(e) => {
                warn!(client = %session.conn.id, channel = %channel, error = ?e, "OpenTx failed: cannot open queue");
                let _ = session.conn.tx.push(DriverMessage::ChannelError(channel));
                return None;
            }
        };

        // Send command to TX thread
        let cmd = TxCommand::AddChannel {
            channel,
            client: session.conn.id,
            queue,
            endpoints: default_endpoints.to_vec(),
        };
        if tx_commands.push(cmd).is_err() {
            warn!(client = %session.conn.id, channel = %channel, "OpenTx failed: command queue full");
            let _ = session.conn.tx.push(DriverMessage::ChannelError(channel));
            return None;
        }

        session.channels.insert(channel, ChannelDirection::Inbound);
        let _ = session.conn.tx.push(DriverMessage::ChannelReady(channel));

        info!(
            client = %session.conn.id,
            channel = %channel,
            type_id = %type_id,
            endpoints = ?default_endpoints,
            "TX channel opened (publication registered)"
        );

        // Return publication info to register
        Some((
            channel,
            Publication {
                type_id,
                client: session.conn.id,
            },
        ))
    }

    /// Handles OpenRx command: client wants to receive data from driver.
    fn handle_open_rx(
        session: &mut Session,
        channel: ChannelId,
        rx_commands: &Producer<RxCommand, COMMAND_QUEUE_CAPACITY>,
    ) {
        debug!(client = %session.conn.id, channel = %channel, "handling OpenRx command");

        if session.channels.contains_key(&channel) {
            warn!(client = %session.conn.id, channel = %channel, "OpenRx failed: channel already exists");
            let _ = session.conn.tx.push(DriverMessage::ChannelError(channel));
            return;
        }

        let path = data_rx_path(&session.conn.id, channel);
        let queue: ClientRxQueue = match IpcProducer::open(path) {
            Ok(q) => q,
            Err(e) => {
                warn!(client = %session.conn.id, channel = %channel, error = ?e, "OpenRx failed: cannot open queue");
                let _ = session.conn.tx.push(DriverMessage::ChannelError(channel));
                return;
            }
        };

        // Send command to RX thread
        let cmd = RxCommand::AddChannel {
            channel,
            client: session.conn.id,
            queue,
        };
        if rx_commands.push(cmd).is_err() {
            warn!(client = %session.conn.id, channel = %channel, "OpenRx failed: command queue full");
            let _ = session.conn.tx.push(DriverMessage::ChannelError(channel));
            return;
        }

        session.channels.insert(channel, ChannelDirection::Outbound);
        let _ = session.conn.tx.push(DriverMessage::ChannelReady(channel));

        info!(client = %session.conn.id, channel = %channel, "RX channel opened");
    }

    /// Handles CloseChannel command.
    fn handle_close_channel(
        session: &mut Session,
        channel: ChannelId,
        tx_commands: &Producer<TxCommand, COMMAND_QUEUE_CAPACITY>,
        rx_commands: &Producer<RxCommand, COMMAND_QUEUE_CAPACITY>,
    ) {
        if let Some(direction) = session.channels.remove(&channel) {
            match direction {
                ChannelDirection::Inbound => {
                    let _ = tx_commands.push(TxCommand::RemoveChannel { channel });
                }
                ChannelDirection::Outbound => {
                    let _ = rx_commands.push(RxCommand::RemoveChannel { channel });
                }
            }
        }
        let _ = session.conn.tx.push(DriverMessage::ChannelClosed(channel));
    }

    /// Cleans up a disconnected session.
    fn cleanup_session(&mut self, client_id: ClientId) {
        if let Some(session) = self.sessions.remove(&client_id) {
            // Remove all channels owned by this client
            for (channel, direction) in session.channels {
                match direction {
                    ChannelDirection::Inbound => {
                        let _ = self.tx_commands.push(TxCommand::RemoveChannel { channel });
                    }
                    ChannelDirection::Outbound => {
                        let _ = self.rx_commands.push(RxCommand::RemoveChannel { channel });
                    }
                }
            }
        }
    }

    /// Returns the number of active sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}
