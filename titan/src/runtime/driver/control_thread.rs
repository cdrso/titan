//! Driver control thread runtime.
//!
//! Responsibilities:
//! - Own canonical state: clients, channels, endpoint lists, publications.
//! - Apply client control commands (open/close channel, etc.).
//! - Emit deltas to TX/RX threads via SPSC command rings.
//! - Handle client session lifecycle (accept, heartbeat, disconnect).
//! - Process driver-driver protocol events (SETUP, TEARDOWN).

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::runtime::timing::{Duration as WheelDuration, Millis, NonZeroDuration, WheelScope, with_wheel};
use crate::trace::{debug, info, warn};

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
    Mtu, NakReason, ProtocolFrame, ReceiverWindow, SessionId, SetupAckFrame, SetupFrame,
    SetupNakFrame, encode_frame,
};

type DriverConnection = ControlConnection<DriverRole>;

/// Timing wheel slots (covers ~255ms max delay at 1ms tick).
/// Session timeouts are typically 30s, so we use larger wheel.
const WHEEL_SLOTS: usize = 512;

/// Timing wheel capacity (max concurrent timers).
const WHEEL_CAPACITY: usize = 256;

/// Timer event types for control thread.
#[derive(Debug, Clone, Copy)]
enum ControlTimerEvent {
    /// Session timeout for a client.
    SessionTimeout(ClientId),
}

/// Per-client session state.
struct Session {
    /// Control connection to the client.
    conn: DriverConnection,
    /// Whether a timeout timer is currently scheduled.
    timeout_scheduled: bool,
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
            timeout_scheduled: false,
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

/// State for a pending outgoing subscription (waiting for `SETUP_ACK`/`NAK`).
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
    _local_channel: ChannelId,
    /// Remote publisher's session ID (for Status Messages).
    _publisher_session: SessionId,
    /// Client that owns this subscription.
    _client: ClientId,
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
    /// Published channels (`ChannelId` → `Publication`).
    /// These are channels where local clients send data that we publish to remote subscribers.
    publications: HashMap<ChannelId, Publication>,
    /// Pending outgoing subscriptions (`SessionId` → `PendingSubscription`).
    /// Keyed by our session ID from the SETUP we sent.
    pending_subscriptions: HashMap<SessionId, PendingSubscription>,
    /// Active remote subscriptions (keyed by (`remote_endpoint`, `remote_channel`)).
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
        let tick_duration =
            NonZeroDuration::<Millis>::new(NonZeroU64::new(1).expect("1 != 0"));
        let capacity = NonZeroUsize::new(WHEEL_CAPACITY).expect("WHEEL_CAPACITY != 0");

        with_wheel::<ControlTimerEvent, Millis, WHEEL_SLOTS, _>(tick_duration, capacity, |wheel| {
            self.run_with_wheel(wheel);
        });
    }

    /// Inner run loop with timing wheel.
    fn run_with_wheel(
        &mut self,
        wheel: &mut WheelScope<'_, ControlTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            // Accept pending connections and schedule their timeout timers
            self.accept_pending(wheel);

            // Process protocol events from RX thread
            self.process_protocol_events();

            // Process client messages (tracks activity for timeout reset)
            let (disconnected, activity_clients) = self.tick(wheel);

            // Clean up disconnected sessions
            for client_id in disconnected {
                self.cleanup_session(client_id);
            }

            // Reset timeout timers for clients with activity
            for client_id in activity_clients {
                self.reset_timeout(client_id, wheel);
            }

            // Fire due timers (session timeouts)
            self.tick_timers(wheel);

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
                    first_seq,
                    mtu: _,
                } => {
                    self.handle_setup_ack(from, session, publisher_session, first_seq);
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
        let Some(publication) = self.publications.get(&channel) else {
            warn!(channel = %channel, from = %from, "SETUP rejected: channel not found");
            self.send_setup_nak(from, session, NakReason::UnknownChannel);
            return;
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
            mtu: negotiated_mtu,
        };
        if self.tx_commands.push(cmd).is_err() {
            warn!(channel = %channel, from = %from, "SETUP rejected: capacity exceeded");
            self.send_setup_nak(from, session, NakReason::ResourceExhausted);
            return;
        }

        // TODO: Get first_seq from TX thread's current sequence for this channel
        // For now, use 0 as starting sequence
        let first_seq = 0u64;

        info!(
            channel = %channel,
            subscriber = %from,
            publisher_session = %publisher_session,
            negotiated_mtu = negotiated_mtu,
            first_seq = first_seq,
            "SETUP accepted, sending ACK"
        );
        self.send_setup_ack(from, session, publisher_session, first_seq, negotiated_mtu);
    }

    /// Sends a `SETUP_ACK` frame to a subscriber.
    fn send_setup_ack(
        &mut self,
        to: Endpoint,
        subscriber_session: SessionId,
        publisher_session: SessionId,
        first_seq: u64,
        mtu: u16,
    ) {
        let frame = ProtocolFrame::SetupAck(SetupAckFrame {
            session: subscriber_session,
            publisher_session,
            first_seq,
            mtu: Mtu::new(mtu),
        });

        if encode_frame(&frame, &mut self.encode_buf).is_ok() {
            let cmd = TxCommand::SendProtocolFrame {
                endpoint: to,
                frame_bytes: self.encode_buf.clone(),
            };
            if self.tx_commands.push(cmd).is_err() {
                warn!(endpoint = %to, "TX command queue full, dropping SETUP_ACK");
            }
        }
    }

    /// Sends a `SETUP_NAK` frame to a subscriber.
    fn send_setup_nak(&mut self, to: Endpoint, session: SessionId, reason: NakReason) {
        let frame = ProtocolFrame::SetupNak(SetupNakFrame { session, reason });

        if encode_frame(&frame, &mut self.encode_buf).is_ok() {
            let cmd = TxCommand::SendProtocolFrame {
                endpoint: to,
                frame_bytes: self.encode_buf.clone(),
            };
            if self.tx_commands.push(cmd).is_err() {
                warn!(endpoint = %to, "TX command queue full, dropping SETUP_NAK");
            }
        }
    }

    /// Handles a TEARDOWN from a remote peer.
    fn handle_teardown(&self, session: SessionId) {
        // Find and remove the subscriber from all channels
        for channel in self.publications.keys() {
            let cmd = TxCommand::RemoveSubscriber {
                channel: *channel,
                session,
            };
            if self.tx_commands.push(cmd).is_err() {
                warn!(channel = %channel, session = %session, "TX command queue full, dropping RemoveSubscriber");
            }
        }
    }

    /// Handles `SETUP_ACK` from a remote publisher (subscription accepted).
    fn handle_setup_ack(
        &mut self,
        from: Endpoint,
        session: SessionId,
        publisher_session: SessionId,
        first_seq: u64,
    ) {
        info!(
            from = %from,
            session = %session,
            publisher_session = %publisher_session,
            first_seq = first_seq,
            "received SETUP_ACK"
        );
        // TODO: Pass first_seq to reorder buffer initialization (Phase 5)
        let _ = first_seq;

        // Find the pending subscription by our session ID
        let Some(pending) = self.pending_subscriptions.remove(&session) else {
            warn!(session = %session, "SETUP_ACK for unknown session, ignoring");
            return;
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
                _local_channel: pending.local_channel,
                _publisher_session: publisher_session,
                _client: pending.client,
            },
        );

        // Tell RX thread to route DATA for this remote channel to the local client
        let remote_key = RemoteStreamKey {
            endpoint: from,
            channel: pending.remote_channel,
        };
        if self
            .rx_commands
            .push(RxCommand::AddRemoteMapping {
                remote: remote_key,
                local_channel: pending.local_channel,
            })
            .is_err()
        {
            warn!(channel = %pending.local_channel, "RX command queue full, dropping AddRemoteMapping");
        }

        // Notify the client that subscription is ready
        if let Some(session) = self.sessions.get(&pending.client)
            && session
                .conn
                .tx
                .push(DriverMessage::SubscriptionReady(pending.local_channel))
                .is_err()
        {
            warn!(client = %pending.client, channel = %pending.local_channel, "client queue full, dropping SubscriptionReady");
        }
    }

    /// Handles `SETUP_NAK` from a remote publisher (subscription rejected).
    fn handle_setup_nak(&mut self, session: SessionId, reason: NakReason) {
        warn!(session = %session, reason = ?reason, "received SETUP_NAK");

        // Find the pending subscription by our session ID
        let Some(pending) = self.pending_subscriptions.remove(&session) else {
            warn!(session = %session, "SETUP_NAK for unknown session, ignoring");
            return;
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
            NakReason::UnknownChannel => SubscriptionFailure::ChannelNotFound,
            NakReason::TypeMismatch => SubscriptionFailure::TypeMismatch,
            NakReason::NotAuthorized => SubscriptionFailure::NetworkError, // TODO: Add NotAuthorized failure type
            NakReason::ResourceExhausted => SubscriptionFailure::CapacityExceeded,
            NakReason::Unknown => SubscriptionFailure::NetworkError,
        };

        // Notify the client that subscription failed
        if let Some(session) = self.sessions.get(&pending.client)
            && session
                .conn
                .tx
                .push(DriverMessage::SubscriptionFailed {
                    channel: pending.local_channel,
                    reason: failure,
                })
                .is_err()
        {
            warn!(client = %pending.client, channel = %pending.local_channel, "client queue full, dropping SubscriptionFailed");
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
            receiver_window: ReceiverWindow::new(128 * 1024), // 128KB default
            mtu: Mtu::new(1500),
        });

        if encode_frame(&frame, &mut self.encode_buf).is_ok() {
            let cmd = TxCommand::SendProtocolFrame {
                endpoint: remote_endpoint,
                frame_bytes: self.encode_buf.clone(),
            };
            if self.tx_commands.push(cmd).is_err() {
                warn!(endpoint = %remote_endpoint, "TX command queue full, dropping SETUP");
            }
        }
    }

    /// Performs graceful shutdown: notifies clients and TX/RX threads.
    fn do_shutdown(&self) {
        // Notify all clients
        for session in self.sessions.values() {
            if session.conn.tx.push(DriverMessage::Shutdown).is_err() {
                warn!(client = %session.conn.id, "client queue full, dropping Shutdown");
            }
        }

        // Send shutdown to TX/RX threads (best-effort during shutdown)
        if self.tx_commands.push(TxCommand::Shutdown).is_err() {
            warn!("TX command queue full, dropping Shutdown");
        }
        if self.rx_commands.push(RxCommand::Shutdown).is_err() {
            warn!("RX command queue full, dropping Shutdown");
        }
    }

    /// Accepts all pending connection requests.
    fn accept_pending(
        &mut self,
        wheel: &mut WheelScope<'_, ControlTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        while let Some(client_id) = self.inbox.pop() {
            debug!(client_id = %client_id, "accepting client from inbox");
            match driver_accept(client_id) {
                Ok(conn) => {
                    info!(client_id = %client_id, "client session established");
                    let id = conn.id;
                    self.sessions.insert(id, Session::new(conn));
                    // Schedule initial timeout timer
                    self.schedule_timeout(id, wheel);
                }
                Err(_e) => {
                    warn!(client_id = %client_id, error = ?_e, "failed to accept client");
                }
            }
        }
    }

    /// Processes messages from all sessions.
    ///
    /// Returns (disconnected clients, clients with activity).
    fn tick(
        &mut self,
        _wheel: &mut WheelScope<'_, ControlTimerEvent, Millis, WHEEL_SLOTS>,
    ) -> (Vec<ClientId>, Vec<ClientId>) {
        let mut disconnected = Vec::new();
        let mut new_publications = Vec::new();
        let mut new_remote_subscriptions: Vec<(
            ClientId,
            ChannelId,
            TypeId,
            RemoteEndpoint,
            ChannelId,
        )> = Vec::new();
        let mut activity_clients = Vec::new();

        for session in self.sessions.values_mut() {
            let mut had_activity = false;

            // Process pending messages
            while let Some(msg) = session.conn.rx.pop() {
                if let Ok(cmd) = ClientCommand::try_from(msg) {
                    had_activity = true;

                    match cmd {
                        ClientCommand::Disconnect => {
                            disconnected.push(session.conn.id);
                            break;
                        }
                        ClientCommand::Heartbeat => {
                            // Activity already tracked
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

            // Track clients with activity (need to reset timeout timer)
            if had_activity {
                activity_clients.push(session.conn.id);
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

        (disconnected, activity_clients)
    }

    /// Handles `OpenTx` command: client wants to send data to driver.
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
            if session.conn.tx.push(DriverMessage::ChannelError(channel)).is_err() {
                warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelError");
            }
            return None;
        }

        let path = data_tx_path(&session.conn.id, channel);
        let queue: ClientTxQueue = match IpcConsumer::open(path) {
            Ok(q) => q,
            Err(_e) => {
                warn!(client = %session.conn.id, channel = %channel, error = ?_e, "OpenTx failed: cannot open queue");
                if session.conn.tx.push(DriverMessage::ChannelError(channel)).is_err() {
                    warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelError");
                }
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
            if session.conn.tx.push(DriverMessage::ChannelError(channel)).is_err() {
                warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelError");
            }
            return None;
        }

        session.channels.insert(channel, ChannelDirection::Inbound);
        if session.conn.tx.push(DriverMessage::ChannelReady(channel)).is_err() {
            warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelReady");
        }

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

    /// Handles `OpenRx` command: client wants to receive data from driver.
    fn handle_open_rx(
        session: &mut Session,
        channel: ChannelId,
        rx_commands: &Producer<RxCommand, COMMAND_QUEUE_CAPACITY>,
    ) {
        debug!(client = %session.conn.id, channel = %channel, "handling OpenRx command");

        if session.channels.contains_key(&channel) {
            warn!(client = %session.conn.id, channel = %channel, "OpenRx failed: channel already exists");
            if session.conn.tx.push(DriverMessage::ChannelError(channel)).is_err() {
                warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelError");
            }
            return;
        }

        let path = data_rx_path(&session.conn.id, channel);
        let queue: ClientRxQueue = match IpcProducer::open(path) {
            Ok(q) => q,
            Err(_e) => {
                warn!(client = %session.conn.id, channel = %channel, error = ?_e, "OpenRx failed: cannot open queue");
                if session.conn.tx.push(DriverMessage::ChannelError(channel)).is_err() {
                    warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelError");
                }
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
            if session.conn.tx.push(DriverMessage::ChannelError(channel)).is_err() {
                warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelError");
            }
            return;
        }

        session.channels.insert(channel, ChannelDirection::Outbound);
        if session.conn.tx.push(DriverMessage::ChannelReady(channel)).is_err() {
            warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelReady");
        }

        info!(client = %session.conn.id, channel = %channel, "RX channel opened");
    }

    /// Handles `CloseChannel` command.
    fn handle_close_channel(
        session: &mut Session,
        channel: ChannelId,
        tx_commands: &Producer<TxCommand, COMMAND_QUEUE_CAPACITY>,
        rx_commands: &Producer<RxCommand, COMMAND_QUEUE_CAPACITY>,
    ) {
        if let Some(direction) = session.channels.remove(&channel) {
            match direction {
                ChannelDirection::Inbound => {
                    if tx_commands.push(TxCommand::RemoveChannel { channel }).is_err() {
                        warn!(channel = %channel, "TX command queue full, dropping RemoveChannel");
                    }
                }
                ChannelDirection::Outbound => {
                    if rx_commands.push(RxCommand::RemoveChannel { channel }).is_err() {
                        warn!(channel = %channel, "RX command queue full, dropping RemoveChannel");
                    }
                }
            }
        }
        if session.conn.tx.push(DriverMessage::ChannelClosed(channel)).is_err() {
            warn!(client = %session.conn.id, channel = %channel, "client queue full, dropping ChannelClosed");
        }
    }

    /// Cleans up a disconnected session.
    fn cleanup_session(&mut self, client_id: ClientId) {
        if let Some(session) = self.sessions.remove(&client_id) {
            // Remove all channels owned by this client
            for (channel, direction) in session.channels {
                match direction {
                    ChannelDirection::Inbound => {
                        if self.tx_commands.push(TxCommand::RemoveChannel { channel }).is_err() {
                            warn!(channel = %channel, "TX command queue full, dropping RemoveChannel");
                        }
                    }
                    ChannelDirection::Outbound => {
                        if self.rx_commands.push(RxCommand::RemoveChannel { channel }).is_err() {
                            warn!(channel = %channel, "RX command queue full, dropping RemoveChannel");
                        }
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

    /// Schedules a session timeout timer for a client.
    fn schedule_timeout(
        &mut self,
        client_id: ClientId,
        wheel: &mut WheelScope<'_, ControlTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            if session.timeout_scheduled {
                return;
            }

            let delay_ms = self.session_timeout.as_millis() as u64;
            let delay = WheelDuration::<Millis>::from_millis(delay_ms);
            if wheel
                .schedule_after(delay, ControlTimerEvent::SessionTimeout(client_id))
                .is_err()
            {
                warn!(client = %client_id, "failed to schedule session timeout (wheel full or delay too long)");
                return;
            }
            session.timeout_scheduled = true;
        }
    }

    /// Resets the timeout timer for a client with recent activity.
    fn reset_timeout(
        &mut self,
        client_id: ClientId,
        wheel: &mut WheelScope<'_, ControlTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            // Clear the scheduled flag so we can reschedule
            session.timeout_scheduled = false;
        }
        self.schedule_timeout(client_id, wheel);
    }

    /// Fires due session timeout timers.
    fn tick_timers(
        &mut self,
        wheel: &mut WheelScope<'_, ControlTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        // Collect fired timers
        let mut timed_out = Vec::new();
        wheel.tick_now(|_handle, event| match event {
            ControlTimerEvent::SessionTimeout(client_id) => {
                timed_out.push(client_id);
            }
        });

        // Process timeouts
        for client_id in timed_out {
            self.handle_timeout(client_id);
        }
    }

    /// Handles a session timeout - disconnects the client.
    fn handle_timeout(&mut self, client_id: ClientId) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            warn!(client = %client_id, "session timeout, disconnecting");
            session.timeout_scheduled = false;

            // Notify client of disconnect (use Shutdown as there's no Disconnected variant)
            if session.conn.tx.push(DriverMessage::Shutdown).is_err() {
                warn!(client = %client_id, "client queue full, dropping Shutdown");
            }
        }

        // Clean up the session
        self.cleanup_session(client_id);
    }
}
