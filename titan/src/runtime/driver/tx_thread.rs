//! Driver TX thread runtime.
//!
//! Responsibilities:
//! - Maintain local snapshot of channel → endpoint fan-out and sequence numbers.
//! - Drain client→driver SPSC queues and send to network endpoints.
//! - Apply control commands to update routing table.
//! - Manage remote subscribers and their flow control state.
//! - Send protocol frames (`SETUP_ACK`, `SETUP_NAK`, etc.) on behalf of control thread.
//! - Process RX→TX events for reliability feedback.
//!
//! # Scheduling
//!
//! The TX loop uses a fair scheduling algorithm:
//! - **Round-robin rotation**: Starting channel index rotates each tick to prevent starvation.
//! - **Per-channel batching**: Drains up to `batch_size` messages per channel to exploit
//!   SPSC cache locality before moving to the next channel.
//! - **Global work budget**: Caps total messages per tick (`max_messages_per_tick`) to
//!   bound worst-case latency.
//!
//! Channel ordering is stable (insertion order preserved) to ensure deterministic
//! round-robin behavior across channel add/remove operations.

use std::collections::HashMap;
use std::sync::Arc;

use crate::control::types::ChannelId;
use crate::data::transport::{DataPlaneMessage, encode_message};
use crate::data::types::SeqNum;
use crate::net::{Endpoint, UdpSocket};
use crate::runtime::timing::{Micros, Now};
use crate::sync::spsc::Consumer;
use crate::trace::{debug, info, trace, warn};

use super::commands::{COMMAND_QUEUE_CAPACITY, DEFAULT_FRAME_CAP, RxToTxEvent, TxCommand};
use super::protocol::SessionId;

/// Default maximum messages to drain per channel per tick.
const DEFAULT_BATCH_SIZE: usize = 16;

/// Default maximum total messages to send per tick (bounds latency).
const DEFAULT_MAX_MESSAGES_PER_TICK: usize = 256;

/// Per-channel state maintained by the TX thread.
struct ChannelState {
    /// Queue to drain for outbound messages.
    queue: super::commands::ClientTxQueue,
    /// Network endpoints to send to (legacy static configuration).
    endpoints: Vec<Endpoint>,
    /// Remote subscribers with flow control state.
    subscribers: HashMap<SessionId, SubscriberState>,
    /// Next sequence number to assign.
    next_seq: SeqNum,
}

/// Per-subscriber flow control state.
struct SubscriberState {
    /// Subscriber's endpoint.
    endpoint: Endpoint,
    /// Last known consumption offset from subscriber.
    consumption_offset: u64,
    /// Subscriber's advertised receiver window in bytes.
    receiver_window: u32,
}

/// Configuration for the TX thread.
///
/// Invariant: All values > 0.
#[derive(Debug, Clone, Copy)]
pub struct TxConfig {
    /// Maximum messages to drain per channel per tick.
    /// Exploits SPSC cache locality by batching reads from a single queue.
    batch_size: usize,
    /// Maximum total messages to send per tick across all channels.
    /// Bounds worst-case latency by limiting work per event loop iteration.
    max_messages_per_tick: usize,
}

impl TxConfig {
    /// Creates a new TX configuration.
    ///
    /// # Panics
    /// Panics if any value is 0.
    #[must_use]
    pub fn new(batch_size: usize, max_messages_per_tick: usize) -> Self {
        assert!(batch_size > 0, "batch_size must be > 0");
        assert!(max_messages_per_tick > 0, "max_messages_per_tick must be > 0");
        Self { batch_size, max_messages_per_tick }
    }

    #[must_use]
    pub const fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[must_use]
    pub const fn max_messages_per_tick(&self) -> usize {
        self.max_messages_per_tick
    }
}

impl Default for TxConfig {
    fn default() -> Self {
        Self::new(DEFAULT_BATCH_SIZE, DEFAULT_MAX_MESSAGES_PER_TICK)
    }
}

/// TX thread state and event loop.
pub struct TxThread {
    /// UDP socket for sending (shared with RX thread).
    socket: Arc<UdpSocket>,
    /// Command queue from control thread.
    commands: Consumer<TxCommand, COMMAND_QUEUE_CAPACITY>,
    /// Event queue from RX thread (for reliability feedback).
    rx_events: Consumer<RxToTxEvent, COMMAND_QUEUE_CAPACITY>,
    /// Per-channel routing state (keyed for O(1) lookup).
    channels: HashMap<ChannelId, ChannelState>,
    /// Stable-order list of channel IDs for fair round-robin scheduling.
    /// Maintained separately from `HashMap` to ensure deterministic iteration order.
    channel_order: Vec<ChannelId>,
    /// Reusable buffer for encoding messages.
    encode_buf: Vec<u8>,
    /// Reusable buffer for collecting endpoints (avoids per-message allocation).
    endpoint_buf: Vec<Endpoint>,
    /// Maximum messages to drain per channel per tick.
    batch_size: usize,
    /// Maximum total messages to send per tick.
    max_messages_per_tick: usize,
    /// Index to rotate starting channel for fairness.
    round_robin_offset: usize,
}

impl TxThread {
    /// Creates a new TX thread state with default configuration.
    ///
    /// # Arguments
    ///
    /// * `socket` - Bound UDP socket for sending (shared with RX thread).
    /// * `commands` - SPSC consumer for control commands.
    /// * `rx_events` - SPSC consumer for RX thread events.
    pub fn new(
        socket: Arc<UdpSocket>,
        commands: Consumer<TxCommand, COMMAND_QUEUE_CAPACITY>,
        rx_events: Consumer<RxToTxEvent, COMMAND_QUEUE_CAPACITY>,
    ) -> Self {
        Self::with_config(socket, commands, rx_events, TxConfig::default())
    }

    /// Creates a new TX thread state with custom configuration.
    pub fn with_config(
        socket: Arc<UdpSocket>,
        commands: Consumer<TxCommand, COMMAND_QUEUE_CAPACITY>,
        rx_events: Consumer<RxToTxEvent, COMMAND_QUEUE_CAPACITY>,
        config: TxConfig,
    ) -> Self {
        Self {
            socket,
            commands,
            rx_events,
            channels: HashMap::new(),
            channel_order: Vec::new(),
            encode_buf: Vec::with_capacity(2048),
            endpoint_buf: Vec::with_capacity(16),
            batch_size: config.batch_size,
            max_messages_per_tick: config.max_messages_per_tick,
            round_robin_offset: 0,
        }
    }

    /// Runs the TX thread event loop.
    ///
    /// Returns when a `Shutdown` command is received.
    pub fn run(&mut self) {
        loop {
            // Process control commands first (cold path)
            if self.process_commands() {
                // Shutdown requested
                return;
            }

            // Process RX events (reliability feedback)
            self.process_rx_events();

            // Drain client queues and send to network (hot path)
            self.drain_and_send();

            // TODO: Process timing wheel for retransmissions
        }
    }

    /// Processes pending control commands.
    ///
    /// Returns `true` if shutdown was requested.
    fn process_commands(&mut self) -> bool {
        while let Some(cmd) = self.commands.pop() {
            match cmd {
                TxCommand::AddChannel {
                    channel,
                    queue,
                    endpoints,
                    client: _client,
                } => {
                    info!(
                        channel = %channel,
                        client = %_client,
                        endpoints = ?endpoints,
                        "TX: adding channel"
                    );
                    // Only add to order list if not already present (idempotent)
                    if !self.channels.contains_key(&channel) {
                        self.channel_order.push(channel);
                    }
                    self.channels.insert(
                        channel,
                        ChannelState {
                            queue,
                            endpoints,
                            subscribers: HashMap::new(),
                            next_seq: SeqNum::ZERO,
                        },
                    );
                }
                TxCommand::RemoveChannel { channel } => {
                    info!(channel = %channel, "TX: removing channel");
                    self.channels.remove(&channel);
                    // Remove from order list (maintains relative order of remaining channels)
                    self.channel_order.retain(|&id| id != channel);
                    // Adjust round_robin_offset if it now points past the end
                    if self.channel_order.is_empty() {
                        self.round_robin_offset = 0;
                    } else {
                        self.round_robin_offset %= self.channel_order.len();
                    }
                }
                TxCommand::UpdateEndpoints { channel, endpoints } => {
                    debug!(channel = %channel, endpoints = ?endpoints, "TX: updating endpoints");
                    if let Some(state) = self.channels.get_mut(&channel) {
                        state.endpoints = endpoints;
                    }
                }
                TxCommand::AddSubscriber {
                    channel,
                    session,
                    endpoint,
                    receiver_window,
                } => {
                    info!(
                        channel = %channel,
                        session = %session,
                        endpoint = %endpoint,
                        receiver_window = receiver_window,
                        "TX: adding subscriber"
                    );
                    if let Some(state) = self.channels.get_mut(&channel) {
                        state.subscribers.insert(
                            session,
                            SubscriberState {
                                endpoint,
                                consumption_offset: 0,
                                receiver_window,
                            },
                        );
                    }
                }
                TxCommand::RemoveSubscriber { channel, session } => {
                    info!(channel = %channel, session = %session, "TX: removing subscriber");
                    if let Some(state) = self.channels.get_mut(&channel) {
                        state.subscribers.remove(&session);
                    }
                }
                TxCommand::SendProtocolFrame {
                    endpoint,
                    frame_bytes,
                } => {
                    debug!(
                        endpoint = %endpoint,
                        len = frame_bytes.len(),
                        "TX: sending protocol frame"
                    );
                    // Best-effort send of protocol frame
                    if self.socket.try_send_to(&frame_bytes, endpoint).is_err() {
                        trace!(endpoint = %endpoint, "send would block or failed");
                    }
                }
                TxCommand::Shutdown => {
                    info!("TX: shutdown command received");
                    return true;
                }
            }
        }
        false
    }

    /// Processes pending RX→TX events.
    fn process_rx_events(&mut self) {
        while let Some(event) = self.rx_events.pop() {
            match event {
                RxToTxEvent::Ack { channel, seq } => {
                    // TODO: Update ack state, remove from retransmit queue
                    let _ = (channel, seq);
                }
                RxToTxEvent::Nack { channel, seq } => {
                    // TODO: Trigger retransmission
                    let _ = (channel, seq);
                }
                RxToTxEvent::Heartbeat {
                    channel,
                    next_expected,
                } => {
                    // TODO: Update peer state
                    let _ = (channel, next_expected);
                }
                RxToTxEvent::StatusMessage {
                    session,
                    consumption_offset,
                    receiver_window,
                } => {
                    // Update subscriber's flow control state
                    // We need to find the subscriber by session ID across all channels
                    for state in self.channels.values_mut() {
                        if let Some(sub) = state.subscribers.get_mut(&session) {
                            sub.consumption_offset = consumption_offset;
                            sub.receiver_window = receiver_window;
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Drains client queues and sends to network endpoints.
    ///
    /// Uses round-robin scheduling with per-channel batching:
    /// - Rotates starting channel each tick for fairness
    /// - Drains up to `batch_size` messages per channel
    /// - Stops after `max_messages_per_tick` total messages
    fn drain_and_send(&mut self) {
        let num_channels = self.channel_order.len();
        if num_channels == 0 {
            return;
        }

        let mut budget = self.max_messages_per_tick;

        // Rotate through channels starting from round_robin_offset
        for i in 0..num_channels {
            if budget == 0 {
                break;
            }

            let idx = (self.round_robin_offset + i) % num_channels;
            let channel_id = self.channel_order[idx];

            let sent = self.drain_channel(channel_id, budget);
            budget = budget.saturating_sub(sent);
        }

        // Advance round-robin offset for next tick
        self.round_robin_offset = (self.round_robin_offset + 1) % num_channels;
    }

    /// Drains a single channel up to `limit` messages (capped by `batch_size`).
    ///
    /// Returns the number of messages actually sent.
    fn drain_channel(&mut self, channel_id: ChannelId, limit: usize) -> usize {
        let max_to_send = self.batch_size.min(limit);
        let mut sent = 0;

        for _ in 0..max_to_send {
            // Get mutable reference to channel state
            let Some(state) = self.channels.get_mut(&channel_id) else {
                return sent;
            };

            // Pop frame from client queue
            let Some(frame) = state.queue.pop() else {
                return sent;
            };

            // Assign sequence number and timestamp
            let seq = state.next_seq;
            state.next_seq = seq.next();
            let timestamp = Micros::now();

            // Collect all endpoints into reusable buffer
            self.endpoint_buf.clear();
            self.endpoint_buf.extend(state.endpoints.iter().copied());
            for sub in state.subscribers.values() {
                self.endpoint_buf.push(sub.endpoint);
            }

            trace!(
                channel = %channel_id,
                seq = %seq,
                endpoints_count = self.endpoint_buf.len(),
                frame_len = frame.len(),
                "TX: sending DATA"
            );

            // Build data plane message
            let msg: DataPlaneMessage<DEFAULT_FRAME_CAP> = DataPlaneMessage::Data {
                channel: channel_id,
                seq,
                send_timestamp: timestamp,
                frame,
            };

            // Encode message
            if encode_message(&msg, &mut self.encode_buf).is_err() {
                warn!(channel = %channel_id, seq = %seq, "TX: failed to encode message");
                continue;
            }

            // Fan-out to all endpoints (static + dynamic subscribers)
            for endpoint in &self.endpoint_buf {
                // Best-effort send - transient failures retried at higher layer
                if self.socket.try_send_to(&self.encode_buf, *endpoint).is_err() {
                    trace!(endpoint = %endpoint, "send would block or failed");
                }
            }

            sent += 1;
        }

        sent
    }
}
