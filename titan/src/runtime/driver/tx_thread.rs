//! Driver TX thread runtime.
//!
//! Responsibilities:
//! - Maintain local snapshot of channel → endpoint fan-out and sequence numbers.
//! - Drain client→driver SPSC queues and send to network endpoints.
//! - Apply control commands to update routing table.
//! - Manage remote subscribers and their flow control state.
//! - Send protocol frames (SETUP_ACK, SETUP_NAK, etc.) on behalf of control thread.
//! - Process RX→TX events for reliability feedback.

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

/// TX thread state and event loop.
pub struct TxThread {
    /// UDP socket for sending (shared with RX thread).
    socket: Arc<UdpSocket>,
    /// Command queue from control thread.
    commands: Consumer<TxCommand, COMMAND_QUEUE_CAPACITY>,
    /// Event queue from RX thread (for reliability feedback).
    rx_events: Consumer<RxToTxEvent, COMMAND_QUEUE_CAPACITY>,
    /// Per-channel routing state.
    channels: HashMap<ChannelId, ChannelState>,
    /// Reusable buffer for encoding messages.
    encode_buf: Vec<u8>,
    /// Maximum messages to drain per channel per tick.
    batch_size: usize,
    /// Index to rotate starting channel for fairness.
    round_robin_offset: usize,
}

impl TxThread {
    /// Creates a new TX thread state.
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
        Self {
            socket,
            commands,
            rx_events,
            channels: HashMap::new(),
            encode_buf: Vec::with_capacity(2048),
            batch_size: 16,
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
                    client,
                } => {
                    info!(
                        channel = %channel,
                        client = %client,
                        endpoints = ?endpoints,
                        "TX: adding channel"
                    );
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
                    let _ = self.socket.try_send_to(&frame_bytes, endpoint);
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
    fn drain_and_send(&mut self) {
        if self.channels.is_empty() {
            return;
        }

        // Get channel IDs for iteration (avoid borrow issues)
        let channel_ids: Vec<_> = self.channels.keys().copied().collect();
        let num_channels = channel_ids.len();

        // Rotate starting point for fairness
        for i in 0..num_channels {
            let idx = (self.round_robin_offset + i) % num_channels;
            let channel_id = channel_ids[idx];

            self.drain_channel(channel_id);
        }

        self.round_robin_offset = (self.round_robin_offset + 1) % num_channels.max(1);
    }

    /// Drains a single channel up to batch_size messages.
    fn drain_channel(&mut self, channel_id: ChannelId) {
        let batch_size = self.batch_size;

        for _ in 0..batch_size {
            // Get mutable reference to channel state
            let state = match self.channels.get_mut(&channel_id) {
                Some(s) => s,
                None => return,
            };

            // Pop frame from client queue
            let frame = match state.queue.pop() {
                Some(f) => f,
                None => return,
            };

            // Assign sequence number and timestamp
            let seq = state.next_seq;
            state.next_seq = seq.next();
            let timestamp = Micros::now();

            // Collect all endpoints: static endpoints + dynamic subscribers
            let mut all_endpoints: Vec<_> = state.endpoints.iter().copied().collect();
            for sub in state.subscribers.values() {
                all_endpoints.push(sub.endpoint);
            }

            trace!(
                channel = %channel_id,
                seq = %seq,
                endpoints_count = all_endpoints.len(),
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
            for endpoint in &all_endpoints {
                // Best-effort send - ignore errors
                let _ = self.socket.try_send_to(&self.encode_buf, *endpoint);
            }
        }
    }
}
