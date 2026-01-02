//! Driver RX thread runtime.
//!
//! Responsibilities:
//! - Maintain local snapshot of channel → client RX queue mapping.
//! - Receive UDP packets and demux to appropriate client queues.
//! - Apply control commands to update demux table.
//! - Forward reliability events to TX thread.
//! - Parse protocol frames (SETUP, SM, TEARDOWN) and forward to control thread.

use std::collections::HashMap;
use std::sync::Arc;

use crate::control::types::ChannelId;
use crate::data::Frame;
use crate::data::transport::{DataPlaneMessage, decode_message};
use crate::net::{Endpoint, UdpSocket};
use crate::sync::spsc::{Consumer, Producer};
use crate::trace::{debug, info, trace, warn};

use super::commands::{
    COMMAND_QUEUE_CAPACITY, ClientRxQueue, DEFAULT_FRAME_CAP, RemoteStreamKey, RxCommand,
    RxToControlEvent, RxToTxEvent,
};
use super::protocol::{FrameKind, ProtocolFrame, decode_frame, frame_type};

/// Maximum UDP datagram size we'll receive.
const MAX_DATAGRAM_SIZE: usize = 65535;

/// Per-channel state maintained by the RX thread.
struct ChannelState {
    /// Queue to push inbound messages to (driver → client).
    queue: ClientRxQueue,
    /// Expected next sequence number (for gap detection).
    expected_seq: u64,
}

/// RX thread state and event loop.
pub struct RxThread {
    /// UDP socket for receiving (shared with TX thread).
    socket: Arc<UdpSocket>,
    /// Command queue from control thread.
    commands: Consumer<RxCommand, COMMAND_QUEUE_CAPACITY>,
    /// Event queue to TX thread (for reliability feedback).
    tx_events: Producer<RxToTxEvent, COMMAND_QUEUE_CAPACITY>,
    /// Event queue to control thread (for protocol frames).
    control_events: Producer<RxToControlEvent, COMMAND_QUEUE_CAPACITY>,
    /// Per-channel demux state (for local channels).
    channels: HashMap<ChannelId, ChannelState>,
    /// Remote stream to local channel mappings.
    /// Maps (`remote_endpoint`, `remote_channel`) → `local_channel`.
    remote_mappings: HashMap<RemoteStreamKey, ChannelId>,
    /// Reusable buffer for receiving datagrams.
    recv_buf: Vec<u8>,
}

impl RxThread {
    /// Creates a new RX thread state.
    ///
    /// # Arguments
    ///
    /// * `socket` - Bound UDP socket for receiving (shared with TX thread).
    /// * `commands` - SPSC consumer for control commands.
    /// * `tx_events` - SPSC producer for sending events to TX thread.
    /// * `control_events` - SPSC producer for sending protocol events to control thread.
    pub fn new(
        socket: Arc<UdpSocket>,
        commands: Consumer<RxCommand, COMMAND_QUEUE_CAPACITY>,
        tx_events: Producer<RxToTxEvent, COMMAND_QUEUE_CAPACITY>,
        control_events: Producer<RxToControlEvent, COMMAND_QUEUE_CAPACITY>,
    ) -> Self {
        Self {
            socket,
            commands,
            tx_events,
            control_events,
            channels: HashMap::new(),
            remote_mappings: HashMap::new(),
            recv_buf: vec![0u8; MAX_DATAGRAM_SIZE],
        }
    }

    /// Runs the RX thread event loop.
    ///
    /// Returns when a `Shutdown` command is received.
    pub fn run(&mut self) {
        loop {
            // Process control commands first (cold path)
            if self.process_commands() {
                // Shutdown requested
                return;
            }

            // Receive and process packets (hot path)
            self.receive_and_demux();

            // TODO: Process timing wheel for ack batching, gap detection
        }
    }

    /// Processes pending control commands.
    ///
    /// Returns `true` if shutdown was requested.
    fn process_commands(&mut self) -> bool {
        while let Some(cmd) = self.commands.pop() {
            match cmd {
                RxCommand::AddChannel {
                    channel,
                    queue,
                    client: _client,
                } => {
                    info!(channel = %channel, client = %_client, "RX: adding channel");
                    self.channels.insert(
                        channel,
                        ChannelState {
                            queue,
                            expected_seq: 0,
                        },
                    );
                }
                RxCommand::RemoveChannel { channel } => {
                    info!(channel = %channel, "RX: removing channel");
                    self.channels.remove(&channel);
                }
                RxCommand::AddRemoteMapping {
                    remote,
                    local_channel,
                } => {
                    info!(
                        remote_endpoint = %remote.endpoint,
                        remote_channel = %remote.channel,
                        local_channel = %local_channel,
                        "RX: adding remote mapping"
                    );
                    self.remote_mappings.insert(remote, local_channel);
                }
                RxCommand::RemoveRemoteMapping { remote } => {
                    info!(
                        remote_endpoint = %remote.endpoint,
                        remote_channel = %remote.channel,
                        "RX: removing remote mapping"
                    );
                    self.remote_mappings.remove(&remote);
                }
                RxCommand::Shutdown => {
                    info!("RX: shutdown command received");
                    return true;
                }
            }
        }
        false
    }

    /// Receives packets from the socket and demuxes to client queues.
    fn receive_and_demux(&mut self) {
        // Try to receive a packet (non-blocking)
        let Ok(Some((len, from))) = self.socket.try_recv_from(&mut self.recv_buf) else {
            return; // No data or I/O error
        };

        if len == 0 {
            return;
        }

        // Classify frame type from first byte - produces typed evidence
        match frame_type::classify(self.recv_buf[0]) {
            FrameKind::Protocol(_) => self.handle_protocol_frame(len, from),
            FrameKind::Data(_) => self.handle_data_frame(len, from),
        }
    }

    /// Handles an incoming protocol frame.
    fn handle_protocol_frame(&self, len: usize, from: Endpoint) {
        let frame = match decode_frame(&self.recv_buf[..len]) {
            Ok(f) => f,
            Err(_e) => {
                warn!(from = %from, error = ?_e, "RX: malformed protocol frame");
                return;
            }
        };

        match frame {
            ProtocolFrame::Setup(setup) => {
                debug!(
                    from = %from,
                    session = %setup.session,
                    channel = %setup.channel,
                    type_id = %setup.type_id,
                    "RX: received SETUP"
                );
                let event = RxToControlEvent::SetupRequest {
                    from,
                    session: setup.session,
                    channel: setup.channel,
                    type_id: setup.type_id,
                    receiver_window: setup.receiver_window.as_u32(),
                    mtu: setup.mtu.as_u16(),
                };
                if self.control_events.push(event).is_err() {
                    warn!(from = %from, "control event queue full, dropping SETUP");
                }
            }
            ProtocolFrame::SetupAck(ack) => {
                debug!(
                    from = %from,
                    session = %ack.session,
                    publisher_session = %ack.publisher_session,
                    "RX: received SETUP_ACK"
                );
                let event = RxToControlEvent::SetupAck {
                    from,
                    session: ack.session,
                    publisher_session: ack.publisher_session,
                    mtu: ack.mtu.as_u16(),
                };
                if self.control_events.push(event).is_err() {
                    warn!(from = %from, "control event queue full, dropping SETUP_ACK");
                }
            }
            ProtocolFrame::SetupNak(nak) => {
                debug!(
                    from = %from,
                    session = %nak.session,
                    reason = ?nak.reason,
                    "RX: received SETUP_NAK"
                );
                let event = RxToControlEvent::SetupNak {
                    from,
                    session: nak.session,
                    reason: nak.reason,
                };
                if self.control_events.push(event).is_err() {
                    warn!(from = %from, "control event queue full, dropping SETUP_NAK");
                }
            }
            ProtocolFrame::StatusMessage(sm) => {
                trace!(
                    session = %sm.session,
                    consumption_offset = sm.consumption_offset.as_u64(),
                    receiver_window = sm.receiver_window.as_u32(),
                    "RX: received StatusMessage"
                );
                // Status messages go to TX thread for flow control
                let event = RxToTxEvent::StatusMessage {
                    session: sm.session,
                    consumption_offset: sm.consumption_offset.as_u64(),
                    receiver_window: sm.receiver_window.as_u32(),
                };
                if self.tx_events.push(event).is_err() {
                    warn!(session = %sm.session, "TX event queue full, dropping StatusMessage");
                }
            }
            ProtocolFrame::Teardown(teardown) => {
                debug!(from = %from, session = %teardown.session, "RX: received TEARDOWN");
                let event = RxToControlEvent::Teardown {
                    from,
                    session: teardown.session,
                };
                if self.control_events.push(event).is_err() {
                    warn!(from = %from, "control event queue full, dropping TEARDOWN");
                }
            }
        }
    }

    /// Handles an incoming data plane frame.
    fn handle_data_frame(&mut self, len: usize, from: Endpoint) {
        // Decode the message
        let msg: DataPlaneMessage<DEFAULT_FRAME_CAP> = match decode_message(&self.recv_buf[..len]) {
            Ok(m) => m,
            Err(_) => return, // Malformed packet, skip
        };

        self.handle_message(&msg, from);
    }

    /// Handles a decoded data plane message.
    fn handle_message(&mut self, msg: &DataPlaneMessage<DEFAULT_FRAME_CAP>, from: Endpoint) {
        match *msg {
            DataPlaneMessage::Data {
                channel,
                seq,
                frame,
                ..
            } => {
                self.handle_data(channel, seq, &frame, from);
            }
            DataPlaneMessage::Ack { channel, seq, .. } => {
                // Forward to TX thread
                if self.tx_events.push(RxToTxEvent::Ack {
                    channel,
                    seq: seq.into(),
                }).is_err() {
                    warn!(channel = %channel, "TX event queue full, dropping ACK");
                }
            }
            DataPlaneMessage::Nack { channel, seq } => {
                // Forward to TX thread
                if self.tx_events.push(RxToTxEvent::Nack {
                    channel,
                    seq: seq.into(),
                }).is_err() {
                    warn!(channel = %channel, "TX event queue full, dropping NACK");
                }
            }
            DataPlaneMessage::Heartbeat {
                channel,
                next_expected,
                ..
            } => {
                // Forward to TX thread
                if self.tx_events.push(RxToTxEvent::Heartbeat {
                    channel,
                    next_expected: next_expected.into(),
                }).is_err() {
                    warn!(channel = %channel, "TX event queue full, dropping Heartbeat");
                }
            }
        }
    }

    /// Handles an inbound data message.
    fn handle_data(
        &mut self,
        channel: ChannelId,
        seq: crate::data::types::SeqNum,
        frame: &Frame<DEFAULT_FRAME_CAP>,
        from: Endpoint,
    ) {
        // First try direct local channel lookup
        let local_channel = if self.channels.contains_key(&channel) {
            channel
        } else {
            // Try remote mapping: (from, remote_channel) → local_channel
            let key = RemoteStreamKey {
                endpoint: from,
                channel,
            };
            match self.remote_mappings.get(&key) {
                Some(&local) => local,
                None => {
                    trace!(from = %from, channel = %channel, "RX: DATA for unknown channel, dropping");
                    return;
                }
            }
        };

        let Some(state) = self.channels.get_mut(&local_channel) else {
            warn!(local_channel = %local_channel, "RX: local channel not found, dropping");
            return;
        };

        trace!(
            from = %from,
            channel = %channel,
            local_channel = %local_channel,
            seq = %seq,
            frame_len = frame.len(),
            "RX: received DATA"
        );

        // Best-effort mode: accept all packets, update expected_seq
        // Future: detect gaps, generate nacks
        let seq_val: u64 = seq.into();
        if seq_val >= state.expected_seq {
            state.expected_seq = seq_val + 1;
        }

        // Push frame to client's RX queue
        // Best-effort: drop if queue is full
        if state.queue.push(*frame).is_err() {
            warn!(local_channel = %local_channel, seq = %seq, "RX: client queue full, dropping frame");
        }
    }
}
