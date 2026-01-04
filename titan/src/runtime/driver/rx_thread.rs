//! Driver RX thread runtime.
//!
//! Responsibilities:
//! - Maintain local snapshot of channel → client RX queue mapping.
//! - Receive UDP packets and demux to appropriate client queues.
//! - Apply control commands to update demux table.
//! - Forward reliability events to TX thread.
//! - Parse protocol frames (SETUP, SM, TEARDOWN) and forward to control thread.
//!
//! On Linux, the RX thread uses `recvmmsg` to batch multiple UDP receives into a single
//! syscall, dramatically reducing syscall overhead and improving throughput.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
#[cfg(target_os = "linux")]
use std::os::fd::{AsFd, AsRawFd};
use std::sync::Arc;

use crate::control::types::ChannelId;
use crate::data::packing::UnpackingIter;
use crate::data::types::SeqNum;
use crate::data::Frame;
use crate::data::transport::{DataPlaneMessage, decode_message};
use crate::net::{Endpoint, UdpSocket};
use crate::runtime::timing::{Duration, Millis, NonZeroDuration, WheelScope, with_wheel};
use crate::sync::spsc::{Consumer, Producer};
use crate::trace::{debug, info, trace, warn};

use super::commands::{
    COMMAND_QUEUE_CAPACITY, ClientRxQueue, DEFAULT_FRAME_CAP, RemoteStreamKey, RxCommand,
    RxToControlEvent, RxToTxEvent,
};
use super::protocol::{
    ConsumptionOffset, FrameKind, NakBitmapEntry, NakBitmapFrame, ProtocolFrame, ReceiverWindow,
    SessionId, StatusMessageFrame, decode_frame, encode_nak_bitmap, encode_status_message,
    frame_type,
};
use super::reassembly::{
    InsertResult as ReassemblyInsertResult, ReassemblyRing, DEFAULT_MAX_FRAG_PAYLOAD,
    DEFAULT_REASSEMBLY_SLOTS, MAX_FRAGS_PER_MSG,
};
use super::reorder::{InsertResult, ReorderBuffer};
use super::timing::TimingConfig;

/// Maximum UDP datagram size we'll receive.
const MAX_DATAGRAM_SIZE: usize = 65535;

/// Maximum number of messages to batch for recvmmsg (Linux only).
/// Balance between reducing syscall overhead and memory usage.
#[cfg(target_os = "linux")]
// CONTRACT_031: Increased to 64 for better syscall amortization.
// RecvBatch is heap-allocated as RxThread member, so no stack overflow risk.
// With packing: 64 packets * ~140 frames/packet = ~8960 frames per syscall.
const RECVMMSG_BATCH_SIZE: usize = 64;

/// Maximum wire frame size (MTU) for pre-allocated buffers.
#[cfg(target_os = "linux")]
// CONTRACT_027: Increased to support MTU packing (8KB packets)
const MAX_WIRE_FRAME_SIZE: usize = 8192;

/// Fallback batch size for non-Linux platforms (simple loop).
#[cfg(not(target_os = "linux"))]
const RECV_BATCH_SIZE: usize = 64;

/// Reorder buffer capacity per channel.
/// Reorder buffer size - must be large enough to handle burst traffic.
/// Matches TX thread's DEFAULT_MAX_MESSAGES_PER_TICK for consistency.
// CONTRACT_027: Increased temporarily for debugging
const REORDER_BUFFER_SIZE: usize = 65536;

/// Default receiver window size (64KB).
const DEFAULT_RECEIVER_WINDOW: u32 = 64 * 1024;

/// Timing wheel slots (covers ~255ms max delay at 1ms tick).
const WHEEL_SLOTS: usize = 256;

/// Timing wheel capacity (max concurrent timers).
const WHEEL_CAPACITY: usize = 1024;

/// How often to tick the timing wheel (every N loop iterations).
/// Wheel has 1ms resolution. With loop running at ~50M iter/s, tick every ~50K iterations.
/// 32768 (2^15) gives ~0.6ms jitter, well within 1ms resolution.
const WHEEL_TICK_DIVISOR: usize = 32768;

/// Pre-allocated batch storage for recvmmsg (Linux only).
/// Eliminates per-message heap allocations in the hot path.
///
/// # Safety
/// This struct must not be moved after `init()` is called, as the internal
/// pointers reference other fields within the struct.
#[cfg(target_os = "linux")]
struct RecvBatch {
    /// Pre-allocated data buffers (one per batch slot).
    data: [[u8; MAX_WIRE_FRAME_SIZE]; RECVMMSG_BATCH_SIZE],
    /// iovec array pointing to data buffers.
    iovecs: [libc::iovec; RECVMMSG_BATCH_SIZE],
    /// mmsghdr array for recvmmsg.
    msgs: [libc::mmsghdr; RECVMMSG_BATCH_SIZE],
    /// Source address storage for each message.
    addrs: [libc::sockaddr_storage; RECVMMSG_BATCH_SIZE],
    /// Whether init() has been called.
    initialized: bool,
}

#[cfg(target_os = "linux")]
impl RecvBatch {
    fn new() -> Self {
        // Safety: zero-initialized structs are valid for libc types
        unsafe {
            Self {
                data: [[0u8; MAX_WIRE_FRAME_SIZE]; RECVMMSG_BATCH_SIZE],
                iovecs: std::mem::zeroed(),
                msgs: std::mem::zeroed(),
                addrs: std::mem::zeroed(),
                initialized: false,
            }
        }
    }

    /// Initializes pointers. Must be called once after the struct is in its final location.
    /// After this, the struct must not be moved.
    #[inline]
    fn init(&mut self) {
        if self.initialized {
            return;
        }
        for i in 0..RECVMMSG_BATCH_SIZE {
            // Set up iovec to point to data buffer
            self.iovecs[i].iov_base = self.data[i].as_mut_ptr().cast();
            self.iovecs[i].iov_len = MAX_WIRE_FRAME_SIZE;

            // Set up mmsghdr
            self.msgs[i].msg_hdr.msg_iov = std::ptr::addr_of_mut!(self.iovecs[i]);
            self.msgs[i].msg_hdr.msg_iovlen = 1;
            self.msgs[i].msg_hdr.msg_name =
                std::ptr::addr_of_mut!(self.addrs[i]).cast();
            self.msgs[i].msg_hdr.msg_namelen =
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        }
        self.initialized = true;
    }

    /// Resets message lengths before recvmmsg call.
    #[inline]
    fn reset(&mut self) {
        for i in 0..RECVMMSG_BATCH_SIZE {
            self.msgs[i].msg_hdr.msg_namelen =
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            self.msgs[i].msg_len = 0;
        }
    }

    /// Returns the received data and source address for message at index.
    #[inline]
    fn get_message(&self, idx: usize) -> (&[u8], Option<SocketAddr>) {
        let len = self.msgs[idx].msg_len as usize;
        let data = &self.data[idx][..len];
        let addr = sockaddr_storage_to_socket_addr(&self.addrs[idx]);
        (data, addr)
    }
}

/// Converts a sockaddr_storage to a SocketAddr.
#[cfg(target_os = "linux")]
fn sockaddr_storage_to_socket_addr(storage: &libc::sockaddr_storage) -> Option<SocketAddr> {
    // Safety: We check the family before interpreting the union
    unsafe {
        match storage.ss_family as libc::c_int {
            libc::AF_INET => {
                let addr: &libc::sockaddr_in = &*(storage as *const _ as *const libc::sockaddr_in);
                let ip = std::net::Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr));
                let port = u16::from_be(addr.sin_port);
                Some(SocketAddr::from((ip, port)))
            }
            libc::AF_INET6 => {
                let addr: &libc::sockaddr_in6 =
                    &*(storage as *const _ as *const libc::sockaddr_in6);
                let ip = std::net::Ipv6Addr::from(addr.sin6_addr.s6_addr);
                let port = u16::from_be(addr.sin6_port);
                Some(SocketAddr::from((ip, port)))
            }
            _ => None,
        }
    }
}

/// Timer event types for RX thread.
#[derive(Debug, Clone, Copy)]
enum RxTimerEvent {
    /// Periodic status message for flow control.
    StatusMsg(ChannelId),
    /// NAK timer (send NAK if gaps still exist after delay).
    Nak(ChannelId),
}

/// Type alias for the reassembly ring with default parameters.
type ChannelReassemblyRing =
    ReassemblyRing<DEFAULT_MAX_FRAG_PAYLOAD, MAX_FRAGS_PER_MSG, DEFAULT_REASSEMBLY_SLOTS>;

/// Per-channel state maintained by the RX thread.
struct ChannelState {
    /// Queue to push inbound messages to (driver → client).
    queue: ClientRxQueue,
    /// Reorder buffer for reassembling out-of-order packets.
    reorder: ReorderBuffer<Frame<DEFAULT_FRAME_CAP>, REORDER_BUFFER_SIZE>,
    /// Fragment reassembly buffer.
    reassembly: ChannelReassemblyRing,
    /// Publisher session ID (for STATUS_MSG and NAK).
    publisher_session: Option<SessionId>,
    /// Publisher endpoint (for sending STATUS_MSG and NAK).
    publisher_endpoint: Option<Endpoint>,
    /// Whether a STATUS_MSG timer is currently scheduled.
    status_scheduled: bool,
    /// Whether a NAK timer is currently scheduled.
    nak_scheduled: bool,
    /// Consumption offset (highest in-order seq delivered to client).
    consumption_offset: u64,
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
    /// Reusable buffer for receiving datagrams (non-Linux fallback).
    recv_buf: Vec<u8>,
    /// Reusable buffer for encoding outbound protocol frames.
    encode_buf: Vec<u8>,
    /// Pre-allocated buffer for timer events (avoids allocation in hot path).
    timer_buf: Vec<RxTimerEvent>,
    /// Timing configuration for reliability protocol.
    timing: TimingConfig,
    /// Counter for rate-limited timer ticks.
    tick_counter: usize,
    /// CONTRACT_031: Cached last channel to avoid HashMap lookup on every message.
    /// Stores (channel_id, local_channel_id) for fast path when messages arrive
    /// on the same channel consecutively (common in high-throughput scenarios).
    last_channel_cache: Option<(ChannelId, ChannelId)>,
    /// Pre-allocated batch buffer for recvmmsg (Linux only).
    #[cfg(target_os = "linux")]
    recv_batch: RecvBatch,
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
        Self::with_timing(socket, commands, tx_events, control_events, TimingConfig::default())
    }

    /// Creates a new RX thread state with custom timing configuration.
    pub fn with_timing(
        socket: Arc<UdpSocket>,
        commands: Consumer<RxCommand, COMMAND_QUEUE_CAPACITY>,
        tx_events: Producer<RxToTxEvent, COMMAND_QUEUE_CAPACITY>,
        control_events: Producer<RxToControlEvent, COMMAND_QUEUE_CAPACITY>,
        timing: TimingConfig,
    ) -> Self {
        Self {
            socket,
            commands,
            tx_events,
            control_events,
            channels: HashMap::new(),
            remote_mappings: HashMap::new(),
            recv_buf: vec![0u8; MAX_DATAGRAM_SIZE],
            encode_buf: Vec::with_capacity(1024),
            timer_buf: Vec::with_capacity(32), // Pre-allocate for typical timer count
            timing,
            tick_counter: 0,
            last_channel_cache: None,
            #[cfg(target_os = "linux")]
            recv_batch: RecvBatch::new(),
        }
    }

    /// Runs the RX thread event loop.
    ///
    /// Returns when a `Shutdown` command is received.
    pub fn run(&mut self) {
        let tick_duration =
            NonZeroDuration::<Millis>::new(NonZeroU64::new(1).expect("1 != 0"));
        let capacity = NonZeroUsize::new(WHEEL_CAPACITY).expect("WHEEL_CAPACITY != 0");

        with_wheel::<RxTimerEvent, Millis, WHEEL_SLOTS, _>(tick_duration, capacity, |wheel| {
            self.run_with_wheel(wheel);
        });
    }

    /// Inner run loop with timing wheel.
    fn run_with_wheel(
        &mut self,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        loop {
            // Process control commands first (cold path)
            if self.process_commands(wheel) {
                // Shutdown requested
                return;
            }

            // Receive and process packets (hot path)
            self.receive_and_demux(wheel);

            // Fire due timers only every WHEEL_TICK_DIVISOR iterations
            // to reduce time-checking overhead in hot path
            self.tick_counter = self.tick_counter.wrapping_add(1);
            if self.tick_counter % WHEEL_TICK_DIVISOR == 0 {
                self.tick_timers(wheel);
            }
        }
    }

    /// Processes pending control commands.
    ///
    /// Returns `true` if shutdown was requested.
    fn process_commands(
        &mut self,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) -> bool {
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
                            reorder: ReorderBuffer::new(),
                            reassembly: ReassemblyRing::new(1_000_000), // 1 second timeout
                            publisher_session: None,
                            publisher_endpoint: None,
                            status_scheduled: false,
                            nak_scheduled: false,
                            consumption_offset: 0,
                        },
                    );
                    // Schedule initial timers
                    self.schedule_status_msg(channel, wheel);
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
    ///
    /// On Linux, uses `recvmmsg` to batch multiple receives into a single syscall.
    /// On other platforms, falls back to a simple receive loop.
    #[cfg(target_os = "linux")]
    fn receive_and_demux(
        &mut self,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        // Initialize pointers once (safe because RxThread doesn't move after construction)
        self.recv_batch.init();

        // Reset message lengths for new batch
        self.recv_batch.reset();

        // Call recvmmsg with MSG_DONTWAIT for non-blocking behavior
        let fd = self.socket.as_fd().as_raw_fd();
        let received = unsafe {
            libc::recvmmsg(
                fd,
                self.recv_batch.msgs.as_mut_ptr(),
                RECVMMSG_BATCH_SIZE as libc::c_uint,
                libc::MSG_DONTWAIT,
                std::ptr::null_mut(), // No timeout
            )
        };

        if received <= 0 {
            // No data available or error (EAGAIN/EWOULDBLOCK is expected)
            return;
        }

        // Process each received message
        for i in 0..received as usize {
            let len = self.recv_batch.msgs[i].msg_len as usize;
            if len == 0 {
                continue;
            }

            let Some(from) = sockaddr_storage_to_socket_addr(&self.recv_batch.addrs[i]) else {
                continue;
            };
            let from = Endpoint::from(from);

            // Classify frame type from first byte and dispatch
            let first_byte = self.recv_batch.data[i][0];
            match frame_type::classify(first_byte) {
                FrameKind::Protocol(_) => {
                    // Protocol frames: copy to recv_buf for handle_protocol_frame
                    self.recv_buf[..len].copy_from_slice(&self.recv_batch.data[i][..len]);
                    self.handle_protocol_frame(len, from);
                }
                FrameKind::Data(_) => {
                    // CONTRACT_031: Use raw pointer to break borrow conflict without copying.
                    // SAFETY: recv_batch.data[i] is not modified during handle_data_frame.
                    // The slice lifetime is limited to this call, and handle_data_frame
                    // only reads from the slice.
                    let data = unsafe {
                        std::slice::from_raw_parts(self.recv_batch.data[i].as_ptr(), len)
                    };
                    self.handle_data_frame(data, from, wheel);
                }
            }
        }
    }

    /// Fallback implementation for non-Linux platforms.
    #[cfg(not(target_os = "linux"))]
    fn receive_and_demux(
        &mut self,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        // Simple loop-based receive for non-Linux
        for _ in 0..RECV_BATCH_SIZE {
            let Ok(Some((len, from))) = self.socket.try_recv_from(&mut self.recv_buf) else {
                break;
            };

            if len == 0 {
                continue;
            }

            match frame_type::classify(self.recv_buf[0]) {
                FrameKind::Protocol(_) => self.handle_protocol_frame(len, from),
                FrameKind::Data(_) => {
                    // CONTRACT_031: Use raw pointer to break borrow conflict without copying.
                    // SAFETY: recv_buf is not modified during handle_data_frame.
                    let data = unsafe {
                        std::slice::from_raw_parts(self.recv_buf.as_ptr(), len)
                    };
                    self.handle_data_frame(data, from, wheel);
                }
            }
        }
    }

    /// Handles an incoming protocol frame.
    fn handle_protocol_frame(&mut self, len: usize, from: Endpoint) {
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
                    first_seq = ack.first_seq,
                    "RX: received SETUP_ACK"
                );
                let event = RxToControlEvent::SetupAck {
                    from,
                    session: ack.session,
                    publisher_session: ack.publisher_session,
                    first_seq: ack.first_seq,
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
                    consumption_offset: SeqNum::from(sm.consumption_offset.as_u64()),
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
            ProtocolFrame::Heartbeat(hb) => {
                trace!(
                    session = %hb.session,
                    next_seq = hb.next_seq,
                    "RX: received HEARTBEAT"
                );
                // TODO: Use next_seq for gap detection during idle periods
                // For now, just log it. Full implementation in Phase 5.
            }
            ProtocolFrame::NakBitmap(nak) => {
                debug!(
                    session = %nak.session,
                    channel = %nak.channel,
                    entry_count = nak.entries.len(),
                    "RX: received NAK_BITMAP"
                );
                // Forward each missing sequence to TX thread for retransmission
                for entry in &nak.entries {
                    // Iterate over bits: bit=0 means missing
                    for bit in 0..64u64 {
                        if (entry.bitmap & (1u64 << bit)) == 0 {
                            let seq = SeqNum::from(entry.base_seq.wrapping_add(bit));
                            if self.tx_events.push(RxToTxEvent::Nack {
                                channel: nak.channel,
                                seq,
                                from,
                            }).is_err() {
                                warn!(channel = %nak.channel, "TX event queue full, dropping NAK");
                                break;
                            }
                        }
                    }
                }
            }
            ProtocolFrame::DataNotAvail(dna) => {
                debug!(
                    session = %dna.session,
                    channel = %dna.channel,
                    unavail_start = dna.unavail_start,
                    unavail_end = dna.unavail_end,
                    oldest_available = dna.oldest_available,
                    "RX: received DATA_NOT_AVAIL"
                );
                // Accept gap in reorder buffer
                self.handle_data_not_avail(dna.channel, dna.unavail_start, dna.unavail_end);
            }
        }
    }

    /// Handles an incoming data plane frame (possibly packed with multiple frames).
    fn handle_data_frame(
        &mut self,
        packet_data: &[u8],
        from: Endpoint,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        // CONTRACT_030: Single-pass decode+handle using UnpackingIter
        // packet_data is a local copy, so no borrow conflict with self
        for frame_bytes in UnpackingIter::new(packet_data) {
            if let Ok(msg) = decode_message(frame_bytes) {
                self.handle_message(msg, from, wheel);
            }
        }
    }

    /// Handles a decoded data plane message.
    /// CONTRACT_031: Takes message by value to avoid frame copy.
    fn handle_message(
        &mut self,
        msg: DataPlaneMessage<DEFAULT_FRAME_CAP>,
        from: Endpoint,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        match msg {
            DataPlaneMessage::Data {
                channel,
                seq,
                frame,
                ..
            } => {
                self.handle_data(channel, seq, frame, from, wheel);
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
                // Forward to TX thread for retransmission
                if self.tx_events.push(RxToTxEvent::Nack {
                    channel,
                    seq: seq.into(),
                    from,
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
            DataPlaneMessage::DataFrag {
                channel,
                seq,
                frag_index,
                frag_count,
                ref payload,
                ..
            } => {
                self.handle_data_frag(channel, seq.into(), frag_index, frag_count, payload, from, wheel);
            }
        }
    }

    /// Looks up the local channel for an incoming message.
    /// Returns None if the channel is unknown.
    #[inline]
    fn lookup_channel(&self, channel: ChannelId, from: Endpoint) -> Option<ChannelId> {
        // Try direct channel first
        if self.channels.contains_key(&channel) {
            return Some(channel);
        }
        // Try remote mapping: (from, remote_channel) → local_channel
        let key = RemoteStreamKey {
            endpoint: from,
            channel,
        };
        if let Some(&local) = self.remote_mappings.get(&key) {
            return Some(local);
        }
        trace!(from = %from, channel = %channel, "RX: DATA for unknown channel, dropping");
        None
    }

    /// Handles an inbound data message.
    fn handle_data(
        &mut self,
        channel: ChannelId,
        seq: crate::data::types::SeqNum,
        frame: Frame<DEFAULT_FRAME_CAP>,
        from: Endpoint,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        // CONTRACT_031: Fast path using channel cache.
        // In high-throughput scenarios, messages typically arrive on the same channel
        // consecutively, so caching the last lookup avoids HashMap operations.
        let local_channel = if let Some((cached_ch, cached_local)) = self.last_channel_cache {
            if cached_ch == channel {
                // Cache hit: use cached local channel
                Some(cached_local)
            } else {
                // Cache miss: do full lookup
                self.lookup_channel(channel, from)
            }
        } else {
            // No cache: do full lookup
            self.lookup_channel(channel, from)
        };

        let Some(local_channel) = local_channel else {
            return;
        };

        // Update cache for next message
        if self.last_channel_cache.map_or(true, |(c, _)| c != channel) {
            self.last_channel_cache = Some((channel, local_channel));
        }

        // Get mutable state - this lookup is unavoidable but HashMap is hot in cache
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

        // Track publisher endpoint for sending NAK/STATUS_MSG
        if state.publisher_endpoint.is_none() {
            state.publisher_endpoint = Some(from);
        }

        // Insert into reorder buffer
        match state.reorder.insert(seq, frame) {
            InsertResult::Accepted => {
                // Frame accepted, drain any ready frames to client
                state.reorder.drain_ready(|delivered_seq, delivered_frame| {
                    if state.queue.push(delivered_frame).is_ok() {
                        state.consumption_offset = delivered_seq.as_u64().wrapping_add(1);
                    } else {
                        warn!(
                            local_channel = %local_channel,
                            seq = %delivered_seq,
                            "RX: client queue full, dropping frame"
                        );
                    }
                });
            }
            InsertResult::Duplicate => {
                trace!(local_channel = %local_channel, seq = %seq, "RX: duplicate frame");
            }
            InsertResult::TooOld => {
                trace!(local_channel = %local_channel, seq = %seq, "RX: frame too old");
            }
            InsertResult::TooNew => {
                warn!(
                    local_channel = %local_channel,
                    seq = %seq,
                    next_expected = %state.reorder.next_expected(),
                    "RX: frame too far ahead, dropping"
                );
            }
        }

        // Check for gaps after processing - schedule NAK timer if needed
        // Reuse the state reference we already have instead of re-looking up
        if state.reorder.has_gaps() {
            self.schedule_nak(local_channel, wheel);
        }
    }

    /// Handles an inbound fragmented data message.
    fn handle_data_frag(
        &mut self,
        channel: ChannelId,
        seq: SeqNum,
        frag_index: u16,
        frag_count: u16,
        payload: &Frame<DEFAULT_FRAME_CAP>,
        from: Endpoint,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        // Optimized lookup: try direct channel first, then remote mapping
        let (local_channel, state) = if let Some(state) = self.channels.get_mut(&channel) {
            (channel, state)
        } else {
            // Try remote mapping
            let key = RemoteStreamKey {
                endpoint: from,
                channel,
            };
            let Some(&local) = self.remote_mappings.get(&key) else {
                trace!(
                    channel = %channel,
                    from = %from,
                    seq = %seq,
                    frag_index = frag_index,
                    "RX: no mapping for fragmented data"
                );
                return;
            };
            let Some(state) = self.channels.get_mut(&local) else {
                return;
            };
            (local, state)
        };

        trace!(
            local_channel = %local_channel,
            seq = %seq,
            frag_index = frag_index,
            frag_count = frag_count,
            payload_len = payload.len(),
            "RX: received fragment"
        );

        // Insert fragment into reassembly buffer
        match state.reassembly.insert_fragment(seq, frag_index, frag_count, payload.payload()) {
            ReassemblyInsertResult::Complete => {
                // All fragments received - assemble and deliver
                let mut assembled_frame = Frame::<DEFAULT_FRAME_CAP>::new();
                if state.reassembly.assemble_into(seq, &mut assembled_frame).is_ok() {
                    trace!(
                        local_channel = %local_channel,
                        seq = %seq,
                        assembled_len = assembled_frame.len(),
                        "RX: message reassembled"
                    );

                    // Deliver to client via reorder buffer (same as normal DATA)
                    match state.reorder.insert(seq, assembled_frame) {
                        InsertResult::Accepted => {
                            // Drain ready frames to client
                            state.reorder.drain_ready(|delivered_seq, delivered_frame| {
                                if state.queue.push(delivered_frame).is_ok() {
                                    state.consumption_offset = delivered_seq.as_u64().wrapping_add(1);
                                } else {
                                    warn!(
                                        local_channel = %local_channel,
                                        seq = %delivered_seq,
                                        "RX: client queue full, dropping reassembled frame"
                                    );
                                }
                            });
                        }
                        InsertResult::Duplicate => {
                            trace!(local_channel = %local_channel, seq = %seq, "RX: reassembled frame duplicate");
                        }
                        InsertResult::TooOld => {
                            trace!(local_channel = %local_channel, seq = %seq, "RX: reassembled frame too old");
                        }
                        InsertResult::TooNew => {
                            warn!(local_channel = %local_channel, seq = %seq, "RX: reassembled frame too far ahead");
                        }
                    }

                    // Clear the reassembly slot
                    state.reassembly.clear_slot(seq);
                } else {
                    warn!(
                        local_channel = %local_channel,
                        seq = %seq,
                        "RX: failed to assemble message"
                    );
                }
            }
            ReassemblyInsertResult::Pending => {
                // More fragments needed
                trace!(
                    local_channel = %local_channel,
                    seq = %seq,
                    frag_index = frag_index,
                    "RX: fragment stored, waiting for more"
                );
            }
            ReassemblyInsertResult::Duplicate => {
                trace!(
                    local_channel = %local_channel,
                    seq = %seq,
                    frag_index = frag_index,
                    "RX: duplicate fragment"
                );
            }
            ReassemblyInsertResult::TooOld => {
                trace!(
                    local_channel = %local_channel,
                    seq = %seq,
                    "RX: fragment too old"
                );
            }
            ReassemblyInsertResult::Invalid => {
                warn!(
                    local_channel = %local_channel,
                    seq = %seq,
                    frag_index = frag_index,
                    frag_count = frag_count,
                    "RX: invalid fragment metadata"
                );
            }
            ReassemblyInsertResult::BufferFull => {
                warn!(
                    local_channel = %local_channel,
                    seq = %seq,
                    "RX: reassembly buffer full"
                );
            }
        }

        // Check for gaps after processing - schedule NAK timer if needed
        // Note: need to re-lookup because schedule_nak takes &mut self
        if let Some(state) = self.channels.get(&local_channel) {
            if state.reorder.has_gaps() {
                self.schedule_nak(local_channel, wheel);
            }
        }
    }

    /// Advances the timing wheel and handles fired timers.
    fn tick_timers(&mut self, wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>) {
        // Collect fired timers using pre-allocated buffer (avoids allocation)
        self.timer_buf.clear();
        wheel.tick_now(|_handle, event| {
            self.timer_buf.push(event);
        });

        // Process fired timers (iterate by index to avoid borrow issues)
        for i in 0..self.timer_buf.len() {
            match self.timer_buf[i] {
                RxTimerEvent::StatusMsg(channel_id) => {
                    self.handle_status_timer(channel_id, wheel);
                }
                RxTimerEvent::Nak(channel_id) => {
                    self.handle_nak_timer(channel_id, wheel);
                }
            }
        }
    }

    /// Schedules a STATUS_MSG timer for a channel.
    fn schedule_status_msg(
        &mut self,
        channel_id: ChannelId,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        let Some(state) = self.channels.get_mut(&channel_id) else {
            return;
        };

        // Don't schedule if already scheduled
        if state.status_scheduled {
            return;
        }

        let delay_ms = self.timing.status_message_interval.as_millis() as u64;
        let delay = Duration::<Millis>::from_millis(delay_ms);

        if wheel.schedule_after(delay, RxTimerEvent::StatusMsg(channel_id)).is_ok() {
            state.status_scheduled = true;
        }
    }

    /// Schedules a NAK timer for a channel.
    fn schedule_nak(
        &mut self,
        channel_id: ChannelId,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        let Some(state) = self.channels.get_mut(&channel_id) else {
            return;
        };

        // Don't schedule if already scheduled
        if state.nak_scheduled {
            return;
        }

        let delay_ms = self.timing.nak_delay.as_millis() as u64;
        let delay = Duration::<Millis>::from_millis(delay_ms);

        if wheel.schedule_after(delay, RxTimerEvent::Nak(channel_id)).is_ok() {
            state.nak_scheduled = true;
        }
    }

    /// Handles a fired STATUS_MSG timer.
    fn handle_status_timer(
        &mut self,
        channel_id: ChannelId,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        let Some(state) = self.channels.get_mut(&channel_id) else {
            return;
        };

        // Mark as not scheduled
        state.status_scheduled = false;

        let Some(endpoint) = state.publisher_endpoint else {
            // No publisher yet, reschedule
            self.schedule_status_msg(channel_id, wheel);
            return;
        };

        let session = state.publisher_session.unwrap_or_else(SessionId::generate);

        let frame = StatusMessageFrame {
            session,
            consumption_offset: ConsumptionOffset::from(state.consumption_offset),
            receiver_window: ReceiverWindow::new(DEFAULT_RECEIVER_WINDOW),
        };

        self.encode_buf.clear();
        if encode_status_message(&frame, &mut self.encode_buf).is_ok() {
            let _ = self.socket.try_send_to(&self.encode_buf, endpoint);
            trace!(
                channel = %channel_id,
                consumption_offset = state.consumption_offset,
                "RX: sent STATUS_MSG"
            );
        }

        // Reschedule for next status message
        self.schedule_status_msg(channel_id, wheel);
    }

    /// Handles a fired NAK timer.
    fn handle_nak_timer(
        &mut self,
        channel_id: ChannelId,
        wheel: &mut WheelScope<'_, RxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        let Some(state) = self.channels.get_mut(&channel_id) else {
            return;
        };

        // Mark as not scheduled
        state.nak_scheduled = false;

        // Only send NAK if there are still gaps
        if !state.reorder.has_gaps() {
            return;
        }

        let Some(endpoint) = state.publisher_endpoint else {
            return;
        };

        let session = state.publisher_session.unwrap_or_else(SessionId::generate);
        let max_nak_entries = self.timing.max_nak_entries;

        let entries = state.reorder.nak_entries(max_nak_entries);
        if entries.is_empty() {
            return;
        }

        let nak_entries: Vec<NakBitmapEntry> = entries
            .into_iter()
            .map(|e| NakBitmapEntry {
                base_seq: e.base_seq,
                bitmap: e.bitmap,
            })
            .collect();

        let frame = NakBitmapFrame {
            session,
            channel: channel_id,
            entries: nak_entries,
        };

        self.encode_buf.clear();
        if encode_nak_bitmap(&frame, &mut self.encode_buf).is_ok() {
            let _ = self.socket.try_send_to(&self.encode_buf, endpoint);
            debug!(
                channel = %channel_id,
                entry_count = frame.entries.len(),
                "RX: sent NAK_BITMAP"
            );
        }

        // Reschedule NAK timer if still has gaps (will be checked when timer fires)
        self.schedule_nak(channel_id, wheel);
    }

    /// Handles a DATA_NOT_AVAIL frame by accepting the gap.
    fn handle_data_not_avail(&mut self, channel: ChannelId, _unavail_start: u64, unavail_end: u64) {
        let Some(state) = self.channels.get_mut(&channel) else {
            return;
        };

        debug!(
            channel = %channel,
            unavail_end = unavail_end,
            "RX: accepting gap from DATA_NOT_AVAIL"
        );

        // Accept gap up to (and including) unavail_end
        state.reorder.accept_gap(SeqNum::from(unavail_end.wrapping_add(1)));

        // Drain any frames that are now ready
        state.reorder.drain_ready(|delivered_seq, delivered_frame| {
            if state.queue.push(delivered_frame).is_ok() {
                state.consumption_offset = delivered_seq.as_u64().wrapping_add(1);
            }
        });
    }
}
