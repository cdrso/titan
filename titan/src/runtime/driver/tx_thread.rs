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
//!
//! # Syscall Batching (Linux)
//!
//! On Linux, the TX thread uses `sendmmsg` to batch multiple UDP sends into a single
//! syscall, significantly reducing syscall overhead and improving throughput.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use crate::control::types::ChannelId;
use crate::data::Frame;
use crate::data::packing::{PackingBuffer, DEFAULT_MTU};
use crate::data::transport::{encode_data_frame, encode_data_frag, DATA_FRAG_HEADER_SIZE};
use crate::data::types::SeqNum;
use crate::net::{Endpoint, UdpSocket};
use crate::runtime::timing::{Duration, Micros, Millis, MonoInstant, NonZeroDuration, Now, WheelScope, with_wheel};
use crate::sync::spsc::Consumer;
use crate::trace::{debug, info, trace, warn};

#[cfg(target_os = "linux")]
use std::os::fd::{AsFd, AsRawFd};

use super::commands::{COMMAND_QUEUE_CAPACITY, DEFAULT_FRAME_CAP, RxToTxEvent, TxCommand};
use super::protocol::{self, SessionId};
use super::retransmit::{RetransmitLookup, RetransmitRing};
use super::timing::TimingConfig;

/// Default maximum messages to drain per channel per tick.
/// Set high to maximize single-channel throughput; multi-channel fairness via round-robin.
const DEFAULT_BATCH_SIZE: usize = 2048;

/// Default maximum total messages to send per tick (bounds latency).
/// High value for throughput - set lower for latency-sensitive workloads.
const DEFAULT_MAX_MESSAGES_PER_TICK: usize = 8192;

/// Retransmit ring capacity (number of frames to keep for retransmission).
/// Should be power of 2 for efficient modulo. Sized for high throughput.
const RETRANSMIT_RING_SIZE: usize = 8192;

/// Timing wheel slots (covers ~255ms max delay at 1ms tick).
const WHEEL_SLOTS: usize = 256;

/// Timing wheel capacity (max concurrent timers).
const WHEEL_CAPACITY: usize = 1024;

/// Timer event types for TX thread.
#[derive(Debug, Clone, Copy)]
enum TxTimerEvent {
    /// Heartbeat timer for a channel.
    Heartbeat(ChannelId),
}

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
    /// Ring buffer for retransmission (stores recent frames).
    retransmit_ring: RetransmitRing<Frame<DEFAULT_FRAME_CAP>, RETRANSMIT_RING_SIZE>,
    /// Whether a heartbeat timer is currently scheduled.
    /// Set to true when scheduling, false when fired/cancelled.
    heartbeat_scheduled: bool,
}

/// Per-subscriber flow control state.
struct SubscriberState {
    /// Subscriber's endpoint.
    endpoint: Endpoint,
    /// Last known consumption offset from subscriber.
    consumption_offset: SeqNum,
    /// Subscriber's advertised receiver window in bytes.
    receiver_window: u32,
    /// Negotiated MTU (max datagram size for this subscriber).
    mtu: u16,
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

/// How often to tick the timing wheel (every N loop iterations).
/// Wheel has 1ms resolution. With loop running at ~50M iter/s, tick every ~50K iterations.
/// 32768 (2^15) gives ~0.6ms jitter, well within 1ms resolution.
const WHEEL_TICK_DIVISOR: usize = 32768;

/// Maximum number of messages to batch before flushing via sendmmsg.
/// Balance between reducing syscall overhead and latency.
/// CONTRACT_027: Reduced from 256 to 32 to avoid stack overflow with 8KB MTU.
/// With packing: 32 packets * ~140 frames/packet = ~4480 frames per syscall.
#[cfg(target_os = "linux")]
const SENDMMSG_BATCH_SIZE: usize = 32;

/// Maximum wire frame size (MTU).
#[cfg(target_os = "linux")]
// CONTRACT_027: Increased to support MTU packing (8KB packets)
const MAX_WIRE_FRAME_SIZE: usize = 8192;

/// Batch for sendmmsg on connected sockets (no per-packet address needed).
/// Uses msg_name=NULL so kernel uses connected socket's cached route.
#[cfg(target_os = "linux")]
struct ConnectedBatch {
    data: [[u8; MAX_WIRE_FRAME_SIZE]; SENDMMSG_BATCH_SIZE],
    iovecs: [libc::iovec; SENDMMSG_BATCH_SIZE],
    msgs: [libc::mmsghdr; SENDMMSG_BATCH_SIZE],
    count: usize,
    initialized: bool,
}

#[cfg(target_os = "linux")]
impl ConnectedBatch {
    fn new() -> Self {
        unsafe {
            Self {
                data: [[0u8; MAX_WIRE_FRAME_SIZE]; SENDMMSG_BATCH_SIZE],
                iovecs: std::mem::zeroed(),
                msgs: std::mem::zeroed(),
                count: 0,
                initialized: false,
            }
        }
    }

    fn init(&mut self) {
        if self.initialized {
            return;
        }
        for i in 0..SENDMMSG_BATCH_SIZE {
            self.iovecs[i].iov_base = self.data[i].as_mut_ptr().cast();
            self.msgs[i].msg_hdr.msg_iov = std::ptr::addr_of_mut!(self.iovecs[i]);
            self.msgs[i].msg_hdr.msg_iovlen = 1;
            // msg_name = NULL for connected socket (uses cached route)
            self.msgs[i].msg_hdr.msg_name = std::ptr::null_mut();
            self.msgs[i].msg_hdr.msg_namelen = 0;
            self.msgs[i].msg_hdr.msg_control = std::ptr::null_mut();
            self.msgs[i].msg_hdr.msg_controllen = 0;
        }
        self.initialized = true;
    }

    #[inline]
    fn push(&mut self, data: &[u8]) -> bool {
        if self.count >= SENDMMSG_BATCH_SIZE || data.len() > MAX_WIRE_FRAME_SIZE {
            return false;
        }
        let idx = self.count;
        self.data[idx][..data.len()].copy_from_slice(data);
        self.iovecs[idx].iov_len = data.len();
        self.count += 1;
        true
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.count >= SENDMMSG_BATCH_SIZE
    }

    #[inline]
    fn len(&self) -> usize {
        self.count
    }

    /// Flushes all queued packets via sendmmsg.
    #[inline]
    fn flush(&mut self, fd: libc::c_int) -> usize {
        if self.count == 0 {
            return 0;
        }

        let batch_size = self.count;

        // Send all queued packets in one syscall
        let sent = unsafe {
            libc::sendmmsg(fd, self.msgs.as_mut_ptr(), batch_size as u32, libc::MSG_DONTWAIT)
        };

        self.count = 0;
        if sent > 0 { sent as usize } else { 0 }
    }
}

/// Pre-allocated batch storage for sendmmsg.
/// Uses libc types directly to eliminate all per-flush heap allocations.
///
/// # Safety
/// This struct must not be moved after `init()` is called, as the internal
/// pointers in `msgs` and `iovecs` reference other fields within the struct.
#[cfg(target_os = "linux")]
struct SendBatch {
    /// Pre-allocated data buffers (one per batch slot).
    data: [[u8; MAX_WIRE_FRAME_SIZE]; SENDMMSG_BATCH_SIZE],
    /// iovec array pointing to data buffers.
    iovecs: [libc::iovec; SENDMMSG_BATCH_SIZE],
    /// mmsghdr array for sendmmsg.
    msgs: [libc::mmsghdr; SENDMMSG_BATCH_SIZE],
    /// Destination address storage for each message (sockaddr_in sized).
    addrs: [libc::sockaddr_in; SENDMMSG_BATCH_SIZE],
    /// Current number of queued messages.
    count: usize,
    /// Whether init() has been called.
    initialized: bool,
}

#[cfg(target_os = "linux")]
impl SendBatch {
    fn new() -> Self {
        // Safety: zero-initialized structs are valid for libc types
        unsafe {
            Self {
                data: [[0u8; MAX_WIRE_FRAME_SIZE]; SENDMMSG_BATCH_SIZE],
                iovecs: std::mem::zeroed(),
                msgs: std::mem::zeroed(),
                addrs: std::mem::zeroed(),
                count: 0,
                initialized: false,
            }
        }
    }

    /// Initializes internal pointers. Must be called once after struct is in final location.
    #[inline]
    fn init(&mut self) {
        if self.initialized {
            return;
        }
        for i in 0..SENDMMSG_BATCH_SIZE {
            // Set up iovec to point to data buffer (iov_len set per-message)
            self.iovecs[i].iov_base = self.data[i].as_mut_ptr().cast();

            // Set up mmsghdr
            self.msgs[i].msg_hdr.msg_iov = std::ptr::addr_of_mut!(self.iovecs[i]);
            self.msgs[i].msg_hdr.msg_iovlen = 1;
            self.msgs[i].msg_hdr.msg_name = std::ptr::addr_of_mut!(self.addrs[i]).cast();
            self.msgs[i].msg_hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_in>() as u32;
            self.msgs[i].msg_hdr.msg_control = std::ptr::null_mut();
            self.msgs[i].msg_hdr.msg_controllen = 0;
            self.msgs[i].msg_hdr.msg_flags = 0;
        }
        self.initialized = true;
    }

    #[inline]
    fn push(&mut self, data: &[u8], addr: SocketAddr) {
        debug_assert!(self.initialized, "SendBatch::init() must be called first");
        debug_assert!(self.count < SENDMMSG_BATCH_SIZE);
        debug_assert!(data.len() <= MAX_WIRE_FRAME_SIZE);

        let idx = self.count;

        // Copy data and set length
        self.data[idx][..data.len()].copy_from_slice(data);
        self.iovecs[idx].iov_len = data.len();

        // Convert SocketAddr to sockaddr_in (IPv4 only for now - test uses localhost)
        if let SocketAddr::V4(v4) = addr {
            self.addrs[idx].sin_family = libc::AF_INET as u16;
            self.addrs[idx].sin_port = v4.port().to_be();
            self.addrs[idx].sin_addr.s_addr = u32::from(*v4.ip()).to_be();
        }

        self.count += 1;
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.count >= SENDMMSG_BATCH_SIZE
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[inline]
    fn clear(&mut self) {
        self.count = 0;
    }

    #[inline]
    fn len(&self) -> usize {
        self.count
    }
}

/// TX thread state and event loop.
pub struct TxThread {
    /// UDP socket for sending (shared with RX thread - used as template for bind address).
    socket: Arc<UdpSocket>,
    /// Connected UDP sockets per destination (avoids route lookup per packet).
    /// Key is the destination SocketAddr, value is a connected socket.
    connected_sockets: HashMap<SocketAddr, std::net::UdpSocket>,
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
    /// Maximum messages to drain per channel per tick.
    batch_size: usize,
    /// Maximum total messages to send per tick.
    max_messages_per_tick: usize,
    /// Index to rotate starting channel for fairness.
    round_robin_offset: usize,
    /// Timing configuration for reliability protocol.
    timing: TimingConfig,
    /// Counter for rate-limiting timing wheel ticks.
    tick_counter: usize,
    /// Pre-allocated batch buffer for sendmmsg (Linux only) - DEPRECATED, kept for fallback.
    #[cfg(target_os = "linux")]
    send_batch: SendBatch,
    /// Placeholder for non-Linux (unused).
    #[cfg(not(target_os = "linux"))]
    send_batch: (),
    /// Pre-allocated batch for connected socket sendmmsg (Linux only).
    #[cfg(target_os = "linux")]
    connected_batch: ConnectedBatch,
    /// Packing buffer for accumulating frames into MTU-sized UDP packets (CONTRACT_027).
    packing_buffer: PackingBuffer,
    /// Pre-allocated buffer for fired timers (avoids allocation in tick_timers).
    fired_timers: Vec<TxTimerEvent>,
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
        Self::with_full_config(socket, commands, rx_events, config, TimingConfig::default())
    }

    /// Creates a new TX thread state with custom TX and timing configuration.
    pub fn with_full_config(
        socket: Arc<UdpSocket>,
        commands: Consumer<TxCommand, COMMAND_QUEUE_CAPACITY>,
        rx_events: Consumer<RxToTxEvent, COMMAND_QUEUE_CAPACITY>,
        config: TxConfig,
        timing: TimingConfig,
    ) -> Self {
        Self {
            socket,
            connected_sockets: HashMap::new(),
            commands,
            rx_events,
            channels: HashMap::new(),
            channel_order: Vec::new(),
            encode_buf: Vec::with_capacity(2048),
            batch_size: config.batch_size,
            max_messages_per_tick: config.max_messages_per_tick,
            round_robin_offset: 0,
            timing,
            tick_counter: 0,
            #[cfg(target_os = "linux")]
            send_batch: SendBatch::new(),
            #[cfg(not(target_os = "linux"))]
            send_batch: (),
            #[cfg(target_os = "linux")]
            connected_batch: ConnectedBatch::new(),
            // Packing buffer for MTU-sized packets (CONTRACT_027)
            packing_buffer: PackingBuffer::new(DEFAULT_MTU),
            // Pre-allocate for typical timer count (heartbeats per channel)
            fired_timers: Vec::with_capacity(64),
        }
    }

    /// Gets or creates a connected UDP socket for the given destination.
    /// Connected sockets cache the route lookup, providing ~100x faster sends.
    fn get_or_create_connected_socket(&mut self, dest: SocketAddr) -> Option<&std::net::UdpSocket> {
        // Use entry API to avoid double lookup
        if !self.connected_sockets.contains_key(&dest) {
            // Create new connected socket
            match std::net::UdpSocket::bind("0.0.0.0:0") {
                Ok(sock) => {
                    // Set non-blocking to match behavior of MSG_DONTWAIT
                    if let Err(e) = sock.set_nonblocking(true) {
                        warn!(dest = %dest, error = %e, "failed to set socket non-blocking");
                    }
                    // Connect to destination (caches route)
                    if let Err(e) = sock.connect(dest) {
                        warn!(dest = %dest, error = %e, "failed to connect socket");
                        return None;
                    }
                    self.connected_sockets.insert(dest, sock);
                }
                Err(e) => {
                    warn!(dest = %dest, error = %e, "failed to bind connected socket");
                    return None;
                }
            }
        }
        self.connected_sockets.get(&dest)
    }

    /// Sends data to an endpoint using a connected socket (fast path).
    /// Static version for split borrowing in drain_channel.
    #[cfg(target_os = "linux")]
    fn send_connected_static(
        connected_sockets: &mut HashMap<SocketAddr, std::net::UdpSocket>,
        _fallback_socket: &UdpSocket,
        data: &[u8],
        endpoint: Endpoint,
    ) {
        use std::os::fd::AsRawFd;
        let dest = endpoint.as_socket_addr();

        let sock = connected_sockets.entry(dest).or_insert_with(|| {
            let sock = std::net::UdpSocket::bind("0.0.0.0:0").expect("bind connected socket");
            let _ = sock.set_nonblocking(true);
            let _ = sock.connect(dest);
            sock
        });

        unsafe {
            libc::send(
                sock.as_raw_fd(),
                data.as_ptr() as *const libc::c_void,
                data.len(),
                libc::MSG_DONTWAIT,
            );
        }
    }

    /// Sends data to an endpoint using a connected socket (fast path).
    #[cfg(not(target_os = "linux"))]
    fn send_connected_static(
        connected_sockets: &mut HashMap<SocketAddr, std::net::UdpSocket>,
        _fallback_socket: &UdpSocket,
        data: &[u8],
        endpoint: Endpoint,
    ) {
        let dest = endpoint.as_socket_addr();

        let sock = connected_sockets.entry(dest).or_insert_with(|| {
            let sock = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
            let _ = sock.set_nonblocking(true);
            let _ = sock.connect(dest);
            sock
        });

        let _ = sock.send(data);
    }

    /// Runs the TX thread event loop.
    ///
    /// Returns when a `Shutdown` command is received.
    pub fn run(&mut self) {
        // Initialize send batch pointers now that struct is in final memory location
        #[cfg(target_os = "linux")]
        self.send_batch.init();

        let tick_duration =
            NonZeroDuration::<Millis>::new(NonZeroU64::new(1).expect("1 != 0"));
        let capacity = NonZeroUsize::new(WHEEL_CAPACITY).expect("WHEEL_CAPACITY != 0");

        with_wheel::<TxTimerEvent, Millis, WHEEL_SLOTS, _>(tick_duration, capacity, |wheel| {
            self.run_with_wheel(wheel);
        });
    }

    /// Inner run loop with timing wheel.
    fn run_with_wheel(
        &mut self,
        wheel: &mut WheelScope<'_, TxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        loop {
            // Process control commands first (cold path)
            if self.process_commands(wheel) {
                // Shutdown requested
                return;
            }

            // Process RX events (reliability feedback)
            self.process_rx_events();

            // Drain client queues and send to network (hot path)
            self.drain_and_send(wheel);

            // Flush any pending sends to ensure timely delivery
            #[cfg(target_os = "linux")]
            self.flush_send_batch();

            // Fire due timers (heartbeats) - rate-limited to reduce timing overhead
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
        wheel: &mut WheelScope<'_, TxTimerEvent, Millis, WHEEL_SLOTS>,
    ) -> bool {
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
                            retransmit_ring: RetransmitRing::new(),
                            heartbeat_scheduled: false,
                        },
                    );
                    // Schedule initial heartbeat timer
                    self.schedule_heartbeat(channel, wheel);
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
                    mtu,
                } => {
                    info!(
                        channel = %channel,
                        session = %session,
                        endpoint = %endpoint,
                        receiver_window = receiver_window,
                        mtu = mtu,
                        "TX: adding subscriber"
                    );
                    if let Some(state) = self.channels.get_mut(&channel) {
                        state.subscribers.insert(
                            session,
                            SubscriberState {
                                endpoint,
                                consumption_offset: SeqNum::ZERO,
                                receiver_window,
                                mtu,
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
                RxToTxEvent::Nack { channel, seq, from } => {
                    // Trigger retransmission for the requested sequence
                    self.handle_retransmit(channel, seq, from);
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
    fn drain_and_send(
        &mut self,
        wheel: &mut WheelScope<'_, TxTimerEvent, Millis, WHEEL_SLOTS>,
    ) -> usize {
        let num_channels = self.channel_order.len();
        if num_channels == 0 {
            return 0;
        }

        // Cache timestamp once per tick to avoid repeated TSC reads and conversions
        let batch_timestamp = Micros::now();
        let mut budget = self.max_messages_per_tick;
        let initial_budget = budget;

        // Rotate through channels starting from round_robin_offset
        for i in 0..num_channels {
            if budget == 0 {
                break;
            }

            let idx = (self.round_robin_offset + i) % num_channels;
            let channel_id = self.channel_order[idx];

            let sent = self.drain_channel(channel_id, budget, batch_timestamp, wheel);
            budget = budget.saturating_sub(sent);
        }

        // Advance round-robin offset for next tick
        self.round_robin_offset = (self.round_robin_offset + 1) % num_channels;

        // Return total dispatched
        initial_budget - budget

        // Note: We don't flush here to allow batches to accumulate across loop iterations.
        // Flush happens when batch is full or before the next tick_timers.
    }

    /// Drains a single channel up to `limit` messages (capped by `batch_size`).
    ///
    /// Returns the number of messages actually sent.
    #[cfg(target_os = "linux")]
    fn drain_channel(
        &mut self,
        channel_id: ChannelId,
        limit: usize,
        timestamp: MonoInstant<Micros>,
        wheel: &mut WheelScope<'_, TxTimerEvent, Millis, WHEEL_SLOTS>,
    ) -> usize {
        use std::os::fd::AsRawFd;

        let max_to_send = self.batch_size.min(limit);
        let mut sent = 0;

        // Get channel state
        let Some(state) = self.channels.get_mut(&channel_id) else {
            return 0;
        };

        // Fast path: single destination (1 endpoint OR 1 subscriber) - use batched sendmmsg
        // We batch sends when there's exactly one target and no other complications
        let single_endpoint = state.endpoints.len() == 1 && state.subscribers.is_empty();
        let single_subscriber = state.endpoints.is_empty() && state.subscribers.len() == 1;
        let use_batched = single_endpoint || single_subscriber;

        // Get connected socket fd for batched path
        let batch_fd = if single_endpoint {
            let endpoint = state.endpoints[0];
            let dest = endpoint.as_socket_addr();
            let sock = self.connected_sockets.entry(dest).or_insert_with(|| {
                // Use 127.0.0.1 bind like standalone benchmark
                let sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind connected socket");
                sock.set_nonblocking(true).expect("set nonblocking");
                sock.connect(dest).expect("connect");

                // CONTRACT_024 FIX: Disable IP_RECVERR to prevent ICMP "port unreachable"
                // from interrupting sendmmsg batches. On loopback, ICMP arrives before
                // sendmmsg can send subsequent messages, causing only 1 msg per call.
                let fd = sock.as_raw_fd();
                unsafe {
                    let optval: libc::c_int = 0;
                    libc::setsockopt(
                        fd,
                        libc::IPPROTO_IP,
                        libc::IP_RECVERR,
                        &optval as *const _ as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    );
                }
                sock
            });
            Some(sock.as_raw_fd())
        } else if single_subscriber {
            // Get the single subscriber's endpoint
            let sub_endpoint = state.subscribers.values().next().unwrap().endpoint;
            let dest = sub_endpoint.as_socket_addr();
            let sock = self.connected_sockets.entry(dest).or_insert_with(|| {
                let sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind connected socket");
                sock.set_nonblocking(true).expect("set nonblocking");
                sock.connect(dest).expect("connect");

                // CONTRACT_024 FIX: Disable IP_RECVERR
                let fd = sock.as_raw_fd();
                unsafe {
                    let optval: libc::c_int = 0;
                    libc::setsockopt(
                        fd,
                        libc::IPPROTO_IP,
                        libc::IP_RECVERR,
                        &optval as *const _ as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    );
                }
                sock
            });
            Some(sock.as_raw_fd())
        } else {
            None
        };

        // Initialize connected batch if needed
        if use_batched {
            self.connected_batch.init();
        }

        // Sanity check: if we have subscribers, batch_fd should be set
        debug_assert!(
            batch_fd.is_some() || (state.endpoints.is_empty() && state.subscribers.is_empty()),
            "batch_fd is None but channel has destinations: endpoints={}, subscribers={}",
            state.endpoints.len(), state.subscribers.len()
        );

        // Re-borrow state after connected_sockets modification
        let state = self.channels.get_mut(&channel_id).unwrap();
        let encode_buf = &mut self.encode_buf;
        let connected_batch = &mut self.connected_batch;
        let packing_buffer = &mut self.packing_buffer;
        let connected_sockets = &mut self.connected_sockets;
        let socket = &self.socket;

        for _ in 0..max_to_send {
            // Pop frame from client queue
            let Some(frame) = state.queue.pop() else {
                break;
            };

            // Assign sequence number (timestamp is cached per batch for efficiency)
            let seq = state.next_seq;
            state.next_seq = seq.next();

            // Store frame in retransmit ring for potential retransmission
            let ring_seq = state.retransmit_ring.push(frame);
            debug_assert_eq!(ring_seq, seq, "retransmit ring seq mismatch");

            // Get frame reference from ring for sending
            let frame_ref = match state.retransmit_ring.get(ring_seq) {
                RetransmitLookup::Available(f) => f,
                _ => {
                    warn!(channel = %channel_id, seq = %seq, "TX: frame disappeared from ring");
                    continue;
                }
            };

            // Encode DATA frame directly (zero-copy from retransmit ring)
            if encode_data_frame(channel_id, seq, timestamp, frame_ref, encode_buf).is_err() {
                warn!(channel = %channel_id, seq = %seq, "TX: failed to encode message");
                continue;
            }

            if let Some(fd) = batch_fd {
                // CONTRACT_027: Pack multiple frames into MTU-sized UDP packets
                // Check if encoded frame fits in packing buffer
                if !packing_buffer.can_fit(encode_buf.len()) {
                    // Flush packed buffer as single UDP packet
                    if let Some(packed) = packing_buffer.flush() {
                        if connected_batch.is_full() {
                            connected_batch.flush(fd);
                        }
                        connected_batch.push(packed);
                    }
                    packing_buffer.clear();
                }
                // Push encoded frame to packing buffer
                packing_buffer.push(encode_buf);
            } else {
                // Unbatched path: send immediately to each endpoint
                for endpoint in state.endpoints.iter() {
                    Self::send_connected_static(connected_sockets, socket, encode_buf, *endpoint);
                }
                // Fan-out to subscribers with MTU enforcement and fragmentation (unbatched only)
                for sub in state.subscribers.values() {
                    let mtu = sub.mtu as usize;

                    // Check if frame fits in subscriber's MTU
                    if encode_buf.len() <= mtu {
                        // No fragmentation needed - use connected socket
                        Self::send_connected_static(connected_sockets, socket, encode_buf, sub.endpoint);
                    } else {
                        // Fragmentation required
                        let payload = frame_ref.payload();
                        let max_frag_payload = mtu.saturating_sub(DATA_FRAG_HEADER_SIZE);

                        if max_frag_payload == 0 {
                            warn!(
                                channel = %channel_id,
                                seq = %seq,
                                mtu = mtu,
                                "TX: MTU too small for fragmentation header"
                            );
                            continue;
                        }

                        // Calculate fragment count (ceiling division)
                        let frag_count = (payload.len() + max_frag_payload - 1) / max_frag_payload;
                        let frag_count_u16 = match u16::try_from(frag_count) {
                            Ok(c) => c,
                            Err(_) => {
                                warn!(
                                    channel = %channel_id,
                                    seq = %seq,
                                    frag_count = frag_count,
                                    "TX: too many fragments required"
                                );
                                continue;
                            }
                        };

                        trace!(
                            channel = %channel_id,
                            seq = %seq,
                            payload_len = payload.len(),
                            mtu = mtu,
                            frag_count = frag_count,
                            "TX: fragmenting message"
                        );

                        // Send each fragment using connected socket
                        for frag_idx in 0..frag_count {
                            let start = frag_idx * max_frag_payload;
                            let end = (start + max_frag_payload).min(payload.len());
                            let frag_payload = &payload[start..end];

                            if encode_data_frag(
                                channel_id,
                                seq,
                                timestamp,
                                frag_idx as u16,
                                frag_count_u16,
                                frag_payload,
                                encode_buf,
                            ).is_err() {
                                warn!(
                                    channel = %channel_id,
                                    seq = %seq,
                                    frag_idx = frag_idx,
                                    "TX: failed to encode fragment"
                                );
                                break;
                            }

                            Self::send_connected_static(connected_sockets, socket, encode_buf, sub.endpoint);
                        }
                    }
                }
            }

            sent += 1;
        }

        // CONTRACT_030: Always flush packing buffer at end of drain_channel
        // This ensures packets are sent even when queue has more messages than max_to_send.
        // The packing buffer accumulates frames within a single drain_channel call,
        // and connected_batch accumulates packed packets for efficient sendmmsg.
        if let Some(fd) = batch_fd {
            // Flush any remaining frames in packing buffer
            if let Some(packed) = self.packing_buffer.flush() {
                if self.connected_batch.is_full() {
                    self.connected_batch.flush(fd);
                }
                self.connected_batch.push(packed);
            }
            self.packing_buffer.clear();
            // Flush remaining sendmmsg batch
            if self.connected_batch.len() > 0 {
                self.connected_batch.flush(fd);
            }
        }

        // If we sent any data, reset heartbeat timer (channel is active)
        if sent > 0 {
            // Mark heartbeat as not scheduled so it gets rescheduled
            if let Some(state) = self.channels.get_mut(&channel_id) {
                state.heartbeat_scheduled = false;
            }
            self.schedule_heartbeat(channel_id, wheel);
        }

        sent
    }

    /// Drains a single channel (non-Linux fallback).
    #[cfg(not(target_os = "linux"))]
    fn drain_channel(
        &mut self,
        channel_id: ChannelId,
        limit: usize,
        timestamp: MonoInstant<Micros>,
        wheel: &mut WheelScope<'_, TxTimerEvent, Millis, WHEEL_SLOTS>,
    ) -> usize {
        let max_to_send = self.batch_size.min(limit);
        let mut sent = 0;

        let connected_sockets = &mut self.connected_sockets;
        let socket = &self.socket;
        let encode_buf = &mut self.encode_buf;

        let Some(state) = self.channels.get_mut(&channel_id) else {
            return 0;
        };

        for _ in 0..max_to_send {
            let Some(frame) = state.queue.pop() else {
                break;
            };

            let seq = state.next_seq;
            state.next_seq = seq.next();

            let ring_seq = state.retransmit_ring.push(frame);
            debug_assert_eq!(ring_seq, seq, "retransmit ring seq mismatch");

            let frame_ref = match state.retransmit_ring.get(ring_seq) {
                RetransmitLookup::Available(f) => f,
                _ => {
                    warn!(channel = %channel_id, seq = %seq, "TX: frame disappeared from ring");
                    continue;
                }
            };

            if encode_data_frame(channel_id, seq, timestamp, frame_ref, encode_buf).is_err() {
                warn!(channel = %channel_id, seq = %seq, "TX: failed to encode message");
                continue;
            }

            for endpoint in state.endpoints.iter() {
                Self::send_connected_static(connected_sockets, socket, encode_buf, *endpoint);
            }

            for sub in state.subscribers.values() {
                Self::send_connected_static(connected_sockets, socket, encode_buf, sub.endpoint);
            }

            sent += 1;
        }

        if sent > 0 {
            if let Some(state) = self.channels.get_mut(&channel_id) {
                state.heartbeat_scheduled = false;
            }
            self.schedule_heartbeat(channel_id, wheel);
        }

        sent
    }

    /// Queues a send to the batch buffer (Linux with sendmmsg) or sends immediately (other platforms).
    /// Uses split borrowing to avoid borrow conflicts with channel state.
    #[cfg(target_os = "linux")]
    #[inline]
    fn queue_send_raw(
        socket: &UdpSocket,
        encode_buf: &[u8],
        send_batch: &mut SendBatch,
        endpoint: Endpoint,
    ) {
        // Flush if batch is full BEFORE pushing
        if send_batch.is_full() {
            Self::flush_send_batch_raw(socket, send_batch);
        }

        // Copy into pre-allocated slot (no heap allocation)
        send_batch.push(encode_buf, endpoint.as_socket_addr());
    }

    #[cfg(not(target_os = "linux"))]
    #[inline]
    fn queue_send_raw(
        socket: &UdpSocket,
        encode_buf: &[u8],
        _send_batch: &mut (),
        endpoint: Endpoint,
    ) {
        // Direct send on non-Linux platforms
        let _ = socket.try_send_to(encode_buf, endpoint);
    }

    /// Flushes the send batch using sendmmsg (Linux only).
    #[cfg(target_os = "linux")]
    fn flush_send_batch(&mut self) -> bool {
        Self::flush_send_batch_raw(&self.socket, &mut self.send_batch)
    }

    /// Static version of flush for split borrowing.
    /// Uses libc::sendmmsg directly with pre-allocated buffers (zero heap allocations).
    /// Returns true if sendmmsg was called.
    #[cfg(target_os = "linux")]
    fn flush_send_batch_raw(socket: &UdpSocket, send_batch: &mut SendBatch) -> bool {
        if send_batch.is_empty() {
            return false;
        }

        let count = send_batch.len();
        let fd = socket.as_fd().as_raw_fd();

        // Safety: send_batch.msgs is properly initialized by init() and populated by push().
        // All pointers in msgs reference valid data within send_batch.
        // count <= SENDMMSG_BATCH_SIZE is enforced by is_full() check in queue_send_raw.
        let sent = unsafe {
            libc::sendmmsg(
                fd,
                send_batch.msgs.as_mut_ptr(),
                count as libc::c_uint,
                libc::MSG_DONTWAIT, // Non-blocking: don't stall on full kernel buffer
            )
        };

        if sent >= 0 {
            trace!(batch_size = count, sent = sent, "sendmmsg completed");
        }

        send_batch.clear();
        true
    }

    /// Advances the timing wheel and handles fired timers.
    fn tick_timers(&mut self, wheel: &mut WheelScope<'_, TxTimerEvent, Millis, WHEEL_SLOTS>) {
        // Collect fired timers using pre-allocated buffer (avoids per-tick allocation)
        self.fired_timers.clear();
        wheel.tick_now(|_handle, event| {
            self.fired_timers.push(event);
        });

        // Process fired timers (iterate by index to avoid borrow conflict)
        for i in 0..self.fired_timers.len() {
            match self.fired_timers[i] {
                TxTimerEvent::Heartbeat(channel_id) => {
                    self.handle_heartbeat_timer(channel_id, wheel);
                }
            }
        }
    }

    /// Schedules a heartbeat timer for a channel.
    ///
    /// If a timer is already scheduled, this is a no-op (the existing timer
    /// will fire and reschedule). When the timer fires, it checks if the
    /// channel still exists and has subscribers.
    fn schedule_heartbeat(
        &mut self,
        channel_id: ChannelId,
        wheel: &mut WheelScope<'_, TxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        let Some(state) = self.channels.get_mut(&channel_id) else {
            return;
        };

        // Don't schedule if already scheduled
        if state.heartbeat_scheduled {
            return;
        }

        // Schedule heartbeat after heartbeat_interval
        let delay_ms = self.timing.heartbeat_interval.as_millis() as u64;
        let delay = Duration::<Millis>::from_millis(delay_ms);

        if wheel.schedule_after(delay, TxTimerEvent::Heartbeat(channel_id)).is_ok() {
            state.heartbeat_scheduled = true;
        }
    }

    /// Handles a fired heartbeat timer.
    fn handle_heartbeat_timer(
        &mut self,
        channel_id: ChannelId,
        wheel: &mut WheelScope<'_, TxTimerEvent, Millis, WHEEL_SLOTS>,
    ) {
        let Some(state) = self.channels.get_mut(&channel_id) else {
            // Channel was removed, don't reschedule
            return;
        };

        // Mark as not scheduled (timer fired)
        state.heartbeat_scheduled = false;

        // Only send heartbeat if channel has subscribers
        if state.subscribers.is_empty() {
            // No subscribers, reschedule anyway (will check again when it fires)
            self.schedule_heartbeat(channel_id, wheel);
            return;
        }

        // Build heartbeat frame
        // Use any subscriber's session for the heartbeat (they all track the same channel)
        if let Some((&session, _)) = state.subscribers.iter().next() {
            let next_seq = state.retransmit_ring.next_seq();

            trace!(
                channel = %channel_id,
                session = %session,
                next_seq = next_seq.as_u64(),
                "TX: sending HEARTBEAT"
            );

            let frame = protocol::HeartbeatFrame {
                session,
                next_seq: next_seq.as_u64(),
            };

            self.encode_buf.clear();
            if protocol::encode_heartbeat(&frame, &mut self.encode_buf).is_ok() {
                // Send to all subscribers
                for sub in state.subscribers.values() {
                    let _ = self.socket.try_send_to(&self.encode_buf, sub.endpoint);
                }
            }
        }

        // Reschedule for next heartbeat
        self.schedule_heartbeat(channel_id, wheel);
    }

    /// Handles a NAK by retransmitting the requested frame.
    fn handle_retransmit(&mut self, channel_id: ChannelId, seq: SeqNum, endpoint: Endpoint) {
        let Some(state) = self.channels.get(&channel_id) else {
            return;
        };

        match state.retransmit_ring.get(seq) {
            RetransmitLookup::Available(frame) => {
                trace!(channel = %channel_id, seq = %seq, "TX: retransmitting frame");

                // Encode directly from retransmit ring (zero-copy)
                if encode_data_frame(channel_id, seq, Micros::now(), frame, &mut self.encode_buf).is_ok() {
                    let _ = self.socket.try_send_to(&self.encode_buf, endpoint);
                }
            }
            RetransmitLookup::Evicted { oldest_available, .. } => {
                // Data is no longer available, send DATA_NOT_AVAIL
                debug!(
                    channel = %channel_id,
                    requested = %seq,
                    oldest_available = %oldest_available,
                    "TX: requested data evicted, sending DATA_NOT_AVAIL"
                );

                // Find the subscriber session for this endpoint
                for (&session, sub) in &state.subscribers {
                    if sub.endpoint == endpoint {
                        let frame = protocol::DataNotAvailFrame {
                            session,
                            channel: channel_id,
                            unavail_start: seq.as_u64(),
                            unavail_end: seq.as_u64(),
                            oldest_available: oldest_available.as_u64(),
                        };

                        self.encode_buf.clear();
                        if protocol::encode_data_not_avail(&frame, &mut self.encode_buf).is_ok() {
                            let _ = self.socket.try_send_to(&self.encode_buf, endpoint);
                        }
                        break;
                    }
                }
            }
            RetransmitLookup::NotYetSent { .. } => {
                // Sequence hasn't been sent yet - ignore spurious NAK
                trace!(channel = %channel_id, seq = %seq, "TX: NAK for unsent sequence, ignoring");
            }
        }
    }
}
