//! Driver-to-driver wire protocol for subscription establishment and flow control.
//!
//! This module defines the frames exchanged between drivers for:
//! - Subscription setup with type validation
//! - Flow control via Status Messages
//! - Reliable delivery via NAK-based retransmission
//! - Graceful teardown
//!
//! # Wire Format
//!
//! All frames share a common 12-byte header:
//!
//! ```text
//! ┌─────────┬─────────┬─────────┬───────────────────────────────────┐
//! │ Type(1) │ Flags(1)│ Len(2)  │ Session ID (8)                    │
//! └─────────┴─────────┴─────────┴───────────────────────────────────┘
//! ```
//!
//! ## Session Management (0x10-0x14)
//! - 0x10 = SETUP (subscriber → publisher)
//! - 0x11 = SETUP_ACK (publisher → subscriber)
//! - 0x12 = SETUP_NAK (publisher → subscriber)
//! - 0x13 = STATUS_MSG (subscriber → publisher)
//! - 0x14 = TEARDOWN (either direction)
//! - 0x15 = HEARTBEAT (publisher → subscriber)
//!
//! ## Data Plane (0x20-0x23)
//! - 0x20 = DATA (publisher → subscriber)
//! - 0x21 = DATA_FRAG (publisher → subscriber, fragmented)
//! - 0x22 = NAK_BITMAP (subscriber → publisher)
//! - 0x23 = DATA_NOT_AVAIL (publisher → subscriber)
//!
//! # Integration with Transport Modes
//!
//! ## Unicast (Current)
//! All protocol frames travel directly between driver endpoints.
//!
//! ## Multicast (Future)
//! - SETUP/SETUP_ACK/SETUP_NAK: Always unicast (subscriber knows publisher address)
//! - SM: Unicast back to publisher (like Aeron)
//! - DATA: Goes to multicast group address
//! - TEARDOWN: Unicast
//!
//! ## MDC (Future)
//! Same as unicast, but publisher maintains list of subscribers and sends
//! DATA to each individually.

use std::fmt;
use thiserror::Error;

use crate::control::types::{ChannelId, TypeId};

/// Discriminated frame kind based on first byte.
///
/// Provides type-safe evidence of frame classification rather than a boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// Data plane frame (type byte < 0x10).
    Data(u8),
    /// Protocol/control frame (type byte >= 0x10).
    Protocol(u8),
}

impl FrameKind {
    /// Classify a frame based on its first byte.
    #[inline]
    #[must_use]
    pub const fn from_first_byte(byte: u8) -> Self {
        if byte >= 0x10 {
            Self::Protocol(byte)
        } else {
            Self::Data(byte)
        }
    }

    /// Returns the raw type byte.
    #[inline]
    #[must_use]
    pub const fn type_byte(self) -> u8 {
        match self {
            Self::Data(b) | Self::Protocol(b) => b,
        }
    }
}

/// Protocol frame type discriminants.
///
/// Types 0x10-0x1F are session management frames.
/// Types 0x20-0x2F are data plane frames.
/// This allows the RX thread to quickly distinguish frame categories.
pub mod frame_type {
    use super::FrameKind;

    // Session management frames (0x10-0x1F)
    pub const SETUP: u8 = 0x10;
    pub const SETUP_ACK: u8 = 0x11;
    pub const SETUP_NAK: u8 = 0x12;
    pub const STATUS_MESSAGE: u8 = 0x13;
    pub const TEARDOWN: u8 = 0x14;
    pub const HEARTBEAT: u8 = 0x15;

    // Data plane frames (0x20-0x2F)
    pub const DATA: u8 = 0x20;
    pub const DATA_FRAG: u8 = 0x21;
    pub const NAK_BITMAP: u8 = 0x22;
    pub const DATA_NOT_AVAIL: u8 = 0x23;

    /// Classify frame type from first byte.
    #[inline]
    #[must_use]
    pub const fn classify(first_byte: u8) -> FrameKind {
        FrameKind::from_first_byte(first_byte)
    }

    /// Returns true if this is a session management frame (0x10-0x1F).
    #[inline]
    #[must_use]
    pub const fn is_session_frame(frame_type: u8) -> bool {
        frame_type >= 0x10 && frame_type < 0x20
    }

    /// Returns true if this is a data plane frame (0x20-0x2F).
    #[inline]
    #[must_use]
    pub const fn is_data_frame(frame_type: u8) -> bool {
        frame_type >= 0x20 && frame_type < 0x30
    }
}

/// Common header for all protocol frames.
///
/// ```text
/// ┌─────────┬─────────┬─────────┬───────────────────────────────────┐
/// │ Type(1) │ Flags(1)│ Len(2)  │ Session ID (8)                    │
/// └─────────┴─────────┴─────────┴───────────────────────────────────┘
/// ```
pub const HEADER_SIZE: usize = 12;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Global session counter for generating unique session IDs.
/// Upper 32 bits: timestamp at init, lower 32 bits: counter.
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Initialize the session counter with current timestamp.
/// Should be called once at driver startup.
pub fn init_session_counter() {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Upper 32 bits = timestamp, lower 32 bits = 0 (counter starts at 0)
    SESSION_COUNTER.store(ts << 32, Ordering::Relaxed);
}

/// Session identifier for correlating protocol exchanges.
///
/// Format: `[timestamp_seconds:32][counter:32]`
/// - Upper 32 bits: Unix timestamp at driver start (seconds)
/// - Lower 32 bits: Incrementing counter
///
/// This ensures:
/// - No collisions within a driver instance (counter)
/// - No collisions across restarts (timestamp prefix)
/// - Monotonically increasing within a session
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    /// Generate a new unique session ID.
    ///
    /// Thread-safe: uses atomic increment.
    #[must_use]
    pub fn generate() -> Self {
        Self(SESSION_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Raw value for wire serialization.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Extracts the timestamp component (upper 32 bits).
    #[must_use]
    pub const fn timestamp(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Extracts the counter component (lower 32 bits).
    #[must_use]
    pub const fn counter(self) -> u32 {
        self.0 as u32
    }
}

impl From<u64> for SessionId {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Receiver window size in bytes.
///
/// Invariant: Must be > 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiverWindow(u32);

impl ReceiverWindow {
    /// Creates a new receiver window.
    ///
    /// # Panics
    /// Panics if `bytes == 0`.
    #[must_use]
    pub fn new(bytes: u32) -> Self {
        assert!(bytes > 0, "ReceiverWindow must be > 0");
        Self(bytes)
    }

    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<u32> for ReceiverWindow {
    fn from(v: u32) -> Self {
        Self::new(v)
    }
}

/// Maximum transmission unit in bytes.
///
/// Invariant: Must be in range [68, 65535] (minimum IP MTU to max UDP).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mtu(u16);

impl Mtu {
    pub const MIN: u16 = 68;    // Minimum IP MTU
    pub const MAX: u16 = 65535; // Max UDP datagram

    /// Creates a new MTU value.
    ///
    /// # Panics
    /// Panics if value is outside valid range.
    #[must_use]
    pub fn new(bytes: u16) -> Self {
        assert!(
            bytes >= Self::MIN,
            "MTU must be >= {} (minimum IP MTU)",
            Self::MIN
        );
        Self(bytes)
    }

    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

impl From<u16> for Mtu {
    fn from(v: u16) -> Self {
        Self::new(v)
    }
}

/// Consumption offset (bytes consumed by receiver).
///
/// Invariant: Monotonically increasing (caller's responsibility).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsumptionOffset(u64);

impl ConsumptionOffset {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for ConsumptionOffset {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// Reasons for rejecting a subscription (SETUP_NAK).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NakReason {
    /// Requested channel doesn't exist on this driver.
    UnknownChannel = 0x01,
    /// Type hash doesn't match the channel's published type.
    TypeMismatch = 0x02,
    /// Subscriber not authorized to access this channel.
    NotAuthorized = 0x03,
    /// Driver has reached maximum subscription capacity.
    ResourceExhausted = 0x04,
    /// Generic/unknown error.
    Unknown = 0xFF,
}

impl From<u8> for NakReason {
    fn from(v: u8) -> Self {
        match v {
            0x01 => Self::UnknownChannel,
            0x02 => Self::TypeMismatch,
            0x03 => Self::NotAuthorized,
            0x04 => Self::ResourceExhausted,
            _ => Self::Unknown,
        }
    }
}

/// Reasons for teardown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TeardownReason {
    /// Clean shutdown requested by application.
    Normal = 0x00,
    /// Session timed out (no activity).
    Timeout = 0x01,
    /// Internal error occurred.
    Error = 0x02,
    /// Unknown reason.
    Unknown = 0xFF,
}

impl From<u8> for TeardownReason {
    fn from(v: u8) -> Self {
        match v {
            0x00 => Self::Normal,
            0x01 => Self::Timeout,
            0x02 => Self::Error,
            _ => Self::Unknown,
        }
    }
}

/// SETUP frame: subscriber requests subscription to a channel.
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x01, Session ID (generated by subscriber)          │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Channel ID (4 bytes) - which channel subscriber wants            │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Type Hash (8 bytes) - structural hash of expected message type   │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Receiver Window (4 bytes) - initial buffer space in bytes        │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ MTU (2 bytes) - max frame size subscriber can receive            │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetupFrame {
    /// Session ID generated by subscriber for correlation.
    pub session: SessionId,
    /// Channel the subscriber wants to receive.
    pub channel: ChannelId,
    /// Expected type hash for validation.
    pub type_id: TypeId,
    /// Initial receiver window in bytes.
    pub receiver_window: ReceiverWindow,
    /// Maximum transmission unit (max frame size).
    pub mtu: Mtu,
}

/// `SETUP_ACK` frame: publisher accepts the subscription.
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x11, Session ID (echo from SETUP)                  │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Publisher Session ID (8 bytes) - for subscriber to use in SM     │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ First Seq (8 bytes) - first sequence number subscriber expects   │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ MTU (2 bytes) - negotiated (min of both)                         │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetupAckFrame {
    /// Subscriber's session ID (echoed from SETUP).
    pub session: SessionId,
    /// Publisher's session ID for the subscriber to use in Status Messages.
    pub publisher_session: SessionId,
    /// First sequence number the subscriber should expect.
    pub first_seq: u64,
    /// Negotiated MTU (minimum of subscriber and publisher).
    pub mtu: Mtu,
}

/// `SETUP_NAK` frame: publisher rejects the subscription.
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x03, Session ID (echo from SETUP)                  │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Reason (1 byte)                                                  │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetupNakFrame {
    /// Subscriber's session ID (echoed from SETUP).
    pub session: SessionId,
    /// Reason for rejection.
    pub reason: NakReason,
}

/// Status Message: subscriber reports consumption progress and flow control.
///
/// Sent periodically by subscriber to:
/// 1. Report consumption progress (allows publisher to free buffers)
/// 2. Update receiver window (flow control)
/// 3. Act as keepalive
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x04, Session ID (publisher's session)              │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Consumption Offset (8 bytes) - highest contiguous seq consumed   │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Receiver Window (4 bytes) - current buffer space in bytes        │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusMessageFrame {
    /// Publisher's session ID.
    pub session: SessionId,
    /// Highest contiguous sequence number consumed by subscriber.
    pub consumption_offset: ConsumptionOffset,
    /// Current receiver window in bytes.
    pub receiver_window: ReceiverWindow,
}

/// TEARDOWN frame: graceful subscription close.
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x14, Session ID                                    │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Reason (1 byte)                                                  │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeardownFrame {
    /// Session ID of the subscription being closed.
    pub session: SessionId,
    /// Reason for teardown.
    pub reason: TeardownReason,
}

/// HEARTBEAT frame: publisher liveness probe.
///
/// Sent periodically by publisher to:
/// 1. Maintain session liveness
/// 2. Communicate next expected sequence number (for gap detection)
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x15, Session ID (publisher's session)              │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Next Seq (8 bytes) - next sequence number publisher will send    │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatFrame {
    /// Publisher's session ID.
    pub session: SessionId,
    /// Next sequence number the publisher will send.
    /// Subscriber uses this for gap detection during idle periods.
    pub next_seq: u64,
}

/// NAK_BITMAP frame: subscriber requests retransmission of missing data.
///
/// Uses bitmap encoding for efficient representation of gaps.
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x22, Session ID (publisher's session)              │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Channel ID (4 bytes)                                             │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Entry Count (2 bytes)                                            │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Entry 0: Base Seq (8 bytes)                                      │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Entry 0: Bitmap (8 bytes) - bit i: 1=received, 0=missing         │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ ... more entries ...                                             │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NakBitmapFrame {
    /// Publisher's session ID.
    pub session: SessionId,
    /// Channel with missing data.
    pub channel: ChannelId,
    /// Bitmap entries: (base_seq, bitmap) pairs.
    /// Each bitmap covers 64 sequence numbers starting at base_seq.
    /// Bit i = 1 means base_seq + i was received, 0 means missing.
    pub entries: Vec<NakBitmapEntry>,
}

/// A single NAK bitmap entry covering 64 sequence numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NakBitmapEntry {
    /// Starting sequence number for this bitmap.
    pub base_seq: u64,
    /// Bitmap: bit i = 1 means base_seq + i received, 0 means missing.
    pub bitmap: u64,
}

impl NakBitmapEntry {
    /// Returns true if this entry has any missing sequences (any bit is 0).
    #[inline]
    #[must_use]
    pub const fn has_missing(&self) -> bool {
        self.bitmap != u64::MAX
    }

    /// Returns the number of missing sequences in this entry.
    #[must_use]
    pub const fn missing_count(&self) -> u32 {
        64 - self.bitmap.count_ones()
    }

    /// Returns an iterator over the missing sequence numbers in this entry.
    pub fn missing_sequences(&self) -> impl Iterator<Item = u64> + '_ {
        (0..64).filter_map(move |i| {
            if (self.bitmap & (1 << i)) == 0 {
                Some(self.base_seq.wrapping_add(i))
            } else {
                None
            }
        })
    }
}

/// DATA_NOT_AVAIL frame: publisher indicates data is no longer available.
///
/// Sent in response to NAK when requested data has been evicted from
/// the retransmit buffer.
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x23, Session ID (publisher's session)              │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Channel ID (4 bytes)                                             │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Unavailable Start Seq (8 bytes) - first unavailable (inclusive)  │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Unavailable End Seq (8 bytes) - last unavailable (exclusive)     │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Oldest Available (8 bytes) - oldest seq still in buffer          │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataNotAvailFrame {
    /// Publisher's session ID.
    pub session: SessionId,
    /// Channel.
    pub channel: ChannelId,
    /// First unavailable sequence number (inclusive).
    pub unavail_start: u64,
    /// Last unavailable sequence number (exclusive).
    pub unavail_end: u64,
    /// Oldest sequence number still available in retransmit buffer.
    pub oldest_available: u64,
}

/// All protocol frame variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolFrame {
    // Session management
    Setup(SetupFrame),
    SetupAck(SetupAckFrame),
    SetupNak(SetupNakFrame),
    StatusMessage(StatusMessageFrame),
    Teardown(TeardownFrame),
    Heartbeat(HeartbeatFrame),
    // Data plane control
    NakBitmap(NakBitmapFrame),
    DataNotAvail(DataNotAvailFrame),
}

/// Errors during protocol encode/decode.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Buffer too small to decode frame.
    #[error("buffer too small: need {need} bytes, have {have}")]
    BufferTooSmall { need: usize, have: usize },
    /// Unknown frame type.
    #[error("unknown frame type: 0x{0:02x}")]
    UnknownFrameType(u8),
    /// Frame length field doesn't match actual data.
    #[error("invalid frame length")]
    InvalidLength,
}

/// Writer for encoding protocol frames.
struct FrameWriter<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> FrameWriter<'a> {
    fn new(buf: &'a mut Vec<u8>) -> Self {
        buf.clear();
        Self { buf }
    }

    fn put_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    fn put_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn put_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn put_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Write header and return position of length field for patching.
    fn write_header(&mut self, frame_type: u8, flags: u8, session: SessionId) -> usize {
        self.put_u8(frame_type);
        self.put_u8(flags);
        let len_pos = self.buf.len();
        self.put_u16(0); // placeholder for length
        self.put_u64(session.as_u64());
        len_pos
    }

    /// Patch the length field after writing payload.
    fn patch_length(&mut self, len_pos: usize) {
        let total_len = self.buf.len();
        let len_bytes = (total_len as u16).to_le_bytes();
        self.buf[len_pos] = len_bytes[0];
        self.buf[len_pos + 1] = len_bytes[1];
    }
}

/// Reader for decoding protocol frames.
struct FrameReader<'a> {
    buf: &'a [u8],
    cursor: usize,
}

impl<'a> FrameReader<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self { buf, cursor: 0 }
    }

    const fn remaining(&self) -> usize {
        self.buf.len() - self.cursor
    }

    fn take_u8(&mut self) -> Result<u8, ProtocolError> {
        if self.remaining() < 1 {
            return Err(ProtocolError::BufferTooSmall {
                need: 1,
                have: self.remaining(),
            });
        }
        let v = self.buf[self.cursor];
        self.cursor += 1;
        Ok(v)
    }

    fn take_u16(&mut self) -> Result<u16, ProtocolError> {
        if self.remaining() < 2 {
            return Err(ProtocolError::BufferTooSmall {
                need: 2,
                have: self.remaining(),
            });
        }
        let mut arr = [0u8; 2];
        arr.copy_from_slice(&self.buf[self.cursor..self.cursor + 2]);
        self.cursor += 2;
        Ok(u16::from_le_bytes(arr))
    }

    fn take_u32(&mut self) -> Result<u32, ProtocolError> {
        if self.remaining() < 4 {
            return Err(ProtocolError::BufferTooSmall {
                need: 4,
                have: self.remaining(),
            });
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&self.buf[self.cursor..self.cursor + 4]);
        self.cursor += 4;
        Ok(u32::from_le_bytes(arr))
    }

    fn take_u64(&mut self) -> Result<u64, ProtocolError> {
        if self.remaining() < 8 {
            return Err(ProtocolError::BufferTooSmall {
                need: 8,
                have: self.remaining(),
            });
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.buf[self.cursor..self.cursor + 8]);
        self.cursor += 8;
        Ok(u64::from_le_bytes(arr))
    }
}

/// Encode a protocol frame into the buffer.
///
/// The buffer is cleared and reused (preserves capacity).
///
/// # Errors
/// Currently infallible but returns `Result` for future extensibility.
pub fn encode_frame(frame: &ProtocolFrame, buf: &mut Vec<u8>) -> Result<(), ProtocolError> {
    let mut w = FrameWriter::new(buf);

    match frame {
        ProtocolFrame::Setup(f) => {
            let len_pos = w.write_header(frame_type::SETUP, 0, f.session);
            w.put_u32(f.channel.into());
            w.put_u64(f.type_id.into());
            w.put_u32(f.receiver_window.as_u32());
            w.put_u16(f.mtu.as_u16());
            w.patch_length(len_pos);
        }
        ProtocolFrame::SetupAck(f) => {
            let len_pos = w.write_header(frame_type::SETUP_ACK, 0, f.session);
            w.put_u64(f.publisher_session.as_u64());
            w.put_u64(f.first_seq);
            w.put_u16(f.mtu.as_u16());
            w.patch_length(len_pos);
        }
        ProtocolFrame::SetupNak(f) => {
            let len_pos = w.write_header(frame_type::SETUP_NAK, 0, f.session);
            w.put_u8(f.reason as u8);
            w.patch_length(len_pos);
        }
        ProtocolFrame::StatusMessage(f) => {
            let len_pos = w.write_header(frame_type::STATUS_MESSAGE, 0, f.session);
            w.put_u64(f.consumption_offset.as_u64());
            w.put_u32(f.receiver_window.as_u32());
            w.patch_length(len_pos);
        }
        ProtocolFrame::Teardown(f) => {
            let len_pos = w.write_header(frame_type::TEARDOWN, 0, f.session);
            w.put_u8(f.reason as u8);
            w.patch_length(len_pos);
        }
        ProtocolFrame::Heartbeat(f) => {
            let len_pos = w.write_header(frame_type::HEARTBEAT, 0, f.session);
            w.put_u64(f.next_seq);
            w.patch_length(len_pos);
        }
        ProtocolFrame::NakBitmap(f) => {
            let len_pos = w.write_header(frame_type::NAK_BITMAP, 0, f.session);
            w.put_u32(f.channel.into());
            w.put_u16(f.entries.len() as u16);
            w.put_u16(0); // reserved
            for entry in &f.entries {
                w.put_u64(entry.base_seq);
                w.put_u64(entry.bitmap);
            }
            w.patch_length(len_pos);
        }
        ProtocolFrame::DataNotAvail(f) => {
            let len_pos = w.write_header(frame_type::DATA_NOT_AVAIL, 0, f.session);
            w.put_u32(f.channel.into());
            w.put_u64(f.unavail_start);
            w.put_u64(f.unavail_end);
            w.put_u64(f.oldest_available);
            w.patch_length(len_pos);
        }
    }

    Ok(())
}

// ============================================================================
// Convenience encode functions for specific frame types
// ============================================================================

/// Encodes a HEARTBEAT frame directly.
///
/// # Errors
/// Currently infallible but returns `Result` for consistency.
pub fn encode_heartbeat(frame: &HeartbeatFrame, buf: &mut Vec<u8>) -> Result<(), ProtocolError> {
    encode_frame(&ProtocolFrame::Heartbeat(frame.clone()), buf)
}

/// Encodes a DATA_NOT_AVAIL frame directly.
///
/// # Errors
/// Currently infallible but returns `Result` for consistency.
pub fn encode_data_not_avail(
    frame: &DataNotAvailFrame,
    buf: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    encode_frame(&ProtocolFrame::DataNotAvail(frame.clone()), buf)
}

/// Encodes a NAK_BITMAP frame directly.
///
/// # Errors
/// Currently infallible but returns `Result` for consistency.
pub fn encode_nak_bitmap(frame: &NakBitmapFrame, buf: &mut Vec<u8>) -> Result<(), ProtocolError> {
    encode_frame(&ProtocolFrame::NakBitmap(frame.clone()), buf)
}

/// Encodes a STATUS_MSG frame directly.
///
/// # Errors
/// Currently infallible but returns `Result` for consistency.
pub fn encode_status_message(
    frame: &StatusMessageFrame,
    buf: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    encode_frame(&ProtocolFrame::StatusMessage(frame.clone()), buf)
}

/// Decode a protocol frame from bytes.
///
/// # Errors
/// Returns `ProtocolError` if the buffer is malformed or too small.
pub fn decode_frame(bytes: &[u8]) -> Result<ProtocolFrame, ProtocolError> {
    let mut r = FrameReader::new(bytes);

    // Read header (12 bytes)
    let frame_type = r.take_u8()?;
    let _flags = r.take_u8()?;
    let _len = r.take_u16()?;
    let session = SessionId::from(r.take_u64()?);

    match frame_type {
        frame_type::SETUP => {
            let channel = ChannelId::from(r.take_u32()?);
            let type_id = TypeId::from(r.take_u64()?);
            let receiver_window = ReceiverWindow::from(r.take_u32()?);
            let mtu = Mtu::from(r.take_u16()?);
            Ok(ProtocolFrame::Setup(SetupFrame {
                session,
                channel,
                type_id,
                receiver_window,
                mtu,
            }))
        }
        frame_type::SETUP_ACK => {
            let publisher_session = SessionId::from(r.take_u64()?);
            let first_seq = r.take_u64()?;
            let mtu = Mtu::from(r.take_u16()?);
            Ok(ProtocolFrame::SetupAck(SetupAckFrame {
                session,
                publisher_session,
                first_seq,
                mtu,
            }))
        }
        frame_type::SETUP_NAK => {
            let reason = NakReason::from(r.take_u8()?);
            Ok(ProtocolFrame::SetupNak(SetupNakFrame { session, reason }))
        }
        frame_type::STATUS_MESSAGE => {
            let consumption_offset = ConsumptionOffset::from(r.take_u64()?);
            let receiver_window = ReceiverWindow::from(r.take_u32()?);
            Ok(ProtocolFrame::StatusMessage(StatusMessageFrame {
                session,
                consumption_offset,
                receiver_window,
            }))
        }
        frame_type::TEARDOWN => {
            let reason = TeardownReason::from(r.take_u8()?);
            Ok(ProtocolFrame::Teardown(TeardownFrame { session, reason }))
        }
        frame_type::HEARTBEAT => {
            let next_seq = r.take_u64()?;
            Ok(ProtocolFrame::Heartbeat(HeartbeatFrame { session, next_seq }))
        }
        frame_type::NAK_BITMAP => {
            let channel = ChannelId::from(r.take_u32()?);
            let entry_count = r.take_u16()? as usize;
            let _reserved = r.take_u16()?;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let base_seq = r.take_u64()?;
                let bitmap = r.take_u64()?;
                entries.push(NakBitmapEntry { base_seq, bitmap });
            }
            Ok(ProtocolFrame::NakBitmap(NakBitmapFrame {
                session,
                channel,
                entries,
            }))
        }
        frame_type::DATA_NOT_AVAIL => {
            let channel = ChannelId::from(r.take_u32()?);
            let unavail_start = r.take_u64()?;
            let unavail_end = r.take_u64()?;
            let oldest_available = r.take_u64()?;
            Ok(ProtocolFrame::DataNotAvail(DataNotAvailFrame {
                session,
                channel,
                unavail_start,
                unavail_end,
                oldest_available,
            }))
        }
        other => Err(ProtocolError::UnknownFrameType(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_setup() {
        let frame = ProtocolFrame::Setup(SetupFrame {
            session: SessionId::from(0x1234_5678_9ABC_DEF0),
            channel: ChannelId::from(42),
            type_id: TypeId::from(0xDEAD_BEEF_CAFE_BABE),
            receiver_window: ReceiverWindow::new(128 * 1024),
            mtu: Mtu::new(1500),
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn roundtrip_setup_ack() {
        let frame = ProtocolFrame::SetupAck(SetupAckFrame {
            session: SessionId::from(0x1111_1111_1111_1111),
            publisher_session: SessionId::from(0x2222_2222_2222_2222),
            first_seq: 12345,
            mtu: Mtu::new(1400),
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn roundtrip_setup_nak() {
        let frame = ProtocolFrame::SetupNak(SetupNakFrame {
            session: SessionId::from(0xAAAA_AAAA_AAAA_AAAA),
            reason: NakReason::TypeMismatch,
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn roundtrip_status_message() {
        let frame = ProtocolFrame::StatusMessage(StatusMessageFrame {
            session: SessionId::from(0xBBBB_BBBB_BBBB_BBBB),
            consumption_offset: ConsumptionOffset::from(999_999),
            receiver_window: ReceiverWindow::new(64 * 1024),
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn roundtrip_teardown() {
        let frame = ProtocolFrame::Teardown(TeardownFrame {
            session: SessionId::from(0xCCCC_CCCC_CCCC_CCCC),
            reason: TeardownReason::Normal,
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn roundtrip_heartbeat() {
        let frame = ProtocolFrame::Heartbeat(HeartbeatFrame {
            session: SessionId::from(0xDDDD_DDDD_DDDD_DDDD),
            next_seq: 42_000,
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn roundtrip_nak_bitmap() {
        let frame = ProtocolFrame::NakBitmap(NakBitmapFrame {
            session: SessionId::from(0xEEEE_EEEE_EEEE_EEEE),
            channel: ChannelId::from(7),
            entries: vec![
                NakBitmapEntry {
                    base_seq: 100,
                    bitmap: 0b1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_0101,
                },
                NakBitmapEntry {
                    base_seq: 200,
                    bitmap: 0b1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_0000_0000,
                },
            ],
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn roundtrip_data_not_avail() {
        let frame = ProtocolFrame::DataNotAvail(DataNotAvailFrame {
            session: SessionId::from(0xFFFF_FFFF_FFFF_FFFF),
            channel: ChannelId::from(99),
            unavail_start: 500,
            unavail_end: 600,
            oldest_available: 600,
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn nak_bitmap_missing_sequences() {
        // Bitmap: bit 0 = 1 (received), bit 1 = 0 (missing), bit 2 = 1, bit 3 = 0
        let entry = NakBitmapEntry {
            base_seq: 100,
            bitmap: 0b0101, // bits 0,2 set (received), bits 1,3 missing
        };
        let missing: Vec<u64> = entry.missing_sequences().take(10).collect();
        // All bits 4-63 are 0 (missing), plus bits 1 and 3
        assert!(missing.contains(&101)); // bit 1 = 0
        assert!(missing.contains(&103)); // bit 3 = 0
        assert!(!missing.contains(&100)); // bit 0 = 1
        assert!(!missing.contains(&102)); // bit 2 = 1
    }

    #[test]
    fn decode_empty_buffer() {
        let result = decode_frame(&[]);
        assert!(matches!(
            result,
            Err(ProtocolError::BufferTooSmall { need: 1, .. })
        ));
    }

    #[test]
    fn decode_unknown_frame_type() {
        // Valid 12-byte header but unknown type
        let bytes = [0xFF, 0x00, 0x0C, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(ProtocolError::UnknownFrameType(0xFF))));
    }

    #[test]
    fn nak_reason_roundtrip() {
        assert_eq!(NakReason::from(0x01), NakReason::UnknownChannel);
        assert_eq!(NakReason::from(0x02), NakReason::TypeMismatch);
        assert_eq!(NakReason::from(0x03), NakReason::NotAuthorized);
        assert_eq!(NakReason::from(0x04), NakReason::ResourceExhausted);
        assert_eq!(NakReason::from(0x99), NakReason::Unknown);
    }

    #[test]
    fn teardown_reason_roundtrip() {
        assert_eq!(TeardownReason::from(0x00), TeardownReason::Normal);
        assert_eq!(TeardownReason::from(0x01), TeardownReason::Timeout);
        assert_eq!(TeardownReason::from(0x02), TeardownReason::Error);
        assert_eq!(TeardownReason::from(0x99), TeardownReason::Unknown);
    }

    #[test]
    fn session_id_generate_increments() {
        init_session_counter();
        let s1 = SessionId::generate();
        let s2 = SessionId::generate();
        // Counter should increment
        assert_eq!(s2.counter(), s1.counter() + 1);
        // Timestamp should be the same (within same second)
        assert_eq!(s2.timestamp(), s1.timestamp());
    }

    #[test]
    fn header_size_is_12() {
        // Verify header size matches spec
        assert_eq!(HEADER_SIZE, 12);

        // Encode a minimal frame and check header
        let frame = ProtocolFrame::Teardown(TeardownFrame {
            session: SessionId::from(0),
            reason: TeardownReason::Normal,
        });
        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();

        // Header (12) + reason (1) = 13
        assert_eq!(buf.len(), 13);
    }
}
