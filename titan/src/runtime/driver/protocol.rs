//! Driver-to-driver wire protocol for subscription establishment and flow control.
//!
//! This module defines the frames exchanged between drivers for:
//! - Subscription setup with type validation
//! - Flow control via Status Messages
//! - Graceful teardown
//!
//! # Wire Format
//!
//! All frames share a common 8-byte header:
//!
//! ```text
//! ┌─────────┬─────────┬─────────┬───────────────────────────────────┐
//! │ Type(1) │ Flags(1)│ Len(2)  │ Session ID (4)                    │
//! └─────────┴─────────┴─────────┴───────────────────────────────────┘
//! ```
//!
//! Frame types:
//! - 0x01 = SETUP (subscriber → publisher)
//! - 0x02 = SETUP_ACK (publisher → subscriber)
//! - 0x03 = SETUP_NAK (publisher → subscriber)
//! - 0x04 = SM - Status Message (subscriber → publisher)
//! - 0x05 = TEARDOWN (either direction)
//!
//! Note: DATA frames use the existing [`DataPlaneMessage`] format from
//! [`crate::data::transport`] - they don't go through this protocol layer.
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
    pub const fn from_first_byte(byte: u8) -> Self {
        if byte >= 0x10 {
            Self::Protocol(byte)
        } else {
            Self::Data(byte)
        }
    }

    /// Returns the raw type byte.
    #[inline]
    pub const fn type_byte(self) -> u8 {
        match self {
            Self::Data(b) | Self::Protocol(b) => b,
        }
    }
}

/// Protocol frame type discriminants.
///
/// These start at 0x10 to avoid collision with data plane message tags (0x00-0x03).
/// This allows the RX thread to quickly distinguish protocol frames from data frames
/// by checking if the first byte is >= 0x10.
pub mod frame_type {
    use super::FrameKind;

    pub const SETUP: u8 = 0x10;
    pub const SETUP_ACK: u8 = 0x11;
    pub const SETUP_NAK: u8 = 0x12;
    pub const STATUS_MESSAGE: u8 = 0x13;
    pub const TEARDOWN: u8 = 0x14;

    /// Classify frame type from first byte.
    #[inline]
    pub const fn classify(first_byte: u8) -> FrameKind {
        FrameKind::from_first_byte(first_byte)
    }
}

/// Common header for all protocol frames.
///
/// ```text
/// ┌─────────┬─────────┬─────────┬───────────────────────────────────┐
/// │ Type(1) │ Flags(1)│ Len(2)  │ Session ID (4)                    │
/// └─────────┴─────────┴─────────┴───────────────────────────────────┘
/// ```
pub const HEADER_SIZE: usize = 8;

/// Session identifier for correlating protocol exchanges.
///
/// Generated randomly by the subscriber when initiating SETUP.
/// Echoed by the publisher in SETUP_ACK/SETUP_NAK.
///
/// Invariant: Opaque. Generated via `SessionId::generate()`, never user-constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u32);

impl SessionId {
    /// Generate a new random session ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(rand::random())
    }

    /// Raw value for wire serialization.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<u32> for SessionId {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}", self.0)
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
            bytes >= Self::MIN && bytes <= Self::MAX,
            "MTU must be in range [{}, {}]",
            Self::MIN,
            Self::MAX
        );
        Self(bytes)
    }

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

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for ConsumptionOffset {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// Reasons for rejecting a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NakReason {
    /// Type hash doesn't match the channel's published type.
    TypeMismatch = 0x01,
    /// Requested channel doesn't exist on this driver.
    ChannelNotFound = 0x02,
    /// Driver has reached maximum subscription capacity.
    CapacityExceeded = 0x03,
    /// Generic/unknown error.
    Unknown = 0xFF,
}

impl From<u8> for NakReason {
    fn from(v: u8) -> Self {
        match v {
            0x01 => Self::TypeMismatch,
            0x02 => Self::ChannelNotFound,
            0x03 => Self::CapacityExceeded,
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

/// SETUP_ACK frame: publisher accepts the subscription.
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────────┐
/// │ Header: Type=0x02, Session ID (echo from SETUP)                  │
/// ├──────────────────────────────────────────────────────────────────┤
/// │ Publisher Session ID (4 bytes) - for subscriber to use in SM     │
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
    /// Negotiated MTU (minimum of subscriber and publisher).
    pub mtu: Mtu,
}

/// SETUP_NAK frame: publisher rejects the subscription.
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
/// │ Header: Type=0x05, Session ID                                    │
/// └──────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeardownFrame {
    /// Session ID of the subscription being closed.
    pub session: SessionId,
}

/// All protocol frame variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolFrame {
    Setup(SetupFrame),
    SetupAck(SetupAckFrame),
    SetupNak(SetupNakFrame),
    StatusMessage(StatusMessageFrame),
    Teardown(TeardownFrame),
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
        self.put_u32(session.as_u32());
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

    fn remaining(&self) -> usize {
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
            w.put_u32(f.publisher_session.as_u32());
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
            w.patch_length(len_pos);
        }
    }

    Ok(())
}

/// Decode a protocol frame from bytes.
pub fn decode_frame(bytes: &[u8]) -> Result<ProtocolFrame, ProtocolError> {
    let mut r = FrameReader::new(bytes);

    // Read header
    let frame_type = r.take_u8()?;
    let _flags = r.take_u8()?;
    let _len = r.take_u16()?;
    let session = SessionId::from(r.take_u32()?);

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
            let publisher_session = SessionId::from(r.take_u32()?);
            let mtu = Mtu::from(r.take_u16()?);
            Ok(ProtocolFrame::SetupAck(SetupAckFrame {
                session,
                publisher_session,
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
        frame_type::TEARDOWN => Ok(ProtocolFrame::Teardown(TeardownFrame { session })),
        other => Err(ProtocolError::UnknownFrameType(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_setup() {
        let frame = ProtocolFrame::Setup(SetupFrame {
            session: SessionId::from(0x12345678),
            channel: ChannelId::from(42),
            type_id: TypeId::from(0xDEADBEEF_CAFEBABE),
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
            session: SessionId::from(0x11111111),
            publisher_session: SessionId::from(0x22222222),
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
            session: SessionId::from(0xAAAAAAAA),
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
            session: SessionId::from(0xBBBBBBBB),
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
            session: SessionId::from(0xCCCCCCCC),
        });

        let mut buf = Vec::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf).unwrap();

        assert_eq!(frame, decoded);
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
        // Valid header but unknown type
        let bytes = [0xFF, 0x00, 0x08, 0x00, 0x01, 0x02, 0x03, 0x04];
        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(ProtocolError::UnknownFrameType(0xFF))));
    }

    #[test]
    fn nak_reason_roundtrip() {
        assert_eq!(NakReason::from(0x01), NakReason::TypeMismatch);
        assert_eq!(NakReason::from(0x02), NakReason::ChannelNotFound);
        assert_eq!(NakReason::from(0x03), NakReason::CapacityExceeded);
        assert_eq!(NakReason::from(0x99), NakReason::Unknown);
    }

    #[test]
    fn session_id_generate_is_random() {
        let s1 = SessionId::generate();
        let s2 = SessionId::generate();
        // Very unlikely to be equal
        assert_ne!(s1, s2);
    }
}
