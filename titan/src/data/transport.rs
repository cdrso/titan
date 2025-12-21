//! Network transport framing for data-plane traffic.
//!
//! ## Wire Format
//!
//! All multi-byte integers are little-endian. The discriminant is 4 bytes for alignment.
//!
//! | Message   | Layout |
//! |-----------|--------|
//! | Data      | `[tag:4][channel:4][seq:8][timestamp:8][len:2][payload:len]` |
//! | Ack       | `[tag:4][channel:4][seq:8][timestamp:8]` |
//! | Nack      | `[tag:4][channel:4][seq:8]` |
//! | Heartbeat | `[tag:4][channel:4][seq:8][timestamp:8]` |

use crate::data::{
    Frame, FrameError,
    types::{DataChannelId, MonoTimestamp, SeqNum},
};
use thiserror::Error;

// Wire format constants
const TAG_DATA: u32 = 0;
const TAG_ACK: u32 = 1;
const TAG_NACK: u32 = 2;
const TAG_HEARTBEAT: u32 = 3;

/// Writer for encoding messages into a byte buffer.
struct MessageWriter<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> MessageWriter<'a> {
    fn new(buf: &'a mut Vec<u8>) -> Self {
        buf.clear();
        Self { buf }
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

    fn put_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }
}

/// Reader for decoding messages from a byte buffer.
struct MessageReader<'a> {
    buf: &'a [u8],
    cursor: usize,
}

impl<'a> MessageReader<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self { buf, cursor: 0 }
    }

    fn take_u16(&mut self) -> Result<u16, TransportError> {
        if self.cursor + 2 > self.buf.len() {
            return Err(TransportError::BufferTooSmall);
        }
        let mut arr = [0u8; 2];
        arr.copy_from_slice(&self.buf[self.cursor..self.cursor + 2]);
        self.cursor += 2;
        Ok(u16::from_le_bytes(arr))
    }

    fn take_u32(&mut self) -> Result<u32, TransportError> {
        if self.cursor + 4 > self.buf.len() {
            return Err(TransportError::BufferTooSmall);
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&self.buf[self.cursor..self.cursor + 4]);
        self.cursor += 4;
        Ok(u32::from_le_bytes(arr))
    }

    fn take_u64(&mut self) -> Result<u64, TransportError> {
        if self.cursor + 8 > self.buf.len() {
            return Err(TransportError::BufferTooSmall);
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.buf[self.cursor..self.cursor + 8]);
        self.cursor += 8;
        Ok(u64::from_le_bytes(arr))
    }

    fn take_bytes(&mut self, len: usize) -> Result<&'a [u8], TransportError> {
        if self.cursor + len > self.buf.len() {
            return Err(TransportError::InvalidLength);
        }
        let slice = &self.buf[self.cursor..self.cursor + len];
        self.cursor += len;
        Ok(slice)
    }
}

/// Errors during transport encode/decode.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Input buffer too short to decode the message.
    #[error("input buffer too small")]
    BufferTooSmall,
    /// Frame construction or validation failed.
    #[error("frame error: {0}")]
    Frame(FrameError),
    /// Unknown message type tag.
    #[error("invalid discriminant {0}")]
    InvalidDiscriminant(u32),
    /// Payload length field inconsistent with buffer size.
    #[error("invalid length")]
    InvalidLength,
}

/// Data-plane message sent over the network.
#[derive(Debug)]
pub enum DataPlaneMessage<const N: usize> {
    /// Application data for a specific channel.
    Data {
        channel: DataChannelId,
        seq: SeqNum,
        send_timestamp: MonoTimestamp,
        frame: Frame<N>,
    },
    /// Acknowledgment for received data (could be cumulative or selective).
    Ack {
        channel: DataChannelId,
        seq: SeqNum,
        echo_timestamp: MonoTimestamp,
    },
    /// Negative acknowledgment / gap report.
    Nack { channel: DataChannelId, seq: SeqNum },
    /// Keepalive or flow-control heartbeat.
    Heartbeat {
        channel: DataChannelId,
        next_expected: SeqNum,
        echo_timestamp: MonoTimestamp,
    },
}

impl From<FrameError> for TransportError {
    fn from(e: FrameError) -> Self {
        Self::Frame(e)
    }
}

/// Encode a message into `buf`.
///
/// Caller should reuse a preallocated `buf` per thread to amortize the heap allocation
/// (`Vec::clear()` preserves capacity). TODO we need to look into this when the e2e flow is in place
///
/// # Errors
/// Returns [`TransportError::InvalidLength`] if the frame length exceeds `u16::MAX`.
pub fn encode_message<const N: usize>(
    msg: &DataPlaneMessage<N>,
    buf: &mut Vec<u8>,
) -> Result<(), TransportError> {
    let mut w = MessageWriter::new(buf);

    match msg {
        DataPlaneMessage::Data {
            channel,
            seq,
            send_timestamp,
            frame,
        } => {
            w.put_u32(TAG_DATA);
            w.put_u32(u32::from(*channel));
            w.put_u64(seq.0);
            w.put_u64(send_timestamp.as_u64());
            let len = u16::try_from(frame.len()).map_err(|_| TransportError::InvalidLength)?;
            w.put_u16(len);
            w.put_bytes(frame.payload());
        }
        DataPlaneMessage::Ack {
            channel,
            seq,
            echo_timestamp,
        } => {
            w.put_u32(TAG_ACK);
            w.put_u32(u32::from(*channel));
            w.put_u64(seq.0);
            w.put_u64(echo_timestamp.as_u64());
        }
        DataPlaneMessage::Nack { channel, seq } => {
            w.put_u32(TAG_NACK);
            w.put_u32(u32::from(*channel));
            w.put_u64(seq.0);
        }
        DataPlaneMessage::Heartbeat {
            channel,
            next_expected,
            echo_timestamp,
        } => {
            w.put_u32(TAG_HEARTBEAT);
            w.put_u32(u32::from(*channel));
            w.put_u64(next_expected.0);
            w.put_u64(echo_timestamp.as_u64());
        }
    }
    Ok(())
}

/// Decode a message from `bytes`.
///
/// # Errors
/// - [`TransportError::BufferTooSmall`] if `bytes` is too short for the message type
/// - [`TransportError::InvalidDiscriminant`] if the tag byte is unrecognized
/// - [`TransportError::InvalidLength`] if the payload length field is inconsistent
/// - [`TransportError::Frame`] if frame construction fails
pub fn decode_message<const N: usize>(bytes: &[u8]) -> Result<DataPlaneMessage<N>, TransportError> {
    let mut r = MessageReader::new(bytes);

    let tag = r.take_u32()?;

    match tag {
        TAG_DATA => {
            let channel = DataChannelId::from(r.take_u32()?);
            let seq = SeqNum(r.take_u64()?);
            let send_timestamp = MonoTimestamp::new(r.take_u64()?);
            let len = r.take_u16()? as usize;
            let payload = r.take_bytes(len)?;
            let frame: Frame<N> = payload.try_into()?;
            Ok(DataPlaneMessage::Data {
                channel,
                seq,
                send_timestamp,
                frame,
            })
        }
        TAG_ACK => {
            let channel = DataChannelId::from(r.take_u32()?);
            let seq = SeqNum(r.take_u64()?);
            let echo_timestamp = MonoTimestamp::new(r.take_u64()?);
            Ok(DataPlaneMessage::Ack {
                channel,
                seq,
                echo_timestamp,
            })
        }
        TAG_NACK => {
            let channel = DataChannelId::from(r.take_u32()?);
            let seq = SeqNum(r.take_u64()?);
            Ok(DataPlaneMessage::Nack { channel, seq })
        }
        TAG_HEARTBEAT => {
            let channel = DataChannelId::from(r.take_u32()?);
            let next_expected = SeqNum(r.take_u64()?);
            let echo_timestamp = MonoTimestamp::new(r.take_u64()?);
            Ok(DataPlaneMessage::Heartbeat {
                channel,
                next_expected,
                echo_timestamp,
            })
        }
        other => Err(TransportError::InvalidDiscriminant(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(data: &[u8]) -> Frame<64> {
        data.try_into().unwrap()
    }

    #[test]
    fn roundtrip_data() {
        let msg = DataPlaneMessage::Data {
            channel: DataChannelId::from(42),
            seq: SeqNum(100),
            send_timestamp: MonoTimestamp::new(123456),
            frame: make_frame(&[1, 2, 3, 4]),
        };
        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::Data {
                channel,
                seq,
                send_timestamp,
                frame,
            } => {
                assert_eq!(u32::from(channel), 42);
                assert_eq!(seq.0, 100);
                assert_eq!(send_timestamp.as_u64(), 123456);
                assert_eq!(frame.payload(), &[1, 2, 3, 4]);
            }
            _ => panic!("expected Data"),
        }
    }

    #[test]
    fn roundtrip_ack() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::Ack {
            channel: DataChannelId::from(7),
            seq: SeqNum(999),
            echo_timestamp: MonoTimestamp::new(555),
        };
        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::Ack {
                channel,
                seq,
                echo_timestamp,
            } => {
                assert_eq!(u32::from(channel), 7);
                assert_eq!(seq.0, 999);
                assert_eq!(echo_timestamp.as_u64(), 555);
            }
            _ => panic!("expected Ack"),
        }
    }

    #[test]
    fn roundtrip_nack() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::Nack {
            channel: DataChannelId::from(3),
            seq: SeqNum(50),
        };
        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::Nack { channel, seq } => {
                assert_eq!(u32::from(channel), 3);
                assert_eq!(seq.0, 50);
            }
            _ => panic!("expected Nack"),
        }
    }

    #[test]
    fn roundtrip_heartbeat() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::Heartbeat {
            channel: DataChannelId::from(1),
            next_expected: SeqNum(200),
            echo_timestamp: MonoTimestamp::new(999999),
        };
        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::Heartbeat {
                channel,
                next_expected,
                echo_timestamp,
            } => {
                assert_eq!(u32::from(channel), 1);
                assert_eq!(next_expected.0, 200);
                assert_eq!(echo_timestamp.as_u64(), 999999);
            }
            _ => panic!("expected Heartbeat"),
        }
    }

    #[test]
    fn decode_empty_buffer() {
        let result: Result<DataPlaneMessage<64>, _> = decode_message(&[]);
        assert!(matches!(result, Err(TransportError::BufferTooSmall)));
    }

    #[test]
    fn decode_invalid_discriminant() {
        // 4-byte discriminant with invalid value
        let result: Result<DataPlaneMessage<64>, _> = decode_message(&[99, 0, 0, 0]);
        assert!(matches!(
            result,
            Err(TransportError::InvalidDiscriminant(99))
        ));
    }

    #[test]
    fn decode_truncated_data() {
        // Valid discriminant (4 bytes) but not enough bytes for Data header
        let result: Result<DataPlaneMessage<64>, _> = decode_message(&[0, 0, 0, 0, 1, 2]);
        assert!(matches!(result, Err(TransportError::BufferTooSmall)));
    }

    #[test]
    fn decode_data_invalid_payload_length() {
        // Encode a valid Data message, then corrupt the length field
        let msg = DataPlaneMessage::Data {
            channel: DataChannelId::from(1),
            seq: SeqNum(1),
            send_timestamp: MonoTimestamp::new(1),
            frame: make_frame(&[1, 2]),
        };
        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        // Corrupt length field to claim more payload than exists
        // Wire format: [tag:4][channel:4][seq:8][timestamp:8][len:2][payload]
        let len_offset = 4 + 4 + 8 + 8; // tag + channel + seq + timestamp
        buf[len_offset] = 0xFF;
        buf[len_offset + 1] = 0xFF;

        let result: Result<DataPlaneMessage<64>, _> = decode_message(&buf);
        assert!(matches!(result, Err(TransportError::InvalidLength)));
    }

    #[test]
    fn encode_reuses_buffer_capacity() {
        let mut buf = Vec::with_capacity(1024);
        let msg: DataPlaneMessage<64> = DataPlaneMessage::Nack {
            channel: DataChannelId::from(1),
            seq: SeqNum(1),
        };

        encode_message(&msg, &mut buf).unwrap();
        assert!(buf.capacity() >= 1024); // capacity preserved

        encode_message(&msg, &mut buf).unwrap();
        assert!(buf.capacity() >= 1024); // still preserved after clear
    }

    #[test]
    fn roundtrip_empty_frame() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::Data {
            channel: DataChannelId::from(0),
            seq: SeqNum::ZERO,
            send_timestamp: MonoTimestamp::new(0),
            frame: Frame::new(),
        };
        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::Data { frame, .. } => {
                assert!(frame.is_empty());
            }
            _ => panic!("expected Data"),
        }
    }
}
