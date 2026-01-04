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
const TAG_DATA_FRAG: u32 = 4;

/// Transport header size for DATA frames: tag(4) + channel(4) + seq(8) + timestamp(8) + len(2) = 26 bytes
pub const DATA_HEADER_SIZE: usize = 4 + 4 + 8 + 8 + 2;

/// Transport header size for DATA_FRAG frames: tag(4) + channel(4) + seq(8) + timestamp(8) + frag_idx(2) + frag_count(2) + len(2) = 30 bytes
pub const DATA_FRAG_HEADER_SIZE: usize = 4 + 4 + 8 + 8 + 2 + 2 + 2;

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
    /// Fragmented application data.
    DataFrag {
        channel: DataChannelId,
        seq: SeqNum,
        send_timestamp: MonoTimestamp,
        /// Fragment index (0-based).
        frag_index: u16,
        /// Total number of fragments.
        frag_count: u16,
        /// Fragment payload (subset of original message).
        payload: Frame<N>,
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

/// Encode a DATA frame directly from references (zero intermediate copies).
///
/// This is the hot-path optimized version that avoids copying the frame into
/// a `DataPlaneMessage` struct. Use this in TX thread's drain loop.
///
/// # Arguments
/// * `channel` - Channel ID
/// * `seq` - Sequence number
/// * `send_timestamp` - Send timestamp
/// * `frame` - Frame reference (not copied)
/// * `buf` - Pre-allocated output buffer
///
/// # Errors
/// Returns [`TransportError::InvalidLength`] if the frame length exceeds `u16::MAX`.
pub fn encode_data_frame<const N: usize>(
    channel: DataChannelId,
    seq: SeqNum,
    send_timestamp: MonoTimestamp,
    frame: &Frame<N>,
    buf: &mut Vec<u8>,
) -> Result<(), TransportError> {
    let mut w = MessageWriter::new(buf);
    w.put_u32(TAG_DATA);
    w.put_u32(u32::from(channel));
    w.put_u64(seq.as_u64());
    w.put_u64(send_timestamp.units());
    let len = u16::try_from(frame.len()).map_err(|_| TransportError::InvalidLength)?;
    w.put_u16(len);
    w.put_bytes(frame.payload());
    Ok(())
}

/// Encode a DATA_FRAG frame directly from references (zero intermediate copies).
///
/// This encodes a single fragment of a larger message.
///
/// # Arguments
/// * `channel` - Channel ID
/// * `seq` - Message sequence number (same for all fragments of one message)
/// * `send_timestamp` - Send timestamp
/// * `frag_index` - Fragment index (0-based)
/// * `frag_count` - Total fragment count
/// * `payload` - Fragment payload bytes (slice of original payload)
/// * `buf` - Pre-allocated output buffer
///
/// # Errors
/// Returns [`TransportError::InvalidLength`] if the payload length exceeds `u16::MAX`.
pub fn encode_data_frag(
    channel: DataChannelId,
    seq: SeqNum,
    send_timestamp: MonoTimestamp,
    frag_index: u16,
    frag_count: u16,
    payload: &[u8],
    buf: &mut Vec<u8>,
) -> Result<(), TransportError> {
    let mut w = MessageWriter::new(buf);
    w.put_u32(TAG_DATA_FRAG);
    w.put_u32(u32::from(channel));
    w.put_u64(seq.as_u64());
    w.put_u64(send_timestamp.units());
    w.put_u16(frag_index);
    w.put_u16(frag_count);
    let len = u16::try_from(payload.len()).map_err(|_| TransportError::InvalidLength)?;
    w.put_u16(len);
    w.put_bytes(payload);
    Ok(())
}

/// Decoded fragment data (returned from `decode_message` for DATA_FRAG).
#[derive(Debug)]
pub struct FragmentInfo {
    /// Channel ID.
    pub channel: DataChannelId,
    /// Message sequence number (same for all fragments of one message).
    pub seq: SeqNum,
    /// Send timestamp.
    pub send_timestamp: MonoTimestamp,
    /// Fragment index (0-based).
    pub frag_index: u16,
    /// Total fragment count.
    pub frag_count: u16,
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
            w.put_u64(seq.as_u64());
            w.put_u64(send_timestamp.units());
            let len = u16::try_from(frame.len()).map_err(|_| TransportError::InvalidLength)?;
            w.put_u16(len);
            w.put_bytes(frame.payload());
        }
        DataPlaneMessage::DataFrag {
            channel,
            seq,
            send_timestamp,
            frag_index,
            frag_count,
            payload,
        } => {
            w.put_u32(TAG_DATA_FRAG);
            w.put_u32(u32::from(*channel));
            w.put_u64(seq.as_u64());
            w.put_u64(send_timestamp.units());
            w.put_u16(*frag_index);
            w.put_u16(*frag_count);
            let len = u16::try_from(payload.len()).map_err(|_| TransportError::InvalidLength)?;
            w.put_u16(len);
            w.put_bytes(payload.payload());
        }
        DataPlaneMessage::Ack {
            channel,
            seq,
            echo_timestamp,
        } => {
            w.put_u32(TAG_ACK);
            w.put_u32(u32::from(*channel));
            w.put_u64(seq.as_u64());
            w.put_u64(echo_timestamp.units());
        }
        DataPlaneMessage::Nack { channel, seq } => {
            w.put_u32(TAG_NACK);
            w.put_u32(u32::from(*channel));
            w.put_u64(seq.as_u64());
        }
        DataPlaneMessage::Heartbeat {
            channel,
            next_expected,
            echo_timestamp,
        } => {
            w.put_u32(TAG_HEARTBEAT);
            w.put_u32(u32::from(*channel));
            w.put_u64(next_expected.as_u64());
            w.put_u64(echo_timestamp.units());
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
            let seq = SeqNum::from(r.take_u64()?);
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
            let seq = SeqNum::from(r.take_u64()?);
            let echo_timestamp = MonoTimestamp::new(r.take_u64()?);
            Ok(DataPlaneMessage::Ack {
                channel,
                seq,
                echo_timestamp,
            })
        }
        TAG_NACK => {
            let channel = DataChannelId::from(r.take_u32()?);
            let seq = SeqNum::from(r.take_u64()?);
            Ok(DataPlaneMessage::Nack { channel, seq })
        }
        TAG_HEARTBEAT => {
            let channel = DataChannelId::from(r.take_u32()?);
            let next_expected = SeqNum::from(r.take_u64()?);
            let echo_timestamp = MonoTimestamp::new(r.take_u64()?);
            Ok(DataPlaneMessage::Heartbeat {
                channel,
                next_expected,
                echo_timestamp,
            })
        }
        TAG_DATA_FRAG => {
            let channel = DataChannelId::from(r.take_u32()?);
            let seq = SeqNum::from(r.take_u64()?);
            let send_timestamp = MonoTimestamp::new(r.take_u64()?);
            let frag_index = r.take_u16()?;
            let frag_count = r.take_u16()?;
            let len = r.take_u16()? as usize;
            let payload_bytes = r.take_bytes(len)?;
            let payload: Frame<N> = payload_bytes.try_into()?;
            Ok(DataPlaneMessage::DataFrag {
                channel,
                seq,
                send_timestamp,
                frag_index,
                frag_count,
                payload,
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
            seq: SeqNum::from(100),
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
                assert_eq!(seq.as_u64(), 100);
                // assert_eq!(send_timestamp.units(), 123456);
                assert_eq!(frame.payload(), &[1, 2, 3, 4]);
            }
            _ => panic!("expected Data"),
        }
    }

    #[test]
    fn roundtrip_ack() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::Ack {
            channel: DataChannelId::from(7),
            seq: SeqNum::from(999),
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
                assert_eq!(seq.as_u64(), 999);
                // assert_eq!(echo_timestamp.units(), 555);
            }
            _ => panic!("expected Ack"),
        }
    }

    #[test]
    fn roundtrip_nack() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::Nack {
            channel: DataChannelId::from(3),
            seq: SeqNum::from(50),
        };
        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::Nack { channel, seq } => {
                assert_eq!(u32::from(channel), 3);
                assert_eq!(seq.as_u64(), 50);
            }
            _ => panic!("expected Nack"),
        }
    }

    #[test]
    fn roundtrip_heartbeat() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::Heartbeat {
            channel: DataChannelId::from(1),
            next_expected: SeqNum::from(200),
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
                assert_eq!(next_expected.as_u64(), 200);
                // assert_eq!(echo_timestamp.units(), 999999);
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
            seq: SeqNum::from(1),
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
            seq: SeqNum::from(1),
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

    // =========================================================================
    // DATA_FRAG Serialization Tests (CONTRACT_012)
    // =========================================================================

    #[test]
    fn roundtrip_data_frag_single_fragment() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::DataFrag {
            channel: DataChannelId::from(42),
            seq: SeqNum::from(100),
            send_timestamp: MonoTimestamp::new(999_999),
            frag_index: 0,
            frag_count: 1,
            payload: make_frame(&[1, 2, 3, 4, 5]),
        };

        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::DataFrag {
                channel,
                seq,
                frag_index,
                frag_count,
                payload,
                ..
            } => {
                assert_eq!(u32::from(channel), 42);
                assert_eq!(seq.as_u64(), 100);
                assert_eq!(frag_index, 0);
                assert_eq!(frag_count, 1);
                assert_eq!(payload.payload(), &[1, 2, 3, 4, 5]);
            }
            _ => panic!("expected DataFrag"),
        }
    }

    #[test]
    fn roundtrip_data_frag_multiple_fragments() {
        // Simulate 3 fragments of a larger message
        for (idx, payload_data) in [(0u16, &[1, 2, 3][..]), (1, &[4, 5, 6][..]), (2, &[7, 8][..])] {
            let msg: DataPlaneMessage<64> = DataPlaneMessage::DataFrag {
                channel: DataChannelId::from(7),
                seq: SeqNum::from(50),
                send_timestamp: MonoTimestamp::new(123_456),
                frag_index: idx,
                frag_count: 3,
                payload: payload_data.try_into().unwrap(),
            };

            let mut buf = Vec::new();
            encode_message(&msg, &mut buf).unwrap();

            let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
            match decoded {
                DataPlaneMessage::DataFrag {
                    channel,
                    seq,
                    frag_index,
                    frag_count,
                    payload,
                    ..
                } => {
                    assert_eq!(u32::from(channel), 7);
                    assert_eq!(seq.as_u64(), 50);
                    assert_eq!(frag_index, idx);
                    assert_eq!(frag_count, 3);
                    assert_eq!(payload.payload(), payload_data);
                }
                _ => panic!("expected DataFrag for fragment {}", idx),
            }
        }
    }

    #[test]
    fn encode_data_frag_direct() {
        // Test the direct encoding function (hot-path optimized)
        let mut buf = Vec::new();
        encode_data_frag(
            DataChannelId::from(99),
            SeqNum::from(200),
            MonoTimestamp::new(555_555),
            3, // frag_index
            5, // frag_count
            &[10, 20, 30, 40],
            &mut buf,
        )
        .unwrap();

        // Decode and verify
        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::DataFrag {
                channel,
                seq,
                frag_index,
                frag_count,
                payload,
                ..
            } => {
                assert_eq!(u32::from(channel), 99);
                assert_eq!(seq.as_u64(), 200);
                assert_eq!(frag_index, 3);
                assert_eq!(frag_count, 5);
                assert_eq!(payload.payload(), &[10, 20, 30, 40]);
            }
            _ => panic!("expected DataFrag"),
        }
    }

    #[test]
    fn data_frag_header_size_correct() {
        // Verify the header size constant matches actual encoding
        let mut buf = Vec::new();
        encode_data_frag(
            DataChannelId::from(1),
            SeqNum::from(1),
            MonoTimestamp::new(1),
            0,
            1,
            &[], // Empty payload
            &mut buf,
        )
        .unwrap();

        assert_eq!(buf.len(), DATA_FRAG_HEADER_SIZE);
    }

    #[test]
    fn data_header_size_correct() {
        // Verify the header size constant matches actual encoding
        let mut buf = Vec::new();
        encode_data_frame(
            DataChannelId::from(1),
            SeqNum::from(1),
            MonoTimestamp::new(1),
            &Frame::<64>::new(), // Empty frame
            &mut buf,
        )
        .unwrap();

        assert_eq!(buf.len(), DATA_HEADER_SIZE);
    }

    #[test]
    fn data_frag_empty_payload() {
        let msg: DataPlaneMessage<64> = DataPlaneMessage::DataFrag {
            channel: DataChannelId::from(1),
            seq: SeqNum::from(1),
            send_timestamp: MonoTimestamp::new(1),
            frag_index: 0,
            frag_count: 1,
            payload: Frame::new(),
        };

        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::DataFrag { payload, .. } => {
                assert!(payload.is_empty());
            }
            _ => panic!("expected DataFrag"),
        }
    }

    #[test]
    fn data_frag_max_fragment_index() {
        // Test with maximum valid fragment index (frag_count - 1)
        let msg: DataPlaneMessage<64> = DataPlaneMessage::DataFrag {
            channel: DataChannelId::from(1),
            seq: SeqNum::from(1),
            send_timestamp: MonoTimestamp::new(1),
            frag_index: 63, // Max index for 64-fragment message
            frag_count: 64,
            payload: make_frame(&[0xAB]),
        };

        let mut buf = Vec::new();
        encode_message(&msg, &mut buf).unwrap();

        let decoded: DataPlaneMessage<64> = decode_message(&buf).unwrap();
        match decoded {
            DataPlaneMessage::DataFrag {
                frag_index,
                frag_count,
                ..
            } => {
                assert_eq!(frag_index, 63);
                assert_eq!(frag_count, 64);
            }
            _ => panic!("expected DataFrag"),
        }
    }

    #[test]
    fn data_vs_data_frag_discriminant() {
        // Verify DATA and DATA_FRAG have different discriminants
        let data_msg: DataPlaneMessage<64> = DataPlaneMessage::Data {
            channel: DataChannelId::from(1),
            seq: SeqNum::from(1),
            send_timestamp: MonoTimestamp::new(1),
            frame: make_frame(&[1, 2, 3]),
        };

        let frag_msg: DataPlaneMessage<64> = DataPlaneMessage::DataFrag {
            channel: DataChannelId::from(1),
            seq: SeqNum::from(1),
            send_timestamp: MonoTimestamp::new(1),
            frag_index: 0,
            frag_count: 1,
            payload: make_frame(&[1, 2, 3]),
        };

        let mut data_buf = Vec::new();
        let mut frag_buf = Vec::new();
        encode_message(&data_msg, &mut data_buf).unwrap();
        encode_message(&frag_msg, &mut frag_buf).unwrap();

        // First 4 bytes are the discriminant
        assert_ne!(&data_buf[..4], &frag_buf[..4], "DATA and DATA_FRAG should have different tags");

        // DATA tag is 0, DATA_FRAG tag is 4
        assert_eq!(&data_buf[..4], &[0, 0, 0, 0]);
        assert_eq!(&frag_buf[..4], &[4, 0, 0, 0]);
    }

    #[test]
    fn decode_truncated_data_frag() {
        // Valid DATA_FRAG discriminant but truncated header
        let result: Result<DataPlaneMessage<64>, _> = decode_message(&[4, 0, 0, 0, 1, 2, 3]);
        assert!(matches!(result, Err(TransportError::BufferTooSmall)));
    }

    #[test]
    fn decode_data_frag_invalid_payload_length() {
        // Encode valid DATA_FRAG, then corrupt length
        let mut buf = Vec::new();
        encode_data_frag(
            DataChannelId::from(1),
            SeqNum::from(1),
            MonoTimestamp::new(1),
            0,
            1,
            &[1, 2],
            &mut buf,
        )
        .unwrap();

        // Corrupt length field (last 2 bytes of header before payload)
        // Header: tag(4) + channel(4) + seq(8) + timestamp(8) + frag_idx(2) + frag_count(2) + len(2)
        let len_offset = 4 + 4 + 8 + 8 + 2 + 2;
        buf[len_offset] = 0xFF;
        buf[len_offset + 1] = 0xFF;

        let result: Result<DataPlaneMessage<64>, _> = decode_message(&buf);
        assert!(matches!(result, Err(TransportError::InvalidLength)));
    }
}
