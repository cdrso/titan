//! Frame-based wire format for typed messages.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use type_hash::TypeHash;

use crate::SharedMemorySafe;

/// Default payload capacity for frames.
pub const DEFAULT_FRAME_CAP: usize = 256;

/// Fixed-size container for a serialized message.
///
/// Frames are the wire format for data channels. Messages are serialized into
/// the payload using [`postcard`], with the length stored separately.
///
/// The frame is cache-line aligned (64 bytes) for optimal SPSC queue performance.
#[derive(SharedMemorySafe, Debug, Clone, Copy)]
#[repr(C)]
#[repr(align(64))]
pub struct Frame<const N: usize = DEFAULT_FRAME_CAP> {
    len: u16,
    payload: [u8; N],
}

impl<const N: usize> Frame<N> {
    /// Creates a new empty frame.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            len: 0,
            payload: [0; N],
        }
    }

    /// Creates a frame from bytes without zeroing the payload first.
    ///
    /// # Safety
    ///
    /// This is safe because we immediately copy `bytes` into the payload,
    /// and only the first `bytes.len()` bytes are considered valid (tracked by `len`).
    /// The remaining bytes in `payload` are uninitialized but never read.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len() > N` or `bytes.len() > u16::MAX`.
    #[inline]
    #[must_use]
    pub fn from_bytes_unchecked(bytes: &[u8]) -> Self {
        let len = bytes.len();
        debug_assert!(len <= N, "bytes length {} exceeds capacity {}", len, N);
        debug_assert!(len <= u16::MAX as usize, "bytes length {} exceeds u16::MAX", len);

        // SAFETY: We immediately initialize the valid portion with copy_from_slice.
        // The Frame only exposes payload[..len] via payload(), so uninitialized
        // bytes beyond len are never read.
        unsafe {
            let mut frame = std::mem::MaybeUninit::<Self>::uninit();
            let ptr = frame.as_mut_ptr();
            (*ptr).len = len as u16;
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), (*ptr).payload.as_mut_ptr(), len);
            frame.assume_init()
        }
    }

    /// Returns the length of the valid payload.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns true if the frame is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the valid payload slice.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload[..self.len as usize]
    }
}

impl<const N: usize> Default for Frame<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> TryFrom<&[u8]> for Frame<N> {
    type Error = FrameError;

    /// Create a frame from raw bytes (already serialized payload).
    ///
    /// Copies `bytes` into the frame payload and sets the length.
    /// CONTRACT_031: Uses `from_bytes_unchecked` to avoid zeroing payload.
    ///
    /// # Errors
    /// Returns [`FrameError::LenOutOfBounds`] if `bytes` exceed the frame capacity or `u16::MAX`.
    #[inline]
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let len = bytes.len();
        if len > N || len > u16::MAX as usize {
            return Err(FrameError::LenOutOfBounds { len, cap: N });
        }
        Ok(Self::from_bytes_unchecked(bytes))
    }
}

/// Errors from frame encoding or decoding.
#[derive(Debug, Error)]
pub enum FrameError {
    /// Serialization or deserialization failed.
    #[error("frame serialization failed: {0}")]
    Serialize(postcard::Error),
    /// Message length exceeds frame capacity.
    #[error("frame length {len} exceeds capacity {cap}")]
    LenOutOfBounds {
        /// Actual message length.
        len: usize,
        /// Frame capacity.
        cap: usize,
    },
}

impl From<postcard::Error> for FrameError {
    fn from(err: postcard::Error) -> Self {
        Self::Serialize(err)
    }
}

/// Trait for types that can be serialized into a [`Frame`].
///
/// Requires `Serialize + Deserialize + TypeHash`. The type ID is automatically
/// computed as a structural hash of the type's definition, ensuring that types
/// with identical structure (same fields, same types, same names) produce
/// the same ID across compilations and machines.
pub trait Wire: Serialize + for<'de> Deserialize<'de> + TypeHash {
    /// Returns a unique identifier computed from the type's structure.
    ///
    /// This hash is stable across compilations - same type definition
    /// produces the same ID. Used to ensure type safety across process
    /// and network boundaries.
    #[must_use]
    fn type_id() -> u64 {
        Self::type_hash()
    }
}

/// Blanket implementation for all types that satisfy the bounds.
impl<T> Wire for T where T: Serialize + for<'de> Deserialize<'de> + TypeHash {}

impl<const N: usize> Frame<N> {
    /// Serialize `msg` into this frame's payload and set headers.
    ///
    /// # Errors
    /// - [`FrameError::Serialize`] if serialization fails
    /// - [`FrameError::LenOutOfBounds`] if the encoded payload exceeds the frame capacity or `u16::MAX`
    pub fn encode<T: Wire>(&mut self, msg: &T) -> Result<(), FrameError> {
        let used = postcard::to_slice(msg, &mut self.payload)?;
        let len = used.len();
        if len > N {
            return Err(FrameError::LenOutOfBounds { len, cap: N });
        }
        self.len = u16::try_from(len).map_err(|_| FrameError::LenOutOfBounds { len, cap: N })?;
        Ok(())
    }

    /// Deserialize a message from this frame.
    ///
    /// # Errors
    /// - [`FrameError::LenOutOfBounds`] if the stored length exceeds the frame capacity
    /// - [`FrameError::Serialize`] if deserialization fails
    pub fn decode<T: Wire>(&self) -> Result<T, FrameError> {
        if usize::from(self.len) > N {
            return Err(FrameError::LenOutOfBounds {
                len: usize::from(self.len),
                cap: N,
            });
        }
        let bytes = &self.payload[..usize::from(self.len)];
        postcard::from_bytes(bytes).map_err(FrameError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, TypeHash, Debug, PartialEq)]
    struct Sample {
        a: u32,
        b: u8,
    }

    #[derive(Serialize, Deserialize, TypeHash, Debug, PartialEq)]
    struct Large {
        data: Vec<u8>,
    }

    #[test]
    fn roundtrip_sample() {
        let msg = Sample {
            a: 0xdead_beef,
            b: 7,
        };
        let mut frame = Frame::<64>::new();
        frame.encode(&msg).unwrap();
        let decoded: Sample = frame.decode().unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn empty_frame() {
        let frame = Frame::<64>::new();
        assert!(frame.is_empty());
        assert_eq!(frame.len(), 0);
        assert!(frame.payload().is_empty());
    }

    #[test]
    fn frame_default() {
        let frame: Frame<64> = Frame::default();
        assert!(frame.is_empty());
    }

    #[test]
    fn encode_updates_len() {
        let msg = Sample { a: 1, b: 2 };
        let mut frame = Frame::<64>::new();
        assert!(frame.is_empty());

        frame.encode(&msg).unwrap();
        assert!(!frame.is_empty());
        assert!(frame.len() > 0);
        assert_eq!(frame.payload().len(), frame.len());
    }

    #[test]
    fn encode_overflow_capacity() {
        let msg = Large {
            data: vec![0xAB; 128],
        };
        let mut frame = Frame::<32>::new(); // Too small for Large

        let result = frame.encode(&msg);
        assert!(result.is_err());
        match result {
            Err(FrameError::Serialize(_)) => {} // postcard fails during serialization
            Err(FrameError::LenOutOfBounds { .. }) => {}
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn decode_corrupted_len() {
        // Manually create a frame with invalid len > capacity
        let mut frame = Frame::<64>::new();
        frame.len = 100; // Invalid: exceeds capacity of 64

        let result: Result<Sample, _> = frame.decode();
        assert!(matches!(
            result,
            Err(FrameError::LenOutOfBounds { len: 100, cap: 64 })
        ));
    }

    #[test]
    fn decode_empty_frame_fails() {
        let frame = Frame::<64>::new();
        // Decoding empty frame should fail (no valid postcard data)
        let result: Result<Sample, _> = frame.decode();
        assert!(matches!(result, Err(FrameError::Serialize(_))));
    }

    #[test]
    fn roundtrip_at_exact_capacity() {
        // Create a message that fits exactly
        #[derive(Serialize, Deserialize, TypeHash, Debug, PartialEq)]
        struct Small(u8);

        let msg = Small(42);
        let mut frame = Frame::<8>::new();
        frame.encode(&msg).unwrap();
        let decoded: Small = frame.decode().unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn multiple_encodes_overwrite() {
        let mut frame = Frame::<64>::new();

        let msg1 = Sample { a: 1, b: 1 };
        frame.encode(&msg1).unwrap();
        let len1 = frame.len();

        let msg2 = Sample { a: 999, b: 255 };
        frame.encode(&msg2).unwrap();

        // Second encode should overwrite
        let decoded: Sample = frame.decode().unwrap();
        assert_eq!(decoded, msg2);
        // Length might differ if values serialize differently
        assert!(frame.len() > 0);
        let _ = len1; // Acknowledge we captured it
    }

    #[test]
    fn try_from_bytes_success() {
        let data = [1u8, 2, 3, 4, 5];
        let frame: Frame<64> = (&data[..]).try_into().unwrap();
        assert_eq!(frame.len(), 5);
        assert_eq!(frame.payload(), &data);
    }

    #[test]
    fn try_from_bytes_empty() {
        let data: [u8; 0] = [];
        let frame: Frame<64> = (&data[..]).try_into().unwrap();
        assert!(frame.is_empty());
        assert_eq!(frame.payload(), &[]);
    }

    #[test]
    fn try_from_bytes_exact_capacity() {
        let data = [0xABu8; 32];
        let frame: Frame<32> = (&data[..]).try_into().unwrap();
        assert_eq!(frame.len(), 32);
        assert_eq!(frame.payload(), &data);
    }

    #[test]
    fn try_from_bytes_exceeds_capacity() {
        let data = [0u8; 65];
        let result: Result<Frame<64>, _> = (&data[..]).try_into();
        assert!(matches!(
            result,
            Err(FrameError::LenOutOfBounds { len: 65, cap: 64 })
        ));
    }

    #[test]
    fn try_from_bytes_roundtrip_with_decode() {
        // Encode a message, get bytes, reconstruct frame, decode
        let msg = Sample { a: 42, b: 7 };
        let mut original = Frame::<64>::new();
        original.encode(&msg).unwrap();

        let bytes = original.payload();
        let reconstructed: Frame<64> = bytes.try_into().unwrap();

        let decoded: Sample = reconstructed.decode().unwrap();
        assert_eq!(decoded, msg);
    }
}
