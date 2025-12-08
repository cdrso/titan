//! Frame-based wire format for typed messages.

use serde::{Deserialize, Serialize};

use crate::SharedMemorySafe;

/// Default payload capacity for frames.
pub const DEFAULT_FRAME_CAP: usize = 256;

#[derive(SharedMemorySafe, Debug, Clone, Copy)]
#[repr(C)]
#[repr(align(64))]
pub struct Frame<const N: usize = DEFAULT_FRAME_CAP> {
    /// Bytes used inside `payload`.
    pub len: u16,
    /// Serialized message bytes.
    pub payload: [u8; N],
}

#[derive(Debug)]
pub enum FrameError {
    /// Serialization failed or out of capacity.
    Serialize(postcard::Error),
    /// `len` exceeds the frame payload.
    LenOutOfBounds { len: usize, cap: usize }, // this will be removed later on when we support chunking
}

impl From<postcard::Error> for FrameError {
    fn from(err: postcard::Error) -> Self {
        Self::Serialize(err)
    }
}

/// Marker trait for types that can be serialized into a [`Frame`].
///
/// Automatically implemented for all `Serialize + Deserialize` types.
pub trait Wire: Serialize + for<'de> Deserialize<'de> {}
impl<T> Wire for T where T: Serialize + for<'de> Deserialize<'de> {}

impl<const N: usize> Frame<N> {
    /// Serialize `msg` into this frame's payload and set headers.
    pub fn encode<T: Wire>(&mut self, msg: &T) -> Result<(), FrameError> {
        let used = postcard::to_slice(msg, &mut self.payload)?;
        let len = used.len();
        if len > N {
            return Err(FrameError::LenOutOfBounds { len, cap: N });
        }
        self.len = len as u16;
        Ok(())
    }

    /// Deserialize a message from this frame.
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

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Sample {
        a: u32,
        b: u8,
    }

    #[test]
    fn roundtrip_sample() {
        let msg = Sample {
            a: 0xdead_beef,
            b: 7,
        };
        let mut frame = Frame::<64> {
            len: 0,
            payload: [0u8; 64],
        };

        frame.encode(&msg).unwrap();
        let decoded = frame.decode::<Sample>().unwrap();
        assert_eq!(decoded, msg);
    }
}
