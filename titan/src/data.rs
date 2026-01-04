//! Data channel types for application message exchange.

pub mod channel;
pub mod frame;
pub mod packing;
pub mod transport;
pub mod types;

pub use channel::{MessageReceiver, MessageSender, TypedChannelError};
pub use frame::{DEFAULT_FRAME_CAP, Frame, FrameError, Wire};
pub use packing::{PackingBuffer, UnpackingIter, DEFAULT_MTU};
