//! Data channel types for application message exchange.

pub mod channel;
pub mod frame;

pub use channel::{TypedChannelError, TypedConsumer, TypedProducer};
pub use frame::{DEFAULT_FRAME_CAP, Frame, FrameError, Wire};
