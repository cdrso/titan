//! Shared types for data-plane identifiers and sequencing.
//!
//! Channels are typed streams (one type per channel). Sequence numbers are u64
//! for wrap-safety. Timestamps are monotonic ticks aligned with the runtime timer unit.

use serde::{Deserialize, Serialize};

use crate::control::types::ChannelId;
use crate::runtime::timing::time::{Micros, Timestamp};

/// Sequence number for data frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct SeqNum(pub u64);

impl From<u64> for SeqNum {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<SeqNum> for u64 {
    fn from(s: SeqNum) -> Self {
        s.0
    }
}

impl SeqNum {
    /// Initial sequence number for a new stream.
    pub const ZERO: Self = Self(0);

    /// Next sequence number (wraps on overflow).
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

/// Monotonic timestamp in microseconds for on-wire RTT/liveness.
pub type MonoTimestamp = Timestamp<Micros>;

/// Alias re-export for convenience.
pub type DataChannelId = ChannelId;
