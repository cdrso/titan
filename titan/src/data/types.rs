//! Shared types for data-plane identifiers and sequencing.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::control::types::ChannelId;
use crate::runtime::timing::{Micros, MonoInstant};

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

impl fmt::Display for SeqNum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonic timestamp in microseconds for on-wire RTT/liveness.
pub type MonoTimestamp = MonoInstant<Micros>;

/// Alias re-export for convenience.
pub type DataChannelId = ChannelId;
