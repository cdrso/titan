//! Tick-space coordinate types for the timing wheel.
//!
//! The wheel operates on a discrete tick lattice derived from physical time:
//! `tick = floor((instant - start) / tick_duration)`. The types in this module
//! represent points and spans in that tick space, keeping dimensional roles
//! explicit even though the underlying representation is a `u64` count.

use core::ops::Add;

/// A point on the discrete tick lattice.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct TickInstant(u64);

impl TickInstant {
    /// Creates a new tick instant from a raw tick count.
    #[inline]
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying tick count.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Adds a tick span to this instant.
    ///
    /// # Panics
    ///
    /// Overflow is treated as an impossible invariant and is not checked in release builds.
    #[inline]
    #[must_use]
    pub fn add_span(self, span: TickSpan) -> Self {
        Self(self.0 + span.0)
    }
}

/// A span in tick space (number of ticks).
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct TickSpan(u64);

impl TickSpan {
    /// Creates a new tick span from a raw tick count.
    #[inline]
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying tick count.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl Add<TickSpan> for TickInstant {
    type Output = Self;

    #[inline]
    fn add(self, rhs: TickSpan) -> Self::Output {
        self.add_span(rhs)
    }
}
