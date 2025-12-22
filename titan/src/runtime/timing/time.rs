//! Minimal time units and strongly-typed durations/timestamps.

use core::marker::PhantomData;
use core::num::{NonZeroU64, TryFromIntError};
use core::ops::{Add, Div, Sub};
use core::time::Duration as StdDuration;

use minstant::Instant;
use std::sync::OnceLock;
use thiserror::Error;

/// Marker trait for a time unit.
pub trait TimeUnit {
    /// Human-readable name.
    const NAME: &'static str;
}

/// Trait for units that can produce a monotonic instant from the monotonic clock.
pub trait Now: TimeUnit + Sized {
    /// Returns the current monotonic instant.
    fn now() -> MonoInstant<Self>;
}

/// Microsecond time unit.
#[derive(Debug)]
pub enum Micros {}

impl TimeUnit for Micros {
    const NAME: &'static str = "us";
}

impl Now for Micros {
    fn now() -> MonoInstant<Self> {
        let dur = monotonic_since_start();
        MonoInstant::new(dur.as_micros() as u64)
    }
}

/// Millisecond time unit.
#[derive(Debug)]
pub enum Millis {}

impl TimeUnit for Millis {
    const NAME: &'static str = "ms";
}

impl Now for Millis {
    fn now() -> MonoInstant<Self> {
        let dur = monotonic_since_start();
        MonoInstant::new(dur.as_millis() as u64)
    }
}

/// Second time unit.
#[derive(Debug)]
pub enum Seconds {}

impl TimeUnit for Seconds {
    const NAME: &'static str = "s";
}

impl Now for Seconds {
    fn now() -> MonoInstant<Self> {
        let dur = monotonic_since_start();
        MonoInstant::new(dur.as_secs())
    }
}

/// Strongly-typed duration in a given unit.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Duration<U: TimeUnit>(u64, PhantomData<U>);

impl<U: TimeUnit> Copy for Duration<U> {}

impl<U: TimeUnit> Clone for Duration<U> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<U: TimeUnit> Duration<U> {
    /// Creates a new duration.
    #[inline]
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value, PhantomData)
    }

    /// Ceiling division by another duration of the same unit.
    #[inline]
    #[must_use]
    pub const fn div_ceil(self, rhs: Self) -> u64 {
        let q = self.0 / rhs.0;
        let r = self.0 % rhs.0;
        q + (r != 0) as u64
    }
}

impl Duration<Millis> {
    /// Convenience constructor for milliseconds.
    #[inline]
    #[must_use]
    pub const fn from_millis(value: u64) -> Self {
        Self::new(value)
    }
}

// Saturate instead of panicking/wrapping in ops traits; overflow would require hundreds of
// thousands of years at microsecond resolution, so clamping is effectively
// unreachable while keeping release builds panic-free.

impl<U: TimeUnit> Add for Duration<U> {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.0.saturating_add(rhs.0))
    }
}

impl<U: TimeUnit> Sub for Duration<U> {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        Self::new(self.0.saturating_sub(rhs.0))
    }
}

impl<U: TimeUnit> Div<u64> for Duration<U> {
    type Output = Self;
    #[inline]
    fn div(self, rhs: u64) -> Self::Output {
        Self::new(self.0 / rhs)
    }
}

impl<U: TimeUnit> Div for Duration<U> {
    type Output = u64;
    #[inline]
    fn div(self, rhs: Self) -> Self::Output {
        self.0 / rhs.0
    }
}

impl TryFrom<StdDuration> for Duration<Micros> {
    type Error = TryFromIntError;
    fn try_from(d: StdDuration) -> Result<Self, Self::Error> {
        Ok(Self::new(u64::try_from(d.as_micros())?))
    }
}

impl TryFrom<StdDuration> for Duration<Millis> {
    type Error = TryFromIntError;
    fn try_from(d: StdDuration) -> Result<Self, Self::Error> {
        Ok(Self::new(u64::try_from(d.as_millis())?))
    }
}

impl TryFrom<StdDuration> for Duration<Seconds> {
    type Error = TryFromIntError;
    fn try_from(d: StdDuration) -> Result<Self, Self::Error> {
        Ok(Self::new(d.as_secs()))
    }
}

/// Error returned when attempting to construct a non-zero duration from zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("duration must be non-zero")]
pub struct ZeroDurationError;

/// Non-zero strongly-typed duration in a given unit.
// Manual Copy/Clone: derive would require U: Copy, but PhantomData is just a marker.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct NonZeroDuration<U: TimeUnit>(NonZeroU64, PhantomData<U>);

impl<U: TimeUnit> Copy for NonZeroDuration<U> {}

impl<U: TimeUnit> Clone for NonZeroDuration<U> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<U: TimeUnit> NonZeroDuration<U> {
    /// Creates a new non-zero duration.
    #[inline]
    #[must_use]
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value, PhantomData)
    }
}

impl<U: TimeUnit> From<NonZeroDuration<U>> for Duration<U> {
    #[inline]
    fn from(d: NonZeroDuration<U>) -> Self {
        Self::new(d.0.get())
    }
}

impl<U: TimeUnit> TryFrom<u64> for NonZeroDuration<U> {
    type Error = ZeroDurationError;
    #[inline]
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(value)
            .map(Self::new)
            .ok_or(ZeroDurationError)
    }
}

/// Strongly-typed monotonic instant in a given unit.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct MonoInstant<U: TimeUnit>(u64, PhantomData<U>);

impl<U: TimeUnit> Copy for MonoInstant<U> {}

impl<U: TimeUnit> Clone for MonoInstant<U> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<U: TimeUnit> MonoInstant<U> {
    /// Creates a new monotonic instant.
    #[inline]
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value, PhantomData)
    }
}

impl<U: TimeUnit> Add<Duration<U>> for MonoInstant<U> {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Duration<U>) -> Self::Output {
        Self::new(self.0.saturating_add(rhs.0))
    }
}

impl<U: TimeUnit> Sub<Duration<U>> for MonoInstant<U> {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: Duration<U>) -> Self::Output {
        Self::new(self.0.saturating_sub(rhs.0))
    }
}

impl<U: TimeUnit> Sub for MonoInstant<U> {
    type Output = Duration<U>;
    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        Duration::new(self.0.saturating_sub(rhs.0))
    }
}

fn monotonic_since_start() -> StdDuration {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed()
}
