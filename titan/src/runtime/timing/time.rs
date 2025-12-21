//! Minimal time units and strongly-typed durations/timestamps.
use core::marker::PhantomData;

/// Marker trait for a time unit.
pub trait TimeUnit {
    /// Human-readable name for debugging/metrics.
    const NAME: &'static str;
}

#[derive(Debug)]
pub enum Micros {}
impl TimeUnit for Micros {
    const NAME: &'static str = "us";
}

#[derive(Debug)]
pub enum Millis {}
impl TimeUnit for Millis {
    const NAME: &'static str = "ms";
}

#[derive(Debug)]
pub enum Seconds {}
impl TimeUnit for Seconds {
    const NAME: &'static str = "s";
}

/// Strongly-typed duration in a given unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Duration<U: TimeUnit>(pub u64, PhantomData<U>);

/// Strongly-typed timestamp in a given unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Timestamp<U: TimeUnit>(pub u64, PhantomData<U>);

impl<U: TimeUnit> Duration<U> {
    /// Create a new duration.
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value, PhantomData)
    }

    /// Return the raw value.
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Duration<Millis> {
    /// Convenience constructor for milliseconds.
    #[inline]
    pub const fn from_millis(value: u64) -> Self {
        Self::new(value)
    }
}

impl<U: TimeUnit> Timestamp<U> {
    /// Create a new timestamp.
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value, PhantomData)
    }

    /// Return the raw value.
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl<U: TimeUnit> core::ops::Add<Duration<U>> for Timestamp<U> {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Duration<U>) -> Self::Output {
        Timestamp::new(self.0 + rhs.0)
    }
}

impl<U: TimeUnit> core::ops::Sub<Duration<U>> for Timestamp<U> {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: Duration<U>) -> Self::Output {
        Timestamp::new(self.0 - rhs.0)
    }
}

impl<U: TimeUnit> core::ops::Sub for Timestamp<U> {
    type Output = Duration<U>;
    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        Duration::new(self.0 - rhs.0)
    }
}
