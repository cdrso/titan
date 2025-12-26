//! Timing primitives.
//!
//! Preferred usage is the scoped wheel API (`with_wheel`), which hides
//! generativity branding while preserving compile-time protection. The
//! low-level branded API is crate-internal.

mod scoped;
mod slab;
mod tick;
mod time;
pub(crate) mod wheel;

pub use scoped::{WheelScope, with_wheel};
pub use time::{Duration, Micros, Millis, MonoInstant, NonZeroDuration, Now, Seconds, TimeUnit};
pub use wheel::{TimerHandle, WheelError};
