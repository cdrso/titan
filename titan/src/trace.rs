//! Tracing infrastructure for debugging titan.
//!
//! Enable with `--features tracing`. All trace macros become no-ops when
//! the feature is disabled, ensuring zero overhead in production.

/// Initialize the tracing subscriber with timestamps.
///
/// Call this at the start of tests or the driver binary to enable trace output.
/// Does nothing if the `tracing` feature is not enabled.
#[cfg(feature = "tracing")]
pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("titan=trace"));

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_file(false)
                .with_line_number(false)
                .with_timer(fmt::time::uptime()),
        )
        .with(filter)
        .init();
}

#[cfg(not(feature = "tracing"))]
pub const fn init_tracing() {}

// When tracing is enabled, re-export macros from the tracing crate.
#[cfg(feature = "tracing")]
pub(crate) use tracing::{debug, error, info, trace, warn};

// When tracing is disabled, provide no-op macro implementations.
#[cfg(not(feature = "tracing"))]
macro_rules! trace_noop {
    ($($arg:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
macro_rules! debug_noop {
    ($($arg:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
macro_rules! info_noop {
    ($($arg:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
macro_rules! warn_noop {
    ($($arg:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
macro_rules! error_noop {
    ($($arg:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
pub(crate) use debug_noop as debug;
#[cfg(not(feature = "tracing"))]
pub(crate) use error_noop as error;
#[cfg(not(feature = "tracing"))]
pub(crate) use info_noop as info;
#[cfg(not(feature = "tracing"))]
pub(crate) use trace_noop as trace;
#[cfg(not(feature = "tracing"))]
pub(crate) use warn_noop as warn;
