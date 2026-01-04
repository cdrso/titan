//! Timing configuration for reliability protocol.
//!
//! These parameters control the timing behavior of the driver's reliability
//! mechanisms: heartbeats, NAK delays, retransmit timeouts, and status messages.
//!
//! # Tuning Guidelines
//!
//! - **Local network (< 1ms RTT)**: Use aggressive defaults for fast recovery.
//! - **Cross-datacenter (1-50ms RTT)**: Increase delays proportionally.
//! - **WAN (50-200ms RTT)**: Much longer timeouts, larger NAK accumulation windows.
//!
//! The defaults are optimized for local/datacenter deployments with sub-millisecond
//! latencies where fast gap detection is more important than bandwidth efficiency.

use std::time::Duration;

/// Timing configuration for the reliability protocol.
///
/// All intervals are specified as [`Duration`] for flexibility.
/// The driver uses these to configure timing wheels and periodic tasks.
#[derive(Debug, Clone)]
pub struct TimingConfig {
    /// How often publishers send HEARTBEAT frames during idle periods.
    ///
    /// Heartbeats allow subscribers to detect gaps even when no data is flowing.
    /// The heartbeat carries `next_seq`, enabling gap detection.
    ///
    /// **Default**: 100ms (10 heartbeats/sec during idle)
    pub heartbeat_interval: Duration,

    /// Delay before sending NAK after detecting a gap.
    ///
    /// This allows time for out-of-order packets to arrive before triggering
    /// retransmission. Too short = spurious NAKs, too long = increased latency.
    ///
    /// **Default**: 5ms (aggressive for local networks)
    pub nak_delay: Duration,

    /// Maximum time to wait for retransmission before accepting gap.
    ///
    /// After this timeout, the subscriber accepts the gap and notifies the
    /// application. Prevents indefinite stalls on persistent loss.
    ///
    /// **Default**: 500ms
    pub retransmit_timeout: Duration,

    /// How often subscribers send STATUS_MSG for flow control.
    ///
    /// Status messages carry consumption offset and receiver window.
    /// More frequent = more responsive flow control, more overhead.
    ///
    /// **Default**: 50ms (20 status messages/sec)
    pub status_message_interval: Duration,

    /// Interval for coalescing multiple NAK entries into a single NAK_BITMAP.
    ///
    /// When multiple gaps are detected close together, we batch them into
    /// a single NAK_BITMAP frame for efficiency.
    ///
    /// **Default**: 2ms
    pub nak_coalesce_interval: Duration,

    /// Maximum number of NAK entries per NAK_BITMAP frame.
    ///
    /// Limits frame size. Additional entries are sent in subsequent frames.
    ///
    /// **Default**: 16 entries (16 * 16 = 256 bytes of bitmap data)
    pub max_nak_entries: usize,

    /// Number of retransmit attempts before giving up on a sequence.
    ///
    /// After this many NAKs without receiving the data, accept the gap.
    ///
    /// **Default**: 3 attempts
    pub max_retransmit_attempts: u32,
}

impl TimingConfig {
    /// Creates a new timing configuration with validation.
    ///
    /// # Panics
    ///
    /// Panics if `max_nak_entries == 0` or `max_retransmit_attempts == 0`.
    #[must_use]
    fn new_validated(
        heartbeat_interval: Duration,
        nak_delay: Duration,
        retransmit_timeout: Duration,
        status_message_interval: Duration,
        nak_coalesce_interval: Duration,
        max_nak_entries: usize,
        max_retransmit_attempts: u32,
    ) -> Self {
        assert!(max_nak_entries > 0, "max_nak_entries must be > 0");
        assert!(max_retransmit_attempts > 0, "max_retransmit_attempts must be > 0");

        Self {
            heartbeat_interval,
            nak_delay,
            retransmit_timeout,
            status_message_interval,
            nak_coalesce_interval,
            max_nak_entries,
            max_retransmit_attempts,
        }
    }

    /// Creates a configuration optimized for local networks (< 1ms RTT).
    ///
    /// Very aggressive timing for fastest possible recovery.
    #[must_use]
    pub fn local() -> Self {
        Self::new_validated(
            Duration::from_millis(50),
            Duration::from_millis(2),
            Duration::from_millis(100),
            Duration::from_millis(20),
            Duration::from_millis(1),
            16,
            3,
        )
    }

    /// Creates a configuration optimized for datacenter networks (1-10ms RTT).
    ///
    /// Balanced timing for typical datacenter deployments.
    #[must_use]
    pub fn datacenter() -> Self {
        Self::default()
    }

    /// Creates a configuration for cross-datacenter links (10-50ms RTT).
    ///
    /// More conservative timing to avoid spurious NAKs.
    #[must_use]
    pub fn cross_dc() -> Self {
        Self::new_validated(
            Duration::from_millis(200),
            Duration::from_millis(20),
            Duration::from_secs(1),
            Duration::from_millis(100),
            Duration::from_millis(10),
            32,
            5,
        )
    }

    /// Creates a configuration for WAN links (50-200ms RTT).
    ///
    /// Very conservative timing for high-latency links.
    #[must_use]
    pub fn wan() -> Self {
        Self::new_validated(
            Duration::from_millis(500),
            Duration::from_millis(100),
            Duration::from_secs(5),
            Duration::from_millis(250),
            Duration::from_millis(50),
            64,
            10,
        )
    }
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self::new_validated(
            Duration::from_millis(100),
            Duration::from_millis(5),
            Duration::from_millis(500),
            Duration::from_millis(50),
            Duration::from_millis(2),
            16,
            3,
        )
    }
}

impl TimingConfig {

    /// Builder-style setter for heartbeat interval.
    #[must_use]
    pub const fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Builder-style setter for NAK delay.
    #[must_use]
    pub const fn with_nak_delay(mut self, delay: Duration) -> Self {
        self.nak_delay = delay;
        self
    }

    /// Builder-style setter for retransmit timeout.
    #[must_use]
    pub const fn with_retransmit_timeout(mut self, timeout: Duration) -> Self {
        self.retransmit_timeout = timeout;
        self
    }

    /// Builder-style setter for status message interval.
    #[must_use]
    pub const fn with_status_message_interval(mut self, interval: Duration) -> Self {
        self.status_message_interval = interval;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values_are_sensible() {
        let config = TimingConfig::default();

        // Heartbeat should be frequent enough to detect gaps quickly
        assert!(config.heartbeat_interval <= Duration::from_millis(200));

        // NAK delay should be short for fast recovery
        assert!(config.nak_delay <= Duration::from_millis(50));

        // Retransmit timeout should be long enough to allow retries
        assert!(config.retransmit_timeout >= Duration::from_millis(100));

        // Status message interval should be reasonable
        assert!(config.status_message_interval >= Duration::from_millis(10));
        assert!(config.status_message_interval <= Duration::from_millis(200));
    }

    #[test]
    fn presets_have_increasing_latency_tolerance() {
        let local = TimingConfig::local();
        let dc = TimingConfig::datacenter();
        let cross_dc = TimingConfig::cross_dc();
        let wan = TimingConfig::wan();

        // NAK delay should increase with latency tolerance
        assert!(local.nak_delay <= dc.nak_delay);
        assert!(dc.nak_delay <= cross_dc.nak_delay);
        assert!(cross_dc.nak_delay <= wan.nak_delay);

        // Retransmit timeout should also increase
        assert!(local.retransmit_timeout <= dc.retransmit_timeout);
        assert!(dc.retransmit_timeout <= cross_dc.retransmit_timeout);
        assert!(cross_dc.retransmit_timeout <= wan.retransmit_timeout);
    }

    #[test]
    fn builder_pattern() {
        let config = TimingConfig::default()
            .with_heartbeat_interval(Duration::from_millis(75))
            .with_nak_delay(Duration::from_millis(10));

        assert_eq!(config.heartbeat_interval, Duration::from_millis(75));
        assert_eq!(config.nak_delay, Duration::from_millis(10));
    }

    #[test]
    #[should_panic(expected = "max_nak_entries must be > 0")]
    fn zero_max_nak_entries_panics() {
        let _ = TimingConfig::new_validated(
            Duration::from_millis(100),
            Duration::from_millis(5),
            Duration::from_millis(500),
            Duration::from_millis(50),
            Duration::from_millis(2),
            0, // Invalid!
            3,
        );
    }

    #[test]
    #[should_panic(expected = "max_retransmit_attempts must be > 0")]
    fn zero_max_retransmit_attempts_panics() {
        let _ = TimingConfig::new_validated(
            Duration::from_millis(100),
            Duration::from_millis(5),
            Duration::from_millis(500),
            Duration::from_millis(50),
            Duration::from_millis(2),
            16,
            0, // Invalid!
        );
    }

    #[test]
    fn all_presets_have_valid_invariants() {
        // All presets must have non-zero counts
        for config in [
            TimingConfig::local(),
            TimingConfig::datacenter(),
            TimingConfig::cross_dc(),
            TimingConfig::wan(),
        ] {
            assert!(config.max_nak_entries > 0, "max_nak_entries must be > 0");
            assert!(config.max_retransmit_attempts > 0, "max_retransmit_attempts must be > 0");
        }
    }
}
