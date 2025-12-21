//! Driver TX thread runtime scaffolding.
//!
//! Responsibilities:
//! - Maintain local, read-only snapshot of channel → endpoint fan-out and next_seq.
//!   * Populated by applying control thread commands (add/remove/update channel).
//!   * No shared mutable maps; control is the single writer.
//! - TX loop scheduling:
//!   * Rotate starting channel index each poll to avoid bias.
//!   * Drain up to a small batch per channel (exploit SPSC locality) before moving on.
//!   * Optional work budget per loop to bound latency.
//! - Data path (initial, best-effort):
//!   * Pop `Frame` from client→driver SPSC.
//!   * Assign `SeqNum`, set monotonic send timestamp.
//!   * Send UDP header + frame payload (no re-serialization) to all endpoints.
//!   * Optionally store in history for future retransmission; not used in best-effort mode.
//! - Timing wheels:
//!   * Later: use for retrans timers and heartbeats; configured with monotonic ticks.
//! - Inter-thread communication:
//!   * Receive control→TX commands via SPSC ring.
//!   * Receive RX→TX events (ack/nack/heartbeat) via command ring; update state.
