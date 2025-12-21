//! Driver RX thread runtime scaffolding.
//!
//! Responsibilities:
//! - Maintain local, read-only snapshot of channel → client RX queue (SPSC) mapping.
//!   * Populated by applying control→RX commands (add/remove channel).
//!   * No shared mutable maps; control is single writer.
//! - RX loop:
//!   * Read UDP, decode `DataPlaneMessage`.
//!   * On `Data`: validate channel, (optionally) check `expected_seq`, copy payload into
//!     new `Frame` and push to client RX SPSC; update `expected_seq`.
//!   * Initial best-effort: ignore `Ack/Nack/Heartbeat` or forward events to TX via command ring.
//! - Timing wheels:
//!   * Later: use for ack batching/nack/gap detection and liveliness timers.
//! - Scheduling: driven by incoming packets; no per-channel polling needed.
//! - Inter-thread communication:
//!   * Receive control→RX commands via SPSC ring.
//!   * Send events to TX (ack/nack/heartbeat) via command ring; TX updates its state.
