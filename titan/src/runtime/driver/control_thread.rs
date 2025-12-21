//! Driver control thread runtime scaffolding.
//!
//! Responsibilities:
//! - Owns canonical state: clients, channels, endpoint lists.
//! - Single writer: applies client control commands (open/close channel, etc.).
//! - Emits deltas over SPSC command rings to TX/RX threads:
//!   - Add/Remove/Update channel endpoints (for TX routing).
//!   - Add/Remove channel → client RX queue mappings (for RX demux).
//! - Does NOT share mutable maps with TX/RX; avoids cache line bouncing.
//! - Cold path: updates are infrequent; hot path lives in TX/RX.
//! - Future: can include per-channel policy (batch limits, tick config) in commands.
