//! Runtime utilities and scaffolding for driver and client.
//!
//! - `timing`: generic timing primitives (shared-nothing, per-thread).
//! - `driver`: design notes for control/TX/RX runtimes (apply control deltas via commands; no shared maps).
//! - `client`: client-side runtime scaffolding.

pub mod client;
pub mod driver;
pub mod timing;
