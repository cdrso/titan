//! Client-side runtime scaffolding.
//!
//! Client responsibilities are:
//! - Control loop (already exists) uses a small timing wheel for handshakes/heartbeats.
//! - Data channels are direct SPSC; client stays unaware of network reliability.
//! - Future: per-client control timers live here; single control thread on client (apart from what
//!   user spawns).
