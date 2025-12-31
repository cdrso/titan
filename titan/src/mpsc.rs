//! Core MPSC (Multi-Producer Single-Consumer) queue primitives.
//!
//! This module contains a bounded wait-free MPSC ring buffer algorithm.
//! Unlike SPSC, multiple producers can safely push concurrently.
//!
//! Used by:
//! - [`crate::ipc::mpsc`] - Cross-process queues over shared memory

pub(crate) mod ring;
