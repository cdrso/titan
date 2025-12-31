//! Core SPSC (Single-Producer Single-Consumer) queue primitives.
//!
//! This module contains the shared ring buffer algorithm used by both:
//! - [`crate::ipc::spsc`] - Cross-process queues over shared memory
//! - [`crate::sync::spsc`] - In-process queues over heap memory

pub(crate) mod ring;
