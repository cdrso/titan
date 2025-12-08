// Allow the crate to reference itself as ::titan for derive macro usage
extern crate self as titan;

pub mod control;
pub mod data;
pub mod ipc;

#[doc(inline)]
pub use titan_derive::SharedMemorySafe;

#[doc(inline)]
pub use ipc::shmem::SharedMemorySafe;

// Hidden re-export for the derive macro
#[doc(hidden)]
pub use ipc::shmem::SharedMemorySafe as __SharedMemorySafePrivate;

// Re-export serde traits for convenience
pub use serde::{Deserialize, Serialize};
