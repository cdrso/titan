// Allow the crate to reference itself as ::titan for derive macro usage
extern crate self as titan;

pub mod ipc;
pub mod protocol;

// Re-export both the derive macro and the trait at crate root
// Rust distinguishes between them based on context:
// - #[derive(SharedMemorySafe)] uses the macro
// - fn foo<T: SharedMemorySafe>() uses the trait
// This matches the pattern used by serde and other derive macro crates
#[doc(inline)]
pub use titan_derive::SharedMemorySafe;

#[doc(inline)]
pub use ipc::shmem::SharedMemorySafe;

// Also provide an alias for those who prefer explicit trait naming
#[doc(inline)]
pub use ipc::shmem::SharedMemorySafe as SharedMemorySafeTrait;

// Hidden re-export for the derive macro to reference
// This provides a stable path that works both from within and outside the crate
#[doc(hidden)]
pub use ipc::shmem::SharedMemorySafe as __SharedMemorySafePrivate;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
