// Allow the crate to reference itself as ::titan for derive macro usage
extern crate self as titan;

pub mod ipc;
pub mod protocol;

#[doc(inline)]
pub use titan_derive::SharedMemorySafe;

#[doc(inline)]
pub use ipc::shmem::SharedMemorySafe;

// Hidden re-export for the derive macro
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
