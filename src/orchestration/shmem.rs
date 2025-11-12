use rustix::fs::{Mode, ftruncate};
use rustix::mm::{MapFlags, ProtFlags, mmap, munmap};
use rustix::{io, shm};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;
use std::sync::atomic::*;

// Trait to define Mode behavior
pub trait ShmMode {
    const SHOULD_UNLINK: bool;
}

pub struct Creator;
impl ShmMode for Creator {
    const SHOULD_UNLINK: bool = true;
}

pub struct User;
impl ShmMode for User {
    const SHOULD_UNLINK: bool = false;
}

/// Types safe to use in shared memory across processes.
///
/// # Safety
///
/// Implementers must ensure:
/// - Type uses `#[repr(C)]` or `#[repr(transparent)]` for stable layout
/// - No pointers, references, or heap allocations (Box, Vec, String, &T, etc.)
/// - All fields are also `SharedMemorySafe`
/// - Concurrent access is handled safely (use atomics for synchronization)
/// - Safe even if Drop is never called on some process mappings
/// - Type is `Send + Sync`
pub unsafe trait SharedMemorySafe {}

macro_rules! impl_shared_memory_safe {
      ($($t:ty),* $(,)?) => {
          $(
              unsafe impl SharedMemorySafe for $t {}
          )*
      };
  }

impl_shared_memory_safe! {
    // Signed integers
    i8, i16, i32, i64, i128, isize,

    // Unsigned integers
    u8, u16, u32, u64, u128, usize,

    // Floats
    f32, f64,

    // Bool
    bool,

    // Atomics
    AtomicBool,
    AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize,
    AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize,
}
unsafe impl<T: SharedMemorySafe, const N: usize> SharedMemorySafe for [T; N] {}

pub struct Shm<T: SharedMemorySafe, Mode: ShmMode> {
    ptr: *mut T,
    size: usize,
    path: String,
    _mode: PhantomData<Mode>,
}

unsafe impl<T: SharedMemorySafe, Mode: ShmMode> Send for Shm<T, Mode> {}
unsafe impl<T: SharedMemorySafe, Mode: ShmMode> Sync for Shm<T, Mode> {}

// Constructor for Creator mode
impl<T: SharedMemorySafe> Shm<T, Creator> {
    /// Create a new shared memory object and map it.
    ///
    /// # Arguments
    /// * `path` - The shared memory object name (e.g., "/my-shm")
    ///
    /// # Safety
    /// The shared memory will be created, truncated to size of T, and mapped.
    /// On drop, the memory will be unmapped and the name will be unlinked.
    pub fn create(path: &str) -> io::Result<Self> {
        // Create the shared memory object
        let fd = shm::open(
            path,
            shm::OFlags::CREATE | shm::OFlags::RDWR,
            Mode::RUSR | Mode::WUSR,
        )?;

        // Resize to hold T
        ftruncate(&fd, size_of::<T>() as u64)?;

        // Map into our address space
        let ptr = unsafe {
            mmap(
                null_mut(),
                size_of::<T>(),
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                &fd,
                0,
            )?
        };

        Ok(Self {
            ptr: ptr as *mut T,
            size: size_of::<T>(),
            path: path.to_string(),
            _mode: PhantomData,
        })
    }
}

// Constructor for User mode
impl<T: SharedMemorySafe> Shm<T, User> {
    /// Open an existing shared memory object and map it.
    ///
    /// # Arguments
    /// * `path` - The shared memory object name (e.g., "/my-shm")
    ///
    /// # Safety
    /// The shared memory must already exist and be the correct size.
    /// On drop, the memory will be unmapped but the name will NOT be unlinked.
    pub fn open(path: &str) -> io::Result<Self> {
        // Open existing shared memory object
        let fd = shm::open(path, shm::OFlags::RDWR, Mode::empty())?;

        // Map into our address space
        let ptr = unsafe {
            mmap(
                null_mut(),
                size_of::<T>(),
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                &fd,
                0,
            )?
        };

        Ok(Self {
            ptr: ptr as *mut T,
            size: size_of::<T>(),
            path: path.to_string(),
            _mode: PhantomData,
        })
    }
}

impl<T: SharedMemorySafe, Mode: ShmMode> Drop for Shm<T, Mode> {
    fn drop(&mut self) {
        unsafe {
            let _ = munmap(self.ptr as *mut _, self.size);
        }

        // Only creator unlinks the shared memory name
        if Mode::SHOULD_UNLINK {
            let _ = shm::unlink(&self.path);
        }
    }
}

impl<T: SharedMemorySafe, Mode: ShmMode> Deref for Shm<T, Mode> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: ptr is valid for the lifetime of Shm, guaranteed by:
        // 1. mmap succeeded during construction
        // 2. Memory remains mapped until Drop
        // 3. T: SharedMemorySafe ensures proper layout and concurrent access
        unsafe { &*self.ptr }
    }
}

impl<T: SharedMemorySafe, Mode: ShmMode> DerefMut for Shm<T, Mode> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: ptr is valid for the lifetime of Shm, guaranteed by:
        // 1. mmap succeeded during construction
        // 2. Memory remains mapped until Drop
        // 3. T: SharedMemorySafe ensures proper layout and concurrent access
        unsafe { &mut *self.ptr }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_create_and_drop() -> io::Result<()> {
        #[repr(C)]
        struct Counter {
            value: AtomicU64,
        }
        unsafe impl SharedMemorySafe for Counter {}

        let path = "/stealth-test-counter";

        // Clean up any leftover
        let _ = shm::unlink(path);

        // Create shared memory
        let counter = Shm::<Counter, Creator>::create(path)?;
        counter.value.store(42, Ordering::SeqCst);

        assert_eq!(counter.value.load(Ordering::SeqCst), 42);

        // Drop will unmap and unlink automatically
        Ok(())
    }

    #[test]
    fn test_shm_creator_and_user() -> io::Result<()> {
        #[repr(C)]
        struct SharedData {
            counter: AtomicU64,
            flag: AtomicBool,
        }
        unsafe impl SharedMemorySafe for SharedData {}

        let path = "/stealth-test-shared";

        // Clean up any leftover
        let _ = shm::unlink(path);

        // Creator process
        {
            let data = Shm::<SharedData, Creator>::create(path)?;
            data.counter.store(100, Ordering::SeqCst);
            data.flag.store(true, Ordering::SeqCst);

            // Simulate another process opening it
            {
                let user_data = Shm::<SharedData, User>::open(path)?;
                assert_eq!(user_data.counter.load(Ordering::SeqCst), 100);
                assert_eq!(user_data.flag.load(Ordering::SeqCst), true);

                // User modifies
                user_data.counter.store(200, Ordering::SeqCst);
            } // User drops (unmap only)

            // Creator sees the change
            assert_eq!(data.counter.load(Ordering::SeqCst), 200);
        } // Creator drops (unmap + unlink)

        Ok(())
    }
}
