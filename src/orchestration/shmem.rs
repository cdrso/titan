//! POSIX shared memory wrapper with type safety and automatic cleanup.
//!
//! This module provides a safe, zero-cost abstraction over POSIX shared memory
//! (`shm_open`, `mmap`) with compile-time guarantees about memory layout and
//! cleanup behavior using Rust's type system.
//!
//! # Overview
//!
//! - [`Shm<T, Mode>`] - Smart pointer to shared memory with typestate-based cleanup
//! - [`SharedMemorySafe`] - Trait marking types safe for cross-process sharing
//! - [`Creator`] - Typestate marker: creates new shared memory, unlinks on drop
//! - [`Opener`] - Typestate marker: opens existing shared memory, no unlink on drop
//!
//! # Basic Usage
//!
//! ```no_run
//! use stealth::orchestration::shmem::*;
//! use std::sync::atomic::{AtomicU64, Ordering};
//!
//! #[repr(C)]
//! struct Counter {
//!     value: AtomicU64,
//! }
//! unsafe impl SharedMemorySafe for Counter {}
//!
//! // Process A: Create and initialize
//! let counter = Shm::<Counter, Creator>::create("/my-counter")?;
//! counter.value.store(42, Ordering::Release);
//!
//! // Process B: Open and read
//! let counter = Shm::<Counter, Opener>::open("/my-counter")?;
//! assert_eq!(counter.value.load(Ordering::Acquire), 42);
//! # Ok::<(), rustix::io::Errno>(())
//! ```
//!
//! # Unsafety Encapsulation
//!
//! This module uses `unsafe` internally but provides a safe public API:
//!
//! ```text
//! Unsafe POSIX operations:    Safe Rust wrappers:
//! ┌──────────────────┐        ┌────────────────────┐
//! │ shm_open()       │───────>│ Shm::create()      │
//! │ mmap()           │───────>│ Shm::open()        │
//! │ munmap()         │───────>│ Drop::drop()       │
//! │ shm_unlink()     │───────>│ Drop::drop()       │
//! │ *mut T           │───────>│ Deref/DerefMut     │
//! └──────────────────┘        └────────────────────┘
//!                              (Safe to use!)
//! ```
//!
//! Safety is guaranteed by:
//! - **Trait bounds**: [`SharedMemorySafe`] enforces layout and content requirements
//! - **Lifetime bounds**: Pointers valid for lifetime of `Shm<T>`
//! - **Typestate pattern**: [`Creator`] vs [`Opener`] enforces cleanup semantics
//! - **RAII**: Drop automatically unmaps memory and unlinks names
//!
//! # Implementing SharedMemorySafe
//!
//! The trait is automatically implemented for primitives, atomics, and arrays.
//! For custom types, you must manually verify all safety properties and implement the trait.
//!
//! See [`SharedMemorySafe`] for detailed requirements and examples.
//!
//! # Cleanup and Crash Handling
//!
//! The typestate pattern ensures correct cleanup:
//! - **[`Creator`]**: Unmaps memory AND unlinks the name on drop
//! - **[`Opener`]**: Only unmaps (name persists for other processes)
//!
//! On daemon startup, clean up any leaked shared memory from crashes:
//!
//! ```no_run
//! # use rustix::shm;
//! // Remove any leftover from previous crashed session
//! let _ = shm::unlink("/my-daemon-inbox");
//!
//! // Create fresh shared memory
//! // ...
//! ```
//!
//! See the [module-level discussion](self#cleanup-and-crash-handling) for details.

use rustix::fs::{Mode, fstat, ftruncate};
use rustix::mm::{MapFlags, ProtFlags, mmap, munmap};
use rustix::{io, shm};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::ptr::{null_mut, write_bytes};
use std::sync::atomic::*;

/// Trait defining cleanup behavior for shared memory modes.
///
/// This is an internal trait used to implement the typestate pattern.
/// Users should use [`Creator`] or [`Opener`] type markers instead of
/// implementing this trait directly.
///
/// # Typestate Pattern
///
/// `Shm<T, Creator>` and `Shm<T, Opener>` are different types with
/// different `Drop` implementations, enforced at compile-time:
///
/// ```text
/// Creator          Opener
///    ↓                ↓
///   Drop            Drop
///    ↓                ↓
/// munmap()        munmap()
/// unlink()        (no unlink)
/// ```
///
/// This prevents accidentally leaving shared memory leaked or
/// prematurely unlinking memory still in use.
pub trait ShmMode {
    /// Whether to unlink the shared memory name on drop.
    ///
    /// - `true` for [`Creator`]: Remove name when last owner drops
    /// - `false` for [`Opener`]: Leave name for creator to clean up
    const SHOULD_UNLINK: bool;
}

/// Typestate marker for processes that create shared memory.
///
/// When `Shm<T, Creator>` is dropped:
/// 1. Memory is unmapped via `munmap()`
/// 2. **Name is unlinked** via `shm_unlink()`
///
/// Use [`Shm::create()`](Shm::create) to create new shared memory as `Creator`.
///
/// # Example
///
/// ```no_run
/// # use stealth::orchestration::shmem::*;
/// # use std::sync::atomic::AtomicU64;
/// # #[repr(C)] struct Counter { value: AtomicU64 }
/// # unsafe impl SharedMemorySafe for Counter {}
/// let counter = Shm::<Counter, Creator>::create("/counter")?;
/// // ... use counter ...
/// // On drop: unmaps AND unlinks "/counter"
/// # Ok::<(), rustix::io::Errno>(())
/// ```
pub struct Creator;
impl ShmMode for Creator {
    const SHOULD_UNLINK: bool = true;
}

/// Typestate marker for processes that open existing shared memory.
///
/// When `Shm<T, Opener>` is dropped:
/// 1. Memory is unmapped via `munmap()`
/// 2. **Name is NOT unlinked** (left for [`Creator`] to clean up)
///
/// Use [`Shm::open()`](Shm::open) to access existing shared memory as `Opener`.
///
/// # Example
///
/// ```no_run
/// # use stealth::orchestration::shmem::*;
/// # use std::sync::atomic::AtomicU64;
/// # #[repr(C)] struct Counter { value: AtomicU64 }
/// # unsafe impl SharedMemorySafe for Counter {}
/// let counter = Shm::<Counter, Opener>::open("/counter")?;
/// // ... use counter ...
/// // On drop: unmaps only, "/counter" name persists
/// # Ok::<(), rustix::io::Errno>(())
/// ```
pub struct Opener;
impl ShmMode for Opener {
    const SHOULD_UNLINK: bool = false;
}

/// Types safe to use in POSIX shared memory across processes.
///
/// This trait marks types that can be safely placed in shared memory and accessed
/// by multiple processes simultaneously. It is `unsafe` to implement because the
/// compiler cannot verify the required memory layout and access pattern guarantees.
///
/// # Provided Implementations
///
/// The trait is automatically implemented for:
/// - **Primitives**: `i8`-`i128`, `u8`-`u128`, `isize`, `usize`, `f32`, `f64`, `bool`
/// - **Atomics**: `AtomicBool`, `AtomicI*`, `AtomicU*` (all sizes)
/// - **Arrays**: `[T; N]` where `T: SharedMemorySafe`
///
/// For custom types (structs, enums), you must manually implement this trait.
///
/// # Safety Properties
///
/// Implementers must guarantee **all** of the following properties:
///
/// | Property | Requirement | Rationale |
/// |----------|-------------|-----------|
/// | **Zero-init** | Valid when all bytes are zero | [`Shm::create()`](Shm::create) zeros memory before exposing `&T` |
/// | **Layout** | `#[repr(C)]` or `#[repr(transparent)]` | Processes may be compiled separately; `#[repr(Rust)]` is unstable |
/// | **Pointers** | No heap/stack pointers or references | Virtual addresses don't transfer across process boundaries |
/// | **Fields** | All fields are `SharedMemorySafe` | Safety constraints apply recursively to nested types |
/// | **Drop** | Safe if `Drop` never runs | Process crashes (SIGKILL) bypass destructors |
/// | **Concurrency** | `Send + Sync` | Multiple processes access memory simultaneously |
///
/// ## Detailed Requirements
///
/// ### Zero-Initialization
///
/// The all-zeros bit pattern must be a valid `T`. This is true for integers, floats, bools,
/// atomics, and arrays thereof. Note: this is about bit-pattern validity, not whether
/// `T::default()` produces zeros.
///
/// ### Layout Stability
///
/// Use `#[repr(C)]` or `#[repr(transparent)]`. Rust's default layout can change between:
/// - Compiler versions (1.75 vs 1.76)
/// - Optimization levels (`-O0` vs `-O3`)
/// - Target platforms (x86 vs ARM)
///
/// ### No Pointers
///
/// **Forbidden**: `Box<T>`, `Vec<T>`, `String`, `&T`, `&mut T`, `*const T`, `*mut T`
/// **Allowed**: Inline data like `[u8; N]`, primitives, atomics
///
/// Virtual memory addresses are process-specific. A pointer valid in Process A points to
/// arbitrary memory in Process B.
///
/// ### Drop Safety
///
/// The type must be safe even if `Drop` never runs. Process crashes bypass destructors.
/// It's fine to use `Drop` for cleanup (closing files, etc.), but not for safety invariants.
///
/// ### Concurrency
///
/// Shared memory is inherently concurrent. Use atomics for synchronization.
/// **Warning**: `std::sync::Mutex` is process-local and won't work across processes
///
/// # Examples
///
/// ## Valid Implementation: Struct
///
/// ```
/// use std::sync::atomic::AtomicUsize;
/// use stealth::orchestration::shmem::SharedMemorySafe;
///
/// #[repr(C)]
/// struct RingBuffer {
///     head: AtomicUsize,
///     tail: AtomicUsize,
///     data: [u8; 4096],
/// }
///
/// // SAFETY:
/// // - Zero-init: AtomicUsize and u8 are valid when zeroed
/// // - Layout: #[repr(C)] ensures stable layout across compilations
/// // - Pointers: All fields inline (AtomicUsize, [u8; N]) - no pointers
/// // - Aliasing: AtomicUsize and [u8; N] are SharedMemorySafe
/// // - Content: No Drop logic - safe without Drop
/// // - Concurrency: Atomics handle concurrent head/tail updates
/// unsafe impl SharedMemorySafe for RingBuffer {}
/// ```
///
/// ## Valid Implementation: Enum
///
/// ```
/// use stealth::orchestration::shmem::SharedMemorySafe;
///
/// #[repr(u8)]
/// enum Status {
///     Idle = 0,
///     Running = 1,
///     Stopped = 2,
/// }
///
/// // SAFETY:
/// // - Zero-init: Idle = 0, so zero bytes represent valid Idle variant
/// // - Layout: #[repr(u8)] gives stable single-byte layout
/// // - Pointers: No pointers - just discriminant
/// // - Aliasing: No fields
/// // - Content: No Drop logic
/// // - Concurrency: Typically wrapped in Atomic or read-only
/// unsafe impl SharedMemorySafe for Status {}
/// ```
///
/// ## Compile-Time Safety
///
/// The trait bound prevents using unsafe types:
///
/// ```compile_fail
/// # use stealth::orchestration::shmem::*;
/// struct MyType { x: u32 }
/// //  Error: MyType doesn't implement SharedMemorySafe
/// let shm = Shm::<MyType, Creator>::create("/test")?;
/// # Ok::<(), rustix::io::Errno>(())
/// ```
///
/// Types that aren't `Send + Sync` cannot implement the trait:
///
/// ```compile_fail
/// # use stealth::orchestration::shmem::SharedMemorySafe;
/// use std::rc::Rc;
/// struct NotSync { data: Rc<u32> }  // Rc is not Send
/// //  Error: NotSync doesn't satisfy Send + Sync
/// unsafe impl SharedMemorySafe for NotSync {}
/// ```
///
/// # See Also
///
/// - [Module-level documentation](self) for usage examples
/// - [`Shm`] for the smart pointer API
pub unsafe trait SharedMemorySafe: Send + Sync {}

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

/// Smart pointer to POSIX shared memory with typestate-based cleanup.
///
/// `Shm<T, Mode>` wraps a pointer to shared memory, providing safe access via
/// [`Deref`]/[`DerefMut`] and automatic cleanup via [`Drop`]. The `Mode` type
/// parameter ([`Creator`] or [`Opener`]) determines cleanup behavior at compile-time.
///
/// # Type Parameters
///
/// - `T`: The type stored in shared memory (must be [`SharedMemorySafe`])
/// - `Mode`: Either [`Creator`] or [`Opener`] (controls cleanup via [`ShmMode`])
///
/// # Type Safety
///
/// The type system enforces correct usage:
/// - Only `Shm<T, Creator>` can call [`create()`](Shm::create)
/// - Only `Shm<T, Opener>` can call [`open()`](Shm::open)
/// - Cleanup behavior matches ownership at compile-time
///
/// # Memory Layout
///
/// ```text
/// POSIX Shared Memory Object: "/my-shm"
/// ┌────────────────────────────────┐
/// │  Kernel Memory                 │
/// │  ┌──────────────────────────┐  │
/// │  │  T: Your data structure  │  │
/// │  │  (size_of::<T>() bytes)  │  │
/// │  └──────────────────────────┘  │
/// │         ↑            ↑          │
/// └─────────┼────────────┼──────────┘
///           │            │
///      Process A    Process B
///      (Creator)    (Opener)
///         ptr          ptr
/// ```
///
/// Both processes access the **same physical memory** via their own virtual addresses.
///
/// # Cleanup Behavior
///
/// Cleanup is automatic via [`Drop`]:
///
/// | Mode | On Drop | Kernel Action |
/// |------|---------|---------------|
/// | [`Creator`] | `munmap()` + `shm_unlink()` | Unmaps memory, removes name |
/// | [`Opener`] | `munmap()` only | Unmaps memory, name persists |
///
/// Memory is freed by the kernel when:
/// 1. Name is unlinked (by [`Creator`])
/// 2. All processes have unmapped (reference count = 0)
///
/// # Examples
///
/// See [module-level documentation](self) for detailed examples.
///
/// ## Basic Creator
///
/// ```no_run
/// # use stealth::orchestration::shmem::*;
/// # use std::sync::atomic::AtomicU64;
/// # #[repr(C)] struct Counter { value: AtomicU64 }
/// # unsafe impl SharedMemorySafe for Counter {}
/// use std::sync::atomic::Ordering;
///
/// let counter = Shm::<Counter, Creator>::create("/counter")?;
/// counter.value.store(42, Ordering::Release);
/// # Ok::<(), rustix::io::Errno>(())
/// ```
///
/// ## Basic Opener
///
/// ```no_run
/// # use stealth::orchestration::shmem::*;
/// # use std::sync::atomic::AtomicU64;
/// # #[repr(C)] struct Counter { value: AtomicU64 }
/// # unsafe impl SharedMemorySafe for Counter {}
/// use std::sync::atomic::Ordering;
///
/// let counter = Shm::<Counter, Opener>::open("/counter")?;
/// let value = counter.value.load(Ordering::Acquire);
/// # Ok::<(), rustix::io::Errno>(())
/// ```
///
/// # Safety Invariants
///
/// This type maintains the following invariants:
/// - **Allocated**: `ptr` points to `size` bytes allocated via `mmap()`
/// - **Mapped**: Memory remains mapped for the lifetime of `Shm<T>`
/// - **Aligned**: `ptr` is properly aligned for `T`
/// - **Valid**: `T` is [`SharedMemorySafe`], ensuring correct layout and access patterns
pub struct Shm<T: SharedMemorySafe, Mode: ShmMode> {
    ptr: *mut T,
    size: usize,
    path: String,
    _mode: PhantomData<Mode>,
}

// SAFETY: Shm<T> can be sent between threads if T is Send + Sync.
// T: SharedMemorySafe already requires Send + Sync.
// The raw pointer is safe to send because it points to shared memory,
// not thread-local data.
unsafe impl<T: SharedMemorySafe, Mode: ShmMode> Send for Shm<T, Mode> {}

// SAFETY: Multiple threads can hold &Shm<T> if T is Sync.
// T: SharedMemorySafe already requires Sync.
// Shared memory is designed for concurrent access.
unsafe impl<T: SharedMemorySafe, Mode: ShmMode> Sync for Shm<T, Mode> {}

// Constructor for Creator mode
impl<T: SharedMemorySafe> Shm<T, Creator> {
    /// Creates new shared memory and maps it into the address space.
    ///
    /// This creates a new POSIX shared memory object with the given name, resizes it
    /// to hold `T`, and maps it with read-write permissions. On drop, the memory will
    /// be unmapped and the name will be unlinked.
    ///
    /// # Arguments
    ///
    /// * `path` - The shared memory object name (e.g., `"/my-shm"`). Must start with `/`
    ///   and contain no other slashes (see POSIX `shm_open` requirements).
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - `path` is invalid (doesn't start with `/`, too long, etc.)
    /// - Shared memory object already exists at `path` (`EEXIST`)
    /// - Insufficient permissions to create shared memory (`EACCES`)
    /// - Out of memory (`ENOMEM`)
    /// - System limit on shared memory objects reached (`EMFILE`, `ENFILE`)
    /// - Memory mapping fails (e.g., address space exhausted)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use stealth::orchestration::shmem::*;
    /// # use std::sync::atomic::AtomicU64;
    /// # #[repr(C)] struct Counter { value: AtomicU64 }
    /// # unsafe impl SharedMemorySafe for Counter {}
    /// let counter = Shm::<Counter, Creator>::create("/my-counter")?;
    /// # Ok::<(), rustix::io::Errno>(())
    /// ```
    pub fn create(path: &str) -> io::Result<Self> {
        // Create the shared memory object exclusively (fail if exists)
        let fd = shm::open(
            path,
            shm::OFlags::CREATE | shm::OFlags::EXCL | shm::OFlags::RDWR,
            Mode::RUSR | Mode::WUSR,
        )?;

        // Resize to hold T
        if let Err(e) = ftruncate(&fd, size_of::<T>() as u64) {
            // Clean up on error
            drop(fd);
            let _ = shm::unlink(path);
            return Err(e);
        }

        // Map into our address space
        //
        // SAFETY: We are creating a new independent mapping that doesn't alias
        // any existing Rust objects:
        // - Allocated: mmap will allocate exactly size_of::<T>() bytes from kernel
        // - Size valid: ftruncate succeeded, so object is correct size
        // - FD valid: shm_open succeeded, fd refers to valid shared memory object
        // - No aliasing: Fresh mapping from kernel, not overlapping existing memory
        // - Permissions: READ|WRITE matches our use case (Deref/DerefMut)
        let ptr = unsafe {
            mmap(
                null_mut(),
                size_of::<T>(),
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                &fd,
                0,
            )
        };

        // Clean up on mmap failure
        let ptr = match ptr {
            Ok(p) => p,
            Err(e) => {
                drop(fd);
                let _ = shm::unlink(path);
                return Err(e);
            }
        };

        // fd can be dropped here - mapping persists independently
        drop(fd);

        // Zero the memory to ensure valid initial state
        //
        // SAFETY: ptr points to size_of::<T>() bytes that we just mapped.
        // Zeroing ensures that the memory contains a valid representation of T
        // (SharedMemorySafe requires types to be valid when zero-initialized).
        unsafe {
            write_bytes(ptr, 0, size_of::<T>());
        }

        Ok(Self {
            ptr: ptr as *mut T,
            size: size_of::<T>(),
            path: path.to_string(),
            _mode: PhantomData,
        })
    }
}

// Constructor for Opener mode
impl<T: SharedMemorySafe> Shm<T, Opener> {
    /// Opens existing shared memory and maps it into the address space.
    ///
    /// This opens a POSIX shared memory object that was created by another process
    /// and maps it with read-write permissions. On drop, the memory will be unmapped
    /// but the name will NOT be unlinked (the creator is responsible for cleanup).
    ///
    /// # Arguments
    ///
    /// * `path` - The shared memory object name (e.g., `"/my-shm"`). Must match the
    ///   name used by the creator.
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - Shared memory object doesn't exist (`ENOENT`)
    /// - Insufficient permissions to access the object (`EACCES`)
    /// - Object size doesn't match `size_of::<T>()` exactly (`EINVAL`)
    /// - Out of memory or address space exhausted (`ENOMEM`)
    /// - System limit on memory mappings reached
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use stealth::orchestration::shmem::*;
    /// # use std::sync::atomic::AtomicU64;
    /// # #[repr(C)] struct Counter { value: AtomicU64 }
    /// # unsafe impl SharedMemorySafe for Counter {}
    /// // Open shared memory created by another process
    /// let counter = Shm::<Counter, Opener>::open("/my-counter")?;
    /// # Ok::<(), rustix::io::Errno>(())
    /// ```
    pub fn open(path: &str) -> io::Result<Self> {
        // Open existing shared memory object
        let fd = shm::open(path, shm::OFlags::RDWR, Mode::empty())?;

        // Validate that the object size matches our type
        let stat = fstat(&fd)?;
        let expected_size = size_of::<T>() as i64;
        if stat.st_size != expected_size {
            drop(fd);
            return Err(io::Errno::INVAL.into());
        }

        // Map into our address space
        //
        // SAFETY: We are mapping existing shared memory that doesn't alias
        // any existing Rust objects in this process:
        // - Allocated: Shared memory exists (shm_open succeeded)
        // - Size valid: fstat confirmed st_size == size_of::<T>()
        // - FD valid: shm_open succeeded, fd refers to valid shared memory object
        // - No aliasing: Mapping in this process doesn't alias other local objects
        // - Permissions: READ|WRITE matches our use case (Deref/DerefMut)
        // - Concurrent access: T: SharedMemorySafe ensures safe cross-process access
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

        // fd can be dropped here - mapping persists independently
        drop(fd);

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
                let opener_data = Shm::<SharedData, Opener>::open(path)?;
                assert_eq!(opener_data.counter.load(Ordering::SeqCst), 100);
                assert_eq!(opener_data.flag.load(Ordering::SeqCst), true);

                // Opener modifies
                opener_data.counter.store(200, Ordering::SeqCst);
            } // Opener drops (unmap only)

            // Creator sees the change
            assert_eq!(data.counter.load(Ordering::SeqCst), 200);
        } // Creator drops (unmap + unlink)

        Ok(())
    }
}
