//! POSIX shared memory wrapper with type safety and automatic cleanup.
//!
//! This module provides a safe, zero-cost abstraction over POSIX shared memory
//! (`shm_open`, `mmap`) with compile-time guarantees about memory layout and
//! cleanup behavior
//!
//! # Overview
//!
//! - [`Shm<T, Mode>`] - Smart pointer to shared memory
//! - [`SharedMemorySafe`] - Trait marking types
//! - [`Creator`] - Typestate marker: creates new shared memory, unlinks on drop
//! - [`Opener`] - Typestate marker: opens existing shared memory, no unlink on drop
//!
//! # Basic Usage
//!
//! ```no_run
//! use titan::SharedMemorySafe;
//! use titan::ipc::shmem::*;
//! use std::sync::atomic::{AtomicU64, Ordering};
//!
//! #[derive(SharedMemorySafe)]
//! #[repr(C)]
//! struct Counter {
//!     value: AtomicU64,
//! }
//!
//! impl Counter {
//!     unsafe fn init_shared(ptr: *mut Self) {
//!         unsafe {
//!             std::ptr::addr_of_mut!((*ptr).value).write(AtomicU64::new(0));
//!         }
//!     }
//! }
//!
//! // Process A: Create and initialize in-place
//! let counter = Shm::<Counter, Creator>::create("/my-counter", |ptr| unsafe {
//!     Counter::init_shared(ptr);
//! })?;
//! counter.value.store(42, Ordering::Release);
//!
//! // Process B: Open and read
//! let counter = Shm::<Counter, Opener>::open("/my-counter")?;
//! assert_eq!(counter.value.load(Ordering::Acquire), 42);
//! # Ok::<(), ShmError>(())
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
//! │ *mut T           │───────>│ Deref              │
//! └──────────────────┘        └────────────────────┘
//! ```
//!
//! Safety is guaranteed by:
//! - **Trait bounds**: [`SharedMemorySafe`] enforces layout and content requirements
//! - **Lifetime bounds**: Pointers valid for lifetime of `Shm<T>`
//! - **Typestate pattern**: [`Creator`] vs [`Opener`] enforces cleanup semantics
//! - **RAII**: Drop automatically unmaps memory and unlinks names
//!
//! # Implementing `SharedMemorySafe`
//!
//! The trait is automatically implemented for primitives, atomics, and arrays.
//! For custom types, use the `#[derive(SharedMemorySafe)]` macro:
//!
//! ```
//! use titan::SharedMemorySafe;
//! use std::sync::atomic::AtomicUsize;
//!
//! #[derive(SharedMemorySafe)]
//! #[repr(C)]
//! struct RingBuffer {
//!     head: AtomicUsize,
//!     tail: AtomicUsize,
//!     data: [u8; 4096],
//! }
//! ```
//!
//! The derive macro checks at compile time:
//! - `#[repr(C)]` or `#[repr(transparent)]` is present
//! - No obvious pointer types (Vec, Box, String, &, *, etc.)
//! - All fields implement `SharedMemorySafe`
//!
//! Types used with [`Shm::create()`] must provide an initialization function
//! that writes fields directly into the mmap'd memory.
//!
//! See [`SharedMemorySafe`] for detailed requirements.
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
use rustix::mm::{Advice, MapFlags, ProtFlags, madvise, mmap, munmap};
use rustix::{io, shm};
use std::fmt;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::ptr::{NonNull, null_mut};
use std::sync::atomic::{
    AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicU8, AtomicU16,
    AtomicU32, AtomicU64, AtomicUsize,
};

/// Result alias for shared memory operations.
pub type Result<T> = std::result::Result<T, ShmError>;

/// Contextual errors produced by [`Shm`].
#[derive(Debug)]
pub enum ShmError {
    /// The provided POSIX shared memory name is invalid.
    InvalidPath { path: String, reason: &'static str },
    /// `mmap`, `shm_open`, `ftruncate`, etc. failed with an errno.
    PosixError {
        op: &'static str,
        path: String,
        source: io::Errno,
    },
    /// The existing shared memory object has a different size than `T`.
    SizeMismatch {
        path: String,
        expected: usize,
        actual: i64,
    },
    /// Timed out waiting for shared memory to be initialized by creator.
    InitTimeout { path: String },
}

impl ShmError {
    fn posix(op: &'static str, path: &str, err: io::Errno) -> Self {
        Self::PosixError {
            op,
            path: path.to_string(),
            source: err,
        }
    }
}

impl fmt::Display for ShmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath { path, reason } => {
                write!(f, "invalid shared memory path `{path}`: {reason}")
            }
            Self::PosixError { op, path, source } => {
                write!(f, "{op} failed for `{path}`: {source}")
            }
            Self::SizeMismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "shared memory `{path}` size mismatch: expected {expected} bytes, got {actual}"
            ),
            Self::InitTimeout { path } => {
                write!(f, "timed out waiting for `{path}` to be initialized")
            }
        }
    }
}

impl std::error::Error for ShmError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PosixError { source, .. } => Some(source),
            _ => None,
        }
    }
}

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
/// # use titan::ipc::shmem::*;
/// # use std::sync::atomic::AtomicU64;
/// # #[repr(C)] struct Counter { value: AtomicU64 }
/// # unsafe impl SharedMemorySafe for Counter {}
/// # impl Counter {
/// #     unsafe fn init_shared(ptr: *mut Self) {
/// #         std::ptr::addr_of_mut!((*ptr).value).write(AtomicU64::new(0));
/// #     }
/// # }
/// let counter = Shm::<Counter, Creator>::create("/counter", |ptr| unsafe {
///     Counter::init_shared(ptr);
/// })?;
/// // ... use counter ...
/// // On drop: unmaps AND unlinks "/counter"
/// # Ok::<(), ShmError>(())
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
/// # use titan::ipc::shmem::*;
/// # use std::sync::atomic::AtomicU64;
/// # #[repr(C)] struct Counter { value: AtomicU64 }
/// # unsafe impl SharedMemorySafe for Counter {}
/// let counter = Shm::<Counter, Opener>::open("/counter")?;
/// // ... use counter ...
/// // On drop: unmaps only, "/counter" name persists
/// # Ok::<(), ShmError>(())
/// ```
pub struct Opener;
impl ShmMode for Opener {
    const SHOULD_UNLINK: bool = false;
}

/// Types safe to use in POSIX shared memory across processes.
///
/// This trait marks types that can be safely placed in shared memory and accessed
/// by multiple processes simultaneously.
///
/// # Using the Derive Macro
///
/// For custom types, use `#[derive(SharedMemorySafe)]`:
///
/// ```
/// use titan::SharedMemorySafe;
/// use std::sync::atomic::AtomicUsize;
///
/// #[derive(SharedMemorySafe)]
/// #[repr(C)]
/// struct MyType {
///     value: AtomicUsize,
///     data: [u8; 1024],
/// }
/// ```
///
/// The macro automatically checks `#[repr(C)]`, detects pointer types, and generates
/// appropriate trait bounds.
///
/// # Provided Implementations
///
/// The trait is automatically implemented for:
/// - **Primitives**: `i8`-`i128`, `u8`-`u128`, `isize`, `usize`, `f32`, `f64`, `bool`
/// - **Atomics**: `AtomicBool`, `AtomicI*`, `AtomicU*` (all sizes)
/// - **Arrays**: `[T; N]` where `T: SharedMemorySafe`
///
/// # Safety
///
/// Implementers must guarantee **all** of the following properties:
///
/// | Property | Requirement | Rationale |
/// |----------|-------------|-----------|
/// | **Initialization** | In-place initializer provided to `create()` | Avoids stack overflow for large types by writing directly to mmap'd memory |
/// | **Layout** | `#[repr(C)]` or `#[repr(transparent)]` | Processes may be compiled separately; `#[repr(Rust)]` is unstable |
/// | **Pointers** | No heap/stack pointers or references | Virtual addresses don't transfer across process boundaries |
/// | **Fields** | All fields are `SharedMemorySafe` | Safety constraints apply recursively to nested types |
/// | **Drop** | Safe if `Drop` never runs | Process crashes (SIGKILL) bypass destructors |
/// | **Concurrency** | `Send + Sync` | Multiple processes access memory simultaneously |
///
/// ## Detailed Requirements
///
/// ### In-Place Initialization
///
/// Types used with [`Shm::create()`] must provide an initialization closure that writes
/// fields directly into the mmap'd memory. This avoids stack overflow for large types
/// (e.g., types with multi-MB embedded arrays). The typical pattern is to define an
/// `unsafe fn init_shared(ptr: *mut Self)` method that uses `std::ptr::addr_of_mut!`
/// to initialize each field individually.
///
/// ### Layout Stability
///
/// Use `#[repr(C)]` or `#[repr(transparent)]`. Rust's default layout can change between:
/// - Compiler versions
/// - Optimization levels
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
/// ## Using Derive Macro: Struct
///
/// ```
/// use titan::SharedMemorySafe;
/// use std::sync::atomic::AtomicUsize;
///
/// #[derive(SharedMemorySafe)]
/// #[repr(C)]
/// struct RingBuffer {
///     head: AtomicUsize,
///     tail: AtomicUsize,
///     data: [u8; 4096],
/// }
/// ```
///
/// The macro checks:
/// - `#[repr(C)]` is present
/// - All fields are `SharedMemorySafe`
/// - No pointer types (Vec, Box, String, etc.)
///
/// ## Compile-Time Safety
///
/// The trait bound prevents using unsafe types:
///
/// ```compile_fail
/// # use titan::ipc::shmem::*;
/// struct MyType { x: u32 }
/// //  Error: MyType doesn't implement SharedMemorySafe
/// let shm = Shm::<MyType, Creator>::create("/test", |ptr| unsafe {
///     std::ptr::write(ptr, MyType { x: 0 });
/// })?;
/// # Ok::<(), ShmError>(())
/// ```
///
/// Types that aren't `Send + Sync` cannot implement the trait:
///
/// ```compile_fail
/// # use titan::ipc::shmem::SharedMemorySafe;
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

// Manual implementations for primitives and atomics
macro_rules! impl_shared_memory_safe {
    ($($t:ty),* $(,)?) => {
        $(
            // SAFETY: Primitive and atomic types satisfy all SharedMemorySafe requirements:
            // - Have stable, well-defined layout (primitives and atomics have fixed representation)
            // - Contain no pointers or references (they are value types)
            // - Are Send + Sync by definition
            // - Safe if Drop never runs (no Drop impl for primitives/atomics)
            // - Can be safely shared across process boundaries (same representation everywhere)
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

// SAFETY: Arrays are SharedMemorySafe when their elements are because:
// - Array layout in repr(C) is well-defined: contiguous elements of type T
// - If T contains no pointers, [T; N] contains no pointers
// - If T is Send + Sync, so is [T; N]
// - Arrays have no Drop impl if T has no Drop impl
// - Elements remain valid across process boundaries if T does
unsafe impl<T: SharedMemorySafe, const N: usize> SharedMemorySafe for [T; N] {}

// SAFETY: MaybeUninit<T> is SharedMemorySafe when T is because:
// - MaybeUninit<T> has the same layout as T (repr(transparent) over union containing T)
// - If T contains no pointers, MaybeUninit<T> contains no pointers
// - If T is Send + Sync, so is MaybeUninit<T>
// - MaybeUninit has no Drop impl
unsafe impl<T: SharedMemorySafe> SharedMemorySafe for std::mem::MaybeUninit<T> {}

/// Smart pointer to POSIX shared memory with typestate-based cleanup.
///
/// `Shm<T, Mode>` wraps a pointer to shared memory, providing safe access via
/// [`Deref`] and automatic cleanup via [`Drop`]. The `Mode` type
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
/// │         ↑            ↑         │
/// └─────────┼────────────┼─────────┘
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
/// # use titan::ipc::shmem::*;
/// # use std::sync::atomic::AtomicU64;
/// # #[repr(C)] struct Counter { value: AtomicU64 }
/// # unsafe impl SharedMemorySafe for Counter {}
/// # impl Counter {
/// #     unsafe fn init_shared(ptr: *mut Self) {
/// #         std::ptr::addr_of_mut!((*ptr).value).write(AtomicU64::new(0));
/// #     }
/// # }
/// use std::sync::atomic::Ordering;
///
/// let counter = Shm::<Counter, Creator>::create("/counter", |ptr| unsafe {
///     Counter::init_shared(ptr);
/// })?;
/// counter.value.store(42, Ordering::Release);
/// # Ok::<(), ShmError>(())
/// ```
///
/// ## Basic Opener
///
/// ```no_run
/// # use titan::ipc::shmem::*;
/// # use std::sync::atomic::AtomicU64;
/// # #[repr(C)] struct Counter { value: AtomicU64 }
/// # unsafe impl SharedMemorySafe for Counter {}
/// use std::sync::atomic::Ordering;
///
/// let counter = Shm::<Counter, Opener>::open("/counter")?;
/// let value = counter.value.load(Ordering::Acquire);
/// # Ok::<(), ShmError>(())
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
    ptr: NonNull<T>,
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
unsafe impl<T: SharedMemorySafe, Mode: ShmMode> Sync for Shm<T, Mode> {}

const POSIX_NAME_MAX: usize = 255;

/// Validates that a path meets POSIX `shm_open` requirements.
///
/// For portable use, POSIX requires:
/// - Must start with '/'
/// - Must not contain additional slashes after the first
/// - Must not exceed `NAME_MAX` (255 characters)
fn validate_shm_path(path: &str) -> Result<()> {
    if !path.starts_with('/') {
        return Err(ShmError::InvalidPath {
            path: path.to_string(),
            reason: "path must start with '/'",
        });
    }

    if path[1..].contains('/') {
        return Err(ShmError::InvalidPath {
            path: path.to_string(),
            reason: "path must not contain additional '/' characters",
        });
    }

    if path.len() > POSIX_NAME_MAX {
        return Err(ShmError::InvalidPath {
            path: path.to_string(),
            reason: "path length must be <= 255 bytes",
        });
    }

    Ok(())
}

// Constructor for Creator mode
impl<T: SharedMemorySafe> Shm<T, Creator> {
    /// Creates new shared memory and initializes it in place.
    ///
    /// This function mmaps shared memory and calls the provided initializer with
    /// a raw pointer to the uninitialized region. The initializer must fully
    /// initialize all bytes of `T` to avoid undefined behavior.
    ///
    /// # Arguments
    ///
    /// * `path` - The shared memory object name (e.g., `"/my-shm"`). Must start with `/`
    ///   and contain no other slashes (see POSIX `shm_open` requirements).
    /// * `init` - Closure that initializes the memory at the provided pointer.
    ///
    /// # Safety
    ///
    /// The `init` closure is responsible for fully initializing the memory. Failing
    /// to initialize all fields will result in undefined behavior when accessing the
    /// shared memory.
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
    /// # Panics
    ///
    /// Panics if the `init` closure panics. The shared memory object will be
    /// properly cleaned up (unmapped and unlinked) before the panic propagates.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::shmem::*;
    /// # use std::sync::atomic::AtomicU64;
    /// # #[repr(C)] struct Counter { value: AtomicU64 }
    /// # unsafe impl SharedMemorySafe for Counter {}
    /// # impl Counter {
    /// #     unsafe fn init_shared(ptr: *mut Self) {
    /// #         std::ptr::addr_of_mut!((*ptr).value).write(AtomicU64::new(0));
    /// #     }
    /// # }
    /// // Initialize in-place without stack allocation
    /// let counter = Shm::<Counter, Creator>::create("/my-counter", |ptr| unsafe {
    ///     Counter::init_shared(ptr);
    /// })?;
    /// # Ok::<(), ShmError>(())
    /// ```
    pub fn create<F>(path: &str, init: F) -> Result<Self>
    where
        F: FnOnce(*mut T),
    {
        validate_shm_path(path)?;

        let fd = shm::open(
            path,
            shm::OFlags::CREATE | shm::OFlags::EXCL | shm::OFlags::RDWR,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|err| ShmError::posix("shm_open", path, err))?;

        if let Err(e) = ftruncate(&fd, size_of::<T>() as u64) {
            let _ = shm::unlink(path);
            return Err(ShmError::posix("ftruncate", path, e));
        }

        // SAFETY: mmap is called with:
        // - null_mut() to let the kernel choose the address
        // - size_of::<T>() which is the exact size we need
        // - READ | WRITE permissions as we need to initialize and use the memory
        // - SHARED flag for cross-process access
        // - Valid file descriptor from shm_open
        // - Offset 0 to map from the beginning
        let ptr_result = unsafe {
            mmap(
                null_mut(),
                size_of::<T>(),
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                &fd,
                0,
            )
        };

        let ptr = match ptr_result {
            Ok(p) => p,
            Err(err) => {
                let _ = shm::unlink(path);
                return Err(ShmError::posix("mmap", path, err));
            }
        };

        // Best-effort huge page hint; ignored if unsupported.
        unsafe {
            let _ = madvise(ptr, size_of::<T>(), Advice::LinuxHugepage);
        }

        // SAFETY: mmap returns a non-null pointer on success (checked above via Ok branch),
        // and the pointer is properly aligned for T as guaranteed by mmap for page-aligned memory
        let ptr = unsafe { NonNull::new_unchecked(ptr.cast::<T>()) };

        let shm = Self {
            ptr,
            size: size_of::<T>(),
            path: path.to_string(),
            _mode: PhantomData,
        };

        let init_result = catch_unwind(AssertUnwindSafe(|| init(shm.ptr.as_ptr())));

        match init_result {
            Ok(()) => Ok(shm),
            Err(payload) => {
                drop(shm);
                resume_unwind(payload);
            }
        }
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
    /// # use titan::ipc::shmem::*;
    /// # use std::sync::atomic::AtomicU64;
    /// # #[repr(C)] struct Counter { value: AtomicU64 }
    /// # unsafe impl SharedMemorySafe for Counter {}
    /// // Open shared memory created by another process
    /// let counter = Shm::<Counter, Opener>::open("/my-counter")?;
    /// # Ok::<(), ShmError>(())
    /// ```
    pub fn open(path: &str) -> Result<Self> {
        validate_shm_path(path)?;

        let fd = shm::open(path, shm::OFlags::RDWR, Mode::empty())
            .map_err(|err| ShmError::posix("shm_open", path, err))?;

        let stat = match fstat(&fd) {
            Ok(stat) => stat,
            Err(err) => {
                return Err(ShmError::posix("fstat", path, err));
            }
        };

        let expected_size = size_of::<T>();

        let actual_size = usize::try_from(stat.st_size).map_err(|_| ShmError::PosixError {
            op: "fstat",
            path: path.to_string(),
            source: io::Errno::INVAL,
        })?;

        if actual_size != expected_size {
            return Err(ShmError::SizeMismatch {
                path: path.to_string(),
                expected: size_of::<T>(),
                actual: stat.st_size,
            });
        }

        // Map into our address space
        let ptr_result = unsafe {
            // SAFETY: We are mapping existing shared memory that doesn't alias
            // any existing Rust objects in this process:
            // - Allocated: Shared memory exists (shm_open succeeded)
            // - Size valid: fstat confirmed st_size == size_of::<T>()
            // - FD valid: shm_open succeeded, fd refers to valid shared memory object
            // - Aligned: mmap returns page-aligned addresses (typically 4KB), satisfying any T's alignment
            // - No aliasing: Mapping in this process doesn't alias other local objects
            // - Permissions: READ|WRITE for interior mutability via atomics
            // - Initialized: Creator must have initialized before sharing
            // - Concurrent access: T: SharedMemorySafe ensures safe cross-process access
            mmap(
                null_mut(),
                size_of::<T>(),
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                &fd,
                0,
            )
        };

        let ptr = match ptr_result {
            Ok(p) => p,
            Err(err) => {
                return Err(ShmError::posix("mmap", path, err));
            }
        };

        // Best-effort huge page hint; ignored if unsupported.
        unsafe {
            let _ = madvise(ptr, size_of::<T>(), Advice::LinuxHugepage);
        }

        // SAFETY: mmap never returns null on success, so this is safe to wrap in NonNull
        let ptr = unsafe { NonNull::new_unchecked(ptr.cast::<T>()) };

        Ok(Self {
            ptr,
            size: size_of::<T>(),
            path: path.to_string(),
            _mode: PhantomData,
        })
    }
}

impl<T: SharedMemorySafe, Mode: ShmMode> Drop for Shm<T, Mode> {
    fn drop(&mut self) {
        // SAFETY: munmap is called with:
        // - A pointer that was returned by mmap (stored in self.ptr)
        // - The exact size that was originally mapped (stored in self.size)
        // The memory region is still valid as we're in drop, meaning no other
        // code has unmapped it yet
        unsafe {
            let _ = munmap(self.ptr.as_ptr().cast(), self.size);
        }

        if Mode::SHOULD_UNLINK {
            let _ = shm::unlink(&self.path);
        }
    }
}

impl<T: SharedMemorySafe, Mode: ShmMode> Deref for Shm<T, Mode> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: ptr is valid for the lifetime of Shm, guaranteed by:
        // 1. mmap succeeded during construction
        // 2. Memory remains mapped until Drop
        // 3. T: SharedMemorySafe ensures proper layout and concurrent access
        unsafe { &*self.ptr.as_ptr() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_shm_create_and_drop() -> Result<()> {
        use crate::SharedMemorySafe;

        #[derive(SharedMemorySafe)]
        #[repr(C)]
        struct Counter {
            value: AtomicU64,
        }

        impl Counter {
            unsafe fn init_shared(ptr: *mut Self) {
                unsafe {
                    std::ptr::addr_of_mut!((*ptr).value).write(AtomicU64::new(0));
                }
            }
        }

        let path = "/titan-test-counter";

        // Clean up any leftover
        let _ = shm::unlink(path);

        let counter = match Shm::<Counter, Creator>::create(path, |ptr| unsafe {
            Counter::init_shared(ptr);
        }) {
            Ok(counter) => counter,
            Err(err @ ShmError::PosixError { source, .. }) if source == io::Errno::ACCESS => {
                eprintln!("Skipping test_shm_create_and_drop: {err}");
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        counter.value.store(42, Ordering::SeqCst);

        assert_eq!(counter.value.load(Ordering::SeqCst), 42);

        Ok(())
    }

    #[test]
    fn test_shm_creator_and_user() -> Result<()> {
        use crate::SharedMemorySafe;

        #[derive(SharedMemorySafe)]
        #[repr(C)]
        struct SharedData {
            counter: AtomicU64,
            flag: AtomicBool,
        }

        impl SharedData {
            unsafe fn init_shared(ptr: *mut Self) {
                unsafe {
                    std::ptr::addr_of_mut!((*ptr).counter).write(AtomicU64::new(0));
                    std::ptr::addr_of_mut!((*ptr).flag).write(AtomicBool::new(false));
                }
            }
        }

        let path = "/titan-test-shared";

        let _ = shm::unlink(path);

        // Creator process
        {
            let data = match Shm::<SharedData, Creator>::create(path, |ptr| unsafe {
                SharedData::init_shared(ptr);
            }) {
                Ok(data) => data,
                Err(err @ ShmError::PosixError { source, .. }) if source == io::Errno::ACCESS => {
                    eprintln!("Skipping test_shm_creator_and_user: {err}");
                    return Ok(());
                }
                Err(err) => return Err(err),
            };
            data.counter.store(100, Ordering::SeqCst);
            data.flag.store(true, Ordering::SeqCst);

            // Simulate another process opening it
            {
                let opener_data = Shm::<SharedData, Opener>::open(path)?;
                assert_eq!(opener_data.counter.load(Ordering::SeqCst), 100);
                assert!(opener_data.flag.load(Ordering::SeqCst));

                // Opener modifies
                opener_data.counter.store(200, Ordering::SeqCst);
            } // Opener drops (unmap only)

            // Creator sees the change
            assert_eq!(data.counter.load(Ordering::SeqCst), 200);
        } // Creator drops (unmap + unlink)

        Ok(())
    }

    #[test]
    fn test_validate_shm_path_valid() {
        assert!(validate_shm_path("/valid").is_ok());
        assert!(validate_shm_path("/valid-name").is_ok());
        assert!(validate_shm_path("/valid_name_123").is_ok());
    }

    #[test]
    fn test_validate_shm_path_no_leading_slash() {
        let result = validate_shm_path("no-slash");
        assert!(matches!(
            result,
            Err(ShmError::InvalidPath { reason, .. }) if reason == "path must start with '/'"
        ));
    }

    #[test]
    fn test_validate_shm_path_extra_slashes() {
        let result = validate_shm_path("/foo/bar");
        assert!(matches!(
            result,
            Err(ShmError::InvalidPath { reason, .. })
                if reason == "path must not contain additional '/' characters"
        ));

        let result = validate_shm_path("/foo/bar/baz");
        assert!(matches!(
            result,
            Err(ShmError::InvalidPath { reason, .. })
                if reason == "path must not contain additional '/' characters"
        ));
    }

    #[test]
    fn test_validate_shm_path_too_long() {
        let long_path = format!("/{}", "a".repeat(255));
        let result = validate_shm_path(&long_path);
        assert!(matches!(
            result,
            Err(ShmError::InvalidPath { reason, .. })
                if reason == "path length must be <= 255 bytes"
        ));
    }

    #[test]
    fn test_validate_shm_path_max_length() {
        // 255 chars total including the leading slash
        let max_path = format!("/{}", "a".repeat(254));
        assert!(validate_shm_path(&max_path).is_ok());
    }

    #[test]
    fn test_shm_open_size_mismatch() -> Result<()> {
        use crate::SharedMemorySafe;

        #[derive(SharedMemorySafe)]
        #[repr(C)]
        struct Small {
            value: AtomicU64,
        }

        #[derive(SharedMemorySafe)]
        #[repr(C)]
        struct Large {
            a: AtomicU64,
            b: AtomicU64,
            c: AtomicU64,
        }

        impl Small {
            unsafe fn init_shared(ptr: *mut Self) {
                unsafe {
                    std::ptr::addr_of_mut!((*ptr).value).write(AtomicU64::new(0));
                }
            }
        }

        let path = "/titan-test-size-mismatch";

        let _ = shm::unlink(path);

        // Create with Small type
        let _small = match Shm::<Small, Creator>::create(path, |ptr| unsafe {
            Small::init_shared(ptr);
        }) {
            Ok(shm) => shm,
            Err(err @ ShmError::PosixError { source, .. }) if source == io::Errno::ACCESS => {
                eprintln!("Skipping test_shm_open_size_mismatch: {err}");
                return Ok(());
            }
            Err(err) => return Err(err),
        };

        // Try to open with Large type - should fail with SizeMismatch
        let result = Shm::<Large, Opener>::open(path);
        match result {
            Err(ShmError::SizeMismatch {
                expected, actual, ..
            }) => {
                assert_eq!(expected, std::mem::size_of::<Large>());
                assert_eq!(actual, i64::try_from(std::mem::size_of::<Small>()).unwrap());
            }
            Err(e) => panic!("Expected SizeMismatch error, got: {e}"),
            Ok(_) => panic!("Expected SizeMismatch error, but open() succeeded"),
        }

        Ok(())
    }
}
