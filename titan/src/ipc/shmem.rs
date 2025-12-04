//! POSIX shared memory wrapper with type safety and automatic cleanup.
//!
//! This module provides a safe abstraction over POSIX shared memory (`shm_open`,
//! `mmap`) with compile-time guarantees about memory layout and cleanup behavior.
//!
//! # Overview
//!
//! - [`Shm<T, Mode>`] - Smart pointer to shared memory with marker-based cleanup
//! - [`SharedMemorySafe`] - Marker trait for types safe to share across processes
//! - [`Creator`] / [`Opener`] - Marker types selecting cleanup behavior
//!
//! # Example
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
//! let path = ShmPath::new("/my-counter").unwrap();
//!
//! // Process A: Create and initialize
//! let counter = Shm::<Counter, Creator>::create(path.clone(), |uninit| {
//!     uninit.write(Counter { value: AtomicU64::new(0) });
//! })?;
//! counter.value.store(42, Ordering::Release);
//!
//! // Process B: Open existing
//! let counter = Shm::<Counter, Opener>::open(path)?;
//! assert_eq!(counter.value.load(Ordering::Acquire), 42);
//! # Ok::<(), ShmError>(())
//! ```
//!
//! # Crash Recovery
//!
//! Shared memory names persist in `/dev/shm` after crashes. Clean up stale
//! names before creating:
//!
//! ```no_run
//! # use rustix::shm;
//! let _ = shm::unlink("/my-daemon-inbox"); // Ignore ENOENT
//! ```

use rustix::fs::{Mode, fstat, ftruncate};
use rustix::param::page_size;
use rustix::mm::{Advice, MapFlags, ProtFlags, madvise, mmap, munmap};
use rustix::{io, shm};
use std::fmt;
use std::marker::PhantomData;
use std::mem::{MaybeUninit, align_of, size_of};
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
    /// Type alignment exceeds system page size.
    AlignmentExceedsPageSize { type_align: usize, page_size: usize },
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
            Self::AlignmentExceedsPageSize {
                type_align,
                page_size,
            } => write!(
                f,
                "type alignment ({type_align} bytes) exceeds system page size ({page_size} bytes)"
            ),
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

pub const POSIX_NAME_MAX: usize = 255;

/// Validated POSIX shared memory path.
///
/// This type guarantees that the contained path:
/// - Starts with `/`
/// - Contains no other `/` characters
/// - Is shorter than `POSIX_NAME_MAX`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ShmPath(String);

impl ShmPath {
    /// Parses and validates a shared memory path.
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        validate_shm_path(&path)?;
        Ok(Self(path))
    }
}

impl fmt::Display for ShmPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<&str> for ShmPath {
    type Error = ShmError;
    fn try_from(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

impl TryFrom<String> for ShmPath {
    type Error = ShmError;
    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}

impl AsRef<str> for ShmPath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<ShmPath> for String {
    fn from(path: ShmPath) -> Self {
        path.0
    }
}

mod private {
    pub trait Sealed {}
}

/// Strategy trait that maps marker types to cleanup behavior.
///
/// This is an internal trait that defines the cleanup strategy for each
/// marker type. Users should use [`Creator`] or [`Opener`] marker types
/// instead of implementing this trait directly.
///
/// # Sealed Trait
///
/// This trait is **sealed**: it cannot be implemented by types outside of this
/// crate. This guarantees that `Creator` and `Opener` are the only possible
/// modes, preserving the closed-system invariants of the API.
pub trait ShmMode: private::Sealed {
    /// Whether to unlink the shared memory name on drop.
    ///
    /// - `true` for [`Creator`]: Remove name when owner drops
    /// - `false` for [`Opener`]: Leave name for creator to clean up
    const SHOULD_UNLINK: bool;
}

/// Marker type for shared memory owners. Unlinks name on drop.
///
/// Use [`Shm::create()`] to construct `Shm<T, Creator>`. See [`Shm`] for
/// cleanup behavior details.
pub struct Creator;
impl private::Sealed for Creator {}
impl ShmMode for Creator {
    const SHOULD_UNLINK: bool = true;
}

/// Marker type for shared memory clients. Does **not** unlink on drop.
///
/// Use [`Shm::open()`] to construct `Shm<T, Opener>`. See [`Shm`] for
/// cleanup behavior details.
pub struct Opener;
impl private::Sealed for Opener {}
impl ShmMode for Opener {
    const SHOULD_UNLINK: bool = false;
}

/// Marker trait for types safe to share across processes via shared memory.
///
/// # Derive Macro
///
/// Use `#[derive(SharedMemorySafe)]` for custom types:
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
/// The macro verifies `#[repr(C)]`, detects pointer types, and checks field bounds.
///
/// # Provided Implementations
///
/// - Primitives: `i8`–`i128`, `u8`–`u128`, `isize`, `usize`, `f32`, `f64`, `bool`
/// - Atomics: `AtomicBool`, `AtomicI*`, `AtomicU*`
/// - Arrays: `[T; N]` where `T: SharedMemorySafe`
///
/// # Safety
///
/// Implementers **must** guarantee:
///
/// 1. **Layout**: `#[repr(C)]` or `#[repr(transparent)]` (Rust's default layout is unstable)
/// 2. **No pointers**: No `Box`, `Vec`, `String`, `&T`, `*T` (addresses are process-local)
/// 3. **Recursive safety**: All fields implement `SharedMemorySafe`
/// 4. **Drop-safety**: Type is safe if `Drop` never runs (crashes bypass destructors)
/// 5. **Concurrency**: `Send + Sync` (use atomics; `std::sync::Mutex` is process-local)
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

/// Smart pointer to POSIX shared memory with marker-based cleanup.
///
/// Wraps a pointer to shared memory, providing [`Deref`] access and automatic
/// cleanup via [`Drop`]. The `Mode` type parameter ([`Creator`] or [`Opener`])
/// statically selects cleanup behavior at compile time.
///
/// # Type Parameters
///
/// - `T`: The shared type (must be [`SharedMemorySafe`])
/// - `Mode`: Cleanup strategy marker ([`Creator`] unlinks on drop, [`Opener`] does not)
///
/// # Memory Model
///
/// ```text
/// POSIX Shared Memory: "/my-shm"
/// ┌────────────────────────────────┐
/// │  Kernel Memory                 │
/// │  ┌──────────────────────────┐  │
/// │  │  T (size_of::<T>() bytes)│  │
/// │  └──────────────────────────┘  │
/// │         ↑            ↑         │
/// └─────────┼────────────┼─────────┘
///      Process A    Process B
///      (Creator)    (Opener)
/// ```
///
/// Both processes access the **same physical memory** via process-local virtual addresses.
///
/// # Cleanup
///
/// | Mode | On Drop |
/// |------|---------|
/// | [`Creator`] | `munmap()` + `shm_unlink()` |
/// | [`Opener`] | `munmap()` only |
///
/// Kernel frees memory when the name is unlinked AND all mappings are closed.
///
/// # Invariants
///
/// - `ptr` points to `size` bytes from `mmap()`, valid for `'self`
/// - `ptr` is aligned for `T` (guaranteed by page-aligned mmap)
/// - `T: SharedMemorySafe` ensures cross-process layout compatibility
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

impl<T: SharedMemorySafe> Shm<T, Creator> {
    /// Creates new shared memory and initializes it in place.
    ///
    /// Maps shared memory and calls `init` with `&mut MaybeUninit<T>` to initialize.
    ///
    /// # Initialization Contract
    ///
    /// The `init` closure **must** fully initialize `T` before returning. Partial
    /// initialization causes UB on subsequent access. Use [`MaybeUninit::write`] for
    /// simple types, or [`std::ptr::addr_of_mut!`] for large types that would overflow
    /// the stack.
    ///
    /// # Errors
    ///
    /// - `EEXIST`: Path already exists
    /// - `EACCES`: Insufficient permissions
    /// - `ENOMEM`: Out of memory
    /// - [`ShmError::AlignmentExceedsPageSize`]: `align_of::<T>() > page_size()`
    ///
    /// # Panics
    ///
    /// If `init` panics, the shared memory is cleaned up before propagating.
    pub fn create<F>(path: ShmPath, init: F) -> Result<Self>
    where
        F: FnOnce(&mut MaybeUninit<T>),
    {
        // Verify type alignment doesn't exceed page alignment.
        // mmap guarantees page-aligned addresses, so any type with alignment
        // greater than page size would result in misaligned access (UB).
        let ps = page_size();
        if align_of::<T>() > ps {
            return Err(ShmError::AlignmentExceedsPageSize {
                type_align: align_of::<T>(),
                page_size: ps,
            });
        }

        let fd = shm::open(
            path.as_ref(),
            shm::OFlags::CREATE | shm::OFlags::EXCL | shm::OFlags::RDWR,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|err| ShmError::posix("shm_open", path.as_ref(), err))?;

        if let Err(e) = ftruncate(&fd, size_of::<T>() as u64) {
            let _ = shm::unlink(path.as_ref());
            return Err(ShmError::posix("ftruncate", path.as_ref(), e));
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
                let _ = shm::unlink(path.as_ref());
                return Err(ShmError::posix("mmap", path.as_ref(), err));
            }
        };

        // Best-effort huge page hint; ignored if unsupported.
        unsafe {
            let _ = madvise(ptr, size_of::<T>(), Advice::LinuxHugepage);
        }

        // SAFETY: mmap returns a non-null pointer on success (checked above via Ok branch),
        // and the pointer is properly aligned for T (verified above: align_of::<T>() <= page_size)
        let ptr = unsafe { NonNull::new_unchecked(ptr.cast::<T>()) };

        let shm = Self {
            ptr,
            size: size_of::<T>(),
            path: path.into(),
            _mode: PhantomData,
        };

        // SAFETY: We pass &mut MaybeUninit<T> which:
        // - Has the same layout as T (MaybeUninit is repr(transparent))
        // - Correctly models uninitialized memory
        // - Allows the user to safely pass references without UB
        // The user is still responsible for fully initializing before returning.
        let uninit_ref = unsafe { &mut *shm.ptr.as_ptr().cast::<MaybeUninit<T>>() };

        let init_result = catch_unwind(AssertUnwindSafe(|| init(uninit_ref)));

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
    /// Opens a POSIX shared memory object created by another process and maps it
    /// with read-write permissions.
    ///
    /// # Preconditions
    ///
    /// The caller must ensure:
    /// - The creator has **fully initialized** `T` before this process calls `open()`
    /// - External synchronization (file locks, signals, etc.) coordinates access
    ///
    /// No initialization barrier is provided—reading uninitialized memory is UB.
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - Shared memory doesn't exist (`ENOENT`)
    /// - Insufficient permissions (`EACCES`)
    /// - Size mismatch with `size_of::<T>()` ([`ShmError::SizeMismatch`])
    /// - Out of memory (`ENOMEM`)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use titan::ipc::shmem::*;
    /// # use std::sync::atomic::AtomicU64;
    /// # #[repr(C)] struct Counter { value: AtomicU64 }
    /// # unsafe impl SharedMemorySafe for Counter {}
    /// let path = ShmPath::new("/my-counter").unwrap();
    /// let counter = Shm::<Counter, Opener>::open(path)?;
    /// # Ok::<(), ShmError>(())
    /// ```
    pub fn open(path: ShmPath) -> Result<Self> {
        let fd = shm::open(path.as_ref(), shm::OFlags::RDWR, Mode::empty())
            .map_err(|err| ShmError::posix("shm_open", path.as_ref(), err))?;

        let stat = match fstat(&fd) {
            Ok(stat) => stat,
            Err(err) => {
                return Err(ShmError::posix("fstat", path.as_ref(), err));
            }
        };

        let expected_size = size_of::<T>();

        let actual_size = usize::try_from(stat.st_size).map_err(|_| ShmError::PosixError {
            op: "fstat",
            path: path.0.clone(),
            source: io::Errno::INVAL,
        })?;

        if actual_size != expected_size {
            return Err(ShmError::SizeMismatch {
                path: path.0,
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
                return Err(ShmError::posix("mmap", path.as_ref(), err));
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
            path: path.into(),
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

        let path = ShmPath::new("/titan-test-counter")?;

        // Clean up any leftover
        let _ = shm::unlink(path.as_ref());

        let counter = match Shm::<Counter, Creator>::create(path, |uninit| {
            uninit.write(Counter {
                value: AtomicU64::new(0),
            });
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

        let path = ShmPath::new("/titan-test-shared")?;

        let _ = shm::unlink(path.as_ref());

        // Creator process
        {
            let data = match Shm::<SharedData, Creator>::create(path.clone(), |uninit| {
                uninit.write(SharedData {
                    counter: AtomicU64::new(0),
                    flag: AtomicBool::new(false),
                });
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
                let opener_data = Shm::<SharedData, Opener>::open(path.clone())?;
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
        assert!(ShmPath::new("/valid").is_ok());
        assert!(ShmPath::new("/valid-name").is_ok());
        assert!(ShmPath::new("/valid_name_123").is_ok());
    }

    #[test]
    fn test_validate_shm_path_no_leading_slash() {
        let result = ShmPath::new("no-slash");
        assert!(matches!(
            result,
            Err(ShmError::InvalidPath { reason, .. }) if reason == "path must start with '/'"
        ));
    }

    #[test]
    fn test_validate_shm_path_extra_slashes() {
        let result = ShmPath::new("/foo/bar");
        assert!(matches!(
            result,
            Err(ShmError::InvalidPath { reason, .. })
                if reason == "path must not contain additional '/' characters"
        ));

        let result = ShmPath::new("/foo/bar/baz");
        assert!(matches!(
            result,
            Err(ShmError::InvalidPath { reason, .. })
                if reason == "path must not contain additional '/' characters"
        ));
    }

    #[test]
    fn test_validate_shm_path_too_long() {
        let long_path = format!("/{}", "a".repeat(255));
        let result = ShmPath::new(&long_path);
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
        assert!(ShmPath::new(&max_path).is_ok());
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

        let path = ShmPath::new("/titan-test-size-mismatch")?;

        let _ = shm::unlink(path.as_ref());

        // Create with Small type
        let _small = match Shm::<Small, Creator>::create(path.clone(), |uninit| {
            uninit.write(Small {
                value: AtomicU64::new(0),
            });
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
