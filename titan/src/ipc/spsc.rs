//! Lock-free SPSC queue over POSIX shared memory.
//!
//! A wait-free bounded queue for cross-process communication using a ring buffer
//! with atomic indices.
//!
//! # Overview
//!
//! - [`Producer`] - Write end (single producer per queue)
//! - [`Consumer`] - Read end (single consumer per queue)
//! - Lock-free, wait-free: no mutexes or syscalls in the hot path
//!
//! # Example
//!
//! ```no_run
//! use titan::ipc::spsc::{Producer, Consumer};
//! use titan::ipc::shmem::ShmPath;
//!
//! let path = ShmPath::new("/my-queue").unwrap();
//!
//! // Process A: Create producer
//! let producer = Producer::<u64, 1024, _>::create(path.clone())?;
//! producer.push(42).expect("Queue full");
//!
//! // Process B: Open consumer
//! let consumer = Consumer::<u64, 1024, _>::open(path)?;
//! assert_eq!(consumer.pop(), Some(42));
//! # Ok::<(), titan::ipc::shmem::ShmError>(())
//! ```
//!
//! Either endpoint can be the [`Creator`] (unlinks on drop) or [`Opener`] (no unlink).
//! See [`shmem`](super::shmem) for cleanup semantics.
//!
//! # Memory Layout
//!
//! ```text
//! ┌────────────────────────────────────────┐
//! │ InitMarker      (64-byte aligned)      │
//! ├────────────────────────────────────────┤
//! │ ProducerState   (head, cached_tail)    │
//! ├────────────────────────────────────────┤
//! │ ConsumerState   (tail, cached_head)    │
//! ├────────────────────────────────────────┤
//! │ Padding         (false sharing guard)  │
//! ├────────────────────────────────────────┤
//! │ Buffer: [Slot<T>; N]                   │
//! ├────────────────────────────────────────┤
//! │ Padding         (false sharing guard)  │
//! └────────────────────────────────────────┘
//! ```

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use minstant::Instant;

use super::shmem::{Creator, Opener, Shm, ShmError, ShmMode, ShmPath};
use crate::SharedMemorySafe;

/// Timeout specification for blocking operations.
#[derive(Debug, Clone, Copy)]
pub enum Timeout {
    /// Wait indefinitely.
    Infinite,
    /// Wait for at most the specified duration.
    Duration(Duration),
}

impl From<Duration> for Timeout {
    fn from(d: Duration) -> Self {
        Self::Duration(d)
    }
}

const INIT_MAGIC: u64 = 0x5350_5343_494E_4954; // "SPSCINIT" in ASCII
const INIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

// =============================================================================
// Role Marker Types
// =============================================================================
//
// These zero-sized marker types provide **nominal type safety** for internal
// fields. While `unsafe` code could bypass them, they serve two purposes:
//
// 1. **Specification**: The type `ProducerCache<usize>` vs `ConsumerCache<usize>`
//    documents ownership intent in the type system (Level 1), which is superior
//    to documentation comments (Level 0) per Cardelli's type theory.
//
// 2. **Logic Error Prevention**: Accidentally passing a consumer's cache to a
//    function expecting a producer's cache becomes a compile-time type error,
//    even though both wrap the same underlying type.

/// Role marker: Fields with this role are owned exclusively by the producer.
///
/// The producer is the only entity that reads/writes these fields.
/// Using a distinct type prevents accidentally mixing producer and consumer caches.
struct ProducerRole;

/// Role marker: Fields with this role are owned exclusively by the consumer.
///
/// The consumer is the only entity that reads/writes these fields.
/// Using a distinct type prevents accidentally mixing producer and consumer caches.
struct ConsumerRole;

/// Role marker: Buffer slots whose ownership transfers via the SPSC protocol.
///
/// Ownership of each slot alternates between producer and consumer based on
/// the head/tail indices. The SPSC invariant guarantees mutual exclusion.
struct SlotRole;

/// Interior-mutable cell with a role marker for nominal type safety.
///
/// `SpscCell<T, Role>` wraps an `UnsafeCell<T>` with a phantom `Role` parameter.
/// The `Role` doesn't affect runtime behavior—it exists purely to make different
/// logical "kinds" of cells into distinct types at compile time.
///
/// # Why Role Markers?
///
/// Consider two fields: `cursor: usize` in `ProducerState` and `cursor: usize`
/// in `ConsumerState`. Without role markers, both would have type `UnsafeCell<usize>`.
/// With role markers:
///
/// - Producer's cursor: `SpscCell<usize, ProducerRole>`
/// - Consumer's cursor: `SpscCell<usize, ConsumerRole>`
///
/// These are now **nominally distinct types**. A function that expects
/// `&SpscCell<usize, ProducerRole>` won't accept `&SpscCell<usize, ConsumerRole>`,
/// catching logic errors at compile time.
#[repr(transparent)]
struct SpscCell<T, Role>(UnsafeCell<T>, PhantomData<Role>);

impl<T, Role> SpscCell<T, Role> {
    const fn new(value: T) -> Self {
        Self(UnsafeCell::new(value), PhantomData)
    }

    const fn get(&self) -> &UnsafeCell<T> {
        &self.0
    }
}

// SAFETY: SpscCell is Sync because the SPSC algorithm guarantees that each slot
// is either being written (by producer) or read (by consumer), never both.
// The atomic head/tail indices with Release/Acquire ordering provide the
// synchronization barrier between writes and reads.
unsafe impl<T: Send, Role> Sync for SpscCell<T, Role> {}
unsafe impl<T: Send, Role> Send for SpscCell<T, Role> {}

// SAFETY: SpscCell can be placed in shared memory because:
// - It's repr(transparent) around UnsafeCell<T> where T: SharedMemorySafe
// - The Role phantom type doesn't affect memory layout
// - Access safety is enforced by the SPSC protocol, not by the type system
unsafe impl<T: SharedMemorySafe, Role> SharedMemorySafe for SpscCell<T, Role> {}

/// Cache cell owned exclusively by the producer.
///
/// Used for producer-local state like the cursor position and cached tail index.
type ProducerCache<T> = SpscCell<T, ProducerRole>;

/// Cache cell owned exclusively by the consumer.
///
/// Used for consumer-local state like the cursor position and cached head index.
type ConsumerCache<T> = SpscCell<T, ConsumerRole>;

/// Buffer slot cell with ownership governed by the SPSC protocol.
///
/// The producer owns a slot while writing; the consumer owns it while reading.
/// Ownership transfers atomically via head/tail index updates.
type SlotCell<T> = SpscCell<T, SlotRole>;

#[derive(SharedMemorySafe)]
#[repr(C)]
#[repr(align(64))]
struct ProducerState {
    /// Write index (next slot to write to).
    /// Owned by producer, read by consumer.
    head: AtomicUsize,

    /// Producer-local cursor tracking `head % N`.
    /// Invariant: always in range `[0, N)`.
    /// Avoids modulo division on each push by incrementing with wrap.
    cursor: ProducerCache<usize>,

    /// Cached copy of tail index.
    cached_tail: ProducerCache<usize>,
}

impl Default for ProducerState {
    fn default() -> Self {
        Self {
            head: AtomicUsize::new(0),
            cursor: ProducerCache::new(0),
            cached_tail: ProducerCache::new(0),
        }
    }
}

#[derive(SharedMemorySafe)]
#[repr(C)]
#[repr(align(64))]
struct ConsumerState {
    /// Read index (next slot to read from).
    /// Owned by consumer, read by producer.
    tail: AtomicUsize,

    /// Consumer-local cursor tracking `tail % N`.
    /// Invariant: always in range `[0, N)`.
    /// Avoids modulo division on each pop by incrementing with wrap.
    cursor: ConsumerCache<usize>,

    /// Cached copy of head index.
    cached_head: ConsumerCache<usize>,
}

impl Default for ConsumerState {
    fn default() -> Self {
        Self {
            tail: AtomicUsize::new(0),
            cursor: ConsumerCache::new(0),
            cached_head: ConsumerCache::new(0),
        }
    }
}

#[derive(SharedMemorySafe)]
#[repr(C)]
#[repr(align(64))]
struct InitMarker(AtomicU64);

#[derive(SharedMemorySafe)]
#[repr(C)]
struct Slot<T: SharedMemorySafe> {
    value: SlotCell<MaybeUninit<T>>,
}

#[derive(SharedMemorySafe)]
#[repr(C)]
struct SpscQueue<T: SharedMemorySafe, const N: usize> {
    /// Magic value to indicate initialization is complete.
    init: InitMarker,

    /// Producer state (head index + cached tail).
    producer: ProducerState,

    /// Consumer state (tail index + cached head).
    consumer: ConsumerState,

    /// Prevent false sharing between consumer state and buffer.
    _padding_0: [u8; 64],

    /// Ring buffer slots.
    buffer: [Slot<T>; N],

    /// Prevent false sharing with adjacent shared memory regions.
    _padding_1: [u8; 64],
}

impl<T: SharedMemorySafe, const N: usize> SpscQueue<T, N> {
    /// Advances a cursor to the next slot index, wrapping to 0 at capacity.
    ///
    /// This is equivalent to `(cursor + 1) % N` but avoids the division instruction,
    /// using a comparison and conditional move instead.
    ///
    /// # Invariant
    ///
    /// If `cursor < N`, then the result is also `< N`.
    #[inline]
    const fn bump_cursor(cursor: usize) -> usize {
        let next = cursor + 1;
        if next == N { 0 } else { next }
    }

    /// Initializes the queue directly inside shared memory.
    ///
    /// The signature `&mut MaybeUninit<Self>` explicitly models that the memory is
    /// allocated but uninitialized—this is semantically honest per Cardelli's
    /// "types as specifications" principle.
    ///
    /// Writes default values for producer and consumer state, then sets the magic
    /// marker with `Release` ordering to signal initialization is complete. The buffer
    /// slots are left uninitialized (`MaybeUninit`) until written by the producer.
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    ///
    /// - **Exclusive access**: No other references to this memory exist during
    ///   initialization
    /// - **Full initialization**: After this function returns, the caller must
    ///   treat the memory as initialized (the function writes all required fields)
    fn init_shared(uninit: &mut MaybeUninit<Self>) {
        // SAFETY: MaybeUninit<T> has the same layout as T, so as_mut_ptr() gives
        // a valid pointer to write fields. We use addr_of_mut! to write fields
        // without creating intermediate references, which would be UB for
        // uninitialized memory.
        let ptr = uninit.as_mut_ptr();
        unsafe {
            // Initialize init field
            std::ptr::addr_of_mut!((*ptr).init).write(InitMarker(AtomicU64::new(0)));
            // Initialize producer and consumer state
            std::ptr::addr_of_mut!((*ptr).producer).write(ProducerState::default());
            std::ptr::addr_of_mut!((*ptr).consumer).write(ConsumerState::default());

            // Release store to signal initialization complete.
            (*ptr)
                .init
                .0
                .store(INIT_MAGIC, std::sync::atomic::Ordering::Release);
        }
    }

    /// Spins until the queue is initialized or timeout expires.
    ///
    /// Polls the magic marker with `Acquire` ordering until it matches [`INIT_MAGIC`],
    /// establishing a synchronizes-with relationship with the `Release` store in
    /// [`init_shared()`](Self::init_shared). This guarantees all initialization writes
    /// are visible before this function returns `true`.
    ///
    /// Returns `true` if initialized, `false` if timed out.
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    ///
    /// - **Pointer validity**: `ptr` is non-null, well-aligned for `SpscQueue<T, N>`,
    ///   and points to mapped shared memory of at least `size_of::<Self>()` bytes
    /// - **Lifetime**: The memory remains mapped for the duration of this call
    unsafe fn wait_for_init(ptr: *const Self, timeout: std::time::Duration) -> bool {
        use std::sync::atomic::Ordering;
        let start = std::time::Instant::now();
        loop {
            // SAFETY: Caller guarantees ptr is valid and points to mapped shared memory.
            // Reading an AtomicU64 is always safe regardless of initialization state
            // (no invalid bit patterns for integers).
            if unsafe { (*ptr).init.0.load(Ordering::Acquire) } == INIT_MAGIC {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }
            std::hint::spin_loop();
        }
    }
}

// SAFETY: SpscQueue is Send because all fields are Send (AtomicUsize, SpscCell).
unsafe impl<T: SharedMemorySafe, const N: usize> Send for SpscQueue<T, N> {}

// SAFETY: SpscQueue is Sync because concurrent access is mediated by atomics:
// - head/tail are AtomicUsize with Release/Acquire ordering
// - Buffer slots are protected by the SPSC invariant (see SpscCell)
unsafe impl<T: SharedMemorySafe, const N: usize> Sync for SpscQueue<T, N> {}

/// Marker type to opt-out of `Sync` while remaining `Send`.
type PhantomUnsync = PhantomData<Cell<&'static ()>>;

/// Write end of the SPSC queue.
///
/// Only one producer should exist per queue—multiple producers cause data races.
///
/// # Thread Safety
///
/// `Producer` is [`Send`] but **not** [`Sync`]:
/// - Can transfer ownership to another thread
/// - Cannot share `&Producer` (no concurrent `push()`)
///
/// **Cross-process**: The type system cannot prevent multiple processes from
/// calling `open()`. Callers must ensure exactly one producer exists.
pub struct Producer<T: SharedMemorySafe, const N: usize, Mode: ShmMode> {
    shm: Shm<SpscQueue<T, N>, Mode>,
    _unsync: PhantomUnsync,
}

struct CapacityCheck<const N: usize>;

impl<const N: usize> CapacityCheck<N> {
    /// Compile-time assertion that queue capacity is non-zero.
    const OK: () = assert!(N > 0, "Queue capacity must be greater than 0");
}

impl<T: SharedMemorySafe, const N: usize> Producer<T, N, Creator> {
    /// Creates a new queue and returns the producer end.
    ///
    /// Unlinks shared memory on drop. Fails to compile if `N == 0`.
    ///
    /// # Errors
    ///
    /// `EEXIST` (path exists), `EACCES` (permissions), `ENOMEM` (resources).
    pub fn create(path: ShmPath) -> Result<Self, ShmError> {
        let () = CapacityCheck::<N>::OK;

        let shm = Shm::<SpscQueue<T, N>, Creator>::create(path, |uninit| {
            SpscQueue::<T, N>::init_shared(uninit);
        })?;
        Ok(Self {
            shm,
            _unsync: PhantomData,
        })
    }
}

impl<T: SharedMemorySafe, const N: usize> Producer<T, N, Opener> {
    /// Opens an existing queue and returns the producer end.
    ///
    /// Does not unlink on drop. Waits up to 1s for initialization.
    /// Fails to compile if `N == 0`.
    ///
    /// # Errors
    ///
    /// `ENOENT` (doesn't exist), `EACCES` (permissions), size mismatch, init timeout.
    pub fn open(path: ShmPath) -> Result<Self, ShmError> {
        let () = CapacityCheck::<N>::OK;

        let shm = Shm::<SpscQueue<T, N>, Opener>::open(path.clone())?;
        // SAFETY: Shm::open guarantees the returned pointer is:
        // - Non-null and well-aligned (mmap guarantees page alignment)
        // - Points to mapped memory of exactly size_of::<SpscQueue<T, N>>() bytes
        // - Valid for reads (mapped with PROT_READ | PROT_WRITE)
        // The memory remains mapped for the lifetime of `shm`.
        if !unsafe { SpscQueue::<T, N>::wait_for_init(&raw const *shm, INIT_TIMEOUT) } {
            return Err(ShmError::InitTimeout { path: path.into() });
        }
        Ok(Self {
            shm,
            _unsync: PhantomData,
        })
    }
}

impl<T: SharedMemorySafe, const N: usize, Mode: ShmMode> Producer<T, N, Mode> {
    /// Attempts to push an item onto the queue (wait-free).
    ///
    /// # Errors
    ///
    /// Returns `Err(item)` if the queue is full, allowing retry.
    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        use std::sync::atomic::Ordering;

        let queue = &*self.shm;

        // Load current head (producer-local, relaxed is fine)
        let head = queue.producer.head.load(Ordering::Relaxed);

        // Load cached tail (local cache, doesn't need atomic ordering)
        // SAFETY: Producer has exclusive access to cached_tail (ProducerRole ensures this)
        let mut cached_tail = unsafe { *queue.producer.cached_tail.get().get() };

        // Check if queue appears full using cached value
        if head.wrapping_sub(cached_tail) >= N {
            // Refresh cache from actual tail (acquire to sync with consumer)
            cached_tail = queue.consumer.tail.load(Ordering::Acquire);
            // SAFETY: Producer has exclusive write access to its cached_tail field
            unsafe {
                *queue.producer.cached_tail.get().get() = cached_tail;
            }

            // Check again with fresh value
            if head.wrapping_sub(cached_tail) >= N {
                return Err(item); // Queue is full
            }
        }

        // SAFETY: Producer has exclusive access to its cursor field (ProducerRole marker
        // ensures this at the type level). The cursor is always in range [0, N) because:
        // - Initialized to 0
        // - Only modified by bump_cursor which preserves the invariant
        let slot_index = unsafe { *queue.producer.cursor.get().get() };

        // SAFETY: The producer owns the slot at `slot_index` because:
        // - head hasn't been published yet (store happens after this write)
        // - The check above ensures head - tail < N, so the consumer isn't reading this slot
        // - slot_index is in [0, N) per the cursor invariant, so the indexing is in bounds
        unsafe {
            let slot_ptr = queue.buffer[slot_index].value.get().get();
            std::ptr::write(slot_ptr, MaybeUninit::new(item));
        }

        // SAFETY: Producer has exclusive write access to its cursor field.
        // bump_cursor preserves the [0, N) invariant.
        unsafe {
            *queue.producer.cursor.get().get() = SpscQueue::<T, N>::bump_cursor(slot_index);
        }

        // Publish the new head (release to sync with consumer)
        queue
            .producer
            .head
            .store(head.wrapping_add(1), Ordering::Release);

        Ok(())
    }

    /// Spins until space is available, then pushes.
    ///
    /// # Errors
    ///
    /// Returns `Err(item)` on timeout.
    #[inline]
    pub fn push_blocking(&self, mut item: T, timeout: Timeout) -> Result<(), T> {
        let deadline = match timeout {
            Timeout::Infinite => None,
            Timeout::Duration(d) => Some(Instant::now() + d),
        };
        loop {
            match self.push(item) {
                Ok(()) => return Ok(()),
                Err(returned) => {
                    item = returned;
                    if let Some(dl) = deadline
                        && Instant::now() > dl
                    {
                        return Err(item);
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }
}

/// Read end of the SPSC queue.
///
/// Only one consumer should exist per queue—multiple consumers cause data races.
/// See [`Producer`] for thread safety details (same semantics apply).
pub struct Consumer<T: SharedMemorySafe, const N: usize, Mode: ShmMode> {
    shm: Shm<SpscQueue<T, N>, Mode>,
    _unsync: PhantomUnsync,
}

impl<T: SharedMemorySafe, const N: usize> Consumer<T, N, Creator> {
    /// Creates a new queue and returns the consumer end.
    ///
    /// Useful for daemons creating an "inbox". See [`Producer::create`] for errors.
    ///
    /// # Errors
    ///
    /// See [`Producer::create`].
    pub fn create(path: ShmPath) -> Result<Self, ShmError> {
        let () = CapacityCheck::<N>::OK;
        let shm = Shm::<SpscQueue<T, N>, Creator>::create(path, |uninit| {
            SpscQueue::<T, N>::init_shared(uninit);
        })?;
        Ok(Self {
            shm,
            _unsync: PhantomData,
        })
    }
}

impl<T: SharedMemorySafe, const N: usize> Consumer<T, N, Opener> {
    /// Opens an existing queue and returns the consumer end.
    ///
    /// # Errors
    ///
    /// See [`Producer::open`].
    pub fn open(path: ShmPath) -> Result<Self, ShmError> {
        let () = CapacityCheck::<N>::OK;
        let shm = Shm::<SpscQueue<T, N>, Opener>::open(path.clone())?;
        // SAFETY: Shm::open guarantees the returned pointer is:
        // - Non-null and well-aligned (mmap guarantees page alignment)
        // - Points to mapped memory of exactly size_of::<SpscQueue<T, N>>() bytes
        // - Valid for reads (mapped with PROT_READ | PROT_WRITE)
        // The memory remains mapped for the lifetime of `shm`.
        if !unsafe { SpscQueue::<T, N>::wait_for_init(&raw const *shm, INIT_TIMEOUT) } {
            return Err(ShmError::InitTimeout { path: path.into() });
        }
        Ok(Self {
            shm,
            _unsync: PhantomData,
        })
    }
}

impl<T: SharedMemorySafe, const N: usize, Mode: ShmMode> Consumer<T, N, Mode> {
    /// Attempts to pop an item from the queue (wait-free).
    ///
    /// Returns `None` if the queue is empty.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        use std::sync::atomic::Ordering;

        let queue = &*self.shm;

        // Load current tail (consumer-local, relaxed is fine)
        let tail = queue.consumer.tail.load(Ordering::Relaxed);

        // Load cached head (local cache, doesn't need atomic ordering)
        // SAFETY: Consumer has exclusive access to cached_head (ConsumerRole ensures this)
        let mut cached_head = unsafe { *queue.consumer.cached_head.get().get() };

        // Check if queue appears empty using cached value
        if cached_head == tail {
            // Refresh cache from actual head (acquire to sync with producer)
            cached_head = queue.producer.head.load(Ordering::Acquire);
            // SAFETY: Consumer has exclusive write access to its cached_head field
            unsafe {
                *queue.consumer.cached_head.get().get() = cached_head;
            }

            // Check again with fresh value
            if cached_head == tail {
                return None; // Queue is empty
            }
        }

        // SAFETY: Consumer has exclusive access to its cursor field (ConsumerRole marker
        // ensures this at the type level). The cursor is always in range [0, N) because:
        // - Initialized to 0
        // - Only modified by bump_cursor which preserves the invariant
        let slot_index = unsafe { *queue.consumer.cursor.get().get() };

        // SAFETY: The consumer owns the slot at `slot_index` because:
        // - The check above ensures head != tail, so there's data to read
        // - tail hasn't been published yet (store happens after this read)
        // - slot_index is in [0, N) per the cursor invariant, so the indexing is in bounds
        // - The producer won't overwrite this slot until we publish the new tail
        // - The slot was initialized by the producer (MaybeUninit::assume_init is valid)
        let item = unsafe {
            let slot_ptr = queue.buffer[slot_index].value.get().get();
            std::ptr::read(slot_ptr).assume_init()
        };

        // Publish the new tail (release to sync with producer)
        queue
            .consumer
            .tail
            .store(tail.wrapping_add(1), Ordering::Release);

        // SAFETY: Consumer has exclusive write access to its cursor field.
        // bump_cursor preserves the [0, N) invariant.
        unsafe {
            *queue.consumer.cursor.get().get() = SpscQueue::<T, N>::bump_cursor(slot_index);
        }

        Some(item)
    }

    /// Spins until an item is available, then pops.
    ///
    /// Returns `None` on timeout.
    #[inline]
    #[must_use]
    pub fn pop_blocking(&self, timeout: Timeout) -> Option<T> {
        let deadline = match timeout {
            Timeout::Infinite => None,
            Timeout::Duration(d) => Some(Instant::now() + d),
        };
        loop {
            if let Some(item) = self.pop() {
                return Some(item);
            }
            if let Some(dl) = deadline
                && Instant::now() > dl
            {
                return None;
            }
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::shmem::ShmPath;
    use rustix::io;

    macro_rules! unwrap_or_skip {
        ($expr:expr) => {
            match $expr {
                Ok(value) => value,
                Err(ShmError::PosixError { source, .. }) if source == io::Errno::ACCESS => {
                    eprintln!("Skipping test due to shared memory permission denial");
                    return;
                }
                Err(err) => panic!("Unexpected shared memory error: {err}"),
            }
        };
    }

    const CACHE_LINE_SIZE: usize = 64;

    #[test]
    fn test_cache_line_alignment() {
        // Verify our structs are properly aligned
        assert_eq!(std::mem::align_of::<ProducerState>(), CACHE_LINE_SIZE);
        assert_eq!(std::mem::align_of::<ConsumerState>(), CACHE_LINE_SIZE);

        // Verify they're on separate cache lines in SpscQueue
        assert!(std::mem::size_of::<ProducerState>() <= CACHE_LINE_SIZE);
        assert!(std::mem::size_of::<ConsumerState>() <= CACHE_LINE_SIZE);
    }

    #[test]
    fn test_buffer_starts_on_separate_cache_line() {
        use std::mem::{offset_of, size_of};

        type TestQueue = SpscQueue<u64, 16>;

        // Verify InitMarker, ProducerState and ConsumerState each take exactly one cache line
        assert_eq!(size_of::<InitMarker>(), CACHE_LINE_SIZE);
        assert_eq!(size_of::<ProducerState>(), CACHE_LINE_SIZE);
        assert_eq!(size_of::<ConsumerState>(), CACHE_LINE_SIZE);

        // Verify padding before buffer is one cache line
        assert_eq!(size_of::<[u8; 64]>(), CACHE_LINE_SIZE);

        // Verify buffer starts at offset 256 (5th cache line)
        // Layout: InitMarker(64) + ProducerState(64) + ConsumerState(64) + Padding(64) + Buffer
        // This ensures no false sharing between consumer and buffer
        assert_eq!(offset_of!(TestQueue, buffer), 4 * CACHE_LINE_SIZE);
    }

    #[test]
    fn test_basic_push_pop() {
        let path = ShmPath::new("/test-basic-push-pop").unwrap();
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::create(path.clone()));
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::open(path));

        // Push a single item
        assert!(producer.push(42).is_ok());

        // Pop it back
        assert_eq!(consumer.pop(), Some(42));

        // Queue should now be empty
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_multiple_items() {
        let path = ShmPath::new("/test-multiple-items").unwrap();
        let producer = unwrap_or_skip!(Producer::<u64, 16, _>::create(path.clone()));
        let consumer = unwrap_or_skip!(Consumer::<u64, 16, _>::open(path));

        // Push multiple items
        for i in 0..10 {
            assert!(producer.push(i).is_ok());
        }

        // Pop them back in order (FIFO)
        for i in 0..10 {
            assert_eq!(consumer.pop(), Some(i));
        }

        // Queue should now be empty
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_queue_full() {
        let path = ShmPath::new("/test-queue-full").unwrap();
        let producer = unwrap_or_skip!(Producer::<u64, 4, _>::create(path.clone()));
        let consumer = unwrap_or_skip!(Consumer::<u64, 4, _>::open(path));

        // Fill the queue (capacity is 4)
        for i in 0..4 {
            assert!(producer.push(i).is_ok(), "Failed to push item {i}");
        }

        // Next push should fail (queue full)
        assert_eq!(producer.push(999), Err(999));

        // Pop one item
        assert_eq!(consumer.pop(), Some(0));

        // Now we should be able to push again
        assert!(producer.push(4).is_ok());

        // Queue is full again, next push should fail
        assert_eq!(producer.push(1000), Err(1000));
    }

    #[test]
    fn test_queue_empty() {
        let path = ShmPath::new("/test-queue-empty").unwrap();
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::create(path.clone()));
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::open(path));

        // Queue starts empty
        assert_eq!(consumer.pop(), None);

        // Push and pop an item
        producer.push(42).unwrap();
        assert_eq!(consumer.pop(), Some(42));

        // Back to empty
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_wrapping_behavior() {
        let path = ShmPath::new("/test-wrapping").unwrap();
        let producer = unwrap_or_skip!(Producer::<u64, 4, _>::create(path.clone()));
        let consumer = unwrap_or_skip!(Consumer::<u64, 4, _>::open(path));

        // Fill, drain, refill multiple times to test index wrapping
        for round in 0..5 {
            // Fill the queue
            for i in 0..4 {
                let value = round * 10 + i;
                assert!(producer.push(value).is_ok());
            }

            // Drain the queue
            for i in 0..4 {
                let expected = round * 10 + i;
                assert_eq!(consumer.pop(), Some(expected));
            }

            // Should be empty again
            assert_eq!(consumer.pop(), None);
        }
    }

    #[test]
    fn test_interleaved_operations() {
        let path = ShmPath::new("/test-interleaved").unwrap();
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::create(path.clone()));
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::open(path));

        // Interleave push and pop operations
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        producer.push(3).unwrap();
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        producer.push(4).unwrap();
        producer.push(5).unwrap();
        assert_eq!(consumer.pop(), Some(4));
        assert_eq!(consumer.pop(), Some(5));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_consumer_creates_producer_opens() {
        // Daemon creates an inbox to receive from clients
        let path = ShmPath::new("/test-daemon-inbox").unwrap();
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::create(path.clone()));

        // Client opens the inbox to send to daemon
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::open(path));

        // Client sends messages to daemon
        producer.push(100).unwrap();
        producer.push(200).unwrap();

        // Daemon receives messages
        assert_eq!(consumer.pop(), Some(100));
        assert_eq!(consumer.pop(), Some(200));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_producer_creates_consumer_opens() {
        // Typical case: Producer creates, Consumer opens
        let path = ShmPath::new("/test-client-response").unwrap();
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::create(path.clone()));
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::open(path));

        producer.push(42).unwrap();
        assert_eq!(consumer.pop(), Some(42));
    }
}
