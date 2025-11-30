//! Lock-free Single Producer Single Consumer (SPSC) queue over POSIX shared memory.
//!
//! This module provides a high-performance, wait-free bounded queue for cross-process
//! communication. The queue uses a ring buffer with atomic indices and leverages POSIX
//! shared memory
//!
//! # Overview
//!
//! - [`Producer`] - Write end of the queue (only one process/thread should hold this)
//! - [`Consumer`] - Read end of the queue (only one process/thread should hold this)
//! - **Lock-free**: No mutexes or syscalls in the hot path
//! - **Wait-free**: Operations complete in bounded time
//!
//! # Basic Usage
//!
//! ```no_run
//! use titan::ipc::spsc::{Producer, Consumer};
//!
//! // Process A: Create producer (daemon/server)
//! let producer = Producer::<u64, 1024, _>::create("/my-queue")
//!     .expect("Failed to create queue");
//!
//! // Process B: Open consumer (client)
//! let consumer = Consumer::<u64, 1024, _>::open("/my-queue")
//!     .expect("Failed to open queue");
//!
//! // Producer pushes
//! producer.push(42).expect("Queue full");
//!
//! // Consumer pops
//! if let Some(value) = consumer.pop() {
//!     assert_eq!(value, 42);
//! }
//! ```
//!
//!
//! # Creator vs Opener Pattern
//!
//! The queue uses typestate to enforce correct cleanup:
//!
//! - **Creator**: Creates new shared memory, unlinks on drop (typically the daemon/server)
//! - **Opener**: Opens existing shared memory, no unlink (typically clients)
//!
//! Either `Producer` or `Consumer` can be the creator:
//!
//! ```no_run
//! # use titan::ipc::spsc::{Producer, Consumer};
//! // Daemon creates an inbox to receive from clients
//! let inbox = Consumer::<u64, 256, _>::create("/daemon-inbox")?;
//!
//! // Clients open the inbox to send to daemon
//! let outbox = Producer::<u64, 256, _>::open("/daemon-inbox")?;
//! # Ok::<(), titan::ipc::shmem::ShmError>(())
//! ```
//!
//! # Memory Layout
//!
//! ```text
//! Queue in Shared Memory (/dev/shm):
//! ┌────────────────────────────────────────┐
//! │ InitMarker (64-byte aligned)           │
//! │  - magic: AtomicU64                    │
//! ├────────────────────────────────────────┤
//! │ ProducerState (64-byte aligned)        │
//! │  - head: AtomicUsize                   │
//! │  - cursor: usize (head % N)            │
//! │  - cached_tail: usize                  │
//! ├────────────────────────────────────────┤
//! │ ConsumerState (64-byte aligned)        │
//! │  - tail: AtomicUsize                   │
//! │  - cursor: usize (tail % N)            │
//! │  - cached_head: usize                  │
//! ├────────────────────────────────────────┤
//! │ Padding (64 bytes)                     │
//! │  - Prevents false sharing              │
//! ├────────────────────────────────────────┤
//! │ Buffer: [Slot<T>; N]                   │
//! │  - Slot 0                              │
//! │  - Slot 1                              │
//! │  - ...                                 │
//! │  - Slot N-1                            │
//! ├────────────────────────────────────────┤
//! │ Padding (64 bytes)                     │
//! │  - Prevents false sharing with         │
//! │    adjacent shared memory regions      │
//! └────────────────────────────────────────┘
//! ```

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use super::shmem::{Creator, Opener, Shm, ShmError, ShmMode};
use crate::SharedMemorySafe;

const INIT_MAGIC: u64 = 0x5350_5343_494E_4954; // "SPSCINIT" in ASCII
const INIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// Marker type: Only producer accesses this cell.
struct ProducerRole;

/// Marker type: Only consumer accesses this cell.
struct ConsumerRole;

/// Marker type: SPSC protocol ensures mutual exclusion for buffer slots.
struct SlotRole;

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

/// Type alias: Cache for producer-side index tracking.
type ProducerCache<T> = SpscCell<T, ProducerRole>;

/// Type alias: Cache for consumer-side index tracking.
type ConsumerCache<T> = SpscCell<T, ConsumerRole>;

/// Type alias: Cell for ring buffer slots.
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
    /// Writes default values for producer and consumer state, then sets the magic
    /// marker with `Release` ordering to signal initialization is complete. The buffer
    /// slots are left uninitialized (`MaybeUninit`) until written by the producer.
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    ///
    /// - **Pointer validity**: `ptr` is non-null, well-aligned for `SpscQueue<T, N>`,
    ///   and points to a dereferenceable region of at least `size_of::<Self>()` bytes
    /// - **Writability**: The memory region is writable
    /// - **Aliasing**: No other references to this memory exist during initialization
    ///   (exclusive access required)
    unsafe fn init_shared(ptr: *mut Self) {
        // SAFETY: Caller guarantees ptr is valid, aligned, writable, and exclusively owned.
        // We use addr_of_mut! to write fields without creating intermediate references,
        // which would be UB for uninitialized memory.
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
/// The producer is responsible for pushing items into the queue. Only one producer
/// should exist per queue - having multiple producers violates the SPSC contract
/// and causes data races.
///
/// # Type Parameters
///
/// - `T`: Element type (must be [`SharedMemorySafe`])
/// - `N`: Queue capacity
/// - `Mode`: Either [`Creator`] or [`Opener`]
///
/// # Examples
///
/// ```no_run
/// # use titan::ipc::spsc::Producer;
/// // Create a new queue with 256 slots
/// let producer = Producer::<u64, 256, _>::create("/my-queue")?;
///
/// // Push items (returns Err(item) if queue is full)
/// match producer.push(42) {
///     Ok(()) => println!("Pushed successfully"),
///     Err(item) => println!("Queue full, item {} not pushed", item),
/// }
/// # Ok::<(), titan::ipc::shmem::ShmError>(())
/// ```
///
/// # Thread Safety
///
/// `Producer` is [`Send`] but **not** [`Sync`]. This enforces the single-producer
/// contract at compile time:
/// - You can move a `Producer` to another thread (ownership transfer)
/// - You cannot share `&Producer` across threads (no concurrent `push()` calls)
///
/// **Cross-process note**: The type system cannot prevent another process from
/// calling [`open()`](Producer::open) on the same path. Users must ensure only
/// one producer exists across all processes.
pub struct Producer<T: SharedMemorySafe, const N: usize, Mode: ShmMode> {
    shm: Shm<SpscQueue<T, N>, Mode>,
    _unsync: PhantomUnsync,
}

impl<T: SharedMemorySafe, const N: usize> Producer<T, N, Creator> {
    /// Creates a new queue and returns the producer end.
    ///
    /// This creates new POSIX shared memory at `path`, initializes the queue structure,
    /// and returns a producer that will unlink the shared memory on drop (Creator mode).
    ///
    /// # Arguments
    ///
    /// * `path` - Shared memory object name (e.g., `"/my-queue"`). Must start with `/`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - Shared memory object already exists at `path`
    /// - Insufficient permissions to create shared memory
    /// - System limits reached (file descriptors, memory)
    ///
    /// # Panics
    ///
    /// Panics if `N` (queue capacity) is 0. The capacity must be at least 1.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::spsc::Producer;
    /// let producer = Producer::<u64, 1024, _>::create("/my-queue")?;
    /// # Ok::<(), titan::ipc::shmem::ShmError>(())
    /// ```
    pub fn create(path: &str) -> Result<Self, ShmError> {
        assert!(N > 0, "Queue capacity must be greater than 0");
        let shm = Shm::<SpscQueue<T, N>, Creator>::create(path, |ptr| unsafe {
            SpscQueue::<T, N>::init_shared(ptr);
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
    /// This opens POSIX shared memory at `path` that was created by another process
    /// and returns a producer that will NOT unlink on drop (Opener mode).
    ///
    /// Waits up to 1 second for the creator to finish initializing the queue.
    ///
    /// # Arguments
    ///
    /// * `path` - Shared memory object name (must match the creator's path).
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - Shared memory object doesn't exist at `path`
    /// - Insufficient permissions to access the object
    /// - Size mismatch (different capacity used by creator)
    /// - Initialization timeout (creator didn't finish within 1 second)
    ///
    /// # Panics
    ///
    /// Panics if `N` (queue capacity) is 0. The capacity must be at least 1.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::spsc::Producer;
    /// // Another process created the queue, we open it
    /// let producer = Producer::<u64, 1024, _>::open("/my-queue")?;
    /// # Ok::<(), titan::ipc::shmem::ShmError>(())
    /// ```
    pub fn open(path: &str) -> Result<Self, ShmError> {
        assert!(N > 0, "Queue capacity must be greater than 0");
        let shm = Shm::<SpscQueue<T, N>, Opener>::open(path)?;
        // SAFETY: Shm::open guarantees the returned pointer is:
        // - Non-null and well-aligned (mmap guarantees page alignment)
        // - Points to mapped memory of exactly size_of::<SpscQueue<T, N>>() bytes
        // - Valid for reads (mapped with PROT_READ | PROT_WRITE)
        // The memory remains mapped for the lifetime of `shm`.
        if !unsafe { SpscQueue::<T, N>::wait_for_init(&raw const *shm, INIT_TIMEOUT) } {
            return Err(ShmError::InitTimeout {
                path: path.to_string(),
            });
        }
        Ok(Self {
            shm,
            _unsync: PhantomData,
        })
    }
}

impl<T: SharedMemorySafe, const N: usize, Mode: ShmMode> Producer<T, N, Mode> {
    /// Attempts to push an item onto the queue.
    ///
    /// This is a wait-free operation that completes in bounded time. If the queue is
    /// full, the item is returned immediately without blocking.
    ///
    /// The producer caches the consumer's tail index to avoid atomic loads on every push.
    /// When the cached value indicates the queue is full, the actual tail is re-loaded
    /// before returning an error.
    ///
    /// # Errors
    ///
    /// Returns `Err(item)` if the queue is full. The item is returned unchanged so it
    /// can be retried or handled by the caller.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::spsc::Producer;
    /// # let producer = Producer::<u64, 4, _>::create("/test")?;
    /// // Successful push
    /// producer.push(42).expect("Queue full");
    ///
    /// // Handle full queue
    /// match producer.push(99) {
    ///     Ok(()) => println!("Pushed"),
    ///     Err(item) => println!("Queue full, retry later with {}", item),
    /// }
    /// # Ok::<(), titan::ipc::shmem::ShmError>(())
    /// ```
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

    /// Blocks until space is available, then pushes the item.
    ///
    /// This spins using `std::hint::spin_loop()` when the queue is full,
    /// which is efficient for short wait times. For longer waits, consider
    /// using external synchronization (e.g., eventfd).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::spsc::Producer;
    /// # let producer = Producer::<u64, 4, _>::create("/test")?;
    /// // Always succeeds (waits if needed)
    /// producer.push_blocking(42);
    /// # Ok::<(), titan::ipc::shmem::ShmError>(())
    /// ```
    #[inline]
    pub fn push_blocking(&self, mut item: T) {
        loop {
            match self.push(item) {
                Ok(()) => return,
                Err(returned) => {
                    item = returned;
                    std::hint::spin_loop();
                }
            }
        }
    }
}

/// Read end of the SPSC queue.
///
/// The consumer is responsible for popping items from the queue. Only one consumer
/// should exist per queue - having multiple consumers violates the SPSC contract
/// and causes data races.
///
/// # Type Parameters
///
/// - `T`: Element type (must be [`SharedMemorySafe`])
/// - `N`: Queue capacity
/// - `Mode`: Either [`Creator`] or [`Opener`]
///
/// # Examples
///
/// ```no_run
/// # use titan::ipc::spsc::Consumer;
/// // Open an existing queue
/// let consumer = Consumer::<u64, 256, _>::open("/my-queue")?;
///
/// // Pop items (returns None if queue is empty)
/// if let Some(value) = consumer.pop() {
///     println!("Received: {}", value);
/// }
/// # Ok::<(), titan::ipc::shmem::ShmError>(())
/// ```
///
/// # Thread Safety
///
/// `Consumer` is [`Send`] but **not** [`Sync`]. This enforces the single-consumer
/// contract at compile time:
/// - You can move a `Consumer` to another thread (ownership transfer)
/// - You cannot share `&Consumer` across threads (no concurrent `pop()` calls)
///
/// **Cross-process note**: The type system cannot prevent another process from
/// calling [`open()`](Consumer::open) on the same path. Users must ensure only
/// one consumer exists across all processes.
pub struct Consumer<T: SharedMemorySafe, const N: usize, Mode: ShmMode> {
    shm: Shm<SpscQueue<T, N>, Mode>,
    _unsync: PhantomUnsync,
}

impl<T: SharedMemorySafe, const N: usize> Consumer<T, N, Creator> {
    /// Creates a new queue and returns the consumer end.
    ///
    /// This creates new POSIX shared memory at `path`, initializes the queue structure,
    /// and returns a consumer that will unlink the shared memory on drop (Creator mode).
    ///
    /// This pattern is useful for daemons/servers that create an "inbox" queue to
    /// receive messages from clients.
    ///
    /// # Arguments
    ///
    /// * `path` - Shared memory object name (e.g., `"/daemon-inbox"`). Must start with `/`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - Shared memory object already exists at `path`
    /// - Insufficient permissions to create shared memory
    /// - System limits reached (file descriptors, memory)
    ///
    /// # Panics
    ///
    /// Panics if `N` (queue capacity) is 0. The capacity must be at least 1.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::spsc::Consumer;
    /// // Daemon creates inbox for clients to send to
    /// let inbox = Consumer::<u64, 1024, _>::create("/daemon-inbox")?;
    /// # Ok::<(), titan::ipc::shmem::ShmError>(())
    /// ```
    pub fn create(path: &str) -> Result<Self, ShmError> {
        assert!(N > 0, "Queue capacity must be greater than 0");
        let shm = Shm::<SpscQueue<T, N>, Creator>::create(path, |ptr| unsafe {
            SpscQueue::<T, N>::init_shared(ptr);
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
    /// This opens POSIX shared memory at `path` that was created by another process
    /// and returns a consumer that will NOT unlink on drop (Opener mode).
    ///
    /// Waits up to 1 second for the creator to finish initializing the queue.
    ///
    /// # Arguments
    ///
    /// * `path` - Shared memory object name (must match the creator's path).
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - Shared memory object doesn't exist at `path`
    /// - Insufficient permissions to access the object
    /// - Size mismatch (different capacity used by creator)
    /// - Initialization timeout (creator didn't finish within 1 second)
    ///
    /// # Panics
    ///
    /// Panics if `N` (queue capacity) is 0. The capacity must be at least 1.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::spsc::Consumer;
    /// // Another process created the queue, we open it
    /// let consumer = Consumer::<u64, 1024, _>::open("/my-queue")?;
    /// # Ok::<(), titan::ipc::shmem::ShmError>(())
    /// ```
    pub fn open(path: &str) -> Result<Self, ShmError> {
        assert!(N > 0, "Queue capacity must be greater than 0");
        let shm = Shm::<SpscQueue<T, N>, Opener>::open(path)?;
        // SAFETY: Shm::open guarantees the returned pointer is:
        // - Non-null and well-aligned (mmap guarantees page alignment)
        // - Points to mapped memory of exactly size_of::<SpscQueue<T, N>>() bytes
        // - Valid for reads (mapped with PROT_READ | PROT_WRITE)
        // The memory remains mapped for the lifetime of `shm`.
        if !unsafe { SpscQueue::<T, N>::wait_for_init(&raw const *shm, INIT_TIMEOUT) } {
            return Err(ShmError::InitTimeout {
                path: path.to_string(),
            });
        }
        Ok(Self {
            shm,
            _unsync: PhantomData,
        })
    }
}

impl<T: SharedMemorySafe, const N: usize, Mode: ShmMode> Consumer<T, N, Mode> {
    /// Attempts to pop an item from the queue.
    ///
    /// This is a wait-free operation that completes in bounded time. If the queue is
    /// empty, `None` is returned immediately without blocking.
    ///
    /// # Returns
    ///
    /// - `Some(item)` - Item was successfully popped
    /// - `None` - Queue is empty
    ///
    /// The consumer caches the producer's head index to avoid atomic loads on every pop.
    /// When the cached value indicates the queue is empty, the actual head is re-loaded
    /// before returning `None`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::spsc::Consumer;
    /// # let consumer = Consumer::<u64, 4, _>::create("/test")?;
    /// // Busy-wait pattern (spin until data available)
    /// loop {
    ///     if let Some(value) = consumer.pop() {
    ///         println!("Received: {}", value);
    ///         break;
    ///     }
    ///     std::hint::spin_loop();
    /// }
    ///
    /// // Or handle empty queue gracefully
    /// match consumer.pop() {
    ///     Some(value) => println!("Got {}", value),
    ///     None => println!("Queue empty, try again later"),
    /// }
    /// # Ok::<(), titan::ipc::shmem::ShmError>(())
    /// ```
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

    /// Blocks until an item is available, then pops and returns it.
    ///
    /// This spins using `std::hint::spin_loop()` when the queue is empty,
    /// which is efficient for short wait times. For longer waits, consider
    /// using external synchronization (e.g., eventfd).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use titan::ipc::spsc::Consumer;
    /// # let consumer = Consumer::<u64, 4, _>::create("/test")?;
    /// // Always succeeds (waits if needed)
    /// let value = consumer.pop_blocking();
    /// println!("Received: {}", value);
    /// # Ok::<(), titan::ipc::shmem::ShmError>(())
    /// ```
    #[inline]
    #[must_use]
    pub fn pop_blocking(&self) -> T {
        loop {
            if let Some(item) = self.pop() {
                return item;
            }
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::create("/test-basic-push-pop"));
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::open("/test-basic-push-pop"));

        // Push a single item
        assert!(producer.push(42).is_ok());

        // Pop it back
        assert_eq!(consumer.pop(), Some(42));

        // Queue should now be empty
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_multiple_items() {
        let producer = unwrap_or_skip!(Producer::<u64, 16, _>::create("/test-multiple-items"));
        let consumer = unwrap_or_skip!(Consumer::<u64, 16, _>::open("/test-multiple-items"));

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
        let producer = unwrap_or_skip!(Producer::<u64, 4, _>::create("/test-queue-full"));
        let consumer = unwrap_or_skip!(Consumer::<u64, 4, _>::open("/test-queue-full"));

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
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::create("/test-queue-empty"));
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::open("/test-queue-empty"));

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
        let producer = unwrap_or_skip!(Producer::<u64, 4, _>::create("/test-wrapping"));
        let consumer = unwrap_or_skip!(Consumer::<u64, 4, _>::open("/test-wrapping"));

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
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::create("/test-interleaved"));
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::open("/test-interleaved"));

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
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::create("/test-daemon-inbox"));

        // Client opens the inbox to send to daemon
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::open("/test-daemon-inbox"));

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
        let producer = unwrap_or_skip!(Producer::<u64, 8, _>::create("/test-client-response"));
        let consumer = unwrap_or_skip!(Consumer::<u64, 8, _>::open("/test-client-response"));

        producer.push(42).unwrap();
        assert_eq!(consumer.pop(), Some(42));
    }
}
