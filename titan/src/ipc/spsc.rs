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

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use minstant::Instant;

use super::shmem::{Creator, Opener, Shm, ShmError, ShmMode, ShmPath};
use crate::SharedMemorySafe;
use crate::spsc::ring::{ConsumerState, ProducerState, Ring, Slot, SpscCell};

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

// SharedMemorySafe implementations for ring types
// SAFETY: SpscCell can be placed in shared memory because:
// - It's repr(transparent) around UnsafeCell<T> where T: SharedMemorySafe
// - The Role phantom type doesn't affect memory layout
// - Access safety is enforced by the SPSC protocol, not by the type system
unsafe impl<T: SharedMemorySafe, Role> SharedMemorySafe for SpscCell<T, Role> {}

// SAFETY: ProducerState is SharedMemorySafe because:
// - It's repr(C) with cache line alignment
// - All fields are SharedMemorySafe (AtomicUsize, SpscCell<usize>)
unsafe impl SharedMemorySafe for ProducerState {}

// SAFETY: ConsumerState is SharedMemorySafe because:
// - It's repr(C) with cache line alignment
// - All fields are SharedMemorySafe (AtomicUsize, SpscCell<usize>)
unsafe impl SharedMemorySafe for ConsumerState {}

// SAFETY: Slot is SharedMemorySafe because:
// - It's repr(C)
// - The value field is SpscCell<MaybeUninit<T>> where T: SharedMemorySafe
unsafe impl<T: SharedMemorySafe> SharedMemorySafe for Slot<T> {}

// SAFETY: Ring is SharedMemorySafe because:
// - It's repr(C)
// - All fields are SharedMemorySafe
unsafe impl<T: SharedMemorySafe, const N: usize> SharedMemorySafe for Ring<T, N> {}

/// Initialization marker for cross-process synchronization.
#[derive(SharedMemorySafe)]
#[repr(C)]
#[repr(align(64))]
struct InitMarker(AtomicU64);

/// IPC-specific queue layout with init marker and trailing padding.
#[repr(C)]
struct IpcQueue<T: SharedMemorySafe, const N: usize> {
    /// Magic value to indicate initialization is complete.
    init: InitMarker,

    /// The core ring buffer.
    ring: Ring<T, N>,

    /// Prevent false sharing with adjacent shared memory regions.
    _padding_tail: [u8; 64],
}

// SAFETY: IpcQueue is SharedMemorySafe because all fields are.
unsafe impl<T: SharedMemorySafe, const N: usize> SharedMemorySafe for IpcQueue<T, N> {}

// SAFETY: IpcQueue is Send because all fields are Send.
unsafe impl<T: SharedMemorySafe, const N: usize> Send for IpcQueue<T, N> {}

// SAFETY: IpcQueue is Sync because concurrent access is mediated by atomics.
unsafe impl<T: SharedMemorySafe, const N: usize> Sync for IpcQueue<T, N> {}

/// Zero-sized proof that initialization succeeded.
///
/// This type can only be constructed by successfully waiting for init,
/// providing compile-time evidence that the queue is ready for use.
#[derive(Debug, Clone, Copy)]
struct InitProof(());

impl<T: SharedMemorySafe, const N: usize> IpcQueue<T, N> {
    /// Initializes the queue directly inside shared memory.
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    /// - **Exclusive access**: No other references to this memory exist during initialization
    /// - **Full initialization**: After this function returns, the caller must treat the memory as initialized
    fn init_shared(uninit: &mut std::mem::MaybeUninit<Self>) {
        use std::sync::atomic::Ordering;

        let ptr = uninit.as_mut_ptr();
        unsafe {
            // Initialize init marker
            std::ptr::addr_of_mut!((*ptr).init).write(InitMarker(AtomicU64::new(0)));

            // Initialize ring's producer and consumer state
            std::ptr::addr_of_mut!((*ptr).ring.producer).write(ProducerState::default());
            std::ptr::addr_of_mut!((*ptr).ring.consumer).write(ConsumerState::default());

            // Release store to signal initialization complete
            (*ptr).init.0.store(INIT_MAGIC, Ordering::Release);
        }
    }

    /// Spins until the queue is initialized or timeout expires.
    ///
    /// Returns `Some(InitProof)` on success, `None` on timeout.
    /// The proof guarantees the queue has been initialized.
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    /// - **Pointer validity**: `ptr` points to mapped shared memory
    /// - **Lifetime**: The memory remains mapped for the duration of this call
    unsafe fn wait_for_init(ptr: *const Self, timeout: std::time::Duration) -> Option<InitProof> {
        use std::sync::atomic::Ordering;

        let start = std::time::Instant::now();
        loop {
            if unsafe { (*ptr).init.0.load(Ordering::Acquire) } == INIT_MAGIC {
                return Some(InitProof(()));
            }
            if start.elapsed() >= timeout {
                return None;
            }
            std::hint::spin_loop();
        }
    }
}

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
    shm: Shm<IpcQueue<T, N>, Mode>,
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

        let shm = Shm::<IpcQueue<T, N>, Creator>::create(path, |uninit| {
            IpcQueue::<T, N>::init_shared(uninit);
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

        let shm = Shm::<IpcQueue<T, N>, Opener>::open(path.clone())?;
        // SAFETY: Shm::open guarantees the pointer is valid and mapped.
        let Some(_proof) = (unsafe { IpcQueue::<T, N>::wait_for_init(&raw const *shm, INIT_TIMEOUT) }) else {
            return Err(ShmError::InitTimeout { path: path.into() });
        };
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
        // SAFETY: Producer has exclusive access to the producer side of the ring.
        // The IpcQueue has been initialized (checked during create/open).
        unsafe { self.shm.ring.push(item) }
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
    shm: Shm<IpcQueue<T, N>, Mode>,
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
        let shm = Shm::<IpcQueue<T, N>, Creator>::create(path, |uninit| {
            IpcQueue::<T, N>::init_shared(uninit);
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
        let shm = Shm::<IpcQueue<T, N>, Opener>::open(path.clone())?;
        // SAFETY: Shm::open guarantees the pointer is valid and mapped.
        let Some(_proof) = (unsafe { IpcQueue::<T, N>::wait_for_init(&raw const *shm, INIT_TIMEOUT) }) else {
            return Err(ShmError::InitTimeout { path: path.into() });
        };
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
        // SAFETY: Consumer has exclusive access to the consumer side of the ring.
        // The IpcQueue has been initialized (checked during create/open).
        unsafe { self.shm.ring.pop() }
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

    const CACHE_LINE_SIZE: usize = 64;

    #[test]
    fn test_cache_line_alignment() {
        // Verify our structs are properly aligned
        assert_eq!(std::mem::align_of::<ProducerState>(), CACHE_LINE_SIZE);
        assert_eq!(std::mem::align_of::<ConsumerState>(), CACHE_LINE_SIZE);

        // Verify they're on separate cache lines
        assert!(std::mem::size_of::<ProducerState>() <= CACHE_LINE_SIZE);
        assert!(std::mem::size_of::<ConsumerState>() <= CACHE_LINE_SIZE);
    }

    #[test]
    fn test_buffer_starts_on_separate_cache_line() {
        use std::mem::{offset_of, size_of};

        type TestQueue = IpcQueue<u64, 16>;

        // Verify InitMarker takes exactly one cache line
        assert_eq!(size_of::<InitMarker>(), CACHE_LINE_SIZE);

        // Ring layout: ProducerState(64) + ConsumerState(64) + Padding(64) = 192 to buffer
        assert_eq!(offset_of!(Ring<u64, 16>, buffer), 3 * CACHE_LINE_SIZE);

        // IpcQueue layout: InitMarker(64) + Ring, so buffer is at 64 + 192 = 256
        assert_eq!(
            offset_of!(TestQueue, ring) + offset_of!(Ring<u64, 16>, buffer),
            4 * CACHE_LINE_SIZE
        );
    }

    #[test]
    fn test_basic_push_pop() {
        let path = ShmPath::new("/test-basic-push-pop").unwrap();
        let producer = Producer::<u64, 8, _>::create(path.clone()).unwrap();
        let consumer = Consumer::<u64, 8, _>::open(path).unwrap();

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
        let producer = Producer::<u64, 16, _>::create(path.clone()).unwrap();
        let consumer = Consumer::<u64, 16, _>::open(path).unwrap();

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
        let producer = Producer::<u64, 4, _>::create(path.clone()).unwrap();
        let consumer = Consumer::<u64, 4, _>::open(path).unwrap();

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
        let producer = Producer::<u64, 8, _>::create(path.clone()).unwrap();
        let consumer = Consumer::<u64, 8, _>::open(path).unwrap();

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
        let producer = Producer::<u64, 4, _>::create(path.clone()).unwrap();
        let consumer = Consumer::<u64, 4, _>::open(path).unwrap();

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
        let producer = Producer::<u64, 8, _>::create(path.clone()).unwrap();
        let consumer = Consumer::<u64, 8, _>::open(path).unwrap();

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
        let consumer = Consumer::<u64, 8, _>::create(path.clone()).unwrap();

        // Client opens the inbox to send to daemon
        let producer = Producer::<u64, 8, _>::open(path).unwrap();

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
        let producer = Producer::<u64, 8, _>::create(path.clone()).unwrap();
        let consumer = Consumer::<u64, 8, _>::open(path).unwrap();

        producer.push(42).unwrap();
        assert_eq!(consumer.pop(), Some(42));
    }
}
