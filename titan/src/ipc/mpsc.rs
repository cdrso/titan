//! Lock-free MPSC queue over POSIX shared memory.
//!
//! A wait-free bounded queue for cross-process communication supporting
//! multiple producers and a single consumer.
//!
//! # Overview
//!
//! - [`Producer`] - Write end (multiple producers allowed per queue)
//! - [`Consumer`] - Read end (single consumer per queue)
//! - Lock-free, wait-free: no mutexes or syscalls in the hot path
//!
//! # Example
//!
//! ```no_run
//! use titan::ipc::mpsc::{Producer, Consumer};
//! use titan::ipc::shmem::ShmPath;
//!
//! let path = ShmPath::new("/my-inbox").unwrap();
//!
//! // Daemon creates inbox (consumer)
//! let consumer = Consumer::<u64, 1024, _>::create(path.clone())?;
//!
//! // Clients open inbox (producers) - multiple allowed!
//! let producer1 = Producer::<u64, 1024, _>::open(path.clone())?;
//! let producer2 = Producer::<u64, 1024, _>::open(path)?;
//!
//! producer1.push(1).expect("Queue full");
//! producer2.push(2).expect("Queue full");
//!
//! assert!(consumer.pop().is_some());
//! assert!(consumer.pop().is_some());
//! # Ok::<(), titan::ipc::shmem::ShmError>(())
//! ```
//!
//! # Algorithm
//!
//! Uses per-slot sequence numbers for synchronization:
//! - Producers atomically reserve positions via `fetch_add` on head
//! - Each slot has a sequence number indicating its state
//! - Consumer checks sequence before reading, updates after to release slot

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use minstant::Instant;

use super::shmem::{Creator, Opener, Shm, ShmError, ShmMode, ShmPath};
use crate::SharedMemorySafe;
use crate::mpsc::ring::{ConsumerState, ProducerState, Ring, Slot};

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

const INIT_MAGIC: u64 = 0x4D50_5343_494E_4954; // "MPSCINIT" in ASCII
const INIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

// SharedMemorySafe implementations for ring types
// SAFETY: Slot can be placed in shared memory because:
// - It's repr(C) with proper alignment
// - seq is AtomicUsize (SharedMemorySafe)
// - value is UnsafeCell<MaybeUninit<T>> where T: SharedMemorySafe
unsafe impl<T: SharedMemorySafe> SharedMemorySafe for Slot<T> {}

// SAFETY: ProducerState is SharedMemorySafe because:
// - It's repr(C) with cache line alignment
// - head is AtomicUsize (SharedMemorySafe)
unsafe impl SharedMemorySafe for ProducerState {}

// SAFETY: ConsumerState is SharedMemorySafe because:
// - It's repr(C) with cache line alignment
// - tail is AtomicUsize (SharedMemorySafe)
unsafe impl SharedMemorySafe for ConsumerState {}

// SAFETY: Ring is SharedMemorySafe because:
// - It's repr(C)
// - All fields are SharedMemorySafe
unsafe impl<T: SharedMemorySafe, const N: usize> SharedMemorySafe for Ring<T, N> {}

/// Initialization marker for cross-process synchronization.
#[derive(SharedMemorySafe)]
#[repr(C)]
#[repr(align(64))]
struct InitMarker(AtomicU64);

/// IPC-specific queue layout with init marker.
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

            // Initialize all slots with their index as initial sequence number
            for i in 0..N {
                std::ptr::addr_of_mut!((*ptr).ring.buffer[i]).write(Slot::new(i));
            }

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

/// Write end of the MPSC queue.
///
/// Multiple producers can exist per queue—this is safe and expected.
/// Each producer can push concurrently without data races.
///
/// # Thread Safety
///
/// `Producer` is [`Send`] but **not** [`Sync`]:
/// - Can transfer ownership to another thread
/// - Cannot share `&Producer` across threads
/// - Multiple `Producer` instances for the same queue IS allowed
pub struct Producer<T: SharedMemorySafe, const N: usize, Mode: ShmMode> {
    shm: Shm<IpcQueue<T, N>, Mode>,
    _unsync: PhantomUnsync,
}

impl<T: SharedMemorySafe, const N: usize> Producer<T, N, Creator> {
    /// Creates a new queue and returns a producer end.
    ///
    /// Unlinks shared memory on drop.
    ///
    /// # Errors
    ///
    /// `EEXIST` (path exists), `EACCES` (permissions), `ENOMEM` (resources).
    pub fn create(path: ShmPath) -> Result<Self, ShmError> {
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
    /// Opens an existing queue and returns a producer end.
    ///
    /// Does not unlink on drop. Waits up to 1s for initialization.
    /// Multiple producers can open the same queue—this is the expected use case.
    ///
    /// # Errors
    ///
    /// `ENOENT` (doesn't exist), `EACCES` (permissions), size mismatch, init timeout.
    pub fn open(path: ShmPath) -> Result<Self, ShmError> {
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
        // SAFETY: The IpcQueue has been initialized (checked during create/open).
        // Multiple producers calling push concurrently is safe by design.
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

/// Read end of the MPSC queue.
///
/// Only one consumer should exist per queue—multiple consumers cause data races.
///
/// # Thread Safety
///
/// `Consumer` is [`Send`] but **not** [`Sync`]:
/// - Can transfer ownership to another thread
/// - Cannot share `&Consumer` (no concurrent `pop()`)
///
/// **Cross-process**: The type system cannot prevent multiple processes from
/// calling `open()` as consumer. Callers must ensure exactly one consumer exists.
pub struct Consumer<T: SharedMemorySafe, const N: usize, Mode: ShmMode> {
    shm: Shm<IpcQueue<T, N>, Mode>,
    _unsync: PhantomUnsync,
}

impl<T: SharedMemorySafe, const N: usize> Consumer<T, N, Creator> {
    /// Creates a new queue and returns the consumer end.
    ///
    /// This is the typical pattern for daemon inboxes: the daemon creates the
    /// queue as consumer, and clients open it as producers.
    ///
    /// # Errors
    ///
    /// See [`Producer::create`].
    pub fn create(path: ShmPath) -> Result<Self, ShmError> {
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
    use std::thread;

    #[test]
    fn test_basic_push_pop() {
        let path = ShmPath::new("/test-mpsc-basic").unwrap();
        let consumer = Consumer::<u64, 8, _>::create(path.clone()).unwrap();
        let producer = Producer::<u64, 8, _>::open(path).unwrap();

        // Push a single item
        assert!(producer.push(42).is_ok());

        // Pop it back
        assert_eq!(consumer.pop(), Some(42));

        // Queue should now be empty
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_multiple_producers_same_process() {
        let path = ShmPath::new("/test-mpsc-multi-prod").unwrap();
        let consumer = Consumer::<u64, 16, _>::create(path.clone()).unwrap();

        // Multiple producers opening the same queue
        let producer1 = Producer::<u64, 16, _>::open(path.clone()).unwrap();
        let producer2 = Producer::<u64, 16, _>::open(path).unwrap();

        // Both can push
        producer1.push(1).unwrap();
        producer2.push(2).unwrap();
        producer1.push(3).unwrap();
        producer2.push(4).unwrap();

        // Consumer gets all items (order depends on timing, but all should arrive)
        let mut items = vec![];
        while let Some(item) = consumer.pop() {
            items.push(item);
        }

        items.sort();
        assert_eq!(items, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_concurrent_producers() {
        let path = ShmPath::new("/test-mpsc-concurrent").unwrap();
        let consumer = Consumer::<u64, 64, _>::create(path.clone()).unwrap();

        let num_producers = 4;
        let items_per_producer = 10;

        let mut handles = vec![];

        for p in 0..num_producers {
            let path = path.clone();
            handles.push(thread::spawn(move || {
                let producer = Producer::<u64, 64, _>::open(path).unwrap();
                for i in 0..items_per_producer {
                    let value = (p * 100 + i) as u64;
                    loop {
                        if producer.push(value).is_ok() {
                            break;
                        }
                        thread::yield_now();
                    }
                }
            }));
        }

        // Wait for all producers
        for h in handles {
            h.join().unwrap();
        }

        // Collect all items
        let mut items = vec![];
        while let Some(item) = consumer.pop() {
            items.push(item);
        }

        // Verify we got all items
        assert_eq!(items.len(), num_producers * items_per_producer);

        // Verify all expected values are present
        for p in 0..num_producers {
            for i in 0..items_per_producer {
                let expected = (p * 100 + i) as u64;
                assert!(items.contains(&expected), "Missing value {expected}");
            }
        }
    }

    #[test]
    fn test_queue_full() {
        let path = ShmPath::new("/test-mpsc-full").unwrap();
        let consumer = Consumer::<u64, 4, _>::create(path.clone()).unwrap();
        let producer = Producer::<u64, 4, _>::open(path).unwrap();

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
    fn test_wraparound() {
        let path = ShmPath::new("/test-mpsc-wrap").unwrap();
        let consumer = Consumer::<u64, 4, _>::create(path.clone()).unwrap();
        let producer = Producer::<u64, 4, _>::open(path).unwrap();

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
}
