//! Lock-free SPSC queue for in-process (inter-thread) communication.
//!
//! A wait-free bounded queue using a heap-allocated ring buffer with atomic indices.
//!
//! # Overview
//!
//! - [`Producer`] - Write end (single producer per queue)
//! - [`Consumer`] - Read end (single consumer per queue)
//! - Lock-free, wait-free: no mutexes or syscalls in the hot path
//!
//! # Example
//!
//! ```
//! use titan::sync::spsc;
//!
//! let (producer, consumer) = spsc::channel::<u64, 1024>();
//!
//! // Producer thread
//! producer.push(42).expect("Queue full");
//!
//! // Consumer thread
//! assert_eq!(consumer.pop(), Some(42));
//! ```
//!
//! # Differences from [`crate::ipc::spsc`]
//!
//! - No shared memory: uses heap allocation via `Arc`
//! - No `SharedMemorySafe` bound: only requires `T: Send`
//! - Simpler construction: `channel()` returns a `(Producer, Consumer)` pair

use std::cell::Cell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::Duration;

use minstant::Instant;

use crate::spsc::ring::{ConsumerState, ProducerState, Ring, Slot};

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

/// Heap-allocated ring buffer for inter-thread SPSC.
///
/// Unlike the IPC version, this has no init marker since construction
/// is synchronous within a single process.
#[repr(C)]
struct HeapRing<T, const N: usize> {
    ring: Ring<T, N>,
}

impl<T, const N: usize> HeapRing<T, N> {
    /// Creates a new initialized ring buffer.
    fn new() -> Self {
        // Initialize producer and consumer state
        // Buffer slots start as MaybeUninit (uninitialized is fine)
        Self {
            ring: Ring {
                producer: ProducerState::new(),
                consumer: ConsumerState::new(),
                _padding: [0u8; 64],
                // SAFETY: MaybeUninit doesn't require initialization
                buffer: unsafe { MaybeUninit::<[Slot<T>; N]>::uninit().assume_init() },
            },
        }
    }
}

// SAFETY: HeapRing is Send because all fields are Send.
unsafe impl<T: Send, const N: usize> Send for HeapRing<T, N> {}

// SAFETY: HeapRing is Sync because concurrent access is mediated by atomics
// and the SPSC protocol.
unsafe impl<T: Send, const N: usize> Sync for HeapRing<T, N> {}

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
pub struct Producer<T: Send, const N: usize> {
    ring: Arc<HeapRing<T, N>>,
    _unsync: PhantomUnsync,
}

/// Read end of the SPSC queue.
///
/// Only one consumer should exist per queue—multiple consumers cause data races.
/// See [`Producer`] for thread safety details (same semantics apply).
pub struct Consumer<T: Send, const N: usize> {
    ring: Arc<HeapRing<T, N>>,
    _unsync: PhantomUnsync,
}

struct CapacityCheck<const N: usize>;

impl<const N: usize> CapacityCheck<N> {
    /// Compile-time assertion that queue capacity is non-zero.
    const OK: () = assert!(N > 0, "Queue capacity must be greater than 0");
}

/// Creates a new SPSC channel with the given capacity.
///
/// Returns a `(Producer, Consumer)` pair. The producer and consumer can be
/// sent to different threads.
///
/// # Panics
///
/// Fails to compile if `N == 0`.
///
/// # Example
///
/// ```
/// use titan::sync::spsc;
///
/// let (tx, rx) = spsc::channel::<String, 16>();
///
/// tx.push("hello".to_string()).unwrap();
/// assert_eq!(rx.pop(), Some("hello".to_string()));
/// ```
#[must_use]
pub fn channel<T: Send, const N: usize>() -> (Producer<T, N>, Consumer<T, N>) {
    let () = CapacityCheck::<N>::OK;

    let ring = Arc::new(HeapRing::new());

    let producer = Producer {
        ring: Arc::clone(&ring),
        _unsync: PhantomData,
    };

    let consumer = Consumer {
        ring,
        _unsync: PhantomData,
    };

    (producer, consumer)
}

impl<T: Send, const N: usize> Producer<T, N> {
    /// Attempts to push an item onto the queue (wait-free).
    ///
    /// # Errors
    ///
    /// Returns `Err(item)` if the queue is full, allowing retry.
    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        // SAFETY: Producer has exclusive access to the producer side of the ring.
        // The ring is initialized during construction.
        unsafe { self.ring.ring.push(item) }
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

impl<T: Send, const N: usize> Consumer<T, N> {
    /// Attempts to pop an item from the queue (wait-free).
    ///
    /// Returns `None` if the queue is empty.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        // SAFETY: Consumer has exclusive access to the consumer side of the ring.
        // The ring is initialized during construction.
        unsafe { self.ring.ring.pop() }
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

    #[test]
    fn test_basic_push_pop() {
        let (producer, consumer) = channel::<u64, 8>();

        assert!(producer.push(42).is_ok());
        assert_eq!(consumer.pop(), Some(42));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_multiple_items() {
        let (producer, consumer) = channel::<u64, 16>();

        for i in 0..10 {
            assert!(producer.push(i).is_ok());
        }

        for i in 0..10 {
            assert_eq!(consumer.pop(), Some(i));
        }

        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_queue_full() {
        let (producer, consumer) = channel::<u64, 4>();

        for i in 0..4 {
            assert!(producer.push(i).is_ok(), "Failed to push item {i}");
        }

        assert_eq!(producer.push(999), Err(999));

        assert_eq!(consumer.pop(), Some(0));
        assert!(producer.push(4).is_ok());
        assert_eq!(producer.push(1000), Err(1000));
    }

    #[test]
    fn test_queue_empty() {
        let (producer, consumer) = channel::<u64, 8>();

        assert_eq!(consumer.pop(), None);

        producer.push(42).unwrap();
        assert_eq!(consumer.pop(), Some(42));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn test_wrapping_behavior() {
        let (producer, consumer) = channel::<u64, 4>();

        for round in 0..5 {
            for i in 0..4 {
                let value = round * 10 + i;
                assert!(producer.push(value).is_ok());
            }

            for i in 0..4 {
                let expected = round * 10 + i;
                assert_eq!(consumer.pop(), Some(expected));
            }

            assert_eq!(consumer.pop(), None);
        }
    }

    #[test]
    fn test_interleaved_operations() {
        let (producer, consumer) = channel::<u64, 8>();

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
    fn test_send_to_thread() {
        let (producer, consumer) = channel::<u64, 16>();

        let handle = std::thread::spawn(move || {
            for i in 0..10 {
                producer.push(i).unwrap();
            }
        });

        handle.join().unwrap();

        for i in 0..10 {
            assert_eq!(consumer.pop(), Some(i));
        }
    }

    #[test]
    fn test_concurrent_push_pop() {
        let (producer, consumer) = channel::<u64, 64>();
        let count = 1000u64;

        let producer_handle = std::thread::spawn(move || {
            for i in 0..count {
                while producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer_handle = std::thread::spawn(move || {
            let mut received = Vec::with_capacity(count as usize);
            while received.len() < count as usize {
                if let Some(item) = consumer.pop() {
                    received.push(item);
                } else {
                    std::hint::spin_loop();
                }
            }
            received
        });

        producer_handle.join().unwrap();
        let received = consumer_handle.join().unwrap();

        // Verify FIFO order
        for (i, &val) in received.iter().enumerate() {
            assert_eq!(val, i as u64);
        }
    }

    #[test]
    fn test_non_copy_type() {
        let (producer, consumer) = channel::<String, 8>();

        producer.push("hello".to_string()).unwrap();
        producer.push("world".to_string()).unwrap();

        assert_eq!(consumer.pop(), Some("hello".to_string()));
        assert_eq!(consumer.pop(), Some("world".to_string()));
        assert_eq!(consumer.pop(), None);
    }
}
