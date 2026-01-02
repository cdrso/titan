//! Core lock-free MPSC ring buffer algorithm.
//!
//! This module provides a bounded wait-free MPSC (Multi-Producer Single-Consumer)
//! ring buffer using per-slot sequence numbers for synchronization.
//!
//! # Algorithm
//!
//! The algorithm is based on Dmitry Vyukov's bounded MPMC queue, simplified for
//! the single-consumer case:
//!
//! - Each slot has an atomic sequence number
//! - Producers use `fetch_add` on head to reserve exclusive write positions
//! - After writing, producers publish by setting `slot.seq = pos + 1`
//! - Consumer checks if `slot.seq == tail + 1` before reading
//! - After reading, consumer sets `slot.seq = tail + N` to release the slot
//!
//! # Safety
//!
//! The producer side is wait-free for multiple concurrent producers.
//! The consumer side requires exactly one consumer (single consumer invariant).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A slot in the MPSC ring buffer with sequence number for synchronization.
#[repr(C)]
#[repr(align(64))] // Each slot on its own cache line to avoid false sharing between producers
pub struct Slot<T> {
    /// Sequence number for synchronization.
    /// - Initial: slot index (0, 1, 2, ..., N-1)
    /// - After producer write: position + 1 (signals "data ready")
    /// - After consumer read: position + N (signals "slot free")
    seq: AtomicUsize,

    /// The actual data stored in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    /// Creates a new slot with the given initial sequence number.
    pub(crate) const fn new(seq: usize) -> Self {
        Self {
            seq: AtomicUsize::new(seq),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// SAFETY: Slot is Sync because:
// - seq is AtomicUsize (inherently Sync)
// - value is protected by the sequence number protocol
unsafe impl<T: Send> Sync for Slot<T> {}
unsafe impl<T: Send> Send for Slot<T> {}

/// Producer-side state: head index for slot reservation.
#[repr(C)]
#[repr(align(64))]
pub struct ProducerState {
    /// Next position to reserve for writing.
    /// Multiple producers atomically increment this via `fetch_add`.
    pub(crate) head: AtomicUsize,
}

impl ProducerState {
    pub(crate) const fn new() -> Self {
        Self {
            head: AtomicUsize::new(0),
        }
    }
}

impl Default for ProducerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Consumer-side state: tail index and cached values.
#[repr(C)]
#[repr(align(64))]
pub struct ConsumerState {
    /// Next position to read from.
    /// Only the consumer modifies this.
    pub(crate) tail: AtomicUsize,
}

impl ConsumerState {
    pub(crate) const fn new() -> Self {
        Self {
            tail: AtomicUsize::new(0),
        }
    }
}

impl Default for ConsumerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Core MPSC ring buffer structure.
///
/// This is the shared implementation used by IPC-backed queues.
#[repr(C)]
pub struct Ring<T, const N: usize> {
    /// Producer state (head index for reservation).
    pub(crate) producer: ProducerState,

    /// Consumer state (tail index).
    pub(crate) consumer: ConsumerState,

    /// Ring buffer slots with per-slot sequence numbers.
    pub(crate) buffer: [Slot<T>; N],
}

impl<T, const N: usize> Ring<T, N> {
    /// Attempts to push an item onto the queue.
    ///
    /// This operation is lock-free for producers. Multiple producers can
    /// safely call this method concurrently.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the item was successfully enqueued
    /// - `Err(item)` if the queue is full
    ///
    /// # Safety
    ///
    /// Caller must ensure the ring has been properly initialized.
    #[inline]
    pub(crate) unsafe fn push(&self, item: T) -> Result<(), T> {
        loop {
            let pos = self.producer.head.load(Ordering::Relaxed);
            let slot_idx = pos % N;
            let slot = &self.buffer[slot_idx];

            let seq = slot.seq.load(Ordering::Acquire);

            // Calculate the difference between sequence and position.
            // This handles wrapping correctly.
            let diff = seq.wrapping_sub(pos) as isize;

            if diff == 0 {
                // Slot is available for writing at this position.
                // Try to reserve it atomically.
                if self
                    .producer
                    .head
                    .compare_exchange_weak(
                        pos,
                        pos.wrapping_add(1),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    // Successfully reserved this slot.
                    // SAFETY: We have exclusive write access because:
                    // - CAS succeeded, so no other producer can claim this slot
                    // - seq == pos means consumer has released it
                    unsafe {
                        (*slot.value.get()).write(item);
                    }
                    // Publish the write by setting seq = pos + 1
                    slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                    return Ok(());
                }
                // CAS failed: another producer beat us, retry with new head
            } else if diff < 0 {
                // seq < pos: Slot is not yet released by consumer.
                // Queue is full.
                return Err(item);
            }
            // diff > 0: seq > pos, which means head has moved past this position.
            // Another producer took it. Retry with fresh head value.
        }
    }

    /// Attempts to pop an item from the queue.
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    /// - Only one thread/process calls this method (single consumer)
    /// - The ring has been properly initialized
    #[inline]
    pub(crate) unsafe fn pop(&self) -> Option<T> {
        let tail = self.consumer.tail.load(Ordering::Relaxed);
        let slot_idx = tail % N;
        let slot = &self.buffer[slot_idx];

        let seq = slot.seq.load(Ordering::Acquire);

        // A slot is ready to read when seq == tail + 1
        // (producer set it after writing)
        let expected_seq = tail.wrapping_add(1);

        if seq != expected_seq {
            // No data ready at this position
            return None;
        }

        // Read the value
        // SAFETY: The sequence check confirms the producer has finished writing
        let item = unsafe { (*slot.value.get()).assume_init_read() };

        // Release the slot by setting seq = tail + N
        // This tells producers this slot is available at position (tail + N)
        slot.seq.store(tail.wrapping_add(N), Ordering::Release);

        // Advance tail
        self.consumer
            .tail
            .store(tail.wrapping_add(1), Ordering::Relaxed);

        Some(item)
    }
}

// SAFETY: Ring is Send because all fields are Send.
unsafe impl<T: Send, const N: usize> Send for Ring<T, N> {}

// SAFETY: Ring is Sync because concurrent access is mediated by atomics:
// - Producers synchronize via fetch_add/CAS on head
// - Per-slot sequence numbers provide producer-consumer synchronization
unsafe impl<T: Send, const N: usize> Sync for Ring<T, N> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    /// Helper to create an initialized ring.
    fn create_ring<T, const N: usize>() -> Ring<T, N> {
        // Initialize slots with their index as initial sequence
        let buffer = std::array::from_fn(|i| Slot::new(i));
        Ring {
            producer: ProducerState::new(),
            consumer: ConsumerState::new(),
            buffer,
        }
    }

    #[test]
    fn test_single_producer_single_consumer() {
        let ring: Ring<u64, 8> = create_ring();

        // Push some items
        unsafe {
            assert!(ring.push(1).is_ok());
            assert!(ring.push(2).is_ok());
            assert!(ring.push(3).is_ok());
        }

        // Pop them back
        unsafe {
            assert_eq!(ring.pop(), Some(1));
            assert_eq!(ring.pop(), Some(2));
            assert_eq!(ring.pop(), Some(3));
            assert_eq!(ring.pop(), None);
        }
    }

    #[test]
    fn test_queue_full() {
        let ring: Ring<u64, 4> = create_ring();

        // Fill the queue
        unsafe {
            assert!(ring.push(1).is_ok());
            assert!(ring.push(2).is_ok());
            assert!(ring.push(3).is_ok());
            assert!(ring.push(4).is_ok());

            // Should be full now
            assert!(ring.push(5).is_err());
        }

        // Pop one, should be able to push again
        unsafe {
            assert_eq!(ring.pop(), Some(1));
            assert!(ring.push(5).is_ok());
            assert!(ring.push(6).is_err()); // Full again
        }
    }

    #[test]
    fn test_multiple_producers() {
        let ring: Arc<Ring<u64, 64>> = Arc::new(create_ring());
        let num_producers = 4;
        let items_per_producer = 10;

        let mut handles = vec![];

        // Spawn multiple producers
        for p in 0..num_producers {
            let ring = Arc::clone(&ring);
            handles.push(thread::spawn(move || {
                for i in 0..items_per_producer {
                    let value = (p * 100 + i) as u64;
                    // Retry until success
                    loop {
                        unsafe {
                            if ring.push(value).is_ok() {
                                break;
                            }
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
        loop {
            match unsafe { ring.pop() } {
                Some(item) => items.push(item),
                None => break,
            }
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
    fn test_concurrent_producer_consumer() {
        let ring: Arc<Ring<u64, 32>> = Arc::new(create_ring());
        let num_items = 1000;

        let ring_producer = Arc::clone(&ring);
        let producer = thread::spawn(move || {
            for i in 0..num_items {
                loop {
                    unsafe {
                        if ring_producer.push(i).is_ok() {
                            break;
                        }
                    }
                    thread::yield_now();
                }
            }
        });

        let ring_consumer = Arc::clone(&ring);
        let consumer = thread::spawn(move || {
            let mut received = 0u64;
            let mut sum = 0u64;
            while received < num_items {
                if let Some(item) = unsafe { ring_consumer.pop() } {
                    sum += item;
                    received += 1;
                } else {
                    thread::yield_now();
                }
            }
            sum
        });

        producer.join().unwrap();
        let sum = consumer.join().unwrap();

        // Sum of 0..1000 = 999 * 1000 / 2 = 499500
        assert_eq!(sum, (num_items - 1) * num_items / 2);
    }
}
