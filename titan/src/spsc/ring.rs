//! Core lock-free SPSC ring buffer algorithm.
//!
//! This module provides the fundamental ring buffer data structure and algorithm
//! used by both IPC (shared memory) and in-process (heap) SPSC queues.
//!
//! # Safety
//!
//! The types in this module have unsafe APIs because they require the caller to
//! uphold the SPSC invariant: exactly one producer and one consumer, with no
//! concurrent access to either role.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Role marker: Fields with this role are owned exclusively by the producer.
pub struct ProducerRole;

/// Role marker: Fields with this role are owned exclusively by the consumer.
pub struct ConsumerRole;

/// Role marker: Buffer slots whose ownership transfers via the SPSC protocol.
pub struct SlotRole;

/// Interior-mutable cell with a role marker for nominal type safety.
///
/// `SpscCell<T, Role>` wraps an `UnsafeCell<T>` with a phantom `Role` parameter.
/// The `Role` doesn't affect runtime behavior—it exists purely to make different
/// logical "kinds" of cells into distinct types at compile time.
#[repr(transparent)]
pub struct SpscCell<T, Role>(UnsafeCell<T>, PhantomData<Role>);

impl<T, Role> SpscCell<T, Role> {
    pub const fn new(value: T) -> Self {
        Self(UnsafeCell::new(value), PhantomData)
    }

    pub const fn get(&self) -> &UnsafeCell<T> {
        &self.0
    }
}

// SAFETY: SpscCell is Sync because the SPSC algorithm guarantees that each slot
// is either being written (by producer) or read (by consumer), never both.
// The atomic head/tail indices with Release/Acquire ordering provide the
// synchronization barrier between writes and reads.
unsafe impl<T: Send, Role> Sync for SpscCell<T, Role> {}
unsafe impl<T: Send, Role> Send for SpscCell<T, Role> {}

/// Cache cell owned exclusively by the producer.
pub type ProducerCache<T> = SpscCell<T, ProducerRole>;

/// Cache cell owned exclusively by the consumer.
pub type ConsumerCache<T> = SpscCell<T, ConsumerRole>;

/// Buffer slot cell with ownership governed by the SPSC protocol.
pub type SlotCell<T> = SpscCell<T, SlotRole>;

/// Producer-side state: head index and cached tail.
#[repr(C)]
#[repr(align(64))]
pub struct ProducerState {
    /// Write index (next slot to write to).
    /// Owned by producer, read by consumer.
    pub head: AtomicUsize,

    /// Producer-local cursor tracking `head % N`.
    pub cursor: ProducerCache<usize>,

    /// Cached copy of tail index.
    pub cached_tail: ProducerCache<usize>,
}

impl ProducerState {
    pub const fn new() -> Self {
        Self {
            head: AtomicUsize::new(0),
            cursor: ProducerCache::new(0),
            cached_tail: ProducerCache::new(0),
        }
    }
}

impl Default for ProducerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Consumer-side state: tail index and cached head.
#[repr(C)]
#[repr(align(64))]
pub struct ConsumerState {
    /// Read index (next slot to read from).
    /// Owned by consumer, read by producer.
    pub tail: AtomicUsize,

    /// Consumer-local cursor tracking `tail % N`.
    pub cursor: ConsumerCache<usize>,

    /// Cached copy of head index.
    pub cached_head: ConsumerCache<usize>,
}

impl ConsumerState {
    pub const fn new() -> Self {
        Self {
            tail: AtomicUsize::new(0),
            cursor: ConsumerCache::new(0),
            cached_head: ConsumerCache::new(0),
        }
    }
}

impl Default for ConsumerState {
    fn default() -> Self {
        Self::new()
    }
}

/// A single slot in the ring buffer.
#[repr(C)]
pub struct Slot<T> {
    pub value: SlotCell<MaybeUninit<T>>,
}

/// Core ring buffer structure without IPC-specific fields.
///
/// This is the shared implementation used by both IPC and heap-backed queues.
/// It contains only the essential SPSC algorithm state.
#[repr(C)]
pub struct Ring<T, const N: usize> {
    /// Producer state (head index + cached tail).
    pub producer: ProducerState,

    /// Consumer state (tail index + cached head).
    pub consumer: ConsumerState,

    /// Prevent false sharing between consumer state and buffer.
    pub _padding: [u8; 64],

    /// Ring buffer slots.
    pub buffer: [Slot<T>; N],
}

impl<T, const N: usize> Ring<T, N> {
    /// Advances a cursor to the next slot index, wrapping to 0 at capacity.
    ///
    /// This is equivalent to `(cursor + 1) % N` but avoids the division instruction,
    /// using a comparison and conditional move instead.
    #[inline]
    pub const fn bump_cursor(cursor: usize) -> usize {
        let next = cursor + 1;
        if next == N { 0 } else { next }
    }

    /// Attempts to push an item onto the queue.
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    /// - Only one thread/process calls this method (single producer)
    /// - The ring has been properly initialized
    #[inline]
    pub unsafe fn push(&self, item: T) -> Result<(), T> {
        // Load current head (producer-local, relaxed is fine)
        let head = self.producer.head.load(Ordering::Relaxed);

        // Load cached tail (local cache, doesn't need atomic ordering)
        // SAFETY: Producer has exclusive access to cached_tail
        let mut cached_tail = unsafe { *self.producer.cached_tail.get().get() };

        // Check if queue appears full using cached value
        if head.wrapping_sub(cached_tail) >= N {
            // Refresh cache from actual tail (acquire to sync with consumer)
            cached_tail = self.consumer.tail.load(Ordering::Acquire);
            // SAFETY: Producer has exclusive write access to its cached_tail field
            unsafe {
                *self.producer.cached_tail.get().get() = cached_tail;
            }

            // Check again with fresh value
            if head.wrapping_sub(cached_tail) >= N {
                return Err(item); // Queue is full
            }
        }

        // SAFETY: Producer has exclusive access to its cursor field.
        // The cursor is always in range [0, N) because:
        // - Initialized to 0
        // - Only modified by bump_cursor which preserves the invariant
        let slot_index = unsafe { *self.producer.cursor.get().get() };

        // SAFETY: The producer owns the slot at `slot_index` because:
        // - head hasn't been published yet (store happens after this write)
        // - The check above ensures head - tail < N, so the consumer isn't reading this slot
        // - slot_index is in [0, N) per the cursor invariant, so the indexing is in bounds
        unsafe {
            let slot_ptr = self.buffer[slot_index].value.get().get();
            std::ptr::write(slot_ptr, MaybeUninit::new(item));
        }

        // SAFETY: Producer has exclusive write access to its cursor field.
        // bump_cursor preserves the [0, N) invariant.
        unsafe {
            *self.producer.cursor.get().get() = Self::bump_cursor(slot_index);
        }

        // Publish the new head (release to sync with consumer)
        self.producer
            .head
            .store(head.wrapping_add(1), Ordering::Release);

        Ok(())
    }

    /// Attempts to pop an item from the queue.
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    /// - Only one thread/process calls this method (single consumer)
    /// - The ring has been properly initialized
    #[inline]
    pub unsafe fn pop(&self) -> Option<T> {
        // Load current tail (consumer-local, relaxed is fine)
        let tail = self.consumer.tail.load(Ordering::Relaxed);

        // Load cached head (local cache, doesn't need atomic ordering)
        // SAFETY: Consumer has exclusive access to cached_head
        let mut cached_head = unsafe { *self.consumer.cached_head.get().get() };

        // Check if queue appears empty using cached value
        if cached_head == tail {
            // Refresh cache from actual head (acquire to sync with producer)
            cached_head = self.producer.head.load(Ordering::Acquire);
            // SAFETY: Consumer has exclusive write access to its cached_head field
            unsafe {
                *self.consumer.cached_head.get().get() = cached_head;
            }

            // Check again with fresh value
            if cached_head == tail {
                return None; // Queue is empty
            }
        }

        // SAFETY: Consumer has exclusive access to its cursor field.
        // The cursor is always in range [0, N) because:
        // - Initialized to 0
        // - Only modified by bump_cursor which preserves the invariant
        let slot_index = unsafe { *self.consumer.cursor.get().get() };

        // SAFETY: The consumer owns the slot at `slot_index` because:
        // - The check above ensures head != tail, so there's data to read
        // - tail hasn't been published yet (store happens after this read)
        // - slot_index is in [0, N) per the cursor invariant, so the indexing is in bounds
        // - The producer won't overwrite this slot until we publish the new tail
        // - The slot was initialized by the producer (MaybeUninit::assume_init is valid)
        let item = unsafe {
            let slot_ptr = self.buffer[slot_index].value.get().get();
            std::ptr::read(slot_ptr).assume_init()
        };

        // Publish the new tail (release to sync with producer)
        self.consumer
            .tail
            .store(tail.wrapping_add(1), Ordering::Release);

        // SAFETY: Consumer has exclusive write access to its cursor field.
        // bump_cursor preserves the [0, N) invariant.
        unsafe {
            *self.consumer.cursor.get().get() = Self::bump_cursor(slot_index);
        }

        Some(item)
    }
}

// SAFETY: Ring is Send because all fields are Send (AtomicUsize, SpscCell).
unsafe impl<T: Send, const N: usize> Send for Ring<T, N> {}

// SAFETY: Ring is Sync because concurrent access is mediated by atomics:
// - head/tail are AtomicUsize with Release/Acquire ordering
// - Buffer slots are protected by the SPSC invariant (see SpscCell)
unsafe impl<T: Send, const N: usize> Sync for Ring<T, N> {}
