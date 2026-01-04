//! Retransmit ring buffer for NAK-based reliability.
//!
//! The `RetransmitRing` stores frames for potential retransmission. When a subscriber
//! sends a NAK requesting missing data, the publisher looks up the frame here.
//!
//! # Design
//!
//! - Fixed-size circular buffer indexed by sequence number
//! - Drop-oldest policy: when full, oldest entries are evicted (publisher never stalls)
//! - O(1) push, O(1) lookup by sequence number
//! - Tracks oldest available sequence for DATA_NOT_AVAIL responses
//! - Uses [`SeqNum`] at public API boundaries for type safety
//!
//! # Example
//!
//! ```ignore
//! use crate::data::types::SeqNum;
//!
//! let mut ring: RetransmitRing<Frame<1500>, 256> = RetransmitRing::new();
//!
//! // Push frames as they're sent
//! let seq: SeqNum = ring.push(frame);
//!
//! // Look up for retransmission
//! match ring.get(requested_seq) {
//!     RetransmitLookup::Available(frame) => { /* retransmit */ }
//!     RetransmitLookup::Evicted { .. } => { /* send DATA_NOT_AVAIL */ }
//!     RetransmitLookup::NotYetSent { .. } => { /* ignore spurious NAK */ }
//! }
//! ```

use std::mem::MaybeUninit;

use crate::data::types::SeqNum;

/// Result of looking up a sequence number in the retransmit ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetransmitLookup<'a, T> {
    /// Frame is available for retransmission.
    Available(&'a T),
    /// Sequence number has been evicted (too old).
    Evicted {
        /// The requested sequence number.
        requested: SeqNum,
        /// Oldest sequence still in the buffer.
        oldest_available: SeqNum,
    },
    /// Sequence number hasn't been sent yet (future).
    NotYetSent {
        /// The requested sequence number.
        requested: SeqNum,
        /// Next sequence that will be assigned.
        next_seq: SeqNum,
    },
}

/// A slot in the retransmit ring.
struct Slot<T> {
    /// The stored frame (valid when occupied).
    value: MaybeUninit<T>,
    /// Sequence number of this slot (used to verify lookup).
    seq: u64,
    /// Whether this slot contains valid data.
    occupied: bool,
}

impl<T> Slot<T> {
    const fn empty() -> Self {
        Self {
            value: MaybeUninit::uninit(),
            seq: 0,
            occupied: false,
        }
    }
}

/// Circular buffer for storing frames pending potential retransmission.
///
/// # Type Parameters
///
/// - `T`: The frame type (e.g., `Frame<1500>`)
/// - `N`: Buffer capacity (number of frames). Should be power of 2 for efficient indexing.
///
/// # Invariants
///
/// - `head` is the next sequence number to be assigned
/// - `tail` is the oldest sequence still in the buffer (or equals head if empty)
/// - `head - tail <= N` (buffer never exceeds capacity)
/// - Slots in range `[tail, head)` are occupied
pub struct RetransmitRing<T, const N: usize> {
    /// Ring buffer slots.
    buffer: Box<[Slot<T>; N]>,
    /// Next sequence number to assign (also: one past the newest entry).
    head: u64,
    /// Oldest sequence in the buffer (or equals head if empty).
    tail: u64,
}

impl<T, const N: usize> RetransmitRing<T, N> {
    /// Creates a new empty retransmit ring.
    ///
    /// # Panics
    ///
    /// Panics if `N` is 0.
    #[must_use]
    pub fn new() -> Self {
        assert!(N > 0, "RetransmitRing capacity must be > 0");

        // Initialize slots - use Box to avoid stack overflow for large N
        let buffer = {
            let mut vec = Vec::with_capacity(N);
            for _ in 0..N {
                vec.push(Slot::empty());
            }
            vec.into_boxed_slice()
                .try_into()
                .unwrap_or_else(|_| unreachable!())
        };

        Self {
            buffer,
            head: 0,
            tail: 0,
        }
    }

    /// Creates a new retransmit ring starting at a specific sequence number.
    ///
    /// Useful when joining an existing stream or resuming from a checkpoint.
    #[must_use]
    pub fn with_start_seq(start_seq: SeqNum) -> Self {
        let mut ring = Self::new();
        let seq_raw = start_seq.as_u64();
        ring.head = seq_raw;
        ring.tail = seq_raw;
        ring
    }

    /// Returns the next sequence number that will be assigned.
    #[inline]
    #[must_use]
    pub fn next_seq(&self) -> SeqNum {
        SeqNum::from(self.head)
    }

    /// Returns the oldest sequence number still in the buffer.
    ///
    /// Returns `None` if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn oldest_seq(&self) -> Option<SeqNum> {
        if self.is_empty() {
            None
        } else {
            Some(SeqNum::from(self.tail))
        }
    }

    /// Returns true if the buffer is empty.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    /// Returns the number of frames currently stored.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.head.wrapping_sub(self.tail) as usize
    }

    /// Returns the buffer capacity.
    #[inline]
    #[must_use]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Pushes a frame into the ring, returning the assigned sequence number.
    ///
    /// If the buffer is full, the oldest entry is evicted (drop-oldest policy).
    /// The frame is moved into the ring.
    ///
    /// # Returns
    ///
    /// The sequence number assigned to this frame.
    pub fn push(&mut self, frame: T) -> SeqNum {
        let seq = self.head;
        let slot_idx = (seq as usize) % N;

        // If buffer is full, evict oldest
        if self.len() == N {
            // Drop the old value if occupied
            let old_slot = &mut self.buffer[slot_idx];
            if old_slot.occupied {
                // SAFETY: slot is occupied, so value is initialized
                unsafe {
                    std::ptr::drop_in_place(old_slot.value.as_mut_ptr());
                }
            }
            self.tail = self.tail.wrapping_add(1);
        }

        // Write new value
        let slot = &mut self.buffer[slot_idx];
        slot.value = MaybeUninit::new(frame);
        slot.seq = seq;
        slot.occupied = true;

        self.head = self.head.wrapping_add(1);
        SeqNum::from(seq)
    }

    /// Looks up a frame by sequence number.
    ///
    /// # Returns
    ///
    /// - `Available(&T)` if the frame is in the buffer
    /// - `Evicted { requested, oldest_available }` if the frame was evicted
    /// - `NotYetSent { requested, next_seq }` if the sequence hasn't been sent yet
    #[must_use]
    pub fn get(&self, seq: SeqNum) -> RetransmitLookup<'_, T> {
        let seq_raw = seq.as_u64();
        // Use wrapping arithmetic for correct behavior across u64 wrap
        // distance_from_tail tells us where seq is relative to our buffer
        let distance_from_tail = seq_raw.wrapping_sub(self.tail);
        let buffer_len = self.len() as u64;

        // If distance >= buffer_len, it's either evicted (past) or not yet sent (future)
        if distance_from_tail >= buffer_len {
            // Distinguish past vs future using half-space comparison:
            // If distance is "small" (less than half of u64 space), it's in the future.
            // If distance is "large" (wrapped around), it's in the past.
            // Since our buffer is tiny compared to u64, anything not in [tail, head)
            // with small distance is future, large distance is past.
            if distance_from_tail <= (u64::MAX / 2) {
                // Sequence is ahead of head (not yet sent)
                return RetransmitLookup::NotYetSent {
                    requested: seq,
                    next_seq: SeqNum::from(self.head),
                };
            } else {
                // Sequence wrapped around far behind tail (evicted)
                return RetransmitLookup::Evicted {
                    requested: seq,
                    oldest_available: SeqNum::from(self.tail),
                };
            }
        }

        // Sequence is in valid range [tail, head), look it up
        let slot_idx = (seq_raw as usize) % N;
        let slot = &self.buffer[slot_idx];

        // Verify the slot contains the expected sequence
        // (handles wrap-around correctness)
        if slot.occupied && slot.seq == seq_raw {
            // SAFETY: slot is occupied and seq matches, so value is valid
            let value = unsafe { slot.value.assume_init_ref() };
            RetransmitLookup::Available(value)
        } else {
            // Slot was overwritten (shouldn't happen if invariants hold)
            RetransmitLookup::Evicted {
                requested: seq,
                oldest_available: SeqNum::from(self.tail),
            }
        }
    }

    /// Checks if a specific sequence number is available for retransmission.
    #[inline]
    #[must_use]
    pub fn contains(&self, seq: SeqNum) -> bool {
        matches!(self.get(seq), RetransmitLookup::Available(_))
    }

    /// Returns the range of available sequences as `[oldest, newest]` inclusive.
    ///
    /// Returns `None` if the buffer is empty.
    #[must_use]
    pub fn available_range(&self) -> Option<(SeqNum, SeqNum)> {
        if self.is_empty() {
            None
        } else {
            Some((
                SeqNum::from(self.tail),
                SeqNum::from(self.head.wrapping_sub(1)),
            ))
        }
    }

    /// Clears all entries from the ring.
    pub fn clear(&mut self) {
        // Drop all occupied slots
        for i in 0..N {
            let slot = &mut self.buffer[i];
            if slot.occupied {
                // SAFETY: slot is occupied, so value is initialized
                unsafe {
                    std::ptr::drop_in_place(slot.value.as_mut_ptr());
                }
                slot.occupied = false;
            }
        }
        self.tail = self.head;
    }
}

impl<T, const N: usize> Default for RetransmitRing<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> Drop for RetransmitRing<T, N> {
    fn drop(&mut self) {
        self.clear();
    }
}

// SAFETY: RetransmitRing is Send if T is Send (no shared references)
unsafe impl<T: Send, const N: usize> Send for RetransmitRing<T, N> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(n: u64) -> SeqNum {
        SeqNum::from(n)
    }

    #[test]
    fn new_ring_is_empty() {
        let ring: RetransmitRing<u64, 8> = RetransmitRing::new();
        assert!(ring.is_empty());
        assert_eq!(ring.len(), 0);
        assert_eq!(ring.next_seq(), seq(0));
        assert_eq!(ring.oldest_seq(), None);
    }

    #[test]
    fn push_increments_sequence() {
        let mut ring: RetransmitRing<u64, 8> = RetransmitRing::new();

        assert_eq!(ring.push(100), seq(0));
        assert_eq!(ring.push(200), seq(1));
        assert_eq!(ring.push(300), seq(2));

        assert_eq!(ring.len(), 3);
        assert_eq!(ring.next_seq(), seq(3));
        assert_eq!(ring.oldest_seq(), Some(seq(0)));
    }

    #[test]
    fn get_returns_pushed_values() {
        let mut ring: RetransmitRing<u64, 8> = RetransmitRing::new();

        ring.push(100);
        ring.push(200);
        ring.push(300);

        assert!(matches!(ring.get(seq(0)), RetransmitLookup::Available(&100)));
        assert!(matches!(ring.get(seq(1)), RetransmitLookup::Available(&200)));
        assert!(matches!(ring.get(seq(2)), RetransmitLookup::Available(&300)));
    }

    #[test]
    fn get_future_seq_returns_not_yet_sent() {
        let mut ring: RetransmitRing<u64, 8> = RetransmitRing::new();
        ring.push(100);

        match ring.get(seq(5)) {
            RetransmitLookup::NotYetSent { requested, next_seq } => {
                assert_eq!(requested, seq(5));
                assert_eq!(next_seq, seq(1));
            }
            _ => panic!("expected NotYetSent"),
        }
    }

    #[test]
    fn eviction_on_full() {
        let mut ring: RetransmitRing<u64, 4> = RetransmitRing::new();

        // Fill the buffer
        ring.push(100); // seq 0
        ring.push(200); // seq 1
        ring.push(300); // seq 2
        ring.push(400); // seq 3

        assert_eq!(ring.len(), 4);
        assert_eq!(ring.oldest_seq(), Some(seq(0)));

        // Push one more, should evict seq 0
        ring.push(500); // seq 4

        assert_eq!(ring.len(), 4);
        assert_eq!(ring.oldest_seq(), Some(seq(1)));

        // seq 0 should be evicted
        match ring.get(seq(0)) {
            RetransmitLookup::Evicted { requested, oldest_available } => {
                assert_eq!(requested, seq(0));
                assert_eq!(oldest_available, seq(1));
            }
            _ => panic!("expected Evicted"),
        }

        // seq 1-4 should still be available
        assert!(matches!(ring.get(seq(1)), RetransmitLookup::Available(&200)));
        assert!(matches!(ring.get(seq(4)), RetransmitLookup::Available(&500)));
    }

    #[test]
    fn with_start_seq() {
        let mut ring: RetransmitRing<u64, 8> = RetransmitRing::with_start_seq(seq(1000));

        assert_eq!(ring.next_seq(), seq(1000));
        assert!(ring.is_empty());

        let assigned = ring.push(42);
        assert_eq!(assigned, seq(1000));
        assert_eq!(ring.next_seq(), seq(1001));
    }

    #[test]
    fn contains() {
        let mut ring: RetransmitRing<u64, 4> = RetransmitRing::new();

        ring.push(100);
        ring.push(200);

        assert!(ring.contains(seq(0)));
        assert!(ring.contains(seq(1)));
        assert!(!ring.contains(seq(2))); // not yet sent
        assert!(!ring.contains(seq(100))); // way in the future
    }

    #[test]
    fn available_range() {
        let mut ring: RetransmitRing<u64, 8> = RetransmitRing::new();

        assert_eq!(ring.available_range(), None);

        ring.push(100);
        assert_eq!(ring.available_range(), Some((seq(0), seq(0))));

        ring.push(200);
        ring.push(300);
        assert_eq!(ring.available_range(), Some((seq(0), seq(2))));
    }

    #[test]
    fn clear() {
        let mut ring: RetransmitRing<u64, 8> = RetransmitRing::new();

        ring.push(100);
        ring.push(200);
        ring.push(300);

        ring.clear();

        assert!(ring.is_empty());
        assert_eq!(ring.len(), 0);
        // next_seq should NOT reset (it's the "high water mark")
        assert_eq!(ring.next_seq(), seq(3));
    }

    #[test]
    fn wraparound_large_sequence() {
        let mut ring: RetransmitRing<u64, 4> = RetransmitRing::with_start_seq(seq(u64::MAX - 2));

        ring.push(100); // seq MAX-2
        ring.push(200); // seq MAX-1
        ring.push(300); // seq MAX (wraps to 0 in slot index)
        ring.push(400); // seq 0 (after wrap)

        assert_eq!(ring.len(), 4);

        // All should be available
        assert!(matches!(ring.get(seq(u64::MAX - 2)), RetransmitLookup::Available(&100)));
        assert!(matches!(ring.get(seq(u64::MAX - 1)), RetransmitLookup::Available(&200)));
        assert!(matches!(ring.get(seq(u64::MAX)), RetransmitLookup::Available(&300)));
        // Note: wrapping_add would give 0, but we use saturating arithmetic
    }

    #[test]
    fn drop_is_called_on_eviction() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        struct DropCounter(Arc<AtomicUsize>);
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let mut ring: RetransmitRing<DropCounter, 2> = RetransmitRing::new();

        ring.push(DropCounter(Arc::clone(&counter)));
        ring.push(DropCounter(Arc::clone(&counter)));
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // This should evict the first entry
        ring.push(DropCounter(Arc::clone(&counter)));
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Clear should drop remaining
        ring.clear();
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    // =========================================================================
    // Invariant Tests (per validation.md §3)
    // =========================================================================

    /// Helper to verify all documented invariants hold.
    ///
    /// Invariants from struct docs:
    /// 1. `head` is the next sequence number to be assigned
    /// 2. `tail` is the oldest sequence still in buffer (or equals head if empty)
    /// 3. `head - tail <= N` (buffer never exceeds capacity)
    /// 4. Slots in range `[tail, head)` are occupied
    fn assert_invariants<T, const N: usize>(ring: &RetransmitRing<T, N>) {
        // Invariant 3: buffer size never exceeds capacity
        let len = ring.len();
        assert!(len <= N, "invariant violated: len {} > capacity {}", len, N);

        // Invariant: head - tail == len
        let computed_len = ring.head.wrapping_sub(ring.tail) as usize;
        assert_eq!(computed_len, len, "invariant violated: head - tail != len");

        // Invariant 2: if empty, head == tail
        if len == 0 {
            assert_eq!(ring.head, ring.tail, "invariant violated: empty but head != tail");
        }

        // Invariant 4: slots in [tail, head) should be occupied with correct seq
        for i in 0..len {
            let seq = ring.tail.wrapping_add(i as u64);
            let slot_idx = (seq as usize) % N;
            let slot = &ring.buffer[slot_idx];
            assert!(slot.occupied, "invariant violated: slot {} should be occupied", seq);
            assert_eq!(slot.seq, seq, "invariant violated: slot seq mismatch");
        }
    }

    #[test]
    fn invariants_hold_after_push() {
        let mut ring: RetransmitRing<u64, 8> = RetransmitRing::new();
        assert_invariants(&ring);

        for i in 0..5 {
            ring.push(i * 100);
            assert_invariants(&ring);
        }
    }

    #[test]
    fn invariants_hold_after_eviction() {
        let mut ring: RetransmitRing<u64, 4> = RetransmitRing::new();

        // Fill to capacity
        for i in 0..4 {
            ring.push(i * 100);
            assert_invariants(&ring);
        }

        // Push more to trigger eviction
        for i in 4..10 {
            ring.push(i * 100);
            assert_invariants(&ring);
        }
    }

    #[test]
    fn invariants_hold_after_clear() {
        let mut ring: RetransmitRing<u64, 8> = RetransmitRing::new();

        ring.push(100);
        ring.push(200);
        ring.push(300);
        assert_invariants(&ring);

        ring.clear();
        assert_invariants(&ring);

        // Can still push after clear
        ring.push(400);
        assert_invariants(&ring);
    }

    #[test]
    fn invariants_hold_with_start_seq() {
        let mut ring: RetransmitRing<u64, 4> = RetransmitRing::with_start_seq(seq(1000));
        assert_invariants(&ring);

        for i in 0..6 {
            ring.push(i * 100);
            assert_invariants(&ring);
        }
    }

    #[test]
    fn invariants_hold_at_wraparound() {
        let mut ring: RetransmitRing<u64, 4> = RetransmitRing::with_start_seq(seq(u64::MAX - 2));
        assert_invariants(&ring);

        // Push across u64 wraparound boundary
        for i in 0..8 {
            ring.push(i * 100);
            assert_invariants(&ring);
        }
    }
}
