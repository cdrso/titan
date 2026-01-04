//! Reorder buffer for NAK-based reliability.
//!
//! The `ReorderBuffer` stores out-of-order packets received by a subscriber,
//! tracks gaps, and yields frames in-order when gaps are filled or accepted.
//!
//! # Design
//!
//! - Fixed-size circular buffer indexed by sequence number
//! - Tracks `next_expected` (next sequence to deliver to application)
//! - Tracks `highest_received` (highest sequence seen, for gap detection)
//! - Gaps between `next_expected` and received sequence trigger NAK generation
//! - Frames are delivered in-order via `drain_ready()`
//!
//! # NAK Bitmap Generation
//!
//! When gaps are detected, the buffer can generate NAK bitmap entries using
//! [`NakBitmapEntry`] from the protocol module:
//! - Each entry is `(base_seq, u64)` where bit=1 means received, bit=0 means missing
//! - Multiple entries cover larger gaps
//!
//! # Example
//!
//! ```ignore
//! let mut reorder: ReorderBuffer<Frame<1500>, 256> = ReorderBuffer::new();
//!
//! // Receive packets (possibly out of order)
//! match reorder.insert(seq, frame) {
//!     InsertResult::Accepted => { /* frame buffered */ }
//!     InsertResult::Duplicate => { /* already have it */ }
//!     InsertResult::TooOld => { /* before our window */ }
//!     InsertResult::TooNew => { /* beyond our capacity */ }
//! }
//!
//! // Drain ready (in-order) frames
//! while let Some((seq, frame)) = reorder.pop_ready() {
//!     // deliver to application
//! }
//!
//! // Check for gaps that need NAKs
//! for entry in reorder.nak_entries() {
//!     // send NAK_BITMAP
//! }
//! ```

use std::mem::MaybeUninit;

use crate::data::types::SeqNum;

// Re-export NakBitmapEntry from protocol to avoid duplication (DRY principle)
pub use super::protocol::NakBitmapEntry;

/// Result of inserting a frame into the reorder buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertResult {
    /// Frame was accepted and buffered.
    Accepted,
    /// Frame is a duplicate (already received).
    Duplicate,
    /// Frame is too old (before our window).
    TooOld,
    /// Frame is too far ahead (beyond buffer capacity).
    TooNew,
}

/// A slot in the reorder buffer.
struct Slot<T> {
    /// The stored frame (valid when occupied).
    value: MaybeUninit<T>,
    /// Sequence number of this slot.
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

/// Reorder buffer for reassembling out-of-order packets.
///
/// # Type Parameters
///
/// - `T`: The frame type (e.g., `Frame<1500>`)
/// - `N`: Buffer capacity. Should be power of 2 for efficient indexing.
///
/// # Invariants
///
/// - `next_expected` is the next sequence to deliver to the application
/// - `highest_received >= next_expected` (or both 0 if no data received)
/// - Buffer holds sequences in range `[next_expected, next_expected + N)`
pub struct ReorderBuffer<T, const N: usize> {
    /// Ring buffer slots.
    buffer: Box<[Slot<T>; N]>,
    /// Next sequence expected to deliver (low water mark).
    next_expected: u64,
    /// Highest sequence received (high water mark for gap detection).
    highest_received: u64,
    /// Number of occupied slots.
    count: usize,
}

impl<T, const N: usize> ReorderBuffer<T, N> {
    /// Creates a new empty reorder buffer.
    ///
    /// # Panics
    ///
    /// Panics if `N` is 0.
    #[must_use]
    pub fn new() -> Self {
        assert!(N > 0, "ReorderBuffer capacity must be > 0");

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
            next_expected: 0,
            highest_received: 0,
            count: 0,
        }
    }

    /// Creates a new reorder buffer starting at a specific sequence number.
    ///
    /// Useful when joining an existing stream mid-flight.
    #[must_use]
    pub fn with_start_seq(start_seq: SeqNum) -> Self {
        let mut buf = Self::new();
        let seq_raw = start_seq.as_u64();
        buf.next_expected = seq_raw;
        buf.highest_received = seq_raw;
        buf
    }

    /// Returns the next expected sequence number.
    #[inline]
    #[must_use]
    pub fn next_expected(&self) -> SeqNum {
        SeqNum::from(self.next_expected)
    }

    /// Returns the highest sequence number received.
    #[inline]
    #[must_use]
    pub fn highest_received(&self) -> SeqNum {
        SeqNum::from(self.highest_received)
    }

    /// Returns the number of buffered frames.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Returns true if no frames are buffered.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the buffer capacity.
    #[inline]
    #[must_use]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Returns true if there are gaps (missing sequences before highest_received).
    #[inline]
    #[must_use]
    pub fn has_gaps(&self) -> bool {
        if self.count == 0 {
            return false;
        }
        // Span is the inclusive range [next_expected, highest_received]
        // +1 because both endpoints are included
        let span = self.highest_received.wrapping_sub(self.next_expected).wrapping_add(1);
        span as usize > self.count
    }

    /// Inserts a frame into the buffer.
    ///
    /// # Returns
    ///
    /// - `Accepted` if the frame was buffered
    /// - `Duplicate` if the sequence was already received
    /// - `TooOld` if the sequence is before our window
    /// - `TooNew` if the sequence is beyond our capacity
    pub fn insert(&mut self, seq: SeqNum, frame: T) -> InsertResult {
        let seq_raw = seq.as_u64();
        // Check if sequence is before our window
        let distance = seq_raw.wrapping_sub(self.next_expected);

        // If distance is huge (wrapped around), it's in the past
        if distance > (u64::MAX / 2) {
            return InsertResult::TooOld;
        }

        // If distance exceeds capacity, it's too far ahead
        if distance as usize >= N {
            return InsertResult::TooNew;
        }

        let slot_idx = (seq_raw as usize) % N;
        let slot = &mut self.buffer[slot_idx];

        // Check for duplicate
        if slot.occupied && slot.seq == seq_raw {
            return InsertResult::Duplicate;
        }

        // If slot is occupied with different seq, it's being reused
        // (this shouldn't happen if invariants hold, but handle gracefully)
        if slot.occupied {
            // SAFETY: We just verified `slot.occupied == true`, which is only set
            // after writing a valid value via `MaybeUninit::new()`. Therefore
            // `slot.value` contains an initialized `T` that must be dropped.
            unsafe {
                std::ptr::drop_in_place(slot.value.as_mut_ptr());
            }
            self.count -= 1;
        }

        // Insert the frame
        slot.value = MaybeUninit::new(frame);
        slot.seq = seq_raw;
        slot.occupied = true;
        self.count += 1;

        // Update highest_received
        if seq_raw.wrapping_sub(self.highest_received) <= (u64::MAX / 2)
            && seq_raw > self.highest_received
        {
            self.highest_received = seq_raw;
        } else if self.count == 1 {
            // First frame, set highest_received
            self.highest_received = seq_raw;
        }

        InsertResult::Accepted
    }

    /// Pops the next ready (in-order) frame if available.
    ///
    /// Returns the sequence number and frame if `next_expected` is in the buffer.
    pub fn pop_ready(&mut self) -> Option<(SeqNum, T)> {
        let slot_idx = (self.next_expected as usize) % N;
        let slot = &mut self.buffer[slot_idx];

        if slot.occupied && slot.seq == self.next_expected {
            // SAFETY: We just verified `slot.occupied == true` AND `slot.seq == self.next_expected`.
            // The `occupied` flag is only set after `MaybeUninit::new()`, so the value is initialized.
            // The seq check ensures this is the correct slot (not a wraparound collision).
            let value = unsafe { slot.value.assume_init_read() };
            slot.occupied = false;
            self.count -= 1;

            let seq = SeqNum::from(self.next_expected);
            self.next_expected = self.next_expected.wrapping_add(1);

            Some((seq, value))
        } else {
            None
        }
    }

    /// Drains all contiguous ready frames into the provided closure.
    ///
    /// Calls `f(seq, frame)` for each frame in sequence order.
    /// Stops at the first gap.
    pub fn drain_ready<F>(&mut self, mut f: F)
    where
        F: FnMut(SeqNum, T),
    {
        while let Some((seq, frame)) = self.pop_ready() {
            f(seq, frame);
        }
    }

    /// Accepts a gap in the sequence, advancing `next_expected` past missing data.
    ///
    /// Use this when DATA_NOT_AVAIL indicates the data will never arrive.
    ///
    /// # Arguments
    ///
    /// * `up_to` - Accept gap up to (but not including) this sequence.
    pub fn accept_gap(&mut self, up_to: SeqNum) {
        let up_to_raw = up_to.as_u64();
        let distance = up_to_raw.wrapping_sub(self.next_expected);

        // Only advance if up_to is ahead
        if distance <= (u64::MAX / 2) {
            // Clear any slots in the gap range
            for seq in self.next_expected..up_to_raw {
                let slot_idx = (seq as usize) % N;
                let slot = &mut self.buffer[slot_idx];
                if slot.occupied && slot.seq == seq {
                    // SAFETY: We just verified `slot.occupied == true` AND `slot.seq == seq`.
                    // The `occupied` flag is only set after `MaybeUninit::new()`, so the value
                    // is initialized and must be dropped before we mark the slot as unoccupied.
                    unsafe {
                        std::ptr::drop_in_place(slot.value.as_mut_ptr());
                    }
                    slot.occupied = false;
                    self.count -= 1;
                }
            }
            self.next_expected = up_to_raw;
        }
    }

    /// Generates NAK bitmap entries for missing sequences.
    ///
    /// Returns entries covering the range `[next_expected, highest_received]`.
    /// Each entry covers 64 sequences.
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of entries to return.
    #[must_use]
    pub fn nak_entries(&self, max_entries: usize) -> Vec<NakBitmapEntry> {
        if !self.has_gaps() {
            return Vec::new();
        }

        let mut entries = Vec::new();
        let mut base = self.next_expected;

        while base <= self.highest_received && entries.len() < max_entries {
            let mut bitmap = 0u64;

            for bit in 0..64 {
                let seq = base.wrapping_add(bit);
                if seq > self.highest_received {
                    // Past the end, mark as "not expected"
                    // (set bit to avoid NAKing)
                    bitmap |= 1u64 << bit;
                } else {
                    let slot_idx = (seq as usize) % N;
                    let slot = &self.buffer[slot_idx];
                    if slot.occupied && slot.seq == seq {
                        bitmap |= 1u64 << bit;
                    }
                    // else: bit stays 0 (missing)
                }
            }

            // Only add entry if there are missing sequences
            if bitmap != u64::MAX {
                entries.push(NakBitmapEntry {
                    base_seq: base,
                    bitmap,
                });
            }

            base = base.wrapping_add(64);
        }

        entries
    }

    /// Clears all buffered frames.
    pub fn clear(&mut self) {
        for slot in self.buffer.iter_mut() {
            if slot.occupied {
                // SAFETY: We just verified `slot.occupied == true`, which is only set
                // after writing a valid value via `MaybeUninit::new()`. Therefore
                // `slot.value` contains an initialized `T` that must be dropped.
                unsafe {
                    std::ptr::drop_in_place(slot.value.as_mut_ptr());
                }
                slot.occupied = false;
            }
        }
        self.count = 0;
        // Keep next_expected and highest_received as-is (or reset them)
    }
}

impl<T, const N: usize> Default for ReorderBuffer<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> Drop for ReorderBuffer<T, N> {
    fn drop(&mut self) {
        self.clear();
    }
}

// SAFETY: ReorderBuffer is Send if T is Send
unsafe impl<T: Send, const N: usize> Send for ReorderBuffer<T, N> {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to construct SeqNum from u64 in tests.
    fn seq(n: u64) -> SeqNum {
        SeqNum::from(n)
    }

    #[test]
    fn new_buffer_is_empty() {
        let buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.next_expected(), seq(0));
        assert!(!buf.has_gaps());
    }

    #[test]
    fn insert_in_order() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        assert_eq!(buf.insert(seq(0), 100), InsertResult::Accepted);
        assert_eq!(buf.insert(seq(1), 200), InsertResult::Accepted);
        assert_eq!(buf.insert(seq(2), 300), InsertResult::Accepted);

        assert_eq!(buf.len(), 3);
        assert!(!buf.has_gaps());

        // Pop in order
        assert_eq!(buf.pop_ready(), Some((seq(0), 100)));
        assert_eq!(buf.pop_ready(), Some((seq(1), 200)));
        assert_eq!(buf.pop_ready(), Some((seq(2), 300)));
        assert_eq!(buf.pop_ready(), None);
    }

    #[test]
    fn insert_out_of_order() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        // Skip seq 0, insert 1 and 2
        assert_eq!(buf.insert(seq(1), 200), InsertResult::Accepted);
        assert_eq!(buf.insert(seq(2), 300), InsertResult::Accepted);

        assert_eq!(buf.len(), 2);
        assert!(buf.has_gaps()); // Missing seq 0

        // Can't pop yet (seq 0 missing)
        assert_eq!(buf.pop_ready(), None);

        // Now insert seq 0
        assert_eq!(buf.insert(seq(0), 100), InsertResult::Accepted);
        assert!(!buf.has_gaps());

        // Now we can pop all
        assert_eq!(buf.pop_ready(), Some((seq(0), 100)));
        assert_eq!(buf.pop_ready(), Some((seq(1), 200)));
        assert_eq!(buf.pop_ready(), Some((seq(2), 300)));
    }

    #[test]
    fn duplicate_detection() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        assert_eq!(buf.insert(seq(0), 100), InsertResult::Accepted);
        assert_eq!(buf.insert(seq(0), 100), InsertResult::Duplicate);
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn too_old_detection() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::with_start_seq(seq(10));

        // Sequence 5 is before our window
        assert_eq!(buf.insert(seq(5), 500), InsertResult::TooOld);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn too_new_detection() {
        let mut buf: ReorderBuffer<u64, 4> = ReorderBuffer::new();

        // Sequence 10 is beyond capacity (buffer size 4)
        assert_eq!(buf.insert(seq(10), 1000), InsertResult::TooNew);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn nak_entries_generation() {
        let mut buf: ReorderBuffer<u64, 16> = ReorderBuffer::new();

        // Insert some but skip others
        buf.insert(seq(0), 0);
        buf.insert(seq(2), 2); // gap at 1
        buf.insert(seq(5), 5); // gap at 3, 4

        assert!(buf.has_gaps());

        let entries = buf.nak_entries(10);
        assert_eq!(entries.len(), 1);

        let entry = &entries[0];
        assert_eq!(entry.base_seq, 0);

        // Check specific bits
        // seq 0: received (bit 0 = 1)
        // seq 1: missing (bit 1 = 0)
        // seq 2: received (bit 2 = 1)
        // seq 3: missing (bit 3 = 0)
        // seq 4: missing (bit 4 = 0)
        // seq 5: received (bit 5 = 1)
        // seq 6+: beyond highest_received, marked as received to avoid false NAK
        let expected_low_bits = 0b100101; // bits 0,2,5 set
        assert_eq!(entry.bitmap & 0b111111, expected_low_bits);
    }

    #[test]
    fn accept_gap() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        // Insert seq 5 (big gap)
        buf.insert(seq(5), 500);
        assert!(buf.has_gaps());
        assert_eq!(buf.pop_ready(), None);

        // Accept gap up to seq 5
        buf.accept_gap(seq(5));
        assert_eq!(buf.next_expected(), seq(5));

        // Now seq 5 is ready
        assert_eq!(buf.pop_ready(), Some((seq(5), 500)));
    }

    #[test]
    fn drain_ready() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        buf.insert(seq(0), 100);
        buf.insert(seq(1), 200);
        buf.insert(seq(2), 300);
        buf.insert(seq(4), 500); // gap at 3

        let mut drained = Vec::new();
        buf.drain_ready(|s, val| drained.push((s, val)));

        // Should stop at gap
        assert_eq!(drained, vec![(seq(0), 100), (seq(1), 200), (seq(2), 300)]);
        assert_eq!(buf.len(), 1); // seq 4 still buffered
    }

    #[test]
    fn nak_bitmap_entry_helpers() {
        let entry = NakBitmapEntry {
            base_seq: 100,
            bitmap: 0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11110101,
        };

        assert!(entry.has_missing());
        assert_eq!(entry.missing_count(), 2); // bits 1 and 3 are 0

        let missing: Vec<_> = entry.missing_sequences().collect();
        assert_eq!(missing, vec![101, 103]);
    }

    #[test]
    fn with_start_seq() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::with_start_seq(seq(1000));

        assert_eq!(buf.next_expected(), seq(1000));
        assert_eq!(buf.insert(seq(1000), 42), InsertResult::Accepted);
        assert_eq!(buf.pop_ready(), Some((seq(1000), 42)));
        assert_eq!(buf.next_expected(), seq(1001));
    }

    #[test]
    fn drop_is_called() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        struct DropCounter(Arc<AtomicUsize>);
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counter = Arc::new(AtomicUsize::new(0));
        {
            let mut buf: ReorderBuffer<DropCounter, 4> = ReorderBuffer::new();
            buf.insert(seq(0), DropCounter(Arc::clone(&counter)));
            buf.insert(seq(1), DropCounter(Arc::clone(&counter)));
            assert_eq!(counter.load(Ordering::SeqCst), 0);
            // Drop buf
        }
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    // =========================================================================
    // Invariant Tests (per validation.md §3)
    // =========================================================================

    /// Helper to verify all documented invariants hold.
    ///
    /// Invariants from struct docs:
    /// 1. `next_expected` is the next sequence to deliver to the application
    /// 2. `highest_received >= next_expected` (or both equal if no gaps)
    /// 3. Buffer holds sequences in range `[next_expected, next_expected + N)`
    /// 4. `count` matches actual occupied slots
    fn assert_invariants<T, const N: usize>(buf: &ReorderBuffer<T, N>) {
        let next_exp = buf.next_expected;
        let highest = buf.highest_received;
        let count = buf.count;

        // Invariant: count <= N
        assert!(count <= N, "invariant violated: count {} > capacity {}", count, N);

        // Invariant 2: highest_received >= next_expected (using half-space for wraparound)
        // If count > 0, highest should be >= next_expected
        if count > 0 {
            let diff = highest.wrapping_sub(next_exp);
            assert!(
                diff <= (u64::MAX / 2),
                "invariant violated: highest_received {} < next_expected {}",
                highest,
                next_exp
            );
        }

        // Invariant 4: count matches actual occupied slots
        let mut actual_count = 0;
        for slot in buf.buffer.iter() {
            if slot.occupied {
                actual_count += 1;

                // Verify occupied slot is in valid range [next_expected, next_expected + N)
                let seq = slot.seq;
                let dist = seq.wrapping_sub(next_exp);
                assert!(
                    dist < N as u64,
                    "invariant violated: occupied slot seq {} outside valid range",
                    seq
                );
            }
        }
        assert_eq!(
            actual_count, count,
            "invariant violated: count {} != actual occupied {}",
            count, actual_count
        );
    }

    #[test]
    fn invariants_hold_after_insert_in_order() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();
        assert_invariants(&buf);

        for i in 0..5 {
            buf.insert(seq(i), i * 100);
            assert_invariants(&buf);
        }
    }

    #[test]
    fn invariants_hold_after_insert_out_of_order() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        // Insert out of order: 2, 0, 4, 1, 3
        buf.insert(seq(2), 200);
        assert_invariants(&buf);
        buf.insert(seq(0), 0);
        assert_invariants(&buf);
        buf.insert(seq(4), 400);
        assert_invariants(&buf);
        buf.insert(seq(1), 100);
        assert_invariants(&buf);
        buf.insert(seq(3), 300);
        assert_invariants(&buf);
    }

    #[test]
    fn invariants_hold_after_pop() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        buf.insert(seq(0), 0);
        buf.insert(seq(1), 100);
        buf.insert(seq(2), 200);
        assert_invariants(&buf);

        buf.pop_ready();
        assert_invariants(&buf);
        buf.pop_ready();
        assert_invariants(&buf);
        buf.pop_ready();
        assert_invariants(&buf);
    }

    #[test]
    fn invariants_hold_after_accept_gap() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        // Insert seq 5 (creates gap 0-4)
        buf.insert(seq(5), 500);
        assert_invariants(&buf);

        // Accept gap up to 5
        buf.accept_gap(seq(5));
        assert_invariants(&buf);

        // Now seq 5 should be poppable
        buf.pop_ready();
        assert_invariants(&buf);
    }

    #[test]
    fn invariants_hold_after_drain_ready() {
        let mut buf: ReorderBuffer<u64, 8> = ReorderBuffer::new();

        buf.insert(seq(0), 0);
        buf.insert(seq(1), 100);
        buf.insert(seq(2), 200);
        buf.insert(seq(4), 400); // gap at 3

        buf.drain_ready(|_, _| {});
        assert_invariants(&buf);

        // Insert missing seq 3
        buf.insert(seq(3), 300);
        assert_invariants(&buf);

        buf.drain_ready(|_, _| {});
        assert_invariants(&buf);
    }

    #[test]
    fn invariants_hold_with_start_seq() {
        let mut buf: ReorderBuffer<u64, 4> = ReorderBuffer::with_start_seq(seq(1000));
        assert_invariants(&buf);

        buf.insert(seq(1000), 0);
        assert_invariants(&buf);
        buf.insert(seq(1002), 200); // gap at 1001
        assert_invariants(&buf);
        buf.insert(seq(1001), 100);
        assert_invariants(&buf);
    }
}
