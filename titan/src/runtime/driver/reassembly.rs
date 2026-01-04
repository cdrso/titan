//! Fragment reassembly buffer for the RX thread.
//!
//! This module provides a pre-allocated, fixed-size buffer for reassembling
//! fragmented messages. All memory is allocated at initialization time to
//! ensure the hot path is allocation-free.
//!
//! # Design
//!
//! The reassembly buffer uses a ring structure indexed by sequence number,
//! similar to `ReorderBuffer`. Each slot can hold fragments for one message.
//!
//! # Memory Layout
//!
//! ```text
//! ReassemblyRing<MAX_FRAG_PAYLOAD, MAX_FRAGS_PER_MSG, SLOTS>
//! └── entries: Box<[ReassemblyEntry; SLOTS]>
//!     └── fragments: [[u8; MAX_FRAG_PAYLOAD]; MAX_FRAGS_PER_MSG]
//!     └── frag_lens: [u16; MAX_FRAGS_PER_MSG]
//!     └── received_mask: u64
//!     └── expected_count: u16
//!     └── seq: u64
//!     └── active: bool
//! ```

use crate::data::types::SeqNum;
use crate::data::Frame;
use crate::trace::{debug, trace, warn};

/// Maximum fragments per message (limits bitmap to u64).
pub const MAX_FRAGS_PER_MSG: usize = 64;

/// Default maximum fragment payload size (MTU - header overhead).
pub const DEFAULT_MAX_FRAG_PAYLOAD: usize = 1400;

/// Default number of concurrent reassembly slots.
pub const DEFAULT_REASSEMBLY_SLOTS: usize = 64;

/// Result of inserting a fragment into the reassembly buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertResult {
    /// Fragment accepted, message not yet complete.
    Pending,
    /// Fragment accepted and message is now complete.
    Complete,
    /// Duplicate fragment (already received).
    Duplicate,
    /// Fragment too old (sequence evicted or never tracked).
    TooOld,
    /// Fragment metadata inconsistent (frag_count mismatch).
    Invalid,
    /// Buffer full, no slot available for this sequence.
    BufferFull,
}

/// Per-message reassembly state.
///
/// This is a fixed-size structure with no heap allocations.
/// The fragment payload array is sized by const generics.
#[derive(Debug)]
struct ReassemblyEntry<const MAX_FRAG_PAYLOAD: usize, const MAX_FRAGS: usize> {
    /// Fragment payload storage (pre-allocated).
    fragments: [[u8; MAX_FRAG_PAYLOAD]; MAX_FRAGS],
    /// Length of each fragment's payload.
    frag_lens: [u16; MAX_FRAGS],
    /// Bitmap of received fragments (bit N = fragment N received).
    received_mask: u64,
    /// Expected total fragment count (from first fragment).
    expected_count: u16,
    /// Message sequence number.
    seq: u64,
    /// Whether this slot is in use.
    active: bool,
    /// Total payload bytes received so far.
    total_payload_len: usize,
}

impl<const MAX_FRAG_PAYLOAD: usize, const MAX_FRAGS: usize> Default
    for ReassemblyEntry<MAX_FRAG_PAYLOAD, MAX_FRAGS>
{
    fn default() -> Self {
        Self {
            fragments: [[0u8; MAX_FRAG_PAYLOAD]; MAX_FRAGS],
            frag_lens: [0u16; MAX_FRAGS],
            received_mask: 0,
            expected_count: 0,
            seq: 0,
            active: false,
            total_payload_len: 0,
        }
    }
}

impl<const MAX_FRAG_PAYLOAD: usize, const MAX_FRAGS: usize>
    ReassemblyEntry<MAX_FRAG_PAYLOAD, MAX_FRAGS>
{
    /// Clears this entry for reuse.
    fn clear(&mut self) {
        self.received_mask = 0;
        self.expected_count = 0;
        self.seq = 0;
        self.active = false;
        self.total_payload_len = 0;
        // Note: We don't clear fragments/frag_lens arrays - they'll be overwritten
    }

    /// Checks if all fragments have been received.
    fn is_complete(&self) -> bool {
        if !self.active || self.expected_count == 0 {
            return false;
        }
        let expected_mask = (1u64 << self.expected_count) - 1;
        self.received_mask == expected_mask
    }

    /// Returns the number of fragments received so far.
    fn received_count(&self) -> u16 {
        self.received_mask.count_ones() as u16
    }
}

/// Ring-based reassembly buffer with pre-allocated storage.
///
/// All memory is allocated at construction time. The hot-path operations
/// (`insert_fragment`, `try_complete`) perform no heap allocations.
///
/// # Type Parameters
/// - `MAX_FRAG_PAYLOAD`: Maximum bytes per fragment payload
/// - `MAX_FRAGS`: Maximum fragments per message (must be <= 64)
/// - `SLOTS`: Number of concurrent reassembly slots
pub struct ReassemblyRing<
    const MAX_FRAG_PAYLOAD: usize,
    const MAX_FRAGS: usize,
    const SLOTS: usize,
> {
    /// Pre-allocated entry storage.
    entries: Box<[ReassemblyEntry<MAX_FRAG_PAYLOAD, MAX_FRAGS>; SLOTS]>,
    /// Timeout in microseconds (for future eviction support).
    _timeout_micros: u64,
}

impl<const MAX_FRAG_PAYLOAD: usize, const MAX_FRAGS: usize, const SLOTS: usize>
    ReassemblyRing<MAX_FRAG_PAYLOAD, MAX_FRAGS, SLOTS>
{
    /// Creates a new reassembly ring with all entries pre-allocated.
    ///
    /// This is the only allocation point - all subsequent operations are O(1) space.
    #[must_use]
    pub fn new(timeout_micros: u64) -> Self {
        // Allocate all entries at once
        let entries = (0..SLOTS)
            .map(|_| ReassemblyEntry::default())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|_| {
                // This should never happen since we're creating exactly SLOTS elements
                panic!("Failed to create reassembly ring entries")
            });

        Self {
            entries,
            _timeout_micros: timeout_micros,
        }
    }

    /// Gets the slot index for a sequence number.
    #[inline]
    const fn slot_index(seq: u64) -> usize {
        (seq as usize) % SLOTS
    }

    /// Inserts a fragment into the reassembly buffer.
    ///
    /// # Arguments
    /// - `seq`: Message sequence number (same for all fragments of a message)
    /// - `frag_index`: Fragment index (0-based)
    /// - `frag_count`: Total number of fragments in the message
    /// - `payload`: Fragment payload bytes
    ///
    /// # Returns
    /// - `InsertResult::Complete` if this was the last fragment needed
    /// - `InsertResult::Pending` if more fragments are needed
    /// - `InsertResult::Duplicate` if this fragment was already received
    /// - `InsertResult::Invalid` if metadata is inconsistent
    /// - `InsertResult::TooOld` if the sequence is no longer tracked
    /// - `InsertResult::BufferFull` if no slot is available
    pub fn insert_fragment(
        &mut self,
        seq: SeqNum,
        frag_index: u16,
        frag_count: u16,
        payload: &[u8],
    ) -> InsertResult {
        // Validate inputs
        if frag_count == 0 || frag_count as usize > MAX_FRAGS {
            warn!(
                seq = %seq,
                frag_count = frag_count,
                max_frags = MAX_FRAGS,
                "Invalid fragment count"
            );
            return InsertResult::Invalid;
        }

        if frag_index >= frag_count {
            warn!(
                seq = %seq,
                frag_index = frag_index,
                frag_count = frag_count,
                "Fragment index out of range"
            );
            return InsertResult::Invalid;
        }

        if payload.len() > MAX_FRAG_PAYLOAD {
            warn!(
                seq = %seq,
                payload_len = payload.len(),
                max_payload = MAX_FRAG_PAYLOAD,
                "Fragment payload too large"
            );
            return InsertResult::Invalid;
        }

        let seq_val = seq.as_u64();
        let slot_idx = Self::slot_index(seq_val);
        let entry = &mut self.entries[slot_idx];

        // Check if slot is for a different sequence
        if entry.active && entry.seq != seq_val {
            // Slot collision - evict old entry if needed
            debug!(
                old_seq = entry.seq,
                new_seq = seq_val,
                slot = slot_idx,
                "Reassembly slot collision, evicting old entry"
            );
            entry.clear();
        }

        // Initialize slot if not active
        if !entry.active {
            entry.seq = seq_val;
            entry.expected_count = frag_count;
            entry.active = true;
        }

        // Validate frag_count consistency
        if entry.expected_count != frag_count {
            warn!(
                seq = %seq,
                expected = entry.expected_count,
                received = frag_count,
                "Fragment count mismatch"
            );
            return InsertResult::Invalid;
        }

        // Check for duplicate
        let frag_bit = 1u64 << frag_index;
        if entry.received_mask & frag_bit != 0 {
            trace!(seq = %seq, frag_index = frag_index, "Duplicate fragment");
            return InsertResult::Duplicate;
        }

        // Store fragment
        let frag_idx = frag_index as usize;
        entry.fragments[frag_idx][..payload.len()].copy_from_slice(payload);
        entry.frag_lens[frag_idx] = payload.len() as u16;
        entry.received_mask |= frag_bit;
        entry.total_payload_len += payload.len();

        trace!(
            seq = %seq,
            frag_index = frag_index,
            frag_count = frag_count,
            received = entry.received_count(),
            "Fragment inserted"
        );

        // Check if complete
        if entry.is_complete() {
            InsertResult::Complete
        } else {
            InsertResult::Pending
        }
    }

    /// Assembles a complete message into a Frame.
    ///
    /// Call this after `insert_fragment` returns `InsertResult::Complete`.
    /// This copies fragments into the destination frame in order.
    ///
    /// # Arguments
    /// - `seq`: Message sequence number
    /// - `dest`: Destination frame to assemble into
    ///
    /// # Returns
    /// - `Ok(())` if assembly succeeded
    /// - `Err(())` if the message is not complete or doesn't fit
    pub fn assemble_into<const N: usize>(
        &mut self,
        seq: SeqNum,
        dest: &mut Frame<N>,
    ) -> Result<(), ()> {
        let seq_val = seq.as_u64();
        let slot_idx = Self::slot_index(seq_val);
        let entry = &self.entries[slot_idx];

        // Verify this is the right message and it's complete
        if !entry.active || entry.seq != seq_val || !entry.is_complete() {
            return Err(());
        }

        // Check if total payload fits in destination frame
        if entry.total_payload_len > N {
            warn!(
                seq = %seq,
                total_len = entry.total_payload_len,
                frame_cap = N,
                "Assembled message too large for frame"
            );
            return Err(());
        }

        // We need to construct a frame from the fragments
        // Since Frame doesn't expose mutable payload access, we'll build a byte slice
        // and use try_into

        // Collect all fragment payloads into a contiguous buffer
        // This is a limitation - we need a temporary buffer
        // For zero-copy, Frame would need a different API
        let mut assembled = [0u8; N];
        let mut offset = 0;

        for i in 0..entry.expected_count as usize {
            let len = entry.frag_lens[i] as usize;
            if offset + len > N {
                return Err(());
            }
            assembled[offset..offset + len].copy_from_slice(&entry.fragments[i][..len]);
            offset += len;
        }

        // Create frame from assembled bytes
        match Frame::<N>::try_from(&assembled[..offset]) {
            Ok(frame) => {
                *dest = frame;
                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    /// Clears a slot after successful assembly.
    ///
    /// Call this after `assemble_into` succeeds to free the slot for reuse.
    pub fn clear_slot(&mut self, seq: SeqNum) {
        let seq_val = seq.as_u64();
        let slot_idx = Self::slot_index(seq_val);
        let entry = &mut self.entries[slot_idx];

        if entry.active && entry.seq == seq_val {
            entry.clear();
        }
    }

    /// Returns memory usage statistics.
    #[must_use]
    pub const fn memory_usage() -> usize {
        std::mem::size_of::<ReassemblyEntry<MAX_FRAG_PAYLOAD, MAX_FRAGS>>() * SLOTS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Use smaller constants for testing
    const TEST_FRAG_PAYLOAD: usize = 64;
    const TEST_MAX_FRAGS: usize = 8;
    const TEST_SLOTS: usize = 4;

    type TestRing = ReassemblyRing<TEST_FRAG_PAYLOAD, TEST_MAX_FRAGS, TEST_SLOTS>;

    #[test]
    fn single_fragment_message() {
        let mut ring = TestRing::new(1000);

        let payload = b"hello";
        let result = ring.insert_fragment(SeqNum::from(1), 0, 1, payload);
        assert_eq!(result, InsertResult::Complete);

        let mut frame = Frame::<256>::new();
        assert!(ring.assemble_into(SeqNum::from(1), &mut frame).is_ok());
        assert_eq!(frame.payload(), payload);

        ring.clear_slot(SeqNum::from(1));
    }

    #[test]
    fn two_fragment_message() {
        let mut ring = TestRing::new(1000);

        // Insert fragment 0
        let result = ring.insert_fragment(SeqNum::from(1), 0, 2, b"hello");
        assert_eq!(result, InsertResult::Pending);

        // Insert fragment 1
        let result = ring.insert_fragment(SeqNum::from(1), 1, 2, b"world");
        assert_eq!(result, InsertResult::Complete);

        let mut frame = Frame::<256>::new();
        assert!(ring.assemble_into(SeqNum::from(1), &mut frame).is_ok());
        assert_eq!(frame.payload(), b"helloworld");

        ring.clear_slot(SeqNum::from(1));
    }

    #[test]
    fn out_of_order_fragments() {
        let mut ring = TestRing::new(1000);

        // Insert fragment 2 first
        let result = ring.insert_fragment(SeqNum::from(1), 2, 3, b"c");
        assert_eq!(result, InsertResult::Pending);

        // Insert fragment 0
        let result = ring.insert_fragment(SeqNum::from(1), 0, 3, b"a");
        assert_eq!(result, InsertResult::Pending);

        // Insert fragment 1 - completes
        let result = ring.insert_fragment(SeqNum::from(1), 1, 3, b"b");
        assert_eq!(result, InsertResult::Complete);

        let mut frame = Frame::<256>::new();
        assert!(ring.assemble_into(SeqNum::from(1), &mut frame).is_ok());
        assert_eq!(frame.payload(), b"abc");
    }

    #[test]
    fn duplicate_fragment() {
        let mut ring = TestRing::new(1000);

        let result = ring.insert_fragment(SeqNum::from(1), 0, 2, b"hello");
        assert_eq!(result, InsertResult::Pending);

        // Duplicate
        let result = ring.insert_fragment(SeqNum::from(1), 0, 2, b"hello");
        assert_eq!(result, InsertResult::Duplicate);
    }

    #[test]
    fn invalid_frag_count() {
        let mut ring = TestRing::new(1000);

        // Zero frag_count
        let result = ring.insert_fragment(SeqNum::from(1), 0, 0, b"hello");
        assert_eq!(result, InsertResult::Invalid);

        // Too many fragments
        let result = ring.insert_fragment(SeqNum::from(1), 0, 100, b"hello");
        assert_eq!(result, InsertResult::Invalid);
    }

    #[test]
    fn frag_index_out_of_range() {
        let mut ring = TestRing::new(1000);

        let result = ring.insert_fragment(SeqNum::from(1), 5, 3, b"hello");
        assert_eq!(result, InsertResult::Invalid);
    }

    #[test]
    fn frag_count_mismatch() {
        let mut ring = TestRing::new(1000);

        // First fragment says 2 total
        let result = ring.insert_fragment(SeqNum::from(1), 0, 2, b"hello");
        assert_eq!(result, InsertResult::Pending);

        // Second fragment says 3 total - mismatch
        let result = ring.insert_fragment(SeqNum::from(1), 1, 3, b"world");
        assert_eq!(result, InsertResult::Invalid);
    }

    #[test]
    fn slot_collision_evicts() {
        let mut ring = TestRing::new(1000);

        // Sequence 0 goes to slot 0
        ring.insert_fragment(SeqNum::from(0), 0, 2, b"old");

        // Sequence 4 also goes to slot 0 (4 % 4 = 0)
        let result = ring.insert_fragment(SeqNum::from(4), 0, 1, b"new");
        assert_eq!(result, InsertResult::Complete);
    }

    // =========================================================================
    // Fragmentation/Reassembly Invariant Tests (CONTRACT_012)
    // =========================================================================

    #[test]
    fn large_payload_fragmented_and_reassembled() {
        // Simulate a 200-byte payload split into 4 fragments of 50 bytes each
        let mut ring = TestRing::new(1000);

        // Original payload
        let original: Vec<u8> = (0u8..200).collect();

        // Fragment into 4 pieces of 50 bytes
        let fragments: Vec<&[u8]> = original.chunks(50).collect();
        assert_eq!(fragments.len(), 4);

        // Insert all fragments
        for (idx, frag) in fragments.iter().enumerate() {
            let result = ring.insert_fragment(
                SeqNum::from(1),
                idx as u16,
                4,
                frag,
            );
            if idx == 3 {
                assert_eq!(result, InsertResult::Complete);
            } else {
                assert_eq!(result, InsertResult::Pending);
            }
        }

        // Assemble and verify
        let mut frame = Frame::<256>::new();
        assert!(ring.assemble_into(SeqNum::from(1), &mut frame).is_ok());
        assert_eq!(frame.payload(), &original[..]);
    }

    #[test]
    fn reassembly_order_independent() {
        // Insert fragments in reverse order
        let mut ring = TestRing::new(1000);

        let original = b"ABCDEFGHIJ"; // 10 bytes, 2 fragments of 5

        // Insert fragment 1 first (second half)
        let result = ring.insert_fragment(SeqNum::from(42), 1, 2, &original[5..10]);
        assert_eq!(result, InsertResult::Pending);

        // Insert fragment 0 second (first half)
        let result = ring.insert_fragment(SeqNum::from(42), 0, 2, &original[0..5]);
        assert_eq!(result, InsertResult::Complete);

        // Assembly should produce correct order
        let mut frame = Frame::<256>::new();
        assert!(ring.assemble_into(SeqNum::from(42), &mut frame).is_ok());
        assert_eq!(frame.payload(), original);
    }

    #[test]
    fn reassembly_byte_integrity() {
        // Test with specific byte patterns to catch off-by-one errors
        let mut ring = TestRing::new(1000);

        // Payload with sentinel values at boundaries
        let frag0 = [0xAA, 0x01, 0x02, 0xBB]; // Start sentinel, end marker
        let frag1 = [0xCC, 0x03, 0x04, 0xDD]; // Middle markers
        let frag2 = [0xEE, 0x05, 0x06, 0xFF]; // End sentinel

        ring.insert_fragment(SeqNum::from(1), 0, 3, &frag0);
        ring.insert_fragment(SeqNum::from(1), 1, 3, &frag1);
        let result = ring.insert_fragment(SeqNum::from(1), 2, 3, &frag2);
        assert_eq!(result, InsertResult::Complete);

        let mut frame = Frame::<256>::new();
        ring.assemble_into(SeqNum::from(1), &mut frame).unwrap();

        let expected = [0xAA, 0x01, 0x02, 0xBB, 0xCC, 0x03, 0x04, 0xDD, 0xEE, 0x05, 0x06, 0xFF];
        assert_eq!(frame.payload(), &expected);
    }

    #[test]
    fn concurrent_reassembly_different_sequences() {
        // Interleave fragments from two different messages
        let mut ring = TestRing::new(1000);

        // Message seq=1: "hello"
        // Message seq=2: "world"

        ring.insert_fragment(SeqNum::from(1), 0, 2, b"hel");
        ring.insert_fragment(SeqNum::from(2), 0, 2, b"wor");
        ring.insert_fragment(SeqNum::from(1), 1, 2, b"lo");
        let result = ring.insert_fragment(SeqNum::from(2), 1, 2, b"ld");
        assert_eq!(result, InsertResult::Complete);

        // Both messages should be complete
        let mut frame1 = Frame::<256>::new();
        let mut frame2 = Frame::<256>::new();

        assert!(ring.assemble_into(SeqNum::from(1), &mut frame1).is_ok());
        assert!(ring.assemble_into(SeqNum::from(2), &mut frame2).is_ok());

        assert_eq!(frame1.payload(), b"hello");
        assert_eq!(frame2.payload(), b"world");
    }

    #[test]
    fn assemble_incomplete_fails() {
        let mut ring = TestRing::new(1000);

        // Only insert first fragment of a 3-fragment message
        ring.insert_fragment(SeqNum::from(1), 0, 3, b"part1");

        let mut frame = Frame::<256>::new();
        assert!(ring.assemble_into(SeqNum::from(1), &mut frame).is_err());
    }

    #[test]
    fn assemble_wrong_sequence_fails() {
        let mut ring = TestRing::new(1000);

        // Complete message at seq=1
        ring.insert_fragment(SeqNum::from(1), 0, 1, b"data");

        // Try to assemble seq=2 (wrong)
        let mut frame = Frame::<256>::new();
        assert!(ring.assemble_into(SeqNum::from(2), &mut frame).is_err());
    }

    #[test]
    fn clear_slot_allows_reuse() {
        let mut ring = TestRing::new(1000);

        // Complete first message
        ring.insert_fragment(SeqNum::from(1), 0, 1, b"first");
        let mut frame = Frame::<256>::new();
        ring.assemble_into(SeqNum::from(1), &mut frame).unwrap();
        ring.clear_slot(SeqNum::from(1));

        // Now seq=1 slot should accept new data for a different sequence
        // (slot 1 % 4 = 1, so seq=5 would collide, but seq=1 is now cleared)
        let result = ring.insert_fragment(SeqNum::from(1), 0, 1, b"second");
        assert_eq!(result, InsertResult::Complete);

        ring.assemble_into(SeqNum::from(1), &mut frame).unwrap();
        assert_eq!(frame.payload(), b"second");
    }

    #[test]
    fn variable_fragment_sizes() {
        // Test with fragments of different sizes (common in real fragmentation)
        let mut ring = TestRing::new(1000);

        // First fragment: 60 bytes
        let frag0: Vec<u8> = (0..60).collect();
        // Second fragment: 40 bytes
        let frag1: Vec<u8> = (60..100).collect();
        // Third fragment: 20 bytes (remainder)
        let frag2: Vec<u8> = (100..120).collect();

        ring.insert_fragment(SeqNum::from(1), 0, 3, &frag0);
        ring.insert_fragment(SeqNum::from(1), 1, 3, &frag1);
        let result = ring.insert_fragment(SeqNum::from(1), 2, 3, &frag2);
        assert_eq!(result, InsertResult::Complete);

        let mut frame = Frame::<256>::new();
        ring.assemble_into(SeqNum::from(1), &mut frame).unwrap();

        let expected: Vec<u8> = (0..120).collect();
        assert_eq!(frame.payload(), &expected[..]);
    }

    #[test]
    fn payload_too_large_for_frame() {
        // Test that reassembly fails if assembled size exceeds Frame capacity
        type SmallFrameRing = ReassemblyRing<TEST_FRAG_PAYLOAD, TEST_MAX_FRAGS, TEST_SLOTS>;
        let mut ring = SmallFrameRing::new(1000);

        // Create payload that will exceed a 32-byte frame
        let frag0: Vec<u8> = (0..60).collect();
        let frag1: Vec<u8> = (60..120).collect();

        ring.insert_fragment(SeqNum::from(1), 0, 2, &frag0);
        let result = ring.insert_fragment(SeqNum::from(1), 1, 2, &frag1);
        assert_eq!(result, InsertResult::Complete);

        // Try to assemble into a frame that's too small
        let mut small_frame = Frame::<32>::new();
        assert!(ring.assemble_into(SeqNum::from(1), &mut small_frame).is_err());

        // But it should work with a larger frame
        let mut big_frame = Frame::<256>::new();
        assert!(ring.assemble_into(SeqNum::from(1), &mut big_frame).is_ok());
    }
}
