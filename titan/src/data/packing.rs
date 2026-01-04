//! MTU-aware message packing for high-throughput UDP transport.
//!
//! # Problem
//! Linux kernel limits loopback UDP to ~200K packets/second.
//! Sending 1 message per packet caps throughput at 200K msg/s.
//!
//! # Solution
//! Pack multiple messages into MTU-sized packets (8KB default).
//! 256 messages × 32 bytes = 8KB → 200K packets/s × 256 = 51M msg/s
//!
//! # Adaptive Batching (CONTRACT_026)
//! Instead of timeout-based flushing, we use load-adaptive batching:
//! - **Idle** (no more messages pending): Flush immediately → low latency
//! - **Loaded** (more messages pending): Keep batching → high throughput
//!
//! This eliminates the latency floor imposed by fixed timeouts while
//! maintaining maximum throughput under load. Self-tuning, zero config.
//!
//! # Design
//! - `PackingBuffer`: Accumulates encoded frames, flushes adaptively
//! - `UnpackingIter`: Iterates over frames in a received packet

/// Default MTU size (8KB like Aeron)
pub const DEFAULT_MTU: usize = 8192;

/// Typical frame size for "buffer nearly full" heuristic.
/// DATA frame = 26 byte header + ~32 byte payload = ~58 bytes.
const TYPICAL_FRAME_SIZE: usize = 64;

/// Accumulates encoded frames into MTU-sized packets with adaptive flushing.
///
/// # Adaptive Batching
/// The caller signals whether more messages are pending. This enables:
/// - **Idle mode**: No more pending → flush immediately (microsecond latency)
/// - **Batch mode**: More pending → wait for buffer to fill (max throughput)
///
/// # Usage
/// ```ignore
/// let mut packer = PackingBuffer::new(8192);
///
/// // TX loop with adaptive batching
/// loop {
///     while let Some(msg) = channel.try_recv() {
///         let frame = encode(&msg);
///         if !packer.can_fit(frame.len()) {
///             socket.send(packer.flush().unwrap());
///             packer.clear();
///         }
///         packer.push(&frame);
///     }
///     // Channel empty - flush immediately (idle mode)
///     if let Some(data) = packer.flush() {
///         socket.send(data);
///         packer.clear();
///     }
/// }
/// ```
pub struct PackingBuffer {
    /// The buffer accumulating frames
    buffer: Vec<u8>,
    /// Maximum packet size (MTU)
    mtu: usize,
    /// Number of frames in current buffer
    frame_count: usize,
}

impl PackingBuffer {
    /// Create a new packing buffer with specified MTU.
    #[inline]
    pub fn new(mtu: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(mtu),
            mtu,
            frame_count: 0,
        }
    }

    /// Returns the MTU (maximum packet size).
    #[inline]
    pub const fn mtu(&self) -> usize {
        self.mtu
    }

    /// Returns the current buffer length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns true if the buffer is full (can't fit a typical frame).
    #[inline]
    pub fn is_full(&self) -> bool {
        self.remaining() < TYPICAL_FRAME_SIZE
    }

    /// Returns the number of frames in the buffer.
    #[inline]
    pub fn frame_count(&self) -> usize {
        self.frame_count
    }

    /// Returns remaining capacity before MTU is reached.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.mtu.saturating_sub(self.buffer.len())
    }

    /// Check if a frame of given size would fit.
    #[inline]
    pub fn can_fit(&self, frame_len: usize) -> bool {
        self.buffer.len() + frame_len <= self.mtu
    }

    /// Push an encoded frame into the buffer.
    ///
    /// Returns `true` if the frame was added, `false` if it doesn't fit.
    /// Caller should flush first if this returns false.
    #[inline]
    pub fn push(&mut self, frame: &[u8]) -> bool {
        if !self.can_fit(frame.len()) {
            return false;
        }
        self.buffer.extend_from_slice(frame);
        self.frame_count += 1;
        true
    }

    /// Adaptive flush decision based on load.
    ///
    /// # Arguments
    /// * `more_pending` - true if caller knows more messages are waiting
    ///
    /// # Returns
    /// * `true` if buffer should be flushed now
    ///
    /// # Behavior
    /// - If buffer empty: never flush (nothing to send)
    /// - If `more_pending == false`: flush immediately (idle mode, low latency)
    /// - If `more_pending == true`: flush only if buffer is nearly full (batch mode)
    ///
    /// # Example
    /// ```ignore
    /// // After draining channel
    /// let more_pending = !channel.is_empty();
    /// if packer.should_flush(more_pending) {
    ///     socket.send(packer.flush().unwrap());
    ///     packer.clear();
    /// }
    /// ```
    #[inline]
    pub fn should_flush(&self, more_pending: bool) -> bool {
        if self.is_empty() {
            return false;
        }
        // Idle: nothing more coming, send what we have NOW for lowest latency
        if !more_pending {
            return true;
        }
        // Loaded: only flush if buffer is nearly full
        self.is_full()
    }

    /// Flush the buffer, returning the packed packet.
    /// Returns `None` if buffer is empty.
    #[inline]
    pub fn flush(&mut self) -> Option<&[u8]> {
        if self.buffer.is_empty() {
            return None;
        }
        Some(self.buffer.as_slice())
    }

    /// Clear the buffer after sending.
    #[inline]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.frame_count = 0;
    }

    /// Flush and clear in one operation, returning owned data.
    #[inline]
    pub fn take(&mut self) -> Option<Vec<u8>> {
        if self.buffer.is_empty() {
            return None;
        }
        let data = std::mem::take(&mut self.buffer);
        self.buffer = Vec::with_capacity(self.mtu);
        self.frame_count = 0;
        Some(data)
    }
}

/// Iterator over frames packed in a received UDP packet.
///
/// # Wire Format
/// Each frame is: `[tag:4][channel:4][seq:8][timestamp:8][len:2][payload:len]`
/// Frames are concatenated with no padding.
///
/// # Usage
/// ```ignore
/// let packet = socket.recv(&mut buf)?;
/// for frame_bytes in UnpackingIter::new(packet) {
///     let msg = decode_transport_message(frame_bytes)?;
///     handle(msg);
/// }
/// ```
pub struct UnpackingIter<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> UnpackingIter<'a> {
    /// Create iterator over packed frames in a UDP packet.
    #[inline]
    pub const fn new(data: &'a [u8]) -> Self {
        Self { data, cursor: 0 }
    }

    /// Remaining bytes not yet parsed.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.cursor)
    }
}

impl<'a> Iterator for UnpackingIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        // Minimum frame size: tag(4) + channel(4) + seq(8) = 16 bytes (for NACK)
        const MIN_FRAME: usize = 16;

        if self.remaining() < MIN_FRAME {
            return None;
        }

        let start = self.cursor;

        // Read tag to determine frame type and size
        let tag = u32::from_le_bytes([
            self.data[self.cursor],
            self.data[self.cursor + 1],
            self.data[self.cursor + 2],
            self.data[self.cursor + 3],
        ]);

        // Frame sizes based on tag (from transport.rs):
        // TAG_DATA(0): tag(4) + channel(4) + seq(8) + timestamp(8) + len(2) + payload
        // TAG_ACK(1): tag(4) + channel(4) + seq(8) + timestamp(8) = 24
        // TAG_NACK(2): tag(4) + channel(4) + seq(8) = 16
        // TAG_HEARTBEAT(3): tag(4) + channel(4) + seq(8) + timestamp(8) = 24
        // TAG_DATA_FRAG(4): tag(4) + channel(4) + seq(8) + timestamp(8) + frag_idx(2) + frag_count(2) + len(2) + payload

        let frame_len = match tag {
            0 => {
                // DATA: tag(4) + channel(4) + seq(8) + timestamp(8) + len(2) + payload
                // Length field at offset 24 (4+4+8+8)
                if self.remaining() < 26 {
                    return None;
                }
                let payload_len = u16::from_le_bytes([
                    self.data[self.cursor + 24],
                    self.data[self.cursor + 25],
                ]) as usize;
                26 + payload_len // header + payload
            }
            1 => 24, // ACK
            2 => 16, // NACK
            3 => 24, // HEARTBEAT
            4 => {
                // DATA_FRAG: need to read length at offset 28
                if self.remaining() < 30 {
                    return None;
                }
                let payload_len = u16::from_le_bytes([
                    self.data[self.cursor + 28],
                    self.data[self.cursor + 29],
                ]) as usize;
                30 + payload_len // header + payload
            }
            _ => {
                // Unknown tag - can't continue parsing
                return None;
            }
        };

        if self.remaining() < frame_len {
            return None;
        }

        self.cursor += frame_len;
        Some(&self.data[start..self.cursor])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packing_buffer_basic() {
        let mut packer = PackingBuffer::new(1024);

        assert!(packer.is_empty());
        assert_eq!(packer.remaining(), 1024);

        // Push some frames
        let frame1 = [0u8; 100];
        let frame2 = [1u8; 100];

        assert!(packer.push(&frame1));
        assert_eq!(packer.len(), 100);
        assert_eq!(packer.frame_count(), 1);

        assert!(packer.push(&frame2));
        assert_eq!(packer.len(), 200);
        assert_eq!(packer.frame_count(), 2);
    }

    #[test]
    fn packing_buffer_overflow() {
        let mut packer = PackingBuffer::new(150);

        let frame = [0u8; 100];
        assert!(packer.push(&frame));

        // Second frame doesn't fit
        assert!(!packer.push(&frame));
        assert_eq!(packer.frame_count(), 1);
    }

    #[test]
    fn packing_buffer_flush() {
        let mut packer = PackingBuffer::new(1024);

        let frame = [42u8; 100];
        packer.push(&frame);

        let flushed = packer.flush().unwrap();
        assert_eq!(flushed.len(), 100);
        assert_eq!(flushed[0], 42);

        packer.clear();
        assert!(packer.is_empty());
    }

    #[test]
    fn adaptive_flush_idle_mode() {
        let mut packer = PackingBuffer::new(1024);

        // Empty buffer - never flush
        assert!(!packer.should_flush(false));
        assert!(!packer.should_flush(true));

        // Add one small frame
        let frame = [0u8; 50];
        packer.push(&frame);

        // Idle (no more pending) - should flush immediately
        assert!(packer.should_flush(false));

        // Loaded (more pending) - should NOT flush (buffer not full)
        assert!(!packer.should_flush(true));
    }

    #[test]
    fn adaptive_flush_loaded_mode() {
        let mut packer = PackingBuffer::new(256);

        // Fill buffer but leave room for another frame
        let frame = [0u8; 50];
        packer.push(&frame);
        packer.push(&frame);
        packer.push(&frame); // 150 bytes, remaining = 106 > TYPICAL_FRAME_SIZE(64)

        // Not full yet (remaining >= TYPICAL_FRAME_SIZE)
        assert!(!packer.should_flush(true));

        // Add more to make it "full"
        packer.push(&[0u8; 50]); // 200 bytes, remaining = 56 < TYPICAL_FRAME_SIZE

        // Now should flush even in loaded mode
        assert!(packer.should_flush(true));
    }

    #[test]
    fn unpacking_iter_single_frame() {
        // Simulate a DATA frame: tag(4) + channel(4) + seq(8) + ts(8) + len(2) + payload
        // Length field at offset 24 (4+4+8+8)
        let mut packet = vec![0u8; 30];
        packet[0] = 0; // TAG_DATA
        packet[24] = 4; // payload length = 4
        packet[25] = 0;
        // payload bytes 26..30

        let mut iter = UnpackingIter::new(&packet);
        let frame = iter.next().unwrap();
        assert_eq!(frame.len(), 30);
        assert!(iter.next().is_none());
    }

    #[test]
    fn unpacking_iter_multiple_frames() {
        // Two ACK frames (24 bytes each)
        let mut packet = vec![0u8; 48];
        packet[0] = 1; // TAG_ACK
        packet[24] = 1; // TAG_ACK

        let mut iter = UnpackingIter::new(&packet);

        let frame1 = iter.next().unwrap();
        assert_eq!(frame1.len(), 24);

        let frame2 = iter.next().unwrap();
        assert_eq!(frame2.len(), 24);

        assert!(iter.next().is_none());
    }

    #[test]
    fn unpacking_iter_mixed_frames() {
        // NACK (16) + ACK (24) = 40 bytes
        let mut packet = vec![0u8; 40];
        packet[0] = 2; // TAG_NACK (16 bytes)
        packet[16] = 1; // TAG_ACK (24 bytes)

        let mut iter = UnpackingIter::new(&packet);

        assert_eq!(iter.next().unwrap().len(), 16);
        assert_eq!(iter.next().unwrap().len(), 24);
        assert!(iter.next().is_none());
    }
}
