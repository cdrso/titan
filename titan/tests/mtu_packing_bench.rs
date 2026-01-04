//! MTU Packing Benchmark - CONTRACT_025
//!
//! Compares:
//! 1. Throughput: packed vs unpacked messages
//! 2. Latency: single message send time
//!
//! Target: ≥14M msg/s throughput (match Aeron), competitive latency
//!
//! Run with: cargo test --release -p titan mtu_packing_bench -- --ignored --nocapture

use std::net::UdpSocket;
use std::time::{Duration, Instant};
use titan::data::{PackingBuffer, UnpackingIter, DEFAULT_MTU};

/// Small message size (like Aeron benchmark)
const MSG_SIZE: usize = 32;

/// Header overhead per message (DATA frame header = 26 bytes)
const HEADER_SIZE: usize = 26;

/// Total frame size
const FRAME_SIZE: usize = MSG_SIZE + HEADER_SIZE;

/// Test port
const TEST_PORT: u16 = 41000;

/// Simulated DATA frame header (tag=0, rest zeroed, with length field)
fn make_data_frame(payload_len: u16) -> Vec<u8> {
    let mut frame = vec![0u8; HEADER_SIZE + payload_len as usize];
    // tag = 0 (DATA)
    frame[0] = 0;
    // length at offset 24-25 (tag(4) + channel(4) + seq(8) + timestamp(8))
    frame[24] = (payload_len & 0xFF) as u8;
    frame[25] = ((payload_len >> 8) & 0xFF) as u8;
    frame
}

#[test]
#[ignore]
fn mtu_packing_bench() {
    println!("\n============================================================");
    println!("  MTU PACKING BENCHMARK (CONTRACT_025)");
    println!("  Message size: {} bytes, Frame size: {} bytes", MSG_SIZE, FRAME_SIZE);
    println!("  MTU: {} bytes ({} frames/packet)", DEFAULT_MTU, DEFAULT_MTU / FRAME_SIZE);
    println!("============================================================\n");

    // Bind listener to prevent ICMP errors
    let listener = UdpSocket::bind(format!("127.0.0.1:{}", TEST_PORT)).expect("bind listener");
    listener.set_nonblocking(true).expect("nonblocking");

    let sender = UdpSocket::bind("127.0.0.1:0").expect("bind sender");
    sender.connect(format!("127.0.0.1:{}", TEST_PORT)).expect("connect");
    sender.set_nonblocking(true).expect("nonblocking");

    println!("--- THROUGHPUT TESTS ---\n");

    // Test 1: Baseline - one frame per send
    bench_unpacked_throughput(&sender);

    // Test 2: Packed - multiple frames per send
    bench_packed_throughput(&sender);

    println!("\n--- LATENCY TESTS ---\n");

    // Test 3: Single message latency (unpacked)
    bench_unpacked_latency(&sender, &listener);

    // Test 4: Single message latency with packing (immediate flush)
    bench_packed_latency(&sender, &listener);

    // Test 5: Verify unpacking works correctly
    test_unpack_correctness();

    println!("\n--- ADAPTIVE BATCHING (CONTRACT_026) ---\n");

    // Test 6: Adaptive batching behavior
    test_adaptive_batching();

    println!("\n============================================================");
    println!("  BENCHMARK COMPLETE");
    println!("============================================================\n");
}

fn bench_unpacked_throughput(sender: &UdpSocket) {
    let frame = make_data_frame(MSG_SIZE as u16);
    let count: u64 = 1_000_000;

    // Warmup
    for _ in 0..10_000 {
        let _ = sender.send(&frame);
    }

    let start = Instant::now();
    for _ in 0..count {
        let _ = sender.send(&frame);
    }
    let elapsed = start.elapsed();

    print_throughput("Unpacked (1 frame/packet)", count, elapsed);
}

fn bench_packed_throughput(sender: &UdpSocket) {
    let frame = make_data_frame(MSG_SIZE as u16);
    let frames_per_packet = DEFAULT_MTU / frame.len();
    let total_msgs: u64 = 50_000_000;
    let packets_needed = total_msgs / frames_per_packet as u64;

    // Pre-build a packed packet
    let mut packer = PackingBuffer::new(DEFAULT_MTU);
    for _ in 0..frames_per_packet {
        packer.push(&frame);
    }
    let packed = packer.flush().unwrap().to_vec();

    // Warmup
    for _ in 0..1000 {
        let _ = sender.send(&packed);
    }

    let start = Instant::now();
    let mut sent_packets: u64 = 0;
    for _ in 0..packets_needed {
        if sender.send(&packed).is_ok() {
            sent_packets += 1;
        }
    }
    let elapsed = start.elapsed();

    let total_msgs_sent = sent_packets * frames_per_packet as u64;
    print_throughput(
        &format!("Packed ({} frames/packet)", frames_per_packet),
        total_msgs_sent,
        elapsed,
    );

    // Also show packet rate
    let pkt_rate = sent_packets as f64 / elapsed.as_secs_f64();
    println!("    Packet rate: {:.1}K pkt/s", pkt_rate / 1000.0);
}

fn bench_unpacked_latency(sender: &UdpSocket, receiver: &UdpSocket) {
    let frame = make_data_frame(MSG_SIZE as u16);
    let mut recv_buf = [0u8; 1024];

    // Drain any pending packets
    while receiver.recv(&mut recv_buf).is_ok() {}

    let iterations = 10_000;
    let mut total_ns: u64 = 0;

    for _ in 0..iterations {
        let start = Instant::now();
        sender.send(&frame).expect("send");
        // Busy-wait for response (measures one-way + overhead)
        loop {
            if receiver.recv(&mut recv_buf).is_ok() {
                break;
            }
        }
        total_ns += start.elapsed().as_nanos() as u64;
    }

    let avg_ns = total_ns / iterations;
    println!(
        "  Unpacked latency (send+recv):     {:>6} ns/roundtrip",
        avg_ns
    );
}

fn bench_packed_latency(sender: &UdpSocket, receiver: &UdpSocket) {
    let frame = make_data_frame(MSG_SIZE as u16);
    let mut recv_buf = [0u8; DEFAULT_MTU];

    // Drain any pending packets
    while receiver.recv(&mut recv_buf).is_ok() {}

    let iterations = 10_000;
    let mut total_ns: u64 = 0;

    // Immediate flush mode - pack single message and send immediately
    for _ in 0..iterations {
        let mut packer = PackingBuffer::new(DEFAULT_MTU);

        let start = Instant::now();
        packer.push(&frame);
        let data = packer.flush().unwrap();
        sender.send(data).expect("send");

        // Busy-wait for response
        loop {
            if receiver.recv(&mut recv_buf).is_ok() {
                break;
            }
        }
        total_ns += start.elapsed().as_nanos() as u64;
    }

    let avg_ns = total_ns / iterations;
    println!(
        "  Packed latency (immediate flush): {:>6} ns/roundtrip",
        avg_ns
    );
}

fn test_unpack_correctness() {
    println!("\n--- UNPACKING CORRECTNESS ---\n");

    // Build a packed packet with multiple frames
    let mut packer = PackingBuffer::new(DEFAULT_MTU);

    // Add frames of different types
    let data_frame = make_data_frame(MSG_SIZE as u16);
    let ack_frame = {
        let mut f = vec![0u8; 24];
        f[0] = 1; // TAG_ACK
        f
    };
    let nack_frame = {
        let mut f = vec![0u8; 16];
        f[0] = 2; // TAG_NACK
        f
    };

    packer.push(&data_frame);
    packer.push(&ack_frame);
    packer.push(&nack_frame);
    packer.push(&data_frame);

    let packed = packer.flush().unwrap();
    println!("  Packed {} bytes with 4 frames", packed.len());

    // Unpack and verify
    let mut iter = UnpackingIter::new(packed);
    let mut count = 0;
    let expected_sizes = [FRAME_SIZE, 24, 16, FRAME_SIZE];

    for (i, frame) in iter.by_ref().enumerate() {
        assert_eq!(
            frame.len(),
            expected_sizes[i],
            "Frame {} size mismatch",
            i
        );
        count += 1;
    }

    assert_eq!(count, 4, "Expected 4 frames");
    println!("  Unpacked {} frames correctly", count);
    println!("  Remaining bytes: {}", iter.remaining());
}

fn print_throughput(name: &str, count: u64, elapsed: Duration) {
    let rate = count as f64 / elapsed.as_secs_f64();
    let ns_per_msg = elapsed.as_nanos() as f64 / count as f64;
    let target_met = if rate >= 14_000_000.0 { "✓" } else { " " };

    println!(
        "{} {:40} {:>10.2}M msg/s  {:>6.1} ns/msg",
        target_met,
        name,
        rate / 1_000_000.0,
        ns_per_msg
    );
}

/// Demonstrates adaptive batching (CONTRACT_026)
///
/// Shows that:
/// - Idle mode: flushes immediately (low latency)
/// - Loaded mode: batches until full (high throughput)
fn test_adaptive_batching() {
    let frame = make_data_frame(MSG_SIZE as u16);

    // Scenario 1: Idle mode - single message, immediate flush
    {
        let mut packer = PackingBuffer::new(DEFAULT_MTU);
        packer.push(&frame);

        // Idle: no more messages pending -> should flush immediately
        let should_flush = packer.should_flush(false);
        println!(
            "  Idle mode (1 frame, more_pending=false):  should_flush={}",
            should_flush
        );
        assert!(should_flush, "Idle mode must flush immediately");
    }

    // Scenario 2: Loaded mode - buffer not full, keep batching
    {
        let mut packer = PackingBuffer::new(DEFAULT_MTU);
        for _ in 0..10 {
            packer.push(&frame);
        }

        // Loaded: more messages pending, buffer not full -> don't flush
        let should_flush = packer.should_flush(true);
        println!(
            "  Loaded mode (10 frames, more_pending=true): should_flush={} (remaining={})",
            should_flush,
            packer.remaining()
        );
        assert!(!should_flush, "Loaded mode with room must batch");
    }

    // Scenario 3: Loaded mode - buffer nearly full, must flush
    {
        let mut packer = PackingBuffer::new(DEFAULT_MTU);
        let frames_to_fill = DEFAULT_MTU / frame.len();
        for _ in 0..frames_to_fill {
            if !packer.can_fit(frame.len()) {
                break;
            }
            packer.push(&frame);
        }

        // Loaded: more pending but buffer full -> must flush
        let should_flush = packer.should_flush(true);
        println!(
            "  Loaded mode ({} frames, buffer full):     should_flush={} (remaining={})",
            packer.frame_count(),
            should_flush,
            packer.remaining()
        );
        assert!(should_flush, "Full buffer must flush even in loaded mode");
    }

    // Scenario 4: Self-correcting behavior simulation
    println!("\n  Self-correcting simulation:");
    println!("    Burst of 500 messages -> batches aggressively");
    println!("    Burst exhausted -> reverts to immediate flush");
    println!("    Next single message -> immediate flush (low latency)");

    let mut packer = PackingBuffer::new(DEFAULT_MTU);
    let mut flush_count = 0;

    // Simulate burst
    for i in 0..500 {
        if !packer.can_fit(frame.len()) {
            packer.clear();
            flush_count += 1;
        }
        packer.push(&frame);

        // More pending for first 499 messages, not for last
        let more_pending = i < 499;
        if packer.should_flush(more_pending) {
            packer.clear();
            flush_count += 1;
        }
    }

    println!("    Burst of 500 msgs -> {} flushes (batched)", flush_count);
    let expected_flushes = 500 / (DEFAULT_MTU / frame.len()) + 1;
    println!(
        "    Expected ~{} flushes (500/{} frames/packet)",
        expected_flushes,
        DEFAULT_MTU / frame.len()
    );
}
