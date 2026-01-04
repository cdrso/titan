//! Standalone UDP dispatch benchmark.
//!
//! This test measures raw UDP send performance without any receiver,
//! to find the optimal configuration for maximum dispatch rate.
//!
//! Run with: cargo test --release -p titan udp_dispatch_bench -- --ignored --nocapture

use std::net::UdpSocket;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

/// Message size (typical Titan payload)
const MSG_SIZE: usize = 256;

/// Number of messages to send per test
const MSG_COUNT: u64 = 2_000_000;

/// Destination port (no listener - packets dropped by kernel)
const DUMMY_PORT: u16 = 39999;

#[test]
#[ignore]
fn udp_dispatch_bench() {
    println!("\n============================================================");
    println!("  UDP DISPATCH BENCHMARK");
    println!("  Target: >14M msg/s (Aeron baseline)");
    println!("============================================================\n");

    // Test 1: Baseline - single send() on connected socket
    bench_single_send();

    // Test 2: sendmmsg with various batch sizes
    for batch_size in [16, 32, 64, 128, 256, 512] {
        bench_sendmmsg(batch_size);
    }

    // Test 3: sendmmsg with larger socket buffer
    bench_sendmmsg_large_buffer(64);

    // Test 4: Multiple sendmmsg calls in tight loop (no syscall batching)
    bench_sendmmsg_burst(64);

    // Test 5: AERON APPROACH - Pack multiple messages into large UDP packets (8KB MTU)
    println!("\n--- AERON APPROACH: Large MTU (pack msgs into 8KB packets) ---");
    for msgs_per_packet in [64, 128, 256] {
        bench_large_mtu(msgs_per_packet, 16); // 16 packets per sendmmsg
    }

    println!("\n============================================================");
    println!("  BENCHMARK COMPLETE");
    println!("============================================================\n");
}

fn bench_single_send() {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
    socket.connect(format!("127.0.0.1:{}", DUMMY_PORT)).expect("connect");
    socket.set_nonblocking(true).expect("nonblocking");

    let fd = socket.as_raw_fd();
    let data = [0u8; MSG_SIZE];

    // Warmup
    for _ in 0..10_000 {
        unsafe {
            libc::send(fd, data.as_ptr() as *const _, data.len(), libc::MSG_DONTWAIT);
        }
    }

    let count = MSG_COUNT / 10; // Fewer iterations for slow path
    let start = Instant::now();
    for _ in 0..count {
        unsafe {
            libc::send(fd, data.as_ptr() as *const _, data.len(), libc::MSG_DONTWAIT);
        }
    }
    let elapsed = start.elapsed();
    print_result("single send()", count, elapsed);
}

fn bench_sendmmsg(batch_size: usize) {
    // Bind a listener to prevent ICMP errors
    let _listener = UdpSocket::bind(format!("127.0.0.1:{}", DUMMY_PORT)).ok();

    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
    socket.connect(format!("127.0.0.1:{}", DUMMY_PORT)).expect("connect");
    socket.set_nonblocking(true).expect("nonblocking");

    let fd = socket.as_raw_fd();

    // Allocate batch structures
    let mut data: Vec<[u8; MSG_SIZE]> = vec![[0u8; MSG_SIZE]; batch_size];
    let mut iovecs: Vec<libc::iovec> = vec![unsafe { std::mem::zeroed() }; batch_size];
    let mut msgs: Vec<libc::mmsghdr> = vec![unsafe { std::mem::zeroed() }; batch_size];

    // Initialize structures
    for i in 0..batch_size {
        iovecs[i].iov_base = data[i].as_mut_ptr() as *mut _;
        iovecs[i].iov_len = MSG_SIZE;
        msgs[i].msg_hdr.msg_iov = &mut iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        // msg_name = NULL for connected socket
        msgs[i].msg_hdr.msg_name = std::ptr::null_mut();
        msgs[i].msg_hdr.msg_namelen = 0;
    }

    // Warmup
    for _ in 0..1000 {
        unsafe {
            libc::sendmmsg(fd, msgs.as_mut_ptr(), batch_size as u32, libc::MSG_DONTWAIT);
        }
    }

    let batches = MSG_COUNT / batch_size as u64;
    let start = Instant::now();
    let mut total_sent: i32 = 0;
    for _ in 0..batches {
        let sent = unsafe {
            libc::sendmmsg(fd, msgs.as_mut_ptr(), batch_size as u32, libc::MSG_DONTWAIT)
        };
        total_sent += sent;
    }
    let elapsed = start.elapsed();

    // Calculate actual metrics
    let actual_batches = if total_sent > 0 { (total_sent as u64 + batch_size as u64 - 1) / batch_size as u64 } else { 0 };
    let avg_us_per_call = if actual_batches > 0 { elapsed.as_micros() as u64 / actual_batches } else { 0 };

    // Always print actual metrics
    let actual_rate = if elapsed.as_secs_f64() > 0.0 { total_sent as f64 / elapsed.as_secs_f64() } else { 0.0 };
    eprintln!("  batch={}: sent={}/{} rate={:.2}M/s avg_call={}µs",
        batch_size, total_sent, (batches as i32) * (batch_size as i32),
        actual_rate / 1_000_000.0, avg_us_per_call);

    let total_msgs = total_sent as u64; // Use actual sent count
    print_result(&format!("sendmmsg batch={}", batch_size), total_msgs, elapsed);
}

fn bench_sendmmsg_large_buffer(batch_size: usize) {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
    socket.connect(format!("127.0.0.1:{}", DUMMY_PORT)).expect("connect");
    socket.set_nonblocking(true).expect("nonblocking");

    // Try to set larger send buffer (may require root/sysctl)
    let fd = socket.as_raw_fd();
    let buf_size: libc::c_int = 64 * 1024 * 1024; // 64MB
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &buf_size as *const _ as *const _,
            std::mem::size_of::<libc::c_int>() as u32,
        );
    }

    // Check actual buffer size
    let mut actual_size: libc::c_int = 0;
    let mut len: libc::socklen_t = std::mem::size_of::<libc::c_int>() as u32;
    unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &mut actual_size as *mut _ as *mut _,
            &mut len,
        );
    }

    // Allocate batch structures
    let mut data: Vec<[u8; MSG_SIZE]> = vec![[0u8; MSG_SIZE]; batch_size];
    let mut iovecs: Vec<libc::iovec> = vec![unsafe { std::mem::zeroed() }; batch_size];
    let mut msgs: Vec<libc::mmsghdr> = vec![unsafe { std::mem::zeroed() }; batch_size];

    for i in 0..batch_size {
        iovecs[i].iov_base = data[i].as_mut_ptr() as *mut _;
        iovecs[i].iov_len = MSG_SIZE;
        msgs[i].msg_hdr.msg_iov = &mut iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        msgs[i].msg_hdr.msg_name = std::ptr::null_mut();
        msgs[i].msg_hdr.msg_namelen = 0;
    }

    // Warmup
    for _ in 0..1000 {
        unsafe {
            libc::sendmmsg(fd, msgs.as_mut_ptr(), batch_size as u32, libc::MSG_DONTWAIT);
        }
    }

    let batches = MSG_COUNT / batch_size as u64;
    let start = Instant::now();
    for _ in 0..batches {
        unsafe {
            libc::sendmmsg(fd, msgs.as_mut_ptr(), batch_size as u32, libc::MSG_DONTWAIT);
        }
    }
    let elapsed = start.elapsed();
    let total_msgs = batches * batch_size as u64;
    print_result(
        &format!("sendmmsg batch={} sndbuf={}KB", batch_size, actual_size / 1024),
        total_msgs,
        elapsed,
    );
}

fn bench_sendmmsg_burst(batch_size: usize) {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
    socket.connect(format!("127.0.0.1:{}", DUMMY_PORT)).expect("connect");
    socket.set_nonblocking(true).expect("nonblocking");

    let fd = socket.as_raw_fd();

    // Allocate batch structures
    let mut data: Vec<[u8; MSG_SIZE]> = vec![[0u8; MSG_SIZE]; batch_size];
    let mut iovecs: Vec<libc::iovec> = vec![unsafe { std::mem::zeroed() }; batch_size];
    let mut msgs: Vec<libc::mmsghdr> = vec![unsafe { std::mem::zeroed() }; batch_size];

    for i in 0..batch_size {
        iovecs[i].iov_base = data[i].as_mut_ptr() as *mut _;
        iovecs[i].iov_len = MSG_SIZE;
        msgs[i].msg_hdr.msg_iov = &mut iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        msgs[i].msg_hdr.msg_name = std::ptr::null_mut();
        msgs[i].msg_hdr.msg_namelen = 0;
    }

    // Warmup
    for _ in 0..1000 {
        unsafe {
            libc::sendmmsg(fd, msgs.as_mut_ptr(), batch_size as u32, libc::MSG_DONTWAIT);
        }
    }

    // Burst: 8 sendmmsg calls per iteration (simulating busy loop)
    const BURST_CALLS: u64 = 8;
    let batches = MSG_COUNT / (batch_size as u64 * BURST_CALLS);
    let start = Instant::now();
    for _ in 0..batches {
        for _ in 0..BURST_CALLS {
            unsafe {
                libc::sendmmsg(fd, msgs.as_mut_ptr(), batch_size as u32, libc::MSG_DONTWAIT);
            }
        }
    }
    let elapsed = start.elapsed();
    let total_msgs = batches * batch_size as u64 * BURST_CALLS;
    print_result(
        &format!("sendmmsg batch={} burst=8", batch_size),
        total_msgs,
        elapsed,
    );
}

/// Aeron-style benchmark: pack multiple small messages into large UDP packets
/// This reduces per-packet kernel overhead by amortizing it across many messages.
fn bench_large_mtu(msgs_per_packet: usize, packets_per_batch: usize) {
    const MSG_SIZE_SMALL: usize = 32; // Aeron default message size
    let packet_size = msgs_per_packet * MSG_SIZE_SMALL;

    // Bind listener to prevent ICMP errors
    let _listener = UdpSocket::bind(format!("127.0.0.1:{}", DUMMY_PORT)).ok();

    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
    socket.connect(format!("127.0.0.1:{}", DUMMY_PORT)).expect("connect");
    socket.set_nonblocking(true).expect("nonblocking");

    let fd = socket.as_raw_fd();

    // Allocate large packet buffers (like Aeron's 8KB MTU)
    let mut packets: Vec<Vec<u8>> = (0..packets_per_batch)
        .map(|_| vec![0u8; packet_size])
        .collect();
    let mut iovecs: Vec<libc::iovec> = vec![unsafe { std::mem::zeroed() }; packets_per_batch];
    let mut msgs: Vec<libc::mmsghdr> = vec![unsafe { std::mem::zeroed() }; packets_per_batch];

    // Initialize structures - each msg is one large packet containing many messages
    for i in 0..packets_per_batch {
        iovecs[i].iov_base = packets[i].as_mut_ptr() as *mut _;
        iovecs[i].iov_len = packet_size;
        msgs[i].msg_hdr.msg_iov = &mut iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        msgs[i].msg_hdr.msg_name = std::ptr::null_mut();
        msgs[i].msg_hdr.msg_namelen = 0;
    }

    // Target total messages
    let total_target_msgs: u64 = 50_000_000;
    let msgs_per_batch = (msgs_per_packet * packets_per_batch) as u64;
    let batches = total_target_msgs / msgs_per_batch;

    // Warmup
    for _ in 0..1000 {
        unsafe {
            libc::sendmmsg(fd, msgs.as_mut_ptr(), packets_per_batch as u32, libc::MSG_DONTWAIT);
        }
    }

    let start = Instant::now();
    let mut total_packets_sent: i64 = 0;
    for _ in 0..batches {
        let sent = unsafe {
            libc::sendmmsg(fd, msgs.as_mut_ptr(), packets_per_batch as u32, libc::MSG_DONTWAIT)
        };
        if sent > 0 {
            total_packets_sent += sent as i64;
        }
    }
    let elapsed = start.elapsed();

    // Calculate message rate (each packet contains msgs_per_packet messages)
    let total_msgs = total_packets_sent as u64 * msgs_per_packet as u64;
    let rate = total_msgs as f64 / elapsed.as_secs_f64();
    let packet_rate = total_packets_sent as f64 / elapsed.as_secs_f64();
    let ns_per_msg = if total_msgs > 0 { elapsed.as_nanos() as f64 / total_msgs as f64 } else { 0.0 };
    let target_met = if rate > 14_000_000.0 { "✓" } else { " " };

    println!(
        "{} large_mtu {}B×{} ({} msgs/pkt)     {:>10.2}M msg/s  {:>6.0} ns/msg  ({:.1}K pkt/s)",
        target_met,
        packet_size,
        packets_per_batch,
        msgs_per_packet,
        rate / 1_000_000.0,
        ns_per_msg,
        packet_rate / 1000.0
    );
}

fn print_result(name: &str, count: u64, elapsed: Duration) {
    let rate = count as f64 / elapsed.as_secs_f64();
    let ns_per_msg = elapsed.as_nanos() as f64 / count as f64;
    let target_met = if rate > 14_000_000.0 { "✓" } else { " " };

    println!(
        "{} {:40} {:>10.2}M msg/s  {:>6.0} ns/msg",
        target_met,
        name,
        rate / 1_000_000.0,
        ns_per_msg
    );
}
