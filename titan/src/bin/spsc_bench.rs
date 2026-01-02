//! SPSC queue throughput and latency benchmark.
//!
//! Usage:
//!     cargo run --release --bin `spsc_bench`
//!
//! Environment variables:
//!     `PRODUCER_CPU=0`  Pin producer to CPU 0 (default: 0)
//!     `CONSUMER_CPU=2`  Pin consumer to CPU 2 (default: 2)

use std::env;
use std::hint;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use titan::ipc::shmem::ShmPath;
use titan::ipc::spsc as ipc_spsc;
use titan::sync::spsc as sync_spsc;

const QUEUE_SIZE: usize = 1_048_576; // 1M - fits in stack for sync version
const ITERATIONS: usize = 10_000_000;

type Payload = i32;

fn get_cpu_affinity() -> (Option<usize>, Option<usize>) {
    let producer_cpu = env::var("PRODUCER_CPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .or(Some(0));
    let consumer_cpu = env::var("CONSUMER_CPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .or(Some(2));
    (producer_cpu, consumer_cpu)
}

fn pin_to_cpu(cpu: Option<usize>) {
    if let Some(id) = cpu {
        core_affinity::set_for_current(core_affinity::CoreId { id });
    }
}

fn unique_path(tag: &str) -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    format!(
        "/{}-{}-{}",
        tag,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

// ============================================================================
// IPC (shared memory) benchmarks
// ============================================================================

fn ipc_bench_throughput(producer_cpu: Option<usize>, consumer_cpu: Option<usize>) {
    let path = ShmPath::new(unique_path("throughput")).unwrap();
    let producer = ipc_spsc::Producer::<Payload, QUEUE_SIZE, _>::create(path.clone()).unwrap();

    let ready = Arc::new(AtomicBool::new(false));
    let ready_clone = ready.clone();

    // Consumer thread
    let consumer_thread = std::thread::spawn(move || {
        let consumer = ipc_spsc::Consumer::<Payload, QUEUE_SIZE, _>::open(path).unwrap();
        pin_to_cpu(consumer_cpu);

        // Signal ready
        ready_clone.store(true, Ordering::Release);

        let iterations = Payload::try_from(ITERATIONS).expect("ITERATIONS exceeds Payload range");
        for expected in 0..iterations {
            let value = consumer.pop_blocking(ipc_spsc::Timeout::Infinite).unwrap();
            assert_eq!(value, expected, "Data corruption");
        }
    });

    // Wait for consumer to be ready
    while !ready.load(Ordering::Acquire) {
        hint::spin_loop();
    }

    pin_to_cpu(producer_cpu);

    let start = Instant::now();

    let iterations = Payload::try_from(ITERATIONS).expect("ITERATIONS exceeds Payload range");
    for i in 0..iterations {
        producer
            .push_blocking(i, ipc_spsc::Timeout::Infinite)
            .unwrap();
    }

    consumer_thread.join().unwrap();
    let elapsed = start.elapsed();

    let ops_per_ms = ITERATIONS as u128 * 1_000_000 / elapsed.as_nanos();
    println!("  throughput: {ops_per_ms} ops/ms");
}

fn ipc_bench_rtt(producer_cpu: Option<usize>, consumer_cpu: Option<usize>) {
    let q1_path = ShmPath::new(unique_path("q1")).unwrap();
    let q2_path = ShmPath::new(unique_path("q2")).unwrap();

    let q1_producer =
        ipc_spsc::Producer::<Payload, QUEUE_SIZE, _>::create(q1_path.clone()).unwrap();

    let ready = Arc::new(AtomicBool::new(false));
    let ready_clone = ready.clone();
    let q2_path_for_consumer = q2_path.clone();

    // Responder thread
    let responder = std::thread::spawn(move || {
        let q1_consumer = ipc_spsc::Consumer::<Payload, QUEUE_SIZE, _>::open(q1_path).unwrap();
        let q2_producer = ipc_spsc::Producer::<Payload, QUEUE_SIZE, _>::create(q2_path).unwrap();
        pin_to_cpu(consumer_cpu);

        // Signal ready
        ready_clone.store(true, Ordering::Release);

        for _ in 0..ITERATIONS {
            let value = q1_consumer
                .pop_blocking(ipc_spsc::Timeout::Infinite)
                .unwrap();
            q2_producer
                .push_blocking(value, ipc_spsc::Timeout::Infinite)
                .unwrap();
        }
    });

    // Wait for responder to be ready
    while !ready.load(Ordering::Acquire) {
        hint::spin_loop();
    }

    // Open q2 after responder created it
    let q2_consumer =
        ipc_spsc::Consumer::<Payload, QUEUE_SIZE, _>::open(q2_path_for_consumer).unwrap();

    pin_to_cpu(producer_cpu);

    let start = Instant::now();

    let iterations = Payload::try_from(ITERATIONS).expect("ITERATIONS exceeds Payload range");
    for i in 0..iterations {
        q1_producer
            .push_blocking(i, ipc_spsc::Timeout::Infinite)
            .unwrap();
        let _ = q2_consumer.pop_blocking(ipc_spsc::Timeout::Infinite);
    }

    let elapsed = start.elapsed();
    responder.join().unwrap();

    let rtt_ns = elapsed.as_nanos() / ITERATIONS as u128;
    println!("  rtt:        {rtt_ns} ns");
}

// ============================================================================
// Sync (heap-backed) benchmarks
// ============================================================================

fn sync_bench_throughput(producer_cpu: Option<usize>, consumer_cpu: Option<usize>) {
    let (producer, consumer) = sync_spsc::channel::<Payload, QUEUE_SIZE>();

    let ready = Arc::new(AtomicBool::new(false));
    let ready_clone = ready.clone();

    // Consumer thread
    let consumer_thread = std::thread::spawn(move || {
        pin_to_cpu(consumer_cpu);

        // Signal ready
        ready_clone.store(true, Ordering::Release);

        let iterations = Payload::try_from(ITERATIONS).expect("ITERATIONS exceeds Payload range");
        for expected in 0..iterations {
            let value = consumer.pop_blocking(sync_spsc::Timeout::Infinite).unwrap();
            assert_eq!(value, expected, "Data corruption");
        }
    });

    // Wait for consumer to be ready
    while !ready.load(Ordering::Acquire) {
        hint::spin_loop();
    }

    pin_to_cpu(producer_cpu);

    let start = Instant::now();

    let iterations = Payload::try_from(ITERATIONS).expect("ITERATIONS exceeds Payload range");
    for i in 0..iterations {
        producer
            .push_blocking(i, sync_spsc::Timeout::Infinite)
            .unwrap();
    }

    consumer_thread.join().unwrap();
    let elapsed = start.elapsed();

    let ops_per_ms = ITERATIONS as u128 * 1_000_000 / elapsed.as_nanos();
    println!("  throughput: {ops_per_ms} ops/ms");
}

fn sync_bench_rtt(producer_cpu: Option<usize>, consumer_cpu: Option<usize>) {
    let (q1_producer, q1_consumer) = sync_spsc::channel::<Payload, QUEUE_SIZE>();
    let (q2_producer, q2_consumer) = sync_spsc::channel::<Payload, QUEUE_SIZE>();

    let ready = Arc::new(AtomicBool::new(false));
    let ready_clone = ready.clone();

    // Responder thread
    let responder = std::thread::spawn(move || {
        pin_to_cpu(consumer_cpu);

        // Signal ready
        ready_clone.store(true, Ordering::Release);

        for _ in 0..ITERATIONS {
            let value = q1_consumer
                .pop_blocking(sync_spsc::Timeout::Infinite)
                .unwrap();
            q2_producer
                .push_blocking(value, sync_spsc::Timeout::Infinite)
                .unwrap();
        }
    });

    // Wait for responder to be ready
    while !ready.load(Ordering::Acquire) {
        hint::spin_loop();
    }

    pin_to_cpu(producer_cpu);

    let start = Instant::now();

    let iterations = Payload::try_from(ITERATIONS).expect("ITERATIONS exceeds Payload range");
    for i in 0..iterations {
        q1_producer
            .push_blocking(i, sync_spsc::Timeout::Infinite)
            .unwrap();
        let _ = q2_consumer.pop_blocking(sync_spsc::Timeout::Infinite);
    }

    let elapsed = start.elapsed();
    responder.join().unwrap();

    let rtt_ns = elapsed.as_nanos() / ITERATIONS as u128;
    println!("  rtt:        {rtt_ns} ns");
}

fn main() {
    let (producer_cpu, consumer_cpu) = get_cpu_affinity();

    println!("SPSC benchmark (size={QUEUE_SIZE}, iters={ITERATIONS})");
    println!();

    println!("ipc (shared memory):");
    ipc_bench_throughput(producer_cpu, consumer_cpu);
    ipc_bench_rtt(producer_cpu, consumer_cpu);
    println!();

    println!("sync (heap-backed):");
    sync_bench_throughput(producer_cpu, consumer_cpu);
    sync_bench_rtt(producer_cpu, consumer_cpu);
}
