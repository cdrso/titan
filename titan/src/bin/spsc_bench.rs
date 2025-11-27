//! SPSC queue throughput and latency benchmark.
//!
//! Usage:
//!     cargo run --release --bin spsc_bench
//!
//! Environment variables:
//!     PRODUCER_CPU=0  Pin producer to CPU 0 (default: 0)
//!     CONSUMER_CPU=2  Pin consumer to CPU 2 (default: 2)

use std::env;
use std::hint;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use titan::ipc::spsc::{Consumer, Producer};

const QUEUE_SIZE: usize = 1 << 24;
const ITERATIONS: usize = 1 << 24;

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

fn bench_throughput(producer_cpu: Option<usize>, consumer_cpu: Option<usize>) {
    let path = unique_path("throughput");
    let producer = Producer::<Payload, QUEUE_SIZE, _>::create(&path).unwrap();

    let ready = Arc::new(AtomicBool::new(false));
    let ready_clone = ready.clone();
    let path_clone = path.clone();

    // Consumer thread
    let consumer_thread = std::thread::spawn(move || {
        let consumer = Consumer::<Payload, QUEUE_SIZE, _>::open(&path_clone).unwrap();
        pin_to_cpu(consumer_cpu);

        // Signal ready
        ready_clone.store(true, Ordering::Release);

        for expected in 0..ITERATIONS as Payload {
            loop {
                if let Some(value) = consumer.pop() {
                    if value != expected {
                        panic!("Data corruption: expected {}, got {}", expected, value);
                    }
                    break;
                }
                hint::spin_loop();
            }
        }
    });

    // Wait for consumer to be ready
    while !ready.load(Ordering::Acquire) {
        hint::spin_loop();
    }

    pin_to_cpu(producer_cpu);

    let start = Instant::now();

    for i in 0..ITERATIONS as Payload {
        producer.push(i).unwrap(); // Queue is large enough, should never fail
    }

    consumer_thread.join().unwrap();
    let elapsed = start.elapsed();

    let ops_per_ms = ITERATIONS as u128 * 1_000_000 / elapsed.as_nanos();
    println!("{} ops/ms", ops_per_ms);
}

fn bench_rtt(producer_cpu: Option<usize>, consumer_cpu: Option<usize>) {
    let q1_path = unique_path("q1");
    let q2_path = unique_path("q2");

    let q1_producer = Producer::<Payload, QUEUE_SIZE, _>::create(&q1_path).unwrap();

    let ready = Arc::new(AtomicBool::new(false));
    let ready_clone = ready.clone();
    let q1_path_clone = q1_path.clone();
    let q2_path_clone = q2_path.clone();

    // Responder thread
    let responder = std::thread::spawn(move || {
        let q1_consumer = Consumer::<Payload, QUEUE_SIZE, _>::open(&q1_path_clone).unwrap();
        let q2_producer = Producer::<Payload, QUEUE_SIZE, _>::create(&q2_path_clone).unwrap();
        pin_to_cpu(consumer_cpu);

        // Signal ready
        ready_clone.store(true, Ordering::Release);

        for _ in 0..ITERATIONS {
            loop {
                if let Some(value) = q1_consumer.pop() {
                    q2_producer.push(value).unwrap();
                    break;
                }
                hint::spin_loop();
            }
        }
    });

    // Wait for responder to be ready
    while !ready.load(Ordering::Acquire) {
        hint::spin_loop();
    }

    // Open q2 after responder created it
    let q2_consumer = Consumer::<Payload, QUEUE_SIZE, _>::open(&q2_path).unwrap();

    pin_to_cpu(producer_cpu);

    let start = Instant::now();

    for i in 0..ITERATIONS as Payload {
        q1_producer.push(i).unwrap();
        loop {
            if q2_consumer.pop().is_some() {
                break;
            }
            hint::spin_loop();
        }
    }

    let elapsed = start.elapsed();
    responder.join().unwrap();

    let rtt_ns = elapsed.as_nanos() / ITERATIONS as u128;
    println!("{} ns RTT", rtt_ns);
}

fn main() {
    let (producer_cpu, consumer_cpu) = get_cpu_affinity();

    println!("titan SPSC (size={}, iters={}):", QUEUE_SIZE, ITERATIONS);
    bench_throughput(producer_cpu, consumer_cpu);
    bench_rtt(producer_cpu, consumer_cpu);
}
