use titan::SharedMemorySafe;
use std::sync::Mutex;

#[derive(SharedMemorySafe)]
#[repr(C)]
struct BadType {
    lock: Mutex<u64>,
}

fn main() {}
