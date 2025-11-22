use titan::SharedMemorySafe;
use std::sync::RwLock;

#[derive(SharedMemorySafe)]
#[repr(C)]
struct BadType {
    lock: RwLock<u64>,
}

fn main() {}
