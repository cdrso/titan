use titan::SharedMemorySafe;

#[derive(SharedMemorySafe)]
#[repr(C)]
struct HasVec {
    data: Vec<u8>,
}

fn main() {}
