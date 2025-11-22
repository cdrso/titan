use titan::SharedMemorySafe;

#[derive(SharedMemorySafe)]
#[repr(C)]
struct HasRawPointer {
    ptr: *const u32,
}

fn main() {}
