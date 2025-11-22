use titan::SharedMemorySafe;

#[derive(SharedMemorySafe)]
#[repr(C)]
struct HasReference {
    data: &'static u32,
}

fn main() {}
