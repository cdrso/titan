use titan::SharedMemorySafe;

#[derive(SharedMemorySafe)]
#[repr(C)]
struct HasBox {
    data: Box<u32>,
}

fn main() {}
