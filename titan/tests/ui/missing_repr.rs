use titan::SharedMemorySafe;

#[derive(SharedMemorySafe)]
struct MissingRepr {
    x: u32,
}

fn main() {}
