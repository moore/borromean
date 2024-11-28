use super::*;
extern crate alloc;


 use mem_io::MemIo;

#[test]
fn new_storage_meta() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mem_io = MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new()
        .expect("Failed to create MemIo");
}