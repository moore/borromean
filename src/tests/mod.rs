use super::*;
use crate::io::mem_io::MemIo;

extern crate alloc;


#[test]
fn new_storage() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io = MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new()
        .expect("Failed to create MemIo");

    let storage = Storage::init(&mut mem_io, DATA_SIZE, REGION_COUNT)
        .expect("Failed to initialize storage");
}
