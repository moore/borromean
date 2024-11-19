use super::*;
extern crate alloc;
use alloc::vec;


 use mem_io::MemIo;

#[test]
fn new_storage_meta() {
    let storage_meta = MemIo::new( 512, 512, 4096, 1000)
        .expect("Could not make StorageMeta");

    

}