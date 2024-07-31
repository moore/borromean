use super::*;
extern crate alloc;
use alloc::vec;

use rkyv::{
    archived_value,
};


 use mem_io::MemIo;

#[test]
fn new_storage_meta() {
    let storage_meta = MemIo::new( 512, 512, 4096, 1000)
        .expect("Could not make StorageMeta");

    /*
    let mut buffer = vec![0u8; 100];

    let pos = storage_meta.write(&mut buffer, 0)
        .expect("could not write to buffer");

    let archived = unsafe { 
        archived_value::<StorageMeta>(buffer.as_ref(), pos)
    };

    assert_eq!(archived, &storage_meta);
    */

}