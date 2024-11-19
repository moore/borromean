use super::*;
extern crate alloc;
use alloc::vec;

/* 
#[test]
fn new_storage_meta() {
    let storage_meta = StorageMeta::new(0, 512, 512, 4096, 1000)
        .expect("Could not make StorageMeta");

    let mut buffer = vec![0u8; 100];

    let pos = storage_meta.write(&mut buffer, 0)
        .expect("could not write to buffer");

    let archived: StorageMeta = from_bytes(&buffer[0..pos])
        .expect("could not deserialize message");

    assert_eq!(archived, storage_meta);

}
    */