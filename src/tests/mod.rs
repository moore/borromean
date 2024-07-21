use super::*;
extern crate alloc;
use alloc::vec;

use rkyv::{
    archived_value,
};

#[test]
fn new_storage_meta() {
    let storage_meta = StorageMeta::new(512, 512, 4096, 1000)
        .expect("Could not make StorageMeta");

    let mut buffer = vec![0u8; 100];

    let pos = storage_meta.write(&mut buffer, 0)
        .expect("could not write to buffer");

    //let mut serializer = BufferSerializer::new(AlignedBytes([0u8; 256]));
    //let pos = serializer.serialize_value(&storage_meta)
    //        .expect("failed to archive event");
    //let buf = serializer.into_inner();
    //let archived = rkyv::check_archived_root::<StorageMeta>(&buf[pos..]).unwrap();
    //assert_eq!(archived, &storage_meta);
    let archived = unsafe { archived_value::<StorageMeta>(buffer.as_ref(), pos) };
    assert_eq!(archived, &storage_meta);

}