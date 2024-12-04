use super::*;
use crate::io::{Io, IoError, RegionHeader};
use crate::io::mem_io::{MemIo, MemRegionHeader};
use crate::RegionAddress;

#[test]
fn test_wal_creation() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io = 
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    let collection_id = CollectionId(1);
    
    // Should be able to create a new WAL
    let wal = Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id)
        .expect("Failed to create WAL");

    assert_eq!(wal.collection_id, collection_id);
    assert_eq!(wal.next_entry, 0);
    assert_eq!(wal.next_region, None);
    assert_eq!(wal.collection_sequence, <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::Sequence::first());

    // Verify the region was allocated and header written correctly
    let header = mem_io.get_region_header(wal.region).expect("Failed to get header");
    assert_eq!(header.collection_id, collection_id);
    assert_eq!(header.collection_type, CollectionType::Wal);
    assert_eq!(header.collection_sequence, <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::Sequence::first());
}

#[test]
fn test_wal_creation_fails_when_storage_full() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8; 
    const REGION_COUNT: usize = 2; // Only space for root region and one more

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    let collection_id = CollectionId(1);

    // Should fail to create WAL when no space
    let _result = Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id);
    let result = Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id);
    assert!(matches!(result, Err(IoError::StorageFull)));
}
