use super::*;
extern crate alloc;

use mem_io::{MemCollectionSequence, MemIo, MemRegionAddress, MemStorageSequence};

#[test]
fn new_storage_meta() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let _mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");
}

#[test]
fn init_io() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let _io: Io<'_, MemIo<1024, 8, 4>, MAX_HEADS> =
        Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");
}
#[test]
fn test_double_init_fails() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    // First init should succeed
    let _io: Io<'_, MemIo<1024, 8, 4>, MAX_HEADS> =
        Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    // Second init should fail with AlreadyInitialized
    assert!(matches!(
        Io::<'_, _, MAX_HEADS>::init(&mut mem_io, DATA_SIZE, REGION_COUNT),
        Err(IoError::AlreadyInitialized)
    ));
}

#[test]
fn test_open_uninitialized_fails() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    // Opening uninitialized storage should fail
    assert!(matches!(
        Io::<'_, _, MAX_HEADS>::open(&mut mem_io),
        Err(IoError::NotInitialized)
    ));
}

#[test]
fn test_init_and_open() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    // Initialize storage
    let _io = Io::<'_, _, MAX_HEADS>::init(&mut mem_io, DATA_SIZE, REGION_COUNT)
        .expect("Failed to initialize Io");

    // Should be able to open initialized storage
    let _io = Io::<'_, _, MAX_HEADS>::open(&mut mem_io).expect("Failed to open Io");
}

#[test]
fn test_invalid_region_size() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    // Try to initialize with wrong region size
    assert!(matches!(
        Io::<'_, _, MAX_HEADS>::init(&mut mem_io, DATA_SIZE + 1, REGION_COUNT),
        Err(IoError::InvalidRegionSize)
    ));
}

#[test]
fn test_invalid_region_count() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    // Try to initialize with wrong region count
    assert!(matches!(
        Io::<'_, _, MAX_HEADS>::init(&mut mem_io, DATA_SIZE, REGION_COUNT + 1),
        Err(IoError::InvalidRegionCount)
    ));
}

#[test]
fn test_allocate_region() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::<'_, _, MAX_HEADS>::init(&mut mem_io, DATA_SIZE, REGION_COUNT)
        .expect("Failed to initialize Io");

    // Should be able to allocate first region
    let collection_id = CollectionId(1);
    let region1 = io
        .allocate_region(collection_id)
        .expect("Failed to allocate first region");
    assert_eq!(region1, MemRegionAddress(1)); // First region after root at 0

    // Should be able to allocate second region
    let region2 = io
        .allocate_region(collection_id)
        .expect("Failed to allocate second region");
    assert_eq!(region2, MemRegionAddress(2));

    // Should be able to allocate third region
    let region3 = io
        .allocate_region(collection_id)
        .expect("Failed to allocate third region");
    assert_eq!(region3, MemRegionAddress(3));

    // Should fail when storage is full
    assert!(matches!(
        io.allocate_region(collection_id),
        Err(IoError::StorageFull)
    ));
}

#[test]
fn test_write_region_header() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::<'_, _, MAX_HEADS>::init(&mut mem_io, DATA_SIZE, REGION_COUNT)
        .expect("Failed to initialize Io");

    // Allocate a region
    let collection_id = CollectionId(1);
    let region = io
        .allocate_region(collection_id)
        .expect("Failed to allocate region");

    // Write header
    let collection_type = CollectionType::Channel;
    let collection_sequence = MemCollectionSequence::first();
    io.write_region_header(region, collection_id, collection_type, collection_sequence)
        .expect("Failed to write header");

    let storage_sequence = io.storage_sequence;
    // Verify header was written correctly
    let header = mem_io
        .get_region_header(region)
        .expect("Failed to get header");
    assert_eq!(header.collection_id, collection_id);
    assert_eq!(header.collection_type, collection_type);
    assert_eq!(header.collection_sequence, collection_sequence);
    assert_eq!(header.sequence, storage_sequence);
    assert_eq!(header.heads.len(), 2);
}

#[test]
fn test_write_region_header_sequence_increments() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::<'_, _, MAX_HEADS>::init(&mut mem_io, DATA_SIZE, REGION_COUNT)
        .expect("Failed to initialize Io");

    let collection_id = CollectionId(1);
    let region = io
        .allocate_region(collection_id)
        .expect("Failed to allocate region");

    // Write header multiple times and verify sequence increments
    for expected_sequence in 2..5 {
        let collection_sequence = MemCollectionSequence::first();
        io.write_region_header(
            region,
            collection_id,
            CollectionType::Channel,
            collection_sequence,
        )
        .expect("Failed to write header");

        let header = io
            .backing
            .get_region_header(region)
            .expect("Failed to get header");
        assert_eq!(header.sequence, MemStorageSequence(expected_sequence));
    }
}
