use super::*;
use crate::io::mem_io::MemIo;
use crate::io::{Io, IoError};

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
    let wal =
        Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id)
            .expect("Failed to create WAL");

    assert_eq!(wal.collection_id, collection_id);
    assert_eq!(wal.next_entry, 0);
    assert_eq!(wal.head, wal.region);
    assert_eq!(
        wal.collection_sequence,
        <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::CollectionSequence::first()
    );

    // Verify the region was allocated and header written correctly
    let header = mem_io
        .get_region_header(wal.region)
        .expect("Failed to get header");
    assert_eq!(header.collection_id, collection_id);
    assert_eq!(header.collection_type, CollectionType::Wal);
    assert_eq!(
        header.collection_sequence,
        <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::CollectionSequence::first()
    );
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
    let _result =
        Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id);
    let result =
        Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id);
    assert!(matches!(result, Err(IoError::StorageFull)));
}

#[test]
fn test_wal_write_read_single_region() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;
    const BUFFER_SIZE: usize = 256;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    let collection_id = CollectionId(1);

    let mut wal =
        Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id)
            .expect("Failed to create WAL");

    let mut write_buffer = [0u8; BUFFER_SIZE];
    let mut read_buffer = [0u8; BUFFER_SIZE];

    // Write some test data
    let test_data = b"Hello World!";
    wal.write(&mut io, CollectionType::Wal, test_data, &mut write_buffer)
        .expect("Failed to write data");

    // Read it back
    let cursor = wal.get_cursor();
    let WalRead::Record { next: _, record } = wal
        .read(&mut io, cursor, &mut read_buffer)
        .expect("Failed to read data")
    else {
        panic!("No Record Found")
    };

    assert_eq!(record.collection_type, CollectionType::Wal);
    assert_eq!(record.data, test_data);
}

#[test]
fn test_wal_write_read_multiple_regions() {
    const DATA_SIZE: usize = 256; // Small size to force multiple regions
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 8;
    const BUFFER_SIZE: usize = 64;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    let collection_id = CollectionId(1);

    let mut wal =
        Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id)
            .expect("Failed to create WAL");

    let mut write_buffer = [0u8; BUFFER_SIZE];
    let mut read_buffer = [0u8; BUFFER_SIZE];

    // Write multiple entries that will span regions
    let test_data = [
        b"First entry that's quite long to help fill up space",
        b"Second entry also taking up space in the log.......",
        b"Third entry that should push us into another region",
        b"Fourth entry to really make sure we span regions...",
    ];

    // Write all entries
    for data in test_data {
        wal.write(&mut io, CollectionType::Wal, data, &mut write_buffer)
            .expect("Failed to write data");
    }

    // Read back all entries
    let mut cursor = wal.get_cursor();
    for expected_data in &test_data {
        let WalRead::Record { next, record } = wal
            .read(&mut io, cursor, &mut read_buffer)
            .expect("Failed to read data")
        else {
            panic!("No data found");
        };

        assert_eq!(record.collection_type, CollectionType::Wal);
        assert_eq!(record.data, *expected_data);

        cursor = next;
    }

    // Verify we've read everything
    match wal.read(&mut io, cursor, &mut read_buffer).unwrap() {
        WalRead::Commit { next: _ } => panic!("Got unexpected Commit"),
        WalRead::EndOfRegion { next: _ } => panic!("Unexpected EndOfRegion"),
        WalRead::Record { next: _, record: _ } => panic!("Got unexpected Record"),
        WalRead::EndOfWAL => (), // Expeceted
    }
}

#[test]
fn test_wal_write_fails_when_full() {
    const DATA_SIZE: usize = 56; // Very small size to test filling up
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 2; // Limited regions
    const BUFFER_SIZE: usize = 128;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    let collection_id = CollectionId(1);

    let mut wal =
        Wal::<DATA_SIZE, MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new(&mut io, collection_id)
            .expect("Failed to create WAL");

    let mut write_buffer = [0u8; BUFFER_SIZE];

    // Write data until we run out of space
    let test_data = b"This is some test data to fill up the WAL";
    let mut write_count = 0;

    while wal
        .write(&mut io, CollectionType::Wal, test_data, &mut write_buffer)
        .is_ok()
    {
        write_count += 1;
    }

    assert!(write_count > 0, "Should have written at least once");

    // Verify we get storage full error
    let result = wal.write(&mut io, CollectionType::Wal, test_data, &mut write_buffer);
    assert!(matches!(result, Err(IoError::StorageFull)));
}
