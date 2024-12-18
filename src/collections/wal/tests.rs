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
        Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id)
            .expect("Failed to create WAL");

    assert_eq!(wal.collection_id, collection_id);
    assert_eq!(wal.tail_next_entry_offset, 0);
    assert_eq!(wal.tail_region, wal.head_region);
    assert_eq!(
        wal.head_sequence,
        <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::CollectionSequence::first()
    );

    // Verify the region was allocated and header written correctly
    let header = mem_io
        .get_region_header(wal.head_region)
        .expect("Failed to get header");
    assert_eq!(header.collection_id, collection_id);
    assert_eq!(header.collection_type, CollectionType::Wal);
    assert_eq!(
        header.collection_sequence,
        <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::CollectionSequence::first()
    );
}

#[test]
fn test_wal_creation_and_open() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;
    const BUFFER_SIZE: usize = 64;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    let collection_id = CollectionId(1);

    // Should be able to create a new WAL
    let wal =
        Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id)
            .expect("Failed to create WAL");

    let region = wal.head_region;
    let mut read_buffer = [0u8; BUFFER_SIZE];

    // Should be able to create a new WAL
    let wal = Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::open::<MAX_HEADS>(
        &mut io,
        region,
        &mut read_buffer,
    )
    .expect("Failed to create WAL");

    assert_eq!(wal.collection_id, collection_id);
    assert_eq!(wal.tail_next_entry_offset, 0);
    assert_eq!(wal.tail_region, wal.head_region);
    assert_eq!(
        wal.head_sequence,
        <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::CollectionSequence::first()
    );

    // Verify the region was allocated and header written correctly
    let header = mem_io
        .get_region_header(wal.head_region)
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
        Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id);
    let result =
        Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id);
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
        Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id)
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
        Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id)
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
        loop {
            match wal
                .read(&mut io, cursor, &mut read_buffer)
                .expect("Read failed")
            {
                WalRead::Record { next, record } => {
                    assert_eq!(record.collection_type, CollectionType::Wal);
                    assert_eq!(record.data, *expected_data);
                    cursor = next;
                    break;
                }
                WalRead::Commit {
                    next, ..
                } => {
                    cursor = next;
                }
                WalRead::EndOfRegion { next } => {
                    cursor = next;
                }
                WalRead::EndOfWAL => {
                    panic!("End of wal. No data found");
                }
            }
        }
    }

    // Verify we've read everything
    match wal.read(&mut io, cursor, &mut read_buffer).unwrap() {
        WalRead::Commit { .. } => panic!("Got unexpected Commit"),
        WalRead::EndOfRegion { next: _ } => panic!("Unexpected EndOfRegion"),
        WalRead::Record { next: _, record: _ } => panic!("Got unexpected Record"),
        WalRead::EndOfWAL => (), // Expeceted
    }
}

#[test]
fn test_wal_write_fails_when_full() {
    const DATA_SIZE: usize = 92; // Very small size to test filling up
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 2; // Limited regions
    const BUFFER_SIZE: usize = 128;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    let collection_id = CollectionId(1);

    let mut wal =
        Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id)
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

#[test]
fn test_wal_commit() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4; 

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");
  
    let collection_id = CollectionId(1);
  
    let mut wal =
    Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id)
        .expect("Failed to create WAL");

    let mut write_buffer = [0u8; 1024];
    let mut read_buffer = [0u8; 1024];

    // Write some initial data
    let initial_data = b"Initial entry before commit";
    wal.write(&mut io, CollectionType::Wal, initial_data, &mut write_buffer)
        .expect("Failed to write initial data");

    // Get cursor for commit point
    let mut commit_cursor = wal.get_cursor();

    if let WalRead::Record{next, ..} = wal.read(&mut io, commit_cursor, &mut read_buffer).expect("Read failed") {
        commit_cursor = next;
    } else {
        panic!("No record found");
    }
    // Write more data after commit point
    let post_commit_data = b"Entry after commit point";
    wal.write(&mut io, CollectionType::Wal, post_commit_data, &mut write_buffer)
        .expect("Failed to write post-commit data");

    // Perform commit
    wal.commit(&mut io, commit_cursor, &mut write_buffer)
        .expect("Failed to commit");

    // Verify reading from start
    let mut cursor = wal.get_cursor();

    let mut found_commit = false;
    loop {
        match wal.read(&mut io, cursor, &mut read_buffer).expect("Read failed") {
            WalRead::Record { next, record } => {
                assert_eq!(record.data, post_commit_data);
                cursor = next;
            }
            WalRead::Commit { next, .. } => {
                found_commit = true;
                cursor = next;
            }
            WalRead::EndOfRegion { next } => {
                cursor = next;
            }
            WalRead::EndOfWAL => break,
        }
    }

    assert!(found_commit, "Commit record not found");
}

#[test]
fn test_wal_open_with_commits() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");
  
    let collection_id = CollectionId(1);
  
    
    let region;
    let mut write_buffer = [0u8; 1024];
    let mut read_buffer = [0u8; 1024];

    // Create and populate initial WAL
    {
        let mut wal =
        Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id)
            .expect("Failed to create WAL");
        region = wal.region();

        // Write initial data
        wal.write(&mut io, CollectionType::Wal, b"First entry", &mut write_buffer)
            .expect("Failed to write");

        let mut commit_point = wal.get_cursor();

        if let WalRead::Record{next, ..} = wal.read(&mut io, commit_point, &mut read_buffer).expect("Read failed") {
            commit_point = next;
        } else {
            panic!("No record found");
        }
        

        wal.write(&mut io, CollectionType::Wal, b"Second entry", &mut write_buffer)
            .expect("Failed to write");

        // Verify we can read the committed data
        let mut cursor = wal.get_cursor();
        let mut entries_found = 0;
        let mut commit_found = false;

        loop {
            match wal.read(&mut io, cursor, &mut read_buffer).expect("Read failed") {
                WalRead::Record { next, .. } => {
                    entries_found += 1;
                    cursor = next;
                }
                WalRead::Commit { .. } => {
                    commit_found = true;
                    break;
                }
                WalRead::EndOfRegion { next } => {
                    cursor = next;
                }
                WalRead::EndOfWAL => break,
            }
        }

        assert_eq!(entries_found, 2, "Expected 2 entries before commit");
        assert!(!commit_found, "Expected to find commit record");


        wal.commit(&mut io, commit_point, &mut write_buffer)
            .expect("Failed to commit");
    }

    // Reopen WAL and verify state
    let mut wal =
    Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::open::<MAX_HEADS>(&mut io, region, &mut read_buffer)
        .expect("Failed to create WAL");

    // Verify we can read the committed data
    let mut cursor = wal.get_cursor();
    let mut entries_found = 0;
    let mut commit_found = false;

    loop {
        match wal.read(&mut io, cursor, &mut read_buffer).expect("Read failed") {
            WalRead::Record { next, .. } => {
                entries_found += 1;
                cursor = next;
            }
            WalRead::Commit { .. } => {
                commit_found = true;
                break;
            }
            WalRead::EndOfRegion { next } => {
                cursor = next;
            }
            WalRead::EndOfWAL => break,
        }
    }

    assert_eq!(entries_found, 1, "Expected 2 entries before commit");
    assert!(commit_found, "Expected to find commit record");
}

#[test]
fn test_wal_sequence_handling() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    let mut io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");
  
    let collection_id = CollectionId(1);
  
    let mut wal =
    Wal::<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>::new::<MAX_HEADS>(&mut io, collection_id)
        .expect("Failed to create WAL");
    let mut write_buffer = [0u8; 1024];

    // Fill up first region to force sequence increment
    let large_data = [b'X'; 400];
    for _ in 0..3 {
        wal.write(&mut io, CollectionType::Wal, &large_data, &mut write_buffer)
            .expect("Failed to write data");
    }

    assert!(wal.head_sequence < wal.tail_sequence, 
        "Collection sequence should increment after region transition");
}
