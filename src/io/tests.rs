use super::*;
extern crate alloc;

use mem_io::MemIo;

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

    let _io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");
}
#[test]
fn test_double_init_fails() {
    const DATA_SIZE: usize = 1024;
    const MAX_HEADS: usize = 8;
    const REGION_COUNT: usize = 4;

    let mut mem_io =
        MemIo::<DATA_SIZE, MAX_HEADS, REGION_COUNT>::new().expect("Failed to create MemIo");

    // First init should succeed
    let _io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    // Second init should fail with AlreadyInitialized
    assert!(matches!(
        Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT),
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
        Io::open(&mut mem_io),
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
    let _io = Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT).expect("Failed to initialize Io");

    // Should be able to open initialized storage
    let _io = Io::open(&mut mem_io).expect("Failed to open Io");
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
        Io::init(&mut mem_io, DATA_SIZE + 1, REGION_COUNT),
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
        Io::init(&mut mem_io, DATA_SIZE, REGION_COUNT + 1),
        Err(IoError::InvalidRegionCount)
    ));
}
