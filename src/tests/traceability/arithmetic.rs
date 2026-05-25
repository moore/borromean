use super::*;
use heapless::Vec as HeaplessVec;
use std::mem::size_of;

fn flash_with_max_seen_sequence() -> MockFlash<128, 4, 256> {
    let mut flash = MockFlash::<128, 4, 256>::new(0xff);
    Storage::<_, 128, 4, 8, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    let header = Header {
        sequence: u64::MAX,
        collection_id: CollectionId(0),
        collection_format: WAL_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    flash.write_region(0, 0, &header_bytes).unwrap();

    flash
}

//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-001` Integer arithmetic that can affect storage
//# layout, region addressing, WAL offsets, lengths, indexes,
//# capacities, or sequence advancement MUST use checked arithmetic or
//# an equivalent construction that makes overflow and underflow
//# impossible by construction.
#[test]
fn requirement_boundary_sensitive_storage_and_map_lengths_stay_in_range() {
    assert_eq!(CollectionId(u64::MAX).increment(), None);

    let metadata = StorageMetadata::new(128, 4, 1, 8, 0xff, 0xa5).unwrap();
    let record_area_offset = metadata.wal_record_area_offset().unwrap();
    assert!(record_area_offset >= Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN);
    assert_eq!(
        record_area_offset % usize::try_from(metadata.wal_write_granule).unwrap(),
        0
    );

    let mut map_buffer = [0u8; 64];
    let mut map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(7),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set(1, 10).unwrap();
    map.set(2, 20).unwrap();

    let snapshot_len = map.snapshot_len().unwrap();
    assert!(snapshot_len < 64);
    assert_eq!(map.region_len().unwrap(), snapshot_len + size_of::<u32>());
}

//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-002` If such arithmetic cannot be proven safe by
//# construction and a checked operation fails, the implementation MUST
//# return an explicit error rather than wrap, saturate, or silently
//# truncate.
#[test]
fn requirement_arithmetic_boundary_failures_surface_explicit_error_variants() {
    let mut flash = MockFlash::<64, 4, 256>::new(0xff);
    let storage = Storage::<_, 64, 4, 8, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    let oversized_payload = [0u8; 64];
    let mut runtime = storage.into_runtime();
    assert!(matches!(
        runtime.write_committed_region::<64, 4, _>(
            &mut flash,
            1,
            CollectionId(9),
            MAP_REGION_V2_FORMAT,
            &oversized_payload,
        ),
        Err(StorageRuntimeError::CommittedRegionTooLarge {
            payload_len,
            capacity,
        }) if payload_len == oversized_payload.len() && capacity < payload_len
    ));

    let mut map_buffer = [0u8; 64];
    let mut map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(9),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    let mut malformed_region = [0u8; 8];
    malformed_region[..size_of::<u32>()].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        map.load_region(&malformed_region),
        Err(MapError::SerializationError)
    ));
}

//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-003` The implementation MUST NOT rely on wrapping
//# integer behavior for correctness unless a future disk-format
//# requirement explicitly defines modulo arithmetic for that field.
#[test]
fn requirement_sequence_advancement_stops_at_the_maximum_value_instead_of_wrapping() {
    assert_eq!(CollectionId(u64::MAX).increment(), None);

    let mut flash = flash_with_max_seen_sequence();
    let storage =
        Storage::<_, 128, 4, 8, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(storage.max_seen_sequence(), u64::MAX);
    let mut runtime = storage.into_runtime();
    assert_eq!(
        runtime.write_committed_region::<128, 4, _>(
            &mut flash,
            1,
            CollectionId(11),
            MAP_REGION_V2_FORMAT,
            &[1, 2, 3],
        ),
        Err(StorageRuntimeError::WalRotationRequired)
    );

    let reopened =
        Storage::<_, 128, 4, 8, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.max_seen_sequence(), u64::MAX);
    assert_eq!(reopened.wal_head(), 0);
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# `RING-IMPL-REGRESSION-135` Entry reference serialization MUST preserve 32-bit offsets for
//# entries beyond 64 KiB of frontier storage.
#[test]
fn requirement_large_map_entry_offsets_round_trip_with_32_bit_refs() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(run_large_map_entry_offsets_round_trip_with_32_bit_refs)
        .unwrap()
        .join()
        .unwrap();
}

fn run_large_map_entry_offsets_round_trip_with_32_bit_refs() {
    let mut map_buffer = [0u8; 70_000];
    let mut map = MapFrontier::<u16, HeaplessVec<u8, 66_000>, 8>::new(
        CollectionId(12),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    let large_value = HeaplessVec::<u8, 66_000>::from_slice(&[0u8; 66_000]).unwrap();

    map.set(1, large_value).unwrap();
    assert!(map.get_frontier(&1).unwrap().is_some());
}
