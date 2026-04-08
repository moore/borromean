use super::*;
use ::core::mem::size_of;
use heapless::Vec as HeaplessVec;

fn storage_with_max_seen_sequence() -> (MockFlash<128, 4, 256>, StorageWorkspace<128>, Storage<8, 4>)
{
    let mut flash = MockFlash::<128, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    Storage::<8, 4>::format::<128, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    let header = Header {
        sequence: u64::MAX,
        collection_id: CollectionId(0),
        collection_format: WAL_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    flash.write_region(0, 0, &header_bytes).unwrap();

    let storage = Storage::<8, 4>::open::<128, 4, _>(&mut flash, &mut workspace).unwrap();
    (flash, workspace, storage)
}

//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-001` Integer arithmetic that can affect storage
//# layout, region addressing, WAL offsets, lengths, indexes,
//# capacities, or sequence advancement MUST use checked arithmetic or
//# an equivalent construction that makes overflow and underflow
//# impossible by construction.
#[test]
fn boundary_sensitive_storage_and_map_lengths_stay_in_range() {
    assert_eq!(CollectionId(u64::MAX).increment(), None);

    let metadata = StorageMetadata::new(128, 4, 1, 8, 0xff, 0xa5).unwrap();
    let record_area_offset = metadata.wal_record_area_offset().unwrap();
    assert!(record_area_offset >= Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN);
    assert_eq!(
        record_area_offset % usize::try_from(metadata.wal_write_granule).unwrap(),
        0
    );

    let mut map_buffer = [0u8; 64];
    let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(7), &mut map_buffer).unwrap();
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
fn arithmetic_boundary_failures_surface_explicit_error_variants() {
    let mut flash = MockFlash::<64, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<64>::new();
    let storage =
        Storage::<8, 4>::format::<64, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    let oversized_payload = [0u8; 64];
    assert!(matches!(
        storage.runtime().write_committed_region::<64, 4, _>(
            &mut flash,
            1,
            CollectionId(9),
            MAP_REGION_V1_FORMAT,
            &oversized_payload,
        ),
        Err(StorageRuntimeError::CommittedRegionTooLarge {
            payload_len,
            capacity,
        }) if payload_len == oversized_payload.len() && capacity < payload_len
    ));

    let mut map_buffer = [0u8; 64];
    let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(9), &mut map_buffer).unwrap();
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
fn sequence_advancement_stops_at_the_maximum_value_instead_of_wrapping() {
    assert_eq!(CollectionId(u64::MAX).increment(), None);

    let (mut flash, mut workspace, storage) = storage_with_max_seen_sequence();
    assert_eq!(storage.max_seen_sequence(), u64::MAX);
    assert_eq!(
        storage.runtime().write_committed_region::<128, 4, _>(
            &mut flash,
            1,
            CollectionId(11),
            MAP_REGION_V1_FORMAT,
            &[1, 2, 3],
        ),
        Err(StorageRuntimeError::WalRotationRequired)
    );

    let reopened = Storage::<8, 4>::open::<128, 4, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(reopened.max_seen_sequence(), u64::MAX);
    assert_eq!(reopened.wal_head(), 0);
}

//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-004` Conversions between integer widths that may
//# lose information MUST be checked and MUST fail explicitly if the
//# value is out of range for the destination type.
#[test]
fn lossy_integer_width_conversions_fail_with_explicit_map_errors() {
    let mut map_buffer = [0u8; 70_000];
    let mut map =
        LsmMap::<u16, HeaplessVec<u8, 66_000>, 8>::new(CollectionId(12), &mut map_buffer).unwrap();
    let large_value = HeaplessVec::<u8, 66_000>::from_slice(&[0u8; 66_000]).unwrap();

    assert!(matches!(
        map.set(1, large_value),
        Err(MapError::SerializationError)
    ));
}
