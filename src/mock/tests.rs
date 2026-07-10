use super::*;
use crate::disk::{FreeSpaceEntry, FreeSpaceRegionPrologue, StorageMetadata, FREE_SPACE_V2_FORMAT};

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-001` Region starts and region sizes MUST be aligned to
//# the backing flash erase-block size so every region can be erased
//# independently.
#[test]
fn requirement_mock_flash_regions_occupy_fixed_independent_storage_spans() {
    let mut flash = MockFlash::<16, 3, 16>::new(0xff);
    flash.write_region(0, 0, &[0x11, 0x22]).unwrap();
    flash.write_region(1, 0, &[0x33, 0x44]).unwrap();

    let mut region_zero_prefix = [0u8; 2];
    let mut region_one_prefix = [0u8; 2];
    flash.read_storage(16, &mut region_zero_prefix).unwrap();
    flash.read_storage(32, &mut region_one_prefix).unwrap();
    assert_eq!(region_zero_prefix, [0x11, 0x22]);
    assert_eq!(region_one_prefix, [0x33, 0x44]);

    flash.erase_region(0).unwrap();
    flash.read_storage(16, &mut region_zero_prefix).unwrap();
    flash.read_storage(32, &mut region_one_prefix).unwrap();
    assert_eq!(region_zero_prefix, [0xff, 0xff]);
    assert_eq!(region_one_prefix, [0x33, 0x44]);
}

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-PRE-001` Backing storage MUST be writable and erasable at region
//# granularity.
#[test]
fn requirement_mock_flash_supports_region_granularity_write_and_erase() {
    let mut flash = MockFlash::<8, 2, 16>::new(0xff);
    flash.write_region(0, 0, &[1, 2, 3, 4]).unwrap();

    let mut buffer = [0u8; 8];
    flash
        .read_region(0, 0, buffer.len(), |bytes| buffer.copy_from_slice(bytes))
        .unwrap();
    assert_eq!(buffer[..4], [1, 2, 3, 4]);

    flash.erase_region(0).unwrap();
    flash
        .read_region(0, 0, buffer.len(), |bytes| buffer.copy_from_slice(bytes))
        .unwrap();
    assert_eq!(buffer, [0xff; 8]);
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-037` Mock flash metadata read/write operations MUST persist metadata and
//# log write/read metadata operations in order.
#[test]
fn requirement_metadata_operations_are_logged() {
    let mut flash = MockFlash::<64, 4, 8>::new(0xff);
    let metadata = StorageMetadata::new(64, 4, 1, 4, 0xff, 0xa5).unwrap();

    flash.write_metadata(metadata).unwrap();
    let read_back = flash.read_metadata().unwrap();

    assert_eq!(read_back, Some(metadata));
    assert_eq!(
        flash.operations(),
        &[MockOperation::WriteMetadata, MockOperation::ReadMetadata]
    );
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-038` Mock flash storage reads MUST span metadata and data regions by
//# absolute offset and reject out-of-bounds reads.
#[test]
fn requirement_read_storage_spans_metadata_and_data_regions_with_bounds_checks() {
    let mut flash = MockFlash::<4, 2, 16>::new(0xff);
    flash.write_region(0, 0, &[0x10, 0x11, 0x12, 0x13]).unwrap();
    flash.write_region(1, 0, &[0x20, 0x21, 0x22, 0x23]).unwrap();

    let mut buffer = [0u8; 6];
    flash.read_storage(2, &mut buffer).unwrap();
    assert_eq!(buffer, [0xff, 0xff, 0x10, 0x11, 0x12, 0x13]);

    flash.read_storage(6, &mut buffer).unwrap();
    assert_eq!(buffer, [0x12, 0x13, 0x20, 0x21, 0x22, 0x23]);

    assert_eq!(
        flash.read_storage(12, &mut [0u8; 1]),
        Err(MockError::OutOfBounds)
    );
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-039` Mock flash metadata writes MUST fail without changing metadata when
//# the metadata region is smaller than encoded StorageMetadata.
#[test]
fn requirement_write_metadata_requires_metadata_region_large_enough() {
    let mut flash = MockFlash::<8, 4, 8>::new(0xff);
    let metadata = StorageMetadata::new(8, 4, 1, 4, 0xff, 0xa5).unwrap();

    assert_eq!(flash.write_metadata(metadata), Err(MockError::OutOfBounds));
    assert_eq!(flash.metadata(), None);
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-040` Mock flash metadata writes MUST succeed when the metadata region
//# exactly matches encoded StorageMetadata and persist decodable metadata.
#[test]
fn requirement_write_metadata_accepts_exact_metadata_region_size() {
    let mut flash = MockFlash::<{ StorageMetadata::ENCODED_LEN }, 4, 8>::new(0xff);
    let metadata =
        StorageMetadata::new(StorageMetadata::ENCODED_LEN as u32, 4, 1, 4, 0xff, 0xa5).unwrap();

    flash.write_metadata(metadata).unwrap();

    assert_eq!(flash.metadata(), Some(&metadata));
    assert_eq!(
        StorageMetadata::decode(&flash.metadata_region[..]).unwrap(),
        metadata
    );
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-041` FlashIo metadata operations on MockFlash MUST delegate to mock
//# metadata storage and return the persisted metadata.
#[test]
fn requirement_flash_io_trait_write_metadata_delegates_to_mock_flash() {
    let mut flash = MockFlash::<64, 4, 8>::new(0xff);
    let metadata = StorageMetadata::new(64, 4, 1, 4, 0xff, 0xa5).unwrap();

    crate::FlashIo::write_metadata(&mut flash, metadata).unwrap();

    assert_eq!(flash.metadata(), Some(&metadata));
    assert_eq!(
        crate::FlashIo::read_metadata(&mut flash).unwrap(),
        Some(metadata)
    );
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-042` Mock flash erase/write/read/sync operations MUST perform the
//# operation and log each operation with region, offset, and length details.
#[test]
fn requirement_erase_write_read_and_sync_are_logged() {
    let mut flash = MockFlash::<16, 2, 16>::new(0xff);

    flash.write_region(1, 4, b"bor").unwrap();
    let mut buffer = [0u8; 3];
    flash
        .read_region(1, 4, buffer.len(), |bytes| buffer.copy_from_slice(bytes))
        .unwrap();
    flash.erase_region(1).unwrap();
    flash.sync().unwrap();

    assert_eq!(&buffer, b"bor");
    assert_eq!(
        flash.operations(),
        &[
            MockOperation::WriteRegion {
                region_index: 1,
                offset: 4,
                len: 3,
            },
            MockOperation::ReadRegion {
                region_index: 1,
                offset: 4,
                len: 3,
            },
            MockOperation::EraseRegion { region_index: 1 },
            MockOperation::Sync,
        ]
    );
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-157` Mock flash operation logging controls MUST
//# clear the existing log when disabled, suppress new operation log entries
//# while disabled, and resume logging when re-enabled without suppressing the
//# underlying flash operation.
#[test]
fn requirement_operation_logging_toggle_clears_and_suppresses_logs() {
    let mut flash = MockFlash::<16, 2, 16>::new(0xff);
    flash.write_region(1, 0, &[1]).unwrap();
    assert_eq!(flash.operations().len(), 1);

    flash.set_operation_logging(false);
    assert!(flash.operations().is_empty());
    flash.write_region(1, 1, &[2]).unwrap();
    flash.sync().unwrap();
    assert!(flash.operations().is_empty());
    assert_eq!(&flash.region_bytes(1).unwrap()[..2], &[1, 2]);

    flash.set_operation_logging(true);
    flash.erase_region(1).unwrap();
    assert_eq!(
        flash.operations(),
        &[MockOperation::EraseRegion { region_index: 1 }]
    );
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-043` Erasing a mock flash region MUST restore every byte in that region to
//# the erased byte.
#[test]
fn requirement_erase_restores_erased_bytes() {
    let mut flash = MockFlash::<8, 1, 8>::new(0xff);
    flash.write_region(0, 0, &[1, 2, 3, 4]).unwrap();
    flash.erase_region(0).unwrap();

    let mut buffer = [0u8; 8];
    flash
        .read_region(0, 0, buffer.len(), |bytes| buffer.copy_from_slice(bytes))
        .unwrap();
    assert_eq!(buffer, [0xff; 8]);
}

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-PRE-007` `transaction_log_count >= 1`.
#[test]
fn requirement_format_empty_store_rejects_too_few_regions() {
    let mut flash = MockFlash::<96, 2, 16>::new(0xff);
    let error = flash.format_empty_store(1, 8, 0xa5).unwrap_err();
    assert_eq!(
        error,
        MockFormatError::InsufficientRegions {
            region_count: 2,
            min_free_regions: 1,
        }
    );

    let mut tiny = MockFlash::<7, 3, 16>::new(0xff);
    assert_eq!(
        tiny.format_empty_store(1, 8, 0xa5),
        Err(MockFormatError::RegionSizeTooSmall {
            region_size: 7,
            min_region_size: 63,
        })
    );
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-044` Formatting an empty mock store MUST accept the exact minimum region
//# count and persist matching metadata.
#[test]
fn requirement_format_empty_store_accepts_exact_minimum_region_count() {
    let mut flash = MockFlash::<96, 3, 16>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    assert_eq!(metadata.region_count, 3);
    assert_eq!(flash.metadata(), Some(&metadata));

    let mut exact_size = MockFlash::<67, 3, 16>::new(0xff);
    let exact_size_metadata = exact_size.format_empty_store(1, 1, 0xa5).unwrap();

    assert_eq!(exact_size_metadata.region_size, 67);
    assert_eq!(exact_size.metadata(), Some(&exact_size_metadata));
}

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-PRE-003` Region `0` MUST be reserved as the initial main WAL region.
#[test]
fn requirement_format_empty_store_reserves_region_zero_as_initial_wal_region() {
    let mut flash = MockFlash::<96, 4, 32>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();

    let header = Header::decode(&flash.region_bytes(0).unwrap()[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, WAL_V1_FORMAT);
}

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-003` Initialize regions
//# `1..=initial_free_space_metadata_region_count` as a linked `free_space_v2` metadata
//# chain. In chain order, their headers MUST use strictly increasing `sequence` values
//# starting at `0`. The chain's `FreeSpaceRegionPrologue` values MUST set
//# `allocation_head`, `ready_boundary`, and `append_tail` so every non-reserved data region
//# after the metadata chain is in the ready range. Its `FreeSpaceEntry` arrays MUST list
//# those ready regions in ascending region-index order. Sync every initialized free-space
//# metadata region.
#[test]
fn requirement_format_empty_store_initializes_region_zero_with_wal_header_and_prologue() {
    let mut flash = MockFlash::<96, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    let header = Header::decode(&flash.region_bytes(0).unwrap()[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(header.sequence, 1);
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, WAL_V1_FORMAT);

    let prologue_offset = Header::ENCODED_LEN;
    let prologue_end = prologue_offset + WalRegionPrologue::ENCODED_LEN;
    let prologue = WalRegionPrologue::decode(
        &flash.region_bytes(0).unwrap()[prologue_offset..prologue_end],
        metadata.region_count,
    )
    .unwrap();
    assert_eq!(prologue.log_head_region_index, 0);

    assert!(flash.operations().contains(&MockOperation::WriteRegion {
        region_index: 0,
        offset: 0,
        len: metadata.wal_record_area_offset().unwrap(),
    }));
    assert_eq!(flash.operations().last(), Some(&MockOperation::Sync));
}

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-POST-003` The free-space collection MUST contain every region after
//# the initial free-space metadata chain in ascending region-index order.
#[test]
fn requirement_format_empty_store_populates_free_space_collection_in_ascending_order() {
    let mut flash = MockFlash::<96, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    assert_eq!(flash.metadata(), Some(&metadata));

    let metadata_region = flash.region_bytes(1).unwrap();
    let header = Header::decode(&metadata_region[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, FREE_SPACE_V2_FORMAT);

    let prologue_offset = Header::ENCODED_LEN;
    let entries_offset = prologue_offset + FreeSpaceRegionPrologue::ENCODED_LEN;
    let prologue =
        FreeSpaceRegionPrologue::decode(&metadata_region[prologue_offset..entries_offset], 4)
            .unwrap();
    assert_eq!(prologue.allocation_head.region_index, 1);
    assert_eq!(prologue.allocation_head.entry_index, 0);
    assert_eq!(prologue.ready_boundary.entry_index, 2);
    assert_eq!(prologue.append_tail.entry_index, 2);
    assert_eq!(prologue.next_metadata_region, None);
    assert_eq!(prologue.entry_count, 2);

    let first_entry =
        FreeSpaceEntry::decode(&metadata_region[entries_offset..entries_offset + 4], 4).unwrap();
    let second_entry =
        FreeSpaceEntry::decode(&metadata_region[entries_offset + 4..entries_offset + 8], 4)
            .unwrap();
    assert_eq!(first_entry.region_index, 2);
    assert_eq!(second_entry.region_index, 3);
    assert!(metadata_region[entries_offset + 8..]
        .iter()
        .all(|byte| *byte == 0xff));
}

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-003` Initialize regions
//# `1..=initial_free_space_metadata_region_count` as a linked
//# `free_space_v2` metadata chain.
#[test]
fn requirement_format_empty_store_builds_multi_region_free_space_metadata_chain() {
    let mut flash = MockFlash::<67, 6, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 1, 0xa5).unwrap();
    assert_eq!(metadata.region_size, 67);
    assert_eq!(metadata.region_count, 6);

    let wal_region = flash.region_bytes(0).unwrap();
    let wal_header = Header::decode(&wal_region[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(wal_header.sequence, 3);
    assert_eq!(wal_header.collection_id, CollectionId(0));
    assert_eq!(wal_header.collection_format, WAL_V1_FORMAT);
    let wal_prologue = WalRegionPrologue::decode(
        &wal_region[Header::ENCODED_LEN..Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN],
        metadata.region_count,
    )
    .unwrap();
    assert_eq!(
        wal_prologue.allocation_head,
        FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        }
    );
    assert_eq!(
        wal_prologue.ready_boundary,
        FreeQueuePosition {
            region_index: 3,
            entry_index: 0,
        }
    );
    assert_eq!(wal_prologue.append_tail, wal_prologue.ready_boundary);

    let prologue_offset = Header::ENCODED_LEN;
    let entries_offset = prologue_offset + FreeSpaceRegionPrologue::ENCODED_LEN;
    for (metadata_region, expected_next, expected_entry_count, expected_entry) in [
        (1, Some(2), 1, Some(4)),
        (2, Some(3), 1, Some(5)),
        (3, None, 0, None),
    ] {
        let bytes = flash.region_bytes(metadata_region).unwrap();
        let header = Header::decode(&bytes[..Header::ENCODED_LEN]).unwrap();
        assert_eq!(header.sequence, u64::from(metadata_region - 1));
        assert_eq!(header.collection_id, CollectionId(0));
        assert_eq!(header.collection_format, FREE_SPACE_V2_FORMAT);
        let prologue =
            FreeSpaceRegionPrologue::decode(&bytes[prologue_offset..entries_offset], 6).unwrap();
        assert_eq!(prologue.next_metadata_region, expected_next);
        assert_eq!(prologue.entry_count, expected_entry_count);
        assert_eq!(prologue.ready_boundary, wal_prologue.ready_boundary);
        assert_eq!(prologue.append_tail, wal_prologue.append_tail);
        if let Some(expected_entry) = expected_entry {
            let entry =
                FreeSpaceEntry::decode(&bytes[entries_offset..entries_offset + 4], 6).unwrap();
            assert_eq!(entry.region_index, expected_entry);
        } else {
            assert!(bytes[entries_offset..].iter().all(|byte| *byte == 0xff));
        }
    }
}

//= spec/ring/05-disk-format.md#free-space-collection-regions
//= type=test
//# `RING-FREE-001` `FreeQueuePosition` MUST be encoded as the exact byte sequence of the
//# fields shown above, in that order, with no implicit padding.
#[test]
fn requirement_format_empty_store_leaves_free_data_regions_erased() {
    let mut flash = MockFlash::<96, 4, 32>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();

    assert!(flash
        .region_bytes(2)
        .unwrap()
        .iter()
        .all(|byte| *byte == 0xff));
    assert!(flash
        .region_bytes(3)
        .unwrap()
        .iter()
        .all(|byte| *byte == 0xff));
}

//= spec/mock.md#mock-flash-requirements
//= type=test
//# `RING-IMPL-REGRESSION-045` Formatting an empty mock store MUST leave reserved bytes after
//# encoded StorageMetadata erased.
#[test]
fn requirement_format_empty_store_leaves_reserved_metadata_bytes_erased() {
    let mut flash = MockFlash::<96, 4, 32>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();

    let mut metadata_region = [0u8; 96];
    flash.read_storage(0, &mut metadata_region).unwrap();
    assert!(metadata_region[StorageMetadata::ENCODED_LEN..]
        .iter()
        .all(|byte| *byte == 0xff));
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-010` The metadata region MUST occupy exactly one `region_size` span at storage
//# offset `0`, MUST NOT be counted in `region_count`, and data region `0` MUST begin immediately
//# after that metadata region.
#[test]
fn requirement_format_empty_store_places_region_zero_immediately_after_metadata_region() {
    let mut flash = MockFlash::<96, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    assert_eq!(metadata.region_count, 4);

    let mut metadata_region = [0u8; 96];
    flash.read_storage(0, &mut metadata_region).unwrap();
    assert_eq!(StorageMetadata::decode(&metadata_region).unwrap(), metadata);

    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    flash.read_storage(96, &mut header_bytes).unwrap();
    assert_eq!(
        header_bytes,
        flash.region_bytes(0).unwrap()[..Header::ENCODED_LEN]
    );

    let header = Header::decode(&header_bytes).unwrap();
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, WAL_V1_FORMAT);
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-001` Storage MUST begin with a static metadata region that records version and
//# configuration parameters that do not change after initialization.
#[test]
fn requirement_format_empty_store_begins_with_static_metadata_region() {
    let mut flash = MockFlash::<96, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    let mut metadata_region = [0u8; 96];
    flash.read_storage(0, &mut metadata_region).unwrap();
    assert_eq!(StorageMetadata::decode(&metadata_region).unwrap(), metadata);

    let mut region_zero_prefix = [0u8; Header::ENCODED_LEN];
    flash.read_storage(96, &mut region_zero_prefix).unwrap();
    let wal_header = Header::decode(&region_zero_prefix).unwrap();
    assert_eq!(wal_header.collection_id, CollectionId(0));
}
