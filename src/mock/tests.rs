use super::*;
use crate::disk::StorageMetadata;

#[test]
fn metadata_operations_are_logged() {
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

#[test]
fn erase_write_read_and_sync_are_logged() {
    let mut flash = MockFlash::<16, 2, 16>::new(0xff);

    flash.write_region(1, 4, b"bor").unwrap();
    let mut buffer = [0u8; 3];
    flash.read_region(1, 4, &mut buffer).unwrap();
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

#[test]
fn erase_restores_erased_bytes() {
    let mut flash = MockFlash::<8, 1, 8>::new(0xff);
    flash.write_region(0, 0, &[1, 2, 3, 4]).unwrap();
    flash.erase_region(0).unwrap();

    let mut buffer = [0u8; 8];
    flash.read_region(0, 0, &mut buffer).unwrap();
    assert_eq!(buffer, [0xff; 8]);
}

//= spec/ring.md#format-storage-on-disk-initialization
//# RING-FORMAT-STORAGE-PRE-006 `region_count >= 2 + min_free_regions`.
#[test]
fn format_empty_store_rejects_too_few_regions() {
    let mut flash = MockFlash::<64, 2, 16>::new(0xff);
    let error = flash.format_empty_store(1, 8, 0xa5).unwrap_err();
    assert_eq!(
        error,
        MockFormatError::InsufficientRegions {
            region_count: 2,
            min_free_regions: 1,
        }
    );
}

//= spec/ring.md#format-storage-on-disk-initialization
//# RING-FORMAT-STORAGE-003 Initialize region `0` as WAL:
//= spec/ring.md#format-storage-on-disk-initialization
//# `RING-FORMAT-STORAGE-POST-001` WAL head and WAL tail MUST both be region `0`.
#[test]
fn format_empty_store_initializes_region_zero_as_wal() {
    let mut flash = MockFlash::<64, 4, 32>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();

    let header = Header::decode(&flash.region_bytes(0).unwrap()[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, WAL_V1_FORMAT);

    let start = Header::ENCODED_LEN;
    let end = start + WalRegionPrologue::ENCODED_LEN;
    let prologue =
        WalRegionPrologue::decode(&flash.region_bytes(0).unwrap()[start..end], 4).unwrap();
    assert_eq!(prologue.wal_head_region_index, 0);
}

//= spec/ring.md#format-storage-on-disk-initialization
//# `RING-FORMAT-STORAGE-POST-003` The free list MUST contain every non-WAL region in ascending region-index
//# order.
#[test]
fn format_empty_store_populates_free_list_in_ascending_order() {
    let mut flash = MockFlash::<64, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    assert_eq!(flash.metadata(), Some(&metadata));

    let footer_offset = 64 - FreePointerFooter::ENCODED_LEN;
    let region1 =
        FreePointerFooter::decode(&flash.region_bytes(1).unwrap()[footer_offset..], 0xff).unwrap();
    let region2 =
        FreePointerFooter::decode(&flash.region_bytes(2).unwrap()[footer_offset..], 0xff).unwrap();
    let region3 =
        FreePointerFooter::decode(&flash.region_bytes(3).unwrap()[footer_offset..], 0xff).unwrap();

    assert_eq!(region1.next_tail, Some(2));
    assert_eq!(region2.next_tail, Some(3));
    assert_eq!(region3.next_tail, None);
}

//= spec/ring.md#storage-metadata
//# `RING-META-005` Any bytes in the metadata region after the encoded `StorageMetadata` are reserved, MUST be left erased by formatting, and MUST be ignored on read.
#[test]
fn format_empty_store_leaves_reserved_metadata_bytes_erased() {
    let mut flash = MockFlash::<64, 4, 32>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();

    let mut metadata_region = [0u8; 64];
    flash.read_storage(0, &mut metadata_region).unwrap();
    assert!(metadata_region[StorageMetadata::ENCODED_LEN..]
        .iter()
        .all(|byte| *byte == 0xff));
}

//= spec/ring.md#storage-requirements
//# `RING-STORAGE-010` The metadata region MUST occupy exactly one `region_size` span at storage offset `0`, MUST NOT be counted in `region_count`, and data region `0` MUST begin immediately after that metadata region.
#[test]
fn format_empty_store_places_region_zero_immediately_after_metadata_region() {
    let mut flash = MockFlash::<64, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    assert_eq!(metadata.region_count, 4);

    let mut metadata_region = [0u8; 64];
    flash.read_storage(0, &mut metadata_region).unwrap();
    assert_eq!(StorageMetadata::decode(&metadata_region).unwrap(), metadata);

    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    flash.read_storage(64, &mut header_bytes).unwrap();
    assert_eq!(
        header_bytes,
        flash.region_bytes(0).unwrap()[..Header::ENCODED_LEN]
    );

    let header = Header::decode(&header_bytes).unwrap();
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, WAL_V1_FORMAT);
}
