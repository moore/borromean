use super::*;

//= spec/ring.md#storage-metadata
//# RING-META-001 The canonical on-disk `storage_version` defined by this specification MUST be `1`.
#[test]
fn storage_metadata_uses_storage_version_1() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    assert_eq!(metadata.storage_version, STORAGE_VERSION);
}

//= spec/ring.md#storage-metadata
//# RING-META-002 `StorageMetadata` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
#[test]
fn storage_metadata_encodes_fields_in_canonical_order() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
    let used = metadata.encode_into(&mut buffer).unwrap();
    assert_eq!(used, StorageMetadata::ENCODED_LEN);

    let expected_prefix = [
        1u32.to_le_bytes().as_slice(),
        4096u32.to_le_bytes().as_slice(),
        32u32.to_le_bytes().as_slice(),
        3u32.to_le_bytes().as_slice(),
        8u32.to_le_bytes().as_slice(),
        &[0xff],
        &[0xa5],
    ]
    .concat();
    assert_eq!(&buffer[..expected_prefix.len()], expected_prefix.as_slice());
}

//= spec/ring.md#storage-metadata
//# RING-META-003 `metadata_checksum` MUST be CRC-32C over every earlier `StorageMetadata` field in on-disk order.
#[test]
fn storage_metadata_checksum_covers_prior_fields() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
    metadata.encode_into(&mut buffer).unwrap();

    let checksum_offset = StorageMetadata::ENCODED_LEN - size_of::<u32>();
    let expected = crc32(&buffer[..checksum_offset]);
    let mut checksum_bytes = [0u8; size_of::<u32>()];
    checksum_bytes.copy_from_slice(&buffer[checksum_offset..]);
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
}

//= spec/ring.md#storage-metadata
//# RING-META-004 Startup MUST reject the store if `metadata_checksum` is invalid or if `storage_version` is unsupported.
#[test]
fn storage_metadata_decode_rejects_bad_checksum() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
    metadata.encode_into(&mut buffer).unwrap();
    buffer[0] ^= 0x01;

    let error = StorageMetadata::decode(&buffer).unwrap_err();
    assert_eq!(error, DiskError::InvalidChecksum);
}

//= spec/ring.md#storage-metadata
//# `RING-META-005` Any bytes in the metadata region after the encoded `StorageMetadata` are reserved, MUST be left erased by formatting, and MUST be ignored on read.
#[test]
fn storage_metadata_decode_ignores_reserved_trailing_bytes() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut buffer = [0u8; 64];
    metadata.encode_into(&mut buffer).unwrap();
    buffer[StorageMetadata::ENCODED_LEN..].fill(0x13);

    assert_eq!(StorageMetadata::decode(&buffer).unwrap(), metadata);
}

//= spec/ring.md#header
//# RING-HEADER-001 `Header` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
#[test]
fn header_encodes_fields_in_canonical_order() {
    let header = Header {
        sequence: 9,
        collection_id: CollectionId(7),
        collection_format: WAL_V1_FORMAT,
    };
    let mut buffer = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut buffer).unwrap();

    let expected_prefix = [
        9u64.to_le_bytes().as_slice(),
        7u64.to_le_bytes().as_slice(),
        WAL_V1_FORMAT.to_le_bytes().as_slice(),
    ]
    .concat();
    assert_eq!(&buffer[..expected_prefix.len()], expected_prefix.as_slice());
}

//= spec/ring.md#header
//# RING-HEADER-002 `header_checksum` MUST be CRC-32C over `sequence`, `collection_id`, and `collection_format` in on-disk order.
#[test]
fn header_checksum_covers_prefix_fields() {
    let header = Header {
        sequence: 9,
        collection_id: CollectionId(7),
        collection_format: WAL_V1_FORMAT,
    };
    let mut buffer = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut buffer).unwrap();

    let checksum_offset = Header::ENCODED_LEN - size_of::<u32>();
    let expected = crc32(&buffer[..checksum_offset]);
    let mut checksum_bytes = [0u8; size_of::<u32>()];
    checksum_bytes.copy_from_slice(&buffer[checksum_offset..]);
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
}

//= spec/ring.md#wal-region-prologue
//# RING-PROLOGUE-001 `WalRegionPrologue` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
#[test]
fn wal_prologue_encodes_fields_in_canonical_order() {
    let prologue = WalRegionPrologue {
        wal_head_region_index: 3,
    };
    let mut buffer = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue.encode_into(&mut buffer, 8).unwrap();

    assert_eq!(&buffer[..size_of::<u32>()], 3u32.to_le_bytes().as_slice());
}

//= spec/ring.md#wal-region-prologue
//# RING-PROLOGUE-002 `prologue_checksum` MUST be CRC-32C over `wal_head_region_index`.
#[test]
fn wal_prologue_checksum_covers_head_region_index() {
    let prologue = WalRegionPrologue {
        wal_head_region_index: 3,
    };
    let mut buffer = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue.encode_into(&mut buffer, 8).unwrap();

    let checksum_offset = WalRegionPrologue::ENCODED_LEN - size_of::<u32>();
    let expected = crc32(&buffer[..checksum_offset]);
    let mut checksum_bytes = [0u8; size_of::<u32>()];
    checksum_bytes.copy_from_slice(&buffer[checksum_offset..]);
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
}

//= spec/ring.md#wal-region-prologue
//# RING-PROLOGUE-003 `wal_head_region_index` MUST be strictly less than `region_count`.
#[test]
fn wal_prologue_rejects_out_of_range_head() {
    let prologue = WalRegionPrologue {
        wal_head_region_index: 4,
    };
    let mut buffer = [0u8; WalRegionPrologue::ENCODED_LEN];

    let error = prologue.encode_into(&mut buffer, 4).unwrap_err();
    assert_eq!(
        error,
        DiskError::InvalidWalHeadRegionIndex {
            region_index: 4,
            region_count: 4,
        }
    );
}

//= spec/ring.md#free-pointer-footer
//# RING-FREE-003 Otherwise the footer MUST decode as `next_tail:u32, footer_checksum:u32`, both little-endian, with `footer_checksum` equal to CRC-32C over `next_tail`.
#[test]
fn free_pointer_footer_uses_crc32c_for_non_erased_value() {
    let footer = FreePointerFooter { next_tail: Some(11) };
    let mut buffer = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut buffer, 0xff).unwrap();

    let expected = crc32(&11u32.to_le_bytes());
    let mut checksum_bytes = [0u8; size_of::<u32>()];
    checksum_bytes.copy_from_slice(&buffer[size_of::<u32>()..]);
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
    assert_eq!(FreePointerFooter::decode(&buffer, 0xff).unwrap(), footer);
}

//= spec/ring.md#free-pointer-footer
//# RING-FREE-002 If all eight footer bytes equal `erased_byte`, the footer is uninitialized and represents `next_tail = none`.
#[test]
fn free_pointer_footer_none_uses_erased_bytes() {
    let footer = FreePointerFooter { next_tail: None };
    let mut buffer = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut buffer, 0xff).unwrap();

    assert!(buffer.iter().all(|byte| *byte == 0xff));
    let decoded = FreePointerFooter::decode(&buffer, 0xff).unwrap();
    assert_eq!(decoded, footer);
}

#[test]
fn wal_record_area_offset_is_granule_aligned() {
    let metadata = StorageMetadata::new(4096, 32, 3, 16, 0xff, 0xa5).unwrap();
    let offset = metadata.wal_record_area_offset().unwrap();
    assert_eq!(offset % 16, 0);
    assert!(offset >= Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN);
}
