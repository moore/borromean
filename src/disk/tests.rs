use super::*;

//= spec/ring.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-001` All fixed-width integer fields in `StorageMetadata`,
//# `Header`, `WalRegionPrologue`, free-pointer footers, and logical WAL
//# records MUST be encoded little-endian.
#[test]
fn disk_structures_encode_fixed_width_fields_little_endian() {
    let metadata = StorageMetadata::new(
        0x1122_3344,
        0x5566_7788,
        0x0102_0304,
        0x0506_0708,
        0xaa,
        0xbb,
    )
    .unwrap();
    let mut metadata_bytes = [0u8; StorageMetadata::ENCODED_LEN];
    metadata.encode_into(&mut metadata_bytes).unwrap();
    let expected_metadata_prefix = [
        1u32.to_le_bytes().as_slice(),
        0x1122_3344u32.to_le_bytes().as_slice(),
        0x5566_7788u32.to_le_bytes().as_slice(),
        0x0102_0304u32.to_le_bytes().as_slice(),
        0x0506_0708u32.to_le_bytes().as_slice(),
        &[0xaa],
        &[0xbb],
    ]
    .concat();
    assert_eq!(
        &metadata_bytes[..expected_metadata_prefix.len()],
        expected_metadata_prefix.as_slice()
    );

    let header = Header {
        sequence: 0x0102_0304_0506_0708,
        collection_id: CollectionId(0x1112_1314_1516_1718),
        collection_format: 0x191a,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    let expected_header_prefix = [
        0x0102_0304_0506_0708u64.to_le_bytes().as_slice(),
        0x1112_1314_1516_1718u64.to_le_bytes().as_slice(),
        0x191au16.to_le_bytes().as_slice(),
    ]
    .concat();
    assert_eq!(
        &header_bytes[..expected_header_prefix.len()],
        expected_header_prefix.as_slice()
    );

    let prologue = WalRegionPrologue {
        wal_head_region_index: 0x0b0c_0d0e,
    };
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue
        .encode_into(&mut prologue_bytes, 0x0f10_1112)
        .unwrap();
    assert_eq!(
        &prologue_bytes[..size_of::<u32>()],
        0x0b0c_0d0eu32.to_le_bytes().as_slice()
    );

    let footer = FreePointerFooter {
        next_tail: Some(0x2122_2324),
    };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut footer_bytes, 0xff).unwrap();
    assert_eq!(
        &footer_bytes[..size_of::<u32>()],
        0x2122_2324u32.to_le_bytes().as_slice()
    );
}

//= spec/ring.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-006` `metadata_checksum`, `header_checksum`,
//# `prologue_checksum`, `footer_checksum`, and `record_checksum` MUST all use the standard
//# CRC-32C (Castagnoli) parameters (`poly = 0x1edc6f41`,
//# `init = 0xffffffff`, `refin = true`, `refout = true`,
//# `xorout = 0xffffffff`) and MUST be stored little-endian.
#[test]
fn disk_structure_checksums_use_crc32c_and_store_little_endian_bytes() {
    assert_eq!(crc32(b"123456789"), 0xe306_9283);

    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut metadata_bytes = [0u8; StorageMetadata::ENCODED_LEN];
    metadata.encode_into(&mut metadata_bytes).unwrap();
    let metadata_checksum_offset = StorageMetadata::ENCODED_LEN - size_of::<u32>();
    assert_eq!(
        &metadata_bytes[metadata_checksum_offset..],
        crc32(&metadata_bytes[..metadata_checksum_offset])
            .to_le_bytes()
            .as_slice()
    );

    let header = Header {
        sequence: 9,
        collection_id: CollectionId(7),
        collection_format: WAL_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    let header_checksum_offset = Header::ENCODED_LEN - size_of::<u32>();
    assert_eq!(
        &header_bytes[header_checksum_offset..],
        crc32(&header_bytes[..header_checksum_offset])
            .to_le_bytes()
            .as_slice()
    );

    let prologue = WalRegionPrologue {
        wal_head_region_index: 3,
    };
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue.encode_into(&mut prologue_bytes, 8).unwrap();
    let prologue_checksum_offset = WalRegionPrologue::ENCODED_LEN - size_of::<u32>();
    assert_eq!(
        &prologue_bytes[prologue_checksum_offset..],
        crc32(&prologue_bytes[..prologue_checksum_offset])
            .to_le_bytes()
            .as_slice()
    );

    let footer = FreePointerFooter {
        next_tail: Some(11),
    };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut footer_bytes, 0xff).unwrap();
    let footer_checksum_offset = FreePointerFooter::ENCODED_LEN - size_of::<u32>();
    assert_eq!(
        &footer_bytes[footer_checksum_offset..],
        crc32(&footer_bytes[..footer_checksum_offset])
            .to_le_bytes()
            .as_slice()
    );
}

//= spec/ring.md#storage-metadata
//= type=test
//# `RING-META-001` The canonical on-disk `storage_version` defined by
//# this specification MUST be `1`.
#[test]
fn storage_metadata_uses_storage_version_1() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    assert_eq!(metadata.storage_version, STORAGE_VERSION);
}

//= spec/ring.md#storage-metadata
//= type=test
//# `RING-META-002` `StorageMetadata` MUST be encoded as the exact byte
//# sequence of the fields shown above, in that order, with no implicit
//# padding.
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
//= type=test
//# `RING-META-003` `metadata_checksum` MUST be CRC-32C over every
//# earlier `StorageMetadata` field in on-disk order.
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
//= type=test
//# `RING-META-004` Startup MUST reject the store if
//# `metadata_checksum` is invalid or if `storage_version` is unsupported.
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
//= type=test
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
//= type=test
//# `RING-HEADER-001` `Header` MUST be encoded as the exact byte
//# sequence of the fields shown above, in that order, with no implicit
//# padding.
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
//= type=test
//# `RING-HEADER-002` `header_checksum` MUST be CRC-32C over `sequence`,
//# `collection_id`, and `collection_format` in on-disk order.
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

//= spec/ring.md#storage-requirements
//= type=test
//# `RING-STORAGE-002` Every region header MUST record the region
//# `sequence`, `collection_id`, `collection_format`, and a checksum over
//# the header itself.
#[test]
fn header_round_trips_sequence_collection_id_collection_format_and_checksum() {
    let header = Header {
        sequence: 0x0102_0304_0506_0708,
        collection_id: CollectionId(0x1112_1314_1516_1718),
        collection_format: 0x191a,
    };
    let mut buffer = [0u8; Header::ENCODED_LEN];
    let used = header.encode_into(&mut buffer).unwrap();

    assert_eq!(used, Header::ENCODED_LEN);
    assert_eq!(Header::decode(&buffer).unwrap(), header);
}

//= spec/ring.md#wal-region-prologue
//= type=test
//# `RING-PROLOGUE-001` `WalRegionPrologue` MUST be encoded as the exact
//# byte sequence of the fields shown above, in that order, with no
//# implicit padding.
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
//= type=test
//# `RING-PROLOGUE-002` `prologue_checksum` MUST be CRC-32C over
//# `wal_head_region_index`.
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
//= type=test
//# `RING-PROLOGUE-003` `wal_head_region_index` MUST be strictly less
//# than `region_count`.
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
//= type=test
//# `RING-FREE-003` Otherwise the footer MUST decode as
//# `next_tail:u32, footer_checksum:u32`, both little-endian, with
//# `footer_checksum` equal to CRC-32C over `next_tail`.
#[test]
fn free_pointer_footer_uses_crc32c_for_non_erased_value() {
    let footer = FreePointerFooter {
        next_tail: Some(11),
    };
    let mut buffer = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut buffer, 0xff).unwrap();

    let expected = crc32(&11u32.to_le_bytes());
    let mut checksum_bytes = [0u8; size_of::<u32>()];
    checksum_bytes.copy_from_slice(&buffer[size_of::<u32>()..]);
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
    assert_eq!(FreePointerFooter::decode(&buffer, 0xff).unwrap(), footer);
}

//= spec/ring.md#free-pointer-footer
//= type=test
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

//= spec/ring.md#free-pointer-footer
//= type=test
//# `RING-FREE-004` A checksum-valid non-erased footer MUST decode to a
//# `u32 region_index` strictly less than `region_count`; any other value is
//# malformed.
#[test]
fn free_pointer_footer_rejects_region_index_at_or_above_region_count() {
    let footer = FreePointerFooter { next_tail: Some(4) };
    let mut buffer = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut buffer, 0xff).unwrap();

    let error = FreePointerFooter::decode_with_region_count(&buffer, 0xff, 4).unwrap_err();
    assert_eq!(
        error,
        DiskError::InvalidRegionIndex {
            region_index: 4,
            region_count: 4,
        }
    );
}

#[test]
fn wal_record_area_offset_is_granule_aligned() {
    let metadata = StorageMetadata::new(4096, 32, 3, 16, 0xff, 0xa5).unwrap();
    let offset = metadata.wal_record_area_offset().unwrap();
    assert_eq!(offset % 16, 0);
    assert!(offset >= Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN);
}
