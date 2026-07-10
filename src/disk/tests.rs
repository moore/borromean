use super::*;

//= spec/ring/05-disk-format.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-001` All fixed-width integer fields in `StorageMetadata`, `Header`,
//# `LogRegionPrologue`, `FreeSpaceRegionPrologue`, `FreeQueuePosition`, `FreeSpaceEntry`,
//# and logical WAL records MUST be encoded little-endian.
#[test]
fn requirement_disk_structures_encode_fixed_width_fields_little_endian() {
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
        2u32.to_le_bytes().as_slice(),
        0x1122_3344u32.to_le_bytes().as_slice(),
        0x5566_7788u32.to_le_bytes().as_slice(),
        0x0102_0304u32.to_le_bytes().as_slice(),
        1u32.to_le_bytes().as_slice(),
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
        log_head_region_index: 0x0b0c_0d0e,
        allocation_head: FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        },
        ready_boundary: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
        append_tail: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    };
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue
        .encode_into(&mut prologue_bytes, 0x0f10_1112)
        .unwrap();
    assert_eq!(
        &prologue_bytes[..size_of::<u32>()],
        0x0b0c_0d0eu32.to_le_bytes().as_slice()
    );

    let entry = FreeSpaceEntry {
        region_index: 0x2122_2324,
    };
    let mut entry_bytes = [0u8; FreeSpaceEntry::ENCODED_LEN];
    entry.encode_into(&mut entry_bytes, u32::MAX).unwrap();
    assert_eq!(
        &entry_bytes[..size_of::<u32>()],
        0x2122_2324u32.to_le_bytes().as_slice()
    );
}

//= spec/ring/05-disk-format.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-006` `metadata_checksum`, `header_checksum`,
//# `prologue_checksum`, and `record_checksum` MUST all use the standard
//# CRC-32C (Castagnoli) parameters (`poly = 0x1edc6f41`,
//# `init = 0xffffffff`, `refin = true`, `refout = true`,
//# `xorout = 0xffffffff`) and MUST be stored little-endian.
#[test]
fn requirement_disk_structure_checksums_use_crc32c_and_store_little_endian_bytes() {
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
        log_head_region_index: 3,
        allocation_head: FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        },
        ready_boundary: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
        append_tail: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
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
}

//= spec/ring/05-disk-format.md#storage-metadata
//= type=test
//# `RING-META-001` The canonical on-disk `storage_version` defined by
//# this specification MUST be `2`.
#[test]
fn requirement_storage_metadata_uses_storage_version_2() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    assert_eq!(metadata.storage_version, STORAGE_VERSION);
}

//= spec/ring/05-disk-format.md#storage-metadata
//= type=test
//# `RING-META-006` `transaction_log_count` MUST be at least `1`.
#[test]
fn requirement_storage_metadata_validates_transaction_log_count() {
    assert_eq!(
        StorageMetadata::new_with_transaction_logs(4096, 32, 3, 0, 8, 0xff, 0xa5),
        Err(DiskError::InvalidTransactionLogCount {
            transaction_log_count: 0,
            region_count: 32,
        })
    );
    assert_eq!(
        StorageMetadata::new_with_transaction_logs(4096, 32, 3, 32, 8, 0xff, 0xa5),
        Err(DiskError::InvalidTransactionLogCount {
            transaction_log_count: 32,
            region_count: 32,
        })
    );

    let metadata =
        StorageMetadata::new_with_transaction_logs(4096, 32, 3, 31, 8, 0xff, 0xa5).unwrap();
    assert_eq!(metadata.transaction_log_count, 31);
}

//= spec/ring/05-disk-format.md#storage-metadata
//= type=test
//# `RING-META-002` `StorageMetadata` MUST be encoded as the exact byte
//# sequence of the fields shown above, in that order, with no implicit
//# padding.
#[test]
fn requirement_storage_metadata_encodes_fields_in_canonical_order() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
    let used = metadata.encode_into(&mut buffer).unwrap();
    assert_eq!(used, StorageMetadata::ENCODED_LEN);

    let expected_prefix = [
        2u32.to_le_bytes().as_slice(),
        4096u32.to_le_bytes().as_slice(),
        32u32.to_le_bytes().as_slice(),
        3u32.to_le_bytes().as_slice(),
        1u32.to_le_bytes().as_slice(),
        8u32.to_le_bytes().as_slice(),
        &[0xff],
        &[0xa5],
    ]
    .concat();
    assert_eq!(&buffer[..expected_prefix.len()], expected_prefix.as_slice());
}

//= spec/ring/05-disk-format.md#storage-metadata
//= type=test
//# `RING-META-003` `metadata_checksum` MUST be CRC-32C over every
//# earlier `StorageMetadata` field in on-disk order.
#[test]
fn requirement_storage_metadata_checksum_covers_prior_fields() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
    metadata.encode_into(&mut buffer).unwrap();

    let checksum_offset = StorageMetadata::ENCODED_LEN - size_of::<u32>();
    let expected = crc32(&buffer[..checksum_offset]);
    let mut checksum_bytes = [0u8; size_of::<u32>()];
    checksum_bytes.copy_from_slice(&buffer[checksum_offset..]);
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
}

//= spec/ring/05-disk-format.md#storage-metadata
//= type=test
//# `RING-META-004` Startup MUST reject the store if
//# `metadata_checksum` is invalid or if `storage_version` is unsupported.
#[test]
fn requirement_storage_metadata_decode_rejects_bad_checksum() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
    metadata.encode_into(&mut buffer).unwrap();
    buffer[0] ^= 0x01;

    let error = StorageMetadata::decode(&buffer).unwrap_err();
    assert_eq!(error, DiskError::InvalidChecksum);
}

//= spec/ring/05-disk-format.md#storage-metadata
//= type=test
//# `RING-META-005` Any bytes in the metadata region after the encoded `StorageMetadata` are
//# reserved, MUST be left erased by formatting, and MUST be ignored on read.
#[test]
fn requirement_storage_metadata_decode_ignores_reserved_trailing_bytes() {
    let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
    let mut buffer = [0u8; 64];
    metadata.encode_into(&mut buffer).unwrap();
    buffer[StorageMetadata::ENCODED_LEN..].fill(0x13);

    assert_eq!(StorageMetadata::decode(&buffer).unwrap(), metadata);
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-035` Disk byte helpers MUST advance offsets on reads and writes
//# and return `BufferTooSmall` with needed and available sizes for short buffers.
#[test]
fn requirement_byte_helpers_advance_offsets_and_reject_short_buffers() {
    let mut buffer = [0u8; 2];

    let next = write_u8(&mut buffer, 1, 0x5a).unwrap();
    assert_eq!(next, 2);
    assert_eq!(buffer, [0, 0x5a]);
    assert_eq!(
        write_u8(&mut buffer, 2, 0xa5),
        Err(DiskError::BufferTooSmall {
            needed: 3,
            available: 2,
        })
    );

    let mut offset = 1usize;
    assert_eq!(read_u8(&buffer, &mut offset).unwrap(), 0x5a);
    assert_eq!(offset, 2);
    assert_eq!(
        read_u8(&buffer, &mut offset),
        Err(DiskError::BufferTooSmall {
            needed: 3,
            available: 2,
        })
    );
}

//= spec/ring/05-disk-format.md#header
//= type=test
//# `RING-HEADER-001` `Header` MUST be encoded as the exact byte
//# sequence of the fields shown above, in that order, with no implicit
//# padding.
#[test]
fn requirement_header_encodes_fields_in_canonical_order() {
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

//= spec/ring/05-disk-format.md#header
//= type=test
//# `RING-HEADER-002` `header_checksum` MUST be CRC-32C over `sequence`,
//# `collection_id`, and `collection_format` in on-disk order.
#[test]
fn requirement_header_checksum_covers_prefix_fields() {
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

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-002` Every region header MUST record the region
//# `sequence`, `collection_id`, `collection_format`, and a checksum over
//# the header itself.
#[test]
fn requirement_header_round_trips_sequence_collection_id_collection_format_and_checksum() {
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

//= spec/ring/05-disk-format.md#log-region-prologue
//= type=test
//# `RING-PROLOGUE-001` `LogRegionPrologue` MUST be encoded as the exact
//# byte sequence of the fields shown above, in that order, with no
//# implicit padding.
#[test]
fn requirement_wal_prologue_encodes_fields_in_canonical_order() {
    let prologue = WalRegionPrologue {
        log_head_region_index: 3,
        allocation_head: FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        },
        ready_boundary: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
        append_tail: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    };
    let mut buffer = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue.encode_into(&mut buffer, 8).unwrap();

    assert_eq!(&buffer[..size_of::<u32>()], 3u32.to_le_bytes().as_slice());
}

//= spec/ring/05-disk-format.md#log-region-prologue
//= type=test
//# `RING-PROLOGUE-002` `prologue_checksum` MUST be CRC-32C over `log_head_region_index`,
//# `allocation_head`, `ready_boundary`, and `append_tail`.
#[test]
fn requirement_wal_prologue_checksum_covers_head_region_index() {
    let prologue = WalRegionPrologue {
        log_head_region_index: 3,
        allocation_head: FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        },
        ready_boundary: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
        append_tail: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    };
    let mut buffer = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue.encode_into(&mut buffer, 8).unwrap();

    let checksum_offset = WalRegionPrologue::ENCODED_LEN - size_of::<u32>();
    let expected = crc32(&buffer[..checksum_offset]);
    let mut checksum_bytes = [0u8; size_of::<u32>()];
    checksum_bytes.copy_from_slice(&buffer[checksum_offset..]);
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
}

//= spec/ring/05-disk-format.md#log-region-prologue
//= type=test
//# `RING-PROLOGUE-003` `log_head_region_index` MUST be strictly less
//# than `region_count`.
#[test]
fn requirement_wal_prologue_rejects_out_of_range_head() {
    let prologue = WalRegionPrologue {
        log_head_region_index: 4,
        allocation_head: FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        },
        ready_boundary: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
        append_tail: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
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

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-036` The WAL record area offset MUST be aligned to the configured WAL
//# write granule and follow the region header and prologue area.
#[test]
fn requirement_wal_record_area_offset_is_granule_aligned() {
    let metadata = StorageMetadata::new(4096, 32, 3, 16, 0xff, 0xa5).unwrap();
    let offset = metadata.wal_record_area_offset().unwrap();
    assert_eq!(offset % 16, 0);
    assert!(offset >= Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN);
}

//= spec/ring/05-disk-format.md#free-space-region-layout
//= type=test
//# `RING-FREE-010` A materialized free-space basis MUST cover exactly
//# the logical positions `[allocation_head, append_tail)`.
#[test]
fn requirement_contiguous_free_space_positions_map_to_metadata_regions() {
    assert_eq!(
        free_queue_position_for_contiguous_metadata(10, 3, 4, 0).unwrap(),
        FreeQueuePosition {
            region_index: 10,
            entry_index: 0,
        }
    );
    assert_eq!(
        free_queue_position_for_contiguous_metadata(10, 3, 4, 3).unwrap(),
        FreeQueuePosition {
            region_index: 10,
            entry_index: 3,
        }
    );
    assert_eq!(
        free_queue_position_for_contiguous_metadata(10, 3, 4, 4).unwrap(),
        FreeQueuePosition {
            region_index: 11,
            entry_index: 0,
        }
    );
    assert_eq!(
        free_queue_position_for_contiguous_metadata(10, 3, 4, 11).unwrap(),
        FreeQueuePosition {
            region_index: 12,
            entry_index: 3,
        }
    );
    assert_eq!(
        free_queue_position_for_contiguous_metadata(10, 3, 4, 12).unwrap(),
        FreeQueuePosition {
            region_index: 12,
            entry_index: 4,
        }
    );
    assert_eq!(
        free_queue_position_for_contiguous_metadata(10, 0, 4, 0),
        Err(DiskError::BufferTooSmall {
            needed: 1,
            available: 0,
        })
    );
    assert_eq!(
        free_queue_position_for_contiguous_metadata(10, 3, 0, 0),
        Err(DiskError::BufferTooSmall {
            needed: 1,
            available: 0,
        })
    );
}

//= spec/ring/05-disk-format.md#log-region-prologue
//= type=test
//# `RING-PROLOGUE-004` The checkpointed free-space cursors MUST satisfy
//# `allocation_head <= ready_boundary <= append_tail` in logical
//# free-space collection order.
#[test]
fn requirement_log_region_prefix_helpers_encode_format_and_cursor_fields() {
    let metadata = StorageMetadata::new(512, 16, 2, 8, 0xff, 0xa5).unwrap();
    let allocation_head = FreeQueuePosition {
        region_index: 1,
        entry_index: 2,
    };
    let ready_boundary = FreeQueuePosition {
        region_index: 1,
        entry_index: 3,
    };
    let append_tail = FreeQueuePosition {
        region_index: 2,
        entry_index: 0,
    };
    let mut wal = [0u8; 128];
    let used = encode_wal_region_prefix_with_cursors(
        &mut wal,
        metadata,
        9,
        4,
        allocation_head,
        ready_boundary,
        append_tail,
    )
    .unwrap();
    assert_eq!(used, metadata.wal_record_area_offset().unwrap());
    let header = Header::decode(&wal[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(header.sequence, 9);
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, MAIN_WAL_V2_FORMAT);
    let prologue = WalRegionPrologue::decode(
        &wal[Header::ENCODED_LEN..Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN],
        metadata.region_count,
    )
    .unwrap();
    assert_eq!(prologue.log_head_region_index, 4);
    assert_eq!(prologue.allocation_head, allocation_head);
    assert_eq!(prologue.ready_boundary, ready_boundary);
    assert_eq!(prologue.append_tail, append_tail);

    let mut tx = [0u8; 128];
    let used = encode_transaction_log_region_prefix_with_cursors(
        &mut tx,
        metadata,
        10,
        5,
        allocation_head,
        ready_boundary,
        append_tail,
    )
    .unwrap();
    assert_eq!(used, metadata.wal_record_area_offset().unwrap());
    let header = Header::decode(&tx[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(header.sequence, 10);
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, TRANSACTION_LOG_V2_FORMAT);
}
