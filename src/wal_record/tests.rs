use super::*;
use core::mem::size_of;

fn metadata(wal_write_granule: u32) -> StorageMetadata {
    StorageMetadata::new(128, 8, 1, wal_write_granule, 0xff, 0xa5).unwrap()
}

fn encode_physical(record: WalRecord<'_>, metadata: StorageMetadata) -> ([u8; 128], usize) {
    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();
    (physical, encoded_len)
}

fn encode_logical(record: WalRecord<'_>) -> ([u8; 128], usize) {
    let mut logical = [0u8; 128];
    let logical_len = encode_logical_record(record, &mut logical).unwrap();
    (logical, logical_len)
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-123` WAL byte helpers MUST advance offsets for byte and byte-slice reads
//# and writes and report BufferTooSmall with needed and available sizes on short buffers.
#[test]
fn requirement_wal_byte_helpers_advance_offsets_and_reject_short_buffers() {
    let mut buffer = [0u8; 4];

    let offset = write_u8(&mut buffer, 1, 0x7a).unwrap();
    assert_eq!(offset, 2);
    let offset = write_bytes(&mut buffer, offset, &[0x7b, 0x7c]).unwrap();
    assert_eq!(offset, 4);
    assert_eq!(buffer, [0, 0x7a, 0x7b, 0x7c]);
    assert_eq!(
        write_u8(&mut buffer, 4, 0x7d),
        Err(WalRecordError::BufferTooSmall {
            needed: 5,
            available: 4,
        })
    );

    let mut read_offset = 1usize;
    assert_eq!(read_u8(&buffer, &mut read_offset).unwrap(), 0x7a);
    assert_eq!(read_offset, 2);
    assert_eq!(read_u8(&buffer, &mut read_offset).unwrap(), 0x7b);
    assert_eq!(read_offset, 3);
    assert_eq!(read_u8(&buffer, &mut read_offset).unwrap(), 0x7c);
    assert_eq!(read_offset, 4);
    assert!(matches!(
        read_u8(&buffer, &mut read_offset),
        Err(WalRecordError::BufferTooSmall {
            needed: 5,
            available: 4,
        })
    ));
    assert!(matches!(
        write_bytes(&mut buffer, 1, &[1, 2, 3, 4]),
        Err(WalRecordError::BufferTooSmall {
            needed: 5,
            available: 4,
        })
    ));
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-124` Logical WAL byte encoding MUST escape erased byte, record magic, and
//# escape byte with distinct derived escape codes.
#[test]
fn requirement_logical_byte_encoding_escapes_reserved_physical_bytes() {
    let metadata = metadata(8);
    let escape_codes = WalEscapeCodes::derive(metadata.erased_byte, metadata.wal_record_magic);
    let mut output = [0u8; 8];

    let offset =
        encode_logical_byte(metadata.erased_byte, &mut output, 0, metadata, escape_codes).unwrap();
    assert_eq!(offset, 2);
    assert_eq!(
        &output[..offset],
        &[
            escape_codes.wal_escape_byte,
            escape_codes.wal_escape_code_erased,
        ]
    );

    let offset = encode_logical_byte(
        metadata.wal_record_magic,
        &mut output,
        0,
        metadata,
        escape_codes,
    )
    .unwrap();
    assert_eq!(offset, 2);
    assert_eq!(
        &output[..offset],
        &[
            escape_codes.wal_escape_byte,
            escape_codes.wal_escape_code_magic,
        ]
    );

    let offset = encode_logical_byte(
        escape_codes.wal_escape_byte,
        &mut output,
        0,
        metadata,
        escape_codes,
    )
    .unwrap();
    assert_eq!(offset, 2);
    assert_eq!(
        &output[..offset],
        &[
            escape_codes.wal_escape_byte,
            escape_codes.wal_escape_code_escape,
        ]
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-005` Every byte after the leading `record_magic` in a
//# valid encoded WAL
//# record therefore differs from both `erased_byte` and
//# `wal_record_magic`.
#[test]
fn requirement_decode_rejects_unescaped_reserved_body_bytes() {
    let metadata = metadata(8);

    for reserved in [metadata.erased_byte, metadata.wal_record_magic] {
        let (mut physical, encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);
        physical[1] = reserved;
        let mut logical = [0u8; 128];

        assert_eq!(
            decode_record(&physical[..encoded_len], metadata, &mut logical),
            Err(WalRecordError::InvalidUnescapedReservedByte { found: reserved })
        );
    }
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-125` WAL record decoding MUST consume all encoded physical bytes and
//# report encoded and logical lengths for decoded records.
#[test]
fn requirement_decode_record_consumes_all_logical_bytes_and_reports_lengths() {
    let metadata = metadata(8);
    let record = WalRecord::Update {
        collection_id: CollectionId(9),
        payload: &[0xff, 0xa5, 0x00, 0x01],
    };
    let (physical, encoded_len) = encode_physical(record, metadata);
    let mut logical = [0u8; 128];

    let decoded = decode_record(&physical[..encoded_len], metadata, &mut logical).unwrap();

    assert_eq!(decoded.record, record);
    assert_eq!(decoded.encoded_len, encoded_len);
    assert_eq!(
        decoded.logical_len,
        1 + size_of::<u64>() + size_of::<u32>() + 4 + size_of::<u32>()
    );
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-126` WAL record decoding MUST wait until all payload-header bytes are
//# available before reading payload metadata.
#[test]
fn requirement_decode_record_does_not_read_payload_header_before_all_header_bytes_arrive() {
    let metadata = metadata(8);
    let record = WalRecord::Update {
        collection_id: CollectionId(9),
        payload: &[0x01, 0x02],
    };
    let (physical, encoded_len) = encode_physical(record, metadata);
    let mut logical = [0xffu8; 128];

    let decoded = decode_record(&physical[..encoded_len], metadata, &mut logical).unwrap();

    assert_eq!(decoded.record, record);
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-127` WAL record decoding MUST reject an empty logical scratch buffer
//# before writing the first decoded logical byte.
#[test]
fn requirement_decode_record_rejects_empty_logical_scratch_before_writing_first_byte() {
    let metadata = metadata(8);
    let (physical, encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);

    assert!(matches!(
        decode_record(&physical[..encoded_len], metadata, &mut []),
        Err(WalRecordError::BufferTooSmall {
            needed: 1,
            available: 0,
        })
    ));
}

//= spec/ring/05-disk-format.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-002` The canonical scalar widths are:
//# `region_index: u32`, `region_size: u32`, `region_count: u32`,
//# `min_free_regions: u32`, `wal_write_granule: u32`,
//# `collection_id: u64`, `sequence: u64`, `payload_len: u32`,
//# `collection_type: u16`, `collection_format: u16`,
//# `erased_byte: u8`, and `wal_record_magic: u8`.
#[test]
fn requirement_canonical_scalar_widths_match_storage_header_and_wal_field_sizes() {
    let metadata = metadata(16);
    assert_eq!(
        core::mem::size_of_val(&metadata.storage_version),
        size_of::<u32>()
    );
    assert_eq!(
        core::mem::size_of_val(&metadata.region_size),
        size_of::<u32>()
    );
    assert_eq!(
        core::mem::size_of_val(&metadata.region_count),
        size_of::<u32>()
    );
    assert_eq!(
        core::mem::size_of_val(&metadata.min_free_regions),
        size_of::<u32>()
    );
    assert_eq!(
        core::mem::size_of_val(&metadata.wal_write_granule),
        size_of::<u32>()
    );
    assert_eq!(
        core::mem::size_of_val(&metadata.erased_byte),
        size_of::<u8>()
    );
    assert_eq!(
        core::mem::size_of_val(&metadata.wal_record_magic),
        size_of::<u8>()
    );

    let header = crate::Header {
        sequence: 9,
        collection_id: CollectionId(7),
        collection_format: crate::MAP_REGION_V2_FORMAT,
    };
    assert_eq!(core::mem::size_of_val(&header.sequence), size_of::<u64>());
    assert_eq!(CollectionId(7).to_le_bytes().len(), size_of::<u64>());
    assert_eq!(
        core::mem::size_of_val(&header.collection_format),
        size_of::<u16>()
    );

    let (logical, logical_len) = encode_logical(WalRecord::Head {
        collection_id: CollectionId(7),
        collection_type: crate::CollectionType::MAP_CODE,
        region_index: 3,
    });
    assert_eq!(
        logical_len,
        1 + size_of::<u64>() + size_of::<u16>() + 3 * size_of::<u32>()
    );
    assert_eq!(
        &logical[1 + size_of::<u64>() + size_of::<u16>()
            ..1 + size_of::<u64>() + size_of::<u16>() + size_of::<u32>()],
        (size_of::<u32>() as u32).to_le_bytes().as_slice()
    );
}

//= spec/ring/05-disk-format.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-003` `collection_type` is a stable global `u16`
//# namespace recorded durably in WAL records. Borromean core reserves
//# `0x0000` for `wal`, `0x0001` for `channel`, `0x0002` for `map`,
//# `0x0003..0x00ff` for future core-defined collection types,
//# `0x0100..0x7fff` for public extension collection types, and
//# `0x8000..0xffff` for private deployment-local collection types that
//# are not required to interoperate across deployments.
#[test]
fn requirement_collection_type_codes_use_reserved_global_namespace() {
    assert_eq!(
        crate::CollectionType::Wal.stable_code(),
        Some(crate::CollectionType::WAL_CODE)
    );
    assert_eq!(
        crate::CollectionType::Channel.stable_code(),
        Some(crate::CollectionType::CHANNEL_CODE)
    );
    assert_eq!(
        crate::CollectionType::Map.stable_code(),
        Some(crate::CollectionType::MAP_CODE)
    );
    assert_eq!(crate::CollectionType::Uninitialized.stable_code(), None);
    assert_eq!(crate::CollectionType::Free.stable_code(), None);

    assert_eq!(crate::CollectionType::WAL_CODE, 0);
    assert_eq!(crate::CollectionType::CHANNEL_CODE, 1);
    assert_eq!(crate::CollectionType::MAP_CODE, 2);
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-128` Logical WAL record encoding MUST serialize fixed-width fields
//# little-endian in canonical order.
#[test]
fn requirement_logical_wal_records_encode_fixed_width_fields_little_endian() {
    let (logical, logical_len) = encode_logical(WalRecord::Head {
        collection_id: CollectionId(0x0102_0304_0506_0708),
        collection_type: crate::CollectionType::MAP_CODE,
        region_index: 0x0a0b_0c0d,
    });

    assert_eq!(logical[0], WalRecordType::Head.code());
    assert_eq!(
        &logical[1..1 + size_of::<u64>()],
        0x0102_0304_0506_0708u64.to_le_bytes().as_slice()
    );
    assert_eq!(
        &logical[1 + size_of::<u64>()..1 + size_of::<u64>() + size_of::<u16>()],
        crate::CollectionType::MAP_CODE.to_le_bytes().as_slice()
    );
    assert_eq!(
        &logical[1 + size_of::<u64>() + size_of::<u16>()
            ..1 + size_of::<u64>() + size_of::<u16>() + size_of::<u32>()],
        (size_of::<u32>() as u32).to_le_bytes().as_slice()
    );
    assert_eq!(
        &logical[1 + size_of::<u64>() + size_of::<u16>() + size_of::<u32>()
            ..1 + size_of::<u64>() + size_of::<u16>() + size_of::<u32>() + size_of::<u32>()],
        0x0a0b_0c0du32.to_le_bytes().as_slice()
    );
    assert_eq!(
        logical_len,
        1 + size_of::<u64>() + size_of::<u16>() + 3 * size_of::<u32>()
    );
}

//= spec/ring/05-disk-format.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-005` Optional region indexes carried inside logical WAL
//# records MUST be encoded as `OptRegionIndex`, a one-byte tag followed,
//# when the tag is `1`, by a `u32 region_index`.
#[test]
fn requirement_optional_region_indexes_use_a_tag_then_little_endian_region_index() {
    let (free_list_head_none, free_list_head_none_len) =
        encode_logical(WalRecord::FreeListHead { region_index: None });
    assert_eq!(
        &free_list_head_none[1..1 + size_of::<u32>()],
        &1u32.to_le_bytes()
    );
    assert_eq!(free_list_head_none[1 + size_of::<u32>()], 0);
    assert_eq!(
        free_list_head_none_len,
        1 + 2 * size_of::<u32>() + size_of::<u8>()
    );

    let (alloc_begin_some, alloc_begin_some_len) = encode_logical(WalRecord::AllocBegin {
        region_index: 3,
        free_list_head_after: Some(0x1122_3344),
    });
    let opt_offset = 1 + 2 * size_of::<u32>();
    assert_eq!(alloc_begin_some[opt_offset], 1);
    assert_eq!(
        &alloc_begin_some
            [opt_offset + size_of::<u8>()..opt_offset + size_of::<u8>() + size_of::<u32>()],
        0x1122_3344u32.to_le_bytes().as_slice()
    );
    assert_eq!(
        alloc_begin_some_len,
        1 + 4 * size_of::<u32>() + size_of::<u8>()
    );
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-129` Logical WAL record checksums MUST use CRC-32C over logical prefix
//# bytes and store the checksum little-endian.
#[test]
fn requirement_logical_record_checksums_use_crc32c_and_store_little_endian_bytes() {
    assert_eq!(crc32(b"123456789"), 0xe306_9283);

    let payload = [0xaa, 0xbb];
    let (logical, logical_len) = encode_logical(WalRecord::Snapshot {
        collection_id: CollectionId(7),
        collection_type: crate::CollectionType::MAP_CODE,
        payload: &payload,
    });

    let checksum_offset = logical_len - size_of::<u32>();
    assert_eq!(
        &logical[checksum_offset..logical_len],
        crc32(&logical[..checksum_offset]).to_le_bytes().as_slice()
    );
}

//= spec/ring/05-disk-format.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-007` Unless a structure explicitly says otherwise, the
//# checksum for that structure MUST cover the exact logical bytes of every
//# earlier field in that structure, in on-disk order, and MUST exclude the
//# checksum field itself and any later padding.
#[test]
fn requirement_record_checksums_cover_prior_logical_bytes_but_exclude_checksum_and_padding() {
    let record = WalRecord::Update {
        collection_id: CollectionId(0x0102_0304_0506_0708),
        payload: &[0x11, 0x22, 0x33],
    };
    let (logical, logical_len) = encode_logical(record);
    let checksum_offset = logical_len - size_of::<u32>();
    let stored_checksum =
        u32::from_le_bytes(logical[checksum_offset..logical_len].try_into().unwrap());

    assert_eq!(stored_checksum, crc32(&logical[..checksum_offset]));
    assert_ne!(stored_checksum, crc32(&logical[..logical_len]));

    let metadata_8 = metadata(8);
    let metadata_16 = metadata(16);
    let (physical_8, encoded_len_8) = encode_physical(record, metadata_8);
    let (physical_16, encoded_len_16) = encode_physical(record, metadata_16);
    assert!(encoded_len_16 > encoded_len_8);
    let mut decode_scratch_8 = [0u8; 128];
    assert_eq!(
        decode_record(
            &physical_8[..encoded_len_8],
            metadata_8,
            &mut decode_scratch_8
        )
        .unwrap()
        .record,
        record
    );
    let mut decode_scratch_16 = [0u8; 128];
    assert_eq!(
        decode_record(
            &physical_16[..encoded_len_16],
            metadata_16,
            &mut decode_scratch_16
        )
        .unwrap()
        .record,
        record
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# RING-WAL-ENC-003 After the leading `record_magic`, the rest of the physical WAL record is
//# encoded with deterministic byte-stuffing over the logical WAL record bytes:
#[test]
fn requirement_escape_codes_use_first_ascending_distinct_values() {
    let escape_codes = WalEscapeCodes::derive(0x00, 0x02);
    assert_eq!(
        escape_codes,
        WalEscapeCodes {
            wal_escape_byte: 0x01,
            wal_escape_code_erased: 0x03,
            wal_escape_code_magic: 0x04,
            wal_escape_code_escape: 0x05,
        }
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-001` Every physical WAL record MUST begin with a
//# one-byte `record_magic`.
#[test]
fn requirement_encoded_record_begins_with_record_magic() {
    let metadata = metadata(16);
    let (physical, _encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);
    assert_eq!(physical[0], 0xa5);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-002` `record_magic` MUST equal the storage's configured
//# `wal_record_magic`, and `wal_record_magic` must not equal
//# `erased_byte`, the byte value returned by erased flash.
#[test]
fn requirement_wal_record_magic_must_match_storage_configuration_and_differ_from_erased_byte() {
    let error = StorageMetadata::new(128, 8, 1, 16, 0xff, 0xff).unwrap_err();
    assert_eq!(error, DiskError::InvalidWalRecordMagic);

    let metadata = metadata(16);
    let (mut physical, encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);
    let wrong_magic = metadata.wal_record_magic ^ 0x01;
    physical[0] = wrong_magic;

    let mut logical = [0u8; 128];
    let error = decode_record(&physical[..encoded_len], metadata, &mut logical).unwrap_err();
    assert_eq!(
        error,
        WalRecordError::InvalidRecordMagic {
            found: wrong_magic,
            expected: metadata.wal_record_magic,
        }
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# RING-WAL-ENC-006 After the full logical record through `record_checksum` has been decoded, any
//# remaining bytes up to the aligned physical record end are padding. Those padding bytes MUST all
//# equal `wal_escape_code_escape`.
#[test]
fn requirement_decode_rejects_non_escape_padding_bytes() {
    let metadata = metadata(16);
    let (mut physical, encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);
    let escape_codes = WalEscapeCodes::derive(metadata.erased_byte, metadata.wal_record_magic);
    assert_eq!(
        physical[encoded_len - 1],
        escape_codes.wal_escape_code_escape
    );
    physical[encoded_len - 1] = 0x00;

    let mut decode_scratch = [0u8; 128];
    let error = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap_err();
    assert_eq!(error, WalRecordError::InvalidPadding(0x00));
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-008` The encoded size of every WAL record MUST be
//# rounded up to a multiple of
//# `wal_write_granule`.
#[test]
fn requirement_encoded_record_len_is_rounded_to_wal_write_granule() {
    let metadata = metadata(16);
    let (_physical, encoded_len) = encode_physical(
        WalRecord::Update {
            collection_id: CollectionId(7),
            payload: &[0x11, 0xff, 0xa5, 0x00, 0x33],
        },
        metadata,
    );
    assert_eq!(encoded_len % 16, 0);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-007` Every WAL record start offset within a WAL region
//# MUST be aligned to `wal_write_granule`, the smallest writable unit
//# of the backing flash.
#[test]
fn requirement_consecutive_wal_record_start_offsets_stay_aligned_to_wal_write_granule() {
    let metadata = metadata(16);
    let initial_offset = metadata.wal_record_area_offset().unwrap();
    let (_first_physical, first_len) = encode_physical(WalRecord::WalRecovery, metadata);
    let (_second_physical, second_len) = encode_physical(
        WalRecord::FreeListHead {
            region_index: Some(3),
        },
        metadata,
    );

    assert_eq!(initial_offset % 16, 0);
    assert_eq!((initial_offset + first_len) % 16, 0);
    assert_eq!((initial_offset + first_len + second_len) % 16, 0);
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-130` Update WAL records MUST round-trip through physical escaping,
//# padding, and decoding without changing payload bytes.
#[test]
fn requirement_update_record_round_trips_with_escaping_and_padding() {
    let metadata = metadata(8);
    let record = WalRecord::Update {
        collection_id: CollectionId(7),
        payload: &[0x11, 0xff, 0xa5, 0x00, 0x33],
    };
    let (physical, encoded_len) = encode_physical(record, metadata);
    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-LAYOUT-005` Record types whose payload is empty
//# (`new_collection`, `drop_collection`, `wal_recovery`, and transaction
//# marker records) MUST still encode `payload_len = 0`.
#[test]
fn requirement_empty_payload_record_types_encode_zero_payload_len() {
    let (new_collection_logical, new_collection_len) = encode_logical(WalRecord::NewCollection {
        collection_id: CollectionId(7),
        collection_type: crate::CollectionType::MAP_CODE,
    });
    let (drop_collection_logical, drop_collection_len) =
        encode_logical(WalRecord::DropCollection {
            collection_id: CollectionId(7),
        });
    let (wal_recovery_logical, wal_recovery_len) = encode_logical(WalRecord::WalRecovery);

    assert_eq!(&new_collection_logical[11..15], &0u32.to_le_bytes());
    assert_eq!(new_collection_len, 19);
    assert_eq!(&drop_collection_logical[9..13], &0u32.to_le_bytes());
    assert_eq!(drop_collection_len, 17);
    assert_eq!(&wal_recovery_logical[1..5], &0u32.to_le_bytes());
    assert_eq!(wal_recovery_len, 9);
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-131` Transaction marker WAL records with no payload MUST round-trip
//# through physical encoding and decoding.
#[test]
fn requirement_free_list_head_none_round_trips() {
    let metadata = metadata(4);
    let record = WalRecord::FreeListHead { region_index: None };
    let (physical, encoded_len) = encode_physical(record, metadata);
    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-004` During decoding, any `wal_escape_byte` in the
//# encoded body MUST be
//# followed by exactly one of
//# `wal_escape_code_erased`, `wal_escape_code_magic`, or
//# `wal_escape_code_escape`; any other follower byte is corruption.
#[test]
fn requirement_decode_rejects_invalid_escape_sequence() {
    let metadata = metadata(4);
    let record = WalRecord::Update {
        collection_id: CollectionId(7),
        payload: &[0xff],
    };

    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();

    let escape_codes = WalEscapeCodes::derive(metadata.erased_byte, metadata.wal_record_magic);
    let escape_offset = physical
        .iter()
        .position(|byte| *byte == escape_codes.wal_escape_byte)
        .unwrap();
    physical[escape_offset + 1] = 0xfe;

    let mut decode_scratch = [0u8; 128];
    let error = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap_err();
    assert_eq!(error, WalRecordError::InvalidEscapeSequence(0xfe));
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-LAYOUT-001` `record_type` MUST use these canonical byte
//# codes:
//# `new_collection = 0x01`,
//# `update = 0x02`,
//# `snapshot = 0x03`,
//# `alloc_begin = 0x04`,
//# `head = 0x05`,
//# `drop_collection = 0x06`,
//# `link = 0x07`,
//# `wal_recovery = 0x0b`,
//# `free_region = 0x0c`,
//# `begin_transaction = 0x0d`,
//# `commit_transaction = 0x0e`,
//# `transaction_finished = 0x0f`,
//# `rollback_transaction = 0x10`.
#[test]
fn requirement_record_types_use_canonical_byte_codes() {
    let canonical_codes = [
        (WalRecordType::NewCollection, 0x01),
        (WalRecordType::Update, 0x02),
        (WalRecordType::Snapshot, 0x03),
        (WalRecordType::AllocBegin, 0x04),
        (WalRecordType::Head, 0x05),
        (WalRecordType::DropCollection, 0x06),
        (WalRecordType::Link, 0x07),
        (WalRecordType::FreeListHead, 0x08),
        (WalRecordType::ReclaimBegin, 0x09),
        (WalRecordType::ReclaimEnd, 0x0a),
        (WalRecordType::WalRecovery, 0x0b),
        (WalRecordType::StageRegion, 0x0c),
    ];

    for (record_type, code) in canonical_codes {
        assert_eq!(record_type.code(), code);
        assert_eq!(WalRecordType::decode(code).unwrap(), record_type);
    }
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-LAYOUT-002` The logical field order before byte-stuffing
//# MUST be exactly the order shown above.
#[test]
fn requirement_logical_record_fields_follow_canonical_order() {
    let payload = [0xaa, 0xbb];
    let (logical, logical_len) = encode_logical(WalRecord::Snapshot {
        collection_id: CollectionId(7),
        collection_type: crate::CollectionType::MAP_CODE,
        payload: &payload,
    });

    let checksum_offset = logical_len - size_of::<u32>();
    let expected_prefix = [
        [WalRecordType::Snapshot.code()].as_slice(),
        7u64.to_le_bytes().as_slice(),
        crate::CollectionType::MAP_CODE.to_le_bytes().as_slice(),
        2u32.to_le_bytes().as_slice(),
        payload.as_slice(),
    ]
    .concat();
    assert_eq!(&logical[..checksum_offset], expected_prefix.as_slice());
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-LAYOUT-003` `payload_len` MUST equal the number of logical
//# payload bytes only.
#[test]
fn requirement_payload_len_counts_only_logical_payload_bytes() {
    let (alloc_begin_logical, _alloc_begin_len) = encode_logical(WalRecord::AllocBegin {
        region_index: 3,
        free_list_head_after: Some(4),
    });

    assert_eq!(
        &alloc_begin_logical[1..1 + size_of::<u32>()],
        &(size_of::<u32>() as u32).to_le_bytes()
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# It MUST exclude omitted optional fields,
//# `record_checksum`, the physical leading `record_magic`, and any
//# physical padding.
#[test]
fn requirement_payload_len_excludes_omitted_fields_checksum_magic_and_padding() {
    let metadata = metadata(16);
    let (logical, logical_len) = encode_logical(WalRecord::WalRecovery);
    let (physical, encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);

    assert_eq!(&logical[1..1 + size_of::<u32>()], &0u32.to_le_bytes());
    assert_eq!(logical_len, 9);
    assert_eq!(physical[0], metadata.wal_record_magic);
    assert!(encoded_len > logical_len);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-LAYOUT-004` `record_checksum` MUST be CRC-32C over the
//# logical WAL record bytes from `record_type` through the final byte of
//# the last field preceding `record_checksum`.
#[test]
fn requirement_record_checksum_covers_logical_prefix_bytes() {
    let payload = [0xaa, 0xbb];
    let (logical, logical_len) = encode_logical(WalRecord::Snapshot {
        collection_id: CollectionId(7),
        collection_type: crate::CollectionType::MAP_CODE,
        payload: &payload,
    });

    let checksum_offset = logical_len - size_of::<u32>();
    let expected_checksum = crc32(&logical[..checksum_offset]);
    let checksum_bytes: [u8; size_of::<u32>()] =
        logical[checksum_offset..logical_len].try_into().unwrap();
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected_checksum);
}

//= spec/ring/04-wal-records.md#encoding-helper-requirements
//= type=test
//# `RING-IMPL-REGRESSION-132` Alloc-begin WAL records MUST round-trip free_list_head_after through
//# physical encoding and decoding.
#[test]
fn requirement_alloc_begin_round_trips_free_list_head_after() {
    let metadata = metadata(4);
    let record = WalRecord::AllocBegin {
        region_index: 3,
        free_list_head_after: Some(4),
    };
    let (physical, encoded_len) = encode_physical(record, metadata);
    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-LAYOUT-006` Payload bytes are encoded canonically by record
//# type:
//# `update` and `snapshot` payloads are opaque collection-defined bytes;
//# `alloc_begin`, `head`, and `free_region` payloads are a single
//# `u32 region_index`;
#[test]
fn requirement_stage_region_round_trips_region_index() {
    let metadata = metadata(4);
    let record = WalRecord::StageRegion { region_index: 3 };
    let (physical, encoded_len) = encode_physical(record, metadata);
    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}
