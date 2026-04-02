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

//= spec/ring.md#wal-record-types
//# RING-WAL-ENC-003 After the leading `record_magic`, the rest of the physical WAL record is encoded with deterministic byte-stuffing over the logical WAL record bytes:
#[test]
fn escape_codes_use_first_ascending_distinct_values() {
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

//= spec/ring.md#wal-record-types
//# `RING-WAL-ENC-001` Every physical WAL record MUST begin with a
//# one-byte `record_magic`.
#[test]
fn encoded_record_begins_with_record_magic() {
    let metadata = metadata(16);
    let (physical, _encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);
    assert_eq!(physical[0], 0xa5);
}

//= spec/ring.md#wal-record-types
//# RING-WAL-ENC-006 After the full logical record through `record_checksum` has been decoded, any remaining bytes up to the aligned physical record end are padding. Those padding bytes MUST all equal `wal_escape_code_escape`.
#[test]
fn decode_rejects_non_escape_padding_bytes() {
    let metadata = metadata(16);
    let (mut physical, encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);
    let escape_codes = WalEscapeCodes::derive(metadata.erased_byte, metadata.wal_record_magic);
    assert_eq!(physical[encoded_len - 1], escape_codes.wal_escape_code_escape);
    physical[encoded_len - 1] = 0x00;

    let mut decode_scratch = [0u8; 128];
    let error = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap_err();
    assert_eq!(error, WalRecordError::InvalidPadding(0x00));
}

//= spec/ring.md#wal-record-types
//# `RING-WAL-ENC-008` The encoded size of every WAL record MUST be
//# rounded up to a multiple of
//# `wal_write_granule`.
#[test]
fn encoded_record_len_is_rounded_to_wal_write_granule() {
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

#[test]
fn update_record_round_trips_with_escaping_and_padding() {
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

//= spec/ring.md#wal-record-types
//# `RING-WAL-LAYOUT-005` Record types whose payload is empty
//# (`new_collection`, `drop_collection`, and `wal_recovery`) MUST still
//# encode `payload_len = 0`.
#[test]
fn empty_payload_record_types_encode_zero_payload_len() {
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

#[test]
fn free_list_head_none_round_trips() {
    let metadata = metadata(4);
    let record = WalRecord::FreeListHead { region_index: None };
    let (physical, encoded_len) = encode_physical(record, metadata);
    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}

//= spec/ring.md#wal-record-types
//# `RING-WAL-ENC-004` During decoding, any `wal_escape_byte` in the
//# encoded body MUST be
//# followed by exactly one of
//# `wal_escape_code_erased`, `wal_escape_code_magic`, or
//# `wal_escape_code_escape`; any other follower byte is corruption.
#[test]
fn decode_rejects_invalid_escape_sequence() {
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

//= spec/ring.md#wal-record-types
//# `RING-WAL-LAYOUT-001` `record_type` MUST use these canonical byte
//# codes:
//# `new_collection = 0x01`,
//# `update = 0x02`,
//# `snapshot = 0x03`,
//# `alloc_begin = 0x04`,
//# `head = 0x05`,
//# `drop_collection = 0x06`,
//# `link = 0x07`,
//# `free_list_head = 0x08`,
//# `reclaim_begin = 0x09`,
//# `reclaim_end = 0x0a`,
//# `wal_recovery = 0x0b`.
#[test]
fn record_types_use_canonical_byte_codes() {
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
    ];

    for (record_type, code) in canonical_codes {
        assert_eq!(record_type.code(), code);
        assert_eq!(WalRecordType::decode(code).unwrap(), record_type);
    }
}

//= spec/ring.md#wal-record-types
//# `RING-WAL-LAYOUT-002` The logical field order before byte-stuffing
//# MUST be exactly the order shown above.
#[test]
fn logical_record_fields_follow_canonical_order() {
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

//= spec/ring.md#wal-record-types
//# `RING-WAL-LAYOUT-003` `payload_len` MUST equal the number of logical
//# payload bytes only.
#[test]
fn payload_len_counts_only_logical_payload_bytes() {
    let (alloc_begin_logical, _alloc_begin_len) = encode_logical(WalRecord::AllocBegin {
        region_index: 3,
        free_list_head_after: Some(4),
    });

    assert_eq!(
        &alloc_begin_logical[1..1 + size_of::<u32>()],
        &(size_of::<u32>() as u32).to_le_bytes()
    );
}

//= spec/ring.md#wal-record-types
//# It MUST exclude omitted optional fields,
//# `record_checksum`, the physical leading `record_magic`, and any
//# physical padding.
#[test]
fn payload_len_excludes_omitted_fields_checksum_magic_and_padding() {
    let metadata = metadata(16);
    let (logical, logical_len) = encode_logical(WalRecord::WalRecovery);
    let (physical, encoded_len) = encode_physical(WalRecord::WalRecovery, metadata);

    assert_eq!(&logical[1..1 + size_of::<u32>()], &0u32.to_le_bytes());
    assert_eq!(logical_len, 9);
    assert_eq!(physical[0], metadata.wal_record_magic);
    assert!(encoded_len > logical_len);
}

//= spec/ring.md#wal-record-types
//# `RING-WAL-LAYOUT-004` `record_checksum` MUST be CRC-32C over the
//# logical WAL record bytes from `record_type` through the final byte of
//# the last field preceding `record_checksum`.
#[test]
fn record_checksum_covers_logical_prefix_bytes() {
    let payload = [0xaa, 0xbb];
    let (logical, logical_len) = encode_logical(WalRecord::Snapshot {
        collection_id: CollectionId(7),
        collection_type: crate::CollectionType::MAP_CODE,
        payload: &payload,
    });

    let checksum_offset = logical_len - size_of::<u32>();
    let expected_checksum = crc32(&logical[..checksum_offset]);
    let checksum_bytes: [u8; size_of::<u32>()] = logical[checksum_offset..logical_len]
        .try_into()
        .unwrap();
    assert_eq!(u32::from_le_bytes(checksum_bytes), expected_checksum);
}

#[test]
fn alloc_begin_round_trips_free_list_head_after() {
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
