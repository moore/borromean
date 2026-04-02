use super::*;

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
//# RING-WAL-ENC-001 Every physical WAL record MUST begin with a one-byte `record_magic`.
//= spec/ring.md#wal-record-types
//# RING-WAL-ENC-006 After the full logical record through `record_checksum` has been decoded, any remaining bytes up to the aligned physical record end are padding. Those padding bytes MUST all equal `wal_escape_code_escape`.
//= spec/ring.md#wal-record-types
//# RING-WAL-ENC-008 The encoded size of every WAL record MUST be rounded up to a multiple of `wal_write_granule`.
#[test]
fn update_record_round_trips_with_escaping_and_padding() {
    let metadata = StorageMetadata::new(128, 8, 1, 8, 0xff, 0xa5).unwrap();
    let record = WalRecord::Update {
        collection_id: CollectionId(7),
        payload: &[0x11, 0xff, 0xa5, 0x00, 0x33],
    };

    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();

    assert_eq!(physical[0], 0xa5);
    assert_eq!(encoded_len % 8, 0);

    let escape_codes = WalEscapeCodes::derive(metadata.erased_byte, metadata.wal_record_magic);
    assert_eq!(physical[encoded_len - 1], escape_codes.wal_escape_code_escape);

    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}

//= spec/ring.md#wal-record-types
//# RING-WAL-LAYOUT-005 Record types whose payload is empty (`new_collection`, `drop_collection`, and `wal_recovery`) MUST still encode `payload_len = 0`.
#[test]
fn free_list_head_none_round_trips() {
    let metadata = StorageMetadata::new(128, 8, 1, 4, 0xff, 0xa5).unwrap();
    let record = WalRecord::FreeListHead { region_index: None };

    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();

    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}

//= spec/ring.md#wal-record-types
//# RING-WAL-ENC-004 During decoding, any `wal_escape_byte` in the encoded body MUST be followed by exactly one of `wal_escape_code_erased`, `wal_escape_code_magic`, or `wal_escape_code_escape`; any other follower byte is corruption.
#[test]
fn decode_rejects_invalid_escape_sequence() {
    let metadata = StorageMetadata::new(128, 8, 1, 4, 0xff, 0xa5).unwrap();
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
//# RING-WAL-LAYOUT-001 `record_type` MUST use these canonical byte codes:
//= spec/ring.md#wal-record-types
//# RING-WAL-LAYOUT-003 `payload_len` MUST equal the number of logical payload bytes only.
#[test]
fn alloc_begin_round_trips_free_list_head_after() {
    let metadata = StorageMetadata::new(128, 8, 1, 4, 0xff, 0xa5).unwrap();
    let record = WalRecord::AllocBegin {
        region_index: 3,
        free_list_head_after: Some(4),
    };

    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();

    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}
