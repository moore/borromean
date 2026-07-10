use super::*;

use crate::{CollectionType, StorageMetadata};

fn metadata() -> StorageMetadata {
    StorageMetadata::new(512, 16, 2, 8, 0xff, 0xa5).unwrap()
}

//= spec/ring/05-disk-format.md#transaction-log-segment-layout
//= type=test
//# `RING-TXLOG-SEG-003` A transaction allocation entry with
//# `purpose = data_region` is transaction-owned until
//# `commit_transaction` publishes it as collection-owned or rollback
//# cleanup frees it.
#[test]
fn requirement_allocation_purpose_codes_are_canonical_and_reject_unknown_values() {
    assert_eq!(TransactionAllocationPurpose::DataRegion.code(), 0x01);
    assert_eq!(
        TransactionAllocationPurpose::TransactionSegment.code(),
        0x02
    );
    assert_eq!(
        TransactionAllocationPurpose::decode(0x01),
        Ok(TransactionAllocationPurpose::DataRegion)
    );
    assert_eq!(
        TransactionAllocationPurpose::decode(0x02),
        Ok(TransactionAllocationPurpose::TransactionSegment)
    );
    assert_eq!(
        TransactionAllocationPurpose::decode(0x03),
        Err(WalRecordError::InvalidRecordType(0x03))
    );
}

//= spec/ring/05-disk-format.md#transaction-log-segment-layout
//= type=test
//# `RING-TXLOG-SEG-003` A durable transaction allocation entry with
//# `purpose = data_region` records transaction-owned allocation recovery state.
#[test]
fn requirement_allocation_entry_round_trips_and_is_aligned() {
    let metadata = metadata();
    let entry = TransactionAllocationEntry {
        region_index: 7,
        allocation_head_after: FreeQueuePosition {
            region_index: 1,
            entry_index: 2,
        },
        purpose: TransactionAllocationPurpose::DataRegion,
    };
    let mut buffer = [0u8; 64];
    let len = entry.encode_into(metadata, &mut buffer).unwrap();
    assert_eq!(len % metadata.wal_write_granule as usize, 0);
    assert_eq!(
        TransactionAllocationEntry::decode(metadata, &buffer[..len]).unwrap(),
        entry
    );
    buffer[0] ^= 1;
    assert_eq!(
        TransactionAllocationEntry::decode(metadata, &buffer[..len]),
        Err(WalRecordError::InvalidChecksum)
    );
}

//= spec/ring/05-disk-format.md#transaction-log-segment-layout
//= type=test
//# `RING-TXLOG-SEG-005` Non-final transaction-log segment seals record the linked
//# segment and the sealed private suffix bounds with checksum protection.
#[test]
fn requirement_segment_seal_round_trips() {
    let seal = TransactionSegmentSeal {
        next_region_index: 3,
        expected_sequence: 9,
        free_intent_start: 44,
        segment_end: 88,
    };
    let mut buffer = [0u8; TransactionSegmentSeal::ENCODED_LEN];
    let len = seal.encode_into(&mut buffer).unwrap();
    assert_eq!(len, TransactionSegmentSeal::ENCODED_LEN);
    assert_eq!(TransactionSegmentSeal::decode(&buffer).unwrap(), seal);
    buffer[0] ^= 1;
    assert_eq!(
        TransactionSegmentSeal::decode(&buffer),
        Err(WalRecordError::InvalidChecksum)
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-019` In `transaction_log_v2`, `free_intent` is encoded as a
//# transaction private suffix entry. It is not replay-visible until
//# its segment is sealed and a matching main-WAL `commit_transaction`
//# imports the transaction.
#[test]
fn requirement_private_suffix_entry_round_trips_free_intent() {
    let record = WalRecord::FreeIntent {
        collection_id: CollectionId(7),
        region_index: 3,
    };
    let mut buffer = [0u8; 64];
    let len = encode_private_suffix_entry(record, &mut buffer).unwrap();
    let decoded = decode_private_suffix_entry(&buffer[..len]).unwrap();
    assert_eq!(decoded.record, record);
    assert_eq!(decoded.encoded_len, len);
}

//= spec/ring/05-disk-format.md#transaction-log-segment-layout
//= type=test
//# `RING-TXLOG-SEG-009` Private suffix entry encoding MUST round-trip every
//# supported transaction-private collection and allocator record shape.
#[test]
fn requirement_private_suffix_entries_round_trip_all_supported_record_shapes() {
    let records = [
        WalRecord::NewCollection {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
        },
        WalRecord::Update {
            collection_id: CollectionId(7),
            payload: &[1, 2, 3],
        },
        WalRecord::Snapshot {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            payload: &[4, 5],
        },
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            region_index: 9,
        },
        WalRecord::DropCollection {
            collection_id: CollectionId(7),
        },
        WalRecord::AddTransactionCollection {
            collection_id: CollectionId(7),
            observed_collection_generation: 0x1122_3344_5566_7788,
        },
        WalRecord::FreeIntent {
            collection_id: CollectionId(7),
            region_index: 3,
        },
    ];

    for record in records {
        let mut buffer = [0u8; 96];
        let len = encode_private_suffix_entry(record, &mut buffer).unwrap();
        assert_eq!(private_suffix_entry_len(record).unwrap(), len);
        let decoded = decode_private_suffix_entry(&buffer[..len]).unwrap();
        assert_eq!(decoded.record, record);
        assert_eq!(decoded.encoded_len, len);
    }
}

//= spec/ring/05-disk-format.md#transaction-log-segment-layout
//= type=test
//# `RING-TXLOG-SEG-010` Private suffix entry encoding MUST reject unsupported
//# record types, short output buffers, truncated input, and checksum-corrupt
//# entries.
#[test]
fn requirement_private_suffix_entries_reject_unsupported_short_and_corrupt_records() {
    let mut buffer = [0u8; 32];
    assert_eq!(
        encode_private_suffix_entry(
            WalRecord::AllocateRegion {
                region_index: 2,
                allocation_head_after: FreeQueuePosition {
                    region_index: 1,
                    entry_index: 1,
                },
            },
            &mut buffer,
        ),
        Err(WalRecordError::InvalidRecordType(
            WalRecordType::AllocateRegion.code()
        ))
    );

    let record = WalRecord::FreeIntent {
        collection_id: CollectionId(7),
        region_index: 3,
    };
    let len = encode_private_suffix_entry(record, &mut buffer).unwrap();
    assert!(matches!(
        encode_private_suffix_entry(record, &mut buffer[..len - 1]),
        Err(WalRecordError::BufferTooSmall { .. })
    ));
    assert!(matches!(
        decode_private_suffix_entry(&buffer[..len - 1]),
        Err(WalRecordError::BufferTooSmall { .. })
    ));
    buffer[1] ^= 0x80;
    assert_eq!(
        decode_private_suffix_entry(&buffer[..len]),
        Err(WalRecordError::InvalidChecksum)
    );
}

//= spec/ring/05-disk-format.md#canonical-on-disk-encoding
//= type=test
//# `RING-TXLOG-SEG-011` Transaction-log byte helpers MUST advance offset
//# cursors exactly and leave cursors unchanged when a short buffer prevents
//# the requested read or write.
#[test]
fn requirement_transaction_log_byte_helpers_advance_offsets_and_reject_short_buffers() {
    let mut buffer = [0u8; 1];
    assert_eq!(write_u8(&mut buffer, 0, 0xab), Ok(1));
    assert_eq!(buffer[0], 0xab);
    assert_eq!(
        write_u8(&mut buffer, 1, 0xcd),
        Err(WalRecordError::BufferTooSmall {
            needed: 2,
            available: 1,
        })
    );

    let mut offset = 0usize;
    assert_eq!(read_u8(&buffer, &mut offset), Ok(0xab));
    assert_eq!(offset, 1);
    assert_eq!(
        read_u8(&buffer, &mut offset),
        Err(WalRecordError::BufferTooSmall {
            needed: 2,
            available: 1,
        })
    );
    assert_eq!(offset, 1);
}
