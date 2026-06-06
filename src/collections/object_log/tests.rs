use super::*;

use crate::wal_record::{WalRecord, WalRecordType};
use crate::{CollectionId, MockFlash, Storage, StorageFormatConfig};
use std::format;

const LOG_METADATA: &[u8] = b"log-meta";

fn assert_get<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
    expected: &[u8],
) {
    let mut scratch = [0u8; 64];
    let len = log
        .get(storage, handle, &mut scratch, |bytes| {
            assert_eq!(bytes, expected);
            bytes.len()
        })
        .unwrap();
    assert_eq!(len, expected.len());
}

fn assert_get_range<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
    offset: u64,
    expected: &[u8],
) {
    let mut scratch = [0u8; 64];
    let len = log
        .get_range(
            storage,
            handle,
            offset,
            expected.len() as u64,
            &mut scratch,
            |bytes| {
                assert_eq!(bytes, expected);
                bytes.len()
            },
        )
        .unwrap();
    assert_eq!(len, expected.len());
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn write_u32_at(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_data_prologue_header(bytes: &mut [u8], sequence: u64, metadata_len: usize) {
    bytes[..DATA_MAGIC.len()].copy_from_slice(&DATA_MAGIC);
    bytes[4..6].copy_from_slice(&DATA_VERSION.to_le_bytes());
    bytes[6..14].copy_from_slice(&sequence.to_le_bytes());
    bytes[14..18].copy_from_slice(&(metadata_len as u32).to_le_bytes());
}

fn count_wal_records<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    record_type: WalRecordType,
) -> usize {
    let mut count = 0usize;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<REGION_SIZE, _, (), _>(
                flash,
                workspace,
                |_flash, record| {
                    if record.record_type() == record_type {
                        count += 1;
                    }
                    Ok(())
                },
            )
        })
        .unwrap();
    count
}

fn assert_region_log_metadata<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    handle: ObjectLogHandle,
    expected: &[u8],
) {
    let region = flash.region_bytes(handle.region_index).unwrap();
    let prologue = &region[Header::ENCODED_LEN..];

    assert_eq!(&prologue[..4], DATA_MAGIC.as_slice());
    assert_eq!(read_u16_at(prologue, 4), DATA_VERSION);
    assert_eq!(read_u64_at(prologue, 6), handle.sequence);
    assert_eq!(read_u32_at(prologue, 14), expected.len() as u32);
    assert_eq!(
        &prologue[DATA_PROLOGUE_FIXED_LEN..DATA_PROLOGUE_FIXED_LEN + expected.len()],
        expected
    );
    assert_eq!(
        usize::try_from(handle.offset).unwrap(),
        Header::ENCODED_LEN + DATA_PROLOGUE_FIXED_LEN + expected.len()
    );
}

fn fill_pattern(bytes: &mut [u8]) {
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
}

fn assert_get_bytes<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
    expected: &[u8],
    scratch: &mut [u8],
) {
    let len = log
        .get(storage, handle, scratch, |bytes| {
            assert_eq!(bytes, expected);
            bytes.len()
        })
        .unwrap();
    assert_eq!(len, expected.len());
}

fn record_info_for<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
) -> (ObjectLogRegion, ObjectLogRecordInfo) {
    let region = log.region_for_handle(handle).unwrap();
    let record = log.read_record_info(storage, region, handle).unwrap();
    (region, record)
}

fn object_end_for<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
) -> ObjectEndInfo {
    let (region, record) = record_info_for(log, storage, handle);
    log.read_object_end(storage, region, handle, record)
        .unwrap()
}

fn replay_into_memory<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
    memory: &mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
) -> Result<(), ObjectLogError> {
    replay_object_log::<IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_REGIONS, LOG_METADATA_MAX>(
        storage,
        collection_id,
        memory,
    )
}

fn seed_log_metadata<
    const REGION_SIZE: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    memory: &mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    metadata: &[u8],
) {
    let mut log = ObjectLog {
        collection_id: CollectionId::new(0),
        memory,
    };
    log.apply_log_metadata(metadata).unwrap();
}

fn memory_log_metadata<
    const REGION_SIZE: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    memory: &ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
) -> &[u8] {
    &memory.log_metadata[..memory.log_metadata_len]
}

fn append_raw_log_metadata_update<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
    metadata: &[u8],
) {
    let mut payload = [0u8; REGION_SIZE];
    let used = encode_set_log_metadata_update(metadata, &mut payload).unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::Update {
            collection_id,
            payload: &payload[..used],
        })
        .unwrap();
}

fn append_raw_inline_update<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
    handle: ObjectLogHandle,
    bytes: &[u8],
) {
    let mut payload = [0u8; REGION_SIZE];
    let encoded = encode_inline_append_update(handle, bytes, &mut payload).unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::Update {
            collection_id,
            payload: &payload[..encoded.used],
        })
        .unwrap();
}

fn raw_inline_handle(metadata: &[u8]) -> ObjectLogHandle {
    ObjectLogHandle::new(
        3,
        0,
        u32::try_from(Header::ENCODED_LEN + data_prologue_len(metadata.len()).unwrap()).unwrap(),
    )
}

fn assert_replayed_inline_object<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    memory: &mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    collection_id: CollectionId,
    handle: ObjectLogHandle,
    expected: &[u8],
) {
    let log = ObjectLog {
        collection_id,
        memory,
    };
    assert_get(&log, storage, handle, expected);
}

fn assert_no_replayed_inline_object<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    memory: &mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    collection_id: CollectionId,
    handle: ObjectLogHandle,
) {
    let log = ObjectLog {
        collection_id,
        memory,
    };
    let mut scratch = [0u8; 32];
    assert!(matches!(
        log.get(storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
}

fn write_region_or_frontier<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &mut ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    region: ObjectLogRegion,
    absolute_offset: u32,
    bytes: &[u8],
) -> Option<[u8; REGION_SIZE]> {
    if region.flushed {
        storage
            .backing
            .write_region(
                region.region_index,
                usize::try_from(absolute_offset).unwrap(),
                bytes,
            )
            .unwrap();
        None
    } else {
        let original = log.memory.frontier_payload;
        let start = payload_offset(absolute_offset).unwrap();
        log.memory.frontier_payload[start..start + bytes.len()].copy_from_slice(bytes);
        Some(original)
    }
}

fn restore_region_or_frontier<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &mut ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    region: ObjectLogRegion,
    saved_frontier: Option<[u8; REGION_SIZE]>,
    saved_region: &[u8; REGION_SIZE],
) {
    if let Some(frontier) = saved_frontier {
        log.memory.frontier_payload = frontier;
    } else {
        storage
            .backing
            .write_region(region.region_index, 0, saved_region)
            .unwrap();
    }
}

fn refresh_record_crc<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &mut ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    region: ObjectLogRegion,
    record: ObjectLogRecordInfo,
) {
    let crc = if region.flushed {
        storage
            .backing
            .read_region(
                region.region_index,
                record.body_start,
                record.body_len,
                crc32,
            )
            .unwrap()
    } else {
        let body_start = payload_offset(u32::try_from(record.body_start).unwrap()).unwrap();
        let body_end = body_start + record.body_len;
        crc32(&log.memory.frontier_payload[body_start..body_end])
    };
    let crc_offset = record.body_start - size_of::<u32>();
    if region.flushed {
        storage
            .backing
            .write_region(region.region_index, crc_offset, &crc.to_le_bytes())
            .unwrap();
    } else {
        let crc_offset = payload_offset(u32::try_from(crc_offset).unwrap()).unwrap();
        log.memory.frontier_payload[crc_offset..crc_offset + size_of::<u32>()]
            .copy_from_slice(&crc.to_le_bytes());
    }
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-025` Object-log durable state MUST be canonical and
//# self-delimiting: persisted handles, data-region prologues, object records,
//# snapshots, and WAL update payloads MUST accept exact valid boundaries and
//# reject padding, trailing bytes, malformed bounds, unknown tags, metadata
//# changes, and record-body requests that cannot be valid for the encoded
//# object kind.
#[test]
fn requirement_object_log_durable_state_is_canonical_and_self_delimiting() {
    check_object_log_layout_lengths_are_exact();
    check_object_log_helper_boundaries_are_exact();
    check_object_log_state_application_validates_exact_edges();
    check_object_log_read_helpers_validate_exact_storage_scratch_boundaries();
    check_object_log_flushed_region_metadata_length_bounds_are_exact();
    check_object_log_snapshot_decode_rejects_corrupt_region_metadata();
    check_object_log_open_state_validates_region_metadata_bounds();
    check_object_log_update_payloads_validate_truncate_and_materialized_region_records();
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-026` Object-log append placement MUST preserve stable handles
//# and forward progress at region boundaries: exact-fit inline objects MUST use
//# the current reserved frontier, objects too large for inline representation
//# MUST use large-object records, empty or already-materialized frontiers MUST
//# not be materialized twice, impossible no-progress large-object geometry MUST
//# fail, and nonempty full frontiers MUST be materialized before continuing in a
//# newly reserved frontier.
#[test]
fn requirement_object_log_append_placement_preserves_handles_and_progress() {
    check_object_log_exact_fit_capacity_boundaries_are_stable();
    check_object_log_direct_inline_append_routing_does_not_start_transactions();
    check_object_log_large_append_rejects_zero_chunk_capacity_frontier();
    check_object_log_large_append_progresses_past_full_nonempty_frontier();
    check_object_log_empty_or_flushed_frontiers_are_not_materialized_again();
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-027` Object-log WAL replay MUST rebuild only the target
//# object-log collection: records for other collection ids or collection types
//# MUST NOT alter target state, and lifecycle or transaction markers MUST affect
//# target updates only when the marker belongs to the target collection.
#[test]
fn requirement_object_log_wal_replay_is_collection_scoped() {
    check_object_log_replay_filters_unrelated_collection_records();
    check_object_log_replay_new_collection_filters_collection_and_type();
    check_object_log_replay_ignores_unrelated_begin_and_commit_markers();
    check_object_log_replay_filters_transaction_finished_markers();
    check_object_log_replay_filters_rollback_markers();
    check_object_log_replay_drop_clears_only_target_collection();
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-024` Object-log reads MUST treat caller scratch length as a
//# minimum capacity requirement: buffers at least as long as the returned whole
//# object or requested range MUST succeed, including exact-size buffers.
#[test]
fn requirement_object_log_reads_treat_scratch_as_minimum_capacity() {
    check_object_log_reads_accept_exact_scratch_lengths();
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-028` Before returning object bytes, Object-log reads MUST
//# validate that flushed data-region headers and prologues still identify the
//# live collection and that large-object chunk runs expose only public
//# `ObjectEnd` handles with valid private chunk body lengths, flags, links,
//# logical positions, and CRCs.
#[test]
fn requirement_object_log_reads_validate_identity_and_large_chunk_runs() {
    check_object_log_flushed_region_prologue_is_validated_on_read();
    check_object_log_large_object_reads_reject_private_or_malformed_chunks();
}

fn check_object_log_layout_lengths_are_exact() {
    assert_eq!(HANDLE_ENCODED_LEN, 16);
    assert_eq!(DATA_PROLOGUE_FIXED_LEN, 18);
    assert_eq!(RECORD_HEADER_LEN, 9);
    assert_eq!(OBJECT_CHUNK_FIXED_BODY_LEN, 45);
    assert_eq!(OBJECT_END_BODY_LEN, 40);
    assert_eq!(OBJECT_CHUNK_FLAGS_VALID_MASK, 0x03);

    assert_eq!(
        data_prologue_len(LOG_METADATA.len()).unwrap(),
        DATA_PROLOGUE_FIXED_LEN + LOG_METADATA.len()
    );
    assert_eq!(record_len(0).unwrap(), RECORD_HEADER_LEN);
    assert_eq!(inline_record_len(7).unwrap(), RECORD_HEADER_LEN + 7);
    assert_eq!(
        chunk_record_len(5).unwrap(),
        RECORD_HEADER_LEN + OBJECT_CHUNK_FIXED_BODY_LEN + 5
    );
    assert_eq!(
        end_record_len().unwrap(),
        RECORD_HEADER_LEN + OBJECT_END_BODY_LEN
    );
}

fn check_object_log_helper_boundaries_are_exact() {
    let both_chunk_links = OBJECT_CHUNK_FLAG_PREV_VALID + OBJECT_CHUNK_FLAG_NEXT_VALID;
    assert_eq!(OBJECT_CHUNK_FLAGS_VALID_MASK, both_chunk_links);
    for flags in [
        0,
        OBJECT_CHUNK_FLAG_PREV_VALID,
        OBJECT_CHUNK_FLAG_NEXT_VALID,
        OBJECT_CHUNK_FLAGS_VALID_MASK,
    ] {
        validate_chunk_flags(flags).unwrap();
    }
    assert!(matches!(
        validate_chunk_flags(OBJECT_CHUNK_FLAGS_VALID_MASK + 1),
        Err(ObjectLogError::InvalidFrame)
    ));

    assert!(record_type_is_public(RECORD_INLINE_OBJECT));
    assert!(!record_type_is_public(RECORD_OBJECT_CHUNK));
    assert!(record_type_is_public(RECORD_OBJECT_END));

    validate_log_metadata_len::<3>(1).unwrap();
    validate_log_metadata_len::<3>(3).unwrap();
    assert!(matches!(
        validate_log_metadata_len::<3>(0),
        Err(ObjectLogError::LogMetadataEmpty)
    ));
    assert!(matches!(
        validate_log_metadata_len::<3>(4),
        Err(ObjectLogError::LogMetadataTooLarge {
            len: 4,
            capacity: 3
        })
    ));
    assert_eq!(next_sequence_after(0).unwrap(), 1);
    assert!(matches!(
        next_sequence_after(u64::MAX),
        Err(ObjectLogError::InvalidEncoding)
    ));

    let metadata = StorageMetadata::new(512, 8, 1, 8, 0xff, 0xa5).unwrap();
    assert_eq!(committed_payload_capacity::<512>(metadata).unwrap(), 482);
    let unaligned_metadata = StorageMetadata::new(512, 8, 1, 16, 0xff, 0xa5).unwrap();
    assert_eq!(
        committed_payload_capacity::<512>(unaligned_metadata).unwrap(),
        474
    );

    let prologue_len = data_prologue_len(LOG_METADATA.len()).unwrap();
    let mut prologue = [0u8; DATA_PROLOGUE_FIXED_LEN + LOG_METADATA.len()];
    encode_data_prologue(9, LOG_METADATA, &mut prologue).unwrap();
    let mut oversized_prologue = [0u8; DATA_PROLOGUE_FIXED_LEN + LOG_METADATA.len() + 1];
    encode_data_prologue(9, LOG_METADATA, &mut oversized_prologue).unwrap();
    assert_eq!(&oversized_prologue[..DATA_MAGIC.len()], &DATA_MAGIC);
    let mut short_prologue = [0u8; DATA_PROLOGUE_FIXED_LEN + LOG_METADATA.len() - 1];
    assert!(matches!(
        encode_data_prologue(9, LOG_METADATA, &mut short_prologue),
        Err(ObjectLogError::BufferTooSmall {
            needed,
            available
        }) if needed == prologue_len && available == prologue_len - 1
    ));

    let first = ObjectLogHandle::new(1, 2, 3);
    let last = ObjectLogHandle::new(4, 5, 6);
    let mut end_record = [0u8; RECORD_HEADER_LEN + OBJECT_END_BODY_LEN];
    assert_eq!(
        encode_end_record(7, first, last, &mut end_record).unwrap(),
        end_record.len()
    );
    let mut oversized_end_record = [0u8; RECORD_HEADER_LEN + OBJECT_END_BODY_LEN + 1];
    assert_eq!(
        encode_end_record(7, first, last, &mut oversized_end_record).unwrap(),
        end_record.len()
    );
    let mut short_end_record = [0u8; RECORD_HEADER_LEN + OBJECT_END_BODY_LEN - 1];
    assert!(matches!(
        encode_end_record(7, first, last, &mut short_end_record),
        Err(ObjectLogError::BufferTooSmall { .. })
    ));

    let typed_len = inline_record_len(3).unwrap();
    let mut typed_record = [0u8; RECORD_HEADER_LEN + 3];
    assert_eq!(
        encode_typed_record(RECORD_INLINE_OBJECT, b"abc", &mut typed_record).unwrap(),
        typed_len
    );
    let mut oversized_typed_record = [0u8; RECORD_HEADER_LEN + 4];
    assert_eq!(
        encode_typed_record(RECORD_INLINE_OBJECT, b"abc", &mut oversized_typed_record).unwrap(),
        typed_len
    );
    let mut short_typed_record = [0u8; RECORD_HEADER_LEN + 2];
    assert!(matches!(
        encode_typed_record(RECORD_INLINE_OBJECT, b"abc", &mut short_typed_record),
        Err(ObjectLogError::BufferTooSmall { .. })
    ));

    let mut header = [0u8; RECORD_HEADER_LEN];
    encode_record_header_parts(RECORD_INLINE_OBJECT, 3, 0x1122_3344, &mut header).unwrap();
    let mut oversized_header = [0u8; RECORD_HEADER_LEN + 1];
    encode_record_header_parts(RECORD_INLINE_OBJECT, 3, 0x1122_3344, &mut oversized_header)
        .unwrap();
    let mut short_header = [0u8; RECORD_HEADER_LEN - 1];
    assert!(matches!(
        encode_record_header_parts(RECORD_INLINE_OBJECT, 3, 0x1122_3344, &mut short_header),
        Err(ObjectLogError::BufferTooSmall { .. })
    ));

    let mut chunk_record = [0u8; RECORD_HEADER_LEN + OBJECT_CHUNK_FIXED_BODY_LEN + 3];
    let chunk_record_len = encode_chunk_record(
        OBJECT_CHUNK_FLAG_PREV_VALID | OBJECT_CHUNK_FLAG_NEXT_VALID,
        11,
        first,
        last,
        b"abc",
        &mut chunk_record,
    )
    .unwrap();
    assert_eq!(chunk_record_len, chunk_record.len());
    let chunk_body = &chunk_record[RECORD_HEADER_LEN..];
    validate_record_body_shape(RECORD_OBJECT_CHUNK, chunk_body).unwrap();
    let mut zero_chunk_record = [0u8; RECORD_HEADER_LEN + OBJECT_CHUNK_FIXED_BODY_LEN];
    encode_chunk_record(0, 0, first, last, &[], &mut zero_chunk_record).unwrap();
    let zero_chunk_body = &zero_chunk_record[RECORD_HEADER_LEN..];
    decode_chunk_body_prefix(
        &zero_chunk_body[..OBJECT_CHUNK_FIXED_BODY_LEN],
        OBJECT_CHUNK_FIXED_BODY_LEN,
    )
    .unwrap();
    assert!(matches!(
        validate_record_body_shape(
            RECORD_OBJECT_CHUNK,
            &chunk_body[..OBJECT_CHUNK_FIXED_BODY_LEN - 1]
        ),
        Err(ObjectLogError::InvalidFrame)
    ));
    decode_chunk_body_prefix(&chunk_body[..OBJECT_CHUNK_FIXED_BODY_LEN], chunk_body.len()).unwrap();
    assert!(matches!(
        decode_chunk_body_prefix(
            &chunk_body[..OBJECT_CHUNK_FIXED_BODY_LEN - 1],
            chunk_body.len()
        ),
        Err(ObjectLogError::InvalidFrame)
    ));
    assert!(matches!(
        decode_chunk_body_prefix(
            &chunk_body[..OBJECT_CHUNK_FIXED_BODY_LEN],
            OBJECT_CHUNK_FIXED_BODY_LEN - 1
        ),
        Err(ObjectLogError::InvalidFrame)
    ));
    assert!(matches!(
        decode_chunk_body_prefix(
            &chunk_body[..OBJECT_CHUNK_FIXED_BODY_LEN],
            chunk_body.len() - 1
        ),
        Err(ObjectLogError::InvalidFrame)
    ));

    validate_record_body_shape(RECORD_INLINE_OBJECT, &[]).unwrap();
    validate_record_body_shape(RECORD_OBJECT_END, &end_record[RECORD_HEADER_LEN..]).unwrap();
    assert!(matches!(
        validate_record_body_shape(
            RECORD_OBJECT_END,
            &end_record[RECORD_HEADER_LEN..end_record.len() - 1]
        ),
        Err(ObjectLogError::InvalidFrame)
    ));

    let exact = checked_object_read_range_u64(10, 10, 0, 0).unwrap();
    assert_eq!(exact.offset, 10);
    assert_eq!(exact.len, 0);
    let exact_end = checked_object_read_range_u64(10, 9, 1, 1).unwrap();
    assert_eq!(exact_end.offset, 9);
    assert_eq!(exact_end.len, 1);
    assert!(matches!(
        checked_object_read_range_u64(10, 11, 0, 0),
        Err(ObjectLogError::ObjectRangeOutOfBounds { .. })
    ));
    assert!(matches!(
        checked_object_read_range_u64(10, 10, 1, 1),
        Err(ObjectLogError::ObjectRangeOutOfBounds { .. })
    ));
    assert!(matches!(
        checked_object_read_range_u64(10, 0, 1, 0),
        Err(ObjectLogError::BufferTooSmall {
            needed: 1,
            available: 0
        })
    ));
    assert!(matches!(
        checked_object_read_range_u64(u64::MAX, u64::MAX, 1, 1),
        Err(ObjectLogError::LengthOverflow)
    ));
}

fn check_object_log_state_application_validates_exact_edges() {
    let exact_metadata = [0x42u8; 24];
    let mut exact_memory = ObjectLogMemory::<64, 2, 32>::new();
    let mut exact_log = ObjectLog {
        collection_id: CollectionId::new(1),
        memory: &mut exact_memory,
    };
    exact_log.apply_log_metadata(&exact_metadata).unwrap();
    exact_log
        .install_reserved_frontier(ReservedObjectLogRegion {
            region_index: 3,
            sequence: 0,
        })
        .unwrap();
    assert_eq!(exact_log.memory.regions[0].start_offset, 64);
    assert_eq!(exact_log.memory.regions[0].end_offset, 64);

    let mut memory = ObjectLogMemory::<64, 4, 16>::new();
    let mut log = ObjectLog {
        collection_id: CollectionId::new(2),
        memory: &mut memory,
    };
    log.apply_log_metadata(b"x").unwrap();
    log.install_reserved_frontier(ReservedObjectLogRegion {
        region_index: 4,
        sequence: 0,
    })
    .unwrap();
    let handle = ObjectLogHandle::new(4, 0, log.memory.regions[0].start_offset);
    let payload_start = payload_offset(handle.offset).unwrap();
    let record_len = log.memory.frontier_payload.len() - payload_start;
    let body = std::vec![0x77u8; record_len - RECORD_HEADER_LEN];
    let mut record = std::vec![0u8; record_len];
    encode_inline_record(&body, &mut record).unwrap();
    log.apply_append_record(handle, &record, AppendVisibility::Committed)
        .unwrap();
    assert_eq!(
        payload_offset(log.memory.regions[0].end_offset).unwrap(),
        log.memory.frontier_payload.len()
    );

    let payload_capacity = log.memory.frontier_payload.len();
    assert!(!log.needs_new_region(0, payload_capacity).unwrap());
    assert!(log.needs_new_region(1, payload_capacity).unwrap());
    assert_eq!(log.find_region(4, 1), None);

    let mut roomy_memory = ObjectLogMemory::<64, 4, 16>::new();
    let mut roomy_log = ObjectLog {
        collection_id: CollectionId::new(22),
        memory: &mut roomy_memory,
    };
    roomy_log.apply_log_metadata(b"x").unwrap();
    roomy_log
        .install_reserved_frontier(ReservedObjectLogRegion {
            region_index: 9,
            sequence: 0,
        })
        .unwrap();
    assert!(!roomy_log.needs_new_region(1, payload_capacity).unwrap());
    assert_eq!(roomy_log.find_region(9, 1), None);
    roomy_log.memory.frontier_payload[63] = 0x5a;
    roomy_log.checkpoint_append_state().unwrap();
    assert_eq!(
        roomy_log.memory.rollback_regions.as_slice(),
        roomy_log.memory.regions.as_slice()
    );
    assert_eq!(
        roomy_log.memory.rollback_frontier_payload,
        roomy_log.memory.frontier_payload
    );
    roomy_log.clear_append_checkpoint();
    assert!(roomy_log.memory.rollback_regions.is_empty());

    assert!(matches!(
        log.apply_append_record(handle, &record, AppendVisibility::Planned),
        Err(ObjectLogError::InvalidHandle)
    ));
    log.memory.regions[0].end_offset = handle.offset;
    log.memory.regions[0].committed_end_offset = handle.offset;
    log.memory.regions[0].flushed = true;
    assert!(matches!(
        log.apply_append_record(handle, &record, AppendVisibility::Planned),
        Err(ObjectLogError::InvalidHandle)
    ));

    let object_start =
        u32::try_from(Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap())
            .unwrap();
    let empty_flushed = ObjectLogRegion {
        region_index: 7,
        sequence: 1,
        start_offset: object_start,
        end_offset: object_start,
        committed_end_offset: object_start,
        first_committed_public_offset: None,
        first_planned_public_offset: None,
        flushed: true,
    };
    let mut replay_memory = ObjectLogMemory::<512, 4, 16>::new();
    let mut replay_log = ObjectLog {
        collection_id: CollectionId::new(3),
        memory: &mut replay_memory,
    };
    replay_log.apply_log_metadata(LOG_METADATA).unwrap();
    replay_log
        .apply_materialized_region(empty_flushed, AppendVisibility::Committed)
        .unwrap();

    let mut invalid = empty_flushed;
    invalid.flushed = false;
    assert!(matches!(
        replay_log.apply_materialized_region(invalid, AppendVisibility::Committed),
        Err(ObjectLogError::InvalidEncoding)
    ));
    invalid = empty_flushed;
    invalid.committed_end_offset = invalid.end_offset + 1;
    assert!(matches!(
        replay_log.apply_materialized_region(invalid, AppendVisibility::Committed),
        Err(ObjectLogError::InvalidEncoding)
    ));

    let mut same_start = empty_flushed;
    same_start.end_offset += u32::try_from(inline_record_len(1).unwrap()).unwrap();
    replay_log
        .apply_materialized_region(same_start, AppendVisibility::Planned)
        .unwrap();
    let mut different_start = same_start;
    different_start.start_offset += 1;
    assert!(matches!(
        replay_log.apply_materialized_region(different_start, AppendVisibility::Planned),
        Err(ObjectLogError::InvalidEncoding)
    ));

    replay_log.apply_log_metadata(LOG_METADATA).unwrap();
    assert!(matches!(
        replay_log.apply_log_metadata(b"different"),
        Err(ObjectLogError::InvalidEncoding)
    ));

    let truncate_start =
        u32::try_from(Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap())
            .unwrap();
    let truncate_end = truncate_start + u32::try_from(inline_record_len(1).unwrap()).unwrap();
    let retained_region = ObjectLogRegion {
        region_index: 11,
        sequence: 4,
        start_offset: truncate_start,
        end_offset: truncate_end,
        committed_end_offset: truncate_end,
        first_committed_public_offset: Some(truncate_start),
        first_planned_public_offset: None,
        flushed: false,
    };
    let public_region = ObjectLogRegion {
        region_index: 12,
        sequence: 5,
        ..retained_region
    };
    let retained = ObjectLogHandle::new(
        retained_region.region_index,
        retained_region.sequence,
        truncate_start,
    );
    let public = ObjectLogHandle::new(
        public_region.region_index,
        public_region.sequence,
        truncate_start,
    );
    let mut truncate_memory = ObjectLogMemory::<512, 4, 16>::new();
    let mut truncate_log = ObjectLog {
        collection_id: CollectionId::new(4),
        memory: &mut truncate_memory,
    };
    truncate_log.apply_log_metadata(LOG_METADATA).unwrap();
    truncate_log.memory.regions.push(retained_region).unwrap();
    truncate_log.memory.regions.push(public_region).unwrap();
    let mut freed = Vec::<u32, 4>::new();
    let invalid_retained = ObjectLogHandle::new(
        retained_region.region_index,
        retained_region.sequence,
        retained_region.committed_end_offset,
    );
    assert!(matches!(
        truncate_log.apply_truncate_before(public, invalid_retained, &mut freed),
        Err(ObjectLogError::InvalidHandle)
    ));

    let mut truncate_memory = ObjectLogMemory::<512, 4, 16>::new();
    let mut truncate_log = ObjectLog {
        collection_id: CollectionId::new(4),
        memory: &mut truncate_memory,
    };
    truncate_log.apply_log_metadata(LOG_METADATA).unwrap();
    truncate_log.memory.regions.push(retained_region).unwrap();
    truncate_log.memory.regions.push(public_region).unwrap();
    let invalid_public = ObjectLogHandle::new(
        public_region.region_index,
        public_region.sequence,
        public_region.committed_end_offset,
    );
    assert!(matches!(
        truncate_log.apply_truncate_before(invalid_public, retained, &mut freed),
        Err(ObjectLogError::InvalidHandle)
    ));
}

fn check_object_log_exact_fit_capacity_boundaries_are_stable() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 24;

    let mut log_metadata = [0x42u8; 192];
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata()).unwrap();
    let footer_offset = REGION_SIZE - FreePointerFooter::ENCODED_LEN;
    let aligned_footer_boundary =
        footer_offset - footer_offset % storage.metadata().wal_write_granule as usize;
    assert_eq!(
        payload_capacity,
        aligned_footer_boundary - Header::ENCODED_LEN
    );

    let object_capacity =
        empty_region_record_capacity(payload_capacity, log_metadata.len()).unwrap();
    let exact_inline_len = object_capacity - RECORD_HEADER_LEN;
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 224>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, &log_metadata).unwrap();
    let exact = std::vec![0x5au8; exact_inline_len];
    let exact_handle = log.append(&mut storage, &exact).unwrap();
    let (region, record) = record_info_for(&log, &mut storage, exact_handle);
    assert_eq!(record.record_type, RECORD_INLINE_OBJECT);
    assert_eq!(record.body_len, exact_inline_len);
    assert_eq!(
        record.record_end as usize,
        Header::ENCODED_LEN + payload_capacity
    );
    assert_eq!(region.end_offset, record.record_end);
    assert_eq!(region.committed_end_offset, record.record_end);

    let next = log.append(&mut storage, b"x").unwrap();
    assert_ne!(next.region_index, exact_handle.region_index);
    assert_eq!(next.sequence, exact_handle.sequence + 1);
    let mut scratch = std::vec![0u8; exact.len()];
    assert_get_bytes(&log, &mut storage, exact_handle, &exact, &mut scratch);
    assert_get(&log, &mut storage, next, b"x");

    log_metadata[0] = 0x24;
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 224>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, &log_metadata).unwrap();
    let too_large_for_inline = std::vec![0x33u8; exact_inline_len + 1];
    let large_handle = log.append(&mut storage, &too_large_for_inline).unwrap();
    let (_, large_record) = record_info_for(&log, &mut storage, large_handle);
    assert_eq!(large_record.record_type, RECORD_OBJECT_END);
}

fn check_object_log_direct_inline_append_routing_does_not_start_transactions() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 16;

    let log_metadata = [0x42u8; 300];
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata()).unwrap();
    let object_capacity =
        empty_region_record_capacity(payload_capacity, log_metadata.len()).unwrap();
    let exact_inline_len = object_capacity - RECORD_HEADER_LEN;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 320>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, &log_metadata).unwrap();
    log.install_reserved_frontier(ReservedObjectLogRegion {
        region_index: 1,
        sequence: 0,
    })
    .unwrap();
    let begin_before = count_wal_records(&mut storage, WalRecordType::BeginTransaction);
    let exact = std::vec![0x5au8; exact_inline_len];
    let exact_handle = log.append_inner(&mut storage, &exact).unwrap();
    assert_eq!(exact_handle.offset, log.memory.regions[0].start_offset);
    assert_eq!(
        log.memory.regions[0].end_offset as usize,
        Header::ENCODED_LEN + payload_capacity
    );
    assert_eq!(
        count_wal_records(&mut storage, WalRecordType::BeginTransaction),
        begin_before
    );

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 320>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, &log_metadata).unwrap();
    log.install_reserved_frontier(ReservedObjectLogRegion {
        region_index: 2,
        sequence: 0,
    })
    .unwrap();
    let begin_before = count_wal_records(&mut storage, WalRecordType::BeginTransaction);
    let small = log.append_inner(&mut storage, b"x").unwrap();
    assert_eq!(small.offset, log.memory.regions[0].start_offset);
    assert_eq!(
        count_wal_records(&mut storage, WalRecordType::BeginTransaction),
        begin_before
    );
}

fn check_object_log_large_append_rejects_zero_chunk_capacity_frontier() {
    const REGION_SIZE: usize = 128;
    const REGION_COUNT: usize = 12;

    let log_metadata = [0x42u8; 32];
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata()).unwrap();
    let object_capacity =
        empty_region_record_capacity(payload_capacity, log_metadata.len()).unwrap();
    let inline_body_capacity = object_capacity - RECORD_HEADER_LEN;
    assert_eq!(
        Header::ENCODED_LEN
            + data_prologue_len(log_metadata.len()).unwrap()
            + RECORD_HEADER_LEN
            + inline_body_capacity,
        Header::ENCODED_LEN + payload_capacity
    );

    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 64>::new();
    let mut log = ObjectLog {
        collection_id: CollectionId::new(7),
        memory: &mut memory,
    };
    log.apply_log_metadata(&log_metadata).unwrap();
    log.install_reserved_frontier(ReservedObjectLogRegion {
        region_index: 1,
        sequence: 0,
    })
    .unwrap();
    let too_large_for_inline = std::vec![0x11u8; inline_body_capacity + 1];
    let mut allocated_regions = Vec::<u32, REGION_COUNT>::new();
    assert!(matches!(
        log.append_large_transactional(&mut storage, &too_large_for_inline, &mut allocated_regions),
        Err(ObjectLogError::ObjectTooLarge {
            len,
            capacity
        }) if len == too_large_for_inline.len() && capacity == payload_capacity
    ));
}

fn check_object_log_large_append_progresses_past_full_nonempty_frontier() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 18;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata()).unwrap();
    let inline_body_capacity = empty_region_record_capacity(payload_capacity, LOG_METADATA.len())
        .unwrap()
        - RECORD_HEADER_LEN;
    let mut memory = ObjectLogMemory::<REGION_SIZE, 8, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    storage
        .memory
        .state
        .begin_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
            storage.backing,
            &mut storage.memory.workspace,
            log.collection_id,
        )
        .unwrap();
    let mut allocated_regions = Vec::<u32, REGION_COUNT>::new();
    let reserved = log
        .reserve_region(&mut storage, &mut allocated_regions)
        .unwrap();
    log.install_reserved_frontier(reserved).unwrap();
    let filler = std::vec![0x41u8; inline_body_capacity];
    let region = log.memory.regions.last().copied().unwrap();
    let filler_handle =
        ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
    let mut filler_record = std::vec![0u8; inline_record_len(filler.len()).unwrap()];
    encode_inline_record(&filler, &mut filler_record).unwrap();
    log.apply_append_record(filler_handle, &filler_record, AppendVisibility::Committed)
        .unwrap();
    let (_, filler_record) = record_info_for(&log, &mut storage, filler_handle);
    assert_eq!(
        usize::try_from(filler_record.record_end).unwrap(),
        Header::ENCODED_LEN + payload_capacity
    );
    log.checkpoint_append_state().unwrap();

    let mut object = std::vec![0u8; inline_body_capacity + 512];
    fill_pattern(&mut object);
    let handle = log
        .append_large_transactional(&mut storage, &object, &mut allocated_regions)
        .unwrap();
    storage
        .memory
        .state
        .commit_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
            storage.backing,
            &mut storage.memory.workspace,
            log.collection_id,
        )
        .unwrap();
    log.commit_staged_appends();
    log.clear_append_checkpoint();
    storage
        .memory
        .state
        .finish_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
            storage.backing,
            &mut storage.memory.workspace,
            log.collection_id,
        )
        .unwrap();
    assert!(log.memory.regions.iter().any(|region| region.flushed));
    let mut scratch = std::vec![0u8; object.len()];
    assert_get_bytes(&log, &mut storage, handle, &object, &mut scratch);
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-001` Appending an object MUST return an
//# opaque `ObjectLogHandle` that names a committed object record, and reopening
//# the collection MUST reconstruct unflushed frontier objects from retained WAL
//# updates.
#[test]
fn requirement_object_log_replays_unflushed_frontier_from_wal_updates() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, handle) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
        let handle = log.append(&mut storage, b"alpha").unwrap();

        assert_get(&log, &mut storage, handle, b"alpha");
        (log.collection_id(), handle)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    assert_get(&reopened_log, &mut reopened, handle, b"alpha");
}

fn check_object_log_replay_filters_unrelated_collection_records() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 16;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let (target_id, target_handle, other_id) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let (target_id, target_handle) = {
            let mut target_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
            let mut target = ObjectLog::new(&mut storage, &mut target_memory, b"target").unwrap();
            let handle = target.append(&mut storage, b"alpha").unwrap();
            target.flush(&mut storage).unwrap();
            (target.collection_id(), handle)
        };
        let other_id = {
            let mut other_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
            let mut other = ObjectLog::new(&mut storage, &mut other_memory, b"other").unwrap();
            other.append(&mut storage, b"beta").unwrap();
            let _: ObjectLogHandle = other
                .transaction(&mut storage, |tx| tx.append(b"committed-other"))
                .unwrap();
            let failed: Result<(), ObjectLogError> = other.transaction(&mut storage, |tx| {
                let _ = tx.append(b"rolled-back-other")?;
                Err(ObjectLogError::InvalidHandle)
            });
            assert!(matches!(failed, Err(ObjectLogError::InvalidHandle)));
            other.flush(&mut storage).unwrap();
            other.collection_id()
        };
        storage.append_drop_collection(other_id).unwrap();
        (target_id, target_handle, other_id)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let log = ObjectLog::open(target_id, &mut reopened, &mut memory).unwrap();
    assert_get(&log, &mut reopened, target_handle, b"alpha");
    assert_ne!(target_id, other_id);
}

fn check_object_log_replay_new_collection_filters_collection_and_type() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let target_id = CollectionId::new(7);
    let other_id = CollectionId::new(8);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::NewCollection {
            collection_id: other_id,
            collection_type: CollectionType::OBJECT_LOG_CODE,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::NewCollection {
            collection_id: target_id,
            collection_type: CollectionType::MAP_CODE,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    seed_log_metadata(&mut memory, b"seed");
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_eq!(memory_log_metadata(&memory), b"seed");

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::NewCollection {
            collection_id: target_id,
            collection_type: CollectionType::OBJECT_LOG_CODE,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    seed_log_metadata(&mut memory, b"seed");
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_eq!(memory_log_metadata(&memory), b"");
}

fn check_object_log_replay_ignores_unrelated_begin_and_commit_markers() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let target_id = CollectionId::new(7);
    let other_id = CollectionId::new(8);
    let metadata = b"target";
    let handle = raw_inline_handle(metadata);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    append_raw_log_metadata_update(&mut storage, target_id, metadata);
    storage
        .append_raw_wal_record_for_test(WalRecord::BeginTransaction {
            collection_id: other_id,
        })
        .unwrap();
    append_raw_inline_update(&mut storage, target_id, handle, b"alpha");
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_replayed_inline_object(&mut storage, &mut memory, target_id, handle, b"alpha");

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    append_raw_log_metadata_update(&mut storage, target_id, metadata);
    storage
        .append_raw_wal_record_for_test(WalRecord::BeginTransaction {
            collection_id: target_id,
        })
        .unwrap();
    append_raw_inline_update(&mut storage, target_id, handle, b"beta");
    storage
        .append_raw_wal_record_for_test(WalRecord::CommitTransaction {
            collection_id: other_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::TransactionFinished {
            collection_id: target_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::RollbackTransaction {
            collection_id: target_id,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_no_replayed_inline_object(&mut storage, &mut memory, target_id, handle);
}

fn check_object_log_replay_filters_transaction_finished_markers() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let target_id = CollectionId::new(7);
    let other_id = CollectionId::new(8);
    let metadata = b"target";
    let handle = raw_inline_handle(metadata);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    append_raw_log_metadata_update(&mut storage, target_id, metadata);
    storage
        .append_raw_wal_record_for_test(WalRecord::BeginTransaction {
            collection_id: target_id,
        })
        .unwrap();
    append_raw_inline_update(&mut storage, target_id, handle, b"gamma");
    storage
        .append_raw_wal_record_for_test(WalRecord::CommitTransaction {
            collection_id: target_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::TransactionFinished {
            collection_id: target_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::RollbackTransaction {
            collection_id: target_id,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_replayed_inline_object(&mut storage, &mut memory, target_id, handle, b"gamma");

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    append_raw_log_metadata_update(&mut storage, target_id, metadata);
    storage
        .append_raw_wal_record_for_test(WalRecord::BeginTransaction {
            collection_id: target_id,
        })
        .unwrap();
    append_raw_inline_update(&mut storage, target_id, handle, b"delta");
    storage
        .append_raw_wal_record_for_test(WalRecord::CommitTransaction {
            collection_id: target_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::TransactionFinished {
            collection_id: other_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::RollbackTransaction {
            collection_id: target_id,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_no_replayed_inline_object(&mut storage, &mut memory, target_id, handle);
}

fn check_object_log_replay_filters_rollback_markers() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let target_id = CollectionId::new(7);
    let other_id = CollectionId::new(8);
    let metadata = b"target";
    let handle = raw_inline_handle(metadata);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    append_raw_log_metadata_update(&mut storage, target_id, metadata);
    storage
        .append_raw_wal_record_for_test(WalRecord::BeginTransaction {
            collection_id: target_id,
        })
        .unwrap();
    append_raw_inline_update(&mut storage, target_id, handle, b"epsilon");
    storage
        .append_raw_wal_record_for_test(WalRecord::RollbackTransaction {
            collection_id: other_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::CommitTransaction {
            collection_id: target_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::TransactionFinished {
            collection_id: target_id,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_replayed_inline_object(&mut storage, &mut memory, target_id, handle, b"epsilon");

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    append_raw_log_metadata_update(&mut storage, target_id, metadata);
    storage
        .append_raw_wal_record_for_test(WalRecord::BeginTransaction {
            collection_id: target_id,
        })
        .unwrap();
    append_raw_inline_update(&mut storage, target_id, handle, b"zeta");
    storage
        .append_raw_wal_record_for_test(WalRecord::RollbackTransaction {
            collection_id: target_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::CommitTransaction {
            collection_id: target_id,
        })
        .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::TransactionFinished {
            collection_id: target_id,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_no_replayed_inline_object(&mut storage, &mut memory, target_id, handle);
}

fn check_object_log_replay_drop_clears_only_target_collection() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let target_id = CollectionId::new(7);
    let other_id = CollectionId::new(8);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::DropCollection {
            collection_id: other_id,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    seed_log_metadata(&mut memory, b"seed");
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_eq!(memory_log_metadata(&memory), b"seed");

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage
        .append_raw_wal_record_for_test(WalRecord::DropCollection {
            collection_id: target_id,
        })
        .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    seed_log_metadata(&mut memory, b"seed");
    replay_into_memory(&mut storage, target_id, &mut memory).unwrap();
    assert_eq!(memory_log_metadata(&memory), b"");
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-015` Object-log range reads MUST accept `u64`
//# object-relative offset and length values, return only that committed byte
//# range, reject ranges outside the object, and require only enough caller
//# scratch for the requested range.
#[test]
fn requirement_object_log_range_reads_return_requested_subrange() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;
    const OBJECT: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
    let handle = log.append(&mut storage, OBJECT).unwrap();

    assert_get_range(&log, &mut storage, handle, 2, b"cdefg");
    let mut empty_scratch = [];
    assert_eq!(
        log.get_range(
            &mut storage,
            handle,
            OBJECT.len() as u64,
            0,
            &mut empty_scratch,
            |bytes| bytes.len(),
        )
        .unwrap(),
        0
    );

    let mut short_scratch = [0u8; 2];
    assert!(matches!(
        log.get_range(&mut storage, handle, 2, 3, &mut short_scratch, |_| ()),
        Err(ObjectLogError::BufferTooSmall {
            needed: 3,
            available: 2
        })
    ));
    let mut scratch = [0u8; 8];
    assert!(matches!(
        log.get_range(
            &mut storage,
            handle,
            (OBJECT.len() - 1) as u64,
            2,
            &mut scratch,
            |_| ()
        ),
        Err(ObjectLogError::ObjectRangeOutOfBounds {
            offset,
            len: 2,
            object_len
        }) if offset == (OBJECT.len() - 1) as u64 && object_len == OBJECT.len() as u64
    ));

    log.flush(&mut storage).unwrap();
    assert_get_range(&log, &mut storage, handle, 10, b"klmn");
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-016` Object-log whole-object reads MUST fail with a
//# buffer-too-small error that reports the stored object length when caller
//# scratch cannot hold the full object, and object-log length queries MUST return
//# the stored `u64` object length without returning object bytes.
#[test]
fn requirement_object_log_reports_object_len_and_full_read_buffer_size() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;
    const OBJECT: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
    let handle = log.append(&mut storage, OBJECT).unwrap();

    assert_eq!(
        log.get_object_len(&mut storage, handle).unwrap(),
        OBJECT.len() as u64
    );

    let mut short_scratch = [0u8; 8];
    assert!(matches!(
        log.get(&mut storage, handle, &mut short_scratch, |_| ()),
        Err(ObjectLogError::BufferTooSmall {
            needed,
            available: 8
        }) if needed == OBJECT.len()
    ));

    log.flush(&mut storage).unwrap();

    assert_eq!(
        log.get_object_len(&mut storage, handle).unwrap(),
        OBJECT.len() as u64
    );
    assert!(matches!(
        log.get(&mut storage, handle, &mut short_scratch, |_| ()),
        Err(ObjectLogError::BufferTooSmall {
            needed,
            available: 8
        }) if needed == OBJECT.len()
    ));
}

fn check_object_log_reads_accept_exact_scratch_lengths() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 96;
    const INLINE: &[u8] = b"exact";

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 32768>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let inline = log.append(&mut storage, INLINE).unwrap();
    let mut exact_inline = [0u8; INLINE.len()];
    assert_eq!(
        log.get(&mut storage, inline, &mut exact_inline, |bytes| {
            assert_eq!(bytes, INLINE);
            bytes.len()
        })
        .unwrap(),
        INLINE.len()
    );
    let mut exact_range = [0u8; 3];
    assert_eq!(
        log.get_range(&mut storage, inline, 1, 3, &mut exact_range, |bytes| {
            assert_eq!(bytes, b"xac");
            bytes.len()
        })
        .unwrap(),
        3
    );

    let mut object = [0u8; 420];
    fill_pattern(&mut object);
    let large = log.append(&mut storage, &object).unwrap();
    let mut exact_large = [0u8; 420];
    assert_get_bytes(&log, &mut storage, large, &object, &mut exact_large);
    let mut exact_large_range = [0u8; 17];
    assert_eq!(
        log.get_range(
            &mut storage,
            large,
            213,
            17,
            &mut exact_large_range,
            |bytes| {
                assert_eq!(bytes, &object[213..230]);
                bytes.len()
            },
        )
        .unwrap(),
        17
    );

    let mut short_large = [0u8; 419];
    assert!(matches!(
        log.get(&mut storage, large, &mut short_large, |_| ()),
        Err(ObjectLogError::BufferTooSmall {
            needed: 420,
            available: 419
        })
    ));
    let mut short_large_range = [0u8; 16];
    assert!(matches!(
        log.get_range(&mut storage, large, 213, 17, &mut short_large_range, |_| ()),
        Err(ObjectLogError::BufferTooSmall {
            needed: 17,
            available: 16
        })
    ));
}

fn check_object_log_read_helpers_validate_exact_storage_scratch_boundaries() {
    const REGION_SIZE: usize = 128;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let log = ObjectLog {
        collection_id: CollectionId::new(44),
        memory: &mut memory,
    };
    let flushed_region = ObjectLogRegion {
        region_index: 1,
        sequence: 0,
        start_offset: 0,
        end_offset: REGION_SIZE as u32,
        committed_end_offset: REGION_SIZE as u32,
        first_committed_public_offset: None,
        first_planned_public_offset: None,
        flushed: true,
    };
    let handle = ObjectLogHandle::new(1, 0, 0);
    let pattern = [0xabu8; REGION_SIZE];
    storage.backing.write_region(1, 0, &pattern).unwrap();
    let exact_record = ObjectLogRecordInfo {
        record_type: RECORD_INLINE_OBJECT,
        body_len: REGION_SIZE,
        body_crc32c: 0,
        body_start: 0,
        record_end: REGION_SIZE as u32,
    };
    log.read_record_body_into_storage_scratch(
        &mut storage,
        flushed_region,
        handle,
        exact_record,
        false,
    )
    .unwrap();
    assert_eq!(&storage.memory.payload_scratch[..], &pattern);
    log.read_record_body_prefix_into_storage_scratch(
        &mut storage,
        flushed_region,
        handle,
        exact_record,
        REGION_SIZE,
    )
    .unwrap();

    let short_record = ObjectLogRecordInfo {
        body_len: 1,
        ..exact_record
    };
    assert!(matches!(
        log.read_record_body_prefix_into_storage_scratch(
            &mut storage,
            flushed_region,
            handle,
            short_record,
            2
        ),
        Err(ObjectLogError::InvalidFrame)
    ));

    let first = ObjectLogHandle::new(1, 0, 9);
    let last = ObjectLogHandle::new(1, 0, 58);
    let mut end_record = [0u8; RECORD_HEADER_LEN + OBJECT_END_BODY_LEN];
    encode_end_record(7, first, last, &mut end_record).unwrap();
    storage
        .backing
        .write_region(1, 0, &end_record[RECORD_HEADER_LEN..])
        .unwrap();
    let wrong_type_end = ObjectLogRecordInfo {
        record_type: RECORD_INLINE_OBJECT,
        body_len: OBJECT_END_BODY_LEN,
        body_crc32c: crc32(&end_record[RECORD_HEADER_LEN..]),
        body_start: 0,
        record_end: OBJECT_END_BODY_LEN as u32,
    };
    assert!(matches!(
        log.read_object_end(&mut storage, flushed_region, handle, wrong_type_end),
        Err(ObjectLogError::InvalidFrame)
    ));
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-006` Flushing an object-log frontier MUST write the
//# frontier bytes into the previously reserved physical data region, persist
//# metadata sufficient to read flushed handles after reopen, and assign a
//# new sequence to a later reserved frontier region.
#[test]
fn requirement_object_log_handles_survive_flush_and_reopen() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, first, second) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();

        let first = log.append(&mut storage, b"alpha").unwrap();
        log.flush(&mut storage).unwrap();
        assert_get(&log, &mut storage, first, b"alpha");

        let second = log.append(&mut storage, b"beta").unwrap();
        assert_ne!(first.region_index, second.region_index);
        assert_ne!(first.sequence, second.sequence);
        assert_get(&log, &mut storage, second, b"beta");
        (log.collection_id(), first, second)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    assert_get(&reopened_log, &mut reopened, first, b"alpha");
    assert_get(&reopened_log, &mut reopened, second, b"beta");
}

fn check_object_log_empty_or_flushed_frontiers_are_not_materialized_again() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    log.install_reserved_frontier(ReservedObjectLogRegion {
        region_index: 1,
        sequence: 0,
    })
    .unwrap();
    let snapshots_before = count_wal_records(&mut storage, WalRecordType::Snapshot);
    log.flush_current(&mut storage).unwrap();
    assert!(!log.memory.regions[0].flushed);
    assert_eq!(
        count_wal_records(&mut storage, WalRecordType::Snapshot),
        snapshots_before
    );

    log.materialize_current_frontier_in_transaction(&mut storage)
        .unwrap();
    assert!(!log.memory.regions[0].flushed);

    let handle = log.append_inner(&mut storage, b"alpha").unwrap();
    log.flush_current(&mut storage).unwrap();
    assert!(log.memory.regions[0].flushed);
    let updates_before = count_wal_records(&mut storage, WalRecordType::Update);
    log.materialize_current_frontier_in_transaction(&mut storage)
        .unwrap();
    assert_eq!(
        count_wal_records(&mut storage, WalRecordType::Update),
        updates_before
    );
    let mut scratch = [0u8; 16];
    assert_get_bytes(&log, &mut storage, handle, b"alpha", &mut scratch);
}

//= spec/object-log.md#truncation
//= type=test
//# `RING-OBJECT-010` Truncating an object log MUST accept a live
//# `ObjectLogHandle` as an exclusive boundary, invalidate handles before that
//# boundary while retaining the boundary handle, and return fully obsolete data
//# regions to Borromean storage.
#[test]
fn requirement_object_log_truncate_before_handle_retains_boundary_handle() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();

    let first = log.append(&mut storage, b"alpha").unwrap();
    log.flush(&mut storage).unwrap();
    let second = log.append(&mut storage, b"beta").unwrap();
    let previous_tail = storage.free_list_tail();

    log.truncate_before(&mut storage, second).unwrap();

    let mut scratch = [0u8; 64];
    assert!(matches!(
        log.get(&mut storage, first, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert_get(&log, &mut storage, second, b"beta");
    assert_ne!(storage.free_list_tail(), previous_tail);
    assert_eq!(storage.free_list_tail(), Some(first.region_index));

    let third = log.append(&mut storage, b"gamma").unwrap();
    assert_get(&log, &mut storage, third, b"gamma");
}

//= spec/object-log.md#truncation
//= type=test
//# `RING-OBJECT-029` When truncating before a large-object handle, the object
//# log MUST retain every chunk region reachable from that large object's
//# `ObjectEnd` record and free only regions wholly before the retained first
//# chunk.
#[test]
fn requirement_object_log_truncate_before_large_object_retains_reachable_run() {
    check_object_log_truncate_before_large_object_retains_object_run();
}

fn check_object_log_truncate_before_large_object_retains_object_run() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 64;

    let log_metadata = [0x42u8; 192];
    let mut object = [0u8; 270];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 32768>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 224>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, &log_metadata).unwrap();
    let first = log.append(&mut storage, b"before").unwrap();
    log.flush(&mut storage).unwrap();
    let large = log.append(&mut storage, &object).unwrap();
    let retained_start = object_end_for(&log, &mut storage, large).first;
    assert_ne!(retained_start.region_index, first.region_index);
    let previous_tail = storage.free_list_tail();

    log.truncate_before(&mut storage, large).unwrap();

    let mut scratch = [0u8; 270];
    assert!(matches!(
        log.get(&mut storage, first, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert_get_bytes(&log, &mut storage, large, &object, &mut scratch);
    assert_eq!(log.first_handle(), Some(large));
    assert_ne!(storage.free_list_tail(), previous_tail);
    assert_eq!(storage.free_list_tail(), Some(first.region_index));
    assert!(log.region_for_handle(retained_start).is_ok());
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-007` Object-log metadata MUST be a non-empty immutable
//# opaque byte sequence supplied at collection creation, persisted with
//# collection state, restored on open, and exposed to callers without requiring
//# the caller to know it before opening the collection.
#[test]
fn requirement_object_log_metadata_is_immutable_and_reopens_from_wal() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, handle) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        let mut empty_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        assert!(matches!(
            ObjectLog::new(&mut storage, &mut empty_memory, b""),
            Err(ObjectLogError::LogMetadataEmpty)
        ));

        let mut small_memory = ObjectLogMemory::<REGION_SIZE, 4, 4>::new();
        assert!(matches!(
            ObjectLog::new(&mut storage, &mut small_memory, b"abcde"),
            Err(ObjectLogError::LogMetadataTooLarge {
                len: 5,
                capacity: 4
            })
        ));

        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
        assert_eq!(
            log.get_log_metadata(|bytes| {
                assert_eq!(bytes, LOG_METADATA);
                bytes.len()
            }),
            LOG_METADATA.len()
        );
        let handle = log.append(&mut storage, b"alpha").unwrap();
        assert_get(&log, &mut storage, handle, b"alpha");
        (log.collection_id(), handle)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    assert_eq!(
        reopened_log.get_log_metadata(|bytes| {
            assert_eq!(bytes, LOG_METADATA);
            bytes.len()
        }),
        LOG_METADATA.len()
    );
    assert_get(&reopened_log, &mut reopened, handle, b"alpha");
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-008` Every object-log data region MUST contain the full
//# immutable log metadata in its object-log prologue, and opening or reading a
//# flushed region MUST reject a prologue whose metadata differs from the
//# collection metadata.
#[test]
fn requirement_object_log_data_regions_carry_immutable_log_metadata() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 12;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, first, second) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();

        let first = log.append(&mut storage, b"alpha").unwrap();
        log.flush(&mut storage).unwrap();
        let second = log.append(&mut storage, b"beta").unwrap();
        log.flush(&mut storage).unwrap();
        assert_get(&log, &mut storage, first, b"alpha");
        assert_get(&log, &mut storage, second, b"beta");
        (log.collection_id(), first, second)
    };

    assert_region_log_metadata(&flash, first, LOG_METADATA);
    assert_region_log_metadata(&flash, second, LOG_METADATA);

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    reopened
        .backing
        .write_region(
            first.region_index,
            Header::ENCODED_LEN + DATA_PROLOGUE_FIXED_LEN,
            b"X",
        )
        .unwrap();
    let mut scratch = [0u8; 64];
    assert!(matches!(
        reopened_log.get(&mut reopened, first, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    assert!(matches!(
        ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory),
        Err(ObjectLogError::InvalidFrame)
    ));
}

fn check_object_log_flushed_region_prologue_is_validated_on_read() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, b"alpha").unwrap();
    log.flush(&mut storage).unwrap();
    let original_region = *storage.backing.region_bytes(handle.region_index).unwrap();
    let mut scratch = [0u8; 16];

    storage
        .backing
        .write_region(
            handle.region_index,
            Header::ENCODED_LEN + 6,
            &handle.sequence.wrapping_add(1).to_le_bytes(),
        )
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    storage
        .backing
        .write_region(handle.region_index, 0, &original_region[..])
        .unwrap();

    storage
        .backing
        .write_region(
            handle.region_index,
            Header::ENCODED_LEN + 14,
            &0u32.to_le_bytes(),
        )
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
    storage
        .backing
        .write_region(handle.region_index, 0, &original_region[..])
        .unwrap();

    storage
        .backing
        .write_region(
            handle.region_index,
            Header::ENCODED_LEN + 14,
            &(LOG_METADATA.len() as u32 + 1).to_le_bytes(),
        )
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
    storage
        .backing
        .write_region(handle.region_index, 0, &original_region[..])
        .unwrap();

    let mut header = Header::decode(&original_region[..Header::ENCODED_LEN]).unwrap();
    header.collection_format = OBJECT_LOG_DATA_V1_FORMAT + 1;
    let mut encoded_header = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut encoded_header).unwrap();
    storage
        .backing
        .write_region(handle.region_index, 0, &encoded_header)
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
}

fn check_object_log_flushed_region_metadata_length_bounds_are_exact() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let metadata = [0x55u8; 8];
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 8>::new();
    let mut log = ObjectLog {
        collection_id: CollectionId::new(88),
        memory: &mut memory,
    };
    log.apply_log_metadata(&metadata).unwrap();
    let region = ObjectLogRegion {
        region_index: 1,
        sequence: 3,
        start_offset: 0,
        end_offset: 0,
        committed_end_offset: 0,
        first_committed_public_offset: None,
        first_planned_public_offset: None,
        flushed: true,
    };
    let header = Header {
        sequence: region.sequence,
        collection_id: log.collection_id,
        collection_format: OBJECT_LOG_DATA_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    storage
        .backing
        .write_region(region.region_index, 0, &header_bytes)
        .unwrap();
    let mut prologue = [0u8; DATA_PROLOGUE_FIXED_LEN + 8];
    encode_data_prologue(region.sequence, &metadata, &mut prologue).unwrap();
    storage
        .backing
        .write_region(region.region_index, Header::ENCODED_LEN, &prologue)
        .unwrap();
    log.validate_flushed_region_prologue(&mut storage, region)
        .unwrap();

    let mut zero_memory = ObjectLogMemory::<REGION_SIZE, 4, 8>::new();
    let zero_log = ObjectLog {
        collection_id: CollectionId::new(88),
        memory: &mut zero_memory,
    };
    let mut zero_prologue = [0u8; DATA_PROLOGUE_FIXED_LEN];
    write_data_prologue_header(&mut zero_prologue, region.sequence, 0);
    storage
        .backing
        .write_region(region.region_index, Header::ENCODED_LEN, &zero_prologue)
        .unwrap();
    assert!(matches!(
        zero_log.validate_flushed_region_prologue(&mut storage, region),
        Err(ObjectLogError::InvalidFrame)
    ));

    let mut oversized_memory = ObjectLogMemory::<REGION_SIZE, 4, 8>::new();
    oversized_memory.log_metadata_len = 9;
    let oversized_log = ObjectLog {
        collection_id: CollectionId::new(88),
        memory: &mut oversized_memory,
    };
    let mut oversized_prologue = [0u8; DATA_PROLOGUE_FIXED_LEN];
    write_data_prologue_header(&mut oversized_prologue, region.sequence, 9);
    storage
        .backing
        .write_region(
            region.region_index,
            Header::ENCODED_LEN,
            &oversized_prologue,
        )
        .unwrap();
    assert!(matches!(
        oversized_log.validate_flushed_region_prologue(&mut storage, region),
        Err(ObjectLogError::InvalidFrame)
    ));
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-002` `ObjectLogHandle` MUST remain opaque to external
//# callers: it MUST NOT expose public field access, an unchecked public field
//# constructor, or debug formatting that reveals internal handle components.
#[test]
fn requirement_object_log_handle_public_representation_is_opaque() {
    let handle = ObjectLogHandle::new(1, 2, 3);

    assert_eq!(format!("{handle:?}"), "ObjectLogHandle { .. }");
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-005` Object-log reads MUST reject handles that do not
//# name a live reserved object record.
#[test]
fn requirement_object_log_rejects_forged_handles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
    let handle = log.append(&mut storage, b"alpha").unwrap();
    let forged = ObjectLogHandle::new(
        handle.region_index,
        handle.sequence.wrapping_add(1),
        handle.offset,
    );

    let mut scratch = [0u8; 64];
    assert!(matches!(
        log.get(&mut storage, forged, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-004` The durable object-log handle and `ObjectLogPointer`
//# encoding MUST be exactly 16 bytes with no padding: bytes 0 through 3 contain
//# `region_index` as a little-endian `u32`, bytes 4 through 11 contain
//# `sequence` as a little-endian `u64`, and bytes 12 through 15 contain
//# `offset` as a little-endian `u32`.
#[test]
fn requirement_object_log_handle_encoding_is_fixed_little_endian_layout() {
    let handle = ObjectLogHandle::new(0x0102_0304, 0x1112_1314_1516_1718, 0x2122_2324);
    let mut encoded = [0u8; HANDLE_ENCODED_LEN];

    assert_eq!(HANDLE_ENCODED_LEN, 16);
    assert_eq!(
        write_handle(&mut encoded, 0, handle).unwrap(),
        HANDLE_ENCODED_LEN
    );
    assert_eq!(
        encoded,
        [
            0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x24, 0x23,
            0x22, 0x21,
        ]
    );

    let mut offset = 0usize;
    assert_eq!(read_handle(&encoded, &mut offset).unwrap(), handle);
    assert_eq!(offset, HANDLE_ENCODED_LEN);
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-014` Object-log region sequences MUST be monotonic `u64`
//# values that never wrap. If replay, snapshot decode, or open observes state
//# that would require advancing past `u64::MAX`, the collection MUST be treated
//# as corrupt.
#[test]
fn requirement_object_log_sequence_overflow_is_corrupt() {
    const REGION_SIZE: usize = 512;

    let object_start =
        u32::try_from(Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap())
            .unwrap();
    let mut snapshot = [0u8; 128];
    let mut offset = 0usize;
    offset = write_bytes(&mut snapshot, offset, &SNAPSHOT_MAGIC).unwrap();
    offset = write_u16(&mut snapshot, offset, SNAPSHOT_VERSION).unwrap();
    offset = write_u16(&mut snapshot, offset, 0).unwrap();
    offset = write_u32(&mut snapshot, offset, 1).unwrap();
    offset = write_u32(&mut snapshot, offset, LOG_METADATA.len() as u32).unwrap();
    offset = write_u32(&mut snapshot, offset, 1).unwrap();
    offset = write_u64(&mut snapshot, offset, u64::MAX).unwrap();
    offset = write_u32(&mut snapshot, offset, object_start).unwrap();
    offset = write_u32(&mut snapshot, offset, object_start).unwrap();
    offset = write_u32(&mut snapshot, offset, object_start).unwrap();
    offset = write_u8(&mut snapshot, offset, 0).unwrap();
    offset = write_bytes(&mut snapshot, offset, LOG_METADATA).unwrap();

    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..offset], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));
}

fn check_object_log_snapshot_decode_rejects_corrupt_region_metadata() {
    const REGION_SIZE: usize = 512;

    let object_start =
        u32::try_from(Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap())
            .unwrap();
    let record_end = object_start + u32::try_from(inline_record_len(3).unwrap()).unwrap();
    let valid_region = ObjectLogRegion {
        region_index: 2,
        sequence: 7,
        start_offset: object_start,
        end_offset: record_end,
        committed_end_offset: record_end,
        first_committed_public_offset: Some(object_start),
        first_planned_public_offset: None,
        flushed: false,
    };
    let mut regions = Vec::<ObjectLogRegion, 4>::new();
    regions.push(valid_region).unwrap();
    let mut snapshot = [0u8; 160];
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory).unwrap();

    let mut corrupt = snapshot;
    write_u32_at(&mut corrupt, 12, 0);
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&corrupt[..used], &mut memory),
        Err(ObjectLogError::LogMetadataEmpty)
    ));

    let mut corrupt = snapshot;
    corrupt[40] = 2;
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&corrupt[..used], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    let mut corrupt = snapshot;
    corrupt[used] = 0x7a;
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&corrupt[..used + 1], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    let mut invalid_region = valid_region;
    invalid_region.committed_end_offset = invalid_region.end_offset + 1;
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    invalid_region = valid_region;
    invalid_region.committed_end_offset = invalid_region.start_offset - 1;
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    invalid_region = valid_region;
    invalid_region.start_offset = object_start - 1;
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    invalid_region = valid_region;
    invalid_region.first_committed_public_offset = Some(valid_region.committed_end_offset);
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    invalid_region = valid_region;
    invalid_region.first_planned_public_offset = Some(valid_region.end_offset);
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    invalid_region = valid_region;
    invalid_region.committed_end_offset = valid_region.start_offset;
    invalid_region.first_committed_public_offset = None;
    invalid_region.first_planned_public_offset = Some(valid_region.start_offset - 1);
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    invalid_region = valid_region;
    invalid_region.end_offset = valid_region.start_offset;
    invalid_region.committed_end_offset = valid_region.start_offset;
    invalid_region.first_committed_public_offset = None;
    invalid_region.first_planned_public_offset = None;
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory).unwrap();

    invalid_region = valid_region;
    invalid_region.committed_end_offset = valid_region.start_offset - 1;
    invalid_region.first_committed_public_offset = None;
    invalid_region.first_planned_public_offset = None;
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));

    invalid_region = valid_region;
    invalid_region.first_planned_public_offset = Some(valid_region.start_offset);
    regions.clear();
    regions.push(invalid_region).unwrap();
    let used = encode_snapshot::<4, 16>(&regions, LOG_METADATA, &mut snapshot).unwrap();
    decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..used], &mut memory).unwrap();
}

fn check_object_log_open_state_validates_region_metadata_bounds() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let object_start =
        u32::try_from(Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap())
            .unwrap();
    let record_end = object_start + u32::try_from(inline_record_len(3).unwrap()).unwrap();
    let valid = ObjectLogRegion {
        region_index: 2,
        sequence: 7,
        start_offset: object_start,
        end_offset: record_end,
        committed_end_offset: record_end,
        first_committed_public_offset: Some(object_start),
        first_planned_public_offset: Some(object_start),
        flushed: false,
    };
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    memory.log_metadata[..LOG_METADATA.len()].copy_from_slice(LOG_METADATA);
    memory.log_metadata_len = LOG_METADATA.len();
    let log = ObjectLog {
        collection_id: CollectionId::new(12),
        memory: &mut memory,
    };

    log.memory.regions.push(valid).unwrap();
    log.validate_open_state(&mut storage).unwrap();

    let mut empty_valid = valid;
    empty_valid.end_offset = object_start;
    empty_valid.committed_end_offset = object_start;
    empty_valid.first_committed_public_offset = None;
    empty_valid.first_planned_public_offset = None;
    log.memory.regions.clear();
    log.memory.regions.push(empty_valid).unwrap();
    log.validate_open_state(&mut storage).unwrap();

    for invalid in [
        ObjectLogRegion {
            start_offset: object_start - 1,
            ..valid
        },
        ObjectLogRegion {
            committed_end_offset: valid.end_offset + 1,
            ..valid
        },
        ObjectLogRegion {
            committed_end_offset: valid.start_offset - 1,
            ..valid
        },
        ObjectLogRegion {
            first_committed_public_offset: Some(valid.start_offset - 1),
            ..valid
        },
        ObjectLogRegion {
            first_committed_public_offset: Some(valid.committed_end_offset),
            ..valid
        },
        ObjectLogRegion {
            first_planned_public_offset: Some(valid.start_offset - 1),
            ..valid
        },
        ObjectLogRegion {
            first_planned_public_offset: Some(valid.end_offset),
            ..valid
        },
    ] {
        log.memory.regions.clear();
        log.memory.regions.push(invalid).unwrap();
        assert!(matches!(
            log.validate_open_state(&mut storage),
            Err(ObjectLogError::InvalidEncoding)
        ));
    }
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-003` Opening an object-log collection by id MUST fail
//# if the live collection exists with a non-object-log collection type.
#[test]
fn requirement_object_log_open_rejects_non_object_log_collection() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(CollectionId(22)).unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    assert!(matches!(
        ObjectLog::open(CollectionId(22), &mut storage, &mut memory),
        Err(ObjectLogError::CollectionTypeMismatch { .. })
    ));
}

//= spec/object-log.md#live-traversal
//= type=test
//# `RING-OBJECT-011` Object-log traversal MUST provide a way to obtain
//# the first live `ObjectLogHandle` and a way to obtain the next live
//# `ObjectLogHandle` after a provided live handle. Empty logs and tail handles
//# MUST return no handle, while handles outside the current live log MUST be
//# rejected as invalid.
#[test]
fn requirement_object_log_traverses_live_handles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();

    assert_eq!(log.first_handle(), None);

    let first = log.append(&mut storage, b"alpha").unwrap();
    log.flush(&mut storage).unwrap();
    let second = log.append(&mut storage, b"beta").unwrap();
    let third = log.append(&mut storage, b"gamma").unwrap();

    assert_eq!(log.first_handle(), Some(first));
    assert_eq!(log.next_handle(&mut storage, first).unwrap(), Some(second));
    assert_eq!(log.next_handle(&mut storage, second).unwrap(), Some(third));
    assert_eq!(log.next_handle(&mut storage, third).unwrap(), None);

    log.truncate_before(&mut storage, second).unwrap();
    assert_eq!(log.first_handle(), Some(second));
    assert!(matches!(
        log.next_handle(&mut storage, first),
        Err(ObjectLogError::InvalidHandle)
    ));
}

//= spec/object-log.md#committed-visibility
//= type=test
//# `RING-OBJECT-009` Object-log reads, traversal, and truncation MUST
//# observe only committed object bounds.
#[test]
fn requirement_object_log_failed_transaction_does_not_publish_planned_handles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
    let mut planned = None;

    let result: Result<(), ObjectLogError> = log.transaction(&mut storage, |tx| {
        let handle = tx.append(b"staged")?;
        planned = Some(handle);
        Err(ObjectLogError::InvalidHandle)
    });
    assert!(matches!(result, Err(ObjectLogError::InvalidHandle)));

    let planned = planned.unwrap();
    let mut scratch = [0u8; 64];
    assert_eq!(log.first_handle(), None);
    assert!(matches!(
        log.get(&mut storage, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert!(matches!(
        log.truncate_before(&mut storage, planned),
        Err(ObjectLogError::InvalidHandle)
    ));

    let committed = log.append(&mut storage, b"committed").unwrap();
    log.checkpoint_append_state().unwrap();
    storage
        .memory
        .state
        .begin_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
            storage.backing,
            &mut storage.memory.workspace,
            log.collection_id(),
        )
        .unwrap();
    let mut allocated_regions = Vec::<u32, REGION_COUNT>::new();
    let planned = log
        .append_transactional(&mut storage, b"planned", &mut allocated_regions)
        .unwrap();

    assert_eq!(log.first_handle(), Some(committed));
    assert_eq!(log.next_handle(&mut storage, committed).unwrap(), None);
    assert!(matches!(
        log.get(&mut storage, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert!(matches!(
        log.truncate_before(&mut storage, planned),
        Err(ObjectLogError::InvalidHandle)
    ));
    log.rollback_transaction(&mut storage, allocated_regions)
        .unwrap();
}

//= spec/object-log.md#append-transactions
//= type=test
//# `RING-OBJECT-012` Scoped append transactions MUST keep appended
//# objects invisible until the durable commit record.
#[test]
fn requirement_object_log_committed_transaction_publishes_handles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
    let first = log.append(&mut storage, b"before").unwrap();
    let alpha = [0x11u8; 200];
    let beta = [0x22u8; 200];
    let gamma = [0x33u8; 200];

    let (second, third, fourth) = log
        .transaction(&mut storage, |tx| {
            let second = tx.append(&alpha)?;
            let third = tx.append(&beta)?;
            let fourth = tx.append(&gamma)?;
            Ok((second, third, fourth))
        })
        .unwrap();

    assert_eq!(first.region_index, second.region_index);
    assert_eq!(second.region_index, third.region_index);
    assert_ne!(third.region_index, fourth.region_index);
    assert_eq!(log.first_handle(), Some(first));
    assert_eq!(log.next_handle(&mut storage, first).unwrap(), Some(second));
    assert_eq!(log.next_handle(&mut storage, second).unwrap(), Some(third));
    assert_eq!(log.next_handle(&mut storage, third).unwrap(), Some(fourth));
    assert_eq!(log.next_handle(&mut storage, fourth).unwrap(), None);
    assert_get(&log, &mut storage, first, b"before");
    let mut scratch = [0u8; 256];
    assert_eq!(
        log.get(&mut storage, second, &mut scratch, |bytes| {
            assert_eq!(bytes, alpha.as_slice());
            bytes.len()
        })
        .unwrap(),
        alpha.len()
    );
    assert_eq!(
        log.get(&mut storage, third, &mut scratch, |bytes| {
            assert_eq!(bytes, beta.as_slice());
            bytes.len()
        })
        .unwrap(),
        beta.len()
    );
    assert_eq!(
        log.get(&mut storage, fourth, &mut scratch, |bytes| {
            assert_eq!(bytes, gamma.as_slice());
            bytes.len()
        })
        .unwrap(),
        gamma.len()
    );
}

//= spec/object-log.md#append-transactions
//= type=test
//# `RING-OBJECT-013` Failed or uncommitted append transactions MUST roll
//# back cleanly by discarding staged object-log state and returning
//# transaction-reserved regions to storage without making planned handles live.
#[test]
fn requirement_object_log_failed_transaction_rolls_back_allocations() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
    let mut planned = None;

    let result: Result<(), ObjectLogError> = log.transaction(&mut storage, |tx| {
        let handle = tx.append(b"staged")?;
        planned = Some(handle);
        Err(ObjectLogError::InvalidHandle)
    });
    assert!(matches!(result, Err(ObjectLogError::InvalidHandle)));

    let planned = planned.unwrap();
    assert_eq!(storage.free_list_tail(), Some(planned.region_index));
    let committed = log.append(&mut storage, b"committed").unwrap();
    let mut scratch = [0u8; 64];
    assert!(matches!(
        log.get(&mut storage, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert_get(&log, &mut storage, committed, b"committed");

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, planned) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
        let collection_id = log.collection_id();
        storage
            .memory
            .state
            .begin_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
                storage.backing,
                &mut storage.memory.workspace,
                collection_id,
            )
            .unwrap();
        let mut allocated_regions = Vec::<u32, REGION_COUNT>::new();
        let planned = log
            .append_transactional_new_region(&mut storage, b"staged", &mut allocated_regions)
            .unwrap();
        (collection_id, planned)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    let mut scratch = [0u8; 64];
    assert_eq!(reopened_log.first_handle(), None);
    assert!(matches!(
        reopened_log.get(&mut reopened, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert_eq!(reopened.free_list_tail(), Some(planned.region_index));
}

fn check_object_log_update_payloads_validate_truncate_and_materialized_region_records() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let first = log.append(&mut storage, b"alpha").unwrap();
    log.flush(&mut storage).unwrap();
    let second = log.append(&mut storage, b"beta").unwrap();
    let retained_start = log
        .retained_start_for_truncate(&mut storage, second)
        .unwrap();

    let mut payload = [0u8; 64];
    let used = encode_truncate_update(second, retained_start, &mut payload).unwrap();
    assert_eq!(used, 1 + HANDLE_ENCODED_LEN * 2);
    apply_update_payload(&payload[..used], log.memory, AppendVisibility::Committed).unwrap();
    assert_eq!(log.first_handle(), Some(second));
    let mut scratch = [0u8; 16];
    assert!(matches!(
        log.get(&mut storage, first, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));

    payload[used] = 0xff;
    assert!(matches!(
        apply_update_payload(
            &payload[..used + 1],
            log.memory,
            AppendVisibility::Committed
        ),
        Err(ObjectLogError::InvalidEncoding)
    ));

    let object_start =
        u32::try_from(Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap())
            .unwrap();
    let materialized = ObjectLogRegion {
        region_index: 5,
        sequence: 9,
        start_offset: object_start,
        end_offset: object_start + u32::try_from(inline_record_len(3).unwrap()).unwrap(),
        committed_end_offset: object_start,
        first_committed_public_offset: None,
        first_planned_public_offset: Some(object_start),
        flushed: true,
    };
    let mut replay_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let used = encode_materialized_region_update(materialized, &mut payload).unwrap();
    assert_eq!(used, 1 + 35);
    apply_update_payload(
        &payload[..used],
        &mut replay_memory,
        AppendVisibility::Committed,
    )
    .unwrap();
    let replayed = ObjectLog {
        collection_id: CollectionId::new(0),
        memory: &mut replay_memory,
    };
    assert_eq!(
        replayed.first_handle(),
        Some(ObjectLogHandle::new(
            materialized.region_index,
            materialized.sequence,
            object_start
        ))
    );
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-017` Object-log V1 data regions MUST encode object records
//# with the common typed-record header
//# `[record_type:u8][body_len:u32 little-endian][body_crc32c:u32
//# little-endian][body]`, MUST compute `body_crc32c` as CRC32C over `body`, and
//# MUST reject unknown record types.
#[test]
fn requirement_object_log_v1_data_regions_use_typed_record_headers() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, b"alpha").unwrap();
    log.flush(&mut storage).unwrap();

    let region = storage.backing.region_bytes(handle.region_index).unwrap();
    let offset = usize::try_from(handle.offset).unwrap();
    assert_eq!(region[offset], RECORD_INLINE_OBJECT);
    assert_eq!(read_u32_at(region, offset + 1), 5);
    assert_eq!(read_u32_at(region, offset + 5), crc32(b"alpha"));
    assert_eq!(
        &region[offset + RECORD_HEADER_LEN..offset + RECORD_HEADER_LEN + 5],
        b"alpha"
    );

    storage
        .backing
        .write_region(handle.region_index, offset + 5, &0u32.to_le_bytes())
        .unwrap();
    let mut scratch = [0u8; 16];
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, b"beta").unwrap();
    log.flush(&mut storage).unwrap();
    storage
        .backing
        .write_region(
            handle.region_index,
            usize::try_from(handle.offset).unwrap(),
            &[0xff],
        )
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-018` Inline objects MUST be encoded as record type `0x01`
//# `InlineObject` whose body is the raw object bytes and whose public handle
//# names that record.
#[test]
fn requirement_object_log_inline_objects_use_inline_object_records() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, b"inline").unwrap();

    let (region, record) = record_info_for(&log, &mut storage, handle);
    assert!(!region.flushed);
    assert_eq!(record.record_type, RECORD_INLINE_OBJECT);
    assert_eq!(record.body_len, b"inline".len());
    assert_eq!(log.first_handle(), Some(handle));
    assert_get(&log, &mut storage, handle, b"inline");
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-019` Large-object handles MUST point to record type `0x03`
//# `ObjectEnd` records encoded as `[total_object_len:u64
//# little-endian][first:ObjectLogPointer][last:ObjectLogPointer]`.
#[test]
fn requirement_object_log_large_object_handles_point_to_end_records() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 18;

    let mut object = [0u8; 900];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let (collection_id, handle) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
        let handle = log.append(&mut storage, &object).unwrap();

        let (_, record) = record_info_for(&log, &mut storage, handle);
        assert_eq!(record.record_type, RECORD_OBJECT_END);
        let object_end = object_end_for(&log, &mut storage, handle);
        assert_eq!(object_end.total_object_len, object.len() as u64);
        assert_eq!(
            record_info_for(&log, &mut storage, object_end.first)
                .1
                .record_type,
            RECORD_OBJECT_CHUNK
        );
        assert_eq!(
            record_info_for(&log, &mut storage, object_end.last)
                .1
                .record_type,
            RECORD_OBJECT_CHUNK
        );

        let mut scratch = [0u8; 900];
        assert_get_bytes(&log, &mut storage, handle, &object, &mut scratch);
        assert_get_range(&log, &mut storage, handle, 140, &object[140..156]);
        assert_eq!(
            log.get_object_len(&mut storage, handle).unwrap(),
            object.len() as u64
        );
        log.flush(&mut storage).unwrap();
        (log.collection_id(), handle)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    let mut scratch = [0u8; 900];
    assert_get_bytes(&reopened_log, &mut reopened, handle, &object, &mut scratch);
    assert_get_range(&reopened_log, &mut reopened, handle, 200, &object[200..216]);
    assert_eq!(
        reopened_log.get_object_len(&mut reopened, handle).unwrap(),
        object.len() as u64
    );
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-020` Object chunks MUST be encoded as record type `0x02`
//# `ObjectChunk` bodies `[flags:u8][logical_start:u64
//# little-endian][chunk_len:u32
//# little-endian][prev:ObjectLogPointer][next:ObjectLogPointer][chunk_bytes]`, MUST reject nonzero
//# reserved flags, and MUST validate each chunk through its record CRC32C.
#[test]
fn requirement_object_log_chunks_encode_links_and_validate_crc() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 18;

    let mut object = [0u8; 420];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, &object).unwrap();
    let object_end = object_end_for(&log, &mut storage, handle);

    let (_, _, first_chunk) = log
        .read_chunk_info(&mut storage, object_end.first, true)
        .unwrap();
    assert_eq!(first_chunk.flags & OBJECT_CHUNK_FLAG_PREV_VALID, 0);
    assert_ne!(first_chunk.flags & OBJECT_CHUNK_FLAG_NEXT_VALID, 0);
    assert_eq!(first_chunk.logical_start, 0);
    let second = first_chunk.next;
    let (_, _, second_chunk) = log.read_chunk_info(&mut storage, second, true).unwrap();
    assert_ne!(second_chunk.flags & OBJECT_CHUNK_FLAG_PREV_VALID, 0);
    assert_eq!(second_chunk.prev, object_end.first);

    let crc_offset = usize::try_from(object_end.first.offset).unwrap() + 5;
    storage
        .backing
        .write_region(
            object_end.first.region_index,
            crc_offset,
            &0u32.to_le_bytes(),
        )
        .unwrap();
    let mut scratch = [0u8; 8];
    assert!(matches!(
        log.get_range(&mut storage, handle, 0, 8, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
}

fn check_object_log_large_object_reads_reject_private_or_malformed_chunks() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 18;

    let mut object = [0u8; 420];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, &object).unwrap();
    let object_end = object_end_for(&log, &mut storage, handle);
    let (first_region, first_record, first_chunk) = log
        .read_chunk_info(&mut storage, object_end.first, true)
        .unwrap();
    assert!(first_region.flushed);
    assert_ne!(first_chunk.flags & OBJECT_CHUNK_FLAG_NEXT_VALID, 0);

    let mut scratch = [0u8; 420];
    assert!(matches!(
        log.get(&mut storage, object_end.first, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));

    let original_region = *storage
        .backing
        .region_bytes(first_region.region_index)
        .unwrap();
    let first_len = first_chunk.chunk_len;
    let mut first_exact = std::vec![0u8; first_len];
    assert_eq!(
        log.get_range(
            &mut storage,
            handle,
            0,
            first_len as u64,
            &mut first_exact,
            |bytes| {
                assert_eq!(bytes, &object[..first_len]);
                bytes.len()
            },
        )
        .unwrap(),
        first_len
    );
    let mut boundary = [0u8; 1];
    assert_eq!(
        log.get_range(
            &mut storage,
            handle,
            first_len as u64,
            1,
            &mut boundary,
            |bytes| {
                assert_eq!(bytes, &object[first_len..first_len + 1]);
                bytes.len()
            },
        )
        .unwrap(),
        1
    );
    let mut too_short_for_copy = [0u8; 1];
    assert!(matches!(
        log.copy_large_object_range(&mut storage, object_end, 0, 2, &mut too_short_for_copy),
        Err(ObjectLogError::InvalidFrame)
    ));

    storage
        .backing
        .write_region(
            first_region.region_index,
            first_record.body_start,
            &[first_chunk.flags | OBJECT_CHUNK_FLAG_PREV_VALID],
        )
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
    storage
        .backing
        .write_region(first_region.region_index, 0, &original_region[..])
        .unwrap();

    storage
        .backing
        .write_region(
            first_region.region_index,
            first_record.body_start,
            &[first_chunk.flags | 0x80],
        )
        .unwrap();
    let mut one_byte = [0u8; 1];
    assert!(matches!(
        log.copy_large_object_range(
            &mut storage,
            object_end,
            first_chunk.chunk_len as u64,
            1,
            &mut one_byte,
        ),
        Err(ObjectLogError::InvalidFrame)
    ));
    storage
        .backing
        .write_region(first_region.region_index, 0, &original_region[..])
        .unwrap();

    storage
        .backing
        .write_region(
            first_region.region_index,
            first_record.body_start,
            &[first_chunk.flags & !OBJECT_CHUNK_FLAG_NEXT_VALID],
        )
        .unwrap();
    refresh_record_crc(&mut log, &mut storage, first_region, first_record);
    let mut one_byte = [0u8; 1];
    assert!(matches!(
        log.copy_large_object_range(
            &mut storage,
            object_end,
            first_chunk.chunk_len as u64,
            1,
            &mut one_byte,
        ),
        Err(ObjectLogError::InvalidFrame)
    ));
    storage
        .backing
        .write_region(first_region.region_index, 0, &original_region[..])
        .unwrap();

    storage
        .backing
        .write_region(
            first_region.region_index,
            first_record.body_start + 1 + size_of::<u64>(),
            &(first_chunk.chunk_len as u32 + 1).to_le_bytes(),
        )
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
    storage
        .backing
        .write_region(first_region.region_index, 0, &original_region[..])
        .unwrap();

    let zero_handle = ObjectLogHandle::new(0, 0, 0);
    let mut encoded_zero = [0u8; HANDLE_ENCODED_LEN];
    write_handle(&mut encoded_zero, 0, zero_handle).unwrap();
    storage
        .backing
        .write_region(
            first_region.region_index,
            first_record.body_start + 1 + size_of::<u64>() + size_of::<u32>() + HANDLE_ENCODED_LEN,
            &encoded_zero,
        )
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame | ObjectLogError::InvalidHandle)
    ));
    storage
        .backing
        .write_region(first_region.region_index, 0, &original_region[..])
        .unwrap();

    let (second_region, second_record, second_chunk) = log
        .read_chunk_info(&mut storage, first_chunk.next, true)
        .unwrap();
    let second_original = *storage
        .backing
        .region_bytes(second_region.region_index)
        .unwrap();
    let saved_frontier = write_region_or_frontier(
        &mut log,
        &mut storage,
        second_region,
        u32::try_from(second_record.body_start).unwrap(),
        &[second_chunk.flags & !OBJECT_CHUNK_FLAG_PREV_VALID],
    );
    refresh_record_crc(&mut log, &mut storage, second_region, second_record);
    let mut one_byte = [0u8; 1];
    assert!(matches!(
        log.copy_large_object_range(&mut storage, object_end, first_len as u64, 1, &mut one_byte,),
        Err(ObjectLogError::InvalidFrame)
    ));
    restore_region_or_frontier(
        &mut log,
        &mut storage,
        second_region,
        saved_frontier,
        &second_original,
    );

    let mut encoded_zero = [0u8; HANDLE_ENCODED_LEN];
    write_handle(&mut encoded_zero, 0, ObjectLogHandle::new(0, 0, 0)).unwrap();
    let saved_frontier = write_region_or_frontier(
        &mut log,
        &mut storage,
        second_region,
        u32::try_from(second_record.body_start + 1 + size_of::<u64>() + size_of::<u32>()).unwrap(),
        &encoded_zero,
    );
    refresh_record_crc(&mut log, &mut storage, second_region, second_record);
    let mut one_byte = [0u8; 1];
    assert!(matches!(
        log.copy_large_object_range(&mut storage, object_end, first_len as u64, 1, &mut one_byte,),
        Err(ObjectLogError::InvalidFrame)
    ));
    restore_region_or_frontier(
        &mut log,
        &mut storage,
        second_region,
        saved_frontier,
        &second_original,
    );

    let (last_region, last_record, last_chunk) = log
        .read_chunk_info(&mut storage, object_end.last, true)
        .unwrap();
    let last_original = *storage
        .backing
        .region_bytes(last_region.region_index)
        .unwrap();
    let saved_frontier = write_region_or_frontier(
        &mut log,
        &mut storage,
        last_region,
        u32::try_from(last_record.body_start).unwrap(),
        &[last_chunk.flags | OBJECT_CHUNK_FLAG_NEXT_VALID],
    );
    refresh_record_crc(&mut log, &mut storage, last_region, last_record);
    let mut one_byte = [0u8; 1];
    assert!(matches!(
        log.copy_large_object_range(
            &mut storage,
            object_end,
            object.len() as u64 - 1,
            1,
            &mut one_byte,
        ),
        Err(ObjectLogError::InvalidFrame)
    ));
    restore_region_or_frontier(
        &mut log,
        &mut storage,
        last_region,
        saved_frontier,
        &last_original,
    );

    let mut wrong_last = object_end;
    wrong_last.last = object_end.first;
    let mut one_byte = [0u8; 1];
    assert!(matches!(
        log.copy_large_object_range(
            &mut storage,
            wrong_last,
            object.len() as u64 - 1,
            1,
            &mut one_byte,
        ),
        Err(ObjectLogError::InvalidFrame)
    ));
}

//= spec/object-log.md#large-objects
//= type=test
//# `RING-OBJECT-021` Large-object runs MUST use linked `ObjectChunk`
//# records with previous and next links or start and end markers rather than a
//# map-style manifest.
#[test]
fn requirement_object_log_large_object_runs_use_linked_chunks() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 20;

    let mut object = [0u8; 560];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, &object).unwrap();
    let object_end = object_end_for(&log, &mut storage, handle);

    let mut current = object_end.first;
    let mut previous = None;
    let mut total = 0usize;
    let mut chunk_count = 0usize;
    loop {
        let (_, _, chunk) = log.read_chunk_info(&mut storage, current, true).unwrap();
        if let Some(previous) = previous {
            assert_eq!(chunk.prev, previous);
        } else {
            assert_eq!(chunk.flags & OBJECT_CHUNK_FLAG_PREV_VALID, 0);
        }
        total += chunk.chunk_len;
        chunk_count += 1;
        if chunk.flags & OBJECT_CHUNK_FLAG_NEXT_VALID == 0 {
            assert_eq!(current, object_end.last);
            break;
        }
        previous = Some(current);
        current = chunk.next;
    }
    assert!(chunk_count > 1);
    assert_eq!(total, object.len());
}

//= spec/object-log.md#large-objects
//= type=test
//# `RING-OBJECT-022` Large-object append placement MUST fill the current
//# frontier first, directly materialize full frontier images, and keep the
//# trailing partial chunk plus `ObjectEnd` record WAL-backed.
#[test]
fn requirement_object_log_large_object_append_placement_uses_frontier_first() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 24;

    let mut object = [0u8; 420];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let seed = log.append(&mut storage, b"seed").unwrap();
    let handle = log.append(&mut storage, &object).unwrap();
    let object_end = object_end_for(&log, &mut storage, handle);

    assert_eq!(object_end.first.region_index, seed.region_index);
    assert!(object_end.first.offset > seed.offset);
    assert!(log.region_for_handle(object_end.first).unwrap().flushed);
    assert!(!log.region_for_handle(handle).unwrap().flushed);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata()).unwrap();
    let max_region_end = Header::ENCODED_LEN + payload_capacity;
    let object_start = Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap();
    let exact_chunk_len = chunk_payload_capacity_at(object_start, max_region_end, false).unwrap();
    let mut exact = std::vec![0u8; exact_chunk_len * 2];
    fill_pattern(&mut exact);
    let exact_handle = log.append(&mut storage, &exact).unwrap();
    let exact_end = object_end_for(&log, &mut storage, exact_handle);
    assert_ne!(exact_end.last.region_index, exact_handle.region_index);
    assert!(log.region_for_handle(exact_end.last).unwrap().flushed);
    assert!(!log.region_for_handle(exact_handle).unwrap().flushed);
}

//= spec/object-log.md#large-objects
//= type=test
//# `RING-OBJECT-023` Every physical region written for a large-object run
//# MUST be transaction-reserved before write and recoverable if the transaction
//# does not commit.
#[test]
fn requirement_object_log_large_object_regions_are_transaction_reserved() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 24;

    let mut object = [0u8; 420];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let mut planned = None;
    let result: Result<(), ObjectLogError> = log.transaction(&mut storage, |tx| {
        let handle = tx.append(&object)?;
        planned = Some(handle);
        Err(ObjectLogError::InvalidHandle)
    });
    assert!(result.is_err());
    let planned = planned.unwrap();
    assert_eq!(log.first_handle(), None);
    assert!(!log
        .memory
        .regions
        .iter()
        .any(|region| region.region_index == planned.region_index));
    let mut scratch = [0u8; 420];
    assert!(matches!(
        log.get(&mut storage, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
}
