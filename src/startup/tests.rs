#![allow(clippy::drop_non_drop)]

use super::*;
use crate::disk::{FreePointerFooter, Header};
use crate::wal_record::{encode_record_into, encoded_record_len, WalRecord};
use crate::{
    MapError, MapStorageError, MockFlash, Storage, StorageFormatConfig, StorageWorkspace,
    MAP_MANIFEST_V2_FORMAT, MAP_REGION_V2_FORMAT,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RecoveryRecordCounts {
    free_region: usize,
    rollback_transaction: usize,
    transaction_finished: usize,
}

fn open_formatted_store<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
) -> Result<StartupState<8>, StartupError> {
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut plan = StartupOpenPlan::<REGION_COUNT, 8>::empty();
    super::open_formatted_store::<REGION_SIZE, REGION_COUNT, _, 8>(flash, &mut workspace, &mut plan)
}

fn append_wal_record<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    metadata: StorageMetadata,
    region_index: u32,
    offset: usize,
    record: WalRecord<'_>,
) -> usize {
    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    let used = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();
    flash
        .write_region(region_index, offset, &physical[..used])
        .unwrap();
    offset + used
}

fn encoded_len_for_record<const REGION_SIZE: usize>(
    metadata: StorageMetadata,
    record: WalRecord<'_>,
) -> usize {
    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    encoded_record_len(record, metadata, &mut physical, &mut logical).unwrap()
}

fn startup_plan_with_append_offset<const REGION_COUNT: usize>(
    metadata: StorageMetadata,
    wal_head_candidate: u32,
    wal_tail: u32,
    wal_append_offset: usize,
) -> StartupOpenPlan<REGION_COUNT, 8> {
    let mut plan = StartupOpenPlan::<REGION_COUNT, 8>::empty();
    plan.reset(
        metadata,
        wal_head_candidate,
        wal_tail,
        RegionScanResult {
            append_offset: wal_append_offset,
            last_valid_record: None,
            wal_head_override: None,
            pending_boundary_open: false,
        },
        0,
    )
    .unwrap();
    plan.wal_append_offset = wal_append_offset;
    plan
}

fn startup_test_collection(collection_id: CollectionId) -> StartupCollection {
    StartupCollection {
        collection_id,
        collection_type: Some(CollectionType::MAP_CODE),
        basis: StartupCollectionBasis::Empty,
        pending_update_count: 0,
    }
}

fn init_wal_region<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    region_index: u32,
    sequence: u64,
    wal_head_region_index: u32,
    region_count: u32,
) {
    flash.erase_region(region_index).unwrap();

    let header = Header {
        sequence,
        collection_id: CollectionId(0),
        collection_format: WAL_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    flash.write_region(region_index, 0, &header_bytes).unwrap();

    let prologue = WalRegionPrologue {
        log_head_region_index: wal_head_region_index,
        allocator_free_list_head: Some(1),
        allocation_sequence: 0,
    };
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue
        .encode_into(&mut prologue_bytes, region_count)
        .unwrap();
    flash
        .write_region(region_index, Header::ENCODED_LEN, &prologue_bytes)
        .unwrap();
}

fn init_user_region_header<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    region_index: u32,
    sequence: u64,
    collection_id: CollectionId,
    collection_format: u16,
) {
    let header = Header {
        sequence,
        collection_id,
        collection_format,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    flash.write_region(region_index, 0, &header_bytes).unwrap();
}

fn write_free_pointer_footer<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    region_index: u32,
    next_tail: Option<u32>,
) {
    let footer = FreePointerFooter { next_tail };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut footer_bytes, 0xff).unwrap();
    flash
        .write_region(
            region_index,
            REGION_SIZE - FreePointerFooter::ENCODED_LEN,
            &footer_bytes,
        )
        .unwrap();
}

fn collection_summary(state: &StartupState<8>, collection_id: CollectionId) -> StartupCollection {
    state
        .collections()
        .iter()
        .copied()
        .find(|collection| collection.collection_id() == collection_id)
        .unwrap()
}

fn count_recovery_records_in_storage<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
    region_index: u32,
) -> RecoveryRecordCounts {
    let mut counts = RecoveryRecordCounts::default();
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<REGION_SIZE, _, (), _>(
                flash,
                workspace,
                |_flash, record| {
                    match record {
                        WalRecord::FreeRegion {
                            collection_id: seen,
                            region_index: seen_region,
                        } if seen == collection_id && seen_region == region_index => {
                            counts.free_region += 1;
                        }
                        WalRecord::RollbackTransaction {
                            transaction_log_id, ..
                        } if transaction_log_id
                            == crate::test_transaction_log_id(collection_id) =>
                        {
                            counts.rollback_transaction += 1;
                        }
                        WalRecord::TransactionFinished {
                            transaction_log_id, ..
                        } if transaction_log_id
                            == crate::test_transaction_log_id(collection_id) =>
                        {
                            counts.transaction_finished += 1;
                        }
                        _ => {}
                    }
                    Ok(())
                },
            )
        })
        .unwrap();
    counts
}

fn storage_collection_summary<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> StartupCollection {
    storage
        .collections()
        .iter()
        .copied()
        .find(|collection| collection.collection_id() == collection_id)
        .unwrap()
}

fn setup_precommit_transaction_recovery(
    append_free_region: bool,
    append_rollback: bool,
) -> MockFlash<512, 6, 256> {
    let mut flash = MockFlash::<512, 6, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);

    let mut offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_begin_transaction_record(collection_id),
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::AllocBegin {
            collection_id,
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::Update {
            collection_id,
            payload: &[1],
        },
    );
    if append_free_region {
        offset = append_wal_record(
            &mut flash,
            metadata,
            0,
            offset,
            WalRecord::FreeRegion {
                collection_id,
                region_index: 1,
            },
        );
    }
    if append_rollback {
        append_wal_record(
            &mut flash,
            metadata,
            0,
            offset,
            crate::test_rollback_transaction_record(collection_id),
        );
    }
    flash
}

fn setup_precommit_unfinished_transaction() -> MockFlash<512, 6, 256> {
    setup_precommit_transaction_recovery(false, false)
}

fn setup_precommit_recovery_after_allocation_free_before_rollback() -> MockFlash<512, 6, 256> {
    setup_precommit_transaction_recovery(true, false)
}

fn setup_precommit_recovered_with_rollback_transaction() -> MockFlash<512, 6, 256> {
    setup_precommit_transaction_recovery(true, true)
}

fn setup_precommit_transaction_requiring_recovery_rotation() -> MockFlash<512, 6, 512> {
    const REGION_SIZE: usize = 512;

    let mut flash = MockFlash::<REGION_SIZE, 6, 512>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let filler_collection_id = CollectionId(8);
    let begin_record = crate::test_begin_transaction_record(collection_id);
    let update_record = WalRecord::Update {
        collection_id,
        payload: &[1],
    };
    let rollback_record = crate::test_rollback_transaction_record(collection_id);
    let rotation_alloc_begin_record = WalRecord::AllocBegin {
        collection_id: CollectionId(0),
        region_index: 1,
        allocation_sequence: 0,
        free_list_head_after: Some(2),
    };
    let rotation_link_record = WalRecord::Link {
        next_region_index: 1,
        expected_sequence: 1,
    };

    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    let begin_len =
        encoded_record_len(begin_record, metadata, &mut physical, &mut logical).unwrap();
    let update_len =
        encoded_record_len(update_record, metadata, &mut physical, &mut logical).unwrap();
    let rollback_len =
        encoded_record_len(rollback_record, metadata, &mut physical, &mut logical).unwrap();
    let rotation_alloc_begin_len = encoded_record_len(
        rotation_alloc_begin_record,
        metadata,
        &mut physical,
        &mut logical,
    )
    .unwrap();
    let rotation_link_len =
        encoded_record_len(rotation_link_record, metadata, &mut physical, &mut logical).unwrap();
    let rotation_reserve = rotation_alloc_begin_len + rotation_link_len;

    let mut offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    let filler_payload = [0u8; REGION_SIZE];
    let filler_payload_len = (0..=filler_payload.len())
        .find(|payload_len| {
            encoded_record_len(
                WalRecord::Snapshot {
                    collection_id: filler_collection_id,
                    collection_type: CollectionType::MAP_CODE,
                    payload: &filler_payload[..*payload_len],
                },
                metadata,
                &mut physical,
                &mut logical,
            )
            .is_ok_and(|filler_len| {
                let transaction_end = offset + filler_len + begin_len + update_len;
                let Some(terminal_end) = transaction_end.checked_add(rollback_len) else {
                    return false;
                };
                let Some(rotation_alloc_end) =
                    transaction_end.checked_add(rotation_alloc_begin_len)
                else {
                    return false;
                };
                if terminal_end > REGION_SIZE || rotation_alloc_end > REGION_SIZE {
                    return false;
                }
                let remaining_after_terminal = REGION_SIZE - terminal_end;
                let remaining_after_rotation_alloc = REGION_SIZE - rotation_alloc_end;
                remaining_after_terminal < rotation_reserve
                    && remaining_after_rotation_alloc >= rotation_link_len
                    && remaining_after_rotation_alloc < rotation_reserve
            })
        })
        .expect("snapshot filler should place rollback past the tail boundary");
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::Snapshot {
            collection_id: filler_collection_id,
            collection_type: CollectionType::MAP_CODE,
            payload: &filler_payload[..filler_payload_len],
        },
    );

    offset = append_wal_record(&mut flash, metadata, 0, offset, begin_record);
    let end = append_wal_record(&mut flash, metadata, 0, offset, update_record);
    assert!(end + rollback_len <= REGION_SIZE);
    assert!(REGION_SIZE - (end + rollback_len) < rotation_reserve);
    flash
}

fn setup_postcommit_transaction_recovery(append_free_region: bool) -> MockFlash<512, 6, 256> {
    let mut flash = MockFlash::<512, 6, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);

    flash.erase_region(1).unwrap();
    init_user_region_header(&mut flash, 1, 1, collection_id, MAP_MANIFEST_V2_FORMAT);
    flash
        .write_region(1, Header::ENCODED_LEN, &0u32.to_le_bytes())
        .unwrap();

    let mut offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::AllocBegin {
            collection_id,
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::Head {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
            region_index: 1,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_begin_transaction_record(collection_id),
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::DropCollection { collection_id },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_commit_transaction_record(collection_id),
    );
    if append_free_region {
        append_wal_record(
            &mut flash,
            metadata,
            0,
            offset,
            WalRecord::FreeRegion {
                collection_id,
                region_index: 1,
            },
        );
    }
    flash
}

fn setup_postcommit_unfinished_transaction() -> MockFlash<512, 6, 256> {
    setup_postcommit_transaction_recovery(false)
}

fn setup_postcommit_recovery_after_cleanup_free_before_finished() -> MockFlash<512, 6, 256> {
    setup_postcommit_transaction_recovery(true)
}

fn open_formatted_store_after_corrupt_slot_without_wal_recovery() -> (usize, StartupError) {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    flash.write_region(0, wal_offset, &[0x10; 8]).unwrap();
    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset + 8,
        WalRecord::FreeRegion {
            collection_id: CollectionId(0),
            region_index: 1,
        },
    );

    (
        wal_offset,
        open_formatted_store::<128, 4, _>(&mut flash).unwrap_err(),
    )
}

fn open_formatted_store_after_corrupt_slot_with_wal_recovery() -> (usize, StartupState<8>) {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    flash.write_region(0, wal_offset, &[0x10; 8]).unwrap();
    let after_recovery = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset + 8,
        WalRecord::WalRecovery,
    );
    let next_offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_recovery,
        WalRecord::FreeRegion {
            collection_id: CollectionId(0),
            region_index: 2,
        },
    );

    (
        next_offset,
        open_formatted_store::<256, 4, _>(&mut flash).unwrap(),
    )
}

fn open_formatted_store_after_torn_slot_with_wal_recovery() -> (usize, StartupState<8>) {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len = encode_record_into(
        WalRecord::FreeRegion {
            collection_id: CollectionId(0),
            region_index: 3,
        },
        metadata,
        &mut physical,
        &mut logical,
    )
    .unwrap();
    assert!(encoded_len >= 8);
    flash.write_region(0, wal_offset, &physical[..4]).unwrap();

    let after_recovery = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset + 8,
        WalRecord::WalRecovery,
    );
    let next_offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_recovery,
        WalRecord::FreeRegion {
            collection_id: CollectionId(0),
            region_index: 2,
        },
    );

    (
        next_offset,
        open_formatted_store::<256, 4, _>(&mut flash).unwrap(),
    )
}

fn open_formatted_store_after_replayed_alloc_begin() -> (usize, StartupState<8>) {
    let mut flash = MockFlash::<256, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut physical = [0u8; 256];
    let mut logical = [0u8; 256];
    let alloc_len = encoded_record_len(
        WalRecord::AllocBegin {
            collection_id: CollectionId(0),
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
        metadata,
        &mut physical,
        &mut logical,
    )
    .unwrap();
    let link_len = encoded_record_len(
        WalRecord::Link {
            next_region_index: 1,
            expected_sequence: 1,
        },
        metadata,
        &mut physical,
        &mut logical,
    )
    .unwrap();
    assert!(256 - (wal_offset + alloc_len) >= alloc_len + link_len);

    let next_offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocBegin {
            collection_id: CollectionId(0),
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    );

    (
        next_offset,
        open_formatted_store::<256, 4, _>(&mut flash).unwrap(),
    )
}

fn open_formatted_store_after_completed_wal_rotation() -> StartupState<8> {
    let mut flash = MockFlash::<256, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocBegin {
            collection_id: CollectionId(0),
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_alloc,
        WalRecord::Link {
            next_region_index: 1,
            expected_sequence: 1,
        },
    );
    init_wal_region(&mut flash, 1, 1, 0, metadata.region_count);

    open_formatted_store::<256, 4, _>(&mut flash).unwrap()
}

fn open_formatted_store_from_fresh_format() -> (StorageMetadata, StartupState<8>) {
    let mut flash = MockFlash::<64, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let state = open_formatted_store::<64, 4, _>(&mut flash).unwrap();
    (metadata, state)
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-001 Read `StorageMetadata`, validate `metadata_checksum`, and validate static
//# geometry (`region_size`, `region_count`, `min_free_regions`, `erased_byte`, `wal_write_granule`,
//# `wal_record_magic`, and storage version support).
#[test]
fn requirement_open_formatted_store_requires_metadata() {
    let mut flash = MockFlash::<64, 4, 32>::new(0xff);
    let error = open_formatted_store::<64, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::MissingMetadata);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-003` Select main WAL tail as the unique candidate main
//# WAL region with the largest valid sequence. If no candidate main WAL
//# region exists, or if multiple candidate main WAL regions share that
//# largest valid sequence, return an error.
#[test]
fn requirement_open_formatted_store_rejects_duplicate_max_sequence_wal_candidates() {
    let mut flash = MockFlash::<64, 4, 32>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();

    let header = Header {
        sequence: 0,
        collection_id: CollectionId(0),
        collection_format: WAL_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    flash.write_region(1, 0, &header_bytes).unwrap();

    let error = open_formatted_store::<64, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::DuplicateWalTailSequence(0));
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-046` Startup tail selection MUST ignore regions with nonzero collection_id
//# even when their format is a private log format while still tracking max seen sequence.
#[test]
fn requirement_open_formatted_store_ignores_nonzero_collection_with_wal_format_when_selecting_tail()
{
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 1, 9, CollectionId(7), WAL_V1_FORMAT);

    let state = open_formatted_store::<128, 4, _>(&mut flash).unwrap();
    assert_eq!(state.wal_tail(), 0);
    assert_eq!(state.max_seen_sequence(), 9);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-006 Parse records in WAL order (region order, then offset order).
#[test]
fn requirement_open_formatted_store_rejects_post_corruption_record_at_the_next_wal_offset() {
    let (wal_offset, error) = open_formatted_store_after_corrupt_slot_without_wal_recovery();
    assert_eq!(
        error,
        StartupError::UnexpectedRecordAfterCorruption {
            region_index: 0,
            offset: wal_offset + 8,
        }
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-022` Replay MAY recover only from checksum-invalid or torn aligned WAL
//# slots. Replay tracks a pending WAL-recovery boundary from the first
//# ignored corrupt/torn aligned slot until a later valid `wal_recovery`
//# record is replayed.
#[test]
fn requirement_open_formatted_store_requires_wal_recovery_before_accepting_later_records() {
    let (wal_offset, error) = open_formatted_store_after_corrupt_slot_without_wal_recovery();
    assert_eq!(
        error,
        StartupError::UnexpectedRecordAfterCorruption {
            region_index: 0,
            offset: wal_offset + 8,
        }
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-024 Reconstruct runtime `free_list_tail` by following free-pointer links starting
//# at `last_free_list_head` until reaching a free region whose free-pointer slot is uninitialized.
#[test]
fn requirement_open_formatted_store_rejects_invalid_free_list_chain() {
    let mut flash = MockFlash::<64, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    assert_eq!(
        read_free_pointer_successor(&mut flash, metadata, 1).unwrap(),
        Some(2)
    );
    assert_eq!(
        read_free_pointer_successor(&mut flash, metadata, 2).unwrap(),
        Some(3)
    );
    assert_eq!(
        read_free_pointer_successor(&mut flash, metadata, 3).unwrap(),
        None
    );
    assert!(region_is_on_free_list_startup(&mut flash, metadata, Some(1), 1).unwrap());
    assert!(region_is_on_free_list_startup(&mut flash, metadata, Some(1), 2).unwrap());
    assert!(region_is_on_free_list_startup(&mut flash, metadata, Some(1), 3).unwrap());
    assert!(!region_is_on_free_list_startup(&mut flash, metadata, Some(1), 0).unwrap());
    assert!(!region_is_on_free_list_startup(&mut flash, metadata, None, 1).unwrap());
    assert_eq!(
        discover_free_list_head_from_footers(&mut flash, metadata).unwrap(),
        Some(1)
    );

    write_free_pointer_footer(&mut flash, 1, Some(3));
    write_free_pointer_footer(&mut flash, 2, Some(1));
    write_free_pointer_footer(&mut flash, 3, None);
    assert_eq!(
        discover_free_list_head_from_footers(&mut flash, metadata).unwrap(),
        Some(2)
    );

    let mut flash = MockFlash::<64, 4, 32>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();
    let footer = FreePointerFooter {
        next_tail: Some(99),
    };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut footer_bytes, 0xff).unwrap();
    flash
        .write_region(1, 64 - FreePointerFooter::ENCODED_LEN, &footer_bytes)
        .unwrap();

    let error = open_formatted_store::<64, 4, _>(&mut flash).unwrap_err();
    assert_eq!(
        error,
        StartupError::InvalidFreeListChain { region_index: 1 }
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-011` On
//# `alloc_begin(collection_id, region_index, allocation_sequence, free_list_head_after)`:
//# if `last_free_list_head = none`, return an error because allocation
//# cannot consume an empty durable free list.
//# if `last_free_list_head != region_index`, return an error because
//# `alloc_begin` did not consume the current durable free-list head.
//# if `allocation_sequence` is not greater than the current replayed
//# allocator sequence once lower-sequence retained allocation decisions
//# have been applied, return an error because allocator decisions are not
//# globally ordered. Set durable `last_free_list_head` to
//# `free_list_head_after` and set the replayed allocator sequence to
//# `allocation_sequence`.
//# If `collection_id = 0`, also require `ready_region` to be clear and set
//# `ready_region = region_index` for WAL rotation recovery.
#[test]
fn requirement_open_formatted_store_replays_alloc_begin_into_allocator_runtime_state() {
    let (_next_offset, state) = open_formatted_store_after_replayed_alloc_begin();
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.ready_region(), Some(1));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-023` Initialize allocator state from
//# `last_free_list_head` and the recovered global `allocation_sequence`.
#[test]
fn requirement_open_formatted_store_initializes_allocator_state_after_alloc_begin() {
    let (_next_offset, state) = open_formatted_store_after_replayed_alloc_begin();
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.free_list_tail(), Some(3));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-025 If `ready_region` is set, hold it in memory as the WAL-rotation target
//# before consuming another free-list entry for rotation.
#[test]
fn requirement_open_formatted_store_keeps_replayed_ready_region_reserved_in_memory() {
    let (_next_offset, state) = open_formatted_store_after_replayed_alloc_begin();
    assert_eq!(state.ready_region(), Some(1));
    assert_eq!(state.last_free_list_head(), Some(2));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-017` On
//# `commit_transaction(transaction_log_id, range)`:
//# verify that `transaction_log_id` and `range` match an active
//# transaction descriptor or a recoverable committed range.
#[test]
fn requirement_open_formatted_store_replays_finished_transaction_interval() {
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);

    let after_new_collection = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    let after_begin = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_new_collection,
        crate::test_begin_transaction_record(collection_id),
    );
    let after_update = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_begin,
        WalRecord::Update {
            collection_id,
            payload: &[1, 2, 3],
        },
    );
    let after_commit = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_update,
        crate::test_commit_transaction_record(collection_id),
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_commit,
        crate::test_transaction_finished_record(collection_id),
    );

    let state = open_formatted_store::<512, 4, _>(&mut flash).unwrap();
    let collection = collection_summary(&state, collection_id);
    assert_eq!(collection.pending_update_count(), 1);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-019` On
//# `rollback_transaction(transaction_log_id, range)`:
//# scan the referenced transaction-log range for transaction-owned
//# allocation and cleanup effects, confirm rollback recovery has made those
//# effects non-visible and reclaimable, and do not apply collection
//# mutations from the range.
#[test]
fn requirement_open_formatted_store_rolls_back_only_transaction_collection_records() {
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let transaction_collection = CollectionId(7);
    let unrelated_collection = CollectionId(8);

    let mut offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id: transaction_collection,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::NewCollection {
            collection_id: unrelated_collection,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_begin_transaction_record(transaction_collection),
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::Update {
            collection_id: transaction_collection,
            payload: &[1],
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::Update {
            collection_id: unrelated_collection,
            payload: &[2],
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_rollback_transaction_record(transaction_collection),
    );

    let state = open_formatted_store::<512, 4, _>(&mut flash).unwrap();
    let transaction = collection_summary(&state, transaction_collection);
    let unrelated = collection_summary(&state, unrelated_collection);
    assert_eq!(transaction.pending_update_count(), 0);
    assert_eq!(unrelated.pending_update_count(), 1);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-011A` Transaction-log records are not applied by
//# ordinary log-chain traversal. Startup scans a transaction-log range only
//# when a retained main-WAL `commit_transaction(transaction_log_id, range)`,
//# `rollback_transaction(transaction_log_id, range)`,
//# `transaction_finished(transaction_log_id, range)`, or active recovery
//# descriptor references that range.
#[test]
fn requirement_open_formatted_store_recovers_unfinished_transaction_before_commit() {
    let mut flash = MockFlash::<512, 6, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);

    let after_new_collection = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    let after_begin = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_new_collection,
        crate::test_begin_transaction_record(collection_id),
    );
    let after_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_begin,
        WalRecord::AllocBegin {
            collection_id,
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_alloc,
        WalRecord::Update {
            collection_id,
            payload: &[1],
        },
    );

    flash.clear_operations();
    let state = open_formatted_store::<512, 6, _>(&mut flash).unwrap();
    let collection = collection_summary(&state, collection_id);
    assert_eq!(collection.pending_update_count(), 0);
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.free_list_tail(), Some(1));

    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let region_five_footer =
        FreePointerFooter::decode(&flash.region_bytes(5).unwrap()[footer_offset..], 0xff).unwrap();
    let recovered_footer =
        FreePointerFooter::decode(&flash.region_bytes(1).unwrap()[footer_offset..], 0xff);
    assert_eq!(region_five_footer.next_tail, Some(1));
    assert_eq!(recovered_footer.unwrap().next_tail, None);
    let recovered_erase = flash
        .operations()
        .iter()
        .position(|operation| *operation == (crate::MockOperation::EraseRegion { region_index: 1 }))
        .unwrap();
    let tail_link = flash
        .operations()
        .iter()
        .position(|operation| {
            matches!(
                operation,
                crate::MockOperation::WriteRegion {
                    region_index: 5,
                    offset,
                    len,
                } if *offset == footer_offset && *len == FreePointerFooter::ENCODED_LEN
            )
        })
        .unwrap();
    assert!(recovered_erase < tail_link);

    let reopened = open_formatted_store::<512, 6, _>(&mut flash).unwrap();
    assert_eq!(reopened.last_free_list_head(), Some(2));
    assert_eq!(reopened.free_list_tail(), Some(1));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-021` If replay reaches WAL end with an active
//# transaction descriptor that has no committed range, run idempotent
//# rollback recovery by scanning that transaction-log range for
//# transaction-owned allocations, returning them through allocator
//# recovery, and appending
//# `rollback_transaction(transaction_log_id, range)`.
#[test]
fn requirement_open_formatted_store_finishes_post_commit_transaction_cleanup() {
    let mut flash = MockFlash::<512, 6, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);

    flash.erase_region(1).unwrap();
    init_user_region_header(&mut flash, 1, 1, collection_id, MAP_MANIFEST_V2_FORMAT);
    flash
        .write_region(1, Header::ENCODED_LEN, &0u32.to_le_bytes())
        .unwrap();

    let mut offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::AllocBegin {
            collection_id,
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::Head {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
            region_index: 1,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_begin_transaction_record(collection_id),
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::DropCollection { collection_id },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_commit_transaction_record(collection_id),
    );

    flash.clear_operations();
    let state = open_formatted_store::<512, 6, _>(&mut flash).unwrap();
    let collection = collection_summary(&state, collection_id);
    assert_eq!(collection.basis(), StartupCollectionBasis::Dropped);
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.free_list_tail(), Some(1));
    assert!(!flash.operations().iter().any(|operation| {
        *operation == (crate::MockOperation::EraseRegion { region_index: 1 })
    }));

    let reopened = open_formatted_store::<512, 6, _>(&mut flash).unwrap();
    assert_eq!(
        collection_summary(&reopened, collection_id).basis(),
        StartupCollectionBasis::Dropped
    );
    assert_eq!(reopened.free_list_tail(), Some(1));
}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=test
//# `RING-TX-RECOVERY-001` If startup reaches main WAL end with an open
//# transaction descriptor and no durable
//# `commit_transaction(transaction_log_id, range)`, it MUST run rollback
//# recovery for that transaction-log range and append
//# `rollback_transaction(transaction_log_id, range)`.
#[test]
fn requirement_startup_recovers_uncommitted_transaction_with_rollback_marker() {
    let mut flash = setup_precommit_unfinished_transaction();
    let collection_id = CollectionId(7);

    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    let collection = storage_collection_summary(&storage, collection_id);
    assert_eq!(collection.pending_update_count(), 0);
    assert_eq!(storage.last_free_list_head(), Some(2));
    assert_eq!(storage.free_list_tail(), Some(1));

    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 1);
    assert_eq!(
        counts,
        RecoveryRecordCounts {
            free_region: 1,
            rollback_transaction: 1,
            transaction_finished: 0,
        }
    );
}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=test
//# `RING-TX-RECOVERY-002` If startup reaches main WAL end after
//# `commit_transaction(transaction_log_id, range)` but before
//# `transaction_finished(transaction_log_id, range)`, it MUST preserve the
//# committed collection state imported from that transaction-log range,
//# finish cleanup frees derived from the committed range, and append
//# `transaction_finished(transaction_log_id, range)`.
#[test]
fn requirement_startup_finishes_post_commit_transaction_cleanup_with_finished_marker() {
    let mut flash = setup_postcommit_unfinished_transaction();
    let collection_id = CollectionId(7);

    flash.clear_operations();
    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    let collection = storage_collection_summary(&storage, collection_id);
    assert_eq!(collection.basis(), StartupCollectionBasis::Dropped);
    assert_eq!(storage.last_free_list_head(), Some(2));
    assert_eq!(storage.free_list_tail(), Some(1));

    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 1);
    assert_eq!(
        counts,
        RecoveryRecordCounts {
            free_region: 1,
            rollback_transaction: 0,
            transaction_finished: 1,
        }
    );
    drop(storage);

    assert!(!flash.operations().iter().any(|operation| {
        *operation == (crate::MockOperation::EraseRegion { region_index: 1 })
    }));
}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=test
//# `RING-TX-RECOVERY-003` Both rollback recovery and cleanup recovery
//# MUST be idempotent if startup crashes before the terminal marker is
//# durable.
#[test]
fn requirement_transaction_recovery_is_idempotent() {
    let collection_id = CollectionId(7);
    let mut precommit_flash = setup_precommit_recovery_after_allocation_free_before_rollback();

    let mut precommit_storage =
        Storage::<_, 512, 6, 8>::open(&mut precommit_flash, crate::test_storage_memory()).unwrap();
    let precommit_counts =
        count_recovery_records_in_storage(&mut precommit_storage, collection_id, 1);
    assert_eq!(
        precommit_counts,
        RecoveryRecordCounts {
            free_region: 1,
            rollback_transaction: 1,
            transaction_finished: 0,
        }
    );
    drop(precommit_storage);

    let mut reopened_precommit =
        Storage::<_, 512, 6, 8>::open(&mut precommit_flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        count_recovery_records_in_storage(&mut reopened_precommit, collection_id, 1),
        precommit_counts
    );
    assert_eq!(
        storage_collection_summary(&reopened_precommit, collection_id).pending_update_count(),
        0
    );

    let mut postcommit_flash = setup_postcommit_recovery_after_cleanup_free_before_finished();

    let mut postcommit_storage =
        Storage::<_, 512, 6, 8>::open(&mut postcommit_flash, crate::test_storage_memory()).unwrap();
    let postcommit_counts =
        count_recovery_records_in_storage(&mut postcommit_storage, collection_id, 1);
    assert_eq!(
        postcommit_counts,
        RecoveryRecordCounts {
            free_region: 1,
            rollback_transaction: 0,
            transaction_finished: 1,
        }
    );
    drop(postcommit_storage);

    let mut reopened_postcommit =
        Storage::<_, 512, 6, 8>::open(&mut postcommit_flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        count_recovery_records_in_storage(&mut reopened_postcommit, collection_id, 1),
        postcommit_counts
    );
    assert_eq!(
        storage_collection_summary(&reopened_postcommit, collection_id).basis(),
        StartupCollectionBasis::Dropped
    );
}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=test
//# `RING-TX-RECOVERY-004` The configured minimum free-region reserve MUST leave enough WAL
//# capacity for startup recovery to append a required terminal transaction
//# record.
#[test]
fn requirement_min_free_region_reserve_covers_transaction_terminal_records() {
    let mut flash = setup_precommit_transaction_requiring_recovery_rotation();
    let collection_id = CollectionId(7);

    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    assert_eq!(storage.wal_tail(), 0);
    assert_eq!(storage.last_free_list_head(), Some(1));
    assert!(storage.free_list_tail().is_some());
    assert_eq!(
        storage_collection_summary(&storage, collection_id).pending_update_count(),
        0
    );

    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 1);
    assert_eq!(
        counts,
        RecoveryRecordCounts {
            free_region: 0,
            rollback_transaction: 1,
            transaction_finished: 0,
        }
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-010` `begin_transaction`
//# Main-WAL-only record. Opens a transaction descriptor and assigns it a
//# transaction log. Payload is
//# `transaction_log_id:u32, start:LogPosition`, where `start` is the first
//# transaction-log position owned by this transaction.
#[test]
fn requirement_wal_begin_transaction_record_starts_collection_interval() {
    let mut flash = setup_precommit_unfinished_transaction();
    let collection_id = CollectionId(7);

    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    assert_eq!(
        storage_collection_summary(&storage, collection_id).pending_update_count(),
        0
    );
    assert_eq!(
        count_recovery_records_in_storage(&mut storage, collection_id, 1).rollback_transaction,
        1
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-012` `commit_transaction`
//# Main-WAL-only record. The payload is
//# `transaction_log_id:u32, range:TransactionLogRange`. It freezes the
//# named range in that transaction log and imports that range into
//# main-WAL replay at the position of this commit record.
#[test]
fn requirement_wal_commit_transaction_record_marks_update_phase() {
    let mut flash = setup_postcommit_unfinished_transaction();
    let collection_id = CollectionId(7);

    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    assert_eq!(
        storage_collection_summary(&storage, collection_id).basis(),
        StartupCollectionBasis::Dropped
    );
    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 1);
    assert_eq!(counts.free_region, 1);
    assert_eq!(counts.transaction_finished, 1);
    assert_eq!(counts.rollback_transaction, 0);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-013` `transaction_finished`
//# Main-WAL-only record. The payload is
//# `transaction_log_id:u32, range:TransactionLogRange`. It records that the
//# committed transaction's cleanup and recovery obligations are complete,
//# so transaction-log garbage collection may release this reference when
//# no other retained record or active descriptor points to the same
//# transaction-log range.
#[test]
fn requirement_wal_transaction_finished_record_closes_cleanup_phase() {
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);

    let mut offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id,
            collection_type: CollectionType::MAP_CODE,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_begin_transaction_record(collection_id),
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::Update {
            collection_id,
            payload: &[1, 2, 3],
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_commit_transaction_record(collection_id),
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        crate::test_transaction_finished_record(collection_id),
    );

    let mut storage =
        Storage::<_, 512, 4, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        storage_collection_summary(&storage, collection_id).pending_update_count(),
        1
    );

    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 1);
    assert_eq!(counts.transaction_finished, 1);
    assert_eq!(counts.rollback_transaction, 0);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-014` `rollback_transaction`
//# Main-WAL-only record. The payload is
//# `transaction_log_id:u32, range:TransactionLogRange`. It records that the
//# transaction did not become visible and that rollback recovery for
//# transaction-owned storage effects in the transaction-log range has
//# completed.
#[test]
fn requirement_wal_rollback_transaction_record_closes_data_recovery() {
    let mut flash = setup_precommit_recovered_with_rollback_transaction();
    let collection_id = CollectionId(7);

    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    assert_eq!(
        storage_collection_summary(&storage, collection_id).pending_update_count(),
        0
    );
    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 1);
    assert_eq!(
        counts,
        RecoveryRecordCounts {
            free_region: 1,
            rollback_transaction: 1,
            transaction_finished: 0,
        }
    );
    drop(storage);

    let mut reopened =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        count_recovery_records_in_storage(&mut reopened, collection_id, 1),
        counts
    );
}

//= spec/ring/07-reclaim.md#free-region
//= type=test
//# `RING-FREE-REGION-PRE-003` The owning collection's committed
//# transaction state MUST contain enough durable information for cleanup
//# recovery to derive that `region_index` must be freed.
#[test]
fn requirement_collection_state_contains_cleanup_free_plan() {
    let mut flash = setup_postcommit_unfinished_transaction();
    let collection_id = CollectionId(7);

    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    assert_eq!(
        storage_collection_summary(&storage, collection_id).basis(),
        StartupCollectionBasis::Dropped
    );
    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 1);
    assert_eq!(counts.free_region, 1);
    assert_eq!(counts.transaction_finished, 1);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-010` The recovered append point for the tail region
//# MUST be the first aligned
//# slot whose first byte is `erased_byte` after the last valid replayed
//# tail record. If no such slot exists, the tail region is currently full
//# and the next log append must rotate via `link` to a new private log
//# region.
#[test]
fn requirement_open_formatted_store_recovers_append_point_after_replayed_alloc_begin() {
    let (next_offset, state) = open_formatted_store_after_replayed_alloc_begin();
    assert_eq!(state.wal_append_offset(), next_offset);
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 0);
}

//= spec/ring/08-durability-formatting.md#durability-and-crash-semantics
//= type=test
//# `RING-DURABILITY-003` Replay MUST treat partially written records as
//# torn and ignore them using checksum validation and WAL tail recovery
//# rules.
#[test]
fn requirement_open_formatted_store_ignores_torn_tail_slots_after_wal_recovery() {
    let (next_offset, state) = open_formatted_store_after_torn_slot_with_wal_recovery();

    assert_eq!(state.wal_append_offset(), next_offset);
    assert_eq!(state.last_free_list_head(), Some(1));
    assert!(!state.pending_wal_recovery_boundary());
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-CHECKSUM-005` An implementation MUST ensure that even
//# intentionally corrupted storage eventually produces a reported error
//# rather than memory unsafety, undefined behavior, control-flow
//# corruption, infinite loops, or unbounded resource consumption
//# amounting to denial of service.
#[test]
fn requirement_open_formatted_store_reports_an_error_for_intentionally_corrupted_wal_bytes() {
    let (_wal_offset, error) = open_formatted_store_after_corrupt_slot_without_wal_recovery();

    assert!(matches!(
        error,
        StartupError::UnexpectedRecordAfterCorruption { .. }
    ));
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-006` A `WALSnapshotClean` basis MUST be loadable into RAM
//# before that collection accepts further mutations.
#[test]
fn requirement_open_formatted_store_tracks_live_collection_snapshot_basis() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let next_offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Snapshot {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            payload: &[1, 2, 3],
        },
    );

    let state = open_formatted_store::<256, 4, _>(&mut flash).unwrap();

    assert_eq!(state.wal_append_offset(), next_offset);
    assert_eq!(state.tracked_user_collection_count(), 1);
    assert_eq!(state.collections().len(), 1);
    assert_eq!(state.collections()[0].collection_id(), CollectionId(7));
    assert_eq!(
        state.collections()[0].collection_type(),
        Some(CollectionType::MAP_CODE)
    );
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::WalSnapshot
    );
    assert_eq!(state.collections()[0].pending_update_count(), 0);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-049` Startup replay MUST count multiple live collections independently.
#[test]
fn requirement_open_formatted_store_counts_multiple_live_collections() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_first = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_first,
        WalRecord::NewCollection {
            collection_id: CollectionId(8),
            collection_type: CollectionType::MAP_CODE,
        },
    );

    let state = open_formatted_store::<128, 4, _>(&mut flash).unwrap();
    assert_eq!(state.tracked_user_collection_count(), 2);
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-006` For a live user collection, the earliest retained
//# type-bearing record seen during replay MUST establish the
//# replay-tracked `collection_type`, and every later valid type-bearing
//# record for that collection MUST carry the same `collection_type`.
#[test]
fn requirement_open_formatted_store_rejects_later_type_bearing_records_with_mismatched_collection_type(
) {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(7), 1);

    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_snapshot = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Snapshot {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            payload: &[1, 2, 3],
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_snapshot,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::CHANNEL_CODE,
            region_index: 2,
        },
    );

    let error = open_formatted_store::<256, 4, _>(&mut flash).unwrap_err();
    assert_eq!(
        error,
        StartupError::CollectionTypeMismatch {
            collection_id: CollectionId(7),
            expected: CollectionType::MAP_CODE,
            actual: CollectionType::CHANNEL_CODE,
        }
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-050` Startup replay MUST accept a committed-region head basis and recover
//# the collection basis, collection type, and max seen sequence from that region.
#[test]
fn requirement_open_formatted_store_accepts_committed_region_head_basis() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(7), 1);

    let wal_offset = metadata.wal_record_area_offset().unwrap();
    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            region_index: 2,
        },
    );

    let state = open_formatted_store::<256, 4, _>(&mut flash).unwrap();

    assert_eq!(state.tracked_user_collection_count(), 1);
    assert_eq!(state.collections().len(), 1);
    assert_eq!(state.collections()[0].collection_id(), CollectionId(7));
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(2)
    );
    assert_eq!(
        state.collections()[0].collection_type(),
        Some(CollectionType::MAP_CODE)
    );
    assert_eq!(state.max_seen_sequence(), 4);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-051` Startup replay MUST accept a replaced historical head and recover the
//# live replacement head with no incomplete transaction work.
#[test]
fn requirement_open_formatted_store_accepts_reclaimed_historical_head_after_replacement() {
    let mut flash = MockFlash::<256, 5, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 1, 4, CollectionId(7), 1);
    init_user_region_header(&mut flash, 2, 5, CollectionId(7), 1);

    let footer_offset = 256 - FreePointerFooter::ENCODED_LEN;
    let footer = FreePointerFooter { next_tail: Some(1) };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut footer_bytes, 0xff).unwrap();
    flash.write_region(4, footer_offset, &footer_bytes).unwrap();
    flash.erase_region(1).unwrap();

    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_first_head = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            region_index: 1,
        },
    );
    let after_second_head = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_first_head,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            region_index: 2,
        },
    );
    let after_first_free = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_second_head,
        WalRecord::FreeRegion {
            collection_id: CollectionId(0),
            region_index: 1,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_first_free,
        WalRecord::FreeRegion {
            collection_id: CollectionId(0),
            region_index: 1,
        },
    );

    let state = open_formatted_store::<256, 5, _>(&mut flash).unwrap();

    assert_eq!(state.collections().len(), 1);
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(2)
    );
    assert_eq!(state.free_list_tail(), Some(1));
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-052` Startup replay MUST track pending updates on an empty collection
//# basis and preserve their count.
#[test]
fn requirement_open_formatted_store_tracks_pending_updates_on_empty_collection_basis() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_new = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::NewCollection {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
        },
    );
    let after_update_1 = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_new,
        WalRecord::Update {
            collection_id: CollectionId(7),
            payload: &[1],
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_update_1,
        WalRecord::Update {
            collection_id: CollectionId(7),
            payload: &[2],
        },
    );

    let state = open_formatted_store::<256, 4, _>(&mut flash).unwrap();

    assert_eq!(state.tracked_user_collection_count(), 1);
    assert_eq!(state.collections().len(), 1);
    assert_eq!(state.collections()[0].collection_id(), CollectionId(7));
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );
    assert_eq!(state.collections()[0].pending_update_count(), 2);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-053` Startup replay MUST reject update records that appear after a
//# collection drop tombstone for the same collection.
#[test]
fn requirement_open_formatted_store_rejects_update_after_drop_collection() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_drop = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::DropCollection {
            collection_id: CollectionId(7),
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_drop,
        WalRecord::Update {
            collection_id: CollectionId(7),
            payload: &[9],
        },
    );

    let error = open_formatted_store::<256, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::DroppedCollection(CollectionId(7)));
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-015` An implementation MUST NOT open a database successfully if replay yields a
//# live collection whose `collection_type` is unsupported by that implementation.
#[test]
fn requirement_open_formatted_store_rejects_unsupported_live_collection_type() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Snapshot {
            collection_id: CollectionId(7),
            collection_type: 0x1234,
            payload: &[1],
        },
    );

    let error = open_formatted_store::<256, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::UnsupportedLiveCollectionType(0x1234));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-028` If replay yields a live collection whose
//# `collection_type` is unsupported by the implementation, startup MUST
//# fail before transaction cleanup frees any region based on collection
//# reachability.
#[test]
fn requirement_open_formatted_store_fails_startup_for_unsupported_live_collection_type() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Snapshot {
            collection_id: CollectionId(7),
            collection_type: 0x1234,
            payload: &[1],
        },
    );

    let error = open_formatted_store::<256, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::UnsupportedLiveCollectionType(0x1234));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-030` A dropped tombstone whose old
//# `collection_type` is unsupported MAY remain as inert metadata and
//# does not by itself require startup failure.
#[test]
fn requirement_validate_live_collection_types_ignores_unsupported_dropped_tombstones() {
    let collections = [StartupCollection {
        collection_id: CollectionId(7),
        collection_type: Some(0x1234),
        basis: StartupCollectionBasis::Dropped,
        pending_update_count: 0,
    }];

    assert_eq!(validate_live_collection_types(&collections), Ok(()));
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-054` Strict private-log region reads MUST reject regions whose
//# collection_id is nonzero even if collection_format is a private log format.
#[test]
fn requirement_read_strict_wal_region_rejects_nonzero_collection_id_even_with_wal_format() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_wal_region(&mut flash, 1, 1, 0, metadata.region_count);
    init_user_region_header(&mut flash, 1, 1, CollectionId(7), WAL_V1_FORMAT);

    assert_eq!(
        read_strict_wal_region(&mut flash, 1, metadata.region_count),
        Err(StartupError::InvalidWalRegion(1))
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-055` WAL target validation MUST require collection_id 0 and
//# the expected private log collection_format.
#[test]
fn requirement_has_valid_wal_target_requires_both_wal_collection_id_and_format() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_wal_region(&mut flash, 1, 1, 0, metadata.region_count);

    init_user_region_header(&mut flash, 1, 1, CollectionId(7), WAL_V1_FORMAT);
    assert_eq!(
        has_valid_wal_target(&mut flash, 1, 1, metadata.region_count),
        Ok(false)
    );

    init_user_region_header(
        &mut flash,
        1,
        1,
        CollectionId(0),
        crate::MAP_REGION_V2_FORMAT,
    );
    assert_eq!(
        has_valid_wal_target(&mut flash, 1, 1, metadata.region_count),
        Ok(false)
    );
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-016` Shared storage validation MUST reject a live retained committed-region
//# basis whose referenced region header does not belong to that collection.
#[test]
fn requirement_validate_live_region_bases_rejects_committed_region_for_different_collection() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(
        &mut flash,
        2,
        4,
        CollectionId(8),
        crate::MAP_REGION_V2_FORMAT,
    );
    let collections = [StartupCollection {
        collection_id: CollectionId(7),
        collection_type: Some(CollectionType::MAP_CODE),
        basis: StartupCollectionBasis::Region(2),
        pending_update_count: 0,
    }];

    assert_eq!(
        validate_live_region_bases(&mut flash, &collections),
        Err(StartupError::InvalidCommittedRegionHead {
            collection_id: CollectionId(7),
            region_index: 2,
        })
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-057` Region index validation MUST reject a region_index equal to
//# region_count.
#[test]
fn requirement_ensure_region_index_in_range_rejects_region_count_boundary() {
    assert_eq!(
        ensure_region_index_in_range(4, 4),
        Err(StartupError::InvalidRegionReference(4))
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-005 Walk the WAL region chain from the resulting WAL head to tail using `link`
//# records.
#[test]
fn requirement_open_formatted_store_follows_completed_link_to_the_next_wal_tail() {
    let state = open_formatted_store_after_completed_wal_rotation();
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 1);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-013 On `link(next_region_index, expected_sequence)`:
//# if `ready_region = next_region_index`, clear `ready_region`.
#[test]
fn requirement_open_formatted_store_clears_ready_region_when_link_matches_it() {
    let state = open_formatted_store_after_completed_wal_rotation();
    assert_eq!(state.ready_region(), None);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-058` Startup replay MUST recover a WAL rotation after a durable link by
//# selecting the linked tail, resetting tail append offset, updating allocator state, and advancing
//# max sequence.
#[test]
fn requirement_open_formatted_store_recovers_rotation_after_link() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocBegin {
            collection_id: CollectionId(0),
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_alloc,
        WalRecord::Link {
            next_region_index: 1,
            expected_sequence: 1,
        },
    );

    let state = open_formatted_store::<256, 4, _>(&mut flash).unwrap();

    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 1);
    assert_eq!(
        state.wal_append_offset(),
        metadata.wal_record_area_offset().unwrap()
    );
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.free_list_tail(), Some(3));
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.max_seen_sequence(), 1);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-059` Startup replay MUST recover a WAL rotation when alloc_begin is
//# durable but link is absent and only rotation reserve remains.
#[test]
fn requirement_open_formatted_store_recovers_rotation_before_link() {
    let mut flash = MockFlash::<160, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let alloc_record = WalRecord::AllocBegin {
        collection_id: CollectionId(0),
        region_index: 1,
        allocation_sequence: 0,
        free_list_head_after: Some(2),
    };
    let filler_record = WalRecord::Head {
        collection_id: CollectionId(0),
        collection_type: CollectionType::WAL_CODE,
        region_index: 0,
    };
    let mut physical = [0u8; 160];
    let mut logical = [0u8; 160];
    let alloc_len =
        encoded_record_len(alloc_record, metadata, &mut physical, &mut logical).unwrap();
    let link_len = encoded_record_len(
        WalRecord::Link {
            next_region_index: 1,
            expected_sequence: 1,
        },
        metadata,
        &mut physical,
        &mut logical,
    )
    .unwrap();

    let mut offset = wal_offset;
    loop {
        let remaining_after_alloc = 160 - (offset + alloc_len);
        if remaining_after_alloc >= link_len && remaining_after_alloc < alloc_len + link_len {
            break;
        }

        let filler_len =
            encoded_record_len(filler_record, metadata, &mut physical, &mut logical).unwrap();
        assert!(offset + filler_len + alloc_len <= 160);
        offset = append_wal_record(&mut flash, metadata, 0, offset, filler_record);
    }

    append_wal_record(&mut flash, metadata, 0, offset, alloc_record);

    let state = open_formatted_store::<160, 4, _>(&mut flash).unwrap();

    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 1);
    assert_eq!(
        state.wal_append_offset(),
        metadata.wal_record_area_offset().unwrap()
    );
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.free_list_tail(), Some(3));
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.max_seen_sequence(), 1);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-060` Startup replay MUST recover a WAL rotation when only the link record
//# fits after alloc_begin at the tail boundary.
#[test]
fn requirement_open_formatted_store_recovers_rotation_when_only_the_link_record_fits_after_alloc_begin(
) {
    const REGION_SIZE: usize = 256;

    let mut flash = MockFlash::<REGION_SIZE, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    let alloc_record = WalRecord::AllocBegin {
        collection_id: CollectionId(0),
        region_index: 1,
        allocation_sequence: 0,
        free_list_head_after: Some(2),
    };
    let alloc_len =
        encoded_record_len(alloc_record, metadata, &mut physical, &mut logical).unwrap();
    let link_len = encoded_record_len(
        WalRecord::Link {
            next_region_index: 1,
            expected_sequence: 1,
        },
        metadata,
        &mut physical,
        &mut logical,
    )
    .unwrap();
    let payload = [0u8; REGION_SIZE];
    let payload_len = (0..=payload.len())
        .find(|payload_len| {
            encoded_record_len(
                WalRecord::Snapshot {
                    collection_id: CollectionId(7),
                    collection_type: CollectionType::MAP_CODE,
                    payload: &payload[..*payload_len],
                },
                metadata,
                &mut physical,
                &mut logical,
            )
            .is_ok_and(|filler_len| wal_offset + filler_len + alloc_len + link_len == REGION_SIZE)
        })
        .expect("snapshot payload should align alloc_begin to exact link capacity");
    let after_filler = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Snapshot {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            payload: &payload[..payload_len],
        },
    );
    let after_alloc = append_wal_record(&mut flash, metadata, 0, after_filler, alloc_record);
    assert_eq!(REGION_SIZE - after_alloc, link_len);

    let state = open_formatted_store::<REGION_SIZE, 4, _>(&mut flash).unwrap();
    assert_eq!(state.wal_tail(), 1);
    assert_eq!(state.max_seen_sequence(), 1);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-061` Startup replay MUST reject an unrecovered corrupt boundary in a
//# non-tail WAL region as a broken WAL chain.
#[test]
fn requirement_open_formatted_store_rejects_unrecovered_boundary_in_non_tail_wal_region() {
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_link = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Link {
            next_region_index: 1,
            expected_sequence: 1,
        },
    );
    init_wal_region(&mut flash, 1, 1, 0, metadata.region_count);
    let corrupt_tail = [0x10; 128];
    flash
        .write_region(0, after_link, &corrupt_tail[..128 - after_link])
        .unwrap();

    let error = open_formatted_store::<128, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::BrokenWalChain { region_index: 0 });
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-020 On `wal_recovery()`: if `pending_wal_recovery_boundary` is clear, return an
//# error. otherwise clear `pending_wal_recovery_boundary`.
#[test]
fn requirement_open_formatted_store_clears_pending_recovery_boundary_when_wal_recovery_is_replayed()
{
    let (next_offset, state) = open_formatted_store_after_corrupt_slot_with_wal_recovery();
    assert_eq!(state.wal_append_offset(), next_offset);
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.last_free_list_head(), Some(1));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-002` Scan all regions, collect candidate main WAL
//# regions (`collection_id == 0` plus `collection_format = main_wal_v2`)
//# and candidate transaction-log regions (`collection_id == 0` plus
//# `collection_format = transaction_log_v2`) with valid headers, and track
//# `max_seen_sequence` as the largest
//# `sequence` value seen in any valid region header.
#[test]
fn requirement_open_formatted_store_scans_fresh_store_geometry_for_wal_candidates() {
    let (metadata, state) = open_formatted_store_from_fresh_format();
    assert_eq!(state.metadata(), metadata);
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 0);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-004` Read and validate the `LogRegionPrologue` stored at the start of the
//# main WAL tail region's user-data area, and use its
//# `log_head_region_index` as the initial WAL-head candidate.
#[test]
fn requirement_open_formatted_store_uses_the_tail_prologue_as_the_initial_wal_head_candidate() {
    let (_metadata, state) = open_formatted_store_from_fresh_format();
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 0);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-062` Opening a freshly formatted store MUST initialize allocator free-list
//# head and tail from the formatted free-list chain.
#[test]
fn requirement_open_formatted_store_initializes_allocator_state_for_a_fresh_store() {
    let (_metadata, state) = open_formatted_store_from_fresh_format();
    assert_eq!(state.last_free_list_head(), Some(1));
    assert_eq!(state.free_list_tail(), Some(3));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-026 Keep `max_seen_sequence` as the runtime source of the next region sequence.
#[test]
fn requirement_open_formatted_store_keeps_max_seen_sequence_for_the_next_region_header() {
    let (_metadata, state) = open_formatted_store_from_fresh_format();
    assert_eq!(state.max_seen_sequence(), 0);
}

//= spec/ring/06-startup-replay.md#why-reclaimed-wal-regions-cannot-confuse-startup
//= type=test
//# `RING-BOOTSTRAP-005` Startup derives the WAL head only from the selected tail's
#[test]
fn requirement_open_formatted_store_reports_recovered_nonzero_wal_head() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    init_wal_region(&mut flash, 0, 2, 1, metadata.region_count);
    init_wal_region(&mut flash, 1, 1, 1, metadata.region_count);
    append_wal_record(
        &mut flash,
        metadata,
        1,
        wal_offset,
        WalRecord::Link {
            next_region_index: 0,
            expected_sequence: 2,
        },
    );

    let state = open_formatted_store::<128, 4, _>(&mut flash).unwrap();
    assert_eq!(state.wal_head(), 1);
    assert_eq!(state.wal_tail(), 0);
    assert_eq!(state.last_free_list_head(), Some(2));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-027` If replay encountered a torn or checksum-invalid tail record,
#[test]
fn requirement_open_formatted_store_preserves_pending_tail_recovery_boundary() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    flash.write_region(0, wal_offset, &[0x10; 8]).unwrap();

    let state = open_formatted_store::<128, 4, _>(&mut flash).unwrap();
    assert!(state.pending_wal_recovery_boundary());
    assert_eq!(state.wal_append_offset(), wal_offset + 8);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-RESULT-007` Transaction-log cursors, live-prefix boundaries, active transaction
#[test]
fn requirement_startup_open_plan_clears_transaction_recovery_scratch() {
    let mut plan = StartupOpenPlan::<4, 8>::empty();
    plan.wal_chain.push(0).unwrap();
    plan.collections
        .push(startup_test_collection(CollectionId(7)))
        .unwrap();
    plan.transaction_original_collections
        .push(startup_test_collection(CollectionId(8)))
        .unwrap();
    plan.transaction_allocations.push(1).unwrap();
    plan.transaction_frees.push(2).unwrap();
    plan.transaction_cleanup_regions.push(3).unwrap();
    plan.transaction_old_regions.push(1).unwrap();
    plan.transaction_new_regions.push(2).unwrap();

    plan.clear_transaction_recovery_scratch();

    assert_eq!(plan.wal_chain.as_slice(), &[0]);
    assert_eq!(plan.collections.len(), 1);
    assert!(plan.transaction_original_collections.is_empty());
    assert!(plan.transaction_allocations.is_empty());
    assert!(plan.transaction_frees.is_empty());
    assert!(plan.transaction_cleanup_regions.is_empty());
    assert!(plan.transaction_old_regions.is_empty());
    assert!(plan.transaction_new_regions.is_empty());

    plan.transaction_allocations.push(1).unwrap();
    plan.clear();
    assert!(plan.wal_chain.is_empty());
    assert!(plan.collections.is_empty());
    assert!(plan.transaction_allocations.is_empty());
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-024` If replay reaches the end of a reachable
#[test]
fn requirement_non_tail_wal_replay_rejects_unrecovered_boundary() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    flash.write_region(0, wal_offset, &[0x10; 8]).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, wal_offset);

    let scan_error =
        scan_wal_region::<128, _, _>(&mut flash, &mut workspace, metadata, 0, false, |_, _, _| {
            Ok(())
        })
        .unwrap_err();
    assert_eq!(scan_error, StartupError::BrokenWalChain { region_index: 0 });

    let replay_error = replay_open_wal_region::<128, 4, _, 8>(
        &mut flash,
        &mut workspace,
        &mut plan,
        0,
        0,
        false,
        &mut None,
    )
    .unwrap_err();
    assert_eq!(
        replay_error,
        StartupError::BrokenWalChain { region_index: 0 }
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-016` On `begin_transaction(transaction_log_id, start)`:
#[test]
fn requirement_classify_replay_record_opens_transaction_descriptor() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, wal_offset);
    plan.collections
        .push(startup_test_collection(CollectionId(7)))
        .unwrap();
    let mut open_transaction = None;
    let aligned_end_offset = wal_offset + 8;

    let step = classify_replay_record(
        &mut plan,
        &mut open_transaction,
        WalReplayPosition {
            chain_index: 0,
            region_index: 0,
            offset: wal_offset,
        },
        aligned_end_offset,
        crate::test_begin_transaction_record(CollectionId(7)),
    )
    .unwrap();

    assert_eq!(
        step,
        ReplayStep::Advance {
            next_offset: aligned_end_offset,
        }
    );
    let transaction = open_transaction.unwrap();
    assert_eq!(
        transaction.transaction_log_id,
        crate::test_transaction_log_id(CollectionId(7))
    );
    assert_eq!(transaction.start.offset, aligned_end_offset);
    assert_eq!(plan.transaction_original_collections.len(), 1);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-028` A transaction log may contain records for any
#[test]
fn requirement_transaction_recovery_observes_only_the_transaction_collection_allocator_records() {
    let mut plan = StartupOpenPlan::<4, 8>::empty();
    let mut transaction = OpenTransactionReplay {
        collection_id: Some(CollectionId(7)),
        transaction_log_id: crate::test_transaction_log_id(CollectionId(7)),
        start: WalReplayPosition {
            chain_index: 0,
            region_index: 0,
            offset: 0,
        },
        commit_seen: false,
    };

    observe_transaction_recovery_record(
        &mut plan,
        &mut transaction,
        WalRecord::AllocBegin {
            collection_id: CollectionId(8),
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    )
    .unwrap();
    observe_transaction_recovery_record(
        &mut plan,
        &mut transaction,
        WalRecord::FreeRegion {
            collection_id: CollectionId(8),
            region_index: 2,
        },
    )
    .unwrap();
    assert!(plan.transaction_allocations.is_empty());
    assert!(plan.transaction_frees.is_empty());

    observe_transaction_recovery_record(
        &mut plan,
        &mut transaction,
        WalRecord::AllocBegin {
            collection_id: CollectionId(7),
            region_index: 1,
            allocation_sequence: 0,
            free_list_head_after: Some(2),
        },
    )
    .unwrap();
    observe_transaction_recovery_record(
        &mut plan,
        &mut transaction,
        WalRecord::FreeRegion {
            collection_id: CollectionId(7),
            region_index: 2,
        },
    )
    .unwrap();
    assert_eq!(plan.transaction_allocations.as_slice(), &[1]);
    assert_eq!(plan.transaction_frees.as_slice(), &[2]);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-029`
#[test]
fn requirement_transaction_marker_ids_must_match_the_open_transaction() {
    assert_eq!(
        ensure_transaction_log_marker_matches(7, 8),
        Err(StartupError::TransactionMismatch {
            expected: CollectionId(7),
            actual: CollectionId(8),
        })
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-013` A WAL record in the current private log tail
#[test]
fn requirement_recovery_record_room_rejects_tail_overflow_and_alloc_without_free_head() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    let wal_recovery_len = encoded_len_for_record::<128>(metadata, WalRecord::WalRecovery);
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 128 - wal_recovery_len + 1);

    assert_eq!(
        recovery_record_has_append_room::<128, 4, _, 8>(
            &mut flash,
            &mut workspace,
            &mut plan,
            WalRecord::WalRecovery,
        ),
        Ok(false)
    );

    plan.last_free_list_head = None;
    plan.wal_append_offset = metadata.wal_record_area_offset().unwrap();
    assert_eq!(
        recovery_record_has_append_room::<128, 4, _, 8>(
            &mut flash,
            &mut workspace,
            &mut plan,
            WalRecord::AllocBegin {
                collection_id: CollectionId(0),
                region_index: 1,
                allocation_sequence: 0,
                free_list_head_after: None,
            },
        ),
        Ok(false)
    );

    plan.wal_append_offset = 128 - wal_recovery_len;
    assert_eq!(
        recovery_record_has_append_room::<128, 4, _, 8>(
            &mut flash,
            &mut workspace,
            &mut plan,
            WalRecord::WalRecovery,
        ),
        Ok(false)
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-013` Appending any WAL record to the current private
#[test]
fn requirement_recovery_record_room_checks_the_next_free_footer_at_exact_tail_end() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let footer_offset = 128 - FreePointerFooter::ENCODED_LEN;
    flash
        .write_region(1, footer_offset, &[0u8; FreePointerFooter::ENCODED_LEN])
        .unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    let wal_recovery_len = encoded_len_for_record::<128>(metadata, WalRecord::WalRecovery);
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 128 - wal_recovery_len);

    assert!(recovery_record_has_append_room::<128, 4, _, 8>(
        &mut flash,
        &mut workspace,
        &mut plan,
        WalRecord::WalRecovery,
    )
    .is_err());
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-009` At an aligned candidate record start in a
#[test]
fn requirement_recovery_record_writers_accept_records_that_end_at_region_boundary() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    let wal_recovery_len = encoded_len_for_record::<128>(metadata, WalRecord::WalRecovery);

    let mut apply_plan =
        startup_plan_with_append_offset::<4>(metadata, 0, 0, 128 - wal_recovery_len);
    write_recovery_record_and_apply::<128, 4, _, 8>(
        &mut flash,
        &mut workspace,
        &mut apply_plan,
        WalRecord::WalRecovery,
    )
    .unwrap();
    assert_eq!(apply_plan.wal_append_offset, 128);

    let mut raw_plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 128 - wal_recovery_len);
    let used = write_recovery_record_raw::<128, 4, _, 8>(
        &mut flash,
        &mut workspace,
        &mut raw_plan,
        WalRecord::WalRecovery,
    )
    .unwrap();
    assert_eq!(used, wal_recovery_len);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-014` Appending the
#[test]
fn requirement_recovery_rotation_start_accepts_only_the_link_reserve_window() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<256>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 0);
    let reserves =
        recovery_rotation_reserves::<256, 4, 8>(&mut workspace, &mut plan, 1, Some(2)).unwrap();

    plan.wal_append_offset = 256 - reserves.alloc_begin_len - reserves.link_reserve;
    let next_region =
        append_recovery_wal_rotation_start::<256, 4, _, 8>(&mut flash, &mut workspace, &mut plan)
            .unwrap();
    assert_eq!(next_region, 1);
    assert_eq!(plan.wal_append_offset, 256 - reserves.link_reserve);
    assert_eq!(plan.ready_region, Some(1));

    let mut too_much_room = startup_plan_with_append_offset::<4>(
        metadata,
        0,
        0,
        256 - reserves.alloc_begin_len - reserves.rotation_reserve,
    );
    assert!(matches!(
        append_recovery_wal_rotation_start::<256, 4, _, 8>(
            &mut flash,
            &mut workspace,
            &mut too_much_room,
        ),
        Err(StartupError::InvalidWalRotationWindow { .. })
    ));

    let mut too_little_room = startup_plan_with_append_offset::<4>(
        metadata,
        0,
        0,
        256 - reserves.alloc_begin_len - reserves.link_reserve + 1,
    );
    assert!(matches!(
        append_recovery_wal_rotation_start::<256, 4, _, 8>(
            &mut flash,
            &mut workspace,
            &mut too_little_room,
        ),
        Err(StartupError::InvalidWalRotationWindow { .. })
    ));
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-011` Let `wal_link_reserve` be the aligned encoded
#[test]
fn requirement_recovery_rotation_bridges_large_windows_before_rotating() {
    const REGION_SIZE: usize = 512;

    let mut flash = MockFlash::<REGION_SIZE, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 0);
    let reserves =
        recovery_rotation_reserves::<REGION_SIZE, 4, 8>(&mut workspace, &mut plan, 1, Some(2))
            .unwrap();
    let recovery_len = encoded_len_for_record::<REGION_SIZE>(metadata, WalRecord::WalRecovery);
    let bridge_len = usize::try_from(metadata.wal_write_granule).unwrap() + recovery_len;
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let bridge_offset = (wal_offset..REGION_SIZE)
        .find(|candidate| {
            let Some(after_alloc) = candidate.checked_add(reserves.alloc_begin_len) else {
                return false;
            };
            let Some(after_bridge) = candidate.checked_add(bridge_len) else {
                return false;
            };
            if after_alloc > REGION_SIZE || after_bridge > REGION_SIZE {
                return false;
            }
            let remaining_after = REGION_SIZE - after_alloc;
            remaining_after >= reserves.rotation_reserve
                && remaining_after
                    .checked_sub(bridge_len)
                    .is_some_and(|after| {
                        after >= reserves.link_reserve && after < reserves.rotation_reserve
                    })
        })
        .expect("bridgeable WAL rotation window");
    plan.wal_append_offset = bridge_offset;

    rotate_recovery_wal_tail::<REGION_SIZE, 4, _, 8>(&mut flash, &mut workspace, &mut plan)
        .unwrap();

    assert_eq!(plan.wal_tail, 1);
    assert_eq!(
        plan.wal_append_offset,
        metadata.wal_record_area_offset().unwrap()
    );
    assert_eq!(plan.max_seen_sequence, 1);
    assert!(!plan.pending_wal_recovery_boundary);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-014` The
#[test]
fn requirement_recovery_rotation_rejects_windows_too_small_for_the_link() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<256>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 0);
    let reserves =
        recovery_rotation_reserves::<256, 4, 8>(&mut workspace, &mut plan, 1, Some(2)).unwrap();
    plan.wal_append_offset = 256 - reserves.alloc_begin_len - reserves.link_reserve + 1;

    assert!(matches!(
        rotate_recovery_wal_tail::<256, 4, _, 8>(&mut flash, &mut workspace, &mut plan),
        Err(StartupError::InvalidWalRotationWindow { .. })
    ));
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-012` Let `wal_rotation_reserve` be the total aligned
#[test]
fn requirement_append_recovery_record_room_rotates_when_tail_lacks_reserve() {
    const REGION_SIZE: usize = 512;

    let mut flash = MockFlash::<REGION_SIZE, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 0);
    let reserves =
        recovery_rotation_reserves::<REGION_SIZE, 4, 8>(&mut workspace, &mut plan, 1, Some(2))
            .unwrap();
    let record = crate::test_rollback_transaction_record(CollectionId(7));
    let record_len = encoded_len_for_record::<REGION_SIZE>(metadata, record);
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let rotation_offset = (wal_offset..REGION_SIZE)
        .find(|candidate| {
            let Some(after_alloc) = candidate.checked_add(reserves.alloc_begin_len) else {
                return false;
            };
            let Some(after_record) = candidate.checked_add(record_len) else {
                return false;
            };
            if after_alloc > REGION_SIZE || after_record > REGION_SIZE {
                return false;
            }
            let remaining_after_alloc = REGION_SIZE - after_alloc;
            let remaining_after_record = REGION_SIZE - after_record;
            remaining_after_alloc >= reserves.link_reserve
                && remaining_after_alloc < reserves.rotation_reserve
                && remaining_after_record < reserves.rotation_reserve
        })
        .expect("rotation window for recovery record");
    plan.wal_append_offset = rotation_offset;

    append_recovery_record_room_with_rotation::<REGION_SIZE, 4, _, 8>(
        &mut flash,
        &mut workspace,
        &mut plan,
        record,
    )
    .unwrap();

    assert_eq!(plan.wal_tail, 1);
    assert_eq!(
        plan.wal_append_offset,
        metadata.wal_record_area_offset().unwrap()
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-008` `wal_recovery`
#[test]
fn requirement_recovery_gap_bridge_writes_invalid_boundary_then_wal_recovery() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    let recovery_len = encoded_len_for_record::<128>(metadata, WalRecord::WalRecovery);
    let granule = usize::try_from(metadata.wal_write_granule).unwrap();
    let bridge_offset = 128 - granule - recovery_len;
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, bridge_offset);

    bridge_recovery_wal_rotation_gap::<128, 4, _, 8>(&mut flash, &mut workspace, &mut plan)
        .unwrap();

    let invalid_byte =
        first_invalid_wal_boundary_byte(metadata.erased_byte, metadata.wal_record_magic);
    assert_eq!(plan.wal_append_offset, 128);
    assert!(plan.pending_wal_recovery_boundary);
    assert_eq!(flash.region_bytes(0).unwrap()[bridge_offset], invalid_byte);
    assert_eq!(
        flash.region_bytes(0).unwrap()[bridge_offset + granule],
        metadata.wal_record_magic
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-007` Every WAL record start offset within a private
#[test]
fn requirement_recovery_gap_bridge_rejects_zero_granule_and_tail_overflow() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    let recovery_len = encoded_len_for_record::<128>(metadata, WalRecord::WalRecovery);
    let granule = usize::try_from(metadata.wal_write_granule).unwrap();
    let mut overflow_plan =
        startup_plan_with_append_offset::<4>(metadata, 0, 0, 128 - granule - recovery_len + 1);
    assert_eq!(
        bridge_recovery_wal_rotation_gap::<128, 4, _, 8>(
            &mut flash,
            &mut workspace,
            &mut overflow_plan,
        ),
        Err(StartupError::LengthOverflow)
    );

    let mut zero_granule_metadata = metadata;
    zero_granule_metadata.wal_write_granule = 0;
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut zero_granule_plan =
        startup_plan_with_append_offset::<4>(zero_granule_metadata, 0, 0, wal_offset);
    assert_eq!(
        bridge_recovery_wal_rotation_gap::<128, 4, _, 8>(
            &mut flash,
            &mut workspace,
            &mut zero_granule_plan,
        ),
        Err(StartupError::LengthOverflow)
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-002` `record_magic` MUST equal the storage's configured
#[test]
fn requirement_first_invalid_wal_boundary_byte_avoids_erased_and_magic_values() {
    assert_eq!(first_invalid_wal_boundary_byte(0, 1), 2);
    assert_ne!(first_invalid_wal_boundary_byte(0xff, 0xa5), 0xff);
    assert_ne!(first_invalid_wal_boundary_byte(0xff, 0xa5), 0xa5);
}

//= spec/ring/07-reclaim.md#free-region
//= type=test
//# `RING-FREE-REGION-001` Establish `region_index` as a free-tail
#[test]
fn requirement_startup_free_pointer_footer_helpers_validate_written_and_decoded_state() {
    let mut flash = MockFlash::<64, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    assert_eq!(
        ensure_free_pointer_footer_unwritten_startup::<64, _>(&mut flash, metadata, 1),
        Err(StartupError::FreeRegionFooterNotUnwritten { region_index: 1 })
    );
    assert_eq!(
        read_free_pointer_successor_startup::<64, 4, _>(&mut flash, metadata, 1),
        Ok(Some(2))
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-033` Transaction-log records after a frozen
#[test]
fn requirement_empty_transaction_replay_interval_does_not_read_past_end_offset() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 128);

    replay_transaction_region_interval::<128, _, 4, 8>(
        &mut flash,
        &mut workspace,
        &mut plan,
        0,
        128,
        128,
        TransactionReplayMode::ApplyFullInterval,
    )
    .unwrap();
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-015` On `free_region(collection_id, region_index)`:
#[test]
fn requirement_open_replay_allocator_record_applies_free_regions() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, wal_offset);
    plan.last_free_list_head = None;

    apply_open_replay_allocator_record(
        &mut plan,
        WalRecord::FreeRegion {
            collection_id: CollectionId(7),
            region_index: 1,
        },
    )
    .unwrap();

    assert_eq!(plan.last_free_list_head, Some(1));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-011` On
#[test]
fn requirement_user_alloc_begin_does_not_conflict_with_reserved_wal_ready_region() {
    let mut collections = heapless::Vec::<StartupCollection, 8>::new();
    let metadata = StorageMetadata {
        storage_version: crate::STORAGE_VERSION,
        region_size: 128,
        region_count: 4,
        min_free_regions: 1,
        transaction_log_count: 0,
        wal_write_granule: 8,
        erased_byte: 0xff,
        wal_record_magic: 0xa5,
    };
    let mut last_free_list_head = Some(2);
    let mut ready_region = Some(1);

    apply_wal_record(
        metadata,
        WalRecord::AllocBegin {
            collection_id: CollectionId(7),
            region_index: 2,
            allocation_sequence: 0,
            free_list_head_after: Some(3),
        },
        &mut collections,
        &mut last_free_list_head,
        &mut ready_region,
    )
    .unwrap();

    assert_eq!(last_free_list_head, Some(3));
    assert_eq!(ready_region, Some(1));
}

//= spec/ring/05-disk-format.md#free-pointer-footer
//= type=test
//# `RING-FREE-006` While a region is allocated for live use, the bytes
#[test]
fn requirement_free_list_head_discovery_ignores_footers_in_non_free_regions() {
    let mut flash = MockFlash::<64, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 9, CollectionId(7), MAP_REGION_V2_FORMAT);
    write_free_pointer_footer(&mut flash, 2, Some(1));

    assert_eq!(
        discover_free_list_head_from_footers(&mut flash, metadata).unwrap(),
        Some(1)
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-029` If replay yields a live collection with unsupported or invalid retained
//# collection data under that collection's normative specification, startup MUST fail before open
//# succeeds and before transaction cleanup frees any region based on collection reachability.
#[test]
fn requirement_storage_open_path_rejects_invalid_retained_map_region_snapshot_and_update_payloads()
{
    {
        let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage = Storage::<_, 512, 5>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        storage.create_map(CollectionId(43)).unwrap();

        let region_index = storage
            .with_runtime_io_workspace(|runtime, flash, workspace| {
                runtime.reserve_next_region::<512, 5, _>(
                    flash,
                    workspace,
                    &mut heapless::Vec::new(),
                    &mut heapless::Vec::new(),
                    &mut crate::storage::WalHeadReclaimPlan::empty(),
                    &mut crate::startup::StartupOpenPlan::empty(),
                )
            })
            .unwrap();
        storage
            .with_runtime_io_workspace(|runtime, flash, workspace| {
                runtime.write_committed_region::<512, 5, _>(
                    flash,
                    workspace,
                    region_index,
                    CollectionId(43),
                    MAP_REGION_V2_FORMAT,
                    &[1, 2, 3],
                )
            })
            .unwrap();
        storage
            .append_head(CollectionId(43), CollectionType::MAP_CODE, region_index)
            .unwrap();

        drop(storage);
        let mut reopened =
            Storage::<_, 512, 5>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4>(
            CollectionId(43),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        );
        assert!(matches!(
            result,
            Err(MapStorageError::UnsupportedRegionFormat {
                collection_id: CollectionId(43),
                region_index: actual_region,
                actual: MAP_REGION_V2_FORMAT,
            }) if actual_region == region_index
        ));
    }

    {
        let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage = Storage::<_, 512, 4>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        storage.create_map(CollectionId(44)).unwrap();
        storage
            .append_snapshot(CollectionId(44), CollectionType::MAP_CODE, &[1])
            .unwrap();

        drop(storage);
        let mut reopened =
            Storage::<_, 512, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4>(
            CollectionId(44),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        );
        assert!(matches!(
            result,
            Err(MapStorageError::Map(MapError::SerializationError))
        ));
    }

    {
        let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage = Storage::<_, 512, 4>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        storage.create_map(CollectionId(45)).unwrap();
        storage.append_update(CollectionId(45), &[0xff]).unwrap();

        drop(storage);
        let mut reopened =
            Storage::<_, 512, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4>(
            CollectionId(45),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        );
        assert!(matches!(
            result,
            Err(MapStorageError::Map(MapError::SerializationError))
        ));
    }
}
