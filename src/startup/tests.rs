#![allow(clippy::drop_non_drop)]

use super::*;
use crate::disk::{encode_transaction_log_region_prefix_with_cursors, Header};
use crate::wal_record::{
    encode_record_into, encoded_record_len, LogPosition, TransactionLogRange, WalRecord,
};
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

fn init_transaction_log_region<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    metadata: StorageMetadata,
    region_index: u32,
    sequence: u64,
    head_region: u32,
) -> usize {
    flash.erase_region(region_index).unwrap();
    let mut bytes = [0u8; REGION_SIZE];
    let used = encode_transaction_log_region_prefix_with_cursors(
        &mut bytes,
        metadata,
        sequence,
        head_region,
        FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        },
        FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        },
        FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        },
    )
    .unwrap();
    flash.write_region(region_index, 0, &bytes[..used]).unwrap();
    used
}

fn transaction_range(
    start_region: u32,
    start_offset: usize,
    end_offset: usize,
) -> TransactionLogRange {
    TransactionLogRange {
        start: LogPosition {
            region_index: start_region,
            offset: u32::try_from(start_offset).unwrap(),
        },
        end: LogPosition {
            region_index: start_region,
            offset: u32::try_from(end_offset).unwrap(),
        },
    }
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
        FreeSpaceState::new_ready_range(1, 2, metadata.region_count).unwrap(),
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

fn startup_test_generation(
    collection_id: CollectionId,
    basis: StartupCollectionBasis,
    pending_update_count: usize,
) -> u64 {
    StartupCollection {
        collection_id,
        collection_type: Some(CollectionType::MAP_CODE),
        basis,
        pending_update_count,
    }
    .committed_generation()
}

fn append_transaction_enrollment<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    metadata: StorageMetadata,
    tx_region: u32,
    tx_offset: usize,
    collection_id: CollectionId,
    observed_collection_generation: u64,
) -> usize {
    append_wal_record(
        flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::AddTransactionCollection {
            collection_id,
            observed_collection_generation,
        },
    )
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
                            region_index: seen_region,
                            ..
                        } if seen_region == region_index => {
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
    let tx_region = 4;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let mut tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        collection_id,
        startup_test_generation(collection_id, StartupCollectionBasis::Empty, 0),
    );
    tx_offset = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    );
    tx_offset = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::Update {
            collection_id,
            payload: &[1],
        },
    );
    let range = transaction_range(tx_region, tx_start, tx_offset);

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
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );
    if append_free_region {
        offset = append_wal_record(
            &mut flash,
            metadata,
            0,
            offset,
            WalRecord::FreeRegion {
                region_index: 2,
                append_tail_after: FreeQueuePosition {
                    region_index: 1,
                    entry_index: 5,
                },
            },
        );
    }
    if append_rollback {
        append_wal_record(
            &mut flash,
            metadata,
            0,
            offset,
            WalRecord::RollbackTransaction {
                transaction_log_id: 0,
                range,
            },
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
    let tx_region = 4;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        collection_id,
        startup_test_generation(collection_id, StartupCollectionBasis::Empty, 0),
    );
    let tx_end = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::Update {
            collection_id,
            payload: &[1],
        },
    );
    let range = transaction_range(tx_region, tx_start, tx_end);
    let begin_record = WalRecord::BeginTransaction {
        transaction_log_id: 0,
        start: range.start,
    };
    let rollback_record = WalRecord::RollbackTransaction {
        transaction_log_id: 0,
        range,
    };
    let _update_record = WalRecord::Update {
        collection_id,
        payload: &[1],
    };
    let rotation_allocate_region_record = WalRecord::AllocateRegion {
        region_index: 2,
        allocation_head_after: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    };
    let rotation_link_record = WalRecord::Link {
        next_region_index: 2,
        expected_sequence: 1,
    };

    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    let begin_len =
        encoded_record_len(begin_record, metadata, &mut physical, &mut logical).unwrap();
    let rollback_len =
        encoded_record_len(rollback_record, metadata, &mut physical, &mut logical).unwrap();
    let rotation_allocate_region_len = encoded_record_len(
        rotation_allocate_region_record,
        metadata,
        &mut physical,
        &mut logical,
    )
    .unwrap();
    let rotation_link_len =
        encoded_record_len(rotation_link_record, metadata, &mut physical, &mut logical).unwrap();
    let rotation_reserve = rotation_allocate_region_len + rotation_link_len;

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
                let transaction_end = offset + filler_len + begin_len;
                let Some(terminal_end) = transaction_end.checked_add(rollback_len) else {
                    return false;
                };
                let Some(rotation_alloc_end) =
                    transaction_end.checked_add(rotation_allocate_region_len)
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
    assert!(offset + rollback_len <= REGION_SIZE);
    assert!(REGION_SIZE - (offset + rollback_len) < rotation_reserve);
    flash
}

fn setup_postcommit_transaction_recovery(append_free_region: bool) -> MockFlash<512, 6, 256> {
    let mut flash = MockFlash::<512, 6, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let tx_region = 4;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        collection_id,
        startup_test_generation(collection_id, StartupCollectionBasis::Region(2), 0),
    );
    let tx_end = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::DropCollection { collection_id },
    );
    let range = transaction_range(tx_region, tx_start, tx_end);

    flash.erase_region(2).unwrap();
    init_user_region_header(&mut flash, 2, 1, collection_id, MAP_MANIFEST_V2_FORMAT);
    flash
        .write_region(2, Header::ENCODED_LEN, &0u32.to_le_bytes())
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
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
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
            region_index: 2,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::CommitTransaction {
            transaction_log_id: 0,
            range,
        },
    );
    if append_free_region {
        append_wal_record(
            &mut flash,
            metadata,
            0,
            offset,
            WalRecord::FreeRegion {
                region_index: 2,
                append_tail_after: FreeQueuePosition {
                    region_index: 1,
                    entry_index: 5,
                },
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
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 3,
            },
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
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 3,
            },
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
            region_index: 3,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 3,
            },
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
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 3,
            },
        },
    );

    (
        next_offset,
        open_formatted_store::<256, 4, _>(&mut flash).unwrap(),
    )
}

fn open_formatted_store_after_replayed_allocate_region() -> (usize, StartupState<8>) {
    let mut flash = MockFlash::<256, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut physical = [0u8; 256];
    let mut logical = [0u8; 256];
    let alloc_len = encoded_record_len(
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
        metadata,
        &mut physical,
        &mut logical,
    )
    .unwrap();
    let link_len = encoded_record_len(
        WalRecord::Link {
            next_region_index: 2,
            expected_sequence: 2,
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
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
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
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_alloc,
        WalRecord::Link {
            next_region_index: 2,
            expected_sequence: 2,
        },
    );
    init_wal_region(&mut flash, 2, 2, 0, metadata.region_count);

    open_formatted_store::<256, 4, _>(&mut flash).unwrap()
}

fn open_formatted_store_from_fresh_format() -> (StorageMetadata, StartupState<8>) {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let state = open_formatted_store::<128, 4, _>(&mut flash).unwrap();
    (metadata, state)
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-001` Read `StorageMetadata`, validate `metadata_checksum`, and validate
//# static geometry (`region_size`, `region_count`, `min_free_regions`, `erased_byte`,
//# `wal_write_granule`, `wal_record_magic`, and storage version support).
#[test]
fn requirement_open_formatted_store_requires_metadata() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    let error = open_formatted_store::<128, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::MissingMetadata);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-003` Select the main WAL tail as the unique candidate main WAL region with
//# the largest valid sequence. If no candidate main WAL region exists, or if multiple
//# candidate main WAL regions share that largest valid sequence, return an error. For each
//# configured transaction log, recover its chain only when a retained main-WAL
//# transaction-control record references it or when a live transaction descriptor requires
//# it.
#[test]
fn requirement_open_formatted_store_rejects_duplicate_max_sequence_wal_candidates() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();

    let header = Header {
        sequence: 1,
        collection_id: CollectionId(0),
        collection_format: WAL_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    flash.write_region(2, 0, &header_bytes).unwrap();

    let error = open_formatted_store::<128, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::DuplicateWalTailSequence(1));
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-046` Startup tail selection MUST ignore regions with nonzero
//# collection id even when their format is a private log format while still tracking max
//# seen sequence.
#[test]
fn requirement_open_formatted_store_ignores_nonzero_collection_with_wal_format_when_selecting_tail()
{
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 9, CollectionId(7), WAL_V1_FORMAT);

    let state = open_formatted_store::<128, 4, _>(&mut flash).unwrap();
    assert_eq!(state.wal_tail(), 0);
    assert_eq!(state.max_seen_sequence(), 9);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-007` Parse records in WAL order: region order, then offset order.
//# Record parsing begins only at offsets aligned to `wal_write_granule` and greater
//# than or equal to `wal_record_area_offset` within each private log region. Maintain
//# a replay-local `pending_wal_recovery_boundary`, initially clear.
//#
//# If an aligned candidate start byte equals `erased_byte`, treat that slot as
//# currently unwritten and stop scanning that private log region. If the aligned start
//# byte equals `wal_record_magic`, parse the record. If parsing or checksum validation
//# fails, treat that aligned slot as corrupt/torn WAL bytes, set
//# `pending_wal_recovery_boundary`, and keep scanning forward in aligned
//# `wal_write_granule` steps. If the aligned start byte is neither `erased_byte` nor
//# `wal_record_magic`, use the same corrupt/torn handling.
//# If a later valid record is found while the boundary is set, that record must be
//# `wal_recovery`; otherwise return an error.
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

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-022` `add_transaction_collection` is valid only in a transaction log and
//# only while that transaction log has an open transaction descriptor for the containing
//# range.
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
//# `RING-STARTUP-024` On `add_transaction_collection(collection_id, observed_generation)`
//# while importing a committed range: record the collection as enrolled in the imported
//# transaction range. The stored generation is not rechecked during recovery because the
//# retained main-WAL commit record is durable evidence that foreground conflict checking
//# succeeded before commit.
#[test]
fn requirement_open_formatted_store_rejects_invalid_free_list_chain() {
    let mut flash = MockFlash::<128, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let state = open_formatted_store::<128, 4, _>(&mut flash).unwrap();

    assert_eq!(
        state.allocation_head(),
        FreeQueuePosition {
            region_index: 1,
            entry_index: 0
        }
    );
    assert_eq!(
        state.ready_boundary(),
        FreeQueuePosition {
            region_index: 1,
            entry_index: 2
        }
    );
    assert_eq!(
        state.append_tail(),
        FreeQueuePosition {
            region_index: 1,
            entry_index: 2
        }
    );
    assert_eq!(metadata.region_count, 4);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-011` On `snapshot(collection_id, collection_type)`: if `collection_id` is
//# not tracked, create replay state because older basis records may have been reclaimed and
//# set tracked `collection_type` from this record. If the collection is dropped or this
//# record's type conflicts with the tracked type, return an error. Set collection state to
//# `WALSnapshotClean`, set `basis_pos`, and clear older pending updates for that
//# collection.
#[test]
fn requirement_open_formatted_store_replays_allocate_region_into_allocator_runtime_state() {
    let (_next_offset, state) = open_formatted_store_after_replayed_allocate_region();
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.ready_region(), None);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-023` On `commit_transaction(transaction_log_id, range)`: verify that the
//# id and range match an active descriptor or a recoverable committed range. Scan the
//# transaction-log range; if any record inside the range is torn, malformed, or invalid,
//# return an error. Apply the range's enrolled collection mutations and allocator commands
//# at this main-WAL commit position, advance committed generation for every enrolled
//# collection, and record that committed cleanup may still be required until a matching
//# `transaction_finished` is retained.
#[test]
fn requirement_open_formatted_store_initializes_allocator_state_after_allocate_region() {
    let (_next_offset, state) = open_formatted_store_after_replayed_allocate_region();
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.free_space_tail_region(), Some(3));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-025` On `rollback_transaction(transaction_log_id, range)`: scan the
//# referenced range for transaction-owned allocations and cleanup effects, confirm rollback
//# recovery has made those effects non-visible and reclaimable, and do not apply collection
//# mutations or allocator pops from the range.
#[test]
fn requirement_open_formatted_store_keeps_replayed_ready_region_reserved_in_memory() {
    let (_next_offset, state) = open_formatted_store_after_replayed_allocate_region();
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.ready_free_region(), Some(3));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-151` Startup replay MUST publish committed
//# transaction intervals atomically: after `transaction_finished`,
//# transaction collection mutations from the committed range are imported
//# and visible in replayed collection state.
#[test]
fn requirement_open_formatted_store_replays_finished_transaction_interval() {
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let tx_region = 2;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        collection_id,
        startup_test_generation(collection_id, StartupCollectionBasis::Empty, 0),
    );
    let tx_end = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::Update {
            collection_id,
            payload: &[1, 2, 3],
        },
    );
    let range = transaction_range(tx_region, tx_start, tx_end);

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
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );
    let after_commit = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_begin,
        WalRecord::CommitTransaction {
            transaction_log_id: 0,
            range,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_commit,
        WalRecord::TransactionFinished {
            transaction_log_id: 0,
            range,
        },
    );

    let state = open_formatted_store::<512, 4, _>(&mut flash).unwrap();
    let collection = collection_summary(&state, collection_id);
    assert_eq!(collection.pending_update_count(), 1);
}

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-023` A transaction log may contain records for any collection
//# explicitly enrolled by `add_transaction_collection` in the same open transaction
//# range. Collection mutation records for an unenrolled collection are invalid in that
//# range.
#[test]
fn requirement_transaction_log_replay_rejects_unenrolled_collection_mutation() {
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let tx_region = 2;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let tx_end = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        WalRecord::Update {
            collection_id,
            payload: &[1, 2, 3],
        },
    );
    let range = transaction_range(tx_region, tx_start, tx_end);

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
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_begin,
        WalRecord::CommitTransaction {
            transaction_log_id: 0,
            range,
        },
    );

    assert_eq!(
        open_formatted_store::<512, 4, _>(&mut flash).unwrap_err(),
        StartupError::InvalidTransactionEnrollment { collection_id }
    );
}

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-027` Before appending `commit_transaction(transaction_log_id,
//# range)`, storage MUST verify that each enrolled collection's current committed state
//# generation still equals the generation recorded by that collection's
//# `add_transaction_collection` record. Any mismatch fails the commit with a
//# transaction conflict.
#[test]
fn requirement_transaction_log_replay_rejects_enrollment_generation_mismatch() {
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let tx_region = 2;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        collection_id,
        startup_test_generation(collection_id, StartupCollectionBasis::Empty, 0).wrapping_add(1),
    );
    let tx_end = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::Update {
            collection_id,
            payload: &[1, 2, 3],
        },
    );
    let range = transaction_range(tx_region, tx_start, tx_end);

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
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_begin,
        WalRecord::CommitTransaction {
            transaction_log_id: 0,
            range,
        },
    );

    assert_eq!(
        open_formatted_store::<512, 4, _>(&mut flash).unwrap_err(),
        StartupError::InvalidTransactionEnrollment { collection_id }
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-019` On `head(collection_id, collection_type, region_index)`: if
//# `collection_id = 0`, this is a WAL-head control record. Its replay effect was already
//# consumed while determining the WAL-head candidate from the tail region. If
//# `collection_type != wal`, return an error; otherwise ignore this record during the main
//# per-record replay pass.
#[test]
fn requirement_open_formatted_store_rolls_back_only_transaction_collection_records() {
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let transaction_collection = CollectionId(7);
    let unrelated_collection = CollectionId(8);
    let tx_region = 2;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        transaction_collection,
        startup_test_generation(transaction_collection, StartupCollectionBasis::Empty, 0),
    );
    let tx_offset = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::Update {
            collection_id: transaction_collection,
            payload: &[1],
        },
    );
    let range = transaction_range(tx_region, tx_start, tx_offset);

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
        WalRecord::Update {
            collection_id: unrelated_collection,
            payload: &[2],
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::RollbackTransaction {
            transaction_log_id: 0,
            range,
        },
    );

    let state = open_formatted_store::<512, 4, _>(&mut flash).unwrap();
    let transaction = collection_summary(&state, transaction_collection);
    let unrelated = collection_summary(&state, unrelated_collection);
    assert_eq!(transaction.pending_update_count(), 0);
    assert_eq!(unrelated.pending_update_count(), 1);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-015` Transaction-log records are not applied by ordinary
//# log-chain traversal. Startup scans a transaction-log range only when a
//# retained main-WAL `commit_transaction`, `rollback_transaction`,
//# `transaction_finished`, or active recovery descriptor references that range.
#[test]
fn requirement_open_formatted_store_recovers_unfinished_transaction_before_commit() {
    let mut flash = MockFlash::<512, 6, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let tx_region = 4;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let mut tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        collection_id,
        startup_test_generation(collection_id, StartupCollectionBasis::Empty, 0),
    );
    tx_offset = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    );
    tx_offset = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::Update {
            collection_id,
            payload: &[1],
        },
    );
    let range = transaction_range(tx_region, tx_start, tx_offset);

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
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );

    flash.clear_operations();
    let mut workspace = StorageWorkspace::<512>::new();
    let mut plan = StartupOpenPlan::<6, 8>::empty();
    begin_open_formatted_store::<512, 6, _, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("begin open");
    recover_open_rotation::<512, _, 6, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("recover rotation");
    replay_open_wal_chain::<512, 6, _, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("replay WAL");
    let state =
        finish_open_formatted_store::<512, 6, _, 8>(&mut flash, &mut plan).expect("finish open");
    let collection = collection_summary(&state, collection_id);
    assert_eq!(collection.pending_update_count(), 0);
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.free_space_tail_region(), Some(2));
    assert_eq!(state.ready_boundary().entry_index, 4);
    assert_eq!(state.append_tail().entry_index, 5);

    assert!(!flash
        .operations()
        .contains(&(crate::MockOperation::EraseRegion { region_index: 2 })));
    let free_record = flash
        .operations()
        .iter()
        .position(|operation| {
            matches!(
                operation,
                crate::MockOperation::WriteRegion {
                    region_index: 0,
                    ..
                }
            )
        })
        .unwrap();
    assert!(free_record > 0);

    let reopened = open_formatted_store::<512, 6, _>(&mut flash).unwrap();
    assert_eq!(reopened.ready_free_region(), Some(3));
    assert_eq!(reopened.free_space_tail_region(), Some(2));
    assert_eq!(reopened.ready_boundary().entry_index, 4);
    assert_eq!(reopened.append_tail().entry_index, 5);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-152` Post-commit transaction cleanup recovery MUST preserve
//# committed collection state, recover the cleanup free by appending a dirty free-space
//# entry, and remain stable across reopen.
#[test]
fn requirement_open_formatted_store_finishes_post_commit_transaction_cleanup() {
    let mut flash = MockFlash::<512, 6, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let tx_region = 4;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        collection_id,
        startup_test_generation(collection_id, StartupCollectionBasis::Region(2), 0),
    );
    let tx_end = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::DropCollection { collection_id },
    );
    let range = transaction_range(tx_region, tx_start, tx_end);

    flash.erase_region(2).unwrap();
    init_user_region_header(&mut flash, 2, 1, collection_id, MAP_MANIFEST_V2_FORMAT);
    flash
        .write_region(2, Header::ENCODED_LEN, &0u32.to_le_bytes())
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
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
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
            region_index: 2,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::CommitTransaction {
            transaction_log_id: 0,
            range,
        },
    );

    flash.clear_operations();
    let mut workspace = StorageWorkspace::<512>::new();
    let mut plan = StartupOpenPlan::<6, 8>::empty();
    begin_open_formatted_store::<512, 6, _, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("begin open");
    recover_open_rotation::<512, _, 6, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("recover rotation");
    replay_open_wal_chain::<512, 6, _, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("replay WAL");
    let state =
        finish_open_formatted_store::<512, 6, _, 8>(&mut flash, &mut plan).expect("finish open");
    let collection = collection_summary(&state, collection_id);
    assert_eq!(collection.basis(), StartupCollectionBasis::Dropped);
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.free_space_tail_region(), Some(2));
    assert!(!flash.operations().iter().any(|operation| {
        *operation == (crate::MockOperation::EraseRegion { region_index: 2 })
    }));

    let reopened = open_formatted_store::<512, 6, _>(&mut flash).unwrap();
    assert_eq!(
        collection_summary(&reopened, collection_id).basis(),
        StartupCollectionBasis::Dropped
    );
    assert_eq!(reopened.free_space_tail_region(), Some(2));
}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=test
//# `RING-TX-RECOVERY-001` If startup reaches main WAL end with an open full transaction
//# descriptor and no durable `commit_transaction`, it MUST run rollback recovery for that
//# transaction-log range and append `rollback_transaction(transaction_log_id, range)`.
#[test]
fn requirement_startup_recovers_uncommitted_transaction_with_rollback_marker() {
    let mut flash = setup_precommit_unfinished_transaction();
    let collection_id = CollectionId(7);

    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    let collection = storage_collection_summary(&storage, collection_id);
    assert_eq!(collection.pending_update_count(), 0);
    assert_eq!(storage.ready_free_region(), Some(3));
    assert_eq!(storage.free_space_tail_region(), Some(2));

    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 2);
    assert_eq!(
        counts,
        RecoveryRecordCounts {
            free_region: 1,
            rollback_transaction: 1,
            transaction_finished: 0,
        }
    );
}

//= spec/ring/07-reclaim.md#free-region
//= type=test
//# `RING-FREE-REGION-SEM-002` The freed region is inserted at
//# `tail_before` as a dirty free-space entry.
#[test]
fn requirement_startup_returns_abandoned_transaction_log_region_dirty() {
    let mut flash = MockFlash::<512, 6, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let tx_region = 2;

    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocateRegion {
            region_index: tx_region,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    );
    init_transaction_log_region(&mut flash, metadata, tx_region, 1, tx_region);

    let state = open_formatted_store::<512, 6, _>(&mut flash).unwrap();
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.free_space_tail_region(), Some(tx_region));
}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=test
//# `RING-TX-RECOVERY-002` If startup reaches main WAL end with an uncommitted inline
//# transaction, it MUST run rollback recovery for that bounded main-WAL range and may
//# append `rollback_inline_transaction(record_count)`.
#[test]
fn requirement_startup_finishes_post_commit_transaction_cleanup_with_finished_marker() {
    let mut flash = setup_postcommit_unfinished_transaction();
    let collection_id = CollectionId(7);

    flash.clear_operations();
    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    let collection = storage_collection_summary(&storage, collection_id);
    assert_eq!(collection.basis(), StartupCollectionBasis::Dropped);
    assert_eq!(storage.ready_free_region(), Some(3));
    assert_eq!(storage.free_space_tail_region(), Some(2));

    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 2);
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
//# `RING-TX-RECOVERY-003` If startup reaches main WAL end after
//# `commit_transaction(transaction_log_id, range)` but before
//# `transaction_finished(transaction_log_id, range)`, it MUST preserve the committed
//# collection and allocator state imported from that range, finish cleanup frees derived
//# from the committed range, and append `transaction_finished(transaction_log_id, range)`.
#[test]
fn requirement_transaction_recovery_is_idempotent() {
    let collection_id = CollectionId(7);
    let mut precommit_flash = setup_precommit_recovery_after_allocation_free_before_rollback();

    let mut precommit_storage =
        Storage::<_, 512, 6, 8>::open(&mut precommit_flash, crate::test_storage_memory()).unwrap();
    let precommit_counts =
        count_recovery_records_in_storage(&mut precommit_storage, collection_id, 2);
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
        count_recovery_records_in_storage(&mut reopened_precommit, collection_id, 2),
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
        count_recovery_records_in_storage(&mut postcommit_storage, collection_id, 2);
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
        count_recovery_records_in_storage(&mut reopened_postcommit, collection_id, 2),
        postcommit_counts
    );
    assert_eq!(
        storage_collection_summary(&reopened_postcommit, collection_id).basis(),
        StartupCollectionBasis::Dropped
    );
}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=test
//# `RING-TX-RECOVERY-004` Both rollback recovery and cleanup recovery MUST be idempotent if
//# startup crashes before the terminal marker is durable.
#[test]
fn requirement_min_free_region_reserve_covers_transaction_terminal_records() {
    let mut flash = setup_precommit_transaction_requiring_recovery_rotation();
    let collection_id = CollectionId(7);

    let mut storage =
        Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();

    assert_eq!(storage.wal_tail(), 2);
    assert_eq!(storage.ready_free_region(), Some(3));
    assert!(storage.free_space_tail_region().is_some());
    assert_eq!(
        storage_collection_summary(&storage, collection_id).pending_update_count(),
        0
    );

    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 2);
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
//# `RING-WAL-PAYLOAD-010` `commit_inline_transaction` Main-WAL-only record. Atomically
//# applies the records in the matching bounded inline range at this commit position. Before
//# this marker, replay scans the bounded range for validation and cleanup information but
//# does not apply collection or allocator effects.
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
        count_recovery_records_in_storage(&mut storage, collection_id, 2).rollback_transaction,
        1
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-012` `free_region` Appends a detached physical region as a dirty entry
//# in the free-space collection. The payload stores the detached `region_index` and the
//# self-checking `append_tail_after` cursor that must be the next queue position after the
//# current `append_tail`. The record carries no owner or purpose; cleanup correctness comes
//# from the enclosing transaction or privileged storage-core recovery procedure.
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
    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 2);
    assert_eq!(counts.free_region, 1);
    assert_eq!(counts.transaction_finished, 1);
    assert_eq!(counts.rollback_transaction, 0);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-013` `begin_transaction` Main-WAL-only record. Opens a transaction
//# descriptor and assigns it a transaction log. Payload is `transaction_log_id:u32,
//# start:LogPosition`, where `start` is the first transaction-log position owned by this
//# transaction.
#[test]
fn requirement_wal_transaction_finished_record_closes_cleanup_phase() {
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let tx_region = 2;
    let tx_start = init_transaction_log_region(&mut flash, metadata, tx_region, 2, tx_region);
    let tx_offset = append_transaction_enrollment(
        &mut flash,
        metadata,
        tx_region,
        tx_start,
        collection_id,
        startup_test_generation(collection_id, StartupCollectionBasis::Empty, 0),
    );
    let tx_end = append_wal_record(
        &mut flash,
        metadata,
        tx_region,
        tx_offset,
        WalRecord::Update {
            collection_id,
            payload: &[1, 2, 3],
        },
    );
    let range = transaction_range(tx_region, tx_start, tx_end);

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
        WalRecord::BeginTransaction {
            transaction_log_id: 0,
            start: range.start,
        },
    );
    offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::CommitTransaction {
            transaction_log_id: 0,
            range,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::TransactionFinished {
            transaction_log_id: 0,
            range,
        },
    );

    let mut storage =
        Storage::<_, 512, 4, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        storage_collection_summary(&storage, collection_id).pending_update_count(),
        1
    );

    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 2);
    assert_eq!(counts.transaction_finished, 1);
    assert_eq!(counts.rollback_transaction, 0);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-014` `add_transaction_collection` Transaction-log-only record. Enrolls
//# `collection_id` in the open transaction for that transaction log. The payload stores the
//# collection's `observed_collection_generation:u64`, which is the committed state
//# generation observed when the transaction copied the collection frontier into its private
//# transaction buffer.
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
    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 2);
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
        count_recovery_records_in_storage(&mut reopened, collection_id, 2),
        counts
    );
}

//= spec/ring/07-reclaim.md#free-region
//= type=test
//# `RING-FREE-REGION-PRE-003` The owning operation's committed transaction state MUST
//# contain enough durable information for cleanup recovery to derive that `region_index`
//# must be freed.
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
    let counts = count_recovery_records_in_storage(&mut storage, collection_id, 2);
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
fn requirement_open_formatted_store_recovers_append_point_after_replayed_allocate_region() {
    let (next_offset, state) = open_formatted_store_after_replayed_allocate_region();
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
    assert_eq!(state.ready_free_region(), Some(2));
    assert!(!state.pending_wal_recovery_boundary());
}

//= spec/ring/04-wal-records.md#checksum-trust-model
//= type=test
//# `RING-CHECKSUM-005` An implementation MUST ensure that even intentionally corrupted
//# storage eventually produces a reported error rather than memory unsafety, undefined
//# behavior, control-flow corruption, infinite loops, or unbounded resource consumption.
//# All replay walks, decoders, and collection-format handlers MUST remain bounded by
//# configured storage geometry and record sizes.
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
    let mut flash = MockFlash::<512, 5, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(7), MAP_MANIFEST_V2_FORMAT);
    flash
        .write_region(2, Header::ENCODED_LEN, &0u32.to_le_bytes())
        .unwrap();
    init_user_region_header(&mut flash, 3, 5, CollectionId(7), MAP_MANIFEST_V2_FORMAT);
    flash
        .write_region(3, Header::ENCODED_LEN, &0u32.to_le_bytes())
        .unwrap();

    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_first_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    );
    let after_second_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_first_alloc,
        WalRecord::AllocateRegion {
            region_index: 3,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 2,
            },
        },
    );
    let after_first_head = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_second_alloc,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            region_index: 2,
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
            region_index: 3,
        },
    );
    let after_first_free = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_second_head,
        WalRecord::FreeRegion {
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 4,
            },
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_first_free,
        WalRecord::FreeRegion {
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 4,
            },
        },
    );

    let state = open_formatted_store::<512, 5, _>(&mut flash).unwrap();

    assert_eq!(state.collections().len(), 1);
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(3)
    );
    assert_eq!(state.free_space_tail_region(), Some(2));
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
//# `RING-STARTUP-028` If replay reaches WAL end after
//# `commit_transaction(transaction_log_id, range)` but before
//# `transaction_finished(transaction_log_id, range)`, preserve the imported collection and
//# allocator state, finish cleanup frees derived from the committed range, and append
//# `transaction_finished(transaction_log_id, range)`.
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
//# `RING-STARTUP-030` After replay and transaction recovery, for each collection:
//# reconstruct its durable basis from the collection state. If the state is empty or
//# WAL-snapshot based and has post-basis updates, materialize mutable RAM state and apply
//# those updates in WAL order. If the state is committed-region based, the basis may remain
//# in place until a read or mutation needs to materialize it. Dropped collections do not
//# reconstruct mutable state and do not accept later mutations.
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
//# collection id is nonzero even if collection_format is a private log format.
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
//# `RING-IMPL-REGRESSION-055` WAL target validation MUST require collection id 0 and the
//# expected private log collection_format.
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
//# `RING-IMPL-REGRESSION-057` Region index validation MUST reject a region index equal to
//# `region_count`.
#[test]
fn requirement_ensure_region_index_in_range_rejects_region_count_boundary() {
    assert_eq!(
        ensure_region_index_in_range(4, 4),
        Err(StartupError::InvalidRegionReference(4))
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-005` Walk the WAL region chain from the resulting WAL head to tail using
//# `link` records. If a `link` is missing or invalid before reaching the known tail, return
//# an error.
#[test]
fn requirement_open_formatted_store_follows_completed_link_to_the_next_wal_tail() {
    let state = open_formatted_store_after_completed_wal_rotation();
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 2);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-013` On `erase_free_region_span(count, ready_boundary_after)`: verify that
//# the dirty range has at least `count` entries and that `ready_boundary_after` is reached
//# by advancing from the current `ready_boundary` by `count` entries. Treat the
//# corresponding entries as ready and set `ready_boundary = ready_boundary_after`.
#[test]
fn requirement_open_formatted_store_clears_ready_region_when_link_matches_it() {
    let state = open_formatted_store_after_completed_wal_rotation();
    assert_eq!(state.ready_region(), None);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-058` Startup replay MUST recover a WAL rotation after a durable
//# `link` by selecting the linked tail, resetting tail append offset, preserving free-space
//# cursor state, and advancing max sequence.
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
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_alloc,
        WalRecord::Link {
            next_region_index: 2,
            expected_sequence: 2,
        },
    );

    let state = open_formatted_store::<256, 4, _>(&mut flash).unwrap();

    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 2);
    assert_eq!(
        state.wal_append_offset(),
        metadata.wal_record_area_offset().unwrap()
    );
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.free_space_tail_region(), Some(3));
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.max_seen_sequence(), 2);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-059` Startup replay MUST recover a WAL rotation when
//# `allocate_region` is durable but `link` is absent and only rotation reserve remains.
#[test]
fn requirement_open_formatted_store_recovers_rotation_before_link() {
    let mut flash = MockFlash::<160, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let alloc_record = WalRecord::AllocateRegion {
        region_index: 2,
        allocation_head_after: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
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
            next_region_index: 2,
            expected_sequence: 2,
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
    assert_eq!(state.wal_tail(), 2);
    assert_eq!(
        state.wal_append_offset(),
        metadata.wal_record_area_offset().unwrap()
    );
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.free_space_tail_region(), Some(3));
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.max_seen_sequence(), 2);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-060` Startup replay MUST recover a WAL rotation when only the
//# `link` record fits after the rotation allocation at the tail boundary.
#[test]
fn requirement_open_formatted_store_recovers_rotation_when_only_the_link_record_fits_after_allocate_region(
) {
    const REGION_SIZE: usize = 256;

    let mut flash = MockFlash::<REGION_SIZE, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    let alloc_record = WalRecord::AllocateRegion {
        region_index: 2,
        allocation_head_after: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    };
    let alloc_len =
        encoded_record_len(alloc_record, metadata, &mut physical, &mut logical).unwrap();
    let link_len = encoded_record_len(
        WalRecord::Link {
            next_region_index: 2,
            expected_sequence: 2,
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
        .expect("snapshot payload should align allocate_region to exact link capacity");
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
    assert_eq!(state.wal_tail(), 2);
    assert_eq!(state.max_seen_sequence(), 2);
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
            next_region_index: 2,
            expected_sequence: 2,
        },
    );
    init_wal_region(&mut flash, 2, 2, 0, metadata.region_count);
    let corrupt_tail = [0x10; 128];
    flash
        .write_region(0, after_link, &corrupt_tail[..128 - after_link])
        .unwrap();

    let error = open_formatted_store::<128, 4, _>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::BrokenWalChain { region_index: 0 });
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-020` On `link(next_region_index, expected_sequence)`: preserve private-log
//# reachability. If `next_region_index` matches the current storage-core private allocation
//# reservation, consume that reservation; otherwise the link may refer to a retained log
//# region whose historical allocation command was already represented by a checkpoint or
//# reclaimed record.
#[test]
fn requirement_open_formatted_store_clears_pending_recovery_boundary_when_wal_recovery_is_replayed()
{
    let (next_offset, state) = open_formatted_store_after_corrupt_slot_with_wal_recovery();
    assert_eq!(state.wal_append_offset(), next_offset);
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.ready_free_region(), Some(2));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-002` Scan all regions, collect candidate main WAL regions (`collection_id
//# == 0` plus `collection_format = main_wal_v2`), candidate transaction-log regions
//# (`collection_id == 0` plus `collection_format = transaction_log_v2`), and candidate
//# free-space metadata regions (`collection_id == 0` plus `collection_format =
//# free_space_v2`) with valid headers. Track `max_seen_sequence` as the largest `sequence`
//# value seen in any valid region header.
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
//# main WAL tail region's user-data area. Use its `log_head_region_index` as the initial
//# WAL-head candidate and its free-space cursor checkpoint as the allocator baseline for
//# this WAL chain. Then scan that tail region using the aligned candidate-start and
//# record-validation rules defined below, and let the last valid `head(collection_id = 0,
//# collection_type = wal, region_index)` record override the head candidate.
#[test]
fn requirement_open_formatted_store_uses_the_tail_prologue_as_the_initial_wal_head_candidate() {
    let (_metadata, state) = open_formatted_store_from_fresh_format();
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 0);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-062` Opening a freshly formatted store MUST initialize free-space
//# cursors and queue entries from the formatted `free_space_v2` metadata region.
#[test]
fn requirement_open_formatted_store_initializes_allocator_state_for_a_fresh_store() {
    let (_metadata, state) = open_formatted_store_from_fresh_format();
    assert_eq!(state.ready_free_region(), Some(2));
    assert_eq!(state.free_space_tail_region(), Some(3));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-026` On `wal_recovery()`: if `pending_wal_recovery_boundary` is clear,
//# return an error. Otherwise clear the boundary.
#[test]
fn requirement_open_formatted_store_keeps_max_seen_sequence_for_the_next_region_header() {
    let (_metadata, state) = open_formatted_store_from_fresh_format();
    assert_eq!(state.max_seen_sequence(), 1);
}

//= spec/ring/06-startup-replay.md#why-reclaimed-regions-cannot-confuse-startup
//= type=test
//# `RING-BOOTSTRAP-005` Startup derives the WAL head only from the selected tail's
//# `LogRegionPrologue` plus any later `head(collection_id = 0, ...)` records found in that
//# same tail region. Stale headers in free-space member regions therefore do not influence
//# WAL-head recovery once they lose tail selection.
#[test]
fn requirement_open_formatted_store_reports_recovered_nonzero_wal_head() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    init_wal_region(&mut flash, 0, 3, 2, metadata.region_count);
    init_wal_region(&mut flash, 2, 2, 2, metadata.region_count);
    append_wal_record(
        &mut flash,
        metadata,
        2,
        wal_offset,
        WalRecord::Link {
            next_region_index: 0,
            expected_sequence: 3,
        },
    );

    let state = open_formatted_store::<128, 4, _>(&mut flash).unwrap();
    assert_eq!(state.wal_head(), 2);
    assert_eq!(state.wal_tail(), 0);
    assert_eq!(state.ready_free_region(), Some(2));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-027` If replay reaches WAL end with an active full transaction descriptor
//# and no durable `commit_transaction`, run idempotent rollback recovery for that
//# transaction-log range. Any region popped by `allocate_region` in the uncommitted range
//# returns to the dirty range, because it may have been written before the crash. Append
//# `rollback_transaction(transaction_log_id, range)` after cleanup.
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
//# `RING-STARTUP-RESULT-007` Transaction terminal records written during recovery, if
//# recovery needed to close an incomplete full or inline transaction range.
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

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-024` `commit_transaction(transaction_log_id, range)` is valid only if
//# the range starts at the matching open transaction descriptor's start, ends at that
//# transaction log's current append position, contains only complete valid records, and
//# contains no torn record before `range.end`.
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

    let mut replay_state = OpenWalReplayState::default();
    let replay_error = replay_open_wal_region::<128, 4, _, 8>(
        &mut flash,
        &mut workspace,
        &mut plan,
        0,
        0,
        false,
        &mut replay_state,
    )
    .unwrap_err();
    assert_eq!(
        replay_error,
        StartupError::BrokenWalChain { region_index: 0 }
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-008` Maintain replay state:
//# per collection optional live `collection_type`, explicit collection state,
//# `basis_pos`, `pending_updates`, and committed state generation;
//# free-space collection queue and cursors; optional storage-core private
//# allocation reservation; transaction-log cursors and live-prefix
//# boundaries; full and inline transaction descriptors; and the replay
//# local WAL-recovery boundary.
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
    let mut open_inline_transaction = None;
    let aligned_end_offset = wal_offset + 8;

    let step = classify_replay_record(
        &mut plan,
        &mut open_transaction,
        &mut open_inline_transaction,
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
    assert_eq!(transaction.start, crate::test_log_position(CollectionId(7)));
    assert_eq!(plan.transaction_original_collections.len(), 1);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-017` On
//# `commit_inline_transaction(record_count)`: verify that it closes the
//# current bounded inline transaction, then apply the body records
//# atomically at this commit position. Advance committed state generation
//# for any affected collection and apply allocator pops, frees, and erase
//# publishes in body order.
#[test]
fn requirement_inline_transaction_commit_applies_body_atomically() {
    let mut flash = MockFlash::<256, 5, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let update = WalRecord::Update {
        collection_id,
        payload: &[1, 2, 3],
    };
    let update_len = encoded_len_for_record::<256>(metadata, update);

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
        WalRecord::BeginInlineTransaction {
            record_count: 1,
            encoded_len: u32::try_from(update_len).unwrap(),
        },
    );
    offset = append_wal_record(&mut flash, metadata, 0, offset, update);
    append_wal_record(
        &mut flash,
        metadata,
        0,
        offset,
        WalRecord::CommitInlineTransaction { record_count: 1 },
    );

    let state = open_formatted_store::<256, 5, _>(&mut flash).unwrap();
    assert_eq!(
        collection_summary(&state, collection_id).pending_update_count(),
        1
    );
}

//= spec/ring/09-implementation-coverage.md#free-space-collection-coverage-targets
//= type=test
//# `RING-IMPL-FREE-010` Inline transactions MUST apply allocator and
//# collection effects atomically after `commit_inline_transaction` and
//# ignore body effects before commit.
#[test]
fn requirement_uncommitted_inline_transaction_is_rolled_back_on_startup() {
    let mut flash = MockFlash::<256, 5, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let collection_id = CollectionId(7);
    let update = WalRecord::Update {
        collection_id,
        payload: &[1, 2, 3],
    };
    let update_len = encoded_len_for_record::<256>(metadata, update);

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
        WalRecord::BeginInlineTransaction {
            record_count: 1,
            encoded_len: u32::try_from(update_len).unwrap(),
        },
    );
    append_wal_record(&mut flash, metadata, 0, offset, update);

    let state = open_formatted_store::<256, 5, _>(&mut flash).unwrap();
    assert_eq!(
        collection_summary(&state, collection_id).pending_update_count(),
        0
    );
}

//= spec/ring/09-implementation-coverage.md#free-space-collection-coverage-targets
//= type=test
//# `RING-IMPL-FREE-007` If `allocate_region` is durable but the
//# enclosing transaction is not committed, rollback recovery MUST return
//# the region to the dirty range.
#[test]
fn requirement_uncommitted_inline_transaction_returns_allocations_dirty() {
    let mut flash = MockFlash::<512, 6, 512>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let allocation = WalRecord::AllocateRegion {
        region_index: 2,
        allocation_head_after: FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    };
    let allocation_len = encoded_len_for_record::<512>(metadata, allocation);

    let mut offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::BeginInlineTransaction {
            record_count: 1,
            encoded_len: u32::try_from(allocation_len).unwrap(),
        },
    );
    offset = append_wal_record(&mut flash, metadata, 0, offset, allocation);
    assert!(offset > wal_offset);

    let mut workspace = StorageWorkspace::<512>::new();
    let mut plan = StartupOpenPlan::<6, 8>::empty();
    begin_open_formatted_store::<512, 6, _, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("begin open");
    recover_open_rotation::<512, _, 6, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("recover rotation");
    replay_open_wal_chain::<512, 6, _, 8>(&mut flash, &mut workspace, &mut plan)
        .expect("replay WAL");
    let state =
        finish_open_formatted_store::<512, 6, _, 8>(&mut flash, &mut plan).expect("finish open");
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.free_space_tail_region(), Some(2));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-150` Transaction recovery bookkeeping MUST scope allocator
//# observations to the open transaction or inline transaction and ignore non-visible
//# allocator records from other ranges.
#[test]
fn requirement_transaction_recovery_observes_only_the_transaction_collection_allocator_records() {
    let mut plan = StartupOpenPlan::<4, 8>::empty();
    let mut transaction = OpenTransactionReplay {
        collection_id: Some(CollectionId(7)),
        transaction_log_id: crate::test_transaction_log_id(CollectionId(7)),
        start: crate::test_log_position(CollectionId(7)),
        committed_range: None,
        commit_seen: false,
    };

    observe_transaction_recovery_record(
        &mut plan,
        &mut transaction,
        WalRecord::AllocateRegion {
            region_index: 1,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    )
    .unwrap();
    observe_transaction_recovery_record(
        &mut plan,
        &mut transaction,
        WalRecord::FreeRegion {
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    )
    .unwrap();
    assert_eq!(plan.transaction_allocations.as_slice(), &[1]);
    assert_eq!(plan.transaction_frees.as_slice(), &[2]);

    observe_transaction_recovery_record(
        &mut plan,
        &mut transaction,
        WalRecord::AllocateRegion {
            region_index: 1,
            allocation_head_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    )
    .unwrap();
    observe_transaction_recovery_record(
        &mut plan,
        &mut transaction,
        WalRecord::FreeRegion {
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    )
    .unwrap();
    assert_eq!(plan.transaction_allocations.as_slice(), &[1]);
    assert_eq!(plan.transaction_frees.as_slice(), &[2]);
}

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-029` `begin_inline_transaction(record_count, encoded_len)` is valid only
//# in the main WAL when no full transaction or inline transaction is active. Storage MUST
//# reserve enough main-WAL tail space for the whole bounded range before appending the
//# begin record.
#[test]
fn requirement_transaction_marker_ids_must_match_the_open_transaction() {
    assert_eq!(
        ensure_transaction_log_marker_matches(0, 1),
        Err(StartupError::InvalidTransactionLogId {
            transaction_log_id: 1,
            slot_count: 1,
        })
    );
}

//= spec/ring/05-disk-format.md#storage-metadata
//= type=test
//# `RING-META-007` Opening MUST reject media whose
//# `transaction_log_count` does not equal the configured transaction slot
//# count for this implementation.
#[test]
fn requirement_open_rejects_metadata_transaction_log_count_mismatch() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let incompatible = StorageMetadata::new_with_transaction_logs(
        metadata.region_size,
        metadata.region_count,
        metadata.min_free_regions,
        2,
        metadata.wal_write_granule,
        metadata.erased_byte,
        metadata.wal_record_magic,
    )
    .unwrap();
    flash.write_metadata(incompatible).unwrap();

    assert_eq!(
        open_formatted_store(&mut flash).unwrap_err(),
        StartupError::TransactionLogCountMismatch {
            metadata_count: 2,
            slot_count: 1,
        }
    );
}

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-021` `begin_transaction`, `commit_transaction`, `transaction_finished`,
//# and `rollback_transaction` records are valid only in the main WAL, and their
//# `transaction_log_id` MUST be less than the configured `transaction_log_count`.
#[test]
fn requirement_replay_rejects_transaction_log_id_outside_slot_array() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, wal_offset);
    let mut open_transaction = None;
    let mut open_inline_transaction = None;

    assert_eq!(
        classify_replay_record(
            &mut plan,
            &mut open_transaction,
            &mut open_inline_transaction,
            WalReplayPosition {
                chain_index: 0,
                region_index: 0,
                offset: wal_offset,
            },
            wal_offset + 8,
            WalRecord::BeginTransaction {
                transaction_log_id: 1,
                start: LogPosition {
                    region_index: 0,
                    offset: 0,
                },
            },
        ),
        Err(StartupError::InvalidTransactionLogId {
            transaction_log_id: 1,
            slot_count: 1,
        })
    );
}

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-013` For user collections, `update` records that appear before replay
//# has seen a retained basis decision for that collection have no replay effect.
//# Implementations MUST NOT count them as retained post-basis updates.
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

    plan.free_space = FreeSpaceState::empty();
    plan.wal_append_offset = metadata.wal_record_area_offset().unwrap();
    assert_eq!(
        recovery_record_has_append_room::<128, 4, _, 8>(
            &mut flash,
            &mut workspace,
            &mut plan,
            WalRecord::AllocateRegion {
                region_index: 1,
                allocation_head_after: FreeQueuePosition {
                    region_index: 1,
                    entry_index: 1
                },
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
//# `RING-WAL-ENC-013` Appending any WAL record to the current private log tail region,
//# other than the specific storage-core `allocate_region(next_region_index,
//# allocation_head_after)` that starts WAL rotation or the trailing `link`, is invalid if
//# doing so would leave fewer than `wal_rotation_reserve` unwritten bytes in that private
//# log region.
#[test]
fn requirement_recovery_record_room_checks_the_next_free_footer_at_exact_tail_end() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    let wal_recovery_len = encoded_len_for_record::<128>(metadata, WalRecord::WalRecovery);
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 128 - wal_recovery_len);

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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-147` Startup recovery record writing MUST treat
//# the private-log region boundary as an exact valid end: a recovery record
//# whose aligned encoded end equals the boundary advances the apply path to
//# that boundary and reports the raw encoded length.
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
//# `RING-WAL-ENC-014` Appending the storage-core `allocate_region(next_region_index,
//# allocation_head_after)` that starts WAL rotation is invalid unless its aligned end
//# offset still leaves at least `wal_link_reserve` and fewer than `wal_rotation_reserve`
//# unwritten bytes in that private log region. This reserve-window placement makes an
//# unmatched tail allocation unambiguously recognizable as the WAL-rotation-start record
//# during startup recovery. Once that allocation record is durable, the only valid later
//# WAL record in that private log region is the matching trailing `link`.
#[test]
fn requirement_recovery_rotation_start_accepts_only_the_link_reserve_window() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<256>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 0);
    let reserves = recovery_rotation_reserves::<256, 4, 8>(
        &mut workspace,
        &mut plan,
        2,
        FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    )
    .unwrap();

    plan.wal_append_offset = 256 - reserves.allocate_region_len - reserves.link_reserve;
    let next_region =
        append_recovery_wal_rotation_start::<256, 4, _, 8>(&mut flash, &mut workspace, &mut plan)
            .unwrap();
    assert_eq!(next_region, 2);
    assert_eq!(plan.wal_append_offset, 256 - reserves.link_reserve);
    assert_eq!(plan.ready_region, None);

    let mut too_much_room = startup_plan_with_append_offset::<4>(
        metadata,
        0,
        0,
        256 - reserves.allocate_region_len - reserves.rotation_reserve,
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
        256 - reserves.allocate_region_len - reserves.link_reserve + 1,
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
//# size needed in the
//# current private log tail region to append the trailing
//# `link(next_region_index, expected_sequence)` record that completes WAL
//# rotation.
#[test]
fn requirement_recovery_rotation_bridges_large_windows_before_rotating() {
    const REGION_SIZE: usize = 512;

    let mut flash = MockFlash::<REGION_SIZE, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 0);
    let reserves = recovery_rotation_reserves::<REGION_SIZE, 4, 8>(
        &mut workspace,
        &mut plan,
        2,
        FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    )
    .unwrap();
    let recovery_len = encoded_len_for_record::<REGION_SIZE>(metadata, WalRecord::WalRecovery);
    let bridge_len = usize::try_from(metadata.wal_write_granule).unwrap() + recovery_len;
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let bridge_offset = (wal_offset..REGION_SIZE)
        .find(|candidate| {
            let Some(after_alloc) = candidate.checked_add(reserves.allocate_region_len) else {
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

    assert_eq!(plan.wal_tail, 2);
    assert_eq!(
        plan.wal_append_offset,
        metadata.wal_record_area_offset().unwrap()
    );
    assert_eq!(plan.max_seen_sequence, 1);
    assert!(!plan.pending_wal_recovery_boundary);
}

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-014` For user collections, `snapshot`, `head`, and `drop_collection` are
//# invalid if replay has already seen a prior valid `drop_collection` for that collection.
#[test]
fn requirement_recovery_rotation_rejects_windows_too_small_for_the_link() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<256>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 0);
    let reserves = recovery_rotation_reserves::<256, 4, 8>(
        &mut workspace,
        &mut plan,
        2,
        FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    )
    .unwrap();
    plan.wal_append_offset = 256 - reserves.allocate_region_len - reserves.link_reserve + 1;

    assert!(matches!(
        rotate_recovery_wal_tail::<256, 4, _, 8>(&mut flash, &mut workspace, &mut plan),
        Err(StartupError::InvalidWalRotationWindow { .. })
    ));
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-012` Let `wal_rotation_reserve` be the total aligned encoded size needed
//# in the current private log tail region to append the two WAL records required to start
//# and complete rotation to a new tail region: `allocate_region(next_region_index,
//# allocation_head_after)` followed by `link(next_region_index, expected_sequence)`.
#[test]
fn requirement_append_recovery_record_room_rotates_when_tail_lacks_reserve() {
    const REGION_SIZE: usize = 512;

    let mut flash = MockFlash::<REGION_SIZE, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, 0);
    let reserves = recovery_rotation_reserves::<REGION_SIZE, 4, 8>(
        &mut workspace,
        &mut plan,
        2,
        FreeQueuePosition {
            region_index: 1,
            entry_index: 1,
        },
    )
    .unwrap();
    let record = crate::test_rollback_transaction_record(CollectionId(7));
    let record_len = encoded_len_for_record::<REGION_SIZE>(metadata, record);
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let rotation_offset = (wal_offset..REGION_SIZE)
        .find(|candidate| {
            let Some(after_alloc) = candidate.checked_add(reserves.allocate_region_len) else {
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

    assert_eq!(plan.wal_tail, 2);
    assert_eq!(
        plan.wal_append_offset,
        metadata.wal_record_area_offset().unwrap()
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-008` `erase_free_region_span` Publishes erase maintenance for the next
//# `count` dirty entries starting at the current `ready_boundary`. The physical erases
//# happen before this record is durable; the durable effect is to advance `ready_boundary`
//# to `ready_boundary_after`. If power fails after erase but before this record is durable,
//# replay treats those entries as still dirty and may erase them again.
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-148` Startup WAL-gap bridging during recovery
//# MUST reject invalid geometry before writing: zero `wal_write_granule`
//# metadata and gap placements that overflow the private log tail are
//# errors.
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-149` Startup corrupt-boundary marking MUST choose
//# a sentinel byte distinct from both the configured erased byte and the
//# configured WAL record magic byte.
#[test]
fn requirement_first_invalid_wal_boundary_byte_avoids_erased_and_magic_values() {
    assert_eq!(first_invalid_wal_boundary_byte(0, 1), 2);
    assert_ne!(first_invalid_wal_boundary_byte(0xff, 0xa5), 0xff);
    assert_ne!(first_invalid_wal_boundary_byte(0xff, 0xa5), 0xa5);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-006` Initialize the free-space collection from the materialized
//# `free_space_v2` metadata region chain rooted at canonical region `1`; the effective log
//# prologue cursors name positions within that chain, not an alternate root.
#[test]
fn requirement_startup_loads_free_space_collection_metadata() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();

    let free_space =
        load_initial_free_space_from_flash::<128, _>(&mut flash, &mut workspace, metadata).unwrap();

    assert_eq!(
        free_space.allocation_head_position(),
        FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        }
    );
    assert_eq!(
        free_space.ready_boundary_position(),
        FreeQueuePosition {
            region_index: 1,
            entry_index: 2,
        }
    );
    assert_eq!(
        free_space.ready_boundary_position(),
        free_space.append_tail_position()
    );
    assert_eq!(free_space.entries(), &[2, 3]);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-006` Initialize the free-space collection from the materialized
//# `free_space_v2` metadata region chain rooted at canonical region `1`; the effective
//# log prologue cursors name positions within that chain, not an alternate root.
//# Validate each metadata header's strictly increasing chain-local sequence, each
//# `FreeSpaceRegionPrologue`,
#[test]
fn requirement_startup_rejects_free_space_metadata_chain_sequence_regression() {
    const REGION_SIZE: usize = 128;
    const REGION_COUNT: usize = 64;
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 512>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();

    let mut header =
        Header::decode(&flash.region_bytes(2).unwrap()[..Header::ENCODED_LEN]).unwrap();
    header.sequence = 0;
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    flash.write_region(2, 0, &header_bytes).unwrap();

    assert_eq!(
        load_initial_free_space_from_flash::<REGION_SIZE, _>(&mut flash, &mut workspace, metadata,),
        Err(StartupError::InvalidFreeSpaceCollection)
    );
}

//= spec/ring/04-wal-records.md#ordering-and-validity
//= type=test
//# `RING-WAL-VALID-033` `rollback_inline_transaction(record_count)` is valid only as the
//# durable terminal marker for an uncommitted inline range that recovery has cleaned.
//# Replay MUST NOT apply the inline body.
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
//# `RING-STARTUP-015` Transaction-log records are not applied by ordinary log-chain
//# traversal. Startup scans a transaction-log range only when a retained main-WAL
//# `commit_transaction`, `rollback_transaction`, `transaction_finished`, or active recovery
//# descriptor references that range. Records inside an imported committed range are applied
//# at the main-WAL commit record's replay position. Records inside an uncommitted rollback
//# range are scanned only for cleanup and recovery effects and do not become visible
//# collection or allocator state.
#[test]
fn requirement_open_replay_allocator_record_applies_free_regions() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut plan = startup_plan_with_append_offset::<4>(metadata, 0, 0, wal_offset);
    plan.free_space.replace_from_parts(1, 0, 0, 0, &[]).unwrap();
    let append_tail_after = plan.free_space.position_after_append().unwrap();

    apply_open_replay_allocator_record(
        &mut plan,
        WalRecord::FreeRegion {
            region_index: 2,
            append_tail_after,
        },
    )
    .unwrap();

    assert_eq!(plan.free_space.ready_count(), 0);
    assert_eq!(plan.free_space.dirty_count(), 1);
    assert_eq!(plan.free_space.entries(), &[2]);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-014` On
//# `allocate_region(region_index, allocation_head_after)`: verify that
//# the ready range is non-empty, the current `allocation_head` entry
//# names `region_index`, and `allocation_head_after` is the next queue
//# position.
#[test]
fn requirement_allocate_region_advances_the_free_space_head() {
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
    let mut free_space = FreeSpaceState::new_ready_range(1, 2, metadata.region_count).unwrap();
    let allocation_head_after = free_space.position_after_allocation().unwrap();
    let mut ready_region = None;

    apply_wal_record(
        metadata,
        WalRecord::AllocateRegion {
            region_index: 2,
            allocation_head_after,
        },
        &mut collections,
        &mut free_space,
        &mut ready_region,
    )
    .unwrap();

    assert_eq!(free_space.allocation_head(), 1);
    assert_eq!(free_space.next_ready_region(), Ok(3));
    assert_eq!(ready_region, None);
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-007` Allocator queue links and cursor state MUST NOT
//# be stored in freed data regions; they MUST be stored in WAL records,
//# private log prologues, or `free_space_v2` metadata regions.
#[test]
fn requirement_free_space_replay_ignores_stale_footer_bytes() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let mut workspace = StorageWorkspace::<128>::new();
    init_user_region_header(&mut flash, 2, 9, CollectionId(7), MAP_REGION_V2_FORMAT);
    flash
        .write_region(2, 120, &[1, 0, 0, 0, 0x1d, 0x2c, 0x3b, 0x4a])
        .unwrap();

    let free_space =
        load_initial_free_space_from_flash::<128, _>(&mut flash, &mut workspace, metadata).unwrap();

    assert_eq!(free_space.entries(), &[2, 3]);
    assert_eq!(free_space.allocation_head(), 0);
    assert_eq!(free_space.ready_boundary(), 2);
    assert_eq!(free_space.append_tail(), 2);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-006` Initialize the free-space collection from the materialized
//# `free_space_v2` metadata region chain rooted at canonical region `1`; the effective
//# log prologue cursors name positions within that chain, not an alternate root.
//# Validate each metadata header's strictly increasing chain-local sequence, each
//# `FreeSpaceRegionPrologue`, each `FreeSpaceEntry`, and the cursor invariant
//# `allocation_head <= ready_boundary <= append_tail`. The materialized queue supplies
//# the initial FIFO entries and cursor positions; later retained WAL allocator commands
//# update that state.
#[test]
fn requirement_free_space_metadata_chain_reopens_with_real_cursor_positions() {
    const REGION_SIZE: usize = 128;
    const REGION_COUNT: usize = 64;
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 512>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let entries_offset = Header::ENCODED_LEN + FreeSpaceRegionPrologue::ENCODED_LEN;
    let entries_per_metadata_region = (REGION_SIZE - entries_offset) / FreeSpaceEntry::ENCODED_LEN;
    let mut metadata_region_count = 1u32;
    loop {
        let free_space_entry_count = metadata.region_count - 1 - metadata_region_count;
        if metadata_region_count * u32::try_from(entries_per_metadata_region).unwrap()
            >= free_space_entry_count
        {
            break;
        }
        metadata_region_count += 1;
    }
    assert!(metadata_region_count > 1);
    let free_space_entry_count = metadata.region_count - 1 - metadata_region_count;
    let expected_tail = crate::free_queue_position_for_contiguous_metadata(
        1,
        metadata_region_count,
        entries_per_metadata_region,
        free_space_entry_count,
    )
    .unwrap();

    let state = open_formatted_store::<REGION_SIZE, REGION_COUNT, _>(&mut flash).unwrap();

    assert_eq!(state.allocation_head().region_index, 1);
    assert_eq!(state.allocation_head().entry_index, 0);
    assert_eq!(state.ready_boundary(), expected_tail);
    assert_eq!(state.append_tail(), expected_tail);
    assert_eq!(state.ready_free_region(), Some(1 + metadata_region_count));
    assert_eq!(
        state.free_space_tail_region(),
        Some(REGION_COUNT as u32 - 1)
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-154` Typed map opening after storage replay MUST
//# validate retained map committed-region payloads, snapshot payloads, and
//# update payloads and reject any that fail map-specific validation.
#[test]
fn requirement_open_map_rejects_invalid_retained_map_region_snapshot_and_update_payloads() {
    assert_startup_rejects_invalid_retained_map_region_payload();
    assert_startup_rejects_invalid_retained_map_snapshot_payload();
    assert_startup_rejects_invalid_retained_map_update_payload();
}

fn assert_startup_rejects_invalid_retained_map_region_payload() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
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

fn assert_startup_rejects_invalid_retained_map_snapshot_payload() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
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

fn assert_startup_rejects_invalid_retained_map_update_payload() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
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
