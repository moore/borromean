#![allow(clippy::drop_non_drop)]

use super::*;
use crate::disk::{FreePointerFooter, Header};
use crate::wal_record::{encode_record_into, encoded_record_len, WalRecord};
use crate::{
    MapError, MapStorageError, MockFlash, Storage, StorageFormatConfig, StorageWorkspace,
    MAP_REGION_V2_FORMAT,
};

fn open_formatted_store<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
) -> Result<StartupState<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StartupError> {
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut plan = StartupOpenPlan::<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>::empty();
    super::open_formatted_store::<REGION_SIZE, REGION_COUNT, _, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
        flash,
        &mut workspace,
        &mut plan,
    )
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
        wal_head_region_index,
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
        WalRecord::FreeListHead { region_index: None },
    );

    (
        wal_offset,
        open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap_err(),
    )
}

fn open_formatted_store_after_corrupt_slot_with_wal_recovery() -> (usize, StartupState<8, 4>) {
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
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
        WalRecord::FreeListHead {
            region_index: Some(2),
        },
    );

    (
        next_offset,
        open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap(),
    )
}

fn open_formatted_store_after_torn_slot_with_wal_recovery() -> (usize, StartupState<8, 4>) {
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len = encode_record_into(
        WalRecord::FreeListHead {
            region_index: Some(3),
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
        WalRecord::FreeListHead {
            region_index: Some(2),
        },
    );

    (
        next_offset,
        open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap(),
    )
}

fn open_formatted_store_after_replayed_alloc_begin() -> (usize, StartupState<8, 4>) {
    let mut flash = MockFlash::<256, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let mut physical = [0u8; 256];
    let mut logical = [0u8; 256];
    let alloc_len = encoded_record_len(
        WalRecord::AllocBegin {
            region_index: 1,
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
            region_index: 1,
            free_list_head_after: Some(2),
        },
    );

    (
        next_offset,
        open_formatted_store::<256, 4, _, 8, 4>(&mut flash).unwrap(),
    )
}

fn open_formatted_store_after_replayed_stage_region() -> (usize, StartupState<8, 4>) {
    let mut flash = MockFlash::<256, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    let after_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocBegin {
            region_index: 1,
            free_list_head_after: Some(2),
        },
    );
    let next_offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_alloc,
        WalRecord::StageRegion { region_index: 1 },
    );

    (
        next_offset,
        open_formatted_store::<256, 4, _, 8, 4>(&mut flash).unwrap(),
    )
}

fn open_formatted_store_after_completed_wal_rotation() -> StartupState<8, 4> {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocBegin {
            region_index: 1,
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

    open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap()
}

fn open_formatted_store_from_fresh_format() -> (StorageMetadata, StartupState<8, 4>) {
    let mut flash = MockFlash::<64, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let state = open_formatted_store::<64, 4, _, 8, 4>(&mut flash).unwrap();
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
    let error = open_formatted_store::<64, 4, _, 8, 4>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::MissingMetadata);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-003 Select WAL tail as the unique candidate WAL region with the largest valid
//# sequence. If no candidate WAL region exists, or if multiple candidate WAL regions share that
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

    let error = open_formatted_store::<64, 4, _, 8, 4>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::DuplicateWalTailSequence(0));
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-046` Startup tail selection MUST ignore regions with nonzero collection_id
//# even when their format is wal_v1 while still tracking max seen sequence.
#[test]
fn requirement_open_formatted_store_ignores_nonzero_collection_with_wal_format_when_selecting_tail()
{
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 1, 9, CollectionId(7), WAL_V1_FORMAT);

    let state = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap();
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
//# RING-STARTUP-021 Reconstruct runtime `free_list_tail` by following free-pointer links starting
//# at `last_free_list_head` until reaching a free region whose free-pointer slot is uninitialized.
#[test]
fn requirement_open_formatted_store_rejects_invalid_free_list_chain() {
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

    let error = open_formatted_store::<64, 4, _, 8, 4>(&mut flash).unwrap_err();
    assert_eq!(
        error,
        StartupError::InvalidFreeListChain { region_index: 1 }
    );
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-011 On `alloc_begin(region_index, free_list_head_after)`: if `ready_region` is
//# already set, return an error because replay found two unmatched allocation reservations. if
//# `last_free_list_head = none`, return an error because allocation cannot consume an empty durable
//# free list. if `last_free_list_head != region_index`, return an error because `alloc_begin` did
//# not consume the current durable free-list head. set durable `last_free_list_head` to
//# `free_list_head_after`. set `ready_region = region_index`.
#[test]
fn requirement_open_formatted_store_replays_alloc_begin_into_allocator_runtime_state() {
    let (_next_offset, state) = open_formatted_store_after_replayed_alloc_begin();
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.ready_region(), Some(1));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-020 Initialize allocator state from `last_free_list_head`.
#[test]
fn requirement_open_formatted_store_initializes_allocator_state_after_alloc_begin() {
    let (_next_offset, state) = open_formatted_store_after_replayed_alloc_begin();
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.free_list_tail(), Some(3));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-022 If `ready_region` is set, hold it in memory as the next region to use before
//# consuming another free-list entry.
#[test]
fn requirement_open_formatted_store_keeps_replayed_ready_region_reserved_in_memory() {
    let (_next_offset, state) = open_formatted_store_after_replayed_alloc_begin();
    assert_eq!(state.ready_region(), Some(1));
    assert_eq!(state.last_free_list_head(), Some(2));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-RESULT-008` Ordered staged regions that have left
//# `ready_region` but are not yet known to be free
#[test]
fn requirement_open_formatted_store_reports_replayed_staged_regions() {
    let (_next_offset, state) = open_formatted_store_after_replayed_stage_region();
    assert_eq!(state.staged_regions(), &[1]);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-011A On `stage_region(region_index)`: if
//# `ready_region != region_index`, return an error. otherwise append
//# `region_index` to staged regions and clear `ready_region`.
#[test]
fn requirement_open_formatted_store_replays_stage_region_into_staged_state() {
    let (_next_offset, state) = open_formatted_store_after_replayed_stage_region();
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.staged_regions(), &[1]);
    assert_eq!(state.last_free_list_head(), Some(2));
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-047` Startup replay MUST preserve staged regions when a WAL head-control
//# record is replayed.
#[test]
fn requirement_open_formatted_store_keeps_staged_regions_when_wal_head_control_is_replayed() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocBegin {
            region_index: 1,
            free_list_head_after: Some(2),
        },
    );
    let after_stage = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_alloc,
        WalRecord::StageRegion { region_index: 1 },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_stage,
        WalRecord::Head {
            collection_id: CollectionId(0),
            collection_type: CollectionType::WAL_CODE,
            region_index: 0,
        },
    );

    let state = open_formatted_store::<256, 4, _, 8, 4>(&mut flash).unwrap();
    assert_eq!(state.staged_regions(), &[1]);
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-048` Startup replay MUST preserve staged regions when non-map collection
//# head and drop records are replayed.
#[test]
fn requirement_open_formatted_store_keeps_staged_regions_when_non_map_head_is_replayed() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocBegin {
            region_index: 1,
            free_list_head_after: Some(2),
        },
    );
    let after_stage = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_alloc,
        WalRecord::StageRegion { region_index: 1 },
    );
    let after_head = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_stage,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::CHANNEL_CODE,
            region_index: 2,
        },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_head,
        WalRecord::DropCollection {
            collection_id: CollectionId(7),
        },
    );

    let state = open_formatted_store::<256, 4, _, 8, 4>(&mut flash).unwrap();
    assert_eq!(state.staged_regions(), &[1]);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-028` `stage_region(region_index)` MUST be preceded by
//# an unmatched valid `alloc_begin(region_index, free_list_head_after)`
//# for the same region.
#[test]
fn requirement_open_formatted_store_rejects_stage_region_without_matching_ready_region() {
    let mut flash = MockFlash::<256, 4, 64>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();

    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::StageRegion { region_index: 1 },
    );

    assert_eq!(
        open_formatted_store::<256, 4, _, 8, 4>(&mut flash).unwrap_err(),
        StartupError::InvalidStageRegion {
            region_index: 1,
            ready_region: None,
        }
    );
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-ENC-010` The recovered append point for the tail region
//# MUST be the first aligned
//# slot whose first byte is `erased_byte` after the last valid replayed
//# tail record. If no such slot exists, the tail region is currently full
//# and the next WAL append must rotate via `link` to a new WAL region.
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
    assert_eq!(state.last_free_list_head(), Some(2));
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
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
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

    let state = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap();

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
    assert!(state.pending_reclaims().is_empty());
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

    let state = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap();
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
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
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

    let error = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap_err();
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
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
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

    let state = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap();

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
//# `RING-IMPL-REGRESSION-051` Startup replay MUST accept a reclaimed historical head after
//# replacement and recover the live replacement head with no pending reclaim.
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
    let after_reclaim_begin = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_second_head,
        WalRecord::ReclaimBegin { region_index: 1 },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_reclaim_begin,
        WalRecord::ReclaimEnd { region_index: 1 },
    );

    let state = open_formatted_store::<256, 5, _, 8, 4>(&mut flash).unwrap();

    assert_eq!(state.collections().len(), 1);
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(2)
    );
    assert!(state.pending_reclaims().is_empty());
    assert_eq!(state.free_list_tail(), Some(1));
}

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-052` Startup replay MUST track pending updates on an empty collection
//# basis and preserve their count.
#[test]
fn requirement_open_formatted_store_tracks_pending_updates_on_empty_collection_basis() {
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
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

    let state = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap();

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
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
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

    let error = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::DroppedCollection(CollectionId(7)));
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-VALID-026` `reclaim_begin(region_index)` and
//# `reclaim_end(region_index)` MUST appear in WAL order and are matched
//# by `region_index`.
#[test]
fn requirement_open_formatted_store_tracks_pending_reclaims_in_order() {
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_begin_3 = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::ReclaimBegin { region_index: 3 },
    );
    let after_begin_2 = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_begin_3,
        WalRecord::ReclaimBegin { region_index: 2 },
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_begin_2,
        WalRecord::ReclaimEnd { region_index: 3 },
    );

    let state = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap();

    assert_eq!(state.pending_reclaims(), &[2]);
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-015` An implementation MUST NOT open a database successfully if replay yields a
//# live collection whose `collection_type` is unsupported by that implementation.
#[test]
fn requirement_open_formatted_store_rejects_unsupported_live_collection_type() {
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
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

    let error = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::UnsupportedLiveCollectionType(0x1234));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-026` If replay yields a live collection whose
//# `collection_type` is unsupported by the implementation, startup MUST
//# fail.
#[test]
fn requirement_open_formatted_store_fails_startup_for_unsupported_live_collection_type() {
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
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

    let error = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::UnsupportedLiveCollectionType(0x1234));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-028` A dropped tombstone whose old
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
//# `RING-IMPL-REGRESSION-054` Strict WAL-region reads MUST reject regions whose collection_id is
//# nonzero even if collection_format is wal_v1.
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
//# `RING-IMPL-REGRESSION-055` WAL target validation MUST require both collection_id 0 and
//# collection_format wal_v1.
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

//= spec/ring/06-startup-replay.md#startup-replay-implementation-requirements
//= type=test
//# `RING-IMPL-REGRESSION-056` Live committed-region basis validation MUST reject a region whose
//# header belongs to a different collection.
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
//# RING-STARTUP-013 On `link(next_region_index, expected_sequence)`: if `ready_region =
//# next_region_index`, clear `ready_region`.
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
    let mut flash = MockFlash::<128, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_alloc = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::AllocBegin {
            region_index: 1,
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

    let state = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap();

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
        region_index: 1,
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

    let state = open_formatted_store::<160, 4, _, 8, 4>(&mut flash).unwrap();

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
        region_index: 1,
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

    let state = open_formatted_store::<REGION_SIZE, 4, _, 8, 4>(&mut flash).unwrap();
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

    let error = open_formatted_store::<128, 4, _, 8, 4>(&mut flash).unwrap_err();
    assert_eq!(error, StartupError::BrokenWalChain { region_index: 0 });
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-018 On `wal_recovery()`: if `pending_wal_recovery_boundary` is clear, return an
//# error. otherwise clear `pending_wal_recovery_boundary`.
#[test]
fn requirement_open_formatted_store_clears_pending_recovery_boundary_when_wal_recovery_is_replayed()
{
    let (next_offset, state) = open_formatted_store_after_corrupt_slot_with_wal_recovery();
    assert_eq!(state.wal_append_offset(), next_offset);
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.last_free_list_head(), Some(2));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-002 Scan all regions, collect candidate WAL regions (`collection_id == 0` plus
//# `collection_format = wal_v1`) with valid headers, and track `max_seen_sequence` as the largest
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
//# RING-STARTUP-004 Read and validate the `WalRegionPrologue` stored at the start of the tail
//# region's user-data area, and use its `wal_head_region_index` as the initial WAL-head candidate.
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
//# RING-STARTUP-023 Keep `max_seen_sequence` as the runtime source of the next region sequence.
#[test]
fn requirement_open_formatted_store_keeps_max_seen_sequence_for_the_next_region_header() {
    let (_metadata, state) = open_formatted_store_from_fresh_format();
    assert_eq!(state.max_seen_sequence(), 0);
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# `RING-STARTUP-027` If replay yields a live collection with unsupported or invalid retained
//# collection data under that collection's normative specification, startup MUST fail before open
//# succeeds.
#[test]
fn requirement_storage_open_path_rejects_invalid_retained_map_region_snapshot_and_update_payloads()
{
    {
        let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage = Storage::<_, 512, 5, 8, 4>::format(
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
            .with_runtime_io_workspace(|runtime, flash, _workspace| {
                runtime.write_committed_region::<512, 5, _>(
                    flash,
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
            Storage::<_, 512, 5, 8, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4, 4>(
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
        let mut storage = Storage::<_, 512, 4, 8, 4>::format(
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
            Storage::<_, 512, 4, 8, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4, 4>(
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
        let mut storage = Storage::<_, 512, 4, 8, 4>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        storage.create_map(CollectionId(45)).unwrap();
        storage.append_update(CollectionId(45), &[0xff]).unwrap();

        drop(storage);
        let mut reopened =
            Storage::<_, 512, 4, 8, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4, 4>(
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
