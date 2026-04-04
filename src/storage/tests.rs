use super::*;
use crate::wal_record::encode_record_into;
use crate::MockFlash;
use crate::StorageWorkspace;
use crate::{
    CollectionId, CollectionType, Header, MockOperation, StartupCollectionBasis, WalRecord,
    WalRegionPrologue,
};
use core::mem::size_of;

fn format<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    min_free_regions: u32,
    wal_write_granule: u32,
    wal_record_magic: u8,
) -> Result<StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StorageRuntimeError> {
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    super::format::<REGION_SIZE, REGION_COUNT, _, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
        flash,
        &mut workspace,
        min_free_regions,
        wal_write_granule,
        wal_record_magic,
    )
}

fn open<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
) -> Result<StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StorageRuntimeError> {
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    super::open::<REGION_SIZE, REGION_COUNT, _, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
        flash,
        &mut workspace,
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

struct CommittedRegionSequenceProgress {
    first_region: u32,
    first_sequence: u64,
    max_seen_after_first: u64,
    second_region: u32,
    second_sequence: u64,
    max_seen_after_second: u64,
}

fn committed_region_sequence_progress() -> CommittedRegionSequenceProgress {
    let mut flash = MockFlash::<512, 6, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 6, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let first_region = state
        .reserve_next_region::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();
    state
        .write_committed_region::<512, 6, _>(
            &mut flash,
            first_region,
            CollectionId(7),
            crate::MAP_REGION_V1_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();
    let first_sequence = read_header_from_flash::<512, 6, _>(&mut flash, first_region)
        .unwrap()
        .sequence;
    state
        .append_head::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            first_region,
        )
        .unwrap();
    let max_seen_after_first = state.max_seen_sequence();

    let second_region = state
        .reserve_next_region::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();
    state
        .write_committed_region::<512, 6, _>(
            &mut flash,
            second_region,
            CollectionId(7),
            crate::MAP_REGION_V1_FORMAT,
            &[4, 5, 6],
        )
        .unwrap();
    let second_sequence = read_header_from_flash::<512, 6, _>(&mut flash, second_region)
        .unwrap()
        .sequence;
    state
        .append_head::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            second_region,
        )
        .unwrap();

    CommittedRegionSequenceProgress {
        first_region,
        first_sequence,
        max_seen_after_first,
        second_region,
        second_sequence,
        max_seen_after_second: state.max_seen_sequence(),
    }
}

//= spec/ring.md#core-requirements
//# `RING-CORE-010` The durable free list MUST be FIFO so allocations
//# consume the oldest free regions first.
#[test]
fn reserve_next_region_consumes_the_oldest_free_regions_first() {
    let progress = committed_region_sequence_progress();

    assert_eq!(progress.first_region, 1);
    assert_eq!(progress.second_region, 2);
}

//= spec/ring.md#core-requirements
//# `RING-CORE-011` Any operation that writes a newly allocated region
//# MUST first durably reserve that region with
//# `alloc_begin(region_index, free_list_head_after)`.
#[test]
fn committed_region_write_uses_a_region_previously_reserved_by_alloc_begin() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<512, 5, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    let region_index = state
        .reserve_next_region::<512, 5, _>(&mut flash, &mut workspace)
        .unwrap();

    let mut saw_alloc_begin = false;
    state
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
            if let WalRecord::AllocBegin {
                region_index: alloc_region,
                free_list_head_after,
            } = record
            {
                if alloc_region == region_index {
                    assert_eq!(free_list_head_after, Some(2));
                    saw_alloc_begin = true;
                }
            }
            Ok(())
        })
        .unwrap();

    assert!(saw_alloc_begin);
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V1_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();
}

//= spec/ring.md#durability-and-crash-semantics
//# `RING-ALLOC-001` Any operation that writes a newly allocated region
//# MUST first make `alloc_begin(region_index, free_list_head_after)`
//# durable.
#[test]
fn committed_region_write_waits_for_alloc_begin_sync() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<512, 5, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    flash.clear_operations();
    let region_index = state
        .reserve_next_region::<512, 5, _>(&mut flash, &mut workspace)
        .unwrap();
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V1_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();

    let erase_index = flash
        .operations()
        .iter()
        .position(|operation| *operation == MockOperation::EraseRegion { region_index })
        .unwrap();
    let alloc_sync_index = flash.operations()[..erase_index]
        .iter()
        .rposition(|operation| *operation == MockOperation::Sync)
        .unwrap();

    assert!(
        flash.operations()[..alloc_sync_index]
            .iter()
            .any(|operation| matches!(
                operation,
                MockOperation::WriteRegion {
                    region_index: 0,
                    ..
                }
            )),
        "expected alloc_begin WAL write before sync"
    );
}

//= spec/ring.md#wal-record-types
//# `RING-REPLAY-ASSUME-004` Any operation that consumes a free-list
//# head MUST first make the allocator advance durable with
//# `alloc_begin(region_index, free_list_head_after)`.
#[test]
fn reopen_after_alloc_begin_recovers_the_advanced_allocator_state() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let region_index = state
        .reserve_next_region::<512, 5, _>(&mut flash, &mut workspace)
        .unwrap();
    let reopened = open::<512, 5, _, 8, 4>(&mut flash).unwrap();

    assert_eq!(reopened.ready_region(), Some(region_index));
    assert_eq!(reopened.last_free_list_head(), Some(2));
    assert_eq!(reopened.free_list_tail(), Some(4));
}

#[test]
fn format_returns_fresh_runtime_state() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert_eq!(state.metadata().region_count, 4);
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 0);
    assert_eq!(state.last_free_list_head(), Some(1));
    assert_eq!(state.free_list_tail(), Some(3));
    assert_eq!(state.ready_region(), None);
    assert!(state.collections().is_empty());
    assert!(state.pending_reclaims().is_empty());
}

//= spec/ring.md#format-storage-on-disk-initialization
//# `RING-FORMAT-STORAGE-POST-002` A user collection durable head MUST
//# NOT exist after formatting.
#[test]
fn format_starts_with_no_user_collection_durable_head() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert!(state.collections().is_empty());
    assert_eq!(state.tracked_user_collection_count(), 0);
}

//= spec/ring.md#core-requirements
//# `RING-CORE-003` Borromean MUST reserve `collection_id = 0` for the
//# WAL, and all user collection identifiers MUST be nonzero stable 64-bit
//# nonces that are never recycled.
#[test]
fn user_collection_ids_are_nonzero_u64_values_and_are_not_recycled() {
    assert_eq!(size_of::<CollectionId>(), size_of::<u64>());

    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert_eq!(
        state.append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(0),
            CollectionType::MAP_CODE,
        ),
        Err(StorageRuntimeError::ReservedCollectionId(CollectionId(0)))
    );

    state
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    state
        .append_drop_collection::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7))
        .unwrap();

    assert_eq!(
        state.append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        ),
        Err(StorageRuntimeError::DuplicateCollection(CollectionId(7)))
    );
}

//= spec/ring.md#core-requirements
//# `RING-CORE-004` Borromean core MUST reserve
//# `collection_type = wal` for `collection_id = 0`, and user collections
//# MUST NOT use that collection type.
#[test]
fn wal_collection_type_is_reserved_for_collection_id_zero() {
    assert_eq!(
        StorageRuntime::<8, 4>::validate_supported_head_collection_type(
            CollectionId(0),
            CollectionType::WAL_CODE,
        ),
        Ok(())
    );
    assert_eq!(
        StorageRuntime::<8, 4>::validate_supported_head_collection_type(
            CollectionId(0),
            CollectionType::MAP_CODE,
        ),
        Err(StorageRuntimeError::CollectionTypeMismatch {
            collection_id: CollectionId(0),
            expected: CollectionType::WAL_CODE,
            actual: CollectionType::MAP_CODE,
        })
    );

    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert_eq!(
        state.append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::WAL_CODE,
        ),
        Err(StorageRuntimeError::UnsupportedCollectionType(
            CollectionType::WAL_CODE
        ))
    );
}

#[test]
fn visit_wal_records_reports_snapshot_and_update_records() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    state
        .append_snapshot::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            &[1, 2, 3],
        )
        .unwrap();
    state
        .append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[9])
        .unwrap();

    let mut seen = [crate::WalRecordType::WalRecovery; 3];
    let mut count = 0usize;
    state
        .visit_wal_records::<256, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
            if count < seen.len() {
                seen[count] = record.record_type();
            }
            count += 1;
            Ok(())
        })
        .unwrap();

    assert_eq!(count, 3);
    assert_eq!(
        seen,
        [
            crate::WalRecordType::NewCollection,
            crate::WalRecordType::Snapshot,
            crate::WalRecordType::Update,
        ]
    );
}

#[test]
fn open_returns_replayed_collection_runtime_state() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(7), 1);

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
    let after_update = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_new,
        WalRecord::Update {
            collection_id: CollectionId(7),
            payload: &[1, 2],
        },
    );
    let next_offset = append_wal_record(
        &mut flash,
        metadata,
        0,
        after_update,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            region_index: 2,
        },
    );

    let state = open::<256, 4, _, 8, 4>(&mut flash).unwrap();

    assert_eq!(state.wal_append_offset(), next_offset);
    assert_eq!(state.max_seen_sequence(), 4);
    assert_eq!(state.collections().len(), 1);
    assert_eq!(state.collections()[0].collection_id(), CollectionId(7));
    assert_eq!(
        state.collections()[0].collection_type(),
        Some(CollectionType::MAP_CODE)
    );
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(2)
    );
    assert_eq!(state.collections()[0].pending_update_count(), 0);
}

#[test]
fn open_completes_reclaims_already_on_the_free_list() {
    let mut flash = MockFlash::<256, 4, 96>::new(0xff);
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
        WalRecord::ReclaimBegin { region_index: 3 },
    );

    let state = open::<256, 4, _, 8, 4>(&mut flash).unwrap();

    assert_eq!(state.ready_region(), Some(1));
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.free_list_tail(), Some(3));
    assert!(state.pending_reclaims().is_empty());
}

#[test]
fn open_discards_pending_reclaims_for_still_live_regions() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(7), 1);

    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let after_head = append_wal_record(
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
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_head,
        WalRecord::ReclaimBegin { region_index: 2 },
    );

    let state = open::<256, 4, _, 8, 4>(&mut flash).unwrap();

    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(2)
    );
    assert!(state.pending_reclaims().is_empty());
}

#[test]
fn append_new_collection_and_update_refresh_runtime_state() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    state
        .append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[1, 2, 3])
        .unwrap();

    assert_eq!(state.collections().len(), 1);
    assert_eq!(state.collections()[0].collection_id(), CollectionId(7));
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );
    assert_eq!(state.collections()[0].pending_update_count(), 1);
}

#[test]
fn append_snapshot_resets_pending_updates() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    state
        .append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[1])
        .unwrap();
    state
        .append_snapshot::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            &[9, 9],
        )
        .unwrap();

    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::WalSnapshot
    );
    assert_eq!(state.collections()[0].pending_update_count(), 0);
}

#[test]
fn append_head_and_drop_refresh_runtime_state() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(7), 1);
    state
        .append_head::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            2,
        )
        .unwrap();
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(2)
    );

    state
        .append_drop_collection::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7))
        .unwrap();
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
    assert_eq!(state.tracked_user_collection_count(), 0);
}

#[test]
fn append_alloc_and_reclaim_methods_refresh_runtime_state() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_alloc_begin::<256, 4, _>(&mut flash, &mut workspace, 1, Some(2))
        .unwrap();
    assert_eq!(state.ready_region(), Some(1));
    assert_eq!(state.last_free_list_head(), Some(2));

    state
        .append_reclaim_begin::<256, 4, _>(&mut flash, &mut workspace, 3)
        .unwrap();
    assert_eq!(state.pending_reclaims(), &[3]);

    state
        .append_reclaim_end::<256, 4, _>(&mut flash, &mut workspace, 3)
        .unwrap();
    assert!(state.pending_reclaims().is_empty());
}

#[test]
fn append_free_list_head_and_wal_recovery_refresh_runtime_state() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let corrupt_offset = metadata.wal_record_area_offset().unwrap();
    flash.write_region(0, corrupt_offset, &[0x10; 8]).unwrap();

    let mut state = open::<256, 4, _, 8, 4>(&mut flash).unwrap();
    assert!(state.pending_wal_recovery_boundary());

    let before_append = state.wal_append_offset();
    state
        .append_wal_recovery::<256, 4, _>(&mut flash, &mut workspace)
        .unwrap();
    assert!(state.wal_append_offset() > before_append);
    assert!(!state.pending_wal_recovery_boundary());

    state
        .append_free_list_head::<256, 4, _>(&mut flash, &mut workspace, Some(2))
        .unwrap();
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.free_list_tail(), Some(3));
}

#[test]
fn append_rotation_start_and_finish_move_to_new_tail() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let mut state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let next_region = state
        .append_wal_rotation_start::<128, 4, _>(&mut flash, &mut workspace)
        .unwrap();
    assert_eq!(next_region, 1);
    assert_eq!(state.ready_region(), Some(1));
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.wal_tail(), 0);

    state
        .append_wal_rotation_finish::<128, 4, _>(&mut flash, &mut workspace, next_region)
        .unwrap();
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 1);
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.last_free_list_head(), Some(2));
    assert_eq!(state.max_seen_sequence(), 1);
}

//= spec/ring.md#storage-requirements
//# `RING-STORAGE-003` Each newly allocated region, whether for a user
//# collection or a newly initialized WAL region, MUST use
//# `sequence = max_seen_sequence + 1`, after which that value becomes the
//# new in-memory `max_seen_sequence`.
#[test]
fn committed_region_allocations_advance_sequence_from_max_seen_sequence() {
    let progress = committed_region_sequence_progress();

    assert_eq!(progress.first_region, 1);
    assert_eq!(progress.first_sequence, 1);
    assert_eq!(progress.max_seen_after_first, progress.first_sequence);

    assert_eq!(progress.second_region, 2);
    assert_eq!(progress.second_sequence, progress.max_seen_after_first + 1);
    assert_eq!(progress.max_seen_after_second, progress.second_sequence);
}

//= spec/ring.md#storage-requirements
//# `RING-STORAGE-003` Each newly allocated region, whether for a user
//# collection or a newly initialized WAL region, MUST use
//# `sequence = max_seen_sequence + 1`, after which that value becomes the
//# new in-memory `max_seen_sequence`.
#[test]
fn wal_rotation_initializes_the_next_wal_region_at_max_seen_sequence_plus_one() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let mut state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let next_region = state
        .append_wal_rotation_start::<128, 4, _>(&mut flash, &mut workspace)
        .unwrap();
    state
        .append_wal_rotation_finish::<128, 4, _>(&mut flash, &mut workspace, next_region)
        .unwrap();

    let header = read_header_from_flash::<128, 4, _>(&mut flash, next_region).unwrap();
    assert_eq!(header.sequence, 1);
    assert_eq!(state.max_seen_sequence(), header.sequence);
}

//= spec/ring.md#storage-requirements
//# `RING-STORAGE-004` Successful later region writes MUST preserve a
//# strictly monotonic `sequence` ordering even if crashes or abandoned
//# allocations leave gaps.
#[test]
fn later_region_writes_keep_sequence_numbers_strictly_monotonic() {
    let progress = committed_region_sequence_progress();

    assert!(progress.first_sequence < progress.second_sequence);
    assert!(progress.max_seen_after_first < progress.max_seen_after_second);
}

//= spec/ring.md#storage-requirements
//# `RING-STORAGE-006` A free region MUST be defined by membership in the
//# durable free-list chain rather than by a distinct on-disk header
//# encoding.
#[test]
fn free_region_membership_is_defined_by_the_free_list_chain() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let reserved_region = state
        .reserve_next_region::<512, 5, _>(&mut flash, &mut workspace)
        .unwrap();

    assert_eq!(reserved_region, 1);
    assert!(!state
        .region_is_on_free_list::<512, 5, _>(&mut flash, reserved_region)
        .unwrap());
    assert!(state
        .region_is_on_free_list::<512, 5, _>(&mut flash, 2)
        .unwrap());
}

//= spec/ring.md#free-pointer-footer
//# `RING-FREE-006` While a region is allocated for live use, the bytes
//# in its free-pointer footer are uninterpreted stale data and MUST NOT
//# be used to infer free-list membership.
#[test]
fn stale_footer_bytes_do_not_make_a_reserved_region_free() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let reserved_region = state
        .reserve_next_region::<512, 5, _>(&mut flash, &mut workspace)
        .unwrap();
    let stale_successor =
        read_free_pointer_successor::<512, 5, _>(&mut flash, state.metadata(), reserved_region)
            .unwrap();

    assert_eq!(reserved_region, 1);
    assert_eq!(stale_successor, Some(2));
    assert!(!state
        .region_is_on_free_list::<512, 5, _>(&mut flash, reserved_region)
        .unwrap());
    assert_eq!(state.ready_region(), Some(reserved_region));
}

//= spec/ring.md#storage-requirements
//# `RING-STORAGE-007` The free-pointer footer of a region MUST NOT be
//# written while that region is allocated for live use.
#[test]
fn committed_region_writes_do_not_write_a_live_free_pointer_footer() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<512, 5, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    let region_index = state
        .reserve_next_region::<512, 5, _>(&mut flash, &mut workspace)
        .unwrap();

    flash.clear_operations();
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V1_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();

    assert_eq!(
        flash.operations(),
        &[
            MockOperation::EraseRegion { region_index },
            MockOperation::WriteRegion {
                region_index,
                offset: 0,
                len: Header::ENCODED_LEN,
            },
            MockOperation::WriteRegion {
                region_index,
                offset: Header::ENCODED_LEN,
                len: 3,
            },
            MockOperation::Sync,
        ]
    );
}

//= spec/ring.md#storage-requirements
//# `RING-STORAGE-008` After a region is durably reachable from the
//# free-list chain, that region MUST NOT be erased until it is allocated
//# for reuse.
#[test]
fn free_regions_are_erased_only_when_reused() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    flash.clear_operations();
    let region_index = state
        .reserve_next_region::<512, 5, _>(&mut flash, &mut workspace)
        .unwrap();
    assert!(!flash
        .operations()
        .contains(&MockOperation::EraseRegion { region_index }));

    flash.clear_operations();
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V1_FORMAT,
            &[9],
        )
        .unwrap();
    assert!(flash
        .operations()
        .contains(&MockOperation::EraseRegion { region_index }));
}

//= spec/ring.md#storage-requirements
//# `RING-STORAGE-009` A WAL region MUST have `collection_id = 0` and
//# `collection_format = wal_v1`.
#[test]
fn initialized_wal_regions_use_reserved_wal_header_fields() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    initialize_wal_region::<128, 4, _>(&mut flash, metadata, 1, 7, 0).unwrap();

    let header = read_header_from_flash::<128, 4, _>(&mut flash, 1).unwrap();
    assert_eq!(header.sequence, 7);
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, WAL_V1_FORMAT);

    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    flash
        .read_region(1, Header::ENCODED_LEN, &mut prologue_bytes)
        .unwrap();
    let prologue = WalRegionPrologue::decode(&prologue_bytes, metadata.region_count).unwrap();
    assert_eq!(prologue.wal_head_region_index, 0);
}

//= spec/ring.md#wal-reclaim-eligibility
//# `RING-WAL-RECLAIM-POST-007` The reclaimed region MUST be erased
//# before reuse.
#[test]
fn initialized_wal_region_erases_the_reclaimed_region_before_reuse() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    flash.clear_operations();
    initialize_wal_region::<128, 4, _>(&mut flash, metadata, 1, 7, 0).unwrap();

    assert_eq!(
        flash.operations(),
        &[
            MockOperation::EraseRegion { region_index: 1 },
            MockOperation::WriteRegion {
                region_index: 1,
                offset: 0,
                len: Header::ENCODED_LEN,
            },
            MockOperation::WriteRegion {
                region_index: 1,
                offset: Header::ENCODED_LEN,
                len: WalRegionPrologue::ENCODED_LEN,
            },
            MockOperation::Sync,
        ]
    );
}

#[test]
fn normal_append_rejects_when_it_would_consume_rotation_reserve() {
    let mut flash = MockFlash::<256, 4, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    state
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    loop {
        match state.append_update::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            &[1, 2, 3, 4, 5, 6, 7, 8],
        ) {
            Ok(()) => continue,
            Err(StorageRuntimeError::WalRotationRequired) => break,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    let next_region = state
        .append_wal_rotation_start::<256, 4, _>(&mut flash, &mut workspace)
        .unwrap();
    state
        .append_wal_rotation_finish::<256, 4, _>(&mut flash, &mut workspace, next_region)
        .unwrap();
    state
        .append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[9])
        .unwrap();
}
