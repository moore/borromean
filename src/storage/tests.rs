use super::*;
use core::mem::size_of;
use crate::wal_record::encode_record_into;
use crate::{CollectionId, CollectionType, Header, StartupCollectionBasis, WalRecord};
use crate::MockFlash;
use crate::StorageWorkspace;

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

fn append_wal_record<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    metadata: StorageMetadata,
    region_index: u32,
    offset: usize,
    record: WalRecord<'_>,
) -> usize {
    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    let used = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();
    flash.write_region(region_index, offset, &physical[..used]).unwrap();
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
    assert_eq!(state.collections()[0].basis(), StartupCollectionBasis::Region(2));
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

    assert_eq!(state.collections()[0].basis(), StartupCollectionBasis::Region(2));
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
    assert_eq!(state.collections()[0].basis(), StartupCollectionBasis::Empty);
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
