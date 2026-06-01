use super::*;
use crate::wal_record::{encode_record_into, encoded_record_len};
use crate::MockFlash;
use crate::StorageWorkspace;
use crate::{
    CollectionId, CollectionType, Header, MapFrontier, MockOperation, StartupCollectionBasis,
    Storage, StorageFormatConfig, WalRecord, WalRegionPrologue,
};
use core::mem::size_of;
use heapless::Vec;

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

fn append_exact_fill_update_record<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    metadata: StorageMetadata,
    region_index: u32,
    collection_id: CollectionId,
) {
    let offset = metadata.wal_record_area_offset().unwrap();
    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    let payload = [0u8; REGION_SIZE];
    let payload_len = (0..=payload.len())
        .find(|payload_len| {
            encoded_record_len(
                WalRecord::Update {
                    collection_id,
                    payload: &payload[..*payload_len],
                },
                metadata,
                &mut physical,
                &mut logical,
            )
            .is_ok_and(|encoded_len| offset + encoded_len == REGION_SIZE)
        })
        .expect("update payload length should exactly fill the WAL region");

    let next_offset = append_wal_record(
        flash,
        metadata,
        region_index,
        offset,
        WalRecord::Update {
            collection_id,
            payload: &payload[..payload_len],
        },
    );
    assert_eq!(next_offset, REGION_SIZE);
}

fn fill_until_append_reserve_requires_rotation<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    state: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    record: WalRecord<'_>,
) {
    for _ in 0..128 {
        match state.ensure_append_reserve::<REGION_SIZE, REGION_COUNT, _>(workspace, flash, record)
        {
            Ok(()) => state
                .append_update::<REGION_SIZE, REGION_COUNT, _>(
                    flash,
                    workspace,
                    CollectionId(7),
                    &[1, 2, 3, 4],
                )
                .unwrap(),
            Err(StorageRuntimeError::WalRotationRequired) => return,
            Err(other) => panic!("unexpected append-reserve error: {other:?}"),
        }
    }

    panic!("append reserve did not reach the WAL rotation window");
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
        .reserve_next_region::<512, 6, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    state
        .write_committed_region::<512, 6, _>(
            &mut flash,
            first_region,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
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
        .reserve_next_region::<512, 6, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    state
        .write_committed_region::<512, 6, _>(
            &mut flash,
            second_region,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
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

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-002` Write `StorageMetadata`
//# (`storage_version`, `region_size`, `region_count`,
//# `min_free_regions`, `wal_write_granule`, `erased_byte`,
//# `wal_record_magic`, `metadata_checksum`) and sync metadata.
#[test]
fn requirement_format_writes_metadata_before_reopening_the_fresh_store() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    let metadata = flash.metadata().copied().unwrap();

    assert_eq!(
        flash.operations().first(),
        Some(&MockOperation::WriteMetadata)
    );
    assert_eq!(state.metadata(), metadata);
    assert_eq!(metadata.region_size, 128);
    assert_eq!(metadata.region_count, 4);
    assert_eq!(metadata.min_free_regions, 1);
    assert_eq!(metadata.wal_write_granule, 8);
    assert_eq!(metadata.erased_byte, 0xff);
    assert_eq!(metadata.wal_record_magic, 0xa5);
}

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-POST-001` WAL head and WAL tail MUST both be
//# region `0`.
#[test]
fn requirement_format_starts_with_region_zero_as_wal_head_and_tail() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 0);
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-010` The durable free list MUST be FIFO so allocations
//# consume the oldest free regions first.
#[test]
fn requirement_reserve_next_region_consumes_the_oldest_free_regions_first() {
    let progress = committed_region_sequence_progress();

    assert_eq!(progress.first_region, 1);
    assert_eq!(progress.second_region, 2);
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-011` Any operation that writes a newly allocated region
//# MUST first durably reserve that region with
//# `alloc_begin(collection_id, region_index, free_list_head_after)`.
#[test]
fn requirement_committed_region_write_uses_a_region_previously_reserved_by_alloc_begin() {
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
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
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
            crate::MAP_REGION_V2_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-063` Committed region writes MUST accept a payload that exactly fills
//# committed payload capacity and persist the full payload bytes.
#[test]
fn requirement_write_committed_region_accepts_payload_that_exactly_fills_committed_capacity() {
    const REGION_SIZE: usize = 256;
    const PAYLOAD_CAPACITY: usize =
        REGION_SIZE - Header::ENCODED_LEN - FreePointerFooter::ENCODED_LEN;

    let mut flash = MockFlash::<REGION_SIZE, 5, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut state = format::<REGION_SIZE, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    let payload = [0x5au8; PAYLOAD_CAPACITY];

    let region_index = state
        .reserve_next_region::<REGION_SIZE, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();

    state
        .write_committed_region::<REGION_SIZE, 5, _>(
            &mut flash,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
            &payload,
        )
        .unwrap();

    let mut stored = [0u8; PAYLOAD_CAPACITY];
    flash
        .read_region(region_index, Header::ENCODED_LEN, stored.len(), |bytes| {
            stored.copy_from_slice(bytes);
        })
        .unwrap();
    assert_eq!(stored, payload);
}

//= spec/ring/08-durability-formatting.md#durability-and-crash-semantics
//= type=test
//# `RING-ALLOC-001` Any operation that writes a newly allocated region
//# MUST first make
//# `alloc_begin(collection_id, region_index, free_list_head_after)`
//# durable.
#[test]
fn requirement_committed_region_write_waits_for_alloc_begin_sync() {
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
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
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

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-REPLAY-ASSUME-004` Any operation that consumes a free-list
//# head MUST first make the allocator advance durable with
//# `alloc_begin(collection_id, region_index, free_list_head_after)`.
#[test]
fn requirement_reopen_after_alloc_begin_recovers_the_advanced_allocator_state() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let region_index = state
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    let reopened = open::<512, 5, _, 8, 4>(&mut flash).unwrap();

    assert_eq!(reopened.ready_region(), Some(region_index));
    assert_eq!(reopened.last_free_list_head(), Some(2));
    assert_eq!(reopened.free_list_tail(), Some(4));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-064` Formatting storage MUST return fresh runtime state with metadata, WAL
//# head/tail, allocator, and collection fields initialized.
#[test]
fn requirement_format_returns_fresh_runtime_state() {
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

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-POST-002` A user collection durable head MUST
//# NOT exist after formatting.
#[test]
fn requirement_format_starts_with_no_user_collection_durable_head() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert!(state.collections().is_empty());
    assert_eq!(state.tracked_user_collection_count(), 0);
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-003` Borromean MUST reserve `collection_id = 0` for the
//# WAL, and all user collection identifiers MUST be nonzero stable 64-bit
//# nonces that are never recycled.
#[test]
fn requirement_user_collection_ids_are_nonzero_u64_values_and_are_not_recycled() {
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

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-004` Borromean core MUST reserve
//# `collection_type = wal` for `collection_id = 0`, and user collections
//# MUST NOT use that collection type.
#[test]
fn requirement_wal_collection_type_is_reserved_for_collection_id_zero() {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-065` WAL record visitation MUST report snapshot and update records after a
//# new collection in durable WAL order.
#[test]
fn requirement_visit_wal_records_reports_snapshot_and_update_records() {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-066` Opening storage MUST return replayed runtime state with append
//# offset, max sequence, collection type, committed basis, and pending update count.
#[test]
fn requirement_open_returns_replayed_collection_runtime_state() {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-067` Opening storage MUST complete transaction cleanup for regions already
//# on the free list and clear incomplete transaction state.
#[test]
fn requirement_open_completes_reclaims_already_on_the_free_list() {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-068` Opening storage MUST discard incomplete cleanup records for regions
//# still reachable from live collection state.
#[test]
fn requirement_open_discards_pending_reclaims_for_still_live_regions() {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-069` Appending a new collection and update MUST refresh runtime collection
//# state and pending update count.
#[test]
fn requirement_append_new_collection_and_update_refresh_runtime_state() {
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

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-005` For user collections, append-time validity MUST
//# require a successful earlier
//# `new_collection(collection_id, collection_type)` before any later
//# record for that collection may be appended.
#[test]
fn requirement_append_update_requires_a_prior_new_collection_record() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert_eq!(
        state.append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[1, 2]),
        Err(StorageRuntimeError::UnknownCollection(CollectionId(7)))
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-070` Appending a snapshot MUST move the collection to WAL snapshot basis
//# and clear prior pending updates.
#[test]
fn requirement_append_snapshot_resets_pending_updates() {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-071` Appending head and drop records MUST refresh runtime basis to
//# committed region and then dropped tombstone while reducing tracked live collection count.
#[test]
fn requirement_append_head_and_drop_refresh_runtime_state() {
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

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-007` A `drop_collection(collection_id)` record that is
//# durable MUST tombstone that collection, MUST forbid later WAL
//# records for that `collection_id`, and MUST make older durable bytes
//# reclaimable once they are no longer physically reachable from live
//# state.
#[test]
fn requirement_drop_collection_tombstones_the_collection_forbids_later_records_and_starts_reclaim()
{
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

    let reclaim = state
        .drop_collection_and_begin_reclaim::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7))
        .unwrap();

    assert_eq!(reclaim, Some(2));
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
    assert_eq!(state.pending_reclaims(), &[2]);
    assert_eq!(state.tracked_user_collection_count(), 0);
    assert_eq!(
        state.append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[9]),
        Err(StorageRuntimeError::DroppedCollection(CollectionId(7)))
    );
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-009` Any multi-step collection operation that commits a
//# new durable basis and frees old regions MUST be tracked as a
//# collection-scoped WAL transaction with durable begin, commit, cleanup,
//# and terminal markers.
#[test]
fn requirement_append_alloc_and_reclaim_methods_refresh_runtime_state() {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-072` Appending WAL recovery MUST clear pending recovery boundary and
//# advance append offset; appending allocator cleanup records MUST refresh allocator head and tail.
#[test]
fn requirement_append_free_list_head_and_wal_recovery_refresh_runtime_state() {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-133` Control-record appends MUST refresh the in-memory runtime state
//# without reopening and replaying the WAL.
#[test]
fn requirement_control_record_appends_refresh_runtime_without_reopen() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
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
    init_user_region_header(
        &mut flash,
        4,
        17,
        CollectionId(7),
        crate::MAP_REGION_V2_FORMAT,
    );

    flash.clear_operations();
    state
        .append_head::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            4,
        )
        .unwrap();
    assert_eq!(
        flash
            .operations()
            .iter()
            .filter(|operation| matches!(operation, MockOperation::ReadMetadata))
            .count(),
        0
    );
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(4)
    );
    assert_eq!(state.max_seen_sequence(), 17);

    flash.clear_operations();
    state
        .append_free_list_head::<512, 6, _>(&mut flash, &mut workspace, Some(2))
        .unwrap();
    assert_eq!(
        flash
            .operations()
            .iter()
            .filter(|operation| matches!(operation, MockOperation::ReadMetadata))
            .count(),
        0
    );
    let reopened = open::<512, 6, _, 8, 4>(&mut flash).unwrap();
    assert_eq!(state.last_free_list_head(), reopened.last_free_list_head());
    assert_eq!(state.free_list_tail(), reopened.free_list_tail());
    assert_eq!(state.max_seen_sequence(), reopened.max_seen_sequence());
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-134` Completing transaction cleanup MUST refresh the free-list tail from
//# footers, not by reopening the store.
#[test]
fn requirement_reclaim_end_refreshes_free_list_tail_without_reopen() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 6, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let reclaimed = state
        .reserve_next_region::<512, 6, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    state
        .append_reclaim_begin::<512, 6, _>(&mut flash, &mut workspace, reclaimed)
        .unwrap();

    flash.clear_operations();
    state
        .complete_pending_reclaim::<512, 6, _>(&mut flash, &mut workspace, reclaimed)
        .unwrap();
    assert_eq!(
        flash
            .operations()
            .iter()
            .filter(|operation| matches!(operation, MockOperation::ReadMetadata))
            .count(),
        0
    );
    assert!(state.pending_reclaims().is_empty());
    assert_eq!(state.free_list_tail(), Some(reclaimed));

    let reopened = open::<512, 6, _, 8, 4>(&mut flash).unwrap();
    assert_eq!(state.last_free_list_head(), reopened.last_free_list_head());
    assert_eq!(state.free_list_tail(), reopened.free_list_tail());
    assert_eq!(state.pending_reclaims(), reopened.pending_reclaims());
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-073` WAL rotation start/finish appends MUST reserve the next free region,
//# advance allocator state, then move WAL tail to the new region and clear ready allocation state.
#[test]
fn requirement_append_rotation_start_and_finish_move_to_new_tail() {
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

    let reopened = open::<128, 4, 128, 8, 4>(&mut flash).unwrap();
    assert_eq!(reopened.wal_head(), state.wal_head());
    assert_eq!(reopened.wal_tail(), state.wal_tail());
    assert_eq!(reopened.wal_append_offset(), state.wal_append_offset());
    assert_eq!(reopened.ready_region(), state.ready_region());
    assert_eq!(reopened.last_free_list_head(), state.last_free_list_head());
    assert_eq!(reopened.free_list_tail(), state.free_list_tail());
    assert_eq!(reopened.max_seen_sequence(), state.max_seen_sequence());
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-003` Each newly allocated region, whether for a user
//# collection or a newly initialized WAL region, MUST use
//# `sequence = max_seen_sequence + 1`, after which that value becomes the
//# new in-memory `max_seen_sequence`.
#[test]
fn requirement_committed_region_allocations_advance_sequence_from_max_seen_sequence() {
    let progress = committed_region_sequence_progress();

    assert_eq!(progress.first_region, 1);
    assert_eq!(progress.first_sequence, 1);
    assert_eq!(progress.max_seen_after_first, progress.first_sequence);

    assert_eq!(progress.second_region, 2);
    assert_eq!(progress.second_sequence, progress.max_seen_after_first + 1);
    assert_eq!(progress.max_seen_after_second, progress.second_sequence);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-074` WAL rotation MUST initialize the new WAL region at
//# `max_seen_sequence + 1` and update runtime max_seen_sequence.
#[test]
fn requirement_wal_rotation_initializes_the_next_wal_region_at_max_seen_sequence_plus_one() {
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

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-004` Successful later region writes MUST preserve a
//# strictly monotonic `sequence` ordering even if crashes or abandoned
//# allocations leave gaps.
#[test]
fn requirement_later_region_writes_keep_sequence_numbers_strictly_monotonic() {
    let progress = committed_region_sequence_progress();

    assert!(progress.first_sequence < progress.second_sequence);
    assert!(progress.max_seen_after_first < progress.max_seen_after_second);
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-006` A free region MUST be defined by membership in the
//# durable free-list chain rather than by a distinct on-disk header
//# encoding.
#[test]
fn requirement_free_region_membership_is_defined_by_the_free_list_chain() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let reserved_region = state
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();

    assert_eq!(reserved_region, 1);
    assert!(!state
        .region_is_on_free_list::<512, 5, _>(&mut flash, reserved_region)
        .unwrap());
    assert!(state
        .region_is_on_free_list::<512, 5, _>(&mut flash, 2)
        .unwrap());
}

//= spec/ring/05-disk-format.md#free-pointer-footer
//= type=test
//# `RING-FREE-006` While a region is allocated for live use, the bytes
//# in its free-pointer footer are uninterpreted stale data and MUST NOT
//# be used to infer free-list membership.
#[test]
fn requirement_stale_footer_bytes_do_not_make_a_reserved_region_free() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let reserved_region = state
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
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

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=todo
//# `RING-STARTUP-RESULT-008` Transaction terminal records written during recovery, if recovery
//# needed to close an incomplete interval
#[test]
fn todo_stage_ready_region_detaches_ready_region_and_allows_next_allocation() {
    let mut flash = MockFlash::<512, 5, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let first_region = state
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            first_region,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();
    state
        .stage_ready_region::<512, 5, _>(&mut flash, &mut workspace, first_region)
        .unwrap();

    assert_eq!(state.ready_region(), None);
    assert_eq!(state.staged_regions(), &[first_region]);

    let second_region = state
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    assert_ne!(second_region, first_region);
    assert_eq!(state.ready_region(), Some(second_region));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-075` Reopening with incomplete transaction allocation state MUST recover
//# allocated regions and leave no abandoned ready regions live.
#[test]
fn requirement_staged_regions_are_reclaimed_on_reopen_when_uncommitted() {
    let mut flash = MockFlash::<512, 5, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    let region_index = state
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();
    state
        .stage_ready_region::<512, 5, _>(&mut flash, &mut workspace, region_index)
        .unwrap();

    let reopened = open::<512, 5, _, 8, 4>(&mut flash).unwrap();
    assert_eq!(reopened.ready_region(), None);
    assert!(reopened.staged_regions().is_empty());
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-076` Allocation cleanup MUST reject region indexes that do not match the
//# current ready allocation state.
#[test]
fn requirement_stage_ready_region_rejects_non_ready_region() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert_eq!(
        state.stage_ready_region::<512, 5, _>(&mut flash, &mut workspace, 1),
        Err(StorageRuntimeError::InvalidStageRegion {
            region_index: 1,
            ready_region: None,
        })
    );
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-007` The free-pointer footer of a region MUST NOT be
//# written while that region is allocated for live use.
#[test]
fn requirement_committed_region_writes_do_not_write_a_live_free_pointer_footer() {
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
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();

    flash.clear_operations();
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
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

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-008` After a region is durably reachable from the
//# free-list chain, that region MUST NOT be erased until it is allocated
//# for reuse.
#[test]
fn requirement_free_regions_are_erased_only_when_reused() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    flash.clear_operations();
    let region_index = state
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
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
            crate::MAP_REGION_V2_FORMAT,
            &[9],
        )
        .unwrap();
    assert!(flash
        .operations()
        .contains(&MockOperation::EraseRegion { region_index }));
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-009` A WAL region MUST have `collection_id = 0` and
//# `collection_format = wal_v1`.
#[test]
fn requirement_initialized_wal_regions_use_reserved_wal_header_fields() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    initialize_wal_region::<128, 4, _>(&mut flash, metadata, 1, 7, 0).unwrap();

    let header = read_header_from_flash::<128, 4, _>(&mut flash, 1).unwrap();
    assert_eq!(header.sequence, 7);
    assert_eq!(header.collection_id, CollectionId(0));
    assert_eq!(header.collection_format, WAL_V1_FORMAT);

    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    flash
        .read_region(1, Header::ENCODED_LEN, prologue_bytes.len(), |bytes| {
            prologue_bytes.copy_from_slice(bytes)
        })
        .unwrap();
    let prologue = WalRegionPrologue::decode(&prologue_bytes, metadata.region_count).unwrap();
    assert_eq!(prologue.wal_head_region_index, 0);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-007` The reclaimed region MUST be erased
//# before reuse.
#[test]
fn requirement_initialized_wal_region_erases_the_reclaimed_region_before_reuse() {
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

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-REPLAY-ASSUME-001` A WAL region MUST be erased before reuse.
#[test]
fn requirement_initialized_wal_region_erases_the_wal_region_before_reuse() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    flash.clear_operations();
    initialize_wal_region::<128, 4, _>(&mut flash, metadata, 1, 7, 0).unwrap();

    assert!(matches!(
        flash.operations().first(),
        Some(MockOperation::EraseRegion { region_index: 1 })
    ));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-077` Normal WAL appends MUST reject writes that would consume rotation
//# reserve until WAL rotation completes, after which appends may continue.
#[test]
fn requirement_normal_append_rejects_when_it_would_consume_rotation_reserve() {
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

    for _ in 0..64 {
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

    assert!(matches!(
        state.append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[1]),
        Err(StorageRuntimeError::WalRotationRequired)
    ));

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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-107` Internal WAL rotation with a large pending record MUST bridge an
//# early rotation-window gap without surfacing InvalidRotationWindow to the caller.
#[test]
fn requirement_internal_rotation_bridges_early_window_gap_for_large_record() {
    let mut flash = MockFlash::<256, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 6, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    state
        .append_new_collection::<256, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let payload = [0u8; 64];
    let large_record = WalRecord::Snapshot {
        collection_id: CollectionId(7),
        collection_type: CollectionType::MAP_CODE,
        payload: &payload,
    };
    fill_until_append_reserve_requires_rotation::<256, 6, _, 8, 4>(
        &mut state,
        &mut flash,
        &mut workspace,
        large_record,
    );

    let next_region = state.last_free_list_head().unwrap();
    let free_list_head_after =
        read_free_pointer_successor::<256, 6, _>(&mut flash, state.metadata(), next_region)
            .unwrap();
    let reserves = state
        .rotation_reserves::<256, 6>(&mut workspace, next_region, free_list_head_after)
        .unwrap();
    let remaining_after_alloc_begin = 256 - (state.wal_append_offset() + reserves.alloc_begin_len);
    assert!(remaining_after_alloc_begin >= reserves.rotation_reserve);

    let previous_tail = state.wal_tail();
    state
        .append_record_with_rotation::<256, 6, _>(&mut flash, &mut workspace, large_record)
        .unwrap();

    assert_ne!(state.wal_tail(), previous_tail);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-078` WAL rotation start MUST reject calls made before the WAL tail has
//# entered the rotation window.
#[test]
fn requirement_append_wal_rotation_start_rejects_when_called_before_rotation_window() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();

    assert!(matches!(
        state.append_wal_rotation_start::<256, 4, _>(&mut flash, &mut workspace),
        Err(StorageRuntimeError::InvalidRotationWindow {
            remaining_after,
            rotation_reserve,
            ..
        }) if remaining_after >= rotation_reserve
    ));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-079` Head append room checks MUST perform WAL rotation when the current
//# tail lacks room for a head record.
#[test]
fn requirement_ensure_head_append_room_with_rotation_rotates_when_tail_lacks_head_room() {
    let mut flash = MockFlash::<256, 6, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 6, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    state
        .append_new_collection::<256, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let target_region = state.last_free_list_head().unwrap();
    fill_until_append_reserve_requires_rotation(
        &mut state,
        &mut flash,
        &mut workspace,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::MAP_CODE,
            region_index: target_region,
        },
    );

    let tail_before = state.wal_tail();
    state
        .ensure_head_append_room_with_rotation::<256, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            target_region,
        )
        .unwrap();
    assert_ne!(state.wal_tail(), tail_before);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-080` Transaction cleanup append room checks MUST reject cleanup when
//# allocator state no longer matches the target region.
#[test]
fn requirement_ensure_stage_region_append_room_with_rotation_rotates_when_tail_lacks_stage_room() {
    let mut flash = MockFlash::<256, 6, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 6, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    state
        .append_new_collection::<256, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let target_region = state.last_free_list_head().unwrap();
    state.last_free_list_head = Some(99);

    assert!(state
        .ensure_stage_region_append_room_with_rotation::<256, 6, _>(
            &mut flash,
            &mut workspace,
            target_region,
        )
        .is_err());
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-081` Encoded append reserve checks for alloc_begin MUST require a free
//# region and return WalRotationRequired when none remains.
#[test]
fn requirement_ensure_encoded_append_reserve_rejects_alloc_begin_when_no_free_region_remains() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let mut state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    state.last_free_list_head = None;

    assert_eq!(state.last_free_list_head(), None);
    assert_eq!(
        state.ensure_encoded_append_reserve::<128, 4, _>(&mut workspace, &mut flash, 1, true),
        Err(StorageRuntimeError::WalRotationRequired)
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-082` Encoded append reserve checks MUST allow alloc_begin when the tail
//# has exactly the rotation reserve plus encoded record length remaining.
#[test]
fn requirement_ensure_encoded_append_reserve_accepts_alloc_begin_at_exact_rotation_reserve_boundary(
) {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    let next_region = state.last_free_list_head().unwrap();
    let free_list_head_after =
        read_free_pointer_successor::<256, 4, _>(&mut flash, state.metadata(), next_region)
            .unwrap();
    let reserves = state
        .rotation_reserves::<256, 4>(&mut workspace, next_region, free_list_head_after)
        .unwrap();
    let encoded_len = 1;
    state.wal_append_offset = 256 - reserves.rotation_reserve - encoded_len;

    assert_eq!(
        state.ensure_encoded_append_reserve::<256, 4, _>(
            &mut workspace,
            &mut flash,
            encoded_len,
            true,
        ),
        Ok(())
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-083` WAL-head reclaim classification MUST copy only head records that
//# still reference the retained live region and skip stale head records.
#[test]
fn requirement_classify_wal_head_reclaim_copies_only_the_retained_region_head() {
    let mut flash = MockFlash::<512, 5, 512>::new(0xff);
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
    let retained_region = state
        .reserve_next_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            retained_region,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();
    state
        .append_head::<512, 5, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            retained_region,
        )
        .unwrap();

    let original_collections = state.collections.clone();
    let mut active_collections = Vec::<CollectionId, 8>::new();
    assert_eq!(
        state
            .classify_wal_head_record_for_reclaim(
                &original_collections,
                &mut active_collections,
                WalRecord::Head {
                    collection_id: CollectionId(7),
                    collection_type: CollectionType::MAP_CODE,
                    region_index: retained_region,
                },
            )
            .unwrap(),
        WalHeadReclaimAction::CopyEncoded
    );
    assert_eq!(
        state
            .classify_wal_head_record_for_reclaim(
                &original_collections,
                &mut active_collections,
                WalRecord::Head {
                    collection_id: CollectionId(7),
                    collection_type: CollectionType::MAP_CODE,
                    region_index: retained_region + 1,
                },
            )
            .unwrap(),
        WalHeadReclaimAction::Skip
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-084` WAL-head reclaim classification MUST copy drop tombstones only for
//# collections that remain dropped and skip drops for live collections.
#[test]
fn requirement_classify_wal_head_reclaim_copies_only_retained_drop_tombstones() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut live_state = format::<256, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    live_state
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let live_collections = live_state.collections.clone();
    let mut active_collections = Vec::<CollectionId, 8>::new();
    assert_eq!(
        live_state
            .classify_wal_head_record_for_reclaim(
                &live_collections,
                &mut active_collections,
                WalRecord::DropCollection {
                    collection_id: CollectionId(7),
                },
            )
            .unwrap(),
        WalHeadReclaimAction::Skip
    );

    live_state
        .append_drop_collection::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7))
        .unwrap();
    let dropped_collections = live_state.collections.clone();
    assert_eq!(
        live_state
            .classify_wal_head_record_for_reclaim(
                &dropped_collections,
                &mut active_collections,
                WalRecord::DropCollection {
                    collection_id: CollectionId(7),
                },
            )
            .unwrap(),
        WalHeadReclaimAction::CopyEncoded
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-085` Foreground allocation headroom checks MUST reject allocations that
//# would consume the configured minimum free-region reserve.
#[test]
fn requirement_ensure_foreground_allocation_headroom_rejects_using_the_minimum_free_reserve() {
    let mut flash = MockFlash::<256, 5, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 5, _, 8, 4>(&mut flash, 3, 8, 0xa5).unwrap();
    let _reserved = state
        .reserve_next_region::<256, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();

    assert_eq!(state.free_region_count::<256, 5, _>(&mut flash), Ok(3));
    assert_eq!(
        state.ensure_foreground_allocation_headroom::<256, 5, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        ),
        Err(StorageRuntimeError::InsufficientFreeRegions {
            free_regions: 3,
            min_free_regions: 3,
        })
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-086` WAL-head reclaim copying MUST stop cleanly when a copied tail record
//# ends exactly at the region end.
#[test]
fn requirement_copy_live_wal_head_reclaim_state_stops_when_a_record_ends_at_region_end() {
    let mut flash = MockFlash::<128, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let mut state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    let metadata = state.metadata();
    append_exact_fill_update_record(&mut flash, metadata, state.wal_head(), CollectionId(77));
    let plan = WalHeadReclaimPlan::<8> {
        old_head: state.wal_head(),
        source_tail: state.wal_tail(),
        source_tail_append_offset: 128,
        original_collections: Vec::new(),
    };

    let mut active_collections = Vec::<CollectionId, 8>::new();
    state
        .copy_live_wal_head_reclaim_state::<128, 4, _>(
            &mut flash,
            &mut workspace,
            &plan,
            &mut active_collections,
            &mut crate::startup::StartupOpenPlan::empty(),
            #[cfg(feature = "perf-counters")]
            None,
        )
        .unwrap();
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-087` Live-state reachability checks MUST NOT parse non-map collection
//# heads as maps.
#[test]
fn requirement_region_reachable_from_live_state_does_not_parse_non_map_region_heads_as_maps() {
    let mut flash = MockFlash::<128, 4, 512>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(
        &mut flash,
        2,
        4,
        CollectionId(7),
        CollectionType::CHANNEL_CODE,
    );
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Head {
            collection_id: CollectionId(7),
            collection_type: CollectionType::CHANNEL_CODE,
            region_index: 2,
        },
    );

    let mut workspace = StorageWorkspace::<128>::new();
    let state = open::<128, 4, _, 8, 4>(&mut flash).unwrap();
    assert!(!state
        .region_reachable_from_live_state::<128, _>(&mut flash, &mut workspace, 3)
        .unwrap());
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-088` Live-state reachability checks MUST follow live map manifest heads to
//# referenced run regions.
#[test]
fn requirement_region_reachable_from_live_state_follows_map_head_references_to_run_regions() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 32;
    const MAX_INDEXES: usize = 128;
    const MAX_RUNS: usize = 16;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 16384>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, 8, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let collection_id = CollectionId(707);
    storage.create_map(collection_id).unwrap();

    let mut map_buffer = [0u8; 8192];
    let mut map = MapFrontier::<i32, i32, MAX_INDEXES, MAX_RUNS>::new(
        collection_id,
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for key in 0..100 {
        map.set(key, key * 10).unwrap();
    }
    storage
        .flush_map::<i32, i32, MAX_INDEXES, MAX_RUNS>(&mut map)
        .unwrap();

    let run_region = storage
        .with_io_workspace(|flash, _workspace| {
            (0..REGION_COUNT as u32).find(|region_index| {
                read_header_from_flash::<REGION_SIZE, REGION_COUNT, _>(flash, *region_index)
                    .is_ok_and(|header| {
                        header.collection_id == collection_id
                            && header.collection_format == crate::MAP_RUN_V2_FORMAT
                    })
            })
        })
        .expect("flush should write at least one map run region");

    assert!(storage
        .with_runtime_io_workspace(|runtime, flash, workspace| runtime
            .region_reachable_from_live_state::<REGION_SIZE, _>(flash, workspace, run_region))
        .unwrap());
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-089` Dropping a transaction-owned region in memory MUST remove only the
//# matching region and preserve other transaction recovery state.
#[test]
fn requirement_drop_staged_region_in_memory_removes_only_the_matching_region() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let mut state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    state.staged_regions.push(1).unwrap();
    state.staged_regions.push(2).unwrap();

    state.drop_staged_region_in_memory(1).unwrap();
    assert_eq!(state.staged_regions(), &[2]);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-090` WAL record visitation MUST process a tail record that ends exactly at
//# the append limit and then stop.
#[test]
fn requirement_visit_wal_records_stops_when_a_tail_record_ends_at_the_append_limit() {
    let mut flash = MockFlash::<128, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let mut state = format::<128, 4, _, 8, 4>(&mut flash, 1, 8, 0xa5).unwrap();
    let metadata = state.metadata();
    append_exact_fill_update_record(&mut flash, metadata, state.wal_tail(), CollectionId(77));
    state.wal_append_offset = 128;

    let mut visited = 0usize;
    state
        .visit_wal_records::<128, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
            assert!(matches!(
                record,
                WalRecord::Update {
                    collection_id: CollectionId(77),
                    ..
                }
            ));
            visited += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(visited, 1);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-091` WAL-chain membership checks MUST follow durable link targets to
//# determine whether a region belongs to the chain.
#[test]
fn requirement_wal_chain_contains_region_follows_the_durable_link_target() {
    let mut flash = MockFlash::<128, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        WalRecord::Link {
            next_region_index: 2,
            expected_sequence: 1,
        },
    );
    initialize_wal_region::<128, 4, _>(&mut flash, metadata, 2, 1, 0).unwrap();

    assert_eq!(
        wal_chain_contains_region::<128, _>(&mut flash, &mut workspace, metadata, 0, 2, 2),
        Ok(true)
    );
}
