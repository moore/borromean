use super::*;
use crate::disk::FreeQueuePosition;
use crate::wal_record::{encode_record_into, encoded_record_len};
use crate::MockFlash;
use crate::StorageWorkspace;
use crate::{
    CollectionId, CollectionType, Header, MapFrontier, MockOperation, StartupCollectionBasis,
    Storage, StorageFormatConfig, WalRecord, WalRegionPrologue, WAL_V1_FORMAT,
};
use core::mem::size_of;
use heapless::Vec;

fn test_free_position(entry_index: u32) -> FreeQueuePosition {
    FreeQueuePosition {
        region_index: 1,
        entry_index,
    }
}

fn format<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    min_free_regions: u32,
    wal_write_granule: u32,
    wal_record_magic: u8,
) -> Result<StorageRuntime<8>, StorageRuntimeError> {
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    super::format::<REGION_SIZE, REGION_COUNT, _, 8>(
        flash,
        &mut workspace,
        min_free_regions,
        wal_write_granule,
        wal_record_magic,
    )
}

fn open<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
) -> Result<StorageRuntime<8>, StorageRuntimeError> {
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    super::open::<REGION_SIZE, REGION_COUNT, _, 8>(flash, &mut workspace)
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
>(
    state: &mut StorageRuntime<MAX_COLLECTIONS>,
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
    let mut state = format::<512, 6, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
            &mut workspace,
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
            &mut workspace,
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
//# `min_free_regions`, `transaction_log_count`,
//# `wal_write_granule`, `erased_byte`,
//# `wal_record_magic`, `metadata_checksum`) and sync metadata.
#[test]
fn requirement_format_writes_metadata_before_reopening_the_fresh_store() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
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
//# `RING-FORMAT-STORAGE-POST-001` Main WAL head and tail MUST both be
//# region `0`, and every transaction-log slot MUST start uninitialized.
#[test]
fn requirement_format_starts_with_region_zero_as_wal_head_and_tail() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 0);
    assert_eq!(StorageRuntime::<8>::SLOT_COUNT, 1);
    assert_eq!(state.metadata().transaction_log_count, 1);
    assert!(matches!(state.transaction_slots[0], TransactionSlot::Empty));
    assert!(state.retained_transaction_logs.is_empty());
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-010` The storage-private free-space collection MUST be FIFO so allocations
//# consume the oldest ready free regions first.
#[test]
fn requirement_reserve_next_region_consumes_the_oldest_free_regions_first() {
    let progress = committed_region_sequence_progress();

    assert_eq!(progress.first_region, 2);
    assert_eq!(progress.second_region, 3);
}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=test
//# `RING-WAL-PAYLOAD-016` `transaction_finished` Main-WAL-only record. The payload is
//# `transaction_log_id:u32, range:TransactionLogRange`. It records that the committed
//# transaction's cleanup and recovery obligations are complete, so transaction-log garbage
//# collection may release this reference when no retained record or active descriptor
//# points to the same range.
#[test]
fn requirement_finished_transaction_releases_slot_but_retains_log_reference() {
    let mut flash = MockFlash::<512, 6, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = super::format::<512, 6, _, 8>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    let collection_id = CollectionId(7);
    state
        .append_new_collection::<512, 6, _>(
            &mut flash,
            &mut workspace,
            collection_id,
            CollectionType::MAP_CODE,
        )
        .unwrap();

    state
        .begin_collection_transaction::<512, 6, _>(&mut flash, &mut workspace, collection_id)
        .unwrap();
    assert!(matches!(
        state.transaction_slots[0],
        TransactionSlot::Active { .. }
    ));

    state
        .commit_collection_transaction::<512, 6, _>(&mut flash, &mut workspace, collection_id)
        .unwrap();
    assert!(matches!(
        state.transaction_slots[0],
        TransactionSlot::Active { .. }
    ));
    assert_eq!(state.retained_transaction_logs.len(), 1);
    assert_eq!(
        state.retained_transaction_logs[0].outcome,
        TransactionLogOutcome::Committed
    );

    state
        .finish_collection_transaction::<512, 6, _>(&mut flash, &mut workspace, collection_id)
        .unwrap();
    assert!(matches!(state.transaction_slots[0], TransactionSlot::Empty));
    assert_eq!(state.retained_transaction_logs.len(), 1);
    assert_eq!(
        state.retained_transaction_logs[0].outcome,
        TransactionLogOutcome::Finished
    );

    state
        .begin_collection_transaction::<512, 6, _>(&mut flash, &mut workspace, collection_id)
        .unwrap();
    assert!(matches!(
        state.transaction_slots[0],
        TransactionSlot::Active { .. }
    ));
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-011` Main-WAL transaction-control and inline
//# transaction-control records are live while startup replay still needs
//# them to import a committed range, prove rollback completed, finish
//# committed cleanup, or keep a transaction-log range referenced for
//# garbage collection.
#[test]
fn requirement_wal_reclaim_preserves_committed_transaction_log_effects() {
    let mut flash = MockFlash::<512, 12, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = super::format::<512, 12, _, 8>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    let collection_id = CollectionId(7);
    state
        .append_new_collection::<512, 12, _>(
            &mut flash,
            &mut workspace,
            collection_id,
            CollectionType::MAP_CODE,
        )
        .unwrap();

    state
        .begin_collection_transaction::<512, 12, _>(&mut flash, &mut workspace, collection_id)
        .unwrap();
    let tx_log_region = match &state.transaction_slots[0] {
        TransactionSlot::Active { head_region, .. } => *head_region,
        TransactionSlot::Empty => panic!("transaction slot should be active"),
    };
    let mut reclaim_source_regions = Vec::<u32, 12>::new();
    let mut active_collections = Vec::<CollectionId, 8>::new();
    let mut reclaim_plan = WalHeadReclaimPlan::<8>::empty();
    let mut open_plan = StartupOpenPlan::<12, 8>::empty();
    let committed_region = state
        .reserve_next_region_for::<512, 12, _>(
            &mut flash,
            &mut workspace,
            collection_id,
            &mut reclaim_source_regions,
            &mut active_collections,
            &mut reclaim_plan,
            &mut open_plan,
        )
        .unwrap();
    state
        .write_committed_region::<512, 12, _>(
            &mut flash,
            &mut workspace,
            committed_region,
            collection_id,
            crate::MAP_REGION_V2_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();
    state
        .append_head::<512, 12, _>(
            &mut flash,
            &mut workspace,
            collection_id,
            CollectionType::MAP_CODE,
            committed_region,
        )
        .unwrap();
    state
        .commit_collection_transaction::<512, 12, _>(&mut flash, &mut workspace, collection_id)
        .unwrap();
    state
        .finish_collection_transaction::<512, 12, _>(&mut flash, &mut workspace, collection_id)
        .unwrap();

    state
        .rotate_wal_tail::<512, 12, _>(&mut flash, &mut workspace)
        .unwrap();
    state
        .reclaim_wal_head::<512, 12, _>(
            &mut flash,
            &mut workspace,
            &mut reclaim_source_regions,
            &mut active_collections,
            &mut reclaim_plan,
            &mut open_plan,
        )
        .unwrap();

    let mut reopened = open::<512, 12, _>(&mut flash).unwrap();
    assert_eq!(reopened.collections().len(), 1);
    assert_eq!(reopened.collections()[0].collection_id(), collection_id);
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Region(committed_region)
    );
    assert!(reopened
        .region_is_on_free_list::<512, 12, _>(&mut flash, tx_log_region)
        .unwrap());
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-011` Any operation that writes a newly allocated region MUST first durably
//# reserve that region with `allocate_region(region_index, allocation_head_after)` in an
//# enclosing transaction or privileged storage-core operation.
#[test]
fn requirement_committed_region_write_uses_a_region_previously_reserved_by_allocate_region() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();

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

    let mut saw_allocate_region = false;
    state
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
            if let WalRecord::AllocateRegion {
                region_index: alloc_region,
                allocation_head_after,
            } = record
            {
                if alloc_region == region_index {
                    assert_eq!(
                        allocation_head_after,
                        FreeQueuePosition {
                            region_index: 1,
                            entry_index: 1
                        }
                    );
                    saw_allocate_region = true;
                }
            }
            Ok(())
        })
        .unwrap();

    assert!(saw_allocate_region);
    state
        .write_committed_region::<512, 5, _>(
            &mut flash,
            &mut workspace,
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
    const REGION_SIZE: usize = 258;
    const ALIGNED_REGION_BOUNDARY: usize = REGION_SIZE - REGION_SIZE % 8;
    const PAYLOAD_CAPACITY: usize = ALIGNED_REGION_BOUNDARY - Header::ENCODED_LEN;

    let mut flash = MockFlash::<REGION_SIZE, 5, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut state = format::<REGION_SIZE, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();
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
            &mut workspace,
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

    let too_large = [0x6bu8; PAYLOAD_CAPACITY + 1];
    assert!(matches!(
        state.write_committed_region::<REGION_SIZE, 5, _>(
            &mut flash,
            &mut workspace,
            region_index,
            CollectionId(7),
            crate::MAP_REGION_V2_FORMAT,
            &too_large,
        ),
        Err(StorageRuntimeError::CommittedRegionTooLarge {
            payload_len,
            capacity,
        }) if payload_len == too_large.len() && capacity == PAYLOAD_CAPACITY
    ));
}

//= spec/ring/08-durability-formatting.md#durability-and-crash-semantics
//= type=test
//# `RING-ALLOC-001` Any operation that writes a newly allocated region MUST first make
//# `allocate_region(region_index, allocation_head_after)` durable in a full transaction,
//# bounded inline transaction, or privileged storage-core operation.
#[test]
fn requirement_committed_region_write_waits_for_allocate_region_sync() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
            &mut workspace,
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
        "expected allocate_region WAL write before sync"
    );
}

//= spec/ring/04-wal-records.md#assumptions-for-replay-correctness
//= type=test
//# `RING-REPLAY-ASSUME-004` Any operation that consumes a ready free-space entry MUST first
//# make `allocate_region(region_index, allocation_head_after)` durable in the main WAL, in
//# an inline transaction, or in a reachable transaction-log range. The log segment prologue
//# supplies the free-space cursor checkpoint before later complete allocator commands are
//# applied.
#[test]
fn requirement_reopen_after_allocate_region_recovers_the_advanced_allocator_state() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
    let reopened = open::<512, 5, _>(&mut flash).unwrap();

    assert_eq!(reopened.ready_region(), None);
    assert_eq!(reopened.ready_free_region(), Some(3));
    assert_eq!(reopened.free_space_tail_region(), Some(4));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-064` Formatting storage MUST return fresh runtime state with
//# metadata, WAL head/tail, free-space cursors, and collection fields initialized.
#[test]
fn requirement_format_returns_fresh_runtime_state() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

    assert_eq!(state.metadata().region_count, 4);
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 0);
    assert_eq!(state.ready_free_region(), Some(2));
    assert_eq!(state.free_space_tail_region(), Some(3));
    assert_eq!(state.ready_region(), None);
    assert!(state.collections().is_empty());
}

//= spec/ring/08-durability-formatting.md#format-storage-on-disk-initialization
//= type=test
//# `RING-FORMAT-STORAGE-POST-002` A user collection durable head MUST
//# NOT exist after formatting.
#[test]
fn requirement_format_starts_with_no_user_collection_durable_head() {
    let mut flash = MockFlash::<128, 4, 64>::new(0xff);
    let state = format::<128, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
    let mut state = format::<256, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
        StorageRuntime::<8>::validate_supported_head_collection_type(
            CollectionId(0),
            CollectionType::WAL_CODE,
        ),
        Ok(())
    );
    assert_eq!(
        StorageRuntime::<8>::validate_supported_head_collection_type(
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
    let mut state = format::<256, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
    let mut flash = MockFlash::<512, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<512, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    state
        .append_snapshot::<512, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
            &[1, 2, 3],
        )
        .unwrap();
    state
        .append_update::<512, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[9])
        .unwrap();

    let mut seen = [crate::WalRecordType::WalRecovery; 3];
    let mut count = 0usize;
    state
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
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
//# `RING-IMPL-REGRESSION-066` Opening storage MUST return replayed runtime state with
//# append offset, max sequence, collection type, committed basis, pending update count, and
//# free-space cursors.
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

    let state = open::<256, 4, _>(&mut flash).unwrap();

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
//# `RING-IMPL-REGRESSION-067` Opening storage MUST complete transaction cleanup for regions
//# already present in the free-space collection and clear incomplete transaction state.
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
        WalRecord::FreeRegion {
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 3,
            },
        },
    );

    let state = open::<256, 4, _>(&mut flash).unwrap();

    assert_eq!(state.ready_region(), None);
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.free_space_tail_region(), Some(2));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-068` Opening storage MUST discard incomplete cleanup records for regions
//# still reachable from live collection state.
#[test]
fn requirement_open_ignores_free_region_records_for_still_live_regions() {
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
        WalRecord::FreeRegion {
            region_index: 2,
            append_tail_after: FreeQueuePosition {
                region_index: 1,
                entry_index: 1,
            },
        },
    );

    let state = open::<256, 4, _>(&mut flash).unwrap();

    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Region(2)
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-069` Appending a new collection and update MUST refresh runtime collection
//# state and pending update count.
#[test]
fn requirement_append_new_collection_and_update_refresh_runtime_state() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
    let mut state = format::<256, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
    let mut flash = MockFlash::<512, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<512, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    state
        .append_update::<512, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[1])
        .unwrap();
    state
        .append_snapshot::<512, 4, _>(
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
    let mut flash = MockFlash::<512, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

    state
        .append_new_collection::<512, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(7), 1);
    state
        .append_head::<512, 4, _>(
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
        .append_drop_collection::<512, 4, _>(&mut flash, &mut workspace, CollectionId(7))
        .unwrap();
    assert_eq!(
        state.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
    assert_eq!(state.tracked_user_collection_count(), 0);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-134` Completing transaction cleanup MUST refresh `append_tail`
//# from `free_region(region_index, append_tail_after)` without reopening the store.
#[test]
fn requirement_free_region_refreshes_free_space_tail_region_without_reopen() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 6, _>(&mut flash, 1, 8, 0xa5).unwrap();

    let reclaimed = state.ready_free_region().unwrap();
    state
        .append_allocate_region_for_test::<512, 6, _>(&mut flash, &mut workspace, reclaimed)
        .unwrap();
    flash.erase_region(reclaimed).unwrap();
    flash.sync().unwrap();

    flash.clear_operations();
    state
        .append_free_region::<512, 6, _>(&mut flash, &mut workspace, CollectionId(0), reclaimed)
        .unwrap();
    assert_eq!(
        flash
            .operations()
            .iter()
            .filter(|operation| matches!(operation, MockOperation::ReadMetadata))
            .count(),
        0
    );
    assert_eq!(state.free_space_tail_region(), Some(reclaimed));

    let reopened = open::<512, 6, _>(&mut flash).unwrap();
    assert_eq!(state.ready_free_region(), reopened.ready_free_region());
    assert_eq!(
        state.free_space_tail_region(),
        reopened.free_space_tail_region()
    );
}

//= spec/ring/07-reclaim.md#free-region
//= type=test
//# `RING-FREE-REGION-003` Append and sync
//# `free_region(region_index, append_tail_after)`.
#[test]
fn requirement_free_region_rejects_written_footer_before_linking_tail() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 6, _>(&mut flash, 1, 8, 0xa5).unwrap();

    let reclaimed = state.ready_free_region().unwrap();
    state
        .append_allocate_region_for_test::<512, 6, _>(&mut flash, &mut workspace, reclaimed)
        .unwrap();

    flash.clear_operations();
    state
        .append_free_region::<512, 6, _>(&mut flash, &mut workspace, CollectionId(0), reclaimed)
        .unwrap();

    assert_eq!(state.free_space_tail_region(), Some(reclaimed));
    assert!(flash.operations().iter().any(|operation| {
        matches!(
            operation,
            MockOperation::WriteRegion {
                region_index,
                ..
            } if *region_index == state.wal_tail()
        )
    }));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-073` WAL rotation start/finish appends MUST reserve the next ready
//# region with `allocate_region`, advance `allocation_head`, move WAL tail to the new
//# region, and clear the matching storage-core private allocation reservation.
#[test]
fn requirement_append_rotation_start_and_finish_move_to_new_tail() {
    let mut flash = MockFlash::<192, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<192>::new();
    let mut state = format::<192, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
    state
        .rotate_wal_tail::<192, 4, _>(&mut flash, &mut workspace)
        .unwrap();
    assert_eq!(state.wal_head(), 0);
    assert_eq!(state.wal_tail(), 2);
    assert_eq!(state.ready_region(), None);
    assert_eq!(state.ready_free_region(), Some(3));
    assert_eq!(state.max_seen_sequence(), 2);

    let reopened = open::<192, 4, 128>(&mut flash).unwrap();
    assert_eq!(reopened.wal_head(), state.wal_head());
    assert_eq!(reopened.wal_tail(), state.wal_tail());
    assert_eq!(reopened.wal_append_offset(), state.wal_append_offset());
    assert_eq!(reopened.ready_region(), state.ready_region());
    assert_eq!(reopened.ready_free_region(), state.ready_free_region());
    assert_eq!(
        reopened.free_space_tail_region(),
        state.free_space_tail_region()
    );
    assert_eq!(reopened.max_seen_sequence(), state.max_seen_sequence());
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-003` Each newly allocated region, whether for a user
//# collection or a newly initialized private log region, MUST use
//# `sequence = max_seen_sequence + 1`, after which that value becomes the
//# new in-memory `max_seen_sequence`.
#[test]
fn requirement_committed_region_allocations_advance_sequence_from_max_seen_sequence() {
    let progress = committed_region_sequence_progress();

    assert_eq!(progress.first_region, 2);
    assert_eq!(progress.first_sequence, 2);
    assert_eq!(progress.max_seen_after_first, progress.first_sequence);

    assert_eq!(progress.second_region, 3);
    assert_eq!(progress.second_sequence, progress.max_seen_after_first + 1);
    assert_eq!(progress.max_seen_after_second, progress.second_sequence);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-074` WAL rotation MUST initialize the new WAL region at
//# `max_seen_sequence + 1` and update runtime `max_seen_sequence`.
#[test]
fn requirement_wal_rotation_initializes_the_next_wal_region_at_max_seen_sequence_plus_one() {
    let mut flash = MockFlash::<192, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<192>::new();
    let mut state = format::<192, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
    let next_region = state.ready_free_region().unwrap();
    let allocation_head_after = state.free_space.position_after_allocation().unwrap();
    let reserves = state
        .rotation_reserves::<192, 4>(&mut workspace, next_region, allocation_head_after)
        .unwrap();
    let append_limit = wal_record_append_limit(state.metadata()).unwrap();
    state.wal_append_offset = append_limit - reserves.allocate_region_len - reserves.link_reserve;

    let next_region = state
        .append_wal_rotation_start::<192, 4, _>(&mut flash, &mut workspace)
        .unwrap();
    state
        .append_wal_rotation_finish::<192, 4, _>(&mut flash, &mut workspace, next_region)
        .unwrap();

    let header = read_header_from_flash::<192, 4, _>(&mut flash, next_region).unwrap();
    assert_eq!(header.sequence, 2);
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
//# `RING-STORAGE-006` A free region MUST be defined by membership in the storage-private
//# free-space collection rather than by a distinct on-disk header encoding or by allocator
//# links stored in that free region.
#[test]
fn requirement_free_region_membership_is_defined_by_the_free_space_collection() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();

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

    assert_eq!(reserved_region, 2);
    assert_eq!(state.free_space.entries(), &[2, 3, 4]);
    assert_eq!(state.free_space.allocation_head(), 1);
    assert_eq!(state.free_space.ready_boundary(), 3);
    assert_eq!(state.free_space.append_tail(), 3);
    assert!(!state
        .region_is_on_free_list::<512, 5, _>(&mut flash, reserved_region)
        .unwrap());
    assert!(state
        .region_is_on_free_list::<512, 5, _>(&mut flash, 3)
        .unwrap());

    assert_eq!(
        state.append_allocate_region_for_test::<512, 5, _>(&mut flash, &mut workspace, 2),
        Err(StorageRuntimeError::InvalidFreeSpaceCommand)
    );
    assert_eq!(
        state.append_allocate_region_for_test::<512, 5, _>(&mut flash, &mut workspace, 2),
        Err(StorageRuntimeError::InvalidFreeSpaceCommand)
    );

    state.wal_tail = 3;
    assert_eq!(
        state.append_allocate_region_for_test::<512, 5, _>(&mut flash, &mut workspace, 2),
        Err(StorageRuntimeError::InvalidFreeSpaceCommand)
    );
    state.wal_tail = 0;
    state.ready_region = Some(3);
    assert_eq!(
        state.append_allocate_region_for_test::<512, 5, _>(&mut flash, &mut workspace, 2),
        Err(StorageRuntimeError::InvalidFreeSpaceCommand)
    );
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-006` A free region MUST be defined by membership in
//# the storage-private free-space collection rather than.
#[test]
fn requirement_stale_footer_bytes_do_not_make_a_reserved_region_free() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();

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

    assert_eq!(reserved_region, 2);
    assert!(!state
        .region_is_on_free_list::<512, 5, _>(&mut flash, reserved_region)
        .unwrap());
    assert_eq!(state.ready_region(), None);
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-007` Allocator queue links and cursor state MUST NOT
//# be stored in freed data regions;
#[test]
fn requirement_committed_region_writes_do_not_write_a_live_free_pointer_footer() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
            &mut workspace,
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
                len: 32,
            },
            MockOperation::Sync,
        ]
    );
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-008` A dirty free-space entry MUST NOT enter the ready range until the
//# named region has been erased and the corresponding `erase_free_region_span` record or
//# equivalent materialized state is durable.
#[test]
fn requirement_free_regions_are_erased_only_when_reused() {
    let mut flash = MockFlash::<512, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut state = format::<512, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();

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
            &mut workspace,
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
//# `RING-STORAGE-009` A main WAL region MUST have `collection_id = 0` and
//# `collection_format = main_wal_v2`; a transaction-log region MUST have `collection_id =
//# 0` and `collection_format = transaction_log_v2`; a free-space metadata region MUST have
//# `collection_id = 0` and `collection_format = free_space_v2`.
#[test]
fn requirement_initialized_wal_regions_use_reserved_wal_header_fields() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    let mut workspace = StorageWorkspace::<128>::new();
    initialize_wal_region::<128, 4, _>(
        &mut flash,
        &mut workspace,
        metadata,
        1,
        7,
        0,
        test_free_position(0),
        test_free_position(1),
        test_free_position(1),
    )
    .unwrap();

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
    assert_eq!(prologue.log_head_region_index, 0);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-007` The reclaimed region MUST be appended to the dirty range
//# with `free_region` before it is eligible for erase maintenance.
#[test]
fn requirement_initialized_wal_region_erases_the_reclaimed_region_before_reuse() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    flash.clear_operations();
    let mut workspace = StorageWorkspace::<128>::new();
    initialize_wal_region::<128, 4, _>(
        &mut flash,
        &mut workspace,
        metadata,
        1,
        7,
        0,
        test_free_position(0),
        test_free_position(1),
        test_free_position(1),
    )
    .unwrap();

    assert_eq!(
        flash.operations(),
        &[
            MockOperation::EraseRegion { region_index: 1 },
            MockOperation::WriteRegion {
                region_index: 1,
                offset: 0,
                len: metadata.wal_record_area_offset().unwrap(),
            },
            MockOperation::Sync,
        ]
    );
}

//= spec/ring/04-wal-records.md#assumptions-for-replay-correctness
//= type=test
//# `RING-REPLAY-ASSUME-001` A private log region MUST be erased before reuse.
#[test]
fn requirement_initialized_wal_region_erases_the_wal_region_before_reuse() {
    let mut flash = MockFlash::<128, 4, 32>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

    flash.clear_operations();
    let mut workspace = StorageWorkspace::<128>::new();
    initialize_wal_region::<128, 4, _>(
        &mut flash,
        &mut workspace,
        metadata,
        1,
        7,
        0,
        test_free_position(0),
        test_free_position(1),
        test_free_position(1),
    )
    .unwrap();

    assert!(matches!(
        flash.operations().first(),
        Some(MockOperation::EraseRegion { region_index: 1 })
    ));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-077` Normal WAL append capacity MUST exclude a logical reserve
//# large enough for the rotation allocation plus rotation-link record.
#[test]
fn requirement_normal_append_rejects_when_it_would_consume_rotation_reserve() {
    let mut flash = MockFlash::<256, 4, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
    state
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let mut saw_rotation_required = false;
    for _ in 0..256 {
        match state.append_update::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            &[1, 2, 3, 4, 5, 6, 7, 8],
        ) {
            Ok(()) => continue,
            Err(StorageRuntimeError::WalRotationRequired) => {
                saw_rotation_required = true;
                break;
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    assert!(saw_rotation_required);

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
//# `RING-IMPL-REGRESSION-107` Internal WAL rotation with a large pending record MUST bridge
//# an early rotation-window gap without surfacing `InvalidRotationWindow` to the caller.
#[test]
fn requirement_internal_rotation_bridges_early_window_gap_for_large_record() {
    let mut flash = MockFlash::<256, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 6, _>(&mut flash, 1, 8, 0xa5).unwrap();
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
    fill_until_append_reserve_requires_rotation::<256, 6, _, 8>(
        &mut state,
        &mut flash,
        &mut workspace,
        large_record,
    );

    let next_region = state.ready_free_region().unwrap();
    let allocation_head_after = state.free_space.position_after_allocation().unwrap();
    let reserves = state
        .rotation_reserves::<256, 6>(&mut workspace, next_region, allocation_head_after)
        .unwrap();
    let remaining_after_allocate_region =
        256 - (state.wal_append_offset() + reserves.allocate_region_len);
    assert!(remaining_after_allocate_region >= reserves.rotation_reserve);

    let previous_tail = state.wal_tail();
    state
        .append_record_with_rotation::<256, 6, _>(&mut flash, &mut workspace, large_record)
        .unwrap();

    assert_ne!(state.wal_tail(), previous_tail);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-078` WAL rotation start MUST be accepted only after normal append capacity
//# is exhausted and while the rotation-link reserve remains available.
#[test]
fn requirement_wal_rotation_rejects_calls_outside_the_rotation_window() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();

    assert!(matches!(
        state.append_wal_rotation_start::<256, 4, _>(&mut flash, &mut workspace),
        Err(StorageRuntimeError::InvalidRotationWindow {
            remaining_after,
            rotation_reserve,
            ..
        }) if remaining_after >= rotation_reserve
    ));

    let next_region = state.ready_free_region().unwrap();
    let allocation_head_after = state.free_space.position_after_allocation().unwrap();
    let reserves = state
        .rotation_reserves::<256, 4>(&mut workspace, next_region, allocation_head_after)
        .unwrap();
    state.wal_append_offset = 256 - reserves.allocate_region_len - (reserves.link_reserve - 1);

    assert!(matches!(
        state.rotate_wal_tail::<256, 4, _>(&mut flash, &mut workspace),
        Err(StorageRuntimeError::InvalidRotationWindow {
            remaining_after,
            link_reserve,
            ..
        }) if remaining_after < link_reserve
    ));
    assert_eq!(state.wal_tail(), 0);
    assert_eq!(state.ready_region(), None);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-079` Head append room checks MUST perform WAL rotation when the current
//# tail lacks room for a head record.
#[test]
fn requirement_ensure_head_append_room_with_rotation_rotates_when_tail_lacks_head_room() {
    let mut flash = MockFlash::<256, 6, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 6, _>(&mut flash, 1, 8, 0xa5).unwrap();
    state
        .append_new_collection::<256, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let target_region = state.ready_free_region().unwrap();
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
//# `RING-IMPL-REGRESSION-081` Encoded append reserve checks for `allocate_region` MUST
//# require a ready entry and return `WalRotationRequired` or an equivalent capacity signal
//# when no ready entry remains.
#[test]
fn requirement_ensure_encoded_append_reserve_rejects_allocate_region_when_no_free_region_remains() {
    let mut flash = MockFlash::<128, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let mut state = format::<128, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
    state.free_space = FreeSpaceState::empty();

    assert_eq!(state.ready_free_region(), None);
    assert_eq!(
        state.ensure_encoded_append_reserve::<128, 4, _>(
            &mut workspace,
            &mut flash,
            1,
            true,
            false
        ),
        Err(StorageRuntimeError::WalRotationRequired)
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-082` Encoded append reserve checks MUST allow the rotation
//# `allocate_region` when the tail has exactly the rotation reserve plus encoded record
//# length remaining.
#[test]
fn requirement_ensure_encoded_append_reserve_accepts_allocate_region_at_exact_rotation_reserve_boundary(
) {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
    let next_region = state.ready_free_region().unwrap();
    let allocation_head_after = state.free_space.position_after_allocation().unwrap();
    let reserves = state
        .rotation_reserves::<256, 4>(&mut workspace, next_region, allocation_head_after)
        .unwrap();
    let encoded_len = 1;
    let append_limit = wal_record_append_limit(state.metadata()).unwrap();
    state.wal_append_offset = append_limit - reserves.rotation_reserve - encoded_len;

    assert_eq!(
        state.ensure_encoded_append_reserve::<256, 4, _>(
            &mut workspace,
            &mut flash,
            encoded_len,
            true,
            false,
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
    let mut state = format::<512, 5, _>(&mut flash, 1, 8, 0xa5).unwrap();
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
            &mut workspace,
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
    let mut live_state = format::<256, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
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
//# `RING-IMPL-REGRESSION-085` Foreground allocation headroom checks MUST reject ordinary
//# allocations that would consume the configured ready-region reserve.
#[test]
fn requirement_ensure_foreground_allocation_headroom_rejects_using_the_minimum_free_reserve() {
    let mut flash = MockFlash::<256, 6, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 6, _>(&mut flash, 3, 8, 0xa5).unwrap();
    let _reserved = state
        .reserve_next_region::<256, 6, _>(
            &mut flash,
            &mut workspace,
            &mut heapless::Vec::new(),
            &mut heapless::Vec::new(),
            &mut crate::storage::WalHeadReclaimPlan::empty(),
            &mut crate::startup::StartupOpenPlan::empty(),
        )
        .unwrap();

    assert_eq!(state.free_region_count::<256, 6, _>(&mut flash), Ok(3));
    assert_eq!(
        state.ensure_foreground_allocation_headroom::<256, 6, _>(
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
    let mut state = format::<128, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
    let metadata = state.metadata();
    append_exact_fill_update_record(&mut flash, metadata, state.wal_head(), CollectionId(77));
    let mut plan = WalHeadReclaimPlan::<8> {
        old_head: state.wal_head(),
        source_tail: state.wal_tail(),
        source_tail_append_offset: 128,
        original_collections: Vec::new(),
        imported_transaction_logs: Vec::new(),
    };

    let mut active_collections = Vec::<CollectionId, 8>::new();
    state
        .copy_live_wal_head_reclaim_state::<128, 4, _>(
            &mut flash,
            &mut workspace,
            &mut plan,
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
    let state = open::<128, 4, _>(&mut flash).unwrap();
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
    const MAX_RUNS: usize = 16;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 16384>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let collection_id = CollectionId(707);
    storage.create_map(collection_id).unwrap();

    let mut map_buffer = [0u8; 8192];
    let mut map = MapFrontier::<i32, i32, MAX_RUNS>::new(
        collection_id,
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for key in 0..100 {
        map.set_in_memory(key, key * 10).unwrap();
    }
    storage.flush_map::<i32, i32, MAX_RUNS>(&mut map).unwrap();

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
//# `RING-IMPL-REGRESSION-090` WAL record visitation MUST process a tail record that ends exactly at
//# the append limit and then stop.
#[test]
fn requirement_visit_wal_records_stops_when_a_tail_record_ends_at_the_append_limit() {
    let mut flash = MockFlash::<128, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let mut state = format::<128, 4, _>(&mut flash, 1, 8, 0xa5).unwrap();
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
    initialize_wal_region::<128, 4, _>(
        &mut flash,
        &mut workspace,
        metadata,
        2,
        1,
        0,
        test_free_position(0),
        test_free_position(1),
        test_free_position(1),
    )
    .unwrap();

    assert_eq!(
        wal_chain_contains_region::<128, _>(&mut flash, &mut workspace, metadata, 0, 2, 2),
        Ok(true)
    );

    let mut flash = MockFlash::<128, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    let granule = usize::try_from(metadata.wal_write_granule).unwrap();
    flash.write_region(0, wal_offset, &[0; 8]).unwrap();
    let after_recovery = append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset + granule,
        WalRecord::WalRecovery,
    );
    append_wal_record(
        &mut flash,
        metadata,
        0,
        after_recovery,
        WalRecord::Link {
            next_region_index: 2,
            expected_sequence: 1,
        },
    );
    initialize_wal_region::<128, 4, _>(
        &mut flash,
        &mut workspace,
        metadata,
        2,
        1,
        0,
        test_free_position(0),
        test_free_position(1),
        test_free_position(1),
    )
    .unwrap();

    assert_eq!(
        wal_chain_contains_region::<128, _>(&mut flash, &mut workspace, metadata, 0, 2, 2),
        Ok(true)
    );
}

//= spec/ring/07-reclaim.md#free-region
//= type=test
//# `RING-FREE-REGION-002` Ensure the current free-space metadata
//# frontier has room for one more dirty entry,
#[test]
fn requirement_free_space_checkpoint_reports_metadata_capacity_exhaustion() {
    let mut flash = MockFlash::<128, 6, 512>::new(0xff);
    let mut state = format::<128, 6, _>(&mut flash, 1, 8, 0xa5).unwrap();
    let entries = [2u32; 19];
    state
        .free_space
        .replace_from_parts(1, 0, 19, 19, &entries)
        .unwrap();

    assert_eq!(
        state.materialize_free_space_collection::<128, _>(&mut flash),
        Err(StorageRuntimeError::InsufficientFreeSpaceMetadataCapacity {
            required_regions: 2,
            available_regions: 1,
        })
    );
}

//= spec/ring/07-reclaim.md#free-region
//= type=test
//# `RING-FREE-REGION-002` Ensure the current free-space metadata
//# frontier has room for one more dirty entry, materializing or
//# checkpointing a new `free_space_v2` metadata region if needed.
#[test]
fn requirement_free_space_checkpoint_grows_metadata_chain_for_dirty_append() {
    let mut flash = MockFlash::<256, 10, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut state = format::<256, 10, _>(&mut flash, 1, 8, 0xa5).unwrap();
    let entries_per_region =
        StorageRuntime::<8>::free_space_entries_per_metadata_region::<256>().unwrap();
    let mut entries = Vec::<u32, 128>::new();
    entries.push(2).unwrap();
    for index in 1..entries_per_region {
        entries.push(3 + u32::try_from(index % 7).unwrap()).unwrap();
    }
    let cursor = u32::try_from(entries_per_region).unwrap();
    state
        .free_space
        .replace_from_parts(1, 0, cursor, cursor, entries.as_slice())
        .unwrap();
    state
        .materialize_free_space_collection::<256, _>(&mut flash)
        .unwrap();

    state
        .append_free_region_with_rotation::<256, 10, _>(
            &mut flash,
            &mut workspace,
            CollectionId(0),
            6,
        )
        .unwrap();

    assert_eq!(state.free_space.metadata_region_count(), 2);
    assert_eq!(state.append_tail().region_index, 2);
    let reopened = open::<256, 10, _>(&mut flash).unwrap();
    assert_eq!(reopened.append_tail().region_index, 2);
    assert_eq!(reopened.free_space_tail_region(), Some(6));
}
