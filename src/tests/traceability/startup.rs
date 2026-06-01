use super::*;
use std::pin::pin;
use std::task::Poll;

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-001` Opening storage MUST be implemented as an
//# operation that can suspend between device interactions without
//# losing its replay context.
#[test]
fn requirement_open_future_preserves_replay_context_across_pending_polls() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut format_memory = StorageMemory::<512, 5, 8, 4>::new();
    let mut storage = Storage::<_, 512, 5, 8, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        &mut format_memory,
    )
    .unwrap();

    storage.create_map(CollectionId(83)).unwrap();
    let mut payload_buffer = [0u8; 64];
    storage
        .append_map_update::<u16, u16, 8>(CollectionId(83), &MapUpdate::Set { key: 7, value: 70 })
        .unwrap();
    drop(storage);

    let mut reopened = {
        let future =
            Storage::<_, 512, 5, 8, 4>::open_future(&mut flash, crate::test_storage_memory());
        let mut future = pin!(future);

        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));

        super::super::poll_until_ready(future.as_mut(), 8).unwrap()
    };
    let mut map_buffer = [0u8; 512];
    let map = reopened
        .open_map::<u16, u16, 8, 8>(
            CollectionId(83),
            &mut map_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(map.get_frontier(&7).unwrap(), Some(70));
}

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-002` Startup replay state MUST itself obey the
//# same no-allocation rule as steady-state operation.
#[test]
fn requirement_startup_open_paths_complete_without_heap_allocation() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 5, 8, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(84)).unwrap();
    storage.append_update(CollectionId(84), &[1, 2, 3]).unwrap();
    drop(storage);

    let mut blocking_memory = StorageMemory::<512, 5, 8, 4>::new();
    assert_no_alloc("blocking open", || {
        let mut reopened =
            Storage::<_, 512, 5, 8, 4>::open(&mut flash, &mut blocking_memory).unwrap();
        assert_eq!(reopened.collections()[0].collection_id(), CollectionId(84));
    });

    let mut future_memory = StorageMemory::<512, 5, 8, 4>::new();
    assert_no_alloc("future open", || {
        let reopened = super::super::poll_until_ready(
            Storage::<_, 512, 5, 8, 4>::open_future(&mut flash, &mut future_memory),
            8,
        )
        .unwrap();
        assert_eq!(reopened.collections()[0].collection_id(), CollectionId(84));
    });
}

//= spec/implementation.md#startup-requirements
//= type=todo
//# `RING-IMPL-STARTUP-003` If startup needs temporary decode storage,
//# that storage MUST come from the `Storage` context or bounded storage
//# supplied when that context is constructed.
#[test]
fn todo_startup_uses_storage_context_decode_scratch() {}

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-004` Recovery of incomplete WAL rotation,
//# allocation, or transaction cleanup state MUST be expressible through the same
//# operation framework used for normal foreground work.
#[test]
fn requirement_blocking_and_future_open_recover_the_same_pending_reclaim_state() {
    let mut blocking_flash = MockFlash::<512, 5, 2048>::new(0xff);
    let (storage, first_region, second_region) =
        super::super::replace_map_into_pending_reclaim_with_empty_free_list(&mut blocking_flash);
    drop(storage);
    let reopened_blocking =
        Storage::<_, 512, 5, 8, 4>::open(&mut blocking_flash, crate::test_storage_memory())
            .unwrap();

    let mut future_flash = MockFlash::<512, 5, 2048>::new(0xff);
    let (future_storage, _, _) =
        super::super::replace_map_into_pending_reclaim_with_empty_free_list(&mut future_flash);
    drop(future_storage);
    let reopened_future = super::super::poll_until_ready(
        Storage::<_, 512, 5, 8, 4>::open_future(&mut future_flash, crate::test_storage_memory()),
        8,
    )
    .unwrap();

    assert_eq!(
        reopened_blocking.collections()[0].basis(),
        StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(reopened_blocking.last_free_list_head(), Some(first_region));
    assert!(reopened_blocking.pending_reclaims().is_empty());

    assert_eq!(
        reopened_future.collections(),
        reopened_blocking.collections()
    );
    assert_eq!(
        reopened_future.last_free_list_head(),
        reopened_blocking.last_free_list_head()
    );
    assert_eq!(
        reopened_future.free_list_tail(),
        reopened_blocking.free_list_tail()
    );
    assert_eq!(
        reopened_future.pending_reclaims(),
        reopened_blocking.pending_reclaims()
    );
}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=todo
//# `RING-TX-RECOVERY-001` If startup reaches WAL end before
//# `commit_transaction(collection_id)`, it MUST run data recovery for that
//# transaction and append `rollback_transaction(collection_id)`.
#[test]
fn todo_startup_recovers_uncommitted_transaction_with_rollback_marker() {}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=todo
//# `RING-TX-RECOVERY-002` If startup reaches WAL end after
//# `commit_transaction(collection_id)` but before
//# `transaction_finished(collection_id)`, it MUST preserve the committed
//# collection state, finish cleanup frees derived from durable
//# collection-specific state, and append
//# `transaction_finished(collection_id)`.
#[test]
fn todo_startup_finishes_post_commit_transaction_cleanup() {}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=todo
//# `RING-TX-RECOVERY-003` Both data recovery and cleanup recovery MUST
//# be idempotent if startup crashes before the terminal marker is durable.
#[test]
fn todo_transaction_recovery_is_idempotent() {}

//= spec/ring/07-reclaim.md#transaction-cleanup-recovery
//= type=todo
//# `RING-TX-RECOVERY-004` The configured minimum free-region reserve MUST leave enough WAL
//# capacity for startup recovery to append a required terminal transaction
//# record.
#[test]
fn todo_min_free_region_reserve_covers_transaction_terminal_records() {}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=todo
//# `RING-WAL-PAYLOAD-010` `begin_transaction`
//# Starts a WAL transaction interval for `collection_id`. Until the
//# matching terminal marker is found or WAL end is reached, replay scans
//# ordinary records for that collection without applying them on the first
//# pass.
#[test]
fn todo_wal_begin_transaction_record_starts_collection_interval() {}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=todo
//# `RING-WAL-PAYLOAD-011` `commit_transaction`
//# Ends the transaction update phase for `collection_id`. Before this
//# marker, recovery abandons the collection-state update. After this
//# marker, recovery preserves the collection-state update and finishes
//# allocator cleanup.
#[test]
fn todo_wal_commit_transaction_record_marks_update_phase() {}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=todo
//# `RING-WAL-PAYLOAD-012` `transaction_finished`
//# Ends the cleanup phase for `collection_id`. Both the collection-state
//# update and allocator cleanup are complete, so replay can apply the full
//# transaction interval in original order.
#[test]
fn todo_wal_transaction_finished_record_closes_cleanup_phase() {}

//= spec/ring/04-wal-records.md#wal-record-types
//= type=todo
//# `RING-WAL-PAYLOAD-013` `rollback_transaction`
//# Records that pre-commit recovery for `collection_id` has completed.
//# Replay skips transaction-scoped records in the interval and does not
//# repeat recovery.
#[test]
fn todo_wal_rollback_transaction_record_closes_data_recovery() {}

//= spec/ring/07-reclaim.md#free-region
//= type=todo
//# `RING-FREE-REGION-PRE-003` The owning collection's committed
//# transaction state MUST contain enough durable information for cleanup
//# recovery to derive that `region_index` must be freed.
#[test]
fn todo_collection_state_contains_cleanup_free_plan() {}
