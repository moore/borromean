use super::*;

//= spec/implementation.md#architecture-requirements
//= type=todo
//# `RING-IMPL-ARCH-002` The backing abstraction MUST be bound to
//# `Storage` during format or open, and normal public operations MUST use
//# that backing through `Storage` rather than accepting a separate backing
//# argument.
#[test]
fn todo_storage_operations_use_bound_backing() {}

//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-002` The public API MUST allow a caller to drive the
//# same storage engine from either blocking test shims or asynchronous
//# device adapters without changing borromean correctness logic.
#[test]
fn requirement_blocking_and_future_entry_points_produce_equivalent_storage_state() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 5;
    let mut blocking_flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut blocking_workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut blocking = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut blocking_flash,
        &mut blocking_workspace,
        1,
        8,
        0xa5,
    )
    .unwrap();

    let mut future_flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut future_workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut future_driven = super::super::poll_until_ready(
        Storage::<8, 4>::format_future::<REGION_SIZE, REGION_COUNT, _>(
            &mut future_flash,
            &mut future_workspace,
            1,
            8,
            0xa5,
        ),
        16,
    )
    .unwrap();

    blocking
        .create_map::<REGION_SIZE, REGION_COUNT, _>(
            &mut blocking_flash,
            &mut blocking_workspace,
            CollectionId(61),
        )
        .unwrap();
    super::super::poll_until_ready(
        future_driven.create_map_future::<REGION_SIZE, REGION_COUNT, _>(
            &mut future_flash,
            &mut future_workspace,
            CollectionId(61),
        ),
        16,
    )
    .unwrap();

    let mut blocking_payload = [0u8; 64];
    let mut future_payload = [0u8; 64];
    blocking
        .append_map_update::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut blocking_flash,
            &mut blocking_workspace,
            CollectionId(61),
            &MapUpdate::Set { key: 7, value: 70 },
            &mut blocking_payload,
        )
        .unwrap();
    super::super::poll_until_ready(
        future_driven.append_map_update_future::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut future_flash,
            &mut future_workspace,
            CollectionId(61),
            &MapUpdate::Set { key: 7, value: 70 },
            &mut future_payload,
        ),
        16,
    )
    .unwrap();

    let reopened_blocking = Storage::<8, 4>::open::<REGION_SIZE, REGION_COUNT, _>(
        &mut blocking_flash,
        &mut blocking_workspace,
    )
    .unwrap();
    let reopened_future = super::super::poll_until_ready(
        Storage::<8, 4>::open_future::<REGION_SIZE, REGION_COUNT, _>(
            &mut future_flash,
            &mut future_workspace,
        ),
        16,
    )
    .unwrap();

    assert_eq!(reopened_blocking.metadata(), reopened_future.metadata());
    assert_eq!(
        reopened_blocking.collections(),
        reopened_future.collections()
    );
    assert_eq!(
        reopened_blocking.pending_reclaims(),
        reopened_future.pending_reclaims()
    );
    assert_eq!(
        reopened_blocking.last_free_list_head(),
        reopened_future.last_free_list_head()
    );
    assert_eq!(
        reopened_blocking.free_list_tail(),
        reopened_future.free_list_tail()
    );

    let mut blocking_map_buffer = [0u8; REGION_SIZE];
    let blocking_map = reopened_blocking
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8, 8>(
            &mut blocking_flash,
            &mut blocking_workspace,
            CollectionId(61),
            &mut blocking_map_buffer,
        )
        .unwrap();
    let mut future_map_buffer = [0u8; REGION_SIZE];
    let future_map = reopened_future
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8, 8>(
            &mut future_flash,
            &mut future_workspace,
            CollectionId(61),
            &mut future_map_buffer,
        )
        .unwrap();
    assert_eq!(blocking_map.get_frontier(&7).unwrap(), Some(70));
    assert_eq!(future_map.get_frontier(&7).unwrap(), Some(70));
}

//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-005` The implementation MAY provide optional helper
//# adapters for common executors or embedded frameworks, but the core
//# crate MUST remain usable without them.
#[test]
fn requirement_core_api_remains_usable_without_executor_or_framework_helpers() {
    let mut flash = MockFlash::<256, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut storage =
        Storage::<8, 4>::format::<256, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    assert_no_alloc("blocking core api", || {
        storage
            .create_map::<256, 5, _>(&mut flash, &mut workspace, CollectionId(85))
            .unwrap();
    });

    let reopened = Storage::<8, 4>::open::<256, 5, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(reopened.metadata().region_size, 256);
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(85));
}

//= spec/ring.md#storage-api-requirements
//= type=todo
//# `RING-API-001` `Storage` MUST be the public database context that owns logical runtime state,
//# replay state, configuration, dirty-frontier tracking, and bounded reusable scratch memory needed
//# by normal storage and collection operations.
#[test]
fn todo_storage_context_owns_operation_scratch() {}

//= spec/ring.md#storage-api-requirements
//= type=todo
//# `RING-API-002` `Storage` MUST own exclusive access to the backing object for the lifetime of an
//# opened database, either by owning the backing value or by holding a mutable reference to it.
#[test]
fn todo_storage_owns_backing_access() {}

//= spec/ring.md#storage-api-requirements
//= type=todo
//# `RING-API-003` Public operations that may touch backing media MUST use the backing object
//# through `Storage` rather than requiring a separate backing argument on each operation.
#[test]
fn todo_operations_use_storage_backing() {}

//= spec/ring.md#storage-api-requirements
//= type=todo
//# `RING-API-004` Public normal collection operations MUST NOT require callers to provide
//# collection frontier buffers, payload serialization buffers, or a `StorageWorkspace`; that
//# bounded memory MUST be supplied by the `Storage` context or storage-owned configuration.
#[test]
fn todo_collection_operations_use_storage_owned_buffers() {}

//= spec/ring.md#storage-api-requirements
//= type=todo
//# `RING-API-005` Any shared-device synchronization required by a platform MUST be encapsulated by
//# the backing implementation rather than by Borromean core requiring a specific mutex, executor,
//# interrupt policy, or sharing primitive.
#[test]
fn todo_shared_backing_synchronization_stays_behind_backing_trait() {}

//= spec/ring.md#ring-state-machine-requirements
//= type=todo
//# `RING-MACHINE-001` Storage runtime MUST expose a single active storage mode so that at most
//# one formatting, opening, append, allocation, region-write, rotation, region-reclaim, or
//# WAL-head-reclaim operation is active for a storage context.
#[test]
fn todo_storage_runtime_exposes_single_active_mode() {}

//= spec/ring.md#ring-state-machine-requirements
//= type=todo
//# `RING-MACHINE-002` Stable replayed runtime state MUST be kept separate from
//# operation-specific progress state owned by the active mode.
#[test]
fn todo_runtime_state_is_separate_from_operation_progress() {}

//= spec/ring.md#ring-state-machine-requirements
//= type=todo
//# `RING-MACHINE-003` Public steady-state operations MUST validate that the storage context is
//# in a valid source mode, normally `Idle`, before beginning their transition sequence.
#[test]
fn todo_public_operations_validate_source_mode() {}

//= spec/ring.md#ring-state-machine-requirements
//= type=todo
//# `RING-MACHINE-004` Every durable write that changes replay-visible state MUST be represented
//# as a named transition edge with defined preconditions, durable effect, runtime effect, replay
//# effect, and crash-cut result.
#[test]
fn todo_durable_writes_are_named_transition_edges() {}

//= spec/ring.md#ring-state-machine-requirements
//= type=todo
//# `RING-MACHINE-005` Normal foreground operation, startup replay, and crash recovery MUST use
//# the same `ApplyWalRecord` semantics for every retained durable WAL record.
#[test]
fn todo_foreground_replay_and_recovery_share_wal_record_semantics() {}

//= spec/ring.md#ring-state-machine-requirements
//= type=todo
//# `RING-MACHINE-006` Startup and recovery modes MUST compose the same collection, allocator,
//# WAL-chain, and reclaim submachine transitions used by normal operation rather than defining
//# separate incompatible transition rules.
#[test]
fn todo_startup_and_recovery_compose_normal_submachines() {}
