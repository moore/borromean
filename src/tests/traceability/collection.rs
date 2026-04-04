use super::*;

//= spec/implementation.md#collection-requirements
//# `RING-IMPL-COLL-001` Collection implementations MUST depend on the
//# shared storage engine for durability, ordering, and recovery rather
//# than duplicating those mechanisms ad hoc.
#[test]
fn map_collection_paths_delegate_durability_ordering_and_recovery_to_shared_storage() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(map
        .contains("use crate::storage::{StorageRuntime, StorageRuntimeError, StorageVisitError};"));
    assert!(map.contains("storage: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>"));
    assert!(map.contains("storage: &StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>"));
    assert!(map.contains("storage.append_snapshot::<REGION_SIZE, REGION_COUNT, IO>("));
    assert!(map.contains(
        "storage.reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;"
    ));
    assert!(map.contains("storage.write_committed_region::<REGION_SIZE, REGION_COUNT, IO>("));
    assert!(map.contains("storage.append_reclaim_begin::<REGION_SIZE, REGION_COUNT, IO>("));
    assert!(map.contains("storage.append_head::<REGION_SIZE, REGION_COUNT, IO>("));
    assert!(map.contains("let visit_result = storage.visit_wal_records::<REGION_SIZE, IO, _, _>("));

    assert!(lib.contains("self.append_new_collection::<REGION_SIZE, REGION_COUNT, IO>("));
    assert!(lib.contains("map.write_snapshot_to_storage::<"));
    assert!(lib.contains("map.flush_to_storage::<"));
    assert!(lib.contains("drop_collection_and_begin_reclaim::<REGION_SIZE, REGION_COUNT, IO>("));
    assert!(lib.contains("LsmMap::<K, V, MAX_INDEXES>::open_from_storage::<"));
}

//= spec/implementation.md#collection-requirements
//# `RING-IMPL-COLL-003` A collection operation that needs I/O MUST be
//# drivable through the same runtime-agnostic future model as core
//# storage operations.
#[test]
fn collection_operations_with_io_are_drivable_as_runtime_agnostic_futures() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    super::super::poll_ready(storage.create_map_future::<512, 5, _>(
        &mut flash,
        &mut workspace,
        CollectionId(84),
    ))
    .unwrap();

    let mut source_buffer = [0u8; 512];
    let mut source = LsmMap::<u16, u16, 8>::new(CollectionId(84), &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    super::super::poll_ready(storage.snapshot_map_future::<512, 5, _, _, _, 8>(
        &mut flash,
        &mut workspace,
        &source,
    ))
    .unwrap();

    let mut payload_buffer = [0u8; 64];
    super::super::poll_ready(storage.append_map_update_future::<512, 5, _, u16, u16, 8>(
        &mut flash,
        &mut workspace,
        CollectionId(84),
        &MapUpdate::Set { key: 2, value: 20 },
        &mut payload_buffer,
    ))
    .unwrap();

    source.set(3, 30).unwrap();
    let committed_region = super::super::poll_until_ready(
        storage.flush_map_future::<512, 5, _, _, _, 8>(&mut flash, &mut workspace, &source),
        4,
    )
    .unwrap();

    let reclaim_region = super::super::poll_ready(storage.drop_map_future::<512, 5, _>(
        &mut flash,
        &mut workspace,
        CollectionId(84),
    ))
    .unwrap();

    assert_eq!(reclaim_region, Some(committed_region));
    assert_eq!(
        storage.collections()[0].basis(),
        crate::StartupCollectionBasis::Dropped
    );
}
