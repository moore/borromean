use super::*;

//= spec/implementation.md#collection-requirements
//= type=test
//# `RING-IMPL-COLL-001` Collection implementations MUST depend on the
//# shared storage engine for durability, ordering, and recovery rather
//# than duplicating those mechanisms ad hoc.
#[test]
fn requirement_map_durability_and_recovery_only_change_when_the_shared_storage_engine_is_used() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(84)).unwrap();

    let mut source_buffer = [0u8; 512];
    let mut source = MapFrontier::<u16, u16, 8>::new(
        CollectionId(84),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source.set_in_memory(1, 10).unwrap();

    let mut before_snapshot_buffer = [0u8; 512];
    let before_snapshot = storage
        .open_map::<u16, u16, 8>(
            CollectionId(84),
            &mut before_snapshot_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(before_snapshot.get_frontier(&1).unwrap(), None);

    storage.snapshot_map(&source).unwrap();

    let mut after_snapshot_buffer = [0u8; 512];
    let after_snapshot = storage
        .open_map::<u16, u16, 8>(
            CollectionId(84),
            &mut after_snapshot_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(after_snapshot.get_frontier(&1).unwrap(), Some(10));

    source.set_in_memory(2, 20).unwrap();
    let mut before_update_buffer = [0u8; 512];
    let before_update = storage
        .open_map::<u16, u16, 8>(
            CollectionId(84),
            &mut before_update_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(before_update.get_frontier(&2).unwrap(), None);

    let mut payload_buffer = [0u8; 64];
    storage
        .append_map_update::<u16, u16>(CollectionId(84), &MapUpdate::Set { key: 2, value: 20 })
        .unwrap();

    let mut reopened_buffer = [0u8; 512];
    let mut reopened =
        Storage::<_, 512, 5, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    let reopened_map = reopened
        .open_map::<u16, u16, 8>(
            CollectionId(84),
            &mut reopened_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(reopened_map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(reopened_map.get_frontier(&2).unwrap(), Some(20));
}

//= spec/implementation.md#collection-requirements
//= type=test
//# `RING-IMPL-COLL-003` A collection operation that needs I/O MUST be
//# drivable through the same runtime-agnostic future model as core
//# storage operations.
#[test]
fn requirement_collection_operations_with_io_are_drivable_as_runtime_agnostic_futures() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    super::super::poll_ready(storage.create_map_future(CollectionId(84))).unwrap();

    let mut source_buffer = [0u8; 512];
    let mut source = MapFrontier::<u16, u16, 8>::new(
        CollectionId(84),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source.set_in_memory(1, 10).unwrap();
    super::super::poll_ready(storage.snapshot_map_future(&source)).unwrap();

    let mut payload_buffer = [0u8; 64];
    super::super::poll_ready(storage.append_map_update_future::<u16, u16>(
        CollectionId(84),
        &MapUpdate::Set { key: 2, value: 20 },
    ))
    .unwrap();

    source.set_in_memory(3, 30).unwrap();
    let committed_region =
        super::super::poll_until_ready(storage.flush_map_future::<_, _, 8>(&mut source), 4)
            .unwrap();

    let reclaim_region =
        super::super::poll_ready(storage.drop_map_future(CollectionId(84))).unwrap();

    assert_eq!(reclaim_region, Some(committed_region));
    assert_eq!(
        storage.collections()[0].basis(),
        crate::StartupCollectionBasis::Dropped
    );
}
