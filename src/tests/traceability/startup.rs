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
    let mut format_memory = StorageMemory::<512, 5, 8>::new();
    let mut storage = Storage::<_, 512, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        &mut format_memory,
    )
    .unwrap();

    storage.create_map(CollectionId(83)).unwrap();
    let mut payload_buffer = [0u8; 64];
    storage
        .append_map_update::<u16, u16>(CollectionId(83), &MapUpdate::Set { key: 7, value: 70 })
        .unwrap();
    drop(storage);

    let mut reopened = {
        let future = Storage::<_, 512, 5, 8>::open_future(&mut flash, crate::test_storage_memory());
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
        .open_map::<u16, u16, 8>(
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
    let mut storage = Storage::<_, 512, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(84)).unwrap();
    storage.append_update(CollectionId(84), &[1, 2, 3]).unwrap();
    drop(storage);

    let mut blocking_memory = StorageMemory::<512, 5, 8>::new();
    assert_no_alloc("blocking open", || {
        let mut reopened = Storage::<_, 512, 5, 8>::open(&mut flash, &mut blocking_memory).unwrap();
        assert_eq!(reopened.collections()[0].collection_id(), CollectionId(84));
    });

    let mut future_memory = StorageMemory::<512, 5, 8>::new();
    assert_no_alloc("future open", || {
        let reopened = super::super::poll_until_ready(
            Storage::<_, 512, 5, 8>::open_future(&mut flash, &mut future_memory),
            8,
        )
        .unwrap();
        assert_eq!(reopened.collections()[0].collection_id(), CollectionId(84));
    });
}

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-003` If startup needs temporary decode storage,
//# that storage MUST come from the `Storage` context or bounded storage
//# supplied when that context is constructed.
#[test]
fn requirement_startup_uses_storage_context_decode_scratch() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut format_memory = StorageMemory::<512, 5, 8>::new();
    let mut storage = Storage::<_, 512, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        &mut format_memory,
    )
    .unwrap();

    storage.create_map(CollectionId(85)).unwrap();
    storage
        .append_update(CollectionId(85), &[0xa5, 0xff, 0x11, 0x22])
        .unwrap();
    drop(storage);

    let mut open_memory = StorageMemory::<512, 5, 8>::new();
    let reopened = assert_no_alloc("startup decode scratch", || {
        Storage::<_, 512, 5, 8>::open(&mut flash, &mut open_memory).unwrap()
    });

    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(85));
    assert_eq!(reopened.collections()[0].pending_update_count(), 1);
}

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-004` Recovery of incomplete WAL rotation,
//# allocation, or transaction cleanup state MUST be expressible through the same
//# operation framework used for normal foreground work.
#[test]
fn requirement_blocking_and_future_open_recover_the_same_pending_reclaim_state() {
    let mut blocking_flash = MockFlash::<512, 10, 2048>::new(0xff);
    let (storage, first_region, second_region) =
        super::super::replace_map_and_free_old_manifest(&mut blocking_flash);
    drop(storage);
    let reopened_blocking =
        Storage::<_, 512, 10, 8>::open(&mut blocking_flash, crate::test_storage_memory()).unwrap();

    let mut future_flash = MockFlash::<512, 10, 2048>::new(0xff);
    let (future_storage, _, _) = super::super::replace_map_and_free_old_manifest(&mut future_flash);
    drop(future_storage);
    let reopened_future = super::super::poll_until_ready(
        Storage::<_, 512, 10, 8>::open_future(&mut future_flash, crate::test_storage_memory()),
        8,
    )
    .unwrap();

    assert_eq!(
        reopened_blocking.collections()[0].basis(),
        StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(
        reopened_blocking.free_space_tail_region(),
        Some(first_region)
    );

    assert_eq!(
        reopened_future.collections(),
        reopened_blocking.collections()
    );
    assert_eq!(
        reopened_future.ready_free_region(),
        reopened_blocking.ready_free_region()
    );
    assert_eq!(
        reopened_future.free_space_tail_region(),
        reopened_blocking.free_space_tail_region()
    );
}
