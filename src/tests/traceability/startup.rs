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
    let mut storage =
        Storage::<_, 512, 5, 8, 4>::format(&mut flash, StorageFormatConfig::new(1, 8, 0xa5))
            .unwrap();

    storage.create_map(CollectionId(83)).unwrap();
    let mut payload_buffer = [0u8; 64];
    storage
        .append_map_update::<u16, u16, 8>(CollectionId(83), &MapUpdate::Set { key: 7, value: 70 })
        .unwrap();
    drop(storage);

    let mut reopened = {
        let future = Storage::<_, 512, 5, 8, 4>::open_future(&mut flash);
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
        .open_map::<u16, u16, 8, 8>(CollectionId(83), &mut map_buffer)
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
    let mut storage =
        Storage::<_, 512, 5, 8, 4>::format(&mut flash, StorageFormatConfig::new(1, 8, 0xa5))
            .unwrap();

    storage.create_map(CollectionId(84)).unwrap();
    storage.append_update(CollectionId(84), &[1, 2, 3]).unwrap();
    drop(storage);

    assert_no_alloc("blocking open", || {
        let mut reopened = Storage::<_, 512, 5, 8, 4>::open(&mut flash).unwrap();
        assert_eq!(reopened.collections()[0].collection_id(), CollectionId(84));
    });

    assert_no_alloc("future open", || {
        let reopened =
            super::super::poll_until_ready(Storage::<_, 512, 5, 8, 4>::open_future(&mut flash), 8)
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

//= spec/ring.md#startup-replay-algorithm
//= type=todo
//# `RING-STARTUP-024` After live collection type and retained data validation
//# has succeeded, process each pending reclaim in WAL order. If an ordered
//# staged region is not reachable from validated live collection state or the
//# WAL chain, recover it through the same WAL-tracked reclaim procedure.
#[test]
fn todo_startup_validates_live_collections_before_reachability_reclaim() {}

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-004` Recovery of incomplete WAL rotation,
//# allocation, or reclaim state MUST be expressible through the same
//# operation framework used for normal foreground work.
#[test]
fn requirement_blocking_and_future_open_recover_the_same_pending_reclaim_state() {
    let mut blocking_flash = MockFlash::<512, 5, 2048>::new(0xff);
    let (storage, first_region, second_region) =
        super::super::replace_map_into_pending_reclaim_with_empty_free_list(&mut blocking_flash);
    drop(storage);
    let reopened_blocking = Storage::<_, 512, 5, 8, 4>::open(&mut blocking_flash).unwrap();

    let mut future_flash = MockFlash::<512, 5, 2048>::new(0xff);
    let (future_storage, _, _) =
        super::super::replace_map_into_pending_reclaim_with_empty_free_list(&mut future_flash);
    drop(future_storage);
    let reopened_future = super::super::poll_until_ready(
        Storage::<_, 512, 5, 8, 4>::open_future(&mut future_flash),
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
