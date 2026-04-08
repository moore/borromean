use super::*;
use ::core::pin::pin;
use ::core::task::Poll;

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-001` Opening storage MUST be implemented as an
//# operation that can suspend between device interactions without
//# losing its replay context.
#[test]
fn open_future_preserves_replay_context_across_pending_polls() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(83))
        .unwrap();
    let mut payload_buffer = [0u8; 64];
    storage
        .append_map_update::<512, 5, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(83),
            &MapUpdate::Set { key: 7, value: 70 },
            &mut payload_buffer,
        )
        .unwrap();
    drop(storage);

    let reopened = {
        let future = Storage::<8, 4>::open_future::<512, 5, _>(&mut flash, &mut workspace);
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
        .open_map::<512, 5, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(83),
            &mut map_buffer,
        )
        .unwrap();
    assert_eq!(map.get(&7).unwrap(), Some(70));
}

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-002` Startup replay state MUST itself obey the
//# same no-allocation rule as steady-state operation.
#[test]
fn startup_open_paths_complete_without_heap_allocation() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(84))
        .unwrap();
    storage
        .append_update::<512, 5, _>(&mut flash, &mut workspace, CollectionId(84), &[1, 2, 3])
        .unwrap();
    drop(storage);

    assert_no_alloc("blocking open", || {
        let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
        assert_eq!(reopened.collections()[0].collection_id(), CollectionId(84));
    });

    assert_no_alloc("future open", || {
        let reopened = super::super::poll_until_ready(
            Storage::<8, 4>::open_future::<512, 5, _>(&mut flash, &mut workspace),
            8,
        )
        .unwrap();
        assert_eq!(reopened.collections()[0].collection_id(), CollectionId(84));
    });
}

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-003` If startup needs temporary decode storage,
//# that storage MUST come from a caller-provided workspace or other
//# bounded static storage.
#[test]
fn startup_can_reuse_the_same_caller_workspace_across_repeated_opens() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(85))
        .unwrap();
    storage
        .append_update::<512, 5, _>(&mut flash, &mut workspace, CollectionId(85), &[9, 9, 9])
        .unwrap();
    drop(storage);

    {
        let (region_bytes, logical_scratch) = workspace.scan_buffers();
        region_bytes.fill(0x11);
        logical_scratch.fill(0x22);
    }
    let reopened_once = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();

    {
        let (physical_scratch, logical_scratch) = workspace.encode_buffers();
        physical_scratch.fill(0x33);
        logical_scratch.fill(0x44);
    }
    let reopened_twice = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();

    assert_eq!(reopened_twice.collections(), reopened_once.collections());
    assert_eq!(reopened_twice.wal_head(), reopened_once.wal_head());
    assert_eq!(reopened_twice.wal_tail(), reopened_once.wal_tail());
    assert_eq!(
        reopened_twice.pending_reclaims(),
        reopened_once.pending_reclaims()
    );
}

//= spec/implementation.md#startup-requirements
//= type=test
//# `RING-IMPL-STARTUP-004` Recovery of incomplete WAL rotation,
//# allocation, or reclaim state MUST be expressible through the same
//# operation framework used for normal foreground work.
#[test]
fn blocking_and_future_open_recover_the_same_pending_reclaim_state() {
    let (mut blocking_flash, mut blocking_workspace, _, first_region, second_region) =
        super::super::replace_map_into_pending_reclaim_with_empty_free_list();
    let reopened_blocking =
        Storage::<8, 4>::open::<512, 3, _>(&mut blocking_flash, &mut blocking_workspace).unwrap();

    let (mut future_flash, mut future_workspace, _, _, _) =
        super::super::replace_map_into_pending_reclaim_with_empty_free_list();
    let reopened_future = super::super::poll_until_ready(
        Storage::<8, 4>::open_future::<512, 3, _>(&mut future_flash, &mut future_workspace),
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
