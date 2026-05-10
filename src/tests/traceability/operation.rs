use super::*;
use std::pin::pin;
use std::task::Poll;

//= spec/implementation.md#operation-requirements
//= type=test
//# `RING-IMPL-OP-001` A borromean future MUST NOT require spawning
//# another borromean future in order to complete.
#[test]
fn requirement_each_public_operation_future_completes_when_polled_directly() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 5;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = super::super::poll_ready(Storage::<8, 4>::format_future::<
        REGION_SIZE,
        REGION_COUNT,
        _,
    >(&mut flash, &mut workspace, 1, 8, 0xa5))
    .unwrap();

    super::super::poll_ready(storage.create_map_future::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        CollectionId(82),
    ))
    .unwrap();

    let mut source_buffer = [0u8; REGION_SIZE];
    let mut source = LsmMap::<u16, u16, 8>::new(CollectionId(82), &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    super::super::poll_ready(
        storage.snapshot_map_future::<REGION_SIZE, REGION_COUNT, _, _, _, 8>(
            &mut flash,
            &mut workspace,
            &source,
        ),
    )
    .unwrap();

    let mut payload_buffer = [0u8; 64];
    super::super::poll_ready(
        storage.append_map_update_future::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(82),
            &MapUpdate::Set { key: 2, value: 20 },
            &mut payload_buffer,
        ),
    )
    .unwrap();

    source.set(3, 30).unwrap();
    let committed_region = super::super::poll_until_ready(
        storage.flush_map_future::<REGION_SIZE, REGION_COUNT, _, _, _, 8, 8>(
            &mut flash,
            &mut workspace,
            &mut source,
        ),
        4,
    )
    .unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(committed_region)
    );

    let reclaim_region =
        super::super::poll_ready(storage.drop_map_future::<REGION_SIZE, REGION_COUNT, _>(
            &mut flash,
            &mut workspace,
            CollectionId(82),
        ))
        .unwrap();
    assert_eq!(reclaim_region, Some(committed_region));

    let reopened = super::super::poll_until_ready(
        Storage::<8, 4>::open_future::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace),
        8,
    )
    .unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );

    let (mut flash, mut workspace, mut storage, next_region) =
        super::super::setup_storage_with_stale_wal_head();
    let reclaimed_head = super::super::poll_until_ready(
        storage.reclaim_wal_head_future::<512, 6, _>(&mut flash, &mut workspace),
        6,
    )
    .unwrap();
    assert_eq!(reclaimed_head, next_region);
}

//= spec/implementation.md#operation-requirements
//= type=test
//# `RING-IMPL-OP-004` Pure in-memory state mutations that make a later
//# durable step mandatory MUST occur in an order that allows the same
//# operation to be retried or reconstructed after reset.
#[test]
fn requirement_flush_future_keeps_collection_basis_on_previous_state_until_head_commit() {
    for pending_polls in 1..=2 {
        let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage =
            Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

        storage
            .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(82))
            .unwrap();

        let previous_region = {
            let mut previous_buffer = [0u8; 512];
            let mut previous =
                LsmMap::<u16, u16, 8>::new(CollectionId(82), &mut previous_buffer).unwrap();
            previous.set(1, 10).unwrap();
            storage
                .flush_map::<512, 5, _, _, _, 8, 8>(&mut flash, &mut workspace, &mut previous)
                .unwrap()
        };

        {
            let mut replacement_buffer = [0u8; 512];
            let mut replacement =
                LsmMap::<u16, u16, 8>::new(CollectionId(82), &mut replacement_buffer).unwrap();
            replacement.set(1, 11).unwrap();
            replacement.set(2, 22).unwrap();

            let future = storage.flush_map_future::<512, 5, _, _, _, 8, 8>(
                &mut flash,
                &mut workspace,
                &mut replacement,
            );
            let mut future = pin!(future);

            for _ in 0..pending_polls {
                assert!(matches!(
                    super::super::poll_once(future.as_mut()),
                    Poll::Pending
                ));
            }
        }

        assert_eq!(
            storage.collections()[0].basis(),
            crate::StartupCollectionBasis::Region(previous_region)
        );
    }

    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 6, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 6, _>(&mut flash, &mut workspace, CollectionId(82))
        .unwrap();

    let previous_region = {
        let mut previous_buffer = [0u8; 512];
        let mut previous =
            LsmMap::<u16, u16, 8>::new(CollectionId(82), &mut previous_buffer).unwrap();
        previous.set(1, 10).unwrap();
        storage
            .flush_map::<512, 6, _, _, _, 8, 8>(&mut flash, &mut workspace, &mut previous)
            .unwrap()
    };

    let replacement_region = {
        let mut replacement_buffer = [0u8; 512];
        let mut replacement =
            LsmMap::<u16, u16, 8>::new(CollectionId(82), &mut replacement_buffer).unwrap();
        replacement.set(1, 11).unwrap();
        replacement.set(2, 22).unwrap();

        let future = storage.flush_map_future::<512, 6, _, _, _, 8, 8>(
            &mut flash,
            &mut workspace,
            &mut replacement,
        );
        let mut future = pin!(future);

        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
        match super::super::poll_once(future.as_mut()) {
            Poll::Ready(Ok(region_index)) => region_index,
            other => panic!("unexpected final flush poll result: {other:?}"),
        }
    };

    assert_ne!(replacement_region, previous_region);
    assert_eq!(
        storage.collections()[0].basis(),
        crate::StartupCollectionBasis::Region(replacement_region)
    );
}

//= spec/implementation.md#operation-requirements
//= type=todo
//# `RING-IMPL-OP-005` Public operations SHOULD keep borrows of
//# storage-owned scratch internal to the operation so embedded callers can
//# reuse one `Storage` context across sequential operations.
#[test]
fn todo_storage_owned_scratch_is_reusable_across_operations() {}
