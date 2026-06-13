use super::*;
use std::pin::pin;
use std::task::Poll;

//= spec/implementation.md#operation-requirements
//= type=test
//# `RING-IMPL-OP-001` A Borromean future MUST NOT require spawning
//# another Borromean future in order to complete.
#[test]
fn requirement_each_public_operation_future_completes_when_polled_directly() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 5;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut storage =
        super::super::poll_ready(Storage::<_, REGION_SIZE, REGION_COUNT, 8>::format_future(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        ))
        .unwrap();

    super::super::poll_ready(storage.create_map_future(CollectionId(82))).unwrap();

    let mut source_buffer = [0u8; REGION_SIZE];
    let mut source = MapFrontier::<u16, u16, 8>::new(
        CollectionId(82),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source.set_in_memory(1, 10).unwrap();
    super::super::poll_ready(storage.snapshot_map_future(&source)).unwrap();

    super::super::poll_ready(storage.append_map_update_future::<u16, u16>(
        CollectionId(82),
        &MapUpdate::Set { key: 2, value: 20 },
    ))
    .unwrap();

    source.set_in_memory(3, 30).unwrap();
    let committed_region =
        super::super::poll_until_ready(storage.flush_map_future::<_, _, 8>(&mut source), 4)
            .unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(committed_region)
    );

    let reclaim_region =
        super::super::poll_ready(storage.drop_map_future(CollectionId(82))).unwrap();
    assert_eq!(reclaim_region, Some(committed_region));

    drop(storage);
    let reopened = super::super::poll_until_ready(
        Storage::<_, REGION_SIZE, REGION_COUNT, 8>::open_future(
            &mut flash,
            crate::test_storage_memory(),
        ),
        8,
    )
    .unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );

    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, next_region) = super::super::setup_storage_with_stale_wal_head(&mut flash);
    let reclaimed_head =
        super::super::poll_until_ready(storage.reclaim_wal_head_future(), 16).unwrap();
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.runtime().wal_head(), next_region);
}

//= spec/implementation.md#operation-requirements
//= type=test
//# `RING-IMPL-OP-004` Pure in-memory state mutations that make a later
//# durable step mandatory MUST occur in an order that allows the same
//# operation to be retried or reconstructed after reset.
#[test]
fn requirement_flush_future_keeps_collection_basis_on_previous_state_until_head_commit() {
    assert_flush_future_keeps_previous_basis_while_pending();
    assert_flush_future_updates_basis_after_head_commit();
}

fn assert_flush_future_keeps_previous_basis_while_pending() {
    for pending_polls in 1..=2 {
        let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage = Storage::<_, 512, 7, 8>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        storage.create_map(CollectionId(82)).unwrap();

        let previous_region = {
            let mut previous_buffer = [0u8; 512];
            let mut previous = MapFrontier::<u16, u16, 8>::new(
                CollectionId(82),
                &mut previous_buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
            previous.set_in_memory(1, 10).unwrap();
            storage.flush_map::<_, _, 8>(&mut previous).unwrap()
        };

        {
            let mut replacement_buffer = [0u8; 512];
            let mut replacement = MapFrontier::<u16, u16, 8>::new(
                CollectionId(82),
                &mut replacement_buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
            replacement.set_in_memory(1, 11).unwrap();
            replacement.set_in_memory(2, 22).unwrap();

            let future = storage.flush_map_future::<_, _, 8>(&mut replacement);
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
}

fn assert_flush_future_updates_basis_after_head_commit() {
    let mut flash = MockFlash::<512, 8, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 8, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(82)).unwrap();

    let previous_region = {
        let mut previous_buffer = [0u8; 512];
        let mut previous = MapFrontier::<u16, u16, 8>::new(
            CollectionId(82),
            &mut previous_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
        previous.set_in_memory(1, 10).unwrap();
        storage.flush_map::<_, _, 8>(&mut previous).unwrap()
    };

    let replacement_region = {
        let mut replacement_buffer = [0u8; 512];
        let mut replacement = MapFrontier::<u16, u16, 8>::new(
            CollectionId(82),
            &mut replacement_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
        replacement.set_in_memory(1, 11).unwrap();
        replacement.set_in_memory(2, 22).unwrap();

        let future = storage.flush_map_future::<_, _, 8>(&mut replacement);
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
//= type=test
//# `RING-IMPL-OP-005` Public operations SHOULD keep borrows of
//# caller-owned scratch internal to the operation so embedded callers can
//# reuse one `Storage` context across sequential operations.
#[test]
fn requirement_storage_owned_scratch_is_reusable_across_operations() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let mut storage = Storage::<_, 512, 6, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(83)).unwrap();
    storage
        .append_map_update::<u16, u16>(CollectionId(83), &MapUpdate::Set { key: 1, value: 10 })
        .unwrap();
    storage
        .append_map_update::<u16, u16>(CollectionId(83), &MapUpdate::Set { key: 2, value: 20 })
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let map = storage
        .open_map::<u16, u16, 8>(
            CollectionId(83),
            &mut map_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(map.get_frontier(&2).unwrap(), Some(20));
    assert_eq!(storage.mode(), StorageMode::Idle);
}
