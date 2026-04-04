use super::*;
use ::core::pin::pin;
use ::core::task::Poll;

//= spec/implementation.md#operation-requirements
//# `RING-IMPL-OP-001` A borromean future MUST NOT require spawning
//# another borromean future in order to complete.
#[test]
fn borromean_futures_do_not_spawn_other_borromean_futures() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let op_future = strip_comment_lines(&read_repo_file("src/op_future.rs"));

    for constructor in [
        "run_once(move || {",
        "self.create_map::<REGION_SIZE, REGION_COUNT, IO>(",
        "self.snapshot_map::<REGION_SIZE, REGION_COUNT, IO, K, V, MAX_INDEXES>(",
        "self.append_map_update::<REGION_SIZE, REGION_COUNT, IO, K, V, MAX_INDEXES>(",
        "self.drop_map::<REGION_SIZE, REGION_COUNT, IO>(",
        "OpenStorageFuture::<",
        "ReclaimWalHeadFuture::<",
        "FlushMapFuture::<",
    ] {
        assert!(
            lib.contains(constructor),
            "missing direct future construction path {constructor}"
        );
    }

    for banned in [
        "format_future::<",
        "open_future::<",
        "reclaim_wal_head_future::<",
        "create_map_future::<",
        "snapshot_map_future::<",
        "append_map_update_future::<",
        "flush_map_future::<",
        "drop_map_future::<",
        ".await",
        "spawn(",
    ] {
        assert!(
            !op_future.contains(banned),
            "operation future implementation unexpectedly nests {banned}"
        );
    }
}

//= spec/implementation.md#operation-requirements
//# `RING-IMPL-OP-004` Pure in-memory state mutations that make a later
//# durable step mandatory MUST occur in an order that allows the same
//# operation to be retried or reconstructed after reset.
#[test]
fn flush_future_keeps_collection_basis_on_previous_state_until_head_commit() {
    for pending_polls in 1..=3 {
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
                .flush_map::<512, 5, _, _, _, 8>(&mut flash, &mut workspace, &previous)
                .unwrap()
        };

        {
            let mut replacement_buffer = [0u8; 512];
            let mut replacement =
                LsmMap::<u16, u16, 8>::new(CollectionId(82), &mut replacement_buffer).unwrap();
            replacement.set(1, 11).unwrap();
            replacement.set(2, 22).unwrap();

            let future = storage.flush_map_future::<512, 5, _, _, _, 8>(
                &mut flash,
                &mut workspace,
                &replacement,
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
            .flush_map::<512, 5, _, _, _, 8>(&mut flash, &mut workspace, &previous)
            .unwrap()
    };

    let replacement_region = {
        let mut replacement_buffer = [0u8; 512];
        let mut replacement =
            LsmMap::<u16, u16, 8>::new(CollectionId(82), &mut replacement_buffer).unwrap();
        replacement.set(1, 11).unwrap();
        replacement.set(2, 22).unwrap();

        let future = storage.flush_map_future::<512, 5, _, _, _, 8>(
            &mut flash,
            &mut workspace,
            &replacement,
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
//# `RING-IMPL-OP-005` Public operations SHOULD minimize the duration of
//# mutable borrows of large caller workspaces so embedded callers can
//# reuse buffers across sequential operations.
#[test]
fn flush_future_limits_large_workspace_borrows_to_the_region_encoding_step() {
    let op_future = strip_comment_lines(&read_repo_file("src/op_future.rs"));

    assert!(op_future.contains("pub struct FlushMapFuture<"));
    assert!(op_future.contains("workspace: &'a mut StorageWorkspace<REGION_SIZE>"));
    assert!(op_future.contains("phase: FlushMapPhase"));
    assert!(op_future.contains("let (payload, _) = this.workspace.encode_buffers();"));
    assert!(op_future.contains("let used = this.map.encode_region_into(payload)?;"));
    assert!(op_future.contains("this.phase = match previous_region {"));

    for banned in ["payload: &'a", "region_bytes:", "logical_scratch:"] {
        assert!(
            !op_future.contains(banned),
            "flush future unexpectedly retains large workspace borrow state via {banned}"
        );
    }
}
