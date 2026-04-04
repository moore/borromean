use super::*;
use ::core::pin::pin;
use ::core::task::Poll;

//= spec/implementation.md#startup-requirements
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
//# `RING-IMPL-STARTUP-002` Startup replay state MUST itself obey the
//# same no-allocation rule as steady-state operation.
#[test]
fn startup_replay_state_uses_fixed_capacity_storage_without_heap_allocation() {
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));

    assert!(startup.contains("pub struct StartupState<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>"));
    assert!(startup.contains("collections: Vec<StartupCollection, MAX_COLLECTIONS>"));
    assert!(startup.contains("pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>"));
    assert!(startup.contains("wal_chain: Vec<u32, REGION_COUNT>"));

    for banned in ["alloc::", "std::vec::Vec", "Box<", "Rc<", "Arc<"] {
        assert!(
            !startup.contains(banned),
            "startup replay state unexpectedly uses heap allocation via {banned}"
        );
    }
}

//= spec/implementation.md#startup-requirements
//# `RING-IMPL-STARTUP-003` If startup needs temporary decode storage,
//# that storage MUST come from a caller-provided workspace or other
//# bounded static storage.
#[test]
fn startup_decode_and_scan_paths_take_workspace_backing_from_callers() {
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));

    for signature in [
        "workspace: &mut StorageWorkspace<REGION_SIZE>",
        "pub(crate) fn begin_open_formatted_store<",
        "pub(crate) fn recover_open_rotation<",
        "pub(crate) fn discover_open_wal_chain<",
        "pub(crate) fn replay_open_wal_chain<",
    ] {
        assert!(startup.contains(signature), "missing startup workspace contract {signature}");
    }

    assert!(startup.contains("let (physical_scratch, logical_scratch) = workspace.encode_buffers();"));
    assert!(startup.contains("let (region_bytes, logical_scratch) = workspace.scan_buffers();"));
    assert!(startup.contains("metadata: StorageMetadata"));
    assert!(startup.contains("wal_chain: Vec<u32, REGION_COUNT>"));
}

//= spec/implementation.md#startup-requirements
//# `RING-IMPL-STARTUP-004` Recovery of incomplete WAL rotation,
//# allocation, or reclaim state MUST be expressible through the same
//# operation framework used for normal foreground work.
#[test]
fn startup_recovery_runs_inside_the_same_operation_phase_framework() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let op_future = strip_comment_lines(&read_repo_file("src/op_future.rs"));

    assert!(lib.contains("pub fn open_future<'a, const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>("));
    assert!(lib.contains("OpenStorageFuture::<"));
    assert!(op_future.contains("enum OpenStoragePhase<"));
    assert!(op_future.contains("RecoverRotation {"));
    assert!(op_future.contains("RecoverPendingReclaims {"));
    assert!(op_future.contains("crate::startup::recover_open_rotation::<"));
    assert!(op_future.contains("runtime.recover_pending_reclaims::<REGION_SIZE, REGION_COUNT, IO>("));
    assert!(op_future.contains("this.phase = OpenStoragePhase::RecoverRotation { plan };"));
    assert!(op_future.contains("this.phase = OpenStoragePhase::RecoverPendingReclaims { runtime };"));
}
