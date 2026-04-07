use super::*;

//= spec/implementation.md#architecture-requirements
//# `RING-IMPL-ARCH-002` The backing I/O object MUST instead be passed
//# into operation entry points or operation builders so the same
//# `Storage` value can participate in externally driven async execution.
//= spec/implementation.md#architecture-requirements
//= type=test
//# `RING-IMPL-ARCH-002` The backing I/O object MUST instead be passed
//# into operation entry points or operation builders so the same
//# `Storage` value can participate in externally driven async execution.
#[test]
fn storage_public_entry_points_take_backing_io_from_callers() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    assert!(lib.contains(
        "pub struct Storage<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize> {"
    ));
    assert!(!lib.contains("pub struct Storage<IO"));

    for signature in [
        "pub fn format_future<",
        "pub fn format<",
        "pub fn open_future<'a",
        "pub fn open<const",
        "pub fn create_map_future<",
        "pub fn append_map_update_future<",
        "pub fn flush_map_future<",
        "pub fn drop_map_future<",
    ] {
        assert!(
            lib.contains(signature),
            "missing public entry point {signature}"
        );
    }

    assert!(lib.contains("flash: &'a mut IO"));
    assert!(lib.contains("flash: &mut IO"));
}

//= spec/implementation.md#api-requirements
//# `RING-IMPL-API-002` The public API MUST allow a caller to drive the
//# same storage engine from either blocking test shims or asynchronous
//# device adapters without changing borromean correctness logic.
//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-002` The public API MUST allow a caller to drive the
//# same storage engine from either blocking test shims or asynchronous
//# device adapters without changing borromean correctness logic.
#[test]
fn blocking_and_future_entry_points_produce_equivalent_storage_state() {
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
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut blocking_flash,
            &mut blocking_workspace,
            CollectionId(61),
            &mut blocking_map_buffer,
        )
        .unwrap();
    let mut future_map_buffer = [0u8; REGION_SIZE];
    let future_map = reopened_future
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut future_flash,
            &mut future_workspace,
            CollectionId(61),
            &mut future_map_buffer,
        )
        .unwrap();
    assert_eq!(blocking_map.get(&7).unwrap(), Some(70));
    assert_eq!(future_map.get(&7).unwrap(), Some(70));
}

//= spec/implementation.md#api-requirements
//# `RING-IMPL-API-005` The implementation MAY provide optional helper
//# adapters for common executors or embedded frameworks, but the core
//# crate MUST remain usable without them.
//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-005` The implementation MAY provide optional helper
//# adapters for common executors or embedded frameworks, but the core
//# crate MUST remain usable without them.
#[test]
fn core_api_remains_usable_without_executor_or_framework_helpers() {
    let manifest = strip_comment_lines(&read_repo_file("Cargo.toml"));
    let dependencies = dependency_names(&manifest, "dependencies");
    for banned in [
        "tokio",
        "async-std",
        "embassy-executor",
        "async-executor",
        "futures-executor",
        "rtic",
        "freertos",
        "zephyr",
        "esp-idf",
        "esp_idf",
        "arduino",
    ] {
        assert!(
            !dependencies.contains(banned),
            "core crate unexpectedly requires helper dependency {banned}"
        );
    }

    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    for forbidden in ["pub mod adapters;", "pub mod executor", "pub mod framework"] {
        assert!(
            !lib.contains(forbidden),
            "core crate unexpectedly requires helper surface {forbidden}"
        );
    }

    let mut flash = MockFlash::<256, 5, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut storage =
        Storage::<8, 4>::format::<256, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    storage
        .create_map::<256, 5, _>(&mut flash, &mut workspace, CollectionId(85))
        .unwrap();

    let reopened = Storage::<8, 4>::open::<256, 5, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(reopened.metadata().region_size, 256);
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(85));
}
