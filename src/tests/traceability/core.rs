use super::*;

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-001` The core library crate MUST compile with
//# `#![no_std]`.
#[test]
fn core_library_crate_declares_no_std() {
    let lib = read_repo_file("src/lib.rs");
    assert!(lib.contains("#![no_std]"));
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-002` The core library crate MUST NOT depend on the
//# Rust `alloc` crate.
#[test]
fn core_library_crate_avoids_alloc_dependency_and_usage() {
    let manifest = strip_comment_lines(&read_repo_file("Cargo.toml"));
    let dependencies = dependency_names(&manifest, "dependencies");
    assert!(!dependencies.contains("alloc"));

    for (path, source) in non_test_sources_without_comments() {
        assert!(
            !source.contains("alloc::"),
            "non-test source unexpectedly references alloc in {}",
            path.display()
        );
        assert!(
            !source.contains("extern crate alloc"),
            "non-test source unexpectedly imports alloc in {}",
            path.display()
        );
    }
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-003` The core library crate MUST NOT depend on an
//# async runtime, executor, scheduler, or timer facility.
#[test]
fn core_library_crate_has_no_async_runtime_or_timer_dependencies() {
    let manifest = strip_comment_lines(&read_repo_file("Cargo.toml"));
    let dependencies = dependency_names(&manifest, "dependencies");
    for banned in [
        "tokio",
        "async-std",
        "smol",
        "glommio",
        "embassy-executor",
        "async-executor",
        "futures-executor",
        "futures-timer",
    ] {
        assert!(
            !dependencies.contains(banned),
            "unexpected runtime-style dependency {banned}"
        );
    }

    for (path, source) in non_test_sources_without_comments() {
        for banned in [
            "tokio::",
            "async_std::",
            "smol::",
            "glommio::",
            "embassy_executor::",
            "futures_timer::",
        ] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly references {banned} in {}",
                path.display()
            );
        }
    }
}

//= spec/implementation.md#non-goal-requirements
//# `RING-IMPL-NONGOAL-001` Borromean core MUST NOT require a specific
//# embedded framework, RTOS, or async executor.
#[test]
fn core_library_crate_requires_no_embedded_framework_or_rtos_dependency() {
    let manifest = strip_comment_lines(&read_repo_file("Cargo.toml"));
    let dependencies = dependency_names(&manifest, "dependencies");
    for dependency in dependencies {
        assert!(
            ![
                "embassy",
                "rtic",
                "freertos",
                "zephyr",
                "esp-idf",
                "esp_idf",
                "arduino",
            ]
            .iter()
            .any(|prefix| dependency.starts_with(prefix)),
            "unexpected framework or RTOS dependency {dependency}"
        );
    }
}

//= spec/implementation.md#non-goal-requirements
//# `RING-IMPL-NONGOAL-002` Borromean core MUST NOT assume thread
//# support, background workers, or heap-backed task scheduling.
#[test]
fn core_library_crate_assumes_no_threads_or_background_workers() {
    for (path, source) in non_test_sources_without_comments() {
        for banned in [
            "std::thread",
            "thread::spawn",
            "spawn_blocking",
            "JoinHandle",
            "crossbeam",
            "tokio::spawn",
            "async_std::task::spawn",
            "std::sync::mpsc",
        ] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly references {banned} in {}",
                path.display()
            );
        }
    }
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-004` The implementation MUST preserve the durable
//# behavior defined by [spec/ring.md](ring.md); this specification MAY
//# constrain implementation structure but MUST NOT weaken ring-level
//# correctness requirements.
#[test]
fn storage_facade_preserves_ring_behavior_through_delegating_entry_points() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    for delegation in [
        "state: storage::format::<",
        "state: storage::open::<",
        ".append_new_collection::<REGION_SIZE, REGION_COUNT, IO>(",
        ".append_update::<REGION_SIZE, REGION_COUNT, IO>(",
        ".append_snapshot::<REGION_SIZE, REGION_COUNT, IO>(",
        ".append_head::<REGION_SIZE, REGION_COUNT, IO>(",
        ".append_reclaim_begin::<REGION_SIZE, REGION_COUNT, IO>(",
        ".reclaim_wal_head::<REGION_SIZE, REGION_COUNT, IO>(",
        "LsmMap::<K, V, MAX_INDEXES>::open_from_storage::<",
    ] {
        assert!(
            lib.contains(delegation),
            "missing delegation to ring-behavior module {delegation}"
        );
    }

    let storage_src = read_repo_file("src/storage.rs");
    let startup_src = read_repo_file("src/startup.rs");
    let map_src = read_repo_file("src/collections/map/mod.rs");
    for ring_trace in [
        "RING-CORE-010",
        "RING-CORE-011",
        "RING-STARTUP-007",
        "RING-STARTUP-026",
        "RING-FORMAT-005",
        "RING-FORMAT-006",
    ] {
        assert!(
            storage_src.contains(ring_trace)
                || startup_src.contains(ring_trace)
                || map_src.contains(ring_trace),
            "expected ring-level requirement trace {ring_trace}"
        );
    }

    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(86))
        .unwrap();

    let mut payload_buffer = [0u8; 64];
    storage
        .append_map_update::<512, 5, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(86),
            &MapUpdate::Set { key: 7, value: 70 },
            &mut payload_buffer,
        )
        .unwrap();

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    let mut map_buffer = [0u8; 512];
    let map = reopened
        .open_map::<512, 5, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(86),
            &mut map_buffer,
        )
        .unwrap();
    assert_eq!(map.get(&7).unwrap(), Some(70));
}
