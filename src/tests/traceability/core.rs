use super::*;

//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-001` The core library crate MUST compile with
//# `#![no_std]`.
#[test]
fn requirement_core_library_no_std_target_build_is_enforced_by_verification() {
    // `scripts/verify.sh` proves this requirement by building the
    // library for a no-std target triple.
}

//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-002` The core library crate MUST NOT depend on the
//# Rust `alloc` crate.
#[test]
fn requirement_core_library_alloc_policy_is_enforced_by_clippy_verification() {
    // The mechanical enforcement for this requirement lives in
    // `clippy.toml`, the crate-level deny configuration in `src/lib.rs`,
    // and the lib-only clippy policy pass in `scripts/verify.sh`.
}

//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-003` The core library crate MUST NOT depend on an
//# async runtime, executor, scheduler, or timer facility.
#[test]
fn requirement_core_library_runtime_policy_is_enforced_by_verification() {
    // `scripts/verify.sh` rejects banned runtime-style dependencies
    // through `cargo tree`, and `clippy.toml` rejects source-level use
    // of runtime or timer APIs in the non-test library target.
}

//= spec/implementation.md#non-goal-requirements
//= type=test
//# `RING-IMPL-NONGOAL-001` Borromean core MUST NOT require a specific
//# embedded framework, RTOS, or async executor.
#[test]
fn requirement_core_library_framework_and_rtos_policy_is_enforced_by_verification() {
    // `scripts/verify.sh` rejects framework, RTOS, and executor
    // dependency declarations through the dependency-tree policy check.
}

//= spec/implementation.md#non-goal-requirements
//= type=test
//# `RING-IMPL-NONGOAL-002` Borromean core MUST NOT assume thread
//# support, background workers, or heap-backed task scheduling.
#[test]
fn requirement_core_library_thread_and_worker_policy_is_enforced_by_clippy_verification() {
    // The mechanical enforcement for this requirement lives in
    // `clippy.toml`, the crate-level deny configuration in `src/lib.rs`,
    // and the lib-only clippy policy pass in `scripts/verify.sh`.
}

//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-004` The implementation MUST preserve the durable
//# behavior defined by [spec/ring/00-introduction.md](ring/00-introduction.md); this specification
//# MAY constrain implementation structure but MUST NOT weaken ring-level
//# correctness requirements.
#[test]
fn requirement_storage_facade_preserves_ring_behavior_through_delegating_entry_points() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(86)).unwrap();

    let mut payload_buffer = [0u8; 64];
    storage
        .append_map_update::<u16, u16>(CollectionId(86), &MapUpdate::Set { key: 7, value: 70 })
        .unwrap();

    let mut reopened =
        Storage::<_, 512, 5, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    let mut map_buffer = [0u8; 512];
    let map = reopened
        .open_map::<u16, u16, 8>(
            CollectionId(86),
            &mut map_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(map.get_frontier(&7).unwrap(), Some(70));
}
