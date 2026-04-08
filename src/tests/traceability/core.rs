use super::*;

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-001` The core library crate MUST compile with
//# `#![no_std]`.
//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-001` The core library crate MUST compile with
//# `#![no_std]`.
#[test]
fn core_library_no_std_target_build_is_enforced_by_verification() {
    // `scripts/verify.sh` proves this requirement by building the
    // library for a no-std target triple.
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-002` The core library crate MUST NOT depend on the
//# Rust `alloc` crate.
//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-002` The core library crate MUST NOT depend on the
//# Rust `alloc` crate.
#[test]
fn core_library_alloc_policy_is_enforced_by_clippy_verification() {
    // The mechanical enforcement for this requirement lives in
    // `clippy.toml`, the crate-level deny configuration in `src/lib.rs`,
    // and the lib-only clippy policy pass in `scripts/verify.sh`.
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-003` The core library crate MUST NOT depend on an
//# async runtime, executor, scheduler, or timer facility.
//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-003` The core library crate MUST NOT depend on an
//# async runtime, executor, scheduler, or timer facility.
#[test]
fn core_library_runtime_policy_is_enforced_by_verification() {
    // `scripts/verify.sh` rejects banned runtime-style dependencies
    // through `cargo tree`, and `clippy.toml` rejects source-level use
    // of runtime or timer APIs in the non-test library target.
}

//= spec/implementation.md#non-goal-requirements
//# `RING-IMPL-NONGOAL-001` Borromean core MUST NOT require a specific
//# embedded framework, RTOS, or async executor.
//= spec/implementation.md#non-goal-requirements
//= type=test
//# `RING-IMPL-NONGOAL-001` Borromean core MUST NOT require a specific
//# embedded framework, RTOS, or async executor.
#[test]
fn core_library_framework_and_rtos_policy_is_enforced_by_verification() {
    // `scripts/verify.sh` rejects framework, RTOS, and executor
    // dependency declarations through the dependency-tree policy check.
}

//= spec/implementation.md#non-goal-requirements
//# `RING-IMPL-NONGOAL-002` Borromean core MUST NOT assume thread
//# support, background workers, or heap-backed task scheduling.
//= spec/implementation.md#non-goal-requirements
//= type=test
//# `RING-IMPL-NONGOAL-002` Borromean core MUST NOT assume thread
//# support, background workers, or heap-backed task scheduling.
#[test]
fn core_library_thread_and_worker_policy_is_enforced_by_clippy_verification() {
    // The mechanical enforcement for this requirement lives in
    // `clippy.toml`, the crate-level deny configuration in `src/lib.rs`,
    // and the lib-only clippy policy pass in `scripts/verify.sh`.
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-004` The implementation MUST preserve the durable
//# behavior defined by [spec/ring.md](ring.md); this specification MAY
//# constrain implementation structure but MUST NOT weaken ring-level
//# correctness requirements.
//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-004` The implementation MUST preserve the durable
//# behavior defined by [spec/ring.md](ring.md); this specification MAY
//# constrain implementation structure but MUST NOT weaken ring-level
//# correctness requirements.
#[test]
fn storage_facade_preserves_ring_behavior_through_delegating_entry_points() {
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
