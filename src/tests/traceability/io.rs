use super::std::vec;
use super::*;

//= spec/implementation.md#i-o-requirements
//# `RING-IMPL-IO-001` The borromean I/O abstraction MUST expose only
//# the primitive operations needed to satisfy [spec/ring.md](ring.md):
//# region or metadata reads, writes, erases, and durability barriers.
#[test]
fn flash_io_trait_exposes_only_primitive_storage_operations() {
    let methods = flash_io_method_names();
    assert_eq!(
        methods,
        vec![
            "read_metadata".to_string(),
            "write_metadata".to_string(),
            "read_region".to_string(),
            "write_region".to_string(),
            "erase_region".to_string(),
            "sync".to_string(),
            "format_empty_store".to_string(),
        ]
    );
}

//= spec/implementation.md#i-o-requirements
//# `RING-IMPL-IO-002` The borromean I/O abstraction MUST be generic
//# over the caller's concrete transport or flash driver type.
#[test]
fn flash_io_trait_accepts_caller_defined_driver_types() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 5;
    let mut flash = ForwardingFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        1,
        8,
        0xa5,
    )
    .unwrap();
    storage
        .create_map::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, CollectionId(62))
        .unwrap();
    assert_eq!(storage.collections()[0].collection_id(), CollectionId(62));
}

//= spec/implementation.md#i-o-requirements
//# `RING-IMPL-IO-003` The borromean I/O abstraction MUST be usable
//# without dynamic dispatch and without heap allocation.
#[test]
fn flash_io_trait_avoids_dynamic_dispatch_surfaces() {
    for (path, source) in non_test_sources_without_comments() {
        for banned in [
            "dyn FlashIo",
            "Box<dyn FlashIo",
            "&dyn FlashIo",
            "Arc<dyn FlashIo",
            "Rc<dyn FlashIo",
        ] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly references {banned} in {}",
                path.display()
            );
        }
    }
}

//= spec/implementation.md#i-o-requirements
//# `RING-IMPL-IO-004` If the target medium does not require an
//# explicit durability barrier, the I/O abstraction MAY implement sync as
//# a zero-cost completed operation.
#[test]
fn mock_flash_sync_can_complete_immediately() {
    let mut flash = MockFlash::<128, 4, 8>::new(0xff);
    flash.clear_operations();
    flash.sync().unwrap();
    assert_eq!(flash.operations(), &[MockOperation::Sync]);
}

//= spec/implementation.md#i-o-requirements
//# `RING-IMPL-IO-005` Borromean MUST treat wakeups, DMA completion, or
//# interrupt delivery as an external concern of the caller-provided I/O
//# implementation rather than as an internal runtime service.
#[test]
fn flash_io_surface_leaves_wakeup_and_interrupt_delivery_external() {
    for name in flash_io_method_names() {
        for forbidden in ["wake", "waker", "callback", "interrupt", "dma", "register"] {
            assert!(
                !name.contains(forbidden),
                "FlashIo unexpectedly exposes runtime-style hook {name}"
            );
        }
    }

    for relative in ["src/flash_io.rs", "src/lib.rs", "src/op_future.rs"] {
        let source = strip_comment_lines(&read_repo_file(relative));
        for forbidden in [
            "register_waker",
            "callback",
            "interrupt",
            "dma",
            "tokio::spawn",
            "async_std::task::spawn",
        ] {
            assert!(
                !source.contains(forbidden),
                "unexpected runtime-owned I/O concern {forbidden} in {relative}"
            );
        }
    }
}
