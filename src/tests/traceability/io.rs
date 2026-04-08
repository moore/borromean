use super::*;

//= spec/implementation.md#i-o-requirements
//= type=test
//# `RING-IMPL-IO-001` The borromean I/O abstraction MUST expose only
//# the primitive operations needed to satisfy [spec/ring.md](ring.md):
//# region or metadata reads, writes, erases, and durability barriers.
#[test]
fn flash_io_trait_exposes_only_primitive_storage_operations() {
    struct SurfaceCheckedFlash;

    impl FlashIo for SurfaceCheckedFlash {
        fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, MockError> {
            Ok(None)
        }

        fn write_metadata(&mut self, _metadata: StorageMetadata) -> Result<(), MockError> {
            Ok(())
        }

        fn read_region(
            &mut self,
            _region_index: u32,
            _offset: usize,
            _buffer: &mut [u8],
        ) -> Result<(), MockError> {
            Ok(())
        }

        fn write_region(
            &mut self,
            _region_index: u32,
            _offset: usize,
            _data: &[u8],
        ) -> Result<(), MockError> {
            Ok(())
        }

        fn erase_region(&mut self, _region_index: u32) -> Result<(), MockError> {
            Ok(())
        }

        fn sync(&mut self) -> Result<(), MockError> {
            Ok(())
        }

        fn format_empty_store(
            &mut self,
            _min_free_regions: u32,
            _wal_write_granule: u32,
            _wal_record_magic: u8,
        ) -> Result<StorageMetadata, MockFormatError> {
            Err(MockFormatError::RegionCountTooLarge)
        }
    }

    // This is a compile-time surface test. The impl above names exactly
    // the primitive operations allowed by the requirement. If `FlashIo`
    // gained another required method, or if any of these signatures
    // stopped exposing the expected `Result`-based contract, this test
    // would stop compiling. Runtime-style hook surfaces are separately
    // prohibited by `RING-IMPL-IO-005`.
    let _: fn(&mut SurfaceCheckedFlash) -> Result<Option<StorageMetadata>, MockError> =
        <SurfaceCheckedFlash as FlashIo>::read_metadata;
    let _: fn(&mut SurfaceCheckedFlash, StorageMetadata) -> Result<(), MockError> =
        <SurfaceCheckedFlash as FlashIo>::write_metadata;
    let _: fn(&mut SurfaceCheckedFlash, u32, usize, &mut [u8]) -> Result<(), MockError> =
        <SurfaceCheckedFlash as FlashIo>::read_region;
    let _: fn(&mut SurfaceCheckedFlash, u32, usize, &[u8]) -> Result<(), MockError> =
        <SurfaceCheckedFlash as FlashIo>::write_region;
    let _: fn(&mut SurfaceCheckedFlash, u32) -> Result<(), MockError> =
        <SurfaceCheckedFlash as FlashIo>::erase_region;
    let _: fn(&mut SurfaceCheckedFlash) -> Result<(), MockError> =
        <SurfaceCheckedFlash as FlashIo>::sync;
    let _: fn(&mut SurfaceCheckedFlash, u32, u32, u8) -> Result<StorageMetadata, MockFormatError> =
        <SurfaceCheckedFlash as FlashIo>::format_empty_store;
}

//= spec/implementation.md#i-o-requirements
//= type=test
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
//= type=test
//# `RING-IMPL-IO-003` The borromean I/O abstraction MUST be usable
//# without dynamic dispatch and without heap allocation.
#[test]
fn flash_io_trait_supports_non_allocating_concrete_driver_usage() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 5;

    assert_no_alloc("FlashIo concrete-driver operation path", || {
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
            .create_map::<REGION_SIZE, REGION_COUNT, _>(
                &mut flash,
                &mut workspace,
                CollectionId(63),
            )
            .unwrap();
        assert_eq!(storage.collections()[0].collection_id(), CollectionId(63));
    });
}

//= spec/implementation.md#i-o-requirements
//= type=test
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
//= type=test
//# `RING-IMPL-IO-005` Borromean MUST treat wakeups, DMA completion, or
//# interrupt delivery as an external concern of the caller-provided I/O
//# implementation rather than as an internal runtime service.
#[test]
fn flash_io_runtime_hook_policy_is_enforced_by_clippy_verification() {
    // The mechanical enforcement for this requirement lives in
    // `clippy.toml`, the crate-level deny configuration in `src/lib.rs`,
    // and the lib-only clippy policy pass in `scripts/verify.sh`.
}
