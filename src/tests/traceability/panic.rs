use super::*;
use crate::{MapFrontierMemory, StorageMemory, StorageOpenError, StorageRuntime};

//= spec/implementation.md#panic-requirements
//= type=test
//# `RING-IMPL-PANIC-001` The borromean core library and its non-test
//# support code MUST be panic free for all input data, including invalid
//# API inputs, corrupt on-storage state, exhausted capacities, and
//# device errors.
#[test]
fn requirement_corrupt_storage_inputs_return_errors_instead_of_panicking() {
    let metadata = StorageMetadata::new(128, 4, 1, 8, 0xff, 0xa5).unwrap();

    let mut metadata_bytes = [0u8; StorageMetadata::ENCODED_LEN];
    metadata.encode_into(&mut metadata_bytes).unwrap();
    metadata_bytes[StorageMetadata::ENCODED_LEN - 1] ^= 0x01;
    assert!(matches!(
        StorageMetadata::decode(&metadata_bytes),
        Err(DiskError::InvalidChecksum)
    ));

    let header = Header {
        sequence: 1,
        collection_id: CollectionId(1),
        collection_format: 2,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    header_bytes[Header::ENCODED_LEN - 1] ^= 0x01;
    assert!(matches!(
        Header::decode(&header_bytes),
        Err(DiskError::InvalidChecksum)
    ));

    let prologue = WalRegionPrologue {
        wal_head_region_index: 0,
    };
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue
        .encode_into(&mut prologue_bytes, metadata.region_count)
        .unwrap();
    prologue_bytes[WalRegionPrologue::ENCODED_LEN - 1] ^= 0x01;
    assert!(matches!(
        WalRegionPrologue::decode(&prologue_bytes, metadata.region_count),
        Err(DiskError::InvalidChecksum)
    ));

    let footer = FreePointerFooter { next_tail: Some(2) };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer
        .encode_into(&mut footer_bytes, metadata.erased_byte)
        .unwrap();
    footer_bytes[FreePointerFooter::ENCODED_LEN - 1] ^= 0x01;
    assert!(matches!(
        FreePointerFooter::decode(&footer_bytes, metadata.erased_byte),
        Err(DiskError::InvalidChecksum)
    ));
}

//= spec/implementation.md#panic-requirements
//= type=test
//# `RING-IMPL-PANIC-002` Recoverable failures and invariant violations
//# that can be caused by external input or storage state MUST be
//# reported through explicit error results rather than by panicking.
#[test]
fn requirement_public_decode_and_open_paths_expose_explicit_error_results() {
    type Flash = MockFlash<256, 5, 2048>;
    type Workspace = StorageWorkspace<256>;
    type Store<'db, 'mem> = Storage<'db, 'mem, Flash, 256, 5>;
    type Map<'a> = MapFrontier<'a, u16, u16, 8>;
    type Update = MapUpdate<u16, u16>;
    type Runtime = StorageRuntime<8>;
    type CreateMapFn<'db, 'mem> =
        fn(&mut Store<'db, 'mem>, CollectionId) -> Result<(), StorageRuntimeError>;
    type AppendMapUpdateFn<'db, 'mem> =
        fn(&mut Store<'db, 'mem>, CollectionId, &Update) -> Result<(), MapStorageError>;
    type OpenMapFn<'db, 'mem, 'a> = fn(
        &mut Store<'db, 'mem>,
        CollectionId,
        &'a mut [u8],
        &'a mut MapFrontierMemory<u16, 8>,
    ) -> Result<Map<'a>, MapStorageError>;
    type OpenFromStorageFn<'a> = fn(
        &Runtime,
        &mut Flash,
        &mut Workspace,
        &mut [u8],
        CollectionId,
        &'a mut [u8],
        &'a mut MapFrontierMemory<u16, 8>,
    ) -> Result<Map<'a>, MapStorageError>;

    // This is an API-shape test. It does not exercise runtime behavior;
    // instead it asks the compiler to prove that these fallible entry
    // points have `Result<..., ...>` return types. If any of them were
    // changed to return a bare value, panic internally, or otherwise
    // stop exposing an explicit error channel in the signature, these
    // bindings would stop compiling.

    // Direct bindings are enough for signatures that do not return
    // values borrowing from an input buffer.
    let _: fn(u32, u32, u32, u32, u8, u8) -> Result<StorageMetadata, DiskError> =
        StorageMetadata::new;
    let _: fn(&StorageMetadata) -> Result<(), DiskError> = StorageMetadata::validate;
    let _: fn(&[u8]) -> Result<StorageMetadata, DiskError> = StorageMetadata::decode;

    fn assert_storage_open_signature<'db, 'mem>(
        _: fn(
            &'db mut Flash,
            &'mem mut StorageMemory<256, 5>,
        ) -> Result<Store<'db, 'mem>, StorageOpenError>,
    ) {
    }
    fn assert_storage_flush_signature<'db, 'mem, 'map>(
        _: fn(&mut Store<'db, 'mem>, &mut Map<'map>) -> Result<u32, MapStorageError>,
    ) {
    }
    assert_storage_open_signature(Storage::<Flash, 256, 5>::open);
    let _: CreateMapFn<'_, '_> = Storage::<Flash, 256, 5>::create_map;
    let _: AppendMapUpdateFn<'_, '_> =
        Storage::<Flash, 256, 5, 8>::append_map_update::<u16, u16, 8>;
    assert_storage_flush_signature(Storage::<Flash, 256, 5, 8>::flush_map::<u16, u16, 8, 8>);

    // Borrow-returning map APIs need named helpers so the compiler can
    // also verify the lifetime relationship between the caller's buffer
    // and the returned map value while still checking that the return
    // type is `Result<..., ...>`.
    fn assert_open_map_signature<'db, 'mem, 'a>(_: OpenMapFn<'db, 'mem, 'a>) {}
    fn assert_map_new_signature<'a>(
        _: fn(
            CollectionId,
            &'a mut [u8],
            &'a mut MapFrontierMemory<u16, 8>,
        ) -> Result<Map<'a>, MapError>,
    ) {
    }
    fn assert_map_set_signature<'a>(_: fn(&mut Map<'a>, u16, u16) -> Result<(), MapError>) {}
    fn assert_map_load_snapshot_signature<'a>(_: fn(&mut Map<'a>, &[u8]) -> Result<(), MapError>) {}
    fn assert_open_from_storage_signature<'a>(_: OpenFromStorageFn<'a>) {}

    assert_open_map_signature(Storage::<Flash, 256, 5, 8>::open_map::<u16, u16, 8, 8>);
    assert_map_new_signature(MapFrontier::<u16, u16, 8>::new);
    assert_map_set_signature(MapFrontier::<u16, u16, 8>::set);
    assert_map_load_snapshot_signature(MapFrontier::<u16, u16, 8>::load_snapshot);
    assert_open_from_storage_signature(
        MapFrontier::<u16, u16, 8>::open_from_storage::<256, 5, Flash, 8>,
    );
}

//= spec/implementation.md#panic-requirements
//= type=test
//# `RING-IMPL-PANIC-003` Non-test code MUST NOT use `panic!`,
//# `unwrap()`, `expect()`, `todo!()`, `unimplemented!()`, or
//# `unreachable!()` in any path that can be reached from public APIs or
//# from storage data under validation.
#[test]
fn requirement_panic_policy_is_enforced_by_clippy_verification() {
    // The mechanical enforcement for this requirement lives in the
    // repository clippy invocation from `scripts/verify.sh` together
    // with the crate-level deny configuration in `src/lib.rs`.
}
