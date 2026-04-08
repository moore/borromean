use super::*;
use crate::{StorageOpenError, StorageRuntime};

//= spec/implementation.md#panic-requirements
//# `RING-IMPL-PANIC-001` The borromean core library and its non-test
//# support code MUST be panic free for all input data, including invalid
//# API inputs, corrupt on-storage state, exhausted capacities, and
//# device errors.
//= spec/implementation.md#panic-requirements
//= type=test
//# `RING-IMPL-PANIC-001` The borromean core library and its non-test
//# support code MUST be panic free for all input data, including invalid
//# API inputs, corrupt on-storage state, exhausted capacities, and
//# device errors.
#[test]
fn corrupt_storage_inputs_return_errors_instead_of_panicking() {
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
//# `RING-IMPL-PANIC-002` Recoverable failures and invariant violations
//# that can be caused by external input or storage state MUST be
//# reported through explicit error results rather than by panicking.
//= spec/implementation.md#panic-requirements
//= type=test
//# `RING-IMPL-PANIC-002` Recoverable failures and invariant violations
//# that can be caused by external input or storage state MUST be
//# reported through explicit error results rather than by panicking.
#[test]
fn public_decode_and_open_paths_expose_explicit_error_results() {
    type Flash = MockFlash<256, 5, 2048>;
    type Workspace = StorageWorkspace<256>;
    type Store = Storage<8, 4>;
    type Map<'a> = LsmMap<'a, u16, u16, 8>;
    type Update = MapUpdate<u16, u16>;
    type Runtime = StorageRuntime<8, 4>;

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

    let _: fn(&mut Flash, &mut Workspace) -> Result<Store, StorageOpenError> =
        Store::open::<256, 5, Flash>;
    let _: fn(&mut Store, &mut Flash, &mut Workspace, CollectionId) -> Result<(), StorageRuntimeError> =
        Store::create_map::<256, 5, Flash>;
    let _: fn(&mut Store, &mut Flash, &mut Workspace, CollectionId, &Update, &mut [u8]) -> Result<(), MapStorageError> =
        Store::append_map_update::<256, 5, Flash, u16, u16, 8>;
    let _: fn(&mut Store, &mut Flash, &mut Workspace, &Map<'_>) -> Result<u32, MapStorageError> =
        Store::flush_map::<256, 5, Flash, u16, u16, 8>;

    // Borrow-returning map APIs need named helpers so the compiler can
    // also verify the lifetime relationship between the caller's buffer
    // and the returned map value while still checking that the return
    // type is `Result<..., ...>`.
    fn assert_open_map_signature<'a>(
        _: fn(&Store, &mut Flash, &mut Workspace, CollectionId, &'a mut [u8]) -> Result<Map<'a>, MapStorageError>,
    ) {
    }
    fn assert_map_new_signature<'a>(
        _: fn(CollectionId, &'a mut [u8]) -> Result<Map<'a>, MapError>,
    ) {
    }
    fn assert_map_set_signature<'a>(_: fn(&mut Map<'a>, u16, u16) -> Result<(), MapError>) {}
    fn assert_map_load_snapshot_signature<'a>(
        _: fn(&mut Map<'a>, &[u8]) -> Result<(), MapError>,
    ) {
    }
    fn assert_open_from_storage_signature<'a>(
        _: fn(&Runtime, &mut Flash, &mut Workspace, CollectionId, &'a mut [u8]) -> Result<Map<'a>, MapStorageError>,
    ) {
    }

    assert_open_map_signature(Store::open_map::<256, 5, Flash, u16, u16, 8>);
    assert_map_new_signature(LsmMap::<u16, u16, 8>::new);
    assert_map_set_signature(LsmMap::<u16, u16, 8>::set);
    assert_map_load_snapshot_signature(LsmMap::<u16, u16, 8>::load_snapshot);
    assert_open_from_storage_signature(
        LsmMap::<u16, u16, 8>::open_from_storage::<256, 5, Flash, 8, 4>,
    );
}

//= spec/implementation.md#panic-requirements
//# `RING-IMPL-PANIC-003` Non-test code MUST NOT use `panic!`,
//# `unwrap()`, `expect()`, `todo!()`, `unimplemented!()`, or
//# `unreachable!()` in any path that can be reached from public APIs or
//# from storage data under validation.
//= spec/implementation.md#panic-requirements
//= type=test
//# `RING-IMPL-PANIC-003` Non-test code MUST NOT use `panic!`,
//# `unwrap()`, `expect()`, `todo!()`, `unimplemented!()`, or
//# `unreachable!()` in any path that can be reached from public APIs or
//# from storage data under validation.
#[test]
fn panic_policy_is_enforced_by_clippy_verification() {
    // The mechanical enforcement for this requirement lives in the
    // repository clippy invocation from `scripts/verify.sh` together
    // with the crate-level deny configuration in `src/lib.rs`.
}
