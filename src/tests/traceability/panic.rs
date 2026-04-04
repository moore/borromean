use super::*;

//= spec/implementation.md#panic-requirements
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
#[test]
fn public_decode_and_open_paths_expose_explicit_error_results() {
    let disk = strip_comment_lines(&read_repo_file("src/disk.rs"));
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));

    assert!(disk.contains("pub fn new("));
    assert!(disk.contains(") -> Result<Self, DiskError>"));
    assert!(disk.contains("pub fn validate(&self) -> Result<(), DiskError>"));
    assert!(disk.contains("pub fn decode(buffer: &[u8]) -> Result<Self, DiskError>"));
    assert!(lib.contains(") -> Result<Self, StorageOpenError>"));
    assert!(lib.contains(") -> Result<(), MapStorageError>"));
    assert!(map.contains(") -> Result<Self, MapStorageError>"));
    assert!(map.contains(") -> Result<(), MapError>"));
    assert!(storage.contains(") -> Result<(), StorageRuntimeError>"));
    assert!(storage.contains(") -> Result<u32, StorageRuntimeError>"));
}

//= spec/implementation.md#panic-requirements
//# `RING-IMPL-PANIC-003` Non-test code MUST NOT use `panic!`,
//# `unwrap()`, `expect()`, `todo!()`, `unimplemented!()`, or
//# `unreachable!()` in any path that can be reached from public APIs or
//# from storage data under validation.
#[test]
fn non_test_code_avoids_forbidden_panic_primitives() {
    let lib = read_repo_file("src/lib.rs");
    assert!(lib.contains("clippy::unwrap_used"));
    assert!(lib.contains("clippy::expect_used"));
    assert!(lib.contains("clippy::panic"));
    assert!(lib.contains("clippy::todo"));
    assert!(lib.contains("clippy::unimplemented"));
    assert!(lib.contains("clippy::unreachable"));

    for (path, source) in non_test_sources_without_comments() {
        for banned in [
            "panic!(",
            "unwrap(",
            "expect(",
            "todo!(",
            "unimplemented!(",
            "unreachable!(",
        ] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly references {banned} in {}",
                path.display()
            );
        }
    }
}
