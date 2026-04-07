use super::*;

//= spec/implementation.md#arithmetic-requirements
//# `RING-IMPL-ARITH-001` Integer arithmetic that can affect storage
//# layout, region addressing, WAL offsets, lengths, indexes,
//# capacities, or sequence advancement MUST use checked arithmetic or
//# an equivalent construction that makes overflow and underflow
//# impossible by construction.
//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-001` Integer arithmetic that can affect storage
//# layout, region addressing, WAL offsets, lengths, indexes,
//# capacities, or sequence advancement MUST use checked arithmetic or
//# an equivalent construction that makes overflow and underflow
//# impossible by construction.
#[test]
fn storage_and_codec_boundaries_use_checked_arithmetic_primitives() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));
    let wal = strip_comment_lines(&read_repo_file("src/wal_record.rs"));

    assert!(lib.contains("checked_add(1)"));
    assert!(lib.contains("u32::try_from("));
    assert!(storage.contains(".checked_sub(Header::ENCODED_LEN)"));
    assert!(storage.contains(".checked_sub(FreePointerFooter::ENCODED_LEN)"));
    assert!(storage.contains(".checked_add(1)"));
    assert!(storage.contains("usize::try_from(metadata.region_size)"));
    assert!(map.contains("entry_count.checked_mul(ENTRY_REF_SIZE)?"));
    assert!(map.contains("end.checked_sub(start)"));
    assert!(map.contains("u32::try_from(snapshot_len)"));
    assert!(startup.contains("usize::try_from(metadata.region_size)"));
    assert!(startup.contains(".checked_add(granule)"));
    assert!(wal.contains("usize::try_from(payload_len)"));
    assert!(wal.contains(".checked_add(payload_len)"));
}

//= spec/implementation.md#arithmetic-requirements
//# `RING-IMPL-ARITH-002` If such arithmetic cannot be proven safe by
//# construction and a checked operation fails, the implementation MUST
//# return an explicit error rather than wrap, saturate, or silently
//# truncate.
//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-002` If such arithmetic cannot be proven safe by
//# construction and a checked operation fails, the implementation MUST
//# return an explicit error rather than wrap, saturate, or silently
//# truncate.
#[test]
fn checked_arithmetic_failures_map_to_explicit_failure_types() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));
    let wal = strip_comment_lines(&read_repo_file("src/wal_record.rs"));

    assert!(lib.contains("pub fn increment(&self) -> Option<Self>"));
    assert!(storage.contains("ok_or(StorageRuntimeError::CommittedRegionTooLarge"));
    assert!(storage.contains("ok_or(StorageRuntimeError::WalRotationRequired)"));
    assert!(map.contains("map_err(|_| MapError::SerializationError)"));
    assert!(map.contains("ok_or(MapError::SerializationError)"));
    assert!(startup.contains("map_err(|_| StartupError::LengthOverflow)"));
    assert!(startup.contains("ok_or(StartupError::LengthOverflow)"));
    assert!(wal.contains("map_err(|_| WalRecordError::LengthOverflow)"));
    assert!(wal.contains("ok_or(WalRecordError::LengthOverflow)"));
}

//= spec/implementation.md#arithmetic-requirements
//# `RING-IMPL-ARITH-003` The implementation MUST NOT rely on wrapping
//# integer behavior for correctness unless a future disk-format
//# requirement explicitly defines modulo arithmetic for that field.
//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-003` The implementation MUST NOT rely on wrapping
//# integer behavior for correctness unless a future disk-format
//# requirement explicitly defines modulo arithmetic for that field.
#[test]
fn non_test_sources_avoid_wrapping_saturating_and_overflowing_arithmetic() {
    for (path, source) in non_test_sources_without_comments() {
        for banned in ["wrapping_", "saturating_", "overflowing_"] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly references {banned} in {}",
                path.display()
            );
        }
    }
}

//= spec/implementation.md#arithmetic-requirements
//# `RING-IMPL-ARITH-004` Conversions between integer widths that may
//# lose information MUST be checked and MUST fail explicitly if the
//# value is out of range for the destination type.
//= spec/implementation.md#arithmetic-requirements
//= type=test
//# `RING-IMPL-ARITH-004` Conversions between integer widths that may
//# lose information MUST be checked and MUST fail explicitly if the
//# value is out of range for the destination type.
#[test]
fn potentially_lossy_integer_width_changes_use_checked_conversions() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));
    let wal = strip_comment_lines(&read_repo_file("src/wal_record.rs"));

    assert!(lib.contains("u32::try_from("));
    assert!(storage.contains("usize::try_from(metadata.region_size)"));
    assert!(map.contains("usize::try_from(self.record_count.0)"));
    assert!(map.contains("u32::try_from(entry_bytes_len)"));
    assert!(map.contains("u32::try_from(snapshot_len)"));
    assert!(startup.contains("usize::try_from(metadata.region_size)"));
    assert!(startup.contains("usize::try_from(metadata.wal_write_granule)"));
    assert!(wal.contains("usize::try_from(payload_len)"));
    assert!(wal.contains("u32::try_from(payload.len())"));
}
