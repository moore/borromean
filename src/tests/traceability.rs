extern crate std;

use crate::{
    CollectionId, LsmMap, MockFlash, Storage, StorageMetadata, StorageWorkspace, WalRecord,
    encode_record_into,
};
use self::std::fs;
use self::std::path::{Path, PathBuf};
use self::std::string::ToString;
use self::std::vec::Vec;

fn collect_rust_sources(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, files);
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn is_dedicated_test_file(path: &Path) -> bool {
    if path.file_name().and_then(|name| name.to_str()) == Some("tests.rs") {
        return true;
    }

    path.components()
        .any(|component| component.as_os_str() == "tests")
}

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-005` Automated test functions and compile-time test
//# harness entries MUST be defined only in dedicated test modules or
//# files rather than inside the functional implementation module they
//# exercise.
#[test]
fn automated_tests_live_only_in_dedicated_test_modules() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&src_root, &mut files);

    let mut offenders = Vec::new();
    for path in files {
        if is_dedicated_test_file(&path) {
            continue;
        }

        let source = fs::read_to_string(&path).unwrap();
        if source.contains("mod tests {") || source.contains("#[test]") {
            offenders.push(path.strip_prefix(&src_root).unwrap().display().to_string());
        }
    }

    assert!(
        offenders.is_empty(),
        "non-test source files still contain inline test bodies: {offenders:?}"
    );
}

//= spec/implementation.md#architecture-requirements
//# `RING-IMPL-ARCH-003` WAL handling, region-management logic, and
//# collection-specific logic MUST remain separable modules with explicit
//# interfaces.
#[test]
fn wal_region_management_and_collection_logic_stay_separate_modules() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for relative in [
        "wal_record.rs",
        "storage.rs",
        "startup.rs",
        "collections.rs",
        "collections/map/mod.rs",
    ] {
        assert!(
            src_root.join(relative).is_file(),
            "expected separate module file {relative}"
        );
    }

    let lib = fs::read_to_string(src_root.join("lib.rs")).unwrap();
    assert!(lib.contains("pub mod wal_record;"));
    assert!(lib.contains("pub mod storage;"));
    assert!(lib.contains("mod collections;"));

    let collections = fs::read_to_string(src_root.join("collections.rs")).unwrap();
    assert!(collections.contains("pub mod map;"));

    let metadata = StorageMetadata::new(128, 4, 1, 8, 0xff, 0xa5).unwrap();
    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len =
        encode_record_into(WalRecord::WalRecovery, metadata, &mut physical, &mut logical).unwrap();
    assert!(encoded_len > 0);

    let mut flash = MockFlash::<128, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let storage =
        Storage::<8, 4>::format::<128, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    assert_eq!(storage.wal_head(), 0);

    let mut map_buffer = [0u8; 128];
    let map = LsmMap::<i32, i32, 4>::new(CollectionId(7), &mut map_buffer).unwrap();
    assert_eq!(map.id(), CollectionId(7));
}
