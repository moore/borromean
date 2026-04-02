extern crate std;

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
