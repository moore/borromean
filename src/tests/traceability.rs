extern crate std;

use crate::{
    CollectionId, LsmMap, MockFlash, Storage, StorageMetadata, StorageWorkspace, WalRecord,
    encode_record_into,
};
use self::std::collections::BTreeSet;
use self::std::fs;
use self::std::format;
use self::std::path::{Path, PathBuf};
use self::std::string::{String, ToString};
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

#[derive(Debug)]
struct TestEntry {
    location: String,
    requirement_ids: Vec<String>,
}

fn push_unique(ids: &mut Vec<String>, value: String) {
    if !ids.contains(&value) {
        ids.push(value);
    }
}

fn extract_requirement_ids(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut ids = Vec::new();
    let mut index = 0usize;
    while index + 5 <= bytes.len() {
        if &bytes[index..index + 5] != b"RING-" {
            index += 1;
            continue;
        }

        let mut end = index + 5;
        while end < bytes.len() {
            let byte = bytes[end];
            if byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-' {
                end += 1;
            } else {
                break;
            }
        }

        if end > index + 5 {
            push_unique(&mut ids, text[index..end].to_string());
            index = end;
        } else {
            index += 1;
        }
    }

    ids
}

fn extract_fn_name(line: &str) -> Option<String> {
    let fn_offset = line.find("fn ")?;
    let mut end = fn_offset + 3;
    let bytes = line.as_bytes();
    while end < bytes.len() {
        let byte = bytes[end];
        if byte.is_ascii_alphanumeric() || byte == b'_' {
            end += 1;
        } else {
            break;
        }
    }

    (end > fn_offset + 3).then(|| line[fn_offset + 3..end].to_string())
}

fn collect_requirement_ids_in_lines(lines: &[&str]) -> Vec<String> {
    let mut ids = Vec::new();
    for line in lines {
        if !line.trim().starts_with("//#") {
            continue;
        }

        for id in extract_requirement_ids(line) {
            push_unique(&mut ids, id);
        }
    }
    ids
}

fn parse_test_entries(path: &Path) -> Vec<TestEntry> {
    let source = fs::read_to_string(path).unwrap();
    let lines: Vec<&str> = source.lines().collect();
    let mut entries = Vec::new();

    for (index, line) in lines.iter().enumerate() {
        if line.trim() != "#[test]" {
            continue;
        }

        let mut start = index;
        while start > 0 {
            let trimmed = lines[start - 1].trim();
            if trimmed.is_empty() || trimmed.starts_with("//#") || trimmed.starts_with("//=") {
                start -= 1;
            } else {
                break;
            }
        }

        let mut fn_index = index + 1;
        while fn_index < lines.len() {
            let trimmed = lines[fn_index].trim();
            if trimmed.is_empty()
                || trimmed.starts_with("//#")
                || trimmed.starts_with("//=")
                || trimmed.starts_with("#[")
            {
                fn_index += 1;
                continue;
            }
            break;
        }

        let name = if fn_index < lines.len() {
            extract_fn_name(lines[fn_index]).unwrap_or_else(|| "<unknown>".to_string())
        } else {
            "<missing fn>".to_string()
        };
        entries.push(TestEntry {
            location: format!("{}::{name}", path.display()),
            requirement_ids: collect_requirement_ids_in_lines(&lines[start..fn_index]),
        });
    }

    entries
}

fn parse_traced_helpers(path: &Path) -> Vec<TestEntry> {
    let source = fs::read_to_string(path).unwrap();
    let lines: Vec<&str> = source.lines().collect();
    let mut helpers = Vec::new();

    for (index, line) in lines.iter().enumerate() {
        let Some(name) = extract_fn_name(line) else {
            continue;
        };

        let mut start = index;
        while start > 0 {
            let trimmed = lines[start - 1].trim();
            if trimmed.is_empty()
                || trimmed.starts_with("//#")
                || trimmed.starts_with("//=")
                || trimmed.starts_with("#[")
            {
                start -= 1;
            } else {
                break;
            }
        }

        let context = &lines[start..index];
        let is_test = context.iter().any(|line| line.trim() == "#[test]");
        let requirement_ids = collect_requirement_ids_in_lines(context);
        if !is_test && !requirement_ids.is_empty() {
            helpers.push(TestEntry {
                location: format!("{}::{name}", path.display()),
                requirement_ids,
            });
        }
    }

    helpers
}

fn collect_dedicated_test_files(src_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_sources(src_root, &mut files);
    files
        .into_iter()
        .filter(|path| is_dedicated_test_file(path))
        .collect()
}

fn strip_numbered_prefix(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }

    if index == 0 || index + 1 >= bytes.len() || bytes[index] != b'.' || bytes[index + 1] != b' ' {
        return None;
    }

    Some(&line[index + 2..])
}

fn collect_normative_requirement_items(spec_path: &Path) -> Vec<String> {
    let source = fs::read_to_string(spec_path).unwrap();
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_code_block = false;

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            continue;
        }
        if in_code_block {
            continue;
        }

        if let Some(rest) = strip_numbered_prefix(trimmed) {
            if current.contains("`RING-")
                && (current.contains(" MUST ")
                    || current.contains(" MUST NOT ")
                    || current.contains(" SHOULD ")
                    || current.contains(" MAY "))
            {
                items.push(current.trim().to_string());
            }
            current.clear();
            current.push_str(rest);
            continue;
        }

        if current.is_empty() {
            continue;
        }

        if trimmed.is_empty() || trimmed.starts_with('#') {
            if current.contains("`RING-")
                && (current.contains(" MUST ")
                    || current.contains(" MUST NOT ")
                    || current.contains(" SHOULD ")
                    || current.contains(" MAY "))
            {
                items.push(current.trim().to_string());
            }
            current.clear();
            continue;
        }

        current.push(' ');
        current.push_str(trimmed);
    }

    if current.contains("`RING-")
        && (current.contains(" MUST ")
            || current.contains(" MUST NOT ")
            || current.contains(" SHOULD ")
            || current.contains(" MAY "))
    {
        items.push(current.trim().to_string());
    }

    items
}

fn assert_spec_requirement_format(spec_path: &Path, expected_prefix: &str) {
    let items = collect_normative_requirement_items(spec_path);
    assert!(!items.is_empty(), "no normative requirement items found in {}", spec_path.display());

    for item in items {
        assert!(
            item.starts_with(&format!("`{expected_prefix}")),
            "requirement item does not start with a stable identifier in {}: {item}",
            spec_path.display()
        );
        assert!(
            item.contains(" MUST ")
                || item.contains(" MUST NOT ")
                || item.contains(" SHOULD ")
                || item.contains(" MAY "),
            "requirement item does not contain explicit normative language in {}: {item}",
            spec_path.display()
        );
    }
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

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-002` A top-level automated test function MUST NOT
//# claim to verify multiple normative requirement identifiers.
#[test]
fn top_level_automated_tests_claim_at_most_one_requirement_identifier() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    for path in collect_dedicated_test_files(&src_root) {
        for entry in parse_test_entries(&path) {
            if entry.requirement_ids.len() > 1 {
                offenders.push(format!("{} -> {:?}", entry.location, entry.requirement_ids));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "top-level test functions still claim multiple requirements: {offenders:?}"
    );
}

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-003` Shared setup, fixtures, helper functions,
//# macros, and data generators MAY be reused across requirement-specific
//# tests, but the final traced test entry point MUST remain specific to
//# one requirement identifier.
#[test]
fn shared_test_helpers_remain_untraced() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    for path in collect_dedicated_test_files(&src_root) {
        for helper in parse_traced_helpers(&path) {
            offenders.push(format!("{} -> {:?}", helper.location, helper.requirement_ids));
        }
    }

    assert!(
        offenders.is_empty(),
        "shared helpers or fixtures still carry traced requirement ids: {offenders:?}"
    );
}

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-004` When a requirement is verified by a
//# compile-fail, compile-pass, or other non-runtime harness, that harness
//# entry MUST still be dedicated to a single requirement identifier.
#[test]
fn non_runtime_harness_entries_claim_at_most_one_requirement_when_present() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut harness_files = Vec::new();
    for relative in ["tests", "ui", "compile"] {
        let root = repo_root.join(relative);
        if root.exists() {
            collect_rust_sources(&root, &mut harness_files);
        }
    }

    let mut offenders = Vec::new();
    for path in harness_files {
        let source = fs::read_to_string(&path).unwrap();
        let mut ids = BTreeSet::new();
        for line in source.lines() {
            if !line.trim().starts_with("//#") {
                continue;
            }
            for id in extract_requirement_ids(line) {
                ids.insert(id);
            }
        }
        if ids.len() > 1 {
            offenders.push(format!("{} -> {:?}", path.display(), ids));
        }
    }

    assert!(
        offenders.is_empty(),
        "non-runtime harness entries still claim multiple requirements: {offenders:?}"
    );
}

//= spec/implementation.md#requirements-format
//# Each normative requirement starts with a stable
//# identifier such as `RING-IMPL-CORE-001` and uses explicit normative
//# language such as `MUST`, `MUST NOT`, `SHOULD`, or `MAY`.
#[test]
fn implementation_spec_requirements_use_stable_identifiers_and_normative_language() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert_spec_requirement_format(
        &repo_root.join("spec/implementation.md"),
        "RING-IMPL-",
    );
}

//= spec/ring.md#requirements-format
//# Each normative requirement starts with a stable
//# identifier such as `RING-WAL-ENC-001` and uses explicit normative
//# language such as `MUST`, `MUST NOT`, `SHOULD`, or `MAY`.
#[test]
fn ring_spec_requirements_use_stable_identifiers_and_normative_language() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert_spec_requirement_format(&repo_root.join("spec/ring.md"), "RING-");
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
