extern crate std;

use crate::{
    CollectionId, FlashIo, FreePointerFooter, Header, LsmMap, MapUpdate, MockError, MockFlash,
    MockFormatError, MockOperation, Storage, StorageMetadata, StorageWorkspace, WalRecord,
    WalRegionPrologue, encode_record_into,
};
use core::mem::size_of;
use self::std::collections::BTreeSet;
use self::std::fs;
use self::std::format;
use self::std::path::{Path, PathBuf};
use self::std::string::{String, ToString};
use self::std::vec;
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

fn collect_normative_requirement_ids(spec_path: &Path) -> Vec<String> {
    let mut ids = Vec::new();
    for item in collect_normative_requirement_items(spec_path) {
        for id in extract_requirement_ids(&item) {
            push_unique(&mut ids, id);
        }
    }
    ids
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

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_repo_file(relative: &str) -> String {
    fs::read_to_string(repo_root().join(relative)).unwrap()
}

fn strip_comment_lines(source: &str) -> String {
    source
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !(trimmed.starts_with("//") || trimmed.starts_with('#'))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn dependency_names(manifest: &str, section: &str) -> BTreeSet<String> {
    let section_header = format!("[{section}]");
    let mut names = BTreeSet::new();
    let mut in_section = false;

    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_section = trimmed == section_header;
            continue;
        }
        if !in_section || trimmed.is_empty() {
            continue;
        }

        let Some((name, _)) = trimmed.split_once('=') else {
            continue;
        };
        names.insert(name.trim().to_string());
    }

    names
}

fn non_test_source_files() -> Vec<PathBuf> {
    let src_root = repo_root().join("src");
    let mut files = Vec::new();
    collect_rust_sources(&src_root, &mut files);
    files
        .into_iter()
        .filter(|path| !is_dedicated_test_file(path))
        .collect()
}

fn non_test_sources_without_comments() -> Vec<(PathBuf, String)> {
    non_test_source_files()
        .into_iter()
        .map(|path| {
            let source = fs::read_to_string(&path).unwrap();
            (path, strip_comment_lines(&source))
        })
        .collect()
}

fn flash_io_method_names() -> Vec<String> {
    let source = read_repo_file("src/flash_io.rs");
    let mut in_trait = false;
    let mut names = Vec::new();

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed == "pub trait FlashIo {" {
            in_trait = true;
            continue;
        }
        if !in_trait {
            continue;
        }
        if trimmed == "}" {
            break;
        }
        if let Some(name) = extract_fn_name(trimmed) {
            names.push(name);
        }
    }

    names
}

struct ForwardingFlash<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> {
    inner: MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>
    ForwardingFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn new(erased_byte: u8) -> Self {
        Self {
            inner: MockFlash::new(erased_byte),
        }
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> FlashIo
    for ForwardingFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, MockError> {
        self.inner.read_metadata()
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), MockError> {
        self.inner.write_metadata(metadata)
    }

    fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), MockError> {
        self.inner.read_region(region_index, offset, buffer)
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), MockError> {
        self.inner.write_region(region_index, offset, data)
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), MockError> {
        self.inner.erase_region(region_index)
    }

    fn sync(&mut self) -> Result<(), MockError> {
        self.inner.sync()
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, MockFormatError> {
        self.inner
            .format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)
    }
}

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-001` Every normative requirement in
//# [spec/ring.md](ring.md) or this specification MUST have at least one
//# dedicated automated test function or dedicated compile-time test case
//# whose primary purpose is to verify that single requirement.
//= spec/implementation.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-001` Every normative requirement in
//# [spec/ring.md](ring.md) or this specification MUST have at least one
//# dedicated automated test function or dedicated compile-time test case
//# whose primary purpose is to verify that single requirement.
#[test]
fn every_normative_requirement_has_a_dedicated_test_or_harness_entry() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut traced_files = Vec::new();
    collect_rust_sources(&repo_root.join("src"), &mut traced_files);
    let mut covered = BTreeSet::new();

    for relative in ["tests", "ui", "compile"] {
        let root = repo_root.join(relative);
        if root.exists() {
            collect_rust_sources(&root, &mut traced_files);
        }
    }

    for path in traced_files {
        let source = fs::read_to_string(&path).unwrap();
        for line in source.lines() {
            if !line.trim().starts_with("//#") {
                continue;
            }
            for id in extract_requirement_ids(line) {
                covered.insert(id);
            }
        }
    }

    let mut missing = Vec::new();
    for spec in ["spec/ring.md", "spec/implementation.md"] {
        for id in collect_normative_requirement_ids(&repo_root.join(spec)) {
            if !covered.contains(&id) {
                missing.push(id);
            }
        }
    }

    assert!(
        missing.is_empty(),
        "normative requirements without dedicated test or harness coverage: {missing:?}"
    );
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

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-001` The core library crate MUST compile with
//# `#![no_std]`.
#[test]
fn core_library_crate_declares_no_std() {
    let lib = read_repo_file("src/lib.rs");
    assert!(lib.contains("#![no_std]"));
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-002` The core library crate MUST NOT depend on the
//# Rust `alloc` crate.
#[test]
fn core_library_crate_avoids_alloc_dependency_and_usage() {
    let manifest = strip_comment_lines(&read_repo_file("Cargo.toml"));
    let dependencies = dependency_names(&manifest, "dependencies");
    assert!(!dependencies.contains("alloc"));

    for (path, source) in non_test_sources_without_comments() {
        assert!(
            !source.contains("alloc::"),
            "non-test source unexpectedly references alloc in {}",
            path.display()
        );
        assert!(
            !source.contains("extern crate alloc"),
            "non-test source unexpectedly imports alloc in {}",
            path.display()
        );
    }
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-003` The core library crate MUST NOT depend on an
//# async runtime, executor, scheduler, or timer facility.
#[test]
fn core_library_crate_has_no_async_runtime_or_timer_dependencies() {
    let manifest = strip_comment_lines(&read_repo_file("Cargo.toml"));
    let dependencies = dependency_names(&manifest, "dependencies");
    for banned in [
        "tokio",
        "async-std",
        "smol",
        "glommio",
        "embassy-executor",
        "async-executor",
        "futures-executor",
        "futures-timer",
    ] {
        assert!(
            !dependencies.contains(banned),
            "unexpected runtime-style dependency {banned}"
        );
    }

    for (path, source) in non_test_sources_without_comments() {
        for banned in [
            "tokio::",
            "async_std::",
            "smol::",
            "glommio::",
            "embassy_executor::",
            "futures_timer::",
        ] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly references {banned} in {}",
                path.display()
            );
        }
    }
}

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-005` All memory required for normal operation MUST
//# come from caller-owned values, fixed-capacity fields, or stack
//# frames whose size is statically bounded by type parameters or API
//# contracts.
#[test]
fn normal_operation_memory_comes_from_caller_owned_or_fixed_capacity_storage() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let workspace = strip_comment_lines(&read_repo_file("src/workspace.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(lib.contains("dirty_frontiers: Vec<CollectionId, MAX_COLLECTIONS>"));
    assert!(storage.contains("collections: Vec<StartupCollection, MAX_COLLECTIONS>"));
    assert!(storage.contains("pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>"));
    assert!(workspace.contains("region_bytes: [u8; REGION_SIZE]"));
    assert!(workspace.contains("physical_scratch: [u8; REGION_SIZE]"));
    assert!(workspace.contains("logical_scratch: [u8; REGION_SIZE]"));
    assert!(map.contains("map: &'a mut [u8]"));

    for (path, source) in non_test_sources_without_comments() {
        for banned in ["alloc::vec::Vec", "std::vec::Vec", "Box<", "Rc<", "Arc<"] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly uses dynamic normal-operation storage via {banned} in {}",
                path.display()
            );
        }
    }
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-001` The maximum number of tracked collections,
//# heads, replay entries, and other bounded in-memory items MUST be an
//# explicit compile-time or constructor-time capacity.
#[test]
fn bounded_runtime_state_uses_explicit_capacity_parameters() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(
        lib.contains("pub struct Storage<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>")
    );
    assert!(lib.contains("dirty_frontiers: Vec<CollectionId, MAX_COLLECTIONS>"));
    assert!(
        storage.contains(
            "pub struct StorageRuntime<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>"
        )
    );
    assert!(storage.contains("collections: Vec<StartupCollection, MAX_COLLECTIONS>"));
    assert!(storage.contains("pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>"));
    assert!(startup.contains("wal_chain: Vec<u32, REGION_COUNT>"));
    assert!(startup.contains("collections: Vec<StartupCollection, MAX_COLLECTIONS>"));
    assert!(startup.contains("pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>"));
    assert!(map.contains("pub struct LsmMap<'a, K, V, const MAX_INDEXES: usize>"));
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-002` Any operation that needs scratch space for
//# encoding, decoding, or staging MUST accept caller-provided buffers or
//# borrow dedicated storage from a caller-provided workspace object.
#[test]
fn scratch_space_enters_through_workspace_or_caller_buffers() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));
    let workspace = strip_comment_lines(&read_repo_file("src/workspace.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(lib.contains("workspace: &'a mut StorageWorkspace<REGION_SIZE>"));
    assert!(lib.contains("workspace: &mut StorageWorkspace<REGION_SIZE>"));
    assert!(lib.contains("payload_buffer: &mut [u8]"));
    assert!(lib.contains("payload_buffer: &'a mut [u8]"));
    assert!(storage.contains("let (physical, logical) = workspace.encode_buffers();"));
    assert!(storage.contains("let (region_bytes, logical_scratch) = workspace.scan_buffers();"));
    assert!(startup.contains("let (physical_scratch, logical_scratch) = workspace.encode_buffers();"));
    assert!(startup.contains("let (region_bytes, logical_scratch) = workspace.scan_buffers();"));
    assert!(workspace.contains("pub struct StorageWorkspace<const REGION_SIZE: usize>"));
    assert!(workspace.contains("region_bytes: [u8; REGION_SIZE]"));
    assert!(workspace.contains("physical_scratch: [u8; REGION_SIZE]"));
    assert!(workspace.contains("logical_scratch: [u8; REGION_SIZE]"));
    for signature in [
        "buffer: &'a mut [u8]",
        "snapshot: &mut [u8]",
        "region_payload: &mut [u8]",
        "scratch: &mut [u8]",
        "payload: &mut [u8]",
    ] {
        assert!(map.contains(signature), "missing caller buffer signature {signature}");
    }
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-004` The implementation SHOULD avoid keeping
//# duplicate copies of large record payloads in memory when a borrowed
//# buffer or streaming decode is sufficient.
#[test]
fn map_storage_paths_reuse_borrowed_buffers_for_payload_data() {
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(map.contains("map: &'a mut [u8]"));
    assert!(map.contains("buffer: &'a mut [u8],"));
    assert!(map.contains("let (payload, _) = workspace.encode_buffers();"));
    assert!(map.contains("&payload[..used]"));
    assert!(map.contains("from_bytes(&self.map["));
    assert!(map.contains("let entry: Entry<K, V> = from_bytes(&self.map[start..end])?;"));
    for banned in ["alloc::vec::Vec", "std::vec::Vec", "Box<[u8]>", "Vec<u8>"] {
        assert!(
            !map.contains(banned),
            "map implementation unexpectedly duplicates payloads via {banned}"
        );
    }
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk
//# format constants MUST be derivable from public constants, associated
//# constants, or documented constructor contracts.
#[test]
fn disk_format_buffer_sizes_are_exposed_by_constants_or_workspace_contracts() {
    assert_eq!(StorageMetadata::ENCODED_LEN, size_of::<u32>() * 6 + size_of::<u8>() * 2);
    assert_eq!(
        Header::ENCODED_LEN,
        size_of::<u64>() + size_of::<u64>() + size_of::<u16>() + size_of::<u32>()
    );
    assert_eq!(WalRegionPrologue::ENCODED_LEN, size_of::<u32>() * 2);
    assert_eq!(FreePointerFooter::ENCODED_LEN, size_of::<u32>() * 2);

    let mut workspace = StorageWorkspace::<128>::new();
    {
        let (region_bytes, logical_scratch) = workspace.scan_buffers();
        assert_eq!(region_bytes.len(), 128);
        assert_eq!(logical_scratch.len(), 128);
    }
    {
        let (physical_scratch, logical_scratch) = workspace.encode_buffers();
        assert_eq!(physical_scratch.len(), 128);
        assert_eq!(logical_scratch.len(), 128);
    }
}

//= spec/implementation.md#collection-requirements
//# `RING-IMPL-COLL-002` Collection-specific in-memory state MUST obey
//# the same explicit-capacity and no-allocation rules as borromean
//# core.
#[test]
fn map_in_memory_state_uses_explicit_capacity_and_borrowed_storage() {
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(map.contains("pub struct LsmMap<'a, K, V, const MAX_INDEXES: usize>"));
    assert!(map.contains("map: &'a mut [u8]"));
    assert!(map.contains("_phantom: PhantomData<(K, V)>"));
    assert!(!map.contains("alloc::vec::Vec"));
    assert!(!map.contains("std::vec::Vec"));
    assert!(!map.contains("Box<"));
}

//= spec/implementation.md#api-requirements
//# `RING-IMPL-API-004` The implementation SHOULD keep collection
//# operation APIs close to the prototype's explicit buffer-passing style
//# where that style avoids hidden allocation.
#[test]
fn collection_update_api_keeps_explicit_payload_buffer_passing() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));

    assert!(lib.contains("pub fn append_map_update<"));
    assert!(lib.contains("pub fn append_map_update_future<"));
    assert!(lib.contains("payload_buffer: &mut [u8]"));
    assert!(lib.contains("payload_buffer: &'a mut [u8]"));
    assert!(lib.contains("LsmMap::<K, V, MAX_INDEXES>::encode_update_into(update, payload_buffer)?;"));
}

//= spec/implementation.md#non-goal-requirements
//# `RING-IMPL-NONGOAL-001` Borromean core MUST NOT require a specific
//# embedded framework, RTOS, or async executor.
#[test]
fn core_library_crate_requires_no_embedded_framework_or_rtos_dependency() {
    let manifest = strip_comment_lines(&read_repo_file("Cargo.toml"));
    let dependencies = dependency_names(&manifest, "dependencies");
    for dependency in dependencies {
        assert!(
            ![
                "embassy",
                "rtic",
                "freertos",
                "zephyr",
                "esp-idf",
                "esp_idf",
                "arduino",
            ]
            .iter()
            .any(|prefix| dependency.starts_with(prefix)),
            "unexpected framework or RTOS dependency {dependency}"
        );
    }
}

//= spec/implementation.md#non-goal-requirements
//# `RING-IMPL-NONGOAL-002` Borromean core MUST NOT assume thread
//# support, background workers, or heap-backed task scheduling.
#[test]
fn core_library_crate_assumes_no_threads_or_background_workers() {
    for (path, source) in non_test_sources_without_comments() {
        for banned in [
            "std::thread",
            "thread::spawn",
            "spawn_blocking",
            "JoinHandle",
            "crossbeam",
            "tokio::spawn",
            "async_std::task::spawn",
            "std::sync::mpsc",
        ] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly references {banned} in {}",
                path.display()
            );
        }
    }
}

//= spec/implementation.md#architecture-requirements
//# `RING-IMPL-ARCH-002` The backing I/O object MUST instead be passed
//# into operation entry points or operation builders so the same
//# `Storage` value can participate in externally driven async execution.
#[test]
fn storage_public_entry_points_take_backing_io_from_callers() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    assert!(
        lib.contains("pub struct Storage<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize> {")
    );
    assert!(!lib.contains("pub struct Storage<IO"));

    for signature in [
        "pub fn format_future<",
        "pub fn format<",
        "pub fn open_future<'a",
        "pub fn open<const",
        "pub fn create_map_future<",
        "pub fn append_map_update_future<",
        "pub fn flush_map_future<",
        "pub fn drop_map_future<",
    ] {
        assert!(lib.contains(signature), "missing public entry point {signature}");
    }

    assert!(lib.contains("flash: &'a mut IO"));
    assert!(lib.contains("flash: &mut IO"));
}

//= spec/implementation.md#api-requirements
//# `RING-IMPL-API-002` The public API MUST allow a caller to drive the
//# same storage engine from either blocking test shims or asynchronous
//# device adapters without changing borromean correctness logic.
#[test]
fn blocking_and_future_entry_points_produce_equivalent_storage_state() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 5;
    let mut blocking_flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut blocking_workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut blocking = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut blocking_flash,
        &mut blocking_workspace,
        1,
        8,
        0xa5,
    )
    .unwrap();

    let mut future_flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut future_workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut future_driven = super::poll_until_ready(Storage::<8, 4>::format_future::<
        REGION_SIZE,
        REGION_COUNT,
        _,
    >(
        &mut future_flash,
        &mut future_workspace,
        1,
        8,
        0xa5,
    ), 16)
    .unwrap();

    blocking
        .create_map::<REGION_SIZE, REGION_COUNT, _>(
            &mut blocking_flash,
            &mut blocking_workspace,
            CollectionId(61),
        )
        .unwrap();
    super::poll_until_ready(future_driven.create_map_future::<REGION_SIZE, REGION_COUNT, _>(
        &mut future_flash,
        &mut future_workspace,
        CollectionId(61),
    ), 16)
    .unwrap();

    let mut blocking_payload = [0u8; 64];
    let mut future_payload = [0u8; 64];
    blocking
        .append_map_update::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut blocking_flash,
            &mut blocking_workspace,
            CollectionId(61),
            &MapUpdate::Set { key: 7, value: 70 },
            &mut blocking_payload,
        )
        .unwrap();
    super::poll_until_ready(
        future_driven.append_map_update_future::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut future_flash,
            &mut future_workspace,
            CollectionId(61),
            &MapUpdate::Set { key: 7, value: 70 },
            &mut future_payload,
        ),
        16,
    )
    .unwrap();

    let reopened_blocking =
        Storage::<8, 4>::open::<REGION_SIZE, REGION_COUNT, _>(&mut blocking_flash, &mut blocking_workspace)
            .unwrap();
    let reopened_future = super::poll_until_ready(Storage::<8, 4>::open_future::<
        REGION_SIZE,
        REGION_COUNT,
        _,
    >(&mut future_flash, &mut future_workspace), 16)
    .unwrap();

    assert_eq!(reopened_blocking.metadata(), reopened_future.metadata());
    assert_eq!(reopened_blocking.collections(), reopened_future.collections());
    assert_eq!(reopened_blocking.pending_reclaims(), reopened_future.pending_reclaims());
    assert_eq!(reopened_blocking.last_free_list_head(), reopened_future.last_free_list_head());
    assert_eq!(reopened_blocking.free_list_tail(), reopened_future.free_list_tail());

    let mut blocking_map_buffer = [0u8; REGION_SIZE];
    let blocking_map = reopened_blocking
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut blocking_flash,
            &mut blocking_workspace,
            CollectionId(61),
            &mut blocking_map_buffer,
        )
        .unwrap();
    let mut future_map_buffer = [0u8; REGION_SIZE];
    let future_map = reopened_future
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut future_flash,
            &mut future_workspace,
            CollectionId(61),
            &mut future_map_buffer,
        )
        .unwrap();
    assert_eq!(blocking_map.get(&7).unwrap(), Some(70));
    assert_eq!(future_map.get(&7).unwrap(), Some(70));
}

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
    let mut storage =
        Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, 1, 8, 0xa5)
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
