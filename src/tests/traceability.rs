extern crate std;

use super::assert_no_alloc;
use self::std::collections::BTreeSet;
use self::std::format;
use self::std::fs;
use self::std::path::{Path, PathBuf};
use self::std::string::{String, ToString};
use self::std::vec::Vec;
use crate::{
    decode_record, encode_record_into, CollectionId, DiskError, FlashIo, FreePointerFooter, Header,
    LsmMap, MapError, MapStorageError, MapUpdate, MockError, MockFlash, MockFormatError,
    MockOperation, StartupCollectionBasis, StartupError, Storage, StorageMetadata,
    StorageRuntimeError, StorageWorkspace, WalRecord, WalRegionPrologue, MAP_REGION_V1_FORMAT,
    WAL_V1_FORMAT,
};

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
    assert!(
        !items.is_empty(),
        "no normative requirement items found in {}",
        spec_path.display()
    );

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
mod api;
mod arch;
mod arithmetic;
mod audit;
mod collection;
mod core;
mod exec;
mod format;
mod io;
mod memory;
mod operation;
mod panic;
mod startup;
