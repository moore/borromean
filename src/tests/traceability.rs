extern crate std;

use self::std::format;
use self::std::fs;
use self::std::path::Path;
use self::std::string::{String, ToString};
use self::std::vec::Vec;
use super::assert_no_alloc;
use crate::{
    decode_record, encode_record_into, CollectionId, DiskError, FlashIo, FreePointerFooter, Header,
    LsmMap, MapError, MapStorageError, MapUpdate, MockError, MockFlash, MockFormatError,
    MockOperation, StartupCollectionBasis, StartupError, Storage, StorageMetadata,
    StorageRuntimeError, StorageWorkspace, WalRecord, WalRegionPrologue, MAP_REGION_V1_FORMAT,
    WAL_V1_FORMAT,
};

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
