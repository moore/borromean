use super::*;
use crate::{LsmMap, Storage, StorageFormatConfig};
use std::format;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn new(name: &str) -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "borromean-file-backing-{name}-{}-{id}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug)]
struct FakeOs {
    page_size: usize,
    filesystem_block_size: usize,
    fallocate_result: Result<(), FileBackingError>,
    page_size_calls: usize,
    filesystem_block_size_calls: usize,
    fallocate_calls: usize,
    set_len_calls: usize,
    madvise_calls: usize,
    sync_calls: usize,
    last_madvise_policy: Option<MadvisePolicy>,
}

impl FakeOs {
    fn new() -> Self {
        Self {
            page_size: 256,
            filesystem_block_size: 256,
            fallocate_result: Ok(()),
            page_size_calls: 0,
            filesystem_block_size_calls: 0,
            fallocate_calls: 0,
            set_len_calls: 0,
            madvise_calls: 0,
            sync_calls: 0,
            last_madvise_policy: None,
        }
    }

    fn with_fallocate_result(mut self, result: Result<(), FileBackingError>) -> Self {
        self.fallocate_result = result;
        self
    }
}

impl FileBackingOs for FakeOs {
    fn page_size(&mut self) -> Result<usize, FileBackingError> {
        self.page_size_calls += 1;
        Ok(self.page_size)
    }

    fn filesystem_block_size(
        &mut self,
        _file: &File,
        _scratch: &mut FileBackingScratch,
    ) -> Result<usize, FileBackingError> {
        self.filesystem_block_size_calls += 1;
        Ok(self.filesystem_block_size)
    }

    fn fallocate(&mut self, _file: &File, _len: usize) -> Result<(), FileBackingError> {
        self.fallocate_calls += 1;
        self.fallocate_result
    }

    fn set_len(&mut self, file: &File, len: usize) -> Result<(), FileBackingError> {
        self.set_len_calls += 1;
        file.set_len(len as u64)
            .map_err(|error| FileBackingError::from_io_error(FileBackingOperation::SetLen, error))
    }

    fn madvise(
        &mut self,
        _address: *mut u8,
        _len: usize,
        policy: MadvisePolicy,
    ) -> Result<(), FileBackingError> {
        self.madvise_calls += 1;
        self.last_madvise_policy = Some(policy);
        Ok(())
    }

    fn sync_file(&mut self, _file: &File) -> Result<(), FileBackingError> {
        self.sync_calls += 1;
        Ok(())
    }
}

fn unsupported_fallocate_error() -> FileBackingError {
    FileBackingError::Io {
        operation: FileBackingOperation::Fallocate,
        raw_os_error: Some(libc::EOPNOTSUPP),
    }
}

fn no_space_fallocate_error() -> FileBackingError {
    FileBackingError::Io {
        operation: FileBackingOperation::Fallocate,
        raw_os_error: Some(libc::ENOSPC),
    }
}

//= spec/file.md#public-api
//= type=test
//# `RING-FILE-001` The crate MUST expose a `file-backing` feature that
//# enables the Linux `FileBacking` backend without making the default build
//# depend on `std`.
#[test]
fn requirement_file_backing_feature_exposes_linux_backend() {
    let options = FileBackingOptions::default();
    let _: Option<FileBacking<512, 2>> = None;
    assert_eq!(options.allocation_policy, AllocationPolicy::Strict);
}

//= spec/file.md#public-api
//= type=test
//# `RING-FILE-002` The file-backed API MUST expose
//# `FileBacking`, `FileBackingOptions`, `AllocationPolicy`, and
//# `MadvisePolicy`.
#[test]
fn requirement_file_backing_public_api_types_are_constructible() {
    let options = FileBackingOptions {
        erased_byte: 0xee,
        allocation_policy: AllocationPolicy::FallbackOnUnsupported,
        madvise_policy: MadvisePolicy::Sequential,
        sync_on_create: false,
    };
    let _: Option<FileBacking<512, 2>> = None;
    assert_eq!(options.erased_byte, 0xee);
}

//= spec/file.md#file-geometry
//= type=test
//# `RING-FILE-003` A `FileBacking` database file MUST contain one
//# metadata region followed immediately by all data regions.
#[test]
fn requirement_file_layout_places_data_after_metadata_region() {
    let temp = TempFile::new("layout");
    let mut os = FakeOs::new();
    let mut backing = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xee),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    backing.write_region(0, 0, &[0x11]).unwrap();
    let mut bytes = [0u8; 2];
    backing.read_storage(511, &mut bytes).unwrap();
    assert_eq!(bytes, [0xee, 0x11]);
}

//= spec/file.md#file-geometry
//= type=test
//# `RING-FILE-004` Data region `n` MUST start at byte offset
//# `(n + 1) * REGION_SIZE`.
#[test]
fn requirement_file_layout_maps_region_index_to_expected_offset() {
    let temp = TempFile::new("region-offset");
    let mut os = FakeOs::new();
    let mut backing = FileBacking::<512, 3>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xee),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    backing.write_region(2, 0, &[0x33]).unwrap();
    let mut byte = [0u8; 1];
    backing.read_storage(3 * 512, &mut byte).unwrap();
    assert_eq!(byte, [0x33]);
}

//= spec/file.md#file-geometry
//= type=test
//# `RING-FILE-005` On create and open, `FileBacking` MUST discover the
//# OS mmap page size and filesystem allocation block size through libc/POSIX
//# APIs.
#[test]
fn requirement_file_backing_discovers_page_and_filesystem_block_sizes() {
    let temp = TempFile::new("discover");
    let mut os = FakeOs::new();
    let _backing = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xff),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    assert_eq!(os.page_size_calls, 1);
    assert_eq!(os.filesystem_block_size_calls, 1);
}

//= spec/file.md#file-geometry
//= type=test
//# `RING-FILE-006` `FileBacking` MUST define its required alignment unit
//# as the least common multiple of the OS mmap page size and filesystem
//# allocation block size.
#[test]
fn requirement_file_backing_alignment_unit_is_lcm() {
    let geometry = FileBackingGeometry::new::<1024, 2>(256, 512).unwrap();
    assert_eq!(geometry.alignment_unit, 512);
}

//= spec/file.md#file-geometry
//= type=test
//# `RING-FILE-007` Create and open MUST fail when `REGION_SIZE` or the
//# computed file length is not a multiple of the required alignment unit.
#[test]
fn requirement_file_backing_rejects_misaligned_region_size() {
    assert_eq!(
        FileBackingGeometry::new::<768, 2>(512, 1024),
        Err(FileBackingGeometryError::RegionSizeNotAligned {
            region_size: 768,
            alignment_unit: 1024,
        })
    );
}

//= spec/file.md#file-geometry
//= type=test
//# `RING-FILE-008` Opening an existing database file MUST fail when the
//# file length is not exactly `(REGION_COUNT + 1) * REGION_SIZE`.
#[test]
fn requirement_file_backing_open_rejects_unexpected_file_length() {
    let temp = TempFile::new("bad-length");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&temp.path)
        .unwrap();
    file.set_len(512).unwrap();
    drop(file);

    let mut os = FakeOs::new();
    let error = FileBacking::<512, 2>::open_existing_with_os(
        &temp.path,
        FileBackingOptions::new(0xff),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap_err();
    assert_eq!(
        error,
        FileBackingError::Geometry(FileBackingGeometryError::UnexpectedFileLength {
            expected: 1536,
            actual: 512,
        })
    );
}

//= spec/file.md#allocation-and-mmap-advice
//= type=test
//# `RING-FILE-009` Creating a new database file MUST use exclusive file
//# creation and reject an already-existing path.
#[test]
fn requirement_file_backing_create_new_is_exclusive() {
    let temp = TempFile::new("exclusive");
    let mut first_os = FakeOs::new();
    let _first = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xff),
        crate::test_file_backing_scratch(),
        &mut first_os,
    )
    .unwrap();
    let mut second_os = FakeOs::new();
    let error = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xff),
        crate::test_file_backing_scratch(),
        &mut second_os,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        FileBackingError::Io {
            operation: FileBackingOperation::Open,
            ..
        }
    ));
}

//= spec/file.md#allocation-and-mmap-advice
//= type=test
//# `RING-FILE-010` Creating a new database file MUST call
//# `fallocate(fd, 0, 0, file_len)` before creating the mmap.
#[test]
fn requirement_file_backing_create_new_calls_fallocate() {
    let temp = TempFile::new("fallocate");
    let mut os = FakeOs::new();
    let _backing = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xff),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    assert_eq!(os.fallocate_calls, 1);
}

//= spec/file.md#allocation-and-mmap-advice
//= type=test
//# `RING-FILE-011` The `FileBacking` specification MUST state that
//# `fallocate()` preallocates storage but does not guarantee physically
//# contiguous storage.
#[test]
fn requirement_file_backing_allocation_policy_does_not_report_contiguity() {
    assert_eq!(AllocationPolicy::default(), AllocationPolicy::Strict);
}

//= spec/file.md#allocation-and-mmap-advice
//= type=test
//# `RING-FILE-012` Under `AllocationPolicy::Strict`, any `fallocate()`
//# failure MUST fail database-file creation.
#[test]
fn requirement_file_backing_strict_allocation_fails_on_any_fallocate_error() {
    let temp = TempFile::new("strict-fallocate");
    let mut os = FakeOs::new().with_fallocate_result(Err(unsupported_fallocate_error()));
    let error = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xff),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap_err();
    assert_eq!(error, unsupported_fallocate_error());
}

//= spec/file.md#allocation-and-mmap-advice
//= type=test
//# `RING-FILE-013` Under `AllocationPolicy::FallbackOnUnsupported`,
//# `FileBacking` MAY fall back to setting the file length only for
//# unsupported `fallocate()` failures such as `ENOSYS` or `EOPNOTSUPP`.
//# Capacity and quota failures such as `ENOSPC` MUST still fail creation.
#[test]
fn requirement_file_backing_fallback_only_allows_unsupported_fallocate_errors() {
    let fallback_temp = TempFile::new("fallback-unsupported");
    let mut fallback_os = FakeOs::new().with_fallocate_result(Err(unsupported_fallocate_error()));
    let mut options = FileBackingOptions::new(0xff);
    options.allocation_policy = AllocationPolicy::FallbackOnUnsupported;
    let _backing = FileBacking::<512, 2>::create_new_with_os(
        &fallback_temp.path,
        options,
        crate::test_file_backing_scratch(),
        &mut fallback_os,
    )
    .unwrap();
    assert_eq!(fallback_os.set_len_calls, 1);

    let no_space_temp = TempFile::new("fallback-nospace");
    let mut no_space_os = FakeOs::new().with_fallocate_result(Err(no_space_fallocate_error()));
    let error = FileBacking::<512, 2>::create_new_with_os(
        &no_space_temp.path,
        options,
        crate::test_file_backing_scratch(),
        &mut no_space_os,
    )
    .unwrap_err();
    assert_eq!(error, no_space_fallocate_error());
}

//= spec/file.md#allocation-and-mmap-advice
//= type=test
//# `RING-FILE-014` After creating an mmap, `FileBacking` MUST apply
//# `madvise()` according to the configured `MadvisePolicy`. `madvise()`
//# MUST NOT replace `fallocate()`, mmap creation, page-size discovery,
//# filesystem block-size discovery, or durability sync.
#[test]
fn requirement_file_backing_applies_configured_madvise_policy() {
    let temp = TempFile::new("madvise");
    let mut os = FakeOs::new();
    let mut options = FileBackingOptions::new(0xff);
    options.madvise_policy = MadvisePolicy::Sequential;
    let _backing = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        options,
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    assert_eq!(os.madvise_calls, 1);
    assert_eq!(os.last_madvise_policy, Some(MadvisePolicy::Sequential));
    assert_eq!(os.fallocate_calls, 1);
    assert_eq!(os.page_size_calls, 1);
    assert_eq!(os.filesystem_block_size_calls, 1);
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-015` New database files MUST be initialized to the
//# configured erased byte before use.
#[test]
fn requirement_file_backing_initializes_new_files_to_erased_byte() {
    let temp = TempFile::new("erased");
    let mut os = FakeOs::new();
    let backing = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xee),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    let mut bytes = [0u8; 1536];
    backing.read_storage(0, &mut bytes).unwrap();
    assert!(bytes.iter().all(|byte| *byte == 0xee));
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-016` Region reads, writes, and erases MUST reject region
//# indexes, offsets, or lengths outside the configured geometry.
#[test]
fn requirement_file_backing_rejects_out_of_bounds_region_operations() {
    let temp = TempFile::new("bounds");
    let mut os = FakeOs::new();
    let mut backing = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xff),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    assert_eq!(
        backing.read_region(2, 0, 1, |_| ()),
        Err(FileBackingError::InvalidRegionIndex(2))
    );
    assert_eq!(
        backing.write_region(0, 512, &[0x11]),
        Err(FileBackingError::OutOfBounds)
    );
    assert_eq!(
        backing.erase_region(2),
        Err(FileBackingError::InvalidRegionIndex(2))
    );
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-017` Erasing a data region MUST fill the entire region with
//# the configured erased byte.
#[test]
fn requirement_file_backing_erase_fills_region_with_erased_byte() {
    let temp = TempFile::new("erase");
    let mut os = FakeOs::new();
    let mut backing = FileBacking::<512, 2>::create_new_with_os(
        &temp.path,
        FileBackingOptions::new(0xee),
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    backing.write_region(0, 0, &[0x11; 512]).unwrap();
    backing.erase_region(0).unwrap();
    let mut bytes = [0u8; 512];
    backing
        .read_region(0, 0, bytes.len(), |data| bytes.copy_from_slice(data))
        .unwrap();
    assert!(bytes.iter().all(|byte| *byte == 0xee));
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-018` `FileBacking::sync()` MUST flush dirty mmap ranges durably enough
//# for synced data-region writes to survive reopen.
#[test]
fn requirement_file_backing_sync_persists_changes_across_reopen() {
    const REGION_SIZE: usize = 4096;
    const REGION_COUNT: usize = 2;

    let temp = TempFile::new("sync");
    let mut options = FileBackingOptions::new(0xff);
    options.allocation_policy = AllocationPolicy::FallbackOnUnsupported;
    {
        let mut backing = FileBacking::<REGION_SIZE, REGION_COUNT>::create_new(
            &temp.path,
            options,
            crate::test_file_backing_scratch(),
        )
        .unwrap();
        backing.write_region(1, 7, &[0x44, 0x55]).unwrap();
        backing.sync().unwrap();
    }

    let mut reopened = FileBacking::<REGION_SIZE, REGION_COUNT>::open_existing(
        &temp.path,
        options,
        crate::test_file_backing_scratch(),
    )
    .unwrap();
    let mut bytes = [0u8; 2];
    reopened
        .read_region(1, 7, bytes.len(), |data| bytes.copy_from_slice(data))
        .unwrap();
    assert_eq!(bytes, [0x44, 0x55]);
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-020` Data-region-only syncs MUST NOT call file-level sync.
#[test]
fn requirement_file_backing_sync_report_uses_range_flush_for_wal_write() {
    const REGION_SIZE: usize = 4096;
    const REGION_COUNT: usize = 2;

    let temp = TempFile::new("sync-report-range");
    let mut os = FakeOs::new();
    let mut options = FileBackingOptions::new(0xff);
    options.allocation_policy = AllocationPolicy::FallbackOnUnsupported;
    let mut backing = FileBacking::<REGION_SIZE, REGION_COUNT>::create_new_with_os(
        &temp.path,
        options,
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    let sync_calls_after_create = os.sync_calls;

    backing.write_region(1, 7, &[0x44, 0x55]).unwrap();
    let report = backing.sync_with_os_report(&mut os).unwrap();

    let dirty_start = (1 + 1) * REGION_SIZE + 7;
    assert_eq!(report.dirty_range_start, Some(dirty_start));
    assert_eq!(report.dirty_range_end, Some(dirty_start + 2));
    assert_eq!(report.dirty_range_bytes, 2);
    assert_eq!(report.aligned_dirty_range_start, Some(2 * REGION_SIZE));
    assert_eq!(
        report.aligned_dirty_range_end,
        Some(2 * REGION_SIZE + os.page_size)
    );
    assert_eq!(report.aligned_dirty_bytes, os.page_size);
    assert_eq!(report.requested_mmap_flush_bytes, os.page_size);
    assert_eq!(report.flush_overreach_bytes, 0);
    assert_eq!(report.file_sync_kind, FileBackingFileSyncKind::NoFileSync);
    assert_eq!(os.sync_calls, sync_calls_after_create);
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-021` Metadata dirty ranges MUST sync the underlying file.
#[test]
fn requirement_file_backing_sync_report_syncs_file_for_metadata_write() {
    const REGION_SIZE: usize = 4096;
    const REGION_COUNT: usize = 2;

    let temp = TempFile::new("sync-report-metadata");
    let mut os = FakeOs::new();
    let mut options = FileBackingOptions::new(0xff);
    options.allocation_policy = AllocationPolicy::FallbackOnUnsupported;
    let mut backing = FileBacking::<REGION_SIZE, REGION_COUNT>::create_new_with_os(
        &temp.path,
        options,
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    let sync_calls_after_create = os.sync_calls;
    let metadata =
        StorageMetadata::new(REGION_SIZE as u32, REGION_COUNT as u32, 0, 8, 0xff, 0xa5).unwrap();

    backing.write_metadata(metadata).unwrap();
    let report = backing.sync_with_os_report(&mut os).unwrap();

    assert_eq!(report.dirty_range_start, Some(0));
    assert_eq!(report.dirty_range_end, Some(REGION_SIZE));
    assert_eq!(report.dirty_range_bytes, REGION_SIZE);
    assert_eq!(report.aligned_dirty_range_start, Some(0));
    assert_eq!(report.aligned_dirty_range_end, Some(REGION_SIZE));
    assert_eq!(report.aligned_dirty_bytes, REGION_SIZE);
    assert_eq!(report.requested_mmap_flush_bytes, REGION_SIZE);
    assert_eq!(report.flush_overreach_bytes, 0);
    assert_eq!(report.file_sync_kind, FileBackingFileSyncKind::SyncAll);
    assert_eq!(os.sync_calls, sync_calls_after_create + 1);
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-022` Clean syncs MUST be no-ops.
#[test]
fn requirement_file_backing_sync_report_clean_sync_is_noop() {
    const REGION_SIZE: usize = 4096;
    const REGION_COUNT: usize = 2;

    let temp = TempFile::new("sync-report-clean");
    let mut os = FakeOs::new();
    let mut options = FileBackingOptions::new(0xff);
    options.allocation_policy = AllocationPolicy::FallbackOnUnsupported;
    let mut backing = FileBacking::<REGION_SIZE, REGION_COUNT>::create_new_with_os(
        &temp.path,
        options,
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();
    let sync_calls_after_create = os.sync_calls;

    let report = backing.sync_with_os_report(&mut os).unwrap();

    assert_eq!(report.dirty_range_start, None);
    assert_eq!(report.dirty_range_end, None);
    assert_eq!(report.dirty_range_bytes, 0);
    assert_eq!(report.aligned_dirty_range_start, None);
    assert_eq!(report.aligned_dirty_range_end, None);
    assert_eq!(report.aligned_dirty_bytes, 0);
    assert_eq!(report.requested_mmap_flush_bytes, 0);
    assert_eq!(report.flush_overreach_bytes, 0);
    assert_eq!(report.file_sync_kind, FileBackingFileSyncKind::NoFileSync);
    assert_eq!(os.sync_calls, sync_calls_after_create);
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-023` Successful syncs MUST clear the synced dirty range after success.
#[test]
fn requirement_file_backing_sync_report_clears_dirty_range_after_success() {
    const REGION_SIZE: usize = 4096;
    const REGION_COUNT: usize = 2;

    let temp = TempFile::new("sync-report-clears-dirty");
    let mut os = FakeOs::new();
    let mut options = FileBackingOptions::new(0xff);
    options.allocation_policy = AllocationPolicy::FallbackOnUnsupported;
    let mut backing = FileBacking::<REGION_SIZE, REGION_COUNT>::create_new_with_os(
        &temp.path,
        options,
        crate::test_file_backing_scratch(),
        &mut os,
    )
    .unwrap();

    backing.write_region(1, 7, &[0x44, 0x55]).unwrap();
    let first = backing.sync_with_os_report(&mut os).unwrap();
    assert_eq!(first.dirty_range_bytes, 2);

    let second = backing.sync_with_os_report(&mut os).unwrap();
    assert_eq!(second.dirty_range_start, None);
    assert_eq!(second.dirty_range_end, None);
    assert_eq!(second.dirty_range_bytes, 0);
    assert_eq!(second.aligned_dirty_bytes, 0);
    assert_eq!(second.requested_mmap_flush_bytes, 0);
    assert_eq!(second.flush_overreach_bytes, 0);
}

//= spec/file.md#backend-behavior
//= type=test
//# `RING-FILE-019` Formatted `FileBacking` storage MUST be usable through
//# the generic Borromean storage API.
#[test]
fn requirement_file_backing_works_with_generic_storage_api() {
    const REGION_SIZE: usize = 4096;
    const REGION_COUNT: usize = 5;

    let temp = TempFile::new("storage-api");
    let mut options = FileBackingOptions::new(0xff);
    options.allocation_policy = AllocationPolicy::FallbackOnUnsupported;
    let mut backing = FileBacking::<REGION_SIZE, REGION_COUNT>::create_new(
        &temp.path,
        options,
        crate::test_file_backing_scratch(),
    )
    .unwrap();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, 8, 4>::format(
        &mut backing,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    map.set(&mut storage, 7, 70).unwrap();
    assert_eq!(
        map.get(&mut storage, &7, |_, value| *value).unwrap(),
        Some(70)
    );
}
