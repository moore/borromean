use core::ops::Range;

use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::Instant;

use crate::disk::{
    encode_free_space_region_segment, encode_log_region_prefix,
    free_queue_position_for_contiguous_metadata, DiskError, FreeSpaceCursors, FreeSpaceEntry,
    FreeSpaceRegionPrologue, Header, StorageMetadata, MAIN_WAL_V2_FORMAT,
};
use crate::flash_io::{FlashIo, StorageFormatError, StorageIoError};

/// Linux allocation behavior used when creating a [`FileBacking`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AllocationPolicy {
    /// Fail creation whenever `fallocate()` fails.
    #[default]
    Strict,
    /// Fall back to `set_len` only when `fallocate()` is unavailable or unsupported.
    FallbackOnUnsupported,
}

/// mmap access advice applied after a [`FileBacking`] map is created.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MadvisePolicy {
    /// Use normal kernel mmap readahead behavior.
    #[default]
    Normal,
    /// Hint that page access is likely to be random.
    Random,
    /// Hint that page access is likely to be sequential.
    Sequential,
    /// Hint that mapped pages should be prefetched.
    WillNeed,
}

impl MadvisePolicy {
    fn advice(self) -> libc::c_int {
        match self {
            Self::Normal => libc::MADV_NORMAL,
            Self::Random => libc::MADV_RANDOM,
            Self::Sequential => libc::MADV_SEQUENTIAL,
            Self::WillNeed => libc::MADV_WILLNEED,
        }
    }
}

/// Creation and open options for [`FileBacking`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileBackingOptions {
    /// Byte used to represent erased backing storage.
    pub erased_byte: u8,
    /// File preallocation behavior used by `create_new`.
    pub allocation_policy: AllocationPolicy,
    /// mmap advice applied after the map is created.
    pub madvise_policy: MadvisePolicy,
    /// Whether `create_new` flushes the initial erased-byte state before returning.
    pub sync_on_create: bool,
}

impl FileBackingOptions {
    /// Creates options with strict allocation, normal mmap advice, and create-time sync.
    pub const fn new(erased_byte: u8) -> Self {
        Self {
            erased_byte,
            allocation_policy: AllocationPolicy::Strict,
            madvise_policy: MadvisePolicy::Normal,
            sync_on_create: true,
        }
    }
}

impl Default for FileBackingOptions {
    fn default() -> Self {
        Self::new(0xff)
    }
}

/// Alignment and length values discovered for a [`FileBacking`] file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileBackingGeometry {
    /// OS mmap page size in bytes.
    pub page_size: usize,
    /// Filesystem allocation block size in bytes.
    pub filesystem_block_size: usize,
    /// Least common multiple of page size and filesystem block size.
    pub alignment_unit: usize,
    /// Expected database file length in bytes.
    pub file_len: usize,
}

/// Caller-owned scratch used while discovering host file geometry.
pub struct FileBackingScratch {
    statvfs: core::mem::MaybeUninit<libc::statvfs>,
}

impl FileBackingScratch {
    /// Allocates caller-owned file-backing scratch.
    pub fn new() -> Self {
        Self {
            statvfs: core::mem::MaybeUninit::uninit(),
        }
    }
}

impl Default for FileBackingScratch {
    fn default() -> Self {
        Self::new()
    }
}

/// Timing split for a [`FileBacking`] durability barrier.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileBackingSyncReport {
    /// Time spent flushing dirty mmap pages.
    pub mmap_flush_nanos: u128,
    /// Time spent syncing the underlying file.
    pub file_sync_nanos: u128,
    /// Start of the exact byte range dirtied since the previous successful sync.
    pub dirty_range_start: Option<usize>,
    /// End of the exact byte range dirtied since the previous successful sync.
    pub dirty_range_end: Option<usize>,
    /// Exact dirty byte range length, or zero when no writes were tracked.
    pub dirty_range_bytes: usize,
    /// Start of the dirty range aligned to the mmap page size.
    pub aligned_dirty_range_start: Option<usize>,
    /// End of the dirty range aligned to the mmap page size.
    pub aligned_dirty_range_end: Option<usize>,
    /// Page-aligned dirty byte range length.
    pub aligned_dirty_bytes: usize,
    /// Bytes requested from the mmap flush operation.
    pub requested_mmap_flush_bytes: usize,
    /// Bytes flushed beyond the aligned dirty range.
    pub flush_overreach_bytes: usize,
    /// File sync primitive used after the mmap flush.
    pub file_sync_kind: FileBackingFileSyncKind,
}

/// File sync primitive used by [`FileBacking::sync_with_report`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileBackingFileSyncKind {
    /// No file-level sync was needed after the mmap flush.
    #[default]
    NoFileSync,
    /// `std::fs::File::sync_all`.
    SyncAll,
}

/// File-backed storage geometry errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileBackingGeometryError {
    /// The OS reported a zero page size.
    ZeroPageSize,
    /// The filesystem reported a zero block size.
    ZeroFilesystemBlockSize,
    /// The configured region size was zero.
    ZeroRegionSize,
    /// Checked geometry arithmetic overflowed.
    LengthOverflow,
    /// `REGION_SIZE` was not aligned to the discovered unit.
    RegionSizeNotAligned {
        /// Configured region size.
        region_size: usize,
        /// Required alignment unit.
        alignment_unit: usize,
    },
    /// The computed file length was not aligned to the discovered unit.
    FileLengthNotAligned {
        /// Computed file length.
        file_len: usize,
        /// Required alignment unit.
        alignment_unit: usize,
    },
    /// An existing file did not match the expected database geometry.
    UnexpectedFileLength {
        /// Expected file length.
        expected: usize,
        /// Actual file length.
        actual: u64,
    },
}

/// OS operation associated with a [`FileBackingError::Io`] failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileBackingOperation {
    /// Opening a file failed.
    Open,
    /// Reading file metadata failed.
    FileMetadata,
    /// Discovering OS page size failed.
    PageSize,
    /// Discovering filesystem block size failed.
    FileSystemBlockSize,
    /// Preallocating the file failed.
    Fallocate,
    /// Resizing the file failed.
    SetLen,
    /// Creating the mutable mmap failed.
    Mmap,
    /// Applying `madvise()` failed.
    Madvise,
    /// Flushing the mmap failed.
    Flush,
    /// Syncing the file failed.
    Sync,
}

/// Errors returned by [`FileBacking`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileBackingError {
    /// Disk structure encoding or decoding failed.
    Disk(DiskError),
    /// File geometry or alignment was invalid.
    Geometry(FileBackingGeometryError),
    /// A metadata field did not match this backing's const geometry or options.
    MetadataMismatch {
        /// Metadata field being checked.
        field: FileBackingMetadataField,
        /// Value expected by this `FileBacking`.
        expected: u32,
        /// Value decoded from metadata.
        actual: u32,
    },
    /// The requested region index was outside the configured range.
    InvalidRegionIndex(u32),
    /// A byte-range operation exceeded the backing storage bounds.
    OutOfBounds,
    /// An OS operation failed.
    Io {
        /// Operation that failed.
        operation: FileBackingOperation,
        /// Captured `errno`, when available.
        raw_os_error: Option<i32>,
    },
}

impl FileBackingError {
    fn from_io_error(operation: FileBackingOperation, error: io::Error) -> Self {
        Self::Io {
            operation,
            raw_os_error: error.raw_os_error(),
        }
    }

    fn last_os_error(operation: FileBackingOperation) -> Self {
        Self::from_io_error(operation, io::Error::last_os_error())
    }

    fn is_unsupported_preallocation(self) -> bool {
        matches!(
            self,
            Self::Io {
                operation: FileBackingOperation::Fallocate,
                raw_os_error: Some(libc::ENOSYS | libc::EOPNOTSUPP)
            }
        )
    }
}

impl From<DiskError> for FileBackingError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

/// Metadata fields validated against `FileBacking` geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileBackingMetadataField {
    /// `StorageMetadata.region_size`.
    RegionSize,
    /// `StorageMetadata.region_count`.
    RegionCount,
    /// `StorageMetadata.erased_byte`.
    ErasedByte,
}

/// Errors returned while formatting [`FileBacking`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileBackingFormatError {
    /// Formatting failed while encoding a disk structure.
    Disk(DiskError),
    /// Formatting failed while mutating the file backing.
    Backing(FileBackingError),
    /// The configured store is too small for the requested free-region policy.
    InsufficientRegions {
        /// Total number of configured regions.
        region_count: u32,
        /// Requested minimum number of free regions.
        min_free_regions: u32,
    },
    /// `REGION_SIZE` is too small for required metadata or private region prologues.
    RegionSizeTooSmall {
        /// Configured region size in bytes.
        region_size: u32,
        /// Minimum required region size in bytes.
        min_region_size: u32,
    },
    /// `REGION_COUNT` does not fit in a `u32`.
    RegionCountTooLarge,
    /// `REGION_SIZE` does not fit in a `u32`.
    RegionSizeTooLarge,
}

impl From<DiskError> for FileBackingFormatError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

impl From<FileBackingError> for FileBackingFormatError {
    fn from(error: FileBackingError) -> Self {
        Self::Backing(error)
    }
}

trait FileBackingOs {
    fn page_size(&mut self) -> Result<usize, FileBackingError>;
    fn filesystem_block_size(
        &mut self,
        file: &File,
        scratch: &mut FileBackingScratch,
    ) -> Result<usize, FileBackingError>;
    fn fallocate(&mut self, file: &File, len: usize) -> Result<(), FileBackingError>;
    fn set_len(&mut self, file: &File, len: usize) -> Result<(), FileBackingError>;
    fn madvise(
        &mut self,
        address: *mut u8,
        len: usize,
        policy: MadvisePolicy,
    ) -> Result<(), FileBackingError>;
    fn sync_file(&mut self, file: &File) -> Result<(), FileBackingError>;
}

#[derive(Debug, Default)]
struct LinuxFileBackingOs;

impl FileBackingOs for LinuxFileBackingOs {
    fn page_size(&mut self) -> Result<usize, FileBackingError> {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(FileBackingError::Io {
                operation: FileBackingOperation::PageSize,
                raw_os_error: None,
            });
        }
        usize::try_from(page_size)
            .map_err(|_| FileBackingError::Geometry(FileBackingGeometryError::LengthOverflow))
    }

    fn filesystem_block_size(
        &mut self,
        file: &File,
        scratch: &mut FileBackingScratch,
    ) -> Result<usize, FileBackingError> {
        let result = unsafe { libc::fstatvfs(file.as_raw_fd(), scratch.statvfs.as_mut_ptr()) };
        if result != 0 {
            return Err(FileBackingError::last_os_error(
                FileBackingOperation::FileSystemBlockSize,
            ));
        }
        let stat = unsafe { scratch.statvfs.assume_init() };
        let block_size = if stat.f_frsize == 0 {
            stat.f_bsize
        } else {
            stat.f_frsize
        };
        usize::try_from(block_size)
            .map_err(|_| FileBackingError::Geometry(FileBackingGeometryError::LengthOverflow))
    }

    fn fallocate(&mut self, file: &File, len: usize) -> Result<(), FileBackingError> {
        let len = libc::off_t::try_from(len)
            .map_err(|_| FileBackingError::Geometry(FileBackingGeometryError::LengthOverflow))?;
        let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len) };
        if result != 0 {
            return Err(FileBackingError::last_os_error(
                FileBackingOperation::Fallocate,
            ));
        }
        Ok(())
    }

    fn set_len(&mut self, file: &File, len: usize) -> Result<(), FileBackingError> {
        let len = u64::try_from(len)
            .map_err(|_| FileBackingError::Geometry(FileBackingGeometryError::LengthOverflow))?;
        file.set_len(len)
            .map_err(|error| FileBackingError::from_io_error(FileBackingOperation::SetLen, error))
    }

    fn madvise(
        &mut self,
        address: *mut u8,
        len: usize,
        policy: MadvisePolicy,
    ) -> Result<(), FileBackingError> {
        let result = unsafe { libc::madvise(address.cast(), len, policy.advice()) };
        if result != 0 {
            return Err(FileBackingError::last_os_error(
                FileBackingOperation::Madvise,
            ));
        }
        Ok(())
    }

    fn sync_file(&mut self, file: &File) -> Result<(), FileBackingError> {
        file.sync_all()
            .map_err(|error| FileBackingError::from_io_error(FileBackingOperation::Sync, error))
    }
}

impl FileBackingGeometry {
    fn discover<const REGION_SIZE: usize, const REGION_COUNT: usize, OS: FileBackingOs>(
        file: &File,
        os: &mut OS,
        scratch: &mut FileBackingScratch,
    ) -> Result<Self, FileBackingError> {
        let page_size = os.page_size()?;
        let filesystem_block_size = os.filesystem_block_size(file, scratch)?;
        Self::new::<REGION_SIZE, REGION_COUNT>(page_size, filesystem_block_size)
            .map_err(FileBackingError::Geometry)
    }

    fn new<const REGION_SIZE: usize, const REGION_COUNT: usize>(
        page_size: usize,
        filesystem_block_size: usize,
    ) -> Result<Self, FileBackingGeometryError> {
        if page_size == 0 {
            return Err(FileBackingGeometryError::ZeroPageSize);
        }
        if filesystem_block_size == 0 {
            return Err(FileBackingGeometryError::ZeroFilesystemBlockSize);
        }
        if REGION_SIZE == 0 {
            return Err(FileBackingGeometryError::ZeroRegionSize);
        }

        let alignment_unit = lcm(page_size, filesystem_block_size)
            .ok_or(FileBackingGeometryError::LengthOverflow)?;
        if !REGION_SIZE.is_multiple_of(alignment_unit) {
            return Err(FileBackingGeometryError::RegionSizeNotAligned {
                region_size: REGION_SIZE,
                alignment_unit,
            });
        }

        let region_slots = REGION_COUNT
            .checked_add(1)
            .ok_or(FileBackingGeometryError::LengthOverflow)?;
        let file_len = REGION_SIZE
            .checked_mul(region_slots)
            .ok_or(FileBackingGeometryError::LengthOverflow)?;
        if !file_len.is_multiple_of(alignment_unit) {
            return Err(FileBackingGeometryError::FileLengthNotAligned {
                file_len,
                alignment_unit,
            });
        }

        Ok(Self {
            page_size,
            filesystem_block_size,
            alignment_unit,
            file_len,
        })
    }

    fn validate_existing_file_len(self, file: &File) -> Result<(), FileBackingError> {
        let actual = file
            .metadata()
            .map_err(|error| {
                FileBackingError::from_io_error(FileBackingOperation::FileMetadata, error)
            })?
            .len();
        let expected = u64::try_from(self.file_len)
            .map_err(|_| FileBackingError::Geometry(FileBackingGeometryError::LengthOverflow))?;
        if actual != expected {
            return Err(FileBackingError::Geometry(
                FileBackingGeometryError::UnexpectedFileLength {
                    expected: self.file_len,
                    actual,
                },
            ));
        }
        Ok(())
    }
}

/// Linux host-file storage backing implemented with a mutable mmap.
pub struct FileBacking<const REGION_SIZE: usize, const REGION_COUNT: usize> {
    file: File,
    map: MmapMut,
    options: FileBackingOptions,
    geometry: FileBackingGeometry,
    dirty_range: Option<Range<usize>>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> core::fmt::Debug
    for FileBacking<REGION_SIZE, REGION_COUNT>
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("FileBacking")
            .field("options", &self.options)
            .field("geometry", &self.geometry)
            .finish_non_exhaustive()
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> FileBacking<REGION_SIZE, REGION_COUNT> {
    /// Creates a new database file and maps it into memory.
    pub fn create_new(
        path: impl AsRef<Path>,
        options: FileBackingOptions,
        scratch: &mut FileBackingScratch,
    ) -> Result<Self, FileBackingError> {
        let mut os = LinuxFileBackingOs;
        Self::create_new_with_os(path.as_ref(), options, scratch, &mut os)
    }

    /// Opens an existing database file and maps it into memory.
    pub fn open_existing(
        path: impl AsRef<Path>,
        options: FileBackingOptions,
        scratch: &mut FileBackingScratch,
    ) -> Result<Self, FileBackingError> {
        let mut os = LinuxFileBackingOs;
        Self::open_existing_with_os(path.as_ref(), options, scratch, &mut os)
    }

    /// Returns file geometry discovered at create/open time.
    pub fn geometry(&self) -> FileBackingGeometry {
        self.geometry
    }

    /// Returns the options used to create or open this backing.
    pub fn options(&self) -> FileBackingOptions {
        self.options
    }

    /// Reads from the metadata region plus data regions as one contiguous space.
    pub fn read_storage(&self, offset: usize, buffer: &mut [u8]) -> Result<(), FileBackingError> {
        let range = checked_range(offset, buffer.len(), self.geometry.file_len)?;
        buffer.copy_from_slice(&self.map[range]);
        Ok(())
    }

    /// Reads formatted storage metadata.
    pub fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, FileBackingError> {
        let metadata_region = self.metadata_region();
        if metadata_region
            .iter()
            .all(|byte| *byte == self.options.erased_byte)
        {
            return Ok(None);
        }

        let metadata = StorageMetadata::decode(metadata_region)?;
        self.validate_metadata(metadata)?;
        Ok(Some(metadata))
    }

    /// Writes formatted storage metadata into the metadata region.
    pub fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), FileBackingError> {
        self.validate_metadata(metadata)?;
        let erased_byte = self.options.erased_byte;
        {
            let metadata_region = self.metadata_region_mut();
            metadata_region.fill(erased_byte);
            metadata.encode_into(metadata_region)?;
        }
        self.mark_dirty_range(0..REGION_SIZE);
        Ok(())
    }

    /// Reads bytes from a single data region and passes the mmap slice to `read`.
    pub fn read_region<R, F>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, FileBackingError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let range = self.region_range(region_index, offset, len)?;
        Ok(read(&self.map[range]))
    }

    /// Writes bytes to a single data region.
    pub fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), FileBackingError> {
        let range = self.region_range(region_index, offset, data.len())?;
        self.map[range.clone()].copy_from_slice(data);
        self.mark_dirty_range(range);
        Ok(())
    }

    /// Erases a single data region to the configured erased byte.
    pub fn erase_region(&mut self, region_index: u32) -> Result<(), FileBackingError> {
        let range = self.region_range(region_index, 0, REGION_SIZE)?;
        self.map[range.clone()].fill(self.options.erased_byte);
        self.mark_dirty_range(range);
        Ok(())
    }

    /// Flushes the mmap and syncs the underlying file.
    pub fn sync(&mut self) -> Result<(), FileBackingError> {
        let mut os = LinuxFileBackingOs;
        self.sync_with_os(&mut os)
    }

    /// Flushes the mmap, syncs the underlying file, and returns a timing split.
    pub fn sync_with_report(&mut self) -> Result<FileBackingSyncReport, FileBackingError> {
        let mut os = LinuxFileBackingOs;
        self.sync_with_os_report(&mut os)
    }

    /// Formats the file backing as an empty Borromean store.
    pub fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, FileBackingFormatError> {
        let region_size =
            u32::try_from(REGION_SIZE).map_err(|_| FileBackingFormatError::RegionSizeTooLarge)?;
        let region_count =
            u32::try_from(REGION_COUNT).map_err(|_| FileBackingFormatError::RegionCountTooLarge)?;

        let metadata = StorageMetadata::new(
            region_size,
            region_count,
            min_free_regions,
            wal_write_granule,
            self.options.erased_byte,
            wal_record_magic,
        )?;
        let free_space_entries_offset = Header::ENCODED_LEN + FreeSpaceRegionPrologue::ENCODED_LEN;
        let entries_per_metadata_region = REGION_SIZE
            .checked_sub(free_space_entries_offset)
            .ok_or(FileBackingFormatError::RegionSizeTooSmall {
                region_size,
                min_region_size: u32::try_from(free_space_entries_offset).unwrap_or(u32::MAX),
            })?
            / FreeSpaceEntry::ENCODED_LEN;
        if entries_per_metadata_region == 0 {
            return Err(FileBackingFormatError::RegionSizeTooSmall {
                region_size,
                min_region_size: u32::try_from(
                    free_space_entries_offset + FreeSpaceEntry::ENCODED_LEN,
                )
                .unwrap_or(u32::MAX),
            });
        }
        let mut metadata_region_count = 1u32;
        loop {
            if region_count <= 1 + metadata_region_count {
                return Err(FileBackingFormatError::InsufficientRegions {
                    region_count,
                    min_free_regions,
                });
            }
            let free_space_entry_count = region_count - 1 - metadata_region_count;
            if free_space_entry_count < min_free_regions {
                return Err(FileBackingFormatError::InsufficientRegions {
                    region_count,
                    min_free_regions,
                });
            }
            let capacity = metadata_region_count
                .checked_mul(
                    u32::try_from(entries_per_metadata_region)
                        .map_err(|_| FileBackingFormatError::RegionCountTooLarge)?,
                )
                .ok_or(FileBackingFormatError::RegionCountTooLarge)?;
            if capacity >= free_space_entry_count {
                break;
            }
            metadata_region_count = metadata_region_count
                .checked_add(1)
                .ok_or(FileBackingFormatError::RegionCountTooLarge)?;
        }
        let free_space_entry_count = region_count - 1 - metadata_region_count;
        let free_space_region_len = free_space_entries_offset + FreeSpaceEntry::ENCODED_LEN;
        if REGION_SIZE < free_space_region_len {
            let min_region_size = u32::try_from(free_space_region_len).unwrap_or(u32::MAX);
            return Err(FileBackingFormatError::RegionSizeTooSmall {
                region_size,
                min_region_size,
            });
        }

        self.write_metadata(metadata)?;

        for region_index in 0..region_count {
            self.erase_region(region_index)?;
        }

        let mut prefix = [self.options.erased_byte; REGION_SIZE];
        let tail = free_queue_position_for_contiguous_metadata(
            1,
            metadata_region_count,
            entries_per_metadata_region,
            free_space_entry_count,
        )?;
        let allocation_head = free_queue_position_for_contiguous_metadata(
            1,
            metadata_region_count,
            entries_per_metadata_region,
            0,
        )?;
        let cursors = FreeSpaceCursors::new(allocation_head, tail, tail);
        let prefix_len = encode_log_region_prefix(
            &mut prefix,
            metadata,
            u64::from(metadata_region_count),
            MAIN_WAL_V2_FORMAT,
            0,
            cursors,
        )?;
        self.write_region(0, 0, &prefix[..prefix_len])?;

        let mut free_space_region = [self.options.erased_byte; REGION_SIZE];
        let mut entries = [0u32; REGION_COUNT];
        let entry_count = usize::try_from(free_space_entry_count)
            .map_err(|_| FileBackingFormatError::RegionCountTooLarge)?;
        for (slot, region_index) in entries[..entry_count]
            .iter_mut()
            .zip((1 + metadata_region_count)..region_count)
        {
            *slot = region_index;
        }
        let tail = free_queue_position_for_contiguous_metadata(
            1,
            metadata_region_count,
            entries_per_metadata_region,
            free_space_entry_count,
        )?;
        let cursors = FreeSpaceCursors::new(allocation_head, tail, tail);
        for metadata_region_index in 0..metadata_region_count {
            let region_index = 1 + metadata_region_index;
            let start = usize::try_from(metadata_region_index)
                .map_err(|_| FileBackingFormatError::RegionCountTooLarge)?
                .checked_mul(entries_per_metadata_region)
                .ok_or(FileBackingFormatError::RegionCountTooLarge)?;
            let end = start
                .saturating_add(entries_per_metadata_region)
                .min(entry_count);
            let next_metadata_region = if metadata_region_index + 1 < metadata_region_count {
                Some(region_index + 1)
            } else {
                None
            };
            let free_space_len = encode_free_space_region_segment(
                &mut free_space_region,
                metadata,
                u64::from(metadata_region_index),
                region_index,
                cursors,
                next_metadata_region,
                &entries[start..end],
            )?;
            self.write_region(region_index, 0, &free_space_region[..free_space_len])?;
        }

        self.sync()?;
        Ok(metadata)
    }

    fn create_new_with_os<OS: FileBackingOs>(
        path: &Path,
        options: FileBackingOptions,
        scratch: &mut FileBackingScratch,
        os: &mut OS,
    ) -> Result<Self, FileBackingError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| FileBackingError::from_io_error(FileBackingOperation::Open, error))?;
        let geometry =
            FileBackingGeometry::discover::<REGION_SIZE, REGION_COUNT, _>(&file, os, scratch)?;
        let mut file_len_set_by_fallback = false;
        match os.fallocate(&file, geometry.file_len) {
            Ok(()) => {}
            Err(error)
                if options.allocation_policy == AllocationPolicy::FallbackOnUnsupported
                    && error.is_unsupported_preallocation() =>
            {
                os.set_len(&file, geometry.file_len)?;
                file_len_set_by_fallback = true;
            }
            Err(error) => return Err(error),
        }
        if !file_len_set_by_fallback {
            os.set_len(&file, geometry.file_len)?;
        }

        let map = unsafe { MmapOptions::new().len(geometry.file_len).map_mut(&file) }
            .map_err(|error| FileBackingError::from_io_error(FileBackingOperation::Mmap, error))?;
        let mut backing = Self {
            file,
            map,
            options,
            geometry,
            dirty_range: None,
        };
        backing.map.fill(options.erased_byte);
        backing.mark_dirty_range(0..geometry.file_len);
        backing.apply_madvise_with_os(os)?;
        if options.sync_on_create {
            backing.sync_with_os(os)?;
        }
        Ok(backing)
    }

    fn open_existing_with_os<OS: FileBackingOs>(
        path: &Path,
        options: FileBackingOptions,
        scratch: &mut FileBackingScratch,
        os: &mut OS,
    ) -> Result<Self, FileBackingError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| FileBackingError::from_io_error(FileBackingOperation::Open, error))?;
        let geometry =
            FileBackingGeometry::discover::<REGION_SIZE, REGION_COUNT, _>(&file, os, scratch)?;
        geometry.validate_existing_file_len(&file)?;
        let map = unsafe { MmapOptions::new().len(geometry.file_len).map_mut(&file) }
            .map_err(|error| FileBackingError::from_io_error(FileBackingOperation::Mmap, error))?;
        let mut backing = Self {
            file,
            map,
            options,
            geometry,
            dirty_range: None,
        };
        backing.apply_madvise_with_os(os)?;
        Ok(backing)
    }

    fn apply_madvise_with_os<OS: FileBackingOs>(
        &mut self,
        os: &mut OS,
    ) -> Result<(), FileBackingError> {
        os.madvise(
            self.map.as_mut_ptr(),
            self.geometry.file_len,
            self.options.madvise_policy,
        )
    }

    fn sync_with_os<OS: FileBackingOs>(&mut self, os: &mut OS) -> Result<(), FileBackingError> {
        self.sync_with_os_report(os).map(|_| ())
    }

    fn sync_with_os_report<OS: FileBackingOs>(
        &mut self,
        os: &mut OS,
    ) -> Result<FileBackingSyncReport, FileBackingError> {
        let dirty_range = self.dirty_range.clone();
        let aligned_dirty_range = dirty_range
            .as_ref()
            .and_then(|range| self.page_aligned_range(range));
        let dirty_range_bytes = dirty_range
            .as_ref()
            .map_or(0, |range| range.end.saturating_sub(range.start));
        let aligned_dirty_bytes = aligned_dirty_range
            .as_ref()
            .map_or(0, |range| range.end.saturating_sub(range.start));
        let requested_mmap_flush_bytes = aligned_dirty_bytes;
        let flush_overreach_bytes = requested_mmap_flush_bytes.saturating_sub(aligned_dirty_bytes);
        let needs_file_sync = dirty_range
            .as_ref()
            .is_some_and(|range| range.start < REGION_SIZE);

        let mmap_flush_nanos = if let Some(range) = aligned_dirty_range.as_ref() {
            let flush_start = Instant::now();
            self.map
                .flush_range(range.start, range.end - range.start)
                .map_err(|error| {
                    FileBackingError::from_io_error(FileBackingOperation::Flush, error)
                })?;
            flush_start.elapsed().as_nanos()
        } else {
            0
        };
        let file_sync_nanos = if needs_file_sync {
            let file_sync_start = Instant::now();
            os.sync_file(&self.file)?;
            file_sync_start.elapsed().as_nanos()
        } else {
            0
        };
        self.dirty_range = None;
        Ok(FileBackingSyncReport {
            mmap_flush_nanos,
            file_sync_nanos,
            dirty_range_start: dirty_range.as_ref().map(|range| range.start),
            dirty_range_end: dirty_range.as_ref().map(|range| range.end),
            dirty_range_bytes,
            aligned_dirty_range_start: aligned_dirty_range.as_ref().map(|range| range.start),
            aligned_dirty_range_end: aligned_dirty_range.as_ref().map(|range| range.end),
            aligned_dirty_bytes,
            requested_mmap_flush_bytes,
            flush_overreach_bytes,
            file_sync_kind: if needs_file_sync {
                FileBackingFileSyncKind::SyncAll
            } else {
                FileBackingFileSyncKind::NoFileSync
            },
        })
    }

    fn mark_dirty_range(&mut self, range: Range<usize>) {
        if range.start == range.end {
            return;
        }
        self.dirty_range = Some(match self.dirty_range.take() {
            Some(existing) => existing.start.min(range.start)..existing.end.max(range.end),
            None => range,
        });
    }

    fn page_aligned_range(&self, range: &Range<usize>) -> Option<Range<usize>> {
        if range.start == range.end {
            return None;
        }
        let page_size = self.geometry.page_size;
        let start = range.start - (range.start % page_size);
        let end = align_up(range.end, page_size)?.min(self.geometry.file_len);
        Some(start..end)
    }

    fn metadata_region(&self) -> &[u8] {
        &self.map[..REGION_SIZE]
    }

    fn metadata_region_mut(&mut self) -> &mut [u8] {
        &mut self.map[..REGION_SIZE]
    }

    fn validate_metadata(&self, metadata: StorageMetadata) -> Result<(), FileBackingError> {
        let region_size = u32::try_from(REGION_SIZE)
            .map_err(|_| FileBackingError::Geometry(FileBackingGeometryError::LengthOverflow))?;
        let region_count = u32::try_from(REGION_COUNT)
            .map_err(|_| FileBackingError::Geometry(FileBackingGeometryError::LengthOverflow))?;
        if metadata.region_size != region_size {
            return Err(FileBackingError::MetadataMismatch {
                field: FileBackingMetadataField::RegionSize,
                expected: region_size,
                actual: metadata.region_size,
            });
        }
        if metadata.region_count != region_count {
            return Err(FileBackingError::MetadataMismatch {
                field: FileBackingMetadataField::RegionCount,
                expected: region_count,
                actual: metadata.region_count,
            });
        }
        if metadata.erased_byte != self.options.erased_byte {
            return Err(FileBackingError::MetadataMismatch {
                field: FileBackingMetadataField::ErasedByte,
                expected: u32::from(self.options.erased_byte),
                actual: u32::from(metadata.erased_byte),
            });
        }
        Ok(())
    }

    fn region_range(
        &self,
        region_index: u32,
        offset: usize,
        len: usize,
    ) -> Result<Range<usize>, FileBackingError> {
        let index = usize::try_from(region_index)
            .map_err(|_| FileBackingError::InvalidRegionIndex(region_index))?;
        if index >= REGION_COUNT {
            return Err(FileBackingError::InvalidRegionIndex(region_index));
        }
        let region_offset_range = checked_range(offset, len, REGION_SIZE)?;
        let region_start = REGION_SIZE
            .checked_mul(index.checked_add(1).ok_or(FileBackingError::OutOfBounds)?)
            .ok_or(FileBackingError::OutOfBounds)?;
        let absolute_start = region_start
            .checked_add(region_offset_range.start)
            .ok_or(FileBackingError::OutOfBounds)?;
        let absolute_end = region_start
            .checked_add(region_offset_range.end)
            .ok_or(FileBackingError::OutOfBounds)?;
        Ok(absolute_start..absolute_end)
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> FlashIo
    for FileBacking<REGION_SIZE, REGION_COUNT>
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, StorageIoError> {
        Self::read_metadata(self).map_err(StorageIoError::from)
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), StorageIoError> {
        Self::write_metadata(self, metadata).map_err(StorageIoError::from)
    }

    fn read_region<R, F>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, StorageIoError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        Self::read_region(self, region_index, offset, len, read).map_err(StorageIoError::from)
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageIoError> {
        Self::write_region(self, region_index, offset, data).map_err(StorageIoError::from)
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), StorageIoError> {
        Self::erase_region(self, region_index).map_err(StorageIoError::from)
    }

    fn sync(&mut self) -> Result<(), StorageIoError> {
        Self::sync(self).map_err(StorageIoError::from)
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, StorageFormatError> {
        Self::format_empty_store(self, min_free_regions, wal_write_granule, wal_record_magic)
            .map_err(StorageFormatError::from)
    }
}

fn checked_range(
    offset: usize,
    len: usize,
    total_len: usize,
) -> Result<Range<usize>, FileBackingError> {
    let end = offset
        .checked_add(len)
        .ok_or(FileBackingError::OutOfBounds)?;
    if end > total_len {
        return Err(FileBackingError::OutOfBounds);
    }
    Ok(offset..end)
}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    if alignment == 0 {
        return None;
    }
    let remainder = value % alignment;
    if remainder == 0 {
        return Some(value);
    }
    value.checked_add(alignment - remainder)
}

fn gcd(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn lcm(left: usize, right: usize) -> Option<usize> {
    left.checked_div(gcd(left, right))?.checked_mul(right)
}

#[cfg(test)]
mod tests;
