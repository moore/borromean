use crate::mock::{MockError, MockFormatError};
use crate::{MockFlash, StorageMetadata};

/// Errors returned by caller-owned backing implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageIoError {
    /// The in-memory mock backend failed.
    Mock(MockError),
    /// The Linux file-backed mmap backend failed.
    #[cfg(all(feature = "file-backing", target_os = "linux"))]
    FileBacking(crate::file_backing::FileBackingError),
}

impl From<MockError> for StorageIoError {
    fn from(error: MockError) -> Self {
        Self::Mock(error)
    }
}

#[cfg(all(feature = "file-backing", target_os = "linux"))]
impl From<crate::file_backing::FileBackingError> for StorageIoError {
    fn from(error: crate::file_backing::FileBackingError) -> Self {
        Self::FileBacking(error)
    }
}

/// Errors returned while formatting caller-owned backing implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFormatError {
    /// The in-memory mock backend failed during formatting.
    Mock(MockFormatError),
    /// The Linux file-backed mmap backend failed during formatting.
    #[cfg(all(feature = "file-backing", target_os = "linux"))]
    FileBacking(crate::file_backing::FileBackingFormatError),
}

impl From<MockFormatError> for StorageFormatError {
    fn from(error: MockFormatError) -> Self {
        Self::Mock(error)
    }
}

#[cfg(all(feature = "file-backing", target_os = "linux"))]
impl From<crate::file_backing::FileBackingFormatError> for StorageFormatError {
    fn from(error: crate::file_backing::FileBackingFormatError) -> Self {
        Self::FileBacking(error)
    }
}

/// Caller-owned flash or transport interface used by Borromean core.
pub trait FlashIo {
    /// Reads the storage metadata region, returning `None` for unformatted media.
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, StorageIoError>;

    /// Writes the storage metadata region durably.
    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), StorageIoError>;

    /// Reads bytes from a region and passes the borrowed bytes to `read`.
    fn read_region<R, F>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, StorageIoError>
    where
        F: FnOnce(&[u8]) -> R;

    /// Writes bytes into a region at the supplied offset.
    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageIoError>;

    /// Erases an entire region.
    fn erase_region(&mut self, region_index: u32) -> Result<(), StorageIoError>;

    /// Applies any durability barrier required by the target medium.
    fn sync(&mut self) -> Result<(), StorageIoError>;

    /// Formats empty metadata and region state for a fresh store.
    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, StorageFormatError>;
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> FlashIo
    for MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
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
