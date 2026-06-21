use heapless::Vec;

use crate::disk::{
    encode_free_space_region_segment, encode_log_region_prefix,
    free_queue_position_for_contiguous_metadata, DiskError, FreeQueuePosition, FreeSpaceCursors,
    FreeSpaceEntry, FreeSpaceRegionPrologue, StorageMetadata, MAIN_WAL_V2_FORMAT,
};

#[cfg(test)]
use crate::{CollectionId, Header, WalRegionPrologue, WAL_V1_FORMAT};

/// Errors returned by [`MockFlash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockError {
    /// The requested region index was outside the configured range.
    InvalidRegionIndex(u32),
    /// A byte-range operation exceeded the backing storage bounds.
    OutOfBounds,
    /// The operation log reached its configured maximum length.
    LogFull,
}

/// Errors returned while formatting [`MockFlash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockFormatError {
    /// Formatting failed while encoding a disk structure.
    Disk(DiskError),
    /// Formatting failed while mutating the mock backend.
    Mock(MockError),
    /// The configured store is too small for the requested free-region policy.
    InsufficientRegions {
        /// Total number of configured regions.
        region_count: u32,
        /// Requested minimum number of free regions.
        min_free_regions: u32,
    },
    /// `REGION_SIZE` is too small for required metadata, WAL, or free-list bytes.
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

impl From<DiskError> for MockFormatError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

impl From<MockError> for MockFormatError {
    fn from(error: MockError) -> Self {
        Self::Mock(error)
    }
}

/// Recorded operations performed against [`MockFlash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockOperation {
    /// Metadata was read.
    ReadMetadata,
    /// Metadata was written.
    WriteMetadata,
    /// Region bytes were read.
    ReadRegion {
        /// Region index read.
        region_index: u32,
        /// Offset within the region.
        offset: usize,
        /// Number of bytes read.
        len: usize,
    },
    /// Region bytes were written.
    WriteRegion {
        /// Region index written.
        region_index: u32,
        /// Offset within the region.
        offset: usize,
        /// Number of bytes written.
        len: usize,
    },
    /// A region was erased.
    EraseRegion {
        /// Region index erased.
        region_index: u32,
    },
    /// A durability barrier was requested.
    Sync,
}

/// In-memory flash model used by tests, examples, and traceability checks.
pub struct MockFlash<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> {
    metadata: Option<StorageMetadata>,
    metadata_region: [u8; REGION_SIZE],
    regions: [[u8; REGION_SIZE]; REGION_COUNT],
    erased_byte: u8,
    log: Vec<MockOperation, MAX_LOG>,
    log_enabled: bool,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>
    MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    /// Creates a new erased mock device.
    pub fn new(erased_byte: u8) -> Self {
        Self {
            metadata: None,
            metadata_region: [erased_byte; REGION_SIZE],
            regions: core::array::from_fn(|_| [erased_byte; REGION_SIZE]),
            erased_byte,
            log: Vec::new(),
            log_enabled: true,
        }
    }

    /// Returns the current formatted metadata, if present.
    pub fn metadata(&self) -> Option<&StorageMetadata> {
        self.metadata.as_ref()
    }

    /// Reads from the metadata region plus data regions as one contiguous space.
    pub fn read_storage(&self, offset: usize, buffer: &mut [u8]) -> Result<(), MockError> {
        let total_len = REGION_SIZE
            .checked_mul(REGION_COUNT + 1)
            .ok_or(MockError::OutOfBounds)?;
        let end = offset
            .checked_add(buffer.len())
            .ok_or(MockError::OutOfBounds)?;
        if end > total_len {
            return Err(MockError::OutOfBounds);
        }

        let mut remaining = buffer;
        let mut current_offset = offset;
        while !remaining.is_empty() {
            let (source, source_offset) = if current_offset < REGION_SIZE {
                (&self.metadata_region[..], current_offset)
            } else {
                let region_space_offset = current_offset - REGION_SIZE;
                let region_index = region_space_offset / REGION_SIZE;
                let region_offset = region_space_offset % REGION_SIZE;
                let region = self
                    .regions
                    .get(region_index)
                    .ok_or(MockError::OutOfBounds)?;
                (&region[..], region_offset)
            };

            let available = source
                .len()
                .checked_sub(source_offset)
                .ok_or(MockError::OutOfBounds)?;
            let chunk_len = remaining.len().min(available);
            if chunk_len == 0 {
                return Err(MockError::OutOfBounds);
            }
            remaining[..chunk_len]
                .copy_from_slice(&source[source_offset..source_offset + chunk_len]);
            remaining = &mut remaining[chunk_len..];
            current_offset = current_offset
                .checked_add(chunk_len)
                .ok_or(MockError::OutOfBounds)?;
        }

        Ok(())
    }

    /// Returns the recorded operation log.
    pub fn operations(&self) -> &[MockOperation] {
        self.log.as_slice()
    }

    /// Returns immutable bytes for a single region.
    pub fn region_bytes(&self, region_index: u32) -> Result<&[u8; REGION_SIZE], MockError> {
        self.region(region_index)
    }

    /// Clears the recorded operation log.
    pub fn clear_operations(&mut self) {
        self.log.clear();
    }

    /// Enables or disables operation recording.
    pub fn set_operation_logging(&mut self, enabled: bool) {
        self.log_enabled = enabled;
        if !enabled {
            self.log.clear();
        }
    }

    /// Reads formatted storage metadata.
    pub fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, MockError> {
        self.log(MockOperation::ReadMetadata)?;
        Ok(self.metadata)
    }

    /// Writes formatted storage metadata and updates the metadata region bytes.
    pub fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), MockError> {
        self.log(MockOperation::WriteMetadata)?;
        if REGION_SIZE < StorageMetadata::ENCODED_LEN {
            return Err(MockError::OutOfBounds);
        }
        self.metadata_region.fill(self.erased_byte);
        metadata
            .encode_into(&mut self.metadata_region)
            .map_err(|_| MockError::OutOfBounds)?;
        self.metadata = Some(metadata);
        Ok(())
    }

    /// Reads bytes from a single region and passes them to `read`.
    pub fn read_region<R, F>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, MockError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.log(MockOperation::ReadRegion {
            region_index,
            offset,
            len,
        })?;
        let region = self.region(region_index)?;
        let end = offset.checked_add(len).ok_or(MockError::OutOfBounds)?;
        let source = region.get(offset..end).ok_or(MockError::OutOfBounds)?;
        Ok(read(source))
    }

    /// Writes bytes to a single region.
    pub fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), MockError> {
        self.log(MockOperation::WriteRegion {
            region_index,
            offset,
            len: data.len(),
        })?;
        let region = self.region_mut(region_index)?;
        let end = offset
            .checked_add(data.len())
            .ok_or(MockError::OutOfBounds)?;
        let target = region.get_mut(offset..end).ok_or(MockError::OutOfBounds)?;
        target.copy_from_slice(data);
        Ok(())
    }

    /// Erases a single region to the configured erased byte.
    pub fn erase_region(&mut self, region_index: u32) -> Result<(), MockError> {
        self.log(MockOperation::EraseRegion { region_index })?;
        let erased_byte = self.erased_byte;
        let region = self.region_mut(region_index)?;
        region.fill(erased_byte);
        Ok(())
    }

    /// Records a durability barrier in the operation log.
    pub fn sync(&mut self) -> Result<(), MockError> {
        self.log(MockOperation::Sync)
    }

    /// Formats the mock device as an empty Borromean store.
    pub fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, MockFormatError> {
        let region_size =
            u32::try_from(REGION_SIZE).map_err(|_| MockFormatError::RegionSizeTooLarge)?;
        let region_count =
            u32::try_from(REGION_COUNT).map_err(|_| MockFormatError::RegionCountTooLarge)?;

        let metadata = StorageMetadata::new(
            region_size,
            region_count,
            min_free_regions,
            wal_write_granule,
            self.erased_byte,
            wal_record_magic,
        )?;
        let wal_record_area_offset = metadata.wal_record_area_offset()?;
        let free_space_entries_offset =
            crate::Header::ENCODED_LEN + FreeSpaceRegionPrologue::ENCODED_LEN;
        let entries_per_metadata_region = REGION_SIZE
            .checked_sub(free_space_entries_offset)
            .ok_or(MockFormatError::RegionSizeTooSmall {
                region_size,
                min_region_size: u32::try_from(free_space_entries_offset).unwrap_or(u32::MAX),
            })?
            / FreeSpaceEntry::ENCODED_LEN;
        if entries_per_metadata_region == 0 {
            return Err(MockFormatError::RegionSizeTooSmall {
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
                return Err(MockFormatError::InsufficientRegions {
                    region_count,
                    min_free_regions,
                });
            }
            let free_space_entry_count = region_count - 1 - metadata_region_count;
            if free_space_entry_count < min_free_regions {
                return Err(MockFormatError::InsufficientRegions {
                    region_count,
                    min_free_regions,
                });
            }
            let capacity = metadata_region_count
                .checked_mul(
                    u32::try_from(entries_per_metadata_region)
                        .map_err(|_| MockFormatError::RegionCountTooLarge)?,
                )
                .ok_or(MockFormatError::RegionCountTooLarge)?;
            if capacity >= free_space_entry_count {
                break;
            }
            metadata_region_count = metadata_region_count
                .checked_add(1)
                .ok_or(MockFormatError::RegionCountTooLarge)?;
        }
        let free_space_entry_count = region_count - 1 - metadata_region_count;
        let min_region_size_usize = StorageMetadata::ENCODED_LEN
            .max(wal_record_area_offset)
            .max(free_space_entries_offset + FreeSpaceEntry::ENCODED_LEN);
        if REGION_SIZE < min_region_size_usize {
            let min_region_size = u32::try_from(min_region_size_usize).unwrap_or(u32::MAX);
            return Err(MockFormatError::RegionSizeTooSmall {
                region_size,
                min_region_size,
            });
        }

        self.write_metadata(metadata)?;

        for region_index in 0..region_count {
            self.erase_region(region_index)?;
        }

        let mut prefix = [self.erased_byte; REGION_SIZE];
        let tail = free_queue_position_for_contiguous_metadata(
            1,
            metadata_region_count,
            entries_per_metadata_region,
            free_space_entry_count,
        )?;
        let allocation_head = FreeQueuePosition {
            region_index: 1,
            entry_index: 0,
        };
        let cursors = FreeSpaceCursors::new(allocation_head, tail, tail);
        let prefix_len =
            encode_log_region_prefix(&mut prefix, metadata, 0, MAIN_WAL_V2_FORMAT, 0, cursors)?;
        self.write_region(0, 0, &prefix[..prefix_len])?;

        let mut free_space_region = [self.erased_byte; REGION_SIZE];
        let mut entries = [0u32; REGION_COUNT];
        let entry_count = usize::try_from(free_space_entry_count)
            .map_err(|_| MockFormatError::RegionCountTooLarge)?;
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
                .map_err(|_| MockFormatError::RegionCountTooLarge)?
                .checked_mul(entries_per_metadata_region)
                .ok_or(MockFormatError::RegionCountTooLarge)?;
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
                1,
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

    fn log(&mut self, operation: MockOperation) -> Result<(), MockError> {
        if !self.log_enabled {
            return Ok(());
        }
        self.log.push(operation).map_err(|_| MockError::LogFull)
    }

    fn region(&self, region_index: u32) -> Result<&[u8; REGION_SIZE], MockError> {
        let index = usize::try_from(region_index)
            .map_err(|_| MockError::InvalidRegionIndex(region_index))?;
        self.regions
            .get(index)
            .ok_or(MockError::InvalidRegionIndex(region_index))
    }

    fn region_mut(&mut self, region_index: u32) -> Result<&mut [u8; REGION_SIZE], MockError> {
        let index = usize::try_from(region_index)
            .map_err(|_| MockError::InvalidRegionIndex(region_index))?;
        self.regions
            .get_mut(index)
            .ok_or(MockError::InvalidRegionIndex(region_index))
    }
}

#[cfg(test)]
mod tests;
