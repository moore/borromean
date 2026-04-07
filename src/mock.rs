use heapless::Vec;

use crate::disk::{
    DiskError, FreePointerFooter, Header, StorageMetadata, WalRegionPrologue, WAL_V1_FORMAT,
};
use crate::CollectionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockError {
    InvalidRegionIndex(u32),
    OutOfBounds,
    LogFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockFormatError {
    Disk(DiskError),
    Mock(MockError),
    InsufficientRegions {
        region_count: u32,
        min_free_regions: u32,
    },
    RegionCountTooLarge,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockOperation {
    ReadMetadata,
    WriteMetadata,
    ReadRegion {
        region_index: u32,
        offset: usize,
        len: usize,
    },
    WriteRegion {
        region_index: u32,
        offset: usize,
        len: usize,
    },
    EraseRegion {
        region_index: u32,
    },
    Sync,
}

pub struct MockFlash<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> {
    metadata: Option<StorageMetadata>,
    metadata_region: [u8; REGION_SIZE],
    regions: [[u8; REGION_SIZE]; REGION_COUNT],
    erased_byte: u8,
    log: Vec<MockOperation, MAX_LOG>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>
    MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    pub fn new(erased_byte: u8) -> Self {
        Self {
            metadata: None,
            metadata_region: [erased_byte; REGION_SIZE],
            regions: core::array::from_fn(|_| [erased_byte; REGION_SIZE]),
            erased_byte,
            log: Vec::new(),
        }
    }

    pub fn metadata(&self) -> Option<&StorageMetadata> {
        self.metadata.as_ref()
    }

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
            remaining[..chunk_len]
                .copy_from_slice(&source[source_offset..source_offset + chunk_len]);
            remaining = &mut remaining[chunk_len..];
            current_offset = current_offset
                .checked_add(chunk_len)
                .ok_or(MockError::OutOfBounds)?;
        }

        Ok(())
    }

    pub fn operations(&self) -> &[MockOperation] {
        self.log.as_slice()
    }

    pub fn region_bytes(&self, region_index: u32) -> Result<&[u8; REGION_SIZE], MockError> {
        self.region(region_index)
    }

    pub fn clear_operations(&mut self) {
        self.log.clear();
    }

    pub fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, MockError> {
        self.log(MockOperation::ReadMetadata)?;
        Ok(self.metadata)
    }

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

    pub fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), MockError> {
        let len = buffer.len();
        self.log(MockOperation::ReadRegion {
            region_index,
            offset,
            len,
        })?;
        let region = self.region(region_index)?;
        let end = offset.checked_add(len).ok_or(MockError::OutOfBounds)?;
        let source = region.get(offset..end).ok_or(MockError::OutOfBounds)?;
        buffer.copy_from_slice(source);
        Ok(())
    }

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

    pub fn erase_region(&mut self, region_index: u32) -> Result<(), MockError> {
        self.log(MockOperation::EraseRegion { region_index })?;
        let erased_byte = self.erased_byte;
        let region = self.region_mut(region_index)?;
        region.fill(erased_byte);
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), MockError> {
        self.log(MockOperation::Sync)
    }

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

        if region_count < 2 + min_free_regions {
            return Err(MockFormatError::InsufficientRegions {
                region_count,
                min_free_regions,
            });
        }

        let metadata = StorageMetadata::new(
            region_size,
            region_count,
            min_free_regions,
            wal_write_granule,
            self.erased_byte,
            wal_record_magic,
        )?;

        self.write_metadata(metadata)?;

        for region_index in 0..region_count {
            self.erase_region(region_index)?;
        }

        let header = Header {
            sequence: 0,
            collection_id: CollectionId(0),
            collection_format: WAL_V1_FORMAT,
        };
        let mut header_bytes = [0u8; Header::ENCODED_LEN];
        header.encode_into(&mut header_bytes)?;
        self.write_region(0, 0, &header_bytes)?;

        let prologue = WalRegionPrologue {
            wal_head_region_index: 0,
        };
        let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
        prologue.encode_into(&mut prologue_bytes, region_count)?;
        self.write_region(0, Header::ENCODED_LEN, &prologue_bytes)?;

        let footer_offset = REGION_SIZE - FreePointerFooter::ENCODED_LEN;
        for region_index in 1..region_count {
            let next_tail = if region_index + 1 < region_count {
                Some(region_index + 1)
            } else {
                None
            };
            let footer = FreePointerFooter { next_tail };
            let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
            footer.encode_into(&mut footer_bytes, self.erased_byte)?;
            self.write_region(region_index, footer_offset, &footer_bytes)?;
        }

        self.sync()?;
        Ok(metadata)
    }

    fn log(&mut self, operation: MockOperation) -> Result<(), MockError> {
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
