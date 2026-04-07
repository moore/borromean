use crate::mock::{MockError, MockFormatError};
use crate::{MockFlash, StorageMetadata};

pub trait FlashIo {
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, MockError>;

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), MockError>;

    fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), MockError>;

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), MockError>;

    fn erase_region(&mut self, region_index: u32) -> Result<(), MockError>;

    fn sync(&mut self) -> Result<(), MockError>;

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, MockFormatError>;
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> FlashIo
    for MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, MockError> {
        Self::read_metadata(self)
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), MockError> {
        Self::write_metadata(self, metadata)
    }

    fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), MockError> {
        Self::read_region(self, region_index, offset, buffer)
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), MockError> {
        Self::write_region(self, region_index, offset, data)
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), MockError> {
        Self::erase_region(self, region_index)
    }

    fn sync(&mut self) -> Result<(), MockError> {
        Self::sync(self)
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, MockFormatError> {
        Self::format_empty_store(self, min_free_regions, wal_write_granule, wal_record_magic)
    }
}
