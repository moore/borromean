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
            regions: core::array::from_fn(|_| [erased_byte; REGION_SIZE]),
            erased_byte,
            log: Vec::new(),
        }
    }

    pub fn metadata(&self) -> Option<&StorageMetadata> {
        self.metadata.as_ref()
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

    //= spec/implementation.md#i-o-requirements
    //# `RING-IMPL-IO-004` If the target medium does not require an
    //# explicit durability barrier, the I/O abstraction MAY implement sync as
    //# a zero-cost completed operation.
    pub fn sync(&mut self) -> Result<(), MockError> {
        self.log(MockOperation::Sync)
    }

    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-PRE-001 Backing storage MUST be writable and erasable at region granularity.
    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-PRE-006 `region_count >= 2 + min_free_regions`.
    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-PRE-003 Region `0` MUST be reserved as the initial WAL region.
    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-003 Initialize region `0` as WAL:
    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-POST-001 WAL head and WAL tail MUST both be region `0`.
    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-POST-002 A user collection durable head MUST NOT exist after formatting.
    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-POST-003 The free list MUST contain every non-WAL region in ascending region-index order.
    pub fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, MockFormatError> {
        let region_size = u32::try_from(REGION_SIZE).map_err(|_| MockFormatError::RegionSizeTooLarge)?;
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
        let index =
            usize::try_from(region_index).map_err(|_| MockError::InvalidRegionIndex(region_index))?;
        self.regions
            .get(index)
            .ok_or(MockError::InvalidRegionIndex(region_index))
    }

    fn region_mut(&mut self, region_index: u32) -> Result<&mut [u8; REGION_SIZE], MockError> {
        let index =
            usize::try_from(region_index).map_err(|_| MockError::InvalidRegionIndex(region_index))?;
        self.regions
            .get_mut(index)
            .ok_or(MockError::InvalidRegionIndex(region_index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::StorageMetadata;

    #[test]
    fn metadata_operations_are_logged() {
        let mut flash = MockFlash::<64, 4, 8>::new(0xff);
        let metadata = StorageMetadata::new(64, 4, 1, 4, 0xff, 0xa5).unwrap();

        flash.write_metadata(metadata).unwrap();
        let read_back = flash.read_metadata().unwrap();

        assert_eq!(read_back, Some(metadata));
        assert_eq!(
            flash.operations(),
            &[MockOperation::WriteMetadata, MockOperation::ReadMetadata]
        );
    }

    #[test]
    fn erase_write_read_and_sync_are_logged() {
        let mut flash = MockFlash::<16, 2, 16>::new(0xff);

        flash.write_region(1, 4, b"bor").unwrap();
        let mut buffer = [0u8; 3];
        flash.read_region(1, 4, &mut buffer).unwrap();
        flash.erase_region(1).unwrap();
        flash.sync().unwrap();

        assert_eq!(&buffer, b"bor");
        assert_eq!(
            flash.operations(),
            &[
                MockOperation::WriteRegion {
                    region_index: 1,
                    offset: 4,
                    len: 3,
                },
                MockOperation::ReadRegion {
                    region_index: 1,
                    offset: 4,
                    len: 3,
                },
                MockOperation::EraseRegion { region_index: 1 },
                MockOperation::Sync,
            ]
        );
    }

    #[test]
    fn erase_restores_erased_bytes() {
        let mut flash = MockFlash::<8, 1, 8>::new(0xff);
        flash.write_region(0, 0, &[1, 2, 3, 4]).unwrap();
        flash.erase_region(0).unwrap();

        let mut buffer = [0u8; 8];
        flash.read_region(0, 0, &mut buffer).unwrap();
        assert_eq!(buffer, [0xff; 8]);
    }

    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-PRE-006 `region_count >= 2 + min_free_regions`.
    #[test]
    fn format_empty_store_rejects_too_few_regions() {
        let mut flash = MockFlash::<64, 2, 16>::new(0xff);
        let error = flash.format_empty_store(1, 8, 0xa5).unwrap_err();
        assert_eq!(
            error,
            MockFormatError::InsufficientRegions {
                region_count: 2,
                min_free_regions: 1,
            }
        );
    }

    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-003 Initialize region `0` as WAL:
    #[test]
    fn format_empty_store_initializes_region_zero_as_wal() {
        let mut flash = MockFlash::<64, 4, 32>::new(0xff);
        flash.format_empty_store(1, 8, 0xa5).unwrap();

        let header = Header::decode(&flash.region_bytes(0).unwrap()[..Header::ENCODED_LEN]).unwrap();
        assert_eq!(header.collection_id, CollectionId(0));
        assert_eq!(header.collection_format, WAL_V1_FORMAT);

        let start = Header::ENCODED_LEN;
        let end = start + WalRegionPrologue::ENCODED_LEN;
        let prologue =
            WalRegionPrologue::decode(&flash.region_bytes(0).unwrap()[start..end], 4).unwrap();
        assert_eq!(prologue.wal_head_region_index, 0);
    }

    //= spec/ring.md#format-storage-on-disk-initialization
    //# RING-FORMAT-STORAGE-POST-003 The free list MUST contain every non-WAL region in ascending region-index order.
    #[test]
    fn format_empty_store_populates_free_list_in_ascending_order() {
        let mut flash = MockFlash::<64, 4, 32>::new(0xff);
        let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();

        assert_eq!(flash.metadata(), Some(&metadata));

        let footer_offset = 64 - FreePointerFooter::ENCODED_LEN;
        let region1 =
            FreePointerFooter::decode(&flash.region_bytes(1).unwrap()[footer_offset..], 0xff).unwrap();
        let region2 =
            FreePointerFooter::decode(&flash.region_bytes(2).unwrap()[footer_offset..], 0xff).unwrap();
        let region3 =
            FreePointerFooter::decode(&flash.region_bytes(3).unwrap()[footer_offset..], 0xff).unwrap();

        assert_eq!(region1.next_tail, Some(2));
        assert_eq!(region2.next_tail, Some(3));
        assert_eq!(region3.next_tail, None);
    }
}
