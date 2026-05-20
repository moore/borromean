extern crate std;

use super::assert_no_alloc;
use crate::{
    decode_record, encode_record_into, CollectionCreateMode, CollectionId, CollectionUpdateMode,
    DiskError, FlashIo, FreePointerFooter, Header, MapError, MapFrontier, MapStorageError,
    MapUpdate, MockFlash, MockFormatError, MockOperation, StartupCollectionBasis, StartupError,
    Storage, StorageFormatConfig, StorageFormatError, StorageIoError, StorageMetadata, StorageMode,
    StorageRuntimeError, StorageWorkspace, WalRecord, WalRegionPrologue, MAP_REGION_V1_FORMAT,
    WAL_V1_FORMAT,
};

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
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, StorageIoError> {
        self.inner.read_metadata().map_err(StorageIoError::from)
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), StorageIoError> {
        self.inner
            .write_metadata(metadata)
            .map_err(StorageIoError::from)
    }

    fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), StorageIoError> {
        self.inner
            .read_region(region_index, offset, buffer)
            .map_err(StorageIoError::from)
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageIoError> {
        self.inner
            .write_region(region_index, offset, data)
            .map_err(StorageIoError::from)
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), StorageIoError> {
        self.inner
            .erase_region(region_index)
            .map_err(StorageIoError::from)
    }

    fn sync(&mut self) -> Result<(), StorageIoError> {
        self.inner.sync().map_err(StorageIoError::from)
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, StorageFormatError> {
        self.inner
            .format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)
            .map_err(StorageFormatError::from)
    }
}
mod api;
mod arch;
mod arithmetic;
mod collection;
mod core;
mod exec;
mod io;
mod memory;
mod operation;
mod panic;
mod startup;
