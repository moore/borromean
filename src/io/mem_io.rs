extern crate alloc;
use heapless::Vec;

use crate::{
    StorageError,
    StorageMeta,
    RegionHeader,
    CollectionId,
    CollectionType,
    io::IoBackend,
    io::IoError,
};

#[derive(Debug, Clone)]
pub enum MemIoError {
    InvalidAddress,
}

#[derive(Debug)]
pub struct MemStorageMeta {
    region_size: usize,
    region_count: usize,
}

impl MemStorageMeta {
    pub fn new(region_size: usize, region_count: usize) -> Self {
        Self { region_size, region_count }
    }
}

impl<'a> StorageMeta for &'a MemStorageMeta {
    fn storage_version(&self) -> u32 { 0 }
    fn region_count(&self) -> usize { self.region_count }
    fn region_size(&self) -> usize { self.region_size }
}

type RegionAddress = usize;
type Sequence = u64;

#[derive(Debug, Clone)]
pub struct MemRegionHeader<const MAX_HEADS: usize> {
    sequence: Sequence,
    collection_id: CollectionId,
    collection_type: CollectionType,
    free_list_head: RegionAddress,
    free_list_tail: RegionAddress,
    heads: Vec<RegionAddress, MAX_HEADS>,
}

impl<'a, const MAX_HEADS: usize> RegionHeader for &'a MemRegionHeader<MAX_HEADS> {
    type RegionAddress = RegionAddress;
    type Sequence = Sequence;
    
    fn sequence(&self) -> Sequence { self.sequence }
    fn collection_id(&self) -> CollectionId { self.collection_id }
    fn collection_type(&self) -> CollectionType { self.collection_type }
    fn free_list_head(&self) -> RegionAddress { self.free_list_head }
    fn free_list_tail(&self) -> RegionAddress { self.free_list_tail }
    fn heads(&self) -> &[RegionAddress] { &self.heads }
}

#[derive(Debug, Clone)]
pub struct MemFreePointer(u32);

#[derive(Debug, Clone)]
pub struct MemRegion<const DATA_SIZE: usize, const MAX_HEADS: usize> {
    header: MemRegionHeader<MAX_HEADS>,
    data: Vec<u8, DATA_SIZE>,
    free_pointer: Option<RegionAddress>,
}

#[derive(Debug)]
pub struct MemIo<
    const DATA_SIZE: usize, 
    const MAX_HEADS: usize,
    const REGION_COUNT: usize,
> {
    meta: MemStorageMeta,
    regions: Vec<MemRegion<DATA_SIZE, MAX_HEADS>, REGION_COUNT>,
}

impl<
    const DATA_SIZE: usize, 
    const MAX_HEADS: usize, 
    const REGION_COUNT: usize
> MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> {
    pub fn new() -> Result<Self, StorageError<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>> {
        let meta = MemStorageMeta::new(DATA_SIZE, REGION_COUNT);
        let regions = Vec::new();

        Ok(Self { meta, regions })
    }
}

impl<
    const DATA_SIZE: usize, 
    const MAX_HEADS: usize, 
    const REGION_COUNT: usize
> IoBackend for MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> {
    type StorageMeta<'a> = &'a MemStorageMeta where Self: 'a;
    type RegionAddress = RegionAddress;
    type BackingError = MemIoError;
    type RegionHeader<'a> = &'a MemRegionHeader<MAX_HEADS> where Self: 'a;
    
    fn is_initialized(&mut self) -> Result<bool, IoError<Self::BackingError>> {
        Ok(!self.regions.is_empty())
    }

    fn write_meta(&mut self, region_size: usize, region_count: usize) -> Result<(), IoError<Self::BackingError>> {
        self.meta = MemStorageMeta::new(region_size, region_count);
        Ok(())
    }

    fn get_meta<'a>(&'a mut self) -> Result<Self::StorageMeta<'a>, IoError<Self::BackingError>> {
        Ok(&self.meta)
    }

    fn get_region_header<'a>(&'a mut self, index: Self::RegionAddress) -> Result<Self::RegionHeader<'a>, IoError<Self::BackingError>> {
        Ok(&self.regions[index].header)
    } 

    fn write_region_header<'a>(&mut self, index: Self::RegionAddress, header: Self::RegionHeader<'a>) -> Result<(), IoError<Self::BackingError>> {
        self.regions[index].header = header.clone();
        Ok(())
    }

    fn get_region_data<'a>(&'a mut self, index: Self::RegionAddress, offset: usize, len: usize) -> Result<&'a [u8], IoError<Self::BackingError>> {

        if offset + len > DATA_SIZE {
            return Err(IoError::OutOfBounds);
        }

        Ok(&self.regions[index].data[offset..offset + len])
    } 

    fn write_region_data(&mut self, index: Self::RegionAddress, offset: usize, data: &[u8]) -> Result<(), IoError<Self::BackingError>> {
        if offset + data.len() > DATA_SIZE {
            return Err(IoError::OutOfBounds);
        }

        self.regions[index].data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }   

    fn get_region_free_pointer(&mut self, index: Self::RegionAddress) -> Result<Option<Self::RegionAddress>, IoError<Self::BackingError>> {
        Ok(self.regions[index].free_pointer.clone())
    }   

    fn write_region_free_pointer(&mut self, index: Self::RegionAddress, pointer: Self::RegionAddress) -> Result<(), IoError<Self::BackingError>> {
        self.regions[index].free_pointer = Some(pointer);
        Ok(())
    }       
}