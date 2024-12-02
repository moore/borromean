extern crate alloc;
use heapless::Vec;

use crate::{
    io::{IoBackend, IoError, RegionAddress},
    CollectionId, CollectionType, FirstSequence, RegionHeader, StorageError, StorageMeta,
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
        Self {
            region_size,
            region_count,
        }
    }
}

impl<'a> StorageMeta for &'a MemStorageMeta {
    fn storage_version(&self) -> u32 {
        0
    }
    fn region_count(&self) -> usize {
        self.region_count
    }
    fn region_size(&self) -> usize {
        self.region_size
    }
}

pub(crate) type MemRegionAddress = usize;

impl RegionAddress for MemRegionAddress {
    fn zero() -> Self {
        0
    }
}

type Sequence = u64;

impl FirstSequence for Sequence {
    fn first() -> Self {
        0
    }
}

#[derive(Debug, Clone)]
pub struct MemRegionHeader<const MAX_HEADS: usize> {
    sequence: Sequence,
    collection_id: CollectionId,
    collection_type: CollectionType,
    wal_address: MemRegionAddress,
    free_list_head: MemRegionAddress,
    free_list_tail: MemRegionAddress,
    heads: Vec<MemRegionAddress, MAX_HEADS>,
}

impl<'a, const DATA_SIZE: usize, const MAX_HEADS: usize, const REGION_COUNT: usize>
    RegionHeader<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>> for &'a MemRegionHeader<MAX_HEADS>
{
    fn sequence(&self) -> Sequence {
        self.sequence
    }
    fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    fn collection_type(&self) -> CollectionType {
        self.collection_type
    }
    fn wal_address(
        &self,
    ) -> <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::RegionAddress {
        self.wal_address
    }
    fn free_list_head(
        &self,
    ) -> <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::RegionAddress {
        self.free_list_head
    }
    fn free_list_tail(
        &self,
    ) -> <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::RegionAddress {
        self.free_list_tail
    }
    fn heads(&self) -> &[<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::RegionAddress] {
        &self.heads
    }
}

#[derive(Debug, Clone)]
pub struct MemFreePointer(u32);

#[derive(Debug, Clone)]
pub struct MemRegion<const DATA_SIZE: usize, const MAX_HEADS: usize> {
    header: MemRegionHeader<MAX_HEADS>,
    data: [u8; DATA_SIZE],
    free_pointer: Option<MemRegionAddress>,
}

#[derive(Debug)]
pub struct MemIo<const DATA_SIZE: usize, const MAX_HEADS: usize, const REGION_COUNT: usize> {
    meta: MemStorageMeta,
    regions: [MemRegion<DATA_SIZE, MAX_HEADS>; REGION_COUNT],
}

impl<const DATA_SIZE: usize, const MAX_HEADS: usize, const REGION_COUNT: usize>
    MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>
{
    pub fn new() -> Result<Self, StorageError<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>>> {
        let meta = MemStorageMeta::new(DATA_SIZE, REGION_COUNT);
        let regions = core::array::from_fn(|_| MemRegion {
            header: MemRegionHeader {
                sequence: Sequence::first(),
                collection_id: CollectionId(0),
                collection_type: CollectionType::Uninitialized,
                wal_address: 0,
                free_list_head: 0,
                free_list_tail: 0,
                heads: Vec::new(),
            },
            data: [0u8; DATA_SIZE],
            free_pointer: None,
        });

        Ok(Self { meta, regions })
    }
}

impl<const DATA_SIZE: usize, const MAX_HEADS: usize, const REGION_COUNT: usize> IoBackend
    for MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>
{
    type StorageMeta<'a> = &'a MemStorageMeta where Self: 'a;
    type Sequence = Sequence;
    type RegionAddress = MemRegionAddress;
    type BackingError = MemIoError;
    type RegionHeader<'a> = &'a MemRegionHeader<MAX_HEADS> where Self: 'a;

    fn is_initialized(&mut self) -> Result<bool, IoError<Self::BackingError, Self::RegionAddress>> {
        Ok(self.regions[0].header.collection_type != CollectionType::Uninitialized)
    }

    fn write_meta(
        &mut self,
        region_size: usize,
        region_count: usize,
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>> {
        if region_size != DATA_SIZE {
            return Err(IoError::InvalidRegionSize);
        }

        if region_count != REGION_COUNT {
            return Err(IoError::InvalidRegionCount);
        }

        self.meta = MemStorageMeta::new(region_size, region_count);
        Ok(())
    }

    fn get_region_address(
        &mut self,
        index: usize,
    ) -> Result<Self::RegionAddress, IoError<Self::BackingError, Self::RegionAddress>> {
        Ok(index)
    }

    fn get_meta<'a>(
        &'a mut self,
    ) -> Result<Self::StorageMeta<'a>, IoError<Self::BackingError, Self::RegionAddress>> {
        Ok(&self.meta)
    }

    fn get_region_header<'a>(
        &'a mut self,
        index: Self::RegionAddress,
    ) -> Result<Self::RegionHeader<'a>, IoError<Self::BackingError, Self::RegionAddress>> {
        self.regions
            .get(index)
            .ok_or(IoError::InvalidAddress(index))
            .map(|region| &region.header)
    }

    fn write_region_header<'a>(
        &mut self,
        index: Self::RegionAddress,
        sequence: Self::Sequence,
        collection_id: CollectionId,
        collection_type: CollectionType,
        wal_address: Self::RegionAddress,
        free_list_head: Self::RegionAddress,
        free_list_tail: Self::RegionAddress,
        addresses: &[Self::RegionAddress],
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>> {
        let region = self
            .regions
            .get_mut(index)
            .ok_or(IoError::InvalidAddress(index))?;

        let heads = Vec::from_slice(addresses).map_err(|_| IoError::InvalidHeads)?;

        region.header = MemRegionHeader {
            sequence,
            collection_id,
            collection_type,
            wal_address,
            free_list_head,
            free_list_tail,
            heads,
        };
        Ok(())
    }

    fn get_region_data<'a>(
        &'a mut self,
        index: Self::RegionAddress,
        offset: usize,
        len: usize,
    ) -> Result<&'a [u8], IoError<Self::BackingError, Self::RegionAddress>> {
        if offset + len > DATA_SIZE {
            return Err(IoError::OutOfBounds);
        }

        self.regions
            .get(index)
            .ok_or(IoError::InvalidAddress(index))
            .map(|region| &region.data[offset..offset + len])
    }

    fn write_region_data(
        &mut self,
        index: Self::RegionAddress,
        offset: usize,
        data: &[u8],
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>> {
        if offset + data.len() > DATA_SIZE {
            return Err(IoError::OutOfBounds);
        }

        let region = self
            .regions
            .get_mut(index)
            .ok_or(IoError::InvalidAddress(index))?;

        region.data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn get_region_free_pointer(
        &mut self,
        index: Self::RegionAddress,
    ) -> Result<Option<Self::RegionAddress>, IoError<Self::BackingError, Self::RegionAddress>> {
        self.regions
            .get(index)
            .ok_or(IoError::InvalidAddress(index))
            .map(|region| region.free_pointer)
    }

    fn write_region_free_pointer(
        &mut self,
        index: Self::RegionAddress,
        pointer: Self::RegionAddress,
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>> {
        let region = self
            .regions
            .get_mut(index)
            .ok_or(IoError::InvalidAddress(index))?;

        region.free_pointer = Some(pointer);
        Ok(())
    }
}
