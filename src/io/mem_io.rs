use heapless::Vec;
use serde::{Deserialize, Serialize};

use crate::{
    io::{IoBackend, IoError, RegionAddress},
    CollectionId, CollectionType, RegionHeader, RegionSequence, StorageError, StorageMeta,
};

use super::REGION_SEQUENCE_BYTES_LEN;

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

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct MemRegionAddress(pub(crate) usize);

impl RegionAddress for MemRegionAddress {
    fn zero() -> Self {
        MemRegionAddress(0)
    }
}

type SequenceLen = u64;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, PartialOrd, Ord)]
pub struct MemCollectionSequence(SequenceLen);

impl RegionSequence for MemCollectionSequence {
    fn first() -> Self {
        MemCollectionSequence(0)
    }

    fn increment(&self) -> Self {
        MemCollectionSequence(self.0 + 1)
    }

    fn to_le_bytes(&self) -> [u8; REGION_SEQUENCE_BYTES_LEN] {
        self.0.to_be_bytes()
    }
}


#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, PartialOrd, Ord)]
pub struct MemStorageSequence(pub(crate) SequenceLen);

impl RegionSequence for MemStorageSequence {
    fn first() -> Self {
        MemStorageSequence(0)
    }

    fn increment(&self) -> Self {
        MemStorageSequence(self.0 + 1)
    }

    fn to_le_bytes(&self) -> [u8; REGION_SEQUENCE_BYTES_LEN] {
        self.0.to_be_bytes()
    }
}

#[derive(Debug, Clone)]
pub struct MemRegionHeader<const MAX_HEADS: usize> {
    pub(crate) sequence: MemStorageSequence,
    pub(crate) collection_id: CollectionId,
    pub(crate) collection_type: CollectionType,
    pub(crate) collection_sequence: MemCollectionSequence,
    pub(crate) wal_address: MemRegionAddress,
    pub(crate) free_list_head: Option<MemRegionAddress>,
    pub(crate) free_list_tail: Option<MemRegionAddress>,
    pub(crate) heads: Vec<MemRegionAddress, MAX_HEADS>,
}

impl<'a, const DATA_SIZE: usize, const MAX_HEADS: usize, const REGION_COUNT: usize>
    RegionHeader<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT>> for &'a MemRegionHeader<MAX_HEADS>
{
    fn sequence(&self) -> MemStorageSequence {
        self.sequence
    }
    fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    fn collection_type(&self) -> CollectionType {
        self.collection_type
    }
    fn collection_sequence(&self) -> MemCollectionSequence {
        self.collection_sequence
    }
    fn wal_address(
        &self,
    ) -> <MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::RegionAddress {
        self.wal_address
    }
    fn free_list_head(
        &self,
    ) -> Option<<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::RegionAddress> {
        self.free_list_head
    }
    fn free_list_tail(
        &self,
    ) -> Option<<MemIo<DATA_SIZE, MAX_HEADS, REGION_COUNT> as IoBackend>::RegionAddress> {
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
                sequence: MemStorageSequence::first(),
                collection_id: CollectionId(0),
                collection_type: CollectionType::Uninitialized,
                collection_sequence: MemCollectionSequence::first(),
                wal_address: MemRegionAddress::zero(),
                free_list_head: None,
                free_list_tail: None,
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
    type StorageSequence = MemStorageSequence;
    type CollectionSequence = MemCollectionSequence;
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
        Ok(MemRegionAddress(index))
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
            .get(index.0)
            .ok_or(IoError::InvalidAddress(index))
            .map(|region| &region.header)
    }

    fn write_region_header<'a>(
        &mut self,
        index: Self::RegionAddress,
        sequence: Self::StorageSequence,
        collection_id: CollectionId,
        collection_type: CollectionType,
        collection_sequence: Self::CollectionSequence,
        wal_address: Self::RegionAddress,
        free_list_head: Option<Self::RegionAddress>,
        free_list_tail: Option<Self::RegionAddress>,
        addresses: &[Self::RegionAddress],
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>> {
        let region = self
            .regions
            .get_mut(index.0)
            .ok_or(IoError::InvalidAddress(index))?;

        let heads = Vec::from_slice(addresses).map_err(|_| IoError::InvalidHeads)?;

        region.header = MemRegionHeader {
            sequence,
            collection_id,
            collection_type,
            collection_sequence,
            wal_address,
            free_list_head,
            free_list_tail,
            heads,
        };
        Ok(())
    }

    fn get_region_data<'a>(
        &mut self,
        index: Self::RegionAddress,
        offset: usize,
        len: usize,
        buffer: &'a mut [u8],
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>> {
        if offset + len > DATA_SIZE {
            return Err(IoError::OutOfBounds);
        }

        if buffer.len() < len {
            return Err(IoError::OutOfBounds);
        }

        let source = self
            .regions
            .get(index.0)
            .ok_or(IoError::InvalidAddress(index))
            .map(|region| &region.data[offset..offset + len])?;

        let Some((target, _)) = buffer.split_at_mut_checked(source.len()) else {
            return Err(IoError::BufferTooSmall(source.len()));
        };

        target.copy_from_slice(source);

        Ok(())
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
            .get_mut(index.0)
            .ok_or(IoError::InvalidAddress(index))?;

        region.data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn get_region_free_pointer(
        &mut self,
        index: Self::RegionAddress,
    ) -> Result<Option<Self::RegionAddress>, IoError<Self::BackingError, Self::RegionAddress>> {
        self.regions
            .get(index.0)
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
            .get_mut(index.0)
            .ok_or(IoError::InvalidAddress(index))?;

        region.free_pointer = Some(pointer);
        Ok(())
    }
}
