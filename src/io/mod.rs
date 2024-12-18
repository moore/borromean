pub mod mem_io;
use crate::{CollectionId, CollectionType, Wal};
use core::{any::Any, fmt::Debug};

use serde::{Deserialize, Serialize};

use heapless::Vec;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum IoError<BackingError, RegionAddress> {
    Unreachable,
    AlreadyInitialized,
    NotInitialized,
    InvalidRegionSize,
    InvalidRegionCount,
    InvalidAddress(RegionAddress),
    InvalidHeads,
    OutOfBounds,
    StorageFull,
    Backing(BackingError),
    RegionNotFound(RegionAddress),
    SerializationError,
    BufferTooSmall(usize),
    RecordTooLarge(usize),
    AlreadyCommitted,
}

impl<BackingError, RegionAddress> From<BackingError> for IoError<BackingError, RegionAddress> {
    fn from(error: BackingError) -> Self {
        IoError::Backing(error)
    }
}

pub trait RegionAddress: Sized + Copy + Eq + PartialEq + Debug {
    fn zero() -> Self;
    fn postcard_max_len() -> usize;
}

const REGION_SEQUENCE_BYTES_LEN: usize = size_of::<u64>();
pub trait RegionSequence: Sized + Eq + PartialEq + Ord + PartialOrd + Debug + Copy {
    fn first() -> Self;
    // TODO: should return result as increment could overflow.
    fn increment(&self) -> Self;

    /// Return the sequence as a byte array.
    fn to_le_bytes(&self) -> [u8; REGION_SEQUENCE_BYTES_LEN];
}
/// Represents the header of a region
pub(crate) trait RegionHeader<B: IoBackend> {
    fn sequence(&self) -> B::StorageSequence;
    fn collection_id(&self) -> CollectionId;
    fn collection_type(&self) -> CollectionType;
    fn collection_sequence(&self) -> B::CollectionSequence;
    fn free_list_head(&self) -> Option<B::RegionAddress>;
    fn free_list_tail(&self) -> Option<B::RegionAddress>;
    fn heads(&self) -> &[(CollectionId, B::RegionAddress)];
}

/// Represents the storage metadata for the database
pub(crate) trait StorageMeta {
    fn storage_version(&self) -> u32;
    fn region_size(&self) -> usize;
    fn region_count(&self) -> usize;
}

pub struct Io<'a, B: IoBackend, const MAX_HEADS: usize> {
    storage_head: B::RegionAddress,
    storage_sequence: B::StorageSequence,
    free_list_head: Option<B::RegionAddress>,
    free_list_tail: Option<B::RegionAddress>,
    backing: &'a mut B,
    heads: Vec<(CollectionId, B::RegionAddress), MAX_HEADS>,
}

impl<'a, B: IoBackend, const MAX_HEADS: usize> Io<'a, B, MAX_HEADS> {
    pub fn init(
        backing: &'a mut B,
        region_size: usize,
        region_count: usize,
    ) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        if backing.is_initialized()? {
            return Err(IoError::AlreadyInitialized);
        }

        if region_count < 2 {
            return Err(IoError::InvalidRegionCount);
        }

        // Write the meta block.
        backing.write_meta(region_size, region_count)?;

        // Write the free list. Put every region but the first
        // one in the free list.
        let first_free_address = backing.get_region_address(0)?;

        let mut last_free_address = first_free_address;
        for i in 1..region_count {
            let address = backing.get_region_address(i)?;
            backing.write_region_free_pointer(last_free_address, address)?;
            last_free_address = address;
        }

        let wal_address = backing.get_region_address(0)?;

        let sequence = <B as IoBackend>::StorageSequence::first();
        let collection_id = CollectionId(0);

        let mut heads = Vec::new();

        // Should only error if heads is 0
        let _ = heads.push((collection_id, wal_address)) else {
            return Err(IoError::OutOfBounds);
        };

        let mut this = Self {
            storage_head: wal_address,
            storage_sequence: sequence,
            free_list_head: Some(first_free_address),
            free_list_tail: Some(last_free_address),
            backing,
            heads,
        };

        let wal = Wal::new(&mut this, collection_id)?;

        Ok(this)
    }

    pub fn open(backing: &'a mut B) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        if !backing.is_initialized()? {
            return Err(IoError::NotInitialized);
        }

        let mut storage_head = backing.get_region_address(0)?;
        let mut storage_sequence = backing.get_region_header(storage_head)?.sequence();
        let mut free_list_head = None;
        let mut free_list_tail = None;

        let region_count = backing.get_meta()?.region_count();
        for i in 1..region_count {
            let address = backing.get_region_address(i)?;
            let header = backing.get_region_header(address)?;
            let this_sequence = header.sequence();
            if this_sequence > storage_sequence {
                storage_head = address;
                storage_sequence = this_sequence;
                free_list_head = header.free_list_head();
                free_list_tail = header.free_list_tail();
            }
        }

        let mut heads = Vec::new();

        {
            // Give some love to the barrow checker
            let current_head = backing.get_region_header(storage_head)?;

            let head_list = current_head.heads();

            let Ok(_) = heads.extend_from_slice(head_list) else {
                return Err(IoError::Unreachable);
            };
        }

        let mut this = Self {
            storage_head,
            storage_sequence,
            free_list_head,
            free_list_tail,
            backing,
            heads,
        };

        // BOOG implement this!
        // let wall = Wall::open(&mut this, wall_address)?;
        // this.wal = Some(wal);

        Ok(this)
    }

    pub(crate) fn region_size(&self) -> usize {
        self.backing.get_region_size()
    }

    pub(crate) fn allocate_region(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<B::RegionAddress, IoError<B::BackingError, B::RegionAddress>> {
        let Some(address) = self.free_list_head else {
            return Err(IoError::StorageFull);
        };
        let free_list_head = self.backing.get_region_free_pointer(address)?;
        self.free_list_head = free_list_head;
        Ok(address)
    }

    pub(crate) fn write_region_header(
        &mut self,
        region: B::RegionAddress,
        collection_id: CollectionId,
        collection_type: CollectionType,
        collection_sequence: B::CollectionSequence,
    ) -> Result<(), IoError<B::BackingError, B::RegionAddress>> {
        // Make the barrow checker happy
        let storage_sequence = self.storage_sequence.increment();
        self.storage_sequence = storage_sequence;

        match self.heads.binary_search_by_key(&collection_id, |k| k.0) {
            Ok(index) => {
                if let Some(entry) = self.heads.get_mut(index) {
                    entry.1 = region;
                } else {
                    return Err(IoError::Unreachable);
                }
            }
            Err(index) => {
                self.heads.insert(index, (collection_id, region));
            }
        }

        self.backing.write_region_header(
            region,
            storage_sequence,
            collection_id,
            collection_type,
            collection_sequence,
            self.free_list_head,
            self.free_list_tail,
            self.heads.as_slice(),
        )?;
        Ok(())
    }

    pub(crate) fn write_region_data(
        &mut self,
        region: B::RegionAddress,
        data: &[u8],
        offset: usize,
    ) -> Result<(), IoError<B::BackingError, B::RegionAddress>> {
        self.backing.write_region_data(region, offset, data)
    }

    pub fn get_region_data(
        &mut self,
        region: B::RegionAddress,
        offset: usize,
        len: usize,
        buffer: &mut [u8],
    ) -> Result<(), IoError<B::BackingError, B::RegionAddress>> {
        self.backing.get_region_data(region, offset, len, buffer)
    }

    pub fn get_region_header<'b>(
        &'b mut self,
        region: B::RegionAddress,
    ) -> Result<B::RegionHeader<'b>, IoError<B::BackingError, B::RegionAddress>> {
        self.backing.get_region_header(region)
    }
}

pub trait IoBackend: Sized + Debug {
    type StorageMeta<'a>: StorageMeta
    where
        Self: 'a;
    type RegionAddress: RegionAddress + Serialize + for<'a> Deserialize<'a>;
    type BackingError: Debug;
    type StorageSequence: RegionSequence + Serialize + for<'a> Deserialize<'a>;
    type CollectionSequence: RegionSequence + Serialize + for<'a> Deserialize<'a>;
    type RegionHeader<'a>: RegionHeader<Self>
    where
        Self: 'a;

    /// Checks if the storage has a valid meta block.
    fn is_initialized(&mut self) -> Result<bool, IoError<Self::BackingError, Self::RegionAddress>>;

    /// Gets the storage meta block.    
    fn get_meta<'a>(
        &'a mut self,
    ) -> Result<Self::StorageMeta<'a>, IoError<Self::BackingError, Self::RegionAddress>>;

    /// Writes the storage meta block.
    fn write_meta(
        &mut self,
        region_size: usize,
        region_count: usize,
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>>;

    /// Get the RegionAddress for the given index.
    fn get_region_address(
        &mut self,
        index: usize,
    ) -> Result<Self::RegionAddress, IoError<Self::BackingError, Self::RegionAddress>>;

    /// Gets the size of the user writable portion of a region.
    fn get_region_size(&self) -> usize;

    /// Gets the region header.
    fn get_region_header<'a>(
        &'a mut self,
        index: Self::RegionAddress,
    ) -> Result<Self::RegionHeader<'a>, IoError<Self::BackingError, Self::RegionAddress>>;

    /// Writes the region header.
    fn write_region_header<'a>(
        &mut self,
        address: Self::RegionAddress,
        storage_sequence: Self::StorageSequence,
        collection_id: CollectionId,
        collection_type: CollectionType,
        collection_sequence: Self::CollectionSequence,
        free_list_head: Option<Self::RegionAddress>,
        free_list_tail: Option<Self::RegionAddress>,
        addresses: &[(CollectionId, Self::RegionAddress)],
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>>;

    /// Gets data from region at offset.
    fn get_region_data(
        &mut self,
        region: Self::RegionAddress,
        offset: usize,
        len: usize,
        buffer: &mut [u8],
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>>;

    /// Writes data to region at offset.
    fn write_region_data(
        &mut self,
        region: Self::RegionAddress,
        offset: usize,
        data: &[u8],
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>>;

    /// Gets the region free pointer.
    fn get_region_free_pointer(
        &mut self,
        region: Self::RegionAddress,
    ) -> Result<Option<Self::RegionAddress>, IoError<Self::BackingError, Self::RegionAddress>>;

    /// Writes the region free pointer.
    fn write_region_free_pointer(
        &mut self,
        region: Self::RegionAddress,
        pointer: Self::RegionAddress,
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>>;
}
