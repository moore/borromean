pub mod mem_io;
use crate::{CollectionId, CollectionType};
use core::fmt::Debug;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum IoError<BackingError, RegionAddress> {
    AlreadyInitialized,
    NotInitialized,
    InvalidRegionSize,
    InvalidRegionCount,
    InvalidAddress(RegionAddress),
    InvalidHeads,
    OutOfBounds,
    Backing(BackingError),
}

impl<BackingError, RegionAddress> From<BackingError> for IoError<BackingError, RegionAddress> {
    fn from(error: BackingError) -> Self {
        IoError::Backing(error)
    }
}

/// Represents the header of a region
pub(crate) trait RegionHeader<B: IoBackend> {
    fn sequence(&self) -> B::Sequence;
    fn collection_id(&self) -> CollectionId;
    fn collection_type(&self) -> CollectionType;
    fn wal_address(&self) -> B::RegionAddress;
    fn free_list_head(&self) -> B::RegionAddress;
    fn free_list_tail(&self) -> B::RegionAddress;
    fn heads(&self) -> &[B::RegionAddress];
}

/// Represents the storage metadata for the database
pub(crate) trait StorageMeta {
    fn storage_version(&self) -> u32;
    fn region_size(&self) -> usize;
    fn region_count(&self) -> usize;
}

pub struct Io<'a, B: IoBackend> {
    backing: &'a mut B,
    wal_address: B::RegionAddress,
    wal_offset: usize,
}

impl<'a, B: IoBackend> Io<'a, B> {
    pub fn init(
        backing: &'a mut B,
        region_size: usize,
        region_count: usize,
    ) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        if backing.is_initialized()? {
            return Err(IoError::AlreadyInitialized);
        }

        // Write the meta block.
        backing.write_meta(region_size, region_count)?;

        // Write the free list. Put every region but the first
        // one in the free list.
        let first_free_address = backing.get_region_address(1)?;

        let mut last_free_address = first_free_address;
        for i in 2..region_count {
            let address = backing.get_region_address(i)?;
            backing.write_region_free_pointer(last_free_address, address)?;
            last_free_address = address;
        }

        let wal_address = backing.get_region_address(0)?;

        let sequence = <B as IoBackend>::Sequence::first();
        let collection_id = CollectionId(0);
        let collection_type = CollectionType::Wal;

        backing.write_region_header(
            wal_address,
            sequence,
            collection_id,
            collection_type,
            wal_address,
            first_free_address,
            last_free_address,
            &[],
        )?;

        Ok(Self {
            backing,
            wal_address,
            wal_offset: 0,
        })
    }

    pub fn open(backing: &'a mut B) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        if !backing.is_initialized()? {
            return Err(IoError::NotInitialized);
        }

        let mut storage_head = backing.get_region_address(0)?;
        let mut storage_sequence = backing.get_region_header(storage_head)?.sequence();

        let region_count = backing.get_meta()?.region_count();
        for i in 1..region_count {
            let address = backing.get_region_address(i)?;
            let header = backing.get_region_header(address)?;
            let this_sequence = header.sequence();
            if this_sequence > storage_sequence {
                storage_head = address;
                storage_sequence = this_sequence;
            }
        }

        let wal_address = storage_head;
        let wal_offset = 0; // BOOG scan wall to work this out

        Ok(Self {
            backing,
            wal_address,
            wal_offset,
        })
    }
}

pub trait FirstSequence {
    fn first() -> Self;
}

pub trait IoBackend: Sized + Debug {
    type StorageMeta<'a>: StorageMeta
    where
        Self: 'a;
    type RegionAddress: Copy + Eq + PartialEq + Debug;
    type BackingError: Debug;
    type Sequence: FirstSequence + Eq + PartialEq + Ord + PartialOrd + Debug;
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

    /// Gets the region header.
    fn get_region_header<'a>(
        &'a mut self,
        index: Self::RegionAddress,
    ) -> Result<Self::RegionHeader<'a>, IoError<Self::BackingError, Self::RegionAddress>>;

    /// Writes the region header.
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
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>>;

    /// Gets data from region at offset.
    fn get_region_data<'a>(
        &'a mut self,
        index: Self::RegionAddress,
        offset: usize,
        len: usize,
    ) -> Result<&'a [u8], IoError<Self::BackingError, Self::RegionAddress>>;

    /// Writes data to region at offset.
    fn write_region_data(
        &mut self,
        index: Self::RegionAddress,
        offset: usize,
        data: &[u8],
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>>;

    /// Gets the region free pointer.
    fn get_region_free_pointer(
        &mut self,
        index: Self::RegionAddress,
    ) -> Result<Option<Self::RegionAddress>, IoError<Self::BackingError, Self::RegionAddress>>;

    /// Writes the region free pointer.
    fn write_region_free_pointer(
        &mut self,
        index: Self::RegionAddress,
        pointer: Self::RegionAddress,
    ) -> Result<(), IoError<Self::BackingError, Self::RegionAddress>>;
}
