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
    StorageFull,
    Backing(BackingError),
}

impl<BackingError, RegionAddress> From<BackingError> for IoError<BackingError, RegionAddress> {
    fn from(error: BackingError) -> Self {
        IoError::Backing(error)
    }
}

pub trait RegionAddress: Sized + Copy + Eq + PartialEq + Debug {
    fn zero() -> Self;
}

/// Represents the header of a region
pub(crate) trait RegionHeader<B: IoBackend> {
    fn sequence(&self) -> B::Sequence;
    fn collection_id(&self) -> CollectionId;
    fn collection_type(&self) -> CollectionType;
    fn collection_sequence(&self) -> B::Sequence;
    fn wal_address(&self) -> B::RegionAddress;
    fn free_list_head(&self) -> Option<B::RegionAddress>;
    fn free_list_tail(&self) -> Option<B::RegionAddress>;
    fn heads(&self) -> &[B::RegionAddress];
}

/// Represents the storage metadata for the database
pub(crate) trait StorageMeta {
    fn storage_version(&self) -> u32;
    fn region_size(&self) -> usize;
    fn region_count(&self) -> usize;
}

pub struct Io<'a, B: IoBackend> {
    storage_head: B::RegionAddress,
    storage_sequence: B::Sequence,
    free_list_head: Option<B::RegionAddress>,
    free_list_tail: Option<B::RegionAddress>,
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
        let collection_sequence = <B as IoBackend>::Sequence::first();
        backing.write_region_header(
            wal_address,
            sequence,
            collection_id,
            collection_type,
            collection_sequence,
            wal_address,
            Some(first_free_address),
            Some(last_free_address),
            &[],
        )?;

        Ok(Self {
            storage_head: wal_address,
            storage_sequence: sequence,
            free_list_head: Some(first_free_address),
            free_list_tail: Some(last_free_address),
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

        let wal_address = storage_head;
        let wal_offset = 0; // BOOG scan wall to work this out

        Ok(Self {
            storage_head,
            storage_sequence,
            free_list_head,
            free_list_tail,
            backing,
            wal_address,
            wal_offset,
        })
    }

    pub fn allocate_region(&mut self, collection_id: CollectionId) -> Result<B::RegionAddress, IoError<B::BackingError, B::RegionAddress>> {
        let Some(address) = self.free_list_head else {
            return Err(IoError::StorageFull);
        };
        let free_list_head = self.backing.get_region_free_pointer(address)?;
        self.free_list_head = free_list_head;
        Ok(address)
    }

    pub fn write_region_header(
        &mut self, 
        region: B::RegionAddress, 
        collection_id: CollectionId, 
        collection_type: CollectionType, 
        collection_sequence: B::Sequence
    ) -> Result<(), IoError<B::BackingError, B::RegionAddress>> {

        // Make the barrow checker happy
        let storage_sequence = self.storage_sequence.increment();
        self.storage_sequence = storage_sequence;

        self.backing.write_region_header(
            region, 
            storage_sequence, 
            collection_id, 
            collection_type, 
            collection_sequence, 
            self.wal_address, 
            self.free_list_head, 
            self.free_list_tail, 
            &[]
        )?;
        Ok(())
    }
}


pub trait RegionSequence: Sized + Eq + PartialEq + Ord + PartialOrd + Debug + Copy {
    fn first() -> Self;
    fn increment(&self) -> Self;
}

pub trait IoBackend: Sized + Debug {
    type StorageMeta<'a>: StorageMeta
    where
        Self: 'a;
    type RegionAddress: RegionAddress;
    type BackingError: Debug;
    type Sequence: RegionSequence;
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
        address: Self::RegionAddress,
        storage_sequence: Self::Sequence,
        collection_id: CollectionId,
        collection_type: CollectionType,
        collection_sequence: Self::Sequence,
        wal_address: Self::RegionAddress,
        free_list_head: Option<Self::RegionAddress>,
        free_list_tail: Option<Self::RegionAddress>,
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
