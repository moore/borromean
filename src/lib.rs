#![no_std]

#[cfg(test)]
mod tests;

mod io;
pub use io::*;

use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub enum StorageError<B: IoBackend> {
    ArithmeticOverflow,
    NoFreeRegions,
    AlreadyInitialized,
    NotInitialized,
    InvalidAddress,
    OutOfBounds,
    BackingError(B::BackingError),
}

impl<B: IoBackend> From<IoError<B::BackingError>> for StorageError<B> {
    fn from(error: IoError<B::BackingError>) -> Self {
        match error {
            IoError::AlreadyInitialized => StorageError::AlreadyInitialized,
            IoError::NotInitialized => StorageError::NotInitialized,
            IoError::InvalidAddress => StorageError::InvalidAddress,
            IoError::OutOfBounds => StorageError::OutOfBounds,
            IoError::Backing(e ) => StorageError::BackingError(e),
        }
    }
}

/// Newtype for collection identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionId(pub(crate) u16);

/// Represents different types of collections that can be stored
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionType {
    Free,   // Used for free regions
    Wal,    // Write-ahead log
    Channel, // FIFO queue
    Map,    // Key-value store
}

/// Represents the header of a region
pub(crate) trait RegionHeader {
    type RegionAddress;
    type Sequence;
    fn sequence(&self) -> Self::Sequence;
    fn collection_id(&self) -> CollectionId;
    fn collection_type(&self) -> CollectionType;
    fn free_list_head(&self) -> Self::RegionAddress;
    fn free_list_tail(&self) -> Self::RegionAddress;
    fn heads(&self) -> &[Self::RegionAddress];
}

/// Represents the storage metadata for the database
pub(crate) trait StorageMeta {
    fn storage_version(&self) -> u32;
    fn region_size(&self) -> usize;
    fn region_count(&self) -> usize;
}

pub trait Collection {
    fn id(&self) -> CollectionId;
    fn collection_type(&self) -> CollectionType;
}

pub struct Storage<'a, B: IoBackend> {
    io: Io<'a, B>
}

impl<'a, B> Storage<'a, B> 
    where 
    B: IoBackend,
    StorageError<B>: From<IoError<<B as IoBackend>::BackingError>>
    {
    pub fn init(backing: &'a mut B, region_size: usize, region_count: usize) -> Result<Self, StorageError<B>> {
        
        if backing.is_initialized()? {
            return Err(StorageError::AlreadyInitialized);
        }

        let io = Io::init(backing, region_size, region_count)?;
        Ok(Self { io })
    }

    pub fn open(backing: &'a mut B) -> Result<Self, StorageError<B>> {
        if !backing.is_initialized()? {
            return Err(StorageError::NotInitialized);
        }


        let io = Io::open(backing)?;
        
      Ok(Self { io })
    }
    pub fn get_collection_mut<'b, C: Collection>(&'b mut self, id: CollectionId) 
        -> Result<&'b mut C, StorageError<B>> {
            unimplemented!()

        }
    pub fn get_collection<'b, C: Collection>(&'b self, id: CollectionId) 
        -> Result<&'b C, StorageError<B>> {
            unimplemented!()
        }
}
