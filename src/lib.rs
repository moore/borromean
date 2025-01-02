#![no_std]

#[cfg(test)]
mod tests;

mod io;
pub use io::*;

mod collections;
pub use collections::*;

pub mod vec_like;
pub use vec_like::*;

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum StorageError<B: IoBackend> {
    Unreachable,
    RecordTooLarge(usize),
    ArithmeticOverflow, // TODO: Remove
    NoFreeRegions,
    AlreadyInitialized,
    NotInitialized,
    InvalidAddress(B::RegionAddress),
    InvalidHeads,
    OutOfBounds,
    InvalidRegionSize,
    InvalidRegionCount,
    StorageFull,
    BackingError(B::BackingError),
    SerializationError,
}

impl<B: IoBackend> From<IoError<B::BackingError, B::RegionAddress>> for StorageError<B> {
    fn from(error: IoError<B::BackingError, B::RegionAddress>) -> Self {
        match error {
            IoError::AlreadyCommitted => StorageError::OutOfBounds,
            IoError::Unreachable => StorageError::Unreachable,
            IoError::RecordTooLarge(len) => StorageError::RecordTooLarge(len),
            IoError::StorageFull => StorageError::StorageFull,
            IoError::AlreadyInitialized => StorageError::AlreadyInitialized,
            IoError::NotInitialized => StorageError::NotInitialized,
            IoError::InvalidAddress(address) => StorageError::InvalidAddress(address),
            IoError::OutOfBounds => StorageError::OutOfBounds,
            IoError::InvalidRegionSize => StorageError::InvalidRegionSize,
            IoError::InvalidRegionCount => StorageError::InvalidRegionCount,
            IoError::InvalidHeads => StorageError::InvalidHeads,
            IoError::Backing(e) => StorageError::BackingError(e),
            IoError::RegionNotFound(address) => StorageError::InvalidAddress(address),
            IoError::SerializationError => StorageError::SerializationError,
            IoError::BufferTooSmall(_) => StorageError::OutOfBounds,
        }
    }
}

type CollectionIdCounter = u16;

/// Newtype for collection identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct CollectionId(pub(crate) CollectionIdCounter);

impl CollectionId {
    pub fn to_le_bytes(&self) -> [u8; size_of::<CollectionIdCounter>()] {
        self.0.to_be_bytes()
    }

    pub fn increment(&self) -> Option<Self> {
        let Some(next) = self.0.checked_add(1) else {
            return None;
        };

        Some(Self(next))
    }
}

/// Represents different types of collections that can be stored
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionType {
    Uninitialized,
    Free,    // Used for free regions
    Wal,     // Write-ahead log
    Channel, // FIFO queue
    Map,     // Key-value store
}

pub trait Collection {
    fn id(&self) -> CollectionId;
    fn collection_type(&self) -> CollectionType;
}

pub struct Storage<'a, B: IoBackend, const MAX_HEADS: usize> {
    io: Io<'a, B, MAX_HEADS>,
}

impl<'a, B, const MAX_HEADS: usize> Storage<'a, B, MAX_HEADS>
where
    B: IoBackend,
    StorageError<B>: From<IoError<B::BackingError, B::RegionAddress>>,
{
    pub fn init(
        backing: &'a mut B,
        region_size: usize,
        region_count: usize,
    ) -> Result<Self, StorageError<B>> {
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

    pub fn new_collection(
        &mut self,
        collection_type: CollectionType,
    ) -> Result<CollectionId, StorageError<B>> {
        unimplemented!()
    }

    pub fn get_collection_mut<'b, C: Collection>(
        &'b mut self,
        id: CollectionId,
    ) -> Result<&'b mut C, StorageError<B>> {
        unimplemented!()
    }

    pub fn get_collection<'b, C: Collection>(
        &'b self,
        id: CollectionId,
    ) -> Result<&'b C, StorageError<B>> {
        unimplemented!()
    }
}
