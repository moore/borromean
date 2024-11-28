pub mod mem_io;
use crate::{StorageMeta, RegionHeader};
use core::fmt::Debug;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum IoError<BackingError> {
    AlreadyInitialized,
    NotInitialized,
    InvalidAddress,
    OutOfBounds,
    Backing(BackingError),
}

impl<BackingError> From<BackingError> for IoError<BackingError> {
    fn from(error: BackingError) -> Self {
        IoError::Backing(error)
    }
}

pub struct Io<B: IoBackend> {
    backing: B,
}

impl<B: IoBackend> Io<B> {
    pub fn init(mut backing: B, region_size: usize, region_count: usize) -> Result<Self, IoError<B::BackingError>> {
        if backing.is_initialized()? {
            return Err(IoError::AlreadyInitialized);
        }

        backing.write_meta(region_size, region_count)?;
        Ok(Self { backing })
    }

    pub fn open(mut backing: B) -> Result<Self, IoError<B::BackingError>> {
        if !backing.is_initialized()? {
            return Err(IoError::NotInitialized);
        }

        Ok(Self { backing })
    }
}

pub trait IoBackend: Sized + Debug {
    type StorageMeta<'a>: StorageMeta where Self: 'a;
    type RegionAddress;
    type BackingError;
    type RegionHeader<'a>: RegionHeader where Self: 'a;
    
    /// Checks if the storage has a valid meta block.
    fn is_initialized(&mut self) -> Result<bool, IoError<Self::BackingError>>;

    /// Gets the storage meta block.    
    fn get_meta<'a>(&'a mut self) -> Result<Self::StorageMeta<'a>, IoError<Self::BackingError>>;

    /// Writes the storage meta block.
    fn write_meta(&mut self, region_size: usize, region_count: usize) -> Result<(), IoError<Self::BackingError>>;

    /// Gets the region header. 
    fn get_region_header<'a>(&'a mut self, index: Self::RegionAddress) -> Result<Self::RegionHeader<'a>, IoError<Self::BackingError>> ;

    /// Writes the region header.
    fn write_region_header<'a>(&mut self, index: Self::RegionAddress, header: Self::RegionHeader<'a>) -> Result<(), IoError<Self::BackingError>>;

    /// Gets data from region at offset.
    fn get_region_data<'a>(&'a mut self, index: Self::RegionAddress, offset: usize, len: usize) -> Result<&'a [u8], IoError<Self::BackingError>>;

    /// Writes data to region at offset.
    fn write_region_data(&mut self, index: Self::RegionAddress, offset: usize, data: &[u8]) -> Result<(), IoError<Self::BackingError>>;

    /// Gets the region free pointer.
    fn get_region_free_pointer(&mut self, index: Self::RegionAddress) -> Result<Option<Self::RegionAddress>, IoError<Self::BackingError>>;

    /// Writes the region free pointer.
    fn write_region_free_pointer(&mut self, index: Self::RegionAddress, pointer: Self::RegionAddress) -> Result<(), IoError<Self::BackingError>>;
}
