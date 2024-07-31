use crate::{Header, FreePointer, StorageMeta};

pub mod mem_io;

#[cfg(test)]
mod tests;

pub struct Region<'a, const MAX_HEADS: usize> {
    pub index: u64,
    pub header: &'a Header<MAX_HEADS>,
    pub data: &'a [u8],
    pub free_pointer: &'a FreePointer,
}

pub enum IoError {

}

pub trait Io<const MAX_HEADS: usize> {    
    fn get_meta<'a>(&'a self) -> &'a StorageMeta;
    fn get_region<'a>(&'a self, index: u64) -> Result<Region<'a, MAX_HEADS>, IoError>;
}

