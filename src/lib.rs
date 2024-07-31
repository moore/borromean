#![no_std]

use core::mem::size_of;

use heapless::Vec;

use rkyv::{
    Archive,
    Serialize,
    Deserialize,
    ser::{
        Serializer, 
        serializers::{
            BufferSerializer,
            BufferSerializerError,
        },
    },
    AlignedBytes,
};

#[cfg(test)]
mod tests;

mod io;

pub struct Storage<const MAX_HEADS: usize> {
    meta: StorageMeta,
    sequence: u64,
    heads: Vec<Head, MAX_HEADS>,
    free_list_head: u64,
    free_list_tail: u64,
}


#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq),check_bytes,)]
#[archive_attr(derive(Debug))]
struct StorageMeta {
    format_version: u32,
    first_region_offset: u32,
    page_size: u32,
    erase_size: u32,
    region_size:u32,
    region_count:u64,
}

#[derive(Debug)]
pub enum StorageError {
    EraseNotPageAligned,
    RegionNotPageAligned,
    RegionAlignmentError,
    SerializerError(BufferSerializerError),
    ArithmeticOverflow,
    InternalError,
}

impl From<BufferSerializerError> for StorageError {
    fn from(value: BufferSerializerError) -> Self {
       StorageError::SerializerError(value) 
    }
}

impl StorageMeta {
    pub fn new(
        first_region_offset: u32,
        page_size: u32,
        erase_size: u32,
        region_size:u32,
        region_count:u64,
    ) -> Result<Self, StorageError> {

        if (erase_size != 1) && (erase_size % page_size != 0) {
            return Err(StorageError::EraseNotPageAligned);
        }

        if region_size % page_size != 0 {
            return Err(StorageError::RegionNotPageAligned)
        }

        if first_region_offset % erase_size != 0 {
            return Err(StorageError::RegionAlignmentError)
        }

        let format_version = 0;

        Ok(StorageMeta {
            format_version,
            first_region_offset,
            page_size,
            erase_size,
            region_size,
            region_count,
        })
    }

    pub fn write(&self, buffer: &mut [u8], offset: usize) -> Result<usize, StorageError> {

        let target = [0u8; size_of::<Self>()];

        let mut serializer = BufferSerializer::new(AlignedBytes(target));
        let pos = serializer.serialize_value(self)?;
        let buf = serializer.into_inner();
        // This sure is a lot of copying for a zero copy API!
        // BUG: is this an alignment thing?
        buffer[offset..(offset+buf.len())].copy_from_slice(buf.as_ref());

        Ok(pos)
       
    }
}


#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq),check_bytes,)]
//#[archive_attr(derive(Debug))]
struct Header<const MAX_HEADS: usize> {
    sequence: u64,
    collection_id: u32,
    heads: Vec<Head, MAX_HEADS>,
    free_list_head: u64,
    free_list_tail: u64,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq),check_bytes,)]
#[archive_attr(derive(Debug))]
struct Head {
    collection_id: u32,
    region: u64,
}


#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq),check_bytes,)]
#[archive_attr(derive(Debug))]
struct FreePointer {
    next: u64,
}