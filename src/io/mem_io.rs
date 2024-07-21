
extern crate alloc;
use alloc::{vec, vec::Vec};

use crate::{FreePointer, Header, StorageError, StorageMeta};
use core::mem::size_of;

pub struct MemIo {
    data: Vec<u8>,
}

impl MemIo {
    pub fn new(
        page_size: u32, 
        erase_size: u32, 
        region_size: u32, 
        region_count: u64
    ) -> Result<MemIo, StorageError> {
        let size: usize = 
              (region_size as usize)
            * (region_count as usize) // not safe as it can over flow!
            + size_of::<StorageMeta>() 
            ;

        let mut data = vec![0; size];

        let storage_meta = StorageMeta::new(
            page_size, 
            erase_size,
            region_size,
            region_count
        )?;


        storage_meta.write(&mut data, 0)?;

        Ok(MemIo {
            data,
        })
    }
}