
extern crate alloc;
use alloc::{vec, vec::Vec};
use core::mem::size_of;

use rkyv::{
    archived_value,
};

use crate::{
    StorageError, 
    StorageMeta, 
    io::{
        Io,
        Region,
        IoError,
    }
};


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

        let region_size_usize: usize = safe_cast(region_size)?;
        let region_count_usize: usize = safe_cast(region_count)?;

        let size: usize = 
              (region_size_usize)
            * (region_count_usize) // not safe as it can over flow!
            + size_of::<StorageMeta>() 
            ;

        let Ok(erase_size_usize) = erase_size.try_into() else {
            return Err(StorageError::ArithmeticOverflow);
        };

        let first_region = round_up_to_next_multiple(size, erase_size_usize)?;
        let first_region_u32 = safe_cast(first_region)?;

        let mut data = vec![0; size];

        let storage_meta = StorageMeta::new(
            first_region_u32,
            page_size, 
            erase_size,
            region_size,
            region_count
        )?;

        let offset = storage_meta.write(&mut data, 0)?;

        if offset > first_region {
            return Err(StorageError::InternalError);
        }

        Ok(MemIo {
            data,
        })
    }

    
}


const fn round_up_to_next_multiple(i: usize, a: usize) -> Result<usize, StorageError> {
    if a <= 1 { 
        Ok(i) // Short 
    } else {
        // ((i + a - 1) / a) * a

        // a - 1 is safe as we checked that a != 0.
        let Some(next) = i.checked_add(a - 1) else {
            return Err(StorageError::ArithmeticOverflow);
        };
        
        // safe as a > 0;
        let count = next / a;

        // safe as result can not be larger then next.
        let result = a * count;

        Ok(result)
    }
}


fn safe_cast<V: TryInto<T>,T>(v: V) -> Result<T, StorageError> {
    let Ok(v_t): Result<T, _> = v.try_into() else {
        return Err(StorageError::ArithmeticOverflow);
    };

    Ok(v_t)
}

impl<const MAX_HEADS: usize> Io<MAX_HEADS> for MemIo {
    fn get_meta<'a>(&'a self) -> &'a StorageMeta {
        unimplemented!()
    }
    fn get_region<'a>(&'a self, index: u64) -> Result<Region<'a, MAX_HEADS>, IoError> {
        unimplemented!()
    }
}