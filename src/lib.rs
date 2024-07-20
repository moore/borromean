use rkyv::{
    Archive,
    Serialize,
    Deserialize,
};

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq),check_bytes,)]
#[archive_attr(derive(Debug))]
struct StorageMeta {
    format_version: u32,
    page_size: u32,
    erase_size: u32,
    region_size:u32,
    region_count:u64,
}

#[derive(Debug)]
pub enum StorageError {
    EraseNotPageAligned,
    RegionNotPageAligned,
}

impl StorageMeta {
    pub fn new(
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

        let format_version = 0;

        Ok(StorageMeta {
            format_version,
            page_size,
            erase_size,
            region_size,
            region_count,
        })
    }
}


#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq),check_bytes,)]
#[archive_attr(derive(Debug))]
struct Header {
    sequence: u64,
    collection_id: u32,
    heads: Vec<Head>,
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




#[cfg(test)]
mod tests {
    use super::*;

    use rkyv::{
        archived_value,
        ser::{Serializer, serializers::BufferSerializer},
        AlignedBytes,
    };

    #[test]
    fn new_storage_meta() {
        let storage_meta = StorageMeta::new(512, 512, 4096, 1000)
            .expect("Could not make StorageMeta");
        let mut serializer = BufferSerializer::new(AlignedBytes([0u8; 256]));
        let pos = serializer.serialize_value(&storage_meta)
                .expect("failed to archive event");
        let buf = serializer.into_inner();
        //let archived = rkyv::check_archived_root::<StorageMeta>(&buf[pos..]).unwrap();
        //assert_eq!(archived, &storage_meta);
        let archived = unsafe { archived_value::<StorageMeta>(buf.as_ref(), pos) };
        assert_eq!(archived, &storage_meta);

    }
}
