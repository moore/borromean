use crate::io::{Io, IoBackend, IoError, RegionSequence};
use crate::{CollectionId, CollectionType};


#[cfg(test)]
mod tests;

// NOTE: We want to keep using the same wall until it is full so that we don't
// ware down the head of the region more then the tail. (This is not just true
// of WALs but of all collections)

struct Wal<const SIZE: usize, B: IoBackend> {
    region: B::RegionAddress,
    collection_id: CollectionId,
    collection_sequence: B::Sequence,
    next_region: Option<B::RegionAddress>,
    next_entry: usize,
}

impl<const SIZE: usize, B: IoBackend> Wal<SIZE, B> {
    pub fn new<'a>(
        io: &mut Io<'a, B>,
        collection_id: CollectionId,
    ) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        let collection_type = CollectionType::Wal;

        let region = io.allocate_region(collection_id)?;
        io.write_region_header(region, collection_id, collection_type, B::Sequence::first())?;
        
        Ok(Self {
            region,
            collection_id,
            collection_sequence: B::Sequence::first(),
            next_region: None,
            next_entry: 0,
        })
    }
}
