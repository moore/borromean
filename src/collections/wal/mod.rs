use crate::io::{Io, IoBackend, IoError, RegionAddress, RegionSequence};
use crate::{CollectionId, CollectionType};

use postcard::{from_bytes_crc32, to_slice_crc32};
use serde::{Deserialize, Serialize};

use crc::{Crc, CRC_32_ISCSI};

#[cfg(test)]
mod tests;

// NOTE: We want to keep using the same wall until it is full so that we don't
// ware down the head of the region more then the tail. (This is not just true
// of WALs but of all collections)

/// This holds a record and its type. We include the collection id and sequence
/// so that we can't be confused with old data that might be contained in a
/// region after it has been reused.
// TODO: A fancier approach would be to include the collection id and sequence in the
// checksum but not the record as we know the values for each segment of the WAL
// when reading it. (each record is checksumed with a CRC)
#[derive(Serialize, Deserialize, Debug)]
struct Entry<'a, S: RegionSequence, A: RegionAddress> {
    collection_id: CollectionId,
    collection_sequence: S,
    #[serde(borrow)]
    record: EntryRecord<'a, A>,
}

#[derive(Serialize, Deserialize, Debug)]
enum EntryRecord<'a, A: RegionAddress> {
    Data(#[serde(borrow)] DataRecord<'a>),
    Commit,
    NextRegion(A),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataRecord<'a> {
    collection_type: CollectionType,
    #[serde(borrow)]
    data: &'a [u8],
}

pub struct WalCursor<A: RegionAddress> {
    region: A,
    offset: usize,
}

pub struct Wal<const SIZE: usize, B: IoBackend> {
    region: B::RegionAddress,
    collection_id: CollectionId,
    collection_sequence: B::StorageSequence,
    head: B::RegionAddress,
    next_entry: usize,
}

pub enum WalRead<'a, A: RegionAddress> {
    Record {
        next: WalCursor<A>,
        record: DataRecord<'a>,
    },
    Commit {
        next: WalCursor<A>,
    },
    EndOfRegion {
        next: WalCursor<A>,
    },
    EndOfWAL,
}

const LEN_BYTES: usize = size_of::<u32>();
const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

impl<const SIZE: usize, B: IoBackend> Wal<SIZE, B> {
    pub fn new<'a>(
        io: &mut Io<'a, B>,
        collection_id: CollectionId,
    ) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        let collection_type = CollectionType::Wal;

        let region = io.allocate_region(collection_id)?;
        io.write_region_header(
            region,
            collection_id,
            collection_type,
            B::CollectionSequence::first(),
        )?;

        Ok(Self {
            region,
            collection_id,
            collection_sequence: B::StorageSequence::first(),
            head: region,
            next_entry: 0,
        })
    }

    pub fn write<'a>(
        &mut self,
        io: &mut Io<'a, B>,
        collection_type: CollectionType,
        data: &[u8],
        buffer: &mut [u8],
    ) -> Result<(), IoError<B::BackingError, B::RegionAddress>> {
        let entry = Entry::<B::StorageSequence, B::RegionAddress> {
            collection_id: self.collection_id,
            collection_sequence: self.collection_sequence,
            record: EntryRecord::Data(DataRecord {
                collection_type,
                data,
            }),
        };

        let Ok(used) = to_slice_crc32(&entry, buffer, CRC.digest()) else {
            // TODO: Log error details
            return Err(IoError::SerializationError);
        };

        let offset = self.next_entry;
        let len: usize = used.len() + LEN_BYTES;

        if offset + len > SIZE {
            return Err(IoError::StorageFull);
        }

        let Ok(len): Result<u32, _> = len.try_into() else {
            return Err(IoError::SerializationError);
        };

        let len_bytes = len.to_le_bytes();
        io.write_region_data(self.region, &len_bytes, offset)?;

        let offset = offset + len_bytes.len();
        io.write_region_data(self.region, used, offset)?;

        // This should never fail but we check anyway to catch
        // refactoring errors.
        let Ok(len): Result<usize, _> = len.try_into() else {
            return Err(IoError::SerializationError);
        };

        self.next_entry += len;
        Ok(())
    }

    pub fn get_cursor(&self) -> WalCursor<B::RegionAddress> {
        WalCursor {
            region: self.head,
            offset: 0,
        }
    }

    pub fn read<'a, 'b>(
        &mut self,
        io: &mut Io<'a, B>,
        cursor: WalCursor<B::RegionAddress>,
        buffer: &'b mut [u8],
    ) -> Result<WalRead<'b, B::RegionAddress>, IoError<B::BackingError, B::RegionAddress>> {
        let region = cursor.region;
        let offset = cursor.offset;

        if offset + LEN_BYTES > SIZE {
            return Ok(WalRead::EndOfWAL);
        }

        let mut len_bytes = [0u8; LEN_BYTES];

        io.get_region_data(region, offset, LEN_BYTES, len_bytes.as_mut_slice())?;
        let len: u32 = u32::from_le_bytes(len_bytes);

        let Ok(len): Result<usize, _> = len.try_into() else {
            return Err(IoError::SerializationError);
        };

        let offset = offset + len_bytes.len();
        let record_len = len - len_bytes.len();
        // TODO: handle the case where we get to the end of the region
        io.get_region_data(region, offset, record_len, buffer)?;

        let entry: Entry<'b, B::StorageSequence, B::RegionAddress> =
            match from_bytes_crc32(buffer, CRC.digest()) {
                Ok(entry) => entry,
                Err(_e) => {
                    // TODO: Log error
                    return Err(IoError::SerializationError);
                }
            };

        // Assume not bugs and that this is due to old stale data.
        if entry.collection_id != self.collection_id
            || entry.collection_sequence != self.collection_sequence
        {
            return Ok(WalRead::EndOfWAL);
        }

        let result: WalRead<'b, <B as IoBackend>::RegionAddress> = match entry.record {
            EntryRecord::Data(data_record) => {
                let region = cursor.region;
                let offset = offset + record_len;
                WalRead::Record {
                    next: WalCursor { region, offset },
                    record: data_record,
                }
            }
            EntryRecord::Commit => {
                let region = cursor.region;
                let offset = offset + record_len;
                WalRead::Commit {
                    next: WalCursor { region, offset },
                }
            }
            EntryRecord::NextRegion(next_region) => {
                let region = next_region;
                let offset = 0;
                WalRead::EndOfRegion {
                    next: WalCursor { region, offset },
                }
            }
        };

        Ok(result)
    }
}
