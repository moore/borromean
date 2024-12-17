use crate::io::{Io, IoBackend, IoError, RegionAddress, RegionSequence};
use crate::{CollectionId, CollectionType, RegionHeader};

use postcard::{from_bytes_crc32, to_slice_crc32};
use serde::{Deserialize, Serialize};

use crc::{Crc, CRC_16_IBM_SDLC, CRC_32_ISCSI};

#[cfg(test)]
mod tests;

// NOTE: We want to keep using the same wall until it is full so that we don't
// ware down the head of the region more then the tail. (This is not just true
// of WALs but of all collections)

#[derive(Serialize, Deserialize, Debug)]
enum EntryRecord<'a, A: RegionAddress, S: RegionSequence> {
    Data(#[serde(borrow)] DataRecord<'a>),
    Commit {
        to_region: A,
        to_offset: usize,
        to_sequence: S,
    },
    NextRegion(A),
}

impl<'a, A: RegionAddress, S: RegionSequence> EntryRecord<'a, A, S> {
    pub fn postcard_max_len() -> usize {
        // we add one because the discriminant will
        // fit in a single byte with 3 variants
        A::postcard_max_len() + 1
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataRecord<'a> {
    collection_type: CollectionType,
    #[serde(borrow)]
    data: &'a [u8],
}

#[derive(Debug)]
pub struct WalCursor<A: RegionAddress, S: RegionSequence> {
    region: A,
    offset: usize,
    collection_sequence: S,
}

pub struct Wal<B: IoBackend> {
    region: B::RegionAddress,
    region_start: usize,
    collection_id: CollectionId,
    collection_sequence: B::CollectionSequence,
    head: B::RegionAddress,
    head_sequence: B::CollectionSequence,
    next_entry: usize,
}

pub enum WalRead<'a, A: RegionAddress, S: RegionSequence> {
    Record {
        next: WalCursor<A, S>,
        record: DataRecord<'a>,
    },
    Commit {
        to_region: A,
        to_offset: usize,
        to_sequence: S,
        next: WalCursor<A, S>,
    },
    EndOfRegion {
        next: WalCursor<A, S>,
    },
    EndOfWAL,
}

enum WriteResult {
    Wrote(usize),
    RegionFull,
}

type RecordLength = u16;
const LEN_RECORD_BYTES: usize = size_of::<RecordLength>();

type LenCrc = u16;
const LEN_CRC_BYTES: usize = size_of::<LenCrc>();

const LEN_BYTES: usize = LEN_RECORD_BYTES + LEN_CRC_BYTES;

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const LEN_CRC: Crc<LenCrc> = Crc::<LenCrc>::new(&CRC_16_IBM_SDLC);

impl<B: IoBackend> Wal<B> {
    pub fn new<const MAX_HEADS: usize>(
        io: &mut Io<B, MAX_HEADS>,
        collection_id: CollectionId,
    ) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        let collection_type = CollectionType::Wal;
        let collection_sequence = B::CollectionSequence::first();

        let region = io.allocate_region(collection_id)?;
        io.write_region_header(region, collection_id, collection_type, collection_sequence)?;

        Ok(Self {
            region,
            region_start: 0,
            collection_id,
            collection_sequence,
            head: region,
            head_sequence: collection_sequence,
            next_entry: 0,
        })
    }

    pub fn open<'b, const MAX_HEADS: usize>(
        io: &mut Io<B, MAX_HEADS>,
        region: B::RegionAddress,
        buffer: &'b mut [u8],
    ) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        // Make sure io barrow from get_region_header ends.
        let (collection_id, mut collection_sequence) = {
            let header: <B as IoBackend>::RegionHeader<'_> = io.get_region_header(region)?;
            let collection_id = header.collection_id();
            let collection_sequence = header.collection_sequence();
            (collection_id, collection_sequence)
        };

        let mut region = region;
        let mut region_start = 0;
        let mut head = region;
        let mut head_sequence = collection_sequence;
        let mut next_entry = 0;

        let mut this = Self {
            region,
            region_start,
            collection_id,
            collection_sequence,
            head,
            head_sequence,
            next_entry,
        };

        let mut cursor = this.get_cursor();

        loop {
            match this.read(io, cursor, buffer)? {
                WalRead::Record { next, record } => {
                    cursor = next;
                }
                WalRead::Commit {
                    to_region,
                    to_offset,
                    to_sequence,
                    next,
                } => {
                    // If we have a commit the current region we are in
                    // is not the head.
                    region = to_region;
                    region_start = to_offset;
                    collection_sequence = to_sequence;

                    cursor = next;
                }
                WalRead::EndOfRegion { next } => {
                    cursor = next;
                }
                WalRead::EndOfWAL => {
                    break;
                }
            }

            head = cursor.region;
            next_entry = cursor.offset;
            head_sequence = cursor.collection_sequence;
        }

        this.region = region;
        this.region_start = region_start;
        this.collection_sequence = collection_sequence;

        Ok(this)
    }

    pub fn region(&self) -> B::RegionAddress {
        self.region
    }

    pub fn commit<const MAX_HEADS: usize>(
        &mut self,
        io: &mut Io<B, MAX_HEADS>,
        cursor: WalCursor<B::RegionAddress, B::CollectionSequence>,
        buffer: &mut [u8],
    ) -> Result<(), IoError<B::BackingError, B::RegionAddress>> {
        let entry = EntryRecord::Commit {
            to_region: cursor.region,
            to_offset: cursor.offset,
            to_sequence: cursor.collection_sequence,
        };

        self.write_entry(io, entry, buffer)
    }

    pub fn write<const MAX_HEADS: usize>(
        &mut self,
        io: &mut Io<B, MAX_HEADS>,
        collection_type: CollectionType,
        data: &[u8],
        buffer: &mut [u8],
    ) -> Result<(), IoError<B::BackingError, B::RegionAddress>> {
        let entry = EntryRecord::Data(DataRecord {
            collection_type,
            data,
        });

        self.write_entry(io, entry, buffer)
    }

    fn write_entry<const MAX_HEADS: usize>(
        &mut self,
        io: &mut Io<B, MAX_HEADS>,
        entry: EntryRecord<B::RegionAddress, B::CollectionSequence>,
        buffer: &mut [u8],
    ) -> Result<(), IoError<B::BackingError, B::RegionAddress>> {
        let collection_id = self.collection_id;

        let result = self.write_worker(io, &entry, buffer)?;

        match result {
            WriteResult::Wrote(_len) => Ok(()),
            WriteResult::RegionFull => {
                let region = io.allocate_region(collection_id)?;

                let next_entry = EntryRecord::NextRegion(region);

                let WriteResult::Wrote(_len) = self.write_worker(io, &next_entry, buffer)? else {
                    // Should not happens as this is a new region.
                    // TODO: Log error
                    return Err(IoError::SerializationError);
                };

                let collection_sequence = self.collection_sequence.increment();
                io.write_region_header(
                    region,
                    collection_id,
                    CollectionType::Wal,
                    collection_sequence,
                )?;

                // do this after writing the header as it may fail.
                self.collection_sequence = collection_sequence;
                self.region = region;
                self.next_entry = 0;

                let result = self.write_worker(io, &entry, buffer)?;

                match result {
                    WriteResult::Wrote(_len) => Ok(()),
                    WriteResult::RegionFull => {
                        // This should not happen
                        // TODO: log error
                        Err(IoError::SerializationError)
                    }
                }
            }
        }
    }

    /// Formant is [record len][record len crc][record]
    /// The crc of the record len is computed over the
    /// length itself as well as the collection_sequence
    /// and the collection_id. Adding this in ensures
    /// that a record will only be read if it is current
    /// and we will reject stale data left from a previous
    /// use of the region.
    pub fn write_worker<const MAX_HEADS: usize>(
        &mut self,
        io: &mut Io<B, MAX_HEADS>,
        entry: &EntryRecord<B::RegionAddress, B::CollectionSequence>,
        buffer: &mut [u8],
    ) -> Result<WriteResult, IoError<B::BackingError, B::RegionAddress>> {
        let Ok(used) = to_slice_crc32(&entry, buffer, CRC.digest()) else {
            // TODO: Log error details
            return Err(IoError::SerializationError);
        };

        let offset = self.next_entry;
        let len: usize = used.len() + LEN_BYTES;

        // We need our own postcard_max_len because the
        // the built in feature is experimental and can't
        // be depended on.
        let next_command_len =
            EntryRecord::<B::RegionAddress, B::CollectionSequence>::postcard_max_len() + LEN_BYTES;
        let size = io.region_size();
        if offset + len + next_command_len > size {
            if len + next_command_len > size {
                return Err(IoError::RecordTooLarge(len));
            } else {
                return Ok(WriteResult::RegionFull);
            }
        }

        let Ok(len): Result<RecordLength, _> = len.try_into() else {
            // TODO: log error. This really should not happen
            // it means that the length is really big.
            return Err(IoError::SerializationError);
        };

        let len_record_bytes = len.to_le_bytes();
        io.write_region_data(self.region, &len_record_bytes, offset)?;

        let offset = offset + len_record_bytes.len();

        let sequence_bytes = self.collection_sequence.to_le_bytes();
        let collection_id_bytes = self.collection_id.to_le_bytes();

        let mut digest = LEN_CRC.digest();
        digest.update(&len_record_bytes);
        digest.update(&sequence_bytes);
        digest.update(&collection_id_bytes);

        let len_crc = digest.finalize();
        let len_crc_bytes = len_crc.to_le_bytes();

        io.write_region_data(self.region, &len_crc_bytes, offset)?;

        let offset = offset + len_crc_bytes.len();

        io.write_region_data(self.region, used, offset)?;

        // This should never fail but we check anyway to catch
        // refactoring errors.
        let Ok(len): Result<usize, _> = len.try_into() else {
            // TODO: Log this error
            return Err(IoError::SerializationError);
        };

        self.next_entry += len;
        Ok(WriteResult::Wrote(len))
    }

    pub fn get_cursor(&self) -> WalCursor<B::RegionAddress, B::CollectionSequence> {
        WalCursor {
            region: self.head,
            offset: 0,
            collection_sequence: self.head_sequence,
        }
    }

    fn read<'b, const MAX_HEADS: usize>(
        &mut self,
        io: &mut Io<B, MAX_HEADS>,
        cursor: WalCursor<B::RegionAddress, B::CollectionSequence>,
        buffer: &'b mut [u8],
    ) -> Result<
        WalRead<'b, B::RegionAddress, B::CollectionSequence>,
        IoError<B::BackingError, B::RegionAddress>,
    > {
        let region = cursor.region;
        let offset = cursor.offset;
        let size = io.region_size();
        if offset + LEN_BYTES > size {
            return Ok(WalRead::EndOfWAL);
        }

        let mut len_bytes = [0u8; LEN_RECORD_BYTES];
        io.get_region_data(region, offset, LEN_RECORD_BYTES, len_bytes.as_mut_slice())?;
        let len = RecordLength::from_le_bytes(len_bytes);

        let offset = offset + len_bytes.len();

        let mut crc_bytes = [0u8; LEN_CRC_BYTES];
        io.get_region_data(region, offset, LEN_CRC_BYTES, crc_bytes.as_mut_slice())?;
        let read_crc = RecordLength::from_le_bytes(crc_bytes);

        let offset = offset + crc_bytes.len();

        let sequence_bytes = cursor.collection_sequence.to_le_bytes();
        let collection_id_bytes = self.collection_id.to_le_bytes();

        let mut digest = LEN_CRC.digest();
        digest.update(&len_bytes);
        digest.update(&sequence_bytes);
        digest.update(&collection_id_bytes);

        let len_crc = digest.finalize();

        // Assume it's not corruption and that this is the end of
        // current wall.
        if len_crc != read_crc {
            return Ok(WalRead::EndOfWAL);
        }

        let Ok(len): Result<usize, _> = len.try_into() else {
            return Err(IoError::SerializationError);
        };

        if len + offset > size {
            // This should not be possible.
            // TODO: Log error case
            return Err(IoError::SerializationError);
        }

        let record_len: usize = len - (len_bytes.len() + crc_bytes.len());
        io.get_region_data(region, offset, record_len, buffer)?;

        let entry: EntryRecord<'b, B::RegionAddress, B::CollectionSequence> =
            match from_bytes_crc32(buffer, CRC.digest()) {
                Ok(entry) => entry,
                Err(_e) => {
                    // TODO: Log error
                    return Err(IoError::SerializationError);
                }
            };

        let result: WalRead<
            'b,
            <B as IoBackend>::RegionAddress,
            <B as IoBackend>::CollectionSequence,
        > = match entry {
            EntryRecord::Data(data_record) => {
                let region = cursor.region;
                let offset = offset + record_len;
                let collection_sequence = cursor.collection_sequence;
                WalRead::Record {
                    next: WalCursor {
                        region,
                        offset,
                        collection_sequence,
                    },
                    record: data_record,
                }
            }
            EntryRecord::Commit {
                to_offset,
                to_region,
                to_sequence,
            } => {
                let region = cursor.region;
                let offset = offset + record_len;
                let collection_sequence = cursor.collection_sequence;
                WalRead::Commit {
                    to_offset,
                    to_region,
                    to_sequence,
                    next: WalCursor {
                        region,
                        offset,
                        collection_sequence,
                    },
                }
            }
            EntryRecord::NextRegion(next_region) => {
                let region = next_region;
                let offset = 0;
                let collection_sequence = cursor.collection_sequence.increment();
                WalRead::EndOfRegion {
                    next: WalCursor {
                        region,
                        offset,
                        collection_sequence,
                    },
                }
            }
        };

        Ok(result)
    }
}
