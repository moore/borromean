use crate::collections::wal::Wal;
use crate::io::{Io, IoBackend, IoError, RegionAddress, RegionSequence};
use crate::CollectionId;
use core::marker::PhantomData;
use core::mem::size_of;
use postcard::{from_bytes, to_slice};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;


// A alloc free version of this would store everything in a array of
// bytes. The format would be:
// ```
// [entry count][first written entry]..[last written entry][index ref n]..[index ref 0]
// ```
// Each `index ref` is of the form `[star offset][end offset]` and they stored in
// sorted order based on sort oder of the entries.
// This design has two main ideas. The first is that we only have to shuffle the index and
// not the entries when we insert values. The second is that we grow the index from the
// top down and the entries from the bottom up. This way we fill the space without knowing
// the size of the entries.
//
// we write the index in backwards order so that we can implement merge join efficiently.

#[derive(Debug)]
pub enum MapError {
    InvalidEntryCount,
    SerializationError,
    IndexOutOfBounds,
}

impl From<postcard::Error> for MapError {
    fn from(_: postcard::Error) -> Self {
        // TODO: log error
        MapError::SerializationError
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
))]
pub struct Entry<K, V>
where
    K: Ord + PartialOrd + Eq + PartialEq,
{
    key: K,
    value: V,
}

type RefInner = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct RefType(RefInner);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryRef {
    start: RefType,
    end: RefType,
}

const ENTRY_REF_POINTER_SIZE: usize = size_of::<RefType>();
const ENTRY_REF_SIZE: usize = ENTRY_REF_POINTER_SIZE * 2;

impl EntryRef {
    fn offset_from_index(index: IndexOffset, buffer: &[u8]) -> Result<usize, MapError> {
        // The 0th index is at the end of the buffer and we work
        // backwards.
        let Some(offset) = buffer.len().checked_sub(ENTRY_REF_SIZE * (index.0 + 1)) else {
            return Err(MapError::IndexOutOfBounds);
        };

        Ok(offset)
    }

    fn write(
        buffer: &mut [u8],
        index: IndexOffset,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<(), MapError> {
        let offset = Self::offset_from_index(index, buffer)?;

        let start: RefInner = start
            .0
            .try_into()
            .map_err(|e| MapError::SerializationError)?;
        
        let end: RefInner = end.0.try_into().map_err(|_| MapError::SerializationError)?;

        let start_bytes = start.to_le_bytes();
        let end_bytes = end.to_le_bytes();

        let buf = &mut buffer[offset..offset + ENTRY_REF_POINTER_SIZE];
        buf.copy_from_slice(&start_bytes);
        let buf = &mut buffer[offset + ENTRY_REF_POINTER_SIZE..offset + ENTRY_REF_POINTER_SIZE * 2];
        buf.copy_from_slice(&end_bytes);

        Ok(())
    }

    fn read(buffer: &[u8], index: IndexOffset) -> Result<Self, MapError> {
        let offset = Self::offset_from_index(index, buffer)?;

        let mut buf = [0u8; ENTRY_REF_POINTER_SIZE];

        buf.copy_from_slice(&buffer[offset..offset + ENTRY_REF_POINTER_SIZE]);
        let start = RefInner::from_le_bytes(buf);

        buf.copy_from_slice(
            &buffer[offset + ENTRY_REF_POINTER_SIZE..offset + ENTRY_REF_POINTER_SIZE * 2],
        );
        let end = RefInner::from_le_bytes(buf);

        let entry = Self {
            start: RefType(start),
            end: RefType(end),
        };

        Ok(entry)
    }

    

}
struct EntryCount(u32);
const ENTRY_COUNT_SIZE: usize = size_of::<EntryCount>();

impl EntryCount {
    fn from_bytes(bytes: &[u8]) -> Result<Self, MapError> {
        if bytes.len() < ENTRY_COUNT_SIZE {
            return Err(MapError::InvalidEntryCount);
        }

        let mut buffer = [0u8; ENTRY_COUNT_SIZE];
        buffer.copy_from_slice(&bytes[..ENTRY_COUNT_SIZE]);
        let count = u32::from_le_bytes(buffer);
        Ok(Self(count))
    }

    fn to_bytes(&self) -> [u8; ENTRY_COUNT_SIZE] {
        self.0.to_le_bytes()
    }

    fn write(&self, buffer: &mut [u8]) {
        let bytes = self.to_bytes();
        buffer[..ENTRY_COUNT_SIZE].copy_from_slice(&bytes);
    }

    fn read(buffer: &[u8]) -> Result<Self, MapError> {
        Self::from_bytes(buffer)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RecordOffset(usize);

impl RecordOffset {
    fn new(offset: usize) -> Self {
        Self(offset)
    }

    fn increment(&mut self, amount: usize) -> Result<(), MapError> {
        self.0
            .checked_add(amount)
            .ok_or(MapError::SerializationError)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct IndexOffset(usize);

impl IndexOffset {
    fn new(offset: usize) -> Self {
        Self(offset)
    }

    fn increment(&mut self) {
        // BUG use checked arithmetic
        self.0 -= size_of::<EntryRef>();
    }

    fn seek(&self, count: i32) -> Self {
        if count < 0 {
            let diff = count.abs() as usize * size_of::<EntryRef>();
            Self(self.0 + diff)
        } else {
            let diff = count as usize * size_of::<EntryRef>();
            Self(self.0 - diff)
        }
    }
}

enum SearchResult {
    Found(usize),
    NotFound(usize),
}

pub struct LsmMap<'a, K, V, B: IoBackend, const CASH_SIZE: usize> {
    id: CollectionId,
    wal: Wal<B>,
    record_count: EntryCount,
    record_offset: RecordOffset,
    index_offset: IndexOffset,
    map: &'a mut [u8],
    next: Option<B::RegionAddress>,
    _phantom: PhantomData<(K, V)>,
}

impl<'a, K, V, B: IoBackend, const CASH_SIZE: usize> LsmMap<'a, K, V, B, CASH_SIZE>
where
    K: Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    pub fn init<const MAX_HEADS: usize>(
        io: &mut Io<B, MAX_HEADS>,
        id: CollectionId,
        buffer: &'a mut [u8],
    ) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        let wal = io.new_wal()?;
        let record_count = EntryCount(0);
        let record_offset = RecordOffset(ENTRY_COUNT_SIZE);
        let index_offset = IndexOffset(buffer.len());
        let map = buffer;
        let _phantom = PhantomData;

        record_count.write(map);

        Ok(Self {
            id,
            wal,
            record_count,
            index_offset,
            record_offset,
            map,
            next: None,
            _phantom,
        })
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), MapError>
    where
        K: Ord + PartialOrd + Eq + PartialEq + Serialize + for<'d> Deserialize<'d>,
        V: Serialize + for<'d> Deserialize<'d>,
    {
        let search_result = self.find_index(&key)?;

        let entry = Entry { key, value };

        match search_result {
            SearchResult::Found(index) => {}
            SearchResult::NotFound(index) => {
                let start = self.record_offset;
                // TODO: check bounds?
                let buf = &mut self.map[start.0..self.index_offset.0];
                let used = to_slice(&entry, buf)?.len();

                let mut end = start;
                end.increment(used)?;

                EntryRef::write(self.map, self.index_offset, start, end)?;

                self.index_offset.increment();

                self.record_offset.increment(used)?;
            }
        }

        Ok(())
    }

    fn find_index(&self, key: &K) -> Result<SearchResult, MapError> {
        let mut left = self.index_offset.0 as i32;
        let mut right = (self.map.len() - ENTRY_REF_SIZE) as i32;

        while left <= right {
            let mid = (left + right) / 2;
            let entry_ref = EntryRef::read(self.map, self.index_offset.seek(mid))?;
            let entry: Entry<K, V> =
                from_bytes(&self.map[entry_ref.start.0 as usize..entry_ref.end.0 as usize])?;

            match key.cmp(&entry.key) {
                core::cmp::Ordering::Equal => return Ok(SearchResult::Found(mid as usize)),
                core::cmp::Ordering::Less => right = mid - 1,
                core::cmp::Ordering::Greater => left = mid + 1,
            }
        }
        Ok(SearchResult::Found(left as usize))
    }
}
