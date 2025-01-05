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

type RefType = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryRef {
    start: RefType,
    end: RefType,
}

const ENTRY_REF_POINTER_SIZE: usize = size_of::<RefType>();
const ENTRY_REF_SIZE: usize = ENTRY_REF_POINTER_SIZE * 2;

impl EntryRef {
    fn write(
        buffer: &mut [u8],
        index: RecordIndex,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<(), MapError> {
        let offset = index.offset(buffer)?;

        let start: RefType = start
            .0
            .try_into()
            .map_err(|e| MapError::SerializationError)?;

        let end: RefType = end.0.try_into().map_err(|_| MapError::SerializationError)?;

        let start_bytes = start.to_le_bytes();
        let end_bytes = end.to_le_bytes();

        let buf = &mut buffer[offset..offset + ENTRY_REF_POINTER_SIZE];
        buf.copy_from_slice(&start_bytes);
        let buf = &mut buffer[offset + ENTRY_REF_POINTER_SIZE..offset + ENTRY_REF_POINTER_SIZE * 2];
        buf.copy_from_slice(&end_bytes);

        Ok(())
    }

    fn insert(
        buffer: &mut [u8],
        index: RecordIndex,
        last_index: RecordIndex,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<(), MapError> {
        let location = index.0;
        let current = index.0 + 1;

        let current_offset = index.offset(buffer)?;
        let target_offset = index.next().offset(buffer)?;
        let end_offset = last_index.previous().offset(buffer)?;

        buffer.copy_within(current_offset..end_offset, target_offset);

        Self::write(buffer, index, start, end)
    }

    fn read(buffer: &[u8], index: RecordIndex) -> Result<Self, MapError> {
        let offset = index.offset(buffer)?;

        let mut buf = [0u8; ENTRY_REF_POINTER_SIZE];

        buf.copy_from_slice(&buffer[offset..offset + ENTRY_REF_POINTER_SIZE]);
        let start = RefType::from_le_bytes(buf);

        buf.copy_from_slice(
            &buffer[offset + ENTRY_REF_POINTER_SIZE..offset + ENTRY_REF_POINTER_SIZE * 2],
        );
        let end = RefType::from_le_bytes(buf);

        let entry = Self {
            start: start,
            end: end,
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

    fn increment(&mut self) {
        self.0 += 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RecordOffset(usize);

impl RecordOffset {
    fn new(offset: usize) -> Self {
        Self(offset)
    }

    fn increment(&mut self, amount: usize) -> Result<(), MapError> {
        self.0 = self
            .0
            .checked_add(amount)
            .ok_or(MapError::SerializationError)?;
        Ok(())
    }
}

type RecordIndexInner = usize;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RecordIndex(RecordIndexInner);

// BUG: switched to checked math and Result
impl RecordIndex {
    fn new(index: RecordIndexInner) -> Self {
        Self(index)
    }

    fn increment(&mut self) {
        self.0 += 1;
    }

    fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    fn previous(&self) -> Self {
        Self(self.0 - 1)
    }

    fn offset(&self, buffer: &[u8]) -> Result<usize, MapError> {
        // The 0th index is at the end of the buffer and we work
        // backwards.
        let Some(offset) = buffer.len().checked_sub(ENTRY_REF_SIZE * (self.0 + 1)) else {
            return Err(MapError::IndexOutOfBounds);
        };

        Ok(offset)
    }
}

#[derive(Debug)]
enum SearchResult {
    Found(RecordIndex),
    NotFound(RecordIndex),
}

pub struct LsmMap<'a, K, V, B: IoBackend> {
    id: CollectionId,
    //wal: Wal<B>, // BUG: implement wal usage
    record_count: EntryCount,
    next_record_offset: RecordOffset,
    next_record_index: RecordIndex,
    map: &'a mut [u8],
    next: Option<B::RegionAddress>,
    _phantom: PhantomData<(K, V)>,
}

impl<'a, K, V, B: IoBackend> LsmMap<'a, K, V, B>
where
    K: Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    pub fn init<const MAX_HEADS: usize>(
        io: &mut Io<B, MAX_HEADS>,
        id: CollectionId,
        buffer: &'a mut [u8],
    ) -> Result<Self, IoError<B::BackingError, B::RegionAddress>> {
        //let wal = io.new_wal()?;
        let record_count = EntryCount(0);
        let next_record_offset = RecordOffset(ENTRY_COUNT_SIZE);
        let next_record_index = RecordIndex(0);
        let map = buffer;
        let _phantom = PhantomData;

        record_count.write(map);

        Ok(Self {
            id,
            //wal,
            record_count,
            next_record_index,
            next_record_offset,
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
            SearchResult::Found(index) => {
                // TODO: Try and overwrite the the entry before we leak it.
                // leak the current value and write in a new location.
                let (start, end) = self.add_entry(&entry)?;

                EntryRef::write(self.map, index, start, end)?;

                self.next_record_offset = end;
            }
            SearchResult::NotFound(index) => {
                let (start, end) = self.add_entry(&entry)?;
                if self.record_count.0 == 0 {
                    EntryRef::write(self.map, index, start, end)?;
                } else {
                    EntryRef::insert(self.map, index, self.next_record_index, start, end)?;
                }

                self.next_record_index.increment();

                self.next_record_offset = end;

                self.record_count.increment();

                self.record_count.write(self.map);
            }
        }

        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, MapError> {
        let search_result = self.find_index(key)?;

        match search_result {
            SearchResult::NotFound(_) => Ok(None),
            SearchResult::Found(index) => {
                let entry_ref = EntryRef::read(self.map, index)?;
                let entry: Entry<K, V> =
                    from_bytes(&self.map[entry_ref.start as usize..entry_ref.end as usize])?;
                Ok(Some(entry.value))
            }
        }
    }

    fn add_entry(&mut self, entry: &Entry<K, V>) -> Result<(RecordOffset, RecordOffset), MapError> {
        let start = self.next_record_offset;
        let index_offset = self.next_record_index.offset(self.map)?;
        // TODO: check bounds?
        let buf = &mut self.map[start.0..index_offset];
        let used = to_slice(&entry, buf)?.len();

        let mut end = start;

        end.increment(used)?;

        Ok((start, end))
    }

    // TODO: Proving the binary search could be done in Kani
    fn find_index(&self, key: &K) -> Result<SearchResult, MapError> {
        if self.record_count.0 == 0 {
            return Ok(SearchResult::NotFound(RecordIndex(0)));
        } else if self.record_count.0 == 1 {
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(0))?;
            let entry: Entry<K, V> =
                from_bytes(&self.map[entry_ref.start as usize..entry_ref.end as usize])?;
            let result = match key.cmp(&entry.key) {
                core::cmp::Ordering::Equal => SearchResult::Found(RecordIndex(0)),
                core::cmp::Ordering::Less => SearchResult::NotFound(RecordIndex(0)),
                core::cmp::Ordering::Greater => SearchResult::NotFound(RecordIndex(1)),
            };

            return Ok(result);
        }

        let mut left = 0;
        let mut right = self.next_record_index.0 - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(mid))?;
            let entry: Entry<K, V> =
                from_bytes(&self.map[entry_ref.start as usize..entry_ref.end as usize])?;

            match key.cmp(&entry.key) {
                core::cmp::Ordering::Equal => return Ok(SearchResult::Found(RecordIndex(mid))),
                core::cmp::Ordering::Less => right = mid + 1,
                core::cmp::Ordering::Greater => left = mid - 1,
            }
        }

        Ok(SearchResult::NotFound(RecordIndex(left)))
    }
}
