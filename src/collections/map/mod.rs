use crate::disk::{DiskError, FreePointerFooter, Header};
use crate::flash_io::FlashIo;
use crate::mock::MockError;
use crate::storage::{StorageRuntime, StorageRuntimeError, StorageVisitError};
use crate::workspace::StorageWorkspace;
use crate::{CollectionId, CollectionType};
use core::fmt::Debug;
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


// We can store merged maps larger than a single segment by building up a skip list as 
// we merge. This works because we will never have to do inserts in to the skip list as we
// merge ordered maps. Can we even optimize this by stopping the merge and reusing previous
// Segments if we end up wit only one map to merge?
//
// so the over all stricture can be: write to log + in memory map -> write to new on disk
// skip list -> merge lists when there are to many.
//
// question is should we always merge all maps in a merge or try and maintain something like
// each layer doubling in size?
//
// In this approach the head of the map is alway the log. 


#[derive(Debug)]
pub enum MapError {
    InvalidEntryCount,
    SerializationError,
    IndexOutOfBounds,
    SnapshotTooLarge,
    BufferTooSmall,
}

impl From<postcard::Error> for MapError {
    fn from(error: postcard::Error) -> Self {
        match error {
            postcard::Error::SerializeBufferFull => MapError::BufferTooSmall,
            _ => MapError::SerializationError,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
))]
pub struct Entry<K, V>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq,
    V: Debug,
{
    key: K,
    value: Option<V>,
}

type RefType = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryRef {
    start: RefType,
    end: RefType,
}

const ENTRY_REF_POINTER_SIZE: usize = size_of::<RefType>();
const ENTRY_REF_SIZE: usize = ENTRY_REF_POINTER_SIZE * 2;
const SNAPSHOT_ENTRY_COUNT_SIZE: usize = size_of::<u32>();
const SNAPSHOT_ENTRY_BYTES_LEN_SIZE: usize = size_of::<u32>();
const REGION_SNAPSHOT_LEN_SIZE: usize = size_of::<u32>();

pub const MAP_REGION_V1_FORMAT: u16 = 1;
pub const EMPTY_MAP_SNAPSHOT: [u8; SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE] =
    [0u8; SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE];

#[derive(Debug, Clone, Copy)]
pub(crate) struct MapCheckpoint {
    record_count: u32,
    next_record_offset: usize,
    next_record_index: usize,
}

#[derive(Debug)]
pub enum MapStorageError {
    Map(MapError),
    Storage(StorageRuntimeError),
    Mock(MockError),
    Disk(DiskError),
    UnknownCollection(CollectionId),
    DroppedCollection(CollectionId),
    CollectionTypeMismatch {
        collection_id: CollectionId,
        expected: u16,
        actual: Option<u16>,
    },
    UnsupportedRegionFormat {
        collection_id: CollectionId,
        region_index: u32,
        actual: u16,
    },
}

impl From<MapError> for MapStorageError {
    fn from(error: MapError) -> Self {
        Self::Map(error)
    }
}

impl From<StorageRuntimeError> for MapStorageError {
    fn from(error: StorageRuntimeError) -> Self {
        Self::Storage(error)
    }
}

impl From<MockError> for MapStorageError {
    fn from(error: MockError) -> Self {
        Self::Mock(error)
    }
}

impl From<DiskError> for MapStorageError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de>, V: Deserialize<'de>"
))]
pub enum MapUpdate<K, V>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq,
    V: Debug,
{
    Set { key: K, value: V },
    Delete { key: K },
}

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
            .map_err(|_| MapError::SerializationError)?;

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
        let current_offset = index.offset(buffer)? + ENTRY_REF_SIZE;
        let target_offset = last_index.next().offset(buffer)?;
        let end_offset = last_index.offset(buffer)?;
        buffer.copy_within(end_offset..current_offset, target_offset);

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

        let entry = Self { start, end };

        Ok(entry)
    }
}

#[derive(Debug)]
struct EntryCount(u32);
const ENTRY_COUNT_SIZE: usize = size_of::<EntryCount>();

impl EntryCount {
    fn to_bytes(&self) -> [u8; ENTRY_COUNT_SIZE] {
        self.0.to_le_bytes()
    }

    fn write(&self, buffer: &mut [u8]) {
        let bytes = self.to_bytes();
        buffer[..ENTRY_COUNT_SIZE].copy_from_slice(&bytes);
    }
    fn increment(&mut self) {
        self.0 += 1;
    }

    fn decode(bytes: [u8; ENTRY_COUNT_SIZE]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RecordOffset(usize);

impl RecordOffset {
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

pub struct LsmMap<'a, K, V, const MAX_INDEXES: usize> {
    //= spec/ring.md#core-requirements
    //# `RING-CORE-002` Each collection MUST be implemented as an append-only data structure whose new writes are added to the head region and whose storage can only be freed by truncating the tail.
    id: CollectionId,
    record_count: EntryCount,
    next_record_offset: RecordOffset,
    next_record_index: RecordIndex,
    map: &'a mut [u8],
    next_layer_count: usize,
    _phantom: PhantomData<(K, V)>,
}

impl<'a, K, V, const MAX_INDEXES: usize> LsmMap<'a, K, V, MAX_INDEXES>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    pub fn new(
        id: CollectionId,
        buffer: &'a mut [u8],
    ) -> Result<Self, MapError> {
        let record_count = EntryCount(0);
        let next_record_offset = RecordOffset(ENTRY_COUNT_SIZE);
        let next_record_index = RecordIndex(0);
        let map = buffer;
        let _phantom = PhantomData;

        record_count.write(map);

        Ok(Self {
            id,
            record_count,
            next_record_index,
            next_record_offset,
            map,
            next_layer_count: 0,
            _phantom,
        })
    }

    pub fn id(&self) -> CollectionId {
        self.id
    }

    pub fn layer_count(&self) -> usize {
        self.next_layer_count
    }

    pub fn set(&mut self, key: K, value: V) -> Result<(), MapError>
    where
        K: Ord + PartialOrd + Eq + PartialEq + Serialize + for<'d> Deserialize<'d>,
        V: Serialize + for<'d> Deserialize<'d>,
    {
        self.set_worker(key, Some(value))
    }

    pub fn delete(&mut self, key: K) -> Result<(), MapError>
    where
        K: Ord + PartialOrd + Eq + PartialEq + Serialize + for<'d> Deserialize<'d>,
    {
        self.set_worker(key, None)
    }

    fn set_worker(&mut self, key: K, value: Option<V>) -> Result<(), MapError>
    where
        K: Ord + PartialOrd + Eq + PartialEq + Serialize + for<'d> Deserialize<'d>,
        V: Serialize + for<'d> Deserialize<'d>,
    {
        let search_result = self.find_index(&key)?;
        let entry = Entry { key, value };

        match search_result {
            SearchResult::Found(index) => {
                // Updating in place is a possible space optimization, but the
                // current format keeps append-only entry payloads until the
                // next snapshot/flush compacts them.
                let (start, end) = self.add_entry(&entry)?;

                EntryRef::write(self.map, index, start, end)?;

                self.next_record_offset = end;
            }
            SearchResult::NotFound(index) => {
                let (start, end) = self.add_entry(&entry)?;
                if index == self.next_record_index {
                    EntryRef::write(self.map, index, start, end)?;
                } else {
                    EntryRef::insert(
                        self.map,
                        index,
                        self.next_record_index.previous(),
                        start,
                        end,
                    )?;
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
                Ok(entry.value)
            }
        }
    }

    fn add_entry(&mut self, entry: &Entry<K, V>) -> Result<(RecordOffset, RecordOffset), MapError> {
        let start = self.next_record_offset;
        let index_offset = self.next_record_index.offset(self.map)?;
        if start.0 >= index_offset {
            return Err(MapError::BufferTooSmall);
        }
        let buf = &mut self.map[start.0..index_offset];
        let used = to_slice(&entry, buf)?.len();

        let mut end = start;

        end.increment(used)?;
        Ok((start, end))
    }

    pub fn snapshot_len(&self) -> Result<usize, MapError> {
        let entry_count =
            usize::try_from(self.record_count.0).map_err(|_| MapError::SerializationError)?;
        let mut entry_bytes_len = 0usize;
        for index in 0..entry_count {
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(index))?;
            let start = usize::from(entry_ref.start);
            let end = usize::from(entry_ref.end);
            let encoded_len = end.checked_sub(start).ok_or(MapError::SerializationError)?;
            entry_bytes_len = entry_bytes_len
                .checked_add(encoded_len)
                .ok_or(MapError::SerializationError)?;
        }
        SNAPSHOT_ENTRY_COUNT_SIZE
            .checked_add(SNAPSHOT_ENTRY_BYTES_LEN_SIZE)
            .and_then(|len| len.checked_add(entry_bytes_len))
            .and_then(|len| len.checked_add(entry_count.checked_mul(ENTRY_REF_SIZE)?))
            .ok_or(MapError::SerializationError)
    }

    pub fn region_len(&self) -> Result<usize, MapError> {
        self.snapshot_len()?
            .checked_add(REGION_SNAPSHOT_LEN_SIZE)
            .ok_or(MapError::SerializationError)
    }

    pub fn encode_snapshot_into(&self, snapshot: &mut [u8]) -> Result<usize, MapError> {
        let snapshot_len = self.snapshot_len()?;
        if snapshot.len() < snapshot_len {
            return Err(MapError::BufferTooSmall);
        }

        let entry_count_bytes = self.record_count.0.to_le_bytes();
        snapshot[..SNAPSHOT_ENTRY_COUNT_SIZE].copy_from_slice(&entry_count_bytes);

        let entry_count =
            usize::try_from(self.record_count.0).map_err(|_| MapError::SerializationError)?;
        let refs_len = entry_count
            .checked_mul(ENTRY_REF_SIZE)
            .ok_or(MapError::SerializationError)?;
        let refs_staging_start = snapshot_len
            .checked_sub(refs_len)
            .ok_or(MapError::SerializationError)?;
        let mut entry_bytes_len = 0usize;
        let mut compact_offset = ENTRY_COUNT_SIZE;
        let entries_offset = SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE;
        let mut write_offset = entries_offset;

        for index in 0..entry_count {
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(index))?;
            let start = usize::from(entry_ref.start);
            let end = usize::from(entry_ref.end);
            let entry_len = end.checked_sub(start).ok_or(MapError::SerializationError)?;

            let next_write_offset = write_offset
                .checked_add(entry_len)
                .ok_or(MapError::SerializationError)?;
            if next_write_offset > refs_staging_start {
                return Err(MapError::BufferTooSmall);
            }
            snapshot[write_offset..next_write_offset].copy_from_slice(&self.map[start..end]);

            let compact_end = compact_offset
                .checked_add(entry_len)
                .ok_or(MapError::SerializationError)?;
            let compact_start_ref: RefType = compact_offset
                .try_into()
                .map_err(|_| MapError::SerializationError)?;
            let compact_end_ref: RefType = compact_end
                .try_into()
                .map_err(|_| MapError::SerializationError)?;
            let ref_offset = refs_staging_start + index * ENTRY_REF_SIZE;
            snapshot[ref_offset..ref_offset + ENTRY_REF_POINTER_SIZE]
                .copy_from_slice(&compact_start_ref.to_le_bytes());
            snapshot[ref_offset + ENTRY_REF_POINTER_SIZE..ref_offset + ENTRY_REF_SIZE]
                .copy_from_slice(&compact_end_ref.to_le_bytes());

            entry_bytes_len = entry_bytes_len
                .checked_add(entry_len)
                .ok_or(MapError::SerializationError)?;
            compact_offset = compact_end;
            write_offset = next_write_offset;
        }

        let entry_bytes_len_u32 =
            u32::try_from(entry_bytes_len).map_err(|_| MapError::SerializationError)?;
        let entry_bytes_len_offset = SNAPSHOT_ENTRY_COUNT_SIZE;
        snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
            .copy_from_slice(&entry_bytes_len_u32.to_le_bytes());
        snapshot.copy_within(refs_staging_start..snapshot_len, entries_offset + entry_bytes_len);

        Ok(snapshot_len)
    }

    pub fn encode_region_into(&self, region_payload: &mut [u8]) -> Result<usize, MapError> {
        let snapshot_len = self.snapshot_len()?;
        let region_len = self.region_len()?;
        if region_payload.len() < region_len {
            return Err(MapError::BufferTooSmall);
        }

        let snapshot_len_u32 =
            u32::try_from(snapshot_len).map_err(|_| MapError::SerializationError)?;
        region_payload[..REGION_SNAPSHOT_LEN_SIZE]
            .copy_from_slice(&snapshot_len_u32.to_le_bytes());
        self.encode_snapshot_into(
            &mut region_payload[REGION_SNAPSHOT_LEN_SIZE..REGION_SNAPSHOT_LEN_SIZE + snapshot_len],
        )?;
        Ok(region_len)
    }

    pub fn load_snapshot(&mut self, snapshot: &[u8]) -> Result<(), MapError> {
        if snapshot.len() < SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE {
            return Err(MapError::SerializationError);
        }

        let mut entry_count_bytes = [0u8; SNAPSHOT_ENTRY_COUNT_SIZE];
        entry_count_bytes.copy_from_slice(&snapshot[..SNAPSHOT_ENTRY_COUNT_SIZE]);
        let record_count = EntryCount::decode(entry_count_bytes);
        let entry_count =
            usize::try_from(record_count.0).map_err(|_| MapError::SerializationError)?;

        let mut entry_bytes_len_bytes = [0u8; SNAPSHOT_ENTRY_BYTES_LEN_SIZE];
        let entry_bytes_len_offset = SNAPSHOT_ENTRY_COUNT_SIZE;
        entry_bytes_len_bytes.copy_from_slice(
            &snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE],
        );
        let entry_bytes_len = usize::try_from(u32::from_le_bytes(entry_bytes_len_bytes))
            .map_err(|_| MapError::SerializationError)?;

        let expected_len = SNAPSHOT_ENTRY_COUNT_SIZE
            .checked_add(SNAPSHOT_ENTRY_BYTES_LEN_SIZE)
            .and_then(|len| len.checked_add(entry_bytes_len))
            .and_then(|len| len.checked_add(entry_count.checked_mul(ENTRY_REF_SIZE)?))
            .ok_or(MapError::SerializationError)?;
        if snapshot.len() != expected_len {
            return Err(MapError::SerializationError);
        }

        let next_record_offset = ENTRY_COUNT_SIZE
            .checked_add(entry_bytes_len)
            .ok_or(MapError::SerializationError)?;
        let refs_start = SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE + entry_bytes_len;
        let index_bytes = entry_count
            .checked_mul(ENTRY_REF_SIZE)
            .ok_or(MapError::SerializationError)?;
        let index_start = self
            .map
            .len()
            .checked_sub(index_bytes)
            .ok_or(MapError::IndexOutOfBounds)?;

        if next_record_offset > index_start {
            return Err(MapError::SnapshotTooLarge);
        }

        self.map.fill(0);
        record_count.write(self.map);
        self.map[ENTRY_COUNT_SIZE..next_record_offset]
            .copy_from_slice(&snapshot[SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE..refs_start]);
        for index in 0..entry_count {
            let ref_offset = refs_start + index * ENTRY_REF_SIZE;
            let mut start_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
            start_bytes.copy_from_slice(&snapshot[ref_offset..ref_offset + ENTRY_REF_POINTER_SIZE]);
            let start = usize::from(RefType::from_le_bytes(start_bytes));

            let mut end_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
            end_bytes.copy_from_slice(
                &snapshot[ref_offset + ENTRY_REF_POINTER_SIZE..ref_offset + ENTRY_REF_SIZE],
            );
            let end = usize::from(RefType::from_le_bytes(end_bytes));

            EntryRef::write(
                self.map,
                RecordIndex::new(index),
                RecordOffset(start),
                RecordOffset(end),
            )?;
        }

        self.record_count = record_count;
        self.next_record_offset = RecordOffset(next_record_offset);
        self.next_record_index = RecordIndex::new(entry_count);
        self.validate_loaded_state()?;
        Ok(())
    }

    pub fn load_region(&mut self, region_payload: &[u8]) -> Result<(), MapError> {
        if region_payload.len() < REGION_SNAPSHOT_LEN_SIZE {
            return Err(MapError::SerializationError);
        }

        let mut snapshot_len_bytes = [0u8; REGION_SNAPSHOT_LEN_SIZE];
        snapshot_len_bytes.copy_from_slice(&region_payload[..REGION_SNAPSHOT_LEN_SIZE]);
        let snapshot_len = usize::try_from(u32::from_le_bytes(snapshot_len_bytes))
            .map_err(|_| MapError::SerializationError)?;
        let snapshot_end = REGION_SNAPSHOT_LEN_SIZE
            .checked_add(snapshot_len)
            .ok_or(MapError::SerializationError)?;
        if snapshot_end > region_payload.len() {
            return Err(MapError::SerializationError);
        }

        self.load_snapshot(&region_payload[REGION_SNAPSHOT_LEN_SIZE..snapshot_end])
    }

    pub(crate) fn checkpoint_into(&self, scratch: &mut [u8]) -> Result<MapCheckpoint, MapError> {
        if scratch.len() < self.map.len() {
            return Err(MapError::BufferTooSmall);
        }

        scratch[..self.map.len()].copy_from_slice(self.map);
        Ok(MapCheckpoint {
            record_count: self.record_count.0,
            next_record_offset: self.next_record_offset.0,
            next_record_index: self.next_record_index.0,
        })
    }

    pub(crate) fn restore_from_checkpoint(
        &mut self,
        checkpoint: MapCheckpoint,
        scratch: &[u8],
    ) -> Result<(), MapError> {
        if scratch.len() < self.map.len() {
            return Err(MapError::BufferTooSmall);
        }

        self.map.copy_from_slice(&scratch[..self.map.len()]);
        self.record_count = EntryCount(checkpoint.record_count);
        self.next_record_offset = RecordOffset(checkpoint.next_record_offset);
        self.next_record_index = RecordIndex::new(checkpoint.next_record_index);
        Ok(())
    }

    pub(crate) fn compact_in_place(&mut self, snapshot: &mut [u8]) -> Result<(), MapError> {
        let snapshot_len = self.encode_snapshot_into(snapshot)?;
        self.load_snapshot(&snapshot[..snapshot_len])
    }

    pub fn encode_update_into(update: &MapUpdate<K, V>, payload: &mut [u8]) -> Result<usize, MapError> {
        Ok(to_slice(update, payload)?.len())
    }

    pub fn apply_update_payload(&mut self, payload: &[u8]) -> Result<(), MapError> {
        let update: MapUpdate<K, V> = from_bytes(payload)?;
        match update {
            MapUpdate::Set { key, value } => self.set(key, value),
            MapUpdate::Delete { key } => self.delete(key),
        }
    }

    fn validate_loaded_state(&self) -> Result<(), MapError> {
        let entry_count =
            usize::try_from(self.record_count.0).map_err(|_| MapError::SerializationError)?;
        let mut previous_key: Option<K> = None;
        for index in 0..entry_count {
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(index))?;
            let start = usize::from(entry_ref.start);
            let end = usize::from(entry_ref.end);
            if start < ENTRY_COUNT_SIZE || start >= end || end > self.next_record_offset.0 {
                return Err(MapError::SerializationError);
            }
            for previous_index in 0..index {
                let previous_ref = EntryRef::read(self.map, RecordIndex::new(previous_index))?;
                let previous_start = usize::from(previous_ref.start);
                let previous_end = usize::from(previous_ref.end);
                if start < previous_end && previous_start < end {
                    return Err(MapError::SerializationError);
                }
            }

            let entry: Entry<K, V> = from_bytes(&self.map[start..end])?;
            if let Some(previous) = previous_key.as_ref() {
                if entry.key.cmp(previous) != core::cmp::Ordering::Greater {
                    return Err(MapError::SerializationError);
                }
            }
            previous_key = Some(entry.key);
        }
        Ok(())
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

        let mut low_index = 0;
        let mut high_index = self.next_record_index.0 - 1;
        while low_index <= high_index {
            // SAFETY: high - low will not under flow and mid will
            // alway be smaller then high.
            let mid = low_index + (high_index - low_index) / 2;

            let entry_ref = EntryRef::read(self.map, RecordIndex::new(mid))?;
            let entry: Entry<K, V> =
                from_bytes(&self.map[entry_ref.start as usize..entry_ref.end as usize])?;
            match key.cmp(&entry.key) {
                core::cmp::Ordering::Equal => return Ok(SearchResult::Found(RecordIndex(mid))),
                core::cmp::Ordering::Less => {
                    if mid == 0 {
                        return Ok(SearchResult::NotFound(RecordIndex(0)));
                    }
                    high_index = mid - 1
                }
                core::cmp::Ordering::Greater => low_index = mid + 1,
            }
        }

        Ok(SearchResult::NotFound(RecordIndex(low_index)))
    }

    pub fn write_snapshot_to_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), MapStorageError> {
        let mut snapshot = [0u8; REGION_SIZE];
        let used = self.encode_snapshot_into(&mut snapshot)?;
        storage.append_snapshot::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            CollectionType::MAP_CODE,
            &snapshot[..used],
        )?;
        Ok(())
    }

    pub fn flush_to_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<u32, MapStorageError> {
        //= spec/ring.md#collection-head-state-machine
        //# `RING-FORMAT-005` Every user collection MUST remain log-structured: flushing mutable state writes a new immutable committed region segment instead of rewriting an existing live region in place.
        let previous_region = storage
            .collections()
            .iter()
            .find(|collection| collection.collection_id() == self.id)
            .and_then(|collection| match collection.basis() {
                crate::StartupCollectionBasis::Region(region_index) => Some(region_index),
                _ => None,
            });
        let region_index =
            storage.reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
        {
            let (payload, _) = workspace.encode_buffers();
            let used = self.encode_region_into(payload)?;
            storage.write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                region_index,
                self.id,
                MAP_REGION_V1_FORMAT,
                &payload[..used],
            )?;
        }
        if let Some(previous_region) = previous_region {
            storage.append_reclaim_begin::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                previous_region,
            )?;
        }
        storage.append_head::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            CollectionType::MAP_CODE,
            region_index,
        )?;
        Ok(region_index)
    }

    pub fn open_from_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        storage: &StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        buffer: &'a mut [u8],
    ) -> Result<Self, MapStorageError> {
        //= spec/ring.md#collection-head-state-machine
        //# `RING-FORMAT-006` A `WALSnapshotHead` MUST be loadable into RAM before that collection accepts further mutations.
        //= spec/ring.md#collection-head-state-machine
        //# `RING-FORMAT-008` Every later retained type-bearing record for that collection MUST carry the same `collection_type`, otherwise replay must treat the mismatch as corruption.
        let Some(collection) = storage
            .collections()
            .iter()
            .find(|collection| collection.collection_id() == collection_id)
        else {
            return Err(MapStorageError::UnknownCollection(collection_id));
        };
        if collection.basis() == crate::StartupCollectionBasis::Dropped {
            return Err(MapStorageError::DroppedCollection(collection_id));
        }
        if collection.collection_type() != Some(CollectionType::MAP_CODE) {
            return Err(MapStorageError::CollectionTypeMismatch {
                collection_id,
                expected: CollectionType::MAP_CODE,
                actual: collection.collection_type(),
            });
        }

        let mut map = Self::new(collection_id, buffer)?;
        let target_basis = collection.basis();
        let mut basis_loaded = matches!(target_basis, crate::StartupCollectionBasis::Empty);
        let visit_result = storage.visit_wal_records::<REGION_SIZE, IO, _, _>(
            flash,
            workspace,
            |flash, record| -> Result<(), MapStorageError> {
                match record {
                    crate::WalRecord::NewCollection {
                        collection_id: record_collection_id,
                        collection_type,
                    } if record_collection_id == collection_id => {
                        if collection_type != CollectionType::MAP_CODE {
                            return Err(MapStorageError::CollectionTypeMismatch {
                                collection_id,
                                expected: CollectionType::MAP_CODE,
                                actual: Some(collection_type),
                            });
                        }
                    }
                    crate::WalRecord::Update {
                        collection_id: record_collection_id,
                        payload,
                    } if record_collection_id == collection_id => {
                        if basis_loaded {
                            map.apply_update_payload(payload)?;
                        }
                    }
                    crate::WalRecord::Snapshot {
                        collection_id: record_collection_id,
                        collection_type,
                        payload,
                    } if record_collection_id == collection_id => {
                        if collection_type != CollectionType::MAP_CODE {
                            return Err(MapStorageError::CollectionTypeMismatch {
                                collection_id,
                                expected: CollectionType::MAP_CODE,
                                actual: Some(collection_type),
                            });
                        }
                        if target_basis == crate::StartupCollectionBasis::WalSnapshot {
                            map.load_snapshot(payload)?;
                            basis_loaded = true;
                        } else {
                            basis_loaded = false;
                        }
                    }
                    crate::WalRecord::Head {
                        collection_id: record_collection_id,
                        collection_type,
                        region_index,
                    } if record_collection_id == collection_id => {
                        if collection_type != CollectionType::MAP_CODE {
                            return Err(MapStorageError::CollectionTypeMismatch {
                                collection_id,
                                expected: CollectionType::MAP_CODE,
                                actual: Some(collection_type),
                            });
                        }
                        if target_basis == crate::StartupCollectionBasis::Region(region_index) {
                            load_map_region_from_flash::<REGION_SIZE, IO, K, V, MAX_INDEXES>(
                                flash,
                                storage.metadata(),
                                collection_id,
                                region_index,
                                &mut map,
                            )?;
                            basis_loaded = true;
                        } else {
                            basis_loaded = false;
                        }
                    }
                    crate::WalRecord::DropCollection {
                        collection_id: record_collection_id,
                    } if record_collection_id == collection_id => {
                        return Err(MapStorageError::DroppedCollection(collection_id));
                    }
                    _ => {}
                }

                Ok(())
            },
        );

        match visit_result {
            Ok(()) => Ok(map),
            Err(StorageVisitError::Storage(error)) => Err(MapStorageError::Storage(error)),
            Err(StorageVisitError::Visitor(error)) => Err(error),
        }
    }
}

fn load_map_region_from_flash<
    const REGION_SIZE: usize,
    IO: FlashIo,
    K,
    V,
    const MAX_INDEXES: usize,
>(
    flash: &mut IO,
    metadata: crate::StorageMetadata,
    collection_id: CollectionId,
    region_index: u32,
    map: &mut LsmMap<'_, K, V, MAX_INDEXES>,
) -> Result<(), MapStorageError>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    //= spec/ring.md#collection-head-state-machine
    //# `RING-FORMAT-014` For non-WAL collections, the pair `(collection_type, collection_format)` MUST identify a unique committed region payload format.
    let mut region_bytes = [0u8; REGION_SIZE];
    flash.read_region(region_index, 0, &mut region_bytes)?;

    let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::UnknownCollection(collection_id));
    }
    if header.collection_format != MAP_REGION_V1_FORMAT {
        return Err(MapStorageError::UnsupportedRegionFormat {
            collection_id,
            region_index,
            actual: header.collection_format,
        });
    }

    let payload_end = usize::try_from(metadata.region_size)
        .map_err(|_| MapStorageError::Map(MapError::SerializationError))?
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(MapStorageError::Map(MapError::SerializationError))?;
    map.load_region(&region_bytes[Header::ENCODED_LEN..payload_end])?;
    Ok(())
}
