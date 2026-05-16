//! Durable map collection implementation and storage helpers.

use crate::disk::{DiskError, FreePointerFooter, Header};
use crate::flash_io::FlashIo;
use crate::mock::MockError;
use crate::storage::{StorageRuntime, StorageRuntimeError, StorageVisitError};
use crate::workspace::StorageWorkspace;
use crate::{CollectionId, CollectionType, StorageMetadata};
use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem::size_of;
use heapless::Vec;
use postcard::{from_bytes, to_slice};
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[allow(unused_mut, unused_variables)]
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

/// Errors returned by in-memory map encoding, decode, and mutation paths.
#[derive(Debug)]
pub enum MapError {
    /// The entry count header was invalid.
    InvalidEntryCount,
    /// Serialization or decode failed.
    SerializationError,
    /// An entry-ref index was outside the available range.
    IndexOutOfBounds,
    /// A decoded snapshot could not fit into the destination buffer.
    SnapshotTooLarge,
    /// The caller-provided buffer was too small.
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

/// Errors returned by public map key encoding helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsmKeyError {
    /// The destination buffer was too small for the encoded key.
    BufferTooSmall,
    /// Serialization or decode failed.
    SerializationError,
}

impl From<postcard::Error> for LsmKeyError {
    fn from(error: postcard::Error) -> Self {
        match error {
            postcard::Error::SerializeBufferFull => Self::BufferTooSmall,
            _ => Self::SerializationError,
        }
    }
}

/// Errors returned by public map value encoding helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsmValueError {
    /// The destination buffer was too small for the encoded value.
    BufferTooSmall,
    /// Serialization or decode failed.
    SerializationError,
}

impl From<postcard::Error> for LsmValueError {
    fn from(error: postcard::Error) -> Self {
        match error {
            postcard::Error::SerializeBufferFull => Self::BufferTooSmall,
            _ => Self::SerializationError,
        }
    }
}

/// Public key boundary for durable LSM maps.
///
/// The default implementation preserves the current postcard-encoded key
/// bytes while giving the API a named extension point for future multipart
/// keys and prefix matching.
pub trait LsmKey:
    Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>
{
    /// Encodes the key into `out` and returns the number of bytes written.
    fn encode_key(&self, out: &mut [u8]) -> Result<usize, LsmKeyError> {
        to_slice(self, out)
            .map(|encoded| encoded.len())
            .map_err(LsmKeyError::from)
    }

    /// Decodes a key from the current stable key bytes.
    fn decode_key(bytes: &[u8]) -> Result<Self, LsmKeyError>
    where
        Self: Sized,
    {
        from_bytes(bytes).map_err(LsmKeyError::from)
    }
}

impl<T> LsmKey for T where
    T: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>
{
}

/// Public value boundary for durable LSM maps.
///
/// The default implementation preserves the current postcard-encoded value
/// bytes while giving the API a named extension point for future validation.
pub trait LsmValue: Debug + Serialize + for<'de> Deserialize<'de> {
    /// Encodes the value into `out` and returns the number of bytes written.
    fn encode_value(&self, out: &mut [u8]) -> Result<usize, LsmValueError> {
        to_slice(self, out)
            .map(|encoded| encoded.len())
            .map_err(LsmValueError::from)
    }

    /// Decodes a value from the current stable value bytes.
    fn decode_value(bytes: &[u8]) -> Result<Self, LsmValueError>
    where
        Self: Sized,
    {
        from_bytes(bytes).map_err(LsmValueError::from)
    }
}

impl<T> LsmValue for T where T: Debug + Serialize + for<'de> Deserialize<'de> {}

/// Logical map entry stored in compacted snapshots and frontiers.
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
const ENTRY_REF_SIZE: usize = size_of::<[RefType; 2]>();
const SNAPSHOT_ENTRY_COUNT_SIZE: usize = size_of::<u32>();
const SNAPSHOT_ENTRY_BYTES_LEN_SIZE: usize = size_of::<u32>();
const REGION_SNAPSHOT_LEN_SIZE: usize = size_of::<u32>();

/// Stable committed-region format identifier for map regions.
pub const MAP_REGION_V1_FORMAT: u16 = 1;
/// Stable committed-region format identifier for map manifest regions.
pub const MAP_MANIFEST_V1_FORMAT: u16 = 2;
/// Stable committed-region format identifier for immutable map run segments.
pub const MAP_RUN_V1_FORMAT: u16 = 3;
/// Snapshot bytes representing an empty map basis.
pub const EMPTY_MAP_SNAPSHOT: [u8; SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE] =
    [0u8; SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE];

const RUN_GENERATION_SIZE: usize = size_of::<u64>();
const RUN_NEXT_REGION_SIZE: usize = size_of::<u32>();
const RUN_STATE_COUNT_SIZE: usize = size_of::<u32>();
const RUN_BOUND_LEN_SIZE: usize = size_of::<u32>();
const RUN_SNAPSHOT_LEN_SIZE: usize = size_of::<u32>();
const RUN_SEGMENT_FIXED_SIZE: usize = RUN_GENERATION_SIZE
    + RUN_NEXT_REGION_SIZE
    + RUN_STATE_COUNT_SIZE
    + RUN_BOUND_LEN_SIZE
    + RUN_BOUND_LEN_SIZE
    + RUN_SNAPSHOT_LEN_SIZE;
const NO_NEXT_RUN_REGION: u32 = u32::MAX;

#[derive(Debug, Clone, Copy)]
pub(crate) struct MapCheckpoint {
    record_count: u32,
    next_record_offset: usize,
    next_record_index: usize,
}

/// Errors returned while combining map operations with storage state.
#[derive(Debug)]
pub enum MapStorageError {
    /// Map-local encoding or validation failed.
    Map(MapError),
    /// Shared storage logic rejected the operation.
    Storage(StorageRuntimeError),
    /// The backing I/O adapter failed.
    Mock(MockError),
    /// A disk structure was invalid.
    Disk(DiskError),
    /// The named collection was not tracked.
    UnknownCollection(CollectionId),
    /// The named collection was already dropped.
    DroppedCollection(CollectionId),
    /// The tracked collection type was not a map.
    CollectionTypeMismatch {
        /// Collection being validated.
        collection_id: CollectionId,
        /// Expected map collection type code.
        expected: u16,
        /// Actual retained collection type.
        actual: Option<u16>,
    },
    /// The retained committed-region format was not supported.
    UnsupportedRegionFormat {
        /// Collection being opened.
        collection_id: CollectionId,
        /// Region whose header used the unsupported format.
        region_index: u32,
        /// Unsupported committed-region format code.
        actual: u16,
    },
    /// A manifest region was structurally invalid.
    InvalidManifest {
        /// Collection being read.
        collection_id: CollectionId,
        /// Manifest region being decoded.
        region_index: u32,
    },
    /// A run segment was structurally invalid.
    InvalidRun {
        /// Collection being read.
        collection_id: CollectionId,
        /// Run segment region being decoded.
        region_index: u32,
    },
    /// A manifest contained more runs than the caller configured.
    TooManyRuns {
        /// Collection being opened.
        collection_id: CollectionId,
        /// Maximum run descriptors available in the map handle.
        max_runs: usize,
    },
    /// A compaction target of zero regions is invalid.
    InvalidRegionTarget,
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

/// Error type returned by the public `LsmMap` object API.
pub type LsmMapError = MapStorageError;

/// Logical map mutation encoded into WAL update payloads.
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
    /// Sets `key` to `value`.
    Set {
        /// Key being updated.
        key: K,
        /// Value that should become visible for `key`.
        value: V,
    },
    /// Removes `key` from the logical map.
    Delete {
        /// Key that should become absent.
        key: K,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MapRunSource {
    LegacyRegion,
    RunChain,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MapRunDescriptor<K> {
    source: MapRunSource,
    generation: u64,
    first_region: u32,
    region_count: u32,
    approx_state_count: u32,
    lower_key: Option<K>,
    upper_key: Option<K>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentPlan {
    start_index: usize,
    entry_count: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum LookupResult<V> {
    NotFound,
    Deleted,
    Set(V),
}

impl<K> MapRunDescriptor<K>
where
    K: Ord,
{
    fn may_contain(&self, key: &K) -> bool {
        if let Some(lower_key) = self.lower_key.as_ref() {
            if key < lower_key {
                return false;
            }
        }
        if let Some(upper_key) = self.upper_key.as_ref() {
            if key > upper_key {
                return false;
            }
        }
        true
    }
}

fn read_u32(buffer: &[u8], offset: &mut usize) -> Result<u32, MapError> {
    let end = offset
        .checked_add(size_of::<u32>())
        .ok_or(MapError::SerializationError)?;
    if end > buffer.len() {
        return Err(MapError::SerializationError);
    }

    let mut bytes = [0u8; size_of::<u32>()];
    bytes.copy_from_slice(&buffer[*offset..end]);
    *offset = end;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(buffer: &[u8], offset: &mut usize) -> Result<u64, MapError> {
    let end = offset
        .checked_add(size_of::<u64>())
        .ok_or(MapError::SerializationError)?;
    if end > buffer.len() {
        return Err(MapError::SerializationError);
    }

    let mut bytes = [0u8; size_of::<u64>()];
    bytes.copy_from_slice(&buffer[*offset..end]);
    *offset = end;
    Ok(u64::from_le_bytes(bytes))
}

fn write_u32(buffer: &mut [u8], offset: &mut usize, value: u32) -> Result<(), MapError> {
    let end = offset
        .checked_add(size_of::<u32>())
        .ok_or(MapError::SerializationError)?;
    if end > buffer.len() {
        return Err(MapError::BufferTooSmall);
    }

    buffer[*offset..end].copy_from_slice(&value.to_le_bytes());
    *offset = end;
    Ok(())
}

fn write_u64(buffer: &mut [u8], offset: &mut usize, value: u64) -> Result<(), MapError> {
    let end = offset
        .checked_add(size_of::<u64>())
        .ok_or(MapError::SerializationError)?;
    if end > buffer.len() {
        return Err(MapError::BufferTooSmall);
    }

    buffer[*offset..end].copy_from_slice(&value.to_le_bytes());
    *offset = end;
    Ok(())
}

fn snapshot_parts(snapshot: &[u8]) -> Result<(usize, usize, usize, usize), MapError> {
    if snapshot.len() < SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE {
        return Err(MapError::SerializationError);
    }

    let mut entry_count_bytes = [0u8; SNAPSHOT_ENTRY_COUNT_SIZE];
    entry_count_bytes.copy_from_slice(&snapshot[..SNAPSHOT_ENTRY_COUNT_SIZE]);
    let entry_count = usize::try_from(u32::from_le_bytes(entry_count_bytes))
        .map_err(|_| MapError::SerializationError)?;

    let entry_bytes_len_offset = SNAPSHOT_ENTRY_COUNT_SIZE;
    let mut entry_bytes_len_bytes = [0u8; SNAPSHOT_ENTRY_BYTES_LEN_SIZE];
    entry_bytes_len_bytes.copy_from_slice(
        &snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE],
    );
    let entry_bytes_len = usize::try_from(u32::from_le_bytes(entry_bytes_len_bytes))
        .map_err(|_| MapError::SerializationError)?;

    let entries_offset = SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE;
    let refs_start = entries_offset
        .checked_add(entry_bytes_len)
        .ok_or(MapError::SerializationError)?;
    let expected_len = refs_start
        .checked_add(
            entry_count
                .checked_mul(ENTRY_REF_SIZE)
                .ok_or(MapError::SerializationError)?,
        )
        .ok_or(MapError::SerializationError)?;
    if snapshot.len() != expected_len {
        return Err(MapError::SerializationError);
    }

    Ok((entry_count, entry_bytes_len, entries_offset, refs_start))
}

fn snapshot_entry_ref(snapshot: &[u8], index: usize) -> Result<EntryRef, MapError> {
    let (entry_count, _, _, refs_start) = snapshot_parts(snapshot)?;
    if index >= entry_count {
        return Err(MapError::IndexOutOfBounds);
    }

    let ref_offset = refs_start
        .checked_add(
            index
                .checked_mul(ENTRY_REF_SIZE)
                .ok_or(MapError::SerializationError)?,
        )
        .ok_or(MapError::SerializationError)?;
    let mut start_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
    start_bytes.copy_from_slice(&snapshot[ref_offset..ref_offset + ENTRY_REF_POINTER_SIZE]);
    let mut end_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
    end_bytes.copy_from_slice(
        &snapshot[ref_offset + ENTRY_REF_POINTER_SIZE..ref_offset + ENTRY_REF_SIZE],
    );
    Ok(EntryRef {
        start: RefType::from_le_bytes(start_bytes),
        end: RefType::from_le_bytes(end_bytes),
    })
}

fn snapshot_entry_bytes(snapshot: &[u8], index: usize) -> Result<&[u8], MapError> {
    let (_, entry_bytes_len, entries_offset, _) = snapshot_parts(snapshot)?;
    let entry_ref = snapshot_entry_ref(snapshot, index)?;
    let compact_start = usize::from(entry_ref.start);
    let compact_end = usize::from(entry_ref.end);
    if compact_start < ENTRY_COUNT_SIZE {
        return Err(MapError::SerializationError);
    }
    if compact_start >= compact_end {
        return Err(MapError::SerializationError);
    }

    let start = entries_offset
        .checked_add(
            compact_start
                .checked_sub(ENTRY_COUNT_SIZE)
                .ok_or(MapError::SerializationError)?,
        )
        .ok_or(MapError::SerializationError)?;
    let end = entries_offset
        .checked_add(
            compact_end
                .checked_sub(ENTRY_COUNT_SIZE)
                .ok_or(MapError::SerializationError)?,
        )
        .ok_or(MapError::SerializationError)?;
    if end > entries_offset + entry_bytes_len {
        return Err(MapError::SerializationError);
    }
    if start >= end {
        return Err(MapError::SerializationError);
    }
    Ok(&snapshot[start..end])
}

fn snapshot_entry<K, V>(snapshot: &[u8], index: usize) -> Result<Entry<K, V>, MapError>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    Ok(from_bytes(snapshot_entry_bytes(snapshot, index)?)?)
}

fn midpoint_index(low_index: usize, high_exclusive: usize) -> Result<usize, MapError> {
    let width = high_exclusive
        .checked_sub(low_index)
        .ok_or(MapError::SerializationError)?;
    let half_width = width.checked_div(2).ok_or(MapError::SerializationError)?;
    low_index
        .checked_add(half_width)
        .ok_or(MapError::SerializationError)
}

fn lookup_snapshot<K, V>(snapshot: &[u8], key: &K) -> Result<LookupResult<V>, MapError>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    let (entry_count, _, _, _) = snapshot_parts(snapshot)?;
    if entry_count == 0 {
        return Ok(LookupResult::NotFound);
    }

    let mut low_index = 0usize;
    let mut high_index = entry_count;
    while low_index < high_index {
        let mid = midpoint_index(low_index, high_index)?;
        if mid < low_index {
            return Err(MapError::SerializationError);
        }
        if mid >= high_index {
            return Err(MapError::SerializationError);
        }
        let entry: Entry<K, V> = snapshot_entry(snapshot, mid)?;
        match key.cmp(&entry.key) {
            core::cmp::Ordering::Equal => {
                return Ok(match entry.value {
                    Some(value) => LookupResult::Set(value),
                    None => LookupResult::Deleted,
                });
            }
            core::cmp::Ordering::Less => high_index = mid,
            core::cmp::Ordering::Greater => {
                low_index = mid.checked_add(1).ok_or(MapError::SerializationError)?;
            }
        }
    }

    Ok(LookupResult::NotFound)
}

fn snapshot_range_len(
    snapshot: &[u8],
    start_index: usize,
    entry_count: usize,
) -> Result<usize, MapError> {
    let (available, _, _, _) = snapshot_parts(snapshot)?;
    let end_index = start_index
        .checked_add(entry_count)
        .ok_or(MapError::SerializationError)?;
    if start_index > available {
        return Err(MapError::IndexOutOfBounds);
    }
    if end_index > available {
        return Err(MapError::IndexOutOfBounds);
    }

    let mut entry_bytes_len = 0usize;
    for index in start_index..end_index {
        entry_bytes_len = entry_bytes_len
            .checked_add(snapshot_entry_bytes(snapshot, index)?.len())
            .ok_or(MapError::SerializationError)?;
    }

    SNAPSHOT_ENTRY_COUNT_SIZE
        .checked_add(SNAPSHOT_ENTRY_BYTES_LEN_SIZE)
        .and_then(|len| len.checked_add(entry_bytes_len))
        .and_then(|len| len.checked_add(entry_count.checked_mul(ENTRY_REF_SIZE)?))
        .ok_or(MapError::SerializationError)
}

fn encode_snapshot_range_from_snapshot_into(
    source: &[u8],
    start_index: usize,
    entry_count: usize,
    snapshot: &mut [u8],
) -> Result<usize, MapError> {
    let snapshot_len = snapshot_range_len(source, start_index, entry_count)?;
    if snapshot.len() < snapshot_len {
        return Err(MapError::BufferTooSmall);
    }

    let entry_count_u32 = u32::try_from(entry_count).map_err(|_| MapError::SerializationError)?;
    snapshot[..SNAPSHOT_ENTRY_COUNT_SIZE].copy_from_slice(&entry_count_u32.to_le_bytes());

    let refs_len = entry_count
        .checked_mul(ENTRY_REF_SIZE)
        .ok_or(MapError::SerializationError)?;
    let refs_start = snapshot_len
        .checked_sub(refs_len)
        .ok_or(MapError::SerializationError)?;
    let entries_offset = SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE;
    let mut write_offset = entries_offset;
    let mut compact_offset = ENTRY_COUNT_SIZE;

    for (target_index, source_index) in (start_index..start_index + entry_count).enumerate() {
        let entry = snapshot_entry_bytes(source, source_index)?;
        let entry_len = entry.len();
        let next_write_offset = write_offset
            .checked_add(entry_len)
            .ok_or(MapError::SerializationError)?;
        if next_write_offset > refs_start {
            return Err(MapError::BufferTooSmall);
        }
        snapshot[write_offset..next_write_offset].copy_from_slice(entry);

        let compact_end = compact_offset
            .checked_add(entry_len)
            .ok_or(MapError::SerializationError)?;
        let compact_start_ref: RefType = compact_offset
            .try_into()
            .map_err(|_| MapError::SerializationError)?;
        let compact_end_ref: RefType = compact_end
            .try_into()
            .map_err(|_| MapError::SerializationError)?;
        let ref_offset = refs_start + target_index * ENTRY_REF_SIZE;
        snapshot[ref_offset..ref_offset + ENTRY_REF_POINTER_SIZE]
            .copy_from_slice(&compact_start_ref.to_le_bytes());
        snapshot[ref_offset + ENTRY_REF_POINTER_SIZE..ref_offset + ENTRY_REF_SIZE]
            .copy_from_slice(&compact_end_ref.to_le_bytes());

        compact_offset = compact_end;
        write_offset = next_write_offset;
    }

    let entry_bytes_len = write_offset
        .checked_sub(entries_offset)
        .ok_or(MapError::SerializationError)?;
    let entry_bytes_len_u32 =
        u32::try_from(entry_bytes_len).map_err(|_| MapError::SerializationError)?;
    snapshot[SNAPSHOT_ENTRY_COUNT_SIZE..SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
        .copy_from_slice(&entry_bytes_len_u32.to_le_bytes());

    Ok(snapshot_len)
}

fn encode_snapshot_from_entries_into<K, V>(
    entries: &[Entry<K, V>],
    snapshot: &mut [u8],
) -> Result<usize, MapError>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize,
    V: Debug + Serialize,
{
    let entry_count = entries.len();
    let refs_len = entry_count
        .checked_mul(ENTRY_REF_SIZE)
        .ok_or(MapError::SerializationError)?;
    let entries_offset = SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE;
    let temp_refs_start = snapshot
        .len()
        .checked_sub(refs_len)
        .ok_or(MapError::BufferTooSmall)?;
    if temp_refs_start < entries_offset {
        return Err(MapError::BufferTooSmall);
    }

    let mut write_offset = entries_offset;
    let mut compact_offset = ENTRY_COUNT_SIZE;
    for (index, entry) in entries.iter().enumerate() {
        let used = to_slice(entry, &mut snapshot[write_offset..temp_refs_start])?.len();
        let next_write_offset = write_offset
            .checked_add(used)
            .ok_or(MapError::SerializationError)?;
        let compact_end = compact_offset
            .checked_add(used)
            .ok_or(MapError::SerializationError)?;
        let compact_start_ref: RefType = compact_offset
            .try_into()
            .map_err(|_| MapError::SerializationError)?;
        let compact_end_ref: RefType = compact_end
            .try_into()
            .map_err(|_| MapError::SerializationError)?;

        let ref_offset = temp_refs_start
            .checked_add(
                index
                    .checked_mul(ENTRY_REF_SIZE)
                    .ok_or(MapError::SerializationError)?,
            )
            .ok_or(MapError::SerializationError)?;
        snapshot[ref_offset..ref_offset + ENTRY_REF_POINTER_SIZE]
            .copy_from_slice(&compact_start_ref.to_le_bytes());
        snapshot[ref_offset + ENTRY_REF_POINTER_SIZE..ref_offset + ENTRY_REF_SIZE]
            .copy_from_slice(&compact_end_ref.to_le_bytes());

        compact_offset = compact_end;
        write_offset = next_write_offset;
    }

    let entry_bytes_len = write_offset
        .checked_sub(entries_offset)
        .ok_or(MapError::SerializationError)?;
    let refs_start = write_offset;
    let snapshot_len = refs_start
        .checked_add(refs_len)
        .ok_or(MapError::SerializationError)?;
    if refs_start != temp_refs_start {
        snapshot.copy_within(temp_refs_start..temp_refs_start + refs_len, refs_start);
    }

    let entry_count_u32 = u32::try_from(entry_count).map_err(|_| MapError::SerializationError)?;
    snapshot[..SNAPSHOT_ENTRY_COUNT_SIZE].copy_from_slice(&entry_count_u32.to_le_bytes());
    let entry_bytes_len_u32 =
        u32::try_from(entry_bytes_len).map_err(|_| MapError::SerializationError)?;
    snapshot[SNAPSHOT_ENTRY_COUNT_SIZE..SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
        .copy_from_slice(&entry_bytes_len_u32.to_le_bytes());

    Ok(snapshot_len)
}

struct RunSegmentView<'a> {
    generation: u64,
    next_region: Option<u32>,
    lower_key: &'a [u8],
    upper_key: &'a [u8],
    snapshot: &'a [u8],
}

fn parse_run_segment_payload(payload: &[u8]) -> Result<RunSegmentView<'_>, MapError> {
    let mut offset = 0usize;
    let generation = read_u64(payload, &mut offset)?;
    let next_region_raw = read_u32(payload, &mut offset)?;
    let next_region = if next_region_raw == NO_NEXT_RUN_REGION {
        None
    } else {
        Some(next_region_raw)
    };
    let state_count = read_u32(payload, &mut offset)?;
    let lower_key_len = usize::try_from(read_u32(payload, &mut offset)?)
        .map_err(|_| MapError::SerializationError)?;
    let upper_key_len = usize::try_from(read_u32(payload, &mut offset)?)
        .map_err(|_| MapError::SerializationError)?;
    let snapshot_len = usize::try_from(read_u32(payload, &mut offset)?)
        .map_err(|_| MapError::SerializationError)?;

    let lower_key_end = offset
        .checked_add(lower_key_len)
        .ok_or(MapError::SerializationError)?;
    let upper_key_end = lower_key_end
        .checked_add(upper_key_len)
        .ok_or(MapError::SerializationError)?;
    let snapshot_end = upper_key_end
        .checked_add(snapshot_len)
        .ok_or(MapError::SerializationError)?;
    if snapshot_end > payload.len() {
        return Err(MapError::SerializationError);
    }

    let lower_key = &payload[offset..lower_key_end];
    let upper_key = &payload[lower_key_end..upper_key_end];
    let snapshot = &payload[upper_key_end..snapshot_end];
    let (entry_count, _, _, _) = snapshot_parts(snapshot)?;
    if usize::try_from(state_count).map_err(|_| MapError::SerializationError)? != entry_count {
        return Err(MapError::SerializationError);
    }

    Ok(RunSegmentView {
        generation,
        next_region,
        lower_key,
        upper_key,
        snapshot,
    })
}

fn read_committed_region<'a, const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
    region_bytes: &'a mut [u8; REGION_SIZE],
) -> Result<(Header, &'a [u8]), MapStorageError> {
    flash.read_region(region_index, 0, region_bytes)?;
    let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
    let payload_end = usize::try_from(metadata.region_size)
        .map_err(|_| MapStorageError::Map(MapError::SerializationError))?
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(MapStorageError::Map(MapError::SerializationError))?;
    if payload_end > region_bytes.len() {
        return Err(MapStorageError::Map(MapError::SerializationError));
    }
    if payload_end < Header::ENCODED_LEN {
        return Err(MapStorageError::Map(MapError::SerializationError));
    }
    Ok((header, &region_bytes[Header::ENCODED_LEN..payload_end]))
}

fn committed_payload_capacity<const REGION_SIZE: usize>() -> Result<usize, MapError> {
    REGION_SIZE
        .checked_sub(Header::ENCODED_LEN)
        .and_then(|remaining| remaining.checked_sub(FreePointerFooter::ENCODED_LEN))
        .ok_or(MapError::SerializationError)
}

fn committed_payload_buffer<const REGION_SIZE: usize>(
    payload: &mut [u8],
) -> Result<&mut [u8], MapError> {
    let capacity = committed_payload_capacity::<REGION_SIZE>()?;
    payload.get_mut(..capacity).ok_or(MapError::BufferTooSmall)
}

fn ensure_manifest_run_capacity<const MAX_RUNS: usize>(
    collection_id: CollectionId,
    manifest_run_count: usize,
) -> Result<(), MapStorageError> {
    match manifest_run_count.checked_sub(MAX_RUNS) {
        Some(0) | None => Ok(()),
        Some(_) => Err(MapStorageError::TooManyRuns {
            collection_id,
            max_runs: MAX_RUNS,
        }),
    }
}

fn legacy_snapshot_from_payload(region_payload: &[u8]) -> Result<&[u8], MapError> {
    if region_payload.get(..REGION_SNAPSHOT_LEN_SIZE).is_none() {
        return Err(MapError::SerializationError);
    }

    let mut offset = 0usize;
    let snapshot_len = usize::try_from(read_u32(region_payload, &mut offset)?)
        .map_err(|_| MapError::SerializationError)?;
    let snapshot_end = offset
        .checked_add(snapshot_len)
        .ok_or(MapError::SerializationError)?;
    if snapshot_end > region_payload.len() {
        return Err(MapError::SerializationError);
    }
    let snapshot = &region_payload[offset..snapshot_end];
    snapshot_parts(snapshot)?;
    Ok(snapshot)
}

fn encode_manifest_descriptor<K>(
    manifest_payload: &mut [u8],
    offset: &mut usize,
    run: &MapRunDescriptor<K>,
) -> Result<(), MapError>
where
    K: Serialize,
{
    write_u64(manifest_payload, offset, run.generation)?;
    write_u32(manifest_payload, offset, run.first_region)?;
    write_u32(manifest_payload, offset, run.region_count)?;
    write_u32(manifest_payload, offset, run.approx_state_count)?;

    let lower_len_offset = *offset;
    write_u32(manifest_payload, offset, 0)?;
    let upper_len_offset = *offset;
    write_u32(manifest_payload, offset, 0)?;

    let lower_len = if let Some(key) = run.lower_key.as_ref() {
        let used = to_slice(key, &mut manifest_payload[*offset..])?.len();
        *offset = (*offset)
            .checked_add(used)
            .ok_or(MapError::SerializationError)?;
        used
    } else {
        0
    };
    let upper_len = if let Some(key) = run.upper_key.as_ref() {
        let used = to_slice(key, &mut manifest_payload[*offset..])?.len();
        *offset = (*offset)
            .checked_add(used)
            .ok_or(MapError::SerializationError)?;
        used
    } else {
        0
    };

    let lower_len_u32 = u32::try_from(lower_len).map_err(|_| MapError::SerializationError)?;
    let upper_len_u32 = u32::try_from(upper_len).map_err(|_| MapError::SerializationError)?;
    manifest_payload[lower_len_offset..lower_len_offset + size_of::<u32>()]
        .copy_from_slice(&lower_len_u32.to_le_bytes());
    manifest_payload[upper_len_offset..upper_len_offset + size_of::<u32>()]
        .copy_from_slice(&upper_len_u32.to_le_bytes());
    Ok(())
}

fn encode_run_segment_with_snapshot_writer<K, F>(
    run_payload: &mut [u8],
    generation: u64,
    next_region: Option<u32>,
    entry_count: usize,
    lower_key: &K,
    upper_key: &K,
    write_snapshot: F,
) -> Result<usize, MapError>
where
    K: Serialize,
    F: FnOnce(&mut [u8]) -> Result<usize, MapError>,
{
    if run_payload.get(..RUN_SEGMENT_FIXED_SIZE).is_none() {
        return Err(MapError::BufferTooSmall);
    }

    let mut offset = RUN_SEGMENT_FIXED_SIZE;
    let lower_len = to_slice(lower_key, &mut run_payload[offset..])?.len();
    offset = offset
        .checked_add(lower_len)
        .ok_or(MapError::SerializationError)?;
    let upper_len = to_slice(upper_key, &mut run_payload[offset..])?.len();
    offset = offset
        .checked_add(upper_len)
        .ok_or(MapError::SerializationError)?;
    let snapshot_len = write_snapshot(&mut run_payload[offset..])?;
    let used = offset
        .checked_add(snapshot_len)
        .ok_or(MapError::SerializationError)?;

    let mut header_offset = 0usize;
    write_u64(run_payload, &mut header_offset, generation)?;
    write_u32(
        run_payload,
        &mut header_offset,
        next_region.unwrap_or(NO_NEXT_RUN_REGION),
    )?;
    write_u32(
        run_payload,
        &mut header_offset,
        u32::try_from(entry_count).map_err(|_| MapError::SerializationError)?,
    )?;
    write_u32(
        run_payload,
        &mut header_offset,
        u32::try_from(lower_len).map_err(|_| MapError::SerializationError)?,
    )?;
    write_u32(
        run_payload,
        &mut header_offset,
        u32::try_from(upper_len).map_err(|_| MapError::SerializationError)?,
    )?;
    write_u32(
        run_payload,
        &mut header_offset,
        u32::try_from(snapshot_len).map_err(|_| MapError::SerializationError)?,
    )?;
    Ok(used)
}

fn encode_run_segment_from_entries_into<K, V>(
    run_payload: &mut [u8],
    generation: u64,
    next_region: Option<u32>,
    entries: &[Entry<K, V>],
) -> Result<usize, MapError>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize,
    V: Debug + Serialize,
{
    let lower = entries.first().ok_or(MapError::SerializationError)?;
    let upper = entries.last().ok_or(MapError::SerializationError)?;
    encode_run_segment_with_snapshot_writer(
        run_payload,
        generation,
        next_region,
        entries.len(),
        &lower.key,
        &upper.key,
        |snapshot| encode_snapshot_from_entries_into(entries, snapshot),
    )
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
        let buf = &mut buffer[offset + ENTRY_REF_POINTER_SIZE..offset + ENTRY_REF_SIZE];
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

        buf.copy_from_slice(&buffer[offset + ENTRY_REF_POINTER_SIZE..offset + ENTRY_REF_SIZE]);
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

/// Small durable map handle used by the public object-level API.
pub struct LsmMap<K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize = MAX_INDEXES> {
    pub(crate) collection_id: CollectionId,
    pub(crate) compaction_region_target: usize,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize> LsmMap<K, V, MAX_INDEXES, MAX_RUNS> {
    pub(crate) const fn from_collection_id(
        collection_id: CollectionId,
        compaction_region_target: usize,
    ) -> Self {
        Self {
            collection_id,
            compaction_region_target,
            _phantom: PhantomData,
        }
    }

    /// Returns the stable collection id for this durable map.
    pub fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
}

/// Caller-owned bounded map frontier used by advanced storage helpers.
pub struct MapFrontier<'a, K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize = MAX_INDEXES> {
    id: CollectionId,
    record_count: EntryCount,
    next_record_offset: RecordOffset,
    next_record_index: RecordIndex,
    map: &'a mut [u8],
    runs: Vec<MapRunDescriptor<K>, MAX_RUNS>,
    _phantom: PhantomData<(K, V)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunChainOrder {
    Ascending,
    Descending,
}

struct RunEntryCursor<K, V>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq,
    V: Debug,
{
    generation: u64,
    first_region: u32,
    region_count: u32,
    order: Option<RunChainOrder>,
    next_segment_position: Option<u32>,
    active_region: Option<u32>,
    active_position: Option<u32>,
    entry_index: usize,
    current: Option<Entry<K, V>>,
}

impl<K, V> RunEntryCursor<K, V>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    fn new(run: &MapRunDescriptor<K>) -> Result<Self, MapStorageError> {
        if run.source != MapRunSource::RunChain || run.region_count == 0 {
            return Err(MapStorageError::Map(MapError::SerializationError));
        }

        Ok(Self {
            generation: run.generation,
            first_region: run.first_region,
            region_count: run.region_count,
            order: None,
            next_segment_position: None,
            active_region: None,
            active_position: None,
            entry_index: 0,
            current: None,
        })
    }

    fn advance<const REGION_SIZE: usize, IO: FlashIo>(
        &mut self,
        collection_id: CollectionId,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), MapStorageError> {
        self.current = None;
        self.ensure_order::<REGION_SIZE, IO>(collection_id, flash, workspace)?;

        loop {
            let region_index = match self.active_region {
                Some(region_index) => region_index,
                None => {
                    let Some(position) = self.next_segment_position else {
                        return Ok(());
                    };
                    let region_index = self.region_at_position::<REGION_SIZE, IO>(
                        collection_id,
                        flash,
                        workspace,
                        position,
                    )?;
                    self.active_region = Some(region_index);
                    self.active_position = Some(position);
                    self.entry_index = 0;
                    region_index
                }
            };

            let (region_bytes, _) = workspace.scan_buffers();
            flash.read_region(region_index, 0, region_bytes)?;
            let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
            if header.collection_id != collection_id {
                return Err(MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                });
            }
            if header.collection_format != MAP_RUN_V1_FORMAT {
                return Err(MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                });
            }
            let payload_end = REGION_SIZE
                .checked_sub(FreePointerFooter::ENCODED_LEN)
                .ok_or(MapStorageError::Map(MapError::SerializationError))?;
            let view = parse_run_segment_payload(&region_bytes[Header::ENCODED_LEN..payload_end])
                .map_err(|_| MapStorageError::InvalidRun {
                collection_id,
                region_index,
            })?;
            if view.generation != self.generation {
                return Err(MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                });
            }

            let (entry_count, _, _, _) = snapshot_parts(view.snapshot)?;
            if entry_count == 0 {
                return Err(MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                });
            }
            if self.entry_index < entry_count {
                self.current = Some(snapshot_entry(view.snapshot, self.entry_index)?);
                self.entry_index = self
                    .entry_index
                    .checked_add(1)
                    .ok_or(MapError::SerializationError)?;
                return Ok(());
            }

            self.advance_segment_position()?;
            self.active_region = None;
            self.active_position = None;
            self.entry_index = 0;
        }
    }

    fn ensure_order<const REGION_SIZE: usize, IO: FlashIo>(
        &mut self,
        collection_id: CollectionId,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), MapStorageError> {
        if self.order.is_some() {
            return Ok(());
        }

        if self.region_count == 1 {
            self.order = Some(RunChainOrder::Ascending);
            self.next_segment_position = Some(0);
            return Ok(());
        }

        let first_region =
            self.region_at_position::<REGION_SIZE, IO>(collection_id, flash, workspace, 0)?;
        let second_region =
            self.region_at_position::<REGION_SIZE, IO>(collection_id, flash, workspace, 1)?;
        let (first_lower, first_upper) = read_run_segment_bounds::<K, REGION_SIZE, IO>(
            collection_id,
            self.generation,
            flash,
            workspace,
            first_region,
        )?;
        let (second_lower, second_upper) = read_run_segment_bounds::<K, REGION_SIZE, IO>(
            collection_id,
            self.generation,
            flash,
            workspace,
            second_region,
        )?;

        if first_upper <= second_lower {
            self.order = Some(RunChainOrder::Ascending);
            self.next_segment_position = Some(0);
        } else if second_upper <= first_lower {
            self.order = Some(RunChainOrder::Descending);
            self.next_segment_position = Some(
                self.region_count
                    .checked_sub(1)
                    .ok_or(MapError::SerializationError)?,
            );
        } else {
            return Err(MapStorageError::InvalidRun {
                collection_id,
                region_index: self.first_region,
            });
        }

        Ok(())
    }

    fn advance_segment_position(&mut self) -> Result<(), MapStorageError> {
        let active_position = self.active_position.ok_or(MapError::SerializationError)?;
        self.next_segment_position = match self.order.ok_or(MapError::SerializationError)? {
            RunChainOrder::Ascending => {
                let next = active_position
                    .checked_add(1)
                    .ok_or(MapError::SerializationError)?;
                if next < self.region_count {
                    Some(next)
                } else {
                    None
                }
            }
            RunChainOrder::Descending => active_position.checked_sub(1),
        };
        Ok(())
    }

    fn region_at_position<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        collection_id: CollectionId,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        position: u32,
    ) -> Result<u32, MapStorageError> {
        if position >= self.region_count {
            return Err(MapStorageError::InvalidRun {
                collection_id,
                region_index: self.first_region,
            });
        }

        let mut region_index = self.first_region;
        for _ in 0..position {
            region_index = read_run_segment_next_region::<REGION_SIZE, IO>(
                collection_id,
                self.generation,
                flash,
                workspace,
                region_index,
            )?
            .ok_or(MapStorageError::InvalidRun {
                collection_id,
                region_index,
            })?;
        }

        Ok(region_index)
    }
}

struct CompactionRunWriter<K, V, const MAX_INDEXES: usize>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq,
    V: Debug,
{
    generation: u64,
    next_region: Option<u32>,
    first_region: Option<u32>,
    lowest_region: Option<u32>,
    region_count: u32,
    state_count: u32,
    segment_entries: Vec<Entry<K, V>, MAX_INDEXES>,
}

impl<K, V, const MAX_INDEXES: usize> CompactionRunWriter<K, V, MAX_INDEXES>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    fn new(generation: u64) -> Self {
        Self {
            generation,
            next_region: None,
            first_region: None,
            lowest_region: None,
            region_count: 0,
            state_count: 0,
            segment_entries: Vec::new(),
        }
    }

    fn push<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &mut self,
        collection_id: CollectionId,
        storage: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        entry: Entry<K, V>,
    ) -> Result<(), MapStorageError> {
        match self.segment_entries.push(entry) {
            Ok(()) => {
                if self.segment_entries_fit(workspace)? {
                    self.increment_state_count()?;
                    return Ok(());
                }

                let entry = self
                    .segment_entries
                    .pop()
                    .ok_or(MapError::SerializationError)?;
                self.flush_segment::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(collection_id, storage, flash, workspace)?;
                self.segment_entries
                    .push(entry)
                    .map_err(|_| MapError::BufferTooSmall)?;
                match self.segment_entries_fit(workspace)? {
                    true => {}
                    false => return Err(MapStorageError::Map(MapError::BufferTooSmall)),
                }
                self.increment_state_count()
            }
            Err(entry) => {
                self.flush_segment::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(collection_id, storage, flash, workspace)?;
                self.segment_entries
                    .push(entry)
                    .map_err(|_| MapError::BufferTooSmall)?;
                match self.segment_entries_fit(workspace)? {
                    true => {}
                    false => return Err(MapStorageError::Map(MapError::BufferTooSmall)),
                }
                self.increment_state_count()
            }
        }
    }

    fn finish<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        mut self,
        collection_id: CollectionId,
        storage: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<Option<MapRunDescriptor<K>>, MapStorageError> {
        self.flush_segment::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
            collection_id,
            storage,
            flash,
            workspace,
        )?;

        let Some(first_region) = self.first_region else {
            return Ok(None);
        };
        let lowest_region = self.lowest_region.ok_or(MapError::SerializationError)?;
        let (lower_key, _) = read_run_segment_bounds::<K, REGION_SIZE, IO>(
            collection_id,
            self.generation,
            flash,
            workspace,
            lowest_region,
        )?;
        let (_, upper_key) = read_run_segment_bounds::<K, REGION_SIZE, IO>(
            collection_id,
            self.generation,
            flash,
            workspace,
            first_region,
        )?;

        Ok(Some(MapRunDescriptor {
            source: MapRunSource::RunChain,
            generation: self.generation,
            first_region,
            region_count: self.region_count,
            approx_state_count: self.state_count,
            lower_key: Some(lower_key),
            upper_key: Some(upper_key),
        }))
    }

    fn segment_entries_fit<const REGION_SIZE: usize>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<bool, MapError> {
        let (payload, _) = workspace.encode_buffers();
        let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
        match encode_run_segment_from_entries_into(
            payload,
            self.generation,
            self.next_region,
            self.segment_entries.as_slice(),
        ) {
            Ok(_) => Ok(true),
            Err(MapError::BufferTooSmall) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn flush_segment<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &mut self,
        collection_id: CollectionId,
        storage: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), MapStorageError> {
        if self.segment_entries.is_empty() {
            return Ok(());
        }

        if let Some(region_index) = storage.last_free_list_head() {
            storage
                .ensure_stage_region_append_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    region_index,
                )?;
        }
        let region_index =
            storage.reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
        {
            let (payload, _) = workspace.encode_buffers();
            let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
            let used = encode_run_segment_from_entries_into(
                payload,
                self.generation,
                self.next_region,
                self.segment_entries.as_slice(),
            )?;
            storage.write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                region_index,
                collection_id,
                MAP_RUN_V1_FORMAT,
                &payload[..used],
            )?;
        }
        storage.stage_ready_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            region_index,
        )?;

        if self.lowest_region.is_none() {
            self.lowest_region = Some(region_index);
        }
        self.next_region = Some(region_index);
        self.first_region = Some(region_index);
        self.region_count = self
            .region_count
            .checked_add(1)
            .ok_or(MapError::SerializationError)?;
        self.segment_entries.clear();
        Ok(())
    }

    fn increment_state_count(&mut self) -> Result<(), MapStorageError> {
        self.state_count = self
            .state_count
            .checked_add(1)
            .ok_or(MapError::SerializationError)?;
        Ok(())
    }
}

fn read_run_segment_bounds<K, const REGION_SIZE: usize, IO: FlashIo>(
    collection_id: CollectionId,
    generation: u64,
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    region_index: u32,
) -> Result<(K, K), MapStorageError>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + for<'de> Deserialize<'de>,
{
    let (region_bytes, _) = workspace.scan_buffers();
    flash.read_region(region_index, 0, region_bytes)?;
    let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::InvalidRun {
            collection_id,
            region_index,
        });
    }
    if header.collection_format != MAP_RUN_V1_FORMAT {
        return Err(MapStorageError::InvalidRun {
            collection_id,
            region_index,
        });
    }
    let payload_end = REGION_SIZE
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(MapStorageError::Map(MapError::SerializationError))?;
    let view = parse_run_segment_payload(&region_bytes[Header::ENCODED_LEN..payload_end]).map_err(
        |_| MapStorageError::InvalidRun {
            collection_id,
            region_index,
        },
    )?;
    if view.generation != generation {
        return Err(MapStorageError::InvalidRun {
            collection_id,
            region_index,
        });
    }

    Ok((
        from_bytes(view.lower_key).map_err(MapError::from)?,
        from_bytes(view.upper_key).map_err(MapError::from)?,
    ))
}

fn read_run_segment_next_region<const REGION_SIZE: usize, IO: FlashIo>(
    collection_id: CollectionId,
    generation: u64,
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    region_index: u32,
) -> Result<Option<u32>, MapStorageError> {
    let (region_bytes, _) = workspace.scan_buffers();
    flash.read_region(region_index, 0, region_bytes)?;
    let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::InvalidRun {
            collection_id,
            region_index,
        });
    }
    if header.collection_format != MAP_RUN_V1_FORMAT {
        return Err(MapStorageError::InvalidRun {
            collection_id,
            region_index,
        });
    }
    let payload_end = REGION_SIZE
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(MapStorageError::Map(MapError::SerializationError))?;
    let view = parse_run_segment_payload(&region_bytes[Header::ENCODED_LEN..payload_end]).map_err(
        |_| MapStorageError::InvalidRun {
            collection_id,
            region_index,
        },
    )?;
    if view.generation != generation {
        return Err(MapStorageError::InvalidRun {
            collection_id,
            region_index,
        });
    }

    Ok(view.next_region)
}

impl<'a, K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize>
    MapFrontier<'a, K, V, MAX_INDEXES, MAX_RUNS>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    /// Creates a new empty map frontier over `buffer`.
    pub fn new(id: CollectionId, buffer: &'a mut [u8]) -> Result<Self, MapError> {
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
            runs: Vec::new(),
            _phantom,
        })
    }

    /// Returns the collection id represented by this frontier.
    pub fn id(&self) -> CollectionId {
        self.id
    }

    /// Returns the number of retained lower layers after open or compaction.
    pub fn layer_count(&self) -> usize {
        self.runs.len()
    }

    /// Returns the number of manifest or legacy run descriptors tracked by this handle.
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    /// Inserts or replaces a key with the supplied value.
    pub fn set(&mut self, key: K, value: V) -> Result<(), MapError>
    where
        K: Ord + PartialOrd + Eq + PartialEq + Serialize + for<'d> Deserialize<'d>,
        V: Serialize + for<'d> Deserialize<'d>,
    {
        self.set_worker(key, Some(value))
    }

    /// Deletes a key from the logical map.
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

    /// Returns the current frontier value for `key`, without consulting durable runs.
    ///
    /// A `None` result can mean either no frontier entry exists for the key or the
    /// newest frontier entry is a delete tombstone. Use [`Self::get`] for full
    /// storage-backed map visibility.
    pub fn get_frontier(&self, key: &K) -> Result<Option<V>, MapError> {
        match self.lookup_frontier(key)? {
            LookupResult::NotFound | LookupResult::Deleted => Ok(None),
            LookupResult::Set(value) => Ok(Some(value)),
        }
    }

    fn lookup_frontier(&self, key: &K) -> Result<LookupResult<V>, MapError> {
        let search_result = self.find_index(key)?;
        match search_result {
            SearchResult::NotFound(_) => Ok(LookupResult::NotFound),
            SearchResult::Found(index) => {
                let entry_ref = EntryRef::read(self.map, index)?;
                let entry: Entry<K, V> =
                    from_bytes(&self.map[entry_ref.start as usize..entry_ref.end as usize])?;
                Ok(match entry.value {
                    Some(value) => LookupResult::Set(value),
                    None => LookupResult::Deleted,
                })
            }
        }
    }

    /// Returns the current visible value for `key`, reading durable runs on demand.
    pub fn get<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        key: &K,
    ) -> Result<Option<V>, MapStorageError> {
        match self.lookup_frontier(key)? {
            LookupResult::Set(value) => return Ok(Some(value)),
            LookupResult::Deleted => return Ok(None),
            LookupResult::NotFound => {}
        }

        for run in self.runs.iter() {
            if !run.may_contain(key) {
                continue;
            }

            match self.lookup_run::<REGION_SIZE, IO>(flash, workspace, run, key)? {
                LookupResult::Set(value) => return Ok(Some(value)),
                LookupResult::Deleted => return Ok(None),
                LookupResult::NotFound => {}
            }
        }

        Ok(None)
    }

    fn clear_frontier(&mut self) {
        self.record_count = EntryCount(0);
        self.next_record_offset = RecordOffset(ENTRY_COUNT_SIZE);
        self.next_record_index = RecordIndex(0);
        self.map.fill(0);
        self.record_count.write(self.map);
    }

    fn lookup_run<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        run: &MapRunDescriptor<K>,
        key: &K,
    ) -> Result<LookupResult<V>, MapStorageError> {
        match run.source {
            MapRunSource::LegacyRegion => self.lookup_legacy_region::<REGION_SIZE, IO>(
                flash,
                workspace,
                run.first_region,
                key,
            ),
            MapRunSource::RunChain => {
                self.lookup_run_chain::<REGION_SIZE, IO>(flash, workspace, run, key)
            }
        }
    }

    fn lookup_legacy_region<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
        key: &K,
    ) -> Result<LookupResult<V>, MapStorageError> {
        let (region_bytes, _) = workspace.scan_buffers();
        flash.read_region(region_index, 0, region_bytes)?;
        let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
        if header.collection_id != self.id {
            return Err(MapStorageError::InvalidRun {
                collection_id: self.id,
                region_index,
            });
        }
        if header.collection_format != MAP_REGION_V1_FORMAT {
            return Err(MapStorageError::InvalidRun {
                collection_id: self.id,
                region_index,
            });
        }
        let payload_end = REGION_SIZE
            .checked_sub(FreePointerFooter::ENCODED_LEN)
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        let payload = &region_bytes[Header::ENCODED_LEN..payload_end];
        let snapshot = legacy_snapshot_from_payload(payload)?;
        Ok(lookup_snapshot(snapshot, key)?)
    }

    fn lookup_run_chain<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        run: &MapRunDescriptor<K>,
        key: &K,
    ) -> Result<LookupResult<V>, MapStorageError> {
        let mut current_region = Some(run.first_region);
        for _ in 0..run.region_count {
            let region_index = current_region.ok_or(MapStorageError::InvalidRun {
                collection_id: self.id,
                region_index: run.first_region,
            })?;
            let (region_bytes, _) = workspace.scan_buffers();
            flash.read_region(region_index, 0, region_bytes)?;
            let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
            if header.collection_id != self.id {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index,
                });
            }
            if header.collection_format != MAP_RUN_V1_FORMAT {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index,
                });
            }
            let payload_end = REGION_SIZE
                .checked_sub(FreePointerFooter::ENCODED_LEN)
                .ok_or(MapStorageError::Map(MapError::SerializationError))?;
            let view = parse_run_segment_payload(&region_bytes[Header::ENCODED_LEN..payload_end])
                .map_err(|_| MapStorageError::InvalidRun {
                collection_id: self.id,
                region_index,
            })?;
            if view.generation != run.generation {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index,
                });
            }

            let lower_key: K = from_bytes(view.lower_key).map_err(MapError::from)?;
            let upper_key: K = from_bytes(view.upper_key).map_err(MapError::from)?;
            if key < &lower_key {
                current_region = view.next_region;
                continue;
            }
            if key > &upper_key {
                current_region = view.next_region;
                continue;
            }
            match lookup_snapshot(view.snapshot, key)? {
                LookupResult::NotFound => {}
                result => return Ok(result),
            }
            current_region = view.next_region;
        }

        Ok(LookupResult::NotFound)
    }

    pub(crate) fn live_run_region_count(&self) -> Result<usize, MapError> {
        let mut count = 0usize;
        for run in self.runs.iter() {
            if run.source == MapRunSource::RunChain {
                count = count
                    .checked_add(
                        usize::try_from(run.region_count)
                            .map_err(|_| MapError::SerializationError)?,
                    )
                    .ok_or(MapError::SerializationError)?;
            }
        }
        Ok(count)
    }

    pub(crate) fn frontier_is_empty(&self) -> bool {
        self.record_count.0 == 0
    }

    pub(crate) fn selected_compaction_run_count(
        &self,
        region_target: usize,
    ) -> Result<Option<usize>, MapError> {
        let total_regions = self.live_run_region_count()?;
        if total_regions <= region_target {
            return Ok(None);
        }

        let mut selected_runs = 0usize;
        let mut selected_regions = 0usize;
        let mut accumulated_states = 0u64;
        for run in self.runs.iter() {
            if run.source != MapRunSource::RunChain {
                break;
            }
            selected_runs = selected_runs
                .checked_add(1)
                .ok_or(MapError::SerializationError)?;
            selected_regions = selected_regions
                .checked_add(
                    usize::try_from(run.region_count).map_err(|_| MapError::SerializationError)?,
                )
                .ok_or(MapError::SerializationError)?;
            accumulated_states = accumulated_states
                .checked_add(u64::from(run.approx_state_count))
                .ok_or(MapError::SerializationError)?;

            let estimated_regions_after_compaction = total_regions
                .checked_sub(selected_regions)
                .and_then(|count| count.checked_add(1))
                .ok_or(MapError::SerializationError)?;
            if estimated_regions_after_compaction <= region_target {
                break;
            }
        }

        if selected_runs == 0 {
            return Ok(None);
        }

        for run in self.runs.iter().skip(selected_runs) {
            if run.source != MapRunSource::RunChain {
                break;
            }
            let run_states = u64::from(run.approx_state_count);
            if run_states >= accumulated_states {
                break;
            }
            selected_runs = selected_runs
                .checked_add(1)
                .ok_or(MapError::SerializationError)?;
            accumulated_states = accumulated_states
                .checked_add(run_states)
                .ok_or(MapError::SerializationError)?;
        }

        Ok(Some(selected_runs))
    }

    pub(crate) fn selected_compaction_state_count(
        &self,
        selected_runs: usize,
    ) -> Result<u32, MapError> {
        if selected_runs > self.runs.len() {
            return Err(MapError::IndexOutOfBounds);
        }

        let mut state_count = 0u32;
        for run in self.runs.iter().take(selected_runs) {
            if run.source != MapRunSource::RunChain {
                return Err(MapError::SerializationError);
            }
            state_count = state_count
                .checked_add(run.approx_state_count)
                .ok_or(MapError::SerializationError)?;
        }
        Ok(state_count)
    }

    pub(crate) fn write_compacted_run_to_storage<
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
        selected_runs: usize,
    ) -> Result<Option<MapRunDescriptor<K>>, MapStorageError> {
        if selected_runs == 0 {
            return Ok(None);
        }
        if selected_runs > self.runs.len() {
            return Err(MapStorageError::Map(MapError::IndexOutOfBounds));
        }

        let mut cursors = Vec::<RunEntryCursor<K, V>, MAX_RUNS>::new();
        for run in self.runs.iter().take(selected_runs) {
            let mut cursor = RunEntryCursor::new(run)?;
            cursor.advance::<REGION_SIZE, IO>(self.id, flash, workspace)?;
            cursors
                .push(cursor)
                .map_err(|_| MapStorageError::TooManyRuns {
                    collection_id: self.id,
                    max_runs: MAX_RUNS,
                })?;
        }

        let mut writer = CompactionRunWriter::<K, V, MAX_INDEXES>::new(self.next_run_generation());
        loop {
            let mut min_index: Option<usize> = None;
            for index in 0..cursors.len() {
                let Some(entry) = cursors[index].current.as_ref() else {
                    continue;
                };
                let should_replace = match min_index {
                    Some(current_min) => {
                        let min_entry = cursors[current_min]
                            .current
                            .as_ref()
                            .ok_or(MapError::SerializationError)?;
                        entry.key.cmp(&min_entry.key) == core::cmp::Ordering::Less
                    }
                    None => true,
                };
                if should_replace {
                    min_index = Some(index);
                }
            }

            let Some(min_index) = min_index else {
                break;
            };
            let mut duplicate_indices = Vec::<usize, MAX_RUNS>::new();
            for index in 0..cursors.len() {
                let same_key = {
                    let min_key = &cursors[min_index]
                        .current
                        .as_ref()
                        .ok_or(MapError::SerializationError)?
                        .key;
                    cursors[index]
                        .current
                        .as_ref()
                        .is_some_and(|entry| entry.key.eq(min_key))
                };
                if same_key {
                    duplicate_indices
                        .push(index)
                        .map_err(|_| MapStorageError::TooManyRuns {
                            collection_id: self.id,
                            max_runs: MAX_RUNS,
                        })?;
                }
            }

            let mut winning_entry = None;
            for index in duplicate_indices.iter().copied() {
                let entry = cursors[index]
                    .current
                    .take()
                    .ok_or(MapError::SerializationError)?;
                if index == min_index {
                    winning_entry = Some(entry);
                }
                cursors[index].advance::<REGION_SIZE, IO>(self.id, flash, workspace)?;
            }
            let winning_entry = winning_entry.ok_or(MapError::SerializationError)?;
            writer.push::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
                self.id,
                storage,
                flash,
                workspace,
                winning_entry,
            )?;
        }

        writer.finish::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
            self.id, storage, flash, workspace,
        )
    }

    pub(crate) fn move_unselected_runs_into(
        &mut self,
        selected_runs: usize,
        target: &mut Self,
    ) -> Result<(), MapStorageError> {
        while self.runs.len() > selected_runs {
            let run = self.runs.remove(selected_runs);
            if run.source == MapRunSource::RunChain {
                target
                    .runs
                    .push(run)
                    .map_err(|_| MapStorageError::TooManyRuns {
                        collection_id: self.id,
                        max_runs: MAX_RUNS,
                    })?;
            }
        }
        Ok(())
    }

    pub(crate) fn push_retained_run(
        &mut self,
        run: MapRunDescriptor<K>,
    ) -> Result<(), MapStorageError> {
        self.runs
            .push(run)
            .map_err(|_| MapStorageError::TooManyRuns {
                collection_id: self.id,
                max_runs: MAX_RUNS,
            })
    }

    pub(crate) fn reclaim_run_regions<
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
        for run in self.runs.iter() {
            match run.source {
                MapRunSource::LegacyRegion => continue,
                MapRunSource::RunChain => {}
            }
            let mut current_region = Some(run.first_region);
            for _ in 0..run.region_count {
                let region_index = current_region.ok_or(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index: run.first_region,
                })?;
                let mut run_region = [0u8; REGION_SIZE];
                let (header, payload) = read_committed_region::<REGION_SIZE, IO>(
                    flash,
                    storage.metadata(),
                    region_index,
                    &mut run_region,
                )?;
                if header.collection_id != self.id {
                    return Err(MapStorageError::InvalidRun {
                        collection_id: self.id,
                        region_index,
                    });
                }
                if header.collection_format != MAP_RUN_V1_FORMAT {
                    return Err(MapStorageError::InvalidRun {
                        collection_id: self.id,
                        region_index,
                    });
                }
                let view = parse_run_segment_payload(payload).map_err(|_| {
                    MapStorageError::InvalidRun {
                        collection_id: self.id,
                        region_index,
                    }
                })?;
                current_region = view.next_region;

                storage.append_reclaim_begin_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    region_index,
                )?;
                if storage.pending_reclaims().contains(&region_index) {
                    storage.complete_pending_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        flash,
                        workspace,
                        region_index,
                    )?;
                }
            }
        }
        Ok(())
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

    /// Returns the encoded byte length of a snapshot payload for this map.
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

    /// Returns the encoded byte length of a committed-region payload for this map.
    pub fn region_len(&self) -> Result<usize, MapError> {
        self.snapshot_len()?
            .checked_add(REGION_SNAPSHOT_LEN_SIZE)
            .ok_or(MapError::SerializationError)
    }

    /// Encodes a compact snapshot payload into `snapshot`.
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
        snapshot.copy_within(
            refs_staging_start..snapshot_len,
            entries_offset + entry_bytes_len,
        );

        Ok(snapshot_len)
    }

    fn snapshot_range_len_from_frontier(
        &self,
        start_index: usize,
        entry_count: usize,
    ) -> Result<usize, MapError> {
        let available =
            usize::try_from(self.record_count.0).map_err(|_| MapError::SerializationError)?;
        let end_index = start_index
            .checked_add(entry_count)
            .ok_or(MapError::SerializationError)?;
        if start_index > available {
            return Err(MapError::IndexOutOfBounds);
        }
        if end_index > available {
            return Err(MapError::IndexOutOfBounds);
        }

        let mut entry_bytes_len = 0usize;
        for index in start_index..end_index {
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

    fn encode_snapshot_range_into(
        &self,
        start_index: usize,
        entry_count: usize,
        snapshot: &mut [u8],
    ) -> Result<usize, MapError> {
        let snapshot_len = self.snapshot_range_len_from_frontier(start_index, entry_count)?;
        if snapshot.len() < snapshot_len {
            return Err(MapError::BufferTooSmall);
        }

        let entry_count_u32 =
            u32::try_from(entry_count).map_err(|_| MapError::SerializationError)?;
        snapshot[..SNAPSHOT_ENTRY_COUNT_SIZE].copy_from_slice(&entry_count_u32.to_le_bytes());

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

        for (target_index, source_index) in (start_index..start_index + entry_count).enumerate() {
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(source_index))?;
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
            let ref_offset = refs_staging_start + target_index * ENTRY_REF_SIZE;
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
        snapshot
            [SNAPSHOT_ENTRY_COUNT_SIZE..SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
            .copy_from_slice(&entry_bytes_len_u32.to_le_bytes());
        snapshot.copy_within(
            refs_staging_start..snapshot_len,
            entries_offset + entry_bytes_len,
        );

        Ok(snapshot_len)
    }

    fn frontier_entry(&self, index: usize) -> Result<Entry<K, V>, MapError> {
        let entry_ref = EntryRef::read(self.map, RecordIndex::new(index))?;
        Ok(from_bytes(
            &self.map[usize::from(entry_ref.start)..usize::from(entry_ref.end)],
        )?)
    }

    /// Encodes a committed-region payload into `region_payload`.
    pub fn encode_region_into(&self, region_payload: &mut [u8]) -> Result<usize, MapError> {
        let snapshot_len = self.snapshot_len()?;
        let region_len = self.region_len()?;
        if region_payload.len() < region_len {
            return Err(MapError::BufferTooSmall);
        }

        let snapshot_len_u32 =
            u32::try_from(snapshot_len).map_err(|_| MapError::SerializationError)?;
        region_payload[..REGION_SNAPSHOT_LEN_SIZE].copy_from_slice(&snapshot_len_u32.to_le_bytes());
        self.encode_snapshot_into(
            &mut region_payload[REGION_SNAPSHOT_LEN_SIZE..REGION_SNAPSHOT_LEN_SIZE + snapshot_len],
        )?;
        Ok(region_len)
    }

    /// Loads a compact snapshot payload into this frontier.
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
            &snapshot
                [entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE],
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
        let refs_start =
            SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE + entry_bytes_len;
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
        self.map[ENTRY_COUNT_SIZE..next_record_offset].copy_from_slice(
            &snapshot[SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE..refs_start],
        );
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

    /// Loads a committed-region payload into this frontier.
    pub fn load_region(&mut self, region_payload: &[u8]) -> Result<(), MapError> {
        if region_payload.get(..REGION_SNAPSHOT_LEN_SIZE).is_none() {
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

    fn load_manifest_descriptors(
        &mut self,
        manifest_payload: &[u8],
        collection_id: CollectionId,
        manifest_region: u32,
    ) -> Result<(), MapStorageError> {
        self.runs.clear();
        let mut offset = 0usize;
        let run_count =
            usize::try_from(read_u32(manifest_payload, &mut offset)?).map_err(|_| {
                MapStorageError::InvalidManifest {
                    collection_id,
                    region_index: manifest_region,
                }
            })?;
        if run_count > MAX_RUNS {
            return Err(MapStorageError::TooManyRuns {
                collection_id,
                max_runs: MAX_RUNS,
            });
        }

        for _ in 0..run_count {
            let generation = read_u64(manifest_payload, &mut offset)?;
            let first_region = read_u32(manifest_payload, &mut offset)?;
            let region_count = read_u32(manifest_payload, &mut offset)?;
            let approx_state_count = read_u32(manifest_payload, &mut offset)?;
            let lower_key_len =
                usize::try_from(read_u32(manifest_payload, &mut offset)?).map_err(|_| {
                    MapStorageError::InvalidManifest {
                        collection_id,
                        region_index: manifest_region,
                    }
                })?;
            let upper_key_len =
                usize::try_from(read_u32(manifest_payload, &mut offset)?).map_err(|_| {
                    MapStorageError::InvalidManifest {
                        collection_id,
                        region_index: manifest_region,
                    }
                })?;
            if region_count == 0 {
                return Err(MapStorageError::InvalidManifest {
                    collection_id,
                    region_index: first_region,
                });
            }

            let lower_end =
                offset
                    .checked_add(lower_key_len)
                    .ok_or(MapStorageError::InvalidManifest {
                        collection_id,
                        region_index: first_region,
                    })?;
            let upper_end =
                lower_end
                    .checked_add(upper_key_len)
                    .ok_or(MapStorageError::InvalidManifest {
                        collection_id,
                        region_index: first_region,
                    })?;
            if upper_end > manifest_payload.len() {
                return Err(MapStorageError::InvalidManifest {
                    collection_id,
                    region_index: first_region,
                });
            }

            let lower_key = if lower_key_len == 0 {
                None
            } else {
                Some(from_bytes(&manifest_payload[offset..lower_end]).map_err(MapError::from)?)
            };
            let upper_key = if upper_key_len == 0 {
                None
            } else {
                Some(from_bytes(&manifest_payload[lower_end..upper_end]).map_err(MapError::from)?)
            };
            offset = upper_end;

            self.runs
                .push(MapRunDescriptor {
                    source: MapRunSource::RunChain,
                    generation,
                    first_region,
                    region_count,
                    approx_state_count,
                    lower_key,
                    upper_key,
                })
                .map_err(|_| MapStorageError::TooManyRuns {
                    collection_id,
                    max_runs: MAX_RUNS,
                })?;
        }

        Ok(())
    }

    fn encode_manifest_into(
        &self,
        manifest_payload: &mut [u8],
        extra_newest: Option<&MapRunDescriptor<K>>,
        extra_older: Option<&MapRunDescriptor<K>>,
    ) -> Result<usize, MapError> {
        let mut prior_run_count = 0usize;
        for run in self.runs.iter() {
            match run.source {
                MapRunSource::RunChain => {
                    prior_run_count = prior_run_count
                        .checked_add(1)
                        .ok_or(MapError::SerializationError)?;
                }
                MapRunSource::LegacyRegion => {}
            }
        }
        let run_count = prior_run_count
            .checked_add(usize::from(extra_newest.is_some()))
            .and_then(|count| count.checked_add(usize::from(extra_older.is_some())))
            .ok_or(MapError::SerializationError)?;
        let run_count_u32 = u32::try_from(run_count).map_err(|_| MapError::SerializationError)?;

        let mut offset = 0usize;
        write_u32(manifest_payload, &mut offset, run_count_u32)?;
        if let Some(run) = extra_newest {
            encode_manifest_descriptor(manifest_payload, &mut offset, run)?;
        }
        if let Some(run) = extra_older {
            encode_manifest_descriptor(manifest_payload, &mut offset, run)?;
        }
        for run in self.runs.iter() {
            match run.source {
                MapRunSource::RunChain => {
                    encode_manifest_descriptor(manifest_payload, &mut offset, run)?;
                }
                MapRunSource::LegacyRegion => {}
            }
        }
        Ok(offset)
    }

    fn encode_run_segment_from_frontier_into(
        &self,
        run_payload: &mut [u8],
        generation: u64,
        next_region: Option<u32>,
        start_index: usize,
        entry_count: usize,
    ) -> Result<usize, MapError> {
        let lower: Entry<K, V> = self.frontier_entry(start_index)?;
        let upper: Entry<K, V> = self.frontier_entry(start_index + entry_count - 1)?;
        encode_run_segment_with_snapshot_writer(
            run_payload,
            generation,
            next_region,
            entry_count,
            &lower.key,
            &upper.key,
            |snapshot| self.encode_snapshot_range_into(start_index, entry_count, snapshot),
        )
    }

    fn encode_run_segment_from_snapshot_into(
        run_payload: &mut [u8],
        generation: u64,
        next_region: Option<u32>,
        source: &[u8],
        start_index: usize,
        entry_count: usize,
    ) -> Result<usize, MapError> {
        let lower: Entry<K, V> = snapshot_entry(source, start_index)?;
        let upper: Entry<K, V> = snapshot_entry(source, start_index + entry_count - 1)?;
        encode_run_segment_with_snapshot_writer(
            run_payload,
            generation,
            next_region,
            entry_count,
            &lower.key,
            &upper.key,
            |snapshot| {
                encode_snapshot_range_from_snapshot_into(source, start_index, entry_count, snapshot)
            },
        )
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

    /// Encodes a map update payload into `payload`.
    pub fn encode_update_into(
        update: &MapUpdate<K, V>,
        payload: &mut [u8],
    ) -> Result<usize, MapError> {
        Ok(to_slice(update, payload)?.len())
    }

    /// Applies an encoded update payload to this frontier.
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
            if start < ENTRY_COUNT_SIZE {
                return Err(MapError::SerializationError);
            }
            if start >= end {
                return Err(MapError::SerializationError);
            }
            if end > self.next_record_offset.0 {
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

    fn find_index(&self, key: &K) -> Result<SearchResult, MapError> {
        let entry_count =
            usize::try_from(self.record_count.0).map_err(|_| MapError::SerializationError)?;
        let mut low_index = 0usize;
        let mut high_index = entry_count;
        while low_index < high_index {
            let mid = midpoint_index(low_index, high_index)?;
            if mid < low_index {
                return Err(MapError::SerializationError);
            }
            if mid >= high_index {
                return Err(MapError::SerializationError);
            }
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(mid))?;
            let entry: Entry<K, V> =
                from_bytes(&self.map[entry_ref.start as usize..entry_ref.end as usize])?;
            match key.cmp(&entry.key) {
                core::cmp::Ordering::Equal => return Ok(SearchResult::Found(RecordIndex(mid))),
                core::cmp::Ordering::Less => high_index = mid,
                core::cmp::Ordering::Greater => {
                    low_index = mid.checked_add(1).ok_or(MapError::SerializationError)?;
                }
            }
        }

        Ok(SearchResult::NotFound(RecordIndex(low_index)))
    }

    /// Writes this frontier as a WAL snapshot for its backing collection.
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

    pub(crate) fn next_run_generation(&self) -> u64 {
        self.runs
            .iter()
            .map(|run| run.generation)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
    }

    fn largest_frontier_segment_ending_at(
        &self,
        scratch: &mut [u8],
        generation: u64,
        end_index: usize,
    ) -> Result<SegmentPlan, MapError> {
        let mut entry_count = end_index;
        loop {
            if entry_count == 0 {
                return Err(MapError::BufferTooSmall);
            }
            let start_index = end_index
                .checked_sub(entry_count)
                .ok_or(MapError::SerializationError)?;
            match self.encode_run_segment_from_frontier_into(
                scratch,
                generation,
                None,
                start_index,
                entry_count,
            ) {
                Ok(_) => {
                    return Ok(SegmentPlan {
                        start_index,
                        entry_count,
                    });
                }
                Err(MapError::BufferTooSmall) => {
                    entry_count = entry_count
                        .checked_sub(1)
                        .ok_or(MapError::SerializationError)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn largest_snapshot_segment_ending_at(
        scratch: &mut [u8],
        generation: u64,
        source: &[u8],
        end_index: usize,
    ) -> Result<SegmentPlan, MapError> {
        let mut entry_count = end_index;
        loop {
            if entry_count == 0 {
                return Err(MapError::BufferTooSmall);
            }
            let start_index = end_index
                .checked_sub(entry_count)
                .ok_or(MapError::SerializationError)?;
            match Self::encode_run_segment_from_snapshot_into(
                scratch,
                generation,
                None,
                source,
                start_index,
                entry_count,
            ) {
                Ok(_) => {
                    return Ok(SegmentPlan {
                        start_index,
                        entry_count,
                    });
                }
                Err(MapError::BufferTooSmall) => {
                    entry_count = entry_count
                        .checked_sub(1)
                        .ok_or(MapError::SerializationError)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn planned_frontier_run_region_count<const REGION_SIZE: usize>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        generation: u64,
    ) -> Result<u32, MapError> {
        let entry_count =
            usize::try_from(self.record_count.0).map_err(|_| MapError::SerializationError)?;
        let mut region_count = 0u32;
        let mut end_index = entry_count;
        while end_index > 0 {
            let plan = {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                self.largest_frontier_segment_ending_at(payload, generation, end_index)?
            };
            region_count = region_count
                .checked_add(1)
                .ok_or(MapError::SerializationError)?;
            end_index = plan.start_index;
        }
        Ok(region_count)
    }

    fn planned_snapshot_run_region_count<const REGION_SIZE: usize>(
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        source: &[u8],
        generation: u64,
    ) -> Result<u32, MapError> {
        let (entry_count, _, _, _) = snapshot_parts(source)?;
        let mut region_count = 0u32;
        let mut end_index = entry_count;
        while end_index > 0 {
            let plan = {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                Self::largest_snapshot_segment_ending_at(payload, generation, source, end_index)?
            };
            region_count = region_count
                .checked_add(1)
                .ok_or(MapError::SerializationError)?;
            end_index = plan.start_index;
        }
        Ok(region_count)
    }

    pub(crate) fn write_frontier_run_to_storage<
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
        generation: u64,
    ) -> Result<Option<MapRunDescriptor<K>>, MapStorageError> {
        let entry_count =
            usize::try_from(self.record_count.0).map_err(|_| MapError::SerializationError)?;
        if entry_count == 0 {
            return Ok(None);
        }

        let lower = self.frontier_entry(0)?;
        let upper = self.frontier_entry(entry_count - 1)?;
        let mut next_region = None;
        let mut first_region = None;
        let mut region_count = 0u32;
        let mut end_index = entry_count;

        while end_index > 0 {
            let plan = {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                self.largest_frontier_segment_ending_at(payload, generation, end_index)?
            };
            if let Some(region_index) = storage.last_free_list_head() {
                storage
                    .ensure_stage_region_append_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        flash,
                        workspace,
                        region_index,
                    )?;
            }
            let region_index =
                storage.reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
            {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                let used = self.encode_run_segment_from_frontier_into(
                    payload,
                    generation,
                    next_region,
                    plan.start_index,
                    plan.entry_count,
                )?;
                storage.write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    region_index,
                    self.id,
                    MAP_RUN_V1_FORMAT,
                    &payload[..used],
                )?;
            }
            storage.stage_ready_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                region_index,
            )?;
            next_region = Some(region_index);
            first_region = Some(region_index);
            region_count = region_count
                .checked_add(1)
                .ok_or(MapError::SerializationError)?;
            end_index = plan.start_index;
        }

        Ok(Some(MapRunDescriptor {
            source: MapRunSource::RunChain,
            generation,
            first_region: first_region.ok_or(MapError::SerializationError)?,
            region_count,
            approx_state_count: u32::try_from(entry_count)
                .map_err(|_| MapError::SerializationError)?,
            lower_key: Some(lower.key),
            upper_key: Some(upper.key),
        }))
    }

    fn write_snapshot_run_to_storage<
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
        source: &[u8],
        generation: u64,
    ) -> Result<Option<MapRunDescriptor<K>>, MapStorageError> {
        let (entry_count, _, _, _) = snapshot_parts(source)?;
        let Some(upper_index) = entry_count.checked_sub(1) else {
            return Ok(None);
        };

        let lower: Entry<K, V> = snapshot_entry(source, 0)?;
        let upper: Entry<K, V> = snapshot_entry(source, upper_index)?;
        let mut next_region = None;
        let mut first_region = None;
        let mut region_count = 0u32;
        let mut end_index = entry_count;

        loop {
            if end_index == 0 {
                break;
            }
            let plan = {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                Self::largest_snapshot_segment_ending_at(payload, generation, source, end_index)?
            };
            if let Some(region_index) = storage.last_free_list_head() {
                storage
                    .ensure_stage_region_append_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        flash,
                        workspace,
                        region_index,
                    )?;
            }
            let region_index =
                storage.reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
            {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                let used = Self::encode_run_segment_from_snapshot_into(
                    payload,
                    generation,
                    next_region,
                    source,
                    plan.start_index,
                    plan.entry_count,
                )?;
                storage.write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    region_index,
                    self.id,
                    MAP_RUN_V1_FORMAT,
                    &payload[..used],
                )?;
            }
            storage.stage_ready_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                region_index,
            )?;
            next_region = Some(region_index);
            first_region = Some(region_index);
            region_count = region_count
                .checked_add(1)
                .ok_or(MapError::SerializationError)?;
            end_index = plan.start_index;
        }

        Ok(Some(MapRunDescriptor {
            source: MapRunSource::RunChain,
            generation,
            first_region: first_region.ok_or(MapError::SerializationError)?,
            region_count,
            approx_state_count: u32::try_from(entry_count)
                .map_err(|_| MapError::SerializationError)?,
            lower_key: Some(lower.key),
            upper_key: Some(upper.key),
        }))
    }

    pub(crate) fn commit_manifest_to_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &mut self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        extra_newest: Option<MapRunDescriptor<K>>,
    ) -> Result<u32, MapStorageError> {
        let previous_region = storage
            .collections()
            .iter()
            .find(|collection| collection.collection_id() == self.id)
            .and_then(|collection| match collection.basis() {
                crate::StartupCollectionBasis::Region(region_index) => Some(region_index),
                _ => None,
            });

        let mut prior_chain_count = 0usize;
        for run in self.runs.iter() {
            match run.source {
                MapRunSource::RunChain => {
                    prior_chain_count = prior_chain_count
                        .checked_add(1)
                        .ok_or(MapError::SerializationError)?;
                }
                MapRunSource::LegacyRegion => {}
            }
        }
        let manifest_run_count = prior_chain_count
            .checked_add(usize::from(extra_newest.is_some()))
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        ensure_manifest_run_capacity::<MAX_RUNS>(self.id, manifest_run_count)?;

        if let Some(region_index) = storage.last_free_list_head() {
            storage.ensure_head_append_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.id,
                CollectionType::MAP_CODE,
                region_index,
            )?;
        }
        let manifest_region =
            storage.reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
        {
            let (payload, _) = workspace.encode_buffers();
            let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
            let used = self.encode_manifest_into(payload, extra_newest.as_ref(), None)?;
            storage.write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                manifest_region,
                self.id,
                MAP_MANIFEST_V1_FORMAT,
                &payload[..used],
            )?;
        }
        storage.append_head_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            CollectionType::MAP_CODE,
            manifest_region,
        )?;

        if let Some(previous_region) = previous_region {
            storage.append_reclaim_begin_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                previous_region,
            )?;
        }

        let mut retained = Vec::<MapRunDescriptor<K>, MAX_RUNS>::new();
        if let Some(run) = extra_newest {
            retained
                .push(run)
                .map_err(|_| MapStorageError::TooManyRuns {
                    collection_id: self.id,
                    max_runs: MAX_RUNS,
                })?;
        }
        while !self.runs.is_empty() {
            let run = self.runs.remove(0);
            match run.source {
                MapRunSource::RunChain => {
                    retained
                        .push(run)
                        .map_err(|_| MapStorageError::TooManyRuns {
                            collection_id: self.id,
                            max_runs: MAX_RUNS,
                        })?;
                }
                MapRunSource::LegacyRegion => {}
            }
        }
        self.runs = retained;
        self.clear_frontier();
        Ok(manifest_region)
    }

    /// Flushes this frontier into immutable run regions and commits a manifest head.
    pub fn flush_to_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &mut self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<u32, MapStorageError> {
        let previous_region = storage
            .collections()
            .iter()
            .find(|collection| collection.collection_id() == self.id)
            .and_then(|collection| match collection.basis() {
                crate::StartupCollectionBasis::Region(region_index) => Some(region_index),
                _ => None,
            });

        let frontier_generation = self.next_run_generation();
        let mut planned_allocations = self
            .planned_frontier_run_region_count(workspace, frontier_generation)?
            .checked_add(1)
            .ok_or(MapError::SerializationError)?;
        if let Some(legacy) = self
            .runs
            .iter()
            .find(|run| run.source == MapRunSource::LegacyRegion)
        {
            let mut legacy_region = [0u8; REGION_SIZE];
            flash.read_region(legacy.first_region, 0, &mut legacy_region)?;
            let header = Header::decode(&legacy_region[..Header::ENCODED_LEN])?;
            if header.collection_id != self.id {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index: legacy.first_region,
                });
            }
            if header.collection_format != MAP_REGION_V1_FORMAT {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index: legacy.first_region,
                });
            }
            let payload_end = usize::try_from(storage.metadata().region_size)
                .map_err(|_| MapStorageError::Map(MapError::SerializationError))?
                .checked_sub(FreePointerFooter::ENCODED_LEN)
                .ok_or(MapStorageError::Map(MapError::SerializationError))?;
            let snapshot =
                legacy_snapshot_from_payload(&legacy_region[Header::ENCODED_LEN..payload_end])?;
            planned_allocations = planned_allocations
                .checked_add(Self::planned_snapshot_run_region_count(
                    workspace,
                    snapshot,
                    legacy.generation,
                )?)
                .ok_or(MapError::SerializationError)?;
        }
        let additional_allocations = if storage.ready_region().is_some() {
            planned_allocations.saturating_sub(1)
        } else {
            planned_allocations
        };
        storage.ensure_foreground_allocation_headroom_for::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            additional_allocations,
        )?;

        let frontier_run = self.write_frontier_run_to_storage::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(storage, flash, workspace, frontier_generation)?;

        let legacy_run = if let Some(legacy) = self
            .runs
            .iter()
            .find(|run| run.source == MapRunSource::LegacyRegion)
        {
            let mut legacy_region = [0u8; REGION_SIZE];
            flash.read_region(legacy.first_region, 0, &mut legacy_region)?;
            let header = Header::decode(&legacy_region[..Header::ENCODED_LEN])?;
            if header.collection_id != self.id {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index: legacy.first_region,
                });
            }
            if header.collection_format != MAP_REGION_V1_FORMAT {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index: legacy.first_region,
                });
            }
            let payload_end = usize::try_from(storage.metadata().region_size)
                .map_err(|_| MapStorageError::Map(MapError::SerializationError))?
                .checked_sub(FreePointerFooter::ENCODED_LEN)
                .ok_or(MapStorageError::Map(MapError::SerializationError))?;
            let snapshot =
                legacy_snapshot_from_payload(&legacy_region[Header::ENCODED_LEN..payload_end])?;
            self.write_snapshot_run_to_storage::<
                REGION_SIZE,
                REGION_COUNT,
                IO,
                MAX_COLLECTIONS,
                MAX_PENDING_RECLAIMS,
            >(storage, flash, workspace, snapshot, legacy.generation)?
        } else {
            None
        };

        let mut prior_chain_count = 0usize;
        for run in self.runs.iter() {
            match run.source {
                MapRunSource::RunChain => {
                    prior_chain_count = prior_chain_count
                        .checked_add(1)
                        .ok_or(MapError::SerializationError)?;
                }
                MapRunSource::LegacyRegion => {}
            }
        }
        let manifest_run_count = prior_chain_count
            .checked_add(usize::from(frontier_run.is_some()))
            .and_then(|count| count.checked_add(usize::from(legacy_run.is_some())))
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        ensure_manifest_run_capacity::<MAX_RUNS>(self.id, manifest_run_count)?;

        if let Some(region_index) = storage.last_free_list_head() {
            storage.ensure_head_append_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.id,
                CollectionType::MAP_CODE,
                region_index,
            )?;
        }
        let manifest_region =
            storage.reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
        {
            let (payload, _) = workspace.encode_buffers();
            let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
            let used =
                self.encode_manifest_into(payload, frontier_run.as_ref(), legacy_run.as_ref())?;
            storage.write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                manifest_region,
                self.id,
                MAP_MANIFEST_V1_FORMAT,
                &payload[..used],
            )?;
        }
        storage.append_head_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            CollectionType::MAP_CODE,
            manifest_region,
        )?;

        if let Some(previous_region) = previous_region {
            storage.append_reclaim_begin_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                previous_region,
            )?;
        }

        let mut retained = Vec::<MapRunDescriptor<K>, MAX_RUNS>::new();
        if let Some(run) = frontier_run {
            retained
                .push(run)
                .map_err(|_| MapStorageError::TooManyRuns {
                    collection_id: self.id,
                    max_runs: MAX_RUNS,
                })?;
        }
        if let Some(run) = legacy_run {
            retained
                .push(run)
                .map_err(|_| MapStorageError::TooManyRuns {
                    collection_id: self.id,
                    max_runs: MAX_RUNS,
                })?;
        }
        while !self.runs.is_empty() {
            let run = self.runs.remove(0);
            match run.source {
                MapRunSource::RunChain => {
                    retained
                        .push(run)
                        .map_err(|_| MapStorageError::TooManyRuns {
                            collection_id: self.id,
                            max_runs: MAX_RUNS,
                        })?;
                }
                MapRunSource::LegacyRegion => {}
            }
        }
        self.runs = retained;
        self.clear_frontier();
        Ok(manifest_region)
    }

    /// Opens a live map collection from replay-tracked storage state.
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
                    crate::WalRecord::Update {
                        collection_id: record_collection_id,
                        payload,
                    } => {
                        if record_collection_id != collection_id {
                            return Ok(());
                        }
                        if basis_loaded {
                            map.apply_update_payload(payload)?;
                        }
                    }
                    crate::WalRecord::Snapshot {
                        collection_id: record_collection_id,
                        collection_type,
                        payload,
                    } => {
                        if record_collection_id != collection_id {
                            return Ok(());
                        }
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
                    } => {
                        if record_collection_id != collection_id {
                            return Ok(());
                        }
                        if collection_type != CollectionType::MAP_CODE {
                            return Err(MapStorageError::CollectionTypeMismatch {
                                collection_id,
                                expected: CollectionType::MAP_CODE,
                                actual: Some(collection_type),
                            });
                        }
                        if target_basis == crate::StartupCollectionBasis::Region(region_index) {
                            load_map_basis_from_flash::<
                                REGION_SIZE,
                                IO,
                                K,
                                V,
                                MAX_INDEXES,
                                MAX_RUNS,
                            >(
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

fn load_map_basis_from_flash<
    const REGION_SIZE: usize,
    IO: FlashIo,
    K,
    V,
    const MAX_INDEXES: usize,
    const MAX_RUNS: usize,
>(
    flash: &mut IO,
    metadata: crate::StorageMetadata,
    collection_id: CollectionId,
    region_index: u32,
    map: &mut MapFrontier<'_, K, V, MAX_INDEXES, MAX_RUNS>,
) -> Result<(), MapStorageError>
where
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    let mut local_region = [0u8; REGION_SIZE];
    let (header, payload) =
        read_committed_region::<REGION_SIZE, IO>(flash, metadata, region_index, &mut local_region)?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::UnknownCollection(collection_id));
    }

    match header.collection_format {
        MAP_REGION_V1_FORMAT => {
            return Err(MapStorageError::UnsupportedRegionFormat {
                collection_id,
                region_index,
                actual: MAP_REGION_V1_FORMAT,
            });
        }
        MAP_MANIFEST_V1_FORMAT => {
            map.load_manifest_descriptors(payload, collection_id, region_index)?;
        }
        actual => {
            return Err(MapStorageError::UnsupportedRegionFormat {
                collection_id,
                region_index,
                actual,
            });
        }
    }
    Ok(())
}

pub(crate) fn map_head_region_references_region<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    collection_id: CollectionId,
    head_region: u32,
    target_region: u32,
) -> Result<bool, MapStorageError> {
    let mut manifest_region = [0u8; REGION_SIZE];
    let (header, payload) = read_committed_region::<REGION_SIZE, IO>(
        flash,
        metadata,
        head_region,
        &mut manifest_region,
    )?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::UnknownCollection(collection_id));
    }
    if header.collection_format == MAP_REGION_V1_FORMAT {
        return Err(MapStorageError::UnsupportedRegionFormat {
            collection_id,
            region_index: head_region,
            actual: MAP_REGION_V1_FORMAT,
        });
    }
    if header.collection_format != MAP_MANIFEST_V1_FORMAT {
        return Err(MapStorageError::UnsupportedRegionFormat {
            collection_id,
            region_index: head_region,
            actual: header.collection_format,
        });
    }
    if head_region == target_region {
        return Ok(true);
    }

    let mut offset = 0usize;
    let run_count = usize::try_from(read_u32(payload, &mut offset)?).map_err(|_| {
        MapStorageError::InvalidManifest {
            collection_id,
            region_index: head_region,
        }
    })?;
    let mut run_region = [0u8; REGION_SIZE];
    for _ in 0..run_count {
        let _generation = read_u64(payload, &mut offset)?;
        let first_region = read_u32(payload, &mut offset)?;
        let region_count = read_u32(payload, &mut offset)?;
        let _approx_state_count = read_u32(payload, &mut offset)?;
        let lower_key_len = usize::try_from(read_u32(payload, &mut offset)?).map_err(|_| {
            MapStorageError::InvalidManifest {
                collection_id,
                region_index: head_region,
            }
        })?;
        let upper_key_len = usize::try_from(read_u32(payload, &mut offset)?).map_err(|_| {
            MapStorageError::InvalidManifest {
                collection_id,
                region_index: head_region,
            }
        })?;
        let bounds_end = offset
            .checked_add(lower_key_len)
            .and_then(|end| end.checked_add(upper_key_len))
            .ok_or(MapStorageError::InvalidManifest {
                collection_id,
                region_index: head_region,
            })?;
        if payload.get(offset..bounds_end).is_none() {
            return Err(MapStorageError::InvalidManifest {
                collection_id,
                region_index: head_region,
            });
        }
        if region_count == 0 {
            return Err(MapStorageError::InvalidManifest {
                collection_id,
                region_index: head_region,
            });
        }
        offset = bounds_end;

        let mut current_region = Some(first_region);
        for _ in 0..region_count {
            let region_index = current_region.ok_or(MapStorageError::InvalidRun {
                collection_id,
                region_index: first_region,
            })?;
            if region_index == target_region {
                return Ok(true);
            }

            let (run_header, run_payload) = read_committed_region::<REGION_SIZE, IO>(
                flash,
                metadata,
                region_index,
                &mut run_region,
            )?;
            if run_header.collection_id != collection_id {
                return Err(MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                });
            }
            if run_header.collection_format != MAP_RUN_V1_FORMAT {
                return Err(MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                });
            }
            let view = parse_run_segment_payload(run_payload).map_err(|_| {
                MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                }
            })?;
            current_region = view.next_region;
        }
    }

    Ok(false)
}
