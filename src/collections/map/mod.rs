//! Durable map collection implementation and storage helpers.
#![allow(clippy::too_many_arguments)]

use crate::disk::{DiskError, FreePointerFooter, Header};
use crate::flash_io::{FlashIo, StorageIoError};
use crate::mock::MockError;
use crate::startup::StartupOpenPlan;
use crate::storage::{StorageRuntime, StorageRuntimeError, StorageVisitError, WalHeadReclaimPlan};
use crate::workspace::StorageWorkspace;
use crate::{CollectionId, CollectionType, StorageMetadata};
use core::cmp::Ordering;
use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem::size_of;
use heapless::Vec;
use postcard::{from_bytes, to_slice};
use serde::{Deserialize, Serialize};

#[cfg(feature = "perf-counters")]
use crate::perf_metrics::{StoragePerfCounter, StoragePerfMetrics};

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

impl From<LsmKeyError> for MapError {
    fn from(error: LsmKeyError) -> Self {
        match error {
            LsmKeyError::BufferTooSmall => Self::BufferTooSmall,
            LsmKeyError::SerializationError => Self::SerializationError,
        }
    }
}

impl From<LsmValueError> for MapError {
    fn from(error: LsmValueError) -> Self {
        match error {
            LsmValueError::BufferTooSmall => Self::BufferTooSmall,
            LsmValueError::SerializationError => Self::SerializationError,
        }
    }
}

/// Public key boundary for durable LSM maps.
///
/// Implementations may choose an ordered byte encoding so lookup can compare
/// stored key bytes without decoding owned key values.
pub trait LsmKey:
    Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>
{
    /// Encodes the key into `out` and returns the number of bytes written.
    fn encode_key(&self, out: &mut [u8]) -> Result<usize, LsmKeyError>;

    /// Decodes a key from the current stable key bytes.
    fn decode_key(bytes: &[u8]) -> Result<Self, LsmKeyError>
    where
        Self: Sized;

    /// Compares stored key bytes with an owned key.
    fn compare_encoded_key(encoded: &[u8], key: &Self) -> Result<Ordering, LsmKeyError>
    where
        Self: Sized,
    {
        Ok(Self::decode_key(encoded)?.cmp(key))
    }

    /// Whether [`Self::compare_encoded_key`] avoids decoding stored key bytes.
    const COMPARES_ENCODED_KEY_WITHOUT_DECODE: bool = false;
}

macro_rules! impl_unsigned_lsm_key {
    ($ty:ty) => {
        impl LsmKey for $ty {
            fn encode_key(&self, out: &mut [u8]) -> Result<usize, LsmKeyError> {
                let bytes = self.to_be_bytes();
                if out.len() < bytes.len() {
                    return Err(LsmKeyError::BufferTooSmall);
                }
                out[..bytes.len()].copy_from_slice(&bytes);
                Ok(bytes.len())
            }

            fn decode_key(bytes: &[u8]) -> Result<Self, LsmKeyError> {
                let array: [u8; size_of::<$ty>()] = bytes
                    .try_into()
                    .map_err(|_| LsmKeyError::SerializationError)?;
                Ok(<$ty>::from_be_bytes(array))
            }

            fn compare_encoded_key(encoded: &[u8], key: &Self) -> Result<Ordering, LsmKeyError> {
                let mut key_bytes = [0u8; size_of::<$ty>()];
                key.encode_key(&mut key_bytes)?;
                if encoded.len() != key_bytes.len() {
                    return Err(LsmKeyError::SerializationError);
                }
                Ok(encoded.cmp(&key_bytes))
            }

            const COMPARES_ENCODED_KEY_WITHOUT_DECODE: bool = true;
        }
    };
}

macro_rules! impl_signed_lsm_key {
    ($ty:ty, $unsigned:ty, $flip:expr) => {
        impl LsmKey for $ty {
            fn encode_key(&self, out: &mut [u8]) -> Result<usize, LsmKeyError> {
                let ordered = ((*self as $unsigned) ^ $flip).to_be_bytes();
                if out.len() < ordered.len() {
                    return Err(LsmKeyError::BufferTooSmall);
                }
                out[..ordered.len()].copy_from_slice(&ordered);
                Ok(ordered.len())
            }

            fn decode_key(bytes: &[u8]) -> Result<Self, LsmKeyError> {
                let array: [u8; size_of::<$unsigned>()] = bytes
                    .try_into()
                    .map_err(|_| LsmKeyError::SerializationError)?;
                let raw = <$unsigned>::from_be_bytes(array) ^ $flip;
                Ok(raw as $ty)
            }

            fn compare_encoded_key(encoded: &[u8], key: &Self) -> Result<Ordering, LsmKeyError> {
                let mut key_bytes = [0u8; size_of::<$unsigned>()];
                key.encode_key(&mut key_bytes)?;
                if encoded.len() != key_bytes.len() {
                    return Err(LsmKeyError::SerializationError);
                }
                Ok(encoded.cmp(&key_bytes))
            }

            const COMPARES_ENCODED_KEY_WITHOUT_DECODE: bool = true;
        }
    };
}

impl_unsigned_lsm_key!(u8);
impl_unsigned_lsm_key!(u16);
impl_unsigned_lsm_key!(u32);
impl_unsigned_lsm_key!(u64);
impl_unsigned_lsm_key!(u128);

impl_signed_lsm_key!(i8, u8, 0x80u8);
impl_signed_lsm_key!(i16, u16, 0x8000u16);
impl_signed_lsm_key!(i32, u32, 0x8000_0000u32);
impl_signed_lsm_key!(i64, u64, 0x8000_0000_0000_0000u64);
impl_signed_lsm_key!(i128, u128, 0x8000_0000_0000_0000_0000_0000_0000_0000u128);

impl LsmKey for bool {
    fn encode_key(&self, out: &mut [u8]) -> Result<usize, LsmKeyError> {
        let Some(slot) = out.first_mut() else {
            return Err(LsmKeyError::BufferTooSmall);
        };
        *slot = u8::from(*self);
        Ok(1)
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, LsmKeyError> {
        match bytes {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(LsmKeyError::SerializationError),
        }
    }

    fn compare_encoded_key(encoded: &[u8], key: &Self) -> Result<Ordering, LsmKeyError> {
        let key_byte = u8::from(*key);
        match encoded {
            [stored] => Ok(stored.cmp(&key_byte)),
            _ => Err(LsmKeyError::SerializationError),
        }
    }

    const COMPARES_ENCODED_KEY_WITHOUT_DECODE: bool = true;
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

type RefType = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryRef {
    start: RefType,
    end: RefType,
}

const ENTRY_REF_POINTER_SIZE: usize = size_of::<RefType>();
const ENTRY_REF_SIZE: usize = size_of::<[RefType; 2]>();
const SNAPSHOT_ENTRY_COUNT_SIZE: usize = size_of::<u32>();
const SNAPSHOT_ENTRY_BYTES_LEN_SIZE: usize = size_of::<u32>();
const SNAPSHOT_MAGIC: [u8; 4] = *b"MAP2";
const SNAPSHOT_MAGIC_SIZE: usize = SNAPSHOT_MAGIC.len();
const SNAPSHOT_HEADER_SIZE: usize =
    SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE;
const REGION_SNAPSHOT_LEN_SIZE: usize = size_of::<u32>();
const ENTRY_KIND_SIZE: usize = size_of::<u8>();
const ENTRY_KEY_LEN_SIZE: usize = size_of::<u32>();
const ENTRY_VALUE_LEN_SIZE: usize = size_of::<u32>();
const ENTRY_HEADER_SIZE: usize = ENTRY_KIND_SIZE + ENTRY_KEY_LEN_SIZE + ENTRY_VALUE_LEN_SIZE;
const ENTRY_KIND_SET: u8 = 1;
const ENTRY_KIND_DELETE: u8 = 2;

/// Stable committed-region format identifier for map regions.
pub const MAP_REGION_V2_FORMAT: u16 = 4;
/// Stable committed-region format identifier for map manifest regions.
pub const MAP_MANIFEST_V2_FORMAT: u16 = 5;
/// Stable committed-region format identifier for immutable map run segments.
pub const MAP_RUN_V2_FORMAT: u16 = 6;
/// Default retained run descriptor capacity for public map handles.
pub const DEFAULT_MAX_RUNS: usize = 8;
/// Snapshot bytes representing an empty map basis.
pub const EMPTY_MAP_SNAPSHOT: [u8; SNAPSHOT_HEADER_SIZE] = [
    SNAPSHOT_MAGIC[0],
    SNAPSHOT_MAGIC[1],
    SNAPSHOT_MAGIC[2],
    SNAPSHOT_MAGIC[3],
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
];

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

#[derive(Debug, Clone, Copy)]
pub(crate) struct MapMutationUndo {
    record_count: u32,
    next_record_offset: usize,
    next_record_index: usize,
    ref_backup_map_offset: usize,
    ref_backup_scratch_offset: usize,
    ref_backup_len: usize,
}

impl MapMutationUndo {
    #[cfg_attr(not(any(test, feature = "perf-counters")), allow(dead_code))]
    pub(crate) fn saved_bytes_len(self) -> usize {
        self.ref_backup_len
    }
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
    /// A compaction target of zero runs is invalid.
    InvalidRunTarget,
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

impl From<StorageIoError> for MapStorageError {
    fn from(error: StorageIoError) -> Self {
        match error {
            StorageIoError::Mock(error) => Self::Mock(error),
            #[cfg(feature = "embedded-storage")]
            StorageIoError::EmbeddedStorage(error) => Self::Storage(StorageRuntimeError::from(
                crate::flash_io::StorageIoError::EmbeddedStorage(error),
            )),
            #[cfg(all(feature = "file-backing", target_os = "linux"))]
            StorageIoError::FileBacking(error) => Self::Storage(StorageRuntimeError::from(
                crate::flash_io::StorageIoError::FileBacking(error),
            )),
        }
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
    if snapshot.len() < SNAPSHOT_HEADER_SIZE {
        return Err(MapError::SerializationError);
    }
    if snapshot[..SNAPSHOT_MAGIC_SIZE] != SNAPSHOT_MAGIC {
        return Err(MapError::SerializationError);
    }

    let mut entry_count_bytes = [0u8; SNAPSHOT_ENTRY_COUNT_SIZE];
    let entry_count_offset = SNAPSHOT_MAGIC_SIZE;
    entry_count_bytes.copy_from_slice(
        &snapshot[entry_count_offset..entry_count_offset + SNAPSHOT_ENTRY_COUNT_SIZE],
    );
    let entry_count = usize::try_from(u32::from_le_bytes(entry_count_bytes))
        .map_err(|_| MapError::SerializationError)?;

    let entry_bytes_len_offset = SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE;
    let mut entry_bytes_len_bytes = [0u8; SNAPSHOT_ENTRY_BYTES_LEN_SIZE];
    entry_bytes_len_bytes.copy_from_slice(
        &snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE],
    );
    let entry_bytes_len = usize::try_from(u32::from_le_bytes(entry_bytes_len_bytes))
        .map_err(|_| MapError::SerializationError)?;

    let entries_offset = SNAPSHOT_HEADER_SIZE;
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
    let compact_start = ref_to_usize(entry_ref.start)?;
    let compact_end = ref_to_usize(entry_ref.end)?;
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
    K: LsmKey,
    V: LsmValue,
{
    encoded_entry_to_entry(snapshot_entry_bytes(snapshot, index)?)
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

#[cfg_attr(not(test), allow(dead_code))]
fn lookup_snapshot<K, V>(snapshot: &[u8], key: &K) -> Result<LookupResult<V>, MapError>
where
    K: LsmKey,
    V: LsmValue,
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
        let entry = snapshot_entry_bytes(snapshot, mid)?;
        match compare_entry_key(entry, key)? {
            Ordering::Equal => return encoded_entry_lookup_value(entry),
            Ordering::Greater => high_index = mid,
            Ordering::Less => {
                low_index = mid.checked_add(1).ok_or(MapError::SerializationError)?;
            }
        }
    }

    Ok(LookupResult::NotFound)
}

fn decode_snapshot_header(snapshot_header: &[u8]) -> Result<(usize, usize), MapError> {
    if snapshot_header.len() != SNAPSHOT_HEADER_SIZE {
        return Err(MapError::SerializationError);
    }
    if snapshot_header[..SNAPSHOT_MAGIC_SIZE] != SNAPSHOT_MAGIC {
        return Err(MapError::SerializationError);
    }

    let mut entry_count_bytes = [0u8; SNAPSHOT_ENTRY_COUNT_SIZE];
    let entry_count_offset = SNAPSHOT_MAGIC_SIZE;
    entry_count_bytes.copy_from_slice(
        &snapshot_header[entry_count_offset..entry_count_offset + SNAPSHOT_ENTRY_COUNT_SIZE],
    );
    let entry_count = usize::try_from(u32::from_le_bytes(entry_count_bytes))
        .map_err(|_| MapError::SerializationError)?;

    let entry_bytes_len_offset = SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE;
    let mut entry_bytes_len_bytes = [0u8; SNAPSHOT_ENTRY_BYTES_LEN_SIZE];
    entry_bytes_len_bytes.copy_from_slice(
        &snapshot_header
            [entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE],
    );
    let entry_bytes_len = usize::try_from(u32::from_le_bytes(entry_bytes_len_bytes))
        .map_err(|_| MapError::SerializationError)?;

    Ok((entry_count, entry_bytes_len))
}

fn decode_entry_ref(ref_bytes: &[u8]) -> Result<EntryRef, MapError> {
    if ref_bytes.len() != ENTRY_REF_SIZE {
        return Err(MapError::SerializationError);
    }

    let mut start_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
    start_bytes.copy_from_slice(&ref_bytes[..ENTRY_REF_POINTER_SIZE]);
    let mut end_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
    end_bytes.copy_from_slice(&ref_bytes[ENTRY_REF_POINTER_SIZE..ENTRY_REF_SIZE]);
    Ok(EntryRef {
        start: RefType::from_le_bytes(start_bytes),
        end: RefType::from_le_bytes(end_bytes),
    })
}

#[derive(Debug, Clone, Copy)]
struct EncodedEntry<'a> {
    kind: u8,
    key: &'a [u8],
    value: Option<&'a [u8]>,
}

fn checked_add_usize(left: usize, right: usize) -> Result<usize, MapError> {
    left.checked_add(right).ok_or(MapError::SerializationError)
}

fn checked_mul_usize(left: usize, right: usize) -> Result<usize, MapError> {
    left.checked_mul(right).ok_or(MapError::SerializationError)
}

fn ref_to_usize(value: RefType) -> Result<usize, MapError> {
    usize::try_from(value).map_err(|_| MapError::SerializationError)
}

fn parse_encoded_entry(entry: &[u8]) -> Result<EncodedEntry<'_>, MapError> {
    if entry.len() < ENTRY_HEADER_SIZE {
        return Err(MapError::SerializationError);
    }

    let kind = entry[0];
    let mut key_len_bytes = [0u8; ENTRY_KEY_LEN_SIZE];
    key_len_bytes.copy_from_slice(&entry[ENTRY_KIND_SIZE..ENTRY_KIND_SIZE + ENTRY_KEY_LEN_SIZE]);
    let key_len = usize::try_from(u32::from_le_bytes(key_len_bytes))
        .map_err(|_| MapError::SerializationError)?;

    let value_len_offset = ENTRY_KIND_SIZE + ENTRY_KEY_LEN_SIZE;
    let mut value_len_bytes = [0u8; ENTRY_VALUE_LEN_SIZE];
    value_len_bytes
        .copy_from_slice(&entry[value_len_offset..value_len_offset + ENTRY_VALUE_LEN_SIZE]);
    let value_len = usize::try_from(u32::from_le_bytes(value_len_bytes))
        .map_err(|_| MapError::SerializationError)?;

    let key_start = ENTRY_HEADER_SIZE;
    let key_end = checked_add_usize(key_start, key_len)?;
    let value_end = checked_add_usize(key_end, value_len)?;
    if value_end != entry.len() {
        return Err(MapError::SerializationError);
    }

    match kind {
        ENTRY_KIND_SET => Ok(EncodedEntry {
            kind,
            key: &entry[key_start..key_end],
            value: Some(&entry[key_end..value_end]),
        }),
        ENTRY_KIND_DELETE => {
            if value_len != 0 {
                return Err(MapError::SerializationError);
            }
            Ok(EncodedEntry {
                kind,
                key: &entry[key_start..key_end],
                value: None,
            })
        }
        _ => Err(MapError::SerializationError),
    }
}

fn encode_entry_into<K, V>(key: &K, value: Option<&V>, out: &mut [u8]) -> Result<usize, MapError>
where
    K: LsmKey,
    V: LsmValue,
{
    if out.len() < ENTRY_HEADER_SIZE {
        return Err(MapError::BufferTooSmall);
    }

    let key_start = ENTRY_HEADER_SIZE;
    let key_len = key.encode_key(&mut out[key_start..])?;
    let key_end = checked_add_usize(key_start, key_len)?;
    let value_len = if let Some(value) = value {
        value.encode_value(&mut out[key_end..])?
    } else {
        0
    };
    let end = checked_add_usize(key_end, value_len)?;

    out[0] = if value.is_some() {
        ENTRY_KIND_SET
    } else {
        ENTRY_KIND_DELETE
    };
    let key_len_u32 = u32::try_from(key_len).map_err(|_| MapError::SerializationError)?;
    let value_len_u32 = u32::try_from(value_len).map_err(|_| MapError::SerializationError)?;
    out[ENTRY_KIND_SIZE..ENTRY_KIND_SIZE + ENTRY_KEY_LEN_SIZE]
        .copy_from_slice(&key_len_u32.to_le_bytes());
    let value_len_offset = ENTRY_KIND_SIZE + ENTRY_KEY_LEN_SIZE;
    out[value_len_offset..value_len_offset + ENTRY_VALUE_LEN_SIZE]
        .copy_from_slice(&value_len_u32.to_le_bytes());
    Ok(end)
}

fn encoded_entry_key<K>(entry: &[u8]) -> Result<K, MapError>
where
    K: LsmKey,
{
    Ok(K::decode_key(parse_encoded_entry(entry)?.key)?)
}

fn encoded_entry_to_entry<K, V>(entry: &[u8]) -> Result<Entry<K, V>, MapError>
where
    K: LsmKey,
    V: LsmValue,
{
    let entry = parse_encoded_entry(entry)?;
    let key = K::decode_key(entry.key)?;
    let value = match entry.value {
        Some(value) => Some(V::decode_value(value)?),
        None => None,
    };
    Ok(Entry { key, value })
}

fn encoded_entry_lookup_value<V>(entry: &[u8]) -> Result<LookupResult<V>, MapError>
where
    V: LsmValue,
{
    let entry = parse_encoded_entry(entry)?;
    match (entry.kind, entry.value) {
        (ENTRY_KIND_SET, Some(value)) => Ok(LookupResult::Set(V::decode_value(value)?)),
        (ENTRY_KIND_DELETE, None) => Ok(LookupResult::Deleted),
        _ => Err(MapError::SerializationError),
    }
}

#[cfg(feature = "perf-counters")]
fn encoded_entry_lookup_value_metered<V>(
    entry: &[u8],
    metrics: Option<&mut StoragePerfMetrics>,
) -> Result<LookupResult<V>, MapError>
where
    V: LsmValue,
{
    let entry = parse_encoded_entry(entry)?;
    match (entry.kind, entry.value) {
        (ENTRY_KIND_SET, Some(value)) => {
            if let Some(metrics) = metrics {
                metrics.increment(StoragePerfCounter::ValueDecodes);
            }
            Ok(LookupResult::Set(V::decode_value(value)?))
        }
        (ENTRY_KIND_DELETE, None) => Ok(LookupResult::Deleted),
        _ => Err(MapError::SerializationError),
    }
}

fn compare_entry_key<K>(entry: &[u8], key: &K) -> Result<Ordering, MapError>
where
    K: LsmKey,
{
    compare_encoded_key_bytes(parse_encoded_entry(entry)?.key, key)
}

fn compare_encoded_key_bytes<K>(encoded: &[u8], key: &K) -> Result<Ordering, MapError>
where
    K: LsmKey,
{
    Ok(K::compare_encoded_key(encoded, key)?)
}

#[cfg(feature = "perf-counters")]
fn compare_encoded_key_bytes_metered<K>(
    encoded: &[u8],
    key: &K,
    metrics: Option<&mut StoragePerfMetrics>,
) -> Result<Ordering, MapError>
where
    K: LsmKey,
{
    if let Some(metrics) = metrics {
        metrics.increment(StoragePerfCounter::EncodedKeyComparisons);
        if !K::COMPARES_ENCODED_KEY_WITHOUT_DECODE {
            metrics.increment(StoragePerfCounter::KeyDecodesDuringComparison);
        }
    }
    compare_encoded_key_bytes(encoded, key)
}

#[cfg(feature = "perf-counters")]
fn compare_entry_key_metered<K>(
    entry: &[u8],
    key: &K,
    metrics: Option<&mut StoragePerfMetrics>,
) -> Result<Ordering, MapError>
where
    K: LsmKey,
{
    compare_encoded_key_bytes_metered(parse_encoded_entry(entry)?.key, key, metrics)
}

#[cfg(test)]
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

    SNAPSHOT_HEADER_SIZE
        .checked_add(entry_bytes_len)
        .and_then(|len| len.checked_add(entry_count.checked_mul(ENTRY_REF_SIZE)?))
        .ok_or(MapError::SerializationError)
}

#[cfg(test)]
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
    snapshot[..SNAPSHOT_MAGIC_SIZE].copy_from_slice(&SNAPSHOT_MAGIC);
    let entry_count_offset = SNAPSHOT_MAGIC_SIZE;
    snapshot[entry_count_offset..entry_count_offset + SNAPSHOT_ENTRY_COUNT_SIZE]
        .copy_from_slice(&entry_count_u32.to_le_bytes());

    let refs_len = entry_count
        .checked_mul(ENTRY_REF_SIZE)
        .ok_or(MapError::SerializationError)?;
    let refs_start = snapshot_len
        .checked_sub(refs_len)
        .ok_or(MapError::SerializationError)?;
    let entries_offset = SNAPSHOT_HEADER_SIZE;
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
    let entry_bytes_len_offset = SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE;
    snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
        .copy_from_slice(&entry_bytes_len_u32.to_le_bytes());

    Ok(snapshot_len)
}

#[cfg(test)]
fn encode_snapshot_from_entries_into<K, V>(
    entries: &[Entry<K, V>],
    snapshot: &mut [u8],
) -> Result<usize, MapError>
where
    K: LsmKey,
    V: LsmValue,
{
    let entry_count = entries.len();
    let refs_len = entry_count
        .checked_mul(ENTRY_REF_SIZE)
        .ok_or(MapError::SerializationError)?;
    let entries_offset = SNAPSHOT_HEADER_SIZE;
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
        let used = encode_entry_into(
            &entry.key,
            entry.value.as_ref(),
            &mut snapshot[write_offset..temp_refs_start],
        )?;
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
    snapshot[..SNAPSHOT_MAGIC_SIZE].copy_from_slice(&SNAPSHOT_MAGIC);
    let entry_count_offset = SNAPSHOT_MAGIC_SIZE;
    snapshot[entry_count_offset..entry_count_offset + SNAPSHOT_ENTRY_COUNT_SIZE]
        .copy_from_slice(&entry_count_u32.to_le_bytes());
    let entry_bytes_len_u32 =
        u32::try_from(entry_bytes_len).map_err(|_| MapError::SerializationError)?;
    let entry_bytes_len_offset = SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE;
    snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
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

#[derive(Debug, Clone, Copy)]
struct RunSegmentHeader {
    generation: u64,
    next_region: Option<u32>,
    state_count: usize,
    lower_key_len: usize,
    upper_key_len: usize,
    snapshot_len: usize,
}

fn parse_run_segment_payload(payload: &[u8]) -> Result<RunSegmentView<'_>, MapError> {
    let header = parse_run_segment_header(payload)?;
    let offset = RUN_SEGMENT_FIXED_SIZE;
    let lower_key_end = offset
        .checked_add(header.lower_key_len)
        .ok_or(MapError::SerializationError)?;
    let upper_key_end = lower_key_end
        .checked_add(header.upper_key_len)
        .ok_or(MapError::SerializationError)?;
    let snapshot_end = upper_key_end
        .checked_add(header.snapshot_len)
        .ok_or(MapError::SerializationError)?;
    if snapshot_end > payload.len() {
        return Err(MapError::SerializationError);
    }

    let lower_key = &payload[offset..lower_key_end];
    let upper_key = &payload[lower_key_end..upper_key_end];
    let snapshot = &payload[upper_key_end..snapshot_end];
    let (entry_count, _, _, _) = snapshot_parts(snapshot)?;
    if header.state_count != entry_count {
        return Err(MapError::SerializationError);
    }

    Ok(RunSegmentView {
        generation: header.generation,
        next_region: header.next_region,
        lower_key,
        upper_key,
        snapshot,
    })
}

fn parse_run_segment_header(payload: &[u8]) -> Result<RunSegmentHeader, MapError> {
    let mut offset = 0usize;
    let generation = read_u64(payload, &mut offset)?;
    let next_region_raw = read_u32(payload, &mut offset)?;
    let next_region = if next_region_raw == NO_NEXT_RUN_REGION {
        None
    } else {
        Some(next_region_raw)
    };
    let state_count = usize::try_from(read_u32(payload, &mut offset)?)
        .map_err(|_| MapError::SerializationError)?;
    let lower_key_len = usize::try_from(read_u32(payload, &mut offset)?)
        .map_err(|_| MapError::SerializationError)?;
    let upper_key_len = usize::try_from(read_u32(payload, &mut offset)?)
        .map_err(|_| MapError::SerializationError)?;
    let snapshot_len = usize::try_from(read_u32(payload, &mut offset)?)
        .map_err(|_| MapError::SerializationError)?;

    if offset != RUN_SEGMENT_FIXED_SIZE {
        return Err(MapError::SerializationError);
    }
    Ok(RunSegmentHeader {
        generation,
        next_region,
        state_count,
        lower_key_len,
        upper_key_len,
        snapshot_len,
    })
}

fn read_committed_region<'a, const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
    region_bytes: &'a mut [u8; REGION_SIZE],
) -> Result<(Header, &'a [u8]), MapStorageError> {
    flash.read_region(region_index, 0, region_bytes.len(), |bytes| {
        region_bytes.copy_from_slice(bytes);
    })?;
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

fn lookup_run_segment_snapshot<K, V, IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
    snapshot_offset: usize,
    snapshot_len: usize,
    expected_entry_count: usize,
    key: &K,
    #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
) -> Result<LookupResult<V>, MapStorageError>
where
    K: LsmKey,
    V: LsmValue,
{
    let snapshot_header_len = SNAPSHOT_HEADER_SIZE;
    let (entry_count, entry_bytes_len) = flash.read_region(
        region_index,
        snapshot_offset,
        snapshot_header_len,
        decode_snapshot_header,
    )??;
    if entry_count != expected_entry_count {
        return Err(MapStorageError::Map(MapError::SerializationError));
    }
    if entry_count == 0 {
        return Ok(LookupResult::NotFound);
    }

    let entries_offset = snapshot_header_len;
    let refs_start = checked_add_usize(entries_offset, entry_bytes_len)?;
    let refs_len = checked_mul_usize(entry_count, ENTRY_REF_SIZE)?;
    let expected_snapshot_len = checked_add_usize(refs_start, refs_len)?;
    if expected_snapshot_len != snapshot_len {
        return Err(MapStorageError::Map(MapError::SerializationError));
    }

    let mut low_index = 0usize;
    let mut high_index = entry_count;
    while low_index < high_index {
        let mid = midpoint_index(low_index, high_index)?;
        if mid < low_index {
            return Err(MapStorageError::Map(MapError::SerializationError));
        }
        if mid >= high_index {
            return Err(MapStorageError::Map(MapError::SerializationError));
        }

        let ref_offset = snapshot_offset
            .checked_add(refs_start)
            .and_then(|offset| offset.checked_add(checked_mul_usize(mid, ENTRY_REF_SIZE).ok()?))
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        let entry_ref =
            flash.read_region(region_index, ref_offset, ENTRY_REF_SIZE, decode_entry_ref)??;
        #[cfg(feature = "perf-counters")]
        if let Some(metrics) = metrics.as_deref_mut() {
            metrics.increment(StoragePerfCounter::CommittedRunSnapshotRefReads);
        }

        let compact_start = ref_to_usize(entry_ref.start)?;
        let compact_end = ref_to_usize(entry_ref.end)?;
        let entry_start_in_snapshot = entries_offset
            .checked_add(
                compact_start
                    .checked_sub(ENTRY_COUNT_SIZE)
                    .ok_or(MapStorageError::Map(MapError::SerializationError))?,
            )
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        let entry_end_in_snapshot = entries_offset
            .checked_add(
                compact_end
                    .checked_sub(ENTRY_COUNT_SIZE)
                    .ok_or(MapStorageError::Map(MapError::SerializationError))?,
            )
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        let entries_end = entries_offset
            .checked_add(entry_bytes_len)
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        if entry_end_in_snapshot > entries_end {
            return Err(MapStorageError::Map(MapError::SerializationError));
        }
        if entry_start_in_snapshot >= entry_end_in_snapshot {
            return Err(MapStorageError::Map(MapError::SerializationError));
        }

        let entry_offset = snapshot_offset
            .checked_add(entry_start_in_snapshot)
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        let entry_len = entry_end_in_snapshot
            .checked_sub(entry_start_in_snapshot)
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        let entry_order = flash.read_region(region_index, entry_offset, entry_len, |bytes| {
            #[cfg(feature = "perf-counters")]
            {
                compare_entry_key_metered(bytes, key, metrics.as_deref_mut())
            }
            #[cfg(not(feature = "perf-counters"))]
            {
                compare_entry_key(bytes, key)
            }
        })??;
        #[cfg(feature = "perf-counters")]
        if let Some(metrics) = metrics.as_deref_mut() {
            metrics.increment(StoragePerfCounter::CommittedRunEntryReads);
        }
        match entry_order {
            Ordering::Equal => {
                return flash.read_region(region_index, entry_offset, entry_len, |bytes| {
                    #[cfg(feature = "perf-counters")]
                    {
                        encoded_entry_lookup_value_metered(bytes, metrics)
                            .map_err(MapStorageError::Map)
                    }
                    #[cfg(not(feature = "perf-counters"))]
                    encoded_entry_lookup_value(bytes).map_err(MapStorageError::Map)
                })?;
            }
            Ordering::Greater => high_index = mid,
            Ordering::Less => {
                low_index = mid.checked_add(1).ok_or(MapError::SerializationError)?;
            }
        }
    }

    Ok(LookupResult::NotFound)
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

fn encode_manifest_descriptor<K>(
    manifest_payload: &mut [u8],
    offset: &mut usize,
    run: &MapRunDescriptor<K>,
) -> Result<(), MapError>
where
    K: LsmKey,
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
        let used = key.encode_key(&mut manifest_payload[*offset..])?;
        *offset = (*offset)
            .checked_add(used)
            .ok_or(MapError::SerializationError)?;
        used
    } else {
        0
    };
    let upper_len = if let Some(key) = run.upper_key.as_ref() {
        let used = key.encode_key(&mut manifest_payload[*offset..])?;
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
    K: LsmKey,
    F: FnOnce(&mut [u8]) -> Result<usize, MapError>,
{
    if run_payload.get(..RUN_SEGMENT_FIXED_SIZE).is_none() {
        return Err(MapError::BufferTooSmall);
    }

    let mut offset = RUN_SEGMENT_FIXED_SIZE;
    let lower_len = lower_key.encode_key(&mut run_payload[offset..])?;
    offset = offset
        .checked_add(lower_len)
        .ok_or(MapError::SerializationError)?;
    let upper_len = upper_key.encode_key(&mut run_payload[offset..])?;
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

#[cfg(test)]
fn encode_run_segment_from_entries_into<K, V>(
    run_payload: &mut [u8],
    generation: u64,
    next_region: Option<u32>,
    entries: &[Entry<K, V>],
) -> Result<usize, MapError>
where
    K: LsmKey,
    V: LsmValue,
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

    #[allow(dead_code)]
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

#[derive(Debug, Clone, Copy)]
enum SearchResult {
    Found(RecordIndex),
    NotFound(RecordIndex),
}

/// Small durable map handle used by the public object-level API.
pub struct LsmMap<'mem, K, V, const MAX_RUNS: usize = DEFAULT_MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    pub(crate) collection_id: CollectionId,
    pub(crate) compaction_run_target: usize,
    pub(crate) memory: &'mem mut LsmMapMemory<K, V, MAX_RUNS>,
    _phantom: PhantomData<(K, V)>,
}

pub(crate) struct CachedMapFrontier {
    pub(crate) buffer_generation: u64,
    pub(crate) state: MapFrontierState,
}

/// Caller-owned memory for the public durable map handle.
pub struct LsmMapMemory<K, V, const MAX_RUNS: usize = DEFAULT_MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    pub(crate) cached_frontier: Option<CachedMapFrontier>,
    pub(crate) frontier: MapFrontierMemory<K, MAX_RUNS>,
    pub(crate) compaction_cursors: Vec<RunEntryCursor<K, V>, MAX_RUNS>,
    pub(crate) duplicate_indices: Vec<usize, MAX_RUNS>,
    pub(crate) retained_runs: Vec<MapRunDescriptor<K>, MAX_RUNS>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, const MAX_RUNS: usize> LsmMapMemory<K, V, MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    /// Allocates caller-owned memory for a durable map handle.
    pub fn new() -> Self {
        Self {
            cached_frontier: None,
            frontier: MapFrontierMemory::new(),
            compaction_cursors: Vec::new(),
            duplicate_indices: Vec::new(),
            retained_runs: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

impl<K, V, const MAX_RUNS: usize> Default for LsmMapMemory<K, V, MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Caller-owned memory for a low-level map frontier.
pub struct MapFrontierMemory<K, const MAX_RUNS: usize> {
    pub(crate) runs: Vec<MapRunDescriptor<K>, MAX_RUNS>,
}

impl<K, const MAX_RUNS: usize> MapFrontierMemory<K, MAX_RUNS> {
    /// Allocates caller-owned frontier memory.
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }
}

impl<K, const MAX_RUNS: usize> Default for MapFrontierMemory<K, MAX_RUNS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'mem, K, V, const MAX_RUNS: usize> LsmMap<'mem, K, V, MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    pub(crate) fn from_collection_id(
        collection_id: CollectionId,
        compaction_run_target: usize,
        memory: &'mem mut LsmMapMemory<K, V, MAX_RUNS>,
    ) -> Self {
        memory.cached_frontier = None;
        memory.frontier.runs.clear();
        memory.compaction_cursors.clear();
        memory.duplicate_indices.clear();
        memory.retained_runs.clear();
        Self {
            collection_id,
            compaction_run_target,
            memory,
            _phantom: PhantomData,
        }
    }

    /// Returns the stable collection id for this durable map.
    pub fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
}

/// Caller-owned bounded map frontier used by advanced storage helpers.
pub struct MapFrontier<'a, K, V, const MAX_RUNS: usize = DEFAULT_MAX_RUNS> {
    id: CollectionId,
    record_count: EntryCount,
    next_record_offset: RecordOffset,
    next_record_index: RecordIndex,
    map: &'a mut [u8],
    runs: &'a mut Vec<MapRunDescriptor<K>, MAX_RUNS>,
    _phantom: PhantomData<(K, V)>,
}

pub(crate) struct MapFrontierState {
    id: CollectionId,
    record_count: EntryCount,
    next_record_offset: RecordOffset,
    next_record_index: RecordIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunChainOrder {
    Ascending,
    Descending,
}

pub(crate) struct RunEntryCursor<K, V>
where
    K: LsmKey,
    V: LsmValue,
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
    K: LsmKey,
    V: LsmValue,
{
    fn new(run: &MapRunDescriptor<K>) -> Result<Self, MapStorageError> {
        if run.region_count == 0 {
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
            flash.read_region(region_index, 0, region_bytes.len(), |bytes| {
                region_bytes.copy_from_slice(bytes);
            })?;
            let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
            if header.collection_id != collection_id {
                return Err(MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                });
            }
            if header.collection_format != MAP_RUN_V2_FORMAT {
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

struct CompactionRunWriter<'a, K, V, const MAX_RUNS: usize>
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
    segment: MapFrontier<'a, K, V, MAX_RUNS>,
}

impl<'a, K, V, const MAX_RUNS: usize> CompactionRunWriter<'a, K, V, MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    fn new(generation: u64, segment: MapFrontier<'a, K, V, MAX_RUNS>) -> Self {
        Self {
            generation,
            next_region: None,
            first_region: None,
            lowest_region: None,
            region_count: 0,
            state_count: 0,
            segment,
        }
    }

    fn push<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        collection_id: CollectionId,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        entry: Entry<K, V>,
    ) -> Result<(), MapStorageError> {
        match self.try_push_entry(workspace, &entry)? {
            true => self.increment_state_count(),
            false => {
                if self.segment.frontier_is_empty() {
                    return Err(MapStorageError::Map(MapError::BufferTooSmall));
                }
                self.flush_segment::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                    collection_id,
                    storage,
                    flash,
                    workspace,
                    reclaim_source_regions,
                    active_collections,
                    reclaim_plan,
                    open_plan,
                )?;
                if !self.try_push_entry(workspace, &entry)? {
                    return Err(MapStorageError::Map(MapError::BufferTooSmall));
                }
                self.increment_state_count()
            }
        }
    }

    fn try_push_entry<const REGION_SIZE: usize>(
        &mut self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        entry: &Entry<K, V>,
    ) -> Result<bool, MapError> {
        let (payload_region, undo_scratch) = workspace.encode_buffers();
        match self
            .segment
            .set_worker_with_undo(&entry.key, entry.value.as_ref(), undo_scratch)
        {
            Ok(undo) => {
                if self.segment_fits_in_payload::<REGION_SIZE>(payload_region)? {
                    Ok(true)
                } else {
                    self.segment
                        .restore_from_mutation_undo(undo, undo_scratch)?;
                    Ok(false)
                }
            }
            Err(MapError::BufferTooSmall) if !self.segment.frontier_is_empty() => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn segment_fits_in_payload<const REGION_SIZE: usize>(
        &self,
        payload_region: &mut [u8; REGION_SIZE],
    ) -> Result<bool, MapError> {
        if self.segment.frontier_is_empty() {
            return Ok(true);
        }

        let lower: Entry<K, V> = self.segment.frontier_entry(0)?;
        let upper_index = self
            .segment
            .frontier_entry_count()
            .checked_sub(1)
            .ok_or(MapError::SerializationError)?;
        let upper: Entry<K, V> = self.segment.frontier_entry(upper_index)?;
        let payload = committed_payload_buffer::<REGION_SIZE>(payload_region)?;
        match encode_run_segment_with_snapshot_writer(
            payload,
            self.generation,
            self.next_region,
            self.segment.frontier_entry_count(),
            &lower.key,
            &upper.key,
            |snapshot| self.segment.encode_snapshot_into(snapshot),
        ) {
            Ok(_) => Ok(true),
            Err(MapError::BufferTooSmall) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn finish<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        mut self,
        collection_id: CollectionId,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<Option<MapRunDescriptor<K>>, MapStorageError> {
        self.flush_segment::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            collection_id,
            storage,
            flash,
            workspace,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
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

    fn flush_segment<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        collection_id: CollectionId,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<(), MapStorageError> {
        if self.segment.frontier_is_empty() {
            return Ok(());
        }

        let lower: Entry<K, V> = self.segment.frontier_entry(0)?;
        let upper_index = self
            .segment
            .frontier_entry_count()
            .checked_sub(1)
            .ok_or(MapError::SerializationError)?;
        let upper: Entry<K, V> = self.segment.frontier_entry(upper_index)?;
        let region_index = storage.reserve_next_region_for::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
        )?;
        let used = {
            let (payload, _) = workspace.encode_buffers();
            let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
            let entry_count = self.segment.frontier_entry_count();
            encode_run_segment_with_snapshot_writer(
                payload,
                self.generation,
                self.next_region,
                entry_count,
                &lower.key,
                &upper.key,
                |snapshot| self.segment.encode_snapshot_into(snapshot),
            )?
        };
        storage.write_committed_region_from_workspace_payload::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            region_index,
            collection_id,
            MAP_RUN_V2_FORMAT,
            used,
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
        self.segment.clear_frontier();
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
    K: LsmKey,
{
    let (region_bytes, _) = workspace.scan_buffers();
    flash.read_region(region_index, 0, region_bytes.len(), |bytes| {
        region_bytes.copy_from_slice(bytes);
    })?;
    let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::InvalidRun {
            collection_id,
            region_index,
        });
    }
    if header.collection_format != MAP_RUN_V2_FORMAT {
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
        K::decode_key(view.lower_key).map_err(MapError::from)?,
        K::decode_key(view.upper_key).map_err(MapError::from)?,
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
    flash.read_region(region_index, 0, region_bytes.len(), |bytes| {
        region_bytes.copy_from_slice(bytes);
    })?;
    let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::InvalidRun {
            collection_id,
            region_index,
        });
    }
    if header.collection_format != MAP_RUN_V2_FORMAT {
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

impl<'a, K, V, const MAX_RUNS: usize> MapFrontier<'a, K, V, MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    /// Creates a new empty map frontier over `buffer`.
    pub(crate) fn new(
        id: CollectionId,
        buffer: &'a mut [u8],
        memory: &'a mut MapFrontierMemory<K, MAX_RUNS>,
    ) -> Result<Self, MapError> {
        Self::new_with_runs(id, buffer, &mut memory.runs)
    }

    pub(crate) fn new_with_runs(
        id: CollectionId,
        buffer: &'a mut [u8],
        runs: &'a mut Vec<MapRunDescriptor<K>, MAX_RUNS>,
    ) -> Result<Self, MapError> {
        if buffer.len() < ENTRY_COUNT_SIZE {
            return Err(MapError::BufferTooSmall);
        }

        let record_count = EntryCount(0);
        let next_record_offset = RecordOffset(ENTRY_COUNT_SIZE);
        let next_record_index = RecordIndex(0);
        let map = buffer;
        let _phantom = PhantomData;

        record_count.write(map);
        runs.clear();

        Ok(Self {
            id,
            record_count,
            next_record_index,
            next_record_offset,
            map,
            runs,
            _phantom,
        })
    }

    pub(crate) fn from_state(
        state: MapFrontierState,
        buffer: &'a mut [u8],
        memory: &'a mut MapFrontierMemory<K, MAX_RUNS>,
    ) -> Self {
        Self {
            id: state.id,
            record_count: state.record_count,
            next_record_index: state.next_record_index,
            next_record_offset: state.next_record_offset,
            map: buffer,
            runs: &mut memory.runs,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn into_state(self) -> MapFrontierState {
        MapFrontierState {
            id: self.id,
            record_count: self.record_count,
            next_record_offset: self.next_record_offset,
            next_record_index: self.next_record_index,
        }
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

    pub(crate) fn frontier_entry_count(&self) -> usize {
        self.record_count.0 as usize
    }

    /// Inserts or replaces a key with the supplied value and persists the update.
    pub fn set<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut crate::Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        key: K,
        value: V,
    ) -> Result<(), MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.apply_update(storage, &MapUpdate::Set { key, value })
    }

    /// Deletes a key from the logical map and persists the update.
    pub fn delete<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut crate::Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        key: K,
    ) -> Result<(), MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.apply_update(storage, &MapUpdate::Delete { key })
    }

    /// Applies and persists a map update, flushing this frontier if byte capacity is exhausted.
    pub fn apply_update<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut crate::Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        update: &MapUpdate<K, V>,
    ) -> Result<(), MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        storage.apply_map_frontier_update(self, update)
    }

    pub(crate) fn set_in_memory(&mut self, key: K, value: V) -> Result<(), MapError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.set_worker(key, Some(value))
    }

    pub(crate) fn delete_in_memory(&mut self, key: K) -> Result<(), MapError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.set_worker(key, None)
    }

    fn set_worker(&mut self, key: K, value: Option<V>) -> Result<(), MapError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.set_worker_ref(&key, value.as_ref())
    }

    fn set_worker_ref(&mut self, key: &K, value: Option<&V>) -> Result<(), MapError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        let search_result = self.find_index(key)?;

        match search_result {
            SearchResult::Found(index) => {
                // Updating in place is a possible space optimization, but the
                // current format keeps append-only entry payloads until the
                // next snapshot/flush compacts them.
                let (start, end) = self.add_entry(key, value)?;

                EntryRef::write(self.map, index, start, end)?;

                self.next_record_offset = end;
            }
            SearchResult::NotFound(index) => {
                let (start, end) = self.add_entry(key, value)?;
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

    fn set_worker_with_undo(
        &mut self,
        key: &K,
        value: Option<&V>,
        scratch: &mut [u8],
    ) -> Result<MapMutationUndo, MapError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        let search_result = self.find_index(key)?;
        let entry_len = encode_entry_into(key, value, scratch)?;
        let start = self.next_record_offset;
        let index_offset = self
            .next_record_index
            .offset(self.map)
            .map_err(|_| MapError::BufferTooSmall)?;
        if start.0 >= index_offset {
            return Err(MapError::BufferTooSmall);
        }
        let available = index_offset
            .checked_sub(start.0)
            .ok_or(MapError::SerializationError)?;
        if entry_len > available {
            return Err(MapError::BufferTooSmall);
        }
        let end = RecordOffset(
            start
                .0
                .checked_add(entry_len)
                .ok_or(MapError::SerializationError)?,
        );

        let _: RefType = start
            .0
            .try_into()
            .map_err(|_| MapError::SerializationError)?;
        let _: RefType = end.0.try_into().map_err(|_| MapError::SerializationError)?;

        let (ref_backup_map_offset, ref_backup_len) = self.ref_backup_span(search_result)?;
        let ref_backup_scratch_offset = entry_len;
        let ref_backup_scratch_end = ref_backup_scratch_offset
            .checked_add(ref_backup_len)
            .ok_or(MapError::SerializationError)?;
        if ref_backup_scratch_end > scratch.len() {
            return Err(MapError::BufferTooSmall);
        }
        let map_end = ref_backup_map_offset
            .checked_add(ref_backup_len)
            .ok_or(MapError::SerializationError)?;
        let map_ref_bytes = self
            .map
            .get(ref_backup_map_offset..map_end)
            .ok_or(MapError::IndexOutOfBounds)?;
        scratch[ref_backup_scratch_offset..ref_backup_scratch_end].copy_from_slice(map_ref_bytes);

        let undo = MapMutationUndo {
            record_count: self.record_count.0,
            next_record_offset: self.next_record_offset.0,
            next_record_index: self.next_record_index.0,
            ref_backup_map_offset,
            ref_backup_scratch_offset,
            ref_backup_len,
        };

        self.map[start.0..end.0].copy_from_slice(&scratch[..entry_len]);
        let mutation_result = (|| -> Result<(), MapError> {
            match search_result {
                SearchResult::Found(index) => {
                    EntryRef::write(self.map, index, start, end)?;
                    self.next_record_offset = end;
                    Ok(())
                }
                SearchResult::NotFound(index) => {
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
                    Ok(())
                }
            }
        })();
        if let Err(error) = mutation_result {
            self.restore_from_mutation_undo(undo, scratch)?;
            return Err(error);
        }

        Ok(undo)
    }

    fn ref_backup_span(&self, search_result: SearchResult) -> Result<(usize, usize), MapError> {
        match search_result {
            SearchResult::Found(index) => Ok((index.offset(self.map)?, ENTRY_REF_SIZE)),
            SearchResult::NotFound(index) if index == self.next_record_index => Ok((0, 0)),
            SearchResult::NotFound(index) => {
                let last_index = self.next_record_index.previous();
                let end_offset = last_index.offset(self.map)?;
                let current_offset = index
                    .offset(self.map)?
                    .checked_add(ENTRY_REF_SIZE)
                    .ok_or(MapError::SerializationError)?;
                let backup_len = current_offset
                    .checked_sub(end_offset)
                    .ok_or(MapError::IndexOutOfBounds)?;
                Ok((end_offset, backup_len))
            }
        }
    }

    /// Returns the current frontier value for `key`, without consulting durable runs.
    ///
    /// A `None` result can mean either no frontier entry exists for the key or the
    /// newest frontier entry is a delete tombstone. Use [`Self::get`] for full
    /// storage-backed map visibility.
    pub fn get_frontier(&self, key: &K) -> Result<Option<V>, MapError> {
        match self.lookup_frontier(
            key,
            #[cfg(feature = "perf-counters")]
            None,
        )? {
            LookupResult::NotFound | LookupResult::Deleted => Ok(None),
            LookupResult::Set(value) => Ok(Some(value)),
        }
    }

    fn lookup_frontier(
        &self,
        key: &K,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<LookupResult<V>, MapError> {
        let search_result = self.find_index_inner(
            key,
            #[cfg(feature = "perf-counters")]
            metrics.as_deref_mut(),
        )?;
        match search_result {
            SearchResult::NotFound(_) => Ok(LookupResult::NotFound),
            SearchResult::Found(index) => {
                let entry_ref = EntryRef::read(self.map, index)?;
                #[cfg(feature = "perf-counters")]
                {
                    encoded_entry_lookup_value_metered(
                        &self.map[entry_ref.start as usize..entry_ref.end as usize],
                        metrics,
                    )
                }
                #[cfg(not(feature = "perf-counters"))]
                encoded_entry_lookup_value(
                    &self.map[entry_ref.start as usize..entry_ref.end as usize],
                )
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
        self.get_inner::<REGION_SIZE, IO>(
            flash,
            workspace,
            key,
            #[cfg(feature = "perf-counters")]
            None,
        )
    }

    #[cfg(feature = "perf-counters")]
    pub(crate) fn get_metered<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        key: &K,
        metrics: &mut StoragePerfMetrics,
    ) -> Result<Option<V>, MapStorageError> {
        self.get_inner::<REGION_SIZE, IO>(flash, workspace, key, Some(metrics))
    }

    fn get_inner<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        key: &K,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<Option<V>, MapStorageError> {
        match self.lookup_frontier(
            key,
            #[cfg(feature = "perf-counters")]
            metrics.as_deref_mut(),
        )? {
            LookupResult::Set(value) => return Ok(Some(value)),
            LookupResult::Deleted => return Ok(None),
            LookupResult::NotFound => {}
        }

        for run in self.runs.iter() {
            if !run.may_contain(key) {
                continue;
            }

            match self.lookup_run::<REGION_SIZE, IO>(
                flash,
                workspace,
                run,
                key,
                #[cfg(feature = "perf-counters")]
                metrics.as_deref_mut(),
            )? {
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
        #[cfg(feature = "perf-counters")] metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<LookupResult<V>, MapStorageError> {
        self.lookup_run_chain_inner::<REGION_SIZE, IO>(
            flash,
            workspace,
            run,
            key,
            #[cfg(feature = "perf-counters")]
            metrics,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn lookup_run_chain<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        run: &MapRunDescriptor<K>,
        key: &K,
    ) -> Result<LookupResult<V>, MapStorageError> {
        self.lookup_run_chain_inner::<REGION_SIZE, IO>(
            flash,
            workspace,
            run,
            key,
            #[cfg(feature = "perf-counters")]
            None,
        )
    }

    fn lookup_run_chain_inner<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        _workspace: &mut StorageWorkspace<REGION_SIZE>,
        run: &MapRunDescriptor<K>,
        key: &K,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<LookupResult<V>, MapStorageError> {
        let payload_end = REGION_SIZE
            .checked_sub(FreePointerFooter::ENCODED_LEN)
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        let fixed_len = Header::ENCODED_LEN
            .checked_add(RUN_SEGMENT_FIXED_SIZE)
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        if fixed_len > payload_end {
            return Err(MapStorageError::InvalidRun {
                collection_id: self.id,
                region_index: run.first_region,
            });
        }

        let mut current_region = Some(run.first_region);
        for _ in 0..run.region_count {
            #[cfg(feature = "perf-counters")]
            if let Some(metrics) = metrics.as_deref_mut() {
                metrics.increment(StoragePerfCounter::CommittedRunSegmentsChecked);
            }
            let region_index = current_region.ok_or(MapStorageError::InvalidRun {
                collection_id: self.id,
                region_index: run.first_region,
            })?;

            let (header, segment) = flash.read_region(
                region_index,
                0,
                fixed_len,
                |bytes| -> Result<(Header, RunSegmentHeader), MapStorageError> {
                    let header = Header::decode(&bytes[..Header::ENCODED_LEN])?;
                    let segment = parse_run_segment_header(&bytes[Header::ENCODED_LEN..fixed_len])
                        .map_err(|_| MapStorageError::InvalidRun {
                            collection_id: self.id,
                            region_index,
                        })?;
                    Ok((header, segment))
                },
            )??;
            if header.collection_id != self.id {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index,
                });
            }
            if header.collection_format != MAP_RUN_V2_FORMAT {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index,
                });
            }
            if segment.generation != run.generation {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index,
                });
            }

            let bounds_len = checked_add_usize(segment.lower_key_len, segment.upper_key_len)
                .map_err(MapStorageError::Map)?;
            let bounds_offset = Header::ENCODED_LEN
                .checked_add(RUN_SEGMENT_FIXED_SIZE)
                .ok_or(MapStorageError::Map(MapError::SerializationError))?;
            let snapshot_offset = bounds_offset
                .checked_add(bounds_len)
                .ok_or(MapStorageError::Map(MapError::SerializationError))?;
            let snapshot_end = snapshot_offset
                .checked_add(segment.snapshot_len)
                .ok_or(MapStorageError::Map(MapError::SerializationError))?;
            if snapshot_end > payload_end {
                return Err(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index,
                });
            }

            let (lower_order, upper_order) = flash.read_region(
                region_index,
                bounds_offset,
                bounds_len,
                |bytes| -> Result<(Ordering, Ordering), MapStorageError> {
                    let lower_end = segment.lower_key_len;
                    #[cfg(feature = "perf-counters")]
                    {
                        let lower_order = compare_encoded_key_bytes_metered(
                            &bytes[..lower_end],
                            key,
                            metrics.as_deref_mut(),
                        )?;
                        let upper_order = compare_encoded_key_bytes_metered(
                            &bytes[lower_end..],
                            key,
                            metrics.as_deref_mut(),
                        )?;
                        Ok((lower_order, upper_order))
                    }
                    #[cfg(not(feature = "perf-counters"))]
                    {
                        let lower_order = compare_encoded_key_bytes(&bytes[..lower_end], key)?;
                        let upper_order = compare_encoded_key_bytes(&bytes[lower_end..], key)?;
                        Ok((lower_order, upper_order))
                    }
                },
            )??;
            #[cfg(feature = "perf-counters")]
            if let Some(metrics) = metrics.as_deref_mut() {
                metrics.increment(StoragePerfCounter::CommittedRunBoundsReads);
            }
            if lower_order == Ordering::Greater {
                current_region = segment.next_region;
                continue;
            }
            if upper_order == Ordering::Less {
                current_region = segment.next_region;
                continue;
            }

            match lookup_run_segment_snapshot::<K, V, IO>(
                flash,
                region_index,
                snapshot_offset,
                segment.snapshot_len,
                segment.state_count,
                key,
                #[cfg(feature = "perf-counters")]
                metrics.as_deref_mut(),
            )? {
                LookupResult::NotFound => {}
                result => return Ok(result),
            }
            current_region = segment.next_region;
        }

        Ok(LookupResult::NotFound)
    }

    #[cfg(test)]
    pub(crate) fn live_run_region_count(&self) -> Result<usize, MapError> {
        let mut count = 0usize;
        for run in self.runs.iter() {
            count = count
                .checked_add(
                    usize::try_from(run.region_count).map_err(|_| MapError::SerializationError)?,
                )
                .ok_or(MapError::SerializationError)?;
        }
        Ok(count)
    }

    pub(crate) fn frontier_is_empty(&self) -> bool {
        self.record_count.0 == 0
    }

    pub(crate) fn selected_compaction_run_count(
        &self,
        run_target: usize,
    ) -> Result<Option<usize>, MapError> {
        if run_target == 0 {
            return Err(MapError::SerializationError);
        }

        let run_count = self.runs.len();
        let frontier_run_count = usize::from(!self.frontier_is_empty());
        let projected_run_count = run_count
            .checked_add(frontier_run_count)
            .ok_or(MapError::SerializationError)?;
        if projected_run_count <= run_target {
            return Ok(None);
        }

        let minimum_selected_runs = run_count
            .checked_add(1)
            .and_then(|count| count.checked_add(frontier_run_count))
            .and_then(|count| count.checked_sub(run_target))
            .ok_or(MapError::SerializationError)?;
        let minimum_selected_runs = minimum_selected_runs.min(run_count);

        let mut selected_runs = 0usize;
        let mut accumulated_states = 0u64;
        for run in self.runs.iter().take(minimum_selected_runs) {
            selected_runs = selected_runs
                .checked_add(1)
                .ok_or(MapError::SerializationError)?;
            accumulated_states = accumulated_states
                .checked_add(u64::from(run.approx_state_count))
                .ok_or(MapError::SerializationError)?;
        }

        if selected_runs == 0 {
            return Ok(None);
        }

        for run in self.runs.iter().skip(selected_runs) {
            let run_states = u64::from(run.approx_state_count);
            if accumulated_states
                .checked_mul(2)
                .ok_or(MapError::SerializationError)?
                <= run_states
            {
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

    #[cfg(test)]
    pub(crate) fn selected_compaction_state_count(
        &self,
        selected_runs: usize,
    ) -> Result<u32, MapError> {
        if selected_runs > self.runs.len() {
            return Err(MapError::IndexOutOfBounds);
        }

        let mut state_count = 0u32;
        for run in self.runs.iter().take(selected_runs) {
            state_count = state_count
                .checked_add(run.approx_state_count)
                .ok_or(MapError::SerializationError)?;
        }
        Ok(state_count)
    }

    pub(crate) fn selected_compaction_region_count(
        &self,
        selected_runs: usize,
    ) -> Result<u32, MapError> {
        if selected_runs > self.runs.len() {
            return Err(MapError::IndexOutOfBounds);
        }

        let mut region_count = 0u32;
        for run in self.runs.iter().take(selected_runs) {
            region_count = region_count
                .checked_add(run.region_count)
                .ok_or(MapError::SerializationError)?;
        }
        Ok(region_count)
    }

    pub(crate) fn write_compacted_run_to_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        selected_runs: usize,
        cursors: &mut Vec<RunEntryCursor<K, V>, MAX_RUNS>,
        duplicate_indices: &mut Vec<usize, MAX_RUNS>,
        segment_buffer: &'a mut [u8],
        segment_runs: &'a mut Vec<MapRunDescriptor<K>, MAX_RUNS>,
    ) -> Result<Option<MapRunDescriptor<K>>, MapStorageError> {
        if selected_runs == 0 {
            return Ok(None);
        }
        if selected_runs > self.runs.len() {
            return Err(MapStorageError::Map(MapError::IndexOutOfBounds));
        }

        cursors.clear();
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

        let segment =
            MapFrontier::<K, V, MAX_RUNS>::new_with_runs(self.id, segment_buffer, segment_runs)?;
        let mut writer =
            CompactionRunWriter::<K, V, MAX_RUNS>::new(self.next_run_generation(), segment);
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
            duplicate_indices.clear();
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
            writer.push::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                self.id,
                storage,
                flash,
                workspace,
                reclaim_source_regions,
                active_collections,
                reclaim_plan,
                open_plan,
                winning_entry,
            )?;
        }

        writer.finish::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            self.id,
            storage,
            flash,
            workspace,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
        )
    }

    pub(crate) fn move_unselected_runs_into(
        &mut self,
        selected_runs: usize,
        target: &mut Self,
    ) -> Result<(), MapStorageError> {
        while self.runs.len() > selected_runs {
            let run = self.runs.remove(selected_runs);
            target
                .runs
                .push(run)
                .map_err(|_| MapStorageError::TooManyRuns {
                    collection_id: self.id,
                    max_runs: MAX_RUNS,
                })?;
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

    pub(crate) fn clear_retained_runs(&mut self) {
        self.runs.clear();
    }

    pub(crate) fn reclaim_run_regions<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), MapStorageError> {
        for run in self.runs.iter() {
            let mut current_region = Some(run.first_region);
            for _ in 0..run.region_count {
                let region_index = current_region.ok_or(MapStorageError::InvalidRun {
                    collection_id: self.id,
                    region_index: run.first_region,
                })?;
                current_region = {
                    let (run_region, _) = workspace.scan_buffers();
                    let (header, payload) = read_committed_region::<REGION_SIZE, IO>(
                        flash,
                        storage.metadata(),
                        region_index,
                        run_region,
                    )?;
                    if header.collection_id != self.id {
                        return Err(MapStorageError::InvalidRun {
                            collection_id: self.id,
                            region_index,
                        });
                    }
                    if header.collection_format != MAP_RUN_V2_FORMAT {
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
                    view.next_region
                };

                storage.append_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    self.id,
                    region_index,
                )?;
            }
        }
        Ok(())
    }

    fn add_entry(
        &mut self,
        key: &K,
        value: Option<&V>,
    ) -> Result<(RecordOffset, RecordOffset), MapError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        let start = self.next_record_offset;
        let index_offset = self
            .next_record_index
            .offset(self.map)
            .map_err(|_| MapError::BufferTooSmall)?;
        if start.0 >= index_offset {
            return Err(MapError::BufferTooSmall);
        }
        let buf = &mut self.map[start.0..index_offset];
        let used = encode_entry_into(key, value, buf)?;

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
            let start = ref_to_usize(entry_ref.start)?;
            let end = ref_to_usize(entry_ref.end)?;
            let encoded_len = end.checked_sub(start).ok_or(MapError::SerializationError)?;
            entry_bytes_len = entry_bytes_len
                .checked_add(encoded_len)
                .ok_or(MapError::SerializationError)?;
        }
        SNAPSHOT_HEADER_SIZE
            .checked_add(entry_bytes_len)
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

        snapshot[..SNAPSHOT_MAGIC_SIZE].copy_from_slice(&SNAPSHOT_MAGIC);
        let entry_count_bytes = self.record_count.0.to_le_bytes();
        let entry_count_offset = SNAPSHOT_MAGIC_SIZE;
        snapshot[entry_count_offset..entry_count_offset + SNAPSHOT_ENTRY_COUNT_SIZE]
            .copy_from_slice(&entry_count_bytes);

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
        let entries_offset = SNAPSHOT_HEADER_SIZE;
        let mut write_offset = entries_offset;

        for index in 0..entry_count {
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(index))?;
            let start = ref_to_usize(entry_ref.start)?;
            let end = ref_to_usize(entry_ref.end)?;
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
        let entry_bytes_len_offset = SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE;
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
            let start = ref_to_usize(entry_ref.start)?;
            let end = ref_to_usize(entry_ref.end)?;
            let encoded_len = end.checked_sub(start).ok_or(MapError::SerializationError)?;
            entry_bytes_len = entry_bytes_len
                .checked_add(encoded_len)
                .ok_or(MapError::SerializationError)?;
        }

        SNAPSHOT_HEADER_SIZE
            .checked_add(entry_bytes_len)
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
        snapshot[..SNAPSHOT_MAGIC_SIZE].copy_from_slice(&SNAPSHOT_MAGIC);
        let entry_count_offset = SNAPSHOT_MAGIC_SIZE;
        snapshot[entry_count_offset..entry_count_offset + SNAPSHOT_ENTRY_COUNT_SIZE]
            .copy_from_slice(&entry_count_u32.to_le_bytes());

        let refs_len = entry_count
            .checked_mul(ENTRY_REF_SIZE)
            .ok_or(MapError::SerializationError)?;
        let refs_staging_start = snapshot_len
            .checked_sub(refs_len)
            .ok_or(MapError::SerializationError)?;
        let mut entry_bytes_len = 0usize;
        let mut compact_offset = ENTRY_COUNT_SIZE;
        let entries_offset = SNAPSHOT_HEADER_SIZE;
        let mut write_offset = entries_offset;

        for (target_index, source_index) in (start_index..start_index + entry_count).enumerate() {
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(source_index))?;
            let start = ref_to_usize(entry_ref.start)?;
            let end = ref_to_usize(entry_ref.end)?;
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
        let entry_bytes_len_offset = SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE;
        snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
            .copy_from_slice(&entry_bytes_len_u32.to_le_bytes());
        snapshot.copy_within(
            refs_staging_start..snapshot_len,
            entries_offset + entry_bytes_len,
        );

        Ok(snapshot_len)
    }

    fn frontier_entry(&self, index: usize) -> Result<Entry<K, V>, MapError> {
        let entry_ref = EntryRef::read(self.map, RecordIndex::new(index))?;
        let start = ref_to_usize(entry_ref.start)?;
        let end = ref_to_usize(entry_ref.end)?;
        encoded_entry_to_entry(&self.map[start..end])
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
        let (entry_count, entry_bytes_len, entries_offset, refs_start) = snapshot_parts(snapshot)?;
        let record_count =
            EntryCount(u32::try_from(entry_count).map_err(|_| MapError::SerializationError)?);

        let next_record_offset = ENTRY_COUNT_SIZE
            .checked_add(entry_bytes_len)
            .ok_or(MapError::SerializationError)?;
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
            .copy_from_slice(&snapshot[entries_offset..refs_start]);
        for index in 0..entry_count {
            let ref_offset = refs_start + index * ENTRY_REF_SIZE;
            let mut start_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
            start_bytes.copy_from_slice(&snapshot[ref_offset..ref_offset + ENTRY_REF_POINTER_SIZE]);
            let start = ref_to_usize(RefType::from_le_bytes(start_bytes))?;

            let mut end_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
            end_bytes.copy_from_slice(
                &snapshot[ref_offset + ENTRY_REF_POINTER_SIZE..ref_offset + ENTRY_REF_SIZE],
            );
            let end = ref_to_usize(RefType::from_le_bytes(end_bytes))?;

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
                Some(K::decode_key(&manifest_payload[offset..lower_end]).map_err(MapError::from)?)
            };
            let upper_key = if upper_key_len == 0 {
                None
            } else {
                Some(
                    K::decode_key(&manifest_payload[lower_end..upper_end])
                        .map_err(MapError::from)?,
                )
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
        let run_count = self
            .runs
            .len()
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
            encode_manifest_descriptor(manifest_payload, &mut offset, run)?;
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

    #[cfg(test)]
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

    pub(crate) fn restore_from_mutation_undo(
        &mut self,
        undo: MapMutationUndo,
        scratch: &[u8],
    ) -> Result<(), MapError> {
        let backup_scratch_end = undo
            .ref_backup_scratch_offset
            .checked_add(undo.ref_backup_len)
            .ok_or(MapError::SerializationError)?;
        if backup_scratch_end > scratch.len() {
            return Err(MapError::BufferTooSmall);
        }
        let backup_map_end = undo
            .ref_backup_map_offset
            .checked_add(undo.ref_backup_len)
            .ok_or(MapError::SerializationError)?;
        if backup_map_end > self.map.len() {
            return Err(MapError::IndexOutOfBounds);
        }
        self.map[undo.ref_backup_map_offset..backup_map_end]
            .copy_from_slice(&scratch[undo.ref_backup_scratch_offset..backup_scratch_end]);

        self.record_count = EntryCount(undo.record_count);
        self.next_record_offset = RecordOffset(undo.next_record_offset);
        self.next_record_index = RecordIndex::new(undo.next_record_index);
        self.record_count.write(self.map);
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
            MapUpdate::Set { key, value } => self.set_in_memory(key, value),
            MapUpdate::Delete { key } => self.delete_in_memory(key),
        }
    }

    pub(crate) fn apply_update_payload_with_undo(
        &mut self,
        payload: &[u8],
        scratch: &mut [u8],
    ) -> Result<MapMutationUndo, MapError> {
        let update: MapUpdate<K, V> = from_bytes(payload)?;
        match update {
            MapUpdate::Set { key, value } => self.set_worker_with_undo(&key, Some(&value), scratch),
            MapUpdate::Delete { key } => self.set_worker_with_undo(&key, None, scratch),
        }
    }

    fn validate_loaded_state(&self) -> Result<(), MapError> {
        let entry_count =
            usize::try_from(self.record_count.0).map_err(|_| MapError::SerializationError)?;
        let mut previous_key: Option<K> = None;
        for index in 0..entry_count {
            let entry_ref = EntryRef::read(self.map, RecordIndex::new(index))?;
            let start = ref_to_usize(entry_ref.start)?;
            let end = ref_to_usize(entry_ref.end)?;
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
                let previous_start = ref_to_usize(previous_ref.start)?;
                let previous_end = ref_to_usize(previous_ref.end)?;
                if start < previous_end && previous_start < end {
                    return Err(MapError::SerializationError);
                }
            }

            let key: K = encoded_entry_key(&self.map[start..end])?;
            if let Some(previous) = previous_key.as_ref() {
                if key.cmp(previous) != Ordering::Greater {
                    return Err(MapError::SerializationError);
                }
            }
            previous_key = Some(key);
        }
        Ok(())
    }

    fn find_index(&self, key: &K) -> Result<SearchResult, MapError> {
        self.find_index_inner(
            key,
            #[cfg(feature = "perf-counters")]
            None,
        )
    }

    fn find_index_inner(
        &self,
        key: &K,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<SearchResult, MapError> {
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
            let entry = &self.map[entry_ref.start as usize..entry_ref.end as usize];
            #[cfg(feature = "perf-counters")]
            let ordering = compare_entry_key_metered(entry, key, metrics.as_deref_mut())?;
            #[cfg(not(feature = "perf-counters"))]
            let ordering = compare_entry_key(entry, key)?;
            match ordering {
                Ordering::Equal => return Ok(SearchResult::Found(RecordIndex(mid))),
                Ordering::Greater => high_index = mid,
                Ordering::Less => {
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
    >(
        &self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        snapshot_scratch: &mut [u8],
    ) -> Result<(), MapStorageError> {
        let used = self.encode_snapshot_into(snapshot_scratch)?;
        storage.append_snapshot_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            CollectionType::MAP_CODE,
            &snapshot_scratch[..used],
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

    #[cfg(test)]
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

    #[cfg(test)]
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
    >(
        &self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
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
            let region_index = storage.reserve_next_region_for::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.id,
                reclaim_source_regions,
                active_collections,
                reclaim_plan,
                open_plan,
            )?;
            let used = {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                self.encode_run_segment_from_frontier_into(
                    payload,
                    generation,
                    next_region,
                    plan.start_index,
                    plan.entry_count,
                )?
            };
            storage
                .write_committed_region_from_workspace_payload::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    region_index,
                    self.id,
                    MAP_RUN_V2_FORMAT,
                    used,
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

    #[cfg(test)]
    fn write_snapshot_run_to_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
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
        let owns_transaction = !storage.transaction_open_for(self.id);
        if owns_transaction {
            storage.begin_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, self.id,
            )?;
        }

        loop {
            if end_index == 0 {
                break;
            }
            let plan = {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                Self::largest_snapshot_segment_ending_at(payload, generation, source, end_index)?
            };
            let region_index = storage.reserve_next_region_for::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.id,
                reclaim_source_regions,
                active_collections,
                reclaim_plan,
                open_plan,
            )?;
            let used = {
                let (payload, _) = workspace.encode_buffers();
                let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
                Self::encode_run_segment_from_snapshot_into(
                    payload,
                    generation,
                    next_region,
                    source,
                    plan.start_index,
                    plan.entry_count,
                )?
            };
            storage
                .write_committed_region_from_workspace_payload::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    region_index,
                    self.id,
                    MAP_RUN_V2_FORMAT,
                    used,
                )?;
            next_region = Some(region_index);
            first_region = Some(region_index);
            region_count = region_count
                .checked_add(1)
                .ok_or(MapError::SerializationError)?;
            end_index = plan.start_index;
        }

        if owns_transaction {
            storage.commit_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, self.id,
            )?;
            storage.finish_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, self.id,
            )?;
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
    >(
        &mut self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        extra_newest: Option<MapRunDescriptor<K>>,
    ) -> Result<u32, MapStorageError> {
        let owns_transaction = !storage.transaction_open_for(self.id);
        if owns_transaction {
            storage.begin_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, self.id,
            )?;
        }

        let previous_region = storage
            .collections()
            .iter()
            .find(|collection| collection.collection_id() == self.id)
            .and_then(|collection| match collection.basis() {
                crate::StartupCollectionBasis::Region(region_index) => Some(region_index),
                _ => None,
            });

        let manifest_run_count = self
            .runs
            .len()
            .checked_add(usize::from(extra_newest.is_some()))
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        ensure_manifest_run_capacity::<MAX_RUNS>(self.id, manifest_run_count)?;

        let manifest_region = storage.reserve_next_region_for::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
        )?;
        let used = {
            let (payload, _) = workspace.encode_buffers();
            let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
            self.encode_manifest_into(payload, extra_newest.as_ref(), None)?
        };
        storage.write_committed_region_from_workspace_payload::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            manifest_region,
            self.id,
            MAP_MANIFEST_V2_FORMAT,
            used,
        )?;
        storage.append_head_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            CollectionType::MAP_CODE,
            manifest_region,
        )?;
        storage.commit_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, self.id,
        )?;

        if let Some(previous_region) = previous_region {
            storage.append_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.id,
                previous_region,
            )?;
        }
        if owns_transaction {
            storage.finish_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, self.id,
            )?;
        }

        if let Some(run) = extra_newest {
            self.runs
                .insert(0, run)
                .map_err(|_| MapStorageError::TooManyRuns {
                    collection_id: self.id,
                    max_runs: MAX_RUNS,
                })?;
        }
        self.clear_frontier();
        Ok(manifest_region)
    }

    /// Flushes this frontier into immutable run regions and commits a manifest head.
    pub(crate) fn flush_to_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
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
        let planned_allocations = self
            .planned_frontier_run_region_count(workspace, frontier_generation)?
            .checked_add(1)
            .ok_or(MapError::SerializationError)?;
        storage.ensure_foreground_allocation_headroom_for::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
            planned_allocations,
        )?;
        storage.begin_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, self.id,
        )?;

        let frontier_run = self
            .write_frontier_run_to_storage::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                storage,
                flash,
                workspace,
                reclaim_source_regions,
                active_collections,
                reclaim_plan,
                open_plan,
                frontier_generation,
            )?;

        let manifest_run_count = self
            .runs
            .len()
            .checked_add(usize::from(frontier_run.is_some()))
            .ok_or(MapStorageError::Map(MapError::SerializationError))?;
        ensure_manifest_run_capacity::<MAX_RUNS>(self.id, manifest_run_count)?;

        let manifest_region = storage.reserve_next_region_for::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
        )?;
        let used = {
            let (payload, _) = workspace.encode_buffers();
            let payload = committed_payload_buffer::<REGION_SIZE>(payload)?;
            self.encode_manifest_into(payload, frontier_run.as_ref(), None)?
        };
        storage.write_committed_region_from_workspace_payload::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            manifest_region,
            self.id,
            MAP_MANIFEST_V2_FORMAT,
            used,
        )?;
        storage.append_head_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.id,
            CollectionType::MAP_CODE,
            manifest_region,
        )?;
        storage.commit_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, self.id,
        )?;

        if let Some(previous_region) = previous_region {
            storage.append_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.id,
                previous_region,
            )?;
        }
        storage.finish_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, self.id,
        )?;

        if let Some(run) = frontier_run {
            self.runs
                .insert(0, run)
                .map_err(|_| MapStorageError::TooManyRuns {
                    collection_id: self.id,
                    max_runs: MAX_RUNS,
                })?;
        }
        self.clear_frontier();
        Ok(manifest_region)
    }

    /// Opens a live map collection from replay-tracked storage state.
    pub fn open_from_storage<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        storage: &StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        basis_scratch: &mut [u8],
        collection_id: CollectionId,
        buffer: &'a mut [u8],
        memory: &'a mut MapFrontierMemory<K, MAX_RUNS>,
    ) -> Result<Self, MapStorageError> {
        Self::open_from_storage_inner::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            storage,
            flash,
            workspace,
            basis_scratch,
            collection_id,
            buffer,
            memory,
            #[cfg(feature = "perf-counters")]
            None,
        )
    }

    #[cfg(feature = "perf-counters")]
    pub(crate) fn open_from_storage_metered<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        storage: &StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        basis_scratch: &mut [u8],
        collection_id: CollectionId,
        buffer: &'a mut [u8],
        memory: &'a mut MapFrontierMemory<K, MAX_RUNS>,
        metrics: &mut StoragePerfMetrics,
    ) -> Result<Self, MapStorageError> {
        Self::open_from_storage_inner::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            storage,
            flash,
            workspace,
            basis_scratch,
            collection_id,
            buffer,
            memory,
            Some(metrics),
        )
    }

    fn open_from_storage_inner<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        const MAX_COLLECTIONS: usize,
    >(
        storage: &StorageRuntime<MAX_COLLECTIONS>,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        basis_scratch: &mut [u8],
        collection_id: CollectionId,
        buffer: &'a mut [u8],
        memory: &'a mut MapFrontierMemory<K, MAX_RUNS>,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
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

        let mut map = Self::new(collection_id, buffer, memory)?;
        let target_basis = collection.basis();
        let mut basis_loaded = matches!(target_basis, crate::StartupCollectionBasis::Empty);
        #[cfg(feature = "perf-counters")]
        if let Some(metrics) = metrics.as_deref_mut() {
            metrics.increment(StoragePerfCounter::FrontierOpenWalScans);
        }

        macro_rules! visit_wal_records_for_map {
            (plain) => {
                storage.visit_wal_records::<REGION_SIZE, IO, _, _>(
                    flash,
                    workspace,
                    |flash: &mut IO, record| -> Result<(), MapStorageError> {
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
                                if target_basis
                                    == crate::StartupCollectionBasis::Region(region_index)
                                {
                                    load_map_basis_from_flash::<REGION_SIZE, IO, K, V, MAX_RUNS>(
                                        flash,
                                        storage.metadata(),
                                        collection_id,
                                        region_index,
                                        basis_scratch,
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
                )
            };
            (metered $metrics:expr) => {
                storage.visit_wal_records_metered::<REGION_SIZE, IO, _, _>(
                    flash,
                    workspace,
                    $metrics,
                    |flash: &mut IO, record| -> Result<(), MapStorageError> {
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
                                if target_basis
                                    == crate::StartupCollectionBasis::Region(region_index)
                                {
                                    load_map_basis_from_flash::<REGION_SIZE, IO, K, V, MAX_RUNS>(
                                        flash,
                                        storage.metadata(),
                                        collection_id,
                                        region_index,
                                        basis_scratch,
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
                )
            };
        }

        #[cfg(feature = "perf-counters")]
        let visit_result = match metrics {
            Some(metrics) => visit_wal_records_for_map!(metered metrics),
            None => visit_wal_records_for_map!(plain),
        };
        #[cfg(not(feature = "perf-counters"))]
        let visit_result = visit_wal_records_for_map!(plain);

        match visit_result {
            Ok(()) => Ok(map),
            Err(StorageVisitError::Storage(error)) => Err(MapStorageError::Storage(error)),
            Err(StorageVisitError::Visitor(error)) => Err(error),
        }
    }
}

fn load_map_basis_from_flash<const REGION_SIZE: usize, IO: FlashIo, K, V, const MAX_RUNS: usize>(
    flash: &mut IO,
    metadata: crate::StorageMetadata,
    collection_id: CollectionId,
    region_index: u32,
    basis_scratch: &mut [u8],
    map: &mut MapFrontier<'_, K, V, MAX_RUNS>,
) -> Result<(), MapStorageError>
where
    K: LsmKey,
    V: LsmValue,
{
    let region_slice = basis_scratch
        .get_mut(..REGION_SIZE)
        .ok_or(MapStorageError::Map(MapError::BufferTooSmall))?;
    let region: &mut [u8; REGION_SIZE] = region_slice
        .try_into()
        .map_err(|_| MapStorageError::Map(MapError::BufferTooSmall))?;
    let (header, payload) =
        read_committed_region::<REGION_SIZE, IO>(flash, metadata, region_index, region)?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::UnknownCollection(collection_id));
    }

    match header.collection_format {
        MAP_REGION_V2_FORMAT => {
            return Err(MapStorageError::UnsupportedRegionFormat {
                collection_id,
                region_index,
                actual: MAP_REGION_V2_FORMAT,
            });
        }
        MAP_MANIFEST_V2_FORMAT => {
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

fn push_unique_collected_region<const CAP: usize>(
    regions: &mut Vec<u32, CAP>,
    collection_id: CollectionId,
    manifest_region: u32,
    region_index: u32,
) -> Result<(), MapStorageError> {
    if regions.contains(&region_index) {
        return Ok(());
    }
    regions
        .push(region_index)
        .map_err(|_| MapStorageError::InvalidManifest {
            collection_id,
            region_index: manifest_region,
        })
}

pub(crate) fn collect_map_head_regions<const REGION_SIZE: usize, IO: FlashIo, const CAP: usize>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    collection_id: CollectionId,
    head_region: u32,
    regions: &mut Vec<u32, CAP>,
) -> Result<(), MapStorageError> {
    let (manifest_region, run_region) = workspace.scan_buffers();
    let (header, payload) =
        read_committed_region::<REGION_SIZE, IO>(flash, metadata, head_region, manifest_region)?;
    if header.collection_id != collection_id {
        return Err(MapStorageError::UnknownCollection(collection_id));
    }
    if header.collection_format == MAP_REGION_V2_FORMAT {
        return Err(MapStorageError::UnsupportedRegionFormat {
            collection_id,
            region_index: head_region,
            actual: MAP_REGION_V2_FORMAT,
        });
    }
    if header.collection_format != MAP_MANIFEST_V2_FORMAT {
        return Err(MapStorageError::UnsupportedRegionFormat {
            collection_id,
            region_index: head_region,
            actual: header.collection_format,
        });
    }
    push_unique_collected_region(regions, collection_id, head_region, head_region)?;

    let mut offset = 0usize;
    let run_count = usize::try_from(read_u32(payload, &mut offset)?).map_err(|_| {
        MapStorageError::InvalidManifest {
            collection_id,
            region_index: head_region,
        }
    })?;
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
            push_unique_collected_region(regions, collection_id, head_region, region_index)?;

            let (run_header, run_payload) = read_committed_region::<REGION_SIZE, IO>(
                flash,
                metadata,
                region_index,
                run_region,
            )?;
            if run_header.collection_id != collection_id {
                return Err(MapStorageError::InvalidRun {
                    collection_id,
                    region_index,
                });
            }
            if run_header.collection_format != MAP_RUN_V2_FORMAT {
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

    Ok(())
}

#[cfg(test)]
pub(crate) fn map_head_region_references_region<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    collection_id: CollectionId,
    head_region: u32,
    target_region: u32,
) -> Result<bool, MapStorageError> {
    let mut regions = Vec::<u32, 128>::new();
    collect_map_head_regions::<REGION_SIZE, IO, 128>(
        flash,
        workspace,
        metadata,
        collection_id,
        head_region,
        &mut regions,
    )?;
    Ok(regions.contains(&target_region))
}
