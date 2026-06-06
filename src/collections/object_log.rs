use core::{fmt, mem::size_of};

use crc::{Crc, CRC_32_ISCSI};
use heapless::Vec;

use crate::disk::{FreePointerFooter, Header};
use crate::flash_io::FlashIo;
use crate::mode::{CollectionUpdateMode, ReadMode, StorageMode};
use crate::startup::StartupCollectionBasis;
use crate::storage::{FreeRegionPreparation, StorageRuntimeError, StorageVisitError};
use crate::wal_record::WalRecord;
use crate::{Collection, CollectionId, CollectionType, Storage, StorageMetadata};

#[cfg(test)]
mod tests;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Stable committed-region format for object-log data regions.
pub const OBJECT_LOG_DATA_V1_FORMAT: u16 = 7;

const DATA_MAGIC: [u8; 4] = *b"OLOG";
const DATA_VERSION: u16 = 1;
const DATA_PROLOGUE_FIXED_LEN: usize =
    size_of::<u32>() + size_of::<u16>() + size_of::<u64>() + size_of::<u32>();
const RECORD_HEADER_LEN: usize = size_of::<u8>() + size_of::<u32>() + size_of::<u32>();
const RECORD_INLINE_OBJECT: u8 = 0x01;
const RECORD_OBJECT_CHUNK: u8 = 0x02;
const RECORD_OBJECT_END: u8 = 0x03;
const OBJECT_CHUNK_FIXED_BODY_LEN: usize =
    size_of::<u8>() + size_of::<u64>() + size_of::<u32>() + HANDLE_ENCODED_LEN + HANDLE_ENCODED_LEN;
const OBJECT_END_BODY_LEN: usize = size_of::<u64>() + HANDLE_ENCODED_LEN + HANDLE_ENCODED_LEN;
const OBJECT_CHUNK_FLAG_PREV_VALID: u8 = 0x01;
const OBJECT_CHUNK_FLAG_NEXT_VALID: u8 = 0x02;
const OBJECT_CHUNK_FLAGS_VALID_MASK: u8 =
    OBJECT_CHUNK_FLAG_PREV_VALID | OBJECT_CHUNK_FLAG_NEXT_VALID;

const SNAPSHOT_MAGIC: [u8; 4] = *b"OLGS";
const SNAPSHOT_VERSION: u16 = 4;
const HANDLE_ENCODED_LEN: usize = 2 * size_of::<u32>() + size_of::<u64>();

const UPDATE_APPEND: u8 = 1;
const UPDATE_TRUNCATE_HEAD: u8 = 2;
const UPDATE_SET_LOG_METADATA: u8 = 3;
const UPDATE_MATERIALIZED_REGION: u8 = 4;

/// Stable object address returned by [`ObjectLog::append`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectLogHandle {
    region_index: u32,
    sequence: u64,
    offset: u32,
}

impl fmt::Debug for ObjectLogHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ObjectLogHandle { .. }")
    }
}

impl ObjectLogHandle {
    /// Creates a handle from its checked fields.
    pub(crate) const fn new(region_index: u32, sequence: u64, offset: u32) -> Self {
        Self {
            region_index,
            sequence,
            offset,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObjectLogRegion {
    region_index: u32,
    sequence: u64,
    start_offset: u32,
    end_offset: u32,
    committed_end_offset: u32,
    first_committed_public_offset: Option<u32>,
    first_planned_public_offset: Option<u32>,
    flushed: bool,
}

impl ObjectLogRegion {
    fn contains_committed(self, handle: ObjectLogHandle) -> bool {
        self.region_index == handle.region_index
            && self.sequence == handle.sequence
            && handle.offset >= self.start_offset
            && handle.offset < self.committed_end_offset
    }
}

#[derive(Clone, Copy)]
struct ObjectLogRecordInfo {
    record_type: u8,
    body_len: usize,
    body_crc32c: u32,
    body_start: usize,
    record_end: u32,
}

#[derive(Clone, Copy)]
struct ObjectChunkInfo {
    flags: u8,
    logical_start: u64,
    chunk_len: usize,
    prev: ObjectLogHandle,
    next: ObjectLogHandle,
}

#[derive(Clone, Copy)]
struct ObjectEndInfo {
    total_object_len: u64,
    first: ObjectLogHandle,
    last: ObjectLogHandle,
}

#[derive(Clone, Copy)]
struct ReservedObjectLogRegion {
    region_index: u32,
    sequence: u64,
}

#[derive(Clone, Copy)]
struct EncodedRecordUpdate {
    used: usize,
    record_start: usize,
}

#[derive(Clone, Copy)]
enum AppendVisibility {
    Planned,
    Committed,
}

struct ObjectLogReplayTransaction {
    committed: bool,
}

/// Caller-owned memory for an [`ObjectLog`].
pub struct ObjectLogMemory<
    const REGION_SIZE: usize,
    const MAX_REGIONS: usize = 16,
    const LOG_METADATA_MAX: usize = 64,
> {
    regions: Vec<ObjectLogRegion, MAX_REGIONS>,
    frontier_payload: [u8; REGION_SIZE],
    rollback_regions: Vec<ObjectLogRegion, MAX_REGIONS>,
    rollback_frontier_payload: [u8; REGION_SIZE],
    log_metadata: [u8; LOG_METADATA_MAX],
    log_metadata_len: usize,
    next_sequence: u64,
}

impl<const REGION_SIZE: usize, const MAX_REGIONS: usize, const LOG_METADATA_MAX: usize>
    ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>
{
    /// Allocates empty object-log memory.
    pub fn new() -> Self {
        Self {
            regions: Vec::new(),
            frontier_payload: [0; REGION_SIZE],
            rollback_regions: Vec::new(),
            rollback_frontier_payload: [0; REGION_SIZE],
            log_metadata: [0; LOG_METADATA_MAX],
            log_metadata_len: 0,
            next_sequence: 0,
        }
    }

    fn clear(&mut self) {
        self.regions.clear();
        self.frontier_payload.fill(0);
        self.rollback_regions.clear();
        self.rollback_frontier_payload.fill(0);
        self.log_metadata.fill(0);
        self.log_metadata_len = 0;
        self.next_sequence = 0;
    }
}

impl<const REGION_SIZE: usize, const MAX_REGIONS: usize, const LOG_METADATA_MAX: usize> Default
    for ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>
{
    fn default() -> Self {
        Self::new()
    }
}

/// Durable opaque object log handle.
pub struct ObjectLog<
    'mem,
    const REGION_SIZE: usize,
    const MAX_REGIONS: usize = 16,
    const LOG_METADATA_MAX: usize = 64,
> {
    collection_id: CollectionId,
    memory: &'mem mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
}

/// Scoped append-only object-log transaction.
pub struct ObjectLogTransaction<
    'tx,
    'mem,
    'db,
    'storage_mem,
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize = 16,
    const LOG_METADATA_MAX: usize = 64,
> {
    log: &'tx mut ObjectLog<'mem, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &'tx mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    allocated_regions: Vec<u32, REGION_COUNT>,
}

impl<
        'tx,
        'mem,
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_REGIONS: usize,
        const LOG_METADATA_MAX: usize,
    >
    ObjectLogTransaction<
        'tx,
        'mem,
        'db,
        'storage_mem,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_REGIONS,
        LOG_METADATA_MAX,
    >
{
    /// Appends an object to this transaction and returns its planned stable handle.
    pub fn append(&mut self, bytes: &[u8]) -> Result<ObjectLogHandle, ObjectLogError> {
        self.log
            .append_transactional(self.storage, bytes, &mut self.allocated_regions)
    }
}

impl<const REGION_SIZE: usize, const MAX_REGIONS: usize, const LOG_METADATA_MAX: usize> Collection
    for ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>
{
    fn id(&self) -> CollectionId {
        self.collection_id
    }

    fn collection_type(&self) -> CollectionType {
        CollectionType::ObjectLog
    }
}

impl<'mem, const REGION_SIZE: usize, const MAX_REGIONS: usize, const LOG_METADATA_MAX: usize>
    ObjectLog<'mem, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>
{
    /// Creates a new object-log collection.
    pub fn new<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        memory: &'mem mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
        log_metadata: &[u8],
    ) -> Result<Self, ObjectLogError> {
        validate_log_metadata_len::<LOG_METADATA_MAX>(log_metadata.len())?;
        let collection_id = storage.allocate_collection_id()?;
        memory.clear();

        storage.append_new_collection(collection_id, CollectionType::OBJECT_LOG_CODE)?;
        let mut update = [0u8; REGION_SIZE];
        let used = encode_set_log_metadata_update(log_metadata, &mut update)?;
        storage.append_update(collection_id, &update[..used])?;
        let mut log = Self {
            collection_id,
            memory,
        };
        log.apply_log_metadata(log_metadata)?;
        log.validate_open_state(storage)?;
        Ok(log)
    }

    /// Opens an existing object-log collection.
    pub fn open<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        collection_id: CollectionId,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        memory: &'mem mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    ) -> Result<Self, ObjectLogError> {
        validate_collection::<IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>(
            storage,
            collection_id,
        )?;
        memory.clear();
        replay_object_log::<
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_REGIONS,
            LOG_METADATA_MAX,
        >(storage, collection_id, memory)?;
        let log = Self {
            collection_id,
            memory,
        };
        log.validate_open_state(storage)?;
        Ok(log)
    }

    /// Returns the stable collection id.
    pub fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    /// Appends an object and returns its stable handle.
    pub fn append<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        bytes: &[u8],
    ) -> Result<ObjectLogHandle, ObjectLogError> {
        storage.enter_mode(StorageMode::UpdatingCollection(
            CollectionUpdateMode::Running,
        ))?;
        let result = self.append_inner(storage, bytes);
        storage.finish_mode();
        result
    }

    /// Flushes the current WAL-backed frontier into its reserved data region.
    pub fn flush<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<(), ObjectLogError> {
        storage.enter_mode(StorageMode::FlushingCollection(
            crate::mode::CollectionFlushMode::CommitRegion,
        ))?;
        let result = self.flush_current(storage);
        storage.finish_mode();
        result
    }

    /// Fetches an object and passes its bytes to `read`.
    pub fn get<
        'db,
        'storage_mem,
        IO: FlashIo,
        R,
        F,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
        scratch: &mut [u8],
        read: F,
    ) -> Result<R, ObjectLogError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        storage.enter_mode(StorageMode::ReadingStorage(ReadMode::Running))?;
        let result = self.get_inner(storage, handle, scratch, read);
        storage.finish_mode();
        result
    }

    /// Returns the stored object length without returning object bytes.
    pub fn get_object_len<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<u64, ObjectLogError> {
        storage.enter_mode(StorageMode::ReadingStorage(ReadMode::Running))?;
        let result = self.get_object_len_inner(storage, handle);
        storage.finish_mode();
        result
    }

    /// Fetches a byte range from an object and passes those bytes to `read`.
    pub fn get_range<
        'db,
        'storage_mem,
        IO: FlashIo,
        R,
        F,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
        offset: u64,
        len: u64,
        scratch: &mut [u8],
        read: F,
    ) -> Result<R, ObjectLogError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        storage.enter_mode(StorageMode::ReadingStorage(ReadMode::Running))?;
        let result = self.get_range_inner(storage, handle, offset, len, scratch, read);
        storage.finish_mode();
        result
    }

    /// Advances the live log head to immediately before `handle`.
    pub fn truncate_before<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<(), ObjectLogError> {
        storage.enter_mode(StorageMode::UpdatingCollection(
            CollectionUpdateMode::Running,
        ))?;
        let result = self.truncate_before_inner(storage, handle);
        storage.finish_mode();
        result
    }

    /// Runs an append-only transaction whose objects become visible at commit.
    pub fn transaction<
        'db,
        'storage_mem,
        IO: FlashIo,
        T,
        F,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        body: F,
    ) -> Result<T, ObjectLogError>
    where
        F: for<'tx> FnOnce(
            &mut ObjectLogTransaction<
                'tx,
                'mem,
                'db,
                'storage_mem,
                IO,
                REGION_SIZE,
                REGION_COUNT,
                MAX_COLLECTIONS,
                MAX_REGIONS,
                LOG_METADATA_MAX,
            >,
        ) -> Result<T, ObjectLogError>,
    {
        storage.enter_mode(StorageMode::UpdatingCollection(
            CollectionUpdateMode::Running,
        ))?;
        let result = self.transaction_inner(storage, body);
        storage.finish_mode();
        result
    }

    /// Reads immutable opaque log metadata.
    pub fn get_log_metadata<R, F>(&self, read: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        read(&self.memory.log_metadata[..self.memory.log_metadata_len])
    }

    /// Returns the first committed live object handle, if the log is non-empty.
    pub fn first_handle(&self) -> Option<ObjectLogHandle> {
        self.memory.regions.iter().copied().find_map(|region| {
            region
                .first_committed_public_offset
                .map(|offset| ObjectLogHandle::new(region.region_index, region.sequence, offset))
        })
    }

    /// Returns the committed live object handle after `handle`, if one exists.
    pub fn next_handle<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<Option<ObjectLogHandle>, ObjectLogError> {
        storage.enter_mode(StorageMode::ReadingStorage(ReadMode::Running))?;
        let result = self.next_handle_inner(storage, handle);
        storage.finish_mode();
        result
    }

    fn append_inner<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        bytes: &[u8],
    ) -> Result<ObjectLogHandle, ObjectLogError> {
        let record_len = inline_record_len(bytes.len())?;
        let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata())?;
        if record_len
            > empty_region_record_capacity(payload_capacity, self.memory.log_metadata_len)?
        {
            return self.append_in_transaction(storage, bytes);
        }

        if self.needs_new_region(record_len, payload_capacity)? {
            return self.append_in_transaction(storage, bytes);
        }

        let region = self
            .memory
            .regions
            .last()
            .copied()
            .ok_or(ObjectLogError::MissingFrontier)?;
        let handle = ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
        let encoded =
            encode_inline_append_update(handle, bytes, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..encoded.used],
            )?;
        self.apply_append_record(
            handle,
            &storage.memory.payload_scratch[encoded.record_start..encoded.used],
            AppendVisibility::Committed,
        )?;
        if usize::try_from(handle.offset).map_err(|_| ObjectLogError::LengthOverflow)?
            < Header::ENCODED_LEN + self.object_payload_start()?
        {
            return Err(ObjectLogError::InvalidHandle);
        }
        Ok(handle)
    }

    fn append_in_transaction<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        bytes: &[u8],
    ) -> Result<ObjectLogHandle, ObjectLogError> {
        self.checkpoint_append_state()?;
        let mut allocated_regions = Vec::<u32, REGION_COUNT>::new();
        storage
            .memory
            .state
            .begin_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
            )?;
        let handle = match self.append_transactional(storage, bytes, &mut allocated_regions) {
            Ok(handle) => handle,
            Err(error) => {
                return match self.rollback_transaction(storage, allocated_regions) {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(cleanup_error),
                };
            }
        };
        if let Err(error) = storage
            .memory
            .state
            .commit_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
            )
        {
            return match self.rollback_transaction(storage, allocated_regions) {
                Ok(()) => Err(error.into()),
                Err(cleanup_error) => Err(cleanup_error),
            };
        }
        self.commit_staged_appends();
        self.clear_append_checkpoint();
        storage
            .memory
            .state
            .finish_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
            )?;
        Ok(handle)
    }

    fn transaction_inner<
        'db,
        'storage_mem,
        IO: FlashIo,
        T,
        F,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        body: F,
    ) -> Result<T, ObjectLogError>
    where
        F: for<'tx> FnOnce(
            &mut ObjectLogTransaction<
                'tx,
                'mem,
                'db,
                'storage_mem,
                IO,
                REGION_SIZE,
                REGION_COUNT,
                MAX_COLLECTIONS,
                MAX_REGIONS,
                LOG_METADATA_MAX,
            >,
        ) -> Result<T, ObjectLogError>,
    {
        self.checkpoint_append_state()?;
        storage
            .memory
            .state
            .begin_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
            )?;

        let mut transaction = ObjectLogTransaction {
            log: self,
            storage,
            allocated_regions: Vec::new(),
        };
        let result = body(&mut transaction);
        let ObjectLogTransaction {
            log,
            storage,
            allocated_regions,
        } = transaction;

        match result {
            Ok(value) => {
                if let Err(error) = storage
                    .memory
                    .state
                    .commit_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                        storage.backing,
                        &mut storage.memory.workspace,
                        log.collection_id,
                    )
                {
                    return match log.rollback_transaction(storage, allocated_regions) {
                        Ok(()) => Err(error.into()),
                        Err(cleanup_error) => Err(cleanup_error),
                    };
                }
                log.commit_staged_appends();
                log.clear_append_checkpoint();
                storage
                    .memory
                    .state
                    .finish_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                        storage.backing,
                        &mut storage.memory.workspace,
                        log.collection_id,
                    )?;
                Ok(value)
            }
            Err(error) => match log.rollback_transaction(storage, allocated_regions) {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(cleanup_error),
            },
        }
    }

    fn append_transactional<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        bytes: &[u8],
        allocated_regions: &mut Vec<u32, REGION_COUNT>,
    ) -> Result<ObjectLogHandle, ObjectLogError> {
        let record_len = inline_record_len(bytes.len())?;
        let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata())?;
        if record_len
            > empty_region_record_capacity(payload_capacity, self.memory.log_metadata_len)?
        {
            return self.append_large_transactional(storage, bytes, allocated_regions);
        }

        if self.needs_new_region(record_len, payload_capacity)? {
            if storage
                .memory
                .state
                .transaction_open_for(self.collection_id)
            {
                self.materialize_current_frontier_in_transaction(storage)?;
            } else {
                self.flush_current(storage)?;
            }
            return self.append_transactional_new_region(storage, bytes, allocated_regions);
        }

        let region = self
            .memory
            .regions
            .last()
            .copied()
            .ok_or(ObjectLogError::MissingFrontier)?;
        let handle = ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
        let encoded =
            encode_inline_append_update(handle, bytes, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..encoded.used],
            )?;
        self.apply_append_record(
            handle,
            &storage.memory.payload_scratch[encoded.record_start..encoded.used],
            AppendVisibility::Planned,
        )?;
        Ok(handle)
    }

    fn append_transactional_new_region<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        bytes: &[u8],
        allocated_regions: &mut Vec<u32, REGION_COUNT>,
    ) -> Result<ObjectLogHandle, ObjectLogError> {
        let record_len = inline_record_len(bytes.len())?;
        let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata())?;
        if record_len
            > empty_region_record_capacity(payload_capacity, self.memory.log_metadata_len)?
        {
            return self.append_large_transactional(storage, bytes, allocated_regions);
        }

        let reserved = self.reserve_region(storage, allocated_regions)?;
        self.install_reserved_frontier(reserved)?;
        let region = self
            .memory
            .regions
            .last()
            .copied()
            .ok_or(ObjectLogError::MissingFrontier)?;
        let handle = ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
        let encoded =
            encode_inline_append_update(handle, bytes, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..encoded.used],
            )?;
        self.apply_append_record(
            handle,
            &storage.memory.payload_scratch[encoded.record_start..encoded.used],
            AppendVisibility::Planned,
        )?;
        Ok(handle)
    }

    fn rollback_transaction<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        allocated_regions: Vec<u32, REGION_COUNT>,
    ) -> Result<(), ObjectLogError> {
        self.restore_append_checkpoint();
        let mut first_error = None::<ObjectLogError>;
        if let Err(error) = storage
            .memory
            .state
            .rollback_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
            )
        {
            if first_error.is_none() {
                first_error = Some(error.into());
            }
        }
        for region_index in allocated_regions {
            if let Err(error) = storage
                .memory
                .state
                .append_free_region_with_rotation_prepared::<REGION_SIZE, REGION_COUNT, IO>(
                    storage.backing,
                    &mut storage.memory.workspace,
                    CollectionId(0),
                    region_index,
                    FreeRegionPreparation::EraseToUnwrittenFooter,
                )
            {
                if first_error.is_none() {
                    first_error = Some(error.into());
                }
            }
        }
        self.clear_append_checkpoint();
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn reserve_region<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        allocated_regions: &mut Vec<u32, REGION_COUNT>,
    ) -> Result<ReservedObjectLogRegion, ObjectLogError> {
        let sequence = self.memory.next_sequence;
        let _ = next_sequence_after(sequence)?;
        let region_index = storage
            .memory
            .state
            .reserve_next_region_for::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &mut storage.memory.reclaim_source_regions,
                &mut storage.memory.active_collections,
                &mut storage.memory.reclaim_plan,
                &mut storage.memory.open_plan,
            )?;
        allocated_regions
            .push(region_index)
            .map_err(|_| ObjectLogError::TooManyRegions)?;
        Ok(ReservedObjectLogRegion {
            region_index,
            sequence,
        })
    }

    fn install_reserved_frontier(
        &mut self,
        reserved: ReservedObjectLogRegion,
    ) -> Result<(), ObjectLogError> {
        let start = u32::try_from(Header::ENCODED_LEN + self.object_payload_start()?)
            .map_err(|_| ObjectLogError::LengthOverflow)?;
        let region = ObjectLogRegion {
            region_index: reserved.region_index,
            sequence: reserved.sequence,
            start_offset: start,
            end_offset: start,
            committed_end_offset: start,
            first_committed_public_offset: None,
            first_planned_public_offset: None,
            flushed: false,
        };
        self.memory
            .regions
            .push(region)
            .map_err(|_| ObjectLogError::TooManyRegions)?;
        self.initialize_frontier_payload(reserved.sequence)?;
        self.memory.next_sequence = self
            .memory
            .next_sequence
            .max(next_sequence_after(reserved.sequence)?);
        Ok(())
    }

    fn ensure_transaction_frontier<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        allocated_regions: &mut Vec<u32, REGION_COUNT>,
    ) -> Result<(), ObjectLogError> {
        if self
            .memory
            .regions
            .last()
            .is_some_and(|region| !region.flushed)
        {
            return Ok(());
        }
        let reserved = self.reserve_region(storage, allocated_regions)?;
        self.install_reserved_frontier(reserved)
    }

    fn materialize_current_frontier_in_transaction<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<(), ObjectLogError> {
        let Some(index) = self.memory.regions.len().checked_sub(1) else {
            return Ok(());
        };
        let region = self
            .memory
            .regions
            .get(index)
            .copied()
            .ok_or(ObjectLogError::InvalidHandle)?;
        if region.flushed || region.end_offset == region.start_offset {
            return Ok(());
        }
        let payload_len = payload_offset(region.end_offset)?;
        storage
            .memory
            .state
            .write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                region.region_index,
                self.collection_id,
                OBJECT_LOG_DATA_V1_FORMAT,
                &self.memory.frontier_payload[..payload_len],
            )?;
        self.memory
            .regions
            .get_mut(index)
            .ok_or(ObjectLogError::InvalidHandle)?
            .flushed = true;
        let region = self
            .memory
            .regions
            .get(index)
            .copied()
            .ok_or(ObjectLogError::InvalidHandle)?;
        let used = encode_materialized_region_update(region, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..used],
            )?;
        Ok(())
    }

    fn append_large_transactional<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        bytes: &[u8],
        allocated_regions: &mut Vec<u32, REGION_COUNT>,
    ) -> Result<ObjectLogHandle, ObjectLogError> {
        let total_object_len =
            u64::try_from(bytes.len()).map_err(|_| ObjectLogError::LengthOverflow)?;
        let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata())?;
        let max_region_end = Header::ENCODED_LEN
            .checked_add(payload_capacity)
            .ok_or(ObjectLogError::LengthOverflow)?;
        let mut cursor = 0usize;
        let mut logical_start = 0u64;
        let mut first_chunk = None::<ObjectLogHandle>;
        let mut previous_chunk = None::<ObjectLogHandle>;

        loop {
            self.ensure_transaction_frontier(storage, allocated_regions)?;
            let region = self
                .memory
                .regions
                .last()
                .copied()
                .ok_or(ObjectLogError::MissingFrontier)?;
            if region.flushed {
                return Err(ObjectLogError::MissingFrontier);
            }
            let record_offset =
                usize::try_from(region.end_offset).map_err(|_| ObjectLogError::LengthOverflow)?;
            let nonfinal_capacity =
                chunk_payload_capacity_at(record_offset, max_region_end, false)?;
            if nonfinal_capacity == 0 {
                self.materialize_current_frontier_in_transaction(storage)?;
                continue;
            }

            let remaining = bytes
                .len()
                .checked_sub(cursor)
                .ok_or(ObjectLogError::LengthOverflow)?;
            let final_same_region_capacity =
                chunk_payload_capacity_at(record_offset, max_region_end, true)?;

            if remaining <= final_same_region_capacity {
                let chunk_handle =
                    ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
                let chunk_len = remaining;
                let end_offset = region
                    .end_offset
                    .checked_add(
                        u32::try_from(chunk_record_len(chunk_len)?)
                            .map_err(|_| ObjectLogError::LengthOverflow)?,
                    )
                    .ok_or(ObjectLogError::LengthOverflow)?;
                let end_handle =
                    ObjectLogHandle::new(region.region_index, region.sequence, end_offset);
                let previous = previous_chunk.unwrap_or(ObjectLogHandle::new(0, 0, 0));
                let flags = if previous_chunk.is_some() {
                    OBJECT_CHUNK_FLAG_PREV_VALID
                } else {
                    0
                };
                let encoded = encode_chunk_append_update(
                    chunk_handle,
                    flags,
                    logical_start,
                    previous,
                    ObjectLogHandle::new(0, 0, 0),
                    &bytes[cursor..cursor + chunk_len],
                    &mut storage.memory.payload_scratch,
                )?;
                storage
                    .memory
                    .state
                    .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        storage.backing,
                        &mut storage.memory.workspace,
                        self.collection_id,
                        &storage.memory.payload_scratch[..encoded.used],
                    )?;
                self.apply_append_record(
                    chunk_handle,
                    &storage.memory.payload_scratch[encoded.record_start..encoded.used],
                    AppendVisibility::Planned,
                )?;
                let first = first_chunk.unwrap_or(chunk_handle);
                let encoded = encode_end_append_update(
                    end_handle,
                    total_object_len,
                    first,
                    chunk_handle,
                    &mut storage.memory.payload_scratch,
                )?;
                storage
                    .memory
                    .state
                    .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        storage.backing,
                        &mut storage.memory.workspace,
                        self.collection_id,
                        &storage.memory.payload_scratch[..encoded.used],
                    )?;
                self.apply_append_record(
                    end_handle,
                    &storage.memory.payload_scratch[encoded.record_start..encoded.used],
                    AppendVisibility::Planned,
                )?;
                return Ok(end_handle);
            }

            if remaining <= nonfinal_capacity {
                let chunk_handle =
                    ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
                let previous = previous_chunk.unwrap_or(ObjectLogHandle::new(0, 0, 0));
                let flags = if previous_chunk.is_some() {
                    OBJECT_CHUNK_FLAG_PREV_VALID
                } else {
                    0
                };
                let record_len = encode_chunk_record(
                    flags,
                    logical_start,
                    previous,
                    ObjectLogHandle::new(0, 0, 0),
                    &bytes[cursor..cursor + remaining],
                    &mut storage.memory.payload_scratch,
                )?;
                self.apply_append_record(
                    chunk_handle,
                    &storage.memory.payload_scratch[..record_len],
                    AppendVisibility::Planned,
                )?;
                self.materialize_current_frontier_in_transaction(storage)?;
                let first = first_chunk.unwrap_or(chunk_handle);
                let reserved = self.reserve_region(storage, allocated_regions)?;
                self.install_reserved_frontier(reserved)?;
                let region = self
                    .memory
                    .regions
                    .last()
                    .copied()
                    .ok_or(ObjectLogError::MissingFrontier)?;
                let end_handle =
                    ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
                let encoded = encode_end_append_update(
                    end_handle,
                    total_object_len,
                    first,
                    chunk_handle,
                    &mut storage.memory.payload_scratch,
                )?;
                storage
                    .memory
                    .state
                    .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        storage.backing,
                        &mut storage.memory.workspace,
                        self.collection_id,
                        &storage.memory.payload_scratch[..encoded.used],
                    )?;
                self.apply_append_record(
                    end_handle,
                    &storage.memory.payload_scratch[encoded.record_start..encoded.used],
                    AppendVisibility::Planned,
                )?;
                return Ok(end_handle);
            }

            let chunk_len = nonfinal_capacity;
            let reserved = self.reserve_region(storage, allocated_regions)?;
            let next_offset = u32::try_from(Header::ENCODED_LEN + self.object_payload_start()?)
                .map_err(|_| ObjectLogError::LengthOverflow)?;
            let next_chunk =
                ObjectLogHandle::new(reserved.region_index, reserved.sequence, next_offset);
            let chunk_handle =
                ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
            let previous = previous_chunk.unwrap_or(ObjectLogHandle::new(0, 0, 0));
            let mut flags = OBJECT_CHUNK_FLAG_NEXT_VALID;
            if previous_chunk.is_some() {
                flags |= OBJECT_CHUNK_FLAG_PREV_VALID;
            }
            let record_len = encode_chunk_record(
                flags,
                logical_start,
                previous,
                next_chunk,
                &bytes[cursor..cursor + chunk_len],
                &mut storage.memory.payload_scratch,
            )?;
            self.apply_append_record(
                chunk_handle,
                &storage.memory.payload_scratch[..record_len],
                AppendVisibility::Planned,
            )?;
            self.materialize_current_frontier_in_transaction(storage)?;
            first_chunk = Some(first_chunk.unwrap_or(chunk_handle));
            previous_chunk = Some(chunk_handle);
            cursor = cursor
                .checked_add(chunk_len)
                .ok_or(ObjectLogError::LengthOverflow)?;
            logical_start = logical_start
                .checked_add(u64::try_from(chunk_len).map_err(|_| ObjectLogError::LengthOverflow)?)
                .ok_or(ObjectLogError::LengthOverflow)?;
            self.install_reserved_frontier(reserved)?;
        }
    }

    fn needs_new_region(
        &self,
        record_len: usize,
        payload_capacity: usize,
    ) -> Result<bool, ObjectLogError> {
        let Some(region) = self.memory.regions.last().copied() else {
            return Ok(true);
        };
        if region.flushed {
            return Ok(true);
        }
        let end = usize::try_from(region.end_offset)
            .map_err(|_| ObjectLogError::LengthOverflow)?
            .checked_add(record_len)
            .ok_or(ObjectLogError::LengthOverflow)?;
        Ok(end > Header::ENCODED_LEN + payload_capacity)
    }

    fn apply_append_record(
        &mut self,
        handle: ObjectLogHandle,
        record: &[u8],
        visibility: AppendVisibility,
    ) -> Result<(), ObjectLogError> {
        let next_sequence = next_sequence_after(handle.sequence)?;
        let record_info = decode_record_info_at(handle.offset, record)?;
        if record.len() != record_len(record_info.body_len)? {
            return Err(ObjectLogError::InvalidFrame);
        }
        let body = record
            .get(RECORD_HEADER_LEN..)
            .ok_or(ObjectLogError::InvalidFrame)?;
        validate_record_body(record_info.body_crc32c, body)?;
        validate_record_body_shape(record_info.record_type, body)?;
        let payload_offset = payload_offset(handle.offset)?;
        let payload_end = payload_offset
            .checked_add(record.len())
            .ok_or(ObjectLogError::LengthOverflow)?;
        if payload_end > self.memory.frontier_payload.len() {
            return Err(ObjectLogError::ObjectTooLarge {
                len: record.len(),
                capacity: self.memory.frontier_payload.len(),
            });
        }

        let region_index = match self.find_region(handle.region_index, handle.sequence) {
            Some(index) => index,
            None => {
                let start = u32::try_from(Header::ENCODED_LEN + self.object_payload_start()?)
                    .map_err(|_| ObjectLogError::LengthOverflow)?;
                let region = ObjectLogRegion {
                    region_index: handle.region_index,
                    sequence: handle.sequence,
                    start_offset: start,
                    end_offset: start,
                    committed_end_offset: start,
                    first_committed_public_offset: None,
                    first_planned_public_offset: None,
                    flushed: false,
                };
                self.memory
                    .regions
                    .push(region)
                    .map_err(|_| ObjectLogError::TooManyRegions)?;
                self.initialize_frontier_payload(handle.sequence)?;
                self.memory.regions.len() - 1
            }
        };

        let region = self
            .memory
            .regions
            .get_mut(region_index)
            .ok_or(ObjectLogError::InvalidHandle)?;
        if region.flushed || region.end_offset != handle.offset {
            return Err(ObjectLogError::InvalidHandle);
        }
        if matches!(visibility, AppendVisibility::Committed)
            && region.committed_end_offset != handle.offset
        {
            return Err(ObjectLogError::InvalidHandle);
        }

        self.memory.frontier_payload[payload_offset..payload_end].copy_from_slice(record);
        region.end_offset = handle
            .offset
            .checked_add(u32::try_from(record.len()).map_err(|_| ObjectLogError::LengthOverflow)?)
            .ok_or(ObjectLogError::LengthOverflow)?;
        if record_type_is_public(record_info.record_type) {
            match visibility {
                AppendVisibility::Committed => {
                    if region.first_committed_public_offset.is_none() {
                        region.first_committed_public_offset = Some(handle.offset);
                    }
                }
                AppendVisibility::Planned => {
                    if region.first_planned_public_offset.is_none() {
                        region.first_planned_public_offset = Some(handle.offset);
                    }
                }
            }
        }
        if matches!(visibility, AppendVisibility::Committed) {
            region.committed_end_offset = region.end_offset;
        }
        self.memory.next_sequence = self.memory.next_sequence.max(next_sequence);
        Ok(())
    }

    fn apply_materialized_region(
        &mut self,
        region: ObjectLogRegion,
        visibility: AppendVisibility,
    ) -> Result<(), ObjectLogError> {
        let next_sequence = next_sequence_after(region.sequence)?;
        if !region.flushed || region.end_offset < region.start_offset {
            return Err(ObjectLogError::InvalidEncoding);
        }
        if region.committed_end_offset > region.end_offset {
            return Err(ObjectLogError::InvalidEncoding);
        }
        match self.find_region(region.region_index, region.sequence) {
            Some(index) => {
                let existing = self
                    .memory
                    .regions
                    .get_mut(index)
                    .ok_or(ObjectLogError::InvalidHandle)?;
                if existing.start_offset != region.start_offset {
                    return Err(ObjectLogError::InvalidEncoding);
                }
                existing.end_offset = region.end_offset;
                existing.flushed = true;
                existing.first_planned_public_offset = region.first_planned_public_offset;
                if existing.first_committed_public_offset.is_none() {
                    existing.first_committed_public_offset = region.first_committed_public_offset;
                }
                if matches!(visibility, AppendVisibility::Committed) {
                    existing.committed_end_offset = region.end_offset;
                    if existing.first_committed_public_offset.is_none() {
                        existing.first_committed_public_offset = region.first_planned_public_offset;
                    }
                    existing.first_planned_public_offset = None;
                }
            }
            None => {
                let mut replayed = region;
                if matches!(visibility, AppendVisibility::Committed) {
                    replayed.committed_end_offset = replayed.end_offset;
                    if replayed.first_committed_public_offset.is_none() {
                        replayed.first_committed_public_offset =
                            replayed.first_planned_public_offset;
                    }
                    replayed.first_planned_public_offset = None;
                }
                self.memory
                    .regions
                    .push(replayed)
                    .map_err(|_| ObjectLogError::TooManyRegions)?;
            }
        }
        self.memory.next_sequence = self.memory.next_sequence.max(next_sequence);
        Ok(())
    }

    fn apply_log_metadata(&mut self, log_metadata: &[u8]) -> Result<(), ObjectLogError> {
        validate_log_metadata_len::<LOG_METADATA_MAX>(log_metadata.len())?;
        if self.memory.log_metadata_len != 0 {
            if &self.memory.log_metadata[..self.memory.log_metadata_len] == log_metadata {
                return Ok(());
            }
            return Err(ObjectLogError::InvalidEncoding);
        }
        self.memory.log_metadata[..log_metadata.len()].copy_from_slice(log_metadata);
        self.memory.log_metadata_len = log_metadata.len();
        Ok(())
    }

    fn checkpoint_append_state(&mut self) -> Result<(), ObjectLogError> {
        self.memory.rollback_regions.clear();
        for region in self.memory.regions.iter().copied() {
            self.memory
                .rollback_regions
                .push(region)
                .map_err(|_| ObjectLogError::TooManyRegions)?;
        }
        self.memory
            .rollback_frontier_payload
            .copy_from_slice(&self.memory.frontier_payload);
        Ok(())
    }

    fn restore_append_checkpoint(&mut self) {
        self.memory.regions.clear();
        for region in self.memory.rollback_regions.iter().copied() {
            let _ = self.memory.regions.push(region);
        }
        self.memory
            .frontier_payload
            .copy_from_slice(&self.memory.rollback_frontier_payload);
    }

    fn clear_append_checkpoint(&mut self) {
        self.memory.rollback_regions.clear();
    }

    fn commit_staged_appends(&mut self) {
        for region in &mut self.memory.regions {
            region.committed_end_offset = region.end_offset;
            if region.first_committed_public_offset.is_none() {
                region.first_committed_public_offset = region.first_planned_public_offset;
            }
            region.first_planned_public_offset = None;
        }
    }

    fn apply_truncate_before(
        &mut self,
        handle: ObjectLogHandle,
        retained_start: ObjectLogHandle,
        freed_regions: &mut Vec<u32, MAX_REGIONS>,
    ) -> Result<(), ObjectLogError> {
        freed_regions.clear();
        let retained_index = self
            .find_region(retained_start.region_index, retained_start.sequence)
            .ok_or(ObjectLogError::InvalidHandle)?;
        let public_index = self
            .find_region(handle.region_index, handle.sequence)
            .ok_or(ObjectLogError::InvalidHandle)?;
        if retained_index > public_index {
            return Err(ObjectLogError::InvalidHandle);
        }
        let retained_region = self
            .memory
            .regions
            .get(retained_index)
            .copied()
            .ok_or(ObjectLogError::InvalidHandle)?;
        let public_region = self
            .memory
            .regions
            .get(public_index)
            .copied()
            .ok_or(ObjectLogError::InvalidHandle)?;
        if !retained_region.contains_committed(retained_start)
            || !public_region.contains_committed(handle)
        {
            return Err(ObjectLogError::InvalidHandle);
        }
        for old in self.memory.regions.iter().take(retained_index).copied() {
            freed_regions
                .push(old.region_index)
                .map_err(|_| ObjectLogError::TooManyRegions)?;
        }
        for _ in 0..retained_index {
            self.memory.regions.remove(0);
        }
        for region in &mut self.memory.regions {
            region.first_committed_public_offset = None;
            region.first_planned_public_offset = None;
        }
        let region_count = self.memory.regions.len();
        if let Some(head) = self.memory.regions.first_mut() {
            head.start_offset = retained_start.offset;
            if head.start_offset == head.committed_end_offset && region_count > 1 {
                let empty = *head;
                freed_regions
                    .push(empty.region_index)
                    .map_err(|_| ObjectLogError::TooManyRegions)?;
                self.memory.regions.remove(0);
            }
        }
        let public_index = self
            .find_region(handle.region_index, handle.sequence)
            .ok_or(ObjectLogError::InvalidHandle)?;
        self.memory
            .regions
            .get_mut(public_index)
            .ok_or(ObjectLogError::InvalidHandle)?
            .first_committed_public_offset = Some(handle.offset);
        Ok(())
    }

    fn truncate_before_inner<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<(), ObjectLogError> {
        let mut freed_regions = Vec::<u32, MAX_REGIONS>::new();
        self.validate_live_handle(storage, handle)?;
        let retained_start = self.retained_start_for_truncate(storage, handle)?;
        let used =
            encode_truncate_update(handle, retained_start, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..used],
            )?;
        self.apply_truncate_before(handle, retained_start, &mut freed_regions)?;

        for region_index in freed_regions {
            storage
                .memory
                .state
                .begin_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                    storage.backing,
                    &mut storage.memory.workspace,
                    self.collection_id,
                )?;
            storage
                .memory
                .state
                .append_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    storage.backing,
                    &mut storage.memory.workspace,
                    self.collection_id,
                    region_index,
                )?;
            storage
                .memory
                .state
                .commit_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                    storage.backing,
                    &mut storage.memory.workspace,
                    self.collection_id,
                )?;
            storage
                .memory
                .state
                .finish_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                    storage.backing,
                    &mut storage.memory.workspace,
                    self.collection_id,
                )?;
        }
        Ok(())
    }

    fn flush_current<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<(), ObjectLogError> {
        let Some(index) = self.memory.regions.len().checked_sub(1) else {
            return Ok(());
        };
        let region = self
            .memory
            .regions
            .get(index)
            .copied()
            .ok_or(ObjectLogError::InvalidHandle)?;
        if region.flushed || region.end_offset == region.start_offset {
            return Ok(());
        }
        let payload_len = payload_offset(region.end_offset)?;
        storage
            .memory
            .state
            .write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                region.region_index,
                self.collection_id,
                OBJECT_LOG_DATA_V1_FORMAT,
                &self.memory.frontier_payload[..payload_len],
            )?;
        self.memory
            .regions
            .get_mut(index)
            .ok_or(ObjectLogError::InvalidHandle)?
            .flushed = true;
        let snapshot_len = encode_snapshot::<MAX_REGIONS, LOG_METADATA_MAX>(
            &self.memory.regions,
            &self.memory.log_metadata[..self.memory.log_metadata_len],
            &mut storage.memory.payload_scratch,
        )?;
        storage
            .memory
            .state
            .append_snapshot_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                CollectionType::OBJECT_LOG_CODE,
                &storage.memory.payload_scratch[..snapshot_len],
            )?;
        Ok(())
    }

    fn get_inner<
        'db,
        'storage_mem,
        IO: FlashIo,
        R,
        F,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
        scratch: &mut [u8],
        read: F,
    ) -> Result<R, ObjectLogError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let (region, record) = self.read_public_record_info(storage, handle)?;
        match record.record_type {
            RECORD_INLINE_OBJECT => {
                if scratch.len() < record.body_len {
                    return Err(ObjectLogError::BufferTooSmall {
                        needed: record.body_len,
                        available: scratch.len(),
                    });
                }
                self.read_record_body_into(storage, region, handle, record, scratch, true)?;
                Ok(read(&scratch[..record.body_len]))
            }
            RECORD_OBJECT_END => {
                let object_end = self.read_object_end(storage, region, handle, record)?;
                let object_len = usize::try_from(object_end.total_object_len)
                    .map_err(|_| ObjectLogError::LengthOverflow)?;
                if scratch.len() < object_len {
                    return Err(ObjectLogError::BufferTooSmall {
                        needed: object_len,
                        available: scratch.len(),
                    });
                }
                self.copy_large_object_range(
                    storage,
                    object_end,
                    0,
                    object_end.total_object_len,
                    scratch,
                )?;
                Ok(read(&scratch[..object_len]))
            }
            _ => Err(ObjectLogError::InvalidHandle),
        }
    }

    fn get_object_len_inner<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<u64, ObjectLogError> {
        let (region, record) = self.read_public_record_info(storage, handle)?;
        match record.record_type {
            RECORD_INLINE_OBJECT => {
                u64::try_from(record.body_len).map_err(|_| ObjectLogError::LengthOverflow)
            }
            RECORD_OBJECT_END => Ok(self
                .read_object_end(storage, region, handle, record)?
                .total_object_len),
            _ => Err(ObjectLogError::InvalidHandle),
        }
    }

    fn get_range_inner<
        'db,
        'storage_mem,
        IO: FlashIo,
        R,
        F,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
        object_offset: u64,
        len: u64,
        scratch: &mut [u8],
        read: F,
    ) -> Result<R, ObjectLogError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let (region, record) = self.read_public_record_info(storage, handle)?;
        match record.record_type {
            RECORD_INLINE_OBJECT => {
                let range =
                    checked_object_read_range(record.body_len, object_offset, len, scratch.len())?;
                if range.is_empty() {
                    return Ok(read(&scratch[..0]));
                }
                if storage.memory.payload_scratch.len() < record.body_len {
                    return Err(ObjectLogError::InvalidFrame);
                }
                self.read_record_body_into_storage_scratch(storage, region, handle, record, true)?;
                let read_len = range.end - range.start;
                scratch[..read_len]
                    .copy_from_slice(&storage.memory.payload_scratch[..record.body_len][range]);
                Ok(read(&scratch[..read_len]))
            }
            RECORD_OBJECT_END => {
                let object_end = self.read_object_end(storage, region, handle, record)?;
                let range = checked_object_read_range_u64(
                    object_end.total_object_len,
                    object_offset,
                    len,
                    scratch.len(),
                )?;
                if range.len == 0 {
                    return Ok(read(&scratch[..0]));
                }
                self.copy_large_object_range(
                    storage,
                    object_end,
                    range.offset,
                    range.len,
                    scratch,
                )?;
                let read_len =
                    usize::try_from(range.len).map_err(|_| ObjectLogError::LengthOverflow)?;
                Ok(read(&scratch[..read_len]))
            }
            _ => Err(ObjectLogError::InvalidHandle),
        }
    }

    fn next_handle_inner<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<Option<ObjectLogHandle>, ObjectLogError> {
        let index = self
            .find_region(handle.region_index, handle.sequence)
            .ok_or(ObjectLogError::InvalidHandle)?;
        let region = self
            .memory
            .regions
            .get(index)
            .copied()
            .ok_or(ObjectLogError::InvalidHandle)?;
        if !region.contains_committed(handle) {
            return Err(ObjectLogError::InvalidHandle);
        }
        let record = self.validate_live_handle(storage, handle)?;
        self.find_next_public_handle(storage, index, record.record_end)
    }

    fn validate_flushed_region_prologue<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        region: ObjectLogRegion,
    ) -> Result<(), ObjectLogError> {
        let header = storage
            .backing
            .read_region(region.region_index, 0, Header::ENCODED_LEN, Header::decode)
            .map_err(StorageRuntimeError::from)?
            .map_err(|_| ObjectLogError::InvalidFrame)?;
        if header.collection_id != self.collection_id
            || header.collection_format != OBJECT_LOG_DATA_V1_FORMAT
        {
            return Err(ObjectLogError::InvalidFrame);
        }

        let mut prologue = [0u8; DATA_PROLOGUE_FIXED_LEN];
        storage
            .backing
            .read_region(
                region.region_index,
                Header::ENCODED_LEN,
                DATA_PROLOGUE_FIXED_LEN,
                |bytes| prologue.copy_from_slice(bytes),
            )
            .map_err(StorageRuntimeError::from)?;
        let (sequence, log_metadata_len) = decode_data_prologue_header(&prologue)?;
        if sequence != region.sequence {
            return Err(ObjectLogError::InvalidHandle);
        }
        if log_metadata_len == 0
            || log_metadata_len > LOG_METADATA_MAX
            || log_metadata_len != self.memory.log_metadata_len
        {
            return Err(ObjectLogError::InvalidFrame);
        }
        let metadata_matches = storage
            .backing
            .read_region(
                region.region_index,
                Header::ENCODED_LEN + DATA_PROLOGUE_FIXED_LEN,
                log_metadata_len,
                |bytes| bytes == &self.memory.log_metadata[..self.memory.log_metadata_len],
            )
            .map_err(StorageRuntimeError::from)?;
        if !metadata_matches {
            return Err(ObjectLogError::InvalidFrame);
        }
        Ok(())
    }

    fn region_for_handle(
        &self,
        handle: ObjectLogHandle,
    ) -> Result<ObjectLogRegion, ObjectLogError> {
        let region = self
            .find_region(handle.region_index, handle.sequence)
            .and_then(|index| self.memory.regions.get(index).copied())
            .ok_or(ObjectLogError::InvalidHandle)?;
        if !region.contains_committed(handle) {
            return Err(ObjectLogError::InvalidHandle);
        }
        Ok(region)
    }

    fn read_public_record_info<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<(ObjectLogRegion, ObjectLogRecordInfo), ObjectLogError> {
        let region = self.region_for_handle(handle)?;
        let record = self.read_record_info(storage, region, handle)?;
        if !record_type_is_public(record.record_type) {
            return Err(ObjectLogError::InvalidHandle);
        }
        Ok((region, record))
    }

    fn read_record_info<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
    ) -> Result<ObjectLogRecordInfo, ObjectLogError> {
        if !region.contains_committed(handle) {
            return Err(ObjectLogError::InvalidHandle);
        }
        let mut header = [0u8; RECORD_HEADER_LEN];
        if region.flushed {
            self.validate_flushed_region_prologue(storage, region)?;
            storage
                .backing
                .read_region(
                    handle.region_index,
                    usize::try_from(handle.offset).map_err(|_| ObjectLogError::LengthOverflow)?,
                    RECORD_HEADER_LEN,
                    |bytes| header.copy_from_slice(bytes),
                )
                .map_err(StorageRuntimeError::from)?;
        } else {
            let record_offset = payload_offset(handle.offset)?;
            let source = self
                .memory
                .frontier_payload
                .get(record_offset..record_offset + RECORD_HEADER_LEN)
                .ok_or(ObjectLogError::InvalidHandle)?;
            header.copy_from_slice(source);
        }
        let record = decode_record_info_at(handle.offset, &header)?;
        if record.record_end > region.committed_end_offset {
            return Err(ObjectLogError::InvalidFrame);
        }
        Ok(record)
    }

    fn read_record_body_into<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
        record: ObjectLogRecordInfo,
        target: &mut [u8],
        validate_crc: bool,
    ) -> Result<(), ObjectLogError> {
        if target.len() < record.body_len {
            return Err(ObjectLogError::BufferTooSmall {
                needed: record.body_len,
                available: target.len(),
            });
        }
        if region.flushed {
            storage
                .backing
                .read_region(
                    handle.region_index,
                    record.body_start,
                    record.body_len,
                    |bytes| target[..record.body_len].copy_from_slice(bytes),
                )
                .map_err(StorageRuntimeError::from)?;
        } else {
            let body_offset = payload_offset(
                u32::try_from(record.body_start).map_err(|_| ObjectLogError::LengthOverflow)?,
            )?;
            let body = self
                .memory
                .frontier_payload
                .get(body_offset..body_offset + record.body_len)
                .ok_or(ObjectLogError::InvalidFrame)?;
            target[..record.body_len].copy_from_slice(body);
        }
        if validate_crc {
            validate_record_body(record.body_crc32c, &target[..record.body_len])?;
        }
        Ok(())
    }

    fn read_record_body_into_storage_scratch<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
        record: ObjectLogRecordInfo,
        validate_crc: bool,
    ) -> Result<(), ObjectLogError> {
        if storage.memory.payload_scratch.len() < record.body_len {
            return Err(ObjectLogError::InvalidFrame);
        }
        if region.flushed {
            storage
                .backing
                .read_region(
                    handle.region_index,
                    record.body_start,
                    record.body_len,
                    |bytes| {
                        storage.memory.payload_scratch[..record.body_len].copy_from_slice(bytes)
                    },
                )
                .map_err(StorageRuntimeError::from)?;
        } else {
            let body_offset = payload_offset(
                u32::try_from(record.body_start).map_err(|_| ObjectLogError::LengthOverflow)?,
            )?;
            let body = self
                .memory
                .frontier_payload
                .get(body_offset..body_offset + record.body_len)
                .ok_or(ObjectLogError::InvalidFrame)?;
            storage.memory.payload_scratch[..record.body_len].copy_from_slice(body);
        }
        if validate_crc {
            validate_record_body(
                record.body_crc32c,
                &storage.memory.payload_scratch[..record.body_len],
            )?;
        }
        Ok(())
    }

    fn read_record_body_prefix_into_storage_scratch<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
        record: ObjectLogRecordInfo,
        len: usize,
    ) -> Result<(), ObjectLogError> {
        if len > record.body_len || storage.memory.payload_scratch.len() < len {
            return Err(ObjectLogError::InvalidFrame);
        }
        if region.flushed {
            storage
                .backing
                .read_region(handle.region_index, record.body_start, len, |bytes| {
                    storage.memory.payload_scratch[..len].copy_from_slice(bytes)
                })
                .map_err(StorageRuntimeError::from)?;
        } else {
            let body_offset = payload_offset(
                u32::try_from(record.body_start).map_err(|_| ObjectLogError::LengthOverflow)?,
            )?;
            let body = self
                .memory
                .frontier_payload
                .get(body_offset..body_offset + len)
                .ok_or(ObjectLogError::InvalidFrame)?;
            storage.memory.payload_scratch[..len].copy_from_slice(body);
        }
        Ok(())
    }

    fn read_object_end<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
        record: ObjectLogRecordInfo,
    ) -> Result<ObjectEndInfo, ObjectLogError> {
        if record.record_type != RECORD_OBJECT_END || record.body_len != OBJECT_END_BODY_LEN {
            return Err(ObjectLogError::InvalidFrame);
        }
        self.read_record_body_into_storage_scratch(storage, region, handle, record, true)?;
        decode_object_end_body(&storage.memory.payload_scratch[..record.body_len])
    }

    fn read_chunk_info<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
        validate_crc: bool,
    ) -> Result<(ObjectLogRegion, ObjectLogRecordInfo, ObjectChunkInfo), ObjectLogError> {
        let region = self.region_for_handle(handle)?;
        let record = self.read_record_info(storage, region, handle)?;
        if record.record_type != RECORD_OBJECT_CHUNK {
            return Err(ObjectLogError::InvalidFrame);
        }
        if validate_crc {
            self.read_record_body_into_storage_scratch(storage, region, handle, record, true)?;
            let chunk = decode_chunk_body_info(&storage.memory.payload_scratch[..record.body_len])?;
            Ok((region, record, chunk))
        } else {
            self.read_record_body_prefix_into_storage_scratch(
                storage,
                region,
                handle,
                record,
                OBJECT_CHUNK_FIXED_BODY_LEN,
            )?;
            let chunk = decode_chunk_body_prefix(
                &storage.memory.payload_scratch[..OBJECT_CHUNK_FIXED_BODY_LEN],
                record.body_len,
            )?;
            Ok((region, record, chunk))
        }
    }

    fn copy_large_object_range<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        object_end: ObjectEndInfo,
        object_offset: u64,
        len: u64,
        scratch: &mut [u8],
    ) -> Result<(), ObjectLogError> {
        let requested_end = object_offset
            .checked_add(len)
            .ok_or(ObjectLogError::LengthOverflow)?;
        let mut current = object_end.first;
        let mut previous = None::<ObjectLogHandle>;
        let mut expected_logical_start = 0u64;
        let mut copied = 0usize;
        let target_len = usize::try_from(len).map_err(|_| ObjectLogError::LengthOverflow)?;

        for _ in 0..MAX_REGIONS {
            let (_, _, chunk) = self.read_chunk_info(storage, current, false)?;
            if chunk.logical_start != expected_logical_start {
                return Err(ObjectLogError::InvalidFrame);
            }
            if previous.is_none() && chunk.flags & OBJECT_CHUNK_FLAG_PREV_VALID != 0 {
                return Err(ObjectLogError::InvalidFrame);
            }
            if let Some(previous_handle) = previous {
                if chunk.flags & OBJECT_CHUNK_FLAG_PREV_VALID == 0 || chunk.prev != previous_handle
                {
                    return Err(ObjectLogError::InvalidFrame);
                }
            }
            let chunk_end = chunk
                .logical_start
                .checked_add(
                    u64::try_from(chunk.chunk_len).map_err(|_| ObjectLogError::LengthOverflow)?,
                )
                .ok_or(ObjectLogError::LengthOverflow)?;
            if chunk_end > object_end.total_object_len {
                return Err(ObjectLogError::InvalidFrame);
            }

            if chunk_end > object_offset && chunk.logical_start < requested_end {
                let (_, record, chunk) = self.read_chunk_info(storage, current, true)?;
                let chunk_start = chunk.logical_start;
                let chunk_end = chunk_start
                    .checked_add(
                        u64::try_from(chunk.chunk_len)
                            .map_err(|_| ObjectLogError::LengthOverflow)?,
                    )
                    .ok_or(ObjectLogError::LengthOverflow)?;
                let copy_start = object_offset.max(chunk_start);
                let copy_end = requested_end.min(chunk_end);
                let source_start = usize::try_from(
                    copy_start
                        .checked_sub(chunk_start)
                        .ok_or(ObjectLogError::LengthOverflow)?,
                )
                .map_err(|_| ObjectLogError::LengthOverflow)?;
                let source_end = usize::try_from(
                    copy_end
                        .checked_sub(chunk_start)
                        .ok_or(ObjectLogError::LengthOverflow)?,
                )
                .map_err(|_| ObjectLogError::LengthOverflow)?;
                let copy_len = source_end
                    .checked_sub(source_start)
                    .ok_or(ObjectLogError::LengthOverflow)?;
                let destination_end = copied
                    .checked_add(copy_len)
                    .ok_or(ObjectLogError::LengthOverflow)?;
                if destination_end > scratch.len() || destination_end > target_len {
                    return Err(ObjectLogError::InvalidFrame);
                }
                let source_base = OBJECT_CHUNK_FIXED_BODY_LEN;
                let source = &storage.memory.payload_scratch
                    [source_base + source_start..source_base + source_end];
                scratch[copied..destination_end].copy_from_slice(source);
                copied = destination_end;
                if record.body_len != OBJECT_CHUNK_FIXED_BODY_LEN + chunk.chunk_len {
                    return Err(ObjectLogError::InvalidFrame);
                }
            }

            if chunk_end == object_end.total_object_len {
                if current != object_end.last || chunk.flags & OBJECT_CHUNK_FLAG_NEXT_VALID != 0 {
                    return Err(ObjectLogError::InvalidFrame);
                }
                if copied != target_len {
                    return Err(ObjectLogError::InvalidFrame);
                }
                return Ok(());
            }
            if chunk.flags & OBJECT_CHUNK_FLAG_NEXT_VALID == 0 {
                return Err(ObjectLogError::InvalidFrame);
            }
            previous = Some(current);
            current = chunk.next;
            expected_logical_start = chunk_end;
        }
        Err(ObjectLogError::InvalidFrame)
    }

    fn find_next_public_handle<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        start_region_index: usize,
        start_offset: u32,
    ) -> Result<Option<ObjectLogHandle>, ObjectLogError> {
        for (index, region) in self
            .memory
            .regions
            .iter()
            .copied()
            .enumerate()
            .skip(start_region_index)
        {
            let mut offset = if index == start_region_index {
                start_offset
            } else {
                region.start_offset
            };
            while offset < region.committed_end_offset {
                let handle = ObjectLogHandle::new(region.region_index, region.sequence, offset);
                let record = self.read_record_info(storage, region, handle)?;
                if record_type_is_public(record.record_type) {
                    return Ok(Some(handle));
                }
                offset = record.record_end;
            }
            if offset > region.committed_end_offset {
                return Err(ObjectLogError::InvalidFrame);
            }
        }
        Ok(None)
    }

    fn retained_start_for_truncate<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<ObjectLogHandle, ObjectLogError> {
        let (region, record) = self.read_public_record_info(storage, handle)?;
        match record.record_type {
            RECORD_INLINE_OBJECT => Ok(handle),
            RECORD_OBJECT_END => Ok(self.read_object_end(storage, region, handle, record)?.first),
            _ => Err(ObjectLogError::InvalidHandle),
        }
    }

    fn initialize_frontier_payload(&mut self, sequence: u64) -> Result<(), ObjectLogError> {
        self.memory.frontier_payload.fill(0);
        let prologue_len = self.object_payload_start()?;
        encode_data_prologue(
            sequence,
            &self.memory.log_metadata[..self.memory.log_metadata_len],
            &mut self.memory.frontier_payload[..prologue_len],
        )
    }

    fn object_payload_start(&self) -> Result<usize, ObjectLogError> {
        data_prologue_len(self.memory.log_metadata_len)
    }

    fn validate_open_state<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<(), ObjectLogError> {
        validate_log_metadata_len::<LOG_METADATA_MAX>(self.memory.log_metadata_len)?;
        let object_start = u32::try_from(Header::ENCODED_LEN + self.object_payload_start()?)
            .map_err(|_| ObjectLogError::LengthOverflow)?;
        for region in self.memory.regions.iter().copied() {
            let _ = next_sequence_after(region.sequence)?;
            if region.start_offset < object_start
                || region.committed_end_offset > region.end_offset
                || region.committed_end_offset < region.start_offset
            {
                return Err(ObjectLogError::InvalidEncoding);
            }
            if let Some(first) = region.first_committed_public_offset {
                if first < region.start_offset || first >= region.committed_end_offset {
                    return Err(ObjectLogError::InvalidEncoding);
                }
            }
            if let Some(first) = region.first_planned_public_offset {
                if first < region.start_offset || first >= region.end_offset {
                    return Err(ObjectLogError::InvalidEncoding);
                }
            }
            if region.flushed {
                self.validate_flushed_region_prologue(storage, region)?;
            }
        }
        Ok(())
    }

    fn find_region(&self, region_index: u32, sequence: u64) -> Option<usize> {
        self.memory
            .regions
            .iter()
            .position(|region| region.region_index == region_index && region.sequence == sequence)
    }

    fn validate_live_handle<
        'db,
        'storage_mem,
        IO: FlashIo,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        storage: &mut Storage<'db, 'storage_mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        handle: ObjectLogHandle,
    ) -> Result<ObjectLogRecordInfo, ObjectLogError> {
        let (region, record) = self.read_public_record_info(storage, handle)?;
        self.read_record_body_into_storage_scratch(storage, region, handle, record, true)?;
        Ok(record)
    }
}

/// Errors returned by [`ObjectLog`].
#[derive(Debug)]
pub enum ObjectLogError {
    /// Shared storage failed.
    Storage(StorageRuntimeError),
    /// WAL visitation failed.
    Visit(StorageVisitError<()>),
    /// The collection does not exist.
    UnknownCollection(CollectionId),
    /// The collection type did not match object log.
    CollectionTypeMismatch {
        collection_id: CollectionId,
        actual: Option<u16>,
    },
    /// The collection was dropped.
    DroppedCollection(CollectionId),
    /// Encoded data was malformed.
    InvalidEncoding,
    /// A handle or position does not name a live object.
    InvalidHandle,
    /// Too many object-log data regions are live for the configured memory.
    TooManyRegions,
    /// A write required an active frontier but none existed.
    MissingFrontier,
    /// Object payload exceeded the region object capacity.
    ObjectTooLarge { len: usize, capacity: usize },
    /// Requested object byte range was outside the stored object.
    ObjectRangeOutOfBounds {
        offset: u64,
        len: u64,
        object_len: u64,
    },
    /// Log metadata must be non-empty.
    LogMetadataEmpty,
    /// Log metadata exceeded configured memory.
    LogMetadataTooLarge { len: usize, capacity: usize },
    /// Read scratch was too small.
    BufferTooSmall { needed: usize, available: usize },
    /// A stored object record was invalid.
    InvalidFrame,
    /// Checked arithmetic overflowed.
    LengthOverflow,
}

impl From<StorageRuntimeError> for ObjectLogError {
    fn from(error: StorageRuntimeError) -> Self {
        Self::Storage(error)
    }
}

impl From<crate::StartupError> for ObjectLogError {
    fn from(error: crate::StartupError) -> Self {
        Self::Storage(error.into())
    }
}

fn validate_collection<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Result<(), ObjectLogError> {
    let collection = storage
        .collections()
        .iter()
        .find(|collection| collection.collection_id() == collection_id)
        .ok_or(ObjectLogError::UnknownCollection(collection_id))?;
    if collection.basis() == StartupCollectionBasis::Dropped {
        return Err(ObjectLogError::DroppedCollection(collection_id));
    }
    if collection.collection_type() != Some(CollectionType::OBJECT_LOG_CODE) {
        return Err(ObjectLogError::CollectionTypeMismatch {
            collection_id,
            actual: collection.collection_type(),
        });
    }
    Ok(())
}

fn replay_object_log<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
    memory: &mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
) -> Result<(), ObjectLogError> {
    let mut transaction = None::<ObjectLogReplayTransaction>;
    let result = storage
        .memory
        .state
        .visit_wal_records::<REGION_SIZE, IO, (), _>(
            storage.backing,
            &mut storage.memory.workspace,
            |_flash, record| {
                match record {
                    WalRecord::NewCollection {
                        collection_id: seen,
                        collection_type,
                    } if seen == collection_id
                        && collection_type == CollectionType::OBJECT_LOG_CODE =>
                    {
                        memory.clear();
                    }
                    WalRecord::BeginTransaction {
                        collection_id: seen,
                    } if seen == collection_id => {
                        let mut log = ObjectLog {
                            collection_id: CollectionId::new(0),
                            memory,
                        };
                        log.checkpoint_append_state().map_err(|_| ())?;
                        transaction = Some(ObjectLogReplayTransaction { committed: false });
                    }
                    WalRecord::Snapshot {
                        collection_id: seen,
                        collection_type,
                        payload,
                    } if seen == collection_id
                        && collection_type == CollectionType::OBJECT_LOG_CODE =>
                    {
                        decode_snapshot(payload, memory).map_err(|_| ())?;
                    }
                    WalRecord::Update {
                        collection_id: seen,
                        payload,
                    } if seen == collection_id => {
                        let visibility = if transaction.is_some() {
                            AppendVisibility::Planned
                        } else {
                            AppendVisibility::Committed
                        };
                        apply_update_payload(payload, memory, visibility).map_err(|_| ())?;
                    }
                    WalRecord::CommitTransaction {
                        collection_id: seen,
                    } if seen == collection_id => {
                        if let Some(open) = transaction.as_mut() {
                            let mut log = ObjectLog {
                                collection_id: CollectionId::new(0),
                                memory,
                            };
                            log.commit_staged_appends();
                            log.clear_append_checkpoint();
                            open.committed = true;
                        }
                    }
                    WalRecord::TransactionFinished {
                        collection_id: seen,
                    } if seen == collection_id => {
                        if transaction.as_ref().is_some_and(|open| open.committed) {
                            transaction = None;
                        }
                    }
                    WalRecord::RollbackTransaction {
                        collection_id: seen,
                    } if seen == collection_id => {
                        if transaction.take().is_some() {
                            let mut log = ObjectLog {
                                collection_id: CollectionId::new(0),
                                memory,
                            };
                            log.restore_append_checkpoint();
                            log.clear_append_checkpoint();
                        }
                    }
                    WalRecord::DropCollection {
                        collection_id: seen,
                    } if seen == collection_id => {
                        memory.clear();
                    }
                    _ => {}
                }
                Ok(())
            },
        );
    match result {
        Ok(()) => Ok(()),
        Err(StorageVisitError::Storage(error)) => Err(ObjectLogError::Storage(error)),
        Err(StorageVisitError::Visitor(())) => Err(ObjectLogError::InvalidEncoding),
    }
}

fn apply_update_payload<
    const REGION_SIZE: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    payload: &[u8],
    memory: &mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    append_visibility: AppendVisibility,
) -> Result<(), ObjectLogError> {
    let mut offset = 0usize;
    let update_type = read_u8(payload, &mut offset)?;
    match update_type {
        UPDATE_APPEND => {
            let handle = read_handle(payload, &mut offset)?;
            let len = usize::try_from(read_u32(payload, &mut offset)?)
                .map_err(|_| ObjectLogError::LengthOverflow)?;
            let end = offset
                .checked_add(len)
                .ok_or(ObjectLogError::LengthOverflow)?;
            let bytes = payload
                .get(offset..end)
                .ok_or(ObjectLogError::InvalidEncoding)?;
            offset = end;
            let mut log = ObjectLog {
                collection_id: CollectionId::new(0),
                memory,
            };
            log.apply_append_record(handle, bytes, append_visibility)?;
        }
        UPDATE_TRUNCATE_HEAD => {
            let handle = read_handle(payload, &mut offset)?;
            let retained_start = read_handle(payload, &mut offset)?;
            let mut freed = Vec::<u32, MAX_REGIONS>::new();
            let mut log = ObjectLog {
                collection_id: CollectionId::new(0),
                memory,
            };
            log.apply_truncate_before(handle, retained_start, &mut freed)?;
        }
        UPDATE_SET_LOG_METADATA => {
            let len = usize::try_from(read_u32(payload, &mut offset)?)
                .map_err(|_| ObjectLogError::LengthOverflow)?;
            let end = offset
                .checked_add(len)
                .ok_or(ObjectLogError::LengthOverflow)?;
            let log_metadata = payload
                .get(offset..end)
                .ok_or(ObjectLogError::InvalidEncoding)?;
            offset = end;
            let mut log = ObjectLog {
                collection_id: CollectionId::new(0),
                memory,
            };
            log.apply_log_metadata(log_metadata)?;
        }
        UPDATE_MATERIALIZED_REGION => {
            let region = decode_region_metadata(payload, &mut offset)?;
            let mut log = ObjectLog {
                collection_id: CollectionId::new(0),
                memory,
            };
            log.apply_materialized_region(region, append_visibility)?;
        }
        _ => return Err(ObjectLogError::InvalidEncoding),
    }
    if offset != payload.len() {
        return Err(ObjectLogError::InvalidEncoding);
    }
    Ok(())
}

pub(crate) fn empty_snapshot() -> &'static [u8] {
    &EMPTY_SNAPSHOT
}

const EMPTY_SNAPSHOT: [u8; 16] = [b'O', b'L', b'G', b'S', 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

fn encode_inline_append_update(
    handle: ObjectLogHandle,
    bytes: &[u8],
    output: &mut [u8],
) -> Result<EncodedRecordUpdate, ObjectLogError> {
    let record_len = inline_record_len(bytes.len())?;
    let mut offset = 0usize;
    offset = write_u8(output, offset, UPDATE_APPEND)?;
    offset = write_handle(output, offset, handle)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(record_len).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    let record_start = offset;
    let record_end = record_start
        .checked_add(record_len)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let available = output.len();
    encode_inline_record(
        bytes,
        output
            .get_mut(record_start..record_end)
            .ok_or(ObjectLogError::BufferTooSmall {
                needed: record_end,
                available,
            })?,
    )?;
    Ok(EncodedRecordUpdate {
        used: record_end,
        record_start,
    })
}

fn encode_chunk_append_update(
    handle: ObjectLogHandle,
    flags: u8,
    logical_start: u64,
    prev: ObjectLogHandle,
    next: ObjectLogHandle,
    chunk_bytes: &[u8],
    output: &mut [u8],
) -> Result<EncodedRecordUpdate, ObjectLogError> {
    let record_len = chunk_record_len(chunk_bytes.len())?;
    let mut offset = 0usize;
    offset = write_u8(output, offset, UPDATE_APPEND)?;
    offset = write_handle(output, offset, handle)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(record_len).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    let record_start = offset;
    let record_end = record_start
        .checked_add(record_len)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let available = output.len();
    encode_chunk_record(
        flags,
        logical_start,
        prev,
        next,
        chunk_bytes,
        output
            .get_mut(record_start..record_end)
            .ok_or(ObjectLogError::BufferTooSmall {
                needed: record_end,
                available,
            })?,
    )?;
    Ok(EncodedRecordUpdate {
        used: record_end,
        record_start,
    })
}

fn encode_end_append_update(
    handle: ObjectLogHandle,
    total_object_len: u64,
    first: ObjectLogHandle,
    last: ObjectLogHandle,
    output: &mut [u8],
) -> Result<EncodedRecordUpdate, ObjectLogError> {
    let record_len = end_record_len()?;
    let mut offset = 0usize;
    offset = write_u8(output, offset, UPDATE_APPEND)?;
    offset = write_handle(output, offset, handle)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(record_len).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    let record_start = offset;
    let record_end = record_start
        .checked_add(record_len)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let available = output.len();
    encode_end_record(
        total_object_len,
        first,
        last,
        output
            .get_mut(record_start..record_end)
            .ok_or(ObjectLogError::BufferTooSmall {
                needed: record_end,
                available,
            })?,
    )?;
    Ok(EncodedRecordUpdate {
        used: record_end,
        record_start,
    })
}

fn encode_truncate_update(
    handle: ObjectLogHandle,
    retained_start: ObjectLogHandle,
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    let mut offset = 0usize;
    offset = write_u8(output, offset, UPDATE_TRUNCATE_HEAD)?;
    offset = write_handle(output, offset, handle)?;
    write_handle(output, offset, retained_start)
}

fn encode_set_log_metadata_update(
    log_metadata: &[u8],
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    let mut offset = 0usize;
    offset = write_u8(output, offset, UPDATE_SET_LOG_METADATA)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(log_metadata.len()).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    write_bytes(output, offset, log_metadata)
}

fn encode_materialized_region_update(
    region: ObjectLogRegion,
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    let mut offset = 0usize;
    offset = write_u8(output, offset, UPDATE_MATERIALIZED_REGION)?;
    encode_region_metadata(region, output, offset)
}

fn encode_region_metadata(
    region: ObjectLogRegion,
    output: &mut [u8],
    mut offset: usize,
) -> Result<usize, ObjectLogError> {
    offset = write_u32(output, offset, region.region_index)?;
    offset = write_u64(output, offset, region.sequence)?;
    offset = write_u32(output, offset, region.start_offset)?;
    offset = write_u32(output, offset, region.end_offset)?;
    offset = write_u32(output, offset, region.committed_end_offset)?;
    offset = write_optional_u32(output, offset, region.first_committed_public_offset)?;
    offset = write_optional_u32(output, offset, region.first_planned_public_offset)?;
    write_u8(output, offset, if region.flushed { 1 } else { 0 })
}

fn decode_region_metadata(
    input: &[u8],
    offset: &mut usize,
) -> Result<ObjectLogRegion, ObjectLogError> {
    let region_index = read_u32(input, offset)?;
    let sequence = read_u64(input, offset)?;
    let start_offset = read_u32(input, offset)?;
    let end_offset = read_u32(input, offset)?;
    let committed_end_offset = read_u32(input, offset)?;
    let first_committed_public_offset = read_optional_u32(input, offset)?;
    let first_planned_public_offset = read_optional_u32(input, offset)?;
    let flushed = match read_u8(input, offset)? {
        0 => false,
        1 => true,
        _ => return Err(ObjectLogError::InvalidEncoding),
    };
    Ok(ObjectLogRegion {
        region_index,
        sequence,
        start_offset,
        end_offset,
        committed_end_offset,
        first_committed_public_offset,
        first_planned_public_offset,
        flushed,
    })
}

fn encode_snapshot<const MAX_REGIONS: usize, const LOG_METADATA_MAX: usize>(
    regions: &Vec<ObjectLogRegion, MAX_REGIONS>,
    log_metadata: &[u8],
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    validate_log_metadata_len::<LOG_METADATA_MAX>(log_metadata.len())?;
    let mut offset = 0usize;
    offset = write_bytes(output, offset, &SNAPSHOT_MAGIC)?;
    offset = write_u16(output, offset, SNAPSHOT_VERSION)?;
    offset = write_u16(output, offset, 0)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(regions.len()).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(log_metadata.len()).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    for region in regions.iter().copied() {
        let _ = next_sequence_after(region.sequence)?;
        offset = encode_region_metadata(region, output, offset)?;
    }
    offset = write_bytes(output, offset, log_metadata)?;
    Ok(offset)
}

fn decode_snapshot<
    const REGION_SIZE: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    input: &[u8],
    memory: &mut ObjectLogMemory<REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
) -> Result<(), ObjectLogError> {
    let mut offset = 0usize;
    if read_bytes(input, &mut offset, SNAPSHOT_MAGIC.len())? != SNAPSHOT_MAGIC.as_slice() {
        return Err(ObjectLogError::InvalidEncoding);
    }
    let version = read_u16(input, &mut offset)?;
    if version != SNAPSHOT_VERSION {
        return Err(ObjectLogError::InvalidEncoding);
    }
    let _reserved = read_u16(input, &mut offset)?;
    let region_count = usize::try_from(read_u32(input, &mut offset)?)
        .map_err(|_| ObjectLogError::LengthOverflow)?;
    let log_metadata_len = usize::try_from(read_u32(input, &mut offset)?)
        .map_err(|_| ObjectLogError::LengthOverflow)?;
    validate_log_metadata_len::<LOG_METADATA_MAX>(log_metadata_len)?;
    memory.clear();
    let object_start = u32::try_from(Header::ENCODED_LEN + data_prologue_len(log_metadata_len)?)
        .map_err(|_| ObjectLogError::LengthOverflow)?;
    for _ in 0..region_count {
        let region = decode_region_metadata(input, &mut offset)?;
        let next_sequence = next_sequence_after(region.sequence)?;
        if region.committed_end_offset > region.end_offset
            || region.committed_end_offset < region.start_offset
            || region.start_offset < object_start
        {
            return Err(ObjectLogError::InvalidEncoding);
        }
        if let Some(first) = region.first_committed_public_offset {
            if first < region.start_offset || first >= region.committed_end_offset {
                return Err(ObjectLogError::InvalidEncoding);
            }
        }
        if let Some(first) = region.first_planned_public_offset {
            if first < region.start_offset || first >= region.end_offset {
                return Err(ObjectLogError::InvalidEncoding);
            }
        }
        memory
            .regions
            .push(region)
            .map_err(|_| ObjectLogError::TooManyRegions)?;
        memory.next_sequence = memory.next_sequence.max(next_sequence);
    }
    let log_metadata = read_bytes(input, &mut offset, log_metadata_len)?;
    memory.log_metadata[..log_metadata_len].copy_from_slice(log_metadata);
    memory.log_metadata_len = log_metadata_len;
    if offset != input.len() {
        return Err(ObjectLogError::InvalidEncoding);
    }
    Ok(())
}

fn encode_data_prologue(
    sequence: u64,
    log_metadata: &[u8],
    output: &mut [u8],
) -> Result<(), ObjectLogError> {
    if output.len() < data_prologue_len(log_metadata.len())? {
        return Err(ObjectLogError::BufferTooSmall {
            needed: data_prologue_len(log_metadata.len())?,
            available: output.len(),
        });
    }
    let mut offset = 0usize;
    offset = write_bytes(output, offset, &DATA_MAGIC)?;
    offset = write_u16(output, offset, DATA_VERSION)?;
    offset = write_u64(output, offset, sequence)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(log_metadata.len()).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    let _ = write_bytes(output, offset, log_metadata)?;
    Ok(())
}

fn decode_data_prologue_header(input: &[u8]) -> Result<(u64, usize), ObjectLogError> {
    let mut offset = 0usize;
    if read_bytes(input, &mut offset, DATA_MAGIC.len())? != DATA_MAGIC.as_slice() {
        return Err(ObjectLogError::InvalidFrame);
    }
    if read_u16(input, &mut offset)? != DATA_VERSION {
        return Err(ObjectLogError::InvalidFrame);
    }
    let sequence = read_u64(input, &mut offset)?;
    let log_metadata_len = usize::try_from(read_u32(input, &mut offset)?)
        .map_err(|_| ObjectLogError::LengthOverflow)?;
    Ok((sequence, log_metadata_len))
}

fn encode_inline_record(bytes: &[u8], output: &mut [u8]) -> Result<usize, ObjectLogError> {
    encode_typed_record(RECORD_INLINE_OBJECT, bytes, output)
}

fn encode_chunk_record(
    flags: u8,
    logical_start: u64,
    prev: ObjectLogHandle,
    next: ObjectLogHandle,
    chunk_bytes: &[u8],
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    validate_chunk_flags(flags)?;
    let body_len = OBJECT_CHUNK_FIXED_BODY_LEN
        .checked_add(chunk_bytes.len())
        .ok_or(ObjectLogError::LengthOverflow)?;
    let record_len = record_len(body_len)?;
    if output.len() < record_len {
        return Err(ObjectLogError::BufferTooSmall {
            needed: record_len,
            available: output.len(),
        });
    }
    let body_start = RECORD_HEADER_LEN;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let mut body_offset = body_start;
    body_offset = write_u8(output, body_offset, flags)?;
    body_offset = write_u64(output, body_offset, logical_start)?;
    body_offset = write_u32(
        output,
        body_offset,
        u32::try_from(chunk_bytes.len()).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    body_offset = write_handle(output, body_offset, prev)?;
    body_offset = write_handle(output, body_offset, next)?;
    let _ = write_bytes(output, body_offset, chunk_bytes)?;
    let body_crc32c = crc32(&output[body_start..body_end]);
    encode_record_header_parts(
        RECORD_OBJECT_CHUNK,
        body_len,
        body_crc32c,
        &mut output[..RECORD_HEADER_LEN],
    )?;
    Ok(record_len)
}

fn encode_end_record(
    total_object_len: u64,
    first: ObjectLogHandle,
    last: ObjectLogHandle,
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    let record_len = end_record_len()?;
    if output.len() < record_len {
        return Err(ObjectLogError::BufferTooSmall {
            needed: record_len,
            available: output.len(),
        });
    }
    let body_start = RECORD_HEADER_LEN;
    let body_end = body_start
        .checked_add(OBJECT_END_BODY_LEN)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let mut body_offset = body_start;
    body_offset = write_u64(output, body_offset, total_object_len)?;
    body_offset = write_handle(output, body_offset, first)?;
    let _ = write_handle(output, body_offset, last)?;
    let body_crc32c = crc32(&output[body_start..body_end]);
    encode_record_header_parts(
        RECORD_OBJECT_END,
        OBJECT_END_BODY_LEN,
        body_crc32c,
        &mut output[..RECORD_HEADER_LEN],
    )?;
    Ok(record_len)
}

fn encode_typed_record(
    record_type: u8,
    body: &[u8],
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    validate_record_type(record_type)?;
    let used = record_len(body.len())?;
    if output.len() < used {
        return Err(ObjectLogError::BufferTooSmall {
            needed: used,
            available: output.len(),
        });
    }
    encode_record_header(record_type, body, &mut output[..RECORD_HEADER_LEN])?;
    let _ = write_bytes(output, RECORD_HEADER_LEN, body)?;
    Ok(used)
}

fn encode_record_header(
    record_type: u8,
    body: &[u8],
    output: &mut [u8],
) -> Result<(), ObjectLogError> {
    encode_record_header_parts(record_type, body.len(), crc32(body), output)
}

fn encode_record_header_parts(
    record_type: u8,
    body_len: usize,
    body_crc32c: u32,
    output: &mut [u8],
) -> Result<(), ObjectLogError> {
    validate_record_type(record_type)?;
    if output.len() < RECORD_HEADER_LEN {
        return Err(ObjectLogError::BufferTooSmall {
            needed: RECORD_HEADER_LEN,
            available: output.len(),
        });
    }
    let mut offset = 0usize;
    offset = write_u8(output, offset, record_type)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(body_len).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    let _ = write_u32(output, offset, body_crc32c)?;
    Ok(())
}

fn decode_record_info_at(
    record_offset: u32,
    input: &[u8],
) -> Result<ObjectLogRecordInfo, ObjectLogError> {
    if input.len() < RECORD_HEADER_LEN {
        return Err(ObjectLogError::InvalidFrame);
    }
    let mut offset = 0usize;
    let record_type = read_u8(input, &mut offset)?;
    validate_record_type(record_type)?;
    let body_len = usize::try_from(read_u32(input, &mut offset)?)
        .map_err(|_| ObjectLogError::LengthOverflow)?;
    let body_crc32c = read_u32(input, &mut offset)?;
    let body_start = usize::try_from(record_offset)
        .map_err(|_| ObjectLogError::LengthOverflow)?
        .checked_add(RECORD_HEADER_LEN)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let record_end = record_offset
        .checked_add(
            u32::try_from(record_len(body_len)?).map_err(|_| ObjectLogError::LengthOverflow)?,
        )
        .ok_or(ObjectLogError::LengthOverflow)?;
    Ok(ObjectLogRecordInfo {
        record_type,
        body_len,
        body_crc32c,
        body_start,
        record_end,
    })
}

fn validate_record_body(expected_crc32c: u32, body: &[u8]) -> Result<(), ObjectLogError> {
    if expected_crc32c == crc32(body) {
        Ok(())
    } else {
        Err(ObjectLogError::InvalidFrame)
    }
}

fn validate_record_body_shape(record_type: u8, body: &[u8]) -> Result<(), ObjectLogError> {
    match record_type {
        RECORD_INLINE_OBJECT => Ok(()),
        RECORD_OBJECT_CHUNK => {
            let _ = decode_chunk_body_info(body)?;
            Ok(())
        }
        RECORD_OBJECT_END => {
            if body.len() != OBJECT_END_BODY_LEN {
                return Err(ObjectLogError::InvalidFrame);
            }
            let _ = decode_object_end_body(body)?;
            Ok(())
        }
        _ => Err(ObjectLogError::InvalidFrame),
    }
}

fn decode_chunk_body_info(body: &[u8]) -> Result<ObjectChunkInfo, ObjectLogError> {
    let chunk = decode_chunk_body_prefix(body, body.len())?;
    if body.len() != OBJECT_CHUNK_FIXED_BODY_LEN + chunk.chunk_len {
        return Err(ObjectLogError::InvalidFrame);
    }
    Ok(chunk)
}

fn decode_chunk_body_prefix(
    body: &[u8],
    full_body_len: usize,
) -> Result<ObjectChunkInfo, ObjectLogError> {
    if body.len() < OBJECT_CHUNK_FIXED_BODY_LEN || full_body_len < OBJECT_CHUNK_FIXED_BODY_LEN {
        return Err(ObjectLogError::InvalidFrame);
    }
    let mut offset = 0usize;
    let flags = read_u8(body, &mut offset)?;
    validate_chunk_flags(flags)?;
    let logical_start = read_u64(body, &mut offset)?;
    let chunk_len = usize::try_from(read_u32(body, &mut offset)?)
        .map_err(|_| ObjectLogError::LengthOverflow)?;
    let prev = read_handle(body, &mut offset)?;
    let next = read_handle(body, &mut offset)?;
    if full_body_len != OBJECT_CHUNK_FIXED_BODY_LEN + chunk_len {
        return Err(ObjectLogError::InvalidFrame);
    }
    Ok(ObjectChunkInfo {
        flags,
        logical_start,
        chunk_len,
        prev,
        next,
    })
}

fn decode_object_end_body(body: &[u8]) -> Result<ObjectEndInfo, ObjectLogError> {
    if body.len() != OBJECT_END_BODY_LEN {
        return Err(ObjectLogError::InvalidFrame);
    }
    let mut offset = 0usize;
    let total_object_len = read_u64(body, &mut offset)?;
    let first = read_handle(body, &mut offset)?;
    let last = read_handle(body, &mut offset)?;
    Ok(ObjectEndInfo {
        total_object_len,
        first,
        last,
    })
}

fn validate_record_type(record_type: u8) -> Result<(), ObjectLogError> {
    match record_type {
        RECORD_INLINE_OBJECT | RECORD_OBJECT_CHUNK | RECORD_OBJECT_END => Ok(()),
        _ => Err(ObjectLogError::InvalidFrame),
    }
}

fn record_type_is_public(record_type: u8) -> bool {
    matches!(record_type, RECORD_INLINE_OBJECT | RECORD_OBJECT_END)
}

fn validate_chunk_flags(flags: u8) -> Result<(), ObjectLogError> {
    if flags & !OBJECT_CHUNK_FLAGS_VALID_MASK == 0 {
        Ok(())
    } else {
        Err(ObjectLogError::InvalidFrame)
    }
}

fn record_len(body_len: usize) -> Result<usize, ObjectLogError> {
    RECORD_HEADER_LEN
        .checked_add(body_len)
        .ok_or(ObjectLogError::LengthOverflow)
}

fn inline_record_len(body_len: usize) -> Result<usize, ObjectLogError> {
    record_len(body_len)
}

fn chunk_record_len(chunk_len: usize) -> Result<usize, ObjectLogError> {
    record_len(
        OBJECT_CHUNK_FIXED_BODY_LEN
            .checked_add(chunk_len)
            .ok_or(ObjectLogError::LengthOverflow)?,
    )
}

fn end_record_len() -> Result<usize, ObjectLogError> {
    record_len(OBJECT_END_BODY_LEN)
}

fn validate_log_metadata_len<const LOG_METADATA_MAX: usize>(
    len: usize,
) -> Result<(), ObjectLogError> {
    if len == 0 {
        return Err(ObjectLogError::LogMetadataEmpty);
    }
    if len > LOG_METADATA_MAX {
        return Err(ObjectLogError::LogMetadataTooLarge {
            len,
            capacity: LOG_METADATA_MAX,
        });
    }
    Ok(())
}

fn data_prologue_len(log_metadata_len: usize) -> Result<usize, ObjectLogError> {
    DATA_PROLOGUE_FIXED_LEN
        .checked_add(log_metadata_len)
        .ok_or(ObjectLogError::LengthOverflow)
}

fn empty_region_record_capacity(
    payload_capacity: usize,
    log_metadata_len: usize,
) -> Result<usize, ObjectLogError> {
    Ok(payload_capacity.saturating_sub(data_prologue_len(log_metadata_len)?))
}

fn chunk_payload_capacity_at(
    record_offset: usize,
    max_region_end: usize,
    reserve_end_record: bool,
) -> Result<usize, ObjectLogError> {
    let reserved = if reserve_end_record {
        end_record_len()?
    } else {
        0
    };
    let overhead = RECORD_HEADER_LEN
        .checked_add(OBJECT_CHUNK_FIXED_BODY_LEN)
        .and_then(|value| value.checked_add(reserved))
        .ok_or(ObjectLogError::LengthOverflow)?;
    Ok(max_region_end
        .saturating_sub(record_offset)
        .saturating_sub(overhead))
}

fn checked_object_read_range(
    payload_len: usize,
    offset: u64,
    len: u64,
    scratch_len: usize,
) -> Result<core::ops::Range<usize>, ObjectLogError> {
    let payload_len_u64 = u64::try_from(payload_len).map_err(|_| ObjectLogError::LengthOverflow)?;
    let end = offset
        .checked_add(len)
        .ok_or(ObjectLogError::LengthOverflow)?;
    if offset > payload_len_u64 || end > payload_len_u64 {
        return Err(ObjectLogError::ObjectRangeOutOfBounds {
            offset,
            len,
            object_len: payload_len_u64,
        });
    }
    let len_usize = usize::try_from(len).map_err(|_| ObjectLogError::LengthOverflow)?;
    let offset_usize = usize::try_from(offset).map_err(|_| ObjectLogError::LengthOverflow)?;
    let end_usize = usize::try_from(end).map_err(|_| ObjectLogError::LengthOverflow)?;
    if scratch_len < len_usize {
        return Err(ObjectLogError::BufferTooSmall {
            needed: len_usize,
            available: scratch_len,
        });
    }
    Ok(offset_usize..end_usize)
}

#[derive(Clone, Copy)]
struct ObjectReadRange {
    offset: u64,
    len: u64,
}

fn checked_object_read_range_u64(
    object_len: u64,
    offset: u64,
    len: u64,
    scratch_len: usize,
) -> Result<ObjectReadRange, ObjectLogError> {
    let end = offset
        .checked_add(len)
        .ok_or(ObjectLogError::LengthOverflow)?;
    if offset > object_len || end > object_len {
        return Err(ObjectLogError::ObjectRangeOutOfBounds {
            offset,
            len,
            object_len,
        });
    }
    let len_usize = usize::try_from(len).map_err(|_| ObjectLogError::LengthOverflow)?;
    if scratch_len < len_usize {
        return Err(ObjectLogError::BufferTooSmall {
            needed: len_usize,
            available: scratch_len,
        });
    }
    Ok(ObjectReadRange { offset, len })
}

fn next_sequence_after(sequence: u64) -> Result<u64, ObjectLogError> {
    sequence
        .checked_add(1)
        .ok_or(ObjectLogError::InvalidEncoding)
}

fn payload_offset(region_offset: u32) -> Result<usize, ObjectLogError> {
    let region_offset =
        usize::try_from(region_offset).map_err(|_| ObjectLogError::LengthOverflow)?;
    region_offset
        .checked_sub(Header::ENCODED_LEN)
        .ok_or(ObjectLogError::InvalidHandle)
}

fn committed_payload_capacity<const REGION_SIZE: usize>(
    metadata: StorageMetadata,
) -> Result<usize, ObjectLogError> {
    let granule =
        usize::try_from(metadata.wal_write_granule).map_err(|_| ObjectLogError::LengthOverflow)?;
    if granule == 0 {
        return Err(ObjectLogError::InvalidEncoding);
    }
    let footer_offset = REGION_SIZE
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let aligned_footer_boundary = footer_offset - footer_offset % granule;
    aligned_footer_boundary
        .checked_sub(Header::ENCODED_LEN)
        .ok_or(ObjectLogError::LengthOverflow)
}

fn crc32(bytes: &[u8]) -> u32 {
    CRC32C.checksum(bytes)
}

fn write_handle(
    output: &mut [u8],
    mut offset: usize,
    handle: ObjectLogHandle,
) -> Result<usize, ObjectLogError> {
    let end = offset
        .checked_add(HANDLE_ENCODED_LEN)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let _ = output
        .get(offset..end)
        .ok_or(ObjectLogError::BufferTooSmall {
            needed: end,
            available: output.len(),
        })?;
    offset = write_u32(output, offset, handle.region_index)?;
    offset = write_u64(output, offset, handle.sequence)?;
    write_u32(output, offset, handle.offset)
}

fn read_handle(input: &[u8], offset: &mut usize) -> Result<ObjectLogHandle, ObjectLogError> {
    Ok(ObjectLogHandle {
        region_index: read_u32(input, offset)?,
        sequence: read_u64(input, offset)?,
        offset: read_u32(input, offset)?,
    })
}

fn write_optional_u32(
    output: &mut [u8],
    offset: usize,
    value: Option<u32>,
) -> Result<usize, ObjectLogError> {
    match value {
        Some(value) => {
            let offset = write_u8(output, offset, 1)?;
            write_u32(output, offset, value)
        }
        None => {
            let offset = write_u8(output, offset, 0)?;
            write_u32(output, offset, 0)
        }
    }
}

fn read_optional_u32(input: &[u8], offset: &mut usize) -> Result<Option<u32>, ObjectLogError> {
    let present = read_u8(input, offset)?;
    let value = read_u32(input, offset)?;
    match present {
        0 => Ok(None),
        1 => Ok(Some(value)),
        _ => Err(ObjectLogError::InvalidEncoding),
    }
}

fn write_u8(output: &mut [u8], offset: usize, value: u8) -> Result<usize, ObjectLogError> {
    let end = offset
        .checked_add(size_of::<u8>())
        .ok_or(ObjectLogError::LengthOverflow)?;
    let available = output.len();
    let target = output
        .get_mut(offset..end)
        .ok_or(ObjectLogError::BufferTooSmall {
            needed: end,
            available,
        })?;
    target[0] = value;
    Ok(end)
}

fn write_u16(output: &mut [u8], offset: usize, value: u16) -> Result<usize, ObjectLogError> {
    write_bytes(output, offset, &value.to_le_bytes())
}

fn write_u32(output: &mut [u8], offset: usize, value: u32) -> Result<usize, ObjectLogError> {
    write_bytes(output, offset, &value.to_le_bytes())
}

fn write_u64(output: &mut [u8], offset: usize, value: u64) -> Result<usize, ObjectLogError> {
    write_bytes(output, offset, &value.to_le_bytes())
}

fn write_bytes(output: &mut [u8], offset: usize, bytes: &[u8]) -> Result<usize, ObjectLogError> {
    let end = offset
        .checked_add(bytes.len())
        .ok_or(ObjectLogError::LengthOverflow)?;
    let available = output.len();
    let target = output
        .get_mut(offset..end)
        .ok_or(ObjectLogError::BufferTooSmall {
            needed: end,
            available,
        })?;
    target.copy_from_slice(bytes);
    Ok(end)
}

fn read_u8(input: &[u8], offset: &mut usize) -> Result<u8, ObjectLogError> {
    let bytes = read_bytes(input, offset, size_of::<u8>())?;
    Ok(bytes[0])
}

fn read_u16(input: &[u8], offset: &mut usize) -> Result<u16, ObjectLogError> {
    let bytes = read_bytes(input, offset, size_of::<u16>())?;
    let mut value = [0u8; size_of::<u16>()];
    value.copy_from_slice(bytes);
    Ok(u16::from_le_bytes(value))
}

fn read_u32(input: &[u8], offset: &mut usize) -> Result<u32, ObjectLogError> {
    let bytes = read_bytes(input, offset, size_of::<u32>())?;
    let mut value = [0u8; size_of::<u32>()];
    value.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(value))
}

fn read_u64(input: &[u8], offset: &mut usize) -> Result<u64, ObjectLogError> {
    let bytes = read_bytes(input, offset, size_of::<u64>())?;
    let mut value = [0u8; size_of::<u64>()];
    value.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(value))
}

fn read_bytes<'a>(
    input: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], ObjectLogError> {
    let end = offset
        .checked_add(len)
        .ok_or(ObjectLogError::LengthOverflow)?;
    let bytes = input
        .get(*offset..end)
        .ok_or(ObjectLogError::InvalidEncoding)?;
    *offset = end;
    Ok(bytes)
}
