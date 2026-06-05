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

/// Stable committed-region format reserved for future object-log manifest regions.
pub const OBJECT_LOG_MANIFEST_V1_FORMAT: u16 = 8;

const DATA_MAGIC: [u8; 4] = *b"OLOG";
const DATA_VERSION: u16 = 1;
const DATA_PROLOGUE_FIXED_LEN: usize =
    size_of::<u32>() + size_of::<u16>() + size_of::<u64>() + size_of::<u32>();
const FRAME_HEADER_LEN: usize = size_of::<u32>() + size_of::<u32>();

const SNAPSHOT_MAGIC: [u8; 4] = *b"OLGS";
const SNAPSHOT_VERSION: u16 = 3;
const HANDLE_ENCODED_LEN: usize = 2 * size_of::<u32>() + size_of::<u64>();

const UPDATE_APPEND: u8 = 1;
const UPDATE_TRUNCATE_HEAD: u8 = 2;
const UPDATE_SET_LOG_METADATA: u8 = 3;

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
struct ObjectLogFrameInfo {
    header: [u8; FRAME_HEADER_LEN],
    payload_start: usize,
    payload_len: usize,
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
        self.memory
            .regions
            .iter()
            .copied()
            .find(|region| region.start_offset < region.committed_end_offset)
            .map(|region| {
                ObjectLogHandle::new(region.region_index, region.sequence, region.start_offset)
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
        let frame_len = frame_len(bytes.len())?;
        let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata())?;
        let object_start = self.object_payload_start()?;
        if frame_len > payload_capacity.saturating_sub(object_start) {
            return Err(ObjectLogError::ObjectTooLarge {
                len: bytes.len(),
                capacity: object_payload_capacity(payload_capacity, self.memory.log_metadata_len)?,
            });
        }

        if self.needs_new_region(frame_len, payload_capacity)? {
            self.flush_current(storage)?;
            return self.append_in_new_region(storage, bytes);
        }

        let region = self
            .memory
            .regions
            .last()
            .copied()
            .ok_or(ObjectLogError::MissingFrontier)?;
        let handle = ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
        let used = encode_append_update(handle, bytes, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..used],
            )?;
        self.apply_append(handle, bytes, AppendVisibility::Committed)?;
        if usize::try_from(handle.offset).map_err(|_| ObjectLogError::LengthOverflow)?
            < Header::ENCODED_LEN + object_start
        {
            return Err(ObjectLogError::InvalidHandle);
        }
        Ok(handle)
    }

    fn append_in_new_region<
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
        let handle =
            match self.append_transactional_new_region(storage, bytes, &mut allocated_regions) {
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
        let frame_len = frame_len(bytes.len())?;
        let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata())?;
        let object_start = self.object_payload_start()?;
        if frame_len > payload_capacity.saturating_sub(object_start) {
            return Err(ObjectLogError::ObjectTooLarge {
                len: bytes.len(),
                capacity: object_payload_capacity(payload_capacity, self.memory.log_metadata_len)?,
            });
        }

        if self.needs_new_region(frame_len, payload_capacity)? {
            self.flush_current(storage)?;
            return self.append_transactional_new_region(storage, bytes, allocated_regions);
        }

        let region = self
            .memory
            .regions
            .last()
            .copied()
            .ok_or(ObjectLogError::MissingFrontier)?;
        let handle = ObjectLogHandle::new(region.region_index, region.sequence, region.end_offset);
        let used = encode_append_update(handle, bytes, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..used],
            )?;
        self.apply_append(handle, bytes, AppendVisibility::Planned)?;
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
        let offset = u32::try_from(Header::ENCODED_LEN + self.object_payload_start()?)
            .map_err(|_| ObjectLogError::LengthOverflow)?;
        let handle = ObjectLogHandle::new(region_index, sequence, offset);
        let used = encode_append_update(handle, bytes, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..used],
            )?;
        self.apply_append(handle, bytes, AppendVisibility::Planned)?;
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
        for region_index in allocated_regions {
            storage
                .memory
                .state
                .append_free_region_with_rotation_prepared::<REGION_SIZE, REGION_COUNT, IO>(
                    storage.backing,
                    &mut storage.memory.workspace,
                    self.collection_id,
                    region_index,
                    FreeRegionPreparation::EraseToUnwrittenFooter,
                )?;
        }
        storage
            .memory
            .state
            .rollback_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
            )?;
        self.clear_append_checkpoint();
        Ok(())
    }

    fn needs_new_region(
        &self,
        frame_len: usize,
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
            .checked_add(frame_len)
            .ok_or(ObjectLogError::LengthOverflow)?;
        Ok(end > Header::ENCODED_LEN + payload_capacity)
    }

    fn apply_append(
        &mut self,
        handle: ObjectLogHandle,
        bytes: &[u8],
        visibility: AppendVisibility,
    ) -> Result<(), ObjectLogError> {
        let next_sequence = next_sequence_after(handle.sequence)?;
        let frame_len = frame_len(bytes.len())?;
        let payload_offset = payload_offset(handle.offset)?;
        let payload_end = payload_offset
            .checked_add(frame_len)
            .ok_or(ObjectLogError::LengthOverflow)?;
        if payload_end > self.memory.frontier_payload.len() {
            return Err(ObjectLogError::ObjectTooLarge {
                len: bytes.len(),
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

        encode_frame_into(
            bytes,
            &mut self.memory.frontier_payload[payload_offset..payload_end],
        )?;
        region.end_offset = handle
            .offset
            .checked_add(u32::try_from(frame_len).map_err(|_| ObjectLogError::LengthOverflow)?)
            .ok_or(ObjectLogError::LengthOverflow)?;
        if matches!(visibility, AppendVisibility::Committed) {
            region.committed_end_offset = region.end_offset;
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
        }
    }

    fn apply_truncate_before(
        &mut self,
        handle: ObjectLogHandle,
        freed_regions: &mut Vec<u32, MAX_REGIONS>,
    ) -> Result<(), ObjectLogError> {
        freed_regions.clear();
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
        for old in self.memory.regions.iter().take(index).copied() {
            freed_regions
                .push(old.region_index)
                .map_err(|_| ObjectLogError::TooManyRegions)?;
        }
        for _ in 0..index {
            self.memory.regions.remove(0);
        }
        let region_count = self.memory.regions.len();
        if let Some(head) = self.memory.regions.first_mut() {
            head.start_offset = handle.offset;
            if head.start_offset == head.committed_end_offset && region_count > 1 {
                let empty = *head;
                freed_regions
                    .push(empty.region_index)
                    .map_err(|_| ObjectLogError::TooManyRegions)?;
                self.memory.regions.remove(0);
            }
        }
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
        let used = encode_truncate_update(handle, &mut storage.memory.payload_scratch)?;
        storage
            .memory
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                self.collection_id,
                &storage.memory.payload_scratch[..used],
            )?;
        self.apply_truncate_before(handle, &mut freed_regions)?;

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
        let region = self
            .find_region(handle.region_index, handle.sequence)
            .and_then(|index| self.memory.regions.get(index).copied())
            .ok_or(ObjectLogError::InvalidHandle)?;
        if !region.contains_committed(handle) {
            return Err(ObjectLogError::InvalidHandle);
        }
        if region.flushed {
            self.get_flushed(storage, region, handle, scratch, read)
        } else {
            self.get_frontier(region, handle, scratch, read)
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
        let region = self
            .find_region(handle.region_index, handle.sequence)
            .and_then(|index| self.memory.regions.get(index).copied())
            .ok_or(ObjectLogError::InvalidHandle)?;
        if !region.contains_committed(handle) {
            return Err(ObjectLogError::InvalidHandle);
        }
        if region.flushed {
            u64::try_from(
                self.read_flushed_frame_info(storage, region, handle)?
                    .payload_len,
            )
            .map_err(|_| ObjectLogError::LengthOverflow)
        } else {
            u64::try_from(self.frontier_payload_len(region, handle)?)
                .map_err(|_| ObjectLogError::LengthOverflow)
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
        let region = self
            .find_region(handle.region_index, handle.sequence)
            .and_then(|index| self.memory.regions.get(index).copied())
            .ok_or(ObjectLogError::InvalidHandle)?;
        if !region.contains_committed(handle) {
            return Err(ObjectLogError::InvalidHandle);
        }
        if region.flushed {
            self.get_range_flushed(storage, region, handle, object_offset, len, scratch, read)
        } else {
            self.get_range_frontier(region, handle, object_offset, len, scratch, read)
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
        self.validate_live_handle(storage, handle)?;

        let next_offset = self.committed_frame_end(storage, region, handle)?;
        if next_offset < region.committed_end_offset {
            return Ok(Some(ObjectLogHandle::new(
                region.region_index,
                region.sequence,
                next_offset,
            )));
        }
        if next_offset > region.committed_end_offset {
            return Err(ObjectLogError::InvalidFrame);
        }

        Ok(self
            .memory
            .regions
            .iter()
            .copied()
            .skip(index + 1)
            .find(|next| next.start_offset < next.committed_end_offset)
            .map(|next| ObjectLogHandle::new(next.region_index, next.sequence, next.start_offset)))
    }

    fn committed_frame_end<
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
    ) -> Result<u32, ObjectLogError> {
        let payload_len = if region.flushed {
            self.validate_flushed_region_prologue(storage, region)?;
            let mut header = [0u8; FRAME_HEADER_LEN];
            storage
                .backing
                .read_region(
                    handle.region_index,
                    usize::try_from(handle.offset).map_err(|_| ObjectLogError::LengthOverflow)?,
                    FRAME_HEADER_LEN,
                    |bytes| header.copy_from_slice(bytes),
                )
                .map_err(StorageRuntimeError::from)?;
            decode_frame_payload_len(&header)?
        } else {
            let frame_offset = payload_offset(handle.offset)?;
            let bytes = self
                .memory
                .frontier_payload
                .get(frame_offset..)
                .ok_or(ObjectLogError::InvalidHandle)?;
            decode_frame_payload_len(bytes)?
        };
        let frame_header_len =
            u32::try_from(FRAME_HEADER_LEN).map_err(|_| ObjectLogError::LengthOverflow)?;
        let payload_len = u32::try_from(payload_len).map_err(|_| ObjectLogError::LengthOverflow)?;
        let next_offset = handle
            .offset
            .checked_add(frame_header_len)
            .and_then(|value| value.checked_add(payload_len))
            .ok_or(ObjectLogError::LengthOverflow)?;
        if next_offset > region.committed_end_offset {
            return Err(ObjectLogError::InvalidFrame);
        }
        Ok(next_offset)
    }

    fn get_flushed<
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
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
        scratch: &mut [u8],
        read: F,
    ) -> Result<R, ObjectLogError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let frame = self.read_flushed_frame_info(storage, region, handle)?;
        let payload_len = frame.payload_len;
        if scratch.len() < payload_len {
            return Err(ObjectLogError::BufferTooSmall {
                needed: payload_len,
                available: scratch.len(),
            });
        }
        if storage.memory.payload_scratch.len() < payload_len {
            return Err(ObjectLogError::InvalidFrame);
        }
        storage
            .backing
            .read_region(
                handle.region_index,
                frame.payload_start,
                payload_len,
                |bytes| storage.memory.payload_scratch[..payload_len].copy_from_slice(bytes),
            )
            .map_err(StorageRuntimeError::from)?;
        validate_frame_checksum(
            &frame.header,
            &storage.memory.payload_scratch[..payload_len],
        )?;
        scratch[..payload_len].copy_from_slice(&storage.memory.payload_scratch[..payload_len]);
        Ok(read(&scratch[..payload_len]))
    }

    fn get_range_flushed<
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
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
        object_offset: u64,
        len: u64,
        scratch: &mut [u8],
        read: F,
    ) -> Result<R, ObjectLogError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let frame = self.read_flushed_frame_info(storage, region, handle)?;
        let payload_len = frame.payload_len;
        let range = checked_object_read_range(payload_len, object_offset, len, scratch.len())?;
        let read_len = range.end - range.start;
        if read_len != 0 {
            storage
                .backing
                .read_region(
                    handle.region_index,
                    frame
                        .payload_start
                        .checked_add(range.start)
                        .ok_or(ObjectLogError::LengthOverflow)?,
                    read_len,
                    |bytes| scratch[..read_len].copy_from_slice(bytes),
                )
                .map_err(StorageRuntimeError::from)?;
        }
        Ok(read(&scratch[..read_len]))
    }

    fn read_flushed_frame_info<
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
    ) -> Result<ObjectLogFrameInfo, ObjectLogError> {
        self.validate_flushed_region_prologue(storage, region)?;
        let mut header = [0u8; FRAME_HEADER_LEN];
        storage
            .backing
            .read_region(
                handle.region_index,
                usize::try_from(handle.offset).map_err(|_| ObjectLogError::LengthOverflow)?,
                FRAME_HEADER_LEN,
                |bytes| header.copy_from_slice(bytes),
            )
            .map_err(StorageRuntimeError::from)?;
        let payload_len = decode_frame_payload_len(&header)?;
        let payload_start = usize::try_from(handle.offset)
            .map_err(|_| ObjectLogError::LengthOverflow)?
            .checked_add(FRAME_HEADER_LEN)
            .ok_or(ObjectLogError::LengthOverflow)?;
        let frame_end = payload_start
            .checked_add(payload_len)
            .ok_or(ObjectLogError::LengthOverflow)?;
        if frame_end
            > usize::try_from(region.committed_end_offset)
                .map_err(|_| ObjectLogError::LengthOverflow)?
        {
            return Err(ObjectLogError::InvalidFrame);
        }
        Ok(ObjectLogFrameInfo {
            header,
            payload_start,
            payload_len,
        })
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

    fn get_frontier<R, F>(
        &self,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
        scratch: &mut [u8],
        read: F,
    ) -> Result<R, ObjectLogError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let frame_offset = payload_offset(handle.offset)?;
        let bytes = self
            .memory
            .frontier_payload
            .get(frame_offset..)
            .ok_or(ObjectLogError::InvalidHandle)?;
        let payload_len = decode_frame_payload_len(bytes)?;
        let frame_end = frame_offset
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|value| value.checked_add(payload_len))
            .ok_or(ObjectLogError::LengthOverflow)?;
        if frame_end > payload_offset(region.committed_end_offset)? {
            return Err(ObjectLogError::InvalidFrame);
        }
        if scratch.len() < payload_len {
            return Err(ObjectLogError::BufferTooSmall {
                needed: payload_len,
                available: scratch.len(),
            });
        }
        let payload = self
            .memory
            .frontier_payload
            .get(frame_offset + FRAME_HEADER_LEN..frame_end)
            .ok_or(ObjectLogError::InvalidFrame)?;
        let frame_header = self
            .memory
            .frontier_payload
            .get(frame_offset..frame_offset + FRAME_HEADER_LEN)
            .ok_or(ObjectLogError::InvalidFrame)?;
        validate_frame_checksum(frame_header, payload)?;
        scratch[..payload_len].copy_from_slice(payload);
        Ok(read(&scratch[..payload_len]))
    }

    fn frontier_payload_len(
        &self,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
    ) -> Result<usize, ObjectLogError> {
        let frame_offset = payload_offset(handle.offset)?;
        let bytes = self
            .memory
            .frontier_payload
            .get(frame_offset..)
            .ok_or(ObjectLogError::InvalidHandle)?;
        let payload_len = decode_frame_payload_len(bytes)?;
        let frame_end = frame_offset
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|value| value.checked_add(payload_len))
            .ok_or(ObjectLogError::LengthOverflow)?;
        if frame_end > payload_offset(region.committed_end_offset)? {
            return Err(ObjectLogError::InvalidFrame);
        }
        Ok(payload_len)
    }

    fn get_range_frontier<R, F>(
        &self,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
        object_offset: u64,
        len: u64,
        scratch: &mut [u8],
        read: F,
    ) -> Result<R, ObjectLogError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let frame_offset = payload_offset(handle.offset)?;
        let bytes = self
            .memory
            .frontier_payload
            .get(frame_offset..)
            .ok_or(ObjectLogError::InvalidHandle)?;
        let payload_len = decode_frame_payload_len(bytes)?;
        let frame_end = frame_offset
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|value| value.checked_add(payload_len))
            .ok_or(ObjectLogError::LengthOverflow)?;
        if frame_end > payload_offset(region.committed_end_offset)? {
            return Err(ObjectLogError::InvalidFrame);
        }
        let payload = self
            .memory
            .frontier_payload
            .get(frame_offset + FRAME_HEADER_LEN..frame_end)
            .ok_or(ObjectLogError::InvalidFrame)?;
        let range = checked_object_read_range(payload_len, object_offset, len, scratch.len())?;
        let read_len = range.end - range.start;
        scratch[..read_len].copy_from_slice(&payload[range]);
        Ok(read(&scratch[..read_len]))
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
            if region.start_offset < object_start {
                return Err(ObjectLogError::InvalidEncoding);
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
    ) -> Result<(), ObjectLogError> {
        let region = self
            .find_region(handle.region_index, handle.sequence)
            .and_then(|index| self.memory.regions.get(index).copied())
            .ok_or(ObjectLogError::InvalidHandle)?;
        if !region.contains_committed(handle) {
            return Err(ObjectLogError::InvalidHandle);
        }
        if region.flushed {
            self.validate_flushed_frame(storage, region, handle)
        } else {
            self.validate_frontier_frame(region, handle)
        }
    }

    fn validate_frontier_frame(
        &self,
        region: ObjectLogRegion,
        handle: ObjectLogHandle,
    ) -> Result<(), ObjectLogError> {
        let frame_offset = payload_offset(handle.offset)?;
        let bytes = self
            .memory
            .frontier_payload
            .get(frame_offset..)
            .ok_or(ObjectLogError::InvalidHandle)?;
        let payload_len = decode_frame_payload_len(bytes)?;
        let frame_end = frame_offset
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|value| value.checked_add(payload_len))
            .ok_or(ObjectLogError::LengthOverflow)?;
        if frame_end > payload_offset(region.committed_end_offset)? {
            return Err(ObjectLogError::InvalidFrame);
        }
        let payload = self
            .memory
            .frontier_payload
            .get(frame_offset + FRAME_HEADER_LEN..frame_end)
            .ok_or(ObjectLogError::InvalidFrame)?;
        let frame_header = self
            .memory
            .frontier_payload
            .get(frame_offset..frame_offset + FRAME_HEADER_LEN)
            .ok_or(ObjectLogError::InvalidFrame)?;
        validate_frame_checksum(frame_header, payload)
    }

    fn validate_flushed_frame<
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
    ) -> Result<(), ObjectLogError> {
        self.validate_flushed_region_prologue(storage, region)?;
        let mut header = [0u8; FRAME_HEADER_LEN];
        storage
            .backing
            .read_region(
                handle.region_index,
                usize::try_from(handle.offset).map_err(|_| ObjectLogError::LengthOverflow)?,
                FRAME_HEADER_LEN,
                |bytes| header.copy_from_slice(bytes),
            )
            .map_err(StorageRuntimeError::from)?;
        let payload_len = decode_frame_payload_len(&header)?;
        let frame_end = usize::try_from(handle.offset)
            .map_err(|_| ObjectLogError::LengthOverflow)?
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|value| value.checked_add(payload_len))
            .ok_or(ObjectLogError::LengthOverflow)?;
        if frame_end
            > usize::try_from(region.committed_end_offset)
                .map_err(|_| ObjectLogError::LengthOverflow)?
        {
            return Err(ObjectLogError::InvalidFrame);
        }
        if storage.memory.payload_scratch.len() < payload_len {
            return Err(ObjectLogError::InvalidFrame);
        }
        storage
            .backing
            .read_region(
                handle.region_index,
                usize::try_from(handle.offset)
                    .map_err(|_| ObjectLogError::LengthOverflow)?
                    .checked_add(FRAME_HEADER_LEN)
                    .ok_or(ObjectLogError::LengthOverflow)?,
                payload_len,
                |bytes| storage.memory.payload_scratch[..payload_len].copy_from_slice(bytes),
            )
            .map_err(StorageRuntimeError::from)?;
        validate_frame_checksum(&header, &storage.memory.payload_scratch[..payload_len])?;
        Ok(())
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
    /// A stored object frame was invalid.
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
            log.apply_append(handle, bytes, append_visibility)?;
        }
        UPDATE_TRUNCATE_HEAD => {
            let handle = read_handle(payload, &mut offset)?;
            let mut freed = Vec::<u32, MAX_REGIONS>::new();
            let mut log = ObjectLog {
                collection_id: CollectionId::new(0),
                memory,
            };
            log.apply_truncate_before(handle, &mut freed)?;
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

const EMPTY_SNAPSHOT: [u8; 16] = [b'O', b'L', b'G', b'S', 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

fn encode_append_update(
    handle: ObjectLogHandle,
    bytes: &[u8],
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    let mut offset = 0usize;
    offset = write_u8(output, offset, UPDATE_APPEND)?;
    offset = write_handle(output, offset, handle)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(bytes.len()).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    write_bytes(output, offset, bytes)
}

fn encode_truncate_update(
    handle: ObjectLogHandle,
    output: &mut [u8],
) -> Result<usize, ObjectLogError> {
    let mut offset = 0usize;
    offset = write_u8(output, offset, UPDATE_TRUNCATE_HEAD)?;
    write_handle(output, offset, handle)
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
        offset = write_u32(output, offset, region.region_index)?;
        offset = write_u64(output, offset, region.sequence)?;
        offset = write_u32(output, offset, region.start_offset)?;
        offset = write_u32(output, offset, region.end_offset)?;
        offset = write_u32(output, offset, region.committed_end_offset)?;
        offset = write_u8(output, offset, if region.flushed { 1 } else { 0 })?;
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
        let region_index = read_u32(input, &mut offset)?;
        let sequence = read_u64(input, &mut offset)?;
        let next_sequence = next_sequence_after(sequence)?;
        let start_offset = read_u32(input, &mut offset)?;
        let end_offset = read_u32(input, &mut offset)?;
        let committed_end_offset = if version >= 2 {
            read_u32(input, &mut offset)?
        } else {
            end_offset
        };
        let region = ObjectLogRegion {
            region_index,
            sequence,
            start_offset,
            end_offset,
            committed_end_offset,
            flushed: match read_u8(input, &mut offset)? {
                0 => false,
                1 => true,
                _ => return Err(ObjectLogError::InvalidEncoding),
            },
        };
        if region.committed_end_offset > region.end_offset
            || region.committed_end_offset < region.start_offset
            || region.start_offset < object_start
        {
            return Err(ObjectLogError::InvalidEncoding);
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

fn encode_frame_into(bytes: &[u8], output: &mut [u8]) -> Result<(), ObjectLogError> {
    let frame_len = frame_len(bytes.len())?;
    if output.len() < frame_len {
        return Err(ObjectLogError::BufferTooSmall {
            needed: frame_len,
            available: output.len(),
        });
    }
    let mut offset = 0usize;
    offset = write_u32(
        output,
        offset,
        u32::try_from(bytes.len()).map_err(|_| ObjectLogError::LengthOverflow)?,
    )?;
    offset = write_u32(output, offset, crc32(bytes))?;
    let _ = write_bytes(output, offset, bytes)?;
    Ok(())
}

fn decode_frame_payload_len(input: &[u8]) -> Result<usize, ObjectLogError> {
    if input.len() < FRAME_HEADER_LEN {
        return Err(ObjectLogError::InvalidFrame);
    }
    let mut offset = 0usize;
    usize::try_from(read_u32(input, &mut offset)?).map_err(|_| ObjectLogError::LengthOverflow)
}

fn validate_frame_checksum(header: &[u8], payload: &[u8]) -> Result<(), ObjectLogError> {
    if header.len() < FRAME_HEADER_LEN {
        return Err(ObjectLogError::InvalidFrame);
    }
    let mut offset = size_of::<u32>();
    let expected = read_u32(header, &mut offset)?;
    if expected == crc32(payload) {
        Ok(())
    } else {
        Err(ObjectLogError::InvalidFrame)
    }
}

fn frame_len(payload_len: usize) -> Result<usize, ObjectLogError> {
    FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(ObjectLogError::LengthOverflow)
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

fn object_payload_capacity(
    payload_capacity: usize,
    log_metadata_len: usize,
) -> Result<usize, ObjectLogError> {
    Ok(payload_capacity
        .saturating_sub(data_prologue_len(log_metadata_len)?)
        .saturating_sub(FRAME_HEADER_LEN))
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
