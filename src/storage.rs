#![allow(clippy::too_many_arguments)]

use heapless::Vec;

use crate::disk::{
    encode_free_space_region_segment, encode_transaction_log_region_prefix_with_cursors,
    FreeQueuePosition, FreeSpaceCursors, FreeSpaceEntry, FreeSpaceRegionPrologue, Header,
    WalRegionPrologue, TRANSACTION_LOG_V2_FORMAT,
};
use crate::flash_io::{FlashIo, StorageFormatError, StorageIoError};
use crate::free_space::{FreeSpaceError, FreeSpaceState};
use crate::mock::{MockError, MockFormatError};
use crate::mode::StorageMode;
use crate::startup::{apply_wal_record, StartupCollection, StartupError, StartupOpenPlan};
use crate::wal_record::{
    decode_record, encode_record_into, encoded_record_len, LogPosition, TransactionLogRange,
    WalRecord, WalRecordError, WalRecordType,
};
use crate::workspace::StorageWorkspace;
use crate::StorageMetadata;
use crate::{CollectionId, CollectionType, StartupCollectionBasis};

pub(crate) const TRANSACTION_SLOT_COUNT: usize = 1;
const PRIMARY_TRANSACTION_SLOT_ID: u32 = 0;
pub(crate) const MAX_RETAINED_TRANSACTION_LOGS: usize = 128;
pub(crate) const MAX_RETAINED_TRANSACTION_LOG_REGIONS: usize = 32;
const MAX_TRANSACTION_SLOT_ALLOCATIONS: usize = 1024;

#[cfg(feature = "perf-counters")]
use crate::perf_metrics::{
    StoragePerfCounter, StoragePerfMetrics, StoragePerfTimer, StoragePerfTimerGuard,
};

/// Errors returned by low-level shared storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageRuntimeError {
    /// A public operation was requested while another operation mode was active.
    InvalidStorageMode {
        /// Required source mode.
        expected: StorageMode,
        /// Actual current mode.
        actual: StorageMode,
    },
    /// Formatting the backing store failed.
    Format(MockFormatError),
    /// Formatting the `embedded-storage` NOR flash adapter failed.
    #[cfg(feature = "embedded-storage")]
    EmbeddedStorageFormat(crate::embedded_storage::EmbeddedStorageFormatError),
    /// Formatting the Linux file-backed mmap backend failed.
    #[cfg(all(feature = "file-backing", target_os = "linux"))]
    FileBackingFormat(crate::file_backing::FileBackingFormatError),
    /// The backing I/O adapter failed.
    Mock(MockError),
    /// The `embedded-storage` NOR flash adapter failed.
    #[cfg(feature = "embedded-storage")]
    EmbeddedStorage(crate::embedded_storage::EmbeddedStorageError),
    /// The Linux file-backed mmap backend failed.
    #[cfg(all(feature = "file-backing", target_os = "linux"))]
    FileBacking(crate::file_backing::FileBackingError),
    /// Startup replay or recovery failed.
    Startup(StartupError),
    /// WAL record encoding or decoding failed.
    WalRecord(WalRecordError),
    /// The configured collection capacity was exceeded.
    TooManyTrackedCollections,
    /// A user collection attempted to use the reserved WAL collection id.
    ReservedCollectionId(CollectionId),
    /// A collection type is not supported by this build.
    UnsupportedCollectionType(u16),
    /// A collection was created more than once.
    DuplicateCollection(CollectionId),
    /// A referenced collection was not tracked.
    UnknownCollection(CollectionId),
    /// A referenced collection was already dropped.
    DroppedCollection(CollectionId),
    /// A retained record changed collection type unexpectedly.
    CollectionTypeMismatch {
        /// Collection being validated.
        collection_id: CollectionId,
        /// Previously retained collection type.
        expected: u16,
        /// Conflicting collection type.
        actual: u16,
    },
    /// A committed region did not belong to the named collection.
    InvalidHeadTarget {
        /// Collection being updated.
        collection_id: CollectionId,
        /// Region named by the head record.
        region_index: u32,
    },
    /// A free-space collection command did not match the recovered allocator state.
    InvalidFreeSpaceCommand,
    /// The durable free-space metadata chain cannot hold the current queue checkpoint.
    InsufficientFreeSpaceMetadataCapacity {
        /// Metadata regions required by the current free-space queue.
        required_regions: usize,
        /// Metadata regions available in the durable metadata chain.
        available_regions: usize,
    },
    /// The ready-region reserve was exhausted while dirty free-space entries remain.
    ReadyRegionReserveExhausted,
    /// The transaction log has no room for the requested private records.
    TransactionLogFull,
    /// Formatted metadata does not match this build's transaction slot count.
    TransactionLogCountMismatch {
        /// Transaction-log slots declared by media metadata.
        metadata_count: u32,
        /// Transaction-log slots supported by this runtime.
        slot_count: u32,
    },
    /// A transaction-control record referenced a slot outside this runtime.
    InvalidTransactionLogId {
        /// Transaction-log slot id from the WAL record.
        transaction_log_id: u32,
        /// Transaction-log slots supported by this runtime.
        slot_count: u32,
    },
    /// The requested operation requires explicit storage maintenance first.
    MaintenanceRequired,
    /// More than one ready region was observed.
    DoubleReadyRegion(u32),
    /// The current WAL tail does not have enough space for the requested record.
    WalRotationRequired,
    /// WAL rotation needed a free region but none were available.
    NoFreeRegionForRotation,
    /// `wal_recovery` was requested without a pending recovery boundary.
    WalRecoveryNotNeeded,
    /// WAL rotation state did not match the requested operation.
    InvalidRotationState {
        /// Ready region currently tracked in memory.
        ready_region: Option<u32>,
        /// Region requested by the caller, if any.
        requested_region: Option<u32>,
    },
    /// Remaining WAL space violated the expected rotation reserve window.
    InvalidRotationWindow {
        /// Bytes that would remain after the record append.
        remaining_after: usize,
        /// Bytes required for the `link` record.
        link_reserve: usize,
        /// Bytes required for the full rotation sequence.
        rotation_reserve: usize,
    },
    /// WAL-head reclaim requires at least two WAL regions.
    WalHeadReclaimRequiresMultipleWalRegions,
    /// WAL-head reclaim is blocked by an open recovery boundary.
    WalHeadReclaimBlockedByRecoveryBoundary,
    /// WAL-head reclaim is blocked by a reserved ready region.
    WalHeadReclaimBlockedByReadyRegion(u32),
    /// WAL-head reclaim is blocked by a retained record kind.
    WalHeadReclaimBlockedByRecord(WalRecordType),
    /// WAL-head reclaim exceeded the active-collection capacity.
    WalHeadReclaimTooManyActiveCollections,
    /// WAL-head reclaim encountered an unsupported collection type.
    WalHeadReclaimUnsupportedCollectionType(u16),
    /// Too few free regions remain to satisfy the configured reserve.
    InsufficientFreeRegions {
        /// Free regions currently available.
        free_regions: u32,
        /// Minimum free-region reserve required by metadata.
        min_free_regions: u32,
    },
    /// Too many dirty frontiers would violate the free-region reserve.
    TooManyDirtyFrontiers {
        /// Number of dirty frontiers that would be active.
        dirty_frontiers: usize,
        /// Minimum free-region reserve required by metadata.
        min_free_regions: u32,
    },
    /// A committed region payload did not fit within the usable region capacity.
    CommittedRegionTooLarge {
        /// Requested payload length in bytes.
        payload_len: usize,
        /// Maximum payload capacity in bytes.
        capacity: usize,
    },
    /// Caller-owned storage memory did not contain an initialized runtime slot.
    StorageMemoryUninitialized,
    /// A transaction was requested while another transaction was already open.
    TransactionAlreadyOpen(CollectionId),
    /// A transaction-scoped operation was requested without an open transaction.
    TransactionNotOpen(CollectionId),
    /// A transaction marker did not match the open transaction collection.
    TransactionMismatch {
        /// Collection expected by the open transaction.
        expected: CollectionId,
        /// Collection requested by the operation.
        actual: CollectionId,
    },
    /// A transaction finish was requested before the commit marker.
    TransactionNotCommitted(CollectionId),
    /// The collection changed after the transaction enrolled it.
    TransactionConflict {
        /// Collection enrolled by the transaction.
        collection_id: CollectionId,
        /// Generation observed when the transaction began.
        observed_generation: u64,
        /// Generation visible at commit validation.
        current_generation: u64,
    },
}

impl From<MockFormatError> for StorageRuntimeError {
    fn from(error: MockFormatError) -> Self {
        Self::Format(error)
    }
}

impl From<StorageFormatError> for StorageRuntimeError {
    fn from(error: StorageFormatError) -> Self {
        match error {
            StorageFormatError::Mock(error) => Self::Format(error),
            #[cfg(feature = "embedded-storage")]
            StorageFormatError::EmbeddedStorage(error) => Self::EmbeddedStorageFormat(error),
            #[cfg(all(feature = "file-backing", target_os = "linux"))]
            StorageFormatError::FileBacking(error) => Self::FileBackingFormat(error),
        }
    }
}

impl From<MockError> for StorageRuntimeError {
    fn from(error: MockError) -> Self {
        Self::Mock(error)
    }
}

impl From<StorageIoError> for StorageRuntimeError {
    fn from(error: StorageIoError) -> Self {
        match error {
            StorageIoError::Mock(error) => Self::Mock(error),
            #[cfg(feature = "embedded-storage")]
            StorageIoError::EmbeddedStorage(error) => Self::EmbeddedStorage(error),
            #[cfg(all(feature = "file-backing", target_os = "linux"))]
            StorageIoError::FileBacking(error) => Self::FileBacking(error),
        }
    }
}

impl From<StartupError> for StorageRuntimeError {
    fn from(error: StartupError) -> Self {
        Self::Startup(error)
    }
}

impl From<WalRecordError> for StorageRuntimeError {
    fn from(error: WalRecordError) -> Self {
        Self::WalRecord(error)
    }
}

impl From<FreeSpaceError> for StorageRuntimeError {
    fn from(_error: FreeSpaceError) -> Self {
        Self::InvalidFreeSpaceCommand
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionLogOutcome {
    Committed,
    RolledBack,
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetainedTransactionLog {
    pub(crate) transaction_log_id: u32,
    pub(crate) range: TransactionLogRange,
    pub(crate) regions: Vec<u32, MAX_RETAINED_TRANSACTION_LOG_REGIONS>,
    pub(crate) outcome: TransactionLogOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransactionAllocation {
    pub(crate) region_index: u32,
    pub(crate) allocation_head_after: FreeQueuePosition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionCollectionEffect {
    NewCollection {
        collection_id: CollectionId,
        collection_type: u16,
    },
    Update {
        collection_id: CollectionId,
    },
    Snapshot {
        collection_id: CollectionId,
        collection_type: u16,
    },
    Head {
        collection_id: CollectionId,
        collection_type: u16,
        region_index: u32,
    },
    DropCollection {
        collection_id: CollectionId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum TransactionSlot {
    Empty,
    Active {
        head_region: u32,
        tail_region: u32,
        append_offset: usize,
        start: LogPosition,
        collection_id: CollectionId,
        observed_collection_generation: u64,
        enrollment_written: bool,
        regions: Vec<u32, MAX_RETAINED_TRANSACTION_LOG_REGIONS>,
        allocated_regions: Vec<TransactionAllocation, MAX_TRANSACTION_SLOT_ALLOCATIONS>,
        free_intents: Vec<u32, MAX_TRANSACTION_SLOT_ALLOCATIONS>,
        collection_effects: Vec<TransactionCollectionEffect, MAX_TRANSACTION_SLOT_ALLOCATIONS>,
    },
}

impl TransactionSlot {
    fn empty() -> Self {
        Self::Empty
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveTransactionSnapshot {
    collection_id: CollectionId,
    start: LogPosition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalHeadReclaimAction {
    Skip,
    CopyEncoded,
    RewriteEmptyBasisAsSnapshot {
        collection_id: CollectionId,
        collection_type: u16,
    },
}

#[derive(Debug)]
pub(crate) struct WalHeadReclaimPlan<const MAX_COLLECTIONS: usize> {
    pub(crate) old_head: u32,
    source_tail: u32,
    source_tail_append_offset: usize,
    original_collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    imported_transaction_logs: Vec<TransactionLogRange, MAX_RETAINED_TRANSACTION_LOGS>,
}

impl<const MAX_COLLECTIONS: usize> WalHeadReclaimPlan<MAX_COLLECTIONS> {
    pub(crate) fn empty() -> Self {
        Self {
            old_head: 0,
            source_tail: 0,
            source_tail_append_offset: 0,
            original_collections: Vec::new(),
            imported_transaction_logs: Vec::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.original_collections.clear();
        self.imported_transaction_logs.clear();
    }

    pub(crate) fn limit_to_source_tail(
        &mut self,
        source_tail: u32,
        source_tail_append_offset: usize,
    ) {
        self.source_tail = source_tail;
        self.source_tail_append_offset = source_tail_append_offset;
    }
}

/// Advanced runtime state for the shared Borromean storage engine.
#[derive(Debug)]
pub struct StorageRuntime<const MAX_COLLECTIONS: usize = 8> {
    metadata: StorageMetadata,
    wal_head: u32,
    wal_tail: u32,
    wal_append_offset: usize,
    free_space: FreeSpaceState,
    ready_region: Option<u32>,
    max_seen_sequence: u64,
    collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    pending_wal_recovery_boundary: bool,
    transaction_slots: [TransactionSlot; TRANSACTION_SLOT_COUNT],
    retained_transaction_logs: Vec<RetainedTransactionLog, MAX_RETAINED_TRANSACTION_LOGS>,
    transaction_original_collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    transaction_original_free_space: Option<FreeSpaceState>,
    transaction_original_ready_region: Option<u32>,
    transaction_original_ready_region_valid: bool,
}

impl<const MAX_COLLECTIONS: usize> StorageRuntime<MAX_COLLECTIONS> {
    pub const SLOT_COUNT: usize = TRANSACTION_SLOT_COUNT;

    pub(crate) fn empty() -> Self {
        Self {
            metadata: StorageMetadata {
                storage_version: 0,
                region_size: 0,
                region_count: 0,
                min_free_regions: 0,
                transaction_log_count: 0,
                wal_write_granule: 0,
                erased_byte: 0,
                wal_record_magic: 0,
            },
            wal_head: 0,
            wal_tail: 0,
            wal_append_offset: 0,
            free_space: FreeSpaceState::empty(),
            ready_region: None,
            max_seen_sequence: 0,
            collections: Vec::new(),
            pending_wal_recovery_boundary: false,
            transaction_slots: core::array::from_fn(|_| TransactionSlot::empty()),
            retained_transaction_logs: Vec::new(),
            transaction_original_collections: Vec::new(),
            transaction_original_free_space: None,
            transaction_original_ready_region: None,
            transaction_original_ready_region_valid: false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn replace_from_startup_parts(
        &mut self,
        metadata: StorageMetadata,
        wal_head: u32,
        wal_tail: u32,
        wal_append_offset: usize,
        free_space: FreeSpaceState,
        ready_region: Option<u32>,
        max_seen_sequence: u64,
        collections: &[StartupCollection],
        retained_transaction_logs: &[RetainedTransactionLog],
        pending_wal_recovery_boundary: bool,
    ) -> Result<(), StorageRuntimeError> {
        let slot_count = u32::try_from(TRANSACTION_SLOT_COUNT)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        if metadata.transaction_log_count != slot_count {
            return Err(StorageRuntimeError::TransactionLogCountMismatch {
                metadata_count: metadata.transaction_log_count,
                slot_count,
            });
        }

        self.collections.clear();
        for collection in collections.iter().copied() {
            self.collections
                .push(collection)
                .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)?;
        }

        self.metadata = metadata;
        self.wal_head = wal_head;
        self.wal_tail = wal_tail;
        self.wal_append_offset = wal_append_offset;
        self.free_space = free_space;
        self.ready_region = ready_region;
        self.max_seen_sequence = max_seen_sequence;
        self.pending_wal_recovery_boundary = pending_wal_recovery_boundary;
        self.transaction_slots = core::array::from_fn(|_| TransactionSlot::empty());
        self.retained_transaction_logs.clear();
        for retained in retained_transaction_logs.iter().cloned() {
            self.retained_transaction_logs
                .push(retained)
                .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
        }
        self.clear_transaction_runtime_snapshot();
        Ok(())
    }

    fn validate_supported_user_collection_type(
        collection_id: CollectionId,
        collection_type: u16,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id == CollectionId(0) {
            return Err(StorageRuntimeError::ReservedCollectionId(collection_id));
        }
        if !matches!(
            collection_type,
            CollectionType::MAP_CODE | CollectionType::OBJECT_LOG_CODE
        ) {
            return Err(StorageRuntimeError::UnsupportedCollectionType(
                collection_type,
            ));
        }
        Ok(())
    }

    fn validate_supported_head_collection_type(
        collection_id: CollectionId,
        collection_type: u16,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id == CollectionId(0) {
            if collection_type == CollectionType::WAL_CODE {
                return Ok(());
            }
            return Err(StorageRuntimeError::CollectionTypeMismatch {
                collection_id,
                expected: CollectionType::WAL_CODE,
                actual: collection_type,
            });
        }

        Self::validate_supported_user_collection_type(collection_id, collection_type)
    }

    /// Returns storage metadata recovered from disk.
    pub fn metadata(&self) -> StorageMetadata {
        self.metadata
    }

    /// Returns the current WAL head region index.
    pub fn wal_head(&self) -> u32 {
        self.wal_head
    }

    /// Returns the current WAL tail region index.
    pub fn wal_tail(&self) -> u32 {
        self.wal_tail
    }

    /// Returns the next append offset within the WAL tail region.
    pub fn wal_append_offset(&self) -> usize {
        self.wal_append_offset
    }

    /// Returns the ready entry at the current free-space allocation head, if any.
    pub fn ready_free_region(&self) -> Option<u32> {
        self.free_space.next_ready_region().ok()
    }

    /// Returns the region at the current free-space append tail, if any.
    pub fn free_space_tail_region(&self) -> Option<u32> {
        self.free_space.entries().last().copied()
    }

    #[cfg(test)]
    pub(crate) fn free_space_cursors(&self) -> (u32, u32, u32, u32, u32) {
        (
            self.free_space.allocation_head(),
            self.free_space.ready_boundary(),
            self.free_space.append_tail(),
            self.free_space.ready_count(),
            self.free_space.dirty_count(),
        )
    }

    #[cfg(test)]
    pub(crate) fn free_space_entries(&self) -> &[u32] {
        self.free_space.entries()
    }

    /// Returns the current free-space allocation cursor.
    pub fn allocation_head(&self) -> FreeQueuePosition {
        self.free_space.allocation_head_position()
    }

    /// Returns the current free-space ready boundary.
    pub fn ready_boundary(&self) -> FreeQueuePosition {
        self.free_space.ready_boundary_position()
    }

    /// Returns the current free-space append tail.
    pub fn append_tail(&self) -> FreeQueuePosition {
        self.free_space.append_tail_position()
    }

    /// Returns a reserved ready region, if one exists.
    pub fn ready_region(&self) -> Option<u32> {
        self.ready_region
    }

    /// Returns the largest region sequence observed so far.
    pub fn max_seen_sequence(&self) -> u64 {
        self.max_seen_sequence
    }

    /// Returns the replay-tracked collections.
    pub fn collections(&self) -> &[StartupCollection] {
        self.collections.as_slice()
    }

    /// Returns whether replay left an open WAL recovery boundary.
    pub fn pending_wal_recovery_boundary(&self) -> bool {
        self.pending_wal_recovery_boundary
    }

    /// Returns the number of non-dropped user collections.
    pub fn tracked_user_collection_count(&self) -> usize {
        self.collections
            .iter()
            .filter(|collection| collection.basis() != StartupCollectionBasis::Dropped)
            .count()
    }

    /// Reserves a free region for a future committed-region write or WAL rotation.
    pub(crate) fn reserve_next_region<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<u32, StorageRuntimeError> {
        self.reserve_next_region_for::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            CollectionId(0),
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
        )
    }

    pub(crate) fn reserve_next_region_for<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<u32, StorageRuntimeError> {
        let transaction_owned_allocation = if collection_id != CollectionId(0) {
            self.require_collection_transaction(collection_id)?;
            true
        } else {
            false
        };

        if !transaction_owned_allocation {
            self.ensure_foreground_allocation_headroom::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                reclaim_source_regions,
                active_collections,
                reclaim_plan,
                open_plan,
            )?;
        }

        let mut allocated_region = None;
        for _attempt in 0..self.metadata.region_count {
            let region_index = self
                .free_space
                .next_ready_region()
                .map_err(|_| StorageRuntimeError::NoFreeRegionForRotation)?;
            let allocation_head_after = self.free_space.position_after_allocation()?;
            match self.append_allocate_region_for_collection::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                region_index,
                allocation_head_after,
            ) {
                Ok(()) => {
                    self.track_transaction_allocation(
                        collection_id,
                        region_index,
                        allocation_head_after,
                    )?;
                    allocated_region = Some(region_index);
                    break;
                }
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        allocated_region.ok_or(StorageRuntimeError::WalRotationRequired)
    }

    /// Writes a committed collection region and syncs it durably.
    pub fn write_committed_region<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
        collection_id: CollectionId,
        collection_format: u16,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        let payload_capacity = committed_payload_capacity::<REGION_SIZE>(self.metadata)?;
        if payload.len() > payload_capacity {
            return Err(StorageRuntimeError::CommittedRegionTooLarge {
                payload_len: payload.len(),
                capacity: payload_capacity,
            });
        }

        let sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;

        flash.erase_region(region_index)?;

        let write_len = committed_write_len(self.metadata, payload.len())?;
        let target = workspace.committed_write_buffer();
        let target =
            target
                .get_mut(..write_len)
                .ok_or(StorageRuntimeError::CommittedRegionTooLarge {
                    payload_len: payload.len(),
                    capacity: payload_capacity,
                })?;
        target.fill(self.metadata.erased_byte);
        let header = Header {
            sequence,
            collection_id,
            collection_format,
        };
        header
            .encode_into(target)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        let payload_end = Header::ENCODED_LEN
            .checked_add(payload.len())
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let target_payload = target.get_mut(Header::ENCODED_LEN..payload_end).ok_or(
            StorageRuntimeError::CommittedRegionTooLarge {
                payload_len: payload.len(),
                capacity: payload_capacity,
            },
        )?;
        target_payload.copy_from_slice(payload);
        flash.write_region(region_index, 0, target)?;
        flash.sync()?;
        self.max_seen_sequence = sequence;
        if self.ready_region == Some(region_index) {
            self.ready_region = None;
        }
        Ok(())
    }

    fn validate_transaction_log_id(transaction_log_id: u32) -> Result<usize, StorageRuntimeError> {
        let slot_count = u32::try_from(TRANSACTION_SLOT_COUNT)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        if transaction_log_id >= slot_count {
            return Err(StorageRuntimeError::InvalidTransactionLogId {
                transaction_log_id,
                slot_count,
            });
        }
        usize::try_from(transaction_log_id).map_err(|_| StorageRuntimeError::WalRotationRequired)
    }

    fn active_transaction_snapshot(&self) -> Option<ActiveTransactionSnapshot> {
        match self
            .transaction_slots
            .get(PRIMARY_TRANSACTION_SLOT_ID as usize)
        {
            Some(TransactionSlot::Active {
                collection_id,
                start,
                ..
            }) => Some(ActiveTransactionSnapshot {
                collection_id: *collection_id,
                start: *start,
            }),
            _ => None,
        }
    }

    fn require_collection_transaction(
        &self,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        let Some(open) = self.active_transaction_snapshot() else {
            return Err(StorageRuntimeError::TransactionNotOpen(collection_id));
        };
        if open.collection_id != collection_id {
            return Err(StorageRuntimeError::TransactionMismatch {
                expected: open.collection_id,
                actual: collection_id,
            });
        }
        Ok(())
    }

    fn transaction_private_record_collection_id(record: WalRecord<'_>) -> Option<CollectionId> {
        match record {
            WalRecord::NewCollection { collection_id, .. }
            | WalRecord::Update { collection_id, .. }
            | WalRecord::Snapshot { collection_id, .. }
            | WalRecord::Head { collection_id, .. }
            | WalRecord::DropCollection { collection_id }
            | WalRecord::AddTransactionCollection { collection_id, .. }
            | WalRecord::FreeIntent { collection_id, .. } => Some(collection_id),
            WalRecord::AllocateRegion { .. }
            | WalRecord::EraseFreeRegionSpan { .. }
            | WalRecord::FreeRegion { .. }
            | WalRecord::BeginInlineTransaction { .. }
            | WalRecord::CommitInlineTransaction { .. }
            | WalRecord::RollbackInlineTransaction { .. }
            | WalRecord::BeginTransaction { .. }
            | WalRecord::CommitTransaction { .. }
            | WalRecord::TransactionFinished { .. }
            | WalRecord::RollbackTransaction { .. }
            | WalRecord::Link { .. }
            | WalRecord::WalRecovery => None,
        }
    }

    fn transaction_collection_effect(record: WalRecord<'_>) -> Option<TransactionCollectionEffect> {
        match record {
            WalRecord::NewCollection {
                collection_id,
                collection_type,
            } => Some(TransactionCollectionEffect::NewCollection {
                collection_id,
                collection_type,
            }),
            WalRecord::Update { collection_id, .. } => {
                Some(TransactionCollectionEffect::Update { collection_id })
            }
            WalRecord::Snapshot {
                collection_id,
                collection_type,
                ..
            } => Some(TransactionCollectionEffect::Snapshot {
                collection_id,
                collection_type,
            }),
            WalRecord::Head {
                collection_id,
                collection_type,
                region_index,
            } => Some(TransactionCollectionEffect::Head {
                collection_id,
                collection_type,
                region_index,
            }),
            WalRecord::DropCollection { collection_id } => {
                Some(TransactionCollectionEffect::DropCollection { collection_id })
            }
            _ => None,
        }
    }

    fn collection_transaction_has_committed_outcome(
        &self,
        collection_id: CollectionId,
    ) -> Result<bool, StorageRuntimeError> {
        let Some(open) = self.active_transaction_snapshot() else {
            return Ok(false);
        };
        if open.collection_id != collection_id {
            return Err(StorageRuntimeError::TransactionMismatch {
                expected: open.collection_id,
                actual: collection_id,
            });
        }
        Ok(self.active_transaction_has_committed_outcome(open.start))
    }

    fn track_transaction_allocation(
        &mut self,
        collection_id: CollectionId,
        region_index: u32,
        allocation_head_after: FreeQueuePosition,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id == CollectionId(0) {
            return Ok(());
        }
        let slot = Self::validate_transaction_log_id(PRIMARY_TRANSACTION_SLOT_ID)?;
        match self.transaction_slots.get_mut(slot) {
            Some(TransactionSlot::Active {
                collection_id: active_collection,
                allocated_regions,
                ..
            }) if *active_collection == collection_id => {
                if !allocated_regions
                    .iter()
                    .any(|allocation| allocation.region_index == region_index)
                {
                    allocated_regions
                        .push(TransactionAllocation {
                            region_index,
                            allocation_head_after,
                        })
                        .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
                }
                Ok(())
            }
            Some(TransactionSlot::Active {
                collection_id: active_collection,
                ..
            }) => Err(StorageRuntimeError::TransactionMismatch {
                expected: *active_collection,
                actual: collection_id,
            }),
            _ => Err(StorageRuntimeError::TransactionNotOpen(collection_id)),
        }
    }

    fn retain_transaction_log(
        &mut self,
        transaction_log_id: u32,
        range: TransactionLogRange,
        regions: &[u32],
        outcome: TransactionLogOutcome,
    ) -> Result<(), StorageRuntimeError> {
        Self::validate_transaction_log_id(transaction_log_id)?;
        if let Some(retained) = self.retained_transaction_logs.iter_mut().find(|retained| {
            retained.transaction_log_id == transaction_log_id && retained.range.start == range.start
        }) {
            retained.range = range;
            retained.outcome = outcome;
            retained.regions.clear();
            for region_index in regions.iter().copied() {
                retained
                    .regions
                    .push(region_index)
                    .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
            }
            return Ok(());
        }

        let mut retained_regions = Vec::new();
        for region_index in regions.iter().copied() {
            retained_regions
                .push(region_index)
                .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
        }
        self.retained_transaction_logs
            .push(RetainedTransactionLog {
                transaction_log_id,
                range,
                regions: retained_regions,
                outcome,
            })
            .map_err(|_| StorageRuntimeError::TransactionLogFull)
    }

    fn capture_transaction_runtime_snapshot(&mut self) -> Result<(), StorageRuntimeError> {
        self.transaction_original_collections.clear();
        for collection in self.collections.iter().copied() {
            self.transaction_original_collections
                .push(collection)
                .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)?;
        }
        self.transaction_original_free_space = Some(self.free_space.clone());
        self.transaction_original_ready_region = self.ready_region;
        self.transaction_original_ready_region_valid = true;
        Ok(())
    }

    fn restore_transaction_runtime_snapshot(&mut self) -> Result<(), StorageRuntimeError> {
        if let Some(free_space) = self.transaction_original_free_space.clone() {
            self.free_space = free_space;
        }
        if self.transaction_original_ready_region_valid {
            self.ready_region = self.transaction_original_ready_region;
        }
        self.collections.clear();
        for collection in self.transaction_original_collections.iter().copied() {
            self.collections
                .push(collection)
                .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)?;
        }
        Ok(())
    }

    fn clear_transaction_runtime_snapshot(&mut self) {
        self.transaction_original_collections.clear();
        self.transaction_original_free_space = None;
        self.transaction_original_ready_region = None;
        self.transaction_original_ready_region_valid = false;
    }

    fn active_transaction_has_committed_outcome(&self, start: LogPosition) -> bool {
        self.retained_transaction_logs.iter().any(|retained| {
            retained.transaction_log_id == PRIMARY_TRANSACTION_SLOT_ID
                && retained.range.start == start
                && matches!(
                    retained.outcome,
                    TransactionLogOutcome::Committed | TransactionLogOutcome::Finished
                )
        })
    }

    pub(crate) fn write_committed_region_from_workspace_payload<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
        collection_id: CollectionId,
        collection_format: u16,
        payload_len: usize,
    ) -> Result<(), StorageRuntimeError> {
        let payload_capacity = committed_payload_capacity::<REGION_SIZE>(self.metadata)?;
        if payload_len > payload_capacity {
            return Err(StorageRuntimeError::CommittedRegionTooLarge {
                payload_len,
                capacity: payload_capacity,
            });
        }

        let sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;

        flash.erase_region(region_index)?;

        let write_len = committed_write_len(self.metadata, payload_len)?;
        let (target, payload_source) = workspace.committed_write_buffers();
        let target =
            target
                .get_mut(..write_len)
                .ok_or(StorageRuntimeError::CommittedRegionTooLarge {
                    payload_len,
                    capacity: payload_capacity,
                })?;
        target.fill(self.metadata.erased_byte);
        let header = Header {
            sequence,
            collection_id,
            collection_format,
        };
        header
            .encode_into(target)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        let payload_end = Header::ENCODED_LEN
            .checked_add(payload_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let target_payload = target.get_mut(Header::ENCODED_LEN..payload_end).ok_or(
            StorageRuntimeError::CommittedRegionTooLarge {
                payload_len,
                capacity: payload_capacity,
            },
        )?;
        let source_payload = payload_source.get(..payload_len).ok_or(
            StorageRuntimeError::CommittedRegionTooLarge {
                payload_len,
                capacity: payload_capacity,
            },
        )?;
        target_payload.copy_from_slice(source_payload);
        flash.write_region(region_index, 0, target)?;
        flash.sync()?;
        self.max_seen_sequence = sequence;
        if self.ready_region == Some(region_index) {
            self.ready_region = None;
        }
        Ok(())
    }

    /// Appends a `new_collection` record for a supported user collection.
    pub fn append_new_collection<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
    ) -> Result<(), StorageRuntimeError> {
        Self::validate_supported_user_collection_type(collection_id, collection_type)?;
        if self.find_collection(collection_id).is_some() {
            return Err(StorageRuntimeError::DuplicateCollection(collection_id));
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::NewCollection {
                collection_id,
                collection_type,
            },
        )
    }

    /// Appends a raw `update` payload for an existing live collection.
    pub fn append_update<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        if self.transaction_open_for(collection_id) {
            return self
                .append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::Update {
                        collection_id,
                        payload,
                    },
                );
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Update {
                collection_id,
                payload,
            },
        )
    }

    #[cfg_attr(feature = "perf-counters", allow(dead_code))]
    pub(crate) fn append_update_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        if self.transaction_open_for(collection_id) {
            return self
                .append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::Update {
                        collection_id,
                        payload,
                    },
                );
        }

        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Update {
                collection_id,
                payload,
            },
        )
    }

    #[cfg(feature = "perf-counters")]
    pub(crate) fn append_update_with_rotation_metered<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        payload: &[u8],
        metrics: &mut StoragePerfMetrics,
    ) -> Result<(), StorageRuntimeError> {
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        metrics.increment(StoragePerfCounter::WalUpdateRecords);
        if self.transaction_open_for(collection_id) {
            return self
                .append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::Update {
                        collection_id,
                        payload,
                    },
                );
        }
        self.append_record_with_rotation_metered::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Update {
                collection_id,
                payload,
            },
            metrics,
        )
    }

    /// Appends a raw `snapshot` payload for an existing live collection.
    pub fn append_snapshot<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        Self::validate_supported_user_collection_type(collection_id, collection_type)?;
        if let Some(collection) = self.find_collection(collection_id) {
            if collection.basis() == StartupCollectionBasis::Dropped {
                return Err(StorageRuntimeError::DroppedCollection(collection_id));
            }
            if let Some(expected) = collection.collection_type() {
                if expected != collection_type {
                    return Err(StorageRuntimeError::CollectionTypeMismatch {
                        collection_id,
                        expected,
                        actual: collection_type,
                    });
                }
            }
        }

        if self.transaction_open_for(collection_id) {
            return self
                .append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::Snapshot {
                        collection_id,
                        collection_type,
                        payload,
                    },
                );
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Snapshot {
                collection_id,
                collection_type,
                payload,
            },
        )
    }

    /// Appends a raw `snapshot` payload, rotating the WAL first if needed.
    pub(crate) fn append_snapshot_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        Self::validate_supported_user_collection_type(collection_id, collection_type)?;
        if let Some(collection) = self.find_collection(collection_id) {
            if collection.basis() == StartupCollectionBasis::Dropped {
                return Err(StorageRuntimeError::DroppedCollection(collection_id));
            }
            if let Some(expected) = collection.collection_type() {
                if expected != collection_type {
                    return Err(StorageRuntimeError::CollectionTypeMismatch {
                        collection_id,
                        expected,
                        actual: collection_type,
                    });
                }
            }
        }

        if self.transaction_open_for(collection_id) {
            return self
                .append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::Snapshot {
                        collection_id,
                        collection_type,
                        payload,
                    },
                );
        }

        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Snapshot {
                collection_id,
                collection_type,
                payload,
            },
        )
    }

    /// Appends a `drop_collection` record for an existing live collection.
    pub fn append_drop_collection<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        if self.transaction_open_for(collection_id) {
            return self
                .append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::DropCollection { collection_id },
                );
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::DropCollection { collection_id },
        )
    }

    /// Drops a collection and begins reclaim for its previous region basis, if any.
    pub fn drop_collection_and_begin_reclaim<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<Option<u32>, StorageRuntimeError> {
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        let previous_region = match collection.basis() {
            StartupCollectionBasis::Region(region_index) => Some(region_index),
            _ => None,
        };

        if let Some(region_index) = previous_region {
            self.ensure_free_space_metadata_capacity_for_len::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.free_space.entries().len().saturating_add(1),
            )?;
            let records = [
                WalRecord::DropCollection { collection_id },
                WalRecord::FreeRegion {
                    region_index,
                    append_tail_after: self.free_space.position_after_append()?,
                },
            ];
            self.append_internal_atomic_records_for_collection_with_rotation::<
                REGION_SIZE,
                REGION_COUNT,
                IO,
            >(flash, workspace, collection_id, &records)?;
        } else {
            let records = [WalRecord::DropCollection { collection_id }];
            self.append_internal_atomic_records_for_collection_with_rotation::<
                REGION_SIZE,
                REGION_COUNT,
                IO,
            >(flash, workspace, collection_id, &records)?;
        }

        Ok(previous_region)
    }

    /// Appends a `head` record pointing at a committed region.
    pub fn append_head<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        Self::validate_supported_head_collection_type(collection_id, collection_type)?;
        let collection = self.find_collection(collection_id);
        if let Some(collection) = collection {
            if collection.basis() == StartupCollectionBasis::Dropped {
                return Err(StorageRuntimeError::DroppedCollection(collection_id));
            }
            if let Some(expected) = collection.collection_type() {
                if expected != collection_type {
                    return Err(StorageRuntimeError::CollectionTypeMismatch {
                        collection_id,
                        expected,
                        actual: collection_type,
                    });
                }
            }
        }

        let header = read_header_from_flash::<REGION_SIZE, REGION_COUNT, IO>(flash, region_index)
            .map_err(|_| StorageRuntimeError::InvalidHeadTarget {
            collection_id,
            region_index,
        })?;
        if header.collection_id != collection_id {
            return Err(StorageRuntimeError::InvalidHeadTarget {
                collection_id,
                region_index,
            });
        }

        if self.transaction_open_for(collection_id) {
            self.append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                WalRecord::Head {
                    collection_id,
                    collection_type,
                    region_index,
                },
            )?;
            if collection_id != CollectionId(0) {
                self.max_seen_sequence = self.max_seen_sequence.max(header.sequence);
            }
            return Ok(());
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Head {
                collection_id,
                collection_type,
                region_index,
            },
        )?;
        if collection_id != CollectionId(0) {
            self.max_seen_sequence = self.max_seen_sequence.max(header.sequence);
        }
        Ok(())
    }

    /// Appends a `head` record, rotating the WAL first if the current tail lacks room.
    pub(crate) fn append_head_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        Self::validate_supported_head_collection_type(collection_id, collection_type)?;
        let collection = self.find_collection(collection_id);
        if let Some(collection) = collection {
            if collection.basis() == StartupCollectionBasis::Dropped {
                return Err(StorageRuntimeError::DroppedCollection(collection_id));
            }
            if let Some(expected) = collection.collection_type() {
                if expected != collection_type {
                    return Err(StorageRuntimeError::CollectionTypeMismatch {
                        collection_id,
                        expected,
                        actual: collection_type,
                    });
                }
            }
        }

        let header = read_header_from_flash::<REGION_SIZE, REGION_COUNT, IO>(flash, region_index)
            .map_err(|_| StorageRuntimeError::InvalidHeadTarget {
            collection_id,
            region_index,
        })?;
        if header.collection_id != collection_id {
            return Err(StorageRuntimeError::InvalidHeadTarget {
                collection_id,
                region_index,
            });
        }

        if self.transaction_open_for(collection_id) {
            self.append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                WalRecord::Head {
                    collection_id,
                    collection_type,
                    region_index,
                },
            )?;
            if collection_id != CollectionId(0) {
                self.max_seen_sequence = self.max_seen_sequence.max(header.sequence);
            }
            return Ok(());
        }

        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Head {
                collection_id,
                collection_type,
                region_index,
            },
        )?;
        if collection_id != CollectionId(0) {
            self.max_seen_sequence = self.max_seen_sequence.max(header.sequence);
        }
        Ok(())
    }

    /// Ensures a `head` record can be appended, rotating before a ready region is held.
    #[cfg(test)]
    pub(crate) fn ensure_head_append_room_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        let mut allocation_region = region_index;
        for _attempt in 0..self.metadata.region_count {
            if self.free_space.next_ready_region().ok() != Some(allocation_region) {
                let Some(current_head) = self.free_space.next_ready_region().ok() else {
                    return Ok(());
                };
                allocation_region = current_head;
            }
            match self.ensure_post_allocation_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
                workspace,
                flash,
                allocation_region,
                WalRecord::Head {
                    collection_id,
                    collection_type,
                    region_index: allocation_region,
                },
            ) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageRuntimeError::WalRotationRequired)
    }

    /// Appends an `allocate_region` record for the current ready free-space head.
    pub fn append_allocate_region<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
        allocation_head_after: FreeQueuePosition,
    ) -> Result<(), StorageRuntimeError> {
        if self.free_space.next_ready_region()? != region_index {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }
        if self.free_space.position_after_allocation()? != allocation_head_after {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }
        self.append_allocate_region_to_main_wal::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            region_index,
            allocation_head_after,
        )
    }

    fn append_allocate_region_for_collection<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        region_index: u32,
        allocation_head_after: FreeQueuePosition,
    ) -> Result<(), StorageRuntimeError> {
        if self.free_space.next_ready_region()? != region_index {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }
        if self.free_space.position_after_allocation()? != allocation_head_after {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }

        if collection_id != CollectionId(0) {
            self.require_collection_transaction(collection_id)?;
            return self
                .append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::AllocateRegion {
                        region_index,
                        allocation_head_after,
                    },
                );
        }

        self.append_allocate_region_to_main_wal::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            region_index,
            allocation_head_after,
        )
    }

    fn append_allocate_region_to_main_wal<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
        allocation_head_after: FreeQueuePosition,
    ) -> Result<(), StorageRuntimeError> {
        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::AllocateRegion {
                region_index,
                allocation_head_after,
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn append_allocate_region_for_test<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        let allocation_head_after = self.free_space.position_after_allocation()?;
        self.append_allocate_region::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            region_index,
            allocation_head_after,
        )
    }

    /// Reclaims the current WAL prefix and returns the new head region.
    pub(crate) fn reclaim_wal_head<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<u32, StorageRuntimeError> {
        self.reclaim_wal_head_inner::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            source_regions,
            active_collections,
            plan,
            open_plan,
            #[cfg(feature = "perf-counters")]
            None,
        )
    }

    #[cfg(feature = "perf-counters")]
    pub(crate) fn reclaim_wal_head_metered<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        metrics: &mut StoragePerfMetrics,
    ) -> Result<u32, StorageRuntimeError> {
        self.reclaim_wal_head_inner::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            source_regions,
            active_collections,
            plan,
            open_plan,
            Some(metrics),
        )
    }

    fn reclaim_wal_head_inner<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        #[cfg(feature = "perf-counters")] metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<u32, StorageRuntimeError> {
        self.ensure_free_space_metadata_capacity_for_len::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.free_space.entries().len(),
        )?;
        self.materialize_free_space_collection::<REGION_SIZE, IO>(flash)?;
        self.prepare_wal_head_reclaim::<REGION_SIZE, IO>(flash, workspace, plan)?;
        source_regions.clear();
        self.collect_wal_head_reclaim_regions::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            plan,
            source_regions,
        )?;

        let new_head = source_regions
            .get(1)
            .copied()
            .ok_or(StorageRuntimeError::WalHeadReclaimRequiresMultipleWalRegions)?;
        plan.limit_to_source_tail(plan.old_head, REGION_SIZE);
        source_regions.truncate(1);

        for region_index in source_regions.iter().copied() {
            self.begin_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                region_index,
            )?;
        }
        active_collections.clear();
        self.copy_live_wal_head_reclaim_state::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            plan,
            active_collections,
            open_plan,
            #[cfg(feature = "perf-counters")]
            metrics,
        )?;
        self.commit_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, new_head)?;
        for region_index in source_regions.iter().copied() {
            self.append_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                CollectionId(0),
                region_index,
            )?;
        }
        self.retire_completed_transaction_logs_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace,
        )?;
        open_into::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash, workspace, self, open_plan,
        )?;
        Ok(new_head)
    }

    fn retire_completed_transaction_logs_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        let mut index = 0usize;
        while index < self.retained_transaction_logs.len() {
            let should_retire = matches!(
                self.retained_transaction_logs[index].outcome,
                TransactionLogOutcome::Finished | TransactionLogOutcome::RolledBack
            );
            if !should_retire {
                index += 1;
                continue;
            }
            if self.retained_transaction_log_is_reachable::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                &self.retained_transaction_logs[index],
            )? {
                index += 1;
                continue;
            }

            let regions = self.retained_transaction_logs[index].regions.clone();
            for region_index in regions.iter().copied() {
                if self.free_space.contains_free_region(region_index) {
                    continue;
                }
                self.append_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    CollectionId(0),
                    region_index,
                )?;
            }
            if regions
                .iter()
                .copied()
                .all(|region_index| self.free_space.contains_free_region(region_index))
            {
                self.retained_transaction_logs.remove(index);
            } else {
                index += 1;
            }
        }
        Ok(())
    }

    fn retained_transaction_log_is_reachable<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        retained: &RetainedTransactionLog,
    ) -> Result<bool, StorageRuntimeError> {
        let region_size = usize::try_from(self.metadata.region_size)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let granule = usize::try_from(self.metadata.wal_write_granule)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let mut current_region = self.wal_head;

        for _ in 0..self.metadata.region_count {
            let (region_bytes, _) = workspace.scan_buffers();
            flash.read_region(current_region, 0, region_bytes.len(), |bytes| {
                region_bytes.copy_from_slice(bytes);
            })?;

            let limit = if current_region == self.wal_tail {
                self.wal_append_offset
            } else {
                region_size
            };
            let mut offset = self
                .metadata
                .wal_record_area_offset()
                .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
            let mut next_region = None;

            while offset < limit {
                let step = {
                    let (region_bytes, logical_scratch) = workspace.scan_buffers();
                    let start_byte = region_bytes[offset];
                    if start_byte == self.metadata.erased_byte {
                        break;
                    }
                    if start_byte != self.metadata.wal_record_magic {
                        offset
                            .checked_add(granule)
                            .ok_or(StorageRuntimeError::WalRotationRequired)?
                    } else {
                        match decode_record(
                            &region_bytes[offset..limit],
                            self.metadata,
                            logical_scratch,
                        ) {
                            Ok(decoded) => {
                                if transaction_control_references_retained_log(
                                    decoded.record,
                                    retained,
                                ) {
                                    return Ok(true);
                                }
                                if let WalRecord::Link {
                                    next_region_index, ..
                                } = decoded.record
                                {
                                    next_region = Some(next_region_index);
                                }
                                offset
                                    .checked_add(decoded.encoded_len)
                                    .ok_or(StorageRuntimeError::WalRotationRequired)?
                            }
                            Err(_) => offset
                                .checked_add(granule)
                                .ok_or(StorageRuntimeError::WalRotationRequired)?,
                        }
                    }
                };
                offset = step;
                if next_region.is_some() {
                    break;
                }
            }

            if current_region == self.wal_tail {
                return Ok(false);
            }
            current_region =
                next_region.ok_or(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                    region_index: current_region,
                }))?;
        }

        Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
            region_index: current_region,
        }))
    }

    /// Appends a `free_region` WAL record, adding the region to the dirty free-space range.
    pub fn append_free_region<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id != CollectionId(0) {
            self.require_collection_transaction(collection_id)?;
            if !self.collection_transaction_has_committed_outcome(collection_id)? {
                return self.append_transaction_private_record_with_rotation::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                >(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::FreeIntent {
                        collection_id,
                        region_index,
                    },
                );
            }
        }
        self.ensure_free_space_metadata_capacity_for_len::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.free_space.entries().len().saturating_add(1),
        )?;
        self.ensure_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
            workspace,
            flash,
            WalRecord::FreeRegion {
                region_index,
                append_tail_after: self.free_space.position_after_append()?,
            },
        )?;
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::FreeRegion {
                region_index,
                append_tail_after: self.free_space.position_after_append()?,
            },
        )
    }

    /// Appends `free_region`, rotating the WAL first if the current tail lacks room.
    pub(crate) fn append_free_region_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id != CollectionId(0) {
            self.require_collection_transaction(collection_id)?;
            if !self.collection_transaction_has_committed_outcome(collection_id)? {
                return self.append_transaction_private_record_with_rotation::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                >(
                    flash,
                    workspace,
                    collection_id,
                    WalRecord::FreeIntent {
                        collection_id,
                        region_index,
                    },
                );
            }
        }
        self.ensure_free_space_metadata_capacity_for_len::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.free_space.entries().len().saturating_add(1),
        )?;
        self.ensure_record_append_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::FreeRegion {
                region_index,
                append_tail_after: self.free_space.position_after_append()?,
            },
        )?;
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::FreeRegion {
                region_index,
                append_tail_after: self.free_space.position_after_append()?,
            },
        )
    }

    fn erase_dirty_free_region_span_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        count: u32,
    ) -> Result<(), StorageRuntimeError> {
        if count == 0 {
            return Ok(());
        }
        if self.free_space.dirty_count() < count {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }
        let start = self.free_space.ready_boundary();
        let end = start
            .checked_add(count)
            .ok_or(StorageRuntimeError::InvalidFreeSpaceCommand)?;
        for entry_index in start..end {
            let region_index = *self
                .free_space
                .entries()
                .get(
                    usize::try_from(entry_index)
                        .map_err(|_| StorageRuntimeError::InvalidFreeSpaceCommand)?,
                )
                .ok_or(StorageRuntimeError::InvalidFreeSpaceCommand)?;
            flash.erase_region(region_index)?;
        }
        flash.sync()?;
        let ready_boundary_after = self.free_space.position_after_erase(count)?;
        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::EraseFreeRegionSpan {
                count,
                ready_boundary_after,
            },
        )
    }

    /// Appends a `wal_recovery` record for an open recovery boundary.
    pub fn append_wal_recovery<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        if !self.pending_wal_recovery_boundary {
            return Err(StorageRuntimeError::WalRecoveryNotNeeded);
        }
        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::WalRecovery,
        )
    }

    /// Starts an internal collection-scoped WAL transaction.
    pub(crate) fn begin_collection_transaction<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        if let Some(open) = self.active_transaction_snapshot() {
            return Err(StorageRuntimeError::TransactionAlreadyOpen(
                open.collection_id,
            ));
        }
        if collection_id == CollectionId(0) {
            return Err(StorageRuntimeError::ReservedCollectionId(collection_id));
        }
        let slot = Self::validate_transaction_log_id(PRIMARY_TRANSACTION_SLOT_ID)?;
        let observed_collection_generation = self
            .find_collection(collection_id)
            .map(StartupCollection::committed_generation)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        let head_region = self
            .allocate_privileged_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace,
            )?;
        let append_offset = self
            .initialize_transaction_log_region::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                head_region,
                head_region,
            )?;
        let start = LogPosition {
            region_index: head_region,
            offset: u32::try_from(append_offset)
                .map_err(|_| StorageRuntimeError::WalRotationRequired)?,
        };
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::BeginTransaction {
                transaction_log_id: PRIMARY_TRANSACTION_SLOT_ID,
                start,
            },
        )?;
        let mut regions = Vec::new();
        regions
            .push(head_region)
            .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
        self.capture_transaction_runtime_snapshot()?;
        self.transaction_slots[slot] = TransactionSlot::Active {
            head_region,
            tail_region: head_region,
            append_offset,
            collection_id,
            start,
            observed_collection_generation,
            enrollment_written: false,
            regions,
            allocated_regions: Vec::new(),
            free_intents: Vec::new(),
            collection_effects: Vec::new(),
        };
        self.append_transaction_private_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
            WalRecord::AddTransactionCollection {
                collection_id,
                observed_collection_generation,
            },
        )?;
        Ok(())
    }

    pub(crate) fn transaction_open_for(&self, collection_id: CollectionId) -> bool {
        self.active_transaction_snapshot()
            .is_some_and(|open| open.collection_id == collection_id)
    }

    #[allow(dead_code)]
    fn current_log_position(&self) -> Result<LogPosition, StorageRuntimeError> {
        Ok(LogPosition {
            region_index: self.wal_tail,
            offset: u32::try_from(self.wal_append_offset)
                .map_err(|_| StorageRuntimeError::WalRotationRequired)?,
        })
    }

    #[allow(dead_code)]
    fn transaction_range(
        &self,
        start: LogPosition,
    ) -> Result<TransactionLogRange, StorageRuntimeError> {
        Ok(TransactionLogRange {
            start,
            end: self.current_log_position()?,
        })
    }

    fn active_transaction_range(
        &self,
        slot: usize,
    ) -> Result<TransactionLogRange, StorageRuntimeError> {
        match self.transaction_slots.get(slot) {
            Some(TransactionSlot::Active {
                tail_region,
                append_offset,
                start,
                ..
            }) => Ok(TransactionLogRange {
                start: *start,
                end: LogPosition {
                    region_index: *tail_region,
                    offset: u32::try_from(*append_offset)
                        .map_err(|_| StorageRuntimeError::WalRotationRequired)?,
                },
            }),
            _ => Err(StorageRuntimeError::TransactionNotOpen(CollectionId(0))),
        }
    }

    fn active_transaction_regions(
        &self,
        slot: usize,
    ) -> Result<Vec<u32, MAX_RETAINED_TRANSACTION_LOG_REGIONS>, StorageRuntimeError> {
        match self.transaction_slots.get(slot) {
            Some(TransactionSlot::Active { regions, .. }) => Ok(regions.clone()),
            _ => Err(StorageRuntimeError::TransactionNotOpen(CollectionId(0))),
        }
    }

    fn allocate_privileged_region_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<u32, StorageRuntimeError> {
        if self.free_space.next_ready_region().is_err() {
            let dirty_regions = self.free_space.dirty_count();
            if dirty_regions > 0 {
                self.erase_dirty_free_region_span_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    dirty_regions,
                )?;
            }
        }

        for _attempt in 0..self.metadata.region_count {
            let region_index = self
                .free_space
                .next_ready_region()
                .map_err(|_| StorageRuntimeError::NoFreeRegionForRotation)?;
            let allocation_head_after = self.free_space.position_after_allocation()?;
            match self.append_allocate_region_to_main_wal::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                region_index,
                allocation_head_after,
            ) {
                Ok(()) => return Ok(region_index),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }

        Err(StorageRuntimeError::WalRotationRequired)
    }

    fn initialize_transaction_log_region<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
        head_region: u32,
    ) -> Result<usize, StorageRuntimeError> {
        let sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        flash.erase_region(region_index)?;
        let target = workspace.committed_write_buffer();
        let prefix_len = encode_transaction_log_region_prefix_with_cursors(
            target,
            self.metadata,
            sequence,
            head_region,
            self.free_space.allocation_head_position(),
            self.free_space.ready_boundary_position(),
            self.free_space.append_tail_position(),
        )
        .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        flash.write_region(region_index, 0, &target[..prefix_len])?;
        flash.sync()?;
        self.max_seen_sequence = sequence;
        Ok(prefix_len)
    }

    fn append_transaction_private_record_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        self.require_collection_transaction(collection_id)?;
        for _attempt in 0..self.metadata.region_count {
            match self.try_append_transaction_private_record::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                record,
            ) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::TransactionLogFull) => {
                    self.grow_transaction_log::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageRuntimeError::TransactionLogFull)
    }

    fn try_append_transaction_private_record<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        let slot = Self::validate_transaction_log_id(PRIMARY_TRANSACTION_SLOT_ID)?;
        let (tail_region, append_offset, enrollment_written, observed_generation) =
            match self.transaction_slots.get(slot) {
                Some(TransactionSlot::Active {
                    collection_id: active_collection,
                    tail_region,
                    append_offset,
                    enrollment_written,
                    observed_collection_generation,
                    ..
                }) if *active_collection == collection_id => (
                    *tail_region,
                    *append_offset,
                    *enrollment_written,
                    *observed_collection_generation,
                ),
                Some(TransactionSlot::Active {
                    collection_id: active_collection,
                    ..
                }) => {
                    return Err(StorageRuntimeError::TransactionMismatch {
                        expected: *active_collection,
                        actual: collection_id,
                    })
                }
                _ => return Err(StorageRuntimeError::TransactionNotOpen(collection_id)),
            };

        match record {
            WalRecord::AddTransactionCollection {
                collection_id: enrolled_collection,
                observed_collection_generation,
            } => {
                if enrolled_collection != collection_id {
                    return Err(StorageRuntimeError::TransactionMismatch {
                        expected: collection_id,
                        actual: enrolled_collection,
                    });
                }
                if enrollment_written {
                    return Err(StorageRuntimeError::TransactionAlreadyOpen(collection_id));
                }
                if observed_collection_generation != observed_generation {
                    return Err(StorageRuntimeError::TransactionConflict {
                        collection_id,
                        observed_generation,
                        current_generation: observed_collection_generation,
                    });
                }
            }
            WalRecord::Link { .. } => {}
            WalRecord::FreeRegion { .. } => {
                return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
            }
            other => {
                if !enrollment_written {
                    return Err(StorageRuntimeError::TransactionNotOpen(collection_id));
                }
                if let Some(record_collection_id) =
                    Self::transaction_private_record_collection_id(other)
                {
                    if record_collection_id != collection_id {
                        return Err(StorageRuntimeError::TransactionMismatch {
                            expected: collection_id,
                            actual: record_collection_id,
                        });
                    }
                }
            }
        }

        let (physical, logical) = workspace.encode_buffers();
        let encoded_len = encode_record_into(record, self.metadata, physical, logical)?;
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        let end = append_offset
            .checked_add(encoded_len)
            .ok_or(StorageRuntimeError::TransactionLogFull)?;
        if end > append_limit {
            return Err(StorageRuntimeError::TransactionLogFull);
        }

        if !matches!(record, WalRecord::Link { .. }) {
            let expected_sequence = self
                .max_seen_sequence
                .checked_add(1)
                .ok_or(StorageRuntimeError::WalRotationRequired)?;
            let link_len = encode_record_into(
                WalRecord::Link {
                    next_region_index: 0,
                    expected_sequence,
                },
                self.metadata,
                physical,
                logical,
            )?;
            let remaining_after = append_limit
                .checked_sub(end)
                .ok_or(StorageRuntimeError::TransactionLogFull)?;
            if remaining_after < link_len {
                return Err(StorageRuntimeError::TransactionLogFull);
            }
            let encoded_again = encode_record_into(record, self.metadata, physical, logical)?;
            if encoded_again != encoded_len {
                return Err(StorageRuntimeError::WalRotationRequired);
            }
        }

        flash.write_region(tail_region, append_offset, &physical[..encoded_len])?;
        flash.sync()?;

        match self.transaction_slots.get_mut(slot) {
            Some(TransactionSlot::Active {
                collection_id: active_collection,
                append_offset,
                enrollment_written,
                allocated_regions,
                free_intents,
                collection_effects,
                ..
            }) if *active_collection == collection_id => {
                *append_offset = end;
                match record {
                    WalRecord::AddTransactionCollection { .. } => {
                        *enrollment_written = true;
                    }
                    WalRecord::AllocateRegion {
                        region_index,
                        allocation_head_after,
                    } => {
                        if !allocated_regions
                            .iter()
                            .any(|allocation| allocation.region_index == region_index)
                        {
                            allocated_regions
                                .push(TransactionAllocation {
                                    region_index,
                                    allocation_head_after,
                                })
                                .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
                        }
                    }
                    WalRecord::FreeIntent { region_index, .. } => {
                        if !free_intents.contains(&region_index) {
                            free_intents
                                .push(region_index)
                                .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
                        }
                    }
                    _ => {}
                }
                if let Some(effect) = Self::transaction_collection_effect(record) {
                    collection_effects
                        .push(effect)
                        .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
                }
            }
            _ => return Err(StorageRuntimeError::TransactionNotOpen(collection_id)),
        }

        if matches!(
            record,
            WalRecord::AllocateRegion { .. } | WalRecord::EraseFreeRegionSpan { .. }
        ) {
            apply_wal_record(
                self.metadata,
                record,
                &mut self.collections,
                &mut self.free_space,
                &mut self.ready_region,
            )?;
        }
        Ok(())
    }

    fn grow_transaction_log<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        let slot = Self::validate_transaction_log_id(PRIMARY_TRANSACTION_SLOT_ID)?;
        let (head_region, tail_region, append_offset) = match self.transaction_slots.get(slot) {
            Some(TransactionSlot::Active {
                head_region,
                tail_region,
                append_offset,
                ..
            }) => (*head_region, *tail_region, *append_offset),
            _ => return Err(StorageRuntimeError::TransactionNotOpen(CollectionId(0))),
        };
        let new_region = self
            .allocate_privileged_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace,
            )?;
        let expected_sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;

        let (physical, logical) = workspace.encode_buffers();
        let link_len = encode_record_into(
            WalRecord::Link {
                next_region_index: new_region,
                expected_sequence,
            },
            self.metadata,
            physical,
            logical,
        )?;
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        if append_offset
            .checked_add(link_len)
            .is_none_or(|end| end > append_limit)
        {
            return Err(StorageRuntimeError::TransactionLogFull);
        }
        flash.write_region(tail_region, append_offset, &physical[..link_len])?;
        flash.sync()?;

        let new_offset = self.initialize_transaction_log_region::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            new_region,
            head_region,
        )?;
        match self.transaction_slots.get_mut(slot) {
            Some(TransactionSlot::Active {
                tail_region,
                append_offset,
                regions,
                ..
            }) => {
                *tail_region = new_region;
                *append_offset = new_offset;
                regions
                    .push(new_region)
                    .map_err(|_| StorageRuntimeError::TransactionLogFull)?;
            }
            _ => return Err(StorageRuntimeError::TransactionNotOpen(CollectionId(0))),
        }
        Ok(())
    }

    fn apply_committed_transaction_effects(
        &mut self,
        effects: &[TransactionCollectionEffect],
    ) -> Result<(), StorageRuntimeError> {
        for effect in effects.iter().copied() {
            let record = match effect {
                TransactionCollectionEffect::NewCollection {
                    collection_id,
                    collection_type,
                } => WalRecord::NewCollection {
                    collection_id,
                    collection_type,
                },
                TransactionCollectionEffect::Update { collection_id } => WalRecord::Update {
                    collection_id,
                    payload: &[],
                },
                TransactionCollectionEffect::Snapshot {
                    collection_id,
                    collection_type,
                } => WalRecord::Snapshot {
                    collection_id,
                    collection_type,
                    payload: &[],
                },
                TransactionCollectionEffect::Head {
                    collection_id,
                    collection_type,
                    region_index,
                } => WalRecord::Head {
                    collection_id,
                    collection_type,
                    region_index,
                },
                TransactionCollectionEffect::DropCollection { collection_id } => {
                    WalRecord::DropCollection { collection_id }
                }
            };
            apply_wal_record(
                self.metadata,
                record,
                &mut self.collections,
                &mut self.free_space,
                &mut self.ready_region,
            )
            .map_err(StorageRuntimeError::Startup)?;
        }
        Ok(())
    }

    /// Appends the transaction commit marker.
    pub(crate) fn commit_collection_transaction<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        let Some(open) = self.active_transaction_snapshot() else {
            return Err(StorageRuntimeError::TransactionNotOpen(collection_id));
        };
        if open.collection_id != collection_id {
            return Err(StorageRuntimeError::TransactionMismatch {
                expected: open.collection_id,
                actual: collection_id,
            });
        }
        let slot = Self::validate_transaction_log_id(PRIMARY_TRANSACTION_SLOT_ID)?;
        let observed_generation = match self.transaction_slots.get(slot) {
            Some(TransactionSlot::Active {
                observed_collection_generation,
                enrollment_written,
                ..
            }) if *enrollment_written => *observed_collection_generation,
            Some(TransactionSlot::Active { .. }) => {
                return Err(StorageRuntimeError::TransactionNotOpen(collection_id))
            }
            Some(TransactionSlot::Empty) | None => {
                return Err(StorageRuntimeError::TransactionNotOpen(collection_id))
            }
        };
        let current_generation =
            self.current_committed_generation_for_transaction(collection_id)?;
        if current_generation != observed_generation {
            return Err(StorageRuntimeError::TransactionConflict {
                collection_id,
                observed_generation,
                current_generation,
            });
        }
        let range = self.active_transaction_range(slot)?;
        let regions = self.active_transaction_regions(slot)?;
        let (free_intents, collection_effects) = match self.transaction_slots.get(slot) {
            Some(TransactionSlot::Active {
                free_intents,
                collection_effects,
                ..
            }) => (free_intents.clone(), collection_effects.clone()),
            _ => return Err(StorageRuntimeError::TransactionNotOpen(collection_id)),
        };
        if !free_intents.is_empty() {
            self.ensure_free_space_metadata_capacity_for_len::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.free_space
                    .entries()
                    .len()
                    .saturating_add(free_intents.len()),
            )?;
        }
        let commit_record = WalRecord::CommitTransaction {
            transaction_log_id: PRIMARY_TRANSACTION_SLOT_ID,
            range,
        };
        let finish_record = WalRecord::TransactionFinished {
            transaction_log_id: PRIMARY_TRANSACTION_SLOT_ID,
            range,
        };
        self.ensure_transaction_terminal_batch_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            commit_record,
            free_intents.len(),
            finish_record,
        )?;
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            commit_record,
        )?;
        self.apply_committed_transaction_effects(collection_effects.as_slice())?;
        self.retain_transaction_log(
            PRIMARY_TRANSACTION_SLOT_ID,
            range,
            regions.as_slice(),
            TransactionLogOutcome::Committed,
        )?;
        Ok(())
    }

    /// Appends the transaction finished marker.
    pub(crate) fn finish_collection_transaction<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        let Some(open) = self.active_transaction_snapshot() else {
            return Err(StorageRuntimeError::TransactionNotOpen(collection_id));
        };
        if open.collection_id != collection_id {
            return Err(StorageRuntimeError::TransactionMismatch {
                expected: open.collection_id,
                actual: collection_id,
            });
        }
        if !self.active_transaction_has_committed_outcome(open.start) {
            return Err(StorageRuntimeError::TransactionNotCommitted(collection_id));
        }
        let slot = Self::validate_transaction_log_id(PRIMARY_TRANSACTION_SLOT_ID)?;
        let range = self.active_transaction_range(slot)?;
        let regions = self.active_transaction_regions(slot)?;
        let free_intents = match self.transaction_slots.get(slot) {
            Some(TransactionSlot::Active { free_intents, .. }) => free_intents.clone(),
            _ => return Err(StorageRuntimeError::TransactionNotOpen(collection_id)),
        };
        for region_index in free_intents.iter().copied() {
            self.ensure_free_space_metadata_capacity_for_len::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.free_space.entries().len().saturating_add(1),
            )?;
            let append_tail_after = self.free_space.position_after_append()?;
            self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                WalRecord::FreeRegion {
                    region_index,
                    append_tail_after,
                },
            )?;
        }
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::TransactionFinished {
                transaction_log_id: PRIMARY_TRANSACTION_SLOT_ID,
                range,
            },
        )?;
        self.retain_transaction_log(
            PRIMARY_TRANSACTION_SLOT_ID,
            range,
            regions.as_slice(),
            TransactionLogOutcome::Finished,
        )?;
        self.transaction_slots[slot] = TransactionSlot::Empty;
        self.clear_transaction_runtime_snapshot();
        Ok(())
    }

    /// Appends the transaction rollback marker and closes the open transaction.
    pub(crate) fn rollback_collection_transaction<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        let Some(open) = self.active_transaction_snapshot() else {
            return Err(StorageRuntimeError::TransactionNotOpen(collection_id));
        };
        if open.collection_id != collection_id {
            return Err(StorageRuntimeError::TransactionMismatch {
                expected: open.collection_id,
                actual: collection_id,
            });
        }
        let slot = Self::validate_transaction_log_id(PRIMARY_TRANSACTION_SLOT_ID)?;
        let active_slot = self
            .transaction_slots
            .get(slot)
            .cloned()
            .ok_or(StorageRuntimeError::TransactionNotOpen(collection_id))?;
        let (range, regions, allocations) = match active_slot {
            TransactionSlot::Active {
                start: _,
                regions,
                allocated_regions,
                ..
            } => (
                self.active_transaction_range(slot)?,
                regions,
                allocated_regions,
            ),
            TransactionSlot::Empty => {
                return Err(StorageRuntimeError::TransactionNotOpen(collection_id))
            }
        };
        if !allocations.is_empty() {
            self.ensure_free_space_metadata_capacity_for_len::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.free_space
                    .entries()
                    .len()
                    .saturating_add(allocations.len()),
            )?;
        }
        let rollback_record = WalRecord::RollbackTransaction {
            transaction_log_id: PRIMARY_TRANSACTION_SLOT_ID,
            range,
        };
        let finish_record = WalRecord::TransactionFinished {
            transaction_log_id: PRIMARY_TRANSACTION_SLOT_ID,
            range,
        };
        self.ensure_transaction_terminal_batch_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            rollback_record,
            allocations.len(),
            finish_record,
        )?;
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            rollback_record,
        )?;
        self.retain_transaction_log(
            PRIMARY_TRANSACTION_SLOT_ID,
            range,
            regions.as_slice(),
            TransactionLogOutcome::RolledBack,
        )?;
        self.restore_transaction_runtime_snapshot()?;
        for allocation in allocations.iter().copied() {
            self.free_space
                .apply_allocate(allocation.region_index, allocation.allocation_head_after)?;
        }
        for allocation in allocations.iter().copied() {
            self.ensure_free_space_metadata_capacity_for_len::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                self.free_space.entries().len().saturating_add(1),
            )?;
            let append_tail_after = self.free_space.position_after_append()?;
            self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                WalRecord::FreeRegion {
                    region_index: allocation.region_index,
                    append_tail_after,
                },
            )?;
        }
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::TransactionFinished {
                transaction_log_id: PRIMARY_TRANSACTION_SLOT_ID,
                range,
            },
        )?;
        self.retain_transaction_log(
            PRIMARY_TRANSACTION_SLOT_ID,
            range,
            regions.as_slice(),
            TransactionLogOutcome::Finished,
        )?;
        self.transaction_slots[slot] = TransactionSlot::Empty;
        self.clear_transaction_runtime_snapshot();
        Ok(())
    }

    /// Begins a WAL tail rotation and returns the reserved next tail region.
    pub fn append_wal_rotation_start<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<u32, StorageRuntimeError> {
        self.append_wal_rotation_start_internal::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, false,
        )
    }

    fn append_wal_rotation_start_internal<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        allow_early_rotation: bool,
    ) -> Result<u32, StorageRuntimeError> {
        if self.ready_region.is_some() {
            return Err(StorageRuntimeError::InvalidRotationState {
                ready_region: self.ready_region,
                requested_region: None,
            });
        }

        if self.free_space.next_ready_region().is_err() {
            let dirty_regions = self.free_space.dirty_count();
            if dirty_regions > 0 {
                self.erase_dirty_free_region_span_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    dirty_regions,
                )?;
            }
        }

        let next_region_index = self
            .free_space
            .next_ready_region()
            .map_err(|_| StorageRuntimeError::NoFreeRegionForRotation)?;
        let allocation_head_after = self.free_space.position_after_allocation()?;

        let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            allocation_head_after,
        )?;
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        let remaining_after = append_limit
            .checked_sub(
                self.wal_append_offset
                    .checked_add(reserves.allocate_region_len)
                    .ok_or(StorageRuntimeError::WalRotationRequired)?,
            )
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        if remaining_after < reserves.link_reserve
            || (!allow_early_rotation && remaining_after >= reserves.rotation_reserve)
        {
            return Err(StorageRuntimeError::InvalidRotationWindow {
                remaining_after,
                link_reserve: reserves.link_reserve,
                rotation_reserve: reserves.rotation_reserve,
            });
        }

        self.write_record_raw::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::AllocateRegion {
                region_index: next_region_index,
                allocation_head_after,
            },
        )?;
        self.wal_append_offset = self
            .wal_append_offset
            .checked_add(reserves.allocate_region_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        self.free_space
            .apply_allocate(next_region_index, allocation_head_after)?;
        self.materialize_free_space_collection::<REGION_SIZE, IO>(flash)?;
        self.ready_region = Some(next_region_index);
        self.pending_wal_recovery_boundary = false;
        Ok(next_region_index)
    }

    /// Finishes a WAL tail rotation previously started by `append_wal_rotation_start`.
    pub fn append_wal_rotation_finish<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        next_region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        if self.ready_region != Some(next_region_index) {
            return Err(StorageRuntimeError::InvalidRotationState {
                ready_region: self.ready_region,
                requested_region: Some(next_region_index),
            });
        }

        let expected_sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        self.write_record_raw::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Link {
                next_region_index,
                expected_sequence,
            },
        )?;
        initialize_wal_region::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.metadata,
            next_region_index,
            expected_sequence,
            self.wal_head,
            self.free_space.allocation_head_position(),
            self.free_space.ready_boundary_position(),
            self.free_space.append_tail_position(),
        )?;

        self.wal_tail = next_region_index;
        self.wal_append_offset = self
            .metadata
            .wal_record_area_offset()
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        self.ready_region = None;
        self.max_seen_sequence = expected_sequence;
        self.pending_wal_recovery_boundary = false;
        Ok(())
    }

    fn find_collection(&self, collection_id: CollectionId) -> Option<&StartupCollection> {
        self.collections
            .iter()
            .find(|collection| collection.collection_id() == collection_id)
    }

    fn collection_generation_in(
        collections: &[StartupCollection],
        collection_id: CollectionId,
    ) -> Result<u64, StorageRuntimeError> {
        collections
            .iter()
            .find(|collection| collection.collection_id() == collection_id)
            .map(StartupCollection::committed_generation)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))
    }

    fn current_committed_generation_for_transaction(
        &self,
        collection_id: CollectionId,
    ) -> Result<u64, StorageRuntimeError> {
        if self.transaction_original_collections.is_empty() {
            return self
                .find_collection(collection_id)
                .map(StartupCollection::committed_generation)
                .ok_or(StorageRuntimeError::UnknownCollection(collection_id));
        }
        Self::collection_generation_in(&self.transaction_original_collections, collection_id)
    }

    fn inline_transaction_total_len<const REGION_SIZE: usize>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        records: &[WalRecord<'_>],
    ) -> Result<(u32, u32, usize), StorageRuntimeError> {
        let record_count =
            u32::try_from(records.len()).map_err(|_| StorageRuntimeError::TransactionLogFull)?;
        let mut body_len = 0usize;
        for record in records.iter().copied() {
            match record {
                WalRecord::BeginInlineTransaction { .. }
                | WalRecord::CommitInlineTransaction { .. }
                | WalRecord::RollbackInlineTransaction { .. }
                | WalRecord::BeginTransaction { .. }
                | WalRecord::CommitTransaction { .. }
                | WalRecord::TransactionFinished { .. }
                | WalRecord::RollbackTransaction { .. }
                | WalRecord::Link { .. }
                | WalRecord::WalRecovery => {
                    return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
                }
                _ => {}
            }
            let (physical, logical) = workspace.encode_buffers();
            body_len = body_len
                .checked_add(encoded_record_len(
                    record,
                    self.metadata,
                    physical,
                    logical,
                )?)
                .ok_or(StorageRuntimeError::WalRotationRequired)?;
        }
        let body_len_u32 =
            u32::try_from(body_len).map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let begin = WalRecord::BeginInlineTransaction {
            record_count,
            encoded_len: body_len_u32,
        };
        let commit = WalRecord::CommitInlineTransaction { record_count };
        let (physical, logical) = workspace.encode_buffers();
        let begin_len = encoded_record_len(begin, self.metadata, physical, logical)?;
        let (physical, logical) = workspace.encode_buffers();
        let commit_len = encoded_record_len(commit, self.metadata, physical, logical)?;
        let total_len = begin_len
            .checked_add(body_len)
            .and_then(|len| len.checked_add(commit_len))
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        Ok((record_count, body_len_u32, total_len))
    }

    fn ensure_inline_transaction_append_room<const REGION_SIZE: usize>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        records: &[WalRecord<'_>],
    ) -> Result<(u32, u32), StorageRuntimeError> {
        let (record_count, body_len, total_len) =
            self.inline_transaction_total_len(workspace, records)?;
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        if self
            .wal_append_offset
            .checked_add(total_len)
            .is_some_and(|end| end <= append_limit)
        {
            return Ok((record_count, body_len));
        }
        Err(StorageRuntimeError::WalRotationRequired)
    }

    fn write_inline_record_raw_advance<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        let encoded_len =
            self.write_record_raw::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record)?;
        self.wal_append_offset = self
            .wal_append_offset
            .checked_add(encoded_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        Ok(())
    }

    fn apply_inline_body_record_and_refresh_runtime<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        apply_wal_record(
            self.metadata,
            record,
            &mut self.collections,
            &mut self.free_space,
            &mut self.ready_region,
        )?;
        match record {
            WalRecord::Head {
                collection_id: CollectionId(0),
                collection_type,
                region_index,
            } => {
                if collection_type == CollectionType::WAL_CODE {
                    self.wal_head = region_index;
                }
            }
            WalRecord::Head {
                collection_id,
                region_index,
                ..
            } if collection_id != CollectionId(0) => {
                let header =
                    read_header_from_flash::<REGION_SIZE, REGION_COUNT, IO>(flash, region_index)
                        .map_err(|_| StorageRuntimeError::InvalidHeadTarget {
                            collection_id,
                            region_index,
                        })?;
                self.max_seen_sequence = self.max_seen_sequence.max(header.sequence);
            }
            WalRecord::FreeRegion { .. }
            | WalRecord::AllocateRegion { .. }
            | WalRecord::EraseFreeRegionSpan { .. } => {
                self.materialize_free_space_collection::<REGION_SIZE, IO>(flash)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn append_inline_transaction_once<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        records: &[WalRecord<'_>],
    ) -> Result<(), StorageRuntimeError> {
        if let Some(open) = self.active_transaction_snapshot() {
            return Err(StorageRuntimeError::TransactionAlreadyOpen(
                open.collection_id,
            ));
        }
        let (record_count, body_len) =
            self.ensure_inline_transaction_append_room(workspace, records)?;
        self.write_inline_record_raw_advance::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::BeginInlineTransaction {
                record_count,
                encoded_len: body_len,
            },
        )?;
        for record in records.iter().copied() {
            self.write_inline_record_raw_advance::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, record,
            )?;
        }
        self.write_inline_record_raw_advance::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::CommitInlineTransaction { record_count },
        )?;
        for record in records.iter().copied() {
            self.apply_inline_body_record_and_refresh_runtime::<REGION_SIZE, REGION_COUNT, IO>(
                flash, record,
            )?;
        }
        Ok(())
    }

    fn append_inline_transaction_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        records: &[WalRecord<'_>],
    ) -> Result<(), StorageRuntimeError> {
        for _attempt in 0..self.metadata.region_count {
            match self.append_inline_transaction_once::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, records,
            ) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageRuntimeError::WalRotationRequired)
    }

    fn append_internal_atomic_records_for_collection_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        records: &[WalRecord<'_>],
    ) -> Result<(), StorageRuntimeError> {
        if self.transaction_open_for(collection_id) {
            for record in records.iter().copied() {
                self.append_transaction_private_record_with_rotation::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                >(flash, workspace, collection_id, record)?;
            }
            return Ok(());
        }
        self.append_inline_transaction_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, records,
        )
    }

    fn append_record<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        self.ensure_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(workspace, flash, record)?;
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record)
    }

    fn append_record_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        for _attempt in 0..self.metadata.region_count {
            match self.append_record::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageRuntimeError::WalRotationRequired)
    }

    #[cfg(test)]
    pub(crate) fn append_raw_record_for_test<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record)
    }

    fn ensure_record_append_room_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        for _attempt in 0..self.metadata.region_count {
            match self
                .ensure_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(workspace, flash, record)
            {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageRuntimeError::WalRotationRequired)
    }

    fn transaction_terminal_batch_len<const REGION_SIZE: usize>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        control_record: WalRecord<'_>,
        cleanup_count: usize,
        finish_record: WalRecord<'_>,
    ) -> Result<usize, StorageRuntimeError> {
        let (physical, logical) = workspace.encode_buffers();
        let control_len = encode_record_into(control_record, self.metadata, physical, logical)?;
        let cleanup_len = if cleanup_count == 0 {
            0
        } else {
            encode_record_into(
                WalRecord::FreeRegion {
                    region_index: 0,
                    append_tail_after: self.free_space.position_after_append()?,
                },
                self.metadata,
                physical,
                logical,
            )?
        };
        let finish_len = encode_record_into(finish_record, self.metadata, physical, logical)?;
        control_len
            .checked_add(
                cleanup_len
                    .checked_mul(cleanup_count)
                    .ok_or(StorageRuntimeError::WalRotationRequired)?,
            )
            .and_then(|len| len.checked_add(finish_len))
            .ok_or(StorageRuntimeError::WalRotationRequired)
    }

    fn ensure_transaction_terminal_batch_room_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        control_record: WalRecord<'_>,
        cleanup_count: usize,
        finish_record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        for _attempt in 0..self.metadata.region_count {
            let encoded_len = self.transaction_terminal_batch_len::<REGION_SIZE>(
                workspace,
                control_record,
                cleanup_count,
                finish_record,
            )?;
            match self.ensure_encoded_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
                workspace,
                flash,
                encoded_len,
                false,
                false,
            ) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageRuntimeError::WalRotationRequired)
    }

    #[cfg(feature = "perf-counters")]
    fn append_record_with_rotation_metered<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
        metrics: &mut StoragePerfMetrics,
    ) -> Result<(), StorageRuntimeError> {
        for _attempt in 0..self.metadata.region_count {
            match self.append_record_metered::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, record, metrics,
            ) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    metrics.increment(StoragePerfCounter::WalRotationRequired);
                    metrics.increment(StoragePerfCounter::WalRotationsAttempted);
                    self.observe_wal_rotation_window::<REGION_SIZE, REGION_COUNT>(
                        workspace, metrics,
                    );
                    let rotation_timer = StoragePerfTimerGuard::start();
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                    metrics.add_nanos(
                        StoragePerfTimer::WalRotation,
                        rotation_timer.elapsed_nanos(),
                    );
                    metrics.increment(StoragePerfCounter::WalRotationsCompleted);
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageRuntimeError::WalRotationRequired)
    }

    #[cfg(feature = "perf-counters")]
    fn observe_wal_rotation_window<const REGION_SIZE: usize, const REGION_COUNT: usize>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        metrics: &mut StoragePerfMetrics,
    ) {
        let remaining_bytes = REGION_SIZE.saturating_sub(self.wal_append_offset) as u64;
        let Ok(next_region_index) = self.free_space.next_ready_region() else {
            metrics.observe_wal_rotation_window(remaining_bytes, 0, 0, 0);
            return;
        };
        let Ok(allocation_head_after) = self.free_space.position_after_allocation() else {
            metrics.observe_wal_rotation_window(remaining_bytes, 0, 0, 0);
            return;
        };
        let Ok(reserves) = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            allocation_head_after,
        ) else {
            metrics.observe_wal_rotation_window(remaining_bytes, 0, 0, 0);
            return;
        };
        metrics.observe_wal_rotation_window(
            remaining_bytes,
            reserves.allocate_region_len as u64,
            reserves.link_reserve as u64,
            reserves.rotation_reserve as u64,
        );
    }

    #[cfg(feature = "perf-counters")]
    fn append_record_metered<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
        metrics: &mut StoragePerfMetrics,
    ) -> Result<(), StorageRuntimeError> {
        match self.ensure_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(workspace, flash, record)
        {
            Ok(()) => {}
            Err(StorageRuntimeError::WalRotationRequired) => {
                return Err(StorageRuntimeError::WalRotationRequired);
            }
            Err(error) => return Err(error),
        }
        self.write_record_and_apply_metered::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, record, metrics,
        )
    }

    #[cfg(feature = "perf-counters")]
    fn write_record_and_apply_metered<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
        metrics: &mut StoragePerfMetrics,
    ) -> Result<(), StorageRuntimeError> {
        let encoded_len = self.write_record_raw_metered::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, record, metrics,
        )?;
        self.apply_synced_record_and_refresh_runtime::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            record,
            encoded_len,
        )?;
        Ok(())
    }

    fn write_record_and_apply<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        let encoded_len =
            self.write_record_raw::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record)?;
        self.apply_synced_record_and_refresh_runtime::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            record,
            encoded_len,
        )?;
        Ok(())
    }

    fn apply_synced_record_and_refresh_runtime<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        record: WalRecord<'_>,
        encoded_len: usize,
    ) -> Result<(), StorageRuntimeError> {
        self.apply_synced_record(record, encoded_len)?;
        match record {
            WalRecord::Head {
                collection_id: CollectionId(0),
                collection_type,
                region_index,
            } => {
                if collection_type == CollectionType::WAL_CODE {
                    self.wal_head = region_index;
                }
            }
            WalRecord::FreeRegion { .. }
            | WalRecord::AllocateRegion { .. }
            | WalRecord::EraseFreeRegionSpan { .. } => {
                self.materialize_free_space_collection::<REGION_SIZE, IO>(flash)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_synced_record(
        &mut self,
        record: WalRecord<'_>,
        encoded_len: usize,
    ) -> Result<(), StorageRuntimeError> {
        apply_wal_record(
            self.metadata,
            record,
            &mut self.collections,
            &mut self.free_space,
            &mut self.ready_region,
        )?;
        self.wal_append_offset = self
            .wal_append_offset
            .checked_add(encoded_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        if matches!(record, WalRecord::WalRecovery) {
            self.pending_wal_recovery_boundary = false;
        }
        Ok(())
    }

    fn free_space_entries_per_metadata_region<const REGION_SIZE: usize>(
    ) -> Result<usize, StorageRuntimeError> {
        let entries_offset = Header::ENCODED_LEN
            .checked_add(FreeSpaceRegionPrologue::ENCODED_LEN)
            .ok_or(StorageRuntimeError::InvalidFreeSpaceCommand)?;
        let entries_per_region = REGION_SIZE
            .checked_sub(entries_offset)
            .ok_or(StorageRuntimeError::InvalidFreeSpaceCommand)?
            / FreeSpaceEntry::ENCODED_LEN;
        if entries_per_region == 0 {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }
        Ok(entries_per_region)
    }

    fn required_free_space_metadata_regions(
        queue_len: usize,
        entries_per_region: usize,
    ) -> Result<usize, StorageRuntimeError> {
        if entries_per_region == 0 {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }
        Ok(queue_len.saturating_add(entries_per_region - 1) / entries_per_region)
    }

    fn ensure_free_space_metadata_capacity_for_len<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        queue_len: usize,
    ) -> Result<(), StorageRuntimeError> {
        let entries_per_region = Self::free_space_entries_per_metadata_region::<REGION_SIZE>()?;
        let required_regions =
            Self::required_free_space_metadata_regions(queue_len, entries_per_region)?;
        let entries_per_region_u32 = u32::try_from(entries_per_region)
            .map_err(|_| StorageRuntimeError::InvalidFreeSpaceCommand)?;

        while self.free_space.metadata_region_count() < required_regions {
            let region_index = self
                .allocate_privileged_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash, workspace,
                )?;
            self.free_space
                .push_metadata_region(region_index, entries_per_region_u32)?;
            self.materialize_free_space_collection::<REGION_SIZE, IO>(flash)?;
        }

        Ok(())
    }

    fn materialize_free_space_collection<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
    ) -> Result<(), StorageRuntimeError> {
        let first_region_index = self.free_space.metadata_region_index();
        if first_region_index == 0 {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }
        let entries_per_region = Self::free_space_entries_per_metadata_region::<REGION_SIZE>()?;
        let required_regions = self
            .free_space
            .entries()
            .len()
            .saturating_add(entries_per_region - 1)
            / entries_per_region;
        let chain_regions =
            self.free_space_metadata_chain_regions::<REGION_SIZE, IO>(flash, required_regions)?;
        if required_regions > chain_regions.len() {
            return Err(StorageRuntimeError::InsufficientFreeSpaceMetadataCapacity {
                required_regions,
                available_regions: chain_regions.len(),
            });
        }

        let mut region = [self.metadata.erased_byte; REGION_SIZE];
        for (index, region_index) in chain_regions.iter().copied().enumerate() {
            let start = index
                .checked_mul(entries_per_region)
                .ok_or(StorageRuntimeError::InvalidFreeSpaceCommand)?;
            let end = start
                .saturating_add(entries_per_region)
                .min(self.free_space.entries().len());
            let entries = &self.free_space.entries()[start..end];
            let next_metadata_region = if index + 1 < required_regions {
                chain_regions.get(index + 1).copied()
            } else {
                None
            };
            let len = encode_free_space_region_segment(
                &mut region,
                self.metadata,
                u64::try_from(index).map_err(|_| StorageRuntimeError::InvalidFreeSpaceCommand)?,
                region_index,
                FreeSpaceCursors::new(
                    self.free_space.allocation_head_position(),
                    self.free_space.ready_boundary_position(),
                    self.free_space.append_tail_position(),
                ),
                next_metadata_region,
                entries,
            )
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
            flash.erase_region(region_index)?;
            flash.write_region(region_index, 0, &region[..len])?;
        }
        flash.sync()?;
        Ok(())
    }

    fn free_space_metadata_chain_regions<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        needed: usize,
    ) -> Result<Vec<u32, { crate::free_space::MAX_FREE_QUEUE_ENTRIES }>, StorageRuntimeError> {
        let mut regions = Vec::<u32, { crate::free_space::MAX_FREE_QUEUE_ENTRIES }>::new();
        if self.free_space.metadata_region_count() >= needed {
            for region_index in self
                .free_space
                .metadata_regions()
                .iter()
                .copied()
                .take(needed.max(1))
            {
                regions
                    .push(region_index)
                    .map_err(|_| StorageRuntimeError::InvalidFreeSpaceCommand)?;
            }
            return Ok(regions);
        }

        let mut current = self.free_space.metadata_region_index();
        for _ in 0..self.metadata.region_count {
            regions
                .push(current)
                .map_err(|_| StorageRuntimeError::InvalidFreeSpaceCommand)?;
            if regions.len() >= needed {
                return Ok(regions);
            }

            let mut region = [self.metadata.erased_byte; REGION_SIZE];
            flash.read_region(current, 0, REGION_SIZE, |bytes| {
                region.copy_from_slice(bytes);
            })?;
            let prologue_start = Header::ENCODED_LEN;
            let prologue_end = prologue_start
                .checked_add(FreeSpaceRegionPrologue::ENCODED_LEN)
                .ok_or(StorageRuntimeError::InvalidFreeSpaceCommand)?;
            let prologue = FreeSpaceRegionPrologue::decode(
                &region[prologue_start..prologue_end],
                self.metadata.region_count,
            )
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
            let Some(next) = prologue.next_metadata_region else {
                return Ok(regions);
            };
            current = next;
        }
        Err(StorageRuntimeError::InvalidFreeSpaceCommand)
    }

    fn write_record_raw<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<usize, StorageRuntimeError> {
        let (physical, logical) = workspace.encode_buffers();
        let encoded_len = encode_record_into(record, self.metadata, physical, logical)?;
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        if self
            .wal_append_offset
            .checked_add(encoded_len)
            .is_none_or(|end| end > append_limit)
        {
            return Err(StorageRuntimeError::WalRotationRequired);
        }

        flash.write_region(
            self.wal_tail,
            self.wal_append_offset,
            &physical[..encoded_len],
        )?;
        flash.sync()?;
        Ok(encoded_len)
    }

    #[cfg(feature = "perf-counters")]
    fn write_record_raw_metered<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
        metrics: &mut StoragePerfMetrics,
    ) -> Result<usize, StorageRuntimeError> {
        let (physical, logical) = workspace.encode_buffers();
        let encode_timer = StoragePerfTimerGuard::start();
        let encoded = encode_record_into(record, self.metadata, physical, logical);
        metrics.add_nanos(StoragePerfTimer::WalEncode, encode_timer.elapsed_nanos());
        let encoded_len = match encoded {
            Ok(encoded_len) => encoded_len,
            Err(error) => {
                metrics.increment(StoragePerfCounter::AppendFailures);
                return Err(error.into());
            }
        };
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        if self
            .wal_append_offset
            .checked_add(encoded_len)
            .is_none_or(|end| end > append_limit)
        {
            metrics.increment(StoragePerfCounter::WalRotationRequired);
            return Err(StorageRuntimeError::WalRotationRequired);
        }

        let write_timer = StoragePerfTimerGuard::start();
        if let Err(error) = flash.write_region(
            self.wal_tail,
            self.wal_append_offset,
            &physical[..encoded_len],
        ) {
            metrics.add_nanos(StoragePerfTimer::WalWrite, write_timer.elapsed_nanos());
            metrics.increment(StoragePerfCounter::AppendFailures);
            return Err(error.into());
        }
        metrics.add_nanos(StoragePerfTimer::WalWrite, write_timer.elapsed_nanos());

        let sync_timer = StoragePerfTimerGuard::start();
        let sync_result = flash.sync();
        metrics.add_nanos(StoragePerfTimer::WalSync, sync_timer.elapsed_nanos());
        metrics.increment(StoragePerfCounter::WalSyncs);
        if let Err(error) = sync_result {
            metrics.increment(StoragePerfCounter::AppendFailures);
            return Err(error.into());
        }

        metrics.increment(StoragePerfCounter::WalRecords);
        metrics.add(StoragePerfCounter::WalBytes, encoded_len as u64);
        Ok(encoded_len)
    }

    fn copy_encoded_record_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        source_region: u32,
        source_offset: usize,
        encoded_len: usize,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        #[cfg(feature = "perf-counters")] _metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<(), StorageRuntimeError> {
        for _attempt in 0..self.metadata.region_count {
            match self.ensure_encoded_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
                workspace,
                flash,
                encoded_len,
                false,
                false,
            ) {
                Ok(()) => {
                    {
                        let (physical, _) = workspace.encode_buffers();
                        flash.read_region(source_region, source_offset, encoded_len, |bytes| {
                            physical[..encoded_len].copy_from_slice(bytes);
                        })?;
                        flash.write_region(
                            self.wal_tail,
                            self.wal_append_offset,
                            &physical[..encoded_len],
                        )?;
                        flash.sync()?;
                    }
                    let _ = open_plan;
                    self.wal_append_offset = self
                        .wal_append_offset
                        .checked_add(encoded_len)
                        .ok_or(StorageRuntimeError::WalRotationRequired)?;
                    return Ok(());
                }
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail_with_progress::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageRuntimeError::WalRotationRequired)
    }

    fn append_empty_basis_snapshot_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
    ) -> Result<(), StorageRuntimeError> {
        let payload = match collection_type {
            crate::CollectionType::MAP_CODE => crate::EMPTY_MAP_SNAPSHOT.as_slice(),
            crate::CollectionType::OBJECT_LOG_CODE => {
                crate::collections::object_log::empty_snapshot()
            }
            other => {
                return Err(StorageRuntimeError::WalHeadReclaimUnsupportedCollectionType(other))
            }
        };

        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Snapshot {
                collection_id,
                collection_type,
                payload,
            },
        )
    }

    #[cfg(test)]
    fn ensure_post_allocation_append_reserve<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        _flash: &mut IO,
        allocation_region: u32,
        post_allocation_record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        if self.ready_region.is_some() {
            return Err(StorageRuntimeError::InvalidRotationState {
                ready_region: self.ready_region,
                requested_region: None,
            });
        }
        if self.free_space.next_ready_region().ok() != Some(allocation_region) {
            return Err(StorageRuntimeError::InvalidFreeSpaceCommand);
        }

        let allocation_head_after = self.free_space.position_after_allocation()?;
        let (physical, logical) = workspace.encode_buffers();
        let allocate_region_len = encode_record_into(
            WalRecord::AllocateRegion {
                region_index: allocation_region,
                allocation_head_after,
            },
            self.metadata,
            physical,
            logical,
        )?;
        let post_record_len =
            encode_record_into(post_allocation_record, self.metadata, physical, logical)?;
        let end = self
            .wal_append_offset
            .checked_add(allocate_region_len)
            .and_then(|offset| offset.checked_add(post_record_len))
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        let remaining_after = append_limit
            .checked_sub(end)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;

        let Ok(next_region_index) = self.free_space.next_ready_region() else {
            return if remaining_after == 0 {
                Err(StorageRuntimeError::WalRotationRequired)
            } else {
                Ok(())
            };
        };
        let next_allocation_head_after = self.free_space.position_after_allocation()?;
        let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            next_allocation_head_after,
        )?;
        if remaining_after < reserves.rotation_reserve {
            return Err(StorageRuntimeError::WalRotationRequired);
        }
        Ok(())
    }

    fn ensure_append_reserve<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        flash: &mut IO,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        if matches!(record, WalRecord::Link { .. }) {
            return Ok(());
        }

        let (physical, logical) = workspace.encode_buffers();
        let encoded_len = encode_record_into(record, self.metadata, physical, logical)?;
        self.ensure_encoded_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
            workspace,
            flash,
            encoded_len,
            matches!(record, WalRecord::AllocateRegion { .. }),
            matches!(record, WalRecord::EraseFreeRegionSpan { .. }),
        )
    }

    fn ensure_encoded_append_reserve<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        _flash: &mut IO,
        encoded_len: usize,
        allocate_region: bool,
        erase_free_region_span: bool,
    ) -> Result<(), StorageRuntimeError> {
        let end = self
            .wal_append_offset
            .checked_add(encoded_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        let remaining_after = append_limit
            .checked_sub(end)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let Ok(next_region_index) = self.free_space.next_ready_region() else {
            let dirty_regions = self.free_space.dirty_count();
            if dirty_regions > 0 {
                let ready_boundary_after = self.free_space.position_after_erase(dirty_regions)?;
                let (physical, logical) = workspace.encode_buffers();
                let erase_len = encode_record_into(
                    WalRecord::EraseFreeRegionSpan {
                        count: dirty_regions,
                        ready_boundary_after,
                    },
                    self.metadata,
                    physical,
                    logical,
                )?;
                let next_region_index = *self
                    .free_space
                    .entries()
                    .get(
                        usize::try_from(self.free_space.allocation_head())
                            .map_err(|_| StorageRuntimeError::InvalidFreeSpaceCommand)?,
                    )
                    .ok_or(StorageRuntimeError::InvalidFreeSpaceCommand)?;
                let allocation_head_after = self.free_space.position_after_allocation()?;
                let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
                    workspace,
                    next_region_index,
                    allocation_head_after,
                )?;
                let required = if erase_free_region_span {
                    reserves.rotation_reserve
                } else {
                    erase_len
                        .checked_add(reserves.rotation_reserve)
                        .ok_or(StorageRuntimeError::WalRotationRequired)?
                };
                if remaining_after < required {
                    return Err(StorageRuntimeError::WalRotationRequired);
                }
            }
            return if remaining_after == 0 || allocate_region {
                Err(StorageRuntimeError::WalRotationRequired)
            } else {
                Ok(())
            };
        };
        let allocation_head_after = self.free_space.position_after_allocation()?;
        let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            allocation_head_after,
        )?;

        if allocate_region {
            if remaining_after < reserves.rotation_reserve {
                return Err(StorageRuntimeError::WalRotationRequired);
            }
            return Ok(());
        }

        if remaining_after < reserves.rotation_reserve {
            return Err(StorageRuntimeError::WalRotationRequired);
        }
        Ok(())
    }

    pub(crate) fn rotate_wal_tail<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        loop {
            match self.append_wal_rotation_start_internal::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, true,
            ) {
                Ok(next_region_index) => {
                    return self.append_wal_rotation_finish::<REGION_SIZE, REGION_COUNT, IO>(
                        flash,
                        workspace,
                        next_region_index,
                    );
                }
                Err(StorageRuntimeError::InvalidRotationWindow {
                    remaining_after,
                    rotation_reserve,
                    ..
                }) if remaining_after >= rotation_reserve => {
                    self.bridge_early_rotation_window_gap::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn rotate_wal_tail_with_progress<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        let before = (
            self.wal_tail,
            self.wal_append_offset,
            self.free_space.allocation_head(),
            self.free_space.ready_boundary(),
            self.free_space.append_tail(),
            self.ready_region,
            self.pending_wal_recovery_boundary,
        );
        self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
        let after = (
            self.wal_tail,
            self.wal_append_offset,
            self.free_space.allocation_head(),
            self.free_space.ready_boundary(),
            self.free_space.append_tail(),
            self.ready_region,
            self.pending_wal_recovery_boundary,
        );
        if before == after {
            return Err(StorageRuntimeError::WalRotationRequired);
        }
        Ok(())
    }

    fn bridge_early_rotation_window_gap<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        let granule = usize::try_from(self.metadata.wal_write_granule)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let (physical, logical) = workspace.encode_buffers();
        let recovery_len =
            encode_record_into(WalRecord::WalRecovery, self.metadata, physical, logical)?;
        let end = self
            .wal_append_offset
            .checked_add(granule)
            .and_then(|offset| offset.checked_add(recovery_len))
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let append_limit = wal_record_append_limit(self.metadata)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        if granule == 0 || granule > physical.len() || end > append_limit {
            return Err(StorageRuntimeError::WalRotationRequired);
        }

        let invalid_byte = first_invalid_wal_boundary_byte(
            self.metadata.erased_byte,
            self.metadata.wal_record_magic,
        );
        physical[..granule].fill(invalid_byte);
        flash.write_region(self.wal_tail, self.wal_append_offset, &physical[..granule])?;
        flash.sync()?;
        self.wal_append_offset = self
            .wal_append_offset
            .checked_add(granule)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        self.pending_wal_recovery_boundary = true;

        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::WalRecovery,
        )
    }

    fn rotation_reserves<const REGION_SIZE: usize, const REGION_COUNT: usize>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        next_region_index: u32,
        allocation_head_after: FreeQueuePosition,
    ) -> Result<RotationReserves, StorageRuntimeError> {
        let expected_sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let (physical, logical) = workspace.encode_buffers();
        let allocate_region_len = encode_record_into(
            WalRecord::AllocateRegion {
                region_index: next_region_index,
                allocation_head_after,
            },
            self.metadata,
            physical,
            logical,
        )?;
        let link_reserve = encode_record_into(
            WalRecord::Link {
                next_region_index,
                expected_sequence,
            },
            self.metadata,
            physical,
            logical,
        )?;
        let rotation_reserve = allocate_region_len
            .checked_add(link_reserve)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        Ok(RotationReserves {
            allocate_region_len,
            link_reserve,
            rotation_reserve,
        })
    }

    fn classify_wal_head_record_for_reclaim(
        &self,
        original_collections: &[StartupCollection],
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        record: WalRecord<'_>,
    ) -> Result<WalHeadReclaimAction, StorageRuntimeError> {
        match record {
            WalRecord::NewCollection {
                collection_id,
                collection_type,
            } => {
                let should_rewrite = find_collection_in(original_collections, collection_id)
                    .is_some_and(|collection| collection.basis() == StartupCollectionBasis::Empty);
                if should_rewrite {
                    activate_collection(active_collections, collection_id)?;
                    return match collection_type {
                        crate::CollectionType::MAP_CODE
                        | crate::CollectionType::OBJECT_LOG_CODE => {
                            Ok(WalHeadReclaimAction::RewriteEmptyBasisAsSnapshot {
                                collection_id,
                                collection_type,
                            })
                        }
                        other => {
                            Err(StorageRuntimeError::WalHeadReclaimUnsupportedCollectionType(other))
                        }
                    };
                }
                Ok(WalHeadReclaimAction::Skip)
            }
            WalRecord::Update { collection_id, .. } => {
                Ok(if active_collections.contains(&collection_id) {
                    WalHeadReclaimAction::CopyEncoded
                } else {
                    WalHeadReclaimAction::Skip
                })
            }
            WalRecord::Snapshot { collection_id, .. } => {
                let should_copy = find_collection_in(original_collections, collection_id)
                    .is_some_and(|collection| {
                        collection.basis() == StartupCollectionBasis::WalSnapshot
                    });
                if should_copy {
                    activate_collection(active_collections, collection_id)?;
                }
                Ok(if should_copy {
                    WalHeadReclaimAction::CopyEncoded
                } else {
                    WalHeadReclaimAction::Skip
                })
            }
            WalRecord::AllocateRegion { .. }
            | WalRecord::EraseFreeRegionSpan { .. }
            | WalRecord::FreeRegion { .. } => Ok(WalHeadReclaimAction::CopyEncoded),
            WalRecord::Head {
                collection_id: CollectionId(0),
                region_index,
                ..
            } => {
                let _ = region_index;
                Ok(WalHeadReclaimAction::Skip)
            }
            WalRecord::Head {
                collection_id,
                region_index,
                ..
            } => {
                let should_copy = find_collection_in(original_collections, collection_id)
                    .is_some_and(|collection| {
                        collection.basis() == StartupCollectionBasis::Region(region_index)
                    });
                if should_copy {
                    activate_collection(active_collections, collection_id)?;
                }
                Ok(if should_copy {
                    WalHeadReclaimAction::CopyEncoded
                } else {
                    WalHeadReclaimAction::Skip
                })
            }
            WalRecord::DropCollection { collection_id } => Ok(
                if find_collection_in(original_collections, collection_id)
                    .is_some_and(|collection| collection.basis() == StartupCollectionBasis::Dropped)
                {
                    WalHeadReclaimAction::CopyEncoded
                } else {
                    WalHeadReclaimAction::Skip
                },
            ),
            WalRecord::Link { .. }
            | WalRecord::WalRecovery
            | WalRecord::BeginInlineTransaction { .. }
            | WalRecord::CommitInlineTransaction { .. }
            | WalRecord::RollbackInlineTransaction { .. }
            | WalRecord::BeginTransaction { .. }
            | WalRecord::CommitTransaction { .. }
            | WalRecord::TransactionFinished { .. }
            | WalRecord::RollbackTransaction { .. }
            | WalRecord::AddTransactionCollection { .. }
            | WalRecord::FreeIntent { .. } => Ok(WalHeadReclaimAction::Skip),
        }
    }

    fn ensure_foreground_allocation_headroom<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<(), StorageRuntimeError> {
        for _ in 0..self.metadata.region_count {
            let free_regions = self.free_region_count::<REGION_SIZE, REGION_COUNT, IO>(flash)?;
            if free_regions > self.metadata.min_free_regions {
                return Ok(());
            }

            let dirty_regions = self.free_space.dirty_count();
            if dirty_regions > 0 {
                self.erase_dirty_free_region_span_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    dirty_regions,
                )?;
                continue;
            }

            match self.reclaim_wal_head::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                reclaim_source_regions,
                active_collections,
                reclaim_plan,
                open_plan,
            ) {
                Ok(_) => {}
                Err(
                    StorageRuntimeError::WalHeadReclaimRequiresMultipleWalRegions
                    | StorageRuntimeError::WalHeadReclaimBlockedByRecoveryBoundary
                    | StorageRuntimeError::WalHeadReclaimBlockedByReadyRegion(_)
                    | StorageRuntimeError::WalHeadReclaimBlockedByRecord(_),
                ) => {
                    return Err(StorageRuntimeError::InsufficientFreeRegions {
                        free_regions,
                        min_free_regions: self.metadata.min_free_regions,
                    });
                }
                Err(error) => return Err(error),
            }
        }

        let free_regions = self.free_region_count::<REGION_SIZE, REGION_COUNT, IO>(flash)?;
        Err(StorageRuntimeError::InsufficientFreeRegions {
            free_regions,
            min_free_regions: self.metadata.min_free_regions,
        })
    }

    /// Ensures a multi-region foreground operation can allocate all planned regions.
    pub(crate) fn ensure_foreground_allocation_headroom_for<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        reclaim_plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        allocations_needed: u32,
    ) -> Result<(), StorageRuntimeError> {
        if allocations_needed == 0 {
            return Ok(());
        }

        let required_free_regions = self
            .metadata
            .min_free_regions
            .checked_add(allocations_needed)
            .ok_or(StorageRuntimeError::InsufficientFreeRegions {
                free_regions: self.free_region_count::<REGION_SIZE, REGION_COUNT, IO>(flash)?,
                min_free_regions: self.metadata.min_free_regions,
            })?;

        for _ in 0..self.metadata.region_count {
            let free_regions = self.free_region_count::<REGION_SIZE, REGION_COUNT, IO>(flash)?;
            if free_regions >= required_free_regions {
                return Ok(());
            }

            let dirty_regions = self.free_space.dirty_count();
            if dirty_regions > 0 {
                let needed = required_free_regions.checked_sub(free_regions).ok_or(
                    StorageRuntimeError::InsufficientFreeRegions {
                        free_regions,
                        min_free_regions: self.metadata.min_free_regions,
                    },
                )?;
                self.erase_dirty_free_region_span_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    dirty_regions.min(needed),
                )?;
                continue;
            }

            match self.reclaim_wal_head::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                reclaim_source_regions,
                active_collections,
                reclaim_plan,
                open_plan,
            ) {
                Ok(_) => {}
                Err(
                    StorageRuntimeError::WalHeadReclaimRequiresMultipleWalRegions
                    | StorageRuntimeError::WalHeadReclaimBlockedByRecoveryBoundary
                    | StorageRuntimeError::WalHeadReclaimBlockedByReadyRegion(_)
                    | StorageRuntimeError::WalHeadReclaimBlockedByRecord(_),
                ) => {
                    return Err(StorageRuntimeError::InsufficientFreeRegions {
                        free_regions,
                        min_free_regions: self.metadata.min_free_regions,
                    });
                }
                Err(error) => return Err(error),
            }
        }

        let free_regions = self.free_region_count::<REGION_SIZE, REGION_COUNT, IO>(flash)?;
        Err(StorageRuntimeError::InsufficientFreeRegions {
            free_regions,
            min_free_regions: self.metadata.min_free_regions,
        })
    }

    pub(crate) fn prepare_wal_head_reclaim<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        _flash: &mut IO,
        _workspace: &mut StorageWorkspace<REGION_SIZE>,
        plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
    ) -> Result<(), StorageRuntimeError> {
        if self.wal_head == self.wal_tail {
            return Err(StorageRuntimeError::WalHeadReclaimRequiresMultipleWalRegions);
        }
        if self.pending_wal_recovery_boundary {
            return Err(StorageRuntimeError::WalHeadReclaimBlockedByRecoveryBoundary);
        }
        if let Some(region_index) = self.ready_region {
            return Err(StorageRuntimeError::WalHeadReclaimBlockedByReadyRegion(
                region_index,
            ));
        }

        plan.old_head = self.wal_head;
        plan.source_tail = self.wal_tail;
        plan.source_tail_append_offset = self.wal_append_offset;
        plan.original_collections.clear();
        plan.imported_transaction_logs.clear();
        for collection in self.collections.iter().copied() {
            plan.original_collections
                .push(collection)
                .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)?;
        }
        Ok(())
    }

    pub(crate) fn collect_wal_head_reclaim_regions<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        plan: &WalHeadReclaimPlan<MAX_COLLECTIONS>,
        regions: &mut Vec<u32, REGION_COUNT>,
    ) -> Result<(), StorageRuntimeError> {
        regions.clear();
        let mut current_region = plan.old_head;

        for _ in 0..self.metadata.region_count {
            regions
                .push(current_region)
                .map_err(|_| StorageRuntimeError::WalRotationRequired)?;

            if current_region == plan.source_tail {
                return Ok(());
            }

            current_region = find_link_target_in_wal_region::<REGION_SIZE, IO>(
                flash,
                workspace,
                self.metadata,
                current_region,
            )?
            .ok_or(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                region_index: current_region,
            }))?;
        }

        Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
            region_index: current_region,
        }))
    }

    pub(crate) fn begin_wal_head_reclaim<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        old_head: u32,
    ) -> Result<(), StorageRuntimeError> {
        let _ = (flash, workspace, old_head);
        Ok(())
    }

    pub(crate) fn copy_live_wal_head_reclaim_state<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<(), StorageRuntimeError> {
        active_collections.clear();
        let metadata = self.metadata;
        let mut current_region = plan.old_head;

        for _ in 0..metadata.region_count {
            let next_region = self
                .copy_live_wal_region_reclaim_state::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    plan,
                    active_collections,
                    open_plan,
                    current_region,
                    #[cfg(feature = "perf-counters")]
                    metrics.as_deref_mut(),
                )?;

            if current_region == plan.source_tail {
                return Ok(());
            }

            current_region =
                next_region.ok_or(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                    region_index: current_region,
                }))?;
        }

        Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
            region_index: current_region,
        }))
    }

    fn copy_live_wal_region_reclaim_state<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        source_region: u32,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<Option<u32>, StorageRuntimeError> {
        let metadata = self.metadata;
        let region_size = usize::try_from(metadata.region_size)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let granule = usize::try_from(metadata.wal_write_granule)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let limit = if source_region == plan.source_tail {
            plan.source_tail_append_offset
        } else {
            region_size
        };
        let mut offset = metadata
            .wal_record_area_offset()
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        let mut pending_recovery_boundary = false;
        loop {
            let remaining = match limit.checked_sub(offset) {
                Some(0) => break,
                Some(remaining) => remaining,
                None => return Err(StorageRuntimeError::WalRotationRequired),
            };

            let action = {
                let (region_bytes, logical_scratch) = workspace.scan_buffers();
                flash.read_region(source_region, offset, remaining, |bytes| {
                    region_bytes[..remaining].copy_from_slice(bytes);
                })?;
                let start_byte = region_bytes[0];
                if start_byte == metadata.erased_byte {
                    None
                } else if start_byte != metadata.wal_record_magic {
                    pending_recovery_boundary = true;
                    let next_offset = offset
                        .checked_add(granule)
                        .ok_or(StorageRuntimeError::WalRotationRequired)?;
                    if next_offset > limit {
                        return Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                            region_index: source_region,
                        }));
                    }
                    Some((next_offset, WalHeadReclaimAction::Skip, None, 0usize, None))
                } else {
                    let decoded =
                        decode_record(&region_bytes[..remaining], metadata, logical_scratch)?;
                    let record_type = decoded.record.record_type();
                    if record_type == crate::WalRecordType::WalRecovery
                        && !pending_recovery_boundary
                    {
                        return Err(StorageRuntimeError::Startup(
                            StartupError::UnexpectedWalRecovery {
                                region_index: source_region,
                                offset,
                            },
                        ));
                    }
                    if pending_recovery_boundary && record_type != crate::WalRecordType::WalRecovery
                    {
                        return Err(StorageRuntimeError::Startup(
                            StartupError::UnexpectedRecordAfterCorruption {
                                region_index: source_region,
                                offset,
                            },
                        ));
                    }
                    if record_type == crate::WalRecordType::WalRecovery {
                        pending_recovery_boundary = false;
                    }
                    let encoded_len = decoded.encoded_len;
                    let record = decoded.record;
                    let retained_to_import = self.retained_transaction_log_for_reclaim(record);
                    let reclaim_action = self.classify_wal_head_record_for_reclaim(
                        &plan.original_collections,
                        active_collections,
                        record,
                    )?;
                    let link_target = match record {
                        WalRecord::Link {
                            next_region_index, ..
                        } => Some(next_region_index),
                        _ => None,
                    };
                    let next_offset = offset
                        .checked_add(encoded_len)
                        .ok_or(StorageRuntimeError::WalRotationRequired)?;
                    Some((
                        next_offset,
                        reclaim_action,
                        link_target,
                        encoded_len,
                        retained_to_import,
                    ))
                }
            };

            let Some((next_offset, reclaim_action, link_target, encoded_len, retained_to_import)) =
                action
            else {
                break;
            };
            if let Some(retained) = retained_to_import {
                self.copy_live_transaction_log_reclaim_state::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    plan,
                    active_collections,
                    open_plan,
                    retained,
                    #[cfg(feature = "perf-counters")]
                    metrics.as_deref_mut(),
                )?;
            }
            match reclaim_action {
                WalHeadReclaimAction::Skip => {}
                WalHeadReclaimAction::CopyEncoded => {
                    #[cfg(feature = "perf-counters")]
                    if let Some(metrics) = metrics.as_deref_mut() {
                        metrics.increment(StoragePerfCounter::WalHeadReclaimCopiedRecords);
                    }
                    self.copy_encoded_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        flash,
                        workspace,
                        source_region,
                        offset,
                        encoded_len,
                        open_plan,
                        #[cfg(feature = "perf-counters")]
                        metrics.as_deref_mut(),
                    )?;
                }
                WalHeadReclaimAction::RewriteEmptyBasisAsSnapshot {
                    collection_id,
                    collection_type,
                } => {
                    self.append_empty_basis_snapshot_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        flash,
                        workspace,
                        collection_id,
                        collection_type,
                    )?;
                }
            }

            offset = next_offset;
            if link_target.is_some() {
                return Ok(link_target);
            }
        }

        if pending_recovery_boundary {
            return Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                region_index: source_region,
            }));
        }
        Ok(None)
    }

    fn retained_transaction_log_for_reclaim(
        &self,
        record: WalRecord<'_>,
    ) -> Option<RetainedTransactionLog> {
        let (transaction_log_id, start) = match record {
            WalRecord::BeginTransaction {
                transaction_log_id,
                start,
            } => (transaction_log_id, start),
            WalRecord::CommitTransaction {
                transaction_log_id,
                range,
            }
            | WalRecord::TransactionFinished {
                transaction_log_id,
                range,
            } => (transaction_log_id, range.start),
            WalRecord::RollbackTransaction { .. } => return None,
            _ => return None,
        };

        self.retained_transaction_logs
            .iter()
            .find(|retained| {
                retained.transaction_log_id == transaction_log_id
                    && retained.range.start == start
                    && matches!(
                        retained.outcome,
                        TransactionLogOutcome::Committed | TransactionLogOutcome::Finished
                    )
            })
            .cloned()
    }

    fn transaction_log_already_imported(
        plan: &WalHeadReclaimPlan<MAX_COLLECTIONS>,
        retained: &RetainedTransactionLog,
    ) -> bool {
        plan.imported_transaction_logs
            .iter()
            .any(|range| range.start == retained.range.start)
    }

    fn remember_imported_transaction_log(
        plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        retained: &RetainedTransactionLog,
    ) -> Result<(), StorageRuntimeError> {
        if Self::transaction_log_already_imported(plan, retained) {
            return Ok(());
        }
        plan.imported_transaction_logs
            .push(retained.range)
            .map_err(|_| StorageRuntimeError::TransactionLogFull)
    }

    fn copy_live_transaction_log_reclaim_state<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        plan: &mut WalHeadReclaimPlan<MAX_COLLECTIONS>,
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
        retained: RetainedTransactionLog,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
    ) -> Result<(), StorageRuntimeError> {
        if Self::transaction_log_already_imported(plan, &retained) {
            return Ok(());
        }
        Self::remember_imported_transaction_log(plan, &retained)?;
        let range = retained.range;
        if range.start == range.end {
            return Ok(());
        }

        let metadata = self.metadata;
        let region_size = usize::try_from(metadata.region_size)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let granule = usize::try_from(metadata.wal_write_granule)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let end_offset = usize::try_from(range.end.offset)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let mut current_region = range.start.region_index;
        let mut offset = usize::try_from(range.start.offset)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;

        for _ in 0..metadata.region_count {
            {
                let (region_bytes, _) = workspace.scan_buffers();
                flash.read_region(current_region, 0, region_bytes.len(), |bytes| {
                    region_bytes.copy_from_slice(bytes);
                })?;
                validate_transaction_log_region_for_visit::<()>(
                    region_bytes,
                    metadata,
                    current_region,
                )
                .map_err(|error| match error {
                    StorageVisitError::Storage(error) => error,
                    StorageVisitError::Visitor(()) => StorageRuntimeError::WalRotationRequired,
                })?;
            }

            let limit = if current_region == range.end.region_index {
                end_offset
            } else {
                region_size
            };
            let mut next_region = None;

            while offset < limit {
                let action = {
                    let (region_bytes, logical_scratch) = workspace.scan_buffers();
                    let start_byte = region_bytes[offset];
                    if start_byte == metadata.erased_byte {
                        break;
                    }
                    if start_byte != metadata.wal_record_magic {
                        let next_offset = offset
                            .checked_add(granule)
                            .ok_or(StorageRuntimeError::WalRotationRequired)?;
                        Some((next_offset, WalHeadReclaimAction::Skip, None, 0usize))
                    } else {
                        let decoded =
                            decode_record(&region_bytes[offset..limit], metadata, logical_scratch)?;
                        let encoded_len = decoded.encoded_len;
                        let record = decoded.record;
                        let next_offset = offset
                            .checked_add(encoded_len)
                            .ok_or(StorageRuntimeError::WalRotationRequired)?;
                        let link_target = match record {
                            WalRecord::Link {
                                next_region_index, ..
                            } => Some(next_region_index),
                            _ => None,
                        };
                        let reclaim_action = if matches!(
                            record,
                            WalRecord::AllocateRegion { .. }
                                | WalRecord::EraseFreeRegionSpan { .. }
                                | WalRecord::FreeRegion { .. }
                        ) {
                            WalHeadReclaimAction::Skip
                        } else {
                            self.classify_wal_head_record_for_reclaim(
                                &plan.original_collections,
                                active_collections,
                                record,
                            )?
                        };
                        Some((next_offset, reclaim_action, link_target, encoded_len))
                    }
                };

                let Some((next_offset, reclaim_action, link_target, encoded_len)) = action else {
                    break;
                };
                match reclaim_action {
                    WalHeadReclaimAction::Skip => {}
                    WalHeadReclaimAction::CopyEncoded => {
                        #[cfg(feature = "perf-counters")]
                        if let Some(metrics) = metrics.as_deref_mut() {
                            metrics.increment(StoragePerfCounter::WalHeadReclaimCopiedRecords);
                        }
                        self.copy_encoded_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                            flash,
                            workspace,
                            current_region,
                            offset,
                            encoded_len,
                            open_plan,
                            #[cfg(feature = "perf-counters")]
                            metrics.as_deref_mut(),
                        )?;
                    }
                    WalHeadReclaimAction::RewriteEmptyBasisAsSnapshot {
                        collection_id,
                        collection_type,
                    } => {
                        self.append_empty_basis_snapshot_with_rotation::<
                            REGION_SIZE,
                            REGION_COUNT,
                            IO,
                        >(flash, workspace, collection_id, collection_type)?;
                    }
                }

                offset = next_offset;
                if let Some(next_region_index) = link_target {
                    next_region = Some(next_region_index);
                    break;
                }
            }

            if current_region == range.end.region_index {
                return Ok(());
            }

            current_region =
                next_region.ok_or(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                    region_index: current_region,
                }))?;
            offset = metadata
                .wal_record_area_offset()
                .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        }

        Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
            region_index: current_region,
        }))
    }

    pub(crate) fn commit_wal_head_reclaim<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        new_head: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Head {
                collection_id: CollectionId(0),
                collection_type: crate::CollectionType::WAL_CODE,
                region_index: new_head,
            },
        )
    }

    fn free_region_count<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &self,
        _flash: &mut IO,
    ) -> Result<u32, StorageRuntimeError> {
        Ok(self.free_space.ready_count())
    }

    #[cfg(test)]
    fn region_reachable_from_live_state<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        target_region_index: u32,
    ) -> Result<bool, StorageRuntimeError> {
        for collection in self.collections.iter() {
            let StartupCollectionBasis::Region(head_region) = collection.basis() else {
                continue;
            };
            if head_region == target_region_index {
                return Ok(true);
            }
            if collection.collection_type() == Some(CollectionType::MAP_CODE)
                && crate::collections::map::map_head_region_references_region::<REGION_SIZE, IO>(
                    flash,
                    workspace,
                    self.metadata,
                    collection.collection_id(),
                    head_region,
                    target_region_index,
                )
                .map_err(|_| {
                    StorageRuntimeError::Startup(StartupError::InvalidCommittedRegionHead {
                        collection_id: collection.collection_id(),
                        region_index: head_region,
                    })
                })?
            {
                return Ok(true);
            }
        }

        wal_chain_contains_region::<REGION_SIZE, IO>(
            flash,
            workspace,
            self.metadata,
            self.wal_head,
            self.wal_tail,
            target_region_index,
        )
    }

    #[cfg(test)]
    fn region_is_on_free_list<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &self,
        _flash: &mut IO,
        target_region_index: u32,
    ) -> Result<bool, StorageRuntimeError> {
        Ok(self
            .free_space
            .entries()
            .iter()
            .copied()
            .skip(usize::try_from(self.free_space.allocation_head()).unwrap_or(usize::MAX))
            .any(|region_index| region_index == target_region_index))
    }
}

fn find_collection_in(
    collections: &[StartupCollection],
    collection_id: CollectionId,
) -> Option<StartupCollection> {
    collections
        .iter()
        .copied()
        .find(|collection| collection.collection_id() == collection_id)
}

fn activate_collection<const MAX_COLLECTIONS: usize>(
    active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Result<(), StorageRuntimeError> {
    if active_collections.contains(&collection_id) {
        return Ok(());
    }

    active_collections
        .push(collection_id)
        .map_err(|_| StorageRuntimeError::WalHeadReclaimTooManyActiveCollections)
}

fn first_invalid_wal_boundary_byte(erased_byte: u8, wal_record_magic: u8) -> u8 {
    for candidate in 0u8..=u8::MAX {
        if candidate != erased_byte && candidate != wal_record_magic {
            return candidate;
        }
    }
    0
}

/// Error returned while visiting decoded WAL records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageVisitError<E> {
    /// Shared storage traversal failed.
    Storage(StorageRuntimeError),
    /// The caller-supplied visitor returned an error.
    Visitor(E),
}

impl<E> From<StorageRuntimeError> for StorageVisitError<E> {
    fn from(error: StorageRuntimeError) -> Self {
        Self::Storage(error)
    }
}

impl<const MAX_COLLECTIONS: usize> StorageRuntime<MAX_COLLECTIONS> {
    /// Visits retained WAL records from head through tail in replay order.
    pub fn visit_wal_records<const REGION_SIZE: usize, IO: FlashIo, E, F>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        visitor: F,
    ) -> Result<(), StorageVisitError<E>>
    where
        F: for<'record> FnMut(&mut IO, WalRecord<'record>) -> Result<(), E>,
    {
        self.visit_wal_records_inner::<REGION_SIZE, IO, E, F>(
            flash,
            workspace,
            #[cfg(feature = "perf-counters")]
            None,
            visitor,
        )
    }

    #[cfg(feature = "perf-counters")]
    pub(crate) fn visit_wal_records_metered<const REGION_SIZE: usize, IO: FlashIo, E, F>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        metrics: &mut StoragePerfMetrics,
        visitor: F,
    ) -> Result<(), StorageVisitError<E>>
    where
        F: for<'record> FnMut(&mut IO, WalRecord<'record>) -> Result<(), E>,
    {
        self.visit_wal_records_inner::<REGION_SIZE, IO, E, F>(
            flash,
            workspace,
            Some(metrics),
            visitor,
        )
    }

    fn visit_wal_records_inner<const REGION_SIZE: usize, IO: FlashIo, E, F>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        #[cfg(feature = "perf-counters")] mut metrics: Option<&mut StoragePerfMetrics>,
        mut visitor: F,
    ) -> Result<(), StorageVisitError<E>>
    where
        F: for<'record> FnMut(&mut IO, WalRecord<'record>) -> Result<(), E>,
    {
        let metadata = self.metadata;
        let region_size = usize::try_from(metadata.region_size)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let mut current_region = self.wal_head;

        loop {
            let is_tail = current_region == self.wal_tail;
            let limit = if is_tail {
                self.wal_append_offset
            } else {
                region_size
            };
            {
                let (region_bytes, _) = workspace.scan_buffers();
                #[cfg(feature = "perf-counters")]
                if let Some(metrics) = metrics.as_deref_mut() {
                    metrics.increment(StoragePerfCounter::WalReplayReads);
                    metrics.add(
                        StoragePerfCounter::WalReplayReadBytes,
                        region_bytes.len() as u64,
                    );
                }
                flash
                    .read_region(current_region, 0, region_bytes.len(), |bytes| {
                        region_bytes.copy_from_slice(bytes);
                    })
                    .map_err(StorageRuntimeError::from)?;
            }

            let mut offset = metadata
                .wal_record_area_offset()
                .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
            let mut next_region = None;
            let mut pending_boundary_open = false;
            let granule = usize::try_from(metadata.wal_write_granule).map_err(|_| {
                StorageVisitError::Storage(StorageRuntimeError::WalRotationRequired)
            })?;

            loop {
                match limit.checked_sub(offset) {
                    Some(0) => break,
                    Some(_) => {}
                    None => {
                        return Err(StorageVisitError::Storage(
                            StorageRuntimeError::WalRotationRequired,
                        ));
                    }
                }

                let start_byte = {
                    let (region_bytes, _) = workspace.scan_buffers();
                    region_bytes[offset]
                };
                if start_byte == metadata.erased_byte {
                    if !is_tail && pending_boundary_open {
                        return Err(StorageVisitError::Storage(StorageRuntimeError::Startup(
                            StartupError::BrokenWalChain {
                                region_index: current_region,
                            },
                        )));
                    }
                    break;
                }

                if start_byte != metadata.wal_record_magic {
                    pending_boundary_open = true;
                    offset = offset
                        .checked_add(granule)
                        .ok_or(StorageRuntimeError::WalRotationRequired)
                        .map_err(StorageVisitError::Storage)?;
                    continue;
                }

                let (encoded_len, link_target, deferred_commit) = {
                    let (region_bytes, logical_scratch) = workspace.scan_buffers();
                    let decoded = match decode_record(
                        &region_bytes[offset..limit],
                        metadata,
                        logical_scratch,
                    ) {
                        Ok(decoded) => decoded,
                        Err(_) => {
                            pending_boundary_open = true;
                            offset = offset
                                .checked_add(granule)
                                .ok_or(StorageRuntimeError::WalRotationRequired)
                                .map_err(StorageVisitError::Storage)?;
                            continue;
                        }
                    };
                    let record = decoded.record;
                    let encoded_len = decoded.encoded_len;
                    if pending_boundary_open && record.record_type() != WalRecordType::WalRecovery {
                        return Err(StorageVisitError::Storage(StorageRuntimeError::Startup(
                            StartupError::UnexpectedRecordAfterCorruption {
                                region_index: current_region,
                                offset,
                            },
                        )));
                    }
                    if !pending_boundary_open && record.record_type() == WalRecordType::WalRecovery
                    {
                        return Err(StorageVisitError::Storage(StorageRuntimeError::Startup(
                            StartupError::UnexpectedWalRecovery {
                                region_index: current_region,
                                offset,
                            },
                        )));
                    }
                    if record.record_type() == WalRecordType::WalRecovery {
                        pending_boundary_open = false;
                        offset = offset
                            .checked_add(encoded_len)
                            .ok_or(StorageRuntimeError::WalRotationRequired)
                            .map_err(StorageVisitError::Storage)?;
                        continue;
                    }
                    let link_target = match record {
                        WalRecord::Link {
                            next_region_index, ..
                        } => Some(next_region_index),
                        _ => None,
                    };
                    let deferred_commit = match record {
                        WalRecord::CommitTransaction {
                            transaction_log_id,
                            range,
                        } => Some((transaction_log_id, range)),
                        _ => None,
                    };
                    if deferred_commit.is_none() {
                        visitor(flash, record).map_err(StorageVisitError::Visitor)?;
                    }
                    (encoded_len, link_target, deferred_commit)
                };
                if let Some(next_region_index) = link_target {
                    next_region = Some(next_region_index);
                }
                if let Some((transaction_log_id, range)) = deferred_commit {
                    visit_transaction_log_range::<REGION_SIZE, IO, E, _>(
                        flash,
                        workspace,
                        metadata,
                        range,
                        &mut visitor,
                    )?;
                    {
                        let (region_bytes, _) = workspace.scan_buffers();
                        flash
                            .read_region(current_region, 0, region_bytes.len(), |bytes| {
                                region_bytes.copy_from_slice(bytes);
                            })
                            .map_err(StorageRuntimeError::from)
                            .map_err(StorageVisitError::Storage)?;
                    }
                    visitor(
                        flash,
                        WalRecord::CommitTransaction {
                            transaction_log_id,
                            range,
                        },
                    )
                    .map_err(StorageVisitError::Visitor)?;
                }
                offset = offset
                    .checked_add(encoded_len)
                    .ok_or(StorageRuntimeError::WalRotationRequired)
                    .map_err(StorageVisitError::Storage)?;

                if next_region.is_some() {
                    break;
                }
            }

            if is_tail {
                return Ok(());
            }

            current_region = next_region.ok_or_else(|| {
                StorageVisitError::Storage(StorageRuntimeError::Startup(
                    StartupError::BrokenWalChain {
                        region_index: current_region,
                    },
                ))
            })?;
        }
    }
}

fn visit_transaction_log_range<const REGION_SIZE: usize, IO: FlashIo, E, F>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    range: TransactionLogRange,
    visitor: &mut F,
) -> Result<(), StorageVisitError<E>>
where
    F: for<'record> FnMut(&mut IO, WalRecord<'record>) -> Result<(), E>,
{
    if range.start == range.end {
        return Ok(());
    }
    let mut current_region = range.start.region_index;
    let mut offset = usize::try_from(range.start.offset)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)
        .map_err(StorageVisitError::Storage)?;
    let end_offset = usize::try_from(range.end.offset)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)
        .map_err(StorageVisitError::Storage)?;
    let region_size = usize::try_from(metadata.region_size)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)
        .map_err(StorageVisitError::Storage)?;
    let granule = usize::try_from(metadata.wal_write_granule)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)
        .map_err(StorageVisitError::Storage)?;

    for _ in 0..metadata.region_count {
        let (region_bytes, _) = workspace.scan_buffers();
        flash
            .read_region(current_region, 0, region_bytes.len(), |bytes| {
                region_bytes.copy_from_slice(bytes);
            })
            .map_err(StorageRuntimeError::from)
            .map_err(StorageVisitError::Storage)?;
        validate_transaction_log_region_for_visit(region_bytes, metadata, current_region)?;

        let limit = if current_region == range.end.region_index {
            end_offset
        } else {
            region_size
        };
        let mut next_region = None;
        while offset < limit {
            let step = {
                let (region_bytes, logical_scratch) = workspace.scan_buffers();
                if region_bytes[offset] == metadata.erased_byte {
                    if current_region == range.end.region_index {
                        return Ok(());
                    }
                    return Err(StorageVisitError::Storage(StorageRuntimeError::Startup(
                        StartupError::BrokenWalChain {
                            region_index: current_region,
                        },
                    )));
                }
                if region_bytes[offset] != metadata.wal_record_magic {
                    offset
                        .checked_add(granule)
                        .ok_or(StorageRuntimeError::WalRotationRequired)
                        .map_err(StorageVisitError::Storage)?
                } else {
                    let decoded =
                        decode_record(&region_bytes[offset..limit], metadata, logical_scratch)
                            .map_err(StorageRuntimeError::from)
                            .map_err(StorageVisitError::Storage)?;
                    let next_offset = offset
                        .checked_add(decoded.encoded_len)
                        .ok_or(StorageRuntimeError::WalRotationRequired)
                        .map_err(StorageVisitError::Storage)?;
                    match decoded.record {
                        WalRecord::Link {
                            next_region_index, ..
                        } => {
                            next_region = Some(next_region_index);
                        }
                        record => {
                            visitor(flash, record).map_err(StorageVisitError::Visitor)?;
                        }
                    }
                    next_offset
                }
            };
            offset = step;
            if next_region.is_some() {
                break;
            }
        }

        if current_region == range.end.region_index {
            return Ok(());
        }

        current_region = next_region.ok_or_else(|| {
            StorageVisitError::Storage(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                region_index: current_region,
            }))
        })?;
        offset = metadata.wal_record_area_offset().map_err(|error| {
            StorageVisitError::Storage(StorageRuntimeError::Startup(error.into()))
        })?;
    }

    Err(StorageVisitError::Storage(StorageRuntimeError::Startup(
        StartupError::BrokenWalChain {
            region_index: current_region,
        },
    )))
}

fn validate_transaction_log_region_for_visit<E>(
    region_bytes: &[u8],
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<(), StorageVisitError<E>> {
    let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])
        .map_err(StartupError::from)
        .map_err(StorageRuntimeError::Startup)
        .map_err(StorageVisitError::Storage)?;
    if header.collection_id != CollectionId(0)
        || header.collection_format != TRANSACTION_LOG_V2_FORMAT
    {
        return Err(StorageVisitError::Storage(StorageRuntimeError::Startup(
            StartupError::InvalidWalRegion(region_index),
        )));
    }
    let prologue_start = Header::ENCODED_LEN;
    let prologue_end = prologue_start
        .checked_add(WalRegionPrologue::ENCODED_LEN)
        .ok_or(StorageRuntimeError::WalRotationRequired)
        .map_err(StorageVisitError::Storage)?;
    WalRegionPrologue::decode(
        &region_bytes[prologue_start..prologue_end],
        metadata.region_count,
    )
    .map_err(StartupError::from)
    .map_err(StorageRuntimeError::Startup)
    .map_err(StorageVisitError::Storage)?;
    Ok(())
}

fn transaction_control_references_retained_log(
    record: WalRecord<'_>,
    retained: &RetainedTransactionLog,
) -> bool {
    match record {
        WalRecord::BeginTransaction {
            transaction_log_id,
            start,
        } => transaction_log_id == retained.transaction_log_id && start == retained.range.start,
        WalRecord::CommitTransaction {
            transaction_log_id,
            range,
        }
        | WalRecord::TransactionFinished {
            transaction_log_id,
            range,
        }
        | WalRecord::RollbackTransaction {
            transaction_log_id,
            range,
        } => {
            transaction_log_id == retained.transaction_log_id && range.start == retained.range.start
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RotationReserves {
    allocate_region_len: usize,
    link_reserve: usize,
    rotation_reserve: usize,
}

/// Formats an empty store and reopens it as runtime state.
#[cfg(test)]
pub fn format<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    min_free_regions: u32,
    wal_write_granule: u32,
    wal_record_magic: u8,
) -> Result<StorageRuntime<MAX_COLLECTIONS>, StorageRuntimeError> {
    let mut runtime = StorageRuntime::empty();
    let mut open_plan = StartupOpenPlan::empty();
    format_into::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        &mut runtime,
        &mut open_plan,
        min_free_regions,
        wal_write_granule,
        wal_record_magic,
    )?;
    Ok(runtime)
}

pub(crate) fn format_into<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    runtime: &mut StorageRuntime<MAX_COLLECTIONS>,
    open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    min_free_regions: u32,
    wal_write_granule: u32,
    wal_record_magic: u8,
) -> Result<(), StorageRuntimeError> {
    flash.format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)?;
    open_into::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, runtime, open_plan,
    )
}

/// Opens a formatted store into runtime state and completes pending reclaims.
#[cfg(test)]
pub fn open<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
) -> Result<StorageRuntime<MAX_COLLECTIONS>, StorageRuntimeError> {
    let mut runtime = StorageRuntime::empty();
    let mut open_plan = StartupOpenPlan::empty();
    open_into::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        &mut runtime,
        &mut open_plan,
    )?;
    Ok(runtime)
}

pub(crate) fn open_into<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    runtime: &mut StorageRuntime<MAX_COLLECTIONS>,
    open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StorageRuntimeError> {
    reopen_without_reclaim_recovery_into::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, runtime, open_plan,
    )?;
    Ok(())
}

pub(crate) fn reopen_without_reclaim_recovery_into<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    runtime: &mut StorageRuntime<MAX_COLLECTIONS>,
    open_plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StorageRuntimeError> {
    crate::startup::begin_open_formatted_store::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, open_plan,
    )?;
    crate::startup::recover_open_rotation::<REGION_SIZE, IO, REGION_COUNT, MAX_COLLECTIONS>(
        flash, workspace, open_plan,
    )?;
    crate::startup::replay_open_wal_chain::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, open_plan,
    )?;
    crate::startup::finish_open_formatted_store_into_runtime::<
        REGION_SIZE,
        REGION_COUNT,
        IO,
        MAX_COLLECTIONS,
    >(flash, open_plan, runtime)
}

fn read_header_from_flash<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
) -> Result<crate::Header, StorageRuntimeError> {
    flash
        .read_region(region_index, 0, crate::Header::ENCODED_LEN, |bytes| {
            crate::Header::decode(bytes)
        })
        .map_err(StorageRuntimeError::from)?
        .map_err(|error| StorageRuntimeError::Startup(error.into()))
}

fn initialize_wal_region<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    region_index: u32,
    sequence: u64,
    wal_head: u32,
    allocation_head: FreeQueuePosition,
    ready_boundary: FreeQueuePosition,
    append_tail: FreeQueuePosition,
) -> Result<(), StorageRuntimeError> {
    flash.erase_region(region_index)?;
    let target = workspace.committed_write_buffer();
    let prefix_len = crate::disk::encode_log_region_prefix(
        target,
        metadata,
        sequence,
        crate::disk::MAIN_WAL_V2_FORMAT,
        wal_head,
        FreeSpaceCursors::new(allocation_head, ready_boundary, append_tail),
    )
    .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    flash.write_region(region_index, 0, &target[..prefix_len])?;
    flash.sync()?;
    Ok(())
}

fn committed_payload_capacity<const REGION_SIZE: usize>(
    metadata: StorageMetadata,
) -> Result<usize, StorageRuntimeError> {
    let granule = usize::try_from(metadata.wal_write_granule)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
    if granule == 0 {
        return Err(StorageRuntimeError::WalRotationRequired);
    }
    let aligned_region_boundary = REGION_SIZE - REGION_SIZE % granule;
    aligned_region_boundary
        .checked_sub(Header::ENCODED_LEN)
        .ok_or(StorageRuntimeError::CommittedRegionTooLarge {
            payload_len: 0,
            capacity: 0,
        })
}

fn wal_record_append_limit(metadata: StorageMetadata) -> Result<usize, crate::DiskError> {
    let region_size = usize::try_from(metadata.region_size).map_err(|_| {
        crate::DiskError::InvalidRegionIndex {
            region_index: metadata.region_size,
            region_count: metadata.region_count,
        }
    })?;
    let granule = usize::try_from(metadata.wal_write_granule)
        .map_err(|_| crate::DiskError::InvalidWalWriteGranule)?;
    if granule == 0 {
        return Err(crate::DiskError::InvalidWalWriteGranule);
    }
    Ok(region_size - region_size % granule)
}

fn committed_write_len(
    metadata: StorageMetadata,
    payload_len: usize,
) -> Result<usize, StorageRuntimeError> {
    let granule = usize::try_from(metadata.wal_write_granule)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
    if granule == 0 {
        return Err(StorageRuntimeError::WalRotationRequired);
    }
    let unaligned = Header::ENCODED_LEN
        .checked_add(payload_len)
        .ok_or(StorageRuntimeError::WalRotationRequired)?;
    let remainder = unaligned % granule;
    if remainder == 0 {
        Ok(unaligned)
    } else {
        unaligned
            .checked_add(granule - remainder)
            .ok_or(StorageRuntimeError::WalRotationRequired)
    }
}

#[cfg(test)]
fn wal_chain_contains_region<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    wal_head: u32,
    wal_tail: u32,
    target_region_index: u32,
) -> Result<bool, StorageRuntimeError> {
    let mut current_region = wal_head;

    for _ in 0..metadata.region_count {
        if current_region == target_region_index {
            return Ok(true);
        }
        if current_region == wal_tail {
            return Ok(false);
        }

        current_region = find_link_target_in_wal_region::<REGION_SIZE, IO>(
            flash,
            workspace,
            metadata,
            current_region,
        )?
        .ok_or(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
            region_index: current_region,
        }))?;
    }

    Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
        region_index: current_region,
    }))
}

fn find_link_target_in_wal_region<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<Option<u32>, StorageRuntimeError> {
    let region_size = usize::try_from(metadata.region_size)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
    let granule = usize::try_from(metadata.wal_write_granule)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
    let (region_bytes, logical_scratch) = workspace.scan_buffers();
    flash.read_region(region_index, 0, region_bytes.len(), |bytes| {
        region_bytes.copy_from_slice(bytes);
    })?;

    let mut offset = metadata
        .wal_record_area_offset()
        .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    let mut pending_recovery_boundary = false;
    loop {
        match region_size.checked_sub(offset) {
            Some(0) => break,
            Some(_) => {}
            None => return Err(StorageRuntimeError::WalRotationRequired),
        }

        let start_byte = region_bytes[offset];
        if start_byte == metadata.erased_byte {
            if pending_recovery_boundary {
                return Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                    region_index,
                }));
            }
            return Ok(None);
        }
        if start_byte != metadata.wal_record_magic {
            pending_recovery_boundary = true;
            offset = offset
                .checked_add(granule)
                .ok_or(StorageRuntimeError::WalRotationRequired)?;
            if offset > region_size {
                return Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
                    region_index,
                }));
            }
            continue;
        }

        let decoded = decode_record(
            &region_bytes[offset..region_size],
            metadata,
            logical_scratch,
        )
        .map_err(StorageRuntimeError::from)?;
        let record_type = decoded.record.record_type();
        if record_type == crate::WalRecordType::WalRecovery && !pending_recovery_boundary {
            return Err(StorageRuntimeError::Startup(
                StartupError::UnexpectedWalRecovery {
                    region_index,
                    offset,
                },
            ));
        }
        if pending_recovery_boundary && record_type != crate::WalRecordType::WalRecovery {
            return Err(StorageRuntimeError::Startup(
                StartupError::UnexpectedRecordAfterCorruption {
                    region_index,
                    offset,
                },
            ));
        }
        if record_type == crate::WalRecordType::WalRecovery {
            pending_recovery_boundary = false;
        }
        if let WalRecord::Link {
            next_region_index, ..
        } = decoded.record
        {
            return Ok(Some(next_region_index));
        }
        offset = offset
            .checked_add(decoded.encoded_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
    }

    if pending_recovery_boundary {
        return Err(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
            region_index,
        }));
    }
    Ok(None)
}

#[cfg(test)]
#[allow(unused_mut, unused_variables)]
mod tests;
