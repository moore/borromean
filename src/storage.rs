#![allow(clippy::too_many_arguments)]

use heapless::Vec;

use crate::disk::{encode_wal_region_prefix, FreePointerFooter, Header};
use crate::flash_io::{FlashIo, StorageFormatError, StorageIoError};
use crate::mock::{MockError, MockFormatError};
use crate::mode::StorageMode;
use crate::startup::{apply_wal_record, StartupCollection, StartupError, StartupOpenPlan};
use crate::wal_record::{
    decode_record, encode_record_into, WalRecord, WalRecordError, WalRecordType,
};
use crate::workspace::StorageWorkspace;
use crate::StorageMetadata;
use crate::{CollectionId, CollectionType, StartupCollectionBasis};

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
    /// An `alloc_begin` record did not match the free-list head.
    InvalidAllocBegin {
        /// Region named by `alloc_begin`.
        region_index: u32,
        /// Free-list head expected at that point.
        free_list_head: Option<u32>,
    },
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
    /// A region being returned to the free list did not have an unwritten footer.
    FreeRegionFooterNotUnwritten {
        /// Region whose free-pointer footer was already written.
        region_index: u32,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FreeRegionPreparation {
    RequireUnwrittenFooter,
    EraseToUnwrittenFooter,
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
}

impl<const MAX_COLLECTIONS: usize> WalHeadReclaimPlan<MAX_COLLECTIONS> {
    pub(crate) fn empty() -> Self {
        Self {
            old_head: 0,
            source_tail: 0,
            source_tail_append_offset: 0,
            original_collections: Vec::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.original_collections.clear();
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
    last_free_list_head: Option<u32>,
    free_list_tail: Option<u32>,
    ready_region: Option<u32>,
    max_seen_sequence: u64,
    collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    pending_wal_recovery_boundary: bool,
    open_transaction: Option<OpenTransaction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpenTransaction {
    collection_id: CollectionId,
    committed: bool,
}

impl<const MAX_COLLECTIONS: usize> StorageRuntime<MAX_COLLECTIONS> {
    pub(crate) fn empty() -> Self {
        Self {
            metadata: StorageMetadata {
                storage_version: 0,
                region_size: 0,
                region_count: 0,
                min_free_regions: 0,
                wal_write_granule: 0,
                erased_byte: 0,
                wal_record_magic: 0,
            },
            wal_head: 0,
            wal_tail: 0,
            wal_append_offset: 0,
            last_free_list_head: None,
            free_list_tail: None,
            ready_region: None,
            max_seen_sequence: 0,
            collections: Vec::new(),
            pending_wal_recovery_boundary: false,
            open_transaction: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn replace_from_startup_parts(
        &mut self,
        metadata: StorageMetadata,
        wal_head: u32,
        wal_tail: u32,
        wal_append_offset: usize,
        last_free_list_head: Option<u32>,
        free_list_tail: Option<u32>,
        ready_region: Option<u32>,
        max_seen_sequence: u64,
        collections: &[StartupCollection],
        pending_wal_recovery_boundary: bool,
    ) -> Result<(), StorageRuntimeError> {
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
        self.last_free_list_head = last_free_list_head;
        self.free_list_tail = free_list_tail;
        self.ready_region = ready_region;
        self.max_seen_sequence = max_seen_sequence;
        self.pending_wal_recovery_boundary = pending_wal_recovery_boundary;
        self.open_transaction = None;
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

    /// Returns the current free-list head, if any.
    pub fn last_free_list_head(&self) -> Option<u32> {
        self.last_free_list_head
    }

    /// Returns the current free-list tail, if any.
    pub fn free_list_tail(&self) -> Option<u32> {
        self.free_list_tail
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
        if collection_id != CollectionId(0) {
            match self.open_transaction {
                Some(open) if open.collection_id == collection_id => {}
                Some(open) => {
                    return Err(StorageRuntimeError::TransactionMismatch {
                        expected: open.collection_id,
                        actual: collection_id,
                    })
                }
                None => return Err(StorageRuntimeError::TransactionNotOpen(collection_id)),
            }
        }

        self.ensure_foreground_allocation_headroom::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
        )?;

        let region_index = loop {
            let region_index = self
                .last_free_list_head
                .ok_or(StorageRuntimeError::NoFreeRegionForRotation)?;
            let free_list_head_after = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                self.metadata,
                region_index,
            )?;
            match self.append_alloc_begin::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                region_index,
                free_list_head_after,
            ) {
                Ok(()) => break region_index,
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                }
                Err(error) => return Err(error),
            }
        };
        Ok(region_index)
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
        Ok(())
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

        self.begin_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
        )?;
        self.append_drop_collection::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
        )?;
        self.commit_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
        )?;
        if let Some(region_index) = previous_region {
            self.append_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                region_index,
            )?;
        }
        self.finish_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
        )?;

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
        loop {
            if self.last_free_list_head != Some(allocation_region) {
                let Some(current_head) = self.last_free_list_head else {
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
                    self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Appends an `alloc_begin` record for a free-list region.
    pub fn append_alloc_begin<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        region_index: u32,
        free_list_head_after: Option<u32>,
    ) -> Result<(), StorageRuntimeError> {
        let _ = collection_id;
        if self.last_free_list_head != Some(region_index) {
            return Err(StorageRuntimeError::InvalidAllocBegin {
                region_index,
                free_list_head: self.last_free_list_head,
            });
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::AllocBegin {
                collection_id,
                region_index,
                free_list_head_after,
            },
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
        open_into::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash, workspace, self, open_plan,
        )?;
        Ok(new_head)
    }

    /// Appends a `free_region` WAL record after linking the region at the free-list tail.
    pub fn append_free_region<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id != CollectionId(0) {
            match self.open_transaction {
                Some(open) if open.collection_id == collection_id => {}
                Some(open) => {
                    return Err(StorageRuntimeError::TransactionMismatch {
                        expected: open.collection_id,
                        actual: collection_id,
                    })
                }
                None => return Err(StorageRuntimeError::TransactionNotOpen(collection_id)),
            }
        }
        self.ensure_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
            workspace,
            flash,
            WalRecord::FreeRegion {
                collection_id,
                region_index,
            },
        )?;
        self.prepare_region_for_free::<REGION_SIZE, IO>(
            flash,
            region_index,
            FreeRegionPreparation::RequireUnwrittenFooter,
        )?;
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::FreeRegion {
                collection_id,
                region_index,
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
        self.append_free_region_with_rotation_prepared::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
            region_index,
            FreeRegionPreparation::RequireUnwrittenFooter,
        )
    }

    pub(crate) fn append_free_region_with_rotation_prepared<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        region_index: u32,
        preparation: FreeRegionPreparation,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id != CollectionId(0) {
            match self.open_transaction {
                Some(open) if open.collection_id == collection_id => {}
                Some(open) => {
                    return Err(StorageRuntimeError::TransactionMismatch {
                        expected: open.collection_id,
                        actual: collection_id,
                    })
                }
                None => return Err(StorageRuntimeError::TransactionNotOpen(collection_id)),
            }
        }
        self.ensure_record_append_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::FreeRegion {
                collection_id,
                region_index,
            },
        )?;
        self.prepare_region_for_free::<REGION_SIZE, IO>(flash, region_index, preparation)?;
        self.write_record_and_apply::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::FreeRegion {
                collection_id,
                region_index,
            },
        )
    }

    fn prepare_region_for_free<const REGION_SIZE: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        region_index: u32,
        preparation: FreeRegionPreparation,
    ) -> Result<(), StorageRuntimeError> {
        match preparation {
            FreeRegionPreparation::RequireUnwrittenFooter => {
                ensure_free_pointer_footer_unwritten::<REGION_SIZE, IO>(
                    flash,
                    self.metadata,
                    region_index,
                )?;
            }
            FreeRegionPreparation::EraseToUnwrittenFooter => {
                flash.erase_region(region_index)?;
                flash.sync()?;
            }
        }

        if let Some(free_list_tail) = self.free_list_tail {
            write_free_pointer_footer::<REGION_SIZE, IO>(
                flash,
                self.metadata,
                free_list_tail,
                Some(region_index),
            )?;
            flash.sync()?;
        }

        Ok(())
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
        if let Some(open) = self.open_transaction {
            return Err(StorageRuntimeError::TransactionAlreadyOpen(
                open.collection_id,
            ));
        }
        if collection_id == CollectionId(0) {
            return Err(StorageRuntimeError::ReservedCollectionId(collection_id));
        }
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::BeginTransaction { collection_id },
        )?;
        self.open_transaction = Some(OpenTransaction {
            collection_id,
            committed: false,
        });
        Ok(())
    }

    pub(crate) fn transaction_open_for(&self, collection_id: CollectionId) -> bool {
        self.open_transaction
            .is_some_and(|open| open.collection_id == collection_id)
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
        let Some(open) = self.open_transaction else {
            return Err(StorageRuntimeError::TransactionNotOpen(collection_id));
        };
        if open.collection_id != collection_id {
            return Err(StorageRuntimeError::TransactionMismatch {
                expected: open.collection_id,
                actual: collection_id,
            });
        }
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::CommitTransaction { collection_id },
        )?;
        self.open_transaction = Some(OpenTransaction {
            collection_id,
            committed: true,
        });
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
        let Some(open) = self.open_transaction else {
            return Err(StorageRuntimeError::TransactionNotOpen(collection_id));
        };
        if open.collection_id != collection_id {
            return Err(StorageRuntimeError::TransactionMismatch {
                expected: open.collection_id,
                actual: collection_id,
            });
        }
        if !open.committed {
            return Err(StorageRuntimeError::TransactionNotCommitted(collection_id));
        }
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::TransactionFinished { collection_id },
        )?;
        self.open_transaction = None;
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
        let Some(open) = self.open_transaction else {
            return Err(StorageRuntimeError::TransactionNotOpen(collection_id));
        };
        if open.collection_id != collection_id {
            return Err(StorageRuntimeError::TransactionMismatch {
                expected: open.collection_id,
                actual: collection_id,
            });
        }
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::RollbackTransaction { collection_id },
        )?;
        self.open_transaction = None;
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
        if self.ready_region.is_some() {
            return Err(StorageRuntimeError::InvalidRotationState {
                ready_region: self.ready_region,
                requested_region: None,
            });
        }

        let next_region_index = self
            .last_free_list_head
            .ok_or(StorageRuntimeError::NoFreeRegionForRotation)?;
        let free_list_head_after = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            next_region_index,
        )?;

        let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            free_list_head_after,
        )?;
        let remaining_after = REGION_SIZE
            .checked_sub(
                self.wal_append_offset
                    .checked_add(reserves.alloc_begin_len)
                    .ok_or(StorageRuntimeError::WalRotationRequired)?,
            )
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        if remaining_after < reserves.link_reserve || remaining_after >= reserves.rotation_reserve {
            return Err(StorageRuntimeError::InvalidRotationWindow {
                remaining_after,
                link_reserve: reserves.link_reserve,
                rotation_reserve: reserves.rotation_reserve,
            });
        }

        self.write_record_raw::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::AllocBegin {
                collection_id: CollectionId(0),
                region_index: next_region_index,
                free_list_head_after,
            },
        )?;
        self.wal_append_offset = self
            .wal_append_offset
            .checked_add(reserves.alloc_begin_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        self.last_free_list_head = free_list_head_after;
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
        )?;

        self.wal_tail = next_region_index;
        self.wal_append_offset = self
            .metadata
            .wal_record_area_offset()
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        self.ready_region = None;
        self.max_seen_sequence = expected_sequence;
        self.pending_wal_recovery_boundary = false;
        if self.last_free_list_head.is_none() {
            self.free_list_tail = None;
        }
        Ok(())
    }

    fn find_collection(&self, collection_id: CollectionId) -> Option<&StartupCollection> {
        self.collections
            .iter()
            .find(|collection| collection.collection_id() == collection_id)
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
        loop {
            match self.append_record::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                }
                Err(error) => return Err(error),
            }
        }
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
        loop {
            match self
                .ensure_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(workspace, flash, record)
            {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                }
                Err(error) => return Err(error),
            }
        }
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
        loop {
            match self.append_record_metered::<REGION_SIZE, REGION_COUNT, IO>(
                flash, workspace, record, metrics,
            ) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    metrics.increment(StoragePerfCounter::WalRotationRequired);
                    metrics.increment(StoragePerfCounter::WalRotationsAttempted);
                    self.observe_wal_rotation_window::<REGION_SIZE, REGION_COUNT, IO>(
                        flash, workspace, metrics,
                    );
                    let rotation_timer = StoragePerfTimerGuard::start();
                    self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                    metrics.add_nanos(
                        StoragePerfTimer::WalRotation,
                        rotation_timer.elapsed_nanos(),
                    );
                    metrics.increment(StoragePerfCounter::WalRotationsCompleted);
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(feature = "perf-counters")]
    fn observe_wal_rotation_window<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        metrics: &mut StoragePerfMetrics,
    ) {
        let remaining_bytes = REGION_SIZE.saturating_sub(self.wal_append_offset) as u64;
        let Some(next_region_index) = self.last_free_list_head else {
            metrics.observe_wal_rotation_window(remaining_bytes, 0, 0, 0);
            return;
        };
        let Ok(free_list_head_after) = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            next_region_index,
        ) else {
            metrics.observe_wal_rotation_window(remaining_bytes, 0, 0, 0);
            return;
        };
        let Ok(reserves) = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            free_list_head_after,
        ) else {
            metrics.observe_wal_rotation_window(remaining_bytes, 0, 0, 0);
            return;
        };
        metrics.observe_wal_rotation_window(
            remaining_bytes,
            reserves.alloc_begin_len as u64,
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
            WalRecord::FreeRegion { .. } => {
                self.refresh_free_list_tail::<REGION_SIZE, REGION_COUNT, IO>(flash)?;
            }
            WalRecord::AllocBegin {
                collection_id: _,
                free_list_head_after,
                ..
            } => {
                if free_list_head_after.is_none() {
                    self.free_list_tail = None;
                } else if self.free_list_tail.is_none() {
                    self.refresh_free_list_tail::<REGION_SIZE, REGION_COUNT, IO>(flash)?;
                }
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
            &mut self.last_free_list_head,
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

    fn refresh_free_list_tail<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
    ) -> Result<(), StorageRuntimeError> {
        let mut current = self.last_free_list_head;
        let Some(mut tail) = current else {
            self.free_list_tail = None;
            return Ok(());
        };

        for _ in 0..self.metadata.region_count {
            let next = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                self.metadata,
                tail,
            )?;
            match next {
                Some(next_region) => {
                    current = Some(next_region);
                    tail = next_region;
                }
                None => {
                    self.free_list_tail = Some(tail);
                    return Ok(());
                }
            }
        }

        Err(StorageRuntimeError::Startup(
            StartupError::InvalidFreeListChain {
                region_index: current.unwrap_or(tail),
            },
        ))
    }

    fn write_record_raw<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<usize, StorageRuntimeError> {
        let (physical, logical) = workspace.encode_buffers();
        let encoded_len = encode_record_into(record, self.metadata, physical, logical)?;
        if self
            .wal_append_offset
            .checked_add(encoded_len)
            .is_none_or(|end| end > REGION_SIZE)
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
        if self
            .wal_append_offset
            .checked_add(encoded_len)
            .is_none_or(|end| end > REGION_SIZE)
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
        loop {
            match self.ensure_encoded_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
                workspace,
                flash,
                encoded_len,
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
                    self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                }
                Err(error) => return Err(error),
            }
        }
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
        flash: &mut IO,
        allocation_region: u32,
        post_allocation_record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        if self.ready_region.is_some() {
            return Err(StorageRuntimeError::InvalidRotationState {
                ready_region: self.ready_region,
                requested_region: None,
            });
        }
        if self.last_free_list_head != Some(allocation_region) {
            return Err(StorageRuntimeError::InvalidAllocBegin {
                region_index: allocation_region,
                free_list_head: self.last_free_list_head,
            });
        }

        let free_list_head_after = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            allocation_region,
        )?;
        let (physical, logical) = workspace.encode_buffers();
        let alloc_begin_len = encode_record_into(
            WalRecord::AllocBegin {
                collection_id: CollectionId(0),
                region_index: allocation_region,
                free_list_head_after,
            },
            self.metadata,
            physical,
            logical,
        )?;
        let post_record_len =
            encode_record_into(post_allocation_record, self.metadata, physical, logical)?;
        let end = self
            .wal_append_offset
            .checked_add(alloc_begin_len)
            .and_then(|offset| offset.checked_add(post_record_len))
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let remaining_after = REGION_SIZE
            .checked_sub(end)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;

        let Some(next_region_index) = free_list_head_after else {
            return if remaining_after == 0 {
                Err(StorageRuntimeError::WalRotationRequired)
            } else {
                Ok(())
            };
        };
        let next_free_list_head_after = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            next_region_index,
        )?;
        let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            next_free_list_head_after,
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
            matches!(record, WalRecord::AllocBegin { .. }),
        )
    }

    fn ensure_encoded_append_reserve<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        flash: &mut IO,
        encoded_len: usize,
        alloc_begin: bool,
    ) -> Result<(), StorageRuntimeError> {
        let end = self
            .wal_append_offset
            .checked_add(encoded_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let remaining_after = REGION_SIZE
            .checked_sub(end)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let Some(next_region_index) = self.last_free_list_head else {
            return if remaining_after == 0 || alloc_begin {
                Err(StorageRuntimeError::WalRotationRequired)
            } else {
                Ok(())
            };
        };
        let free_list_head_after = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            next_region_index,
        )?;
        let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            free_list_head_after,
        )?;

        if alloc_begin {
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
            match self.append_wal_rotation_start::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)
            {
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
        if granule == 0 || granule > physical.len() || end > REGION_SIZE {
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
        free_list_head_after: Option<u32>,
    ) -> Result<RotationReserves, StorageRuntimeError> {
        let expected_sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let (physical, logical) = workspace.encode_buffers();
        let alloc_begin_len = encode_record_into(
            WalRecord::AllocBegin {
                collection_id: CollectionId(0),
                region_index: next_region_index,
                free_list_head_after,
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
        let rotation_reserve = alloc_begin_len
            .checked_add(link_reserve)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        Ok(RotationReserves {
            alloc_begin_len,
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
            WalRecord::AllocBegin { .. } | WalRecord::FreeRegion { .. } => {
                Ok(WalHeadReclaimAction::Skip)
            }
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
            | WalRecord::BeginTransaction { .. }
            | WalRecord::CommitTransaction { .. }
            | WalRecord::TransactionFinished { .. }
            | WalRecord::RollbackTransaction { .. } => Ok(WalHeadReclaimAction::Skip),
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
        plan: &WalHeadReclaimPlan<MAX_COLLECTIONS>,
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
        plan: &WalHeadReclaimPlan<MAX_COLLECTIONS>,
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
                    Some((next_offset, WalHeadReclaimAction::Skip, None, 0usize))
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
                    let reclaim_action = self.classify_wal_head_record_for_reclaim(
                        &plan.original_collections,
                        active_collections,
                        decoded.record,
                    )?;
                    let link_target = match decoded.record {
                        WalRecord::Link {
                            next_region_index, ..
                        } => Some(next_region_index),
                        _ => None,
                    };
                    let next_offset = offset
                        .checked_add(encoded_len)
                        .ok_or(StorageRuntimeError::WalRotationRequired)?;
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
        flash: &mut IO,
    ) -> Result<u32, StorageRuntimeError> {
        let mut current = self.last_free_list_head;
        let mut count = 0u32;

        for _ in 0..self.metadata.region_count {
            let Some(region_index) = current else {
                return Ok(count);
            };
            count = count
                .checked_add(1)
                .ok_or(StorageRuntimeError::WalRotationRequired)?;

            current = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                self.metadata,
                region_index,
            )?;
        }

        if let Some(region_index) = current {
            return Err(StorageRuntimeError::Startup(
                StartupError::InvalidFreeListChain { region_index },
            ));
        }

        Ok(count)
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
        flash: &mut IO,
        target_region_index: u32,
    ) -> Result<bool, StorageRuntimeError> {
        let mut current = self.last_free_list_head;

        for _ in 0..self.metadata.region_count {
            let Some(region_index) = current else {
                return Ok(false);
            };
            if region_index == target_region_index {
                return Ok(true);
            }

            current = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                self.metadata,
                region_index,
            )?;
        }

        Ok(false)
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
            let (region_bytes, logical_scratch) = workspace.scan_buffers();
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

                if region_bytes[offset] == metadata.erased_byte {
                    if !is_tail && pending_boundary_open {
                        return Err(StorageVisitError::Storage(StorageRuntimeError::Startup(
                            StartupError::BrokenWalChain {
                                region_index: current_region,
                            },
                        )));
                    }
                    break;
                }

                if region_bytes[offset] != metadata.wal_record_magic {
                    pending_boundary_open = true;
                    offset = offset
                        .checked_add(granule)
                        .ok_or(StorageRuntimeError::WalRotationRequired)
                        .map_err(StorageVisitError::Storage)?;
                    continue;
                }

                let decoded =
                    match decode_record(&region_bytes[offset..limit], metadata, logical_scratch) {
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
                if !pending_boundary_open && record.record_type() == WalRecordType::WalRecovery {
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
                if let WalRecord::Link {
                    next_region_index, ..
                } = record
                {
                    next_region = Some(next_region_index);
                }

                visitor(flash, record).map_err(StorageVisitError::Visitor)?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RotationReserves {
    alloc_begin_len: usize,
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

fn read_free_pointer_successor<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<Option<u32>, StorageRuntimeError> {
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?
        - FreePointerFooter::ENCODED_LEN;
    let footer = flash
        .read_region(
            region_index,
            footer_offset,
            FreePointerFooter::ENCODED_LEN,
            |bytes| {
                FreePointerFooter::decode_with_region_count(
                    bytes,
                    metadata.erased_byte,
                    metadata.region_count,
                )
            },
        )?
        .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    Ok(footer.next_tail)
}

fn initialize_wal_region<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    region_index: u32,
    sequence: u64,
    wal_head: u32,
) -> Result<(), StorageRuntimeError> {
    flash.erase_region(region_index)?;
    let target = workspace.committed_write_buffer();
    let prefix_len =
        encode_wal_region_prefix(target, metadata, sequence, wal_head, metadata.erased_byte)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    flash.write_region(region_index, 0, &target[..prefix_len])?;
    flash.sync()?;
    Ok(())
}

fn write_free_pointer_footer<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
    next_tail: Option<u32>,
) -> Result<(), StorageRuntimeError> {
    let footer = FreePointerFooter { next_tail };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer
        .encode_into(&mut footer_bytes, metadata.erased_byte)
        .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(StorageRuntimeError::WalRotationRequired)?;
    flash.write_region(region_index, footer_offset, &footer_bytes)?;
    Ok(())
}

fn ensure_free_pointer_footer_unwritten<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<(), StorageRuntimeError> {
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(StorageRuntimeError::WalRotationRequired)?;
    let unwritten = flash.read_region(
        region_index,
        footer_offset,
        FreePointerFooter::ENCODED_LEN,
        |bytes| bytes.iter().all(|byte| *byte == metadata.erased_byte),
    )?;
    if unwritten {
        Ok(())
    } else {
        Err(StorageRuntimeError::FreeRegionFooterNotUnwritten { region_index })
    }
}

fn committed_payload_capacity<const REGION_SIZE: usize>(
    metadata: StorageMetadata,
) -> Result<usize, StorageRuntimeError> {
    let granule = usize::try_from(metadata.wal_write_granule)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
    if granule == 0 {
        return Err(StorageRuntimeError::WalRotationRequired);
    }
    let footer_offset = REGION_SIZE
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(StorageRuntimeError::CommittedRegionTooLarge {
            payload_len: 0,
            capacity: 0,
        })?;
    let aligned_footer_boundary = footer_offset - footer_offset % granule;
    aligned_footer_boundary
        .checked_sub(Header::ENCODED_LEN)
        .ok_or(StorageRuntimeError::CommittedRegionTooLarge {
            payload_len: 0,
            capacity: 0,
        })
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
