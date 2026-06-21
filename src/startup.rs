use heapless::Vec;

use crate::disk::{
    free_space_entries_checksum, DiskError, FreeQueuePosition, FreeSpaceCursors, FreeSpaceEntry,
    FreeSpaceRegionPrologue, Header, StorageMetadata, WalRegionPrologue, FREE_SPACE_V2_FORMAT,
    TRANSACTION_LOG_V2_FORMAT, WAL_V1_FORMAT,
};
use crate::flash_io::FlashIo;
use crate::flash_io::StorageIoError;
use crate::free_space::{FreeSpaceError, FreeSpaceState};
use crate::storage::{
    RetainedTransactionLog, StorageRuntime, StorageRuntimeError, TransactionLogOutcome,
    MAX_RETAINED_TRANSACTION_LOGS, MAX_RETAINED_TRANSACTION_LOG_REGIONS, TRANSACTION_SLOT_COUNT,
};
use crate::wal_record::{
    decode_record, encode_record_into, encoded_record_len, LogPosition, TransactionLogRange,
    WalRecord, WalRecordError,
};
use crate::workspace::StorageWorkspace;
use crate::{CollectionId, CollectionType};

/// Errors returned while replaying or recovering storage state at open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupError {
    /// Disk structure decoding failed.
    Disk(DiskError),
    /// The backing I/O adapter failed.
    Mock(crate::MockError),
    /// The `embedded-storage` NOR flash adapter failed.
    #[cfg(feature = "embedded-storage")]
    EmbeddedStorage(crate::embedded_storage::EmbeddedStorageError),
    /// The Linux file-backed mmap backend failed.
    #[cfg(all(feature = "file-backing", target_os = "linux"))]
    FileBacking(crate::file_backing::FileBackingError),
    /// WAL record decoding failed.
    WalRecord(WalRecordError),
    /// Metadata was missing from the device.
    MissingMetadata,
    /// Replay could not identify a WAL tail.
    NoWalTailCandidate,
    /// Two candidate WAL tails used the same sequence number.
    DuplicateWalTailSequence(u64),
    /// A region failed WAL validation.
    InvalidWalRegion(u32),
    /// A WAL link pointed at an unexpected successor.
    InvalidWalLinkTarget {
        /// Region containing the bad link.
        region_index: u32,
        /// Sequence number the link should have targeted.
        expected_sequence: u64,
    },
    /// A replayed WAL head control record used the wrong collection type.
    InvalidWalHeadControlType(u16),
    /// The free-list chain was malformed.
    InvalidFreeListChain {
        /// Region at which the malformed chain was detected.
        region_index: u32,
    },
    /// The free-space collection metadata or a free-space WAL command was malformed.
    InvalidFreeSpaceCollection,
    /// Replay referenced a region outside the formatted range.
    InvalidRegionReference(u32),
    /// A WAL chain was missing its expected link record.
    BrokenWalChain {
        /// Region where the chain broke.
        region_index: u32,
    },
    /// Replay found a valid record after an earlier corruption point.
    UnexpectedRecordAfterCorruption {
        /// Region containing the unexpected record.
        region_index: u32,
        /// Offset of the unexpected record.
        offset: usize,
    },
    /// Replay found a `wal_recovery` marker where it was not allowed.
    UnexpectedWalRecovery {
        /// Region containing the unexpected marker.
        region_index: u32,
        /// Offset of the unexpected marker.
        offset: usize,
    },
    /// Replay saw two distinct ready regions.
    DoubleReadyRegion {
        /// Previously tracked ready region.
        existing: u32,
        /// Newly discovered conflicting ready region.
        next: u32,
    },
    /// Replay saw the same collection created twice.
    DuplicateCollection(CollectionId),
    /// Replay referenced a collection that was never created.
    UnknownCollection(CollectionId),
    /// Replay referenced a collection that was already dropped.
    DroppedCollection(CollectionId),
    /// Replay attempted to use the reserved WAL collection id as a user collection.
    ReservedCollectionId(CollectionId),
    /// A retained collection record changed collection type unexpectedly.
    CollectionTypeMismatch {
        /// Collection being validated.
        collection_id: CollectionId,
        /// Previously retained collection type.
        expected: u16,
        /// Conflicting collection type.
        actual: u16,
    },
    /// A committed region head did not point at a region for that collection.
    InvalidCommittedRegionHead {
        /// Collection being validated.
        collection_id: CollectionId,
        /// Region named by the retained head.
        region_index: u32,
    },
    /// Replay exceeded `MAX_COLLECTIONS`.
    TooManyTrackedCollections,
    /// Replay found a transaction begin before the previous transaction closed.
    NestedTransaction(CollectionId),
    /// A transaction marker did not match the open transaction collection.
    TransactionMismatch {
        /// Collection expected by the open transaction.
        expected: CollectionId,
        /// Collection carried by the marker.
        actual: CollectionId,
    },
    /// Replay reached the WAL end before a transaction terminal marker.
    UnfinishedTransaction(CollectionId),
    /// Startup transaction recovery needed to rotate the WAL but no free region was available.
    NoFreeRegionForTransactionRecovery,
    /// Startup transaction recovery found an invalid WAL rotation reserve window.
    InvalidWalRotationWindow {
        /// Bytes that would remain after the `allocate_region` record.
        remaining_after: usize,
        /// Bytes required for the full rotation sequence.
        rotation_reserve: usize,
    },
    /// A region being returned to the free list did not have an unwritten footer.
    /// Replay found a live collection type not supported by this build.
    UnsupportedLiveCollectionType(u16),
    /// Formatted metadata does not match this build's transaction slot count.
    TransactionLogCountMismatch {
        /// Transaction-log slots declared by media metadata.
        metadata_count: u32,
        /// Transaction-log slots supported by this runtime.
        slot_count: u32,
    },
    /// Replay referenced a transaction-log slot outside this runtime.
    InvalidTransactionLogId {
        /// Transaction-log slot id from the WAL record.
        transaction_log_id: u32,
        /// Transaction-log slots supported by this runtime.
        slot_count: u32,
    },
    /// A checked length conversion or addition overflowed.
    LengthOverflow,
}

impl From<DiskError> for StartupError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

impl From<crate::MockError> for StartupError {
    fn from(error: crate::MockError) -> Self {
        Self::Mock(error)
    }
}

impl From<StorageIoError> for StartupError {
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

impl From<WalRecordError> for StartupError {
    fn from(error: WalRecordError) -> Self {
        Self::WalRecord(error)
    }
}

impl From<FreeSpaceError> for StartupError {
    fn from(_error: FreeSpaceError) -> Self {
        Self::InvalidFreeSpaceCollection
    }
}

/// Replay-tracked durable basis for a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupCollectionBasis {
    /// The collection exists with only an empty basis.
    Empty,
    /// The collection basis is a retained WAL snapshot.
    WalSnapshot,
    /// The collection basis is a committed region.
    Region(u32),
    /// The collection has been durably dropped.
    Dropped,
}

/// Replay summary for one tracked collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupCollection {
    collection_id: CollectionId,
    collection_type: Option<u16>,
    basis: StartupCollectionBasis,
    pending_update_count: usize,
}

impl StartupCollection {
    /// Returns the stable collection identifier.
    pub fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    /// Returns the retained collection type, if one is known.
    pub fn collection_type(&self) -> Option<u16> {
        self.collection_type
    }

    /// Returns the retained durable basis after replay.
    pub fn basis(&self) -> StartupCollectionBasis {
        self.basis
    }

    /// Returns the number of retained updates layered over the basis.
    pub fn pending_update_count(&self) -> usize {
        self.pending_update_count
    }
}

/// Bounded replay state returned by startup before it is wrapped in [`crate::StorageRuntime`].
#[derive(Debug)]
pub struct StartupState<const MAX_COLLECTIONS: usize = 8> {
    metadata: StorageMetadata,
    wal_head: u32,
    wal_tail: u32,
    wal_append_offset: usize,
    free_space: FreeSpaceState,
    ready_region: Option<u32>,
    max_seen_sequence: u64,
    collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    #[allow(dead_code)]
    retained_transaction_logs: Vec<RetainedTransactionLog, MAX_RETAINED_TRANSACTION_LOGS>,
    pending_wal_recovery_boundary: bool,
}

impl<const MAX_COLLECTIONS: usize> StartupState<MAX_COLLECTIONS> {
    /// Returns storage metadata recovered during replay.
    pub fn metadata(&self) -> StorageMetadata {
        self.metadata
    }

    /// Returns the replay-selected WAL head region.
    pub fn wal_head(&self) -> u32 {
        self.wal_head
    }

    /// Returns the replay-selected WAL tail region.
    pub fn wal_tail(&self) -> u32 {
        self.wal_tail
    }

    /// Returns the next append offset within the WAL tail region.
    pub fn wal_append_offset(&self) -> usize {
        self.wal_append_offset
    }

    /// Returns the recovered free-space allocation cursor.
    pub fn allocation_head(&self) -> FreeQueuePosition {
        self.free_space.allocation_head_position()
    }

    /// Returns the recovered free-space ready boundary.
    pub fn ready_boundary(&self) -> FreeQueuePosition {
        self.free_space.ready_boundary_position()
    }

    /// Returns the recovered free-space append tail.
    pub fn append_tail(&self) -> FreeQueuePosition {
        self.free_space.append_tail_position()
    }

    /// Returns the ready entry at the recovered free-space allocation head, if any.
    pub fn ready_free_region(&self) -> Option<u32> {
        self.free_space.next_ready_region().ok()
    }

    /// Returns the region at the recovered free-space append tail, if any.
    pub fn free_space_tail_region(&self) -> Option<u32> {
        self.free_space.entries().last().copied()
    }

    /// Returns a reserved ready region, if replay found one.
    pub fn ready_region(&self) -> Option<u32> {
        self.ready_region
    }

    /// Returns the largest region sequence seen during replay.
    pub fn max_seen_sequence(&self) -> u64 {
        self.max_seen_sequence
    }

    /// Returns the replay-tracked collections.
    pub fn collections(&self) -> &[StartupCollection] {
        self.collections.as_slice()
    }

    /// Returns transaction-log regions pinned by reachable main-WAL control records.
    #[allow(dead_code)]
    pub(crate) fn retained_transaction_logs(&self) -> &[RetainedTransactionLog] {
        self.retained_transaction_logs.as_slice()
    }

    /// Returns whether replay left an open WAL recovery boundary.
    pub fn pending_wal_recovery_boundary(&self) -> bool {
        self.pending_wal_recovery_boundary
    }

    /// Returns the number of non-dropped user collections.
    pub fn tracked_user_collection_count(&self) -> usize {
        self.collections
            .iter()
            .filter(|collection| collection.basis != StartupCollectionBasis::Dropped)
            .count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LastValidRecord {
    AllocateRegion {
        region_index: u32,
        allocation_head_after: FreeQueuePosition,
        aligned_end_offset: usize,
    },
    Link {
        next_region_index: u32,
        expected_sequence: u64,
        aligned_end_offset: usize,
    },
    Other {
        aligned_end_offset: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegionScanResult {
    append_offset: usize,
    last_valid_record: Option<LastValidRecord>,
    wal_head_override: Option<u32>,
    pending_boundary_open: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalReplayPosition {
    chain_index: usize,
    region_index: u32,
    offset: usize,
}

#[allow(dead_code)]
fn transaction_replay_range(
    start: WalReplayPosition,
    end: WalReplayPosition,
) -> Result<TransactionLogRange, StartupError> {
    Ok(TransactionLogRange {
        start: LogPosition {
            region_index: start.region_index,
            offset: u32::try_from(start.offset).map_err(|_| StartupError::LengthOverflow)?,
        },
        end: LogPosition {
            region_index: end.region_index,
            offset: u32::try_from(end.offset).map_err(|_| StartupError::LengthOverflow)?,
        },
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpenTransactionReplay {
    collection_id: Option<CollectionId>,
    transaction_log_id: u32,
    start: LogPosition,
    committed_range: Option<TransactionLogRange>,
    commit_seen: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpenInlineTransactionReplay {
    body_start: WalReplayPosition,
    expected_record_count: u32,
    expected_encoded_len: u32,
    seen_record_count: u32,
    seen_encoded_len: u32,
}

#[derive(Debug, Default)]
struct OpenWalReplayState {
    open_transaction: Option<OpenTransactionReplay>,
    open_inline_transaction: Option<OpenInlineTransactionReplay>,
    pending_inline_rollback_cleanup: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum TransactionReplayMode {
    ApplyFullInterval,
    SkipTransactionCollectionData(CollectionId),
    ApplyRollbackCleanupOnly,
}

#[derive(Debug)]
pub(crate) struct StartupOpenPlan<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize> {
    metadata: StorageMetadata,
    wal_head_candidate: u32,
    wal_tail: u32,
    tail_scan: RegionScanResult,
    max_seen_sequence: u64,
    wal_chain: Vec<u32, REGION_COUNT>,
    collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    free_space: FreeSpaceState,
    ready_region: Option<u32>,
    wal_append_offset: usize,
    pending_wal_recovery_boundary: bool,
    transaction_original_collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    transaction_allocations: Vec<u32, REGION_COUNT>,
    transaction_frees: Vec<u32, REGION_COUNT>,
    transaction_cleanup_regions: Vec<u32, REGION_COUNT>,
    transaction_old_regions: Vec<u32, REGION_COUNT>,
    transaction_new_regions: Vec<u32, REGION_COUNT>,
    retained_transaction_logs: Vec<RetainedTransactionLog, MAX_RETAINED_TRANSACTION_LOGS>,
}

impl<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>
    StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>
{
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
            wal_head_candidate: 0,
            wal_tail: 0,
            tail_scan: RegionScanResult {
                append_offset: 0,
                last_valid_record: None,
                wal_head_override: None,
                pending_boundary_open: false,
            },
            max_seen_sequence: 0,
            wal_chain: Vec::new(),
            collections: Vec::new(),
            free_space: FreeSpaceState::empty(),
            ready_region: None,
            wal_append_offset: 0,
            pending_wal_recovery_boundary: false,
            transaction_original_collections: Vec::new(),
            transaction_allocations: Vec::new(),
            transaction_frees: Vec::new(),
            transaction_cleanup_regions: Vec::new(),
            transaction_old_regions: Vec::new(),
            transaction_new_regions: Vec::new(),
            retained_transaction_logs: Vec::new(),
        }
    }

    fn reset(
        &mut self,
        metadata: StorageMetadata,
        wal_head_candidate: u32,
        wal_tail: u32,
        tail_scan: RegionScanResult,
        max_seen_sequence: u64,
        free_space: FreeSpaceState,
    ) -> Result<(), StartupError> {
        self.metadata = metadata;
        self.wal_head_candidate = wal_head_candidate;
        self.wal_tail = wal_tail;
        self.tail_scan = tail_scan;
        self.max_seen_sequence = max_seen_sequence;
        self.wal_chain.clear();
        self.collections.clear();
        self.free_space = free_space;
        self.ready_region = None;
        self.wal_append_offset =
            usize::try_from(metadata.region_size).map_err(|_| StartupError::LengthOverflow)?;
        self.pending_wal_recovery_boundary = false;
        self.transaction_original_collections.clear();
        self.transaction_allocations.clear();
        self.transaction_frees.clear();
        self.transaction_cleanup_regions.clear();
        self.transaction_old_regions.clear();
        self.transaction_new_regions.clear();
        self.retained_transaction_logs.clear();
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.wal_chain.clear();
        self.collections.clear();
        self.free_space = FreeSpaceState::empty();
        self.transaction_original_collections.clear();
        self.transaction_allocations.clear();
        self.transaction_frees.clear();
        self.transaction_cleanup_regions.clear();
        self.transaction_old_regions.clear();
        self.transaction_new_regions.clear();
        self.retained_transaction_logs.clear();
    }

    fn capture_transaction_original_collections(&mut self) -> Result<(), StartupError> {
        self.transaction_original_collections.clear();
        for collection in self.collections.iter().copied() {
            self.transaction_original_collections
                .push(collection)
                .map_err(|_| StartupError::TooManyTrackedCollections)?;
        }
        self.transaction_allocations.clear();
        self.transaction_frees.clear();
        self.transaction_cleanup_regions.clear();
        self.transaction_old_regions.clear();
        self.transaction_new_regions.clear();
        Ok(())
    }

    fn clear_transaction_recovery_scratch(&mut self) {
        self.transaction_original_collections.clear();
        self.transaction_allocations.clear();
        self.transaction_frees.clear();
        self.transaction_cleanup_regions.clear();
        self.transaction_old_regions.clear();
        self.transaction_new_regions.clear();
    }

    fn retain_transaction_log(
        &mut self,
        transaction_log_id: u32,
        range: TransactionLogRange,
        regions: &[u32],
        outcome: TransactionLogOutcome,
    ) -> Result<(), StartupError> {
        ensure_transaction_log_id_in_range(transaction_log_id)?;
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
                    .map_err(|_| StartupError::LengthOverflow)?;
            }
            return Ok(());
        }

        let mut retained_regions = Vec::new();
        for region_index in regions.iter().copied() {
            retained_regions
                .push(region_index)
                .map_err(|_| StartupError::LengthOverflow)?;
        }
        self.retained_transaction_logs
            .push(RetainedTransactionLog {
                transaction_log_id,
                range,
                regions: retained_regions,
                outcome,
            })
            .map_err(|_| StartupError::LengthOverflow)
    }

    fn mark_transaction_log_finished(
        &mut self,
        transaction_log_id: u32,
        range: TransactionLogRange,
    ) -> Result<(), StartupError> {
        ensure_transaction_log_id_in_range(transaction_log_id)?;
        if let Some(retained) = self.retained_transaction_logs.iter_mut().find(|retained| {
            retained.transaction_log_id == transaction_log_id && retained.range.start == range.start
        }) {
            retained.range = range;
            retained.outcome = TransactionLogOutcome::Finished;
            return Ok(());
        }

        let mut regions = Vec::new();
        regions
            .push(range.start.region_index)
            .map_err(|_| StartupError::LengthOverflow)?;
        self.retained_transaction_logs
            .push(RetainedTransactionLog {
                transaction_log_id,
                range,
                regions,
                outcome: TransactionLogOutcome::Finished,
            })
            .map_err(|_| StartupError::LengthOverflow)
    }
}

/// Replays a formatted store into bounded in-memory startup state.
#[cfg(test)]
pub(crate) fn open_formatted_store<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<StartupState<MAX_COLLECTIONS>, StartupError> {
    begin_open_formatted_store::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, plan,
    )?;
    recover_open_rotation::<REGION_SIZE, IO, REGION_COUNT, MAX_COLLECTIONS>(
        flash, workspace, plan,
    )?;
    replay_open_wal_chain::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, plan,
    )?;
    finish_open_formatted_store::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(flash, plan)
}

pub(crate) fn begin_open_formatted_store<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StartupError> {
    let metadata = flash
        .read_metadata()?
        .ok_or(StartupError::MissingMetadata)?;
    metadata.validate()?;
    let slot_count =
        u32::try_from(TRANSACTION_SLOT_COUNT).map_err(|_| StartupError::LengthOverflow)?;
    if metadata.transaction_log_count != slot_count {
        return Err(StartupError::TransactionLogCountMismatch {
            metadata_count: metadata.transaction_log_count,
            slot_count,
        });
    }
    let free_space =
        load_initial_free_space_from_flash::<REGION_SIZE, IO>(flash, workspace, metadata)?;

    let (known_tail, max_seen_sequence) = locate_wal_tail::<REGION_SIZE, _>(flash, metadata)?;
    let tail_prologue = read_wal_prologue(flash, known_tail, metadata.region_count)?;
    let mut wal_head_candidate = tail_prologue.log_head_region_index;
    let tail_scan = scan_wal_region::<REGION_SIZE, _, _>(
        flash,
        workspace,
        metadata,
        known_tail,
        true,
        |_, _, record| {
            if let WalRecord::Head {
                collection_id,
                collection_type,
                region_index,
            } = record
            {
                if collection_id == CollectionId(0) {
                    if collection_type != CollectionType::WAL_CODE {
                        return Err(StartupError::InvalidWalHeadControlType(collection_type));
                    }
                    wal_head_candidate = region_index;
                }
            }

            Ok(())
        },
    )?;

    plan.reset(
        metadata,
        wal_head_candidate,
        known_tail,
        tail_scan,
        max_seen_sequence,
        free_space,
    )?;
    Ok(())
}

pub(crate) fn recover_open_rotation<
    const REGION_SIZE: usize,
    IO: FlashIo,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StartupError> {
    if let Some(recovered_tail) = recover_incomplete_rotation::<REGION_SIZE, _>(
        flash,
        workspace,
        plan.metadata,
        RotationRecoveryContext {
            wal_head: plan.wal_head_candidate,
            known_tail: plan.wal_tail,
            tail_scan: plan.tail_scan,
            cursors: FreeSpaceCursors::new(
                plan.free_space.allocation_head_position(),
                plan.free_space.ready_boundary_position(),
                plan.free_space.append_tail_position(),
            ),
        },
        &mut plan.max_seen_sequence,
    )? {
        plan.wal_tail = recovered_tail;
    }

    Ok(())
}

fn build_open_wal_replay_chain<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StartupError> {
    walk_wal_chain::<REGION_SIZE, REGION_COUNT, _>(
        flash,
        workspace,
        plan.metadata,
        plan.wal_head_candidate,
        plan.wal_tail,
        &mut plan.wal_chain,
    )?;
    Ok(())
}

pub(crate) fn replay_open_wal_chain<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StartupError> {
    build_open_wal_replay_chain::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, plan,
    )?;
    loop {
        match replay_open_wal_chain_once::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash, workspace, plan,
        )? {
            ReplayWalChainOutcome::Complete => return Ok(()),
            ReplayWalChainOutcome::RecoveredTransaction => {
                begin_open_formatted_store::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                    flash, workspace, plan,
                )?;
                recover_open_rotation::<REGION_SIZE, IO, REGION_COUNT, MAX_COLLECTIONS>(
                    flash, workspace, plan,
                )?;
                build_open_wal_replay_chain::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                    flash, workspace, plan,
                )?;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayWalChainOutcome {
    Complete,
    RecoveredTransaction,
}

fn replay_open_wal_chain_once<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<ReplayWalChainOutcome, StartupError> {
    let mut replay_state = OpenWalReplayState::default();

    let wal_chain_len = plan.wal_chain.len();
    for index in 0..wal_chain_len {
        let region_index = plan
            .wal_chain
            .get(index)
            .copied()
            .ok_or(StartupError::LengthOverflow)?;
        let is_tail = index + 1 == wal_chain_len;
        replay_open_wal_region::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            index,
            region_index,
            is_tail,
            &mut replay_state,
        )?;
    }

    if let Some(open_transaction) = replay_state.open_transaction {
        recover_unfinished_transaction::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            open_transaction,
        )?;
        return Ok(ReplayWalChainOutcome::RecoveredTransaction);
    }

    if let Some(open_inline) = replay_state.open_inline_transaction {
        let end = WalReplayPosition {
            chain_index: wal_chain_len.saturating_sub(1),
            region_index: plan.wal_tail,
            offset: plan.wal_append_offset,
        };
        replay_transaction_interval::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            open_inline.body_start,
            end,
            plan.wal_append_offset,
            TransactionReplayMode::ApplyRollbackCleanupOnly,
        )?;
        append_recovery_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            WalRecord::RollbackInlineTransaction {
                record_count: open_inline.seen_record_count,
            },
        )?;
        let _ = append_recovered_transaction_allocation_frees::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(flash, workspace, plan, CollectionId(0))?;
        plan.clear_transaction_recovery_scratch();
        return Ok(ReplayWalChainOutcome::RecoveredTransaction);
    }

    if replay_state.pending_inline_rollback_cleanup {
        let appended = append_recovered_transaction_allocation_frees::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(flash, workspace, plan, CollectionId(0))?;
        plan.clear_transaction_recovery_scratch();
        if appended {
            return Ok(ReplayWalChainOutcome::RecoveredTransaction);
        }
    }

    recover_abandoned_transaction_log_regions::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, plan,
    )?;

    Ok(ReplayWalChainOutcome::Complete)
}

fn replay_open_wal_region<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    chain_index: usize,
    region_index: u32,
    is_tail: bool,
    replay_state: &mut OpenWalReplayState,
) -> Result<(), StartupError> {
    ensure_region_index_in_range(region_index, plan.metadata.region_count)?;

    let region_size =
        usize::try_from(plan.metadata.region_size).map_err(|_| StartupError::LengthOverflow)?;
    let granule = usize::try_from(plan.metadata.wal_write_granule)
        .map_err(|_| StartupError::LengthOverflow)?;
    let mut offset = plan.metadata.wal_record_area_offset()?;
    let mut pending_boundary_open = false;
    let mut reload_region = true;

    while offset < region_size {
        if reload_region {
            let (region_bytes, _) = workspace.scan_buffers();
            flash.read_region(region_index, 0, region_bytes.len(), |bytes| {
                region_bytes.copy_from_slice(bytes);
            })?;
            reload_region = false;
        }

        let start_byte = {
            let (region_bytes, _) = workspace.scan_buffers();
            region_bytes[offset]
        };
        if start_byte == plan.metadata.erased_byte {
            if !is_tail && pending_boundary_open {
                return Err(StartupError::BrokenWalChain { region_index });
            }
            if is_tail {
                plan.wal_append_offset = offset;
                plan.pending_wal_recovery_boundary = pending_boundary_open;
            }
            return Ok(());
        }

        if start_byte != plan.metadata.wal_record_magic {
            pending_boundary_open = true;
            offset = offset
                .checked_add(granule)
                .ok_or(StartupError::LengthOverflow)?;
            continue;
        }

        let step = {
            let (region_bytes, logical_scratch) = workspace.scan_buffers();
            match decode_record(&region_bytes[offset..], plan.metadata, logical_scratch) {
                Ok(decoded) => {
                    if pending_boundary_open
                        && decoded.record.record_type() != crate::WalRecordType::WalRecovery
                    {
                        return Err(StartupError::UnexpectedRecordAfterCorruption {
                            region_index,
                            offset,
                        });
                    }

                    if decoded.record.record_type() == crate::WalRecordType::WalRecovery
                        && !pending_boundary_open
                    {
                        return Err(StartupError::UnexpectedWalRecovery {
                            region_index,
                            offset,
                        });
                    }

                    let aligned_end_offset = offset
                        .checked_add(decoded.encoded_len)
                        .ok_or(StartupError::LengthOverflow)?;
                    let current_position = WalReplayPosition {
                        chain_index,
                        region_index,
                        offset,
                    };
                    if replay_state.pending_inline_rollback_cleanup {
                        observe_inline_rollback_cleanup_record(plan, decoded.record)?;
                    }
                    let step = classify_replay_record(
                        plan,
                        &mut replay_state.open_transaction,
                        &mut replay_state.open_inline_transaction,
                        current_position,
                        aligned_end_offset,
                        decoded.record,
                    )?;

                    if decoded.record.record_type() == crate::WalRecordType::WalRecovery {
                        pending_boundary_open = false;
                    }

                    step
                }
                Err(_) => {
                    pending_boundary_open = true;
                    ReplayStep::Advance {
                        next_offset: offset
                            .checked_add(granule)
                            .ok_or(StartupError::LengthOverflow)?,
                    }
                }
            }
        };

        match step {
            ReplayStep::Advance { next_offset } => {
                offset = next_offset;
            }
            ReplayStep::ReplayTransaction {
                start,
                end,
                end_exclusive_offset,
                resume_offset,
                mode,
                cleanup_after_replay,
            } => {
                replay_transaction_interval::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                    flash,
                    workspace,
                    plan,
                    start,
                    end,
                    end_exclusive_offset,
                    mode,
                )?;
                if cleanup_after_replay {
                    replay_state.pending_inline_rollback_cleanup = true;
                }
                offset = resume_offset;
                reload_region = true;
            }
            ReplayStep::ReplayTransactionLog {
                transaction_log_id,
                range,
                mode,
                outcome,
                next_offset,
            } => {
                let replay_result =
                    replay_transaction_log_range::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                        flash, workspace, plan, range, mode,
                    )?;
                plan.retain_transaction_log(
                    transaction_log_id,
                    range,
                    replay_result.regions.as_slice(),
                    outcome,
                )?;
                if let Some(collection_id) = replay_result.collection_id {
                    if let Some(transaction) = replay_state.open_transaction.as_mut() {
                        if transaction.collection_id.is_none() {
                            transaction.collection_id = Some(collection_id);
                        }
                    }
                }
                offset = next_offset;
                reload_region = true;
            }
        }
    }

    if !is_tail && pending_boundary_open {
        return Err(StartupError::BrokenWalChain { region_index });
    }
    if is_tail {
        plan.wal_append_offset = region_size;
        plan.pending_wal_recovery_boundary = pending_boundary_open;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum ReplayStep {
    Advance {
        next_offset: usize,
    },
    ReplayTransaction {
        start: WalReplayPosition,
        end: WalReplayPosition,
        end_exclusive_offset: usize,
        resume_offset: usize,
        mode: TransactionReplayMode,
        cleanup_after_replay: bool,
    },
    ReplayTransactionLog {
        transaction_log_id: u32,
        range: TransactionLogRange,
        mode: TransactionReplayMode,
        outcome: TransactionLogOutcome,
        next_offset: usize,
    },
}

fn classify_replay_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    open_transaction: &mut Option<OpenTransactionReplay>,
    open_inline_transaction: &mut Option<OpenInlineTransactionReplay>,
    current_position: WalReplayPosition,
    aligned_end_offset: usize,
    record: WalRecord<'_>,
) -> Result<ReplayStep, StartupError> {
    validate_transaction_log_record_id(record)?;
    let next = ReplayStep::Advance {
        next_offset: aligned_end_offset,
    };

    if let Some(inline) = open_inline_transaction.as_mut() {
        match record {
            WalRecord::CommitInlineTransaction { record_count } => {
                if record_count != inline.expected_record_count
                    || inline.seen_record_count != inline.expected_record_count
                    || inline.seen_encoded_len != inline.expected_encoded_len
                {
                    return Err(StartupError::LengthOverflow);
                }
                let start = inline.body_start;
                let end = current_position;
                *open_inline_transaction = None;
                return Ok(ReplayStep::ReplayTransaction {
                    start,
                    end,
                    end_exclusive_offset: current_position.offset,
                    resume_offset: aligned_end_offset,
                    mode: TransactionReplayMode::ApplyFullInterval,
                    cleanup_after_replay: false,
                });
            }
            WalRecord::RollbackInlineTransaction { record_count } => {
                if record_count != inline.seen_record_count {
                    return Err(StartupError::LengthOverflow);
                }
                let start = inline.body_start;
                let end = current_position;
                *open_inline_transaction = None;
                return Ok(ReplayStep::ReplayTransaction {
                    start,
                    end,
                    end_exclusive_offset: current_position.offset,
                    resume_offset: aligned_end_offset,
                    mode: TransactionReplayMode::ApplyRollbackCleanupOnly,
                    cleanup_after_replay: true,
                });
            }
            WalRecord::BeginInlineTransaction { .. } | WalRecord::BeginTransaction { .. } => {
                return Err(StartupError::NestedTransaction(CollectionId(0)));
            }
            _ => {
                let record_len = aligned_end_offset
                    .checked_sub(current_position.offset)
                    .ok_or(StartupError::LengthOverflow)?;
                inline.seen_record_count = inline
                    .seen_record_count
                    .checked_add(1)
                    .ok_or(StartupError::LengthOverflow)?;
                inline.seen_encoded_len = inline
                    .seen_encoded_len
                    .checked_add(
                        u32::try_from(record_len).map_err(|_| StartupError::LengthOverflow)?,
                    )
                    .ok_or(StartupError::LengthOverflow)?;
                if inline.seen_record_count > inline.expected_record_count
                    || inline.seen_encoded_len > inline.expected_encoded_len
                {
                    return Err(StartupError::LengthOverflow);
                }
                return Ok(next);
            }
        }
    }

    let Some(transaction) = open_transaction.as_mut() else {
        match record {
            WalRecord::BeginInlineTransaction {
                record_count,
                encoded_len,
            } => {
                *open_inline_transaction = Some(OpenInlineTransactionReplay {
                    body_start: WalReplayPosition {
                        chain_index: current_position.chain_index,
                        region_index: current_position.region_index,
                        offset: aligned_end_offset,
                    },
                    expected_record_count: record_count,
                    expected_encoded_len: encoded_len,
                    seen_record_count: 0,
                    seen_encoded_len: 0,
                });
                return Ok(next);
            }
            WalRecord::CommitInlineTransaction { .. }
            | WalRecord::RollbackInlineTransaction { .. } => {
                return Err(StartupError::LengthOverflow);
            }
            WalRecord::BeginTransaction {
                transaction_log_id,
                start,
            } => {
                plan.capture_transaction_original_collections()?;
                *open_transaction = Some(OpenTransactionReplay {
                    collection_id: None,
                    transaction_log_id,
                    start,
                    committed_range: None,
                    commit_seen: false,
                });
                return Ok(next);
            }
            _ => {}
        }

        apply_open_replay_record(plan, record)?;
        return Ok(next);
    };

    match record {
        WalRecord::BeginInlineTransaction { .. } => {
            return Err(StartupError::NestedTransaction(
                transaction.collection_id.unwrap_or(CollectionId(0)),
            ));
        }
        WalRecord::BeginTransaction { .. } => {
            return Err(StartupError::NestedTransaction(
                transaction.collection_id.unwrap_or(CollectionId(0)),
            ));
        }
        WalRecord::CommitTransaction {
            transaction_log_id,
            range,
        } => {
            ensure_transaction_log_marker_matches(
                transaction.transaction_log_id,
                transaction_log_id,
            )?;
            transaction.commit_seen = true;
            transaction.committed_range = Some(range);
            return Ok(ReplayStep::ReplayTransactionLog {
                transaction_log_id,
                range,
                mode: TransactionReplayMode::ApplyFullInterval,
                outcome: TransactionLogOutcome::Committed,
                next_offset: aligned_end_offset,
            });
        }
        WalRecord::TransactionFinished {
            transaction_log_id,
            range,
        } => {
            ensure_transaction_log_marker_matches(
                transaction.transaction_log_id,
                transaction_log_id,
            )?;
            plan.mark_transaction_log_finished(transaction_log_id, range)?;
            *open_transaction = None;
            plan.clear_transaction_recovery_scratch();
            return Ok(next);
        }
        WalRecord::RollbackTransaction {
            transaction_log_id,
            range,
        } => {
            ensure_transaction_log_marker_matches(
                transaction.transaction_log_id,
                transaction_log_id,
            )?;
            *open_transaction = None;
            plan.clear_transaction_recovery_scratch();
            return Ok(ReplayStep::ReplayTransactionLog {
                transaction_log_id,
                range,
                mode: TransactionReplayMode::ApplyRollbackCleanupOnly,
                outcome: TransactionLogOutcome::RolledBack,
                next_offset: aligned_end_offset,
            });
        }
        _ => {
            if transaction.commit_seen {
                observe_transaction_recovery_record(plan, transaction, record)?;
                apply_open_replay_record(plan, record)?;
            } else {
                observe_transaction_recovery_record(plan, transaction, record)?;
                if matches!(
                    record,
                    WalRecord::AllocateRegion { .. }
                        | WalRecord::FreeRegion { .. }
                        | WalRecord::EraseFreeRegionSpan { .. }
                ) {
                    apply_open_replay_allocator_record(plan, record)?;
                }
            }
        }
    }

    Ok(next)
}

fn observe_transaction_recovery_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    transaction: &mut OpenTransactionReplay,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    match record {
        WalRecord::AllocateRegion { region_index, .. } => {
            push_unique_region(&mut plan.transaction_allocations, region_index)?;
        }
        WalRecord::FreeRegion { region_index, .. } => {
            push_unique_region(&mut plan.transaction_frees, region_index)?;
        }
        _ => {}
    }

    if let Some(collection_id) = transaction_record_collection_id(record) {
        remember_transaction_collection(transaction, collection_id)?;
    }
    if transaction.collection_id.is_none() {
        return Ok(());
    }

    Ok(())
}

fn observe_inline_rollback_cleanup_record<
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    if let WalRecord::FreeRegion { region_index, .. } = record {
        push_unique_region(&mut plan.transaction_frees, region_index)?;
    }
    Ok(())
}

fn transaction_record_collection_id(record: WalRecord<'_>) -> Option<CollectionId> {
    match record {
        WalRecord::NewCollection { collection_id, .. }
        | WalRecord::Update { collection_id, .. }
        | WalRecord::Snapshot { collection_id, .. }
        | WalRecord::Head { collection_id, .. }
        | WalRecord::DropCollection { collection_id }
        | WalRecord::AddTransactionCollection { collection_id, .. } => Some(collection_id),
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

fn remember_transaction_collection(
    transaction: &mut OpenTransactionReplay,
    collection_id: CollectionId,
) -> Result<(), StartupError> {
    if collection_id != CollectionId(0) && transaction.collection_id.is_none() {
        transaction.collection_id = Some(collection_id);
    }
    Ok(())
}

fn transaction_collection_id(
    transaction: &OpenTransactionReplay,
) -> Result<CollectionId, StartupError> {
    transaction
        .collection_id
        .ok_or(StartupError::UnfinishedTransaction(CollectionId(0)))
}

fn ensure_transaction_log_marker_matches(expected: u32, actual: u32) -> Result<(), StartupError> {
    ensure_transaction_log_id_in_range(actual)?;
    if actual != expected {
        return Err(StartupError::TransactionMismatch {
            expected: CollectionId(u64::from(expected)),
            actual: CollectionId(u64::from(actual)),
        });
    }
    Ok(())
}

fn validate_transaction_log_record_id(record: WalRecord<'_>) -> Result<(), StartupError> {
    match record {
        WalRecord::BeginTransaction {
            transaction_log_id, ..
        }
        | WalRecord::CommitTransaction {
            transaction_log_id, ..
        }
        | WalRecord::TransactionFinished {
            transaction_log_id, ..
        }
        | WalRecord::RollbackTransaction {
            transaction_log_id, ..
        } => ensure_transaction_log_id_in_range(transaction_log_id),
        _ => Ok(()),
    }
}

fn ensure_transaction_log_id_in_range(transaction_log_id: u32) -> Result<(), StartupError> {
    let slot_count =
        u32::try_from(TRANSACTION_SLOT_COUNT).map_err(|_| StartupError::LengthOverflow)?;
    if transaction_log_id >= slot_count {
        return Err(StartupError::InvalidTransactionLogId {
            transaction_log_id,
            slot_count,
        });
    }
    Ok(())
}

fn push_unique_region<const CAP: usize>(
    regions: &mut Vec<u32, CAP>,
    region_index: u32,
) -> Result<(), StartupError> {
    if regions.contains(&region_index) {
        return Ok(());
    }
    regions
        .push(region_index)
        .map_err(|_| StartupError::LengthOverflow)
}

fn load_initial_free_space_from_flash<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
) -> Result<FreeSpaceState, StartupError> {
    if metadata.region_count < 2 {
        return Err(StartupError::InvalidFreeSpaceCollection);
    }
    let mut entries = heapless::Vec::<u32, { crate::free_space::MAX_FREE_QUEUE_ENTRIES }>::new();
    let mut metadata_regions =
        heapless::Vec::<u32, { crate::free_space::MAX_FREE_QUEUE_ENTRIES }>::new();
    let mut first_prologue = None;
    let mut region_index = 1;
    let entries_per_region = {
        let entries_offset = Header::ENCODED_LEN
            .checked_add(FreeSpaceRegionPrologue::ENCODED_LEN)
            .ok_or(StartupError::LengthOverflow)?;
        let available = REGION_SIZE
            .checked_sub(entries_offset)
            .ok_or(StartupError::InvalidFreeSpaceCollection)?;
        let entries_per_region = available / FreeSpaceEntry::ENCODED_LEN;
        if entries_per_region == 0 {
            return Err(StartupError::InvalidFreeSpaceCollection);
        }
        u32::try_from(entries_per_region).map_err(|_| StartupError::LengthOverflow)?
    };
    for _ in 0..metadata.region_count {
        metadata_regions
            .push(region_index)
            .map_err(|_| StartupError::InvalidFreeSpaceCollection)?;
        let region_bytes = workspace.committed_write_buffer();
        flash.read_region(region_index, 0, REGION_SIZE, |bytes| {
            region_bytes.copy_from_slice(bytes);
        })?;

        let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
        if header.collection_id != CollectionId(0)
            || header.collection_format != FREE_SPACE_V2_FORMAT
        {
            return Err(StartupError::InvalidFreeSpaceCollection);
        }

        let prologue_start = Header::ENCODED_LEN;
        let prologue_end = prologue_start
            .checked_add(FreeSpaceRegionPrologue::ENCODED_LEN)
            .ok_or(StartupError::LengthOverflow)?;
        let prologue = FreeSpaceRegionPrologue::decode(
            &region_bytes[prologue_start..prologue_end],
            metadata.region_count,
        )?;
        if first_prologue.is_none() {
            first_prologue = Some(prologue);
        }

        let entry_count =
            usize::try_from(prologue.entry_count).map_err(|_| StartupError::LengthOverflow)?;
        let entries_start = prologue_end;
        let entries_len = entry_count
            .checked_mul(FreeSpaceEntry::ENCODED_LEN)
            .ok_or(StartupError::LengthOverflow)?;
        let entries_end = entries_start
            .checked_add(entries_len)
            .ok_or(StartupError::LengthOverflow)?;
        if entries_end > REGION_SIZE {
            return Err(StartupError::InvalidFreeSpaceCollection);
        }
        if free_space_entries_checksum(&region_bytes[entries_start..entries_end])
            != prologue.entries_checksum
        {
            return Err(StartupError::InvalidFreeSpaceCollection);
        }

        let mut offset = entries_start;
        for _ in 0..entry_count {
            let entry_end = offset
                .checked_add(FreeSpaceEntry::ENCODED_LEN)
                .ok_or(StartupError::LengthOverflow)?;
            let entry =
                FreeSpaceEntry::decode(&region_bytes[offset..entry_end], metadata.region_count)?;
            entries
                .push(entry.region_index)
                .map_err(|_| StartupError::InvalidFreeSpaceCollection)?;
            offset = entry_end;
        }

        let Some(next_region_index) = prologue.next_metadata_region else {
            break;
        };
        region_index = next_region_index;
    }
    let Some(prologue) = first_prologue else {
        return Err(StartupError::InvalidFreeSpaceCollection);
    };

    let mut state = FreeSpaceState::empty();
    state.replace_from_position_parts(
        metadata_regions.as_slice(),
        entries_per_region,
        prologue.allocation_head,
        prologue.ready_boundary,
        prologue.append_tail,
        entries.as_slice(),
    )?;
    Ok(state)
}

fn recover_unfinished_transaction<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    open_transaction: OpenTransactionReplay,
) -> Result<(), StartupError> {
    if open_transaction.commit_seen {
        let range = open_transaction
            .committed_range
            .ok_or(StartupError::UnfinishedTransaction(CollectionId(0)))?;
        let collection_id = transaction_collection_id(&open_transaction).unwrap_or(CollectionId(0));
        derive_transaction_cleanup_regions::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            collection_id,
        )?;
        append_missing_transaction_cleanup_frees::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            collection_id,
        )?;
        append_recovery_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            WalRecord::TransactionFinished {
                transaction_log_id: open_transaction.transaction_log_id,
                range,
            },
        )?;
    } else {
        let range = scan_transaction_log_to_durable_end::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(flash, workspace, plan, open_transaction.start)?;
        let _ = replay_transaction_log_range::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            range,
            TransactionReplayMode::ApplyRollbackCleanupOnly,
        )?;
        let collection_id = transaction_collection_id(&open_transaction).unwrap_or(CollectionId(0));
        let _ = append_recovered_transaction_allocation_frees::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(flash, workspace, plan, collection_id)?;
        append_recovery_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            WalRecord::RollbackTransaction {
                transaction_log_id: open_transaction.transaction_log_id,
                range,
            },
        )?;
    }

    plan.clear_transaction_recovery_scratch();
    Ok(())
}

fn append_recovered_transaction_allocation_frees<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Result<bool, StartupError> {
    let mut appended = false;
    let allocation_count = plan.transaction_allocations.len();
    for index in 0..allocation_count {
        let region_index = plan.transaction_allocations[index];
        if plan.transaction_frees.contains(&region_index) {
            continue;
        }
        append_recovery_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            collection_id,
            region_index,
        )?;
        push_unique_region(&mut plan.transaction_frees, region_index)?;
        appended = true;
    }
    Ok(appended)
}

fn append_missing_transaction_cleanup_frees<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Result<(), StartupError> {
    let cleanup_region_count = plan.transaction_cleanup_regions.len();
    for index in 0..cleanup_region_count {
        let region_index = plan.transaction_cleanup_regions[index];
        if plan.transaction_frees.contains(&region_index) {
            continue;
        }
        append_recovery_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            collection_id,
            region_index,
        )?;
        push_unique_region(&mut plan.transaction_frees, region_index)?;
    }
    Ok(())
}

fn derive_transaction_cleanup_regions<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Result<(), StartupError> {
    let old_collection = find_collection(
        plan.transaction_original_collections.as_slice(),
        collection_id,
    )
    .and_then(|index| plan.transaction_original_collections.get(index).copied());
    let new_collection = find_collection(plan.collections.as_slice(), collection_id)
        .and_then(|index| plan.collections.get(index).copied());

    plan.transaction_cleanup_regions.clear();
    plan.transaction_old_regions.clear();
    plan.transaction_new_regions.clear();
    collect_collection_committed_regions::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan.metadata,
        old_collection,
        &mut plan.transaction_old_regions,
    )?;
    collect_collection_committed_regions::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan.metadata,
        new_collection,
        &mut plan.transaction_new_regions,
    )?;

    for region_index in plan.transaction_old_regions.iter().copied() {
        if !plan.transaction_new_regions.contains(&region_index) {
            push_unique_region(&mut plan.transaction_cleanup_regions, region_index)?;
        }
    }
    Ok(())
}

fn collect_collection_committed_regions<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    collection: Option<StartupCollection>,
    regions: &mut Vec<u32, REGION_COUNT>,
) -> Result<(), StartupError> {
    let Some(collection) = collection else {
        return Ok(());
    };
    let StartupCollectionBasis::Region(head_region) = collection.basis() else {
        return Ok(());
    };
    match collection.collection_type() {
        Some(CollectionType::MAP_CODE) => {
            crate::collections::map::collect_map_head_regions::<REGION_SIZE, IO, REGION_COUNT>(
                flash,
                workspace,
                metadata,
                collection.collection_id(),
                head_region,
                regions,
            )
            .map_err(|_| StartupError::InvalidCommittedRegionHead {
                collection_id: collection.collection_id(),
                region_index: head_region,
            })
        }
        Some(CollectionType::OBJECT_LOG_CODE) => {
            crate::collections::object_log::collect_committed_regions::<
                REGION_SIZE,
                IO,
                REGION_COUNT,
            >(
                flash,
                workspace,
                metadata,
                collection.collection_id(),
                head_region,
                regions,
            )
            .map_err(|_| StartupError::InvalidCommittedRegionHead {
                collection_id: collection.collection_id(),
                region_index: head_region,
            })
        }
        Some(other) => Err(StartupError::UnsupportedLiveCollectionType(other)),
        None => Ok(()),
    }
}

fn recover_abandoned_transaction_log_regions<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StartupError> {
    for region_index in 0..plan.metadata.region_count {
        if retained_transaction_log_contains_region(plan, region_index)
            || free_space_active_range_contains_region(&plan.free_space, region_index)
            || plan.wal_chain.contains(&region_index)
        {
            continue;
        }

        let is_abandoned = flash.read_region(region_index, 0, Header::ENCODED_LEN, |bytes| {
            Header::decode(bytes).is_ok_and(|header| {
                header.collection_id == CollectionId(0)
                    && header.collection_format == TRANSACTION_LOG_V2_FORMAT
            })
        })?;
        if !is_abandoned {
            continue;
        }

        append_recovery_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            CollectionId(0),
            region_index,
        )?;
    }
    Ok(())
}

fn retained_transaction_log_contains_region<
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    plan: &StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    region_index: u32,
) -> bool {
    plan.retained_transaction_logs
        .iter()
        .any(|retained| retained.regions.contains(&region_index))
}

fn free_space_active_range_contains_region(free_space: &FreeSpaceState, region_index: u32) -> bool {
    let start = match usize::try_from(free_space.allocation_head()) {
        Ok(start) => start,
        Err(_) => return false,
    };
    let end = match usize::try_from(free_space.append_tail()) {
        Ok(end) => end.min(free_space.entries().len()),
        Err(_) => return false,
    };
    free_space
        .entries()
        .get(start..end)
        .is_some_and(|entries| entries.contains(&region_index))
}

fn append_recovery_free_region_with_rotation<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    _collection_id: CollectionId,
    region_index: u32,
) -> Result<(), StartupError> {
    append_recovery_record_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan,
        WalRecord::FreeRegion {
            region_index,
            append_tail_after: plan.free_space.position_after_append()?,
        },
    )?;
    write_recovery_record_and_apply::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan,
        WalRecord::FreeRegion {
            region_index,
            append_tail_after: plan.free_space.position_after_append()?,
        },
    )
}

fn append_recovery_record_with_rotation<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    append_recovery_record_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, plan, record,
    )?;
    write_recovery_record_and_apply::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash, workspace, plan, record,
    )
}

fn append_recovery_record_room_with_rotation<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    loop {
        if recovery_record_has_append_room::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash, workspace, plan, record,
        )? {
            return Ok(());
        }
        rotate_recovery_wal_tail::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash, workspace, plan,
        )?;
    }
}

fn recovery_record_has_append_room<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    _flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<bool, StartupError> {
    if matches!(record, WalRecord::Link { .. }) {
        return Ok(true);
    }

    let (physical, logical) = workspace.encode_buffers();
    let encoded_len = encode_record_into(record, plan.metadata, physical, logical)?;
    let Some(end) = plan.wal_append_offset.checked_add(encoded_len) else {
        return Ok(false);
    };
    if end > REGION_SIZE {
        return Ok(false);
    }
    let remaining_after = REGION_SIZE
        .checked_sub(end)
        .ok_or(StartupError::LengthOverflow)?;

    let Ok(next_region_index) = plan.free_space.next_ready_region() else {
        return Ok(remaining_after != 0 && !matches!(record, WalRecord::AllocateRegion { .. }));
    };
    let allocation_head_after = plan.free_space.position_after_allocation()?;
    let reserves = recovery_rotation_reserves::<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>(
        workspace,
        plan,
        next_region_index,
        allocation_head_after,
    )?;
    Ok(remaining_after >= reserves.rotation_reserve)
}

fn write_recovery_record_and_apply<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    let (physical, logical) = workspace.encode_buffers();
    let encoded_len = encode_record_into(record, plan.metadata, physical, logical)?;
    if plan
        .wal_append_offset
        .checked_add(encoded_len)
        .is_none_or(|end| end > REGION_SIZE)
    {
        return Err(StartupError::LengthOverflow);
    }

    flash.write_region(
        plan.wal_tail,
        plan.wal_append_offset,
        &physical[..encoded_len],
    )?;
    flash.sync()?;
    apply_open_replay_record(plan, record)?;
    plan.wal_append_offset = plan
        .wal_append_offset
        .checked_add(encoded_len)
        .ok_or(StartupError::LengthOverflow)?;
    if matches!(record, WalRecord::FreeRegion { .. }) {
        plan.pending_wal_recovery_boundary = false;
    }
    Ok(())
}

fn rotate_recovery_wal_tail<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StartupError> {
    loop {
        match append_recovery_wal_rotation_start::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash, workspace, plan,
        ) {
            Ok(next_region_index) => {
                return append_recovery_wal_rotation_finish::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                >(flash, workspace, plan, next_region_index);
            }
            Err(StartupError::InvalidWalRotationWindow {
                remaining_after,
                rotation_reserve,
            }) if remaining_after >= rotation_reserve => {
                bridge_recovery_wal_rotation_gap::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                    flash, workspace, plan,
                )?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn append_recovery_wal_rotation_start<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<u32, StartupError> {
    if plan.ready_region.is_some() {
        return Err(StartupError::DoubleReadyRegion {
            existing: plan.ready_region.unwrap_or(0),
            next: plan.ready_region.unwrap_or(0),
        });
    }

    let next_region_index = plan
        .free_space
        .next_ready_region()
        .map_err(|_| StartupError::NoFreeRegionForTransactionRecovery)?;
    let allocation_head_after = plan.free_space.position_after_allocation()?;
    let reserves = recovery_rotation_reserves::<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>(
        workspace,
        plan,
        next_region_index,
        allocation_head_after,
    )?;
    let remaining_after = REGION_SIZE
        .checked_sub(
            plan.wal_append_offset
                .checked_add(reserves.allocate_region_len)
                .ok_or(StartupError::LengthOverflow)?,
        )
        .ok_or(StartupError::LengthOverflow)?;
    if remaining_after < reserves.link_reserve || remaining_after >= reserves.rotation_reserve {
        return Err(StartupError::InvalidWalRotationWindow {
            remaining_after,
            rotation_reserve: reserves.rotation_reserve,
        });
    }

    write_recovery_record_raw::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan,
        WalRecord::AllocateRegion {
            region_index: next_region_index,
            allocation_head_after,
        },
    )?;
    plan.wal_append_offset = plan
        .wal_append_offset
        .checked_add(reserves.allocate_region_len)
        .ok_or(StartupError::LengthOverflow)?;
    apply_open_replay_record(
        plan,
        WalRecord::AllocateRegion {
            region_index: next_region_index,
            allocation_head_after,
        },
    )?;
    plan.pending_wal_recovery_boundary = false;
    Ok(next_region_index)
}

fn append_recovery_wal_rotation_finish<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    next_region_index: u32,
) -> Result<(), StartupError> {
    let expected_sequence = plan
        .max_seen_sequence
        .checked_add(1)
        .ok_or(StartupError::LengthOverflow)?;
    write_recovery_record_raw::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan,
        WalRecord::Link {
            next_region_index,
            expected_sequence,
        },
    )?;
    initialize_wal_region::<REGION_SIZE, IO>(
        flash,
        workspace,
        plan.metadata,
        next_region_index,
        expected_sequence,
        plan.wal_head_candidate,
        FreeSpaceCursors::new(
            plan.free_space.allocation_head_position(),
            plan.free_space.ready_boundary_position(),
            plan.free_space.append_tail_position(),
        ),
    )?;
    plan.wal_tail = next_region_index;
    plan.wal_append_offset = plan.metadata.wal_record_area_offset()?;
    plan.ready_region = None;
    plan.max_seen_sequence = expected_sequence;
    plan.pending_wal_recovery_boundary = false;
    Ok(())
}

fn bridge_recovery_wal_rotation_gap<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<(), StartupError> {
    let granule = usize::try_from(plan.metadata.wal_write_granule)
        .map_err(|_| StartupError::LengthOverflow)?;
    let (physical, logical) = workspace.encode_buffers();
    if granule == 0 || granule > physical.len() {
        return Err(StartupError::LengthOverflow);
    }
    let recovery_len =
        encode_record_into(WalRecord::WalRecovery, plan.metadata, physical, logical)?;
    let end = plan
        .wal_append_offset
        .checked_add(granule)
        .and_then(|offset| offset.checked_add(recovery_len))
        .ok_or(StartupError::LengthOverflow)?;
    if end > REGION_SIZE {
        return Err(StartupError::LengthOverflow);
    }

    let invalid_byte =
        first_invalid_wal_boundary_byte(plan.metadata.erased_byte, plan.metadata.wal_record_magic);
    physical[..granule].fill(invalid_byte);
    flash.write_region(plan.wal_tail, plan.wal_append_offset, &physical[..granule])?;
    flash.sync()?;
    plan.wal_append_offset = plan
        .wal_append_offset
        .checked_add(granule)
        .ok_or(StartupError::LengthOverflow)?;
    plan.pending_wal_recovery_boundary = true;
    write_recovery_record_and_apply::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan,
        WalRecord::WalRecovery,
    )
}

fn first_invalid_wal_boundary_byte(erased_byte: u8, wal_record_magic: u8) -> u8 {
    for candidate in 0u8..=u8::MAX {
        if candidate != erased_byte && candidate != wal_record_magic {
            return candidate;
        }
    }
    0
}

fn write_recovery_record_raw<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<usize, StartupError> {
    let (physical, logical) = workspace.encode_buffers();
    let encoded_len = encode_record_into(record, plan.metadata, physical, logical)?;
    if plan
        .wal_append_offset
        .checked_add(encoded_len)
        .is_none_or(|end| end > REGION_SIZE)
    {
        return Err(StartupError::LengthOverflow);
    }
    flash.write_region(
        plan.wal_tail,
        plan.wal_append_offset,
        &physical[..encoded_len],
    )?;
    flash.sync()?;
    Ok(encoded_len)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryRotationReserves {
    allocate_region_len: usize,
    link_reserve: usize,
    rotation_reserve: usize,
}

fn recovery_rotation_reserves<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    next_region_index: u32,
    allocation_head_after: FreeQueuePosition,
) -> Result<RecoveryRotationReserves, StartupError> {
    let expected_sequence = plan
        .max_seen_sequence
        .checked_add(1)
        .ok_or(StartupError::LengthOverflow)?;
    let (physical, logical) = workspace.encode_buffers();
    let allocate_region_len = encode_record_into(
        WalRecord::AllocateRegion {
            region_index: next_region_index,
            allocation_head_after,
        },
        plan.metadata,
        physical,
        logical,
    )?;
    let link_reserve = encode_record_into(
        WalRecord::Link {
            next_region_index,
            expected_sequence,
        },
        plan.metadata,
        physical,
        logical,
    )?;
    let rotation_reserve = allocate_region_len
        .checked_add(link_reserve)
        .ok_or(StartupError::LengthOverflow)?;
    Ok(RecoveryRotationReserves {
        allocate_region_len,
        link_reserve,
        rotation_reserve,
    })
}

fn replay_transaction_interval<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    start: WalReplayPosition,
    end: WalReplayPosition,
    end_exclusive_offset: usize,
    mode: TransactionReplayMode,
) -> Result<(), StartupError> {
    let region_size =
        usize::try_from(plan.metadata.region_size).map_err(|_| StartupError::LengthOverflow)?;

    for chain_index in start.chain_index..=end.chain_index {
        let region_index = plan
            .wal_chain
            .get(chain_index)
            .copied()
            .ok_or(StartupError::LengthOverflow)?;
        let offset_start = if chain_index == start.chain_index {
            start.offset
        } else {
            plan.metadata.wal_record_area_offset()?
        };
        let offset_end = if chain_index == end.chain_index {
            end_exclusive_offset
        } else {
            region_size
        };
        replay_transaction_region_interval::<REGION_SIZE, IO, REGION_COUNT, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            region_index,
            offset_start,
            offset_end,
            mode,
        )?;
    }
    Ok(())
}

fn replay_transaction_log_range<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    range: TransactionLogRange,
    mode: TransactionReplayMode,
) -> Result<TransactionLogReplayResult, StartupError> {
    let mut result = TransactionLogReplayResult {
        collection_id: None,
        regions: Vec::new(),
    };
    push_unique_region(&mut result.regions, range.start.region_index)?;
    if range.start == range.end {
        return Ok(result);
    }
    ensure_region_index_in_range(range.start.region_index, plan.metadata.region_count)?;
    ensure_region_index_in_range(range.end.region_index, plan.metadata.region_count)?;
    let mut current_region = range.start.region_index;
    let mut offset =
        usize::try_from(range.start.offset).map_err(|_| StartupError::LengthOverflow)?;
    let end_offset = usize::try_from(range.end.offset).map_err(|_| StartupError::LengthOverflow)?;
    let region_size =
        usize::try_from(plan.metadata.region_size).map_err(|_| StartupError::LengthOverflow)?;
    let granule = usize::try_from(plan.metadata.wal_write_granule)
        .map_err(|_| StartupError::LengthOverflow)?;

    for _ in 0..plan.metadata.region_count {
        push_unique_region(&mut result.regions, current_region)?;
        let (region_bytes, _) = workspace.scan_buffers();
        flash.read_region(current_region, 0, region_bytes.len(), |bytes| {
            region_bytes.copy_from_slice(bytes);
        })?;
        validate_transaction_log_region_bytes(region_bytes, plan.metadata, current_region)?;

        let limit = if current_region == range.end.region_index {
            end_offset
        } else {
            region_size
        };
        let mut next_region = None;

        while offset < limit {
            let next_offset = {
                let (region_bytes, logical_scratch) = workspace.scan_buffers();
                if region_bytes[offset] == plan.metadata.erased_byte {
                    if current_region == range.end.region_index {
                        return Ok(result);
                    }
                    return Err(StartupError::BrokenWalChain {
                        region_index: current_region,
                    });
                }
                if region_bytes[offset] != plan.metadata.wal_record_magic {
                    offset
                        .checked_add(granule)
                        .ok_or(StartupError::LengthOverflow)?
                } else {
                    let decoded = decode_record(
                        &region_bytes[offset..limit],
                        plan.metadata,
                        logical_scratch,
                    )?;
                    let next_offset = offset
                        .checked_add(decoded.encoded_len)
                        .ok_or(StartupError::LengthOverflow)?;
                    match decoded.record {
                        WalRecord::Link {
                            next_region_index, ..
                        } => {
                            next_region = Some(next_region_index);
                        }
                        record => {
                            if result.collection_id.is_none() {
                                if let Some(collection_id) =
                                    transaction_record_collection_id(record)
                                {
                                    if collection_id != CollectionId(0) {
                                        result.collection_id = Some(collection_id);
                                    }
                                }
                            }
                            observe_transaction_log_replay_record(plan, record)?;
                            apply_transaction_replay_record(plan, record, mode)?;
                        }
                    }
                    next_offset
                }
            };
            offset = next_offset;
            if next_region.is_some() {
                break;
            }
        }

        if current_region == range.end.region_index {
            return Ok(result);
        }

        current_region = next_region.ok_or(StartupError::BrokenWalChain {
            region_index: current_region,
        })?;
        ensure_region_index_in_range(current_region, plan.metadata.region_count)?;
        offset = plan.metadata.wal_record_area_offset()?;
    }

    Err(StartupError::BrokenWalChain {
        region_index: current_region,
    })
}

fn scan_transaction_log_to_durable_end<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    start: LogPosition,
) -> Result<TransactionLogRange, StartupError> {
    ensure_region_index_in_range(start.region_index, plan.metadata.region_count)?;
    let mut current_region = start.region_index;
    let mut offset = usize::try_from(start.offset).map_err(|_| StartupError::LengthOverflow)?;
    let region_size =
        usize::try_from(plan.metadata.region_size).map_err(|_| StartupError::LengthOverflow)?;
    let granule = usize::try_from(plan.metadata.wal_write_granule)
        .map_err(|_| StartupError::LengthOverflow)?;

    for _ in 0..plan.metadata.region_count {
        let (region_bytes, _) = workspace.scan_buffers();
        flash.read_region(current_region, 0, region_bytes.len(), |bytes| {
            region_bytes.copy_from_slice(bytes);
        })?;
        validate_transaction_log_region_bytes(region_bytes, plan.metadata, current_region)?;

        while offset < region_size {
            let step = {
                let (region_bytes, logical_scratch) = workspace.scan_buffers();
                let start_byte = region_bytes[offset];
                if start_byte == plan.metadata.erased_byte {
                    return Ok(TransactionLogRange {
                        start,
                        end: LogPosition {
                            region_index: current_region,
                            offset: u32::try_from(offset)
                                .map_err(|_| StartupError::LengthOverflow)?,
                        },
                    });
                }
                if start_byte != plan.metadata.wal_record_magic {
                    return Ok(TransactionLogRange {
                        start,
                        end: LogPosition {
                            region_index: current_region,
                            offset: u32::try_from(offset)
                                .map_err(|_| StartupError::LengthOverflow)?,
                        },
                    });
                }
                let decoded = match decode_record(
                    &region_bytes[offset..region_size],
                    plan.metadata,
                    logical_scratch,
                ) {
                    Ok(decoded) => decoded,
                    Err(_) => {
                        return Ok(TransactionLogRange {
                            start,
                            end: LogPosition {
                                region_index: current_region,
                                offset: u32::try_from(offset)
                                    .map_err(|_| StartupError::LengthOverflow)?,
                            },
                        });
                    }
                };
                let next_offset = offset
                    .checked_add(decoded.encoded_len)
                    .ok_or(StartupError::LengthOverflow)?;
                match decoded.record {
                    WalRecord::Link {
                        next_region_index, ..
                    } => Some((next_region_index, next_offset)),
                    _ => {
                        let _ = granule;
                        None
                    }
                }
            };

            if let Some((next_region, _next_offset)) = step {
                current_region = next_region;
                ensure_region_index_in_range(current_region, plan.metadata.region_count)?;
                offset = plan.metadata.wal_record_area_offset()?;
                break;
            }
            let (region_bytes, logical_scratch) = workspace.scan_buffers();
            let decoded = decode_record(
                &region_bytes[offset..region_size],
                plan.metadata,
                logical_scratch,
            )?;
            offset = offset
                .checked_add(decoded.encoded_len)
                .ok_or(StartupError::LengthOverflow)?;
        }

        if offset >= region_size {
            return Ok(TransactionLogRange {
                start,
                end: LogPosition {
                    region_index: current_region,
                    offset: u32::try_from(region_size).map_err(|_| StartupError::LengthOverflow)?,
                },
            });
        }
    }

    Err(StartupError::BrokenWalChain {
        region_index: current_region,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransactionLogReplayResult {
    collection_id: Option<CollectionId>,
    regions: Vec<u32, MAX_RETAINED_TRANSACTION_LOG_REGIONS>,
}

fn validate_transaction_log_region_bytes(
    region_bytes: &[u8],
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<(), StartupError> {
    let header = Header::decode(&region_bytes[..Header::ENCODED_LEN])?;
    if header.collection_id != CollectionId(0)
        || header.collection_format != TRANSACTION_LOG_V2_FORMAT
    {
        return Err(StartupError::InvalidWalRegion(region_index));
    }
    let prologue_start = Header::ENCODED_LEN;
    let prologue_end = prologue_start
        .checked_add(WalRegionPrologue::ENCODED_LEN)
        .ok_or(StartupError::LengthOverflow)?;
    WalRegionPrologue::decode(
        &region_bytes[prologue_start..prologue_end],
        metadata.region_count,
    )?;
    Ok(())
}

fn observe_transaction_log_replay_record<
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    match record {
        WalRecord::AllocateRegion { region_index, .. } => {
            push_unique_region(&mut plan.transaction_allocations, region_index)?;
        }
        WalRecord::FreeRegion { region_index, .. } => {
            push_unique_region(&mut plan.transaction_frees, region_index)?;
        }
        _ => {}
    }
    Ok(())
}

fn replay_transaction_region_interval<
    const REGION_SIZE: usize,
    IO: FlashIo,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    region_index: u32,
    mut offset: usize,
    end_offset: usize,
    mode: TransactionReplayMode,
) -> Result<(), StartupError> {
    let (region_bytes, _) = workspace.scan_buffers();
    flash.read_region(region_index, 0, region_bytes.len(), |bytes| {
        region_bytes.copy_from_slice(bytes);
    })?;
    let granule = usize::try_from(plan.metadata.wal_write_granule)
        .map_err(|_| StartupError::LengthOverflow)?;

    while offset < end_offset {
        let next_offset = {
            let (region_bytes, logical_scratch) = workspace.scan_buffers();
            if region_bytes[offset] == plan.metadata.erased_byte {
                break;
            }
            if region_bytes[offset] != plan.metadata.wal_record_magic {
                offset
                    .checked_add(granule)
                    .ok_or(StartupError::LengthOverflow)?
            } else {
                let decoded = match decode_record(
                    &region_bytes[offset..end_offset],
                    plan.metadata,
                    logical_scratch,
                ) {
                    Ok(decoded) => decoded,
                    Err(_) => {
                        return Ok(());
                    }
                };
                let next_offset = offset
                    .checked_add(decoded.encoded_len)
                    .ok_or(StartupError::LengthOverflow)?;
                let is_link = matches!(decoded.record, WalRecord::Link { .. });
                if mode == TransactionReplayMode::ApplyRollbackCleanupOnly {
                    observe_transaction_log_replay_record(plan, decoded.record)?;
                }
                apply_transaction_replay_record(plan, decoded.record, mode)?;
                if is_link {
                    return Ok(());
                }
                next_offset
            }
        };
        offset = next_offset;
    }
    Ok(())
}

fn apply_transaction_replay_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
    mode: TransactionReplayMode,
) -> Result<(), StartupError> {
    match mode {
        TransactionReplayMode::ApplyFullInterval => apply_open_replay_record(plan, record),
        TransactionReplayMode::ApplyRollbackCleanupOnly => match record {
            WalRecord::AllocateRegion { .. } | WalRecord::EraseFreeRegionSpan { .. } => {
                apply_open_replay_allocator_record(plan, record)
            }
            WalRecord::FreeRegion { .. } => Ok(()),
            _ => Ok(()),
        },
        TransactionReplayMode::SkipTransactionCollectionData(collection_id) => match record {
            WalRecord::AllocateRegion { .. }
            | WalRecord::FreeRegion { .. }
            | WalRecord::EraseFreeRegionSpan { .. } => {
                apply_open_replay_allocator_record(plan, record)
            }
            _ if wal_record_collection_id(record) == Some(collection_id) => Ok(()),
            _ => apply_open_replay_record(plan, record),
        },
    }
}

fn wal_record_collection_id(record: WalRecord<'_>) -> Option<CollectionId> {
    match record {
        WalRecord::NewCollection { collection_id, .. }
        | WalRecord::Update { collection_id, .. }
        | WalRecord::Snapshot { collection_id, .. }
        | WalRecord::Head { collection_id, .. }
        | WalRecord::DropCollection { collection_id }
        | WalRecord::AddTransactionCollection { collection_id, .. } => Some(collection_id),
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

fn apply_open_replay_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    apply_wal_record(
        plan.metadata,
        record,
        &mut plan.collections,
        &mut plan.free_space,
        &mut plan.ready_region,
    )
}

fn apply_open_replay_allocator_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    match record {
        WalRecord::AllocateRegion {
            region_index,
            allocation_head_after,
        } => apply_wal_record(
            plan.metadata,
            WalRecord::AllocateRegion {
                region_index,
                allocation_head_after,
            },
            &mut plan.collections,
            &mut plan.free_space,
            &mut plan.ready_region,
        ),
        WalRecord::FreeRegion {
            region_index,
            append_tail_after,
        } => apply_wal_record(
            plan.metadata,
            WalRecord::FreeRegion {
                region_index,
                append_tail_after,
            },
            &mut plan.collections,
            &mut plan.free_space,
            &mut plan.ready_region,
        ),
        WalRecord::EraseFreeRegionSpan {
            count,
            ready_boundary_after,
        } => apply_wal_record(
            plan.metadata,
            WalRecord::EraseFreeRegionSpan {
                count,
                ready_boundary_after,
            },
            &mut plan.collections,
            &mut plan.free_space,
            &mut plan.ready_region,
        ),
        _ => Ok(()),
    }
}

#[cfg(test)]
pub(crate) fn finish_open_formatted_store<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<StartupState<MAX_COLLECTIONS>, StartupError> {
    validate_live_collection_types(&plan.collections)?;
    validate_live_region_bases(flash, &plan.collections)?;
    Ok(StartupState {
        metadata: plan.metadata,
        wal_head: plan.wal_head_candidate,
        wal_tail: plan.wal_tail,
        wal_append_offset: plan.wal_append_offset,
        free_space: plan.free_space.clone(),
        ready_region: plan.ready_region,
        max_seen_sequence: plan.max_seen_sequence,
        collections: plan.collections.clone(),
        retained_transaction_logs: plan.retained_transaction_logs.clone(),
        pending_wal_recovery_boundary: plan.pending_wal_recovery_boundary,
    })
}

pub(crate) fn finish_open_formatted_store_into_runtime<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    runtime: &mut StorageRuntime<MAX_COLLECTIONS>,
) -> Result<(), StorageRuntimeError> {
    validate_live_collection_types(&plan.collections)?;
    validate_live_region_bases(flash, &plan.collections)?;
    runtime.replace_from_startup_parts(
        plan.metadata,
        plan.wal_head_candidate,
        plan.wal_tail,
        plan.wal_append_offset,
        plan.free_space.clone(),
        plan.ready_region,
        plan.max_seen_sequence,
        plan.collections.as_slice(),
        plan.retained_transaction_logs.as_slice(),
        plan.pending_wal_recovery_boundary,
    )
}

fn locate_wal_tail<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
) -> Result<(u32, u64), StartupError> {
    let mut max_seen_sequence = 0u64;
    let mut wal_tail = None;
    let mut wal_tail_sequence = 0u64;
    let mut duplicate_tail = false;

    for region_index in 0..metadata.region_count {
        let Ok(header) = flash.read_region(region_index, 0, Header::ENCODED_LEN, Header::decode)?
        else {
            continue;
        };

        max_seen_sequence = max_seen_sequence.max(header.sequence);
        if header.collection_id == CollectionId(0) && header.collection_format == WAL_V1_FORMAT {
            if wal_tail.is_none() || header.sequence > wal_tail_sequence {
                wal_tail = Some(region_index);
                wal_tail_sequence = header.sequence;
                duplicate_tail = false;
            } else if header.sequence == wal_tail_sequence {
                duplicate_tail = true;
            }
        }
    }

    if duplicate_tail {
        return Err(StartupError::DuplicateWalTailSequence(wal_tail_sequence));
    }

    let wal_tail = wal_tail.ok_or(StartupError::NoWalTailCandidate)?;
    Ok((wal_tail, max_seen_sequence))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RotationRecoveryContext {
    wal_head: u32,
    known_tail: u32,
    tail_scan: RegionScanResult,
    cursors: FreeSpaceCursors,
}

fn recover_incomplete_rotation<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    recovery: RotationRecoveryContext,
    max_seen_sequence: &mut u64,
) -> Result<Option<u32>, StartupError> {
    let RotationRecoveryContext {
        wal_head,
        known_tail,
        tail_scan,
        cursors,
    } = recovery;
    let Some(last_valid_record) = tail_scan.last_valid_record else {
        return Ok(None);
    };

    match last_valid_record {
        LastValidRecord::Link {
            next_region_index,
            expected_sequence,
            ..
        } => {
            ensure_region_index_in_range(next_region_index, metadata.region_count)?;
            if has_valid_wal_target(
                flash,
                next_region_index,
                expected_sequence,
                metadata.region_count,
            )? {
                return Ok(None);
            }

            initialize_wal_region::<REGION_SIZE, IO>(
                flash,
                workspace,
                metadata,
                next_region_index,
                expected_sequence,
                wal_head,
                cursors,
            )?;
            *max_seen_sequence = (*max_seen_sequence).max(expected_sequence);
            Ok(Some(next_region_index))
        }
        LastValidRecord::AllocateRegion {
            region_index,
            allocation_head_after,
            aligned_end_offset,
        } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;

            let expected_sequence = max_seen_sequence
                .checked_add(1)
                .ok_or(StartupError::LengthOverflow)?;

            let (physical_scratch, logical_scratch) = workspace.encode_buffers();
            let link_reserve = encoded_record_len(
                WalRecord::Link {
                    next_region_index: region_index,
                    expected_sequence,
                },
                metadata,
                physical_scratch,
                logical_scratch,
            )?;
            let alloc_reserve = encoded_record_len(
                WalRecord::AllocateRegion {
                    region_index,
                    allocation_head_after,
                },
                metadata,
                physical_scratch,
                logical_scratch,
            )?;
            let rotation_reserve = alloc_reserve
                .checked_add(link_reserve)
                .ok_or(StartupError::LengthOverflow)?;
            let remaining = REGION_SIZE
                .checked_sub(aligned_end_offset)
                .ok_or(StartupError::LengthOverflow)?;

            if remaining < link_reserve || remaining >= rotation_reserve {
                return Ok(None);
            }

            let link_record = WalRecord::Link {
                next_region_index: region_index,
                expected_sequence,
            };
            let encoded_len =
                encode_record_into(link_record, metadata, physical_scratch, logical_scratch)?;
            flash.write_region(
                known_tail,
                aligned_end_offset,
                &physical_scratch[..encoded_len],
            )?;
            flash.sync()?;

            initialize_wal_region::<REGION_SIZE, IO>(
                flash,
                workspace,
                metadata,
                region_index,
                expected_sequence,
                wal_head,
                cursors,
            )?;
            *max_seen_sequence = expected_sequence;
            Ok(Some(region_index))
        }
        LastValidRecord::Other { .. } => Ok(None),
    }
}

fn initialize_wal_region<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    region_index: u32,
    sequence: u64,
    wal_head: u32,
    cursors: FreeSpaceCursors,
) -> Result<(), StartupError> {
    ensure_region_index_in_range(region_index, metadata.region_count)?;

    flash.erase_region(region_index)?;
    let target = workspace.committed_write_buffer();
    let prefix_len = crate::disk::encode_log_region_prefix(
        target,
        metadata,
        sequence,
        WAL_V1_FORMAT,
        wal_head,
        cursors,
    )?;
    flash.write_region(region_index, 0, &target[..prefix_len])?;
    flash.sync()?;
    Ok(())
}

fn walk_wal_chain<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    wal_head: u32,
    wal_tail: u32,
    wal_chain: &mut Vec<u32, REGION_COUNT>,
) -> Result<(), StartupError> {
    let mut current = wal_head;
    let chain = wal_chain;
    chain.clear();

    for _visited in 0..metadata.region_count {
        read_strict_wal_region(flash, current, metadata.region_count)?;
        if chain
            .push(current)
            .map_err(|_| StartupError::LengthOverflow)
            .is_err()
        {
            return Err(StartupError::LengthOverflow);
        }

        if current == wal_tail {
            return Ok(());
        }

        let scan = scan_wal_region::<REGION_SIZE, _, _>(
            flash,
            workspace,
            metadata,
            current,
            false,
            |_, _, _| Ok(()),
        )?;
        let LastValidRecord::Link {
            next_region_index,
            expected_sequence,
            ..
        } = scan.last_valid_record.ok_or(StartupError::BrokenWalChain {
            region_index: current,
        })?
        else {
            return Err(StartupError::BrokenWalChain {
                region_index: current,
            });
        };

        ensure_region_index_in_range(next_region_index, metadata.region_count)?;
        if !has_valid_wal_target(
            flash,
            next_region_index,
            expected_sequence,
            metadata.region_count,
        )? {
            return Err(StartupError::InvalidWalLinkTarget {
                region_index: next_region_index,
                expected_sequence,
            });
        }
        current = next_region_index;
    }

    Err(StartupError::BrokenWalChain {
        region_index: current,
    })
}

fn scan_wal_region<const REGION_SIZE: usize, IO: FlashIo, F>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    region_index: u32,
    is_tail: bool,
    mut on_record: F,
) -> Result<RegionScanResult, StartupError>
where
    F: FnMut(&mut IO, usize, WalRecord<'_>) -> Result<(), StartupError>,
{
    ensure_region_index_in_range(region_index, metadata.region_count)?;

    let region_size =
        usize::try_from(metadata.region_size).map_err(|_| StartupError::LengthOverflow)?;
    let granule =
        usize::try_from(metadata.wal_write_granule).map_err(|_| StartupError::LengthOverflow)?;
    let (region_bytes, logical_scratch) = workspace.scan_buffers();
    flash.read_region(region_index, 0, region_bytes.len(), |bytes| {
        region_bytes.copy_from_slice(bytes);
    })?;
    let mut offset = metadata.wal_record_area_offset()?;
    let mut last_valid_record = None;
    let mut wal_head_override = None;
    let mut pending_boundary_open = false;

    while offset < region_size {
        let start_byte = region_bytes[offset];
        if start_byte == metadata.erased_byte {
            if !is_tail && pending_boundary_open {
                return Err(StartupError::BrokenWalChain { region_index });
            }

            return Ok(RegionScanResult {
                append_offset: offset,
                last_valid_record,
                wal_head_override,
                pending_boundary_open,
            });
        }

        if start_byte != metadata.wal_record_magic {
            pending_boundary_open = true;
            offset = offset
                .checked_add(granule)
                .ok_or(StartupError::LengthOverflow)?;
            continue;
        }

        let decoded = match decode_record(&region_bytes[offset..], metadata, logical_scratch) {
            Ok(decoded) => decoded,
            Err(_) => {
                pending_boundary_open = true;
                offset = offset
                    .checked_add(granule)
                    .ok_or(StartupError::LengthOverflow)?;
                continue;
            }
        };

        if pending_boundary_open
            && decoded.record.record_type() != crate::WalRecordType::WalRecovery
        {
            return Err(StartupError::UnexpectedRecordAfterCorruption {
                region_index,
                offset,
            });
        }

        if decoded.record.record_type() == crate::WalRecordType::WalRecovery
            && !pending_boundary_open
        {
            return Err(StartupError::UnexpectedWalRecovery {
                region_index,
                offset,
            });
        }

        if let WalRecord::Head {
            collection_id,
            collection_type,
            region_index: new_wal_head,
        } = decoded.record
        {
            if collection_id == CollectionId(0) {
                if collection_type != CollectionType::WAL_CODE {
                    return Err(StartupError::InvalidWalHeadControlType(collection_type));
                }
                wal_head_override = Some(new_wal_head);
            }
        }

        if decoded.record.record_type() == crate::WalRecordType::WalRecovery {
            pending_boundary_open = false;
        }

        let aligned_end_offset = offset
            .checked_add(decoded.encoded_len)
            .ok_or(StartupError::LengthOverflow)?;
        last_valid_record = Some(match decoded.record {
            WalRecord::AllocateRegion {
                region_index,
                allocation_head_after,
            } => LastValidRecord::AllocateRegion {
                region_index,
                allocation_head_after,
                aligned_end_offset,
            },
            WalRecord::Link {
                next_region_index,
                expected_sequence,
            } => LastValidRecord::Link {
                next_region_index,
                expected_sequence,
                aligned_end_offset,
            },
            _ => LastValidRecord::Other { aligned_end_offset },
        });

        on_record(flash, offset, decoded.record)?;
        offset = aligned_end_offset;
    }

    if !is_tail && pending_boundary_open {
        return Err(StartupError::BrokenWalChain { region_index });
    }

    Ok(RegionScanResult {
        append_offset: region_size,
        last_valid_record,
        wal_head_override,
        pending_boundary_open,
    })
}

fn free_queue_position_at_or_before(
    position: FreeQueuePosition,
    boundary: FreeQueuePosition,
) -> bool {
    position.region_index == boundary.region_index && position.entry_index <= boundary.entry_index
}

pub(crate) fn apply_wal_record<const MAX_COLLECTIONS: usize>(
    metadata: StorageMetadata,
    record: WalRecord<'_>,
    collections: &mut Vec<StartupCollection, MAX_COLLECTIONS>,
    free_space: &mut FreeSpaceState,
    ready_region: &mut Option<u32>,
) -> Result<(), StartupError> {
    match record {
        WalRecord::NewCollection {
            collection_id,
            collection_type,
        } => {
            if collection_id == CollectionId(0) {
                return Err(StartupError::ReservedCollectionId(collection_id));
            }
            if find_collection(collections.as_slice(), collection_id).is_some() {
                return Err(StartupError::DuplicateCollection(collection_id));
            }

            collections
                .push(StartupCollection {
                    collection_id,
                    collection_type: Some(collection_type),
                    basis: StartupCollectionBasis::Empty,
                    pending_update_count: 0,
                })
                .map_err(|_| StartupError::TooManyTrackedCollections)?;
        }
        WalRecord::Update {
            collection_id,
            payload: _,
        } => {
            let Some(collection) = find_collection_mut(collections, collection_id) else {
                return Ok(());
            };
            if collection.basis == StartupCollectionBasis::Dropped {
                return Err(StartupError::DroppedCollection(collection_id));
            }
            collection.pending_update_count = collection
                .pending_update_count
                .checked_add(1)
                .ok_or(StartupError::LengthOverflow)?;
        }
        WalRecord::Snapshot {
            collection_id,
            collection_type,
            payload: _,
        } => {
            if collection_id == CollectionId(0) {
                return Err(StartupError::ReservedCollectionId(collection_id));
            }

            match find_collection_mut(collections, collection_id) {
                Some(collection) => {
                    if collection.basis == StartupCollectionBasis::Dropped {
                        return Err(StartupError::DroppedCollection(collection_id));
                    }
                    if collection.collection_type != Some(collection_type) {
                        return Err(StartupError::CollectionTypeMismatch {
                            collection_id,
                            expected: collection.collection_type.unwrap_or(collection_type),
                            actual: collection_type,
                        });
                    }
                    collection.basis = StartupCollectionBasis::WalSnapshot;
                    collection.pending_update_count = 0;
                }
                None => {
                    collections
                        .push(StartupCollection {
                            collection_id,
                            collection_type: Some(collection_type),
                            basis: StartupCollectionBasis::WalSnapshot,
                            pending_update_count: 0,
                        })
                        .map_err(|_| StartupError::TooManyTrackedCollections)?;
                }
            }
        }
        WalRecord::AllocateRegion {
            region_index,
            allocation_head_after,
        } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;
            let current_allocation_head = free_space.allocation_head_position();
            if allocation_head_after == current_allocation_head {
                return Ok(());
            }
            if free_queue_position_at_or_before(allocation_head_after, current_allocation_head) {
                return Ok(());
            }
            free_space.apply_allocate(region_index, allocation_head_after)?;
        }
        WalRecord::Head {
            collection_id,
            collection_type,
            region_index,
        } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;

            if collection_id == CollectionId(0) {
                if collection_type != CollectionType::WAL_CODE {
                    return Err(StartupError::InvalidWalHeadControlType(collection_type));
                }
                return Ok(());
            }

            match find_collection_mut(collections, collection_id) {
                Some(collection) => {
                    if collection.basis == StartupCollectionBasis::Dropped {
                        return Err(StartupError::DroppedCollection(collection_id));
                    }
                    if collection.collection_type != Some(collection_type) {
                        return Err(StartupError::CollectionTypeMismatch {
                            collection_id,
                            expected: collection.collection_type.unwrap_or(collection_type),
                            actual: collection_type,
                        });
                    }
                    collection.basis = StartupCollectionBasis::Region(region_index);
                    collection.pending_update_count = 0;
                }
                None => {
                    collections
                        .push(StartupCollection {
                            collection_id,
                            collection_type: Some(collection_type),
                            basis: StartupCollectionBasis::Region(region_index),
                            pending_update_count: 0,
                        })
                        .map_err(|_| StartupError::TooManyTrackedCollections)?;
                }
            }

            if *ready_region == Some(region_index) {
                *ready_region = None;
            }
        }
        WalRecord::DropCollection { collection_id } => {
            if collection_id == CollectionId(0) {
                return Err(StartupError::ReservedCollectionId(collection_id));
            }

            match find_collection_mut(collections, collection_id) {
                Some(collection) => {
                    if collection.basis == StartupCollectionBasis::Dropped {
                        return Err(StartupError::DroppedCollection(collection_id));
                    }
                    collection.collection_type = None;
                    collection.basis = StartupCollectionBasis::Dropped;
                    collection.pending_update_count = 0;
                }
                None => {
                    collections
                        .push(StartupCollection {
                            collection_id,
                            collection_type: None,
                            basis: StartupCollectionBasis::Dropped,
                            pending_update_count: 0,
                        })
                        .map_err(|_| StartupError::TooManyTrackedCollections)?;
                }
            }
        }
        WalRecord::Link {
            next_region_index,
            expected_sequence: _,
        } => {
            ensure_region_index_in_range(next_region_index, metadata.region_count)?;
            if *ready_region == Some(next_region_index) {
                *ready_region = None;
            }
        }
        WalRecord::FreeRegion {
            region_index,
            append_tail_after,
        } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;
            if free_queue_position_at_or_before(
                append_tail_after,
                free_space.append_tail_position(),
            ) {
                return Ok(());
            }
            free_space.apply_free(region_index, append_tail_after)?;
        }
        WalRecord::EraseFreeRegionSpan {
            count,
            ready_boundary_after,
        } => {
            if free_queue_position_at_or_before(
                ready_boundary_after,
                free_space.ready_boundary_position(),
            ) {
                return Ok(());
            }
            free_space.apply_erase(count, ready_boundary_after)?;
        }
        WalRecord::WalRecovery => {}
        WalRecord::BeginInlineTransaction { .. }
        | WalRecord::CommitInlineTransaction { .. }
        | WalRecord::RollbackInlineTransaction { .. }
        | WalRecord::BeginTransaction { .. }
        | WalRecord::CommitTransaction { .. }
        | WalRecord::TransactionFinished { .. }
        | WalRecord::RollbackTransaction { .. }
        | WalRecord::AddTransactionCollection { .. } => {}
    }

    Ok(())
}

fn read_region_header<IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
) -> Result<Header, StartupError> {
    flash
        .read_region(region_index, 0, Header::ENCODED_LEN, Header::decode)?
        .map_err(StartupError::from)
}

fn read_wal_prologue<IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
    region_count: u32,
) -> Result<WalRegionPrologue, StartupError> {
    flash
        .read_region(
            region_index,
            Header::ENCODED_LEN,
            WalRegionPrologue::ENCODED_LEN,
            |bytes| WalRegionPrologue::decode(bytes, region_count),
        )?
        .map_err(StartupError::from)
}

fn read_strict_wal_region<IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
    region_count: u32,
) -> Result<Header, StartupError> {
    ensure_region_index_in_range(region_index, region_count)?;
    let header = read_region_header(flash, region_index)?;
    if header.collection_id != CollectionId(0) || header.collection_format != WAL_V1_FORMAT {
        return Err(StartupError::InvalidWalRegion(region_index));
    }

    let _prologue = read_wal_prologue(flash, region_index, region_count)?;
    Ok(header)
}

fn has_valid_wal_target<IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
    expected_sequence: u64,
    region_count: u32,
) -> Result<bool, StartupError> {
    ensure_region_index_in_range(region_index, region_count)?;

    let Ok(header) = flash.read_region(region_index, 0, Header::ENCODED_LEN, Header::decode)?
    else {
        return Ok(false);
    };
    if header.collection_id != CollectionId(0)
        || header.collection_format != WAL_V1_FORMAT
        || header.sequence != expected_sequence
    {
        return Ok(false);
    }

    Ok(flash
        .read_region(
            region_index,
            Header::ENCODED_LEN,
            WalRegionPrologue::ENCODED_LEN,
            |bytes| WalRegionPrologue::decode(bytes, region_count),
        )?
        .is_ok())
}

fn validate_live_collection_types(collections: &[StartupCollection]) -> Result<(), StartupError> {
    for collection in collections {
        if collection.basis == StartupCollectionBasis::Dropped {
            continue;
        }

        let Some(collection_type) = collection.collection_type else {
            return Err(StartupError::UnsupportedLiveCollectionType(0xffff));
        };

        if !matches!(
            collection_type,
            CollectionType::CHANNEL_CODE
                | CollectionType::MAP_CODE
                | CollectionType::OBJECT_LOG_CODE
        ) {
            return Err(StartupError::UnsupportedLiveCollectionType(collection_type));
        }
    }

    Ok(())
}

fn validate_live_region_bases<IO: FlashIo>(
    flash: &mut IO,
    collections: &[StartupCollection],
) -> Result<(), StartupError> {
    for collection in collections {
        let StartupCollectionBasis::Region(region_index) = collection.basis else {
            continue;
        };

        let region_header = read_region_header(flash, region_index)?;
        if region_header.collection_id != collection.collection_id {
            return Err(StartupError::InvalidCommittedRegionHead {
                collection_id: collection.collection_id,
                region_index,
            });
        }
    }

    Ok(())
}

fn ensure_region_index_in_range(region_index: u32, region_count: u32) -> Result<(), StartupError> {
    if region_index >= region_count {
        return Err(StartupError::InvalidRegionReference(region_index));
    }
    Ok(())
}

fn find_collection(
    collections: &[StartupCollection],
    collection_id: CollectionId,
) -> Option<usize> {
    collections
        .iter()
        .position(|collection| collection.collection_id == collection_id)
}

fn find_collection_mut<const MAX_COLLECTIONS: usize>(
    collections: &mut Vec<StartupCollection, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Option<&mut StartupCollection> {
    let index = find_collection(collections.as_slice(), collection_id)?;
    collections.get_mut(index)
}

#[cfg(test)]
#[allow(unused_mut, unused_variables)]
mod tests;
