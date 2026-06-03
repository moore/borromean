use heapless::Vec;

use crate::disk::{
    encode_wal_region_prefix, DiskError, FreePointerFooter, Header, StorageMetadata,
    WalRegionPrologue, WAL_V1_FORMAT,
};
use crate::flash_io::FlashIo;
use crate::flash_io::StorageIoError;
use crate::storage::{FreeRegionPreparation, StorageRuntime, StorageRuntimeError};
use crate::wal_record::{
    decode_record, encode_record_into, encoded_record_len, WalRecord, WalRecordError,
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
    /// An `alloc_begin` record did not match the tracked free-list head.
    InvalidAllocBegin {
        /// Region named by the bad record.
        region_index: u32,
        /// Free-list head replay expected at that point.
        last_free_list_head: Option<u32>,
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
        /// Bytes that would remain after the `alloc_begin` record.
        remaining_after: usize,
        /// Bytes required for the full rotation sequence.
        rotation_reserve: usize,
    },
    /// A region being returned to the free list did not have an unwritten footer.
    FreeRegionFooterNotUnwritten {
        /// Region whose free-pointer footer was already written.
        region_index: u32,
    },
    /// Replay found a live collection type not supported by this build.
    UnsupportedLiveCollectionType(u16),
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
    last_free_list_head: Option<u32>,
    free_list_tail: Option<u32>,
    ready_region: Option<u32>,
    max_seen_sequence: u64,
    collections: Vec<StartupCollection, MAX_COLLECTIONS>,
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

    /// Returns the current free-list head, if any.
    pub fn last_free_list_head(&self) -> Option<u32> {
        self.last_free_list_head
    }

    /// Returns the current free-list tail, if any.
    pub fn free_list_tail(&self) -> Option<u32> {
        self.free_list_tail
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
    AllocBegin {
        collection_id: CollectionId,
        region_index: u32,
        free_list_head_after: Option<u32>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpenTransactionReplay {
    collection_id: CollectionId,
    start: WalReplayPosition,
    commit_seen: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionReplayMode {
    ApplyFullInterval,
    SkipTransactionCollectionData(CollectionId),
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
    last_free_list_head: Option<u32>,
    ready_region: Option<u32>,
    wal_append_offset: usize,
    pending_wal_recovery_boundary: bool,
    transaction_original_collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    transaction_allocations: Vec<u32, REGION_COUNT>,
    transaction_frees: Vec<u32, REGION_COUNT>,
    transaction_cleanup_regions: Vec<u32, REGION_COUNT>,
    transaction_old_regions: Vec<u32, REGION_COUNT>,
    transaction_new_regions: Vec<u32, REGION_COUNT>,
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
            last_free_list_head: None,
            ready_region: None,
            wal_append_offset: 0,
            pending_wal_recovery_boundary: false,
            transaction_original_collections: Vec::new(),
            transaction_allocations: Vec::new(),
            transaction_frees: Vec::new(),
            transaction_cleanup_regions: Vec::new(),
            transaction_old_regions: Vec::new(),
            transaction_new_regions: Vec::new(),
        }
    }

    fn reset(
        &mut self,
        metadata: StorageMetadata,
        wal_head_candidate: u32,
        wal_tail: u32,
        tail_scan: RegionScanResult,
        max_seen_sequence: u64,
    ) -> Result<(), StartupError> {
        self.metadata = metadata;
        self.wal_head_candidate = wal_head_candidate;
        self.wal_tail = wal_tail;
        self.tail_scan = tail_scan;
        self.max_seen_sequence = max_seen_sequence;
        self.wal_chain.clear();
        self.collections.clear();
        self.last_free_list_head = if metadata.region_count >= 2 {
            Some(1)
        } else {
            None
        };
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
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.wal_chain.clear();
        self.collections.clear();
        self.transaction_original_collections.clear();
        self.transaction_allocations.clear();
        self.transaction_frees.clear();
        self.transaction_cleanup_regions.clear();
        self.transaction_old_regions.clear();
        self.transaction_new_regions.clear();
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

    let (known_tail, max_seen_sequence) = locate_wal_tail::<REGION_SIZE, _>(flash, metadata)?;
    let tail_prologue = read_wal_prologue(flash, known_tail, metadata.region_count)?;
    let mut wal_head_candidate = tail_prologue.wal_head_region_index;
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
    )?;
    if wal_head_candidate != 0 {
        plan.last_free_list_head = discover_free_list_head_from_footers(flash, metadata)?;
    }
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
        plan.wal_head_candidate,
        plan.wal_tail,
        plan.tail_scan,
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
    let mut open_transaction = None;

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
            &mut open_transaction,
        )?;
    }

    if let Some(open_transaction) = open_transaction {
        recover_unfinished_transaction::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            open_transaction,
        )?;
        return Ok(ReplayWalChainOutcome::RecoveredTransaction);
    }

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
    open_transaction: &mut Option<OpenTransactionReplay>,
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
                    let step = classify_replay_record(
                        plan,
                        open_transaction,
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
                mode,
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
                offset = end_exclusive_offset;
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
enum ReplayStep {
    Advance {
        next_offset: usize,
    },
    ReplayTransaction {
        start: WalReplayPosition,
        end: WalReplayPosition,
        end_exclusive_offset: usize,
        mode: TransactionReplayMode,
    },
}

fn classify_replay_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    open_transaction: &mut Option<OpenTransactionReplay>,
    current_position: WalReplayPosition,
    aligned_end_offset: usize,
    record: WalRecord<'_>,
) -> Result<ReplayStep, StartupError> {
    let next = ReplayStep::Advance {
        next_offset: aligned_end_offset,
    };

    let Some(transaction) = open_transaction.as_mut() else {
        if let WalRecord::BeginTransaction { collection_id } = record {
            if collection_id == CollectionId(0) {
                return Err(StartupError::ReservedCollectionId(collection_id));
            }
            plan.capture_transaction_original_collections()?;
            *open_transaction = Some(OpenTransactionReplay {
                collection_id,
                start: current_position,
                commit_seen: false,
            });
            return Ok(next);
        }

        apply_open_replay_record(plan, record)?;
        return Ok(next);
    };

    match record {
        WalRecord::BeginTransaction { collection_id } => {
            let _ = collection_id;
            return Err(StartupError::NestedTransaction(transaction.collection_id));
        }
        WalRecord::CommitTransaction { collection_id } => {
            ensure_transaction_marker_matches(transaction.collection_id, collection_id)?;
            transaction.commit_seen = true;
        }
        WalRecord::TransactionFinished { collection_id } => {
            ensure_transaction_marker_matches(transaction.collection_id, collection_id)?;
            let start = transaction.start;
            *open_transaction = None;
            plan.clear_transaction_recovery_scratch();
            return Ok(ReplayStep::ReplayTransaction {
                start,
                end: current_position,
                end_exclusive_offset: aligned_end_offset,
                mode: TransactionReplayMode::ApplyFullInterval,
            });
        }
        WalRecord::RollbackTransaction { collection_id } => {
            ensure_transaction_marker_matches(transaction.collection_id, collection_id)?;
            let start = transaction.start;
            *open_transaction = None;
            plan.clear_transaction_recovery_scratch();
            return Ok(ReplayStep::ReplayTransaction {
                start,
                end: current_position,
                end_exclusive_offset: aligned_end_offset,
                mode: TransactionReplayMode::SkipTransactionCollectionData(collection_id),
            });
        }
        _ => {
            observe_transaction_recovery_record(plan, transaction.collection_id, record)?;
        }
    }

    Ok(next)
}

fn observe_transaction_recovery_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    transaction_collection_id: CollectionId,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    match record {
        WalRecord::AllocBegin {
            collection_id,
            region_index,
            ..
        } if collection_id == transaction_collection_id => {
            push_unique_region(&mut plan.transaction_allocations, region_index)?;
        }
        WalRecord::FreeRegion {
            collection_id,
            region_index,
        } if collection_id == transaction_collection_id => {
            push_unique_region(&mut plan.transaction_frees, region_index)?;
        }
        _ => {}
    }
    Ok(())
}

fn ensure_transaction_marker_matches(
    expected: CollectionId,
    actual: CollectionId,
) -> Result<(), StartupError> {
    if actual == CollectionId(0) {
        return Err(StartupError::ReservedCollectionId(actual));
    }
    if actual != expected {
        return Err(StartupError::TransactionMismatch { expected, actual });
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
    let tail_chain_index =
        plan.wal_chain
            .len()
            .checked_sub(1)
            .ok_or(StartupError::BrokenWalChain {
                region_index: plan.wal_tail,
            })?;
    let tail_position = WalReplayPosition {
        chain_index: tail_chain_index,
        region_index: plan.wal_tail,
        offset: plan.wal_append_offset,
    };

    if open_transaction.commit_seen {
        replay_transaction_interval::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            open_transaction.start,
            tail_position,
            plan.wal_append_offset,
            TransactionReplayMode::ApplyFullInterval,
        )?;
        derive_transaction_cleanup_regions::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            open_transaction.collection_id,
        )?;
        append_missing_transaction_cleanup_frees::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            open_transaction.collection_id,
        )?;
        append_recovery_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            WalRecord::TransactionFinished {
                collection_id: open_transaction.collection_id,
            },
        )?;
    } else {
        replay_transaction_interval::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            open_transaction.start,
            tail_position,
            plan.wal_append_offset,
            TransactionReplayMode::SkipTransactionCollectionData(open_transaction.collection_id),
        )?;
        append_recovered_transaction_allocation_frees::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(flash, workspace, plan, open_transaction.collection_id)?;
        append_recovery_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            WalRecord::RollbackTransaction {
                collection_id: open_transaction.collection_id,
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
) -> Result<(), StartupError> {
    let mut index = 0usize;
    while index < plan.transaction_allocations.len() {
        let region_index = plan.transaction_allocations[index];
        if plan.transaction_frees.contains(&region_index) {
            index += 1;
            continue;
        }
        append_recovery_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            collection_id,
            region_index,
            FreeRegionPreparation::EraseToUnwrittenFooter,
        )?;
        push_unique_region(&mut plan.transaction_frees, region_index)?;
        index += 1;
    }
    Ok(())
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
    let mut index = 0usize;
    while index < plan.transaction_cleanup_regions.len() {
        let region_index = plan.transaction_cleanup_regions[index];
        if plan.transaction_frees.contains(&region_index) {
            index += 1;
            continue;
        }
        append_recovery_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            workspace,
            plan,
            collection_id,
            region_index,
            FreeRegionPreparation::RequireUnwrittenFooter,
        )?;
        push_unique_region(&mut plan.transaction_frees, region_index)?;
        index += 1;
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
        Some(other) => Err(StartupError::UnsupportedLiveCollectionType(other)),
        None => Ok(()),
    }
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
    collection_id: CollectionId,
    region_index: u32,
    preparation: FreeRegionPreparation,
) -> Result<(), StartupError> {
    append_recovery_record_room_with_rotation::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan,
        WalRecord::FreeRegion {
            collection_id,
            region_index,
        },
    )?;
    let already_on_free_list = region_is_on_free_list_startup(
        flash,
        plan.metadata,
        plan.last_free_list_head,
        region_index,
    )?;
    if !already_on_free_list {
        prepare_region_for_recovery_free::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            flash,
            plan,
            region_index,
            preparation,
        )?;
    }
    write_recovery_record_and_apply::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        flash,
        workspace,
        plan,
        WalRecord::FreeRegion {
            collection_id,
            region_index,
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
    flash: &mut IO,
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

    let Some(next_region_index) = plan.last_free_list_head else {
        return Ok(remaining_after != 0 && !matches!(record, WalRecord::AllocBegin { .. }));
    };
    let free_list_head_after = read_free_pointer_successor_startup::<REGION_SIZE, REGION_COUNT, IO>(
        flash,
        plan.metadata,
        next_region_index,
    )?;
    let reserves = recovery_rotation_reserves::<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>(
        workspace,
        plan,
        next_region_index,
        free_list_head_after,
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
        .last_free_list_head
        .ok_or(StartupError::NoFreeRegionForTransactionRecovery)?;
    let free_list_head_after = read_free_pointer_successor_startup::<REGION_SIZE, REGION_COUNT, IO>(
        flash,
        plan.metadata,
        next_region_index,
    )?;
    let reserves = recovery_rotation_reserves::<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>(
        workspace,
        plan,
        next_region_index,
        free_list_head_after,
    )?;
    let remaining_after = REGION_SIZE
        .checked_sub(
            plan.wal_append_offset
                .checked_add(reserves.alloc_begin_len)
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
        WalRecord::AllocBegin {
            collection_id: CollectionId(0),
            region_index: next_region_index,
            free_list_head_after,
        },
    )?;
    plan.wal_append_offset = plan
        .wal_append_offset
        .checked_add(reserves.alloc_begin_len)
        .ok_or(StartupError::LengthOverflow)?;
    apply_open_replay_record(
        plan,
        WalRecord::AllocBegin {
            collection_id: CollectionId(0),
            region_index: next_region_index,
            free_list_head_after,
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
    )?;
    plan.wal_tail = next_region_index;
    plan.wal_append_offset = plan.metadata.wal_record_area_offset()?;
    plan.ready_region = None;
    plan.max_seen_sequence = expected_sequence;
    plan.pending_wal_recovery_boundary = false;
    if plan.last_free_list_head.is_none() {
        plan.transaction_frees.clear();
    }
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
    let recovery_len =
        encode_record_into(WalRecord::WalRecovery, plan.metadata, physical, logical)?;
    let end = plan
        .wal_append_offset
        .checked_add(granule)
        .and_then(|offset| offset.checked_add(recovery_len))
        .ok_or(StartupError::LengthOverflow)?;
    if granule == 0 || granule > physical.len() || end > REGION_SIZE {
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
    alloc_begin_len: usize,
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
    free_list_head_after: Option<u32>,
) -> Result<RecoveryRotationReserves, StartupError> {
    let expected_sequence = plan
        .max_seen_sequence
        .checked_add(1)
        .ok_or(StartupError::LengthOverflow)?;
    let (physical, logical) = workspace.encode_buffers();
    let alloc_begin_len = encode_record_into(
        WalRecord::AllocBegin {
            collection_id: CollectionId(0),
            region_index: next_region_index,
            free_list_head_after,
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
    let rotation_reserve = alloc_begin_len
        .checked_add(link_reserve)
        .ok_or(StartupError::LengthOverflow)?;
    Ok(RecoveryRotationReserves {
        alloc_begin_len,
        link_reserve,
        rotation_reserve,
    })
}

fn prepare_region_for_recovery_free<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
>(
    flash: &mut IO,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    region_index: u32,
    preparation: FreeRegionPreparation,
) -> Result<(), StartupError> {
    let free_list_tail =
        reconstruct_free_list_tail(flash, plan.metadata, plan.last_free_list_head)?;
    match preparation {
        FreeRegionPreparation::RequireUnwrittenFooter => {
            ensure_free_pointer_footer_unwritten_startup::<REGION_SIZE, IO>(
                flash,
                plan.metadata,
                region_index,
            )?;
        }
        FreeRegionPreparation::EraseToUnwrittenFooter => {
            flash.erase_region(region_index)?;
            flash.sync()?;
        }
    }

    if let Some(free_list_tail) = free_list_tail {
        write_free_pointer_footer_startup::<REGION_SIZE, IO>(
            flash,
            plan.metadata,
            free_list_tail,
            Some(region_index),
        )?;
        flash.sync()?;
    }

    Ok(())
}

fn ensure_free_pointer_footer_unwritten_startup<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<(), StartupError> {
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StartupError::LengthOverflow)?
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(StartupError::LengthOverflow)?;
    let unwritten = flash.read_region(
        region_index,
        footer_offset,
        FreePointerFooter::ENCODED_LEN,
        |bytes| bytes.iter().all(|byte| *byte == metadata.erased_byte),
    )?;
    if unwritten {
        Ok(())
    } else {
        Err(StartupError::FreeRegionFooterNotUnwritten { region_index })
    }
}

fn read_free_pointer_successor_startup<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<Option<u32>, StartupError> {
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StartupError::LengthOverflow)?
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(StartupError::LengthOverflow)?;
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
        .map_err(StartupError::from)?;
    Ok(footer.next_tail)
}

fn write_free_pointer_footer_startup<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
    next_tail: Option<u32>,
) -> Result<(), StartupError> {
    ensure_region_index_in_range(region_index, metadata.region_count)?;
    if let Some(next_tail) = next_tail {
        ensure_region_index_in_range(next_tail, metadata.region_count)?;
    }
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StartupError::LengthOverflow)?
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(StartupError::LengthOverflow)?;
    let footer = FreePointerFooter { next_tail };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer.encode_into(&mut footer_bytes, metadata.erased_byte)?;
    flash.write_region(region_index, footer_offset, &footer_bytes)?;
    Ok(())
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

    while offset < end_offset {
        let next_offset = {
            let (region_bytes, logical_scratch) = workspace.scan_buffers();
            if region_bytes[offset] == plan.metadata.erased_byte {
                break;
            }
            let decoded = decode_record(
                &region_bytes[offset..end_offset],
                plan.metadata,
                logical_scratch,
            )?;
            let next_offset = offset
                .checked_add(decoded.encoded_len)
                .ok_or(StartupError::LengthOverflow)?;
            let is_link = matches!(decoded.record, WalRecord::Link { .. });
            apply_transaction_replay_record(plan, decoded.record, mode)?;
            if is_link {
                return Ok(());
            }
            next_offset
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
        TransactionReplayMode::SkipTransactionCollectionData(collection_id) => match record {
            WalRecord::AllocBegin { .. } | WalRecord::FreeRegion { .. } => {
                apply_open_replay_allocator_record(plan, record)
            }
            WalRecord::BeginTransaction { collection_id }
            | WalRecord::CommitTransaction { collection_id }
            | WalRecord::TransactionFinished { collection_id }
            | WalRecord::RollbackTransaction { collection_id } => {
                if collection_id == CollectionId(0) {
                    return Err(StartupError::ReservedCollectionId(collection_id));
                }
                Ok(())
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
        | WalRecord::AllocBegin { collection_id, .. }
        | WalRecord::Head { collection_id, .. }
        | WalRecord::DropCollection { collection_id }
        | WalRecord::FreeRegion { collection_id, .. }
        | WalRecord::BeginTransaction { collection_id }
        | WalRecord::CommitTransaction { collection_id }
        | WalRecord::TransactionFinished { collection_id }
        | WalRecord::RollbackTransaction { collection_id } => Some(collection_id),
        WalRecord::Link { .. } | WalRecord::WalRecovery => None,
    }
}

fn apply_open_replay_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    if plan.wal_head_candidate != 0
        && matches!(
            record,
            WalRecord::AllocBegin { .. } | WalRecord::FreeRegion { .. }
        )
    {
        return Ok(());
    }
    apply_wal_record(
        plan.metadata,
        record,
        &mut plan.collections,
        &mut plan.last_free_list_head,
        &mut plan.ready_region,
    )
}

fn apply_open_replay_allocator_record<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>(
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    record: WalRecord<'_>,
) -> Result<(), StartupError> {
    if plan.wal_head_candidate != 0 {
        return Ok(());
    }

    match record {
        WalRecord::AllocBegin {
            collection_id,
            region_index,
            free_list_head_after,
        } => apply_wal_record(
            plan.metadata,
            WalRecord::AllocBegin {
                collection_id,
                region_index,
                free_list_head_after,
            },
            &mut plan.collections,
            &mut plan.last_free_list_head,
            &mut plan.ready_region,
        ),
        WalRecord::FreeRegion {
            collection_id,
            region_index,
        } => apply_wal_record(
            plan.metadata,
            WalRecord::FreeRegion {
                collection_id,
                region_index,
            },
            &mut plan.collections,
            &mut plan.last_free_list_head,
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
    let free_list_tail =
        reconstruct_free_list_tail(flash, plan.metadata, plan.last_free_list_head)?;
    Ok(StartupState {
        metadata: plan.metadata,
        wal_head: plan.wal_head_candidate,
        wal_tail: plan.wal_tail,
        wal_append_offset: plan.wal_append_offset,
        last_free_list_head: plan.last_free_list_head,
        free_list_tail,
        ready_region: plan.ready_region,
        max_seen_sequence: plan.max_seen_sequence,
        collections: plan.collections.clone(),
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
    let free_list_tail =
        reconstruct_free_list_tail(flash, plan.metadata, plan.last_free_list_head)?;
    runtime.replace_from_startup_parts(
        plan.metadata,
        plan.wal_head_candidate,
        plan.wal_tail,
        plan.wal_append_offset,
        plan.last_free_list_head,
        free_list_tail,
        plan.ready_region,
        plan.max_seen_sequence,
        plan.collections.as_slice(),
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

fn recover_incomplete_rotation<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    wal_head: u32,
    known_tail: u32,
    tail_scan: RegionScanResult,
    max_seen_sequence: &mut u64,
) -> Result<Option<u32>, StartupError> {
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
            )?;
            *max_seen_sequence = (*max_seen_sequence).max(expected_sequence);
            Ok(Some(next_region_index))
        }
        LastValidRecord::AllocBegin {
            collection_id,
            region_index,
            free_list_head_after,
            aligned_end_offset,
        } => {
            if collection_id != CollectionId(0) {
                return Ok(None);
            }
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
                WalRecord::AllocBegin {
                    collection_id: CollectionId(0),
                    region_index,
                    free_list_head_after,
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
) -> Result<(), StartupError> {
    ensure_region_index_in_range(region_index, metadata.region_count)?;

    flash.erase_region(region_index)?;
    let target = workspace.committed_write_buffer();
    let prefix_len =
        encode_wal_region_prefix(target, metadata, sequence, wal_head, metadata.erased_byte)?;
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
            WalRecord::AllocBegin {
                collection_id,
                region_index,
                free_list_head_after,
            } => LastValidRecord::AllocBegin {
                collection_id,
                region_index,
                free_list_head_after,
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

pub(crate) fn apply_wal_record<const MAX_COLLECTIONS: usize>(
    metadata: StorageMetadata,
    record: WalRecord<'_>,
    collections: &mut Vec<StartupCollection, MAX_COLLECTIONS>,
    last_free_list_head: &mut Option<u32>,
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
        WalRecord::AllocBegin {
            collection_id,
            region_index,
            free_list_head_after,
        } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;
            if let Some(next_head) = free_list_head_after {
                ensure_region_index_in_range(next_head, metadata.region_count)?;
            }

            if collection_id == CollectionId(0) {
                if let Some(existing) = ready_region {
                    return Err(StartupError::DoubleReadyRegion {
                        existing: *existing,
                        next: region_index,
                    });
                }
            }

            if *last_free_list_head != Some(region_index) {
                return Err(StartupError::InvalidAllocBegin {
                    region_index,
                    last_free_list_head: *last_free_list_head,
                });
            }

            *last_free_list_head = free_list_head_after;
            if collection_id == CollectionId(0) {
                *ready_region = Some(region_index);
            }
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
            collection_id: _,
            region_index,
        } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;
            if last_free_list_head.is_none() {
                *last_free_list_head = Some(region_index);
            }
        }
        WalRecord::WalRecovery => {}
        WalRecord::BeginTransaction { collection_id }
        | WalRecord::CommitTransaction { collection_id }
        | WalRecord::TransactionFinished { collection_id }
        | WalRecord::RollbackTransaction { collection_id } => {
            if collection_id == CollectionId(0) {
                return Err(StartupError::ReservedCollectionId(collection_id));
            }
        }
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
            CollectionType::CHANNEL_CODE | CollectionType::MAP_CODE
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

fn reconstruct_free_list_tail<IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    free_list_head: Option<u32>,
) -> Result<Option<u32>, StartupError> {
    let Some(mut current_region) = free_list_head else {
        return Ok(None);
    };

    let footer_offset =
        usize::try_from(metadata.region_size).map_err(|_| StartupError::InvalidFreeListChain {
            region_index: current_region,
        })? - FreePointerFooter::ENCODED_LEN;
    for _visited in 0..metadata.region_count {
        let footer = flash
            .read_region(
                current_region,
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
            .map_err(|_| StartupError::InvalidFreeListChain {
                region_index: current_region,
            })?;

        match footer.next_tail {
            Some(next_region) => current_region = next_region,
            None => return Ok(Some(current_region)),
        }
    }

    Err(StartupError::InvalidFreeListChain {
        region_index: current_region,
    })
}

fn region_is_on_free_list_startup<IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    free_list_head: Option<u32>,
    target_region_index: u32,
) -> Result<bool, StartupError> {
    let Some(mut current_region) = free_list_head else {
        return Ok(false);
    };

    let footer_offset =
        usize::try_from(metadata.region_size).map_err(|_| StartupError::InvalidFreeListChain {
            region_index: current_region,
        })? - FreePointerFooter::ENCODED_LEN;
    for _visited in 0..metadata.region_count {
        if current_region == target_region_index {
            return Ok(true);
        }
        let footer = flash
            .read_region(
                current_region,
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
            .map_err(|_| StartupError::InvalidFreeListChain {
                region_index: current_region,
            })?;

        match footer.next_tail {
            Some(next_region) => current_region = next_region,
            None => return Ok(false),
        }
    }

    Err(StartupError::InvalidFreeListChain {
        region_index: current_region,
    })
}

fn discover_free_list_head_from_footers<IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
) -> Result<Option<u32>, StartupError> {
    for candidate in 0..metadata.region_count {
        if !region_looks_free(flash, metadata, candidate)? {
            continue;
        }

        let mut pointed_to = false;
        for region_index in 0..metadata.region_count {
            if region_index == candidate || !region_looks_free(flash, metadata, region_index)? {
                continue;
            }
            if read_free_pointer_successor(flash, metadata, region_index)? == Some(candidate) {
                pointed_to = true;
                break;
            }
        }

        if !pointed_to {
            return Ok(Some(candidate));
        }
    }

    Ok(None)
}

fn region_looks_free<IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<bool, StartupError> {
    if read_region_header(flash, region_index).is_ok() {
        return Ok(false);
    }
    read_free_pointer_successor(flash, metadata, region_index).map(|_| true)
}

fn read_free_pointer_successor<IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<Option<u32>, StartupError> {
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StartupError::InvalidFreeListChain { region_index })?
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
        .map_err(|_| StartupError::InvalidFreeListChain { region_index })?;
    Ok(footer.next_tail)
}

#[cfg(test)]
#[allow(unused_mut, unused_variables)]
mod tests;
