/// Single active operation mode for a storage context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Idle,
    Formatting(FormatMode),
    Opening(OpenMode),
    ReadingStorage(ReadMode),
    LoadingCollection(CollectionLoadMode),
    CreatingCollection(CollectionCreateMode),
    UpdatingCollection(CollectionUpdateMode),
    AppendingWal(WalAppendMode),
    AllocatingRegion(AllocationMode),
    WritingCommittedRegion(CommittedRegionWriteMode),
    RotatingWal(WalRotationMode),
    ReclaimingRegion(RegionReclaimMode),
    ReclaimingWalHead(WalHeadReclaimMode),
    SnapshottingCollection(CollectionSnapshotMode),
    FlushingCollection(CollectionFlushMode),
    CompactingCollection(CollectionCompactionMode),
    DroppingCollection(CollectionDropMode),
}

impl StorageMode {
    pub(crate) const fn expected_idle() -> Self {
        Self::Idle
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    Begin,
    RecoverRotation,
    ReplayWalChain,
    BuildRuntimeState,
    ValidateLiveCollections,
    RecoverPendingReclaims,
    RecoverStagedRegions,
    Finish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionLoadMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionCreateMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionUpdateMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalAppendMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommittedRegionWriteMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRotationMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionReclaimMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalHeadReclaimMode {
    Plan,
    BeginReclaim,
    CopyLiveState,
    CommitHead,
    CompleteReclaim,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionSnapshotMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionFlushMode {
    ReserveRegion,
    CommitRegion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionCompactionMode {
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionDropMode {
    Running,
}

/// Named operation identifiers from the ring state-machine model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateMachineOperation {
    FormatStorage,
    OpenStorage,
    ReadStorage,
    LoadCollection,
    CreateCollection,
    ApplyCollectionUpdate,
    CommitCollectionSnapshot,
    CommitCollectionRegion,
    DropCollection,
    ReplayRetainedSnapshotBasis,
    ReplayRetainedRegionBasis,
    ReplayRetainedDropTombstone,
    AllocateRegionForUse,
    RotateWalTail,
    BeginTransaction,
    BeginInlineTransaction,
    AddTransactionCollection,
    StageFreeIntent,
    CommitTransaction,
    CommitInlineTransaction,
    RollbackInlineTransaction,
    RollbackTransaction,
    FreeRegion,
    EraseFreeRegionSpan,
    ReclaimWalHead,
    CommitWalRecovery,
    AppendRawWalRecord,
}

impl StateMachineOperation {
    pub const ALL: &'static [Self] = &[
        Self::FormatStorage,
        Self::OpenStorage,
        Self::ReadStorage,
        Self::LoadCollection,
        Self::CreateCollection,
        Self::ApplyCollectionUpdate,
        Self::CommitCollectionSnapshot,
        Self::CommitCollectionRegion,
        Self::DropCollection,
        Self::ReplayRetainedSnapshotBasis,
        Self::ReplayRetainedRegionBasis,
        Self::ReplayRetainedDropTombstone,
        Self::AllocateRegionForUse,
        Self::RotateWalTail,
        Self::BeginTransaction,
        Self::BeginInlineTransaction,
        Self::AddTransactionCollection,
        Self::StageFreeIntent,
        Self::CommitTransaction,
        Self::CommitInlineTransaction,
        Self::RollbackInlineTransaction,
        Self::RollbackTransaction,
        Self::FreeRegion,
        Self::EraseFreeRegionSpan,
        Self::ReclaimWalHead,
        Self::CommitWalRecovery,
        Self::AppendRawWalRecord,
    ];

    /// Returns the state-machine rule attached to this operation name.
    pub fn rule(self) -> StateMachineOperationRule {
        match self {
            Self::FormatStorage => StateMachineOperationRule {
                operation: self,
                active_mode: "Formatting(FormatMode)",
                source: "unformatted or caller-erased media",
                durable_edges: FORMAT_STORAGE_EDGES,
                target_or_effect: "initialized storage in Idle",
            },
            Self::OpenStorage => StateMachineOperationRule {
                operation: self,
                active_mode: "Opening(OpenMode)",
                source: "formatted media",
                durable_edges: OPEN_STORAGE_EDGES,
                target_or_effect: "recovered storage in Idle",
            },
            Self::ReadStorage => StateMachineOperationRule {
                operation: self,
                active_mode: "ReadingStorage(ReadMode)",
                source: "Idle",
                durable_edges: NO_DURABLE_EDGES,
                target_or_effect: "no durable state change",
            },
            Self::LoadCollection => StateMachineOperationRule {
                operation: self,
                active_mode: "LoadingCollection(CollectionLoadMode)",
                source: "any live collection state",
                durable_edges: NO_DURABLE_EDGES,
                target_or_effect: "materialized collection handle or frontier",
            },
            Self::CreateCollection => StateMachineOperationRule {
                operation: self,
                active_mode: "CreatingCollection(CollectionCreateMode)",
                source: "NoCollection",
                durable_edges: CREATE_COLLECTION_EDGES,
                target_or_effect: "EmptyClean",
            },
            Self::ApplyCollectionUpdate => StateMachineOperationRule {
                operation: self,
                active_mode: "UpdatingCollection(CollectionUpdateMode)",
                source: "any live clean or dirty collection state",
                durable_edges: APPLY_COLLECTION_UPDATE_EDGES,
                target_or_effect: "matching dirty collection state",
            },
            Self::CommitCollectionSnapshot => StateMachineOperationRule {
                operation: self,
                active_mode: "SnapshottingCollection(CollectionSnapshotMode)",
                source: "any live collection state",
                durable_edges: COMMIT_COLLECTION_SNAPSHOT_EDGES,
                target_or_effect: "WALSnapshotClean",
            },
            Self::CommitCollectionRegion => StateMachineOperationRule {
                operation: self,
                active_mode: "FlushingCollection, CompactingCollection, or WritingCommittedRegion",
                source: "any live collection state",
                durable_edges: COMMIT_COLLECTION_REGION_EDGES,
                target_or_effect: "RegionClean",
            },
            Self::DropCollection => StateMachineOperationRule {
                operation: self,
                active_mode: "DroppingCollection(CollectionDropMode)",
                source: "any live collection state",
                durable_edges: DROP_COLLECTION_EDGES,
                target_or_effect: "Dropped",
            },
            Self::ReplayRetainedSnapshotBasis => StateMachineOperationRule {
                operation: self,
                active_mode: "Opening(OpenMode) or ReclaimingWalHead(WalHeadReclaimMode)",
                source: "NoCollection",
                durable_edges: REPLAY_RETAINED_BASIS_EDGES,
                target_or_effect: "WALSnapshotClean",
            },
            Self::ReplayRetainedRegionBasis => StateMachineOperationRule {
                operation: self,
                active_mode: "Opening(OpenMode) or ReclaimingWalHead(WalHeadReclaimMode)",
                source: "NoCollection",
                durable_edges: REPLAY_RETAINED_BASIS_EDGES,
                target_or_effect: "RegionClean",
            },
            Self::ReplayRetainedDropTombstone => StateMachineOperationRule {
                operation: self,
                active_mode: "Opening(OpenMode) or ReclaimingWalHead(WalHeadReclaimMode)",
                source: "NoCollection",
                durable_edges: REPLAY_RETAINED_BASIS_EDGES,
                target_or_effect: "Dropped",
            },
            Self::AllocateRegionForUse => StateMachineOperationRule {
                operation: self,
                active_mode: "AllocatingRegion(AllocationMode)",
                source: "active transaction, inline transaction, or storage-core operation",
                durable_edges: ALLOCATE_REGION_FOR_USE_EDGES,
                target_or_effect: "free-space allocation head advances",
            },
            Self::RotateWalTail => StateMachineOperationRule {
                operation: self,
                active_mode: "RotatingWal(WalRotationMode)",
                source: "current WAL tail in rotation window",
                durable_edges: ROTATE_WAL_TAIL_EDGES,
                target_or_effect: "WAL tail moves to linked region",
            },
            Self::BeginTransaction => StateMachineOperationRule {
                operation: self,
                active_mode: "Transacting(TransactionMode)",
                source: "Idle with an available transaction log",
                durable_edges: BEGIN_TRANSACTION_EDGES,
                target_or_effect: "transaction descriptor opens",
            },
            Self::BeginInlineTransaction => StateMachineOperationRule {
                operation: self,
                active_mode: "Transacting(TransactionMode)",
                source: "no active full transaction and reserved main-WAL tail space",
                durable_edges: BEGIN_INLINE_TRANSACTION_EDGES,
                target_or_effect: "bounded inline transaction opens",
            },
            Self::AddTransactionCollection => StateMachineOperationRule {
                operation: self,
                active_mode: "Transacting(TransactionMode)",
                source: "open transaction descriptor and live collection",
                durable_edges: ADD_TRANSACTION_COLLECTION_EDGES,
                target_or_effect: "collection enrolled with private frontier state",
            },
            Self::StageFreeIntent => StateMachineOperationRule {
                operation: self,
                active_mode: "Transacting(TransactionMode)",
                source: "open transaction descriptor and enrolled live collection region",
                durable_edges: STAGE_FREE_INTENT_EDGES,
                target_or_effect: "free intent retained without allocator effect",
            },
            Self::CommitTransaction => StateMachineOperationRule {
                operation: self,
                active_mode: "Transacting(TransactionMode)",
                source: "open transaction descriptor with no generation conflicts",
                durable_edges: COMMIT_TRANSACTION_EDGES,
                target_or_effect: "transaction-log range becomes visible atomically",
            },
            Self::CommitInlineTransaction => StateMachineOperationRule {
                operation: self,
                active_mode: "Transacting(TransactionMode)",
                source: "open inline transaction whose bounded range is complete",
                durable_edges: COMMIT_INLINE_TRANSACTION_EDGES,
                target_or_effect: "bounded inline range becomes visible atomically",
            },
            Self::RollbackInlineTransaction => StateMachineOperationRule {
                operation: self,
                active_mode:
                    "Transacting(TransactionMode) or TransactionRecovery(TransactionRecoveryMode)",
                source: "open or recovering uncommitted inline transaction",
                durable_edges: ROLLBACK_INLINE_TRANSACTION_EDGES,
                target_or_effect: "bounded inline range remains non-visible",
            },
            Self::RollbackTransaction => StateMachineOperationRule {
                operation: self,
                active_mode:
                    "Transacting(TransactionMode) or TransactionRecovery(TransactionRecoveryMode)",
                source: "open or recovering uncommitted transaction range",
                durable_edges: ROLLBACK_TRANSACTION_EDGES,
                target_or_effect: "transaction-log range remains non-visible and cleanup begins",
            },
            Self::FreeRegion => StateMachineOperationRule {
                operation: self,
                active_mode: "transaction cleanup mode",
                source: "cleanup owner and detached next cleanup obligation",
                durable_edges: FREE_REGION_EDGES,
                target_or_effect: "region enters dirty free-space range",
            },
            Self::EraseFreeRegionSpan => StateMachineOperationRule {
                operation: self,
                active_mode: "AllocatingRegion(AllocationMode) or storage maintenance mode",
                source: "dirty range is non-empty and no transaction owns cleanup",
                durable_edges: ERASE_FREE_REGION_SPAN_EDGES,
                target_or_effect: "dirty entries become ready entries",
            },
            Self::ReclaimWalHead => StateMachineOperationRule {
                operation: self,
                active_mode: "ReclaimingWalHead(WalHeadReclaimMode)",
                source: "reclaimable WAL head",
                durable_edges: RECLAIM_WAL_HEAD_EDGES,
                target_or_effect: "WAL head moves and old head enters free-space collection",
            },
            Self::CommitWalRecovery => StateMachineOperationRule {
                operation: self,
                active_mode: "AppendingWal(WalAppendMode)",
                source: "pending WAL recovery boundary",
                durable_edges: COMMIT_WAL_RECOVERY_EDGES,
                target_or_effect: "boundary cleared so normal append may resume",
            },
            Self::AppendRawWalRecord => StateMachineOperationRule {
                operation: self,
                active_mode: "AppendingWal(WalAppendMode)",
                source: "valid record-specific source state",
                durable_edges: APPEND_RAW_WAL_RECORD_EDGES,
                target_or_effect: "record-specific ApplyWalRecord effect",
            },
        }
    }
}

/// Named durable write/sync boundaries used by state-machine operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableTransitionEdge {
    FormatMetadata,
    FormatInitialWalRegion,
    FormatInitialFreeSpaceCollection,
    CreateCollection,
    AppendUpdate,
    CommitSnapshotHead,
    AllocateRegion,
    AppendFreeRegion,
    EraseFreeRegionSpan,
    StartWalRotation,
    WriteCommittedRegion,
    CommitRegionHead,
    RotateWalLink,
    InitializeRotatedWalRegion,
    CommitWalHeadControl,
    CopyRetainedWalRecord,
    RewriteRetainedEmptyBasis,
    BeginTransaction,
    BeginInlineTransaction,
    AddTransactionCollection,
    StageFreeIntent,
    CommitTransaction,
    CommitInlineTransaction,
    FinishTransaction,
    RollbackTransaction,
    RollbackInlineTransaction,
    CommitWalRecoveryBoundary,
    CommitDropCollection,
}

impl DurableTransitionEdge {
    pub const ALL: &'static [Self] = &[
        Self::FormatMetadata,
        Self::FormatInitialWalRegion,
        Self::FormatInitialFreeSpaceCollection,
        Self::CreateCollection,
        Self::AppendUpdate,
        Self::CommitSnapshotHead,
        Self::AllocateRegion,
        Self::AppendFreeRegion,
        Self::EraseFreeRegionSpan,
        Self::StartWalRotation,
        Self::WriteCommittedRegion,
        Self::CommitRegionHead,
        Self::RotateWalLink,
        Self::InitializeRotatedWalRegion,
        Self::CommitWalHeadControl,
        Self::CopyRetainedWalRecord,
        Self::RewriteRetainedEmptyBasis,
        Self::BeginTransaction,
        Self::BeginInlineTransaction,
        Self::AddTransactionCollection,
        Self::StageFreeIntent,
        Self::CommitTransaction,
        Self::CommitInlineTransaction,
        Self::FinishTransaction,
        Self::RollbackTransaction,
        Self::RollbackInlineTransaction,
        Self::CommitWalRecoveryBoundary,
        Self::CommitDropCollection,
    ];

    /// Returns the named durable-edge semantics required by the state-machine model.
    pub fn semantics(self) -> DurableEdgeSemantics {
        match self {
            Self::FormatMetadata => DurableEdgeSemantics {
                edge: self,
                preconditions: "caller selected valid geometry for erased or unformatted media",
                durable_effect: "write and sync storage metadata",
                runtime_effect: "format operation records immutable geometry",
                replay_effect: "later open can validate the store metadata",
                crash_cut_result: "media before this edge is not a formatted store",
            },
            Self::FormatInitialWalRegion => DurableEdgeSemantics {
                edge: self,
                preconditions: "metadata edge is durable and region 0 is available",
                durable_effect: "initialize and sync region 0 as the first WAL region",
                runtime_effect: "format operation records the initial WAL head and tail",
                replay_effect: "startup can discover the initial WAL chain",
                crash_cut_result: "open rejects incomplete formatting before a valid WAL exists",
            },
            Self::FormatInitialFreeSpaceCollection => DurableEdgeSemantics {
                edge: self,
                preconditions: "metadata and initial WAL region are durable",
                durable_effect: "initialize and sync the free-space collection metadata chain",
                runtime_effect: "format operation builds the initial allocator cursors",
                replay_effect: "startup recovers the initial free-space basis",
                crash_cut_result:
                    "open rejects incomplete formatting before allocator basis exists",
            },
            Self::CreateCollection => DurableEdgeSemantics {
                edge: self,
                preconditions: "collection id is not already live or dropped",
                durable_effect: "write and sync new_collection",
                runtime_effect: "ApplyWalRecord creates an empty clean collection",
                replay_effect: "startup creates the same empty clean collection",
                crash_cut_result: "cut before sync leaves no collection; cut after sync creates it",
            },
            Self::AppendUpdate => DurableEdgeSemantics {
                edge: self,
                preconditions: "target collection is live and not dropped",
                durable_effect: "write and sync update",
                runtime_effect: "ApplyWalRecord retains the update and marks the collection dirty",
                replay_effect: "startup retains the update and rebuilds the dirty frontier",
                crash_cut_result: "cut before sync loses the update; cut after sync replays it",
            },
            Self::CommitSnapshotHead => DurableEdgeSemantics {
                edge: self,
                preconditions: "snapshot payload is valid for the collection type",
                durable_effect: "write and sync snapshot",
                runtime_effect: "ApplyWalRecord installs a WAL snapshot basis",
                replay_effect: "startup installs the same WAL snapshot basis",
                crash_cut_result:
                    "old basis survives before sync; snapshot is authoritative after sync",
            },
            Self::AllocateRegion => DurableEdgeSemantics {
                edge: self,
                preconditions: "free-space ready range has an allocatable entry",
                durable_effect: "write and sync allocate_region",
                runtime_effect: "ApplyWalRecord advances allocation_head and records ownership",
                replay_effect: "startup advances allocation_head and reconstructs ownership",
                crash_cut_result:
                    "allocation is not replay-visible before sync and is owned after sync",
            },
            Self::AppendFreeRegion => DurableEdgeSemantics {
                edge: self,
                preconditions:
                    "region is detached from live references and cleanup owner may append",
                durable_effect: "write and sync free_region",
                runtime_effect: "ApplyWalRecord advances append_tail",
                replay_effect: "startup advances append_tail in the same order",
                crash_cut_result:
                    "region remains unavailable before sync and enters dirty range after sync",
            },
            Self::EraseFreeRegionSpan => DurableEdgeSemantics {
                edge: self,
                preconditions: "dirty free-space span is non-empty and cleanup is not owned",
                durable_effect: "erase entries, then write and sync erase_free_region_span",
                runtime_effect: "ApplyWalRecord advances ready_boundary",
                replay_effect: "startup advances ready_boundary",
                crash_cut_result:
                    "dirty entries are not ready before sync and are ready after sync",
            },
            Self::StartWalRotation => DurableEdgeSemantics {
                edge: self,
                preconditions: "tail is in the rotation window with a reserved free region",
                durable_effect: "write and sync rotation-window allocate_region",
                runtime_effect: "ApplyWalRecord records a storage-core allocation reservation",
                replay_effect: "startup reconstructs the reservation for a matching link",
                crash_cut_result:
                    "rotation can be retried or completed without losing the allocation",
            },
            Self::WriteCommittedRegion => DurableEdgeSemantics {
                edge: self,
                preconditions: "region is allocated for committed collection data",
                durable_effect: "erase, write, and sync committed-region header and payload",
                runtime_effect: "operation records a staged physical basis",
                replay_effect: "no public state changes until the publishing head record",
                crash_cut_result: "unpublished physical writes remain unreachable",
            },
            Self::CommitRegionHead => DurableEdgeSemantics {
                edge: self,
                preconditions: "committed region payload is durable and valid",
                durable_effect: "write and sync user-collection head",
                runtime_effect: "ApplyWalRecord installs the region basis",
                replay_effect: "startup installs the same region basis",
                crash_cut_result:
                    "old basis survives before sync; new region basis is visible after sync",
            },
            Self::RotateWalLink => DurableEdgeSemantics {
                edge: self,
                preconditions: "storage-core allocation reservation names the linked region",
                durable_effect: "write and sync WAL link",
                runtime_effect: "ApplyWalRecord links the next WAL tail",
                replay_effect: "startup follows the same WAL link",
                crash_cut_result: "startup can finish or ignore incomplete rotation safely",
            },
            Self::InitializeRotatedWalRegion => DurableEdgeSemantics {
                edge: self,
                preconditions: "link to the new WAL region is durable",
                durable_effect: "erase, initialize, and sync the linked WAL region",
                runtime_effect: "operation exposes the linked region as append tail",
                replay_effect: "startup can recover a linked but uninitialized tail",
                crash_cut_result: "rotation recovery completes missing initialization",
            },
            Self::CommitWalHeadControl => DurableEdgeSemantics {
                edge: self,
                preconditions: "replacement WAL head contains required retained state",
                durable_effect: "write and sync main-WAL head control record",
                runtime_effect: "ApplyWalRecord moves the effective WAL head",
                replay_effect: "startup selects the same effective WAL head",
                crash_cut_result: "old head remains effective before sync; new head after sync",
            },
            Self::CopyRetainedWalRecord => DurableEdgeSemantics {
                edge: self,
                preconditions: "record is retained by WAL-head reclaim liveness rules",
                durable_effect: "copy and sync retained WAL record into the new WAL head",
                runtime_effect: "reclaim plan preserves replay-visible state",
                replay_effect: "ApplyWalRecord gives the copied record the same effect",
                crash_cut_result:
                    "old WAL head remains authoritative until head control is durable",
            },
            Self::RewriteRetainedEmptyBasis => DurableEdgeSemantics {
                edge: self,
                preconditions:
                    "empty live collection would lose its creation record during reclaim",
                durable_effect: "write and sync an equivalent retained snapshot basis",
                runtime_effect: "reclaim plan preserves the empty live collection",
                replay_effect: "startup sees an equivalent empty collection basis",
                crash_cut_result:
                    "old WAL head remains authoritative until head control is durable",
            },
            Self::BeginTransaction => DurableEdgeSemantics {
                edge: self,
                preconditions: "transaction log is available and no conflicting descriptor is open",
                durable_effect: "write and sync begin_transaction",
                runtime_effect: "ApplyWalRecord opens a transaction descriptor",
                replay_effect: "startup opens the same transaction descriptor",
                crash_cut_result: "uncommitted descriptor is rolled back during recovery",
            },
            Self::BeginInlineTransaction => DurableEdgeSemantics {
                edge: self,
                preconditions: "bounded inline body fits in reserved main-WAL space",
                durable_effect: "write and sync begin_inline_transaction",
                runtime_effect: "ApplyWalRecord opens an inline transaction body",
                replay_effect: "startup recognizes the bounded inline body",
                crash_cut_result: "uncommitted inline body is ignored or rolled back",
            },
            Self::AddTransactionCollection => DurableEdgeSemantics {
                edge: self,
                preconditions: "transaction is open and collection generation matches enrollment",
                durable_effect: "write and sync add_transaction_collection in the transaction log",
                runtime_effect: "ApplyWalRecord enrolls private collection state",
                replay_effect: "startup reconstructs the private enrollment",
                crash_cut_result: "enrollment is private until commit imports the range",
            },
            Self::StageFreeIntent => DurableEdgeSemantics {
                edge: self,
                preconditions: "transaction is open and the enrolled collection owns the region",
                durable_effect: "write and sync free_intent in the transaction log",
                runtime_effect:
                    "ApplyWalRecord records a private free intent with no allocator effect",
                replay_effect:
                    "startup reconstructs the free intent without freeing the region before commit",
                crash_cut_result: "intent is ignored on rollback and cleaned only after commit",
            },
            Self::CommitTransaction => DurableEdgeSemantics {
                edge: self,
                preconditions:
                    "transaction-log range is complete and enrolled generations still match",
                durable_effect: "write and sync commit_transaction in the main WAL",
                runtime_effect:
                    "ApplyWalRecord imports private effects and starts cleanup ownership",
                replay_effect: "startup imports the same private effects and resumes cleanup",
                crash_cut_result: "range is private before sync and visible atomically after sync",
            },
            Self::CommitInlineTransaction => DurableEdgeSemantics {
                edge: self,
                preconditions: "inline body is complete and bounded by the matching begin record",
                durable_effect: "write and sync commit_inline_transaction",
                runtime_effect: "ApplyWalRecord imports the inline body atomically",
                replay_effect: "startup imports the same inline body",
                crash_cut_result: "body is ignored before sync and visible atomically after sync",
            },
            Self::FinishTransaction => DurableEdgeSemantics {
                edge: self,
                preconditions: "all ordered transaction cleanup obligations are durable",
                durable_effect: "write and sync transaction_finished",
                runtime_effect: "ApplyWalRecord releases cleanup owner and transaction-log range",
                replay_effect: "startup releases the same cleanup owner and range",
                crash_cut_result: "unfinished cleanup is resumed until this marker is durable",
            },
            Self::RollbackTransaction => DurableEdgeSemantics {
                edge: self,
                preconditions: "transaction-log range is uncommitted or caller requested rollback",
                durable_effect: "write and sync rollback_transaction",
                runtime_effect:
                    "ApplyWalRecord marks the range non-visible and starts allocation cleanup",
                replay_effect: "startup rolls back the same range and resumes cleanup",
                crash_cut_result: "private collection effects remain non-visible",
            },
            Self::RollbackInlineTransaction => DurableEdgeSemantics {
                edge: self,
                preconditions: "inline transaction is incomplete or explicitly aborted",
                durable_effect: "write and sync rollback_inline_transaction",
                runtime_effect: "ApplyWalRecord keeps the bounded inline range non-visible",
                replay_effect: "startup keeps the same inline range non-visible",
                crash_cut_result: "inline body remains ignored",
            },
            Self::CommitWalRecoveryBoundary => DurableEdgeSemantics {
                edge: self,
                preconditions: "startup found a pending WAL recovery boundary",
                durable_effect: "write and sync wal_recovery",
                runtime_effect: "ApplyWalRecord clears the recovery boundary",
                replay_effect: "startup sees the boundary as closed",
                crash_cut_result: "append remains blocked until the marker is durable",
            },
            Self::CommitDropCollection => DurableEdgeSemantics {
                edge: self,
                preconditions: "target collection is live and not already dropped",
                durable_effect: "write and sync drop_collection",
                runtime_effect: "ApplyWalRecord marks the collection dropped",
                replay_effect: "startup marks the same collection dropped",
                crash_cut_result: "collection remains live before sync and dropped after sync",
            },
        }
    }
}

/// Concrete rule attached to a named state-machine operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateMachineOperationRule {
    pub operation: StateMachineOperation,
    pub active_mode: &'static str,
    pub source: &'static str,
    pub durable_edges: &'static [DurableTransitionEdge],
    pub target_or_effect: &'static str,
}

/// Concrete semantics attached to one named durable edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableEdgeSemantics {
    pub edge: DurableTransitionEdge,
    pub preconditions: &'static str,
    pub durable_effect: &'static str,
    pub runtime_effect: &'static str,
    pub replay_effect: &'static str,
    pub crash_cut_result: &'static str,
}

const NO_DURABLE_EDGES: &[DurableTransitionEdge] = &[];
const FORMAT_STORAGE_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::FormatMetadata,
    DurableTransitionEdge::FormatInitialWalRegion,
    DurableTransitionEdge::FormatInitialFreeSpaceCollection,
];
const OPEN_STORAGE_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::RollbackTransaction,
    DurableTransitionEdge::AppendFreeRegion,
    DurableTransitionEdge::FinishTransaction,
    DurableTransitionEdge::CommitWalRecoveryBoundary,
];
const CREATE_COLLECTION_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::CreateCollection];
const APPLY_COLLECTION_UPDATE_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::AppendUpdate];
const COMMIT_COLLECTION_SNAPSHOT_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::CommitSnapshotHead];
const COMMIT_COLLECTION_REGION_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::AllocateRegion,
    DurableTransitionEdge::WriteCommittedRegion,
    DurableTransitionEdge::CommitRegionHead,
    DurableTransitionEdge::AppendFreeRegion,
    DurableTransitionEdge::FinishTransaction,
];
const DROP_COLLECTION_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::CommitDropCollection,
    DurableTransitionEdge::AppendFreeRegion,
    DurableTransitionEdge::FinishTransaction,
];
const REPLAY_RETAINED_BASIS_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::CopyRetainedWalRecord];
const ALLOCATE_REGION_FOR_USE_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::AllocateRegion];
const ROTATE_WAL_TAIL_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::StartWalRotation,
    DurableTransitionEdge::RotateWalLink,
    DurableTransitionEdge::InitializeRotatedWalRegion,
];
const BEGIN_TRANSACTION_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::BeginTransaction];
const BEGIN_INLINE_TRANSACTION_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::BeginInlineTransaction];
const ADD_TRANSACTION_COLLECTION_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::AddTransactionCollection];
const STAGE_FREE_INTENT_EDGES: &[DurableTransitionEdge] = &[DurableTransitionEdge::StageFreeIntent];
const COMMIT_TRANSACTION_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::CommitTransaction,
    DurableTransitionEdge::AppendFreeRegion,
    DurableTransitionEdge::FinishTransaction,
];
const COMMIT_INLINE_TRANSACTION_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::CommitInlineTransaction];
const ROLLBACK_INLINE_TRANSACTION_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::RollbackInlineTransaction];
const ROLLBACK_TRANSACTION_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::RollbackTransaction,
    DurableTransitionEdge::AppendFreeRegion,
    DurableTransitionEdge::FinishTransaction,
];
const FREE_REGION_EDGES: &[DurableTransitionEdge] = &[DurableTransitionEdge::AppendFreeRegion];
const ERASE_FREE_REGION_SPAN_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::EraseFreeRegionSpan];
const RECLAIM_WAL_HEAD_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::BeginTransaction,
    DurableTransitionEdge::AllocateRegion,
    DurableTransitionEdge::StartWalRotation,
    DurableTransitionEdge::RotateWalLink,
    DurableTransitionEdge::CopyRetainedWalRecord,
    DurableTransitionEdge::RewriteRetainedEmptyBasis,
    DurableTransitionEdge::CommitWalHeadControl,
    DurableTransitionEdge::AppendFreeRegion,
    DurableTransitionEdge::FinishTransaction,
];
const COMMIT_WAL_RECOVERY_EDGES: &[DurableTransitionEdge] =
    &[DurableTransitionEdge::CommitWalRecoveryBoundary];
const APPEND_RAW_WAL_RECORD_EDGES: &[DurableTransitionEdge] = &[
    DurableTransitionEdge::CreateCollection,
    DurableTransitionEdge::AppendUpdate,
    DurableTransitionEdge::CommitSnapshotHead,
    DurableTransitionEdge::AllocateRegion,
    DurableTransitionEdge::AppendFreeRegion,
    DurableTransitionEdge::EraseFreeRegionSpan,
    DurableTransitionEdge::CommitRegionHead,
    DurableTransitionEdge::RotateWalLink,
    DurableTransitionEdge::CommitWalHeadControl,
    DurableTransitionEdge::BeginTransaction,
    DurableTransitionEdge::BeginInlineTransaction,
    DurableTransitionEdge::AddTransactionCollection,
    DurableTransitionEdge::StageFreeIntent,
    DurableTransitionEdge::CommitTransaction,
    DurableTransitionEdge::CommitInlineTransaction,
    DurableTransitionEdge::FinishTransaction,
    DurableTransitionEdge::RollbackTransaction,
    DurableTransitionEdge::RollbackInlineTransaction,
    DurableTransitionEdge::CommitWalRecoveryBoundary,
    DurableTransitionEdge::CommitDropCollection,
];
