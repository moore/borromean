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
    DiscoverWalChain,
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
