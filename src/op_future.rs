use core::fmt::Debug;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::task::{Context, Poll};
use serde::{Deserialize, Serialize};

use crate::mode::{CollectionFlushMode, OpenMode, StorageMode, WalHeadReclaimMode};
use crate::startup::StartupOpenPlan;
use crate::storage::WalHeadReclaimPlan;
use crate::{
    CollectionType, FlashIo, MapFrontier, MapStorageError, StartupCollectionBasis, Storage,
    StorageFormatConfig, StorageOpenError, StorageRuntimeError, StorageWorkspace,
};

/// Minimal future wrapper that executes a closure exactly once when first polled.
pub struct RunOnce<F> {
    operation: Option<F>,
}

/// Wraps a synchronous closure as a trivially ready future.
pub fn run_once<F>(operation: F) -> RunOnce<F> {
    RunOnce {
        operation: Some(operation),
    }
}

impl<F> Unpin for RunOnce<F> {}

impl<F, T> Future for RunOnce<F>
where
    F: FnOnce() -> T,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.operation.take() {
            Some(operation) => Poll::Ready(operation()),
            None => Poll::Pending,
        }
    }
}

/// Caller-driven future for formatting storage and binding the backing object.
pub struct FormatStorageFuture<
    'db,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
> where
    IO: FlashIo,
{
    backing: Option<&'db mut IO>,
    config: StorageFormatConfig,
}

impl<
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > FormatStorageFuture<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
where
    IO: FlashIo,
{
    pub(crate) fn new(backing: &'db mut IO, config: StorageFormatConfig) -> Self {
        Self {
            backing: Some(backing),
            config,
        }
    }
}

impl<
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > Unpin
    for FormatStorageFuture<
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >
where
    IO: FlashIo,
{
}

impl<
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > Future
    for FormatStorageFuture<
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >
where
    IO: FlashIo,
{
    type Output = Result<
        Storage<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        StorageRuntimeError,
    >;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.backing.take() {
            Some(backing) => Poll::Ready(Storage::format(backing, this.config)),
            None => Poll::Pending,
        }
    }
}

/// Caller-driven future for flushing a map through the manifest-backed path.
pub struct YieldingFlushMapFuture<
    'a,
    'db,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
    K,
    V,
    const MAX_INDEXES: usize,
    const MAX_RUNS: usize,
> where
    IO: FlashIo,
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    storage:
        &'a mut Storage<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    map: &'a mut MapFrontier<'a, K, V, MAX_INDEXES, MAX_RUNS>,
    phase: u8,
}

impl<
        'a,
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        K,
        V,
        const MAX_INDEXES: usize,
        const MAX_RUNS: usize,
    >
    YieldingFlushMapFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        K,
        V,
        MAX_INDEXES,
        MAX_RUNS,
    >
where
    IO: FlashIo,
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    /// Creates a new yielding manifest flush future.
    pub fn new(
        storage: &'a mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
        map: &'a mut MapFrontier<'a, K, V, MAX_INDEXES, MAX_RUNS>,
    ) -> Self {
        Self {
            storage,
            map,
            phase: 0,
        }
    }
}

impl<
        'a,
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        K,
        V,
        const MAX_INDEXES: usize,
        const MAX_RUNS: usize,
    > Unpin
    for YieldingFlushMapFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        K,
        V,
        MAX_INDEXES,
        MAX_RUNS,
    >
where
    IO: FlashIo,
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
}

impl<
        'a,
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        K,
        V,
        const MAX_INDEXES: usize,
        const MAX_RUNS: usize,
    > Future
    for YieldingFlushMapFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        K,
        V,
        MAX_INDEXES,
        MAX_RUNS,
    >
where
    IO: FlashIo,
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Output = Result<u32, MapStorageError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.phase {
            0 => {
                if let Err(error) = this.storage.enter_mode(StorageMode::FlushingCollection(
                    CollectionFlushMode::ReserveRegion,
                )) {
                    this.phase = 3;
                    return Poll::Ready(Err(error.into()));
                }
                if let Err(error) = this
                    .storage
                    .state
                    .reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.workspace,
                    )
                {
                    this.storage.finish_mode();
                    this.phase = 3;
                    return Poll::Ready(Err(error.into()));
                }
                this.storage
                    .set_mode_unchecked(StorageMode::FlushingCollection(
                        CollectionFlushMode::CommitRegion,
                    ));
                this.phase = 1;
                Poll::Pending
            }
            1 => {
                this.phase = 2;
                Poll::Pending
            }
            2 => {
                let result = this
                    .storage
                    .flush_map_inner::<K, V, MAX_INDEXES, MAX_RUNS>(this.map);
                this.storage.finish_mode();
                this.phase = 3;
                Poll::Ready(result)
            }
            _ => Poll::Pending,
        }
    }
}

impl<
        'a,
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        K,
        V,
        const MAX_INDEXES: usize,
        const MAX_RUNS: usize,
    > Drop
    for YieldingFlushMapFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        K,
        V,
        MAX_INDEXES,
        MAX_RUNS,
    >
where
    IO: FlashIo,
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    fn drop(&mut self) {
        self.storage.finish_mode();
    }
}

#[derive(Debug)]
enum ReclaimWalHeadPhase<const MAX_COLLECTIONS: usize> {
    Plan,
    BeginReclaim {
        plan: WalHeadReclaimPlan<MAX_COLLECTIONS>,
    },
    PreserveFreeListHead {
        plan: WalHeadReclaimPlan<MAX_COLLECTIONS>,
    },
    CopyLiveState {
        plan: WalHeadReclaimPlan<MAX_COLLECTIONS>,
    },
    CommitHead {
        plan: WalHeadReclaimPlan<MAX_COLLECTIONS>,
    },
    CompleteReclaim {
        plan: WalHeadReclaimPlan<MAX_COLLECTIONS>,
    },
    Done,
}

enum OpenStoragePhase<
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
> {
    Begin,
    RecoverRotation {
        plan: StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    DiscoverWalChain {
        plan: StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    ReplayWalChain {
        plan: StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    FinishStartup {
        plan: StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    ValidateCollections {
        runtime: crate::StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    RecoverPendingReclaims {
        runtime: crate::StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    RecoverStagedRegions {
        runtime: crate::StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    Done,
}

/// Explicit phase-machine future for reclaiming the current WAL head.
pub struct ReclaimWalHeadFuture<
    'a,
    'db,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
> where
    IO: FlashIo,
{
    storage:
        &'a mut Storage<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    phase: ReclaimWalHeadPhase<MAX_COLLECTIONS>,
}

/// Explicit phase-machine future for opening storage through replay.
pub struct OpenStorageFuture<
    'db,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
> where
    IO: FlashIo,
{
    backing: Option<&'db mut IO>,
    workspace: StorageWorkspace<REGION_SIZE>,
    phase: OpenStoragePhase<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
}

impl<
        'a,
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >
    ReclaimWalHeadFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >
where
    IO: FlashIo,
{
    /// Creates a new WAL-head reclaim future.
    pub fn new(
        storage: &'a mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
    ) -> Self {
        Self {
            storage,
            phase: ReclaimWalHeadPhase::Plan,
        }
    }
}

impl<
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > OpenStorageFuture<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
where
    IO: FlashIo,
{
    /// Creates a new open-storage future.
    pub fn new(backing: &'db mut IO) -> Self {
        Self {
            backing: Some(backing),
            workspace: StorageWorkspace::new(),
            phase: OpenStoragePhase::Begin,
        }
    }
}

impl<
        'a,
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > Unpin
    for ReclaimWalHeadFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >
where
    IO: FlashIo,
{
}

impl<
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > Unpin
    for OpenStorageFuture<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
where
    IO: FlashIo,
{
}

impl<
        'a,
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > Future
    for ReclaimWalHeadFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >
where
    IO: FlashIo,
{
    type Output = Result<u32, StorageRuntimeError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let phase = mem::replace(&mut this.phase, ReclaimWalHeadPhase::Done);

        match phase {
            ReclaimWalHeadPhase::Plan => {
                if let Err(error) = this
                    .storage
                    .enter_mode(StorageMode::ReclaimingWalHead(WalHeadReclaimMode::Plan))
                {
                    this.phase = ReclaimWalHeadPhase::Done;
                    return Poll::Ready(Err(error));
                }
                let plan = match this
                    .storage
                    .state
                    .prepare_wal_head_reclaim::<REGION_SIZE, IO>(
                        this.storage.backing,
                        &mut this.storage.workspace,
                    ) {
                    Ok(plan) => plan,
                    Err(error) => {
                        this.storage.finish_mode();
                        return Poll::Ready(Err(error));
                    }
                };
                this.phase = ReclaimWalHeadPhase::BeginReclaim { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::BeginReclaim { plan } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::BeginReclaim,
                    ));
                if let Err(error) = this
                    .storage
                    .state
                    .begin_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.workspace,
                        plan.old_head,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.phase = ReclaimWalHeadPhase::PreserveFreeListHead { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::PreserveFreeListHead { plan } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::PreserveFreeListHead,
                    ));
                if let Err(error) = this
                    .storage
                    .state
                    .preserve_free_list_head_for_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.workspace,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.phase = ReclaimWalHeadPhase::CopyLiveState { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CopyLiveState { plan } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::CopyLiveState,
                    ));
                if let Err(error) = this
                    .storage
                    .state
                    .copy_live_wal_head_reclaim_state::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.workspace,
                        &plan,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.phase = ReclaimWalHeadPhase::CommitHead { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CommitHead { plan } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::CommitHead,
                    ));
                if let Err(error) = this
                    .storage
                    .state
                    .commit_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.workspace,
                        plan.new_head,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.phase = ReclaimWalHeadPhase::CompleteReclaim { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CompleteReclaim { plan } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::CompleteReclaim,
                    ));
                let result = this
                    .storage
                    .state
                    .complete_pending_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.workspace,
                        plan.old_head,
                    )
                    .map(|()| plan.new_head);
                this.storage.finish_mode();
                this.phase = ReclaimWalHeadPhase::Done;
                Poll::Ready(result)
            }
            ReclaimWalHeadPhase::Done => Poll::Pending,
        }
    }
}

impl<
        'a,
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > Drop
    for ReclaimWalHeadFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >
where
    IO: FlashIo,
{
    fn drop(&mut self) {
        self.storage.finish_mode();
    }
}

impl<
        'db,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > Future
    for OpenStorageFuture<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
where
    IO: FlashIo,
{
    type Output = Result<
        Storage<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        StorageOpenError,
    >;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let phase = mem::replace(&mut this.phase, OpenStoragePhase::Done);

        match phase {
            OpenStoragePhase::Begin => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                let plan = crate::startup::begin_open_formatted_store::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(backing, &mut this.workspace)?;
                this.phase = OpenStoragePhase::RecoverRotation { plan };
                Poll::Pending
            }
            OpenStoragePhase::RecoverRotation { mut plan } => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                crate::startup::recover_open_rotation::<
                    REGION_SIZE,
                    IO,
                    REGION_COUNT,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(backing, &mut this.workspace, &mut plan)?;
                this.phase = OpenStoragePhase::DiscoverWalChain { plan };
                Poll::Pending
            }
            OpenStoragePhase::DiscoverWalChain { mut plan } => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                crate::startup::discover_open_wal_chain::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(backing, &mut this.workspace, &mut plan)?;
                this.phase = OpenStoragePhase::ReplayWalChain { plan };
                Poll::Pending
            }
            OpenStoragePhase::ReplayWalChain { mut plan } => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                crate::startup::replay_open_wal_chain::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(backing, &mut this.workspace, &mut plan)?;
                this.phase = OpenStoragePhase::FinishStartup { plan };
                Poll::Pending
            }
            OpenStoragePhase::FinishStartup { mut plan } => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                let startup = crate::startup::finish_open_formatted_store::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(backing, &mut plan)?;
                let runtime = crate::storage::from_startup_state(startup)?;
                this.phase = OpenStoragePhase::ValidateCollections { runtime };
                Poll::Pending
            }
            OpenStoragePhase::ValidateCollections { runtime } => {
                for collection in runtime.collections() {
                    if collection.basis() == StartupCollectionBasis::Dropped {
                        continue;
                    }

                    let Some(collection_type) = collection.collection_type() else {
                        return Poll::Ready(Err(StorageOpenError::UnsupportedLiveCollectionType(
                            0xffff,
                        )));
                    };

                    if collection_type != CollectionType::MAP_CODE {
                        return Poll::Ready(Err(StorageOpenError::UnsupportedLiveCollectionType(
                            collection_type,
                        )));
                    }
                }
                this.phase = OpenStoragePhase::RecoverPendingReclaims { runtime };
                Poll::Pending
            }
            OpenStoragePhase::RecoverPendingReclaims { mut runtime } => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                runtime.recover_pending_reclaims::<REGION_SIZE, REGION_COUNT, IO>(
                    backing,
                    &mut this.workspace,
                )?;
                this.phase = OpenStoragePhase::RecoverStagedRegions { runtime };
                Poll::Pending
            }
            OpenStoragePhase::RecoverStagedRegions { mut runtime } => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                runtime.recover_abandoned_staged_regions::<REGION_SIZE, REGION_COUNT, IO>(
                    backing,
                    &mut this.workspace,
                )?;
                let backing = this.backing.take().ok_or(StorageOpenError::Runtime(
                    StorageRuntimeError::InvalidStorageMode {
                        expected: StorageMode::Opening(OpenMode::Finish),
                        actual: StorageMode::Idle,
                    },
                ))?;
                let workspace = mem::take(&mut this.workspace);
                this.phase = OpenStoragePhase::Done;
                Poll::Ready(Ok(Storage::from_runtime(backing, workspace, runtime)))
            }
            OpenStoragePhase::Done => Poll::Pending,
        }
    }
}
