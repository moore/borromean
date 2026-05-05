use core::fmt::Debug;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::task::{Context, Poll};
use serde::{Deserialize, Serialize};

use crate::startup::StartupOpenPlan;
use crate::storage::WalHeadReclaimPlan;
use crate::{FlashIo, LsmMap, MapStorageError, Storage, StorageRuntimeError, StorageWorkspace};

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

/// Caller-driven future for flushing a map through the manifest-backed path.
pub struct YieldingFlushMapFuture<
    'a,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO,
    K,
    V,
    const MAX_INDEXES: usize,
    const MAX_RUNS: usize,
> where
    IO: FlashIo,
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    storage: &'a mut Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    flash: &'a mut IO,
    workspace: &'a mut StorageWorkspace<REGION_SIZE>,
    map: &'a mut LsmMap<'a, K, V, MAX_INDEXES, MAX_RUNS>,
    phase: u8,
}

impl<
        'a,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
        K,
        V,
        const MAX_INDEXES: usize,
        const MAX_RUNS: usize,
    >
    YieldingFlushMapFuture<
        'a,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        REGION_SIZE,
        REGION_COUNT,
        IO,
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
        storage: &'a mut Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
        map: &'a mut LsmMap<'a, K, V, MAX_INDEXES, MAX_RUNS>,
    ) -> Self {
        Self {
            storage,
            flash,
            workspace,
            map,
            phase: 0,
        }
    }
}

impl<
        'a,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
        K,
        V,
        const MAX_INDEXES: usize,
        const MAX_RUNS: usize,
    > Unpin
    for YieldingFlushMapFuture<
        'a,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        REGION_SIZE,
        REGION_COUNT,
        IO,
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
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
        K,
        V,
        const MAX_INDEXES: usize,
        const MAX_RUNS: usize,
    > Future
    for YieldingFlushMapFuture<
        'a,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        REGION_SIZE,
        REGION_COUNT,
        IO,
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
                if let Err(error) = this
                    .storage
                    .runtime_mut()
                    .reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(
                        this.flash,
                        this.workspace,
                    )
                {
                    this.phase = 3;
                    return Poll::Ready(Err(error.into()));
                }
                this.phase = 1;
                Poll::Pending
            }
            1 => {
                this.phase = 2;
                Poll::Pending
            }
            2 => {
                this.phase = 3;
                Poll::Ready(
                    this.storage
                        .flush_map::<REGION_SIZE, REGION_COUNT, IO, K, V, MAX_INDEXES, MAX_RUNS>(
                            this.flash,
                            this.workspace,
                            this.map,
                        ),
                )
            }
            _ => Poll::Pending,
        }
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
    RecoverPendingReclaims {
        runtime: crate::StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    ValidateCollections {
        storage: Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    },
    Done,
}

/// Explicit phase-machine future for reclaiming the current WAL head.
pub struct ReclaimWalHeadFuture<
    'a,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO,
> where
    IO: FlashIo,
{
    storage: &'a mut Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    flash: &'a mut IO,
    workspace: &'a mut StorageWorkspace<REGION_SIZE>,
    phase: ReclaimWalHeadPhase<MAX_COLLECTIONS>,
}

/// Explicit phase-machine future for opening storage through replay.
pub struct OpenStorageFuture<
    'a,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO,
> where
    IO: FlashIo,
{
    flash: &'a mut IO,
    workspace: &'a mut StorageWorkspace<REGION_SIZE>,
    phase: OpenStoragePhase<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
}

impl<
        'a,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
    > ReclaimWalHeadFuture<'a, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS, REGION_SIZE, REGION_COUNT, IO>
where
    IO: FlashIo,
{
    /// Creates a new WAL-head reclaim future.
    pub fn new(
        storage: &'a mut Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
    ) -> Self {
        Self {
            storage,
            flash,
            workspace,
            phase: ReclaimWalHeadPhase::Plan,
        }
    }
}

impl<
        'a,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
    > OpenStorageFuture<'a, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS, REGION_SIZE, REGION_COUNT, IO>
where
    IO: FlashIo,
{
    /// Creates a new open-storage future.
    pub fn new(flash: &'a mut IO, workspace: &'a mut StorageWorkspace<REGION_SIZE>) -> Self {
        Self {
            flash,
            workspace,
            phase: OpenStoragePhase::Begin,
        }
    }
}

impl<
        'a,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
    > Unpin
    for ReclaimWalHeadFuture<
        'a,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        REGION_SIZE,
        REGION_COUNT,
        IO,
    >
where
    IO: FlashIo,
{
}

impl<
        'a,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
    > Unpin
    for OpenStorageFuture<'a, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS, REGION_SIZE, REGION_COUNT, IO>
where
    IO: FlashIo,
{
}

impl<
        'a,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
    > Future
    for ReclaimWalHeadFuture<
        'a,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
        REGION_SIZE,
        REGION_COUNT,
        IO,
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
                let plan = this
                    .storage
                    .state
                    .prepare_wal_head_reclaim::<REGION_SIZE, IO>(this.flash, this.workspace)?;
                this.phase = ReclaimWalHeadPhase::BeginReclaim { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::BeginReclaim { plan } => {
                this.storage
                    .state
                    .begin_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.flash,
                        this.workspace,
                        plan.old_head,
                    )?;
                this.phase = ReclaimWalHeadPhase::PreserveFreeListHead { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::PreserveFreeListHead { plan } => {
                this.storage
                    .state
                    .preserve_free_list_head_for_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.flash,
                        this.workspace,
                    )?;
                this.phase = ReclaimWalHeadPhase::CopyLiveState { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CopyLiveState { plan } => {
                this.storage
                    .state
                    .copy_live_wal_head_reclaim_state::<REGION_SIZE, REGION_COUNT, IO>(
                        this.flash,
                        this.workspace,
                        &plan,
                    )?;
                this.phase = ReclaimWalHeadPhase::CommitHead { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CommitHead { plan } => {
                this.storage
                    .state
                    .commit_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.flash,
                        this.workspace,
                        plan.new_head,
                    )?;
                this.phase = ReclaimWalHeadPhase::CompleteReclaim { plan };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CompleteReclaim { plan } => {
                this.storage
                    .state
                    .complete_pending_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.flash,
                        this.workspace,
                        plan.old_head,
                    )?;
                this.phase = ReclaimWalHeadPhase::Done;
                Poll::Ready(Ok(plan.new_head))
            }
            ReclaimWalHeadPhase::Done => Poll::Pending,
        }
    }
}

impl<
        'a,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO,
    > Future
    for OpenStorageFuture<'a, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS, REGION_SIZE, REGION_COUNT, IO>
where
    IO: FlashIo,
{
    type Output = Result<Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, crate::StorageOpenError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let phase = mem::replace(&mut this.phase, OpenStoragePhase::Done);

        match phase {
            OpenStoragePhase::Begin => {
                let plan = crate::startup::begin_open_formatted_store::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(this.flash, this.workspace)?;
                this.phase = OpenStoragePhase::RecoverRotation { plan };
                Poll::Pending
            }
            OpenStoragePhase::RecoverRotation { mut plan } => {
                crate::startup::recover_open_rotation::<
                    REGION_SIZE,
                    IO,
                    REGION_COUNT,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(this.flash, this.workspace, &mut plan)?;
                this.phase = OpenStoragePhase::DiscoverWalChain { plan };
                Poll::Pending
            }
            OpenStoragePhase::DiscoverWalChain { mut plan } => {
                crate::startup::discover_open_wal_chain::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(this.flash, this.workspace, &mut plan)?;
                this.phase = OpenStoragePhase::ReplayWalChain { plan };
                Poll::Pending
            }
            OpenStoragePhase::ReplayWalChain { mut plan } => {
                crate::startup::replay_open_wal_chain::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(this.flash, this.workspace, &mut plan)?;
                this.phase = OpenStoragePhase::FinishStartup { plan };
                Poll::Pending
            }
            OpenStoragePhase::FinishStartup { mut plan } => {
                let startup = crate::startup::finish_open_formatted_store::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(this.flash, &mut plan)?;
                let runtime = crate::storage::from_startup_state(startup)?;
                this.phase = OpenStoragePhase::RecoverPendingReclaims { runtime };
                Poll::Pending
            }
            OpenStoragePhase::RecoverPendingReclaims { mut runtime } => {
                runtime.recover_pending_reclaims::<REGION_SIZE, REGION_COUNT, IO>(
                    this.flash,
                    this.workspace,
                )?;
                this.phase = OpenStoragePhase::ValidateCollections {
                    storage: Storage::from_runtime(runtime),
                };
                Poll::Pending
            }
            OpenStoragePhase::ValidateCollections { storage } => {
                storage.validate_live_collections::<REGION_SIZE, REGION_COUNT, IO>(
                    this.flash,
                    this.workspace,
                )?;
                this.phase = OpenStoragePhase::Done;
                Poll::Ready(Ok(storage))
            }
            OpenStoragePhase::Done => Poll::Pending,
        }
    }
}
