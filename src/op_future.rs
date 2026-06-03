use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::mode::{CollectionFlushMode, OpenMode, StorageMode, WalHeadReclaimMode};
use crate::{
    CollectionType, FlashIo, LsmKey, LsmValue, MapFrontier, MapStorageError,
    StartupCollectionBasis, Storage, StorageFormatConfig, StorageMemory, StorageOpenError,
    StorageRuntimeError,
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
    'mem,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize = 8,
> where
    IO: FlashIo,
{
    backing: Option<&'db mut IO>,
    memory: Option<&'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>>,
    config: StorageFormatConfig,
}

impl<
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > FormatStorageFuture<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
    pub(crate) fn new(
        backing: &'db mut IO,
        config: StorageFormatConfig,
        memory: &'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Self {
        Self {
            backing: Some(backing),
            memory: Some(memory),
            config,
        }
    }
}

impl<
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > Unpin for FormatStorageFuture<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
}

impl<
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > Future for FormatStorageFuture<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
    type Output = Result<
        Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        StorageRuntimeError,
    >;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match (this.backing.take(), this.memory.take()) {
            (Some(backing), Some(memory)) => {
                Poll::Ready(Storage::format(backing, this.config, memory))
            }
            _ => Poll::Pending,
        }
    }
}

/// Caller-driven future for flushing a map through the manifest-backed path.
pub struct YieldingFlushMapFuture<
    'a,
    'db,
    'mem,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    K,
    V,
    const MAX_RUNS: usize,
> where
    IO: FlashIo,
    K: LsmKey,
    V: LsmValue,
{
    storage: &'a mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    map: &'a mut MapFrontier<'a, K, V, MAX_RUNS>,
    phase: u8,
}

impl<
        'a,
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        K,
        V,
        const MAX_RUNS: usize,
    >
    YieldingFlushMapFuture<
        'a,
        'db,
        'mem,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        K,
        V,
        MAX_RUNS,
    >
where
    IO: FlashIo,
    K: LsmKey,
    V: LsmValue,
{
    /// Creates a new yielding manifest flush future.
    pub fn new(
        storage: &'a mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        map: &'a mut MapFrontier<'a, K, V, MAX_RUNS>,
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
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        K,
        V,
        const MAX_RUNS: usize,
    > Unpin
    for YieldingFlushMapFuture<
        'a,
        'db,
        'mem,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        K,
        V,
        MAX_RUNS,
    >
where
    IO: FlashIo,
    K: LsmKey,
    V: LsmValue,
{
}

impl<
        'a,
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        K,
        V,
        const MAX_RUNS: usize,
    > Future
    for YieldingFlushMapFuture<
        'a,
        'db,
        'mem,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        K,
        V,
        MAX_RUNS,
    >
where
    IO: FlashIo,
    K: LsmKey,
    V: LsmValue,
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
                let result = this.storage.flush_map_inner::<K, V, MAX_RUNS>(this.map);
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
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        K,
        V,
        const MAX_RUNS: usize,
    > Drop
    for YieldingFlushMapFuture<
        'a,
        'db,
        'mem,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        K,
        V,
        MAX_RUNS,
    >
where
    IO: FlashIo,
    K: LsmKey,
    V: LsmValue,
{
    fn drop(&mut self) {
        self.storage.finish_mode();
    }
}

#[derive(Debug)]
pub(crate) enum ReclaimWalHeadPhase<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize = 8> {
    Plan,
    RotateToContinuation { new_head: Option<u32> },
    BeginReclaim { next_index: usize, new_head: u32 },
    CopyLiveState { new_head: u32 },
    CommitHead { new_head: u32 },
    CompleteReclaim { next_index: usize, new_head: u32 },
    Done,
}

pub(crate) enum OpenStoragePhase<const REGION_COUNT: usize, const MAX_COLLECTIONS: usize = 8> {
    Begin,
    RecoverRotation,
    ReplayWalChain,
    FinishStartup,
    ValidateCollections,
    Done,
}

/// Explicit phase-machine future for reclaiming the current WAL head.
pub struct ReclaimWalHeadFuture<
    'a,
    'db,
    'mem,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize = 8,
> where
    IO: FlashIo,
{
    storage: &'a mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    phase: ReclaimWalHeadPhase<REGION_COUNT, MAX_COLLECTIONS>,
}

/// Explicit phase-machine future for opening storage through replay.
pub struct OpenStorageFuture<
    'db,
    'mem,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize = 8,
> where
    IO: FlashIo,
{
    backing: Option<&'db mut IO>,
    memory: Option<&'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>>,
    phase: OpenStoragePhase<REGION_COUNT, MAX_COLLECTIONS>,
}

impl<
        'a,
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > ReclaimWalHeadFuture<'a, 'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
    /// Creates a new WAL-head reclaim future.
    pub fn new(
        storage: &'a mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Self {
        Self {
            storage,
            phase: ReclaimWalHeadPhase::Plan,
        }
    }
}

impl<
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > OpenStorageFuture<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
    /// Creates a new open-storage future.
    pub fn new(
        backing: &'db mut IO,
        memory: &'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Self {
        Self {
            backing: Some(backing),
            memory: Some(memory),
            phase: OpenStoragePhase::Begin,
        }
    }
}

impl<
        'a,
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > Unpin for ReclaimWalHeadFuture<'a, 'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
}

impl<
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > Unpin for OpenStorageFuture<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
}

impl<
        'a,
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > Future for ReclaimWalHeadFuture<'a, 'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
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
                if let Err(error) = this
                    .storage
                    .memory
                    .state
                    .prepare_wal_head_reclaim::<REGION_SIZE, IO>(
                        this.storage.backing,
                        &mut this.storage.memory.workspace,
                        &mut this.storage.memory.reclaim_plan,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.storage.memory.reclaim_source_regions.clear();
                match this
                    .storage
                    .memory
                    .state
                    .collect_wal_head_reclaim_regions::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.memory.workspace,
                        &this.storage.memory.reclaim_plan,
                        &mut this.storage.memory.reclaim_source_regions,
                    ) {
                    Ok(()) => {}
                    Err(error) => {
                        this.storage.finish_mode();
                        return Poll::Ready(Err(error));
                    }
                };
                let plan = &mut this.storage.memory.reclaim_plan;
                let Some(new_head) = this.storage.memory.reclaim_source_regions.get(1).copied()
                else {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(
                        StorageRuntimeError::WalHeadReclaimRequiresMultipleWalRegions,
                    ));
                };
                plan.limit_to_source_tail(plan.old_head, REGION_SIZE);
                this.storage.memory.reclaim_source_regions.truncate(1);
                let new_head = Some(new_head);
                this.phase = ReclaimWalHeadPhase::RotateToContinuation { new_head };
                Poll::Pending
            }
            ReclaimWalHeadPhase::RotateToContinuation { new_head } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::BeginReclaim,
                    ));
                let new_head = if let Some(new_head) = new_head {
                    new_head
                } else {
                    if let Err(error) = this
                        .storage
                        .memory
                        .state
                        .rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(
                            this.storage.backing,
                            &mut this.storage.memory.workspace,
                        )
                    {
                        this.storage.finish_mode();
                        return Poll::Ready(Err(error));
                    }
                    this.storage.memory.state.wal_tail()
                };
                this.phase = ReclaimWalHeadPhase::BeginReclaim {
                    next_index: 0,
                    new_head,
                };
                Poll::Pending
            }
            ReclaimWalHeadPhase::BeginReclaim {
                next_index,
                new_head,
            } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::BeginReclaim,
                    ));
                let Some(region_index) = this
                    .storage
                    .memory
                    .reclaim_source_regions
                    .get(next_index)
                    .copied()
                else {
                    this.phase = ReclaimWalHeadPhase::CopyLiveState { new_head };
                    return Poll::Pending;
                };
                if let Err(error) = this
                    .storage
                    .memory
                    .state
                    .begin_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.memory.workspace,
                        region_index,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.phase = ReclaimWalHeadPhase::BeginReclaim {
                    next_index: next_index + 1,
                    new_head,
                };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CopyLiveState { new_head } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::CopyLiveState,
                    ));
                if let Err(error) = this
                    .storage
                    .memory
                    .state
                    .copy_live_wal_head_reclaim_state::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.memory.workspace,
                        &this.storage.memory.reclaim_plan,
                        &mut this.storage.memory.active_collections,
                        &mut this.storage.memory.open_plan,
                        #[cfg(feature = "perf-counters")]
                        None,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.phase = ReclaimWalHeadPhase::CommitHead { new_head };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CommitHead { new_head } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::CommitHead,
                    ));
                if let Err(error) = this
                    .storage
                    .memory
                    .state
                    .commit_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.memory.workspace,
                        new_head,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.phase = ReclaimWalHeadPhase::CompleteReclaim {
                    next_index: 0,
                    new_head,
                };
                Poll::Pending
            }
            ReclaimWalHeadPhase::CompleteReclaim {
                next_index,
                new_head,
            } => {
                this.storage
                    .set_mode_unchecked(StorageMode::ReclaimingWalHead(
                        WalHeadReclaimMode::CompleteReclaim,
                    ));
                let Some(region_index) = this
                    .storage
                    .memory
                    .reclaim_source_regions
                    .get(next_index)
                    .copied()
                else {
                    if let Err(error) =
                        crate::storage::open_into::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                            this.storage.backing,
                            &mut this.storage.memory.workspace,
                            &mut this.storage.memory.state,
                            &mut this.storage.memory.open_plan,
                        )
                    {
                        this.storage.finish_mode();
                        return Poll::Ready(Err(error));
                    }
                    this.storage.finish_mode();
                    this.phase = ReclaimWalHeadPhase::Done;
                    return Poll::Ready(Ok(new_head));
                };
                if let Err(error) = this
                    .storage
                    .memory
                    .state
                    .append_free_region_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        this.storage.backing,
                        &mut this.storage.memory.workspace,
                        crate::CollectionId(0),
                        region_index,
                    )
                {
                    this.storage.finish_mode();
                    return Poll::Ready(Err(error));
                }
                this.phase = ReclaimWalHeadPhase::CompleteReclaim {
                    next_index: next_index + 1,
                    new_head,
                };
                Poll::Pending
            }
            ReclaimWalHeadPhase::Done => Poll::Pending,
        }
    }
}

impl<
        'a,
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > Drop for ReclaimWalHeadFuture<'a, 'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
    fn drop(&mut self) {
        self.storage.finish_mode();
    }
}

impl<
        'db,
        'mem,
        IO,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > Future for OpenStorageFuture<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
where
    IO: FlashIo,
{
    type Output = Result<
        Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
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
                let memory = match this.memory.as_deref_mut() {
                    Some(memory) => memory,
                    None => return Poll::Pending,
                };
                crate::startup::begin_open_formatted_store::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                >(backing, &mut memory.workspace, &mut memory.open_plan)?;
                this.phase = OpenStoragePhase::RecoverRotation;
                Poll::Pending
            }
            OpenStoragePhase::RecoverRotation => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                let memory = match this.memory.as_deref_mut() {
                    Some(memory) => memory,
                    None => return Poll::Pending,
                };
                crate::startup::recover_open_rotation::<
                    REGION_SIZE,
                    IO,
                    REGION_COUNT,
                    MAX_COLLECTIONS,
                >(backing, &mut memory.workspace, &mut memory.open_plan)?;
                this.phase = OpenStoragePhase::ReplayWalChain;
                Poll::Pending
            }
            OpenStoragePhase::ReplayWalChain => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                let memory = match this.memory.as_deref_mut() {
                    Some(memory) => memory,
                    None => return Poll::Pending,
                };
                crate::startup::replay_open_wal_chain::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                >(backing, &mut memory.workspace, &mut memory.open_plan)?;
                this.phase = OpenStoragePhase::FinishStartup;
                Poll::Pending
            }
            OpenStoragePhase::FinishStartup => {
                let backing = match this.backing.as_deref_mut() {
                    Some(backing) => backing,
                    None => return Poll::Pending,
                };
                let memory = match this.memory.as_deref_mut() {
                    Some(memory) => memory,
                    None => return Poll::Pending,
                };
                crate::startup::finish_open_formatted_store_into_runtime::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                >(backing, &mut memory.open_plan, &mut memory.state)?;
                this.phase = OpenStoragePhase::ValidateCollections;
                Poll::Pending
            }
            OpenStoragePhase::ValidateCollections => {
                let memory = match this.memory.as_deref_mut() {
                    Some(memory) => memory,
                    None => return Poll::Pending,
                };
                let runtime = &memory.state;
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
                let backing = this.backing.take().ok_or(StorageOpenError::Runtime(
                    StorageRuntimeError::InvalidStorageMode {
                        expected: StorageMode::Opening(OpenMode::Finish),
                        actual: StorageMode::Idle,
                    },
                ))?;
                let memory = this.memory.take().ok_or(StorageOpenError::Runtime(
                    StorageRuntimeError::StorageMemoryUninitialized,
                ))?;
                this.phase = OpenStoragePhase::Done;
                Poll::Ready(Storage::from_initialized_memory(backing, memory).map_err(Into::into))
            }
            OpenStoragePhase::Done => Poll::Pending,
        }
    }
}
