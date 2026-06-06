//! Borromean is a `no_std` flash storage engine built around an
//! append-only ring, caller-provided I/O, and storage-owned scratch memory.
//!
//! The main ownership model is:
//!
//! - [`StorageMemory`] owns logical storage state, bounded operation scratch, and durability
//!   invariants.
//! - [`Storage`] binds exclusive mutable access to a caller-provided [`FlashIo`] backing object.
//! - [`StorageWorkspace`] remains an advanced internal/test-support scratch type.
//!
//! Tier 1 supported APIs are [`Storage`], [`FlashIo`],
//! [`CollectionId`], [`CollectionType`], [`LsmMap`], [`MapUpdate`],
//! and [`MockFlash`] for tests and examples.
//! With the `embedded-storage` feature enabled, `EmbeddedStorageFlash`
//! is the Tier 1 adapter for `embedded-storage` NOR flash drivers.
//! Low-level modules such as [`disk`], [`wal_record`], [`startup`], and
//! [`storage`] are documented as advanced reference surfaces.
//!
//! # Example
//!
//! ```
//! use borromean::{
//!     LsmMap, LsmMapMemory, MockFlash, Storage, StorageFormatConfig, StorageMemory,
//! };
//!
//! const REGION_SIZE: usize = 512;
//! const REGION_COUNT: usize = 8;
//! const MAX_COLLECTIONS: usize = 8;
//!
//! let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
//!
//! let collection_id = {
//!     let mut storage_memory = StorageMemory::<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::new();
//!     let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::format(
//!         &mut flash,
//!         StorageFormatConfig::new(2, 8, 0xa5),
//!         &mut storage_memory,
//!     )
//!     .unwrap();
//!
//!     let mut map_memory = LsmMapMemory::<u16, u16>::new();
//!     let mut map = LsmMap::<u16, u16>::new(&mut storage, &mut map_memory).unwrap();
//!
//!     map.set(&mut storage, 7, 70).unwrap();
//!     assert_eq!(map.get(&mut storage, &7, |_, value| *value).unwrap(), Some(70));
//!
//!     map.collection_id()
//! };
//!
//! let mut reopen_memory = StorageMemory::<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::new();
//! let mut reopened = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::open(
//!     &mut flash,
//!     &mut reopen_memory,
//! )
//! .unwrap();
//!
//! let mut reopened_map_memory = LsmMapMemory::<u16, u16>::new();
//! let mut reopened_map =
//!     LsmMap::<u16, u16>::open(collection_id, &mut reopened, &mut reopened_map_memory).unwrap();
//!
//! assert_eq!(
//!     reopened_map
//!         .get(&mut reopened, &7, |_, value| *value)
//!         .unwrap(),
//!     Some(70)
//! );
//! ```
#![no_std]
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unreachable,
        clippy::disallowed_methods,
        clippy::disallowed_types,
        clippy::disallowed_macros
    )
)]

#[cfg(all(feature = "file-backing", not(target_os = "linux")))]
compile_error!("the file-backing feature is currently supported only on Linux");

#[cfg(any(test, feature = "std", feature = "file-backing"))]
extern crate std;

#[cfg(test)]
#[allow(unused_mut, unused_variables)]
mod tests;

#[cfg(test)]
pub(crate) fn test_storage_memory<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>() -> &'static mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS> {
    std::boxed::Box::leak(std::boxed::Box::new(StorageMemory::new()))
}

#[cfg(test)]
pub(crate) fn test_lsm_map_memory<K, V, const MAX_RUNS: usize>(
) -> &'static mut LsmMapMemory<K, V, MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    std::boxed::Box::leak(std::boxed::Box::new(LsmMapMemory::new()))
}

#[cfg(test)]
pub(crate) fn test_map_frontier_memory<K, const MAX_RUNS: usize>(
) -> &'static mut MapFrontierMemory<K, MAX_RUNS> {
    std::boxed::Box::leak(std::boxed::Box::new(MapFrontierMemory::new()))
}

#[cfg(all(test, feature = "file-backing", target_os = "linux"))]
pub(crate) fn test_file_backing_scratch() -> &'static mut FileBackingScratch {
    std::boxed::Box::leak(std::boxed::Box::new(FileBackingScratch::new()))
}

/// Advanced reference types for exact metadata and region-header bytes.
pub mod disk;
pub use disk::*;

/// Test and example backends for exercising the storage engine in memory.
pub mod mock;
pub use mock::*;

/// Tier 1 I/O trait implemented by caller-owned flash adapters.
pub mod flash_io;
pub use flash_io::*;

/// Feature-gated `embedded-storage` NOR flash adapter.
#[cfg(feature = "embedded-storage")]
pub mod embedded_storage;
#[cfg(feature = "embedded-storage")]
pub use embedded_storage::*;

/// Linux host-file backing implemented with a mutable mmap.
#[cfg(all(feature = "file-backing", target_os = "linux"))]
pub mod file_backing;
#[cfg(all(feature = "file-backing", target_os = "linux"))]
pub use file_backing::*;

/// Tier 1 workspace buffers borrowed by replay and mutation operations.
pub mod workspace;
pub use workspace::*;

/// Explicit storage operation modes used by the public storage context.
pub mod mode;
pub use mode::*;

/// Advanced replay and startup-state types used by open and recovery.
pub mod startup;
pub use startup::*;

/// Advanced runtime-state and low-level storage operation helpers.
pub mod storage;
pub use storage::*;

/// Advanced reference types for WAL record encoding and decoding.
pub mod wal_record;
pub use wal_record::*;

/// Future helpers for caller-driven async-style storage operations.
pub mod op_future;
pub use op_future::*;

#[cfg(feature = "perf-counters")]
pub mod perf_metrics;
#[cfg(feature = "perf-counters")]
pub use perf_metrics::*;

mod collections;
pub use collections::*;

/// Small vector-like abstractions used by advanced collection helpers.
pub mod vec_like;
pub use vec_like::*;

use core::fmt::Debug;
use core::future::Future;
use heapless::Vec;
use serde::{Deserialize, Serialize};

#[cfg(feature = "perf-counters")]
use crate::perf_metrics::{StoragePerfCounter, StoragePerfTimer, StoragePerfTimerGuard};

type CollectionIdCounter = u64;

/// Stable identifier for a collection tracked by the storage engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct CollectionId(pub(crate) CollectionIdCounter);

impl CollectionId {
    /// Creates a collection identifier from a raw stable integer value.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the identifier encoded as little-endian bytes.
    pub fn to_le_bytes(&self) -> [u8; size_of::<CollectionIdCounter>()] {
        self.0.to_le_bytes()
    }

    /// Returns the next identifier, or `None` on integer overflow.
    pub fn increment(&self) -> Option<Self> {
        let next = self.0.checked_add(1)?;
        Some(Self(next))
    }
}

/// Stable collection kinds recognized by the storage engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionType {
    /// Placeholder type for uninitialized regions or values.
    Uninitialized,
    /// Free-list region marker.
    Free, // Used for free regions
    /// Write-ahead-log collection type.
    Wal, // Write-ahead log
    /// Experimental channel collection type.
    Channel, // FIFO queue
    /// Durable map collection type.
    Map, // Key-value store
    /// WAL-backed opaque object log collection type.
    ObjectLog, // Opaque object log
}

impl CollectionType {
    /// Stable on-disk code reserved for WAL collections.
    pub const WAL_CODE: u16 = 0;
    /// Stable on-disk code reserved for the experimental channel type.
    pub const CHANNEL_CODE: u16 = 1;
    /// Stable on-disk code reserved for durable map collections.
    pub const MAP_CODE: u16 = 2;
    /// Stable on-disk code reserved for durable object-log collections.
    pub const OBJECT_LOG_CODE: u16 = 3;

    /// Returns the stable on-disk code for durable collection kinds.
    pub fn stable_code(self) -> Option<u16> {
        match self {
            Self::Wal => Some(Self::WAL_CODE),
            Self::Channel => Some(Self::CHANNEL_CODE),
            Self::Map => Some(Self::MAP_CODE),
            Self::ObjectLog => Some(Self::OBJECT_LOG_CODE),
            Self::Uninitialized | Self::Free => None,
        }
    }
}

/// Common metadata exposed by collection-specific APIs.
pub trait Collection {
    /// Returns the stable collection identifier.
    fn id(&self) -> CollectionId;
    /// Returns the collection kind.
    fn collection_type(&self) -> CollectionType;
}

/// Configuration used when formatting a new store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageFormatConfig {
    /// Minimum number of regions that must remain reserved for recovery.
    pub min_free_regions: u32,
    /// Required alignment for physical WAL record starts and lengths.
    pub wal_write_granule: u32,
    /// Physical byte that starts each WAL record.
    pub wal_record_magic: u8,
}

impl StorageFormatConfig {
    /// Creates a storage format configuration.
    pub const fn new(min_free_regions: u32, wal_write_granule: u32, wal_record_magic: u8) -> Self {
        Self {
            min_free_regions,
            wal_write_granule,
            wal_record_magic,
        }
    }
}

/// Collection kind currently occupying the storage-owned hot frontier buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierBufferOwner {
    /// The hot frontier buffer is available for reuse.
    Empty { generation: u64 },
    /// The hot frontier buffer contains a map frontier for a collection.
    Map {
        /// Collection currently represented in the buffer.
        collection_id: CollectionId,
        /// Generation used to invalidate stale typed capabilities.
        generation: u64,
        /// Whether the frontier has uncheckpointed updates.
        dirty: bool,
    },
}

impl FrontierBufferOwner {
    fn generation(self) -> u64 {
        match self {
            Self::Empty { generation } | Self::Map { generation, .. } => generation,
        }
    }
}

/// Tier 1 storage facade for formatting, opening, and mutating a store.
pub struct Storage<
    'db,
    'mem,
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize = 8,
> {
    pub(crate) backing: &'db mut IO,
    pub(crate) memory: &'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
}

/// Caller-owned memory borrowed by [`Storage`] and its operation futures.
pub struct StorageMemory<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize = 8,
> {
    pub(crate) workspace: StorageWorkspace<REGION_SIZE>,
    pub(crate) state: StorageRuntime<MAX_COLLECTIONS>,
    pub(crate) dirty_frontiers: Vec<CollectionId, MAX_COLLECTIONS>,
    pub(crate) payload_scratch: [u8; REGION_SIZE],
    pub(crate) checkpoint_scratch: [u8; REGION_SIZE],
    pub(crate) collection_scratch: [u8; REGION_SIZE],
    pub(crate) open_scratch: [u8; REGION_SIZE],
    pub(crate) frontier_buffer_owner: FrontierBufferOwner,
    pub(crate) open_plan: crate::startup::StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    pub(crate) reclaim_plan: crate::storage::WalHeadReclaimPlan<MAX_COLLECTIONS>,
    pub(crate) reclaim_source_regions: Vec<u32, REGION_COUNT>,
    pub(crate) active_collections: Vec<CollectionId, MAX_COLLECTIONS>,
    pub(crate) wal_chain_scratch: Vec<u32, REGION_COUNT>,
    #[cfg(feature = "perf-counters")]
    pub(crate) perf_metrics: StoragePerfMetrics,
    pub(crate) mode: StorageMode,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_COLLECTIONS: usize>
    StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
{
    /// Allocates caller-owned storage memory.
    pub fn new() -> Self {
        Self {
            workspace: StorageWorkspace::new(),
            state: StorageRuntime::empty(),
            dirty_frontiers: Vec::new(),
            payload_scratch: [0; REGION_SIZE],
            checkpoint_scratch: [0; REGION_SIZE],
            collection_scratch: [0; REGION_SIZE],
            open_scratch: [0; REGION_SIZE],
            frontier_buffer_owner: FrontierBufferOwner::Empty { generation: 0 },
            open_plan: crate::startup::StartupOpenPlan::empty(),
            reclaim_plan: crate::storage::WalHeadReclaimPlan::empty(),
            reclaim_source_regions: Vec::new(),
            active_collections: Vec::new(),
            wal_chain_scratch: Vec::new(),
            #[cfg(feature = "perf-counters")]
            perf_metrics: StoragePerfMetrics::default(),
            mode: StorageMode::Idle,
        }
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_COLLECTIONS: usize> Default
    for StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
{
    fn default() -> Self {
        Self::new()
    }
}

/// Errors returned while opening storage through [`Storage::open`] or
/// [`Storage::open_future`].
#[derive(Debug)]
pub enum StorageOpenError {
    /// The shared storage runtime rejected the open path.
    Runtime(StorageRuntimeError),
    /// Replay discovered a live collection type that this build does not support.
    UnsupportedLiveCollectionType(u16),
    /// Map-specific validation failed while opening live map collections.
    Map(MapStorageError),
}

impl From<StorageRuntimeError> for StorageOpenError {
    fn from(error: StorageRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<StartupError> for StorageOpenError {
    fn from(error: StartupError) -> Self {
        Self::Runtime(error.into())
    }
}

impl From<MapStorageError> for StorageOpenError {
    fn from(error: MapStorageError) -> Self {
        Self::Map(error)
    }
}

fn dirty_frontier_is_active_in<const MAX_COLLECTIONS: usize>(
    dirty_frontiers: &Vec<CollectionId, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> bool {
    dirty_frontiers.contains(&collection_id)
}

fn ensure_dirty_frontier_budget_for<const MAX_COLLECTIONS: usize>(
    state: &StorageRuntime<MAX_COLLECTIONS>,
    dirty_frontiers: &Vec<CollectionId, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Result<(), StorageRuntimeError> {
    if dirty_frontier_is_active_in(dirty_frontiers, collection_id) {
        return Ok(());
    }

    let dirty_after = dirty_frontiers
        .len()
        .checked_add(1)
        .ok_or(StorageRuntimeError::TooManyTrackedCollections)?;
    let required_min_free_regions = u32::try_from(
        dirty_after
            .checked_add(1)
            .ok_or(StorageRuntimeError::TooManyTrackedCollections)?,
    )
    .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)?;
    if required_min_free_regions > state.metadata().min_free_regions {
        return Err(StorageRuntimeError::TooManyDirtyFrontiers {
            dirty_frontiers: dirty_after,
            min_free_regions: state.metadata().min_free_regions,
        });
    }
    Ok(())
}

fn mark_dirty_frontier_in<const MAX_COLLECTIONS: usize>(
    dirty_frontiers: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Result<(), StorageRuntimeError> {
    if dirty_frontiers.contains(&collection_id) {
        return Ok(());
    }
    dirty_frontiers
        .push(collection_id)
        .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)
}

fn clear_dirty_frontier_in<const MAX_COLLECTIONS: usize>(
    dirty_frontiers: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) {
    if let Some(index) = dirty_frontiers
        .iter()
        .position(|active| *active == collection_id)
    {
        dirty_frontiers.remove(index);
    }
}

enum AppliedMapUpdate {
    Undo(crate::collections::map::MapMutationUndo),
    Checkpoint(crate::collections::map::MapCheckpoint),
}

#[allow(clippy::too_many_arguments)]
fn apply_map_frontier_update_parts<
    K,
    V,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_RUNS: usize,
>(
    state: &mut StorageRuntime<MAX_COLLECTIONS>,
    backing: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    dirty_frontiers: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    payload_scratch: &mut [u8; REGION_SIZE],
    checkpoint_scratch: &mut [u8; REGION_SIZE],
    reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
    active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    reclaim_plan: &mut storage::WalHeadReclaimPlan<MAX_COLLECTIONS>,
    open_plan: &mut startup::StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    #[cfg(feature = "perf-counters")] perf_metrics: &mut StoragePerfMetrics,
    map: &mut MapFrontier<'_, K, V, MAX_RUNS>,
    update: &MapUpdate<K, V>,
) -> Result<(), MapStorageError>
where
    IO: FlashIo,
    K: LsmKey,
    V: LsmValue,
{
    let collection_id = map.id();
    let Some(collection) = state
        .collections()
        .iter()
        .find(|collection| collection.collection_id() == collection_id)
    else {
        return Err(MapStorageError::UnknownCollection(collection_id));
    };
    if collection.basis() == StartupCollectionBasis::Dropped {
        return Err(MapStorageError::DroppedCollection(collection_id));
    }
    if collection.collection_type() != Some(CollectionType::MAP_CODE) {
        return Err(MapStorageError::CollectionTypeMismatch {
            collection_id,
            expected: CollectionType::MAP_CODE,
            actual: collection.collection_type(),
        });
    }
    ensure_dirty_frontier_budget_for(state, dirty_frontiers, collection_id)
        .map_err(MapStorageError::from)?;

    #[cfg(feature = "perf-counters")]
    let encode_timer = StoragePerfTimerGuard::start();
    let encoded_update = MapFrontier::<K, V>::encode_update_into(update, payload_scratch);
    #[cfg(feature = "perf-counters")]
    {
        perf_metrics.add_nanos(StoragePerfTimer::UpdateEncode, encode_timer.elapsed_nanos());
        if let Ok(used) = encoded_update {
            perf_metrics.increment(StoragePerfCounter::UpdateEncodes);
            perf_metrics.add(StoragePerfCounter::EncodedUpdateBytes, used as u64);
        }
    }
    let used = encoded_update?;

    #[cfg(feature = "perf-counters")]
    let apply_timer = StoragePerfTimerGuard::start();
    let apply_result =
        map.apply_update_payload_with_undo(&payload_scratch[..used], checkpoint_scratch);
    #[cfg(feature = "perf-counters")]
    {
        perf_metrics.add_nanos(StoragePerfTimer::FrontierApply, apply_timer.elapsed_nanos());
        if let Ok(undo) = apply_result.as_ref() {
            perf_metrics.increment(StoragePerfCounter::FrontierApplies);
            perf_metrics.increment(StoragePerfCounter::FrontierUndoRecords);
            perf_metrics.add(
                StoragePerfCounter::FrontierUndoBytes,
                undo.saved_bytes_len() as u64,
            );
        }
    }

    let applied = match apply_result {
        Ok(undo) => AppliedMapUpdate::Undo(undo),
        Err(MapError::BufferTooSmall) => {
            if map.frontier_is_empty() {
                return Err(MapError::BufferTooSmall.into());
            }
            #[cfg(feature = "perf-counters")]
            {
                perf_metrics.increment(StoragePerfCounter::BufferTooSmallErrors);
                perf_metrics.increment(StoragePerfCounter::FrontierFullCheckpointFallbacks);
            }

            #[cfg(feature = "perf-counters")]
            let checkpoint_timer = StoragePerfTimerGuard::start();
            let checkpoint = map.checkpoint_into(checkpoint_scratch)?;
            #[cfg(feature = "perf-counters")]
            {
                perf_metrics.increment(StoragePerfCounter::FrontierCheckpoints);
                perf_metrics.add_nanos(
                    StoragePerfTimer::FrontierCheckpoint,
                    checkpoint_timer.elapsed_nanos(),
                );
            }

            #[cfg(feature = "perf-counters")]
            let apply_timer = StoragePerfTimerGuard::start();
            let apply_result = map.apply_update_payload(&payload_scratch[..used]);
            #[cfg(feature = "perf-counters")]
            {
                perf_metrics
                    .add_nanos(StoragePerfTimer::FrontierApply, apply_timer.elapsed_nanos());
                if apply_result.is_ok() {
                    perf_metrics.increment(StoragePerfCounter::FrontierApplies);
                }
            }

            match apply_result {
                Ok(()) => AppliedMapUpdate::Checkpoint(checkpoint),
                Err(MapError::BufferTooSmall) => {
                    map.restore_from_checkpoint(checkpoint, checkpoint_scratch)?;

                    #[cfg(feature = "perf-counters")]
                    let flush_timer = StoragePerfTimerGuard::start();
                    let flush_result = map
                        .flush_to_storage::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                            state,
                            backing,
                            workspace,
                            reclaim_source_regions,
                            active_collections,
                            reclaim_plan,
                            open_plan,
                        );
                    #[cfg(feature = "perf-counters")]
                    {
                        let flush_nanos = flush_timer.elapsed_nanos();
                        perf_metrics.increment(StoragePerfCounter::OverflowFlushes);
                        perf_metrics.increment(StoragePerfCounter::Flushes);
                        perf_metrics.add_nanos(StoragePerfTimer::OverflowFlush, flush_nanos);
                        perf_metrics.add_nanos(StoragePerfTimer::Flush, flush_nanos);
                    }
                    flush_result?;
                    clear_dirty_frontier_in(dirty_frontiers, collection_id);

                    #[cfg(feature = "perf-counters")]
                    let retry_apply_timer = StoragePerfTimerGuard::start();
                    let retry_apply_result = map.apply_update_payload_with_undo(
                        &payload_scratch[..used],
                        checkpoint_scratch,
                    );
                    #[cfg(feature = "perf-counters")]
                    {
                        perf_metrics.add_nanos(
                            StoragePerfTimer::FrontierApply,
                            retry_apply_timer.elapsed_nanos(),
                        );
                        if let Ok(undo) = retry_apply_result.as_ref() {
                            perf_metrics.increment(StoragePerfCounter::FrontierApplies);
                            perf_metrics.increment(StoragePerfCounter::FrontierUndoRecords);
                            perf_metrics.add(
                                StoragePerfCounter::FrontierUndoBytes,
                                undo.saved_bytes_len() as u64,
                            );
                        }
                    }
                    match retry_apply_result {
                        Ok(undo) => AppliedMapUpdate::Undo(undo),
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) => {
                    map.restore_from_checkpoint(checkpoint, checkpoint_scratch)?;
                    return Err(error.into());
                }
            }
        }
        Err(error) => return Err(error.into()),
    };

    #[cfg(feature = "perf-counters")]
    let append_result = state.append_update_with_rotation_metered::<REGION_SIZE, REGION_COUNT, IO>(
        backing,
        workspace,
        collection_id,
        &payload_scratch[..used],
        perf_metrics,
    );
    #[cfg(not(feature = "perf-counters"))]
    let append_result = state.append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
        backing,
        workspace,
        collection_id,
        &payload_scratch[..used],
    );
    if let Err(error) = append_result {
        #[cfg(feature = "perf-counters")]
        perf_metrics.increment(StoragePerfCounter::AppendFailures);
        match applied {
            AppliedMapUpdate::Undo(undo) => {
                #[cfg(feature = "perf-counters")]
                perf_metrics.increment(StoragePerfCounter::FrontierUndoRestores);
                map.restore_from_mutation_undo(undo, checkpoint_scratch)?;
            }
            AppliedMapUpdate::Checkpoint(checkpoint) => {
                map.restore_from_checkpoint(checkpoint, checkpoint_scratch)?;
            }
        }
        return Err(error.into());
    }

    mark_dirty_frontier_in(dirty_frontiers, collection_id).map_err(MapStorageError::from)
}

#[allow(clippy::too_many_arguments)]
fn compact_map_frontier_parts<
    'a,
    K,
    V,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_RUNS: usize,
>(
    state: &mut StorageRuntime<MAX_COLLECTIONS>,
    backing: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    dirty_frontiers: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    collection_scratch: &'a mut [u8; REGION_SIZE],
    reclaim_source_regions: &mut Vec<u32, REGION_COUNT>,
    active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    reclaim_plan: &mut storage::WalHeadReclaimPlan<MAX_COLLECTIONS>,
    open_plan: &mut startup::StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS>,
    collection_id: CollectionId,
    run_target: usize,
    mut opened: MapFrontier<'a, K, V, MAX_RUNS>,
    compaction_cursors: &mut heapless::Vec<crate::collections::map::RunEntryCursor<K, V>, MAX_RUNS>,
    duplicate_indices: &mut heapless::Vec<usize, MAX_RUNS>,
    retained_runs: &'a mut heapless::Vec<crate::collections::map::MapRunDescriptor<K>, MAX_RUNS>,
) -> Result<(crate::collections::map::MapFrontierState, Option<u32>), MapStorageError>
where
    IO: FlashIo,
    K: LsmKey,
    V: LsmValue,
{
    if run_target == 0 {
        return Err(MapStorageError::InvalidRunTarget);
    }

    let Some(selected_runs) = opened.selected_compaction_run_count(run_target)? else {
        return Ok((opened.into_state(), None));
    };
    let frontier_generation = opened.next_run_generation().saturating_add(1);

    let mut planned_allocations = opened
        .selected_compaction_region_count(selected_runs)?
        .checked_add(1)
        .ok_or(MapStorageError::Map(
            crate::collections::map::MapError::SerializationError,
        ))?;
    if !opened.frontier_is_empty() {
        planned_allocations = planned_allocations
            .checked_add(opened.planned_frontier_run_region_count(workspace, frontier_generation)?)
            .ok_or(MapStorageError::Map(
                crate::collections::map::MapError::SerializationError,
            ))?;
    }
    state.ensure_foreground_allocation_headroom_for::<REGION_SIZE, REGION_COUNT, IO>(
        backing,
        workspace,
        reclaim_source_regions,
        active_collections,
        reclaim_plan,
        open_plan,
        planned_allocations,
    )?;
    state.begin_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
        backing,
        workspace,
        collection_id,
    )?;

    let replacement_run = opened
        .write_compacted_run_to_storage::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            state,
            backing,
            workspace,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
            selected_runs,
            compaction_cursors,
            duplicate_indices,
            collection_scratch,
            retained_runs,
        )?;
    retained_runs.clear();
    let frontier_run = if opened.frontier_is_empty() {
        None
    } else {
        opened.write_frontier_run_to_storage::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            state,
            backing,
            workspace,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
            frontier_generation,
        )?
    };
    let mut replacement = MapFrontier::<K, V, MAX_RUNS>::new_with_runs(
        collection_id,
        collection_scratch,
        retained_runs,
    )?;
    if let Some(run) = replacement_run {
        replacement.push_retained_run(run)?;
    }
    opened.move_unselected_runs_into(selected_runs, &mut replacement)?;
    let manifest_region = replacement
        .commit_manifest_to_storage::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            state,
            backing,
            workspace,
            reclaim_source_regions,
            active_collections,
            reclaim_plan,
            open_plan,
            frontier_run,
        )?;
    opened.reclaim_run_regions::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
        state, backing, workspace,
    )?;
    state.finish_collection_transaction::<REGION_SIZE, REGION_COUNT, IO>(
        backing,
        workspace,
        collection_id,
    )?;
    opened.clear_retained_runs();
    replacement.move_unselected_runs_into(0, &mut opened)?;
    clear_dirty_frontier_in(dirty_frontiers, collection_id);
    Ok((replacement.into_state(), Some(manifest_region)))
}

impl<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    > Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
{
    /// Formats an empty store and returns it as a caller-driven future.
    pub fn format_future(
        backing: &'db mut IO,
        config: StorageFormatConfig,
        memory: &'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> FormatStorageFuture<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS> {
        FormatStorageFuture::new(backing, config, memory)
    }

    /// Formats an empty store and returns the opened [`Storage`] state.
    pub fn format(
        backing: &'db mut IO,
        config: StorageFormatConfig,
        memory: &'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<Self, StorageRuntimeError> {
        storage::format_into::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            backing,
            &mut memory.workspace,
            &mut memory.state,
            &mut memory.open_plan,
            config.min_free_regions,
            config.wal_write_granule,
            config.wal_record_magic,
        )?;
        Self::from_initialized_memory(backing, memory)
    }

    /// Opens an already formatted store as a caller-driven future.
    pub fn open_future(
        backing: &'db mut IO,
        memory: &'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> OpenStorageFuture<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS> {
        OpenStorageFuture::<IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::new(backing, memory)
    }

    /// Opens an already formatted store and validates live collections.
    pub fn open(
        backing: &'db mut IO,
        memory: &'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<Self, StorageOpenError> {
        storage::reopen_without_reclaim_recovery_into::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(
            backing,
            &mut memory.workspace,
            &mut memory.state,
            &mut memory.open_plan,
        )?;
        let storage = Self::from_initialized_memory(backing, memory)?;
        storage.validate_live_collections()?;
        Ok(storage)
    }

    /// Consumes the storage context and returns its bound backing object.
    pub fn into_backing(self) -> &'db mut IO {
        self.backing
    }

    /// Returns the current active storage mode.
    pub fn mode(&self) -> StorageMode {
        self.memory.mode
    }

    /// Returns current performance metrics for this storage context.
    #[cfg(feature = "perf-counters")]
    pub fn perf_metrics(&self) -> StoragePerfMetrics {
        self.memory.perf_metrics
    }

    /// Resets performance metrics for this storage context.
    #[cfg(feature = "perf-counters")]
    pub fn reset_perf_metrics(&mut self) {
        self.memory.perf_metrics = StoragePerfMetrics::default();
    }

    /// Returns current performance metrics and resets this storage context.
    #[cfg(feature = "perf-counters")]
    pub fn take_perf_metrics(&mut self) -> StoragePerfMetrics {
        core::mem::take(&mut self.memory.perf_metrics)
    }

    /// Returns the current owner of the storage-owned hot frontier buffer.
    pub fn frontier_buffer_owner(&self) -> FrontierBufferOwner {
        self.memory.frontier_buffer_owner
    }

    fn cached_map_frontier_generation(&self, collection_id: CollectionId) -> Option<u64> {
        match self.memory.frontier_buffer_owner {
            FrontierBufferOwner::Map {
                collection_id: active,
                generation,
                ..
            } if active == collection_id => Some(generation),
            _ => None,
        }
    }

    fn assign_map_frontier_buffer(&mut self, collection_id: CollectionId) -> u64 {
        let generation = match self.memory.frontier_buffer_owner {
            FrontierBufferOwner::Map {
                collection_id: active,
                generation,
                ..
            } if active == collection_id => generation,
            other => other.generation().wrapping_add(1),
        };
        self.memory.frontier_buffer_owner = FrontierBufferOwner::Map {
            collection_id,
            generation,
            dirty: false,
        };
        generation
    }

    fn mark_map_frontier_dirty(&mut self, collection_id: CollectionId) {
        if let FrontierBufferOwner::Map {
            collection_id: active,
            generation,
            ..
        } = self.memory.frontier_buffer_owner
        {
            if active == collection_id {
                self.memory.frontier_buffer_owner = FrontierBufferOwner::Map {
                    collection_id,
                    generation,
                    dirty: true,
                };
            }
        }
    }

    fn mark_map_frontier_clean(&mut self, collection_id: CollectionId) {
        if let FrontierBufferOwner::Map {
            collection_id: active,
            generation,
            ..
        } = self.memory.frontier_buffer_owner
        {
            if active == collection_id {
                self.memory.frontier_buffer_owner = FrontierBufferOwner::Map {
                    collection_id,
                    generation,
                    dirty: false,
                };
            }
        }
    }

    fn invalidate_map_frontier_buffer(&mut self, collection_id: CollectionId) {
        if let FrontierBufferOwner::Map {
            collection_id: active,
            generation,
            ..
        } = self.memory.frontier_buffer_owner
        {
            if active == collection_id {
                self.memory.frontier_buffer_owner = FrontierBufferOwner::Empty {
                    generation: generation.wrapping_add(1),
                };
            }
        }
    }

    fn ensure_map_frontier_cached<K, V, const MAX_RUNS: usize>(
        &mut self,
        collection_id: CollectionId,
        memory: &mut crate::collections::map::LsmMapMemory<K, V, MAX_RUNS>,
    ) -> Result<(), MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        if let Some(generation) = self.cached_map_frontier_generation(collection_id) {
            if memory
                .cached_frontier
                .as_ref()
                .is_some_and(|cached| cached.buffer_generation == generation)
            {
                #[cfg(feature = "perf-counters")]
                self.memory
                    .perf_metrics
                    .increment(StoragePerfCounter::FrontierCacheHits);
                return Ok(());
            }
        }

        #[cfg(feature = "perf-counters")]
        {
            self.memory
                .perf_metrics
                .increment(StoragePerfCounter::FrontierCacheMisses);
            self.memory
                .perf_metrics
                .increment(StoragePerfCounter::FrontierReloads);
        }

        memory.cached_frontier = None;
        memory.frontier.runs.clear();
        let generation = self.assign_map_frontier_buffer(collection_id);
        #[cfg(feature = "perf-counters")]
        let frontier = MapFrontier::<K, V, MAX_RUNS>::open_from_storage_metered::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(
            &self.memory.state,
            self.backing,
            &mut self.memory.workspace,
            &mut self.memory.collection_scratch,
            collection_id,
            &mut self.memory.open_scratch,
            &mut memory.frontier,
            &mut self.memory.perf_metrics,
        )?;
        #[cfg(not(feature = "perf-counters"))]
        let frontier = MapFrontier::<K, V, MAX_RUNS>::open_from_storage::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(
            &self.memory.state,
            self.backing,
            &mut self.memory.workspace,
            &mut self.memory.collection_scratch,
            collection_id,
            &mut self.memory.open_scratch,
            &mut memory.frontier,
        )?;
        memory.cached_frontier = Some(crate::collections::map::CachedMapFrontier {
            buffer_generation: generation,
            state: frontier.into_state(),
        });
        Ok(())
    }

    pub(crate) fn enter_mode(&mut self, next: StorageMode) -> Result<(), StorageRuntimeError> {
        if self.memory.mode != StorageMode::Idle {
            return Err(StorageRuntimeError::InvalidStorageMode {
                expected: StorageMode::expected_idle(),
                actual: self.memory.mode,
            });
        }
        self.memory.mode = next;
        Ok(())
    }

    pub(crate) fn finish_mode(&mut self) {
        self.memory.mode = StorageMode::Idle;
    }

    pub(crate) fn set_mode_unchecked(&mut self, mode: StorageMode) {
        self.memory.mode = mode;
    }

    /// Returns the advanced runtime state backing this [`Storage`] value.
    pub fn runtime(&self) -> &StorageRuntime<MAX_COLLECTIONS> {
        &self.memory.state
    }

    /// Consumes the facade and returns the underlying runtime state.
    #[cfg(test)]
    pub(crate) fn into_runtime(self) -> &'mem mut StorageRuntime<MAX_COLLECTIONS> {
        &mut self.memory.state
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_io_workspace<T>(
        &mut self,
        operation: impl FnOnce(
            &mut StorageRuntime<MAX_COLLECTIONS>,
            &mut IO,
            &mut StorageWorkspace<REGION_SIZE>,
        ) -> T,
    ) -> T {
        operation(
            &mut self.memory.state,
            self.backing,
            &mut self.memory.workspace,
        )
    }

    #[cfg(test)]
    pub(crate) fn append_raw_wal_record_for_test(
        &mut self,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        self.memory
            .state
            .append_raw_record_for_test::<REGION_SIZE, REGION_COUNT, IO>(
                self.backing,
                &mut self.memory.workspace,
                record,
            )
    }

    #[cfg(test)]
    pub(crate) fn with_io_workspace<T>(
        &mut self,
        operation: impl FnOnce(&mut IO, &mut StorageWorkspace<REGION_SIZE>) -> T,
    ) -> T {
        operation(self.backing, &mut self.memory.workspace)
    }

    /// Returns storage metadata recovered from disk.
    pub fn metadata(&self) -> StorageMetadata {
        self.memory.state.metadata()
    }

    /// Returns the current WAL head region index.
    pub fn wal_head(&self) -> u32 {
        self.memory.state.wal_head()
    }

    /// Returns the current WAL tail region index.
    pub fn wal_tail(&self) -> u32 {
        self.memory.state.wal_tail()
    }

    /// Returns the next append offset in the WAL tail region.
    pub fn wal_append_offset(&self) -> usize {
        self.memory.state.wal_append_offset()
    }

    /// Returns the current free-list head, if any.
    pub fn last_free_list_head(&self) -> Option<u32> {
        self.memory.state.last_free_list_head()
    }

    /// Returns the current free-list tail, if any.
    pub fn free_list_tail(&self) -> Option<u32> {
        self.memory.state.free_list_tail()
    }

    /// Returns a region reserved by `alloc_begin` but not yet linked.
    pub fn ready_region(&self) -> Option<u32> {
        self.memory.state.ready_region()
    }

    /// Returns the largest region sequence observed during replay.
    pub fn max_seen_sequence(&self) -> u64 {
        self.memory.state.max_seen_sequence()
    }

    /// Returns the replay-tracked collections currently known to storage.
    pub fn collections(&self) -> &[StartupCollection] {
        self.memory.state.collections()
    }

    pub(crate) fn allocate_collection_id(&self) -> Result<CollectionId, StorageRuntimeError> {
        let mut next = 1u64;
        for collection in self.collections() {
            let candidate = collection
                .collection_id()
                .0
                .checked_add(1)
                .ok_or(StorageRuntimeError::TooManyTrackedCollections)?;
            if candidate > next {
                next = candidate;
            }
        }
        Ok(CollectionId(next))
    }

    pub(crate) fn allocate_map_collection_id(&self) -> Result<CollectionId, StorageRuntimeError> {
        self.allocate_collection_id()
    }

    /// Returns whether replay left an open WAL recovery boundary.
    pub fn pending_wal_recovery_boundary(&self) -> bool {
        self.memory.state.pending_wal_recovery_boundary()
    }

    /// Returns the number of non-dropped user collections tracked in memory.
    pub fn tracked_user_collection_count(&self) -> usize {
        self.memory.state.tracked_user_collection_count()
    }

    pub(crate) fn validate_live_collections(&self) -> Result<(), StorageOpenError> {
        for collection in self.collections() {
            if collection.basis() == StartupCollectionBasis::Dropped {
                continue;
            }

            let Some(collection_type) = collection.collection_type() else {
                return Err(StorageOpenError::UnsupportedLiveCollectionType(0xffff));
            };

            match collection_type {
                CollectionType::MAP_CODE | CollectionType::OBJECT_LOG_CODE => {}
                other => return Err(StorageOpenError::UnsupportedLiveCollectionType(other)),
            }
        }

        Ok(())
    }

    pub(crate) fn from_initialized_memory(
        backing: &'db mut IO,
        memory: &'mem mut StorageMemory<REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<Self, StorageRuntimeError> {
        memory.dirty_frontiers.clear();
        memory.payload_scratch.fill(0);
        memory.checkpoint_scratch.fill(0);
        memory.collection_scratch.fill(0);
        memory.open_scratch.fill(0);
        memory.frontier_buffer_owner = FrontierBufferOwner::Empty { generation: 0 };
        memory.open_plan.clear();
        memory.reclaim_plan.clear();
        memory.reclaim_source_regions.clear();
        memory.active_collections.clear();
        memory.wal_chain_scratch.clear();
        #[cfg(feature = "perf-counters")]
        {
            memory.perf_metrics = StoragePerfMetrics::default();
        }
        memory.mode = StorageMode::Idle;

        Ok(Self { backing, memory })
    }

    fn clear_dirty_frontier(&mut self, collection_id: CollectionId) {
        if let Some(index) = self
            .memory
            .dirty_frontiers
            .iter()
            .position(|candidate| *candidate == collection_id)
        {
            self.memory.dirty_frontiers.remove(index);
        }
    }

    fn run_storage_operation<T>(
        &mut self,
        mode: StorageMode,
        operation: impl FnOnce(&mut Self) -> Result<T, StorageRuntimeError>,
    ) -> Result<T, StorageRuntimeError> {
        self.enter_mode(mode)?;
        let result = operation(self);
        self.finish_mode();
        result
    }

    fn run_map_operation<T>(
        &mut self,
        mode: StorageMode,
        operation: impl FnOnce(&mut Self) -> Result<T, MapStorageError>,
    ) -> Result<T, MapStorageError> {
        self.enter_mode(mode).map_err(MapStorageError::from)?;
        let result = operation(self);
        self.finish_mode();
        result
    }

    /// Appends a `new_collection` WAL record for a supported user collection type.
    pub fn append_new_collection(
        &mut self,
        collection_id: CollectionId,
        collection_type: u16,
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(
            StorageMode::CreatingCollection(CollectionCreateMode::Running),
            |this| {
                this.memory
                    .state
                    .append_new_collection::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        collection_id,
                        collection_type,
                    )
            },
        )
    }

    /// Appends a raw `update` WAL payload for an existing collection.
    pub fn append_update(
        &mut self,
        collection_id: CollectionId,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(
            StorageMode::UpdatingCollection(CollectionUpdateMode::Running),
            |this| {
                let result = this
                    .memory
                    .state
                    .append_update::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        collection_id,
                        payload,
                    );
                if result.is_ok() {
                    this.invalidate_map_frontier_buffer(collection_id);
                }
                result
            },
        )
    }

    /// Appends a raw `snapshot` WAL payload for an existing collection.
    pub fn append_snapshot(
        &mut self,
        collection_id: CollectionId,
        collection_type: u16,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(
            StorageMode::SnapshottingCollection(CollectionSnapshotMode::Running),
            |this| {
                let result = this
                    .memory
                    .state
                    .append_snapshot::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        collection_id,
                        collection_type,
                        payload,
                    );
                if result.is_ok() {
                    this.invalidate_map_frontier_buffer(collection_id);
                }
                result
            },
        )
    }

    /// Appends a `head` WAL record pointing at a committed region.
    pub fn append_head(
        &mut self,
        collection_id: CollectionId,
        collection_type: u16,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(StorageMode::AppendingWal(WalAppendMode::Running), |this| {
            let result = this
                .memory
                .state
                .append_head::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.memory.workspace,
                    collection_id,
                    collection_type,
                    region_index,
                );
            if result.is_ok() {
                this.invalidate_map_frontier_buffer(collection_id);
            }
            result
        })
    }

    /// Appends a `drop_collection` WAL record.
    pub fn append_drop_collection(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(
            StorageMode::DroppingCollection(CollectionDropMode::Running),
            |this| {
                let result = this
                    .memory
                    .state
                    .append_drop_collection::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        collection_id,
                    );
                if result.is_ok() {
                    this.invalidate_map_frontier_buffer(collection_id);
                }
                result
            },
        )
    }

    /// Appends an `alloc_begin` WAL record for a free-list region.
    pub fn append_alloc_begin(
        &mut self,
        region_index: u32,
        free_list_head_after: Option<u32>,
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(
            StorageMode::AllocatingRegion(AllocationMode::Running),
            |this| {
                this.memory
                    .state
                    .append_alloc_begin::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        CollectionId(0),
                        region_index,
                        free_list_head_after,
                    )
            },
        )
    }

    /// Reserves a free region for a later committed-region write or WAL rotation.
    pub fn reserve_next_region(&mut self) -> Result<u32, StorageRuntimeError> {
        self.run_storage_operation(
            StorageMode::AllocatingRegion(AllocationMode::Running),
            |this| {
                this.memory
                    .state
                    .reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        &mut this.memory.reclaim_source_regions,
                        &mut this.memory.active_collections,
                        &mut this.memory.reclaim_plan,
                        &mut this.memory.open_plan,
                    )
            },
        )
    }

    /// Writes and syncs a committed collection region.
    pub fn write_committed_region(
        &mut self,
        region_index: u32,
        collection_id: CollectionId,
        collection_format: u16,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(
            StorageMode::WritingCommittedRegion(CommittedRegionWriteMode::Running),
            |this| {
                this.memory
                    .state
                    .write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        region_index,
                        collection_id,
                        collection_format,
                        payload,
                    )
            },
        )
    }

    /// Reclaims the current WAL head region and returns the new head.
    pub fn reclaim_wal_head(&mut self) -> Result<u32, StorageRuntimeError> {
        #[cfg(feature = "perf-counters")]
        let reclaim_timer = StoragePerfTimerGuard::start();
        let result = self.run_storage_operation(
            StorageMode::ReclaimingWalHead(WalHeadReclaimMode::Plan),
            |this| {
                #[cfg(feature = "perf-counters")]
                {
                    this.memory
                        .state
                        .reclaim_wal_head_metered::<REGION_SIZE, REGION_COUNT, IO>(
                            this.backing,
                            &mut this.memory.workspace,
                            &mut this.memory.reclaim_source_regions,
                            &mut this.memory.active_collections,
                            &mut this.memory.reclaim_plan,
                            &mut this.memory.open_plan,
                            &mut this.memory.perf_metrics,
                        )
                }
                #[cfg(not(feature = "perf-counters"))]
                {
                    this.memory
                        .state
                        .reclaim_wal_head::<REGION_SIZE, REGION_COUNT, IO>(
                            this.backing,
                            &mut this.memory.workspace,
                            &mut this.memory.reclaim_source_regions,
                            &mut this.memory.active_collections,
                            &mut this.memory.reclaim_plan,
                            &mut this.memory.open_plan,
                        )
                }
            },
        );
        #[cfg(feature = "perf-counters")]
        self.memory
            .perf_metrics
            .add_nanos(StoragePerfTimer::Reclaim, reclaim_timer.elapsed_nanos());
        result
    }

    /// Reclaims the current WAL head region as a caller-driven future.
    pub fn reclaim_wal_head_future<'a>(
        &'a mut self,
    ) -> ReclaimWalHeadFuture<'a, 'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS> {
        ReclaimWalHeadFuture::<'a, 'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::new(
            self,
        )
    }

    /// Appends a `wal_recovery` record when replay requires one.
    pub fn append_wal_recovery(&mut self) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(StorageMode::AppendingWal(WalAppendMode::Running), |this| {
            this.memory
                .state
                .append_wal_recovery::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.memory.workspace,
                )
        })
    }

    /// Begins a WAL tail rotation and returns the reserved next region.
    pub fn append_wal_rotation_start(&mut self) -> Result<u32, StorageRuntimeError> {
        self.run_storage_operation(StorageMode::RotatingWal(WalRotationMode::Running), |this| {
            this.memory
                .state
                .append_wal_rotation_start::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.memory.workspace,
                )
        })
    }

    /// Finishes a WAL tail rotation after `append_wal_rotation_start`.
    pub fn append_wal_rotation_finish(
        &mut self,
        next_region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(StorageMode::RotatingWal(WalRotationMode::Running), |this| {
            this.memory
                .state
                .append_wal_rotation_finish::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.memory.workspace,
                    next_region_index,
                )
        })
    }

    /// Creates a new durable map collection.
    pub fn create_map(&mut self, collection_id: CollectionId) -> Result<(), StorageRuntimeError> {
        self.append_new_collection(collection_id, CollectionType::MAP_CODE)
    }

    /// Creates a new durable map collection as a caller-driven future.
    pub fn create_map_future<'a>(
        &'a mut self,
        collection_id: CollectionId,
    ) -> impl Future<Output = Result<(), StorageRuntimeError>>
           + 'a
           + use<'a, 'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS> {
        run_once(move || self.create_map(collection_id))
    }

    pub(crate) fn flush_map_inner<K, V, const MAX_RUNS: usize>(
        &mut self,
        map: &mut MapFrontier<'_, K, V, MAX_RUNS>,
    ) -> Result<u32, MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        let region_index = map.flush_to_storage::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
            &mut self.memory.state,
            self.backing,
            &mut self.memory.workspace,
            &mut self.memory.reclaim_source_regions,
            &mut self.memory.active_collections,
            &mut self.memory.reclaim_plan,
            &mut self.memory.open_plan,
        )?;
        self.clear_dirty_frontier(map.id());
        self.invalidate_map_frontier_buffer(map.id());
        Ok(region_index)
    }

    /// Persists the supplied map frontier as a WAL snapshot basis.
    pub fn snapshot_map<K, V, const MAX_RUNS: usize>(
        &mut self,
        map: &MapFrontier<'_, K, V, MAX_RUNS>,
    ) -> Result<(), MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.run_map_operation(
            StorageMode::SnapshottingCollection(CollectionSnapshotMode::Running),
            |this| {
                map.write_snapshot_to_storage::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS>(
                    &mut this.memory.state,
                    this.backing,
                    &mut this.memory.workspace,
                    &mut this.memory.payload_scratch,
                )?;
                this.clear_dirty_frontier(map.id());
                this.invalidate_map_frontier_buffer(map.id());
                Ok(())
            },
        )
    }

    /// Persists the supplied map frontier as a caller-driven snapshot future.
    pub fn snapshot_map_future<'a, K, V, const MAX_RUNS: usize>(
        &'a mut self,
        map: &'a MapFrontier<'a, K, V, MAX_RUNS>,
    ) -> impl Future<Output = Result<(), MapStorageError>>
           + 'a
           + use<'a, 'db, 'mem, K, V, MAX_RUNS, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
    where
        K: LsmKey,
        V: LsmValue,
    {
        run_once(move || self.snapshot_map::<K, V, MAX_RUNS>(map))
    }

    /// Encodes and appends a map update payload without mutating a caller-owned frontier.
    pub fn append_map_update<K, V>(
        &mut self,
        collection_id: CollectionId,
        update: &MapUpdate<K, V>,
    ) -> Result<(), MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.run_map_operation(
            StorageMode::UpdatingCollection(CollectionUpdateMode::Running),
            |this| {
                let Some(collection) = this
                    .memory
                    .state
                    .collections()
                    .iter()
                    .find(|collection| collection.collection_id() == collection_id)
                else {
                    return Err(MapStorageError::UnknownCollection(collection_id));
                };
                if collection.basis() == StartupCollectionBasis::Dropped {
                    return Err(MapStorageError::DroppedCollection(collection_id));
                }
                if collection.collection_type() != Some(CollectionType::MAP_CODE) {
                    return Err(MapStorageError::CollectionTypeMismatch {
                        collection_id,
                        expected: CollectionType::MAP_CODE,
                        actual: collection.collection_type(),
                    });
                }

                let used = MapFrontier::<K, V>::encode_update_into(
                    update,
                    &mut this.memory.payload_scratch,
                )?;
                let result = this
                    .memory
                    .state
                    .append_update::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        collection_id,
                        &this.memory.payload_scratch[..used],
                    )
                    .map_err(MapStorageError::from);
                if result.is_ok() {
                    this.invalidate_map_frontier_buffer(collection_id);
                }
                result
            },
        )
    }

    pub(crate) fn apply_map_frontier_update<K, V, const MAX_RUNS: usize>(
        &mut self,
        map: &mut MapFrontier<'_, K, V, MAX_RUNS>,
        update: &MapUpdate<K, V>,
    ) -> Result<(), MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.enter_mode(StorageMode::UpdatingCollection(
            CollectionUpdateMode::Running,
        ))
        .map_err(MapStorageError::from)?;

        let result = apply_map_frontier_update_parts::<
            K,
            V,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_RUNS,
        >(
            &mut self.memory.state,
            self.backing,
            &mut self.memory.workspace,
            &mut self.memory.dirty_frontiers,
            &mut self.memory.payload_scratch,
            &mut self.memory.checkpoint_scratch,
            &mut self.memory.reclaim_source_regions,
            &mut self.memory.active_collections,
            &mut self.memory.reclaim_plan,
            &mut self.memory.open_plan,
            #[cfg(feature = "perf-counters")]
            &mut self.memory.perf_metrics,
            map,
            update,
        );
        if result.is_ok() {
            self.invalidate_map_frontier_buffer(map.id());
        }

        self.finish_mode();
        result
    }

    /// Encodes and appends a map update as a caller-driven future.
    pub fn append_map_update_future<'a, K, V>(
        &'a mut self,
        collection_id: CollectionId,
        update: &'a MapUpdate<K, V>,
    ) -> impl Future<Output = Result<(), MapStorageError>>
           + 'a
           + use<'a, 'db, 'mem, K, V, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>
    where
        K: LsmKey,
        V: LsmValue,
    {
        run_once(move || self.append_map_update::<K, V>(collection_id, update))
    }

    /// Flushes the supplied map frontier into a new committed region.
    pub fn flush_map<K, V, const MAX_RUNS: usize>(
        &mut self,
        map: &mut MapFrontier<'_, K, V, MAX_RUNS>,
    ) -> Result<u32, MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.run_map_operation(
            StorageMode::FlushingCollection(CollectionFlushMode::CommitRegion),
            |this| this.flush_map_inner::<K, V, MAX_RUNS>(map),
        )
    }

    /// Flushes the supplied map frontier as a caller-driven future.
    pub fn flush_map_future<'a, K, V, const MAX_RUNS: usize>(
        &'a mut self,
        map: &'a mut MapFrontier<'a, K, V, MAX_RUNS>,
    ) -> YieldingFlushMapFuture<
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
        K: LsmKey,
        V: LsmValue,
    {
        YieldingFlushMapFuture::<
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
        >::new(self, map)
    }

    /// Compacts a map's committed run set into one replacement manifest.
    pub fn compact_map<K, V, const MAX_RUNS: usize, const RUN_TARGET: usize>(
        &mut self,
        collection_id: CollectionId,
        memory: &mut crate::collections::map::LsmMapMemory<K, V, MAX_RUNS>,
    ) -> Result<Option<u32>, MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.compact_map_with_run_target::<K, V, MAX_RUNS>(collection_id, RUN_TARGET, memory)
    }

    pub(crate) fn compact_map_with_run_target<K, V, const MAX_RUNS: usize>(
        &mut self,
        collection_id: CollectionId,
        run_target: usize,
        memory: &mut crate::collections::map::LsmMapMemory<K, V, MAX_RUNS>,
    ) -> Result<Option<u32>, MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.invalidate_map_frontier_buffer(collection_id);
        #[cfg(feature = "perf-counters")]
        let compaction_timer = StoragePerfTimerGuard::start();
        let result = self.run_map_operation(
            StorageMode::CompactingCollection(CollectionCompactionMode::Running),
            |this| {
                #[cfg(feature = "perf-counters")]
                let opened = MapFrontier::<K, V, MAX_RUNS>::open_from_storage_metered::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                >(
                    &this.memory.state,
                    this.backing,
                    &mut this.memory.workspace,
                    &mut this.memory.collection_scratch,
                    collection_id,
                    &mut this.memory.open_scratch,
                    &mut memory.frontier,
                    &mut this.memory.perf_metrics,
                )?;
                #[cfg(not(feature = "perf-counters"))]
                let opened = MapFrontier::<K, V, MAX_RUNS>::open_from_storage::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                >(
                    &this.memory.state,
                    this.backing,
                    &mut this.memory.workspace,
                    &mut this.memory.collection_scratch,
                    collection_id,
                    &mut this.memory.open_scratch,
                    &mut memory.frontier,
                )?;
                let (_, manifest_region) = compact_map_frontier_parts::<
                    K,
                    V,
                    IO,
                    REGION_SIZE,
                    REGION_COUNT,
                    MAX_COLLECTIONS,
                    MAX_RUNS,
                >(
                    &mut this.memory.state,
                    this.backing,
                    &mut this.memory.workspace,
                    &mut this.memory.dirty_frontiers,
                    &mut this.memory.collection_scratch,
                    &mut this.memory.reclaim_source_regions,
                    &mut this.memory.active_collections,
                    &mut this.memory.reclaim_plan,
                    &mut this.memory.open_plan,
                    collection_id,
                    run_target,
                    opened,
                    &mut memory.compaction_cursors,
                    &mut memory.duplicate_indices,
                    &mut memory.retained_runs,
                )?;
                Ok(manifest_region)
            },
        );
        #[cfg(feature = "perf-counters")]
        {
            self.memory.perf_metrics.add_nanos(
                StoragePerfTimer::Compaction,
                compaction_timer.elapsed_nanos(),
            );
            if matches!(result, Ok(Some(_))) {
                self.memory
                    .perf_metrics
                    .increment(StoragePerfCounter::CompactionsRun);
            }
        }
        result
    }

    /// Drops a live map collection and begins reclaim for its last region basis.
    pub fn drop_map(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<Option<u32>, MapStorageError> {
        self.run_map_operation(
            StorageMode::DroppingCollection(CollectionDropMode::Running),
            |this| {
                this.invalidate_map_frontier_buffer(collection_id);
                let Some(collection) = this
                    .memory
                    .state
                    .collections()
                    .iter()
                    .find(|collection| collection.collection_id() == collection_id)
                else {
                    return Err(MapStorageError::UnknownCollection(collection_id));
                };
                if collection.basis() == StartupCollectionBasis::Dropped {
                    return Err(MapStorageError::DroppedCollection(collection_id));
                }
                if collection.collection_type() != Some(CollectionType::MAP_CODE) {
                    return Err(MapStorageError::CollectionTypeMismatch {
                        collection_id,
                        expected: CollectionType::MAP_CODE,
                        actual: collection.collection_type(),
                    });
                }

                let reclaim = this
                    .memory
                    .state
                    .drop_collection_and_begin_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.memory.workspace,
                        collection_id,
                    )
                    .map_err(MapStorageError::from)?;
                this.clear_dirty_frontier(collection_id);
                Ok(reclaim)
            },
        )
    }

    /// Drops a live map collection as a caller-driven future.
    pub fn drop_map_future<'a>(
        &'a mut self,
        collection_id: CollectionId,
    ) -> impl Future<Output = Result<Option<u32>, MapStorageError>>
           + 'a
           + use<'a, 'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS> {
        run_once(move || self.drop_map(collection_id))
    }

    /// Opens a live map collection into a caller-owned frontier buffer.
    pub fn open_map<'a, K, V, const MAX_RUNS: usize>(
        &mut self,
        collection_id: CollectionId,
        buffer: &'a mut [u8],
        memory: &'a mut crate::collections::map::MapFrontierMemory<K, MAX_RUNS>,
    ) -> Result<MapFrontier<'a, K, V, MAX_RUNS>, MapStorageError>
    where
        K: LsmKey,
        V: LsmValue,
    {
        self.enter_mode(StorageMode::LoadingCollection(CollectionLoadMode::Running))
            .map_err(MapStorageError::from)?;
        let result = MapFrontier::<K, V, MAX_RUNS>::open_from_storage::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
        >(
            &self.memory.state,
            self.backing,
            &mut self.memory.workspace,
            &mut self.memory.collection_scratch,
            collection_id,
            buffer,
            memory,
        );
        self.finish_mode();
        result
    }
}

impl<'map, K, V, const MAX_RUNS: usize> LsmMap<'map, K, V, MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    fn default_compaction_run_target() -> usize {
        match MAX_RUNS.checked_sub(1) {
            Some(0) | None => 1,
            Some(target) => target,
        }
    }

    /// Creates a new durable map collection and returns its small handle.
    pub fn new<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        storage: &mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        memory: &'map mut LsmMapMemory<K, V, MAX_RUNS>,
    ) -> Result<Self, LsmMapError> {
        let collection_id = storage.allocate_map_collection_id()?;
        storage.create_map(collection_id)?;
        Ok(Self::from_collection_id(
            collection_id,
            Self::default_compaction_run_target(),
            memory,
        ))
    }

    /// Opens and validates an existing durable map collection.
    pub fn open<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        collection_id: CollectionId,
        storage: &mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        memory: &'map mut LsmMapMemory<K, V, MAX_RUNS>,
    ) -> Result<Self, LsmMapError> {
        storage
            .enter_mode(StorageMode::LoadingCollection(CollectionLoadMode::Running))
            .map_err(MapStorageError::from)?;
        let result: Result<(), MapStorageError> = (|| {
            let _frontier = MapFrontier::<K, V, MAX_RUNS>::open_from_storage::<
                REGION_SIZE,
                REGION_COUNT,
                IO,
                MAX_COLLECTIONS,
            >(
                &storage.memory.state,
                storage.backing,
                &mut storage.memory.workspace,
                &mut storage.memory.collection_scratch,
                collection_id,
                &mut storage.memory.open_scratch,
                &mut memory.frontier,
            )?;
            Ok(())
        })();
        storage.finish_mode();
        result?;
        Ok(Self::from_collection_id(
            collection_id,
            Self::default_compaction_run_target(),
            memory,
        ))
    }

    /// Overrides the live-run threshold used by `set` and `delete`.
    pub fn with_compaction_run_target(mut self, run_target: usize) -> Result<Self, LsmMapError> {
        if run_target == 0 {
            return Err(MapStorageError::InvalidRunTarget);
        }
        self.compaction_run_target = run_target;
        Ok(self)
    }

    /// Returns the configured live-run compaction threshold.
    pub fn compaction_run_target(&self) -> usize {
        self.compaction_run_target
    }

    /// Reads `key` and calls `f` once with the visible value when present.
    pub fn get<
        'db,
        'mem,
        R,
        F,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        key: &K,
        f: F,
    ) -> Result<Option<R>, LsmMapError>
    where
        F: FnOnce(&K, &V) -> R,
    {
        storage
            .enter_mode(StorageMode::ReadingStorage(ReadMode::Running))
            .map_err(MapStorageError::from)?;
        #[cfg(feature = "perf-counters")]
        storage
            .memory
            .perf_metrics
            .increment(StoragePerfCounter::MapReads);
        let result = (|| {
            storage
                .ensure_map_frontier_cached::<K, V, MAX_RUNS>(self.collection_id, self.memory)?;
            let cached_frontier = self
                .memory
                .cached_frontier
                .take()
                .ok_or(MapStorageError::UnknownCollection(self.collection_id))?;
            let buffer_generation = cached_frontier.buffer_generation;
            let frontier = MapFrontier::<K, V, MAX_RUNS>::from_state(
                cached_frontier.state,
                &mut storage.memory.open_scratch,
                &mut self.memory.frontier,
            );
            #[cfg(feature = "perf-counters")]
            let read_timer = StoragePerfTimerGuard::start();
            #[cfg(feature = "perf-counters")]
            let result = frontier.get_metered::<REGION_SIZE, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                key,
                &mut storage.memory.perf_metrics,
            );
            #[cfg(not(feature = "perf-counters"))]
            let result = frontier.get::<REGION_SIZE, IO>(
                storage.backing,
                &mut storage.memory.workspace,
                key,
            );
            #[cfg(feature = "perf-counters")]
            storage
                .memory
                .perf_metrics
                .add_nanos(StoragePerfTimer::MapReadLookup, read_timer.elapsed_nanos());
            self.memory.cached_frontier = Some(crate::collections::map::CachedMapFrontier {
                buffer_generation,
                state: frontier.into_state(),
            });
            result
        })();
        storage.finish_mode();

        match result? {
            Some(value) => Ok(Some(f(key, &value))),
            None => Ok(None),
        }
    }

    /// Sets `key` to `value` and reports whether compaction is now needed.
    pub fn set<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        key: K,
        value: V,
    ) -> Result<bool, LsmMapError> {
        storage
            .enter_mode(StorageMode::UpdatingCollection(
                CollectionUpdateMode::Running,
            ))
            .map_err(MapStorageError::from)?;
        #[cfg(feature = "perf-counters")]
        {
            storage
                .memory
                .perf_metrics
                .increment(StoragePerfCounter::MapSets);
        }
        #[cfg(feature = "perf-counters")]
        let write_timer = StoragePerfTimerGuard::start();
        let result = (|| {
            storage
                .ensure_map_frontier_cached::<K, V, MAX_RUNS>(self.collection_id, self.memory)?;
            let cached_frontier = self
                .memory
                .cached_frontier
                .take()
                .ok_or(MapStorageError::UnknownCollection(self.collection_id))?;
            let buffer_generation = cached_frontier.buffer_generation;
            let mut frontier = MapFrontier::<K, V, MAX_RUNS>::from_state(
                cached_frontier.state,
                &mut storage.memory.open_scratch,
                &mut self.memory.frontier,
            );
            let update = MapUpdate::Set { key, value };
            let update_result = apply_map_frontier_update_parts::<
                K,
                V,
                IO,
                REGION_SIZE,
                REGION_COUNT,
                MAX_COLLECTIONS,
                MAX_RUNS,
            >(
                &mut storage.memory.state,
                storage.backing,
                &mut storage.memory.workspace,
                &mut storage.memory.dirty_frontiers,
                &mut storage.memory.payload_scratch,
                &mut storage.memory.checkpoint_scratch,
                &mut storage.memory.reclaim_source_regions,
                &mut storage.memory.active_collections,
                &mut storage.memory.reclaim_plan,
                &mut storage.memory.open_plan,
                #[cfg(feature = "perf-counters")]
                &mut storage.memory.perf_metrics,
                &mut frontier,
                &update,
            );
            let update_applied = update_result.is_ok();
            let result = update_result.and_then(|()| {
                #[cfg(feature = "perf-counters")]
                let check_timer = StoragePerfTimerGuard::start();
                let check_result =
                    frontier.selected_compaction_run_count(self.compaction_run_target);
                #[cfg(feature = "perf-counters")]
                let check_nanos = check_timer.elapsed_nanos();
                #[cfg(feature = "perf-counters")]
                {
                    storage
                        .memory
                        .perf_metrics
                        .increment(StoragePerfCounter::CompactionChecks);
                    storage
                        .memory
                        .perf_metrics
                        .add_nanos(StoragePerfTimer::CompactionCheck, check_nanos);
                }
                Ok(check_result?.is_some())
            });
            self.memory.cached_frontier = Some(crate::collections::map::CachedMapFrontier {
                buffer_generation,
                state: frontier.into_state(),
            });
            if update_applied {
                storage.mark_map_frontier_dirty(self.collection_id);
            }
            result
        })();
        #[cfg(feature = "perf-counters")]
        storage
            .memory
            .perf_metrics
            .add_nanos(StoragePerfTimer::FullWritePath, write_timer.elapsed_nanos());
        storage.finish_mode();
        result
    }

    /// Deletes `key` and reports whether compaction is now needed.
    pub fn delete<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
        key: K,
    ) -> Result<bool, LsmMapError> {
        storage
            .enter_mode(StorageMode::UpdatingCollection(
                CollectionUpdateMode::Running,
            ))
            .map_err(MapStorageError::from)?;
        #[cfg(feature = "perf-counters")]
        {
            storage
                .memory
                .perf_metrics
                .increment(StoragePerfCounter::MapDeletes);
        }
        #[cfg(feature = "perf-counters")]
        let write_timer = StoragePerfTimerGuard::start();
        let result = (|| {
            storage
                .ensure_map_frontier_cached::<K, V, MAX_RUNS>(self.collection_id, self.memory)?;
            let cached_frontier = self
                .memory
                .cached_frontier
                .take()
                .ok_or(MapStorageError::UnknownCollection(self.collection_id))?;
            let buffer_generation = cached_frontier.buffer_generation;
            let mut frontier = MapFrontier::<K, V, MAX_RUNS>::from_state(
                cached_frontier.state,
                &mut storage.memory.open_scratch,
                &mut self.memory.frontier,
            );
            let update = MapUpdate::Delete { key };
            let update_result = apply_map_frontier_update_parts::<
                K,
                V,
                IO,
                REGION_SIZE,
                REGION_COUNT,
                MAX_COLLECTIONS,
                MAX_RUNS,
            >(
                &mut storage.memory.state,
                storage.backing,
                &mut storage.memory.workspace,
                &mut storage.memory.dirty_frontiers,
                &mut storage.memory.payload_scratch,
                &mut storage.memory.checkpoint_scratch,
                &mut storage.memory.reclaim_source_regions,
                &mut storage.memory.active_collections,
                &mut storage.memory.reclaim_plan,
                &mut storage.memory.open_plan,
                #[cfg(feature = "perf-counters")]
                &mut storage.memory.perf_metrics,
                &mut frontier,
                &update,
            );
            let update_applied = update_result.is_ok();
            let result = update_result.and_then(|()| {
                #[cfg(feature = "perf-counters")]
                let check_timer = StoragePerfTimerGuard::start();
                let check_result =
                    frontier.selected_compaction_run_count(self.compaction_run_target);
                #[cfg(feature = "perf-counters")]
                let check_nanos = check_timer.elapsed_nanos();
                #[cfg(feature = "perf-counters")]
                {
                    storage
                        .memory
                        .perf_metrics
                        .increment(StoragePerfCounter::CompactionChecks);
                    storage
                        .memory
                        .perf_metrics
                        .add_nanos(StoragePerfTimer::CompactionCheck, check_nanos);
                }
                Ok(check_result?.is_some())
            });
            self.memory.cached_frontier = Some(crate::collections::map::CachedMapFrontier {
                buffer_generation,
                state: frontier.into_state(),
            });
            if update_applied {
                storage.mark_map_frontier_dirty(self.collection_id);
            }
            result
        })();
        #[cfg(feature = "perf-counters")]
        storage
            .memory
            .perf_metrics
            .add_nanos(StoragePerfTimer::FullWritePath, write_timer.elapsed_nanos());
        storage.finish_mode();
        result
    }

    /// Compacts selected committed runs and reports whether a replacement manifest was committed.
    pub fn compact_and_report<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<bool, LsmMapError> {
        storage
            .enter_mode(StorageMode::CompactingCollection(
                CollectionCompactionMode::Running,
            ))
            .map_err(MapStorageError::from)?;
        #[cfg(feature = "perf-counters")]
        let compaction_timer = StoragePerfTimerGuard::start();
        let result = (|| {
            storage
                .ensure_map_frontier_cached::<K, V, MAX_RUNS>(self.collection_id, self.memory)?;
            let cached_frontier = self
                .memory
                .cached_frontier
                .take()
                .ok_or(MapStorageError::UnknownCollection(self.collection_id))?;
            let buffer_generation = cached_frontier.buffer_generation;
            let opened = MapFrontier::<K, V, MAX_RUNS>::from_state(
                cached_frontier.state,
                &mut storage.memory.open_scratch,
                &mut self.memory.frontier,
            );
            match compact_map_frontier_parts::<
                K,
                V,
                IO,
                REGION_SIZE,
                REGION_COUNT,
                MAX_COLLECTIONS,
                MAX_RUNS,
            >(
                &mut storage.memory.state,
                storage.backing,
                &mut storage.memory.workspace,
                &mut storage.memory.dirty_frontiers,
                &mut storage.memory.collection_scratch,
                &mut storage.memory.reclaim_source_regions,
                &mut storage.memory.active_collections,
                &mut storage.memory.reclaim_plan,
                &mut storage.memory.open_plan,
                self.collection_id,
                self.compaction_run_target,
                opened,
                &mut self.memory.compaction_cursors,
                &mut self.memory.duplicate_indices,
                &mut self.memory.retained_runs,
            ) {
                Ok((state, manifest_region)) => {
                    self.memory.cached_frontier =
                        Some(crate::collections::map::CachedMapFrontier {
                            buffer_generation,
                            state,
                        });
                    if manifest_region.is_some() {
                        storage.mark_map_frontier_clean(self.collection_id);
                    }
                    Ok(manifest_region.is_some())
                }
                Err(error) => {
                    storage.invalidate_map_frontier_buffer(self.collection_id);
                    Err(error)
                }
            }
        })();
        #[cfg(feature = "perf-counters")]
        {
            storage.memory.perf_metrics.add_nanos(
                StoragePerfTimer::Compaction,
                compaction_timer.elapsed_nanos(),
            );
            if matches!(result, Ok(true)) {
                storage
                    .memory
                    .perf_metrics
                    .increment(StoragePerfCounter::CompactionsRun);
            }
        }
        storage.finish_mode();
        result
    }

    /// Compacts selected committed runs. Having nothing to compact is success.
    pub fn compact<
        'db,
        'mem,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        storage: &mut Storage<'db, 'mem, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    ) -> Result<(), LsmMapError> {
        let _ =
            self.compact_and_report::<IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>(storage)?;
        Ok(())
    }
}
