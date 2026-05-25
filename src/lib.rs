//! Borromean is a `no_std` flash storage engine built around an
//! append-only ring, caller-provided I/O, and storage-owned scratch memory.
//!
//! The main ownership model is:
//!
//! - [`Storage`] owns logical storage state, bounded operation scratch, and durability invariants.
//! - [`Storage`] binds exclusive mutable access to a caller-provided [`FlashIo`] backing object.
//! - [`StorageWorkspace`] remains an advanced internal/test-support scratch type.
//!
//! Tier 1 supported APIs are [`Storage`], [`FlashIo`],
//! [`CollectionId`], [`CollectionType`], [`LsmMap`], [`MapUpdate`],
//! and [`MockFlash`] for tests and examples.
//! Low-level modules such as [`disk`], [`wal_record`], [`startup`], and
//! [`storage`] are documented as advanced reference surfaces.
//!
//! # Example
//!
//! ```
//! use borromean::{
//!     LsmMap, MockFlash, Storage, StorageFormatConfig,
//! };
//!
//! const REGION_SIZE: usize = 512;
//! const REGION_COUNT: usize = 4;
//!
//! let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
//! let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, 8, 4>::format(
//!     &mut flash,
//!     StorageFormatConfig::new(2, 8, 0xa5),
//! )
//! .unwrap();
//!
//! let mut map = LsmMap::<u16, u16, 8>::new(&mut storage).unwrap();
//! map.set(&mut storage, 7, 70).unwrap();
//! assert_eq!(map.get(&mut storage, &7, |_, value| *value).unwrap(), Some(70));
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

/// Advanced reference types for exact metadata and region-header bytes.
pub mod disk;
pub use disk::*;

/// Test and example backends for exercising the storage engine in memory.
pub mod mock;
pub use mock::*;

/// Tier 1 I/O trait implemented by caller-owned flash adapters.
pub mod flash_io;
pub use flash_io::*;

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
}

impl CollectionType {
    /// Stable on-disk code reserved for WAL collections.
    pub const WAL_CODE: u16 = 0;
    /// Stable on-disk code reserved for the experimental channel type.
    pub const CHANNEL_CODE: u16 = 1;
    /// Stable on-disk code reserved for durable map collections.
    pub const MAP_CODE: u16 = 2;

    /// Returns the stable on-disk code for durable collection kinds.
    pub fn stable_code(self) -> Option<u16> {
        match self {
            Self::Wal => Some(Self::WAL_CODE),
            Self::Channel => Some(Self::CHANNEL_CODE),
            Self::Map => Some(Self::MAP_CODE),
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
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
> {
    pub(crate) backing: &'db mut IO,
    pub(crate) workspace: StorageWorkspace<REGION_SIZE>,
    pub(crate) state: StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    dirty_frontiers: Vec<CollectionId, MAX_COLLECTIONS>,
    pub(crate) payload_scratch: [u8; REGION_SIZE],
    pub(crate) checkpoint_scratch: [u8; REGION_SIZE],
    pub(crate) collection_scratch: [u8; REGION_SIZE],
    pub(crate) open_scratch: [u8; REGION_SIZE],
    pub(crate) frontier_buffer_owner: FrontierBufferOwner,
    #[cfg(feature = "perf-counters")]
    pub(crate) perf_metrics: StoragePerfMetrics,
    pub(crate) mode: StorageMode,
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

fn ensure_dirty_frontier_budget_for<
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    state: &StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
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

#[allow(clippy::too_many_arguments)]
fn update_map_frontier_parts<
    K,
    V,
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
    const MAX_INDEXES: usize,
    const MAX_RUNS: usize,
>(
    state: &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    backing: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    dirty_frontiers: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    payload_scratch: &mut [u8; REGION_SIZE],
    checkpoint_scratch: &mut [u8; REGION_SIZE],
    #[cfg(feature = "perf-counters")] perf_metrics: &mut StoragePerfMetrics,
    map: &mut MapFrontier<'_, K, V, MAX_INDEXES, MAX_RUNS>,
    update: &MapUpdate<K, V>,
) -> Result<(), MapStorageError>
where
    IO: FlashIo,
    K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
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
    let encoded_update =
        MapFrontier::<K, V, MAX_INDEXES>::encode_update_into(update, payload_scratch);
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
    let checkpoint_timer = StoragePerfTimerGuard::start();
    let mut checkpoint = map.checkpoint_into(checkpoint_scratch)?;
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
        perf_metrics.add_nanos(StoragePerfTimer::FrontierApply, apply_timer.elapsed_nanos());
        if apply_result.is_ok() {
            perf_metrics.increment(StoragePerfCounter::FrontierApplies);
        }
    }

    match apply_result {
        Ok(()) => {}
        Err(MapError::BufferTooSmall) => {
            #[cfg(feature = "perf-counters")]
            perf_metrics.increment(StoragePerfCounter::BufferTooSmallErrors);
            map.restore_from_checkpoint(checkpoint, checkpoint_scratch)?;

            #[cfg(feature = "perf-counters")]
            let flush_timer = StoragePerfTimerGuard::start();
            let flush_result = map.flush_to_storage::<
                REGION_SIZE,
                REGION_COUNT,
                IO,
                MAX_COLLECTIONS,
                MAX_PENDING_RECLAIMS,
            >(state, backing, workspace);
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
            let checkpoint_timer = StoragePerfTimerGuard::start();
            checkpoint = map.checkpoint_into(checkpoint_scratch)?;
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
            let retry_apply_result = map.apply_update_payload(&payload_scratch[..used]);
            #[cfg(feature = "perf-counters")]
            {
                perf_metrics
                    .add_nanos(StoragePerfTimer::FrontierApply, apply_timer.elapsed_nanos());
                if retry_apply_result.is_ok() {
                    perf_metrics.increment(StoragePerfCounter::FrontierApplies);
                }
            }
            if let Err(error) = retry_apply_result {
                map.restore_from_checkpoint(checkpoint, checkpoint_scratch)?;
                return Err(error.into());
            }
        }
        Err(error) => {
            map.restore_from_checkpoint(checkpoint, checkpoint_scratch)?;
            return Err(error.into());
        }
    }

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
        map.restore_from_checkpoint(checkpoint, checkpoint_scratch)?;
        return Err(error.into());
    }

    mark_dirty_frontier_in(dirty_frontiers, collection_id).map_err(MapStorageError::from)
}

impl<
        'db,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    > Storage<'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
{
    /// Formats an empty store and returns it as a caller-driven future.
    pub fn format_future(
        backing: &'db mut IO,
        config: StorageFormatConfig,
    ) -> FormatStorageFuture<
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    > {
        FormatStorageFuture::new(backing, config)
    }

    /// Formats an empty store and returns the opened [`Storage`] state.
    pub fn format(
        backing: &'db mut IO,
        config: StorageFormatConfig,
    ) -> Result<Self, StorageRuntimeError> {
        let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
        let state = storage::format::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(
            backing,
            &mut workspace,
            config.min_free_regions,
            config.wal_write_granule,
            config.wal_record_magic,
        )?;
        Ok(Self::from_parts(backing, workspace, state))
    }

    /// Opens an already formatted store as a caller-driven future.
    pub fn open_future(
        backing: &'db mut IO,
    ) -> impl Future<Output = Result<Self, StorageOpenError>> + 'db {
        OpenStorageFuture::<
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >::new(backing)
    }

    /// Opens an already formatted store and validates live collections.
    pub fn open(backing: &'db mut IO) -> Result<Self, StorageOpenError> {
        let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
        let state = storage::reopen_without_reclaim_recovery::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(backing, &mut workspace)?;
        let mut storage = Self::from_parts(backing, workspace, state);
        storage.validate_live_collections()?;
        storage
            .state
            .recover_pending_reclaims::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.workspace,
            )?;
        storage
            .state
            .recover_abandoned_staged_regions::<REGION_SIZE, REGION_COUNT, IO>(
                storage.backing,
                &mut storage.workspace,
            )?;
        Ok(storage)
    }

    fn from_parts(
        backing: &'db mut IO,
        workspace: StorageWorkspace<REGION_SIZE>,
        state: StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    ) -> Self {
        Self {
            backing,
            workspace,
            state,
            dirty_frontiers: Vec::new(),
            payload_scratch: [0; REGION_SIZE],
            checkpoint_scratch: [0; REGION_SIZE],
            collection_scratch: [0; REGION_SIZE],
            open_scratch: [0; REGION_SIZE],
            frontier_buffer_owner: FrontierBufferOwner::Empty { generation: 0 },
            #[cfg(feature = "perf-counters")]
            perf_metrics: StoragePerfMetrics::default(),
            mode: StorageMode::Idle,
        }
    }

    /// Consumes the storage context and returns its bound backing object.
    pub fn into_backing(self) -> &'db mut IO {
        self.backing
    }

    /// Returns the current active storage mode.
    pub fn mode(&self) -> StorageMode {
        self.mode
    }

    /// Returns current performance metrics for this storage context.
    #[cfg(feature = "perf-counters")]
    pub fn perf_metrics(&self) -> StoragePerfMetrics {
        self.perf_metrics
    }

    /// Resets performance metrics for this storage context.
    #[cfg(feature = "perf-counters")]
    pub fn reset_perf_metrics(&mut self) {
        self.perf_metrics = StoragePerfMetrics::default();
    }

    /// Returns current performance metrics and resets this storage context.
    #[cfg(feature = "perf-counters")]
    pub fn take_perf_metrics(&mut self) -> StoragePerfMetrics {
        core::mem::take(&mut self.perf_metrics)
    }

    /// Returns the current owner of the storage-owned hot frontier buffer.
    pub fn frontier_buffer_owner(&self) -> FrontierBufferOwner {
        self.frontier_buffer_owner
    }

    fn cached_map_frontier_generation(&self, collection_id: CollectionId) -> Option<u64> {
        match self.frontier_buffer_owner {
            FrontierBufferOwner::Map {
                collection_id: active,
                generation,
                ..
            } if active == collection_id => Some(generation),
            _ => None,
        }
    }

    fn assign_map_frontier_buffer(&mut self, collection_id: CollectionId) -> u64 {
        let generation = match self.frontier_buffer_owner {
            FrontierBufferOwner::Map {
                collection_id: active,
                generation,
                ..
            } if active == collection_id => generation,
            other => other.generation().wrapping_add(1),
        };
        self.frontier_buffer_owner = FrontierBufferOwner::Map {
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
        } = self.frontier_buffer_owner
        {
            if active == collection_id {
                self.frontier_buffer_owner = FrontierBufferOwner::Map {
                    collection_id,
                    generation,
                    dirty: true,
                };
            }
        }
    }

    fn invalidate_map_frontier_buffer(&mut self, collection_id: CollectionId) {
        if let FrontierBufferOwner::Map {
            collection_id: active,
            generation,
            ..
        } = self.frontier_buffer_owner
        {
            if active == collection_id {
                self.frontier_buffer_owner = FrontierBufferOwner::Empty {
                    generation: generation.wrapping_add(1),
                };
            }
        }
    }

    fn ensure_map_frontier_cached<K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize>(
        &mut self,
        collection_id: CollectionId,
        cache: &core::cell::RefCell<
            Option<crate::collections::map::CachedMapFrontier<K, MAX_RUNS>>,
        >,
    ) -> Result<(), MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        if let Some(generation) = self.cached_map_frontier_generation(collection_id) {
            if cache
                .borrow()
                .as_ref()
                .is_some_and(|cached| cached.buffer_generation == generation)
            {
                #[cfg(feature = "perf-counters")]
                self.perf_metrics
                    .increment(StoragePerfCounter::FrontierCacheHits);
                return Ok(());
            }
        }

        #[cfg(feature = "perf-counters")]
        {
            self.perf_metrics
                .increment(StoragePerfCounter::FrontierCacheMisses);
            self.perf_metrics
                .increment(StoragePerfCounter::FrontierReloads);
        }

        cache.borrow_mut().take();
        let generation = self.assign_map_frontier_buffer(collection_id);
        let frontier = MapFrontier::<K, V, MAX_INDEXES, MAX_RUNS>::open_from_storage::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(
            &self.state,
            self.backing,
            &mut self.workspace,
            &mut self.collection_scratch,
            collection_id,
            &mut self.open_scratch,
        )?;
        *cache.borrow_mut() = Some(crate::collections::map::CachedMapFrontier {
            buffer_generation: generation,
            state: frontier.into_state(),
        });
        Ok(())
    }

    pub(crate) fn enter_mode(&mut self, next: StorageMode) -> Result<(), StorageRuntimeError> {
        if self.mode != StorageMode::Idle {
            return Err(StorageRuntimeError::InvalidStorageMode {
                expected: StorageMode::expected_idle(),
                actual: self.mode,
            });
        }
        self.mode = next;
        Ok(())
    }

    pub(crate) fn finish_mode(&mut self) {
        self.mode = StorageMode::Idle;
    }

    pub(crate) fn set_mode_unchecked(&mut self, mode: StorageMode) {
        self.mode = mode;
    }

    /// Returns the advanced runtime state backing this [`Storage`] value.
    pub fn runtime(&self) -> &StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS> {
        &self.state
    }

    /// Consumes the facade and returns the underlying runtime state.
    #[cfg(test)]
    pub(crate) fn into_runtime(self) -> StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS> {
        self.state
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_io_workspace<T>(
        &mut self,
        operation: impl FnOnce(
            &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
            &mut IO,
            &mut StorageWorkspace<REGION_SIZE>,
        ) -> T,
    ) -> T {
        operation(&mut self.state, self.backing, &mut self.workspace)
    }

    #[cfg(test)]
    pub(crate) fn with_io_workspace<T>(
        &mut self,
        operation: impl FnOnce(&mut IO, &mut StorageWorkspace<REGION_SIZE>) -> T,
    ) -> T {
        operation(self.backing, &mut self.workspace)
    }

    /// Returns storage metadata recovered from disk.
    pub fn metadata(&self) -> StorageMetadata {
        self.state.metadata()
    }

    /// Returns the current WAL head region index.
    pub fn wal_head(&self) -> u32 {
        self.state.wal_head()
    }

    /// Returns the current WAL tail region index.
    pub fn wal_tail(&self) -> u32 {
        self.state.wal_tail()
    }

    /// Returns the next append offset in the WAL tail region.
    pub fn wal_append_offset(&self) -> usize {
        self.state.wal_append_offset()
    }

    /// Returns the current free-list head, if any.
    pub fn last_free_list_head(&self) -> Option<u32> {
        self.state.last_free_list_head()
    }

    /// Returns the current free-list tail, if any.
    pub fn free_list_tail(&self) -> Option<u32> {
        self.state.free_list_tail()
    }

    /// Returns a region reserved by `alloc_begin` but not yet linked.
    pub fn ready_region(&self) -> Option<u32> {
        self.state.ready_region()
    }

    /// Returns the largest region sequence observed during replay.
    pub fn max_seen_sequence(&self) -> u64 {
        self.state.max_seen_sequence()
    }

    /// Returns the replay-tracked collections currently known to storage.
    pub fn collections(&self) -> &[StartupCollection] {
        self.state.collections()
    }

    pub(crate) fn allocate_map_collection_id(&self) -> Result<CollectionId, StorageRuntimeError> {
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

    /// Returns regions awaiting reclaim completion.
    pub fn pending_reclaims(&self) -> &[u32] {
        self.state.pending_reclaims()
    }

    /// Returns allocated regions staged outside the single ready-region slot.
    pub fn staged_regions(&self) -> &[u32] {
        self.state.staged_regions()
    }

    /// Returns whether replay left an open WAL recovery boundary.
    pub fn pending_wal_recovery_boundary(&self) -> bool {
        self.state.pending_wal_recovery_boundary()
    }

    /// Returns the number of non-dropped user collections tracked in memory.
    pub fn tracked_user_collection_count(&self) -> usize {
        self.state.tracked_user_collection_count()
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
                CollectionType::MAP_CODE => {}
                other => return Err(StorageOpenError::UnsupportedLiveCollectionType(other)),
            }
        }

        Ok(())
    }

    pub(crate) fn from_runtime(
        backing: &'db mut IO,
        workspace: StorageWorkspace<REGION_SIZE>,
        state: StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    ) -> Self {
        Self::from_parts(backing, workspace, state)
    }

    fn clear_dirty_frontier(&mut self, collection_id: CollectionId) {
        if let Some(index) = self
            .dirty_frontiers
            .iter()
            .position(|candidate| *candidate == collection_id)
        {
            self.dirty_frontiers.remove(index);
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
                this.state
                    .append_new_collection::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
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
                let result = this.state.append_update::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.workspace,
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
                let result = this.state.append_snapshot::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.workspace,
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
            let result = this.state.append_head::<REGION_SIZE, REGION_COUNT, IO>(
                this.backing,
                &mut this.workspace,
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
                    .state
                    .append_drop_collection::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
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
                this.state
                    .append_alloc_begin::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
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
                this.state
                    .reserve_next_region::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
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
                this.state
                    .write_committed_region::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        region_index,
                        collection_id,
                        collection_format,
                        payload,
                    )
            },
        )
    }

    /// Appends `stage_region` for the current ready region.
    pub fn stage_ready_region(&mut self, region_index: u32) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(StorageMode::AppendingWal(WalAppendMode::Running), |this| {
            this.state
                .stage_ready_region::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.workspace,
                    region_index,
                )
        })
    }

    /// Appends a `free_list_head` WAL record.
    pub fn append_free_list_head(
        &mut self,
        region_index: Option<u32>,
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(StorageMode::AppendingWal(WalAppendMode::Running), |this| {
            this.state
                .append_free_list_head::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.workspace,
                    region_index,
                )
        })
    }

    /// Appends a `reclaim_begin` WAL record for a detached region.
    pub fn append_reclaim_begin(&mut self, region_index: u32) -> Result<(), StorageRuntimeError> {
        #[cfg(feature = "perf-counters")]
        {
            self.perf_metrics
                .increment(StoragePerfCounter::ReclaimStarts);
        }
        #[cfg(feature = "perf-counters")]
        let reclaim_timer = StoragePerfTimerGuard::start();
        let result = self.run_storage_operation(
            StorageMode::ReclaimingRegion(RegionReclaimMode::Running),
            |this| {
                this.state
                    .append_reclaim_begin::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
                        region_index,
                    )
            },
        );
        #[cfg(feature = "perf-counters")]
        self.perf_metrics
            .add_nanos(StoragePerfTimer::Reclaim, reclaim_timer.elapsed_nanos());
        result
    }

    /// Appends a `reclaim_end` WAL record for a previously detached region.
    pub fn append_reclaim_end(&mut self, region_index: u32) -> Result<(), StorageRuntimeError> {
        #[cfg(feature = "perf-counters")]
        {
            self.perf_metrics.increment(StoragePerfCounter::ReclaimEnds);
        }
        #[cfg(feature = "perf-counters")]
        let reclaim_timer = StoragePerfTimerGuard::start();
        let result = self.run_storage_operation(
            StorageMode::ReclaimingRegion(RegionReclaimMode::Running),
            |this| {
                this.state
                    .append_reclaim_end::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
                        region_index,
                    )
            },
        );
        #[cfg(feature = "perf-counters")]
        self.perf_metrics
            .add_nanos(StoragePerfTimer::Reclaim, reclaim_timer.elapsed_nanos());
        result
    }

    /// Reclaims the current WAL head region and returns the new head.
    pub fn reclaim_wal_head(&mut self) -> Result<u32, StorageRuntimeError> {
        #[cfg(feature = "perf-counters")]
        let reclaim_timer = StoragePerfTimerGuard::start();
        let result = self.run_storage_operation(
            StorageMode::ReclaimingWalHead(WalHeadReclaimMode::Plan),
            |this| {
                this.state
                    .reclaim_wal_head::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
                    )
            },
        );
        #[cfg(feature = "perf-counters")]
        self.perf_metrics
            .add_nanos(StoragePerfTimer::Reclaim, reclaim_timer.elapsed_nanos());
        result
    }

    /// Reclaims the current WAL head region as a caller-driven future.
    pub fn reclaim_wal_head_future<'a>(
        &'a mut self,
    ) -> ReclaimWalHeadFuture<
        'a,
        'db,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    > {
        ReclaimWalHeadFuture::<
            'a,
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >::new(self)
    }

    /// Completes physical reclaim for a region already marked pending.
    pub fn complete_pending_reclaim(
        &mut self,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        #[cfg(feature = "perf-counters")]
        {
            self.perf_metrics.increment(StoragePerfCounter::ReclaimEnds);
        }
        #[cfg(feature = "perf-counters")]
        let reclaim_timer = StoragePerfTimerGuard::start();
        let result = self.run_storage_operation(
            StorageMode::ReclaimingRegion(RegionReclaimMode::Running),
            |this| {
                this.state
                    .complete_pending_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
                        region_index,
                    )
            },
        );
        #[cfg(feature = "perf-counters")]
        self.perf_metrics
            .add_nanos(StoragePerfTimer::Reclaim, reclaim_timer.elapsed_nanos());
        result
    }

    /// Appends a `wal_recovery` record when replay requires one.
    pub fn append_wal_recovery(&mut self) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(StorageMode::AppendingWal(WalAppendMode::Running), |this| {
            this.state
                .append_wal_recovery::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.workspace,
                )
        })
    }

    /// Begins a WAL tail rotation and returns the reserved next region.
    pub fn append_wal_rotation_start(&mut self) -> Result<u32, StorageRuntimeError> {
        self.run_storage_operation(StorageMode::RotatingWal(WalRotationMode::Running), |this| {
            this.state
                .append_wal_rotation_start::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.workspace,
                )
        })
    }

    /// Finishes a WAL tail rotation after `append_wal_rotation_start`.
    pub fn append_wal_rotation_finish(
        &mut self,
        next_region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.run_storage_operation(StorageMode::RotatingWal(WalRotationMode::Running), |this| {
            this.state
                .append_wal_rotation_finish::<REGION_SIZE, REGION_COUNT, IO>(
                    this.backing,
                    &mut this.workspace,
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
           + use<'a, 'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
    {
        run_once(move || self.create_map(collection_id))
    }

    pub(crate) fn flush_map_inner<K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize>(
        &mut self,
        map: &mut MapFrontier<'_, K, V, MAX_INDEXES, MAX_RUNS>,
    ) -> Result<u32, MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        let region_index = map.flush_to_storage::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(&mut self.state, self.backing, &mut self.workspace)?;
        self.clear_dirty_frontier(map.id());
        self.invalidate_map_frontier_buffer(map.id());
        Ok(region_index)
    }

    /// Persists the supplied map frontier as a WAL snapshot basis.
    pub fn snapshot_map<K, V, const MAX_INDEXES: usize>(
        &mut self,
        map: &MapFrontier<'_, K, V, MAX_INDEXES>,
    ) -> Result<(), MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        self.run_map_operation(
            StorageMode::SnapshottingCollection(CollectionSnapshotMode::Running),
            |this| {
                map.write_snapshot_to_storage::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(
                    &mut this.state,
                    this.backing,
                    &mut this.workspace,
                    &mut this.payload_scratch,
                )?;
                this.clear_dirty_frontier(map.id());
                this.invalidate_map_frontier_buffer(map.id());
                Ok(())
            },
        )
    }

    /// Persists the supplied map frontier as a caller-driven snapshot future.
    pub fn snapshot_map_future<'a, K, V, const MAX_INDEXES: usize>(
        &'a mut self,
        map: &'a MapFrontier<'a, K, V, MAX_INDEXES>,
    ) -> impl Future<Output = Result<(), MapStorageError>>
           + 'a
           + use<
        'a,
        'db,
        K,
        V,
        MAX_INDEXES,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        run_once(move || self.snapshot_map::<K, V, MAX_INDEXES>(map))
    }

    /// Encodes and appends a map update payload without mutating a caller-owned frontier.
    pub fn append_map_update<K, V, const MAX_INDEXES: usize>(
        &mut self,
        collection_id: CollectionId,
        update: &MapUpdate<K, V>,
    ) -> Result<(), MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        self.run_map_operation(
            StorageMode::UpdatingCollection(CollectionUpdateMode::Running),
            |this| {
                let Some(collection) = this
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

                let used = MapFrontier::<K, V, MAX_INDEXES>::encode_update_into(
                    update,
                    &mut this.payload_scratch,
                )?;
                let result = this
                    .state
                    .append_update::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
                        collection_id,
                        &this.payload_scratch[..used],
                    )
                    .map_err(MapStorageError::from);
                if result.is_ok() {
                    this.invalidate_map_frontier_buffer(collection_id);
                }
                result
            },
        )
    }

    /// Applies a map update to both the caller frontier and durable WAL state.
    pub fn update_map_frontier<K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize>(
        &mut self,
        map: &mut MapFrontier<'_, K, V, MAX_INDEXES, MAX_RUNS>,
        update: &MapUpdate<K, V>,
    ) -> Result<(), MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        self.enter_mode(StorageMode::UpdatingCollection(
            CollectionUpdateMode::Running,
        ))
        .map_err(MapStorageError::from)?;

        let result = update_map_frontier_parts::<
            K,
            V,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
            MAX_INDEXES,
            MAX_RUNS,
        >(
            &mut self.state,
            self.backing,
            &mut self.workspace,
            &mut self.dirty_frontiers,
            &mut self.payload_scratch,
            &mut self.checkpoint_scratch,
            #[cfg(feature = "perf-counters")]
            &mut self.perf_metrics,
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
    pub fn append_map_update_future<'a, K, V, const MAX_INDEXES: usize>(
        &'a mut self,
        collection_id: CollectionId,
        update: &'a MapUpdate<K, V>,
    ) -> impl Future<Output = Result<(), MapStorageError>>
           + 'a
           + use<
        'a,
        'db,
        K,
        V,
        MAX_INDEXES,
        IO,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        run_once(move || self.append_map_update::<K, V, MAX_INDEXES>(collection_id, update))
    }

    /// Flushes the supplied map frontier into a new committed region.
    pub fn flush_map<K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize>(
        &mut self,
        map: &mut MapFrontier<'_, K, V, MAX_INDEXES, MAX_RUNS>,
    ) -> Result<u32, MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        self.run_map_operation(
            StorageMode::FlushingCollection(CollectionFlushMode::CommitRegion),
            |this| this.flush_map_inner::<K, V, MAX_INDEXES, MAX_RUNS>(map),
        )
    }

    /// Flushes the supplied map frontier as a caller-driven future.
    pub fn flush_map_future<'a, K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize>(
        &'a mut self,
        map: &'a mut MapFrontier<'a, K, V, MAX_INDEXES, MAX_RUNS>,
    ) -> YieldingFlushMapFuture<
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
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        YieldingFlushMapFuture::<
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
        >::new(self, map)
    }

    /// Compacts a map's committed run set into one replacement manifest.
    pub fn compact_map<
        K,
        V,
        const MAX_INDEXES: usize,
        const MAX_RUNS: usize,
        const REGION_TARGET: usize,
    >(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<Option<u32>, MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        self.compact_map_with_target::<K, V, MAX_INDEXES, MAX_RUNS>(collection_id, REGION_TARGET)
    }

    pub(crate) fn compact_map_with_target<K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize>(
        &mut self,
        collection_id: CollectionId,
        region_target: usize,
    ) -> Result<Option<u32>, MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        self.invalidate_map_frontier_buffer(collection_id);
        #[cfg(feature = "perf-counters")]
        let compaction_timer = StoragePerfTimerGuard::start();
        let result = self.run_map_operation(
            StorageMode::CompactingCollection(CollectionCompactionMode::Running),
            |this| {
                if region_target == 0 {
                    return Err(MapStorageError::InvalidRegionTarget);
                }

                let mut opened = MapFrontier::<K, V, MAX_INDEXES, MAX_RUNS>::open_from_storage::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(
                    &this.state,
                    this.backing,
                    &mut this.workspace,
                    &mut this.collection_scratch,
                    collection_id,
                    &mut this.open_scratch,
                )?;

                let Some(selected_runs) = opened.selected_compaction_run_count(region_target)?
                else {
                    return Ok(None);
                };
                let frontier_generation = opened.next_run_generation().saturating_add(1);

                let mut planned_allocations = opened
                    .selected_compaction_state_count(selected_runs)?
                    .checked_add(1)
                    .ok_or(MapStorageError::Map(
                        crate::collections::map::MapError::SerializationError,
                    ))?;
                if !opened.frontier_is_empty() {
                    planned_allocations = planned_allocations
                        .checked_add(opened.planned_frontier_run_region_count(
                            &mut this.workspace,
                            frontier_generation,
                        )?)
                        .ok_or(MapStorageError::Map(
                            crate::collections::map::MapError::SerializationError,
                        ))?;
                }
                let additional_allocations = if this.state.ready_region().is_some() {
                    planned_allocations.saturating_sub(1)
                } else {
                    planned_allocations
                };
                this.state
                    .ensure_foreground_allocation_headroom_for::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
                        additional_allocations,
                    )?;

                let replacement_run = opened.write_compacted_run_to_storage::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(&mut this.state, this.backing, &mut this.workspace, selected_runs)?;
                let frontier_run = if opened.frontier_is_empty() {
                    None
                } else {
                    opened.write_frontier_run_to_storage::<
                        REGION_SIZE,
                        REGION_COUNT,
                        IO,
                        MAX_COLLECTIONS,
                        MAX_PENDING_RECLAIMS,
                    >(
                        &mut this.state,
                        this.backing,
                        &mut this.workspace,
                        frontier_generation,
                    )?
                };
                let mut replacement = MapFrontier::<K, V, MAX_INDEXES, MAX_RUNS>::new(
                    collection_id,
                    &mut this.collection_scratch,
                )?;
                if let Some(run) = replacement_run {
                    replacement.push_retained_run(run)?;
                }
                opened.move_unselected_runs_into(selected_runs, &mut replacement)?;
                let manifest_region = replacement.commit_manifest_to_storage::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(&mut this.state, this.backing, &mut this.workspace, frontier_run)?;
                opened.reclaim_run_regions::<
                    REGION_SIZE,
                    REGION_COUNT,
                    IO,
                    MAX_COLLECTIONS,
                    MAX_PENDING_RECLAIMS,
                >(&mut this.state, this.backing, &mut this.workspace)?;
                this.clear_dirty_frontier(collection_id);
                Ok(Some(manifest_region))
            },
        );
        #[cfg(feature = "perf-counters")]
        {
            self.perf_metrics.add_nanos(
                StoragePerfTimer::Compaction,
                compaction_timer.elapsed_nanos(),
            );
            if matches!(result, Ok(Some(_))) {
                self.perf_metrics
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
                    .state
                    .drop_collection_and_begin_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                        this.backing,
                        &mut this.workspace,
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
           + use<'a, 'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
    {
        run_once(move || self.drop_map(collection_id))
    }

    /// Opens a live map collection into a caller-owned frontier buffer.
    pub fn open_map<'a, K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize>(
        &mut self,
        collection_id: CollectionId,
        buffer: &'a mut [u8],
    ) -> Result<MapFrontier<'a, K, V, MAX_INDEXES, MAX_RUNS>, MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        self.enter_mode(StorageMode::LoadingCollection(CollectionLoadMode::Running))
            .map_err(MapStorageError::from)?;
        let result = MapFrontier::<K, V, MAX_INDEXES, MAX_RUNS>::open_from_storage::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(
            &self.state,
            self.backing,
            &mut self.workspace,
            &mut self.collection_scratch,
            collection_id,
            buffer,
        );
        self.finish_mode();
        result
    }
}

impl<K, V, const MAX_INDEXES: usize, const MAX_RUNS: usize> LsmMap<K, V, MAX_INDEXES, MAX_RUNS>
where
    K: LsmKey,
    V: LsmValue,
{
    fn default_compaction_region_target() -> usize {
        match MAX_RUNS.checked_sub(1) {
            Some(0) | None => 1,
            Some(target) => target,
        }
    }

    /// Creates a new durable map collection and returns its small handle.
    pub fn new<
        'db,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        storage: &mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
    ) -> Result<Self, LsmMapError> {
        let collection_id = storage.allocate_map_collection_id()?;
        storage.create_map(collection_id)?;
        Ok(Self::from_collection_id(
            collection_id,
            Self::default_compaction_region_target(),
        ))
    }

    /// Opens and validates an existing durable map collection.
    pub fn open<
        'db,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        collection_id: CollectionId,
        storage: &mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
    ) -> Result<Self, LsmMapError> {
        storage
            .enter_mode(StorageMode::LoadingCollection(CollectionLoadMode::Running))
            .map_err(MapStorageError::from)?;
        let result = (|| {
            let _frontier = MapFrontier::<K, V, MAX_INDEXES, MAX_RUNS>::open_from_storage::<
                REGION_SIZE,
                REGION_COUNT,
                IO,
                MAX_COLLECTIONS,
                MAX_PENDING_RECLAIMS,
            >(
                &storage.state,
                storage.backing,
                &mut storage.workspace,
                &mut storage.collection_scratch,
                collection_id,
                &mut storage.open_scratch,
            )?;
            Ok(Self::from_collection_id(
                collection_id,
                Self::default_compaction_region_target(),
            ))
        })();
        storage.finish_mode();
        result
    }

    /// Overrides the region-count threshold used by `set` and `delete`.
    pub fn with_compaction_region_target(
        mut self,
        region_target: usize,
    ) -> Result<Self, LsmMapError> {
        if region_target == 0 {
            return Err(MapStorageError::InvalidRegionTarget);
        }
        self.compaction_region_target = region_target;
        Ok(self)
    }

    /// Returns the configured compaction region threshold.
    pub fn compaction_region_target(&self) -> usize {
        self.compaction_region_target
    }

    /// Reads `key` and calls `f` once with the visible value when present.
    pub fn get<
        'db,
        R,
        F,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &self,
        storage: &mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
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
        storage.perf_metrics.increment(StoragePerfCounter::MapReads);
        let result = (|| {
            storage.ensure_map_frontier_cached::<K, V, MAX_INDEXES, MAX_RUNS>(
                self.collection_id,
                &self.cached_frontier,
            )?;
            let mut cached = self.cached_frontier.borrow_mut();
            let cached_frontier = cached
                .take()
                .ok_or(MapStorageError::UnknownCollection(self.collection_id))?;
            let buffer_generation = cached_frontier.buffer_generation;
            let frontier = MapFrontier::<K, V, MAX_INDEXES, MAX_RUNS>::from_state(
                cached_frontier.state,
                &mut storage.open_scratch,
            );
            #[cfg(feature = "perf-counters")]
            let read_timer = StoragePerfTimerGuard::start();
            let result =
                frontier.get::<REGION_SIZE, IO>(storage.backing, &mut storage.workspace, key);
            #[cfg(feature = "perf-counters")]
            storage
                .perf_metrics
                .add_nanos(StoragePerfTimer::MapReadLookup, read_timer.elapsed_nanos());
            *cached = Some(crate::collections::map::CachedMapFrontier {
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
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &mut self,
        storage: &mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
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
            storage.perf_metrics.increment(StoragePerfCounter::MapSets);
        }
        #[cfg(feature = "perf-counters")]
        let write_timer = StoragePerfTimerGuard::start();
        let result = (|| {
            storage.ensure_map_frontier_cached::<K, V, MAX_INDEXES, MAX_RUNS>(
                self.collection_id,
                &self.cached_frontier,
            )?;
            let mut cached = self.cached_frontier.borrow_mut();
            let cached_frontier = cached
                .take()
                .ok_or(MapStorageError::UnknownCollection(self.collection_id))?;
            let buffer_generation = cached_frontier.buffer_generation;
            let mut frontier = MapFrontier::<K, V, MAX_INDEXES, MAX_RUNS>::from_state(
                cached_frontier.state,
                &mut storage.open_scratch,
            );
            let update = MapUpdate::Set { key, value };
            let update_result = update_map_frontier_parts::<
                K,
                V,
                IO,
                REGION_SIZE,
                REGION_COUNT,
                MAX_COLLECTIONS,
                MAX_PENDING_RECLAIMS,
                MAX_INDEXES,
                MAX_RUNS,
            >(
                &mut storage.state,
                storage.backing,
                &mut storage.workspace,
                &mut storage.dirty_frontiers,
                &mut storage.payload_scratch,
                &mut storage.checkpoint_scratch,
                #[cfg(feature = "perf-counters")]
                &mut storage.perf_metrics,
                &mut frontier,
                &update,
            );
            let update_applied = update_result.is_ok();
            let result = update_result.and_then(|()| {
                #[cfg(feature = "perf-counters")]
                let check_timer = StoragePerfTimerGuard::start();
                let check_result =
                    frontier.selected_compaction_run_count(self.compaction_region_target);
                #[cfg(feature = "perf-counters")]
                let check_nanos = check_timer.elapsed_nanos();
                #[cfg(feature = "perf-counters")]
                {
                    storage
                        .perf_metrics
                        .increment(StoragePerfCounter::CompactionChecks);
                    storage
                        .perf_metrics
                        .add_nanos(StoragePerfTimer::CompactionCheck, check_nanos);
                }
                Ok(check_result?.is_some())
            });
            *cached = Some(crate::collections::map::CachedMapFrontier {
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
            .perf_metrics
            .add_nanos(StoragePerfTimer::FullWritePath, write_timer.elapsed_nanos());
        storage.finish_mode();
        result
    }

    /// Deletes `key` and reports whether compaction is now needed.
    pub fn delete<
        'db,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &mut self,
        storage: &mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
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
                .perf_metrics
                .increment(StoragePerfCounter::MapDeletes);
        }
        #[cfg(feature = "perf-counters")]
        let write_timer = StoragePerfTimerGuard::start();
        let result = (|| {
            storage.ensure_map_frontier_cached::<K, V, MAX_INDEXES, MAX_RUNS>(
                self.collection_id,
                &self.cached_frontier,
            )?;
            let mut cached = self.cached_frontier.borrow_mut();
            let cached_frontier = cached
                .take()
                .ok_or(MapStorageError::UnknownCollection(self.collection_id))?;
            let buffer_generation = cached_frontier.buffer_generation;
            let mut frontier = MapFrontier::<K, V, MAX_INDEXES, MAX_RUNS>::from_state(
                cached_frontier.state,
                &mut storage.open_scratch,
            );
            let update = MapUpdate::Delete { key };
            let update_result = update_map_frontier_parts::<
                K,
                V,
                IO,
                REGION_SIZE,
                REGION_COUNT,
                MAX_COLLECTIONS,
                MAX_PENDING_RECLAIMS,
                MAX_INDEXES,
                MAX_RUNS,
            >(
                &mut storage.state,
                storage.backing,
                &mut storage.workspace,
                &mut storage.dirty_frontiers,
                &mut storage.payload_scratch,
                &mut storage.checkpoint_scratch,
                #[cfg(feature = "perf-counters")]
                &mut storage.perf_metrics,
                &mut frontier,
                &update,
            );
            let update_applied = update_result.is_ok();
            let result = update_result.and_then(|()| {
                #[cfg(feature = "perf-counters")]
                let check_timer = StoragePerfTimerGuard::start();
                let check_result =
                    frontier.selected_compaction_run_count(self.compaction_region_target);
                #[cfg(feature = "perf-counters")]
                let check_nanos = check_timer.elapsed_nanos();
                #[cfg(feature = "perf-counters")]
                {
                    storage
                        .perf_metrics
                        .increment(StoragePerfCounter::CompactionChecks);
                    storage
                        .perf_metrics
                        .add_nanos(StoragePerfTimer::CompactionCheck, check_nanos);
                }
                Ok(check_result?.is_some())
            });
            *cached = Some(crate::collections::map::CachedMapFrontier {
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
            .perf_metrics
            .add_nanos(StoragePerfTimer::FullWritePath, write_timer.elapsed_nanos());
        storage.finish_mode();
        result
    }

    /// Compacts selected committed runs and reports whether a replacement manifest was committed.
    pub fn compact_and_report<
        'db,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &mut self,
        storage: &mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
    ) -> Result<bool, LsmMapError> {
        self.cached_frontier.borrow_mut().take();
        storage.invalidate_map_frontier_buffer(self.collection_id);
        Ok(storage
            .compact_map_with_target::<K, V, MAX_INDEXES, MAX_RUNS>(
                self.collection_id,
                self.compaction_region_target,
            )?
            .is_some())
    }

    /// Compacts selected committed runs. Having nothing to compact is success.
    pub fn compact<
        'db,
        IO: FlashIo,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
        const MAX_PENDING_RECLAIMS: usize,
    >(
        &mut self,
        storage: &mut Storage<
            'db,
            IO,
            REGION_SIZE,
            REGION_COUNT,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >,
    ) -> Result<(), LsmMapError> {
        let _ = self.compact_and_report::<IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
            storage,
        )?;
        Ok(())
    }
}
