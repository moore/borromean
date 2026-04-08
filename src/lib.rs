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

#[cfg(test)]
mod tests;

pub mod disk;
pub use disk::*;

pub mod mock;
pub use mock::*;

pub mod flash_io;
pub use flash_io::*;

pub mod workspace;
pub use workspace::*;

pub mod startup;
pub use startup::*;

pub mod storage;
pub use storage::*;

pub mod wal_record;
pub use wal_record::*;

pub mod op_future;
pub use op_future::*;

mod collections;
pub use collections::*;

pub mod vec_like;
pub use vec_like::*;

use core::fmt::Debug;
use core::future::Future;
use heapless::Vec;
use serde::{Deserialize, Serialize};

type CollectionIdCounter = u64;

/// Newtype for collection identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct CollectionId(pub(crate) CollectionIdCounter);

impl CollectionId {
    pub fn to_le_bytes(&self) -> [u8; size_of::<CollectionIdCounter>()] {
        self.0.to_le_bytes()
    }

    pub fn increment(&self) -> Option<Self> {
        let next = self.0.checked_add(1)?;
        Some(Self(next))
    }
}

/// Represents different types of collections that can be stored
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionType {
    Uninitialized,
    Free,    // Used for free regions
    Wal,     // Write-ahead log
    Channel, // FIFO queue
    Map,     // Key-value store
}

impl CollectionType {
    pub const WAL_CODE: u16 = 0;
    pub const CHANNEL_CODE: u16 = 1;
    pub const MAP_CODE: u16 = 2;

    pub fn stable_code(self) -> Option<u16> {
        match self {
            Self::Wal => Some(Self::WAL_CODE),
            Self::Channel => Some(Self::CHANNEL_CODE),
            Self::Map => Some(Self::MAP_CODE),
            Self::Uninitialized | Self::Free => None,
        }
    }
}

pub trait Collection {
    fn id(&self) -> CollectionId;
    fn collection_type(&self) -> CollectionType;
}

pub struct Storage<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize> {
    state: StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    dirty_frontiers: Vec<CollectionId, MAX_COLLECTIONS>,
}

#[derive(Debug)]
pub enum StorageOpenError {
    Runtime(StorageRuntimeError),
    UnsupportedLiveCollectionType(u16),
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

impl<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>
    Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
{
    pub fn format_future<'a, const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> impl Future<Output = Result<Self, StorageRuntimeError>> + 'a {
        run_once(move || {
            Self::format::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                min_free_regions,
                wal_write_granule,
                wal_record_magic,
            )
        })
    }

    pub fn format<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<Self, StorageRuntimeError> {
        Ok(Self {
            state: storage::format::<
                REGION_SIZE,
                REGION_COUNT,
                IO,
                MAX_COLLECTIONS,
                MAX_PENDING_RECLAIMS,
            >(
                flash,
                workspace,
                min_free_regions,
                wal_write_granule,
                wal_record_magic,
            )?,
            dirty_frontiers: Vec::new(),
        })
    }

    pub fn open_future<'a, const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
    ) -> impl Future<Output = Result<Self, StorageOpenError>> + 'a {
        OpenStorageFuture::<
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
            REGION_SIZE,
            REGION_COUNT,
            IO,
        >::new(flash, workspace)
    }

    pub fn open<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<Self, StorageOpenError> {
        let storage = Self {
            state: storage::open::<
                REGION_SIZE,
                REGION_COUNT,
                IO,
                MAX_COLLECTIONS,
                MAX_PENDING_RECLAIMS,
            >(flash, workspace)?,
            dirty_frontiers: Vec::new(),
        };
        storage.validate_live_collections::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
        Ok(storage)
    }

    pub fn runtime(&self) -> &StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS> {
        &self.state
    }

    pub fn runtime_mut(&mut self) -> &mut StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS> {
        &mut self.state
    }

    pub fn into_runtime(self) -> StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS> {
        self.state
    }

    pub fn metadata(&self) -> StorageMetadata {
        self.state.metadata()
    }

    pub fn wal_head(&self) -> u32 {
        self.state.wal_head()
    }

    pub fn wal_tail(&self) -> u32 {
        self.state.wal_tail()
    }

    pub fn wal_append_offset(&self) -> usize {
        self.state.wal_append_offset()
    }

    pub fn last_free_list_head(&self) -> Option<u32> {
        self.state.last_free_list_head()
    }

    pub fn free_list_tail(&self) -> Option<u32> {
        self.state.free_list_tail()
    }

    pub fn ready_region(&self) -> Option<u32> {
        self.state.ready_region()
    }

    pub fn max_seen_sequence(&self) -> u64 {
        self.state.max_seen_sequence()
    }

    pub fn collections(&self) -> &[StartupCollection] {
        self.state.collections()
    }

    pub fn pending_reclaims(&self) -> &[u32] {
        self.state.pending_reclaims()
    }

    pub fn pending_wal_recovery_boundary(&self) -> bool {
        self.state.pending_wal_recovery_boundary()
    }

    pub fn tracked_user_collection_count(&self) -> usize {
        self.state.tracked_user_collection_count()
    }

    pub(crate) fn validate_live_collections<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        _flash: &mut IO,
        _workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageOpenError> {
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
        state: StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
    ) -> Self {
        Self {
            state,
            dirty_frontiers: Vec::new(),
        }
    }

    fn dirty_frontier_is_active(&self, collection_id: CollectionId) -> bool {
        self.dirty_frontiers.contains(&collection_id)
    }

    fn ensure_dirty_frontier_budget(
        &self,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        if self.dirty_frontier_is_active(collection_id) {
            return Ok(());
        }

        let dirty_after = self
            .dirty_frontiers
            .len()
            .checked_add(1)
            .ok_or(StorageRuntimeError::TooManyTrackedCollections)?;
        let required_min_free_regions = u32::try_from(
            dirty_after
                .checked_add(1)
                .ok_or(StorageRuntimeError::TooManyTrackedCollections)?,
        )
        .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)?;
        if required_min_free_regions > self.state.metadata().min_free_regions {
            return Err(StorageRuntimeError::TooManyDirtyFrontiers {
                dirty_frontiers: dirty_after,
                min_free_regions: self.state.metadata().min_free_regions,
            });
        }

        Ok(())
    }

    fn mark_dirty_frontier(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        if self.dirty_frontier_is_active(collection_id) {
            return Ok(());
        }

        self.dirty_frontiers
            .push(collection_id)
            .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)
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
        self.state
            .append_new_collection::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                collection_type,
            )
    }

    pub fn append_update<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        self.state.append_update::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
            payload,
        )
    }

    pub fn append_snapshot<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        self.state.append_snapshot::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
            collection_type,
            payload,
        )
    }

    pub fn append_head<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.state.append_head::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
            collection_type,
            region_index,
        )
    }

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
        self.state
            .append_drop_collection::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
            )
    }

    pub fn append_alloc_begin<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
        free_list_head_after: Option<u32>,
    ) -> Result<(), StorageRuntimeError> {
        self.state
            .append_alloc_begin::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                region_index,
                free_list_head_after,
            )
    }

    pub fn append_free_list_head<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: Option<u32>,
    ) -> Result<(), StorageRuntimeError> {
        self.state
            .append_free_list_head::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, region_index)
    }

    pub fn append_reclaim_begin<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.state
            .append_reclaim_begin::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, region_index)
    }

    pub fn append_reclaim_end<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.state
            .append_reclaim_end::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, region_index)
    }

    pub fn reclaim_wal_head<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<u32, StorageRuntimeError> {
        self.state
            .reclaim_wal_head::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)
    }

    pub fn reclaim_wal_head_future<
        'a,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &'a mut self,
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
    ) -> impl Future<Output = Result<u32, StorageRuntimeError>> + 'a {
        ReclaimWalHeadFuture::<
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
            REGION_SIZE,
            REGION_COUNT,
            IO,
        >::new(self, flash, workspace)
    }

    pub fn complete_pending_reclaim<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.state
            .complete_pending_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                region_index,
            )
    }

    pub fn append_wal_recovery<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        self.state
            .append_wal_recovery::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)
    }

    pub fn append_wal_rotation_start<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<u32, StorageRuntimeError> {
        self.state
            .append_wal_rotation_start::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)
    }

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
        self.state
            .append_wal_rotation_finish::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                next_region_index,
            )
    }

    pub fn create_map<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError> {
        self.append_new_collection::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
            CollectionType::MAP_CODE,
        )
    }

    pub fn create_map_future<
        'a,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &'a mut self,
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> impl Future<Output = Result<(), StorageRuntimeError>> + 'a {
        run_once(move || {
            self.create_map::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, collection_id)
        })
    }

    pub fn snapshot_map<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        K,
        V,
        const MAX_INDEXES: usize,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        map: &LsmMap<'_, K, V, MAX_INDEXES>,
    ) -> Result<(), MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        map.write_snapshot_to_storage::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(&mut self.state, flash, workspace)?;
        self.clear_dirty_frontier(map.id());
        Ok(())
    }

    pub fn snapshot_map_future<
        'a,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        K,
        V,
        const MAX_INDEXES: usize,
    >(
        &'a mut self,
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
        map: &'a LsmMap<'a, K, V, MAX_INDEXES>,
    ) -> impl Future<Output = Result<(), MapStorageError>> + 'a
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        run_once(move || {
            self.snapshot_map::<REGION_SIZE, REGION_COUNT, IO, K, V, MAX_INDEXES>(
                flash, workspace, map,
            )
        })
    }

    pub fn append_map_update<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        K,
        V,
        const MAX_INDEXES: usize,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        update: &MapUpdate<K, V>,
        payload_buffer: &mut [u8],
    ) -> Result<(), MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        let Some(collection) = self
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

        let used = LsmMap::<K, V, MAX_INDEXES>::encode_update_into(update, payload_buffer)?;
        self.state
            .append_update::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                &payload_buffer[..used],
            )
            .map_err(MapStorageError::from)
    }

    pub fn update_map_frontier<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        K,
        V,
        const MAX_INDEXES: usize,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        map: &mut LsmMap<'_, K, V, MAX_INDEXES>,
        update: &MapUpdate<K, V>,
        payload_buffer: &mut [u8],
    ) -> Result<(), MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        let collection_id = map.id();
        let Some(collection) = self
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
        self.ensure_dirty_frontier_budget(collection_id)
            .map_err(MapStorageError::from)?;

        let used = LsmMap::<K, V, MAX_INDEXES>::encode_update_into(update, payload_buffer)?;
        let mut checkpoint = {
            let (checkpoint_buffer, _) = workspace.encode_buffers();
            map.checkpoint_into(checkpoint_buffer)?
        };

        match map.apply_update_payload(&payload_buffer[..used]) {
            Ok(()) => {}
            Err(MapError::BufferTooSmall) => {
                {
                    let (checkpoint_buffer, _) = workspace.encode_buffers();
                    map.restore_from_checkpoint(checkpoint, checkpoint_buffer)?;
                }

                self.flush_map::<REGION_SIZE, REGION_COUNT, IO, K, V, MAX_INDEXES>(
                    flash, workspace, map,
                )?;

                checkpoint = {
                    let (checkpoint_buffer, _) = workspace.encode_buffers();
                    map.compact_in_place(checkpoint_buffer)?;
                    map.checkpoint_into(checkpoint_buffer)?
                };

                if let Err(error) = map.apply_update_payload(&payload_buffer[..used]) {
                    let (checkpoint_buffer, _) = workspace.encode_buffers();
                    map.restore_from_checkpoint(checkpoint, checkpoint_buffer)?;
                    return Err(error.into());
                }
            }
            Err(error) => {
                let (checkpoint_buffer, _) = workspace.encode_buffers();
                map.restore_from_checkpoint(checkpoint, checkpoint_buffer)?;
                return Err(error.into());
            }
        }

        if let Err(error) = self
            .state
            .append_update_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
                &payload_buffer[..used],
            )
        {
            let (checkpoint_buffer, _) = workspace.encode_buffers();
            map.restore_from_checkpoint(checkpoint, checkpoint_buffer)?;
            return Err(error.into());
        }

        self.mark_dirty_frontier(collection_id)
            .map_err(MapStorageError::from)?;
        Ok(())
    }

    pub fn append_map_update_future<
        'a,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        K,
        V,
        const MAX_INDEXES: usize,
    >(
        &'a mut self,
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        update: &'a MapUpdate<K, V>,
        payload_buffer: &'a mut [u8],
    ) -> impl Future<Output = Result<(), MapStorageError>> + 'a
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        run_once(move || {
            self.append_map_update::<REGION_SIZE, REGION_COUNT, IO, K, V, MAX_INDEXES>(
                flash,
                workspace,
                collection_id,
                update,
                payload_buffer,
            )
        })
    }

    pub fn flush_map<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        K,
        V,
        const MAX_INDEXES: usize,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        map: &LsmMap<'_, K, V, MAX_INDEXES>,
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
        >(&mut self.state, flash, workspace)?;
        self.clear_dirty_frontier(map.id());
        Ok(region_index)
    }

    pub fn flush_map_future<
        'a,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        K,
        V,
        const MAX_INDEXES: usize,
    >(
        &'a mut self,
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
        map: &'a LsmMap<'a, K, V, MAX_INDEXES>,
    ) -> impl Future<Output = Result<u32, MapStorageError>> + 'a
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        FlushMapFuture::<
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
            REGION_SIZE,
            REGION_COUNT,
            IO,
            K,
            V,
            MAX_INDEXES,
        >::new(self, flash, workspace, map)
    }

    pub fn drop_map<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<Option<u32>, MapStorageError> {
        let Some(collection) = self
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

        let reclaim = self
            .state
            .drop_collection_and_begin_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                collection_id,
            )
            .map_err(MapStorageError::from)?;
        self.clear_dirty_frontier(collection_id);
        Ok(reclaim)
    }

    pub fn drop_map_future<'a, const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &'a mut self,
        flash: &'a mut IO,
        workspace: &'a mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> impl Future<Output = Result<Option<u32>, MapStorageError>> + 'a {
        run_once(move || {
            self.drop_map::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, collection_id)
        })
    }

    pub fn open_map<
        'a,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
        K,
        V,
        const MAX_INDEXES: usize,
    >(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        buffer: &'a mut [u8],
    ) -> Result<LsmMap<'a, K, V, MAX_INDEXES>, MapStorageError>
    where
        K: Debug + Ord + PartialOrd + Eq + PartialEq + Serialize + for<'de> Deserialize<'de>,
        V: Debug + Serialize + for<'de> Deserialize<'de>,
    {
        LsmMap::<K, V, MAX_INDEXES>::open_from_storage::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(&self.state, flash, workspace, collection_id, buffer)
    }
}
