use heapless::Vec;

use crate::disk::{FreePointerFooter, Header, WalRegionPrologue, WAL_V1_FORMAT};
use crate::flash_io::FlashIo;
use crate::mock::{MockError, MockFormatError};
use crate::startup::{open_formatted_store, StartupCollection, StartupError, StartupState};
use crate::wal_record::{
    decode_record, encode_record_into, WalRecord, WalRecordError, WalRecordType,
};
use crate::workspace::StorageWorkspace;
use crate::StorageMetadata;
use crate::{CollectionId, CollectionType, StartupCollectionBasis};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageRuntimeError {
    Format(MockFormatError),
    Mock(MockError),
    Startup(StartupError),
    WalRecord(WalRecordError),
    TooManyTrackedCollections,
    TooManyPendingReclaims,
    ReservedCollectionId(CollectionId),
    UnsupportedCollectionType(u16),
    DuplicateCollection(CollectionId),
    UnknownCollection(CollectionId),
    DroppedCollection(CollectionId),
    CollectionTypeMismatch {
        collection_id: CollectionId,
        expected: u16,
        actual: u16,
    },
    InvalidHeadTarget {
        collection_id: CollectionId,
        region_index: u32,
    },
    InvalidAllocBegin {
        region_index: u32,
        free_list_head: Option<u32>,
    },
    DoubleReadyRegion(u32),
    InvalidReclaimEnd(u32),
    DuplicatePendingReclaim(u32),
    WalRotationRequired,
    NoFreeRegionForRotation,
    WalRecoveryNotNeeded,
    InvalidRotationState {
        ready_region: Option<u32>,
        requested_region: Option<u32>,
    },
    InvalidRotationWindow {
        remaining_after: usize,
        link_reserve: usize,
        rotation_reserve: usize,
    },
    WalHeadReclaimRequiresMultipleWalRegions,
    WalHeadReclaimBlockedByRecoveryBoundary,
    WalHeadReclaimBlockedByReadyRegion(u32),
    WalHeadReclaimBlockedByPendingReclaims,
    WalHeadReclaimBlockedByRecord(WalRecordType),
    WalHeadReclaimTooManyActiveCollections,
    WalHeadReclaimUnsupportedCollectionType(u16),
    InsufficientFreeRegions {
        free_regions: u32,
        min_free_regions: u32,
    },
    TooManyDirtyFrontiers {
        dirty_frontiers: usize,
        min_free_regions: u32,
    },
    CommittedRegionTooLarge {
        payload_len: usize,
        capacity: usize,
    },
}

impl From<MockFormatError> for StorageRuntimeError {
    fn from(error: MockFormatError) -> Self {
        Self::Format(error)
    }
}

impl From<MockError> for StorageRuntimeError {
    fn from(error: MockError) -> Self {
        Self::Mock(error)
    }
}

impl From<StartupError> for StorageRuntimeError {
    fn from(error: StartupError) -> Self {
        Self::Startup(error)
    }
}

impl From<WalRecordError> for StorageRuntimeError {
    fn from(error: WalRecordError) -> Self {
        Self::WalRecord(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalHeadReclaimAction {
    Skip,
    CopyEncoded,
    RewriteEmptyBasisAsSnapshot {
        collection_id: CollectionId,
        collection_type: u16,
    },
}

#[derive(Debug)]
pub(crate) struct WalHeadReclaimPlan<const MAX_COLLECTIONS: usize> {
    pub(crate) old_head: u32,
    pub(crate) new_head: u32,
    original_collections: Vec<StartupCollection, MAX_COLLECTIONS>,
}

#[derive(Debug)]
pub struct StorageRuntime<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize> {
    metadata: StorageMetadata,
    wal_head: u32,
    wal_tail: u32,
    wal_append_offset: usize,
    last_free_list_head: Option<u32>,
    free_list_tail: Option<u32>,
    ready_region: Option<u32>,
    max_seen_sequence: u64,
    collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>,
    pending_wal_recovery_boundary: bool,
}

impl<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>
    StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
{
    //= spec/ring.md#collection-head-state-machine
    //# `RING-FORMAT-012` Every non-WAL `collection_type` that may appear durably on disk MUST have a corresponding normative collection specification.
    fn validate_supported_user_collection_type(
        collection_id: CollectionId,
        collection_type: u16,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id == CollectionId(0) {
            return Err(StorageRuntimeError::ReservedCollectionId(collection_id));
        }
        if collection_type != CollectionType::MAP_CODE {
            return Err(StorageRuntimeError::UnsupportedCollectionType(
                collection_type,
            ));
        }
        Ok(())
    }

    fn validate_supported_head_collection_type(
        collection_id: CollectionId,
        collection_type: u16,
    ) -> Result<(), StorageRuntimeError> {
        if collection_id == CollectionId(0) {
            if collection_type == CollectionType::WAL_CODE {
                return Ok(());
            }
            return Err(StorageRuntimeError::CollectionTypeMismatch {
                collection_id,
                expected: CollectionType::WAL_CODE,
                actual: collection_type,
            });
        }

        Self::validate_supported_user_collection_type(collection_id, collection_type)
    }

    pub fn metadata(&self) -> StorageMetadata {
        self.metadata
    }

    pub fn wal_head(&self) -> u32 {
        self.wal_head
    }

    pub fn wal_tail(&self) -> u32 {
        self.wal_tail
    }

    pub fn wal_append_offset(&self) -> usize {
        self.wal_append_offset
    }

    pub fn last_free_list_head(&self) -> Option<u32> {
        self.last_free_list_head
    }

    pub fn free_list_tail(&self) -> Option<u32> {
        self.free_list_tail
    }

    pub fn ready_region(&self) -> Option<u32> {
        self.ready_region
    }

    pub fn max_seen_sequence(&self) -> u64 {
        self.max_seen_sequence
    }

    pub fn collections(&self) -> &[StartupCollection] {
        self.collections.as_slice()
    }

    pub fn pending_reclaims(&self) -> &[u32] {
        self.pending_reclaims.as_slice()
    }

    pub fn pending_wal_recovery_boundary(&self) -> bool {
        self.pending_wal_recovery_boundary
    }

    pub fn tracked_user_collection_count(&self) -> usize {
        self.collections
            .iter()
            .filter(|collection| collection.basis() != StartupCollectionBasis::Dropped)
            .count()
    }

    pub fn reserve_next_region<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<u32, StorageRuntimeError> {
        if let Some(region_index) = self.ready_region {
            return Ok(region_index);
        }

        self.complete_detached_pending_reclaims::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
        self.ensure_foreground_allocation_headroom::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace,
        )?;

        let region_index = self
            .last_free_list_head
            .ok_or(StorageRuntimeError::NoFreeRegionForRotation)?;
        let free_list_head_after = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            region_index,
        )?;
        self.append_alloc_begin::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            region_index,
            free_list_head_after,
        )?;
        self.ready_region
            .ok_or(StorageRuntimeError::InvalidRotationState {
                ready_region: None,
                requested_region: Some(region_index),
            })
    }

    pub fn write_committed_region<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        flash: &mut IO,
        region_index: u32,
        collection_id: CollectionId,
        collection_format: u16,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        let payload_capacity = REGION_SIZE
            .checked_sub(Header::ENCODED_LEN)
            .and_then(|remaining| remaining.checked_sub(FreePointerFooter::ENCODED_LEN))
            .ok_or(StorageRuntimeError::CommittedRegionTooLarge {
                payload_len: payload.len(),
                capacity: 0,
            })?;
        if payload.len() > payload_capacity {
            return Err(StorageRuntimeError::CommittedRegionTooLarge {
                payload_len: payload.len(),
                capacity: payload_capacity,
            });
        }

        let sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;

        flash.erase_region(region_index)?;

        let header = Header {
            sequence,
            collection_id,
            collection_format,
        };
        let mut header_bytes = [0u8; Header::ENCODED_LEN];
        header
            .encode_into(&mut header_bytes)
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        flash.write_region(region_index, 0, &header_bytes)?;
        flash.write_region(region_index, Header::ENCODED_LEN, payload)?;
        flash.sync()?;
        Ok(())
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
        Self::validate_supported_user_collection_type(collection_id, collection_type)?;
        if self.find_collection(collection_id).is_some() {
            return Err(StorageRuntimeError::DuplicateCollection(collection_id));
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::NewCollection {
                collection_id,
                collection_type,
            },
        )
    }

    pub fn append_update<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        //= spec/ring.md#core-requirements
        //# `RING-CORE-005` For user collections, append-time validity MUST require a successful earlier `new_collection(collection_id, collection_type)` before any later record for that collection may be appended.
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Update {
                collection_id,
                payload,
            },
        )
    }

    pub(crate) fn append_update_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        payload: &[u8],
    ) -> Result<(), StorageRuntimeError> {
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Update {
                collection_id,
                payload,
            },
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
        Self::validate_supported_user_collection_type(collection_id, collection_type)?;
        if let Some(collection) = self.find_collection(collection_id) {
            if collection.basis() == StartupCollectionBasis::Dropped {
                return Err(StorageRuntimeError::DroppedCollection(collection_id));
            }
            if let Some(expected) = collection.collection_type() {
                if expected != collection_type {
                    return Err(StorageRuntimeError::CollectionTypeMismatch {
                        collection_id,
                        expected,
                        actual: collection_type,
                    });
                }
            }
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Snapshot {
                collection_id,
                collection_type,
                payload,
            },
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
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::DropCollection { collection_id },
        )
    }

    pub fn drop_collection_and_begin_reclaim<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<Option<u32>, StorageRuntimeError> {
        //= spec/ring.md#core-requirements
        //# `RING-CORE-007` A `drop_collection(collection_id)` record that is durable MUST tombstone that collection, MUST forbid later WAL records for that `collection_id`, and MUST make older durable bytes reclaimable once they are no longer physically reachable from live state.
        let collection = self
            .find_collection(collection_id)
            .ok_or(StorageRuntimeError::UnknownCollection(collection_id))?;
        if collection.basis() == StartupCollectionBasis::Dropped {
            return Err(StorageRuntimeError::DroppedCollection(collection_id));
        }

        let previous_region = match collection.basis() {
            StartupCollectionBasis::Region(region_index) => Some(region_index),
            _ => None,
        };

        if let Some(region_index) = previous_region {
            self.append_reclaim_begin::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                region_index,
            )?;
        }
        self.append_drop_collection::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            collection_id,
        )?;

        Ok(previous_region)
    }

    pub fn append_head<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        collection_type: u16,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        Self::validate_supported_head_collection_type(collection_id, collection_type)?;
        let collection = self.find_collection(collection_id);
        if let Some(collection) = collection {
            if collection.basis() == StartupCollectionBasis::Dropped {
                return Err(StorageRuntimeError::DroppedCollection(collection_id));
            }
            if let Some(expected) = collection.collection_type() {
                if expected != collection_type {
                    return Err(StorageRuntimeError::CollectionTypeMismatch {
                        collection_id,
                        expected,
                        actual: collection_type,
                    });
                }
            }
        }

        let header = read_header_from_flash::<REGION_SIZE, REGION_COUNT, IO>(flash, region_index)
            .map_err(|_| StorageRuntimeError::InvalidHeadTarget {
            collection_id,
            region_index,
        })?;
        if header.collection_id != collection_id {
            return Err(StorageRuntimeError::InvalidHeadTarget {
                collection_id,
                region_index,
            });
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Head {
                collection_id,
                collection_type,
                region_index,
            },
        )
    }

    pub fn append_alloc_begin<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
        free_list_head_after: Option<u32>,
    ) -> Result<(), StorageRuntimeError> {
        if let Some(ready_region) = self.ready_region {
            return Err(StorageRuntimeError::DoubleReadyRegion(ready_region));
        }
        if self.last_free_list_head != Some(region_index) {
            return Err(StorageRuntimeError::InvalidAllocBegin {
                region_index,
                free_list_head: self.last_free_list_head,
            });
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::AllocBegin {
                region_index,
                free_list_head_after,
            },
        )
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
        //= spec/ring.md#core-requirements
        //# `RING-CORE-009` Any reclaim that frees a region MUST be tracked as a WAL transaction bounded by durable `reclaim_begin(region_index)` and `reclaim_end(region_index)` records.
        if self.pending_reclaims.contains(&region_index) {
            return Err(StorageRuntimeError::DuplicatePendingReclaim(region_index));
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::ReclaimBegin { region_index },
        )
    }

    pub fn append_reclaim_end<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        //= spec/ring.md#core-requirements
        //# `RING-CORE-009` Any reclaim that frees a region MUST be tracked as a WAL transaction bounded by durable `reclaim_begin(region_index)` and `reclaim_end(region_index)` records.
        if !self.pending_reclaims.contains(&region_index) {
            return Err(StorageRuntimeError::InvalidReclaimEnd(region_index));
        }

        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::ReclaimEnd { region_index },
        )
    }

    pub fn reclaim_wal_head<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<u32, StorageRuntimeError> {
        let plan = self.prepare_wal_head_reclaim::<REGION_SIZE, IO>(flash, workspace)?;
        self.begin_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            plan.old_head,
        )?;
        self.preserve_free_list_head_for_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace,
        )?;
        self.copy_live_wal_head_reclaim_state::<REGION_SIZE, REGION_COUNT, IO>(
            flash, workspace, &plan,
        )?;
        self.commit_wal_head_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            plan.new_head,
        )?;
        self.complete_pending_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            plan.old_head,
        )?;
        Ok(plan.new_head)
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
        if !self.pending_reclaims.contains(&region_index) {
            return Err(StorageRuntimeError::InvalidReclaimEnd(region_index));
        }

        if let Some(free_list_tail) = self.free_list_tail {
            write_free_pointer_footer::<REGION_SIZE, IO>(
                flash,
                self.metadata,
                free_list_tail,
                Some(region_index),
            )?;
        }

        flash.erase_region(region_index)?;
        write_free_pointer_footer::<REGION_SIZE, IO>(flash, self.metadata, region_index, None)?;
        flash.sync()?;

        if self.last_free_list_head.is_none() {
            self.append_free_list_head::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                Some(region_index),
            )?;
            if !self.pending_reclaims.contains(&region_index) {
                return Ok(());
            }
        }

        self.append_reclaim_end::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, region_index)
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
        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::FreeListHead { region_index },
        )
    }

    pub fn append_wal_recovery<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        if !self.pending_wal_recovery_boundary {
            return Err(StorageRuntimeError::WalRecoveryNotNeeded);
        }
        self.append_record::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::WalRecovery,
        )
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
        if self.ready_region.is_some() {
            return Err(StorageRuntimeError::InvalidRotationState {
                ready_region: self.ready_region,
                requested_region: None,
            });
        }

        let next_region_index = self
            .last_free_list_head
            .ok_or(StorageRuntimeError::NoFreeRegionForRotation)?;
        let free_list_head_after = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            next_region_index,
        )?;

        let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            free_list_head_after,
        )?;
        let remaining_after = REGION_SIZE
            .checked_sub(
                self.wal_append_offset
                    .checked_add(reserves.alloc_begin_len)
                    .ok_or(StorageRuntimeError::WalRotationRequired)?,
            )
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        if remaining_after < reserves.link_reserve || remaining_after >= reserves.rotation_reserve {
            return Err(StorageRuntimeError::InvalidRotationWindow {
                remaining_after,
                link_reserve: reserves.link_reserve,
                rotation_reserve: reserves.rotation_reserve,
            });
        }

        self.write_record_raw::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::AllocBegin {
                region_index: next_region_index,
                free_list_head_after,
            },
        )?;
        self.wal_append_offset = self
            .wal_append_offset
            .checked_add(reserves.alloc_begin_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        self.last_free_list_head = free_list_head_after;
        self.ready_region = Some(next_region_index);
        self.pending_wal_recovery_boundary = false;
        Ok(next_region_index)
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
        //= spec/ring.md#core-requirements
        //# `RING-CORE-008` Borromean MUST model WAL-head movement as ordinary `head(collection_id = 0, collection_type = wal, region_index = ...)` records rather than a WAL-specific head record type.
        if self.ready_region != Some(next_region_index) {
            return Err(StorageRuntimeError::InvalidRotationState {
                ready_region: self.ready_region,
                requested_region: Some(next_region_index),
            });
        }

        let expected_sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        self.write_record_raw::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Link {
                next_region_index,
                expected_sequence,
            },
        )?;
        initialize_wal_region::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            next_region_index,
            expected_sequence,
            self.wal_head,
        )?;
        *self = reopen_without_reclaim_recovery::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(flash, workspace)?;
        self.append_free_list_head::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            self.last_free_list_head,
        )?;
        Ok(())
    }

    fn find_collection(&self, collection_id: CollectionId) -> Option<&StartupCollection> {
        self.collections
            .iter()
            .find(|collection| collection.collection_id() == collection_id)
    }

    fn append_record<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        self.ensure_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(workspace, flash, record)?;
        self.write_record_and_reopen::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record)
    }

    fn append_record_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        loop {
            match self.append_record::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record) {
                Ok(()) => return Ok(()),
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn write_record_and_reopen<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        self.write_record_raw::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace, record)?;
        *self = reopen_without_reclaim_recovery::<
            REGION_SIZE,
            REGION_COUNT,
            IO,
            MAX_COLLECTIONS,
            MAX_PENDING_RECLAIMS,
        >(flash, workspace)?;
        Ok(())
    }

    fn write_record_raw<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        let (physical, logical) = workspace.encode_buffers();
        let encoded_len = encode_record_into(record, self.metadata, physical, logical)?;
        if self
            .wal_append_offset
            .checked_add(encoded_len)
            .is_none_or(|end| end > REGION_SIZE)
        {
            return Err(StorageRuntimeError::WalRotationRequired);
        }

        flash.write_region(
            self.wal_tail,
            self.wal_append_offset,
            &physical[..encoded_len],
        )?;
        flash.sync()?;
        Ok(())
    }

    fn copy_encoded_record_with_rotation<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        source_region: u32,
        source_offset: usize,
        encoded_len: usize,
    ) -> Result<(), StorageRuntimeError> {
        loop {
            match self.ensure_encoded_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
                workspace,
                flash,
                encoded_len,
                false,
            ) {
                Ok(()) => {
                    {
                        let (physical, _) = workspace.encode_buffers();
                        flash.read_region(
                            source_region,
                            source_offset,
                            &mut physical[..encoded_len],
                        )?;
                        flash.write_region(
                            self.wal_tail,
                            self.wal_append_offset,
                            &physical[..encoded_len],
                        )?;
                        flash.sync()?;
                    }
                    *self = reopen_without_reclaim_recovery::<
                        REGION_SIZE,
                        REGION_COUNT,
                        IO,
                        MAX_COLLECTIONS,
                        MAX_PENDING_RECLAIMS,
                    >(flash, workspace)?;
                    return Ok(());
                }
                Err(StorageRuntimeError::WalRotationRequired) => {
                    self.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn append_empty_basis_snapshot_with_rotation<
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
        let payload = match collection_type {
            crate::CollectionType::MAP_CODE => crate::EMPTY_MAP_SNAPSHOT.as_slice(),
            other => {
                return Err(StorageRuntimeError::WalHeadReclaimUnsupportedCollectionType(other))
            }
        };

        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Snapshot {
                collection_id,
                collection_type,
                payload,
            },
        )
    }

    fn ensure_append_reserve<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        flash: &mut IO,
        record: WalRecord<'_>,
    ) -> Result<(), StorageRuntimeError> {
        if matches!(record, WalRecord::Link { .. }) {
            return Ok(());
        }

        let (physical, logical) = workspace.encode_buffers();
        let encoded_len = encode_record_into(record, self.metadata, physical, logical)?;
        self.ensure_encoded_append_reserve::<REGION_SIZE, REGION_COUNT, IO>(
            workspace,
            flash,
            encoded_len,
            matches!(record, WalRecord::AllocBegin { .. }),
        )
    }

    fn ensure_encoded_append_reserve<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        flash: &mut IO,
        encoded_len: usize,
        alloc_begin: bool,
    ) -> Result<(), StorageRuntimeError> {
        let end = self
            .wal_append_offset
            .checked_add(encoded_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        if end > REGION_SIZE {
            return Err(StorageRuntimeError::WalRotationRequired);
        }

        let remaining_after = REGION_SIZE - end;
        let Some(next_region_index) = self.last_free_list_head else {
            return if remaining_after == 0 || alloc_begin {
                Err(StorageRuntimeError::WalRotationRequired)
            } else {
                Ok(())
            };
        };
        let free_list_head_after = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            self.metadata,
            next_region_index,
        )?;
        let reserves = self.rotation_reserves::<REGION_SIZE, REGION_COUNT>(
            workspace,
            next_region_index,
            free_list_head_after,
        )?;

        if alloc_begin {
            if remaining_after < reserves.rotation_reserve {
                return Err(StorageRuntimeError::WalRotationRequired);
            }
            return Ok(());
        }

        if remaining_after < reserves.rotation_reserve {
            return Err(StorageRuntimeError::WalRotationRequired);
        }
        Ok(())
    }

    fn rotate_wal_tail<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        let next_region_index =
            self.append_wal_rotation_start::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
        self.append_wal_rotation_finish::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            next_region_index,
        )
    }

    fn rotation_reserves<const REGION_SIZE: usize, const REGION_COUNT: usize>(
        &self,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        next_region_index: u32,
        free_list_head_after: Option<u32>,
    ) -> Result<RotationReserves, StorageRuntimeError> {
        let expected_sequence = self
            .max_seen_sequence
            .checked_add(1)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        let (physical, logical) = workspace.encode_buffers();
        let alloc_begin_len = encode_record_into(
            WalRecord::AllocBegin {
                region_index: next_region_index,
                free_list_head_after,
            },
            self.metadata,
            physical,
            logical,
        )?;
        let link_reserve = encode_record_into(
            WalRecord::Link {
                next_region_index,
                expected_sequence,
            },
            self.metadata,
            physical,
            logical,
        )?;
        let rotation_reserve = alloc_begin_len
            .checked_add(link_reserve)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
        Ok(RotationReserves {
            alloc_begin_len,
            link_reserve,
            rotation_reserve,
        })
    }

    pub(crate) fn recover_pending_reclaims<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        let pending_reclaims = self.pending_reclaims.clone();
        for region_index in pending_reclaims {
            if !self.pending_reclaims.contains(&region_index) {
                continue;
            }

            if self.region_reachable_from_live_state::<REGION_SIZE, IO>(
                flash,
                workspace,
                region_index,
            )? {
                self.drop_pending_reclaim_in_memory(region_index)?;
                continue;
            }

            if self.region_is_on_free_list::<REGION_SIZE, REGION_COUNT, IO>(flash, region_index)? {
                self.append_reclaim_end::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    region_index,
                )?;
            } else {
                self.complete_pending_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    region_index,
                )?;
            }
        }

        Ok(())
    }

    fn classify_wal_head_record_for_reclaim(
        &self,
        original_collections: &[StartupCollection],
        active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
        record: WalRecord<'_>,
    ) -> Result<WalHeadReclaimAction, StorageRuntimeError> {
        match record {
            WalRecord::NewCollection {
                collection_id,
                collection_type,
            } => {
                let should_rewrite = find_collection_in(original_collections, collection_id)
                    .is_some_and(|collection| collection.basis() == StartupCollectionBasis::Empty);
                if should_rewrite {
                    activate_collection(active_collections, collection_id)?;
                    return match collection_type {
                        crate::CollectionType::MAP_CODE => {
                            Ok(WalHeadReclaimAction::RewriteEmptyBasisAsSnapshot {
                                collection_id,
                                collection_type,
                            })
                        }
                        other => {
                            Err(StorageRuntimeError::WalHeadReclaimUnsupportedCollectionType(other))
                        }
                    };
                }
                Ok(WalHeadReclaimAction::Skip)
            }
            WalRecord::Update { collection_id, .. } => {
                Ok(if active_collections.contains(&collection_id) {
                    WalHeadReclaimAction::CopyEncoded
                } else {
                    WalHeadReclaimAction::Skip
                })
            }
            WalRecord::Snapshot { collection_id, .. } => {
                let should_copy = find_collection_in(original_collections, collection_id)
                    .is_some_and(|collection| {
                        collection.basis() == StartupCollectionBasis::WalSnapshot
                    });
                if should_copy {
                    activate_collection(active_collections, collection_id)?;
                }
                Ok(if should_copy {
                    WalHeadReclaimAction::CopyEncoded
                } else {
                    WalHeadReclaimAction::Skip
                })
            }
            WalRecord::AllocBegin { region_index, .. } => {
                let _ = region_index;
                Ok(WalHeadReclaimAction::Skip)
            }
            WalRecord::Head {
                collection_id: CollectionId(0),
                region_index,
                ..
            } => {
                let _ = region_index;
                Ok(WalHeadReclaimAction::Skip)
            }
            WalRecord::Head {
                collection_id,
                region_index,
                ..
            } => {
                let should_copy = find_collection_in(original_collections, collection_id)
                    .is_some_and(|collection| {
                        collection.basis() == StartupCollectionBasis::Region(region_index)
                    });
                if should_copy {
                    activate_collection(active_collections, collection_id)?;
                }
                Ok(if should_copy {
                    WalHeadReclaimAction::CopyEncoded
                } else {
                    WalHeadReclaimAction::Skip
                })
            }
            WalRecord::DropCollection { collection_id } => Ok(
                if find_collection_in(original_collections, collection_id)
                    .is_some_and(|collection| collection.basis() == StartupCollectionBasis::Dropped)
                {
                    WalHeadReclaimAction::CopyEncoded
                } else {
                    WalHeadReclaimAction::Skip
                },
            ),
            WalRecord::Link { .. }
            | WalRecord::FreeListHead { .. }
            | WalRecord::ReclaimBegin { .. }
            | WalRecord::ReclaimEnd { .. }
            | WalRecord::WalRecovery => Ok(WalHeadReclaimAction::Skip),
        }
    }

    fn complete_detached_pending_reclaims<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        let pending_reclaims = self.pending_reclaims.clone();
        for region_index in pending_reclaims {
            if !self.pending_reclaims.contains(&region_index) {
                continue;
            }

            if self.region_reachable_from_live_state::<REGION_SIZE, IO>(
                flash,
                workspace,
                region_index,
            )? {
                continue;
            }

            if self.region_is_on_free_list::<REGION_SIZE, REGION_COUNT, IO>(flash, region_index)? {
                self.append_reclaim_end::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    region_index,
                )?;
            } else {
                self.complete_pending_reclaim::<REGION_SIZE, REGION_COUNT, IO>(
                    flash,
                    workspace,
                    region_index,
                )?;
            }
        }

        Ok(())
    }

    fn ensure_foreground_allocation_headroom<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        for _ in 0..self.metadata.region_count {
            let free_regions = self.free_region_count::<REGION_SIZE, REGION_COUNT, IO>(flash)?;
            if free_regions > self.metadata.min_free_regions {
                return Ok(());
            }

            match self.reclaim_wal_head::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace) {
                Ok(_) => {}
                Err(
                    StorageRuntimeError::WalHeadReclaimRequiresMultipleWalRegions
                    | StorageRuntimeError::WalHeadReclaimBlockedByRecoveryBoundary
                    | StorageRuntimeError::WalHeadReclaimBlockedByReadyRegion(_)
                    | StorageRuntimeError::WalHeadReclaimBlockedByPendingReclaims
                    | StorageRuntimeError::WalHeadReclaimBlockedByRecord(_),
                ) => {
                    return Err(StorageRuntimeError::InsufficientFreeRegions {
                        free_regions,
                        min_free_regions: self.metadata.min_free_regions,
                    });
                }
                Err(error) => return Err(error),
            }
        }

        let free_regions = self.free_region_count::<REGION_SIZE, REGION_COUNT, IO>(flash)?;
        Err(StorageRuntimeError::InsufficientFreeRegions {
            free_regions,
            min_free_regions: self.metadata.min_free_regions,
        })
    }

    pub(crate) fn prepare_wal_head_reclaim<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<WalHeadReclaimPlan<MAX_COLLECTIONS>, StorageRuntimeError> {
        if self.wal_head == self.wal_tail {
            return Err(StorageRuntimeError::WalHeadReclaimRequiresMultipleWalRegions);
        }
        if self.pending_wal_recovery_boundary {
            return Err(StorageRuntimeError::WalHeadReclaimBlockedByRecoveryBoundary);
        }
        if let Some(region_index) = self.ready_region {
            return Err(StorageRuntimeError::WalHeadReclaimBlockedByReadyRegion(
                region_index,
            ));
        }
        if !self.pending_reclaims.is_empty() {
            return Err(StorageRuntimeError::WalHeadReclaimBlockedByPendingReclaims);
        }

        let old_head = self.wal_head;
        let new_head = find_link_target_in_wal_region::<REGION_SIZE, IO>(
            flash,
            workspace,
            self.metadata,
            old_head,
        )?
        .ok_or(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
            region_index: old_head,
        }))?;

        Ok(WalHeadReclaimPlan {
            old_head,
            new_head,
            original_collections: self.collections.clone(),
        })
    }

    pub(crate) fn begin_wal_head_reclaim<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        old_head: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::ReclaimBegin {
                region_index: old_head,
            },
        )
    }

    pub(crate) fn preserve_free_list_head_for_wal_head_reclaim<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
    ) -> Result<(), StorageRuntimeError> {
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::FreeListHead {
                region_index: self.last_free_list_head,
            },
        )
    }

    pub(crate) fn copy_live_wal_head_reclaim_state<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        plan: &WalHeadReclaimPlan<MAX_COLLECTIONS>,
    ) -> Result<(), StorageRuntimeError> {
        let mut active_collections = Vec::<CollectionId, MAX_COLLECTIONS>::new();
        let metadata = self.metadata;
        let region_size = usize::try_from(metadata.region_size)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let mut offset = metadata
            .wal_record_area_offset()
            .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
        while offset < region_size {
            let action = {
                let remaining = region_size
                    .checked_sub(offset)
                    .ok_or(StorageRuntimeError::WalRotationRequired)?;
                let (region_bytes, logical_scratch) = workspace.scan_buffers();
                flash.read_region(plan.old_head, offset, &mut region_bytes[..remaining])?;
                if region_bytes[0] == metadata.erased_byte {
                    None
                } else {
                    let decoded =
                        decode_record(&region_bytes[..remaining], metadata, logical_scratch)?;
                    let encoded_len = decoded.encoded_len;
                    let reclaim_action = self.classify_wal_head_record_for_reclaim(
                        &plan.original_collections,
                        &mut active_collections,
                        decoded.record,
                    )?;
                    let hit_link = matches!(decoded.record, WalRecord::Link { .. });
                    Some((encoded_len, reclaim_action, hit_link))
                }
            };

            let Some((encoded_len, reclaim_action, hit_link)) = action else {
                break;
            };
            match reclaim_action {
                WalHeadReclaimAction::Skip => {}
                WalHeadReclaimAction::CopyEncoded => {
                    self.copy_encoded_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        flash,
                        workspace,
                        plan.old_head,
                        offset,
                        encoded_len,
                    )?;
                }
                WalHeadReclaimAction::RewriteEmptyBasisAsSnapshot {
                    collection_id,
                    collection_type,
                } => {
                    self.append_empty_basis_snapshot_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
                        flash,
                        workspace,
                        collection_id,
                        collection_type,
                    )?;
                }
            }

            offset = offset
                .checked_add(encoded_len)
                .ok_or(StorageRuntimeError::WalRotationRequired)?;
            if hit_link {
                break;
            }
        }

        Ok(())
    }

    pub(crate) fn commit_wal_head_reclaim<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        new_head: u32,
    ) -> Result<(), StorageRuntimeError> {
        self.append_record_with_rotation::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            WalRecord::Head {
                collection_id: CollectionId(0),
                collection_type: crate::CollectionType::WAL_CODE,
                region_index: new_head,
            },
        )
    }

    fn free_region_count<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
    ) -> Result<u32, StorageRuntimeError> {
        let mut current = self.last_free_list_head;
        let mut count = 0u32;

        while let Some(region_index) = current {
            count = count
                .checked_add(1)
                .ok_or(StorageRuntimeError::WalRotationRequired)?;
            if count > self.metadata.region_count {
                return Err(StorageRuntimeError::Startup(
                    StartupError::InvalidFreeListChain { region_index },
                ));
            }

            current = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                self.metadata,
                region_index,
            )?;
        }

        Ok(count)
    }

    fn region_reachable_from_live_state<const REGION_SIZE: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        target_region_index: u32,
    ) -> Result<bool, StorageRuntimeError> {
        if self.collections.iter().any(|collection| {
            collection.basis() == StartupCollectionBasis::Region(target_region_index)
        }) {
            return Ok(true);
        }

        wal_chain_contains_region::<REGION_SIZE, IO>(
            flash,
            workspace,
            self.metadata,
            self.wal_head,
            self.wal_tail,
            target_region_index,
        )
    }

    fn region_is_on_free_list<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        target_region_index: u32,
    ) -> Result<bool, StorageRuntimeError> {
        let mut current = self.last_free_list_head;
        let mut visited = 0u32;

        while let Some(region_index) = current {
            if region_index == target_region_index {
                return Ok(true);
            }

            visited = visited
                .checked_add(1)
                .ok_or(StorageRuntimeError::WalRotationRequired)?;
            if visited > self.metadata.region_count {
                return Ok(false);
            }

            current = read_free_pointer_successor::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                self.metadata,
                region_index,
            )?;
        }

        Ok(false)
    }

    fn drop_pending_reclaim_in_memory(
        &mut self,
        region_index: u32,
    ) -> Result<(), StorageRuntimeError> {
        let Some(index) = self
            .pending_reclaims
            .iter()
            .position(|pending| *pending == region_index)
        else {
            return Err(StorageRuntimeError::InvalidReclaimEnd(region_index));
        };
        self.pending_reclaims.remove(index);
        Ok(())
    }
}

fn find_collection_in(
    collections: &[StartupCollection],
    collection_id: CollectionId,
) -> Option<StartupCollection> {
    collections
        .iter()
        .copied()
        .find(|collection| collection.collection_id() == collection_id)
}

fn activate_collection<const MAX_COLLECTIONS: usize>(
    active_collections: &mut Vec<CollectionId, MAX_COLLECTIONS>,
    collection_id: CollectionId,
) -> Result<(), StorageRuntimeError> {
    if active_collections.contains(&collection_id) {
        return Ok(());
    }

    active_collections
        .push(collection_id)
        .map_err(|_| StorageRuntimeError::WalHeadReclaimTooManyActiveCollections)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageVisitError<E> {
    Storage(StorageRuntimeError),
    Visitor(E),
}

impl<E> From<StorageRuntimeError> for StorageVisitError<E> {
    fn from(error: StorageRuntimeError) -> Self {
        Self::Storage(error)
    }
}

impl<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>
    StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
{
    pub fn visit_wal_records<const REGION_SIZE: usize, IO: FlashIo, E, F>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        mut visitor: F,
    ) -> Result<(), StorageVisitError<E>>
    where
        F: FnMut(&mut IO, WalRecord<'_>) -> Result<(), E>,
    {
        let metadata = self.metadata;
        let region_size = usize::try_from(metadata.region_size)
            .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
        let mut current_region = self.wal_head;

        loop {
            let is_tail = current_region == self.wal_tail;
            let limit = if is_tail {
                self.wal_append_offset
            } else {
                region_size
            };
            let (region_bytes, logical_scratch) = workspace.scan_buffers();
            flash
                .read_region(current_region, 0, region_bytes)
                .map_err(StorageRuntimeError::from)?;

            let mut offset = metadata
                .wal_record_area_offset()
                .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
            let mut next_region = None;

            while offset < limit {
                if region_bytes[offset] == metadata.erased_byte {
                    break;
                }

                let decoded =
                    decode_record(&region_bytes[offset..limit], metadata, logical_scratch)
                        .map_err(StorageRuntimeError::from)?;
                let record = decoded.record;
                let encoded_len = decoded.encoded_len;
                if let WalRecord::Link {
                    next_region_index, ..
                } = record
                {
                    next_region = Some(next_region_index);
                }

                visitor(flash, record).map_err(StorageVisitError::Visitor)?;
                offset = offset
                    .checked_add(encoded_len)
                    .ok_or(StorageRuntimeError::WalRotationRequired)
                    .map_err(StorageVisitError::Storage)?;

                if next_region.is_some() {
                    break;
                }
            }

            if is_tail {
                return Ok(());
            }

            current_region = next_region.ok_or_else(|| {
                StorageVisitError::Storage(StorageRuntimeError::Startup(
                    StartupError::BrokenWalChain {
                        region_index: current_region,
                    },
                ))
            })?;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RotationReserves {
    alloc_begin_len: usize,
    link_reserve: usize,
    rotation_reserve: usize,
}

pub fn format<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    min_free_regions: u32,
    wal_write_granule: u32,
    wal_record_magic: u8,
) -> Result<StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StorageRuntimeError> {
    flash.format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)?;
    open::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(flash, workspace)
}

//= spec/ring.md#startup-replay-algorithm
//# RING-STARTUP-007 Maintain replay state: per collection optional live `collection_type`, `last_head`, `basis_pos`, and `pending_updates`, plus global `last_free_list_head`, optional reserved `ready_region`, ordered pending region reclaims, and the replay-local `pending_wal_recovery_boundary`.
pub fn open<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
) -> Result<StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StorageRuntimeError> {
    let mut state = reopen_without_reclaim_recovery::<
        REGION_SIZE,
        REGION_COUNT,
        IO,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >(flash, workspace)?;
    state.recover_pending_reclaims::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)?;
    Ok(state)
}

pub(crate) fn reopen_without_reclaim_recovery<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
) -> Result<StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StorageRuntimeError> {
    let startup = open_formatted_store::<
        REGION_SIZE,
        REGION_COUNT,
        IO,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >(flash, workspace)?;
    from_startup_state(startup)
}

pub(crate) fn from_startup_state<
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    startup: StartupState<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
) -> Result<StorageRuntime<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StorageRuntimeError> {
    let mut collections = Vec::new();
    for collection in startup.collections().iter().copied() {
        collections
            .push(collection)
            .map_err(|_| StorageRuntimeError::TooManyTrackedCollections)?;
    }

    let mut pending_reclaims = Vec::new();
    for region_index in startup.pending_reclaims().iter().copied() {
        pending_reclaims
            .push(region_index)
            .map_err(|_| StorageRuntimeError::TooManyPendingReclaims)?;
    }

    Ok(StorageRuntime {
        metadata: startup.metadata(),
        wal_head: startup.wal_head(),
        wal_tail: startup.wal_tail(),
        wal_append_offset: startup.wal_append_offset(),
        last_free_list_head: startup.last_free_list_head(),
        free_list_tail: startup.free_list_tail(),
        ready_region: startup.ready_region(),
        max_seen_sequence: startup.max_seen_sequence(),
        collections,
        pending_reclaims,
        pending_wal_recovery_boundary: startup.pending_wal_recovery_boundary(),
    })
}

fn read_header_from_flash<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
) -> Result<crate::Header, StorageRuntimeError> {
    let mut header_bytes = [0u8; crate::Header::ENCODED_LEN];
    flash
        .read_region(region_index, 0, &mut header_bytes)
        .map_err(StorageRuntimeError::Mock)?;
    crate::Header::decode(&header_bytes).map_err(|error| StorageRuntimeError::Startup(error.into()))
}

fn read_free_pointer_successor<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<Option<u32>, StorageRuntimeError> {
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?
        - FreePointerFooter::ENCODED_LEN;
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    flash.read_region(region_index, footer_offset, &mut footer_bytes)?;
    let footer = FreePointerFooter::decode_with_region_count(
        &footer_bytes,
        metadata.erased_byte,
        metadata.region_count,
    )
    .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    Ok(footer.next_tail)
}

fn initialize_wal_region<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
    sequence: u64,
    wal_head: u32,
) -> Result<(), StorageRuntimeError> {
    //= spec/ring.md#wal-record-types
    //# `RING-REPLAY-ASSUME-001` A WAL region MUST be erased before reuse.
    flash.erase_region(region_index)?;

    let header = Header {
        sequence,
        collection_id: CollectionId(0),
        collection_format: WAL_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header
        .encode_into(&mut header_bytes)
        .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    flash.write_region(region_index, 0, &header_bytes)?;

    let prologue = WalRegionPrologue {
        wal_head_region_index: wal_head,
    };
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue
        .encode_into(&mut prologue_bytes, metadata.region_count)
        .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    flash.write_region(region_index, Header::ENCODED_LEN, &prologue_bytes)?;
    flash.sync()?;
    Ok(())
}

fn write_free_pointer_footer<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
    region_index: u32,
    next_tail: Option<u32>,
) -> Result<(), StorageRuntimeError> {
    let footer = FreePointerFooter { next_tail };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer
        .encode_into(&mut footer_bytes, metadata.erased_byte)
        .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    let footer_offset = usize::try_from(metadata.region_size)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?
        .checked_sub(FreePointerFooter::ENCODED_LEN)
        .ok_or(StorageRuntimeError::WalRotationRequired)?;
    flash.write_region(region_index, footer_offset, &footer_bytes)?;
    Ok(())
}

fn wal_chain_contains_region<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    wal_head: u32,
    wal_tail: u32,
    target_region_index: u32,
) -> Result<bool, StorageRuntimeError> {
    let mut current_region = wal_head;

    loop {
        if current_region == target_region_index {
            return Ok(true);
        }
        if current_region == wal_tail {
            return Ok(false);
        }

        current_region = find_link_target_in_wal_region::<REGION_SIZE, IO>(
            flash,
            workspace,
            metadata,
            current_region,
        )?
        .ok_or(StorageRuntimeError::Startup(StartupError::BrokenWalChain {
            region_index: current_region,
        }))?;
    }
}

fn find_link_target_in_wal_region<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    region_index: u32,
) -> Result<Option<u32>, StorageRuntimeError> {
    let region_size = usize::try_from(metadata.region_size)
        .map_err(|_| StorageRuntimeError::WalRotationRequired)?;
    let (region_bytes, logical_scratch) = workspace.scan_buffers();
    flash.read_region(region_index, 0, region_bytes)?;

    let mut offset = metadata
        .wal_record_area_offset()
        .map_err(|error| StorageRuntimeError::Startup(error.into()))?;
    while offset < region_size {
        if region_bytes[offset] == metadata.erased_byte {
            return Ok(None);
        }

        let decoded = decode_record(
            &region_bytes[offset..region_size],
            metadata,
            logical_scratch,
        )
        .map_err(StorageRuntimeError::from)?;
        if let WalRecord::Link {
            next_region_index, ..
        } = decoded.record
        {
            return Ok(Some(next_region_index));
        }
        offset = offset
            .checked_add(decoded.encoded_len)
            .ok_or(StorageRuntimeError::WalRotationRequired)?;
    }

    Ok(None)
}

#[cfg(test)]
mod tests;
