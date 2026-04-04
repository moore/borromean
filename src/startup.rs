use heapless::Vec;

use crate::disk::{
    DiskError, FreePointerFooter, Header, StorageMetadata, WalRegionPrologue, WAL_V1_FORMAT,
};
use crate::flash_io::FlashIo;
use crate::mock::MockError;
use crate::wal_record::{
    decode_record, encode_record_into, encoded_record_len, WalRecord, WalRecordError,
};
use crate::workspace::StorageWorkspace;
use crate::{CollectionId, CollectionType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupError {
    Disk(DiskError),
    Mock(MockError),
    WalRecord(WalRecordError),
    MissingMetadata,
    NoWalTailCandidate,
    DuplicateWalTailSequence(u64),
    InvalidWalRegion(u32),
    InvalidWalLinkTarget {
        region_index: u32,
        expected_sequence: u64,
    },
    InvalidWalHeadControlType(u16),
    InvalidFreeListChain {
        region_index: u32,
    },
    InvalidRegionReference(u32),
    BrokenWalChain {
        region_index: u32,
    },
    UnexpectedRecordAfterCorruption {
        region_index: u32,
        offset: usize,
    },
    UnexpectedWalRecovery {
        region_index: u32,
        offset: usize,
    },
    InvalidAllocBegin {
        region_index: u32,
        last_free_list_head: Option<u32>,
    },
    DoubleReadyRegion {
        existing: u32,
        next: u32,
    },
    DuplicatePendingReclaim(u32),
    InvalidReclaimEnd(u32),
    DuplicateCollection(CollectionId),
    UnknownCollection(CollectionId),
    DroppedCollection(CollectionId),
    ReservedCollectionId(CollectionId),
    CollectionTypeMismatch {
        collection_id: CollectionId,
        expected: u16,
        actual: u16,
    },
    InvalidCommittedRegionHead {
        collection_id: CollectionId,
        region_index: u32,
    },
    TooManyTrackedCollections,
    TooManyPendingReclaims,
    UnsupportedLiveCollectionType(u16),
    LengthOverflow,
}

impl From<DiskError> for StartupError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

impl From<MockError> for StartupError {
    fn from(error: MockError) -> Self {
        Self::Mock(error)
    }
}

impl From<WalRecordError> for StartupError {
    fn from(error: WalRecordError) -> Self {
        Self::WalRecord(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupCollectionBasis {
    Empty,
    WalSnapshot,
    Region(u32),
    Dropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupCollection {
    collection_id: CollectionId,
    collection_type: Option<u16>,
    basis: StartupCollectionBasis,
    pending_update_count: usize,
}

impl StartupCollection {
    pub fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    pub fn collection_type(&self) -> Option<u16> {
        self.collection_type
    }

    pub fn basis(&self) -> StartupCollectionBasis {
        self.basis
    }

    pub fn pending_update_count(&self) -> usize {
        self.pending_update_count
    }
}

#[derive(Debug)]
pub struct StartupState<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize> {
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
    StartupState<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
{
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
            .filter(|collection| collection.basis != StartupCollectionBasis::Dropped)
            .count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LastValidRecord {
    AllocBegin {
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

#[derive(Debug)]
pub(crate) struct StartupOpenPlan<
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
> {
    metadata: StorageMetadata,
    wal_head_candidate: u32,
    wal_tail: u32,
    tail_scan: RegionScanResult,
    max_seen_sequence: u64,
    wal_chain: Vec<u32, REGION_COUNT>,
    collections: Vec<StartupCollection, MAX_COLLECTIONS>,
    last_free_list_head: Option<u32>,
    ready_region: Option<u32>,
    pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>,
    wal_append_offset: usize,
    pending_wal_recovery_boundary: bool,
}

pub fn open_formatted_store<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
) -> Result<StartupState<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StartupError> {
    let mut plan = begin_open_formatted_store::<
        REGION_SIZE,
        REGION_COUNT,
        IO,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >(flash, workspace)?;
    recover_open_rotation::<REGION_SIZE, IO, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
        flash, workspace, &mut plan,
    )?;
    discover_open_wal_chain::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
        flash, workspace, &mut plan,
    )?;
    replay_open_wal_chain::<REGION_SIZE, REGION_COUNT, IO, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>(
        flash, workspace, &mut plan,
    )?;
    finish_open_formatted_store::<
        REGION_SIZE,
        REGION_COUNT,
        IO,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >(flash, &mut plan)
}

pub(crate) fn begin_open_formatted_store<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
) -> Result<StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StartupError> {
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

    Ok(StartupOpenPlan {
        metadata,
        wal_head_candidate,
        wal_tail: known_tail,
        tail_scan,
        max_seen_sequence,
        wal_chain: Vec::new(),
        collections: Vec::new(),
        last_free_list_head: if metadata.region_count >= 2 {
            Some(1)
        } else {
            None
        },
        ready_region: None,
        pending_reclaims: Vec::new(),
        wal_append_offset: usize::try_from(metadata.region_size)
            .map_err(|_| StartupError::LengthOverflow)?,
        pending_wal_recovery_boundary: false,
    })
}

pub(crate) fn recover_open_rotation<
    const REGION_SIZE: usize,
    IO: FlashIo,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
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

pub(crate) fn discover_open_wal_chain<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
) -> Result<(), StartupError> {
    plan.wal_chain = walk_wal_chain::<REGION_SIZE, REGION_COUNT, _>(
        flash,
        workspace,
        plan.metadata,
        plan.wal_head_candidate,
        plan.wal_tail,
    )?;
    Ok(())
}

pub(crate) fn replay_open_wal_chain<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
) -> Result<(), StartupError> {
    for (index, region_index) in plan.wal_chain.iter().copied().enumerate() {
        let is_tail = index + 1 == plan.wal_chain.len();
        let scan = scan_wal_region::<REGION_SIZE, _, _>(
            flash,
            workspace,
            plan.metadata,
            region_index,
            is_tail,
            |flash, _offset, record| {
                apply_record(
                    flash,
                    plan.metadata,
                    record,
                    &mut plan.collections,
                    &mut plan.last_free_list_head,
                    &mut plan.ready_region,
                    &mut plan.pending_reclaims,
                )
            },
        )?;

        if is_tail {
            plan.wal_append_offset = scan.append_offset;
            plan.pending_wal_recovery_boundary = scan.pending_boundary_open;
        }
    }

    Ok(())
}

pub(crate) fn finish_open_formatted_store<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    IO: FlashIo,
    const MAX_COLLECTIONS: usize,
    const MAX_PENDING_RECLAIMS: usize,
>(
    flash: &mut IO,
    plan: &mut StartupOpenPlan<REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>,
) -> Result<StartupState<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>, StartupError> {
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
        pending_reclaims: plan.pending_reclaims.clone(),
        pending_wal_recovery_boundary: plan.pending_wal_recovery_boundary,
    })
}

fn locate_wal_tail<const REGION_SIZE: usize, IO: FlashIo>(
    flash: &mut IO,
    metadata: StorageMetadata,
) -> Result<(u32, u64), StartupError> {
    let mut max_seen_sequence = 0u64;
    let mut wal_tail = None;
    let mut wal_tail_sequence = 0u64;
    let mut duplicate_tail = false;

    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    for region_index in 0..metadata.region_count {
        flash.read_region(region_index, 0, &mut header_bytes)?;
        let Ok(header) = Header::decode(&header_bytes) else {
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
                metadata,
                next_region_index,
                expected_sequence,
                wal_head,
            )?;
            *max_seen_sequence = (*max_seen_sequence).max(expected_sequence);
            Ok(Some(next_region_index))
        }
        LastValidRecord::AllocBegin {
            region_index,
            free_list_head_after,
            aligned_end_offset,
        } => {
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
    metadata: StorageMetadata,
    region_index: u32,
    sequence: u64,
    wal_head: u32,
) -> Result<(), StartupError> {
    ensure_region_index_in_range(region_index, metadata.region_count)?;

    flash.erase_region(region_index)?;

    let header = Header {
        sequence,
        collection_id: CollectionId(0),
        collection_format: WAL_V1_FORMAT,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes)?;
    flash.write_region(region_index, 0, &header_bytes)?;

    let prologue = WalRegionPrologue {
        wal_head_region_index: wal_head,
    };
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue.encode_into(&mut prologue_bytes, metadata.region_count)?;
    flash.write_region(region_index, Header::ENCODED_LEN, &prologue_bytes)?;
    flash.sync()?;
    Ok(())
}

fn walk_wal_chain<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
    flash: &mut IO,
    workspace: &mut StorageWorkspace<REGION_SIZE>,
    metadata: StorageMetadata,
    wal_head: u32,
    wal_tail: u32,
) -> Result<Vec<u32, REGION_COUNT>, StartupError> {
    let mut chain = Vec::<u32, REGION_COUNT>::new();
    let mut current = wal_head;

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
            return Ok(chain);
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
    //= spec/ring.md#durability-and-crash-semantics
    //# `RING-DURABILITY-003` Replay MUST treat partially written records as torn and ignore them using checksum validation and WAL tail recovery rules.
    //= spec/ring.md#wal-record-types
    //# `RING-WAL-ENC-007` Every WAL record start offset within a WAL region
    //# MUST be aligned to
    //# `wal_write_granule`, the smallest writable unit of the backing flash.
    //= spec/ring.md#wal-record-types
    //# `RING-WAL-ENC-010` The recovered append point for the tail region
    //# MUST be the first aligned
    //# slot whose first byte is `erased_byte` after the last valid replayed
    //# tail record. If no such slot exists, the tail region is currently full
    //# and the next WAL append must rotate via `link` to a new WAL region.
    //= spec/ring.md#wal-record-types
    //# `RING-CHECKSUM-005` An implementation MUST ensure that even intentionally corrupted storage eventually produces a reported error rather than memory unsafety, undefined behavior, control-flow corruption, infinite loops, or unbounded resource consumption amounting to denial of service.
    //= spec/ring.md#wal-record-types
    //# All replay walks, decoders, and collection-format handlers MUST remain bounded by configured storage geometry and record sizes.
    ensure_region_index_in_range(region_index, metadata.region_count)?;

    let region_size =
        usize::try_from(metadata.region_size).map_err(|_| StartupError::LengthOverflow)?;
    let granule =
        usize::try_from(metadata.wal_write_granule).map_err(|_| StartupError::LengthOverflow)?;
    let (region_bytes, logical_scratch) = workspace.scan_buffers();
    flash.read_region(region_index, 0, region_bytes)?;
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

        //= spec/ring.md#wal-record-types
        //# `RING-WAL-VALID-022` Replay MAY recover only from checksum-invalid or torn aligned WAL
        //# slots. Replay tracks a pending WAL-recovery boundary from the first
        //# ignored corrupt/torn aligned slot until a later valid `wal_recovery`
        //# record is replayed.
        if start_byte != metadata.wal_record_magic {
            pending_boundary_open = true;
            offset = offset
                .checked_add(granule)
                .ok_or(StartupError::LengthOverflow)?;
            continue;
        }

        //= spec/ring.md#wal-record-types
        //# `RING-WAL-VALID-022` Replay MAY recover only from checksum-invalid or torn aligned WAL
        //# slots. Replay tracks a pending WAL-recovery boundary from the first
        //# ignored corrupt/torn aligned slot until a later valid `wal_recovery`
        //# record is replayed.
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
                region_index,
                free_list_head_after,
            } => LastValidRecord::AllocBegin {
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

fn apply_record<IO: FlashIo, const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>(
    _flash: &mut IO,
    metadata: StorageMetadata,
    record: WalRecord<'_>,
    collections: &mut Vec<StartupCollection, MAX_COLLECTIONS>,
    last_free_list_head: &mut Option<u32>,
    ready_region: &mut Option<u32>,
    pending_reclaims: &mut Vec<u32, MAX_PENDING_RECLAIMS>,
) -> Result<(), StartupError> {
    //= spec/ring.md#core-requirements
    //# `RING-CORE-006` For a live user collection, the earliest retained type-bearing record seen during replay MUST establish the replay-tracked `collection_type`, and every later valid type-bearing record for that collection MUST carry the same `collection_type`.
    //= spec/ring.md#wal-record-types
    //# `RING-WAL-VALID-026` `reclaim_begin(region_index)` and `reclaim_end(region_index)` MUST appear in WAL order and are matched by `region_index`.
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
            let collection = find_collection_mut(collections, collection_id)
                .ok_or(StartupError::UnknownCollection(collection_id))?;
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
            region_index,
            free_list_head_after,
        } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;
            if let Some(next_head) = free_list_head_after {
                ensure_region_index_in_range(next_head, metadata.region_count)?;
            }

            if let Some(existing) = ready_region {
                return Err(StartupError::DoubleReadyRegion {
                    existing: *existing,
                    next: region_index,
                });
            }

            if *last_free_list_head != Some(region_index) {
                return Err(StartupError::InvalidAllocBegin {
                    region_index,
                    last_free_list_head: *last_free_list_head,
                });
            }

            *last_free_list_head = free_list_head_after;
            *ready_region = Some(region_index);
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
        WalRecord::FreeListHead { region_index } => {
            if let Some(region_index) = region_index {
                ensure_region_index_in_range(region_index, metadata.region_count)?;
            }
            *last_free_list_head = region_index;
        }
        WalRecord::ReclaimBegin { region_index } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;
            if pending_reclaims.contains(&region_index) {
                return Err(StartupError::DuplicatePendingReclaim(region_index));
            }

            pending_reclaims
                .push(region_index)
                .map_err(|_| StartupError::TooManyPendingReclaims)?;
        }
        WalRecord::ReclaimEnd { region_index } => {
            ensure_region_index_in_range(region_index, metadata.region_count)?;
            let Some(index) = pending_reclaims
                .iter()
                .position(|pending| *pending == region_index)
            else {
                return Err(StartupError::InvalidReclaimEnd(region_index));
            };
            pending_reclaims.remove(index);
        }
        WalRecord::WalRecovery => {}
    }

    Ok(())
}

fn read_region_header<IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
) -> Result<Header, StartupError> {
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    flash.read_region(region_index, 0, &mut header_bytes)?;
    Header::decode(&header_bytes).map_err(StartupError::from)
}

fn read_wal_prologue<IO: FlashIo>(
    flash: &mut IO,
    region_index: u32,
    region_count: u32,
) -> Result<WalRegionPrologue, StartupError> {
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    flash.read_region(region_index, Header::ENCODED_LEN, &mut prologue_bytes)?;
    WalRegionPrologue::decode(&prologue_bytes, region_count).map_err(StartupError::from)
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

    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    flash.read_region(region_index, 0, &mut header_bytes)?;
    let Ok(header) = Header::decode(&header_bytes) else {
        return Ok(false);
    };
    if header.collection_id != CollectionId(0)
        || header.collection_format != WAL_V1_FORMAT
        || header.sequence != expected_sequence
    {
        return Ok(false);
    }

    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    flash.read_region(region_index, Header::ENCODED_LEN, &mut prologue_bytes)?;
    Ok(WalRegionPrologue::decode(&prologue_bytes, region_count).is_ok())
}

fn validate_live_collection_types(collections: &[StartupCollection]) -> Result<(), StartupError> {
    for collection in collections {
        //= spec/ring.md#startup-replay-algorithm
        //# `RING-STARTUP-026` If replay yields a live collection whose `collection_type` is unsupported by the implementation, startup MUST fail.
        //= spec/ring.md#startup-replay-algorithm
        //# `RING-STARTUP-028` A dropped tombstone whose old
        //# `collection_type` is unsupported MAY remain as inert metadata and does
        //# not by itself require startup failure.
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
    //= spec/ring.md#region-reclaim
    //# `RING-REGION-RECLAIM-POST-005` If a prior tail existed, replay of free pointers MUST follow
    //# `... -> t_prev -> r`, and `r` is recognized as the tail because its
    //# free-pointer slot is uninitialized.
    let Some(mut current_region) = free_list_head else {
        return Ok(None);
    };

    let footer_offset =
        usize::try_from(metadata.region_size).map_err(|_| StartupError::InvalidFreeListChain {
            region_index: current_region,
        })? - FreePointerFooter::ENCODED_LEN;
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];

    for _visited in 0..metadata.region_count {
        flash.read_region(current_region, footer_offset, &mut footer_bytes)?;
        let footer = FreePointerFooter::decode_with_region_count(
            &footer_bytes,
            metadata.erased_byte,
            metadata.region_count,
        )
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

#[cfg(test)]
mod tests;
