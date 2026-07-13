// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};
use heapless::Vec;

use super::format::{
    align_up, basis_segment_capacity, bootstrap_layout, encode_basis_segment, encode_initial_wal,
    encode_wal_region, program_metadata_span, program_region_span, read_metadata_exact,
    read_region_exact, HeaderPurpose, RegionHeader, V3FormatConfig, V3Metadata, WAL_PROLOGUE_LEN,
};
use super::{
    FreeQueue, KernelError, LogicalQueuePosition, MaintenanceFlags, OperationId, OperationResult,
    OwnershipTable, RawFlash, RegionOwner, RegionPurpose, ReservationToken,
};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const EVENT_MAGIC: [u8; 4] = *b"EVT3";
const EVENT_HEADER_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum EventKind {
    User = 0,
    ReserveWal = 1,
    LinkWal = 2,
    FreeRegion = 3,
    ReadyRegion = 4,
    NewCollection = 5,
    TransactionCommit = 6,
}

struct PreparedWalSpare {
    token: ReservationToken,
    record_start: usize,
    sequence: u64,
    durable: bool,
    attempted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoreReplayEvent {
    ReserveWal(OperationId, u32),
    LinkWal(u32),
    FreeRegion(u32),
    ReadyRegion(u32),
}

struct SegmentScan<const MAX_CORE_EVENTS: usize> {
    append_offset: usize,
    next_operation: u64,
    core_events: Vec<CoreReplayEvent, MAX_CORE_EVENTS>,
}

/// Caller-owned scratch and startup-header storage for the v3 facade.
pub struct V3Memory<const REGION_SIZE: usize, const REGION_COUNT: usize> {
    region: [u8; REGION_SIZE],
    event_payload: [u8; REGION_SIZE],
    entries: [u32; REGION_COUNT],
    headers: [Option<RegionHeader>; REGION_COUNT],
    free_queue: FreeQueue<REGION_COUNT>,
    ownership: OwnershipTable<REGION_COUNT>,
    prepared_wal: Option<PreparedWalSpare>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> V3Memory<REGION_SIZE, REGION_COUNT> {
    pub const fn new() -> Self {
        Self {
            region: [0; REGION_SIZE],
            event_payload: [0; REGION_SIZE],
            entries: [0; REGION_COUNT],
            headers: [None; REGION_COUNT],
            free_queue: FreeQueue::new(LogicalQueuePosition(0)),
            ownership: OwnershipTable::new(),
            prepared_wal: None,
        }
    }

    /// Headers collected by the one-header-per-region startup pass.
    pub fn headers(&self) -> &[Option<RegionHeader>; REGION_COUNT] {
        &self.headers
    }

    pub fn free_queue(&self) -> &FreeQueue<REGION_COUNT> {
        &self.free_queue
    }

    pub fn ownership(&self) -> &OwnershipTable<REGION_COUNT> {
        &self.ownership
    }

    fn reset_runtime(&mut self) {
        self.free_queue.reset(LogicalQueuePosition(0));
        self.ownership.reset();
        self.prepared_wal = None;
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> Default
    for V3Memory<REGION_SIZE, REGION_COUNT>
{
    fn default() -> Self {
        Self::new()
    }
}

/// Minimal blocking v3 facade. Collection semantics are layered above this
/// append and recovery kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V3Store {
    metadata: V3Metadata,
    wal_tail_region: u32,
    wal_append_offset: usize,
    next_operation: u64,
    next_sequence: u64,
}

impl V3Store {
    pub fn metadata(&self) -> V3Metadata {
        self.metadata
    }

    pub const fn wal_tail_region(&self) -> u32 {
        self.wal_tail_region
    }

    pub const fn wal_append_offset(&self) -> usize {
        self.wal_append_offset
    }

    /// Destructively formats a v3 store, publishing metadata last.
    pub fn format<D: RawFlash, const REGION_SIZE: usize, const REGION_COUNT: usize>(
        device: &mut D,
        config: V3FormatConfig,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<Self, KernelError<D::Error>> {
        let geometry = device.geometry().validate()?;
        validate_const_geometry::<D::Error, REGION_SIZE, REGION_COUNT>(geometry)?;
        if !usize::try_from(config.wal_write_granule)
            .map_err(|_| KernelError::InvalidAlignment)?
            .is_multiple_of(geometry.program_alignment)
        {
            return Err(KernelError::InvalidAlignment);
        }

        let (basis_segments, initial_wal_region) = bootstrap_layout(geometry)?;
        let free_count = geometry
            .region_count
            .checked_sub(initial_wal_region + 1)
            .ok_or(KernelError::InsufficientRegions)?;
        if free_count < config.min_prepared_regions {
            return Err(KernelError::InsufficientRegions);
        }
        let metadata = V3Metadata {
            region_size: u32::try_from(geometry.region_size)
                .map_err(|_| KernelError::CorruptFormat)?,
            region_count: geometry.region_count,
            min_prepared_regions: config.min_prepared_regions,
            program_alignment: u32::try_from(geometry.program_alignment)
                .map_err(|_| KernelError::InvalidAlignment)?,
            bootstrap_basis_root: 0,
            bootstrap_basis_segments: basis_segments,
            initial_wal_region,
            wal_write_granule: config.wal_write_granule,
            erased_byte: geometry.erased_byte,
        };

        device.erase_metadata().map_err(KernelError::Device)?;
        for region_index in 0..geometry.region_count {
            device
                .erase_region(region_index)
                .map_err(KernelError::Device)?;
        }
        device.sync().map_err(KernelError::Device)?;

        let free_count_usize =
            usize::try_from(free_count).map_err(|_| KernelError::InsufficientRegions)?;
        for (entry, region_index) in memory.entries[..free_count_usize]
            .iter_mut()
            .zip((initial_wal_region + 1)..geometry.region_count)
        {
            *entry = region_index;
        }
        let capacity =
            basis_segment_capacity(geometry.region_size).ok_or(KernelError::InsufficientRegions)?;
        for segment in 0..basis_segments {
            let start = usize::try_from(segment)
                .map_err(|_| KernelError::InsufficientRegions)?
                .checked_mul(capacity)
                .ok_or(KernelError::InsufficientRegions)?;
            let end = start.saturating_add(capacity).min(free_count_usize);
            let encoded = encode_basis_segment(
                &mut memory.region,
                metadata,
                segment,
                u64::try_from(start).map_err(|_| KernelError::CorruptFormat)?,
                &memory.entries[start..end],
            )?;
            let program_len = align_up(encoded, geometry.program_alignment)?;
            memory.region[encoded..program_len].fill(geometry.erased_byte);
            program_region_span(device, segment, 0, &memory.region[..program_len])?;
        }
        device.sync().map_err(KernelError::Device)?;

        let wal_start = encode_initial_wal(&mut memory.region, metadata)?;
        let wal_program_len = align_up(wal_start, geometry.program_alignment)?;
        memory.region[wal_start..wal_program_len].fill(geometry.erased_byte);
        program_region_span(
            device,
            initial_wal_region,
            0,
            &memory.region[..wal_program_len],
        )?;
        device.sync().map_err(KernelError::Device)?;

        memory.region[..geometry.metadata_size.min(REGION_SIZE)].fill(geometry.erased_byte);
        let metadata_len = metadata.encode(&mut memory.region)?;
        let metadata_program_len = align_up(metadata_len, geometry.program_alignment)?;
        if metadata_program_len > geometry.metadata_size {
            return Err(KernelError::BufferTooSmall {
                needed: metadata_program_len,
                available: geometry.metadata_size,
            });
        }
        memory.region[metadata_len..metadata_program_len].fill(geometry.erased_byte);
        program_metadata_span(device, 0, &memory.region[..metadata_program_len])?;
        device.sync().map_err(KernelError::Device)?;

        initialize_formatted_runtime(metadata, memory)?;

        Ok(Self {
            metadata,
            wal_tail_region: initial_wal_region,
            wal_append_offset: wal_start,
            next_operation: 1,
            next_sequence: u64::from(basis_segments) + 1,
        })
    }

    /// Opens v3 media using one fixed header read from every region.
    pub fn open<D: RawFlash, const REGION_SIZE: usize, const REGION_COUNT: usize>(
        device: &mut D,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<Self, KernelError<D::Error>> {
        let geometry = device.geometry().validate()?;
        validate_const_geometry::<D::Error, REGION_SIZE, REGION_COUNT>(geometry)?;
        let metadata_read_len = align_up(V3Metadata::ENCODED_LEN, geometry.read_alignment)?;
        if metadata_read_len > geometry.metadata_size || metadata_read_len > REGION_SIZE {
            return Err(KernelError::BufferTooSmall {
                needed: metadata_read_len,
                available: geometry.metadata_size.min(REGION_SIZE),
            });
        }
        read_metadata_exact(device, &mut memory.region[..metadata_read_len])?;
        let metadata = V3Metadata::decode(&memory.region[..V3Metadata::ENCODED_LEN])?;
        metadata.validate_geometry(geometry)?;

        let header_read_len = align_up(RegionHeader::ENCODED_LEN, geometry.read_alignment)?;
        memory.headers.fill(None);
        let mut max_sequence = 0u64;
        for region_index in 0..geometry.region_count {
            read_region_exact(
                device,
                region_index,
                0,
                &mut memory.region[..header_read_len],
            )?;
            let header = RegionHeader::decode(
                &memory.region[..RegionHeader::ENCODED_LEN],
                metadata.erased_byte,
            )?;
            let index = usize::try_from(region_index)
                .map_err(|_| KernelError::InvalidRegionIndex(region_index))?;
            memory.headers[index] = header;
            if let Some(candidate) = header {
                max_sequence = max_sequence.max(candidate.sequence);
            }
        }

        validate_bootstrap_headers(metadata, &memory.headers)?;
        initialize_formatted_runtime(metadata, memory)?;

        let mut visited = [false; REGION_COUNT];
        let mut wal_tail_region = metadata.initial_wal_region;
        let mut wal_append_offset = wal_record_start::<D::Error>(metadata)?;
        let mut next_operation = 1u64;
        for _ in 0..REGION_COUNT {
            let tail_index = usize::try_from(wal_tail_region)
                .map_err(|_| KernelError::InvalidRegionIndex(wal_tail_region))?;
            if tail_index >= REGION_COUNT || visited[tail_index] {
                return Err(KernelError::CorruptFormat);
            }
            visited[tail_index] = true;
            let tail_header = memory.headers[tail_index].ok_or(KernelError::CorruptFormat)?;
            if tail_header.purpose != HeaderPurpose::MainWal {
                return Err(KernelError::CorruptFormat);
            }
            read_region_exact(device, wal_tail_region, 0, &mut memory.region)?;
            let scan = scan_wal::<D::Error, REGION_COUNT>(&memory.region, metadata)?;
            wal_append_offset = scan.append_offset;
            next_operation = next_operation.max(scan.next_operation);
            let mut link_target = None;
            for event in scan.core_events {
                match event {
                    CoreReplayEvent::ReserveWal(operation, target) => {
                        if memory.prepared_wal.is_some() {
                            return Err(KernelError::CorruptFormat);
                        }
                        memory
                            .free_queue
                            .apply_allocate(target)
                            .map_err(KernelError::cast)?;
                        let token = memory
                            .ownership
                            .reserve(target, RegionPurpose::MainWal, operation)
                            .map_err(KernelError::cast)?;
                        let target_index = usize::try_from(target)
                            .map_err(|_| KernelError::InvalidRegionIndex(target))?;
                        let target_header = memory.headers.get(target_index).copied().flatten();
                        let durable = target_header
                            .is_some_and(|header| header.purpose == HeaderPurpose::MainWal);
                        let sequence = target_header.map_or(max_sequence, |header| header.sequence);
                        memory.prepared_wal = Some(PreparedWalSpare {
                            token,
                            record_start: wal_record_start::<D::Error>(metadata)?,
                            sequence,
                            durable,
                            attempted: durable,
                        });
                    }
                    CoreReplayEvent::LinkWal(target) => link_target = Some(target),
                    CoreReplayEvent::FreeRegion(region) => {
                        memory
                            .ownership
                            .release(region)
                            .map_err(KernelError::cast)?;
                        memory
                            .free_queue
                            .append_dirty(region)
                            .map_err(KernelError::cast)?;
                    }
                    CoreReplayEvent::ReadyRegion(region) => {
                        memory
                            .free_queue
                            .publish_next_erased(region)
                            .map_err(KernelError::cast)?;
                        memory
                            .ownership
                            .publish_erased_ready(region)
                            .map_err(KernelError::cast)?;
                    }
                }
            }

            let Some(link_target) = link_target else {
                break;
            };
            let spare = memory
                .prepared_wal
                .take()
                .ok_or(KernelError::CorruptFormat)?;
            if !spare.durable || spare.token.region_index() != link_target {
                return Err(KernelError::CorruptFormat);
            }
            memory
                .ownership
                .publish(spare.token, RegionOwner::System(RegionPurpose::MainWal))
                .map_err(KernelError::cast)?;
            wal_tail_region = link_target;
            wal_append_offset = spare.record_start;
        }
        Ok(Self {
            metadata,
            wal_tail_region,
            wal_append_offset,
            next_operation,
            next_sequence: max_sequence
                .checked_add(1)
                .ok_or(KernelError::CorruptFormat)?,
        })
    }

    /// Appends one inline logical event and performs exactly one publication sync.
    pub fn append_inline<D: RawFlash, const REGION_SIZE: usize, const REGION_COUNT: usize>(
        &mut self,
        device: &mut D,
        payload: &[u8],
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<OperationResult<OperationId>, KernelError<D::Error>> {
        let geometry = device.geometry().validate()?;
        validate_const_geometry::<D::Error, REGION_SIZE, REGION_COUNT>(geometry)?;
        let encoded_len = event_encoded_len::<D::Error>(self.metadata, payload.len())?;
        let link_len = event_encoded_len::<D::Error>(self.metadata, size_of::<u32>())?;
        let fresh_start = wal_record_start::<D::Error>(self.metadata)?;
        if fresh_start
            .checked_add(encoded_len)
            .and_then(|end| end.checked_add(link_len))
            .is_none_or(|end| end > geometry.region_size)
        {
            return Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::PREPARE_WAL_SPARE,
            ));
        }
        let needs_rotation = self
            .wal_append_offset
            .checked_add(encoded_len)
            .and_then(|end| end.checked_add(link_len))
            .is_none_or(|end| end > geometry.region_size);
        if needs_rotation {
            self.rotate_wal(device, memory)?;
        }
        let operation =
            self.append_event_no_rotation(device, EventKind::User, payload, &mut memory.region)?;
        let granule = usize::try_from(self.metadata.wal_write_granule)
            .map_err(|_| KernelError::InvalidAlignment)?;
        let remaining = geometry.region_size - self.wal_append_offset;
        let pressure =
            if memory.prepared_wal.is_none() || remaining < link_len.saturating_add(granule) {
                MaintenanceFlags::PREPARE_WAL_SPARE
            } else {
                MaintenanceFlags::NONE
            };
        Ok(OperationResult::new(operation, pressure))
    }

    /// Rebuilds generic collection metadata from the retained WAL chain.
    /// Typed payload validation remains the responsibility of collection open.
    pub fn replay_catalog<
        D: RawFlash,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &self,
        device: &mut D,
        catalog: &mut super::CollectionCatalog<MAX_COLLECTIONS>,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<(), KernelError<D::Error>> {
        catalog.clear();
        let mut current = self.metadata.initial_wal_region;
        let mut visited = [false; REGION_COUNT];
        for _ in 0..REGION_COUNT {
            let index =
                usize::try_from(current).map_err(|_| KernelError::InvalidRegionIndex(current))?;
            if index >= REGION_COUNT || visited[index] {
                return Err(KernelError::CorruptFormat);
            }
            visited[index] = true;
            read_region_exact(device, current, 0, &mut memory.region)?;
            let scan = scan_wal::<D::Error, REGION_COUNT>(&memory.region, self.metadata)?;
            replay_catalog_segment(&memory.region, scan.append_offset, self.metadata, catalog)?;
            let link = scan.core_events.iter().find_map(|event| match event {
                CoreReplayEvent::LinkWal(target) => Some(*target),
                _ => None,
            });
            match link {
                Some(target) => current = target,
                None => return Ok(()),
            }
        }
        Err(KernelError::CorruptFormat)
    }

    /// Durably releases a published region into the ordered dirty queue.
    pub fn release_region<D: RawFlash, const REGION_SIZE: usize, const REGION_COUNT: usize>(
        &mut self,
        device: &mut D,
        region_index: u32,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<OperationResult<()>, KernelError<D::Error>> {
        if !memory.free_queue.has_append_capacity() {
            return Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::CHECKPOINT_FREE_SPACE,
            ));
        }
        if !matches!(
            memory
                .ownership
                .state(region_index)
                .map_err(KernelError::cast)?,
            super::RegionLifecycle::Published(_)
        ) {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        self.preflight_event_with_link(device, size_of::<u32>())?;
        self.append_event_no_rotation(
            device,
            EventKind::FreeRegion,
            &region_index.to_le_bytes(),
            &mut memory.region,
        )?;
        memory
            .ownership
            .release(region_index)
            .map_err(KernelError::cast)?;
        memory
            .free_queue
            .append_dirty(region_index)
            .map_err(KernelError::cast)?;
        Ok(OperationResult::new((), MaintenanceFlags::ERASE_DIRTY))
    }

    /// Publishes generic collection metadata without interpreting its payload format.
    pub fn create_collection<
        D: RawFlash,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        device: &mut D,
        entry: super::CatalogEntry,
        catalog: &mut super::CollectionCatalog<MAX_COLLECTIONS>,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<OperationResult<()>, KernelError<D::Error>> {
        catalog
            .validate_insert(entry.collection_id)
            .map_err(KernelError::cast)?;
        self.preflight_event_with_link(device, 24)?;
        memory.event_payload[..24].fill(0);
        memory.event_payload[..8].copy_from_slice(&entry.collection_id.to_le_bytes());
        write_u16(&mut memory.event_payload, 8, entry.collection_type);
        write_u64(&mut memory.event_payload, 16, entry.generation);
        let (payload, scratch) = (&memory.event_payload[..24], &mut memory.region[..]);
        self.append_event_no_rotation(device, EventKind::NewCollection, payload, scratch)?;
        catalog.insert(entry).map_err(KernelError::cast)?;
        Ok(OperationResult::new((), MaintenanceFlags::NONE))
    }

    /// Publishes every enrolled collection generation with one durable decision.
    pub fn commit_transaction<
        D: RawFlash,
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        const MAX_COLLECTIONS: usize,
    >(
        &mut self,
        device: &mut D,
        transaction: &mut super::TransactionMemory<MAX_COLLECTIONS>,
        catalog: &mut super::CollectionCatalog<MAX_COLLECTIONS>,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<OperationResult<OperationId>, KernelError<D::Error>> {
        let transaction_id = transaction
            .active()
            .ok_or(KernelError::TransactionNotOpen)?;
        let changes = transaction.prepare_commit().map_err(KernelError::cast)?;
        catalog
            .validate_commit(changes)
            .map_err(KernelError::cast)?;
        let needed = 16usize
            .checked_add(
                changes
                    .len()
                    .checked_mul(24)
                    .ok_or(KernelError::CorruptFormat)?,
            )
            .ok_or(KernelError::CorruptFormat)?;
        if needed > REGION_SIZE {
            return Err(KernelError::BufferTooSmall {
                needed,
                available: REGION_SIZE,
            });
        }
        self.preflight_event_with_link(device, needed)?;
        memory.event_payload[..needed].fill(0);
        write_u64(&mut memory.event_payload, 0, transaction_id.0);
        write_u32(
            &mut memory.event_payload,
            8,
            u32::try_from(changes.len()).map_err(|_| KernelError::CorruptFormat)?,
        );
        let mut offset = 16usize;
        for change in changes {
            memory.event_payload[offset..offset + 8]
                .copy_from_slice(&change.collection_id.to_le_bytes());
            write_u64(
                &mut memory.event_payload,
                offset + 8,
                change.committed_generation,
            );
            write_u64(
                &mut memory.event_payload,
                offset + 16,
                change.private_generation,
            );
            offset += 24;
        }
        let (payload, scratch) = (&memory.event_payload[..needed], &mut memory.region[..]);
        let operation =
            self.append_event_no_rotation(device, EventKind::TransactionCommit, payload, scratch)?;
        catalog.apply_commit(changes).map_err(KernelError::cast)?;
        transaction.apply_commit().map_err(KernelError::cast)?;
        Ok(OperationResult::new(operation, MaintenanceFlags::NONE))
    }

    /// Performs one explicit bounded maintenance task.
    pub fn maintain_once<D: RawFlash, const REGION_SIZE: usize, const REGION_COUNT: usize>(
        &mut self,
        device: &mut D,
        task: super::MaintenanceTask,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<super::MaintenanceOutcome, KernelError<D::Error>> {
        match task {
            super::MaintenanceTask::EraseDirty => {
                let Some(region_index) = memory.free_queue.next_dirty() else {
                    return Ok(super::MaintenanceOutcome {
                        progressed: false,
                        remaining: MaintenanceFlags::NONE,
                    });
                };
                self.preflight_event_with_link(device, size_of::<u32>())?;
                device
                    .erase_region(region_index)
                    .map_err(KernelError::Device)?;
                self.append_event_no_rotation(
                    device,
                    EventKind::ReadyRegion,
                    &region_index.to_le_bytes(),
                    &mut memory.region,
                )?;
                memory
                    .free_queue
                    .publish_next_erased(region_index)
                    .map_err(KernelError::cast)?;
                memory
                    .ownership
                    .publish_erased_ready(region_index)
                    .map_err(KernelError::cast)?;
                Ok(super::MaintenanceOutcome {
                    progressed: true,
                    remaining: if memory.free_queue.next_dirty().is_some() {
                        MaintenanceFlags::ERASE_DIRTY
                    } else {
                        MaintenanceFlags::NONE
                    },
                })
            }
            super::MaintenanceTask::PrepareWalSpare => {
                if memory
                    .prepared_wal
                    .as_ref()
                    .is_some_and(|spare| spare.durable)
                {
                    return Ok(super::MaintenanceOutcome {
                        progressed: false,
                        remaining: MaintenanceFlags::NONE,
                    });
                }
                if memory.prepared_wal.is_none() {
                    self.preflight_event_with_link(device, size_of::<u32>())?;
                    let region_index = memory.free_queue.next_prepared().ok_or(
                        KernelError::MaintenanceRequired(MaintenanceFlags::ERASE_DIRTY),
                    )?;
                    let payload = region_index.to_le_bytes();
                    let operation = self.append_event_no_rotation(
                        device,
                        EventKind::ReserveWal,
                        &payload,
                        &mut memory.region,
                    )?;
                    memory
                        .free_queue
                        .apply_allocate(region_index)
                        .map_err(KernelError::cast)?;
                    let token = memory
                        .ownership
                        .reserve(region_index, RegionPurpose::MainWal, operation)
                        .map_err(KernelError::cast)?;
                    let record_start = wal_record_start::<D::Error>(self.metadata)?;
                    memory.prepared_wal = Some(PreparedWalSpare {
                        token,
                        record_start,
                        sequence: self.next_sequence,
                        durable: false,
                        attempted: false,
                    });
                    self.next_sequence = self
                        .next_sequence
                        .checked_add(1)
                        .ok_or(KernelError::CorruptFormat)?;
                }
                self.finish_wal_spare(device, memory)?;
                Ok(super::MaintenanceOutcome {
                    progressed: true,
                    remaining: MaintenanceFlags::NONE,
                })
            }
            _ => Ok(super::MaintenanceOutcome {
                progressed: false,
                remaining: maintenance_flag_for_task(task),
            }),
        }
    }

    fn finish_wal_spare<D: RawFlash, const REGION_SIZE: usize, const REGION_COUNT: usize>(
        &mut self,
        device: &mut D,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<(), KernelError<D::Error>> {
        let mut spare = memory
            .prepared_wal
            .take()
            .ok_or(KernelError::InvalidOwnershipTransition)?;
        if spare.durable {
            memory.prepared_wal = Some(spare);
            return Ok(());
        }
        let region_index = spare.token.region_index();
        if spare.attempted {
            if let Err(error) = device
                .erase_region(region_index)
                .map_err(KernelError::Device)
            {
                memory.prepared_wal = Some(spare);
                return Err(error);
            }
        }
        spare.attempted = true;
        let geometry = device.geometry().validate()?;
        let encoded = encode_wal_region(
            &mut memory.region,
            self.metadata,
            region_index,
            spare.sequence,
        )?;
        let program_len = align_up(encoded, geometry.program_alignment)?;
        memory.region[encoded..program_len].fill(self.metadata.erased_byte);
        if let Err(error) =
            program_region_span(device, region_index, 0, &memory.region[..program_len])
        {
            memory.prepared_wal = Some(spare);
            return Err(error);
        }
        if let Err(error) = device.sync().map_err(KernelError::Device) {
            memory.prepared_wal = Some(spare);
            return Err(error);
        }
        spare.durable = true;
        memory.prepared_wal = Some(spare);
        Ok(())
    }

    fn rotate_wal<D: RawFlash, const REGION_SIZE: usize, const REGION_COUNT: usize>(
        &mut self,
        device: &mut D,
        memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
    ) -> Result<(), KernelError<D::Error>> {
        let spare = memory
            .prepared_wal
            .take()
            .ok_or(KernelError::MaintenanceRequired(
                MaintenanceFlags::PREPARE_WAL_SPARE,
            ))?;
        if !spare.durable {
            memory.prepared_wal = Some(spare);
            return Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::PREPARE_WAL_SPARE,
            ));
        }
        let region_index = spare.token.region_index();
        let payload = region_index.to_le_bytes();
        if let Err(error) =
            self.append_event_no_rotation(device, EventKind::LinkWal, &payload, &mut memory.region)
        {
            memory.prepared_wal = Some(spare);
            return Err(error);
        }
        memory
            .ownership
            .publish(spare.token, RegionOwner::System(RegionPurpose::MainWal))
            .map_err(KernelError::cast)?;
        self.wal_tail_region = region_index;
        self.wal_append_offset = spare.record_start;
        Ok(())
    }

    fn append_event_no_rotation<D: RawFlash>(
        &mut self,
        device: &mut D,
        kind: EventKind,
        payload: &[u8],
        scratch: &mut [u8],
    ) -> Result<OperationId, KernelError<D::Error>> {
        let geometry = device.geometry().validate()?;
        let encoded_len = event_encoded_len::<D::Error>(self.metadata, payload.len())?;
        let end = self
            .wal_append_offset
            .checked_add(encoded_len)
            .ok_or(KernelError::CorruptFormat)?;
        if end > geometry.region_size {
            return Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::PREPARE_WAL_SPARE,
            ));
        }
        if encoded_len > scratch.len() {
            return Err(KernelError::BufferTooSmall {
                needed: encoded_len,
                available: scratch.len(),
            });
        }
        encode_event(
            &mut scratch[..encoded_len],
            self.metadata,
            self.next_operation,
            kind,
            payload,
        )?;
        program_region_span(
            device,
            self.wal_tail_region,
            self.wal_append_offset,
            &scratch[..encoded_len],
        )?;
        device.sync().map_err(KernelError::Device)?;
        let operation = OperationId(self.next_operation);
        self.wal_append_offset = end;
        self.next_operation = self
            .next_operation
            .checked_add(1)
            .ok_or(KernelError::CorruptFormat)?;
        Ok(operation)
    }

    /// Ensures a one-sync record cannot strand the WAL without enough room to
    /// publish its successor. This check performs no raw device I/O.
    fn preflight_event_with_link<D: RawFlash>(
        &self,
        device: &D,
        payload_len: usize,
    ) -> Result<(), KernelError<D::Error>> {
        let geometry = device.geometry().validate()?;
        let event_len = event_encoded_len::<D::Error>(self.metadata, payload_len)?;
        let link_len = event_encoded_len::<D::Error>(self.metadata, size_of::<u32>())?;
        if self
            .wal_append_offset
            .checked_add(event_len)
            .and_then(|end| end.checked_add(link_len))
            .is_none_or(|end| end > geometry.region_size)
        {
            return Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::PREPARE_WAL_SPARE,
            ));
        }
        Ok(())
    }
}

fn maintenance_flag_for_task(task: super::MaintenanceTask) -> MaintenanceFlags {
    match task {
        super::MaintenanceTask::EraseDirty => MaintenanceFlags::ERASE_DIRTY,
        super::MaintenanceTask::PrepareWalSpare => MaintenanceFlags::PREPARE_WAL_SPARE,
        super::MaintenanceTask::BuildFreeSpaceBasis
        | super::MaintenanceTask::PublishFreeSpaceBasis => MaintenanceFlags::CHECKPOINT_FREE_SPACE,
        super::MaintenanceTask::ReclaimWal => MaintenanceFlags::RECLAIM_WAL,
        super::MaintenanceTask::FinishTransaction => MaintenanceFlags::FINISH_TRANSACTION,
    }
}

fn wal_record_start<E>(metadata: V3Metadata) -> Result<usize, KernelError<E>> {
    align_up(
        RegionHeader::ENCODED_LEN + WAL_PROLOGUE_LEN,
        usize::try_from(metadata.wal_write_granule).map_err(|_| KernelError::InvalidAlignment)?,
    )
}

fn event_encoded_len<E>(metadata: V3Metadata, payload_len: usize) -> Result<usize, KernelError<E>> {
    let raw = EVENT_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(KernelError::CorruptFormat)?;
    align_up(
        raw,
        usize::try_from(metadata.wal_write_granule).map_err(|_| KernelError::InvalidAlignment)?,
    )
}

fn encode_event<E>(
    output: &mut [u8],
    metadata: V3Metadata,
    operation: u64,
    kind: EventKind,
    payload: &[u8],
) -> Result<(), KernelError<E>> {
    let encoded_len = event_encoded_len::<E>(metadata, payload.len())?;
    if output.len() < encoded_len {
        return Err(KernelError::BufferTooSmall {
            needed: encoded_len,
            available: output.len(),
        });
    }
    output[..encoded_len].fill(metadata.erased_byte);
    output[..4].copy_from_slice(&EVENT_MAGIC);
    write_u32(
        output,
        4,
        u32::try_from(encoded_len).map_err(|_| KernelError::CorruptFormat)?,
    );
    write_u64(output, 8, operation);
    write_u16(output, 16, kind as u16);
    write_u32(
        output,
        20,
        u32::try_from(payload.len()).map_err(|_| KernelError::CorruptFormat)?,
    );
    output[EVENT_HEADER_LEN..EVENT_HEADER_LEN + payload.len()].copy_from_slice(payload);
    let mut digest = CRC32C.digest();
    digest.update(&output[..28]);
    digest.update(&output[EVENT_HEADER_LEN..EVENT_HEADER_LEN + payload.len()]);
    write_u32(output, 28, digest.finalize());
    Ok(())
}

fn initialize_formatted_runtime<E, const REGION_SIZE: usize, const REGION_COUNT: usize>(
    metadata: V3Metadata,
    memory: &mut V3Memory<REGION_SIZE, REGION_COUNT>,
) -> Result<(), KernelError<E>> {
    memory.reset_runtime();
    for segment in 0..metadata.bootstrap_basis_segments {
        let token = memory
            .ownership
            .reserve(
                segment,
                RegionPurpose::FreeSpaceBasis,
                OperationId(u64::from(segment)),
            )
            .map_err(KernelError::cast)?;
        memory
            .ownership
            .publish(token, RegionOwner::System(RegionPurpose::FreeSpaceBasis))
            .map_err(KernelError::cast)?;
    }
    let wal_token = memory
        .ownership
        .reserve(
            metadata.initial_wal_region,
            RegionPurpose::MainWal,
            OperationId(u64::from(metadata.initial_wal_region)),
        )
        .map_err(KernelError::cast)?;
    memory
        .ownership
        .publish(wal_token, RegionOwner::System(RegionPurpose::MainWal))
        .map_err(KernelError::cast)?;
    for region in (metadata.initial_wal_region + 1)..metadata.region_count {
        memory
            .free_queue
            .append_prepared(region)
            .map_err(KernelError::cast)?;
    }
    memory
        .free_queue
        .install_current_basis()
        .map_err(KernelError::cast)?;
    Ok(())
}

fn validate_const_geometry<E, const REGION_SIZE: usize, const REGION_COUNT: usize>(
    geometry: super::DeviceGeometry,
) -> Result<(), KernelError<E>> {
    if geometry.region_size != REGION_SIZE
        || usize::try_from(geometry.region_count).ok() != Some(REGION_COUNT)
        || geometry.metadata_size > REGION_SIZE
    {
        return Err(KernelError::CorruptFormat);
    }
    Ok(())
}

fn validate_bootstrap_headers<E, const REGION_COUNT: usize>(
    metadata: V3Metadata,
    headers: &[Option<RegionHeader>; REGION_COUNT],
) -> Result<(), KernelError<E>> {
    for segment in 0..metadata.bootstrap_basis_segments {
        let index =
            usize::try_from(segment).map_err(|_| KernelError::InvalidRegionIndex(segment))?;
        let header = headers
            .get(index)
            .copied()
            .flatten()
            .ok_or(KernelError::CorruptFormat)?;
        let expected_next =
            (segment + 1 < metadata.bootstrap_basis_segments).then_some(segment + 1);
        if header.purpose != HeaderPurpose::FreeSpaceBasis || header.next_region != expected_next {
            return Err(KernelError::CorruptFormat);
        }
    }
    Ok(())
}

fn scan_wal<E, const MAX_CORE_EVENTS: usize>(
    region: &[u8],
    metadata: V3Metadata,
) -> Result<SegmentScan<MAX_CORE_EVENTS>, KernelError<E>> {
    if region.len() < RegionHeader::ENCODED_LEN + WAL_PROLOGUE_LEN {
        return Err(KernelError::CorruptFormat);
    }
    let record_start = usize::try_from(read_u32(
        region,
        RegionHeader::ENCODED_LEN + size_of::<u32>(),
    ))
    .map_err(|_| KernelError::CorruptFormat)?;
    let granule =
        usize::try_from(metadata.wal_write_granule).map_err(|_| KernelError::InvalidAlignment)?;
    if record_start > region.len() || !record_start.is_multiple_of(granule) {
        return Err(KernelError::CorruptFormat);
    }
    let mut offset = record_start;
    let mut next_operation = 1u64;
    let mut core_events = Vec::new();
    let mut link_seen = false;
    while offset < region.len() {
        let remaining = &region[offset..];
        if remaining.iter().all(|byte| *byte == metadata.erased_byte) {
            return Ok(SegmentScan {
                append_offset: offset,
                next_operation,
                core_events,
            });
        }
        if remaining.len() < EVENT_HEADER_LEN || remaining[..4] != EVENT_MAGIC {
            return Ok(SegmentScan {
                append_offset: offset,
                next_operation,
                core_events,
            });
        }
        let total_len =
            usize::try_from(read_u32(remaining, 4)).map_err(|_| KernelError::CorruptFormat)?;
        let kind = match event_kind::<E>(read_u16(remaining, 16)) {
            Ok(kind) => kind,
            Err(_) => {
                return Ok(SegmentScan {
                    append_offset: offset,
                    next_operation,
                    core_events,
                });
            }
        };
        let payload_len =
            usize::try_from(read_u32(remaining, 20)).map_err(|_| KernelError::CorruptFormat)?;
        if total_len < EVENT_HEADER_LEN
            || !total_len.is_multiple_of(granule)
            || total_len > remaining.len()
            || payload_len > total_len - EVENT_HEADER_LEN
        {
            return Ok(SegmentScan {
                append_offset: offset,
                next_operation,
                core_events,
            });
        }
        let mut digest = CRC32C.digest();
        digest.update(&remaining[..28]);
        digest.update(&remaining[EVENT_HEADER_LEN..EVENT_HEADER_LEN + payload_len]);
        if read_u32(remaining, 28) != digest.finalize() {
            return Ok(SegmentScan {
                append_offset: offset,
                next_operation,
                core_events,
            });
        }
        next_operation = read_u64(remaining, 8)
            .checked_add(1)
            .ok_or(KernelError::CorruptFormat)?;
        let payload = &remaining[EVENT_HEADER_LEN..EVENT_HEADER_LEN + payload_len];
        match kind {
            EventKind::User => {}
            EventKind::ReserveWal => {
                if link_seen
                    || core_events
                        .iter()
                        .any(|event| matches!(event, CoreReplayEvent::ReserveWal(_, _)))
                    || payload.len() != size_of::<u32>()
                {
                    return Err(KernelError::CorruptFormat);
                }
                core_events
                    .push(CoreReplayEvent::ReserveWal(
                        OperationId(read_u64(remaining, 8)),
                        read_u32(payload, 0),
                    ))
                    .map_err(|_| KernelError::CorruptFormat)?;
            }
            EventKind::LinkWal => {
                if link_seen || payload.len() != size_of::<u32>() {
                    return Err(KernelError::CorruptFormat);
                }
                link_seen = true;
                core_events
                    .push(CoreReplayEvent::LinkWal(read_u32(payload, 0)))
                    .map_err(|_| KernelError::CorruptFormat)?;
            }
            EventKind::FreeRegion | EventKind::ReadyRegion => {
                if link_seen || payload.len() != size_of::<u32>() {
                    return Err(KernelError::CorruptFormat);
                }
                let event = if kind == EventKind::FreeRegion {
                    CoreReplayEvent::FreeRegion(read_u32(payload, 0))
                } else {
                    CoreReplayEvent::ReadyRegion(read_u32(payload, 0))
                };
                core_events
                    .push(event)
                    .map_err(|_| KernelError::CorruptFormat)?;
            }
            EventKind::NewCollection | EventKind::TransactionCommit => {}
        }
        offset += total_len;
        if link_seen
            && region[offset..]
                .iter()
                .any(|byte| *byte != metadata.erased_byte)
        {
            return Err(KernelError::CorruptFormat);
        }
    }
    Ok(SegmentScan {
        append_offset: offset,
        next_operation,
        core_events,
    })
}

fn event_kind<E>(encoded: u16) -> Result<EventKind, KernelError<E>> {
    match encoded {
        0 => Ok(EventKind::User),
        1 => Ok(EventKind::ReserveWal),
        2 => Ok(EventKind::LinkWal),
        3 => Ok(EventKind::FreeRegion),
        4 => Ok(EventKind::ReadyRegion),
        5 => Ok(EventKind::NewCollection),
        6 => Ok(EventKind::TransactionCommit),
        _ => Err(KernelError::CorruptFormat),
    }
}

fn replay_catalog_segment<E, const MAX_COLLECTIONS: usize>(
    region: &[u8],
    append_offset: usize,
    metadata: V3Metadata,
    catalog: &mut super::CollectionCatalog<MAX_COLLECTIONS>,
) -> Result<(), KernelError<E>> {
    let mut offset = wal_record_start::<E>(metadata)?;
    while offset < append_offset {
        let record = &region[offset..append_offset];
        let total_len =
            usize::try_from(read_u32(record, 4)).map_err(|_| KernelError::CorruptFormat)?;
        let payload_len =
            usize::try_from(read_u32(record, 20)).map_err(|_| KernelError::CorruptFormat)?;
        let payload = &record[EVENT_HEADER_LEN..EVENT_HEADER_LEN + payload_len];
        match event_kind::<E>(read_u16(record, 16))? {
            EventKind::NewCollection => {
                if payload.len() != 24 {
                    return Err(KernelError::CorruptFormat);
                }
                catalog
                    .insert(super::CatalogEntry {
                        collection_id: crate::CollectionId::new(read_u64(payload, 0)),
                        collection_type: read_u16(payload, 8),
                        generation: read_u64(payload, 16),
                    })
                    .map_err(KernelError::cast)?;
            }
            EventKind::TransactionCommit => {
                if payload.len() < 16 {
                    return Err(KernelError::CorruptFormat);
                }
                let count = usize::try_from(read_u32(payload, 8))
                    .map_err(|_| KernelError::CorruptFormat)?;
                let expected = 16usize
                    .checked_add(count.checked_mul(24).ok_or(KernelError::CorruptFormat)?)
                    .ok_or(KernelError::CorruptFormat)?;
                if expected != payload.len() || count > MAX_COLLECTIONS {
                    return Err(KernelError::CorruptFormat);
                }
                let mut changes = Vec::<super::EnrolledCollection, MAX_COLLECTIONS>::new();
                let mut payload_offset = 16usize;
                for _ in 0..count {
                    changes
                        .push(super::EnrolledCollection {
                            collection_id: crate::CollectionId::new(read_u64(
                                payload,
                                payload_offset,
                            )),
                            committed_generation: read_u64(payload, payload_offset + 8),
                            private_generation: read_u64(payload, payload_offset + 16),
                        })
                        .map_err(|_| KernelError::CorruptFormat)?;
                    payload_offset += 24;
                }
                catalog
                    .apply_commit(changes.as_slice())
                    .map_err(KernelError::cast)?;
            }
            _ => {}
        }
        offset = offset
            .checked_add(total_len)
            .ok_or(KernelError::CorruptFormat)?;
    }
    Ok(())
}

fn write_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_le_bytes());
}
fn write_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + size_of::<u16>()].copy_from_slice(&value.to_le_bytes());
}
fn write_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + size_of::<u64>()].copy_from_slice(&value.to_le_bytes());
}
fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
    ])
}

fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([input[offset], input[offset + 1]])
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
        input[offset + 4],
        input[offset + 5],
        input[offset + 6],
        input[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{
        CrashPersistence, FailureMode, TraceFlash, TraceFlashError, TraceOperation,
    };

    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 8;
    const MAX_EVENTS: usize = 256;

    fn flash() -> TraceFlash<REGION_SIZE, REGION_COUNT, MAX_EVENTS> {
        TraceFlash::new(0xff, 8, 8, REGION_SIZE, REGION_SIZE).unwrap()
    }

    fn config() -> V3FormatConfig {
        V3FormatConfig::new(2, 8)
    }

    fn new_memory() -> V3Memory<REGION_SIZE, REGION_COUNT> {
        V3Memory::new()
    }

    #[test]
    fn format_publishes_metadata_last_and_open_scans_one_header_per_region() {
        let mut flash = flash();
        let mut memory = new_memory();
        let formatted = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        assert_eq!(formatted.metadata().region_count, REGION_COUNT as u32);
        assert!(matches!(
            flash.operations()[flash.operations().len() - 2],
            TraceOperation::ProgramMetadata { .. }
        ));
        assert_eq!(flash.operations().last(), Some(&TraceOperation::Sync));

        flash.clear_trace();
        let reopened = V3Store::open(&mut flash, &mut memory).unwrap();
        assert_eq!(reopened.wal_tail_region(), formatted.wal_tail_region());
        let header_reads = flash
            .operations()
            .iter()
            .filter(|operation| {
                matches!(
                    operation,
                    TraceOperation::ReadRegion {
                        offset: 0,
                        len: RegionHeader::ENCODED_LEN,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(header_reads, REGION_COUNT);
    }

    #[test]
    fn failed_final_format_sync_leaves_media_unformatted_after_crash() {
        let final_operation = {
            let mut probe = flash();
            let mut memory = new_memory();
            V3Store::format(&mut probe, config(), &mut memory).unwrap();
            probe.operation_number()
        };

        let mut flash = flash();
        flash.inject_failure(FailureMode::Before {
            operation: final_operation,
        });
        let mut memory = new_memory();
        assert_eq!(
            V3Store::format(&mut flash, config(), &mut memory),
            Err(KernelError::Device(TraceFlashError::InjectedFailure {
                operation: final_operation
            }))
        );
        flash.crash();
        assert_eq!(
            V3Store::open(&mut flash, &mut memory),
            Err(KernelError::Unformatted)
        );
    }

    #[test]
    fn inline_append_is_one_contiguous_program_and_one_sync() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        flash.clear_trace();

        let result = store
            .append_inline(&mut flash, &[1, 2, 3], &mut memory)
            .unwrap();
        assert_eq!(result.value, OperationId(1));
        assert_eq!(flash.counts().region_programs, 1);
        assert_eq!(flash.counts().syncs, 1);
        assert_eq!(flash.counts().region_reads, 0);
        assert_eq!(flash.counts().erases, 0);

        let append_offset = store.wal_append_offset();
        let reopened = V3Store::open(&mut flash, &mut memory).unwrap();
        assert_eq!(reopened.wal_append_offset(), append_offset);
    }

    #[test]
    fn insufficient_inline_capacity_is_rejected_before_io() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        flash.clear_trace();
        let oversized = [0u8; REGION_SIZE];
        assert_eq!(
            store.append_inline(&mut flash, &oversized, &mut memory),
            Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::PREPARE_WAL_SPARE
            ))
        );
        assert!(flash.operations().is_empty());
    }

    #[test]
    fn torn_unsynced_append_disappears_on_crash() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let old_offset = store.wal_append_offset();
        let program_operation = flash.operation_number() + 1;
        flash.inject_failure(FailureMode::TornProgram {
            operation: program_operation,
            programmed_bytes: 8,
        });
        assert_eq!(
            store.append_inline(&mut flash, &[9, 8, 7], &mut memory),
            Err(KernelError::Device(TraceFlashError::InjectedFailure {
                operation: program_operation
            }))
        );
        flash.crash();
        let reopened = V3Store::open(&mut flash, &mut memory).unwrap();
        assert_eq!(reopened.wal_append_offset(), old_offset);
    }

    #[test]
    fn explicit_spare_preparation_reserves_then_syncs_target_without_erase() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        flash.clear_trace();

        let outcome = store
            .maintain_once(
                &mut flash,
                super::super::MaintenanceTask::PrepareWalSpare,
                &mut memory,
            )
            .unwrap();
        assert!(outcome.progressed);
        assert_eq!(flash.counts().region_programs, 2);
        assert_eq!(flash.counts().syncs, 2);
        assert_eq!(flash.counts().erases, 0);
        let spare = memory.prepared_wal.as_ref().unwrap();
        assert!(spare.durable);
        assert!(matches!(
            memory.ownership.state(spare.token.region_index()).unwrap(),
            super::super::RegionLifecycle::Reserved { .. }
        ));
    }

    #[test]
    fn startup_ignores_a_durable_but_unlinked_prepared_wal() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let old_tail = store.wal_tail_region();
        store
            .maintain_once(
                &mut flash,
                super::super::MaintenanceTask::PrepareWalSpare,
                &mut memory,
            )
            .unwrap();
        let prepared = memory
            .prepared_wal
            .as_ref()
            .map(|spare| spare.token.region_index())
            .unwrap();
        assert_ne!(prepared, old_tail);

        let mut reopen_memory = new_memory();
        let reopened = V3Store::open(&mut flash, &mut reopen_memory).unwrap();
        assert_eq!(reopened.wal_tail_region(), old_tail);
        assert_eq!(
            reopen_memory
                .prepared_wal
                .as_ref()
                .map(|spare| spare.token.region_index()),
            Some(prepared)
        );
    }

    #[test]
    fn foreground_rotation_uses_prepared_target_and_one_extra_sync() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        store
            .maintain_once(
                &mut flash,
                super::super::MaintenanceTask::PrepareWalSpare,
                &mut memory,
            )
            .unwrap();
        let old_tail = store.wal_tail_region();
        let payload = [5u8; 16];
        store
            .append_inline(&mut flash, &payload, &mut memory)
            .unwrap();
        store
            .append_inline(&mut flash, &payload, &mut memory)
            .unwrap();
        flash.clear_trace();

        store
            .append_inline(&mut flash, &payload, &mut memory)
            .unwrap();
        assert_ne!(store.wal_tail_region(), old_tail);
        assert_eq!(flash.counts().region_programs, 2);
        assert_eq!(flash.counts().syncs, 2);
        assert_eq!(flash.counts().erases, 0);

        let new_tail = store.wal_tail_region();
        let mut reopen_memory = new_memory();
        let reopened = V3Store::open(&mut flash, &mut reopen_memory).unwrap();
        assert_eq!(reopened.wal_tail_region(), new_tail);
    }

    #[test]
    fn crash_after_link_before_user_record_recovers_new_tail() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        store
            .maintain_once(
                &mut flash,
                super::super::MaintenanceTask::PrepareWalSpare,
                &mut memory,
            )
            .unwrap();
        let payload = [6u8; 16];
        store
            .append_inline(&mut flash, &payload, &mut memory)
            .unwrap();
        store
            .append_inline(&mut flash, &payload, &mut memory)
            .unwrap();
        let target = memory
            .prepared_wal
            .as_ref()
            .map(|spare| spare.token.region_index())
            .unwrap();
        let fail_user_program = flash.operation_number() + 3;
        flash.inject_failure(FailureMode::Before {
            operation: fail_user_program,
        });
        assert_eq!(
            store.append_inline(&mut flash, &payload, &mut memory),
            Err(KernelError::Device(TraceFlashError::InjectedFailure {
                operation: fail_user_program
            }))
        );
        flash.crash();

        let mut reopen_memory = new_memory();
        let reopened = V3Store::open(&mut flash, &mut reopen_memory).unwrap();
        assert_eq!(reopened.wal_tail_region(), target);
        assert_eq!(
            reopened.wal_append_offset(),
            wal_record_start::<TraceFlashError>(reopened.metadata()).unwrap()
        );
    }

    #[test]
    fn complete_unsynced_tail_may_survive_a_failed_sync() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let old_offset = store.wal_append_offset();
        let sync_operation = flash.operation_number() + 2;
        flash.inject_failure(FailureMode::Before {
            operation: sync_operation,
        });
        assert_eq!(
            store.append_inline(&mut flash, &[1, 3, 5], &mut memory),
            Err(KernelError::Device(TraceFlashError::InjectedFailure {
                operation: sync_operation
            }))
        );
        flash.crash_with(CrashPersistence::PersistWorking);
        let mut reopen_memory = new_memory();
        let reopened = V3Store::open(&mut flash, &mut reopen_memory).unwrap();
        assert!(reopened.wal_append_offset() > old_offset);
    }

    #[test]
    fn release_and_erase_are_separate_bounded_io_patterns() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let region = memory.free_queue.next_prepared().unwrap();
        memory.free_queue.apply_allocate(region).unwrap();
        let token = memory
            .ownership
            .reserve(
                region,
                RegionPurpose::CollectionData { collection_type: 7 },
                OperationId(90),
            )
            .unwrap();
        memory
            .ownership
            .publish(
                token,
                RegionOwner::Collection {
                    collection_id: crate::CollectionId::new(12),
                    collection_type: 7,
                },
            )
            .unwrap();

        flash.clear_trace();
        let released = store
            .release_region(&mut flash, region, &mut memory)
            .unwrap();
        assert!(released.maintenance.contains(MaintenanceFlags::ERASE_DIRTY));
        assert_eq!(flash.counts().region_programs, 1);
        assert_eq!(flash.counts().syncs, 1);
        assert_eq!(flash.counts().erases, 0);

        flash.clear_trace();
        store
            .maintain_once(
                &mut flash,
                super::super::MaintenanceTask::EraseDirty,
                &mut memory,
            )
            .unwrap();
        assert_eq!(flash.counts().erases, 1);
        assert_eq!(flash.counts().region_programs, 1);
        assert_eq!(flash.counts().syncs, 1);
        assert_eq!(
            memory.ownership.state(region).unwrap(),
            super::super::RegionLifecycle::ErasedPrepared
        );
    }

    #[test]
    fn erase_maintenance_preflights_readiness_record_before_erasing() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let region = memory.free_queue.next_prepared().unwrap();
        memory.free_queue.apply_allocate(region).unwrap();
        let token = memory
            .ownership
            .reserve(
                region,
                RegionPurpose::CollectionData { collection_type: 7 },
                OperationId(91),
            )
            .unwrap();
        memory
            .ownership
            .publish(
                token,
                RegionOwner::Collection {
                    collection_id: crate::CollectionId::new(13),
                    collection_type: 7,
                },
            )
            .unwrap();
        store
            .release_region(&mut flash, region, &mut memory)
            .unwrap();

        let payload = [7u8; 16];
        let mut exhausted = false;
        for _ in 0..REGION_COUNT {
            match store.append_inline(&mut flash, &payload, &mut memory) {
                Ok(_) => {}
                Err(error) => {
                    assert_eq!(
                        error,
                        KernelError::MaintenanceRequired(MaintenanceFlags::PREPARE_WAL_SPARE)
                    );
                    exhausted = true;
                    break;
                }
            }
        }
        assert!(exhausted);

        flash.clear_trace();
        assert_eq!(
            store.maintain_once(
                &mut flash,
                super::super::MaintenanceTask::EraseDirty,
                &mut memory,
            ),
            Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::PREPARE_WAL_SPARE
            ))
        );
        assert!(flash.operations().is_empty());
        assert_eq!(
            memory.ownership.state(region).unwrap(),
            super::super::RegionLifecycle::Dirty
        );
    }

    #[test]
    fn persisted_torn_tail_is_ignored_at_its_start() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let old_offset = store.wal_append_offset();
        let program_operation = flash.operation_number() + 1;
        flash.inject_failure(FailureMode::TornProgram {
            operation: program_operation,
            programmed_bytes: 8,
        });
        assert!(store
            .append_inline(&mut flash, &[2, 4, 6], &mut memory)
            .is_err());
        flash.crash_with(CrashPersistence::PersistWorking);
        let mut reopen_memory = new_memory();
        let reopened = V3Store::open(&mut flash, &mut reopen_memory).unwrap();
        assert_eq!(reopened.wal_append_offset(), old_offset);
    }

    #[test]
    fn multi_collection_commit_is_one_durable_batch_then_one_runtime_apply() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let first = crate::CollectionId::new(21);
        let second = crate::CollectionId::new(22);
        let mut catalog = super::super::CollectionCatalog::<2>::new();
        catalog
            .insert(super::super::CatalogEntry {
                collection_id: first,
                collection_type: 1,
                generation: 0,
            })
            .unwrap();
        catalog
            .insert(super::super::CatalogEntry {
                collection_id: second,
                collection_type: 2,
                generation: 0,
            })
            .unwrap();
        let mut transaction = super::super::TransactionMemory::<2>::new();
        transaction.begin(super::super::TransactionId(5)).unwrap();
        transaction.enroll(first, 0).unwrap();
        transaction.enroll(second, 0).unwrap();
        transaction.stage_write(first).unwrap();
        transaction.stage_write(second).unwrap();
        flash.clear_trace();

        store
            .commit_transaction(&mut flash, &mut transaction, &mut catalog, &mut memory)
            .unwrap();
        assert_eq!(flash.counts().region_programs, 1);
        assert_eq!(flash.counts().syncs, 1);
        assert_eq!(catalog.entry(first).unwrap().generation, 1);
        assert_eq!(catalog.entry(second).unwrap().generation, 1);
        assert_eq!(transaction.active(), None);
    }

    #[test]
    fn transaction_generation_conflict_is_rejected_before_io() {
        let mut flash = flash();
        let mut memory = new_memory();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let collection = crate::CollectionId::new(31);
        let mut catalog = super::super::CollectionCatalog::<1>::new();
        catalog
            .insert(super::super::CatalogEntry {
                collection_id: collection,
                collection_type: 3,
                generation: 4,
            })
            .unwrap();
        let mut transaction = super::super::TransactionMemory::<1>::new();
        transaction.begin(super::super::TransactionId(6)).unwrap();
        transaction.enroll(collection, 3).unwrap();
        transaction.stage_write(collection).unwrap();
        flash.clear_trace();
        assert_eq!(
            store.commit_transaction(&mut flash, &mut transaction, &mut catalog, &mut memory),
            Err(KernelError::CollectionGenerationChanged(collection))
        );
        assert!(flash.operations().is_empty());
        assert_eq!(catalog.entry(collection).unwrap().generation, 4);
    }

    #[test]
    fn startup_replay_restores_atomic_multi_collection_commit() {
        const LARGE_REGION: usize = 512;
        let mut flash = TraceFlash::<LARGE_REGION, REGION_COUNT, MAX_EVENTS>::new(
            0xff,
            8,
            8,
            LARGE_REGION,
            LARGE_REGION,
        )
        .unwrap();
        let mut memory = V3Memory::<LARGE_REGION, REGION_COUNT>::new();
        let mut store = V3Store::format(&mut flash, config(), &mut memory).unwrap();
        let first = crate::CollectionId::new(41);
        let second = crate::CollectionId::new(42);
        let mut catalog = super::super::CollectionCatalog::<2>::new();
        for (collection_id, collection_type) in [(first, 1), (second, 2)] {
            store
                .create_collection(
                    &mut flash,
                    super::super::CatalogEntry {
                        collection_id,
                        collection_type,
                        generation: 0,
                    },
                    &mut catalog,
                    &mut memory,
                )
                .unwrap();
        }
        let mut transaction = super::super::TransactionMemory::<2>::new();
        transaction.begin(super::super::TransactionId(12)).unwrap();
        transaction.enroll(first, 0).unwrap();
        transaction.enroll(second, 0).unwrap();
        transaction.stage_write(first).unwrap();
        transaction.stage_write(second).unwrap();
        store
            .commit_transaction(&mut flash, &mut transaction, &mut catalog, &mut memory)
            .unwrap();

        flash.crash();
        let mut reopen_memory = V3Memory::<LARGE_REGION, REGION_COUNT>::new();
        let reopened = V3Store::open(&mut flash, &mut reopen_memory).unwrap();
        let mut replayed = super::super::CollectionCatalog::<2>::new();
        reopened
            .replay_catalog(&mut flash, &mut replayed, &mut reopen_memory)
            .unwrap();
        assert_eq!(replayed.entry(first).unwrap().generation, 1);
        assert_eq!(replayed.entry(second).unwrap().generation, 1);
    }
}
