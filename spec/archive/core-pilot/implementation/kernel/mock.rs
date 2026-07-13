// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use heapless::Vec;

use super::{DeviceGeometry, IoCounts, RawFlash};

/// Primitive operation emitted at the raw v3 device boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceOperation {
    ReadMetadata {
        offset: usize,
        len: usize,
    },
    ProgramMetadata {
        offset: usize,
        len: usize,
    },
    EraseMetadata,
    ReadRegion {
        region_index: u32,
        offset: usize,
        len: usize,
    },
    ProgramRegion {
        region_index: u32,
        offset: usize,
        len: usize,
    },
    EraseRegion {
        region_index: u32,
    },
    Sync,
}

/// One deterministic fault injected at a primitive operation number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureMode {
    Before {
        operation: u64,
    },
    TornProgram {
        operation: u64,
        programmed_bytes: usize,
    },
}

/// Media outcome selected when power is cut before a successful sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPersistence {
    /// Lose every unsynced program and erase.
    DiscardUnsynced,
    /// Persist the current working image, including a complete or torn program.
    PersistWorking,
}

/// Errors returned by [`TraceFlash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceFlashError {
    InvalidGeometry,
    InvalidRegion(u32),
    InvalidRange,
    NotErased,
    OperationLogFull,
    InjectedFailure { operation: u64 },
}

/// In-memory raw flash with separate working and durable images.
pub struct TraceFlash<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_EVENTS: usize>
{
    geometry: DeviceGeometry,
    metadata: [u8; REGION_SIZE],
    durable_metadata: [u8; REGION_SIZE],
    regions: [[u8; REGION_SIZE]; REGION_COUNT],
    durable_regions: [[u8; REGION_SIZE]; REGION_COUNT],
    operations: Vec<TraceOperation, MAX_EVENTS>,
    counts: IoCounts,
    operation_number: u64,
    failure: Option<FailureMode>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_EVENTS: usize>
    TraceFlash<REGION_SIZE, REGION_COUNT, MAX_EVENTS>
{
    pub fn new(
        erased_byte: u8,
        read_alignment: usize,
        program_alignment: usize,
        max_read_len: usize,
        max_program_len: usize,
    ) -> Result<Self, TraceFlashError> {
        let region_count =
            u32::try_from(REGION_COUNT).map_err(|_| TraceFlashError::InvalidGeometry)?;
        let geometry = DeviceGeometry {
            metadata_size: REGION_SIZE,
            region_size: REGION_SIZE,
            region_count,
            erased_byte,
            read_alignment,
            program_alignment,
            max_read_len,
            max_program_len,
        };
        geometry
            .validate()
            .map_err(|_| TraceFlashError::InvalidGeometry)?;
        Ok(Self {
            geometry,
            metadata: [erased_byte; REGION_SIZE],
            durable_metadata: [erased_byte; REGION_SIZE],
            regions: [[erased_byte; REGION_SIZE]; REGION_COUNT],
            durable_regions: [[erased_byte; REGION_SIZE]; REGION_COUNT],
            operations: Vec::new(),
            counts: IoCounts::default(),
            operation_number: 0,
            failure: None,
        })
    }

    pub fn operations(&self) -> &[TraceOperation] {
        self.operations.as_slice()
    }

    pub const fn counts(&self) -> IoCounts {
        self.counts
    }

    pub const fn operation_number(&self) -> u64 {
        self.operation_number
    }

    pub fn clear_trace(&mut self) {
        self.operations.clear();
        self.counts = IoCounts::default();
    }

    pub fn inject_failure(&mut self, failure: FailureMode) {
        self.failure = Some(failure);
    }

    pub fn clear_failure(&mut self) {
        self.failure = None;
    }

    /// Simulates loss of every operation not covered by a successful sync.
    pub fn crash(&mut self) {
        self.crash_with(CrashPersistence::DiscardUnsynced);
    }

    /// Simulates a power cut with an explicitly selected unsynced-media outcome.
    pub fn crash_with(&mut self, persistence: CrashPersistence) {
        match persistence {
            CrashPersistence::DiscardUnsynced => {
                self.metadata = self.durable_metadata;
                self.regions = self.durable_regions;
            }
            CrashPersistence::PersistWorking => {
                self.durable_metadata = self.metadata;
                self.durable_regions = self.regions;
            }
        }
        self.failure = None;
    }

    pub fn durable_metadata(&self) -> &[u8; REGION_SIZE] {
        &self.durable_metadata
    }

    pub fn durable_region(&self, region_index: u32) -> Result<&[u8; REGION_SIZE], TraceFlashError> {
        let index = self.region_index(region_index)?;
        Ok(&self.durable_regions[index])
    }

    fn begin(&mut self, operation: TraceOperation) -> Result<u64, TraceFlashError> {
        self.operation_number = self.operation_number.saturating_add(1);
        let number = self.operation_number;
        self.operations
            .push(operation)
            .map_err(|_| TraceFlashError::OperationLogFull)?;
        match operation {
            TraceOperation::ReadMetadata { len, .. } => {
                self.counts.metadata_reads = self.counts.metadata_reads.saturating_add(1);
                self.counts.bytes_read = self.counts.bytes_read.saturating_add(len as u64);
            }
            TraceOperation::ProgramMetadata { len, .. } => {
                self.counts.metadata_programs = self.counts.metadata_programs.saturating_add(1);
                self.counts.bytes_programmed =
                    self.counts.bytes_programmed.saturating_add(len as u64);
            }
            TraceOperation::ReadRegion { len, .. } => {
                self.counts.region_reads = self.counts.region_reads.saturating_add(1);
                self.counts.bytes_read = self.counts.bytes_read.saturating_add(len as u64);
            }
            TraceOperation::ProgramRegion { len, .. } => {
                self.counts.region_programs = self.counts.region_programs.saturating_add(1);
                self.counts.bytes_programmed =
                    self.counts.bytes_programmed.saturating_add(len as u64);
            }
            TraceOperation::EraseMetadata | TraceOperation::EraseRegion { .. } => {
                self.counts.erases = self.counts.erases.saturating_add(1);
            }
            TraceOperation::Sync => self.counts.syncs = self.counts.syncs.saturating_add(1),
        }
        if self.failure == Some(FailureMode::Before { operation: number }) {
            return Err(TraceFlashError::InjectedFailure { operation: number });
        }
        Ok(number)
    }

    fn torn_len(&self, operation: u64, requested: usize) -> Option<usize> {
        match self.failure {
            Some(FailureMode::TornProgram {
                operation: target,
                programmed_bytes,
            }) if target == operation => Some(programmed_bytes.min(requested)),
            _ => None,
        }
    }

    fn region_index(&self, region_index: u32) -> Result<usize, TraceFlashError> {
        let index = usize::try_from(region_index)
            .map_err(|_| TraceFlashError::InvalidRegion(region_index))?;
        if index >= REGION_COUNT {
            return Err(TraceFlashError::InvalidRegion(region_index));
        }
        Ok(index)
    }

    fn validate_read(&self, offset: usize, len: usize) -> Result<(), TraceFlashError> {
        if self.geometry.valid_read(offset, len) {
            Ok(())
        } else {
            Err(TraceFlashError::InvalidRange)
        }
    }

    fn validate_program(&self, offset: usize, len: usize) -> Result<(), TraceFlashError> {
        if self.geometry.valid_program(offset, len) {
            Ok(())
        } else {
            Err(TraceFlashError::InvalidRange)
        }
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_EVENTS: usize> RawFlash
    for TraceFlash<REGION_SIZE, REGION_COUNT, MAX_EVENTS>
{
    type Error = TraceFlashError;

    fn geometry(&self) -> DeviceGeometry {
        self.geometry
    }

    fn read_metadata<R>(
        &mut self,
        offset: usize,
        len: usize,
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Self::Error> {
        self.validate_read(offset, len)?;
        self.begin(TraceOperation::ReadMetadata { offset, len })?;
        Ok(read(&self.metadata[offset..offset + len]))
    }

    fn program_metadata(&mut self, offset: usize, bytes: &[u8]) -> Result<(), Self::Error> {
        self.validate_program(offset, bytes.len())?;
        let operation = self.begin(TraceOperation::ProgramMetadata {
            offset,
            len: bytes.len(),
        })?;
        let written = self.torn_len(operation, bytes.len()).unwrap_or(bytes.len());
        program_erased(
            &mut self.metadata[offset..offset + written],
            &bytes[..written],
            self.geometry.erased_byte,
        )?;
        if written != bytes.len() {
            return Err(TraceFlashError::InjectedFailure { operation });
        }
        Ok(())
    }

    fn erase_metadata(&mut self) -> Result<(), Self::Error> {
        self.begin(TraceOperation::EraseMetadata)?;
        self.metadata.fill(self.geometry.erased_byte);
        Ok(())
    }

    fn read_region<R>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Self::Error> {
        self.validate_read(offset, len)?;
        let index = self.region_index(region_index)?;
        self.begin(TraceOperation::ReadRegion {
            region_index,
            offset,
            len,
        })?;
        Ok(read(&self.regions[index][offset..offset + len]))
    }

    fn program_region(
        &mut self,
        region_index: u32,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), Self::Error> {
        self.validate_program(offset, bytes.len())?;
        let index = self.region_index(region_index)?;
        let operation = self.begin(TraceOperation::ProgramRegion {
            region_index,
            offset,
            len: bytes.len(),
        })?;
        let written = self.torn_len(operation, bytes.len()).unwrap_or(bytes.len());
        program_erased(
            &mut self.regions[index][offset..offset + written],
            &bytes[..written],
            self.geometry.erased_byte,
        )?;
        if written != bytes.len() {
            return Err(TraceFlashError::InjectedFailure { operation });
        }
        Ok(())
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), Self::Error> {
        let index = self.region_index(region_index)?;
        self.begin(TraceOperation::EraseRegion { region_index })?;
        self.regions[index].fill(self.geometry.erased_byte);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), Self::Error> {
        self.begin(TraceOperation::Sync)?;
        self.durable_metadata = self.metadata;
        self.durable_regions = self.regions;
        Ok(())
    }
}

fn program_erased(
    target: &mut [u8],
    source: &[u8],
    erased_byte: u8,
) -> Result<(), TraceFlashError> {
    if target.iter().any(|byte| *byte != erased_byte) {
        return Err(TraceFlashError::NotErased);
    }
    target.copy_from_slice(source);
    Ok(())
}
