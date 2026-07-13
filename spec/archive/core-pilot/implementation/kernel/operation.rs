// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use crate::CollectionId;

/// Compact reasons for caller-invoked maintenance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceFlags(u16);

impl MaintenanceFlags {
    pub const NONE: Self = Self(0);
    pub const ERASE_DIRTY: Self = Self(1 << 0);
    pub const PREPARE_WAL_SPARE: Self = Self(1 << 1);
    pub const CHECKPOINT_FREE_SPACE: Self = Self(1 << 2);
    pub const RECLAIM_WAL: Self = Self(1 << 3);
    pub const FINISH_TRANSACTION: Self = Self(1 << 4);
    pub const FLUSH_COLLECTION: Self = Self(1 << 5);

    /// Returns the union of two pressure sets.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns whether `other` is present.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns whether no maintenance reason is present.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// Logical result paired with maintenance pressure observed after success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationResult<T> {
    pub value: T,
    pub maintenance: MaintenanceFlags,
}

impl<T> OperationResult<T> {
    pub const fn new(value: T, maintenance: MaintenanceFlags) -> Self {
        Self { value, maintenance }
    }
}

/// One explicitly requested bounded maintenance action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceTask {
    EraseDirty,
    PrepareWalSpare,
    BuildFreeSpaceBasis,
    PublishFreeSpaceBasis,
    ReclaimWal,
    FinishTransaction,
}

/// Result of one bounded maintenance step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceOutcome {
    pub progressed: bool,
    pub remaining: MaintenanceFlags,
}

/// V3 kernel failures that are independent of collection payload semantics.
#[derive(Debug, PartialEq, Eq)]
pub enum KernelError<E> {
    Device(E),
    InvalidGeometry(GeometryFailure),
    Unformatted,
    CorruptFormat,
    UnsupportedStorageVersion(u32),
    BufferTooSmall { needed: usize, available: usize },
    InsufficientRegions,
    InvalidAlignment,
    DuplicateWalSequence(u64),
    InvalidRegionIndex(u32),
    InvalidOwnershipTransition,
    MaintenanceRequired(MaintenanceFlags),
    CollectionWriteLocked(CollectionId),
    DuplicateCollection(CollectionId),
    UnknownCollection(CollectionId),
    CollectionGenerationChanged(CollectionId),
    TransactionAlreadyOpen,
    TransactionNotOpen,
    TransactionNotEnrolled(CollectionId),
    TransactionCapacityExceeded,
    IoBudgetExceeded,
}

/// Geometry error stored without tying the public error to a module path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeometryFailure(pub super::GeometryError);

impl<E> From<super::GeometryError> for KernelError<E> {
    fn from(error: super::GeometryError) -> Self {
        Self::InvalidGeometry(GeometryFailure(error))
    }
}

impl KernelError<core::convert::Infallible> {
    /// Changes the unreachable device-error parameter for a pure transition.
    pub(crate) fn cast<E>(self) -> KernelError<E> {
        match self {
            Self::Device(never) => match never {},
            Self::InvalidGeometry(error) => KernelError::InvalidGeometry(error),
            Self::Unformatted => KernelError::Unformatted,
            Self::CorruptFormat => KernelError::CorruptFormat,
            Self::UnsupportedStorageVersion(version) => {
                KernelError::UnsupportedStorageVersion(version)
            }
            Self::BufferTooSmall { needed, available } => {
                KernelError::BufferTooSmall { needed, available }
            }
            Self::InsufficientRegions => KernelError::InsufficientRegions,
            Self::InvalidAlignment => KernelError::InvalidAlignment,
            Self::DuplicateWalSequence(sequence) => KernelError::DuplicateWalSequence(sequence),
            Self::InvalidRegionIndex(region) => KernelError::InvalidRegionIndex(region),
            Self::InvalidOwnershipTransition => KernelError::InvalidOwnershipTransition,
            Self::MaintenanceRequired(flags) => KernelError::MaintenanceRequired(flags),
            Self::CollectionWriteLocked(collection) => {
                KernelError::CollectionWriteLocked(collection)
            }
            Self::DuplicateCollection(collection) => KernelError::DuplicateCollection(collection),
            Self::UnknownCollection(collection) => KernelError::UnknownCollection(collection),
            Self::CollectionGenerationChanged(collection) => {
                KernelError::CollectionGenerationChanged(collection)
            }
            Self::TransactionAlreadyOpen => KernelError::TransactionAlreadyOpen,
            Self::TransactionNotOpen => KernelError::TransactionNotOpen,
            Self::TransactionNotEnrolled(collection) => {
                KernelError::TransactionNotEnrolled(collection)
            }
            Self::TransactionCapacityExceeded => KernelError::TransactionCapacityExceeded,
            Self::IoBudgetExceeded => KernelError::IoBudgetExceeded,
        }
    }
}

/// Primitive I/O counts used for structural budget assertions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoCounts {
    pub metadata_reads: u32,
    pub metadata_programs: u32,
    pub region_reads: u32,
    pub region_programs: u32,
    pub erases: u32,
    pub syncs: u32,
    pub bytes_read: u64,
    pub bytes_programmed: u64,
}

impl IoCounts {
    /// Returns the saturating difference between two snapshots.
    pub fn since(self, earlier: Self) -> Self {
        Self {
            metadata_reads: self.metadata_reads.saturating_sub(earlier.metadata_reads),
            metadata_programs: self
                .metadata_programs
                .saturating_sub(earlier.metadata_programs),
            region_reads: self.region_reads.saturating_sub(earlier.region_reads),
            region_programs: self.region_programs.saturating_sub(earlier.region_programs),
            erases: self.erases.saturating_sub(earlier.erases),
            syncs: self.syncs.saturating_sub(earlier.syncs),
            bytes_read: self.bytes_read.saturating_sub(earlier.bytes_read),
            bytes_programmed: self
                .bytes_programmed
                .saturating_sub(earlier.bytes_programmed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_flags_form_a_compact_set() {
        let pressure = MaintenanceFlags::ERASE_DIRTY.union(MaintenanceFlags::CHECKPOINT_FREE_SPACE);
        assert!(pressure.contains(MaintenanceFlags::ERASE_DIRTY));
        assert!(pressure.contains(MaintenanceFlags::CHECKPOINT_FREE_SPACE));
        assert!(!pressure.contains(MaintenanceFlags::RECLAIM_WAL));
    }
}
