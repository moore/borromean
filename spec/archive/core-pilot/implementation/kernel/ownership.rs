// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use crate::{kernel::KernelError, CollectionId};

/// Stable identifier for one durable operation plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationId(pub u64);

/// Purpose recorded before a region can be published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionPurpose {
    MainWal,
    TransactionLog,
    FreeSpaceBasis,
    CollectionData { collection_type: u16 },
}

/// Owner of a published region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionOwner {
    System(RegionPurpose),
    Collection {
        collection_id: CollectionId,
        collection_type: u16,
    },
}

/// Pure lifecycle classification for one physical region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionLifecycle {
    ErasedPrepared,
    Reserved {
        purpose: RegionPurpose,
        operation: OperationId,
    },
    Published(RegionOwner),
    Dirty,
}

/// Linear capability returned by a successful reservation.
#[derive(Debug, PartialEq, Eq)]
pub struct ReservationToken {
    region_index: u32,
    purpose: RegionPurpose,
    operation: OperationId,
}

impl ReservationToken {
    pub const fn region_index(&self) -> u32 {
        self.region_index
    }

    pub const fn purpose(&self) -> RegionPurpose {
        self.purpose
    }

    pub const fn operation(&self) -> OperationId {
        self.operation
    }
}

/// Caller-owned pure ownership table.
pub struct OwnershipTable<const REGION_COUNT: usize> {
    states: [RegionLifecycle; REGION_COUNT],
}

impl<const REGION_COUNT: usize> OwnershipTable<REGION_COUNT> {
    pub const fn new() -> Self {
        Self {
            states: [RegionLifecycle::ErasedPrepared; REGION_COUNT],
        }
    }

    pub fn reset(&mut self) {
        self.states.fill(RegionLifecycle::ErasedPrepared);
    }

    pub fn state(
        &self,
        region_index: u32,
    ) -> Result<RegionLifecycle, KernelError<core::convert::Infallible>> {
        let index = self.index(region_index)?;
        Ok(self.states[index])
    }

    pub fn reserve(
        &mut self,
        region_index: u32,
        purpose: RegionPurpose,
        operation: OperationId,
    ) -> Result<ReservationToken, KernelError<core::convert::Infallible>> {
        let index = self.index(region_index)?;
        if self.states[index] != RegionLifecycle::ErasedPrepared {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        self.states[index] = RegionLifecycle::Reserved { purpose, operation };
        Ok(ReservationToken {
            region_index,
            purpose,
            operation,
        })
    }

    pub fn publish(
        &mut self,
        token: ReservationToken,
        owner: RegionOwner,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        let index = self.index(token.region_index)?;
        let expected = RegionLifecycle::Reserved {
            purpose: token.purpose,
            operation: token.operation,
        };
        if self.states[index] != expected || !owner_matches_purpose(owner, token.purpose) {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        self.states[index] = RegionLifecycle::Published(owner);
        Ok(())
    }

    pub fn release(
        &mut self,
        region_index: u32,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        let index = self.index(region_index)?;
        if !matches!(self.states[index], RegionLifecycle::Published(_)) {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        self.states[index] = RegionLifecycle::Dirty;
        Ok(())
    }

    /// Applies the readiness publication after the caller has erased a dirty region.
    pub fn publish_erased_ready(
        &mut self,
        region_index: u32,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        let index = self.index(region_index)?;
        if self.states[index] != RegionLifecycle::Dirty {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        self.states[index] = RegionLifecycle::ErasedPrepared;
        Ok(())
    }

    fn index(&self, region_index: u32) -> Result<usize, KernelError<core::convert::Infallible>> {
        let index = usize::try_from(region_index)
            .map_err(|_| KernelError::InvalidRegionIndex(region_index))?;
        if index >= REGION_COUNT {
            return Err(KernelError::InvalidRegionIndex(region_index));
        }
        Ok(index)
    }
}

impl<const REGION_COUNT: usize> Default for OwnershipTable<REGION_COUNT> {
    fn default() -> Self {
        Self::new()
    }
}

fn owner_matches_purpose(owner: RegionOwner, purpose: RegionPurpose) -> bool {
    match (owner, purpose) {
        (RegionOwner::System(actual), expected) => actual == expected,
        (
            RegionOwner::Collection {
                collection_type: actual,
                ..
            },
            RegionPurpose::CollectionData {
                collection_type: expected,
            },
        ) => actual == expected,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_lifecycle_requires_linear_order_and_matching_purpose() {
        let mut table = OwnershipTable::<2>::new();
        let token = table
            .reserve(1, RegionPurpose::MainWal, OperationId(9))
            .unwrap();
        assert_eq!(token.region_index(), 1);
        assert_eq!(
            table.publish(token, RegionOwner::System(RegionPurpose::TransactionLog)),
            Err(KernelError::InvalidOwnershipTransition)
        );

        // The failed publication consumed the token but did not change ownership.
        assert!(matches!(
            table.state(1).unwrap(),
            RegionLifecycle::Reserved { .. }
        ));
    }

    #[test]
    fn published_region_must_become_dirty_before_ready() {
        let mut table = OwnershipTable::<1>::new();
        let token = table
            .reserve(0, RegionPurpose::FreeSpaceBasis, OperationId(1))
            .unwrap();
        table
            .publish(token, RegionOwner::System(RegionPurpose::FreeSpaceBasis))
            .unwrap();
        assert_eq!(
            table.publish_erased_ready(0),
            Err(KernelError::InvalidOwnershipTransition)
        );
        table.release(0).unwrap();
        table.publish_erased_ready(0).unwrap();
        assert_eq!(table.state(0).unwrap(), RegionLifecycle::ErasedPrepared);
    }
}
