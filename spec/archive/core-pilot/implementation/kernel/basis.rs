// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use heapless::Vec;

use super::{BasisInterval, KernelError, RegionPurpose, ReservationToken};

/// One published immutable free-space basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedBasis {
    pub root_region: u32,
    pub segment_count: u32,
    pub interval: BasisInterval,
    pub ready_position: super::LogicalQueuePosition,
    pub generation: u64,
}

struct BasisTarget {
    token: ReservationToken,
    durable: bool,
}

/// Pure construction state for a copy-on-write basis replacement.
pub struct BasisReplacement<const MAX_SEGMENTS: usize> {
    previous: PublishedBasis,
    interval: BasisInterval,
    ready_position: super::LogicalQueuePosition,
    targets: Vec<BasisTarget, MAX_SEGMENTS>,
}

impl<const MAX_SEGMENTS: usize> BasisReplacement<MAX_SEGMENTS> {
    pub const fn new(
        previous: PublishedBasis,
        interval: BasisInterval,
        ready_position: super::LogicalQueuePosition,
    ) -> Self {
        Self {
            previous,
            interval,
            ready_position,
            targets: Vec::new(),
        }
    }

    pub const fn previous(&self) -> PublishedBasis {
        self.previous
    }

    pub fn reserve_segment(
        &mut self,
        token: ReservationToken,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        if token.purpose() != RegionPurpose::FreeSpaceBasis {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        self.targets
            .push(BasisTarget {
                token,
                durable: false,
            })
            .map_err(|_| KernelError::TransactionCapacityExceeded)
    }

    pub fn mark_segment_durable(
        &mut self,
        region_index: u32,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        let target = self
            .targets
            .iter_mut()
            .find(|target| target.token.region_index() == region_index)
            .ok_or(KernelError::InvalidOwnershipTransition)?;
        target.durable = true;
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        !self.targets.is_empty() && self.targets.iter().all(|target| target.durable)
    }

    /// Produces the publication description without modifying the previous
    /// basis. The caller publishes this root durably before consuming `self`.
    pub fn publication(&self) -> Result<PublishedBasis, KernelError<core::convert::Infallible>> {
        if !self.is_complete() {
            return Err(KernelError::MaintenanceRequired(
                super::MaintenanceFlags::CHECKPOINT_FREE_SPACE,
            ));
        }
        let root_region = self.targets[0].token.region_index();
        Ok(PublishedBasis {
            root_region,
            segment_count: u32::try_from(self.targets.len())
                .map_err(|_| KernelError::CorruptFormat)?,
            interval: self.interval,
            ready_position: self.ready_position,
            generation: self
                .previous
                .generation
                .checked_add(1)
                .ok_or(KernelError::CorruptFormat)?,
        })
    }

    /// Returns reserved tokens after the publication record is durable so the
    /// ownership table can publish each segment.
    pub fn into_tokens(self) -> Vec<ReservationToken, MAX_SEGMENTS> {
        self.targets
            .into_iter()
            .fold(Vec::new(), |mut tokens, target| {
                let _ = tokens.push(target.token);
                tokens
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{OperationId, OwnershipTable};

    #[test]
    fn replacement_cannot_publish_until_every_new_segment_is_durable() {
        let old = PublishedBasis {
            root_region: 1,
            segment_count: 1,
            interval: BasisInterval {
                start: super::super::LogicalQueuePosition(0),
                end: super::super::LogicalQueuePosition(4),
            },
            ready_position: super::super::LogicalQueuePosition(4),
            generation: 2,
        };
        let mut ownership = OwnershipTable::<4>::new();
        let token = ownership
            .reserve(3, RegionPurpose::FreeSpaceBasis, OperationId(8))
            .unwrap();
        let mut replacement = BasisReplacement::<2>::new(
            old,
            BasisInterval {
                start: super::super::LogicalQueuePosition(1),
                end: super::super::LogicalQueuePosition(4),
            },
            super::super::LogicalQueuePosition(4),
        );
        replacement.reserve_segment(token).unwrap();
        assert!(replacement.publication().is_err());
        assert_eq!(replacement.previous(), old);

        replacement.mark_segment_durable(3).unwrap();
        let publication = replacement.publication().unwrap();
        assert_eq!(publication.root_region, 3);
        assert_eq!(publication.generation, 3);
        assert_eq!(replacement.previous(), old);
    }
}
