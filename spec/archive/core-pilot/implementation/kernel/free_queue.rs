// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use heapless::Vec;

use super::{KernelError, MaintenanceFlags, OperationResult};

/// Monotonic logical address of a free-space entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogicalQueuePosition(pub u64);

/// Half-open logical interval represented by an immutable basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BasisInterval {
    pub start: LogicalQueuePosition,
    pub end: LogicalQueuePosition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    Prepared,
    Dirty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueueEntry {
    region_index: u32,
    state: EntryState,
}

/// Bounded post-basis queue frontier.
pub struct FreeQueue<const CAPACITY: usize> {
    basis: BasisInterval,
    base_position: LogicalQueuePosition,
    entries: Vec<QueueEntry, CAPACITY>,
    allocation_index: usize,
    ready_index: usize,
}

impl<const CAPACITY: usize> FreeQueue<CAPACITY> {
    pub const fn new(position: LogicalQueuePosition) -> Self {
        Self {
            basis: BasisInterval {
                start: position,
                end: position,
            },
            base_position: position,
            entries: Vec::new(),
            allocation_index: 0,
            ready_index: 0,
        }
    }

    pub const fn basis_interval(&self) -> BasisInterval {
        self.basis
    }

    pub fn reset(&mut self, position: LogicalQueuePosition) {
        self.basis = BasisInterval {
            start: position,
            end: position,
        };
        self.base_position = position;
        self.entries.clear();
        self.allocation_index = 0;
        self.ready_index = 0;
    }

    pub fn next_prepared(&self) -> Option<u32> {
        (self.allocation_index < self.ready_index)
            .then(|| self.entries[self.allocation_index].region_index)
    }

    pub fn next_dirty(&self) -> Option<u32> {
        self.entries
            .get(self.ready_index)
            .filter(|entry| entry.state == EntryState::Dirty)
            .map(|entry| entry.region_index)
    }

    pub fn unconsumed_len(&self) -> usize {
        self.entries.len().saturating_sub(self.allocation_index)
    }

    pub fn has_append_capacity(&self) -> bool {
        self.entries.len() < CAPACITY
    }

    pub fn unconsumed_entry(&self, index: usize) -> Option<(u32, bool)> {
        let absolute = self.allocation_index.checked_add(index)?;
        self.entries
            .get(absolute)
            .map(|entry| (entry.region_index, absolute < self.ready_index))
    }

    /// Makes the current unconsumed interval the immutable basis represented
    /// by this runtime queue and discards consumed history.
    pub fn install_current_basis(
        &mut self,
    ) -> Result<BasisInterval, KernelError<core::convert::Infallible>> {
        let consumed = self.allocation_index;
        for destination in 0..self.unconsumed_len() {
            self.entries[destination] = self.entries[consumed + destination];
        }
        self.entries.truncate(self.unconsumed_len());
        self.ready_index = self.ready_index.saturating_sub(consumed);
        self.allocation_index = 0;
        self.base_position = LogicalQueuePosition(
            self.base_position
                .0
                .checked_add(
                    u64::try_from(consumed).map_err(|_| KernelError::InvalidOwnershipTransition)?,
                )
                .ok_or(KernelError::InvalidOwnershipTransition)?,
        );
        let end = self
            .base_position
            .0
            .checked_add(
                u64::try_from(self.entries.len())
                    .map_err(|_| KernelError::InvalidOwnershipTransition)?,
            )
            .ok_or(KernelError::InvalidOwnershipTransition)?;
        self.basis = BasisInterval {
            start: self.base_position,
            end: LogicalQueuePosition(end),
        };
        Ok(self.basis)
    }

    pub fn apply_allocate(
        &mut self,
        expected_region: u32,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        if self.next_prepared() != Some(expected_region) {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        self.allocation_index += 1;
        Ok(())
    }

    pub fn append_prepared(
        &mut self,
        region_index: u32,
    ) -> Result<LogicalQueuePosition, KernelError<core::convert::Infallible>> {
        if self.ready_index != self.entries.len() {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        let position = self.append_position()?;
        self.entries
            .push(QueueEntry {
                region_index,
                state: EntryState::Prepared,
            })
            .map_err(|_| {
                KernelError::MaintenanceRequired(MaintenanceFlags::CHECKPOINT_FREE_SPACE)
            })?;
        self.ready_index += 1;
        Ok(position)
    }

    pub fn allocate(
        &mut self,
    ) -> Result<OperationResult<u32>, KernelError<core::convert::Infallible>> {
        if self.allocation_index >= self.ready_index {
            return Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::ERASE_DIRTY,
            ));
        }
        let entry = self.entries[self.allocation_index];
        if entry.state != EntryState::Prepared {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        self.apply_allocate(entry.region_index)?;
        let remaining = self.ready_index - self.allocation_index;
        let maintenance = if remaining <= 1 {
            MaintenanceFlags::ERASE_DIRTY
        } else {
            MaintenanceFlags::NONE
        };
        Ok(OperationResult::new(entry.region_index, maintenance))
    }

    pub fn append_dirty(
        &mut self,
        region_index: u32,
    ) -> Result<LogicalQueuePosition, KernelError<core::convert::Infallible>> {
        let position = self.append_position()?;
        self.entries
            .push(QueueEntry {
                region_index,
                state: EntryState::Dirty,
            })
            .map_err(|_| {
                KernelError::MaintenanceRequired(MaintenanceFlags::CHECKPOINT_FREE_SPACE)
            })?;
        Ok(position)
    }

    /// Publishes the next dirty queue entry after its physical region was erased.
    pub fn publish_next_erased(
        &mut self,
        region_index: u32,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        let entry = self
            .entries
            .get_mut(self.ready_index)
            .ok_or(KernelError::InvalidOwnershipTransition)?;
        if entry.state != EntryState::Dirty || entry.region_index != region_index {
            return Err(KernelError::InvalidOwnershipTransition);
        }
        entry.state = EntryState::Prepared;
        self.ready_index += 1;
        Ok(())
    }

    pub fn allocation_position(&self) -> LogicalQueuePosition {
        LogicalQueuePosition(self.base_position.0 + self.allocation_index as u64)
    }

    pub fn ready_position(&self) -> LogicalQueuePosition {
        LogicalQueuePosition(self.base_position.0 + self.ready_index as u64)
    }

    pub fn append_position(
        &self,
    ) -> Result<LogicalQueuePosition, KernelError<core::convert::Infallible>> {
        let offset = u64::try_from(self.entries.len())
            .map_err(|_| KernelError::InvalidOwnershipTransition)?;
        self.base_position
            .0
            .checked_add(offset)
            .map(LogicalQueuePosition)
            .ok_or(KernelError::InvalidOwnershipTransition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_is_fifo_and_positions_never_wrap() {
        let mut queue = FreeQueue::<4>::new(LogicalQueuePosition(u64::from(u32::MAX)));
        assert_eq!(
            queue.append_prepared(7).unwrap(),
            LogicalQueuePosition(u64::from(u32::MAX))
        );
        assert_eq!(
            queue.append_prepared(8).unwrap(),
            LogicalQueuePosition(u64::from(u32::MAX) + 1)
        );
        assert_eq!(queue.allocate().unwrap().value, 7);
        assert_eq!(queue.allocate().unwrap().value, 8);
    }

    #[test]
    fn dirty_entry_is_unavailable_until_erased_publication() {
        let mut queue = FreeQueue::<2>::new(LogicalQueuePosition(0));
        queue.append_dirty(3).unwrap();
        assert_eq!(
            queue.allocate(),
            Err(KernelError::MaintenanceRequired(
                MaintenanceFlags::ERASE_DIRTY
            ))
        );
        queue.publish_next_erased(3).unwrap();
        assert_eq!(queue.allocate().unwrap().value, 3);
    }
}
