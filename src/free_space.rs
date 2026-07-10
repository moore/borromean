use heapless::Vec;

use crate::disk::FreeQueuePosition;

pub(crate) const MAX_FREE_QUEUE_ENTRIES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FreeSpaceError {
    QueueOverflow,
    InvalidCursor,
    InvalidPosition {
        expected: FreeQueuePosition,
        actual: FreeQueuePosition,
    },
    ReadyRangeEmpty,
    DirtyRangeTooSmall {
        available: u32,
        requested: u32,
    },
    RegionMismatch {
        expected: u32,
        actual: u32,
    },
}

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FreeSpaceState {
    metadata_region_index: u32,
    metadata_regions: Vec<u32, MAX_FREE_QUEUE_ENTRIES>,
    entries_per_region: u32,
    allocation_head: u32,
    ready_boundary: u32,
    append_tail: u32,
    queue: Vec<u32, MAX_FREE_QUEUE_ENTRIES>,
}

impl FreeSpaceState {
    pub(crate) fn empty() -> Self {
        Self {
            metadata_region_index: 0,
            metadata_regions: Vec::new(),
            entries_per_region: 0,
            allocation_head: 0,
            ready_boundary: 0,
            append_tail: 0,
            queue: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_ready_range(
        metadata_region_index: u32,
        first_region: u32,
        region_count: u32,
    ) -> Result<Self, FreeSpaceError> {
        let mut state = Self::empty();
        state.metadata_region_index = metadata_region_index;
        state
            .metadata_regions
            .push(metadata_region_index)
            .map_err(|_| FreeSpaceError::QueueOverflow)?;
        state.entries_per_region = u32::MAX;
        for region_index in first_region..region_count {
            state
                .queue
                .push(region_index)
                .map_err(|_| FreeSpaceError::QueueOverflow)?;
        }
        state.allocation_head = 0;
        state.ready_boundary =
            u32::try_from(state.queue.len()).map_err(|_| FreeSpaceError::QueueOverflow)?;
        state.append_tail = state.ready_boundary;
        Ok(state)
    }

    pub(crate) fn replace_from_parts(
        &mut self,
        metadata_region_index: u32,
        allocation_head: u32,
        ready_boundary: u32,
        append_tail: u32,
        entries: &[u32],
    ) -> Result<(), FreeSpaceError> {
        if allocation_head > ready_boundary || ready_boundary > append_tail {
            return Err(FreeSpaceError::InvalidCursor);
        }
        let len = u32::try_from(entries.len()).map_err(|_| FreeSpaceError::QueueOverflow)?;
        if append_tail > len {
            return Err(FreeSpaceError::InvalidCursor);
        }

        self.metadata_region_index = metadata_region_index;
        self.metadata_regions.clear();
        self.metadata_regions
            .push(metadata_region_index)
            .map_err(|_| FreeSpaceError::QueueOverflow)?;
        self.entries_per_region = u32::MAX;
        self.allocation_head = allocation_head;
        self.ready_boundary = ready_boundary;
        self.append_tail = append_tail;
        self.queue.clear();
        for entry in entries.iter().copied() {
            self.queue
                .push(entry)
                .map_err(|_| FreeSpaceError::QueueOverflow)?;
        }
        Ok(())
    }

    pub(crate) fn replace_from_position_parts(
        &mut self,
        metadata_regions: &[u32],
        entries_per_region: u32,
        allocation_head: FreeQueuePosition,
        ready_boundary: FreeQueuePosition,
        append_tail: FreeQueuePosition,
        entries: &[u32],
    ) -> Result<(), FreeSpaceError> {
        if metadata_regions.is_empty() || entries_per_region == 0 {
            return Err(FreeSpaceError::InvalidCursor);
        }
        let allocation_head =
            Self::index_for_position(metadata_regions, entries_per_region, allocation_head)?;
        let ready_boundary =
            Self::index_for_position(metadata_regions, entries_per_region, ready_boundary)?;
        let append_tail =
            Self::index_for_position(metadata_regions, entries_per_region, append_tail)?;
        self.replace_from_parts(
            metadata_regions[0],
            allocation_head,
            ready_boundary,
            append_tail,
            entries,
        )?;
        self.metadata_regions.clear();
        for region_index in metadata_regions.iter().copied() {
            self.metadata_regions
                .push(region_index)
                .map_err(|_| FreeSpaceError::QueueOverflow)?;
        }
        self.entries_per_region = entries_per_region;
        Ok(())
    }

    pub(crate) fn metadata_region_index(&self) -> u32 {
        self.metadata_region_index
    }

    pub(crate) fn metadata_regions(&self) -> &[u32] {
        self.metadata_regions.as_slice()
    }

    pub(crate) fn metadata_region_count(&self) -> usize {
        self.metadata_regions.len()
    }

    pub(crate) fn push_metadata_region(
        &mut self,
        region_index: u32,
        entries_per_region: u32,
    ) -> Result<(), FreeSpaceError> {
        if entries_per_region == 0 {
            return Err(FreeSpaceError::InvalidCursor);
        }
        if self.metadata_regions.is_empty() {
            self.metadata_region_index = region_index;
        }
        self.entries_per_region = entries_per_region;
        self.metadata_regions
            .push(region_index)
            .map_err(|_| FreeSpaceError::QueueOverflow)
    }

    pub(crate) fn allocation_head(&self) -> u32 {
        self.allocation_head
    }

    pub(crate) fn ready_boundary(&self) -> u32 {
        self.ready_boundary
    }

    pub(crate) fn append_tail(&self) -> u32 {
        self.append_tail
    }

    pub(crate) fn allocation_head_position(&self) -> FreeQueuePosition {
        self.position(self.allocation_head)
    }

    pub(crate) fn ready_boundary_position(&self) -> FreeQueuePosition {
        self.position(self.ready_boundary)
    }

    pub(crate) fn append_tail_position(&self) -> FreeQueuePosition {
        self.position(self.append_tail)
    }

    pub(crate) fn position_after_allocation(&self) -> Result<FreeQueuePosition, FreeSpaceError> {
        let next = self
            .allocation_head
            .checked_add(1)
            .ok_or(FreeSpaceError::InvalidCursor)?;
        Ok(self.position(next))
    }

    pub(crate) fn position_after_append(&self) -> Result<FreeQueuePosition, FreeSpaceError> {
        let next = self
            .append_tail
            .checked_add(1)
            .ok_or(FreeSpaceError::InvalidCursor)?;
        Ok(self.position(next))
    }

    pub(crate) fn position_after_erase(
        &self,
        count: u32,
    ) -> Result<FreeQueuePosition, FreeSpaceError> {
        let next = self
            .ready_boundary
            .checked_add(count)
            .ok_or(FreeSpaceError::InvalidCursor)?;
        Ok(self.position(next))
    }

    pub(crate) fn ready_count(&self) -> u32 {
        self.ready_boundary.saturating_sub(self.allocation_head)
    }

    pub(crate) fn dirty_count(&self) -> u32 {
        self.append_tail.saturating_sub(self.ready_boundary)
    }

    pub(crate) fn next_ready_region(&self) -> Result<u32, FreeSpaceError> {
        if self.allocation_head >= self.ready_boundary {
            return Err(FreeSpaceError::ReadyRangeEmpty);
        }
        self.queue
            .get(usize::try_from(self.allocation_head).map_err(|_| FreeSpaceError::InvalidCursor)?)
            .copied()
            .ok_or(FreeSpaceError::InvalidCursor)
    }

    pub(crate) fn apply_allocate(
        &mut self,
        region_index: u32,
        allocation_head_after: FreeQueuePosition,
    ) -> Result<(), FreeSpaceError> {
        let expected_region = self.next_ready_region()?;
        if expected_region != region_index {
            return Err(FreeSpaceError::RegionMismatch {
                expected: expected_region,
                actual: region_index,
            });
        }
        let expected_position = self.position_after_allocation()?;
        if allocation_head_after != expected_position {
            return Err(FreeSpaceError::InvalidPosition {
                expected: expected_position,
                actual: allocation_head_after,
            });
        }
        self.allocation_head = self
            .allocation_head
            .checked_add(1)
            .ok_or(FreeSpaceError::InvalidCursor)?;
        Ok(())
    }

    pub(crate) fn apply_free(
        &mut self,
        region_index: u32,
        append_tail_after: FreeQueuePosition,
    ) -> Result<(), FreeSpaceError> {
        let expected_position = self.position_after_append()?;
        if append_tail_after != expected_position {
            return Err(FreeSpaceError::InvalidPosition {
                expected: expected_position,
                actual: append_tail_after,
            });
        }
        self.queue
            .push(region_index)
            .map_err(|_| FreeSpaceError::QueueOverflow)?;
        self.append_tail = self
            .append_tail
            .checked_add(1)
            .ok_or(FreeSpaceError::InvalidCursor)?;
        Ok(())
    }

    pub(crate) fn apply_erase(
        &mut self,
        count: u32,
        ready_boundary_after: FreeQueuePosition,
    ) -> Result<(), FreeSpaceError> {
        if self.dirty_count() < count {
            return Err(FreeSpaceError::DirtyRangeTooSmall {
                available: self.dirty_count(),
                requested: count,
            });
        }
        let expected_position = self.position_after_erase(count)?;
        if ready_boundary_after != expected_position {
            return Err(FreeSpaceError::InvalidPosition {
                expected: expected_position,
                actual: ready_boundary_after,
            });
        }
        self.ready_boundary = self
            .ready_boundary
            .checked_add(count)
            .ok_or(FreeSpaceError::InvalidCursor)?;
        Ok(())
    }

    pub(crate) fn entries(&self) -> &[u32] {
        self.queue.as_slice()
    }

    pub(crate) fn contains_free_region(&self, region_index: u32) -> bool {
        let Ok(start) = usize::try_from(self.allocation_head) else {
            return false;
        };
        let Ok(end) = usize::try_from(self.append_tail) else {
            return false;
        };
        if start > end || end > self.queue.len() {
            return false;
        }
        self.queue[start..end].contains(&region_index)
    }

    fn position(&self, entry_index: u32) -> FreeQueuePosition {
        if !self.metadata_regions.is_empty() && self.entries_per_region != 0 {
            let segment = entry_index / self.entries_per_region;
            let segment = usize::try_from(segment)
                .unwrap_or(usize::MAX)
                .min(self.metadata_regions.len() - 1);
            let base = u32::try_from(segment)
                .ok()
                .and_then(|segment| segment.checked_mul(self.entries_per_region))
                .unwrap_or(0);
            return FreeQueuePosition {
                region_index: self.metadata_regions[segment],
                entry_index: entry_index.saturating_sub(base),
            };
        }
        FreeQueuePosition {
            region_index: self.metadata_region_index,
            entry_index,
        }
    }

    fn index_for_position(
        metadata_regions: &[u32],
        entries_per_region: u32,
        position: FreeQueuePosition,
    ) -> Result<u32, FreeSpaceError> {
        if entries_per_region == 0 {
            return Err(FreeSpaceError::InvalidCursor);
        }
        if position.entry_index > entries_per_region {
            return Err(FreeSpaceError::InvalidCursor);
        }
        let segment = metadata_regions
            .iter()
            .position(|region_index| *region_index == position.region_index)
            .ok_or(FreeSpaceError::InvalidCursor)?;
        let segment = u32::try_from(segment).map_err(|_| FreeSpaceError::InvalidCursor)?;
        segment
            .checked_mul(entries_per_region)
            .and_then(|base| base.checked_add(position.entry_index))
            .ok_or(FreeSpaceError::InvalidCursor)
    }
}
