// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use heapless::Vec;

use crate::CollectionId;

use super::{KernelError, MaintenanceFlags, OperationResult};

/// Stable identity of one active v3 transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionId(pub u64);

/// Per-collection private view tracked by the active transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnrolledCollection {
    pub collection_id: CollectionId,
    pub committed_generation: u64,
    pub private_generation: u64,
}

/// Caller-owned state for the single active multi-collection transaction.
pub struct TransactionMemory<const MAX_COLLECTIONS: usize> {
    active: Option<TransactionId>,
    enrolled: Vec<EnrolledCollection, MAX_COLLECTIONS>,
}

impl<const MAX_COLLECTIONS: usize> TransactionMemory<MAX_COLLECTIONS> {
    pub const fn new() -> Self {
        Self {
            active: None,
            enrolled: Vec::new(),
        }
    }

    pub fn begin(
        &mut self,
        transaction_id: TransactionId,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        if self.active.is_some() {
            return Err(KernelError::TransactionAlreadyOpen);
        }
        self.active = Some(transaction_id);
        self.enrolled.clear();
        Ok(())
    }

    pub const fn active(&self) -> Option<TransactionId> {
        self.active
    }

    pub fn enroll(
        &mut self,
        collection_id: CollectionId,
        committed_generation: u64,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        if self.active.is_none() {
            return Err(KernelError::TransactionNotOpen);
        }
        if self
            .enrolled
            .iter()
            .any(|entry| entry.collection_id == collection_id)
        {
            return Ok(());
        }
        self.enrolled
            .push(EnrolledCollection {
                collection_id,
                committed_generation,
                private_generation: committed_generation,
            })
            .map_err(|_| KernelError::TransactionCapacityExceeded)
    }

    pub fn ordinary_read_generation(
        &self,
        _collection_id: CollectionId,
        committed_generation: u64,
    ) -> u64 {
        committed_generation
    }

    pub fn transaction_read_generation(
        &self,
        collection_id: CollectionId,
    ) -> Result<u64, KernelError<core::convert::Infallible>> {
        self.enrolled(collection_id)
            .map(|entry| entry.private_generation)
            .ok_or(KernelError::TransactionNotEnrolled(collection_id))
    }

    pub fn require_ordinary_write(
        &self,
        collection_id: CollectionId,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        if self.enrolled(collection_id).is_some() {
            return Err(KernelError::CollectionWriteLocked(collection_id));
        }
        Ok(())
    }

    pub fn stage_write(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<OperationResult<u64>, KernelError<core::convert::Infallible>> {
        let entry = self
            .enrolled
            .iter_mut()
            .find(|entry| entry.collection_id == collection_id)
            .ok_or(KernelError::TransactionNotEnrolled(collection_id))?;
        entry.private_generation = entry
            .private_generation
            .checked_add(1)
            .ok_or(KernelError::CorruptFormat)?;
        Ok(OperationResult::new(
            entry.private_generation,
            MaintenanceFlags::NONE,
        ))
    }

    /// Returns the atomic collection-generation changes to encode in one commit.
    pub fn prepare_commit(
        &self,
    ) -> Result<&[EnrolledCollection], KernelError<core::convert::Infallible>> {
        if self.active.is_none() {
            return Err(KernelError::TransactionNotOpen);
        }
        Ok(self.enrolled.as_slice())
    }

    /// Clears private state after the durable commit was applied to every collection.
    pub fn apply_commit(&mut self) -> Result<(), KernelError<core::convert::Infallible>> {
        if self.active.is_none() {
            return Err(KernelError::TransactionNotOpen);
        }
        self.active = None;
        self.enrolled.clear();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), KernelError<core::convert::Infallible>> {
        self.apply_commit()
    }

    fn enrolled(&self, collection_id: CollectionId) -> Option<&EnrolledCollection> {
        self.enrolled
            .iter()
            .find(|entry| entry.collection_id == collection_id)
    }
}

impl<const MAX_COLLECTIONS: usize> Default for TransactionMemory<MAX_COLLECTIONS> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrollment_allows_committed_reads_but_excludes_ordinary_writes() {
        let collection = CollectionId::new(7);
        let unrelated = CollectionId::new(8);
        let mut transaction = TransactionMemory::<2>::new();
        transaction.begin(TransactionId(1)).unwrap();
        transaction.enroll(collection, 4).unwrap();
        transaction.stage_write(collection).unwrap();

        assert_eq!(transaction.ordinary_read_generation(collection, 4), 4);
        assert_eq!(transaction.transaction_read_generation(collection), Ok(5));
        assert_eq!(
            transaction.require_ordinary_write(collection),
            Err(KernelError::CollectionWriteLocked(collection))
        );
        assert_eq!(transaction.require_ordinary_write(unrelated), Ok(()));
    }

    #[test]
    fn rollback_discards_private_generations() {
        let collection = CollectionId::new(3);
        let mut transaction = TransactionMemory::<1>::new();
        transaction.begin(TransactionId(9)).unwrap();
        transaction.enroll(collection, 11).unwrap();
        transaction.stage_write(collection).unwrap();
        transaction.rollback().unwrap();
        assert_eq!(transaction.active(), None);
        assert_eq!(
            transaction.transaction_read_generation(collection),
            Err(KernelError::TransactionNotEnrolled(collection))
        );
    }
}
