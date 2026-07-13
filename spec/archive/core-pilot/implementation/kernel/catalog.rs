// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use heapless::Vec;

use crate::CollectionId;

use super::{EnrolledCollection, KernelError};

/// Collection metadata owned by the generic storage core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogEntry {
    pub collection_id: CollectionId,
    pub collection_type: u16,
    pub generation: u64,
}

/// Bounded collection catalog. Payload semantics remain in typed adapters.
pub struct CollectionCatalog<const MAX_COLLECTIONS: usize> {
    entries: Vec<CatalogEntry, MAX_COLLECTIONS>,
}

impl<const MAX_COLLECTIONS: usize> CollectionCatalog<MAX_COLLECTIONS> {
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn insert(
        &mut self,
        entry: CatalogEntry,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        self.validate_insert(entry.collection_id)?;
        self.entries
            .push(entry)
            .map_err(|_| KernelError::TransactionCapacityExceeded)
    }

    pub fn validate_insert(
        &self,
        collection_id: CollectionId,
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        if self
            .entries
            .iter()
            .any(|current| current.collection_id == collection_id)
        {
            return Err(KernelError::DuplicateCollection(collection_id));
        }
        if self.entries.is_full() {
            return Err(KernelError::TransactionCapacityExceeded);
        }
        Ok(())
    }

    pub fn entry(&self, collection_id: CollectionId) -> Option<CatalogEntry> {
        self.entries
            .iter()
            .copied()
            .find(|entry| entry.collection_id == collection_id)
    }

    pub fn validate_commit(
        &self,
        changes: &[EnrolledCollection],
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        for change in changes {
            let current = self
                .entry(change.collection_id)
                .ok_or(KernelError::UnknownCollection(change.collection_id))?;
            if current.generation != change.committed_generation {
                return Err(KernelError::CollectionGenerationChanged(
                    change.collection_id,
                ));
            }
        }
        Ok(())
    }

    /// Applies all changes only after validating the complete set.
    pub fn apply_commit(
        &mut self,
        changes: &[EnrolledCollection],
    ) -> Result<(), KernelError<core::convert::Infallible>> {
        self.validate_commit(changes)?;
        for change in changes {
            let entry = self
                .entries
                .iter_mut()
                .find(|entry| entry.collection_id == change.collection_id)
                .ok_or(KernelError::UnknownCollection(change.collection_id))?;
            entry.generation = change.private_generation;
        }
        Ok(())
    }

    pub fn entries(&self) -> &[CatalogEntry] {
        self.entries.as_slice()
    }
}

impl<const MAX_COLLECTIONS: usize> Default for CollectionCatalog<MAX_COLLECTIONS> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_apply_validates_every_generation_before_changing_any() {
        let mut catalog = CollectionCatalog::<2>::new();
        catalog
            .insert(CatalogEntry {
                collection_id: CollectionId::new(1),
                collection_type: 10,
                generation: 4,
            })
            .unwrap();
        catalog
            .insert(CatalogEntry {
                collection_id: CollectionId::new(2),
                collection_type: 11,
                generation: 8,
            })
            .unwrap();
        let changes = [
            EnrolledCollection {
                collection_id: CollectionId::new(1),
                committed_generation: 4,
                private_generation: 5,
            },
            EnrolledCollection {
                collection_id: CollectionId::new(2),
                committed_generation: 7,
                private_generation: 9,
            },
        ];
        assert_eq!(
            catalog.apply_commit(&changes),
            Err(KernelError::CollectionGenerationChanged(CollectionId::new(
                2
            )))
        );
        assert_eq!(catalog.entry(CollectionId::new(1)).unwrap().generation, 4);
    }
}
