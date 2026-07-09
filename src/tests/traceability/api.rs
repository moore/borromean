use super::*;

//= spec/implementation.md#architecture-requirements
//= type=test
//# `RING-IMPL-ARCH-002` The backing abstraction MUST be bound to
//# `Storage` during format or open, and normal public operations MUST use
//# that backing through `Storage` rather than accepting a separate backing
//# argument.
#[test]
fn requirement_storage_operations_use_bound_backing() {
    let mut flash = MockFlash::<1024, 10, 512>::new(0xff);
    let mut storage = Storage::<_, 1024, 10, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(11)).unwrap();
    storage
        .append_map_update::<u16, u16>(CollectionId(11), &MapUpdate::Set { key: 1, value: 10 })
        .unwrap();
    assert_eq!(storage.mode(), StorageMode::Idle);

    let backing = storage.into_backing();
    let reopened = Storage::<_, 1024, 10, 8>::open(backing, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(11));
}

//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-002` The public API MUST allow a caller to drive the
//# same storage engine from either blocking test shims or asynchronous
//# device adapters without changing Borromean correctness logic.
#[test]
fn requirement_blocking_and_future_entry_points_produce_equivalent_storage_state() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 5;
    let mut blocking_flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut blocking = Storage::<_, REGION_SIZE, REGION_COUNT, 8>::format(
        &mut blocking_flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    let mut future_flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut future_driven = super::super::poll_until_ready(
        Storage::<_, REGION_SIZE, REGION_COUNT, 8>::format_future(
            &mut future_flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        ),
        16,
    )
    .unwrap();

    blocking.create_map(CollectionId(61)).unwrap();
    super::super::poll_until_ready(future_driven.create_map_future(CollectionId(61)), 16).unwrap();

    blocking
        .append_map_update::<u16, u16>(CollectionId(61), &MapUpdate::Set { key: 7, value: 70 })
        .unwrap();
    super::super::poll_until_ready(
        future_driven.append_map_update_future::<u16, u16>(
            CollectionId(61),
            &MapUpdate::Set { key: 7, value: 70 },
        ),
        16,
    )
    .unwrap();

    drop(blocking);
    drop(future_driven);

    let mut reopened_blocking = Storage::<_, REGION_SIZE, REGION_COUNT, 8>::open(
        &mut blocking_flash,
        crate::test_storage_memory(),
    )
    .unwrap();
    let reopened_future = super::super::poll_until_ready(
        Storage::<_, REGION_SIZE, REGION_COUNT, 8>::open_future(
            &mut future_flash,
            crate::test_storage_memory(),
        ),
        16,
    )
    .unwrap();
    let mut reopened_future = reopened_future;

    assert_eq!(reopened_blocking.metadata(), reopened_future.metadata());
    assert_eq!(
        reopened_blocking.collections(),
        reopened_future.collections()
    );
    assert_eq!(
        reopened_blocking.ready_free_region(),
        reopened_future.ready_free_region()
    );
    assert_eq!(
        reopened_blocking.free_space_tail_region(),
        reopened_future.free_space_tail_region()
    );

    let mut blocking_map_buffer = [0u8; REGION_SIZE];
    let blocking_map = reopened_blocking
        .open_map::<u16, u16, 8>(
            CollectionId(61),
            &mut blocking_map_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    let mut future_map_buffer = [0u8; REGION_SIZE];
    let future_map = reopened_future
        .open_map::<u16, u16, 8>(
            CollectionId(61),
            &mut future_map_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(blocking_map.get_frontier(&7).unwrap(), Some(70));
    assert_eq!(future_map.get_frontier(&7).unwrap(), Some(70));
}

//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-005` The implementation MAY provide optional helper
//# adapters for common executors or embedded frameworks, but the core
//# crate MUST remain usable without them.
#[test]
fn requirement_core_api_remains_usable_without_executor_or_framework_helpers() {
    let mut flash = MockFlash::<256, 5, 256>::new(0xff);
    let mut storage = Storage::<_, 256, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    assert_no_alloc("blocking core api", || {
        storage.create_map(CollectionId(85)).unwrap();
    });

    drop(storage);
    let reopened = Storage::<_, 256, 5, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.metadata().region_size, 256);
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(85));
}

//= spec/ring/02-state-machines.md#storage-api-requirements
//= type=test
//# `RING-API-001` `Storage` MUST be the public database context that owns logical runtime state,
//# replay state, configuration, dirty-frontier tracking, and bounded reusable scratch memory needed
//# by normal storage and collection operations.
#[test]
fn requirement_storage_context_owns_operation_scratch() {
    let mut flash = MockFlash::<1024, 10, 512>::new(0xff);
    let mut storage = Storage::<_, 1024, 10, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    assert_eq!(storage.mode(), StorageMode::Idle);
    storage.create_map(CollectionId(12)).unwrap();
    storage
        .append_map_update::<u16, u16>(CollectionId(12), &MapUpdate::Set { key: 3, value: 30 })
        .unwrap();

    let mut map_buffer = [0u8; 256];
    let map = storage
        .open_map::<u16, u16, 8>(
            CollectionId(12),
            &mut map_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(map.get_frontier(&3).unwrap(), Some(30));
}

//= spec/ring/02-state-machines.md#storage-api-requirements
//= type=test
//# `RING-API-002` `Storage` MUST own exclusive access to the backing object for the lifetime of an
//# opened database, either by owning the backing value or by holding a mutable reference to it.
#[test]
fn requirement_storage_owns_backing_access() {
    let mut flash = MockFlash::<256, 5, 512>::new(0xff);
    let storage = Storage::<_, 256, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    let backing = storage.into_backing();
    let reopened = Storage::<_, 256, 5, 8>::open(backing, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.metadata().region_count, 5);
}

//= spec/ring/02-state-machines.md#storage-api-requirements
//= type=test
//# `RING-API-003` Public operations that may touch backing media MUST use the backing object
//# through `Storage` rather than requiring a separate backing argument on each operation.
#[test]
fn requirement_operations_use_storage_backing() {
    let mut flash = MockFlash::<1024, 10, 512>::new(0xff);
    let mut storage = Storage::<_, 1024, 10, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(13)).unwrap();
    storage
        .append_map_update::<u16, u16>(CollectionId(13), &MapUpdate::Set { key: 4, value: 40 })
        .unwrap();
    storage.drop_map(CollectionId(13)).unwrap();
}

//= spec/ring/02-state-machines.md#storage-api-requirements
//= type=test
//# `RING-API-004` Public normal collection operations MUST NOT require callers to provide
//# collection frontier buffers, payload serialization buffers, or a `StorageWorkspace`; that
//# bounded memory MUST be supplied through caller-owned memory borrowed by `Storage` or the
//# collection handle.
#[test]
fn requirement_collection_operations_use_storage_owned_buffers() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    let collection_id = map.collection_id();

    assert!(!map.set(&mut storage, 9, 90).unwrap());
    assert_eq!(
        storage.frontier_buffer_owner(),
        crate::FrontierBufferOwner::Map {
            collection_id,
            generation: 1,
            dirty: true,
        }
    );
    assert_eq!(
        map.get(&mut storage, &9, |_, value| *value).unwrap(),
        Some(90)
    );
    drop(map);
    drop(storage);

    let mut reopened =
        Storage::<_, 512, 8, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    let mut reopened_map =
        LsmMap::<u16, u16, 8>::open(collection_id, &mut reopened, crate::test_lsm_map_memory())
            .unwrap();
    assert_eq!(
        reopened_map
            .get(&mut reopened, &9, |_, value| *value)
            .unwrap(),
        Some(90)
    );
}

//= spec/ring/02-state-machines.md#storage-api-requirements
//= type=test
//# `RING-API-005` Any shared-device synchronization required by a platform MUST be encapsulated by
//# the backing implementation rather than by Borromean core requiring a specific mutex, executor,
//# interrupt policy, or sharing primitive.
#[test]
fn requirement_shared_backing_synchronization_stays_behind_backing_trait() {
    let mut flash = GuardedFlash::<256, 5, 2048>::new(0xff);
    let mut storage = Storage::<_, 256, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(14)).unwrap();
    storage
        .append_map_update::<u16, u16>(CollectionId(14), &MapUpdate::Set { key: 2, value: 20 })
        .unwrap();

    let backing = storage.into_backing();
    assert!(backing.guard_entries() > 0);
    assert!(!backing.in_guard);
}

struct GuardedFlash<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> {
    inner: MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    guard_entries: usize,
    in_guard: bool,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>
    GuardedFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn new(erased_byte: u8) -> Self {
        Self {
            inner: MockFlash::new(erased_byte),
            guard_entries: 0,
            in_guard: false,
        }
    }

    fn guard_entries(&self) -> usize {
        self.guard_entries
    }

    fn with_guard<T>(
        &mut self,
        operation: impl FnOnce(&mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>) -> T,
    ) -> T {
        assert!(!self.in_guard);
        self.in_guard = true;
        self.guard_entries += 1;
        let result = operation(&mut self.inner);
        self.in_guard = false;
        result
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> FlashIo
    for GuardedFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, StorageIoError> {
        self.with_guard(|inner| inner.read_metadata().map_err(StorageIoError::from))
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), StorageIoError> {
        self.with_guard(|inner| inner.write_metadata(metadata).map_err(StorageIoError::from))
    }

    fn read_region<R, F>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, StorageIoError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.with_guard(|inner| {
            inner
                .read_region(region_index, offset, len, read)
                .map_err(StorageIoError::from)
        })
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageIoError> {
        self.with_guard(|inner| {
            inner
                .write_region(region_index, offset, data)
                .map_err(StorageIoError::from)
        })
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), StorageIoError> {
        self.with_guard(|inner| {
            inner
                .erase_region(region_index)
                .map_err(StorageIoError::from)
        })
    }

    fn sync(&mut self) -> Result<(), StorageIoError> {
        self.with_guard(|inner| inner.sync().map_err(StorageIoError::from))
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, StorageFormatError> {
        self.with_guard(|inner| {
            inner
                .format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)
                .map_err(StorageFormatError::from)
        })
    }
}

//= spec/ring/02-state-machines.md#ring-state-machine-requirements
//= type=test
//# `RING-MACHINE-001` Storage runtime MUST expose a single active storage mode so that at most
//# one read, collection, WAL, allocation, region-write, rotation, reclaim, formatting, or opening
//# operation is active for a storage context.
#[test]
fn requirement_storage_runtime_exposes_single_active_mode() {
    let mut flash = MockFlash::<256, 5, 512>::new(0xff);
    let mut storage = Storage::<_, 256, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    assert_eq!(storage.mode(), StorageMode::Idle);
    storage.set_mode_unchecked(StorageMode::CreatingCollection(
        CollectionCreateMode::Running,
    ));
    assert_eq!(
        storage.create_map(CollectionId(14)),
        Err(StorageRuntimeError::InvalidStorageMode {
            expected: StorageMode::Idle,
            actual: StorageMode::CreatingCollection(CollectionCreateMode::Running),
        })
    );
    storage.finish_mode();
    assert_eq!(storage.mode(), StorageMode::Idle);
}

//= spec/ring/02-state-machines.md#ring-state-machine-requirements
//= type=test
//# `RING-MACHINE-002` Stable replayed runtime state MUST be kept separate from
//# operation-specific progress state owned by the active mode.
#[test]
fn requirement_runtime_state_is_separate_from_operation_progress() {
    let mut flash = MockFlash::<256, 5, 512>::new(0xff);
    let mut storage = Storage::<_, 256, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let metadata = storage.metadata();

    storage.set_mode_unchecked(StorageMode::UpdatingCollection(
        CollectionUpdateMode::Running,
    ));
    assert_eq!(storage.metadata(), metadata);
    assert_eq!(
        storage.mode(),
        StorageMode::UpdatingCollection(CollectionUpdateMode::Running)
    );
    storage.finish_mode();
}

//= spec/ring/02-state-machines.md#ring-state-machine-requirements
//= type=test
//# `RING-MACHINE-003` Public steady-state operations MUST validate that the storage context is
//# in a valid source mode, normally `Idle`, before beginning their transition sequence.
#[test]
fn requirement_public_operations_validate_source_mode() {
    let mut flash = MockFlash::<256, 5, 512>::new(0xff);
    let mut storage = Storage::<_, 256, 5, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.set_mode_unchecked(StorageMode::UpdatingCollection(
        CollectionUpdateMode::Running,
    ));

    assert_eq!(
        storage.create_map(CollectionId(15)),
        Err(StorageRuntimeError::InvalidStorageMode {
            expected: StorageMode::Idle,
            actual: StorageMode::UpdatingCollection(CollectionUpdateMode::Running),
        })
    );
    storage.finish_mode();
    storage.create_map(CollectionId(15)).unwrap();
}

//= spec/ring/02-state-machines.md#ring-state-machine-requirements
//= type=test
//# `RING-MACHINE-004` Every durable write that changes replay-visible state MUST be represented
//# as a named transition edge with defined preconditions, durable effect, runtime effect, replay
//# effect, and crash-cut result.
#[test]
fn requirement_durable_writes_are_named_transition_edges() {
    for edge in DurableTransitionEdge::ALL {
        let semantics = edge.semantics();
        assert_eq!(semantics.edge, *edge);
        assert!(!semantics.preconditions.is_empty());
        assert!(!semantics.durable_effect.is_empty());
        assert!(!semantics.runtime_effect.is_empty());
        assert!(!semantics.replay_effect.is_empty());
        assert!(!semantics.crash_cut_result.is_empty());
    }

    let free_intent = DurableTransitionEdge::StageFreeIntent.semantics();
    assert!(free_intent.runtime_effect.contains("no allocator effect"));
    assert!(free_intent.replay_effect.contains("before commit"));

    let commit = DurableTransitionEdge::CommitTransaction.semantics();
    assert!(commit.runtime_effect.contains("imports private effects"));
    assert!(commit.crash_cut_result.contains("visible atomically"));
}

//= spec/ring/02-state-machines.md#ring-state-machine-requirements
//= type=test
//# `RING-MACHINE-005` Normal foreground operation, startup replay, and crash recovery MUST use
//# the same `ApplyWalRecord` semantics for every retained durable WAL record.
#[test]
fn requirement_foreground_replay_and_recovery_share_wal_record_semantics() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let mut storage = Storage::<_, 512, 6, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(16)).unwrap();
    storage
        .append_map_update::<u16, u16>(CollectionId(16), &MapUpdate::Set { key: 1, value: 10 })
        .unwrap();
    let foreground_collections = [storage.collections()[0]];
    let foreground_append_offset = storage.wal_append_offset();
    drop(storage);

    let reopened = Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.collections(), foreground_collections.as_slice());
    assert_eq!(reopened.wal_append_offset(), foreground_append_offset);
}

//= spec/ring/02-state-machines.md#ring-state-machine-requirements
//= type=test
//# `RING-MACHINE-009` Startup and recovery modes MUST compose the same
//# collection, allocator, WAL-chain, and transaction submachine
//# transitions used by normal operation rather than defining separate
//# incompatible transition rules.
#[test]
fn requirement_startup_and_recovery_compose_normal_submachines() {
    let open = StateMachineOperation::OpenStorage.rule();
    assert!(open.active_mode.contains("Opening"));
    assert!(open
        .durable_edges
        .contains(&DurableTransitionEdge::RollbackTransaction));
    assert!(open
        .durable_edges
        .contains(&DurableTransitionEdge::AppendFreeRegion));
    assert!(open
        .durable_edges
        .contains(&DurableTransitionEdge::FinishTransaction));

    let rollback = StateMachineOperation::RollbackTransaction.rule();
    assert!(rollback.active_mode.contains("TransactionRecovery"));
    assert_eq!(
        rollback.durable_edges,
        &[
            DurableTransitionEdge::RollbackTransaction,
            DurableTransitionEdge::AppendFreeRegion,
            DurableTransitionEdge::FinishTransaction,
        ]
    );

    let reclaim = StateMachineOperation::ReclaimWalHead.rule();
    assert!(reclaim.active_mode.contains("ReclaimingWalHead"));
    assert!(reclaim
        .durable_edges
        .contains(&DurableTransitionEdge::CopyRetainedWalRecord));
    assert!(reclaim
        .durable_edges
        .contains(&DurableTransitionEdge::CommitWalHeadControl));
}

//= spec/ring/02-state-machines.md#ring-state-machine-requirements
//= type=test
//# `RING-MACHINE-010` State-machine transition rules MUST use named operation identifiers, and
//# each named operation MUST define its source state, active mode, durable edge sequence, and
//# target state or runtime effect.
#[test]
fn requirement_state_machine_transitions_use_named_operations() {
    for operation in StateMachineOperation::ALL {
        let rule = operation.rule();
        assert_eq!(rule.operation, *operation);
        assert!(!rule.source.is_empty());
        assert!(!rule.active_mode.is_empty());
        assert!(!rule.target_or_effect.is_empty());
        for edge in rule.durable_edges {
            assert!(DurableTransitionEdge::ALL.contains(edge));
        }
    }

    assert_eq!(
        StateMachineOperation::CreateCollection.rule().durable_edges,
        &[DurableTransitionEdge::CreateCollection]
    );
    assert!(StateMachineOperation::CommitCollectionRegion
        .rule()
        .durable_edges
        .contains(&DurableTransitionEdge::WriteCommittedRegion));
    assert!(StateMachineOperation::StageFreeIntent
        .rule()
        .durable_edges
        .contains(&DurableTransitionEdge::StageFreeIntent));
}
