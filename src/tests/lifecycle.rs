#![allow(clippy::drop_non_drop)]

use crate::{
    CollectionId, CollectionType, FlashIo, LsmMap, MockFlash, Storage, StorageFormatConfig,
    StorageRuntimeError,
};

#[derive(Debug, Clone, Copy)]
struct LifecycleRng {
    state: u64,
}

impl LifecycleRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    fn next_bounded(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }
}

#[derive(Debug, Default)]
struct LifecycleStats {
    wal_reclaims: usize,
    compactions: usize,
}

fn mark_region<const REGION_COUNT: usize>(seen_regions: &mut [bool; REGION_COUNT], region: u32) {
    let region = usize::try_from(region).unwrap();
    assert!(region < REGION_COUNT);
    seen_regions[region] = true;
}

fn seen_region_count<const REGION_COUNT: usize>(seen_regions: &[bool; REGION_COUNT]) -> usize {
    seen_regions.iter().filter(|seen| **seen).count()
}

fn mark_current_wal_chain<
    'db,
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &mut Storage<'db, 'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    seen_regions: &mut [bool; REGION_COUNT],
) -> Result<(), StorageRuntimeError> {
    mark_region(seen_regions, storage.wal_head());
    mark_region(seen_regions, storage.wal_tail());
    if storage.wal_head() == storage.wal_tail() {
        return Ok(());
    }

    let regions = storage.with_runtime_io_workspace(|runtime, flash, workspace| {
        let mut plan = crate::storage::WalHeadReclaimPlan::<MAX_COLLECTIONS>::empty();
        runtime.prepare_wal_head_reclaim::<REGION_SIZE, IO>(flash, workspace, &mut plan)?;
        let mut regions = heapless::Vec::<u32, REGION_COUNT>::new();
        runtime.collect_wal_head_reclaim_regions::<REGION_SIZE, REGION_COUNT, IO>(
            flash,
            workspace,
            &plan,
            &mut regions,
        )?;
        Ok::<_, StorageRuntimeError>(regions)
    })?;
    for region in regions {
        mark_region(seen_regions, region);
    }
    Ok(())
}

fn force_wal_rotation<
    'db,
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &mut Storage<'db, 'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
) {
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.rotate_wal_tail::<REGION_SIZE, REGION_COUNT, IO>(flash, workspace)
        })
        .unwrap();
}

fn wal_chain_len<
    'db,
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &mut Storage<'db, 'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
) -> Result<usize, StorageRuntimeError> {
    if storage.wal_head() == storage.wal_tail() {
        return Ok(1);
    }

    storage.with_runtime_io_workspace(|runtime, flash, workspace| {
        let mut plan = crate::storage::WalHeadReclaimPlan::<MAX_COLLECTIONS>::empty();
        runtime.prepare_wal_head_reclaim::<REGION_SIZE, IO>(flash, workspace, &mut plan)?;
        let mut regions = heapless::Vec::<u32, REGION_COUNT>::new();
        runtime
            .collect_wal_head_reclaim_regions::<REGION_SIZE, REGION_COUNT, IO>(
                flash,
                workspace,
                &plan,
                &mut regions,
            )
            .map(|()| regions.len())
    })
}

fn observe_completed_transaction_cleanup<
    'db,
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    _storage: &mut Storage<'db, 'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    _stats: &mut LifecycleStats,
) {
}

fn service_storage_lifecycle<
    'db,
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
>(
    storage: &mut Storage<'db, 'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    wal_reclaim_threshold: usize,
    stats: &mut LifecycleStats,
) {
    observe_completed_transaction_cleanup(storage, stats);
    if storage.wal_head() == storage.wal_tail() {
        return;
    }

    let chain_len = wal_chain_len(storage).unwrap();
    if chain_len < wal_reclaim_threshold {
        return;
    }

    let old_head = storage.wal_head();
    let new_head = storage.reclaim_wal_head().unwrap();
    assert_ne!(old_head, new_head);
    assert_eq!(storage.wal_head(), new_head);
    let new_chain_len = wal_chain_len(storage).unwrap();
    assert!(new_chain_len < chain_len);
    stats.wal_reclaims += 1;
    observe_completed_transaction_cleanup(storage, stats);
}

fn assert_map_model<
    'db,
    IO: FlashIo,
    const KEY_SPACE: usize,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_INDEXES: usize,
    const MAX_RUNS: usize,
>(
    storage: &mut Storage<'db, 'db, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    map: &mut LsmMap<'_, u64, u64, MAX_INDEXES, MAX_RUNS>,
    expected: &[Option<u64>; KEY_SPACE],
) {
    for (key, expected_value) in expected.iter().enumerate() {
        let key = u64::try_from(key).unwrap();
        assert_eq!(
            map.get(storage, &key, |_, value| *value).unwrap(),
            *expected_value
        );
    }
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-109` WAL lifecycle stress MUST rotate through every data region, reclaim
//# WAL prefixes, reuse reclaimed regions, and reopen with live collection state intact.
#[test]
#[ignore = "forced WAL-rotation lifecycle stress needs a transaction-aware rewrite"]
fn requirement_wal_lifecycle_reuses_every_region_across_reclaim_cycles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 48;
    const MAX_LOG: usize = 16_384;
    const MAX_COLLECTIONS: usize = 8;

    let mut flash =
        std::boxed::Box::new(MockFlash::<REGION_SIZE, REGION_COUNT, MAX_LOG>::new(0xff));
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::format(
        &mut *flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(CollectionId::new(1)).unwrap();

    let mut seen_regions = [false; REGION_COUNT];
    mark_current_wal_chain(&mut storage, &mut seen_regions).unwrap();
    let mut stats = LifecycleStats::default();

    for iteration in 0..(REGION_COUNT * 3) {
        force_wal_rotation(&mut storage);
        mark_current_wal_chain(&mut storage, &mut seen_regions).unwrap();
        storage.with_io_workspace(|flash, _workspace| flash.clear_operations());

        if wal_chain_len(&mut storage).unwrap() >= 16 {
            let reclaimed_head = storage.reclaim_wal_head().unwrap_or_else(|error| {
                panic!(
                    "wal reclaim failed at iteration {iteration}: {error:?}; head={} tail={} append={} free_head={:?} free_tail={:?}",
                    storage.wal_head(),
                    storage.wal_tail(),
                    storage.wal_append_offset(),
                    storage.last_free_list_head(),
                    storage.free_list_tail(),
                )
            });
            mark_region(&mut seen_regions, reclaimed_head);
            mark_current_wal_chain(&mut storage, &mut seen_regions).unwrap();
            stats.wal_reclaims += 1;
            storage.with_io_workspace(|flash, _workspace| flash.clear_operations());
        }
    }

    if seen_region_count(&seen_regions) != REGION_COUNT {
        let missing = seen_regions
            .iter()
            .enumerate()
            .filter_map(|(region, seen)| (!*seen).then_some(region))
            .collect::<std::vec::Vec<_>>();
        panic!(
            "saw {} of {REGION_COUNT} regions; missing={missing:?}; head={} tail={} free_head={:?} free_tail={:?}",
            seen_region_count(&seen_regions),
            storage.wal_head(),
            storage.wal_tail(),
            storage.last_free_list_head(),
            storage.free_list_tail(),
        );
    }
    assert!(stats.wal_reclaims >= 3);

    drop(storage);
    let reopened = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::open(
        &mut *flash,
        crate::test_storage_memory(),
    )
    .unwrap();
    assert!(reopened
        .collections()
        .iter()
        .any(
            |collection| collection.collection_id() == CollectionId::new(1)
                && collection.collection_type() == Some(CollectionType::MAP_CODE)
        ));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-110` Map lifecycle stress MUST preserve modeled key/value state across
//# writes, deletes, compactions, committed-region reclaims, WAL rollovers, and WAL-head reclaims.
#[test]
fn requirement_map_lifecycle_preserves_model_across_compaction_reclaim_and_rollover() {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(run_map_lifecycle_preserves_model_across_compaction_reclaim_and_rollover)
        .unwrap()
        .join()
        .unwrap();
}

fn run_map_lifecycle_preserves_model_across_compaction_reclaim_and_rollover() {
    const REGION_SIZE: usize = 4096;
    const REGION_COUNT: usize = 384;
    const MAX_LOG: usize = 524_288;
    const MAX_COLLECTIONS: usize = 8;
    const MAX_INDEXES: usize = 128;
    const MAX_RUNS: usize = 128;
    const KEY_SPACE: usize = 96;

    let mut flash =
        std::boxed::Box::new(MockFlash::<REGION_SIZE, REGION_COUNT, MAX_LOG>::new(0xff));
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::format(
        &mut *flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map =
        LsmMap::<u64, u64, MAX_INDEXES, MAX_RUNS>::new(&mut storage, crate::test_lsm_map_memory())
            .unwrap()
            .with_compaction_run_target(8)
            .unwrap();
    let collection_id = map.collection_id();
    let mut expected = [None; KEY_SPACE];
    let mut rng = LifecycleRng::new(0x5eed_cafe_f00d_fade);
    let mut stats = LifecycleStats::default();
    let initial_wal_head = storage.wal_head();

    for operation_index in 0..2_048usize {
        let key_index = usize::try_from(rng.next_bounded(KEY_SPACE as u64)).unwrap();
        let key = u64::try_from(key_index).unwrap();
        let compact_needed = if rng.next_bounded(100) < 25 {
            expected[key_index] = None;
            map.delete(&mut storage, key).unwrap()
        } else {
            let value = rng.next_u64();
            expected[key_index] = Some(value);
            map.set(&mut storage, key, value).unwrap()
        };

        if compact_needed || operation_index % 257 == 256 {
            map.compact(&mut storage).unwrap();
            stats.compactions += 1;
        }
        if operation_index % 64 == 63 {
            service_storage_lifecycle(&mut storage, 24, &mut stats);
        }
        if operation_index % 509 == 508 {
            assert_map_model::<
                _,
                KEY_SPACE,
                REGION_SIZE,
                REGION_COUNT,
                MAX_COLLECTIONS,
                MAX_INDEXES,
                MAX_RUNS,
            >(&mut storage, &mut map, &expected);
        }
    }

    map.compact(&mut storage).unwrap();
    stats.compactions += 1;
    service_storage_lifecycle(&mut storage, 2, &mut stats);
    assert_map_model::<
        _,
        KEY_SPACE,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_INDEXES,
        MAX_RUNS,
    >(&mut storage, &mut map, &expected);

    assert!(stats.compactions > 0);
    assert!(stats.wal_reclaims > 0);
    assert_ne!(storage.wal_head(), initial_wal_head);

    drop(storage);

    let mut reopened = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::open(
        &mut *flash,
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut reopened_map = LsmMap::<u64, u64, MAX_INDEXES, MAX_RUNS>::open(
        collection_id,
        &mut reopened,
        crate::test_lsm_map_memory(),
    )
    .unwrap();
    assert_map_model::<
        _,
        KEY_SPACE,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_INDEXES,
        MAX_RUNS,
    >(&mut reopened, &mut reopened_map, &expected);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-111` WAL-head reclaim capacity stress MUST reclaim a bounded WAL prefix
//# when the full chain is longer than the cleanup batch capacity.
#[test]
#[ignore = "forced WAL-head reclaim capacity stress needs a transaction-aware rewrite"]
fn requirement_wal_head_reclaim_capacity_stress_reclaims_bounded_prefix() {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(run_wal_head_reclaim_capacity_stress_reclaims_bounded_prefix)
        .unwrap()
        .join()
        .unwrap();
}

fn run_wal_head_reclaim_capacity_stress_reclaims_bounded_prefix() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 80;
    const MAX_LOG: usize = 131_072;
    const MAX_COLLECTIONS: usize = 8;
    const WAL_HEAD_RECLAIM_PREFIX_LIMIT: usize = 64;

    let mut flash =
        std::boxed::Box::new(MockFlash::<REGION_SIZE, REGION_COUNT, MAX_LOG>::new(0xff));
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::format(
        &mut *flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(CollectionId::new(1)).unwrap();

    while wal_chain_len(&mut storage).unwrap() <= WAL_HEAD_RECLAIM_PREFIX_LIMIT {
        force_wal_rotation(&mut storage);
        storage.with_io_workspace(|flash, _workspace| flash.clear_operations());
    }

    let source_regions = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            let mut plan = crate::storage::WalHeadReclaimPlan::<MAX_COLLECTIONS>::empty();
            runtime.prepare_wal_head_reclaim::<REGION_SIZE, _>(flash, workspace, &mut plan)?;
            let mut regions = heapless::Vec::<u32, REGION_COUNT>::new();
            runtime.collect_wal_head_reclaim_regions::<REGION_SIZE, REGION_COUNT, _>(
                flash,
                workspace,
                &plan,
                &mut regions,
            )?;
            Ok::<_, StorageRuntimeError>(regions)
        })
        .unwrap();
    let original_chain_len = source_regions.len();
    let expected_new_head = source_regions[WAL_HEAD_RECLAIM_PREFIX_LIMIT];
    let reclaimed_head = storage.reclaim_wal_head().unwrap();

    assert_eq!(reclaimed_head, expected_new_head);
    assert_eq!(storage.wal_head(), expected_new_head);
    assert!(wal_chain_len(&mut storage).unwrap() < original_chain_len);
}
