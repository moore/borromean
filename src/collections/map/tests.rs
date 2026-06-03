#![allow(clippy::drop_non_drop)]

use super::*;
extern crate std;
use crate::wal_record::encode_record_into;
#[cfg(feature = "perf-counters")]
use crate::StoragePerfMetrics;
use crate::{MockFlash, MockOperation, Storage, StorageFormatConfig, StorageWorkspace};
use postcard::to_slice;
use proptest::prelude::*;
#[cfg(feature = "perf-counters")]
use serde::{Deserialize, Serialize};
use std::{vec, vec::Vec};

type LargeValue = ([u8; 16], [u8; 16]);

fn large_value(byte: u8) -> LargeValue {
    ([byte; 16], [byte; 16])
}

fn vec_and_indexes() -> impl Strategy<Value = (Vec<u8>, usize, usize)> {
    prop::collection::vec(0..1u8, (ENTRY_REF_SIZE * 2)..(10 * ENTRY_REF_SIZE)).prop_flat_map(
        |vec| {
            let len = vec.len();
            let first = 1..(len / ENTRY_REF_SIZE);
            let second = 1..(len / ENTRY_REF_SIZE);
            (Just(vec), first, second)
        },
    )
}

fn append_wal_record<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    metadata: crate::StorageMetadata,
    region_index: u32,
    offset: usize,
    record: crate::WalRecord<'_>,
) -> usize {
    let mut physical = [0u8; REGION_SIZE];
    let mut logical = [0u8; REGION_SIZE];
    let used = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();
    flash
        .write_region(region_index, offset, &physical[..used])
        .unwrap();
    offset + used
}

fn init_user_region_header<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    region_index: u32,
    sequence: u64,
    collection_id: CollectionId,
    collection_format: u16,
) {
    let header = crate::Header {
        sequence,
        collection_id,
        collection_format,
    };
    let mut header_bytes = [0u8; crate::Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    flash.write_region(region_index, 0, &header_bytes).unwrap();
}

fn write_committed_payload<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &mut MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    region_index: u32,
    sequence: u64,
    collection_id: CollectionId,
    collection_format: u16,
    payload: &[u8],
) {
    init_user_region_header(
        flash,
        region_index,
        sequence,
        collection_id,
        collection_format,
    );
    flash
        .write_region(region_index, Header::ENCODED_LEN, payload)
        .unwrap();
}

fn snapshot_for_entries(entries: &[(i32, Option<i32>)]) -> ([u8; 512], usize) {
    let mut buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 8>::new(
        CollectionId(90),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for (key, value) in entries.iter().copied() {
        match value {
            Some(value) => map.set_in_memory(key, value).unwrap(),
            None => map.delete_in_memory(key).unwrap(),
        }
    }

    let mut snapshot = [0u8; 512];
    let snapshot_len = map.encode_snapshot_into(&mut snapshot).unwrap();
    (snapshot, snapshot_len)
}

//= spec/map.md#map-api-model
//= type=test
//# `LsmMap::new(storage, memory)` creates a durable map
//# collection, assigns it a stable collection id, and returns an empty map
//# handle.
#[test]
fn requirement_object_lsm_map_new_open_get_set_delete_use_storage_owned_scratch() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    assert_eq!(map.collection_id(), CollectionId(1));
    assert_eq!(map.get(&mut storage, &7, |_, value| *value).unwrap(), None);

    assert!(!map.set(&mut storage, 7, 70).unwrap());

    let mut callback_calls = 0usize;
    let got = map
        .get(&mut storage, &7, |lookup_key, value| {
            callback_calls += 1;
            assert_eq!(*lookup_key, 7);
            *value + 1
        })
        .unwrap();
    assert_eq!(got, Some(71));
    assert_eq!(callback_calls, 1);

    assert!(!map.delete(&mut storage, 7).unwrap());
    callback_calls = 0;
    assert_eq!(
        map.get(&mut storage, &7, |_, _| {
            callback_calls += 1;
        })
        .unwrap(),
        None
    );
    assert_eq!(callback_calls, 0);

    let second = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    assert_eq!(second.collection_id(), CollectionId(2));

    let first_collection_id = map.collection_id();
    let second_collection_id = second.collection_id();
    let mut saw_first_new_collection = false;
    let mut saw_second_new_collection = false;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, record| {
                match record {
                    crate::WalRecord::NewCollection {
                        collection_id,
                        collection_type,
                    } if collection_id == first_collection_id => {
                        assert_eq!(collection_type, crate::CollectionType::MAP_CODE);
                        saw_first_new_collection = true;
                    }
                    crate::WalRecord::NewCollection {
                        collection_id,
                        collection_type,
                    } if collection_id == second_collection_id => {
                        assert_eq!(collection_type, crate::CollectionType::MAP_CODE);
                        saw_second_new_collection = true;
                    }
                    _ => {}
                }
                Ok(())
            })
        })
        .unwrap();
    assert!(saw_first_new_collection);
    assert!(saw_second_new_collection);

    let collection_id = first_collection_id;
    drop(storage);
    let mut reopened =
        Storage::<_, 512, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    let mut reopened_map =
        LsmMap::<u16, u16, 8>::open(collection_id, &mut reopened, crate::test_lsm_map_memory())
            .unwrap();
    assert_eq!(
        reopened_map
            .get(&mut reopened, &7, |_, value| *value)
            .unwrap(),
        None
    );
}

#[cfg(feature = "perf-counters")]
//= spec/map.md#map-api-model
//= type=test
//# `open` reconstructs the logical map from the retained durable basis
//# and later retained updates using buffers borrowed through
//# `StorageMemory`.
#[test]
fn requirement_perf_metrics_first_read_records_frontier_miss_and_reload() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();

    storage.reset_perf_metrics();
    assert_eq!(map.get(&mut storage, &7, |_, value| *value).unwrap(), None);

    let metrics = storage.perf_metrics();
    assert_eq!(metrics.map_reads, 1);
    assert_eq!(metrics.frontier_cache_misses, 1);
    assert_eq!(metrics.frontier_reloads, 1);
    assert_eq!(metrics.frontier_cache_hits, 0);
    assert!(metrics.map_read_lookup_nanos > 0);
}

#[cfg(feature = "perf-counters")]
//= spec/map.md#map-api-model
//= type=test
//# This keeps each `LsmMap` handle small enough that many collections
//# can be open at once; the handle primarily tracks the collection id and
//# borrows `LsmMapMemory` for cached frontier state and compaction
//# scratch.
#[test]
fn requirement_perf_metrics_hot_reads_record_frontier_cache_hits() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    assert!(!map.set(&mut storage, 7, 70).unwrap());

    storage.reset_perf_metrics();
    assert_eq!(
        map.get(&mut storage, &7, |_, value| *value).unwrap(),
        Some(70)
    );
    assert_eq!(
        map.get(&mut storage, &7, |_, value| *value).unwrap(),
        Some(70)
    );

    let metrics = storage.perf_metrics();
    assert_eq!(metrics.map_reads, 2);
    assert_eq!(metrics.frontier_cache_hits, 2);
    assert_eq!(metrics.frontier_cache_misses, 0);
    assert_eq!(metrics.frontier_reloads, 0);
    assert_eq!(metrics.encoded_key_comparisons, 2);
    assert_eq!(metrics.key_decodes_during_comparison, 0);
    assert_eq!(metrics.value_decodes, 2);
    assert!(metrics.map_read_lookup_nanos > 0);
}

#[cfg(feature = "perf-counters")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct DecodingTestKey(u16);

#[cfg(feature = "perf-counters")]
impl LsmKey for DecodingTestKey {
    fn encode_key(&self, out: &mut [u8]) -> Result<usize, LsmKeyError> {
        to_slice(self, out)
            .map(|encoded| encoded.len())
            .map_err(LsmKeyError::from)
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, LsmKeyError> {
        from_bytes(bytes).map_err(LsmKeyError::from)
    }
}

#[cfg(feature = "perf-counters")]
//= spec/map.md#key-and-value-model
//= type=test
//# For keys whose natural ordering is not lexicographic over their raw
//# bytes, `K: Ord` must define the stable map ordering and the encoded
//# representation must remain compatible with that ordering wherever
//# committed run metadata uses decoded key bounds.
#[test]
fn requirement_perf_metrics_custom_key_fallback_records_key_decodes() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map =
        LsmMap::<DecodingTestKey, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    assert!(!map.set(&mut storage, DecodingTestKey(7), 70).unwrap());

    storage.reset_perf_metrics();
    assert_eq!(
        map.get(&mut storage, &DecodingTestKey(7), |_, value| *value)
            .unwrap(),
        Some(70)
    );

    let metrics = storage.perf_metrics();
    assert_eq!(metrics.encoded_key_comparisons, 1);
    assert_eq!(metrics.key_decodes_during_comparison, 1);
    assert_eq!(metrics.value_decodes, 1);
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# The in-memory frontier and snapshot payload use the same sorted-entry
//# model, so the low-level helper behavior is part of the durable
//# contract.
#[test]
fn requirement_v2_entry_layout_validates_headers_and_lengths() {
    let mut encoded = [0u8; 32];
    let used = encode_entry_into(&5u16, Some(&70u16), &mut encoded).unwrap();
    assert_eq!(encoded[0], ENTRY_KIND_SET);
    let parsed = parse_encoded_entry(&encoded[..used]).unwrap();
    assert_eq!(u16::decode_key(parsed.key).unwrap(), 5);
    assert_eq!(u16::decode_value(parsed.value.unwrap()).unwrap(), 70);

    let delete_used = encode_entry_into::<u16, u16>(&5, None, &mut encoded).unwrap();
    assert_eq!(encoded[0], ENTRY_KIND_DELETE);
    assert!(matches!(
        encoded_entry_lookup_value::<u16>(&encoded[..delete_used]).unwrap(),
        LookupResult::Deleted
    ));

    let mut corrupt = encoded[..delete_used].to_vec();
    corrupt.extend_from_slice(&[0]);
    let value_len_offset = ENTRY_KIND_SIZE + ENTRY_KEY_LEN_SIZE;
    corrupt[value_len_offset..value_len_offset + ENTRY_VALUE_LEN_SIZE]
        .copy_from_slice(&1u32.to_le_bytes());
    assert!(matches!(
        parse_encoded_entry(&corrupt),
        Err(MapError::SerializationError)
    ));

    let mut corrupt = encoded[..used].to_vec();
    corrupt[ENTRY_KIND_SIZE..ENTRY_KIND_SIZE + ENTRY_KEY_LEN_SIZE]
        .copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        parse_encoded_entry(&corrupt),
        Err(MapError::SerializationError)
    ));
}

//= spec/map.md#key-and-value-model
//= type=test
//# `encode_key` produces the canonical durable key bytes.
#[test]
fn requirement_primitive_encoded_key_bytes_sort_like_ord() {
    fn assert_encoded_order<K>(values: &[K])
    where
        K: LsmKey + Copy + Ord,
    {
        let mut by_ord = values.to_vec();
        by_ord.sort();

        let mut encoded = Vec::new();
        for value in values {
            let mut bytes = [0u8; 16];
            let len = value.encode_key(&mut bytes).unwrap();
            encoded.push((bytes[..len].to_vec(), *value));
        }
        encoded.sort_by(|left, right| left.0.cmp(&right.0));
        let by_bytes: Vec<K> = encoded.into_iter().map(|(_, value)| value).collect();
        assert_eq!(by_bytes, by_ord);
    }

    assert_encoded_order(&[3u64, 0, u64::MAX, 7, 1]);
    assert_encoded_order(&[-3i64, 0, i64::MIN, i64::MAX, 7, 1]);
    assert_encoded_order(&[3u16, 0, u16::MAX, 7, 1]);
    assert_encoded_order(&[-3i32, 0, i32::MIN, i32::MAX, 7, 1]);
}

#[cfg(feature = "perf-counters")]
//= spec/map.md#map-api-model
//= type=test
//# `set` and `delete` update the logical map and persist the mutation,
//# flushing the frontier first if bounded in-memory capacity would
//# otherwise be exceeded.
#[test]
fn requirement_perf_metrics_set_delete_record_write_path() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();

    storage.reset_perf_metrics();
    assert!(!map.set(&mut storage, 7, 70).unwrap());
    assert!(!map.delete(&mut storage, 7).unwrap());

    let metrics = storage.perf_metrics();
    assert_eq!(metrics.map_sets, 1);
    assert_eq!(metrics.map_deletes, 1);
    assert_eq!(metrics.update_encodes, 2);
    assert!(metrics.encoded_update_bytes > 0);
    assert_eq!(metrics.frontier_checkpoints, 0);
    assert_eq!(metrics.frontier_applies, 2);
    assert_eq!(metrics.frontier_undo_records, 2);
    assert!(metrics.frontier_undo_bytes >= ENTRY_REF_SIZE as u64);
    assert_eq!(metrics.frontier_undo_restores, 0);
    assert_eq!(metrics.frontier_full_checkpoint_fallbacks, 0);
    assert_eq!(metrics.wal_update_records, 2);
    assert_eq!(metrics.wal_records, 2);
    assert_eq!(metrics.wal_syncs, 2);
    assert!(metrics.full_write_path_nanos > 0);
    assert!(metrics.update_encode_nanos > 0);
    assert!(metrics.frontier_apply_nanos > 0);
    assert!(metrics.wal_encode_nanos > 0);
    assert!(metrics.wal_write_nanos > 0);
    assert!(metrics.wal_sync_nanos > 0);
}

#[cfg(feature = "perf-counters")]
//= spec/ring/02-state-machines.md#storage-api-requirements
//= type=test
//# `Storage` MUST be the public database context that owns logical
//# runtime state, replay state, configuration, dirty-frontier tracking,
//# and bounded reusable scratch memory needed by normal storage and
//# collection operations.
#[test]
fn requirement_perf_metrics_reset_and_take_clear_metrics() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();

    assert!(!map.set(&mut storage, 7, 70).unwrap());
    assert!(storage.perf_metrics().map_sets > 0);

    storage.reset_perf_metrics();
    assert_eq!(storage.perf_metrics(), StoragePerfMetrics::default());

    assert!(!map.set(&mut storage, 8, 80).unwrap());
    let taken = storage.take_perf_metrics();
    assert_eq!(taken.map_sets, 1);
    assert_eq!(storage.perf_metrics(), StoragePerfMetrics::default());
}

#[cfg(feature = "perf-counters")]
fn count_mock_operations(
    operations: &[MockOperation],
    matches_operation: impl Fn(MockOperation) -> bool,
) -> usize {
    operations
        .iter()
        .copied()
        .filter(|operation| matches_operation(*operation))
        .count()
}

#[cfg(feature = "perf-counters")]
//= spec/map.md#map-api-model
//= type=test
//# On success, `set` and `delete` return `true` when the map's
//# configured compaction policy says compaction is needed after that
//# mutation and any required frontier flush.
#[test]
fn requirement_sync_audit_hot_inserts_write_one_wal_record_and_sync_once() {
    const COUNT: u16 = 10;
    let mut flash = MockFlash::<4096, 16, 8192>::new(0xff);
    let mut storage = Storage::<_, 4096, 16>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();

    storage.with_io_workspace(|flash, _| flash.clear_operations());
    storage.reset_perf_metrics();
    for key in 0..COUNT {
        assert!(!map.set(&mut storage, key, key + 100).unwrap());
    }

    let metrics = storage.perf_metrics();
    let operations = storage.with_io_workspace(|flash, _| flash.operations().to_vec());
    assert_eq!(metrics.map_sets, u64::from(COUNT));
    assert_eq!(metrics.frontier_checkpoints, 0);
    assert_eq!(metrics.frontier_undo_records, u64::from(COUNT));
    assert_eq!(metrics.frontier_undo_bytes, 0);
    assert_eq!(metrics.frontier_full_checkpoint_fallbacks, 0);
    assert_eq!(metrics.wal_records, u64::from(COUNT));
    assert_eq!(metrics.wal_syncs, u64::from(COUNT));
    assert_eq!(metrics.flushes, 0);
    assert_eq!(metrics.compactions_run, 0);
    assert_eq!(
        count_mock_operations(&operations, |operation| matches!(
            operation,
            MockOperation::WriteRegion { .. }
        )),
        usize::from(COUNT)
    );
    assert_eq!(
        count_mock_operations(&operations, |operation| operation == MockOperation::Sync),
        usize::from(COUNT)
    );
    assert_eq!(
        count_mock_operations(&operations, |operation| operation
            == MockOperation::WriteMetadata),
        0
    );
    assert_eq!(
        count_mock_operations(&operations, |operation| matches!(
            operation,
            MockOperation::EraseRegion { .. }
        )),
        0
    );
}

#[cfg(feature = "perf-counters")]
//= spec/map.md#map-api-model
//= type=test
//# They return `false` when no compaction is currently needed.
#[test]
fn requirement_sync_audit_hot_updates_exclude_preload_and_sync_once() {
    const COUNT: u16 = 10;
    let mut flash = MockFlash::<4096, 16, 8192>::new(0xff);
    let mut storage = Storage::<_, 4096, 16>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    for key in 0..COUNT {
        assert!(!map.set(&mut storage, key, key + 100).unwrap());
    }

    storage.with_io_workspace(|flash, _| flash.clear_operations());
    storage.reset_perf_metrics();
    for key in 0..COUNT {
        assert!(!map.set(&mut storage, key, key + 200).unwrap());
    }

    let metrics = storage.perf_metrics();
    let operations = storage.with_io_workspace(|flash, _| flash.operations().to_vec());
    assert_eq!(metrics.map_sets, u64::from(COUNT));
    assert_eq!(metrics.frontier_checkpoints, 0);
    assert_eq!(metrics.frontier_undo_records, u64::from(COUNT));
    assert_eq!(
        metrics.frontier_undo_bytes,
        u64::from(COUNT) * ENTRY_REF_SIZE as u64
    );
    assert_eq!(metrics.frontier_full_checkpoint_fallbacks, 0);
    assert_eq!(metrics.wal_records, u64::from(COUNT));
    assert_eq!(metrics.wal_syncs, u64::from(COUNT));
    assert_eq!(metrics.flushes, 0);
    assert_eq!(metrics.compactions_run, 0);
    assert_eq!(
        count_mock_operations(&operations, |operation| matches!(
            operation,
            MockOperation::WriteRegion { .. }
        )),
        usize::from(COUNT)
    );
    assert_eq!(
        count_mock_operations(&operations, |operation| operation == MockOperation::Sync),
        usize::from(COUNT)
    );
    assert_eq!(
        count_mock_operations(&operations, |operation| operation
            == MockOperation::WriteMetadata),
        0
    );
    assert_eq!(
        count_mock_operations(&operations, |operation| matches!(
            operation,
            MockOperation::EraseRegion { .. }
        )),
        0
    );
}

//= spec/map.md#map-api-model
//= type=test
//# The `StorageMemory` value borrowed by `Storage` owns storage runtime
//# state and operation scratch. The `LsmMapMemory` value borrowed by
//# `LsmMap` owns cached frontier descriptors and compaction temporaries.
#[test]
fn requirement_lsm_map_reuses_storage_owned_frontier_buffer_for_hot_reads() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();

    assert!(matches!(
        storage.frontier_buffer_owner(),
        crate::FrontierBufferOwner::Empty { .. }
    ));

    map.set(&mut storage, 7, 70).unwrap();
    let owner_after_write = storage.frontier_buffer_owner();
    assert!(matches!(
        owner_after_write,
        crate::FrontierBufferOwner::Map {
            collection_id,
            dirty: true,
            ..
        } if collection_id == map.collection_id()
    ));

    assert_eq!(
        map.get(&mut storage, &7, |_, value| *value).unwrap(),
        Some(70)
    );
    assert_eq!(storage.frontier_buffer_owner(), owner_after_write);
    assert_eq!(
        map.get(&mut storage, &7, |_, value| *value).unwrap(),
        Some(70)
    );
    assert_eq!(storage.frontier_buffer_owner(), owner_after_write);
}

//= spec/map.md#map-api-model
//= type=test
//# `get` observes newest-wins map visibility across the mutable frontier
//# and retained durable layers.
#[test]
fn requirement_lsm_map_raw_update_invalidates_cached_frontier() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();

    map.set(&mut storage, 7, 70).unwrap();
    assert_eq!(
        map.get(&mut storage, &7, |_, value| *value).unwrap(),
        Some(70)
    );

    storage
        .append_map_update::<u16, u16>(map.collection_id(), &MapUpdate::Set { key: 7, value: 71 })
        .unwrap();

    assert!(matches!(
        storage.frontier_buffer_owner(),
        crate::FrontierBufferOwner::Empty { .. }
    ));
    assert_eq!(
        map.get(&mut storage, &7, |_, value| *value).unwrap(),
        Some(71)
    );
}

//= spec/map.md#map-api-model
//= type=test
//# Map operations borrow `&mut Storage` while they use storage scratch.
#[test]
fn requirement_two_lsm_maps_reload_storage_owned_frontier_buffer() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(3, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut first = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    let mut second =
        LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();

    first.set(&mut storage, 1, 10).unwrap();
    assert!(matches!(
        storage.frontier_buffer_owner(),
        crate::FrontierBufferOwner::Map {
            collection_id,
            ..
        } if collection_id == first.collection_id()
    ));

    second.set(&mut storage, 2, 20).unwrap();
    assert!(matches!(
        storage.frontier_buffer_owner(),
        crate::FrontierBufferOwner::Map {
            collection_id,
            ..
        } if collection_id == second.collection_id()
    ));

    assert_eq!(
        first.get(&mut storage, &1, |_, value| *value).unwrap(),
        Some(10)
    );
    assert!(matches!(
        storage.frontier_buffer_owner(),
        crate::FrontierBufferOwner::Map {
            collection_id,
            ..
        } if collection_id == first.collection_id()
    ));

    assert_eq!(
        second.get(&mut storage, &2, |_, value| *value).unwrap(),
        Some(20)
    );
    assert!(matches!(
        storage.frontier_buffer_owner(),
        crate::FrontierBufferOwner::Map {
            collection_id,
            ..
        } if collection_id == second.collection_id()
    ));
}

//= spec/map.md#map-api-model
//= type=test
//# `set` and `delete` update the logical map and persist the mutation.
#[test]
fn requirement_lsm_map_writes_remain_durable_without_frontier_flush() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 8>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u16, u16, 8>::new(&mut storage, crate::test_lsm_map_memory()).unwrap();
    let collection_id = map.collection_id();

    map.set(&mut storage, 7, 70).unwrap();
    assert!(matches!(
        storage.frontier_buffer_owner(),
        crate::FrontierBufferOwner::Map { dirty: true, .. }
    ));

    drop(storage);
    let mut reopened =
        Storage::<_, 512, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    let mut reopened_map =
        LsmMap::<u16, u16, 8>::open(collection_id, &mut reopened, crate::test_lsm_map_memory())
            .unwrap();
    assert_eq!(
        reopened_map
            .get(&mut reopened, &7, |_, value| *value)
            .unwrap(),
        Some(70)
    );
}

//= spec/map.md#map-api-model
//= type=test
//# `compact`
//# performs whole-run compaction for that map using caller-owned scratch
//# buffers; if no compaction is needed, it returns successfully without
//# changing the logical map.
#[test]
fn requirement_object_lsm_map_compaction_signal_and_compact_preserve_visible_state() {
    let mut flash = MockFlash::<512, 12, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 12>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<i32, i32, 4>::new(&mut storage, crate::test_lsm_map_memory())
        .unwrap()
        .with_compaction_run_target(1)
        .unwrap();
    map.compact(&mut storage).unwrap();

    assert!(!map.set(&mut storage, 1, 10).unwrap());
    {
        let mut buffer = [0u8; 512];
        let mut frontier = storage
            .open_map::<i32, i32, 4>(
                map.collection_id(),
                &mut buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
        storage.flush_map(&mut frontier).unwrap();
    }

    assert!(map.set(&mut storage, 2, 20).unwrap());
    {
        let mut buffer = [0u8; 512];
        let mut frontier = storage
            .open_map::<i32, i32, 4>(
                map.collection_id(),
                &mut buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
        storage.flush_map(&mut frontier).unwrap();
    }

    assert!(map.set(&mut storage, 3, 30).unwrap());
    assert!(map.delete(&mut storage, 1).unwrap());
    map.compact(&mut storage).unwrap();

    assert_eq!(map.get(&mut storage, &1, |_, value| *value).unwrap(), None);
    assert_eq!(
        map.get(&mut storage, &2, |_, value| *value).unwrap(),
        Some(20)
    );
    assert_eq!(
        map.get(&mut storage, &3, |_, value| *value).unwrap(),
        Some(30)
    );
}

#[cfg(feature = "perf-counters")]
//= spec/map.md#map-api-model
//= type=test
//# `compact` performs whole-run compaction for that map using caller-owned
//# scratch buffers.
#[test]
fn requirement_object_lsm_map_compaction_reuses_cached_frontier() {
    let mut flash = MockFlash::<512, 12, 4096>::new(0xff);
    let mut storage = Storage::<_, 512, 12>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<i32, i32, 4>::new(&mut storage, crate::test_lsm_map_memory())
        .unwrap()
        .with_compaction_run_target(1)
        .unwrap();

    map.set(&mut storage, 1, 10).unwrap();
    {
        let mut buffer = [0u8; 512];
        let mut frontier = storage
            .open_map::<i32, i32, 4>(
                map.collection_id(),
                &mut buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
        storage.flush_map(&mut frontier).unwrap();
    }
    map.set(&mut storage, 2, 20).unwrap();
    {
        let mut buffer = [0u8; 512];
        let mut frontier = storage
            .open_map::<i32, i32, 4>(
                map.collection_id(),
                &mut buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
        storage.flush_map(&mut frontier).unwrap();
    }

    assert!(map.set(&mut storage, 3, 30).unwrap());
    assert!(map.delete(&mut storage, 1).unwrap());

    storage.reset_perf_metrics();
    assert!(map.compact_and_report(&mut storage).unwrap());
    let metrics = storage.perf_metrics();
    assert_eq!(metrics.frontier_reloads, 0);
    assert_eq!(metrics.frontier_open_wal_scans, 0);

    drop(storage);
    let mut reopened =
        Storage::<_, 512, 12>::open(&mut flash, crate::test_storage_memory()).unwrap();
    let mut reopened_map = LsmMap::<i32, i32, 4>::open(
        map.collection_id(),
        &mut reopened,
        crate::test_lsm_map_memory(),
    )
    .unwrap();
    assert_eq!(
        reopened_map
            .get(&mut reopened, &1, |_, value| *value)
            .unwrap(),
        None
    );
    assert_eq!(
        reopened_map
            .get(&mut reopened, &2, |_, value| *value)
            .unwrap(),
        Some(20)
    );
    assert_eq!(
        reopened_map
            .get(&mut reopened, &3, |_, value| *value)
            .unwrap(),
        Some(30)
    );
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-009` Map run descriptors MUST use inclusive lower and upper key bounds for
//# may_contain, integer helpers MUST advance offsets and reject short buffers, and manifest
//# capacity checks MUST reject excess runs.
#[test]
fn requirement_map_descriptor_and_integer_helpers_cover_bounds() {
    let bounded = MapRunDescriptor {
        source: MapRunSource::RunChain,
        generation: 3,
        first_region: 2,
        region_count: 1,
        approx_state_count: 2,
        lower_key: Some(10),
        upper_key: Some(20),
    };
    assert!(!bounded.may_contain(&9));
    assert!(bounded.may_contain(&10));
    assert!(bounded.may_contain(&15));
    assert!(bounded.may_contain(&20));
    assert!(!bounded.may_contain(&21));

    let mut bytes = [0u8; 12];
    let mut offset = 0usize;
    write_u32(&mut bytes, &mut offset, 0x0102_0304).unwrap();
    write_u64(&mut bytes, &mut offset, 0x1112_1314_1516_1718).unwrap();
    assert_eq!(offset, 12);
    assert!(matches!(
        write_u32(&mut bytes, &mut offset, 1),
        Err(MapError::BufferTooSmall)
    ));

    let mut offset = 0usize;
    assert_eq!(read_u32(&bytes, &mut offset).unwrap(), 0x0102_0304);
    assert_eq!(
        read_u64(&bytes, &mut offset).unwrap(),
        0x1112_1314_1516_1718
    );
    assert_eq!(offset, 12);
    assert!(matches!(
        read_u32(&bytes, &mut offset),
        Err(MapError::SerializationError)
    ));

    let mut exact = [0u8; size_of::<u32>()];
    let mut offset = 0usize;
    write_u32(&mut exact, &mut offset, 0x2122_2324).unwrap();
    assert_eq!(offset, exact.len());
    let mut offset = 0usize;
    assert_eq!(read_u32(&exact, &mut offset).unwrap(), 0x2122_2324);
    assert_eq!(offset, exact.len());

    ensure_manifest_run_capacity::<2>(CollectionId(97), 2).unwrap();
    assert!(matches!(
        ensure_manifest_run_capacity::<2>(CollectionId(97), 3),
        Err(MapStorageError::TooManyRuns {
            collection_id: CollectionId(97),
            max_runs: 2,
        })
    ));
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# `RING-IMPL-REGRESSION-010` Snapshot helpers MUST validate snapshot layout, preserve
//# set/delete/not-found lookup semantics, encode exact subranges, and reject out-of-bounds or
//# undersized buffers.
#[test]
fn requirement_snapshot_helpers_validate_ranges_and_lookup_semantics() {
    let (snapshot, snapshot_len) =
        snapshot_for_entries(&[(1, Some(10)), (2, None), (3, Some(30)), (4, Some(40))]);
    let snapshot = &snapshot[..snapshot_len];

    let (entry_count, entry_bytes_len, entries_offset, refs_start) =
        snapshot_parts(snapshot).unwrap();
    assert_eq!(entry_count, 4);
    assert_eq!(entries_offset, SNAPSHOT_HEADER_SIZE);
    assert_eq!(refs_start, snapshot_len - entry_count * ENTRY_REF_SIZE);
    assert_eq!(
        snapshot_len,
        entries_offset + entry_bytes_len + entry_count * ENTRY_REF_SIZE
    );

    assert_eq!(
        lookup_snapshot::<i32, i32>(snapshot, &0).unwrap(),
        LookupResult::NotFound
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(snapshot, &1).unwrap(),
        LookupResult::Set(10)
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(snapshot, &2).unwrap(),
        LookupResult::Deleted
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(snapshot, &4).unwrap(),
        LookupResult::Set(40)
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(snapshot, &5).unwrap(),
        LookupResult::NotFound
    );

    let empty_range_len = snapshot_range_len(snapshot, entry_count, 0).unwrap();
    assert_eq!(empty_range_len, SNAPSHOT_HEADER_SIZE);
    assert_eq!(
        snapshot_range_len(snapshot, 0, entry_count).unwrap(),
        snapshot_len
    );
    assert!(matches!(
        snapshot_range_len(snapshot, entry_count + 1, 0),
        Err(MapError::IndexOutOfBounds)
    ));
    assert!(matches!(
        snapshot_range_len(snapshot, entry_count - 1, 2),
        Err(MapError::IndexOutOfBounds)
    ));

    let range_len = snapshot_range_len(snapshot, 1, 2).unwrap();
    let mut range = [0u8; 512];
    assert_eq!(
        encode_snapshot_range_from_snapshot_into(snapshot, 1, 2, &mut range).unwrap(),
        range_len
    );
    let range = &range[..range_len];
    assert_eq!(
        lookup_snapshot::<i32, i32>(range, &1).unwrap(),
        LookupResult::NotFound
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(range, &2).unwrap(),
        LookupResult::Deleted
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(range, &3).unwrap(),
        LookupResult::Set(30)
    );
    assert!(matches!(
        encode_snapshot_range_from_snapshot_into(snapshot, 1, 2, &mut [0u8; 8]),
        Err(MapError::BufferTooSmall)
    ));

    let mut exact = vec![0u8; range_len];
    assert_eq!(
        encode_snapshot_range_from_snapshot_into(snapshot, 1, 2, exact.as_mut_slice()).unwrap(),
        range_len
    );
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# `RING-IMPL-REGRESSION-011` Snapshot and frontier search helpers MUST find even-window keys and
//# return the correct insertion position for missing keys.
#[test]
fn requirement_search_helpers_cover_even_windows_and_insert_positions() {
    let (snapshot, snapshot_len) = snapshot_for_entries(&[
        (1, Some(10)),
        (3, Some(30)),
        (5, Some(50)),
        (7, Some(70)),
        (9, Some(90)),
        (11, Some(110)),
    ]);
    let snapshot = &snapshot[..snapshot_len];

    assert_eq!(
        lookup_snapshot::<i32, i32>(snapshot, &11).unwrap(),
        LookupResult::Set(110)
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(snapshot, &6).unwrap(),
        LookupResult::NotFound
    );

    let mut buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 8>::new(
        CollectionId(92),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for key in [1, 3, 5, 7, 9, 11] {
        map.set_in_memory(key, key * 10).unwrap();
    }
    map.validate_loaded_state().unwrap();

    match map.find_index(&5).unwrap() {
        SearchResult::Found(index) => assert_eq!(index, RecordIndex::new(2)),
        result => panic!("unexpected search result: {result:?}"),
    }
    match map.find_index(&6).unwrap() {
        SearchResult::NotFound(index) => assert_eq!(index, RecordIndex::new(3)),
        result => panic!("unexpected search result: {result:?}"),
    }
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# `RING-IMPL-REGRESSION-012` Loading a snapshot MUST use entry reference offsets rather than
//# physical entry byte order so reversed adjacent entry storage still loads sorted keys.
#[test]
fn requirement_load_snapshot_accepts_reversed_adjacent_entry_storage() {
    let mut first_bytes = [0u8; 32];
    let first_len = encode_entry_into(&1i32, Some(&10i32), &mut first_bytes).unwrap();
    let mut second_bytes = [0u8; 32];
    let second_len = encode_entry_into(&2i32, Some(&20i32), &mut second_bytes).unwrap();

    let entry_bytes_len = first_len + second_len;
    let snapshot_len = SNAPSHOT_HEADER_SIZE + entry_bytes_len + 2 * ENTRY_REF_SIZE;
    let entries_offset = SNAPSHOT_HEADER_SIZE;
    let refs_start = entries_offset + entry_bytes_len;
    let mut snapshot = [0u8; 128];
    snapshot[..SNAPSHOT_MAGIC_SIZE].copy_from_slice(&SNAPSHOT_MAGIC);
    snapshot[SNAPSHOT_MAGIC_SIZE..SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE]
        .copy_from_slice(&2u32.to_le_bytes());
    let entry_bytes_len_offset = SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE;
    snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
        .copy_from_slice(&(entry_bytes_len as u32).to_le_bytes());

    snapshot[entries_offset..entries_offset + second_len]
        .copy_from_slice(&second_bytes[..second_len]);
    snapshot[entries_offset + second_len..entries_offset + entry_bytes_len]
        .copy_from_slice(&first_bytes[..first_len]);

    let first_start: RefType = (ENTRY_COUNT_SIZE + second_len).try_into().unwrap();
    let first_end: RefType = (ENTRY_COUNT_SIZE + entry_bytes_len).try_into().unwrap();
    let second_start: RefType = ENTRY_COUNT_SIZE.try_into().unwrap();
    let second_end: RefType = (ENTRY_COUNT_SIZE + second_len).try_into().unwrap();
    snapshot[refs_start..refs_start + ENTRY_REF_POINTER_SIZE]
        .copy_from_slice(&first_start.to_le_bytes());
    snapshot[refs_start + ENTRY_REF_POINTER_SIZE..refs_start + ENTRY_REF_SIZE]
        .copy_from_slice(&first_end.to_le_bytes());
    snapshot[refs_start + ENTRY_REF_SIZE..refs_start + ENTRY_REF_SIZE + ENTRY_REF_POINTER_SIZE]
        .copy_from_slice(&second_start.to_le_bytes());
    snapshot[refs_start + ENTRY_REF_SIZE + ENTRY_REF_POINTER_SIZE..refs_start + 2 * ENTRY_REF_SIZE]
        .copy_from_slice(&second_end.to_le_bytes());

    let mut buffer = [0u8; 128];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(93),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.load_snapshot(&snapshot[..snapshot_len]).unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(map.get_frontier(&2).unwrap(), Some(20));
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# `RING-IMPL-REGRESSION-013` Snapshot encoding MUST accept exact empty snapshot capacity and
//# snapshot decoding MUST reject invalid entry references.
#[test]
fn requirement_snapshot_encoding_accepts_exact_empty_capacity_and_rejects_invalid_refs() {
    let mut empty = [0u8; SNAPSHOT_HEADER_SIZE];
    let len = encode_snapshot_from_entries_into::<i32, i32>(&[], &mut empty).unwrap();
    assert_eq!(len, empty.len());
    assert_eq!(snapshot_parts(&empty).unwrap().0, 0);
    assert!(matches!(
        snapshot_parts(&[]),
        Err(MapError::SerializationError)
    ));

    let (mut snapshot, snapshot_len) = snapshot_for_entries(&[(1, Some(10)), (2, Some(20))]);
    let refs_offset = snapshot_len - ENTRY_REF_SIZE * 2;

    let start_ref: RefType = (ENTRY_COUNT_SIZE - 1).try_into().unwrap();
    let mut start = start_ref.to_le_bytes();
    snapshot[refs_offset..refs_offset + ENTRY_REF_POINTER_SIZE].copy_from_slice(&start);
    let mut dest_buffer = [0u8; 512];
    let mut dest = MapFrontier::<i32, i32, 8>::new(
        CollectionId(91),
        &mut dest_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    assert!(matches!(
        dest.load_snapshot(&snapshot[..snapshot_len]),
        Err(MapError::SerializationError)
    ));

    let (mut snapshot, snapshot_len) = snapshot_for_entries(&[(1, Some(10)), (2, Some(20))]);
    let start_ref: RefType = ENTRY_COUNT_SIZE.try_into().unwrap();
    start = start_ref.to_le_bytes();
    let end_ref: RefType = ENTRY_COUNT_SIZE.try_into().unwrap();
    let end = end_ref.to_le_bytes();
    snapshot[refs_offset..refs_offset + ENTRY_REF_POINTER_SIZE].copy_from_slice(&start);
    snapshot[refs_offset + ENTRY_REF_POINTER_SIZE..refs_offset + ENTRY_REF_SIZE]
        .copy_from_slice(&end);
    let mut dest_buffer = [0u8; 512];
    let mut dest = MapFrontier::<i32, i32, 8>::new(
        CollectionId(92),
        &mut dest_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    assert!(matches!(
        dest.load_snapshot(&snapshot[..snapshot_len]),
        Err(MapError::SerializationError)
    ));

    let (mut snapshot, snapshot_len) = snapshot_for_entries(&[(1, Some(10)), (2, Some(20))]);
    let refs_offset = snapshot_len - ENTRY_REF_SIZE * 2;
    let entry_bytes_len = snapshot_parts(&snapshot[..snapshot_len]).unwrap().1;
    let too_large_end_ref: RefType = (ENTRY_COUNT_SIZE + entry_bytes_len + 1).try_into().unwrap();
    snapshot[refs_offset + ENTRY_REF_POINTER_SIZE..refs_offset + ENTRY_REF_SIZE]
        .copy_from_slice(&too_large_end_ref.to_le_bytes());
    assert!(matches!(
        snapshot_entry_bytes(&snapshot[..snapshot_len], 0),
        Err(MapError::SerializationError)
    ));
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-014` Run cursors MUST advance segment positions correctly for ascending
//# and descending run chains, and compaction writers MUST report segment-fit and state-count
//# overflow errors.
#[test]
fn requirement_run_cursor_and_compaction_writer_helpers_cover_boundaries() {
    let run = MapRunDescriptor {
        source: MapRunSource::RunChain,
        generation: 1,
        first_region: 4,
        region_count: 2,
        approx_state_count: 2,
        lower_key: Some(1),
        upper_key: Some(2),
    };
    let mut cursor = RunEntryCursor::<i32, i32>::new(&run).unwrap();
    cursor.order = Some(RunChainOrder::Ascending);
    cursor.active_position = Some(0);
    cursor.advance_segment_position().unwrap();
    assert_eq!(cursor.next_segment_position, Some(1));
    cursor.active_position = Some(1);
    cursor.advance_segment_position().unwrap();
    assert_eq!(cursor.next_segment_position, None);

    cursor.order = Some(RunChainOrder::Descending);
    cursor.active_position = Some(1);
    cursor.advance_segment_position().unwrap();
    assert_eq!(cursor.next_segment_position, Some(0));

    let mut segment_buffer = [0u8; ENTRY_COUNT_SIZE];
    let mut segment_memory = MapFrontierMemory::<i32, 1>::new();
    let segment = MapFrontier::<i32, LargeValue, 1>::new(
        CollectionId(7),
        &mut segment_buffer,
        &mut segment_memory,
    )
    .unwrap();
    let mut writer = CompactionRunWriter::<i32, LargeValue, 1>::new(3, segment);
    writer.state_count = u32::MAX;
    assert!(matches!(
        writer.increment_state_count(),
        Err(MapStorageError::Map(MapError::SerializationError))
    ));
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# `RING-IMPL-REGRESSION-015` Entry reference and entry count helpers MUST preserve exact
//# serialized offsets and counts, and map checkpoints MUST restore prior frontier state while
//# rejecting undersized buffers.
#[test]
fn requirement_entry_refs_counts_and_checkpoints_preserve_exact_offsets() {
    let mut refs = [0u8; ENTRY_COUNT_SIZE + ENTRY_REF_SIZE * 3];
    EntryRef::write(
        &mut refs,
        RecordIndex::new(0),
        RecordOffset(4),
        RecordOffset(9),
    )
    .unwrap();
    EntryRef::write(
        &mut refs,
        RecordIndex::new(1),
        RecordOffset(9),
        RecordOffset(13),
    )
    .unwrap();

    assert_eq!(
        EntryRef::read(&refs, RecordIndex::new(0)).unwrap(),
        EntryRef { start: 4, end: 9 }
    );
    assert_eq!(
        EntryRef::read(&refs, RecordIndex::new(1)).unwrap(),
        EntryRef { start: 9, end: 13 }
    );
    assert_eq!(RecordIndex::new(1).previous(), RecordIndex::new(0));

    let count = EntryCount(7);
    count.write(&mut refs);
    assert_eq!(&refs[..ENTRY_COUNT_SIZE], 7u32.to_le_bytes().as_slice());
    assert_eq!(
        EntryCount::decode(refs[..ENTRY_COUNT_SIZE].try_into().unwrap()).0,
        7
    );

    let mut buffer = [0u8; 128];
    let mut map = MapFrontier::<i32, i32, 8>::new(
        CollectionId(93),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    map.set_in_memory(2, 20).unwrap();
    let mut scratch = [0u8; 128];
    let checkpoint = map.checkpoint_into(&mut scratch).unwrap();
    map.set_in_memory(1, 99).unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), Some(99));
    map.restore_from_checkpoint(checkpoint, &scratch).unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(map.get_frontier(&2).unwrap(), Some(20));
    assert!(matches!(
        map.checkpoint_into(&mut [0u8; 127]),
        Err(MapError::BufferTooSmall)
    ));
    assert!(matches!(
        map.restore_from_checkpoint(checkpoint, &[0u8; 127]),
        Err(MapError::BufferTooSmall)
    ));
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-016` Run segment payloads MUST round-trip generation, next-region link,
//# key bounds, and snapshot lookup semantics, and reject undersized or truncated payloads.
#[test]
fn requirement_run_segment_payload_round_trip_preserves_header_bounds_and_snapshot() {
    let entries = [
        Entry {
            key: 3i32,
            value: Some(30i32),
        },
        Entry {
            key: 5i32,
            value: Some(50i32),
        },
    ];
    let mut payload = [0u8; 256];
    let used = encode_run_segment_from_entries_into(&mut payload, 11, Some(9), &entries).unwrap();
    let view = parse_run_segment_payload(&payload[..used]).unwrap();

    assert_eq!(view.generation, 11);
    assert_eq!(view.next_region, Some(9));
    assert_eq!(i32::decode_key(view.lower_key).unwrap(), 3);
    assert_eq!(i32::decode_key(view.upper_key).unwrap(), 5);
    assert_eq!(
        lookup_snapshot::<i32, i32>(view.snapshot, &3).unwrap(),
        LookupResult::Set(30)
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(view.snapshot, &4).unwrap(),
        LookupResult::NotFound
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(view.snapshot, &5).unwrap(),
        LookupResult::Set(50)
    );
    assert!(matches!(
        encode_run_segment_from_entries_into::<i32, i32>(
            &mut [0u8; RUN_SEGMENT_FIXED_SIZE - 1],
            11,
            None,
            &entries
        ),
        Err(MapError::BufferTooSmall)
    ));
    assert!(matches!(
        parse_run_segment_payload(&payload[..used - 1]),
        Err(MapError::SerializationError)
    ));
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-017` Committed-region helpers MUST accept boundary-sized payload regions
//# and snapshot helpers MUST decode exact empty-snapshot payloads.
#[test]
fn requirement_committed_region_and_legacy_snapshot_helpers_accept_exact_boundaries() {
    const REGION_SIZE: usize = Header::ENCODED_LEN + FreePointerFooter::ENCODED_LEN;
    let metadata = StorageMetadata::new(REGION_SIZE as u32, 2, 0, 8, 0xff, 0xa5).unwrap();
    let mut flash = MockFlash::<REGION_SIZE, 2, 16>::new(0xff);
    init_user_region_header(&mut flash, 0, 1, CollectionId(95), MAP_REGION_V2_FORMAT);

    let mut region = [0u8; REGION_SIZE];
    let (header, payload) = read_committed_region(&mut flash, metadata, 0, &mut region).unwrap();
    assert_eq!(header.collection_id, CollectionId(95));
    assert_eq!(payload, &[] as &[u8]);

    const FULL_PAYLOAD_REGION_SIZE: usize = 64;
    let full_payload_metadata = StorageMetadata::new(
        (FULL_PAYLOAD_REGION_SIZE + FreePointerFooter::ENCODED_LEN) as u32,
        2,
        0,
        8,
        0xff,
        0xa5,
    )
    .unwrap();
    let mut flash = MockFlash::<FULL_PAYLOAD_REGION_SIZE, 2, 16>::new(0xff);
    init_user_region_header(&mut flash, 0, 1, CollectionId(96), MAP_REGION_V2_FORMAT);
    let mut region = [0u8; FULL_PAYLOAD_REGION_SIZE];
    let (_, payload) =
        read_committed_region(&mut flash, full_payload_metadata, 0, &mut region).unwrap();
    assert_eq!(
        payload.len(),
        FULL_PAYLOAD_REGION_SIZE - Header::ENCODED_LEN
    );

    let mut legacy_payload = [0u8; REGION_SNAPSHOT_LEN_SIZE + EMPTY_MAP_SNAPSHOT.len()];
    legacy_payload[..REGION_SNAPSHOT_LEN_SIZE]
        .copy_from_slice(&(EMPTY_MAP_SNAPSHOT.len() as u32).to_le_bytes());
    legacy_payload[REGION_SNAPSHOT_LEN_SIZE..].copy_from_slice(&EMPTY_MAP_SNAPSHOT);
    let mut map_buffer = [0u8; ENTRY_COUNT_SIZE];
    let mut map = MapFrontier::<i32, i32, 0>::new(
        CollectionId(96),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.load_region(&legacy_payload).unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), None);
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# `RING-IMPL-REGRESSION-018` Loading an empty snapshot MUST fit in a frontier buffer containing
//# only the entry-count header and MUST leave lookups empty.
#[test]
fn requirement_loading_empty_snapshot_can_exactly_fill_frontier_header() {
    let mut buffer = [0u8; ENTRY_COUNT_SIZE];
    let mut map = MapFrontier::<i32, i32, 0>::new(
        CollectionId(94),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();

    map.load_snapshot(&EMPTY_MAP_SNAPSHOT).unwrap();

    assert_eq!(map.get_frontier(&1).unwrap(), None);
    assert_eq!(map.snapshot_len().unwrap(), EMPTY_MAP_SNAPSHOT.len());
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-019` Map run selection and generation helpers MUST retain live region
//# totals for allocation, select compaction by live run count, and compute next generation from
//# run descriptors.
#[test]
fn requirement_run_descriptor_selection_and_generation_helpers_count_only_run_chains() {
    let mut buffer = [0u8; 128];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(97),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.runs
        .push(MapRunDescriptor {
            source: MapRunSource::RunChain,
            generation: 2,
            first_region: 10,
            region_count: 2,
            approx_state_count: 5,
            lower_key: Some(1),
            upper_key: Some(5),
        })
        .unwrap();
    map.runs
        .push(MapRunDescriptor {
            source: MapRunSource::RunChain,
            generation: 7,
            first_region: 12,
            region_count: 1,
            approx_state_count: 2,
            lower_key: Some(6),
            upper_key: Some(9),
        })
        .unwrap();
    assert_eq!(map.layer_count(), 2);
    assert_eq!(map.run_count(), 2);
    assert_eq!(map.live_run_region_count().unwrap(), 3);
    assert_eq!(map.next_run_generation(), 8);
    assert_eq!(map.selected_compaction_run_count(2).unwrap(), None);
    assert_eq!(map.selected_compaction_run_count(1).unwrap(), Some(2));
    assert_eq!(map.selected_compaction_state_count(0).unwrap(), 0);
    assert_eq!(map.selected_compaction_state_count(2).unwrap(), 7);
    assert!(matches!(
        map.selected_compaction_state_count(3),
        Err(MapError::IndexOutOfBounds)
    ));
}

fn push_test_run<const MAX_RUNS: usize>(
    map: &mut MapFrontier<i32, i32, MAX_RUNS>,
    generation: u64,
    region_count: u32,
    approx_state_count: u32,
) {
    map.runs
        .push(MapRunDescriptor {
            source: MapRunSource::RunChain,
            generation,
            first_region: u32::try_from(generation).unwrap(),
            region_count,
            approx_state_count,
            lower_key: Some(i32::try_from(generation).unwrap()),
            upper_key: Some(i32::try_from(generation + 1).unwrap()),
        })
        .unwrap();
}

//= spec/map.md#map-compaction-requirements
//= type=test
//# `RING-IMPL-REGRESSION-136` Run-target then greedy map compaction MUST select by live run count
//# rather than physical region count.
#[test]
fn requirement_compaction_selection_allows_one_run_spanning_many_regions() {
    let mut buffer = [0u8; 128];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(98),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    push_test_run(&mut map, 10, 128, 10_000);

    assert_eq!(map.live_run_region_count().unwrap(), 128);
    assert_eq!(map.selected_compaction_run_count(1).unwrap(), None);
}

//= spec/map.md#map-compaction-requirements
//= type=test
//# `RING-IMPL-REGRESSION-137` Run-target then greedy map compaction MUST select enough newest runs
//# to keep the post-compaction run manifest within the configured run target.
#[test]
fn requirement_compaction_selection_triggers_on_run_count_not_region_count() {
    let mut buffer = [0u8; 128];
    let mut map = MapFrontier::<i32, i32, 8>::new(
        CollectionId(99),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    push_test_run(&mut map, 10, 1, 3);
    push_test_run(&mut map, 9, 1, 4);
    push_test_run(&mut map, 8, 1, 100);

    assert_eq!(map.live_run_region_count().unwrap(), 3);
    assert_eq!(map.selected_compaction_run_count(2).unwrap(), Some(2));
}

//= spec/map.md#map-compaction-requirements
//= type=test
//# `RING-IMPL-REGRESSION-138` Run-target then greedy map compaction MUST account for a dirty
//# frontier being flushed as an additional run during compaction.
#[test]
fn requirement_compaction_selection_accounts_for_dirty_frontier_run() {
    let mut buffer = [0u8; 256];
    let mut map = MapFrontier::<i32, i32, 8>::new(
        CollectionId(100),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    push_test_run(&mut map, 10, 1, 3);
    push_test_run(&mut map, 9, 1, 4);
    push_test_run(&mut map, 8, 1, 5);
    push_test_run(&mut map, 7, 1, 100);
    map.set_in_memory(99, 100).unwrap();

    assert_eq!(map.selected_compaction_run_count(3).unwrap(), Some(3));

    let mut exact_target_buffer = [0u8; 256];
    let mut exact_target = MapFrontier::<i32, i32, 8>::new(
        CollectionId(103),
        &mut exact_target_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    push_test_run(&mut exact_target, 12, 1, 3);
    push_test_run(&mut exact_target, 11, 1, 4);
    push_test_run(&mut exact_target, 10, 1, 100);
    exact_target.set_in_memory(99, 100).unwrap();

    assert_eq!(
        exact_target.selected_compaction_run_count(3).unwrap(),
        Some(2)
    );
}

//= spec/map.md#map-compaction-requirements
//= type=test
//# `RING-IMPL-REGRESSION-139` Run-target then greedy map compaction MUST stop when the next older
//# run is at least twice the selected state count.
#[test]
fn requirement_compaction_selection_stops_at_half_size_heuristic() {
    let mut buffer = [0u8; 128];
    let mut map = MapFrontier::<i32, i32, 8>::new(
        CollectionId(101),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    push_test_run(&mut map, 10, 1, 5);
    push_test_run(&mut map, 9, 1, 5);
    push_test_run(&mut map, 8, 1, 20);
    push_test_run(&mut map, 7, 1, 20);

    assert_eq!(map.selected_compaction_run_count(3).unwrap(), Some(2));
}

//= spec/map.md#map-compaction-requirements
//= type=test
//# `RING-IMPL-REGRESSION-140` Run-target then greedy map compaction MUST merge equal-sized small
//# runs into a larger tier instead of repeatedly selecting only the minimum count.
#[test]
fn requirement_compaction_selection_merges_equal_sized_small_runs() {
    let mut buffer = [0u8; 128];
    let mut map = MapFrontier::<i32, i32, 8>::new(
        CollectionId(102),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for generation in (5..=10).rev() {
        push_test_run(&mut map, generation, 1, 10);
    }

    assert_eq!(map.selected_compaction_run_count(4).unwrap(), Some(6));
}

//= spec/map.md#snapshot-frontier-and-logical-map-requirements
//= type=test
//# `RING-IMPL-REGRESSION-020` Frontier range, region encoding, and checkpoint helpers MUST accept
//# exact-size buffers, preserve lookup state, and reject undersized or malformed inputs.
#[test]
fn requirement_frontier_range_region_and_checkpoint_helpers_accept_exact_buffers() {
    let mut buffer = [0u8; 256];
    let mut map = MapFrontier::<i32, i32, 8>::new(
        CollectionId(98),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for key in 1..=4 {
        map.set_in_memory(key, key * 10).unwrap();
    }

    let range_len = map.snapshot_range_len_from_frontier(1, 2).unwrap();
    let mut range = vec![0u8; range_len];
    assert_eq!(
        map.encode_snapshot_range_into(1, 2, range.as_mut_slice())
            .unwrap(),
        range_len
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(&range, &1).unwrap(),
        LookupResult::NotFound
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(&range, &2).unwrap(),
        LookupResult::Set(20)
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(&range, &3).unwrap(),
        LookupResult::Set(30)
    );
    assert_eq!(
        map.snapshot_range_len_from_frontier(4, 0).unwrap(),
        SNAPSHOT_HEADER_SIZE
    );
    assert!(matches!(
        map.snapshot_range_len_from_frontier(5, 0),
        Err(MapError::IndexOutOfBounds)
    ));

    let region_len = map.region_len().unwrap();
    let mut region = vec![0u8; region_len];
    assert_eq!(
        map.encode_region_into(region.as_mut_slice()).unwrap(),
        region_len
    );
    let mut restored_buffer = [0u8; 256];
    let mut restored = MapFrontier::<i32, i32, 8>::new(
        CollectionId(98),
        &mut restored_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    restored.load_region(&region).unwrap();
    assert_eq!(restored.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(restored.get_frontier(&4).unwrap(), Some(40));

    assert!(matches!(
        map.encode_snapshot_range_into(1, 2, &mut [0u8; 8]),
        Err(MapError::BufferTooSmall)
    ));
    assert!(matches!(
        restored.load_region(&region[..REGION_SNAPSHOT_LEN_SIZE - 1]),
        Err(MapError::SerializationError)
    ));

    let mut undersized_buffer = [0u8; ENTRY_COUNT_SIZE - 1];
    assert!(matches!(
        MapFrontier::<i32, i32, 0>::new(
            CollectionId(98),
            &mut undersized_buffer,
            crate::test_map_frontier_memory()
        ),
        Err(MapError::BufferTooSmall)
    ));
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-021` Manifest descriptor loading MUST preserve run metadata and reject too
//# many runs, zero-length run chains, and truncated descriptor payloads.
#[test]
fn requirement_manifest_descriptor_loading_validates_counts_bounds_and_lengths() {
    let mut source_buffer = [0u8; 128];
    let mut source = MapFrontier::<i32, i32, 2>::new(
        CollectionId(99),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source
        .runs
        .push(MapRunDescriptor {
            source: MapRunSource::RunChain,
            generation: 3,
            first_region: 7,
            region_count: 2,
            approx_state_count: 5,
            lower_key: Some(1),
            upper_key: Some(9),
        })
        .unwrap();

    let mut manifest = [0u8; 128];
    let used = source
        .encode_manifest_into(&mut manifest, None, None)
        .unwrap();

    let mut dest_buffer = [0u8; 128];
    let mut dest = MapFrontier::<i32, i32, 1>::new(
        CollectionId(99),
        &mut dest_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    dest.load_manifest_descriptors(&manifest[..used], CollectionId(99), 4)
        .unwrap();
    assert_eq!(dest.run_count(), 1);
    assert_eq!(dest.runs[0].generation, 3);
    assert_eq!(dest.runs[0].first_region, 7);
    assert_eq!(dest.runs[0].region_count, 2);
    assert_eq!(dest.runs[0].lower_key, Some(1));
    assert_eq!(dest.runs[0].upper_key, Some(9));

    let mut too_many = [0u8; size_of::<u32>()];
    too_many.copy_from_slice(&2u32.to_le_bytes());
    assert!(matches!(
        dest.load_manifest_descriptors(&too_many, CollectionId(99), 4),
        Err(MapStorageError::TooManyRuns {
            collection_id: CollectionId(99),
            max_runs: 1,
        })
    ));

    let mut zero_region_count = manifest;
    zero_region_count[size_of::<u32>() + size_of::<u64>() + size_of::<u32>()
        ..size_of::<u32>() + size_of::<u64>() + 2 * size_of::<u32>()]
        .copy_from_slice(&0u32.to_le_bytes());
    assert!(matches!(
        dest.load_manifest_descriptors(&zero_region_count[..used], CollectionId(99), 4),
        Err(MapStorageError::InvalidManifest {
            collection_id: CollectionId(99),
            region_index: 7,
        })
    ));
    assert!(matches!(
        dest.load_manifest_descriptors(&manifest[..used - 1], CollectionId(99), 4),
        Err(MapStorageError::InvalidManifest {
            collection_id: CollectionId(99),
            region_index: 7,
        }) | Err(MapStorageError::Map(MapError::SerializationError))
    ));
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-022` Snapshot run segment helpers MUST plan at least one region and encode
//# requested snapshot subranges with generation, next-region link, bounds, and lookup semantics.
#[test]
fn requirement_snapshot_run_segment_helpers_plan_and_encode_exact_subranges() {
    let (snapshot, snapshot_len) =
        snapshot_for_entries(&[(1, Some(10)), (2, Some(20)), (3, Some(30))]);
    let snapshot = &snapshot[..snapshot_len];
    let mut workspace = StorageWorkspace::<256>::new();

    let planned =
        MapFrontier::<i32, i32, 8>::planned_snapshot_run_region_count(&mut workspace, snapshot, 4)
            .unwrap();
    assert!(planned >= 1);

    let mut payload = [0u8; 128];
    let used = MapFrontier::<i32, i32, 8>::encode_run_segment_from_snapshot_into(
        &mut payload,
        4,
        Some(11),
        snapshot,
        1,
        2,
    )
    .unwrap();
    let view = parse_run_segment_payload(&payload[..used]).unwrap();
    assert_eq!(view.generation, 4);
    assert_eq!(view.next_region, Some(11));
    assert_eq!(i32::decode_key(view.lower_key).unwrap(), 2);
    assert_eq!(i32::decode_key(view.upper_key).unwrap(), 3);
    assert_eq!(
        lookup_snapshot::<i32, i32>(view.snapshot, &1).unwrap(),
        LookupResult::NotFound
    );
    assert_eq!(
        lookup_snapshot::<i32, i32>(view.snapshot, &2).unwrap(),
        LookupResult::Set(20)
    );
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-023` Snapshot run planning and storage writes MUST split snapshots that
//# exceed one committed run payload across multiple run regions, return a descriptor with the exact
//# state count and lower and upper keys, and return no descriptor for an empty snapshot.
#[test]
fn requirement_snapshot_run_planning_and_storage_write_cover_multi_region_snapshots() {
    let mut source_buffer = [0u8; 2048];
    let mut source = MapFrontier::<i32, LargeValue>::new(
        CollectionId(102),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for key in 0..10 {
        source.set_in_memory(key, large_value(key as u8)).unwrap();
    }
    let mut snapshot = [0u8; 2048];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();
    let snapshot = &snapshot[..snapshot_len];

    let mut planning_workspace = StorageWorkspace::<256>::new();
    let planned = MapFrontier::<i32, LargeValue>::planned_snapshot_run_region_count(
        &mut planning_workspace,
        snapshot,
        17,
    )
    .unwrap();
    assert!(planned > 1);

    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 16;
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(CollectionId(102)).unwrap();

    let mut target_buffer = [0u8; 128];
    let target = MapFrontier::<i32, LargeValue>::new(
        CollectionId(102),
        &mut target_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    let written = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            target.write_snapshot_run_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
                snapshot,
                17,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(written.approx_state_count, 10);
    assert!(written.region_count > 1);
    assert_eq!(written.lower_key, Some(0));
    assert_eq!(written.upper_key, Some(9));

    let mut empty = [0u8; SNAPSHOT_HEADER_SIZE];
    let empty_len = encode_snapshot_from_entries_into::<i32, LargeValue>(&[], &mut empty).unwrap();
    assert!(storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            target.write_snapshot_run_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
                &empty[..empty_len],
                18,
            )
        })
        .unwrap()
        .is_none());
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-024` Frontier run planning MUST count every committed run payload segment
//# required for frontier contents that exceed one run-region payload.
#[test]
fn requirement_frontier_run_planning_counts_all_committed_payload_segments() {
    let mut buffer = [0u8; 2048];
    let mut map = MapFrontier::<i32, LargeValue>::new(
        CollectionId(105),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for key in 0..10 {
        map.set_in_memory(key, large_value(key as u8)).unwrap();
    }

    let mut workspace = StorageWorkspace::<256>::new();
    assert!(
        map.planned_frontier_run_region_count(&mut workspace, 23)
            .unwrap()
            > 1
    );
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-025` Reclaiming map run regions MUST move all tracked run-chain regions to
//# the storage free-list tail.
#[test]
fn requirement_reclaim_run_regions_moves_run_segments_to_free_list_tail() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 8;

    let collection_id = CollectionId(106);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(collection_id).unwrap();

    let (snapshot, snapshot_len) = snapshot_for_entries(&[(1, Some(10))]);
    let mut buffer = [0u8; REGION_SIZE];
    let mut map = MapFrontier::<i32, i32>::new(
        collection_id,
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    let run = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            map.write_snapshot_run_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
                &snapshot[..snapshot_len],
                1,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(run.region_count, 1);
    let first_region = run.first_region;
    let previous_tail = storage.free_list_tail();
    map.runs.push(run).unwrap();

    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.begin_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
                flash,
                workspace,
                collection_id,
            )?;
            runtime.commit_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
                flash,
                workspace,
                collection_id,
            )?;
            map.reclaim_run_regions::<REGION_SIZE, REGION_COUNT, _, 8>(runtime, flash, workspace)?;
            runtime.finish_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
                flash,
                workspace,
                collection_id,
            )?;
            Ok::<(), MapStorageError>(())
        })
        .unwrap();
    assert_ne!(storage.free_list_tail(), previous_tail);
    assert_eq!(storage.free_list_tail(), Some(first_region));
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-026` Committing a map manifest MUST reclaim the previous manifest region
//# and retain only run-chain descriptors in the manifest state.
#[test]
fn requirement_commit_manifest_reclaims_previous_manifest_and_retains_only_run_chains() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 12;
    const MAX_RUNS: usize = 3;

    let collection_id = CollectionId(107);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(collection_id).unwrap();

    let mut buffer = [0u8; REGION_SIZE];
    let mut map = MapFrontier::<i32, i32, MAX_RUNS>::new(
        collection_id,
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    let first_manifest = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            map.flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
            )
        })
        .unwrap();
    let second_manifest = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            map.commit_manifest_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
                None,
            )
        })
        .unwrap();
    assert_ne!(second_manifest, first_manifest);
    assert_eq!(storage.free_list_tail(), Some(first_manifest));
    assert!(map
        .runs
        .iter()
        .all(|run| run.source == MapRunSource::RunChain));
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-027` Flushing a map to storage MUST commit a manifest-backed run-chain
//# basis and reject flushes that exceed configured run capacity.
#[test]
fn requirement_flush_to_storage_commits_manifest_and_enforces_run_capacity() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 12;
    let collection_id = CollectionId(108);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(collection_id).unwrap();

    let mut buffer = [0u8; REGION_SIZE];
    let mut map = MapFrontier::<i32, i32, 2>::new(
        collection_id,
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(2, 20).unwrap();
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            map.flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
            )
        })
        .unwrap();
    assert_eq!(map.runs.len(), 1);

    let mut too_many_buffer = [0u8; REGION_SIZE];
    let mut too_many = MapFrontier::<i32, i32, 1>::new(
        collection_id,
        &mut too_many_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    too_many
        .runs
        .push(MapRunDescriptor {
            source: MapRunSource::RunChain,
            generation: 11,
            first_region: 6,
            region_count: 1,
            approx_state_count: 1,
            lower_key: Some(1),
            upper_key: Some(1),
        })
        .unwrap();
    too_many.set_in_memory(3, 30).unwrap();
    assert!(matches!(
        storage.with_runtime_io_workspace(|runtime, flash, workspace| {
            too_many.flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
            )
        }),
        Err(MapStorageError::TooManyRuns {
            collection_id: found,
            max_runs: 1,
        }) if found == collection_id
    ));
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-028` Committed run storage helpers MUST read run segment bounds and next
//# links only from matching map-run regions and reject non-run region headers.
#[test]
fn requirement_committed_run_storage_helpers_validate_headers_and_next_links() {
    const REGION_SIZE: usize = 256;
    let collection_id = CollectionId(100);
    let metadata = StorageMetadata::new(REGION_SIZE as u32, 4, 0, 8, 0xff, 0xa5).unwrap();
    let mut flash = MockFlash::<REGION_SIZE, 4, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let entries = [
        Entry {
            key: 4i32,
            value: Some(40i32),
        },
        Entry {
            key: 8i32,
            value: Some(80i32),
        },
    ];
    let mut payload = [0u8; REGION_SIZE];
    let used = encode_run_segment_from_entries_into(&mut payload, 12, Some(3), &entries).unwrap();
    write_committed_payload(
        &mut flash,
        2,
        1,
        collection_id,
        MAP_RUN_V2_FORMAT,
        &payload[..used],
    );

    assert_eq!(
        read_run_segment_bounds::<i32, REGION_SIZE, _>(
            collection_id,
            12,
            &mut flash,
            &mut workspace,
            2
        )
        .unwrap(),
        (4, 8)
    );
    assert_eq!(
        read_run_segment_next_region::<REGION_SIZE, _>(
            collection_id,
            12,
            &mut flash,
            &mut workspace,
            2
        )
        .unwrap(),
        Some(3)
    );

    init_user_region_header(&mut flash, 2, 2, collection_id, MAP_REGION_V2_FORMAT);
    assert!(matches!(
        read_run_segment_bounds::<i32, REGION_SIZE, _>(
            collection_id,
            12,
            &mut flash,
            &mut workspace,
            2
        ),
        Err(MapStorageError::InvalidRun {
            collection_id: found,
            region_index: 2,
        }) if found == collection_id
    ));
    let _ = metadata;
}

//= spec/map.md#run-manifest-and-committed-map-region-requirements
//= type=test
//# `RING-IMPL-REGRESSION-029` Map lookup helpers MUST read manifest run chains, and
//# head-reference checks MUST report manifest and run regions as reachable.
#[test]
fn requirement_lookup_and_head_reference_helpers_follow_manifest_runs() {
    const REGION_SIZE: usize = 256;
    let collection_id = CollectionId(101);
    let metadata = StorageMetadata::new(REGION_SIZE as u32, 5, 0, 8, 0xff, 0xa5).unwrap();
    let mut flash = MockFlash::<REGION_SIZE, 5, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();

    let mut map_buffer = [0u8; REGION_SIZE];
    let map = MapFrontier::<i32, i32, 8>::new(
        collection_id,
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();

    let run_entries = [
        Entry {
            key: 5i32,
            value: Some(50i32),
        },
        Entry {
            key: 6i32,
            value: Some(60i32),
        },
    ];
    let mut run_payload = [0u8; REGION_SIZE];
    let run_used =
        encode_run_segment_from_entries_into(&mut run_payload, 14, None, &run_entries).unwrap();
    write_committed_payload(
        &mut flash,
        2,
        2,
        collection_id,
        MAP_RUN_V2_FORMAT,
        &run_payload[..run_used],
    );

    let run = MapRunDescriptor {
        source: MapRunSource::RunChain,
        generation: 14,
        first_region: 2,
        region_count: 1,
        approx_state_count: 2,
        lower_key: Some(5),
        upper_key: Some(6),
    };
    flash.clear_operations();
    assert_eq!(
        map.lookup_run_chain::<REGION_SIZE, _>(&mut flash, &mut workspace, &run, &6)
            .unwrap(),
        LookupResult::Set(60)
    );
    let lookup_reads: Vec<_> = flash
        .operations()
        .iter()
        .copied()
        .filter(|operation| matches!(operation, MockOperation::ReadRegion { .. }))
        .collect();
    assert!(lookup_reads.iter().all(|operation| match operation {
        MockOperation::ReadRegion { len, .. } => *len < REGION_SIZE,
        _ => true,
    }));
    assert!(lookup_reads.iter().any(|operation| matches!(
        operation,
        MockOperation::ReadRegion {
            offset: 0,
            len,
            ..
        } if *len == Header::ENCODED_LEN + RUN_SEGMENT_FIXED_SIZE
    )));
    assert_eq!(
        map.lookup_run_chain::<REGION_SIZE, _>(&mut flash, &mut workspace, &run, &7)
            .unwrap(),
        LookupResult::NotFound
    );

    let mut manifest_map_buffer = [0u8; REGION_SIZE];
    let mut manifest_map = MapFrontier::<i32, i32, 8>::new(
        collection_id,
        &mut manifest_map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    manifest_map.runs.push(run).unwrap();
    let mut manifest_payload = [0u8; REGION_SIZE];
    let manifest_used = manifest_map
        .encode_manifest_into(&mut manifest_payload, None, None)
        .unwrap();
    write_committed_payload(
        &mut flash,
        3,
        3,
        collection_id,
        MAP_MANIFEST_V2_FORMAT,
        &manifest_payload[..manifest_used],
    );

    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    assert!(map_head_region_references_region::<REGION_SIZE, _>(
        &mut flash,
        &mut workspace,
        metadata,
        collection_id,
        3,
        2
    )
    .unwrap());
    assert!(map_head_region_references_region::<REGION_SIZE, _>(
        &mut flash,
        &mut workspace,
        metadata,
        collection_id,
        3,
        3
    )
    .unwrap());
    assert!(!map_head_region_references_region::<REGION_SIZE, _>(
        &mut flash,
        &mut workspace,
        metadata,
        collection_id,
        3,
        4
    )
    .unwrap());
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-030` Opening a map from storage MUST replay only WAL records for the
//# requested collection and ignore updates and drop records for other collections.
#[test]
fn requirement_open_from_storage_uses_only_target_collection_wal_records() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;
    let target_id = CollectionId(103);
    let other_id = CollectionId(104);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(target_id).unwrap();
    storage.create_map(other_id).unwrap();

    let mut payload = [0u8; 128];
    storage
        .append_map_update::<i32, i32>(target_id, &MapUpdate::Set { key: 1, value: 10 })
        .unwrap();
    storage
        .append_map_update::<i32, i32>(other_id, &MapUpdate::Set { key: 1, value: 99 })
        .unwrap();
    storage.drop_map(other_id).unwrap();

    let mut target_buffer = [0u8; REGION_SIZE];
    let opened = storage
        .open_map::<i32, i32, 8>(
            target_id,
            &mut target_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(opened.get_frontier(&1).unwrap(), Some(10));
}

proptest! {

    //= spec/map.md#snapshot-frontier-and-logical-map-requirements
    //= type=test
    //# `RING-IMPL-REGRESSION-031` Entry reference serialization MUST preserve independent start and end offsets for distinct record indexes.
    #[test]
    fn requirement_check_entry_ref(
        (buffer, index1, index2) in vec_and_indexes(),
        start1 in 0..RefType::MAX,
        end1 in 0..RefType::MAX,
        start2 in 0..RefType::MAX,
        end2 in 0..RefType::MAX
    ) {
        if index1 == index2 {
            return Ok(());
        }

        let index1 = RecordIndex(index1);
        let index2 = RecordIndex(index2);
        let start1 = RecordOffset(start1 as usize);
        let end1 = RecordOffset(end1 as usize);
        let start2 = RecordOffset(start2 as usize);
        let end2 = RecordOffset(end2 as usize);

        let mut buffer = buffer;

        EntryRef::write(&mut buffer, index1, start1, end1).unwrap();
        EntryRef::write(&mut buffer, index2, start2, end2).unwrap();
        let entry1 = EntryRef::read(&buffer, index1).unwrap();
        let entry2 = EntryRef::read(&buffer, index2).unwrap();

        assert_eq!(entry1.start, start1.0 as RefType);
        assert_eq!(entry1.end, end1.0 as RefType);

        assert_eq!(entry2.start, start2.0 as RefType);
        assert_eq!(entry2.end, end2.0 as RefType);

    }


}

//= spec/implementation.md#panic-requirements
//= type=test
//# `RING-IMPL-PANIC-004` If a condition is believed to be impossible by construction, the
//# implementation SHOULD encode that proof in types, control flow, or checked validation before the
//# point of use rather than relying on a panic as a backstop.
#[test]
fn requirement_set_returns_buffer_too_small_when_map_storage_is_exhausted() {
    let mut buffer = [0u8; 8];
    let mut map = MapFrontier::<i32, i32>::new(
        CollectionId(27),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();

    assert!(matches!(
        map.set_in_memory(1, 10),
        Err(MapError::BufferTooSmall)
    ));
}

//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-003` If the configured capacities are insufficient to open the store or complete
//# an operation, the implementation MUST fail explicitly with a capacity-related error rather than
//# silently allocate or truncate state.
#[test]
fn requirement_encode_snapshot_returns_buffer_too_small_when_output_capacity_is_insufficient() {
    const BUFFER_SIZE: usize = 64;

    let mut map_buffer = [0u8; BUFFER_SIZE];
    let mut map = MapFrontier::<i32, i32>::new(
        CollectionId(28),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    map.set_in_memory(2, 20).unwrap();

    let mut snapshot = [0u8; 8];
    assert!(matches!(
        map.encode_snapshot_into(&mut snapshot),
        Err(MapError::BufferTooSmall)
    ));
}

//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-003` Loading a valid snapshot payload MUST reconstruct
//# the same logical key/value visibility encoded by that payload.
#[test]
fn requirement_snapshot_round_trip_restores_logical_state() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(7);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source =
        MapFrontier::<i32, i32>::new(id, &mut source_buffer, crate::test_map_frontier_memory())
            .unwrap();
    source.set_in_memory(1, 10).unwrap();
    source.set_in_memory(2, 20).unwrap();
    source.delete_in_memory(1).unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();

    let mut dest_buffer = [0u8; BUFFER_SIZE];
    let mut restored =
        MapFrontier::<i32, i32>::new(id, &mut dest_buffer, crate::test_map_frontier_memory())
            .unwrap();
    restored.load_snapshot(&snapshot[..snapshot_len]).unwrap();

    assert_eq!(restored.get_frontier(&1).unwrap(), None);
    assert_eq!(restored.get_frontier(&2).unwrap(), Some(20));
}

//= spec/map.md#update-payload-format
//= type=test
//# `MAP-UPDATE-001` A map update payload MUST be the exact `postcard`
//# serialization of `MapUpdate<K, V>`.
#[test]
fn requirement_encoded_update_payload_matches_postcard_serialization() {
    let update = MapUpdate::Set {
        key: 5i32,
        value: 42i32,
    };
    let mut encoded = [0u8; 64];
    let used = MapFrontier::<i32, i32, 4>::encode_update_into(&update, &mut encoded).unwrap();

    let mut expected = [0u8; 64];
    let expected_used = to_slice(&update, &mut expected).unwrap().len();
    assert_eq!(&encoded[..used], &expected[..expected_used]);

    let update = MapUpdate::Delete { key: 5i32 };
    let used = MapFrontier::<i32, i32, 4>::encode_update_into(&update, &mut encoded).unwrap();
    let expected_used = to_slice(&update, &mut expected).unwrap().len();
    assert_eq!(&encoded[..used], &expected[..expected_used]);
}

//= spec/map.md#update-payload-format
//= type=test
//# `MAP-UPDATE-002` Applying a `Set` update payload MUST make the key
//# visible with the supplied value, and applying a `Delete` update payload
//# MUST make the key absent from the frontier.
#[test]
fn requirement_update_payload_round_trip_applies_frontier_change() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(9);

    let mut buffer = [0u8; BUFFER_SIZE];
    let mut map =
        MapFrontier::<i32, i32>::new(id, &mut buffer, crate::test_map_frontier_memory()).unwrap();

    let mut set_payload = [0u8; 64];
    let set_len = MapFrontier::<i32, i32>::encode_update_into(
        &MapUpdate::Set { key: 5, value: 42 },
        &mut set_payload,
    )
    .unwrap();
    map.apply_update_payload(&set_payload[..set_len]).unwrap();
    assert_eq!(map.get_frontier(&5).unwrap(), Some(42));

    let mut delete_payload = [0u8; 64];
    let delete_len = MapFrontier::<i32, i32>::encode_update_into(
        &MapUpdate::Delete { key: 5 },
        &mut delete_payload,
    )
    .unwrap();
    map.apply_update_payload(&delete_payload[..delete_len])
        .unwrap();
    assert_eq!(map.get_frontier(&5).unwrap(), None);
}

//= spec/map.md#update-payload-format
//= type=test
//# `MAP-UPDATE-003` Rolling back an in-memory map update MUST restore the
//# previous frontier metadata and visible key/value state.
#[test]
fn requirement_update_payload_undo_restores_found_update_and_delete() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(91);

    let mut buffer = [0u8; BUFFER_SIZE];
    let mut map =
        MapFrontier::<i32, i32>::new(id, &mut buffer, crate::test_map_frontier_memory()).unwrap();
    map.set_in_memory(1, 10).unwrap();
    map.set_in_memory(2, 20).unwrap();

    let mut payload = [0u8; 64];
    let update_len = MapFrontier::<i32, i32>::encode_update_into(
        &MapUpdate::Set { key: 1, value: 99 },
        &mut payload,
    )
    .unwrap();
    let mut scratch = [0u8; BUFFER_SIZE];
    let undo = map
        .apply_update_payload_with_undo(&payload[..update_len], &mut scratch)
        .unwrap();
    assert_eq!(undo.saved_bytes_len(), ENTRY_REF_SIZE);
    assert_eq!(map.get_frontier(&1).unwrap(), Some(99));
    map.restore_from_mutation_undo(undo, &scratch).unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(map.get_frontier(&2).unwrap(), Some(20));

    let delete_len =
        MapFrontier::<i32, i32>::encode_update_into(&MapUpdate::Delete { key: 2 }, &mut payload)
            .unwrap();
    let undo = map
        .apply_update_payload_with_undo(&payload[..delete_len], &mut scratch)
        .unwrap();
    assert_eq!(map.get_frontier(&2).unwrap(), None);
    map.restore_from_mutation_undo(undo, &scratch).unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(map.get_frontier(&2).unwrap(), Some(20));
}

//= spec/map.md#update-payload-format
//= type=test
//# `MAP-UPDATE-004` Rolling back an in-memory map insert MUST make the
//# inserted key unreachable while preserving older keys.
#[test]
fn requirement_update_payload_undo_restores_end_and_middle_insert() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(92);

    let mut buffer = [0u8; BUFFER_SIZE];
    let mut map =
        MapFrontier::<i32, i32>::new(id, &mut buffer, crate::test_map_frontier_memory()).unwrap();
    map.set_in_memory(1, 10).unwrap();

    let mut payload = [0u8; 64];
    let mut scratch = [0u8; BUFFER_SIZE];
    let end_insert_len = MapFrontier::<i32, i32>::encode_update_into(
        &MapUpdate::Set { key: 3, value: 30 },
        &mut payload,
    )
    .unwrap();
    let undo = map
        .apply_update_payload_with_undo(&payload[..end_insert_len], &mut scratch)
        .unwrap();
    assert_eq!(undo.saved_bytes_len(), 0);
    assert_eq!(map.get_frontier(&3).unwrap(), Some(30));
    map.restore_from_mutation_undo(undo, &scratch).unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(map.get_frontier(&3).unwrap(), None);

    map.set_in_memory(3, 30).unwrap();
    let middle_insert_len = MapFrontier::<i32, i32>::encode_update_into(
        &MapUpdate::Set { key: 2, value: 20 },
        &mut payload,
    )
    .unwrap();
    let undo = map
        .apply_update_payload_with_undo(&payload[..middle_insert_len], &mut scratch)
        .unwrap();
    assert_eq!(undo.saved_bytes_len(), ENTRY_REF_SIZE);
    assert_eq!(map.get_frontier(&2).unwrap(), Some(20));
    map.restore_from_mutation_undo(undo, &scratch).unwrap();
    assert_eq!(map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(map.get_frontier(&2).unwrap(), None);
    assert_eq!(map.get_frontier(&3).unwrap(), Some(30));
}

//= spec/map.md#empty-logical-state
//= type=test
//# `MAP-STATE-001` After successful durable creation of a map
//# collection, opening that collection
//# MUST yield an empty logical map.
#[test]
fn requirement_empty_map_open_matches_new_map_state() {
    assert_empty_map_open_matches_new_map_state();
}

fn assert_empty_map_open_matches_new_map_state() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;
    let id = CollectionId(70);

    let mut empty_buffer = [0u8; REGION_SIZE];
    let empty =
        MapFrontier::<i32, i32>::new(id, &mut empty_buffer, crate::test_map_frontier_memory())
            .unwrap();
    assert_eq!(empty.get_frontier(&1).unwrap(), None);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(id).unwrap();

    drop(storage);
    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened_map = reopened
        .open_map::<i32, i32, 4>(id, &mut reopen_buffer, crate::test_map_frontier_memory())
        .unwrap();

    assert_eq!(reopened_map.get_frontier(&1).unwrap(), None);
}

//= spec/map.md#empty-logical-state
//= type=test
//# `MAP-STATE-002` `LsmMap::new` MUST construct the same empty logical
//# state used by an empty durable map basis.
#[test]
fn requirement_empty_map_new_matches_empty_durable_basis_state() {
    assert_empty_map_open_matches_new_map_state();
}

//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-001` A map snapshot payload MUST be encoded as
//# `[magic:"MAP2"][entry_count:u32 little-endian][entry_bytes_len:u32
//# little-endian][entry_bytes][entry_refs]`.
#[test]
fn requirement_snapshot_encoding_stores_header_compact_entries_and_refs() {
    assert_snapshot_encoding_stores_header_compact_entries_and_refs();
}

fn assert_snapshot_encoding_stores_header_compact_entries_and_refs() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(71);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source =
        MapFrontier::<i32, i32>::new(id, &mut source_buffer, crate::test_map_frontier_memory())
            .unwrap();
    source.set_in_memory(5, 50).unwrap();
    source.set_in_memory(2, 20).unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();

    assert_eq!(&snapshot[..SNAPSHOT_MAGIC_SIZE], SNAPSHOT_MAGIC.as_slice());
    let entry_count_offset = SNAPSHOT_MAGIC_SIZE;
    let entry_count = u32::from_le_bytes(
        snapshot[entry_count_offset..entry_count_offset + SNAPSHOT_ENTRY_COUNT_SIZE]
            .try_into()
            .unwrap(),
    );
    assert_eq!(entry_count, 2);

    let entry_bytes_len_offset = SNAPSHOT_MAGIC_SIZE + SNAPSHOT_ENTRY_COUNT_SIZE;
    let entry_bytes_len = usize::try_from(u32::from_le_bytes(
        snapshot[entry_bytes_len_offset..entry_bytes_len_offset + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    assert_eq!(
        snapshot_len,
        SNAPSHOT_HEADER_SIZE
            + entry_bytes_len
            + usize::try_from(entry_count).unwrap() * ENTRY_REF_SIZE
    );

    let refs_start = snapshot_len - usize::try_from(entry_count).unwrap() * ENTRY_REF_SIZE;
    let mut first_start = [0u8; ENTRY_REF_POINTER_SIZE];
    let mut first_end = [0u8; ENTRY_REF_POINTER_SIZE];
    let mut second_start = [0u8; ENTRY_REF_POINTER_SIZE];
    let mut second_end = [0u8; ENTRY_REF_POINTER_SIZE];
    first_start.copy_from_slice(&snapshot[refs_start..refs_start + ENTRY_REF_POINTER_SIZE]);
    first_end.copy_from_slice(
        &snapshot[refs_start + ENTRY_REF_POINTER_SIZE..refs_start + ENTRY_REF_SIZE],
    );
    second_start.copy_from_slice(
        &snapshot
            [refs_start + ENTRY_REF_SIZE..refs_start + ENTRY_REF_SIZE + ENTRY_REF_POINTER_SIZE],
    );
    second_end.copy_from_slice(
        &snapshot
            [refs_start + ENTRY_REF_SIZE + ENTRY_REF_POINTER_SIZE..refs_start + ENTRY_REF_SIZE * 2],
    );

    let first_start = usize::try_from(RefType::from_le_bytes(first_start)).unwrap();
    let first_end = usize::try_from(RefType::from_le_bytes(first_end)).unwrap();
    let second_start = usize::try_from(RefType::from_le_bytes(second_start)).unwrap();
    let second_end = usize::try_from(RefType::from_le_bytes(second_end)).unwrap();
    assert_eq!(first_start, ENTRY_COUNT_SIZE);
    assert_eq!(first_end, second_start);
    assert_eq!(second_end, ENTRY_COUNT_SIZE + entry_bytes_len);

    let entry_bytes_offset = SNAPSHOT_HEADER_SIZE;
    let first_entry: Entry<i32, i32> = encoded_entry_to_entry(
        &snapshot[entry_bytes_offset + first_start - ENTRY_COUNT_SIZE
            ..entry_bytes_offset + first_end - ENTRY_COUNT_SIZE],
    )
    .unwrap();
    let second_entry: Entry<i32, i32> = encoded_entry_to_entry(
        &snapshot[entry_bytes_offset + second_start - ENTRY_COUNT_SIZE
            ..entry_bytes_offset + second_end - ENTRY_COUNT_SIZE],
    )
    .unwrap();
    assert!(first_entry.key < second_entry.key);
}

//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-002` Snapshot encoding MUST write `entry_count` as the
//# number of visible entries in the logical map and `entry_bytes_len` as
//# the exact byte length of the compact serialized entry data that follows.
#[test]
fn requirement_snapshot_encoding_records_entry_count_and_entry_bytes_len() {
    assert_snapshot_encoding_stores_header_compact_entries_and_refs();
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-013` That collection specification MUST define, at
//# minimum: the empty logical state established by `new_collection`; the
//# exact bytes and interpretation of every supported committed-region
//# `collection_format`; the exact bytes and interpretation of `snapshot`
//# payloads; the exact bytes and interpretation of `update` payloads; the
//# rules for applying updates and merging a durable basis with the
//# in-memory frontier; and the collection-specific validation rules used
//# when loading a basis or replaying WAL payloads.
#[test]
fn requirement_map_collection_format_covers_empty_state_snapshot_update_region_and_validation() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(10);

    {
        let mut empty_buffer = [0u8; BUFFER_SIZE];
        let empty =
            MapFrontier::<i32, i32>::new(id, &mut empty_buffer, crate::test_map_frontier_memory())
                .unwrap();
        assert_eq!(empty.get_frontier(&1).unwrap(), None);
    }

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source =
        MapFrontier::<i32, i32>::new(id, &mut source_buffer, crate::test_map_frontier_memory())
            .unwrap();
    source.set_in_memory(1, 10).unwrap();
    source.set_in_memory(2, 20).unwrap();

    let mut update_payload = [0u8; 64];
    let update_len = MapFrontier::<i32, i32>::encode_update_into(
        &MapUpdate::Set { key: 2, value: 99 },
        &mut update_payload,
    )
    .unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();
    let mut from_snapshot_buffer = [0u8; BUFFER_SIZE];
    let mut from_snapshot = MapFrontier::<i32, i32>::new(
        id,
        &mut from_snapshot_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    from_snapshot
        .load_snapshot(&snapshot[..snapshot_len])
        .unwrap();
    assert_eq!(from_snapshot.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(from_snapshot.get_frontier(&2).unwrap(), Some(20));

    from_snapshot
        .apply_update_payload(&update_payload[..update_len])
        .unwrap();
    assert_eq!(from_snapshot.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(from_snapshot.get_frontier(&2).unwrap(), Some(99));

    let mut region = [0u8; BUFFER_SIZE];
    let region_len = source.encode_region_into(&mut region).unwrap();
    assert_eq!(
        usize::try_from(u32::from_le_bytes(region[..4].try_into().unwrap())).unwrap(),
        snapshot_len
    );
    let mut from_region_buffer = [0u8; BUFFER_SIZE];
    let mut from_region = MapFrontier::<i32, i32>::new(
        id,
        &mut from_region_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    from_region.load_region(&region[..region_len]).unwrap();
    assert_eq!(from_region.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(from_region.get_frontier(&2).unwrap(), Some(20));

    let mut invalid_snapshot = snapshot;
    let refs_offset = snapshot_len - ENTRY_REF_SIZE * 2;
    let mut first_ref = [0u8; ENTRY_REF_SIZE];
    let mut second_ref = [0u8; ENTRY_REF_SIZE];
    first_ref.copy_from_slice(&invalid_snapshot[refs_offset..refs_offset + ENTRY_REF_SIZE]);
    second_ref.copy_from_slice(
        &invalid_snapshot[refs_offset + ENTRY_REF_SIZE..refs_offset + ENTRY_REF_SIZE * 2],
    );
    invalid_snapshot[refs_offset..refs_offset + ENTRY_REF_SIZE].copy_from_slice(&second_ref);
    invalid_snapshot[refs_offset + ENTRY_REF_SIZE..refs_offset + ENTRY_REF_SIZE * 2]
        .copy_from_slice(&first_ref);

    let mut invalid_buffer = [0u8; BUFFER_SIZE];
    let mut invalid =
        MapFrontier::<i32, i32>::new(id, &mut invalid_buffer, crate::test_map_frontier_memory())
            .unwrap();
    assert!(matches!(
        invalid.load_snapshot(&invalid_snapshot[..snapshot_len]),
        Err(MapError::SerializationError)
    ));
}

//= spec/map.md#committed-head-format
//= type=test
//# `MAP-REGION-001` A committed map head with
//# `collection_format = MAP_MANIFEST_V2_FORMAT` MUST encode a manifest that
//# describes the live immutable map run set.
#[test]
fn requirement_region_round_trip_restores_logical_state() {
    assert_region_round_trip_restores_logical_state();
}

fn assert_region_round_trip_restores_logical_state() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(11);

    let mut flash = MockFlash::<BUFFER_SIZE, 8, 4096>::new(0xff);
    let mut storage = Storage::<_, BUFFER_SIZE, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(id).unwrap();

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source =
        MapFrontier::<i32, i32>::new(id, &mut source_buffer, crate::test_map_frontier_memory())
            .unwrap();
    source.set_in_memory(3, 30).unwrap();
    source.set_in_memory(4, 40).unwrap();

    let manifest_region = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            source.flush_to_storage::<BUFFER_SIZE, 8, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
            )
        })
        .unwrap();

    let mut committed_region = [0u8; BUFFER_SIZE];
    storage.with_io_workspace(|flash, _workspace| {
        flash
            .read_region(manifest_region, 0, committed_region.len(), |bytes| {
                committed_region.copy_from_slice(bytes);
            })
            .unwrap()
    });
    let header = Header::decode(&committed_region[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(header.collection_id, id);
    assert_eq!(header.collection_format, MAP_MANIFEST_V2_FORMAT);

    let mut dest_buffer = [0u8; BUFFER_SIZE];
    let restored = storage
        .open_map::<i32, i32, 4>(id, &mut dest_buffer, crate::test_map_frontier_memory())
        .unwrap();

    assert_eq!(
        restored
            .get::<BUFFER_SIZE, _>(storage.backing, &mut storage.memory.workspace, &3)
            .unwrap(),
        Some(30)
    );
    assert_eq!(
        restored
            .get::<BUFFER_SIZE, _>(storage.backing, &mut storage.memory.workspace, &4)
            .unwrap(),
        Some(40)
    );
    assert_eq!(restored.run_count(), 1);
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-014` For non-WAL collections, the pair `(collection_type, collection_format)` MUST
//# identify a unique committed region payload format.
#[test]
fn requirement_non_wal_collection_format_pair_identifies_map_region_payloads() {
    assert_region_round_trip_restores_logical_state();
}

//= spec/map.md#committed-head-format
//= type=test
//# `MAP-REGION-002` A live map collection MUST NOT use the retired
//# single-region snapshot format as its committed durable basis.
#[test]
fn requirement_region_payload_prefix_matches_embedded_snapshot_len() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut storage = Storage::<_, 512, 5>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(60)).unwrap();
    let region_index = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.reserve_next_region::<512, 5, _>(
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
            )
        })
        .unwrap();
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.write_committed_region::<512, 5, _>(
                flash,
                workspace,
                region_index,
                CollectionId(60),
                MAP_REGION_V2_FORMAT,
                &EMPTY_MAP_SNAPSHOT,
            )
        })
        .unwrap();
    storage
        .append_head(CollectionId(60), CollectionType::MAP_CODE, region_index)
        .unwrap();

    let mut buffer = [0u8; 512];
    let result = storage.open_map::<i32, i32, 4>(
        CollectionId(60),
        &mut buffer,
        crate::test_map_frontier_memory(),
    );
    assert!(matches!(
        result,
        Err(MapStorageError::UnsupportedRegionFormat {
            collection_id: CollectionId(60),
            region_index: actual_region,
            actual: MAP_REGION_V2_FORMAT,
        }) if actual_region == region_index
    ));
}

//= spec/map.md#committed-head-format
//= type=test
//# `MAP-REGION-003` Loading a valid committed manifest head MUST recover
//# the same logical state as reading the manifest-described run chains.
#[test]
fn requirement_region_loading_matches_embedded_snapshot_loading() {
    assert_region_round_trip_restores_logical_state();
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-002` Each collection MUST be represented as
//# log-structured state: new durable collection state is written to WAL
//# records or fresh committed regions, and live committed collection
//# regions MUST NOT be rewritten in place.
#[test]
fn requirement_map_updates_append_new_head_records_and_replacement_reclaims_the_old_tail_region() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 6;

    let mut buffer = [0u8; REGION_SIZE];
    let mut map = MapFrontier::<i32, i32>::new(
        CollectionId(60),
        &mut buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    let first_end = map.next_record_offset.0;
    let first_prefix = map.map[..first_end].to_vec();

    map.set_in_memory(1, 20).unwrap();
    assert!(map.next_record_offset.0 > first_end);
    assert_eq!(&map.map[..first_end], first_prefix.as_slice());
    assert_eq!(map.get_frontier(&1).unwrap(), Some(20));

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(map.id()).unwrap();

    let first_region = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            map.flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
            )
        })
        .unwrap();

    map.delete_in_memory(1).unwrap();
    let second_region = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            map.flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
            )
        })
        .unwrap();

    assert_ne!(second_region, first_region);
    assert_eq!(
        storage.runtime().collections()[0].basis(),
        crate::StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(storage.free_list_tail(), Some(first_region));
}

//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-002` Opening or loading a live map collection MUST
//# reject retained collection state whose `collection_type` is not
//# `MAP_CODE`.
#[test]
fn requirement_open_from_storage_rejects_live_collections_with_a_non_map_collection_type() {
    assert_open_from_storage_rejects_live_collections_with_a_non_map_collection_type();
}

fn assert_open_from_storage_rejects_live_collections_with_a_non_map_collection_type() {
    let mut flash = MockFlash::<512, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(61), MAP_REGION_V2_FORMAT);
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        crate::WalRecord::Head {
            collection_id: CollectionId(61),
            collection_type: crate::CollectionType::CHANNEL_CODE,
            region_index: 2,
        },
    );

    let mut workspace = StorageWorkspace::<512>::new();
    let runtime = crate::storage::open::<512, 4, _, 8>(&mut flash, &mut workspace).unwrap();
    let mut basis_scratch = [0u8; 512];
    let mut reopen_buffer = [0u8; 512];
    let result = MapFrontier::<i32, i32, 4>::open_from_storage::<512, 4, _, 8>(
        &runtime,
        &mut flash,
        &mut workspace,
        &mut basis_scratch,
        CollectionId(61),
        &mut reopen_buffer,
        crate::test_map_frontier_memory(),
    );

    assert!(matches!(
        result,
        Err(MapStorageError::CollectionTypeMismatch {
            collection_id: CollectionId(61),
            expected: crate::CollectionType::MAP_CODE,
            actual: Some(crate::CollectionType::CHANNEL_CODE),
        })
    ));
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-008` Every later retained type-bearing record for that
//# collection MUST carry the same `collection_type`, otherwise replay
//# must treat the mismatch as corruption.
#[test]
fn requirement_replay_rejects_retained_type_bearing_record_type_mismatch() {
    assert_open_from_storage_rejects_live_collections_with_a_non_map_collection_type();
}

//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-001` Map snapshot loading MUST reject payloads whose
//# lengths, entry ranges, ordering, or entry decoding are invalid.
#[test]
fn requirement_load_snapshot_rejects_unsorted_entry_refs() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(12);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source =
        MapFrontier::<i32, i32>::new(id, &mut source_buffer, crate::test_map_frontier_memory())
            .unwrap();
    source.set_in_memory(1, 10).unwrap();
    source.set_in_memory(2, 20).unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();

    let refs_offset = snapshot_len - ENTRY_REF_SIZE * 2;
    let mut first_ref = [0u8; ENTRY_REF_SIZE];
    let mut second_ref = [0u8; ENTRY_REF_SIZE];
    first_ref.copy_from_slice(&snapshot[refs_offset..refs_offset + ENTRY_REF_SIZE]);
    second_ref
        .copy_from_slice(&snapshot[refs_offset + ENTRY_REF_SIZE..refs_offset + ENTRY_REF_SIZE * 2]);
    snapshot[refs_offset..refs_offset + ENTRY_REF_SIZE].copy_from_slice(&second_ref);
    snapshot[refs_offset + ENTRY_REF_SIZE..refs_offset + ENTRY_REF_SIZE * 2]
        .copy_from_slice(&first_ref);

    let mut dest_buffer = [0u8; BUFFER_SIZE];
    let mut restored =
        MapFrontier::<i32, i32>::new(id, &mut dest_buffer, crate::test_map_frontier_memory())
            .unwrap();
    assert!(matches!(
        restored.load_snapshot(&snapshot[..snapshot_len]),
        Err(MapError::SerializationError)
    ));
}

//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-004` Snapshot loaders MUST treat `entry_refs` as an
//# ordered, non-overlapping description of the compact entry bytes.
#[test]
fn requirement_load_snapshot_rejects_overlapping_entry_refs() {
    const BUFFER_SIZE: usize = 512;
    let id = CollectionId(13);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source =
        MapFrontier::<i32, i32>::new(id, &mut source_buffer, crate::test_map_frontier_memory())
            .unwrap();
    source.set_in_memory(1, 10).unwrap();
    source.set_in_memory(2, 20).unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();

    let refs_offset = snapshot_len - ENTRY_REF_SIZE * 2;
    let mut first_start_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
    let mut second_end_bytes = [0u8; ENTRY_REF_POINTER_SIZE];
    first_start_bytes.copy_from_slice(&snapshot[refs_offset..refs_offset + ENTRY_REF_POINTER_SIZE]);
    second_end_bytes.copy_from_slice(
        &snapshot[refs_offset + ENTRY_REF_SIZE + ENTRY_REF_POINTER_SIZE
            ..refs_offset + ENTRY_REF_SIZE * 2],
    );
    snapshot[refs_offset..refs_offset + ENTRY_REF_POINTER_SIZE].copy_from_slice(&first_start_bytes);
    snapshot[refs_offset + ENTRY_REF_POINTER_SIZE..refs_offset + ENTRY_REF_SIZE]
        .copy_from_slice(&second_end_bytes);

    let mut dest_buffer = [0u8; BUFFER_SIZE];
    let mut restored =
        MapFrontier::<i32, i32>::new(id, &mut dest_buffer, crate::test_map_frontier_memory())
            .unwrap();
    assert!(matches!(
        restored.load_snapshot(&snapshot[..snapshot_len]),
        Err(MapError::SerializationError)
    ));
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-015` Each storage-managed resident mutable collection
//# frontier MUST have usable byte capacity exactly equal to the
//# committed-region payload capacity of one configured durable region.
#[test]
fn requirement_mutable_map_frontier_capacity_is_bounded_by_its_configured_buffer() {
    let min_capacity_for_three_updates = (ENTRY_COUNT_SIZE..256)
        .find(|capacity| {
            let mut buffer = vec![0u8; *capacity];
            let mut map = MapFrontier::<u16, u16, 8>::new(
                CollectionId(30),
                &mut buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
            map.set_in_memory(1, 10).is_ok()
                && map.set_in_memory(1, 20).is_ok()
                && map.set_in_memory(1, 30).is_ok()
        })
        .expect("expected a bounded capacity for three updates");

    let mut bounded_buffer = vec![0u8; min_capacity_for_three_updates];
    let mut bounded_map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(31),
        &mut bounded_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    bounded_map.set_in_memory(1, 10).unwrap();
    bounded_map.set_in_memory(1, 20).unwrap();
    bounded_map.set_in_memory(1, 30).unwrap();
    assert!(matches!(
        bounded_map.set_in_memory(1, 40),
        Err(MapError::BufferTooSmall)
    ));

    let min_capacity_for_four_updates = (ENTRY_COUNT_SIZE..256)
        .find(|capacity| {
            let mut buffer = vec![0u8; *capacity];
            let mut map = MapFrontier::<u16, u16, 8>::new(
                CollectionId(32),
                &mut buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
            map.set_in_memory(1, 10).is_ok()
                && map.set_in_memory(1, 20).is_ok()
                && map.set_in_memory(1, 30).is_ok()
                && map.set_in_memory(1, 40).is_ok()
        })
        .expect("expected a bounded capacity for four updates");

    let mut larger_buffer = vec![0u8; min_capacity_for_four_updates];
    let mut larger_map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(32),
        &mut larger_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    larger_map.set_in_memory(1, 10).unwrap();
    larger_map.set_in_memory(1, 20).unwrap();
    larger_map.set_in_memory(1, 30).unwrap();
    larger_map.set_in_memory(1, 40).unwrap();
}

//= spec/map.md#merge-and-frontier-rules
//= type=test
//# `MAP-MERGE-001` When opening a live map collection, the retained
//# durable basis MUST be selected from the replay-tracked empty basis,
//# retained snapshot basis, or retained committed-region basis, and any
//# later retained update payloads for that collection MUST then be applied
//# in replay order.
#[test]
fn requirement_storage_snapshot_replay_restores_map_frontier() {
    assert_storage_snapshot_replay_restores_map_frontier();
}

fn assert_storage_snapshot_replay_restores_map_frontier() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage
        .append_new_collection(CollectionId(7), CollectionType::MAP_CODE)
        .unwrap();

    let mut snapshot_buffer = [0u8; REGION_SIZE];
    let mut source = MapFrontier::<i32, i32>::new(
        CollectionId(7),
        &mut snapshot_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source.set_in_memory(1, 10).unwrap();
    source.set_in_memory(2, 20).unwrap();
    storage.snapshot_map(&source).unwrap();

    let mut update_payload = [0u8; 64];
    let update_len = MapFrontier::<i32, i32>::encode_update_into(
        &MapUpdate::Set { key: 2, value: 99 },
        &mut update_payload,
    )
    .unwrap();
    storage
        .append_update(CollectionId(7), &update_payload[..update_len])
        .unwrap();

    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened = storage
        .open_map::<i32, i32, 8>(
            CollectionId(7),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();

    assert_eq!(reopened.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(reopened.get_frontier(&2).unwrap(), Some(99));
}

//= spec/map.md#merge-and-frontier-rules
//= type=test
//# `MAP-MERGE-002` Later retained updates MUST take precedence over
//# older values from the retained basis for the same key.
#[test]
fn requirement_later_retained_map_updates_override_durable_basis_values() {
    assert_storage_snapshot_replay_restores_map_frontier();
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-003` The frontier MUST take precedence over older values in the durable basis.
#[test]
fn requirement_map_frontier_takes_precedence_over_older_durable_basis_values() {
    assert_storage_snapshot_replay_restores_map_frontier();
}

//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-003` Opening or loading a live map collection MUST
//# reject retained committed-region bases whose `collection_format` is not
//# a supported map head format.
#[test]
fn requirement_open_from_storage_rejects_unsupported_committed_region_format() {
    let mut flash = MockFlash::<512, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(72), MAP_RUN_V2_FORMAT + 1);
    let wal_offset = metadata.wal_record_area_offset().unwrap();
    append_wal_record(
        &mut flash,
        metadata,
        0,
        wal_offset,
        crate::WalRecord::Head {
            collection_id: CollectionId(72),
            collection_type: crate::CollectionType::MAP_CODE,
            region_index: 2,
        },
    );

    let mut workspace = StorageWorkspace::<512>::new();
    let runtime = crate::storage::open::<512, 4, _, 8>(&mut flash, &mut workspace).unwrap();
    let mut basis_scratch = [0u8; 512];
    let mut reopen_buffer = [0u8; 512];
    let result = MapFrontier::<i32, i32, 4>::open_from_storage::<512, 4, _, 8>(
        &runtime,
        &mut flash,
        &mut workspace,
        &mut basis_scratch,
        CollectionId(72),
        &mut reopen_buffer,
        crate::test_map_frontier_memory(),
    );

    assert!(matches!(
        result,
        Err(MapStorageError::UnsupportedRegionFormat {
            collection_id: CollectionId(72),
            region_index: 2,
            actual,
        }) if actual == MAP_RUN_V2_FORMAT + 1
    ));
}

fn assert_open_rejects_invalid_retained_region_snapshot_and_update_payloads(
    region_collection_id: CollectionId,
    snapshot_collection_id: CollectionId,
    update_collection_id: CollectionId,
) {
    {
        let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage = Storage::<_, 512, 5>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        storage.create_map(region_collection_id).unwrap();

        let region_index = storage
            .with_runtime_io_workspace(|runtime, flash, workspace| {
                runtime.reserve_next_region::<512, 5, _>(
                    flash,
                    workspace,
                    &mut heapless::Vec::new(),
                    &mut heapless::Vec::new(),
                    &mut crate::storage::WalHeadReclaimPlan::empty(),
                    &mut crate::startup::StartupOpenPlan::empty(),
                )
            })
            .unwrap();
        storage
            .with_runtime_io_workspace(|runtime, flash, workspace| {
                runtime.write_committed_region::<512, 5, _>(
                    flash,
                    workspace,
                    region_index,
                    region_collection_id,
                    MAP_REGION_V2_FORMAT,
                    &[1, 2, 3],
                )
            })
            .unwrap();
        storage
            .append_head(region_collection_id, CollectionType::MAP_CODE, region_index)
            .unwrap();

        drop(storage);
        let mut reopened =
            Storage::<_, 512, 5>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4>(
            region_collection_id,
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        );
        assert!(matches!(
            result,
            Err(MapStorageError::UnsupportedRegionFormat {
                collection_id,
                region_index: actual_region,
                actual: MAP_REGION_V2_FORMAT,
            }) if collection_id == region_collection_id && actual_region == region_index
        ));
    }

    {
        let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage = Storage::<_, 512, 4>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        storage.create_map(snapshot_collection_id).unwrap();
        storage
            .append_snapshot(snapshot_collection_id, CollectionType::MAP_CODE, &[1])
            .unwrap();

        drop(storage);
        let mut reopened =
            Storage::<_, 512, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4>(
            snapshot_collection_id,
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        );
        assert!(matches!(
            result,
            Err(MapStorageError::Map(MapError::SerializationError))
        ));
    }

    {
        let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage = Storage::<_, 512, 4>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();

        storage.create_map(update_collection_id).unwrap();
        storage
            .append_update(update_collection_id, &[0xff])
            .unwrap();

        drop(storage);
        let mut reopened =
            Storage::<_, 512, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<i32, i32, 4>(
            update_collection_id,
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        );
        assert!(matches!(
            result,
            Err(MapStorageError::Map(MapError::SerializationError))
        ));
    }
}

//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-004` Opening a live map collection MUST reject
//# retained committed-region payloads, snapshot payloads, or update
//# payloads that fail map-specific validation.
#[test]
fn requirement_open_from_storage_rejects_invalid_retained_region_snapshot_and_update_payloads() {
    assert_open_rejects_invalid_retained_region_snapshot_and_update_payloads(
        CollectionId(40),
        CollectionId(41),
        CollectionId(42),
    );
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-016A` Typed collection open or load MUST fail if retained
//# committed-region payloads, retained `snapshot` payloads, or retained
//# post-basis `update` payloads are unsupported or invalid under that
//# collection's normative specification.
#[test]
fn requirement_replay_rejects_invalid_live_collection_payloads() {
    assert_open_rejects_invalid_retained_region_snapshot_and_update_payloads(
        CollectionId(43),
        CollectionId(44),
        CollectionId(45),
    );
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-032` Storage WAL record visitation for maps MUST expose typed
//# new-collection and snapshot records for map collections in durable order.
#[test]
fn requirement_storage_visit_wal_records_exposes_map_collection_records() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage
        .append_new_collection(CollectionId(7), CollectionType::MAP_CODE)
        .unwrap();

    let mut snapshot_buffer = [0u8; REGION_SIZE];
    let mut source = MapFrontier::<i32, i32>::new(
        CollectionId(7),
        &mut snapshot_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source.set_in_memory(1, 10).unwrap();
    storage.snapshot_map(&source).unwrap();

    let mut seen = [(crate::WalRecordType::WalRecovery, CollectionId(0)); 2];
    let mut count = 0usize;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<REGION_SIZE, _, (), _>(
                flash,
                workspace,
                |_flash, record| {
                    let collection_id = match record {
                        crate::WalRecord::NewCollection { collection_id, .. }
                        | crate::WalRecord::Update { collection_id, .. }
                        | crate::WalRecord::Snapshot { collection_id, .. }
                        | crate::WalRecord::Head { collection_id, .. }
                        | crate::WalRecord::DropCollection { collection_id } => collection_id,
                        _ => CollectionId(0),
                    };
                    if count < seen.len() {
                        seen[count] = (record.record_type(), collection_id);
                    }
                    count += 1;
                    Ok(())
                },
            )
        })
        .unwrap();

    assert_eq!(
        seen,
        [
            (crate::WalRecordType::NewCollection, CollectionId(7)),
            (crate::WalRecordType::Snapshot, CollectionId(7)),
        ]
    );
}

//= spec/map.md#merge-and-frontier-rules
//= type=test
//# `MAP-MERGE-003` Flushing a mutable map frontier MUST write a new
//# immutable committed region rather than rewriting the previous live
//# region in place.
#[test]
fn requirement_storage_region_flush_restores_map_basis() {
    assert_storage_region_flush_restores_map_basis();
}

fn assert_storage_region_flush_restores_map_basis() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage
        .append_new_collection(CollectionId(9), CollectionType::MAP_CODE)
        .unwrap();

    let mut map_buffer = [0u8; REGION_SIZE];
    let mut map = MapFrontier::<i32, i32>::new(
        CollectionId(9),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(5, 50).unwrap();
    map.set_in_memory(7, 70).unwrap();

    let region_index = storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            map.flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8>(
                runtime,
                flash,
                workspace,
                &mut heapless::Vec::new(),
                &mut heapless::Vec::new(),
                &mut crate::storage::WalHeadReclaimPlan::empty(),
                &mut crate::startup::StartupOpenPlan::empty(),
            )
        })
        .unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        crate::StartupCollectionBasis::Region(region_index)
    );
    assert_eq!(storage.ready_region(), None);

    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened = storage
        .open_map::<i32, i32, 8>(
            CollectionId(9),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();

    assert_eq!(
        storage
            .with_io_workspace(
                |flash, workspace| reopened.get::<REGION_SIZE, _>(flash, workspace, &5)
            )
            .unwrap(),
        Some(50)
    );
    assert_eq!(
        storage
            .with_io_workspace(
                |flash, workspace| reopened.get::<REGION_SIZE, _>(flash, workspace, &7)
            )
            .unwrap(),
        Some(70)
    );
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-005` Every user collection MUST remain log-structured:
//# flushing mutable state writes new immutable committed region state
//# instead of rewriting existing live region state in place. An LSM-style
//# layout with manifest-described immutable runs is one valid way to
//# satisfy this requirement.
#[test]
fn requirement_map_flush_preserves_log_structured_collection_writes() {
    assert_storage_region_flush_restores_map_basis();
}

fn k_v_vec(count: usize) -> impl Strategy<Value = Vec<(i32, i32)>> {
    prop::collection::vec((0..i32::MAX, 0..i32::MAX), count..(count + 1))
}

proptest! {

    //= spec/map.md#snapshot-frontier-and-logical-map-requirements
    //= type=test
    //# `RING-IMPL-REGRESSION-033` Map read/write operations MUST return the latest inserted values for generated key/value workloads.
    #[test]
    fn requirement_test_read_write(entries in k_v_vec(100)) {
        const BUFFER_SIZE: usize = 4096;
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let id = CollectionId(1);

        let mut map = MapFrontier::<_, _>::new(id, buffer.as_mut_slice(), crate::test_map_frontier_memory())
            .expect("Could not construct MapFrontier.");

        let (mut last_key, mut last_value) = entries[0];
        map.set_in_memory(last_key, last_value).expect("insert failed");

        for (key, value) in entries[1..].iter() {
            map.set_in_memory(*key, *value).expect("insert failed");
            if *key != last_key {
                let got = map
                .get_frontier(&last_key)
                .expect("could not get key")
                .expect("got None for key");

                assert_eq!(got, last_value);
            }

            last_key = *key;
            last_value = *value;
        }
    }

}

proptest! {

    //= spec/map.md#snapshot-frontier-and-logical-map-requirements
    //= type=test
    //# `RING-IMPL-REGRESSION-034` Map write/delete operations MUST remove deleted keys while preserving non-deleted entries for generated workloads.
    #[test]
    fn requirement_test_write_delete(entries in k_v_vec(5), delete in 0usize..5) {
        const BUFFER_SIZE: usize = 2048;
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let id = CollectionId(1);

        let mut map = MapFrontier::<_, _>::new(id, buffer.as_mut_slice(), crate::test_map_frontier_memory())
            .expect("Could not construct MapFrontier.");



        for (key, value) in entries.iter() {
            map.set_in_memory(*key, *value).expect("insert failed");
        }

        let delete_key = entries[delete].0;

        map.delete_in_memory(delete_key).expect("delete failed");


        for (key, value) in entries.iter() {


            let got = map
            .get_frontier(key)
            .expect("could not get key");

            if *key == delete_key {
                assert_eq!(got, None);
            } else {
                assert_eq!(got, Some(*value));
            }

        }
    }

}
