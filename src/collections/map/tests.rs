use super::*;
extern crate std;
use crate::wal_record::encode_record_into;
use crate::{MockFlash, Storage, StorageWorkspace};
use postcard::to_slice;
use proptest::prelude::*;
use std::{vec, vec::Vec};

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

proptest! {

    #[test]
    fn check_entry_ref(
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
//# `RING-IMPL-PANIC-004` If a condition is believed to be impossible by construction, the implementation SHOULD encode that proof in types, control flow, or checked validation before the point of use rather than relying on a panic as a backstop.
#[test]
fn set_returns_buffer_too_small_when_map_storage_is_exhausted() {
    const MAX_INDEXES: usize = 4;

    let mut buffer = [0u8; 8];
    let mut map = LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(27), &mut buffer).unwrap();

    assert!(matches!(map.set(1, 10), Err(MapError::BufferTooSmall)));
}

//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-003` If the configured capacities are insufficient to open the store or complete an operation, the implementation MUST fail explicitly with a capacity-related error rather than silently allocate or truncate state.
#[test]
fn encode_snapshot_returns_buffer_too_small_when_output_capacity_is_insufficient() {
    const BUFFER_SIZE: usize = 64;
    const MAX_INDEXES: usize = 4;

    let mut map_buffer = [0u8; BUFFER_SIZE];
    let mut map = LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(28), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    map.set(2, 20).unwrap();

    let mut snapshot = [0u8; 8];
    assert!(matches!(
        map.encode_snapshot_into(&mut snapshot),
        Err(MapError::BufferTooSmall)
    ));
}

//= spec/ring.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-003` The frontier MUST take precedence over older values in the durable basis.
//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-003` Loading a valid snapshot payload MUST reconstruct
//# the same logical key/value visibility encoded by that payload.
#[test]
fn snapshot_round_trip_restores_logical_state() {
    const BUFFER_SIZE: usize = 512;
    const MAX_INDEXES: usize = 4;
    let id = CollectionId(7);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    source.set(2, 20).unwrap();
    source.delete(1).unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();

    let mut dest_buffer = [0u8; BUFFER_SIZE];
    let mut restored = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut dest_buffer).unwrap();
    restored.load_snapshot(&snapshot[..snapshot_len]).unwrap();

    assert_eq!(restored.get(&1).unwrap(), None);
    assert_eq!(restored.get(&2).unwrap(), Some(20));
}

//= spec/map.md#update-payload-format
//= type=test
//# `MAP-UPDATE-001` A map update payload MUST be the exact `postcard`
//# serialization of `MapUpdate<K, V>`.
#[test]
fn encoded_update_payload_matches_postcard_serialization() {
    let update = MapUpdate::Set {
        key: 5i32,
        value: 42i32,
    };
    let mut encoded = [0u8; 64];
    let used = LsmMap::<i32, i32, 4>::encode_update_into(&update, &mut encoded).unwrap();

    let mut expected = [0u8; 64];
    let expected_used = to_slice(&update, &mut expected).unwrap().len();
    assert_eq!(&encoded[..used], &expected[..expected_used]);

    let update = MapUpdate::Delete { key: 5i32 };
    let used = LsmMap::<i32, i32, 4>::encode_update_into(&update, &mut encoded).unwrap();
    let expected_used = to_slice(&update, &mut expected).unwrap().len();
    assert_eq!(&encoded[..used], &expected[..expected_used]);
}

//= spec/map.md#update-payload-format
//= type=test
//# `MAP-UPDATE-002` Applying a `Set` update payload MUST make the key
//# visible with the supplied value, and applying a `Delete` update payload
//# MUST make the key absent from the frontier.
#[test]
fn update_payload_round_trip_applies_frontier_change() {
    const BUFFER_SIZE: usize = 512;
    const MAX_INDEXES: usize = 4;
    let id = CollectionId(9);

    let mut buffer = [0u8; BUFFER_SIZE];
    let mut map = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut buffer).unwrap();

    let mut set_payload = [0u8; 64];
    let set_len = LsmMap::<i32, i32, MAX_INDEXES>::encode_update_into(
        &MapUpdate::Set { key: 5, value: 42 },
        &mut set_payload,
    )
    .unwrap();
    map.apply_update_payload(&set_payload[..set_len]).unwrap();
    assert_eq!(map.get(&5).unwrap(), Some(42));

    let mut delete_payload = [0u8; 64];
    let delete_len = LsmMap::<i32, i32, MAX_INDEXES>::encode_update_into(
        &MapUpdate::Delete { key: 5 },
        &mut delete_payload,
    )
    .unwrap();
    map.apply_update_payload(&delete_payload[..delete_len])
        .unwrap();
    assert_eq!(map.get(&5).unwrap(), None);
}

//= spec/map.md#empty-logical-state
//= type=test
//# `MAP-STATE-001` After a durable
//# `new_collection(collection_id, MAP_CODE)` basis, opening the collection
//# MUST yield an empty logical map.
//= spec/map.md#empty-logical-state
//= type=test
//# `MAP-STATE-002` `LsmMap::new` MUST construct the same empty logical
//# state used by an empty durable map basis.
#[test]
fn empty_map_open_matches_new_map_state() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;
    const MAX_INDEXES: usize = 4;
    let id = CollectionId(70);

    let mut empty_buffer = [0u8; REGION_SIZE];
    let empty = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut empty_buffer).unwrap();
    assert_eq!(empty.get(&1).unwrap(), None);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        1,
        8,
        0xa5,
    )
    .unwrap();
    storage
        .create_map::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, id)
        .unwrap();

    let reopened =
        Storage::<8, 4>::open::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace).unwrap();
    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened_map = reopened
        .open_map::<REGION_SIZE, REGION_COUNT, _, i32, i32, MAX_INDEXES>(
            &mut flash,
            &mut workspace,
            id,
            &mut reopen_buffer,
        )
        .unwrap();

    assert_eq!(reopened_map.get(&1).unwrap(), None);
}

//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-001` A map snapshot payload MUST be encoded as
//# `[entry_count:u32 little-endian][entry_bytes_len:u32 little-endian][entry_bytes][entry_refs]`.
//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-002` Snapshot encoding MUST write `entry_count` as the
//# number of visible entries in the logical map and `entry_bytes_len` as
//# the exact byte length of the compact serialized entry data that follows.
#[test]
fn snapshot_encoding_stores_header_compact_entries_and_refs() {
    const BUFFER_SIZE: usize = 512;
    const MAX_INDEXES: usize = 4;
    let id = CollectionId(71);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut source_buffer).unwrap();
    source.set(5, 50).unwrap();
    source.set(2, 20).unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();

    let entry_count = u32::from_le_bytes(snapshot[..SNAPSHOT_ENTRY_COUNT_SIZE].try_into().unwrap());
    assert_eq!(entry_count, 2);

    let entry_bytes_len = usize::try_from(u32::from_le_bytes(
        snapshot
            [SNAPSHOT_ENTRY_COUNT_SIZE..SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    assert_eq!(
        snapshot_len,
        SNAPSHOT_ENTRY_COUNT_SIZE
            + SNAPSHOT_ENTRY_BYTES_LEN_SIZE
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

    let first_start = usize::from(RefType::from_le_bytes(first_start));
    let first_end = usize::from(RefType::from_le_bytes(first_end));
    let second_start = usize::from(RefType::from_le_bytes(second_start));
    let second_end = usize::from(RefType::from_le_bytes(second_end));
    assert_eq!(first_start, ENTRY_COUNT_SIZE);
    assert_eq!(first_end, second_start);
    assert_eq!(second_end, ENTRY_COUNT_SIZE + entry_bytes_len);

    let entry_bytes_offset = SNAPSHOT_ENTRY_COUNT_SIZE + SNAPSHOT_ENTRY_BYTES_LEN_SIZE;
    let first_entry: Entry<i32, i32> = postcard::from_bytes(
        &snapshot[entry_bytes_offset + first_start - ENTRY_COUNT_SIZE
            ..entry_bytes_offset + first_end - ENTRY_COUNT_SIZE],
    )
    .unwrap();
    let second_entry: Entry<i32, i32> = postcard::from_bytes(
        &snapshot[entry_bytes_offset + second_start - ENTRY_COUNT_SIZE
            ..entry_bytes_offset + second_end - ENTRY_COUNT_SIZE],
    )
    .unwrap();
    assert!(first_entry.key < second_entry.key);
}

//= spec/ring.md#collection-head-state-machine
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
fn map_collection_format_covers_empty_state_snapshot_update_region_and_validation() {
    const BUFFER_SIZE: usize = 512;
    const MAX_INDEXES: usize = 4;
    let id = CollectionId(10);

    {
        let mut empty_buffer = [0u8; BUFFER_SIZE];
        let empty = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut empty_buffer).unwrap();
        assert_eq!(empty.get(&1).unwrap(), None);
    }

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    source.set(2, 20).unwrap();

    let mut update_payload = [0u8; 64];
    let update_len = LsmMap::<i32, i32, MAX_INDEXES>::encode_update_into(
        &MapUpdate::Set { key: 2, value: 99 },
        &mut update_payload,
    )
    .unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();
    let mut from_snapshot_buffer = [0u8; BUFFER_SIZE];
    let mut from_snapshot =
        LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut from_snapshot_buffer).unwrap();
    from_snapshot
        .load_snapshot(&snapshot[..snapshot_len])
        .unwrap();
    assert_eq!(from_snapshot.get(&1).unwrap(), Some(10));
    assert_eq!(from_snapshot.get(&2).unwrap(), Some(20));

    from_snapshot
        .apply_update_payload(&update_payload[..update_len])
        .unwrap();
    assert_eq!(from_snapshot.get(&1).unwrap(), Some(10));
    assert_eq!(from_snapshot.get(&2).unwrap(), Some(99));

    let mut region = [0u8; BUFFER_SIZE];
    let region_len = source.encode_region_into(&mut region).unwrap();
    assert_eq!(
        usize::try_from(u32::from_le_bytes(region[..4].try_into().unwrap())).unwrap(),
        snapshot_len
    );
    let mut from_region_buffer = [0u8; BUFFER_SIZE];
    let mut from_region =
        LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut from_region_buffer).unwrap();
    from_region.load_region(&region[..region_len]).unwrap();
    assert_eq!(from_region.get(&1).unwrap(), Some(10));
    assert_eq!(from_region.get(&2).unwrap(), Some(20));

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
    let mut invalid = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut invalid_buffer).unwrap();
    assert!(matches!(
        invalid.load_snapshot(&invalid_snapshot[..snapshot_len]),
        Err(MapError::SerializationError)
    ));
}

//= spec/ring.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-014` For non-WAL collections, the pair `(collection_type, collection_format)` MUST identify a unique committed region payload format.
//= spec/map.md#committed-region-format
//= type=test
//# `MAP-REGION-001` A committed map region with
//# `collection_format = MAP_REGION_V1_FORMAT` MUST encode its payload as
//# `[snapshot_len:u32 little-endian][snapshot_payload]`.
//= spec/map.md#committed-region-format
//= type=test
//# `MAP-REGION-002` The `snapshot_len` prefix MUST equal the exact byte
//# length of the embedded snapshot payload used as the region's durable
//# basis.
//= spec/map.md#committed-region-format
//= type=test
//# `MAP-REGION-003` Loading a valid committed region payload MUST
//# reconstruct the same logical state as loading its embedded snapshot
//# payload.
#[test]
fn region_round_trip_restores_logical_state() {
    const BUFFER_SIZE: usize = 512;
    const MAX_INDEXES: usize = 4;
    let id = CollectionId(11);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut source_buffer).unwrap();
    source.set(3, 30).unwrap();
    source.set(4, 40).unwrap();

    let mut snapshot = [0u8; BUFFER_SIZE];
    let snapshot_len = source.encode_snapshot_into(&mut snapshot).unwrap();
    let mut region = [0u8; BUFFER_SIZE];
    let region_len = source.encode_region_into(&mut region).unwrap();
    assert_eq!(
        usize::try_from(u32::from_le_bytes(region[..4].try_into().unwrap())).unwrap(),
        snapshot_len
    );
    assert_eq!(&region[4..4 + snapshot_len], &snapshot[..snapshot_len]);

    let mut dest_buffer = [0u8; BUFFER_SIZE];
    let mut restored = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut dest_buffer).unwrap();
    let mut direct_buffer = [0u8; BUFFER_SIZE];
    let mut direct = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut direct_buffer).unwrap();
    direct.load_snapshot(&region[4..4 + snapshot_len]).unwrap();
    assert_eq!(direct.get(&3).unwrap(), Some(30));
    assert_eq!(direct.get(&4).unwrap(), Some(40));
    restored.load_region(&region[..region_len]).unwrap();

    assert_eq!(restored.get(&3).unwrap(), Some(30));
    assert_eq!(restored.get(&4).unwrap(), Some(40));
}

//= spec/ring.md#core-requirements
//= type=test
//# `RING-CORE-002` Each collection MUST be implemented as an
//# append-only data structure whose new writes are added to the head
//# region and whose storage can only be freed by truncating the tail.
#[test]
fn map_updates_append_new_head_records_and_replacement_reclaims_the_old_tail_region() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 5;
    const MAX_INDEXES: usize = 4;

    let mut buffer = [0u8; REGION_SIZE];
    let mut map = LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(60), &mut buffer).unwrap();
    map.set(1, 10).unwrap();
    let first_end = map.next_record_offset.0;
    let first_prefix = map.map[..first_end].to_vec();

    map.set(1, 20).unwrap();
    assert!(map.next_record_offset.0 > first_end);
    assert_eq!(&map.map[..first_end], first_prefix.as_slice());
    assert_eq!(map.get(&1).unwrap(), Some(20));

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        1,
        8,
        0xa5,
    )
    .unwrap();
    storage
        .create_map::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, map.id())
        .unwrap();

    let first_region = map
        .flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8, 4>(
            storage.runtime_mut(),
            &mut flash,
            &mut workspace,
        )
        .unwrap();

    map.delete(1).unwrap();
    let second_region = map
        .flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8, 4>(
            storage.runtime_mut(),
            &mut flash,
            &mut workspace,
        )
        .unwrap();

    assert_ne!(second_region, first_region);
    assert_eq!(
        storage.runtime().collections()[0].basis(),
        crate::StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(storage.runtime().pending_reclaims(), &[first_region]);
}

//= spec/ring.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-008` Every later retained type-bearing record for that
//# collection MUST carry the same `collection_type`, otherwise replay
//# must treat the mismatch as corruption.
//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-002` Opening or loading a live map collection MUST
//# reject retained collection state whose `collection_type` is not
//# `MAP_CODE`.
#[test]
fn open_from_storage_rejects_live_collections_with_a_non_map_collection_type() {
    let mut flash = MockFlash::<512, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(61), MAP_REGION_V1_FORMAT);
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
    let runtime = crate::storage::open::<512, 4, _, 8, 4>(&mut flash, &mut workspace).unwrap();
    let mut reopen_buffer = [0u8; 512];
    let result = LsmMap::<i32, i32, 4>::open_from_storage::<512, 4, _, 8, 4>(
        &runtime,
        &mut flash,
        &mut workspace,
        CollectionId(61),
        &mut reopen_buffer,
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

//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-001` Map snapshot loading MUST reject payloads whose
//# lengths, entry ranges, ordering, or entry decoding are invalid.
//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-004` Snapshot loaders MUST treat `entry_refs` as an
//# ordered, non-overlapping description of the compact entry bytes.
#[test]
fn load_snapshot_rejects_unsorted_entry_refs() {
    const BUFFER_SIZE: usize = 512;
    const MAX_INDEXES: usize = 4;
    let id = CollectionId(12);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    source.set(2, 20).unwrap();

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
    let mut restored = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut dest_buffer).unwrap();
    assert!(matches!(
        restored.load_snapshot(&snapshot[..snapshot_len]),
        Err(MapError::SerializationError)
    ));
}

//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-001` Map snapshot loading MUST reject payloads whose
//# lengths, entry ranges, ordering, or entry decoding are invalid.
//= spec/map.md#snapshot-payload-format
//= type=test
//# `MAP-SNAPSHOT-004` Snapshot loaders MUST treat `entry_refs` as an
//# ordered, non-overlapping description of the compact entry bytes.
#[test]
fn load_snapshot_rejects_overlapping_entry_refs() {
    const BUFFER_SIZE: usize = 512;
    const MAX_INDEXES: usize = 4;
    let id = CollectionId(13);

    let mut source_buffer = [0u8; BUFFER_SIZE];
    let mut source = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    source.set(2, 20).unwrap();

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
    let mut restored = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut dest_buffer).unwrap();
    assert!(matches!(
        restored.load_snapshot(&snapshot[..snapshot_len]),
        Err(MapError::SerializationError)
    ));
}

//= spec/ring.md#core-requirements
//= type=test
//# `RING-CORE-015` Each collection's mutable in-memory update frontier
//# MUST have a bounded configured capacity.
#[test]
fn mutable_map_frontier_capacity_is_bounded_by_its_configured_buffer() {
    let min_capacity_for_three_updates = (ENTRY_COUNT_SIZE..256)
        .find(|capacity| {
            let mut buffer = vec![0u8; *capacity];
            let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(30), &mut buffer).unwrap();
            map.set(1, 10).is_ok() && map.set(1, 20).is_ok() && map.set(1, 30).is_ok()
        })
        .expect("expected a bounded capacity for three updates");

    let mut bounded_buffer = vec![0u8; min_capacity_for_three_updates];
    let mut bounded_map =
        LsmMap::<u16, u16, 8>::new(CollectionId(31), &mut bounded_buffer).unwrap();
    bounded_map.set(1, 10).unwrap();
    bounded_map.set(1, 20).unwrap();
    bounded_map.set(1, 30).unwrap();
    assert!(matches!(
        bounded_map.set(1, 40),
        Err(MapError::BufferTooSmall)
    ));

    let min_capacity_for_four_updates = (ENTRY_COUNT_SIZE..256)
        .find(|capacity| {
            let mut buffer = vec![0u8; *capacity];
            let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(32), &mut buffer).unwrap();
            map.set(1, 10).is_ok()
                && map.set(1, 20).is_ok()
                && map.set(1, 30).is_ok()
                && map.set(1, 40).is_ok()
        })
        .expect("expected a bounded capacity for four updates");

    let mut larger_buffer = vec![0u8; min_capacity_for_four_updates];
    let mut larger_map = LsmMap::<u16, u16, 8>::new(CollectionId(32), &mut larger_buffer).unwrap();
    larger_map.set(1, 10).unwrap();
    larger_map.set(1, 20).unwrap();
    larger_map.set(1, 30).unwrap();
    larger_map.set(1, 40).unwrap();
}

//= spec/map.md#merge-and-frontier-rules
//= type=test
//# `MAP-MERGE-001` When opening a live map collection, the retained
//# durable basis MUST be selected from the replay-tracked empty basis,
//# retained snapshot basis, or retained committed-region basis, and any
//# later retained update payloads for that collection MUST then be applied
//# in replay order.
//= spec/map.md#merge-and-frontier-rules
//= type=test
//# `MAP-MERGE-002` Later retained updates MUST take precedence over
//# older values from the retained basis for the same key.
//= spec/ring.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-003` The frontier MUST take precedence over older values in the durable basis.
#[test]
fn storage_snapshot_replay_restores_map_frontier() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;
    const MAX_INDEXES: usize = 4;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        1,
        8,
        0xa5,
    )
    .unwrap();
    storage
        .append_new_collection::<REGION_SIZE, REGION_COUNT, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let mut snapshot_buffer = [0u8; REGION_SIZE];
    let mut source =
        LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(7), &mut snapshot_buffer).unwrap();
    source.set(1, 10).unwrap();
    source.set(2, 20).unwrap();
    source
        .write_snapshot_to_storage::<REGION_SIZE, REGION_COUNT, _, 8, 4>(
            storage.runtime_mut(),
            &mut flash,
            &mut workspace,
        )
        .unwrap();

    let mut update_payload = [0u8; 64];
    let update_len = LsmMap::<i32, i32, MAX_INDEXES>::encode_update_into(
        &MapUpdate::Set { key: 2, value: 99 },
        &mut update_payload,
    )
    .unwrap();
    storage
        .append_update::<REGION_SIZE, REGION_COUNT, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            &update_payload[..update_len],
        )
        .unwrap();

    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened =
        LsmMap::<i32, i32, MAX_INDEXES>::open_from_storage::<REGION_SIZE, REGION_COUNT, _, 8, 4>(
            storage.runtime(),
            &mut flash,
            &mut workspace,
            CollectionId(7),
            &mut reopen_buffer,
        )
        .unwrap();

    assert_eq!(reopened.get(&1).unwrap(), Some(10));
    assert_eq!(reopened.get(&2).unwrap(), Some(99));
}

//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-003` Opening or loading a live map collection MUST
//# reject retained committed-region bases whose `collection_format` is not
//# `MAP_REGION_V1_FORMAT`.
#[test]
fn open_from_storage_rejects_unsupported_committed_region_format() {
    let mut flash = MockFlash::<512, 4, 256>::new(0xff);
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    init_user_region_header(&mut flash, 2, 4, CollectionId(72), MAP_REGION_V1_FORMAT + 1);
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
    let runtime = crate::storage::open::<512, 4, _, 8, 4>(&mut flash, &mut workspace).unwrap();
    let mut reopen_buffer = [0u8; 512];
    let result = LsmMap::<i32, i32, 4>::open_from_storage::<512, 4, _, 8, 4>(
        &runtime,
        &mut flash,
        &mut workspace,
        CollectionId(72),
        &mut reopen_buffer,
    );

    assert!(matches!(
        result,
        Err(MapStorageError::UnsupportedRegionFormat {
            collection_id: CollectionId(72),
            region_index: 2,
            actual,
        }) if actual == MAP_REGION_V1_FORMAT + 1
    ));
}

//= spec/ring.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-016` An implementation MUST NOT open a database
//# successfully if replay yields a live collection whose retained
//# committed-region basis, retained `snapshot` payload, or retained
//# post-basis `update` payloads are unsupported or invalid under that
//# collection's normative specification.
//= spec/map.md#validation-and-open-rules
//= type=test
//# `MAP-VALIDATE-004` Opening a live map collection MUST reject
//# retained committed-region payloads, snapshot payloads, or update
//# payloads that fail map-specific validation.
#[test]
fn open_from_storage_rejects_invalid_retained_region_snapshot_and_update_payloads() {
    {
        let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage =
            Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

        storage
            .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(40))
            .unwrap();

        let region_index = storage
            .runtime_mut()
            .reserve_next_region::<512, 5, _>(&mut flash, &mut workspace)
            .unwrap();
        storage
            .runtime()
            .write_committed_region::<512, 5, _>(
                &mut flash,
                region_index,
                CollectionId(40),
                MAP_REGION_V1_FORMAT,
                &[1, 2, 3],
            )
            .unwrap();
        storage
            .append_head::<512, 5, _>(
                &mut flash,
                &mut workspace,
                CollectionId(40),
                CollectionType::MAP_CODE,
                region_index,
            )
            .unwrap();

        let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<512, 5, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(40),
            &mut reopen_buffer,
        );
        assert!(matches!(
            result,
            Err(MapStorageError::Map(MapError::SerializationError))
        ));
    }

    {
        let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage =
            Storage::<8, 4>::format::<512, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

        storage
            .create_map::<512, 4, _>(&mut flash, &mut workspace, CollectionId(41))
            .unwrap();
        storage
            .append_snapshot::<512, 4, _>(
                &mut flash,
                &mut workspace,
                CollectionId(41),
                CollectionType::MAP_CODE,
                &[1],
            )
            .unwrap();

        let reopened = Storage::<8, 4>::open::<512, 4, _>(&mut flash, &mut workspace).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<512, 4, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(41),
            &mut reopen_buffer,
        );
        assert!(matches!(
            result,
            Err(MapStorageError::Map(MapError::SerializationError))
        ));
    }

    {
        let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
        let mut workspace = StorageWorkspace::<512>::new();
        let mut storage =
            Storage::<8, 4>::format::<512, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

        storage
            .create_map::<512, 4, _>(&mut flash, &mut workspace, CollectionId(42))
            .unwrap();
        storage
            .append_update::<512, 4, _>(&mut flash, &mut workspace, CollectionId(42), &[0xff])
            .unwrap();

        let reopened = Storage::<8, 4>::open::<512, 4, _>(&mut flash, &mut workspace).unwrap();
        let mut reopen_buffer = [0u8; 512];
        let result = reopened.open_map::<512, 4, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(42),
            &mut reopen_buffer,
        );
        assert!(matches!(
            result,
            Err(MapStorageError::Map(MapError::SerializationError))
        ));
    }
}

#[test]
fn storage_visit_wal_records_exposes_map_collection_records() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;
    const MAX_INDEXES: usize = 4;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        1,
        8,
        0xa5,
    )
    .unwrap();
    storage
        .append_new_collection::<REGION_SIZE, REGION_COUNT, _>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let mut snapshot_buffer = [0u8; REGION_SIZE];
    let mut source =
        LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(7), &mut snapshot_buffer).unwrap();
    source.set(1, 10).unwrap();
    source
        .write_snapshot_to_storage::<REGION_SIZE, REGION_COUNT, _, 8, 4>(
            storage.runtime_mut(),
            &mut flash,
            &mut workspace,
        )
        .unwrap();

    let mut seen = [(crate::WalRecordType::WalRecovery, CollectionId(0)); 2];
    let mut count = 0usize;
    storage
        .runtime()
        .visit_wal_records::<REGION_SIZE, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
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
//= spec/ring.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-005` Every user collection MUST remain log-structured: flushing mutable state writes a new immutable committed region segment instead of rewriting an existing live region in place.
#[test]
fn storage_region_flush_restores_map_basis() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;
    const MAX_INDEXES: usize = 4;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        1,
        8,
        0xa5,
    )
    .unwrap();
    storage
        .append_new_collection::<REGION_SIZE, REGION_COUNT, _>(
            &mut flash,
            &mut workspace,
            CollectionId(9),
            CollectionType::MAP_CODE,
        )
        .unwrap();

    let mut map_buffer = [0u8; REGION_SIZE];
    let mut map = LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(9), &mut map_buffer).unwrap();
    map.set(5, 50).unwrap();
    map.set(7, 70).unwrap();

    let region_index = map
        .flush_to_storage::<REGION_SIZE, REGION_COUNT, _, 8, 4>(
            storage.runtime_mut(),
            &mut flash,
            &mut workspace,
        )
        .unwrap();
    assert_eq!(
        storage.runtime().collections()[0].basis(),
        crate::StartupCollectionBasis::Region(region_index)
    );
    assert_eq!(storage.runtime().ready_region(), None);

    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened =
        LsmMap::<i32, i32, MAX_INDEXES>::open_from_storage::<REGION_SIZE, REGION_COUNT, _, 8, 4>(
            storage.runtime(),
            &mut flash,
            &mut workspace,
            CollectionId(9),
            &mut reopen_buffer,
        )
        .unwrap();

    assert_eq!(reopened.get(&5).unwrap(), Some(50));
    assert_eq!(reopened.get(&7).unwrap(), Some(70));
}

fn k_v_vec(count: usize) -> impl Strategy<Value = Vec<(i32, i32)>> {
    prop::collection::vec((0..i32::MAX, 0..i32::MAX), count..(count + 1))
}

proptest! {

    #[test]
    fn test_read_write(entries in k_v_vec(100)) {
        const BUFFER_SIZE: usize = 2048;
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let id = CollectionId(1);

        const MAX_INDEXES: usize = 4;

        let mut map = LsmMap::<_, _, MAX_INDEXES>::new(id, buffer.as_mut_slice())
            .expect("Could not construct LsmMap.");

        let (mut last_key, mut last_value) = entries[0];
        map.set(last_key, last_value).expect("insert failed");

        for (key, value) in entries[1..].iter() {
            map.set(*key, *value).expect("insert failed");
            if *key != last_key {
                let got = map
                .get(&last_key)
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

    #[test]
    fn test_write_delete(entries in k_v_vec(5), delete in 0usize..5) {
        const BUFFER_SIZE: usize = 2048;
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let id = CollectionId(1);

        const MAX_INDEXES: usize = 4;

        let mut map = LsmMap::<_, _, MAX_INDEXES>::new(id, buffer.as_mut_slice())
            .expect("Could not construct LsmMap.");



        for (key, value) in entries.iter() {
            map.set(*key, *value).expect("insert failed");
        }

        let delete_key = entries[delete].0;

        map.delete(delete_key).expect("delete failed");


        for (key, value) in entries.iter() {


            let got = map
            .get(key)
            .expect("could not get key");

            if *key == delete_key {
                assert_eq!(got, None);
            } else {
                assert_eq!(got, Some(*value));
            }

        }
    }

}
