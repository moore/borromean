use super::*;
extern crate std;
use crate::{MockFlash, Storage, StorageWorkspace};
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

#[test]
//= spec/implementation.md#panic-requirements
//# `RING-IMPL-PANIC-004` If a condition is believed to be impossible by construction, the implementation SHOULD encode that proof in types, control flow, or checked validation before the point of use rather than relying on a panic as a backstop.
fn set_returns_buffer_too_small_when_map_storage_is_exhausted() {
    const MAX_INDEXES: usize = 4;

    let mut buffer = [0u8; 8];
    let mut map = LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(27), &mut buffer).unwrap();

    assert!(matches!(map.set(1, 10), Err(MapError::BufferTooSmall)));
}

#[test]
//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-003` If the configured capacities are insufficient to open the store or complete an operation, the implementation MUST fail explicitly with a capacity-related error rather than silently allocate or truncate state.
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

#[test]
//= spec/ring.md#collection-head-state-machine
//# `RING-FORMAT-003` The frontier MUST take precedence over older values in the durable basis.
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
    map.apply_update_payload(&delete_payload[..delete_len]).unwrap();
    assert_eq!(map.get(&5).unwrap(), None);
}

#[test]
//= spec/ring.md#collection-head-state-machine
//# `RING-FORMAT-013` That collection specification MUST define, at
//# minimum: the empty logical state established by `new_collection`; the
//# exact bytes and interpretation of every supported committed-region
//# `collection_format`; the exact bytes and interpretation of `snapshot`
//# payloads; the exact bytes and interpretation of `update` payloads; the
//# rules for applying updates and merging a durable basis with the
//# in-memory frontier; and the collection-specific validation rules used
//# when loading a basis or replaying WAL payloads.
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
    let mut from_snapshot = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut from_snapshot_buffer).unwrap();
    from_snapshot.load_snapshot(&snapshot[..snapshot_len]).unwrap();
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
    let mut from_region = LsmMap::<i32, i32, MAX_INDEXES>::new(id, &mut from_region_buffer).unwrap();
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

#[test]
//= spec/ring.md#collection-head-state-machine
//# `RING-FORMAT-014` For non-WAL collections, the pair `(collection_type, collection_format)` MUST identify a unique committed region payload format.
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
    direct
        .load_snapshot(&region[4..4 + snapshot_len])
        .unwrap();
    assert_eq!(direct.get(&3).unwrap(), Some(30));
    assert_eq!(direct.get(&4).unwrap(), Some(40));
    restored.load_region(&region[..region_len]).unwrap();

    assert_eq!(restored.get(&3).unwrap(), Some(30));
    assert_eq!(restored.get(&4).unwrap(), Some(40));
}

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
    second_ref.copy_from_slice(&snapshot[refs_offset + ENTRY_REF_SIZE..refs_offset + ENTRY_REF_SIZE * 2]);
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

#[test]
//= spec/ring.md#core-requirements
//# `RING-CORE-015` Each collection's mutable in-memory update frontier
//# MUST have a bounded configured capacity.
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

#[test]
//= spec/ring.md#collection-head-state-machine
//# `RING-FORMAT-003` The frontier MUST take precedence over older values in the durable basis.
fn storage_snapshot_replay_restores_map_frontier() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;
    const MAX_INDEXES: usize = 4;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage =
        Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, 1, 8, 0xa5)
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
    let mut source = LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(7), &mut snapshot_buffer).unwrap();
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
    let reopened = LsmMap::<i32, i32, MAX_INDEXES>::open_from_storage::<
        REGION_SIZE,
        REGION_COUNT,
        _,
        8,
        4,
    >(storage.runtime(), &mut flash, &mut workspace, CollectionId(7), &mut reopen_buffer)
    .unwrap();

    assert_eq!(reopened.get(&1).unwrap(), Some(10));
    assert_eq!(reopened.get(&2).unwrap(), Some(99));
}

#[test]
//= spec/ring.md#collection-head-state-machine
//# `RING-FORMAT-016` An implementation MUST NOT open a database
//# successfully if replay yields a live collection whose retained
//# committed-region basis, retained `snapshot` payload, or retained
//# post-basis `update` payloads are unsupported or invalid under that
//# collection's normative specification.
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
    let mut storage =
        Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, 1, 8, 0xa5)
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
    let mut source = LsmMap::<i32, i32, MAX_INDEXES>::new(CollectionId(7), &mut snapshot_buffer).unwrap();
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

#[test]
//= spec/ring.md#collection-head-state-machine
//# `RING-FORMAT-005` Every user collection MUST remain log-structured: flushing mutable state writes a new immutable committed region segment instead of rewriting an existing live region in place.
fn storage_region_flush_restores_map_basis() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 4;
    const MAX_INDEXES: usize = 4;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage =
        Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, 1, 8, 0xa5)
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
    let reopened = LsmMap::<i32, i32, MAX_INDEXES>::open_from_storage::<
        REGION_SIZE,
        REGION_COUNT,
        _,
        8,
        4,
    >(storage.runtime(), &mut flash, &mut workspace, CollectionId(9), &mut reopen_buffer)
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
