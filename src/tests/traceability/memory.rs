use super::*;
use ::core::mem::size_of;
use heapless::Vec as HeaplessVec;

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-005` All memory required for normal operation MUST
//# come from caller-owned values, fixed-capacity fields, or stack
//# frames whose size is statically bounded by type parameters or API
//# contracts.
//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-005` All memory required for normal operation MUST
//# come from caller-owned values, fixed-capacity fields, or stack
//# frames whose size is statically bounded by type parameters or API
//# contracts.
#[test]
fn normal_operation_uses_caller_owned_buffers_without_heap_allocation() {
    let mut flash = MockFlash::<256, 5, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut payload_buffer = [0u8; 64];
    let mut map_buffer = [0u8; 256];

    assert_no_alloc("format/create/update/open", || {
        let mut storage =
            Storage::<8, 4>::format::<256, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
        storage
            .create_map::<256, 5, _>(&mut flash, &mut workspace, CollectionId(90))
            .unwrap();
        storage
            .append_map_update::<256, 5, _, u16, u16, 8>(
                &mut flash,
                &mut workspace,
                CollectionId(90),
                &MapUpdate::Set { key: 7, value: 70 },
                &mut payload_buffer,
            )
            .unwrap();

        let reopened = Storage::<8, 4>::open::<256, 5, _>(&mut flash, &mut workspace).unwrap();
        let map = reopened
            .open_map::<256, 5, _, u16, u16, 8>(
                &mut flash,
                &mut workspace,
                CollectionId(90),
                &mut map_buffer,
            )
            .unwrap();
        assert_eq!(map.get(&7).unwrap(), Some(70));
    });
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-001` The maximum number of tracked collections,
//# heads, replay entries, and other bounded in-memory items MUST be an
//# explicit compile-time or constructor-time capacity.
//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-001` The maximum number of tracked collections,
//# heads, replay entries, and other bounded in-memory items MUST be an
//# explicit compile-time or constructor-time capacity.
#[test]
fn explicit_collection_and_reclaim_capacities_fail_when_exhausted() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<1, 1>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(1))
        .unwrap();
    assert!(matches!(
        storage.create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(2)),
        Err(StorageRuntimeError::TooManyTrackedCollections)
            | Err(StorageRuntimeError::Startup(
                StartupError::TooManyTrackedCollections
            ))
    ));

    let mut tiny_buffer = [0u8; 16];
    let mut tiny_map = LsmMap::<u16, u16, 8>::new(CollectionId(3), &mut tiny_buffer).unwrap();
    tiny_map.set(1, 10).unwrap();
    assert!(matches!(tiny_map.set(2, 20), Err(MapError::BufferTooSmall)));
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-002` Any operation that needs scratch space for
//# encoding, decoding, or staging MUST accept caller-provided buffers or
//# borrow dedicated storage from a caller-provided workspace object.
//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-002` Any operation that needs scratch space for
//# encoding, decoding, or staging MUST accept caller-provided buffers or
//# borrow dedicated storage from a caller-provided workspace object.
#[test]
fn scratch_space_boundaries_are_enforced_on_caller_buffers() {
    let mut map_buffer = [0u8; 128];
    let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(4), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();

    let mut tiny_snapshot = [0u8; 4];
    assert!(matches!(
        map.encode_snapshot_into(&mut tiny_snapshot),
        Err(MapError::BufferTooSmall)
    ));

    let mut tiny_region = [0u8; 8];
    assert!(matches!(
        map.encode_region_into(&mut tiny_region),
        Err(MapError::BufferTooSmall)
    ));

    let mut tiny_scratch = [0u8; 8];
    assert!(matches!(
        map.checkpoint_into(&mut tiny_scratch),
        Err(MapError::BufferTooSmall)
    ));

    let mut flash = MockFlash::<256, 5, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut storage =
        Storage::<8, 4>::format::<256, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    storage
        .create_map::<256, 5, _>(&mut flash, &mut workspace, CollectionId(5))
        .unwrap();

    let mut tiny_payload = [0u8; 1];
    assert!(matches!(
        storage.append_map_update::<256, 5, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(5),
            &MapUpdate::Set { key: 7, value: 70 },
            &mut tiny_payload,
        ),
        Err(MapStorageError::Map(MapError::BufferTooSmall))
    ));
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-004` The implementation SHOULD avoid keeping
//# duplicate copies of large record payloads in memory when a borrowed
//# buffer or streaming decode is sufficient.
//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-004` The implementation SHOULD avoid keeping
//# duplicate copies of large record payloads in memory when a borrowed
//# buffer or streaming decode is sufficient.
#[test]
fn map_round_trips_large_snapshots_using_only_borrowed_buffers() {
    let mut source_buffer = [0u8; 512];
    let mut source =
        LsmMap::<u16, HeaplessVec<u8, 96>, 8>::new(CollectionId(6), &mut source_buffer).unwrap();
    source
        .set(1, HeaplessVec::<u8, 96>::from_slice(&[0x11; 96]).unwrap())
        .unwrap();
    source
        .set(2, HeaplessVec::<u8, 96>::from_slice(&[0x22; 96]).unwrap())
        .unwrap();

    let mut snapshot = [0u8; 512];
    let snapshot_len = assert_no_alloc("encode_snapshot_into", || {
        source.encode_snapshot_into(&mut snapshot).unwrap()
    });

    let mut reopened_buffer = [0u8; 512];
    let mut reopened =
        LsmMap::<u16, HeaplessVec<u8, 96>, 8>::new(CollectionId(6), &mut reopened_buffer).unwrap();
    assert_no_alloc("load_snapshot", || {
        reopened.load_snapshot(&snapshot[..snapshot_len]).unwrap();
    });

    assert_eq!(
        reopened.get(&1).unwrap(),
        Some(HeaplessVec::<u8, 96>::from_slice(&[0x11; 96]).unwrap())
    );
    assert_eq!(
        reopened.get(&2).unwrap(),
        Some(HeaplessVec::<u8, 96>::from_slice(&[0x22; 96]).unwrap())
    );
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk
//# format constants MUST be derivable from public constants, associated
//# constants, or documented constructor contracts.
//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk
//# format constants MUST be derivable from public constants, associated
//# constants, or documented constructor contracts.
#[test]
fn disk_format_buffer_sizes_are_exposed_by_constants_or_workspace_contracts() {
    assert_eq!(
        StorageMetadata::ENCODED_LEN,
        size_of::<u32>() * 6 + size_of::<u8>() * 2
    );
    assert_eq!(
        Header::ENCODED_LEN,
        size_of::<u64>() + size_of::<u64>() + size_of::<u16>() + size_of::<u32>()
    );
    assert_eq!(WalRegionPrologue::ENCODED_LEN, size_of::<u32>() * 2);
    assert_eq!(FreePointerFooter::ENCODED_LEN, size_of::<u32>() * 2);

    let mut workspace = StorageWorkspace::<128>::new();
    {
        let (region_bytes, logical_scratch) = workspace.scan_buffers();
        assert_eq!(region_bytes.len(), 128);
        assert_eq!(logical_scratch.len(), 128);
    }
    {
        let (physical_scratch, logical_scratch) = workspace.encode_buffers();
        assert_eq!(physical_scratch.len(), 128);
        assert_eq!(logical_scratch.len(), 128);
    }
}

//= spec/implementation.md#collection-requirements
//# `RING-IMPL-COLL-002` Collection-specific in-memory state MUST obey
//# the same explicit-capacity and no-allocation rules as borromean
//# core.
//= spec/implementation.md#collection-requirements
//= type=test
//# `RING-IMPL-COLL-002` Collection-specific in-memory state MUST obey
//# the same explicit-capacity and no-allocation rules as borromean
//# core.
#[test]
fn map_in_memory_state_runs_inside_a_borrowed_buffer_without_allocating() {
    let mut map_buffer = [0u8; 128];
    let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(7), &mut map_buffer).unwrap();

    assert_no_alloc("map set/get", || {
        map.set(1, 10).unwrap();
        map.set(2, 20).unwrap();
        assert_eq!(map.get(&1).unwrap(), Some(10));
        assert_eq!(map.get(&2).unwrap(), Some(20));
    });

    let mut tiny_buffer = [0u8; 16];
    let mut tiny_map = LsmMap::<u16, u16, 8>::new(CollectionId(8), &mut tiny_buffer).unwrap();
    tiny_map.set(1, 10).unwrap();
    assert!(matches!(tiny_map.set(2, 20), Err(MapError::BufferTooSmall)));
}

//= spec/implementation.md#api-requirements
//# `RING-IMPL-API-004` The implementation SHOULD keep collection
//# operation APIs close to the prototype's explicit buffer-passing style
//# where that style avoids hidden allocation.
//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-004` The implementation SHOULD keep collection
//# operation APIs close to the prototype's explicit buffer-passing style
//# where that style avoids hidden allocation.
#[test]
fn map_updates_require_and_reuse_a_caller_provided_payload_buffer() {
    let mut flash = MockFlash::<256, 5, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut storage =
        Storage::<8, 4>::format::<256, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    storage
        .create_map::<256, 5, _>(&mut flash, &mut workspace, CollectionId(9))
        .unwrap();

    let mut payload_buffer = [0u8; 64];
    assert_no_alloc("append_map_update buffer reuse", || {
        storage
            .append_map_update::<256, 5, _, u16, u16, 8>(
                &mut flash,
                &mut workspace,
                CollectionId(9),
                &MapUpdate::Set { key: 1, value: 10 },
                &mut payload_buffer,
            )
            .unwrap();
        storage
            .append_map_update::<256, 5, _, u16, u16, 8>(
                &mut flash,
                &mut workspace,
                CollectionId(9),
                &MapUpdate::Set { key: 2, value: 20 },
                &mut payload_buffer,
            )
            .unwrap();
    });

    let reopened = Storage::<8, 4>::open::<256, 5, _>(&mut flash, &mut workspace).unwrap();
    let mut map_buffer = [0u8; 256];
    let map = reopened
        .open_map::<256, 5, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(9),
            &mut map_buffer,
        )
        .unwrap();
    assert_eq!(map.get(&1).unwrap(), Some(10));
    assert_eq!(map.get(&2).unwrap(), Some(20));
}
