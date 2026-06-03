use super::*;
use heapless::Vec as HeaplessVec;
use std::mem::size_of;

fn assert_stack_handle_size<T>(label: &str) {
    assert!(
        size_of::<T>() <= 64,
        "{label} is {} bytes, expected <= 64",
        size_of::<T>()
    );
}

//= spec/implementation.md#memory-requirements
//= type=test
//# Core handles and operation futures MUST remain small stack values;
//# caller-owned memory structs carry bounded scratch and runtime state.
#[test]
fn requirement_core_handles_and_futures_fit_small_stack_frames() {
    type Flash = MockFlash<256, 6, 1024>;

    assert_stack_handle_size::<Storage<'_, '_, Flash, 256, 6, 8>>("Storage");
    assert_stack_handle_size::<FormatStorageFuture<'_, '_, Flash, 256, 6, 8>>(
        "FormatStorageFuture",
    );
    assert_stack_handle_size::<OpenStorageFuture<'_, '_, Flash, 256, 6, 8>>("OpenStorageFuture");
    assert_stack_handle_size::<ReclaimWalHeadFuture<'_, '_, '_, Flash, 256, 6, 8>>(
        "ReclaimWalHeadFuture",
    );
    assert_stack_handle_size::<LsmMap<'_, u16, u16, 8>>("LsmMap");
    assert_stack_handle_size::<MapFrontier<'_, u16, u16, 8>>("MapFrontier");
    assert_stack_handle_size::<crate::op_future::ReclaimWalHeadPhase<6, 8>>("ReclaimWalHeadPhase");
    assert_stack_handle_size::<crate::op_future::OpenStoragePhase<6, 8>>("OpenStoragePhase");
}

//= spec/implementation.md#core-requirements
//= type=test
//# `RING-IMPL-CORE-005` All memory required for normal operation MUST
//# come from caller-owned values, fixed-capacity fields, or stack
//# frames whose size is statically bounded by type parameters or API
//# contracts.
#[test]
fn requirement_normal_operation_uses_caller_owned_buffers_without_heap_allocation() {
    let mut flash = MockFlash::<256, 5, 1024>::new(0xff);
    let mut map_buffer = [0u8; 256];
    let mut format_memory = StorageMemory::<256, 5, 8>::new();
    let mut open_memory = StorageMemory::<256, 5, 8>::new();
    let mut frontier_memory = MapFrontierMemory::<u16, 8>::new();

    assert_no_alloc("format/create/update/open", || {
        let mut storage = Storage::<_, 256, 5, 8>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            &mut format_memory,
        )
        .unwrap();
        storage.create_map(CollectionId(90)).unwrap();
        storage
            .append_map_update::<u16, u16>(CollectionId(90), &MapUpdate::Set { key: 7, value: 70 })
            .unwrap();

        drop(storage);
        let mut reopened = Storage::<_, 256, 5, 8>::open(&mut flash, &mut open_memory).unwrap();
        let map = reopened
            .open_map::<u16, u16, 8>(CollectionId(90), &mut map_buffer, &mut frontier_memory)
            .unwrap();
        assert_eq!(map.get_frontier(&7).unwrap(), Some(70));
    });
}

//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-001` The maximum number of tracked collections,
//# heads, replay entries, and other bounded in-memory items MUST be an
//# explicit compile-time or constructor-time capacity.
#[test]
fn requirement_explicit_collection_and_reclaim_capacities_fail_when_exhausted() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut storage = Storage::<_, 512, 5, 1>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(1)).unwrap();
    assert!(matches!(
        storage.create_map(CollectionId(2)),
        Err(StorageRuntimeError::TooManyTrackedCollections)
            | Err(StorageRuntimeError::Startup(
                StartupError::TooManyTrackedCollections
            ))
    ));

    let mut tiny_buffer = [0u8; 32];
    let mut tiny_map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(3),
        &mut tiny_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    tiny_map.set_in_memory(1, 10).unwrap();
    assert!(matches!(
        tiny_map.set_in_memory(2, 20),
        Err(MapError::BufferTooSmall)
    ));
}

//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-002` Any operation that needs scratch space for
//# encoding, decoding, or staging MUST use bounded caller-owned storage
//# borrowed by the `Storage` context or supplied to the collection handle.
#[test]
fn requirement_scratch_space_is_owned_by_storage_context() {
    let mut flash = MockFlash::<256, 6, 1024>::new(0xff);
    let mut storage = Storage::<_, 256, 6, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(91)).unwrap();
    for key in 0..2 {
        storage
            .append_map_update::<u16, u16>(
                CollectionId(91),
                &MapUpdate::Set {
                    key,
                    value: key + 100,
                },
            )
            .unwrap();
    }
    assert_eq!(storage.mode(), StorageMode::Idle);
}

//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-004` The implementation SHOULD avoid keeping
//# duplicate copies of large record payloads in memory when a borrowed
//# buffer or streaming decode is sufficient.
#[test]
fn requirement_map_round_trips_large_snapshots_using_only_borrowed_buffers() {
    let mut source_buffer = [0u8; 512];
    let mut source = MapFrontier::<u16, HeaplessVec<u8, 96>, 8>::new(
        CollectionId(6),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source
        .set_in_memory(1, HeaplessVec::<u8, 96>::from_slice(&[0x11; 96]).unwrap())
        .unwrap();
    source
        .set_in_memory(2, HeaplessVec::<u8, 96>::from_slice(&[0x22; 96]).unwrap())
        .unwrap();

    let mut snapshot = [0u8; 512];
    let snapshot_len = assert_no_alloc("encode_snapshot_into", || {
        source.encode_snapshot_into(&mut snapshot).unwrap()
    });

    let mut reopened_buffer = [0u8; 512];
    let mut reopened = MapFrontier::<u16, HeaplessVec<u8, 96>, 8>::new(
        CollectionId(6),
        &mut reopened_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    assert_no_alloc("load_snapshot", || {
        reopened.load_snapshot(&snapshot[..snapshot_len]).unwrap();
    });

    assert_eq!(
        reopened.get_frontier(&1).unwrap(),
        Some(HeaplessVec::<u8, 96>::from_slice(&[0x11; 96]).unwrap())
    );
    assert_eq!(
        reopened.get_frontier(&2).unwrap(),
        Some(HeaplessVec::<u8, 96>::from_slice(&[0x22; 96]).unwrap())
    );
}

//= spec/implementation.md#memory-requirements
//= type=test
//# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk
//# format constants MUST be derivable from public constants, associated
//# constants, or documented constructor contracts.
#[test]
fn requirement_disk_format_buffer_sizes_are_exposed_by_constants_or_workspace_contracts() {
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
//= type=test
//# `RING-IMPL-COLL-002` Collection-specific in-memory state MUST obey
//# the same explicit-capacity and no-allocation rules as borromean
//# core.
#[test]
fn requirement_map_in_memory_state_runs_inside_a_borrowed_buffer_without_allocating() {
    let mut map_buffer = [0u8; 128];
    let mut map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(7),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();

    assert_no_alloc("map set/get", || {
        map.set_in_memory(1, 10).unwrap();
        map.set_in_memory(2, 20).unwrap();
        assert_eq!(map.get_frontier(&1).unwrap(), Some(10));
        assert_eq!(map.get_frontier(&2).unwrap(), Some(20));
    });

    let mut tiny_buffer = [0u8; 32];
    let mut tiny_map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(8),
        &mut tiny_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    tiny_map.set_in_memory(1, 10).unwrap();
    assert!(matches!(
        tiny_map.set_in_memory(2, 20),
        Err(MapError::BufferTooSmall)
    ));
}

//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-004` Normal public collection operation APIs SHOULD
//# avoid repeated caller-provided frontier, payload, or workspace buffers
//# and instead use bounded memory owned by the `Storage` context.
#[test]
fn requirement_collection_api_uses_storage_owned_operation_buffers() {
    let mut flash = MockFlash::<256, 6, 1024>::new(0xff);
    let mut storage = Storage::<_, 256, 6, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(92)).unwrap();
    storage
        .append_map_update::<u16, u16>(CollectionId(92), &MapUpdate::Set { key: 9, value: 90 })
        .unwrap();
    let mut map_buffer = [0u8; 256];
    let mut map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(92),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(9, 90).unwrap();
    storage.snapshot_map(&map).unwrap();
}
