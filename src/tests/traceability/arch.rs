use super::*;
use std::pin::pin;
use std::task::Poll;

//= spec/implementation.md#architecture-requirements
//= type=test
//# `RING-IMPL-ARCH-003` WAL handling, region-management logic, and
//# collection-specific logic MUST remain separable modules with explicit
//# interfaces.
#[test]
fn requirement_wal_storage_and_map_logic_are_exercised_through_separate_interfaces() {
    let metadata = StorageMetadata::new(256, 4, 1, 8, 0xff, 0xa5).unwrap();

    let record = WalRecord::Update {
        collection_id: CollectionId(7),
        payload: &[0x11, 0xff, 0xa5, 0x00, 0x33],
    };
    let mut physical = [0u8; 256];
    let mut logical = [0u8; 256];
    let encoded_len = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();
    let mut decode_scratch = [0u8; 256];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);

    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut storage = Storage::<_, 256, 4, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(CollectionId(7)).unwrap();

    let mut source_buffer = [0u8; 256];
    let mut source = MapFrontier::<u16, u16, 8>::new(
        CollectionId(7),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source.set_in_memory(5, 50).unwrap();
    let region_index = storage.flush_map::<_, _, 8>(&mut source).unwrap();

    drop(storage);
    let mut reopened =
        Storage::<_, 256, 4, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Region(region_index)
    );

    let mut reopened_buffer = [0u8; 256];
    let reopened_map = reopened
        .open_map::<u16, u16, 8>(
            CollectionId(7),
            &mut reopened_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(
        reopened
            .with_io_workspace(|flash, workspace| reopened_map.get::<256, _>(flash, workspace, &5))
            .unwrap(),
        Some(50)
    );
}

//= spec/implementation.md#architecture-requirements
//= type=test
//# `RING-IMPL-ARCH-004` Encoding and decoding code MUST be usable from
//# pure tests without requiring live device I/O.
#[test]
fn requirement_encoding_and_decoding_round_trip_from_plain_byte_buffers() {
    let metadata = StorageMetadata::new(128, 4, 1, 8, 0xff, 0xa5).unwrap();

    let mut metadata_bytes = [0u8; StorageMetadata::ENCODED_LEN];
    metadata.encode_into(&mut metadata_bytes).unwrap();
    assert_eq!(StorageMetadata::decode(&metadata_bytes).unwrap(), metadata);

    let header = Header {
        sequence: 9,
        collection_id: CollectionId(7),
        collection_format: 3,
    };
    let mut header_bytes = [0u8; Header::ENCODED_LEN];
    header.encode_into(&mut header_bytes).unwrap();
    assert_eq!(Header::decode(&header_bytes).unwrap(), header);

    let prologue = WalRegionPrologue {
        wal_head_region_index: 2,
    };
    let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
    prologue
        .encode_into(&mut prologue_bytes, metadata.region_count)
        .unwrap();
    assert_eq!(
        WalRegionPrologue::decode(&prologue_bytes, metadata.region_count).unwrap(),
        prologue
    );

    let footer = FreePointerFooter { next_tail: Some(3) };
    let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
    footer
        .encode_into(&mut footer_bytes, metadata.erased_byte)
        .unwrap();
    assert_eq!(
        FreePointerFooter::decode(&footer_bytes, metadata.erased_byte).unwrap(),
        footer
    );

    let record = WalRecord::Update {
        collection_id: CollectionId(7),
        payload: &[0x11, 0xff, 0xa5, 0x00, 0x33],
    };
    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len = encode_record_into(record, metadata, &mut physical, &mut logical).unwrap();
    let mut decode_scratch = [0u8; 128];
    let decoded = decode_record(&physical[..encoded_len], metadata, &mut decode_scratch).unwrap();
    assert_eq!(decoded.record, record);
}

//= spec/implementation.md#architecture-requirements
//= type=test
//# `RING-IMPL-ARCH-005` The implementation SHOULD model complex
//# multi-step procedures such as startup replay, append, allocation,
//# region write, WAL rotation, region reclaim, and WAL-head reclaim as an
//# explicit storage-mode machine with operation-specific sub-enums so that
//# each durable transition is inspectable in code review and testable in
//# isolation.
#[test]
fn requirement_startup_and_reclaim_expose_stepwise_intermediate_states_between_polls() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut storage = Storage::<_, 256, 4, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(CollectionId(83)).unwrap();
    storage.append_update(CollectionId(83), &[7, 70]).unwrap();
    drop(storage);

    {
        let future = Storage::<_, 256, 4, 8>::open_future(&mut flash, crate::test_storage_memory());
        let mut future = pin!(future);

        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
        let reopened = super::super::poll_until_ready(future.as_mut(), 8).unwrap();
        assert_eq!(reopened.wal_head(), 0);
        assert_eq!(reopened.collections()[0].collection_id(), CollectionId(83));
    }

    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, _next_region) = super::super::setup_storage_with_stale_wal_head(&mut flash);
    {
        let future = storage.reclaim_wal_head_future();
        let mut future = pin!(future);

        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
    }

    let reopened = Storage::<_, 512, 6, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::WalSnapshot
    );

    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, next_region) = super::super::setup_storage_with_stale_wal_head(&mut flash);
    let reclaimed_head =
        super::super::poll_until_ready(storage.reclaim_wal_head_future(), 16).unwrap();
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), reclaimed_head);
}
