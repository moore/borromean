use super::*;
use ::core::pin::pin;
use ::core::task::Poll;

//= spec/implementation.md#architecture-requirements
//= type=test
//# `RING-IMPL-ARCH-003` WAL handling, region-management logic, and
//# collection-specific logic MUST remain separable modules with explicit
//# interfaces.
#[test]
fn wal_storage_and_map_logic_are_exercised_through_separate_interfaces() {
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
    let mut storage =
        Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    storage
        .create_map::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7))
        .unwrap();

    let mut source_buffer = [0u8; 256];
    let mut source = LsmMap::<u16, u16, 8>::new(CollectionId(7), &mut source_buffer).unwrap();
    source.set(5, 50).unwrap();
    let region_index = storage
        .flush_map::<256, 4, _, _, _, 8>(&mut flash, &mut workspace, &source)
        .unwrap();

    let reopened = Storage::<8, 4>::open::<256, 4, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Region(region_index)
    );

    let mut reopened_buffer = [0u8; 256];
    let reopened_map = reopened
        .open_map::<256, 4, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(7),
            &mut reopened_buffer,
        )
        .unwrap();
    assert_eq!(reopened_map.get(&5).unwrap(), Some(50));
}

//= spec/implementation.md#architecture-requirements
//= type=test
//# `RING-IMPL-ARCH-004` Encoding and decoding code MUST be usable from
//# pure tests without requiring live device I/O.
#[test]
fn encoding_and_decoding_round_trip_from_plain_byte_buffers() {
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
//# multi-step procedures such as startup replay and reclaim as explicit
//# phase machines so that each durable transition is inspectable in code
//# review and testable in isolation.
#[test]
fn startup_and_reclaim_expose_stepwise_intermediate_states_between_polls() {
    let mut flash = MockFlash::<256, 4, 512>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let mut storage =
        Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    storage
        .create_map::<256, 4, _>(&mut flash, &mut workspace, CollectionId(83))
        .unwrap();
    storage
        .append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(83), &[7, 70])
        .unwrap();
    drop(storage);

    {
        let future = Storage::<8, 4>::open_future::<256, 4, _>(&mut flash, &mut workspace);
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

    let (mut flash, mut workspace, mut storage, _next_region) =
        super::super::setup_storage_with_stale_wal_head();
    {
        let future = storage.reclaim_wal_head_future::<512, 6, _>(&mut flash, &mut workspace);
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

    let reopened = Storage::<8, 4>::open::<512, 6, _>(&mut flash, &mut workspace).unwrap();
    assert!(reopened.pending_reclaims().is_empty());
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::WalSnapshot
    );

    let (mut flash, mut workspace, mut storage, next_region) =
        super::super::setup_storage_with_stale_wal_head();
    let reclaimed_head = super::super::poll_until_ready(
        storage.reclaim_wal_head_future::<512, 6, _>(&mut flash, &mut workspace),
        6,
    )
    .unwrap();
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), next_region);
}
