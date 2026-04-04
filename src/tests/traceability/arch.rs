use super::*;

//= spec/implementation.md#architecture-requirements
//# `RING-IMPL-ARCH-003` WAL handling, region-management logic, and
//# collection-specific logic MUST remain separable modules with explicit
//# interfaces.
#[test]
fn wal_region_management_and_collection_logic_stay_separate_modules() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for relative in [
        "wal_record.rs",
        "storage.rs",
        "startup.rs",
        "collections.rs",
        "collections/map/mod.rs",
    ] {
        assert!(
            src_root.join(relative).is_file(),
            "expected separate module file {relative}"
        );
    }

    let lib = fs::read_to_string(src_root.join("lib.rs")).unwrap();
    assert!(lib.contains("pub mod wal_record;"));
    assert!(lib.contains("pub mod storage;"));
    assert!(lib.contains("mod collections;"));

    let collections = fs::read_to_string(src_root.join("collections.rs")).unwrap();
    assert!(collections.contains("pub mod map;"));

    let metadata = StorageMetadata::new(128, 4, 1, 8, 0xff, 0xa5).unwrap();
    let mut physical = [0u8; 128];
    let mut logical = [0u8; 128];
    let encoded_len =
        encode_record_into(WalRecord::WalRecovery, metadata, &mut physical, &mut logical).unwrap();
    assert!(encoded_len > 0);

    let mut flash = MockFlash::<128, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let storage =
        Storage::<8, 4>::format::<128, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    assert_eq!(storage.wal_head(), 0);

    let mut map_buffer = [0u8; 128];
    let map = LsmMap::<i32, i32, 4>::new(CollectionId(7), &mut map_buffer).unwrap();
    assert_eq!(map.id(), CollectionId(7));
}

//= spec/implementation.md#architecture-requirements
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
    footer.encode_into(&mut footer_bytes, metadata.erased_byte).unwrap();
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
//# `RING-IMPL-ARCH-005` The implementation SHOULD model complex
//# multi-step procedures such as startup replay and reclaim as explicit
//# phase machines so that each durable transition is inspectable in code
//# review and testable in isolation.
#[test]
fn startup_and_reclaim_use_explicit_phase_machine_enums() {
    let op_future = strip_comment_lines(&read_repo_file("src/op_future.rs"));

    assert!(op_future.contains("enum ReclaimWalHeadPhase<const MAX_COLLECTIONS: usize>"));
    for variant in [
        "Plan,",
        "BeginReclaim {",
        "PreserveFreeListHead {",
        "CopyLiveState {",
        "CommitHead {",
        "CompleteReclaim {",
        "Done,",
    ] {
        assert!(
            op_future.contains(variant),
            "missing reclaim phase variant {variant}"
        );
    }

    assert!(op_future.contains("enum OpenStoragePhase<"));
    for variant in [
        "Begin,",
        "RecoverRotation {",
        "DiscoverWalChain {",
        "ReplayWalChain {",
        "FinishStartup {",
        "RecoverPendingReclaims {",
        "ValidateCollections {",
        "Done,",
    ] {
        assert!(
            op_future.contains(variant),
            "missing startup phase variant {variant}"
        );
    }
}
