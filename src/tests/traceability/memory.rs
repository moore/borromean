use super::*;
use ::core::mem::size_of;

//= spec/implementation.md#core-requirements
//# `RING-IMPL-CORE-005` All memory required for normal operation MUST
//# come from caller-owned values, fixed-capacity fields, or stack
//# frames whose size is statically bounded by type parameters or API
//# contracts.
#[test]
fn normal_operation_memory_comes_from_caller_owned_or_fixed_capacity_storage() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let workspace = strip_comment_lines(&read_repo_file("src/workspace.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(lib.contains("dirty_frontiers: Vec<CollectionId, MAX_COLLECTIONS>"));
    assert!(storage.contains("collections: Vec<StartupCollection, MAX_COLLECTIONS>"));
    assert!(storage.contains("pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>"));
    assert!(workspace.contains("region_bytes: [u8; REGION_SIZE]"));
    assert!(workspace.contains("physical_scratch: [u8; REGION_SIZE]"));
    assert!(workspace.contains("logical_scratch: [u8; REGION_SIZE]"));
    assert!(map.contains("map: &'a mut [u8]"));

    for (path, source) in non_test_sources_without_comments() {
        for banned in ["alloc::vec::Vec", "std::vec::Vec", "Box<", "Rc<", "Arc<"] {
            assert!(
                !source.contains(banned),
                "non-test source unexpectedly uses dynamic normal-operation storage via {banned} in {}",
                path.display()
            );
        }
    }
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-001` The maximum number of tracked collections,
//# heads, replay entries, and other bounded in-memory items MUST be an
//# explicit compile-time or constructor-time capacity.
#[test]
fn bounded_runtime_state_uses_explicit_capacity_parameters() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(lib.contains(
        "pub struct Storage<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>"
    ));
    assert!(lib.contains("dirty_frontiers: Vec<CollectionId, MAX_COLLECTIONS>"));
    assert!(
        storage.contains(
            "pub struct StorageRuntime<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>"
        )
    );
    assert!(storage.contains("collections: Vec<StartupCollection, MAX_COLLECTIONS>"));
    assert!(storage.contains("pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>"));
    assert!(startup.contains("wal_chain: Vec<u32, REGION_COUNT>"));
    assert!(startup.contains("collections: Vec<StartupCollection, MAX_COLLECTIONS>"));
    assert!(startup.contains("pending_reclaims: Vec<u32, MAX_PENDING_RECLAIMS>"));
    assert!(map.contains("pub struct LsmMap<'a, K, V, const MAX_INDEXES: usize>"));
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-002` Any operation that needs scratch space for
//# encoding, decoding, or staging MUST accept caller-provided buffers or
//# borrow dedicated storage from a caller-provided workspace object.
#[test]
fn scratch_space_enters_through_workspace_or_caller_buffers() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let storage = strip_comment_lines(&read_repo_file("src/storage.rs"));
    let startup = strip_comment_lines(&read_repo_file("src/startup.rs"));
    let workspace = strip_comment_lines(&read_repo_file("src/workspace.rs"));
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(lib.contains("workspace: &'a mut StorageWorkspace<REGION_SIZE>"));
    assert!(lib.contains("workspace: &mut StorageWorkspace<REGION_SIZE>"));
    assert!(lib.contains("payload_buffer: &mut [u8]"));
    assert!(lib.contains("payload_buffer: &'a mut [u8]"));
    assert!(storage.contains("let (physical, logical) = workspace.encode_buffers();"));
    assert!(storage.contains("let (region_bytes, logical_scratch) = workspace.scan_buffers();"));
    assert!(
        startup.contains("let (physical_scratch, logical_scratch) = workspace.encode_buffers();")
    );
    assert!(startup.contains("let (region_bytes, logical_scratch) = workspace.scan_buffers();"));
    assert!(workspace.contains("pub struct StorageWorkspace<const REGION_SIZE: usize>"));
    assert!(workspace.contains("region_bytes: [u8; REGION_SIZE]"));
    assert!(workspace.contains("physical_scratch: [u8; REGION_SIZE]"));
    assert!(workspace.contains("logical_scratch: [u8; REGION_SIZE]"));
    for signature in [
        "buffer: &'a mut [u8]",
        "snapshot: &mut [u8]",
        "region_payload: &mut [u8]",
        "scratch: &mut [u8]",
        "payload: &mut [u8]",
    ] {
        assert!(
            map.contains(signature),
            "missing caller buffer signature {signature}"
        );
    }
}

//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-004` The implementation SHOULD avoid keeping
//# duplicate copies of large record payloads in memory when a borrowed
//# buffer or streaming decode is sufficient.
#[test]
fn map_storage_paths_reuse_borrowed_buffers_for_payload_data() {
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(map.contains("map: &'a mut [u8]"));
    assert!(map.contains("buffer: &'a mut [u8],"));
    assert!(map.contains("let (payload, _) = workspace.encode_buffers();"));
    assert!(map.contains("&payload[..used]"));
    assert!(map.contains("from_bytes(&self.map["));
    assert!(map.contains("let entry: Entry<K, V> = from_bytes(&self.map[start..end])?;"));
    for banned in ["alloc::vec::Vec", "std::vec::Vec", "Box<[u8]>", "Vec<u8>"] {
        assert!(
            !map.contains(banned),
            "map implementation unexpectedly duplicates payloads via {banned}"
        );
    }
}

//= spec/implementation.md#memory-requirements
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
#[test]
fn map_in_memory_state_uses_explicit_capacity_and_borrowed_storage() {
    let map = strip_comment_lines(&read_repo_file("src/collections/map/mod.rs"));

    assert!(map.contains("pub struct LsmMap<'a, K, V, const MAX_INDEXES: usize>"));
    assert!(map.contains("map: &'a mut [u8]"));
    assert!(map.contains("_phantom: PhantomData<(K, V)>"));
    assert!(!map.contains("alloc::vec::Vec"));
    assert!(!map.contains("std::vec::Vec"));
    assert!(!map.contains("Box<"));
}

//= spec/implementation.md#api-requirements
//# `RING-IMPL-API-004` The implementation SHOULD keep collection
//# operation APIs close to the prototype's explicit buffer-passing style
//# where that style avoids hidden allocation.
#[test]
fn collection_update_api_keeps_explicit_payload_buffer_passing() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));

    assert!(lib.contains("pub fn append_map_update<"));
    assert!(lib.contains("pub fn append_map_update_future<"));
    assert!(lib.contains("payload_buffer: &mut [u8]"));
    assert!(lib.contains("payload_buffer: &'a mut [u8]"));
    assert!(
        lib.contains("LsmMap::<K, V, MAX_INDEXES>::encode_update_into(update, payload_buffer)?;")
    );
}
