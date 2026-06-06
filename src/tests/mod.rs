#![allow(clippy::drop_non_drop)]

use super::*;
use core::future::Future;
use core::pin::{pin, Pin};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
extern crate std;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::thread_local;
use std::vec;

mod lifecycle;
mod traceability;

thread_local! {
    static TRACKED_ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static TRACKING_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

#[global_allocator]
static TEST_ALLOCATOR: CountingAllocator = CountingAllocator;

fn note_allocation() {
    TRACKING_DEPTH.with(|depth| {
        if depth.get() == 0 {
            return;
        }

        TRACKED_ALLOCATIONS.with(|count| {
            count.set(count.get().checked_add(1).unwrap());
        });
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            note_allocation();
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            note_allocation();
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() && new_ptr != ptr {
            note_allocation();
        }
        new_ptr
    }
}

struct AllocationTrackingGuard;

impl AllocationTrackingGuard {
    fn new() -> Self {
        TRACKED_ALLOCATIONS.with(|count| count.set(0));
        TRACKING_DEPTH.with(|depth| {
            depth.set(depth.get().checked_add(1).unwrap());
        });
        Self
    }
}

impl Drop for AllocationTrackingGuard {
    fn drop(&mut self) {
        TRACKING_DEPTH.with(|depth| {
            depth.set(depth.get().checked_sub(1).unwrap());
        });
    }
}

fn assert_no_alloc<T>(label: &str, operation: impl FnOnce() -> T) -> T {
    let guard = AllocationTrackingGuard::new();
    let result = operation();
    drop(guard);

    let allocations = TRACKED_ALLOCATIONS.with(Cell::get);
    assert_eq!(
        allocations, 0,
        "{} unexpectedly performed {} heap allocation(s)",
        label, allocations
    );
    result
}

fn noop_raw_waker() -> RawWaker {
    unsafe fn clone(_data: *const ()) -> RawWaker {
        noop_raw_waker()
    }

    unsafe fn wake(_data: *const ()) {}

    unsafe fn wake_by_ref(_data: *const ()) {}

    unsafe fn drop(_data: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

    RawWaker::new(core::ptr::null(), &VTABLE)
}

fn noop_waker() -> Waker {
    // SAFETY: The no-op waker never dereferences the null data pointer and its
    // vtable functions do not retain or free any backing state.
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

fn poll_ready<F>(future: F) -> F::Output
where
    F: Future,
{
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);

    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("future unexpectedly returned Poll::Pending"),
    }
}

fn poll_once<F>(future: Pin<&mut F>) -> Poll<F::Output>
where
    F: Future,
{
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    future.poll(&mut context)
}

fn poll_until_ready<F>(future: F, max_polls: usize) -> F::Output
where
    F: Future,
{
    let mut future = pin!(future);
    for _ in 0..max_polls {
        match poll_once(future.as_mut()) {
            Poll::Ready(output) => return output,
            Poll::Pending => {}
        }
    }

    panic!("future did not complete within {max_polls} polls");
}

fn free_list_chain<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
    const CAP: usize,
>(
    flash: &MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    erased_byte: u8,
    head: Option<u32>,
) -> heapless::Vec<u32, CAP> {
    let footer_offset = REGION_SIZE - FreePointerFooter::ENCODED_LEN;
    let mut current = head;
    let mut chain = heapless::Vec::new();

    for _ in 0..REGION_COUNT {
        let Some(region_index) = current else {
            break;
        };
        chain.push(region_index).unwrap();
        let footer = FreePointerFooter::decode(
            &flash.region_bytes(region_index).unwrap()[footer_offset..],
            erased_byte,
        )
        .unwrap();
        current = footer.next_tail;
    }

    assert!(current.is_none(), "free-list chain should terminate");
    chain
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-092` CollectionId helpers MUST expose little-endian bytes and checked
//# increment semantics, returning none on u64 overflow.
#[test]
fn requirement_collection_id_helpers_preserve_little_endian_and_overflow_semantics() {
    let id = CollectionId::new(0x0102_0304_0506_0708);

    assert_eq!(id.to_le_bytes(), 0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(
        id.increment(),
        Some(CollectionId::new(0x0102_0304_0506_0709))
    );
    assert_eq!(CollectionId::new(u64::MAX).increment(), None);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-093` Storage facade accessors MUST reflect underlying runtime state and
//# tracked collection metadata.
#[test]
fn requirement_storage_facade_accessors_reflect_runtime_state() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    assert_eq!(
        storage.wal_append_offset(),
        storage.runtime().wal_append_offset()
    );
    assert_eq!(
        storage.pending_wal_recovery_boundary(),
        storage.runtime().pending_wal_recovery_boundary()
    );
    assert_eq!(storage.tracked_user_collection_count(), 0);
    assert_eq!(storage.allocate_collection_id().unwrap(), CollectionId(1));

    storage
        .append_new_collection(CollectionId(321), CollectionType::MAP_CODE)
        .unwrap();

    assert_eq!(storage.tracked_user_collection_count(), 1);
    assert_eq!(storage.collections()[0].collection_id(), CollectionId(321));
    assert_eq!(storage.allocate_collection_id().unwrap(), CollectionId(322));

    let first_region = storage.reserve_next_region().unwrap();
    assert_eq!(first_region, 1);
    assert_eq!(storage.ready_region(), Some(first_region));
    storage
        .write_committed_region(
            first_region,
            CollectionId(321),
            MAP_REGION_V2_FORMAT,
            &[1, 2, 3],
        )
        .unwrap();
    let header =
        Header::decode(&storage.backing.region_bytes(first_region).unwrap()[..Header::ENCODED_LEN])
            .unwrap();
    assert_eq!(header.collection_id, CollectionId(321));
    assert_eq!(header.collection_format, MAP_REGION_V2_FORMAT);
    storage
        .append_head(CollectionId(321), CollectionType::MAP_CODE, first_region)
        .unwrap();
    assert_eq!(storage.ready_region(), None);

    let second_region = storage.reserve_next_region().unwrap();
    assert_eq!(second_region, 2);
    assert_eq!(storage.ready_region(), Some(second_region));
    assert!(matches!(
        storage.append_alloc_begin(99, None),
        Err(StorageRuntimeError::InvalidAllocBegin { .. })
    ));

    storage.append_drop_collection(CollectionId(321)).unwrap();
    assert_eq!(storage.tracked_user_collection_count(), 0);
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
}

//= spec/map.md#map-api-model
//= type=test
//# The storage-owned hot frontier buffer cache is keyed by both collection
//# id and generation; assigning the same owner reuses the current generation.
#[test]
fn requirement_map_frontier_cache_generation_tracks_the_actual_owner() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
    let mut storage = Storage::<_, 512, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.memory.frontier_buffer_owner = FrontierBufferOwner::Map {
        collection_id: CollectionId(11),
        generation: 7,
        dirty: true,
    };

    assert_eq!(
        storage.cached_map_frontier_generation(CollectionId(11)),
        Some(7)
    );
    assert_eq!(
        storage.cached_map_frontier_generation(CollectionId(12)),
        None
    );

    assert_eq!(storage.assign_map_frontier_buffer(CollectionId(11)), 7);
    assert_eq!(
        storage.frontier_buffer_owner(),
        FrontierBufferOwner::Map {
            collection_id: CollectionId(11),
            generation: 7,
            dirty: false,
        }
    );

    assert_eq!(storage.assign_map_frontier_buffer(CollectionId(12)), 8);
    assert_eq!(
        storage.frontier_buffer_owner(),
        FrontierBufferOwner::Map {
            collection_id: CollectionId(12),
            generation: 8,
            dirty: false,
        }
    );
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-095` Storage facade WAL recovery append MUST reject recovery records when
//# no recovery boundary is pending.
#[test]
fn requirement_storage_facade_rejects_unneeded_wal_recovery_record() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    assert!(matches!(
        storage.append_wal_recovery(),
        Err(StorageRuntimeError::WalRecoveryNotNeeded)
    ));
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-096` Storage facade recovery status MUST report pending WAL recovery
//# boundaries and clear them after appending wal_recovery.
#[test]
fn requirement_storage_facade_reports_and_clears_pending_wal_recovery_boundary() {
    let mut flash = MockFlash::<256, 4, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    let metadata = flash.format_empty_store(1, 8, 0xa5).unwrap();
    let corrupt_offset = metadata.wal_record_area_offset().unwrap();
    flash.write_region(0, corrupt_offset, &[0x10; 8]).unwrap();

    let mut storage = Storage::<_, 256, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert!(storage.pending_wal_recovery_boundary());

    storage.append_wal_recovery().unwrap();

    assert!(!storage.pending_wal_recovery_boundary());
}

fn smallest_map_capacity_for_repeated_updates(update_count: usize) -> usize {
    (4..256)
        .find(|capacity| {
            let mut buffer = vec![0u8; *capacity];
            let mut map = MapFrontier::<u16, u16, 8>::new(
                CollectionId(200),
                &mut buffer,
                crate::test_map_frontier_memory(),
            )
            .unwrap();
            (0..update_count)
                .all(|index| map.set_in_memory(1, u16::try_from(index).unwrap()).is_ok())
        })
        .expect("expected a bounded map capacity within the search range")
}

struct CompletedTransactionCleanupResult {
    allocated_region: u32,
    reclaimed_region: u32,
    previous_tail: u32,
    previous_chain: heapless::Vec<u32, 8>,
    new_chain: heapless::Vec<u32, 8>,
    previous_tail_next: Option<u32>,
    reclaimed_tail_next: Option<u32>,
    reclaimed_footer_unwritten: bool,
    reopened_chain: heapless::Vec<u32, 8>,
    reopened_free_list_tail: Option<u32>,
    reclaim_operations: heapless::Vec<crate::MockOperation, 64>,
}

fn transaction_cleanup_returns_old_map_region_to_free_list_result(
) -> CompletedTransactionCleanupResult {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(14)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(14),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(3, 30).unwrap();
    let first_region = storage.flush_map::<_, _, 4>(&mut map).unwrap();

    map.set_in_memory(4, 40).unwrap();
    let previous_head = storage.last_free_list_head();
    let allocated_region = previous_head.unwrap();
    let previous_chain = storage.with_io_workspace(|flash, _workspace| {
        free_list_chain::<512, 7, 2048, 8>(&*flash, 0xff, previous_head)
    });
    let previous_tail = storage.free_list_tail().unwrap();
    storage.with_io_workspace(|flash, _workspace| flash.clear_operations());
    storage.flush_map::<_, _, 4>(&mut map).unwrap();

    let mut reclaim_operations = heapless::Vec::<crate::MockOperation, 64>::new();
    storage.with_io_workspace(|flash, _workspace| {
        for operation in flash.operations() {
            reclaim_operations.push(*operation).unwrap();
        }
    });

    let new_head = storage.last_free_list_head();
    let new_chain = storage.with_io_workspace(|flash, _workspace| {
        free_list_chain::<512, 7, 2048, 8>(&*flash, 0xff, new_head)
    });
    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let previous_tail_footer = storage
        .with_io_workspace(|flash, _workspace| {
            FreePointerFooter::decode(
                &flash.region_bytes(previous_tail).unwrap()[footer_offset..],
                0xff,
            )
        })
        .unwrap();
    let reclaimed_footer = storage
        .with_io_workspace(|flash, _workspace| {
            FreePointerFooter::decode(
                &flash.region_bytes(first_region).unwrap()[footer_offset..],
                0xff,
            )
        })
        .unwrap();
    let reclaimed_footer_unwritten = storage.with_io_workspace(|flash, _workspace| {
        flash.region_bytes(first_region).unwrap()
            [footer_offset..footer_offset + FreePointerFooter::ENCODED_LEN]
            .iter()
            .all(|byte| *byte == 0xff)
    });

    drop(storage);
    let mut reopened =
        Storage::<_, 512, 7>::open(&mut flash, crate::test_storage_memory()).unwrap();
    let reopened_head = reopened.last_free_list_head();
    let reopened_chain = reopened.with_io_workspace(|flash, _workspace| {
        free_list_chain::<512, 7, 2048, 8>(&*flash, 0xff, reopened_head)
    });

    CompletedTransactionCleanupResult {
        allocated_region,
        reclaimed_region: first_region,
        previous_tail,
        previous_chain,
        new_chain,
        previous_tail_next: previous_tail_footer.next_tail,
        reclaimed_tail_next: reclaimed_footer.next_tail,
        reclaimed_footer_unwritten,
        reopened_chain,
        reopened_free_list_tail: reopened.free_list_tail(),
        reclaim_operations,
    }
}

struct DelegatingFlash<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> {
    inner: MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>
    DelegatingFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn new(erased_byte: u8) -> Self {
        Self {
            inner: MockFlash::new(erased_byte),
        }
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> FlashIo
    for DelegatingFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, StorageIoError> {
        self.inner.read_metadata().map_err(StorageIoError::from)
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), StorageIoError> {
        self.inner
            .write_metadata(metadata)
            .map_err(StorageIoError::from)
    }

    fn read_region<R, F>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, StorageIoError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.inner
            .read_region(region_index, offset, len, read)
            .map_err(StorageIoError::from)
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageIoError> {
        self.inner
            .write_region(region_index, offset, data)
            .map_err(StorageIoError::from)
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), StorageIoError> {
        self.inner
            .erase_region(region_index)
            .map_err(StorageIoError::from)
    }

    fn sync(&mut self) -> Result<(), StorageIoError> {
        self.inner.sync().map_err(StorageIoError::from)
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, StorageFormatError> {
        self.inner
            .format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)
            .map_err(StorageFormatError::from)
    }
}

fn rotate_wal_tail_for_collection<'db, IO: FlashIo, const REGION_COUNT: usize>(
    storage: &mut Storage<'db, 'db, IO, 512, REGION_COUNT>,
    collection_id: CollectionId,
) -> u32 {
    for _ in 0..128 {
        match storage.append_wal_rotation_start() {
            Ok(region_index) => {
                storage.append_wal_rotation_finish(region_index).unwrap();
                return region_index;
            }
            Err(StorageRuntimeError::InvalidRotationWindow { .. }) => {
                storage.append_update(collection_id, &[0]).unwrap()
            }
            Err(other) => panic!("unexpected rotation-start error: {other:?}"),
        }
    }

    panic!("WAL tail rotation did not reach a valid rotation window");
}

fn wal_and_map_region_formats() -> (Header, Header) {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    let wal_header = storage
        .with_io_workspace(|flash, _workspace| {
            Header::decode(&flash.region_bytes(0).unwrap()[..Header::ENCODED_LEN])
        })
        .unwrap();

    storage.create_map(CollectionId(43)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(43),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(3, 30).unwrap();

    let region_index = storage.flush_map::<_, _, 4>(&mut map).unwrap();
    let map_header = storage
        .with_io_workspace(|flash, _workspace| {
            Header::decode(&flash.region_bytes(region_index).unwrap()[..Header::ENCODED_LEN])
        })
        .unwrap();

    (wal_header, map_header)
}

//= spec/ring/05-disk-format.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-004` `collection_format` is a stable per-region `u16`
//# namespace recorded durably in region headers. The pair
//# `(collection_type, collection_format)` identifies a concrete
//# committed region payload encoding. Borromean core reserves
//# `collection_format = 0x0000` globally for `wal_v1`; every non-WAL
//# collection format MUST be nonzero.
#[test]
fn requirement_wal_and_map_regions_use_distinct_collection_format_namespace_values() {
    let (wal_header, map_header) = wal_and_map_region_formats();

    assert_eq!(WAL_V1_FORMAT, 0);
    assert_eq!(wal_header.collection_id, CollectionId(0));
    assert_eq!(wal_header.collection_format, WAL_V1_FORMAT);
    assert_eq!(map_header.collection_id, CollectionId(43));
    assert_eq!(map_header.collection_format, MAP_MANIFEST_V2_FORMAT);
    assert_ne!(MAP_MANIFEST_V2_FORMAT, WAL_V1_FORMAT);
    assert!(map_header.collection_format > 0);
}

//= spec/ring/05-disk-format.md#storage-requirements
//= type=test
//# `RING-STORAGE-005` Borromean core MUST reserve the canonical
//# `collection_format` value `wal_v1` for WAL regions, and user
//# collections MUST NOT use that identifier.
#[test]
fn requirement_wal_v1_collection_format_is_reserved_to_wal_regions() {
    let (wal_header, map_header) = wal_and_map_region_formats();

    assert_eq!(wal_header.collection_format, WAL_V1_FORMAT);
    assert_eq!(wal_header.collection_id, CollectionId(0));
    assert_ne!(map_header.collection_format, WAL_V1_FORMAT);
}

fn setup_storage_with_stale_wal_head<'db>(
    flash: &'db mut MockFlash<512, 8, 4096>,
) -> (Storage<'db, 'db, MockFlash<512, 8, 4096>, 512, 8, 8>, u32) {
    let mut storage = Storage::<_, 512, 8>::format(
        flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage
        .append_new_collection(CollectionId(31), CollectionType::MAP_CODE)
        .unwrap();
    storage.append_update(CollectionId(31), &[1, 2, 3]).unwrap();

    let next_region = rotate_wal_tail_for_collection(&mut storage, CollectionId(31));
    storage
        .append_snapshot(CollectionId(31), CollectionType::MAP_CODE, &[9, 8, 7])
        .unwrap();

    (storage, next_region)
}

fn setup_storage_with_live_snapshot_in_wal_head<'db>(
    flash: &'db mut MockFlash<512, 6, 4096>,
) -> (Storage<'db, 'db, MockFlash<512, 6, 4096>, 512, 6, 8>, u32) {
    let mut storage = Storage::<_, 512, 6>::format(
        flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage
        .append_new_collection(CollectionId(32), CollectionType::MAP_CODE)
        .unwrap();
    storage
        .append_snapshot(CollectionId(32), CollectionType::MAP_CODE, &[4, 5, 6])
        .unwrap();
    storage
        .append_new_collection(CollectionId(132), CollectionType::MAP_CODE)
        .unwrap();
    storage
        .append_snapshot(CollectionId(132), CollectionType::MAP_CODE, &[0])
        .unwrap();

    let next_region = rotate_wal_tail_for_collection(&mut storage, CollectionId(132));

    (storage, next_region)
}

fn setup_storage_with_live_snapshot_and_update_in_wal_head<'db>(
    flash: &'db mut MockFlash<512, 6, 4096>,
) -> (Storage<'db, 'db, MockFlash<512, 6, 4096>, 512, 6, 8>, u32) {
    let mut storage = Storage::<_, 512, 6>::format(
        flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage
        .append_new_collection(CollectionId(33), CollectionType::MAP_CODE)
        .unwrap();
    storage
        .append_snapshot(CollectionId(33), CollectionType::MAP_CODE, &[7, 8, 9])
        .unwrap();
    storage.append_update(CollectionId(33), &[1, 3, 5]).unwrap();
    storage
        .append_new_collection(CollectionId(133), CollectionType::MAP_CODE)
        .unwrap();
    storage
        .append_snapshot(CollectionId(133), CollectionType::MAP_CODE, &[0])
        .unwrap();

    let next_region = rotate_wal_tail_for_collection(&mut storage, CollectionId(133));

    (storage, next_region)
}

fn setup_storage_with_live_empty_head_map_in_wal_head<'db>(
    flash: &'db mut MockFlash<512, 6, 4096>,
) -> (Storage<'db, 'db, MockFlash<512, 6, 4096>, 512, 6, 8>, u32) {
    let mut storage = Storage::<_, 512, 6>::format(
        flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(36)).unwrap();
    let mut target_payload = [0u8; 64];
    storage
        .append_map_update::<i32, i32>(CollectionId(36), &MapUpdate::Set { key: 1, value: 10 })
        .unwrap();

    storage.create_map(CollectionId(136)).unwrap();

    let next_region = rotate_wal_tail_for_collection(&mut storage, CollectionId(136));
    storage
        .append_snapshot(
            CollectionId(136),
            CollectionType::MAP_CODE,
            &crate::EMPTY_MAP_SNAPSHOT,
        )
        .unwrap();

    (storage, next_region)
}

//= spec/implementation.md#operation-future-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-097` Storage format futures MUST poll to completion and return initialized
//# storage state.
#[test]
fn requirement_storage_format_future_polls_to_completion() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    let storage = poll_ready(Storage::<_, 256, 4>::format_future(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    ))
    .unwrap();

    assert_eq!(storage.metadata().region_size, 256);
    assert_eq!(storage.metadata().region_count, 4);
    assert_eq!(storage.wal_head(), 0);
    assert_eq!(storage.last_free_list_head(), Some(1));
    assert_eq!(storage.free_list_tail(), Some(3));
}

//= spec/implementation.md#operation-future-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-098` Storage open futures MUST poll to completion and replay collection
//# pending update state.
#[test]
fn requirement_storage_open_future_polls_to_completion() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    {
        let mut storage = Storage::<_, 256, 4>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        storage
            .append_new_collection(CollectionId(7), CollectionType::MAP_CODE)
            .unwrap();
        storage.append_update(CollectionId(7), &[1, 2, 3]).unwrap();
    }

    let reopened = poll_until_ready(
        Storage::<_, 256, 4>::open_future(&mut flash, crate::test_storage_memory()),
        8,
    )
    .unwrap();

    assert_eq!(reopened.collections().len(), 1);
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(7));
    assert_eq!(reopened.collections()[0].pending_update_count(), 1);
}

//= spec/implementation.md#operation-future-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-099` Storage open futures MUST yield pending between startup phases before
//# completing with recovered WAL head and tail.
#[test]
fn requirement_storage_open_future_yields_between_startup_phases() {
    let mut flash = MockFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    Storage::<_, 256, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    let future = Storage::<_, 256, 4>::open_future(&mut flash, crate::test_storage_memory());
    let mut future = pin!(future);

    assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    assert!(matches!(poll_once(future.as_mut()), Poll::Pending));

    let reopened = match poll_until_ready(future, 5) {
        Ok(reopened) => reopened,
        Err(error) => panic!("unexpected open error: {error:?}"),
    };
    assert_eq!(reopened.wal_head(), 0);
    assert_eq!(reopened.wal_tail(), 0);
}

//= spec/implementation.md#operation-future-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-100` Dropping a partially polled storage open future MUST leave the store
//# openable with unchanged recovered state.
#[test]
fn requirement_storage_open_future_drop_before_completion_leaves_store_openable() {
    let mut flash = MockFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    Storage::<_, 256, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    {
        let future = Storage::<_, 256, 4>::open_future(&mut flash, crate::test_storage_memory());
        let mut future = pin!(future);
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    }

    let mut reopened =
        Storage::<_, 256, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.wal_head(), 0);
    assert_eq!(reopened.wal_tail(), 0);
    assert!(reopened.collections().is_empty());
}

//= spec/implementation.md#operation-future-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-101` Storage WAL-head reclaim futures MUST poll to completion, update WAL
//# head to the reclaimed continuation, and append the old WAL prefix to the free-list tail.
#[test]
fn requirement_storage_reclaim_wal_head_future_polls_to_completion() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, next_region) = setup_storage_with_stale_wal_head(&mut flash);

    let reclaimed_head = poll_until_ready(storage.reclaim_wal_head_future(), 16).unwrap();

    assert_eq!(reclaimed_head, storage.wal_head());
    assert_eq!(storage.wal_head(), storage.wal_tail());
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.free_list_tail(), Some(0));
}

//= spec/implementation.md#operation-future-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-102` Storage WAL-head reclaim futures MUST yield between reclaim phases
//# before completing with updated WAL head.
#[test]
fn requirement_storage_reclaim_wal_head_future_yields_between_reclaim_phases() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, next_region) = setup_storage_with_stale_wal_head(&mut flash);

    let (first, second, third, fourth, fifth, reclaimed_head) = {
        let future = storage.reclaim_wal_head_future();
        let mut future = pin!(future);

        let first = matches!(poll_once(future.as_mut()), Poll::Pending);
        let second = matches!(poll_once(future.as_mut()), Poll::Pending);
        let third = matches!(poll_once(future.as_mut()), Poll::Pending);
        let fourth = matches!(poll_once(future.as_mut()), Poll::Pending);
        let fifth = matches!(poll_once(future.as_mut()), Poll::Pending);
        let reclaimed_head = poll_until_ready(future, 16).unwrap();

        (first, second, third, fourth, fifth, reclaimed_head)
    };

    assert!(first);
    assert!(second);
    assert!(third);
    assert!(fourth);
    assert!(fifth);
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), reclaimed_head);
}

//= spec/implementation.md#operation-future-regression-requirements
//= type=test
//# `RING-IMPL-REGRESSION-103` Dropping a WAL-head reclaim future after reclaim begins MUST leave
//# the store recoverable with original WAL head and live collection basis.
#[test]
fn requirement_storage_reclaim_wal_head_future_drop_after_begin_remains_recoverable() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, _next_region) = setup_storage_with_stale_wal_head(&mut flash);
    let original_head = storage.wal_head();

    {
        let future = storage.reclaim_wal_head_future();
        let mut future = pin!(future);

        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    }
    assert!(matches!(
        storage.append_update(CollectionId(999), &[4]),
        Err(StorageRuntimeError::UnknownCollection(CollectionId(999)))
    ));

    drop(storage);
    let mut reopened =
        Storage::<_, 512, 8>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.wal_head(), original_head);
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::WalSnapshot
    );
}

//= spec/implementation.md#operation-requirements
//= type=test
//# `RING-IMPL-OP-002` A Borromean future MUST either complete with a terminal result or remain
//# safely resumable by further polling after any `Poll::Pending`.
#[test]
fn requirement_storage_map_operation_futures_poll_to_completion() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    poll_ready(storage.create_map_future(CollectionId(41))).unwrap();

    let mut source_buffer = [0u8; 512];
    let mut source = MapFrontier::<i32, i32, 4>::new(
        CollectionId(41),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source.set_in_memory(1, 10).unwrap();
    poll_ready(storage.snapshot_map_future(&source)).unwrap();

    let mut payload_buffer = [0u8; 128];
    poll_ready(storage.append_map_update_future::<i32, i32>(
        CollectionId(41),
        &MapUpdate::Set { key: 2, value: 20 },
    ))
    .unwrap();

    source.set_in_memory(3, 30).unwrap();
    let committed_region =
        poll_until_ready(storage.flush_map_future::<_, _, 4>(&mut source), 4).unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(committed_region)
    );

    let reclaim_region = poll_ready(storage.drop_map_future(CollectionId(41))).unwrap();

    assert_eq!(reclaim_region, Some(committed_region));
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
}

//= spec/implementation.md#execution-requirements
//= type=test
//# `RING-IMPL-EXEC-005` Await boundaries inside Borromean operations MUST align only with
//# externally visible I/O steps or with pure in-memory decision points that preserve the ring
//# ordering rules.
#[test]
fn requirement_storage_flush_map_future_yields_between_durable_phases() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(42)).unwrap();

    let region_index = {
        let mut map_buffer = [0u8; 512];
        let mut map = MapFrontier::<i32, i32, 4>::new(
            CollectionId(42),
            &mut map_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
        map.set_in_memory(5, 50).unwrap();

        let future = storage.flush_map_future::<_, _, 4>(&mut map);
        let mut future = pin!(future);

        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));

        match poll_once(future.as_mut()) {
            Poll::Ready(Ok(region_index)) => region_index,
            other => panic!("unexpected third poll result: {other:?}"),
        }
    };

    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(region_index)
    );
    assert_eq!(storage.ready_region(), None);
}

//= spec/implementation.md#operation-requirements
//= type=test
//# `RING-IMPL-OP-003` If an operation future is dropped before completion, any already-issued
//# durable writes MUST still satisfy the crash-safety rules from
//# [spec/ring/00-introduction.md](ring/00-introduction.md).
#[test]
fn requirement_storage_flush_map_future_drop_after_region_write_remains_recoverable() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(43)).unwrap();

    {
        let mut map_buffer = [0u8; 512];
        let mut map = MapFrontier::<i32, i32, 4>::new(
            CollectionId(43),
            &mut map_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
        map.set_in_memory(7, 70).unwrap();

        let future = storage.flush_map_future::<_, _, 4>(&mut map);
        let mut future = pin!(future);

        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    }

    assert_eq!(storage.ready_region(), None);
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );

    drop(storage);
    let mut reopened =
        Storage::<_, 512, 7>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );
    assert_eq!(reopened.ready_region(), None);
}

//= spec/implementation.md#architecture-requirements
//= type=todo
//# `RING-IMPL-ARCH-001` `Storage` MUST own logical storage state, configuration, bounded operation
//# scratch, and exclusive access to the backing object by value or mutable reference for the
//# lifetime of the opened database.
#[test]
fn todo_storage_format_binds_backing_and_scratch() {}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-104` Storage append operations MUST persist new collection and update
//# records so reopening through flash restores the collection and pending update state.
#[test]
fn requirement_storage_append_and_reopen_round_trip_through_flash() {
    let mut flash = MockFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    {
        let mut storage = Storage::<_, 256, 4>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        storage
            .append_new_collection(CollectionId(7), CollectionType::MAP_CODE)
            .unwrap();
        storage.append_update(CollectionId(7), &[1, 2, 3]).unwrap();

        assert_eq!(storage.collections().len(), 1);
        assert_eq!(storage.collections()[0].collection_id(), CollectionId(7));
        assert_eq!(storage.collections()[0].pending_update_count(), 1);
    }

    let mut reopened =
        Storage::<_, 256, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();

    assert_eq!(reopened.collections().len(), 1);
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(7));
    assert_eq!(
        reopened.collections()[0].collection_type(),
        Some(CollectionType::MAP_CODE)
    );
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );
    assert_eq!(reopened.collections()[0].pending_update_count(), 1);
}

//= spec/ring/03-collection-lifecycle.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-012` Every non-WAL `collection_type` that may appear durably on disk MUST have a
//# corresponding normative collection specification.
#[test]
fn requirement_storage_append_new_collection_rejects_unsupported_channel_collection() {
    let mut flash = MockFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    assert!(matches!(
        Storage::<_, 256, 4>::format(
            &mut flash,
            StorageFormatConfig::new(1, 8, 0xa5),
            crate::test_storage_memory()
        )
        .unwrap()
        .append_new_collection(CollectionId(22), CollectionType::CHANNEL_CODE,),
        Err(StorageRuntimeError::UnsupportedCollectionType(
            CollectionType::CHANNEL_CODE
        ))
    ));
}

//= spec/implementation.md#api-requirements
//= type=todo
//# `RING-IMPL-API-001` Public format and open entry points MUST bind a backing implementation and
//# bounded operation scratch into the returned `Storage` context, and normal replay or mutating
//# operations MUST use those dependencies through `Storage`.
#[test]
fn todo_storage_api_binds_backing_and_scratch() {}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-105` WAL-head reclaim MUST update runtime WAL head and tail to a fresh
//# continuation region.
#[test]
fn requirement_storage_reclaim_wal_head_updates_runtime_head_to_next_region() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, next_region) = setup_storage_with_stale_wal_head(&mut flash);

    let reclaimed_head = storage.reclaim_wal_head().unwrap();

    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), reclaimed_head);
    assert_eq!(storage.wal_tail(), reclaimed_head);
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-008` Borromean MUST model WAL-head movement as ordinary
//# `head(collection_id = 0, collection_type = wal, region_index = ...)`
//# records rather than a WAL-specific head record type.
#[test]
fn requirement_storage_reclaim_wal_head_appends_an_ordinary_head_record_for_wal_movement() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, next_region) = setup_storage_with_stale_wal_head(&mut flash);

    let reclaimed_head = storage.reclaim_wal_head().unwrap();

    let mut saw_wal_head_record = false;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, record| {
                if let WalRecord::Head {
                    collection_id,
                    collection_type,
                    region_index,
                } = record
                {
                    if collection_id == CollectionId(0)
                        && collection_type == CollectionType::WAL_CODE
                        && region_index == reclaimed_head
                    {
                        saw_wal_head_record = true;
                    }
                }
                Ok(())
            })
        })
        .unwrap();

    assert_eq!(reclaimed_head, next_region);
    assert!(saw_wal_head_record);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-PRE-001` The candidate region MUST be the head of
//# the WAL.
#[test]
fn requirement_storage_reclaim_wal_head_returns_old_head_region_to_free_list_tail() {
    let mut flash = MockFlash::<512, 8, 4096>::new(0xff);
    let (mut storage, _) = setup_storage_with_stale_wal_head(&mut flash);

    storage.reclaim_wal_head().unwrap();

    assert_eq!(storage.free_list_tail(), Some(0));
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-PRE-002` For every live record in the candidate, an equivalent live state MUST
//# already be represented durably outside the candidate.
#[test]
fn requirement_storage_reclaim_wal_head_copies_live_snapshot_basis_to_tail() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (mut storage, next_region) = setup_storage_with_live_snapshot_in_wal_head(&mut flash);

    let reclaimed_head = storage.reclaim_wal_head().unwrap();

    let mut saw_snapshot = false;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, record| {
                if let WalRecord::Snapshot {
                    collection_id,
                    payload,
                    ..
                } = record
                {
                    if collection_id == CollectionId(32) {
                        assert_eq!(payload, &[4, 5, 6]);
                        saw_snapshot = true;
                    }
                }
                Ok(())
            })
        })
        .unwrap();

    assert!(saw_snapshot);
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), reclaimed_head);
    assert!(storage.wal_tail() >= reclaimed_head);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-001` A collection's `H(c)`, `B(c)`, and live
//# post-basis updates MUST NOT depend on bytes in the reclaimed region.
#[test]
fn requirement_storage_reclaim_wal_head_copies_live_updates_after_basis_to_tail() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (mut storage, next_region) =
        setup_storage_with_live_snapshot_and_update_in_wal_head(&mut flash);

    let reclaimed_head = storage.reclaim_wal_head().unwrap();

    let mut saw_snapshot = false;
    let mut saw_update = false;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, record| {
                match record {
                    WalRecord::Snapshot {
                        collection_id: CollectionId(33),
                        payload,
                        ..
                    } => {
                        assert_eq!(payload, &[7, 8, 9]);
                        saw_snapshot = true;
                    }
                    WalRecord::Update {
                        collection_id: CollectionId(33),
                        payload,
                    } => {
                        assert_eq!(payload, &[1, 3, 5]);
                        saw_update = true;
                    }
                    _ => {}
                }
                Ok(())
            })
        })
        .unwrap();

    assert!(saw_snapshot);
    assert!(saw_update);
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), reclaimed_head);
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::WalSnapshot
    );
    assert_eq!(storage.collections()[0].pending_update_count(), 1);
}

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-106` WAL-head reclaim MUST rewrite a live
//# `EmptyClean` map as a WAL snapshot basis while preserving pending
//# updates.
#[test]
fn requirement_storage_reclaim_wal_head_rewrites_empty_head_map_as_snapshot_basis() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (mut storage, next_region) = setup_storage_with_live_empty_head_map_in_wal_head(&mut flash);

    let reclaimed_head = storage.reclaim_wal_head().unwrap();

    let target = storage
        .collections()
        .iter()
        .find(|collection| collection.collection_id() == CollectionId(36))
        .unwrap();
    assert_eq!(target.basis(), StartupCollectionBasis::WalSnapshot);
    assert_eq!(target.pending_update_count(), 1);
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), reclaimed_head);
}

#[derive(Debug, Clone, Copy)]
struct StressRng {
    state: u64,
}

impl StressRng {
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

//= spec/ring/09-implementation-coverage.md#storage-runtime-state-requirements
//= type=test
//# `RING-IMPL-REGRESSION-108` A long mixed map workload MUST preserve collection identity across
//# writes, deletes, compactions, and storage reclamation.
#[test]
fn requirement_long_mixed_map_workload_preserves_collection_identity_across_reclaim_and_compaction()
{
    std::thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(
            run_long_mixed_map_workload_preserves_collection_identity_across_reclaim_and_compaction,
        )
        .unwrap()
        .join()
        .unwrap();
}

fn run_long_mixed_map_workload_preserves_collection_identity_across_reclaim_and_compaction() {
    const REGION_SIZE: usize = 4096;
    const REGION_COUNT: usize = 1024;
    const MAX_LOG: usize = 1_048_576;
    const MAX_COLLECTIONS: usize = 8;
    const MAX_RUNS: usize = 128;

    let mut flash =
        std::boxed::Box::new(MockFlash::<REGION_SIZE, REGION_COUNT, MAX_LOG>::new(0xff));
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>::format(
        &mut *flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut map = LsmMap::<u64, u64, MAX_RUNS>::new(&mut storage, crate::test_lsm_map_memory())
        .unwrap()
        .with_compaction_run_target(8)
        .unwrap();
    let mut rng = StressRng::new(0x0123_4567_89ab_cdef);
    let initial_wal_head = storage.wal_head();
    let mut compactions = 0usize;
    let mut reclaims = 0usize;
    let mut deletes = 0usize;
    let mut sets = 0usize;

    for operation_index in 0..3_000 {
        let key = rng.next_bounded(256);
        let compact_needed = if rng.next_bounded(100) < 30 {
            deletes += 1;
            map.delete(&mut storage, key).unwrap()
        } else {
            sets += 1;
            map.set(&mut storage, key, rng.next_u64()).unwrap()
        };
        if compact_needed || operation_index % 500 == 499 {
            map.compact(&mut storage).unwrap_or_else(|error| {
                panic!(
                    "compact failed at op {operation_index}: {error:?}; wal_head={} wal_tail={} append_offset={} collections={:?}",
                    storage.wal_head(),
                    storage.wal_tail(),
                    storage.wal_append_offset(),
                    storage.collections(),
                )
            });
            compactions += 1;
        }
        if operation_index % 500 == 499 && storage.wal_head() != storage.wal_tail() {
            storage.reclaim_wal_head().unwrap_or_else(|error| {
                panic!(
                    "wal reclaim failed at op {operation_index}: {error:?}; wal_head={} wal_tail={} append_offset={} collections={:?}",
                    storage.wal_head(),
                    storage.wal_tail(),
                    storage.wal_append_offset(),
                    storage.collections(),
                )
            });
            reclaims += 1;
        }
        storage.with_io_workspace(|flash, _workspace| flash.clear_operations());
    }

    assert!(sets > 0);
    assert!(deletes > 0);
    assert!(compactions > 0);
    assert!(reclaims > 0);
    assert_ne!(storage.wal_head(), initial_wal_head);
    assert!(storage
        .collections()
        .iter()
        .any(|collection| collection.collection_id() == map.collection_id()));
}

#[derive(Debug)]
struct ReclaimReplaySnapshot {
    collections: heapless::Vec<StartupCollection, 8>,
    last_free_list_head: Option<u32>,
    ready_region: Option<u32>,
    free_list_tail: Option<u32>,
    wal_head: u32,
}

fn reclaim_wal_head_and_reopen_empty_head_map<'db>(
    flash: &'db mut MockFlash<512, 6, 4096>,
) -> (
    ReclaimReplaySnapshot,
    Storage<'db, 'db, MockFlash<512, 6, 4096>, 512, 6, 8>,
    Option<u32>,
    Option<u32>,
) {
    let (mut storage, _) = setup_storage_with_live_empty_head_map_in_wal_head(flash);
    let expected_free_list_head = storage.last_free_list_head();
    let expected_ready_region = storage.ready_region();

    storage.reclaim_wal_head().unwrap();

    let snapshot = ReclaimReplaySnapshot {
        collections: heapless::Vec::from_slice(storage.collections()).unwrap(),
        last_free_list_head: storage.last_free_list_head(),
        ready_region: storage.ready_region(),
        free_list_tail: storage.free_list_tail(),
        wal_head: storage.wal_head(),
    };
    drop(storage);
    let mut reopened = Storage::<_, 512, 6>::open(flash, crate::test_storage_memory()).unwrap();

    (
        snapshot,
        reopened,
        expected_free_list_head,
        expected_ready_region,
    )
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-PRE-003` After planned metadata updates, startup
//# replay MUST still be able to walk a valid WAL chain from head to
//# tail.
#[test]
fn requirement_storage_reclaim_wal_head_reopen_keeps_the_wal_chain_walkable() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (_, mut reopened, _, _) = reclaim_wal_head_and_reopen_empty_head_map(&mut flash);

    let mut record_count = 0usize;
    reopened
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, _record| {
                record_count += 1;
                Ok(())
            })
        })
        .unwrap();

    assert!(record_count > 0);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-SAFE-001` Reclaim MUST NOT change replay result: the recovered collection
//# submachine state and pending updates for every collection, the recovered
//# `last_free_list_head`, reserved WAL-rotation `ready_region`,
//# transaction recovery state, and reconstructed `free_list_tail`, after
//# reclaim must match the pre-reclaim logical state.
#[test]
fn requirement_storage_reclaim_wal_head_reopen_preserves_replay_result() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (snapshot, mut reopened, _, _) = reclaim_wal_head_and_reopen_empty_head_map(&mut flash);

    assert_eq!(reopened.collections(), snapshot.collections.as_slice());
    assert_eq!(reopened.last_free_list_head(), snapshot.last_free_list_head);
    assert_eq!(reopened.ready_region(), snapshot.ready_region);
    assert_eq!(reopened.free_list_tail(), snapshot.free_list_tail);

    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<i32, i32, 4>(
            CollectionId(36),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();

    assert_eq!(reopened_map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(reopened_map.get_frontier(&2).unwrap(), None);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-005` Startup step 4 MUST recover the same effective WAL head after
//# reclaim as before reclaim, using the current tail region's
//# `WalRegionPrologue` plus the last valid tail-local
//# `head(collection_id = 0, collection_type = wal, region_index = ...)`
//# override, if any.
#[test]
fn requirement_storage_reclaim_wal_head_reopen_preserves_effective_wal_head() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (snapshot, reopened, _, _) = reclaim_wal_head_and_reopen_empty_head_map(&mut flash);

    assert_eq!(reopened.wal_head(), snapshot.wal_head);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-002` The recovered free-list head MUST match pre-reclaim allocator state.
#[test]
fn requirement_storage_reclaim_wal_head_reopen_preserves_free_list_head() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (snapshot, reopened, _, _) = reclaim_wal_head_and_reopen_empty_head_map(&mut flash);

    assert_eq!(reopened.last_free_list_head(), snapshot.last_free_list_head);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-003` The recovered WAL-rotation `ready_region`, if any, MUST match
//# pre-reclaim allocator state.
#[test]
fn requirement_storage_reclaim_wal_head_reopen_preserves_ready_region() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (_, reopened, _, expected_ready_region) =
        reclaim_wal_head_and_reopen_empty_head_map(&mut flash);

    assert_eq!(reopened.ready_region(), expected_ready_region);
}

//= spec/ring/07-reclaim.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-006` WAL chain integrity MUST remain valid
//# with no broken `link` path.
#[test]
fn requirement_storage_reclaim_wal_head_reopen_has_no_broken_link_path() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let (_, mut reopened, _, _) = reclaim_wal_head_and_reopen_empty_head_map(&mut flash);
    let mut reopen_buffer = [0u8; 512];

    reopened
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, _record| Ok(()))
        })
        .unwrap();

    let reopened_map = reopened
        .open_map::<i32, i32, 4>(
            CollectionId(36),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(
        reopened
            .with_io_workspace(|flash, workspace| reopened_map.get::<512, _>(flash, workspace, &1))
            .unwrap(),
        Some(10)
    );
}

//= spec/implementation.md#i-o-requirements
//= type=test
//# `RING-IMPL-REGRESSION-107` Storage operations MUST work through any backing implementation that
//# implements the trait, including delegating or synchronized backings.
#[test]
fn requirement_storage_works_through_flash_io_trait_backend() {
    let mut flash = DelegatingFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    let mut storage = Storage::<_, 256, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage
        .append_new_collection(CollectionId(9), CollectionType::MAP_CODE)
        .unwrap();
    storage.append_update(CollectionId(9), &[4, 5]).unwrap();

    let mut reopened =
        Storage::<_, 256, 4>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.collections().len(), 1);
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(9));
    assert_eq!(reopened.collections()[0].pending_update_count(), 1);
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-108` Storage map APIs MUST restore snapshot basis values and later typed
//# updates when opening a map.
#[test]
fn requirement_storage_map_api_restores_snapshot_and_updates() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(11)).unwrap();

    let mut source_buffer = [0u8; 512];
    let mut source = MapFrontier::<i32, i32, 4>::new(
        CollectionId(11),
        &mut source_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    source.set_in_memory(1, 10).unwrap();
    source.set_in_memory(2, 20).unwrap();
    storage.snapshot_map(&source).unwrap();

    let mut update_payload = [0u8; 64];
    let update_len = MapFrontier::<i32, i32, 4>::encode_update_into(
        &MapUpdate::Set { key: 2, value: 99 },
        &mut update_payload,
    )
    .unwrap();
    storage
        .append_update(CollectionId(11), &update_payload[..update_len])
        .unwrap();

    let mut reopen_buffer = [0u8; 512];
    let reopened = storage
        .open_map::<i32, i32, 4>(
            CollectionId(11),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();

    assert_eq!(reopened.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(reopened.get_frontier(&2).unwrap(), Some(99));
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-012` The implementation MUST maintain
//# `min_free_regions >= max_in_memory_dirty_collections + 1` so every
//# storage-managed dirty frontier can be preserved using one committed
//# region while one additional region remains reserved for WAL rotation,
//# transaction terminal records, or crash recovery.
#[test]
fn requirement_storage_map_frontiers_do_not_exceed_the_configured_dirty_collection_reserve() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 6;
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(48)).unwrap();
    storage.create_map(CollectionId(49)).unwrap();

    let mut first_buffer = [0u8; 128];
    let mut second_buffer = [0u8; 128];
    let mut first_map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(48),
        &mut first_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    let mut second_map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(49),
        &mut second_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    let mut payload_buffer = [0u8; 64];

    first_map
        .apply_update(&mut storage, &MapUpdate::Set { key: 1, value: 10 })
        .unwrap();

    let error = second_map
        .apply_update(&mut storage, &MapUpdate::Set { key: 2, value: 20 })
        .unwrap_err();

    assert!(matches!(
        error,
        MapStorageError::Storage(StorageRuntimeError::TooManyDirtyFrontiers {
            dirty_frontiers: 2,
            min_free_regions: 2,
        })
    ));
    assert_eq!(first_map.get_frontier(&1).unwrap(), Some(10));
    assert_eq!(second_map.get_frontier(&2).unwrap(), None);

    storage.flush_map::<u16, u16, 8>(&mut first_map).unwrap();

    second_map
        .apply_update(&mut storage, &MapUpdate::Set { key: 2, value: 20 })
        .unwrap();
    assert_eq!(second_map.get_frontier(&2).unwrap(), Some(20));

    {
        let mut flash = MockFlash::<REGION_SIZE, 9, 8192>::new(0xff);
        let mut storage = Storage::<_, REGION_SIZE, 9>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let first_id = CollectionId(50);
        let second_id = CollectionId(51);
        storage.create_map(first_id).unwrap();
        storage.create_map(second_id).unwrap();

        let mut first_buffer = [0u8; REGION_SIZE];
        let mut first_frontier = MapFrontier::<u16, u16, 4>::new(
            first_id,
            &mut first_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
        first_frontier.set_in_memory(1, 10).unwrap();
        storage
            .flush_map::<u16, u16, 4>(&mut first_frontier)
            .unwrap();
        first_frontier.set_in_memory(2, 20).unwrap();
        storage
            .flush_map::<u16, u16, 4>(&mut first_frontier)
            .unwrap();

        let mut first_map =
            LsmMap::<u16, u16, 4>::open(first_id, &mut storage, crate::test_lsm_map_memory())
                .unwrap()
                .with_compaction_run_target(1)
                .unwrap();
        let mut second_map =
            LsmMap::<u16, u16, 4>::open(second_id, &mut storage, crate::test_lsm_map_memory())
                .unwrap();

        assert!(first_map.set(&mut storage, 3, 30).unwrap());
        let error = second_map.set(&mut storage, 4, 40).unwrap_err();
        assert!(matches!(
            error,
            MapStorageError::Storage(StorageRuntimeError::TooManyDirtyFrontiers {
                dirty_frontiers: 2,
                min_free_regions: 2,
            })
        ));

        let error = first_map.compact_and_report(&mut storage).unwrap_err();
        assert!(matches!(
            error,
            MapStorageError::Storage(StorageRuntimeError::InsufficientFreeRegions {
                free_regions: 5,
                min_free_regions: 2,
            })
        ));
    }

    {
        let mut flash = MockFlash::<REGION_SIZE, 12, 8192>::new(0xff);
        let mut storage = Storage::<_, REGION_SIZE, 12>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let first_id = CollectionId(52);
        let second_id = CollectionId(53);
        storage.create_map(first_id).unwrap();
        storage.create_map(second_id).unwrap();

        let mut first_buffer = [0u8; REGION_SIZE];
        let mut first_frontier = MapFrontier::<u16, u16, 4>::new(
            first_id,
            &mut first_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
        first_frontier.set_in_memory(1, 10).unwrap();
        storage
            .flush_map::<u16, u16, 4>(&mut first_frontier)
            .unwrap();
        first_frontier.set_in_memory(2, 20).unwrap();
        storage
            .flush_map::<u16, u16, 4>(&mut first_frontier)
            .unwrap();

        let mut first_map =
            LsmMap::<u16, u16, 4>::open(first_id, &mut storage, crate::test_lsm_map_memory())
                .unwrap()
                .with_compaction_run_target(1)
                .unwrap();
        let mut second_map =
            LsmMap::<u16, u16, 4>::open(second_id, &mut storage, crate::test_lsm_map_memory())
                .unwrap();

        assert!(first_map.set(&mut storage, 3, 30).unwrap());
        let error = second_map.set(&mut storage, 4, 40).unwrap_err();
        assert!(matches!(
            error,
            MapStorageError::Storage(StorageRuntimeError::TooManyDirtyFrontiers {
                dirty_frontiers: 2,
                min_free_regions: 2,
            })
        ));

        assert!(first_map.compact_and_report(&mut storage).unwrap());
        second_map.set(&mut storage, 4, 40).unwrap();
        assert_eq!(
            second_map.get(&mut storage, &4, |_, value| *value).unwrap(),
            Some(40)
        );
    }
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-016` If applying another update would exceed that
//# capacity, the implementation MUST flush the collection's current
//# logical frontier into collection-defined committed state, durably commit
//# a new collection head, and clear the in-memory frontier before accepting
//# further updates for that collection.
#[test]
fn requirement_storage_map_frontier_overflow_flushes_and_commits_a_new_region_head() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 6;
    let capacity = smallest_map_capacity_for_repeated_updates(3);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(46)).unwrap();

    let mut map_buffer = vec![0u8; capacity];
    let mut map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(46),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    let mut payload_buffer = [0u8; 64];

    map.apply_update(&mut storage, &MapUpdate::Set { key: 1, value: 10 })
        .unwrap();
    map.apply_update(&mut storage, &MapUpdate::Set { key: 1, value: 20 })
        .unwrap();
    map.apply_update(&mut storage, &MapUpdate::Set { key: 1, value: 30 })
        .unwrap();

    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );

    map.apply_update(&mut storage, &MapUpdate::Set { key: 1, value: 40 })
        .unwrap();

    let StartupCollectionBasis::Region(region_index) = storage.collections()[0].basis() else {
        panic!("frontier overflow should commit a durable region head");
    };

    let mut seen = heapless::Vec::<WalRecordType, 32>::new();
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<REGION_SIZE, _, (), _>(
                flash,
                workspace,
                |_flash, record| {
                    seen.push(record.record_type()).unwrap();
                    Ok(())
                },
            )
        })
        .unwrap();

    assert!(seen.contains(&WalRecordType::BeginTransaction));
    assert!(seen.contains(&WalRecordType::CommitTransaction));
    assert!(seen.contains(&WalRecordType::TransactionFinished));
    assert!(seen.contains(&WalRecordType::Head));
    assert_eq!(seen.last().copied(), Some(WalRecordType::Update));

    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened = storage
        .open_map::<u16, u16, 8>(
            CollectionId(46),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(region_index)
    );
    assert_eq!(reopened.get_frontier(&1).unwrap(), Some(40));
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-016A` If a single update cannot fit in an empty mutable
//# frontier buffer, the implementation MUST report explicit buffer
//# exhaustion instead of flushing an empty frontier.
#[test]
fn requirement_storage_map_frontier_single_oversized_update_returns_buffer_too_small() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 6;
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(146)).unwrap();

    let mut map_buffer = [0u8; core::mem::size_of::<u32>()];
    let mut map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(146),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();

    let error = map.set(&mut storage, 1, 10).unwrap_err();
    assert!(matches!(
        error,
        MapStorageError::Map(MapError::BufferTooSmall)
    ));
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );
    assert!(map.frontier_is_empty());
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-017` After such a frontier-capacity flush, later updates
//# for that collection MUST accumulate in a fresh in-memory frontier
//# layered over the newly committed collection head.
#[test]
fn requirement_storage_map_frontier_continues_accumulating_updates_after_an_overflow_flush() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 6;
    let capacity = smallest_map_capacity_for_repeated_updates(3);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(47)).unwrap();

    let mut map_buffer = vec![0u8; capacity];
    let mut map = MapFrontier::<u16, u16, 8>::new(
        CollectionId(47),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    let mut payload_buffer = [0u8; 64];

    for value in [10u16, 20, 30] {
        map.apply_update(&mut storage, &MapUpdate::Set { key: 1, value })
            .unwrap();
    }

    map.apply_update(&mut storage, &MapUpdate::Set { key: 1, value: 40 })
        .unwrap();

    let StartupCollectionBasis::Region(head_after_flush) = storage.collections()[0].basis() else {
        panic!("overflow flush should leave the collection on a committed region head");
    };

    map.apply_update(&mut storage, &MapUpdate::Set { key: 2, value: 50 })
        .unwrap();

    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(head_after_flush)
    );
    assert_eq!(map.get_frontier(&1).unwrap(), Some(40));
    assert_eq!(map.get_frontier(&2).unwrap(), Some(50));

    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened = storage
        .open_map::<u16, u16, 8>(
            CollectionId(47),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(reopened.get_frontier(&1).unwrap(), Some(40));
    assert_eq!(reopened.get_frontier(&2).unwrap(), Some(50));
}

//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-003` Collection implementations MUST define their opaque payload semantics above
//# the shared storage primitives rather than bypassing WAL and region-management invariants.
#[test]
fn requirement_storage_map_api_appends_typed_updates() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(17)).unwrap();

    let mut payload_buffer = [0u8; 128];
    storage
        .append_map_update::<i32, i32>(CollectionId(17), &MapUpdate::Set { key: 4, value: 40 })
        .unwrap();
    storage
        .append_map_update::<i32, i32>(CollectionId(17), &MapUpdate::Delete { key: 4 })
        .unwrap();

    let mut reopen_buffer = [0u8; 512];
    let reopened = storage
        .open_map::<i32, i32, 4>(
            CollectionId(17),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();

    assert_eq!(reopened.get_frontier(&4).unwrap(), None);
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-109` Storage map flush API MUST write a committed region basis, clear
//# ready_region, and preserve flushed key/value lookups.
#[test]
fn requirement_storage_map_api_flushes_committed_region_basis() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 4>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(12)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(12),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(5, 50).unwrap();
    map.set_in_memory(7, 70).unwrap();

    let region_index = storage.flush_map::<_, _, 4>(&mut map).unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(region_index)
    );
    assert_eq!(storage.ready_region(), None);

    let mut reopen_buffer = [0u8; 512];
    let reopened = storage
        .open_map::<i32, i32, 4>(
            CollectionId(12),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();

    assert_eq!(
        storage
            .with_io_workspace(|flash, workspace| reopened.get::<512, _>(flash, workspace, &5))
            .unwrap(),
        Some(50)
    );
    assert_eq!(
        storage
            .with_io_workspace(|flash, workspace| reopened.get::<512, _>(flash, workspace, &7))
            .unwrap(),
        Some(70)
    );
}

//= spec/map.md#map-compaction-requirements
//= type=test
//# `RING-IMPL-REGRESSION-110` Run-target then greedy map compaction MUST reduce selected runs while
//# preserving unselected runs and all visible key/value lookups.
#[test]
fn requirement_storage_compact_map_target_then_greedy_preserves_unselected_runs() {
    const REGION_SIZE: usize = 1024;
    const REGION_COUNT: usize = 18;
    const MAX_RUNS: usize = 8;

    assert_eq!(LsmMap::<i32, i32, 1>::default_compaction_run_target(), 1);
    assert_eq!(
        LsmMap::<i32, i32, MAX_RUNS>::default_compaction_run_target(),
        MAX_RUNS - 1
    );
    let mut target_memory = LsmMapMemory::<i32, i32, MAX_RUNS>::new();
    let target_map =
        LsmMap::<i32, i32, MAX_RUNS>::from_collection_id(CollectionId(170), 5, &mut target_memory);
    assert_eq!(target_map.compaction_run_target(), 5);
    let target_map = target_map.with_compaction_run_target(3).unwrap();
    assert_eq!(target_map.compaction_run_target(), 3);
    assert!(matches!(
        target_map.with_compaction_run_target(0),
        Err(MapStorageError::InvalidRunTarget)
    ));

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(70)).unwrap();

    let mut map_buffer = [0u8; REGION_SIZE];
    let mut map = MapFrontier::<i32, i32, MAX_RUNS>::new(
        CollectionId(70),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    storage.flush_map::<_, _, MAX_RUNS>(&mut map).unwrap();

    for key in 10..15 {
        map.set_in_memory(key, key * 10).unwrap();
    }
    storage.flush_map::<_, _, MAX_RUNS>(&mut map).unwrap();

    map.set_in_memory(100, 1000).unwrap();
    storage.flush_map::<_, _, MAX_RUNS>(&mut map).unwrap();

    map.set_in_memory(200, 2000).unwrap();
    storage.flush_map::<_, _, MAX_RUNS>(&mut map).unwrap();

    let mut before_buffer = [0u8; REGION_SIZE];
    let before = storage
        .open_map::<i32, i32, MAX_RUNS>(
            CollectionId(70),
            &mut before_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(before.run_count(), 4);

    let mut scratch_buffer = [0u8; REGION_SIZE];
    let compacted_manifest = storage
        .compact_map::<i32, i32, MAX_RUNS, 3>(CollectionId(70), crate::test_lsm_map_memory())
        .unwrap();
    assert!(compacted_manifest.is_some());

    let mut after_buffer = [0u8; REGION_SIZE];
    let after = storage
        .open_map::<i32, i32, MAX_RUNS>(
            CollectionId(70),
            &mut after_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(after.run_count(), 3);
    assert_eq!(
        storage
            .with_io_workspace(
                |flash, workspace| after.get::<REGION_SIZE, _>(flash, workspace, &200)
            )
            .unwrap(),
        Some(2000)
    );
    assert_eq!(
        storage
            .with_io_workspace(
                |flash, workspace| after.get::<REGION_SIZE, _>(flash, workspace, &100)
            )
            .unwrap(),
        Some(1000)
    );
    assert_eq!(
        storage
            .with_io_workspace(|flash, workspace| after.get::<REGION_SIZE, _>(flash, workspace, &10))
            .unwrap(),
        Some(100)
    );
    assert_eq!(
        storage
            .with_io_workspace(|flash, workspace| after.get::<REGION_SIZE, _>(flash, workspace, &1))
            .unwrap(),
        Some(10)
    );
}

//= spec/map.md#map-compaction-requirements
//= type=test
//# `RING-IMPL-REGRESSION-111` Map compaction MUST preserve tombstone masking so deleted keys remain
//# absent and later live keys remain visible.
#[test]
fn requirement_storage_compact_map_preserves_tombstone_masking() {
    const REGION_SIZE: usize = 1024;
    const REGION_COUNT: usize = 14;
    const MAX_RUNS: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(71)).unwrap();

    let mut map_buffer = [0u8; REGION_SIZE];
    let mut map = MapFrontier::<i32, i32, MAX_RUNS>::new(
        CollectionId(71),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    storage.flush_map::<_, _, MAX_RUNS>(&mut map).unwrap();

    map.delete_in_memory(1).unwrap();
    storage.flush_map::<_, _, MAX_RUNS>(&mut map).unwrap();

    map.set_in_memory(2, 20).unwrap();
    storage.flush_map::<_, _, MAX_RUNS>(&mut map).unwrap();

    let mut before_buffer = [0u8; REGION_SIZE];
    let before = storage
        .open_map::<i32, i32, MAX_RUNS>(
            CollectionId(71),
            &mut before_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(
        storage
            .with_io_workspace(|flash, workspace| before.get::<REGION_SIZE, _>(flash, workspace, &1))
            .unwrap(),
        None
    );

    let mut scratch_buffer = [0u8; REGION_SIZE];
    storage
        .compact_map::<i32, i32, MAX_RUNS, 1>(CollectionId(71), crate::test_lsm_map_memory())
        .unwrap();

    let mut after_buffer = [0u8; REGION_SIZE];
    let after = storage
        .open_map::<i32, i32, MAX_RUNS>(
            CollectionId(71),
            &mut after_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(
        storage
            .with_io_workspace(|flash, workspace| after.get::<REGION_SIZE, _>(flash, workspace, &1))
            .unwrap(),
        None
    );
    assert_eq!(
        storage
            .with_io_workspace(|flash, workspace| after.get::<REGION_SIZE, _>(flash, workspace, &2))
            .unwrap(),
        Some(20)
    );
}

//= spec/map.md#map-compaction-requirements
//= type=test
//# `RING-IMPL-REGRESSION-112` Map compaction MUST stream replacements larger than frontier capacity
//# into a single run while preserving all visible key/value lookups.
#[test]
fn requirement_storage_compact_map_streams_replacement_larger_than_frontier_capacity() {
    const REGION_SIZE: usize = 1024;
    const REGION_COUNT: usize = 40;
    const ENTRY_COUNT: usize = 4;
    const MAX_RUNS: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 16384>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, 8>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(72)).unwrap();

    let mut map_buffer = [0u8; REGION_SIZE];
    let mut map = MapFrontier::<i32, i32, MAX_RUNS>::new(
        CollectionId(72),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    for run in 0..3 {
        for offset in 0..ENTRY_COUNT {
            let key = run * 10 + offset as i32;
            map.set_in_memory(key, key * 10).unwrap();
        }
        storage.flush_map::<_, _, MAX_RUNS>(&mut map).unwrap();
    }

    let mut before_buffer = [0u8; REGION_SIZE];
    let before = storage
        .open_map::<i32, i32, MAX_RUNS>(
            CollectionId(72),
            &mut before_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(before.run_count(), 3);

    let mut scratch_buffer = [0u8; REGION_SIZE];
    let compacted_manifest = storage
        .compact_map::<i32, i32, MAX_RUNS, 1>(CollectionId(72), crate::test_lsm_map_memory())
        .unwrap();
    assert!(compacted_manifest.is_some());

    let mut after_buffer = [0u8; REGION_SIZE];
    let after = storage
        .open_map::<i32, i32, MAX_RUNS>(
            CollectionId(72),
            &mut after_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(after.run_count(), 1);
    for run in 0..3 {
        for offset in 0..ENTRY_COUNT {
            let key = run * 10 + offset as i32;
            assert_eq!(
                storage
                    .with_io_workspace(|flash, workspace| {
                        after.get::<REGION_SIZE, _>(flash, workspace, &key)
                    })
                    .unwrap(),
                Some(key * 10)
            );
        }
    }

    let second_manifest = storage
        .compact_map::<i32, i32, MAX_RUNS, 1>(CollectionId(72), crate::test_lsm_map_memory())
        .unwrap();
    assert_eq!(second_manifest, None);

    let mut second_buffer = [0u8; REGION_SIZE];
    let second = storage
        .open_map::<i32, i32, MAX_RUNS>(
            CollectionId(72),
            &mut second_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(second.run_count(), 1);
    for run in 0..3 {
        for offset in 0..ENTRY_COUNT {
            let key = run * 10 + offset as i32;
            assert_eq!(
                storage
                    .with_io_workspace(|flash, workspace| {
                        second.get::<REGION_SIZE, _>(flash, workspace, &key)
                    })
                    .unwrap(),
                Some(key * 10)
            );
        }
    }
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-PRE-001` Transaction cleanup MUST make the
//# transaction begin marker durable before durable collection metadata
//# stops referencing regions that cleanup may free.
#[test]
fn requirement_storage_map_replacement_flush_records_reclaim_after_new_head() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(18)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(18),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    let first_region = storage.flush_map::<_, _, 4>(&mut map).unwrap();

    map.set_in_memory(2, 20).unwrap();
    let second_region = storage.flush_map::<_, _, 4>(&mut map).unwrap();

    let mut saw_free_region = false;
    let mut saw_alloc_begin = false;
    let mut saw_replacement_head = false;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, record| {
                match record {
                    crate::WalRecord::AllocBegin { .. } => {
                        saw_alloc_begin = true;
                    }
                    crate::WalRecord::FreeRegion { region_index, .. }
                        if region_index == first_region =>
                    {
                        assert!(saw_replacement_head);
                        saw_free_region = true;
                    }
                    crate::WalRecord::Head {
                        collection_id,
                        region_index,
                        ..
                    } if collection_id == CollectionId(18) && region_index == second_region => {
                        assert!(!saw_free_region);
                        saw_replacement_head = true;
                    }
                    _ => {}
                }
                Ok(())
            })
        })
        .unwrap();

    assert!(saw_alloc_begin);
    assert!(saw_free_region);
    assert!(saw_replacement_head);
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-113` Reopening after a map replacement flush MUST complete transaction
//# cleanup of the replaced region and preserve the replacement map value.
#[test]
fn requirement_storage_map_replacement_flush_is_completed_during_reopen() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(13)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(13),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    let first_region = storage.flush_map::<_, _, 4>(&mut map).unwrap();

    map.set_in_memory(1, 20).unwrap();
    let second_region = storage.flush_map::<_, _, 4>(&mut map).unwrap();

    assert_ne!(first_region, second_region);
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(storage.free_list_tail(), Some(first_region));

    let mut reopened =
        Storage::<_, 512, 7>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(reopened.free_list_tail(), Some(first_region));

    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<i32, i32, 4>(
            CollectionId(13),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(
        reopened_map
            .get::<512, _>(&mut flash, &mut workspace, &1)
            .unwrap(),
        Some(20)
    );
}

fn replace_map_and_free_old_manifest<'db>(
    flash: &'db mut MockFlash<512, 6, 2048>,
) -> (
    Storage<'db, 'db, MockFlash<512, 6, 2048>, 512, 6, 8>,
    u32,
    u32,
) {
    let mut storage = Storage::<_, 512, 6>::format(
        flash,
        StorageFormatConfig::new(0, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(26)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(26),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    let first_region = storage.flush_map::<_, _, 4>(&mut map).unwrap();

    map.set_in_memory(2, 20).unwrap();
    let second_region = storage.flush_map::<_, _, 4>(&mut map).unwrap();

    assert_ne!(first_region, second_region);
    assert_eq!(storage.free_list_tail(), Some(first_region));

    (storage, first_region, second_region)
}

#[derive(Debug)]
struct ReplacementSnapshot {
    last_free_list_head: Option<u32>,
    free_list_tail: Option<u32>,
}

fn replace_map_and_reopen_after_cleanup<'db>(
    flash: &'db mut MockFlash<512, 6, 2048>,
) -> (
    ReplacementSnapshot,
    Storage<'db, 'db, MockFlash<512, 6, 2048>, 512, 6, 8>,
    u32,
    u32,
) {
    let (storage, first_region, second_region) = replace_map_and_free_old_manifest(flash);
    let snapshot = ReplacementSnapshot {
        last_free_list_head: storage.last_free_list_head(),
        free_list_tail: storage.free_list_tail(),
    };
    drop(storage);
    let mut reopened = Storage::<_, 512, 6>::open(flash, crate::test_storage_memory()).unwrap();

    (snapshot, reopened, first_region, second_region)
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-114` Reopening after replacement with an empty free list MUST initialize
//# free-list head from the recovered reclaimed region.
#[test]
fn requirement_storage_reopen_after_replacement_initializes_allocator_from_recovered_free_list_head(
) {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (_, reopened, first_region, _) = replace_map_and_reopen_after_cleanup(&mut flash);

    assert_eq!(reopened.free_list_tail(), Some(first_region));
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-115` Reopening after replacement with an empty free list MUST reconstruct
//# free-list tail from the recovered reclaimed region.
#[test]
fn requirement_storage_reopen_after_replacement_reconstructs_free_list_tail() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (_, reopened, first_region, _) = replace_map_and_reopen_after_cleanup(&mut flash);

    assert_eq!(reopened.free_list_tail(), Some(first_region));
}

//= spec/ring/06-startup-replay.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-007 Maintain replay state:
//# per collection optional live `collection_type`, explicit collection
//# state, `basis_pos`, and `pending_updates`, plus global
//# `last_free_list_head`, optional reserved WAL-rotation `ready_region`, transaction
//# scan state, and the replay-local `pending_wal_recovery_boundary`.
#[test]
fn requirement_storage_reopen_after_replacement_recovers_collection_and_reclaim_state() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (_, mut reopened, _, second_region) = replace_map_and_reopen_after_cleanup(&mut flash);

    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(reopened.ready_region(), None);

    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<i32, i32, 4>(
            CollectionId(26),
            &mut reopen_buffer,
            crate::test_map_frontier_memory(),
        )
        .unwrap();
    assert_eq!(
        reopened
            .with_io_workspace(|flash, workspace| reopened_map.get::<512, _>(flash, workspace, &1))
            .unwrap(),
        Some(10)
    );
    assert_eq!(
        reopened
            .with_io_workspace(|flash, workspace| reopened_map.get::<512, _>(flash, workspace, &2))
            .unwrap(),
        Some(20)
    );
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-PRE-002` After the committed collection-state
//# update, a region selected for cleanup MUST no longer be reachable from
//# any live collection head, WAL chain, or ready allocation state.
#[test]
fn requirement_storage_replacement_flush_detaches_reclaimed_region_from_live_state() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (storage, first_region, second_region) = replace_map_and_free_old_manifest(&mut flash);

    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(storage.free_list_tail(), Some(first_region));
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-PRE-003` A cleanup target MUST NOT already be
//# reachable from the free-list chain unless startup is re-entering
//# idempotent recovery.
#[test]
fn requirement_storage_replacement_flush_keeps_detached_region_out_of_free_list_chain() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (storage, first_region, _) = replace_map_and_free_old_manifest(&mut flash);
    let last_free_list_head = storage.last_free_list_head();
    drop(storage);

    let chain = free_list_chain::<512, 6, 2048, 8>(&flash, 0xff, last_free_list_head);
    assert!(chain.contains(&first_region));
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-SEM-003` If no free-list tail exists, cleanup
//# MUST make the freed region the durable free-list head through the
//# `free_region(collection_id, region_index)` record.
#[test]
fn requirement_storage_reopen_after_replacement_recovers_singleton_free_list_for_reclaimed_region()
{
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (snapshot, reopened, first_region, _) = replace_map_and_reopen_after_cleanup(&mut flash);

    assert!(snapshot.last_free_list_head.is_some());
    assert_eq!(snapshot.free_list_tail, Some(first_region));
    assert_eq!(reopened.free_list_tail(), Some(first_region));
    drop(reopened);

    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let footer = FreePointerFooter::decode(
        &flash.region_bytes(first_region).unwrap()[footer_offset..],
        0xff,
    )
    .unwrap();
    assert_eq!(footer.next_tail, None);
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-004` Cleanup MUST leave the newly freed region's
//# free-pointer successor uninitialized so it is recognizable as the
//# free-list tail.
#[test]
fn requirement_storage_reopen_after_replacement_leaves_new_free_list_tail_uninitialized() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (_, reopened, first_region, _) = replace_map_and_reopen_after_cleanup(&mut flash);
    drop(reopened);

    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let footer = FreePointerFooter::decode(
        &flash.region_bytes(first_region).unwrap()[footer_offset..],
        0xff,
    )
    .unwrap();
    assert_eq!(footer.next_tail, None);
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-005` Transaction cleanup MUST be
//# idempotent across crashes between any two cleanup steps.
#[test]
fn requirement_storage_reopen_after_replacement_recovers_reclaim_idempotently() {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (_, reopened_once, _, _) = replace_map_and_reopen_after_cleanup(&mut flash);
    drop(reopened_once);

    let reopened_once =
        Storage::<_, 512, 6>::open(&mut flash, crate::test_storage_memory()).unwrap();
    let snapshot_collections: heapless::Vec<StartupCollection, 8> =
        heapless::Vec::from_slice(reopened_once.collections()).unwrap();
    let snapshot_last_free_list_head = reopened_once.last_free_list_head();
    let snapshot_free_list_tail = reopened_once.free_list_tail();
    let snapshot_ready_region = reopened_once.ready_region();
    drop(reopened_once);
    let reopened_twice =
        Storage::<_, 512, 6>::open(&mut flash, crate::test_storage_memory()).unwrap();

    assert_eq!(
        reopened_twice.collections(),
        snapshot_collections.as_slice()
    );
    assert_eq!(
        reopened_twice.last_free_list_head(),
        snapshot_last_free_list_head
    );
    assert_eq!(reopened_twice.free_list_tail(), snapshot_free_list_tail);
    assert_eq!(reopened_twice.ready_region(), snapshot_ready_region);
}

//= spec/ring/01-theory.md#core-requirements
//= type=test
//# `RING-CORE-014` If space-recovery operations cannot restore more
//# than `min_free_regions` free regions, the database MUST treat
//# ordinary writes as out of space until space is freed or the store is
//# migrated.
#[test]
fn requirement_storage_map_flush_rejects_consuming_min_free_region_reserve() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(4, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(23)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(23),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    storage.flush_map::<_, _, 4>(&mut map).unwrap();

    map.set_in_memory(2, 20).unwrap();
    let error = storage.flush_map::<_, _, 4>(&mut map).unwrap_err();

    assert!(matches!(
        error,
        MapStorageError::Storage(StorageRuntimeError::InsufficientFreeRegions {
            free_regions: 4,
            min_free_regions: 4,
        })
    ));
}

//= spec/ring/07-reclaim.md#free-region
//= type=test
//# `RING-FREE-REGION-001` Establish `region_index` as a free-tail
//# candidate without erasing it. Its free-pointer footer MUST be
//# unwritten: all footer bytes equal `erased_byte`.
#[test]
fn requirement_storage_transaction_cleanup_does_not_erase_freed_region() {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();

    assert!(result.reclaimed_footer_unwritten);
    assert!(!result.reclaim_operations.iter().any(|operation| {
        matches!(
            operation,
            crate::MockOperation::EraseRegion { region_index }
                if *region_index == result.reclaimed_region
        )
    }));
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-SEM-002` If a prior free-list tail exists,
//# cleanup MUST durably write that tail's `next_tail` pointer to the freed
//# region.
#[test]
fn requirement_storage_transaction_cleanup_writes_the_previous_tail_next_pointer() {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();
    assert_eq!(result.previous_tail_next, Some(result.reclaimed_region));
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-001` The free-list chain MUST remain acyclic and FIFO-ordered.
#[test]
fn requirement_storage_transaction_cleanup_preserves_fifo_free_list_order() {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();
    assert_eq!(
        result.previous_chain.first().copied(),
        Some(result.allocated_region)
    );
    assert!(result.new_chain.len() <= result.previous_chain.len());
    assert!(!result.new_chain.is_empty());
    let new_prefix = &result.new_chain[..result.new_chain.len() - 1];
    let previous_suffix =
        &result.previous_chain.as_slice()[result.previous_chain.len() - new_prefix.len()..];
    assert_eq!(new_prefix, previous_suffix);
    assert_eq!(
        result.new_chain.last().copied(),
        Some(result.reclaimed_region)
    );
    assert_eq!(
        result.reopened_chain.as_slice(),
        result.new_chain.as_slice()
    );
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-002` Cleanup MUST append exactly one
//# newly freed region for each `free_region(collection_id, region_index)`
//# record.
#[test]
fn requirement_storage_transaction_cleanup_appends_exactly_one_region_to_the_tail() {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();

    assert!(!result.new_chain.contains(&result.allocated_region));
    assert_eq!(
        result
            .new_chain
            .iter()
            .filter(|region_index| **region_index == result.reclaimed_region)
            .count(),
        1
    );
    assert_eq!(
        result.new_chain.last().copied(),
        Some(result.reclaimed_region)
    );
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-003` If a prior free-list tail existed,
//# its
//# `next_tail` pointer MUST reference the newly freed region after
//# cleanup.
#[test]
fn requirement_storage_transaction_cleanup_points_the_previous_tail_at_the_reclaimed_region() {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();
    assert_eq!(result.previous_tail_next, Some(result.reclaimed_region));
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-004` The newly freed region's
//# free-pointer successor MUST remain uninitialized after cleanup.
#[test]
fn requirement_storage_transaction_cleanup_keeps_the_reclaimed_tail_footer_uninitialized() {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();
    assert_eq!(result.reclaimed_tail_next, None);
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-005` Replay of free pointers MUST follow
//# the previous tail to the newly freed region when a prior free-list tail
//# existed.
#[test]
fn requirement_storage_transaction_cleanup_links_the_previous_tail_to_the_reclaimed_region() {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();
    assert_eq!(result.previous_tail_next, Some(result.reclaimed_region));
    assert_eq!(result.reclaimed_tail_next, None);
    assert_eq!(
        result.reopened_free_list_tail,
        Some(result.reclaimed_region)
    );
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-002` Before any durable write links a
//# freed region from the previous free-list tail, the freed region MUST
//# already have the correct uninitialized free-list-tail footer state.
#[test]
fn requirement_storage_transaction_cleanup_prepares_the_reclaimed_footer_before_syncing_the_tail_link(
) {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();
    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let reclaimed_footer_read = result
        .reclaim_operations
        .iter()
        .position(|operation| {
            matches!(
                operation,
                crate::MockOperation::ReadRegion {
                    region_index,
                    offset,
                    len,
                } if *region_index == result.reclaimed_region
                    && *offset == footer_offset
                    && *len == FreePointerFooter::ENCODED_LEN
            )
        })
        .unwrap();
    let reclaimed_footer_write = result.reclaim_operations.iter().position(|operation| {
        matches!(
            operation,
            crate::MockOperation::WriteRegion {
                region_index,
                offset,
                len,
            } if *region_index == result.reclaimed_region
                && *offset == footer_offset
                && *len == FreePointerFooter::ENCODED_LEN
        )
    });
    let previous_tail_link_write = result
        .reclaim_operations
        .iter()
        .position(|operation| {
            matches!(
                operation,
                crate::MockOperation::WriteRegion {
                    region_index,
                    offset,
                    len,
                } if *region_index == result.previous_tail
                    && *offset == footer_offset
                    && *len == FreePointerFooter::ENCODED_LEN
            )
        })
        .unwrap();

    assert_eq!(reclaimed_footer_write, None);
    assert!(reclaimed_footer_read < previous_tail_link_write);
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-003` If the free list was empty, the
//# `free_region(collection_id, region_index)` record MUST be durable before
//# cleanup can be considered finished.
#[test]
fn requirement_storage_transaction_cleanup_records_free_list_head_before_transaction_finish_when_the_list_was_empty(
) {
    let mut flash = MockFlash::<512, 6, 2048>::new(0xff);
    let (mut storage, first_region, _) = replace_map_and_free_old_manifest(&mut flash);

    let mut saw_free_region = false;
    let mut saw_transaction_finish_after_free = false;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, record| {
                match record {
                    crate::WalRecord::FreeRegion { region_index, .. }
                        if region_index == first_region =>
                    {
                        saw_free_region = true;
                    }
                    crate::WalRecord::TransactionFinished { .. } if saw_free_region => {
                        saw_transaction_finish_after_free = true;
                    }
                    _ => {}
                }
                Ok(())
            })
        })
        .unwrap();

    assert!(saw_free_region);
    assert!(saw_transaction_finish_after_free);
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-004` If a prior free-list tail exists,
//# the
//# tail-link footer write MUST be synced before cleanup can be
//# considered finished.
#[test]
fn requirement_storage_transaction_cleanup_syncs_the_tail_link_before_transaction_finish() {
    let result = transaction_cleanup_returns_old_map_region_to_free_list_result();
    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let previous_tail_link_write = result
        .reclaim_operations
        .iter()
        .position(|operation| {
            matches!(
                operation,
                crate::MockOperation::WriteRegion {
                    region_index,
                    offset,
                    len,
                } if *region_index == result.previous_tail
                    && *offset == footer_offset
                    && *len == FreePointerFooter::ENCODED_LEN
            )
        })
        .unwrap();
    let first_sync_after_tail_link = result
        .reclaim_operations
        .iter()
        .enumerate()
        .find_map(|(index, operation)| {
            (index > previous_tail_link_write && *operation == crate::MockOperation::Sync)
                .then_some(index)
        })
        .unwrap();
    let last_write_after_tail_link_sync = result
        .reclaim_operations
        .iter()
        .enumerate()
        .rfind(|(index, operation)| {
            *index > first_sync_after_tail_link
                && matches!(operation, crate::MockOperation::WriteRegion { .. })
        })
        .map(|(index, _)| index)
        .unwrap();

    assert!(previous_tail_link_write < first_sync_after_tail_link);
    assert!(first_sync_after_tail_link < last_write_after_tail_link_sync);
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-118` Dropping a map with committed-region basis MUST free that region
//# through transaction cleanup, tombstone the collection, complete cleanup on reopen, and reject
//# reopening the dropped map.
#[test]
fn requirement_storage_drop_map_starts_reclaim_for_committed_region_basis() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(15)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(15),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(8, 80).unwrap();
    let region_index = storage.flush_map::<_, _, 4>(&mut map).unwrap();

    let reclaim_region = storage.drop_map(CollectionId(15)).unwrap();

    assert_eq!(reclaim_region, Some(region_index));
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
    assert_eq!(storage.free_list_tail(), Some(region_index));
    assert_eq!(storage.tracked_user_collection_count(), 0);

    let mut reopened =
        Storage::<_, 512, 7>::open(&mut flash, crate::test_storage_memory()).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
    assert_eq!(reopened.free_list_tail(), Some(region_index));

    let mut reopen_buffer = [0u8; 512];
    let result = reopened.open_map::<i32, i32, 4>(
        CollectionId(15),
        &mut reopen_buffer,
        crate::test_map_frontier_memory(),
    );
    assert!(matches!(
        result,
        Err(MapStorageError::DroppedCollection(CollectionId(15)))
    ));
}

//= spec/ring/07-reclaim.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-001` Transaction cleanup MUST not make a
//# region free until the committed collection state no longer references
//# that region.
#[test]
fn requirement_storage_drop_map_records_free_region_after_drop_commit() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(19)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(19),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(8, 80).unwrap();
    storage.flush_map::<_, _, 4>(&mut map).unwrap();

    storage.drop_map(CollectionId(19)).unwrap();

    let mut drop_seen_at = None;
    let mut commit_seen_at = None;
    let mut free_seen_at = None;
    let mut record_index = 0usize;
    storage
        .with_runtime_io_workspace(|runtime, flash, workspace| {
            runtime.visit_wal_records::<512, _, (), _>(flash, workspace, |_flash, record| {
                match record {
                    crate::WalRecord::DropCollection {
                        collection_id: CollectionId(19),
                    } => {
                        drop_seen_at = Some(record_index);
                    }
                    crate::WalRecord::CommitTransaction {
                        collection_id: CollectionId(19),
                    } => {
                        commit_seen_at = Some(record_index);
                    }
                    crate::WalRecord::FreeRegion {
                        collection_id: CollectionId(19),
                        region_index: _,
                    } => {
                        free_seen_at = Some(record_index);
                    }
                    _ => {}
                }
                record_index += 1;
                Ok(())
            })
        })
        .unwrap();

    let drop_seen_at = drop_seen_at.unwrap();
    let commit_seen_at = commit_seen_at.unwrap();
    let free_seen_at = free_seen_at.unwrap();
    assert!(drop_seen_at < commit_seen_at);
    assert!(commit_seen_at < free_seen_at);
}

//= spec/map.md#map-storage-integration-requirements
//= type=test
//# `RING-IMPL-REGRESSION-120` Dropping a map whose basis is a WAL snapshot MUST tombstone the
//# collection without starting committed-region cleanup.
#[test]
fn requirement_storage_drop_map_from_snapshot_basis_has_no_region_reclaim() {
    let mut flash = MockFlash::<512, 7, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage = Storage::<_, 512, 7>::format(
        &mut flash,
        StorageFormatConfig::new(1, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();

    storage.create_map(CollectionId(16)).unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = MapFrontier::<i32, i32, 4>::new(
        CollectionId(16),
        &mut map_buffer,
        crate::test_map_frontier_memory(),
    )
    .unwrap();
    map.set_in_memory(1, 10).unwrap();
    storage.snapshot_map(&map).unwrap();

    let reclaim_region = storage.drop_map(CollectionId(16)).unwrap();

    assert_eq!(reclaim_region, None);
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
}
