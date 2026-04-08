use super::*;
use core::future::Future;
use core::pin::{pin, Pin};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
extern crate std;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::thread_local;
use std::vec;

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

fn smallest_map_capacity_for_repeated_updates(update_count: usize) -> usize {
    (4..256)
        .find(|capacity| {
            let mut buffer = vec![0u8; *capacity];
            let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(200), &mut buffer).unwrap();
            (0..update_count).all(|index| map.set(1, u16::try_from(index).unwrap()).is_ok())
        })
        .expect("expected a bounded map capacity within the search range")
}

struct CompletedPendingReclaimResult {
    reclaimed_region: u32,
    previous_tail: u32,
    wal_tail_before_reclaim: u32,
    previous_chain: heapless::Vec<u32, 8>,
    new_chain: heapless::Vec<u32, 8>,
    previous_tail_next: Option<u32>,
    reclaimed_tail_next: Option<u32>,
    reopened_chain: heapless::Vec<u32, 8>,
    reopened_free_list_tail: Option<u32>,
    reclaim_operations: heapless::Vec<crate::MockOperation, 64>,
}

fn complete_pending_reclaim_returns_old_map_region_to_free_list_result(
) -> CompletedPendingReclaimResult {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(14))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(14), &mut map_buffer).unwrap();
    map.set(3, 30).unwrap();
    let first_region = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    map.set(4, 40).unwrap();
    storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();
    assert_eq!(storage.pending_reclaims(), &[first_region]);
    let previous_chain =
        free_list_chain::<512, 5, 2048, 8>(&flash, 0xff, storage.last_free_list_head());
    let previous_tail = storage.free_list_tail().unwrap();
    let wal_tail_before_reclaim = storage.runtime().wal_tail();

    flash.clear_operations();
    storage
        .complete_pending_reclaim::<512, 5, _>(&mut flash, &mut workspace, first_region)
        .unwrap();

    let mut reclaim_operations = heapless::Vec::<crate::MockOperation, 64>::new();
    for operation in flash.operations() {
        reclaim_operations.push(*operation).unwrap();
    }

    assert!(storage.pending_reclaims().is_empty());
    let new_chain = free_list_chain::<512, 5, 2048, 8>(&flash, 0xff, storage.last_free_list_head());
    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let previous_tail_footer = FreePointerFooter::decode(
        &flash.region_bytes(previous_tail).unwrap()[footer_offset..],
        0xff,
    )
    .unwrap();
    let reclaimed_footer = FreePointerFooter::decode(
        &flash.region_bytes(first_region).unwrap()[footer_offset..],
        0xff,
    )
    .unwrap();

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert!(reopened.pending_reclaims().is_empty());
    let reopened_chain =
        free_list_chain::<512, 5, 2048, 8>(&flash, 0xff, reopened.last_free_list_head());

    CompletedPendingReclaimResult {
        reclaimed_region: first_region,
        previous_tail,
        wal_tail_before_reclaim,
        previous_chain,
        new_chain,
        previous_tail_next: previous_tail_footer.next_tail,
        reclaimed_tail_next: reclaimed_footer.next_tail,
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
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, MockError> {
        self.inner.read_metadata()
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), MockError> {
        self.inner.write_metadata(metadata)
    }

    fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), MockError> {
        self.inner.read_region(region_index, offset, buffer)
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), MockError> {
        self.inner.write_region(region_index, offset, data)
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), MockError> {
        self.inner.erase_region(region_index)
    }

    fn sync(&mut self) -> Result<(), MockError> {
        self.inner.sync()
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, MockFormatError> {
        self.inner
            .format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)
    }
}

fn rotate_wal_tail_for_collection(
    storage: &mut Storage<8, 4>,
    flash: &mut MockFlash<512, 6, 4096>,
    workspace: &mut StorageWorkspace<512>,
    collection_id: CollectionId,
) -> u32 {
    loop {
        match storage.append_wal_rotation_start::<512, 6, _>(flash, workspace) {
            Ok(region_index) => {
                storage
                    .append_wal_rotation_finish::<512, 6, _>(flash, workspace, region_index)
                    .unwrap();
                return region_index;
            }
            Err(StorageRuntimeError::InvalidRotationWindow { .. }) => storage
                .append_update::<512, 6, _>(flash, workspace, collection_id, &[0])
                .unwrap(),
            Err(other) => panic!("unexpected rotation-start error: {other:?}"),
        }
    }
}

fn wal_and_map_region_formats() -> (Header, Header) {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    let wal_header =
        Header::decode(&flash.region_bytes(0).unwrap()[..Header::ENCODED_LEN]).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(43))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(43), &mut map_buffer).unwrap();
    map.set(3, 30).unwrap();

    let region_index = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();
    let map_header =
        Header::decode(&flash.region_bytes(region_index).unwrap()[..Header::ENCODED_LEN]).unwrap();

    (wal_header, map_header)
}

//= spec/ring.md#canonical-on-disk-encoding
//= type=test
//# `RING-DISK-004` `collection_format` is a stable per-region `u16`
//# namespace recorded durably in region headers. The pair
//# `(collection_type, collection_format)` identifies a concrete
//# committed region payload encoding. Borromean core reserves
//# `collection_format = 0x0000` globally for `wal_v1`; every non-WAL
//# collection format MUST be nonzero.
#[test]
fn wal_and_map_regions_use_distinct_collection_format_namespace_values() {
    let (wal_header, map_header) = wal_and_map_region_formats();

    assert_eq!(WAL_V1_FORMAT, 0);
    assert_eq!(wal_header.collection_id, CollectionId(0));
    assert_eq!(wal_header.collection_format, WAL_V1_FORMAT);
    assert_eq!(map_header.collection_id, CollectionId(43));
    assert_eq!(map_header.collection_format, MAP_REGION_V1_FORMAT);
    assert_ne!(MAP_REGION_V1_FORMAT, WAL_V1_FORMAT);
    assert!(map_header.collection_format > 0);
}

//= spec/ring.md#storage-requirements
//= type=test
//# `RING-STORAGE-005` Borromean core MUST reserve the canonical
//# `collection_format` value `wal_v1` for WAL regions, and user
//# collections MUST NOT use that identifier.
#[test]
fn wal_v1_collection_format_is_reserved_to_wal_regions() {
    let (wal_header, map_header) = wal_and_map_region_formats();

    assert_eq!(wal_header.collection_format, WAL_V1_FORMAT);
    assert_eq!(wal_header.collection_id, CollectionId(0));
    assert_ne!(map_header.collection_format, WAL_V1_FORMAT);
}

fn setup_storage_with_stale_wal_head() -> (
    MockFlash<512, 6, 4096>,
    StorageWorkspace<512>,
    Storage<8, 4>,
    u32,
) {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 6, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .append_new_collection::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(31),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    storage
        .append_update::<512, 6, _>(&mut flash, &mut workspace, CollectionId(31), &[1, 2, 3])
        .unwrap();

    let next_region =
        rotate_wal_tail_for_collection(&mut storage, &mut flash, &mut workspace, CollectionId(31));
    storage
        .append_snapshot::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(31),
            CollectionType::MAP_CODE,
            &[9, 8, 7],
        )
        .unwrap();

    (flash, workspace, storage, next_region)
}

fn setup_storage_with_live_snapshot_in_wal_head() -> (
    MockFlash<512, 6, 4096>,
    StorageWorkspace<512>,
    Storage<8, 4>,
    u32,
) {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 6, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .append_new_collection::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(32),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    storage
        .append_snapshot::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(32),
            CollectionType::MAP_CODE,
            &[4, 5, 6],
        )
        .unwrap();
    storage
        .append_new_collection::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(132),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    storage
        .append_snapshot::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(132),
            CollectionType::MAP_CODE,
            &[0],
        )
        .unwrap();

    let next_region =
        rotate_wal_tail_for_collection(&mut storage, &mut flash, &mut workspace, CollectionId(132));

    (flash, workspace, storage, next_region)
}

fn setup_storage_with_live_snapshot_and_update_in_wal_head() -> (
    MockFlash<512, 6, 4096>,
    StorageWorkspace<512>,
    Storage<8, 4>,
    u32,
) {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 6, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .append_new_collection::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(33),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    storage
        .append_snapshot::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(33),
            CollectionType::MAP_CODE,
            &[7, 8, 9],
        )
        .unwrap();
    storage
        .append_update::<512, 6, _>(&mut flash, &mut workspace, CollectionId(33), &[1, 3, 5])
        .unwrap();
    storage
        .append_new_collection::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(133),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    storage
        .append_snapshot::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(133),
            CollectionType::MAP_CODE,
            &[0],
        )
        .unwrap();

    let next_region =
        rotate_wal_tail_for_collection(&mut storage, &mut flash, &mut workspace, CollectionId(133));

    (flash, workspace, storage, next_region)
}

fn setup_storage_with_live_empty_head_map_in_wal_head() -> (
    MockFlash<512, 6, 4096>,
    StorageWorkspace<512>,
    Storage<8, 4>,
    u32,
) {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 6, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 6, _>(&mut flash, &mut workspace, CollectionId(36))
        .unwrap();
    let mut target_payload = [0u8; 64];
    storage
        .append_map_update::<512, 6, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(36),
            &MapUpdate::Set { key: 1, value: 10 },
            &mut target_payload,
        )
        .unwrap();

    storage
        .create_map::<512, 6, _>(&mut flash, &mut workspace, CollectionId(136))
        .unwrap();

    let next_region =
        rotate_wal_tail_for_collection(&mut storage, &mut flash, &mut workspace, CollectionId(136));
    storage
        .append_snapshot::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(136),
            CollectionType::MAP_CODE,
            &crate::EMPTY_MAP_SNAPSHOT,
        )
        .unwrap();

    (flash, workspace, storage, next_region)
}

#[test]
fn storage_format_future_polls_to_completion() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    let storage = poll_ready(Storage::<8, 4>::format_future::<256, 4, _>(
        &mut flash,
        &mut workspace,
        1,
        8,
        0xa5,
    ))
    .unwrap();

    assert_eq!(storage.metadata().region_size, 256);
    assert_eq!(storage.metadata().region_count, 4);
    assert_eq!(storage.wal_head(), 0);
    assert_eq!(storage.last_free_list_head(), Some(1));
    assert_eq!(storage.free_list_tail(), Some(3));
}

#[test]
fn storage_open_future_polls_to_completion() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    {
        let mut storage =
            Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
        storage
            .append_new_collection::<256, 4, _>(
                &mut flash,
                &mut workspace,
                CollectionId(7),
                CollectionType::MAP_CODE,
            )
            .unwrap();
        storage
            .append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[1, 2, 3])
            .unwrap();
    }

    let reopened = poll_until_ready(
        Storage::<8, 4>::open_future::<256, 4, _>(&mut flash, &mut workspace),
        7,
    )
    .unwrap();

    assert_eq!(reopened.collections().len(), 1);
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(7));
    assert_eq!(reopened.collections()[0].pending_update_count(), 1);
}

#[test]
fn storage_open_future_yields_between_startup_phases() {
    let mut flash = MockFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    let future = Storage::<8, 4>::open_future::<256, 4, _>(&mut flash, &mut workspace);
    let mut future = pin!(future);

    assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    assert!(matches!(poll_once(future.as_mut()), Poll::Pending));

    let reopened = match poll_until_ready(future, 4) {
        Ok(reopened) => reopened,
        Err(error) => panic!("unexpected open error: {error:?}"),
    };
    assert_eq!(reopened.wal_head(), 0);
    assert_eq!(reopened.wal_tail(), 0);
}

#[test]
fn storage_open_future_drop_before_completion_leaves_store_openable() {
    let mut flash = MockFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    {
        let future = Storage::<8, 4>::open_future::<256, 4, _>(&mut flash, &mut workspace);
        let mut future = pin!(future);
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    }

    let reopened = Storage::<8, 4>::open::<256, 4, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(reopened.wal_head(), 0);
    assert_eq!(reopened.wal_tail(), 0);
    assert!(reopened.collections().is_empty());
}

#[test]
fn storage_reclaim_wal_head_future_polls_to_completion() {
    let (mut flash, mut workspace, mut storage, next_region) = setup_storage_with_stale_wal_head();

    let reclaimed_head = poll_until_ready(
        storage.reclaim_wal_head_future::<512, 6, _>(&mut flash, &mut workspace),
        6,
    )
    .unwrap();

    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), next_region);
    assert_eq!(storage.free_list_tail(), Some(0));
}

#[test]
fn storage_reclaim_wal_head_future_yields_between_reclaim_phases() {
    let (mut flash, mut workspace, mut storage, next_region) = setup_storage_with_stale_wal_head();

    let (first, second, third, fourth, fifth, reclaimed_head) = {
        let future = storage.reclaim_wal_head_future::<512, 6, _>(&mut flash, &mut workspace);
        let mut future = pin!(future);

        let first = matches!(poll_once(future.as_mut()), Poll::Pending);
        let second = matches!(poll_once(future.as_mut()), Poll::Pending);
        let third = matches!(poll_once(future.as_mut()), Poll::Pending);
        let fourth = matches!(poll_once(future.as_mut()), Poll::Pending);
        let fifth = matches!(poll_once(future.as_mut()), Poll::Pending);
        let reclaimed_head = match poll_once(future.as_mut()) {
            Poll::Ready(Ok(reclaimed_head)) => reclaimed_head,
            other => panic!("unexpected sixth poll result: {other:?}"),
        };

        (first, second, third, fourth, fifth, reclaimed_head)
    };

    assert!(first);
    assert!(second);
    assert!(third);
    assert!(fourth);
    assert!(fifth);
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), next_region);
}

#[test]
fn storage_reclaim_wal_head_future_drop_after_reclaim_begin_remains_recoverable() {
    let (mut flash, mut workspace, mut storage, _next_region) = setup_storage_with_stale_wal_head();
    let original_head = storage.wal_head();

    {
        let future = storage.reclaim_wal_head_future::<512, 6, _>(&mut flash, &mut workspace);
        let mut future = pin!(future);

        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    }

    let reopened = Storage::<8, 4>::open::<512, 6, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(reopened.wal_head(), original_head);
    assert!(reopened.pending_reclaims().is_empty());
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::WalSnapshot
    );
}

//= spec/implementation.md#operation-requirements
//= type=test
//# `RING-IMPL-OP-002` A borromean future MUST either complete with a terminal result or remain safely resumable by further polling after any `Poll::Pending`.
#[test]
fn storage_map_operation_futures_poll_to_completion() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    poll_ready(storage.create_map_future::<512, 5, _>(
        &mut flash,
        &mut workspace,
        CollectionId(41),
    ))
    .unwrap();

    let mut source_buffer = [0u8; 512];
    let mut source = LsmMap::<i32, i32, 4>::new(CollectionId(41), &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    poll_ready(storage.snapshot_map_future::<512, 5, _, _, _, 4>(
        &mut flash,
        &mut workspace,
        &source,
    ))
    .unwrap();

    let mut payload_buffer = [0u8; 128];
    poll_ready(storage.append_map_update_future::<512, 5, _, i32, i32, 4>(
        &mut flash,
        &mut workspace,
        CollectionId(41),
        &MapUpdate::Set { key: 2, value: 20 },
        &mut payload_buffer,
    ))
    .unwrap();

    source.set(3, 30).unwrap();
    let committed_region = poll_until_ready(
        storage.flush_map_future::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &source),
        4,
    )
    .unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(committed_region)
    );

    let reclaim_region = poll_ready(storage.drop_map_future::<512, 5, _>(
        &mut flash,
        &mut workspace,
        CollectionId(41),
    ))
    .unwrap();

    assert_eq!(reclaim_region, Some(committed_region));
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
}

//= spec/implementation.md#execution-requirements
//= type=test
//# `RING-IMPL-EXEC-005` Await boundaries inside borromean operations MUST align only with externally visible I/O steps or with pure in-memory decision points that preserve the ring ordering rules.
#[test]
fn storage_flush_map_future_yields_between_durable_phases() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(42))
        .unwrap();

    let region_index = {
        let mut map_buffer = [0u8; 512];
        let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(42), &mut map_buffer).unwrap();
        map.set(5, 50).unwrap();

        let future =
            storage.flush_map_future::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map);
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
//# `RING-IMPL-OP-003` If an operation future is dropped before completion, any already-issued durable writes MUST still satisfy the crash-safety rules from [spec/ring.md](ring.md).
#[test]
fn storage_flush_map_future_drop_after_region_write_remains_recoverable() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(43))
        .unwrap();

    {
        let mut map_buffer = [0u8; 512];
        let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(43), &mut map_buffer).unwrap();
        map.set(7, 70).unwrap();

        let future =
            storage.flush_map_future::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map);
        let mut future = pin!(future);

        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    }

    assert!(storage.ready_region().is_some());
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );
    assert_eq!(reopened.ready_region(), storage.ready_region());
}

//= spec/implementation.md#architecture-requirements
//= type=test
//# `RING-IMPL-ARCH-001` `Storage` MUST own logical storage state and configuration, but MUST NOT require long-lived ownership of the backing I/O object.
#[test]
fn storage_format_returns_logical_state_without_owning_backend() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    let storage =
        Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    assert_eq!(storage.metadata().region_size, 256);
    assert_eq!(storage.metadata().region_count, 4);
    assert_eq!(storage.wal_head(), 0);
    assert_eq!(storage.wal_tail(), 0);
    assert_eq!(storage.last_free_list_head(), Some(1));
    assert_eq!(storage.free_list_tail(), Some(3));
    assert_eq!(storage.tracked_user_collection_count(), 0);
}

#[test]
fn storage_append_and_reopen_round_trip_through_flash() {
    let mut flash = MockFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    {
        let mut storage =
            Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
        storage
            .append_new_collection::<256, 4, _>(
                &mut flash,
                &mut workspace,
                CollectionId(7),
                CollectionType::MAP_CODE,
            )
            .unwrap();
        storage
            .append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(7), &[1, 2, 3])
            .unwrap();

        assert_eq!(storage.collections().len(), 1);
        assert_eq!(storage.collections()[0].collection_id(), CollectionId(7));
        assert_eq!(storage.collections()[0].pending_update_count(), 1);
    }

    let reopened = Storage::<8, 4>::open::<256, 4, _>(&mut flash, &mut workspace).unwrap();

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

//= spec/ring.md#collection-head-state-machine
//= type=test
//# `RING-FORMAT-012` Every non-WAL `collection_type` that may appear durably on disk MUST have a corresponding normative collection specification.
#[test]
fn storage_append_new_collection_rejects_unsupported_channel_collection() {
    let mut flash = MockFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();
    assert!(matches!(
        Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5)
            .unwrap()
            .append_new_collection::<256, 4, _>(
                &mut flash,
                &mut workspace,
                CollectionId(22),
                CollectionType::CHANNEL_CODE,
            ),
        Err(StorageRuntimeError::UnsupportedCollectionType(
            CollectionType::CHANNEL_CODE
        ))
    ));
}

//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-001` Public entry points for format, open, replay, and mutating collection operations MUST make their workspace and I/O dependencies explicit in the function signature.
#[test]
fn storage_rotation_api_keeps_backend_explicit() {
    let mut flash = MockFlash::<128, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<128>::new();
    let mut storage =
        Storage::<8, 4>::format::<128, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    let next_region_index = storage
        .append_wal_rotation_start::<128, 4, _>(&mut flash, &mut workspace)
        .unwrap();
    storage
        .append_wal_rotation_finish::<128, 4, _>(&mut flash, &mut workspace, next_region_index)
        .unwrap();

    assert_eq!(storage.wal_head(), 0);
    assert_eq!(storage.wal_tail(), next_region_index);
    assert_eq!(storage.ready_region(), None);
}

#[test]
fn storage_reclaim_wal_head_updates_runtime_head_to_next_region() {
    let (mut flash, mut workspace, mut storage, next_region) = setup_storage_with_stale_wal_head();

    let reclaimed_head = storage
        .reclaim_wal_head::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();

    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), next_region);
    assert_eq!(storage.wal_tail(), next_region);
}

//= spec/ring.md#core-requirements
//= type=test
//# `RING-CORE-008` Borromean MUST model WAL-head movement as ordinary
//# `head(collection_id = 0, collection_type = wal, region_index = ...)`
//# records rather than a WAL-specific head record type.
#[test]
fn storage_reclaim_wal_head_appends_an_ordinary_head_record_for_wal_movement() {
    let (mut flash, mut workspace, mut storage, next_region) = setup_storage_with_stale_wal_head();

    let reclaimed_head = storage
        .reclaim_wal_head::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();

    let mut saw_wal_head_record = false;
    storage
        .runtime()
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
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
        .unwrap();

    assert_eq!(reclaimed_head, next_region);
    assert!(saw_wal_head_record);
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-PRE-001` The candidate region MUST be the head of
//# the WAL.
#[test]
fn storage_reclaim_wal_head_returns_old_head_region_to_free_list_tail() {
    let (mut flash, mut workspace, mut storage, _) = setup_storage_with_stale_wal_head();

    storage
        .reclaim_wal_head::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();

    assert_eq!(storage.free_list_tail(), Some(0));
    assert!(storage.pending_reclaims().is_empty());
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-PRE-002` For every live record in the candidate,
//# an equivalent live state MUST be already represented durably outside
//# the candidate (typically by newer `snapshot`, `drop_collection`, or
//# by `head(collection_id, collection_type, region_index)` plus newer
//# updates).
#[test]
fn storage_reclaim_wal_head_copies_live_snapshot_basis_to_tail() {
    let (mut flash, mut workspace, mut storage, next_region) =
        setup_storage_with_live_snapshot_in_wal_head();

    let reclaimed_head = storage
        .reclaim_wal_head::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();

    let mut saw_snapshot = false;
    storage
        .runtime()
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
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
        .unwrap();

    assert!(saw_snapshot);
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), next_region);
    assert!(storage.wal_tail() >= next_region);
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-001` No collection's `H(c)`, `B(c)`, or live
//# post-basis updates MUST NOT depend on bytes in the reclaimed region.
#[test]
fn storage_reclaim_wal_head_copies_live_updates_after_basis_to_tail() {
    let (mut flash, mut workspace, mut storage, next_region) =
        setup_storage_with_live_snapshot_and_update_in_wal_head();

    let reclaimed_head = storage
        .reclaim_wal_head::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();

    let mut saw_snapshot = false;
    let mut saw_update = false;
    storage
        .runtime()
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
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
        .unwrap();

    assert!(saw_snapshot);
    assert!(saw_update);
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), next_region);
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::WalSnapshot
    );
    assert_eq!(storage.collections()[0].pending_update_count(), 1);
}

#[test]
fn storage_reclaim_wal_head_rewrites_empty_head_map_as_snapshot_basis() {
    let (mut flash, mut workspace, mut storage, next_region) =
        setup_storage_with_live_empty_head_map_in_wal_head();

    let reclaimed_head = storage
        .reclaim_wal_head::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();

    let target = storage
        .collections()
        .iter()
        .find(|collection| collection.collection_id() == CollectionId(36))
        .unwrap();
    assert_eq!(target.basis(), StartupCollectionBasis::WalSnapshot);
    assert_eq!(target.pending_update_count(), 1);
    assert_eq!(reclaimed_head, next_region);
    assert_eq!(storage.wal_head(), next_region);
}

fn reclaim_wal_head_and_reopen_empty_head_map() -> (
    MockFlash<512, 6, 4096>,
    StorageWorkspace<512>,
    Storage<8, 4>,
    Storage<8, 4>,
    Option<u32>,
    Option<u32>,
) {
    let (mut flash, mut workspace, mut storage, _) =
        setup_storage_with_live_empty_head_map_in_wal_head();
    let expected_free_list_head = storage.last_free_list_head();
    let expected_ready_region = storage.ready_region();

    storage
        .reclaim_wal_head::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();

    let reopened = Storage::<8, 4>::open::<512, 6, _>(&mut flash, &mut workspace).unwrap();

    (
        flash,
        workspace,
        storage,
        reopened,
        expected_free_list_head,
        expected_ready_region,
    )
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-PRE-003` After planned metadata updates, startup
//# replay MUST still be able to walk a valid WAL chain from head to
//# tail.
#[test]
fn storage_reclaim_wal_head_reopen_keeps_the_wal_chain_walkable() {
    let (mut flash, mut workspace, _, reopened, _, _) =
        reclaim_wal_head_and_reopen_empty_head_map();

    let mut record_count = 0usize;
    reopened
        .runtime()
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, _record| {
            record_count += 1;
            Ok(())
        })
        .unwrap();

    assert!(record_count > 0);
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-SAFE-001` Reclaim MUST NOT change replay result: the recovered `last_head` and `pending_updates` for every collection, the recovered `last_free_list_head`, reserved `ready_region`, ordered incomplete reclaim state, and reconstructed `free_list_tail`, after reclaim must match the pre-reclaim logical state.
#[test]
fn storage_reclaim_wal_head_reopen_preserves_replay_result() {
    let (mut flash, mut workspace, storage, reopened, _, _) =
        reclaim_wal_head_and_reopen_empty_head_map();

    assert_eq!(reopened.collections(), storage.collections());
    assert_eq!(
        reopened.last_free_list_head(),
        storage.last_free_list_head()
    );
    assert_eq!(reopened.ready_region(), storage.ready_region());
    assert_eq!(reopened.pending_reclaims(), storage.pending_reclaims());
    assert_eq!(reopened.free_list_tail(), storage.free_list_tail());

    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<512, 6, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(36),
            &mut reopen_buffer,
        )
        .unwrap();

    assert_eq!(reopened_map.get(&1).unwrap(), Some(10));
    assert_eq!(reopened_map.get(&2).unwrap(), None);
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-005` Startup step 4 MUST recover the same effective WAL head after
//# reclaim as before reclaim, using the current tail region's
//# `WalRegionPrologue` plus the last valid tail-local
//# `head(collection_id = 0, collection_type = wal, region_index = ...)`
//# override, if any.
#[test]
fn storage_reclaim_wal_head_reopen_preserves_effective_wal_head() {
    let (_, _, storage, reopened, _, _) = reclaim_wal_head_and_reopen_empty_head_map();

    assert_eq!(reopened.wal_head(), storage.wal_head());
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-002` The recovered free-list head MUST match pre-reclaim allocator state.
#[test]
fn storage_reclaim_wal_head_reopen_preserves_free_list_head() {
    let (_, _, _, reopened, expected_free_list_head, _) =
        reclaim_wal_head_and_reopen_empty_head_map();

    assert_eq!(reopened.last_free_list_head(), expected_free_list_head);
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-003` The recovered `ready_region`, if any, MUST match pre-reclaim allocator state.
#[test]
fn storage_reclaim_wal_head_reopen_preserves_ready_region() {
    let (_, _, _, reopened, _, expected_ready_region) =
        reclaim_wal_head_and_reopen_empty_head_map();

    assert_eq!(reopened.ready_region(), expected_ready_region);
}

//= spec/ring.md#wal-reclaim-eligibility
//= type=test
//# `RING-WAL-RECLAIM-POST-006` WAL chain integrity MUST remain valid
//# with no broken `link` path.
#[test]
fn storage_reclaim_wal_head_reopen_has_no_broken_link_path() {
    let (mut flash, mut workspace, _, reopened, _, _) =
        reclaim_wal_head_and_reopen_empty_head_map();
    let mut reopen_buffer = [0u8; 512];

    reopened
        .runtime()
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, _record| Ok(()))
        .unwrap();

    let reopened_map = reopened
        .open_map::<512, 6, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(36),
            &mut reopen_buffer,
        )
        .unwrap();
    assert_eq!(reopened_map.get(&1).unwrap(), Some(10));
}

#[test]
fn storage_works_through_flash_io_trait_backend() {
    let mut flash = DelegatingFlash::<256, 4, 256>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    let mut storage =
        Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    storage
        .append_new_collection::<256, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(9),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    storage
        .append_update::<256, 4, _>(&mut flash, &mut workspace, CollectionId(9), &[4, 5])
        .unwrap();

    let reopened = Storage::<8, 4>::open::<256, 4, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(reopened.collections().len(), 1);
    assert_eq!(reopened.collections()[0].collection_id(), CollectionId(9));
    assert_eq!(reopened.collections()[0].pending_update_count(), 1);
}

#[test]
fn storage_map_api_restores_snapshot_and_updates() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 4, _>(&mut flash, &mut workspace, CollectionId(11))
        .unwrap();

    let mut source_buffer = [0u8; 512];
    let mut source = LsmMap::<i32, i32, 4>::new(CollectionId(11), &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    source.set(2, 20).unwrap();
    storage
        .snapshot_map::<512, 4, _, _, _, 4>(&mut flash, &mut workspace, &source)
        .unwrap();

    let mut update_payload = [0u8; 64];
    let update_len = LsmMap::<i32, i32, 4>::encode_update_into(
        &MapUpdate::Set { key: 2, value: 99 },
        &mut update_payload,
    )
    .unwrap();
    storage
        .append_update::<512, 4, _>(
            &mut flash,
            &mut workspace,
            CollectionId(11),
            &update_payload[..update_len],
        )
        .unwrap();

    let mut reopen_buffer = [0u8; 512];
    let reopened = storage
        .open_map::<512, 4, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(11),
            &mut reopen_buffer,
        )
        .unwrap();

    assert_eq!(reopened.get(&1).unwrap(), Some(10));
    assert_eq!(reopened.get(&2).unwrap(), Some(99));
}

//= spec/ring.md#core-requirements
//= type=test
//# `RING-CORE-012` The implementation MUST maintain
//# `min_free_regions >= max_in_memory_dirty_collections + 1`.
#[test]
fn storage_map_frontiers_do_not_exceed_the_configured_dirty_collection_reserve() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 6;
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        2,
        8,
        0xa5,
    )
    .unwrap();

    storage
        .create_map::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, CollectionId(48))
        .unwrap();
    storage
        .create_map::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, CollectionId(49))
        .unwrap();

    let mut first_buffer = [0u8; 128];
    let mut second_buffer = [0u8; 128];
    let mut first_map = LsmMap::<u16, u16, 8>::new(CollectionId(48), &mut first_buffer).unwrap();
    let mut second_map = LsmMap::<u16, u16, 8>::new(CollectionId(49), &mut second_buffer).unwrap();
    let mut payload_buffer = [0u8; 64];

    storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut first_map,
            &MapUpdate::Set { key: 1, value: 10 },
            &mut payload_buffer,
        )
        .unwrap();

    let error = storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut second_map,
            &MapUpdate::Set { key: 2, value: 20 },
            &mut payload_buffer,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        MapStorageError::Storage(StorageRuntimeError::TooManyDirtyFrontiers {
            dirty_frontiers: 2,
            min_free_regions: 2,
        })
    ));
    assert_eq!(first_map.get(&1).unwrap(), Some(10));
    assert_eq!(second_map.get(&2).unwrap(), None);

    storage
        .flush_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &first_map,
        )
        .unwrap();

    storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut second_map,
            &MapUpdate::Set { key: 2, value: 20 },
            &mut payload_buffer,
        )
        .unwrap();
    assert_eq!(second_map.get(&2).unwrap(), Some(20));
}

//= spec/ring.md#core-requirements
//= type=test
//# `RING-CORE-016` If applying another update would exceed that
//# capacity, the implementation MUST flush the collection's current
//# logical frontier into a newly allocated region, durably commit that
//# region as the collection head, and clear the in-memory frontier before
//# accepting further updates for that collection.
#[test]
fn storage_map_frontier_overflow_flushes_and_commits_a_new_region_head() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 6;
    let capacity = smallest_map_capacity_for_repeated_updates(3);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        2,
        8,
        0xa5,
    )
    .unwrap();

    storage
        .create_map::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, CollectionId(46))
        .unwrap();

    let mut map_buffer = vec![0u8; capacity];
    let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(46), &mut map_buffer).unwrap();
    let mut payload_buffer = [0u8; 64];

    storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut map,
            &MapUpdate::Set { key: 1, value: 10 },
            &mut payload_buffer,
        )
        .unwrap();
    storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut map,
            &MapUpdate::Set { key: 1, value: 20 },
            &mut payload_buffer,
        )
        .unwrap();
    storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut map,
            &MapUpdate::Set { key: 1, value: 30 },
            &mut payload_buffer,
        )
        .unwrap();

    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Empty
    );

    storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut map,
            &MapUpdate::Set { key: 1, value: 40 },
            &mut payload_buffer,
        )
        .unwrap();

    let StartupCollectionBasis::Region(region_index) = storage.collections()[0].basis() else {
        panic!("frontier overflow should commit a durable region head");
    };

    let mut seen = [WalRecordType::WalRecovery; 7];
    let mut count = 0usize;
    storage
        .runtime()
        .visit_wal_records::<REGION_SIZE, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
            seen[count] = record.record_type();
            count += 1;
            Ok(())
        })
        .unwrap();

    assert_eq!(count, 7);
    assert_eq!(
        seen,
        [
            WalRecordType::NewCollection,
            WalRecordType::Update,
            WalRecordType::Update,
            WalRecordType::Update,
            WalRecordType::AllocBegin,
            WalRecordType::Head,
            WalRecordType::Update,
        ]
    );

    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened = storage
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(46),
            &mut reopen_buffer,
        )
        .unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(region_index)
    );
    assert_eq!(reopened.get(&1).unwrap(), Some(40));
}

//= spec/ring.md#core-requirements
//= type=test
//# `RING-CORE-017` After such a frontier-capacity flush, later updates
//# for that collection MUST accumulate in a fresh in-memory frontier
//# layered over the newly committed region head.
#[test]
fn storage_map_frontier_continues_accumulating_updates_after_an_overflow_flush() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 6;
    let capacity = smallest_map_capacity_for_repeated_updates(3);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        2,
        8,
        0xa5,
    )
    .unwrap();

    storage
        .create_map::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, CollectionId(47))
        .unwrap();

    let mut map_buffer = vec![0u8; capacity];
    let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(47), &mut map_buffer).unwrap();
    let mut payload_buffer = [0u8; 64];

    for value in [10u16, 20, 30] {
        storage
            .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
                &mut flash,
                &mut workspace,
                &mut map,
                &MapUpdate::Set { key: 1, value },
                &mut payload_buffer,
            )
            .unwrap();
    }

    storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut map,
            &MapUpdate::Set { key: 1, value: 40 },
            &mut payload_buffer,
        )
        .unwrap();

    let StartupCollectionBasis::Region(head_after_flush) = storage.collections()[0].basis() else {
        panic!("overflow flush should leave the collection on a committed region head");
    };

    storage
        .update_map_frontier::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            &mut map,
            &MapUpdate::Set { key: 2, value: 50 },
            &mut payload_buffer,
        )
        .unwrap();

    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(head_after_flush)
    );
    assert_eq!(map.get(&1).unwrap(), Some(40));
    assert_eq!(map.get(&2).unwrap(), Some(50));

    let mut reopen_buffer = [0u8; REGION_SIZE];
    let reopened = storage
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(47),
            &mut reopen_buffer,
        )
        .unwrap();
    assert_eq!(reopened.get(&1).unwrap(), Some(40));
    assert_eq!(reopened.get(&2).unwrap(), Some(50));
}

//= spec/implementation.md#api-requirements
//= type=test
//# `RING-IMPL-API-003` Collection implementations MUST define their opaque payload semantics above the shared storage primitives rather than bypassing WAL and region-management invariants.
#[test]
fn storage_map_api_appends_typed_updates() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(17))
        .unwrap();

    let mut payload_buffer = [0u8; 128];
    storage
        .append_map_update::<512, 5, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(17),
            &MapUpdate::Set { key: 4, value: 40 },
            &mut payload_buffer,
        )
        .unwrap();
    storage
        .append_map_update::<512, 5, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(17),
            &MapUpdate::Delete { key: 4 },
            &mut payload_buffer,
        )
        .unwrap();

    let mut reopen_buffer = [0u8; 512];
    let reopened = storage
        .open_map::<512, 5, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(17),
            &mut reopen_buffer,
        )
        .unwrap();

    assert_eq!(reopened.get(&4).unwrap(), None);
}

#[test]
fn storage_map_api_flushes_committed_region_basis() {
    let mut flash = MockFlash::<512, 4, 1024>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 4, _>(&mut flash, &mut workspace, CollectionId(12))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(12), &mut map_buffer).unwrap();
    map.set(5, 50).unwrap();
    map.set(7, 70).unwrap();

    let region_index = storage
        .flush_map::<512, 4, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(region_index)
    );
    assert_eq!(storage.ready_region(), None);

    let mut reopen_buffer = [0u8; 512];
    let reopened = storage
        .open_map::<512, 4, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(12),
            &mut reopen_buffer,
        )
        .unwrap();

    assert_eq!(reopened.get(&5).unwrap(), Some(50));
    assert_eq!(reopened.get(&7).unwrap(), Some(70));
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-PRE-001` `reclaim_begin(r)` MUST be durable in
//# the WAL before any live metadata is updated to stop referencing `r`.
#[test]
fn storage_map_replacement_flush_records_reclaim_before_new_head() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(18))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(18), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    let first_region = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    map.set(2, 20).unwrap();
    let second_region = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    let mut saw_reclaim_begin = false;
    let mut saw_alloc_begin = false;
    let mut saw_replacement_head = false;
    storage
        .runtime()
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
            match record {
                crate::WalRecord::AllocBegin { .. } => {
                    saw_alloc_begin = true;
                }
                crate::WalRecord::ReclaimBegin { region_index } if region_index == first_region => {
                    assert!(!saw_replacement_head);
                    saw_reclaim_begin = true;
                }
                crate::WalRecord::Head {
                    collection_id,
                    region_index,
                    ..
                } if collection_id == CollectionId(18) && region_index == second_region => {
                    assert!(saw_reclaim_begin);
                    saw_replacement_head = true;
                }
                _ => {}
            }
            Ok(())
        })
        .unwrap();

    assert!(saw_alloc_begin);
    assert!(saw_reclaim_begin);
    assert!(saw_replacement_head);
}

#[test]
fn storage_map_replacement_flush_is_completed_during_reopen() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(13))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(13), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    let first_region = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    map.set(1, 20).unwrap();
    let second_region = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    assert_ne!(first_region, second_region);
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(storage.pending_reclaims(), &[first_region]);

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert!(reopened.pending_reclaims().is_empty());
    assert_eq!(reopened.free_list_tail(), Some(first_region));

    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<512, 5, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(13),
            &mut reopen_buffer,
        )
        .unwrap();
    assert_eq!(reopened_map.get(&1).unwrap(), Some(20));
}

fn replace_map_into_pending_reclaim_with_empty_free_list() -> (
    MockFlash<512, 3, 2048>,
    StorageWorkspace<512>,
    Storage<8, 4>,
    u32,
    u32,
) {
    let mut flash = MockFlash::<512, 3, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 3, _>(&mut flash, &mut workspace, 0, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 3, _>(&mut flash, &mut workspace, CollectionId(26))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(26), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    let first_region = storage
        .flush_map::<512, 3, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    map.set(2, 20).unwrap();
    let second_region = storage
        .flush_map::<512, 3, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    assert_ne!(first_region, second_region);
    assert_eq!(storage.last_free_list_head(), None);
    assert_eq!(storage.free_list_tail(), None);
    assert_eq!(storage.pending_reclaims(), &[first_region]);

    (flash, workspace, storage, first_region, second_region)
}

fn replace_map_and_reopen_empty_free_list() -> (
    MockFlash<512, 3, 2048>,
    StorageWorkspace<512>,
    Storage<8, 4>,
    Storage<8, 4>,
    u32,
    u32,
) {
    let (mut flash, mut workspace, storage, first_region, second_region) =
        replace_map_into_pending_reclaim_with_empty_free_list();
    let reopened = Storage::<8, 4>::open::<512, 3, _>(&mut flash, &mut workspace).unwrap();

    (
        flash,
        workspace,
        storage,
        reopened,
        first_region,
        second_region,
    )
}

//= spec/ring.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-020 Initialize allocator state from `last_free_list_head`.
#[test]
fn storage_reopen_after_replacement_initializes_allocator_from_recovered_free_list_head() {
    let (_, _, _, reopened, first_region, _) = replace_map_and_reopen_empty_free_list();

    assert_eq!(reopened.last_free_list_head(), Some(first_region));
}

//= spec/ring.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-021 Reconstruct runtime `free_list_tail` by following free-pointer links starting at `last_free_list_head` until reaching a free region whose free-pointer slot is uninitialized.
#[test]
fn storage_reopen_after_replacement_reconstructs_free_list_tail() {
    let (_, _, _, reopened, first_region, _) = replace_map_and_reopen_empty_free_list();

    assert_eq!(reopened.free_list_tail(), Some(first_region));
}

//= spec/ring.md#startup-replay-algorithm
//= type=test
//# RING-STARTUP-007 Maintain replay state: per collection optional live `collection_type`, `last_head`, `basis_pos`, and `pending_updates`, plus global `last_free_list_head`, optional reserved `ready_region`, ordered pending region reclaims, and the replay-local `pending_wal_recovery_boundary`.
#[test]
fn storage_reopen_after_replacement_recovers_collection_and_reclaim_state() {
    let (mut flash, mut workspace, _, reopened, _, second_region) =
        replace_map_and_reopen_empty_free_list();

    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Region(second_region)
    );
    assert!(reopened.pending_reclaims().is_empty());
    assert_eq!(reopened.ready_region(), None);

    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<512, 3, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(26),
            &mut reopen_buffer,
        )
        .unwrap();
    assert_eq!(reopened_map.get(&1).unwrap(), Some(10));
    assert_eq!(reopened_map.get(&2).unwrap(), Some(20));
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-PRE-002` After the detach step, the reclaimed region `r` MUST no longer be reachable from any live collection head or live WAL state.
#[test]
fn storage_replacement_flush_detaches_reclaimed_region_from_live_state() {
    let (_, _, storage, first_region, second_region) =
        replace_map_into_pending_reclaim_with_empty_free_list();

    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Region(second_region)
    );
    assert_eq!(storage.pending_reclaims(), &[first_region]);
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-PRE-003` `r` MUST NOT already be reachable from the free-list chain, unless this procedure is being re-entered during crash recovery.
#[test]
fn storage_replacement_flush_keeps_detached_region_out_of_free_list_chain() {
    let (flash, _, storage, first_region, _) =
        replace_map_into_pending_reclaim_with_empty_free_list();

    let chain = free_list_chain::<512, 3, 2048, 8>(&flash, 0xff, storage.last_free_list_head());
    assert!(!chain.contains(&first_region));
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-SEM-003` If `t_prev = none`, reclaim MUST NOT write any predecessor link and MUST durably append `free_list_head(r)` and set `free_list_head = r` and `free_list_tail = r`.
#[test]
fn storage_reopen_after_replacement_recovers_singleton_free_list_for_reclaimed_region() {
    let (flash, _, storage, reopened, first_region, _) = replace_map_and_reopen_empty_free_list();

    assert_eq!(storage.last_free_list_head(), None);
    assert_eq!(storage.free_list_tail(), None);
    assert_eq!(reopened.last_free_list_head(), Some(first_region));
    assert_eq!(reopened.free_list_tail(), Some(first_region));

    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let footer = FreePointerFooter::decode(
        &flash.region_bytes(first_region).unwrap()[footer_offset..],
        0xff,
    )
    .unwrap();
    assert_eq!(footer.next_tail, None);
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-004` Establish `r` as a free region without
//# erasing it. In particular,
//# `r.free_pointer.next_tail` MUST still be uninitialized when `r` is
//# about to become the new free-list tail.
#[test]
fn storage_reopen_after_replacement_leaves_new_free_list_tail_uninitialized() {
    let (flash, _, _, _, first_region, _) = replace_map_and_reopen_empty_free_list();

    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let footer = FreePointerFooter::decode(
        &flash.region_bytes(first_region).unwrap()[footer_offset..],
        0xff,
    )
    .unwrap();
    assert_eq!(footer.next_tail, None);
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-005` The reclaim procedure MUST be idempotent across crashes between any two steps above.
#[test]
fn storage_reopen_after_replacement_recovers_reclaim_idempotently() {
    let (mut flash, mut workspace, _, reopened_once, _, _) =
        replace_map_and_reopen_empty_free_list();

    let reopened_twice = Storage::<8, 4>::open::<512, 3, _>(&mut flash, &mut workspace).unwrap();

    assert_eq!(reopened_twice.collections(), reopened_once.collections());
    assert_eq!(
        reopened_twice.last_free_list_head(),
        reopened_once.last_free_list_head()
    );
    assert_eq!(
        reopened_twice.free_list_tail(),
        reopened_once.free_list_tail()
    );
    assert_eq!(
        reopened_twice.pending_reclaims(),
        reopened_once.pending_reclaims()
    );
    assert_eq!(reopened_twice.ready_region(), reopened_once.ready_region());
}

//= spec/ring.md#core-requirements
//= type=test
//# `RING-CORE-014` If reclaim cannot restore at least
//# `min_free_regions` free regions, the database MUST treat ordinary
//# writes as out of space until space is freed or the store is migrated.
#[test]
fn storage_map_flush_rejects_consuming_min_free_region_reserve() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 3, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(23))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(23), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    map.set(2, 20).unwrap();
    let error = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap_err();

    assert!(matches!(
        error,
        MapStorageError::Storage(StorageRuntimeError::InsufficientFreeRegions {
            free_regions: 3,
            min_free_regions: 3,
        })
    ));
}

#[test]
fn storage_map_flush_completes_detached_reclaims_before_using_reserve() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 6, _>(&mut flash, &mut workspace, 3, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 6, _>(&mut flash, &mut workspace, CollectionId(24))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(24), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    let first_region = storage
        .flush_map::<512, 6, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    map.set(2, 20).unwrap();
    let second_region = storage
        .flush_map::<512, 6, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();
    assert_eq!(storage.pending_reclaims(), &[first_region]);

    map.set(3, 30).unwrap();
    let third_region = storage
        .flush_map::<512, 6, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    assert_ne!(third_region, first_region);
    assert_ne!(third_region, second_region);
    assert_eq!(storage.pending_reclaims(), &[second_region]);
}

//= spec/ring.md#core-requirements
//= type=test
//# `RING-CORE-013` Ordinary foreground allocations MUST NOT consume the
//# last `min_free_regions` free regions.
#[test]
fn storage_map_flush_reclaims_wal_head_before_consuming_min_free_region_reserve() {
    let mut flash = MockFlash::<512, 6, 4096>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 6, _>(&mut flash, &mut workspace, 3, 8, 0xa5).unwrap();

    storage
        .append_new_collection::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(130),
            CollectionType::MAP_CODE,
        )
        .unwrap();
    storage
        .append_update::<512, 6, _>(&mut flash, &mut workspace, CollectionId(130), &[1, 2, 3])
        .unwrap();

    let reclaimed_head =
        rotate_wal_tail_for_collection(&mut storage, &mut flash, &mut workspace, CollectionId(130));
    storage
        .append_snapshot::<512, 6, _>(
            &mut flash,
            &mut workspace,
            CollectionId(130),
            CollectionType::MAP_CODE,
            &[9, 9, 9],
        )
        .unwrap();

    storage
        .create_map::<512, 6, _>(&mut flash, &mut workspace, CollectionId(25))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(25), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    let first_region = storage
        .flush_map::<512, 6, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    map.set(2, 20).unwrap();
    let second_region = storage
        .flush_map::<512, 6, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    assert_ne!(second_region, first_region);
    assert_eq!(storage.wal_head(), reclaimed_head);
    assert_eq!(storage.pending_reclaims(), &[first_region]);
    assert_eq!(storage.free_list_tail(), Some(0));

    let reopened = Storage::<8, 4>::open::<512, 6, _>(&mut flash, &mut workspace).unwrap();
    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<512, 6, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(25),
            &mut reopen_buffer,
        )
        .unwrap();

    assert_eq!(reopened_map.get(&1).unwrap(), Some(10));
    assert_eq!(reopened_map.get(&2).unwrap(), Some(20));
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-SEM-002` If `t_prev != none`, reclaim MUST
//# durably write `t_prev.free_pointer.next_tail = r` when freeing region
//# `r`.
#[test]
fn storage_complete_pending_reclaim_writes_the_previous_tail_next_pointer() {
    let result = complete_pending_reclaim_returns_old_map_region_to_free_list_result();
    assert_eq!(result.previous_tail_next, Some(result.reclaimed_region));
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-001` The free-list chain MUST remain acyclic and FIFO-ordered.
#[test]
fn storage_complete_pending_reclaim_preserves_fifo_free_list_order() {
    let result = complete_pending_reclaim_returns_old_map_region_to_free_list_result();
    assert_eq!(
        &result.new_chain[..result.previous_chain.len()],
        result.previous_chain.as_slice()
    );
    assert_eq!(
        result.new_chain.last().copied(),
        Some(result.reclaimed_region)
    );
    assert_eq!(
        result.reopened_chain.as_slice(),
        result.new_chain.as_slice()
    );
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-002` Exactly one new region (`r`) MUST be
//# appended to the tail.
#[test]
fn storage_complete_pending_reclaim_appends_exactly_one_region_to_the_tail() {
    let result = complete_pending_reclaim_returns_old_map_region_to_free_list_result();

    assert_eq!(result.new_chain.len(), result.previous_chain.len() + 1);
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

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-003` If a prior tail existed, its
//# `next_tail` pointer MUST now reference `r`.
#[test]
fn storage_complete_pending_reclaim_points_the_previous_tail_at_the_reclaimed_region() {
    let result = complete_pending_reclaim_returns_old_map_region_to_free_list_result();
    assert_eq!(result.previous_tail_next, Some(result.reclaimed_region));
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-004` `r.free_pointer.next_tail` MUST
//# remain uninitialized after reclaim.
#[test]
fn storage_complete_pending_reclaim_keeps_the_reclaimed_tail_footer_uninitialized() {
    let result = complete_pending_reclaim_returns_old_map_region_to_free_list_result();
    assert_eq!(result.reclaimed_tail_next, None);
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-POST-005` If a prior tail existed, replay of free pointers MUST follow
//# `... -> t_prev -> r`, and `r` is recognized as the tail because its
//# free-pointer slot is uninitialized.
#[test]
fn storage_complete_pending_reclaim_links_the_previous_tail_to_the_reclaimed_region() {
    let result = complete_pending_reclaim_returns_old_map_region_to_free_list_result();
    assert_eq!(result.previous_tail_next, Some(result.reclaimed_region));
    assert_eq!(result.reclaimed_tail_next, None);
    assert_eq!(
        result.reopened_free_list_tail,
        Some(result.reclaimed_region)
    );
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-002` Before any durable write makes `r`
//# reachable from `t_prev.next_tail`, the implementation MUST ensure
//# that `r` already has the correct free-list-tail footer state, namely
//# an uninitialized `r.free_pointer.next_tail`.
#[test]
fn storage_complete_pending_reclaim_prepares_the_reclaimed_footer_before_syncing_the_tail_link() {
    let result = complete_pending_reclaim_returns_old_map_region_to_free_list_result();
    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let reclaimed_footer_write = result
        .reclaim_operations
        .iter()
        .position(|operation| {
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
        })
        .unwrap();
    let first_sync = result
        .reclaim_operations
        .iter()
        .position(|operation| *operation == crate::MockOperation::Sync)
        .unwrap();

    assert!(reclaimed_footer_write < first_sync);
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-003` If `t_prev = none`,
//# `free_list_head(r)` MUST be durable before `reclaim_end(r)` is
//# acknowledged.
#[test]
fn storage_complete_pending_reclaim_records_free_list_head_before_reclaim_end_when_the_list_was_empty(
) {
    let (mut flash, mut workspace, mut storage, first_region, _) =
        replace_map_into_pending_reclaim_with_empty_free_list();

    storage
        .complete_pending_reclaim::<512, 3, _>(&mut flash, &mut workspace, first_region)
        .unwrap();

    let mut saw_free_list_head = false;
    let mut saw_reclaim_end = false;
    storage
        .runtime()
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
            match record {
                crate::WalRecord::FreeListHead { region_index }
                    if region_index == Some(first_region) =>
                {
                    assert!(!saw_reclaim_end);
                    saw_free_list_head = true;
                }
                crate::WalRecord::ReclaimEnd { region_index } if region_index == first_region => {
                    assert!(saw_free_list_head);
                    saw_reclaim_end = true;
                }
                _ => {}
            }
            Ok(())
        })
        .unwrap();

    assert!(saw_free_list_head);
    assert!(saw_reclaim_end);
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-004` If `t_prev` exists, the
//# `t_prev.next_tail = r` write MUST be synced before `reclaim_end(r)`
//# is acknowledged.
#[test]
fn storage_complete_pending_reclaim_syncs_the_tail_link_before_reclaim_end() {
    let result = complete_pending_reclaim_returns_old_map_region_to_free_list_result();
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
    let first_sync = result
        .reclaim_operations
        .iter()
        .position(|operation| *operation == crate::MockOperation::Sync)
        .unwrap();
    let reclaim_end_write = result
        .reclaim_operations
        .iter()
        .position(|operation| {
            matches!(
                operation,
                crate::MockOperation::WriteRegion {
                    region_index,
                    ..
                } if *region_index == result.wal_tail_before_reclaim
            )
        })
        .unwrap();

    assert!(previous_tail_link_write < first_sync);
    assert!(first_sync < reclaim_end_write);
}

#[test]
fn storage_reopen_discards_reclaim_begin_before_replacement_detaches_old_head() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(20))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(20), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    let first_region = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    storage
        .append_reclaim_begin::<512, 5, _>(&mut flash, &mut workspace, first_region)
        .unwrap();
    assert_eq!(storage.pending_reclaims(), &[first_region]);

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Region(first_region)
    );
    assert!(reopened.pending_reclaims().is_empty());

    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<512, 5, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(20),
            &mut reopen_buffer,
        )
        .unwrap();
    assert_eq!(reopened_map.get(&1).unwrap(), Some(10));
}

#[test]
fn storage_drop_map_starts_reclaim_for_committed_region_basis() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(15))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(15), &mut map_buffer).unwrap();
    map.set(8, 80).unwrap();
    let region_index = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    let reclaim_region = storage
        .drop_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(15))
        .unwrap();

    assert_eq!(reclaim_region, Some(region_index));
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
    assert_eq!(storage.pending_reclaims(), &[region_index]);
    assert_eq!(storage.tracked_user_collection_count(), 0);

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
    assert!(reopened.pending_reclaims().is_empty());
    assert_eq!(reopened.free_list_tail(), Some(region_index));

    let mut reopen_buffer = [0u8; 512];
    let result = reopened.open_map::<512, 5, _, i32, i32, 4>(
        &mut flash,
        &mut workspace,
        CollectionId(15),
        &mut reopen_buffer,
    );
    assert!(matches!(
        result,
        Err(MapStorageError::DroppedCollection(CollectionId(15)))
    ));
}

#[test]
fn storage_reopen_discards_reclaim_begin_before_drop_detaches_live_region() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(21))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(21), &mut map_buffer).unwrap();
    map.set(8, 80).unwrap();
    let region_index = storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    storage
        .append_reclaim_begin::<512, 5, _>(&mut flash, &mut workspace, region_index)
        .unwrap();
    assert_eq!(storage.pending_reclaims(), &[region_index]);

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Region(region_index)
    );
    assert!(reopened.pending_reclaims().is_empty());

    let mut reopen_buffer = [0u8; 512];
    let reopened_map = reopened
        .open_map::<512, 5, _, i32, i32, 4>(
            &mut flash,
            &mut workspace,
            CollectionId(21),
            &mut reopen_buffer,
        )
        .unwrap();
    assert_eq!(reopened_map.get(&8).unwrap(), Some(80));
}

//= spec/ring.md#region-reclaim
//= type=test
//# `RING-REGION-RECLAIM-ORDER-001` `reclaim_begin(r)` MUST be durable
//# before any live metadata stops referencing `r`.
#[test]
fn storage_drop_map_records_reclaim_before_drop() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(19))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(19), &mut map_buffer).unwrap();
    map.set(8, 80).unwrap();
    storage
        .flush_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    storage
        .drop_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(19))
        .unwrap();

    let mut last_two = [(crate::WalRecordType::WalRecovery, CollectionId(0)); 2];
    storage
        .runtime()
        .visit_wal_records::<512, _, (), _>(&mut flash, &mut workspace, |_flash, record| {
            let record_type = record.record_type();
            if matches!(
                record_type,
                crate::WalRecordType::ReclaimBegin | crate::WalRecordType::DropCollection
            ) {
                let collection_id = match record {
                    crate::WalRecord::DropCollection { collection_id } => collection_id,
                    _ => CollectionId(0),
                };
                last_two.rotate_left(1);
                last_two[last_two.len() - 1] = (record_type, collection_id);
            }
            Ok(())
        })
        .unwrap();

    assert_eq!(
        last_two,
        [
            (crate::WalRecordType::ReclaimBegin, CollectionId(0)),
            (crate::WalRecordType::DropCollection, CollectionId(19)),
        ]
    );
}

#[test]
fn storage_drop_map_from_snapshot_basis_has_no_region_reclaim() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();

    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(16))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<i32, i32, 4>::new(CollectionId(16), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();
    storage
        .snapshot_map::<512, 5, _, _, _, 4>(&mut flash, &mut workspace, &map)
        .unwrap();

    let reclaim_region = storage
        .drop_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(16))
        .unwrap();

    assert_eq!(reclaim_region, None);
    assert_eq!(
        storage.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
    assert!(storage.pending_reclaims().is_empty());
}
