use super::*;
use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

mod traceability;

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
        let footer =
            FreePointerFooter::decode(&flash.region_bytes(region_index).unwrap()[footer_offset..], erased_byte)
                .unwrap();
        current = footer.next_tail;
    }

    assert!(current.is_none(), "free-list chain should terminate");
    chain
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

    let next_region = rotate_wal_tail_for_collection(
        &mut storage,
        &mut flash,
        &mut workspace,
        CollectionId(31),
    );
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

    let next_region = rotate_wal_tail_for_collection(
        &mut storage,
        &mut flash,
        &mut workspace,
        CollectionId(132),
    );

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

    let next_region = rotate_wal_tail_for_collection(
        &mut storage,
        &mut flash,
        &mut workspace,
        CollectionId(133),
    );

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

    let next_region = rotate_wal_tail_for_collection(
        &mut storage,
        &mut flash,
        &mut workspace,
        CollectionId(136),
    );
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
        let mut storage = Storage::<8, 4>::format::<256, 4, _>(
            &mut flash,
            &mut workspace,
            1,
            8,
            0xa5,
        )
        .unwrap();
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
    assert_eq!(reopened.collections()[0].basis(), StartupCollectionBasis::WalSnapshot);
}

#[test]
//= spec/implementation.md#operation-requirements
//# `RING-IMPL-OP-002` A borromean future MUST either complete with a terminal result or remain safely resumable by further polling after any `Poll::Pending`.
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
        storage.flush_map_future::<512, 5, _, _, _, 4>(
            &mut flash,
            &mut workspace,
            &source,
        ),
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
    assert_eq!(storage.collections()[0].basis(), StartupCollectionBasis::Dropped);
}

#[test]
//= spec/implementation.md#execution-requirements
//# `RING-IMPL-EXEC-005` Await boundaries inside borromean operations MUST align only with externally visible I/O steps or with pure in-memory decision points that preserve the ring ordering rules.
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

        let future = storage.flush_map_future::<512, 5, _, _, _, 4>(
            &mut flash,
            &mut workspace,
            &map,
        );
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

#[test]
//= spec/implementation.md#operation-requirements
//# `RING-IMPL-OP-003` If an operation future is dropped before completion, any already-issued durable writes MUST still satisfy the crash-safety rules from [spec/ring.md](ring.md).
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

        let future = storage.flush_map_future::<512, 5, _, _, _, 4>(
            &mut flash,
            &mut workspace,
            &map,
        );
        let mut future = pin!(future);

        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
    }

    assert!(storage.ready_region().is_some());
    assert_eq!(storage.collections()[0].basis(), StartupCollectionBasis::Empty);

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(reopened.collections()[0].basis(), StartupCollectionBasis::Empty);
    assert_eq!(reopened.ready_region(), storage.ready_region());
}

#[test]
//= spec/implementation.md#architecture-requirements
//# `RING-IMPL-ARCH-001` `Storage` MUST own logical storage state and configuration, but MUST NOT require long-lived ownership of the backing I/O object.
fn storage_format_returns_logical_state_without_owning_backend() {
    let mut flash = MockFlash::<256, 4, 128>::new(0xff);
    let mut workspace = StorageWorkspace::<256>::new();

    let storage = Storage::<8, 4>::format::<256, 4, _>(&mut flash, &mut workspace, 1, 8, 0xa5)
        .unwrap();

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

#[test]
//= spec/ring.md#collection-head-state-machine
//# `RING-FORMAT-012` Every non-WAL `collection_type` that may appear durably on disk MUST have a corresponding normative collection specification.
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

#[test]
//= spec/implementation.md#api-requirements
//# `RING-IMPL-API-001` Public entry points for format, open, replay, and mutating collection operations MUST make their workspace and I/O dependencies explicit in the function signature.
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

#[test]
fn storage_reclaim_wal_head_returns_old_head_region_to_free_list_tail() {
    let (mut flash, mut workspace, mut storage, _) = setup_storage_with_stale_wal_head();

    storage
        .reclaim_wal_head::<512, 6, _>(&mut flash, &mut workspace)
        .unwrap();

    assert_eq!(storage.free_list_tail(), Some(0));
    assert!(storage.pending_reclaims().is_empty());
}

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
    assert_eq!(storage.collections()[0].basis(), StartupCollectionBasis::WalSnapshot);
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

#[test]
//= spec/ring.md#wal-reclaim-eligibility
//# `RING-WAL-RECLAIM-SAFE-001` Reclaim MUST NOT change replay result: the recovered `last_head` and `pending_updates` for every collection, the recovered `last_free_list_head`, reserved `ready_region`, ordered incomplete reclaim state, and reconstructed `free_list_tail`, after reclaim must match the pre-reclaim logical state.
fn storage_reclaim_wal_head_reopen_preserves_replay_result() {
    let (mut flash, mut workspace, storage, reopened, _, _) =
        reclaim_wal_head_and_reopen_empty_head_map();

    assert_eq!(reopened.collections(), storage.collections());
    assert_eq!(reopened.last_free_list_head(), storage.last_free_list_head());
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

#[test]
//= spec/ring.md#wal-reclaim-eligibility
//# `RING-WAL-RECLAIM-POST-005` Startup step 4 MUST recover the same effective WAL head after
//# reclaim as before reclaim, using the current tail region's
//# `WalRegionPrologue` plus the last valid tail-local
//# `head(collection_id = 0, collection_type = wal, region_index = ...)`
//# override, if any.
fn storage_reclaim_wal_head_reopen_preserves_effective_wal_head() {
    let (_, _, storage, reopened, _, _) = reclaim_wal_head_and_reopen_empty_head_map();

    assert_eq!(reopened.wal_head(), storage.wal_head());
}

#[test]
//= spec/ring.md#wal-reclaim-eligibility
//# `RING-WAL-RECLAIM-POST-002` The recovered free-list head MUST match pre-reclaim allocator state.
fn storage_reclaim_wal_head_reopen_preserves_free_list_head() {
    let (_, _, _, reopened, expected_free_list_head, _) =
        reclaim_wal_head_and_reopen_empty_head_map();

    assert_eq!(reopened.last_free_list_head(), expected_free_list_head);
}

#[test]
//= spec/ring.md#wal-reclaim-eligibility
//# `RING-WAL-RECLAIM-POST-003` The recovered `ready_region`, if any, MUST match pre-reclaim allocator state.
fn storage_reclaim_wal_head_reopen_preserves_ready_region() {
    let (_, _, _, reopened, _, expected_ready_region) =
        reclaim_wal_head_and_reopen_empty_head_map();

    assert_eq!(reopened.ready_region(), expected_ready_region);
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

#[test]
//= spec/implementation.md#api-requirements
//# `RING-IMPL-API-003` Collection implementations MUST define their opaque payload semantics above the shared storage primitives rather than bypassing WAL and region-management invariants.
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

    (flash, workspace, storage, reopened, first_region, second_region)
}

#[test]
//= spec/ring.md#startup-replay-algorithm
//# RING-STARTUP-020 Initialize allocator state from `last_free_list_head`.
fn storage_reopen_after_replacement_initializes_allocator_from_recovered_free_list_head() {
    let (_, _, _, reopened, first_region, _) = replace_map_and_reopen_empty_free_list();

    assert_eq!(reopened.last_free_list_head(), Some(first_region));
}

#[test]
//= spec/ring.md#startup-replay-algorithm
//# RING-STARTUP-021 Reconstruct runtime `free_list_tail` by following free-pointer links starting at `last_free_list_head` until reaching a free region whose free-pointer slot is uninitialized.
fn storage_reopen_after_replacement_reconstructs_free_list_tail() {
    let (_, _, _, reopened, first_region, _) = replace_map_and_reopen_empty_free_list();

    assert_eq!(reopened.free_list_tail(), Some(first_region));
}

#[test]
//= spec/ring.md#startup-replay-algorithm
//# RING-STARTUP-007 Maintain replay state: per collection optional live `collection_type`, `last_head`, `basis_pos`, and `pending_updates`, plus global `last_free_list_head`, optional reserved `ready_region`, ordered pending region reclaims, and the replay-local `pending_wal_recovery_boundary`.
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

#[test]
//= spec/ring.md#region-reclaim
//# `RING-REGION-RECLAIM-PRE-002` After the detach step, the reclaimed region `r` MUST no longer be reachable from any live collection head or live WAL state.
fn storage_replacement_flush_detaches_reclaimed_region_from_live_state() {
    let (_, _, storage, first_region, second_region) =
        replace_map_into_pending_reclaim_with_empty_free_list();

    assert_eq!(storage.collections()[0].basis(), StartupCollectionBasis::Region(second_region));
    assert_eq!(storage.pending_reclaims(), &[first_region]);
}

#[test]
//= spec/ring.md#region-reclaim
//# `RING-REGION-RECLAIM-PRE-003` `r` MUST NOT already be reachable from the free-list chain, unless this procedure is being re-entered during crash recovery.
fn storage_replacement_flush_keeps_detached_region_out_of_free_list_chain() {
    let (flash, _, storage, first_region, _) =
        replace_map_into_pending_reclaim_with_empty_free_list();

    let chain = free_list_chain::<512, 3, 2048, 8>(&flash, 0xff, storage.last_free_list_head());
    assert!(!chain.contains(&first_region));
}

#[test]
//= spec/ring.md#region-reclaim
//# `RING-REGION-RECLAIM-SEM-003` If `t_prev = none`, reclaim MUST NOT write any predecessor link and MUST durably append `free_list_head(r)` and set `free_list_head = r` and `free_list_tail = r`.
fn storage_reopen_after_replacement_recovers_singleton_free_list_for_reclaimed_region() {
    let (flash, _, storage, reopened, first_region, _) = replace_map_and_reopen_empty_free_list();

    assert_eq!(storage.last_free_list_head(), None);
    assert_eq!(storage.free_list_tail(), None);
    assert_eq!(reopened.last_free_list_head(), Some(first_region));
    assert_eq!(reopened.free_list_tail(), Some(first_region));

    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let footer =
        FreePointerFooter::decode(&flash.region_bytes(first_region).unwrap()[footer_offset..], 0xff).unwrap();
    assert_eq!(footer.next_tail, None);
}

#[test]
//= spec/ring.md#region-reclaim
//# In particular,
//# `r.free_pointer.next_tail` MUST still be uninitialized when `r` is
//# about to become the new free-list tail.
fn storage_reopen_after_replacement_leaves_new_free_list_tail_uninitialized() {
    let (flash, _, _, _, first_region, _) = replace_map_and_reopen_empty_free_list();

    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let footer =
        FreePointerFooter::decode(&flash.region_bytes(first_region).unwrap()[footer_offset..], 0xff).unwrap();
    assert_eq!(footer.next_tail, None);
}

#[test]
//= spec/ring.md#region-reclaim
//# `RING-REGION-RECLAIM-ORDER-005` The reclaim procedure MUST be idempotent across crashes between any two steps above.
fn storage_reopen_after_replacement_recovers_reclaim_idempotently() {
    let (mut flash, mut workspace, _, reopened_once, _, _) =
        replace_map_and_reopen_empty_free_list();

    let reopened_twice = Storage::<8, 4>::open::<512, 3, _>(&mut flash, &mut workspace).unwrap();

    assert_eq!(reopened_twice.collections(), reopened_once.collections());
    assert_eq!(reopened_twice.last_free_list_head(), reopened_once.last_free_list_head());
    assert_eq!(reopened_twice.free_list_tail(), reopened_once.free_list_tail());
    assert_eq!(reopened_twice.pending_reclaims(), reopened_once.pending_reclaims());
    assert_eq!(reopened_twice.ready_region(), reopened_once.ready_region());
}

#[test]
//= spec/ring.md#core-requirements
//# `RING-CORE-014` If reclaim cannot restore at least
//# `min_free_regions` free regions, the database MUST treat ordinary
//# writes as out of space until space is freed or the store is migrated.
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

    let reclaimed_head = rotate_wal_tail_for_collection(
        &mut storage,
        &mut flash,
        &mut workspace,
        CollectionId(130),
    );
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

#[test]
//= spec/ring.md#region-reclaim
//# `RING-REGION-RECLAIM-POST-001` The free-list chain MUST remain acyclic and FIFO-ordered.
//= spec/ring.md#region-reclaim
//# `RING-REGION-RECLAIM-POST-005` If a prior tail existed, replay of free pointers MUST follow
//# `... -> t_prev -> r`, and `r` is recognized as the tail because its
//# free-pointer slot is uninitialized.
fn storage_complete_pending_reclaim_returns_old_map_region_to_free_list() {
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
    let previous_chain = free_list_chain::<512, 5, 2048, 8>(&flash, 0xff, storage.last_free_list_head());
    let previous_tail = storage.free_list_tail().unwrap();

    storage
        .complete_pending_reclaim::<512, 5, _>(&mut flash, &mut workspace, first_region)
        .unwrap();

    assert!(storage.pending_reclaims().is_empty());
    assert_eq!(storage.free_list_tail(), Some(first_region));
    let new_chain = free_list_chain::<512, 5, 2048, 8>(&flash, 0xff, storage.last_free_list_head());
    assert_eq!(&new_chain[..previous_chain.len()], previous_chain.as_slice());
    assert_eq!(new_chain.last().copied(), Some(first_region));
    let footer_offset = 512 - FreePointerFooter::ENCODED_LEN;
    let previous_tail_footer =
        FreePointerFooter::decode(&flash.region_bytes(previous_tail).unwrap()[footer_offset..], 0xff).unwrap();
    assert_eq!(previous_tail_footer.next_tail, Some(first_region));
    let reclaimed_footer =
        FreePointerFooter::decode(&flash.region_bytes(first_region).unwrap()[footer_offset..], 0xff).unwrap();
    assert_eq!(reclaimed_footer.next_tail, None);

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert!(reopened.pending_reclaims().is_empty());
    assert_eq!(reopened.free_list_tail(), Some(first_region));
    let reopened_chain =
        free_list_chain::<512, 5, 2048, 8>(&flash, 0xff, reopened.last_free_list_head());
    assert_eq!(reopened_chain.as_slice(), new_chain.as_slice());
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
    assert_eq!(reopened.collections()[0].basis(), StartupCollectionBasis::Region(first_region));
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
    assert_eq!(storage.collections()[0].basis(), StartupCollectionBasis::Dropped);
    assert_eq!(storage.pending_reclaims(), &[region_index]);
    assert_eq!(storage.tracked_user_collection_count(), 0);

    let reopened = Storage::<8, 4>::open::<512, 5, _>(&mut flash, &mut workspace).unwrap();
    assert_eq!(reopened.collections()[0].basis(), StartupCollectionBasis::Dropped);
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
    assert_eq!(reopened.collections()[0].basis(), StartupCollectionBasis::Region(region_index));
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
    assert_eq!(storage.collections()[0].basis(), StartupCollectionBasis::Dropped);
    assert!(storage.pending_reclaims().is_empty());
}
