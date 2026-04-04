use super::*;
use ::core::pin::pin;
use ::core::task::Poll;
use std::cell::Cell;
use std::rc::Rc;

struct ObservedFlash<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> {
    inner: MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    call_count: Rc<Cell<usize>>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize>
    ObservedFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn new(erased_byte: u8, call_count: Rc<Cell<usize>>) -> Self {
        Self {
            inner: MockFlash::new(erased_byte),
            call_count,
        }
    }

    fn note_call(&self) {
        self.call_count
            .set(self.call_count.get().checked_add(1).unwrap());
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize, const MAX_LOG: usize> FlashIo
    for ObservedFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, MockError> {
        self.note_call();
        self.inner.read_metadata()
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), MockError> {
        self.note_call();
        self.inner.write_metadata(metadata)
    }

    fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), MockError> {
        self.note_call();
        self.inner.read_region(region_index, offset, buffer)
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), MockError> {
        self.note_call();
        self.inner.write_region(region_index, offset, data)
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), MockError> {
        self.note_call();
        self.inner.erase_region(region_index)
    }

    fn sync(&mut self) -> Result<(), MockError> {
        self.note_call();
        self.inner.sync()
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, MockFormatError> {
        self.note_call();
        self.inner
            .format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)
    }
}

//= spec/implementation.md#execution-requirements
//# `RING-IMPL-EXEC-001` Every fallible storage operation that may
//# require one or more device interactions MUST be expressible as a
//# single future.
#[test]
fn fallible_storage_operations_are_expressible_as_single_futures() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let op_future = strip_comment_lines(&read_repo_file("src/op_future.rs"));

    for signature in [
        "pub fn format_future<",
        "pub fn open_future<'a",
        "pub fn reclaim_wal_head_future<",
        "pub fn create_map_future<",
        "pub fn snapshot_map_future<",
        "pub fn append_map_update_future<",
        "pub fn flush_map_future<",
        "pub fn drop_map_future<",
    ] {
        assert!(
            lib.contains(signature),
            "missing single-future entry point {signature}"
        );
    }

    assert!(op_future.contains("pub struct RunOnce<F>"));
    assert!(op_future.contains("pub struct OpenStorageFuture<"));
    assert!(op_future.contains("pub struct ReclaimWalHeadFuture<"));
    assert!(op_future.contains("pub struct FlushMapFuture<"));

    for constructor in [
        "run_once(move || {",
        "self.create_map::<REGION_SIZE, REGION_COUNT, IO>(",
        "self.snapshot_map::<REGION_SIZE, REGION_COUNT, IO, K, V, MAX_INDEXES>(",
        "self.append_map_update::<REGION_SIZE, REGION_COUNT, IO, K, V, MAX_INDEXES>(",
        "self.drop_map::<REGION_SIZE, REGION_COUNT, IO>(",
        "OpenStorageFuture::<",
        "ReclaimWalHeadFuture::<",
        "FlushMapFuture::<",
    ] {
        assert!(
            lib.contains(constructor),
            "missing single-future construction pattern {constructor}"
        );
    }
}

//= spec/implementation.md#execution-requirements
//# `RING-IMPL-EXEC-002` Borromean futures MUST make progress only when
//# polled by the caller and when the caller-provided I/O object becomes
//# ready; they MUST NOT rely on background tasks internal to borromean.
#[test]
fn operation_futures_advance_only_when_polled_and_without_internal_runtime_hooks() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 4;

    let call_count = Rc::new(Cell::new(0usize));
    let mut flash = ObservedFlash::<REGION_SIZE, REGION_COUNT, 256>::new(0xff, call_count.clone());
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, 1, 8, 0xa5)
        .unwrap();
    call_count.set(0);

    {
        let future =
            Storage::<8, 4>::open_future::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace);
        let mut future = pin!(future);

        assert_eq!(call_count.get(), 0);
        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
        let after_first_poll = call_count.get();
        assert!(after_first_poll > 0);
        assert_eq!(call_count.get(), after_first_poll);
    }

    let op_future = strip_comment_lines(&read_repo_file("src/op_future.rs"));
    for banned in [
        "tokio::spawn",
        "async_std::task::spawn",
        "thread::spawn",
        "register_waker",
        "wake_by_ref",
        "callback",
        "interrupt",
        "dma",
    ] {
        assert!(
            !op_future.contains(banned),
            "operation futures unexpectedly reference runtime hook {banned}"
        );
    }
}

//= spec/implementation.md#execution-requirements
//# `RING-IMPL-EXEC-003` A simple single-threaded poll-to-completion
//# executor MUST be sufficient to drive any borromean operation future
//# to completion.
#[test]
fn single_threaded_poll_loop_drives_operation_futures_to_completion() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 5;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = super::super::poll_ready(Storage::<8, 4>::format_future::<
        REGION_SIZE,
        REGION_COUNT,
        _,
    >(
        &mut flash,
        &mut workspace,
        1,
        8,
        0xa5,
    ))
    .unwrap();

    super::super::poll_ready(storage.create_map_future::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        CollectionId(81),
    ))
    .unwrap();

    let committed_region = {
        let mut map_buffer = [0u8; REGION_SIZE];
        let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(81), &mut map_buffer).unwrap();
        map.set(7, 70).unwrap();
        super::super::poll_until_ready(
            storage.flush_map_future::<REGION_SIZE, REGION_COUNT, _, _, _, 8>(
                &mut flash,
                &mut workspace,
                &map,
            ),
            4,
        )
        .unwrap()
    };
    assert_eq!(
        storage.collections()[0].basis(),
        crate::StartupCollectionBasis::Region(committed_region)
    );

    drop(storage);

    let reopened = super::super::poll_until_ready(
        Storage::<8, 4>::open_future::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace),
        8,
    )
    .unwrap();

    let mut reopened_map_buffer = [0u8; REGION_SIZE];
    let reopened_map = reopened
        .open_map::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(81),
            &mut reopened_map_buffer,
        )
        .unwrap();
    assert_eq!(reopened_map.get(&7).unwrap(), Some(70));
}

//= spec/implementation.md#execution-requirements
//# `RING-IMPL-EXEC-004` Borromean operations on a given `Storage`
//# instance MUST require exclusive mutable access to that instance
//# unless and until a separate concurrency specification defines
//# stronger sharing rules.
#[test]
fn operation_futures_require_exclusive_mutable_storage_access() {
    let lib = strip_comment_lines(&read_repo_file("src/lib.rs"));
    let op_future = strip_comment_lines(&read_repo_file("src/op_future.rs"));

    for signature in [
        "pub fn reclaim_wal_head_future<",
        "pub fn create_map_future<",
        "pub fn snapshot_map_future<",
        "pub fn append_map_update_future<",
        "pub fn flush_map_future<",
        "pub fn drop_map_future<",
    ] {
        assert!(lib.contains(signature), "missing mutably-borrowed entry point {signature}");
    }

    assert!(lib.contains("&'a mut self,"));
    assert!(lib.contains("flash: &'a mut IO"));
    assert!(lib.contains("workspace: &'a mut StorageWorkspace<REGION_SIZE>"));
    assert!(
        op_future.contains("storage: &'a mut Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>")
    );

    for banned in [
        "&'a Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>",
        "Arc<Storage",
        "Rc<Storage",
        "RefCell<Storage",
        "Mutex<Storage",
    ] {
        assert!(
            !lib.contains(banned) && !op_future.contains(banned),
            "unexpected shared-storage handle {banned}"
        );
    }
}
