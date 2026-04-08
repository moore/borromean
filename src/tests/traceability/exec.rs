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
//= type=test
//# `RING-IMPL-EXEC-001` Every fallible storage operation that may
//# require one or more device interactions MUST be expressible as a
//# single future.
#[test]
fn each_fallible_storage_operation_is_drivable_as_one_future() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 5;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    let mut storage = super::super::poll_ready(Storage::<8, 4>::format_future::<
        REGION_SIZE,
        REGION_COUNT,
        _,
    >(&mut flash, &mut workspace, 1, 8, 0xa5))
    .unwrap();

    super::super::poll_ready(storage.create_map_future::<REGION_SIZE, REGION_COUNT, _>(
        &mut flash,
        &mut workspace,
        CollectionId(81),
    ))
    .unwrap();

    let mut source_buffer = [0u8; REGION_SIZE];
    let mut source = LsmMap::<u16, u16, 8>::new(CollectionId(81), &mut source_buffer).unwrap();
    source.set(1, 10).unwrap();
    super::super::poll_ready(
        storage.snapshot_map_future::<REGION_SIZE, REGION_COUNT, _, _, _, 8>(
            &mut flash,
            &mut workspace,
            &source,
        ),
    )
    .unwrap();

    let mut payload_buffer = [0u8; 64];
    super::super::poll_ready(
        storage.append_map_update_future::<REGION_SIZE, REGION_COUNT, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(81),
            &MapUpdate::Set { key: 2, value: 20 },
            &mut payload_buffer,
        ),
    )
    .unwrap();

    source.set(3, 30).unwrap();
    let committed_region = super::super::poll_until_ready(
        storage.flush_map_future::<REGION_SIZE, REGION_COUNT, _, _, _, 8>(
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

    let reclaim_region =
        super::super::poll_ready(storage.drop_map_future::<REGION_SIZE, REGION_COUNT, _>(
            &mut flash,
            &mut workspace,
            CollectionId(81),
        ))
        .unwrap();
    assert_eq!(reclaim_region, Some(committed_region));

    let reopened = super::super::poll_until_ready(
        Storage::<8, 4>::open_future::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace),
        8,
    )
    .unwrap();
    assert_eq!(
        reopened.collections()[0].basis(),
        StartupCollectionBasis::Dropped
    );
}

//= spec/implementation.md#execution-requirements
//= type=test
//# `RING-IMPL-EXEC-002` Borromean futures MUST make progress only when
//# polled by the caller and when the caller-provided I/O object becomes
//# ready; they MUST NOT rely on background tasks internal to borromean.
#[test]
fn operation_futures_advance_only_when_the_caller_polls_them() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 4;

    let call_count = Rc::new(Cell::new(0usize));
    let mut flash = ObservedFlash::<REGION_SIZE, REGION_COUNT, 256>::new(0xff, call_count.clone());
    let mut workspace = StorageWorkspace::<REGION_SIZE>::new();
    Storage::<8, 4>::format::<REGION_SIZE, REGION_COUNT, _>(&mut flash, &mut workspace, 1, 8, 0xa5)
        .unwrap();
    call_count.set(0);

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

    let reopened = super::super::poll_until_ready(future.as_mut(), 8).unwrap();
    assert!(call_count.get() >= after_first_poll);
    assert_eq!(reopened.wal_head(), 0);
}

//= spec/implementation.md#execution-requirements
//= type=test
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
    >(&mut flash, &mut workspace, 1, 8, 0xa5))
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
//= type=test
//# `RING-IMPL-EXEC-004` Borromean operations on a given `Storage`
//# instance MUST require exclusive mutable access to that instance
//# unless and until a separate concurrency specification defines
//# stronger sharing rules.
#[test]
fn storage_can_be_reused_only_after_an_operation_future_is_finished_or_dropped() {
    let mut flash = MockFlash::<512, 5, 2048>::new(0xff);
    let mut workspace = StorageWorkspace::<512>::new();
    let mut storage =
        Storage::<8, 4>::format::<512, 5, _>(&mut flash, &mut workspace, 1, 8, 0xa5).unwrap();
    storage
        .create_map::<512, 5, _>(&mut flash, &mut workspace, CollectionId(82))
        .unwrap();

    let mut map_buffer = [0u8; 512];
    let mut map = LsmMap::<u16, u16, 8>::new(CollectionId(82), &mut map_buffer).unwrap();
    map.set(1, 10).unwrap();

    {
        let future =
            storage.flush_map_future::<512, 5, _, _, _, 8>(&mut flash, &mut workspace, &map);
        let mut future = pin!(future);
        assert!(matches!(
            super::super::poll_once(future.as_mut()),
            Poll::Pending
        ));
    }

    storage
        .append_map_update::<512, 5, _, u16, u16, 8>(
            &mut flash,
            &mut workspace,
            CollectionId(82),
            &MapUpdate::Set { key: 2, value: 20 },
            &mut [0u8; 64],
        )
        .unwrap();
}
