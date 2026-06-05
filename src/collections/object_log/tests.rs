use super::*;

use crate::{CollectionId, MockFlash, Storage, StorageFormatConfig};

fn assert_get<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const ROOT_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, ROOT_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
    expected: &[u8],
) {
    let mut scratch = [0u8; 64];
    let len = log
        .get(storage, handle, &mut scratch, |bytes| {
            assert_eq!(bytes, expected);
            bytes.len()
        })
        .unwrap();
    assert_eq!(len, expected.len());
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-001` Appending an object MUST return an
//# opaque `ObjectLogHandle` that names the reserved final data-region frame,
//# and reopening the collection MUST
//# reconstruct unflushed frontier objects from retained WAL updates.
#[test]
fn requirement_object_log_replays_unflushed_frontier_from_wal_updates() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, handle) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();
        let handle = log.append(&mut storage, b"alpha").unwrap();

        assert_get(&log, &mut storage, handle, b"alpha");
        (log.collection_id(), handle)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    assert_get(&reopened_log, &mut reopened, handle, b"alpha");
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-002` Flushing an object-log frontier MUST write the
//# frontier bytes into the previously reserved physical data region, persist
//# metadata sufficient to read flushed handles after reopen, and assign a
//# new sequence to a later reserved frontier region.
#[test]
fn requirement_object_log_handles_survive_flush_and_reopen() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, first, second) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();

        let first = log.append(&mut storage, b"alpha").unwrap();
        log.flush(&mut storage).unwrap();
        assert_get(&log, &mut storage, first, b"alpha");

        let second = log.append(&mut storage, b"beta").unwrap();
        assert_ne!(first.region_index, second.region_index);
        assert_ne!(first.sequence, second.sequence);
        assert_get(&log, &mut storage, second, b"beta");
        (log.collection_id(), first, second)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    assert_get(&reopened_log, &mut reopened, first, b"alpha");
    assert_get(&reopened_log, &mut reopened, second, b"beta");
}

//= spec/object-log.md#truncation
//= type=test
//# `RING-OBJECT-003` Truncating an object log MUST accept a live
//# `ObjectLogHandle` as an exclusive boundary, invalidate handles before that
//# boundary while retaining the boundary handle, and return fully obsolete data
//# regions to Borromean storage.
#[test]
fn requirement_object_log_truncate_before_handle_retains_boundary_handle() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();

    let first = log.append(&mut storage, b"alpha").unwrap();
    log.flush(&mut storage).unwrap();
    let second = log.append(&mut storage, b"beta").unwrap();
    let previous_tail = storage.free_list_tail();

    log.truncate_before(&mut storage, second).unwrap();

    let mut scratch = [0u8; 64];
    assert!(matches!(
        log.get(&mut storage, first, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert_get(&log, &mut storage, second, b"beta");
    assert_ne!(storage.free_list_tail(), previous_tail);
    assert_eq!(storage.free_list_tail(), Some(first.region_index));

    let third = log.append(&mut storage, b"gamma").unwrap();
    assert_get(&log, &mut storage, third, b"gamma");
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-004` Object-log root metadata MUST be persisted through
//# WAL state and restored when the collection is reopened.
#[test]
fn requirement_object_log_root_metadata_reopens_from_wal() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let collection_id = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();
        log.set_root(&mut storage, b"root").unwrap();
        assert_eq!(log.get_root(|bytes| bytes.len()), Some(4));
        log.collection_id()
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    assert_eq!(
        reopened_log.get_root(|bytes| {
            assert_eq!(bytes, b"root");
            bytes.len()
        }),
        Some(4)
    );
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-005` `ObjectLogHandle` MUST NOT expose public field
//# access or an unchecked public field constructor, and object-log reads MUST
//# reject handles that do not name a live reserved frame.
#[test]
fn requirement_object_log_rejects_forged_handles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();
    let handle = log.append(&mut storage, b"alpha").unwrap();
    let forged = ObjectLogHandle::new(
        handle.region_index,
        handle.sequence.wrapping_add(1),
        handle.offset,
    );

    let mut scratch = [0u8; 64];
    assert!(matches!(
        log.get(&mut storage, forged, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-011` The durable object-log handle encoding MUST be
//# exactly 12 bytes with no padding: bytes 0 through 3 contain
//# `region_index` as a little-endian `u32`, bytes 4 through 7 contain
//# `sequence` as a little-endian `u32`, and bytes 8 through 11 contain
//# `offset` as a little-endian `u32`.
#[test]
fn requirement_object_log_handle_encoding_is_fixed_little_endian_layout() {
    let handle = ObjectLogHandle::new(0x0102_0304, 0x1112_1314, 0x2122_2324);
    let mut encoded = [0u8; HANDLE_ENCODED_LEN];

    assert_eq!(HANDLE_ENCODED_LEN, 12);
    assert_eq!(
        write_handle(&mut encoded, 0, handle).unwrap(),
        HANDLE_ENCODED_LEN
    );
    assert_eq!(
        encoded,
        [0x04, 0x03, 0x02, 0x01, 0x14, 0x13, 0x12, 0x11, 0x24, 0x23, 0x22, 0x21,]
    );

    let mut offset = 0usize;
    assert_eq!(read_handle(&encoded, &mut offset).unwrap(), handle);
    assert_eq!(offset, HANDLE_ENCODED_LEN);
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-006` Opening an object-log collection by id MUST fail
//# if the live collection exists with a non-object-log collection type.
#[test]
fn requirement_object_log_open_rejects_non_object_log_collection() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    storage.create_map(CollectionId(22)).unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    assert!(matches!(
        ObjectLog::open(CollectionId(22), &mut storage, &mut memory),
        Err(ObjectLogError::CollectionTypeMismatch { .. })
    ));
}

//= spec/object-log.md#live-traversal
//= type=test
//# `RING-OBJECT-007` Object-log traversal MUST provide a way to obtain
//# the first live `ObjectLogHandle` and a way to obtain the next live
//# `ObjectLogHandle` after a provided live handle. Empty logs and tail handles
//# MUST return no handle, while handles outside the current live log MUST be
//# rejected as invalid.
#[test]
fn requirement_object_log_traverses_live_handles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();

    assert_eq!(log.first_handle(), None);

    let first = log.append(&mut storage, b"alpha").unwrap();
    log.flush(&mut storage).unwrap();
    let second = log.append(&mut storage, b"beta").unwrap();
    let third = log.append(&mut storage, b"gamma").unwrap();

    assert_eq!(log.first_handle(), Some(first));
    assert_eq!(log.next_handle(&mut storage, first).unwrap(), Some(second));
    assert_eq!(log.next_handle(&mut storage, second).unwrap(), Some(third));
    assert_eq!(log.next_handle(&mut storage, third).unwrap(), None);

    log.truncate_before(&mut storage, second).unwrap();
    assert_eq!(log.first_handle(), Some(second));
    assert!(matches!(
        log.next_handle(&mut storage, first),
        Err(ObjectLogError::InvalidHandle)
    ));
}

//= spec/object-log.md#committed-visibility
//= type=test
//# `RING-OBJECT-008` Object-log reads, traversal, and truncation MUST
//# observe only committed object bounds.
#[test]
fn requirement_object_log_failed_transaction_does_not_publish_planned_handles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();
    let mut planned = None;

    let result: Result<(), ObjectLogError> = log.transaction(&mut storage, |tx| {
        let handle = tx.append(b"staged")?;
        planned = Some(handle);
        Err(ObjectLogError::InvalidHandle)
    });
    assert!(matches!(result, Err(ObjectLogError::InvalidHandle)));

    let planned = planned.unwrap();
    let mut scratch = [0u8; 64];
    assert_eq!(log.first_handle(), None);
    assert!(matches!(
        log.get(&mut storage, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert!(matches!(
        log.truncate_before(&mut storage, planned),
        Err(ObjectLogError::InvalidHandle)
    ));

    let committed = log.append(&mut storage, b"committed").unwrap();
    log.checkpoint_append_state().unwrap();
    storage
        .memory
        .state
        .begin_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
            storage.backing,
            &mut storage.memory.workspace,
            log.collection_id(),
        )
        .unwrap();
    let mut allocated_regions = Vec::<u32, REGION_COUNT>::new();
    let planned = log
        .append_transactional(&mut storage, b"planned", &mut allocated_regions)
        .unwrap();

    assert_eq!(log.first_handle(), Some(committed));
    assert_eq!(log.next_handle(&mut storage, committed).unwrap(), None);
    assert!(matches!(
        log.get(&mut storage, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert!(matches!(
        log.truncate_before(&mut storage, planned),
        Err(ObjectLogError::InvalidHandle)
    ));
    log.rollback_transaction(&mut storage, allocated_regions)
        .unwrap();
}

//= spec/object-log.md#append-transactions
//= type=test
//# `RING-OBJECT-009` Scoped append transactions MUST keep appended
//# objects invisible until the durable commit record.
#[test]
fn requirement_object_log_committed_transaction_publishes_handles() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();
    let first = log.append(&mut storage, b"before").unwrap();
    let alpha = [0x11u8; 200];
    let beta = [0x22u8; 200];
    let gamma = [0x33u8; 200];

    let (second, third, fourth) = log
        .transaction(&mut storage, |tx| {
            let second = tx.append(&alpha)?;
            let third = tx.append(&beta)?;
            let fourth = tx.append(&gamma)?;
            Ok((second, third, fourth))
        })
        .unwrap();

    assert_eq!(first.region_index, second.region_index);
    assert_eq!(second.region_index, third.region_index);
    assert_ne!(third.region_index, fourth.region_index);
    assert_eq!(log.first_handle(), Some(first));
    assert_eq!(log.next_handle(&mut storage, first).unwrap(), Some(second));
    assert_eq!(log.next_handle(&mut storage, second).unwrap(), Some(third));
    assert_eq!(log.next_handle(&mut storage, third).unwrap(), Some(fourth));
    assert_eq!(log.next_handle(&mut storage, fourth).unwrap(), None);
    assert_get(&log, &mut storage, first, b"before");
    let mut scratch = [0u8; 256];
    assert_eq!(
        log.get(&mut storage, second, &mut scratch, |bytes| {
            assert_eq!(bytes, alpha.as_slice());
            bytes.len()
        })
        .unwrap(),
        alpha.len()
    );
    assert_eq!(
        log.get(&mut storage, third, &mut scratch, |bytes| {
            assert_eq!(bytes, beta.as_slice());
            bytes.len()
        })
        .unwrap(),
        beta.len()
    );
    assert_eq!(
        log.get(&mut storage, fourth, &mut scratch, |bytes| {
            assert_eq!(bytes, gamma.as_slice());
            bytes.len()
        })
        .unwrap(),
        gamma.len()
    );
}

//= spec/object-log.md#append-transactions
//= type=test
//# `RING-OBJECT-010` Failed or uncommitted append transactions MUST roll
//# back cleanly by discarding staged object-log state and returning
//# transaction-reserved regions to storage without making planned handles live.
#[test]
fn requirement_object_log_failed_transaction_rolls_back_allocations() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 10;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();
    let mut planned = None;

    let result: Result<(), ObjectLogError> = log.transaction(&mut storage, |tx| {
        let handle = tx.append(b"staged")?;
        planned = Some(handle);
        Err(ObjectLogError::InvalidHandle)
    });
    assert!(matches!(result, Err(ObjectLogError::InvalidHandle)));

    let planned = planned.unwrap();
    assert_eq!(storage.free_list_tail(), Some(planned.region_index));
    let committed = log.append(&mut storage, b"committed").unwrap();
    let mut scratch = [0u8; 64];
    assert!(matches!(
        log.get(&mut storage, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert_get(&log, &mut storage, committed, b"committed");

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, planned) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory).unwrap();
        let collection_id = log.collection_id();
        storage
            .memory
            .state
            .begin_collection_transaction::<REGION_SIZE, REGION_COUNT, _>(
                storage.backing,
                &mut storage.memory.workspace,
                collection_id,
            )
            .unwrap();
        let mut allocated_regions = Vec::<u32, REGION_COUNT>::new();
        let planned = log
            .append_transactional_new_region(&mut storage, b"staged", &mut allocated_regions)
            .unwrap();
        (collection_id, planned)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    let mut scratch = [0u8; 64];
    assert_eq!(reopened_log.first_handle(), None);
    assert!(matches!(
        reopened_log.get(&mut reopened, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
    assert_eq!(reopened.free_list_tail(), Some(planned.region_index));
}
