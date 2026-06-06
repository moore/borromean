use super::*;

use crate::{CollectionId, MockFlash, Storage, StorageFormatConfig};
use std::format;

const LOG_METADATA: &[u8] = b"log-meta";

fn assert_get<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
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

fn assert_get_range<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
    offset: u64,
    expected: &[u8],
) {
    let mut scratch = [0u8; 64];
    let len = log
        .get_range(
            storage,
            handle,
            offset,
            expected.len() as u64,
            &mut scratch,
            |bytes| {
                assert_eq!(bytes, expected);
                bytes.len()
            },
        )
        .unwrap();
    assert_eq!(len, expected.len());
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn assert_region_log_metadata<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_LOG: usize,
>(
    flash: &MockFlash<REGION_SIZE, REGION_COUNT, MAX_LOG>,
    handle: ObjectLogHandle,
    expected: &[u8],
) {
    let region = flash.region_bytes(handle.region_index).unwrap();
    let prologue = &region[Header::ENCODED_LEN..];

    assert_eq!(&prologue[..4], DATA_MAGIC.as_slice());
    assert_eq!(read_u16_at(prologue, 4), DATA_VERSION);
    assert_eq!(read_u64_at(prologue, 6), handle.sequence);
    assert_eq!(read_u32_at(prologue, 14), expected.len() as u32);
    assert_eq!(
        &prologue[DATA_PROLOGUE_FIXED_LEN..DATA_PROLOGUE_FIXED_LEN + expected.len()],
        expected
    );
    assert_eq!(
        usize::try_from(handle.offset).unwrap(),
        Header::ENCODED_LEN + DATA_PROLOGUE_FIXED_LEN + expected.len()
    );
}

fn fill_pattern(bytes: &mut [u8]) {
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
}

fn assert_get_bytes<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
    expected: &[u8],
    scratch: &mut [u8],
) {
    let len = log
        .get(storage, handle, scratch, |bytes| {
            assert_eq!(bytes, expected);
            bytes.len()
        })
        .unwrap();
    assert_eq!(len, expected.len());
}

fn record_info_for<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
) -> (ObjectLogRegion, ObjectLogRecordInfo) {
    let region = log.region_for_handle(handle).unwrap();
    let record = log.read_record_info(storage, region, handle).unwrap();
    (region, record)
}

fn object_end_for<
    IO: FlashIo,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const MAX_COLLECTIONS: usize,
    const MAX_REGIONS: usize,
    const LOG_METADATA_MAX: usize,
>(
    log: &ObjectLog<'_, REGION_SIZE, MAX_REGIONS, LOG_METADATA_MAX>,
    storage: &mut Storage<'_, '_, IO, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS>,
    handle: ObjectLogHandle,
) -> ObjectEndInfo {
    let (region, record) = record_info_for(log, storage, handle);
    log.read_object_end(storage, region, handle, record)
        .unwrap()
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-001` Appending an object MUST return an
//# opaque `ObjectLogHandle` that names a committed object record, and reopening
//# the collection MUST reconstruct unflushed frontier objects from retained WAL
//# updates.
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
        let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
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

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-015` Object-log range reads MUST accept `u64`
//# object-relative offset and length values, return only that committed byte
//# range, reject ranges outside the object, and require only enough caller
//# scratch for the requested range.
#[test]
fn requirement_object_log_range_reads_return_requested_subrange() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;
    const OBJECT: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
    let handle = log.append(&mut storage, OBJECT).unwrap();

    assert_get_range(&log, &mut storage, handle, 2, b"cdefg");
    let mut empty_scratch = [];
    assert_eq!(
        log.get_range(
            &mut storage,
            handle,
            OBJECT.len() as u64,
            0,
            &mut empty_scratch,
            |bytes| bytes.len(),
        )
        .unwrap(),
        0
    );

    let mut short_scratch = [0u8; 2];
    assert!(matches!(
        log.get_range(&mut storage, handle, 2, 3, &mut short_scratch, |_| ()),
        Err(ObjectLogError::BufferTooSmall {
            needed: 3,
            available: 2
        })
    ));
    let mut scratch = [0u8; 8];
    assert!(matches!(
        log.get_range(
            &mut storage,
            handle,
            (OBJECT.len() - 1) as u64,
            2,
            &mut scratch,
            |_| ()
        ),
        Err(ObjectLogError::ObjectRangeOutOfBounds {
            offset,
            len: 2,
            object_len
        }) if offset == (OBJECT.len() - 1) as u64 && object_len == OBJECT.len() as u64
    ));

    log.flush(&mut storage).unwrap();
    assert_get_range(&log, &mut storage, handle, 10, b"klmn");
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-016` Object-log whole-object reads MUST fail with a
//# buffer-too-small error that reports the stored object length when caller
//# scratch cannot hold the full object, and object-log length queries MUST return
//# the stored `u64` object length without returning object bytes.
#[test]
fn requirement_object_log_reports_object_len_and_full_read_buffer_size() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 8;
    const OBJECT: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
    let handle = log.append(&mut storage, OBJECT).unwrap();

    assert_eq!(
        log.get_object_len(&mut storage, handle).unwrap(),
        OBJECT.len() as u64
    );

    let mut short_scratch = [0u8; 8];
    assert!(matches!(
        log.get(&mut storage, handle, &mut short_scratch, |_| ()),
        Err(ObjectLogError::BufferTooSmall {
            needed,
            available: 8
        }) if needed == OBJECT.len()
    ));

    log.flush(&mut storage).unwrap();

    assert_eq!(
        log.get_object_len(&mut storage, handle).unwrap(),
        OBJECT.len() as u64
    );
    assert!(matches!(
        log.get(&mut storage, handle, &mut short_scratch, |_| ()),
        Err(ObjectLogError::BufferTooSmall {
            needed,
            available: 8
        }) if needed == OBJECT.len()
    ));
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-006` Flushing an object-log frontier MUST write the
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
        let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();

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
//# `RING-OBJECT-010` Truncating an object log MUST accept a live
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
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();

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
//# `RING-OBJECT-007` Object-log metadata MUST be a non-empty immutable
//# opaque byte sequence supplied at collection creation, persisted with
//# collection state, restored on open, and exposed to callers without requiring
//# the caller to know it before opening the collection.
#[test]
fn requirement_object_log_metadata_is_immutable_and_reopens_from_wal() {
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

        let mut empty_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        assert!(matches!(
            ObjectLog::new(&mut storage, &mut empty_memory, b""),
            Err(ObjectLogError::LogMetadataEmpty)
        ));

        let mut small_memory = ObjectLogMemory::<REGION_SIZE, 4, 4>::new();
        assert!(matches!(
            ObjectLog::new(&mut storage, &mut small_memory, b"abcde"),
            Err(ObjectLogError::LogMetadataTooLarge {
                len: 5,
                capacity: 4
            })
        ));

        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
        assert_eq!(
            log.get_log_metadata(|bytes| {
                assert_eq!(bytes, LOG_METADATA);
                bytes.len()
            }),
            LOG_METADATA.len()
        );
        let handle = log.append(&mut storage, b"alpha").unwrap();
        assert_get(&log, &mut storage, handle, b"alpha");
        (log.collection_id(), handle)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    assert_eq!(
        reopened_log.get_log_metadata(|bytes| {
            assert_eq!(bytes, LOG_METADATA);
            bytes.len()
        }),
        LOG_METADATA.len()
    );
    assert_get(&reopened_log, &mut reopened, handle, b"alpha");
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-008` Every object-log data region MUST contain the full
//# immutable log metadata in its object-log prologue, and opening or reading a
//# flushed region MUST reject a prologue whose metadata differs from the
//# collection metadata.
#[test]
fn requirement_object_log_data_regions_carry_immutable_log_metadata() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 12;

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let (collection_id, first, second) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();

        let first = log.append(&mut storage, b"alpha").unwrap();
        log.flush(&mut storage).unwrap();
        let second = log.append(&mut storage, b"beta").unwrap();
        log.flush(&mut storage).unwrap();
        assert_get(&log, &mut storage, first, b"alpha");
        assert_get(&log, &mut storage, second, b"beta");
        (log.collection_id(), first, second)
    };

    assert_region_log_metadata(&flash, first, LOG_METADATA);
    assert_region_log_metadata(&flash, second, LOG_METADATA);

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    reopened
        .backing
        .write_region(
            first.region_index,
            Header::ENCODED_LEN + DATA_PROLOGUE_FIXED_LEN,
            b"X",
        )
        .unwrap();
    let mut scratch = [0u8; 64];
    assert!(matches!(
        reopened_log.get(&mut reopened, first, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    assert!(matches!(
        ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory),
        Err(ObjectLogError::InvalidFrame)
    ));
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-002` `ObjectLogHandle` MUST remain opaque to external
//# callers: it MUST NOT expose public field access, an unchecked public field
//# constructor, or debug formatting that reveals internal handle components.
#[test]
fn requirement_object_log_handle_public_representation_is_opaque() {
    let handle = ObjectLogHandle::new(1, 2, 3);

    assert_eq!(format!("{handle:?}"), "ObjectLogHandle { .. }");
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-005` Object-log reads MUST reject handles that do not
//# name a live reserved object record.
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
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
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
//# `RING-OBJECT-004` The durable object-log handle and `ObjectLogPointer`
//# encoding MUST be exactly 16 bytes with no padding: bytes 0 through 3 contain
//# `region_index` as a little-endian `u32`, bytes 4 through 11 contain
//# `sequence` as a little-endian `u64`, and bytes 12 through 15 contain
//# `offset` as a little-endian `u32`.
#[test]
fn requirement_object_log_handle_encoding_is_fixed_little_endian_layout() {
    let handle = ObjectLogHandle::new(0x0102_0304, 0x1112_1314_1516_1718, 0x2122_2324);
    let mut encoded = [0u8; HANDLE_ENCODED_LEN];

    assert_eq!(HANDLE_ENCODED_LEN, 16);
    assert_eq!(
        write_handle(&mut encoded, 0, handle).unwrap(),
        HANDLE_ENCODED_LEN
    );
    assert_eq!(
        encoded,
        [
            0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x24, 0x23,
            0x22, 0x21,
        ]
    );

    let mut offset = 0usize;
    assert_eq!(read_handle(&encoded, &mut offset).unwrap(), handle);
    assert_eq!(offset, HANDLE_ENCODED_LEN);
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-014` Object-log region sequences MUST be monotonic `u64`
//# values that never wrap. If replay, snapshot decode, or open observes state
//# that would require advancing past `u64::MAX`, the collection MUST be treated
//# as corrupt.
#[test]
fn requirement_object_log_sequence_overflow_is_corrupt() {
    const REGION_SIZE: usize = 512;

    let object_start =
        u32::try_from(Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap())
            .unwrap();
    let mut snapshot = [0u8; 128];
    let mut offset = 0usize;
    offset = write_bytes(&mut snapshot, offset, &SNAPSHOT_MAGIC).unwrap();
    offset = write_u16(&mut snapshot, offset, SNAPSHOT_VERSION).unwrap();
    offset = write_u16(&mut snapshot, offset, 0).unwrap();
    offset = write_u32(&mut snapshot, offset, 1).unwrap();
    offset = write_u32(&mut snapshot, offset, LOG_METADATA.len() as u32).unwrap();
    offset = write_u32(&mut snapshot, offset, 1).unwrap();
    offset = write_u64(&mut snapshot, offset, u64::MAX).unwrap();
    offset = write_u32(&mut snapshot, offset, object_start).unwrap();
    offset = write_u32(&mut snapshot, offset, object_start).unwrap();
    offset = write_u32(&mut snapshot, offset, object_start).unwrap();
    offset = write_u8(&mut snapshot, offset, 0).unwrap();
    offset = write_bytes(&mut snapshot, offset, LOG_METADATA).unwrap();

    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    assert!(matches!(
        decode_snapshot::<REGION_SIZE, 4, 16>(&snapshot[..offset], &mut memory),
        Err(ObjectLogError::InvalidEncoding)
    ));
}

//= spec/object-log.md#api-and-handles
//= type=test
//# `RING-OBJECT-003` Opening an object-log collection by id MUST fail
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
//# `RING-OBJECT-011` Object-log traversal MUST provide a way to obtain
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
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();

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
//# `RING-OBJECT-009` Object-log reads, traversal, and truncation MUST
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
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
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
//# `RING-OBJECT-012` Scoped append transactions MUST keep appended
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
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
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
//# `RING-OBJECT-013` Failed or uncommitted append transactions MUST roll
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
    let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
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
        let mut log = ObjectLog::new(&mut storage, &mut memory, b"log-meta").unwrap();
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

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-017` Object-log V1 data regions MUST encode object records
//# with the common typed-record header
//# `[record_type:u8][body_len:u32 little-endian][body_crc32c:u32
//# little-endian][body]`, MUST compute `body_crc32c` as CRC32C over `body`, and
//# MUST reject unknown record types.
#[test]
fn requirement_object_log_v1_data_regions_use_typed_record_headers() {
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
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, b"alpha").unwrap();
    log.flush(&mut storage).unwrap();

    let region = storage.backing.region_bytes(handle.region_index).unwrap();
    let offset = usize::try_from(handle.offset).unwrap();
    assert_eq!(region[offset], RECORD_INLINE_OBJECT);
    assert_eq!(read_u32_at(region, offset + 1), 5);
    assert_eq!(read_u32_at(region, offset + 5), crc32(b"alpha"));
    assert_eq!(
        &region[offset + RECORD_HEADER_LEN..offset + RECORD_HEADER_LEN + 5],
        b"alpha"
    );

    storage
        .backing
        .write_region(handle.region_index, offset + 5, &0u32.to_le_bytes())
        .unwrap();
    let mut scratch = [0u8; 16];
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 4096>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 4, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, b"beta").unwrap();
    log.flush(&mut storage).unwrap();
    storage
        .backing
        .write_region(
            handle.region_index,
            usize::try_from(handle.offset).unwrap(),
            &[0xff],
        )
        .unwrap();
    assert!(matches!(
        log.get(&mut storage, handle, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-018` Inline objects MUST be encoded as record type `0x01`
//# `InlineObject` whose body is the raw object bytes and whose public handle
//# names that record.
#[test]
fn requirement_object_log_inline_objects_use_inline_object_records() {
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
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, b"inline").unwrap();

    let (region, record) = record_info_for(&log, &mut storage, handle);
    assert!(!region.flushed);
    assert_eq!(record.record_type, RECORD_INLINE_OBJECT);
    assert_eq!(record.body_len, b"inline".len());
    assert_eq!(log.first_handle(), Some(handle));
    assert_get(&log, &mut storage, handle, b"inline");
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-019` Large-object handles MUST point to record type `0x03`
//# `ObjectEnd` records encoded as `[total_object_len:u64
//# little-endian][first:ObjectLogPointer][last:ObjectLogPointer]`.
#[test]
fn requirement_object_log_large_object_handles_point_to_end_records() {
    const REGION_SIZE: usize = 512;
    const REGION_COUNT: usize = 18;

    let mut object = [0u8; 900];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let (collection_id, handle) = {
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
            &mut flash,
            StorageFormatConfig::new(2, 8, 0xa5),
            crate::test_storage_memory(),
        )
        .unwrap();
        let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
        let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
        let handle = log.append(&mut storage, &object).unwrap();

        let (_, record) = record_info_for(&log, &mut storage, handle);
        assert_eq!(record.record_type, RECORD_OBJECT_END);
        let object_end = object_end_for(&log, &mut storage, handle);
        assert_eq!(object_end.total_object_len, object.len() as u64);
        assert_eq!(
            record_info_for(&log, &mut storage, object_end.first)
                .1
                .record_type,
            RECORD_OBJECT_CHUNK
        );
        assert_eq!(
            record_info_for(&log, &mut storage, object_end.last)
                .1
                .record_type,
            RECORD_OBJECT_CHUNK
        );

        let mut scratch = [0u8; 900];
        assert_get_bytes(&log, &mut storage, handle, &object, &mut scratch);
        assert_get_range(&log, &mut storage, handle, 140, &object[140..156]);
        assert_eq!(
            log.get_object_len(&mut storage, handle).unwrap(),
            object.len() as u64
        );
        log.flush(&mut storage).unwrap();
        (log.collection_id(), handle)
    };

    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT>::open(&mut flash, crate::test_storage_memory())
            .unwrap();
    let mut reopened_memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let reopened_log = ObjectLog::open(collection_id, &mut reopened, &mut reopened_memory).unwrap();
    let mut scratch = [0u8; 900];
    assert_get_bytes(&reopened_log, &mut reopened, handle, &object, &mut scratch);
    assert_get_range(&reopened_log, &mut reopened, handle, 200, &object[200..216]);
    assert_eq!(
        reopened_log.get_object_len(&mut reopened, handle).unwrap(),
        object.len() as u64
    );
}

//= spec/object-log.md#durability
//= type=test
//# `RING-OBJECT-020` Object chunks MUST be encoded as record type `0x02`
//# `ObjectChunk` bodies `[flags:u8][logical_start:u64
//# little-endian][chunk_len:u32
//# little-endian][prev:ObjectLogPointer][next:ObjectLogPointer][chunk_bytes]`, MUST reject nonzero
//# reserved flags, and MUST validate each chunk through its record CRC32C.
#[test]
fn requirement_object_log_chunks_encode_links_and_validate_crc() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 18;

    let mut object = [0u8; 420];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, &object).unwrap();
    let object_end = object_end_for(&log, &mut storage, handle);

    let (_, _, first_chunk) = log
        .read_chunk_info(&mut storage, object_end.first, true)
        .unwrap();
    assert_eq!(first_chunk.flags & OBJECT_CHUNK_FLAG_PREV_VALID, 0);
    assert_ne!(first_chunk.flags & OBJECT_CHUNK_FLAG_NEXT_VALID, 0);
    assert_eq!(first_chunk.logical_start, 0);
    let second = first_chunk.next;
    let (_, _, second_chunk) = log.read_chunk_info(&mut storage, second, true).unwrap();
    assert_ne!(second_chunk.flags & OBJECT_CHUNK_FLAG_PREV_VALID, 0);
    assert_eq!(second_chunk.prev, object_end.first);

    let crc_offset = usize::try_from(object_end.first.offset).unwrap() + 5;
    storage
        .backing
        .write_region(
            object_end.first.region_index,
            crc_offset,
            &0u32.to_le_bytes(),
        )
        .unwrap();
    let mut scratch = [0u8; 8];
    assert!(matches!(
        log.get_range(&mut storage, handle, 0, 8, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidFrame)
    ));
}

//= spec/object-log.md#large-objects
//= type=test
//# `RING-OBJECT-021` Large-object runs MUST use linked `ObjectChunk`
//# records with previous and next links or start and end markers rather than a
//# map-style manifest.
#[test]
fn requirement_object_log_large_object_runs_use_linked_chunks() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 20;

    let mut object = [0u8; 560];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let handle = log.append(&mut storage, &object).unwrap();
    let object_end = object_end_for(&log, &mut storage, handle);

    let mut current = object_end.first;
    let mut previous = None;
    let mut total = 0usize;
    let mut chunk_count = 0usize;
    loop {
        let (_, _, chunk) = log.read_chunk_info(&mut storage, current, true).unwrap();
        if let Some(previous) = previous {
            assert_eq!(chunk.prev, previous);
        } else {
            assert_eq!(chunk.flags & OBJECT_CHUNK_FLAG_PREV_VALID, 0);
        }
        total += chunk.chunk_len;
        chunk_count += 1;
        if chunk.flags & OBJECT_CHUNK_FLAG_NEXT_VALID == 0 {
            assert_eq!(current, object_end.last);
            break;
        }
        previous = Some(current);
        current = chunk.next;
    }
    assert!(chunk_count > 1);
    assert_eq!(total, object.len());
}

//= spec/object-log.md#large-objects
//= type=test
//# `RING-OBJECT-022` Large-object append placement MUST fill the current
//# frontier first, directly materialize full frontier images, and keep the
//# trailing partial chunk plus `ObjectEnd` record WAL-backed.
#[test]
fn requirement_object_log_large_object_append_placement_uses_frontier_first() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 24;

    let mut object = [0u8; 420];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let seed = log.append(&mut storage, b"seed").unwrap();
    let handle = log.append(&mut storage, &object).unwrap();
    let object_end = object_end_for(&log, &mut storage, handle);

    assert_eq!(object_end.first.region_index, seed.region_index);
    assert!(object_end.first.offset > seed.offset);
    assert!(log.region_for_handle(object_end.first).unwrap().flushed);
    assert!(!log.region_for_handle(handle).unwrap().flushed);

    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let payload_capacity = committed_payload_capacity::<REGION_SIZE>(storage.metadata()).unwrap();
    let max_region_end = Header::ENCODED_LEN + payload_capacity;
    let object_start = Header::ENCODED_LEN + data_prologue_len(LOG_METADATA.len()).unwrap();
    let exact_chunk_len = chunk_payload_capacity_at(object_start, max_region_end, false).unwrap();
    let mut exact = std::vec![0u8; exact_chunk_len * 2];
    fill_pattern(&mut exact);
    let exact_handle = log.append(&mut storage, &exact).unwrap();
    let exact_end = object_end_for(&log, &mut storage, exact_handle);
    assert_ne!(exact_end.last.region_index, exact_handle.region_index);
    assert!(log.region_for_handle(exact_end.last).unwrap().flushed);
    assert!(!log.region_for_handle(exact_handle).unwrap().flushed);
}

//= spec/object-log.md#large-objects
//= type=test
//# `RING-OBJECT-023` Every physical region written for a large-object run
//# MUST be transaction-reserved before write and recoverable if the transaction
//# does not commit.
#[test]
fn requirement_object_log_large_object_regions_are_transaction_reserved() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 24;

    let mut object = [0u8; 420];
    fill_pattern(&mut object);
    let mut flash = MockFlash::<REGION_SIZE, REGION_COUNT, 8192>::new(0xff);
    let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT>::format(
        &mut flash,
        StorageFormatConfig::new(2, 8, 0xa5),
        crate::test_storage_memory(),
    )
    .unwrap();
    let mut memory = ObjectLogMemory::<REGION_SIZE, 16, 16>::new();
    let mut log = ObjectLog::new(&mut storage, &mut memory, LOG_METADATA).unwrap();
    let mut planned = None;
    let result: Result<(), ObjectLogError> = log.transaction(&mut storage, |tx| {
        let handle = tx.append(&object)?;
        planned = Some(handle);
        Err(ObjectLogError::InvalidHandle)
    });
    assert!(result.is_err());
    let planned = planned.unwrap();
    assert_eq!(log.first_handle(), None);
    assert!(!log
        .memory
        .regions
        .iter()
        .any(|region| region.region_index == planned.region_index));
    let mut scratch = [0u8; 420];
    assert!(matches!(
        log.get(&mut storage, planned, &mut scratch, |_| ()),
        Err(ObjectLogError::InvalidHandle)
    ));
}
