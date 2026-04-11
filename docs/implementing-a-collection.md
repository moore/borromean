# Implementing A Durable Collection

This guide is for contributors adding a new durably integrated collection type to Borromean.

Today the map collection in [`src/collections/map/mod.rs`](../src/collections/map/mod.rs) is the only complete example. The experimental channel module in [`src/collections/channel/mod.rs`](../src/collections/channel/mod.rs) is useful as a public-API example, but it is not wired into startup, WAL replay, reclaim, or committed-region handling.

The goal is to add a collection that:

- has a stable on-disk collection type code
- defines its own update, snapshot, and committed-region payload semantics
- uses the shared storage engine for WAL appends, region allocation, reclaim, and recovery
- can be reopened through replay after reset
- is covered by specs, tests, and traceability annotations

## 1. Start With A Collection-Local Module

Create a new module under `src/collections/<name>/mod.rs` and export it from [`src/collections.rs`](../src/collections.rs).

Follow the same split that the map module uses:

- collection-local error types
- caller-facing in-memory frontier type
- pure encoding and decoding helpers for update payloads
- pure encoding and decoding helpers for snapshot payloads
- pure encoding and decoding helpers for committed-region payloads
- storage adapter methods that call into the shared runtime

At minimum, a durable collection usually needs:

- a stable committed-region format constant like `MAP_REGION_V1_FORMAT`
- an empty snapshot byte sequence like `EMPTY_MAP_SNAPSHOT`
- an `open_from_storage` helper that reconstructs the frontier from replay state
- one or more helpers that turn typed operations into raw WAL payload bytes

Keep the payload logic pure and buffer-based. The shared runtime should not need to know your collection's internal encoding rules.

## 2. Reserve A Stable Collection Type Code

Add the new kind to [`src/lib.rs`](../src/lib.rs):

- add a `CollectionType` enum variant
- add a stable `*_CODE` constant
- extend `CollectionType::stable_code()`

Treat the numeric code as an on-disk contract. Once it is used in WAL records or committed region headers, do not recycle or renumber it.

If the collection is part of the public surface, also re-export its public types through [`src/collections.rs`](../src/collections.rs) and the crate root in [`src/lib.rs`](../src/lib.rs).

## 3. Keep Durability In The Shared Runtime

Do not invent a collection-specific device protocol. New collections should build on the existing helpers in [`src/storage.rs`](../src/storage.rs):

- `append_new_collection`
- `append_update`
- `append_snapshot`
- `reserve_next_region`
- `write_committed_region`
- `append_head`
- `append_reclaim_begin`
- `drop_collection_and_begin_reclaim`
- `visit_wal_records`

The usual sequences look like this:

```rust
storage.append_new_collection(..., collection_id, CollectionType::FOO_CODE)?;
```

```rust
let used = frontier.encode_update_into(payload)?;
storage.append_update(..., collection_id, &payload[..used])?;
```

```rust
let region_index = storage.reserve_next_region(...)?;
storage.write_committed_region(
    ...,
    region_index,
    collection_id,
    FOO_REGION_V1_FORMAT,
    &region_payload[..used],
)?;
if let Some(previous_region) = previous_region {
    storage.append_reclaim_begin(..., previous_region)?;
}
storage.append_head(..., collection_id, CollectionType::FOO_CODE, region_index)?;
```

That keeps WAL ordering, region allocation, reclaim, and crash recovery in one place.

## 4. Update Runtime Type Gates

The shared runtime currently whitelists supported durable user collections. You must extend those gates in [`src/storage.rs`](../src/storage.rs):

- `StorageRuntime::validate_supported_user_collection_type`
- any `match`es that currently special-case `CollectionType::MAP_CODE`

One engine assumption is easy to miss: WAL-head reclaim rewrites an empty-basis collection into a retained snapshot. That means a new durable collection needs an explicit empty snapshot representation, and [`src/storage.rs`](../src/storage.rs) must learn how to emit it from:

- `append_empty_basis_snapshot_with_rotation`
- `classify_wal_head_record_for_reclaim`

If your collection cannot express an empty basis as a snapshot payload, you will need to generalize that reclaim path before the collection can be durably supported.

## 5. Update Startup And Open Validation

Replay first reconstructs generic collection state, then `Storage::open` rejects live collection types that the build does not support. Extend both layers:

- [`src/startup.rs`](../src/startup.rs): `validate_live_collection_types`
- [`src/lib.rs`](../src/lib.rs): `Storage::validate_live_collections`

Your collection-local `open_from_storage` helper should then rebuild its frontier from `StartupCollectionBasis`:

- `Empty`: start from the collection's empty in-memory state
- `WalSnapshot`: load the retained snapshot payload, then replay later updates
- `Region(region_index)`: load the retained committed region, then replay later updates
- `Dropped`: reject the open

The map implementation in [`src/collections/map/mod.rs`](../src/collections/map/mod.rs) is the reference pattern. It scans retained WAL records with `visit_wal_records`, loads the selected basis when it appears, and applies later updates in order.

## 6. Add Storage Facade Methods

Once the runtime and collection module exist, add the user-facing `Storage` helpers in [`src/lib.rs`](../src/lib.rs). The map API is the current template:

- `create_map` and `create_map_future`
- `open_map`
- `append_map_update` or `update_map_frontier`
- `snapshot_map`
- `flush_map` and `flush_map_future`
- `drop_map`

For a new collection, keep the same ownership model:

- caller-owned `FlashIo`
- caller-owned `StorageWorkspace`
- caller-owned frontier buffers
- explicit payload staging buffers when encoding may need scratch

If the collection has operations that mutate in-memory state before the durable append finishes, add checkpoint and rollback helpers like the map's `checkpoint_into` and `restore_from_checkpoint` so failures do not leave the caller frontier half-applied.

## 7. Worked Example: An Append-Only Log Collection

A log collection is a useful tutorial example because it looks simple at the API level but immediately exercises the storage architecture in a different way than the map does.

The target behavior is:

- append log records in order
- read records back in order
- truncate exactly at a record boundary
- retain any number of records over time

For a text-oriented API, treat each record as one UTF-8 log line without a trailing newline. On disk, it is still better to store records as length-delimited bytes rather than relying on newline parsing.

### Public API Sketch

The collection-facing API could look like this:

```rust
pub struct LogRecordId(u64);

pub struct DurableLog<'a> {
    // caller-owned buffers plus collection-local indexing state
}

impl<const MAX_COLLECTIONS: usize, const MAX_PENDING_RECLAIMS: usize>
    Storage<MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>
{
    pub fn create_log<const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
    ) -> Result<(), StorageRuntimeError>;

    pub fn open_log<'a, const REGION_SIZE: usize, const REGION_COUNT: usize, IO: FlashIo>(
        &self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        buffer: &'a mut [u8],
    ) -> Result<DurableLog<'a>, LogStorageError>;

    pub fn append_log_entry<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        entry: &[u8],
        payload_buffer: &mut [u8],
    ) -> Result<LogRecordId, LogStorageError>;

    pub fn truncate_log_after<
        const REGION_SIZE: usize,
        const REGION_COUNT: usize,
        IO: FlashIo,
    >(
        &mut self,
        flash: &mut IO,
        workspace: &mut StorageWorkspace<REGION_SIZE>,
        collection_id: CollectionId,
        last_kept: Option<LogRecordId>,
        payload_buffer: &mut [u8],
    ) -> Result<(), LogStorageError>;
}

impl<'a> DurableLog<'a> {
    pub fn record_count(&self) -> u64;
    pub fn first_record_id(&self) -> Option<LogRecordId>;
    pub fn last_record_id(&self) -> Option<LogRecordId>;
    pub fn read_record(&self, id: LogRecordId) -> Result<Option<&[u8]>, LogError>;
    pub fn iter_from(
        &self,
        start: LogRecordId,
    ) -> impl Iterator<Item = Result<LogRecordView<'_>, LogError>> + '_;
}
```

If you want a stricter line-oriented API, make `append_log_entry` validate UTF-8 and reject embedded newlines. If you want a generic binary record log, keep the API byte-oriented and let higher layers interpret the payload.

### Update Model

The WAL updates for a log collection are naturally:

- `Append { entry }`
- `TruncateAfter { last_kept }`

Those are collection-local payload semantics encoded above `append_update`.

`TruncateAfter { last_kept: None }` means "truncate to empty." Using a record id instead of a byte offset is important because it makes truncation land on a record boundary by construction.

### Read Model

Do not make callers reconstruct the whole log from raw payload bytes. The opened frontier should provide:

- direct access to counts and first or last ids
- random read by `LogRecordId` when the record is still indexed in memory
- sequential scan from a chosen record id

For large logs, the most practical API is usually sequential reading from a cursor or record id. Random reads are fine as a convenience if the frontier keeps enough index state, but a streaming iterator is the core read path.

### Why A Log Collection Pushes The Current Runtime

The current engine stores one retained committed-region basis per collection through `StartupCollectionBasis::Region(u32)`. That is enough for the map, because the whole compacted basis fits in one region.

An unbounded log is different. A single committed region cannot hold any number of records, so a true durable log needs a multi-region basis. That means the tutorial for a log collection should call out a required shared-runtime extension before the collection itself is wired in:

- replay state must be able to retain more than one committed region for a live collection
- live-state reachability must understand that all regions in the retained log basis are still live
- reclaim must only detach segments that are no longer reachable after append or truncate
- open must be able to recover the retained segment set before replaying later WAL updates

The important design rule does not change: make this a shared storage-engine feature, not a log-only side protocol. A log collection should use a generic retained-segment mechanism added to the runtime rather than quietly managing extra live regions behind the runtime's back.

### Segment Layout

A reasonable committed-region design is "one immutable log segment per region":

- segment header with first and last `LogRecordId`
- entry count
- entry-offset index
- packed entry bytes

Each segment contains only complete records. Appends go to the in-memory frontier and WAL first. Flush compacts a bounded range of records into a new immutable segment. The retained durable basis then becomes an ordered list of segments plus any later WAL updates.

With that model:

- append adds records after the current last id
- truncate detaches whole tail segments when possible
- if truncation lands inside the newest retained segment, rewrite just that surviving prefix into a fresh segment and reclaim the old one

### Recovery Model

After `open`, the collection should rebuild state in this order:

1. Load the retained segment basis selected by replay.
2. Rebuild the in-memory record index from those retained segments.
3. Replay later `Append` and `TruncateAfter` updates in WAL order.

The same record id should never be reused after a truncate. Keep ids monotonic and let truncation change visibility, not identity assignment. That makes recovery and iterator resume logic much simpler.

### Test Matrix

For a log collection, add tests for:

- append three records, reopen, and read them back in order
- append enough records to require multiple committed segments
- truncate to empty and reopen
- truncate to the middle record and verify later records are gone
- flush after several appends, then append more records without flushing
- reopen after retained segments plus later WAL updates
- reject malformed entry payloads and out-of-order truncation requests

## 8. Write Specs And Tests With The Code

A durable collection is not complete with code alone. Add:

- a collection spec under `spec/<name>.md`
- collection-local tests in `src/collections/<name>/tests.rs`
- end-to-end storage tests where the collection interacts with replay or reclaim
- traceability tests under `src/tests/traceability/` for any new normative requirements

Use the current map work as the template:

- normative storage-independent payload rules live in [`spec/map.md`](../spec/map.md)
- shared architecture rules live in [`spec/implementation.md`](../spec/implementation.md)
- traceability tests tie requirements back to code under [`src/tests/traceability/`](../src/tests/traceability/)

At a minimum, test these paths:

- create, reopen, and read back state
- replay after retained updates
- snapshot or flush followed by reopen
- drop and reclaim behavior
- WAL-head reclaim if the collection can exist with only an empty basis
- buffer-too-small and malformed-payload failures

## 9. Update The Documentation Entry Points

After the code lands, update the high-level docs so readers can find the new collection:

- [`README.md`](../README.md) documentation map and supported-surface summary
- [`docs/architecture-and-api.md`](./architecture-and-api.md) module guide or collection overview
- rustdoc on the new public types and methods

If the collection is still experimental, say that explicitly. The channel module is the precedent for "public but not durably integrated yet."

## Checklist

- Added a new `CollectionType` variant and stable code
- Exported the collection module from `src/collections.rs`
- Implemented collection-local update, snapshot, and region codecs
- Added an empty snapshot constant for reclaim preservation
- Extended runtime support in `src/storage.rs`
- Extended startup and `Storage::open` type validation
- Added `Storage` facade methods and future-returning variants where needed
- If the collection is unbounded, extended the shared runtime to retain multi-region collection bases
- Added a collection spec and traceability coverage
- Updated README and narrative docs
