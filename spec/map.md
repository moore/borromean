# Map Collection Specification

## Purpose

This specification defines the durable payload semantics for the
Borromean map collection. Shared storage ordering, replay, reclaim, WAL
layout, and committed-region mechanics remain defined by
[spec/ring.md](ring.md). Implementation-level architecture and API
constraints remain defined by
[spec/implementation.md](implementation.md).

This document has two layers:

- the current Duvet-backed requirements for the implemented durable map
  format, helper behavior, storage integration, and compaction behavior
- the intended whole-run LSM map design that the durable map is expected to grow
  toward

The current implementation still supports a single committed-region map
basis. The target design is a manifest-backed LSM: frontier updates
flush into immutable sorted runs, runs can overlap by key range, reads
resolve newest-to-oldest, and asynchronous compaction replaces complete
runs with merged replacement runs.

## Empty Logical State

The map collection represents a partial function from keys `K` to values
`V`. The empty logical state contains no visible keys.

1. `MAP-STATE-001` After a durable
`new_collection(collection_id, MAP_CODE)` basis, opening the collection
MUST yield an empty logical map.
2. `MAP-STATE-002` `LsmMap::new` MUST construct the same empty logical
state used by an empty durable map basis.

## Snapshot Payload Format

The current implemented snapshot payload is a compact complete basis
for the logical map.

1. `MAP-SNAPSHOT-001` A map snapshot payload MUST be encoded as
`[entry_count:u32 little-endian][entry_bytes_len:u32 little-endian][entry_bytes][entry_refs]`.
2. `MAP-SNAPSHOT-002` Snapshot encoding MUST write `entry_count` as the
number of visible entries in the logical map and `entry_bytes_len` as
the exact byte length of the compact serialized entry data that follows.
3. `MAP-SNAPSHOT-003` Loading a valid snapshot payload MUST reconstruct
the same logical key/value visibility encoded by that payload.
4. `MAP-SNAPSHOT-004` Snapshot loaders MUST treat `entry_refs` as an
ordered, non-overlapping description of the compact entry bytes.

## Update Payload Format

Map updates represent logical mutations layered over the retained basis.

1. `MAP-UPDATE-001` A map update payload MUST be the exact `postcard`
serialization of `MapUpdate<K, V>`.
2. `MAP-UPDATE-002` Applying a `Set` update payload MUST make the key
visible with the supplied value, and applying a `Delete` update payload
MUST make the key absent from the frontier.

## Committed Region Format

The current implementation supports one committed-region map format,
`MAP_REGION_V1_FORMAT`.

1. `MAP-REGION-001` A committed map region with
`collection_format = MAP_REGION_V1_FORMAT` MUST encode its payload as
`[snapshot_len:u32 little-endian][snapshot_payload]`.
2. `MAP-REGION-002` The `snapshot_len` prefix MUST equal the exact byte
length of the embedded snapshot payload used as the region's durable
basis.
3. `MAP-REGION-003` Loading a valid committed region payload MUST
reconstruct the same logical state as loading its embedded snapshot
payload.

## Merge And Frontier Rules

The current implemented map collection uses a durable basis plus a
mutable frontier.

1. `MAP-MERGE-001` When opening a live map collection, the retained
durable basis MUST be selected from the replay-tracked empty basis,
retained snapshot basis, or retained committed-region basis, and any
later retained update payloads for that collection MUST then be applied
in replay order.
2. `MAP-MERGE-002` Later retained updates MUST take precedence over
older values from the retained basis for the same key.
3. `MAP-MERGE-003` Flushing a mutable map frontier MUST write a new
immutable committed region rather than rewriting the previous live
region in place.

## Validation And Open Rules

Map-specific validation is authoritative for live map collections.

1. `MAP-VALIDATE-001` Map snapshot loading MUST reject payloads whose
lengths, entry ranges, ordering, or entry decoding are invalid.
2. `MAP-VALIDATE-002` Opening or loading a live map collection MUST
reject retained collection state whose `collection_type` is not
`MAP_CODE`.
3. `MAP-VALIDATE-003` Opening or loading a live map collection MUST
reject retained committed-region bases whose `collection_format` is not
`MAP_REGION_V1_FORMAT`.
4. `MAP-VALIDATE-004` Opening a live map collection MUST reject
retained committed-region payloads, snapshot payloads, or update
payloads that fail map-specific validation.

## Snapshot Frontier And Logical Map Requirements

These requirements cover implemented map snapshot helpers, in-memory frontier behavior, and logical
read/write semantics.

1. `RING-IMPL-REGRESSION-010` Snapshot helpers MUST validate snapshot layout, preserve
   set/delete/not-found lookup semantics, encode exact subranges, and reject out-of-bounds or
   undersized buffers.
2. `RING-IMPL-REGRESSION-011` Snapshot and frontier search helpers MUST find even-window keys and
   return the correct insertion position for missing keys.
3. `RING-IMPL-REGRESSION-012` Loading a snapshot MUST use entry reference offsets rather than
   physical entry byte order so reversed adjacent entry storage still loads sorted keys.
4. `RING-IMPL-REGRESSION-013` Snapshot encoding MUST accept exact empty snapshot capacity and
   snapshot decoding MUST reject invalid entry references.
5. `RING-IMPL-REGRESSION-015` Entry reference and entry count helpers MUST preserve exact
   serialized offsets and counts, and map checkpoints MUST restore prior frontier state while
   rejecting undersized buffers.
6. `RING-IMPL-REGRESSION-018` Loading an empty snapshot MUST fit in a frontier buffer containing
   only the entry-count header and MUST leave lookups empty.
7. `RING-IMPL-REGRESSION-020` Frontier range, region encoding, and checkpoint helpers MUST accept
   exact-size buffers, preserve lookup state, and reject undersized or malformed inputs.
8. `RING-IMPL-REGRESSION-031` Entry reference serialization MUST preserve independent start and end
   offsets for distinct record indexes.
9. `RING-IMPL-REGRESSION-033` Map read/write operations MUST return the latest inserted values for
   generated key/value workloads.
10. `RING-IMPL-REGRESSION-034` Map write/delete operations MUST remove deleted keys while preserving
    non-deleted entries for generated workloads.

## Run Manifest And Committed Map Region Requirements

These requirements cover implemented helper behavior for map run descriptors, run segment payloads,
manifest descriptors, and committed map regions.

1. `RING-IMPL-REGRESSION-009` Map run descriptors MUST use inclusive lower and upper key bounds for
   may_contain, integer helpers MUST advance offsets and reject short buffers, and manifest capacity
   checks MUST reject excess runs.
2. `RING-IMPL-REGRESSION-014` Run cursors MUST advance segment positions correctly for ascending
   and descending run chains, and compaction writers MUST report segment-fit and state-count
   overflow errors.
3. `RING-IMPL-REGRESSION-016` Run segment payloads MUST round-trip generation, next-region link,
   key bounds, and snapshot lookup semantics, and reject undersized or truncated payloads.
4. `RING-IMPL-REGRESSION-017` Committed-region helpers MUST accept boundary-sized payload regions
   and legacy snapshot helpers MUST decode exact empty-snapshot payloads.
5. `RING-IMPL-REGRESSION-019` Map run selection and generation helpers MUST count only run-chain
   regions for live region totals, compaction selection, and next generation calculations.
6. `RING-IMPL-REGRESSION-021` Manifest descriptor loading MUST preserve run metadata and reject too
   many runs, zero-length run chains, and truncated descriptor payloads.
7. `RING-IMPL-REGRESSION-022` Snapshot run segment helpers MUST plan at least one region and encode
   requested snapshot subranges with generation, next-region link, bounds, and lookup semantics.
8. `RING-IMPL-REGRESSION-023` Snapshot run planning and storage writes MUST split snapshots that
   exceed one committed run payload across multiple run regions, return a descriptor with the exact
   state count and lower and upper keys, and return no descriptor for an empty snapshot.
9. `RING-IMPL-REGRESSION-024` Frontier run planning MUST count every committed run payload segment
   required for frontier contents that exceed one run-region payload.
10. `RING-IMPL-REGRESSION-025` Reclaiming map run regions MUST move all tracked run-chain regions to
    the storage free-list tail.
11. `RING-IMPL-REGRESSION-026` Committing a map manifest MUST reclaim the previous manifest region
    and retain only run-chain descriptors in the manifest state.
12. `RING-IMPL-REGRESSION-027` Flushing a map to storage MUST convert valid legacy region bases into
    run-chain descriptors and reject flushes that exceed configured run capacity.
13. `RING-IMPL-REGRESSION-028` Committed run storage helpers MUST read run segment bounds and next
    links only from matching map-run regions and reject non-run region headers.
14. `RING-IMPL-REGRESSION-029` Map lookup helpers MUST read both legacy region snapshots and
    manifest run chains, and head-reference checks MUST report manifest and run regions as
    reachable.

## Map Storage Integration Requirements

These requirements cover the map collection's integration with shared storage replay, flush, drop,
and reopen behavior.

1. `RING-IMPL-REGRESSION-030` Opening a map from storage MUST replay only WAL records for the
   requested collection and ignore updates and drop records for other collections.
2. `RING-IMPL-REGRESSION-032` Storage WAL record visitation for maps MUST expose typed
   new-collection and snapshot records for map collections in durable order.
3. `RING-IMPL-REGRESSION-108` Storage map APIs MUST restore snapshot basis values and later typed
   updates when opening a map.
4. `RING-IMPL-REGRESSION-109` Storage map flush API MUST write a committed region basis, clear
   ready_region, and preserve flushed key/value lookups.
5. `RING-IMPL-REGRESSION-113` Reopening after a map replacement flush MUST complete pending
   reclaim of the replaced region and preserve the replacement map value.
6. `RING-IMPL-REGRESSION-114` Reopening after replacement with an empty free list MUST initialize
   free-list head from the recovered reclaimed region.
7. `RING-IMPL-REGRESSION-115` Reopening after replacement with an empty free list MUST reconstruct
   free-list tail from the recovered reclaimed region.
8. `RING-IMPL-REGRESSION-116` Map flush MUST complete detached pending reclaims before allocating
   from the minimum free-region reserve.
9. `RING-IMPL-REGRESSION-117` Reopening after a premature reclaim_begin before replacement
   detaches the old head MUST discard the pending reclaim and preserve the old map basis and
   value.
10. `RING-IMPL-REGRESSION-118` Dropping a map with committed-region basis MUST start reclaim for
    that region, tombstone the collection, complete reclaim on reopen, and reject reopening the
    dropped map.
11. `RING-IMPL-REGRESSION-119` Reopening after a premature reclaim_begin before drop detaches the
    live region MUST discard the pending reclaim and preserve the live map basis and value.
12. `RING-IMPL-REGRESSION-120` Dropping a map whose basis is a WAL snapshot MUST tombstone the
    collection without starting a region reclaim.

## Map Compaction Requirements

These requirements cover implemented whole-run compaction behavior.

1. `RING-IMPL-REGRESSION-110` Targeted then greedy map compaction MUST reduce selected runs while
   preserving unselected runs and all visible key/value lookups.
2. `RING-IMPL-REGRESSION-111` Map compaction MUST preserve tombstone masking so deleted keys remain
   absent and later live keys remain visible.
3. `RING-IMPL-REGRESSION-112` Map compaction MUST stream replacements larger than frontier
   capacity into a single run while preserving all visible key/value lookups across repeated
   compaction.

## Target Whole-Run LSM Model

The intended durable map model is an LSM made from immutable sorted
runs. The mutable frontier remains the newest state. When the frontier
is flushed, Borromean writes a new immutable run rather than rewriting
an older run. A run is a sorted table of key states, where each key
state is either a visible value or a tombstone. Tombstones are retained
until compaction proves that no older live run can expose a value for
the same key.

Runs are ordered by generation. Higher generations are newer. Runs can
have overlapping key ranges, especially newly flushed runs. Lookup
therefore checks the mutable frontier first, then WAL-retained updates,
then immutable runs from newest generation to oldest generation. The
first visible `Set` determines the returned value. The first visible
`Delete` determines absence and masks older values.

Iteration over the map is a merge of the mutable frontier, retained WAL
updates, and live immutable runs. It yields each logical key at most
once, with the same newest-wins and tombstone-masking semantics as
point lookup.

## Target Manifest And Run Formats

The intended committed map head is a manifest region using
`MAP_MANIFEST_V1_FORMAT`. The manifest describes the live run set for a
map collection. It records enough metadata to recover read order,
identify all physically live run regions, and choose future compaction
work without scanning every segment payload first.

Each live run descriptor records:

- generation, where larger values are newer
- first physical region in the run
- number of physical regions in the run
- approximate count of entries plus tombstones in the run
- encoded lower and upper key bounds for the run

The intended immutable run segment format is `MAP_RUN_V1_FORMAT`. A run
can occupy one region or a chain of regions. Each segment stores sorted
key states and enough chain metadata for recovery to validate that the
manifest's declared region count and first region describe the complete
run. The manifest, not hidden region ownership, is authoritative for
which runs are live.

The exact byte layouts for `MAP_MANIFEST_V1_FORMAT` and
`MAP_RUN_V1_FORMAT` are intentionally left to the implementation pass
that introduces those formats. Once those format values are accepted by
the runtime, this specification needs to be extended with Duvet-backed
requirements for the exact bytes and validation rules.

## Target Flush And Manifest Commit Rules

A frontier flush writes a new immutable sorted run and then commits a
new manifest head. The new manifest includes the new run plus every
prior live run that has not been removed by compaction. Older overlapping
runs remain live after a normal flush.

A manifest commit is the logical point at which the live run set
changes. Regions referenced by the previous manifest remain live until a
new manifest that omits them is durable. After the replacement manifest
is durable, omitted run regions become eligible for ordinary Borromean
reclaim.

## Target Whole-Run Async Compaction

Compaction operates on complete runs, not on key-range slices and not on
individual physical regions within a run. A compaction job reads the
selected runs in generation order, performs one sorted merge, drops
obsolete older key states hidden by newer states, writes one replacement
run chain, and commits a replacement manifest.

The target selection policy is "Target Then Greedy":

1. Select newest runs until replacing them would move the collection
   toward its configured live map-run region target.
2. Continue including older runs while each older run's approximate
   entry-plus-tombstone count is smaller than the accumulated selected
   count.
3. Stop when the next older run is not smaller than the accumulated
   selected count or when no older run remains.

The sum of selected run entry-plus-tombstone counts is the conservative
proxy for output size. The exact output size is discovered during the
single merge pass after duplicate keys and masked tombstones are
discarded.

This gives the map the deferred whole-run merge behavior wanted from a
fractal-index-inspired design, but schedules the work asynchronously
like an LSM database instead of pushing buffered messages through an
internal tree on node overflow.

## Target Runtime Prerequisites

The current replay model retains only one committed region basis per
collection. The target LSM map needs shared runtime support for a
manifest basis whose payload makes additional run regions live. Startup,
reachability, and reclaim need to validate manifest-referenced regions
and keep them live until a newer manifest removes them.

Until that shared runtime support exists, `MAP_REGION_V1_FORMAT` remains
the only implemented committed map region format.
