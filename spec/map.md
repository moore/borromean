# Map Collection Specification

## Purpose

This specification defines the logical API and durable payload semantics
for the Borromean map collection. Shared storage ordering, replay,
reclaim, WAL layout, and committed-region mechanics remain defined by
[spec/ring.md](ring.md). Implementation-level architecture and API
constraints remain defined by
[spec/implementation.md](implementation.md).

This document keeps normative requirements adjacent to the text that
motivates them. Stable identifiers remain the Duvet traceability
targets; the surrounding prose explains why those targets are needed
and how the set fits together.

This document has two layers:

- the current Duvet-backed requirements for the implemented durable map
  format, helper behavior, storage integration, manifest/run-chain
  support, and compaction behavior
- the implemented whole-run LSM map model, including manifest/run
  structure, flush rules, and deferred explicit whole-run compaction

The map is implemented as a manifest-backed LSM for committed
run-chain state: frontier updates flush into immutable sorted runs, runs
can overlap by key range, reads resolve newest-to-oldest, and
deferred explicit compaction replaces complete runs with merged replacement
runs when callers invoke compaction. The earlier single committed-region
snapshot basis is retired and is no longer part of the supported map contract.

## Key And Value Model

The map is parameterized by `K: LsmKey` and `V: LsmValue`. These traits
separate the parts of a map entry that storage must reason about from
the parts it can treat as opaque bytes.

Keys need a stable encoded form because committed runs are sorted,
searched, merged, and compacted without keeping every decoded `K` in
memory. The initial key trait is:

```rust
trait LsmKey {
    fn encode_key(&self, out: &mut [u8]) -> Result<usize, LsmKeyError>;

    fn decode_key(encoded: &[u8]) -> Result<Self, LsmKeyError>
    where
        Self: Sized;
}
```

`encode_key` produces the canonical durable key bytes. The current
implementation still orders map entries and run bounds with `K: Ord`, while
the encoded bytes provide the stable persisted key representation. For keys
whose natural ordering is not lexicographic over their raw bytes, `K: Ord`
must define the stable map ordering and the encoded representation must remain
compatible with that ordering wherever committed run metadata uses decoded key
bounds.

Multipart keys should be represented as ordered, self-delimiting encoded
parts inside the canonical key bytes. For example, a logical key such as
`(tenant_id, device_id, timestamp)` should encode as three complete key
parts in that order. Storage does not need to understand the semantic
meaning of those parts, but the encoded form should preserve enough
structure that a future prefix/range API can ask the key type for bounds
or matching logic for complete leading parts. That keeps partial-key
matching in the key layer instead of teaching storage about application
schemas.

Values are intentionally less structured:

```rust
trait LsmValue {
    fn encode_value(&self, out: &mut [u8]) -> Result<usize, LsmValueError>;

    fn decode_value(encoded: &[u8]) -> Result<Self, LsmValueError>
    where
        Self: Sized;
}
```

The storage layer treats value bytes as opaque payloads. It writes value
bytes for `set`, carries them through snapshots, run segments, and
compaction, and decodes them only when an API such as `get` materializes
a value for caller code. Values do not participate in map ordering,
prefix matching, or compaction selection except through their encoded
size.

The current implementation uses `Ord` plus `serde`/`postcard` for keys
and values. That is an implementation path toward these traits, not the
desired long-term API boundary.

## Map API Model

The map collection is exposed as a typed partial function from `K` to
`V`, plus lifecycle operations that bind that logical map to durable
storage. The API model is intentionally object-level: callers should not
need to name WAL records, collection type codes, or committed-region
formats in normal map use. Operation scratch buffers belong to the
underlying Borromean database rather than to individual open map handles.
This keeps each `LsmMap` handle small enough that many collections can
be open at once; the handle primarily tracks the collection id and may
cache small metadata for efficiency.

The design-level map API is:

```rust
impl<K, V> LsmMap<K, V>
where
    K: LsmKey,
    V: LsmValue,
{
    fn new(storage: &mut Storage) -> Result<Self, StorageError>;
    fn open(collection_id: CollectionId, storage: &mut Storage)
        -> Result<Self, StorageError>;
    fn collection_id(&self) -> CollectionId;
    fn get<R, F>(&self, storage: &mut Storage, key: &K, f: F)
        -> Result<Option<R>, LsmMapError>
    where
        F: FnOnce(&K, &V) -> R;
    fn set(&mut self, storage: &mut Storage, key: K, value: V)
        -> Result<bool, LsmMapError>;
    fn delete(&mut self, storage: &mut Storage, key: K)
        -> Result<bool, LsmMapError>;
    fn compact(&mut self, storage: &mut Storage) -> Result<(), LsmMapError>;
}
```

The `Storage` value owns the bounded memory used for mutable frontiers,
read/value materialization, serialization scratch, run descriptor
capacity, and compaction scratch. Map operations borrow `&mut Storage`
while they use that memory. `LsmMap::new(storage)` creates a durable map
collection, assigns it a stable collection id, and returns an empty map
handle. `collection_id` returns the stable id that can later be passed to
`LsmMap::open`. `open` reconstructs the logical map from the retained
durable basis and later retained updates using storage-owned buffers.
`get` observes newest-wins map visibility across the mutable frontier and
retained durable layers. If the key is visible, `get` materializes the
value using storage-owned buffers, calls `f(key, &V)` exactly once while
the value borrow is valid, and returns `Ok(Some(f_result))`. The `&K`
passed to `f` is the same lookup key reference passed to `get`; it is not
a separately materialized stored key.
If the key is absent, `get` returns `Ok(None)` and does not call `f`.
This lets callers use the value immediately inside the callback or copy
the needed result into `R`, while preventing storage-backed value borrows
from escaping the operation. `set` and `delete` update the logical map and
persist the mutation, flushing the frontier first if bounded in-memory
capacity would otherwise be exceeded. On success, `set` and `delete`
return `true` when the map's configured compaction policy says
compaction is needed after that mutation and any required frontier flush.
They return `false` when no compaction is currently needed. `compact`
performs whole-run compaction for that map using storage-owned scratch
buffers; if no compaction is needed, it returns successfully without
changing the logical map.

`MAP_CODE` is the stable shared-storage `collection_type` code reserved
for durable map collections; in the current implementation it is
`CollectionType::MAP_CODE`, whose on-disk value is `2`. It identifies the
collection kind in WAL and committed-head records. It is an internal
storage discriminator, not a caller-facing map API argument, and it is
distinct from map committed-region format codes such as
`MAP_MANIFEST_V1_FORMAT` and `MAP_RUN_V1_FORMAT`.

The repository implementation also exposes lower-level storage bindings such
as `Storage::create_map`, `Storage::open_map` with a frontier byte buffer,
`update_map_frontier`, `append_map_update`, `snapshot_map`, `flush_map`,
`compact_map`, and `drop_map`. Those APIs are advanced plumbing around
`MapFrontier` and the shared runtime; normal map use should prefer the
object-level `LsmMap` API above. The `*_future` methods are caller-driven
future variants of the same lower-level operations and do not define separate
logical behavior.

## Empty Logical State

The map collection represents a partial function from keys `K` to values
`V`. The empty logical state contains no visible keys.

The empty state is the identity element for map replay. A newly created
durable collection has no snapshot, region, or update payload to
interpret, so open must produce the same logical state that an in-memory
frontier constructor produces. Without this equivalence, later update
replay would depend on whether a map was observed before or after the
first persisted mutation. These two requirements are sufficient for this
case because every non-empty state is introduced by the snapshot, region,
update, and merge rules below.

1. `MAP-STATE-001` After successful durable creation of a map
collection, opening that collection
MUST yield an empty logical map.
2. `MAP-STATE-002` `LsmMap::new` MUST construct the same empty logical
state used by an empty durable map basis.

## Snapshot Payload Format

The current implemented snapshot payload is a compact complete basis
for the logical map.

A snapshot is the map's smallest complete durable basis: it has to carry
all visible key states without depending on the WAL records that produced
them. The count and byte-length header make the payload self-delimiting
inside WAL records and run segments. The
separate `entry_refs` area lets serialized entries remain compact while
the lookup index remains sorted by key; that separation is why loaders
validate references instead of trusting physical byte order. Together,
the header, entry bytes, ordered references, and round-trip rule are
enough to decode the same partial function and reject ambiguous payloads.

1. `MAP-SNAPSHOT-001` A map snapshot payload MUST be encoded as
`[magic:"MAP2"][entry_count:u32 little-endian][entry_bytes_len:u32
little-endian][entry_bytes][entry_refs]`.
2. `MAP-SNAPSHOT-002` Snapshot encoding MUST write `entry_count` as the
number of visible entries in the logical map and `entry_bytes_len` as
the exact byte length of the compact serialized entry data that follows.
3. `MAP-SNAPSHOT-003` Loading a valid snapshot payload MUST reconstruct
the same logical key/value visibility encoded by that payload.
4. `MAP-SNAPSHOT-004` Snapshot loaders MUST treat `entry_refs` as an
ordered, non-overlapping description of the compact entry bytes.

## Update Payload Format

Map updates represent logical mutations layered over the retained basis.

The shared WAL stores map updates as opaque bytes, so the map layer needs
one canonical byte representation and one canonical interpretation. Using
the `MapUpdate<K, V>` postcard representation keeps the payload tied to
the same typed key/value serialization used by snapshots, while the
`Set` and `Delete` variants cover the complete mutation surface of a
partial function. The merge rules later define where these mutations sit
relative to a durable basis, so no additional update operation is needed.

1. `MAP-UPDATE-001` A map update payload MUST be the exact `postcard`
serialization of `MapUpdate<K, V>`.
2. `MAP-UPDATE-002` Applying a `Set` update payload MUST make the key
visible with the supplied value, and applying a `Delete` update payload
MUST make the key absent from the frontier.

## Committed Head Format

The supported committed map head is `MAP_MANIFEST_V1_FORMAT`. Its payload
describes the live immutable run set for one map collection. The retired
single-region snapshot format, historically named `MAP_REGION_V1_FORMAT`,
is not a supported durable map basis in this specification.

1. `MAP-REGION-001` A committed map head with
`collection_format = MAP_MANIFEST_V1_FORMAT` MUST encode a manifest that
describes the live immutable map run set.
2. `MAP-REGION-002` A live map collection MUST NOT use the retired
single-region snapshot format as its committed durable basis.
3. `MAP-REGION-003` Loading a valid committed manifest head MUST recover
the same logical state as reading the manifest-described run chains.

## Merge And Frontier Rules

The current implemented map collection uses a durable basis plus a
mutable frontier.

Replay has to answer one question: which durable basis is current, and
which later updates are still live above it. Borromean reclaim can remove
older WAL records once a newer basis survives, so the map cannot assume
that the retained history starts with `new_collection`. It must accept
the replay-selected empty basis, WAL snapshot, or manifest region as the
base layer, then apply only later retained updates in
durable order. Newer frontier state wins because every update is a
logical replacement for its key. Flushing writes a new immutable basis so
crash recovery and reclaim can reason about old and new heads without
in-place mutation.

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

The shared storage layer can validate record ordering and region
ownership, but it cannot know whether opaque map bytes describe a valid
map. Opening a live map is therefore the point where collection type,
committed-region format, snapshot layout, update decoding, and run
metadata all meet. Rejecting mismatches and malformed payloads here is
necessary because accepting them would make later lookup behavior depend
on undefined bytes. It is also sufficient for open-time safety: once the
retained basis and all post-basis updates validate under the map rules,
the merge rules define the resulting logical state.

1. `MAP-VALIDATE-001` Map snapshot loading MUST reject payloads whose
lengths, entry ranges, ordering, or entry decoding are invalid.
2. `MAP-VALIDATE-002` Opening or loading a live map collection MUST
reject retained collection state whose `collection_type` is not
`MAP_CODE`.
3. `MAP-VALIDATE-003` Opening or loading a live map collection MUST
reject retained committed-region bases whose `collection_format` is not
a supported map head format.
4. `MAP-VALIDATE-004` Opening a live map collection MUST reject
retained committed-region payloads, snapshot payloads, or update
payloads that fail map-specific validation.

## Snapshot Frontier And Logical Map Requirements

These requirements cover implemented map snapshot helpers, in-memory frontier behavior, and logical
read/write semantics.

The in-memory frontier and snapshot payload use the same sorted-entry
model, so the low-level helper behavior is part of the durable contract.
Exact buffer handling matters because this code is intended for bounded,
allocation-free environments: exact-size buffers should work, undersized
buffers should fail explicitly, and checkpoints need to roll back a
speculative frontier mutation without changing visible state. Correct
entry-reference serialization, binary search, subrange encoding, and
empty-snapshot handling are the mechanical pieces that make snapshot
round trips, run segmentation, and update replay reliable. The generated
read/write and delete workloads then exercise the logical consequence:
the latest non-deleted value is visible and deleted keys are absent.

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
9. `RING-IMPL-REGRESSION-135` Entry reference serialization MUST preserve 32-bit offsets for
   entries beyond 64 KiB of frontier storage.
10. `RING-IMPL-REGRESSION-033` Map read/write operations MUST return the latest inserted values for
    generated key/value workloads.
11. `RING-IMPL-REGRESSION-034` Map write/delete operations MUST remove deleted keys while preserving
    non-deleted entries for generated workloads.

## Run Manifest And Committed Map Region Requirements

These requirements cover implemented helper behavior for map run descriptors, run segment payloads,
manifest descriptors, and committed map regions.

Runs are the physical units that let the map become an LSM rather than a
single rewritten region. A run descriptor gives reads enough information
to skip impossible key ranges, choose newest-to-oldest search order,
estimate compaction work, and find every physical region that remains
live. A run segment stores a sorted snapshot subrange plus generation,
bounds, and a next-region link so runs can exceed one committed region
while still being recoverable from a manifest. Manifest helpers are
needed because the manifest, not hidden allocation state, owns the live
run set. The reachability requirements close the loop: old manifests and
omitted run chains can be reclaimed only after replacement, and lookup
reads manifest-backed run state. This set is sufficient for the
implemented run-chain basis because it covers descriptor metadata,
segment bytes, planning, writing, manifest commit, lookup, and reclaim.

1. `RING-IMPL-REGRESSION-009` Map run descriptors MUST use inclusive lower and upper key bounds for
   may_contain, integer helpers MUST advance offsets and reject short buffers, and manifest capacity
   checks MUST reject excess runs.
2. `RING-IMPL-REGRESSION-014` Run cursors MUST advance segment positions correctly for ascending
   and descending run chains, and compaction writers MUST report segment-fit and state-count
   overflow errors.
3. `RING-IMPL-REGRESSION-016` Run segment payloads MUST round-trip generation, next-region link,
   key bounds, and snapshot lookup semantics, and reject undersized or truncated payloads.
4. `RING-IMPL-REGRESSION-017` Committed-region helpers MUST accept boundary-sized payload regions
   and snapshot helpers MUST decode exact empty-snapshot payloads.
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
12. `RING-IMPL-REGRESSION-027` Flushing a map to storage MUST commit a manifest-backed run-chain
    basis and reject flushes that exceed configured run capacity.
13. `RING-IMPL-REGRESSION-028` Committed run storage helpers MUST read run segment bounds and next
    links only from matching map-run regions and reject non-run region headers.
14. `RING-IMPL-REGRESSION-029` Map lookup helpers MUST read manifest run chains, and
    head-reference checks MUST report manifest and run regions as reachable.

## Map Storage Integration Requirements

These requirements cover the map collection's integration with shared storage replay, flush, drop,
and reopen behavior.

The storage runtime knows collection ids, WAL order, region allocation,
and reclaim transactions; the map layer knows how to interpret map
payloads. The integration requirements are the boundary contract between
those layers. Opening must filter WAL records by collection id so one
collection's update or drop cannot affect another. Storage visitation
must preserve typed durable order so map replay can select the correct
basis and post-basis updates. Flush and drop then have to respect the
shared allocator and reclaim protocol: a replacement basis preserves
lookups while detaching old regions, a dropped committed basis becomes
reclaimable, and a WAL-snapshot basis has no committed region to reclaim.
The crash-reopen cases are necessary because reclaim is transactional;
they show which pending reclaims are completed, reconstructed, or
discarded depending on whether the live basis was already detached.

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

Compaction is a physical rewrite of immutable runs, not a logical map
mutation. It is deferred and explicit rather than performed in the middle of a
CRUD operation. The requirements therefore focus on the properties that make a
separate compaction operation safe: selected runs are reduced according to the
target-then-greedy policy, unselected runs remain live, duplicate keys resolve
with newest-wins semantics, and tombstones continue masking older values.
Streaming the replacement directly into run storage is necessary because the
merged output can be larger than the mutable frontier. These properties are
sufficient for compaction correctness because after the replacement manifest
commits, every visible lookup observes the same logical result while the
physical run set has fewer or better-shaped runs.

1. `RING-IMPL-REGRESSION-110` Targeted then greedy map compaction MUST reduce selected runs while
   preserving unselected runs and all visible key/value lookups.
2. `RING-IMPL-REGRESSION-111` Map compaction MUST preserve tombstone masking so deleted keys remain
   absent and later live keys remain visible.
3. `RING-IMPL-REGRESSION-112` Map compaction MUST stream replacements larger than frontier
   capacity into a single run while preserving all visible key/value lookups across repeated
   compaction.

## Whole-Run LSM Model

The durable map model is an LSM made from immutable sorted runs. The
mutable frontier remains the newest state. When the frontier is flushed,
Borromean writes a new immutable run rather than rewriting an older run.
A run is a sorted table of key states, where each key state is either a
visible value or a tombstone. Tombstones are retained until compaction
proves that no older live run can expose a value for the same key.

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

## Manifest And Run Formats

The committed map head for run-chain maps is a manifest region using
`MAP_MANIFEST_V1_FORMAT`. The manifest describes the live run set for a
map collection. It records enough metadata to recover read order,
identify all physically live run regions, and choose later compaction
work without scanning every segment payload first.

Each live run descriptor records:

- generation, where larger values are newer
- first physical region in the run
- number of physical regions in the run
- approximate count of entries plus tombstones in the run
- encoded lower and upper key bounds for the run

The immutable run segment format is `MAP_RUN_V1_FORMAT`. A run can
occupy one region or a chain of regions. Each segment stores sorted key
states and enough chain metadata for recovery to validate that the
manifest's declared region count and first region describe the complete
run. The manifest, not hidden region ownership, is authoritative for
which runs are live.

The Duvet-backed requirements above currently cover the behavior that
depends on these bytes: descriptor metadata preservation, segment
payload parsing, chain traversal, manifest loading, lookup, reachability,
and reclaim. A future specification pass can promote the exact field
order and scalar widths for `MAP_MANIFEST_V1_FORMAT` and
`MAP_RUN_V1_FORMAT` into `MAP-` byte-format requirements if those
layouts need review separate from helper behavior.

## Flush And Manifest Commit Rules

A frontier flush writes a new immutable sorted run and then commits a
new manifest head. The new manifest includes the new run plus every
prior live run that has not been removed by compaction. Older overlapping
runs remain live after a normal flush.

A manifest commit is the logical point at which the live run set
changes. Regions referenced by the previous manifest remain live until a
new manifest that omits them is durable. After the replacement manifest
is durable, omitted run regions become eligible for ordinary Borromean
reclaim.

## Whole-Run Deferred Compaction

Compaction operates on complete runs, not on key-range slices and not on
individual physical regions within a run. A compaction job reads the
selected runs in generation order, performs one sorted merge, drops
obsolete older key states hidden by newer states, writes one replacement
run chain, and commits a replacement manifest.

The implemented selection policy is "Target Then Greedy":

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
fractal-index-inspired design. CRUD operations do not run that work inline:
`set` and `delete` report whether compaction is needed, and callers invoke
`compact` or `compact_map` separately instead of pushing buffered messages
through an internal tree on node overflow.

## Runtime Integration Status

The shared replay model still records one retained committed head region
per collection. For manifest-backed maps, that head region is the
manifest, and map-specific reachability expands it into the run-chain
regions named by the manifest. Startup, lookup, compaction, and reclaim
therefore cooperate through the manifest: run regions stay live while a
live manifest names them, and omitted runs become reclaimable only after
a replacement manifest is durable.
