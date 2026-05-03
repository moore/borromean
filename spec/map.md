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
  format
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
