# Low Level Storage

## Motivation

When using built-in flash storage on small microcontrollers, some
kind of database or file system is needed. This allows management of
multiple objects in flash and enables wear leveling to increase
storage longevity.

Some RTOSes include file systems and there are embedded databases
such as [ekv](https://github.com/embassy-rs/ekv),
[tickv](https://github.com/tock/tock/tree/master/libraries/tickv), and
[sequential-storage](https://github.com/tweedegolf/sequential-storage),
but none of these options fit the needs of
[finder](https://github.com/moore/finder).

For finder we need to support many instances of many collection types:
maps, queues, sets, logs, etc. For each of these types we require
efficient queries, allocations, and truncation.

If we used an RTOS this might be achievable with a file system, but
finder is planned for an embassy/bare-metal approach without that
option.

## Overview

To solve these challenges, borromean divides flash into equal-size
regions. Region starts and sizes must be aligned to the backing
flash's erase-block size so every region can be erased independently.
Each collection is implemented as an append-only data
structure where new writes are added to the head region and data can
only be freed by truncating the tail. For each collection, borromean
tracks a collection id and current head.

Before being written to storage, updates to a collection are kept in
memory. To persist mutations before a full region flush or snapshot,
each mutation is also written to a global write-ahead log (WAL)
shared by all collections.
Per-collection WAL entries contain a stable collection id and opaque
bytes. Collection ids are opaque 64-bit nonces that are assigned when
a collection is created by `new_collection(collection_id,
collection_type)`. Collection
id `0` is reserved for the WAL; all user collection ids are nonzero
and are not recycled.

A collection may be removed durably by appending
`drop_collection(collection_id)` to the WAL. Once that record is
durable, the collection is no longer live, no later WAL record for
that collection id is valid, and its older durable bytes may be
reclaimed once they are no longer physically reachable from live
storage state.

A collection head may refer either to
a committed region or to a WAL-resident snapshot. The data payload in
each region is defined by the collection type implementation.
For user collections, append-time validity requires a successful
`new_collection(collection_id, collection_type)` before any later
record for that collection may be appended. WAL reclaim may later
remove that `new_collection` record once a newer durable basis for the
collection survives elsewhere in the WAL or in committed regions.
Replay therefore distinguishes historical validity from retained
basis: after reclaim, the earliest retained basis record for a user
collection may be `snapshot`, `head`, or `drop_collection` even though
`new_collection` was required historically.

Borromean tracks the current collection type for each live collection
in WAL replay state. Any durable record that carries
`collection_type` (`new_collection`, `snapshot`, or `head`) is
authoritative for that collection. For a user collection, the earliest
retained type-bearing record seen during replay establishes the
replay-tracked `collection_type`, and that type does not change for
the lifetime of the live collection. Every later valid type-bearing
record for that collection must carry the same `collection_type`. A
drop-only retained tombstone does not by itself re-establish a live
`collection_type`; it only reserves the dropped `collection_id`.

A collection can be flushed either as a full region write or
as a partial state snapshot into the WAL. A WAL snapshot is a durable
staging point: when that collection is mutated again, the snapshot is
loaded into RAM, later mutations are still appended to the WAL as
`update` records, and the in-memory state is allowed to accumulate
enough change to eventually justify writing a full region. Allowing
snapshots to the WAL prevents many partially filled regions and low
effective storage utilization because partial snapshots can be
intermixed with other WAL entries and more easily collected when
stale.

Further snapshotting to the WAL allows bounded RAM usage with an
unbounded number of collections. If an update
targets a collection that does not currently have a free in-memory
buffer, the system may evict the least-frequently-used buffered
collection by flushing its current state snapshot to the WAL and
marking that WAL snapshot as the collection's current head.

When a WAL region is filled, its last record points to the next WAL
region.

A WAL region can be reclaimed when the number of live records drops
below a configurable threshold. During reclaim, we write the current
live state for each affected uncommitted collection into a new WAL
region by snapshotting that collection into the WAL head region. If a
collection's data is not in memory, that implies its current snapshot
is already in the WAL. If a current snapshot is in the region being
collected, it can be copied directly to the WAL tail while updating
the head pointer to the new location.

Once collection data is flushed from a WAL head being reclaimed, any
current head records are moved to the WAL tail, a normal
`head(collection_id = 0, collection_type = wal, region_index =
new_head)` record is written pointing to the new WAL head, and the old
WAL head is added to the free list. The WAL does not have a separate
WAL-only head-record type; it uses the same `head` record as every
other collection.

Any reclaim that frees a region is a WAL-tracked transaction. Before
removing a region from live collection or WAL state, borromean writes
and syncs `reclaim_begin(region_index)`. After the region is no longer
live, it is appended to the free list. Reclaim completes only after
`reclaim_end(region_index)` is written and synced. Startup replay treats
any `reclaim_begin` without a matching `reclaim_end` as an incomplete,
idempotent reclaim operation that must either be completed or proven
unnecessary before open succeeds.

The storage system also keeps a free list of regions that are
available to satisfy new allocations. This list is FIFO (First In,
First Out), to support wear leveling. The durable free-list head
is tracked in WAL replay order so every durable allocator-head change
is replayed exactly once. Allocations advance the durable free-list
head through `alloc_begin(..., free_list_head_after)`. Reclaim or
recovery steps that make a region the new free-list head without
consuming one use an explicit `free_list_head(region_index_or_none)`
record. Any WAL command that writes a newly allocated region must
persist the post-allocation free-list head in the same WAL record,
otherwise that region write and its allocation are not considered
durable.

Borromean must also maintain a configured `min_free_regions` reserve.
Let `max_in_memory_dirty_collections` be the maximum number of dirty
collections that may simultaneously have in-memory working state.
Each such dirty in-memory collection must be preservable using at most
one newly allocated region before reclaim frees any region: either by
writing a WAL snapshot if that snapshot fits in the available WAL
space, or by writing a normal collection region instead if the
snapshot would not fit efficiently in the WAL.
Under that assumption, `min_free_regions` must be at least
`max_in_memory_dirty_collections + 1`. The extra `+1` region is
reserved so WAL rotation, reclaim bookkeeping, or crash recovery can
still make forward progress before the first region is freed.
Ordinary foreground allocations must not consume the last
`min_free_regions` free regions; those regions are reserved so reclaim,
WAL rotation, and crash recovery can always make forward progress
instead of deadlocking while trying to free space. If an ordinary write
would require consuming that reserve, the implementation must first try
to reclaim regions. If, after such reclaim attempts, the free-list
still contains fewer than `min_free_regions` free regions, the database
must be treated as full for purposes of accepting further ordinary
writes. At that point, more drastic action such as dropping or
truncating collections, or migrating/reformatting onto a larger backing
store, is required before additional ordinary writes may be accepted.

### Storage Structure

Storage starts with a static metadata region that describes the
version and configuration parameters that cannot change after
initialization.

The rest of the database is made up of regions. Each region has a
header, user data, and a free pointer. The header describes the
region's sequence number, collection id, collection format, and a
checksum over the header itself.

The sequence number is a monotonically increasing value assigned each
time a new region is written. This lets us scan regions and identify
the newest region for each collection. This is primarily used to find
the head and tail of the WAL when opening the database.

The collection format defines how user data is encoded in the user
data section. Storing the format in each region allows format
evolution over time.

The free pointer stores the location of the next free region for
regions that have been freed, so the region in question is in the free
list. This field is written not when the region is freed, but when the
next region is freed. This is the mechanism used to make the free list
a FIFO. A free region whose free-pointer slot is still uninitialized
(for example, left in the erased state) is the current free-list tail.
A free region is defined by membership in the durable free-list chain,
not by a distinct on-disk header encoding. Free regions may still
contain stale header and payload bytes from their prior use; those
bytes are ignored while the region is free. Because the free-pointer
chain is stored inside the free regions themselves, a free region must
not be erased until it is allocated for reuse.

For WAL regions, the user-data area begins with a fixed
`WalRegionPrologue`. That prologue records the WAL head that was
current when the WAL region was initialized. WAL records do not begin
immediately after the region `Header`; they begin at the first
`wal_write_granule`-aligned byte after the end of the
`WalRegionPrologue`.

```mermaid
block-beta
 columns 4
 Storage["Allocated Storage"]:4
 Meta["Storage Metadata"]
 R1["First Region"]
 e1["..."]
 R2["Last Region"]
 space:4
 block:exp:4
  h1["Header"]
  d1["User Data"]
  a1["Free Pointer"]
 end
 space:4
 block:header:4
  s1["Sequence Number"]
  cid["Collection Id"]
  type["Collection Format"]
  check["Header Checksum"]
 end
 R1 --> exp
 h1 --> header
```

### Challenges

The core design constraint is that we cannot have any stable
locations that get repeatedly rewritten or those regions of the flash
will fail before the rest of the device. This leads to two main
conclusions:

 1. We should always attempt to free the oldest regions first.
 2. All data structures should be log structured/append only.

Freeing the oldest first must be performed on a per-collection basis,
as each collection is responsible for its own data and is
opaque to borromean at a high level.

The requirement that data structures be append only affects not
just the implementation of collection types but also the management
of:

 1. The current heads of each collection instance.
 2. The tracking of free regions.
 3. The tracking of the root of the database.

Each of these is solved by tracking this information in the WAL.
The WAL is collection 0. At startup we scan regions to find the WAL
region with the largest sequence number (the current WAL tail). The
start of each WAL region records the WAL head at the time that region
was created. We must also scan the tail region for any changes to the
head caused by reclaiming the WAL head region; those changes are
represented by ordinary `head` records with `collection_id = 0`.
Startup uses this metadata plus WAL replay to reconstruct uncommitted
state in memory and the current free-list head.

## WAL Record Types

All WAL records are append-only and ordered by physical write order
within the WAL region chain.

WAL record encoding and alignment:

Let `wal_record_area_offset` be the first offset within a WAL region
that is both:
past the end of the region `Header` plus `WalRegionPrologue`; and
aligned to `wal_write_granule`.
Replay and append scanning consider candidate WAL record starts only at
aligned offsets greater than or equal to `wal_record_area_offset`.
1. Every physical WAL record begins with a one-byte `record_magic`.
2. `record_magic` must equal the storage's configured
`wal_record_magic`, and `wal_record_magic` must not equal
`erased_byte`, the byte value returned by erased flash.
3. After the leading `record_magic`, the rest of the physical WAL
record is encoded with COBS (Consistent Overhead Byte Stuffing), or an
equivalent compatible byte-stuffing profile, such that neither
`erased_byte` nor `wal_record_magic` appears anywhere else in the
encoded record bytes.
4. Every WAL record start offset within a WAL region must be aligned to
`wal_write_granule`, the smallest writable unit of the backing flash.
5. The encoded size of every WAL record is rounded up to a multiple of
`wal_write_granule`. Replay advances from one candidate record start to
the next in aligned `wal_write_granule` steps.
6. At an aligned candidate record start in a reachable WAL region:
if the first byte is `erased_byte`, that slot is currently unwritten and
marks the end of the written portion of that WAL region;
if the first byte is `wal_record_magic`, that slot is a candidate WAL
record and must parse and validate normally;
if the first byte is neither, that slot lies inside a torn/corrupt WAL
record, so replay keeps scanning forward by aligned
`wal_write_granule` steps and ignores the corrupt bytes.
7. The recovered append point for the tail region is the first aligned
slot whose first byte is `erased_byte` after the last valid replayed
tail record. If no such slot exists, the tail region is currently full
and the next WAL append must rotate via `link` to a new WAL region.
8. Let `wal_link_reserve` be the aligned encoded size needed in the
current tail region to append the trailing
`link(next_region_index, expected_sequence)` record that completes WAL
rotation.
9. Let `wal_rotation_reserve` be the total aligned encoded size needed
in the current tail region to append the two WAL records required to
start and complete rotation to a new tail region:
`alloc_begin(next_region_index, free_list_head_after)` followed by
`link(next_region_index, expected_sequence)`.
10. Appending any WAL record to the current tail region, other than the
specific `alloc_begin(next_region_index, free_list_head_after)` that
starts WAL rotation or the trailing `link`, is invalid if doing so
would leave fewer than `wal_rotation_reserve` unwritten bytes in that
region.
11. Appending the `alloc_begin(next_region_index, free_list_head_after)`
that starts WAL rotation is invalid unless its aligned end offset still
leaves at least `wal_link_reserve` unwritten bytes in that region. Once
that rotation `alloc_begin` is durable, the only valid later WAL record
in that region is the matching trailing `link`.

Each WAL record encodes the following fields:

1. `record_type`: one of `new_collection`, `update`, `snapshot`,
`alloc_begin`, `head`, `drop_collection`, `link`, `free_list_head`,
`reclaim_begin`, `reclaim_end`, `wal_recovery`
2. `collection_id`: required for `new_collection`, `update`,
`snapshot`, `head`, and `drop_collection`
3. `collection_type`: required for `new_collection`, `snapshot`, and
`head`; omitted for `update`, `alloc_begin`, `drop_collection`, `link`,
`free_list_head`, `reclaim_begin`, `reclaim_end`, and `wal_recovery`
4. `payload_len`: payload size in bytes
5. `payload`: opaque bytes defined by `record_type`
6. `free_list_head_after`: required for `alloc_begin`; omitted for
`update`, `snapshot`, `head`, `drop_collection`, `link`, `free_list_head`,
`reclaim_begin`, `reclaim_end`, and `wal_recovery`
7. `record_checksum`: checksum covering the full logical record before
COBS encoding
8. `padding`: zero or more non-reserved bytes so the encoded record size is a
multiple of `wal_write_granule`

The record payloads are:

1. `new_collection`
Declares a new user collection with the given `collection_id` and
`collection_type`. Payload is empty. The record is the durable basis
decision for an empty collection with no committed regions, no
snapshots, and no updates in its durable basis.

2. `update`
Collection-local mutation delta. Applied in WAL order during replay.

3. `snapshot`
Full logical state for one collection at a point in time, tagged with
the collection type for that snapshot basis. Supersedes older `update`
records for that collection that appear before the snapshot.

4. `alloc_begin`
Reserves the current free-list head region for imminent use. The
payload contains the reserved `region_index`.
The record stores `free_list_head_after`, the next free region after
removing `region_index` from the free list. Once `alloc_begin` is
durable, allocator replay state advances even if the reserved region
is erased before a later `head` or `link` record uses it.
When written, `region_index` must equal the durable free-list head in
replay order, and `free_list_head_after` must be the successor that was
observed from that head's free-pointer chain at allocation time.
`alloc_begin(region_index, free_list_head_after)` has two replay-visible
effects:
1. It advances the durable free-list head to `free_list_head_after`.
2. It reserves `region_index` as `ready_region` until a matching durable
`head(..., region_index)` or `link(... next_region_index = region_index ...)`
consumes it.

5. `head`
Commits a collection to a new durable region head. Payload contains
the target `region_index`. The record also carries the collection type for
that durable region basis. When `collection_id = 0`, this record
commits a new WAL head region; there is no distinct WAL-head record
type. If `region_index` equals the currently reserved `ready_region`,
the `head` consumes that reservation and commits a newly allocated
region. Otherwise, the `head` retargets the logical collection head to
an already allocated existing region. Before appending such a
retargeting `head`, the implementation must validate that the target
region's header has the same `collection_id` and that the target
region is not currently free. Replay does not revalidate that
not-free append-time invariant.

6. `drop_collection`
Payload is empty. Durably tombstones a user collection. The record
detaches that collection's current durable basis from the live
namespace, discards any pending WAL updates for that collection, and
forbids any later WAL record for the same `collection_id`.
Previously live WAL snapshots or committed regions for that collection
become reclaimable once region reclaim removes any remaining physical
references to them. Any region associated with that dropped collection
may be added to the free list through normal reclaim processing if it
is not already reachable from the free-list chain.

7. `link`
Points from a full WAL region to the next WAL region. Payload contains
`next_region_index` and `expected_sequence` for the next WAL region
header.

8. `free_list_head`
Commits a new durable free-list head. Payload contains the new
`region_index` or `none` if the free list is empty. This record is used
when reclaim or crash recovery changes the durable allocator head
without consuming the prior head through `alloc_begin`. If the payload
is `region_index`, that region must be the start of a durable
free-pointer chain whose walk reaches an
uninitialized tail slot in at most `region_count` visited regions. If
the payload is `none`, the record asserts that the durable free list is
empty.

9. `reclaim_begin`
Marks the start of reclaim for `region_index`. The payload contains the
region being freed. This record does not itself make the region free;
it only makes the reclaim intent durable before any live references to
that region are removed.

10. `reclaim_end`
Marks successful completion of reclaim for `region_index`. The payload
contains the same `region_index` as the matching `reclaim_begin`.

11. `wal_recovery`
Payload is empty. Marks that replay or a prior open detected and
intentionally skipped one or more corrupt/torn aligned WAL slots before
resuming WAL appends. `wal_recovery` has no direct collection or
allocator effect; it only makes that recovery boundary explicit and
durable.

Ordering and validity rules:

1. A valid `new_collection(collection_id, collection_type)` record is
invalid if `collection_id = 0`, if `collection_type` is missing or
corrupt, or if replay has already seen any prior valid record for that
collection.
2. A valid `snapshot(collection_id, collection_type, ...)` record is
itself a durable WAL-snapshot head for that collection.
3. A `snapshot(collection_id, collection_type, ...)` record is invalid
if `collection_type` is missing or corrupt.
4. A `head(collection_id, collection_type, region_index)` record is the
commit point for a region flush.
5. A `head(collection_id, collection_type, region_index)` record is
invalid if `collection_type` is missing or corrupt.
6. A `drop_collection(collection_id)` record is invalid if
`collection_id = 0`.
7. For non-WAL collections (`collection_id != 0`), append-time
validity requires a successful earlier
`new_collection(collection_id, collection_type)` before any `update`,
`snapshot`, `head(collection_id, collection_type, region_index)`, or
`drop_collection(collection_id)` for that collection may be appended.
Replay of reclaimed WAL may no longer be able to observe that older
`new_collection`, so replay validity is defined separately below in
terms of retained basis records.
8. For user collections (`collection_id != 0`), `snapshot` and
`head(collection_id, collection_type, region_index)` are replay-valid
only if their `collection_type` either matches the already tracked
type for that collection or, when no retained type-bearing record for
that collection has been seen yet, establishes the replay-tracked
type from the earliest retained type-bearing basis record.
9. A retained `drop_collection(collection_id)` record may be the
earliest retained basis record for a user collection after reclaim. In
that case replay reconstructs only the dropped tombstone for that
`collection_id`; it does not infer a live `collection_type` from the
drop record alone.
10. A `head(collection_id, collection_type, region_index)` record for a
user collection is valid only if the target region header has the same
`collection_id`. Replay does not revalidate the append-time check that
an existing-region head target was not free.
11. For the WAL (`collection_id = 0`), `head` records are valid only if
their `collection_type` is the WAL collection type.
12. A `link` is only valid as the last complete record in a WAL region.
During WAL-chain traversal, a `link` in a reachable non-tail WAL region
is valid only if its target has a valid WAL header with sequence equal
to `expected_sequence` and a valid `WalRegionPrologue`. For the known
tail WAL region only, a durable trailing `link` whose target header is
missing, corrupt, or wrong-sequence, or whose `WalRegionPrologue` is
missing or corrupt, is treated as an incomplete rotation rather than
corruption; startup may finish initializing the target region using
`expected_sequence`.
13. A WAL record in the current tail region, other than the specific
`alloc_begin(next_region_index, free_list_head_after)` that starts WAL
rotation or the matching trailing `link`, is invalid if its aligned end
offset leaves fewer than `wal_rotation_reserve` bytes of currently
unwritten space remaining in that WAL region.
14. The `alloc_begin(next_region_index, free_list_head_after)` that
starts WAL rotation is invalid unless its aligned end offset leaves at
least `wal_link_reserve` bytes of currently unwritten space remaining
in that WAL region.
15. For non-WAL collections (`collection_id != 0`), `update` is
replay-valid only if replay has already seen a retained basis decision
for that collection.
16. For non-WAL collections (`collection_id != 0`), `snapshot`,
`head(collection_id, collection_type, region_index)`, and
`drop_collection(collection_id)` are invalid if replay has already seen
a prior valid `drop_collection(collection_id)` for that collection.
17. For non-WAL collections (`collection_id != 0`), a
`new_collection(collection_id, collection_type)` record is also invalid
if replay has already seen a prior valid
`drop_collection(collection_id)` for that collection.
18. An `alloc_begin(region_index, free_list_head_after)` record is invalid
if `free_list_head_after` is missing or corrupt, if replay's current
durable `last_free_list_head` is `none`, or if `region_index` does not
equal that durable free-list head.
19. A `free_list_head(region_index_or_none)` record is invalid if the
payload is corrupt. If `region_index_or_none = region_index`, the
record is valid only if startup can reconstruct a valid free-pointer
chain beginning at that region and terminating at a tail whose
free-pointer slot is uninitialized after visiting at most
`region_count` regions. If `region_index_or_none = none`, the record
asserts that the durable free list is empty.
20. A `head(region_index)` or `link(next_region_index, ...)` record that
writes a newly allocated region is valid only if replay has already
seen a prior unmatched `alloc_begin` for the same region index.
21. Durable allocator-head advance happens at `alloc_begin` or
`free_list_head`, not at `head` or `link`.
22. Replay may recover only from checksum-invalid or torn aligned WAL
slots. Replay tracks a pending WAL-recovery boundary from the first
ignored corrupt/torn aligned slot until a later valid `wal_recovery`
record is replayed.
23. If replay has a pending WAL-recovery boundary and encounters a
later valid complete record whose `record_type` is not `wal_recovery`,
startup must fail because later WAL data exists after unexplained
corruption.
24. If replay reaches the end of a reachable non-tail WAL region with a
pending WAL-recovery boundary that was not closed by `wal_recovery`,
startup must fail because that region contains unresolved mid-log
corruption. A pending WAL-recovery boundary may remain open only at the
end of the current replay tail region.
25. Any other invalidity of a complete record is storage corruption and
startup must fail rather than skipping that record. This includes
duplicate `new_collection`, collection-type mismatch, `head` or `link`
without a matching prior `alloc_begin`, any record after a valid
`drop_collection` for the same collection, broken non-tail WAL chain
links, and committed-region/header mismatch.
26. `reclaim_begin(region_index)` and `reclaim_end(region_index)` must appear
in WAL order and are matched by `region_index`.
27. `reclaim_end(region_index)` is only valid if preceded by a valid
`reclaim_begin(region_index)`.

Assumptions for replay correctness:

1. A WAL region must be erased before reuse.
2. Replay's tail-resynchronization rule depends on this
erase-before-reuse guarantee so stale bytes from prior use cannot be
misinterpreted as new valid records.
3. Replay distinguishes unwritten space from a torn record by checking
the aligned slot's first byte against `erased_byte` and
`wal_record_magic`, and by relying on the COBS-encoded WAL format to
exclude both reserved byte values from record bodies.
An aligned slot whose first byte is `erased_byte` marks end of the
written portion of that WAL region.
4. Any operation that consumes a free-list head must first make the
allocator advance durable with `alloc_begin(region_index,
free_list_head_after)`.
5. If replay ends with an unmatched `alloc_begin(region_index, ...)`, that
region is treated as a reserved `ready_region` for the next allocation
instead of being returned to the free list.

## Collection Head State Machine

Each tracked user collection is either durably dropped or has exactly
one logical current head after replay.

States:

1. `EmptyHead`
Latest durable basis is the empty collection created by a
`new_collection(collection_id, collection_type)` record. The
collection has a tracked collection type, but no durable region head,
no durable WAL snapshot, and no updates in its durable basis.

2. `InMemoryDirty`
Latest state is represented by a collection-defined in-memory
frontier layered over a durable basis. The frontier may be a full
materialization, but it may also be a compact delta or memtable that
supersedes data still stored in the durable basis.

3. `WALSnapshotHead`
Latest durable head points to a WAL `snapshot` record.

4. `RegionHead`
Latest durable head points to a committed collection region.

5. `Dropped`
Latest durable basis is a `drop_collection(collection_id)` tombstone.
The collection id remains reserved and tracked, but the collection no
longer has a live durable basis, accepts no further mutations, and its
older durable bytes are reclaimable once physically detached. Any
region associated with the dropped collection may be appended to the
free list if it is not already present there.

Transitions:

1. `NoCollection -> EmptyHead`
Write `new_collection(collection_id, collection_type)`.
Durable after the `new_collection` record is durable. The collection
starts in memory with tracked `collection_type`, no region basis, no
snapshot basis, and no pending updates.

2. `EmptyHead -> InMemoryDirty`
Open a mutable empty working state for the collection and append new
updates to the WAL while updating that RAM state.

3. `InMemoryDirty -> WALSnapshotHead`
Write `snapshot`.
Durable after the `snapshot` record is durable.

4. `InMemoryDirty -> RegionHead`
Write `alloc_begin(region_index, free_list_head_after)`, write collection
region, then write `head(collection_id, collection_type, region_index)`.
Durable after the `head` record is durable.

5. `WALSnapshotHead -> InMemoryDirty`
Load the snapshot into RAM as the mutable working state, then append
new updates to the WAL while updating that RAM state.

6. `WALSnapshotHead -> RegionHead`
Write `alloc_begin(region_index, free_list_head_after)`, materialize
snapshot (plus any RAM updates) into that new region, then write
`head(collection_id, collection_type, region_index)`.

7. `RegionHead -> InMemoryDirty`
Open a mutable frontier over the committed region basis and apply new
updates without requiring the full region contents to be loaded into
RAM first.

8. `EmptyHead | InMemoryDirty | WALSnapshotHead | RegionHead -> Dropped`
Write `drop_collection(collection_id)`.
Durable after the `drop_collection` record is durable. Any pending WAL
updates for that collection are discarded from the durable basis, the
collection leaves the live namespace, and no later WAL record for that
collection id is valid.

Collection format responsibility:

1. Each collection format defines how reads merge the durable basis
with the in-memory frontier.
2. The frontier must take precedence over older values in the durable
basis.
3. Flush to `RegionHead` materializes the logical state produced by
that merge.
4. Formats such as append-only logs or LSM-like structures may keep
only recent mutable state in RAM while older immutable state remains
in committed regions.
5. A `WALSnapshotHead` must be loadable into RAM before that
collection accepts further mutations.
6. For live user collections, the replay-tracked collection type is
fixed by the earliest retained type-bearing record for that collection
(`new_collection`, `snapshot`, or `head`). Historically this begins at
`new_collection`, but WAL reclaim may later remove that record.
7. Every later retained type-bearing record for that collection must
carry the same `collection_type`, otherwise replay must treat the
mismatch as corruption.
8. Per-region format evolution remains allowed because region headers
carry `collection_format` independently of the collection's stable
type.

Invariants:

1. The active durable basis for a collection is the last valid basis
decision in replay order, where a basis decision is
`new_collection`, `snapshot`,
`drop_collection`, or
`head(collection_id, collection_type, region_index)`.
2. `new_collection`, `snapshot`,
`drop_collection`, and
`head(collection_id, collection_type, region_index)` records totally
order durable basis decisions per collection.
3. Any `new_collection`, `update`, `snapshot`, or `head` older than the
active basis for that collection is reclaimable.
4. If the active basis for a collection is `drop_collection`, then that
collection is logically absent from the live namespace and any older
durable basis or update bytes for that collection are reclaimable once
they are no longer physically reachable. Any region associated with
that dropped collection may then be added to the free list if it is
not already in the free-list chain.
5. Historical append validity and retained replay basis are distinct:
`new_collection` is required before later user-collection records are
appended, but reclaim may later remove it so replay reconstructs from
the earliest retained basis record instead.

## Startup Replay Algorithm

Startup recovery reconstructs five things:

1. Durable collection states (live heads plus dropped tombstones)
2. In-memory working state for collections with uncommitted updates
3. Durable free-list head
4. Reserved `ready_region`, if an allocation was started but not yet
committed by `head` or `link`
5. Runtime `free_list_tail`, reconstructed from the free-pointer chain
after the durable free-list head is known

Algorithm:

1. Read `StorageMetadata` and validate static geometry (`region_size`,
`region_count`, `min_free_regions`, `erased_byte`,
`wal_write_granule`, `wal_record_magic`, and storage version support).
2. Scan all regions and collect candidate WAL regions
(`collection_id == 0`) with valid headers.
3. Select WAL tail as the WAL region with the largest valid sequence.
4. Read and validate the `WalRegionPrologue` stored at the start of the
tail region's user-data area, and use its `wal_head_region_index` as
the initial WAL-head candidate. Then scan valid records in that tail
region and let the last valid
`head(collection_id = 0, collection_type = wal, region_index)`
record override that candidate.
5. Walk the WAL region chain from the resulting WAL head to tail using
`link` records.
If a `link` is missing/invalid before reaching the known tail, return
an error (corrupted WAL chain).
If the known tail contains a trailing `link` whose target header is
missing/corrupt or has the wrong sequence, treat this as an incomplete
rotation. Use the known tail as replay tail.
For incomplete rotation recovery, if the known tail ends with a durable
`link(next_region_index, expected_sequence)` and the target WAL header is
missing/corrupt/wrong sequence, or the target `WalRegionPrologue` is
missing/corrupt, finish initializing the target region:
erase target region if needed, write a valid WAL header with
`collection_id = 0` and `sequence = expected_sequence`, then write a
valid `WalRegionPrologue` whose `wal_head_region_index` equals the WAL
head already determined for this WAL chain before the incomplete
rotation target is considered. Sync the initialized target region. If
this recovery init fails, startup fails with error. After successful
recovery init, use the target region as the active append tail.
6. Parse records in WAL order (region order, then offset order).
Record parsing begins only at offsets aligned to `wal_write_granule`
and greater than or equal to `wal_record_area_offset` within each WAL
region.
Maintain a replay-local flag `pending_wal_recovery_boundary`,
initially clear.
If an aligned candidate start byte equals `erased_byte`, treat that
slot as currently unwritten and stop scanning that WAL region.
If the aligned start byte equals `wal_record_magic`, parse the record.
If parsing or checksum validation fails, treat that aligned slot as a
corrupt/torn WAL slot, set `pending_wal_recovery_boundary`, and keep
scanning forward in aligned `wal_write_granule` steps.
If the aligned start byte is neither `erased_byte` nor
`wal_record_magic`, treat that aligned slot as corrupt/torn WAL bytes,
set `pending_wal_recovery_boundary`, and keep scanning forward in
aligned `wal_write_granule` steps. Do not attempt to decode or repair
those corrupt bytes.
If a later valid record is found while
`pending_wal_recovery_boundary` is set, that record must be
`wal_recovery`; otherwise return an error.
At the end of each reachable non-tail WAL region,
`pending_wal_recovery_boundary` must be clear; otherwise return an
error.
After scanning the tail region, recover the append point as the first
aligned slot whose first byte is `erased_byte` after the last valid
replayed tail record. If no such slot exists, the tail region is full.
7. Maintain replay state:
per collection optional live `collection_type`, `last_head`,
`basis_pos`, and
`pending_updates`, plus global `last_free_list_head`, optional
reserved `ready_region`, ordered pending region reclaims, and the
replay-local `pending_wal_recovery_boundary`.
Initialize `last_free_list_head` to `Some(1)` iff `region_count >= 2`,
otherwise `None`, because format establishes that as the initial
durable free-list head. Later `alloc_begin` and `free_list_head`
records override this baseline in replay order.
8. On `new_collection(collection_id, collection_type)`:
if `collection_id` is already tracked, return an error.
otherwise create replay state for that collection with durable basis
`EmptyHead`, set tracked `collection_type` from the record, set
`basis_pos` to this record's WAL position, and start with no pending
updates.
9. On `update(collection_id)`:
if `collection_id` is not tracked, return an error.
if that collection's durable `last_head` is `Dropped`, return an error.
append to `pending_updates` for that collection.
10. On `snapshot(collection_id, collection_type)`:
if `collection_id` is not tracked, create replay state for that
collection because an earlier `new_collection` may have been reclaimed,
and set tracked `collection_type` from this record.
if that collection's durable `last_head` is `Dropped`, return an error.
if this record's `collection_type` does not match the tracked
`collection_type`, return an error.
set durable `last_head` to this snapshot, set `basis_pos` to this
record's WAL position, and clear older pending updates for that
collection at WAL positions up to and including this snapshot.
11. On `alloc_begin(region_index, free_list_head_after)`:
if `ready_region` is already set, return an error because replay found
two unmatched allocation reservations.
if `last_free_list_head = none`, return an error because allocation
cannot consume an empty durable free list.
if `last_free_list_head != region_index`, return an error because
`alloc_begin` did not consume the current durable free-list head.
set durable `last_free_list_head` to `free_list_head_after`.
set `ready_region = region_index`.
12. On `head(collection_id, collection_type, region_index)`:
if `collection_id != 0` and `collection_id` is not tracked, create
replay state for that collection because an earlier `new_collection`
may have been reclaimed, and set tracked `collection_type` from this
record.
if `collection_id != 0` and that collection's durable `last_head` is
`Dropped`, return an error.
if `collection_id != 0` and this record's `collection_type` does not
match the tracked `collection_type`, return an error.
set durable `last_head` to that region, set `basis_pos` to this
record's WAL position, and clear WAL updates/snapshots older than this
basis decision.
if `ready_region = region_index`, clear `ready_region`;
otherwise leave `ready_region` unchanged because this `head`
retargeted the collection to an already allocated existing region.
13. On `link(next_region_index, expected_sequence)`:
if `ready_region = next_region_index`, clear `ready_region`.
otherwise return an error because the region was never reserved by
`alloc_begin`.
14. On `drop_collection(collection_id)`:
if `collection_id` is not tracked, create replay state for that
collection because older retained basis records may already have been
reclaimed; record this collection as durably `Dropped`, with no
retained live `collection_type`, set `basis_pos` to this record's WAL
position, and leave no pending updates.
otherwise if that collection's durable `last_head` is `Dropped`,
return an error.
otherwise set durable `last_head` to `Dropped`, set `basis_pos` to this
record's WAL position, and clear all pending updates for that
collection.
15. On `free_list_head(region_index_or_none)`:
set tentative durable `last_free_list_head` to `region_index_or_none`.
16. On `reclaim_begin(region_index)`:
append `region_index` to pending reclaims unless a later matching
`reclaim_end` removes it.
17. On `reclaim_end(region_index)`:
mark the matching pending reclaim as finished.
18. On `wal_recovery()`:
if `pending_wal_recovery_boundary` is clear, return an error.
otherwise clear `pending_wal_recovery_boundary`.
19. After replay, for each collection:
reconstruct its durable basis from `last_head`. If `last_head` is
`empty`, the basis is the empty collection declared by
`new_collection`; if that collection has post-basis updates,
initialize empty mutable state in RAM and apply those
`pending_updates` in WAL order. If `last_head` is `region`, the basis
may remain in-place in flash. If `last_head` is `wal_snapshot` and the
collection has post-basis updates, load that snapshot into RAM and
apply the remaining `pending_updates` in WAL order to reconstruct
mutable working state. If `last_head` is `wal_snapshot` and there are
no post-basis updates, the snapshot may remain dormant until the next
mutation, but it must be loaded into RAM before accepting that
mutation. If `last_head` is `Dropped`, do not reconstruct mutable
state for that collection and do not accept further mutations for that
collection id.
20. Initialize allocator state from `last_free_list_head`.
21. Reconstruct runtime `free_list_tail` by following free-pointer
links starting at `last_free_list_head` until reaching a free region
whose free-pointer slot is uninitialized.
If this walk encounters a malformed next pointer, a region that is not
a valid member of that free-list chain, or exceeds `region_count`
visited regions before reaching an uninitialized tail slot, return an
error because the
durable free-list head does not name a valid free-list chain.
If `last_free_list_head = none`, then `free_list_tail = none`.
22. If `ready_region` is set, hold it in memory as the next region to
use before consuming another free-list entry.
23. For each pending reclaim in WAL order:
if the target region is still reachable from any live collection head
or the WAL chain, leave it allocated because the reclaim did not reach
the detach point durably.
If the target region is unreachable from live state and not yet in the
free-list chain, complete the free-list append using the Region
Reclaim procedure.
If the target region is already reachable from the free-list chain,
finish the reclaim transaction by appending `reclaim_end(region_index)`.
24. If replay encountered a torn or checksum-invalid tail record,
retain all state recovered from earlier complete records. The WAL head
is unchanged. Replay may still recover and apply later valid tail
records that begin after the torn bytes, but the first such later valid
record must be `wal_recovery`. The recovered append point is the first
aligned slot whose first byte is `erased_byte` after the last valid
replayed tail record, so later WAL appends may resume there while the
ignored corrupt span before that point remains uninterpreted until that
region is reclaimed or erased for reuse.


## no_std Tracker Types (Rust)

The replay and allocator terms above map to the following explicit
`no_std` tracker state. These structs are runtime state, not on-disk
layout. Region references in tracker state are indexes into the
configured region array, not opaque identifiers.

```rust
#![no_std]

use heapless::Vec;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionIndex(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectionId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalSequence(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalOffset(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalPosition {
  pub region_index: RegionIndex,
  pub offset: WalOffset,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurableHead {
  Empty,
  Region { region_index: RegionIndex },
  WalSnapshot { wal_pos: WalPosition },
  Dropped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectionReplayState {
  pub collection_id: CollectionId,
  // `None` is used only for a retained drop-only tombstone whose older
  // type-bearing records were reclaimed.
  pub collection_type: Option<CollectionType>,
  pub last_head: DurableHead,
  // WAL position of the durable basis decision record that established
  // `last_head` (`new_collection`, `snapshot`, `drop_collection`, or `head`).
  pub basis_pos: WalPosition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingUpdateRef {
  pub collection_id: CollectionId,
  pub wal_pos: WalPosition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingReclaim {
  pub region_index: RegionIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FreeListTracker {
  // Durable allocator cursor reconstructed from replay decisions.
  pub last_free_list_head: Option<RegionIndex>,
  // Region reserved by `alloc_begin` but not yet consumed by a durable
  // `head` or `link` record.
  pub ready_region: Option<RegionIndex>,
  // Runtime-only convenience for append-on-free operations.
  pub free_list_tail: Option<RegionIndex>,
}

pub struct ReplayTracker<
  const MAX_COLLECTIONS: usize,
  const MAX_PENDING_UPDATES: usize,
  const MAX_PENDING_RECLAIMS: usize,
> {
  pub free_list: FreeListTracker,
  pub collections: Vec<CollectionReplayState, MAX_COLLECTIONS>,
  pub pending_updates: Vec<PendingUpdateRef, MAX_PENDING_UPDATES>,
  pub pending_reclaims: Vec<PendingReclaim, MAX_PENDING_RECLAIMS>,
}
```

`heapless` dependency form:

```toml
[dependencies]
heapless = { version = "0.8", default-features = false }
```

Field mapping to this spec:

1. `CollectionReplayState.last_head` maps to replay `last_head`,
including the durable `Dropped` tombstone state.
2. `WalPosition` identifies a WAL record by WAL region index plus
byte offset within that region.
3. `CollectionReplayState.basis_pos` is `B(c)`, the WAL position of
the durable basis decision record for that collection.
4. `CollectionReplayState.collection_type` is the replay-tracked
collection type established by the earliest retained valid
type-bearing record for that collection and validated by later
type-bearing records. It is `None` only for a drop-only retained
tombstone whose older type-bearing records were reclaimed.
5. `FreeListTracker.last_free_list_head` maps to replay
`last_free_list_head`.
6. `FreeListTracker.ready_region` maps to replay `ready_region`.
7. `FreeListTracker.free_list_tail` is runtime state reconstructed by
walking the free-pointer chain from `last_free_list_head`; reclaim uses
it to link `t_prev.next_tail = r`.
8. `ReplayTracker.pending_reclaims` maps to replay's ordered pending
region reclaims that remain incomplete after WAL replay and are
processed during post-replay recovery.

## WAL Reclaim Eligibility

Reclaim operates on WAL regions but correctness is defined per record.
A record is reclaimable only when replay no longer needs it to rebuild
the same `last_head`, `pending_updates`, `last_free_list_head`,
reserved `ready_region`, and ordered incomplete reclaim state.

Per-collection cutoff:

1. Let `H(c)` be the current durable logical head for collection `c`
(`EmptyHead`, `WalSnapshot`, `RegionHead`, or `Dropped`).
2. Let `D(c)` be the WAL position of the last durable basis decision
record for collection `c` (`new_collection`, `snapshot`,
`drop_collection`, or
`head(collection_id, collection_type, region_index)`).
3. `B(c) = D(c)` is the collection's durable basis position.

Per-record liveness rules:

1. `new_collection(collection_id, collection_type)` record:
live only if it is the basis decision at `D(c)` for a collection whose
logical head `H(c)` is `EmptyHead`; otherwise reclaimable.
2. `head(collection_id, collection_type, region_index)` record:
live only if it is the decision record at `D(c)` for a collection
whose logical head `H(c)` is a `RegionHead`; older `head(...)` records
are reclaimable.
3. `snapshot` record:
live only if it is the decision record at `D(c)` for a collection
whose logical head `H(c)` is a `WalSnapshot`; otherwise reclaimable.
4. `drop_collection(collection_id)` record:
live only if it is the decision record at `D(c)` for a collection
whose logical head `H(c)` is `Dropped`; older `drop_collection(...)`
records are reclaimable.
5. `update` record for collection `c`:
live only if its WAL position is greater than `B(c)`; updates at or
before `B(c)` are reclaimable.
6. `link` record:
live only while required to maintain a valid WAL chain from current
WAL head to current WAL tail.
7. `free_list_head(region_index_or_none)` record:
live only if it is the last valid explicit free-list-head decision in
replay order that has not been superseded by a later `alloc_begin` or
`free_list_head`.
8. `alloc_begin(region_index, free_list_head_after)` record:
live if either:
it is the last valid free-list-head decision in replay order; or
its reservation is still needed to recover unmatched `ready_region`.
It becomes reclaimable only after both of those properties are false.
9. `reclaim_begin(region_index)` record:
live only if replay still needs it to reconstruct an incomplete reclaim
transaction for `region_index` that would remain pending after replay.
If a later durable `reclaim_end(region_index)` closes that transaction,
or replay can prove the reclaim was unnecessary because the region
never became durably detached from live state, the `reclaim_begin`
record is reclaimable.
10. `reclaim_end(region_index)` record:
live only if replay still needs it to cancel a still-live
`reclaim_begin(region_index)` that would otherwise reconstruct as an
incomplete reclaim transaction. Once the matching `reclaim_begin`
becomes reclaimable, the matching `reclaim_end` is reclaimable too.
11. `wal_recovery` record:
live only if replay still needs it to justify later valid WAL records
that appear after an ignored corrupt/torn span in that WAL region.
Once those later dependent records are reclaimable or have been
superseded by newer durable state, the `wal_recovery` record is
reclaimable too.

WAL-region reclaim preconditions:

1. The candidate region is the head of the WAL.
2. For every live record in the candidate, an equivalent live state is
already represented durably outside the candidate (typically by newer
`snapshot`, `drop_collection`, or by
`head(collection_id, collection_type, region_index)` plus newer
updates).
3. After planned metadata updates, startup replay can still walk a
valid WAL chain from head to tail.

WAL-region reclaim postconditions:

1. No collection's `H(c)`, `B(c)`, or live post-basis updates depend on
bytes in the reclaimed region.
2. The recovered free-list head matches pre-reclaim allocator state.
3. The recovered `ready_region`, if any, matches pre-reclaim allocator
state.
4. The ordered set of incomplete reclaim transactions that replay would
continue matches pre-reclaim crash-recovery state.
5. WAL chain integrity remains valid (no broken `link` path).
6. The reclaimed region is erased before reuse.
7. If reclaim allocates any replacement WAL regions, replay-visible
`alloc_begin` records for those allocations carry
`free_list_head_after` so replay reconstructs the same allocator
position.

Safety invariant:

1. Reclaim must not change replay result: the recovered `last_head` and
`pending_updates` for every collection, the recovered
`last_free_list_head`, reserved `ready_region`, ordered incomplete
reclaim state, and reconstructed `free_list_tail`, after reclaim must
match the pre-reclaim logical state.

Example timeline (`collection_id = 7`):

1. WAL appends `update(u1)`, `update(u2)`.
2. WAL appends `snapshot(s1)`.
`u1` and `u2` are now reclaimable.
3. WAL appends `update(u3)`.
`u3` is live because it is after basis `B(7) = pos(s1)`.
4. WAL appends `alloc_begin(r44, free_list_head_after=f9)`.
5. Collection flushes to region `r44`, then WAL appends
`head(collection_id = 7, collection_type = T, region_index = r44)`.
Now `s1` and `u3` are reclaimable because
`head(collection_id = 7, collection_type = T, region_index = r44)` becomes
the new basis.

## Durability and Crash Semantics

Durability boundary:

1. A write is durable only after both:
the bytes are written, and a sync/flush that covers those bytes
completes.
2. Write ordering without sync ordering is not sufficient for
durability guarantees.
3. Replay must treat partially written records as torn and ignore
them using checksum validation and WAL tail recovery rules.

Notation:

1. `W(x)`: write bytes for `x`.
2. `S(x)`: sync/flush that guarantees durability for `x`.

Required write and sync ordering:

1. `update` durability:
`W(update_record) -> S(update_record) -> acknowledge update durable`.
2. `snapshot` head transition:
`W(snapshot(collection_id, collection_type, payload)) -> S(snapshot)`.
3. `drop_collection` transition:
`W(drop_collection(collection_id)) -> S(drop_collection)`.
4. `region` head transition:
`W(alloc_begin(region_index, free_list_head_after)) -> S(alloc_begin) -> erase/init reserved region if needed -> W(region header+data) -> S(region) -> W(head(collection_id, collection_type, ref=region_index)) -> S(head)`.
5. WAL rotation:
`W(alloc_begin(next_region_index, free_list_head_after)) -> S(alloc_begin) -> W(link(next_region_index, expected_sequence)) -> S(link) -> W(new_wal_region_init(sequence=expected_sequence, wal_head_region_index=current_wal_head)) -> S(new_wal_region_init)`.
6. Reclaim:
`W(reclaim_begin(region_index)) -> S(reclaim_begin) -> W(replacement_live_state_and_new_links) -> S(replacement_state) -> append old region to free list (write+sync) -> W(reclaim_end(region_index)) -> S(reclaim_end)`.
7. Resuming WAL appends after a recovered torn/corrupt tail record:
`W(wal_recovery()) -> S(wal_recovery) -> W(next_normal_wal_record) -> S(next_normal_wal_record)`.

General region-allocation rule:

1. Any operation that writes a newly allocated region must first make
`alloc_begin(region_index, free_list_head_after)` durable.
2. Erasing or initializing the reserved region is allowed only after
`S(alloc_begin)`.
3. If crash occurs after `S(alloc_begin)` but before a durable `head`
or `link` uses `region_index`, replay must preserve `region_index` as
`ready_region` and must not attempt to recover the old free-pointer
contents from flash.
4. Any allocation that is not itself part of reclaim or crash recovery
is invalid if consuming it would reduce the number of free regions
below `min_free_regions`.

Crash-cut outcomes:

1. Crash before `S(snapshot(collection_id, collection_type, payload))`:
snapshot may be missing/torn and is ignored.
2. Crash after `S(snapshot(collection_id, collection_type, payload))`:
snapshot transition is durable and acts as the collection WAL head.
3. Crash before `S(drop_collection(collection_id))`:
the collection drop may be missing/torn and is ignored.
4. Crash after `S(drop_collection(collection_id))`:
the collection is durably dropped and no later WAL record for that
collection id may be accepted.
5. Crash before `S(region)`:
new region is not considered durable.
If `alloc_begin` was already durable, replay still preserves the
reserved `ready_region`.
6. Crash after `S(region)` but before
`S(head(collection_id, collection_type, region_index))`:
region exists but is not committed as collection head.
The allocator advance remains durable because `alloc_begin` already
committed it, so replay keeps `region_index` reserved as `ready_region`
unless a later durable `head` consumes it.
7. Crash after `S(head(collection_id, collection_type, region_index))`:
region head transition is durable and consumes the reserved
`ready_region`.
8. Crash after `W(link)` but before `S(link)`:
link may be torn/missing and old tail remains active, but the reserved
region remains tracked by `alloc_begin`.
9. Crash after `S(link)` but before `S(new_wal_region_init)`:
startup validates the link target header sequence and
`WalRegionPrologue`; if the header is missing/corrupt/wrong sequence,
or the `WalRegionPrologue` is missing/corrupt, rotation is incomplete
and startup finishes initialization using `expected_sequence`.
10. Crash during tail-record write:
replay detects the torn/invalid tail record; earlier complete
records remain valid. Recovery ignores the torn record bytes and keeps
scanning in aligned `wal_write_granule` steps for later valid
`wal_record_magic` starts, so valid records written after the torn one
are still replayed. After open, the recovered append point is the first
aligned slot whose first byte is `erased_byte` after the last valid
replayed tail record. If later WAL appends resume after that recovered
append point, the first durable later record must be `wal_recovery()`.
An aligned tail slot whose first byte is still `erased_byte` is not a
torn record; it is an unwritten slot that marks end of the written
portion of the tail region.
11. Crash after `S(reclaim_begin)` but before the region is detached
from all live state:
startup sees an incomplete reclaim, but the region is still live and
must not be freed.
12. Crash after the region is detached from live state but before
`S(reclaim_end)`:
startup sees an incomplete reclaim and must complete the free-list
append idempotently if the region is not already free.

## Storage Metadata

```alloy
one sig StorageMetadata {
  storage_version: Int,
  region_size: Int,
  region_count: Int,
  min_free_regions: Int,
  erased_byte: Int,
  wal_write_granule: Int,
  wal_record_magic: Int,
}
```

The `StorageMetadata` struct describes the version of the storage as
well as the size of each region in bytes, the number of regions in the
database, the configured `min_free_regions` reserve, the erased-flash
byte value, the minimum writable granule used to align WAL records, and
the WAL record magic byte. The stored `wal_record_magic` must differ
from `erased_byte`.

## Header

```rust
struct Header {
  sequence: u64,
  collection_id: u64,
  collection_format: CollectionFormat,
  header_checksum: [u8; 32],
}
```

The `Header` is the first data in the region.

The `sequence` field is a monotonic value that is used to find the
newest header when the database is opened.

The `collection_id` defines which collection this region belongs to,
and is a stable 64-bit nonce, not a small reusable counter. The
`collection_format` defines the per-region encoding format for replay
and read semantics. This format may evolve across regions over time
without changing the collection's stable `collection_type`.

The `header_checksum` validates header integrity.

## WAL Region Prologue

```rust
struct WalRegionPrologue {
  wal_head_region_index: u32,
  prologue_checksum: [u8; 32],
}
```

`WalRegionPrologue` is present only in WAL regions (`collection_id = 0`)
and occupies the first bytes of the region user-data area immediately
after the region `Header`.

`wal_head_region_index` is the durable WAL head that was current when
that WAL region was initialized. It must name a region index strictly
less than `region_count`. If startup finishes an incomplete WAL
rotation by initializing a missing/corrupt target region, it must write
the same already-determined WAL head into this field rather than
choosing a new value during recovery.

`prologue_checksum` validates the logical prologue contents. It covers
`wal_head_region_index` in the same byte order used on disk.

Let `wal_record_area_offset` be the first offset within a WAL region
that is both greater than or equal to the end of `Header` plus
`WalRegionPrologue`, and aligned to `wal_write_granule`.
Replay scans candidate WAL record starts only at aligned offsets
greater than or equal to `wal_record_area_offset`, and new WAL appends
must begin at such offsets as well.

## Operations

### Init

Initialization is defined normatively by
`Format Storage (On-Disk Initialization)`. This section is informative
only.

### Format Storage (On-Disk Initialization)

Formatting creates a valid empty store that can be opened by normal
startup replay without special recovery paths.

Preconditions:

1. Backing storage is writable and erasable at region granularity.
2. `region_count >= 1`.
3. Region `0` is reserved as the initial WAL region.
4. `wal_write_granule >= 1`.
5. `wal_record_magic != erased_byte`.
6. `region_count >= 2 + min_free_regions`.
This guarantees that after reserving region `0` for the WAL and
preserving the configured `min_free_regions` reserve, a freshly
formatted store still has at least one non-reserved free region
available for ordinary allocations.

Procedure:

1. Erase metadata area and all data regions.
2. Write `StorageMetadata` (`storage_version`, `region_size`,
`region_count`, `min_free_regions`, `erased_byte`,
`wal_write_granule`, `wal_record_magic`) and sync metadata.
3. Initialize region `0` as WAL:
write valid `Header` with `collection_id = 0` and `sequence = 0`,
write a valid `WalRegionPrologue` with `wal_head_region_index = 0`,
then sync region `0`.
4. For each region `r` in `[1, region_count - 1]`:
leave any stale prior region contents uninterpreted, write
`r.free_pointer.next_tail` to the next region index (`r + 1`) for every
region except the last, leave the last region's free-pointer slot
uninitialized, and
sync `r`.
5. Formatting is complete only after metadata and all initialized
regions are durable.

Postconditions:

1. WAL head and WAL tail are both region `0`.
2. No user collection durable heads exist.
3. Free list contains every non-WAL region in ascending region-index
order.
4. Because region `0` is reserved as the WAL, the initial durable
free-list head is region `1` iff `region_count >= 2`; otherwise the
durable free list is empty.

### First Open After Fresh Format

Opening a freshly formatted store uses the same startup replay
algorithm as any other open.

Expected replay outcome on first open:

1. Region scan finds WAL tail at region `0` (`sequence = 0`).
2. WAL chain walk yields a single-region chain (`head = tail = 0`).
3. No `new_collection`, `update`, `snapshot`, `head`,
`drop_collection`, `link`, or `free_list_head` records are replayed.
4. Replay therefore yields:
no tracked user collections,
`pending_updates = empty`,
and durable `last_free_list_head = Some(1)` iff `region_count >= 2`,
otherwise `None`, inherited from the formatted initial free-list root.
5. Normal replay reconstruction then yields
`free_list.ready_region = None`,
`free_list.free_list_tail = Some(region_count - 1)` iff
`region_count >= 2`, otherwise `None`,
`collections = empty`,
and `pending_updates = empty`.

This is not a special-case bootstrap. Replay always starts with the
formatted initial durable free-list head and then applies later
`alloc_begin` / `free_list_head` decisions in WAL order. `free_list_tail`
is always reconstructed by walking the free-pointer chain from the
recovered durable free-list head; it is not found by scanning WAL
regions.

### Region Reclaim

Region reclaim appends a newly freed region to the tail of the free
list. If the free list was non-empty, reclaim must update the previous
tail region's `next_tail` pointer so the chain now ends at the newly
reclaimed region. Because reclaim removes a region from live metadata
before making it reachable from the free-list chain, it is always
modeled as a WAL-tracked transaction.

Normative append semantics:

1. Let `t_prev` be the value of `free_list_tail` before reclaim starts.
2. If `t_prev != none`, reclaim must durably write
`t_prev.free_pointer.next_tail = r` when freeing region `r`.
3. If `t_prev = none`, reclaim must not write any predecessor link and
must durably append `free_list_head(r)` and set `free_list_head = r`
and `free_list_tail = r`.
4. Reclaim is not complete until the predecessor-link write (when
required), or the `free_list_head(r)` record (when the free list was
empty), is durable; otherwise `r` is not yet a durable member of the
free list.

Preconditions:

1. `reclaim_begin(r)` is durable in the WAL before any live metadata is
updated to stop referencing `r`.
2. After the detach step, the reclaimed region `r` is no longer
reachable from any live collection head or live WAL state.
3. `r` is not already reachable from the free-list chain, unless this
procedure is being re-entered during crash recovery.
4. If a current free-list tail exists, call it `t_prev`.

Procedure:

1. Ensure `reclaim_begin(r)` is durable. On the initial reclaim
attempt this means append and sync `reclaim_begin(r)`. On recovery
re-entry the existing durable record satisfies this step.
2. Durably perform any collection-head or WAL-head updates needed so
that `r` has no remaining live references.
3. If recovery finds that `r` is already reachable from the free-list
chain, skip to step 10.
4. Erase region `r` before reuse.
5. Leave `r.free_pointer.next_tail` uninitialized so `r` is a valid
free-list tail once linked.
6. Sync `r` so its free-pointer state is durable before linking it.
7. If `t_prev` exists, write `t_prev.free_pointer.next_tail = r`.
This is the operation that links the previous free tail to the new
tail.
8. If `t_prev` exists, sync `t_prev` after writing `next_tail`.
9. If `t_prev` exists, update in-memory `free_list_tail = r`.
If no tail existed before step 7, append and sync `free_list_head(r)`,
then set both in-memory `free_list_head = r` and `free_list_tail = r`.
10. If recovery found `r` already reachable from the free-list chain,
update in-memory free-list state so it reflects `r` as the current
tail when needed.
11. Append and sync `reclaim_end(r)`.

Postconditions:

1. The free-list chain remains acyclic and FIFO-ordered.
2. Exactly one new region (`r`) is appended to the tail.
3. If a prior tail existed, its `next_tail` pointer now references
`r`.
4. `r.free_pointer.next_tail` remains uninitialized after reclaim.
5. If a prior tail existed, replay of free pointers follows
`... -> t_prev -> r`, and `r` is recognized as the tail because its
free-pointer slot is uninitialized.
6. If a prior tail existed, the only new durable predecessor link for
`r` is `t_prev.next_tail = r`, where `t_prev` is the free-list tail
from before reclaim.
7. Replay either finds a matching `reclaim_end(r)` or can safely
re-enter the procedure and derive the same result without duplicating
`r` in the free-list chain.

Crash-safety ordering requirement:

1. `reclaim_begin(r)` must be durable before any live metadata stops
referencing `r`.
2. `r` must be erased/initialized and synced before any durable write
that makes it reachable from `t_prev.next_tail`.
3. If `t_prev = none`, `free_list_head(r)` must be durable before
`reclaim_end(r)` is acknowledged.
4. If `t_prev` exists, the `t_prev.next_tail = r` write must be synced before
`reclaim_end(r)` is acknowledged.
5. The reclaim procedure must be idempotent across crashes between any
two steps above.
