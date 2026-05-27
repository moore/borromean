# Chapter 1: Theory Of Operation

This chapter describes the design problem and the core borromean model:
regions provide erase granularity, bounded memory holds mutable
frontiers, and the WAL records every durable state transition needed to
recover after reset.

## Design Proof Outline

Borromean is built from a small set of mutually reinforcing choices:

- Region alignment makes every durable object reclaimable without
  rewriting neighboring data.
- Append-only collection state avoids hot stable locations and gives
  reclaim a clear oldest-first direction.
- The WAL serializes collection, allocator, reclaim, and WAL-chain
  decisions into one replay order.
- Bounded frontiers let many collections stay open while still forcing
  large or old frontiers into snapshots or committed regions.
- Every region allocation is made durable before use, so a reset cannot
  lose a removed free-list head.
- Every region free is bracketed by reclaim records, so a reset can
  complete or discard incomplete free-list work without duplicating a
  region.

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
Per-collection WAL entries contain a stable collection id and bytes
whose meaning is defined by the corresponding collection-specific
specification; those bytes are opaque to borromean core. Collection ids
are opaque 64-bit nonces that are assigned when
a collection is created by `new_collection(collection_id,
collection_type)`. Collection
id `0` is reserved for the WAL; all user collection ids are nonzero
and are not recycled. Borromean core also reserves
`collection_type = wal` for `collection_id = 0`; user collections must
not use that collection type.

A collection may be removed durably by appending
`drop_collection(collection_id)` to the WAL. Once that record is
durable, the collection is no longer live, no later WAL record for
that collection id is valid, and its older durable bytes may be
reclaimed once they are no longer physically reachable from live
storage state.

A collection head may refer either to a committed region or to a
WAL-resident snapshot. The data payload in each committed region is
defined by the corresponding collection specification. Some collection
formats may use that committed region as a manifest whose payload names
additional committed regions that remain live collection state. For
user collections, append-time validity requires a successful
`new_collection(collection_id, collection_type)` before any later record
for that collection may be appended. WAL reclaim may later remove that
`new_collection` record once a newer durable basis for the collection
survives elsewhere in the WAL or in committed regions. Replay therefore
distinguishes historical validity from retained basis: after reclaim,
the earliest retained basis record for a user collection may be
`snapshot`, `head`, or `drop_collection` even though `new_collection`
was required historically.

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
unbounded number of collections. However, each collection's mutable
in-memory update frontier is bounded. If applying another update would
overflow that frontier, the implementation flushes the current logical
frontier into collection-defined committed state, commits a new durable
head, clears the in-memory frontier, and continues accepting later
updates into RAM over the new committed head. For simple collections the
head may be the newly written data region itself. For manifest-based
collections the head may be a manifest region that makes one or more
data-region segments live. Collections therefore remain log-structured:
a flush creates new immutable committed region state, analogous to an
LSM SSTable, instead of rewriting existing live region state in place.

In a completed WAL rotation, the last record of the old WAL tail is
`link(next_region_index, expected_sequence)`, which points to the next
WAL region. A crash may leave an incomplete rotation whose durable
tail ends earlier; startup recovery finishes that rotation before
resuming normal appends.

A WAL region can be reclaimed when the number of live records drops
below a configurable threshold. During reclaim, we write the current
live state for each affected uncommitted collection into a new WAL
region by snapshotting that collection into the current WAL tail
region, rotating to a new tail region first if needed. If a
collection's data is not in memory, that implies its current snapshot
is already in the WAL. If a current snapshot is in the region being
collected, it can be copied directly to the WAL tail while updating
the head pointer to the new location. Here "WAL head" means the
logical oldest live WAL region in the chain; new WAL records are always
appended at the WAL tail.

Once collection data is flushed from a WAL head being reclaimed, any
current user-collection basis records that must remain live are
rewritten to the WAL tail. If reclaim advances the WAL head, a normal
`head(collection_id = 0, collection_type = wal, region_index =
new_head)` control record is appended in the current WAL tail pointing
to the new WAL head, and the old WAL head is added to the free list.
Startup step 4 derives the WAL head only from the current tail
region's `WalRegionPrologue` plus the last valid tail-local
`head(collection_id = 0, ...)` override, so reclaim and rotation must
preserve the effective WAL head in one of those two forms before the
older representation becomes unreachable. The WAL does not have a
separate WAL-only head-record type; it uses the same `head` record as
every other collection.

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
record. Any operation that writes a newly allocated region must first
durably reserve that region with
`alloc_begin(region_index, free_list_head_after)`. The later `head`,
`link`, or `stage_region` record that uses that region consumes the
single ready-region reservation. A staged region remains allocated, but
it is not live collection state unless a later committed collection
format, such as a manifest, references it. That reservation exists to
prevent a free region from being leaked across a crash between
allocation and consumption; once the region has been durably consumed,
replay no longer needs the historical `alloc_begin` for
region-consumption validity.

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

## Design Constraints

The flash constraint driving the design is that repeatedly rewriting a
small set of stable locations would wear those regions out before the
rest of the backing media. Borromean therefore treats stable state as
log-structured state: collection heads, allocator decisions, WAL-chain
movement, and reclaim bookkeeping are all represented by append-only
facts.

Oldest-first freeing is necessary for wear leveling, but borromean
cannot reclaim old bytes merely because they are old. A region remains
live while a collection basis, manifest, WAL-chain link, ready-region
reservation, staged-region entry, pending reclaim, or free-list link can
still require it during replay. The rest of this specification defines
the operations that make those reachability decisions explicit.

This is why the WAL is collection `0`: it is the single replay order for
state that would otherwise need stable mutable roots. Startup finds the
WAL tail by sequence number, derives the effective WAL head from the
tail prologue and tail-local WAL-head control records, then replays
retained records to rebuild collection state, allocator state, staged
regions, pending reclaims, and recovery boundaries.

## Core Requirements

1. `RING-CORE-001` Region starts and region sizes MUST be aligned to
the backing flash erase-block size so every region can be erased
independently.
2. `RING-CORE-002` Each collection MUST be implemented as an
append-only data structure whose new writes are added to the head
region and whose storage can only be freed by truncating the tail.
3. `RING-CORE-003` Borromean MUST reserve `collection_id = 0` for the
WAL, and all user collection identifiers MUST be nonzero stable 64-bit
nonces that are never recycled.
4. `RING-CORE-004` Borromean core MUST reserve
`collection_type = wal` for `collection_id = 0`, and user collections
MUST NOT use that collection type.
5. `RING-CORE-005` For user collections, append-time validity MUST
require a successful earlier
`new_collection(collection_id, collection_type)` before any later
record for that collection may be appended.
6. `RING-CORE-006` For a live user collection, the earliest retained
type-bearing record seen during replay MUST establish the
replay-tracked `collection_type`, and every later valid type-bearing
record for that collection MUST carry the same `collection_type`.
7. `RING-CORE-007` A `drop_collection(collection_id)` record that is
durable MUST tombstone that collection, MUST forbid later WAL records
for that `collection_id`, and MUST make older durable bytes reclaimable
once they are no longer physically reachable from live state.
8. `RING-CORE-008` Borromean MUST model WAL-head movement as ordinary
`head(collection_id = 0, collection_type = wal, region_index = ...)`
records rather than a WAL-specific head record type.
9. `RING-CORE-009` Any reclaim that frees a region MUST be tracked as a
WAL transaction bounded by durable `reclaim_begin(region_index)` and
`reclaim_end(region_index)` records.
10. `RING-CORE-010` The durable free list MUST be FIFO so allocations
consume the oldest free regions first.
11. `RING-CORE-011` Any operation that writes a newly allocated region
MUST first durably reserve that region with
`alloc_begin(region_index, free_list_head_after)`.
12. `RING-CORE-012` The implementation MUST maintain
`min_free_regions >= max_in_memory_dirty_collections + 1`.
13. `RING-CORE-013` Ordinary foreground allocations MUST NOT consume
the last `min_free_regions` free regions.
14. `RING-CORE-014` If reclaim cannot restore at least
`min_free_regions` free regions, the database MUST treat ordinary
writes as out of space until space is freed or the store is migrated.
15. `RING-CORE-015` Each collection's mutable in-memory update frontier
MUST have a bounded configured capacity.
16. `RING-CORE-016` If applying another update would exceed that
capacity, the implementation MUST flush the collection's current
logical frontier into collection-defined committed state, durably commit
a new collection head, and clear the in-memory frontier before accepting
further updates for that collection.
17. `RING-CORE-017` After such a frontier-capacity flush, later updates
for that collection MUST accumulate in a fresh in-memory frontier
layered over the newly committed collection head.
