# Chapter 1: Theory of Operation

This chapter describes the design problem and the core Borromean model:
regions provide erase granularity, bounded memory holds mutable
frontiers, and the WAL records every durable state transition needed to
recover after reset.

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

## Design Principles

Borromean is built from a small set of mutually reinforcing choices:

- Region alignment makes every durable object reclaimable without
  rewriting neighboring data.
- Log-structured collection state avoids hot stable locations and gives
  reclaim a clear oldest-first direction.
- The WAL serializes collection, allocator, reclaim, and WAL-chain
  decisions into one replay order.
- Checkpointing partially filled frontiers to the WAL lets the store
  support more live collections than available in-memory frontier
  buffers, within the configured collection limit.
- Every region allocation is made durable before use in a transaction
  or privileged storage-core operation, so a reset cannot lose a
  consumed ready entry.
- Multi-step collection changes use WAL transactions so replay can
  distinguish uncommitted updates from committed updates that still
  need allocator cleanup.

## Overview

To solve these challenges, Borromean divides flash into equal-size
regions. Region starts and sizes must be aligned to the backing
flash's erase-block size so every region can be erased independently.
Collections are log-structured rather than updated in place. New
durable collection state is written to fresh WAL records or fresh
committed regions, and old committed regions become reclaimable only
after a newer durable basis no longer references them. For each
collection, Borromean tracks a stable collection id and the latest
durable basis selected by replay.

Collection updates accumulate in bounded in-memory frontiers, but a
normal mutation is appended and synced to the global write-ahead log
(WAL) before the corresponding frontier is advanced to represent the
current collection state.
Per-collection WAL entries contain a stable collection id and bytes
whose meaning is defined by the corresponding collection-specific
specification; those bytes are opaque to Borromean core. Collection ids
are opaque 64-bit nonces that are assigned when
a collection is created by `new_collection(collection_id,
collection_type)`. Collection id `0` is reserved for storage-private
state; all user collection ids are nonzero and are not recycled.
Borromean core also reserves `collection_type = main_wal_v2` and
`collection_type = free_space_v2` for `collection_id = 0`; user
collections must not use those collection types.

A collection may be removed durably by appending
`drop_collection(collection_id)` to the WAL. Once that record is
durable, the collection is no longer live, no later WAL record for
that collection id is valid, and its older durable bytes may be
reclaimed once they are no longer physically reachable from live
storage state.

A collection head may refer either to a committed region or to a
WAL-resident snapshot. The data payload in each committed region is
defined by the corresponding collection specification. For user
collections, append-time validity requires a successful
`new_collection(collection_id, collection_type)` before any later record
for that collection may be appended. WAL reclaim may later remove that
`new_collection` record once a newer durable basis for the collection
survives elsewhere in the WAL or in committed regions. Replay therefore
distinguishes historical validity from retained basis: after reclaim,
the earliest retained basis record for a user collection may be
`snapshot`, `head`, or `drop_collection` even though `new_collection`
was required at initialization.

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

A collection can checkpoint a dirty frontier as a partial state snapshot
in the WAL or flush it into collection-defined committed state. A WAL
snapshot is a durable staging point: when that collection is mutated
again, the snapshot is loaded into RAM, later mutations are still
appended to the WAL as `update` records, and the in-memory state can
accumulate enough change to eventually justify writing committed
regions. WAL snapshots avoid many underfilled committed regions because
partial state can be intermixed with other WAL entries and collected
when stale. They also let the store support more live collections than
resident in-memory frontier buffers, within the configured collection
limit.

Each collection's storage-managed resident update frontier has exactly
the committed-region payload capacity for the configured region size:
the region size minus the region header and any collection-format
overhead. That keeps dirty frontier memory aligned with the amount of
collection data that can be written to one committed region and
prevents undersized resident buffers from causing avoidable region
underutilization. If applying another update would overflow that
frontier, the implementation flushes the current logical frontier into
collection-defined committed state, commits a new durable head, clears
the in-memory frontier, and continues accepting later updates into RAM
over the new committed head. Collections therefore remain
log-structured: a flush creates new immutable committed state instead of
rewriting existing live committed state in place.

In a completed WAL rotation, the last record of the old WAL tail is
`link(next_region_index, expected_sequence)`, which points to the next
WAL region. A crash may leave an incomplete rotation whose durable
tail ends earlier; startup recovery finishes that rotation before
resuming normal appends.

A WAL region can be reclaimed when the number of live records drops
below a configurable threshold. During reclaim, the implementation
preserves replay-visible state by copying any WAL-resident basis or
update records that must survive the reclaimed region into the current
WAL tail, rotating to a new tail region first if needed. Here "WAL head"
means the logical oldest live WAL region in the chain; new WAL records
are always appended at the WAL tail.

If reclaim advances the WAL head, a normal
`head(collection_id = 0, collection_type = main_wal_v2, region_index =
new_head)` control record is appended in the current WAL tail before
the old WAL head becomes unreachable. The WAL does not have a separate
WAL-only head-record type; it uses the same `head` record as every
other collection.

Multi-step collection operations that replace durable state use
transaction-log-backed transactions. A transaction begins with a main
WAL `begin_transaction(transaction_log_id, start)` record, then writes
durable allocation entries and private suffix entries into that
transaction log. Allocation entries advance allocator recovery state
immediately but remain transaction-owned. Private collection data and
free intents remain non-visible until their suffix range is sealed and
imported by `commit_transaction(transaction_log_id, range, seal)`.
Before that commit, replay can abandon private collection state and
recover transaction-owned allocation effects. After commit, replay
imports the sealed transaction-log suffix ranges at the main WAL commit
position, keeps the new collection state, and finishes any allocator
cleanup. The transaction is complete only after
`transaction_finished(transaction_log_id, range)` is durable; if
pre-commit recovery has already cleaned up an abandoned transaction,
replay records that fact with
`rollback_transaction(transaction_log_id, range)`.

This rollback contract is intentionally limited to volatile collection state
and regions allocated by the transaction. A collection transaction may update
in-memory private frontier state and may write transaction-owned regions
because rollback can discard the former and return the latter to the dirty
free-space range. It must not durably mutate a preexisting collection-owned
region unless the collection defines an explicit durable undo protocol for that
mutation. Otherwise a rollback could not distinguish the region's public
pre-transaction contents from private uncommitted contents.

The storage system also keeps a storage-private free-space collection
of regions that are available to satisfy new allocations. It is a WAL
sub-collection under `collection_id = 0` and
`collection_type = free_space_v2`, with the same basis/frontier shape
as ordinary collections but storage-defined update records. Its durable
basis may be the initial formatted state, a retained
`snapshot(collection_id = 0, collection_type = free_space_v2, ...)`
record, or a retained
`head(collection_id = 0, collection_type = free_space_v2, ...)` record
that points at a materialized `free_space_v2` metadata chain. WAL
snapshots and materialized metadata are alternative durable bases;
neither requires the other to be written first. The post-basis frontier
is made of retained allocator records. Free regions themselves do not
store allocator links.

The free-space collection has three replay-visible cursors:
`allocation_head`, `ready_boundary`, and `append_tail`, with the
invariant `allocation_head <= ready_boundary <= append_tail`. Entries
before `allocation_head` are no longer free. Entries from
`allocation_head` up to `ready_boundary` name erased ready regions that
may be allocated without running erase inline. Entries from
`ready_boundary` up to `append_tail` name dirty regions that must be
erased before they can be allocated.

The free-space collection is FIFO (first in, first out), to support
wear leveling. Freeing a region appends
`free_region(region_index, append_tail_after)`, which adds a detached
region at `append_tail` as dirty. Erase maintenance erases a bounded
dirty span and then appends
`erase_free_region_span(count, ready_boundary_after)`, which publishes
the ready-boundary advance. Allocation appends
`allocate_region(region_index, allocation_head_after)`, which consumes
the current ready entry and reserves that region. These commands are
self-checking cursor transitions in the free-space frontier: replay
validates the named region or cursor update against the current
free-space collection state before applying it.

Ordinary user/data allocations are transaction-owned. If the enclosing
transaction commits, the allocator pop and collection state update
become visible together. If it rolls back, any reserved region that may
have been written is returned to the dirty range. Storage-core
allocations needed to run WAL rotation, transaction-log growth, or
allocator maintenance may use privileged non-ordinary paths, but those
paths must preserve a ready-region reserve.

Borromean must also maintain a configured `min_free_regions` reserve.
Let `max_in_memory_dirty_collections` be the maximum number of dirty
collections that may simultaneously have in-memory working state.
This reserve calculation relies on each storage-managed dirty frontier
having exactly one committed-region payload of usable capacity. Each
such dirty in-memory collection must be preservable using at most one
newly allocated region before reclaim frees any region: either by
writing a WAL snapshot if that snapshot fits in the available WAL space,
or by writing a normal collection region instead if the snapshot would
not fit efficiently in the WAL.
Under that assumption, `min_free_regions` must be at least
`max_in_memory_dirty_collections + 1`. The extra `+1` region is
reserved so WAL rotation, reclaim bookkeeping, or crash recovery can
still make forward progress before the first region is freed.
While the ready range contains at most the configured ready-region
reserve, ordinary foreground mutations must not be accepted unless they
are part of space-recovery work: operations that make regions
reclaimable, erase dirty free-space entries, or complete reclaim. If
accepting an ordinary foreground mutation would leave the store at or
below the reserve, the implementation must first attempt such
space-recovery work. If space-recovery operations cannot restore enough
ready entries, the database must be treated as full for purposes of
accepting further ordinary writes. At that point, more drastic action
such as dropping or truncating collections, erasing dirty free-space
entries, or migrating/reformatting onto a larger backing store is
required before additional ordinary writes may be accepted.

## Design Constraints

The flash constraint driving the design is that repeatedly rewriting a
small set of stable locations would wear those regions out before the
rest of the backing media. Borromean therefore treats stable state as
log-structured state: collection heads, allocator decisions, WAL-chain
movement, and transaction phase markers are all represented by
append-only facts.

Oldest-first freeing is necessary for wear leveling, but Borromean
cannot reclaim old bytes merely because they are old. A region remains
live while a collection basis, collection-defined region reference,
WAL-chain link, storage-core private allocation reservation, open
transaction, or free-space collection entry can still require it during
replay. The rest of this specification defines the operations that make
those reachability decisions explicit.

This is why the WAL is collection `0`: it is the single replay order for
state that would otherwise need stable mutable roots. Startup finds the
WAL tail by sequence number, derives the effective WAL head from the
tail prologue and tail-local WAL-head control records, then replays
retained records to rebuild collection state, allocator state,
transaction recovery state, and recovery boundaries.

## Core Requirements

1. `RING-CORE-001` Region starts and region sizes MUST be aligned to
the backing flash erase-block size so every region can be erased
independently.
2. `RING-CORE-002` Each collection MUST be represented as
log-structured state: new durable collection state is written to WAL
records or fresh committed regions, and live committed collection
regions MUST NOT be rewritten in place.
3. `RING-CORE-003` Borromean MUST reserve `collection_id = 0` for
storage-private state, and all user collection identifiers MUST be
nonzero stable 64-bit nonces that are never recycled.
4. `RING-CORE-004` Borromean core MUST reserve
`collection_type = main_wal_v2` and
`collection_type = free_space_v2` for `collection_id = 0`, and user
collections MUST NOT use those collection types.
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
`head(collection_id = 0, collection_type = main_wal_v2,
region_index = ...)` records rather than a WAL-specific head record
type.
9. `RING-CORE-009` Any multi-step collection operation that commits a
new durable basis and frees old regions MUST be tracked as either a
bounded inline transaction or a transaction-log-backed transaction with
durable begin, commit, cleanup, and terminal markers.
10. `RING-CORE-010` The storage-private free-space collection MUST be a
`collection_id = 0`, `collection_type = free_space_v2` WAL
sub-collection whose retained state is the latest free-space basis plus
later allocator update records. It MUST be FIFO so allocations consume
the oldest ready free regions first.
11. `RING-CORE-011` Any operation that writes a newly allocated region
MUST first durably reserve that region with
`allocate_region(region_index, allocation_head_after)` in an enclosing
transaction or privileged storage-core operation.
12. `RING-CORE-012` `free_region`,
`erase_free_region_span`, and `allocate_region` records MUST be
self-checking cursor transitions over the free-space collection, and
replay MUST reject any allocator command whose region or cursor update
does not match the current replayed free-space collection state.
13. `RING-CORE-013` The implementation MUST maintain
`min_free_regions >= max_in_memory_dirty_collections + 1` so every
storage-managed dirty frontier can be preserved using one committed
region while one additional region remains reserved for WAL rotation,
transaction terminal records, or crash recovery.
14. `RING-CORE-014` While the ready range contains at most the
configured ready-region reserve, ordinary foreground mutations MUST NOT
be accepted unless they are part of a space-recovery operation that
makes regions reclaimable, erases dirty free-space entries, or completes
reclaim.
15. `RING-CORE-015` If space-recovery operations cannot restore enough
ready entries, the database MUST treat ordinary writes as out of space
until space is freed, dirty entries are erased, or the store is
migrated.
16. `RING-CORE-016` Each storage-managed resident mutable collection
frontier MUST have usable byte capacity exactly equal to the
committed-region payload capacity of one configured durable region.
17. `RING-CORE-016` If applying another update would exceed that
capacity, the implementation MUST flush the collection's current
logical frontier into collection-defined committed state, durably commit
a new collection head, and clear the in-memory frontier before accepting
further updates for that collection.
18. `RING-CORE-016A` If a single update cannot fit in an empty mutable
frontier buffer, the implementation MUST report explicit buffer
exhaustion instead of flushing an empty frontier.
19. `RING-CORE-017` After such a frontier-capacity flush, later updates
for that collection MUST accumulate in a fresh in-memory frontier
layered over the newly committed collection head.
