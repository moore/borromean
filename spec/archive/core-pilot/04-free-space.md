# Free Space And Wear Leveling

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose and motivation

The free-space manager answers two questions that must remain consistent after
every crash: which physical region may be reused next, and which regions still
require erase before reuse? It also supplies native wear leveling by making
reuse order FIFO instead of repeatedly choosing a convenient low-numbered or
physically adjacent region.

A single mutable allocator snapshot would be a fragile recovery dependency.
V3 instead combines an immutable materialized basis with an append-only
frontier of later allocator facts. Checkpointing constructs a replacement basis
copy-on-write and publishes it only after every replacement segment is durable.

## Logical queue model

The allocator is one non-wrapping logical queue. Each entry has a monotonic
`FreeQueuePosition` and names one physical region. Physical region numbers may
repeat over the life of the store as a region is allocated, released, erased,
and appended at a later logical position; queue positions never repeat.

Three logical cursors describe the active interval:

- the **allocation position** names the oldest prepared entry not yet consumed;
- the **ready position** separates prepared entries from later dirty entries;
- the **append position** is the next position at which a release will add an
  entry.

Their order is `allocation <= ready <= append`. Allocation consumes only at the
allocation position. Erase maintenance processes the dirty entry at the ready
position and advances readiness only after the readiness record is synced.
Release appends at the end. These rules keep allocation and erase order FIFO
without searching the device for candidates.

The allocator also maintains a non-wrapping 64-bit allocation sequence. Each
allocation fact contains the next sequence, the region and purpose consuming
the queue entry, and `allocation_head_after`. The sequence is global to
allocator facts in the main WAL and every transaction log. Allocation holds one
global allocator lock while it derives the next head and sequence, appends and
syncs that fact, and advances the in-memory allocator state. The next allocation
may enter only after that state advances and the lock is released. This supplies
the total order that physical positions across several logs cannot provide. The
bootstrap basis records sequence `0` as its baseline, and the first allocation
uses sequence `1`.

### Queue and allocation requirements

1. `CORE-FREE-001` Free-space entries MUST be addressed by a monotonic logical
   `FreeQueuePosition` rather than a wrapping physical pair.
2. `CORE-FREE-008` Allocation MUST consume the oldest eligible prepared entry.
3. `CORE-FREE-009` A dirty entry MUST not become allocatable before erase and
   durable readiness publication.
4. `CORE-FREE-012` Every allocation fact MUST encode the allocated region,
   reserved purpose, allocation sequence number, and `allocation_head_after`.
5. `CORE-FREE-013` Allocation sequences MUST use one monotonically increasing,
   non-wrapping 64-bit namespace across the main WAL and every transaction log.
   An allocation MUST hold the global allocation lock until its fact is durable
   and its global in-memory head and sequence have advanced; only then may
   another allocation enter. Sequence exhaustion MUST be reported before
   durable I/O.

## Basis and frontier

A basis is an immutable ordered serialization of an exact half-open queue
interval `[start, end)`, together with the cursors needed to interpret which
entries are prepared, the current allocation head, and the greatest allocation
sequence incorporated by that head. The selected WAL-tail preamble supplies a
second allocator checkpoint. Recovery uses whichever consistent checkpoint has
the greater allocation sequence, then orders retained allocator facts with
later sequences from the main WAL and transaction logs. Thus a WAL preamble can
preserve cursor progress across WAL-head reclaim without pretending to be a
complete free-space basis.

Metadata locates and validates the bootstrap basis during format/open, while
the initial WAL selects it as the allocator's first retained basis. It anchors
recovery even if every later checkpoint attempt crashes. Its regions are
permanently reserved and are not part of wear accounting. It is never erased,
rewritten, or returned to the free queue.

The in-memory frontier is fixed-capacity caller-owned state. Before appending a
free, readiness, reservation, or allocation fact, the operation checks that
the complete durable fact can be represented. When the bound is too close,
foreground work returns checkpoint pressure before I/O rather than publishing
allocator history that startup cannot hold.

### Basis and frontier requirements

1. `CORE-FREE-002` A materialized basis MUST name the exact half-open logical
   queue interval it represents.
2. `CORE-FREE-003` The metadata-named bootstrap basis MUST remain immutable and
   permanently reserved in v3.
3. `CORE-FREE-007` The post-basis frontier MUST have a configured bound that is
   checked before any allocator record becomes durable.
4. `CORE-FREE-011` The initial WAL MUST select the metadata-located bootstrap
   basis as the allocator's first retained basis; later basis selection MUST
   come only from WAL replay.
5. `CORE-FREE-014` A free-space basis MUST checkpoint its allocation head and
   the greatest allocation sequence incorporated into that head. Recovery MUST
   compare the selected basis checkpoint with the selected WAL-tail preamble
   checkpoint, use the consistent pair with the greater allocation sequence as
   its baseline, order later retained allocation facts by sequence, and use
   `allocation_head_after` from the greatest later sequence as the recovered
   allocation head. A conflicting checkpoint, duplicate, gap, or invalid cursor
   transition MUST fail open with a typed corruption error.
6. `CORE-FREE-015` Allocation facts MUST remain reachable by recovery until a
   durably selected replacement basis or retained WAL preamble incorporates
   their allocator-head effects. Reclaim MUST separately retain any ownership,
   transaction, or cleanup evidence still supplied by those facts; transaction
   outcome MUST NOT hide an allocation that durably consumed a queue entry.

## Replacement-basis construction

Checkpointing uses these mechanical steps:

1. determine the unconsumed logical interval and the exact number of segments
   required to encode it;
2. reserve those segment regions through ordinary FIFO allocation, so basis
   traffic participates in wear leveling;
3. write and sync at most one immutable segment per unbudgeted maintenance
   step, with explicit ordered links between segments;
4. after every segment is durable, append and sync one WAL installation record
   naming the new root, interval, cursor positions, and generation;
5. apply the new basis selection in runtime and begin a fresh bounded frontier;
   and
6. later reclaim the old non-bootstrap basis through ordinary ordered frees
   once the retained roots prove it unreachable.

The old basis remains authoritative throughout steps 1–3. Durable but
unpublished replacement segments may be reclaimed after recovery; they never
replace the selected basis merely because their headers are present.

### Replacement-basis requirements

1. `CORE-FREE-004` Replacement basis segments MUST be allocated through the
   same FIFO policy as other recyclable structures.
2. `CORE-FREE-005` Every replacement segment MUST be written and synced before a
   WAL record publishes the replacement root.
3. `CORE-FREE-006` The old basis MUST remain live until ordinary reclaim proves
   it unreachable and appends its ordered dirty frees.

## Wear behavior

Every recyclable system and collection allocation consumes the oldest eligible
prepared queue entry. Physical adjacency across allocations is neither
required nor preferred over FIFO order. In a controlled cycle where the same
set of regions remains equally eligible, this produces allocation and erase
counts that differ by at most one. Pinned live objects and permanent bootstrap
regions are excluded because they are not eligible for reuse.

Erase eligibility comes from retained WAL state: release removes ownership and
places the region at the queue's dirty cursor. The current WAL tail remains
published ownership, so it cannot enter the dirty queue while it is the tail.

### Wear requirement

1. `CORE-FREE-010` In a controlled full cycle, equally eligible recyclable
   regions MUST differ by at most one allocation/erase cycle; live pinned and
   permanently reserved bootstrap regions are excluded.
