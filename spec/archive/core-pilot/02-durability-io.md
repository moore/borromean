# Device Durability And I/O Contracts

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose and motivation

Crash correctness depends on an explicit contract between logical state
transitions and the storage device. Performance predictability depends on that
same boundary: a logical append that is one span at the core can become several
backend media-write calls because of transfer limits, while a sync remains a
visible durability phase. This document defines both what a crash may preserve
and how much physical work an operation may request.

## Device and durability model

The blocking raw-device contract exposes geometry, byte-range reads and
writes, whole-region erase, and an explicit `sync` barrier. Geometry includes
region count and size, erased byte, read and media-write alignment, and maximum
transfer lengths. The core owns all v3 formatting and encoding above this raw
interface. At a flash backend, this raw write is the operation hardware APIs
often call “program.”

Media writes and erases initially change an uncommitted device image. After a
successful `sync`, every preceding operation in that durability phase is part
of the recoverable image. If power is lost before a successful sync, recovery
must accept every allowed backend outcome: the new bytes may be absent,
complete, or torn at an allowed primitive-write boundary. In an open-ended
append stream, record separators, byte stuffing, aligned extents, lengths, and
individual checksums distinguish complete append-framed records from unwritten
or unusable spans. A bounded bulk materialization may instead use its enclosing
lengths and integrity scheme; its entries are not append-framed merely because
they are stored sequentially.

A publication record is a durable fact that makes another object reachable.
The target must already be complete and synced before its publication is
synced. Runtime state deliberately changes after that durable publication, not
before it. If power fails between the sync and runtime apply, replay observes
the publication and performs the missing pure transition. This ordering makes
the durable record the recovery authority.

Published control structures are append-only or copy-on-write. Rewriting a
live allocator basis, WAL segment, manifest, or root in place would create a
crash state in which neither the old nor new value is trustworthy.

### Durability requirements

1. `CORE-DUR-001` A media write becomes durable only after a successful covering
   sync.
2. `CORE-DUR-002` Without an intervening sync, recovery MUST tolerate a write
   being absent, complete, or torn at an allowed primitive-write boundary.
3. `CORE-DUR-003` The core MUST prepare and sync a new object before syncing a
   record that publishes a reference to it.
4. `CORE-DUR-004` Runtime state MUST be updated only after the corresponding
   publication record is durable.
5. `CORE-DUR-005` A live published control structure MUST NOT be erased or
   overwritten in place.

## I/O accounting model

Specifications count two levels of I/O:

- a **logical span** is one contiguous range requested by the core; and
- a **primitive operation** is one backend call after geometry-driven
  splitting.

Tests record both. Transfer limits explain primitive count; they do not permit
the core to issue scattered logical spans where one sequential span is
required. Search and memory bounds name the configuration constant that limits
them rather than relying on an informal expectation that data remains small.

A budget is always a measurable resource limit: for example, a maximum number
of reads or syncs, maximum bytes transferred, maximum search steps, maximum
scratch memory, or a time limit where an environment-specific specification can
make one meaningful. The operation set separately names which actions exist or
are permitted. Adding a new operation type is not itself adding or changing a
budget.

Before the first media write, erase, or sync, a state-changing operation
preflights the encoded and granule-rounded append size where applicable, WAL
successor-link space, reservation capacity, scratch memory, and any other
resource needed to reach its publication point. A capacity or lock failure
therefore leaves no new media fact.

### Accounting and preflight requirements

1. `CORE-IO-008` Backends that split a logical span MUST expose transfer limits
   so primitive-operation bounds can be normalized from geometry.
2. `CORE-IO-009` Each operation MUST name the configured quantity controlling
   every non-constant read, search, write, and memory bound.
3. `CORE-IO-003` A write rejected for a lock conflict MUST issue no raw-device
   operation; a write rejected for capacity pressure MUST issue no media write,
   erase, or sync.

## Baseline operation patterns

These are the default physical I/O patterns and limits. Component and
collection specifications may make them tighter but may not silently make them
looser.

| Operation | Physical pattern |
| --- | --- |
| Ordinary read | Reads the selected committed root: a resident frontier when present, otherwise the later snapshot or `head` basis; no media write, erase, or sync |
| Transaction-aware read | Reads the ordinary committed view plus its bounded private overlay |
| Hot inline mutation | One contiguous WAL append batch and exactly one sync |
| Locked write | Return the lock error before any raw-device operation |
| Insufficient capacity | Return maintenance pressure before any media write, erase, or sync |
| Region publication | At most reservation, sequential-content, and publication phases |
| WAL rotation | Consume a spare with a synced WAL-purpose region header but no valid preamble; sync the predecessor link, then publish the next-WAL-sequence preamble containing the retained head and allocator checkpoint with the next normal publication |
| Allocation fact | Hold the global allocation lock; append the region, purpose, allocation sequence, and `allocation_head_after`; sync once; advance runtime allocator state; release the lock |
| Logical free | Append ordered free facts and sync once; no erase |
| Transaction commit | Append one decision batch and sync once; no cleanup search |
| Transaction rollback | Append one rollback decision batch and sync once; no cleanup search |
| Erase maintenance | Erase one dirty region, append readiness, and sync once |
| Basis construction | Write and sync at most one immutable replacement segment per step |
| Basis publication | Append and sync one installation record after all segments are durable |
| WAL snapshot | Serialize one bounded collection frontier as one append-framed snapshot and sync once; allocate no data region when it fits the declared WAL budget |
| Collection materialization | Reorder or pack one bounded memory frontier, write the bounded result using the collection format's declared spans and limits, then publish it |
| Startup | Metadata and one fixed header per region; validate preambles for collection-ID-`0`, system-type-`WAL` candidates; fail open on duplicate valid WAL sequences; select the largest valid WAL sequence, obtain its preamble's retained head and allocator checkpoint, replay head-to-tail, order later retained allocation facts by allocation sequence, then follow only structures selected by replay |
| Cleanup or reclaim | Follow a retained cursor and caller-supplied budget; WAL-head reclaim must first publish an allocator checkpoint covering allocation facts in the excluded prefix |

Foreground operations may consume already prepared capacity. Erase, reclaim,
basis checkpointing, transaction cleanup, collection snapshot, collection
flush, and compaction are separate caller-visible maintenance because hiding
them would destroy latency and write-amplification bounds.

### Foreground operation requirements

1. `CORE-IO-001` A hot inline mutation MUST issue one contiguous WAL append
   batch and exactly one publication sync.
2. `CORE-IO-002` An ordinary read MUST issue no write, erase, or sync.
3. `CORE-IO-004` Publishing a new data region MUST use at most three durability
   phases: reservation, bounded content, and publication.
4. `CORE-IO-005` Foreground log rotation MUST consume a prepared, synced spare
   and add at most one sync beyond normal publication.
5. `CORE-IO-006` Logical free MUST append ordered free facts and sync once; it
   MUST NOT erase.
6. `CORE-IO-007` Foreground operations MUST NOT hide erase, reclaim, basis
   checkpoint, transaction cleanup, collection snapshot, collection flush, or
   compaction.
