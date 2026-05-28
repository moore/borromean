# Low Level Storage

Borromean's low-level storage layer provides a bounded-memory,
log-structured database core over fixed-size erase regions. It is
designed to run without a heap allocator by using statically bounded
memory, so storage operations do not fail due to dynamic allocation
exhaustion. Completed durable operations must remain recoverable after
an unexpected halt. Recovery must also preserve allocator integrity:
regions may be temporarily staged or pending reclaim, but storage space
must not be permanently leaked by an interrupted operation.

A collection's visible state is reconstructed from three layers:

1. A bounded in-memory frontier containing recent changes.
2. Durable update and control records appended to the write-ahead log
   (WAL).
3. Immutable committed regions that hold compacted collection data.

The backing store is divided into one static metadata region followed by
equal-sized data regions. Data regions may be used as WAL regions,
committed collection regions, or free-list members.

Borromean can host multiple collections in the same store, subject to
compile-time capacity limits such as maximum live collections, pending
reclaims, and collection-specific runtime state. These limits keep core
memory usage explicit and avoid heap allocation in the storage layer.

TODO: Remove pending reclaims as a compile-time capacity limit.

The current implementation exposes `Map<K, V>` as the supported
high-level collection. The storage format itself is typed by collection
identifier, collection type, and collection format so additional
collection implementations can be added later.

## Table Of Contents

This document is organized from the conceptual model toward the concrete mechanisms that make the
model durable and recoverable.

- [Requirements Format](#requirements-format)
- [Reader Model](#reader-model)
- [Chapter 1: Theory Of Operation](01-theory.md)
- [Chapter 2: Storage Context And State Machines](02-state-machines.md)
- [Chapter 3: Collection Lifecycle](03-collection-lifecycle.md)
- [Chapter 4: WAL Model And Records](04-wal-records.md)
- [Chapter 5: Region And Disk Format](05-disk-format.md)
- [Chapter 6: Startup And Replay](06-startup-replay.md)
- [Chapter 7: Reclaim And Freeing](07-reclaim.md)
- [Chapter 8: Durability, Crash Cuts, And Formatting](08-durability-formatting.md)
- [Chapter 9: Current Implementation Coverage](09-implementation-coverage.md)

## Requirements Format

This specification keeps normative requirements adjacent to the text
that motivates them. Stable identifier and RFC-2119 language
conventions for borromean specifications are defined by
[spec/implementation-policy.md](../implementation-policy.md).

These identifiers are intended to be the primary Duvet traceability
targets. The surrounding narrative is informative unless it also
includes a requirement identifier.

## Reader Model

Read this specification as a crash-recovery model. The system is
defined by durable state, named operations that change that state, the
writes that make each operation durable, and replay rules that
reconstruct the same state after reset.

Glossary:

- **Region**: the fixed-size, erase-aligned unit of storage. User data,
  WAL data, and free-list links all live in regions.
- **WAL head / WAL tail**: the WAL head is the oldest live WAL region in
  the reachable chain. The WAL tail is the region where new records are
  appended.
- **Durable basis**: the latest replay-visible basis decision for a
  collection: empty creation, WAL snapshot, committed region head, or
  drop tombstone.
- **Retained basis**: the earliest basis record still retained after
  WAL reclaim. It may be newer than the historical `new_collection`
  record because reclaim can remove superseded records.
- **Frontier**: bounded in-memory collection state containing mutations
  newer than the durable basis.
- **Clean / dirty collection state**: clean means the durable basis is
  enough to load the collection. Dirty means newer WAL updates must
  also be replayed over that basis. A dirty collection may also have
  those updates loaded into an in-memory frontier.
- **Ready region**: a region removed from the free-list head by
  `alloc_begin` but not yet consumed by `head`, `link`, or
  `stage_region`.
- **Staged region**: a ready region durably moved out of the ready slot
  but not yet proven live or free.
- **Pending reclaim**: a region with durable `reclaim_begin` and no
  matching durable `reclaim_end`.
- **Crash cut**: a point in a multi-step operation where reset may leave
  only the durable prefix of that operation visible to replay.

Mechanism chapters use the same review pattern:

- **Purpose**: why the mechanism exists.
- **State**: stable state and operation-local state the mechanism reads
  or writes.
- **Named operations**: the operation identifiers used in state-machine
  transitions and diagrams.
- **Durable edge sequence**: the ordered records, region writes, and
  syncs that make the operation durable.
- **Replay effect**: how startup reconstructs equivalent state from the
  retained durable facts.
- **Crash cuts**: which partially completed prefixes are valid and how
  recovery handles them.
- **Requirements**: normative rules with stable identifiers.
