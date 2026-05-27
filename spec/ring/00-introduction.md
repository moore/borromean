# Low Level Storage

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

Read this specification as an operation-first storage model. The
system is defined by stable state, named operations that may change
that state, durable edges inside those operations, and replay rules
that reconstruct the same state after reset.

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
  sufficient to reconstruct the collection. Dirty means retained
  post-basis updates and possibly a materialized frontier are also
  needed.
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
