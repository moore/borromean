# Borromean Core v3 Specification

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

Status: superseded design pilot. Its requirements and refinement claims are
retained as historical inputs and do not define conformance for the active core
design.

## How to read this specification

Each component document has two equally important jobs:

1. communicate why the component exists, the objects it manipulates, and the
   mechanical design intended to satisfy the system goals; and
2. state stable, individually traceable requirements that distinguish a
   conforming implementation from a merely similar one.

The explanatory sections are design authority, not optional background. The
numbered `CORE-*` requirements are the verification boundary derived from that
design. If an implementation satisfies the literal requirements but violates
the described mechanism or leaves part of it unspecified, the specification
and refinement matrix must be corrected before conformance can be claimed.

Goals, motivations, and specification-completeness rules do not receive
`CORE-*` identifiers merely because they are important. A numbered requirement
must identify an observable behavior or statically checkable structure. The
refinement matrix and tests name the verification method and retain its
evidence. Design goals are reviewed against the complete set of refined
requirements; they are not made artificially “testable” by assigning an
identifier to a statement that only says the design must be correct, bounded,
or fully defined.

Within a component document, each requirement group follows the prose that
introduces and motivates it. The refinement matrix is the centralized index of
requirements and verification evidence, so chapters do not repeat a detached
requirement catalogue at the end.

## System model

Borromean stores several independently typed collections on one erase-oriented
device. The storage core does not understand map keys, channel messages, or
object-log records. It manages the physical facts shared by every collection:
ownership, durability, allocation, transactions, recovery, and maintenance.
The WAL is the logical root of the database: startup discovers its retained
bounds and replays it to select every other live storage structure.

The architecture is layered as follows:

| Layer | Responsibility |
| --- | --- |
| Typed collections | Logical records, indexes, manifests, flush, and compaction |
| Transaction registry | Private collection views, enrollment, and serialized atomic publication |
| WAL and recovery | Durable decisions, ordered replay, and retained boundaries |
| Free-space manager | FIFO reuse, dirty/ready state, and basis checkpoints |
| Ownership machine | Purpose-safe region lifecycle transitions |
| V3 format and raw device | Encoding, geometry, media write, erase, read, and sync |

Dependencies point downward. Recovery reconstructs the same logical machines
used by foreground operations; it is not a separate source of storage rules.
Typed collections extend the operation set with collection-specific reads,
mutations, snapshots, flushes, and compactions. Each operation may add
collection-format validation and stricter I/O, search, and memory bounds, but
none may weaken or bypass core ownership and durability transitions.

## Document map

The documents are intentionally divided by state-machine responsibility:

- [goals and locality](00-goals-locality.md);
- [ownership](01-ownership.md);
- [durability and I/O](02-durability-io.md);
- [format and WAL](03-format-wal.md);
- [free space](04-free-space.md);
- [transactions](05-transactions.md);
- [recovery and maintenance](06-recovery-maintenance.md);
- [API and memory](07-api-memory.md);
- [refinement matrix](08-refinement.md).

Normative words such as MUST, MUST NOT, and MAY are interpreted as described by
RFC 2119. Requirement identifiers are globally unique and remain stable when
text moves between these files.

## Shared vocabulary

- **region**: a fixed-size, geometry-addressable unit of erase, ownership, and
  FIFO reuse, with its discovery header at a predictable offset;
- **WAL sequence number**: a monotonically increasing 64-bit generation stored
  in each valid WAL preamble. It orders WAL-tail publications, not arbitrary
  region writes, and never wraps or repeats;
- **allocation sequence number**: a monotonically increasing 64-bit number in
  every durable allocation fact. It supplies one allocator order across the
  main WAL and all transaction logs, which otherwise have no total replay
  order;
- **media write**: one aligned raw-device byte-range write, conventionally
  called a flash program operation. It is not durable until a covering sync
  succeeds;
- **logical record**: a typed fact before any physical framing; whether it has
  its own checksum depends on the structure that contains it;
- **append-framed record**: one independently discoverable fact in an
  open-ended append stream. It has an individual checksum and is physically
  framed, escaped, padded, and aligned to the stream's write granule;
- **materialized entry**: an item inside an explicitly bounded object such as a
  basis, data region, manifest, or cleanup list. It may rely on the enclosing
  object's bounds and integrity scheme rather than carrying an individual
  checksum or append framing;
- **record separator**: the reserved leading marker for an append-framed
  record. It differs from the erased byte and cannot appear unescaped in the
  encoded record body;
- **publication**: the synced fact that makes a prepared object reachable from
  a retained root or committed view;
- **database root**: the retained WAL from its selected head through its
  selected tail. All other live roots and recovery obligations are selected by
  replaying this range;
- **runtime apply**: the in-memory transition performed after publication is
  durable, using the same pure transition used by replay;
- **prepared region**: an erased region whose readiness is durably known;
- **dirty region**: an unowned region that still requires explicit erase;
- **materialization**: an immutable bounded representation built after the
  complete input unit is known, allowing collection-specific reordering,
  indexing, dense packing, and a collection-defined physical write layout;
- **collection memory frontier**: bounded caller-owned logical mutations that
  are durable in the WAL but not yet incorporated into an immutable collection
  materialization;
- **WAL snapshot**: a self-contained collection-specific serialization of a
  memory frontier appended as one WAL record. It is an interstitial durable
  basis for later updates and replay, not a final data-region materialization;
- **basis**: a durable self-contained starting state selected by WAL replay;
- **collection basis**: the later WAL position between a collection snapshot
  and `head` record when no resident committed frontier supersedes it;
- **free-space basis**: an immutable materialization of a precise interval of
  the logical free queue;
- **allocator frontier**: retained allocator facts whose allocation sequences
  are later than the newest selected allocator checkpoint;
- **allocator checkpoint**: a consistent pair of allocation sequence and
  allocation head. The sequence identifies exactly which allocation facts are
  reflected in the head;
- **operation set**: the kinds of actions exposed or permitted by a component,
  such as read, mutate, snapshot, flush, compact, reclaim, or erase;
- **budget**: an explicit upper limit on resource consumption by an operation,
  measured in quantities such as operation count, bytes read or written,
  memory, or elapsed time. A budget never describes which operation kinds are
  allowed;
- **maintenance**: bounded caller-invoked work that is never hidden inside a
  foreground mutation.

## Operation template

Every state-changing operation specification MUST state:

1. logical preconditions and capacity preflight;
2. ownership reservations;
3. ordered physical actions and exact sync boundaries;
4. the durable publication event;
5. the runtime transition applied after publication;
6. every meaningful crash cut;
7. read visibility and cleanup ownership;
8. read, write, erase, search, memory, and deferred-work bounds;
9. its Quint and Rust verification obligations.

An operation is underspecified if a reader cannot use those nine items to
enumerate its possible durable states after a crash.
