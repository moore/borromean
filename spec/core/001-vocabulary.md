# Core Specification Vocabulary

This document collects terms introduced by the
[system narrative](000-system-narrative.md) for review and cross-reference.
Component and composition chapters remain the authoritative homes for their
complete abstract and mechanical definitions.

### Storage geometry and I/O

**Database header.** The database header is an erase-block-aligned span outside
the indexed region area. It is written when the database is created, versions
the stored data, and describes the underlying hardware constraints and database
geometry.

**Region.** Each region is an equal-sized, non-overlapping byte range following
the database header. Its start is erase-block aligned, and its length is a
multiple of the erase-block length. Its complete span includes the region
header and the collection-defined data that follows it. Regions are allocated,
reclaimed, and reused only as whole units. Every region can be located using a
deterministic index.

**Region header.** A region header identifies the collection and encoding
expected for the region's bytes.

**Region index.** A region index identifies one region's reusable byte range.
It does not by itself identify the region's current role or the meaning of its
bytes.

**Write granule.** The write granule is the minimum write size exposed by the
logical storage geometry. Write and sync addresses and lengths are aligned to
this unit.

### Collections and representations

**Collection.** A collection groups logical state governed by
collection-defined records, operations, region formats, and reachability rules.
User collections may group data according to expected write and access
patterns. A collection may own zero or more regions.

**Operation.** An operation is one collection-defined mutation of a particular
collection's logical state.

**Operation record.** An operation record is a finite sequence of bytes
representing an operation. It describes what to apply, not where or how it is
persisted, and its meaning does not depend on its storage location or persistent
framing.

**Collection root.** A collection root is a single region, snapshot, or
in-memory frontier from which all live data in the collection can be accessed.

**Reachability.** Reachability is a collection-defined relation between a
collection basis and the records or regions represented by that basis. A record
or region is reachable from a basis when it is part of the basis's starting
representation or can be located and interpreted by following
collection-defined references from that representation.

**Collection head record.** A collection head record is a WAL-protocol record
that names a region as the collection root.

**Collection basis.** A collection basis is the current
collection-specific state produced by the ordered application of the
collection's operation history, whether or not the records representing that
history remain retained.

**In-memory frontier.** An in-memory frontier is a RAM-resident
collection basis. It can be recovered from the collection's current durable
root by applying, in order, the operation records for that collection that
follow the root.

**Snapshot.** A snapshot is a compact encoding of an in-memory frontier stored
in the WAL as a temporary durable collection basis.

**Region materialization.** A region materialization is a collection-defined
encoding of an in-memory frontier written into a region. A collection head
record can select that materialization as the collection's durable basis.

### Logs, durability, and transactions

**Write-ahead log (WAL).** A WAL is an append-ordered durable carrier for
operation records, snapshots, and WAL-protocol records.

**WAL-protocol record.** A WAL-protocol record is an operation
record whose meaning is defined by the WAL protocol. Ordinary
collection operation records are embedded in carrying WAL records, but the WAL
logic does not interpret or impose structure on the carried collection records.

**Main WAL.** The main WAL is the shared database WAL and serves as the root of
the whole database. It orders shared collection records and snapshots,
allocator records, transaction decisions, cleanup records, and other
database-wide protocol records.

**Transaction region.** A transaction region is a region assigned exclusively
to one transaction object at a time for writing transaction-log data. It
contains one or more transaction segments and is logically part of the WAL. The
transaction and WAL protocols determine when the records in those segments are
retained, replayed, or included in committed collection state.

**Transaction.** A transaction groups private collection operations,
allocations, and free intents under one commit or rollback decision.

**Transaction decision.** A transaction decision is a durable main-WAL protocol
record that selects commit or rollback for an open transaction. Commit
publishes the transaction's collection operations and free intents; rollback
discards them. Either outcome determines the transaction cleanup that must
follow.

**Transaction cleanup.** Transaction cleanup is the process of appending, in
order, free commands to the main WAL to retire each free-intent record after
commit or each transaction-private allocation after rollback.

**Transaction-finish record.** A transaction-finish record is a durable
main-WAL protocol record that closes a transaction's ordered cleanup range after
all required free commands are durable. Recovery that finds the finish record
performs no further cleanup for that transaction.

**Transaction segment.** A transaction segment is a write-granule-aligned
portion of a transaction region containing allocations, free intents, an
optional next-segment link, and collection operations.

**Transaction allocation entry.** A transaction allocation entry records the
allocated region, the global allocation sequence, and the allocation cursor
after the entry is consumed. It becomes durable before the runtime allocation
cursor advances.

**Free intent.** A free intent is a transaction-private proposal to detach a
collection-owned region and return it through transaction cleanup when the
transaction commits. Rollback discards the intent.

**Durable operation record.** An operation record is durable
when its complete persistent representation will survive power loss. Durability
alone does not make its effect visible, committed, or part of a particular
logical view.

**Publication.** Publication is the WAL-protocol-defined durability point after
which recovery must include an operation's effect in the reconstructed logical
state, whether or not recovery retains or replays the original operation
record.

**Retained.** A record, representation, or reference is retained while recovery
may still require its logical role or while another retained structure still
references it. A region containing a retained record or representation cannot
be reclaimed or reused until none of its retained contents are needed.

### Free-list positions and relational lifecycle

The lifecycle terms in this group name relationships derived from the free
list, transaction structures, and collection roots.

**Region Free List.** The Region Free List is a cohesive internal collection
represented as one logical FIFO queue. Its backing may include linked
materialized regions and a WAL-resident tail.

**Allocation cursor.** The allocation cursor identifies the first Ready Free
entry that may be consumed.

**Ready cursor.** The ready cursor identifies the first Dirty Free entry.

**Append cursor.** The append cursor identifies the first unused free-list
position.

**Allocation sequence.** The allocation sequence is a global, monotonically
increasing order assigned to durable records that consume the entry at the
allocation cursor.

**Ready Free.** A region is Ready Free when it occurs at a free-list position in
the half-open range `[allocation, ready)`. Only the entry at `allocation` may be
consumed.

**Dirty Free.** A region is Dirty Free when it occurs at a free-list position in
the half-open range `[ready, append)`. It is unavailable for allocation until a
readiness record advances the ready cursor beyond it.

**Readiness record.** A readiness record is a Region Free List operation record
that publishes a new ready cursor after erase maintenance has successfully
erased every selected region. Its effect moves the erased free-list entries
from Dirty Free to Ready Free.

**Transaction Owned.** A region is Transaction Owned while
retained transaction structures are responsible for its next safe outcome. On
commit, every region allocated by the transaction atomically becomes Collection
Owned. The collection implementation must guarantee that each such region is
reachable from its committed representation. Core assumes this invariant for
opaque collection formats and enforces it for internal collections. On
rollback, an allocated region continues to be Transaction Owned until it is
freed by an explicit durable free operation during transaction cleanup. On
commit, every region named by a free-intent command becomes Transaction Owned
between the commit and its free command during cleanup. On rollback, free-intent
records are ignored and ownership does not transfer.

**Collection Owned.** A region is Collection Owned when it is
reachable from the committed root of exactly one user or internal collection.

**Free-list-local command.** A free-list-local command is a Free List protocol
record that moves a region within the Region Free List without transferring it
to a different owner.
