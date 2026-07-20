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
that names the root region of a region materialization as the collection root.
The record is not itself the collection root.

**Collection basis.** A collection basis is an object representing a collection
at one point in its history. Interpreting the basis and following its
collection-defined references yields the complete logical state at that point.
Those references may lead to earlier bases, so the basis need not contain the
complete state in its own bytes.

**In-memory frontier.** An in-memory frontier is the newest collection basis
currently held in RAM. It can be reconstructed from the collection's durable
root by applying later published operation records in order.

**Snapshot.** A snapshot is a collection basis stored in the WAL. Once
published, it can serve as the collection's durable root.

**Region materialization.** A region materialization is a collection basis
rooted in a region. Once published, it can serve as the collection's durable
root.

### Logs, durability, and transactions

**Write-ahead log (WAL).** A WAL is an append-ordered durable carrier for
operation records, snapshots, and WAL-protocol records.

**WAL-protocol record.** A WAL-protocol record is an operation
record whose meaning is defined by the WAL protocol. Ordinary
collection operation records are embedded in carrying WAL records, but the WAL
logic does not interpret or impose structure on the carried collection records.

**Main WAL.** The main WAL is the shared part of the WAL collection and serves
as the root of the whole database. It orders the records recovery uses to
reconstruct shared database state, including collection publications, allocator
progress, transaction decisions, cleanup, and references to transaction logs.

**Transaction log.** A transaction log is durable WAL storage assigned to one
open transaction. It holds the transaction's allocation entries, free intents,
and collection operation records. Collection operations and free intents remain
private until a commit record in the main WAL publishes them. An allocation
entry affects allocator state when it becomes durable, allowing recovery to
account for a region even when the transaction has no durable decision.

**Transaction region.** A transaction region is a region assigned exclusively
to one transaction object at a time for writing transaction-log data. It
contains one or more transaction segments and is logically part of the WAL. A
transaction region remains retained while any retained WAL record refers to it.

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

**Publication.** Publication is the protocol-defined durability point after
which recovery must include a specified effect in the relevant logical view.
Publication may coincide with durability of the operation record, or a later
record such as transaction commit may publish an already durable private
effect.

**Retained.** Data is retained while Borromean may still need it during recovery
or to finish work interrupted by power loss. Retained data, and any region
containing it, cannot be reclaimed or reused. Retaining a collection root also
retains every record and region reachable from that root.

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

**Ready Free.** A region is Ready Free when a free-list entry naming it lies in
the half-open range `[allocation, ready)`. Only the entry at `allocation` may be
consumed.

**Dirty Free.** A region is Dirty Free when a free-list entry naming it lies in
the half-open range `[ready, append)`. It is unavailable for allocation until a
readiness record advances the ready cursor beyond it. Erasing the region's bytes
alone does not change this relationship.

**Readiness record.** A readiness record is a Region Free List operation record
that publishes a new ready cursor after erase maintenance has successfully
erased every selected region. Its effect moves the erased free-list entries
from Dirty Free to Ready Free.

**Transaction Owned.** A newly allocated region becomes Transaction Owned when
its transaction allocation entry becomes durable. It remains Transaction Owned
until a committed collection basis reaches it or cleanup returns it to the free
list. A staged free intent leaves ownership unchanged. On commit, the detached
region becomes Transaction Owned until cleanup returns it to the free list. On
rollback, the region remains Collection Owned.

**Collection Owned.** A user collection owns every region reachable from its
retained committed basis. Core relies on that collection's reachability
contract. An internal collection owns each region its retained basis depends
on. Core defines when that region may be reclaimed.

**Free-list-internal command.** A free-list-internal command is a Free List
protocol record that moves a region within the Region Free List without
transferring it to a different owner.
