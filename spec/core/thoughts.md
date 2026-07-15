# Core Specification Narrative Sketch

I think we want to decompose this a little differently. The shape is not all
wrong but I think the ordering of ideas and which to stress are a little off.
Here is a ruff sequancing of how I would structure it. These chapters are not
meant to nessarly be complete but to sketch out the core ideas.

## 1. 

Borromean is a database that is intended to run without the help of an
undelying filesystem. Fallowing from that one of it's core conserns is how to
manage storage allocations, reclomation, and the geomitry of the storage.

The primary target for Borromean is flash storage connecte to mrico controlers.
This added the complexity that the database must be responsible for ware
leveling. To achieve this Boarroman combineds sevral key ideas.

The first idea is that Borromean uses an append-only model. Every change to
collection state is persisted as an append rather than an in-place update. When
a collection value changes, a delta or replacement copy is stored without
modifying its earlier persisted representation. When a value is deleted, a
tombstone is recorded.

The abstract form of one such state change is an **operation record**: a finite
sequence of bytes representing one collection-defined mutation of a particular
collection's logical state. Applying an ordered sequence of operation records
to a collection basis produces the collection's later state. The operation
record describes what to apply, not where or how it is persisted.

The second idea is that free regions are managed in a FIFO (First In First Out)
queue where the oldes entry in the free queue is the first removed when neew
space is needed. This promotes equal ware across all regions of flash.

> NOTE: This is not a perfict system as data which is rarly updated can hold on
> to a allocation for an extended period of time while othere regons of storage
> are cyceld many times. We will need to model real world useage but I suspect
> that this will lead to a portion of the storage seeing serveal times the write
> cycleing then the rest based on different records having very differet life
> cycles. An aproach to hand this would be to perdiocally move very old
> allocations to more heavelly used regions of storage but we have chosen not to
> do that to maintain refererential intergrrty without adding a second level of
> inderaction.

A second consideration for working with flash is accommodating its write-erase
nature. Once a flash cell has been written, it cannot be updated to a new value
without first being erased. This differs from magnetic storage, where a value
can be overwritten directly from A to B. Erase latency would make foreground
allocation and write latency unpredictable if it were performed on demand.
Borromean therefore splits the free list into dirty and ready ranges. Freed
regions enter the dirty range and move to the ready range only through explicit
caller-requested erase maintenance. Allocation, logical free, and writes to
newly allocated space do not perform that erase work themselves.

Lastly to encorage wareleveling there is no conistent location to look for the
root of the database. If we picked one or a few location to conistangly store
the root those regions would need to be update reguarlly likely causing them to
fail befor the rest of the storage. Instead Borromean devides the avalible
storage in to a sequance of equal sized regions, each with a standerd header
format. At startup all of these regions headders will be scanned to find the
root of the database. To minimise this scan time larger regions are prefered
thou as we will see later the size of the regions has a direct impact on memory
useage so a traid must be made between start up effechency and required ram.

## 2. 

The storage geometry of a Borromean database is constrained by four
parameters:

   1. The flash erase block size.
   2. The minimum write size (refered to in Borromean as the write granule).
   3. The totall liniear size allocated to Borromean.
   4. The region size for the database.

The database header for the whole store occupies a separate,
erase-block-aligned span outside the indexed region area. The remaining storage
is divided into equal-sized, non-overlapping **regions**:

```text
[Database header][Region 0]...[Region n]
```

A region is Borromean's unit of storage allocation, reclamation, and reuse. A
structure's responsibility covers an entire region, and erase operations cover
one or more whole regions.

Each region has the format:

```text
[Region header][collection-defined data]
```

Each region begins on an erase-block boundary, and its length is a multiple of
the erase-block length. The region header and the collection-defined data that
follows it are written together as one logical write, so the header has no
independent padding requirement. The configured region length includes that
complete span. Geometry that violates these constraints is rejected during
initialization.

A region index names only the reusable byte range. Its current role and
responsibility derive from retained structures, and its bytes are interpreted
only after structure-specific validation.

Borromean uses a logical byte-oriented storage interface with four core
operations:

1. `write(address, data: &[u8]) -> Result<(), Error>`
2. `sync(address, length) -> Result<(), Error>`
3. `read(address, length) -> Result<[u8], Error>`
4. `erase(address, length) -> Result<(), Error>`

> TODO: This is missing life times.

The interface is logical rather than a direct representation of the physical
device API. A `FlashIo` implementation is responsible for expanding unaligned
reads, splitting transfers, widening range sync into a global barrier when
necessary, and performing any lower-level work needed to satisfy these
guarantees.

`write()` stages data beginning at `address` and spanning `data.len()` bytes.
The address must be write-granule aligned and the length must be a multiple of
the write granule. After `write()` succeeds, later reads under continuous power
must observe the written data. The write is not guaranteed to survive power loss
until a covering `sync()` succeeds, although an implementation is allowed to
make it durable earlier.

After `sync(address, length)` succeeds, every previously successful write fully
covered by that range is durable. The requested range is a minimum guarantee: an
implementation may synchronize a larger range or all storage, and callers must
not depend on writes outside the requested range remaining non-durable. On
directly programmed NOR flash the operation may be a no-op because successful
writes may already be durable. The sync address and length must be write-granule
aligned.

`read()` has no logical alignment restriction. Under continuous power it returns
bytes from the most recent successful writes. If power is lost before a covering
sync, recovery may observe an unsynced write as absent, complete, or torn at an
allowed underlying write boundary. Recovery produces one resulting storage
image; repeated reads after restart observe that stable image.

`erase()` accepts only a region-aligned address and a nonzero length that is a
multiple of the Borromean region size. A successful erase is immediately
read-visible and power-failure durable; if the underlying storage needs a
barrier, the `FlashIo` implementation performs it before returning. If power is
lost during erase, or erase returns an error, the range may be unchanged,
completely erased, or partially erased.

Any operation that addresses bytes outside the configured storage range or
violates its geometry returns an error.

Later chapters define how Borromean orders the storage primitives to publish
higher-level state safely.

## 3

In Borromean, records are grouped into logical collections. A collection may
own zero or more regions, and its format may link those regions into a larger
structure. Access to that structure begins at a collection root which is a single region, snapshot, or in-memory frontier from which all live data in the collection can be accessed.

The core is not responsible for interpreting collection-specific data after the
region header. A header identifies the collection and encoding
expected for the region's bytes.

There are three internal collection types used by Borromean core to manage
region and record lifecycles:

1. The Write Ahead Log (WAL)
2. Region Free list
3. Transacton buffers

Other higher-level collection types are defined by consumers of Borromean core,
each defining their own records, operations, region formats, and reachability
rules. Core guarantees the atomic allocator and ownership transfer, while the
collection implementation guarantees that every region allocated by a committing
transaction is reachable from its committed representation. Core assumes that
contract for opaque collection types and must satisfy it for its own internal
collections.

...Eaplain that there is a bounded number collections, and how there heads
tracked by storage struct in memory...

## 4. 

Each operation's effect is represented by exactly one of:

   1. An operation record in the WAL.
   2. A snapshot in the WAL.
   3. A region materialization.

The newest valid snapshot or head record for a collection establishes its
current root. A snapshot stores the root in the WAL, while a head record points
to a region materialization. A new root supersedes all earlier operation
records, snapshots, and head records for that collection.

Operation records written after the current root are applied in order. The
current root and these later operation records coexist, but they never represent
the same operation.

The write-ahead log (WAL) provides short-term durable storage for operation
records and snapshots in append order. Operation records are read from the WAL
only during startup replay. After replay, the effects of operation records
following each collection's current root are held in RAM.

Each open collection maintines a RAM buffer with a size equal to a region. Once
a opperation is persted in the WAL is is applied by the collection implmentation
to this RAM buffer to update the state of the collection. When the RAM buffer is
full it is meterlized in to a region in flash allowing new updates to be held in
the RAM buffer.

When a region is not imeadeatly needed but it's RAM buffer is not filled a
compact version of the buffer accounting for only the currently consumed buffer
space can be stored as a snapshot in the WAL. This allow the collection to be
closed, and its RAM buffer returned, without writing a partally filled region
whiles still allowing effechent reads from flash.

## 5.

 The Region Free List is a cohesive internal collection represented by a
   logical FIFO queue with three cursor positions: allocation, ready, and
   append. Materialized free-list regions form a linked representation of the
   queue. The current tail may instead be represented by WAL records and a
   snapshot until it is materialized into a region and linked to a newly
   prepared tail.

The allocation cursor identifies the first ready entry that may be consumed. The
ready cursor identifies the first dirty entry. The append cursor identifies the
first unused queue position:

```text
[consumed or stale][ready entries][dirty entries][unused capacity]
                   ^ allocation  ^ ready         ^ append
```

The active ranges are half-open:

```text
Ready Free = [allocation, ready)
Dirty Free = [ready, append)
```

Allocation consumes only the entry at `allocation`. Every allocation or free
that transfers a region between the free-list collection, a transaction, and
another collection is performed through the transaction protocol. A durable
transaction allocation entry records the allocated region, the allocator
position after the allocation, and a monotonically increasing allocation
sequence. Only after that entry is durable does the runtime allocation cursor
advance and the transaction become responsible for the region.

The global allocator lock protects selection of the current allocation entry,
assignment of the global allocation sequence, and advancement of the allocation
cursor. An operation should preflight the space needed for its durable
head-consuming command before acquiring this lock. It then acquires the lock,
revalidates and selects the entry at `allocation`, assigns the next sequence and
`allocation_head_after`, writes and syncs the command, applies the runtime
cursor advance, and releases the lock. For an ordinary allocation that command
is the transaction allocation entry; a free-list-local command that consumes the
same head entry uses the same lock and durable-apply ordering.

Transactions may append allocation entries in parallel transaction logs.
Therefore the physically last allocation record observed during replay is not
necessarily the newest allocator state. Replay uses the retained allocation
record with the largest valid allocation sequence and its
`allocation_head_after` value. Transaction cleanup frees are written in their
ordered main-WAL cleanup range and advance the append cursor.

When the allocation cursor crosses into a new materialized free-list region, the
old representation region is no longer needed as backing storage. Moving that
region from free-list structure into the dirty range does not change owners, so
it does not require transaction cleanup. A free-list-local WAL command can
atomically unlink the old representation region, append it at the dirty tail,
and advance the affected free-list cursors. A crash before that command leaves
the old representation reachable; a crash after it leaves the region in the
dirty range. Erase maintenance likewise changes only the boundary between dirty
and ready entries inside the same free-list collection.

The remaining details of free-list-local tail growth and representation
retirement are recorded in
[todo.md](todo.md#free-list-collection-chapter-d28-d30). Transaction-log and
main-WAL continuation questions remain in the recursive-allocation TODOs.

## 6.

Transactions are required whenever allocating or freeing a region moves
   responsibility between two objects. Allocation must remove a region from the
   global free-list collection and make a transaction responsible for it before
   a collection may publish it. Freeing performs the reverse transfer without
   allowing a crash to leak the region or expose a still-reachable region for
   erase and reuse. This applies to both user collections and Borromean's
   internal collections.

Transactions also allow long-running multi-step updates to avoid blocking reads
to the collecton being written or to writes in unrelated collections. A possible
example is a large file streamed over a slow network connection.

The two cross-transaction locks used by the paths described here have distinct
scopes:

| Lock | Protects | Required scope |
| --- | --- | --- |
| Global allocator lock | Selection of the ready head, global allocation sequence, and allocation cursor | From final head validation through the durable head-consuming command and runtime cursor apply |
| Main-WAL finish lock | The uninterrupted transaction decision, ordered cleanup, and finish interval | From immediately before appending commit or rollback through durable finish and runtime finish apply |

Preparation that touches only a transaction's private state should occur before
either global lock is acquired. The locks are runtime concurrency controls and
are not persisted. Replay reconstructs allocator and transaction state from
durable operation records.

...explain that there is a fixed number of transactions held by the storage
structure, with bounded ephemeral state for each transaction and a reference to
the transaction-log region containing its current segment...

Transaction-log regions can be reclaimed only when no retained WAL record
references any segment in the region.

For locality, transaction operations are stored in transaction segments rather
than directly in the main WAL. Beginning a transaction writes a main-WAL begin
record that identifies the start of its transaction-log segment. Only one
transaction owns a transaction region at a time, although a region may contain
more than one segment over its lifetime. Each segment is write-granule aligned
and contains:

```text
[Allocations][Free intents][Optional next segment][Collection operations]
```

Allocation entries use WAL framing and contain the region, global allocation
sequence, and `allocation_head_after`. The containing transaction establishes
initial transaction ownership; a durable next-segment link or committed
collection operation establishes the region's later structural role. Free
intents are a packed list of collection-owned regions proposed for transfer to
transaction cleanup on commit. Collection operations describe the private
collection changes.

Before commit, collection operations and free intents may be buffered in memory,
but every allocation is appended and synced immediately in the transaction log
before the global allocator cursor advances. A crash can therefore recover every
consumed free-list entry even though the transaction never committed.

Commit preparation does not require the main-WAL finish lock. The transaction
first encodes, flushes, and syncs its private transaction segments and performs
any other preparation that does not change shared state. Only when it is ready
to append the durable decision does it acquire the finish lock, revalidate the
commit preconditions, and write and sync the main-WAL commit record identifying
the imported segment range. It holds that lock through ordered cleanup and the
durable transaction-finish record.

The durable commit atomically interprets the collection operations and free
intents. A new allocation used by a collection becomes collection-owned because
the committed collection representation reaches it. A committed free intent
stops being collection-owned and becomes transaction-owned cleanup work. A
collection implementation is responsible for ensuring that every allocation it
uses is reachable from its committed representation; core assumes this for
opaque collection formats and must enforce it for its internal collections.

Following commit, ordered free records move committed free-intent regions into
the dirty free-list range, and a transaction-finish record closes the cleanup
range and releases the finish lock.

If replay finds an open transaction without a durable decision, it writes a
rollback decision and returns its durable transaction allocations to the dirty
free-list range through ordered cleanup. It ignores the transaction's staged
free intents, so those regions remain collection-owned. If replay ends after a
commit or rollback but before transaction finish, it resumes the remaining
cleanup and writes the finish record.

## 7. 

Region lifecycle names are derived relationships, not fields stored in a
   region table. Borromean keeps no persistent or runtime strcture containing
   one lifecycle state per region. A region's classification follows from its
   position in the free-list collection, the retained transaction structures
   that name it, or the retained collection representation that reaches it.

The common recyclable collection-data path is:

```text
Ready Free -> Transaction Owned -> Collection Owned -> Transaction Owned -> Dirty Free -> Ready Free
```

Rollback skips the collection-owned state for new allocations. Transaction-log
continuation regions may remain transaction-owned while retained transaction and
WAL structures still reference them.
>TODO: This belongs in the transation machinal desing chapter..

Free-list backing regions have additional collection-local paths. A free-list
command may move the ready-head region directly into Free-List Collection Owned
backing storage, or move an obsolete backing region directly into Dirty Free,
because neither operation transfers responsibility to a different owner.
>TODO: This belongs in the free lisit machinal desing chapter.

Ready Free:

A region is Ready Free when it occurs at a logical free-queue position in
`[allocation, ready)`. Only the entry at `allocation` may be consumed. An
ordinary cross-owner allocation becomes Transaction Owned after its transaction
allocation entry is durable.

A free-list-local growth command may instead consume that same entry and make it
Free-List Collection Owned without passing through a transaction. This provieds
a direct transition from Ready Free -> Collection Owned but only when moving the
region internally to the Free Queue collection.

Transaction Owned:

A region is Transaction Owned when retained transaction structures are
responsible for its next safe outcome. This occurs in two principal cases:

1. A durable transaction allocation entry has consumed the region, but no
   committed collection operation yet owns it.
2. A durable commit has removed a free-intent region from the logical collection
   view, but ordered cleanup has not yet appended its free record.

An allocation becomes Collection Owned at commit when the collection's committed
representation reaches it. On rollback, allocation cleanup instead writes an
ordered free record, which moves it to Dirty Free. A committed free-intent
region remains Transaction Owned until its ordered free record becomes durable.

Collection Owned:

A region is Collection Owned when it is reachable from the retained committed
head of exactly one collection. The collection may be a user collection or an
internal WAL or free-list collection. Transaction-log regions use the
specialized Transaction Owned classification while retained transaction or WAL
structures reach them. Region headers validate the expected encoding but do not
create ownership. The collection implementation supplies the reachability
guarantee.
> TODO: This discription of transaction owned regions is not quite write.

A staged free intent does not change collection ownership before the transaction
decision. Durable rollback discards the intent and leaves the region Collection
Owned. Durable commit applies the collection operation that detaches it and
simultaneously makes it Transaction Owned cleanup work.

Dirty Free:

A region is Dirty Free when it occurs at a logical free-queue position in
`[ready, append)`. It remains unavailable for allocation even if a previous
failed or interrupted maintenance call happened to erase its physical bytes.

Erase maintenance accepts a caller-supplied maximum region count and selects
`min(requested_count, dirty_count)` entries beginning at `ready`. If the
selected count is zero, it returns with an error. Otherwise it erases the
selected regions in queue order. Each successful `erase()` is already
power-failure durable.

If any erase returns an error, maintenance stops immediately, performs no
further erase or WAL operation, publishes no readiness record, and leaves the
runtime ready cursor unchanged. Any successfully erased prefix remains
relationally Dirty Free until the caller explicitly retries.

After every selected erase succeeds, maintenance writes and syncs one readiness
record containing the new ready cursor, then advances the runtime cursor. A
crash before that record is durable leaves the entire prefix Dirty Free and
permits safe re-erase. A crash after the record is durable but before runtime
apply is repaired by replay, which reconstructs the advanced cursor. Adjacent
physical regions may be coalesced into one larger erase call without changing
the logical region-count budget.

## Draft glossary

This glossary collects the definitions introduced in the preceding narrative
so they can be reviewed together. It does not replace the in-line definitions
that introduce each concept where the surrounding design motivates it. Every
entry remains draft. An entry marked as a working or deferred draft still needs
an agreed in-line home in the appropriate narrative or mechanical chapter.

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

**Collection basis (working draft).** A collection basis is the current
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

### Runtime locks

**Global allocator lock.** The global allocator lock serializes selection of
the entry at the allocation cursor, assignment of the global allocation
sequence, writing and syncing the corresponding durable head-consuming command,
and the subsequent runtime cursor advancement.

**Main-WAL finish lock.** The main-WAL finish lock serializes a transaction's
decision, ordered cleanup, and finish interval. Private commit preparation may
occur before this lock is acquired.

## 8. 

TODO: A detaild michanical discription of the in memory storage state,
   fields, rust structs, etc. This should the signitures of public APIs that
   will be used to interact with the storage. This should build on the privouse
   high level discrition.

## 9.

 TODO: A detaild michanical description of the WAL. Record framing: byte
   stuffing, checksums, record-start discovery after torn records, and related
   details. In memory rust structs, life cycle of the regions, etc. This should
   build on the privouse high level discrition.

## 10. 

TODO: Detaild discption of the michanical desing of the free list.

## 11.

 A percisc discriton of start up and WAL replay.
