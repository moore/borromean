# Core Specification System Narrative

Common terms are collected in the [core vocabulary](001-vocabulary.md).

This narrative introduces the core subsystems in the order needed to understand
how they work together. It states the minimum shared contracts needed by later
chapters without defining all of their mechanics.

The remaining specification chapters are planned in this reading order:

1. `002-device-format-and-io.md`
2. `003-region-relations.md`
3. `004-main-wal.md`
4. `005-transactions.md`
5. `006-free-list.md`
6. `007-self-hosting-and-progress.md`
7. `008-storage-service-and-collections.md`
8. `009-runtime-and-maintenance.md`
9. `010-recovery.md`
10. `011-verification-and-refinement.md`

The main WAL, transaction logs, and free list depend on one another. Their
component chapters use the minimum contracts introduced here rather than
pretending that one can be defined without the others. The self-hosting and
progress chapter then reconnects their complete contracts and establishes that
allocation, log growth, reclamation, and capacity reserves form a closed system.

## 1. Raw flash constraints and design strategy

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

The top-level storage object follows a reader/writer access rule. Read-only
top-level operations may coexist. An operation that may change shared
persistent state or shared mutable runtime state has exclusive access and is
non-reentrant at the top-level API boundary. Internal allocator, WAL,
transaction, cleanup, and free-list operations may call one another while
retaining that same exclusive access; they do not acquire subsystem locks.
Transaction-private preparation may occur without entering the top-level
storage object. This access discipline is a runtime invariant and is not
persisted.

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

## 2. Device geometry and logical I/O

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

## 3. Collections and roots

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

## 4. Operations and collection bases

A collection change begins as an operation represented by an operation record.
When reconstructing a selected collection state, each operation's effect is
accounted for exactly once. The effect is either:

1. applied from a published operation record that follows the durable root;
2. already represented by the selected snapshot; or
3. already represented by the selected region materialization.

These durable representations may coexist physically. Once a published basis
represents an operation's effect, recovery starts from that basis and does not
apply the earlier operation record separately. Published operation records that
follow the durable root remain separate and are applied in order.

A collection basis is an object representing a collection at one point in its
history. Interpreting the basis and following its collection-defined references
yields the complete logical state at that point. Those references may lead to
earlier bases, so the basis need not contain the complete state in its own
bytes.

Borromean uses three forms of collection basis:

1. An **in-memory frontier** is the newest basis currently held in RAM.
2. A **snapshot** is a basis stored in the WAL.
3. A **region materialization** is a basis rooted in a region.

A snapshot or region materialization remains logically complete when its
interpretation depends on earlier bases or other collection-defined references.

While an in-memory frontier is resident, it serves as the collection's current
root. For recovery, the newest valid snapshot or collection head record
establishes the durable root. The snapshot is itself a root. A collection head
record instead names the root region of a region materialization; the record is
not itself the root. Published operation records following the durable root are
applied in order to reconstruct the in-memory frontier.

When a snapshot or region materialization of the frontier is published as the
new durable root, it replaces the previous root as the selected starting basis.
The new basis may contain earlier effects directly or reach them through
references to earlier bases. An earlier basis or record remains retained
whenever the new basis still depends on it.

The later collection and runtime chapters define when a frontier must be
snapshotted or materialized and which durable representations may be read after
startup.

## 5. Region Free List

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

Every mutating top-level storage operation has exclusive access to allocator
state. An operation that consumes the entry at `allocation` preflights the space
needed for its durable command, selects that entry, assigns the next sequence
and `allocation_head_after`, writes and syncs the command, and only then applies
the runtime cursor advance. No other top-level mutation may interleave with that
sequence. For an ordinary allocation the command is the transaction allocation
entry. A transaction allocation and a free-list-internal allocation may both
consume the region at the allocation cursor. Each writes and syncs its own
allocation record before advancing the runtime allocation cursor.

Different transactions store allocation entries in different transaction logs.
The physically last allocation record observed in any one log is therefore not
necessarily the newest allocator state. Replay uses the retained allocation
record with the largest valid allocation sequence and its
`allocation_head_after` value. Transaction cleanup frees are written in their
ordered main-WAL cleanup range and advance the append cursor.

Free-list appends update the WAL and the in-memory frontier; they do not modify
a materialized region. Materialized free-list regions are immutable. When the
current frontier must be materialized into its already reserved region `n`, the
free list first uses a free-list-internal allocation command to consume a Ready
Free region as reserved successor `n+1`. This does not transfer ownership. It
then writes and syncs the complete frontier into region `n`, including the link
to `n+1`, and writes and syncs a free-list tail-advance command in the WAL. The
tail-advance command publishes `n` as the materialized tail and `n+1` as the new
reserved successor. Runtime tail state advances only after that command is
durable.

If replay finds the successor allocation without its corresponding durable
tail-advance command, region `n` is an incomplete or unpublished
materialization. Recovery erases region `n` before retrying the materialization even if its bytes appear valid.

When the allocation cursor crosses into a new materialized free-list region, the
old representation region is no longer needed as backing storage. Moving that
region from free-list structure into the dirty range does not change owners, so
it does not require transaction cleanup. A free-list-internal WAL command can
atomically unlink the old representation region, append it at the dirty tail,
and advance the affected free-list cursors. A crash before that command leaves
the old representation reachable; a crash after it leaves the region in the
dirty range. Erase maintenance likewise changes only the boundary between dirty
and ready entries inside the same free-list collection.

The remaining details of free-list-internal tail growth and representation
retirement are recorded in
[todo.md](todo.md#free-list-collection-chapter-d28-d30). Transaction-log and
main-WAL continuation questions remain in the recursive-allocation TODOs.

## 6. Transactions

Transactions are required whenever allocating or freeing a region moves
responsibility between two objects. Allocation must remove a region from the
global free-list collection and make a transaction responsible for it before
a collection may publish it. Freeing performs the reverse transfer without
allowing a crash to leak the region or expose a still-reachable region for
erase and reuse. This applies to both user collections and Borromean's
internal collections.

Transactions also allow long-running multi-step updates without reserving
exclusive top-level storage access for the transaction's entire lifetime. A
possible example is a large file streamed over a slow network connection. The
exact representation of an open transaction between calls belongs to the
mechanical design.

Replay reconstructs allocator and transaction state from durable operation
records.

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

Using only the transaction object, commit preparation first encodes, flushes,
and syncs the private transaction segments and performs any other work that does
not change shared state. The exact mechanical construction of the transaction
object is defined later. Once preparation is complete, commit enters the
top-level storage object with exclusive access, revalidates the commit
preconditions, and writes and syncs the main-WAL commit record identifying the
imported segment range. The same exclusive storage operation continues through
ordered cleanup and the durable transaction-finish record.

The durable commit atomically interprets the collection operations and free
intents. A new allocation used by a collection becomes collection-owned because
the committed collection representation reaches it. A committed free intent
stops being collection-owned and becomes transaction-owned cleanup work. A
collection implementation is responsible for ensuring that every allocation it
uses is reachable from its committed representation; core assumes this for
opaque collection formats and must enforce it for its internal collections.

Following commit, ordered free records move committed free-intent regions into
the dirty free-list range, and a transaction-finish record closes the cleanup
range.

If replay finds an open transaction without a durable decision, it writes a
rollback decision and returns its durable transaction allocations to the dirty
free-list range through ordered cleanup. It ignores the transaction's staged
free intents, so those regions remain collection-owned. If replay ends after a
commit or rollback but before transaction finish, it resumes the remaining
cleanup and writes the finish record.

## 7. Region relationships and erase maintenance

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

Free-list backing regions have additional collection-local paths. A free-list
command may move the region at the allocation cursor directly into Free-List
Collection Owned backing storage, or move an obsolete backing region directly
into Dirty Free, because neither operation transfers responsibility to a
different owner.

Ready Free:

A region is Ready Free when it occurs at a logical free-queue position in
`[allocation, ready)`. Only the entry at `allocation` may be consumed. An
ordinary cross-owner allocation becomes Transaction Owned after its transaction
allocation entry is durable.

A free-list-internal growth command may instead consume that same entry and make
it Free-List Collection Owned without passing through a transaction. This
provieds a direct transition from Ready Free -> Collection Owned but only when
moving the region internally to the Free Queue collection.

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
root of exactly one collection. The collection may be a user collection or an
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
