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

Borromean is an append-only store. An update appends a new representation
instead of overwriting the previous one. Each collection defines how updates and
deletions are represented.

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

Free regions are kept in a FIFO queue. When space is needed, allocation takes
the oldest ready entry. Using a FIFO guarantees that no region is returned to
use before any free region that entered the queue earlier.

This does not guarantee equal wear across the whole device. Regions holding
long-lived data are not available for reuse and therefore do not take part in
the FIFO cycle. Borromean does not move live data solely to balance wear.

A second consideration for working with flash is accommodating its write-erase
nature. Once a flash cell has been written, it cannot be updated to a new value
without first being erased. This differs from magnetic storage, where a value
can be overwritten directly from A to B. Erase latency would make foreground
allocation and write latency unpredictable if it were performed on demand.
Borromean therefore splits the free list into dirty and ready ranges. Freed
regions enter the dirty range and move to the ready range only through explicit
caller-requested erase maintenance. Allocation, logical free, and writes to
newly allocated space do not perform that erase work themselves.

Repeatedly updating a fixed database-root location would wear out that part of
the flash before the rest. Borromean therefore uses a rolling root location: the
main-WAL tail moves through the region area as the database changes.

The fixed database header contains only immutable facts, such as the database
geometry and physical storage parameters. At startup, Borromean uses those
facts to locate the region headers. It scans all region headers to find the
current WAL tail, which points to the retained WAL head. The WAL range from the
retained head through the current tail is the root of the database. Replaying
that range recovers the current collection roots.

Larger regions reduce the number of headers that must be scanned at startup,
but region size also affects RAM use. Region size therefore trades startup scan
time against required RAM.

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

The database header is written when the database is formatted and does not
change during normal operation. It contains only the immutable facts needed to
interpret the store:

1. A Borromean format marker, an explicitly supported format version, the
   encoded metadata length, and an integrity check.
2. The database-header span length, erase-block size, region size, region count,
   logical write granule, and erased byte.
3. Format-time capacity limits that recovery must know, such as the number of
   transaction-log slots.
4. Values needed to recognize encoded data, such as the WAL record marker.

Runtime tuning settings are not stored in the database header because they do
not change the meaning or layout of stored data.

The header does not contain a mutable WAL head or tail, allocator cursor,
collection root, or other changing database state. The configured database
length is:

```text
database-header span + (region count * region size)
```

The logical storage range presented to Borromean must have exactly that length.
A physical device may be larger, but bytes outside the presented range are not
part of the database.

On open, Borromean validates the header before scanning region headers. It first
checks the format marker and metadata integrity, then uses only a decoder that
explicitly supports the stored version. It does not guess compatibility or
silently upgrade the format. It rejects zero or overflowing geometry, a range
length mismatch, spans or starts that violate erase-block alignment, a logical
write granule incompatible with the physical write size, an erased-byte
mismatch, or fixed capacities that cannot fit in the configured region count.
A validation failure does not modify storage.

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

Each allocation receives the global allocation sequence assigned when its
free-list entry is consumed. When the allocated region is initialized, its
header records that allocation sequence. Allocation sequences never wrap or
repeat. If an allocation would require a value after the largest value the
encoding can represent, it returns `SequenceExhausted` before issuing media
I/O. Existing data remains readable; obtaining more sequence space requires an
explicit migration or reformat.

A region index names only the reusable byte range. Its current role and
responsibility derive from retained structures, and its bytes are interpreted
only after structure-specific validation.

A logical region is a region index paired with the allocation sequence assigned
to that use of the byte range. Every durable reference used to interpret a
region's contents names a logical region. Before following the reference,
Borromean validates the target header and requires its allocation sequence,
collection, and format to match the reference's expectations. A mismatch makes
the reference invalid: Borromean does not follow it, interpret the target, or
use it as evidence that the target is reachable. Records that name a physical
region only to allocate, free, erase, or perform cleanup need not interpret its
contents and may identify the region by index alone.

Borromean uses a logical byte-oriented storage interface with four core
operations:

1. `write(address, data: &[u8]) -> Result<(), Error>`
2. `sync(address, length) -> Result<(), Error>`
3. `read(address, length, consume: FnOnce(&[u8]) -> R) -> Result<R, Error>`
4. `erase(address, length) -> Result<(), Error>`

The interface is logical rather than a direct representation of the physical
device API. A `FlashIo` implementation is responsible for expanding unaligned
reads, splitting transfers, widening range sync into a global barrier when
necessary, and performing any lower-level work needed to satisfy these
guarantees.

`write()` stages data beginning at `address` and spanning `data.len()` bytes.
The address must be write-granule aligned, and the length must be a multiple of
the write granule. Every granule in the requested range must still be erased:
it must not have been programmed since its last erase. Alignment, bounds, and
erased-range checks occur before any device program operation; if one fails,
storage is unchanged.

After programming begins, an error may leave the requested range unchanged,
completely written, or torn. A torn write consists of zero or more complete
leading granules, followed by at most one partly programmed granule; later
granules remain erased. The write granule is therefore an alignment unit, not
an atomicity guarantee. Bytes outside the requested range do not change. A
caller cannot assume that the requested range remains erased after such an
error.

After `write()` succeeds, later reads under continuous power observe all the
requested data. The write is not guaranteed to survive power loss until a
covering `sync()` succeeds, although an implementation may make it durable
earlier.

The address and length passed to `sync()` must be write-granule aligned and
within the configured storage range. A zero-length sync at an aligned, in-range
address succeeds without invoking a backend barrier and adds no durability
guarantee.

After a nonempty `sync(address, length)` succeeds, every granule written by an
earlier successful `write()` and covered by the requested range is durable.
These guarantees compose across successful sync calls: once their ranges have
covered every granule of a write, that complete write is durable. The requested
range is a minimum guarantee. An implementation may synchronize a larger range
or all storage, and callers must not depend on writes outside the requested
range remaining non-durable. On directly programmed NOR flash the operation may
be a no-op because successful writes may already be durable.

Sync changes durability only; it does not change the bytes visible under
continuous power. Alignment or bounds rejection occurs before invoking a
backend barrier and adds no durability guarantee. Once a backend barrier is
attempted, an error may leave none, some, or all earlier write effects durable,
including effects outside the requested range if the implementation widened
the operation. Previously durable data remains durable, but the error does not
identify which additional effects became durable.

`read()` accepts an unaligned range within either the fixed database-header span
or one region. A successful read calls `consume` exactly once with a contiguous
borrowed slice containing exactly the requested bytes and returns the value
produced by `consume`. The slice is valid only during that call; the callback may
copy or interpret the bytes, but it cannot retain the slice after returning. A
zero-length read at an in-range address performs no device transfer and calls
`consume` with an empty slice. A read cannot exceed the region size. A larger
logical value must be processed through multiple reads.

Under continuous power, the slice contains bytes from the most recent successful
writes. If power is lost before a covering sync, recovery may observe an
unsynced write as absent, complete, or torn at an allowed underlying write
boundary. Recovery produces one resulting storage image; repeated reads after
restart observe that stable image.

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

There are two internal collection types used by Borromean core to manage region
and record lifecycles:

1. The Write Ahead Log (WAL)
2. The Region Free List

The WAL has two parts: the shared main WAL and transaction regions assigned to
individual transactions.

Other higher-level collection types are defined by consumers of Borromean core,
each defining their own records, operations, region formats, and reachability
rules. Core defines and enforces the transitions that move responsibility for a
whole region among the Region Free List, transactions, and collections. A user
collection guarantees that each transaction allocation it incorporates is
reachable from its committed basis. Core relies on that contract
without interpreting the collection's data. Core defines which regions belong
to its internal collections and when those regions may be reclaimed.

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
state. The allocation state includes the allocation cursor and the next
allocation sequence. A command that consumes the entry at the allocation cursor
records the resulting logical region, `allocation_head_after`, and
`allocation_sequence_after`. The logical region pairs the consumed entry's
region index with the current next allocation sequence. The sequence after is
that value plus one.

The operation preflights space for the command and checks that the sequence can
advance before issuing media I/O. It writes and syncs the command before
applying both after-values to runtime state. Advancing the allocation cursor and
the allocation sequence is one durable transition. No other top-level mutation
may interleave with it.

For an ordinary allocation the command is the transaction allocation entry. A
transaction allocation and a free-list-internal allocation may both consume the
region at the allocation cursor. Each follows the same transition and returns
or retains the logical region rather than a bare region index.

Different transactions store allocation entries in different transaction logs.
The physically last allocation record observed in any one log is therefore not
necessarily the newest allocator state. Replay uses the retained
allocation-consuming command with the largest valid
`allocation_sequence_after` and restores both after-values recorded by that
command. Transaction cleanup frees are written in their ordered main-WAL
cleanup range and advance the append cursor; they do not advance the allocation
sequence.

Free-list appends update the WAL and the in-memory frontier; they do not modify
a materialized region. Materialized free-list regions are immutable. When the
current frontier must be materialized into its already reserved region `n`, the
free list first uses a free-list-internal allocation command to consume a Ready
Free region as reserved successor `n+1`. The command retains `n+1` as a logical
region. This does not transfer ownership. The free list then writes and syncs
the complete frontier into region `n`, including the link to `n+1`, and writes
and syncs a free-list tail-advance command in the WAL. The tail-advance command
publishes `n` as the materialized tail and `n+1` as the new reserved successor.
Runtime tail state advances only after that command is durable.

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

Allocation entries use WAL framing and contain the allocated logical region,
`allocation_head_after`, and `allocation_sequence_after`. The containing
transaction establishes initial transaction ownership; a durable next-segment
link or committed collection operation establishes the region's later
structural role. Free intents are a packed list of collection-owned regions
proposed for transfer to transaction cleanup on commit. Collection operations
describe the private collection changes.

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
the committed collection basis reaches it. A committed free intent
stops being collection-owned and becomes transaction-owned cleanup work. A
collection implementation is responsible for ensuring that every allocation it
uses is reachable from its committed basis; core assumes this for
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

Region lifecycle names are relationships derived from durable structures, not
values stored in a region table. Borromean keeps no persistent or runtime
lifecycle state for every region. A region's relationship follows from a
free-list entry, a retained transaction obligation, or a retained committed
collection basis.

Retention is separate from ownership. A region is retained when recovery may
still need its contents or may need to finish work that refers to it. A retained
region cannot be reclaimed or reused.

The common recyclable collection-data path is shown below. The collection may
be a user collection or an internal collection.

```text
Ready Free -> Transaction Owned -> Collection Owned -> Transaction Owned -> Dirty Free -> Ready Free
```

Rollback may move a new allocation directly from Transaction Owned to Dirty
Free. These arrows summarize common changes in derived relationships; they are
not a stored state machine.

Every region must be accounted for exactly once: it is either Ready Free, Dirty
Free, owned by one transaction, or owned by one collection. A region must never
be used in more than one of these ways, and no region may be left unaccounted
for. Free-list backing regions are owned by the Region Free List.

A transaction owns an allocated or detached region until a collection takes
ownership or cleanup returns it to the free list. Core preserves exact-once
accounting through its operations and replay. User collections preserve it
through their reachability contracts. Foreground operation and recovery replay
derive the same accounting from the same durable records.

Free-list backing regions have additional collection-local paths. A free-list
command may use the region at the allocation cursor as new backing storage for
the Region Free List, or move an obsolete backing region directly into Dirty
Free. The Region Free List remains responsible for the region throughout either
move.

Ready Free:

A region is Ready Free when a free-list entry naming it lies in
`[allocation, ready)`. Only the entry at `allocation` may be consumed. An
ordinary allocation becomes Transaction Owned when its durable transaction
allocation entry consumes that free-list entry.

A free-list-internal growth command may instead consume that same entry and use
the region as new backing storage for the Region Free List. The Region Free List
remains responsible for the region throughout this move.

Transaction Owned:

A newly allocated region becomes Transaction Owned when its transaction
allocation entry becomes durable. It remains Transaction Owned until a
committed collection basis reaches it or cleanup returns it to the free list.

A staged free intent leaves ownership unchanged. If the transaction commits,
the detached region becomes Transaction Owned until cleanup returns it to the
free list. If the transaction rolls back, the region remains Collection Owned.

Collection Owned:

A user collection owns every region reachable from its retained committed
basis. Core relies on the user collection's reachability contract.

An internal collection owns each region its retained basis depends on. Core
defines when that region may be reclaimed.

A transaction region remains retained while any retained WAL record refers to
it.

Dirty Free:

A region is Dirty Free when a free-list entry naming it lies in
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
