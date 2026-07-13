# Goals And Locality

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose and motivation

Borromean is a fast, multi-collection and multi-collection-type storage system
that natively implements wear leveling and crash recovery. Correctness,
predictable I/O, and wear distribution are coequal architectural properties.

Here, speed means predictable physical work as well as wall-clock performance.
The design preserves locality, avoids pointer chasing, bounds searches, and
makes durability barriers visible to the caller. Wear leveling and crash
recovery shape allocation, encoding, and publication from the beginning of the
design.

Borromean divides the device into fixed-size erase regions. A region is the
physical unit of ownership, erase, and reuse used throughout this chapter.

## Design goals

- The generic core supports multiple collection instances and formats while
  collection implementations own their logical record semantics.
- Every region-owning state has an intentional foreground, crash-recovery, and
  reclaim interpretation.
- Crash correctness, physical I/O patterns, and measurable resource limits are
  all conformance concerns.
- Related writes remain local to their active region, while structures that
  cross regions provide an explicit bounded traversal order.
- Startup, foreground work, recovery, and maintenance perform work proportional
  to declared inputs and retained structures.
- The durable and in-memory representations balance immediate persistence,
  bounded RAM, recovery work, packing density, and query efficiency.

These goals guide review of the complete design. The traceable requirements at
the end of this chapter are the concrete, testable obligations derived from
them.

## Regions, locality, and wear leveling

The database divides its byte range into relatively large, fixed-size erase
regions. Metadata fixes the region size, count, and discovery-header offset, so
the physical start and header location of any region index can be calculated
without reading variable-length data. Geometry validation rejects indices
outside the configured region count before issuing device I/O.

Reusable regions form a logical FIFO queue addressed by monotonic
`FreeQueuePosition` values. Allocation consumes the region at the queue's
current allocation position. Because FIFO order follows reuse history rather
than physical address, the next region can be numerically before, after, or far
from the current region. A structure spanning several regions therefore uses
retained handles and encoded traversal order instead of physical adjacency.
The queue's durable state consists of an immutable free-space basis plus later
ordered facts that advance its allocation, readiness, and append positions.

Append-framed structures such as the WAL have an active region and an append
cursor. Before writing, the core encodes a complete batch in memory. The
physical record extents include framing, checksum, byte stuffing, alignment,
and the append format's canonical padding through the write-granule boundary.
The batch is written at the cursor, and its complete physical length determines
the next cursor. The stream continues in the region while the next complete
extent and its control reserve fit; otherwise it links a FIFO-allocated
successor.

A bounded materialization starts from a complete logical input and produces a
collection-defined image with known bounds. Its collection format chooses the
layout, padding, and physical write spans needed for that image. This allows a
map to build sorted ranges and an object log to pack variable-sized payloads
without imposing the WAL append layout on collection data.

### Region and append requirements

1. `CORE-LOC-007` Fixed metadata geometry MUST compute the physical start and
   discovery-header offset of every valid region index without reading
   variable-length device data, and MUST reject every out-of-range index.
2. `CORE-LOC-002` The physical region selected for allocation MUST be the region
   named at the current `FreeQueuePosition`, independent of its numerical
   distance or direction from the previously active region.
3. `CORE-LOC-001` In an append-framed area, each logical append batch MUST begin
   at the current append cursor, contain the complete write-granule-rounded
   physical extents of its records including canonical padding, and advance the
   cursor to the end of that batch. A later append MUST NOT overwrite an earlier
   extent.
4. `CORE-LOC-008` An append-stream writer MUST accept the next complete aligned
   physical record extent whenever that extent plus the stream's declared
   control reserve fits. It MUST NOT close the region or allocate a successor in
   that case, and MUST NOT partially append the record when it does not fit.

## WAL, memory frontier, and materialization

The system intentionally uses four representations because immediate durable
append, bounded memory, efficient checkpointing, and final query layout require
different information.

1. The **WAL** stores each accepted mutation promptly in receive order. At that
   moment the core does not know the complete set of records that will share a
   future data region. Append separators, byte stuffing, checksums, alignment,
   and padding make an unknown-length stream recoverable after a torn write, at
   the cost of additional space and a receive-order layout that is inefficient
   for collection queries.
2. The **collection memory frontier** holds the corresponding logical mutations
   in bounded caller-owned memory. Reads combine immutable materializations with
   this frontier, so durable recent changes remain queryable before a flush. The
   frontier also accumulates the complete input needed to plan one efficient
   materialization.
3. A **WAL snapshot** serializes the complete logical state of a memory frontier
   into one self-contained collection-specific WAL record. Its single append
   frame and checksum cover the snapshot payload as a whole; entries inside the
   payload share that frame rather than carrying individual WAL framing. The
   shared WAL can therefore checkpoint a frontier alongside records for other
   collections. The snapshot becomes a compact durable basis for later updates,
   bounds replay, and allows the resident frontier buffer to be released.
4. A **materialization** reorganizes that complete bounded input before writing
   it as a bounded collection-format image. A map can sort keys and build
   SSTable-like ranges or indexes to reduce search. An object log can preserve
   its required logical order while packing metadata and variable-sized
   payloads densely. The enclosing format supplies the entry bounds and
   integrity rules for this bounded image, while the collection format declares
   its physical write pattern.

Snapshots and materializations are alternative physical representations of the
same logical collection state held by a frontier. Encoding, eviction, reload,
and recovery must therefore preserve the collection's read-visible results.

This design deliberately spends a bounded amount of RAM to improve flash
density, query cost, and region locality. Each resident frontier slot must hold
every logical record and planning structure needed for one materialization
unit, while configuration declares how many collection frontiers may be
resident simultaneously. Before accepting a durable mutation, the operation
preflights that the mutation can also be represented in a frontier slot;
otherwise it requests explicit snapshot or collection-flush maintenance before
media I/O.

### Representation requirements

1. `CORE-LOC-010` Each collection format that defers materialization MUST
   declare the maximum logical input and planning-memory requirement for one
   materialization unit. Its caller-owned frontier-slot capacity MUST cover that
   declared requirement, and admission MUST return maintenance pressure before
   device I/O when the next mutation would exceed it.
2. `CORE-LOC-012` For every input within its declared materialization bound, a
   collection materialization's decoded query results MUST equal the collection
   reference model before materialization.
3. `CORE-LOC-013` Decoding a collection WAL snapshot without any superseded
   update record MUST reconstruct the same read-visible logical state as the
   bounded memory frontier encoded by that snapshot.
4. `CORE-LOC-014` Collection memory configuration MUST declare the count and
   capacity of resident frontier slots. After a replacement snapshot or
   materialization is durable, releasing and later reconstructing its clean slot
   MUST preserve the collection's read-visible state.

## Retained roots and bounded traversal

The WAL is the logical database root. Its retained head and selected tail bound
the record range that recovery replays. Records in that range select immutable
collection materializations, free-space bases, snapshots, transaction outcomes,
and maintenance cursors.

WAL records name the immutable collection materializations over which a memory
frontier applies. A retained WAL snapshot replaces the earlier frontier updates
whose complete logical effect it contains, and later updates replay over that
snapshot. Each update remains retained until replay can start from a later
self-contained snapshot or durably published replacement materialization that
contains its logical effect.

The committed root for one collection is selected with a separate rule. If a
committed memory frontier is resident, that frontier is the collection root and
may refer to an older immutable basis beneath it. If no memory frontier exists,
the root is whichever appears later in WAL order: the newest collection
snapshot record or the newest collection `head` record. A `head` record
publishes an immutable collection materialization. Their WAL positions provide
the order used for root selection.

Snapshots are intentionally interstitial. They checkpoint a frontier inside the
shared WAL, allowing checkpoints from several collections to share WAL regions.
When a snapshot exceeds the collection's declared WAL size or reserve limit,
the same logical state is flushed as a collection materialization. A later
mutation reloads a WAL-snapshot basis into a frontier slot and continues with
subsequent update records.

Every long-lived structure therefore has:

- one retained start or direct handle;
- a deterministic rule for deriving the next handle from the current element
  and operation input;
- bounded metadata that identifies the relevant range, index, cursor, or
  maximum number of visited regions; and
- an explicit terminal condition.

This applies to WAL chains, free-space bases, transaction cleanup lists, and
collection manifests. A collection can choose different logical indexes, but
its reads are still bounded by the retained handles, metadata, and limits in its
operation specification. Cleanup lists use the same structure: obligations are
encoded in execution order, and a durable cursor identifies the next item so
recovery can resume directly after a crash.

Traversal stops at the encoded terminal condition. A missing terminal, cycle,
or chain longer than the configured visit limit produces a typed error before
the traversal reads beyond that limit.

### Root and traversal requirements

1. `CORE-LOC-011` When a committed collection memory frontier is resident, a
   committed read MUST use that frontier and its referenced immutable basis as
   the collection root.
2. `CORE-LOC-015` When no committed memory frontier is resident, replay MUST
   select the later WAL position between the newest retained snapshot and the
   newest retained collection `head` as that collection's root.
3. `CORE-LOC-003` Traversal of an encoded cross-region structure MUST begin from
   one retained start handle, determine each next handle solely from the current
   decoded element and operation input, stop at an explicit encoded terminal
   condition, and fail before visiting more than the configured maximum number
   of regions. It MUST read only the regions yielded by that traversal.
4. `CORE-LOC-005` A collection read operation's media-read trace MUST contain
   only regions reached from its specified direct handles, manifests, ranges,
   indexes, and cursors, and its read count MUST remain within that operation's
   configured bound.
5. `CORE-LOC-004` Cleanup obligations MUST be encoded in execution order with a
   durable cursor naming the next obligation; after each possible crash cut,
   recovery's next cleanup read MUST address the obligation named by that
   cursor.

## Allocator ordering and checkpoints

Allocation facts can be recorded in the main WAL or in several open transaction
logs, so physical record positions do not provide one allocation order. The
global allocator supplies that order. It holds its allocation lock while it
reads the current allocation head and sequence, appends and syncs the allocation
fact, advances the in-memory head and sequence, and returns. Each fact records
its allocation sequence and resulting `allocation_head_after`.

An allocator checkpoint is a consistent pair of allocation sequence and
allocation head. Each valid WAL region begins with a preamble that publishes
recovery metadata for that tail, including an allocator checkpoint. The selected
free-space basis carries the same pair. Recovery starts with the newer of those
checkpoints, orders later retained allocation facts by sequence, and obtains the
final head from the
`allocation_head_after` in the greatest later fact. When a retained checkpoint
incorporates an old fact's cursor effect, WAL reclaim can remove that fact after
separate ownership and cleanup evidence has also been retained.

## WAL-rooted startup

Wear leveling moves the WAL through FIFO-allocated regions over the life of the
store. Keeping its current location in one repeatedly updated physical slot
would concentrate wear in that slot. Startup instead uses the fixed geometry to
read one discovery header at the known offset of every region. This is the only
phase that discovers a structure by inspecting every physical region.

A valid region header identifies its purpose. WAL candidates use system
collection ID `0` and system type `WAL`. Their valid preambles carry monotonic
WAL sequences, so the greatest sequence identifies the current tail. Replay can
then find multiple undecided open transactions and at most one transaction with
a durable decision but no finish record, as defined by the transaction finish
lock.

Startup then performs one ordered recovery process:

1. read discovery headers in ascending physical-region order and retain each
   valid header's location and purpose;
2. validate the preambles of the collection-ID-`0`, system-type-`WAL`
   candidates and select the valid candidate with the greatest WAL sequence as
   the current tail;
3. obtain the retained WAL head and allocator checkpoint from that tail
   preamble;
4. replay the WAL from retained head through selected tail to reconstruct
   ownership, free-space state, collection roots and frontiers, transaction
   state, and maintenance cursors;
5. finish the possible transaction that has a durable commit or rollback
   decision without a finish record; and
6. after releasing that transaction's finish lock, roll back the remaining
   undecided open transactions in durable begin order.

After the header pass selects the WAL, recovery reads only the WAL chain and the
ordered structures selected by replay. The header pass supplies WAL candidate
locations; replay supplies collection, allocator, and transaction state.

### Startup locality requirements

1. `CORE-LOC-009` After reading metadata, the fixed startup discovery pass MUST
   issue exactly one logical read of the fixed discovery-header range for every
   physical region in ascending physical-index order and MUST read no region
   body before that pass completes.
2. `CORE-LOC-006` Outside the startup discovery pass, an operation MUST NOT read
   a region's discovery header unless that region index was supplied directly
   to the operation or reached from a retained root through the format's
   declared traversal rule.
