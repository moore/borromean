# Journal

This journal captures design ideas, motivation, and decision history before they
are ready to become normative specification text.

## 2026-06-20: Put the Programmer in Control

A recent conversation about TinyFS highlighted an issue in Borromean: a write
that needs a new region can force an erase cycle under the current design. Erase
may be slow and a write can unexpectedly turn into an erase causing a unpredictable cost for write. I think this is
part of a bigger issue where one operation may imply another operation. If we
are out of buffers, a read could force a write of another collection to free the
buffer.

The general rule should be that ordinary foreground operations do not hide
unbounded maintenance work. They may report pressure or return a status that
says maintenance is needed, but the caller should stay in control of when erase,
reclaim, compaction, or cross-collection flushing happens.

### Fixing Hidden Erases

The allocator should stop treating allocation as the place where erase happens.
The allocator should become one storage-private logical collection that owns one
FIFO of free-region entries. Every entry is appended as dirty, then becomes
ready when erase maintenance advances the ready boundary, then is consumed from
the head when allocated. This free-space collection should not be user-visible.
It is allocator-private state, but it should use the same WAL-first management
style as collections: WAL updates for ordinary changes, with snapshots or region
materialization available when the allocator metadata needs to be compacted.

The dirty and ready states are ranges in the same queue rather than separate
queues. The free-space collection tracks the allocation head, the ready
boundary, and the append tail. Entries from the allocation head up to the ready
boundary are pre-erased and ready to allocate. Entries from the ready boundary
to the append tail are dirty and must be erased before allocation. The queue
frontier, snapshots, and materialized regions can therefore share one internal
buffer. Any links between materialized queue regions are free-space collection
metadata; they are not stored in the freed data regions themselves.

The goal is to make the WAL the sole source of allocator truth. The previous
allocator state was split between WAL records and allocator metadata stored in
free regions. Moving to a storage-private free-space collection removes that
special region footer machinery and doubles down on the core design: "use the
WAL".

The free-space collection can be driven by three commands.
`free_region(region_index, append_tail_after)` appends a released region at the
tail as dirty. `erase_free_region_span(count, ready_boundary_after)` erases one
or more entries starting at the ready boundary and then advances the ready
boundary through those entries. `allocate_region(region_index,
allocation_head_after)` removes the head ready entry and reserves that region
for writing. The commands carry their resulting cursor updates so replay and
cleanup can validate the expected transition.

Freeing a region should be cheap: append
`free_region(region_index, append_tail_after)`. Preparing capacity should be a
bounded maintenance operation: erase some number of dirty regions and then
append `erase_free_region_span(count, ready_boundary_after)` to publish the
ready-boundary bump. The
ordering is erase first, then publish the boundary transition. There does not
need to be a separate sync between the erase and the ready WAL record. If power
fails before the ready record is durable, replay still sees those entries as
dirty and may erase them again. If power fails after the ready record is
durable, the ready record is the evidence that the erase completed before
publication.

Allocation should take a pre-erased region from the ready range and should not
perform erase itself. For ordinary collection work, allocation should lean on
the improved transaction model instead of adding a separate `consumed()`
command. A `consumed()` marker would create another mini-protocol for
allocated-but-not-yet-linked regions. Transactions already provide the right
shape: `allocate_region(region_index, allocation_head_after)` is
transaction-owned until commit, and the collection state change that makes the
region reachable becomes visible at the same commit boundary as the allocator
pop.

If the system crashes before commit, recovery rolls back the transaction-owned
allocation. If the reserved region may have been partially written, rollback
returns it to the dirty range, not the ready range, so later erase maintenance
can prepare it again. If the system crashes after commit, replay applies the
allocator pop and the collection state update together. This costs more buffers
because allocation now depends on transaction machinery, but that is an
acceptable tradeoff for an explicit embedded API: the memory requirement is
visible instead of hidden in allocator recovery complexity.

The allocator still needs a ready-region reserve so user allocation cannot
consume the last ready region needed for WAL rotation, recovery, transaction-log
growth, or allocator maintenance. Storage-core allocation paths that are needed
to run the transaction system itself may still need a privileged non-ordinary
path, but normal user/data collection allocation should be transaction-scoped.
A store with many dirty entries but too few ready entries should report
ready-region pressure instead of letting an ordinary write silently run erase
work.

All write operations on collections should return a maintenance status that
indicates whether there is free-region pressure, so the caller does not have to
poll after each write to know whether they need to run erase, reclaim, or
collection compaction. I do not know whether that status should include full
ready/dirty list sizes or just low-water marks plus an explicit stats query.

### Fixing Hidden Writes on Read

When I started this, I thought it would be a feature that callers would not have
to think about how many buffers they needed and that buffers would be swapped on
demand. I still think it is a good idea for collection handles to be small and
not hold buffers, but buffer allocation should be under caller control instead
of automatic. The current API can silently turn reads into writes if a buffer
needs to be freed to support the read, or amplify writes if a write needs to
flush another collection to finish.

Public reads should be storage-write-free. A read may read the device and mutate
temporary reader state, but it must not perform durable writes, flush another
collection, erase, reclaim, compact, or evict dirty state. If a read cannot
complete with the buffers supplied by the caller, it should fail explicitly with
a capacity or pressure error.

For now, the low-level API should focus on explicit embedded-system control
rather than a high-level ergonomic abstraction. Predictability and ownership are
more important for the target use case. Collection/object handles should stay
small and not hold buffers. Using a handle should require an explicit operation
object that borrows the buffers it will use. A `Reader` borrows read scratch. A
normal `Writer` borrows ordinary write scratch. A `TransactionWriter` borrows
the additional transaction buffers needed for a caller-visible transaction.
Requiring transaction buffers only for `TransactionWriter` keeps simple writes
from paying the full transaction-memory cost up front.

Some ordinary writes may still need allocation or other internal multi-command
work that must be atomic. If no caller-visible transaction is active, storage
should be able to run a short WAL-only transaction directly in the main WAL.
This gives us two transaction start forms: the full transaction start that
points at a transaction log for longer caller-visible transaction work, and a
bounded inline/WAL-only transaction start for short storage-internal atomic
groups. The inline form is not a competing public transaction API. It is an
implementation tool for operations such as allocation when the caller is using a
normal `Writer`.

Inline transactions should be bounded before they start. Storage should know the
maximum record count or encoded WAL length, ensure enough WAL room up front, and
rotate before beginning if necessary. If a full transaction is already active,
allocation joins that transaction instead of nesting an inline transaction. If
an inline transaction crashes before commit, replay ignores its effects and
rolls back any transaction-owned allocation. If its commit is durable, replay
applies the allocator and collection updates together.

A higher-level API backed by `StorageMemory` or other internally managed buffers
may still be useful later, but it should be layered over the explicit API rather
than driving the core design.

We should also clean up the API to take buffer-provider objects instead of
taking `&mut [u8]` or similar directly everywhere. The exact trait shape can
come later; we may need different providers for fixed scratch, region-sized
buffers, DMA-aligned buffers, or payload/range buffers. The goal is to manage
buffer lifetimes better and support buffer pools. Specifically, Aranya expects
the `IoManager` to own all the buffers it needs, and our current API makes that
hard.

## 2026-06-09: Transaction Logs For Read-Committed WAL Transactions

The current WAL transaction design is useful for internal multi-record recovery,
especially allocation and freeing, but it is not a good general transaction
model. It practically supports only one open transaction at a time because the
transaction interval lives inline in the ordinary WAL. It also lets foreground
execution expose collection updates before `commit_transaction`: recovery is
all-or-nothing, but the in-memory logical state is not read-committed. That is a
poor fit for the common ACID expectation that transaction effects become visible
only at commit.

One possible replacement is to give storage a fixed number of transaction logs.
Each transaction log is its own WAL chain for one active transaction stream. The
ordinary WAL still serializes transaction control decisions, but its start,
commit, rollback, and finish records point to positions in a transaction log
instead of containing the full transaction interval inline.

Beginning a transaction should be separate from adding a collection to that
transaction. `begin_transaction` creates an empty transaction context and
selects the transaction log that will receive transaction-scoped records. A
collection joins the transaction through a separate add-collection step. That
step requires one frontier buffer for that collection, copies the collection's
current frontier into the transaction-owned buffer, and records the collection
state that commit will later validate for conflicts.

A transaction writes its enrolled collection updates, allocation records, and
other transaction-scoped records into the selected transaction log. The
collections' ordinary mutable frontiers are not updated directly. Instead, the
transaction applies those records immediately to the private frontier buffers
for the enrolled collections. Those private-buffer changes are not visible to
ordinary reads until commit. On commit, the ordinary WAL records a commit
decision that freezes the covered transaction-log range. During recovery, that
frozen range is replayed at the position of the ordinary-WAL commit record. In
foreground execution, each private frontier buffer is swapped in as its
collection's mutable frontier. At that point the commit is logically complete
and transaction data becomes visible.

If the transaction rolls back, the covered transaction-log range is scanned for
transaction-owned regions or other storage effects that must be freed or
invalidated. The private frontier buffers are discarded instead of becoming
collection frontiers. This keeps rollback as an internal storage operation, but
the visibility boundary becomes explicit and read-committed.

Multiple transaction logs allow the database to support as many concurrent
transactions as there are logs. A single transaction log may contain a sequence
of transactions, but only one transaction may be open in that log at a time.
Transaction-log regions can be reclaimed once no ordinary-WAL commit record and
no open transaction still points into them.

Current decisions:

- Public storage, collection, and transaction operations are non-reentrant.
  Every call into a `Storage` method must be serialized, normally by requiring
  `&mut Storage`. Collection and transaction objects should follow the same
  rule, with transaction closures providing the natural scoped exclusive access
  pattern for transaction-local operations.
- If a collection frontier no longer matches the state observed when that
  collection was added to the transaction, commit fails with a transaction
  conflict. The base storage model does not replay transaction commands on top
  of a newer frontier. This is the safest rule for the current embedded use case
  and keeps commit behavior deterministic. Future collection implementations may
  define explicit merge/rebase semantics for selected command types, but that
  would be collection-specific behavior layered above the base transaction-log
  protocol.
- A transaction commit must durably record the ordinary-WAL commit decision
  before any private frontier buffer is installed as visible collection state or
  before the commit is acknowledged to the caller. If a transaction enrolls
  multiple collections, installing the private frontiers must be atomic with
  respect to other public operations: no read or mutation may observe only part
  of the committed set.
- Collection-specific transaction buffers may remain bounded by spilling large
  transaction state into newly allocated regions. Those regions are private to
  the transaction until commit. Commit promotes them into the collection's
  visible state; rollback scans the transaction log and frees or invalidates
  them.
- Transaction-scoped allocation records may update the global allocator before
  the ordinary-WAL commit decision. The allocated regions are transaction-owned
  and private until commit. If the transaction rolls back, those regions must be
  returned through the same allocation recovery rules used by the current
  transaction model.
- Allocation records should carry a global `u64` ordering value assigned
  while storage has exclusive allocator access. Allocator access is globally
  serialized from selecting the current allocator head through durably recording
  the allocation decision in the ordinary WAL or a reachable transaction log.
  Recovery needs to order the allocation decisions, not the allocated regions
  themselves: after replaying the ordinary WAL and all reachable transaction-log
  ranges, the reachable allocation record with the largest sequence identifies
  the newest allocator head decision. Allocated regions younger than that
  decision are either reachable from live committed state, already returned, or
  transaction-owned regions that rollback/recovery must return.
- Every WAL-compatible log segment should checkpoint the allocator cursor in its
  segment prologue: the allocator head and ordering value that were current
  when the segment was initialized. This gives replay a durable
  baseline if an allocation/reservation record at the end of a log segment is
  torn or if an older log prefix containing prior allocation records has been
  reclaimed. Replay starts from the segment checkpoint and then applies only
  complete allocation records after that point; a truncated allocation record is
  ignored and does not advance the recovered allocator head.
- Transaction-log regions are reclaimed like ordinary WAL regions: only a
  reclaimable prefix may be freed, and only after no retained ordinary-WAL
  commit record, open transaction descriptor, or pending recovery descriptor
  points into that prefix. Storage must reconstruct and maintain transaction-log
  metadata from ordinary WAL replay and active runtime usage so it can identify
  each transaction log's live prefix boundary.
- Transaction logs should reuse the existing WAL region and record framing where
  possible. The main WAL and transaction logs are both private storage
  structures, so transaction-log pointers do not need a public generation or
  epoch field just to reject stale external references. Stale internal
  references should be rejected through ordinary WAL replay, transaction-log
  membership, and live-prefix reachability.
- Transaction-log head, tail, and append-position facts belong in the ordinary
  WAL records that point to transaction-log positions or ranges. `Storage`
  should track the corresponding per-log cursors, live-prefix boundaries, and
  active usage as ephemeral runtime state recovered from ordinary WAL replay and
  updated by active operations.
- Each collection should carry an in-memory `u64` committed state generation
  counter derived from ordinary WAL order. The generation changes only when an
  ordinary-WAL record makes a collection state, frontier, or basis change
  visible; for a transaction, the ordinary-WAL commit record advances the
  generation for every enrolled collection and covers the transaction-log
  changes. Appending or applying transaction-log records to private transaction
  buffers does not change the collection generation. When a collection is added
  to a transaction, the transaction object records that collection's current
  generation in a per-collection slot. Before commit succeeds, storage checks
  every enrolled collection slot against the current collection generation; any
  mismatch fails the transaction with a conflict.

## 2026-06-05: Large Objects v2

The current object-log design handles small objects well: each public handle
names one object-log record, unflushed data can be rebuilt from the WAL, and
truncation can advance through ordinary object-log order. Large objects need a
different path because sending their bulk bytes through the WAL is expensive,
but their storage layout still has to preserve simple truncation semantics.

This design separates public object-log records from private large-object data:

- A small record is a public inline object-log record. It stores the whole
  object through the ordinary frontier and WAL path, and it is used when the
  object fits within one chunk's logical capacity.
- A large record entry is a public object-log record and the public handle
  target for a large object. It stores the total logical object length and an
  optional pointer to the first auxiliary region.
- An auxiliary chunk is private large-object data in an auxiliary region. It
  records its logical length and checksum.
- A tail chunk is private large-object data written after the large record entry
  through the ordinary object-log path.

A large write uses one region-capacity scratch buffer. The writer fills that
buffer with auxiliary chunks until the buffer contains a complete auxiliary
region image, leaving only the reserved next-link slot. That full image is then
materialized as an auxiliary region. If another auxiliary region follows, the
previous auxiliary region's reserved next-link slot is written once to point to
the new region.

If the object ends exactly when scratch contains a full auxiliary image, that
image becomes the final auxiliary region. If the object ends with partial
scratch contents, the writer publishes the object through the ordinary
object-log path by appending the large record entry followed immediately by the
remaining private tail chunks. The large record entry and tail chunks are
contiguous in ordinary object-log order, even if that span crosses ordinary
object-log region boundaries.

Auxiliary regions are not ordinary object-log chain regions. Each auxiliary
region belongs to exactly one large object and is reachable only from that
object's committed large record entry. Public traversal sees small records and
large record entries; it skips auxiliary chunks and tail chunks. Truncating away
a large record entry frees its whole auxiliary chain, without retaining
unrelated object data beyond any ordinary log region that still contains the
live head.

Every auxiliary region is allocated and written inside the large-object
transaction. Before commit, those regions are transaction-owned and recoverable.
The commit publishes the large record entry that makes the auxiliary chain
reachable from exactly one object. If the transaction aborts before commit, all
reserved auxiliary regions are reclaimed. If it commits, the auxiliary data and
auxiliary links are durable before the large record entry becomes visible.

The current WAL transaction model still limits true concurrent large writes
because it supports only one open transaction. This design keeps the future
concurrency story simple: each in-flight large write needs one fixed-size
scratch buffer, small writes can continue while auxiliary data is assembled, and
only the final publish span serializes through the ordinary object log.

## 2026-06-05: Large ObjectLog Objects And Direct Region Materialization

The current object-log design reserves a stable handle for a packed frame in one
object-log data region, persists the object bytes through a WAL update, and later
flushes the in-memory frontier into that reserved region. That is a good fit for
small objects because the handle is stable before and after flush, and the WAL
contains enough information to rebuild an unflushed frontier after reset.

Objects larger than one region push against that model in two places. First, an
object frame no longer fits in the object-log frontier buffer or in a single
committed-region payload. Second, writing the full object bytes into the WAL is a
poor fit for large payloads because it double-writes the data and makes WAL
rotation and reclaim carry bulk object bytes that are already destined for data
regions.

A better direction is to keep WAL records small without abandoning partially
used object-log regions. A large append should be staged through the same
in-memory frontier used by ordinary appends. The first chunk consumes whatever
payload space is left in the current frontier. If the object continues after the
frontier becomes full, that full frontier image can be written and synced as a
committed object-log data region without also writing those object bytes to the
WAL. Subsequent full-region chunks follow the same rule: fill an empty frontier
image, commit it directly to a new data region, and skip the WAL bytes for that
full image. The final partial chunk and object-end record stay in the live
frontier and are persisted through the normal WAL path, so later appends can
still use the remaining space in that region.

The object-log handle should continue to be opaque and stable, but for a large
object it should name an object-end record rather than the first chunk. The end
record should hold the total logical object length and point into the run so the
reader can find both the end and the start of the object. The object run itself
should be a linked list of chunk records instead of a map-style manifest: the
number of regions in one object can be unbounded, and a manifest would either
need its own growth strategy or impose an artificial limit. Chunk records can
carry both forward and backward links. The writer can reserve or otherwise know
the next span before it finishes and commits the current full frontier image,
so the current chunk can point forward and the next chunk can point back to the
previous chunk. Backward links remain useful for future streaming writes because
each new chunk can be connected to the previous chunk before the final object
length is known. Forward links make normal start-to-end reads and validation
cheaper once the completed object is published. Later, the same linked structure
can grow skip-list-style links so reads can seek toward a byte offset without
walking every frame.

Continuation frames remain internal implementation details. Public reads,
traversal, and truncation should continue to operate on logical object
boundaries, with the handle identifying the completed object through its end
record.

The large-object path should still be transaction-backed. Every physical region
that becomes part of the object run must be reserved by the transaction before
it is written, so recovery can return those regions to free storage if the
transaction never commits. The transaction does not need to reserve the whole run
up front or pre-pad every region in the span:

1. Append `begin_transaction(collection_id)`.
2. Write the leading partial chunk, if any, into the current in-memory frontier
   and persist that frontier mutation through the ordinary WAL path.
3. Whenever a frontier image becomes full before the object-end record can be
   written, transaction-reserve storage for the next span before sealing the
   current image, fill the current chunk's forward link and the next chunk's
   backward link, write and sync the full object-log region image, and record
   only the small allocation/run metadata needed for recovery.
4. Repeat the full-frontier direct materialization step for any middle chunks.
5. Write the trailing partial chunk, if any, and the object-end record into the
   live in-memory frontier and persist them through the ordinary WAL path. If the
   final data byte exactly fills a frontier image, directly materialize that full
   image first and write the object-end record at the start of the next frontier.
6. Append a small object-log publish update that records the end-record handle
   and any run metadata needed for replay and validation.
7. Append `commit_transaction(collection_id)` so recovery keeps the published
   object state.
8. Append `transaction_finished(collection_id)` after any cleanup is complete.

If reset happens before commit, startup recovery should treat the append as
uncommitted and return the transaction-reserved regions to storage after erasing
or otherwise preparing them as required by the free-region rules. If reset
happens after commit but before `transaction_finished`, recovery is simpler than
stable-head replacement because object writing does not free old regions. In
that case recovery should keep the published object run and append
`transaction_finished` once allocator state and object-log state agree.

This means the large-object format should not require padding merely because an
object crosses a region boundary. The split is a normal span boundary between
declared chunks, not a reason to close out the region with artificial object-log
padding. Direct materialization happens only for full frontier images, and the
tail remains in the ordinary frontier/WAL flow. Any physical write-alignment
padding that the committed-region writer needs remains an implementation detail
of the storage write path rather than logical object padding.

The object-log frame format should distinguish chunk boundaries from object
boundaries. Every object span chunk records its chunk length and local span
metadata, including previous and next links or markers that the chunk is the
start or end of the run. Each chunk should also carry its own checksum. That
lets partial reads validate only the chunks they touch instead of needing a
whole-object checksum, and it keeps corruption localized to a specific run
segment. The object-end record declares the logical object length and completes
the run. Readers validate the declared chunk bounds, validate touched chunk
checksums, and follow run metadata, but bytes outside the declared chunks are
simply unused frontier capacity available to later appends.

The near-term API should stay slice-based. Callers that append an object provide
the whole object buffer to the existing append/write path, and the object log can
derive the total length from that slice while deciding how much lands in the
current frontier, how many full frontier images can be directly materialized,
and how much remains as the trailing frontier/WAL-backed chunk. This keeps the
first implementation smaller and avoids introducing a no-std streaming trait
before the storage format is settled.

Reads still benefit from a smaller-range API. A range read can take an opaque
handle, an object-relative offset, and a length, then return only that requested
committed byte range through caller scratch. For the first implementation, range
reads can validate the handle, data-region prologue, frame header, and requested
bounds without validating a whole-object checksum. Once the multi-region chunk
format exists, range reads should validate the per-chunk checksums for the
chunks they read. Full-object `get` can keep validating every chunk it returns.
A separate whole-span checksum can be revisited later if there is a concrete
need for it.

A streaming API can be explored later. If Borromean eventually needs append or
read APIs that do not require a whole object buffer, we should look at the
existing embedded Rust APIs to see if this problem has already been solved.

Open questions before this becomes normative specification text:

- What exact chunk and end-record fields are required for bidirectional
  traversal, startup validation, and future skip-list-style offset traversal.
- Whether directly materialized full frontier images need any storage write
  granule metadata beyond the committed-region writer's existing alignment
  rules.
- Whether a whole-span checksum adds enough value beyond per-chunk checksums to
  justify extra object-end metadata.
- How much run metadata belongs in the publish update, the object-end record,
  and committed data region prologues.
- How to bound the number of regions consumed by one append relative to
  `min_free_regions` and recovery's need to append terminal transaction records.

## 2026-06-01: WAL Transactions For Multi-Record Recovery

Staged regions are currently the explicit mechanism for making multi-region
operations recoverable. A WAL transaction layer should let the implementation
persist each partial step as a normal tagged command and rely on recovery for
incomplete transactions instead of adding special commands for staged state. The
purpose is atomic multi-step durability, not user-visible rollback.

A concrete target is stable-head replacement and reclaim. Build the new stable
head during the transaction update phase, then write `commit_transaction` as the
middle marker saying the collection state update is durable and must be kept.
After that, enter a cleanup phase that frees old regions by mutating the durable
allocator queue. The transaction is complete only after `transaction_finished`
has been written. This should remove the need for a fixed pending-reclaim limit
because free commands can be persisted and recovered as part of the transaction
instead of held as a bounded staged list.

Transaction markers:

- `begin_transaction(collection_id)`: starts the transaction interval for one collection and records
  the WAL position that recovery can jump back to after it has scanned the interval. Because the
  transaction is scoped to one collection, the collection id acts as the transaction tag.
- `commit_transaction`: ends the update phase. Before this marker, recovery abandons the
  collection-state update. After this marker, recovery keeps the collection-state update and must
  finish cleanup.
- `transaction_finished`: ends the cleanup phase. This marker means both the collection-state update
  and allocator cleanup completed, so recovery can replay the interval normally.
- `rollback_transaction`: records that pre-commit recovery already cleaned up an uncommitted
  transaction. Recovery can skip transaction-tagged commands in the interval and replay only
  non-transaction-tagged commands.

Required invariants:

- The update phase must make enough durable collection-specific information available before
  `commit_transaction` for cleanup recovery to know what regions need to be freed. The transaction
  layer should not need to understand collection reachability.
- Transaction-scoped commands are identified by the collection id from `begin_transaction`. Region
  allocation and free WAL records always carry the collection id of the operation that owns them.
  Although these records mutate global allocator state, allocation brings a region into a collection
  and free removes a region from a collection. Keeping the collection id explicit makes the WAL
  self-describing even when the owner could often be inferred.
- Commands for other collections are outside the transaction interval and may replay normally only
  if they do not depend on transaction-private allocator or storage effects. Otherwise, unrelated
  mutating commands must be forbidden while a transaction is open.
- Data recovery and cleanup recovery must be idempotent because storage open can crash before it
  writes `rollback_transaction` or `transaction_finished`.
- The min-free-region invariant must reserve enough WAL capacity for recovery to append terminal
  transaction records such as `rollback_transaction` or `transaction_finished`.

The sketch:

1. Append `begin_transaction(collection_id)`, recording the WAL position where the transaction
   starts.
2. Treat commands for `collection_id`, including region allocation and free commands carrying that
   collection id, as transaction commands until a terminal transaction marker is reached. Because
   Borromean would support only one open transaction at a time, a separate transaction id is not
   needed.
3. During normal foreground execution, append each collection-tagged command to the WAL and apply
   its storage and in-memory effects exactly as the same command would be applied outside a
   transaction.
4. After all transaction commands needed to update retained collection state have reached the WAL,
   durably write `commit_transaction`. The update phase must also leave enough durable
   collection-specific information for cleanup recovery to derive the required frees. This is the
   point where the new collection state becomes the committed state for recovery.
5. After `commit_transaction`, append cleanup commands that free superseded regions. These free
   commands carry `collection_id` because freeing removes the region from that collection. Freeing a
  region mutates durable allocator state by adding the region to the allocator queue, so cleanup is
   part of transaction recovery rather than passive bookkeeping.
6. After all cleanup commands are complete, durably write `transaction_finished`.
7. On storage open/recovery, replay can apply commands normally until it reaches
   `begin_transaction`. From that point, replay scans the transaction interval until it finds
   `transaction_finished`, `rollback_transaction`, or WAL end. During this first scan, replay skips
   ordinary commands for `collection_id` and region allocation/free records carrying
   `collection_id`. It only pays attention to transaction-control records, including
   `commit_transaction` as a phase marker.
8. If `transaction_finished` is found, replay jumps back to the transaction begin position and
   replays the full transaction interval in original order before continuing past
   `transaction_finished`.
9. If `rollback_transaction` is found, replay jumps back to the transaction begin position and
   replays only commands outside `collection_id` in the interval before continuing past
   `rollback_transaction`. Cleanup or data recovery is not repeated because the rollback record
   means it already completed.
10. If WAL end is reached before `commit_transaction`, replay jumps back to the transaction begin
    position and runs data recovery. On that recovery pass, commands in the uncommitted update phase
    are recovered instead of applied, reclaiming or completing any transaction-private storage
    effects as needed, while commands outside `collection_id` in the interval are replayed normally
    if they are independent of the failed transaction. Recovery then writes `rollback_transaction`.
    This recovery path must be idempotent if storage open crashes before the rollback marker is
    durable.
11. If WAL end is reached after `commit_transaction` but before `transaction_finished`, replay jumps
    back to the transaction begin position and runs cleanup recovery. The committed collection state
    is kept, cleanup recovery derives the remaining frees from durable collection-specific state,
    and allocator mutations are replayed or completed until allocator state is consistent with the
    committed collection state. Recovery then writes `transaction_finished`. This recovery path must
    be idempotent if storage open crashes before the finished marker is durable.

Collections may use this mechanism for their own multi-step storage operations, but Borromean
transactions should not expose rollback as a collection or application feature. Foreground
execution applies transaction effects as it proceeds; storage open/recovery is responsible for
recovering any transaction that reached durable media without `transaction_finished` before the
recovered runtime state is exposed.

Current decisions:

- Only one transaction may be open at a time; nested and concurrent transactions are forbidden by
  construction.
- Transaction ids are not needed because the current open transaction is implicit and scoped to the
  collection id carried by `begin_transaction(collection_id)`.
- Region allocation and free commands are collection-scoped WAL records: allocation brings a region
  into a collection, and free removes a region from a collection. They keep the collection id even
  when it could be inferred from surrounding records.
- Transactions are not a user-visible rollback feature. They are only a way to make internal
  multi-step storage and collection operations recoverable.
- `commit_transaction` is the middle marker, not the final marker. Before commit, recovery abandons
  the collection-state update. After commit, recovery preserves the collection-state update and
  finishes cleanup.
- Replay does not jump back when `commit_transaction` is found. It scans until
  `transaction_finished`, `rollback_transaction`, or WAL end, then uses the presence of
  `commit_transaction` to choose data recovery or cleanup recovery if the transaction did not
  finish.
- Storage open may rescan the transaction span starting at the recorded `begin_transaction`
  position, but it should not need an extra full-WAL scan.
- Recovery of an incomplete transaction happens during storage open before the recovered runtime
  state is exposed, so it should not require repairing externally visible in-memory state.
- Collections own the durable information needed to recover or finish their transaction-specific
  cleanup. The transaction layer supplies ordering and phase markers.
- Terminal transaction records rely on the existing min-free-region/free-space invariant to remain
  writable during recovery.

Expected simplifications:

- Region staging can become ordinary transaction-tagged WAL records instead of a separate recovery
  protocol.
- Stable-head replacement and old-region frees can be one durable transaction with two recovery
  phases: preserve the old state before commit, and preserve the new state while finishing frees
  after commit.
- Reclaim should no longer need a fixed pending-reclaim count for the number of old regions
  collected by one operation.
