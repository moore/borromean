# Journal

This journal captures design ideas, motivation, and decision history before they
are ready to become normative specification text.

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
free-list chain. The transaction is complete only after `transaction_finished`
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
  and allocator/free-list cleanup completed, so recovery can replay the interval normally.
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
   region mutates durable allocator state by adding the region to the free-list chain, so cleanup is
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
    and free-list mutations are replayed or completed until allocator state is consistent with the
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
