# Chapter 2: Storage Context And State Machines

This chapter defines the public storage context, the stable runtime
state, the active operation mode, and the named operation and durable
edge vocabulary used by later chapters.

## How To Read The State Machine

The storage model has three layers:

- **Stable runtime state**: replayed facts such as WAL head/tail,
  free-space collection cursors, collection states, storage-core
  allocation reservations, and transaction recovery state.
- **Active mode**: the single in-flight operation and its local progress
  state.
- **Durable edge**: one replay-visible write/sync boundary inside a
  named operation.

Public operations enter from `Idle`, move through one active mode, and
return to `Idle` after their durable edge sequence reaches a terminal
state. Some low-level storage-core operations intentionally expose a
stable intermediate runtime state, such as a reserved private
allocation for log rotation. That state is not an in-flight mode and is
replayable after reset.

The named operation table is the index for later chapters. Collection,
WAL, allocator, reclaim, format, and open chapters should use those
operation names rather than ad hoc prose labels.

## Storage API Model

The public storage API should make the durable ownership model match
the logical ownership model. `Storage` is the database context: it owns
the replayed runtime state, configuration, dirty-frontier accounting,
and bounded reusable memory needed to perform storage and collection
operations. It also owns exclusive access to the backing object for the
life of the opened database. That backing object is the abstraction over
the caller's device, transport, emulator, or synchronized sharing
adapter.

At the design level, formatting and opening create a storage context
by binding caller-provided backing media and caller-owned storage memory
into `Storage`:

```rust
let mut memory = StorageMemory::new();
Storage::format(backing, config, &mut memory) -> Result<Storage, StorageError>;
Storage::open(backing, &mut memory) -> Result<Storage, StorageError>;
```

The `backing` and `memory` arguments are borrowed mutably by `Storage`
for its lifetime. Normal storage operations then use both through
`Storage`:

```rust
storage.operation(...);
```

Collection operations use the same storage context:

```rust
collection.operation(&mut storage, ...);
```

Public storage, collection, and transaction operations are
non-reentrant. At most one call may be active for a given `Storage`,
collection handle, or transaction object. Rust APIs normally enforce
this by requiring `&mut Storage` and `&mut` access to collection or
transaction state; closure-based transaction APIs provide the same
scoped exclusive access for transaction-local operations.

Public collection reads are read-committed with respect to
transaction-log-backed transactions. Records appended to a transaction
log may update that transaction's private frontier buffers and allocator
recovery state, but they do not become visible to public collection
operations until the main WAL
`commit_transaction(transaction_log_id, range)` record is durable and
the private frontiers are atomically installed.

Normal collection APIs should not require callers to provide separate
frontier buffers, payload serialization buffers, or a `StorageWorkspace`
for each operation. Those bounded buffers may still exist as internal
implementation types, storage fields, or constructor-provided storage,
but once a `Storage` value exists they are part of the storage context
rather than a repeated collection-operation argument. This keeps open
collection handles small while keeping operation scratch bounded and
explicit.

If a platform must share the physical device with other code, that
sharing policy belongs inside the backing implementation. For example,
the backing object may contain a mutex, critical-section guard, or
platform-specific synchronization primitive around a lower-level driver.
Borromean still receives one backing abstraction and one mutable
`Storage`, so core storage code does not choose a mutex type, executor,
interrupt policy, or sharing discipline.

## Storage API Requirements

1. `RING-API-001` `Storage` MUST be the public database context that
owns logical runtime state, replay state, configuration,
dirty-frontier tracking, and bounded reusable scratch memory needed by
normal storage and collection operations.
2. `RING-API-002` `Storage` MUST own exclusive access to the backing
object for the lifetime of an opened database, either by owning the
backing value or by holding a mutable reference to it.
3. `RING-API-003` Public operations that may touch backing media MUST
use the backing object through `Storage` rather than requiring a
separate backing argument on each operation.
4. `RING-API-004` Public normal collection operations MUST NOT require
callers to provide collection frontier buffers, payload serialization
buffers, or a `StorageWorkspace`; that bounded memory MUST be supplied
through caller-owned memory borrowed by `Storage` or the collection
handle.
5. `RING-API-005` Any shared-device synchronization required by a
platform MUST be encapsulated by the backing implementation rather than
by Borromean core requiring a specific mutex, executor, interrupt
policy, or sharing primitive.
6. `RING-API-006` Public storage, collection, and transaction
operations MUST be non-reentrant for the same object and MUST require
exclusive mutable access or an equivalent API-level serialization
discipline.
7. `RING-API-007` Public collection reads MUST observe only committed
main-WAL-visible collection state. Transaction-log records MUST NOT
become public collection state before the corresponding main WAL
`commit_transaction(transaction_log_id, range)` is durable.

## Core Ring State Machine

The ring is a hierarchical state machine. The long-lived runtime state
is the replayed database state, while the active mode records the
single operation currently advancing that state. This keeps durable
state, operation progress, replay, and recovery described with the
same vocabulary.

The stable runtime state contains:

- `metadata`, including immutable geometry and WAL encoding
  parameters.
- Main WAL position: current `wal_head`, `wal_tail`, and next
  `wal_append_offset` in the tail.
- Transaction-log positions: for each configured transaction log, its
  head, tail, append offset, live-prefix boundary, and whether an
  active or recovery transaction descriptor currently references it.
- Free-space collection state: retained free-space durable basis,
  post-basis allocator frontier, durable `allocation_head`,
  `ready_boundary`, and `append_tail` cursors, plus any storage-core
  allocation reservation that has been popped from the ready range but
  not yet consumed by a durable private-log `link`.
- `max_seen_sequence`, used to assign the next initialized region
  sequence.
- The replayed collection table, including each collection id,
  collection type, durable basis, dropped state, and retained
  post-basis update count or WAL record locations, plus a committed
  state generation counter advanced by visible main-WAL collection
  decisions. A main-WAL
  `commit_transaction(transaction_log_id, range)` advances the
  generation for every enrolled collection when it imports that range;
  private transaction-log records do not advance the committed
  generation before commit.
- Optional transaction descriptors: transaction log id, transaction-log
  range start, enrolled collections with their observed committed
  generation values and private frontier buffers, transaction-owned
  allocations, transaction-private free intents, commit/rollback phase,
  cleanup cursor, cleanup start tail, and any pending cleanup/recovery
  range. The storage-level cleanup owner, when present, names the only
  transaction allowed to append cleanup frees or finish cleanup in the
  main WAL.
- The `pending_wal_recovery_boundary` flag used when valid tail
  records were found after a torn or corrupt tail span.

`Storage` also owns volatile runtime state that is not itself a durable
replay result. This includes dirty-frontier accounting and any
collection-defined in-memory frontier materialized from retained
post-basis updates. Startup can reconstruct equivalent volatile
collection state from stable runtime state and retained WAL payloads, but
dirty-frontier bookkeeping is not a separate durable fact.

Outside storage open, retained WAL `update` records are not an unloaded
collection storage form. A live collection is operationally either dirty
and resident in RAM, clean with a WAL snapshot basis, or clean with a
committed-region basis. If a dirty resident collection must leave RAM,
the implementation first checkpoints it as a WAL snapshot or flushes it
as committed region state. Borromean has no separate clean-shutdown
path; every open is a recovery pass. Retained WAL updates become the
effective collection state only during storage open, where replay
reconstructs the dirty resident frontier that existed before reset.

Operation-specific progress is distinct from both stable replayed state
and volatile collection state. It belongs to the active mode: planned
regions, encoded record lengths, current scan offsets, saved reclaim
plans, pending copy actions, or any other state that exists only while
one operation is in flight.

At the design level the storage mode is:

```rust
enum StorageMode {
  Idle,
  Formatting(FormatMode),
  Opening(OpenMode),
  ReadingStorage(ReadMode),
  LoadingCollection(CollectionLoadMode),
  CreatingCollection(CollectionCreateMode),
  UpdatingCollection(CollectionUpdateMode),
  AppendingWal(WalAppendMode),
  AllocatingRegion(AllocationMode),
  WritingCommittedRegion(CommittedRegionWriteMode),
  RotatingWal(WalRotationMode),
  ReclaimingWalHead(WalHeadReclaimMode),
  Transacting(TransactionMode),
  TransactionRecovery(TransactionRecoveryMode),
  SnapshottingCollection(CollectionSnapshotMode),
  FlushingCollection(CollectionFlushMode),
  CompactingCollection(CollectionCompactionMode),
  DroppingCollection(CollectionDropMode),
}
```

`Idle` is the only steady-state mode. Public operations enter from
`Idle`, transition through one operation mode, and return to `Idle`
after reaching a terminal result. Mutating operations may change the
stable runtime state through named durable transition edges. Read-only
operations may scan WAL or committed regions and materialize volatile
views, but they do not write durable transition edges. While a mode is
active, its sub-state owns the operation's interstitial data and the
stable runtime state remains the replayable state that a reset would
recover from durable media.

Some low-level APIs expose individual transition edges rather than a
whole collection operation. For example, a WAL rotation start may return
successfully after `AllocateRegionForStorageCore`, leaving a
storage-core allocation reservation in stable runtime state. That is
not a lingering active mode; it is replayable stable runtime state that
a later rotation finish may consume from `Idle`.

`Opening(OpenMode)` has these phases:

- `ReadMetadataAndScanRegions`: validate metadata, scan region
  headers, choose the WAL tail, and collect `max_seen_sequence`.
- `RecoverIncompleteRotation`: finish a tail rotation that had a
  durable rotation-start record or durable link but no initialized
  target WAL region.
- `ReplayWalRecords`: scan reachable WAL records in order, following
  `link` records from the effective main WAL head to the selected tail,
  scan referenced transaction-log ranges as they are imported by main
  WAL transaction records, and apply each durable record through
  `ApplyWalRecord`. Startup fails if the selected tail is not reachable
  through a valid link chain.
- `BuildRuntimeState`: construct the stable runtime state from the
  replay tracker and recovered free-space collection cursors.
- `ValidateLiveCollections`: let supported collection implementations
  validate retained live bases and payloads needed for reads and
  reachability decisions.
- `RecoverTransactions`: finish or roll back incomplete collection
  transactions before exposing recovered runtime state.
- `Finish`: expose a `Storage` context whose mode is `Idle`.

`ApplyWalRecord` is the shared implementation boundary for durable WAL
record effects. It takes the current stable runtime or replay state, a
validated decoded WAL record, and the replay context needed to
interpret that record, such as main WAL order, transaction-log range,
inline-transaction body, recovery path, or WAL-head reclaim
preservation. It is used when startup replays records, when a
foreground operation appends and syncs a record, when recovery writes a
missing record, and when WAL-head reclaim preserves or rewrites live
records. Foreground code may pass the already-decoded record object
after sync; it need not read the record back from media. The table
describes the replay-visible effect after a record is durable:

| WAL record | `ApplyWalRecord` effect |
| --- | --- |
| `new_collection(collection_id, collection_type)` | Create a collection entry and move its collection submachine from `NoCollection` to `EmptyClean`. |
| `update(collection_id, payload)` | Require an existing non-dropped collection, retain the update after that collection's current durable basis, and move `EmptyClean` to `EmptyDirty`, `WALSnapshotClean` to `WALSnapshotDirty`, or `RegionClean` to `RegionDirty`; an already dirty collection remains in the matching dirty state. |
| `snapshot(collection_id, collection_type, payload)` | For a user collection, if this is the first retained basis record, create replay state and set the collection type from this record; otherwise require `collection_type` to match the replay-tracked type. Move the collection submachine to `WALSnapshotClean` and discard older pending updates for that collection. For `collection_id = 0, collection_type = free_space_v2`, replace the free-space durable basis with the self-contained snapshot and discard older allocator update records. |
| `free_region(region_index, append_tail_after)` | Append `region_index` as a dirty entry at the current `append_tail` and advance `append_tail` to `append_tail_after`. During transaction cleanup, the append must be the cleanup owner's next ordered cleanup slot. |
| `erase_free_region_span(count, ready_boundary_after)` | Publish that the next `count` dirty entries starting at the current `ready_boundary` have been erased, then advance `ready_boundary` to `ready_boundary_after`. This transition is blocked while a transaction owns main-WAL cleanup. |
| `allocate_region(region_index, allocation_head_after)` | Pop the current ready entry, require it to name `region_index`, advance `allocation_head` to `allocation_head_after`, and, inside a full transaction, record a transaction-owned allocation. A transaction-owned allocation is not collection-live before commit; on rollback the retained allocation list is returned by ordered cleanup. In a privileged storage-core operation, the pop becomes a replayable private allocation reservation immediately. |
| `head(collection_id, collection_type, region_index)` for a user collection | Create or validate the collection type, move the collection submachine to `RegionClean`, and discard older pending updates for that collection. |
| `head(collection_id = 0, collection_type = main_wal_v2, region_index)` | As a WAL-chain control record, update the effective WAL head in foreground operation. During startup, the last valid tail-local control record selects the effective WAL head before main replay; during the main replay pass it has no collection-basis effect. |
| `head(collection_id = 0, collection_type = free_space_v2, region_index)` | Replace the free-space durable basis with the materialized `free_space_v2` metadata chain rooted at `region_index` and discard older allocator update records. |
| `link(next_region_index, expected_sequence)` | Preserve private-log reachability and consume a matching storage-core allocation reservation for the linked log region. |
| `drop_collection(collection_id)` | Move the collection submachine to `Dropped`, clear retained pending updates and volatile frontier state, and leave the collection id reserved. |
| `begin_transaction(transaction_log_id, start)` | Open a transaction descriptor for one transaction log starting at `start`. |
| `begin_inline_transaction(record_count, encoded_len)` | Main-WAL-only bounded transaction for short storage-internal atomic groups; records inside it are ignored until a matching inline commit is durable. |
| `add_transaction_collection(collection_id, observed_generation)` | In a transaction log, enroll `collection_id`, copy its frontier into a private transaction buffer, and remember the observed committed generation for conflict checks. |
| `free_intent(collection_id, region_index)` | In a transaction log, record a transaction-private intent to free a region that is still live in the enrolled collection. It has no allocator effect before commit. |
| `commit_transaction(transaction_log_id, range)` | In the main WAL, freeze and import `range` from `transaction_log_id` at this commit position. The imported private frontiers become visible, transaction-owned allocations become collection-owned, free intents are detached from collection live state as pending cleanup obligations, and the transaction becomes the main-WAL cleanup owner at the current `append_tail`. |
| `commit_inline_transaction(record_count)` | Atomically apply the bounded inline range that began at the matching `begin_inline_transaction`. |
| `rollback_inline_transaction(record_count)` | Record that an uncommitted inline range was cleaned up and remains non-visible. |
| `transaction_finished(transaction_log_id, range)` | Close a committed or rolled-back transaction after its ordered cleanup is complete, release the main-WAL cleanup owner, and release this transaction-log range reference for garbage collection. |
| `rollback_transaction(transaction_log_id, range)` | In the main WAL, mark a transaction-log range non-visible. The transaction becomes the main-WAL cleanup owner at the current `append_tail`; cleanup of the range's transaction-owned allocations remains pending until `transaction_finished`. |
| `wal_recovery()` | Clear the WAL recovery boundary opened by a prior torn or corrupt tail span. |

The main operation modes are transition sequences over the same table:

- `ReadingStorage(ReadMode)` visits WAL records, collection bases, or
  committed regions without changing durable state. It may use bounded
  memory for decoding or region reads and may materialize volatile read
  views, then returns to `Idle`.
- `LoadingCollection(CollectionLoadMode)` validates an existing live
  collection and constructs the collection-specific handle or resident
  frontier from its current durable basis. It may materialize volatile
  collection state, but it does not append records or change durable
  state.
- `CreatingCollection(CollectionCreateMode)` reserves a new collection
  id, appends and syncs `new_collection`, applies that decoded record
  through `ApplyWalRecord`, and returns a collection handle for the new
  empty basis.
- `UpdatingCollection(CollectionUpdateMode)` validates a live
  collection, encodes, appends, and syncs a collection-defined
  `update`, then applies that decoded record through `ApplyWalRecord`.
  Any resident volatile frontier update uses the same
  collection-defined update logic that replay uses when rebuilding that
  frontier from retained updates.
- `AppendingWal(WalAppendMode)` validates the source state, ensures the
  tail has room or asks `RotatingWal` to make room, writes and syncs one
  WAL record, then applies `ApplyWalRecord` to the stable runtime
  state.
- `AllocatingRegion(AllocationMode)` completes safe foreground reclaim
  or erase maintenance if needed, preserves the ready-region reserve,
  writes and syncs `allocate_region(region_index,
  allocation_head_after)` inside the active full transaction, bounded
  inline transaction, or privileged storage-core operation, then applies
  that decoded record through `ApplyWalRecord` to record either a
  transaction-owned user allocation or a storage-core private
  allocation reservation.
- `WritingCommittedRegion(CommittedRegionWriteMode)` reserves a region,
  erases and writes a committed-region header plus payload, syncs the
  region, appends and syncs the user `head` record, then applies that
  decoded `head` record through `ApplyWalRecord`.
- `RotatingWal(WalRotationMode)` writes and syncs a privileged
  `allocate_region` in the reserved tail window, writes and syncs
  `link`, applies each decoded durable record through `ApplyWalRecord`,
  initializes and syncs the new WAL region, then exposes the linked
  region as the append tail.
- `ReclaimingWalHead(WalHeadReclaimMode)` plans the old and new WAL
  heads, preserves free-space collection cursor state, copies or
  rewrites live records from the old head, commits the new WAL head, and
  returns the old head to the free-space collection inside a
  transaction-log-backed cleanup transaction for private log storage.
- `TransactionRecovery(TransactionRecoveryMode)` scans incomplete or
  unfinished transaction-log ranges during open, selects rollback
  cleanup or committed cleanup recovery based on retained main-WAL
  transaction records, and writes the missing rollback, cleanup, and
  finish records in replay order.
- `SnapshottingCollection(CollectionSnapshotMode)` serializes the
  collection's current logical state into a WAL `snapshot`, appends and
  syncs that record, then applies that decoded record through
  `ApplyWalRecord` to clear superseded post-basis updates and return
  the collection to a clean WAL-snapshot basis.
- `FlushingCollection(CollectionFlushMode)` writes collection-defined
  committed state into one or more allocated regions, appends and syncs
  the user `head`, then applies that decoded record through
  `ApplyWalRecord`. It uses a collection transaction when old basis
  regions must be freed.
- `CompactingCollection(CollectionCompactionMode)` reads the current
  committed layout, writes replacement committed layout, commits a new
  user `head`, applies that decoded record through `ApplyWalRecord`,
  and frees committed regions made stale by the compaction during
  transaction cleanup. The logical collection state remains clean.
- `DroppingCollection(CollectionDropMode)` appends and syncs
  `drop_collection`, applies that decoded record through
  `ApplyWalRecord`, clears volatile collection state, and frees old
  basis regions through transaction cleanup when needed.

State-machine operations are named transition labels. Diagrams and
transition rules use the operation identifier, with arguments omitted
when the surrounding source and target states make them clear. A named
operation may contain no durable writes, one durable edge, or an ordered
sequence of durable edges. When a single-record operation and its
single durable edge have the same name, the operation is the
state-machine transition and the durable edge is the write/sync boundary
inside that transition.

| Operation | Active mode | Source | Durable edge sequence | Target or effect |
| --- | --- | --- | --- | --- |
| `FormatStorage` | `Formatting(FormatMode)` | unformatted or caller-erased media | `FormatMetadata`, `FormatInitialWalRegion`, `FormatInitialFreeSpaceCollection` | initialized storage in `Idle` |
| `OpenStorage` | `Opening(OpenMode)` | formatted media | none unless recovery sub-operations are needed | recovered storage in `Idle` |
| `ReadStorage` | `ReadingStorage(ReadMode)` | `Idle` | none | no durable state change |
| `LoadCollection` | `LoadingCollection(CollectionLoadMode)` | any live collection state | none | materialized collection handle or frontier |
| `CreateCollection` | `CreatingCollection(CollectionCreateMode)` | `NoCollection` | `CreateCollection` | `EmptyClean` |
| `ApplyCollectionUpdate` | `UpdatingCollection(CollectionUpdateMode)` | any live clean or dirty collection state | `AppendUpdate` | matching dirty collection state |
| `CommitCollectionSnapshot` | `SnapshottingCollection(CollectionSnapshotMode)` | any live collection state | `CommitSnapshotHead` | `WALSnapshotClean` |
| `CommitCollectionRegion` | `FlushingCollection(CollectionFlushMode)`, `CompactingCollection(CollectionCompactionMode)`, or `WritingCommittedRegion(CommittedRegionWriteMode)` | any live collection state | optional transaction edges, optional `ReserveRegion`, `WriteCommittedRegion`, `CommitRegionHead`, optional cleanup frees | `RegionClean` |
| `DropCollection` | `DroppingCollection(CollectionDropMode)` | any live collection state | optional transaction edges, `CommitDropCollection`, optional cleanup frees | `Dropped` |
| `ReplayRetainedSnapshotBasis` | `Opening(OpenMode)` or `ReclaimingWalHead(WalHeadReclaimMode)` | `NoCollection` | none during replay; `CopyRetainedWalRecord` when preserving the record into a new WAL head | `WALSnapshotClean` |
| `ReplayRetainedRegionBasis` | `Opening(OpenMode)` or `ReclaimingWalHead(WalHeadReclaimMode)` | `NoCollection` | none during replay; `CopyRetainedWalRecord` when preserving the record into a new WAL head | `RegionClean` |
| `ReplayRetainedDropTombstone` | `Opening(OpenMode)` or `ReclaimingWalHead(WalHeadReclaimMode)` | `NoCollection` | none during replay; `CopyRetainedWalRecord` when preserving the record into a new WAL head | `Dropped` |
| `AllocateRegionForUse` | `AllocatingRegion(AllocationMode)` | active full transaction, inline transaction, or privileged storage-core operation with ready range above reserve | `AllocateRegion` | free-space `allocation_head` advances; user allocation remains transaction-owned, storage-core allocation becomes a private reservation |
| `RotateWalTail` | `RotatingWal(WalRotationMode)` | current WAL tail in rotation window | `StartWalRotation`, `RotateWalLink`, `InitializeRotatedWalRegion` | WAL tail moves to linked region |
| `BeginTransaction` | `Transacting(TransactionMode)` | `Idle` with an available transaction log | `BeginTransaction` | transaction descriptor opens for one transaction log |
| `BeginInlineTransaction` | `Transacting(TransactionMode)` | no active full transaction and enough reserved main-WAL tail space | `BeginInlineTransaction` | bounded inline transaction opens in the main WAL |
| `AddTransactionCollection` | `Transacting(TransactionMode)` | open transaction descriptor and live collection | `AddTransactionCollection` | collection enrolled with private frontier buffer and observed generation |
| `StageFreeIntent` | `Transacting(TransactionMode)` | open transaction descriptor and enrolled live collection region | `StageFreeIntent` | free intent is retained in the transaction and has no allocator effect |
| `CommitTransaction` | `Transacting(TransactionMode)` | open transaction descriptor with no generation conflicts | `CommitTransaction` | referenced transaction-log range becomes visible atomically |
| `CommitInlineTransaction` | `Transacting(TransactionMode)` | open inline transaction whose bounded range is complete | `CommitInlineTransaction` | bounded inline range becomes visible atomically |
| `RollbackInlineTransaction` | `Transacting(TransactionMode)` or `TransactionRecovery(TransactionRecoveryMode)` | open or recovering uncommitted inline transaction | `RollbackInlineTransaction` | bounded inline range remains non-visible after cleanup |
| `RollbackTransaction` | `Transacting(TransactionMode)` or `TransactionRecovery(TransactionRecoveryMode)` | open or recovering uncommitted transaction range | `RollbackTransaction` | transaction-log range remains non-visible and cleanup of its transaction-owned allocations becomes owned by the transaction |
| `FreeRegion` | transaction cleanup mode | cleanup owner names the transaction and the next cleanup obligation is detached from live references | `AppendFreeRegion` | region enters the dirty range of the free-space collection at the ordered cleanup slot |
| `EraseFreeRegionSpan` | `AllocatingRegion(AllocationMode)` or storage maintenance mode | dirty range is non-empty, erase work is allowed, and no transaction owns cleanup | `EraseFreeRegionSpan` | dirty entries become ready entries |
| `ReclaimWalHead` | `ReclaimingWalHead(WalHeadReclaimMode)` | reclaimable WAL head | transaction edges, preservation edges, `CommitWalHeadControl`, cleanup frees | WAL head moves and old head enters the free-space collection |
| `CommitWalRecovery` | `AppendingWal(WalAppendMode)` | pending WAL recovery boundary | `CommitWalRecoveryBoundary` | boundary cleared so normal append may resume |
| `AppendRawWalRecord` | `AppendingWal(WalAppendMode)` | valid record-specific source state | one record-specific durable edge from the table below | `ApplyWalRecord` effect for that record |

Named durable edges for replay-visible durable writes:

| Edge | Durable action | Detailed source |
| --- | --- | --- |
| `FormatMetadata` | Write and sync `StorageMetadata`. | `Format Storage`, `Storage Metadata` |
| `FormatInitialWalRegion` | Initialize and sync region `0` as the first WAL region. | `Format Storage`, `Header`, `WAL Region Prologue` |
| `FormatInitialFreeSpaceCollection` | Initialize and sync the formatted free-space collection metadata chain. | `Format Storage`, `Free-Space Collection Regions` |
| `CreateCollection` | Write and sync `new_collection`. | `ApplyWalRecord`, `Collection Head Submachine` |
| `AppendUpdate` | Write and sync `update`. | `ApplyWalRecord`, `RING-ORDER-001`, `RING-CRASH-016` |
| `CommitSnapshotHead` | Write and sync `snapshot`. | `ApplyWalRecord`, `RING-ORDER-002`, `RING-CRASH-001` through `RING-CRASH-002` |
| `AllocateRegion` | Write and sync `allocate_region(region_index, allocation_head_after)` in the active transaction, inline transaction, or privileged storage-core operation. | `ApplyWalRecord`, `RING-ALLOC-*`, `RING-CRASH-005` through `RING-CRASH-010` |
| `AppendFreeRegion` | Write and sync `free_region(region_index, append_tail_after)`. | `ApplyWalRecord`, `Free Region` |
| `EraseFreeRegionSpan` | Erase one or more dirty free-space entries and then write and sync `erase_free_region_span(count, ready_boundary_after)`. | `ApplyWalRecord`, `Free Region`, `RING-CRASH-014` through `RING-CRASH-015` |
| `StartWalRotation` | Write and sync the rotation-window `allocate_region` that only a matching `link` may follow in that tail. | `ApplyWalRecord`, `RING-WAL-ENC-014`, `RING-CRASH-010` |
| `WriteCommittedRegion` | Erase, write, and sync a committed-region header and payload. | `RING-ORDER-004`, `Header`, collection-format requirements |
| `CommitRegionHead` | Write and sync a user-collection `head`. | `ApplyWalRecord`, `RING-ORDER-004`, `RING-CRASH-007` through `RING-CRASH-009` |
| `RotateWalLink` | Write and sync WAL `link`. | `ApplyWalRecord`, `RING-ORDER-005`, `RING-CRASH-010` through `RING-CRASH-012` |
| `InitializeRotatedWalRegion` | Erase, initialize, and sync the linked WAL region. | `RING-ORDER-005`, `WAL Region Prologue`, startup rotation recovery |
| `CommitWalHeadControl` | Write and sync `head(collection_id = 0, collection_type = main_wal_v2, ...)`. | `ApplyWalRecord`, WAL reclaim postconditions, startup WAL-head discovery |
| `CopyRetainedWalRecord` | Copy and sync a retained WAL record into the new WAL head during WAL-head reclaim. | `ApplyWalRecord`, WAL reclaim liveness rules |
| `RewriteRetainedEmptyBasis` | Write and sync a retained `snapshot` basis that represents an empty live collection whose original `new_collection` record would otherwise be reclaimed. | `ApplyWalRecord`, WAL reclaim liveness rules |
| `BeginTransaction` | Write and sync main-WAL `begin_transaction(transaction_log_id, start)`. | `ApplyWalRecord`, transaction recovery |
| `BeginInlineTransaction` | Write and sync main-WAL `begin_inline_transaction(record_count, encoded_len)`. | `ApplyWalRecord`, transaction recovery |
| `AddTransactionCollection` | Write and sync transaction-log `add_transaction_collection(collection_id, observed_generation)`. | `ApplyWalRecord`, transaction private frontier state |
| `StageFreeIntent` | Write and sync transaction-log `free_intent(collection_id, region_index)`. | `ApplyWalRecord`, transaction private free-intent state |
| `CommitTransaction` | Write and sync main-WAL `commit_transaction(transaction_log_id, range)`. | `ApplyWalRecord`, transaction recovery |
| `CommitInlineTransaction` | Write and sync main-WAL `commit_inline_transaction(record_count)`. | `ApplyWalRecord`, transaction recovery |
| `FinishTransaction` | Write and sync main-WAL `transaction_finished(transaction_log_id, range)`. | `ApplyWalRecord`, transaction recovery |
| `RollbackTransaction` | Write and sync main-WAL `rollback_transaction(transaction_log_id, range)`. | `ApplyWalRecord`, transaction recovery |
| `RollbackInlineTransaction` | Write and sync main-WAL `rollback_inline_transaction(record_count)` when recovery needs a durable terminal marker for an uncommitted inline range. | `ApplyWalRecord`, transaction recovery |
| `CommitWalRecoveryBoundary` | Write and sync `wal_recovery`. | `ApplyWalRecord`, `RING-ORDER-010`, `RING-CRASH-016` |
| `CommitDropCollection` | Write and sync `drop_collection`. | `ApplyWalRecord`, `Collection Head Submachine` |

Formatting is modeled as `Formatting(FormatMode)` until the freshly
initialized metadata, initial WAL region, and initial free-space
collection metadata are durable. Opening is modeled as
`Opening(OpenMode)` until replay and
post-replay recovery produce stable runtime state. After format or
open succeeds, the storage context is in `Idle`.

### Ring State Machine Requirements

1. `RING-MACHINE-001` Storage runtime MUST expose a single active
storage mode so that at most one read, collection, WAL, allocation,
region-write, rotation, reclaim, formatting, or opening operation is
active for a storage context.
2. `RING-MACHINE-002` Stable replayed runtime state MUST be kept
separate from operation-specific progress state owned by the active
mode.
3. `RING-MACHINE-003` Public steady-state operations MUST validate
that the storage context is in a valid source mode, normally `Idle`,
before beginning their transition sequence.
4. `RING-MACHINE-004` Every durable write that changes replay-visible
state MUST be represented as a named transition edge with defined
preconditions, durable effect, runtime effect, replay effect, and
crash-cut result.
5. `RING-MACHINE-005` Normal foreground operation, startup replay,
crash recovery, and WAL-head reclaim MUST use the same
`ApplyWalRecord` implementation for every replay-visible WAL record.
6. `RING-MACHINE-006` Stable runtime state for replay-visible
collection, allocator, WAL-chain, and transaction state MUST NOT
advance until the corresponding WAL record is durable. After that
durability boundary, stable runtime state MUST be updated by applying
the validated decoded record through `ApplyWalRecord`.
7. `RING-MACHINE-007` Foreground append paths MAY perform append-time
validation before writing and MAY pass the already-decoded record to
`ApplyWalRecord` after sync. They MUST NOT maintain an alternate
post-sync state mutation path that can diverge from startup replay.
8. `RING-MACHINE-008` Active operation progress MAY hold planned,
scratch, or private interstitial state before durability. Physical
region writes, erases, or initialization MAY happen before the WAL
record that publishes them, but stable replay-visible state MUST remain
unchanged until the publish record is durable and applied through
`ApplyWalRecord`.
9. `RING-MACHINE-009` Startup and recovery modes MUST compose the same
collection, allocator, WAL-chain, and transaction submachine
transitions used by normal operation rather than defining separate
incompatible transition rules.
10. `RING-MACHINE-010` State-machine transition rules MUST use named
operation identifiers, and each named operation MUST define its source
state, active mode, durable edge sequence, and target state or runtime
effect.
