# Chapter 2: Storage Context And State Machines

This chapter defines the public storage context, the stable runtime
state, the active operation mode, and the named operation and durable
edge vocabulary used by later chapters.

## How To Read The State Machine

The storage model has three layers:

- **Stable runtime state**: replayed facts such as WAL head/tail, allocator
  head/tail, collection states, ready allocation state, and transaction
  recovery state.
- **Active mode**: the single in-flight operation and its local progress
  state.
- **Durable edge**: one replay-visible write/sync boundary inside a
  named operation.

Public operations enter from `Idle`, move through one active mode, and
return to `Idle` after their durable edge sequence reaches a terminal
state. Some low-level operations intentionally expose a stable
intermediate runtime state, such as a reserved `ready_region`; that
state is not an in-flight mode and is replayable after reset.

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

## Core Ring State Machine

The ring is a hierarchical state machine. The long-lived runtime state
is the replayed database state, while the active mode records the
single operation currently advancing that state. This keeps durable
state, operation progress, replay, and recovery described with the
same vocabulary.

The stable runtime state contains:

- `metadata`, including immutable geometry and WAL encoding
  parameters.
- WAL position: current `wal_head`, `wal_tail`, and next
  `wal_append_offset` in the tail.
- Allocator position: durable `last_free_list_head`, runtime
  `free_list_tail`, and optional WAL-rotation `ready_region`.
- `max_seen_sequence`, used to assign the next initialized region
  sequence.
- The replayed collection table, including each collection id,
  collection type, durable basis, dropped state, and retained
  post-basis update count or WAL record locations.
- Optional transaction recovery state: active transaction collection id,
  WAL interval start, whether `commit_transaction` has been seen, and
  whether the interval ended with `transaction_finished`,
  `rollback_transaction`, or WAL end.
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
successfully after `ReserveRegion`, leaving `ready_region` in stable
runtime state. That is not a lingering active mode; it is replayable
stable runtime state that a later rotation finish may consume from
`Idle`.

`Opening(OpenMode)` has these phases:

- `ReadMetadataAndScanRegions`: validate metadata, scan region
  headers, choose the WAL tail, and collect `max_seen_sequence`.
- `RecoverIncompleteRotation`: finish a tail rotation that had a
  durable rotation-start record or durable link but no initialized
  target WAL region.
- `DiscoverWalChain`: walk `link` records from the effective WAL head
  to the selected tail.
- `ReplayWalRecords`: scan reachable WAL records in order and apply
  each durable record through `ApplyWalRecord`.
- `BuildRuntimeState`: construct the stable runtime state from the
  replay tracker and reconstructed free-list tail.
- `ValidateLiveCollections`: let supported collection implementations
  validate retained live bases and payloads needed for reads and
  reachability decisions.
- `RecoverTransactions`: finish or roll back incomplete collection
  transactions before exposing recovered runtime state.
- `Finish`: expose a `Storage` context whose mode is `Idle`.

`ApplyWalRecord` is the shared transition table for durable WAL record
effects. It is used when startup replays records, when a foreground
operation appends and syncs a record, when recovery decides whether a
record still carries live state, and when WAL-head reclaim preserves
or rewrites live records. The table describes the replay-visible
effect after a record is durable:

| WAL record | `ApplyWalRecord` effect |
| --- | --- |
| `new_collection(collection_id, collection_type)` | Create a collection entry and move its collection submachine from `NoCollection` to `EmptyClean`. |
| `update(collection_id, payload)` | Require an existing non-dropped collection, retain the update after that collection's current durable basis, and move `EmptyClean` to `EmptyDirty`, `WALSnapshotClean` to `WALSnapshotDirty`, or `RegionClean` to `RegionDirty`; an already dirty collection remains in the matching dirty state. |
| `snapshot(collection_id, collection_type, payload)` | If this is the first retained basis record for the collection, create replay state and set the collection type from this record; otherwise require `collection_type` to match the replay-tracked type. Move the collection submachine to `WALSnapshotClean` and discard older pending updates for that collection. |
| `alloc_begin(collection_id, region_index, free_list_head_after)` | Advance `last_free_list_head` to `free_list_head_after`. For `collection_id = 0`, also set `ready_region = region_index` for WAL rotation recovery; user collection allocations are transaction-owned and do not occupy `ready_region`. |
| `head(collection_id, collection_type, region_index)` for a user collection | Create or validate the collection type, move the collection submachine to `RegionClean`, and discard older pending updates for that collection. |
| `head(collection_id = 0, collection_type = wal, region_index)` | As a WAL-chain control record, update the effective WAL head in foreground operation. During startup, the last valid tail-local control record selects the effective WAL head before main replay; during the main user-collection replay pass it has no collection-basis effect. |
| `link(next_region_index, expected_sequence)` | Preserve WAL-chain reachability and consume matching `ready_region` for the linked WAL region. |
| `drop_collection(collection_id)` | Move the collection submachine to `Dropped`, clear retained pending updates and volatile frontier state, and leave the collection id reserved. |
| `free_region(collection_id, region_index)` | Add a region removed from `collection_id` to the durable free-list chain and refresh allocator head/tail state. |
| `begin_transaction(collection_id)` | Start a collection-scoped transaction interval. |
| `commit_transaction(collection_id)` | Mark the transaction update phase committed; recovery must preserve the new collection state after this marker. |
| `transaction_finished(collection_id)` | Close a committed transaction after cleanup is complete. |
| `rollback_transaction(collection_id)` | Close a pre-commit recovery interval whose transaction effects have already been cleaned up. |
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
  id, appends and syncs `new_collection`, applies the `EmptyClean`
  transition, and returns a collection handle for the new empty basis.
- `UpdatingCollection(CollectionUpdateMode)` validates a live
  collection, encodes and appends a collection-defined `update`, applies
  that update to the volatile frontier, and leaves the collection in
  the matching dirty state.
- `AppendingWal(WalAppendMode)` validates the source state, ensures the
  tail has room or asks `RotatingWal` to make room, writes and syncs one
  WAL record, then applies `ApplyWalRecord` to the stable runtime
  state.
- `AllocatingRegion(AllocationMode)` completes safe foreground reclaim
  if needed, preserves the minimum free-region reserve, writes and
  syncs `alloc_begin(collection_id, region_index, free_list_head_after)`,
  then either leaves a WAL rotation target in `ready_region` or records a
  transaction-owned user allocation.
- `WritingCommittedRegion(CommittedRegionWriteMode)` reserves a region,
  erases and writes a committed-region header plus payload, syncs the
  region, appends and syncs the user `head` record, then applies that
  head transition.
- `RotatingWal(WalRotationMode)` writes and syncs the rotation
  `alloc_begin` in the reserved tail window, writes and syncs `link`,
  initializes and syncs the new WAL region, then makes the linked region
  the append tail.
- `ReclaimingWalHead(WalHeadReclaimMode)` plans the old and new WAL
  heads, preserves allocator state, copies or rewrites live records from
  the old head, commits the new WAL head, and frees the old head inside
  the collection-scoped transaction for the WAL collection.
- `TransactionRecovery(TransactionRecoveryMode)` scans an incomplete
  transaction interval during open, selects data recovery or cleanup
  recovery based on whether `commit_transaction` is present, and writes
  the matching terminal transaction marker.
- `SnapshottingCollection(CollectionSnapshotMode)` serializes the
  collection's current logical state into a WAL `snapshot`, appends and
  syncs that record, clears superseded post-basis updates, and returns
  the collection to a clean WAL-snapshot basis.
- `FlushingCollection(CollectionFlushMode)` writes collection-defined
  committed state into one or more allocated regions, appends and syncs
  the user `head`, clears superseded post-basis updates, and uses a
  collection transaction when old basis regions must be freed.
- `CompactingCollection(CollectionCompactionMode)` reads the current
  committed layout, writes replacement committed layout, commits a new
  user `head`, and frees committed regions made stale by the compaction
  during transaction cleanup. The logical collection state remains clean.
- `DroppingCollection(CollectionDropMode)` appends and syncs
  `drop_collection`, clears volatile collection state, and frees old
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
| `FormatStorage` | `Formatting(FormatMode)` | unformatted or caller-erased media | `FormatMetadata`, `FormatInitialWalRegion`, `FormatInitialFreeList` | initialized storage in `Idle` |
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
| `ReserveRegionForUse` | `AllocatingRegion(AllocationMode)` | `Idle` with free-list capacity above reserve | `ReserveRegion` | allocator advances; WAL rotation reserves `ready_region`, user allocation remains transaction-owned |
| `RotateWalTail` | `RotatingWal(WalRotationMode)` | current WAL tail in rotation window | `StartWalRotation`, `RotateWalLink`, `InitializeRotatedWalRegion` | WAL tail moves to linked region |
| `FreeRegion` | transaction cleanup mode | region detached from its owning collection | `LinkFreeTail`, `CommitFreeRegion` | region enters durable free-list chain |
| `ReclaimWalHead` | `ReclaimingWalHead(WalHeadReclaimMode)` | reclaimable WAL head | transaction edges, preservation edges, `CommitWalHeadControl`, cleanup frees | WAL head moves and old head enters free-list chain |
| `CommitWalRecovery` | `AppendingWal(WalAppendMode)` | pending WAL recovery boundary | `CommitWalRecoveryBoundary` | boundary cleared so normal append may resume |
| `AppendRawWalRecord` | `AppendingWal(WalAppendMode)` | valid record-specific source state | one record-specific durable edge from the table below | `ApplyWalRecord` effect for that record |

Named durable edges for replay-visible durable writes:

| Edge | Durable action | Detailed source |
| --- | --- | --- |
| `FormatMetadata` | Write and sync `StorageMetadata`. | `Format Storage`, `Storage Metadata` |
| `FormatInitialWalRegion` | Initialize and sync region `0` as the first WAL region. | `Format Storage`, `Header`, `WAL Region Prologue` |
| `FormatInitialFreeList` | Initialize and sync the formatted free-list chain. | `Format Storage`, `Free-Pointer Footer` |
| `CreateCollection` | Write and sync `new_collection`. | `ApplyWalRecord`, `Collection Head Submachine` |
| `AppendUpdate` | Write and sync `update`. | `ApplyWalRecord`, `RING-ORDER-001`, `RING-CRASH-011` |
| `CommitSnapshotHead` | Write and sync `snapshot`. | `ApplyWalRecord`, `RING-ORDER-002`, `RING-CRASH-001` through `RING-CRASH-002` |
| `ReserveRegion` | Write and sync `alloc_begin`. | `ApplyWalRecord`, `RING-ALLOC-*`, `RING-CRASH-005` through `RING-CRASH-008` |
| `StartWalRotation` | Write and sync the rotation-window `alloc_begin` that only a matching `link` may follow in that tail. | `ApplyWalRecord`, `RING-WAL-ENC-014`, `RING-CRASH-008` |
| `WriteCommittedRegion` | Erase, write, and sync a committed-region header and payload. | `RING-ORDER-004`, `Header`, collection-format requirements |
| `CommitRegionHead` | Write and sync a user-collection `head`. | `ApplyWalRecord`, `RING-ORDER-004`, `RING-CRASH-006` through `RING-CRASH-007` |
| `RotateWalLink` | Write and sync WAL `link`. | `ApplyWalRecord`, `RING-ORDER-005`, `RING-CRASH-008` through `RING-CRASH-010` |
| `InitializeRotatedWalRegion` | Erase, initialize, and sync the linked WAL region. | `RING-ORDER-005`, `WAL Region Prologue`, startup rotation recovery |
| `CommitWalHeadControl` | Write and sync `head(collection_id = 0, collection_type = wal, ...)`. | `ApplyWalRecord`, WAL reclaim postconditions, startup WAL-head discovery |
| `CopyRetainedWalRecord` | Copy and sync a retained WAL record into the new WAL head during WAL-head reclaim. | `ApplyWalRecord`, WAL reclaim liveness rules |
| `RewriteRetainedEmptyBasis` | Write and sync a retained `snapshot` basis that represents an empty live collection whose original `new_collection` record would otherwise be reclaimed. | `ApplyWalRecord`, WAL reclaim liveness rules |
| `BeginTransaction` | Write and sync `begin_transaction(collection_id)`. | `ApplyWalRecord`, transaction recovery |
| `CommitTransaction` | Write and sync `commit_transaction(collection_id)`. | `ApplyWalRecord`, transaction recovery |
| `FinishTransaction` | Write and sync `transaction_finished(collection_id)`. | `ApplyWalRecord`, transaction recovery |
| `RollbackTransaction` | Write and sync `rollback_transaction(collection_id)`. | `ApplyWalRecord`, transaction recovery |
| `LinkFreeTail` | Write and sync the previous free-list tail footer. | `Free Region`, `Free-Pointer Footer` |
| `CommitFreeRegion` | Write and sync `free_region(collection_id, region_index)`. | `ApplyWalRecord`, `Free Region` |
| `CommitWalRecoveryBoundary` | Write and sync `wal_recovery`. | `ApplyWalRecord`, `RING-ORDER-007`, `RING-CRASH-011` |
| `CommitDropCollection` | Write and sync `drop_collection`. | `ApplyWalRecord`, `Collection Head Submachine` |

Formatting is modeled as `Formatting(FormatMode)` until the freshly
initialized metadata, initial WAL region, and initial free-list chain
are durable. Opening is modeled as `Opening(OpenMode)` until replay and
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
5. `RING-MACHINE-005` Normal foreground operation, startup replay, and
crash recovery MUST use the same `ApplyWalRecord` semantics for every
retained durable WAL record.
6. `RING-MACHINE-006` Startup and recovery modes MUST compose the same
collection, allocator, WAL-chain, and transaction submachine
transitions used by normal operation rather than defining separate
incompatible transition rules.
7. `RING-MACHINE-007` State-machine transition rules MUST use named
operation identifiers, and each named operation MUST define its source
state, active mode, durable edge sequence, and target state or runtime
effect.
