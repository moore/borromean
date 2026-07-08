# Temporary Note: Transaction Recovery And Log-Based Free Space

This note captures the current transaction/free-space recovery model
before it is folded back into the numbered ring specification chapters.

## Modeling Rule

The Quint model treats every action as one durable WAL command plus the
semantic replay effect of that command.

Online execution and crash replay must use the same transition:

```text
online:
    construct WAL command -> make command durable -> apply command

startup replay:
    read durable WAL command -> apply command
```

Under this rule, a crash cut is any prefix of the action trace. The
model does not need a stored WAL-command list, replay cursors, an
`Opening` mode, or an explicit `Crash` action. If every allowed action
preserves the invariants, then every recovered crash prefix preserves
the invariants.

This is an abstraction boundary. It does not prove byte-level WAL
scanning, record decoding, WAL-head selection, checksum validation, or
torn-record recovery. It proves the semantic transaction/allocator
protocol assuming replay and foreground apply share one command
implementation.

The model lives at:

```text
models/transaction_free_recovery.qnt
```

## Replayed State

The model state is the replayed semantic state after the durable WAL
prefix:

- `storage: StorageState`: replayed implementation state.

`StorageState` carries:

- `freeQueue`: the allocator queue.
- `allocationHead`: first unconsumed ready entry.
- `readyBoundary`: first dirty entry.
- `appendTail`: one past the last unconsumed free entry.
- `walCleanupOwner`: the transaction that owns the serialized main-WAL
  cleanup section, or `NoTransaction`.
- `collections: List[CollectionState]`: committed collection state.
- `transactions: List[TxState]`: two fixed transaction slots indexed
  by `0` and `1`.

The free queue is partitioned as:

```text
[0, allocationHead)                  consumed historical prefix
[allocationHead, readyBoundary)      allocatable ready range
[readyBoundary, appendTail)          dirty appended range
[appendTail, freeQueue.length())     unused tail in the modeled list
```

Allocator cursor safety is:

```text
0 <= allocationHead <= readyBoundary <= appendTail <= freeQueue.length
```

The model calls `freeQueue[allocationHead, appendTail)` the unconsumed
free range. It includes both ready entries and dirty cleanup appends.
Dirty entries are not allocatable until `readyBoundary` advances.

`CollectionState` carries:

- `live: Set[int]`: committed live physical regions for the collection.
- `generation: int`: the collection generation head.

The model keeps committed live membership per collection. Invariants
quantify over `storage.collections[*].live` directly, and transaction
operations select and mutate only the collection named by the
transaction slot. No region may be live in more than one collection.

## Transaction State

Each transaction slot carries:

- `phase`
- `collectionIndex`
- `observedGeneration`
- `cleanupIndex`
- `cleanupStartTail`
- `allocations: List[int]`
- `freeIntents: List[int]`
- `rollbackAllocations: List[int]`

All transaction-owned lists contain physical region indexes. There are
no logical region headers or sequence numbers in this model.

The lifecycle is:

```text
TxIdle
  -> BeginTransaction -> TxOpen

TxOpen
  -> CommitTransaction -> TxCommittedCleanup
  -> StartRollbackPreparation -> TxRollbackPreparing

TxRollbackPreparing
  -> RecordRollbackAllocation* -> RollbackTransaction -> TxRolledBackCleanup

TxCommittedCleanup
  -> FreeCommittedIntent* -> FinishCommit -> TxIdle

TxRolledBackCleanup
  -> FreeRolledBackAllocation* -> FinishRollback -> TxIdle
```

`CommitTransaction` and `RollbackTransaction` acquire
`walCleanupOwner`. `FinishCommit` and `FinishRollback` release it.
While `walCleanupOwner` names a transaction, only that transaction may
append cleanup free records or write `transaction_finished` in the main
WAL. Transaction-log-only work for already open transactions may still
proceed.

`cleanupIndex` is a phase-dependent cursor:

- in `TxRollbackPreparing`, it scans `allocations`;
- in `TxCommittedCleanup`, it scans `freeIntents`;
- in `TxRolledBackCleanup`, it scans `rollbackAllocations`;
- outside those phases, it is zero.

`cleanupStartTail` is set when commit or rollback enters cleanup. The
cleanup free slot for the current entry is:

```text
cleanupStartTail + cleanupIndex
```

## Command Semantics

`BeginTransaction(tx)` selects a collection, records that collection's
current generation in the selected slot, and enters `TxOpen`.

`AllocateInTransaction(tx)` consumes the current ready entry by
advancing `allocationHead` and records only the physical region in that
transaction's `allocations` list. Allocation has no collection effect
before commit.

`StageFreeIntent(tx)` records a transaction-private physical region
chosen from the live set for the transaction's collection, excluding
only regions already staged by that same transaction. Staging does not
append to `freeQueue`, does not advance `appendTail`, and has no
allocator effect before commit.

`CommitTransaction(tx)` requires:

```text
tx.observedGeneration == storage.collections[tx.collectionIndex].generation
```

It atomically updates:

```text
storage.collections[tx.collectionIndex].live =
    (storage.collections[tx.collectionIndex].live - tx.freeIntents)
        union tx.allocations
storage.collections[tx.collectionIndex].generation += 1
tx.phase = TxCommittedCleanup
tx.cleanupStartTail = storage.appendTail
storage.walCleanupOwner = tx
```

Commit removes `tx.freeIntents` from collection live state, but does
not append those regions to `freeQueue`. The committed free intents are
in an in-between retained cleanup state: they are no longer
collection-live and not yet allocator-free.

`StartRollbackPreparation(tx)` records the decision to roll back but
does not write the durable rollback marker. The transaction remains
recoverable as open until rollback allocation records are forced.

`RecordRollbackAllocation(tx)` copies
`tx.allocations[tx.cleanupIndex]` into `rollbackAllocations` and
advances `cleanupIndex`. These records are written before the durable
rollback marker, so recovery after the marker can clean up from durable
transaction records instead of inferring obligations from raw
allocation entries.

`RollbackTransaction(tx)` is allowed only when `cleanupIndex` has
reached the end of `allocations`, which means every raw allocation has
an explicit rollback cleanup record. It then enters
`TxRolledBackCleanup`, records `cleanupStartTail = storage.appendTail`,
and acquires `walCleanupOwner`.

`FreeCommittedIntent(tx)` processes
`tx.freeIntents[tx.cleanupIndex]`. It appends that region at:

```text
free_slot = tx.cleanupStartTail + tx.cleanupIndex
```

The action requires `storage.appendTail == free_slot`, appends exactly
one free entry, and advances `cleanupIndex`. There is no scan of the
free list and no header validation in this model.

`FreeRolledBackAllocation(tx)` is the rollback equivalent over
`tx.rollbackAllocations[tx.cleanupIndex]`. It uses the same ordered
`free_slot` rule and appends exactly one free entry.

`FinishCommit(tx)` is allowed only when `cleanupIndex` has reached the
end of `freeIntents`. `FinishRollback(tx)` is allowed only when
`cleanupIndex` has reached the end of `rollbackAllocations`. Both model
the same durable `transaction_finished(tx)` command shape, reset the
slot to `TxIdle`, and release `walCleanupOwner`.

`EraseOneDirty` advances `readyBoundary` by one. It is blocked while
`walCleanupOwner` is set, so dirty cleanup frees cannot become ready
and allocatable before the owning transaction writes
`transaction_finished`.

## Important Recovery Point

Cleanup must be idempotent without a separate cleanup-progress log.

The current model proves idempotence by assigning every cleanup free an
ordered free-log slot:

```text
free_region(region, free_slot)
```

In a real implementation, `free_slot` is a free reference such as
`(free_log_region, offset)`. In the bounded model it is an integer
queue position.

When replay finds an unfinished transaction in cleanup, the transaction
owns the serialized main-WAL cleanup section. Since no other main-WAL
operation can interleave before `transaction_finished`, replay can
resume at `cleanupStartTail + cleanupIndex`. If the expected free slot
is already present and contains the expected region, replay advances to
the next entry. If the expected slot is not yet written, replay writes
it. A mismatch is outside the normal transition system and represents
storage corruption.

The model intentionally does not use logical region sequence headers.
During transaction cleanup recovery, the ready boundary cannot advance
over the cleanup suffix, so a just-written cleanup free cannot be
reallocated before `transaction_finished`.

## Invariants

The model checks:

1. Allocator cursors remain ordered.
2. Unconsumed free entries have no duplicate physical regions.
3. No region is live in more than one modeled collection.
4. No collection live set overlaps unconsumed free-space membership.
5. The WAL cleanup owner is either empty or names the only transaction
   in a cleanup phase.
6. Cleanup free commands form an ordered suffix matching the processed
   prefix of the retained cleanup list.
7. Cleanup frees remain dirty until `transaction_finished`.
8. Open or rollback-preparing transaction allocations do not overlap
   any collection live set or unconsumed free regions.
9. Current transaction free intents remain live in their collection and
   outside unconsumed free space while their generation is current.
10. Rollback preparation records are not already unconsumed-free.
11. Pending committed cleanup entries are detached from collection
    live state.
12. Pending rollback allocations are not live in their transaction's
    collection, and rollback cleanup has an explicit rollback record
    for every raw transaction allocation.
13. Current recovery sets across transaction slots do not overlap.
14. Idle transactions have no collection; non-idle transactions name a
    valid modeled collection.
15. `cleanupIndex` is zero outside scan phases and bounded by the
    current scanned list while recording rollback allocations or
    running cleanup.
16. Every region is accounted for by collection live state,
    unconsumed free state, outstanding transaction allocations, or
    pending cleanup.

## Unsafe Comparison Paths

The model also contains unsafe actions used only with `unsafeStep`:

- `UnsafeFreeIntentBeforeCommit`
- `UnsafeCommittedCleanupWithoutSlotOrder`
- `UnsafeRollbackCleanupWithoutSlotOrder`
- `UnsafeEraseDuringCleanup`
- `UnsafeCommitWithoutGenerationCheck`

The unsafe run is expected to violate `safety`; for example, early
freeing of a transaction-private free intent can place a still-live
region into the unconsumed free range.

## Current Implementation Risk

The current implementation allows `append_free_region(collection_id !=
0, region_index)` to write the allocator command `FreeRegion` into an
open transaction log before the transaction has committed. It also
applies that private record to runtime free-space state immediately.

That behavior is unsafe under this model. Before
`commit_transaction`, old committed collection state is still visible
and may still reference the region named by the private `FreeRegion`.
If the transaction rolls back, that earlier allocator effect was never
valid.

The replacement rule is:

1. Before commit, record only transaction-private physical free
   intents.
2. On rollback, write explicit `rollback_allocation(region)` records
   for every transaction allocation before the durable rollback marker.
3. Commit or rollback enters cleanup, records `cleanupStartTail`, and
   acquires the main-WAL cleanup owner.
4. Cleanup writes `free_region(region, free_slot)` in retained-list
   order, where `free_slot = cleanupStartTail + cleanupIndex`.
5. No main-WAL command that advances `readyBoundary` may run before
   `transaction_finished`; transaction allocations may only consume
   the already-ready range.
6. `transaction_finished` clears retained transaction records and
   releases the cleanup owner.

## Model Checking

Typecheck:

```sh
quint typecheck models/transaction_free_recovery.qnt
```

Safe randomized simulation:

```sh
quint run models/transaction_free_recovery.qnt \
  --backend=typescript \
  --invariant=safety \
  --max-samples=2000 \
  --max-steps=30
```

Unsafe comparison, expected to fail:

```sh
quint run models/transaction_free_recovery.qnt \
  --backend=typescript \
  --step=unsafeStep \
  --invariant=safety \
  --max-samples=2000 \
  --max-steps=30
```

Two-collection commit reachability, expected to pass because cleanup
serialization prevents two simultaneous committed-cleanup transactions:

```sh
quint run models/transaction_free_recovery.qnt \
  --backend=typescript \
  --invariant=noTwoDifferentCollectionsCommitted \
  --max-samples=5000 \
  --max-steps=10
```

Same-collection duplicate free-intent reachability, expected to fail:

```sh
quint run models/transaction_free_recovery.qnt \
  --backend=typescript \
  --invariant=noTwoSameCollectionOpenTransactionsStagedSameFreeIntent \
  --max-samples=5000 \
  --max-steps=10
```
