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
scanning, record decoding, WAL-head selection, or torn-record recovery.
It proves the semantic transaction/allocator protocol assuming replay
and foreground apply share one command implementation.

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
- `allocationHead`: first active ready entry.
- `readyBoundary`: first dirty entry.
- `appendTail`: one past the last active free entry.
- `regions: List[RegionHeader]`: abstract decoded header state,
  indexed by physical region id.
- `nextSequence`: the next sequence assigned when a valid region header
  is written.
- `collections: List[CollectionState]`: committed collection state.
- `transactions: List[TxState]`: two fixed transaction slots indexed
  by `0` and `1`.

`CollectionState` carries:

- `live: Set[int]`: committed live regions for the collection.
- `generation: int`: the collection generation head.

The model keeps committed live membership per collection. Allocator
invariants use the union of `storage.collections[*].live`, while
transaction operations select and mutate only the collection named by
the transaction slot. No region may be live in more than one collection.
A live region does not need a valid header; headers are sequence
references for durable cleanup obligations, not the source of live
ownership.

The active free range is:

```text
freeQueue[allocationHead, appendTail)
```

The ready sub-range is:

```text
freeQueue[allocationHead, readyBoundary)
```

The dirty sub-range is:

```text
freeQueue[readyBoundary, appendTail)
```

Allocator cursor safety is:

```text
0 <= allocationHead <= readyBoundary <= appendTail <= freeQueue.length
```

## Transaction State

Each transaction slot carries:

- `phase`
- `collectionIndex`
- `observedGeneration`
- `cleanupIndex: int`
- `allocations: List[int]`
- `freeIntents: List[RegionRef]`
- `rollbackAllocations: List[RegionRef]`

`RegionRef` is:

```text
{ region, sequence }
```

The lifecycle is:

```text
TxIdle -> TxOpen -> TxCommittedCleanup -> TxIdle
TxIdle -> TxOpen -> TxRollbackPreparing -> TxRolledBackCleanup -> TxIdle
```

`CommitTransaction`, `StartRollbackPreparation`, and
`RollbackTransaction` do not clear the transaction lists. They reset
`cleanupIndex = 0` when entering a scan phase: committed cleanup,
rollback-allocation recording, or rollback cleanup. Only
`FinishCommit` and `FinishRollback` clear `allocations`, `freeIntents`,
and `rollbackAllocations`. Rollback cleanup uses `rollbackAllocations`,
which are explicit records written during rollback preparation.

Each non-idle transaction is bound to exactly one collection. This
models the production "add collection to transaction" path at the
granularity needed here: two transactions can commit independently when
their `collectionIndex` values differ, while same-collection stale
commits are rejected by generation checks.

Transaction allocation lists are retained provenance until `FinishCommit`
or `FinishRollback`. Before commit or rollback they are private and must
not overlap the global live set or active free space. After commit,
allocations become ordinary committed collection live regions; a later
transaction may detach them before the earlier transaction finishes.
After rollback cleanup starts they may overlap active free space as
individual rollback allocation records are freed.

Rollback-allocation recording and cleanup are modeled as ordered scans
over the relevant retained list. `cleanupIndex` is model/runtime control
state for the current scan, not a durable progress log. Each scan step
examines the current list entry, performs the relevant header or
retained-reference check, and then advances `cleanupIndex`.

For each entry, cleanup checks the allocator and the region header
sequence:

1. If the target region is already active-free, the obligation is done.
2. If the target region is not active-free and its valid header still
   matches the retained sequence, cleanup may append `free_region`.
3. If the target region is not active-free and the header no longer
   matches the retained sequence, the old obligation is obsolete and
   must not append another free record.

This avoids the crash race where a separate cleanup-progress list would
need its own recovery protocol.

## Command Semantics

`BeginTransaction(tx)` selects a collection, records that collection's
current generation in the selected slot, and enters `TxOpen`.

`AllocateInTransaction(tx)` consumes the current ready entry by
advancing `allocationHead` and records only the region index in that
transaction's `allocations` list. Allocation does not assign a sequence,
does not write a header, and does not modify `regions`. Ready free
regions are already erased before allocation.

`WriteTransactionAllocationHeader(tx)` writes one missing allocation
header for a transaction-owned region. This is the sequence-assignment
point: it writes `validHeader(nextSequence)` and advances
`nextSequence`. Durable rollback needs these headers before writing
explicit rollback allocation records. Commit can publish transaction
allocations into the collection live set without allocation headers.

`StageFreeIntent(tx)` records a transaction-private free intent for a
region chosen from the live set for the transaction's collection,
excluding only regions already staged by that same transaction. If the
live region already has a valid header, the action records that
sequence. If it does not, the action writes `validHeader(nextSequence)`,
advances `nextSequence`, and records the new sequence. Another
transaction may stage an intent for the same region; generation checks
and retained sequence references decide which later actions are valid.
Staging does not require the transaction's observed generation to still
be current; commit is the authoritative generation conflict check.
Staging the free intent does not append to `freeQueue`, does not advance
`appendTail`, and has no allocator effect before commit.

`CommitTransaction(tx)` requires:

```text
tx.observedGeneration == storage.collections[tx.collectionIndex].generation
```

It atomically updates:

```text
storage.collections[tx.collectionIndex].live =
    (storage.collections[tx.collectionIndex].live - tx.freeIntents.region)
        union tx.allocations.region
storage.collections[tx.collectionIndex].generation += 1
tx.phase = TxCommittedCleanup
```

Commit removes `tx.freeIntents.region` from collection live state, but
does not append those regions to `freeQueue`. The committed free intents
are in an in-between retained cleanup state: they are no longer
collection-live and not yet allocator-free.

Commit does not revalidate free-intent live membership or header
matches. The generation check makes live membership an inductive
property for current transactions, and committed cleanup performs the
retained header check before appending each free.

`StartRollbackPreparation(tx)` records the decision to roll back but
does not write the durable rollback marker. This is the safe window
where all transaction-owned allocation headers can be forced: the
regions are still private to the transaction and cannot have been
reallocated.

`RecordRollbackAllocation(tx)` processes
`tx.allocations[tx.cleanupIndex]`. Once that allocated region has a
valid header, it records one rollback cleanup obligation as
`{ region, sequence }` and advances `cleanupIndex`. These records are
written before the durable rollback marker, so recovery after the marker
can clean up from durable transaction records instead of inferring
obligations from raw allocation entries.

`RollbackTransaction(tx)` is allowed only when `cleanupIndex` has
reached the end of `allocations`, which means every raw allocation has
been scanned into `rollbackAllocations`. It does not re-check those
retained headers at the rollback marker; in this model, rollback
allocation records are written while transaction allocations are
private, so the header match is inductively preserved until rollback. It
then resets `cleanupIndex` and enters `TxRolledBackCleanup` without
changing collection live state.

`FreeCommittedIntent(tx)` processes
`tx.freeIntents[tx.cleanupIndex]`. If the target is not active-free and
the retained reference still has the expected valid header sequence, it
appends that region to the dirty free range. If the target is already
active-free or the retained header no longer matches, it skips the
entry. In both cases it advances `cleanupIndex`. The collection live
removal already happened at commit, and the model keeps "not
collection-live" as an inductive invariant of pending committed
cleanup. In this model, committed cleanup frees do not bump the
collection generation.

`FreeRolledBackAllocation(tx)` processes
`tx.rollbackAllocations[tx.cleanupIndex]` using the same
retained-reference rule: append only when the target is not active-free
and the retained rollback allocation reference still matches the valid
region header; otherwise skip. In both cases it advances
`cleanupIndex`. Rollback allocations are not live in the transaction's
collection as an inductive invariant of pending rollback cleanup.

`FinishCommit(tx)` is allowed only when `cleanupIndex` has reached the
end of `freeIntents`. `FinishRollback(tx)` is allowed only when
`cleanupIndex` has reached the end of `rollbackAllocations`. Both model
the same durable `transaction_finished(tx)` command shape and reset the
slot to `TxIdle`.

`AllocateDirectlyLive` models a non-transaction WAL allocation that
immediately becomes committed live ownership for one selected
collection. It consumes one ready free entry, writes
`validHeader(nextSequence)`, advances `nextSequence`, adds the region to
that collection's live set, and bumps that collection generation.

`FreeDirectLive` models a non-transaction WAL free of a committed live
region from one selected collection. It removes the region from that
collection's live set, appends it to the dirty free range, and bumps
that collection generation. Because the action chooses from collection
live, disjointness from active free space, outstanding transaction
allocations, and pending committed cleanup is maintained by the safety
invariants rather than by direct action guards. It does not exclude
regions merely because another open transaction has staged a private
free intent for them.

`EraseOneDirty` invalidates the region header for the entry becoming
ready-free, matching the requirement that ready free space has been
erased.

## Important Recovery Point

Cleanup must be idempotent without a separate cleanup-progress log.

It is not enough to ask whether a cleanup target is currently present
in the active free range. A recovery pass may have already appended
`free_region(R)`, then `erase_free_region_span` may make `R` ready, and
a later allocator command may reallocate `R` before
`transaction_finished` is durable.

In that state, `R` is no longer active-free, but cleanup for the old
transaction is already complete. Repeating `free_region(R)` would be a
double free of a region that may now belong to another owner.

The retained `{ region, sequence }` reference handles this case. For
committed frees, the reference comes from `freeIntents`. For rollback,
the reference comes from explicit `rollbackAllocations` records written
before the rollback marker. After reallocation, the active header
sequence is different from the retained cleanup reference, so cleanup
must not append another `free_region`.

## Invariants

The model checks:

1. Allocator cursors remain ordered.
2. Active free entries have no duplicates.
3. No region is live in more than one modeled collection.
4. The union of collection live sets does not overlap active
   free-space membership.
5. Ready free regions do not have valid headers.
6. Open or rollback-preparing transaction allocations do not overlap
   the global live set or active-free regions.
7. Outstanding transaction allocations across the two slots do not
   overlap.
8. Current open transaction free intents that are still actionable
   remain live in that transaction's collection. Open transactions may
   retain obsolete free intents because commit is blocked when the
   transaction generation is stale.
9. Pending committed cleanup intents are detached from collection live
   state and remain outside active free space until cleanup appends them
   or the retained header reference becomes obsolete.
10. Pending rollback allocations are not live in their transaction's
    collection.
11. Rollback cleanup has an explicit rollback record for every raw
    transaction allocation.
12. Idle transactions have no collection; non-idle transactions name a
    valid modeled collection.
13. `cleanupIndex` is zero outside scan phases and bounded by the
    active list while recording rollback allocations or running cleanup.
14. Every region is accounted for by collection live state, active free
    state, outstanding transaction allocations, or pending committed
    free-intent cleanup.

## Unsafe Comparison Paths

The model also contains unsafe actions used only with `unsafeStep`:

- `UnsafeFreeIntentBeforeCommit`
- `UnsafeCommittedCleanupWithoutMembershipCheck`
- `UnsafeRollbackCleanupWithoutMembershipCheck`
- `UnsafeCommitWithoutGenerationCheck`

The unsafe run is expected to violate `safety`; for example, early
freeing of a transaction-private free intent can place a still-live
region into the active free range.

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

1. Before commit, record only transaction-private free intents with a
   sequence resolved by writing or observing the current region header.
2. On rollback, first force headers for all transaction-owned
   allocations.
3. Then write explicit `rollback_allocation(region, sequence)` records
   for each allocation.
4. Then write the durable rollback marker.
5. Rollback cleanup frees only rollback allocation records that still
   have the retained sequence and are not already active-free.
6. After commit, cleanup frees committed free intents that are detached
   from collection live, still have the retained sequence, and are not
   already active-free.
7. `transaction_finished` clears the retained transaction refs.

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
  --max-samples=200 \
  --max-steps=12
```

Two-collection commit reachability, expected to fail the negative
invariant:

```sh
quint run models/transaction_free_recovery.qnt \
  --backend=typescript \
  --invariant=noTwoDifferentCollectionsCommitted \
  --max-samples=5000 \
  --max-steps=10
```

Same-collection duplicate free-intent reachability, expected to fail
the negative invariant:

```sh
quint run models/transaction_free_recovery.qnt \
  --backend=typescript \
  --invariant=noTwoSameCollectionOpenTransactionsStagedSameFreeIntent \
  --max-samples=5000 \
  --max-steps=10
```
