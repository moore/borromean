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
- `live: Set[int]`: semantic/oracle state for all committed collection
  contents modeled here.

`StorageState` carries:

- `freeQueue`: the allocator queue.
- `allocationHead`: first active ready entry.
- `readyBoundary`: first dirty entry.
- `appendTail`: one past the last active free entry.
- `regions: List[RegionHeader]`: abstract decoded header state,
  indexed by physical region id.
- `nextSequence`: the next sequence assigned when a valid region header
  is written.
- `collectionGenerations: List[int]`: per-collection generation heads.
- `transactions: List[TxState]`: two fixed transaction slots indexed
  by `0` and `1`.

The model intentionally keeps `live` global in this step. It represents
allocator ownership across the modeled collections, not the exact
per-collection object graph. Per-collection conflict behavior is modeled
by `storage.collectionGenerations`; wrong-collection free selection is
deferred to a later, higher-fidelity model.

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

`CommitTransaction` and `RollbackTransaction` do not clear the
transaction lists. Only `FinishTransaction` clears `allocations`,
`freeIntents`, and `rollbackAllocations`. Rollback cleanup uses
`rollbackAllocations`, which are explicit records written during
rollback preparation.

Each non-idle transaction is bound to exactly one collection. This
models the production "add collection to transaction" path at the
granularity needed here: two transactions can commit independently when
their `collectionIndex` values differ, while same-collection stale
commits are rejected by generation checks.

Transaction allocation lists are retained provenance until
`FinishTransaction`. Before commit or rollback they are private and must
not overlap `live` or active free space. After commit they overlap
`live`. After rollback cleanup starts they may overlap active free space
as individual rollback allocation records are freed.

Cleanup progress is not tracked by a retained progress list. Instead,
cleanup checks the allocator and the region header sequence:

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
`nextSequence`. `CommitTransaction` requires every retained allocation
to have a valid header before publishing it into `live`.

`StageFreeIntent(tx)` records a transaction-private free intent using
the sequence currently stored in `storage.regions[region]`. The region
is chosen from the global semantic `live` set in this model step. The
transaction's `collectionIndex` still controls generation conflict
behavior. Staging the free intent does not append to `freeQueue`, does
not advance `appendTail`, and has no allocator effect before commit.

`CommitTransaction(tx)` requires:

```text
tx.observedGeneration == storage.collectionGenerations[tx.collectionIndex]
all allocation headers are valid
all free-intent headers match
```

It atomically updates:

```text
live = (live - tx.freeIntents.region) union tx.allocations.region
storage.collectionGenerations[tx.collectionIndex] += 1
tx.phase = TxCommittedCleanup
```

`StartRollbackPreparation(tx)` records the decision to roll back but
does not write the durable rollback marker. This is the safe window
where all transaction-owned allocation headers can be forced: the
regions are still private to the transaction and cannot have been
reallocated.

`RecordRollbackAllocation(tx)` records one rollback cleanup obligation
as `{ region, sequence }` after that allocated region has a valid
header. These records are written before the durable rollback marker, so
recovery after the marker can clean up from durable transaction records
instead of inferring obligations from raw allocation entries.

`RollbackTransaction(tx)` requires every raw allocation to have a
rollback allocation record, and requires those records to still match
their region headers. It then enters `TxRolledBackCleanup` without
changing `live`.

`FreeCommittedIntent(tx)` appends one committed free intent only if the
target is not active-free and still has the expected valid header
sequence.

`FreeRolledBackAllocation(tx)` applies the same rule to one explicit
rollback allocation record.

`FinishTransaction(tx)` is allowed only when every relevant cleanup
obligation is either active-free or no longer header-matching. It then
resets the slot to `TxIdle`.

`AllocateDirectlyLive` models a non-transaction WAL allocation that
immediately becomes committed live ownership. It consumes one ready free
entry, writes `validHeader(nextSequence)`, advances `nextSequence`, and
adds the region to `live`.

`FreeDirectLive` models a non-transaction WAL free of a committed live
region. It removes the region from `live` and appends it to the dirty
free range. The model excludes regions retained by unfinished
transactions from this direct free action, because those retained
records remain provenance until `transaction_finished`.

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
3. `live` does not overlap active free-space membership.
4. Live regions have valid headers.
5. Ready free regions do not have valid headers.
6. Open or rollback-preparing transaction allocations do not overlap
   live or active-free regions.
7. Committed transaction allocations are in `live` while retained as
   provenance until finish.
8. Outstanding transaction allocations across the two slots do not
   overlap.
9. Open transaction free intents remain live, header-matching, and have
   no allocator effect.
10. Pending committed cleanup intents are detached from live state.
11. Pending rollback allocations are not live.
12. Free intents are not shared between transaction slots.
13. Rollback cleanup has an explicit rollback record for every raw
    transaction allocation.
14. Idle transactions have no collection; non-idle transactions name a
    valid modeled collection.
15. Every region is accounted for by `live`, active free state, or
    retained transaction regions. Retained transaction regions include
    transaction allocations and pending committed free-intent cleanup.

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

1. Before commit, record only transaction-private free intents with the
   current region sequence.
2. On rollback, first force headers for all transaction-owned
   allocations.
3. Then write explicit `rollback_allocation(region, sequence)` records
   for each allocation.
4. Then write the durable rollback marker.
5. Rollback cleanup frees only rollback allocation records that still
   have the retained sequence and are not already active-free.
6. After commit, cleanup frees committed free intents that still have
   the retained sequence and are not already active-free.
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
