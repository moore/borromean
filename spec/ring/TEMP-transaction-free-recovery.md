# Temporary Note: Transaction Recovery And Log-Based Free Space

This note captures the current design discussion before it is folded
back into the numbered ring specification chapters.

## Problem

The free-space collection is log based. A free region is not marked by
local state in the freed region itself. Whether a region is free is
known only by replaying the materialized free-space checkpoint plus the
retained allocator records in WAL order.

This matters during recovery. If startup recovery appends a
`free_region(R)` record and then crashes before appending the terminal
transaction marker, the next startup must not append a second
`free_region(R)`.

## Core Model

Allocator commands have global effects in the main WAL order.

An `allocate_region` record immediately consumes a ready entry from the
global free-space collection when that allocator command is replayed.
The allocated region may then be owned by a transaction, a private log
rotation, or another privileged storage-core operation.

A transaction does not make old committed regions free inside the
transaction log. The transaction commit atomically changes the visible
collection state at the main WAL `commit_transaction` record. Only after
that commit is durable can old committed regions be detached and freed.
Those frees are ordinary main-WAL `free_region` records appended after
`commit_transaction` and before `transaction_finished`.

This implies a two-stage free model. A transaction may record that a
region should be freed if the transaction commits, but that record is a
transaction-private free intent, not an allocator command. It must not
append to the global free-space collection and must not advance
`append_tail` during transaction replay before the main-WAL commit.
After `commit_transaction` is durable, recovery or foreground cleanup
turns those committed free intents into ordinary main-WAL `free_region`
records before `transaction_finished`.

This avoids forcing each collection to keep a separate long-lived list
of detached regions until some later collection phase. The transaction
log carries the bounded cleanup intent list while the transaction is
open or committed-but-unfinished. Once `transaction_finished` is
durable, the intents are no longer needed.

Because of this split, "cleanup" has two distinct meanings:

1. Rollback cleanup before commit: return transaction-owned allocations
   that already affected the global allocator.
2. Post-commit free completion: append main-WAL frees for regions that
   became detached when the committed transaction swapped the collection
   state.

## Invariants

1. The free-space collection is reconstructed by replaying the
   materialized checkpoint and retained WAL allocator records.
2. Replay builds enough free-space membership state to answer whether a
   region is already present in the active free-space collection.
3. A durable `free_region(R)` is the progress record for freeing `R`.
   No separate per-region free marker exists.
4. A recovery path that wants to free `R` must skip the append if `R` is
   already present in the recovered active free-space collection.
5. A recovery path must also skip the append if the same recovery pass
   has already observed or appended `free_region(R)`.
6. A region may appear at most once in the active free-space collection.
7. Transaction-log records do not apply allocator-effective frees.
   Actual free-space mutation for freed regions happens only through
   main-WAL `free_region` records.
8. A committed transaction is not complete until every required
   post-commit main-WAL free is durable and
   `transaction_finished(transaction_log_id, range)` is durable.
9. An uncommitted transaction never frees old committed collection
   regions. It can only require rollback cleanup for regions it
   allocated.
10. A committed transaction must retain enough replay information to
    derive post-commit free obligations until `transaction_finished` is
    durable.
11. A main-WAL `free_region` command in a transaction log is invalid.
    Allowing it would let an uncommitted transaction free a region that
    remains live in the committed collection state, and rollback would
    not make that earlier free safe.
12. A transaction-private free-intent record is valid only inside a
    transaction range. It has no allocator effect before commit. On
    rollback it is ignored. After commit it becomes a cleanup obligation
    that must be satisfied by a main-WAL `free_region`.

## Startup Replay Shape

Startup first reconstructs the free-space collection from the
checkpoint and retained WAL records. When replay sees the first retained
`free_region` after the checkpoint, it validates that the record appends
at the current `append_tail`. Each following retained free must advance
the append position exactly once. During this pass, startup can maintain
a bounded membership set keyed by region index.

After allocator replay, recovery can test a cleanup target with:

```text
already_done =
    region is present in recovered active free-space membership
    or region was observed/appended in this recovery pass
```

If `already_done` is true, recovery must not append another
`free_region` for that region.

## Transaction Crash Cuts

### Before `commit_transaction`

The visible collection state is still the old committed state. Old
committed regions remain live and must not be freed.

Any transaction-owned allocation that reached the global allocator must
be returned to the dirty free-space range with a main-WAL
`free_region`. Recovery then appends
`rollback_transaction(transaction_log_id, range)`.

If recovery crashes after one rollback `free_region` but before the
rollback marker, the next startup sees the free in the recovered
free-space collection and skips that allocation before appending any
remaining missing frees.

### After `commit_transaction` Before `transaction_finished`

The transaction's new collection state is visible. The commit point is
the atomic state swap.

Startup must preserve the committed state, scan the committed
transaction range for free intents, append any missing main-WAL
`free_region` records for those intents, and then append
`transaction_finished(transaction_log_id, range)`.

If recovery crashes after freeing some detached regions but before
`transaction_finished`, the next startup sees those regions in the
recovered free-space collection and appends only the missing frees.

### After `transaction_finished`

All rollback or post-commit free obligations are complete. Startup must
not repeat transaction recovery for that range.

## Transaction Segment Layout

The transaction segment must not contain allocator-effective free
records. It may contain transaction-private free intents. It also needs
to represent transaction-private collection changes and
transaction-owned allocations.

Transaction-log writes occur when committing or when an in-memory
frontier is full. At those points the writer already knows the boundary
between data records and transaction bookkeeping. If the physical
layout uses one side for data records and the other side for allocation
or bookkeeping records, the commit descriptor should record the exact
ranges. Recovery should not infer the boundary heuristically.

The commit descriptor may therefore carry enough layout information to
scan only the transaction-owned allocation records needed for rollback
recovery and the transaction-private free-intent records needed for
post-commit cleanup.

## Current Implementation Risk

The current implementation allows `append_free_region(collection_id !=
0, region_index)` to write the allocator command `FreeRegion` into an
open transaction log before the transaction has committed. It also
applies that private record to runtime free-space state immediately.

That behavior is unsafe under the model in this note. Before
`commit_transaction`, the old committed collection state is still
visible and may still reference the region named by that private
`FreeRegion`. If the transaction rolls back, the attempted free was
never valid as a global allocator fact. Therefore transaction-log
`FreeRegion` should be rejected rather than treated as staged cleanup.
A separate transaction-private free-intent record can safely represent
the staged cleanup because it has no allocator effect until after
commit.

The replacement rule is simple:

1. Before commit, do not free old committed regions.
2. On rollback, free only transaction-owned allocations in the main WAL.
3. After commit, scan committed free intents and free them in the main
   WAL before `transaction_finished`.

## Open Questions

1. Should duplicate free-region membership be enforced directly in the
   shared allocator replay helper, not only by transaction recovery
   bookkeeping?
2. What is the exact bounded representation for recovered free-space
   membership: bitset by region index, sorted bounded vector, or
   existing queue scan?
3. What should the transaction-private free-intent record be named and
   encoded as?
4. Should the transaction commit descriptor explicitly name the
   allocation-record and free-intent ranges, or is scanning the full
   committed range acceptable under the configured bounds?

## Model Checking

The first Quint model for this design lives at
`models/transaction_free_recovery.qnt`.

Run the safe transition relation with:

```sh
quint run models/transaction_free_recovery.qnt \
  --backend=typescript \
  --invariant=safety \
  --max-samples=1000 \
  --max-steps=16
```

The model also includes `unsafeStep`, which adds the current unsafe
behavior where a transaction-private free mutates the global allocator
before commit. This should violate `safety`:

```sh
quint run models/transaction_free_recovery.qnt \
  --backend=typescript \
  --step=unsafeStep \
  --invariant=safety \
  --max-samples=200 \
  --max-steps=6
```
