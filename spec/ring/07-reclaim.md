# Chapter 7: Reclaim And Freeing

This chapter groups the rules that make space reusable without losing
replayability: WAL-head reclaim decides which records remain live, and
collection-scoped free operations append detached regions to the FIFO
free-list chain.

Mechanism review:

- **Purpose**: reclaim obsolete WAL and data regions while preserving
  the exact replay result and FIFO free-list ordering.
- **State**: per-record WAL liveness, WAL-rotation `ready_region`,
  transaction recovery state, transaction-log range references,
  free-list head/tail, and live collection/WAL reachability.
- **Named operations**: `ReclaimWalHead`, `FreeRegion`,
  `ReserveRegionForUse`, and retained-basis replay operations.
- **Durable edge sequence**: multi-step reclaim work uses
  transaction-log-backed transactions. Cleanup writes free-list links and
  `free_region(collection_id, region_index)` records.
- **Replay effect**: replay either sees the same live state as before
  reclaim, a completed transaction, or an incomplete transaction that
  recovery can finish or roll back.
- **Crash cuts**: every prefix leaves either the old state live, the new
  state live with cleanup still required, or an idempotent recovery
  task.

## WAL Reclaim Eligibility

WAL-head reclaim is the `ReclaimingWalHead(WalHeadReclaimMode)`
operation. It operates on WAL regions, but correctness is defined per
record. A record is reclaimable only when replay no longer needs it to
rebuild the same collection submachine state, pending updates,
`last_free_list_head`, reserved WAL-rotation `ready_region`,
transaction recovery state, transaction-log live-prefix boundaries, and
reconstructed `free_list_tail` produced by `ApplyWalRecord`.

Per-collection cutoff:

These cutoff terms apply only to user collections (`collection_id !=
0`). WAL-head bootstrap records for `collection_id = 0` are governed
separately below because startup step 4 reconstructs them only from the
current WAL tail region.

1. Let `H(c)` be the current clean durable-basis state for collection
`c` (`EmptyClean`, `WALSnapshotClean`, `RegionClean`, or `Dropped`).
2. Let `D(c)` be the WAL position of the last durable basis decision
record for collection `c` (`new_collection`, `snapshot`,
`drop_collection`, or
`head(collection_id, collection_type, region_index)`).
3. `B(c) = D(c)` is the collection's durable basis position.

Per-record liveness rules:

1. `RING-WAL-RECLAIM-001` `new_collection(collection_id, collection_type)` record:
live only if it is the basis decision at `D(c)` for a collection whose
logical head `H(c)` is `EmptyClean`; otherwise reclaimable.
2. `RING-WAL-RECLAIM-002` `head(collection_id = 0, collection_type = wal, region_index)`
record:
live only if startup step 4 would currently use it as the effective
WAL-head override for the current tail region. Any earlier such control
record, or any such record in a non-tail WAL region, is reclaimable
once the same effective WAL head is preserved by a later tail-local
control record or by the current tail region's `LogRegionPrologue`.
3. `RING-WAL-RECLAIM-003` `head(collection_id, collection_type, region_index)` record for a
user collection:
live only if it is the decision record at `D(c)` for a collection whose
logical head `H(c)` is `RegionClean`; older `head(...)` records are
reclaimable.
4. `RING-WAL-RECLAIM-004` `snapshot` record:
live only if it is the decision record at `D(c)` for a collection whose
logical head `H(c)` is `WALSnapshotClean`; otherwise reclaimable.
5. `RING-WAL-RECLAIM-005` `drop_collection(collection_id)` record:
live only if it is the decision record at `D(c)` for a collection whose
logical head `H(c)` is `Dropped`; older `drop_collection(...)` records
are reclaimable.
6. `RING-WAL-RECLAIM-006` `update` record for collection `c`:
live only if its WAL position is greater than `B(c)`; updates at or
before `B(c)` are reclaimable.
7. `RING-WAL-RECLAIM-007` `link` record:
live only while required to maintain a valid WAL chain from current
WAL head to current WAL tail.
8. `RING-WAL-RECLAIM-008` `alloc_begin(collection_id, region_index,
allocation_sequence, free_list_head_after)` record:
live if either it is needed after the retained log segment prologue
checkpoint to reconstruct the newest allocator-head decision, or its
WAL-rotation reservation is still needed to recover an unmatched
`ready_region`. The reservation role exists only for `collection_id = 0`
until `link` durably consumes the allocated WAL region; after that
point, retaining the record is no longer required for
region-consumption validity if the allocator cursor is represented by a
later segment prologue or retained allocation record.
9. `RING-WAL-RECLAIM-009` Main-WAL transaction-control records are live
while startup replay still needs them to import a committed
transaction-log range, prove rollback completed, finish committed
cleanup, or keep a transaction-log range referenced for garbage
collection.
10. `RING-WAL-RECLAIM-010` `free_region(collection_id, region_index)` record:
live only while replay still needs it to reconstruct the durable
allocator head/tail state or prove that cleanup for the owning
collection's transaction completed.
11. `RING-WAL-RECLAIM-011` `wal_recovery` record:
live only if replay still needs it to justify later valid WAL records
that appear after an ignored corrupt/torn span in that WAL region.
12. `RING-WAL-RECLAIM-013` Transaction-log records are live while any
retained main-WAL commit, rollback, finish record, open transaction
descriptor, or pending recovery descriptor points to a range containing
them. They become reclaimable only when the transaction log's live
prefix advances past them.

WAL-region reclaim preconditions:

1. `RING-WAL-RECLAIM-PRE-001` The candidate region MUST be the head of the WAL.
2. `RING-WAL-RECLAIM-PRE-002` For every live record in the candidate, an equivalent live state MUST
already be represented durably outside the candidate.
3. `RING-WAL-RECLAIM-PRE-003` After planned metadata updates, startup replay MUST still be able to
walk a valid WAL chain from head to tail.

WAL-region reclaim postconditions:

1. `RING-WAL-RECLAIM-POST-001` A collection's `H(c)`, `B(c)`, and live
post-basis updates MUST NOT depend on bytes in the reclaimed region.
2. `RING-WAL-RECLAIM-POST-002` The recovered free-list head MUST match pre-reclaim allocator state.
3. `RING-WAL-RECLAIM-POST-003` The recovered WAL-rotation `ready_region`, if any, MUST match
pre-reclaim allocator state.
4. `RING-WAL-RECLAIM-POST-004` Transaction recovery state that replay would continue MUST match
pre-reclaim crash-recovery state.
5. `RING-WAL-RECLAIM-POST-005` Startup step 4 MUST recover the same effective WAL head after
reclaim as before reclaim, using the current tail region's
`LogRegionPrologue` plus the last valid tail-local
`head(collection_id = 0, collection_type = wal, region_index = ...)`
override, if any.
6. `RING-WAL-RECLAIM-POST-006` WAL chain integrity MUST remain valid with no broken `link` path.
7. `RING-WAL-RECLAIM-POST-007` The reclaimed region MUST be erased before reuse.
8. `RING-WAL-RECLAIM-POST-008` If reclaim allocates any replacement WAL regions, replay-visible
`alloc_begin` records for those allocations carry collection id,
`allocation_sequence`, and `free_list_head_after`, and any new
`LogRegionPrologue` carries the allocator cursor checkpoint so replay
reconstructs the same allocator position.

Safety invariant:

1. `RING-WAL-RECLAIM-SAFE-001` Reclaim MUST NOT change replay result: the recovered collection
submachine state and pending updates for every collection, the recovered
`last_free_list_head`, reserved WAL-rotation `ready_region`,
transaction recovery state, and reconstructed `free_list_tail`, after
reclaim must match the pre-reclaim logical state.

## Transaction-Log Reclaim Eligibility

Transaction-log reclaim is prefix-only for each transaction log. A
transaction-log region or record may be reclaimed only when no retained
main-WAL `commit_transaction(transaction_log_id, range)`,
`rollback_transaction(transaction_log_id, range)`, or
`transaction_finished(transaction_log_id, range)` record, no open
transaction descriptor, and no pending recovery descriptor points into
that prefix. The reclaim result must preserve the same imported
committed ranges, rollback ranges, cleanup obligations,
transaction-log append cursors, and allocator cursor recovery result as
before reclaim.

1. `RING-TXLOG-RECLAIM-001` Transaction-log reclaim MUST advance only a
contiguous prefix of one transaction log.
2. `RING-TXLOG-RECLAIM-002` A transaction-log prefix MUST NOT be
reclaimed while any retained main-WAL transaction-control record
references a range overlapping that prefix.
3. `RING-TXLOG-RECLAIM-003` A transaction-log prefix MUST NOT be
reclaimed while an open transaction or pending recovery descriptor
references a range overlapping that prefix.
4. `RING-TXLOG-RECLAIM-004` After transaction-log reclaim, startup MUST
recover the same transaction-log cursors, live-prefix boundaries,
imported committed collection state, rollback state, and allocator
state as before reclaim.

## Free Region

Freeing a region is the `FreeRegion` cleanup operation. It appends a
newly detached region to the tail of the free-list chain and records
that append with `free_region(collection_id, region_index)`. Although
the free-list chain is global allocator state, the WAL record is
collection-scoped because the region is leaving the named collection.

Normative append semantics:

1. `RING-FREE-REGION-SEM-001` Let `t_prev` be the value of `free_list_tail` before freeing
`region_index`.
2. `RING-FREE-REGION-SEM-002` If `t_prev != none`, freeing MUST durably write
`t_prev.free_pointer.next_tail = region_index`.
3. `RING-FREE-REGION-SEM-003` If `t_prev = none`, freeing MUST NOT write any predecessor link; the
`free_region(collection_id, region_index)` record becomes the new
durable free-list head decision.
4. `RING-FREE-REGION-SEM-004` The free is not complete until the predecessor-link write, when
required, and the `free_region` record are both durable.

Preconditions:

1. `RING-FREE-REGION-PRE-001` The region MUST no longer be reachable from any live collection head,
collection-defined region reference, WAL chain, or ready allocation
state before `free_region(collection_id, region_index)` is appended.
2. `RING-FREE-REGION-PRE-002` The region MUST NOT already be reachable from the free-list chain,
unless this procedure is being re-entered during idempotent recovery.
3. `RING-FREE-REGION-PRE-003` The owning collection's committed
transaction state MUST contain enough durable information for cleanup
recovery to derive that `region_index` must be freed.

Procedure:

1. `RING-FREE-REGION-001` Establish `region_index` as a free-tail
candidate without erasing it. Its free-pointer footer MUST be
unwritten: all footer bytes equal `erased_byte`.
2. `RING-FREE-REGION-001A` A normal `free_region` operation MUST fail
before linking if the freed region's free-pointer footer is not
unwritten.
3. `RING-FREE-REGION-002` If `t_prev` exists, write and sync
`t_prev.free_pointer.next_tail = region_index`.
4. `RING-FREE-REGION-003` Append and sync
`free_region(collection_id, region_index)`.
5. `RING-FREE-REGION-004` Update runtime `free_list_tail = region_index`; if the free list was
empty, also update runtime `last_free_list_head = Some(region_index)`.

Normal cleanup MUST NOT erase `region_index`. Recovery may erase a
transaction-owned allocation that crashed before its allocation erase
completed, but only after proving the region is not reachable from live
collection state.

Postconditions:

1. `RING-FREE-REGION-POST-001` The free-list chain MUST remain acyclic and FIFO-ordered.
2. `RING-FREE-REGION-POST-002` Exactly one new region MUST be appended to the tail.
3. `RING-FREE-REGION-POST-003` If a prior tail existed, its `next_tail` pointer MUST now reference
the freed region.
4. `RING-FREE-REGION-POST-004` The freed region's `next_tail` pointer MUST remain uninitialized
after the free.
5. `RING-FREE-REGION-POST-005` The free operation MUST be idempotent across crashes between any two
steps above.

## Transaction Cleanup Recovery

1. `RING-TX-RECOVERY-001` If startup reaches main WAL end with an open
transaction descriptor and no durable
`commit_transaction(transaction_log_id, range)`, it MUST run rollback
recovery for that transaction-log range and append
`rollback_transaction(transaction_log_id, range)`.
2. `RING-TX-RECOVERY-002` If startup reaches main WAL end after
`commit_transaction(transaction_log_id, range)` but before
`transaction_finished(transaction_log_id, range)`, it MUST preserve the
committed collection state imported from that transaction-log range,
finish cleanup frees derived from the committed range, and append
`transaction_finished(transaction_log_id, range)`.
3. `RING-TX-RECOVERY-003` Both rollback recovery and cleanup recovery
MUST be idempotent if startup crashes before the terminal marker is
durable.
4. `RING-TX-RECOVERY-004` The configured minimum free-region reserve MUST leave enough WAL
capacity for startup recovery to append a required terminal transaction
record.

## Region Reclaim

These requirements preserve existing trace identifiers while the
implementation moves from the previous cleanup mechanism to the
transaction-log-backed transaction model.

1. `RING-REGION-RECLAIM-PRE-001` Transaction cleanup MUST make the
transaction begin marker durable before durable collection metadata
stops referencing regions that cleanup may free.
2. `RING-REGION-RECLAIM-PRE-002` After the committed collection-state
update, a region selected for cleanup MUST no longer be reachable from
any live collection head, WAL chain, or ready allocation state.
3. `RING-REGION-RECLAIM-PRE-003` A cleanup target MUST NOT already be
reachable from the free-list chain unless startup is re-entering
idempotent recovery.
4. `RING-REGION-RECLAIM-SEM-002` If a prior free-list tail exists,
cleanup MUST durably write that tail's `next_tail` pointer to the freed
region.
5. `RING-REGION-RECLAIM-SEM-003` If no free-list tail exists, cleanup
MUST make the freed region the durable free-list head through the
`free_region(collection_id, region_index)` record.
6. `RING-REGION-RECLAIM-004` Cleanup MUST leave the newly freed region's
free-pointer successor uninitialized so it is recognizable as the
free-list tail.
7. `RING-REGION-RECLAIM-ORDER-001` Transaction cleanup MUST not make a
region free until the committed collection state no longer references
that region.
8. `RING-REGION-RECLAIM-ORDER-002` Before any durable write links a
freed region from the previous free-list tail, the freed region MUST
already have the correct uninitialized free-list-tail footer state.
9. `RING-REGION-RECLAIM-ORDER-003` If the free list was empty, the
`free_region(collection_id, region_index)` record MUST be durable before
cleanup can be considered finished.
10. `RING-REGION-RECLAIM-ORDER-004` If a prior free-list tail exists,
the tail-link footer write MUST be synced before cleanup can be
considered finished.
11. `RING-REGION-RECLAIM-ORDER-005` Transaction cleanup MUST be
idempotent across crashes between any two cleanup steps.
12. `RING-REGION-RECLAIM-POST-001` The free-list chain MUST remain
acyclic and FIFO-ordered after cleanup.
13. `RING-REGION-RECLAIM-POST-002` Cleanup MUST append exactly one
newly freed region for each `free_region(collection_id, region_index)`
record.
14. `RING-REGION-RECLAIM-POST-003` If a prior free-list tail existed,
its `next_tail` pointer MUST reference the newly freed region after
cleanup.
15. `RING-REGION-RECLAIM-POST-004` The newly freed region's
free-pointer successor MUST remain uninitialized after cleanup.
16. `RING-REGION-RECLAIM-POST-005` Replay of free pointers MUST follow
the previous tail to the newly freed region when a prior free-list tail
existed.
