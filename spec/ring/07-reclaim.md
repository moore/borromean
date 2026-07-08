# Chapter 7: Reclaim And Freeing

This chapter groups the rules that make space reusable without losing
replayability: WAL-head reclaim decides which records remain live, and
cleanup appends detached regions to the storage-private free-space
collection.

Mechanism review:

- **Purpose**: reclaim obsolete WAL and data regions while preserving
  the exact replay result and FIFO free-space ordering.
- **State**: per-record WAL liveness, storage-core private allocation
  reservations, transaction recovery state, transaction-log range
  references, free-space collection cursors, and live collection/WAL
  reachability.
- **Named operations**: `ReclaimWalHead`, `FreeRegion`,
  `EraseFreeRegionSpan`, `AllocateRegionForUse`, and retained-basis
  replay operations.
- **Durable edge sequence**: multi-step reclaim work uses full
  transaction-log-backed transactions or bounded inline transactions.
  Full-transaction cleanup writes ordered
  `free_region(region_index, append_tail_after)` records while the
  transaction owns main-WAL cleanup; maintenance erases dirty entries
  before publishing `erase_free_region_span(count,
  ready_boundary_after)`.
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
free-space collection cursors, storage-core private allocation
reservation, transaction recovery state, transaction-log live-prefix
boundaries, and WAL-chain reachability produced by `ApplyWalRecord`.

Per-collection cutoff:

These cutoff terms apply only to user collections (`collection_id !=
0`). WAL-head bootstrap records for `collection_id = 0` are governed
separately below because startup reconstructs them only from the current
WAL tail region.

1. Let `H(c)` be the current clean durable-basis state for collection
   `c` (`EmptyClean`, `WALSnapshotClean`, `RegionClean`, or `Dropped`).
2. Let `D(c)` be the WAL position of the last durable basis decision
   record for collection `c` (`new_collection`, `snapshot`,
   `drop_collection`, or `head(collection_id, collection_type,
   region_index)`).
3. `B(c) = D(c)` is the collection's durable basis position.

Per-record liveness rules:

1. `RING-WAL-RECLAIM-001` `new_collection(collection_id,
collection_type)` record:
live only if it is the basis decision at `D(c)` for a collection whose
logical head `H(c)` is `EmptyClean`; otherwise reclaimable.
2. `RING-WAL-RECLAIM-002` `head(collection_id = 0, collection_type =
wal, region_index)` record:
live only if startup would currently use it as the effective WAL-head
override for the current tail region. Any earlier such control record,
or any such record in a non-tail WAL region, is reclaimable once the
same effective WAL head is preserved by a later tail-local control
record or by the current tail region's `LogRegionPrologue`.
3. `RING-WAL-RECLAIM-003` `head(collection_id, collection_type,
region_index)` record for a user collection:
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
live only while required to maintain a valid private-log chain from
current head to current tail.
8. `RING-WAL-RECLAIM-008` `free_region(region_index,
append_tail_after)` record:
live while replay still needs it after the retained log-segment
prologue checkpoint to reconstruct the newest `append_tail` position
and dirty-range membership.
9. `RING-WAL-RECLAIM-009` `erase_free_region_span(count,
ready_boundary_after)` record:
live while replay still needs it after the retained prologue checkpoint
to reconstruct the newest `ready_boundary`.
10. `RING-WAL-RECLAIM-010` `allocate_region(region_index,
allocation_head_after)` record:
live while replay still needs it after the retained prologue checkpoint
to reconstruct the newest `allocation_head`, or while a storage-core
private allocation reservation is still needed to recover an incomplete
private-log rotation.
11. `RING-WAL-RECLAIM-011` Main-WAL transaction-control and inline
transaction-control records are live while startup replay still needs
them to import a committed range, prove rollback was decided, finish
committed or rolled-back cleanup, or keep a transaction-log range
referenced for garbage collection.
12. `RING-WAL-RECLAIM-012` `wal_recovery` record:
live only if replay still needs it to justify later valid WAL records
that appear after an ignored corrupt/torn span in that WAL region.
13. `RING-WAL-RECLAIM-013` Transaction-log records, including
`free_intent` and `rollback_allocation`, are live while any retained
main-WAL commit, rollback, finish record, open transaction descriptor,
or pending recovery descriptor points to a range containing them. They
become reclaimable only when the transaction log's live prefix advances
past them.

WAL-region reclaim preconditions:

1. `RING-WAL-RECLAIM-PRE-001` The candidate region MUST be the head of
the WAL.
2. `RING-WAL-RECLAIM-PRE-002` For every live record in the candidate,
an equivalent live state MUST already be represented durably outside
the candidate.
3. `RING-WAL-RECLAIM-PRE-003` After planned metadata updates, startup
replay MUST still be able to walk a valid WAL chain from head to tail.

WAL-region reclaim postconditions:

1. `RING-WAL-RECLAIM-POST-001` A collection's `H(c)`, `B(c)`, and live
post-basis updates MUST NOT depend on bytes in the reclaimed region.
2. `RING-WAL-RECLAIM-POST-002` The recovered free-space cursors MUST
match pre-reclaim allocator state.
3. `RING-WAL-RECLAIM-POST-003` The recovered storage-core private
allocation reservation, if any, MUST match pre-reclaim allocator state.
4. `RING-WAL-RECLAIM-POST-004` Transaction recovery state that replay
would continue MUST match pre-reclaim crash-recovery state.
5. `RING-WAL-RECLAIM-POST-005` Startup MUST recover the same effective
WAL head after reclaim as before reclaim, using the current tail
region's `LogRegionPrologue` plus the last valid tail-local
`head(collection_id = 0, collection_type = wal, region_index = ...)`
override, if any.
6. `RING-WAL-RECLAIM-POST-006` WAL chain integrity MUST remain valid
with no broken `link` path.
7. `RING-WAL-RECLAIM-POST-007` The reclaimed region MUST be appended to
the dirty range with `free_region` before it is eligible for erase
maintenance.
8. `RING-WAL-RECLAIM-POST-008` If reclaim allocates any replacement
private log regions, replay-visible `allocate_region` records for those
allocations carry the popped region and new `allocation_head`, and any
new `LogRegionPrologue` carries the free-space cursor checkpoint so
replay reconstructs the same allocator position.

Safety invariant:

1. `RING-WAL-RECLAIM-SAFE-001` Reclaim MUST NOT change replay result:
the recovered collection submachine state and pending updates for every
collection, the recovered free-space cursors, storage-core private
allocation reservation, and transaction recovery state after reclaim
must match the pre-reclaim logical state.

## Transaction-Log Reclaim Eligibility

Transaction-log reclaim is prefix-only for each transaction log. A
transaction-log region or record may be reclaimed only when no retained
main-WAL `commit_transaction(transaction_log_id, range)`,
`rollback_transaction(transaction_log_id, range)`, or
`transaction_finished(transaction_log_id, range)` record, no open
transaction descriptor, and no pending recovery descriptor points into
that prefix. The reclaim result must preserve the same imported
committed ranges, rollback ranges, free intents, rollback allocation
records, cleanup obligations, transaction-log append cursors, and
allocator cursor recovery result as before reclaim.

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
newly detached region to the dirty range of the storage-private
free-space collection and records that append with
`free_region(region_index, append_tail_after)`. The operation does not
erase the detached region and does not store allocator links in that
region.

Normative append semantics:

1. `RING-FREE-REGION-SEM-001` Let `tail_before` be the current
`append_tail` before freeing `region_index`.
2. `RING-FREE-REGION-SEM-002` The freed region is inserted at
`tail_before` as a dirty free-space entry.
3. `RING-FREE-REGION-SEM-003` `append_tail_after` MUST be the next
queue position after `tail_before`.
4. `RING-FREE-REGION-SEM-004` The free is complete when
`free_region(region_index, append_tail_after)` is durable.

Preconditions:

1. `RING-FREE-REGION-PRE-001` The region MUST no longer be reachable
from any live collection head, collection-defined region reference, WAL
chain, transaction-log chain, or storage-core private allocation
reservation before `free_region(region_index, append_tail_after)` is
appended.
2. `RING-FREE-REGION-PRE-002` The region MUST NOT already be present in
the free-space collection unless this procedure is being re-entered
during idempotent recovery.
3. `RING-FREE-REGION-PRE-003` The owning operation's committed or
rolled-back transaction state MUST contain enough durable information
for cleanup recovery to derive that `region_index` must be freed and to
derive its ordered cleanup slot.

Procedure:

1. `RING-FREE-REGION-001` Establish `region_index` as detached from all
live reachability. Do not erase it.
2. `RING-FREE-REGION-002` Ensure the current free-space metadata
frontier has room for one more dirty entry, materializing or
checkpointing a new `free_space_v2` metadata region if needed.
3. `RING-FREE-REGION-003` Append and sync
`free_region(region_index, append_tail_after)`.
4. `RING-FREE-REGION-004` Update runtime `append_tail =
append_tail_after`; the new entry lies in the dirty range
`[ready_boundary, append_tail)`.

Postconditions:

1. `RING-FREE-REGION-POST-001` The free-space FIFO order MUST be
preserved.
2. `RING-FREE-REGION-POST-002` Exactly one new dirty entry MUST be
appended.
3. `RING-FREE-REGION-POST-003` The freed region MUST NOT become
allocatable until it has been erased and that erase has been published
by `erase_free_region_span`.
4. `RING-FREE-REGION-POST-004` The free operation MUST be idempotent
across crashes before or after the append record becomes durable.

## Erase Maintenance

Erase maintenance moves a prefix of the dirty range into the ready
range. It is represented by `erase_free_region_span(count,
ready_boundary_after)`.

1. `RING-ERASE-FREE-001` The erased span MUST begin at the current
`ready_boundary` and contain exactly `count` dirty entries.
2. `RING-ERASE-FREE-002` Each named region in the span MUST be erased
before `erase_free_region_span(count, ready_boundary_after)` is made
durable.
3. `RING-ERASE-FREE-003` Erase does not require its own sync before the
WAL command. The durable publication point is the synced
`erase_free_region_span` record or equivalent materialized free-space
state.
4. `RING-ERASE-FREE-004` If power fails after one or more physical
erases but before the command is durable, replay leaves those entries in
the dirty range. Later recovery or maintenance may erase them again.
5. `RING-ERASE-FREE-005` If power fails after the command is durable,
replay advances `ready_boundary` to `ready_boundary_after`; the entries
are ready for allocation.
6. `RING-ERASE-FREE-006` Erase maintenance MUST preserve the configured
ready-region reserve for privileged storage-core operations whenever
ordinary user/data allocation is allowed to proceed.

## Transaction Cleanup Recovery

Full transaction cleanup has two retained lists:
transaction-private `free_intent` records, which become cleanup
obligations only after commit, and `rollback_allocation` records, which
become cleanup obligations after rollback. Cleanup is serialized by one
main-WAL cleanup owner. The owner records `cleanup_start_tail` when
`commit_transaction` or `rollback_transaction` is applied and processes
entry `cleanup_index` at queue slot
`cleanup_start_tail + cleanup_index`, represented by
`free_region(region_index, append_tail_after = cleanup_start_tail +
cleanup_index + 1)`.

1. `RING-TX-RECOVERY-001` If startup reaches main WAL end with an open
full transaction descriptor and no durable `commit_transaction`, it
MUST run rollback preparation for that transaction-log range by writing
any missing `rollback_allocation(region_index)` records for
transaction-owned allocations.
2. `RING-TX-RECOVERY-002` If startup reaches main WAL end with an
uncommitted inline transaction, it MUST run rollback recovery for that
bounded main-WAL range and may append
`rollback_inline_transaction(record_count)`.
3. `RING-TX-RECOVERY-003` After all transaction-owned allocations in an
uncommitted full transaction have durable `rollback_allocation` records,
startup or foreground rollback MUST append
`rollback_transaction(transaction_log_id, range)`. The durable rollback
marker makes the transaction range non-visible, records
`cleanup_start_tail = append_tail`, and transfers main-WAL cleanup
ownership to that transaction.
4. `RING-TX-RECOVERY-004` If startup reaches main WAL end after
`commit_transaction(transaction_log_id, range)` but before
`transaction_finished(transaction_log_id, range)`, it MUST preserve the
committed collection state imported from that range and finish ordered
cleanup frees for the range's `free_intent` records.
5. `RING-TX-RECOVERY-005` If startup reaches main WAL end after
`rollback_transaction(transaction_log_id, range)` but before
`transaction_finished(transaction_log_id, range)`, it MUST preserve the
range as non-visible and finish ordered cleanup frees for the range's
`rollback_allocation` records.
6. `RING-TX-RECOVERY-006` Cleanup recovery MUST be idempotent if
startup crashes before `transaction_finished` is durable. If the
expected cleanup slot is already present with the expected region,
recovery MUST advance to the next cleanup entry. If the expected slot is
absent, recovery MUST append it. If the slot is present with a different
region, recovery MUST report corruption.
7. `RING-TX-RECOVERY-007` While a transaction owns cleanup, no other
main-WAL operation may append records that affect the free-space
append-tail order, and erase maintenance MUST NOT advance
`ready_boundary` over the cleanup suffix. Transaction-log-only records
for already open transactions may continue.
8. `RING-TX-RECOVERY-008` On commit, `free_intent` records MUST be
removed from the enrolled collection's live state before their cleanup
`free_region` records are appended. On rollback,
`rollback_allocation` records MUST name transaction-owned allocations
that were never made collection-live.
9. `RING-TX-RECOVERY-009` `transaction_finished(transaction_log_id,
range)` MUST be appended only after the cleanup cursor has reached the
end of the committed free-intent list or rolled-back allocation list;
it releases cleanup ownership and clears retained transaction state.
10. `RING-TX-RECOVERY-010` The configured ready-region reserve and WAL
space reserve MUST leave enough capacity for startup recovery to append
required rollback allocation records, a rollback marker, ordered cleanup
free records, and the finish marker.

## Region Reclaim

These requirements preserve the cleanup obligations while the storage
implementation uses the free-space collection allocator.

1. `RING-REGION-RECLAIM-PRE-001` Transaction cleanup MUST make the
transaction begin marker durable before durable collection metadata
stops referencing regions that cleanup may free.
2. `RING-REGION-RECLAIM-PRE-002` After the committed collection-state
update, a region selected for cleanup MUST no longer be reachable from
any live collection head, WAL chain, transaction-log chain, or
storage-core private allocation reservation.
3. `RING-REGION-RECLAIM-PRE-003` A cleanup target MUST NOT already be
present in the free-space collection unless startup is re-entering
idempotent recovery.
4. `RING-REGION-RECLAIM-SEM-001` Cleanup MUST append each detached
region as a dirty entry with `free_region(region_index,
append_tail_after)`.
5. `RING-REGION-RECLAIM-SEM-002` Cleanup MUST NOT make a detached
region ready without erase maintenance publishing an
`erase_free_region_span` record or equivalent materialized
free-space state.
6. `RING-REGION-RECLAIM-ORDER-001` Transaction cleanup MUST not make a
committed free-intent region free until the committed collection state
no longer references that region. Rollback cleanup may free only regions
recorded as transaction-owned rollback allocations.
7. `RING-REGION-RECLAIM-ORDER-002` If cleanup needs a new free-space
metadata region to append dirty entries, it MUST allocate that metadata
region through a transaction or privileged storage-core operation whose
recovery path is already reserved.
8. `RING-REGION-RECLAIM-ORDER-003` Cleanup is not complete for a
detached region until its ordered `free_region(region_index,
append_tail_after)` record is durable.
9. `RING-REGION-RECLAIM-ORDER-004` Transaction cleanup MUST be
idempotent across crashes between any two cleanup steps.
10. `RING-REGION-RECLAIM-POST-001` The free-space FIFO order MUST
remain valid after cleanup.
11. `RING-REGION-RECLAIM-POST-002` Cleanup MUST append exactly one
dirty free-space entry for each detached region.
12. `RING-REGION-RECLAIM-POST-003` Replay of the cleanup WAL prefix
MUST reconstruct the same `append_tail`, `ready_boundary`, and
`allocation_head` values as foreground cleanup.
