# Journal

This journal captures design ideas, motivation, and decision history before they
are ready to become normative specification text.

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
