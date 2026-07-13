# Multi-Collection Transactions

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose and motivation

Transactions allow one logical change to span several independently typed
collections without exposing a partially published result. They must preserve
the inexpensive ordinary read path: creating a transaction must not make
committed data unavailable to readers or force every reader through an OS lock.

V3 supports multiple simultaneously open transactions up to a configured
fixed-capacity limit. Each may enroll several collections and publishes those
collections with one durable decision. Enrollment remains exclusive: two open
transactions cannot enroll the same collection for writing.

The WAL also has one transaction-finish lock. A durable commit or rollback
decision acquires that lock, and the matching durable `finish` record releases
it. Open transactions may continue to exist while the lock is held, but no
other transaction may enter the decision-to-finish interval. Consequently
replay may find many open transactions with no decision, but at most one
transaction with a commit or rollback decision and no finish record.

## Mental model

The transaction registry tracks reader-compatible write reservations.
Enrollment prevents another transaction or ordinary writer from changing an
enrolled collection's committed generation, while ordinary readers continue to
read the last committed roots. There is no waiting mutex at the storage
interface: a conflicting enrollment or write returns immediately, and
unrelated collections continue to operate normally while no
decision-to-finish WAL lock is held. While that lock is held, committed reads
remain available but normal WAL-backed writes return finish pressure before
I/O.

Caller-owned fixed-capacity transaction memory contains:

- transaction identity and lifecycle state;
- enrolled collection identities and their expected committed generations;
- private roots or ordered overlays containing transaction writes;
- the proposed committed generation for each enrolled collection; and
- ordered cleanup obligations plus the next-cleanup cursor.

The committed view and private view are distinct. An ordinary reader uses the
committed memory frontier as the collection root when one is resident. Without
a resident frontier, it uses whichever is later in WAL order: the newest
snapshot or newest collection `head` record. A transaction-aware reader starts
from that same committed root and overlays the transaction's ordered private
updates, providing read-your-writes without modifying public roots.

### Registry and visibility requirements

1. `CORE-TX-001` Enrollment MUST acquire a reader-compatible write reservation
   for the collection.
2. `CORE-TX-002` Ordinary readers of an enrolled collection MUST continue to
   observe its last committed view.
3. `CORE-TX-003` Transaction-aware readers MUST observe the committed view plus
   their transaction's ordered private updates.
4. `CORE-TX-004` A competing ordinary write to an enrolled collection MUST
   return `CollectionWriteLocked` before any raw-device operation.
5. `CORE-TX-005` Reads and writes to unenrolled collections MUST remain allowed
   while no decision-to-finish WAL lock is held; committed reads MUST remain
   allowed while that lock is held.
6. `CORE-TX-015` Multiple transactions MAY be open concurrently up to a
   configured fixed-capacity limit, but no collection MAY be write-enrolled by
   more than one transaction.
7. `CORE-TX-018` A collection write-enrolled by one open transaction MUST reject
   enrollment or mutation by another transaction with `CollectionWriteLocked`
   before any raw-device operation.
8. `CORE-TX-019` While the decision-to-finish WAL lock is held, normal
   WAL-backed writes MUST return transaction-finish pressure before any
   raw-device operation; transaction finish/cleanup operations are the
   permitted WAL writers.

## Mechanical operation

**Begin and enroll.** Beginning initializes empty caller-owned transaction
state. Enrollment records the collection's current committed generation and
acquires its logical write reservation without media I/O. Re-enrollment is
idempotent only when it names the same expected state. A conflicting ordinary
write returns `CollectionWriteLocked` before any device operation.

**Private write.** A transaction write allocates and publishes transaction-log
or private-data regions owned by the transaction. It advances only the private
root or overlay. Committed collection heads, indexes, manifests, and caches
remain unchanged, so ordinary readers see a coherent old view.

Transaction allocation entries are a deliberate exception to treating an
uncommitted private log as invisible. Each entry independently records and
syncs the allocated region, purpose, global allocation sequence, and
`allocation_head_after`. It becomes part of allocator recovery immediately,
because the queue entry has been consumed whether the transaction later commits
or rolls back. Transaction logs may append independently, so recovery uses the
allocation sequence—not transaction begin order, commit order, or physical log
position—to reconstruct their common allocation order.

### Private-write and allocation requirements

1. `CORE-TX-006` Transaction writes MUST not mutate committed heads, indexes,
   caches, or manifests before commit.
2. `CORE-TX-020` The allocator-head effect of a durable transaction allocation
   entry MUST remain visible through that entry or a later allocator checkpoint
   regardless of private-payload sealing or transaction outcome. Its ownership
   evidence MUST remain retained, and an uncommitted allocation MUST remain
   transaction-owned until ordered rollback cleanup frees it.

**Commit.** Commit first validates every expected committed generation,
capacity bound, encoded decision size, and runtime apply precondition. It then
appends one decision batch listing all enrolled old and new generations/roots
and performs exactly one sync. After that decision is durable, the same atomic
transition used by replay installs all new committed views in runtime. There
is no state in which only a subset is visible. Cleanup is not part of commit's
foreground I/O budget.

### Commit requirements

1. `CORE-TX-007` One synced commit decision MUST atomically publish every
   enrolled collection.
2. `CORE-TX-008` Foreground apply and replay MUST expose either the complete old
   view or the complete committed view, never a mixed collection state.

**Rollback.** Rollback preflights its decision and cleanup representation,
appends one rollback decision batch, and syncs once. The decision leaves every
committed view unchanged, removes the private view from transaction visibility,
establishes ordered cleanup obligations, and acquires the decision-to-finish
WAL lock. Cleanup and the final finish record remain explicit work. Dropping
transaction memory alone performs no I/O and is not an implicit rollback
protocol.

### Rollback requirements

1. `CORE-TX-009` One synced rollback decision MUST remove the transaction's
   private view without changing any committed view and MUST durably establish
   its ordered cleanup obligations.
2. `CORE-TX-010` Commit and rollback decisions MUST not synchronously perform
   cleanup or an ownership search.
3. `CORE-TX-012` Dropping caller transaction memory MUST perform no implicit
   I/O; explicit rollback or recovery owns completion.

**Cleanup.** Commit and rollback can both leave obsolete regions. Their
obligations are serialized in execution order with a durable cursor. Each
bounded cleanup step processes the next obligation idempotently, publishes any
free fact, advances the cursor durably, and can be retried after a crash without
searching collection payloads.

### Cleanup requirement

1. `CORE-TX-011` Cleanup MUST be ordered, cursor-addressed, idempotent, and
   resumable after each durable free.

## Crash interpretation

WAL replay reconstructs historical transaction decisions and durable progress.
It may yield many open transactions with no decision and at most one
decided-but-unfinished transaction because the finish lock prevents a second
decision interval. Recovery first resolves that optional decided transaction:

- with a commit decision, atomically apply every listed collection transition,
  resume post-commit cleanup, and append the finish record;
- with a rollback decision, resume rollback cleanup and append the finish
  record.

Only after that finish releases the WAL lock does recovery process the remaining
open transactions. It rolls them back one at a time in their durable begin
order: append the rollback decision, clean up private state, append the finish
record, release the lock, and continue to the next open transaction. Open does
not expose normal WAL mutation until all recovery rollbacks are complete.

### Recovery requirements

1. `CORE-TX-013` WAL replay MAY yield any number of undecided open transactions
   up to the configured capacity, but MUST yield at most one transaction with a
   durable commit or rollback decision and no finish record.
2. `CORE-TX-014` A durable commit or rollback decision MUST hold the
   transaction-finish WAL lock until the matching durable finish record, and
   another transaction MUST NOT enter that interval while the lock is held.
3. `CORE-TX-016` Recovery MUST finish the one possible decided transaction
   before rolling back every undecided open transaction one at a time in durable
   begin order.
4. `CORE-TX-017` WAL replay MUST report corruption if retained transaction
   records imply overlapping decision-to-finish intervals or exceed the
   configured open-transaction capacity.
