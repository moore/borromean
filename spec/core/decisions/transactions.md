# Transaction Decisions

Transaction visibility, lifecycle, and recovery decisions made while finishing
the system narrative.

These records preserve reviewed decisions moved from the active
[design queue](../todo.md). Later decisions may explicitly supersede an earlier
record.

## D22 — Transaction committed and private views

Agree what ordinary readers see during a transaction, how transaction-aware
reads overlay private updates, and the atomic visibility change produced by
commit. The follow-up patch changes only transaction read/visibility semantics.

Decision: Transaction operations remain private until commit. Ordinary reads
continue to see committed collection state and never see an open transaction's
private operations. A read through a transaction sees its committed collection
view with its own private operations applied in order and does not see another
transaction's private operations.

A durable commit publishes all of the transaction's collection operations
together. Runtime applies them only after the commit record is durable, and
replay applies the same operations from that record. Rollback discards the
private operations, so they never become visible outside the transaction.

Rationale: One committed view for ordinary readers prevents partial visibility
while a transaction is being prepared. Applying private operations to
transaction reads gives read-your-writes behavior without exposing those
operations globally. Using the durable commit as the single visibility point
gives foreground operation and recovery the same atomic outcome.

Patch scope: Add only the transaction-view and commit-visibility summary to
section 6 of `000-system-narrative.md`. Do not decide enrollment timing,
committed-basis capture, conflict detection, exact read APIs, borrowing,
implementation, or models.

Verification: Review ordinary, transaction-aware, commit, rollback, and replay
views, including a transaction spanning multiple collections. Run Markdown and
diff checks.

## D23 — Transaction enrollment and mutation serialization

Starting from shared-read/exclusive-mutation top-level access, agree collection
enrollment, generation validation, competing-writer rejection, bounded
simultaneous open transactions, and which work may occur between exclusive
mutating calls. The follow-up patch changes only concurrency rules and does not
choose exact Rust borrowing or transaction-handle types.

Decision: Before a transaction reads or changes a collection, it enrolls that
collection. Enrollment captures the collection's committed view and generation
under shared top-level access. A collection may be enrolled only once in one
transaction; a second attempt fails without changing the transaction. Different
transactions may enroll the same collection without holding a collection lock
until commit.

Commit obtains exclusive top-level access and validates every modified
collection's current generation against the generation captured at enrollment
before writing its commit record. If any generation differs, commit reports a
conflict without writing a commit record or publishing any private operation. A
successful commit advances every modified collection's generation.

The format fixes a maximum number of open transactions and a maximum number of
collections enrolled in one transaction. Beginning or enrolling at the
applicable limit fails without changing durable or transaction state. Runtime
and recovery need at most the product of those limits in transaction-collection
view buffers. Beginning a transaction, consuming a free-list allocation,
committing, rolling back, cleanup, and finish require exclusive top-level
access. Enrollment capture uses shared access. Private operations and
transaction-log preparation may proceed between those calls without retaining
top-level access, so commit must perform generation revalidation.

Rationale: Optimistic enrollment permits long-running and competing
transactions without holding collection locks. Generation validation under
exclusive access gives one competing writer the committed generation and
prevents a stale transaction from publishing changes based on an older view.
Separate transaction and per-transaction collection limits give runtime and
recovery an explicit bound on transaction-collection view buffers without
treating a transaction-log layout detail as the semantic bound.

Patch scope: Add only enrollment, generation-conflict, transaction-capacity,
and serialization summaries to section 6 of `000-system-narrative.md`. Do not
choose exact Rust handles or borrowing, generation width or exhaustion,
post-conflict transaction lifecycle, transaction-segment mechanics, exact error
types, implementation, or models.

Verification: Review duplicate-enrollment rejection, competing enrollment, a
multi-collection commit with one stale generation, both capacity limits and
their product bound, and every listed shared or exclusive access boundary. Run
Markdown and diff checks.
