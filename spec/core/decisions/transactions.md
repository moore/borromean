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
