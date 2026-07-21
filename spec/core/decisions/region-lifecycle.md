# Region Lifecycle Decisions

Relational ownership, exact-once accounting, and logical-region identity.

These records preserve reviewed decisions moved from the active
[design queue](../todo.md). Later decisions may explicitly supersede an earlier
record.

## D17 — Relational ownership vocabulary

Agree the general meanings of
Ready Free, Dirty Free, Transaction Owned, user/internal Collection Owned,
retention, and transaction-region retention. The follow-up patch is limited
to definitions and a short lifecycle summary.

Decision: Ready Free and Dirty Free come from a region's free-list position.
A region named by a durable transaction allocation entry is Transaction Owned
until a committed collection basis reaches it or cleanup returns it to the
free list. A region detached by a committed free intent is Transaction Owned
until cleanup returns it to the free list. For a user collection, Collection
Owned comes from collection-defined reachability, which core assumes. For an
internal collection, Collection Owned comes from core-defined retained bases.
Retention prevents reclamation while recovery may still need a region.

A transaction region remains retained while any retained WAL record refers to
it. D27 and D32 define the exact reference and reclamation mechanics.

Rationale: These terms describe relationships derived from durable structures,
not values in a per-region state table. Separating retention from ownership
also lets core fully define its recovery obligations without interpreting a
user collection's reachability rules.

Patch scope: Change only the ownership definitions and short lifecycle summary
in section 7 of `000-system-narrative.md`, the matching vocabulary entries,
and the directly conflicting statement that transaction logs are a third
internal collection. Do not define exclusivity or completeness invariants,
transaction-log continuation or release mechanics, publication ordering, or
recovery algorithms.

Verification: Confirm every term is derived from an authoritative durable
structure, user reachability remains a collection contract, internal
retention remains core-defined, and transaction-region retention follows
retained WAL references. Run Markdown and diff checks, and leave D17
unchecked until this bounded patch has been reviewed.

## D18 — Relational safety invariants

Agree the disjointness,
completeness, and reachability predicates that replace a per-region state
table, including free-list backing, cleanup obligations, transaction
allocations, and foreground/replay equivalence. The follow-up patch states
invariants only; implementation and model replacement remain separate work.

Decision: Every region must be accounted for exactly once: it is either Ready
Free, Dirty Free, owned by one transaction, or owned by one collection. A
region must never be used in more than one of these ways, and no region may be
left unaccounted for. Free-list backing regions are owned by the Region Free
List. A transaction owns an allocated or detached region until a collection
takes ownership or cleanup returns it to the free list. Core preserves this
invariant through its operations and replay; user collections preserve it
through their reachability contracts. Foreground operation and recovery
replay derive the same accounting from the same durable records.

Rationale: Exact-once accounting states the no-double-use and no-leak rules
without introducing a per-region state table. These are protocol obligations,
not a promise that recovery can discover every violation: core does not
interpret user-collection reachability and cannot generally detect a region
assigned by two opaque collections or prove that an arbitrary region is
unaccounted for by scanning its bytes.

Patch scope: Add only the agreed invariant and its immediate explanation to
section 7 of `000-system-narrative.md`. Do not add a per-region table, require
recovery to detect violations, define stale-link validation, change recovery
mechanics, or replace implementation or model state.

Verification: Review every lifecycle transition described in the narrative
against exact-once accounting, including ordinary allocation, commit,
rollback, cleanup, free-list backing growth and retirement, and foreground
and replay interpretation. Run Markdown and diff checks, and leave D18
unchecked until that bounded patch has been reviewed.

## D19 — Region-incarnation and stale-link validation

Decide what
structure-specific generation, nonce, preamble, or other evidence proves that
a retained link names the intended reuse of a physical region. The follow-up
patch defines the safety requirement without choosing unrelated header fields.

Decision: A logical region is a region index paired with the global
allocation sequence assigned when that free-list entry is consumed. Region
initialization records the allocation sequence in the region header. Every
durable reference used to interpret region contents names a logical region
and is valid only when the target has a valid header with the expected
allocation sequence, collection, and format. An invalid reference is not
followed or used as reachability evidence. Physical-accounting records that
allocate, free, erase, or clean up a region without interpreting its contents
may identify the byte range by region index alone.

Rationale: A region index can be erased and reused, so it cannot distinguish
the target intended by an old reference. The already-required global
allocation sequence never repeats and therefore identifies that use without
adding another generation or nonce. Assigning the value during allocation
also avoids the superseded D12 assumption that region initialization order is
globally monotonic when transactions may initialize private regions in a
different order.

Patch scope: Replace the conflicting region-initialization sequence text and
add the logical-region validation rule in section 2 of
`000-system-narrative.md`; add the matching vocabulary entry and clarify that
a collection head names a logical region in `001-vocabulary.md`. Do not define
exact reference codecs, free-list sequence-counter transitions, WAL sequence
mechanics, or component-specific error and recovery behavior.

Verification: Confirm that every content-bearing reference distinguishes a
reuse of its physical index, validation checks sequence, collection, and
format before interpretation, and index-only accounting records do not gain
an unnecessary content-validation requirement. Run Markdown and diff checks,
and leave D19 unchecked until that bounded patch has been reviewed.

## D19A — Allocation-sequence propagation

Define the free list's next
allocation-sequence state and its exhaustion rule. Every durable command that
defines `allocation_head_after` must also define
`allocation_sequence_after`; every allocation result, reserved successor, or
other holder of a preallocated region must retain the logical-region pair;
and region initialization must copy that allocation sequence into the header.
The follow-up patch aligns only the allocator narrative and vocabulary. It
does not change the independent WAL sequence namespace or its prologue-based
mechanics.

Decision: The free-list allocation state consists of the allocation cursor
and the next allocation sequence. A command that consumes the entry at the
cursor records the allocated logical region, `allocation_head_after`, and
`allocation_sequence_after`. The logical region uses the current next
allocation sequence, and the sequence after is that value plus one. The
command becomes durable before runtime applies either after-value. Every
allocation result or retained preallocation stores the logical region, and
region initialization copies its allocation sequence into the header.
Cleanup and free-list append commands do not advance the allocation sequence.

If the next allocation sequence cannot advance, allocation returns
`SequenceExhausted` before consuming the entry or issuing media I/O. Replay
selects the retained allocation-consuming command with the greatest valid
`allocation_sequence_after` and restores both after-values from that command.

Rationale: Assigning the incarnation when the free-list entry is consumed
gives private and preallocated regions a stable identity before they are
initialized or published. Recording both after-values makes cursor and
sequence advancement one durable transition and gives foreground operation
and replay the same allocator state.

Patch scope: Align the allocation, replay, free-list successor, and transaction
entry summaries in `000-system-narrative.md`, and update only the allocation
sequence and transaction allocation entry definitions in
`001-vocabulary.md`. Do not change exact codecs, WAL sequence mechanics,
component error types, implementation, or models.

Verification: Review ordinary and free-list-internal allocation, replay,
preallocated-region retention, initialization, cleanup, and exhaustion
against the decision. Run Markdown and diff checks, and leave D19A unchecked
until that bounded patch has been reviewed.

## D20 — Publication and runtime-apply rule

Agree the ordinary ordering for preparing immutable bytes, syncing them,
publishing reachability, applying the runtime transition, retaining the old
representation, and treating an unpublished target after a crash. Separately
define a durable claim or retention fact that may create a recoverable
initialization obligation before target bytes are usable, as required by
free-list-internal growth. The follow-up patch adds only these shared rules for
later mechanical chapters.

Decision: A region materialization becomes live only through a durable WAL
record written after the complete region has been written and synced. Runtime
applies the publication only after that record is durable. Recovery ignores a
region without its publishing record even when its bytes appear complete, and
the previous materialization remains live until its replacement is published.

A collection materializes a preallocated region only through a transaction.
Before writing the region, the transaction writes and syncs a materialization
intent in its immediately durable allocation area. The intent identifies the
collection and logical region but does not make the region live. Commit is the
successful outcome and is allowed only after every intended materialization is
completely written and synced. Rollback, including recovery of an undecided
transaction, is the failed outcome. Foreground processing and replay report the
same outcome to the collection.

A failed intent leaves a collection-owned preallocated logical region requiring
erase. It must be successfully erased before another write or use as a completed
materialization. A region allocated privately by the same transaction and not
named by a committed reference instead returns to the dirty free-list tail
through rollback cleanup. Free-list materialization does not use a transaction:
its durable successor-allocation command establishes the obligation, its
durable tail advance publishes completion, and an unmatched obligation requires
erase before retry.

Rationale: The transaction decision supplies the materialization outcome, so no
checksum or separate completion record is needed. Recording intent before the
first region write lets recovery conservatively identify a target that may have
been partially programmed. A committed reference fixes the logical region, so
failed preallocated materialization must retry that region rather than return it
to the free list.

Patch scope: Add only the shared publication rule, transactional preallocated-
materialization rule, and free-list correspondence to
`000-system-narrative.md`; add the deferred wear concern to the backlog. Do not
define exact record codecs, transaction-area layout, notification APIs, retry
scheduling, WAL sequence mechanics, implementation, or models.

Verification: Review publication crash cuts, commit and rollback outcomes,
committed and private target disposition, erase-before-retry, and the existing
free-list interrupted-materialization path. Run Markdown and diff checks.
