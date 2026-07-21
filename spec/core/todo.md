# Core specification design TODOs

This file records unresolved design and specification work discovered while
reviewing `000-system-narrative.md` and `001-vocabulary.md`. Agreed decisions
belong in the specification and are indexed in [decisions.md](decisions.md).
This file contains only work that still needs to be resolved or propagated.

## Incremental review protocol

This queue is intentionally designed for small changes. Unless we explicitly
agree otherwise, work proceeds as follows:

1. Select only the first unchecked discussion item.
2. Discuss the design without editing the specification, models, or Rust code.
3. Record the agreed decision under that item, including any disagreement or
   deliberately deferred question.
4. Agree on one bounded patch and the exact files it may touch.
5. Apply and review that patch, run only the relevant checks, and then mark the
   item complete.
6. Move the completed decision record to its feature file under `decisions/`
   and add it to the decision index.

An item is complete only when its decision is recorded and its agreed patch has
been reviewed. Recording the decision in this TODO is allowed during the
discussion; it does not authorize changes elsewhere. Do not batch later items
into the current patch merely because they are related. If resolving one item
uncovers another decision, add a new queue item instead.

Before discussion begins, split any item that still contains independently
disputable decisions or would require more than one conceptual subsection or
one implementation slice. Keeping the queue fine-grained takes precedence over
preserving its current numbering.

When a discussion reaches agreement, add this short record beneath its queue
item before editing another file:

```text
Decision: <the agreed rule>
Rationale: <why, including rejected alternatives when useful>
Patch scope: <exact files and the one bounded change allowed>
Verification: <review or targeted checks required before completion>
```

Previously agreed design decisions recorded in [decisions.md](decisions.md)
are not reopened by this queue unless a later contradiction requires it. In
particular, the logical `FlashIo` semantics, relational rather than table-based
ownership, transaction-only cross-owner transfers, free-list-internal
representation movement, and shared-read/exclusive-mutation access to the
top-level storage object remain the working design. A top-level mutation of
shared state is non-reentrant, while internal subsystem operations may compose
under the same exclusive access without subsystem locks. Transaction-private
preparation may occur without entering the top-level storage object.

## Ordered design discussion queue

- [ ] **D20 — Publication and runtime-apply rule.** Agree the ordinary ordering
  for preparing immutable bytes, syncing them, publishing reachability, applying
  the runtime transition, retaining the old representation, and treating an
  unpublished target after a crash. Separately define a durable claim or
  retention fact that may create a recoverable initialization obligation before
  target bytes are usable, as required by free-list-internal growth. The
  follow-up patch adds only these shared rules for later mechanical chapters.
- [ ] **D21 — Main-WAL root and retained-boundary model.** Agree the main WAL's
  role as database root, head/tail meanings, WAL sequence namespace, tail
  selection, and the facts replay must retain. The follow-up patch adds only the
  conceptual main-WAL chapter.
- [ ] **D22 — Transaction committed and private views.** Agree what ordinary
  readers see during a transaction, how transaction-aware reads overlay private
  updates, and the atomic visibility change produced by commit. The follow-up
  patch changes only transaction read/visibility semantics.
- [ ] **D23 — Transaction enrollment and mutation serialization.** Starting
  from shared-read/exclusive-mutation top-level access, agree collection
  enrollment, generation validation, competing-writer rejection, bounded
  simultaneous open transactions, and which work may occur between exclusive
  mutating calls. The follow-up patch changes only concurrency rules and does
  not choose exact Rust borrowing or transaction-handle types.
- [ ] **D24 — Free-list durable representation abstraction.** Agree how a basis,
  materialized backing regions, WAL-resident tail, and the allocation/ready/
  append cursors together represent one logical FIFO. The follow-up patch
  changes only the abstract free-list representation.
- [ ] **D25 — Transaction decision semantics.** Agree begin, sealed private
  range, atomic main-WAL commit import, rollback interpretation, free-intent
  visibility, and the preconditions revalidated before a decision. The follow-up
  patch excludes cleanup and segment continuation.
- [ ] **D26 — Transaction-segment layout and sealing.** Agree allocation,
  collection-operation, and free-intent areas; framing versus bounded encoding;
  exact segment bounds; and the seal that commit imports. The follow-up patch is
  limited to one segment format.
- [ ] **D27 — Transaction-log continuation and retention.** Agree reserved
  continuation capacity, successor allocation/link ordering, reuse of a region
  for multiple segments, and the final retained WAL reference that permits log-
  region reclamation. The follow-up patch excludes format bootstrap.
- [ ] **D28 — Free-list tail growth.** Preserve the collection-local ownership
  rule and the agreed reserved-successor/materialize/tail-advance sequence, then
  define the commands' exact cursor and sequence fields, replay comparisons,
  admission reserves, I/O-error results, and crash cuts. The follow-up patch is
  limited to tail growth.
- [ ] **D29 — Free-list backing retirement.** Agree the atomic
  unlink-and-append- dirty command, its cursor fields, validation, and crash
  cuts. The follow-up patch is limited to retirement of one obsolete backing
  region.
- [ ] **D30 — Erase maintenance completion and failure.** Preserve the agreed
  bounded prefix and stop-on-erase-error behavior, correct a zero selected count
  to success with no I/O, and decide readiness-record write/sync error handling.
  The follow-up patch is limited to the erase-maintenance operation.
- [ ] **D31 — Allocator facts, checkpoints, and replay.** Replace “take the
  largest allocation record” with an agreed basis/preamble checkpoint, ordered
  validation of all later transaction and free-list-internal allocation facts,
  duplicate/gap/FIFO rejection, retention, and exhaustion behavior. The follow-
  up patch is limited to allocator recovery semantics.
- [ ] **D32 — Cleanup and finish boundary.** Under exclusive top-level mutation,
  decide one-call versus resumable cleanup, durable cursor and idempotence,
  transaction-log cleanup, Drop behavior, finish publication, admission of
  later mutations after interruption, and the response to cleanup I/O failure.
  The follow-up patch excludes the commit/rollback decision already covered by
  D25.
- [ ] **D33 — WAL record framing and torn-tail continuation.** Agree separator,
  escaping, checksum, granule padding, candidate discovery, corrupt versus torn
  interpretation, and any recovery-boundary command. The follow-up patch changes
  only append framing and tail scanning.
- [ ] **D34 — Main-WAL successor allocation and rotation.** Agree control
  reserve, transaction-only successor allocation, link and target-preamble
  ordering, tail switch, allocator checkpoint capture, and rotation crash cuts.
  The follow-up patch excludes head reclaim.
- [ ] **D35 — Main-WAL head advancement and reclaim.** Agree how a new retained
  head is published, how collection, allocator, transaction, and cleanup facts
  are preserved, and when excluded WAL regions become reclaimable. The follow-up
  patch is limited to reclaim and its crash cuts.
- [ ] **D36 — Recursive progress and admission reserves.** Derive the ready
  regions and control bytes required for every admitted operation to extend the
  main WAL, transaction log, and free list and still roll back, clean up, and
  finish. Account for every simultaneously open transaction. The follow-up patch
  states the capacity/progress contract before any numeric implementation.
- [ ] **D13 — Collection catalog and lifecycle.** Agree stable collection IDs
  and type identities, bounded catalog capacity, create/open/drop behavior,
  whether IDs can be reused, dropped tombstones, and generation/conflict state.
  The follow-up patch is limited to the collections chapter.
- [ ] **D14 — Collection basis, retained deltas, and WAL rereads.** Replace the
  three exclusive residence states with an agreed basis-plus-later-WAL-deltas
  model and decide when retained WAL snapshots and updates may be reread. The
  follow-up patch replaces only the semantic residency section; cache allocation
  and scaling remain D39.

  Agreed premise from D04A: Each operation's effect has one authoritative
  representation. The newest valid snapshot or head record establishes the
  current collection root and supersedes earlier representations for that
  collection. Only operation records later than that root remain separately
  represented. Operation records are read from the WAL only during startup
  replay; before their reconstructed RAM effects can be discarded, a snapshot
  or region materialization must incorporate them. D14 still owns the complete
  basis model and snapshot reread rules.
- [ ] **D15 — Snapshot and materialization equivalence.** Agree what complete
  logical state a WAL snapshot represents, how later deltas apply, and the read-
  result equivalence required of an immutable collection materialization. The
  follow-up patch changes only representation semantics.
- [ ] **D16 — Collection reachability and traversal contract.** Agree committed
  reachability for possibly multi-region structures, deterministic traversal
  bounds and cycle errors, flush/compaction publication hooks, and reclaim
  enumeration. The follow-up patch changes only the abstract collection/core
  integration contract.
- [ ] **D37 — Reader/writer access and fail-stop behavior.** Starting from the
  agreed shared-read/exclusive-mutation invariant, classify top-level
  operations, define internal call composition and required revalidation, and
  decide which access remains admissible after ambiguous I/O failure or an
  interrupted mutation. The follow-up patch is limited to the shared access and
  fail-stop contract, not exact Rust signatures.
- [ ] **D38 — Format bootstrap publication.** Using the already agreed format,
  ownership, WAL, transaction-log, free-list, continuation, and progress rules,
  specify construction of the initial carriers, metadata-last publication, and
  every interrupted-format outcome. The follow-up patch changes only format.
- [ ] **D39 — Bounded runtime-memory model.** Agree which state is proportional
  to region count, collection capacity, transaction capacity, retained WAL
  frontier, and collection frontier slots; decide what is materialized, cached,
  or read on demand. The follow-up patch defines memory ownership and scaling,
  not exact private Rust fields. Use the
  [frontier-capacity design question](design-questions/frontier-capacity-preflight.md)
  as discussion input and feed the result back into D14 and D41.
- [ ] **D40 — Public execution surface and resumability.** Agree blocking versus
  caller-driven step/future APIs, safe interruption, how open transactions are
  represented between calls without retaining top-level storage access, and no
  implicit I/O on Drop. The follow-up patch defines behavior only; exact Rust
  borrowing and signatures are separate mechanical-design work.
- [ ] **D41 — Result, error, and pressure model.** Agree backend-error
  propagation; typed geometry, corruption, capacity, conflict, and
  ambiguous-I/O failures; preflight rejection before I/O; and
  success-with-maintenance-pressure reporting. The follow-up patch changes
  only result semantics.
- [ ] **D42 — Maintenance task inventory and budgets.** Agree the explicit tasks
  for erase, snapshot/materialization, WAL rotation/reclaim, free-list work, and
  transaction cleanup; their budgets; remaining-work signaling; and which work
  foreground calls must never hide. The follow-up patch changes only maintenance
  API semantics.
- [ ] **D43 — Startup discovery pass.** Agree metadata validation, exactly one
  fixed-header read per region, candidate indexing, WAL-preamble validation,
  duplicate-sequence handling, and the prohibition on a second whole-device
  scan. The follow-up patch is limited to discovery.
- [ ] **D44 — Replay order and corruption policy.** Agree selected head-to-tail
  traversal, root and basis selection, allocator-fact ordering, retained-object
  validation, bounded/cyclic traversal failures, and the rule against guessing
  through ambiguity. The follow-up patch excludes recovery writes.
- [ ] **D45 — Recovery completion before open.** Agree completion of a decided
  transaction, rollback order for undecided transactions, unfinished WAL/free-
  list operations, recovery capacity, idempotence, and the point at which normal
  operations become available. The follow-up patch is limited to recovery work.
- [ ] **D46 — Relational models and crash evidence.** Agree replacements for the
  ownership pilot, focused WAL/allocator/transaction/free-list models,
  refinement bridges to Rust transitions, and the required
  failure-before/after-write, sync, and erase scenarios. The follow-up patch
  creates the verification plan; each model or code change receives its own
  later item.
- [ ] **D47 — Generate the post-design migration queue.** Once the design
  decisions are stable, derive ordered, independently reviewable items for each
  normative chapter patch, exact Rust API and private-state slice, model change,
  adapter update, and targeted test. Generating this queue does not authorize
  executing more than its first unchecked item.
- [ ] **D48 — Final semantic preservation and refinement audit.** Once the v3
  component and composition chapters are stable, extract candidate obligations
  from the current specification, public behavior, hardening tests, models, and
  relevant defensive checks. Compare semantic outcomes rather than old APIs or
  mechanisms, one subsystem at a time. Classify each candidate as preserved,
  replaced by different or stronger semantics, intentionally removed with a
  rationale, or accidentally missing. Every retained behavior must have a v3
  requirement and suitable evidence before v3 supersedes the current design.

## Detailed implementation and propagation backlog

The items below retain the detailed concerns already discovered. They are not an
independent work queue: each is gated by the applicable `Dxx` discussion above,
and none should be implemented merely because it appears earlier in this file.

### Post-v3 traceability tooling (D02A-D02B)

These tasks are deliberately gated until a working v3 exists, and may be done
later if other work remains more valuable.

- [ ] Design purpose-built requirement-tracing tooling that distinguishes
  executable evidence from navigational citations and matches Borromean's
  acceptance model.
- [ ] Decide whether complete normalized requirement-and-verification
  quotations remain the canonical evidence identity, then enforce the chosen
  rule without relying on Duvet's partial-text matching.
- [ ] Register model evidence by running a named model invocation and
  associating its checked invariant or temporal property with the requirement
  it verifies.

### Recursive internal allocation (D27, D32, D34, D36, D38)

- [ ] Specify transaction-log continuation completely. Every writable segment
  likely needs reserved control space for one successor allocation and link,
  but the exact write ordering, crash cuts, bootstrap carrier for each fixed
  transaction slot, and later WAL-reference-based reclamation still need to be
  defined.
- [ ] Derive admission reserves for transaction-log control bytes, main-WAL
  commit/rollback/finish records, and ready regions. Account for every
  simultaneously open transaction and for the free-list collection's local
  tail-growth command so admitted work can always stop safely or roll back.
- [ ] State the formatting exception that creates the first main WAL,
  free-queue representation, and transaction-log carriers before transactional
  allocation is available.
- [ ] Resolve main-WAL self-growth under the transaction-only allocation rule.
  Commit, rollback, cleanup, and finish must not discover that WAL rotation
  requires a transaction after the current transaction can no longer grow.

### Free-list collection chapter (D28-D30)

- [ ] Incorporate the agreed free-list self-growth rule into the chapter that
  defines the free-list collection. Moving the region at the allocation cursor
  into the free-list backing structure does not change owners, so it is a
  free-list-internal command rather than a transaction or privileged allocator
  exception.
- [ ] Define the exact free-list-internal successor-allocation command. It
  consumes the region at the allocation cursor as reserved successor `n+1`,
  advances allocator state without changing owners, and carries the global
  allocation sequence and `allocation_head_after` needed to order it with
  transaction-log allocations during replay.
- [ ] Define the exact materialization and tail-advance protocol. Free-list
  appends update the WAL and in-memory frontier rather than a materialized
  region. After reserving `n+1`, write and sync the frontier into already
  reserved region `n` with its link to `n+1`, then write and sync a free-list
  tail-advance command that publishes `n` and advances runtime tail state.
- [ ] Define recovery and validation for every tail-growth cut. A retained
  successor allocation without its tail-advance command requires erasing `n`
  before retry, even when its bytes appear valid. Every startup replay must
  validate every retained free-list operation against the reconstructed basis
  and materialized data, regardless of whether that startup repaired a region.
- [ ] Specify retiring an old free-list backing region as the inverse
  collection-local operation: unlink it and append it to the dirty range in one
  free-list command rather than transaction cleanup. Define its cursor fields,
  replay validation, and crash cuts.

### Reader/writer access model (D23, D37, D40)

- [ ] Classify every top-level operation as read-only or mutating. Read-only
  operations may coexist; an operation that mutates shared persistent or runtime
  state has exclusive top-level storage access and is non-reentrant at that
  boundary. Transaction-private preparation may occur outside that access.
- [ ] Specify how allocator, WAL, transaction, cleanup, and free-list operations
  call one another under one exclusive top-level mutation without acquiring
  subsystem locks. Preserve each required durable-write, sync, and runtime-apply
  ordering explicitly.
- [ ] Define revalidation and fail-stop/recovery behavior when storage I/O fails
  or has an ambiguous result during an exclusive mutation, including which later
  reads or mutations remain admissible.
- [ ] Define the eventual Rust borrowing and open-transaction representation
  without requiring a concrete runtime reader/writer lock.

### Logical storage interface propagation (D09-D11)

- [ ] Update the Rust storage boundary to the agreed logical contract: arbitrary
  byte-range reads, aligned writes, range sync, and region-aligned multi-region
  erase that is durable when it returns successfully.
- [ ] Keep physical alignment, transfer splitting, widened/global sync, and any
  erase durability barrier inside each `FlashIo` implementation while exposing
  their physical work through I/O accounting.
- [ ] Update `02-durability-io.md`, device models, mocks, and crash tests so an
  unsynced write may be absent, complete, or torn, while a successful erase is
  already power-failure durable.
- [ ] Define range-sync substitution precisely: the requested range is the
  minimum durability guarantee, and an implementation may make additional
  earlier writes durable.
- [ ] Decide the exact rule for a sync range that only partially overlaps an
  earlier write: require callers to cover the complete write, define durability
  per covered granule, or have the implementation widen to the full operation.

### Replace the explicit ownership pilot (D17-D20, D46-D48)

- [ ] Rewrite `01-ownership.md` around derived relations. Ready and dirty state
  comes from free-queue intervals, transaction ownership comes from retained
  transaction structures, and collection ownership comes from retained
  collection reachability. There is no authoritative per-region lifecycle
  table.
- [ ] Remove or redesign the Rust `OwnershipTable`, per-region lifecycle array,
  token registry, and duplicated prepared/dirty entry classifications. Runtime
  state should contain only the roots, cursors, transaction descriptors, and
  bounded working state required by the authoritative structures.
- [ ] Replace `region_ownership.qnt` and its refinement bridge with relational
  invariants over the free queue, transaction allocations and cleanup, WAL
  reachability, and collection roots. A derived lifecycle may be used as a
  ghost observation but not as implementation authority.
- [ ] Remove purpose, owner, and operation identity from transaction allocation
  facts unless a later design demonstrates that one is independently required.
  Region roles should be established by durable transaction-segment links and
  committed collection operations; headers validate encoding but do not confer
  ownership.
- [ ] Audit and remove the old general-purpose main-WAL allocation, direct
  reserve/publish, and privileged storage-core allocation paths. Runtime
  transfers between owners should use transactions. Preserve the free-list
  tail-growth command as a collection-local representation change, not an
  ownership-transfer exception.
- [ ] Specify the collection implementation contract: every allocation made by
  a committing transaction must become reachable from the collection's
  committed basis. Core assumes this for opaque collection formats
  and must prove the equivalent structural reachability for its own WAL and
  free-queue collections and for Transaction Owned transaction-log regions.
- [ ] State the relational exclusivity invariants without requiring a global
  scan or table: no active free entry may be live elsewhere, cleanup ownership
  must not overlap live collection ownership, and every runtime transfer must
  have exactly one authoritative structural explanation.

### Chapters and questions not yet worked through (D01-D48)

- [ ] Add a first-class main-WAL lifecycle chapter covering bootstrap, append
  capacity, rotation, successor publication, tail selection after a crash,
  retained-head advancement, allocator checkpoints, and safe reclamation.
- [ ] Specify WAL record framing and torn-tail recovery, then specify replay as
  the final composition of WAL, collection, allocator, and transaction state.
- [ ] Correct startup language: scanning fixed headers finds candidates, while
  validated WAL metadata and replay select the logical roots.
- [ ] Decide whether transaction cleanup is performed synchronously through
  `finish` or exposed as bounded caller-driven maintenance, and make the
  latency/error contract consistent throughout the chapters.
- [ ] Specify readiness-record write and sync failures after all requested
  erases succeeded. Runtime must not advance speculatively, while recovery must
  consistently interpret any complete readiness record that nevertheless
  became durable.
- [ ] Clarify whether WAL-resident and snapshot-resident data may be read from
  storage after startup; avoid describing residence representations as mutually
  exclusive lifecycle states unless the implementation guarantees that.
- [ ] Add the runtime API, caller-owned memory, maintenance scheduling, and
  refinement-evidence chapters after the storage, ownership, and WAL designs
  stabilize.
