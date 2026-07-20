# Core specification design TODOs

This file records unresolved design and specification work discovered while
reviewing `000-system-narrative.md` and `001-vocabulary.md`. Agreed decisions
belong in the specification; this file is only for work that still needs to be
resolved or propagated.

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

Previously agreed design decisions are not reopened by this queue unless a
later contradiction requires it. In particular, the logical `FlashIo`
semantics, relational rather than table-based ownership, transaction-only
cross-owner transfers, free-list-internal representation movement, and
shared-read/exclusive-mutation access to the top-level storage object remain the
working design. A top-level mutation of shared state is non-reentrant, while
internal subsystem operations may compose under the same exclusive access
without subsystem locks. Transaction-private preparation may occur without
entering the top-level storage object.

## Ordered design discussion queue

- [x] **D01 — Document layers and chapter template.** Agree how each chapter
  separates motivation, abstract state and invariants, mechanical protocol, and
  verification requirements.

  Decision: Use a three-pass spiral: a gradual cross-subsystem system narrative;
  component deep dives that combine complete abstract and mechanical design; and
  composition chapters for runtime, recovery, capacity, and verification.
  Mechanical chapters embed designated normative Rust code blocks for exact
  in-memory state, operation progress, inputs, results, and pure
  foreground/replay transitions. Specification verification extracts those
  blocks into a generated crate and requires them to compile and type-check;
  the Markdown blocks remain authoritative. Prose defines meaning, durability
  ordering, crash interpretation, and bounds. Persistent layouts use explicit
  codecs rather than Rust memory layout.

  Rationale: Borromean's subsystems motivate one another cyclically, so a strict
  dependency order would either overwhelm the reader or introduce unexplained
  abstractions. Progressive introductions may rely on a minimal contract, while
  later chapters add precision without changing earlier meaning. Embedded,
  compile-checked Rust keeps precise definitions beside their explanations,
  avoids pseudocode interpretation gaps, and avoids a second hand-maintained
  source of authority. Production should reuse the extracted definitions or
  provide an explicit refinement.

  Patch scope: Record this decision here and add `spec/core/authoring.md` as a
  flexible checklist. Keep `000-system-narrative.md` together as the system
  narrative and do not alter its design content in this patch.

  Verification: Review the guide against this decision, confirm that every
  template section is optional when inapplicable, and run Markdown/diff checks.
- [x] **D02 — Requirement and evidence method.** Agree when stable requirement
  IDs are assigned, how a chapter records model/Rust/test obligations, and how
  decisions remain traceable while chapters move. The follow-up patch adds only
  the specification-writing and evidence template.

  Decision: Give an ID only to a normative rule in its authoritative component
  or composition chapter after a concrete semantic pass/fail method has been
  defined. The evidence may be an executable Rust test, a compile or static
  check, or a model check, but it must compute or establish the required
  property rather than search source text for a string. An ID may precede the
  implementation of its evidence so that the unmet obligation remains visible.
  Narrative previews, rationale, assumptions, and unresolved claims do not
  receive requirement IDs.

  The canonical traceability identity is the complete normalized requirement
  text, including its verification method; the ID is a convenient label that
  must be unique among active requirements. Write each record as one grammatical
  RFC-2119 sentence with one ID and one normative keyword, followed within that
  sentence by a semicolon and a `Verification` clause that states what evidence
  computes or establishes. Tests quote the complete record. Duvet `type=test`
  evidence counts toward coverage; citations do not. Do not maintain a retired
  ID ledger for now.

  Moving an unchanged canonical record preserves its ID and updates the Duvet
  document-and-anchor reference. Reflow does not change identity. A substantive
  wording or verification change updates the complete quotation and is reviewed
  as a change to the acceptance contract. Git retains its history.

  Rationale: A requirement without an executable or mechanically checked
  acceptance condition cannot provide meaningful conformance traceability.
  Keeping the verification method in the cited record makes the intended test
  oracle difficult to overlook. Duvet 0.4.1 accepts partial and even ID-only
  quotations, so Duvet alone cannot enforce this convention; the existing local
  traceability audit must add exact-record enforcement. The complete behavior
  and verification text matters more than permanent reservation of its label.

  Patch scope: Add only the requirement-writing and evidence-record template to
  `spec/core/authoring.md`. Do not add v3 requirements, change Duvet
  configuration, or modify traceability tooling in this patch.

  Verification: Review the template against this decision and the observed
  Duvet extraction behavior, then run Markdown and diff checks.
- [x] **D02A — Defer complete-record enforcement.**

  Decision: Treat complete requirement quotations as an authoring and review
  convention for v3. Do not extend the traceability audit while designing or
  producing the first working v3. Reconsider purpose-built requirement-tracing
  tooling only after that milestone, or later.

  Rationale: Duvet accepts partial quotations and therefore cannot enforce the
  complete-record convention. Building a second enforcement layer now would
  interrupt the storage-design work, while the specification and its evidence
  format may still change substantially.

  Patch scope: Record the deferral in this queue and state the present tooling
  limitation in `spec/core/authoring.md`. Do not change traceability tooling.

  Verification: Confirm the future work remains in the detailed backlog and run
  Markdown and diff checks.
- [x] **D02B — Defer model-evidence integration.**

  Decision: Design model-check registration as part of the same purpose-built
  post-v3 traceability tooling, rather than adding an adapter to Duvet now.

  Rationale: A model check must run the model and identify the checked property;
  a placeholder Rust test is not evidence. The right representation depends on
  the future traceability design and need not block the v3 specification.

  Patch scope: Retain the question in the post-v3 tooling backlog only. Do not
  change models, Duvet configuration, or verification scripts.

  Verification: Confirm D03 becomes the first unchecked design item and run
  Markdown and diff checks.
- [x] **D03 — Defer the implementation-preservation inventory.**

  Decision: Perform semantic preservation review as a near-final design audit
  under D48, after the relevant v3 component and composition chapters are
  stable. Audit one subsystem at a time rather than creating a detailed mapping
  against the incomplete design now.

  Rationale: v3 intentionally changes some semantics and mechanisms. An early
  point-by-point comparison would report unresolved or deliberately changed
  behavior as accidental omissions. The archived pilot, current specification,
  tests, models, and implementation snapshot preserve the source material for a
  meaningful later comparison.

  Patch scope: Record the deferral here and expand D48 to own the comparison
  method. Do not create an inventory or change design or implementation files.

  Verification: Confirm D04 becomes the first unchecked design item and run
  Markdown and diff checks.
- [x] **D04A — Storage and encoding units.** Agree the minimum cross-chapter
  meanings of region and operation record, including what each term deliberately
  leaves to later geometry and framing decisions. The follow-up patch adds only
  these terms to the common vocabulary.

  Decision: A region is one of the equal-sized, non-overlapping byte ranges in
  Borromean's configured logical region area. Its start is erase-block aligned,
  its length is an erase-block multiple, and its complete span includes any
  region header or format prefix. A region is Borromean's unit of storage
  allocation, reclamation, and reuse. A structure's responsibility covers an
  entire region, and erase operations cover one or more whole regions. The
  database header for the whole store is outside the indexed region area. A
  region index names only the reusable byte range; current role and
  responsibility derive from retained structures, and bytes are interpreted
  only after structure-specific validation. The common region header has no
  independent padding requirement because it is written with the
  collection-defined data that follows it as one logical write.

  An operation record is a finite byte sequence representing one
  collection-defined mutation of a particular collection's logical state.
  Applying an ordered sequence of operation records to a collection basis
  produces later collection state. Its meaning does not depend on its own
  storage location or persistent framing. A log format may add routing,
  framing, integrity, alignment, and ordering metadata. WAL-protocol records
  are one subset of operation records; an operation record carried by the WAL
  does not become a WAL-protocol record merely because of that placement.

  Rationale: Region geometry must remain distinct from the backing erase-block
  geometry and from relational ownership roles. Operation record names the
  semantic state transition without conflating it with a stored collection
  value, a physical append frame, or a WAL-protocol operation. This is the
  ordered-operation foundation also used by Operational Transform systems;
  whether concurrent operations are transformed is outside this definition.

  Patch scope: Add the operation-record definition to the opening state-change
  discussion in `spec/core/000-system-narrative.md`, add the region definition
  to its geometry introduction, clarify the immediately related WAL
  terminology, and replace the misleading residence list with the agreed
  current-root and later-operation distinction. Do not decide exact geometry,
  routing fields, persistent framing, stale-link validation, snapshot and
  materialization
  equivalence, or snapshot reread behavior.

  Verification: Review the resulting definitions against this decision and run
  Markdown and diff checks. Leave D04A unchecked until that bounded patch has
  been reviewed.
- [x] **D04B1 — Durable authority and publication.** Agree the minimum
  cross-chapter meanings of durable operation record and publication. The
  follow-up patch aligns only their entries in the common vocabulary and records
  the later owners of their mechanical treatment.

  Decision: An operation record is durable when its complete persistent
  representation will survive power loss under the logical storage contract.
  Durability alone does not make its effect committed, visible, or part of a
  particular logical view. Publication is the protocol-defined durability point
  after which recovery must include a specified effect in the relevant logical
  view. A record's durability and the publication of its effect may coincide,
  or a later record such as transaction commit may publish an already durable
  private effect.

  Command, durable fact, and runtime apply are not first-class vocabulary.
  Operation names the abstract state change, and operation record names its
  collection-defined bytes. Fact retains only its ordinary-language meaning.
  Prose may say that RAM is updated after publication; D20 and the component
  chapters own the exact foreground and replay transitions.

  Rationale: These two terms distinguish survival of recorded bytes from
  recoverable authority without adding an intermediate command representation
  or formalizing ordinary words. The distinction is required by durable private
  transaction operations, which remain outside the committed view until commit.

  Patch scope: Keep the minimum definitions in `001-vocabulary.md` and align
  publication with the distinction above. Do not add standalone definitions to
  the system narrative or define exact carriers, sync sequences, retention,
  reachability, or replay mechanics. D20 and the mechanical WAL chapter own the
  exact foreground and replay transitions.

  Verification: Review both vocabulary entries against this decision, confirm
  no standalone durability or publication definition appears in the system
  narrative, and run Markdown and diff checks. Leave D04B1 unchecked until this
  bounded patch has been reviewed.
- [x] **D04B2 — Reachability.** Agree the minimum cross-chapter meaning of
  reachable without implying an explicit ownership table.

  Decision: Reachability remains a relationship derived from collection,
  transaction, WAL, and free-queue structures rather than an intrinsic property
  recorded against a region. It does not need a standalone definition in the
  introductory narrative; the collection-root definition supplies the access
  model needed there.

  Rationale: A separate definition would repeat the root's essential property
  while introducing traversal and link validity before their mechanics are
  motivated.

  Patch scope: Keep relational ownership language, but remove the standalone
  reachability paragraph. D16 owns the traversal contract, D18 owns the
  relational safety invariants, and D19 owns stale-link validation.

  Verification: The narrower treatment was reviewed in context.
- [x] **D04B3 — Retention.** Agree the minimum cross-chapter meaning of retained
  while deferring exact retention sets and release points to the mechanical WAL
  and recovery chapters.

  Decision: Data is retained while Borromean may still need it during recovery
  or to finish work interrupted by power loss. Retained data, and any region
  containing it, cannot be reclaimed or reused. Retaining a collection root also
  retains every record and region reachable from that root. The mechanical WAL
  and recovery chapters define what must be retained and when it may be
  released.

  Rationale: Reachability describes the data accessible from one collection
  root. Retention says when Borromean must preserve that root and its reachable
  data. This gives reclamation a clear safety rule without prematurely selecting
  WAL boundaries, checkpoints, traversal rules, or replay mechanics.

  Patch scope: Align only the `Retained` entry in `001-vocabulary.md`. Do not
  change the reachability definition or add a standalone retention definition to
  the system narrative. D21, D27, D35, and the recovery chapters own the exact
  retention and release mechanics.

  Verification: Review the vocabulary entry against this decision, confirm no
  standalone retention definition appears in the system narrative, and run
  Markdown and diff checks. Leave D04B3 unchecked until this bounded patch has
  been reviewed.
- [x] **D04C1 — Collection root.** Agree the minimum cross-chapter meaning of a
  collection root.

  Decision: A collection root is a single region, snapshot, or in-memory
  frontier from which all live data in the collection can be accessed.

  Rationale: A collection may link multiple regions into a larger structure and
  therefore needs one access point. The definition states that essential role
  without conflating the current root with a recovery basis or prescribing
  traversal mechanics.

  Patch scope: Motivate and define the collection root in the introductory
  collection discussion. Later chapters explain the three root forms and how
  the root moves between them.

  Verification: The definition was reviewed in its narrative context.
- [x] **D04C3 — Basis, snapshot, and materialization.** Agree their minimum
  cross-chapter meanings and their relationship to the current collection root.

  Decision: A collection basis is an object representing a collection at one
  point in its history. Interpreting the basis and following its
  collection-defined references yields the complete logical state at that point.
  Those references may lead to earlier bases, so the basis need not contain the
  complete state in its own bytes.

  An in-memory frontier is the newest basis currently held in RAM. A snapshot is
  a basis stored in the WAL, and a region materialization is a basis rooted in a
  region. Once published, either durable form can serve as the collection's
  durable root. Applying later published operation records in order reconstructs
  the in-memory frontier, which serves as the current root while resident.

  Rationale: Logical completeness means the whole collection state can be found
  by interpreting the basis, not that all of its bytes are stored together. The
  three forms separate the role of a basis from where its starting object
  resides, while the durable-root and frontier distinction explains how later
  operations coexist with a published basis without being represented twice.

  Patch scope: Align the four terms in `001-vocabulary.md`, add the basis
  discussion alongside the separate operation-representation explanation in
  section 4 of `000-system-narrative.md`, and move D04C3 before its dependent
  D04C2. Do not change the meaning of an operation record or define head
  terminology, snapshot/materialization equivalence, persistent layouts,
  frontier admission, reread policy, or publication mechanics.

  Verification: Review each form against the logical-but-not-physical
  completeness rule, confirm section 4 explains operation records and collection
  bases as separate concepts, confirm operation effects are not applied twice,
  and run Markdown and diff checks. Leave D04C3 unchecked until this bounded
  patch has been reviewed.
- [x] **D04C2 — Head terminology.** Agree qualified uses of head without
  conflating a head record with the collection root it names.

  Decision: A collection head record is a WAL-protocol record that names the
  root region of a region materialization as the collection root. The record is
  not itself the root. The word head has no unqualified cross-chapter meaning. A
  queue or log position uses its existing cursor name or identifies the
  structure whose head is meant.

  Rationale: The collection head record publishes a reference to the collection
  root; it is not the object from which collection data is read. Requiring a
  cursor name or a qualified structure also prevents free-list, main-WAL, and
  transaction-log positions from being confused with one another.

  Patch scope: Align the vocabulary entry, add the distinction to section 4,
  replace the mistaken committed-head use with committed root, and replace
  ambiguous free-list head phrases with the existing allocation-cursor terms.
  Apply the agreed `free-list-internal` terminology throughout the active core
  documents. Leave the exact main-WAL and transaction-log positions and
  mechanics to their later chapters.

  Verification: Review every use of head in the system narrative. Collection
  head record must mean the WAL-protocol record, collection root must mean the
  basis it names, and free-list positions must use their cursor terms. Run
  Markdown and diff checks, and leave D04C2 unchecked until this bounded patch
  has been reviewed.
- [x] **D04D — Log roles.** Agree provisional cross-chapter definitions of the
  main WAL and transaction log without deciding the detailed protocols owned by
  their later chapters. The follow-up patch adds only these terms to the common
  vocabulary.

  Decision: The WAL is one internal collection with two roles. The main WAL is
  its shared history and serves as the root of the whole database. It orders the
  records recovery uses to reconstruct shared state, including collection
  publications, allocator progress, transaction decisions, cleanup, and
  references to transaction logs.

  A transaction log is durable WAL storage assigned to one open transaction. It
  holds the transaction's allocation entries, free intents, and collection
  operation records. Collection operations and free intents remain private until
  a main-WAL commit record publishes them. An allocation entry affects allocator
  state when it becomes durable so recovery can account for every region removed
  from the free list, even when the transaction has no durable decision.

  Rationale: Transaction logs let a transaction prepare durable work without
  placing each private operation directly in the shared history. The main WAL
  still provides one shared order for publication and recovery. Allocation
  entries take effect earlier than the transaction decision because otherwise a
  crash could lose track of a region already removed from the free list.

  Patch scope: Align the `Main WAL` entry and add the `Transaction log` entry in
  `001-vocabulary.md`. Do not change the system narrative or define persistent
  layouts, framing, log continuation, rotation, retention boundaries, commit
  mechanics, cleanup mechanics, or replay order.

  Verification: Review that transaction-log durability does not by itself
  publish collection operations or free intents, while a durable allocation
  entry does affect allocator recovery. Confirm the main WAL remains the shared
  database root, then run Markdown and diff checks. Leave D04D unchecked until
  this bounded patch has been reviewed.
- [x] **D05 — Exact chapter spine and dependency cycle.** Agree the chapter
  order and how the circular dependency among the main WAL, transaction logs,
  and free list is introduced through abstract contracts before concrete
  self-hosting mechanics. Every specification-chapter filename uses a
  three-digit, zero-padded reading-order prefix. The follow-up patch may reorder
  or add outline sections in `000-system-narrative.md` and may reorder or split
  the remaining unchecked `Dxx` items here without changing their substantive
  questions; it must not fill in component protocols.

  Decision: Use the following specification reading order:

  1. `000-system-narrative.md`
  2. `001-vocabulary.md`
  3. `002-device-format-and-io.md`
  4. `003-region-relations.md`
  5. `004-main-wal.md`
  6. `005-transactions.md`
  7. `006-free-list.md`
  8. `007-self-hosting-and-progress.md`
  9. `008-storage-service-and-collections.md`
  10. `009-runtime-and-maintenance.md`
  11. `010-recovery.md`
  12. `011-verification-and-refinement.md`

  The first two chapters form the narrative pass. Chapters 002 through 006 are
  component deep dives. Chapters 007 through 011 reconnect those components into
  the self-hosting storage system, its collection service, runtime, recovery,
  and verification argument.

  The main WAL, transaction logs, and free list use the minimum contracts
  introduced by the narrative and vocabulary while their component chapters are
  read. `007-self-hosting-and-progress.md` then closes their dependency cycle
  and establishes bootstrap, recursive allocation, log growth, reclamation, and
  capacity progress. `008-storage-service-and-collections.md` defines the
  collection contract only after the internal machinery supporting it has been
  defined. Recovery follows the storage service and runtime because it composes
  their durable transitions, maintenance work, memory bounds, and open-state
  contract rather than defining a second set of component rules.

  Rationale: The collection service is supported by the WAL, transactions, free
  list, and in-memory runtime structures, so its complete contract belongs after
  those internals. The internal components still need a small shared collection
  language; the narrative and vocabulary supply it without prematurely defining
  the user-facing service. Recovery is last among the operational composition
  chapters because it reconstructs the already-defined runtime state by applying
  the already-defined component transitions.

  Patch scope: Record the chapter spine here, replace the system narrative's
  drafting note with a short reading guide, and give its existing numbered
  sections descriptive headings. Do not create chapter stubs, reorder narrative
  content, reorder later design questions, or add component mechanics.

  Verification: Confirm every planned specification chapter has one unique
  three-digit reading-order prefix, the component and composition passes remain
  distinct, the dependency cycle has an explicit later closure, and each new
  narrative heading describes its existing content. Run Markdown and diff
  checks, and leave D05 unchecked until this bounded patch has been reviewed.
- [x] **D06 — Append-only scope.** Agree that an update appends a new
  representation instead of overwriting the previous one, while update and
  deletion encodings remain collection-defined. The follow-up patch changes only
  that introductory claim.

  Decision: Borromean is an append-only store. An update appends a new
  representation instead of overwriting the previous one. Each collection
  defines how updates and deletions are represented.

  Rationale: Append-only describes how an update changes stored state, not how
  that update first becomes durable or published. Requiring a delta, replacement
  copy, or tombstone would incorrectly make a collection-specific encoding
  choice part of the core storage rule.

  Patch scope: Replace only the append-only paragraph in section 1 of
  `000-system-narrative.md`. Do not change WAL persistence, transaction
  publication, retention, reclamation, or collection operation formats.

  Verification: Confirm the paragraph prohibits overwriting an earlier
  representation, leaves update and deletion encoding to the collection, and
  does not describe a WAL or transaction path. Run Markdown and diff checks, and
  leave D06 unchecked until this bounded patch has been reviewed.
- [x] **D07 — Discovery and logical-root wording.** Agree how the fixed database
  header, region-header scan, WAL head and tail, and recovered collection roots
  relate. The follow-up patch changes only the introductory discovery
  explanation.

  Decision: Repeatedly updating a fixed database-root location would concentrate
  wear, so the main-WAL tail moves through the region area as the database
  changes. The fixed database header contains immutable facts, including the
  database geometry and physical storage parameters. At startup, those facts
  locate the region headers. Borromean scans all region headers to find the
  current WAL tail, which points to the retained WAL head. The WAL range from
  the retained head through the current tail is the root of the database.
  Replaying that range recovers the current collection roots.

  Rationale: The immutable header can remain at a fixed location without being
  rewritten for each database change. Keeping physical discovery separate from
  logical replay also makes clear that geometry helps locate the WAL but is not
  itself part of the database root.

  Patch scope: Replace only the introductory root-discovery paragraph in section
  1 of `000-system-narrative.md`, while retaining the region-size tradeoff as a
  separate paragraph. Do not define header fields, WAL validation, tail
  selection rules, replay order, or recovery failure handling.

  Verification: Confirm that startup scans all region headers, the database root
  is only the retained WAL head-to-tail range, and replay recovers the current
  collection roots. Confirm that the paragraph does not make geometry part of
  the root or introduce WAL-validation mechanics. Run Markdown and diff checks,
  and leave D07 unchecked until this bounded patch has been reviewed.
- [x] **D08 — Qualified wear claim.** Agree the ordering FIFO provides among
  free regions and why it does not guarantee equal wear across the whole
  device. The follow-up patch changes only the wear-leveling claim and non-goal
  note.

  Decision: Free regions are kept in a FIFO queue, and allocation takes the
  oldest ready entry. No region is returned to use before any free region that
  entered the queue earlier. This does not cover regions holding long-lived
  data because they are not available for reuse. Borromean does not move live
  data solely to balance wear.

  Rationale: FIFO guarantees reuse order among free regions, not equal wear
  across every region of the device. Describing the boundary in terms of whether
  a region is free covers pinned, reserved, bootstrap, and other long-lived uses
  without requiring a separate list of special cases.

  Patch scope: Replace only the introductory FIFO paragraph and its non-goal
  note in section 1 of `000-system-narrative.md`. Do not change free-list
  representation, dirty-to-ready maintenance, allocation eligibility, or add
  live-data relocation.

  Verification: Confirm the paragraph states the FIFO ordering guarantee,
  excludes unavailable long-lived regions from that guarantee, and does not
  claim equal wear across the whole device. Run Markdown and diff checks, and
  leave D08 unchecked until this bounded patch has been reviewed.
- [x] **D09 — Logical read API and lifetime.** Preserve unaligned logical reads
  and define the callback shape, borrowing lifetime, zero-length reads, and
  large-range bound. The follow-up patch changes only the `read` contract.

  Decision: `read` takes a callback and invokes it once with one contiguous
  borrowed slice containing exactly the requested bytes. The slice is valid
  only during the callback; the callback may copy or interpret it but cannot
  retain it. Reads need not be aligned, must remain within either the fixed
  database-header span or one region, and cannot exceed the region size. A
  zero-length read at an in-range address performs no device transfer and calls
  the callback once with an empty slice. Larger logical values are processed
  through multiple reads.

  Rationale: A callback lets mapped storage lend its bytes directly while an
  embedded backend may use fixed scratch memory. Limiting one read to one
  region bounds that scratch memory by the configured region size. Keeping the
  borrow inside the callback makes its lifetime explicit and prevents a backend
  buffer or mapped view from escaping its valid access period.

  Patch scope: Change only the logical `read` signature and its explanatory
  paragraph in section 2 of `000-system-narrative.md`. Preserve the existing
  continuous-power and post-restart visibility rules. Do not change write,
  sync, erase, physical transfer alignment, error types, or exact backend Rust
  traits.

  Verification: Confirm that successful reads provide exactly one contiguous
  slice for the requested range, the slice cannot escape the callback,
  zero-length reads require no device transfer, and no read requires more than
  one region of scratch memory. Confirm that unaligned reads and the existing
  power-loss interpretation remain unchanged. Run Markdown and diff checks, and
  leave D09 unchecked until this bounded patch has been reviewed.
- [x] **D10 — Logical write and failed-write semantics.** Agree the erased-range
  precondition, alignment, continuous-power visibility, tear boundary, and
  allowed physical effects when `write` returns an error. The follow-up patch
  changes only the `write` contract.

  Decision: A write address is write-granule aligned, its length is a multiple
  of the write granule, and every granule in its range has not been programmed
  since its last erase. Alignment, bounds, and erased-range rejection occur
  before any device program operation and leave storage unchanged. Once
  programming begins, an error may leave the range unchanged, complete, or
  torn. A torn write has zero or more complete leading granules, at most one
  partly programmed granule, and an erased remainder. Bytes outside the range
  remain unchanged. Success makes the complete data visible under continuous
  power; a covering sync is still the minimum durability guarantee, though a
  backend may make the write durable earlier.

  Rationale: Requiring an unused erased range keeps the logical I/O contract
  consistent with append-only storage and does not depend on media-specific
  in-place bit clearing. Treating the write granule as alignment rather than
  atomicity makes the ambiguous effects of a failed device write explicit. A
  caller therefore cannot assume that the same range remains erased and safe
  to retry after programming has begun.

  Patch scope: Replace only the `write` explanation in section 2 of
  `000-system-narrative.md`. Do not change the operation signature, read, sync,
  erase, exact backend error types, post-error fail-stop policy, or backend
  implementations.

  Verification: Confirm that precondition rejection performs no program
  operation, success is fully read-visible under continuous power, a failed
  program can be absent, complete, or torn inside one granule, and no write
  changes bytes outside its requested range. Confirm that sync remains the
  minimum durability guarantee and earlier durability remains allowed. Run
  Markdown and diff checks, and leave D10 unchecked until this bounded patch
  has been reviewed.
- [x] **D11 — Range-sync and failed-sync semantics.** Preserve range sync as the
  API and define zero-length behavior, composable partial ranges, widened
  durability, and the effects allowed when `sync` returns an error. The
  follow-up patch changes only the `sync` contract.

  Decision: Sync addresses and lengths are write-granule aligned and in range.
  A zero-length sync succeeds without a backend barrier and adds no durability
  guarantee. A successful nonempty sync makes each previously written granule
  in its requested range durable. Successful sync ranges compose, so several
  calls may together make a complete write durable. The requested range is a
  minimum guarantee: an implementation may widen it or use a global barrier,
  and callers cannot depend on other writes remaining non-durable. Sync changes
  durability but not bytes visible under continuous power. Validation rejection
  invokes no barrier. After a barrier is attempted, an error may leave none,
  some, or all earlier write effects durable, including effects outside the
  requested range. Previously durable data remains durable.

  Rationale: Defining the guarantee per aligned granule matches a range API and
  lets successful partial ranges compose without requiring one call to cover a
  whole write. A backend that supports only a global barrier can still satisfy
  the contract by widening the operation. A failed barrier cannot roll back
  durability or reliably report how much work completed.

  Patch scope: Replace only the `sync` explanation in section 2 of
  `000-system-narrative.md`. Do not change the operation signature, write, read,
  erase, exact backend error types, post-error fail-stop policy, or backend
  implementations.

  Verification: Confirm that zero length invokes no barrier, successful ranges
  compose at write-granule boundaries, a wider or global barrier is allowed,
  sync never changes continuously visible bytes, and a barrier error provides
  no new durability guarantee while preserving all prior guarantees. Run
  Markdown and diff checks, and leave D11 unchecked until this bounded patch
  has been reviewed.
- [x] **D12 — Format metadata, version, and geometry.** Agree immutable metadata
  fields, erased byte, format-version compatibility, configured storage range,
  geometry validation, and sequence-exhaustion policy. The follow-up patch adds
  only the static format contract; bootstrap ordering is D38.

  Decision: The fixed database header contains only immutable interpretation
  facts: format marker, explicitly supported format version, encoded metadata
  length, integrity check, database-header span, erase-block size, region size
  and count, logical write granule, and erased byte. It also contains format-time
  capacity limits that recovery must know, such as the number of transaction-log
  slots, and values needed to recognize encoded data, such as the WAL record
  marker. It contains no WAL head or tail, allocator cursor, collection root, or
  other mutable state. Runtime tuning settings are excluded because they do not
  change the meaning or layout of stored data. The configured database length
  is the header span plus region count times region size. The logical range
  presented to Borromean matches that length exactly; a larger physical device
  may expose the database as a bounded subrange.

  Open validates the format marker and integrity before trusting metadata,
  selects only an explicitly implemented version decoder, and never guesses
  compatibility or silently upgrades. It rejects invalid or overflowing
  geometry, range mismatch, erase-alignment violations, an incompatible logical
  write granule, erased-byte mismatch, or fixed capacities that do not fit the
  region count. Rejection does not modify storage.

  Each newly initialized region receives a monotonically increasing sequence
  number in its region header. Region sequence numbers never wrap or repeat. An
  operation that would need a value beyond the encoding's maximum returns the
  permanent `SequenceExhausted` error before media I/O. Existing data remains
  readable; additional sequence space requires explicit migration or
  reformatting.

  Rationale: The fixed header supplies the physical search geometry and stable
  interpretation parameters needed before WAL discovery. Keeping mutable roots
  out of it avoids repeatedly rewriting a fixed flash location. Exact version
  dispatch prevents accidental misinterpretation. Rejecting region-sequence
  exhaustion preserves region ordering without reuse or wraparound.

  Patch scope: Add only the static database-header, validation, compatibility,
  record-recognition, and region-sequence-exhaustion contracts to section 2 of
  `000-system-narrative.md`. Do not define the exact byte codec, format
  publication order, interrupted-format outcomes, migration procedure, mutable
  WAL discovery state, or backend implementation changes. D33 owns exact WAL
  framing, D36 owns reserve mechanics, and D38 owns bootstrap publication.

  Verification: Confirm every listed header fact is immutable, no mutable root
  appears in the header, the configured range has one unambiguous formula,
  unsupported versions and geometry mismatches are rejected without
  writes, and no region sequence can wrap or repeat. Run Markdown and diff
  checks, and leave D12 unchecked until this bounded patch has been reviewed.
- [x] **D17 — Relational ownership vocabulary.** Agree the general meanings of
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
- [ ] **D18 — Relational safety invariants.** Agree the disjointness,
  completeness, and reachability predicates that replace a per-region state
  table, including free-list backing, cleanup obligations, transaction
  allocations, and foreground/replay equivalence. The follow-up patch states
  invariants only; implementation and model replacement remain separate work.
- [ ] **D19 — Region-incarnation and stale-link validation.** Decide what
  structure-specific generation, nonce, preamble, or other evidence proves that
  a retained link names the intended reuse of a physical region. The follow-up
  patch defines the safety requirement without choosing unrelated header fields.
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
