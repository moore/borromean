# Core Stabilization And Modularization Working Plan

> **Archive status:** Historical planning input from 2026-07-10. The active
> narrative and design queue supersede this document.

Date: 2026-07-10

Status: provisional discussion note; intended to capture the current direction
before further design comments.

## Motivation

The development audit found multiple correctness and consistency problems in
the WAL, free-space, transaction, reclaim, and startup paths. Fixing each
finding directly is possible, but many of the findings arise from the same
underlying problem: the core durability and ownership rules are distributed
across several large specifications and several partially duplicated
implementation paths.

The proposed direction is to pause collection development and stabilize the
storage core through:

- smaller specification units;
- explicit lower-level operating principles;
- a clearer implementation layering;
- several focused Quint models;
- one blocking implementation interface;
- a traceable connection from English properties to models, Rust invariants,
  proofs, and tests.

The audit findings remain useful as counterexamples and acceptance criteria.
The goal is not to discard the current work, but to use it to define a smaller
and more comprehensible kernel.

## Core Product Goal

Borromean's core goal is:

> A fast, multi-collection and multi-collection-type storage system that
> natively implements wear leveling and crash recovery.

These four qualities motivate the architecture. They are not independent
features to add after the storage engine is otherwise complete:

- **Fast** means operations minimize device work, preserve useful locality,
  avoid unnecessary pointer chasing, and avoid broad searches for state that
  can be maintained incrementally.
- **Multi-collection** means many independent collection instances share one
  device and one durability, allocation, reclaim, and recovery kernel.
- **Multi-collection-type** means the core supports different logical data
  structures and durable layouts without embedding one collection's semantics
  into the storage kernel.
- **Native wear leveling** means allocation, freeing, erase maintenance, and
  reclaim distribute physical wear as part of the normal ownership protocol.
- **Native crash recovery** means every publishing transition and intermediate
  ownership state has defined restart behavior rather than relying on
  collection-specific repair after failure.

The specification and implementation should evaluate important design choices
against this goal. Correctness is mandatory, but a correct design that requires
unbounded searches, excessive pointer chasing, or concentrated rewriting would
not satisfy the product goal.

## Performance Principles

Performance is a core design property, not only a later benchmarking concern.
The specifications should state structural and operation-cost expectations
where they affect the protocol.

### Preserve Locality

Durable layouts and operation plans should keep data that is consumed together
close in the logical access order.

This includes:

- favoring sequential or bounded-range reads over unrelated random reads;
- keeping operation metadata sufficient to continue from a known local
  position;
- avoiding reference graphs that require chasing pointers across many regions;
- using immutable materialized regions that can be scanned in a predictable
  order;
- keeping recovery work close to the retained log or basis that describes it.

Physical contiguity is useful when available, but it must not defeat wear
leveling. The stronger requirement is bounded, predictable access locality:
the protocol should know which small set or ordered sequence of regions it must
visit rather than search the whole device.

### Minimize Search

Operations should maintain enough ordered state that completion and recovery
can continue directly instead of rediscovering obligations with a global
search.

The ordered cleanup list used by transaction finish is an important example:

- commit or rollback establishes the exact ordered cleanup obligations;
- a cleanup cursor identifies the next obligation;
- each completed free advances that cursor;
- restart resumes at the cursor;
- transaction finish does not search all regions or all WAL history to
  reconstruct what remains.

This principle should also guide:

- retained-basis selection;
- WAL traversal;
- free-space cursor lookup;
- reclaim liveness;
- collection lookup and compaction metadata;
- recovery of private reservations.

### Make Operation Cost Explicit

For important operations, the specification should identify expected bounds
or cost drivers such as:

- regions read;
- regions written or erased;
- sync operations;
- records scanned;
- pointer or link traversals;
- search range;
- memory required;
- cleanup work retained for later.

Not every bound must be a single constant independent of device size. It must,
however, be clear which retained structure or configured capacity controls the
cost. Hidden whole-device or whole-history searches should be treated as design
failures unless they are an explicit maintenance operation.

### Preserve Predictability

For the target use case, fast also means predictable:

- normal reads should not trigger hidden writes or maintenance;
- ordinary writes should not unexpectedly perform unbounded erase, reclaim, or
  cross-collection work;
- maintenance pressure should be reported explicitly;
- bounded work may be deferred through an explicit cleanup or maintenance
  operation.

### Track Goal Tensions Explicitly

Some core goals pull in different directions:

- wear leveling spreads physical writes, while locality favors clustering;
- crash recovery adds durable metadata and sync points, while speed favors
  fewer device operations;
- generic multi-type support favors common protocols, while individual
  collection types may benefit from specialized layouts;
- append-only replacement improves recovery, while it increases allocation and
  later reclaim work.

The design should record these tradeoffs rather than optimize one goal
implicitly. Models prove safety and protocol properties; operation-cost
requirements, instrumentation, and benchmarks provide the corresponding
performance evidence.

## Decisions Reached So Far

### Core Before Collections

User-facing collection work should stop until the WAL, free-space, transaction,
reclaim, and recovery protocols are correct, understood, and well tested.

Map, object-log, and channel behavior can provide examples and regression
tests, but their feature development is not part of the stabilization
milestone.

### Preserve The Frontier And Immutable-Basis Pattern

The current collection pattern is a good starting point:

- bounded mutable state lives in an in-memory frontier;
- durable materialized regions are immutable;
- new durable state is prepared separately;
- a durable record publishes the new basis;
- replay combines the retained basis with later frontier updates.

This shape is already close to a pure state-transition model without I/O. The
storage core should apply the same pattern to its private free-space and log
metadata instead of rewriting live durable state in place.

### Drop The Async Interface

The async/Future model should be removed for now.

Reasons:

- it was scope growth beyond the immediate storage problem;
- FlashIo is currently synchronous;
- blocking and Future paths already contain divergent behavior;
- maintaining two interfaces increases the verification surface;
- a real async design can be reconsidered after the blocking kernel is stable.

The public core should expose one blocking interface.

Internal operation phases may still be useful for:

- making durable boundaries explicit;
- modeling crash cuts;
- fault injection;
- keeping long operations understandable;
- driving one blocking executor.

Internal phases do not imply a public Future interface.

### Use Quint As The Modeling Language

Quint is the single modeling language for now.

Other modeling languages should be introduced only if Quint presents a
specific demonstrated limitation that blocks an important property.

The repository should use actual reproducible Quint verification, not treat a
successful randomized simulation as proof.

### Separate Abstract Modeling From Rust Verification

There are two complementary formal-method goals:

1. Use several small Quint models to explore and verify abstract state-machine
   properties.
2. Where valuable and practical, formally verify pure Rust implementation
   modules against explicit invariants and transition contracts.

Ordinary tests, property tests, model-based tests, fuzzing, and crash injection
remain useful. Formal proof should be concentrated on small, important logical
modules rather than assumed necessary for every byte-level or device-level
operation.

## Core Operating Principles

The specifications should begin with a short set of principles that can be
read and retained independently of the full protocols.

### Unique Region Ownership

Every physical region has exactly one current owner or recovery role.

Candidate ownership states include:

- ready free;
- dirty free;
- privately reserved for a named purpose;
- main-WAL owned;
- transaction-log owned;
- allocator-metadata owned;
- collection owned;
- detached but protected by unfinished recovery.

A region must never be both live and free.

### No Lost Regions

Every physical region is either:

- reachable from committed live state;
- present in unconsumed free space;
- held by a durable private reservation;
- protected by unfinished transaction or recovery state.

No operation or crash may make a region disappear from all of these classes.

### Append-Only Durable Publication

Live durable state is not rewritten in place.

A replacement is:

1. privately reserved;
2. prepared;
3. written and synced;
4. published by a durable record;
5. applied to stable runtime state;
6. followed by reclamation of the previous state.

Recovery before publication selects the old state. Recovery after publication
selects the new state.

### Durability Before Visibility

Replay-visible runtime state advances only after its publishing durable event
has been synced.

A crash after durability but before runtime application is recovered by
replaying the durable event.

### Shared Transition Semantics

Foreground execution, startup replay, crash recovery, and reclaim must use the
same logical event validation and application rules.

There should not be separate handwritten interpretations of one durable
record.

### Preflight Bounded Resources

Capacity and reserve failures must be detected before writing a durable event
whose replay requires that capacity.

This includes:

- in-memory queue capacity;
- transaction bookkeeping;
- WAL space;
- cleanup space;
- allocator metadata capacity;
- maintenance reserves.

### Explicit Allocation Purpose

Every private allocation records why it exists.

Main-WAL segments, transaction-log segments, allocator metadata, and
collection data must not be inferred from unrelated surrounding records.

### Idempotent Recovery

Recovery may itself fail or be interrupted and must be safely repeatable.

Repeated recovery must not:

- duplicate a free entry;
- lose a reservation;
- publish an uncommitted effect;
- free live state;
- advance cleanup out of order.

### Crash Equivalence

At every durability boundary, recovery must produce:

- the valid pre-operation state;
- the valid post-operation state; or
- a specifically defined intermediate state with bounded, resumable cleanup.

### Forward Progress Under Declared Reserves

The system must state the exact reserve assumptions needed for:

- main-WAL rotation;
- transaction-log growth;
- allocator basis replacement;
- recovery completion;
- reclaim completion;
- cleanup frees.

Ordinary user work may be refused before consuming those reserves.

## Proposed Specification Layers

The exact file layout remains open, but the normative content should be divided
by reasoning responsibility rather than accumulated in a few large documents.

### Core State And Invariants

Defines:

- region ownership;
- durable versus volatile state;
- retained bases;
- private reservations;
- transaction outcomes;
- global safety properties.

### Device Durability Model

Defines:

- read, write, erase, and sync;
- which effects may survive a crash;
- write and erase alignment;
- torn-write assumptions;
- checksum trust boundaries.

### Durable Encoding

Defines:

- metadata versions;
- headers;
- record formats;
- checksums;
- sequence rules;
- validation of positions and lengths.

It should not define ownership transitions.

### Append-Only Log Primitive

Defines:

- append;
- region initialization;
- link publication;
- tail rotation;
- reachability;
- incomplete-rotation recovery.

It should request a purpose-specific region reservation rather than implement
allocator policy internally.

### Region Ownership And Free Space

Defines:

- ready and dirty queue ranges;
- allocation;
- free;
- erase publication;
- logical cursor behavior;
- bounded prefix compaction;
- free-space snapshots and materialized bases.

### Transactions

Defines:

- begin and enrollment;
- private allocation;
- free intent;
- segment sealing;
- conflict checks;
- commit and rollback;
- cleanup ownership;
- finish and retirement.

### Composed Core Operations

Defines the places where coupling is intentional:

- WAL rotation plus allocator reservation;
- transaction allocation plus allocator pop;
- commit plus ownership transfer;
- rollback plus dirty free;
- reclaim plus basis preservation;
- allocator basis replacement plus old-chain cleanup.

### Startup And Recovery

Defines how the same durable events reconstruct:

- region ownership;
- free-space state;
- WAL reachability;
- transaction outcome;
- pending cleanup;
- retained bases.

### Implementation Refinement And Coverage

Defines:

- the Rust function implementing each transition;
- the Quint model property;
- proof or test obligations;
- crash-injection coverage;
- traceability identifiers.

## Operation Specification Template

Each nontrivial operation should have the same compact structure:

1. Source state.
2. Preconditions.
3. Required reserves.
4. Private planned state.
5. Ordered physical effects.
6. Sync boundaries.
7. Publishing durable event.
8. Stable runtime application.
9. Ownership changes.
10. Recovery before publication.
11. Recovery after publication.
12. Cleanup and reclaim.
13. Postconditions.
14. Model, proof, and test obligations.

This should make cross-protocol operations readable without reconstructing
their behavior from several large narrative documents.

## Quint Modeling Strategy

### Current Model

models/transaction_free_recovery.qnt should be retained as useful work.

It is best understood as a transaction/free-space composition model. Its finite
domain is appropriately small, but it currently combines several conceptual
questions:

- transaction-owned allocation;
- free-intent buffering and sealing;
- commit and rollback publication;
- generation conflicts;
- ordered cleanup;
- the WAL cleanup lock;
- free-queue cursor movement;
- multi-transaction interference.

It is not a complete model of storage crashes, WAL topology, allocator basis
replacement, or startup media selection.

The current model can remain as an integration check while smaller models
provide easier-to-understand property and counterexample boundaries.

### Proposed Focused Models

#### Region Ownership

Checks:

- exactly one owner or recovery role;
- no live/free overlap;
- no lost region;
- reservations carry explicit purpose.

#### Free-Space Basis Publication

Checks:

- old basis remains valid before publication;
- new basis is complete before publication;
- startup selects one complete basis;
- old metadata is not freed early;
- abandoned candidate metadata does not leak;
- basis rollover keeps history bounded.

#### Free-Space Queue

Checks:

- cursor ordering;
- FIFO allocation;
- dirty versus ready behavior;
- erase-before-ready publication;
- bounded prefix compaction;
- stale allocator events are rejected or skipped correctly.

#### WAL Rotation

Checks:

- the target is reserved for WAL use;
- the target is initialized before link publication;
- the chain remains reachable at every crash cut;
- incomplete rotation is recoverable;
- a WAL region cannot be freed as transaction data.

#### Transaction Ownership And Publication

Checks:

- allocations stay private before commit;
- free intents stay live before commit;
- commit transfers ownership atomically;
- rollback returns allocations as dirty;
- generation conflict blocks stale commit.

#### Ordered Transaction Cleanup

Checks:

- one cleanup owner;
- cleanup slots are appended in order;
- finish occurs only after all cleanup;
- recovery resumes at the correct cursor;
- repeated recovery is idempotent.

#### Reclaim Composition

Checks:

- reclaim preserves the replay result;
- all live bases and transaction references remain reachable;
- retired logs are not visited later;
- old regions become free only after replacement publication.

#### Reserve And Progress

Checks:

- maintenance can complete under explicitly stated reserves;
- ordinary allocation cannot consume recovery-critical capacity;
- basis rollover and WAL rotation cannot deadlock each other.

### Model Shape

Each model should explicitly identify:

- modeled state;
- abstracted state;
- assumptions;
- actions;
- safety invariants;
- any liveness property;
- finite bounds;
- expected unsafe comparison;
- repeatable verification command.

Simulation is useful for early counterexamples. The verification lane must run
Quint verification with pinned tool versions and checked bounds or exhaustive
finite exploration appropriate to the model.

## Rust Implementation Direction

### Pure Logical Kernel

The implementation should move logical validation and state application into
small functions without I/O.

The conceptual interface is:

- validate current state and command;
- produce a bounded operation plan;
- apply a validated durable event;
- evaluate explicit invariants.

The pure kernel owns:

- region ownership;
- cursor changes;
- transaction phase changes;
- basis selection;
- reservation purpose;
- cleanup progress.

### Blocking Effect Executor

One blocking executor performs physical effects from the plan:

- erase;
- write;
- sync;
- construct or decode the durable event;
- apply the event to stable runtime state;
- continue cleanup.

There is no parallel Future implementation.

### Shared Apply Functions

Foreground execution and startup replay must call the same logical durable
event application functions.

Recovery may plan new durable events, but it must not invent a second meaning
for existing events.

### Verification-Friendly Boundaries

Formal Rust verification should focus first on pure modules such as:

- region-ownership transitions;
- free-space cursor transitions;
- transaction phase transitions;
- cleanup cursor behavior;
- basis selection and publication state.

Device effects, byte encodings, and integration code may initially rely on:

- property tests;
- fuzzing;
- model-based trace comparison;
- fault injection;
- corruption tests.

The choice of Rust verification tool remains open until the proof surface and
its use of no-std, const generics, heapless containers, and traits are clearer.

## Evidence And Traceability

For each important invariant, the desired chain is:

| Evidence layer | Purpose |
| --- | --- |
| Short English property | Human intent |
| Stable requirement ID | Cross-reference |
| Quint invariant | Abstract state-machine property |
| Rust invariant or contract | Implementation-level meaning |
| Proof harness or independent oracle | Implementation assurance |
| Fault-injection or regression test | Physical integration behavior |
| CI result | Reproducible evidence |

A reviewer should be able to understand the property and evidence without
reading the complete storage implementation.

Formal verification still depends on reviewing that the encoded property
matches the intended English property. The goal is to shrink that trusted
review boundary, not claim it can be eliminated.

## Vertical Implementation Slices

Because the core protocols are coupled, implementation should proceed through
small end-to-end slices rather than completing one subsystem in isolation.

Suggested slices:

1. Format and reopen an unchanged store.
2. Allocate one region and reopen at every crash boundary.
3. Free and erase one region and reopen.
4. Rotate the main WAL and reopen.
5. Start and roll back one transaction-owned allocation.
6. Commit a transaction and resume interrupted cleanup.
7. Publish a replacement free-space basis.
8. Reclaim a WAL head while preserving ownership and replay state.
9. Repeat allocation, rotation, basis replacement, and reclaim across many
   bounded rollover cycles.

Each slice includes:

- specification;
- focused Quint model or model transition;
- blocking implementation;
- shared foreground/replay apply path;
- fault injection;
- invariant and traceability evidence.

## Relationship To Audit Findings

The critical audit findings should be preserved as concrete counterexamples:

- free-space basis rewritten in place;
- free-space queue capacity failure after a durable append;
- transaction-log link published before target initialization;
- main-WAL segment mistaken for transaction-owned data;
- completed transaction log visited after retirement;
- blocking and Future reclaim divergence;
- committed cleanup failure without a resumable phase.

Dropping the Future interface removes one source of divergence. The remaining
findings should become model traces and vertical-slice acceptance tests rather
than being fixed only as isolated local patches.

The detailed free-space basis discrepancy is recorded separately in
[the archived materialization TODO](free-space-basis-materialization-todo.md).

## Proposed Work Order

1. Freeze collection feature development.
2. Remove the async/Future public model and normative requirements.
3. Write the short core principles and property catalog.
4. Define shared region-ownership and durable-publication vocabulary.
5. Add focused Quint models, beginning with region ownership and free-space
   basis publication.
6. Add reproducible Quint verification to the development gate.
7. Decide the next media-format version and compatibility policy.
8. Refactor pure Rust transition modules and one blocking executor.
9. Implement vertical slices with model and crash-test evidence.
10. Evaluate formal Rust verification against the resulting pure modules.
11. Resume collection work only after core exit criteria are satisfied.

## Core Exit Criteria

The core is ready for renewed collection development when:

- every physical region has one recoverable ownership classification;
- all live durable structures are append-only or use explicit replacement
  publication;
- every crash cut recovers to a specified state;
- foreground and replay use the same event semantics;
- bounded capacity failure occurs before incompatible durable writes;
- WAL rotation, basis replacement, reclaim, and transaction cleanup make
  progress under documented reserves;
- focused Quint models verify their properties;
- Rust proofs or tests map directly to the property catalog;
- operation specifications expose their locality, search, I/O, and memory cost
  drivers;
- core recovery and cleanup operations resume from retained ordered state
  without an unspecified whole-device or whole-history search;
- performance regression tests cover locality, search work, write
  amplification, sync count, and wear-distribution signals;
- the complete verification lane is reproducibly green;
- the current media version has one authoritative specification.

## Explicit Non-Goals For This Milestone

- New collection types.
- Object-log feature expansion.
- Channel integration.
- Async or Future APIs.
- Preserving accidental version-2 media behavior at the cost of clarity.
- Proving every backing-driver or encoding implementation immediately.
- Using multiple abstract modeling languages without a demonstrated need.

## Open Questions

The following decisions remain open for further discussion:

- Exact specification file/module boundaries.
- Exact minimal region-ownership state vocabulary.
- Whether the existing Quint model should be split immediately or retained
  unchanged while new focused models are added.
- How explicit volatile and durable device state should be in every model.
- The first bounds and verification backend for each Quint model.
- The new media-format version and migration/reformat policy.
- The exact internal blocking operation-plan representation.
- Which pure Rust module should be the first formal-verification experiment.
- Which Rust verification tool best fits the eventual proof surface.
- The initial operation-cost vocabulary and which costs are normative bounds.
- How locality should be measured without conflicting with FIFO wear leveling.
- Whether benchmark and performance work pauses entirely or remains as a
  regression-only activity.
