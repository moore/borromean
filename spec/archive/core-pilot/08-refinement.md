# V3 Refinement Matrix

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose

The narrative design communicates the intended machine, while requirements
make its obligations traceable. The refinement matrix connects those
obligations to three different kinds of evidence:

1. a small Quint model explores the abstract state machine and crash
   interleavings within a bounded configuration;
2. a pure Rust transition implements the same state vocabulary and invariants;
   and
3. tests show that the encoded I/O and recovery paths refine that transition
   within their declared budgets.

No one column substitutes for the others. A model can prove the wrong
abstraction, a transition can be correct but called in the wrong durability
order, and an end-to-end test can execute a path without asserting its safety
property.

## Reading and completing a row

Read each row from left to right: the property summarizes the design claim,
the requirement range defines conformance, the model checks the abstract
machine, and the Rust evidence connects concrete behavior back to it.

This matrix is expanded as vertical slices land. A row is complete only when
all named requirements are represented in the model or explicitly identified
as concrete-only I/O constraints, the Rust transition uses equivalent state,
and the named test checks the stated property rather than merely executing
code. Incomplete rows remain visible during implementation; they are not
silently treated as satisfied because a neighboring test passes.

Unnumbered design goals do not appear as requirement rows. Design review checks
that the complete refined requirement set realizes those goals; the matrix is
reserved for statements with an actual verification oracle.

| Property | Requirements | Quint model | Rust transition/test |
| --- | --- | --- | --- |
| Durability and operation contracts | `CORE-DUR-001..005`, `CORE-IO-001..009` | `core_composition` plus concrete I/O | durability-order and exact operation-pattern/limit tests |
| Append-stream cursor locality | `CORE-LOC-001` | concrete append encoding and I/O ordering | pending: cursor, complete-extent, and canonical-padding trace tests for WAL and transaction allocation appenders |
| FIFO locality | `CORE-LOC-002` | `fifo_wear` | free-queue selection tests with permuted physical region indices |
| Bounded cross-region traversal | `CORE-LOC-003`, `CORE-LOC-005..006` | pending: traversal abstraction | pending: exact visited-region and discovery-header provenance tests for every structure/collection read operation |
| Ordered cleanup locality | `CORE-LOC-004` | `ordered_cleanup` | cleanup crash-cut, cursor-resume, and exact read-trace tests |
| Fixed geometry and discovery pass | `CORE-LOC-007`, `CORE-LOC-009` | concrete geometry | geometry boundary/property tests and exact one-header-per-region discovery traces |
| Append-stream fill threshold | `CORE-LOC-008` | concrete capacity arithmetic | pending: below/at/above aligned-record-extent-plus-reserve tests for each append-stream writer |
| Frontier memory declaration and reuse | `CORE-LOC-010`, `CORE-LOC-014` | `capacity_preflight`, `core_composition` | pending: configuration arithmetic, zero-I/O capacity rejection, eviction, reopen, and reconstruction tests |
| Committed collection root selection | `CORE-LOC-011`, `CORE-LOC-015` | `transaction_views`, `core_composition` | pending: resident-frontier and snapshot/head-order read-result plus exact-read-trace tests |
| Materialization and snapshot equivalence | `CORE-LOC-012..013` | collection reference models plus concrete I/O | pending: generated pre/post query equivalence and snapshot-only replay tests per collection format; collection-specific specifications own physical write-pattern tests |
| WAL-tail sequencing | `CORE-FMT-009..010`, `CORE-LOG-022`, `CORE-REC-009` | `wal_rotation`, `core_composition` | duplicate-preamble corruption, maximum-WAL-sequence tail selection, rotation crash-restart, and exhaustion tests |
| WAL frontier retention | `CORE-LOG-015..017`, `CORE-LOG-023`, `CORE-API-011..012` | `core_composition` | snapshot/head ordering, frontier replay/eviction, read visibility, and flush-budget tests |
| Unique region ownership | `CORE-OWN-001..011` | `region_ownership` abstract machine, `storage_mechanical`, and `ownership_refinement` forward-simulation bridge | lifecycle source-state/error tests, purpose- and operation-safe token publication, non-copyable-token API checks, and foreground/replay equivalence tests |
| Format publication and namespace | `CORE-FMT-001..008`, `CORE-FREE-003`, `CORE-FREE-011` | `core_composition` plus concrete encoding | format crash cuts, bootstrap selection, version, namespace, sequence-free region headers, and geometry tests |
| FIFO wear | `CORE-FREE-008..010` | `fifo_wear` | free-queue full-cycle tests |
| Allocation ordering and recovery | `CORE-FREE-012..015`, `CORE-LOG-025`, `CORE-REC-010`, `CORE-TX-020` | `fifo_wear`, `free_basis_publication`, `wal_rotation`, `ordered_cleanup` | main-WAL/transaction-log interleaving, sequence validation, preamble/basis checkpoint selection, maximum-sequence `allocation_head_after`, and ownership-retention tests |
| Immutable basis install | `CORE-DUR-005`, `CORE-FREE-001..007`, `CORE-FREE-011`, `CORE-FREE-014..015` | `free_basis_publication` | basis interval, bootstrap selection, allocation-sequence checkpoint, frontier-capacity, retention, and crash-cut tests |
| Safe WAL rotation | `CORE-LOG-001..005`, `CORE-LOG-024..025`, `CORE-IO-005` | `wal_rotation` | prepared-preamble exclusion, consistent allocator-checkpoint capture, and rotation I/O/crash tests |
| WAL-head reclaim | `CORE-LOG-006`, `CORE-LOG-021`, `CORE-LOG-025`, `CORE-MAINT-005` | `wal_rotation`, `core_composition` | retained-fact preservation, allocation-checkpoint coverage, head publication, cursor resume, and excluded-region free tests |
| WAL-rooted startup | `CORE-LOG-018..021`, `CORE-REC-001..010` | `wal_rotation`, `core_composition` | valid-header-and-preamble discovery, maximum-WAL-sequence selection, allocator-checkpoint recovery, bounded replay, decided-first and multiple-open transaction recovery tests |
| Append framing | `CORE-FMT-006`, `CORE-LOG-007..014` | concrete encoding | codec and torn-span scan tests |
| Atomic commit | `CORE-TX-006..010` | `transaction_atomicity` | multi-collection crash tests |
| Read visibility | `CORE-TX-001..005`, `CORE-TX-018..019` | `transaction_views`, `writer_exclusion` | committed/private view, cross-transaction exclusion, and finish-lock read/write tests |
| Ordered cleanup | `CORE-TX-009..017` | `ordered_cleanup` | rollback-decision, cleanup cursor, finish-lock exclusion, multiple-open recovery, overlap rejection, and recovery-decision crash tests |
| Safe reuse | `CORE-OWN-007..008`, `CORE-FREE-009` | `reclaim_reuse` | dirty/erase/readiness tests |
| Preflight before durability | `CORE-FREE-007`, `CORE-API-005` | `capacity_preflight` | zero-I/O rejection tests |
| Blocking API and bounded memory | `CORE-API-001..010` | concrete API | no-std, caller-memory, error, maintenance, and no-Future tests |
| Explicit maintenance | `CORE-MAINT-001..005` | `core_composition` | one-step bounds, cursor resume, and maintenance I/O tests |
| Composed safety | all above | `core_composition` | end-to-end crash harness |

## Refinement expectations

For ownership, dependency direction is explicit: the abstract and mechanical
models are independent, and a third proof module imports both. The bridge's
abstraction function replays valid durable mechanical ownership events. Every
mechanical step must map to one abstract event batch or an abstract stutter;
the proof does not require the mechanical model to import every chapter model.
Later chapter bridges should follow this shape unless their abstraction is
relational rather than functional.

The ownership abstraction, mechanical protocol, and bridge currently use
Quint's TLC backend. The unchanged legacy recovery model is checked through an
eight-action TLC wrapper around its exact transition and safety relation.
Apalache 0.56.1 verifies the smaller focused models, but its Z3 translation
reports `UNKNOWN` or fails to complete for the richer ownership and legacy
protocols. An unsupported translation is not recorded as successful
verification.

The ownership models enforce their trace bounds as model state because Quint
0.32.0 does not pass `--max-steps` to TLC. The abstract, mechanical, and short
full-composition bounds are 8, 6, and 6 actions. Focused refinement bounds are
13 actions for reservation/publication, 8 for release/reuse, 21 for transaction
decision/cleanup/finish, and 10 for crash/replay. The checked-in TLC runtime
configuration caps memory and workers, and the verification runner executes
rich checks sequentially with a timeout.

- Requirement identifiers appear beside the model invariant, Rust transition,
  and test that provide evidence for them.
- Abstract operations that combine several concrete I/O steps document the
  allowed intermediate crash states.
- Concrete-only concerns such as append separators, reserved-byte exclusion,
  granule alignment, checksums, transfer splitting, and exact I/O counts have
  direct Rust tests even when omitted from a pure model.
- Bounded model configurations are replayable against the pure Rust
  transitions so the two state vocabularies cannot drift unnoticed.
- A counterexample or failing crash test is resolved in the design or
  transition; it is not excluded only because the current workload seems
  unlikely to trigger it.

## Required crash scenarios

- failure before and after every media write, erase, and sync;
- absent, complete, and allowed torn unsynced writes;
- basis construction and publication with one and several segments;
- WAL and transaction-log rotation before and after link publication;
- transaction commit before decision, after decision, and during cleanup;
- allocator frontier exhaustion before a proposed durable event;
- repeated rollover, reclaim, and controlled FIFO wear cycles.
