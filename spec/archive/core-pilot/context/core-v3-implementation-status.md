# Core v3 Implementation Status

> **Archive status:** Historical implementation checkpoint from 2026-07-10.
> Requirement identifiers and paths refer to the superseded core pilot.

Date: 2026-07-10

This is an implementation-delta and verification note. It is not a source of
normative storage behavior. The requirement identifiers below refer to the
[archived core pilot](../pilot-index.md).

A passing provisional test means only that the currently implemented behavior
is internally consistent with that test. It does not establish v3 conformance
when the implementation or test has not yet been refined to the current
requirement.

## Status vocabulary

- **Foundation implemented**: reusable code exists and its local tests pass,
  but higher-level durable integration may remain.
- **Partial**: some required transitions exist, but the requirement group is
  not satisfied end to end.
- **Nonconforming provisional path**: code exists but implements a superseded
  or incomplete design.
- **Not implemented**: no v3 vertical slice satisfies the requirement group.
- **Blocked by specification**: a design decision or mechanical protocol must
  be completed before implementation should proceed.

## Requirement conformance matrix

| Requirement group | Status | Current implementation delta |
| --- | --- | --- |
| Locality and fixed geometry (`CORE-LOC-001..009`) | Partial | Geometry-aware regions and one-header-per-region tracing exist. The provisional evidence does not yet cover complete append extents including padding, append-stream fill thresholds, deliberately permuted FIFO selection, exact cross-region visited sets, collection read bounds, cleanup read traces, or discovery-header provenance. Bounded collection materializations retain collection-specific physical write patterns. The chapter's unnumbered design goals are review criteria rather than requirement rows. |
| Ownership lifecycle (`CORE-OWN-001..011`) | Foundation implemented | The pure lifecycle and non-copyable reservation token exist. Not every durable v3 path uses the ownership transitions; owner/purpose/operation compatibility and runtime/replay error equivalence are not yet established end to end. |
| Durability and structural I/O (`CORE-DUR-*`, `CORE-IO-*`) | Partial | The fault flash distinguishes working and durable images and traces primitive I/O. Inline append, logical free, erase readiness, and spare preparation have trace tests. The full operation set and all declared bounds are not covered. |
| V3 metadata and raw format (`CORE-FMT-001..008`) | Partial | Metadata-last format, geometry rejection, and v3 version rejection exist. The provisional header encoding does not yet implement the current system collection-ID/type contract and incorrectly retains a global region sequence field. |
| WAL and allocation sequencing (`CORE-FMT-009..010`, `CORE-LOG-022`, `CORE-LOG-025`, `CORE-REC-009..010`, `CORE-FREE-012..015`, `CORE-TX-020`) | Nonconforming provisional path | The provisional code puts one sequence in every region header. V3 instead needs a WAL-tail sequence plus an allocation-sequence/allocation-head checkpoint in each valid WAL preamble, and an independent allocation sequence in every main-WAL and transaction-log allocation fact. Checkpoint capture/selection, replay ordering, duplicate/gap validation, exhaustion, and maximum-sequence `allocation_head_after` recovery are not implemented. |
| Append framing (`CORE-LOG-007..014`) | Nonconforming provisional path | `EVT3` uses length/checksum framing and erased-byte padding. It does not implement the configured separator, deterministic byte stuffing, reserved-byte exclusion, canonical padding, or aligned torn-span resynchronization. |
| WAL rotation (`CORE-LOG-001..005`, `CORE-LOG-024..025`, `CORE-IO-005`) | Nonconforming provisional path | A spare can be reserved, written, synced, linked, and recovered, but preparation currently writes a complete WAL prologue. The required prepared state has a final region header and an invalid preamble until tail publication, when the preamble must capture a consistent allocation-sequence/allocation-head pair. Rotation crash cuts and sync bounds must be rebuilt around that rule. |
| WAL-rooted startup (`CORE-LOG-018..021`, `CORE-REC-001..010`) | Nonconforming provisional path | Open scans fixed headers but starts from the metadata-named initial WAL and follows links forward. It must validate WAL preambles, select the valid WAL region with the largest WAL sequence as tail, obtain the retained head and allocator checkpoint from that preamble, compare that checkpoint with the selected basis, and replay head-to-tail as the sole logical root. |
| Collection frontier, snapshot, and root selection (`CORE-LOC-010..015`, `CORE-LOG-015..017`, `CORE-LOG-023`, `CORE-API-011..012`) | Not implemented | The v3 catalog stores collection identity and generation only. It lacks bounded committed frontiers, WAL snapshots, snapshot/head ordering, frontier eviction/reload, collection-specific materialization, and frontier-capacity admission. |
| FIFO free space and immutable basis replacement (`CORE-FREE-001..011`, `CORE-MAINT-003..005`) | Partial | The pure monotonic FIFO and copy-on-write replacement construction exist. The initial WAL does not yet select the bootstrap basis, and multi-segment writing, installation replay, frontier reset, abandoned-build recovery, old-basis reclaim, and forward-progress integration are absent from the blocking facade. |
| Transaction visibility and atomic decision (`CORE-TX-001..010`, `CORE-TX-018..019`) | Partial | Pure committed/private generation views and one-record catalog generation commit exist. They do not yet cover collection payload roots, cross-transaction enrollment, finish-lock write exclusion, transaction-private logs, durable rollback decisions, or complete multi-open visibility. |
| Multiple open transactions and finish lock (`CORE-TX-011..017`, `CORE-API-009`) | Nonconforming provisional path | `TransactionMemory` represents one transaction and there is no fixed-capacity registry or decision-to-finish WAL lock. Ordered durable cleanup and recovery of many undecided transactions are absent. |
| Startup transaction recovery (`CORE-TX-013..017`, `CORE-REC-007`) | Not implemented | Replay does not yield the optional decided-but-unfinished transaction plus all undecided open transactions, finish the decided outcome, and then roll back opens in durable begin order. |
| Explicit maintenance (`CORE-MAINT-001..005`, `CORE-API-007`) | Partial | `EraseDirty` and `PrepareWalSpare` perform work. Basis build/publication, WAL reclaim, collection checkpointing, and transaction finish currently report pressure or remain outside the v3 facade. |
| WAL-head reclaim (`CORE-LOG-006`, `CORE-LOG-021`, `CORE-LOG-025`, `CORE-MAINT-005`) | Not implemented | No v3 path publishes a replacement tail/preamble with a new retained head and allocator checkpoint, preserves the necessary ownership/transaction/cleanup evidence, or reclaims the excluded prefix through ordered dirty frees. |
| Blocking API and bounded memory (`CORE-API-001..012`) | Partial | `RawFlash`, `V3Memory`, blocking format/open, operation results, and maintenance flags exist. Legacy public Future APIs remain, typed collections are not adapted, and current transaction/frontier memory does not satisfy the revised bounds. |
| Refinement evidence (`spec/core/08-refinement.md`) | Stale | The existing focused models and tests predate several current requirements. Their successful execution is a baseline, not evidence that the current refinement rows are complete. |

No row above should be marked conforming until the refinement matrix identifies
the current model property, Rust transition, and assertion-bearing test for
every requirement in that group.

## Known specification gaps

These items are not merely missing Rust. Their mechanical design needs more
normative detail before implementation should be considered stable.

### Append-stream torn-span recovery

The core defines separator encoding, granule alignment, reserved-byte
exclusion, and candidate classifications, but `CORE-LOG-013` still refers to a
“specified torn-span scan rule” without fully defining when recovery stops,
advances one granule, reports corruption, or requires an explicit recovery
record after a damaged candidate. The older v2 WAL specification contains a
more detailed rule, but it should not be copied into v3 without explicit review.

### Transaction-private log protocol

The transaction specification defines visibility, atomic decision, finish
locking, and ordered cleanup, but does not yet fully define:

- transaction-log region headers, preambles, and segment links;
- transaction-log allocation-entry placement, segment discovery, and use of the
  global allocator lock;
- private payload/free-intent materialization and sealing;
- discovery and retention of open-transaction allocation facts before a
  decision or replacement allocator basis;
- exact commit/rollback cleanup-list encoding and cursor publication; and
- the I/O and capacity bounds for log growth, decision, finish, and recovery.

### WAL-head reclaim protocol

The core requires a new tail/preamble generation before excluding the old head,
but still needs a complete reclaim operation specification covering:

- selection and ordered preservation of live collection bases, snapshots,
  allocator facts, and transaction state;
- capacity preflight and prepared-spare requirements;
- publication of the new retained head and a consistent
  allocation-sequence/allocation-head checkpoint covering allocator facts in
  the excluded prefix;
- every crash cut while copying or rewriting retained facts; and
- ordered freeing of excluded WAL regions after publication.

### Free-space replacement forward progress

The immutable replacement rules exist, but the specification still needs to
close the recursive-capacity and abandoned-build details recorded in
[`free-space-basis-materialization-todo.md`](free-space-basis-materialization-todo.md),
including how replacement segments are reserved when the allocator frontier is
near its bound and how incomplete private replacement regions are recovered.

### Frontier admission policy

`CORE-API-011` currently requires memory-frontier capacity before WAL append,
but that choice is intentionally not frozen. The active design queue retains
the alternatives and decision gate under its bounded runtime-memory discussion.

### Typed collection operation specifications

Map, object-log, and channel v3 operation sets still need their own layouts,
query rules, snapshot/materialization formats, memory limits, and structural I/O
bounds. Those belong in typed collection specifications rather than in the
generic core.

## Implementation-only cutover work

The following work does not add core storage semantics and should not be copied
into normative requirements:

- adapt map, object log, and channel to the blocking v3 core;
- adapt embedded-storage and file backends to the final raw v3 interface;
- remove legacy Future APIs and v2 implementation modules after cutover;
- replace `RING-*` trace IDs and Duvet configuration with `CORE-*` evidence;
- remove obsolete v2 specifications only after the normative cutover; and
- resolve or eliminate the legacy retired-transaction visitor regression. An
  unsafe workaround that skips an erased finished body can discard the only
  retained collection-head fact and remains prohibited.

## Provisional implementation inventory

The following foundation code exists and has local tests, but its presence does
not override the conformance states above:

- Rust 1.90 and the no-std RISC-V target are pinned;
- Quint 0.32.0 and Apalache 0.56.1 are pinned with a reproducible runner;
- the blocking raw v3 device trait exposes associated errors and geometry;
- the fault flash supports absent, complete, and torn unsynced writes and
  records logical/primitive I/O;
- pure ownership, monotonic FIFO, basis-construction, catalog, and provisional
  transaction transitions exist;
- metadata-last v3 formatting and fixed-header scanning exist;
- provisional inline append, spare preparation/rotation, logical free, erase
  readiness, collection catalog creation, and catalog-generation commit slices
  exist; and
- library formatting, lint, no-std, model-runner, and regression-test commands
  are wired into repository tooling.

## Verification snapshot

These results describe the provisional tree at this checkpoint. They are not a
claim that the current specification is implemented.

- The ten smaller focused Quint files typecheck and pass their current bounded
  Apalache properties. Models outside the ownership pilot remain stale where the
  refinement matrix still records pending WAL-preamble, frontier/snapshot, and
  composition work.
- The ownership pilot now has an independently executable abstract model, a
  shared mechanical durability model, and a forward-refinement bridge. Their
  model-enforced TLC bounds cover abstract ownership (8 actions), mechanical
  composition (6), reservation/publication (13), release/reuse (8), transaction
  decision through cleanup/finish (21), crash/replay (10), and short full
  composition (6). The unchanged legacy recovery relation also passes through
  an eight-action TLC wrapper. Four unsafe controls produce the expected
  invariant violations.
- V3 kernel tests: 26 passing against the provisional kernel behavior.
- Default legacy plus v3 library tests: 510 passing.
- Stable no-std RISC-V library build: passing.
- Formatting, Markdown lint, diff check, and strict library Clippy: passing.
- All-feature suite: 547 of 548 tests pass. The remaining failure is the known
  retired transaction-log/map-open regression. The two invalid
  embedded-storage geometry fixtures were corrected.

The next status update should report progress by changing matrix cells and
linking their current refinement evidence, not by restating normative behavior.
