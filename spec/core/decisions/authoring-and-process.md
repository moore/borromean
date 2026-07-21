# Authoring and Process Decisions

Specification structure, evidence policy, and review sequencing.

These records preserve reviewed decisions moved from the active
[design queue](../todo.md). Later decisions may explicitly supersede an earlier
record.

## D01 — Document layers and chapter template

Agree how each chapter
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

## D02 — Requirement and evidence method

Agree when stable requirement
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

## D02A — Defer complete-record enforcement

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

## D02B — Defer model-evidence integration

Decision: Design model-check registration as part of the same purpose-built
post-v3 traceability tooling, rather than adding an adapter to Duvet now.

Rationale: A model check must run the model and identify the checked property;
a placeholder Rust test is not evidence. The right representation depends on
the future traceability design and need not block the v3 specification.

Patch scope: Retain the question in the post-v3 tooling backlog only. Do not
change models, Duvet configuration, or verification scripts.

Verification: Confirm D03 becomes the first unchecked design item and run
Markdown and diff checks.

## D03 — Defer the implementation-preservation inventory

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

## D05 — Exact chapter spine and dependency cycle

Agree the chapter
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
