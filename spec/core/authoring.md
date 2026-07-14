# Core Specification Authoring Guide

## Purpose

This guide records how the Borromean core specification is developed. It is a
flexible checklist, not a mandatory document layout. Authors may omit or reorder
sections when that improves the explanation, provided the resulting chapter
still gives every normative rule one authoritative home and does not leave a
required operation or invariant unspecified.

The specification uses a spiral presentation because its subsystems motivate
one another. The main WAL, transaction logs, free list, collections, and
recovery cannot be explained in a useful strict dependency order. A concept may
therefore be introduced before its complete mechanics, but its first use must
state the minimum stable contract on which the surrounding explanation relies.

Later passes add precision without changing the meaning of earlier claims. If a
deep dive reveals that an earlier summary is inaccurate, the summary is
corrected; the inconsistency is not resolved by silently giving one statement
precedence.

## Three-pass structure

### System narrative

The system narrative gradually builds the reader's mental model, moving between
subsystems as their motivations require. A narrative chapter normally covers:

1. the constraint or problem being addressed;
2. the new concept and why it follows from concepts already introduced;
3. the minimum contract needed by the next part of the narrative;
4. important interactions, lifecycle paths, tradeoffs, and non-goals; and
5. a forward reference to the authoritative component deep dive.

The narrative should avoid record fields, exact lock spans, Rust struct layouts,
and exhaustive crash cuts unless one is essential to understanding the core
idea at that point.

### Component deep dive

A component deep dive is the authoritative home for one subsystem's complete
abstract and mechanical design. Its useful sections normally include:

1. purpose, scope, and explicitly excluded responsibilities;
2. dependencies and vocabulary used from earlier chapters;
3. abstract objects, state, relationships, and invariants;
4. embedded, compile-checked normative Rust definitions of state, operation
   progress, inputs, results, errors, and pure transitions;
5. persistent objects and their explicit codecs;
6. state-changing and read-only operations;
7. local crash, replay, retry, and reclaim interpretation;
8. concurrency, memory, I/O, search, and deferred-work bounds; and
9. normative requirements and their verification obligations.

An operation that changes durable or shared state should answer:

1. What are its logical preconditions and complete capacity preflight?
2. Which locks or reservations are acquired, what do they protect, and what is
   revalidated while they are held?
3. What physical reads, writes, erases, and syncs occur, in what order?
4. Which durable fact publishes the transition?
5. Which pure transition applies the durable fact to runtime state?
6. What can each error and meaningful crash cut leave behind?
7. How do replay, retry, cleanup, or fail-stop behavior interpret that state?
8. What bounds apply to memory, I/O, traversal, latency, and deferred work?

### Composition

A composition chapter reconnects already-defined component contracts. It does
not create a second definition of their local behavior. It normally covers:

1. participating component contracts and their dependency order;
2. cross-component lock ordering and publication ordering;
3. global admission, progress, and capacity closure;
4. startup discovery, replay, recovery, and maintenance orchestration;
5. composed safety and liveness invariants; and
6. end-to-end refinement and crash evidence.

## Embedded, compile-checked normative Rust

Precise mechanical definitions are written in designated normative Rust code
blocks alongside the prose they refine. Specification verification extracts
these blocks into a generated crate and requires them to compile and type-check.
The Markdown blocks are authoritative; extracted files are generated artifacts
with no independent normative status.

Normative Rust should define semantic in-memory state, operation-progress state
when interruption matters, exact inputs and results, validation errors, and the
pure transitions shared by foreground apply and replay. Production code should
reuse the extracted normative types and transitions directly where practical.
When an optimized implementation uses a different representation, it must
define an explicit projection to the normative state and demonstrate that its
operations refine the normative transitions.

Prose should explain the nearby normative definitions rather than restating
them as pseudocode. Illustrative Rust must be explicitly marked non-normative.
Required scaffolding must be small and visible in designated blocks; extraction
must not silently supply semantic definitions. Verification diagnostics should
identify the originating Markdown file and line.

Compilation checks syntax and types, not behavioral correctness. Embedded tests,
models, or other evidence must separately verify the required transition and
invariant properties. Prose remains authoritative for meaning that a Rust type
does not express by itself, including durability, visibility, ownership,
concurrency, crash behavior, and resource bounds.

Rust memory layout is not a persistent codec. On-media formats use fixed-width
fields, explicit endianness, explicit framing and padding, and defined integrity
checks. A format may use a Rust type as its logical value model, but encoding
and decoding define its durable bytes.

## Testable requirements and traceability

Only a normative rule with a concrete semantic pass/fail method receives a
requirement ID. The rule must have an authoritative home in a component or
composition chapter and must state what evidence computes or establishes.
System-narrative previews, rationale, assumptions, examples, and unresolved
design claims do not receive requirement IDs.

Evidence may be an executable Rust test, a compile or static check, or a model
check. It must exercise, compute, or mechanically establish the property. A
source search that merely finds an identifier, field name, function name, or
other string is not evidence. A citation may aid navigation, but under the
current policy only test evidence counts toward requirement coverage.

An ID may be assigned before its evidence is implemented when the pass/fail
method is already precise. The requirement then remains visibly unmet; authors
must not add a placeholder or source-text search and label it as satisfying
evidence.

### Canonical requirement record

The canonical traceability identity is the complete normalized requirement
record, not the ID by itself. The ID is a convenient label and must be unique
among active requirements. Use a semantic namespace that does not encode a
chapter number, filename, section, or specification version.

Write the behavior and its verification method as one grammatical sentence.
The sentence contains one RFC-2119 keyword, then uses a semicolon to introduce a
`Verification` clause without another RFC-2119 keyword. For example:

```markdown
1. `CORE-ALLOC-001` The runtime allocation cursor MUST advance only after the
   head-consuming command is durable; **Verification:** an executable crash-cut
   check computes recovered state at every interruption point through runtime
   apply.
```

The verification clause describes both the exercised cases and the oracle that
decides whether the requirement holds. Naming a test kind without saying what
it computes or establishes is insufficient.

### Evidence annotation

A test annotation quotes the complete canonical record, including the
`Verification` clause. The final traced entry point is dedicated to that one
requirement, although setup, generators, and assertion helpers may be shared.
The following is a non-normative shape example:

```rust
//= spec/core/allocator.md#allocation-publication
//= type=test
//# `CORE-ALLOC-001` The runtime allocation cursor MUST advance only after the
//# head-consuming command is durable; **Verification:** an executable crash-cut
//# check computes recovered state at every interruption point through runtime
//# apply.
#[test]
fn requirement_allocation_cursor_advances_after_durability() {
    // Exercise every specified crash cut and compare recovered state.
}
```

Line wrapping may differ between the Markdown and evidence annotation because
matching normalizes whitespace. The words and punctuation of the canonical
record do not differ.

Duvet 0.4.1 matches quoted text spans rather than semantic IDs and accepts
partial, even ID-only, quotations as test coverage. Consequently, complete
quotation is currently an authoring and review convention rather than a property
enforced by the traceability tooling. A passing Duvet report does not prove that
an annotation includes the complete canonical record. Purpose-built enforcement
is deferred until after a working v3 exists, or later.

Model evidence counts only when a verification process runs the model and
identifies the invariant or temporal property that it checked. A Rust
placeholder does not substitute for running the model. Integration of that
evidence into requirement reporting is deferred with the purpose-built
traceability tooling.

### Moving and revising requirements

Moving an unchanged canonical record preserves its ID and text. The move also
updates every Duvet document-and-anchor reference. Traceability checks should
fail until all evidence follows the requirement to its authoritative location.

Reflow does not change a requirement's identity. A substantive behavior or
verification change updates the complete record and every evidence quotation
and is reviewed as a change to the acceptance contract. IDs remain unique among
active records, but no retired-ID ledger is maintained for now; Git retains the
history, and the complete behavior-and-verification text is more important than
permanent reservation of its label.

## Authority and refinement rules

- A preview states only the minimum contract needed at that point and links to
  the detailed owner of the rule.
- Each precise fact has one authoritative detailed home. Other chapters
  summarize and reference it rather than independently redefining it.
- A later pass may add detail but must not change an earlier contract without
  correcting that earlier text.
- Abstract state, embedded compile-checked Rust transitions, persistent codecs,
  I/O orchestration, and verification evidence remain distinct layers with
  explicit connections.
- Exact private state is normative when its semantics, recovery, concurrency, or
  memory bound matters. Incidental organization and scratch layout need only
  satisfy their specified ownership and capacity contracts.
