# Shared Vocabulary Decisions

Cross-component terms for storage, collections, durability, and logs.

These records preserve reviewed decisions moved from the active
[design queue](../todo.md). Later decisions may explicitly supersede an earlier
record.

## D04A — Storage and encoding units

Agree the minimum cross-chapter
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

## D04B1 — Durable authority and publication

Agree the minimum
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

## D04B2 — Reachability

Agree the minimum cross-chapter meaning of
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

## D04B3 — Retention

Agree the minimum cross-chapter meaning of retained
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

## D04C1 — Collection root

Agree the minimum cross-chapter meaning of a
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

## D04C3 — Basis, snapshot, and materialization

Agree their minimum
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

## D04C2 — Head terminology

Agree qualified uses of head without
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

## D04D — Log roles

Agree provisional cross-chapter definitions of the
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
