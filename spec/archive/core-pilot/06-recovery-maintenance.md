# Startup, Recovery, And Maintenance

> Archived design pilot. This document is retained for historical reference and
> is not current design authority.

## Purpose and motivation

Startup must reconstruct the same ownership and visibility state that existed
after the last durable publication, without relying on volatile caches or an
unbounded search. Maintenance must make deferred work predictable without
smuggling long erase, reclaim, or cleanup paths into a foreground call.

These concerns meet at the WAL database root and its durable cursors. WAL replay
identifies every selected root and next obligation, while maintenance advances
those obligations one bounded transition at a time.

## Discovery and recovery are separate phases

Wear leveling prevents the current roots from living in one repeatedly updated
physical slot: roots and control structures move as FIFO allocation rotates
through the device. Startup therefore begins with one necessary whole-device
discovery pass. It reads metadata, then exactly one fixed-size header from every
physical region in ascending index order. This produces a bounded header index
containing purpose, operation identity, and link metadata needed to validate
retained candidates. Region headers do not contain a global sequence and the
scan does not try to order headers of unrelated purposes. It does not read every
region body or infer ownership merely because a header looks plausible.

The fixed-size region geometry is what keeps this discovery pass from becoming
a scan of every byte. Metadata determines every region start and header offset,
so startup issues one small predictable read per relatively large region.
Sequentially filling an active region before moving to the next FIFO region
keeps the number of discovery headers low relative to stored data while FIFO
selection distributes which physical regions receive subsequent write and
erase cycles.

From the header index, startup identifies candidates whose valid header carries
collection ID `0` and system type `WAL`, then reads and validates each
candidate's WAL preamble. The valid WAL region whose preamble has the largest
WAL sequence number is the tail. Duplicate valid WAL-preamble sequences fail
open with typed corruption, so tail selection is unambiguous. The selected tail
preamble contains the most recently published retained WAL head and an
allocator checkpoint consisting of allocation sequence and allocation head.
Together the head and tail bound the database root. Recovery follows the WAL
chain from head through tail and replays it in order to select the free-space
basis and frontier, collection roots and snapshots, transaction decisions, and
maintenance cursors. Region headers do not select those logical states
independently.

For each collection, replay tracks the WAL positions of its newest snapshot and
newest `head` record. A later update creates or advances its committed memory
frontier. When a frontier exists, it is the collection root. When no frontier
exists, replay chooses whichever tracked snapshot or `head` record occurs later
in WAL order.

Allocator recovery has an additional ordering step because allocation facts
may occur in the main WAL or in several transaction logs. The selected
free-space basis and selected tail preamble each supply an allocation-sequence
and allocation-head pair. Recovery validates both, uses the pair with the
greater allocation sequence as its baseline, then collects every retained later
allocation fact, rejects duplicates, gaps, or invalid FIFO pops, and applies
them in allocation-sequence order. If later facts exist, the
`allocation_head_after` in the fact with the greatest sequence becomes the
recovered allocation head; otherwise the checkpoint head remains current. This
allocator effect is recovered even for an undecided or rolling-back
transaction, whose allocated regions remain transaction-owned until cleanup.

After WAL replay reaches the selected tail, it may have many undecided open
transactions but at most one decided transaction without a finish record. A
commit or rollback decision holds the transaction-finish WAL lock until the
matching finish record, preventing a second decided-but-unfinished transaction.

Recovery first resolves that decided transaction: complete committed apply and
cleanup for a commit, or resume cleanup for a rollback, then append its finish
record. After releasing the lock, recovery rolls back each undecided open
transaction one at a time in durable begin order, writing its rollback and
finish records before moving to the next. Normal WAL mutation remains
unavailable until this recovery queue is empty. Overlapping decision-to-finish
intervals or more open transactions than configured are corruption.

Recovery then reads only the structures named by replay in their explicit
order. It does not make a second device-wide pass and does not scan collection
payloads to reconstruct allocator or transaction state.

This distinction limits startup header I/O to a predictable function of region
count while keeping body reads proportional to the retained WAL and the
structures it names.

### Discovery and reconstructed-state requirements

1. `CORE-REC-001` Startup MUST read metadata and exactly one fixed header from
   every physical region in ascending physical-index order.
2. `CORE-REC-002` After the fixed-header pass, startup MUST validate the WAL
   preamble of each collection-ID-`0`, system-type-`WAL` header candidate,
   select the valid WAL region whose preamble has the largest WAL sequence
   number, obtain the retained head and allocator checkpoint from that preamble,
   and replay head to tail before following any other logical root.
3. `CORE-REC-003` Recovery MUST not perform a second whole-device scan.
4. `CORE-REC-006` WAL replay MUST reconstruct ownership, free-space state,
   collection roots and frontiers, transaction decisions, and maintenance
   obligations before open exposes the database.
5. `CORE-REC-007` Recovery MUST first finish the at-most-one transaction with a
   durable commit or rollback decision, then roll back all undecided open
   transactions one at a time in durable begin order before normal WAL use.
6. `CORE-REC-008` For each collection, replay MUST use a resident committed
   memory frontier when one exists; otherwise it MUST select the later WAL
   position between the newest retained snapshot and collection `head` record.
7. `CORE-REC-009` The fixed-header pass MUST NOT derive a global order from
   unrelated region headers. Startup MUST fail open with a typed corruption
   error for duplicate valid WAL-preamble sequence numbers and otherwise use the
   greatest valid WAL-preamble sequence only for WAL-tail selection.
8. `CORE-REC-010` Allocator recovery MUST validate the allocation-sequence and
   allocation-head pair in the selected tail preamble, compare it with the
   selected free-space-basis checkpoint, use the consistent checkpoint with the
   greater allocation sequence as its baseline, and apply only retained
   allocation facts with later sequences.

## Replay mechanics

Recovery validates purpose before interpreting a region body. For each retained
chain it checks link direction, applicable WAL or allocation sequences,
operation identities, encoded lengths, alignment, checksums, and configured
bounds required by that object's encoding. It then replays durable facts in
order through the same pure ownership, free-queue, catalog, and transaction
transitions used by foreground runtime apply.

In an append stream, separators, reserved-byte exclusion, write-granule
alignment, and individual checksums identify valid record candidates and
allowed torn spans. In a bounded materialization, the enclosing header, length,
and integrity scheme identify its usable contents; recovery does not invent
per-entry append framing. A malformed retained root, impossible transition,
cycle, out-of-range link, or purpose mismatch is a typed corruption error.
Recovery does not “repair” ambiguity by guessing which object was intended.

An erased discovery-header slot is unused. A non-erased header that does not
validate is not a root candidate. If WAL replay or another retained link
requires that region, open fails with a typed corruption error. This distinction
permits recovery from a torn, unpublished header while refusing corruption in
retained state.

Because runtime is applied only after durable publication, a crash can leave
media one transition ahead of memory. Replay is responsible for applying that
transition. Conversely, prepared bytes without a publication remain invisible
and become reclaim work.

### Replay-validation requirements

1. `CORE-REC-004` A corrupt header, encoded length, required checksum, purpose,
   or link required by retained WAL replay MUST produce a typed error rather
   than a panic or ownership guess; an invalid unpublished header MUST NOT be
   treated as a root.
2. `CORE-REC-005` Recovery MUST use retained roots, ordered links, and durable
   cursors instead of rediscovering obligations from unrelated payloads.

## Maintenance model

Maintenance is an explicit blocking API. A task names the kind of work, and one
unbudgeted `maintain_once` call performs at most one declared transition:

- **erase dirty**: select the next FIFO dirty region, preflight WAL capacity,
  erase it, append and sync readiness, and make it allocatable;
- **prepare WAL spare**: durably reserve one FIFO region, write and sync its
  collection-ID-`0`, system-type-`WAL` region header without a WAL sequence,
  leave its WAL preamble invalid, and retain its reservation token;
- **build free-space basis**: write and sync at most one reserved immutable
  replacement segment;
- **publish free-space basis**: append and sync the installation record only
  after all segments are durable;
- **snapshot collection**: serialize one bounded resident frontier as a
  self-contained WAL snapshot, append and sync it, then make the clean frontier
  slot evictable;
- **reclaim WAL**: after a new tail preamble checkpoints the allocator effects
  and retained replacements cover all other live facts in the excluded prefix,
  process the next segment excluded by the published retained boundary; and
- **finish transaction**: process the next ordered cleanup obligation and
  advance its durable cursor.

When an operation accepts an explicit numeric budget, it may repeat the named
transition only up to that budget and must report the remaining pressure.
Maintenance never searches for work that should have been retained in an
ordered list or cursor.

### Maintenance-step requirements

1. `CORE-MAINT-001` Maintenance MUST be caller-invoked and bounded to one
   declared transition or caller-supplied budget.
2. `CORE-MAINT-002` Erase maintenance MUST erase one selected dirty region, then
   append and sync its readiness publication.
3. `CORE-MAINT-003` Basis construction MUST write and sync at most one new basis
   segment per unbudgeted `maintain_once` call.
4. `CORE-MAINT-004` Basis publication MUST be a separate final step after all
   replacement segments are durable.

## Idempotence and crash behavior

Each maintenance step has a durable publication point and may be retried after
a crash. An erase whose readiness record did not become durable leaves the
region logically dirty and safe to erase again. A completely written basis or
WAL spare without its installation/link remains unpublished. A cleanup free
whose cursor did not advance must be recognizable as an idempotent replay of
the same obligation.

### Resumption requirement

1. `CORE-MAINT-005` Cleanup and reclaim MUST resume from their durable cursor
   and MUST NOT search the device for their next obligation.
